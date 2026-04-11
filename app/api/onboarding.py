import html
import json
import logging
import os
import re
import time
import uuid

import requests
from fastapi import APIRouter, Body, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.clients.evotor_client import fetch_stores_by_token
from app.clients.telegram_client import TelegramClient
from app.db import get_connection, adapt_query as aq
from app.stores.telegram_link_token_store import (
    create_telegram_link_token,
    get_active_telegram_link_token,
    get_telegram_link_token_by_value,
    mark_telegram_link_token_linked,
)

router = APIRouter(tags=["Onboarding"])
log = logging.getLogger("api.onboarding")

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
TELEGRAM_LINK_TOKEN_TTL_SEC = 60 * 60

MS_APP_ID = os.getenv("MS_APP_ID", "").strip()
MS_VENDOR_API_BASE = "https://apps-api.moysklad.ru/api/vendor/1.0"


def _get_ms_context(context_key: str) -> dict | None:
    secret = os.getenv("MS_VENDOR_SECRET_KEY", "").strip()
    if not MS_APP_ID or not secret:
        return None
    try:
        url = f"{MS_VENDOR_API_BASE}/apps/{MS_APP_ID}/context"
        r = requests.get(url, params={"contextKey": context_key},
                         headers={"Authorization": f"Bearer {secret}"}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.exception("Failed to get MS context err=%s", e)
        return None


def _extract_telegram_link_token_from_text(text: str) -> str | None:
    parts = (text or "").strip().split(maxsplit=1)
    if len(parts) != 2:
        return None
    command, payload = parts
    if not command.startswith("/start"):
        return None
    if not payload.startswith("tglink_"):
        return None
    return payload[len("tglink_"):].strip() or None


# ---------------------------------------------------------------------------
# MoySklad helpers
# ---------------------------------------------------------------------------

def _ms_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept-Encoding": "gzip", "Content-Type": "application/json"}


def _ms_fetch(path: str, token: str, params: dict | None = None) -> dict:
    url = f"{MS_BASE}{path}"
    r = requests.get(url, headers=_ms_headers(token), params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def _ms_fetch_all(token: str) -> tuple[list[dict], list[dict], list[dict]]:
    def extract(data: dict) -> list[dict]:
        return [
            {"id": row["id"], "name": row.get("name") or row.get("description") or row["id"]}
            for row in data.get("rows", []) if row.get("id")
        ]
    orgs = extract(_ms_fetch("/entity/organization", token))
    stores = extract(_ms_fetch("/entity/store", token))
    agents = extract(_ms_fetch("/entity/counterparty", token, params={"limit": 100}))
    return orgs, stores, agents


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_session(session_id: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(aq("SELECT * FROM evotor_onboarding_sessions WHERE id = ?"), (session_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Onboarding session not found")
    return dict(row)


def _extract_store_id(store: dict) -> str:
    return str(store.get("id") or store.get("uuid") or store.get("storeId") or "").strip()


def _extract_store_name(store: dict, fallback_id: str) -> str:
    return str(store.get("name") or store.get("title") or store.get("storeName") or f"Store {fallback_id}").strip()


def _load_tenant(tenant_id: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(aq("SELECT * FROM tenants WHERE id = ?"), (tenant_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return dict(row)


def _get_telegram_bot_username() -> str:
    return os.getenv("TELEGRAM_BOT_USERNAME", "").strip().lstrip("@")


def _get_telegram_bot_token() -> str:
    return os.getenv("TELEGRAM_BOT_TOKEN", "").strip()


def _build_telegram_deep_link(link_token: str) -> str | None:
    bot_username = _get_telegram_bot_username()
    if not bot_username:
        return None
    return f"https://t.me/{bot_username}?start=tglink_{link_token}"


def _format_ts(ts: int | None) -> str:
    if ts is None:
        return "—"
    return time.strftime("%d.%m.%Y %H:%M", time.localtime(int(ts)))


def _reply_in_telegram(chat_id: str | int | None, text: str) -> None:
    bot_token = _get_telegram_bot_token()
    if not bot_token or chat_id is None:
        return
    try:
        TelegramClient(bot_token=bot_token, chat_id=str(chat_id)).send_message(text)
    except Exception:
        log.exception("Failed to reply in Telegram chat_id=%s", chat_id)


def _get_lk_data(tenant_id: str) -> tuple[dict, dict, int, dict | None, int, dict | None]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("SELECT * FROM tenants WHERE id = ?"), (tenant_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Tenant not found")
        tenant = dict(row)

        cur.execute(aq("SELECT COUNT(*) as cnt FROM mappings WHERE tenant_id = ? AND entity_type = 'product'"), (tenant_id,))
        mappings_count = cur.fetchone()["cnt"]

        cur.execute(aq("SELECT * FROM stock_sync_status WHERE tenant_id = ?"), (tenant_id,))
        stock = cur.fetchone()
        stock_row = dict(stock) if stock else None

        cur.execute(aq("SELECT status, COUNT(*) as cnt FROM event_store WHERE tenant_id = ? GROUP BY status"), (tenant_id,))
        event_counts = {r["status"]: r["cnt"] for r in cur.fetchall()}

        cur.execute(aq("""
            SELECT event_type, created_at, payload_json
            FROM event_store
            WHERE tenant_id = ? AND status = 'DONE'
            ORDER BY created_at DESC LIMIT 1
        """), (tenant_id,))
        last_event_row = cur.fetchone()
        last_event = dict(last_event_row) if last_event_row else None
    finally:
        conn.close()

    # Получаем количество товаров в МойСклад
    ms_products_count = 0
    if tenant.get("moysklad_token"):
        try:
            r = requests.get(
                f"{MS_BASE}/entity/product",
                headers=_ms_headers(tenant["moysklad_token"]),
                params={"limit": 1},
                timeout=10,
            )
            if r.ok:
                ms_products_count = r.json().get("meta", {}).get("size", 0)
        except Exception:
            pass

    return tenant, event_counts, mappings_count, stock_row, ms_products_count, last_event


def _is_valid_email(email: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", (email or "").strip()))


# ---------------------------------------------------------------------------
# LK styles & layout
# ---------------------------------------------------------------------------

LK_STYLE = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Inter', -apple-system, sans-serif; background: #f0f2f7; color: #1a1d2e; min-height: 100vh; }
.lk-header { background: #fff; border-bottom: 1px solid #e4e8f0; padding: 0 32px; position: sticky; top: 0; z-index: 100; }
.lk-header-inner { max-width: 860px; margin: 0 auto; display: flex; align-items: center; justify-content: space-between; height: 56px; }
.lk-logo { font-size: 15px; font-weight: 700; color: #1a1d2e; letter-spacing: -0.3px; }
.lk-logo span { color: #3b6ff5; }
.lk-tenant-name { font-size: 13px; color: #6b7280; font-weight: 500; }
.lk-container { max-width: 860px; margin: 0 auto; padding: 32px 24px; }
.lk-page-title { font-size: 26px; font-weight: 700; letter-spacing: -0.5px; color: #1a1d2e; margin-bottom: 4px; }
.lk-page-subtitle { font-size: 13px; color: #6b7280; margin-bottom: 28px; }
.lk-tabs { display: flex; gap: 2px; background: #e9ecf5; border-radius: 10px; padding: 3px; margin-bottom: 28px; width: fit-content; }
.lk-tab { padding: 7px 18px; border-radius: 8px; font-size: 13px; font-weight: 500; color: #6b7280; text-decoration: none; transition: all 0.15s; white-space: nowrap; }
.lk-tab:hover { color: #1a1d2e; text-decoration: none; }
.lk-tab.active { background: #fff; color: #1a1d2e; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }
.lk-card { background: #fff; border: 1px solid #e4e8f0; border-radius: 12px; padding: 24px; margin-bottom: 16px; }
.lk-card-title { font-size: 13px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.6px; color: #9ca3af; margin-bottom: 16px; }
.lk-row { display: flex; align-items: center; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #f3f4f8; }
.lk-row:last-child { border-bottom: none; }
.lk-row-label { font-size: 14px; color: #6b7280; font-weight: 500; }
.lk-row-value { font-size: 14px; font-weight: 600; color: #1a1d2e; }
.badge-ok { display: inline-flex; align-items: center; gap: 5px; background: #ecfdf5; color: #059669; border-radius: 6px; padding: 3px 10px; font-size: 12px; font-weight: 600; }
.badge-ok::before { content: "●"; font-size: 8px; }
.badge-err { display: inline-flex; align-items: center; gap: 5px; background: #fef2f2; color: #dc2626; border-radius: 6px; padding: 3px 10px; font-size: 12px; font-weight: 600; }
.badge-err::before { content: "●"; font-size: 8px; }
.badge-warn { display: inline-flex; align-items: center; gap: 5px; background: #fffbeb; color: #d97706; border-radius: 6px; padding: 3px 10px; font-size: 12px; font-weight: 600; }
.badge-warn::before { content: "●"; font-size: 8px; }
.badge-neutral { display: inline-flex; align-items: center; gap: 5px; background: #f3f4f8; color: #6b7280; border-radius: 6px; padding: 3px 10px; font-size: 12px; font-weight: 600; }
.stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
.stat-card { background: #f8faff; border: 1px solid #e4e8f0; border-radius: 10px; padding: 16px; text-align: center; }
.stat-value { font-size: 28px; font-weight: 700; letter-spacing: -1px; margin-bottom: 4px; }
.stat-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: #9ca3af; }
.btn { display: inline-flex; align-items: center; gap: 8px; padding: 10px 20px; border-radius: 8px; font-size: 14px; font-weight: 600; cursor: pointer; text-decoration: none; border: none; transition: all 0.15s; font-family: inherit; }
.btn-primary { background: #3b6ff5; color: #fff; }
.btn-primary:hover { background: #2d5de0; text-decoration: none; color: #fff; }
.btn-outline { background: #fff; color: #3b6ff5; border: 1.5px solid #3b6ff5; }
.btn-outline:hover { background: #f0f5ff; text-decoration: none; color: #3b6ff5; }
.btn-ghost { background: #f3f4f8; color: #1a1d2e; border: 1px solid #e4e8f0; width: 100%; justify-content: space-between; }
.btn-ghost:hover { background: #eaecf5; text-decoration: none; color: #1a1d2e; }
.btn-ghost::after { content: "→"; }
.actions-list { display: flex; flex-direction: column; gap: 10px; }
.alert-box { border-radius: 10px; padding: 14px 18px; font-size: 14px; margin-bottom: 16px; }
.alert-success { background: #ecfdf5; border: 1px solid #a7f3d0; color: #065f46; }
.alert-error { background: #fef2f2; border: 1px solid #fecaca; color: #991b1b; }
.alert-warning { background: #fffbeb; border: 1px solid #fde68a; color: #92400e; }
.form-field { display: flex; flex-direction: column; gap: 6px; margin-bottom: 16px; }
.form-label { font-size: 13px; font-weight: 600; color: #374151; }
.form-input { padding: 10px 14px; border: 1.5px solid #e4e8f0; border-radius: 8px; font-size: 14px; font-family: inherit; color: #1a1d2e; transition: border-color 0.15s; width: 100%; }
.form-input:focus { outline: none; border-color: #3b6ff5; box-shadow: 0 0 0 3px rgba(59,111,245,0.1); }
.form-hint { font-size: 12px; color: #9ca3af; }
.deep-link-box { background: #f8faff; border: 1px solid #dbeafe; border-radius: 10px; padding: 16px; margin-top: 14px; }
.deep-link-label { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: #6b7280; margin-bottom: 8px; }
.deep-link-url { font-size: 13px; color: #3b6ff5; word-break: break-all; }
.deep-link-exp { font-size: 12px; color: #9ca3af; margin-top: 6px; }
code { background: #f3f4f8; padding: 2px 7px; border-radius: 5px; font-size: 12px; font-family: 'Courier New', monospace; color: #4b5563; }
.onboarding-card { background: #fff; border: 1px solid #d8deea; border-radius: 8px; padding: 24px; max-width: 640px; }
.store { border: 1px solid #d8deea; border-radius: 8px; padding: 16px; margin-bottom: 12px; background: #fafcff; }
.field { display: flex; flex-direction: column; gap: 6px; margin-bottom: 16px; }
label { font-size: 13px; font-weight: 600; color: #3a4255; }
input[type=text], input[type=email], input[type=password], select { padding: 10px 12px; border: 1px solid #cfd7e6; border-radius: 6px; font-size: 14px; background: #fff; width: 100%; font-family: inherit; }
input[type=checkbox] { width: auto; }
.checkbox { display: flex; align-items: center; gap: 10px; margin-bottom: 12px; }
.hint { font-size: 12px; color: #8793a8; }
.ob-btn { background: #2458d3; color: #fff; border: none; border-radius: 6px; padding: 10px 20px; cursor: pointer; font-size: 14px; font-weight: 600; font-family: inherit; }
.ob-btn:hover { background: #1a44b0; }
.back-btn { display: inline-flex; align-items: center; gap: 6px; margin-bottom: 16px; padding: 6px 12px; background: #f5f7fb; border: 1px solid #d8deea; border-radius: 6px; color: #5b6475; font-size: 13px; text-decoration: none; }
.back-btn:hover { background: #eef3fb; color: #2458d3; text-decoration: none; }
.ob-success { background: #eef8f0; border: 1px solid #b8dfc1; color: #214d2d; padding: 12px 14px; border-radius: 6px; margin-bottom: 16px; }
.ob-error { background: #fff1f0; border: 1px solid #f0b7b3; color: #7a1f17; padding: 12px 14px; border-radius: 6px; margin-bottom: 16px; }
.section-title { font-size: 13px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; color: #8793a8; margin: 20px 0 12px; }
.sync-result { background: #f5f7fb; border: 1px solid #d8deea; border-radius: 6px; padding: 12px 14px; margin-top: 16px; font-size: 13px; }
.sync-result table { width: 100%; border-collapse: collapse; margin-top: 8px; }
.sync-result td { padding: 4px 8px; color: #3a4255; }
.sync-result td:first-child { color: #8793a8; width: 120px; }
hr { border: none; border-top: 1px solid #e8edf5; margin: 20px 0; }
a { color: #2458d3; }
a:hover { text-decoration: underline; }
</style>
"""


def _lk_layout(tenant: dict, active_tab: str, content: str,
                info_message: str | None = None, error_message: str | None = None) -> HTMLResponse:
    tenant_id = tenant["id"]
    name = html.escape(tenant.get("name", ""))
    tabs = [
        ("overview",      f"/onboarding/tenants/{tenant_id}",               "Обзор"),
        ("integration",   f"/onboarding/tenants/{tenant_id}/integration",   "Интеграция"),
        ("notifications", f"/onboarding/tenants/{tenant_id}/notifications", "Уведомления"),
        ("actions",       f"/onboarding/tenants/{tenant_id}/actions",       "Действия"),
    ]
    tabs_html = "\n".join(
        f'<a href="{url}" class="lk-tab {"active" if key == active_tab else ""}">{label}</a>'
        for key, url, label in tabs
    )
    alerts = ""
    if info_message:
        alerts += f'<div class="alert-box alert-success">{html.escape(info_message)}</div>'
    if error_message:
        alerts += f'<div class="alert-box alert-error">{html.escape(error_message)}</div>'
    page = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Личный кабинет — {name}</title>
    {LK_STYLE}
</head>
<body>
    <header class="lk-header">
        <div class="lk-header-inner">
            <div class="lk-logo">Evotor <span>↔</span> MoySklad</div>
            <div class="lk-tenant-name">{name}</div>
        </div>
    </header>
    <div class="lk-container">
        <div class="lk-page-title">Личный кабинет</div>
        <div class="lk-page-subtitle"></code></div>
        <div class="lk-tabs">{tabs_html}</div>
        {alerts}
        {content}
    </div>
</body>
</html>"""
    return HTMLResponse(page)


def _badge(ok: bool, ok_text: str, fail_text: str) -> str:
    cls = "badge-ok" if ok else "badge-err"
    return f'<span class="{cls}">{ok_text if ok else fail_text}</span>'


def _ob_layout(title: str, body: str, back_url: str | None = None) -> str:
    back_btn = f'<a href="{html.escape(back_url)}" class="back-btn">← Назад</a>' if back_url else ""
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>{html.escape(title)}</title>
    {LK_STYLE}
</head>
<body style="background:#f5f7fb;padding:24px;">
    <div style="margin-bottom:8px;font-size:15px;font-weight:700;">Evotor ↔ MoySklad</div>
    <div style="color:#5b6475;margin-bottom:24px;font-size:14px;">Создание профиля магазина</div>
    <div class="onboarding-card">
        {back_btn}
        <h2 style="font-size:20px;font-weight:700;margin-bottom:20px;">{html.escape(title)}</h2>
        {body}
    </div>
</body>
</html>"""


def _select(name: str, items: list[dict]) -> str:
    options = "\n".join(
        f'<option value="{html.escape(item["id"])}">{html.escape(item["name"])}</option>'
        for item in items
    )
    return f'<select name="{html.escape(name)}" required>\n{options}\n</select>'


# ---------------------------------------------------------------------------
# Auto initial sync
# ---------------------------------------------------------------------------

def _run_initial_sync(tenant_id: str) -> dict:
    from app.api.sync import initial_sync
    try:
        return initial_sync(tenant_id)
    except Exception as e:
        log.exception("Auto initial sync failed tenant_id=%s", tenant_id)
        return {"status": "error", "error": str(e), "synced": 0, "failed": 0, "skipped": 0}


def _render_sync_result(result: dict) -> str:
    status = result.get("status", "error")
    synced = result.get("synced", 0)
    failed = result.get("failed", 0)
    skipped = result.get("skipped", 0)
    if status == "error":
        return f'<div class="ob-error"><strong>Синхронизация не выполнена.</strong><br>{html.escape(str(result.get("error", "")))}</div>'
    cls = "ob-success" if status == "ok" else "ob-error"
    errors_html = ""
    if result.get("errors"):
        items = "".join(f'<li>{html.escape(str(e))}</li>' for e in result["errors"][:5])
        errors_html = f'<ul style="margin:8px 0 0;padding-left:18px;font-size:12px;">{items}</ul>'
    return f"""
    <div class="{cls}">
        <strong>{"Синхронизация выполнена успешно" if status == "ok" else "Выполнена частично"}</strong>
        <div class="sync-result">
            <table>
                <tr><td>Синхронизировано</td><td><strong>{synced}</strong> товаров</td></tr>
                <tr><td>Пропущено</td><td>{skipped}</td></tr>
                <tr><td>Ошибок</td><td>{failed}</td></tr>
            </table>
            {errors_html}
        </div>
    </div>"""


# ---------------------------------------------------------------------------
# Step 1 — Evotor token (ручной онбординг)
# ---------------------------------------------------------------------------

@router.get("/onboarding/evotor/connect", response_class=HTMLResponse)
def onboarding_token_form():
    body = """
    <p style="color:#5b6475;font-size:14px;margin-bottom:20px;line-height:1.6;">
        Введите токен из личного кабинета Эвотор. Система автоматически загрузит список ваших магазинов.
    </p>
    <form method="post" action="/onboarding/evotor/connect">
        <div class="field">
            <label>Evotor token</label>
            <input type="text" name="evotor_token" required placeholder="Вставьте токен из личного кабинета Эвотор" />
        </div>
        <div style="margin-bottom:20px;">
            <a href="/static/help/evotor-token.html" target="_blank"
               style="display:inline-flex;align-items:center;gap:6px;font-size:13px;color:#2458d3;text-decoration:none;">
                <span style="font-size:16px;">📖</span>
                Где найти токен Эвотор? Пошаговая инструкция
            </a>
        </div>
        <button type="submit" class="ob-btn">Получить мои магазины →</button>
    </form>"""
    return HTMLResponse(_ob_layout("Подключение Эвотор", body))


@router.post("/onboarding/evotor/connect", response_class=HTMLResponse)
def onboarding_token_submit(evotor_token: str = Form(...)):
    evotor_token = evotor_token.strip()
    if not evotor_token:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Evotor token обязателен.</div>'), status_code=400)
    try:
        stores = fetch_stores_by_token(evotor_token)
    except Exception as exc:
        log.exception("Failed to fetch stores")
        return HTMLResponse(_ob_layout("Ошибка", f'<div class="ob-error">Не удалось получить магазины: {html.escape(str(exc))}</div>'), status_code=502)
    if not stores:
        return HTMLResponse(_ob_layout("Магазины не найдены", '<div class="ob-error">По этому token не найдено магазинов.</div>'), status_code=400)
    session_id = str(uuid.uuid4())
    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("INSERT INTO evotor_onboarding_sessions (id, evotor_token, stores_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)"),
            (session_id, evotor_token, json.dumps(stores, ensure_ascii=False), now, now),
        )
        conn.commit()
    finally:
        conn.close()
    link = f"/onboarding/evotor/sessions/{session_id}/stores"
    body = f'<div class="ob-success">Магазины получены — {len(stores)} шт.</div><p><a href="{html.escape(link)}">Перейти к выбору магазина →</a></p>'
    return HTMLResponse(_ob_layout("Подключение Эвотор", body))


# ---------------------------------------------------------------------------
# Step 2
# ---------------------------------------------------------------------------

@router.get("/onboarding/evotor/sessions/{session_id}/stores", response_class=HTMLResponse)
def onboarding_evotor_stores(session_id: str):
    session = _load_session(session_id)
    stores = json.loads(session["stores_json"] or "[]")
    if not stores:
        return HTMLResponse(_ob_layout("Выбор магазина", '<div class="ob-error">Магазины не найдены.</div>'), status_code=400)
    parts = []
    for store in stores:
        store_id = _extract_store_id(store)
        if not store_id:
            continue
        store_name = _extract_store_name(store, store_id)
        link = f"/onboarding/evotor/sessions/{html.escape(session_id)}/stores/{html.escape(store_id)}/ms-token"
        parts.append(f"""
        <div class="store">
            <p><strong>{html.escape(store_name)}</strong></p>
            <p style="margin:4px 0 12px;color:#5b6475;font-size:13px;">ID: <code>{html.escape(store_id)}</code></p>
            <a href="{link}">Создать профиль →</a>
        </div>""")
    return HTMLResponse(_ob_layout("Выбор магазина", "".join(parts), back_url="/onboarding/evotor/connect"))


# ---------------------------------------------------------------------------
# Step 3
# ---------------------------------------------------------------------------

@router.get("/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token", response_class=HTMLResponse)
def onboarding_ms_token_form(session_id: str, store_id: str):
    body = f"""
    <p style="margin-bottom:20px;color:#5b6475;font-size:14px;">Магазин: <code>{html.escape(store_id)}</code></p>
    <form method="post" action="/onboarding/evotor/sessions/{html.escape(session_id)}/stores/{html.escape(store_id)}/ms-token">
        <div class="field">
            <label>MoySklad token</label>
            <input type="text" name="moysklad_token" required placeholder="Токен из раздела «Безопасность» → «Токены»" />
            <span class="hint">Система загрузит организации, склады и контрагентов автоматически.</span>
        </div>
        <button type="submit" class="ob-btn">Загрузить данные →</button>
    </form>"""
    return HTMLResponse(_ob_layout("Подключение МойСклад", body, back_url=f"/onboarding/evotor/sessions/{session_id}/stores"))


@router.post("/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token", response_class=HTMLResponse)
def onboarding_ms_token_submit(session_id: str, store_id: str, moysklad_token: str = Form(...)):
    moysklad_token = moysklad_token.strip()
    if not moysklad_token:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Токен обязателен.</div>'), status_code=400)
    try:
        orgs, ms_stores, agents = _ms_fetch_all(moysklad_token)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        msg = "Неверный токен." if status == 401 else f"Ошибка API: {status}"
        return HTMLResponse(_ob_layout("Ошибка", f'<div class="ob-error">{html.escape(msg)}</div>',
            back_url=f"/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token"), status_code=502)
    except Exception as exc:
        log.exception("Failed to fetch MoySklad data")
        return HTMLResponse(_ob_layout("Ошибка", f'<div class="ob-error">{html.escape(str(exc))}</div>'), status_code=502)
    for check, msg in [(orgs, "организаций"), (ms_stores, "складов"), (agents, "контрагентов")]:
        if not check:
            return HTMLResponse(_ob_layout("Ошибка", f'<div class="ob-error">Не найдено {msg}.</div>'), status_code=400)
    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("UPDATE evotor_onboarding_sessions SET moysklad_token=?, ms_data_json=?, updated_at=? WHERE id=?"),
            (moysklad_token, json.dumps({"orgs": orgs, "stores": ms_stores, "agents": agents}, ensure_ascii=False), now, session_id),
        )
        conn.commit()
    finally:
        conn.close()
    body = f"""
    <div class="ob-success">Данные МойСклад загружены.</div>
    <form method="post" action="/onboarding/store-profile">
        <input type="hidden" name="session_id" value="{html.escape(session_id)}" />
        <input type="hidden" name="evotor_store_id" value="{html.escape(store_id)}" />
        <div class="field"><label>Имя профиля</label><input type="text" name="name" required placeholder="Мой магазин на Ленина" /></div>
        <div class="section-title">МойСклад</div>
        <div class="field"><label>Организация</label>{_select("ms_organization_id", orgs)}</div>
        <div class="field"><label>Склад</label>{_select("ms_store_id", ms_stores)}</div>
        <div class="field"><label>Контрагент по умолчанию</label>{_select("ms_agent_id", agents)}</div>
        <div class="section-title">Уведомления (необязательно)</div>
        <div class="field"><label>Email</label><input type="email" name="alert_email" placeholder="owner@example.com" /></div>
        <div class="checkbox">
            <input id="alerts_email_enabled" type="checkbox" name="alerts_email_enabled" checked />
            <label for="alerts_email_enabled">Включить email-уведомления</label>
        </div>
        <div class="section-title">Фискализация (необязательно)</div>
        <div class="field"><label>Fiscal token</label><input type="text" name="fiscal_token" placeholder="Оставьте пустым если не нужна" /></div>
        <div class="field"><label>Fiscal client UID</label><input type="text" name="fiscal_client_uid" /></div>
        <div class="field"><label>Fiscal device UID</label><input type="text" name="fiscal_device_uid" /></div>
        <button type="submit" class="ob-btn">Создать профиль →</button>
    </form>"""
    return HTMLResponse(_ob_layout("Настройка профиля", body,
        back_url=f"/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token"))


# ---------------------------------------------------------------------------
# Step 4
# ---------------------------------------------------------------------------

@router.post("/onboarding/store-profile", response_class=HTMLResponse)
def onboarding_store_profile_submit(
    session_id: str = Form(...),
    evotor_store_id: str = Form(...),
    name: str = Form(...),
    ms_organization_id: str = Form(...),
    ms_store_id: str = Form(...),
    ms_agent_id: str = Form(...),
    alert_email: str = Form(""),
    alerts_email_enabled: bool = Form(False),
    fiscal_token: str = Form(""),
    fiscal_client_uid: str = Form(""),
    fiscal_device_uid: str = Form(""),
):
    session = _load_session(session_id)
    moysklad_token = session.get("moysklad_token", "").strip()
    if not moysklad_token:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Сессия истекла. Начните заново.</div>'), status_code=400)
    ms_data = json.loads(session.get("ms_data_json") or "{}")
    if ms_organization_id not in {i["id"] for i in ms_data.get("orgs", [])}:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Неверная организация.</div>'), status_code=400)
    if ms_store_id not in {i["id"] for i in ms_data.get("stores", [])}:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Неверный склад.</div>'), status_code=400)
    if ms_agent_id not in {i["id"] for i in ms_data.get("agents", [])}:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Неверный контрагент.</div>'), status_code=400)
    alert_email_value = alert_email.strip() or None
    alerts_email_enabled_value = 1 if alerts_email_enabled and alert_email_value else 0
    tenant_id = str(uuid.uuid4())
    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("SELECT id FROM tenants WHERE evotor_store_id = ?"), (evotor_store_id,))
        existing = cur.fetchone()
        if existing:
            body = (
                f'<div class="ob-error">Профиль уже существует: <code>{html.escape(existing["id"])}</code></div>'
                f'<p style="margin-top:16px;"><a href="/onboarding/evotor/connect">← Начать сначала</a></p>'
            )
            return HTMLResponse(_ob_layout("Профиль уже существует", body), status_code=409)
        cur.execute(
            aq("""INSERT INTO tenants (id, name, evotor_api_key, moysklad_token, created_at,
                evotor_token, evotor_store_id, ms_organization_id, ms_store_id, ms_agent_id,
                alert_email, alerts_email_enabled, telegram_chat_id, alerts_telegram_enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""),
            (tenant_id, name.strip(), "", moysklad_token, now,
             session["evotor_token"], evotor_store_id.strip(),
             ms_organization_id.strip(), ms_store_id.strip(), ms_agent_id.strip(),
             alert_email_value, alerts_email_enabled_value, None, 0),
        )
        if fiscal_token.strip() and fiscal_client_uid.strip() and fiscal_device_uid.strip():
            cur.execute(
                aq("UPDATE tenants SET fiscal_token=?, fiscal_client_uid=?, fiscal_device_uid=? WHERE id=?"),
                (fiscal_token.strip(), fiscal_client_uid.strip(), fiscal_device_uid.strip(), tenant_id),
            )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.exception("Failed to create store profile")
        return HTMLResponse(_ob_layout("Ошибка", f'<div class="ob-error">{html.escape(str(exc))}</div>'), status_code=500)
    finally:
        conn.close()
    log.info("Starting auto initial sync for tenant_id=%s", tenant_id)
    sync_result = _run_initial_sync(tenant_id)
    sync_html = _render_sync_result(sync_result)
    body = f"""
    <div class="ob-success"><strong>Профиль создан!</strong></div>
    <div class="section-title">Первичная синхронизация</div>
    {sync_html}
    <hr>
    <a href="/onboarding/tenants/{html.escape(tenant_id)}" class="ob-btn" style="display:inline-block;text-decoration:none;">
        Перейти в личный кабинет →
    </a>
    <p style="color:#8793a8;font-size:12px;margin-top:16px;">Tenant ID: <code>{html.escape(tenant_id)}</code></p>"""
    return HTMLResponse(_ob_layout("Профиль создан", body))


# ---------------------------------------------------------------------------
# Точка входа из iframe МойСклад
# ---------------------------------------------------------------------------

@router.get("/onboarding/ms", response_class=HTMLResponse)
def onboarding_ms_entry(request: Request, contextKey: str | None = None):
    params = dict(request.query_params)
    log.info("onboarding_ms_entry params=%s", params)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("""
            SELECT id, name, evotor_token, evotor_store_id, ms_organization_id, ms_store_id
            FROM tenants
            WHERE ms_status = 'active' OR ms_status IS NULL
            ORDER BY created_at DESC
        """))
        tenants = cur.fetchall()
    finally:
        conn.close()

    if not tenants:
        return HTMLResponse(_ob_layout("Настройка решения", """
            <div class="ob-error">
                <strong>Профиль не найден.</strong><br>
                Попробуйте переустановить решение из каталога МойСклад.
            </div>
        """))

    if len(tenants) == 1:
        tenant = tenants[0]
        evotor_ready = bool(tenant["evotor_token"] and tenant["evotor_store_id"])
        ms_ready = bool(tenant["ms_organization_id"] and tenant["ms_store_id"])
        if not evotor_ready or not ms_ready:
            return RedirectResponse(url=f"/onboarding/tenants/{tenant['id']}/evotor", status_code=302)
        return RedirectResponse(url=f"/onboarding/tenants/{tenant['id']}", status_code=302)

    # Несколько tenant'ов
    items = "".join(
        f"""<div class="store">
            <p><strong>{html.escape(t["name"])}</strong></p>
            <a href="/onboarding/tenants/{html.escape(t['id'])}">Открыть →</a>
        </div>"""
        for t in tenants
    )
    return HTMLResponse(_ob_layout("Выбор магазина", f"""
        <p style="color:#5b6475;margin-bottom:16px;font-size:14px;">Выберите профиль:</p>
        {items}
    """))


# ---------------------------------------------------------------------------
# Подключение Эвотор к существующему tenant'у (из МойСклад)
# ---------------------------------------------------------------------------

@router.get("/onboarding/tenants/{tenant_id}/evotor", response_class=HTMLResponse)
def onboarding_tenant_evotor_form(tenant_id: str):
    tenant = _load_tenant(tenant_id)
    evotor_ok = bool(tenant.get("evotor_token") and tenant.get("evotor_store_id"))
    ms_configured = bool(tenant.get("ms_organization_id") and tenant.get("ms_store_id"))

    # Загружаем данные МойСклад если не заполнены
    ms_selects_html = ""
    if tenant.get("moysklad_token") and not ms_configured:
        try:
            orgs, ms_stores, agents = _ms_fetch_all(tenant["moysklad_token"])
            ms_selects_html = f"""
            <hr>
            <div class="section-title">Настройки МойСклад</div>
            <div class="field"><label>Организация</label>{_select("ms_organization_id", orgs)}</div>
            <div class="field"><label>Склад</label>{_select("ms_store_id", ms_stores)}</div>
            <div class="field">
                <label>Контрагент по умолчанию</label>
                {_select("ms_agent_id", agents)}
                <span class="hint">Используется если данные клиента в чеке отсутствуют.</span>
            </div>"""
        except Exception as e:
            log.warning("Failed to fetch MS data tenant_id=%s err=%s", tenant_id, e)

    status_html = '<div class="ob-success">Эвотор уже подключён. Можно обновить токен.</div>' if evotor_ok else \
                  '<div class="ob-error" style="margin-bottom:16px;">Подключите кассу Эвотор для завершения настройки.</div>'

    body = f"""
    {status_html}
    <form method="post" action="/onboarding/tenants/{html.escape(tenant_id)}/evotor">
        <div class="field">
            <label>Evotor token</label>
            <input type="text" name="evotor_token" required placeholder="Токен из личного кабинета Эвотор" />
            <span class="hint">Найдите в evotor.ru → Настройки → API</span>
        </div>
        {ms_selects_html}
        <hr>
        <div class="section-title">Фискализация (необязательно)</div>
        <div class="field"><label>Fiscal token</label><input type="text" name="fiscal_token" placeholder="Оставьте пустым если не нужна" /></div>
        <div class="field"><label>Fiscal client UID</label><input type="text" name="fiscal_client_uid" /></div>
        <div class="field"><label>Fiscal device UID</label><input type="text" name="fiscal_device_uid" /></div>
        <button type="submit" class="ob-btn" style="margin-top:8px;">Подключить →</button>
    </form>"""

    return HTMLResponse(_ob_layout("Подключение Эвотор", body, back_url=f"/onboarding/tenants/{tenant_id}"))


@router.post("/onboarding/tenants/{tenant_id}/evotor", response_class=HTMLResponse)
def onboarding_tenant_evotor_submit(
    tenant_id: str,
    evotor_token: str = Form(...),
    ms_organization_id: str = Form(""),
    ms_store_id: str = Form(""),
    ms_agent_id: str = Form(""),
    fiscal_token: str = Form(""),
    fiscal_client_uid: str = Form(""),
    fiscal_device_uid: str = Form(""),
):
    tenant = _load_tenant(tenant_id)
    evotor_token = evotor_token.strip()
    if not evotor_token:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Токен обязателен.</div>'))

    try:
        stores = fetch_stores_by_token(evotor_token)
    except Exception as exc:
        log.exception("Failed to fetch Evotor stores")
        return HTMLResponse(_ob_layout("Ошибка",
            f'<div class="ob-error">Не удалось получить магазины: {html.escape(str(exc))}</div>',
            back_url=f"/onboarding/tenants/{tenant_id}/evotor"))

    if not stores:
        return HTMLResponse(_ob_layout("Ошибка",
            '<div class="ob-error">По этому токену не найдено магазинов.</div>',
            back_url=f"/onboarding/tenants/{tenant_id}/evotor"))

    if len(stores) == 1:
        store = stores[0]
        store_id = _extract_store_id(store)
        store_name = _extract_store_name(store, store_id)
        return _save_evotor_and_sync(tenant_id, tenant, evotor_token, store_id, store_name,
                                      ms_organization_id=ms_organization_id,
                                      ms_store_id=ms_store_id,
                                      ms_agent_id=ms_agent_id)

    # Несколько магазинов — выбор
    parts = []
    for store in stores:
        store_id = _extract_store_id(store)
        if not store_id:
            continue
        store_name = _extract_store_name(store, store_id)
        parts.append(f"""
        <div class="store">
            <p><strong>{html.escape(store_name)}</strong></p>
            <p style="margin:4px 0 12px;color:#5b6475;font-size:13px;">ID: <code>{html.escape(store_id)}</code></p>
            <form method="post" action="/onboarding/tenants/{html.escape(tenant_id)}/evotor/store">
                <input type="hidden" name="evotor_token" value="{html.escape(evotor_token)}" />
                <input type="hidden" name="store_id" value="{html.escape(store_id)}" />
                <input type="hidden" name="store_name" value="{html.escape(store_name)}" />
                <input type="hidden" name="ms_organization_id" value="{html.escape(ms_organization_id)}" />
                <input type="hidden" name="ms_store_id" value="{html.escape(ms_store_id)}" />
                <input type="hidden" name="ms_agent_id" value="{html.escape(ms_agent_id)}" />
                <button type="submit" class="ob-btn">Выбрать →</button>
            </form>
        </div>""")

    return HTMLResponse(_ob_layout("Выберите магазин Эвотор", "".join(parts),
        back_url=f"/onboarding/tenants/{tenant_id}/evotor"))


@router.post("/onboarding/tenants/{tenant_id}/evotor/store", response_class=HTMLResponse)
def onboarding_tenant_evotor_store_submit(
    tenant_id: str,
    evotor_token: str = Form(...),
    store_id: str = Form(...),
    store_name: str = Form(...),
    ms_organization_id: str = Form(""),
    ms_store_id: str = Form(""),
    ms_agent_id: str = Form(""),
):
    tenant = _load_tenant(tenant_id)
    return _save_evotor_and_sync(tenant_id, tenant, evotor_token, store_id, store_name,
                                  ms_organization_id=ms_organization_id,
                                  ms_store_id=ms_store_id,
                                  ms_agent_id=ms_agent_id)


def _save_evotor_and_sync(
    tenant_id: str,
    tenant: dict,
    evotor_token: str,
    store_id: str,
    store_name: str,
    ms_organization_id: str = "",
    ms_store_id: str = "",
    ms_agent_id: str = "",
    fiscal_token: str = "",
    fiscal_client_uid: str = "",
    fiscal_device_uid: str = "",
) -> HTMLResponse:
    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("SELECT id FROM tenants WHERE evotor_store_id = ? AND id != ?"), (store_id, tenant_id))
        conflict = cur.fetchone()
        if conflict:
            return HTMLResponse(_ob_layout("Конфликт",
                f'<div class="ob-error">Этот магазин уже подключён к другому профилю: <code>{html.escape(conflict["id"])}</code></div>',
                back_url=f"/onboarding/tenants/{tenant_id}/evotor"))

        update_fields = "evotor_token = ?, evotor_store_id = ?, updated_at = ?"
        update_values: list = [evotor_token, store_id, now]

        if ms_organization_id.strip():
            update_fields += ", ms_organization_id = ?"
            update_values.append(ms_organization_id.strip())
        if ms_store_id.strip():
            update_fields += ", ms_store_id = ?"
            update_values.append(ms_store_id.strip())
        if ms_agent_id.strip():
            update_fields += ", ms_agent_id = ?"
            update_values.append(ms_agent_id.strip())
        if fiscal_token.strip() and fiscal_client_uid.strip() and fiscal_device_uid.strip():
            update_fields += ", fiscal_token = ?, fiscal_client_uid = ?, fiscal_device_uid = ?"
            update_values.extend([fiscal_token.strip(), fiscal_client_uid.strip(), fiscal_device_uid.strip()])

        update_values.append(tenant_id)
        cur.execute(aq(f"UPDATE tenants SET {update_fields} WHERE id = ?"), update_values)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.exception("Failed to save evotor token tenant_id=%s", tenant_id)
        return HTMLResponse(_ob_layout("Ошибка",
            f'<div class="ob-error">Не удалось сохранить: {html.escape(str(exc))}</div>'))
    finally:
        conn.close()

    log.info("Evotor connected tenant_id=%s store_id=%s", tenant_id, store_id)
    sync_result = _run_initial_sync(tenant_id)
    sync_html = _render_sync_result(sync_result)

    body = f"""
    <div class="ob-success"><strong>Эвотор успешно подключён!</strong><br>
    Магазин: {html.escape(store_name)}</div>
    <div class="section-title">Первичная синхронизация</div>
    {sync_html}
    <hr>
    <a href="/onboarding/tenants/{html.escape(tenant_id)}"
       class="ob-btn" style="display:inline-block;text-decoration:none;margin-top:8px;">
        Перейти в личный кабинет →
    </a>"""
    return HTMLResponse(_ob_layout("Подключение завершено", body))


# ---------------------------------------------------------------------------
# ЛК — Обзор
# ---------------------------------------------------------------------------

@router.get("/onboarding/tenants/{tenant_id}", response_class=HTMLResponse)
def lk_overview(tenant_id: str):
    tenant, event_counts, mappings_count, stock_row, ms_products_count, last_event = _get_lk_data(tenant_id)
    ms_ok = bool(tenant.get("moysklad_token"))
    evotor_ok = bool(tenant.get("evotor_token"))
    sync_ok = bool(tenant.get("sync_completed_at"))
    tg_ok = bool(tenant.get("telegram_chat_id") and tenant.get("alerts_telegram_enabled"))
    email_ok = bool(tenant.get("alert_email") and tenant.get("alerts_email_enabled"))

    stock_badge = ""
    if stock_row:
        ok = stock_row["status"] == "ok"
        stock_badge = f'<div class="lk-row"><span class="lk-row-label">Синхронизация остатков</span><span class="{"badge-ok" if ok else "badge-err"}">{stock_row["status"].upper()}</span></div>'

    failed = event_counts.get("FAILED", 0)
    retry = event_counts.get("RETRY", 0)
    done = event_counts.get("DONE", 0)
    new_ev = event_counts.get("NEW", 0)

    # Баннер новых товаров
    new_products_banner = ""
    if ms_products_count > mappings_count:
        diff = ms_products_count - mappings_count
        new_products_banner = f'''<div class="lk-row" style="background:#fffbeb;margin:0 -24px;padding:10px 24px;border-radius:0;">
            <span class="lk-row-label" style="color:#92400e;">⚠️ Новых товаров в МойСклад: {diff}</span>
            <a href="/onboarding/tenants/{tenant_id}/actions" class="btn btn-outline" style="padding:5px 12px;font-size:12px;">Синхронизировать →</a>
        </div>'''

    # Последнее событие
    last_event_html = ""
    if last_event:
        try:
            import json as _json
            payload = _json.loads(last_event.get("payload_json") or "{}")
            body = payload.get("body", {})
            total = body.get("sum", 0)
            total_str = f" на {int(total):,} ₽".replace(",", " ") if total else ""
        except Exception:
            total_str = ""
        ts = _format_ts(last_event.get("created_at"))
        last_event_html = f'''<div class="lk-row">
            <span class="lk-row-label">Последняя продажа</span>
            <span class="lk-row-value" style="font-size:13px;color:#059669;">✓ {ts}{total_str}</span>
        </div>'''

    content = f"""
    <div class="lk-card">
        <div class="lk-card-title">Статус</div>
        <div class="lk-row"><span class="lk-row-label">МойСклад</span>{_badge(ms_ok, "Подключён", "Не подключён")}</div>
        <div class="lk-row"><span class="lk-row-label">Эвотор</span>{_badge(evotor_ok, "Подключён", "Не подключён")}</div>
        <div class="lk-row"><span class="lk-row-label">Первичная синхронизация</span>{_badge(sync_ok, f"Выполнена {_format_ts(tenant.get('sync_completed_at'))}", "Не выполнена")}</div>
        <div class="lk-row">
            <span class="lk-row-label">Товаров синхронизировано</span>
            <span class="lk-row-value">{mappings_count}{"/" + str(ms_products_count) if ms_products_count else ""}</span>
        </div>
        {new_products_banner}
        {stock_badge}
        {last_event_html}
        <div class="lk-row"><span class="lk-row-label">Telegram</span>{_badge(tg_ok, "Подключён", "Не подключён")}</div>
        <div class="lk-row"><span class="lk-row-label">Email</span>{_badge(email_ok, html.escape(tenant.get("alert_email") or "Активен"), "Не настроен")}</div>
    </div>
    <div class="lk-card">
        <div class="lk-card-title">События</div>
        <div class="stats-grid">
            <div class="stat-card"><div class="stat-value" style="color:#059669;">{done}</div><div class="stat-label">Обработано</div></div>
            <div class="stat-card"><div class="stat-value">{new_ev}</div><div class="stat-label">В очереди</div></div>
            <div class="stat-card"><div class="stat-value" style="color:{'#d97706' if retry > 0 else '#1a1d2e'};">{retry}</div><div class="stat-label">Повтор</div></div>
            <div class="stat-card"><div class="stat-value" style="color:{'#dc2626' if failed > 0 else '#1a1d2e'};">{failed}</div><div class="stat-label">Ошибки</div></div>
        </div>
    </div>"""

    return _lk_layout(tenant, "overview", content)


# ---------------------------------------------------------------------------
# ЛК — Интеграция
# ---------------------------------------------------------------------------

@router.get("/onboarding/tenants/{tenant_id}/integration", response_class=HTMLResponse)
def lk_integration(tenant_id: str):
    tenant = _load_tenant(tenant_id)
    ms_ok = bool(tenant.get("moysklad_token"))
    evotor_ok = bool(tenant.get("evotor_token"))
    sync_ok = bool(tenant.get("sync_completed_at"))
    content = f"""
    <div class="lk-card">
        <div class="lk-card-title">Эвотор</div>
        <div class="lk-row"><span class="lk-row-label">Токен</span>{_badge(evotor_ok, "Активен", "Не задан")}</div>
        <div style="margin-top:16px;">
            <a href="/onboarding/tenants/{html.escape(tenant_id)}/evotor" class="btn btn-outline">Обновить токен Эвотор</a>
        </div>
    </div>
    <div class="lk-card">
        <div class="lk-card-title">Синхронизация</div>
        <div class="lk-row"><span class="lk-row-label">Первичная синхронизация</span>{_badge(sync_ok, f"Выполнена {_format_ts(tenant.get('sync_completed_at'))}", "Не выполнена")}</div>
    </div>"""
    return _lk_layout(tenant, "integration", content)


# ---------------------------------------------------------------------------
# ЛК — Уведомления
# ---------------------------------------------------------------------------

@router.get("/onboarding/tenants/{tenant_id}/notifications", response_class=HTMLResponse)
def lk_notifications(
    tenant_id: str,
    edit_email: int = 0,
    email_status: str | None = None,
    email_error: str | None = None,
):
    tenant = _load_tenant(tenant_id)
    conn = get_connection()
    try:
        token_row = get_active_telegram_link_token(conn, tenant_id)
    finally:
        conn.close()
    tg_ok = bool(tenant.get("telegram_chat_id") and tenant.get("alerts_telegram_enabled"))
    deep_link = _build_telegram_deep_link(token_row["link_token"]) if token_row else None
    deep_link_html = ""
    if deep_link:
        deep_link_label = "Ссылка для переподключения" if tg_ok else "Ссылка для подключения"
        deep_link_html = f"""
        <div class="deep-link-box">
            <div class="deep-link-label">{html.escape(deep_link_label)}</div>
            <div class="deep-link-url"><a href="{html.escape(deep_link)}" target="_blank">{html.escape(deep_link)}</a></div>
            <div class="deep-link-exp">Действует до {_format_ts(token_row["expires_at"])}</div>
        </div>"""
    tg_label = "Переподключить Telegram" if tg_ok else "Подключить Telegram"
    email_value = (tenant.get("alert_email") or "").strip()
    email_enabled = bool(tenant.get("alerts_email_enabled"))
    if email_value and email_enabled:
        email_badge = '<span class="badge-ok">Активен</span>'
    elif email_value and not email_enabled:
        email_badge = '<span class="badge-warn">Выключен</span>'
    else:
        email_badge = '<span class="badge-err">Не настроен</span>'
    email_action_label = "Заменить Email" if email_value else "Указать Email"
    if edit_email:
        checked_attr = "checked" if email_enabled or not email_value else ""
        email_form_html = f"""
        <div style="margin-top:16px;">
            <form method="post" action="/onboarding/tenants/{html.escape(tenant_id)}/notifications/email">
                <div class="form-field">
                    <label class="form-label">Email для уведомлений</label>
                    <input class="form-input" type="email" name="alert_email" value="{html.escape(email_value)}" required placeholder="owner@example.com" />
                    <span class="form-hint">На этот адрес будут приходить уведомления об ошибках.</span>
                </div>
                <div class="checkbox">
                    <input id="alerts_email_enabled_lk" type="checkbox" name="alerts_email_enabled" {checked_attr} />
                    <label for="alerts_email_enabled_lk">Включить email-уведомления</label>
                </div>
                <div style="display:flex;gap:10px;margin-top:14px;flex-wrap:wrap;">
                    <button type="submit" class="btn btn-primary">Сохранить</button>
                    <a href="/onboarding/tenants/{html.escape(tenant_id)}/notifications" class="btn btn-outline">Отмена</a>
                </div>
            </form>
        </div>"""
    else:
        email_form_html = f"""
        <div style="margin-top:16px;">
            <a href="/onboarding/tenants/{html.escape(tenant_id)}/notifications?edit_email=1" class="btn btn-outline">{email_action_label}</a>
        </div>"""
    content = f"""
    <div class="lk-card">
        <div class="lk-card-title">Telegram</div>
        <div class="lk-row"><span class="lk-row-label">Статус</span>{_badge(tg_ok, "Подключён", "Не подключён")}</div>
        <div style="margin-top:16px;">
            <form method="post" action="/onboarding/tenants/{html.escape(tenant_id)}/telegram/link">
                <button type="submit" class="btn btn-outline">{tg_label}</button>
            </form>
        </div>
        {deep_link_html}
    </div>
    <div class="lk-card">
        <div class="lk-card-title">Email</div>
        <div class="lk-row"><span class="lk-row-label">Адрес</span><span class="lk-row-value">{html.escape(email_value or "—")}</span></div>
        <div class="lk-row"><span class="lk-row-label">Статус</span>{email_badge}</div>
        {email_form_html}
    </div>"""
    info_message = "Email для уведомлений обновлён." if email_status == "updated" else None
    return _lk_layout(tenant, "notifications", content, info_message=info_message, error_message=email_error)


@router.post("/onboarding/tenants/{tenant_id}/notifications/email", response_class=HTMLResponse)
def onboarding_tenant_email_update(
    tenant_id: str,
    alert_email: str = Form(...),
    alerts_email_enabled: bool = Form(False),
):
    _load_tenant(tenant_id)
    alert_email = alert_email.strip()
    if not alert_email:
        return lk_notifications(tenant_id, edit_email=1, email_error="Укажите email.")
    if not _is_valid_email(alert_email):
        return lk_notifications(tenant_id, edit_email=1, email_error="Укажите корректный email.")
    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("UPDATE tenants SET alert_email=?, alerts_email_enabled=?, updated_at=? WHERE id=?"),
            (alert_email, 1 if alerts_email_enabled else 0, now, tenant_id),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        return lk_notifications(tenant_id, edit_email=1, email_error=f"Не удалось сохранить: {e}")
    finally:
        conn.close()
    return RedirectResponse(url=f"/onboarding/tenants/{tenant_id}/notifications?email_status=updated", status_code=303)


# ---------------------------------------------------------------------------
# ЛК — Действия
# ---------------------------------------------------------------------------

@router.get("/onboarding/tenants/{tenant_id}/actions", response_class=HTMLResponse)
def lk_actions(tenant_id: str):
    tenant = _load_tenant(tenant_id)
    tid = html.escape(tenant_id)

    content = f"""
    <style>
    .tooltip-wrap {{ position: relative; display: inline-block; }}
    .tooltip-btn {{
        background: none; border: none; cursor: pointer;
        color: #9ca3af; font-size: 15px; padding: 0 4px;
        vertical-align: middle; line-height: 1;
        transition: color 0.15s;
    }}
    .tooltip-btn:hover {{ color: #3b6ff5; }}
    .tooltip-popup {{
        display: none;
        position: absolute;
        bottom: calc(100% + 8px);
        left: 50%;
        transform: translateX(-50%);
        background: #1a1d2e;
        color: #fff;
        font-size: 13px;
        line-height: 1.5;
        padding: 10px 14px;
        border-radius: 8px;
        width: 260px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.2);
        z-index: 100;
        pointer-events: none;
    }}
    .tooltip-popup::after {{
        content: '';
        position: absolute;
        top: 100%;
        left: 50%;
        transform: translateX(-50%);
        border: 6px solid transparent;
        border-top-color: #1a1d2e;
    }}
    .tooltip-wrap:hover .tooltip-popup {{ display: block; }}
    .action-row {{
        display: flex;
        align-items: center;
        gap: 8px;
    }}
    .action-row form {{ flex: 1; }}
    .action-row .btn-ghost {{ width: 100%; }}
    
    .btn-ghost:disabled {{
        opacity: 0.6;
        cursor: not-allowed;
        pointer-events: none;
    }}
    .btn-ghost.loading::after {{
        content: ' ⏳';
    }}
    .sync-overlay {{
        display: none;
        position: fixed;
        inset: 0;
        background: rgba(255,255,255,0.8);
        backdrop-filter: blur(2px);
        z-index: 999;
        align-items: center;
        justify-content: center;
        flex-direction: column;
        gap: 16px;
    }}
    .sync-overlay.visible {{ display: flex; }}
    .sync-spinner {{
        width: 48px; height: 48px;
        border: 4px solid #e4e8f0;
        border-top-color: #3b6ff5;
        border-radius: 50%;
        animation: spin 0.8s linear infinite;
    }}
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
    .sync-label {{
        font-size: 15px;
        font-weight: 600;
        color: #1a1d2e;
    }}
    .sync-sublabel {{
        font-size: 13px;
        color: #6b7280;
    }}
    </style>

    <div class="lk-card">
        <div class="lk-card-title">Синхронизация</div>
        <div class="actions-list">

            <div class="action-row">
                <form method="post" action="/onboarding/tenants/{tid}/sync">
                    <button type="submit" class="btn btn-ghost">Повторная синхронизация товаров</button>
                </form>
                <div class="tooltip-wrap">
                    <button class="tooltip-btn">?</button>
                    <div class="tooltip-popup">
                        Пересоздаёт маппинг всех товаров из Эвотор в МойСклад.<br><br>
                        Используйте если товары пропали или данные расходятся между системами.
                    </div>
                </div>
            </div>

            <div class="action-row">
                <form method="post" action="/onboarding/tenants/{tid}/reconcile">
                    <button type="submit" class="btn btn-ghost">Синхронизировать остатки</button>
                </form>
                <div class="tooltip-wrap">
                    <button class="tooltip-btn">?</button>
                    <div class="tooltip-popup">
                        Обновляет количество товаров в Эвотор на основе текущих остатков в МойСклад.<br><br>
                        Используйте если остатки на кассе не совпадают с МойСклад.
                    </div>
                </div>
            </div>

            <div class="action-row">
                <form method="post" action="/onboarding/tenants/{tid}/sync-ms-to-evotor">
                    <button type="submit" class="btn btn-ghost">Синхронизировать новые товары из МойСклад</button>
                </form>
                <div class="tooltip-wrap">
                    <button class="tooltip-btn">?</button>
                    <div class="tooltip-popup">
                        Создаёт в Эвотор товары, которые были добавлены в МойСклад после первичной синхронизации.<br><br>
                        Используйте когда добавили новые товары в МойСклад и хотите чтобы они появились на кассе.
                    </div>
                </div>
            </div>

        </div>
    </div>
    
    <div class="sync-overlay" id="syncOverlay">
        <div class="sync-spinner"></div>
        <div class="sync-label" id="syncLabel">Синхронизация...</div>
        <div class="sync-sublabel">Это может занять до 30 секунд</div>
    </div>

    <script>
    document.querySelectorAll('.action-row form').forEach(function(form) {{
        form.addEventListener('submit', function(e) {{
            var btn = form.querySelector('button');
            var label = btn ? btn.textContent.trim() : 'Синхронизация';
            btn && btn.classList.add('loading');
            btn && (btn.disabled = true);
            document.getElementById('syncLabel').textContent = label + '...';
            document.getElementById('syncOverlay').classList.add('visible');
        }});
    }});
    </script>
    </div>"""

    return _lk_layout(tenant, "actions", content)


# ---------------------------------------------------------------------------
# Telegram linking
# ---------------------------------------------------------------------------

@router.get("/onboarding/tenants/{tenant_id}/telegram", response_class=HTMLResponse)
def lk_telegram_redirect(tenant_id: str):
    return RedirectResponse(url=f"/onboarding/tenants/{tenant_id}/notifications", status_code=301)


@router.post("/onboarding/tenants/{tenant_id}/telegram/link", response_class=HTMLResponse)
def onboarding_tenant_telegram_link(tenant_id: str):
    tenant = _load_tenant(tenant_id)
    if not _get_telegram_bot_username():
        return _lk_layout(tenant, "notifications", "", error_message="TELEGRAM_BOT_USERNAME не настроен.")
    conn = get_connection()
    try:
        create_telegram_link_token(conn, tenant_id=tenant_id, ttl_sec=TELEGRAM_LINK_TOKEN_TTL_SEC)
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url=f"/onboarding/tenants/{tenant_id}/notifications", status_code=303)


@router.post("/webhooks/telegram")
def telegram_link_webhook(update: dict = Body(...)):
    message = update.get("message") or update.get("edited_message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    text = (message.get("text") or "").strip()
    if chat_id is None or not text:
        return {"ok": True}
    link_token = _extract_telegram_link_token_from_text(text)
    if not link_token:
        if text.startswith("/start"):
            _reply_in_telegram(chat_id, "Используйте ссылку из личного кабинета чтобы привязать Telegram.")
        return {"ok": True}
    now = int(time.time())
    conn = get_connection()
    try:
        token_row = get_telegram_link_token_by_value(conn, link_token, now_ts=now)
        if not token_row:
            _reply_in_telegram(chat_id, "Ссылка не найдена или недействительна.")
            conn.commit()
            return {"ok": True}
        if token_row["status"] in ("expired", "linked"):
            msg = "Срок ссылки истёк." if token_row["status"] == "expired" else "Ссылка уже использована."
            _reply_in_telegram(chat_id, msg + " Создайте новую в личном кабинете.")
            conn.commit()
            return {"ok": True}
        if int(token_row["expires_at"]) <= now:
            conn.cursor().execute(aq("UPDATE telegram_link_tokens SET status='expired' WHERE id=?"), (token_row["id"],))
            conn.commit()
            _reply_in_telegram(chat_id, "Срок ссылки истёк. Создайте новую в личном кабинете.")
            return {"ok": True}
        conn.cursor().execute(
            aq("UPDATE tenants SET telegram_chat_id=?, alerts_telegram_enabled=1 WHERE id=?"),
            (str(chat_id), token_row["tenant_id"]),
        )
        mark_telegram_link_token_linked(conn, token_id=token_row["id"], linked_chat_id=str(chat_id), linked_at=now)
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Failed to process Telegram webhook chat_id=%s", chat_id)
        _reply_in_telegram(chat_id, "Не удалось завершить подключение. Попробуйте снова.")
        return {"ok": True}
    finally:
        conn.close()
    _reply_in_telegram(chat_id, "✅ Telegram подключён. Уведомления будут приходить сюда.")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Обновление токена МойСклад
# ---------------------------------------------------------------------------

@router.get("/onboarding/tenants/{tenant_id}/token", response_class=HTMLResponse)
def onboarding_tenant_token_form(tenant_id: str):
    tenant = _load_tenant(tenant_id)
    body = f"""
    <form method="post" action="/onboarding/tenants/{html.escape(tenant_id)}/token">
        <div class="form-field">
            <label class="form-label">Новый токен МойСклад</label>
            <input class="form-input" name="moysklad_token" required placeholder="Токен из «Безопасность» → «Токены»" />
            <span class="form-hint">Система проверит токен перед сохранением.</span>
        </div>
        <button type="submit" class="btn btn-primary">Обновить токен</button>
    </form>"""
    content = f'<div class="lk-card"><div class="lk-card-title">Обновление токена МойСклад</div>{body}</div>'
    return _lk_layout(tenant, "integration", content)


@router.post("/onboarding/tenants/{tenant_id}/token", response_class=HTMLResponse)
def onboarding_tenant_token_submit(tenant_id: str, moysklad_token: str = Form(...)):
    tenant = _load_tenant(tenant_id)
    moysklad_token = moysklad_token.strip()
    if not moysklad_token:
        return _lk_layout(tenant, "integration", "", error_message="Токен обязателен.")
    try:
        orgs, _, _ = _ms_fetch_all(moysklad_token)
        if not orgs:
            raise ValueError("Нет организаций")
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        msg = "Неверный токен." if status == 401 else f"Ошибка API: {status}"
        return _lk_layout(tenant, "integration", "", error_message=msg)
    except Exception as e:
        return _lk_layout(tenant, "integration", "", error_message=str(e))
    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("UPDATE tenants SET moysklad_token=?, updated_at=? WHERE id=?"), (moysklad_token, now, tenant_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return _lk_layout(tenant, "integration", "", error_message=str(e))
    finally:
        conn.close()
    tenant = _load_tenant(tenant_id)
    return _lk_layout(tenant, "integration", "", info_message=f"Токен обновлён. Найдено организаций: {len(orgs)}")


# ---------------------------------------------------------------------------
# Действия — синхронизация
# ---------------------------------------------------------------------------

@router.post("/onboarding/tenants/{tenant_id}/sync", response_class=HTMLResponse)
def onboarding_tenant_sync(tenant_id: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("UPDATE tenants SET sync_completed_at=NULL WHERE id=?"), (tenant_id,))
        conn.commit()
    finally:
        conn.close()
    result = _run_initial_sync(tenant_id)
    tenant = _load_tenant(tenant_id)
    synced = result.get("synced", 0)
    failed = result.get("failed", 0)
    msg = f"Синхронизация завершена: {synced} новых товаров, {failed} ошибок."
    if failed > 0:
        return _lk_layout(tenant, "actions", "", error_message=msg)
    return _lk_layout(tenant, "actions", "", info_message=msg)


@router.post("/onboarding/tenants/{tenant_id}/reconcile", response_class=HTMLResponse)
def onboarding_tenant_reconcile(tenant_id: str):
    from app.clients.evotor_client import EvotorClient
    from app.clients.moysklad_client import MoySkladClient
    tenant = _load_tenant(tenant_id)
    if not tenant.get("sync_completed_at"):
        return _lk_layout(tenant, "actions", "", error_message="Сначала выполните первичную синхронизацию.")
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("SELECT evotor_id, ms_id FROM mappings WHERE tenant_id=? AND entity_type='product'"), (tenant_id,))
        mappings = cur.fetchall()
    finally:
        conn.close()
    if not mappings:
        return _lk_layout(tenant, "actions", "", error_message="Нет маппингов товаров.")
    ms_client = MoySkladClient(tenant_id)
    evotor_client = EvotorClient(tenant_id)
    synced = 0
    failed = 0
    for row in mappings:
        try:
            quantity = ms_client.get_product_stock(row["ms_id"])
            evotor_client.update_product_stock(row["evotor_id"], quantity)
            synced += 1
        except Exception as e:
            log.error("reconcile stock failed ms_id=%s err=%s", row["ms_id"], e)
            failed += 1
    tenant = _load_tenant(tenant_id)
    msg = f"Остатки синхронизированы: {synced} товаров, {failed} ошибок."
    if failed > 0:
        return _lk_layout(tenant, "actions", "", error_message=msg)
    return _lk_layout(tenant, "actions", "", info_message=msg)

@router.post("/onboarding/tenants/{tenant_id}/sync-ms-to-evotor", response_class=HTMLResponse)
def onboarding_tenant_sync_ms_to_evotor(tenant_id: str):
    from app.api.sync import sync_all_ms_products_to_evotor
    try:
        result = sync_all_ms_products_to_evotor(tenant_id)
    except Exception as e:
        tenant = _load_tenant(tenant_id)
        return _lk_layout(tenant, "actions", "", error_message=f"Ошибка: {e}")
    tenant = _load_tenant(tenant_id)
    synced = result.get("synced", 0)
    failed = result.get("failed", 0)
    msg = f"Синхронизация МС→Эвотор: {synced} товаров, {failed} ошибок."
    if failed > 0:
        return _lk_layout(tenant, "actions", "", error_message=msg)
    return _lk_layout(tenant, "actions", "", info_message=msg)