import html
import json
import logging
import os
import time
import uuid

import requests
from fastapi import APIRouter, Body, Form, HTTPException
from fastapi.responses import HTMLResponse

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
    return {
        "Authorization": f"Bearer {token}",
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json",
    }


def _ms_fetch(path: str, token: str, params: dict | None = None) -> dict:
    url = f"{MS_BASE}{path}"
    response = requests.get(url, headers=_ms_headers(token), params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def _ms_fetch_all(token: str) -> tuple[list[dict], list[dict], list[dict]]:
    def extract(data: dict) -> list[dict]:
        return [
            {"id": row["id"], "name": row.get("name") or row.get("description") or row["id"]}
            for row in data.get("rows", [])
            if row.get("id")
        ]
    orgs = extract(_ms_fetch("/entity/organization", token))
    stores = extract(_ms_fetch("/entity/store", token))
    agents = extract(_ms_fetch("/entity/counterparty", token, params={"limit": 100}))
    return orgs, stores, agents


# ---------------------------------------------------------------------------
# HTML layout
# ---------------------------------------------------------------------------

def _layout(title: str, body: str, back_url: str | None = None) -> str:
    back_btn = ""
    if back_url:
        back_btn = f'<a href="{html.escape(back_url)}" class="back-btn">← Назад</a>'

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>{html.escape(title)}</title>
    <style>
        *, *::before, *::after {{ box-sizing: border-box; }}
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 24px;
            background: #f5f7fb;
            color: #172033;
        }}
        h1 {{ margin-bottom: 8px; }}
        .meta {{ color: #5b6475; margin-bottom: 24px; font-size: 14px; }}
        .card {{
            background: #fff;
            border: 1px solid #d8deea;
            border-radius: 8px;
            padding: 24px;
            max-width: 640px;
        }}
        .store {{
            border: 1px solid #d8deea;
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 12px;
            background: #fafcff;
        }}
        .field {{
            display: flex;
            flex-direction: column;
            gap: 6px;
            margin-bottom: 16px;
        }}
        label {{ font-size: 13px; font-weight: 600; color: #3a4255; }}
        input, select {{
            padding: 10px 12px;
            border: 1px solid #cfd7e6;
            border-radius: 6px;
            font-size: 14px;
            background: #fff;
            width: 100%;
        }}
        input[type="checkbox"] {{
            width: auto;
            padding: 0;
            margin: 0;
        }}
        select {{ cursor: pointer; }}
        .checkbox {{
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 12px;
        }}
        .checkbox label {{
            margin: 0;
            font-size: 14px;
            font-weight: 400;
            color: #172033;
            cursor: pointer;
        }}
        .hint {{ font-size: 12px; color: #8793a8; }}
        button {{
            background: #2458d3;
            color: #fff;
            border: none;
            border-radius: 6px;
            padding: 10px 20px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
        }}
        button:hover {{ background: #1a44b0; }}
        .success {{
            background: #eef8f0;
            border: 1px solid #b8dfc1;
            color: #214d2d;
            padding: 12px 14px;
            border-radius: 6px;
            margin-bottom: 16px;
        }}
        .error {{
            background: #fff1f0;
            border: 1px solid #f0b7b3;
            color: #cc0000;
            padding: 12px 14px;
            border-radius: 6px;
            margin-bottom: 16px;
        }}
        .warning {{
            background: #fffbea;
            border: 1px solid #f5d97a;
            color: #7a5c00;
            padding: 12px 14px;
            border-radius: 6px;
            margin-bottom: 16px;
        }}
        .section-title {{
            font-size: 13px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: #8793a8;
            margin: 20px 0 12px;
        }}
        .sync-result {{
            background: #f5f7fb;
            border: 1px solid #d8deea;
            border-radius: 6px;
            padding: 12px 14px;
            margin-top: 16px;
            font-size: 13px;
        }}
        .sync-result table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 8px;
        }}
        .sync-result td {{
            padding: 4px 8px;
            color: #3a4255;
        }}
        .sync-result td:first-child {{
            color: #8793a8;
            width: 120px;
        }}
        code {{
            background: #eef3fb;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 13px;
        }}
        .btn-secondary {{
            display: inline-block;
            margin-top: 8px;
            background: #f5f7fb;
            color: #2458d3;
            border: 1px solid #d8deea;
            border-radius: 6px;
            padding: 10px 20px;
            font-size: 14px;
            font-weight: 600;
            text-decoration: none;
        }}
        .btn-secondary:hover {{
            background: #eef3fb;
            text-decoration: none;
        }}
        .back-btn {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            margin-bottom: 16px;
            padding: 6px 12px;
            background: #f5f7fb;
            border: 1px solid #d8deea;
            border-radius: 6px;
            color: #5b6475;
            font-size: 13px;
            text-decoration: none;
        }}
        .back-btn:hover {{
            background: #eef3fb;
            color: #2458d3;
            text-decoration: none;
        }}
        a {{ color: #2458d3; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        hr {{ border: none; border-top: 1px solid #e8edf5; margin: 20px 0; }}
    </style>
</head>
<body>
    <h1>{html.escape(title)}</h1>
    <div class="meta">Evotor ↔ MoySklad</div>
    <div class="card">
        {back_btn}
        {body}
    </div>
</body>
</html>
"""


def _select(name: str, items: list[dict]) -> str:
    options = "\n".join(
        f'<option value="{html.escape(item["id"])}">{html.escape(item["name"])}</option>'
        for item in items
    )
    return f'<select name="{html.escape(name)}" required>\n{options}\n</select>'


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


# ---------------------------------------------------------------------------
# Личный кабинет
# ---------------------------------------------------------------------------

def _render_status_badge(ok: bool, ok_text: str, fail_text: str) -> str:
    if ok:
        return f'<span style="color:#1a7f3c;font-weight:600;">✓ {ok_text}</span>'
    return f'<span style="color:#c0392b;font-weight:600;">✗ {fail_text}</span>'


def _render_lk(tenant: dict, info_message: str | None = None, error_message: str | None = None) -> HTMLResponse:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("SELECT COUNT(*) as cnt FROM mappings WHERE tenant_id = ? AND entity_type = 'product'"),
            (tenant["id"],),
        )
        mappings_count = cur.fetchone()["cnt"]

        cur.execute(aq("SELECT * FROM stock_sync_status WHERE tenant_id = ?"), (tenant["id"],))
        stock_row = cur.fetchone()
        stock_row = dict(stock_row) if stock_row else None

        cur.execute(
            aq("SELECT status, COUNT(*) as cnt FROM event_store WHERE tenant_id = ? GROUP BY status"),
            (tenant["id"],),
        )
        event_counts = {r["status"]: r["cnt"] for r in cur.fetchall()}

        token_row = get_active_telegram_link_token(conn, tenant["id"])
        conn.commit()
    finally:
        conn.close()

    ms_ok = bool(tenant.get("moysklad_token"))
    evotor_ok = bool(tenant.get("evotor_token"))
    sync_ok = bool(tenant.get("sync_completed_at"))
    tg_ok = bool(tenant.get("telegram_chat_id") and tenant.get("alerts_telegram_enabled"))
    email_ok = bool(tenant.get("alert_email") and tenant.get("alerts_email_enabled"))
    deep_link = _build_telegram_deep_link(token_row["link_token"]) if token_row else None

    # Сообщения
    msgs_html = ""
    if info_message:
        msgs_html += f'<div class="success">{html.escape(info_message)}</div>'
    if error_message:
        msgs_html += f'<div class="error">{html.escape(error_message)}</div>'

    # Блок 1 — Статус интеграции
    stock_status_html = ""
    if stock_row:
        stock_color = "#1a7f3c" if stock_row["status"] == "ok" else "#c0392b"
        last_sync = f'<span style="color:#8793a8;font-size:12px;margin-left:8px;">{_format_ts(stock_row.get("last_sync_at"))}</span>' if stock_row.get("last_sync_at") else ""
        stock_status_html = f"""
        <tr>
            <td style="color:#8793a8;padding:6px 0;width:200px;">Синхронизация остатков</td>
            <td style="padding:6px 0;"><span style="color:{stock_color};font-weight:600;">{stock_row["status"].upper()}</span>{last_sync}</td>
        </tr>"""

    integration_block = f"""
    <div class="section-title">Статус интеграции</div>
    <table style="width:100%;border-collapse:collapse;">
        <tr>
            <td style="color:#8793a8;padding:6px 0;width:200px;">МойСклад</td>
            <td style="padding:6px 0;">{_render_status_badge(ms_ok, "Подключён", "Не подключён")}</td>
        </tr>
        <tr>
            <td style="color:#8793a8;padding:6px 0;">Эвотор</td>
            <td style="padding:6px 0;">{_render_status_badge(evotor_ok, "Подключён", "Не подключён")}</td>
        </tr>
        <tr>
            <td style="color:#8793a8;padding:6px 0;">Первичная синхронизация</td>
            <td style="padding:6px 0;">{_render_status_badge(sync_ok, f"Выполнена {_format_ts(tenant.get('sync_completed_at'))}", "Не выполнена")}</td>
        </tr>
        <tr>
            <td style="color:#8793a8;padding:6px 0;">Товаров синхронизировано</td>
            <td style="padding:6px 0;font-weight:600;">{mappings_count}</td>
        </tr>
        {stock_status_html}
    </table>"""

    # Блок 2 — События
    failed = event_counts.get("FAILED", 0)
    retry = event_counts.get("RETRY", 0)
    done = event_counts.get("DONE", 0)
    new_ev = event_counts.get("NEW", 0)

    events_block = f"""
    <div class="section-title">События</div>
    <table style="width:100%;border-collapse:collapse;">
        <tr>
            <td style="color:#8793a8;padding:6px 0;width:200px;">Обработано</td>
            <td style="padding:6px 0;font-weight:600;color:#1a7f3c;">{done}</td>
        </tr>
        <tr>
            <td style="color:#8793a8;padding:6px 0;">В очереди</td>
            <td style="padding:6px 0;font-weight:600;">{new_ev}</td>
        </tr>
        <tr>
            <td style="color:#8793a8;padding:6px 0;">Повтор</td>
            <td style="padding:6px 0;font-weight:600;color:#e67e22;">{retry}</td>
        </tr>
        <tr>
            <td style="color:#8793a8;padding:6px 0;">Ошибки</td>
            <td style="padding:6px 0;font-weight:600;color:{'#c0392b' if failed > 0 else '#1a7f3c'};">{failed}</td>
        </tr>
    </table>"""

    # Блок 3 — Уведомления
    tg_connect_label = "Переподключить Telegram" if tg_ok else "Подключить Telegram"
    tg_status = _render_status_badge(tg_ok, f"Подключён", "Не подключён")
    email_val = html.escape(tenant.get("alert_email") or "")
    email_status = _render_status_badge(email_ok, f"Активен ({email_val})", "Не настроен")

    deep_link_html = ""
    if deep_link and not tg_ok: 
        deep_link_html = f"""
        <div style="margin-top:12px;padding:12px;background:#f5f7fb;border:1px solid #d8deea;border-radius:6px;font-size:13px;">
            <strong>Ссылка для подключения:</strong><br>
            <a href="{html.escape(deep_link)}">{html.escape(deep_link)}</a><br>
            <span style="color:#8793a8;">Действует до {_format_ts(token_row["expires_at"])}</span>
        </div>"""

    notifications_block = f"""
    <div class="section-title">Уведомления</div>
    <table style="width:100%;border-collapse:collapse;margin-bottom:16px;">
        <tr>
            <td style="color:#8793a8;padding:6px 0;width:200px;">Telegram</td>
            <td style="padding:6px 0;">{tg_status}</td>
        </tr>
        <tr>
            <td style="color:#8793a8;padding:6px 0;">Email</td>
            <td style="padding:6px 0;">{email_status}</td>
        </tr>
    </table>
    <form method="post" action="/onboarding/tenants/{html.escape(tenant['id'])}/telegram/link" style="display:inline;margin-right:8px;">
        <button type="submit" style="background:#f5f7fb;color:#2458d3;border:1px solid #d8deea;border-radius:6px;padding:8px 16px;cursor:pointer;font-size:13px;font-weight:600;">
            {tg_connect_label}
        </button>
    </form>
    {deep_link_html}"""

    # Блок 4 — Действия
    actions_block = f"""
    <div class="section-title">Действия</div>
    <div style="display:flex;flex-direction:column;gap:10px;">
        <a href="/onboarding/tenants/{html.escape(tenant['id'])}/token" class="btn-secondary">
            Обновить токен МойСклад →
        </a>
        <form method="post" action="/onboarding/tenants/{html.escape(tenant['id'])}/sync">
            <button type="submit" style="background:#f5f7fb;color:#2458d3;border:1px solid #d8deea;
                    border-radius:6px;padding:10px 20px;cursor:pointer;font-size:14px;font-weight:600;
                    width:100%;text-align:left;">
                Повторная синхронизация товаров →
            </button>
        </form>
        <form method="post" action="/onboarding/tenants/{html.escape(tenant['id'])}/reconcile">
            <button type="submit" style="background:#f5f7fb;color:#2458d3;border:1px solid #d8deea;
                    border-radius:6px;padding:10px 20px;cursor:pointer;font-size:14px;font-weight:600;
                    width:100%;text-align:left;">
                Синхронизировать остатки →
            </button>
        </form>
    </div>"""

    body = f"""
    {msgs_html}
    <div style="margin-bottom:20px;">
        <div style="font-size:18px;font-weight:700;color:#172033;">{html.escape(tenant["name"])}</div>
        <div style="font-size:12px;color:#8793a8;margin-top:4px;">ID: <code>{html.escape(tenant["id"])}</code></div>
    </div>
    <hr>
    {integration_block}
    <hr>
    {events_block}
    <hr>
    {notifications_block}
    <hr>
    {actions_block}
    """

    return HTMLResponse(_layout("Личный кабинет", body))


# ---------------------------------------------------------------------------
# Auto initial sync helper
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
    error = result.get("error", "")

    if status == "error":
        return f"""
        <div class="warning">
            <strong>Профиль создан, но первичная синхронизация не выполнена.</strong><br>
            Ошибка: {html.escape(str(error))}<br>
            <small>Запустите синхронизацию вручную: <code>POST /sync/{{tenant_id}}/initial</code></small>
        </div>"""

    css_class = "success" if status == "ok" else "warning"
    status_text = "Синхронизация выполнена успешно" if status == "ok" else "Синхронизация выполнена частично"

    errors_html = ""
    if result.get("errors"):
        errors_list = "".join(f'<li>{html.escape(str(e))}</li>' for e in result["errors"][:5])
        errors_html = f'<ul style="margin:8px 0 0;padding-left:18px;font-size:12px;">{errors_list}</ul>'

    return f"""
    <div class="{css_class}">
        <strong>{status_text}</strong>
        <div class="sync-result">
            <table>
                <tr><td>Синхронизировано</td><td><strong>{synced}</strong> товаров</td></tr>
                <tr><td>Пропущено</td><td>{skipped} товаров</td></tr>
                <tr><td>Ошибок</td><td>{failed} товаров</td></tr>
            </table>
            {errors_html}
        </div>
    </div>"""


# ---------------------------------------------------------------------------
# Step 1 — Evotor token
# ---------------------------------------------------------------------------

@router.get("/onboarding/evotor/connect", response_class=HTMLResponse)
def onboarding_token_form():
    body = """
    <form method="post" action="/onboarding/evotor/connect">
        <div class="field">
            <label>Evotor token</label>
            <input name="evotor_token" required placeholder="Вставьте токен из личного кабинета Эвотор" />
        </div>
        <button type="submit">Получить мои магазины →</button>
    </form>
    """
    return HTMLResponse(_layout("Подключение Эвотор", body))


@router.post("/onboarding/evotor/connect", response_class=HTMLResponse)
def onboarding_token_submit(evotor_token: str = Form(...)):
    evotor_token = evotor_token.strip()
    if not evotor_token:
        body = '<div class="error">Evotor token обязателен.</div>'
        return HTMLResponse(_layout("Ошибка подключения", body), status_code=400)

    try:
        stores = fetch_stores_by_token(evotor_token)
    except Exception as exc:
        log.exception("Failed to fetch stores by Evotor token")
        body = f'<div class="error">Не удалось получить магазины: {html.escape(str(exc))}</div>'
        return HTMLResponse(_layout("Ошибка подключения", body), status_code=502)

    if not stores:
        body = '<div class="error">По этому token не найдено ни одного магазина.</div>'
        return HTMLResponse(_layout("Магазины не найдены", body), status_code=400)

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

    stores_link = f"/onboarding/evotor/sessions/{session_id}/stores"
    body = f"""
    <div class="success">Магазины успешно получены — {len(stores)} шт.</div>
    <p><a href="{html.escape(stores_link)}">Перейти к выбору магазина →</a></p>
    """
    return HTMLResponse(_layout("Подключение Эвотор", body))


# ---------------------------------------------------------------------------
# Step 2 — выбор магазина Эвотор
# ---------------------------------------------------------------------------

@router.get("/onboarding/evotor/sessions/{session_id}/stores", response_class=HTMLResponse)
def onboarding_evotor_stores(session_id: str):
    session = _load_session(session_id)
    stores = json.loads(session["stores_json"] or "[]")

    if not stores:
        body = '<div class="error">Для этой сессии магазины не найдены.</div>'
        return HTMLResponse(_layout("Выбор магазина", body), status_code=400)

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
            <a href="{link}">Создать профиль для этого магазина →</a>
        </div>""")

    return HTMLResponse(_layout("Выбор магазина Эвотор", "".join(parts), back_url="/onboarding/evotor/connect"))


# ---------------------------------------------------------------------------
# Step 3 — ввод MS токена и автозагрузка данных
# ---------------------------------------------------------------------------

@router.get("/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token", response_class=HTMLResponse)
def onboarding_ms_token_form(session_id: str, store_id: str):
    body = f"""
    <p style="margin-bottom:20px;color:#5b6475;font-size:14px;">
        Магазин Эвотор: <code>{html.escape(store_id)}</code>
    </p>
    <form method="post" action="/onboarding/evotor/sessions/{html.escape(session_id)}/stores/{html.escape(store_id)}/ms-token">
        <div class="field">
            <label>MoySklad token</label>
            <input name="moysklad_token" required placeholder="Токен из раздела «Безопасность» → «Токены» в МойСклад" />
            <span class="hint">После ввода система автоматически загрузит ваши организации, склады и контрагентов.</span>
        </div>
        <button type="submit">Загрузить данные МойСклад →</button>
    </form>
    """
    return HTMLResponse(_layout("Подключение МойСклад", body, back_url=f"/onboarding/evotor/sessions/{session_id}/stores"))


@router.post("/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token", response_class=HTMLResponse)
def onboarding_ms_token_submit(session_id: str, store_id: str, moysklad_token: str = Form(...)):
    moysklad_token = moysklad_token.strip()
    if not moysklad_token:
        body = '<div class="error">MoySklad token обязателен.</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=400)

    try:
        orgs, ms_stores, agents = _ms_fetch_all(moysklad_token)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        msg = "Неверный MoySklad token." if status == 401 else f"Ошибка API МойСклад: {status}"
        body = f'<div class="error">{html.escape(msg)}</div>'
        return HTMLResponse(_layout("Ошибка подключения МойСклад", body,
            back_url=f"/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token"), status_code=502)
    except Exception as exc:
        log.exception("Failed to fetch MoySklad data")
        body = f'<div class="error">Не удалось получить данные МойСклад: {html.escape(str(exc))}</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=502)

    if not orgs:
        return HTMLResponse(_layout("Ошибка", '<div class="error">Не найдено ни одной организации.</div>'), status_code=400)
    if not ms_stores:
        return HTMLResponse(_layout("Ошибка", '<div class="error">Не найдено ни одного склада.</div>'), status_code=400)
    if not agents:
        return HTMLResponse(_layout("Ошибка", '<div class="error">Не найдено ни одного контрагента.</div>'), status_code=400)

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

    org_select = _select("ms_organization_id", orgs)
    store_select = _select("ms_store_id", ms_stores)
    agent_select = _select("ms_agent_id", agents)

    body = f"""
    <div class="success">Данные МойСклад успешно загружены.</div>
    <form method="post" action="/onboarding/store-profile">
        <input type="hidden" name="session_id" value="{html.escape(session_id)}" />
        <input type="hidden" name="evotor_store_id" value="{html.escape(store_id)}" />
        <div class="field">
            <label>Имя профиля магазина</label>
            <input name="name" required placeholder="Например: Мой магазин на Ленина" />
        </div>
        <hr>
        <div class="section-title">МойСклад</div>
        <div class="field"><label>Организация</label>{org_select}</div>
        <div class="field"><label>Склад</label>{store_select}</div>
        <div class="field">
            <label>Контрагент по умолчанию</label>
            {agent_select}
            <span class="hint">Используется как покупатель если данные клиента в чеке отсутствуют.</span>
        </div>
        <hr>
        <div class="section-title">Уведомления (необязательно)</div>
        <div class="field">
            <label>Email для уведомлений</label>
            <input type="email" name="alert_email" placeholder="owner@example.com" />
        </div>
        <div class="checkbox">
            <input id="alerts_email_enabled" type="checkbox" name="alerts_email_enabled" checked />
            <label for="alerts_email_enabled">Включить email-уведомления</label>
        </div>
        <hr>
        <div class="section-title">Фискализация (необязательно)</div>
        <div class="field">
            <label>Fiscal token</label>
            <input name="fiscal_token" placeholder="Оставьте пустым если не нужна фискализация" />
        </div>
        <div class="field"><label>Fiscal client UID</label><input name="fiscal_client_uid" /></div>
        <div class="field"><label>Fiscal device UID</label><input name="fiscal_device_uid" /></div>
        <button type="submit">Создать профиль магазина →</button>
    </form>
    """
    return HTMLResponse(_layout("Настройка профиля магазина", body,
        back_url=f"/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token"))


# ---------------------------------------------------------------------------
# Step 4 — сохранение профиля + автосинхронизация
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
        body = '<div class="error">Сессия не содержит MoySklad token. Начните онбординг заново.</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=400)

    ms_data = json.loads(session.get("ms_data_json") or "{}")
    valid_org_ids = {item["id"] for item in ms_data.get("orgs", [])}
    valid_store_ids = {item["id"] for item in ms_data.get("stores", [])}
    valid_agent_ids = {item["id"] for item in ms_data.get("agents", [])}

    if ms_organization_id not in valid_org_ids:
        return HTMLResponse(_layout("Ошибка", '<div class="error">Выбрана неверная организация.</div>'), status_code=400)
    if ms_store_id not in valid_store_ids:
        return HTMLResponse(_layout("Ошибка", '<div class="error">Выбран неверный склад.</div>'), status_code=400)
    if ms_agent_id not in valid_agent_ids:
        return HTMLResponse(_layout("Ошибка", '<div class="error">Выбран неверный контрагент.</div>'), status_code=400)

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
                f'<div class="error">Профиль для этого магазина уже существует: '
                f'<code>{html.escape(existing["id"])}</code></div>'
                f'<a href="/onboarding/evotor/connect" class="btn-secondary">Начать сначала →</a>'
            )
            return HTMLResponse(_layout("Профиль уже существует", body), status_code=409)

        cur.execute(
            aq("""INSERT INTO tenants (
                id, name, evotor_api_key, moysklad_token, created_at,
                evotor_token, evotor_store_id,
                ms_organization_id, ms_store_id, ms_agent_id,
                alert_email, alerts_email_enabled,
                telegram_chat_id, alerts_telegram_enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""),
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
        body = f'<div class="error">Не удалось создать профиль магазина: {html.escape(str(exc))}</div>'
        return HTMLResponse(_layout("Ошибка создания профиля", body), status_code=500)
    finally:
        conn.close()

    log.info("Starting auto initial sync for tenant_id=%s", tenant_id)
    sync_result = _run_initial_sync(tenant_id)
    sync_html = _render_sync_result(sync_result)

    body = f"""
    <div class="success"><strong>Профиль магазина успешно создан!</strong></div>
    <div class="section-title">Первичная синхронизация товаров</div>
    {sync_html}
    <hr>
    <div class="section-title">Следующий шаг</div>
    <p style="color:#5b6475;font-size:13px;">Подключите Telegram чтобы получать уведомления о сбоях.</p>
    <a href="/onboarding/tenants/{html.escape(tenant_id)}/telegram" class="btn-secondary">
        Перейти в личный кабинет →
    </a>
    <hr>
    <p style="color:#8793a8;font-size:12px;margin-top:16px;">
        Tenant ID: <code>{html.escape(tenant_id)}</code>
    </p>
    """
    return HTMLResponse(_layout("Профиль создан", body))


# ---------------------------------------------------------------------------
# Личный кабинет
# ---------------------------------------------------------------------------

@router.get("/onboarding/tenants/{tenant_id}", response_class=HTMLResponse)
def onboarding_tenant_lk(tenant_id: str):
    tenant = _load_tenant(tenant_id)
    return _render_lk(tenant)


@router.get("/onboarding/tenants/{tenant_id}/telegram", response_class=HTMLResponse)
def onboarding_tenant_lk_telegram_redirect(tenant_id: str):
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=f"/onboarding/tenants/{tenant_id}", status_code=301)


@router.post("/onboarding/tenants/{tenant_id}/telegram/link", response_class=HTMLResponse)
def onboarding_tenant_telegram_link(tenant_id: str):
    tenant = _load_tenant(tenant_id)

    if not _get_telegram_bot_username():
        return _render_lk(tenant, error_message="TELEGRAM_BOT_USERNAME не настроен.")

    conn = get_connection()
    try:
        create_telegram_link_token(conn, tenant_id=tenant_id, ttl_sec=TELEGRAM_LINK_TOKEN_TTL_SEC)
        conn.commit()
    finally:
        conn.close()

    return _render_lk(tenant, info_message="Ссылка для подключения Telegram создана. Перейдите по ссылке и нажмите /start в боте.")


# ---------------------------------------------------------------------------
# Telegram webhook
# ---------------------------------------------------------------------------

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
            _reply_in_telegram(chat_id, "Используйте ссылку подключения из личного кабинета чтобы привязать Telegram.")
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
            msg = "Срок действия ссылки истёк." if token_row["status"] == "expired" else "Ссылка уже использована."
            _reply_in_telegram(chat_id, msg + " Создайте новую ссылку в личном кабинете.")
            conn.commit()
            return {"ok": True}

        if int(token_row["expires_at"]) <= now:
            conn.cursor().execute(aq("UPDATE telegram_link_tokens SET status='expired' WHERE id=?"), (token_row["id"],))
            conn.commit()
            _reply_in_telegram(chat_id, "Срок действия ссылки истёк. Создайте новую ссылку в личном кабинете.")
            return {"ok": True}

        conn.cursor().execute(
            aq("UPDATE tenants SET telegram_chat_id=?, alerts_telegram_enabled=1 WHERE id=?"),
            (str(chat_id), token_row["tenant_id"]),
        )
        mark_telegram_link_token_linked(conn, token_id=token_row["id"], linked_chat_id=str(chat_id), linked_at=now)
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Failed to process Telegram link webhook chat_id=%s", chat_id)
        _reply_in_telegram(chat_id, "Не удалось завершить подключение. Попробуйте создать новую ссылку.")
        return {"ok": True}
    finally:
        conn.close()

    _reply_in_telegram(chat_id, f"Telegram успешно подключен к магазину {token_row['tenant_id']}. Уведомления будут приходить сюда.")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Обновление токена МойСклад
# ---------------------------------------------------------------------------

@router.get("/onboarding/tenants/{tenant_id}/token", response_class=HTMLResponse)
def onboarding_tenant_token_form(tenant_id: str):
    tenant = _load_tenant(tenant_id)
    body = f"""
    <div class="field">
        <label>Магазин</label>
        <div><strong>{html.escape(tenant["name"])}</strong></div>
    </div>
    <div class="success">Введите новый токен МойСклад если старый был сброшен или истёк.</div>
    <form method="post" action="/onboarding/tenants/{html.escape(tenant_id)}/token">
        <div class="field">
            <label>Новый токен МойСклад</label>
            <input name="moysklad_token" required placeholder="Токен из раздела «Безопасность» → «Токены»" />
            <span class="hint">Система проверит токен перед сохранением.</span>
        </div>
        <button type="submit">Обновить токен →</button>
    </form>
    """
    return HTMLResponse(_layout("Обновление токена МойСклад", body, back_url=f"/onboarding/tenants/{tenant_id}/telegram"))


@router.post("/onboarding/tenants/{tenant_id}/token", response_class=HTMLResponse)
def onboarding_tenant_token_submit(tenant_id: str, moysklad_token: str = Form(...)):
    _load_tenant(tenant_id)
    moysklad_token = moysklad_token.strip()

    if not moysklad_token:
        body = '<div class="error">Токен обязателен.</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=400)

    try:
        orgs, _, _ = _ms_fetch_all(moysklad_token)
        if not orgs:
            raise ValueError("Нет доступных организаций")
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        msg = "Неверный токен." if status == 401 else f"Ошибка API МойСклад: {status}"
        body = f'<div class="error">{html.escape(msg)}</div>'
        return HTMLResponse(_layout("Ошибка", body, back_url=f"/onboarding/tenants/{tenant_id}/token"), status_code=502)
    except Exception as e:
        body = f'<div class="error">Не удалось проверить токен: {html.escape(str(e))}</div>'
        return HTMLResponse(_layout("Ошибка", body, back_url=f"/onboarding/tenants/{tenant_id}/token"), status_code=502)

    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("UPDATE tenants SET moysklad_token=?, updated_at=? WHERE id=?"), (moysklad_token, now, tenant_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.exception("Failed to update moysklad token tenant_id=%s", tenant_id)
        body = f'<div class="error">Не удалось сохранить токен: {html.escape(str(e))}</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=500)
    finally:
        conn.close()

    log.info("moysklad token updated tenant_id=%s", tenant_id)

    tenant = _load_tenant(tenant_id)
    return _render_lk(tenant, info_message=f"Токен МойСклад успешно обновлён. Найдено организаций: {len(orgs)}")

@router.post("/onboarding/tenants/{tenant_id}/sync", response_class=HTMLResponse)
def onboarding_tenant_sync(tenant_id: str):
    tenant = _load_tenant(tenant_id)
    
    # Сбрасываем флаг синхронизации чтобы можно было запустить заново
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("UPDATE tenants SET sync_completed_at = NULL WHERE id = ?"), (tenant_id,))
        conn.commit()
    finally:
        conn.close()

    result = _run_initial_sync(tenant_id)
    tenant = _load_tenant(tenant_id)
    return _render_lk(tenant, info_message=f"Синхронизация завершена: {result.get('synced', 0)} новых товаров синхронизировано, {result.get('failed', 0)} ошибок.")

@router.post("/onboarding/tenants/{tenant_id}/reconcile", response_class=HTMLResponse)
def onboarding_tenant_reconcile(tenant_id: str):
    import requests as req
    from app.clients.evotor_client import EvotorClient
    from app.clients.moysklad_client import MoySkladClient
    from app.stores.mapping_store import MappingStore

    tenant = _load_tenant(tenant_id)

    if not tenant.get("sync_completed_at"):
        tenant = _load_tenant(tenant_id)
        return _render_lk(tenant, error_message="Сначала выполните первичную синхронизацию товаров.")

    store = MappingStore()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("SELECT evotor_id, ms_id FROM mappings WHERE tenant_id = ? AND entity_type = 'product'"),
            (tenant_id,),
        )
        mappings = cur.fetchall()
    finally:
        conn.close()

    if not mappings:
        tenant = _load_tenant(tenant_id)
        return _render_lk(tenant, error_message="Нет маппингов товаров для синхронизации остатков.")

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
    return _render_lk(tenant, info_message=f"Остатки синхронизированы: {synced} товаров, {failed} ошибок.")