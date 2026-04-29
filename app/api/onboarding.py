import urllib.parse as _urlparse
import html
import json
import logging
import os
import re
import time
import uuid
import requests

from urllib.parse import quote_plus
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


from app.services.product_snapshot_service import (
    create_product_snapshot,
    get_last_product_snapshot,
    rollback_evotor_catalog_from_snapshot,
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
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json;charset=utf-8",
        "Accept-Encoding": "gzip",
    }

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


def _get_session_store(session: dict, store_id: str) -> dict | None:
    stores = json.loads(session.get("stores_json") or "[]")
    for store in stores:
        if _extract_store_id(store) == store_id:
            return store
    return None


def _ensure_primary_store_row(tenant_id: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("SELECT * FROM tenants WHERE id = ?"), (tenant_id,))
        tenant = cur.fetchone()
        if not tenant:
            return
        tenant = dict(tenant)

        evotor_store_id = (tenant.get("evotor_store_id") or "").strip()
        if not evotor_store_id:
            return

        cur.execute(
            aq("SELECT id FROM tenant_stores WHERE tenant_id = ? AND evotor_store_id = ?"),
            (tenant_id, evotor_store_id),
        )
        row = cur.fetchone()
        now = int(time.time())

        if row:
            cur.execute(
                aq("""
                    UPDATE tenant_stores
                    SET name = COALESCE(NULLIF(name, ''), ?),
                        ms_store_id = COALESCE(ms_store_id, ?),
                        ms_organization_id = COALESCE(ms_organization_id, ?),
                        ms_agent_id = COALESCE(ms_agent_id, ?),
                        sync_completed_at = COALESCE(sync_completed_at, ?),
                        is_primary = CASE WHEN is_primary IS NULL THEN 1 ELSE is_primary END,
                        updated_at = ?
                    WHERE tenant_id = ? AND evotor_store_id = ?
                """),
                (
                    tenant.get("name") or None,
                    tenant.get("ms_store_id") or None,
                    tenant.get("ms_organization_id") or None,
                    tenant.get("ms_agent_id") or None,
                    tenant.get("sync_completed_at") or None,
                    now,
                    tenant_id,
                    evotor_store_id,
                ),
            )
        else:
            cur.execute(
                aq("""
                    INSERT INTO tenant_stores (
                        id, tenant_id, evotor_store_id, name,
                        ms_store_id, ms_organization_id, ms_agent_id,
                        is_primary, sync_completed_at, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                """),
                (
                    str(uuid.uuid4()),
                    tenant_id,
                    evotor_store_id,
                    tenant.get("name") or None,
                    tenant.get("ms_store_id") or None,
                    tenant.get("ms_organization_id") or None,
                    tenant.get("ms_agent_id") or None,
                    tenant.get("sync_completed_at") or None,
                    tenant.get("created_at") or now,
                    now,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Failed to ensure primary store row tenant_id=%s", tenant_id)
    finally:
        conn.close()


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


def _load_tenant_stores(tenant_id: str) -> list[dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            SELECT id, tenant_id, evotor_store_id, name, ms_store_id,
                   ms_organization_id, ms_agent_id, is_primary, sync_completed_at, created_at
            FROM tenant_stores
            WHERE tenant_id = ?
            ORDER BY is_primary DESC, created_at ASC
            """),
            (tenant_id,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _get_lk_data(tenant_id: str) -> tuple[dict, dict, int, dict | None, int, dict | None]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("SELECT * FROM tenants WHERE id = ?"), (tenant_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Tenant not found")
        tenant = dict(row)

        # Считаем уникальные ms_id — один товар МС может быть в нескольких магазинах
        # Считаем уникальные ms_id — один товар МС может быть в нескольких магазинах
        cur.execute(
            aq("SELECT COUNT(DISTINCT ms_id) as cnt FROM mappings WHERE tenant_id = ? AND entity_type = 'product'"),
            (tenant_id,),
        )
        mappings_count = cur.fetchone()["cnt"]

        # Считаем все store-level связки:
        # один товар МС может быть выгружен в несколько магазинов Эвотор
        cur.execute(
            aq("SELECT COUNT(*) as cnt FROM mappings WHERE tenant_id = ? AND entity_type = 'product'"),
            (tenant_id,),
        )
        store_mappings_count = cur.fetchone()["cnt"]

        cur.execute(aq("SELECT * FROM stock_sync_status WHERE tenant_id = ?"), (tenant_id,))
        stock = cur.fetchone()
        stock_row = dict(stock) if stock else None

        cur.execute(
            aq("""
            SELECT status, COUNT(*) as cnt
            FROM (
                SELECT
                    status,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(NULLIF(event_key, ''), id)
                        ORDER BY created_at DESC, id DESC
                    ) as rn
                FROM event_store
                WHERE tenant_id = ?
            ) t
            WHERE rn = 1
            GROUP BY status
            """),
            (tenant_id,),
        )
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

    return tenant, event_counts, mappings_count, store_mappings_count, stock_row, ms_products_count, last_event


def _is_valid_email(email: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", (email or "").strip()))


# ---------------------------------------------------------------------------
# LK styles & layout
# ---------------------------------------------------------------------------

LK_STYLE = """
<style>
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
.sync-overlay { display:none; position:fixed; inset:0; background:rgba(255,255,255,0.8); backdrop-filter:blur(2px); z-index:999; align-items:center; justify-content:center; flex-direction:column; gap:16px; }
.sync-overlay.visible { display:flex; }
.sync-spinner { width:48px; height:48px; border:4px solid #e4e8f0; border-top-color:#3b6ff5; border-radius:50%; animation:spin 0.8s linear infinite; }
.sync-label { font-size:15px; font-weight:600; color:#1a1d2e; }
.sync-sublabel { font-size:13px; color:#6b7280; }
.tooltip-wrap { position: relative; display: inline-block; }
.tooltip-btn { background: none; border: none; cursor: pointer; color: #9ca3af; font-size: 15px; padding: 0 4px; vertical-align: middle; line-height: 1; transition: color 0.15s; }
.tooltip-btn:hover { color: #3b6ff5; }
.tooltip-popup { display: none; position: absolute; bottom: calc(100% + 8px); left: 50%; transform: translateX(-50%); background: #1a1d2e; color: #fff; font-size: 13px; line-height: 1.5; padding: 10px 14px; border-radius: 8px; width: 260px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); z-index: 100; pointer-events: none; }
.tooltip-popup::after { content: ''; position: absolute; top: 100%; left: 50%; transform: translateX(-50%); border: 6px solid transparent; border-top-color: #1a1d2e; }
.tooltip-wrap:hover .tooltip-popup { display: block; }
.action-row { display: flex; align-items: center; gap: 8px; }
.action-row form { flex: 1; }
.action-row .btn-ghost { width: 100%; }
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

@media (max-width: 600px) {
  .lk-header { padding: 0 16px; }
  .lk-header-inner { height: 48px; }
  .lk-logo { font-size: 13px; }
  .lk-tenant-name { font-size: 11px; max-width: 130px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .lk-container { padding: 16px 12px 60px; }
  .lk-page-title { font-size: 20px; margin-bottom: 2px; }
  .lk-page-subtitle { font-size: 11px; margin-bottom: 16px; }
  .lk-tabs { width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch; scrollbar-width: none; }
  .lk-tabs::-webkit-scrollbar { display: none; }
  .lk-tab { padding: 6px 12px; font-size: 12px; }
  .lk-card { padding: 16px; margin-bottom: 12px; }
  .lk-card-title { font-size: 11px; margin-bottom: 12px; }
  .lk-row { flex-wrap: wrap; gap: 4px; padding: 8px 0; }
  .lk-row-label { font-size: 13px; width: 100%; }
  .lk-row-value { font-size: 13px; }
  .stats-grid { grid-template-columns: repeat(2, 1fr); gap: 8px; }
  .stat-value { font-size: 22px; }
  .stat-card { padding: 12px; }
  .btn { padding: 8px 14px; font-size: 13px; }
  .btn-ghost { font-size: 13px; }
  .deep-link-url { font-size: 12px; }
}
</style>
"""


def _lk_layout(tenant: dict, active_tab: str, content: str,
                info_message: str | None = None, error_message: str | None = None) -> HTMLResponse:
    tenant_id = tenant["id"]
    name = html.escape(tenant.get("name", ""))
    tabs = [
        ("overview",      f"/onboarding/tenants/{tenant_id}",               "Обзор"),
        ("stores",        f"/onboarding/tenants/{tenant_id}/stores",        "Магазины"),
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
            <div class="lk-page-subtitle">Tenant ID: <code>{html.escape(tenant_id)}</code></div>
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
    try:
        cleanup_result = cleanup_stale_product_mappings(tenant_id)

        if cleanup_result.get("deleted"):
            log.warning(
                "_run_initial_sync: stale mapping cleanup tenant_id=%s deleted=%s",
                tenant_id,
                cleanup_result.get("deleted"),
            )
    except Exception as e:
        log.warning(
            "_run_initial_sync: stale mapping cleanup skipped tenant_id=%s err=%s",
            tenant_id,
            e,
        )

    """
    Запускает первичную синхронизацию для всех магазинов тенанта.
    Использует store-level sync для каждого магазина.
    """
    from app.api.sync import initial_sync_store
    from app.api.vendor import _notify_ms_activated

    # Загружаем все магазины тенанта
    stores = _load_tenant_stores(tenant_id)
    if not stores:
        # Fallback: если магазинов нет в tenant_stores — пробуем старый путь
        from app.api.sync import initial_sync
        try:
            return initial_sync(tenant_id)
        except Exception as e:
            log.exception("Auto initial sync (legacy) failed tenant_id=%s", tenant_id)
            return {"status": "error", "error": str(e), "synced": 0, "failed": 0, "skipped": 0}

    total_synced = total_failed = total_skipped = 0
    all_errors = []

    for store in stores:
        store_id = store["evotor_store_id"]
        try:
            result = initial_sync_store(tenant_id, store_id)
            total_synced += result.get("synced", 0)
            total_failed += result.get("failed", 0)
            total_skipped += result.get("skipped", 0)
            all_errors.extend(result.get("errors", []))
            log.info(
                "_run_initial_sync: store=%s synced=%s failed=%s",
                store_id, result.get("synced", 0), result.get("failed", 0),
            )
        except Exception as e:
            log.exception("_run_initial_sync: store=%s failed err=%s", store_id, e)
            total_failed += 1
            all_errors.append({"store": store_id, "error": str(e)})

    # Уведомляем МойСклад, что настройка завершена, только если синхронизация прошла без ошибок
    if total_failed == 0:
        try:
            conn = get_connection()
            try:
                cur = conn.cursor()
                cur.execute(
                    aq("SELECT ms_account_id, moysklad_token FROM tenants WHERE id = ?"),
                    (tenant_id,),
                )
                t = cur.fetchone()
            finally:
                conn.close()

            if t and t["ms_account_id"] and t["moysklad_token"]:
                _notify_ms_activated(t["ms_account_id"])

        except Exception as notify_err:
                log.warning("_run_initial_sync: notify_ms_activated failed err=%s", notify_err)
    return {
        "status": "ok" if total_failed == 0 else "partial",
        "synced": total_synced,
        "failed": total_failed,
        "skipped": total_skipped,
        "errors": all_errors[:10],
    }


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
# NEW WIZARD — пошаговый онбординг для тенантов с токеном МС
# ---------------------------------------------------------------------------

def _ob_step_layout(step: int, total: int, title: str, body: str, back_url: str | None = None) -> str:
    """Layout для пошагового онбординга с индикатором прогресса."""
    back_btn = f'<a href="{html.escape(back_url)}" class="back-btn">← Назад</a>' if back_url else ""
    steps_html = ""
    for i in range(1, total + 1):
        if i < step:
            cls = "step-done"
            icon = "✓"
        elif i == step:
            cls = "step-active"
            icon = str(i)
        else:
            cls = "step-pending"
            icon = str(i)
        steps_html += f'<div class="ob-step {cls}"><span class="ob-step-num">{icon}</span></div>'

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>{html.escape(title)}</title>
    {LK_STYLE}
    <style>
    .ob-steps {{display:flex;align-items:center;gap:8px;margin-bottom:28px;}}
    .ob-step {{display:flex;align-items:center;gap:8px;}}
    .ob-step-num {{width:28px;height:28px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;}}
    .step-done .ob-step-num {{background:#059669;color:#fff;}}
    .step-active .ob-step-num {{background:#2458d3;color:#fff;}}
    .step-pending .ob-step-num {{background:#e5e7eb;color:#9ca3af;}}
    .ob-step:not(:last-child)::after {{content:"";flex:1;height:2px;background:#e5e7eb;margin:0 4px;}}
    .step-done::after {{background:#059669 !important;}}
    .help-toggle {{display:inline-flex;align-items:center;gap:6px;font-size:13px;color:#2458d3;
                   text-decoration:none;cursor:pointer;background:none;border:none;padding:0;margin-top:8px;}}
    .help-toggle:hover {{text-decoration:underline;}}
    .help-box {{display:none;background:#f0f4ff;border:1px solid #c7d7f5;border-radius:10px;
                padding:16px;margin-top:12px;font-size:13px;color:#374151;line-height:1.6;}}
    .help-box.visible {{display:block;}}
    </style>
</head>
<body style="background:#f5f7fb;padding:24px;">
    <div style="margin-bottom:8px;font-size:15px;font-weight:700;">Evotor ↔ MoySklad</div>
    <div style="color:#5b6475;margin-bottom:24px;font-size:14px;">Настройка интеграции</div>
    <div class="onboarding-card">
        {back_btn}
        <div class="ob-steps">{steps_html}</div>
        <h2 style="font-size:20px;font-weight:700;margin-bottom:20px;">{html.escape(title)}</h2>
        {body}
    </div>


</body>
</html>"""


# Шаг 1 — ввод токена Эвотор
@router.get("/onboarding/wizard/{tenant_id}/step1", response_class=HTMLResponse)
def wizard_step1(tenant_id: str, err: str | None = None):
    tenant = _load_tenant(tenant_id)
    if not tenant.get("moysklad_token"):
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Токен МойСклад не найден. Переустановите решение.</div>'))

    err_html = f'<div class="ob-error">{html.escape(err)}</div>' if err else ""
    body = f"""
    <style>
    .help-modal {{ display:none; position:fixed; inset:0; background:rgba(0,0,0,0.5);
        z-index:1000; align-items:flex-start; justify-content:center; overflow-y:auto; padding:20px; }}
    .help-modal.visible {{ display:flex; }}
    .help-modal-box {{ background:#fff; border-radius:14px; width:100%; max-width:700px;
        margin:auto; overflow:hidden; box-shadow:0 8px 40px rgba(0,0,0,0.2); }}
    .help-modal-header {{ display:flex; align-items:center; justify-content:space-between;
        padding:16px 20px; border-bottom:1px solid #e4e8f0; }}
    .help-modal-title {{ font-size:16px; font-weight:700; color:#1a1d2e; }}
    .help-modal-close {{ background:none; border:none; font-size:22px; cursor:pointer;
        color:#9ca3af; line-height:1; padding:0 4px; }}
    .help-modal-close:hover {{ color:#1a1d2e; }}
    .help-modal-content {{ padding:20px; max-height:75vh; overflow-y:auto; }}
    .help-step {{ margin-bottom:24px; }}
    .help-step-num {{ display:inline-flex; align-items:center; justify-content:center;
        width:28px; height:28px; border-radius:8px; background:#FF4D00; color:#fff;
        font-size:13px; font-weight:700; margin-bottom:10px; }}
    .help-step-title {{ font-size:15px; font-weight:700; color:#1a1d2e; margin-bottom:6px; }}
    .help-step-desc {{ font-size:13px; color:#6b7280; margin-bottom:10px; line-height:1.5; }}
    .help-step img {{ width:100%; border-radius:8px; border:1px solid #e4e8f0; }}
    .help-link {{ display:inline-flex; align-items:center; gap:6px; font-size:13px;
        color:#2458d3; text-decoration:none; cursor:pointer; background:none; border:none;
        padding:0; font-family:inherit; margin-top:6px; }}
    .help-link:hover {{ text-decoration:underline; }}
    </style>
    {err_html}
    <p style="color:#5b6475;font-size:14px;margin-bottom:20px;line-height:1.6;">
        Введите токен из личного кабинета Эвотор. Мы автоматически загрузим список ваших магазинов.
    </p>
    <form method="post" action="/onboarding/wizard/{html.escape(tenant_id)}/step1">
        <div class="field">
            <label>Evotor Token</label>
            <input type="text" name="evotor_token" required
                   placeholder="Вставьте токен из личного кабинета Эвотор"
                   value="{html.escape(tenant.get('evotor_token') or '')}" />
            <span class="hint">Найдите в evotor.ru → Мои приложения → Универсальный фискализатор → Настройки</span>
            <button type="button" class="help-link" onclick="document.getElementById('helpModal').classList.add('visible')">
                📖 Где найти токен? Пошаговая инструкция
            </button>
        </div>
        <button type="submit" class="ob-btn" style="margin-top:20px;">Получить магазины →</button>
    </form>

    <div class="help-modal" id="helpModal">
        <div class="help-modal-box">
            <div class="help-modal-header">
                <div class="help-modal-title">Как найти токен Эвотор</div>
                <button class="help-modal-close" onclick="document.getElementById('helpModal').classList.remove('visible')">×</button>
            </div>
            <div class="help-modal-content">
                <div class="help-step">
                    <div class="help-step-num">1</div>
                    <div class="help-step-title">Войдите в личный кабинет Эвотор</div>
                    <div class="help-step-desc">Откройте <strong>evotor.ru</strong> и войдите в аккаунт.</div>
                    <img src="/static/help/img/step1.png" alt="Шаг 1">
                </div>
                <div class="help-step">
                    <div class="help-step-num">2</div>
                    <div class="help-step-title">Найдите "Универсальный фискализатор"</div>
                    <div class="help-step-desc">В строке поиска введите <strong>«Универсальный фискализатор»</strong> и перейдите на вкладку <strong>Приложения</strong>.</div>
                    <img src="/static/help/img/step2.png" alt="Шаг 2">
                </div>
                <div class="help-step">
                    <div class="help-step-num">3</div>
                    <div class="help-step-title">Откройте страницу приложения</div>
                    <div class="help-step-desc">Нажмите на приложение и затем кнопку <strong>«Открыть»</strong>.</div>
                    <img src="/static/help/img/step3.png" alt="Шаг 3">
                </div>
                <div class="help-step">
                    <div class="help-step-num">4</div>
                    <div class="help-step-title">Установите приложение на кассу</div>
                    <div class="help-step-desc">На вкладке <strong>«Установка / Удаление»</strong> выберите кассу и нажмите <strong>«Установить»</strong>.</div>
                    <img src="/static/help/img/step4.png" alt="Шаг 4">
                </div>
                <div class="help-step">
                    <div class="help-step-num">5</div>
                    <div class="help-step-title">Откройте "Мои приложения"</div>
                    <div class="help-step-desc">В меню слева нажмите <strong>«Мои приложения»</strong> и откройте <strong>«Универсальный фискализатор»</strong>.</div>
                    <img src="/static/help/img/step5.png" alt="Шаг 5">
                </div>
                <div class="help-step">
                    <div class="help-step-num">6</div>
                    <div class="help-step-title">Скопируйте токен</div>
                    <div class="help-step-desc">На странице приложения найдите поле <strong>Evotor Token</strong> и скопируйте его значение.</div>
                    <img src="/static/help/img/step6.png" alt="Шаг 6">
                </div>
            </div>
        </div>
    </div>"""
    return HTMLResponse(_ob_step_layout(1, 5, "Подключение Эвотор", body))


@router.post("/onboarding/wizard/{tenant_id}/step1", response_class=HTMLResponse)
def wizard_step1_submit(tenant_id: str, evotor_token: str = Form(...)):
    from urllib.parse import quote_plus
    evotor_token = evotor_token.strip()
    if not evotor_token:
        return RedirectResponse(
            url=f"/onboarding/wizard/{tenant_id}/step1?err=Токен+обязателен",
            status_code=303,
        )
    try:
        stores = fetch_stores_by_token(evotor_token)
    except Exception as e:
        return RedirectResponse(
            url=f"/onboarding/wizard/{tenant_id}/step1?err={quote_plus(f'Не удалось получить магазины: {e}')}",
            status_code=303,
        )
    if not stores:
        return RedirectResponse(
            url=f"/onboarding/wizard/{tenant_id}/step1?err=Магазины+не+найдены+по+этому+токену",
            status_code=303,
        )
    # Сохраняем токен и магазины в сессии
    now = int(time.time())
    session_id = str(uuid.uuid4())
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
    return RedirectResponse(
        url=f"/onboarding/wizard/{tenant_id}/step2/{session_id}",
        status_code=303,
    )


# Шаг 2 — выбор магазина
@router.get("/onboarding/wizard/{tenant_id}/step2/{session_id}", response_class=HTMLResponse)
def wizard_step2(tenant_id: str, session_id: str):
    session = _load_session(session_id)
    stores = json.loads(session.get("stores_json") or "[]")

    # Фильтруем магазины которые уже привязаны к этому тенанту
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(aq("SELECT evotor_store_id FROM tenant_stores WHERE tenant_id = ?"), (tenant_id,))
    already_added = {r["evotor_store_id"] for r in cur.fetchall()}
    conn.close()

    parts = [
        '<p style="color:#5b6475;font-size:14px;margin-bottom:20px;line-height:1.6;">'
        'Выберите магазин Эвотор который хотите подключить.'
        '</p>'
    ]

    has_stores = False
    for store in stores:
        store_id = _extract_store_id(store)
        if not store_id:
            continue
        store_name = _extract_store_name(store, store_id)
        if store_id in already_added:
            parts.append(f"""
            <div class="store" style="opacity:0.5;">
                <p style="font-size:16px;font-weight:700;margin-bottom:4px;">{html.escape(store_name)}</p>
                <p style="margin:0;color:#8793a8;font-size:13px;">Уже добавлен</p>
            </div>""")
        else:
            has_stores = True
            parts.append(f"""
            <div class="store">
                <p style="font-size:16px;font-weight:700;margin-bottom:4px;">{html.escape(store_name)}</p>
                <p style="margin:0 0 12px;color:#8793a8;font-size:13px;">ID: {html.escape(store_id)}</p>
                <a href="/onboarding/wizard/{html.escape(tenant_id)}/step3/{html.escape(session_id)}/{html.escape(store_id)}"
                   class="ob-btn" style="display:inline-block;text-decoration:none;">Выбрать →</a>
            </div>""")

    if not has_stores:
        parts.append('<div class="ob-success">Все магазины уже подключены.</div>')
        parts.append(f'<a href="/onboarding/tenants/{html.escape(tenant_id)}" class="ob-btn" style="display:inline-block;text-decoration:none;margin-top:12px;">Перейти в личный кабинет →</a>')

    return HTMLResponse(_ob_step_layout(2, 5, "Выберите магазин Эвотор", "".join(parts),
                        back_url=f"/onboarding/wizard/{tenant_id}/step1"))


# Шаг 3 — настройка магазина
@router.get("/onboarding/wizard/{tenant_id}/step3/{session_id}/{store_id}", response_class=HTMLResponse)
def wizard_step3(tenant_id: str, session_id: str, store_id: str, err: str | None = None):
    tenant = _load_tenant(tenant_id)
    session = _load_session(session_id)
    store = _get_session_store(session, store_id)
    if not store:
        return HTMLResponse(_ob_step_layout(3, 5, "Ошибка", '<div class="ob-error">Магазин не найден.</div>'))

    store_name = _extract_store_name(store, store_id)

    # Загружаем данные МС
    ms_orgs, ms_stores, ms_agents = [], [], []
    ms_err = ""
    try:
        ms_orgs, ms_stores, ms_agents = _ms_fetch_all(tenant["moysklad_token"])
    except Exception as e:
        ms_err = str(e)

    err_html = f'<div class="ob-error">{html.escape(err)}</div>' if err else ""
    ms_err_html = f'<div class="ob-error">Ошибка загрузки данных МС: {html.escape(ms_err)}</div>' if ms_err else ""

    # Определяем первичный ли это магазин
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(aq("SELECT COUNT(*) as cnt FROM tenant_stores WHERE tenant_id = ?"), (tenant_id,))
    is_first = cur.fetchone()["cnt"] == 0
    conn.close()

    body = f"""
    {err_html}{ms_err_html}
    <div class="ob-success" style="margin-bottom:20px;">
        <strong>Магазин:</strong> {html.escape(store_name)}
    </div>
    <form method="post" action="/onboarding/wizard/{html.escape(tenant_id)}/step3/{html.escape(session_id)}/{html.escape(store_id)}">
        <div class="field">
            <label>Название магазина</label>
            <input type="text" name="name" value="{html.escape(store_name)}" required />
            <span class="hint">Можно оставить или изменить вручную</span>
        </div>

        <div class="section-title">Настройки МойСклад</div>
        <div class="field"><label>Организация</label>{_select("ms_organization_id", ms_orgs)}</div>
        <div class="field"><label>Склад</label>{_select("ms_store_id", ms_stores)}</div>
        <div class="field"><label>Контрагент по умолчанию</label>{_select("ms_agent_id", ms_agents)}</div>

        <div class="section-title">Фискализация <span style="font-weight:400;color:#9ca3af;">(необязательно)</span></div>
        <div class="field"><label>Fiscal Token</label><input type="text" name="fiscal_token" placeholder="Оставьте пустым если не нужна" /></div>
        <div class="field"><label>Fiscal Client UID</label><input type="text" name="fiscal_client_uid" /></div>
        <div class="field"><label>Fiscal Device UID</label><input type="text" name="fiscal_device_uid" /></div>

        {"<input type='hidden' name='is_primary' value='1' />" if is_first else ""}
        <button type="submit" class="ob-btn" style="margin-top:8px;">Синхронизировать →</button>
    </form>"""

    return HTMLResponse(_ob_step_layout(3, 5, "Настройка магазина", body,
                        back_url=f"/onboarding/wizard/{tenant_id}/step2/{session_id}"))


@router.post("/onboarding/wizard/{tenant_id}/step3/{session_id}/{store_id}", response_class=HTMLResponse)
def wizard_step3_submit(
    tenant_id: str,
    session_id: str,
    store_id: str,
    name: str = Form(""),
    ms_organization_id: str = Form(...),
    ms_store_id: str = Form(...),
    ms_agent_id: str = Form(...),
    fiscal_token: str = Form(""),
    fiscal_client_uid: str = Form(""),
    fiscal_device_uid: str = Form(""),
    is_primary: str = Form("0"),
):
    from urllib.parse import quote_plus
    tenant = _load_tenant(tenant_id)
    session = _load_session(session_id)
    store = _get_session_store(session, store_id)
    if not store:
        return HTMLResponse(_ob_step_layout(3, 5, "Ошибка", '<div class="ob-error">Магазин не найден.</div>'))

    store_name = _extract_store_name(store, store_id)
    final_name = name.strip() or store_name
    now = int(time.time())

    # Сохраняем evotor_token в тенант если ещё не сохранён
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("UPDATE tenants SET evotor_token = ?, evotor_store_id = COALESCE(evotor_store_id, ?), updated_at = ? WHERE id = ?"),
            (session["evotor_token"], store_id, now, tenant_id),
        )

        # Создаём или обновляем tenant_store
        primary_val = 1 if is_primary == "1" else 0
        if primary_val:
            cur.execute(aq("UPDATE tenant_stores SET is_primary = 0 WHERE tenant_id = ?"), (tenant_id,))

        cur.execute(
            aq("""
            INSERT INTO tenant_stores
                (id, tenant_id, evotor_store_id, name, ms_store_id, ms_organization_id, ms_agent_id, is_primary, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (evotor_store_id) DO UPDATE SET
                tenant_id = EXCLUDED.tenant_id,
                name = EXCLUDED.name,
                ms_store_id = EXCLUDED.ms_store_id,
                ms_organization_id = EXCLUDED.ms_organization_id,
                ms_agent_id = EXCLUDED.ms_agent_id,
                is_primary = EXCLUDED.is_primary,
                updated_at = EXCLUDED.updated_at
            """),
            (str(uuid.uuid4()), tenant_id, store_id, final_name,
             ms_store_id.strip(), ms_organization_id.strip(), ms_agent_id.strip(),
             primary_val, now, now),
        )

        if fiscal_token.strip() and fiscal_client_uid.strip() and fiscal_device_uid.strip():
            cur.execute(
                aq("UPDATE tenants SET fiscal_token=?, fiscal_client_uid=?, fiscal_device_uid=? WHERE id=?"),
                (fiscal_token.strip(), fiscal_client_uid.strip(), fiscal_device_uid.strip(), tenant_id),
            )
        # Если это основной магазин — обновляем поля в tenants тоже
        if primary_val:
            cur.execute(
                aq("""UPDATE tenants SET
                    ms_organization_id = ?,
                    ms_store_id = ?,
                    ms_agent_id = ?,
                    evotor_store_id = ?,
                    updated_at = ?
                WHERE id = ?"""),
                (ms_organization_id.strip(), ms_store_id.strip(), ms_agent_id.strip(),
                 store_id, now, tenant_id),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return RedirectResponse(
            url=f"/onboarding/wizard/{tenant_id}/step3/{session_id}/{store_id}?err={quote_plus(str(e))}",
            status_code=303,
        )
    finally:
        conn.close()

    return RedirectResponse(
        url=f"/onboarding/wizard/{tenant_id}/step4/{session_id}/{store_id}",
        status_code=303,
    )


# Шаг 4 — синхронизация
@router.get("/onboarding/wizard/{tenant_id}/step4/{session_id}/{store_id}", response_class=HTMLResponse)
def wizard_step4(tenant_id: str, session_id: str, store_id: str):
    body = f"""
    <p style="color:#5b6475;font-size:14px;margin-bottom:20px;">
        Выполняем первичную синхронизацию товаров из Эвотор в МойСклад и передаём остатки.
    </p>
    <div id="syncStatus" style="text-align:center;padding:32px;">
        <div style="font-size:15px;font-weight:600;margin-bottom:8px;">Синхронизация...</div>
        <div style="font-size:13px;color:#6b7280;">Это может занять до 30 секунд</div>
    </div>
    <script>
    fetch('/onboarding/wizard/{tenant_id}/step4/{session_id}/{store_id}/run', {{method:'POST'}})
        .then(r => r.json())
        .then(data => {{
            const el = document.getElementById('syncStatus');
            const synced = data.synced || 0;
            const skipped = data.skipped || 0;
            const failed = data.failed || 0;
            const ok = data.status === 'ok';
            el.innerHTML = `
                <div style="font-size:15px;font-weight:600;color:${{ok ? '#059669' : '#dc2626'}};margin-bottom:16px;">
                    ${{ok ? '✓ Синхронизация завершена' : '⚠ Завершена с ошибками'}}
                </div>
                <div style="background:#f9fafb;border-radius:8px;padding:16px;text-align:left;margin-bottom:20px;">
                    <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #e5e7eb;">
                        <span style="color:#6b7280;">Товаров создано в МойСклад</span>
                        <strong>${{synced}}</strong>
                    </div>
                    <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #e5e7eb;">
                        <span style="color:#6b7280;">Уже существовали</span>
                        <strong>${{skipped}}</strong>
                    </div>
                    <div style="display:flex;justify-content:space-between;padding:6px 0;">
                        <span style="color:#6b7280;">Ошибок</span>
                        <strong style="color:${{failed > 0 ? '#dc2626' : 'inherit'}}">${{failed}}</strong>
                    </div>
                </div>
                <a href="/onboarding/wizard/{tenant_id}/step5/{session_id}/{store_id}"
                   style="display:block;background:#2458d3;color:#fff;text-align:center;padding:12px;border-radius:8px;text-decoration:none;font-weight:600;">
                   Далее →
                </a>`;
        }})
        .catch(err => {{
            document.getElementById('syncStatus').innerHTML =
                '<div class="ob-error">Ошибка при синхронизации: ' + err + '</div>';
        }});
    </script>"""
    return HTMLResponse(_ob_step_layout(4, 5, "Синхронизация товаров", body))


@router.post("/onboarding/wizard/{tenant_id}/step4/{session_id}/{store_id}/run")
def wizard_step4_run(tenant_id: str, session_id: str, store_id: str):
    from app.api.sync import initial_sync_store
    from app.api.vendor import _notify_ms_activated

    try:
        result = initial_sync_store(tenant_id, store_id)

        failed = result.get("failed", 0)
        if failed == 0:
            conn = get_connection()
            try:
                cur = conn.cursor()
                cur.execute(
                    aq("SELECT ms_account_id, moysklad_token FROM tenants WHERE id = ?"),
                    (tenant_id,),
                )
                t = cur.fetchone()
            finally:
                conn.close()

            if t and t["ms_account_id"] and t["moysklad_token"]:
                _notify_ms_activated(t["ms_account_id"])

        return result

    except Exception as e:
        log.exception("wizard_step4_run failed")
        return {"status": "error", "error": str(e), "synced": 0, "skipped": 0, "failed": 1}

# Шаг 5 — готово
@router.get("/onboarding/wizard/{tenant_id}/step5/{session_id}/{store_id}", response_class=HTMLResponse)
def wizard_step5(tenant_id: str, session_id: str, store_id: str):
    tenant = _load_tenant(tenant_id)
    session = _load_session(session_id)
    stores = json.loads(session.get("stores_json") or "[]")

    # Сколько магазинов ещё не добавлено
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(aq("SELECT evotor_store_id FROM tenant_stores WHERE tenant_id = ?"), (tenant_id,))
    added = {r["evotor_store_id"] for r in cur.fetchall()}
    conn.close()

    remaining = [s for s in stores if _extract_store_id(s) and _extract_store_id(s) not in added]

    add_store_btn = ""
    if remaining:
        add_store_btn = f"""
        <a href="/onboarding/wizard/{html.escape(tenant_id)}/step2/{html.escape(session_id)}"
           class="ob-btn" style="display:block;text-align:center;text-decoration:none;
                                  background:#f0f4ff;color:#2458d3;border:1px solid #c7d7f5;margin-bottom:12px;">
            + Добавить ещё магазин ({len(remaining)} доступно)
        </a>"""

    body = f"""
    <div style="text-align:center;margin-bottom:28px;">
        <div style="font-size:48px;margin-bottom:12px;">🎉</div>
        <div style="font-size:18px;font-weight:700;color:#1a1d2e;margin-bottom:8px;">Интеграция настроена!</div>
        <div style="font-size:14px;color:#6b7280;">Товары и остатки синхронизированы</div>
    </div>
    {add_store_btn}
    <a href="/onboarding/tenants/{html.escape(tenant_id)}"
       class="ob-btn" style="display:block;text-align:center;text-decoration:none;">
        Перейти в личный кабинет →
    </a>"""

    return HTMLResponse(_ob_step_layout(5, 5, "Готово", body))


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
            <a href="/static/help/evotor-token.html?back=/onboarding/evotor/connect" target="_blank"
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
    stores = json.loads(session.get("stores_json") or "[]")
    if not stores:
        return HTMLResponse(_ob_layout("Выбор магазина", '<div class="ob-error">Магазины не найдены.</div>'), status_code=400)

    parts = [
        '<p style="color:#5b6475;font-size:14px;margin-bottom:20px;line-height:1.6;">'
        'Выберите магазин Эвотор, для которого хотите создать профиль интеграции.'
        '</p>'
    ]

    for store in stores:
        store_id = _extract_store_id(store)
        if not store_id:
            continue
        store_name = _extract_store_name(store, store_id)
        link = f"/onboarding/evotor/sessions/{html.escape(session_id)}/stores/{html.escape(store_id)}/ms-token"
        parts.append(f"""
        <div class="store">
            <p style="font-size:16px;font-weight:700;margin-bottom:6px;">{html.escape(store_name)}</p>
            <p style="margin:0 0 14px;color:#8793a8;font-size:13px;">Подключение МойСклад и настройка привязки для этого магазина</p>
            <a href="{link}" class="ob-btn" style="display:inline-block;text-decoration:none;">Выбрать магазин →</a>
        </div>""")

    return HTMLResponse(_ob_layout("Выбор магазина", "".join(parts), back_url="/onboarding/evotor/connect"))


# ---------------------------------------------------------------------------
# Step 3
# ---------------------------------------------------------------------------

@router.get("/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token", response_class=HTMLResponse)
def onboarding_ms_token_form(session_id: str, store_id: str):
    session = _load_session(session_id)
    store = _get_session_store(session, store_id)
    if not store:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Выбранный магазин не найден в сессии.</div>'), status_code=404)

    store_name = _extract_store_name(store, store_id)
    body = f"""
    <div class="ob-success" style="margin-bottom:20px;">
        <strong>Выбран магазин:</strong> {html.escape(store_name)}
    </div>
    <p style="margin-bottom:20px;color:#5b6475;font-size:14px;line-height:1.6;">
        Теперь введите токен МойСклад. После этого мы загрузим организации, склады и контрагентов именно для этого магазина.
    </p>
    <form method="post" action="/onboarding/evotor/sessions/{html.escape(session_id)}/stores/{html.escape(store_id)}/ms-token">
        <div class="field">
            <label>MoySklad token</label>
            <input type="text" name="moysklad_token" required placeholder="Токен из раздела «Безопасность» → «Токены»" />
            <span class="hint">Система автоматически загрузит организации, склады и контрагентов.</span>
        </div>
        <button type="submit" class="ob-btn">Загрузить данные →</button>
    </form>"""
    return HTMLResponse(_ob_layout("Подключение МойСклад", body, back_url=f"/onboarding/evotor/sessions/{session_id}/stores"))


@router.post("/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token", response_class=HTMLResponse)
def onboarding_ms_token_submit(session_id: str, store_id: str, moysklad_token: str = Form(...)):
    session = _load_session(session_id)
    store = _get_session_store(session, store_id)
    if not store:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Выбранный магазин не найден в сессии.</div>'), status_code=404)

    store_name = _extract_store_name(store, store_id)
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
    <div class="ob-success"><strong>Выбран магазин:</strong> {html.escape(store_name)}</div>
    <div class="ob-success">Данные МойСклад загружены.</div>
    <form method="post" action="/onboarding/store-profile">
        <input type="hidden" name="session_id" value="{html.escape(session_id)}" />
        <input type="hidden" name="evotor_store_id" value="{html.escape(store_id)}" />

        <div class="field">
            <label>Название магазина</label>
            <input type="text" name="name" value="{html.escape(store_name)}" required placeholder="Название магазина" />
            <span class="hint">Можно оставить название из Эвотор или изменить вручную.</span>
        </div>

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
    return HTMLResponse(_ob_layout("Настройка магазина", body,
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
    session_store = _get_session_store(session, evotor_store_id)
    if not session_store:
        return HTMLResponse(
            _ob_layout("Ошибка", '<div class="ob-error">Выбранный магазин не найден в сессии. Начните заново.</div>'),
            status_code=400,
        )

    moysklad_token = session.get("moysklad_token", "").strip()
    if not moysklad_token:
        return HTMLResponse(
            _ob_layout("Ошибка", '<div class="ob-error">Сессия истекла. Начните заново.</div>'),
            status_code=400,
        )

    ms_data = json.loads(session.get("ms_data_json") or "{}")
    if ms_organization_id not in {i["id"] for i in ms_data.get("orgs", [])}:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Неверная организация.</div>'), status_code=400)
    if ms_store_id not in {i["id"] for i in ms_data.get("stores", [])}:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Неверный склад.</div>'), status_code=400)
    if ms_agent_id not in {i["id"] for i in ms_data.get("agents", [])}:
        return HTMLResponse(_ob_layout("Ошибка", '<div class="ob-error">Неверный контрагент.</div>'), status_code=400)

    store_name = _extract_store_name(session_store, evotor_store_id)
    final_name = (name or "").strip() or store_name
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
            aq("""
                INSERT INTO tenants (
                    id, name, evotor_api_key, moysklad_token, created_at,
                    evotor_token, evotor_store_id, ms_organization_id, ms_store_id, ms_agent_id,
                    alert_email, alerts_email_enabled, telegram_chat_id, alerts_telegram_enabled
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """),
            (
                tenant_id,
                final_name,
                "",
                moysklad_token,
                now,
                session["evotor_token"],
                evotor_store_id.strip(),
                ms_organization_id.strip(),
                ms_store_id.strip(),
                ms_agent_id.strip(),
                alert_email_value,
                alerts_email_enabled_value,
                None,
                0,
            ),
        )

        cur.execute(
            aq("""
                INSERT INTO tenant_stores (
                    id,
                    tenant_id,
                    evotor_store_id,
                    name,
                    ms_store_id,
                    ms_organization_id,
                    ms_agent_id,
                    is_primary,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT (evotor_store_id) DO UPDATE SET
                    tenant_id = EXCLUDED.tenant_id,
                    name = COALESCE(EXCLUDED.name, tenant_stores.name),
                    ms_store_id = COALESCE(EXCLUDED.ms_store_id, tenant_stores.ms_store_id),
                    ms_organization_id = COALESCE(EXCLUDED.ms_organization_id, tenant_stores.ms_organization_id),
                    ms_agent_id = COALESCE(EXCLUDED.ms_agent_id, tenant_stores.ms_agent_id),
                    is_primary = 1,
                    updated_at = EXCLUDED.updated_at
            """),
            (
                str(uuid.uuid4()),
                tenant_id,
                evotor_store_id.strip(),
                final_name or None,
                ms_store_id.strip() or None,
                ms_organization_id.strip() or None,
                ms_agent_id.strip() or None,
                now,
                now,
            ),
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

    tenant_after_sync = _load_tenant(tenant_id)
    tenant_sync_ts = tenant_after_sync.get("sync_completed_at")
    if tenant_sync_ts:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                aq("""
                    UPDATE tenant_stores
                    SET sync_completed_at = ?, updated_at = ?
                    WHERE tenant_id = ? AND evotor_store_id = ?
                """),
                (tenant_sync_ts, int(time.time()), tenant_id, evotor_store_id.strip()),
            )
            conn.commit()
        finally:
            conn.close()

    sync_html = _render_sync_result(sync_result)
    body = f"""
    <div class="ob-success"><strong>Профиль создан!</strong><br>Магазин: {html.escape(final_name)}</div>
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
        if not evotor_ready:
            return RedirectResponse(url=f"/onboarding/wizard/{tenant['id']}/step1", status_code=302)
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
def onboarding_tenant_evotor_form(tenant_id: str, err: str | None = None, msg: str | None = None):
    tenant = _load_tenant(tenant_id)
    evotor_ok = bool(tenant.get("evotor_token"))

    status_html = ""
    if msg:
        status_html = f'<div class="ob-success">{html.escape(msg)}</div>'
    elif err:
        status_html = f'<div class="ob-error">{html.escape(err)}</div>'
    elif evotor_ok:
        status_html = '<div class="ob-success">Эвотор подключён. Можно обновить токен.</div>'
    else:
        status_html = '<div class="ob-error" style="margin-bottom:16px;">Подключите кассу Эвотор для завершения настройки.</div>'

    body = f"""
    <style>
    .help-modal {{ display:none; position:fixed; inset:0; background:rgba(0,0,0,0.5);
        z-index:1000; align-items:flex-start; justify-content:center; overflow-y:auto; padding:20px; }}
    .help-modal.visible {{ display:flex; }}
    .help-modal-box {{ background:#fff; border-radius:14px; width:100%; max-width:700px;
        margin:auto; overflow:hidden; box-shadow:0 8px 40px rgba(0,0,0,0.2); }}
    .help-modal-header {{ display:flex; align-items:center; justify-content:space-between;
        padding:16px 20px; border-bottom:1px solid #e4e8f0; }}
    .help-modal-title {{ font-size:16px; font-weight:700; color:#1a1d2e; }}
    .help-modal-close {{ background:none; border:none; font-size:22px; cursor:pointer;
        color:#9ca3af; line-height:1; padding:0 4px; }}
    .help-modal-close:hover {{ color:#1a1d2e; }}
    .help-modal-content {{ padding:20px; max-height:75vh; overflow-y:auto; }}
    .help-step {{ margin-bottom:24px; }}
    .help-step-num {{ display:inline-flex; align-items:center; justify-content:center;
        width:28px; height:28px; border-radius:8px; background:#FF4D00; color:#fff;
        font-size:13px; font-weight:700; margin-bottom:10px; }}
    .help-step-title {{ font-size:15px; font-weight:700; color:#1a1d2e; margin-bottom:6px; }}
    .help-step-desc {{ font-size:13px; color:#6b7280; margin-bottom:10px; line-height:1.5; }}
    .help-step img {{ width:100%; border-radius:8px; border:1px solid #e4e8f0; }}
    .help-link {{ display:inline-flex; align-items:center; gap:6px; font-size:13px;
        color:#2458d3; text-decoration:none; cursor:pointer; background:none; border:none;
        padding:0; font-family:inherit; margin-top:6px; }}
    .help-link:hover {{ text-decoration:underline; }}
    </style>

    {status_html}
    <form method="post" action="/onboarding/tenants/{html.escape(tenant_id)}/evotor">
        <div class="field">
            <label>Evotor Token</label>
            <input type="text" name="evotor_token" required
                   placeholder="Токен из личного кабинета Эвотор" />
            <span class="hint">Найдите в evotor.ru → Мои приложения → Универсальный фискализатор → Настройки</span>
            <button type="button" class="help-link"
                    onclick="document.getElementById('helpModal').classList.add('visible')">
                📖 Где найти токен? Пошаговая инструкция
            </button>
        </div>
        <div class="field" style="margin-top:20px;">
            <label style="font-size:14px;font-weight:600;color:#374151;margin-bottom:10px;display:block;">
                Тип обновления
            </label>
            <div style="display:flex;flex-direction:column;gap:10px;">
                <label style="display:flex;align-items:flex-start;gap:10px;padding:12px;
                              border:2px solid #2458d3;border-radius:8px;cursor:pointer;background:#f0f4ff;"
                       id="label_a">
                    <input type="radio" name="account_type" value="same" id="choice_a" checked
                           style="width:auto;margin-top:2px;"
                           onchange="document.getElementById('label_a').style.borderColor='#2458d3';document.getElementById('label_a').style.background='#f0f4ff';document.getElementById('label_b').style.borderColor='#e5e7eb';document.getElementById('label_b').style.background='#fff';" />
                    <div>
                        <div style="font-weight:600;font-size:14px;color:#1a1d2e;">Тот же аккаунт Эвотор</div>
                        <div style="font-size:13px;color:#6b7280;margin-top:2px;">
                            Просто обновить токен. Магазины и данные синхронизации сохранятся.
                        </div>
                    </div>
                </label>
                <label style="display:flex;align-items:flex-start;gap:10px;padding:12px;
                              border:2px solid #e5e7eb;border-radius:8px;cursor:pointer;background:#fff;"
                       id="label_b">
                    <input type="radio" name="account_type" value="new" id="choice_b"
                           style="width:auto;margin-top:2px;"
                           onchange="document.getElementById('label_b').style.borderColor='#dc2626';document.getElementById('label_b').style.background='#fff5f5';document.getElementById('label_a').style.borderColor='#e5e7eb';document.getElementById('label_a').style.background='#fff';" />
                    <div>
                        <div style="font-weight:600;font-size:14px;color:#dc2626;">Новый аккаунт Эвотор</div>
                        <div style="font-size:13px;color:#6b7280;margin-top:2px;">
                            Удалить все магазины и начать заново. Все данные синхронизации будут сброшены.
                        </div>
                    </div>
                </label>
            </div>
        </div>
        <button type="submit" class="ob-btn" style="margin-top:8px;">Обновить токен →</button>
    </form>

    <div class="help-modal" id="helpModal">
        <div class="help-modal-box">
            <div class="help-modal-header">
                <div class="help-modal-title">Как найти токен Эвотор</div>
                <button class="help-modal-close"
                        onclick="document.getElementById('helpModal').classList.remove('visible')">×</button>
            </div>
            <div class="help-modal-content">
                <div class="help-step">
                    <div class="help-step-num">1</div>
                    <div class="help-step-title">Войдите в личный кабинет Эвотор</div>
                    <div class="help-step-desc">Откройте <strong>evotor.ru</strong> и войдите в аккаунт.</div>
                    <img src="/static/help/img/step1.png" alt="Шаг 1">
                </div>
                <div class="help-step">
                    <div class="help-step-num">2</div>
                    <div class="help-step-title">Найдите "Универсальный фискализатор"</div>
                    <div class="help-step-desc">В строке поиска введите <strong>«Универсальный фискализатор»</strong> и перейдите на вкладку <strong>Приложения</strong>.</div>
                    <img src="/static/help/img/step2.png" alt="Шаг 2">
                </div>
                <div class="help-step">
                    <div class="help-step-num">3</div>
                    <div class="help-step-title">Откройте страницу приложения</div>
                    <div class="help-step-desc">Нажмите на приложение и затем кнопку <strong>«Открыть»</strong>.</div>
                    <img src="/static/help/img/step3.png" alt="Шаг 3">
                </div>
                <div class="help-step">
                    <div class="help-step-num">4</div>
                    <div class="help-step-title">Установите приложение на кассу</div>
                    <div class="help-step-desc">На вкладке <strong>«Установка / Удаление»</strong> выберите кассу и нажмите <strong>«Установить»</strong>.</div>
                    <img src="/static/help/img/step4.png" alt="Шаг 4">
                </div>
                <div class="help-step">
                    <div class="help-step-num">5</div>
                    <div class="help-step-title">Откройте "Мои приложения"</div>
                    <div class="help-step-desc">В меню слева нажмите <strong>«Мои приложения»</strong> и откройте <strong>«Универсальный фискализатор»</strong>.</div>
                    <img src="/static/help/img/step5.png" alt="Шаг 5">
                </div>
                <div class="help-step">
                    <div class="help-step-num">6</div>
                    <div class="help-step-title">Скопируйте токен</div>
                    <div class="help-step-desc">На странице приложения найдите поле <strong>Evotor Token</strong> и скопируйте его значение.</div>
                    <img src="/static/help/img/step6.png" alt="Шаг 6">
                </div>
            </div>
        </div>
    </div>"""
    return HTMLResponse(_ob_layout("Обновление токена Эвотор", body,
                        back_url=f"/onboarding/tenants/{tenant_id}/integration"))


@router.post("/onboarding/tenants/{tenant_id}/evotor", response_class=HTMLResponse)
def onboarding_tenant_evotor_submit(
    tenant_id: str,
    evotor_token: str = Form(...),
    account_type: str = Form("same"),
):
    from urllib.parse import quote_plus
    from app.stores.mapping_store import MappingStore
    evotor_token = evotor_token.strip()
    if not evotor_token:
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/evotor?err=Токен+обязателен",
            status_code=303,
        )
    try:
        stores = fetch_stores_by_token(evotor_token)
    except Exception as exc:
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/evotor?err={quote_plus(f'Не удалось получить магазины: {exc}')}",
            status_code=303,
        )
    if not stores:
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/evotor?err=По+этому+токену+не+найдено+магазинов",
            status_code=303,
        )

    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()

        if account_type == "new":
            # Новый аккаунт — удаляем все магазины и маппинги
            ms_store = MappingStore()
            stores_list = _load_tenant_stores(tenant_id)
            for s in stores_list:
                ms_store.delete_by_store(tenant_id, s["evotor_store_id"])

            cur.execute(aq("DELETE FROM product_group_mappings WHERE tenant_id = ?"), (tenant_id,))
            cur.execute(aq("DELETE FROM tenant_stores WHERE tenant_id = ?"), (tenant_id,))
            cur.execute(
                aq("UPDATE tenants SET evotor_token = ?, evotor_store_id = NULL, sync_completed_at = NULL, updated_at = ? WHERE id = ?"),
                (evotor_token, now, tenant_id),
            )
            log.info("evotor_submit: new account — cleared all stores and mappings tenant_id=%s", tenant_id)
        else:
            # Тот же аккаунт — просто обновляем токен
            cur.execute(
                aq("UPDATE tenants SET evotor_token = ?, updated_at = ? WHERE id = ?"),
                (evotor_token, now, tenant_id),
            )
            log.info("evotor_submit: updated token tenant_id=%s", tenant_id)

        conn.commit()
    finally:
        conn.close()

    # Создаём сессию и редиректим на выбор магазина
    session_id = str(uuid.uuid4())
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

    return RedirectResponse(
        url=f"/onboarding/wizard/{tenant_id}/step2/{session_id}",
        status_code=303,
    )


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
    final_name = (store_name or "").strip() or (tenant.get("name") or "Магазин Эвотор")

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

        cur.execute(
            aq("""
                INSERT INTO tenant_stores (
                    id, tenant_id, evotor_store_id, name,
                    ms_store_id, ms_organization_id, ms_agent_id,
                    is_primary, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT (evotor_store_id) DO UPDATE SET
                    tenant_id = EXCLUDED.tenant_id,
                    name = COALESCE(EXCLUDED.name, tenant_stores.name),
                    ms_store_id = COALESCE(EXCLUDED.ms_store_id, tenant_stores.ms_store_id),
                    ms_organization_id = COALESCE(EXCLUDED.ms_organization_id, tenant_stores.ms_organization_id),
                    ms_agent_id = COALESCE(EXCLUDED.ms_agent_id, tenant_stores.ms_agent_id),
                    is_primary = 1,
                    updated_at = EXCLUDED.updated_at
            """),
            (
                str(uuid.uuid4()),
                tenant_id,
                store_id,
                final_name,
                ms_store_id.strip() or None,
                ms_organization_id.strip() or None,
                ms_agent_id.strip() or None,
                now,
                now,
            ),
        )
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

    tenant_after_sync = _load_tenant(tenant_id)
    tenant_sync_ts = tenant_after_sync.get("sync_completed_at")
    if tenant_sync_ts:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                aq("""
                    UPDATE tenant_stores
                    SET sync_completed_at = ?, updated_at = ?
                    WHERE tenant_id = ? AND evotor_store_id = ?
                """),
                (tenant_sync_ts, int(time.time()), tenant_id, store_id),
            )
            conn.commit()
        finally:
            conn.close()

    sync_html = _render_sync_result(sync_result)

    body = f"""
    <div class="ob-success"><strong>Эвотор успешно подключён!</strong><br>
    Магазин: {html.escape(final_name)}</div>
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
    tenant, event_counts, mappings_count, store_mappings_count, stock_row, ms_products_count, last_event = _get_lk_data(tenant_id)
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

    new_products_banner = ""
    if ms_products_count > mappings_count:
        diff = ms_products_count - mappings_count
        new_products_banner = f'''
            <div class="lk-row" style="background:#fffbeb;margin:0 -24px;padding:10px 24px;border-radius:0;">
                <span class="lk-row-label" style="color:#92400e;">
                    ℹ️ Не выгружено в Эвотор: {diff} товар(ов). Обычно причина — нет остатка на связанных складах или товар не проходит фильтр синхронизации.
                </span>
                <form method="post" action="/onboarding/tenants/{tenant_id}/sync-ms-to-evotor" style="margin:0;">
                    <button type="submit" class="btn btn-outline" style="padding:5px 12px;font-size:12px;">
                        Синхронизировать →
                    </button>
                </form>
            </div>
        '''
        
    # Баннер удалённых товаров
    deleted_products_banner = ""
    if ms_products_count > 0 and mappings_count > ms_products_count:
        diff = mappings_count - ms_products_count
        deleted_products_banner = f'''<div class="lk-row" style="background:#fef2f2;margin:0 -24px;padding:10px 24px;border-radius:0;">
            <span class="lk-row-label" style="color:#991b1b;">🗑️ Удалённых товаров в маппинге: {diff}</span>
            <form method="post" action="/onboarding/tenants/{tenant_id}/cleanup-stale-mappings" style="margin:0;"
                  onsubmit="
                    var btn = this.querySelector('button');
                    btn.disabled = true;
                    btn.textContent = '⏳ Синхронизация...';
                    document.getElementById('syncOverlay') && document.getElementById('syncOverlay').classList.add('visible');
                    var lbl = document.getElementById('syncLabel');
                    if(lbl) lbl.textContent = 'Удаляем устаревшие маппинги...';
                  ">
                <button type="submit" class="btn btn-outline" style="padding:5px 12px;font-size:12px;color:#dc2626;border-color:#dc2626;">
                    Очистить →
                </button>
            </form>
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
            <span class="lk-row-label">Товаров с остатком выгружено в Эвотор</span>
            <span class="lk-row-value">{store_mappings_count} связи / {mappings_count}{"/" + str(ms_products_count) if ms_products_count else ""} товаров МС</span>
        </div>
        {new_products_banner}
        {deleted_products_banner}
        {stock_badge}
        {last_event_html}
        <div class="lk-row"><span class="lk-row-label">Telegram</span>{_badge(tg_ok, "Подключён", "Не подключён")}</div>
        <div class="lk-row"><span class="lk-row-label">Email</span>{_badge(email_ok, html.escape(tenant.get("alert_email") or "Активен"), "Не настроен")}</div>
    </div>
    <div class="lk-card" style="margin-top:16px;">
        <div class="lk-card-title">События</div>
        <div class="stats-grid">
            <div class="stat-card"><div class="stat-value" style="color:#059669;">{done}</div><div class="stat-label">Обработано</div></div>
            <div class="stat-card"><div class="stat-value">{new_ev}</div><div class="stat-label">В очереди</div></div>
            <div class="stat-card"><div class="stat-value" style="color:{'#d97706' if retry > 0 else '#1a1d2e'};">{retry}</div><div class="stat-label">Повтор</div></div>
            <div class="stat-card"><div class="stat-value" style="color:{'#dc2626' if failed > 0 else '#1a1d2e'};">{failed}</div><div class="stat-label">Ошибки</div></div>
        </div>"""
    content += """
        <div class="sync-overlay" id="syncOverlay">
            <div class="sync-spinner"></div>
            <div class="sync-label" id="syncLabel">Синхронизация...</div>
            <div class="sync-sublabel">Это может занять до 30 секунд</div>
        </div>
"""

    # Блок магазинов
    stores = _load_tenant_stores(tenant_id)
    if stores:
        stores_rows = ""
        for s in stores:
            sname = html.escape(s.get("name") or s["evotor_store_id"])
            sid = html.escape(s["evotor_store_id"])
            sync_ts = _format_ts(s.get("sync_completed_at"))
            primary_badge = '<span class="badge-ok" style="font-size:11px;margin-left:6px;">основной</span>' if s["is_primary"] else ""
            sync_badge = f'<span class="badge-ok">Синхр. {sync_ts}</span>' if s.get("sync_completed_at") else '<span class="badge-warn">Не синхронизирован</span>'
            stores_rows += f"""
            <div class="lk-row" style="flex-wrap:wrap;gap:8px;">
                <div style="display:flex;align-items:center;gap:6px;flex:1;min-width:0;">
                    <span class="lk-row-label" style="font-weight:600;">{sname}</span>
                    {primary_badge}
                </div>
                <div style="display:flex;align-items:center;gap:8px;">
                    {sync_badge}
                    <a href="/onboarding/tenants/{html.escape(tenant_id)}/stores/{sid}"
                       class="btn btn-outline" style="padding:4px 12px;font-size:12px;">Управлять →</a>
                </div>
            </div>"""

        content += f"""
    <div class="lk-card" style="margin-top:16px;">
        <div class="lk-card-title" style="display:flex;justify-content:space-between;align-items:center;">
            <span>Магазины</span>
            <a href="/onboarding/tenants/{html.escape(tenant_id)}/stores"
               style="font-size:12px;font-weight:500;color:#3b6ff5;text-decoration:none;">Все магазины →</a>
        </div>
        {stores_rows}
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
def lk_actions(tenant_id: str, msg: str | None = None, err: str | None = None):
    tenant = _load_tenant(tenant_id)
    tid = html.escape(tenant_id)

    content = f"""
    <style>
    .lk-actions-list {{
        display: flex;
        flex-direction: column;
        gap: 12px;
        margin-top: 6px;
    }}

    .lk-action-form {{
        margin: 0;
    }}

    .lk-action-button {{
        width: 100%;
        border: 1px solid #e4e8f0;
        background: #fff;
        border-radius: 14px;
        padding: 14px 16px;
        display: flex;
        align-items: center;
        gap: 14px;
        text-align: left;
        cursor: pointer;
        transition: all 0.15s ease;
    }}

    .lk-action-button:hover {{
        border-color: #d7ddea;
        box-shadow: 0 2px 8px rgba(15, 23, 42, 0.05);
        background: #fbfcff;
    }}

    .lk-action-icon {{
        width: 40px;
        height: 40px;
        min-width: 40px;
        border-radius: 12px;
        background: #eef3ff;
        color: #3b6ff5;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 18px;
        font-weight: 700;
    }}

    .lk-action-content {{
        flex: 1;
        min-width: 0;
    }}

    .lk-action-title {{
        display: block;
        font-size: 15px;
        font-weight: 700;
        color: #1a1d2e;
        line-height: 1.25;
        margin-bottom: 3px;
    }}

    .lk-action-desc {{
        display: block;
        font-size: 13px;
        color: #6b7280;
        line-height: 1.45;
    }}

    .lk-action-arrow {{
        color: #3b6ff5;
        font-size: 20px;
        font-weight: 700;
        line-height: 1;
        margin-left: 8px;
    }}

    .lk-recovery-panel {{
        border: 1px solid #f2dfb1;
        background: #fffaf0;
        border-radius: 14px;
        padding: 16px;
        margin-top: 2px;
    }}

    .lk-recovery-title {{
        font-size: 15px;
        font-weight: 700;
        color: #1a1d2e;
        margin-bottom: 6px;
    }}

    .lk-recovery-text {{
        font-size: 13px;
        color: #9a6700;
        line-height: 1.5;
        margin-bottom: 12px;
    }}

    .lk-recovery-actions {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-bottom: 8px;
    }}

    .lk-btn-outline {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        height: 38px;
        padding: 0 16px;
        border-radius: 10px;
        border: 1px solid #3b6ff5;
        background: #fff;
        color: #3b6ff5;
        font-size: 14px;
        font-weight: 600;
        text-decoration: none;
        cursor: pointer;
        transition: all 0.15s ease;
    }}

    .lk-btn-outline:hover {{
        background: #f7faff;
    }}

    .lk-btn-soft {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        height: 38px;
        padding: 0 16px;
        border-radius: 10px;
        border: 1px solid #e5e7eb;
        background: #f3f4f6;
        color: #1f2937;
        font-size: 14px;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.15s ease;
    }}

    .lk-btn-soft:hover {{
        background: #eaecef;
    }}

    .lk-recovery-note {{
        font-size: 12px;
        color: #a16207;
        line-height: 1.45;
    }}

    @media (max-width: 720px) {{
        .lk-action-button {{
            padding: 13px 14px;
            gap: 12px;
        }}

        .lk-action-icon {{
            width: 36px;
            height: 36px;
            min-width: 36px;
            font-size: 16px;
            border-radius: 10px;
        }}

        .lk-action-title {{
            font-size: 14px;
        }}

        .lk-action-desc {{
            font-size: 12px;
        }}

        .lk-recovery-panel {{
            padding: 14px;
        }}

        .lk-recovery-actions {{
            flex-direction: column;
            align-items: stretch;
        }}

        .lk-btn-outline,
        .lk-btn-soft {{
            width: 100%;
        }}
    }}
    </style>

    <div class="lk-card">
    <div class="lk-card-title">СИНХРОНИЗАЦИЯ</div>

    
<style id="lk-actions-clean-style">
.lk-actions-list {{
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin-top: 6px;
}}

.lk-action-form {{
    margin: 0;
}}

.lk-action-button {{
    width: 100%;
    border: 1px solid #e4e8f0;
    background: #fff;
    border-radius: 14px;
    padding: 14px 16px;
    display: flex;
    align-items: center;
    gap: 14px;
    text-align: left;
    cursor: pointer;
    transition: all 0.15s ease;
}}

.lk-action-button:hover {{
    border-color: #d7ddea;
    box-shadow: 0 2px 8px rgba(15, 23, 42, 0.05);
    background: #fbfcff;
}}

.lk-action-icon {{
    width: 40px;
    height: 40px;
    min-width: 40px;
    border-radius: 12px;
    background: #eef3ff;
    color: #3b6ff5;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 18px;
    font-weight: 700;
}}

.lk-action-content {{
    flex: 1;
    min-width: 0;
}}

.lk-action-title {{
    display: block;
    font-size: 15px;
    font-weight: 700;
    color: #1a1d2e;
    line-height: 1.25;
    margin-bottom: 3px;
}}

.lk-action-desc {{
    display: block;
    font-size: 13px;
    color: #6b7280;
    line-height: 1.45;
    font-weight: 500;
}}

.lk-action-arrow {{
    color: #3b6ff5;
    font-size: 20px;
    font-weight: 700;
    line-height: 1;
    margin-left: 8px;
}}

.lk-recovery-panel {{
    border: 1px solid #f2dfb1;
    background: #fffaf0;
    border-radius: 14px;
    padding: 16px;
    margin-top: 2px;
}}

.lk-recovery-title {{
    font-size: 15px;
    font-weight: 700;
    color: #1a1d2e;
    margin-bottom: 6px;
}}

.lk-recovery-text {{
    font-size: 13px;
    color: #9a6700;
    line-height: 1.5;
    margin-bottom: 12px;
}}

.lk-recovery-actions {{
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin-bottom: 8px;
}}

.lk-btn-outline {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    height: 38px;
    padding: 0 16px;
    border-radius: 10px;
    border: 1px solid #3b6ff5;
    background: #fff;
    color: #3b6ff5;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
}}

.lk-btn-soft {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    height: 38px;
    padding: 0 16px;
    border-radius: 10px;
    border: 1px solid #e5e7eb;
    background: #f3f4f6;
    color: #1f2937;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
}}

.lk-recovery-note {{
    font-size: 12px;
    color: #a16207;
    line-height: 1.45;
}}
</style>

<div class="lk-actions-list">

        <form method="post" action="/onboarding/tenants/{tenant_id}/sync" class="lk-action-form">
            <button type="submit" class="lk-action-button">
                <span class="lk-action-icon">↻</span>
                <span class="lk-action-content">
                    <span class="lk-action-title">Повторная синхронизация товаров</span>
                    <span class="lk-action-desc">Повторно выгрузить связанные товары из МойСклад в Эвотор.</span>
                </span>
                <span class="lk-action-arrow">→</span>
            </button>
        </form>

        <form method="post" action="/onboarding/tenants/{tenant_id}/reconcile" class="lk-action-form">
            <button type="submit" class="lk-action-button">
                <span class="lk-action-icon">◇</span>
                <span class="lk-action-content">
                    <span class="lk-action-title">Синхронизировать остатки</span>
                    <span class="lk-action-desc">Обновить остатки товаров в Эвотор по данным МойСклад.</span>
                </span>
                <span class="lk-action-arrow">→</span>
            </button>
        </form>

        <form method="post" action="/onboarding/tenants/{tenant_id}/sync-ms-to-evotor" class="lk-action-form">
            <button type="submit" class="lk-action-button">
                <span class="lk-action-icon">＋</span>
                <span class="lk-action-content">
                    <span class="lk-action-title">Синхронизировать новые товары из МойСклад</span>
                    <span class="lk-action-desc">Добавить в Эвотор товары, которых ещё нет в кассе.</span>
                </span>
                <span class="lk-action-arrow">→</span>
            </button>
        </form>

        <div class="lk-recovery-panel">
            <div class="lk-recovery-title">Точки восстановления товаров</div>
            <div class="lk-recovery-text">
                Перед тестовой синхронизацией можно сохранить текущие карточки товаров Эвотор.
                Если синхронизация изменит карточки неправильно, их можно откатить из последней точки.
                Остатки при откате не перетираются.
            </div>

            <div class="lk-recovery-actions">
                <form method="post" action="/onboarding/tenants/{tenant_id}/product-snapshot" style="margin:0;">
                    <button type="submit" class="lk-btn-outline">Создать точку восстановления товаров</button>
                </form>

                <form method="post" action="/onboarding/tenants/{tenant_id}/product-rollback-latest" style="margin:0;">
                    <button type="submit" class="lk-btn-soft">Откатить карточки из последней точки</button>
                </form>
            </div>

            <div class="lk-recovery-note">
                После отката нажмите «Синхронизировать остатки», чтобы подтянуть актуальные остатки из МойСклад.
            </div>
        </div>

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


    

    """

    return _lk_layout(tenant, "actions", content, info_message=msg, error_message=err)

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
    _reply_in_telegram(chat_id, (
    "✅ Telegram успешно подключён к вашему магазину!\n\n"
    "Я буду присылать уведомления когда:\n"
    "• Продажа с кассы не смогла создать отгрузку в МойСклад\n"
    "• Синхронизация остатков завершилась с ошибкой\n"
    "• Появились товары требующие внимания\n\n"
    "Если всё работает штатно — я молчу 🤫\n"
    "Удачных продаж! 🛒"
))
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
    from app.stores.mapping_store import MappingStore
    from urllib.parse import quote_plus
    # Сбрасываем sync для всех магазинов
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("UPDATE tenants SET sync_completed_at = NULL WHERE id = ?"), (tenant_id,))
        cur.execute(
            aq("UPDATE tenant_stores SET sync_completed_at = NULL, updated_at = ? WHERE tenant_id = ?"),
            (int(time.time()), tenant_id),
        )
        conn.commit()
    finally:
        conn.close()
    # Не удаляем mappings перед повторной синхронизацией.
    # Повторный sync должен только добирать/обновлять связи, но не ломать рабочие mappings
    # при частичной ошибке или rate limit 429 от МойСклад.
    # Запускаем синхронизацию
    result = _run_initial_sync(tenant_id)
    synced = result.get("synced", 0)
    failed = result.get("failed", 0)
    msg = f"Синхронизация завершена: связей восстановлено — {synced}, ошибок — {failed}."
    if failed > 0:
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/actions?err={quote_plus(msg)}",
            status_code=303,
        )
    return RedirectResponse(
        url=f"/onboarding/tenants/{tenant_id}/actions?msg={quote_plus(msg)}",
        status_code=303,
    )


@router.post("/onboarding/tenants/{tenant_id}/reconcile", response_class=HTMLResponse)
def onboarding_tenant_reconcile(tenant_id: str):
    from app.api.sync import reconcile_stock_store
    from urllib.parse import quote_plus
    stores = _load_tenant_stores(tenant_id)
    total_synced = total_failed = 0
    for store in stores:
        if not store.get("sync_completed_at"):
            continue
        try:
            result = reconcile_stock_store(tenant_id, store["evotor_store_id"])
            total_synced += result.get("synced", 0)
            total_failed += result.get("failed", 0)
        except Exception as e:
            log.error("reconcile store=%s err=%s", store["evotor_store_id"], e)
            total_failed += 1
    msg = f"Остатки синхронизированы: товаров — {total_synced}, ошибок — {total_failed}."
    if total_failed > 0:
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/actions?err={quote_plus(msg)}",
            status_code=303,
        )
    return RedirectResponse(
        url=f"/onboarding/tenants/{tenant_id}/actions?msg={quote_plus(msg)}",
        status_code=303,
    )


@router.post("/onboarding/tenants/{tenant_id}/sync-ms-to-evotor", response_class=HTMLResponse)
def onboarding_tenant_sync_ms_to_evotor(tenant_id: str):
    from app.api.sync import sync_ms_to_evotor_store
    from urllib.parse import quote_plus
    stores = _load_tenant_stores(tenant_id)

    total_synced = 0
    total_failed = 0
    total_products_total = 0
    total_products_with_stock = 0
    total_products_without_stock = 0
    total_stock_check_failed = 0

    for store in stores:
        try:
            result = sync_ms_to_evotor_store(tenant_id, store["evotor_store_id"])
            total_synced += int(result.get("synced", 0) or 0)
            total_failed += int(result.get("failed", 0) or 0)

            products_total = int(result.get("products_total", result.get("checked_products", 0)) or 0)
            products_with_stock = int(result.get("products_with_stock", 0) or 0)
            products_without_stock = int(
                result.get(
                    "products_without_stock",
                    max(products_total - products_with_stock, 0),
                ) or 0
            )

            total_products_total += products_total
            total_products_with_stock += products_with_stock
            total_products_without_stock += products_without_stock
            total_stock_check_failed += int(result.get("stock_check_failed", 0) or 0)

        except Exception as e:
            log.error("sync_ms_to_evotor store=%s err=%s", store["evotor_store_id"], e)
            total_failed += 1

    msg = (
        f"МС→Эвотор: товаров с остатком синхронизировано — "
        f"{total_products_with_stock}/{total_products_total}; "
        f"без остатка на складе — {total_products_without_stock}; "
        f"добавлено/обновлено — {total_synced}; ошибок — {total_failed}."
    )
    if total_stock_check_failed > 0:
        msg += f" Не удалось проверить остаток — {total_stock_check_failed}."
    if total_failed > 0:
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/actions?err={quote_plus(msg)}",
            status_code=303,
        )
    return RedirectResponse(
        url=f"/onboarding/tenants/{tenant_id}/actions?msg={quote_plus(msg)}",
        status_code=303,
    )


@router.get("/onboarding/tenants/{tenant_id}/stores", response_class=HTMLResponse)
def lk_stores(tenant_id: str, msg: str | None = None, err: str | None = None):
    _ensure_primary_store_row(tenant_id)
    tenant = _load_tenant(tenant_id)
    stores = _load_tenant_stores(tenant_id)
    tid = html.escape(tenant_id)

    # Загружаем данные МойСклад
    ms_orgs = []
    ms_stores_list = []
    ms_agents = []
    ms_load_error = ""
    ms_store_name_map = {}

    if tenant.get("moysklad_token"):
        try:
            ms_orgs, ms_stores_list, ms_agents = _ms_fetch_all(tenant["moysklad_token"])
            ms_store_name_map = {
                str(item["id"]): item["name"]
                for item in ms_stores_list
                if item.get("id")
            }
        except Exception as e:
            ms_load_error = str(e)

    # Загружаем магазины Эвотор
    evotor_available_stores = []
    evotor_load_error = ""
    evotor_name_map = {}

    try:
        evotor_token = (tenant.get("evotor_token") or tenant.get("evotor_api_key") or "").strip()
        if evotor_token:
            raw_evotor_stores = fetch_stores_by_token(evotor_token) or []

            linked_ids = {
                str(row.get("evotor_store_id") or "").strip()
                for row in stores
                if row.get("evotor_store_id")
            }

            for item in raw_evotor_stores:
                store_id = _extract_store_id(item)
                if not store_id:
                    continue

                store_name = _extract_store_name(item, store_id)
                evotor_name_map[store_id] = store_name

                if store_id not in linked_ids:
                    evotor_available_stores.append({
                        "id": store_id,
                        "name": store_name,
                    })
    except Exception as e:
        log.exception("Failed to load Evotor stores for tenant_id=%s", tenant_id)
        evotor_load_error = str(e)

    # Карточки уже привязанных магазинов
    rows_parts = []
    for s in stores:
        sid_raw = s["evotor_store_id"]
        sid = html.escape(sid_raw)

        store_display_name = html.escape(
            s.get("name")
            or evotor_name_map.get(sid_raw)
            or "Магазин Эвотор"
        )

        ms_store_display = html.escape(
            ms_store_name_map.get(str(s.get("ms_store_id") or ""), "Склад не выбран")
        )

        sync_ts = _format_ts(s.get("sync_completed_at"))
        primary_badge = (
            '<span class="badge-ok" style="font-size:11px;margin-left:6px;">основной</span>'
            if s["is_primary"] else ""
        )
        sync_badge = (
            f'<span class="badge-ok">Синхронизирован {sync_ts}</span>'
            if s.get("sync_completed_at")
            else '<span class="badge-warn">Не синхронизирован</span>'
        )

        rows_parts.append(
            '<div class="lk-card" style="margin-bottom:12px;">'
            '<div style="display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap;">'
            '<div>'
            f'<div style="font-size:15px;font-weight:700;color:#1a1d2e;">{store_display_name}{primary_badge}</div>'
            f'<div style="font-size:12px;color:#9ca3af;margin-top:2px;">Склад МойСклад: {ms_store_display}</div>'
            '</div>'
            '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">'
            f'{sync_badge}'
            f'<a href="/onboarding/tenants/{tid}/stores/{sid}" class="btn btn-primary" style="padding:6px 14px;font-size:13px;">Управлять &rarr;</a>'
            '</div></div></div>'
        )

    rows_html = ''.join(rows_parts)

    def build_select(field_name, items, placeholder):
        if not items:
            return f'<input class="form-input" name="{field_name}" placeholder="{placeholder}" />'

        options = '<option value="">— выберите —</option>'
        for item in items:
            item_id = html.escape(item["id"])
            item_name = html.escape(item["name"])
            options += f'<option value="{item_id}">{item_name}</option>'

        return f'<select class="form-input" name="{field_name}">{options}</select>'

    def build_evotor_store_select(items):
        if not items:
            return (
                '<div class="form-hint" style="color:#92400e;">'
                'Нет доступных магазинов Эвотор для привязки. Возможно, все магазины уже добавлены.'
                '</div>'
            )

        options = '<option value="">— выберите магазин —</option>'
        for item in items:
            sid = html.escape(item["id"])
            sname = html.escape(item["name"])
            options += f'<option value="{sid}" data-store-name="{sname}">{sname}</option>'

        return (
            '<select class="form-input" name="evotor_store_id" id="evotor_store_id" required '
            'onchange="document.getElementById(\'store_name_input\').value=this.options[this.selectedIndex].dataset.storeName || \'\';">'
            + options +
            '</select>'
        )

    ms_error_html = (
        f'<div class="alert-box alert-warning">Не удалось загрузить данные МойСклад: {html.escape(ms_load_error)}</div>'
        if ms_load_error else ""
    )

    evotor_error_html = (
        f'<div class="alert-box alert-warning">Не удалось загрузить магазины Эвотор: {html.escape(evotor_load_error)}</div>'
        if evotor_load_error else ""
    )

    add_form_parts = [
        '<div class="lk-card" id="addStoreCard" style="margin-top:8px;display:none;">',
        '<div class="lk-card-title">Добавить магазин</div>',
        evotor_error_html,
        ms_error_html,
        f'<form method="post" action="/onboarding/tenants/{tid}/stores/add">',
        '<div style="display:flex;flex-direction:column;gap:12px;">',

        '<div class="form-field">',
        '<label class="form-label">Магазин Эвотор <span style="color:#dc2626;">*</span></label>',
        build_evotor_store_select(evotor_available_stores),
        '<span class="form-hint">Показываются магазины, доступные по токену этого тенанта и ещё не привязанные в системе</span>',
        '</div>',

        '<div class="form-field">',
        '<label class="form-label">Название магазина</label>',
        '<input class="form-input" id="store_name_input" name="name" placeholder="Название подтянется автоматически" />',
        '<span class="form-hint">Можно оставить как есть или изменить вручную</span>',
        '</div>',

        '<div class="form-field">',
        '<label class="form-label">Организация МойСклад</label>',
        build_select("ms_organization_id", ms_orgs, "UUID организации"),
        '</div>',

        '<div class="form-field">',
        '<label class="form-label">Склад МойСклад</label>',
        build_select("ms_store_id", ms_stores_list, "UUID склада"),
        '</div>',

        '<div class="form-field">',
        '<label class="form-label">Контрагент по умолчанию</label>',
        build_select("ms_agent_id", ms_agents, "UUID контрагента"),
        '</div>',

        '<div style="display:flex;align-items:center;gap:10px;">',
        '<input type="checkbox" name="is_primary" id="is_primary_new" style="width:auto;cursor:pointer;" />',
        '<label for="is_primary_new" style="font-size:14px;color:#374151;cursor:pointer;">Сделать основным магазином</label>',
        '</div>',

        '<div style="display:flex;gap:10px;margin-top:4px;">',
        '<button type="submit" class="btn btn-primary">Добавить магазин</button>',
        '<button type="button" class="btn btn-outline" onclick="document.getElementById(\'addStoreCard\').style.display=\'none\';document.getElementById(\'addStoreBtn\').style.display=\'\';">Отмена</button>',
        '</div>',

        '</div></form></div>',
    ]
    add_form = ''.join(add_form_parts)

    add_btn = (
        '<button id="addStoreBtn" class="btn btn-primary" '
        'onclick="this.style.display=\'none\';document.getElementById(\'addStoreCard\').style.display=\'\';">'
        'Добавить магазин &rarr;</button>'
    )

    content = (
        '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">'
        f'<div style="font-size:13px;color:#6b7280;">Всего магазинов: {len(stores)}</div>'
        + add_btn +
        '</div>'
        + rows_html
        + add_form
    )

    return _lk_layout(tenant, "stores", content, info_message=msg, error_message=err)


@router.get("/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}", response_class=HTMLResponse)
def lk_store_detail(
    tenant_id: str,
    evotor_store_id: str,
    msg: str | None = None,
    err: str | None = None,
):
    _ensure_primary_store_row(tenant_id)
    tenant = _load_tenant(tenant_id)
    stores = _load_tenant_stores(tenant_id)
    store = next((s for s in stores if s["evotor_store_id"] == evotor_store_id), None)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    tid = html.escape(tenant_id)
    sid = html.escape(evotor_store_id)
    sync_ok = bool(store.get("sync_completed_at"))

    ms_store_name_map = {}
    ms_org_name_map = {}
    ms_agent_name_map = {}

    if tenant.get("moysklad_token"):
        try:
            ms_orgs, ms_stores_list, ms_agents = _ms_fetch_all(tenant["moysklad_token"])
            ms_store_name_map = {
                str(item["id"]): item["name"]
                for item in ms_stores_list
                if item.get("id")
            }
            ms_org_name_map = {
                str(item["id"]): item["name"]
                for item in ms_orgs
                if item.get("id")
            }
            ms_agent_name_map = {
                str(item["id"]): item["name"]
                for item in ms_agents
                if item.get("id")
            }
        except Exception as e:
            log.warning("Failed to load MS names for store detail tenant_id=%s err=%s", tenant_id, e)

    evotor_name_map = {}
    try:
        evotor_token = (tenant.get("evotor_token") or tenant.get("evotor_api_key") or "").strip()
        if evotor_token:
            raw_evotor_stores = fetch_stores_by_token(evotor_token) or []
            for item in raw_evotor_stores:
                store_id = _extract_store_id(item)
                if store_id:
                    evotor_name_map[store_id] = _extract_store_name(item, store_id)
    except Exception as e:
        log.warning("Failed to load Evotor names for store detail tenant_id=%s err=%s", tenant_id, e)

    store_display_name = html.escape(
        store.get("name")
        or evotor_name_map.get(evotor_store_id)
        or "Магазин Эвотор"
    )

    ms_store_display = html.escape(
        ms_store_name_map.get(str(store.get("ms_store_id") or ""), "Склад не выбран")
    )
    ms_org_display = html.escape(
        ms_org_name_map.get(str(store.get("ms_organization_id") or ""), "Организация не выбрана")
    )
    ms_agent_display = html.escape(
        ms_agent_name_map.get(str(store.get("ms_agent_id") or ""), "Контрагент не выбран")
    )

    store_switcher = ""
    if len(stores) > 1:
        options_parts = []
        for s in stores:
            option_value = html.escape(s["evotor_store_id"])
            option_name = html.escape(
                s.get("name")
                or evotor_name_map.get(s["evotor_store_id"])
                or "Магазин Эвотор"
            )
            selected = ' selected' if s["evotor_store_id"] == evotor_store_id else ""
            options_parts.append(
                f'<option value="{option_value}"{selected}>{option_name}</option>'
            )

        store_switcher = (
            '<div style="margin-bottom:20px;">'
            '<label style="font-size:13px;font-weight:600;color:#374151;display:block;margin-bottom:6px;">Магазин</label>'
            f'<select data-base="/onboarding/tenants/{tid}/stores/" '
            'onchange="window.location=this.dataset.base + this.value" '
            'style="padding:8px 12px;border:1.5px solid #e4e8f0;border-radius:8px;font-size:14px;'
            'font-family:inherit;color:#1a1d2e;background:#fff;cursor:pointer;width:100%;max-width:400px;">'
            + ''.join(options_parts) +
            '</select></div>'
        )

    set_primary_btn = ""
    if not store["is_primary"]:
        set_primary_btn = (
            '<div class="action-row">'
            f'<form method="post" action="/onboarding/tenants/{tid}/stores/{sid}/set-primary">'
            '<button type="submit" class="btn btn-ghost">Сделать основным магазином</button>'
            '</form>'
            '<div class="tooltip-wrap"><button class="tooltip-btn">?</button>'
            '<div class="tooltip-popup">Назначает этот магазин основным для тенанта.</div>'
            '</div></div>'
        )

    content_parts = [
        f'<a href="/onboarding/tenants/{tid}/stores" '
        'style="display:inline-flex;align-items:center;gap:6px;font-size:13px;'
        'color:#6b7280;text-decoration:none;margin-bottom:20px;">&larr; Все магазины</a>',
        store_switcher,

        '<div class="lk-card">',
        '<div class="lk-card-title">Статус магазина</div>',

        '<div class="lk-row"><span class="lk-row-label">Магазин</span>'
        f'<span class="lk-row-value">{store_display_name}</span></div>',

        '<div class="lk-row"><span class="lk-row-label">Организация МойСклад</span>'
        f'<span class="lk-row-value">{ms_org_display}</span></div>',

        '<div class="lk-row"><span class="lk-row-label">Склад МойСклад</span>'
        f'<span class="lk-row-value">{ms_store_display}</span></div>',

        '<div class="lk-row"><span class="lk-row-label">Контрагент по умолчанию</span>'
        f'<span class="lk-row-value">{ms_agent_display}</span></div>',

        '<div class="lk-row"><span class="lk-row-label">Первичная синхронизация</span>'
        + _badge(sync_ok, "Выполнена " + _format_ts(store.get("sync_completed_at")), "Не выполнена")
        + '</div>',

        '<div class="lk-row"><span class="lk-row-label">Основной магазин</span>'
        + _badge(bool(store["is_primary"]), "Да", "Нет")
        + '</div>',

        '</div>',

        '<div class="lk-card">',
        '<div class="lk-card-title">Действия</div>',
        '<div class="actions-list">',

        '<div class="action-row">'
        f'<form method="post" action="/onboarding/tenants/{tid}/stores/{sid}/sync">'
        '<button type="submit" class="btn btn-ghost">Повторная синхронизация товаров</button>'
        '</form>'
        '<div class="tooltip-wrap"><button class="tooltip-btn">?</button>'
        '<div class="tooltip-popup">Пересоздаёт маппинг товаров для этого магазина.</div>'
        '</div></div>',

        '<div class="action-row">'
        f'<form method="post" action="/onboarding/tenants/{tid}/stores/{sid}/reconcile">'
        '<button type="submit" class="btn btn-ghost">Синхронизировать остатки</button>'
        '</form>'
        '<div class="tooltip-wrap"><button class="tooltip-btn">?</button>'
        '<div class="tooltip-popup">Обновляет остатки в этом магазине из МойСклад.</div>'
        '</div></div>',

        '<div class="action-row">'
        f'<form method="post" action="/onboarding/tenants/{tid}/stores/{sid}/sync-ms-to-evotor">'
        '<button type="submit" class="btn btn-ghost">Синхронизировать новые товары из МойСклад</button>'
        '</form>'
        '<div class="tooltip-wrap"><button class="tooltip-btn">?</button>'
        '<div class="tooltip-popup">Создаёт в этом магазине товары, добавленные в МойСклад.</div>'
        '</div></div>',

        set_primary_btn,

        '</div></div>',

        '<div class="sync-overlay" id="syncOverlay">',
        '<div class="sync-spinner"></div>',
        '<div class="sync-label" id="syncLabel">Синхронизация...</div>',
        '<div class="sync-sublabel">Это может занять до 30 секунд</div>',
        '</div>',

        '<script>'
        'document.querySelectorAll(".action-row form").forEach(function(form){'
        'form.addEventListener("submit",function(){'
        'var btn=form.querySelector("button");'
        'var label=btn?btn.textContent.trim():"Синхронизация";'
        'if(btn){btn.disabled=true;}'
        'document.getElementById("syncLabel").textContent=label+"...";'
        'document.getElementById("syncOverlay").classList.add("visible");'
        '});'
        '});'
        '</script>',
    ]

    content = ''.join(content_parts)

    return _lk_layout(tenant, "stores", content, info_message=msg, error_message=err)


@router.post("/onboarding/tenants/{tenant_id}/stores/add", response_class=HTMLResponse)
def store_add(
    tenant_id: str,
    evotor_store_id: str = Form(...),
    name: str = Form(""),
    ms_store_id: str = Form(""),
    ms_organization_id: str = Form(""),
    ms_agent_id: str = Form(""),
    is_primary: bool = Form(False),
):
    tenant = _load_tenant(tenant_id)
    evotor_store_id = evotor_store_id.strip()
    if not evotor_store_id:
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores?err=Магазин+Эвотор+обязателен",
            status_code=303,
        )

    evotor_token = (tenant.get("evotor_token") or tenant.get("evotor_api_key") or "").strip()
    if not evotor_token:
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores?err=Сначала+подключите+Эвотор",
            status_code=303,
        )

    allowed_store_map = {}
    try:
        raw_evotor_stores = fetch_stores_by_token(evotor_token) or []
        for item in raw_evotor_stores:
            sid = _extract_store_id(item)
            if sid:
                allowed_store_map[sid] = _extract_store_name(item, sid)
    except Exception as e:
        log.exception("store_add failed to load Evotor stores tenant_id=%s", tenant_id)
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores?err=Не+удалось+загрузить+магазины+Эвотор",
            status_code=303,
        )

    if evotor_store_id not in allowed_store_map:
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores?err=Выбранный+магазин+недоступен+для+этого+тенанта",
            status_code=303,
        )

    final_name = (name or "").strip() or allowed_store_map[evotor_store_id]

    import uuid as _uuid_mod
    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("SELECT tenant_id FROM tenant_stores WHERE evotor_store_id = ?"),
            (evotor_store_id,),
        )
        existing = cur.fetchone()
        if existing and existing["tenant_id"] != tenant_id:
            return RedirectResponse(
                url=f"/onboarding/tenants/{tenant_id}/stores?err=Магазин+уже+привязан+к+другому+тенанту",
                status_code=303,
            )

        if is_primary:
            cur.execute(
                aq("UPDATE tenant_stores SET is_primary = 0 WHERE tenant_id = ?"),
                (tenant_id,),
            )

        cur.execute(
            aq("SELECT COUNT(*) as cnt FROM tenant_stores WHERE tenant_id = ?"),
            (tenant_id,),
        )
        count = cur.fetchone()["cnt"]
        final_primary = 1 if (is_primary or count == 0) else 0

        sql = (
            "INSERT INTO tenant_stores "
            "(id, tenant_id, evotor_store_id, name, ms_store_id, "
            "ms_organization_id, ms_agent_id, is_primary, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (evotor_store_id) DO UPDATE SET "
            "tenant_id = EXCLUDED.tenant_id, "
            "name = COALESCE(EXCLUDED.name, tenant_stores.name), "
            "ms_store_id = COALESCE(EXCLUDED.ms_store_id, tenant_stores.ms_store_id), "
            "ms_organization_id = COALESCE(EXCLUDED.ms_organization_id, tenant_stores.ms_organization_id), "
            "ms_agent_id = COALESCE(EXCLUDED.ms_agent_id, tenant_stores.ms_agent_id), "
            "is_primary = EXCLUDED.is_primary, "
            "updated_at = EXCLUDED.updated_at"
        )

        cur.execute(
            aq(sql),
            (
                str(_uuid_mod.uuid4()),
                tenant_id,
                evotor_store_id,
                final_name or None,
                ms_store_id.strip() or None,
                ms_organization_id.strip() or None,
                ms_agent_id.strip() or None,
                final_primary,
                now,
                now,
            ),
        )

        if final_primary:
            cur.execute(
                aq("""
                UPDATE tenants
                SET evotor_store_id = ?,
                    ms_store_id = ?,
                    ms_organization_id = ?,
                    ms_agent_id = ?,
                    sync_completed_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """),
                (
                    evotor_store_id,
                    ms_store_id.strip() or None,
                    ms_organization_id.strip() or None,
                    ms_agent_id.strip() or None,
                    now,
                    tenant_id,
                ),
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        log.exception("store_add failed tenant_id=%s", tenant_id)
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores?err=Ошибка+при+сохранении+магазина",
            status_code=303,
        )
    finally:
        conn.close()

    # Автоматически запускаем первичную синхронизацию нового магазина
    try:
        from app.api.sync import initial_sync_store
        sync_result = initial_sync_store(tenant_id, evotor_store_id)
        synced = sync_result.get("synced", 0)
        skipped = sync_result.get("skipped", 0)
        failed = sync_result.get("failed", 0)
        log.info(
            "store_add auto sync store=%s synced=%s skipped=%s failed=%s",
            evotor_store_id, synced, skipped, failed,
        )
        from urllib.parse import quote_plus
        msg = f"Магазин добавлен. Синхронизация: {synced + skipped} товаров, {failed} ошибок."
        if failed > 0:
            return RedirectResponse(
                url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?err={quote_plus(msg)}",
                status_code=303,
            )
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?msg={quote_plus(msg)}",
            status_code=303,
        )
    except Exception as e:
        log.exception("store_add auto sync failed store=%s", evotor_store_id)
        from urllib.parse import quote_plus
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?err={quote_plus(f'Магазин добавлен, но синхронизация не выполнена: {e}')}",
            status_code=303,
        )


@router.post("/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}/sync", response_class=HTMLResponse)
def store_sync(tenant_id: str, evotor_store_id: str):
    from app.api.sync import initial_sync_store
    from app.stores.mapping_store import MappingStore
    from urllib.parse import quote_plus

    now = int(time.time())

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            aq("""
            UPDATE tenant_stores
            SET sync_completed_at = NULL,
                updated_at = ?
            WHERE tenant_id = ?
              AND evotor_store_id = ?
            """),
            (now, tenant_id, evotor_store_id),
        )

        cur.execute(
            aq("""
            UPDATE tenants
            SET sync_completed_at = NULL,
                updated_at = ?
            WHERE id = ?
              AND evotor_store_id = ?
            """),
            (now, tenant_id, evotor_store_id),
        )

        conn.commit()
    finally:
        conn.close()

    try:
        ms_store = MappingStore()
        deleted = ms_store.delete_by_store(tenant_id, evotor_store_id)
        log.info(
            "store_sync: deleted %s mappings tenant_id=%s store=%s",
            deleted,
            tenant_id,
            evotor_store_id,
        )

        result = initial_sync_store(tenant_id, evotor_store_id)

        synced = result.get("synced", 0)
        skipped = result.get("skipped", 0)
        failed = result.get("failed", 0)

        msg = f"Синхронизация завершена: {synced + skipped} товаров, {failed} ошибок."

        if failed > 0:
            return RedirectResponse(
                url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?err={quote_plus(msg)}",
                status_code=303,
            )

        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?msg={quote_plus(msg)}",
            status_code=303,
        )

    except Exception as e:
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?err={quote_plus(str(e))}",
            status_code=303,
        )


@router.post("/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}/reconcile", response_class=HTMLResponse)
def store_reconcile(tenant_id: str, evotor_store_id: str):
    from app.api.sync import reconcile_stock_store
    from urllib.parse import quote_plus

    try:
        result = reconcile_stock_store(tenant_id, evotor_store_id)
        synced = result.get("synced", 0)
        failed = result.get("failed", 0)
        msg = f"Остатки синхронизированы: {synced} товаров, {failed} ошибок."

        if failed > 0:
            return RedirectResponse(
                url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?err={quote_plus(msg)}",
                status_code=303,
            )

        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?msg={quote_plus(msg)}",
            status_code=303,
        )

    except Exception as e:
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?err={quote_plus(str(e))}",
            status_code=303,
        )

@router.post("/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}/sync-ms-to-evotor", response_class=HTMLResponse)
def store_sync_ms_to_evotor(tenant_id: str, evotor_store_id: str):
    from app.api.sync import sync_ms_to_evotor_store
    from urllib.parse import quote_plus

    try:
        result = sync_ms_to_evotor_store(tenant_id, evotor_store_id)

        synced = int(result.get("synced", 0) or 0)
        failed = int(result.get("failed", 0) or 0)
        deleted = int(result.get("deleted", 0) or 0)

        products_total = int(result.get("products_total", result.get("checked_products", 0)) or 0)
        products_with_stock = int(result.get("products_with_stock", 0) or 0)
        products_without_stock = int(
            result.get(
                "products_without_stock",
                max(products_total - products_with_stock, 0),
            ) or 0
        )
        stock_check_failed = int(result.get("stock_check_failed", 0) or 0)

        msg = (
            f"МС→Эвотор: товаров с остатком синхронизировано — "
            f"{products_with_stock}/{products_total}; "
            f"без остатка на складе — {products_without_stock}; "
            f"добавлено/обновлено — {synced}, удалено — {deleted}, ошибок — {failed}."
        )
        if stock_check_failed > 0:
            msg += f" Не удалось проверить остаток — {stock_check_failed}."

        if failed > 0:
            return RedirectResponse(
                url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?err={quote_plus(msg)}",
                status_code=303,
            )

        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?msg={quote_plus(msg)}",
            status_code=303,
        )

    except Exception as e:
        log.exception(
            "store_sync_ms_to_evotor failed tenant_id=%s store=%s",
            tenant_id,
            evotor_store_id,
        )
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?err={quote_plus(str(e))}",
            status_code=303,
        )
        
@router.post("/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}/set-primary", response_class=HTMLResponse)
def store_set_primary(tenant_id: str, evotor_store_id: str):
    from urllib.parse import quote_plus

    now = int(time.time())

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            aq("""
            SELECT evotor_store_id, ms_store_id, ms_organization_id, ms_agent_id, sync_completed_at
            FROM tenant_stores
            WHERE tenant_id = ?
              AND evotor_store_id = ?
            """),
            (tenant_id, evotor_store_id),
        )
        store = cur.fetchone()

        if not store:
            return RedirectResponse(
                url=f"/onboarding/tenants/{tenant_id}/stores?err={quote_plus('Магазин не найден')}",
                status_code=303,
            )

        cur.execute(
            aq("UPDATE tenant_stores SET is_primary = 0 WHERE tenant_id = ?"),
            (tenant_id,),
        )

        cur.execute(
            aq("""
            UPDATE tenant_stores
            SET is_primary = 1,
                updated_at = ?
            WHERE tenant_id = ?
              AND evotor_store_id = ?
            """),
            (now, tenant_id, evotor_store_id),
        )

        cur.execute(
            aq("""
            UPDATE tenants
            SET evotor_store_id = ?,
                ms_store_id = ?,
                ms_organization_id = ?,
                ms_agent_id = ?,
                sync_completed_at = ?,
                updated_at = ?
            WHERE id = ?
            """),
            (
                store["evotor_store_id"],
                store["ms_store_id"],
                store["ms_organization_id"],
                store["ms_agent_id"],
                store["sync_completed_at"],
                now,
                tenant_id,
            ),
        )

        conn.commit()

    except Exception as e:
        conn.rollback()
        log.exception("store_set_primary failed tenant_id=%s store=%s", tenant_id, evotor_store_id)
        return RedirectResponse(
            url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?err={quote_plus(str(e))}",
            status_code=303,
        )

    finally:
        conn.close()

    return RedirectResponse(
        url=f"/onboarding/tenants/{tenant_id}/stores/{evotor_store_id}?msg={quote_plus('Магазин назначен основным')}",
        status_code=303,
    )


# ---------------------------------------------------------------------
# Product snapshot / rollback actions for LK
# ---------------------------------------------------------------------
@router.post("/onboarding/tenants/{tenant_id}/product-snapshot")
def lk_create_product_snapshot(tenant_id: str):
    try:
        snapshot_path = create_product_snapshot(
            tenant_id=tenant_id,
            evotor_store_id="all",
            reason="manual_lk",
        )
        msg = f"Точка восстановления товаров создана: {snapshot_path}"
        return RedirectResponse(
            f"/onboarding/tenants/{tenant_id}/actions?ok={_urlparse.quote(msg)}",
            status_code=303,
        )
    except Exception as e:
        msg = f"Не удалось создать точку восстановления товаров: {e}"
        return RedirectResponse(
            f"/onboarding/tenants/{tenant_id}/actions?err={_urlparse.quote(msg)}",
            status_code=303,
        )


@router.post("/onboarding/tenants/{tenant_id}/product-rollback-latest")
def lk_rollback_latest_product_snapshot(tenant_id: str):
    try:
        snapshot_path = get_last_product_snapshot(tenant_id)
        if not snapshot_path:
            msg = "Нет доступной точки восстановления товаров для этого клиента"
            return RedirectResponse(
                f"/onboarding/tenants/{tenant_id}/actions?err={_urlparse.quote(msg)}",
                status_code=303,
            )

        result = rollback_evotor_catalog_from_snapshot(
            tenant_id=tenant_id,
            snapshot_dir=snapshot_path,
            evotor_store_id="all",
        )

        if result["failed"] == 0:
            msg = f"Карточки товаров восстановлены из последней точки: восстановлено — {result['restored']}, ошибок — 0. Теперь рекомендуется синхронизировать остатки."
            return RedirectResponse(
                f"/onboarding/tenants/{tenant_id}/actions?ok={_urlparse.quote(msg)}",
                status_code=303,
            )

        msg = f"Откат выполнен частично: восстановлено — {result['restored']}, ошибок — {result['failed']}."
        return RedirectResponse(
            f"/onboarding/tenants/{tenant_id}/actions?err={_urlparse.quote(msg)}",
            status_code=303,
        )

    except Exception as e:
        msg = f"Не удалось откатить карточки товаров: {e}"
        return RedirectResponse(
            f"/onboarding/tenants/{tenant_id}/actions?err={_urlparse.quote(msg)}",
            status_code=303,
        )


# ---------------------------------------------------------------------
# Cleanup stale product mappings from LK
# ---------------------------------------------------------------------
@router.post("/onboarding/tenants/{tenant_id}/cleanup-stale-mappings")
def lk_cleanup_stale_mappings(tenant_id: str):
    """
    Удаляет локальные mappings, которые ссылаются на товары МойСклад,
    которых больше нет в списке товаров МС.

    Внешние системы не трогает:
    - не удаляет товары Эвотор;
    - не удаляет товары МойСклад;
    - чистит только нашу таблицу mappings.
    """
    import os
    import time
    import requests

    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(aq("SELECT moysklad_token FROM tenants WHERE id = ?"), (tenant_id,))
            tenant = cur.fetchone()

            if not tenant:
                msg = "Клиент не найден"
                return RedirectResponse(
                    f"/onboarding/tenants/{tenant_id}/actions?err={_urlparse.quote(msg)}",
                    status_code=303,
                )

            ms_token = tenant["moysklad_token"]

            if not ms_token:
                msg = "Не настроен токен МойСклад"
                return RedirectResponse(
                    f"/onboarding/tenants/{tenant_id}/actions?err={_urlparse.quote(msg)}",
                    status_code=303,
                )

            cur.execute(
                aq("""
                SELECT tenant_id, evotor_store_id, entity_type, evotor_id, ms_id
                FROM mappings
                WHERE tenant_id = ?
                  AND entity_type = 'product'
                """),
                (tenant_id,),
            )
            mapping_rows = [dict(r) for r in cur.fetchall()]

        finally:
            conn.close()

        ms_base = os.getenv("MS_BASE", "https://api.moysklad.ru/api/remap/1.2").rstrip("/")
        headers = {
            "Authorization": f"Bearer {ms_token}",
            "Accept": "application/json;charset=utf-8",
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip",
        }

        ms_product_ids = set()
        offset = 0
        limit = 100

        while True:
            response = requests.get(
                f"{ms_base}/entity/product",
                headers=headers,
                params={"limit": limit, "offset": offset},
                timeout=30,
            )

            if response.status_code == 429:
                time.sleep(1)
                response = requests.get(
                    f"{ms_base}/entity/product",
                    headers=headers,
                    params={"limit": limit, "offset": offset},
                    timeout=30,
                )

            if not response.ok:
                msg = f"Не удалось получить товары МойСклад: status={response.status_code}"
                return RedirectResponse(
                    f"/onboarding/tenants/{tenant_id}/actions?err={_urlparse.quote(msg)}",
                    status_code=303,
                )

            rows = response.json().get("rows", [])

            for product in rows:
                if product.get("id"):
                    ms_product_ids.add(product["id"])

            if len(rows) < limit:
                break

            offset += limit

        stale = [m for m in mapping_rows if m["ms_id"] not in ms_product_ids]

        deleted = 0

        if stale:
            conn = get_connection()
            try:
                cur = conn.cursor()

                for m in stale:
                    cur.execute(
                        aq("""
                        DELETE FROM mappings
                        WHERE tenant_id = ?
                          AND evotor_store_id = ?
                          AND entity_type = ?
                          AND evotor_id = ?
                          AND ms_id = ?
                        """),
                        (
                            m["tenant_id"],
                            m["evotor_store_id"],
                            m["entity_type"],
                            m["evotor_id"],
                            m["ms_id"],
                        ),
                    )
                    deleted += cur.rowcount or 0

                conn.commit()
            finally:
                conn.close()

        msg = f"Очистка завершена: удалено устаревших связей — {deleted}."
        return RedirectResponse(
            f"/onboarding/tenants/{tenant_id}/actions?ok={_urlparse.quote(msg)}",
            status_code=303,
        )

    except Exception as e:
        msg = f"Ошибка очистки устаревших связей: {e}"
        return RedirectResponse(
            f"/onboarding/tenants/{tenant_id}/actions?err={_urlparse.quote(msg)}",
            status_code=303,
        )

