import html
import json
import logging
import time
import uuid

from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import HTMLResponse

from app.db import get_connection
from app.clients.evotor_client import fetch_stores_by_token

router = APIRouter(tags=["Onboarding"])
log = logging.getLogger("api.onboarding")


def _layout(title: str, body: str) -> str:
    return f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <title>{html.escape(title)}</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 24px;
                background: #f5f7fb;
                color: #172033;
            }}
            h1 {{ margin-bottom: 12px; }}
            .meta {{ color: #5b6475; margin-bottom: 20px; }}
            .card {{
                background: #ffffff;
                border: 1px solid #d8deea;
                border-radius: 8px;
                padding: 20px;
                max-width: 1000px;
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
                margin-bottom: 14px;
            }}
            input {{
                padding: 10px 12px;
                border: 1px solid #cfd7e6;
                border-radius: 6px;
                font-size: 14px;
            }}
            button {{
                background: #2458d3;
                color: white;
                border: none;
                border-radius: 6px;
                padding: 10px 16px;
                cursor: pointer;
                font-size: 14px;
                font-weight: 600;
            }}
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
                color: #7a1f17;
                padding: 12px 14px;
                border-radius: 6px;
                margin-bottom: 16px;
            }}
            code {{
                background: #eef3fb;
                padding: 2px 6px;
                border-radius: 4px;
            }}
            a {{ color: #2458d3; text-decoration: none; }}
        </style>
    </head>
    <body>
        <h1>{html.escape(title)}</h1>
        <div class="meta">Onboarding профиля магазина Evotor ↔ MoySklad</div>
        <div class="card">{body}</div>
    </body>
    </html>
    """


def _extract_store_id(store: dict) -> str:
    return str(
        store.get("id")
        or store.get("uuid")
        or store.get("storeId")
        or ""
    ).strip()


def _extract_store_name(store: dict, fallback_id: str) -> str:
    return str(
        store.get("name")
        or store.get("title")
        or store.get("storeName")
        or f"Store {fallback_id}"
    ).strip()


def _load_session(session_id: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM evotor_onboarding_sessions
        WHERE id = ?
    """, (session_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Onboarding session not found")
    return dict(row)


@router.get("/onboarding/evotor/connect", response_class=HTMLResponse)
def onboarding_token_form():
    body = """
    <form method="post" action="/onboarding/evotor/connect">
        <div class="field">
            <label>Evotor token</label>
            <input name="evotor_token" required />
        </div>
        <button type="submit">Получить мои магазины</button>
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
    except Exception as e:
        log.exception("Failed to fetch stores by Evotor token")
        body = f'<div class="error">Не удалось получить магазины по token: {html.escape(str(e))}</div>'
        return HTMLResponse(_layout("Ошибка подключения", body), status_code=502)

    now = int(time.time())
    session_id = str(uuid.uuid4())

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO evotor_onboarding_sessions (
                id, evotor_token, stores_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            session_id,
            evotor_token,
            json.dumps(stores, ensure_ascii=False),
            now,
            now,
        ))
        conn.commit()
    finally:
        conn.close()

    if not stores:
        body = '<div class="error">По этому token не найдено ни одного магазина.</div>'
        return HTMLResponse(_layout("Магазины не найдены", body), status_code=400)

    stores_link = f"/onboarding/evotor/sessions/{session_id}/stores"
    body = f"""
    <div class="success">Магазины успешно получены.</div>
    <p><a href="{html.escape(stores_link)}">Перейти к выбору магазина</a></p>
    """
    return HTMLResponse(_layout("Подключение Эвотор", body))


@router.get("/onboarding/evotor/sessions/{session_id}/stores", response_class=HTMLResponse)
def onboarding_evotor_stores(session_id: str):
    session = _load_session(session_id)
    stores = json.loads(session["stores_json"] or "[]")

    if not stores:
        body = '<div class="error">Для этой onboarding-сессии магазины не найдены.</div>'
        return HTMLResponse(_layout("Выбор магазина", body), status_code=400)

    parts = []
    for store in stores:
        store_id = _extract_store_id(store)
        if not store_id:
            continue
        store_name = _extract_store_name(store, store_id)

        parts.append(f"""
        <div class="store">
            <p><strong>Магазин:</strong> {html.escape(store_name)}</p>
            <p><strong>Store ID:</strong> <code>{html.escape(store_id)}</code></p>
            <p><a href="/onboarding/evotor/sessions/{html.escape(session_id)}/stores/{html.escape(store_id)}/profile">Создать профиль для этого магазина</a></p>
        </div>
        """)

    return HTMLResponse(_layout("Выбор магазина Эвотор", "".join(parts)))


@router.get("/onboarding/evotor/sessions/{session_id}/stores/{store_id}/profile", response_class=HTMLResponse)
def onboarding_profile_form(session_id: str, store_id: str):
    body = f"""
    <form method="post" action="/onboarding/store-profile">
        <input type="hidden" name="session_id" value="{html.escape(session_id)}" />
        <input type="hidden" name="evotor_store_id" value="{html.escape(store_id)}" />

        <div class="field">
            <label>Имя профиля магазина</label>
            <input name="name" required />
        </div>

        <div class="field">
            <label>MoySklad token</label>
            <input name="moysklad_token" required />
        </div>

        <div class="field">
            <label>MS organization ID</label>
            <input name="ms_organization_id" required />
        </div>

        <div class="field">
            <label>MS store ID</label>
            <input name="ms_store_id" required />
        </div>

        <div class="field">
            <label>MS agent ID</label>
            <input name="ms_agent_id" required />
        </div>

        <div class="field">
            <label>Fiscal token</label>
            <input name="fiscal_token" />
        </div>

        <div class="field">
            <label>Fiscal client UID</label>
            <input name="fiscal_client_uid" />
        </div>

        <div class="field">
            <label>Fiscal device UID</label>
            <input name="fiscal_device_uid" />
        </div>

        <button type="submit">Создать профиль магазина</button>
    </form>
    """
    return HTMLResponse(_layout("Создание профиля магазина", body))


@router.post("/onboarding/store-profile", response_class=HTMLResponse)
def onboarding_store_profile_submit(
    session_id: str = Form(...),
    evotor_store_id: str = Form(...),
    name: str = Form(...),
    moysklad_token: str = Form(...),
    ms_organization_id: str = Form(...),
    ms_store_id: str = Form(...),
    ms_agent_id: str = Form(...),
    fiscal_token: str = Form(""),
    fiscal_client_uid: str = Form(""),
    fiscal_device_uid: str = Form(""),
):
    session = _load_session(session_id)

    tenant_id = str(uuid.uuid4())
    now = int(time.time())

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT id
            FROM tenants
            WHERE evotor_store_id = ?
        """, (evotor_store_id,))
        existing = cur.fetchone()
        if existing:
            body = f'<div class="error">Профиль для этого магазина уже существует: <code>{html.escape(existing["id"])}</code></div>'
            return HTMLResponse(_layout("Профиль уже существует", body), status_code=409)

        cur.execute(
            """
            INSERT INTO tenants (
                id, name, evotor_api_key, moysklad_token, created_at,
                evotor_token, evotor_store_id,
                ms_organization_id, ms_store_id, ms_agent_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tenant_id,
                name.strip(),
                "",
                moysklad_token.strip(),
                now,
                session["evotor_token"],
                evotor_store_id.strip(),
                ms_organization_id.strip(),
                ms_store_id.strip(),
                ms_agent_id.strip(),
            ),
        )

        if fiscal_token.strip() and fiscal_client_uid.strip() and fiscal_device_uid.strip():
            cur.execute("""
                UPDATE tenants
                SET fiscal_token = ?, fiscal_client_uid = ?, fiscal_device_uid = ?
                WHERE id = ?
            """, (
                fiscal_token.strip(),
                fiscal_client_uid.strip(),
                fiscal_device_uid.strip(),
                tenant_id,
            ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        log.exception("Failed to create store profile")
        body = f'<div class="error">Не удалось создать профиль магазина: {html.escape(str(e))}</div>'
        return HTMLResponse(_layout("Ошибка создания профиля", body), status_code=500)
    finally:
        conn.close()

    body = f"""
    <div class="success">Профиль магазина успешно создан.</div>
    <p><strong>ID профиля:</strong> <code>{html.escape(tenant_id)}</code></p>
    <p><strong>Store ID:</strong> <code>{html.escape(evotor_store_id)}</code></p>
    <p>Теперь можно запускать initial sync:</p>
    <p><code>POST /sync/{html.escape(tenant_id)}/initial</code></p>
    """
    return HTMLResponse(_layout("Профиль создан", body))
