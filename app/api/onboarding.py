import html
import json
import logging
import time
import uuid

from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import HTMLResponse

from app.db import get_connection

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


def _load_connections() -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, evotor_user_id, stores_json, created_at, updated_at
        FROM evotor_connections
        ORDER BY updated_at DESC
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def _load_connection(connection_id: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM evotor_connections
        WHERE id = ?
    """, (connection_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Evotor connection not found")
    return dict(row)


@router.get("/onboarding/evotor/connect", response_class=HTMLResponse)
def onboarding_evotor_connect():
    rows = _load_connections()

    parts = [
        """
        <p><strong>Шаг 1.</strong> Установите или переавторизуйте приложение Эвотор.</p>
        <p>После этого Эвотор должен вызвать <code>/api/v1/user/token</code>, и здесь появится подключение аккаунта со списком магазинов.</p>
        """
    ]

    if not rows:
        parts.append('<div class="error">Подключения Эвотор пока не найдены.</div>')

    for row in rows:
        stores = json.loads(row["stores_json"] or "[]")
        parts.append(f"""
        <div class="store">
            <p><strong>Evotor user ID:</strong> <code>{html.escape(row["evotor_user_id"])}</code></p>
            <p><strong>Магазинов найдено:</strong> {len(stores)}</p>
            <p><a href="/onboarding/evotor/connections/{html.escape(row["id"])}/stores">Выбрать магазин</a></p>
        </div>
        """)

    return HTMLResponse(_layout("Подключение Эвотор", "".join(parts)))


@router.get("/onboarding/evotor/connections/{connection_id}/stores", response_class=HTMLResponse)
def onboarding_evotor_stores(connection_id: str):
    connection = _load_connection(connection_id)
    stores = json.loads(connection["stores_json"] or "[]")

    if not stores:
        body = '<div class="error">Для этого подключения не найдено ни одного магазина.</div>'
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
            <p><a href="/onboarding/evotor/connections/{html.escape(connection_id)}/stores/{html.escape(store_id)}/profile">Создать профиль для этого магазина</a></p>
        </div>
        """)

    return HTMLResponse(_layout("Выбор магазина Эвотор", "".join(parts)))


@router.get("/onboarding/evotor/connections/{connection_id}/stores/{store_id}/profile", response_class=HTMLResponse)
def onboarding_profile_form(connection_id: str, store_id: str):
    body = f"""
    <form method="post" action="/onboarding/store-profile">
        <input type="hidden" name="connection_id" value="{html.escape(connection_id)}" />
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
    connection_id: str = Form(...),
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
    connection = _load_connection(connection_id)

    tenant_id = str(uuid.uuid4())
    now = int(time.time())

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT id
            FROM tenants
            WHERE evotor_user_id = ? AND evotor_store_id = ?
        """, (connection["evotor_user_id"], evotor_store_id))
        existing = cur.fetchone()
        if existing:
            body = f'<div class="error">Профиль для этого магазина уже существует: <code>{html.escape(existing["id"])}</code></div>'
            return HTMLResponse(_layout("Профиль уже существует", body), status_code=409)

        cur.execute(
            """
            INSERT INTO tenants (
                id, name, evotor_api_key, moysklad_token, created_at,
                evotor_user_id, evotor_token, evotor_store_id,
                ms_organization_id, ms_store_id, ms_agent_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tenant_id,
                name.strip(),
                "",
                moysklad_token.strip(),
                now,
                connection["evotor_user_id"],
                connection["evotor_token"],
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
