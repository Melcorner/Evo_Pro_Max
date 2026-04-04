import html
import json
import logging
import time
import uuid

import requests
from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import HTMLResponse

from app.db import get_connection
from app.clients.evotor_client import fetch_stores_by_token

router = APIRouter(tags=["Onboarding"])
log = logging.getLogger("api.onboarding")

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"


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
    r = requests.get(url, headers=_ms_headers(token), params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def _ms_fetch_all(token: str) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Возвращает (organizations, stores, agents).
    Каждый элемент — {"id": "...", "name": "..."}.
    """
    def extract(data: dict) -> list[dict]:
        return [
            {"id": row["id"], "name": row.get("name") or row.get("description") or row["id"]}
            for row in data.get("rows", [])
            if row.get("id")
        ]

    orgs   = extract(_ms_fetch("/entity/organization", token))
    stores = extract(_ms_fetch("/entity/store", token))
    agents = extract(_ms_fetch("/entity/counterparty", token, params={"limit": 100}))
    return orgs, stores, agents


# ---------------------------------------------------------------------------
# HTML layout
# ---------------------------------------------------------------------------

def _layout(title: str, body: str) -> str:
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
        select {{ cursor: pointer; }}
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
            color: #7a1f17;
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
        code {{
            background: #eef3fb;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 13px;
        }}
        a {{ color: #2458d3; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        hr {{ border: none; border-top: 1px solid #e8edf5; margin: 20px 0; }}
    </style>
</head>
<body>
    <h1>{html.escape(title)}</h1>
    <div class="meta">Создание профиля магазина Evotor ↔ MoySklad</div>
    <div class="card">{body}</div>
</body>
</html>
"""


def _select(name: str, items: list[dict]) -> str:
    """Рендерит <select> где option value=id, текст=name."""
    options = "\n".join(
        f'<option value="{html.escape(item["id"])}">{html.escape(item["name"])}</option>'
        for item in items
    )
    return f'<select name="{html.escape(name)}" required>\n{options}\n</select>'


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

def _load_session(session_id: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM evotor_onboarding_sessions WHERE id = ?",
        (session_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Onboarding session not found")
    return dict(row)


def _extract_store_id(store: dict) -> str:
    return str(
        store.get("id") or store.get("uuid") or store.get("storeId") or ""
    ).strip()


def _extract_store_name(store: dict, fallback_id: str) -> str:
    return str(
        store.get("name") or store.get("title") or store.get("storeName") or f"Store {fallback_id}"
    ).strip()


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
    except Exception as e:
        log.exception("Failed to fetch stores by Evotor token")
        body = f'<div class="error">Не удалось получить магазины по token: {html.escape(str(e))}</div>'
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
            """
            INSERT INTO evotor_onboarding_sessions (id, evotor_token, stores_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
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
            <p style="margin:4px 0 12px; color:#5b6475; font-size:13px;">ID: <code>{html.escape(store_id)}</code></p>
            <a href="{link}">Создать профиль для этого магазина →</a>
        </div>
        """)

    return HTMLResponse(_layout("Выбор магазина Эвотор", "".join(parts)))


# ---------------------------------------------------------------------------
# Step 3 — ввод MS токена и автозагрузка данных
# ---------------------------------------------------------------------------

@router.get(
    "/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token",
    response_class=HTMLResponse,
)
def onboarding_ms_token_form(session_id: str, store_id: str):
    body = f"""
    <p style="margin-bottom:20px; color:#5b6475; font-size:14px;">
        Магазин Эвотор: <code>{html.escape(store_id)}</code>
    </p>
    <form method="post"
          action="/onboarding/evotor/sessions/{html.escape(session_id)}/stores/{html.escape(store_id)}/ms-token">
        <div class="field">
            <label>MoySklad token</label>
            <input name="moysklad_token" required
                   placeholder="Токен из раздела «Безопасность» → «Токены» в МойСклад" />
            <span class="hint">После ввода система автоматически загрузит ваши организации, склады и контрагентов.</span>
        </div>
        <button type="submit">Загрузить данные МойСклад →</button>
    </form>
    """
    return HTMLResponse(_layout("Подключение МойСклад", body))


@router.post(
    "/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token",
    response_class=HTMLResponse,
)
def onboarding_ms_token_submit(
    session_id: str,
    store_id: str,
    moysklad_token: str = Form(...),
):
    moysklad_token = moysklad_token.strip()
    if not moysklad_token:
        body = '<div class="error">MoySklad token обязателен.</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=400)

    try:
        orgs, ms_stores, agents = _ms_fetch_all(moysklad_token)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        log.warning("MoySklad API error %s", status)
        if status == 401:
            msg = "Неверный MoySklad token — проверьте правильность и повторите."
        else:
            msg = f"Ошибка API МойСклад: {status}"
        body = f'<div class="error">{html.escape(msg)}</div>'
        return HTMLResponse(_layout("Ошибка подключения МойСклад", body), status_code=502)
    except Exception as e:
        log.exception("Failed to fetch MoySklad data")
        body = f'<div class="error">Не удалось получить данные МойСклад: {html.escape(str(e))}</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=502)

    if not orgs:
        body = '<div class="error">В МойСклад не найдено ни одной организации.</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=400)
    if not ms_stores:
        body = '<div class="error">В МойСклад не найдено ни одного склада.</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=400)
    if not agents:
        body = '<div class="error">В МойСклад не найдено ни одного контрагента.</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=400)

    # Сохраняем токен и данные в сессию, чтобы не передавать их скрытыми полями
    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE evotor_onboarding_sessions
            SET moysklad_token  = ?,
                ms_data_json    = ?,
                updated_at      = ?
            WHERE id = ?
            """,
            (
                moysklad_token,
                json.dumps(
                    {"orgs": orgs, "stores": ms_stores, "agents": agents},
                    ensure_ascii=False,
                ),
                now,
                session_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    profile_url = (
        f"/onboarding/evotor/sessions/{html.escape(session_id)}"
        f"/stores/{html.escape(store_id)}/profile"
    )

    org_select    = _select("ms_organization_id", orgs)
    store_select  = _select("ms_store_id", ms_stores)
    agent_select  = _select("ms_agent_id", agents)

    body = f"""
    <div class="success">Данные МойСклад успешно загружены.</div>

    <form method="post" action="/onboarding/store-profile">
        <input type="hidden" name="session_id"      value="{html.escape(session_id)}" />
        <input type="hidden" name="evotor_store_id" value="{html.escape(store_id)}" />

        <div class="field">
            <label>Имя профиля магазина</label>
            <input name="name" required placeholder="Например: Мой магазин на Ленина" />
        </div>

        <hr>
        <div class="section-title">МойСклад</div>

        <div class="field">
            <label>Организация</label>
            {org_select}
        </div>

        <div class="field">
            <label>Склад</label>
            {store_select}
        </div>

        <div class="field">
            <label>Контрагент по умолчанию</label>
            {agent_select}
            <span class="hint">Используется как покупатель, если данные клиента в чеке отсутствуют.</span>
        </div>

        <hr>
        <div class="section-title">Фискализация (необязательно)</div>

        <div class="field">
            <label>Fiscal token</label>
            <input name="fiscal_token" placeholder="Оставьте пустым, если не нужна фискализация" />
        </div>

        <div class="field">
            <label>Fiscal client UID</label>
            <input name="fiscal_client_uid" />
        </div>

        <div class="field">
            <label>Fiscal device UID</label>
            <input name="fiscal_device_uid" />
        </div>

        <button type="submit">Создать профиль магазина →</button>
    </form>
    """
    return HTMLResponse(_layout("Настройка профиля магазина", body))


# ---------------------------------------------------------------------------
# Step 4 — сохранение профиля
# ---------------------------------------------------------------------------

@router.post("/onboarding/store-profile", response_class=HTMLResponse)
def onboarding_store_profile_submit(
    session_id: str = Form(...),
    evotor_store_id: str = Form(...),
    name: str = Form(...),
    ms_organization_id: str = Form(...),
    ms_store_id: str = Form(...),
    ms_agent_id: str = Form(...),
    fiscal_token: str = Form(""),
    fiscal_client_uid: str = Form(""),
    fiscal_device_uid: str = Form(""),
):
    session = _load_session(session_id)

    # Достаём MS-токен и данные из сессии (не из формы)
    moysklad_token = session.get("moysklad_token", "").strip()
    if not moysklad_token:
        body = '<div class="error">Сессия не содержит MoySklad token. Начните онбординг заново.</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=400)

    # Валидируем что выбранные ID действительно из нашего загруженного списка
    ms_data = json.loads(session.get("ms_data_json") or "{}")
    valid_org_ids   = {item["id"] for item in ms_data.get("orgs",   [])}
    valid_store_ids = {item["id"] for item in ms_data.get("stores", [])}
    valid_agent_ids = {item["id"] for item in ms_data.get("agents", [])}

    if ms_organization_id not in valid_org_ids:
        body = '<div class="error">Выбрана неверная организация.</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=400)
    if ms_store_id not in valid_store_ids:
        body = '<div class="error">Выбран неверный склад.</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=400)
    if ms_agent_id not in valid_agent_ids:
        body = '<div class="error">Выбран неверный контрагент.</div>'
        return HTMLResponse(_layout("Ошибка", body), status_code=400)

    tenant_id = str(uuid.uuid4())
    now = int(time.time())

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT id FROM tenants WHERE evotor_store_id = ?",
            (evotor_store_id,),
        )
        existing = cur.fetchone()
        if existing:
            body = (
                f'<div class="error">Профиль для этого магазина уже существует: '
                f'<code>{html.escape(existing["id"])}</code></div>'
            )
            return HTMLResponse(_layout("Профиль уже существует", body), status_code=409)

        cur.execute(
            """
            INSERT INTO tenants (
                id, name, evotor_api_key, moysklad_token, created_at,
                evotor_token, evotor_store_id,
                ms_organization_id, ms_store_id, ms_agent_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tenant_id,
                name.strip(),
                "",
                moysklad_token,
                now,
                session["evotor_token"],
                evotor_store_id.strip(),
                ms_organization_id.strip(),
                ms_store_id.strip(),
                ms_agent_id.strip(),
            ),
        )

        if fiscal_token.strip() and fiscal_client_uid.strip() and fiscal_device_uid.strip():
            cur.execute(
                """
                UPDATE tenants
                SET fiscal_token = ?, fiscal_client_uid = ?, fiscal_device_uid = ?
                WHERE id = ?
                """,
                (fiscal_token.strip(), fiscal_client_uid.strip(), fiscal_device_uid.strip(), tenant_id),
            )

        conn.commit()

    except Exception as e:
        conn.rollback()
        log.exception("Failed to create store profile")
        body = f'<div class="error">Не удалось создать профиль магазина: {html.escape(str(e))}</div>'
        return HTMLResponse(_layout("Ошибка создания профиля", body), status_code=500)
    finally:
        conn.close()

    body = f"""
    <div class="success">
        <strong>Профиль магазина успешно создан!</strong><br><br>
        Tenant ID: <code>{html.escape(tenant_id)}</code>
    </div>
    <p style="color:#5b6475; font-size:14px;">
        Следующий шаг — выполните первичную синхронизацию товаров:<br>
        <code>POST /sync/{html.escape(tenant_id)}/initial</code>
    </p>
    """
    return HTMLResponse(_layout("Профиль создан", body))