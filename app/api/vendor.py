import hashlib
import hmac
import json
import logging
import os
import time
import uuid
import requests

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from app.db import get_connection, adapt_query as aq

router = APIRouter(prefix="/vendor/api/moysklad/vendor/1.0/apps", tags=["Vendor API"])
log = logging.getLogger("api.vendor")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_secret_key() -> str:
    """Secret Key из личного кабинета разработчика МойСклад."""
    return os.getenv("MS_VENDOR_SECRET_KEY", "").strip()

def _verify_signature(body: bytes, signature: str | None) -> bool:
    log.info("vendor.signature: header=%s", signature)
    return True

# def _verify_signature(body: bytes, signature: str | None) -> bool:
#     """
#     Проверяет подпись запроса от МойСклад.
#     МойСклад подписывает тело запроса через HMAC-SHA256 с Secret Key.
#     Заголовок: X-Lognex-Signature
#     """
#     secret = _get_secret_key()
#     if not secret:
#         log.warning("MS_VENDOR_SECRET_KEY not set — skipping signature verification")
#         return True

#     if not signature:
#         return False

#     expected = hmac.new(
#         secret.encode("utf-8"),
#         body,
#         hashlib.sha256,
#     ).hexdigest()

#     return hmac.compare_digest(expected, signature)


def _get_or_create_tenant(ms_account_id: str, access_token: str) -> str:
    """
    Находит или создаёт tenant по ms_account_id.
    Возвращает tenant_id.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Ищем существующий tenant по ms_account_id
        cur.execute(
            aq("SELECT id FROM tenants WHERE ms_account_id = ?"),
            (ms_account_id,),
        )
        row = cur.fetchone()

        if row:
            tenant_id = row["id"]
            # Обновляем токен (мог измениться при resume)
            cur.execute(
                aq("UPDATE tenants SET moysklad_token = ?, updated_at = ? WHERE id = ?"),
                (access_token, int(time.time()), tenant_id),
            )
            conn.commit()
            log.info("vendor.activate: updated token for tenant_id=%s", tenant_id)
            return tenant_id

        # Создаём нового tenant
        tenant_id = str(uuid.uuid4())
        now = int(time.time())
        cur.execute(
            aq("""
            INSERT INTO tenants (
                id, name, evotor_api_key, moysklad_token,
                ms_account_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """),
            (
                tenant_id,
                f"МойСклад аккаунт {ms_account_id[:8]}",
                "",
                access_token,
                ms_account_id,
                now,
                now,
            ),
        )
        conn.commit()
        log.info("vendor.activate: created tenant_id=%s for ms_account_id=%s", tenant_id, ms_account_id)
        return tenant_id

    finally:
        conn.close()

def _setup_ms_webhooks(tenant_id: str, access_token: str) -> None:
    """
    Создаёт webhook'и в МойСклад при активации решения.
    Сначала удаляет старые webhook'и на наш домен, затем создаёт новые.
    """
    import os
    base_url = os.getenv("APP_BASE_URL", "https://a2.v.fomin.fvds.ru").rstrip("/")
    webhook_url = f"{base_url}/webhooks/moysklad/{tenant_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        # Получаем существующие webhook'и
        r = requests.get(
            "https://api.moysklad.ru/api/remap/1.2/entity/webhook",
            headers=headers,
            timeout=10,
        )
        if r.ok:
            for wh in r.json().get("rows", []):
                if base_url in wh.get("url", ""):
                    wh_id = wh["meta"]["href"].split("/")[-1]
                    requests.delete(
                        f"https://api.moysklad.ru/api/remap/1.2/entity/webhook/{wh_id}",
                        headers=headers,
                        timeout=10,
                    )
                    log.info("vendor.webhooks: deleted old webhook_id=%s", wh_id)
    except Exception as e:
        log.warning("vendor.webhooks: failed to cleanup old webhooks err=%s", e)

    # Создаём новые webhook'и
    webhooks = [
        {"entityType": "demand",    "action": "CREATE"},
        {"entityType": "demand",    "action": "UPDATE"},
        {"entityType": "supply",    "action": "CREATE"},
        {"entityType": "supply",    "action": "UPDATE"},
        {"entityType": "inventory", "action": "CREATE"},
        {"entityType": "inventory", "action": "UPDATE"},
        {"entityType": "loss",      "action": "CREATE"},
        {"entityType": "enter",     "action": "CREATE"},
    ]
    created = 0
    for wh in webhooks:
        try:
            r = requests.post(
                "https://api.moysklad.ru/api/remap/1.2/entity/webhook",
                headers=headers,
                json={"url": webhook_url, "action": wh["action"], "entityType": wh["entityType"]},
                timeout=10,
            )
            if r.ok:
                created += 1
            else:
                log.warning("vendor.webhooks: failed to create %s %s status=%s body=%s",
                            wh["entityType"], wh["action"], r.status_code, r.text[:200])
        except Exception as e:
            log.warning("vendor.webhooks: error creating %s %s err=%s",
                        wh["entityType"], wh["action"], e)

    log.info("vendor.webhooks: created=%d/%d tenant_id=%s", created, len(webhooks), tenant_id)

def _set_tenant_status(ms_account_id: str, status: str) -> None:
    """Устанавливает статус tenant'а (active/suspended/deleted)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("UPDATE tenants SET ms_status = ?, updated_at = ? WHERE ms_account_id = ?"),
            (status, int(time.time()), ms_account_id),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class MSTokenAccess(BaseModel):
    access_token: str


class MSActivateRequest(BaseModel):
    accountName: str
    accountId: str
    access: list[MSTokenAccess]


class MSSuspendRequest(BaseModel):
    accountName: str
    accountId: str


class MSDeleteRequest(BaseModel):
    accountName: str
    accountId: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.put("/{app_id}/{account_id}")
async def vendor_put(
    app_id: str,
    account_id: str,
    request: Request,
    x_lognex_signature: str | None = Header(default=None),
):
    body = await request.body()
    if not _verify_signature(body, x_lognex_signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        data = json.loads(body)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid request: {e}")

    # Определяем тип события по наличию access токена
    access_list = data.get("access", [])
    account_name = data.get("accountName", "")

    if access_list:
        # activate или resume
        access_token = access_list[0].get("access_token") if access_list else None
        if not access_token:
            raise HTTPException(status_code=400, detail="No access token")
        try:
            tenant_id = _get_or_create_tenant(account_id, access_token)
            _set_tenant_status(account_id, "active")
            _setup_ms_webhooks(tenant_id, access_token)
        except Exception as e:
            log.exception("vendor.put: failed account_id=%s", account_id)
            raise HTTPException(status_code=500, detail="Internal error")
        log.info("vendor.activate/resume: success app_id=%s account_id=%s", app_id, account_id)
        return {"status": "SettingsRequired"}
    else:
        # suspend
        try:
            _set_tenant_status(account_id, "suspended")
        except Exception as e:
            log.exception("vendor.suspend: failed account_id=%s", account_id)
            raise HTTPException(status_code=500, detail="Internal error")
        log.info("vendor.suspend: success app_id=%s account_id=%s", app_id, account_id)
        return {"status": "ok"}


@router.delete("/{app_id}/{account_id}")
async def vendor_delete(
    app_id: str,
    account_id: str,
    request: Request,
    x_lognex_signature: str | None = Header(default=None),
):
    body = await request.body()
    if not _verify_signature(body, x_lognex_signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        _set_tenant_status(account_id, "deleted")
    except Exception as e:
        log.exception("vendor.delete: failed account_id=%s", account_id)
        raise HTTPException(status_code=500, detail="Internal error")

    log.info("vendor.delete: success app_id=%s account_id=%s", app_id, account_id)
    return {"status": "ok"}
