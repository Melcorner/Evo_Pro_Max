import hashlib
import hmac
import json
import logging
import os
import time
import uuid

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from app.db import get_connection, adapt_query as aq

router = APIRouter(prefix="/vendor/api/v1/app", tags=["Vendor API"])
log = logging.getLogger("api.vendor")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_secret_key() -> str:
    """Secret Key из личного кабинета разработчика МойСклад."""
    return os.getenv("MS_VENDOR_SECRET_KEY", "").strip()


def _verify_signature(body: bytes, signature: str | None) -> bool:
    """
    Проверяет подпись запроса от МойСклад.
    МойСклад подписывает тело запроса через HMAC-SHA256 с Secret Key.
    Заголовок: X-Lognex-Signature
    """
    secret = _get_secret_key()
    if not secret:
        log.warning("MS_VENDOR_SECRET_KEY not set — skipping signature verification")
        return True

    if not signature:
        return False

    expected = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(expected, signature)


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

@router.put("/{app_id}/activate")
async def vendor_activate(
    app_id: str,
    request: Request,
    x_lognex_signature: str | None = Header(default=None),
):
    """
    Активация решения на аккаунте МойСклад.

    МойСклад вызывает этот endpoint когда пользователь устанавливает решение.
    Передаёт токен доступа к JSON API 1.2 для данного аккаунта.

    Ответ:
    - {"status": "activated"} — решение активировано
    - {"status": "SettingsRequired"} — требуется настройка пользователем
    """
    body = await request.body()

    if not _verify_signature(body, x_lognex_signature):
        log.warning("vendor.activate: invalid signature app_id=%s", app_id)
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        data = MSActivateRequest(**json.loads(body))
    except Exception as e:
        log.error("vendor.activate: invalid request body err=%s", e)
        raise HTTPException(status_code=400, detail=f"Invalid request: {e}")

    access_token = data.access[0].access_token if data.access else None
    if not access_token:
        log.error("vendor.activate: no access token in request account_id=%s", data.accountId)
        raise HTTPException(status_code=400, detail="No access token provided")

    try:
        tenant_id = _get_or_create_tenant(data.accountId, access_token)
    except Exception as e:
        log.exception("vendor.activate: failed to create tenant account_id=%s err=%s", data.accountId, e)
        raise HTTPException(status_code=500, detail="Internal error")

    log.info(
        "vendor.activate: success app_id=%s account_id=%s account_name=%s tenant_id=%s",
        app_id, data.accountId, data.accountName, tenant_id,
    )

    # Возвращаем SettingsRequired — пользователь должен настроить решение
    # (выбрать магазин Эвотор и пройти онбординг)
    return {"status": "SettingsRequired"}


@router.put("/{app_id}/suspend")
async def vendor_suspend(
    app_id: str,
    request: Request,
    x_lognex_signature: str | None = Header(default=None),
):
    """
    Приостановка решения на аккаунте МойСклад.
    Вызывается при приостановке подписки.
    Вебхуки на аккаунте будут отключены МойСкладом автоматически.
    """
    body = await request.body()

    if not _verify_signature(body, x_lognex_signature):
        log.warning("vendor.suspend: invalid signature app_id=%s", app_id)
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        data = MSSuspendRequest(**json.loads(body))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid request: {e}")

    try:
        _set_tenant_status(data.accountId, "suspended")
    except Exception as e:
        log.exception("vendor.suspend: failed account_id=%s err=%s", data.accountId, e)
        raise HTTPException(status_code=500, detail="Internal error")

    log.info(
        "vendor.suspend: success app_id=%s account_id=%s account_name=%s",
        app_id, data.accountId, data.accountName,
    )

    return {"status": "ok"}


@router.put("/{app_id}/resume")
async def vendor_resume(
    app_id: str,
    request: Request,
    x_lognex_signature: str | None = Header(default=None),
):
    """
    Возобновление решения на аккаунте МойСклад.
    Вызывается при возобновлении подписки после приостановки.
    МойСклад передаёт новый токен доступа.
    """
    body = await request.body()

    if not _verify_signature(body, x_lognex_signature):
        log.warning("vendor.resume: invalid signature app_id=%s", app_id)
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        data = MSActivateRequest(**json.loads(body))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid request: {e}")

    access_token = data.access[0].access_token if data.access else None
    if not access_token:
        raise HTTPException(status_code=400, detail="No access token provided")

    try:
        _get_or_create_tenant(data.accountId, access_token)
        _set_tenant_status(data.accountId, "active")
    except Exception as e:
        log.exception("vendor.resume: failed account_id=%s err=%s", data.accountId, e)
        raise HTTPException(status_code=500, detail="Internal error")

    log.info(
        "vendor.resume: success app_id=%s account_id=%s account_name=%s",
        app_id, data.accountId, data.accountName,
    )

    return {"status": "ok"}


@router.delete("/{app_id}/delete")
async def vendor_delete(
    app_id: str,
    request: Request,
    x_lognex_signature: str | None = Header(default=None),
):
    """
    Удаление решения с аккаунта МойСклад.
    Вызывается когда пользователь удаляет решение.
    Помечаем tenant как deleted, данные не удаляем (для возможного восстановления).
    """
    body = await request.body()

    if not _verify_signature(body, x_lognex_signature):
        log.warning("vendor.delete: invalid signature app_id=%s", app_id)
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        data = MSDeleteRequest(**json.loads(body))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid request: {e}")

    try:
        _set_tenant_status(data.accountId, "deleted")
    except Exception as e:
        log.exception("vendor.delete: failed account_id=%s err=%s", data.accountId, e)
        raise HTTPException(status_code=500, detail="Internal error")

    log.info(
        "vendor.delete: success app_id=%s account_id=%s account_name=%s",
        app_id, data.accountId, data.accountName,
    )

    return {"status": "ok"}
