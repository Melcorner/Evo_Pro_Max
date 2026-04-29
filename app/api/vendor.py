import hashlib
import hmac
import json
import logging
import os
import time
import uuid

import jwt as pyjwt
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


def _get_expected_app_id() -> str:
    """App ID решения из личного кабинета разработчика МойСклад."""
    return os.getenv("MS_APP_ID", "").strip()


def _get_expected_app_uid() -> str:
    """App UID решения из личного кабинета разработчика МойСклад."""
    return os.getenv("MS_APP_UID", "").strip()


def _normalize_signature(signature: str | None) -> str | None:
    """
    Нормализует подпись из заголовка подписи.

    Поддерживает:
    - обычный hex digest;
    - значение с префиксом sha256=...
    """
    if not signature:
        return None

    value = signature.strip()

    if value.lower().startswith("sha256="):
        value = value.split("=", 1)[1].strip()

    return value.lower() or None


def _extract_signature_from_headers(
    request: Request,
    explicit_signature: str | None = None,
) -> str | None:
    """
    Пытается достать подпись из возможных заголовков.

    Основной ожидаемый заголовок:
    - X-Lognex-Signature

    Дополнительно проверяем несколько вариантов на случай,
    если Vendor API МойСклад использует другое имя заголовка.
    """
    if explicit_signature:
        return explicit_signature

    possible_headers = (
        "x-lognex-signature",
        "x-moysklad-signature",
        "x-signature",
        "signature",
    )

    for header_name in possible_headers:
        value = request.headers.get(header_name)
        if value:
            return value

    return None


def _verify_signature(body: bytes, signature: str | None) -> bool:
    """
    Проверяет подпись запроса от МойСклад через HMAC-SHA256.

    Важно:
    - не логируем само значение подписи;
    - без MS_VENDOR_SECRET_KEY запрос не принимаем;
    - сравнение выполняем через hmac.compare_digest.
    """
    secret = _get_secret_key()

    if not secret:
        log.error("vendor.signature: MS_VENDOR_SECRET_KEY is not set")
        return False

    normalized_signature = _normalize_signature(signature)

    log.info("vendor.signature: present=%s", bool(normalized_signature))

    if not normalized_signature:
        return False

    expected = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(expected, normalized_signature)


def _extract_bearer_token(request: Request) -> str | None:
    authorization = request.headers.get("authorization", "").strip()

    if not authorization:
        return None

    parts = authorization.split(" ", 1)

    if len(parts) != 2:
        return None

    scheme, token = parts[0].lower(), parts[1].strip()

    if scheme != "bearer" or not token:
        return None

    return token


def _verify_authorization_jwt(request: Request) -> bool:
    """
    Проверяет Bearer JWT от Vendor API МойСклад.

    Важно:
    - подпись JWT проверяется через MS_VENDOR_SECRET_KEY;
    - если в JWT есть sub, сверяем его с MS_APP_UID;
    - если sub отсутствует, не отклоняем запрос автоматически, потому что
      МойСклад может присылать JWT только с exp/iat/jti.
    """
    secret = os.getenv("MS_VENDOR_SECRET_KEY", "").strip()
    expected_uid = os.getenv("MS_APP_UID", "").strip()

    auth_header = request.headers.get("authorization", "").strip()
    bearer_present = auth_header.lower().startswith("bearer ")

    log.info("vendor.auth: bearer_present=%s", bearer_present)

    if not bearer_present:
        return False

    if not secret:
        log.error("vendor.auth: MS_VENDOR_SECRET_KEY is not set")
        return False

    token = auth_header.split(" ", 1)[1].strip()
    if not token:
        log.warning("vendor.auth: empty bearer token")
        return False

    try:
        claims = pyjwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            options={"verify_aud": False},
        )
    except Exception as e:
        log.warning("vendor.auth: invalid bearer jwt err=%s", type(e).__name__)
        return False

    token_sub = claims.get("sub")

    if token_sub:
        if expected_uid and token_sub != expected_uid:
            log.warning(
                "vendor.auth: invalid jwt sub expected_len=%s actual_len=%s expected_prefix=%s actual_prefix=%s",
                len(expected_uid),
                len(str(token_sub)),
                expected_uid[:4],
                str(token_sub)[:4],
            )
            return False
    else:
        log.info(
            "vendor.auth: jwt sub missing; accepting by valid HS256 signature and app_id validation claims=%s",
            sorted(claims.keys()),
        )

    log.info(
        "vendor.auth: bearer jwt verified claims=%s sub_present=%s",
        sorted(claims.keys()),
        bool(token_sub),
    )
    return True


def _verify_vendor_request(request: Request, body: bytes, signature: str | None) -> bool:
    """
    Проверяет входящий Vendor API запрос.

    Поддерживает:
    - signature-header, если он есть;
    - Authorization: Bearer JWT, если signature-header отсутствует.
    """
    if signature:
        return _verify_signature(body, signature)

    return _verify_authorization_jwt(request)


def _validate_app_id(app_id: str) -> None:
    """
    Проверяет, что запрос пришёл для нашего приложения МойСклад.

    Если MS_APP_ID задан в окружении, app_id из URL должен совпадать с ним.
    """
    expected_app_id = _get_expected_app_id()

    if not expected_app_id:
        log.warning("vendor.app_id: MS_APP_ID is not set, app_id validation skipped")
        return

    if not hmac.compare_digest(str(app_id), expected_app_id):
        log.warning("vendor.app_id: invalid app_id=%s", app_id)
        raise HTTPException(status_code=404, detail="App not found")


def _get_or_create_tenant(ms_account_id: str, access_token: str) -> str:
    """
    Находит или создаёт tenant по ms_account_id.
    Возвращает tenant_id.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            aq("SELECT id FROM tenants WHERE ms_account_id = ?"),
            (ms_account_id,),
        )
        row = cur.fetchone()

        if row:
            tenant_id = row["id"]

            cur.execute(
                aq("""
                UPDATE tenants
                SET moysklad_token = ?,
                    updated_at = ?
                WHERE id = ?
                """),
                (access_token, int(time.time()), tenant_id),
            )

            conn.commit()
            log.info("vendor.activate: updated token for tenant_id=%s", tenant_id)
            return tenant_id

        tenant_id = str(uuid.uuid4())
        now = int(time.time())

        cur.execute(
            aq("""
            INSERT INTO tenants (
                id,
                name,
                evotor_api_key,
                moysklad_token,
                ms_account_id,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
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
        log.info(
            "vendor.activate: created tenant_id=%s for ms_account_id=%s",
            tenant_id,
            ms_account_id,
        )
        return tenant_id

    finally:
        conn.close()


def _setup_ms_webhooks(tenant_id: str, access_token: str) -> None:
    """
    Создаёт webhook'и в МойСклад при активации решения.

    Сначала удаляет старые webhook'и на URL текущего tenant,
    затем создаёт новые.
    """
    base_url = os.getenv("APP_BASE_URL", "").strip().rstrip("/")

    if not base_url:
        log.error("vendor.webhooks: APP_BASE_URL is not set, cannot setup MoySklad webhooks")
        return

    webhook_url = f"{base_url}/webhooks/moysklad/{tenant_id}"
    webhook_url_normalized = webhook_url.rstrip("/")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        r = requests.get(
            "https://api.moysklad.ru/api/remap/1.2/entity/webhook",
            headers=headers,
            timeout=10,
        )

        if r.ok:
            for wh in r.json().get("rows", []):
                wh_url = wh.get("url", "").rstrip("/")

                if wh_url == webhook_url_normalized:
                    wh_href = wh.get("meta", {}).get("href", "")
                    wh_id = wh_href.rstrip("/").split("/")[-1] if wh_href else None

                    if not wh_id:
                        continue

                    delete_response = requests.delete(
                        f"https://api.moysklad.ru/api/remap/1.2/entity/webhook/{wh_id}",
                        headers=headers,
                        timeout=10,
                    )

                    if delete_response.ok:
                        log.info("vendor.webhooks: deleted old webhook_id=%s", wh_id)
                    else:
                        log.warning(
                            "vendor.webhooks: failed to delete old webhook_id=%s status=%s body=%s",
                            wh_id,
                            delete_response.status_code,
                            delete_response.text[:200],
                        )
        else:
            log.warning(
                "vendor.webhooks: failed to list existing webhooks status=%s body=%s",
                r.status_code,
                r.text[:200],
            )

    except Exception as e:
        log.warning("vendor.webhooks: failed to cleanup old webhooks err=%s", e)

    webhooks = [
        {"entityType": "demand", "action": "CREATE"},
        {"entityType": "demand", "action": "UPDATE"},
        {"entityType": "supply", "action": "CREATE"},
        {"entityType": "supply", "action": "UPDATE"},
        {"entityType": "inventory", "action": "CREATE"},
        {"entityType": "inventory", "action": "UPDATE"},
        {"entityType": "loss", "action": "CREATE"},
        {"entityType": "enter", "action": "CREATE"},
        {"entityType": "product", "action": "CREATE"},
        {"entityType": "product", "action": "UPDATE"},
    ]

    created = 0

    for wh in webhooks:
        try:
            r = requests.post(
                "https://api.moysklad.ru/api/remap/1.2/entity/webhook",
                headers=headers,
                json={
                    "url": webhook_url,
                    "action": wh["action"],
                    "entityType": wh["entityType"],
                },
                timeout=10,
            )

            if r.ok:
                created += 1
            else:
                log.warning(
                    "vendor.webhooks: failed to create %s %s status=%s body=%s",
                    wh["entityType"],
                    wh["action"],
                    r.status_code,
                    r.text[:200],
                )

        except Exception as e:
            log.warning(
                "vendor.webhooks: error creating %s %s err=%s",
                wh["entityType"],
                wh["action"],
                e,
            )

    log.info(
        "vendor.webhooks: created=%d/%d tenant_id=%s",
        created,
        len(webhooks),
        tenant_id,
    )


def _notify_ms_activated(ms_account_id: str) -> None:
    """
    Уведомляет МойСклад, что настройка решения завершена.

    Сейчас функция оставлена как helper.
    Вызывать её лучше после фактического завершения wizard-настройки,
    а не сразу в vendor_put, потому что vendor_put возвращает SettingsRequired.
    """
    app_id = os.getenv("MS_APP_ID", "").strip()
    app_uid = os.getenv("MS_APP_UID", "").strip()
    secret_key = os.getenv("MS_VENDOR_SECRET_KEY", "").strip()

    if not app_id or not app_uid or not secret_key:
        log.warning("notify_ms_activated: MS_APP_ID/MS_APP_UID/MS_VENDOR_SECRET_KEY not set — skipping")
        return

    try:
        now = int(time.time())

        token = pyjwt.encode(
            {
                "sub": app_uid,
                "iat": now,
                "exp": now + 300,
                "jti": str(uuid.uuid4()),
            },
            secret_key,
            algorithm="HS256",
        )

        url = f"https://apps-api.moysklad.ru/api/vendor/1.0/apps/{app_id}/{ms_account_id}/status"

        r = requests.put(
            url,
            json={"status": "Activated"},
            headers={
                "Content-Type": "application/json",
                "Accept-Encoding": "gzip",
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            },
            timeout=10,
        )

        if r.ok:
            log.info("notify_ms_activated: success account_id=%s", ms_account_id)
        else:
            log.warning(
                "notify_ms_activated: failed status=%s body=%s",
                r.status_code,
                r.text[:300],
            )

    except Exception as e:
        log.warning("notify_ms_activated: error account_id=%s err=%s", ms_account_id, e)


def _set_tenant_status(ms_account_id: str, status: str) -> None:
    """Устанавливает статус tenant'а: active/suspended/deleted."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            UPDATE tenants
            SET ms_status = ?,
                updated_at = ?
            WHERE ms_account_id = ?
            """),
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
    _validate_app_id(app_id)

    body = await request.body()

    signature = _extract_signature_from_headers(
        request=request,
        explicit_signature=x_lognex_signature,
    )

    auth_header = request.headers.get("authorization", "").strip()
    auth_scheme = auth_header.split(" ", 1)[0].lower() if auth_header else None

    log.debug(
        "vendor.auth.debug: method=%s path=%s app_id=%s account_id=%s signature_present=%s authorization_present=%s auth_scheme=%s content_length=%s",
        request.method,
        request.url.path,
        app_id,
        account_id,
        bool(signature),
        bool(auth_header),
        auth_scheme,
        len(body or b""),
    )

    if not _verify_vendor_request(request, body, signature):
        log.warning(
            "vendor.auth.failed: method=%s path=%s app_id=%s account_id=%s signature_present=%s authorization_present=%s auth_scheme=%s",
            request.method,
            request.url.path,
            app_id,
            account_id,
            bool(signature),
            bool(auth_header),
            auth_scheme,
        )
        raise HTTPException(status_code=401, detail="Invalid vendor authorization")

    try:
        data = json.loads(body)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid request: {e}")

    access_list = data.get("access", [])

    if access_list:
        if not isinstance(access_list, list) or not isinstance(access_list[0], dict):
            raise HTTPException(status_code=400, detail="Invalid access format")

        access_token = access_list[0].get("access_token")

        if not access_token:
            raise HTTPException(status_code=400, detail="No access token")

        try:
            tenant_id = _get_or_create_tenant(account_id, access_token)
            _set_tenant_status(account_id, "active")
            _setup_ms_webhooks(tenant_id, access_token)

        except Exception:
            log.exception("vendor.put: failed account_id=%s", account_id)
            raise HTTPException(status_code=500, detail="Internal error")

        log.info(
            "vendor.activate/resume: success app_id=%s account_id=%s",
            app_id,
            account_id,
        )

        return {"status": "SettingsRequired"}

    try:
        _set_tenant_status(account_id, "suspended")

    except Exception:
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
    _validate_app_id(app_id)

    body = await request.body()

    signature = _extract_signature_from_headers(
        request=request,
        explicit_signature=x_lognex_signature,
    )

    auth_header = request.headers.get("authorization", "").strip()
    auth_scheme = auth_header.split(" ", 1)[0].lower() if auth_header else None

    log.debug(
        "vendor.auth.debug: method=%s path=%s app_id=%s account_id=%s signature_present=%s authorization_present=%s auth_scheme=%s content_length=%s",
        request.method,
        request.url.path,
        app_id,
        account_id,
        bool(signature),
        bool(auth_header),
        auth_scheme,
        len(body or b""),
    )

    if not _verify_vendor_request(request, body, signature):
        log.warning(
            "vendor.auth.failed: method=%s path=%s app_id=%s account_id=%s signature_present=%s authorization_present=%s auth_scheme=%s",
            request.method,
            request.url.path,
            app_id,
            account_id,
            bool(signature),
            bool(auth_header),
            auth_scheme,
        )
        raise HTTPException(status_code=401, detail="Invalid vendor authorization")

    try:
        _set_tenant_status(account_id, "deleted")

    except Exception:
        log.exception("vendor.delete: failed account_id=%s", account_id)
        raise HTTPException(status_code=500, detail="Internal error")

    log.info("vendor.delete: success app_id=%s account_id=%s", app_id, account_id)
    return {"status": "ok"}