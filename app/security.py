import hmac
import logging
import os

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

log = logging.getLogger("api.security")

# auto_error=False, чтобы мы сами возвращали понятные 401
bearer_scheme = HTTPBearer(auto_error=False)


def _get_admin_api_token() -> str:
    return os.getenv("ADMIN_API_TOKEN", "").strip()


def _get_app_env() -> str:
    return os.getenv("APP_ENV", "").strip().lower()


def _is_production() -> bool:
    return _get_app_env() in {"prod", "production"}


def require_admin_api_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> None:
    """
    Bearer-auth защита для внутренних/admin endpoint'ов.

    Логика:
    - если ADMIN_API_TOKEN задан, нужен Authorization: Bearer <token>;
    - если ADMIN_API_TOKEN не задан в production, доступ запрещён;
    - если ADMIN_API_TOKEN не задан вне production, auth остаётся отключённой
      для локальной разработки и старых тестов.
    """
    expected = _get_admin_api_token()

    if not expected:
        if _is_production():
            log.error("ADMIN_API_TOKEN is not set in production - admin API is disabled")
            raise HTTPException(status_code=500, detail="Admin API is not configured")

        log.warning("ADMIN_API_TOKEN not set - admin API auth is disabled outside production")
        return

    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    if credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid Authorization scheme")

    provided = credentials.credentials.strip()

    if not hmac.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="Invalid admin token")