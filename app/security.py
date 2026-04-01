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


def require_admin_api_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> None:
    """
    Простая Bearer-auth защита для внутренних/admin endpoint'ов.

    Логика:
    - если ADMIN_API_TOKEN не задан, защита отключается
      (удобно для локальной разработки)
    - если токен задан, нужен Bearer token
    """
    expected = _get_admin_api_token()

    if not expected:
        log.debug("ADMIN_API_TOKEN not set — admin API auth is disabled")
        return

    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    if credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid Authorization scheme")

    provided = credentials.credentials.strip()

    if not hmac.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="Invalid admin token")