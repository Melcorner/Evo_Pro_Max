import hashlib
import logging
import os
import time

import requests

log = logging.getLogger("fiscalization_client")

DEFAULT_FISCAL_BASE = "https://fiscalization24.ru/api/fiscal"

# Позволяет скомпенсировать clock skew без перезапуска инфраструктуры
TIME_OFFSET_SEC = int(os.getenv("FISCAL_TIME_OFFSET_SEC", "0"))

# Отключение SSL-верификации через env — только для диагностики/тестов.
# В проде всегда должно быть True. Если сертификат не совпадает по hostname —
# правильное решение: указать корректный домен в FISCAL_BASE_URL.
_SSL_VERIFY_ENV = os.getenv("FISCAL_SSL_VERIFY", "true").strip().lower()
SSL_VERIFY: bool | str = _SSL_VERIFY_ENV not in ("0", "false", "no")

# Статусы чека fiscalization24
FISCAL_STATUS = {
    1: "new",
    2: "sent_to_device",
    5: "accepted_by_device",
    9: "error",
    10: "fiscalized",
}


class FiscalizationError(Exception):
    """Ошибка API Universal Fiscalization."""
    pass


class FiscalizationClient:
    """
    Клиент для API Универсального фискализатора (fiscalization24.ru).

    Авторизация:
    - X-Datetime: unix timestamp UTC
    - Authorization: SHA1(X-Datetime + token)

    Если расхождение времени между клиентом и сервером больше 10 минут,
    сервер возвращает ошибку авторизации.
    """

    def __init__(self, token: str, base_url: str | None = None):
        self.token = token
        self.base_url = (
            base_url or os.getenv("FISCAL_BASE_URL") or DEFAULT_FISCAL_BASE
        ).rstrip("/")
        if self.base_url.startswith("http://"):
            log.warning(
                "Fiscalization base URL is using insecure http: %s. "
                "Use https in production.", self.base_url
            )
        _ssl_env = os.getenv("FISCAL_SSL_VERIFY", "true").strip().lower()
        self.ssl_verify = _ssl_env not in ("0", "false", "no")
        if not self.ssl_verify:
            log.warning(
                "SSL verification DISABLED (FISCAL_SSL_VERIFY=false). "
                "Use only for diagnostics."
            )

    def _make_headers(self) -> dict:
        """
        Собирает заголовки авторизации:
        X-Datetime = текущее unix-время UTC
        Authorization = SHA1(X-Datetime + token)
        """
        x_datetime = str(int(time.time()) + TIME_OFFSET_SEC)
        raw = x_datetime + self.token
        signature = hashlib.sha1(raw.encode("utf-8")).hexdigest()

        return {
            "X-Datetime": x_datetime,
            "Authorization": signature,
            "Content-Type": "application/json",
        }

    def _handle_response(
        self,
        response: requests.Response,
        allow_codes: set | None = None,
    ) -> dict:
        """
        Проверяет HTTP-ответ и бизнес-код API.

        По умолчанию успешным считается только Code=0.
        Допустимые дополнительные коды можно передать через allow_codes.
        """
        if not response.ok:
            log.error(
                "Fiscalization24 HTTP error status=%s body=%s",
                response.status_code,
                response.text,
            )
            response.raise_for_status()

        try:
            data = response.json()
        except Exception as e:
            log.error(
                "Fiscalization24 invalid JSON status=%s body=%s error=%s",
                response.status_code,
                response.text,
                e,
            )
            raise FiscalizationError("Fiscalization24 returned invalid JSON")

        code = data.get("Code")
        info = data.get("Info")

        ok_codes = {0}
        if allow_codes:
            ok_codes |= allow_codes

        if code not in ok_codes:
            log.error(
                "Fiscalization24 API error code=%s info=%s body=%s",
                code,
                info,
                data,
            )
            raise FiscalizationError(
                f"Fiscalization24 API error Code={code} Info={info}"
            )

        return data

    def get_clients(self) -> list:
        """
        GET /clients — получить список клиентов интегратора.

        Возвращает список клиентов:
        [
            {
                "UID": "...",
                "Name": "...",
                "Stores": [...]
            }
        ]

        Code=50 считаем допустимым случаем для пустого списка.
        """
        url = f"{self.base_url}/clients"
        response = requests.get(url, headers=self._make_headers(), timeout=20, verify=self.ssl_verify)
        data = self._handle_response(response, allow_codes={50})

        clients = data.get("Clients") or []

        log.info("Fiscalization24 clients fetched count=%s", len(clients))
        log.debug("Fiscalization24 get_clients response=%s", data)

        return clients

    def create_check(self, payload: dict) -> dict:
        """
        POST /check — отправить чек на фискализацию.

        payload должен соответствовать контракту API fiscalization24.
        Возвращает полный ответ API.
        """
        url = f"{self.base_url}/check"
        response = requests.post(
            url,
            headers=self._make_headers(),
            json=payload,
            timeout=30,
            verify=self.ssl_verify,
        )
        data = self._handle_response(response)

        log.info("Fiscalization24 check created uid=%s", payload.get("UID"))
        log.debug("Fiscalization24 create_check response=%s", data)

        return data

    def get_check_state(self, uid: str) -> dict:
        """
        GET /check/<uid> — получить состояние чека.

        Возможные состояния:
        1  — новый
        2  — отправлен на кассу
        5  — принят кассой
        9  — ошибка фискализации
        10 — успешно фискализирован

        Возвращает словарь CheckState.
        """
        url = f"{self.base_url}/check/{uid}"
        response = requests.get(url, headers=self._make_headers(), timeout=20, verify=self.ssl_verify)
        data = self._handle_response(response)

        check_state = data.get("CheckState") or data.get("checkState") or {}

        log.info(
            "Fiscalization24 check state uid=%s state=%s description=%s error=%s error_message=%s",
            uid,
            check_state.get("State"),
            check_state.get("Description"),
            check_state.get("Error"),
            check_state.get("ErrorMessage"),
        )

        return check_state