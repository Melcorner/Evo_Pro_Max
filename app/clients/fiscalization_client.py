import hashlib
import logging
import time

import requests

log = logging.getLogger("fiscalization_client")

FISCAL_BASE = "http://fiscalization24.ru/api/fiscal"

# Статусы чека fiscalization24
FISCAL_STATUS = {
    1: "new",
    2: "sent_to_device",
    5: "accepted_by_device",
    9: "error",
    10: "fiscalized",
}


class FiscalizationClient:
    """
    Клиент для API Универсального фискализатора (fiscalization24.ru).

    Авторизация:
    - X-Datetime: unix timestamp UTC
    - Authorization: SHA1(X-Datetime + token)

    При расхождении времени > 10 минут сервер вернёт ошибку.
    """

    def __init__(self, token: str):
        self.token = token

    def _make_headers(self) -> dict:
        x_datetime = str(int(time.time()))
        raw = x_datetime + self.token
        signature = hashlib.sha1(raw.encode("utf-8")).hexdigest()
        return {
            "X-Datetime": x_datetime,
            "Authorization": signature,
            "Content-Type": "application/json",
        }

    def _handle_response(self, r: requests.Response, allow_codes: set | None = None) -> dict:
        if not r.ok:
            log.error(
                "Fiscalization24 error status=%s body=%s",
                r.status_code, r.text,
            )
            r.raise_for_status()
        data = r.json()
        code = data.get("Code", 0)
        # Code=0 — успех, Code=50 — пустой список (не ошибка)
        ok_codes = {0, 50} | (allow_codes or set())
        if code not in ok_codes:
            raise Exception(
                f"Fiscalization24 API error Code={code} Info={data.get('Info')}"
            )
        return data

    def get_clients(self) -> list:
        """
        GET /clients — список клиентов (магазинов и касс) интегратора.
        Используется для получения fiscal_client_uid и fiscal_device_uid.
        """
        url = f"{FISCAL_BASE}/clients"
        r = requests.get(url, headers=self._make_headers(), timeout=20)
        data = self._handle_response(r)
        clients = data.get("Clients") or []
        log.info("Fiscalization24 clients fetched count=%s", len(clients))
        
        log.info("Fiscalization get_clients response: %s", data)
        return clients

    def create_check(self, payload: dict) -> dict:
        """
        POST /check — отправить чек на фискализацию.
        Возвращает ответ сервера (Code=0 при успехе).
        """
        url = f"{FISCAL_BASE}/check"
        r = requests.post(url, headers=self._make_headers(), json=payload, timeout=30)
        data = self._handle_response(r)
        log.info("Fiscalization24 check created uid=%s", payload.get("UID"))
        return data

    def get_check_state(self, uid: str) -> dict:
        """
        GET /check/<uid> — получить состояние чека.

        Возможные состояния (State):
        1  — новый
        2  — отправлен на кассу
        5  — принят кассой
        9  — ошибка фискализации
        10 — успешно фискализирован
        """
        url = f"{FISCAL_BASE}/check/{uid}"
        r = requests.get(url, headers=self._make_headers(), timeout=20)
        data = self._handle_response(r)
        state = data.get("CheckState", {})
        log.info(
            "Fiscalization24 check state uid=%s state=%s description=%s",
            uid,
            state.get("State"),
            state.get("Description"),
        )
        return state