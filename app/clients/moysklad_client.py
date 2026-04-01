import requests
import logging
import os
import time

from app.db import get_connection

log = logging.getLogger("moysklad")

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"


class MoySkladClient:
    BASE_URL = os.getenv("MS_BASE_URL", MS_BASE)

    def __init__(self, tenant_id):
        self.tenant_id = tenant_id
        self.token = self._load_token()

    def _load_token(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT moysklad_token FROM tenants WHERE id = ?", (self.tenant_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            raise Exception("Tenant not found")
        return row["moysklad_token"]

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip",
        }

    def _handle_error(self, r: requests.Response):
        if not r.ok:
            try:
                log.error(f"MoySklad error status={r.status_code} body={r.json()}")
            except Exception:
                log.error(f"MoySklad error status={r.status_code} text={r.text}")
        r.raise_for_status()

    def ping(self):
        url = f"{self.BASE_URL}/entity/organization"
        log.info("MoySklad ping")
        r = requests.get(url, headers=self._headers(), timeout=10)
        log.info(f"MoySklad status={r.status_code}")
        self._handle_error(r)
        return r.json()

    def get_products(self, search: str | None = None, limit: int = 1000, offset: int = 0) -> dict:
        """
        Получает список товаров из МойСклад.
        Возвращает полный JSON-ответ, чтобы можно было брать rows, meta и т.д.
        """
        url = f"{self.BASE_URL}/entity/product"
        params = {"limit": limit, "offset": offset}

        if search:
            params["search"] = search

        r = requests.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=30
        )
        self._handle_error(r)
        data = r.json()
        rows = data.get("rows", []) if isinstance(data, dict) else []
        log.info(f"Fetched {len(rows)} products from MoySklad search={search!r}")
        return data

    def get_product(self, ms_product_id: str) -> dict:
        url = f"{self.BASE_URL}/entity/product/{ms_product_id}"
        r = requests.get(url, headers=self._headers(), timeout=15)
        self._handle_error(r)
        return r.json()

    def create_product(self, payload: dict) -> dict:
        url = f"{self.BASE_URL}/entity/product"
        r = requests.post(url, headers=self._headers(), json=payload, timeout=15)
        log.info(f"MoySklad create_product response={r.status_code}")
        self._handle_error(r)
        return r.json()

    def update_product(self, ms_product_id: str, payload: dict) -> dict:
        url = f"{self.BASE_URL}/entity/product/{ms_product_id}"
        r = requests.put(url, headers=self._headers(), json=payload, timeout=15)
        log.info(f"MoySklad update_product response={r.status_code}")
        self._handle_error(r)
        return r.json()

    def find_demand_by_external_code(self, external_code: str) -> dict | None:
        """
        Ищет demand в МойСклад по externalCode.
        Используется для идемпотентного создания — защита от дублей при retry.
        """
        external_code = (external_code or "").strip()
        if not external_code:
            return None
        url = f"{self.BASE_URL}/entity/demand"
        r = requests.get(
            url,
            headers=self._headers(),
            params={"filter": f"externalCode={external_code}"},
            timeout=20,
        )
        self._handle_error(r)
        rows = r.json().get("rows", [])
        return rows[0] if rows else None

    def create_sale_document(self, payload: dict, external_code: str | None = None):
        """
        Создаёт demand в МойСклад.
        Если external_code передан — сначала ищет существующий demand по externalCode,
        и возвращает его без повторного создания (идемпотентность при retry).
        """
        external_code = (external_code or payload.get("syncId") or "").strip()

        if external_code:
            existing = self.find_demand_by_external_code(external_code)
            if existing:
                log.info(
                    "Demand already exists externalCode=%s id=%s — skipping create (idempotent)",
                    external_code,
                    existing.get("id"),
                )
                return {
                    "success": True,
                    "result_ref": existing.get("id"),
                    "raw_response": existing,
                    "idempotent": True,
                }
            payload = dict(payload)
            payload["externalCode"] = external_code

        if "httpbin.org" in self.BASE_URL:
            url = f"{self.BASE_URL}/post"
        else:
            url = f"{self.BASE_URL}/entity/demand"

        log.info(f"Creating sale document url={url}")
        log.debug(f"Sale payload={payload}")  # PII: debug only

        r = requests.post(url, headers=self._headers(), json=payload, timeout=15)
        log.info(f"MoySklad response={r.status_code}")
        self._handle_error(r)
        response_json = r.json()

        result_ref = "httpbin:created" if "httpbin.org" in self.BASE_URL else response_json.get("id")
        return {
            "success": True,
            "result_ref": result_ref,
            "raw_response": response_json,
            "idempotent": False,
        }

    # -----------------------------
    # Stock helpers
    # -----------------------------

    def get_product_with_stock(self, ms_product_id: str) -> dict | None:
        """
        Получает строку остатка товара через /report/stock/all.

        /entity/assortment при нулевом остатке может вернуть пустой rows,
        что приводило к исключению вместо 0. /report/stock/all надёжнее —
        всегда возвращает строку для известного товара.
        """
        url = f"{self.BASE_URL}/report/stock/all"
        params = {"filter": f"product={self.BASE_URL}/entity/product/{ms_product_id}"}

        try:
            r = requests.get(url, headers=self._headers(), params=params, timeout=20)
            self._handle_error(r)
            data = r.json()
            rows = data.get("rows", []) if isinstance(data, dict) else []
            if not rows:
                log.warning(
                    "Product %s not found in MoySklad stock report", ms_product_id
                )
                return None
            return rows[0]
        except Exception as e:
            log.warning(
                "Failed to fetch stock report ms_product_id=%s err=%s", ms_product_id, e
            )
            raise Exception(f"Failed to fetch product stock from MoySklad: {e}")

    def get_product_stock(self, ms_product_id: str) -> float:
        """
        Возвращает текущий остаток товара.
        Если товар не найден в отчёте — возвращает 0.0 (не исключение).
        """
        data = self.get_product_with_stock(ms_product_id)
        if data is None:
            return 0.0

        for key in ("stock", "quantity", "inStock"):
            value = data.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                pass

        # Строка есть, поле остатка отсутствует — товар есть, остаток 0
        log.warning("Stock field not found for product %s, returning 0", ms_product_id)
        return 0.0

    # -----------------------------
    # Counterparty helpers
    # -----------------------------

    def _search_counterparties(self, query: str, limit: int = 100) -> list:
        if not query:
            return []

        url = f"{self.BASE_URL}/entity/counterparty"
        r = requests.get(
            url,
            headers=self._headers(),
            params={"search": query, "limit": limit},
            timeout=20,
        )
        self._handle_error(r)
        data = r.json()
        return data.get("rows", []) if isinstance(data, dict) else []

    def _normalize_phone(self, phone: str | None) -> str | None:
        if not phone:
            return None

        digits = "".join(ch for ch in str(phone) if ch.isdigit())
        if not digits:
            return None

        if len(digits) == 11 and digits.startswith("8"):
            digits = "7" + digits[1:]

        return digits

    def _extract_phone_candidates(self, row: dict) -> list[str]:
        values = []
        for key in ("phone", "mobilePhone", "telephone", "tel"):
            value = row.get(key)
            if value not in (None, ""):
                values.append(str(value))
        return values

    def _extract_email_candidates(self, row: dict) -> list[str]:
        values = []
        for key in ("email", "eMail", "mail"):
            value = row.get(key)
            if value not in (None, ""):
                values.append(str(value))
        return values

    def find_counterparty_by_email(self, email: str | None) -> dict | None:
        if not email:
            return None

        email_lower = str(email).strip().lower()
        rows = self._search_counterparties(email_lower)
        for row in rows:
            for candidate in self._extract_email_candidates(row):
                if candidate.strip().lower() == email_lower:
                    log.debug(f"Counterparty found by email={email_lower} id={row.get('id')}")  # PII: debug only
                    return row
        return None

    def find_counterparty_by_phone(self, phone: str | None) -> dict | None:
        if not phone:
            return None

        normalized_target = self._normalize_phone(phone)
        if not normalized_target:
            return None

        rows = self._search_counterparties(normalized_target)
        for row in rows:
            for candidate in self._extract_phone_candidates(row):
                if self._normalize_phone(candidate) == normalized_target:
                    log.debug(f"Counterparty found by phone={normalized_target} id={row.get('id')}")  # PII: debug only
                    return row
        return None
    
    def create_counterparty(
        self,
        name: str | None,
        phone: str | None,
        email: str | None,
        inn: str | None = None,
    ) -> dict:
        url = f"{self.BASE_URL}/entity/counterparty"
        safe_name = (name or "").strip() or email or phone or f"Buyer {int(time.time())}"

        payload = {"name": safe_name}
        if phone:
            payload["phone"] = str(phone).strip()
        if email:
            payload["email"] = str(email).strip()
        if inn:
            payload["inn"] = str(inn).strip()
        payload["description"] = "Auto-created from Evotor receipt"

        r = requests.post(url, headers=self._headers(), json=payload, timeout=20)
        log.info(f"MoySklad create_counterparty response={r.status_code}")
        self._handle_error(r)
        return r.json()