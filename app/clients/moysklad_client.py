import logging
import os
import requests

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

    def get_products(self, limit: int = 1000, offset: int = 0) -> list:
        """Получает список товаров из МойСклад."""
        url = f"{self.BASE_URL}/entity/product"
        r = requests.get(
            url,
            headers=self._headers(),
            params={"limit": limit, "offset": offset},
            timeout=30,
        )
        self._handle_error(r)
        data = r.json()
        rows = data.get("rows", [])
        log.info(f"Fetched {len(rows)} products from MoySklad")
        return rows

    def get_product(self, ms_product_id: str) -> dict:
        """Получает товар из МойСклад по ID."""
        url = f"{self.BASE_URL}/entity/product/{ms_product_id}"
        r = requests.get(url, headers=self._headers(), timeout=15)
        self._handle_error(r)
        return r.json()

    def get_product_with_stock(self, ms_product_id: str) -> dict:
        """
        Получает товар с остатком из MoySklad через assortment по filter=id=...
        """
        url = f"{self.BASE_URL}/entity/assortment"
        params = {"filter": f"id={ms_product_id}"}

        try:
            r = requests.get(url, headers=self._headers(), params=params, timeout=20)
            self._handle_error(r)
            data = r.json()

            rows = data.get("rows", []) if isinstance(data, dict) else []
            if not rows:
                raise Exception(f"Product {ms_product_id} not found in MoySklad assortment")

            return rows[0]

        except Exception as e:
            log.warning(
                f"Failed to fetch assortment stock url={url} ms_product_id={ms_product_id} err={e}"
            )
            raise Exception(f"Failed to fetch product stock from MoySklad: {e}")

    def get_product_stock(self, ms_product_id: str) -> float:
        """
        Возвращает текущий остаток товара.
        Поддерживает несколько возможных форматов ответа МойСклад.
        """
        data = self.get_product_with_stock(ms_product_id)

        for key in ("stock", "quantity", "inStock"):
            value = data.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                pass

        raise Exception(f"Stock field not found in MoySklad response for product {ms_product_id}")

    def create_product(self, payload: dict) -> dict:
        """Создаёт товар в МойСклад."""
        url = f"{self.BASE_URL}/entity/product"
        r = requests.post(url, headers=self._headers(), json=payload, timeout=15)
        log.info(f"MoySklad create_product response={r.status_code}")
        self._handle_error(r)
        return r.json()

    def update_product(self, ms_product_id: str, payload: dict) -> dict:
        """Обновляет товар в МойСклад."""
        url = f"{self.BASE_URL}/entity/product/{ms_product_id}"
        r = requests.put(url, headers=self._headers(), json=payload, timeout=15)
        log.info(f"MoySklad update_product response={r.status_code}")
        self._handle_error(r)
        return r.json()

    def create_sale_document(self, payload):
        if "httpbin.org" in self.BASE_URL:
            url = f"{self.BASE_URL}/post"
        else:
            url = f"{self.BASE_URL}/entity/demand"

        log.info(f"Creating sale document url={url}")
        log.info(f"Sale payload={payload}")

        r = requests.post(url, headers=self._headers(), json=payload, timeout=15)
        log.info(f"MoySklad response={r.status_code}")
        self._handle_error(r)
        response_json = r.json()

        if "httpbin.org" in self.BASE_URL:
            result_ref = "httpbin:created"
        else:
            result_ref = response_json.get("id")

        return {
            "success": True,
            "result_ref": result_ref,
            "raw_response": response_json,
        }
