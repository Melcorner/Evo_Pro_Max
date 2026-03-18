import requests
import logging

from app.db import get_connection

log = logging.getLogger("evotor_client")

EVOTOR_BASE = "https://api.evotor.ru"


class EvotorClient:

    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.token, self.store_id = self._load_config()

    def _load_config(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT evotor_token, evotor_store_id
            FROM tenants WHERE id = ?
        """, (self.tenant_id,))
        row = cur.fetchone()
        conn.close()

        if not row:
            raise Exception(f"Tenant not found: {self.tenant_id}")
        if not row["evotor_token"]:
            raise Exception(f"evotor_token not configured for tenant {self.tenant_id}")
        if not row["evotor_store_id"]:
            raise Exception(f"evotor_store_id not configured for tenant {self.tenant_id}")

        return row["evotor_token"], row["evotor_store_id"]

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def get_products(self) -> list:
        """Получает все товары из облака Эвотор."""
        url = f"{EVOTOR_BASE}/stores/{self.store_id}/products"
        r = requests.get(url, headers=self._headers(), timeout=30)

        if not r.ok:
            log.error(f"Evotor get_products error status={r.status_code}")
            r.raise_for_status()

        data = r.json()
        products = data.get("items", [])
        log.info(f"Fetched {len(products)} products from Evotor store={self.store_id}")
        return products

    def create_product(self, product: dict) -> dict:
        """
        Создаёт товар в облаке Эвотор.

        product — формат Эвотор:
        {
            "id": "<uuid>",           # обязательно — внешний ID из МойСклад
            "name": "Название",       # обязательно
            "price": 100.0,           # цена продажи
            "cost_price": 80.0,       # закупочная цена
            "measure_name": "шт",
            "tax": "NO_VAT",
            "allow_to_sell": true,
            "description": "...",
            "barcodes": ["..."]
        }
        """
        url = f"{EVOTOR_BASE}/stores/{self.store_id}/products"
        r = requests.post(url, headers=self._headers(), json=product, timeout=15)

        if not r.ok:
            log.error(f"Evotor create_product error status={r.status_code} body={r.text}")
            r.raise_for_status()

        log.info(f"Created Evotor product id={product.get('id')} name={product.get('name')}")
        return r.json() if r.text else {}

    def update_product(self, evotor_product_id: str, product: dict) -> dict:
        """Обновляет товар в облаке Эвотор."""
        url = f"{EVOTOR_BASE}/stores/{self.store_id}/products/{evotor_product_id}"
        r = requests.put(url, headers=self._headers(), json=product, timeout=15)

        if not r.ok:
            log.error(f"Evotor update_product error status={r.status_code} body={r.text}")
            r.raise_for_status()

        log.info(f"Updated Evotor product id={evotor_product_id}")
        return r.json() if r.text else {}

    def delete_product(self, evotor_product_id: str):
        """Удаляет товар из облака Эвотор."""
        url = f"{EVOTOR_BASE}/stores/{self.store_id}/products/{evotor_product_id}"
        r = requests.delete(url, headers=self._headers(), timeout=15)

        if not r.ok:
            log.error(f"Evotor delete_product error status={r.status_code}")
            r.raise_for_status()

        log.info(f"Deleted Evotor product id={evotor_product_id}")