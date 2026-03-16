import requests
import logging
import os

from app.db import get_connection

log = logging.getLogger("moysklad")


class MoySkladClient:

    BASE_URL = os.getenv(
        "MS_BASE_URL",
        "https://api.moysklad.ru/api/remap/1.2"
    )

    def __init__(self, tenant_id):
        self.tenant_id = tenant_id
        self.token = self._load_token()

    def _load_token(self):
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT moysklad_token
            FROM tenants
            WHERE id = ?
        """, (self.tenant_id,))

        row = cur.fetchone()
        conn.close()

        if not row:
            raise Exception("Tenant not found")

        return row["moysklad_token"]

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip"
        }

    def ping(self):
        url = f"{self.BASE_URL}/entity/organization"
        log.info("MoySklad ping")
        r = requests.get(url, headers=self._headers(), timeout=10)
        log.info(f"MoySklad status={r.status_code}")
        r.raise_for_status()
        return r.json()

    def create_sale_document(self, payload):
        if "httpbin.org" in self.BASE_URL:
            url = f"{self.BASE_URL}/post"
        else:
            url = f"{self.BASE_URL}/entity/demand"

        log.info(f"Creating sale document url={url}")
        log.info(f"Sale payload={payload}")

        r = requests.post(
            url,
            headers=self._headers(),
            json=payload,
            timeout=15
        )

        log.info(f"MoySklad response={r.status_code}")

        if not r.ok:
            try:
                error_body = r.json()
                log.error(f"MoySklad error body={error_body}")
            except Exception:
                log.error(f"MoySklad error text={r.text}")

        r.raise_for_status()
        response_json = r.json()

        if "httpbin.org" in self.BASE_URL:
            result_ref = "httpbin:created"
        else:
            result_ref = response_json.get("id")

        return {
            "success": True,
            "result_ref": result_ref,
            "raw_response": response_json
        }