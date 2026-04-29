import logging
import os
import time

import requests

from app.db import get_connection, adapt_query as aq

log = logging.getLogger("stale_mapping_service")

MS_BASE = os.getenv("MS_BASE", "https://api.moysklad.ru/api/remap/1.2").rstrip("/")


def _ms_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json;charset=utf-8",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
    }


def _load_tenant_token(tenant_id: str) -> str:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("SELECT moysklad_token FROM tenants WHERE id = ?"), (tenant_id,))
        row = cur.fetchone()

        if not row:
            raise RuntimeError(f"Tenant not found: {tenant_id}")

        token = row["moysklad_token"]

        if not token:
            raise RuntimeError(f"MoySklad token is not configured for tenant_id={tenant_id}")

        return token
    finally:
        conn.close()


def _fetch_ms_product_ids(ms_token: str) -> set[str]:
    ids = set()
    offset = 0
    limit = 100

    while True:
        response = requests.get(
            f"{MS_BASE}/entity/product",
            headers=_ms_headers(ms_token),
            params={"limit": limit, "offset": offset},
            timeout=30,
        )

        if response.status_code == 429:
            log.warning("MoySklad product list 429, retry after 1s offset=%s", offset)
            time.sleep(1)
            response = requests.get(
                f"{MS_BASE}/entity/product",
                headers=_ms_headers(ms_token),
                params={"limit": limit, "offset": offset},
                timeout=30,
            )

        if not response.ok:
            raise RuntimeError(
                f"Failed to fetch MoySklad products: status={response.status_code} body={response.text[:500]}"
            )

        rows = response.json().get("rows", [])

        for product in rows:
            product_id = product.get("id")
            if product_id:
                ids.add(product_id)

        if len(rows) < limit:
            break

        offset += limit

        if offset > 100000:
            raise RuntimeError("Too many MoySklad products, pagination guard stopped cleanup")

    return ids


def cleanup_stale_product_mappings(
    tenant_id: str,
    evotor_store_id: str | None = None,
) -> dict:
    """
    Удаляет локальные mappings, которые указывают на товары МойСклад,
    которых больше нет в МойСклад.

    Внешние системы не трогает:
    - не удаляет товары Эвотор;
    - не удаляет товары МойСклад;
    - чистит только нашу таблицу mappings.
    """
    ms_token = _load_tenant_token(tenant_id)
    ms_product_ids = _fetch_ms_product_ids(ms_token)

    conn = get_connection()
    try:
        cur = conn.cursor()

        if evotor_store_id and evotor_store_id != "all":
            cur.execute(
                aq("""
                SELECT tenant_id, evotor_store_id, entity_type, evotor_id, ms_id
                FROM mappings
                WHERE tenant_id = ?
                  AND evotor_store_id = ?
                  AND entity_type = 'product'
                """),
                (tenant_id, evotor_store_id),
            )
        else:
            cur.execute(
                aq("""
                SELECT tenant_id, evotor_store_id, entity_type, evotor_id, ms_id
                FROM mappings
                WHERE tenant_id = ?
                  AND entity_type = 'product'
                """),
                (tenant_id,),
            )

        mappings = [dict(row) for row in cur.fetchall()]
        stale = [m for m in mappings if m["ms_id"] not in ms_product_ids]

        deleted = 0

        for m in stale:
            cur.execute(
                aq("""
                DELETE FROM mappings
                WHERE tenant_id = ?
                  AND evotor_store_id = ?
                  AND entity_type = ?
                  AND evotor_id = ?
                  AND ms_id = ?
                """),
                (
                    m["tenant_id"],
                    m["evotor_store_id"],
                    m["entity_type"],
                    m["evotor_id"],
                    m["ms_id"],
                ),
            )
            deleted += cur.rowcount or 0

        conn.commit()

        result = {
            "tenant_id": tenant_id,
            "evotor_store_id": evotor_store_id,
            "ms_products": len(ms_product_ids),
            "checked_mappings": len(mappings),
            "stale_mappings": len(stale),
            "deleted": deleted,
        }

        if deleted:
            log.warning("Stale product mappings cleaned: %s", result)
        else:
            log.info("No stale product mappings found: %s", result)

        return result

    finally:
        conn.close()
