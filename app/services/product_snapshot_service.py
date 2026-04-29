import json
import logging
import os
import re
import time
import uuid
from copy import deepcopy
from pathlib import Path

import requests

from app.db import get_connection, adapt_query as aq

log = logging.getLogger("product_snapshot")

EVOTOR_BASE = os.getenv("EVOTOR_BASE", "https://api.evotor.ru").rstrip("/")

STOCK_KEYS = {
    "quantity",
    "balance",
    "stock",
    "stockQuantity",
    "remaining",
    "available",
    "reserved",
    "reservedQuantity",
}


def _safe_name(value: str) -> str:
    value = value or "unknown"
    value = re.sub(r"[^a-zA-Z0-9а-яА-Я_.-]+", "_", value)
    return value[:120]


def _evotor_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _load_tenant(tenant_id: str) -> dict:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("SELECT * FROM tenants WHERE id = ?"), (tenant_id,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Tenant not found: {tenant_id}")
        return dict(row)
    finally:
        conn.close()


def _load_stores(tenant_id: str, evotor_store_id: str | None = None) -> list[dict]:
    if evotor_store_id and evotor_store_id != "all":
        return [{"evotor_store_id": evotor_store_id, "name": None}]

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            SELECT evotor_store_id, name
            FROM tenant_stores
            WHERE tenant_id = ?
            ORDER BY is_primary DESC, created_at ASC
            """),
            (tenant_id,),
        )
        stores = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    if stores:
        return stores

    tenant = _load_tenant(tenant_id)
    if tenant.get("evotor_store_id"):
        return [{"evotor_store_id": tenant["evotor_store_id"], "name": None}]

    return []


def _load_mappings(tenant_id: str, evotor_store_id: str) -> list[dict]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            SELECT tenant_id, evotor_store_id, entity_type, evotor_id, ms_id, created_at, updated_at
            FROM mappings
            WHERE tenant_id = ?
              AND evotor_store_id = ?
              AND entity_type = 'product'
            ORDER BY evotor_id
            """),
            (tenant_id, evotor_store_id),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _parse_products_response(data):
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("items", "rows", "products", "data"):
            value = data.get(key)
            if isinstance(value, list):
                return value

    return []


def _fetch_evotor_products(evotor_token: str, evotor_store_id: str) -> list[dict]:
    all_products = []
    limit = 1000
    offset = 0

    while True:
        r = requests.get(
            f"{EVOTOR_BASE}/stores/{evotor_store_id}/products",
            headers=_evotor_headers(evotor_token),
            params={"limit": limit, "offset": offset},
            timeout=30,
        )

        if not r.ok:
            raise RuntimeError(
                f"Failed to fetch Evotor products store={evotor_store_id} "
                f"status={r.status_code} body={r.text[:500]}"
            )

        batch = _parse_products_response(r.json())
        all_products.extend(batch)

        if not batch or len(batch) < limit:
            break

        offset += limit

        if offset > 100000:
            raise RuntimeError("Too many Evotor products, pagination guard stopped snapshot")

    return all_products


def _get_current_product(evotor_token: str, evotor_store_id: str, product_id: str) -> dict | None:
    r = requests.get(
        f"{EVOTOR_BASE}/stores/{evotor_store_id}/products/{product_id}",
        headers=_evotor_headers(evotor_token),
        timeout=30,
    )

    if r.ok:
        return r.json()

    log.warning(
        "Cannot fetch current Evotor product product_id=%s store=%s status=%s body=%s",
        product_id,
        evotor_store_id,
        r.status_code,
        r.text[:300],
    )
    return None


def _put_evotor_product(evotor_token: str, evotor_store_id: str, product_id: str, payload: dict) -> tuple[bool, str]:
    r = requests.put(
        f"{EVOTOR_BASE}/stores/{evotor_store_id}/products/{product_id}",
        headers=_evotor_headers(evotor_token),
        json=payload,
        timeout=30,
    )

    if r.ok:
        return True, r.text[:200]

    return False, f"status={r.status_code} body={r.text[:500]}"


def _preserve_current_stock(snapshot_product: dict, current_product: dict | None) -> dict:
    payload = deepcopy(snapshot_product)

    if not current_product:
        return payload

    for key in STOCK_KEYS:
        if key in current_product:
            payload[key] = current_product[key]

    return payload


def create_product_snapshot(
    tenant_id: str,
    evotor_store_id: str | None = None,
    reason: str = "manual_lk",
) -> str:
    tenant = _load_tenant(tenant_id)
    evotor_token = tenant.get("evotor_token") or tenant.get("evotor_api_key")

    if not evotor_token:
        raise RuntimeError("Evotor token is not configured")

    stores = _load_stores(tenant_id, evotor_store_id)

    if not stores:
        raise RuntimeError("No tenant stores found")

    root = Path(os.getenv("PRODUCT_SNAPSHOT_DIR", "product_snapshots"))
    root.mkdir(parents=True, exist_ok=True)

    ts = time.strftime("%Y%m%d_%H%M%S")
    snapshot_id = f"{ts}_{_safe_name(tenant_id)}_{_safe_name(reason)}_{uuid.uuid4().hex[:8]}"
    snapshot_dir = root / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    manifest = {
        "snapshot_id": snapshot_id,
        "created_at": int(time.time()),
        "created_at_human": time.strftime("%Y-%m-%d %H:%M:%S"),
        "tenant_id": tenant_id,
        "reason": reason,
        "stores": [],
        "note": "Product card snapshot. Stock fields are preserved during rollback.",
    }

    for store in stores:
        store_id = store["evotor_store_id"]
        store_dir = snapshot_dir / store_id
        store_dir.mkdir(parents=True, exist_ok=True)

        products = _fetch_evotor_products(evotor_token, store_id)
        mappings = _load_mappings(tenant_id, store_id)

        (store_dir / "evotor_products.json").write_text(
            json.dumps(products, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (store_dir / "mappings.json").write_text(
            json.dumps(mappings, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        manifest["stores"].append({
            "evotor_store_id": store_id,
            "name": store.get("name"),
            "evotor_products_count": len(products),
            "mappings_count": len(mappings),
        })

    (snapshot_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    (root / f"LAST_PRODUCT_SNAPSHOT_{_safe_name(tenant_id)}").write_text(
        str(snapshot_dir),
        encoding="utf-8",
    )
    (root / "LAST_PRODUCT_SNAPSHOT").write_text(str(snapshot_dir), encoding="utf-8")

    log.info("Product snapshot created tenant_id=%s path=%s", tenant_id, snapshot_dir)

    return str(snapshot_dir)


def get_last_product_snapshot(tenant_id: str) -> str | None:
    root = Path(os.getenv("PRODUCT_SNAPSHOT_DIR", "product_snapshots"))
    tenant_last = root / f"LAST_PRODUCT_SNAPSHOT_{_safe_name(tenant_id)}"

    if tenant_last.exists():
        value = tenant_last.read_text(encoding="utf-8").strip()
        if value and Path(value).exists():
            return value

    if not root.exists():
        return None

    candidates = []
    for path in root.iterdir():
        if not path.is_dir():
            continue

        manifest_path = path / "manifest.json"
        if not manifest_path.exists():
            continue

        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue

        if manifest.get("tenant_id") == tenant_id:
            candidates.append((manifest.get("created_at", 0), str(path)))

    if not candidates:
        return None

    candidates.sort(reverse=True)
    return candidates[0][1]


def rollback_evotor_catalog_from_snapshot(
    tenant_id: str,
    snapshot_dir: str,
    evotor_store_id: str | None = None,
    limit: int = 0,
) -> dict:
    tenant = _load_tenant(tenant_id)
    evotor_token = tenant.get("evotor_token") or tenant.get("evotor_api_key")

    if not evotor_token:
        raise RuntimeError("Evotor token is not configured")

    root = Path(snapshot_dir)
    manifest_path = root / "manifest.json"

    if not manifest_path.exists():
        raise RuntimeError(f"manifest.json not found: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    if manifest.get("tenant_id") != tenant_id:
        raise RuntimeError("Snapshot tenant_id does not match current tenant_id")

    total = restored = failed = skipped = 0
    errors = []

    for store in manifest.get("stores", []):
        store_id = store["evotor_store_id"]

        if evotor_store_id and evotor_store_id != "all" and evotor_store_id != store_id:
            continue

        products_path = root / store_id / "evotor_products.json"

        if not products_path.exists():
            skipped += 1
            errors.append({"store": store_id, "error": f"Missing {products_path}"})
            continue

        products = json.loads(products_path.read_text(encoding="utf-8"))

        for product in products:
            if limit and total >= limit:
                break

            product_id = product.get("id")

            if not product_id:
                skipped += 1
                continue

            total += 1

            current = _get_current_product(evotor_token, store_id, product_id)
            payload = _preserve_current_stock(product, current)

            ok, info = _put_evotor_product(evotor_token, store_id, product_id, payload)

            if ok:
                restored += 1
            else:
                failed += 1
                errors.append({
                    "store": store_id,
                    "product_id": product_id,
                    "name": product.get("name"),
                    "error": info,
                })

            time.sleep(0.2)

        if limit and total >= limit:
            break

    result = {
        "status": "ok" if failed == 0 else "partial",
        "tenant_id": tenant_id,
        "snapshot_dir": snapshot_dir,
        "total": total,
        "restored": restored,
        "failed": failed,
        "skipped": skipped,
        "errors": errors[:50],
    }

    log.info("Product rollback result=%s", result)

    return result
