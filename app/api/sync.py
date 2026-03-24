import time
import logging
import requests

from fastapi import APIRouter, HTTPException
from app.db import get_connection
from app.stores.mapping_store import MappingStore

log = logging.getLogger("api.sync")
router = APIRouter(tags=["Sync"])

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
EVOTOR_BASE = "https://api.evotor.ru"


# ------------------------------------------------------------------------------
# Common helpers
# ------------------------------------------------------------------------------

def _now() -> int:
    return int(time.time())


def _load_tenant(tenant_id: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tenants WHERE id = ?", (tenant_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return dict(row)


def _evotor_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _ms_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json;charset=utf-8",
        "Accept-Encoding": "gzip",
    }


def _upsert_stock_status(
    tenant_id: str,
    status: str,
    started_at: int | None = None,
    last_sync_at: int | None = None,
    last_error: str | None = None,
    synced_items_count: int = 0,
    total_items_count: int = 0,
) -> None:
    now = _now()
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO stock_sync_status (
            tenant_id,
            status,
            started_at,
            updated_at,
            last_sync_at,
            last_error,
            synced_items_count,
            total_items_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tenant_id)
        DO UPDATE SET
            status=excluded.status,
            started_at=excluded.started_at,
            updated_at=excluded.updated_at,
            last_sync_at=excluded.last_sync_at,
            last_error=excluded.last_error,
            synced_items_count=excluded.synced_items_count,
            total_items_count=excluded.total_items_count
        """,
        (
            tenant_id,
            status,
            started_at,
            now,
            last_sync_at,
            last_error,
            synced_items_count,
            total_items_count,
        ),
    )
    conn.commit()
    conn.close()


def _get_stock_status_row(tenant_id: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM stock_sync_status WHERE tenant_id = ?", (tenant_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def _list_product_mappings(tenant_id: str) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT tenant_id, entity_type, evotor_id, ms_id, created_at, updated_at
        FROM mappings
        WHERE tenant_id = ? AND entity_type = 'product'
        ORDER BY created_at ASC
        """,
        (tenant_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


# ------------------------------------------------------------------------------
# Evotor / MoySklad data extraction
# ------------------------------------------------------------------------------

def _extract_ms_prices(ms_product: dict) -> tuple[float, float]:
    sale_price = 0.0
    cost_price = 0.0

    sale_prices = ms_product.get("salePrices", []) or []
    if sale_prices:
        try:
            sale_price = float(sale_prices[0].get("value", 0)) / 100
        except Exception:
            sale_price = 0.0

    buy_price = ms_product.get("buyPrice") or {}
    try:
        cost_price = float(buy_price.get("value", 0)) / 100
    except Exception:
        cost_price = 0.0

    return sale_price, cost_price


def _extract_ms_measure_name(ms_product: dict) -> str:
    uom = ms_product.get("uom")
    if isinstance(uom, str) and uom.strip():
        return uom.strip()
    if isinstance(uom, dict):
        for key in ("name", "code"):
            value = uom.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return "шт"


def _extract_ms_barcodes(ms_product: dict) -> list[str]:
    result: list[str] = []
    for barcode in ms_product.get("barcodes", []) or []:
        if isinstance(barcode, str) and barcode.strip():
            result.append(barcode.strip())
            continue
        if not isinstance(barcode, dict):
            continue
        value = (
            barcode.get("ean13")
            or barcode.get("code128")
            or barcode.get("ean8")
            or barcode.get("value")
        )
        if value:
            result.append(str(value).strip())

    seen = set()
    cleaned: list[str] = []
    for value in result:
        if not value or value in seen:
            continue
        seen.add(value)
        cleaned.append(value)
    return cleaned


def _map_ms_tax_to_evotor(ms_product: dict, current_tax: str | None = None) -> str:
    vat_enabled = ms_product.get("vatEnabled")
    vat_raw = ms_product.get("vatDecimal", ms_product.get("vat"))

    # vat=0 + vatEnabled=false => без НДС
    if vat_enabled is False:
        return "NO_VAT"

    vat_value: float | None = None
    if vat_raw is not None:
        try:
            vat_value = float(vat_raw)
        except (TypeError, ValueError):
            vat_value = None

    if vat_value is None:
        if current_tax:
            log.warning(
                "MS product has no VAT fields; preserving existing Evotor tax=%s product_id=%s",
                current_tax,
                ms_product.get("id"),
            )
            return current_tax
        return "NO_VAT"

    mapping = {
        0.0: "VAT_0",
        5.0: "VAT_5",
        7.0: "VAT_7",
        10.0: "VAT_10",
        18.0: "VAT_18",
        20.0: "VAT_20",
        22.0: "VAT_22",
    }

    evotor_tax = mapping.get(vat_value)
    if evotor_tax:
        return evotor_tax

    if current_tax:
        log.warning(
            "Unsupported MS VAT=%s; preserving existing Evotor tax=%s product_id=%s",
            vat_value,
            current_tax,
            ms_product.get("id"),
        )
        return current_tax

    log.warning(
        "Unsupported MS VAT=%s; falling back to NO_VAT product_id=%s",
        vat_value,
        ms_product.get("id"),
    )
    return "NO_VAT"


def _map_ms_tracking_type_to_evotor_type(ms_product: dict, current_type: str | None = None) -> str:
    tracking_type = ms_product.get("trackingType")
    if not tracking_type:
        return current_type or "NORMAL"

    mapping = {
        "MILK": "DAIRY_MARKED",
        # reserve room for future extensions if/when those categories appear in MS:
        # "WATER": "WATER_MARKED",
        # "TOBACCO": "TOBACCO_MARKED",
        # "MEDICINE": "MEDICINE_MARKED",
    }

    evotor_type = mapping.get(str(tracking_type).upper())
    if evotor_type:
        return evotor_type

    if current_type:
        log.warning(
            "Unsupported MS trackingType=%s; preserving existing Evotor type=%s product_id=%s",
            tracking_type,
            current_type,
            ms_product.get("id"),
        )
        return current_type

    return "NORMAL"


def _extract_classification_code(ms_product: dict) -> str | None:
    value = ms_product.get("tnved")
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _build_evotor_product_payload(
    ms_product: dict,
    evotor_id: str | None,
    current_product: dict | None = None,
    for_create: bool = False,
) -> dict:
    sale_price, cost_price = _extract_ms_prices(ms_product)
    current_product = current_product or {}

    payload = dict(current_product) if isinstance(current_product, dict) else {}

    base_fields = {
            "name": ms_product.get("name", ""),
            "price": sale_price,
            "cost_price": cost_price,
            "measure_name": _extract_ms_measure_name(ms_product),
            "tax": _map_ms_tax_to_evotor(ms_product, current_tax=current_product.get("tax")),
            "allow_to_sell": bool(current_product.get("allow_to_sell", True)),
            "description": ms_product.get("description", "") or "",
            "type": _map_ms_tracking_type_to_evotor_type(
                ms_product,
                current_type=current_product.get("type"),
            ),
            "article_number": ms_product.get("article", "") or "",
        }

    if not for_create and evotor_id:
        base_fields["id"] = evotor_id

    payload.update(base_fields)

    if ms_product.get("archived") is True:
        payload["allow_to_sell"] = False

    barcodes = _extract_ms_barcodes(ms_product)
    if barcodes:
        payload["barcodes"] = barcodes
    elif "barcodes" in payload and not payload["barcodes"]:
        payload.pop("barcodes", None)

    classification_code = _extract_classification_code(ms_product)
    # Evotor rejects classification_code for at least DAIRY_MARKED with
    # validation "must be null", so do not send it for marked milk.
    if classification_code and payload.get("type") not in {"DAIRY_MARKED"}:
        payload["classification_code"] = classification_code
    else:
        payload.pop("classification_code", None)

    # Product sync must not overwrite stock. Preserve quantity if the product already exists.
    if current_product and "quantity" in current_product:
        payload["quantity"] = current_product["quantity"]

    return payload


# ------------------------------------------------------------------------------
# Low-level API helpers
# ------------------------------------------------------------------------------

def _get_evotor_product(tenant: dict, evotor_product_id: str) -> dict:
    url = f"{EVOTOR_BASE}/stores/{tenant['evotor_store_id']}/products/{evotor_product_id}"
    r = requests.get(url, headers=_evotor_headers(tenant["evotor_token"]), timeout=20)
    if not r.ok:
        log.error("Evotor get_product error status=%s body=%s", r.status_code, r.text)
        r.raise_for_status()
    return r.json() if r.text else {}



def _get_ms_product(ms_token: str, ms_product_id: str) -> dict:
    url = f"{MS_BASE}/entity/product/{ms_product_id}"
    r = requests.get(url, headers=_ms_headers(ms_token), timeout=20)
    if not r.ok:
        log.error("MoySklad get_product error status=%s body=%s", r.status_code, r.text)
        r.raise_for_status()
    return r.json()


def _search_ms_products(ms_token: str, search: str | None = None, limit: int = 1000, offset: int = 0) -> dict:
    url = f"{MS_BASE}/entity/product"
    params = {"limit": limit, "offset": offset}
    if search:
        params["search"] = search

    r = requests.get(url, headers=_ms_headers(ms_token), params=params, timeout=30)
    if not r.ok:
        log.error("MoySklad search products error status=%s body=%s", r.status_code, r.text)
        r.raise_for_status()

    return r.json()


def _get_ms_product_stock(ms_token: str, ms_product_id: str) -> float:
    """
    Читает остаток товара через /report/stock/all.
    Использование assortment ненадёжно: при нулевом остатке rows может быть пустым.
    /report/stock/all всегда возвращает строку для товара, даже при quantity=0.
    """
    url = f"{MS_BASE}/report/stock/all"
    params = {"filter": f"product={MS_BASE}/entity/product/{ms_product_id}"}
    r = requests.get(url, headers=_ms_headers(ms_token), params=params, timeout=20)
    if not r.ok:
        log.error("MoySklad stock report error status=%s body=%s", r.status_code, r.text)
        r.raise_for_status()

    data = r.json()
    rows = data.get("rows", []) if isinstance(data, dict) else []
    if not rows:
        # Товар не найден в отчёте — возвращаем 0 (не исключение)
        log.warning("Product %s not found in MoySklad stock report, returning 0", ms_product_id)
        return 0.0

    row = rows[0]
    for key in ("stock", "quantity", "inStock"):
        value = row.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            pass

    # Строка есть, но поля остатка нет — товар найден, остаток 0
    log.warning("Stock field not found for product %s, returning 0", ms_product_id)
    return 0.0


def _get_evotor_products(evotor_token: str, store_id: str) -> list:
    url = f"{EVOTOR_BASE}/stores/{store_id}/products"
    r = requests.get(url, headers=_evotor_headers(evotor_token), timeout=30)

    if not r.ok:
        log.error("Evotor products error status=%s body=%s", r.status_code, r.text)
        r.raise_for_status()

    data = r.json()
    products = data.get("items", [])
    log.info("Fetched %s products from Evotor store=%s", len(products), store_id)
    return products


def _create_ms_product(ms_token: str, product: dict) -> str:
    payload = {
        "name": product["name"],
        "externalCode": product["id"],
        "description": product.get("description", ""),
        "salePrices": [
            {
                "value": round(float(product.get("price", 0)) * 100),
                "currency": {
                    "meta": {
                        "href": f"{MS_BASE}/entity/currency",
                        "type": "currency",
                        "mediaType": "application/json",
                    }
                },
                "priceType": {
                    "meta": {
                        "href": f"{MS_BASE}/context/companysettings/pricetype/",
                        "type": "pricetype",
                        "mediaType": "application/json",
                    }
                },
            }
        ],
    }

    if product.get("cost_price") is not None:
        payload["buyPrice"] = {
            "value": round(float(product.get("cost_price", 0)) * 100),
            "currency": {
                "meta": {
                    "href": f"{MS_BASE}/entity/currency",
                    "type": "currency",
                    "mediaType": "application/json",
                }
            },
        }

    barcodes = product.get("barcodes", [])
    if barcodes:
        # Передаём все штрихкоды, а не только первый
        payload["barcodes"] = [{"ean13": bc} for bc in barcodes]

    url = f"{MS_BASE}/entity/product"
    r = requests.post(url, headers=_ms_headers(ms_token), json=payload, timeout=20)

    if not r.ok:
        log.error("MoySklad create product error status=%s body=%s", r.status_code, r.text)
        r.raise_for_status()

    ms_product = r.json()
    return ms_product["id"]


# ------------------------------------------------------------------------------
# Initial sync Evotor -> MoySklad
# ------------------------------------------------------------------------------

@router.post("/sync/{tenant_id}/initial")
def initial_sync(tenant_id: str):
    tenant = _load_tenant(tenant_id)

    if tenant.get("sync_completed_at"):
        raise HTTPException(
            status_code=409,
            detail="Initial sync already completed. Use DELETE /tenants/{id}/complete-sync to reset.",
        )

    if not tenant.get("evotor_token"):
        raise HTTPException(status_code=400, detail="evotor_token not configured")
    if not tenant.get("evotor_store_id"):
        raise HTTPException(status_code=400, detail="evotor_store_id not configured")
    if not tenant.get("moysklad_token"):
        raise HTTPException(status_code=400, detail="moysklad_token not configured")

    try:
        products = _get_evotor_products(tenant["evotor_token"], tenant["evotor_store_id"])
    except Exception as e:
        log.error("Failed to fetch Evotor products tenant_id=%s err=%s", tenant_id, e)
        raise HTTPException(status_code=502, detail=f"Failed to fetch Evotor products: {e}")

    if not products:
        return {
            "status": "ok",
            "synced": 0,
            "skipped": 0,
            "failed": 0,
            "message": "No products found in Evotor",
        }

    store = MappingStore()
    synced = 0
    skipped = 0
    failed = 0
    errors = []

    for product in products:
        evotor_id = product.get("id")
        if not evotor_id:
            skipped += 1
            continue

        existing = store.get_by_evotor_id(tenant_id=tenant_id, entity_type="product", evotor_id=evotor_id)
        if existing:
            log.info("Skipping already mapped product evotor_id=%s ms_id=%s", evotor_id, existing)
            skipped += 1
            continue

        try:
            ms_id = _create_ms_product(tenant["moysklad_token"], product)
            log.info("Created MS product evotor_id=%s ms_id=%s name=%s", evotor_id, ms_id, product.get("name"))
        except Exception as e:
            log.error("Failed to create MS product evotor_id=%s name=%s err=%s", evotor_id, product.get("name"), e)
            failed += 1
            errors.append({"evotor_id": evotor_id, "name": product.get("name"), "error": str(e)})
            continue

        ok = store.upsert_mapping(tenant_id=tenant_id, entity_type="product", evotor_id=evotor_id, ms_id=ms_id)
        if ok:
            synced += 1
        else:
            log.warning("Mapping conflict evotor_id=%s ms_id=%s", evotor_id, ms_id)
            failed += 1

    if failed == 0:
        conn = get_connection()
        conn.execute("UPDATE tenants SET sync_completed_at = ? WHERE id = ?", (_now(), tenant_id))
        conn.commit()
        conn.close()
        log.info("Initial sync completed tenant_id=%s synced=%s", tenant_id, synced)

    return {
        "status": "ok" if failed == 0 else "partial",
        "synced": synced,
        "skipped": skipped,
        "failed": failed,
        "errors": errors,
        "sync_mode": "moysklad" if failed == 0 else "evotor",
    }


@router.get("/sync/{tenant_id}/status")
def sync_status(tenant_id: str):
    tenant = _load_tenant(tenant_id)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) as cnt FROM mappings WHERE tenant_id = ? AND entity_type = 'product'",
        (tenant_id,),
    )
    mapping_count = cur.fetchone()["cnt"]
    conn.close()

    return {
        "tenant_id": tenant_id,
        "sync_mode": "moysklad" if tenant.get("sync_completed_at") else "evotor",
        "sync_completed_at": tenant.get("sync_completed_at"),
        "product_mappings_count": mapping_count,
        "evotor_store_configured": bool(tenant.get("evotor_store_id")),
        "moysklad_configured": bool(tenant.get("ms_organization_id")),
    }


# ------------------------------------------------------------------------------
# Utility endpoint: list MoySklad products with API/UI IDs
# ------------------------------------------------------------------------------

@router.get("/sync/{tenant_id}/moysklad/products")
def list_moysklad_products(tenant_id: str, search: str | None = None):
    tenant = _load_tenant(tenant_id)

    if not tenant.get("moysklad_token"):
        raise HTTPException(status_code=400, detail="moysklad_token not configured")

    try:
        data = _search_ms_products(tenant["moysklad_token"], search=search)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch MoySklad products: {e}")

    items = []
    for row in data.get("rows", []):
        meta = row.get("meta") or {}
        uuid_href = meta.get("uuidHref")
        ui_id = None
        if isinstance(uuid_href, str) and "id=" in uuid_href:
            ui_id = uuid_href.split("id=")[-1]

        items.append(
            {
                "name": row.get("name"),
                "ms_id": row.get("id"),
                "ui_id": ui_id,
                "ui_url": uuid_href,
                "tracking_type": row.get("trackingType"),
                "vat": row.get("vat"),
                "vat_enabled": row.get("vatEnabled"),
                "is_serial_trackable": row.get("isSerialTrackable"),
            }
        )

    return {"count": len(items), "items": items}


# ------------------------------------------------------------------------------
# Product sync MoySklad -> Evotor (without stock overwrite)
# ------------------------------------------------------------------------------

@router.post("/sync/{tenant_id}/product/{ms_product_id}")
def sync_product_to_evotor(tenant_id: str, ms_product_id: str):
    """
    Синхронизирует карточку товара из МойСклад -> Эвотор.
    Остаток здесь НЕ трогается. Для остатка используйте /stock/... endpoint'ы.

    Надёжная схема:
    - если mapping уже есть -> обновляем товар в Эвотор через PUT /products/{evotor_id}
    - если mapping нет -> создаём новый товар через POST /products и сохраняем evotor_id из ответа
    """
    tenant = _load_tenant(tenant_id)

    if not tenant.get("sync_completed_at"):
        raise HTTPException(
            status_code=409,
            detail="Initial sync not completed. Run POST /sync/{tenant_id}/initial first.",
        )

    if not tenant.get("evotor_token"):
        raise HTTPException(status_code=400, detail="evotor_token not configured")
    if not tenant.get("evotor_store_id"):
        raise HTTPException(status_code=400, detail="evotor_store_id not configured")
    if not tenant.get("moysklad_token"):
        raise HTTPException(status_code=400, detail="moysklad_token not configured")

    try:
        ms_product = _get_ms_product(tenant["moysklad_token"], ms_product_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch MS product: {e}")

    store = MappingStore()
    existing_evotor_id = store.get_by_ms_id(
        tenant_id=tenant_id,
        entity_type="product",
        ms_id=ms_product_id,
    )

    try:
        if existing_evotor_id:
            current_evotor_product = _get_evotor_product(tenant, existing_evotor_id)
            evotor_payload = _build_evotor_product_payload(
                ms_product,
                evotor_id=existing_evotor_id,
                current_product=current_evotor_product,
                for_create=False,
            )

            url = f"{EVOTOR_BASE}/stores/{tenant['evotor_store_id']}/products/{existing_evotor_id}"
            r = requests.put(
                url,
                headers=_evotor_headers(tenant["evotor_token"]),
                json=evotor_payload,
                timeout=20,
            )
            if not r.ok:
                log.error(
                    "Evotor update_product error status=%s ms_id=%s evotor_id=%s payload=%s body=%s",
                    r.status_code,
                    ms_product_id,
                    existing_evotor_id,
                    evotor_payload,
                    r.text,
                )
                r.raise_for_status()

            log.info(
                "Evotor product updated evotor_id=%s ms_id=%s type=%s tax=%s",
                existing_evotor_id,
                ms_product_id,
                evotor_payload.get("type"),
                evotor_payload.get("tax"),
            )
            return {
                "status": "updated",
                "ms_product_id": ms_product_id,
                "evotor_product_id": existing_evotor_id,
                "evotor_payload": evotor_payload,
            }

        # create new product via POST so Evotor cloud generates identifiers
        evotor_payload = _build_evotor_product_payload(
            ms_product,
            evotor_id=None,
            current_product=None,
            for_create=True,
        )
        url = f"{EVOTOR_BASE}/stores/{tenant['evotor_store_id']}/products"
        r = requests.post(
            url,
            headers=_evotor_headers(tenant["evotor_token"]),
            json=evotor_payload,
            timeout=20,
        )
        if not r.ok:
            log.error(
                "Evotor create_product error status=%s ms_id=%s payload=%s body=%s",
                r.status_code,
                ms_product_id,
                evotor_payload,
                r.text,
            )
            r.raise_for_status()

        created = r.json() if r.text else {}
        created_id = created.get("id")
        if not created_id:
            raise HTTPException(status_code=502, detail="Evotor create product response has no id")

        ok = store.upsert_mapping(
            tenant_id=tenant_id,
            entity_type="product",
            evotor_id=created_id,
            ms_id=ms_product_id,
        )
        if not ok:
            raise HTTPException(status_code=409, detail="Failed to save product mapping")

        log.info(
            "Evotor product created evotor_id=%s ms_id=%s type=%s tax=%s",
            created_id,
            ms_product_id,
            evotor_payload.get("type"),
            evotor_payload.get("tax"),
        )
        return {
            "status": "created",
            "ms_product_id": ms_product_id,
            "evotor_product_id": created_id,
            "evotor_payload": evotor_payload,
            "evotor_response": created,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to sync product to Evotor: {e}")


# ------------------------------------------------------------------------------
# Stock sync MoySklad -> Evotor
# ------------------------------------------------------------------------------

@router.get("/sync/{tenant_id}/stock/status")
def stock_sync_status(tenant_id: str):
    tenant = _load_tenant(tenant_id)
    row = _get_stock_status_row(tenant_id)

    if row:
        return {
            "tenant_id": tenant_id,
            "status": row["status"],
            "last_sync_time": row["last_sync_at"],
            "last_error": row["last_error"],
            "count_synced_items": row["synced_items_count"],
            "total_items_count": row["total_items_count"],
        }

    configured = bool(
        tenant.get("sync_completed_at")
        and tenant.get("moysklad_token")
        and tenant.get("evotor_token")
        and tenant.get("evotor_store_id")
    )
    return {
        "tenant_id": tenant_id,
        "status": "configured" if configured else "error",
        "last_sync_time": None,
        "last_error": None if configured else "Sync is not fully configured",
        "count_synced_items": 0,
        "total_items_count": 0,
    }


@router.post("/sync/{tenant_id}/stock/reconcile")
def reconcile_stock_to_evotor(tenant_id: str):
    from app.clients.evotor_client import EvotorClient
    from app.clients.moysklad_client import MoySkladClient

    tenant = _load_tenant(tenant_id)

    if not tenant.get("sync_completed_at"):
        raise HTTPException(
            status_code=409,
            detail="Initial sync not completed. Run POST /sync/{tenant_id}/initial first.",
        )

    mappings = _list_product_mappings(tenant_id)
    total_items = len(mappings)
    started_at = _now()

    _upsert_stock_status(
        tenant_id=tenant_id,
        status="in_progress",
        started_at=started_at,
        last_sync_at=None,
        last_error=None,
        synced_items_count=0,
        total_items_count=total_items,
    )

    if total_items == 0:
        _upsert_stock_status(
            tenant_id=tenant_id,
            status="ok",
            started_at=started_at,
            last_sync_at=_now(),
            last_error=None,
            synced_items_count=0,
            total_items_count=0,
        )
        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "synced": 0,
            "failed": 0,
            "errors": [],
            "message": "No product mappings found",
        }

    ms_client = MoySkladClient(tenant_id)
    evotor_client = EvotorClient(tenant_id)

    synced = 0
    failed = 0
    errors = []

    for item in mappings:
        ms_id = item["ms_id"]
        evotor_id = item["evotor_id"]
        try:
            stock_value = ms_client.get_product_stock(ms_id)
            evotor_client.update_product_stock(evotor_id, stock_value)
            synced += 1
            log.info(
                "Stock synced tenant_id=%s ms_id=%s evotor_id=%s quantity=%s",
                tenant_id,
                ms_id,
                evotor_id,
                stock_value,
            )
        except Exception as e:
            failed += 1
            err_text = str(e)
            errors.append(
                {
                    "ms_product_id": ms_id,
                    "evotor_product_id": evotor_id,
                    "error": err_text,
                }
            )
            log.error(
                "Stock reconcile failed tenant_id=%s ms_id=%s evotor_id=%s err=%s",
                tenant_id,
                ms_id,
                evotor_id,
                err_text,
            )

    final_status = "ok" if failed == 0 else "error"
    last_error = None if failed == 0 else errors[-1]["error"]

    _upsert_stock_status(
        tenant_id=tenant_id,
        status=final_status,
        started_at=started_at,
        last_sync_at=_now(),
        last_error=last_error,
        synced_items_count=synced,
        total_items_count=total_items,
    )

    return {
        "status": final_status,
        "tenant_id": tenant_id,
        "synced": synced,
        "failed": failed,
        "errors": errors,
    }


@router.post("/sync/{tenant_id}/stock/{ms_product_id}")
def sync_stock_to_evotor(tenant_id: str, ms_product_id: str):
    """
    Синхронизирует остаток одного товара из МойСклад -> Эвотор.
    """
    from app.clients.evotor_client import EvotorClient
    from app.clients.moysklad_client import MoySkladClient

    tenant = _load_tenant(tenant_id)

    if not tenant.get("sync_completed_at"):
        raise HTTPException(
            status_code=409,
            detail="Initial sync not completed. Run POST /sync/{tenant_id}/initial first.",
        )

    store = MappingStore()
    evotor_product_id = store.get_by_ms_id(
        tenant_id=tenant_id,
        entity_type="product",
        ms_id=ms_product_id,
    )
    if not evotor_product_id:
        raise HTTPException(status_code=404, detail="Product mapping not found")

    try:
        ms_client = MoySkladClient(tenant_id)
        evotor_client = EvotorClient(tenant_id)

        stock_value = ms_client.get_product_stock(ms_product_id)
        evotor_client.update_product_stock(evotor_product_id, stock_value)

        # Одиночная синхронизация не перезаписывает агрегированный статус reconcile.
        # Просто обновляем last_error=None и last_sync_at если статус уже ok/error,
        # не трогая total_items_count из предыдущего reconcile.
        existing = _get_stock_status_row(tenant_id)
        _upsert_stock_status(
            tenant_id=tenant_id,
            status="ok",
            started_at=existing["started_at"] if existing else _now(),
            last_sync_at=_now(),
            last_error=None,
            synced_items_count=existing["synced_items_count"] if existing else 1,
            total_items_count=existing["total_items_count"] if existing else 1,
        )

        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "ms_product_id": ms_product_id,
            "evotor_product_id": evotor_product_id,
            "quantity": stock_value,
        }
    except Exception as e:
        err_text = str(e)
        existing = _get_stock_status_row(tenant_id)
        _upsert_stock_status(
            tenant_id=tenant_id,
            status="error",
            started_at=existing["started_at"] if existing else _now(),
            last_sync_at=_now(),
            last_error=err_text,
            synced_items_count=existing["synced_items_count"] if existing else 0,
            total_items_count=existing["total_items_count"] if existing else 1,
        )
        raise HTTPException(status_code=502, detail=f"Failed to sync stock to Evotor: {err_text}")