import os
import time
import logging
import requests

from fastapi import APIRouter, HTTPException
from app.observability.metrics import (
    fiscalization_request_duration_seconds,
    fiscalization_requests_total,
    fiscalization_state_total,
    fiscalization_status_checks_total,
    observe_duration,
)
from app.db import get_connection, adapt_query as aq
from app.stores.mapping_store import MappingStore
from app.services.stale_mapping_service import cleanup_stale_product_mappings

log = logging.getLogger("api.sync")


def _sync_extra(
    tenant_id: str | None = None,
    *,
    uid: str | None = None,
    doc_id: str | None = None,
    operation: str,
    status: str | None = None,
    exception_type: str | None = None,
    component: str = "sync",
) -> dict:
    payload = {
        "component": component,
        "operation": operation,
    }

    if tenant_id is not None:
        payload["tenant_id"] = tenant_id
    if uid is not None:
        payload["uid"] = uid
    if doc_id is not None:
        payload["doc_id"] = doc_id
    if status is not None:
        payload["status"] = status
    if exception_type is not None:
        payload["exception_type"] = exception_type

    return payload

router = APIRouter(tags=["Sync"])

MS_BASE = os.getenv("MS_BASE_URL", "https://api.moysklad.ru/api/remap/1.2").rstrip("/")
EVOTOR_BASE = "https://api.evotor.ru"


# ------------------------------------------------------------------------------
# Common helpers
# ------------------------------------------------------------------------------

def _now() -> int:
    return int(time.time())


def _fiscalization_state_label(state_code: int) -> str:
    state_labels = {
        1: "new",
        2: "sent_to_device",
        5: "accepted_by_device",
        9: "error",
        10: "fiscalized",
    }
    return state_labels.get(state_code, "unknown")


def _load_tenant(tenant_id: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(aq("SELECT * FROM tenants WHERE id = ?"), (tenant_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return dict(row)


def _load_store(tenant_id: str, evotor_store_id: str | None = None) -> dict:
    """
    Загружает конфигурацию магазина из tenant_stores.
    Если evotor_store_id не передан — берёт primary магазин.
    Возвращает dict совместимый с tenant dict (поля evotor_store_id, ms_store_id и т.д.)
    """
    conn = get_connection()
    cur = conn.cursor()

    if evotor_store_id:
        cur.execute(
            aq("""
            SELECT ts.*, t.evotor_token, t.moysklad_token, t.ms_agent_id as tenant_ms_agent_id
            FROM tenant_stores ts
            JOIN tenants t ON t.id = ts.tenant_id
            WHERE ts.tenant_id = ? AND ts.evotor_store_id = ?
            """),
            (tenant_id, evotor_store_id),
        )
    else:
        cur.execute(
            aq("""
            SELECT ts.*, t.evotor_token, t.moysklad_token, t.ms_agent_id as tenant_ms_agent_id
            FROM tenant_stores ts
            JOIN tenants t ON t.id = ts.tenant_id
            WHERE ts.tenant_id = ? AND ts.is_primary = 1
            ORDER BY ts.created_at ASC LIMIT 1
            """),
            (tenant_id,),
        )

    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Store not found for tenant {tenant_id}" + (f" store {evotor_store_id}" if evotor_store_id else " (no primary store)"),
        )

    store = dict(row)
    # ms_agent_id: приоритет у store-level, fallback на tenant-level
    if not store.get("ms_agent_id"):
        store["ms_agent_id"] = store.get("tenant_ms_agent_id")

    return store


def _merge_tenant_store(tenant: dict, store: dict) -> dict:
    """
    Мержит tenant + store в единый dict для передачи в существующие функции.
    store-level значения имеют приоритет над tenant-level.
    """
    merged = dict(tenant)
    for key in ("evotor_store_id", "ms_store_id", "ms_organization_id", "ms_agent_id", "sync_completed_at"):
        if store.get(key) is not None:
            merged[key] = store[key]
    return merged


def _evotor_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _ms_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json;charset=utf-8",
        "Accept-Encoding": "gzip",
    }


def _ms_get_with_retry(
    url: str,
    *,
    headers: dict,
    params: dict | None = None,
    timeout: int = 20,
    context: str = "moysklad.get",
):
    """
    GET в МойСклад с мягким retry/backoff.

    Нужен для ручной синхронизации МС→Эвотор, потому что МойСклад
    может возвращать 429 при серии запросов к товарам/остаткам.
    """
    max_attempts = int(os.getenv("MS_API_RETRY_MAX", "5"))
    base_wait = float(os.getenv("MS_API_RETRY_BASE_WAIT_SEC", "1.0"))
    success_delay = float(os.getenv("MS_API_REQUEST_DELAY_SEC", "0.20"))

    last_response = None

    for attempt in range(1, max_attempts + 1):
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
        last_response = r

        if r.status_code != 429:
            if success_delay > 0:
                time.sleep(success_delay)
            return r

        retry_after = r.headers.get("Retry-After")
        if retry_after:
            try:
                wait = float(retry_after)
            except ValueError:
                wait = base_wait * attempt
        else:
            wait = base_wait * attempt

        log.warning(
            "%s rate limited status=429 attempt=%s/%s wait=%.2fs url=%s",
            context,
            attempt,
            max_attempts,
            wait,
            url,
        )
        time.sleep(wait)

    return last_response


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
    cur = conn.cursor()
    try:
        cur.execute(
            aq(
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
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id)
            DO UPDATE SET
                status = EXCLUDED.status,
                started_at = EXCLUDED.started_at,
                updated_at = EXCLUDED.updated_at,
                last_sync_at = EXCLUDED.last_sync_at,
                last_error = EXCLUDED.last_error,
                synced_items_count = EXCLUDED.synced_items_count,
                total_items_count = EXCLUDED.total_items_count
            """
            ),
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
    finally:
        cur.close()
        conn.close()

def _get_stock_status_row(tenant_id: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(aq("SELECT * FROM stock_sync_status WHERE tenant_id = ?"), (tenant_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def _list_product_mappings(tenant_id: str) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        aq("""
        SELECT tenant_id, evotor_store_id, entity_type, evotor_id, ms_id, created_at, updated_at
        FROM mappings
        WHERE tenant_id = ? AND entity_type = 'product'
        ORDER BY created_at ASC
        """),
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
            or barcode.get("gtin")
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
        20.0: "VAT_22",  # С 2026 года НДС 20% -> VAT_22 в Эвотор
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
        "MILK":                 "DAIRY_MARKED",
        "TOBACCO":              "TOBACCO_MARKED",
        "SHOES":                "SHOES_MARKED",
        "LP_CLOTHES":           "LIGHT_INDUSTRY_MARKED",
        "LP_LINENS":            "LIGHT_INDUSTRY_MARKED",
        "PERFUMERY":            "PERFUME_MARKED",
        "ELECTRONICS":          "PHOTOS_MARKED",
        "TIRES":                "TYRES_MARKED",
        "CAMERA_PHOTO":         "PHOTOS_MARKED",
        "WATER":                "WATER_MARKED",
        "OTP":                  "TOBACCO_PRODUCTS_MARKED",
        "BICYCLE":              "BIKE_MARKED",
        "WHEELCHAIRS":          "WHEELCHAIRS_MARKED",
        "ALCOHOL":              "ALCOHOL_MARKED",
        "MEDICINE":             "MEDICINE_MARKED",
        "DIETARY_SUPPLEMENT":   "DIETARY_SUPPLEMENTS_MARKED",
        "ANTISEPTIC":           "ANTISEPTIC_MARKED",
        "JUICE":                "JUICE_MARKED",
        "MEDICAL_DEVICES":      "MEDICAL_DEVICES_MARKED",
        "VETERINARY":           "VETERINARY_MARKED",
        "CAVIAR":               "CAVIAR_MARKED",
        "PET_FOOD":             "PET_FOOD_MARKED",
        "VEGETABLE_OIL":        "VEGETABLE_OIL_MARKED",
        "FUR":                  "FUR_MARKED",
        "AUTO_FLUIDS":          "AUTO_FLUIDS_MARKED",
        "CHEMICALS":            "CHEMICALS_MARKED",
        "JEWELRY":              "JEWELRY_MARKED",
        "MEDICINE":       "MEDICINE_MARKED",
        "NABEER":         "NOT_ALCOHOL_BEER_MARKED",
        "NICOTINE":        "TOBACCO_STICKS_MARKED",
        "FOOD_SUPPLEMENT": "DIETARY_SUPPLEMENTS_MARKED",
        "ANTISEPTIC":      "ANTISEPTIC_MARKED",
        "MEDICAL_DEVICES": "MEDICAL_DEVICES_MARKED",
        "SOFT_DRINKS":     "JUICE_MARKED",
        "VETPHARMA":       "VETERINARY_MARKED",
        "SEAFOOD":         "PRESERVES_MARKED",
        "VEGETABLE_OIL":   "VEGETABLE_OIL_MARKED",
        "ANIMAL_FOOD":     "PET_FOOD_MARKED",
        "MOTOR_OIL":       "AUTO_FLUIDS_MARKED",
        "GROCERIES":       "NORMAL",
        "COSMETICS":       "PERFUME_MARKED",
        "FUR":             "FUR_MARKED",
        "NOT_TRACKED":    "NORMAL",
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


def _apply_product_group(
    payload: dict,
    ms_product: dict,
    tenant_id: str,
    evotor_store_id: str,
    evotor_token: str,
) -> None:
    """Добавляет parent_id в payload если товар МС имеет папку."""
    ms_folder_id, ms_folder_name = _get_ms_folder_info(ms_product)
    if ms_folder_id and ms_folder_name:
        evotor_group_id = _get_or_create_evotor_group(
            tenant_id=tenant_id,
            evotor_store_id=evotor_store_id,
            evotor_token=evotor_token,
            ms_folder_id=ms_folder_id,
            ms_folder_name=ms_folder_name,
        )
        if evotor_group_id:
            payload["parent_id"] = evotor_group_id


def _get_or_create_evotor_group(
    tenant_id: str,
    evotor_store_id: str,
    evotor_token: str,
    ms_folder_id: str,
    ms_folder_name: str,
) -> str | None:
    """
    Возвращает evotor_group_id для папки МС.
    Создаёт группу в Эвотор если её ещё нет.
    """
    import requests as _req

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            SELECT evotor_group_id FROM product_group_mappings
            WHERE tenant_id = ? AND evotor_store_id = ? AND ms_folder_id = ?
            """),
            (tenant_id, evotor_store_id, ms_folder_id),
        )
        row = cur.fetchone()
        if row:
            return row["evotor_group_id"]
    finally:
        conn.close()

    # Ищем существующую группу в Эвотор по имени
    headers = _evotor_headers(evotor_token)
    r_list = _req.get(
        f"{EVOTOR_BASE}/stores/{evotor_store_id}/product-groups",
        headers=headers,
        timeout=20,
    )
    if r_list.ok:
        existing_groups = r_list.json().get("items", [])
        for g in existing_groups:
            if g.get("name") == ms_folder_name:
                evotor_group_id = g["id"]
                log.info("Found existing Evotor group name=%s id=%s", ms_folder_name, evotor_group_id)
                # Сохраняем маппинг
                now = _now()
                conn = get_connection()
                try:
                    cur = conn.cursor()
                    cur.execute(
                        aq("""
                        INSERT INTO product_group_mappings
                            (tenant_id, evotor_store_id, ms_folder_id, ms_folder_name, evotor_group_id, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (tenant_id, evotor_store_id, ms_folder_id)
                        DO UPDATE SET evotor_group_id = EXCLUDED.evotor_group_id, updated_at = EXCLUDED.updated_at
                        """),
                        (tenant_id, evotor_store_id, ms_folder_id, ms_folder_name, evotor_group_id, now, now),
                    )
                    conn.commit()
                finally:
                    conn.close()
                return evotor_group_id

    # Создаём группу в Эвотор
    r = _req.post(
        f"{EVOTOR_BASE}/stores/{evotor_store_id}/product-groups",
        headers=headers,
        json={"name": ms_folder_name},
        timeout=20,
    )
    if not r.ok:
        log.error(
            "Failed to create Evotor group name=%s status=%s body=%s",
            ms_folder_name, r.status_code, r.text,
        )
        return None

    evotor_group_id = r.json().get("id")
    if not evotor_group_id:
        return None

    # Сохраняем маппинг
    now = _now()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            INSERT INTO product_group_mappings
                (tenant_id, evotor_store_id, ms_folder_id, ms_folder_name, evotor_group_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (tenant_id, evotor_store_id, ms_folder_id)
            DO UPDATE SET evotor_group_id = EXCLUDED.evotor_group_id, updated_at = EXCLUDED.updated_at
            """),
            (tenant_id, evotor_store_id, ms_folder_id, ms_folder_name, evotor_group_id, now, now),
        )
        conn.commit()
        log.info(
            "Created Evotor group name=%s group_id=%s store=%s",
            ms_folder_name, evotor_group_id, evotor_store_id,
        )
    finally:
        conn.close()

    return evotor_group_id


def _get_ms_folder_info(ms_product: dict) -> tuple[str | None, str | None]:
    """Возвращает (ms_folder_id, ms_folder_name) из товара МС."""
    folder = ms_product.get("productFolder")
    if not folder:
        return None, None
    if isinstance(folder, dict):
        # Может быть либо полный объект либо meta-ссылка
        folder_id = folder.get("id")
        folder_name = folder.get("name")
        if not folder_id:
            href = folder.get("meta", {}).get("href", "")
            folder_id = href.rstrip("/").split("/")[-1].split("?")[0] or None
        return folder_id, folder_name
    return None, None


def _build_evotor_product_payload(
    ms_product: dict,
    evotor_id: str | None,
    current_product: dict | None = None,
    for_create: bool = False,
) -> dict:
    sale_price, cost_price = _extract_ms_prices(ms_product)
    current_product = current_product or {}

    payload = dict(current_product) if isinstance(current_product, dict) else {}

    ms_product_id = ms_product.get("id", "")
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
    # Всегда передаём externalCode = ms_id для идемпотентности
    if ms_product_id:
        base_fields["externalCode"] = ms_product_id

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
    _classification_allowed = {
        "NORMAL", "DAIRY_MARKED", "WATER_MARKED", "JEWELRY_MARKED", "FUR_MARKED",
        "BIKE_MARKED", "LOTTERY_TICKET", "LOTTERY_PRIZE", "DIETARY_SUPPLEMENTS_MARKED",
        "JUICE_MARKED", "MEDICAL_DEVICES_MARKED", "WHEELCHAIRS_MARKED", "FURSLP_MARKED",
        "AUTO_FLUIDS_MARKED", "CHEMICALS_MARKED",
    }
    if classification_code and payload.get("type") in _classification_allowed:
        payload["classification_code"] = classification_code
    else:
        payload.pop("classification_code", None)

    if current_product and "quantity" in current_product:
        payload["quantity"] = current_product["quantity"]

    # Очищаем алкогольные поля если тип не алкогольный
    alcohol_types = {"ALCOHOL_MARKED", "ALCOHOL_NOT_MARKED", "BEER_MARKED", "BEER_MARKED_KEG", "NOT_ALCOHOL_BEER_MARKED", "ANTISEPTIC_MARKED"}
    if payload.get("type") not in alcohol_types:
        for field in ("alcohol_by_volume", "alcohol_product_kind_code", "tare_volume", "is_excisable"):
            payload.pop(field, None)

    # Убираем служебные поля Эвотора которые нельзя передавать
    for field in ("store_id", "user_id", "created_at", "updated_at", "quantity_in_package"):
        payload.pop(field, None)

    # Убираем alcocodes если пустой список
    if not payload.get("alcocodes"):
        payload.pop("alcocodes", None)

    # classification_code только для типов которые его поддерживают (по документации Эвотор)
    supported_classification = {
        "NORMAL", "DAIRY_MARKED", "WATER_MARKED", "JEWELRY_MARKED", "FUR_MARKED",
        "BIKE_MARKED", "LOTTERY_TICKET", "LOTTERY_PRIZE", "DIETARY_SUPPLEMENTS_MARKED",
        "JUICE_MARKED", "MEDICAL_DEVICES_MARKED", "WHEELCHAIRS_MARKED", "FURSLP_MARKED",
        "AUTO_FLUIDS_MARKED", "CHEMICALS_MARKED",
    }
    if payload.get("type") not in supported_classification:
        payload.pop("classification_code", None)

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

def _find_evotor_product_by_external_code(tenant: dict, external_code: str) -> dict | None:
    """
    Ищет товар в Эвотор по externalCode (= ms_id).
    Возвращает dict товара или None если не найден.
    """
    try:
        url = f"{EVOTOR_BASE}/stores/{tenant['evotor_store_id']}/products"
        r = requests.get(
            url,
            headers=_evotor_headers(tenant["evotor_token"]),
            timeout=20,
        )
        if not r.ok:
            return None
        data = r.json()
        items = data.get("items", data) if isinstance(data, dict) else data
        for item in items:
            if item.get("externalCode") == external_code:
                return item
        return None
    except Exception as e:
        log.warning("_find_evotor_product_by_external_code failed code=%s err=%s", external_code, e)
        return None

def _get_ms_product(ms_token: str, ms_product_id: str, expand: str | None = None) -> dict:
    url = f"{MS_BASE}/entity/product/{ms_product_id}"
    params = {}
    if expand:
        params["expand"] = expand
    r = _ms_get_with_retry(
        url,
        headers=_ms_headers(ms_token),
        params=params,
        timeout=20,
        context="MoySklad get_product",
    )
    if not r.ok:
        log.error("MoySklad get_product error status=%s body=%s", r.status_code, r.text)
        r.raise_for_status()
    return r.json()


def _search_ms_products(ms_token: str, search: str | None = None, limit: int = 1000, offset: int = 0) -> dict:
    url = f"{MS_BASE}/entity/product"
    params = {"limit": limit, "offset": offset, "expand": "uom"}
    if search:
        params["search"] = search

    r = _ms_get_with_retry(
        url,
        headers=_ms_headers(ms_token),
        params=params,
        timeout=30,
        context="MoySklad search products",
    )
    if not r.ok:
        log.error("MoySklad search products error status=%s body=%s", r.status_code, r.text)
        r.raise_for_status()

    return r.json()

def _get_ms_product_stock_for_store(ms_token: str, ms_product_id: str, ms_store_id: str) -> float:
    url = f"{MS_BASE}/report/stock/all"
    params = {
        "filter": (
            f"product={MS_BASE}/entity/product/{ms_product_id};"
            f"store={MS_BASE}/entity/store/{ms_store_id}"
        )
    }
    r = _ms_get_with_retry(
        url,
        headers=_ms_headers(ms_token),
        params=params,
        timeout=20,
        context="MoySklad stock report by store",
    )
    if not r.ok:
        log.error(
            "MoySklad stock report by store error status=%s body=%s ms_product_id=%s ms_store_id=%s",
            r.status_code,
            r.text,
            ms_product_id,
            ms_store_id,
        )
        r.raise_for_status()

    data = r.json()
    rows = data.get("rows", []) if isinstance(data, dict) else []
    if not rows:
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

    return 0.0

def _get_ms_product_stock_by_store(ms_token: str, ms_product_id: str, ms_store_id: str) -> float:
    """Получить остаток товара по конкретному складу МС."""
    import requests as _req
    url = f"{MS_BASE}/report/stock/all"
    store_url = f"{MS_BASE}/entity/store/{ms_store_id}"
    params = {
        "filter": f"product={MS_BASE}/entity/product/{ms_product_id};store={store_url}"
    }
    r = _req.get(url, headers=_ms_headers(ms_token), params=params, timeout=20)
    if not r.ok:
        log.error("MoySklad stock by store error status=%s", r.status_code)
        r.raise_for_status()
    rows = r.json().get("rows", [])
    if not rows:
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
    return 0.0


def _get_ms_product_stock(ms_token: str, ms_product_id: str) -> float:
    url = f"{MS_BASE}/report/stock/all"
    params = {"filter": f"product={MS_BASE}/entity/product/{ms_product_id}"}
    r = requests.get(url, headers=_ms_headers(ms_token), params=params, timeout=20)
    if not r.ok:
        log.error("MoySklad stock report error status=%s body=%s", r.status_code, r.text)
        r.raise_for_status()

    data = r.json()
    rows = data.get("rows", []) if isinstance(data, dict) else []
    if not rows:
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

    log.warning("Stock field not found for product %s, returning 0", ms_product_id)
    return 0.0

def _extract_stock_value_from_row(row: dict) -> float:
    for key in ("stock", "quantity", "inStock"):
        value = row.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            pass
    return 0.0


def _get_ms_product_stock_for_store(ms_token: str, ms_product_id: str, ms_store_id: str) -> float:
    url = f"{MS_BASE}/report/stock/all"
    params = {
        "filter": (
            f"product={MS_BASE}/entity/product/{ms_product_id};"
            f"store={MS_BASE}/entity/store/{ms_store_id}"
        )
    }
    r = _ms_get_with_retry(
        url,
        headers=_ms_headers(ms_token),
        params=params,
        timeout=20,
        context="MoySklad stock report by store",
    )
    if not r.ok:
        log.error(
            "MoySklad stock report by store error status=%s body=%s ms_product_id=%s ms_store_id=%s",
            r.status_code,
            r.text,
            ms_product_id,
            ms_store_id,
        )
        r.raise_for_status()

    data = r.json()
    rows = data.get("rows", []) if isinstance(data, dict) else []
    if not rows:
        return 0.0

    return _extract_stock_value_from_row(rows[0])


def _list_ms_products_for_store(ms_token: str, ms_store_id: str) -> list[dict]:
    result: list[dict] = []
    offset = 0
    limit = 200

    while True:
        data = _search_ms_products(ms_token, limit=limit, offset=offset)
        rows = data.get("rows", []) or []
        if not rows:
            break

        for row in rows:
            ms_id = row.get("id")
            if not ms_id:
                continue
            try:
                stock_value = _get_ms_product_stock_for_store(ms_token, ms_id, ms_store_id)
            except Exception as e:
                log.warning(
                    "Failed to get stock for store-filtered product ms_id=%s ms_store_id=%s err=%s",
                    ms_id,
                    ms_store_id,
                    e,
                )
                continue

            # В магазин тянем только товары, реально относящиеся к этому складу
            if stock_value > 0:
                row["_store_stock"] = stock_value
                result.append(row)

        if len(rows) < limit:
            break
        offset += limit

    return result


def _delete_product_mapping_by_evotor_id(tenant_id: str, evotor_id: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            DELETE FROM mappings
            WHERE tenant_id = ? AND entity_type = 'product' AND evotor_id = ?
            """),
            (tenant_id, evotor_id),
        )
        conn.commit()
    finally:
        conn.close()


def _sync_product_to_evotor_store(tenant_id: str, evotor_store_id: str, ms_product_id: str) -> dict:
    from app.clients.evotor_client import EvotorClient

    tenant = _load_tenant(tenant_id)
    store = _load_store(tenant_id, evotor_store_id)
    merged = _merge_tenant_store(tenant, store)

    if not merged.get("evotor_token"):
        raise HTTPException(status_code=400, detail="evotor_token not configured")
    if not merged.get("moysklad_token"):
        raise HTTPException(status_code=400, detail="moysklad_token not configured")
    if not merged.get("ms_store_id"):
        raise HTTPException(status_code=400, detail="ms_store_id not configured for this store")

    try:
        ms_product = _get_ms_product(merged["moysklad_token"], ms_product_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch MS product: {e}")

    try:
        stock_value = _get_ms_product_stock_for_store(
            merged["moysklad_token"],
            ms_product_id,
            merged["ms_store_id"],
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch store stock: {e}")

    if stock_value <= 0:
        return {
            "status": "skipped",
            "reason": "no_stock_on_store",
            "ms_product_id": ms_product_id,
            "store": evotor_store_id,
        }

    mapping_store = MappingStore()
    evotor_client = EvotorClient(tenant_id, store_id=evotor_store_id)

    try:
        existing_evotor_product = _find_evotor_product_by_external_code(merged, ms_product_id)

        if existing_evotor_product:
            evotor_id = existing_evotor_product.get("id")
            evotor_payload = _build_evotor_product_payload(
                ms_product,
                evotor_id=evotor_id,
                current_product=existing_evotor_product,
                for_create=False,
            )

            url = f"{EVOTOR_BASE}/stores/{evotor_store_id}/products/{evotor_id}"
            r = requests.put(
                url,
                headers=_evotor_headers(merged["evotor_token"]),
                json=evotor_payload,
                timeout=20,
            )
            if not r.ok:
                log.error(
                    "Evotor update store product error status=%s ms_id=%s evotor_id=%s store=%s payload=%s body=%s",
                    r.status_code,
                    ms_product_id,
                    evotor_id,
                    evotor_store_id,
                    evotor_payload,
                    r.text,
                )
                r.raise_for_status()

            evotor_client.update_product_stock(evotor_id, stock_value)

            mapping_store.upsert_mapping(
                tenant_id=tenant_id,
                evotor_store_id=evotor_store_id,
                entity_type="product",
                evotor_id=evotor_id,
                ms_id=ms_product_id,
            )

            return {
                "status": "updated",
                "ms_product_id": ms_product_id,
                "evotor_product_id": evotor_id,
                "store": evotor_store_id,
                "quantity": stock_value,
            }

        evotor_payload = _build_evotor_product_payload(
            ms_product,
            evotor_id=None,
            current_product=None,
            for_create=True,
        )

        url = f"{EVOTOR_BASE}/stores/{evotor_store_id}/products"
        r = requests.post(
            url,
            headers=_evotor_headers(merged["evotor_token"]),
            json=evotor_payload,
            timeout=20,
        )
        if not r.ok:
            log.error(
                "Evotor create store product error status=%s ms_id=%s store=%s payload=%s body=%s",
                r.status_code,
                ms_product_id,
                evotor_store_id,
                evotor_payload,
                r.text,
            )
            r.raise_for_status()

        created = r.json() if r.text else {}
        created_id = created.get("id")
        if not created_id:
            raise HTTPException(status_code=502, detail="Evotor create product response has no id")

        mapping_store.upsert_mapping(
            tenant_id=tenant_id,
            evotor_store_id=evotor_store_id,
            entity_type="product",
            evotor_id=created_id,
            ms_id=ms_product_id,
        )

        evotor_client.update_product_stock(created_id, stock_value)

        return {
            "status": "created",
            "ms_product_id": ms_product_id,
            "evotor_product_id": created_id,
            "store": evotor_store_id,
            "quantity": stock_value,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to sync product to store Evotor: {e}")

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


def _find_ms_product_by_external_code(ms_token: str, external_code: str, _retry: int = 0) -> str | None:
    url = f"{MS_BASE}/entity/product"
    params = {"filter": f"externalCode={external_code}"}
    r = requests.get(url, headers=_ms_headers(ms_token), params=params, timeout=20)
    if not r.ok:
        if r.status_code == 429 and _retry < 3:
            wait = 2 ** _retry
            log.warning("_find_ms_product_by_external_code rate limited retry=%d wait=%ds", _retry + 1, wait)
            time.sleep(wait)
            return _find_ms_product_by_external_code(ms_token, external_code, _retry + 1)
        log.error(
            "MoySklad search by externalCode failed status=%s external_code=%s — aborting to prevent duplicate",
            r.status_code,
            external_code,
        )
        r.raise_for_status()
    rows = r.json().get("rows", [])
    if rows:
        return rows[0].get("id")
    return None

def _find_ms_product_by_name(ms_token: str, name: str) -> str | None:
    """Ищет товар в МойСклад по названию. Возвращает ms_id или None."""
    try:
        url = f"{MS_BASE}/entity/product"
        params = {"filter": f"name={name}"}
        r = requests.get(url, headers=_ms_headers(ms_token), params=params, timeout=20)
        if not r.ok:
            return None
        rows = r.json().get("rows", [])
        if rows:
            return rows[0].get("id")
        return None
    except Exception as e:
        log.warning("_find_ms_product_by_name failed name=%s err=%s", name, e)
        return None


def _ms_retry_wait_seconds(response, retry: int) -> float:
    """
    Вычисляет паузу для повторного запроса к МойСклад.
    Поддерживает стандартный Retry-After и заголовки Lognex.
    """
    headers = getattr(response, "headers", {}) or {}

    for header_name, divider in (
        ("X-Lognex-Retry-After", 1000.0),
        ("X-Lognex-Reset", 1000.0),
        ("Retry-After", 1.0),
    ):
        raw = headers.get(header_name)
        if raw:
            try:
                wait = float(raw) / divider
                return max(1.0, min(wait, 30.0))
            except Exception:
                pass

    return max(1.0, min(float(2 ** retry), 10.0))


def _request_with_ms_retry(
    method: str,
    url: str,
    *,
    headers=None,
    params=None,
    json=None,
    timeout: int = 20,
    retries: int = 4,
    context: str = "moysklad",
):
    """
    Единая обёртка для запросов к МойСклад.
    Повторяет 429 и временные 5xx с паузой.
    Не бросает исключение сама — возвращает последний response.
    """
    last_response = None

    for retry in range(retries + 1):
        response = requests.request(
            method,
            url,
            headers=headers,
            params=params,
            json=json,
            timeout=timeout,
        )
        last_response = response

        if response.status_code == 429 and retry < retries:
            wait = _ms_retry_wait_seconds(response, retry)
            log.warning(
                "%s: MoySklad 429, retry=%s/%s wait=%.1fs url=%s",
                context,
                retry + 1,
                retries,
                wait,
                url,
            )
            time.sleep(wait)
            continue

        if response.status_code in (500, 502, 503, 504) and retry < retries:
            wait = max(1.0, min(float(2 ** retry), 10.0))
            log.warning(
                "%s: MoySklad transient status=%s, retry=%s/%s wait=%.1fs url=%s",
                context,
                response.status_code,
                retry + 1,
                retries,
                wait,
                url,
            )
            time.sleep(wait)
            continue

        return response

    return last_response


def _detect_barcode_format(value: str) -> str:
    value = str(value).strip()
    digits_only = value.isdigit()
    length = len(value)

    if digits_only and length == 13:
        return "ean13"
    if digits_only and length == 8:
        return "ean8"
    if digits_only and length == 14:
        return "gtin"
    return "code128"


# ------------------------------------------------------------------------------
# Маппинги Эвотор → МойСклад (используются при initial sync)
# ------------------------------------------------------------------------------

# Эвотор tax → МС vat + vatEnabled
EVOTOR_TAX_TO_MS: dict[str, dict] = {
    "NO_VAT":  {"vat": 0,  "vatEnabled": False},
    "VAT_0":   {"vat": 0,  "vatEnabled": True},
    "VAT_5":   {"vat": 5,  "vatEnabled": True},
    "VAT_7":   {"vat": 7,  "vatEnabled": True},
    "VAT_10":  {"vat": 10, "vatEnabled": True},
    "VAT_18":  {"vat": 18, "vatEnabled": True},
    "VAT_20":  {"vat": 20, "vatEnabled": True},
    "VAT_22":  {"vat": 20, "vatEnabled": True},  # VAT_22 в Эвотор = 20% НДС в МС
    #"VAT_22":  {"vat": 22, "vatEnabled": True},
}

# Эвотор type → МС trackingType
# NORMAL намеренно отсутствует — МС ставит NOT_TRACKED по умолчанию,
# явная передача NOT_TRACKED вызывает ошибку валидации для ряда товаров.
EVOTOR_TYPE_TO_MS_TRACKING: dict[str, str] = {
    "DAIRY_MARKED":                 "MILK",
    "TOBACCO_MARKED":               "TOBACCO",
    "SHOES_MARKED":                 "SHOES",
    "MEDICINE_MARKED":              "MEDICINE",
    "WATER_MARKED":                 "WATER",
    "LIGHT_INDUSTRY_MARKED":        "LP_CLOTHES",
    "TOBACCO_PRODUCTS_MARKED":      "OTP",
    "PERFUME_MARKED":               "PERFUMERY",
    "ALCOHOL_MARKED":               "ALCOHOL",
    "PHOTOS_MARKED":                "CAMERA_PHOTO",
    "TYRES_MARKED":                 "TIRES",
    "BIKE_MARKED":                  "BICYCLE",
    "WHEELCHAIRS_MARKED":           "WHEELCHAIRS",
    "DIETARY_SUPPLEMENTS_MARKED":   "DIETARY_SUPPLEMENT",
    "ANTISEPTIC_MARKED":            "ANTISEPTIC",
    "JUICE_MARKED":                 "JUICE",
    "MEDICAL_DEVICES_MARKED":       "MEDICAL_DEVICES",
    "VETERINARY_MARKED":            "VETERINARY",
    "CAVIAR_MARKED":                "CAVIAR",
    "PET_FOOD_MARKED":              "PET_FOOD",
    "VEGETABLE_OIL_MARKED":         "VEGETABLE_OIL",
    "FUR_MARKED":                   "FUR",
    "AUTO_FLUIDS_MARKED":           "AUTO_FLUIDS",
    "CHEMICALS_MARKED":             "CHEMICALS",
    "JEWELRY_MARKED":               "JEWELRY",
    "ALCOHOL_NOT_MARKED":           "ALCOHOL",
    "BEER_MARKED":                  "ALCOHOL",
    "FUR_MARKED":                   "FUR",
}

# Fix 7: кэш для мета дефолтного priceType и валюты
_price_type_meta_cache: dict[str, dict] = {}
_currency_meta_cache: dict[str, dict] = {}


def _extract_rows_from_ms_response(data) -> list:
    if isinstance(data, dict):
        rows = data.get("rows", [])
        return rows if isinstance(rows, list) else []
    if isinstance(data, list):
        return data
    return []


def _get_default_price_type_meta(ms_token: str) -> dict:
    cached = _price_type_meta_cache.get(ms_token)
    if cached:
        return cached

    url = f"{MS_BASE}/context/companysettings/pricetype"
    r = _request_with_ms_retry(
        "GET",
        url,
        headers=_ms_headers(ms_token),
        timeout=20,
        retries=4,
        context="fetch MS priceType list",
    )
    if not r.ok:
        log.error("Failed to fetch priceType list status=%s body=%s", r.status_code, r.text)
        r.raise_for_status()

    rows = _extract_rows_from_ms_response(r.json())
    if not rows:
        raise ValueError("No price types found in MoySklad companysettings")

    first = rows[0]
    if not isinstance(first, dict) or "meta" not in first:
        raise ValueError("Invalid price type response structure from MoySklad")

    meta = first["meta"]
    _price_type_meta_cache[ms_token] = meta
    return meta


def _get_default_currency_meta(ms_token: str) -> dict:
    cached = _currency_meta_cache.get(ms_token)
    if cached:
        return cached

    url = f"{MS_BASE}/entity/currency"
    r = _request_with_ms_retry(
        "GET",
        url,
        headers=_ms_headers(ms_token),
        params={"filter": "default=true"},
        timeout=20,
        retries=4,
        context="fetch MS default currency",
    )
    if not r.ok:
        log.error("Failed to fetch currency list status=%s body=%s", r.status_code, r.text)
        r.raise_for_status()

    rows = _extract_rows_from_ms_response(r.json())

    if not rows:
        r2 = _request_with_ms_retry(
            "GET",
            url,
            headers=_ms_headers(ms_token),
            timeout=20,
            retries=4,
            context="fetch MS fallback currency list",
        )
        if not r2.ok:
            log.error("Failed to fetch fallback currency list status=%s body=%s", r2.status_code, r2.text)
            r2.raise_for_status()
        rows = _extract_rows_from_ms_response(r2.json())

    if not rows:
        raise ValueError("No currencies found in MoySklad")

    first = rows[0]
    if not isinstance(first, dict) or "meta" not in first:
        raise ValueError("Invalid currency response structure from MoySklad")

    meta = first["meta"]
    _currency_meta_cache[ms_token] = meta
    return meta


# Кэш UOM
_uom_cache: dict[str, list] = {}

EVOTOR_MEASURE_TO_MS_CODE = {
    "шт": "796", "кг": "166", "г": "163", "л": "112",
    "мл": "111", "м": "006", "см": "004", "мм": "003",
    "м2": "055", "м3": "113", "км": "008", "т": "168",
    "упак": "796", "уп": "796", "пара": "796", "компл": "796",
    "рулон": "736", "блок": "813", "ящ": "812", "пог. м": "018",
}


def _get_ms_uom_meta(ms_token: str, measure_name: str) -> dict | None:
    if not measure_name:
        return None
    if ms_token not in _uom_cache:
        try:
            r = _request_with_ms_retry(
                "GET",
                f"{MS_BASE}/entity/uom",
                headers=_ms_headers(ms_token),
                params={"limit": 100},
                timeout=20,
                retries=4,
                context="fetch MS UOM list",
            )
            if r.ok:
                _uom_cache[ms_token] = r.json().get("rows", [])
            else:
                return None
        except Exception as e:
            log.warning("Failed to fetch UOM list err=%s", e)
            return None
    uoms = _uom_cache[ms_token]
    measure_lower = measure_name.strip().lower()
    for uom in uoms:
        if uom.get("name", "").lower() == measure_lower:
            return uom.get("meta")
    ms_code = EVOTOR_MEASURE_TO_MS_CODE.get(measure_lower)
    if ms_code:
        for uom in uoms:
            if uom.get("code") == ms_code:
                return uom.get("meta")
    log.warning("UOM not found for measure_name=%s", measure_name)
    return None


def _update_ms_product_folder(
    ms_token: str,
    ms_product_id: str,
    evotor_product: dict,
    evotor_token: str,
    evotor_store_id: str,
) -> None:
    """
    Обновляет папку товара в МС, если товар находится в группе Эвотор.
    429/5xx от МойСклад повторяются через _request_with_ms_retry.
    Ошибка обновления папки не должна ломать mapping товара.
    """
    parent_id = evotor_product.get("parent_id")
    if not parent_id:
        return

    try:
        folder_name = None

        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                aq("SELECT ms_folder_name FROM product_group_mappings WHERE evotor_group_id = ? LIMIT 1"),
                (parent_id,),
            )
            pgm = cur.fetchone()
        finally:
            conn.close()

        if pgm and pgm["ms_folder_name"]:
            folder_name = pgm["ms_folder_name"]
        else:
            # Получаем название группы из Эвотор API.
            # Это не МойСклад, поэтому оставляем обычный request.
            evotor_response = requests.get(
                f"{EVOTOR_BASE}/stores/{evotor_store_id}/product-groups/{parent_id}",
                headers=_evotor_headers(evotor_token),
                timeout=15,
            )
            if evotor_response.ok:
                folder_name = evotor_response.json().get("name")

        if not folder_name:
            return

        folder_meta = _get_or_create_ms_folder(ms_token, folder_name)
        if not folder_meta:
            log.warning(
                "_update_ms_product_folder skipped: cannot resolve folder_meta folder=%s ms_id=%s",
                folder_name,
                ms_product_id,
            )
            return

        response = _request_with_ms_retry(
            "PUT",
            f"{MS_BASE}/entity/product/{ms_product_id}",
            headers=_ms_headers(ms_token),
            json={"productFolder": {"meta": folder_meta}},
            timeout=20,
            retries=4,
            context="update MS product folder",
        )

        if response.ok:
            log.info("Updated MS product folder=%s ms_id=%s", folder_name, ms_product_id)
        else:
            log.warning(
                "Failed to update MS product folder ms_id=%s status=%s body=%s",
                ms_product_id,
                response.status_code,
                response.text[:500],
            )

    except Exception as e:
        log.warning("_update_ms_product_folder failed ms_id=%s err=%s", ms_product_id, e)


def _get_or_create_ms_folder(ms_token: str, folder_name: str, _retry: int = 0) -> dict | None:
    """
    Находит или создаёт папку в МС по названию.
    Использует общий retry-wrapper для 429/5xx.
    """
    response = _request_with_ms_retry(
        "GET",
        f"{MS_BASE}/entity/productfolder",
        headers=_ms_headers(ms_token),
        params={"filter": f"name={folder_name}"},
        timeout=20,
        retries=4,
        context="find MS product folder",
    )

    if response.ok:
        rows = response.json().get("rows", [])
        if rows:
            return rows[0].get("meta")
    else:
        log.warning(
            "Failed to find MS folder name=%s status=%s body=%s",
            folder_name,
            response.status_code,
            response.text[:500],
        )
        return None

    create_response = _request_with_ms_retry(
        "POST",
        f"{MS_BASE}/entity/productfolder",
        headers=_ms_headers(ms_token),
        json={"name": folder_name},
        timeout=20,
        retries=4,
        context="create MS product folder",
    )

    if create_response.ok:
        log.info("Created MS folder name=%s", folder_name)
        return create_response.json().get("meta")

    log.error(
        "Failed to create MS folder name=%s status=%s body=%s",
        folder_name,
        create_response.status_code,
        create_response.text[:500],
    )
    return None


def _create_ms_product(ms_token: str, product: dict, _retry: int = 0) -> str:
    price_type_meta = _get_default_price_type_meta(ms_token)
    currency_meta = _get_default_currency_meta(ms_token)

    payload = {
        "name": product["name"],
        "externalCode": product["id"],
        "description": product.get("description", ""),
        "salePrices": [
            {
                "value": round(float(product.get("price", 0)) * 100),
                "currency": {"meta": currency_meta},
                "priceType": {"meta": price_type_meta},
            }
        ],
    }

    if product.get("cost_price") is not None:
        payload["buyPrice"] = {
            "value": round(float(product.get("cost_price", 0)) * 100),
            "currency": {"meta": currency_meta},
        }

    # --- Единица измерения ---
    measure_name = product.get("measure_name", "").strip()
    if measure_name:
        uom_meta = _get_ms_uom_meta(ms_token, measure_name)
        if uom_meta:
            payload["uom"] = {"meta": uom_meta}


    # --- НДС ---
    evotor_tax = str(product.get("tax") or "").strip().upper()
    if evotor_tax:
        vat_fields = EVOTOR_TAX_TO_MS.get(evotor_tax)
        if vat_fields:
            payload["vat"] = vat_fields["vat"]
            payload["vatEnabled"] = vat_fields["vatEnabled"]
        else:
            log.warning(
                "Unknown Evotor tax=%s for product id=%s — skipping VAT fields",
                evotor_tax,
                product.get("id"),
            )

    # --- Маркировка ---
    evotor_type = str(product.get("type") or "NORMAL").strip().upper()
    ms_tracking = EVOTOR_TYPE_TO_MS_TRACKING.get(evotor_type)
    if ms_tracking:
        payload["trackingType"] = ms_tracking
    # NORMAL → не передаём trackingType, МС ставит NOT_TRACKED сам

    # --- Штрихкоды ---
    barcodes = product.get("barcodes", [])
    if barcodes:
        payload["barcodes"] = [
            {_detect_barcode_format(bc): str(bc).strip()}
            for bc in barcodes
            if str(bc).strip()
        ]

    # --- Папка (группа) из Эвотор parent_id ---
    evotor_parent_id = product.get("parent_id") or product.get("parentId")
    if evotor_parent_id:
        try:
            folder_name = None
            # Сначала ищем в маппинге
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                aq("SELECT ms_folder_name FROM product_group_mappings WHERE evotor_group_id = ? LIMIT 1"),
                (evotor_parent_id,),
            )
            pgm = cur.fetchone()
            conn.close()
            if pgm and pgm["ms_folder_name"]:
                folder_name = pgm["ms_folder_name"]
            else:
                # Получаем название группы из Эвотор по parent_id
                # Ищем store_id через product
                _store_id = product.get("store_id")
                if _store_id and product.get("_evotor_token"):
                    _r = requests.get(
                        f"{EVOTOR_BASE}/stores/{_store_id}/product-groups/{evotor_parent_id}",
                        headers=_evotor_headers(product["_evotor_token"]),
                        timeout=15,
                    )
                    if _r.ok:
                        folder_name = _r.json().get("name")

            if folder_name:
                folder_meta = _get_or_create_ms_folder(ms_token, folder_name)
                if folder_meta:
                    payload["productFolder"] = {"meta": folder_meta}
                    log.info("Set MS folder=%s for product=%s", folder_name, product.get("name"))
        except Exception as e:
            log.warning("Failed to set MS folder for product err=%s", e)

    url = f"{MS_BASE}/entity/product"
    r = requests.post(url, headers=_ms_headers(ms_token), json=payload, timeout=20)

    if not r.ok:
        log.error(
            "MoySklad create product error status=%s body=%s payload=%s",
            r.status_code,
            r.text,
            payload,
        )
        if r.status_code == 429 and _retry < 3:
            wait = 2 ** _retry
            log.warning("_create_ms_product rate limited retry=%d wait=%ds", _retry + 1, wait)
            time.sleep(wait)
            return _create_ms_product(ms_token, product, _retry + 1)
        r.raise_for_status()

    ms_product = r.json()
    return ms_product["id"]


# ------------------------------------------------------------------------------
# Initial sync Evotor -> MoySklad
# ------------------------------------------------------------------------------

@router.post("/sync/{tenant_id}/initial")
def initial_sync(tenant_id: str):
    tenant = _load_tenant(tenant_id)
    evotor_store_id = tenant.get("evotor_store_id")

    if tenant.get("sync_completed_at"):
        raise HTTPException(
            status_code=409,
            detail="Initial sync already completed. Use DELETE /tenants/{id}/complete-sync to reset.",
        )

    if not tenant.get("evotor_token"):
        raise HTTPException(status_code=409, detail="Evotor token is not connected yet")

    if not evotor_store_id:
        raise HTTPException(status_code=409, detail="Evotor store is not selected yet")

    if not tenant.get("moysklad_token"):
        raise HTTPException(status_code=400, detail="moysklad_token not configured")

    try:
        cleanup_result = cleanup_stale_product_mappings(tenant_id, evotor_store_id)
        if cleanup_result.get("deleted"):
            log.warning(
                "initial_sync_store: cleaned stale mappings tenant_id=%s store=%s deleted=%s",
                tenant_id,
                evotor_store_id,
                cleanup_result.get("deleted"),
            )
    except Exception as e:
        log.warning(
            "initial_sync_store: stale mapping cleanup skipped tenant_id=%s store=%s err=%s",
            tenant_id,
            evotor_store_id,
            e,
        )

    try:
        products = _get_evotor_products(tenant["evotor_token"], evotor_store_id)
    except Exception as e:
        log.error("Failed to fetch Evotor products tenant_id=%s err=%s", tenant_id, e)
        raise HTTPException(status_code=502, detail=f"Failed to fetch Evotor products: {e}")

    if not products:
        now = _now()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            aq("UPDATE tenants SET sync_completed_at = ?, updated_at = ? WHERE id = ?"),
            (now, now, tenant_id),
        )
        cur.execute(
            aq("UPDATE tenant_stores SET sync_completed_at = ?, updated_at = ? WHERE tenant_id = ? AND evotor_store_id = ?"),
            (now, now, tenant_id, evotor_store_id),
        )
        conn.commit()
        conn.close()
        log.info("Initial sync completed for empty Evotor catalog tenant_id=%s", tenant_id)
        return {
            "status": "ok",
            "synced": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
            "message": "No products found in Evotor",
            "sync_mode": "moysklad",
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

        existing = store.get_by_evotor_id(
            tenant_id=tenant_id,
            entity_type="product",
            evotor_id=evotor_id,
            evotor_store_id=evotor_store_id,
        )
        if existing:
            log.info("Skipping already mapped product evotor_id=%s ms_id=%s", evotor_id, existing)
            skipped += 1
            continue

        try:
            ms_id = _find_ms_product_by_external_code(tenant["moysklad_token"], evotor_id)
            if ms_id:
                log.info(
                    "Found existing MS product by externalCode evotor_id=%s ms_id=%s — saving mapping only",
                    evotor_id,
                    ms_id,
                )
                # Обновляем папку если товар в группе Эвотор
                _update_ms_product_folder(
                    tenant["moysklad_token"], ms_id, product,
                    tenant["evotor_token"], evotor_store_id,
                )
            else:
                # Ищем по названию — fallback для старых товаров без externalCode.
                # Важно: name-match безопасен только если найденный ms_id ещё не занят
                # другим evotor_id в этом же магазине.
                ms_id = _find_ms_product_by_name(tenant["moysklad_token"], product.get("name", ""))
                if ms_id:
                    mapped_evotor_id = store.get_by_ms_id(
                        tenant_id=tenant_id,
                        entity_type="product",
                        ms_id=ms_id,
                        evotor_store_id=evotor_store_id,
                    )
                    if mapped_evotor_id and mapped_evotor_id != evotor_id:
                        log.warning(
                            "Unsafe name-match rejected evotor_id=%s name=%s ms_id=%s mapped_evotor_id=%s store=%s",
                            evotor_id,
                            product.get("name"),
                            ms_id,
                            mapped_evotor_id,
                            evotor_store_id,
                        )
                        ms_id = None

                if ms_id:
                    log.info(
                        "Found existing MS product by name evotor_id=%s ms_id=%s name=%s — saving mapping only",
                        evotor_id,
                        ms_id,
                        product.get("name"),
                    )
                    # Обновляем папку если товар в группе Эвотор
                    _update_ms_product_folder(
                        tenant["moysklad_token"], ms_id, product,
                        tenant["evotor_token"], evotor_store_id,
                    )
                else:
                    # Передаём evotor_token и store_id для резолва группы
                    product["_evotor_token"] = tenant["evotor_token"]
                    product["store_id"] = evotor_store_id
                    ms_id = _create_ms_product(tenant["moysklad_token"], product)
                log.info(
                    "Created MS product evotor_id=%s ms_id=%s name=%s tax=%s type=%s",
                    evotor_id,
                    ms_id,
                    product.get("name"),
                    product.get("tax"),
                    product.get("type"),
                )
        except Exception as e:
            log.error("Failed to create MS product evotor_id=%s name=%s err=%s", evotor_id, product.get("name"), e)
            failed += 1
            errors.append({"evotor_id": evotor_id, "name": product.get("name"), "error": str(e)})
            continue

        ok = store.upsert_mapping(
            tenant_id=tenant_id,
            evotor_store_id=evotor_store_id,
            entity_type="product",
            evotor_id=evotor_id,
            ms_id=ms_id,
        )
        if ok:
            synced += 1
        else:
            msg = (
                f"Mapping conflict: evotor_id={evotor_id} ms_id={ms_id} "
                f"store={evotor_store_id}; ms_id already mapped to another evotor_id in this store"
            )
            log.warning(msg)
            failed += 1
            errors.append({
                "evotor_id": evotor_id,
                "name": product.get("name"),
                "ms_id": ms_id,
                "error": msg,
            })

    should_complete = failed == 0 and (synced + skipped) > 0

    if should_complete:
        conn = get_connection()
        cur = conn.cursor()
        now = _now()
        cur.execute(
            aq("UPDATE tenants SET sync_completed_at = ?, updated_at = ? WHERE id = ?"),
            (now, now, tenant_id),
        )
        cur.execute(
            aq("UPDATE tenant_stores SET sync_completed_at = ?, updated_at = ? WHERE tenant_id = ? AND evotor_store_id = ?"),
            (now, now, tenant_id, evotor_store_id),
        )
        conn.commit()
        conn.close()
        log.info(
            "Initial sync completed tenant_id=%s synced=%s skipped=%s",
            tenant_id,
            synced,
            skipped,
        )

    return {
        "status": "ok" if failed == 0 else "partial",
        "synced": synced,
        "skipped": skipped,
        "failed": failed,
        "errors": errors,
        "sync_mode": "moysklad" if should_complete else "evotor",
    }


@router.get("/sync/{tenant_id}/status")
def sync_status(tenant_id: str):
    tenant = _load_tenant(tenant_id)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        aq("SELECT COUNT(*) as cnt FROM mappings WHERE tenant_id = ? AND entity_type = 'product'"),
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
        "moysklad_configured": bool(tenant.get("moysklad_token")),
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
    evotor_store_id = tenant.get("evotor_store_id")

    if not tenant.get("sync_completed_at"):
        raise HTTPException(
            status_code=409,
            detail="Initial sync not completed. Run POST /sync/{tenant_id}/initial first.",
        )

    if not tenant.get("evotor_token"):
        raise HTTPException(status_code=400, detail="evotor_token not configured")
    if not evotor_store_id:
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
        evotor_store_id=evotor_store_id,
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
            r = requests.patch(
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

        # Проверяем нет ли уже товара в Эвотор по externalCode = ms_product_id
        existing_by_code = _find_evotor_product_by_external_code(tenant, ms_product_id)
        if existing_by_code:
            evotor_id = existing_by_code.get("id")
            log.info("Found existing Evotor product by externalCode ms_id=%s evotor_id=%s — saving mapping only", ms_product_id, evotor_id)
            store.upsert(
                tenant_id=tenant_id,
                evotor_store_id=evotor_store_id,
                entity_type="product",
                evotor_id=evotor_id,
                ms_id=ms_product_id,
            )
            return {
                "status": "mapped",
                "ms_product_id": ms_product_id,
                "evotor_product_id": evotor_id,
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
            evotor_store_id=evotor_store_id,
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
    evotor_store_id = tenant.get("evotor_store_id")

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
    evotor_clients: dict[str, EvotorClient] = {}

    synced = 0
    failed = 0
    errors = []

    for item in mappings:
        ms_id = item["ms_id"]
        evotor_id = item["evotor_id"]
        item_store_id = item.get("evotor_store_id") or evotor_store_id
        try:
            if not item_store_id:
                raise ValueError("evotor_store_id is not configured for mapping")

            stock_value = ms_client.get_product_stock(ms_id)
            if item_store_id not in evotor_clients:
                evotor_clients[item_store_id] = EvotorClient(tenant_id, store_id=item_store_id)
            evotor_clients[item_store_id].update_product_stock(evotor_id, stock_value)
            synced += 1
            log.info(
                "Stock synced tenant_id=%s store=%s ms_id=%s evotor_id=%s quantity=%s",
                tenant_id,
                item_store_id,
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
                    "evotor_store_id": item.get("evotor_store_id"),
                    "error": err_text,
                }
            )
            log.error(
                "Stock reconcile failed tenant_id=%s store=%s ms_id=%s evotor_id=%s err=%s",
                tenant_id,
                item.get("evotor_store_id"),
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
    evotor_store_id = tenant.get("evotor_store_id")

    if not tenant.get("sync_completed_at"):
        raise HTTPException(
            status_code=409,
            detail="Initial sync not completed. Run POST /sync/{tenant_id}/initial first.",
        )
    if not evotor_store_id:
        raise HTTPException(status_code=400, detail="evotor_store_id not configured")

    store = MappingStore()
    evotor_product_id = store.get_by_ms_id(
        tenant_id=tenant_id,
        entity_type="product",
        ms_id=ms_product_id,
        evotor_store_id=evotor_store_id,
    )
    if not evotor_product_id:
        raise HTTPException(status_code=404, detail="Product mapping not found")

    try:
        ms_client = MoySkladClient(tenant_id)
        evotor_client = EvotorClient(tenant_id, store_id=evotor_store_id)

        stock_value = ms_client.get_product_stock(ms_product_id)
        evotor_client.update_product_stock(evotor_product_id, stock_value)

        existing = _get_stock_status_row(tenant_id)
        _upsert_stock_status(
            tenant_id=tenant_id,
            status=existing["status"] if existing else "ok",
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
            status=existing["status"] if existing else "error",
            started_at=existing["started_at"] if existing else _now(),
            last_sync_at=_now(),
            last_error=err_text,
            synced_items_count=existing["synced_items_count"] if existing else 0,
            total_items_count=existing["total_items_count"] if existing else 1,
        )
        raise HTTPException(status_code=502, detail=f"Failed to sync stock to Evotor: {err_text}")


# ------------------------------------------------------------------------------
# Fiscalization: MoySklad demand -> fiscalization24.ru
# ------------------------------------------------------------------------------

# Маппинг НДС МойСклад → fiscalization24 (целые числа)
# fiscalization24 допускает: -1 (без НДС), 0, 5, 7, 10, 18, 20
VAT_MS_TO_FISCAL = {
    (0, False): -1,   # без НДС
    (0, True):   0,   # НДС 0%
    (5, True):   5,
    (7, True):   7,
    (10, True):  10,
    (18, True):  18,
    (20, True):  20,
}

# Маппинг типов товаров Эвотор → fiscalization24
EVOTOR_TYPE_TO_FISCAL = {
    "NORMAL":         0,   # обычный
    "DAIRY_MARKED":   12,  # молочная продукция
    "ALCOHOL_MARKED":  1,  # маркированный алкоголь
    "TOBACCO_MARKED":  4,  # маркированный табак
    "SHOES_MARKED":    5,  # маркированная обувь
    "MEDICINE_MARKED": 6,  # маркированные лекарства
    "WATER":          13,  # вода
}


def _money_from_ms(value) -> float:
    try:
        return round(float(value or 0) / 100.0, 2)
    except (TypeError, ValueError):
        return 0.0


def _fetch_demand_positions(ms_token: str, positions_raw) -> list:
    if isinstance(positions_raw, dict) and "meta" in positions_raw:
        pos_url = positions_raw["meta"]["href"]
        r = requests.get(
            pos_url,
            headers=_ms_headers(ms_token),
            params={"expand": "assortment"},
            timeout=20,
        )
        if not r.ok:
            log.error("Failed to fetch demand positions status=%s", r.status_code)
            r.raise_for_status()
        return r.json().get("rows", [])
    return positions_raw if isinstance(positions_raw, list) else []


def _map_demand_to_fiscal_check(
    demand: dict,
    tenant_id: str,
    ms_token: str,
    fiscal_client_uid: str,
    fiscal_device_uid: str,
    check_uid: str,
) -> dict:
    positions_list = _fetch_demand_positions(ms_token, demand.get("positions", {}))

    fiscal_products = []

    for item in positions_list:
        assortment = item.get("assortment", {}) or {}
        meta = assortment.get("meta", {}) or {}

        quantity = round(float(item.get("quantity", 1) or 1), 3)
        price_value = _money_from_ms(item.get("price"))

        vat = item.get("vat", 0) or 0
        vat_enabled = item.get("vatEnabled")
        if vat_enabled is False:
            tax = -1
        else:
            vat_value = int(vat or 0)
            tax = VAT_MS_TO_FISCAL.get((vat_value, True), -1)

        tracking_type = assortment.get("trackingType") or "NORMAL"
        product_type = EVOTOR_TYPE_TO_FISCAL.get(str(tracking_type).upper(), 0)

        discount_percent = float(item.get("discount", 0) or 0)
        if discount_percent < 0 or discount_percent > 100:
            raise ValueError(
                f"Fiscalization discount must be 0..100, got {discount_percent}"
            )

        product_name = (
            assortment.get("name")
            or item.get("name")
            or meta.get("name")
            or f"Product {meta.get('href', '').rstrip('/').split('/')[-1]}"
        )

        if not product_name.strip():
            raise ValueError("Fiscalization product name is empty")

        if price_value <= 0:
            raise ValueError(f"Fiscalization product price must be > 0, got {price_value}")

        if quantity <= 0:
            raise ValueError(f"Fiscalization product quantity must be > 0, got {quantity}")

        fiscal_products.append({
            "type": product_type,
            "tax": tax,
            "name": product_name,
            "price": price_value,
            "discount": round(discount_percent, 2),
            "quantity": quantity,
        })

    total_sum_value = _money_from_ms(demand.get("sum"))

    if total_sum_value <= 0:
        raise ValueError(f"Fiscalization total sum must be > 0, got {total_sum_value}")

    calc_sum = round(sum(
        p["price"] * p["quantity"] * (1 - p["discount"] / 100.0)
        for p in fiscal_products
    ), 2)
    if abs(calc_sum - total_sum_value) > 0.05:
        log.warning(
            "Fiscal sum mismatch: demand.sum=%.2f calc=%.2f tenant_id=%s",
            total_sum_value, calc_sum, tenant_id,
        )

    return {
        "UID": check_uid,
        "ClientUid": fiscal_client_uid,
        "DeviceUid": fiscal_device_uid,
        "Data": {
            "uid": check_uid,
            "products": fiscal_products,
            "payCashSumma": total_sum_value,
            "payCardSumma": 0.0,
            "discount": 0,
            "type": 0,
            "paymentType": 1,
        },
    }


def _get_existing_fiscal_check(tenant_id: str, ms_demand_id: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        aq("""
        SELECT *
        FROM fiscalization_checks
        WHERE tenant_id = ? AND ms_demand_id = ?
        """),
        (tenant_id, ms_demand_id),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def _save_fiscal_check(
    tenant_id: str,
    uid: str,
    ms_demand_id: str,
    fiscal_client_uid: str,
    fiscal_device_uid: str,
    status: int,
    request_json: str,
    response_json: str,
    description: str | None = None,
    error_code: int | None = None,
    error_message: str | None = None,
) -> None:
    now = _now()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            INSERT INTO fiscalization_checks (
                uid, tenant_id, ms_demand_id,
                fiscal_client_uid, fiscal_device_uid,
                status, description, error_code, error_message,
                request_json, response_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET
                status=excluded.status,
                description=excluded.description,
                error_code=excluded.error_code,
                error_message=excluded.error_message,
                response_json=excluded.response_json,
                updated_at=excluded.updated_at
            """),
            (
                uid, tenant_id, ms_demand_id,
                fiscal_client_uid, fiscal_device_uid,
                status, description, error_code, error_message,
                request_json, response_json,
                now, now,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _update_fiscal_check_state(uid: str, state: dict) -> None:
    now = _now()
    import json as _json
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            UPDATE fiscalization_checks
            SET status=?, description=?, error_code=?, error_message=?,
                response_json=?, updated_at=?
            WHERE uid=?
            """),
            (
                state.get("State", 1),
                state.get("Description"),
                state.get("Error"),
                state.get("ErrorMessage"),
                _json.dumps(state, ensure_ascii=False),
                now,
                uid,
            ),
        )
        conn.commit()
    finally:
        conn.close()


@router.post("/sync/{tenant_id}/fiscalize/{ms_demand_id}")
def fiscalize_demand(tenant_id: str, ms_demand_id: str):
    """
    Отправляет документ Отгрузка из МойСклад на фискализацию
    через Универсальный фискализатор (fiscalization24.ru).

    Поведение идемпотентное:
    - если для tenant_id + ms_demand_id запись уже есть,
      повторный POST не создаёт второй чек, а возвращает existing uid.
    """
    import json as _json
    import uuid as _uuid
    from app.clients.fiscalization_client import FiscalizationClient

    metric_recorded = False

    def record_request(result: str) -> None:
        nonlocal metric_recorded
        if metric_recorded:
            return
        fiscalization_requests_total.labels(result=result).inc()
        metric_recorded = True

    with observe_duration(fiscalization_request_duration_seconds):
        try:
            tenant = _load_tenant(tenant_id)

            if not tenant.get("sync_completed_at"):
                raise HTTPException(status_code=409, detail="Initial sync not completed.")
            if not tenant.get("moysklad_token"):
                raise HTTPException(status_code=400, detail="moysklad_token not configured")
            if not tenant.get("fiscal_token"):
                raise HTTPException(
                    status_code=400,
                    detail="fiscal_token not configured. Use PATCH /tenants/{tenant_id}/fiscal",
                )
            if not tenant.get("fiscal_client_uid"):
                raise HTTPException(status_code=400, detail="fiscal_client_uid not configured")
            if not tenant.get("fiscal_device_uid"):
                raise HTTPException(status_code=400, detail="fiscal_device_uid not configured")

            ms_token = tenant["moysklad_token"]
            fiscal_client_uid = tenant["fiscal_client_uid"]
            fiscal_device_uid = tenant["fiscal_device_uid"]

            existing = _get_existing_fiscal_check(tenant_id, ms_demand_id)
            if existing:
                record_request("already_exists")
                log.info(
                    "fiscalization request already exists",
                    extra=_sync_extra(
                        tenant_id,
                        doc_id=ms_demand_id,
                        operation="sync.fiscalize",
                        status="already_exists",
                    ),
                )
                return {
                    "status": "already_exists",
                    "uid": existing["uid"],
                    "tenant_id": existing["tenant_id"],
                    "ms_demand_id": existing["ms_demand_id"],
                    "state": existing["status"],
                    "description": existing.get("description"),
                    "error_code": existing.get("error_code"),
                    "error_message": existing.get("error_message"),
                }

            check_uid = str(_uuid.uuid4())

            try:
                url = f"{MS_BASE}/entity/demand/{ms_demand_id}"
                r = requests.get(url, headers=_ms_headers(ms_token), timeout=20)
                if not r.ok:
                    r.raise_for_status()
                demand = r.json()
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"Failed to fetch demand: {e}")

            try:
                payload = _map_demand_to_fiscal_check(
                    demand=demand,
                    tenant_id=tenant_id,
                    ms_token=ms_token,
                    fiscal_client_uid=fiscal_client_uid,
                    fiscal_device_uid=fiscal_device_uid,
                    check_uid=check_uid,
                )
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=422, detail=f"Failed to map demand: {e}")

            log.info(
                "fiscalization request prepared",
                extra=_sync_extra(
                    tenant_id,
                    uid=check_uid,
                    doc_id=ms_demand_id,
                    operation="sync.fiscalize",
                    status="prepared",
                ),
            )

            request_json = _json.dumps(payload, ensure_ascii=False)
            try:
                fiscal_client = FiscalizationClient(tenant["fiscal_token"])
                result = fiscal_client.create_check(payload)
                response_json = _json.dumps(result, ensure_ascii=False)
                description = result.get("Info") if isinstance(result.get("Info"), str) else None
                error_code = None
                error_message = None
                state = None
                if isinstance(result, dict):
                    state = (result.get("CheckState") or result.get("checkState") or {}).get("State")
                status = int(state) if state is not None else 2
            except Exception as e:
                response_json = str(e)
                try:
                    _save_fiscal_check(
                        tenant_id, check_uid, ms_demand_id,
                        fiscal_client_uid, fiscal_device_uid,
                        status=1,
                        request_json=request_json,
                        response_json=response_json,
                        description="Send failed (will retry)",
                        error_code=None,
                        error_message=str(e),
                    )
                except Exception:
                    log.exception(
                        "failed to save fiscalization error record",
                        extra=_sync_extra(
                            tenant_id,
                            doc_id=ms_demand_id,
                            operation="sync.fiscalize",
                            status="save_error_failed",
                        ),
                    )
                record_request("send_failed")
                log.error(
                    "fiscalization request failed",
                    extra=_sync_extra(
                        tenant_id,
                        doc_id=ms_demand_id,
                        operation="sync.fiscalize",
                        status="send_failed",
                        exception_type=type(e).__name__,
                    ),
                )
                raise HTTPException(status_code=502, detail=f"Failed to send check: {e}")

            try:
                _save_fiscal_check(
                    tenant_id, check_uid, ms_demand_id,
                    fiscal_client_uid, fiscal_device_uid,
                    status=status,
                    request_json=request_json,
                    response_json=response_json,
                    description=description,
                    error_code=error_code,
                    error_message=error_message,
                )
            except Exception as e:
                err_str = str(e).lower()
                if "unique" in err_str or "duplicate" in err_str:
                    existing = _get_existing_fiscal_check(tenant_id, ms_demand_id)
                    if existing:
                        record_request("already_exists")
                        return {
                            "status": "already_exists",
                            "uid": existing["uid"],
                            "tenant_id": existing["tenant_id"],
                            "ms_demand_id": existing["ms_demand_id"],
                            "state": existing["status"],
                            "description": existing.get("description"),
                            "error_code": existing.get("error_code"),
                            "error_message": existing.get("error_message"),
                        }
                record_request("unexpected_error")
                raise

            fiscalization_state_total.labels(
                state_label=_fiscalization_state_label(status)
            ).inc()
            record_request("queued")
            log.info(
                "fiscalization request queued",
                extra=_sync_extra(
                    tenant_id,
                    uid=check_uid,
                    doc_id=ms_demand_id,
                    operation="sync.fiscalize",
                    status="queued",
                ),
            )
            return {
                "status": "queued",
                "uid": check_uid,
                "tenant_id": tenant_id,
                "ms_demand_id": ms_demand_id,
                "positions": len(payload["Data"]["products"]),
                "sum": payload["Data"]["payCashSumma"],
            }
        except HTTPException:
            if not metric_recorded:
                record_request("unexpected_error")
            raise
        except Exception:
            if not metric_recorded:
                record_request("unexpected_error")
            raise


@router.get("/sync/{tenant_id}/fiscalization/{uid}")
def get_fiscal_check_status(tenant_id: str, uid: str):
    """
    Получает актуальный статус чека из fiscalization24.ru и обновляет запись в БД.

    Статусы:
    1  — новый
    2  — отправлен на кассу
    5  — принят кассой
    9  — ошибка фискализации
    10 — успешно фискализирован
    """
    from app.clients.fiscalization_client import FiscalizationClient

    metric_recorded = False

    def record_status_check(result: str) -> None:
        nonlocal metric_recorded
        if metric_recorded:
            return
        fiscalization_status_checks_total.labels(result=result).inc()
        metric_recorded = True

    tenant = _load_tenant(tenant_id)

    if not tenant.get("fiscal_token"):
        raise HTTPException(status_code=400, detail="fiscal_token not configured")

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        aq("SELECT * FROM fiscalization_checks WHERE uid=? AND tenant_id=?"),
        (uid, tenant_id),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        record_status_check("not_found")
        log.warning(
            "fiscalization status check not found",
            extra=_sync_extra(
                tenant_id,
                uid=uid,
                operation="sync.fiscalization_status",
                status="not_found",
            ),
        )
        raise HTTPException(status_code=404, detail="Fiscal check not found")

    try:
        fiscal_client = FiscalizationClient(tenant["fiscal_token"])
        state = fiscal_client.get_check_state(uid)
    except Exception as e:
        record_status_check("transport_error")
        log.error(
            "fiscalization status check transport error",
            extra=_sync_extra(
                tenant_id,
                uid=uid,
                operation="sync.fiscalization_status",
                status="transport_error",
                exception_type=type(e).__name__,
            ),
        )
        raise HTTPException(status_code=502, detail=f"Failed to get check state: {e}")

    _update_fiscal_check_state(uid, state)

    state_code = int(state.get("State", 1))
    state_label = _fiscalization_state_label(state_code)
    fiscalization_state_total.labels(state_label=state_label).inc()
    record_status_check("ok")
    log.info(
        "fiscalization status fetched",
        extra=_sync_extra(
            tenant_id,
            uid=uid,
            doc_id=dict(row)["ms_demand_id"],
            operation="sync.fiscalization_status",
            status=state_label,
        ),
    )

    return {
        "uid": uid,
        "tenant_id": tenant_id,
        "ms_demand_id": dict(row)["ms_demand_id"],
        "state": state_code,
        "state_label": state_label,
        "description": state.get("Description"),
        "error_code": state.get("Error"),
        "error_message": state.get("ErrorMessage"),
    }


@router.get("/sync/{tenant_id}/demands")
def list_demands(tenant_id: str, limit: int = 20):
    """
    Возвращает последние отгрузки из МойСклад.
    Удобно для получения ms_demand_id перед вызовом /fiscalize.
    """
    tenant = _load_tenant(tenant_id)

    if not tenant.get("moysklad_token"):
        raise HTTPException(status_code=400, detail="moysklad_token not configured")

    ms_token = tenant["moysklad_token"]
    url = f"{MS_BASE}/entity/demand"
    params = {
        "limit": min(limit, 100),
        "order": "moment,desc",
    }

    try:
        r = requests.get(url, headers=_ms_headers(ms_token), params=params, timeout=20)
        if not r.ok:
            log.error(
                "failed to fetch demands",
                extra=_sync_extra(
                    tenant_id,
                    operation="sync.demands_list",
                    status="http_error",
                ),
            )
            r.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch demands from MoySklad: {e}")

    rows = r.json().get("rows", [])
    items = []
    for row in rows:
        meta = row.get("meta", {})
        agent = row.get("agent", {})
        agent_name = agent.get("name") if isinstance(agent, dict) else None

        items.append({
            "ms_demand_id": row.get("id"),
            "name": row.get("name"),
            "moment": row.get("moment"),
            "sum": row.get("sum"),
            "agent": agent_name,
            "url": meta.get("uuidHref"),
        })

    return {"count": len(items), "items": items}


@router.get("/sync/{tenant_id}/fiscal/clients")
def get_fiscal_clients(tenant_id: str):
    """
    Возвращает список клиентов (магазинов и касс) из fiscalization24.ru.
    Используется для получения fiscal_client_uid и fiscal_device_uid.
    """
    from app.clients.fiscalization_client import FiscalizationClient

    tenant = _load_tenant(tenant_id)

    if not tenant.get("fiscal_token"):
        raise HTTPException(
            status_code=400,
            detail="fiscal_token not configured. Use PATCH /tenants/{tenant_id}/fiscal",
        )

    try:
        client = FiscalizationClient(tenant["fiscal_token"])
        clients = client.get_clients()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to get clients: {e}")

    return {"count": len(clients), "clients": clients}

def _import_evotor_stock_to_ms(ms_token: str, tenant_id: str, evotor_store_id: str, products: list) -> dict:
    """Создаёт оприходование в МС с остатками из Эвотор после initial_sync."""
    from app.stores.mapping_store import MappingStore
    products_with_stock = [p for p in products if float(p.get("quantity") or 0) > 0]
    if not products_with_stock:
        return {"status": "ok", "entered": 0, "skipped": len(products)}
    ms_map = MappingStore()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        aq("SELECT ms_store_id, ms_organization_id FROM tenant_stores WHERE tenant_id = ? AND evotor_store_id = ?"),
        (tenant_id, evotor_store_id),
    )
    store_row = cur.fetchone()
    conn.close()
    if not store_row or not store_row["ms_store_id"] or not store_row["ms_organization_id"]:
        log.warning("_import_evotor_stock_to_ms: ms_store_id or ms_organization_id not set store=%s", evotor_store_id)
        return {"status": "skipped", "reason": "ms_store_id or ms_organization_id not configured"}
    ms_store_id = store_row["ms_store_id"]
    ms_org_id = store_row["ms_organization_id"]
    positions = []
    for product in products_with_stock:
        evotor_id = product.get("id")
        quantity = float(product.get("quantity") or 0)
        if not evotor_id or quantity <= 0:
            continue
        ms_id = ms_map.get_by_evotor_id(
            tenant_id=tenant_id, entity_type="product",
            evotor_id=evotor_id, evotor_store_id=evotor_store_id,
        )
        if not ms_id:
            continue
        cost_price = float(product.get("cost_price") or 0)
        positions.append({
            "quantity": quantity,
            "price": round(cost_price * 100),
            "assortment": {"meta": {"href": f"{MS_BASE}/entity/product/{ms_id}", "type": "product"}},
        })
    if not positions:
        return {"status": "ok", "entered": 0, "skipped": len(products)}
    r = requests.post(
        f"{MS_BASE}/entity/enter",
        headers=_ms_headers(ms_token),
        json={
            "organization": {"meta": {"href": f"{MS_BASE}/entity/organization/{ms_org_id}", "type": "organization"}},
            "store": {"meta": {"href": f"{MS_BASE}/entity/store/{ms_store_id}", "type": "store"}},
            "positions": positions,
        },
        timeout=30,
    )
    if r.ok:
        log.info("_import_evotor_stock_to_ms: created enter doc store=%s positions=%d", evotor_store_id, len(positions))
        return {"status": "ok", "entered": len(positions), "skipped": len(products) - len(positions)}
    else:
        log.error("_import_evotor_stock_to_ms: failed status=%s body=%s", r.status_code, r.text[:200])
        return {"status": "error", "error": r.text[:200]}


@router.post("/sync/{tenant_id}/stores/{evotor_store_id}/initial")
def initial_sync_store(tenant_id: str, evotor_store_id: str):
    """Первичная синхронизация конкретного магазина Эвотор -> МойСклад."""
    if evotor_store_id == "all":
        return initial_sync_all_stores(tenant_id)

    tenant = _load_tenant(tenant_id)
    store = _load_store(tenant_id, evotor_store_id)

    if not tenant.get("evotor_token"):
        raise HTTPException(status_code=409, detail="evotor_token not configured")
    if not tenant.get("moysklad_token"):
        raise HTTPException(status_code=400, detail="moysklad_token not configured")

    try:
        products = _get_evotor_products(tenant["evotor_token"], evotor_store_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Evotor products: {e}")

    ms_store = MappingStore()
    synced = skipped = failed = 0
    errors = []

    for product in products:
        evotor_id = product.get("id")
        if not evotor_id:
            skipped += 1
            continue

        # Ищем маппинг строго по этому магазину
        existing = ms_store.get_by_evotor_id(
            tenant_id=tenant_id,
            entity_type="product",
            evotor_id=evotor_id,
            evotor_store_id=evotor_store_id,
        )
        if existing:
            skipped += 1
            continue

        try:
            ms_id = _find_ms_product_by_external_code(tenant["moysklad_token"], evotor_id)
            if not ms_id:
                ms_id = _find_ms_product_by_name(tenant["moysklad_token"], product.get("name", ""))
                if ms_id:
                    mapped_evotor_id = ms_store.get_by_ms_id(
                        tenant_id=tenant_id,
                        entity_type="product",
                        ms_id=ms_id,
                        evotor_store_id=evotor_store_id,
                    )
                    if mapped_evotor_id and mapped_evotor_id != evotor_id:
                        log.warning(
                            "Unsafe name-match rejected evotor_id=%s name=%s ms_id=%s mapped_evotor_id=%s store=%s",
                            evotor_id,
                            product.get("name"),
                            ms_id,
                            mapped_evotor_id,
                            evotor_store_id,
                        )
                        ms_id = None

            if not ms_id:
                product["_evotor_token"] = tenant["evotor_token"]
                product["store_id"] = evotor_store_id
                ms_id = _create_ms_product(tenant["moysklad_token"], product)
                log.info(
                    "Created MS product evotor_id=%s ms_id=%s store=%s",
                    evotor_id, ms_id, evotor_store_id,
                )
                time.sleep(0.3)  # rate limit МС: не более ~5 req/sec
        except Exception as e:
            failed += 1
            errors.append({"evotor_id": evotor_id, "name": product.get("name"), "error": str(e)})
            continue

        # Финальная защита перед upsert:
        # даже если ms_id был найден по externalCode или name, нельзя использовать его,
        # если в этом же магазине он уже связан с другим evotor_id.
        occupied_evotor_id = ms_store.get_by_ms_id(
            tenant_id=tenant_id,
            entity_type="product",
            ms_id=ms_id,
            evotor_store_id=evotor_store_id,
        )
        if occupied_evotor_id and occupied_evotor_id != evotor_id:
            log.warning(
                "Resolved ms_id is occupied; creating separate MS product evotor_id=%s name=%s ms_id=%s occupied_by=%s store=%s",
                evotor_id,
                product.get("name"),
                ms_id,
                occupied_evotor_id,
                evotor_store_id,
            )
            product["_evotor_token"] = tenant["evotor_token"]
            product["store_id"] = evotor_store_id
            ms_id = _create_ms_product(tenant["moysklad_token"], product)
            log.info(
                "Created separate MS product for duplicate evotor_id=%s ms_id=%s store=%s",
                evotor_id,
                ms_id,
                evotor_store_id,
            )
            time.sleep(0.3)

        ok = ms_store.upsert_mapping(
            tenant_id=tenant_id,
            entity_type="product",
            evotor_id=evotor_id,
            ms_id=ms_id,
            evotor_store_id=evotor_store_id,
        )
        if ok:
            synced += 1
        else:
            msg = (
                f"Mapping conflict: evotor_id={evotor_id} ms_id={ms_id} "
                f"store={evotor_store_id}; ms_id already mapped to another evotor_id in this store"
            )
            log.warning(msg)
            failed += 1
            errors.append({
                "evotor_id": evotor_id,
                "name": product.get("name"),
                "ms_id": ms_id,
                "error": msg,
            })

    # Импортируем остатки из Эвотор в МС через оприходование
    if failed == 0 and products:
        try:
            stock_result = _import_evotor_stock_to_ms(
                tenant["moysklad_token"], tenant_id, evotor_store_id, products,
            )
            log.info("Evotor stock import store=%s result=%s", evotor_store_id, stock_result)
        except Exception as e:
            log.warning("Evotor stock import failed store=%s err=%s", evotor_store_id, e)

    # Обновляем папки для всех товаров с parent_id (включая уже существующие)
    ms_map_for_folders = MappingStore()
    for product in products:
        if not product.get("parent_id"):
            continue
        evotor_id = product.get("id")
        ms_id = ms_map_for_folders.get_by_evotor_id(
            tenant_id=tenant_id, entity_type="product",
            evotor_id=evotor_id, evotor_store_id=evotor_store_id,
        )
        if ms_id:
            try:
                _update_ms_product_folder(
                    tenant["moysklad_token"], ms_id, product,
                    tenant["evotor_token"], evotor_store_id,
                )
            except Exception as e:
                log.warning("Failed to update folder evotor_id=%s err=%s", evotor_id, e)

    if failed == 0:
        now = _now()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            aq("UPDATE tenant_stores SET sync_completed_at = ?, updated_at = ? WHERE tenant_id = ? AND evotor_store_id = ?"),
            (now, now, tenant_id, evotor_store_id),
        )
        # Обновляем tenants.sync_completed_at если это primary магазин
        # или если все магазины тенанта теперь синхронизированы
        cur.execute(
            aq("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN sync_completed_at IS NOT NULL THEN 1 ELSE 0 END) as done
            FROM tenant_stores WHERE tenant_id = ?
            """),
            (tenant_id,),
        )
        counts = cur.fetchone()
        if store.get("is_primary") or (counts and counts["total"] == counts["done"]):
            cur.execute(
                aq("UPDATE tenants SET sync_completed_at = ?, updated_at = ? WHERE id = ?"),
                (now, now, tenant_id),
            )
        conn.commit()
        conn.close()

    # Автоматически синхронизируем остатки после первичной синхронизации
    if failed == 0:
        try:
            reconcile_result = reconcile_stock_store(tenant_id, evotor_store_id)
            log.info(
                "Auto reconcile after initial_sync store=%s synced=%s failed=%s",
                evotor_store_id,
                reconcile_result.get("synced", 0),
                reconcile_result.get("failed", 0),
            )
        except Exception as e:
            log.warning("Auto reconcile failed store=%s err=%s", evotor_store_id, e)

    return {
        "status": "ok" if failed == 0 else "partial",
        "store": evotor_store_id,
        "synced": synced,
        "skipped": skipped,
        "failed": failed,
        "errors": errors,
    }


@router.post("/sync/{tenant_id}/stores/all/initial")
def initial_sync_all_stores(tenant_id: str):
    """Первичная синхронизация всех магазинов тенанта."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        aq("SELECT evotor_store_id FROM tenant_stores WHERE tenant_id = ? ORDER BY is_primary DESC, created_at ASC"),
        (tenant_id,),
    )
    stores = [r["evotor_store_id"] for r in cur.fetchall()]
    conn.close()

    if not stores:
        raise HTTPException(status_code=404, detail="No stores found for tenant")

    results = []
    for store_id in stores:
        try:
            result = initial_sync_store(tenant_id, store_id)
            results.append(result)
        except HTTPException as e:
            results.append({"store": store_id, "status": "error", "detail": e.detail})
        except Exception as e:
            results.append({"store": store_id, "status": "error", "detail": str(e)})

    total_synced = sum(r.get("synced", 0) for r in results)
    total_failed = sum(r.get("failed", 0) for r in results)

    return {
        "status": "ok" if total_failed == 0 else "partial",
        "stores_processed": len(results),
        "total_synced": total_synced,
        "total_failed": total_failed,
        "results": results,
    }


@router.post("/sync/{tenant_id}/stores/{evotor_store_id}/reconcile")
def reconcile_stock_store(tenant_id: str, evotor_store_id: str):
    """Синхронизация остатков конкретного магазина."""
    if evotor_store_id == "all":
        return reconcile_stock_all_stores(tenant_id)

    from app.clients.evotor_client import EvotorClient
    from app.clients.moysklad_client import MoySkladClient

    store = _load_store(tenant_id, evotor_store_id)

    if not store.get("sync_completed_at"):
        raise HTTPException(status_code=409, detail="Initial sync not completed for this store.")

    # Берём маппинги строго по этому магазину
    ms_store = MappingStore()
    mappings = ms_store.list_by_store(
        tenant_id=tenant_id,
        evotor_store_id=evotor_store_id,
        entity_type="product",
    )

    if not mappings:
        return {
            "status": "ok",
            "store": evotor_store_id,
            "synced": 0,
            "failed": 0,
            "errors": [],
            "message": "No mappings for this store",
        }

    ms_client = MoySkladClient(tenant_id)
    evotor_client = EvotorClient(tenant_id, store_id=evotor_store_id)
    synced = failed = 0
    errors = []

    # Получаем все остатки одним bulk запросом
    ms_stock_map: dict[str, float] = {}
    ms_store_id = store.get("ms_store_id")
    try:
        url = f"{MS_BASE}/report/stock/all?limit=1000"
        if ms_store_id:
            url += f"&filter=store={MS_BASE}/entity/store/{ms_store_id}"
        r = requests.get(
            url,
            headers=_ms_headers(ms_client.token),
            timeout=30,
        )
        if r.ok:
            for row in r.json().get("rows", []):
                pid = _extract_ms_product_id_from_stock_row(row)
                if pid:
                    stock = 0.0
                    for key in ("stock", "quantity", "inStock"):
                        val = row.get(key)
                        if val is not None:
                            try:
                                stock = float(val)
                                break
                            except (TypeError, ValueError):
                                pass
                    ms_stock_map[pid] = stock
            log.info("reconcile_stock_store bulk loaded %d stock rows store=%s", len(ms_stock_map), evotor_store_id)
        else:
            log.error("reconcile_stock_store bulk stock error status=%s", r.status_code)
    except Exception as e:
        log.error("reconcile_stock_store bulk stock failed err=%s", e)

    for item in mappings:
        try:
            quantity = ms_stock_map.get(item["ms_id"], 0.0)
            evotor_client.update_product_stock(item["evotor_id"], quantity)
            synced += 1
        except Exception as e:
            failed += 1
            errors.append({"ms_id": item["ms_id"], "error": str(e)})
    return {
        "status": "ok" if failed == 0 else "partial",
        "store": evotor_store_id,
        "synced": synced,
        "failed": failed,
        "errors": errors[:10],
    }


@router.post("/sync/{tenant_id}/stores/all/reconcile")
def reconcile_stock_all_stores(tenant_id: str):
    """Синхронизация остатков всех магазинов тенанта."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        aq("""
        SELECT evotor_store_id FROM tenant_stores
        WHERE tenant_id = ? AND sync_completed_at IS NOT NULL
        ORDER BY is_primary DESC, created_at ASC
        """),
        (tenant_id,),
    )
    stores = [r["evotor_store_id"] for r in cur.fetchall()]
    conn.close()

    if not stores:
        raise HTTPException(status_code=404, detail="No synced stores found for tenant")

    results = []
    for store_id in stores:
        try:
            result = reconcile_stock_store(tenant_id, store_id)
            results.append(result)
        except HTTPException as e:
            results.append({"store": store_id, "status": "error", "detail": e.detail})
        except Exception as e:
            results.append({"store": store_id, "status": "error", "detail": str(e)})

    return {
        "status": "ok" if all(r.get("status") == "ok" for r in results) else "partial",
        "stores_processed": len(results),
        "results": results,
    }


def _extract_ms_product_id_from_stock_row(row: dict) -> str | None:
    """
    Пытается безопасно извлечь ms_product_id из строки отчёта /report/stock/all.
    Приоритет:
    1) assortment.id
    2) assortment.meta.href
    3) product.id
    4) product.meta.href
    5) variant.id / consignment.id
    row["meta"]["href"] НЕ используем как источник product id.
    """
    def _from_meta(obj: dict | None) -> str | None:
        if not isinstance(obj, dict):
            return None
        href = ((obj.get("meta") or {}).get("href") or "").strip()
        if not href:
            return None
        return href.rstrip("/").split("/")[-1].split("?")[0] or None

    # assortment
    assortment = row.get("assortment")
    if isinstance(assortment, dict):
        if assortment.get("id"):
            return str(assortment["id"]).strip()
        val = _from_meta(assortment)
        if val:
            return val

    # product
    product = row.get("product")
    if isinstance(product, dict):
        if product.get("id"):
            return str(product["id"]).strip()
        val = _from_meta(product)
        if val:
            return val

    # variant
    variant = row.get("variant")
    if isinstance(variant, dict):
        if variant.get("id"):
            return str(variant["id"]).strip()
        val = _from_meta(variant)
        if val:
            return val

    # consignment
    consignment = row.get("consignment")
    if isinstance(consignment, dict):
        if consignment.get("id"):
            return str(consignment["id"]).strip()
        val = _from_meta(consignment)
        if val:
            return val

    # fallback: берём id из row["meta"]["href"] — это стандартная структура /report/stock/all
    meta = row.get("meta")
    if isinstance(meta, dict):
        href = (meta.get("href") or "").strip()
        if href:
            # убираем query string (?expand=supplier и т.д.)
            val = href.rstrip("/").split("/")[-1].split("?")[0]
            if val:
                return val
    return None

@router.post("/sync/{tenant_id}/stores/{evotor_store_id}/ms-to-evotor")
def sync_ms_to_evotor_store(tenant_id: str, evotor_store_id: str):
    """
    Синхронизация товаров МойСклад -> конкретный магазин Эвотор.
    Для тестового контура:
    - список товаров магазина определяется как товары с остатком > 0 на ms_store_id этого магазина
    - существующие товары обновляются bulk PUT
    - лишние товары удаляются, если у них больше нет остатка на этом складе
    """
    tenant = _load_tenant(tenant_id)
    store = _load_store(tenant_id, evotor_store_id)

    if not tenant.get("moysklad_token"):
        raise HTTPException(status_code=400, detail="moysklad_token not configured")
    if not tenant.get("evotor_token"):
        raise HTTPException(status_code=400, detail="evotor_token not configured")

    ms_store_id = store.get("ms_store_id")
    if not ms_store_id:
        raise HTTPException(status_code=400, detail="ms_store_id not configured for this store")

    try:
        data = _search_ms_products(tenant["moysklad_token"], limit=1000, offset=0)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch MoySklad products: {e}")

    all_ms_products = data.get("rows", []) or []

    ms_products = []
    positive_stock_ids = set()
    stock_checked = 0

    for row in all_ms_products:
        ms_id = row.get("id")
        if not ms_id:
            continue

        try:
            qty = _get_ms_product_stock_for_store(
                tenant["moysklad_token"],
                ms_id,
                ms_store_id,
            )
            stock_checked += 1
        except Exception as e:
            log.warning(
                "sync_ms_to_evotor_store: stock check failed ms_id=%s ms_store_id=%s err=%s",
                ms_id,
                ms_store_id,
                e,
            )
            continue

        if qty > 0:
            row["_store_stock"] = qty
            ms_products.append(row)
            positive_stock_ids.add(ms_id)

    log.info(
        "sync_ms_to_evotor_store: store=%s ms_store=%s products_with_stock=%d checked=%d",
        evotor_store_id,
        ms_store_id,
        len(ms_products),
        stock_checked,
    )

    ms_ids_in_ms = {row["id"] for row in ms_products if row.get("id")}

    ms_map = MappingStore()
    synced = 0
    skipped = 0
    failed = 0
    deleted = 0
    bulk_update_items = []
    errors = []

    for row in ms_products:
        ms_id = row.get("id")
        if not ms_id:
            skipped += 1
            continue

        try:
            import requests as _req

            existing_evotor_id = ms_map.get_by_ms_id(
                tenant_id=tenant_id,
                entity_type="product",
                ms_id=ms_id,
                evotor_store_id=evotor_store_id,
            )

            if existing_evotor_id:
                try:
                    ms_product_full = _get_ms_product(tenant["moysklad_token"], ms_id, expand="productFolder")
                    upd_payload = _build_evotor_product_payload(
                        ms_product_full,
                        evotor_id=existing_evotor_id,
                        current_product=None,
                        for_create=False,
                    )
                    upd_payload["id"] = existing_evotor_id

                    for _f in ("store_id", "user_id", "created_at", "updated_at"):
                        upd_payload.pop(_f, None)

                    # Передаём группу товара если есть папка в МС
                    _apply_product_group(
                        upd_payload, ms_product_full,
                        tenant_id, evotor_store_id, tenant["evotor_token"],
                    )
                    bulk_update_items.append(upd_payload)
                except Exception as _e:
                    log.warning("Failed to build update payload ms_id=%s err=%s", ms_id, _e)
                    skipped += 1
                continue

            payload = _build_evotor_product_payload(
                row,
                evotor_id=None,
                current_product=None,
                for_create=True,
            )
            _apply_product_group(payload, row, tenant_id, evotor_store_id, tenant["evotor_token"])

            r = _req.post(
                f"{EVOTOR_BASE}/stores/{evotor_store_id}/products",
                headers=_evotor_headers(tenant["evotor_token"]),
                json=payload,
                timeout=20,
            )
            if r.ok:
                created = r.json() if r.text else {}
                created_id = created.get("id")
                if created_id:
                    ms_map.upsert_mapping(
                        tenant_id=tenant_id,
                        entity_type="product",
                        evotor_id=created_id,
                        ms_id=ms_id,
                        evotor_store_id=evotor_store_id,
                    )
                    synced += 1
                else:
                    failed += 1
                    errors.append(f"{ms_id}: no id in response")
            else:
                failed += 1
                errors.append(f"{ms_id}: Evotor POST {r.status_code} {r.text[:120]}")
        except Exception as e:
            failed += 1
            errors.append(f"{ms_id}: {e}")

    # Bulk update существующих товаров
    if bulk_update_items:
        import requests as _req_bulk
        CHUNK_SIZE = 100

        for _chunk_start in range(0, len(bulk_update_items), CHUNK_SIZE):
            _chunk = bulk_update_items[_chunk_start:_chunk_start + CHUNK_SIZE]
            try:
                _r = _req_bulk.put(
                    f"{EVOTOR_BASE}/stores/{evotor_store_id}/products",
                    headers=_evotor_headers(tenant["evotor_token"]),
                    json=_chunk,
                    timeout=30,
                )
                if _r.ok:
                    synced += len(_chunk)
                    log.info("Bulk updated %d products store=%s", len(_chunk), evotor_store_id)
                else:
                    failed += len(_chunk)
                    errors.append(f"Bulk update failed: {_r.status_code}")
                    log.error("Bulk update failed status=%s body=%s", _r.status_code, _r.text[:200])
            except Exception as _e:
                failed += len(_chunk)
                errors.append(f"Bulk update error: {_e}")

    # Безопасный режим: ручная МС→Эвотор синхронизация только добавляет/обновляет товары.
    # Автоудаление отключено, потому что временный сбой проверки остатков МойСклад
    # может ошибочно удалить store-specific mapping и товар из Эвотор.
    store_mappings = ms_map.list_by_store(tenant_id, evotor_store_id, "product")
    would_delete = [m for m in store_mappings if m["ms_id"] not in positive_stock_ids]

    log.info(
        "sync_ms_to_evotor_store diff: store=%s mapped=%d current=%d would_delete=%d auto_delete_disabled=true",
        evotor_store_id,
        len(store_mappings),
        len(positive_stock_ids),
        len(would_delete),
    )

    return {
        "status": "ok" if failed == 0 else "partial",
        "store": evotor_store_id,
        "checked_products": stock_checked,
        "products_total": len(all_ms_products),
        "products_with_stock": len(ms_products),
        "products_without_stock": max(stock_checked - len(ms_products), 0),
        "stock_check_failed": max(len(all_ms_products) - stock_checked, 0),
        "synced": synced,
        "skipped": skipped,
        "failed": failed,
        "deleted": deleted,
        "errors": errors[:10],
    }

# ------------------------------------------------------------------------------
# Product sync MoySklad -> Evotor (without stock overwrite)
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Stock sync MoySklad -> Evotor
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Fiscalization: MoySklad demand -> fiscalization24.ru
# ------------------------------------------------------------------------------

# Маппинг НДС МойСклад → fiscalization24 (целые числа)
# fiscalization24 допускает: -1 (без НДС), 0, 5, 7, 10, 18, 20
VAT_MS_TO_FISCAL = {
    (0, False): -1,   # без НДС
    (0, True):   0,   # НДС 0%
    (5, True):   5,
    (7, True):   7,
    (10, True):  10,
    (18, True):  18,
    (20, True):  20,
}

# Маппинг типов товаров Эвотор → fiscalization24
EVOTOR_TYPE_TO_FISCAL = {
    "NORMAL":         0,   # обычный
    "DAIRY_MARKED":   12,  # молочная продукция
    "ALCOHOL_MARKED":  1,  # маркированный алкоголь
    "TOBACCO_MARKED":  4,  # маркированный табак
    "SHOES_MARKED":    5,  # маркированная обувь
    "MEDICINE_MARKED": 6,  # маркированные лекарства
    "WATER":          13,  # вода
}


def _money_from_ms(value) -> float:
    try:
        return round(float(value or 0) / 100.0, 2)
    except (TypeError, ValueError):
        return 0.0


def _fetch_demand_positions(ms_token: str, positions_raw) -> list:
    if isinstance(positions_raw, dict) and "meta" in positions_raw:
        pos_url = positions_raw["meta"]["href"]
        r = requests.get(
            pos_url,
            headers=_ms_headers(ms_token),
            params={"expand": "assortment"},
            timeout=20,
        )
        if not r.ok:
            log.error("Failed to fetch demand positions status=%s", r.status_code)
            r.raise_for_status()
        return r.json().get("rows", [])
    return positions_raw if isinstance(positions_raw, list) else []


def _map_demand_to_fiscal_check(
    demand: dict,
    tenant_id: str,
    ms_token: str,
    fiscal_client_uid: str,
    fiscal_device_uid: str,
    check_uid: str,
) -> dict:
    positions_list = _fetch_demand_positions(ms_token, demand.get("positions", {}))

    fiscal_products = []

    for item in positions_list:
        assortment = item.get("assortment", {}) or {}
        meta = assortment.get("meta", {}) or {}

        quantity = round(float(item.get("quantity", 1) or 1), 3)
        price_value = _money_from_ms(item.get("price"))

        vat = item.get("vat", 0) or 0
        vat_enabled = item.get("vatEnabled")
        if vat_enabled is False:
            tax = -1
        else:
            vat_value = int(vat or 0)
            tax = VAT_MS_TO_FISCAL.get((vat_value, True), -1)

        tracking_type = assortment.get("trackingType") or "NORMAL"
        product_type = EVOTOR_TYPE_TO_FISCAL.get(str(tracking_type).upper(), 0)

        discount_percent = float(item.get("discount", 0) or 0)
        if discount_percent < 0 or discount_percent > 100:
            raise ValueError(
                f"Fiscalization discount must be 0..100, got {discount_percent}"
            )

        product_name = (
            assortment.get("name")
            or item.get("name")
            or meta.get("name")
            or f"Product {meta.get('href', '').rstrip('/').split('/')[-1]}"
        )

        if not product_name.strip():
            raise ValueError("Fiscalization product name is empty")

        if price_value <= 0:
            raise ValueError(f"Fiscalization product price must be > 0, got {price_value}")

        if quantity <= 0:
            raise ValueError(f"Fiscalization product quantity must be > 0, got {quantity}")

        fiscal_products.append({
            "type": product_type,
            "tax": tax,
            "name": product_name,
            "price": price_value,
            "discount": round(discount_percent, 2),
            "quantity": quantity,
        })

    total_sum_value = _money_from_ms(demand.get("sum"))

    if total_sum_value <= 0:
        raise ValueError(f"Fiscalization total sum must be > 0, got {total_sum_value}")

    calc_sum = round(sum(
        p["price"] * p["quantity"] * (1 - p["discount"] / 100.0)
        for p in fiscal_products
    ), 2)
    if abs(calc_sum - total_sum_value) > 0.05:
        log.warning(
            "Fiscal sum mismatch: demand.sum=%.2f calc=%.2f tenant_id=%s",
            total_sum_value, calc_sum, tenant_id,
        )

    return {
        "UID": check_uid,
        "ClientUid": fiscal_client_uid,
        "DeviceUid": fiscal_device_uid,
        "Data": {
            "uid": check_uid,
            "products": fiscal_products,
            "payCashSumma": total_sum_value,
            "payCardSumma": 0.0,
            "discount": 0,
            "type": 0,
            "paymentType": 1,
        },
    }


def _get_existing_fiscal_check(tenant_id: str, ms_demand_id: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        aq("""
        SELECT *
        FROM fiscalization_checks
        WHERE tenant_id = ? AND ms_demand_id = ?
        """),
        (tenant_id, ms_demand_id),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def _save_fiscal_check(
    tenant_id: str,
    uid: str,
    ms_demand_id: str,
    fiscal_client_uid: str,
    fiscal_device_uid: str,
    status: int,
    request_json: str,
    response_json: str,
    description: str | None = None,
    error_code: int | None = None,
    error_message: str | None = None,
) -> None:
    now = _now()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            INSERT INTO fiscalization_checks (
                uid, tenant_id, ms_demand_id,
                fiscal_client_uid, fiscal_device_uid,
                status, description, error_code, error_message,
                request_json, response_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET
                status=excluded.status,
                description=excluded.description,
                error_code=excluded.error_code,
                error_message=excluded.error_message,
                response_json=excluded.response_json,
                updated_at=excluded.updated_at
            """),
            (
                uid, tenant_id, ms_demand_id,
                fiscal_client_uid, fiscal_device_uid,
                status, description, error_code, error_message,
                request_json, response_json,
                now, now,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _update_fiscal_check_state(uid: str, state: dict) -> None:
    now = _now()
    import json as _json
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            UPDATE fiscalization_checks
            SET status=?, description=?, error_code=?, error_message=?,
                response_json=?, updated_at=?
            WHERE uid=?
            """),
            (
                state.get("State", 1),
                state.get("Description"),
                state.get("Error"),
                state.get("ErrorMessage"),
                _json.dumps(state, ensure_ascii=False),
                now,
                uid,
            ),
        )
        conn.commit()
    finally:
        conn.close()


