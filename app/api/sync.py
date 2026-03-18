import time
import logging
import requests

from fastapi import APIRouter, HTTPException
from app.db import get_connection
from app.stores.mapping_store import MappingStore

log = logging.getLogger("api.sync")
router = APIRouter()

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
EVOTOR_BASE = "https://api.evotor.ru"


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
        "Accept-Encoding": "gzip"
    }


def _get_evotor_products(evotor_token: str, store_id: str) -> list:
    """Получает все товары из облака Эвотор."""
    url = f"{EVOTOR_BASE}/stores/{store_id}/products"
    r = requests.get(url, headers=_evotor_headers(evotor_token), timeout=30)

    if not r.ok:
        log.error(f"Evotor products error status={r.status_code} body={r.text}")
        r.raise_for_status()

    data = r.json()
    products = data.get("items", [])
    log.info(f"Fetched {len(products)} products from Evotor store={store_id}")
    return products


def _create_ms_product(ms_token: str, product: dict) -> str:
    """Создаёт товар в МойСклад. Возвращает ms_id."""
    payload = {
        "name": product["name"],
        "externalCode": product["id"],  # evotor_id как внешний код
        "description": product.get("description", ""),
        "salePrices": [
            {
                "value": round(product.get("price", 0) * 100),
                "currency": {
                    "meta": {
                        "href": f"{MS_BASE}/entity/currency",
                        "type": "currency",
                        "mediaType": "application/json"
                    }
                },
                "priceType": {
                    "meta": {
                        "href": f"{MS_BASE}/context/companysettings/pricetype/",
                        "type": "pricetype",
                        "mediaType": "application/json"
                    }
                }
            }
        ]
    }

    # Добавляем закупочную цену если есть
    if product.get("cost_price"):
        payload["buyPrice"] = {
            "value": round(product["cost_price"] * 100),
            "currency": {
                "meta": {
                    "href": f"{MS_BASE}/entity/currency",
                    "type": "currency",
                    "mediaType": "application/json"
                }
            }
        }

    # Добавляем штрихкод если есть
    barcodes = product.get("barcodes", [])
    if barcodes:
        payload["barcodes"] = [{"ean13": barcodes[0]}]

    url = f"{MS_BASE}/entity/product"
    r = requests.post(url, headers=_ms_headers(ms_token), json=payload, timeout=15)

    if not r.ok:
        log.error(f"MoySklad create product error status={r.status_code} body={r.text}")
        r.raise_for_status()

    ms_product = r.json()
    return ms_product["id"]


@router.post("/sync/{tenant_id}/initial")
def initial_sync(tenant_id: str):
    """
    Первичная синхронизация товаров из Эвотор в МойСклад.

    Алгоритм:
    1. Проверяем что sync_completed_at IS NULL (синхронизация не была выполнена)
    2. Получаем все товары из облака Эвотор
    3. Для каждого товара создаём его в МойСклад
    4. Сохраняем маппинг evotor_id → ms_id
    5. Устанавливаем sync_completed_at = now()

    Идемпотентен — пропускает товары у которых уже есть маппинг.
    """
    tenant = _load_tenant(tenant_id)

    # Проверяем что синхронизация не была выполнена
    if tenant.get("sync_completed_at"):
        raise HTTPException(
            status_code=409,
            detail="Initial sync already completed. Use DELETE /tenants/{id}/complete-sync to reset."
        )

    # Проверяем что tenant настроен
    if not tenant.get("evotor_token"):
        raise HTTPException(status_code=400, detail="evotor_token not configured")
    if not tenant.get("evotor_store_id"):
        raise HTTPException(status_code=400, detail="evotor_store_id not configured. Use PATCH /tenants/{id}/moysklad")
    if not tenant.get("moysklad_token"):
        raise HTTPException(status_code=400, detail="moysklad_token not configured")

    evotor_token = tenant["evotor_token"]
    evotor_store_id = tenant["evotor_store_id"]
    ms_token = tenant["moysklad_token"]

    # Получаем товары из Эвотор
    try:
        products = _get_evotor_products(evotor_token, evotor_store_id)
    except Exception as e:
        log.error(f"Failed to fetch Evotor products tenant_id={tenant_id} err={e}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch Evotor products: {e}")

    if not products:
        return {
            "status": "ok",
            "synced": 0,
            "skipped": 0,
            "failed": 0,
            "message": "No products found in Evotor"
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

        # Проверяем есть ли уже маппинг — идемпотентность
        existing = store.get_by_evotor_id(
            tenant_id=tenant_id,
            entity_type="product",
            evotor_id=evotor_id
        )
        if existing:
            log.info(f"Skipping already mapped product evotor_id={evotor_id} ms_id={existing}")
            skipped += 1
            continue

        # Создаём товар в МойСклад
        try:
            ms_id = _create_ms_product(ms_token, product)
            log.info(f"Created MS product evotor_id={evotor_id} ms_id={ms_id} name={product.get('name')}")
        except Exception as e:
            log.error(f"Failed to create MS product evotor_id={evotor_id} name={product.get('name')} err={e}")
            failed += 1
            errors.append({"evotor_id": evotor_id, "name": product.get("name"), "error": str(e)})
            continue

        # Сохраняем маппинг
        ok = store.upsert_mapping(
            tenant_id=tenant_id,
            entity_type="product",
            evotor_id=evotor_id,
            ms_id=ms_id
        )
        if ok:
            synced += 1
        else:
            log.warning(f"Mapping conflict evotor_id={evotor_id} ms_id={ms_id}")
            failed += 1

    # Если все товары синхронизированы успешно — отмечаем синхронизацию завершённой
    if failed == 0:
        conn = get_connection()
        conn.execute(
            "UPDATE tenants SET sync_completed_at = ? WHERE id = ?",
            (int(time.time()), tenant_id)
        )
        conn.commit()
        conn.close()
        log.info(f"Initial sync completed tenant_id={tenant_id} synced={synced}")

    return {
        "status": "ok" if failed == 0 else "partial",
        "synced": synced,
        "skipped": skipped,
        "failed": failed,
        "errors": errors,
        "sync_mode": "moysklad" if failed == 0 else "evotor"
    }


@router.get("/sync/{tenant_id}/status")
def sync_status(tenant_id: str):
    """
    Возвращает статус синхронизации tenant'а.
    """
    tenant = _load_tenant(tenant_id)

    store = MappingStore()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) as cnt FROM mappings WHERE tenant_id = ? AND entity_type = 'product'",
        (tenant_id,)
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