import json
import logging

from app.db import get_connection
from app.stores.mapping_store import MappingStore

log = logging.getLogger("stock_handler")

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"


class StockPayloadError(ValueError):
    """Фатальная ошибка валидации stock payload."""
    status_code = 422


class StockMappingNotFoundError(ValueError):
    """Маппинг товара не найден."""
    status_code = 404


def _load_ms_config(tenant_id: str) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT moysklad_token, ms_store_id
        FROM tenants WHERE id = ?
    """, (tenant_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else {}


def validate_stock_payload(payload: dict):
    """
    Валидирует stock payload от Эвотор.

    Ожидаемый формат:
    {
        "id": "<evotor_product_uuid>",
        "store_id": "<evotor_store_uuid>",
        "quantity": 10.0
    }
    """
    if not payload.get("id"):
        raise StockPayloadError("Missing required field: id")

    quantity = payload.get("quantity")
    if quantity is None or not isinstance(quantity, (int, float)):
        raise StockPayloadError(f"Invalid quantity={quantity}")


def handle_stock(event_row):
    """
    Обрабатывает событие изменения остатков.

    Алгоритм:
    1. Берём evotor_product_id из payload
    2. Резолвим evotor_id → ms_product_id через MappingStore
    3. Обновляем остаток в МойСклад через PUT /entity/store/{ms_store_id}/quantity
    """
    log.info(f"Handle stock event_id={event_row['id']} event_key={event_row['event_key']}")

    payload = json.loads(event_row["payload_json"])
    tenant_id = event_row["tenant_id"]

    validate_stock_payload(payload)

    evotor_product_id = payload["id"]
    quantity = payload["quantity"]

    # Резолвим маппинг
    store = MappingStore()
    ms_product_id = store.get_by_evotor_id(
        tenant_id=tenant_id,
        entity_type="product",
        evotor_id=evotor_product_id
    )

    if not ms_product_id:
        raise StockMappingNotFoundError(
            f"Mapping not found for evotor_product_id={evotor_product_id}"
        )

    log.info(f"Stock mapping found evotor_id={evotor_product_id} ms_id={ms_product_id}")

    # Загружаем конфиг МойСклад
    ms_config = _load_ms_config(tenant_id)
    ms_token = ms_config.get("moysklad_token")
    ms_store_id = ms_config.get("ms_store_id")

    if not ms_token:
        raise StockPayloadError("moysklad_token not configured")
    if not ms_store_id:
        raise StockPayloadError("ms_store_id not configured")

    # Обновляем остаток в МойСклад
    import requests

    headers = {
        "Authorization": f"Bearer {ms_token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip"
    }

    # Создаём инвентаризацию (оприходование/списание) для обновления остатка
    payload_ms = {
        "store": {
            "meta": {
                "href": f"{MS_BASE}/entity/store/{ms_store_id}",
                "type": "store",
                "mediaType": "application/json"
            }
        },
        "positions": [
            {
                "assortment": {
                    "meta": {
                        "href": f"{MS_BASE}/entity/product/{ms_product_id}",
                        "type": "product",
                        "mediaType": "application/json"
                    }
                },
                "quantity": quantity,
                "correctionAmount": quantity,
                "calculatedQuantity": quantity
            }
        ]
    }

    url = f"{MS_BASE}/entity/inventory"
    r = requests.post(url, headers=headers, json=payload_ms, timeout=15)

    log.info(f"MoySklad inventory response={r.status_code}")

    if not r.ok:
        try:
            log.error(f"MoySklad inventory error body={r.json()}")
        except Exception:
            log.error(f"MoySklad inventory error text={r.text}")
        r.raise_for_status()

    result = r.json()
    result_ref = result.get("id", "inventory:created")

    log.info(f"Stock updated event_id={event_row['id']} ms_product_id={ms_product_id} quantity={quantity} result_ref={result_ref}")

    return result_ref