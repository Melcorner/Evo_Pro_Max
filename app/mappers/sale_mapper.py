import logging

from app.stores.mapping_store import MappingStore

log = logging.getLogger("sale_mapper")

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"


class SalePayloadError(ValueError):
    """Фатальная ошибка валидации payload — не требует retry."""
    status_code = 422


class MappingNotFoundError(ValueError):
    """Ошибка отсутствия mapping — классифицируется как FAILED (status_code=404)."""
    status_code = 404


def validate_sale_payload(payload: dict):
    """
    Валидирует payload в формате Эвотор.
    """
    if not payload.get("id"):
        raise SalePayloadError("Missing required field: id")

    if payload.get("type") not in ("SELL", "sell"):
        raise SalePayloadError(f"Unexpected document type: {payload.get('type')}")

    body = payload.get("body")
    if not body:
        raise SalePayloadError("Missing required field: body")

    positions = body.get("positions")
    if positions is None:
        raise SalePayloadError("Missing required field: body.positions")

    if not isinstance(positions, list) or len(positions) == 0:
        raise SalePayloadError("Field 'body.positions' must be a non-empty list")

    for i, item in enumerate(positions):
        if not item.get("product_id"):
            raise SalePayloadError(f"Position[{i}]: missing product_id")

        quantity = item.get("quantity")
        if quantity is None or not isinstance(quantity, (int, float)) or quantity <= 0:
            raise SalePayloadError(f"Position[{i}]: invalid quantity={quantity}")

        price = item.get("price")
        if price is None or not isinstance(price, (int, float)) or price < 0:
            raise SalePayloadError(f"Position[{i}]: invalid price={price}")


def _meta(entity_type: str, entity_id: str) -> dict:
    return {
        "meta": {
            "href": f"{MS_BASE}/entity/{entity_type}/{entity_id}",
            "type": entity_type,
            "mediaType": "application/json"
        }
    }


def map_sale_to_ms(
    payload: dict,
    sync_id: str = None,
    tenant_id: str = None,
    ms_organization_id: str = None,
    ms_store_id: str = None,
    ms_agent_id: str = None,
) -> dict:
    """
    Маппит payload формата Эвотор в формат МойСклад demand.
    """
    log.info("Mapping sale payload")

    validate_sale_payload(payload)

    event_id = payload.get("id")   # id документа из Эвотора
    body = payload.get("body", {})

    if not sync_id:
        raise SalePayloadError("Missing required field: sync_id")
    raw_positions = body.get("positions", [])

    store = MappingStore() if tenant_id else None
    ms_positions = []
    total_sum = 0

    for i, item in enumerate(raw_positions):
        evotor_product_id = item.get("product_id")
        quantity = item.get("quantity", 0)
        price = item.get("price", 0)
        line_sum = item.get("sum") or quantity * price
        total_sum += line_sum

        ms_product_id = None
        if store and tenant_id and evotor_product_id:
            ms_product_id = store.get_by_evotor_id(
                tenant_id=tenant_id,
                entity_type="product",
                evotor_id=evotor_product_id
            )
            if ms_product_id:
                log.info(f"Position[{i}]: mapping found {evotor_product_id} -> {ms_product_id}")
            else:
                raise MappingNotFoundError(
                    f"Mapping not found for product_id={evotor_product_id} "
                    f"name={item.get('product_name')}"
                )

        ms_position = {
            "quantity": quantity,
            "price": int(price * 100),
            "sum": int(line_sum * 100),
        }

        if ms_product_id:
            ms_position["assortment"] = _meta("product", ms_product_id)

        ms_positions.append(ms_position)

    ms_payload = {
        "syncId": sync_id,
        "name": f"Sale {event_id}",
        "description": "Created from Evotor webhook",
        "positions": ms_positions,
        "sum": int(total_sum * 100),
    }

    # Обязательные поля МойСклад
    if ms_organization_id:
        ms_payload["organization"] = _meta("organization", ms_organization_id)
    if ms_store_id:
        ms_payload["store"] = _meta("store", ms_store_id)
    if ms_agent_id:
        ms_payload["agent"] = _meta("counterparty", ms_agent_id)

    log.info(f"Mapped sale payload syncId={sync_id} positions={len(ms_positions)} sum={total_sum}")


    return ms_payload