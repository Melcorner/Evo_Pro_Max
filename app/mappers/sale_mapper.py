import logging

from app.stores.mapping_store import MappingStore

log = logging.getLogger("sale_mapper")


class SalePayloadError(ValueError):
    """Фатальная ошибка валидации payload — не требует retry."""
    status_code = 422

class MappingNotFoundError(ValueError):
    """Ошибка отсутствия mapping — может быть RETRY или FAILED."""
    status_code = 404
    
def validate_sale_payload(payload: dict):
    """
    Проверяет входящий payload перед маппингом.
    Бросает SalePayloadError при невалидных данных.
    """
    if not payload.get("event_id"):
        raise SalePayloadError("Missing required field: event_id")

    positions = payload.get("positions")

    if positions is None:
        raise SalePayloadError("Missing required field: positions")

    if not isinstance(positions, list):
        raise SalePayloadError("Field 'positions' must be a list")

    if len(positions) == 0:
        raise SalePayloadError("Field 'positions' must not be empty")

    for i, item in enumerate(positions):
        if not item.get("product_id"):
            raise SalePayloadError(f"Position[{i}]: missing product_id")

        quantity = item.get("quantity")
        if quantity is None or not isinstance(quantity, (int, float)) or quantity <= 0:
            raise SalePayloadError(f"Position[{i}]: invalid quantity={quantity}")

        price = item.get("price")
        if price is None or not isinstance(price, (int, float)) or price < 0:
            raise SalePayloadError(f"Position[{i}]: invalid price={price}")


def map_sale_to_ms(payload: dict, tenant_id: str = None) -> dict:
    log.info("Mapping sale payload")

    validate_sale_payload(payload)

    event_id = payload.get("event_id")
    store = MappingStore() if tenant_id else None

    positions = payload.get("positions", [])
    ms_positions = []
    total_sum = 0

    for i, item in enumerate(positions):
        evotor_product_id = item.get("product_id")
        quantity = item.get("quantity", 0)
        price = item.get("price", 0)

        line_sum = quantity * price
        total_sum += line_sum

        # Резолвим product_id Эвотора -> ms_id МойСклад
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
                    f"Mapping not found for product_id={evotor_product_id}"
                )

        # Формируем позицию в формате МойСклад
        ms_position = {
            "quantity": quantity,
            "price": price,
            "sum": line_sum,
        }

        if ms_product_id:
            ms_position["assortment"] = {
                "meta": {
                    "href": f"https://api.moysklad.ru/api/remap/1.2/entity/product/{ms_product_id}",
                    "type": "product",
                    "mediaType": "application/json"
                }
            }
        else:
            # Маппинга нет — кладём raw product_id для диагностики
            ms_position["product_id"] = evotor_product_id

        ms_positions.append(ms_position)

    ms_payload = {
        "syncId": event_id,
        "name": f"Sale {event_id}",
        "description": "Created from Evotor webhook",
        "positions": ms_positions,
        "sum": total_sum
    }

    log.info(f"Mapped sale payload syncId={event_id} positions={len(ms_positions)} sum={total_sum}")

    return ms_payload