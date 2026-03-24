import logging

from app.stores.mapping_store import MappingStore

log = logging.getLogger("sale_mapper")

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

LEGACY_VAT_MAP = {
    "NO_VAT": {"vat": 0, "vatEnabled": False},
    "WITHOUT_VAT": {"vat": 0, "vatEnabled": False},
    "VAT_10": {"vat": 10, "vatEnabled": True},
    "VAT_20": {"vat": 20, "vatEnabled": True},
}


class SalePayloadError(ValueError):
    """Фатальная ошибка валидации payload — не требует retry."""
    status_code = 422


class MappingNotFoundError(ValueError):
    """Ошибка отсутствия mapping — классифицируется как FAILED (status_code=404)."""
    status_code = 404


def validate_sale_payload(payload: dict):
    if not payload.get("id"):
        raise SalePayloadError("Missing required field: id")

    if payload.get("type") != "SELL":
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
            "mediaType": "application/json",
        }
    }


def _build_description(payload: dict, counterparty_resolution_source: str | None) -> str:
    lines = ["Created from Evotor webhook"]

    customer = payload.get("customer") or {}
    if isinstance(customer, dict):
        if customer.get("name"):
            lines.append(f"Customer: {customer['name']}")
        if customer.get("phone"):
            lines.append(f"Phone: {customer['phone']}")
        if customer.get("email"):
            lines.append(f"Email: {customer['email']}")
        if customer.get("inn"):
            lines.append(f"INN: {customer['inn']}")

    if counterparty_resolution_source:
        lines.append(f"Counterparty resolution: {counterparty_resolution_source}")

    return "\n".join(lines)


def _to_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_discount_percent(item: dict, base_sum: float, final_sum: float) -> float | None:
    """
    Вариант 2:
    - в МойСклад отправляем исходную цену/сумму
    - discount передаём отдельно

    Поддерживаются сценарии:
    1) position_discount/doc_distributed_discount с discount_percent
    2) простой абсолютный item.discount из реальных ReceiptCreated
    3) вычисление по разнице base_sum -> final_sum
    """
    position_discount = item.get("position_discount", item.get("positionDiscount")) or {}
    doc_distributed_discount = item.get(
        "doc_distributed_discount", item.get("docDistributedDiscount")
    ) or {}

    total_percent = 0.0
    found_explicit_percent = False

    for src in (position_discount, doc_distributed_discount):
        if not isinstance(src, dict):
            continue
        raw_percent = src.get("discount_percent", src.get("discountPercent"))
        if raw_percent is None:
            continue
        try:
            total_percent += float(raw_percent)
            found_explicit_percent = True
        except (TypeError, ValueError):
            pass

    if found_explicit_percent and total_percent > 0:
        return round(total_percent, 2)

    raw_discount = _to_float(item.get("discount"))
    if raw_discount is not None and base_sum > 0:
        return round((raw_discount / base_sum) * 100, 2)

    if base_sum > 0 and final_sum < base_sum:
        return round((1 - final_sum / base_sum) * 100, 2)

    return None


def _extract_vat_fields(item: dict) -> dict:
    """
    Поддерживает два сценария:
    1) Реальный ReceiptCreated: taxPercent = 0 / 10 / 20
    2) Легаси-формат: tax = {type: NO_VAT/VAT_10/VAT_20}
    """
    raw_tax_percent = _to_float(item.get("tax_percent", item.get("taxPercent")))
    if raw_tax_percent is not None:
        rounded = int(round(raw_tax_percent))
        if rounded == 0:
            return {"vat": 0, "vatEnabled": False}
        if rounded in (10, 20):
            return {"vat": rounded, "vatEnabled": True}
        log.warning(f"Unsupported Evotor taxPercent={raw_tax_percent}")
        return {}

    tax = item.get("tax")
    if isinstance(tax, dict):
        tax_type = tax.get("type")
        if not tax_type:
            return {}
        result = LEGACY_VAT_MAP.get(tax_type)
        if result is None:
            log.warning(f"Unsupported Evotor tax.type={tax_type}")
            return {}
        return result

    return {}


def map_sale_to_ms(
    payload: dict,
    sync_id: str = None,
    tenant_id: str = None,
    ms_organization_id: str = None,
    ms_store_id: str = None,
    ms_agent_id: str = None,
    counterparty_resolution_source: str | None = None,
) -> dict:
    log.info("Mapping sale payload")

    validate_sale_payload(payload)

    event_id = payload.get("id")
    body = payload.get("body", {}) or {}
    effective_sync_id = sync_id or event_id
    raw_positions = body.get("positions", [])

    store = MappingStore() if tenant_id else None
    ms_positions = []

    for i, item in enumerate(raw_positions):
        evotor_product_id = item.get("product_id")
        quantity = float(item.get("quantity", 0) or 0)
        base_price = float(item.get("price", 0) or 0)
        base_sum = float(item.get("sum", 0) or (quantity * base_price))

        result_sum_raw = item.get("result_sum", item.get("resultSum"))
        final_sum = float(result_sum_raw) if result_sum_raw is not None else base_sum

        ms_product_id = None
        if store and tenant_id and evotor_product_id:
            ms_product_id = store.get_by_evotor_id(
                tenant_id=tenant_id,
                entity_type="product",
                evotor_id=evotor_product_id,
            )
            if ms_product_id:
                log.info(
                    f"Position[{i}]: mapping found {evotor_product_id} -> {ms_product_id}"
                )
            else:
                raise MappingNotFoundError(
                    f"Mapping not found for product_id={evotor_product_id} "
                    f"name={item.get('product_name')}"
                )

        ms_position = {
            "quantity": quantity,
            "price": round(base_price * 100),
            "sum": round(base_sum * 100),
        }

        discount_percent = _extract_discount_percent(item, base_sum, final_sum)
        if discount_percent is not None and discount_percent > 0:
            ms_position["discount"] = round(discount_percent, 2)

        tax_fields = _extract_vat_fields(item)
        if tax_fields:
            ms_position.update(tax_fields)

        if ms_product_id:
            ms_position["assortment"] = _meta("product", ms_product_id)

        ms_positions.append(ms_position)

    document_sum_raw = body.get("sum")
    if document_sum_raw is not None:
        total_sum = float(document_sum_raw)
    else:
        total_sum = 0.0
        for item in raw_positions:
            result_sum_raw = item.get("result_sum", item.get("resultSum"))
            if result_sum_raw is not None:
                total_sum += float(result_sum_raw)
            else:
                quantity = float(item.get("quantity", 0) or 0)
                price = float(item.get("price", 0) or 0)
                total_sum += float(item.get("sum", 0) or (quantity * price))

    ms_payload = {
        "syncId": effective_sync_id,
        "name": f"Sale {event_id}",
        "description": _build_description(payload, counterparty_resolution_source),
        "positions": ms_positions,
        "sum": round(total_sum * 100),
    }

    if ms_organization_id:
        ms_payload["organization"] = _meta("organization", ms_organization_id)
    if ms_store_id:
        ms_payload["store"] = _meta("store", ms_store_id)
    if ms_agent_id:
        ms_payload["agent"] = _meta("counterparty", ms_agent_id)

    log.info(
        f"Mapped sale payload syncId={effective_sync_id} positions={len(ms_positions)} sum={total_sum}"
    )

    return ms_payload