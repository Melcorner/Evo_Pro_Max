import logging
from datetime import datetime, timezone

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
        doc_type = str(payload.get("type") or "").strip().upper()
        if doc_type != "SELL":
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
    1) Реальный ReceiptCreated: taxPercent = 0 / 10 / 20 / 22
    2) Легаси-формат: tax = {type: NO_VAT/VAT_10/VAT_20}
    """
    raw_tax_percent = _to_float(item.get("tax_percent", item.get("taxPercent")))

    if raw_tax_percent is not None:
        rounded = int(round(raw_tax_percent))

        if rounded == 0:
            return {"vat": 0, "vatEnabled": False}

        if rounded in (10, 20, 22):
            return {"vat": rounded, "vatEnabled": True}

        log.warning("Unsupported Evotor taxPercent=%s", raw_tax_percent)
        return {}

    tax = item.get("tax")

    if isinstance(tax, dict):
        tax_type = tax.get("type")

        if not tax_type:
            return {}

        result = LEGACY_VAT_MAP.get(tax_type)

        if result is None:
            log.warning("Unsupported Evotor tax.type=%s", tax_type)
            return {}

        return result

    return {}


def _extract_evotor_store_id(payload: dict, explicit_store_id: str | None = None) -> str | None:
    """
    Определяет магазин Эвотор для store-aware поиска mappings.

    Основной нормализованный payload содержит store_id.
    На всякий случай поддерживаем storeId, evotor_store_id и source_data.storeId.
    """
    if explicit_store_id:
        return explicit_store_id

    store_id = (
        payload.get("store_id")
        or payload.get("storeId")
        or payload.get("evotor_store_id")
    )

    if store_id:
        return str(store_id)

    source_data = payload.get("source_data") or {}
    if isinstance(source_data, dict):
        source_store_id = source_data.get("storeId") or source_data.get("store_id")
        if source_store_id:
            return str(source_store_id)

    return None


def map_sale_to_ms(
    payload: dict,
    sync_id: str = None,
    tenant_id: str = None,
    ms_organization_id: str = None,
    ms_store_id: str = None,
    ms_agent_id: str = None,
    counterparty_resolution_source: str | None = None,
    evotor_store_id: str | None = None,
) -> dict:
    log.info("Mapping sale payload")

    validate_sale_payload(payload)

    event_id = payload.get("id")
    body = payload.get("body", {}) or {}
    effective_sync_id = sync_id or event_id
    raw_positions = body.get("positions", [])
    effective_evotor_store_id = _extract_evotor_store_id(payload, evotor_store_id)

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
                evotor_store_id=effective_evotor_store_id,
            )

            if ms_product_id:
                log.info(
                    "Position[%s]: mapping found store=%s %s -> %s",
                    i,
                    effective_evotor_store_id,
                    evotor_product_id,
                    ms_product_id,
                )
            else:
                raise MappingNotFoundError(
                    f"Mapping not found for product_id={evotor_product_id} "
                    f"store_id={effective_evotor_store_id} "
                    f"name={item.get('product_name')}"
                )

        ms_position = {
            "quantity": quantity,
            "price": round(base_price * 100),
            # "sum" убран — вычисляемое поле в МойСклад, отправка вызывает 400
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
        "externalCode": effective_sync_id,  # для идемпотентности (find_demand_by_external_code)
        "name": f"Sale {event_id}",
        "description": _build_description(payload, counterparty_resolution_source),
        "positions": ms_positions,
        # "sum" убран — вычисляемое поле в МойСклад
    }

    if ms_organization_id:
        ms_payload["organization"] = _meta("organization", ms_organization_id)

    if ms_store_id:
        ms_payload["store"] = _meta("store", ms_store_id)

    if ms_agent_id:
        ms_payload["agent"] = _meta("counterparty", ms_agent_id)

    log.info(
        "Mapped sale payload syncId=%s store=%s positions=%s sum=%s",
        effective_sync_id,
        effective_evotor_store_id,
        len(ms_positions),
        total_sum,
    )

    return ms_payload


def _now_ms_moment() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _extract_document_total(payload: dict) -> float:
    body = payload.get("body", {}) or {}

    if body.get("sum") is not None:
        return float(body.get("sum") or 0)

    total = 0.0
    for item in body.get("positions", []) or []:
        result_sum_raw = item.get("result_sum", item.get("resultSum"))
        if result_sum_raw is not None:
            total += float(result_sum_raw or 0)
            continue

        quantity = float(item.get("quantity", 0) or 0)
        price = float(item.get("price", 0) or 0)
        total += float(item.get("sum", 0) or (quantity * price))

    return total


def _extract_payment_sums(payload: dict, total_sum: float) -> tuple[int, int]:
    """
    Возвращает суммы оплаты для retaildemand в копейках:
    - cashSum — наличные;
    - noCashSum — безналичная оплата.

    Поддерживаем два режима:
    1. Если в payload есть явная разбивка payments/payment/paymentsList — считаем по ней.
    2. Если разбивки нет, используем paymentSource из исходного чека Эвотор.

    Для неизвестного способа оплаты временно используем cashSum, чтобы не потерять продажу.
    """
    total_sum = float(total_sum or 0)

    def _norm(value) -> str:
        return str(value or "").strip().upper()

    def _amount_from_payment(payment: dict) -> float:
        for key in ("sum", "amount", "value", "total", "paidSum", "paymentSum"):
            if payment.get(key) is not None:
                try:
                    return float(payment.get(key) or 0)
                except (TypeError, ValueError):
                    return 0.0
        return 0.0

    def _payment_type(payment: dict) -> str:
        for key in ("type", "paymentType", "payment_source", "paymentSource", "source", "kind"):
            value = payment.get(key)
            if value:
                return _norm(value)
        return ""

    def _is_cash(value: str) -> bool:
        return value in {
            "CASH",
            "PAY_CASH",
            "CASH_PAYMENT",
            "нал",
            "НАЛ",
            "НАЛИЧНЫЕ",
        }

    def _is_cashless(value: str) -> bool:
        return value in {
            "CARD",
            "PAY_CARD",
            "BANK_CARD",
            "ELECTRONIC",
            "CASHLESS",
            "NO_CASH",
            "NON_CASH",
            "SBP",
            "SBERBANK",
            "QR",
            "безнал",
            "БЕЗНАЛ",
            "БЕЗНАЛИЧНЫЕ",
        }

    source_data = payload.get("source_data") or {}
    body = payload.get("body") or {}

    # 1. Явная разбивка оплат, если она появится в реальных чеках.
    payment_lists = []
    for container in (payload, body, source_data):
        for key in ("payments", "payment", "paymentsList", "paymentList"):
            value = container.get(key) if isinstance(container, dict) else None
            if isinstance(value, list):
                payment_lists.extend(value)
            elif isinstance(value, dict):
                payment_lists.append(value)

    if payment_lists:
        cash_sum = 0.0
        no_cash_sum = 0.0
        unknown_sum = 0.0

        for payment in payment_lists:
            if not isinstance(payment, dict):
                continue

            payment_type = _payment_type(payment)
            amount = _amount_from_payment(payment)

            if _is_cash(payment_type):
                cash_sum += amount
            elif _is_cashless(payment_type):
                no_cash_sum += amount
            else:
                unknown_sum += amount

        # Неизвестную часть не теряем.
        cash_sum += unknown_sum

        # Если список оплат был, но суммы в нём не распознаны — fallback по paymentSource.
        if cash_sum > 0 or no_cash_sum > 0:
            return round(cash_sum * 100), round(no_cash_sum * 100)

    # 2. Основной сценарий Эвотор: paymentSource в source_data.
    payment_source = _norm(
        source_data.get("paymentSource")
        or payload.get("paymentSource")
        or body.get("paymentSource")
        or source_data.get("payment_source")
        or payload.get("payment_source")
        or body.get("payment_source")
    )

    if _is_cashless(payment_source):
        return 0, round(total_sum * 100)

    if _is_cash(payment_source):
        return round(total_sum * 100), 0

    # 3. Fallback: продажу не теряем, но оставляем в наличных.
    log.warning(
        "Unknown paymentSource for retaildemand; fallback to cashSum paymentSource=%s total_sum=%s",
        payment_source,
        total_sum,
    )
    return round(total_sum * 100), 0


def map_sale_to_ms_retail_demand(
    payload: dict,
    sync_id: str = None,
    tenant_id: str = None,
    ms_organization_id: str = None,
    ms_store_id: str = None,
    ms_agent_id: str = None,
    ms_retail_store_id: str = None,
    ms_retail_shift_id: str = None,
    ms_cashier_id: str = None,
    counterparty_resolution_source: str | None = None,
    evotor_store_id: str | None = None,
) -> dict:
    """
    Маппинг кассового чека Эвотор в Розничную продажу МойСклад /entity/retaildemand.

    Старый map_sale_to_ms оставлен для режима demand.
    """
    base = map_sale_to_ms(
        payload=payload,
        sync_id=sync_id,
        tenant_id=tenant_id,
        ms_organization_id=ms_organization_id,
        ms_store_id=ms_store_id,
        ms_agent_id=ms_agent_id,
        counterparty_resolution_source=counterparty_resolution_source,
        evotor_store_id=evotor_store_id,
    )

    if not ms_retail_store_id:
        raise SalePayloadError("ms_retail_store_id is required for retaildemand mode")

    total_sum = _extract_document_total(payload)
    cash_sum, no_cash_sum = _extract_payment_sums(payload, total_sum)

    retail_payload = {
        # Для retaildemand не передаём syncId: МойСклад ожидает UUID.
        # Для нашей идемпотентности используем externalCode.
        "externalCode": base.get("externalCode"),
        "name": base.get("name"),
        "description": base.get("description"),
        "moment": _now_ms_moment(),
        "positions": base.get("positions", []),
        "retailStore": _meta("retailstore", ms_retail_store_id),
        "cashSum": cash_sum,
        "noCashSum": no_cash_sum,
    }

    if base.get("organization"):
        retail_payload["organization"] = base["organization"]

    if base.get("store"):
        retail_payload["store"] = base["store"]

    if base.get("agent"):
        retail_payload["agent"] = base["agent"]

    if ms_cashier_id:
        retail_payload["cashier"] = _meta("employee", ms_cashier_id)

    if ms_retail_shift_id:
        retail_payload["retailShift"] = _meta("retailshift", ms_retail_shift_id)

    log.info(
        "Mapped retaildemand payload syncId=%s store=%s positions=%s cashSum=%s noCashSum=%s retailStore=%s shift=%s",
        retail_payload.get("syncId"),
        evotor_store_id,
        len(retail_payload.get("positions", [])),
        cash_sum,
        no_cash_sum,
        ms_retail_store_id,
        bool(ms_retail_shift_id),
    )

    return retail_payload

