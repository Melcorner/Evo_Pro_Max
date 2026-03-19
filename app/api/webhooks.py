import json
import time
import uuid
import logging
from typing import Optional, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db import get_connection

router = APIRouter()
log = logging.getLogger("api.webhooks")


class EvotorPosition(BaseModel):
    product_id: str
    product_name: Optional[str] = None
    quantity: float
    price: float
    sum: Optional[float] = None

    class Config:
        extra = "allow"


class EvotorBody(BaseModel):
    positions: List[EvotorPosition]
    sum: Optional[float] = None

    class Config:
        extra = "allow"


class EvotorWebhook(BaseModel):
    type: str
    id: str
    store_id: Optional[str] = None
    device_id: Optional[str] = None
    body: Optional[EvotorBody] = None

    class Config:
        extra = "allow"


LIKELY_CUSTOMER_KEYS = (
    "buyer",
    "customer",
    "client",
    "contractor",
    "customerInfo",
    "buyerInfo",
    "customer_info",
    "buyer_info",
    "buyerRequisites",
    "buyer_requisites",
    "paymentCustomer",
    "payment_customer",
)


def _pick_first_direct(d: dict, aliases: list[str]):
    if not isinstance(d, dict):
        return None

    aliases_lower = {alias.lower() for alias in aliases}
    for key, value in d.items():
        if value in (None, "", [], {}):
            continue
        if str(key).lower() in aliases_lower:
            return value
    return None


def _safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_customer(candidate_root: dict | None) -> dict | None:
    """
    Пытается вытащить данные покупателя из наиболее вероятных полей ReceiptCreated.
    Работает мягко: если поля нет, пайплайн продажи не ломается.
    """
    if not isinstance(candidate_root, dict):
        return None

    candidate_dicts: list[dict] = []

    for key in LIKELY_CUSTOMER_KEYS:
        value = candidate_root.get(key)
        if isinstance(value, dict):
            candidate_dicts.append(value)

    candidate_dicts.append(candidate_root)

    for candidate in candidate_dicts:
        name = _pick_first_direct(candidate, [
            "name", "fullName", "fio", "customerName", "buyerName", "clientName"
        ])
        phone = _pick_first_direct(candidate, [
            "phone", "phoneNumber", "customerPhone", "buyerPhone", "clientPhone", "tel", "telephone", "mobilePhone"
        ])
        email = _pick_first_direct(candidate, [
            "email", "eMail", "mail", "customerEmail", "buyerEmail", "clientEmail"
        ])
        inn = _pick_first_direct(candidate, [
            "inn", "customerInn", "buyerInn", "clientInn", "payerInn"
        ])

        if any(v not in (None, "") for v in (name, phone, email, inn)):
            return {
                "name": str(name).strip() if name not in (None, "") else None,
                "phone": str(phone).strip() if phone not in (None, "") else None,
                "email": str(email).strip() if email not in (None, "") else None,
                "inn": str(inn).strip() if inn not in (None, "") else None,
                "raw": candidate,
            }

    return None


def _normalize_receipt_created(body_dict: dict) -> tuple[str, str, dict] | tuple[None, None, None]:
    """
    Преобразует Evotor ReceiptCreated во внутренний формат события.

    Поддерживает несколько реальных вариантов скидок:
    1) resultPrice / resultSum
    2) positionDiscount / docDistributedDiscount
    3) простое поле discount в item + totalDiscount на уровне документа
    """
    data = body_dict.get("data") or {}
    receipt_doc_type = data.get("type")

    if receipt_doc_type not in ("SELL", "sell"):
        return None, None, None

    positions = []
    computed_total_sum = 0.0
    has_any_result_sum = False

    for item in data.get("items", []):
        quantity = _safe_float(item.get("quantity")) or 0.0
        price = _safe_float(item.get("price")) or 0.0
        line_sum = _safe_float(item.get("sumPrice"))
        if line_sum is None:
            line_sum = quantity * price

        result_price = _safe_float(item.get("resultPrice", item.get("result_price")))
        result_sum = _safe_float(item.get("resultSum", item.get("result_sum")))

        # Реальный payload Эвотор может присылать только абсолютную скидку по позиции:
        # item.discount = 0.1, без resultSum/resultPrice.
        raw_discount = _safe_float(item.get("discount"))
        if result_sum is None and raw_discount is not None:
            result_sum = max(0.0, line_sum - raw_discount)

        if result_price is None and result_sum is not None and quantity > 0:
            result_price = result_sum / quantity

        if result_sum is not None:
            computed_total_sum += result_sum
            has_any_result_sum = True
        else:
            computed_total_sum += line_sum

        positions.append({
            "product_id": item.get("id"),
            "product_name": item.get("name"),
            "quantity": quantity,
            "price": price,
            "sum": line_sum,
            "result_price": result_price,
            "result_sum": result_sum,
            "position_discount": item.get("positionDiscount", item.get("position_discount")),
            "doc_distributed_discount": item.get("docDistributedDiscount", item.get("doc_distributed_discount")),
            "discount": raw_discount,
            "tax": item.get("tax"),
            "tax_percent": item.get("taxPercent", item.get("tax_percent")),
            "raw": item,
        })

    # Для итоговой суммы документа приоритет такой:
    # 1) сумма пересчитанных result_sum по позициям
    # 2) totalAmount из webhook
    document_sum = computed_total_sum if positions else None
    if not has_any_result_sum:
        total_amount = _safe_float(data.get("totalAmount"))
        if total_amount is not None:
            document_sum = total_amount

    normalized_payload = {
        "id": body_dict.get("id") or data.get("id") or str(uuid.uuid4()),
        "type": receipt_doc_type,
        "store_id": body_dict.get("store_id") or data.get("storeId"),
        "device_id": body_dict.get("device_id") or data.get("deviceId"),
        "customer": _extract_customer(data),
        "body": {
            "positions": positions,
            "sum": document_sum,
            "doc_discounts": data.get("docDiscounts", data.get("doc_discounts", [])) or [],
            "total_discount": _safe_float(data.get("totalDiscount")),
            "total_tax": _safe_float(data.get("totalTax")),
        },
        "source_event_type": body_dict.get("type"),
        "source_data": data,
    }

    event_type = "sale"
    event_key = normalized_payload["id"]
    return event_type, event_key, normalized_payload


@router.post("/webhooks/evotor/{tenant_id}")
async def evotor_webhook(tenant_id: str, raw_body: EvotorWebhook):
    body_dict = raw_body.dict()

    log.info(f"RAW EVOTOR BODY tenant_id={tenant_id} body={json.dumps(body_dict, ensure_ascii=False)}")

    # Событие установки приложения — сохраняем токен клиента
    if "token" in body_dict and "userUuid" in body_dict:
        evotor_token = body_dict.get("token")
        evotor_user_id = body_dict.get("userId") or body_dict.get("userUuid")

        log.info(f"Install event tenant_id={tenant_id} userId={evotor_user_id} token_exists={bool(evotor_token)}")

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE tenants
            SET evotor_user_id = ?, evotor_token = ?
            WHERE id = ?
            """,
            (evotor_user_id, evotor_token, tenant_id),
        )
        conn.commit()
        conn.close()

        log.info(f"Evotor token saved tenant_id={tenant_id}")
        return {"status": "accepted"}

    event_type_raw = body_dict.get("type") or "sale"

    if event_type_raw == "ReceiptCreated":
        event_type, event_id, normalized_payload = _normalize_receipt_created(body_dict)

        if not event_type:
            log.warning(f"Unsupported ReceiptCreated subtype tenant_id={tenant_id} raw_type={event_type_raw}")
            return {"status": "skipped", "reason": "unsupported receipt subtype"}

        payload_to_store = normalized_payload
    else:
        event_id = body_dict.get("id") or str(uuid.uuid4())
        event_type_map = {
            "SELL": "sale",
            "sell": "sale",
            "Receipt": "sale",
            "receipt": "sale",
        }
        event_type = event_type_map.get(event_type_raw, event_type_raw.lower())
        payload_to_store = body_dict

    log.info(f"Webhook parsed tenant_id={tenant_id} event_type={event_type} event_key={event_id}")

    if event_type not in ("sale", "product"):
        log.warning(f"Unknown event_type={event_type} raw={event_type_raw} — skipping")
        return {"status": "skipped", "reason": f"unknown event_type: {event_type_raw}"}

    now = int(time.time())
    event_store_id = str(uuid.uuid4())

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM tenants WHERE id = ?", (tenant_id,))
    row = cursor.fetchone()

    if row is None:
        conn.close()
        raise HTTPException(status_code=404, detail="tenant not found")

    cursor.execute(
        """
        SELECT 1 FROM processed_events
        WHERE tenant_id = ? AND event_key = ?
        """,
        (tenant_id, event_id),
    )

    if cursor.fetchone() is not None:
        conn.close()
        log.info(f"Already processed tenant_id={tenant_id} event_key={event_id}")
        return {"status": "already_processed"}

    try:
        cursor.execute(
            """
            INSERT INTO event_store (
                id, tenant_id, event_type, event_key, payload_json,
                status, retries, next_retry_at,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'NEW', 0, NULL, ?, ?)
            """,
            (
                event_store_id,
                tenant_id,
                event_type,
                event_id,
                json.dumps(payload_to_store, ensure_ascii=False),
                now,
                now,
            ),
        )
        conn.commit()
    except Exception as e:
        conn.close()
        return {"status": "duplicate_or_error", "detail": str(e)}

    conn.close()

    log.info(f"Event stored NEW event_id={event_store_id} tenant_id={tenant_id} event_key={event_id}")

    return {"status": "accepted", "event_id": event_store_id}
