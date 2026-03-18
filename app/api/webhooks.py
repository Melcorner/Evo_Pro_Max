import json
import time
import uuid
import logging
from typing import Optional, List, Any, Dict

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


def _normalize_receipt_created(body_dict: dict) -> tuple[str, str, dict] | tuple[None, None, None]:
    """
    Преобразует Evotor ReceiptCreated во внутренний формат события.
    Возвращает:
      (event_type, event_key, normalized_payload)
    либо (None, None, None), если тип пока не поддерживаем.
    """
    data = body_dict.get("data") or {}
    receipt_doc_type = data.get("type")

    # Пока поддерживаем только чек продажи
    if receipt_doc_type not in ("SELL", "sell"):
        return None, None, None

    positions = []
    for item in data.get("items", []):
        quantity = item.get("quantity", 0)
        price = item.get("price", 0)
        line_sum = item.get("sumPrice")
        if line_sum is None:
            line_sum = quantity * price

        positions.append({
            "product_id": item.get("id"),
            "product_name": item.get("name"),
            "quantity": quantity,
            "price": price,
            "sum": line_sum,
        })

    normalized_payload = {
        # Для payload продажи оставляем id документа/чека Эвотора
        "id": body_dict.get("id") or data.get("id") or str(uuid.uuid4()),
        "type": receipt_doc_type,
        "store_id": body_dict.get("store_id") or data.get("storeId"),
        "device_id": body_dict.get("device_id") or data.get("deviceId"),
        "body": {
            "positions": positions,
            "sum": data.get("totalAmount"),
        },
        # Сырые данные можно оставить для отладки
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
        cursor.execute("""
            UPDATE tenants
            SET evotor_user_id = ?, evotor_token = ?
            WHERE id = ?
        """, (evotor_user_id, evotor_token, tenant_id))
        conn.commit()
        conn.close()

        log.info(f"Evotor token saved tenant_id={tenant_id}")
        return {"status": "accepted"}

    event_type_raw = body_dict.get("type") or "sale"

    # Спец-обработка нового формата Evotor
    if event_type_raw == "ReceiptCreated":
        event_type, event_id, normalized_payload = _normalize_receipt_created(body_dict)

        if not event_type:
            log.warning(f"Unsupported ReceiptCreated subtype tenant_id={tenant_id} raw_type={event_type_raw}")
            return {"status": "skipped", "reason": "unsupported receipt subtype"}

        payload_to_store = normalized_payload
    else:
        # Старый формат / уже нормализованные события
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

    cursor.execute("""
        SELECT 1 FROM processed_events
        WHERE tenant_id = ? AND event_key = ?
    """, (tenant_id, event_id))

    if cursor.fetchone() is not None:
        conn.close()
        log.info(f"Already processed tenant_id={tenant_id} event_key={event_id}")
        return {"status": "already_processed"}

    try:
        cursor.execute("""
            INSERT INTO event_store (
                id, tenant_id, event_type, event_key, payload_json,
                status, retries, next_retry_at,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'NEW', 0, NULL, ?, ?)
        """, (
            event_store_id,
            tenant_id,
            event_type,
            event_id,
            json.dumps(payload_to_store, ensure_ascii=False),
            now,
            now
        ))
        conn.commit()
    except Exception as e:
        conn.close()
        return {"status": "duplicate_or_error", "detail": str(e)}

    conn.close()

    log.info(f"Event stored NEW event_id={event_store_id} tenant_id={tenant_id} event_key={event_id}")

    return {"status": "accepted", "event_id": event_store_id}