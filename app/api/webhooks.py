import json
import time
import uuid
import logging
from typing import Optional, List, Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.db import get_connection

router = APIRouter()
log = logging.getLogger("api.webhooks")


@router.post("/webhooks/evotor/{tenant_id}")
async def evotor_webhook(tenant_id: str, request: Request):
    try:
        raw_body = await request.json()
    except Exception as e:
        log.error(f"Failed to parse request body: {e}")
        return {"status": "error", "detail": "invalid json"}

    log.info(f"RAW EVOTOR BODY tenant_id={tenant_id} body={json.dumps(raw_body, ensure_ascii=False)}")

    # Событие установки приложения — сохраняем токен клиента
    if "token" in raw_body and "userUuid" in raw_body:
        evotor_token = raw_body.get("token")
        evotor_user_id = raw_body.get("userId") or raw_body.get("userUuid")

        log.info(f"Install event tenant_id={tenant_id} userId={evotor_user_id} token={evotor_token}")

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

    # Определяем event_id
    event_id = (
        raw_body.get("id") or
        raw_body.get("event_id") or
        str(uuid.uuid4())
    )

    # Определяем тип события
    event_type_raw = (
        raw_body.get("type") or
        raw_body.get("event_type") or
        "sale"
    )

    event_type_map = {
        "SELL": "sale",
        "sell": "sale",
        "Receipt": "sale",
        "receipt": "sale",
    }
    event_type = event_type_map.get(event_type_raw, event_type_raw.lower())

    log.info(f"Webhook parsed tenant_id={tenant_id} event_type={event_type} event_key={event_id}")

    if event_type not in ("sale", "product", "stock"):
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
            json.dumps(raw_body, ensure_ascii=False),
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