import json
import time
import uuid
import logging
from typing import Optional, List, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db import get_connection

router = APIRouter()
log = logging.getLogger("api.webhooks")


class EvotorWebhook(BaseModel):
    type: str
    event_id: str
    amount: Optional[int] = None
    positions: Optional[List[Any]] = None

    class Config:
        extra = "allow"


@router.post("/webhooks/evotor/{tenant_id}")
async def evotor_webhook(tenant_id: str, body: EvotorWebhook):
    log.info(f"Webhook received tenant_id={tenant_id}")

    payload = body.dict()

    event_key = payload.get("event_id") or str(uuid.uuid4())
    event_type = payload.get("type") or "sale"

    log.info(f"Webhook parsed tenant_id={tenant_id} event_type={event_type} event_key={event_key}")

    now = int(time.time())
    event_id = str(uuid.uuid4())

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
    """, (tenant_id, event_key))

    if cursor.fetchone() is not None:
        conn.close()
        log.info(f"Already processed tenant_id={tenant_id} event_key={event_key}")
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
            event_id,
            tenant_id,
            event_type,
            event_key,
            json.dumps(payload),
            now,
            now
        ))
        conn.commit()
    except Exception as e:
        conn.close()
        return {"status": "duplicate_or_error", "detail": str(e)}

    conn.close()

    log.info(f"Event stored NEW event_id={event_id} tenant_id={tenant_id} event_key={event_key}")

    return {"status": "accepted", "event_id": event_id}