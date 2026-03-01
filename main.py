import time
import uuid

from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

from db import get_connection

app = FastAPI()

class EvotorWebhook(BaseModel):
    type: str
    event_id: str
    amount: Optional[int] = None

class TenantCreate(BaseModel):
    name: str
    evotor_api_key: str
    moysklad_token: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/tenants")
def create_tenant(body: TenantCreate):
    tenant_id = str(uuid.uuid4())
    now = int(time.time())

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO tenants (id, name, evotor_api_key, moysklad_token, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (tenant_id, body.name, body.evotor_api_key, body.moysklad_token, now))

    conn.commit()
    conn.close()

    return {"id": tenant_id}


@app.get("/tenants")
def list_tenants():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM tenants ORDER BY created_at DESC")
    rows = cursor.fetchall()

    conn.close()
    return [dict(r) for r in rows]

import json
from fastapi import HTTPException, Request

@app.post("/webhooks/evotor/{tenant_id}")
async def evotor_webhook(tenant_id: str, body: EvotorWebhook):
    payload = body.dict()

    event_key = payload.get("event_id") or str(uuid.uuid4())
    event_type = payload.get("type") or "sale"

    now = int(time.time())
    event_id = str(uuid.uuid4())

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM tenants WHERE id = ?", (tenant_id,))
    row = cursor.fetchone()
    if row is None:
        conn.close()
        raise HTTPException(status_code=404, detail="tenant not found")

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
    return {"status": "accepted", "event_id": event_id}