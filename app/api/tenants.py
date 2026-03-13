import time
import uuid

from fastapi import APIRouter
from pydantic import BaseModel

from app.db import get_connection

router = APIRouter()


class TenantCreate(BaseModel):
    name: str
    evotor_api_key: str
    moysklad_token: str


@router.post("/tenants")
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


@router.get("/tenants")
def list_tenants():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM tenants ORDER BY created_at DESC")
    rows = cursor.fetchall()

    conn.close()
    return [dict(r) for r in rows]