import time
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db import get_connection

router = APIRouter()


class TenantCreate(BaseModel):
    name: str
    evotor_api_key: str
    moysklad_token: str


class TenantMoySkladConfig(BaseModel):
    ms_organization_id: str
    ms_store_id: str
    ms_agent_id: str
    evotor_store_id: Optional[str] = None


class TenantPublic(BaseModel):
    id: str
    name: str
    created_at: int
    evotor_user_id: Optional[str] = None
    evotor_store_id: Optional[str] = None
    ms_organization_id: Optional[str] = None
    ms_store_id: Optional[str] = None
    ms_agent_id: Optional[str] = None
    sync_completed_at: Optional[int] = None
    sync_mode: str  # "evotor" | "moysklad" | "not_configured"
    has_evotor_api_key: bool
    has_moysklad_token: bool
    has_evotor_token: bool


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


@router.patch("/tenants/{tenant_id}/moysklad")
def configure_moysklad(tenant_id: str, body: TenantMoySkladConfig):
    """
    Сохраняет конфигурацию МойСклад для tenant.
    Опционально принимает evotor_store_id — нужен для синхронизации товаров.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM tenants WHERE id = ?", (tenant_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Tenant not found")

    cursor.execute("""
        UPDATE tenants
        SET ms_organization_id = ?,
            ms_store_id = ?,
            ms_agent_id = ?,
            evotor_store_id = ?
        WHERE id = ?
    """, (
        body.ms_organization_id,
        body.ms_store_id,
        body.ms_agent_id,
        body.evotor_store_id,
        tenant_id
    ))

    conn.commit()
    conn.close()

    return {"status": "ok", "tenant_id": tenant_id}


@router.post("/tenants/{tenant_id}/complete-sync")
def complete_sync(tenant_id: str):
    """
    Отмечает первичную синхронизацию как завершённую.
    После этого система работает в режиме МойСклад → Эвотор.

    Вызывается автоматически после POST /sync/{tenant_id}/initial
    или вручную если синхронизация была сделана другим способом.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, sync_completed_at FROM tenants WHERE id = ?", (tenant_id,))
    row = cursor.fetchone()

    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Tenant not found")

    if row["sync_completed_at"]:
        conn.close()
        return {
            "status": "already_completed",
            "sync_completed_at": row["sync_completed_at"],
            "sync_mode": "moysklad"
        }

    now = int(time.time())
    cursor.execute("""
        UPDATE tenants SET sync_completed_at = ? WHERE id = ?
    """, (now, tenant_id))

    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "sync_completed_at": now,
        "sync_mode": "moysklad",
        "message": "Первичная синхронизация завершена. Режим: МойСклад → Эвотор"
    }


@router.delete("/tenants/{tenant_id}/complete-sync")
def reset_sync(tenant_id: str):
    """
    Сбрасывает флаг синхронизации — возвращает tenant в режим первичной синхронизации.
    Используется если нужно повторно запустить синхронизацию.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM tenants WHERE id = ?", (tenant_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Tenant not found")

    cursor.execute("""
        UPDATE tenants SET sync_completed_at = NULL WHERE id = ?
    """, (tenant_id,))

    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "sync_mode": "evotor",
        "message": "Флаг синхронизации сброшен. Доступна первичная синхронизация."
    }


@router.get("/tenants", response_model=list[TenantPublic])
def list_tenants():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            id, name, created_at,
            evotor_user_id, evotor_store_id,
            ms_organization_id, ms_store_id, ms_agent_id,
            sync_completed_at,
            evotor_api_key, moysklad_token, evotor_token
        FROM tenants
        ORDER BY created_at DESC
    """)
    rows = cursor.fetchall()
    conn.close()

    result = []
    for r in rows:
        # Определяем режим работы
        if r["sync_completed_at"]:
            sync_mode = "moysklad"   # рабочий режим — МойСклад мастер
        elif r["ms_organization_id"]:
            sync_mode = "evotor"     # готов к первичной синхронизации
        else:
            sync_mode = "not_configured"  # не настроен

        result.append({
            "id": r["id"],
            "name": r["name"],
            "created_at": r["created_at"],
            "evotor_user_id": r["evotor_user_id"],
            "evotor_store_id": r["evotor_store_id"],
            "ms_organization_id": r["ms_organization_id"],
            "ms_store_id": r["ms_store_id"],
            "ms_agent_id": r["ms_agent_id"],
            "sync_completed_at": r["sync_completed_at"],
            "sync_mode": sync_mode,
            "has_evotor_api_key": bool(r["evotor_api_key"]),
            "has_moysklad_token": bool(r["moysklad_token"]),
            "has_evotor_token": bool(r["evotor_token"]),
        })

    return result