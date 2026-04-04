import time
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db import get_connection

router = APIRouter(tags=["Tenants"])


class TenantCreate(BaseModel):
    name: str
    evotor_api_key: str = ""
    moysklad_token: str


class TenantMoySkladConfig(BaseModel):
    moysklad_token: Optional[str] = None
    ms_organization_id: str
    ms_store_id: str
    ms_agent_id: str
    evotor_store_id: Optional[str] = None


class TenantFiscalConfig(BaseModel):
    fiscal_token: str
    fiscal_client_uid: str
    fiscal_device_uid: str

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
    sync_mode: str
    has_evotor_api_key: bool
    has_moysklad_token: bool
    has_evotor_token: bool
    has_fiscal_config: bool


@router.post("/tenants")
def create_tenant(body: TenantCreate):
    tenant_id = str(uuid.uuid4())
    now = int(time.time())

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO tenants (id, name, evotor_api_key, moysklad_token, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            tenant_id,
            body.name,
            body.evotor_api_key or "",
            body.moysklad_token,
            now,
        ),
    )
    conn.commit()
    conn.close()

    return {"id": tenant_id}

@router.patch("/tenants/{tenant_id}/moysklad")
def configure_moysklad(tenant_id: str, body: TenantMoySkladConfig):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM tenants WHERE id = ?", (tenant_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Tenant not found")

    if body.moysklad_token is not None:
        cursor.execute(
            """
            UPDATE tenants
            SET moysklad_token = ?,
                ms_organization_id = ?,
                ms_store_id = ?,
                ms_agent_id = ?,
                evotor_store_id = ?
            WHERE id = ?
            """,
            (
                body.moysklad_token,
                body.ms_organization_id,
                body.ms_store_id,
                body.ms_agent_id,
                body.evotor_store_id,
                tenant_id,
            ),
        )
    else:
        cursor.execute(
            """
            UPDATE tenants
            SET ms_organization_id = ?,
                ms_store_id = ?,
                ms_agent_id = ?,
                evotor_store_id = ?
            WHERE id = ?
            """,
            (
                body.ms_organization_id,
                body.ms_store_id,
                body.ms_agent_id,
                body.evotor_store_id,
                tenant_id,
            ),
        )

    conn.commit()
    conn.close()

    return {"status": "ok", "tenant_id": tenant_id}


@router.patch("/tenants/{tenant_id}/fiscal")
def configure_fiscal(tenant_id: str, body: TenantFiscalConfig):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM tenants WHERE id = ?", (tenant_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Tenant not found")

    cursor.execute(
        """
        UPDATE tenants
        SET fiscal_token = ?,
            fiscal_client_uid = ?,
            fiscal_device_uid = ?
        WHERE id = ?
        """,
        (body.fiscal_token, body.fiscal_client_uid, body.fiscal_device_uid, tenant_id),
    )

    conn.commit()
    conn.close()

    return {"status": "ok", "tenant_id": tenant_id}


@router.post("/tenants/{tenant_id}/complete-sync")
def complete_sync(tenant_id: str):
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
            "sync_mode": "moysklad",
        }

    now = int(time.time())
    cursor.execute("UPDATE tenants SET sync_completed_at = ? WHERE id = ?", (now, tenant_id))

    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "sync_completed_at": now,
        "sync_mode": "moysklad",
        "message": "Первичная синхронизация завершена. Режим: МойСклад → Эвотор",
    }


@router.delete("/tenants/{tenant_id}/complete-sync")
def reset_sync(tenant_id: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM tenants WHERE id = ?", (tenant_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Tenant not found")

    cursor.execute("UPDATE tenants SET sync_completed_at = NULL WHERE id = ?", (tenant_id,))

    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "sync_mode": "evotor",
        "message": "Флаг синхронизации сброшен. Доступна первичная синхронизация.",
    }


@router.get("/tenants", response_model=list[TenantPublic])
def list_tenants():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            id, name, created_at,
            evotor_user_id, evotor_store_id,
            ms_organization_id, ms_store_id, ms_agent_id,
            sync_completed_at,
            evotor_api_key, moysklad_token, evotor_token,
            fiscal_token, fiscal_client_uid, fiscal_device_uid
        FROM tenants
        ORDER BY created_at DESC
        """
    )
    rows = cursor.fetchall()
    conn.close()

    result = []
    for r in rows:
        if r["sync_completed_at"]:
            sync_mode = "moysklad"
        elif r["ms_organization_id"]:
            sync_mode = "evotor"
        else:
            sync_mode = "not_configured"

        has_fiscal_config = bool(
            r["fiscal_token"] and r["fiscal_client_uid"] and r["fiscal_device_uid"]
        )

        result.append(
            {
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
                "has_fiscal_config": has_fiscal_config,
            }
        )

    return result
@router.delete("/tenants/{tenant_id}")
def delete_tenant(tenant_id: str):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT id, name FROM tenants WHERE id = ?", (tenant_id,))
    tenant = cur.fetchone()
    if not tenant:
        conn.close()
        raise HTTPException(status_code=404, detail="Tenant not found")

    try:
        cur.execute("BEGIN")

        cur.execute("DELETE FROM mappings WHERE tenant_id = ?", (tenant_id,))
        deleted_mappings = cur.rowcount

        cur.execute("DELETE FROM errors WHERE tenant_id = ?", (tenant_id,))
        deleted_errors = cur.rowcount

        cur.execute("DELETE FROM stock_sync_status WHERE tenant_id = ?", (tenant_id,))
        deleted_stock_sync_status = cur.rowcount

        cur.execute("DELETE FROM fiscalization_checks WHERE tenant_id = ?", (tenant_id,))
        deleted_fiscalization_checks = cur.rowcount

        cur.execute("DELETE FROM processed_events WHERE tenant_id = ?", (tenant_id,))
        deleted_processed_events = cur.rowcount

        cur.execute("DELETE FROM event_store WHERE tenant_id = ?", (tenant_id,))
        deleted_event_store = cur.rowcount

        cur.execute("DELETE FROM tenants WHERE id = ?", (tenant_id,))
        deleted_tenants = cur.rowcount

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "status": "ok",
        "deleted": {
            "tenant_id": tenant_id,
            "tenant_name": tenant["name"],
            "tenants": deleted_tenants,
            "mappings": deleted_mappings,
            "errors": deleted_errors,
            "stock_sync_status": deleted_stock_sync_status,
            "fiscalization_checks": deleted_fiscalization_checks,
            "processed_events": deleted_processed_events,
            "event_store": deleted_event_store,
        }
    }