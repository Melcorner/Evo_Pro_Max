"""
app/api/stores.py

CRUD для магазинов тенанта (tenant_stores).
Позволяет управлять несколькими магазинами Эвотор на одном тенанте.
"""
import time
import uuid
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from app.db import get_connection, adapt_query as aq

router = APIRouter(tags=["Stores"])
log = logging.getLogger("api.stores")


class StoreCreate(BaseModel):
    evotor_store_id: str
    name: Optional[str] = None
    ms_store_id: Optional[str] = None
    ms_organization_id: Optional[str] = None
    ms_agent_id: Optional[str] = None
    is_primary: bool = False


class StoreUpdate(BaseModel):
    name: Optional[str] = None
    ms_store_id: Optional[str] = None
    ms_organization_id: Optional[str] = None
    ms_agent_id: Optional[str] = None
    is_primary: Optional[bool] = None


class StorePublic(BaseModel):
    id: str
    tenant_id: str
    evotor_store_id: str
    name: Optional[str] = None
    ms_store_id: Optional[str] = None
    ms_organization_id: Optional[str] = None
    ms_agent_id: Optional[str] = None
    is_primary: bool
    sync_completed_at: Optional[int] = None
    created_at: int


def _require_tenant(tenant_id: str, conn=None):
    close = conn is None
    if conn is None:
        conn = get_connection()
    cur = conn.cursor()
    cur.execute(aq("SELECT id FROM tenants WHERE id = ?"), (tenant_id,))
    row = cur.fetchone()
    if close:
        conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Tenant not found")


@router.get("/tenants/{tenant_id}/stores", response_model=list[StorePublic])
def list_stores(tenant_id: str):
    """Список всех магазинов тенанта."""
    conn = get_connection()
    cur = conn.cursor()
    _require_tenant(tenant_id, conn)
    cur.execute(
        aq("""
        SELECT id, tenant_id, evotor_store_id, name,
               ms_store_id, ms_organization_id, ms_agent_id,
               is_primary, sync_completed_at, created_at
        FROM tenant_stores
        WHERE tenant_id = ?
        ORDER BY is_primary DESC, created_at ASC
        """),
        (tenant_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.post("/tenants/{tenant_id}/stores", response_model=StorePublic)
def add_store(tenant_id: str, body: StoreCreate):
    """Добавить магазин к тенанту."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        _require_tenant(tenant_id, conn)

        # Проверяем что evotor_store_id не занят другим тенантом
        cur.execute(
            aq("SELECT tenant_id FROM tenant_stores WHERE evotor_store_id = ?"),
            (body.evotor_store_id,),
        )
        existing = cur.fetchone()
        if existing and existing["tenant_id"] != tenant_id:
            raise HTTPException(
                status_code=409,
                detail=f"Store {body.evotor_store_id} already belongs to another tenant",
            )

        now = int(time.time())
        store_id = str(uuid.uuid4())

        # Если is_primary=True — снимаем флаг с остальных
        if body.is_primary:
            cur.execute(
                aq("UPDATE tenant_stores SET is_primary = 0 WHERE tenant_id = ?"),
                (tenant_id,),
            )

        # Если это первый магазин — делаем его primary автоматически
        cur.execute(
            aq("SELECT COUNT(*) as cnt FROM tenant_stores WHERE tenant_id = ?"),
            (tenant_id,),
        )
        count = cur.fetchone()["cnt"]
        is_primary = 1 if (body.is_primary or count == 0) else 0

        cur.execute(
            aq("""
            INSERT INTO tenant_stores
                (id, tenant_id, evotor_store_id, name, ms_store_id,
                 ms_organization_id, ms_agent_id, is_primary, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (evotor_store_id) DO UPDATE SET
                tenant_id = EXCLUDED.tenant_id,
                name = COALESCE(EXCLUDED.name, tenant_stores.name),
                ms_store_id = COALESCE(EXCLUDED.ms_store_id, tenant_stores.ms_store_id),
                ms_organization_id = COALESCE(EXCLUDED.ms_organization_id, tenant_stores.ms_organization_id),
                ms_agent_id = COALESCE(EXCLUDED.ms_agent_id, tenant_stores.ms_agent_id),
                is_primary = EXCLUDED.is_primary,
                updated_at = EXCLUDED.updated_at
            """),
            (
                store_id, tenant_id, body.evotor_store_id,
                body.name, body.ms_store_id, body.ms_organization_id,
                body.ms_agent_id, is_primary, now, now,
            ),
        )
        conn.commit()

        cur.execute(
            aq("SELECT * FROM tenant_stores WHERE evotor_store_id = ?"),
            (body.evotor_store_id,),
        )
        row = cur.fetchone()
        return dict(row)
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        log.exception("add_store failed tenant_id=%s", tenant_id)
        raise
    finally:
        conn.close()


@router.patch("/tenants/{tenant_id}/stores/{evotor_store_id}", response_model=StorePublic)
def update_store(tenant_id: str, evotor_store_id: str, body: StoreUpdate):
    """Обновить настройки магазина."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("SELECT * FROM tenant_stores WHERE tenant_id = ? AND evotor_store_id = ?"),
            (tenant_id, evotor_store_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Store not found")

        now = int(time.time())
        fields = []
        values = []

        if body.name is not None:
            fields.append("name = ?"); values.append(body.name)
        if body.ms_store_id is not None:
            fields.append("ms_store_id = ?"); values.append(body.ms_store_id)
        if body.ms_organization_id is not None:
            fields.append("ms_organization_id = ?"); values.append(body.ms_organization_id)
        if body.ms_agent_id is not None:
            fields.append("ms_agent_id = ?"); values.append(body.ms_agent_id)
        if body.is_primary is not None:
            if body.is_primary:
                cur.execute(
                    aq("UPDATE tenant_stores SET is_primary = 0 WHERE tenant_id = ?"),
                    (tenant_id,),
                )
            fields.append("is_primary = ?"); values.append(1 if body.is_primary else 0)

        if not fields:
            return dict(row)

        fields.append("updated_at = ?"); values.append(now)
        values.extend([tenant_id, evotor_store_id])

        cur.execute(
            aq(f"UPDATE tenant_stores SET {', '.join(fields)} WHERE tenant_id = ? AND evotor_store_id = ?"),
            values,
        )
        conn.commit()

        cur.execute(
            aq("SELECT * FROM tenant_stores WHERE tenant_id = ? AND evotor_store_id = ?"),
            (tenant_id, evotor_store_id),
        )
        return dict(cur.fetchone())
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        log.exception("update_store failed tenant_id=%s store=%s", tenant_id, evotor_store_id)
        raise
    finally:
        conn.close()


@router.delete("/tenants/{tenant_id}/stores/{evotor_store_id}")
def delete_store(tenant_id: str, evotor_store_id: str):
    """Удалить магазин из тенанта."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("SELECT * FROM tenant_stores WHERE tenant_id = ? AND evotor_store_id = ?"),
            (tenant_id, evotor_store_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Store not found")

        if row["is_primary"]:
            # Проверяем есть ли другие магазины
            cur.execute(
                aq("SELECT COUNT(*) as cnt FROM tenant_stores WHERE tenant_id = ? AND evotor_store_id != ?"),
                (tenant_id, evotor_store_id),
            )
            if cur.fetchone()["cnt"] > 0:
                raise HTTPException(
                    status_code=409,
                    detail="Cannot delete primary store. Set another store as primary first.",
                )

        cur.execute(
            aq("DELETE FROM tenant_stores WHERE tenant_id = ? AND evotor_store_id = ?"),
            (tenant_id, evotor_store_id),
        )
        conn.commit()
        return {"status": "ok", "deleted": evotor_store_id}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@router.post("/tenants/{tenant_id}/stores/{evotor_store_id}/set-primary")
def set_primary_store(tenant_id: str, evotor_store_id: str):
    """Назначить магазин основным."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("SELECT id FROM tenant_stores WHERE tenant_id = ? AND evotor_store_id = ?"),
            (tenant_id, evotor_store_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Store not found")

        cur.execute(
            aq("UPDATE tenant_stores SET is_primary = 0 WHERE tenant_id = ?"),
            (tenant_id,),
        )
        cur.execute(
            aq("UPDATE tenant_stores SET is_primary = 1, updated_at = ? WHERE tenant_id = ? AND evotor_store_id = ?"),
            (int(time.time()), tenant_id, evotor_store_id),
        )
        conn.commit()
        return {"status": "ok", "primary_store": evotor_store_id}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@router.post("/tenants/{tenant_id}/stores/{evotor_store_id}/complete-sync")
def complete_store_sync(tenant_id: str, evotor_store_id: str):
    """Отметить первичную синхронизацию магазина как завершённую."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("SELECT * FROM tenant_stores WHERE tenant_id = ? AND evotor_store_id = ?"),
            (tenant_id, evotor_store_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Store not found")

        now = int(time.time())
        cur.execute(
            aq("UPDATE tenant_stores SET sync_completed_at = ?, updated_at = ? WHERE tenant_id = ? AND evotor_store_id = ?"),
            (now, now, tenant_id, evotor_store_id),
        )

        # Если это primary магазин — синхронизируем и tenants.sync_completed_at
        if row["is_primary"]:
            cur.execute(
                aq("UPDATE tenants SET sync_completed_at = ? WHERE id = ?"),
                (now, tenant_id),
            )

        conn.commit()
        return {"status": "ok", "sync_completed_at": now}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
