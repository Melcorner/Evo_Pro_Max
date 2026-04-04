# endpoints/mappings.py

import logging
from typing import Optional, List

from fastapi import APIRouter, Query, HTTPException, Body
from pydantic import BaseModel

from app.db import get_connection
from app.stores.mapping_store import MappingStore

log = logging.getLogger("mappings_endpoint")

router = APIRouter(
    prefix="/mappings",
    tags=["Mappings"]
)

# ==============================================================================
# Pydantic Models
# ==============================================================================

class MappingItem(BaseModel):
    tenant_id: str
    entity_type: str
    evotor_id: str
    ms_id: str
    created_at: int
    updated_at: int

class MappingsResponse(BaseModel):
    items: List[MappingItem]
    total: int
    limit: int
    offset: int

class MappingCreate(BaseModel):
    tenant_id: str
    entity_type: str
    evotor_id: str
    ms_id: str

# ==============================================================================
# MappingStore instance
# ==============================================================================

store = MappingStore()


# ==============================================================================
# GET /mappings
# ==============================================================================

@router.get("", response_model=MappingsResponse)
def list_mappings(
    tenant_id: Optional[str] = Query(None),
    entity_type: Optional[str] = Query(None),
    limit: int = Query(10),
    offset: int = Query(0)
):
    """
    Получить список mappings.

    Можно фильтровать по:
    - tenant_id
    - entity_type
    """

    limit = max(1, min(limit, 100))
    offset = max(0, offset)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        base_query = """
        SELECT
            tenant_id,
            entity_type,
            evotor_id,
            ms_id,
            created_at,
            updated_at
        FROM mappings
        """

        conditions = []
        params = []

        if tenant_id:
            conditions.append("tenant_id = ?")
            params.append(tenant_id)

        if entity_type:
            conditions.append("entity_type = ?")
            params.append(entity_type)

        where_clause = ""
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)

        cursor.execute(
            "SELECT COUNT(*) FROM mappings" + where_clause,
            params
        )
        total = cursor.fetchone()[0]

        query = base_query + where_clause + " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        cursor.execute(query, params + [limit, offset])
        rows = cursor.fetchall()
        items = [dict(r) for r in rows]

        log.info(
            f"List mappings returned={len(items)} total={total} limit={limit} offset={offset}"
        )

        return {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset
        }

    except Exception as e:
        log.error(f"Error listing mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()


# ==============================================================================
# POST /mappings
# ==============================================================================

@router.post("/")
def create_mapping(data: MappingCreate):
    """
    Создать или обновить маппинг Evotor <-> MS.
    """
    ok = store.upsert_mapping(
        data.tenant_id,
        data.entity_type,
        data.evotor_id,
        data.ms_id
    )

    if not ok:
        return {
            "status": "conflict",
            "message": "ms_id already mapped to another evotor_id"
        }

    return {"status": "ok"}


# ==============================================================================
# DELETE /mappings/{tenant_id}/{entity_type}/{evotor_id}
# Удалить один конкретный маппинг
# ==============================================================================

@router.delete(
    "/{tenant_id}/{entity_type}/{evotor_id}",
    summary="Удалить маппинг по evotor_id",
)
def delete_mapping(tenant_id: str, entity_type: str, evotor_id: str):
    """
    Удаляет один маппинг по `tenant_id` + `entity_type` + `evotor_id`.

    Пример:
    ```
    DELETE /mappings/my-tenant/product/evotor-uuid-123
    ```
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM mappings WHERE tenant_id=? AND entity_type=? AND evotor_id=?",
            (tenant_id, entity_type, evotor_id),
        )
        conn.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Маппинг не найден")

        log.info(
            "Mapping deleted tenant_id=%s entity_type=%s evotor_id=%s",
            tenant_id, entity_type, evotor_id,
        )
        return {"status": "ok", "deleted": 1}

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error deleting mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ==============================================================================
# DELETE /mappings/{tenant_id}/{entity_type}
# Удалить все маппинги тенанта по типу сущности
# ==============================================================================

@router.delete(
    "/{tenant_id}/{entity_type}",
    summary="Удалить все маппинги тенанта по типу сущности",
)
def delete_mappings_by_type(tenant_id: str, entity_type: str):
    """
    Удаляет все маппинги для `tenant_id` + `entity_type`.

    Полезно для сброса маппингов товаров перед повторным initial sync.

    Пример:
    ```
    DELETE /mappings/my-tenant/product
    ```
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM mappings WHERE tenant_id=? AND entity_type=?",
            (tenant_id, entity_type),
        )
        conn.commit()
        deleted = cursor.rowcount

        log.info(
            "Mappings deleted tenant_id=%s entity_type=%s count=%s",
            tenant_id, entity_type, deleted,
        )
        return {"status": "ok", "deleted": deleted}

    except Exception as e:
        log.error(f"Error deleting mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ==============================================================================
# DELETE /mappings/{tenant_id}
# Удалить все маппинги тенанта
# ==============================================================================

@router.delete(
    "/{tenant_id}",
    summary="Удалить все маппинги тенанта",
)
def delete_all_tenant_mappings(tenant_id: str):
    """
    Удаляет все маппинги для указанного `tenant_id`.

    Пример:
    ```
    DELETE /mappings/my-tenant
    ```
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM mappings WHERE tenant_id=?",
            (tenant_id,),
        )
        conn.commit()
        deleted = cursor.rowcount

        log.info(
            "All mappings deleted tenant_id=%s count=%s",
            tenant_id, deleted,
        )
        return {"status": "ok", "deleted": deleted}

    except Exception as e:
        log.error(f"Error deleting mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()