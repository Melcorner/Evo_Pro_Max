# endpoints/mappings.py

import logging
from typing import Optional, List

from fastapi import APIRouter, Query, HTTPException, Body
from pydantic import BaseModel

from db import get_connection
from mapping_store import MappingStore

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
    
class MappingDelete(BaseModel):
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

        # считаем total
        cursor.execute(
            "SELECT COUNT(*) FROM mappings" + where_clause,
            params
        )
        total = cursor.fetchone()[0]

        # получаем данные
        query = base_query + where_clause + " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        cursor.execute(
            query,
            params + [limit, offset]
        )
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

    return {
        "status": "ok"
    }

@router.delete("/", summary="Удалить маппинг по tenant/entity/evotor_id")
def delete_mapping(data: MappingDelete = Body(...)):
    """
    Удаляет маппинг Evotor ID <-> MS ID.
    """

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            DELETE FROM mappings
            WHERE tenant_id=? AND entity_type=? AND evotor_id=?
            """,
            (data.tenant_id, data.entity_type, data.evotor_id)
        )

        conn.commit()

        if cursor.rowcount == 0:
            return {"status": "not_found", "message": "Mapping не найден"}

        return {"status": "ok", "message": "Mapping удалён"}

    except Exception as e:
        log.error(f"Error deleting mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()