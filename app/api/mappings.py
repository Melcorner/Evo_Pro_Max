import logging
from typing import Optional, List

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from app.db import get_connection, adapt_query as aq
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
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """
    Получить список mappings.

    Можно фильтровать по:
    - tenant_id
    - entity_type
    """
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
        params: list = []

        if tenant_id:
            conditions.append("tenant_id = ?")
            params.append(tenant_id)

        if entity_type:
            conditions.append("entity_type = ?")
            params.append(entity_type)

        where_clause = ""
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)

        count_sql = aq(f"SELECT COUNT(*) AS cnt FROM mappings{where_clause}")
        cursor.execute(count_sql, tuple(params))
        total_row = cursor.fetchone()
        total = int(total_row["cnt"]) if total_row else 0

        query_sql = aq(base_query + where_clause + " ORDER BY created_at DESC LIMIT ? OFFSET ?")
        cursor.execute(query_sql, tuple(params + [limit, offset]))
        rows = cursor.fetchall()
        items = [dict(r) for r in rows]

        log.info(
            "List mappings returned=%s total=%s limit=%s offset=%s tenant_id=%s entity_type=%s",
            len(items), total, limit, offset, tenant_id, entity_type,
        )

        return {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    except Exception as e:
        log.error("Error listing mappings: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ==============================================================================
# POST /mappings
# ==============================================================================

@router.post("/", status_code=201)
def create_mapping(data: MappingCreate):
    """
    Создать или обновить маппинг Evotor <-> MS.
    """
    ok = store.upsert_mapping(
        data.tenant_id,
        data.entity_type,
        data.evotor_id,
        data.ms_id,
    )

    if not ok:
        raise HTTPException(
            status_code=409,
            detail="ms_id already mapped to another evotor_id",
        )

    return {
        "status": "ok",
        "tenant_id": data.tenant_id,
        "entity_type": data.entity_type,
        "evotor_id": data.evotor_id,
        "ms_id": data.ms_id,
    }


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
    DELETE /mappings/my-tenant/product/evotor-uuid-123
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            aq("DELETE FROM mappings WHERE tenant_id = ? AND entity_type = ? AND evotor_id = ?"),
            (tenant_id, entity_type, evotor_id),
        )
        conn.commit()

        deleted = cursor.rowcount or 0
        if deleted == 0:
            raise HTTPException(status_code=404, detail="Маппинг не найден")

        log.info(
            "Mapping deleted tenant_id=%s entity_type=%s evotor_id=%s",
            tenant_id, entity_type, evotor_id,
        )
        return {"status": "ok", "deleted": deleted}

    except HTTPException:
        raise
    except Exception as e:
        log.error("Error deleting mapping: %s", e)
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
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            aq("DELETE FROM mappings WHERE tenant_id = ? AND entity_type = ?"),
            (tenant_id, entity_type),
        )
        conn.commit()

        deleted = cursor.rowcount or 0

        log.info(
            "Mappings deleted tenant_id=%s entity_type=%s count=%s",
            tenant_id, entity_type, deleted,
        )
        return {"status": "ok", "deleted": deleted}

    except Exception as e:
        log.error("Error deleting mappings: %s", e)
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
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            aq("DELETE FROM mappings WHERE tenant_id = ?"),
            (tenant_id,),
        )
        conn.commit()

        deleted = cursor.rowcount or 0

        log.info(
            "All mappings deleted tenant_id=%s count=%s",
            tenant_id, deleted,
        )
        return {"status": "ok", "deleted": deleted}

    except Exception as e:
        log.error("Error deleting mappings: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()