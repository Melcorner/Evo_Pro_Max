import logging
from typing import Optional, List

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from app.db import get_connection, adapt_query as aq
from app.stores.mapping_store import MappingStore

log = logging.getLogger("mappings_endpoint")

router = APIRouter(
    prefix="/mappings",
    tags=["Mappings"],
)


# ==============================================================================
# Pydantic Models
# ==============================================================================

class MappingItem(BaseModel):
    tenant_id: str
    evotor_store_id: Optional[str] = None
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
    evotor_store_id: Optional[str] = None


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
    evotor_store_id: Optional[str] = Query(None),
    entity_type: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """
    Получить список mappings.

    Можно фильтровать по:
    - tenant_id
    - evotor_store_id
    - entity_type
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        base_query = """
        SELECT
            tenant_id,
            evotor_store_id,
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

        if evotor_store_id:
            conditions.append("evotor_store_id = ?")
            params.append(evotor_store_id)

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
            "List mappings returned=%s total=%s limit=%s offset=%s tenant_id=%s evotor_store_id=%s entity_type=%s",
            len(items),
            total,
            limit,
            offset,
            tenant_id,
            evotor_store_id,
            entity_type,
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

    Для store-aware схемы evotor_store_id обязателен.
    """
    if not data.evotor_store_id:
        raise HTTPException(status_code=400, detail="evotor_store_id is required for mappings")

    ok = store.upsert_mapping(
        tenant_id=data.tenant_id,
        entity_type=data.entity_type,
        evotor_id=data.evotor_id,
        ms_id=data.ms_id,
        evotor_store_id=data.evotor_store_id,
    )

    if not ok:
        raise HTTPException(
            status_code=409,
            detail="mapping conflict: ms_id already mapped to another evotor_id in this store",
        )

    return {
        "status": "ok",
        "tenant_id": data.tenant_id,
        "evotor_store_id": data.evotor_store_id,
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
def delete_mapping(
    tenant_id: str,
    entity_type: str,
    evotor_id: str,
    evotor_store_id: Optional[str] = Query(None),
):
    """
    Удаляет один маппинг.

    Если evotor_store_id передан — удаляет только mapping конкретного магазина.
    Если evotor_store_id не передан — сохраняется legacy-поведение и удаляются
    mappings этого tenant/entity_type/evotor_id во всех магазинах.
    """
    conn = get_connection()

    try:
        cursor = conn.cursor()

        if evotor_store_id:
            cursor.execute(
                aq("""
                DELETE FROM mappings
                WHERE tenant_id = ?
                  AND evotor_store_id = ?
                  AND entity_type = ?
                  AND evotor_id = ?
                """),
                (tenant_id, evotor_store_id, entity_type, evotor_id),
            )
        else:
            cursor.execute(
                aq("""
                DELETE FROM mappings
                WHERE tenant_id = ?
                  AND entity_type = ?
                  AND evotor_id = ?
                """),
                (tenant_id, entity_type, evotor_id),
            )

        conn.commit()

        deleted = cursor.rowcount or 0

        if deleted == 0:
            raise HTTPException(status_code=404, detail="Маппинг не найден")

        log.info(
            "Mapping deleted tenant_id=%s evotor_store_id=%s entity_type=%s evotor_id=%s count=%s",
            tenant_id,
            evotor_store_id,
            entity_type,
            evotor_id,
            deleted,
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
def delete_mappings_by_type(
    tenant_id: str,
    entity_type: str,
    evotor_store_id: Optional[str] = Query(None),
):
    """
    Удаляет mappings для tenant_id + entity_type.

    Если evotor_store_id передан — удаляет mappings только конкретного магазина.
    Если evotor_store_id не передан — сохраняется legacy-поведение и удаляются
    mappings этого типа по всему tenant.
    """
    conn = get_connection()

    try:
        cursor = conn.cursor()

        if evotor_store_id:
            cursor.execute(
                aq("""
                DELETE FROM mappings
                WHERE tenant_id = ?
                  AND evotor_store_id = ?
                  AND entity_type = ?
                """),
                (tenant_id, evotor_store_id, entity_type),
            )
        else:
            cursor.execute(
                aq("""
                DELETE FROM mappings
                WHERE tenant_id = ?
                  AND entity_type = ?
                """),
                (tenant_id, entity_type),
            )

        conn.commit()

        deleted = cursor.rowcount or 0

        log.info(
            "Mappings deleted tenant_id=%s evotor_store_id=%s entity_type=%s count=%s",
            tenant_id,
            evotor_store_id,
            entity_type,
            deleted,
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
def delete_all_tenant_mappings(
    tenant_id: str,
    evotor_store_id: Optional[str] = Query(None),
):
    """
    Удаляет mappings tenant.

    Если evotor_store_id передан — удаляет mappings только конкретного магазина.
    Если evotor_store_id не передан — сохраняется legacy-поведение и удаляются
    все mappings tenant.
    """
    conn = get_connection()

    try:
        cursor = conn.cursor()

        if evotor_store_id:
            cursor.execute(
                aq("""
                DELETE FROM mappings
                WHERE tenant_id = ?
                  AND evotor_store_id = ?
                """),
                (tenant_id, evotor_store_id),
            )
        else:
            cursor.execute(
                aq("""
                DELETE FROM mappings
                WHERE tenant_id = ?
                """),
                (tenant_id,),
            )

        conn.commit()

        deleted = cursor.rowcount or 0

        log.info(
            "All mappings deleted tenant_id=%s evotor_store_id=%s count=%s",
            tenant_id,
            evotor_store_id,
            deleted,
        )

        return {"status": "ok", "deleted": deleted}

    except Exception as e:
        log.error("Error deleting mappings: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()