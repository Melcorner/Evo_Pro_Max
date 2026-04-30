import logging
import time
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db import get_connection, adapt_query as aq

router = APIRouter(tags=["Tenants"])
log = logging.getLogger("api.tenants")


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
    alert_email: Optional[str] = None
    alerts_email_enabled: bool
    telegram_chat_id: Optional[str] = None
    alerts_telegram_enabled: bool
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
        aq("""
        INSERT INTO tenants (id, name, evotor_api_key, moysklad_token, created_at)
        VALUES (?, ?, ?, ?, ?)
        """),
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

    cursor.execute(aq("SELECT id FROM tenants WHERE id = ?"), (tenant_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Tenant not found")

    if body.moysklad_token is not None:
        cursor.execute(
            aq("""
            UPDATE tenants
            SET moysklad_token = ?,
                ms_organization_id = ?,
                ms_store_id = ?,
                ms_agent_id = ?,
                evotor_store_id = ?
            WHERE id = ?
            """),
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
            aq("""
            UPDATE tenants
            SET ms_organization_id = ?,
                ms_store_id = ?,
                ms_agent_id = ?,
                evotor_store_id = ?
            WHERE id = ?
            """),
            (
                body.ms_organization_id,
                body.ms_store_id,
                body.ms_agent_id,
                body.evotor_store_id,
                tenant_id,
            ),
        )

    # Upsert в tenant_stores если evotor_store_id задан
    if body.evotor_store_id and body.evotor_store_id.strip():
        import uuid as _uuid_mod, time as _time_mod
        now = int(_time_mod.time())
        cursor.execute(
            aq("""
            INSERT INTO tenant_stores (
                id, tenant_id, evotor_store_id,
                ms_store_id, ms_organization_id, ms_agent_id,
                is_primary, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT (evotor_store_id) DO UPDATE SET
                tenant_id = EXCLUDED.tenant_id,
                ms_store_id = EXCLUDED.ms_store_id,
                ms_organization_id = EXCLUDED.ms_organization_id,
                ms_agent_id = EXCLUDED.ms_agent_id,
                is_primary = 1,
                updated_at = EXCLUDED.updated_at
            """),
            (
                str(_uuid_mod.uuid4()),
                tenant_id,
                body.evotor_store_id.strip(),
                body.ms_store_id or None,
                body.ms_organization_id or None,
                body.ms_agent_id or None,
                now,
                now,
            ),
        )

    conn.commit()
    conn.close()

    return {"status": "ok", "tenant_id": tenant_id}


@router.patch("/tenants/{tenant_id}/fiscal")
def configure_fiscal(tenant_id: str, body: TenantFiscalConfig):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(aq("SELECT id FROM tenants WHERE id = ?"), (tenant_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Tenant not found")

    cursor.execute(
        aq("""
        UPDATE tenants
        SET fiscal_token = ?,
            fiscal_client_uid = ?,
            fiscal_device_uid = ?
        WHERE id = ?
        """),
        (body.fiscal_token, body.fiscal_client_uid, body.fiscal_device_uid, tenant_id),
    )

    conn.commit()
    conn.close()

    return {"status": "ok", "tenant_id": tenant_id}


@router.post("/tenants/{tenant_id}/complete-sync")
def complete_sync(tenant_id: str):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            aq("SELECT id, sync_completed_at, ms_account_id FROM tenants WHERE id = ?"),
            (tenant_id,),
        )
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Tenant not found")

        if row["sync_completed_at"]:
            return {
                "status": "already_completed",
                "sync_completed_at": row["sync_completed_at"],
                "sync_mode": "moysklad",
            }

        now = int(time.time())
        cursor.execute(
            aq("UPDATE tenants SET sync_completed_at = ?, updated_at = ? WHERE id = ?"),
            (now, now, tenant_id),
        )

        # Для legacy tenant-level завершения отмечаем primary store, если он есть.
        cursor.execute(
            aq("UPDATE tenant_stores SET sync_completed_at = ?, updated_at = ? WHERE tenant_id = ? AND is_primary = 1"),
            (now, now, tenant_id),
        )

        conn.commit()
        ms_account_id = row["ms_account_id"]

    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        log.exception("complete_sync failed tenant_id=%s", tenant_id)
        raise
    finally:
        conn.close()

    # Уведомляем МойСклад, что настройка завершена.
    try:
        if ms_account_id:
            from app.api.vendor import _notify_ms_activated
            _notify_ms_activated(ms_account_id)
    except Exception as e:
        log.warning("complete_sync: notify_ms_activated failed err=%s", e)

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

    cursor.execute(aq("SELECT id FROM tenants WHERE id = ?"), (tenant_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Tenant not found")

    cursor.execute(aq("UPDATE tenants SET sync_completed_at = NULL WHERE id = ?"), (tenant_id,))
    cursor.execute(
        aq("UPDATE tenant_stores SET sync_completed_at = NULL, updated_at = ? WHERE tenant_id = ?"),
        (int(time.time()), tenant_id),
    )

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
            alert_email, alerts_email_enabled,
            telegram_chat_id, alerts_telegram_enabled,
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
                "alert_email": r["alert_email"],
                "alerts_email_enabled": bool(r["alerts_email_enabled"]),
                "telegram_chat_id": r["telegram_chat_id"],
                "alerts_telegram_enabled": bool(r["alerts_telegram_enabled"]),
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
    """
    Безопасно удаляет tenant и связанные локальные данные.

    Важно:
    - удаляет только данные нашей БД;
    - не удаляет товары в Эвотор;
    - не удаляет товары в МойСклад;
    - не падает, если необязательной таблицы нет.
    """
    conn = get_connection()
    cur = conn.cursor()

    deleted = {}

    try:
        cur.execute(aq("SELECT id FROM tenants WHERE id = ?"), (tenant_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Tenant not found")

        # Находим все реальные таблицы public, где есть колонка tenant_id.
        # Это безопаснее, чем вручную перечислять sync_locks/action_logs/etc.,
        # потому что часть таблиц может отсутствовать на конкретном стенде.
        cur.execute("""
            SELECT table_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND column_name = 'tenant_id'
            ORDER BY
              CASE WHEN table_name = 'tenant_stores' THEN 2 ELSE 1 END,
              table_name
        """)

        tables = [row["table_name"] for row in cur.fetchall()]

        # Удаляем сначала все дочерние таблицы с tenant_id, кроме tenant_stores.
        # tenant_stores удаляем ближе к концу.
        for table_name in tables:
            if table_name == "tenant_stores":
                continue

            # table_name берётся только из information_schema, но всё равно держим простую защиту.
            if not table_name.replace("_", "").isalnum():
                log.warning("delete_tenant: skipped suspicious table name=%s", table_name)
                continue

            cur.execute(
                aq(f"DELETE FROM {table_name} WHERE tenant_id = ?"),
                (tenant_id,),
            )
            deleted[table_name] = cur.rowcount

        if "tenant_stores" in tables:
            cur.execute(
                aq("DELETE FROM tenant_stores WHERE tenant_id = ?"),
                (tenant_id,),
            )
            deleted["tenant_stores"] = cur.rowcount
        else:
            deleted["tenant_stores"] = 0

        cur.execute(aq("DELETE FROM tenants WHERE id = ?"), (tenant_id,))
        deleted["tenants"] = cur.rowcount

        conn.commit()

        log.warning("Tenant deleted tenant_id=%s deleted=%s", tenant_id, deleted)

        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "deleted": deleted,
        }

    except HTTPException:
        conn.rollback()
        raise

    except Exception as e:
        conn.rollback()
        log.exception("delete_tenant failed tenant_id=%s", tenant_id)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete tenant: {type(e).__name__}: {e}",
        )

    finally:
        conn.close()

