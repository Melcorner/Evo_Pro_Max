import json
import logging
import time
import uuid

from app.db import adapt_query as aq, get_connection

log = logging.getLogger("sync_control")

DEFAULT_SYNC_LOCK_TTL_SEC = 60 * 60


class SyncLockBusy(Exception):
    def __init__(self, lock: dict | None = None):
        super().__init__("Synchronization is already running")
        self.lock = lock or {}


def _ensure_tables(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_locks (
            tenant_id       TEXT NOT NULL,
            evotor_store_id TEXT NOT NULL,
            action_type     TEXT NOT NULL,
            locked_at       INTEGER NOT NULL,
            expires_at      INTEGER NOT NULL,
            owner           TEXT,
            PRIMARY KEY (tenant_id, evotor_store_id, action_type)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_snapshots (
            id                  TEXT PRIMARY KEY,
            tenant_id           TEXT NOT NULL,
            evotor_store_id     TEXT,
            action_type         TEXT NOT NULL,
            mappings_count      INTEGER NOT NULL DEFAULT 0,
            products_count      INTEGER,
            stock_items_count   INTEGER,
            actor               TEXT,
            source              TEXT,
            status              TEXT NOT NULL,
            message             TEXT,
            metadata_json       TEXT,
            created_at          INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_sync_snapshots_tenant_created_at "
        "ON sync_snapshots(tenant_id, created_at)"
    )


def acquire_sync_lock(
    *,
    tenant_id: str,
    evotor_store_id: str | None,
    action_type: str,
    ttl_sec: int = DEFAULT_SYNC_LOCK_TTL_SEC,
    owner: str | None = None,
) -> dict:
    store_id = evotor_store_id or "all"
    now = int(time.time())
    expires_at = now + int(ttl_sec)
    lock_id = owner or str(uuid.uuid4())

    conn = get_connection()
    try:
        _ensure_tables(conn)
        cur = conn.cursor()
        cur.execute(
            aq(
                """
                DELETE FROM sync_locks
                WHERE tenant_id = ?
                  AND evotor_store_id = ?
                  AND action_type = ?
                  AND expires_at <= ?
                """
            ),
            (tenant_id, store_id, action_type, now),
        )
        try:
            cur.execute(
                aq(
                    """
                    INSERT INTO sync_locks (
                        tenant_id, evotor_store_id, action_type,
                        locked_at, expires_at, owner
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """
                ),
                (tenant_id, store_id, action_type, now, expires_at, lock_id),
            )
            conn.commit()
            return {
                "acquired": True,
                "tenant_id": tenant_id,
                "evotor_store_id": store_id,
                "action_type": action_type,
                "locked_at": now,
                "expires_at": expires_at,
                "owner": lock_id,
            }
        except Exception:
            conn.rollback()
            cur = conn.cursor()
            cur.execute(
                aq(
                    """
                    SELECT tenant_id, evotor_store_id, action_type,
                           locked_at, expires_at, owner
                    FROM sync_locks
                    WHERE tenant_id = ?
                      AND evotor_store_id = ?
                      AND action_type = ?
                    """
                ),
                (tenant_id, store_id, action_type),
            )
            row = cur.fetchone()
            existing = dict(row) if row else {}
            existing["acquired"] = False
            return existing
    finally:
        conn.close()


def release_sync_lock(*, tenant_id: str, evotor_store_id: str | None, action_type: str) -> None:
    store_id = evotor_store_id or "all"
    conn = get_connection()
    try:
        _ensure_tables(conn)
        cur = conn.cursor()
        cur.execute(
            aq(
                """
                DELETE FROM sync_locks
                WHERE tenant_id = ?
                  AND evotor_store_id = ?
                  AND action_type = ?
                """
            ),
            (tenant_id, store_id, action_type),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("failed to release sync lock tenant_id=%s store=%s action=%s", tenant_id, store_id, action_type)
    finally:
        conn.close()


def create_sync_snapshot(
    *,
    tenant_id: str,
    evotor_store_id: str | None,
    action_type: str,
    actor: str | None = None,
    source: str | None = None,
    message: str | None = None,
    metadata: dict | None = None,
) -> str | None:
    store_id = evotor_store_id or "all"
    now = int(time.time())
    conn = get_connection()
    try:
        _ensure_tables(conn)
        cur = conn.cursor()

        if store_id == "all":
            cur.execute(
                aq(
                    "SELECT COUNT(*) AS cnt FROM mappings "
                    "WHERE tenant_id = ? AND entity_type = 'product'"
                ),
                (tenant_id,),
            )
        else:
            cur.execute(
                aq(
                    "SELECT COUNT(*) AS cnt FROM mappings "
                    "WHERE tenant_id = ? AND evotor_store_id = ? AND entity_type = 'product'"
                ),
                (tenant_id, store_id),
            )
        mappings_row = cur.fetchone()
        mappings_count = int(mappings_row["cnt"] or 0) if mappings_row else 0

        cur.execute(
            aq(
                """
                SELECT synced_items_count, total_items_count
                FROM stock_sync_status
                WHERE tenant_id = ?
                """
            ),
            (tenant_id,),
        )
        stock_row = cur.fetchone()
        stock_items_count = int(stock_row["total_items_count"] or 0) if stock_row else None

        snapshot_id = str(uuid.uuid4())
        cur.execute(
            aq(
                """
                INSERT INTO sync_snapshots (
                    id, tenant_id, evotor_store_id, action_type,
                    mappings_count, products_count, stock_items_count,
                    actor, source, status, message, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            ),
            (
                snapshot_id,
                tenant_id,
                store_id,
                action_type,
                mappings_count,
                mappings_count,
                stock_items_count,
                actor,
                source,
                "created",
                message or "Snapshot before sync",
                json.dumps(metadata or {}, ensure_ascii=False) if metadata else None,
                now,
            ),
        )
        conn.commit()
        return snapshot_id
    except Exception:
        conn.rollback()
        log.exception("failed to create sync snapshot tenant_id=%s store=%s action=%s", tenant_id, store_id, action_type)
        return None
    finally:
        conn.close()
