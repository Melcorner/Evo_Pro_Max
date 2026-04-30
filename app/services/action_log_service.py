import json
import logging
import time
import uuid

from app.db import adapt_query as aq, db_backend, get_connection

log = logging.getLogger("action_log")


def _shorten(value: str | None, limit: int = 500) -> str | None:
    if value is None:
        return None
    normalized = " ".join(str(value).split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3] + "..."


def _ensure_action_log_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS action_log (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            evotor_store_id TEXT,
            action_type     TEXT NOT NULL,
            status          TEXT NOT NULL,
            message         TEXT,
            actor           TEXT,
            source          TEXT,
            metadata_json   TEXT,
            created_at      INTEGER NOT NULL
        )
        """
    )
    if db_backend() == "sqlite":
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_action_log_tenant_created_at "
            "ON action_log(tenant_id, created_at)"
        )
    else:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_action_log_tenant_created_at "
            "ON action_log(tenant_id, created_at)"
        )


def log_action(
    *,
    tenant_id: str,
    action_type: str,
    status: str,
    message: str | None = None,
    evotor_store_id: str | None = None,
    actor: str | None = None,
    source: str | None = None,
    metadata: dict | None = None,
    created_at: int | None = None,
) -> None:
    if not tenant_id:
        return

    conn = get_connection()
    try:
        _ensure_action_log_table(conn)
        cur = conn.cursor()
        cur.execute(
            aq(
                """
                INSERT INTO action_log (
                    id, tenant_id, evotor_store_id, action_type, status,
                    message, actor, source, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            ),
            (
                str(uuid.uuid4()),
                tenant_id,
                evotor_store_id,
                action_type,
                status,
                _shorten(message),
                actor,
                source,
                json.dumps(metadata or {}, ensure_ascii=False) if metadata else None,
                int(time.time()) if created_at is None else int(created_at),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("failed to write action log tenant_id=%s action=%s", tenant_id, action_type)
    finally:
        conn.close()


def list_recent_actions(tenant_id: str, limit: int = 20) -> list[dict]:
    conn = get_connection()
    try:
        _ensure_action_log_table(conn)
        cur = conn.cursor()
        cur.execute(
            aq(
                """
                SELECT id, tenant_id, evotor_store_id, action_type, status,
                       message, actor, source, created_at
                FROM action_log
                WHERE tenant_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """
            ),
            (tenant_id, int(limit)),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
