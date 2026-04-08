import time
import uuid

from app.db import adapt_query as aq


def insert_notification_log(
    conn,
    *,
    tenant_id=None,
    channel_type: str,
    destination: str,
    event_type: str,
    message: str,
    status: str,
    error_message: str | None = None,
    created_at: int | None = None,
    sent_at: int | None = None,
):
    cursor = conn.cursor()
    cursor.execute(
        aq("""
        INSERT INTO notification_log (
            id,
            tenant_id,
            channel_type,
            destination,
            event_type,
            message,
            status,
            error_message,
            created_at,
            sent_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """),
        (
            str(uuid.uuid4()),
            tenant_id,
            channel_type,
            destination,
            event_type,
            message,
            status,
            error_message,
            int(time.time()) if created_at is None else int(created_at),
            sent_at,
        ),
    )


def list_notification_log(conn, tenant_id: str | None = None, limit: int = 50, offset: int = 0):
    cursor = conn.cursor()

    if tenant_id:
        cursor.execute(
            aq("""
            SELECT *
            FROM notification_log
            WHERE tenant_id = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """),
            (tenant_id, limit, offset),
        )
    else:
        cursor.execute(
            aq("""
            SELECT *
            FROM notification_log
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """),
            (limit, offset),
        )

    return [dict(r) for r in cursor.fetchall()]
