import secrets
import time
import uuid

from app.db import adapt_query as aq


def expire_telegram_link_tokens(conn, now_ts: int | None = None) -> int:
    now = int(time.time()) if now_ts is None else int(now_ts)
    cursor = conn.cursor()
    cursor.execute(
        aq("""
        UPDATE telegram_link_tokens
        SET status = 'expired'
        WHERE status = 'pending' AND expires_at <= ?
        """),
        (now,),
    )
    return cursor.rowcount


def get_active_telegram_link_token(conn, tenant_id: str, now_ts: int | None = None):
    expire_telegram_link_tokens(conn, now_ts=now_ts)
    cursor = conn.cursor()
    cursor.execute(
        aq("""
        SELECT *
        FROM telegram_link_tokens
        WHERE tenant_id = ? AND status = 'pending'
        ORDER BY created_at DESC
        LIMIT 1
        """),
        (tenant_id,),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


def create_telegram_link_token(
    conn,
    *,
    tenant_id: str,
    ttl_sec: int,
    now_ts: int | None = None,
):
    now = int(time.time()) if now_ts is None else int(now_ts)
    expires_at = now + int(ttl_sec)
    token_id = str(uuid.uuid4())
    link_token = secrets.token_urlsafe(24)
    cursor = conn.cursor()

    expire_telegram_link_tokens(conn, now_ts=now)
    cursor.execute(
        aq("""
        UPDATE telegram_link_tokens
        SET status = 'expired'
        WHERE tenant_id = ? AND status = 'pending'
        """),
        (tenant_id,),
    )
    cursor.execute(
        aq("""
        INSERT INTO telegram_link_tokens (
            id,
            tenant_id,
            link_token,
            status,
            created_at,
            expires_at,
            linked_chat_id,
            linked_at
        ) VALUES (?, ?, ?, 'pending', ?, ?, NULL, NULL)
        """),
        (token_id, tenant_id, link_token, now, expires_at),
    )

    return {
        "id": token_id,
        "tenant_id": tenant_id,
        "link_token": link_token,
        "status": "pending",
        "created_at": now,
        "expires_at": expires_at,
        "linked_chat_id": None,
        "linked_at": None,
    }


def get_telegram_link_token_by_value(conn, link_token: str, now_ts: int | None = None):
    expire_telegram_link_tokens(conn, now_ts=now_ts)
    cursor = conn.cursor()
    cursor.execute(
        aq("""
        SELECT *
        FROM telegram_link_tokens
        WHERE link_token = ?
        LIMIT 1
        """),
        (link_token,),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


def mark_telegram_link_token_linked(
    conn,
    *,
    token_id: str,
    linked_chat_id: str,
    linked_at: int | None = None,
) -> None:
    ts = int(time.time()) if linked_at is None else int(linked_at)
    cursor = conn.cursor()
    cursor.execute(
        aq("""
        UPDATE telegram_link_tokens
        SET status = 'linked',
            linked_chat_id = ?,
            linked_at = ?
        WHERE id = ?
        """),
        (str(linked_chat_id), ts, token_id),
    )
