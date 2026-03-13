from fastapi import APIRouter

from app.db import get_connection

router = APIRouter()


@router.get("/events")
def list_events():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, tenant_id, event_type, event_key, status, retries, next_retry_at, created_at, updated_at
        FROM event_store
        ORDER BY created_at DESC
        LIMIT 50
    """)
    rows = cursor.fetchall()

    conn.close()
    return [dict(r) for r in rows]


@router.get("/processed")
def list_processed():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT tenant_id, event_key, result_ref, processed_at
        FROM processed_events
        ORDER BY processed_at DESC
        LIMIT 50
    """)
    rows = cursor.fetchall()
    conn.close()

    return [dict(r) for r in rows]


@router.get("/events/retry")
def list_retry():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, tenant_id, event_key, status, retries, next_retry_at, last_error_message
        FROM event_store
        WHERE status IN ('RETRY','FAILED')
        ORDER BY updated_at DESC
        LIMIT 50
    """)
    rows = cursor.fetchall()
    conn.close()

    return [dict(r) for r in rows]