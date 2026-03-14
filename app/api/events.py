import time
import logging

from fastapi import APIRouter, HTTPException
from app.db import get_connection

log = logging.getLogger("api")
router = APIRouter()


@router.get("/events")
def list_events():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM event_store ORDER BY created_at DESC LIMIT 100")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


@router.get("/events/retry")
def list_retry_events():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM event_store WHERE status = 'RETRY' ORDER BY created_at DESC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


@router.get("/events/failed")
def list_failed_events():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM event_store WHERE status = 'FAILED' ORDER BY created_at DESC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


@router.post("/events/{event_id}/requeue")
def requeue_event(event_id: str):
    """
    Переводит FAILED событие обратно в NEW для повторной обработки.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM event_store WHERE id = ?", (event_id,))
    row = cur.fetchone()

    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Event not found")

    if row["status"] != "FAILED":
        conn.close()
        raise HTTPException(
            status_code=409,
            detail=f"Cannot requeue event with status={row['status']}. Only FAILED events can be requeued."
        )

    now = int(time.time())

    cur.execute("""
        UPDATE event_store
        SET status = 'NEW',
            retries = 0,
            next_retry_at = NULL,
            last_error_message = NULL,
            updated_at = ?
        WHERE id = ?
    """, (now, event_id))

    conn.commit()
    conn.close()

    log.info(f"Requeued event_id={event_id} -> NEW")

    return {"event_id": event_id, "status": "NEW", "message": "Event requeued successfully"}