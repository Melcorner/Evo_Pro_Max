import time
import uuid
import logging

from app.logger import setup_logging
from app.handlers.sale_handler import handle_sale
from app.services.error_logic import classify_error, RETRY, FAILED
from app.stores.error_store import insert_error
from app.services.event_dispatcher import dispatch_event

setup_logging()
log = logging.getLogger("worker")

from app.db import get_connection


def process_one_event():
    conn = get_connection()
    cursor = conn.cursor()

    now = int(time.time())

    cursor.execute("""
        SELECT * FROM event_store
        WHERE (status = 'NEW')
           OR (status = 'RETRY' AND next_retry_at IS NOT NULL AND next_retry_at <= ? AND retries < 5)
        ORDER BY created_at
        LIMIT 1
    """, (now,))
    row = cursor.fetchone()

    if not row:
        conn.close()
        return False

    event_id = row["id"]
    log.info(f"Picked event_id={event_id} status={row['status']} retries={row['retries']} event_key={row['event_key']}")

    # пытаемся захватить событие (optimistic locking)
    cursor.execute("""
        UPDATE event_store
        SET status = 'PROCESSING', updated_at = ?
        WHERE id = ? AND status IN ('NEW','RETRY')
    """, (int(time.time()), event_id))

    if cursor.rowcount == 0:
        conn.close()
        return True  # другой воркер уже взял событие

    conn.commit()
    log.info(f"Locked event_id={event_id} -> PROCESSING")

    try:
        log.info(f"Processing event_id={event_id} event_type={row['event_type']}")

        result_ref = dispatch_event(row)

        now = int(time.time())

        # помечаем DONE
        cursor.execute("""
            UPDATE event_store
            SET status = 'DONE', updated_at = ?
            WHERE id = ?
        """, (now, event_id))

        # фиксируем идемпотентность
        cursor.execute("""
            INSERT OR IGNORE INTO processed_events (tenant_id, event_key, result_ref, processed_at)
            VALUES (?, ?, ?, ?)
        """, (
            row["tenant_id"],
            row["event_key"],
            result_ref,
            now,
        ))

        conn.commit()
        log.info(f"DONE event_id={event_id} event_key={row['event_key']}")
        return True

    except Exception as e:
        now = int(time.time())
        decision = classify_error(e)
        new_retries = row["retries"] + 1
        err = str(e)

        error_code = None
        if hasattr(e, "response") and e.response is not None:
            error_code = str(getattr(e.response, "status_code", ""))
        elif hasattr(e, "status_code"):
            error_code = str(getattr(e, "status_code", ""))

        response_body = None
        if hasattr(e, "response") and e.response is not None:
            try:
                response_body = e.response.text
            except Exception:
                response_body = None

        log.warning(
            "Handler error event_id=%s event_key=%s tenant_id=%s event_type=%s retries=%s decision=%s err=%s",
            event_id,
            row["event_key"],
            row["tenant_id"],
            row["event_type"],
            new_retries,
            decision,
            err,
        )

        # decision == RETRY и исчерпаны попытки → тоже FAILED
        delay = 60 * (2 ** (new_retries - 1))  # 60,120,240,480,960
        go_failed = decision == FAILED or new_retries >= 5

        if go_failed:
            cursor.execute("""
                UPDATE event_store
                SET status = 'FAILED',
                    retries = ?,
                    next_retry_at = NULL,
                    last_error_message = ?,
                    updated_at = ?
                WHERE id = ?
            """, (new_retries, err, now, event_id))
        else:
            cursor.execute("""
                UPDATE event_store
                SET status = 'RETRY',
                    retries = ?,
                    next_retry_at = ?,
                    last_error_message = ?,
                    updated_at = ?
                WHERE id = ?
            """, (new_retries, now + delay, err, now, event_id))

        insert_error(conn, row, error_code, err, response_body)
        conn.commit()

        if go_failed:
            log.error(
                "FAILED event_id=%s event_key=%s retries=%s err=%s",
                event_id, row["event_key"], new_retries, err,
            )
        else:
            log.warning(
                "RETRY event_id=%s event_key=%s retries=%s next_retry_at=%s err=%s",
                event_id, row["event_key"], new_retries, now + delay, err,
            )

        return True

    finally:
        conn.close()

def main_loop():
    print("Worker started")
    while True:
        processed = process_one_event()
        if not processed:
            time.sleep(2)


if __name__ == "__main__":
    main_loop()