import time
import uuid
import logging

from logger import setup_logging
from sale_handler import handle_sale

setup_logging()
log = logging.getLogger("worker")

from db import get_connection


def process_one_event():
    conn = get_connection()
    cursor = conn.cursor()

    now = int(time.time()) 

    cursor.execute("""
        SELECT * FROM event_store
        WHERE (status = 'NEW') OR (status = 'RETRY' AND next_retry_at IS NOT NULL AND next_retry_at <= ? AND retries < 5)
        ORDER BY created_at
        LIMIT 1
    """, (now,))
    row = cursor.fetchone()

    if not row:
        conn.close()
        return False

    event_id = row["id"]
    log.info(f"Picked event_id={event_id} status={row['status']} retries={row['retries']} event_key={row['event_key']}")

    # пытаемся захватить (важно для будущих параллельных воркеров)
    cursor.execute("""
        UPDATE event_store
        SET status = 'PROCESSING', updated_at = ?
        WHERE id = ? AND status IN('NEW','RETRY')
    """, (int(time.time()), event_id))

    if cursor.rowcount == 0:
        conn.close()
        return True  # кто-то уже взял

    conn.commit()
    log.info(f"Locked event_id={event_id} -> PROCESSING")

    try:
        #raise Exception("test retry")
        log.info(f"Processing event_id={event_id} event_type={row['event_type']}")

        if row["event_type"] == "sale":
            result_ref = handle_sale(row)
        else:
            log.info(f"Unknown event_type={row['event_type']}, using stub")
            result_ref = "stub"

        now = int(time.time())

        #помечаем DONE
        cursor.execute("""
            UPDATE event_store
            SET status = 'DONE', updated_at = ?
            WHERE id = ?
        """, (now, event_id))

        # фиксируем идемпотентность: событие обработано
        cursor.execute("""
            INSERT OR IGNORE INTO processed_events (tenant_id, event_key, result_ref, processed_at)
            VALUES (?, ?, ?, ?)
        """, (
            row["tenant_id"],
            row["event_key"],
            result_ref,
            now
        ))

        conn.commit()
        log.info(f"DONE event_id={event_id} event_key={row['event_key']}")

    except Exception as e:
        now = int(time.time())

        # retries после инкремента
        new_retries = row["retries"] + 1
        delay = 60 * (2 ** (new_retries - 1))  # 60,120,240,480,960

        # если попыток стало 5 — считаем окончательно проваленным
        if new_retries >= 5:
            cursor.execute("""
                UPDATE event_store
                SET status = 'FAILED',
                    retries = ?,
                    next_retry_at = NULL,
                    last_error_message = ?,
                    updated_at = ?
                WHERE id = ?
            """, (new_retries, str(e), now, event_id))
            conn.commit()
            log.error(f"FAILED event_id={event_id} event_key={row['event_key']} retries={row['retries']} err={e}")
        else:
            cursor.execute("""
                UPDATE event_store
                SET status = 'RETRY',
                    retries = ?,
                    next_retry_at = ?,
                    last_error_message = ?,
                    updated_at = ?
                WHERE id = ?
            """, (new_retries, now + delay, str(e), now, event_id))
            conn.commit()
            log.warning(f"RETRY event_id={event_id} event_key={row['event_key']} retries={new_retries} next_retry_at={now+delay} err={e}")

    conn.close()
    return True


def main_loop():
    print("Worker started")
    while True:
        processed = process_one_event()
        if not processed:
            time.sleep(2)


if __name__ == "__main__":
    main_loop()