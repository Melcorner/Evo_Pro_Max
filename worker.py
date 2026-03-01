import time
import uuid

from db import get_connection


def process_one_event():
    conn = get_connection()
    cursor = conn.cursor()

    # забираем одно NEW событие
    cursor.execute("""
        SELECT * FROM event_store
        WHERE status = 'NEW'
        ORDER BY created_at
        LIMIT 1
    """)
    row = cursor.fetchone()

    if not row:
        conn.close()
        return False

    event_id = row["id"]

    # пытаемся захватить (важно для будущих параллельных воркеров)
    cursor.execute("""
        UPDATE event_store
        SET status = 'PROCESSING', updated_at = ?
        WHERE id = ? AND status = 'NEW'
    """, (int(time.time()), event_id))

    if cursor.rowcount == 0:
        conn.close()
        return True  # кто-то уже взял

    conn.commit()

    try:
        # здесь потом будет реальная логика sync
        print(f"Processing event {event_id}")
        time.sleep(1)

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
            None,
            now
        ))

        conn.commit()

    except Exception as e:
        cursor.execute("""
            UPDATE event_store
            SET status = 'RETRY',
                retries = retries + 1,
                next_retry_at = ?,
                last_error_message = ?
            WHERE id = ?
        """, (
            int(time.time()) + 60,
            str(e),
            event_id
        ))
        conn.commit()

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