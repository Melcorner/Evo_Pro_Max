import json
import logging
import os
import signal
import time

from app.logger import setup_logging
from app.services.error_logic import classify_error, RETRY, FAILED
from app.stores.error_store import insert_error
from app.services.event_dispatcher import dispatch_event
from app.db import get_connection

setup_logging()
log = logging.getLogger("worker")

WORKER_HEARTBEAT_NAME = "worker"
WORKER_HEARTBEAT_INTERVAL_SEC = int(os.getenv("WORKER_HEARTBEAT_INTERVAL_SEC", "5"))
STALE_PROCESSING_TIMEOUT_SEC = int(os.getenv("STALE_PROCESSING_TIMEOUT_SEC", "300"))
STALE_PROCESSING_CHECK_INTERVAL_SEC = int(os.getenv("STALE_PROCESSING_CHECK_INTERVAL_SEC", "60"))

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Shutdown signal received (signum=%s), finishing current event…", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def heartbeat_worker() -> None:
    now = int(time.time())
    meta_json = json.dumps({"service": WORKER_HEARTBEAT_NAME})
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT INTO service_heartbeats (service_name, last_seen_at, meta_json)
            VALUES (?, ?, ?)
            ON CONFLICT(service_name) DO UPDATE SET
                last_seen_at = excluded.last_seen_at,
                meta_json = excluded.meta_json
            """,
            (WORKER_HEARTBEAT_NAME, now, meta_json),
        )
        conn.commit()
    finally:
        conn.close()


def recover_stale_processing() -> None:
    """
    Сбрасывает события, зависшие в статусе PROCESSING, обратно в RETRY/FAILED.

    Важно:
    - recovery использует CAS по старому updated_at,
      чтобы не перетереть событие, которое уже успело завершиться в DONE;
    - insert_error пишем только если recovery-UPDATE реально сработал.
    """
    conn = get_connection()
    try:
        now = int(time.time())
        stale_before = now - STALE_PROCESSING_TIMEOUT_SEC
        cur = conn.cursor()

        cur.execute(
            """
            SELECT *
            FROM event_store
            WHERE status = 'PROCESSING'
              AND updated_at < ?
            """,
            (stale_before,),
        )
        rows = cur.fetchall()

        recovered = 0

        for row in rows:
            locked_at = row["updated_at"]

            cur.execute(
                """
                UPDATE event_store
                SET status = CASE WHEN retries + 1 >= 5 THEN 'FAILED' ELSE 'RETRY' END,
                    retries = retries + 1,
                    next_retry_at = CASE WHEN retries + 1 >= 5 THEN NULL ELSE ? END,
                    last_error_code = ?,
                    last_error_message = ?,
                    updated_at = ?
                WHERE id = ?
                  AND status = 'PROCESSING'
                  AND updated_at = ?
                """,
                (
                    now + 60,
                    "STALE_PROCESSING",
                    "stale PROCESSING: recovered by worker",
                    now,
                    row["id"],
                    locked_at,
                ),
            )

            if cur.rowcount == 0:
                log.info(
                    "Skip stale recovery CAS miss event_id=%s — event already changed state",
                    row["id"],
                )
                continue

            insert_error(
                conn,
                row,
                error_code="STALE_PROCESSING",
                message="stale PROCESSING: recovered by worker",
                response_body=None,
            )
            recovered += 1

        if recovered:
            log.warning("Recovered %s stale PROCESSING event(s) -> RETRY/FAILED", recovered)

        conn.commit()
    except Exception:
        log.exception("recover_stale_processing failed")
    finally:
        conn.close()


def process_one_event():
    conn = get_connection()
    cursor = conn.cursor()

    now = int(time.time())

    cursor.execute(
        """
        SELECT * FROM event_store
        WHERE (status = 'NEW')
           OR (status = 'RETRY' AND next_retry_at IS NOT NULL AND next_retry_at <= ? AND retries < 5)
        ORDER BY created_at
        LIMIT 1
        """,
        (now,),
    )
    row = cursor.fetchone()

    if not row:
        conn.close()
        return False

    event_id = row["id"]
    log.info(
        "Picked event_id=%s status=%s retries=%s event_key=%s",
        event_id,
        row["status"],
        row["retries"],
        row["event_key"],
    )

    locked_at = int(time.time())
    cursor.execute(
        """
        UPDATE event_store
        SET status = 'PROCESSING', updated_at = ?
        WHERE id = ? AND status IN ('NEW','RETRY')
        """,
        (locked_at, event_id),
    )

    if cursor.rowcount == 0:
        conn.close()
        return True

    conn.commit()
    log.info("Locked event_id=%s -> PROCESSING locked_at=%s", event_id, locked_at)

    try:
        log.info("Processing event_id=%s event_type=%s", event_id, row["event_type"])

        result_ref = dispatch_event(row)
        now = int(time.time())

        cursor.execute(
            """
            UPDATE event_store
            SET status = 'DONE', updated_at = ?
            WHERE id = ? AND status = 'PROCESSING' AND updated_at = ?
            """,
            (now, event_id, locked_at),
        )

        if cursor.rowcount == 0:
            log.warning(
                "CAS miss on DONE: event_id=%s was already recovered by stale-recovery — skipping DONE",
                event_id,
            )
            conn.commit()
            return True

        cursor.execute(
            """
            INSERT OR IGNORE INTO processed_events (tenant_id, event_key, result_ref, processed_at)
            VALUES (?, ?, ?, ?)
            """,
            (row["tenant_id"], row["event_key"], result_ref, now),
        )

        conn.commit()
        log.info("DONE event_id=%s event_key=%s", event_id, row["event_key"])
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

        delay = 60 * (2 ** (new_retries - 1))
        go_failed = decision == FAILED or new_retries >= 5

        if go_failed:
            cursor.execute(
                """
                UPDATE event_store
                SET status = 'FAILED',
                    retries = ?,
                    next_retry_at = NULL,
                    last_error_code = ?,
                    last_error_message = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'PROCESSING' AND updated_at = ?
                """,
                (new_retries, error_code, err, now, event_id, locked_at),
            )
        else:
            cursor.execute(
                """
                UPDATE event_store
                SET status = 'RETRY',
                    retries = ?,
                    next_retry_at = ?,
                    last_error_code = ?,
                    last_error_message = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'PROCESSING' AND updated_at = ?
                """,
                (new_retries, now + delay, error_code, err, now, event_id, locked_at),
            )

        if cursor.rowcount == 0:
            log.warning(
                "CAS miss on error path: event_id=%s was already recovered by stale-recovery — skipping retry/failed overwrite",
                event_id,
            )
            conn.commit()
            return True

        insert_error(conn, row, error_code, err, response_body)
        conn.commit()

        if go_failed:
            log.error(
                "FAILED event_id=%s event_key=%s retries=%s err=%s",
                event_id,
                row["event_key"],
                new_retries,
                err,
            )
        else:
            log.warning(
                "RETRY event_id=%s event_key=%s retries=%s next_retry_at=%s err=%s",
                event_id,
                row["event_key"],
                new_retries,
                now + delay,
                err,
            )

        return True

    finally:
        conn.close()


def main_loop():
    log.info("Worker started")
    last_heartbeat_at = 0
    last_stale_check_at = 0

    while not _shutdown:
        now = time.time()

        if now - last_heartbeat_at >= WORKER_HEARTBEAT_INTERVAL_SEC:
            heartbeat_worker()
            last_heartbeat_at = now

        if now - last_stale_check_at >= STALE_PROCESSING_CHECK_INTERVAL_SEC:
            recover_stale_processing()
            last_stale_check_at = time.time()

        processed = process_one_event()

        if processed:
            heartbeat_worker()
            last_heartbeat_at = time.time()
        else:
            time.sleep(2)

    log.info("Worker stopped gracefully")


if __name__ == "__main__":
    main_loop()