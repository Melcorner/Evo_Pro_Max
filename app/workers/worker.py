import json
import logging
import os
import signal
import time

from app.observability.metrics import (
    observe_duration,
    start_worker_metrics_server,
    worker_cycles_total,
    worker_events_picked_total,
    worker_events_processed_total,
    worker_idle_cycles_total,
    worker_last_heartbeat_unixtime,
    worker_processing_duration_seconds,
    worker_stale_recovered_total,
)

from app.logger import setup_logging
from app.services.error_logic import classify_error, RETRY, FAILED
from app.stores.error_store import insert_error
from app.services.event_dispatcher import dispatch_event
from app.db import get_connection, adapt_query as aq

setup_logging()
log = logging.getLogger("worker")


def _event_extra(
    row: dict | None = None,
    *,
    event_id: str | None = None,
    operation: str,
    status: str | None = None,
    exception_type: str | None = None,
    component: str = "worker",
) -> dict:
    payload = {
        "component": component,
        "operation": operation,
    }

    if row is not None:
        if row.get("tenant_id") is not None:
            payload["tenant_id"] = row["tenant_id"]
        if row.get("id") is not None:
            payload["event_id"] = row["id"]
        if row.get("event_key") is not None:
            payload["event_key"] = row["event_key"]

    if event_id is not None:
        payload["event_id"] = event_id

    if status is not None:
        payload["status"] = status

    if exception_type is not None:
        payload["exception_type"] = exception_type

    return payload


WORKER_HEARTBEAT_NAME = "worker"
WORKER_HEARTBEAT_INTERVAL_SEC = int(os.getenv("WORKER_HEARTBEAT_INTERVAL_SEC", "5"))
STALE_PROCESSING_TIMEOUT_SEC = int(os.getenv("STALE_PROCESSING_TIMEOUT_SEC", "300"))
STALE_PROCESSING_CHECK_INTERVAL_SEC = int(os.getenv("STALE_PROCESSING_CHECK_INTERVAL_SEC", "60"))
WORKER_METRICS_HOST = os.getenv("WORKER_METRICS_HOST", "0.0.0.0")
WORKER_METRICS_PORT = int(os.getenv("WORKER_METRICS_PORT", "8001"))

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Shutdown signal received (signum=%s), finishing current event…", signum)
    _shutdown = True


def _start_metrics_exporter() -> None:
    if WORKER_METRICS_PORT <= 0:
        log.info(
            "Worker Prometheus exporter disabled because WORKER_METRICS_PORT=%s",
            WORKER_METRICS_PORT,
        )
        return

    try:
        start_worker_metrics_server(port=WORKER_METRICS_PORT, host=WORKER_METRICS_HOST)
        log.info(
            "Worker Prometheus exporter listening on http://%s:%s/metrics",
            WORKER_METRICS_HOST,
            WORKER_METRICS_PORT,
        )
    except Exception:
        log.exception(
            "Failed to start worker Prometheus exporter host=%s port=%s",
            WORKER_METRICS_HOST,
            WORKER_METRICS_PORT,
        )


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def heartbeat_worker() -> None:
    now = int(time.time())
    worker_last_heartbeat_unixtime.set(now)

    meta_json = json.dumps({"service": WORKER_HEARTBEAT_NAME})
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            INSERT INTO service_heartbeats (service_name, last_seen_at, meta_json)
            VALUES (?, ?, ?)
            ON CONFLICT(service_name) DO UPDATE SET
                last_seen_at = excluded.last_seen_at,
                meta_json = excluded.meta_json
            """),
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
            aq("""
            SELECT *
            FROM event_store
            WHERE status = 'PROCESSING'
              AND updated_at < ?
            """),
            (stale_before,),
        )
        rows = cur.fetchall()

        recovered = 0

        for row in rows:
            locked_at = row["updated_at"]
            will_fail = (row["retries"] + 1) >= 5

            cur.execute(
                aq("""
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
                """),
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
                    "worker stale recovery cas miss",
                    extra=_event_extra(
                        row,
                        operation="worker.recover_stale_processing",
                        status="cas_miss",
                    ),
                )
                continue

            insert_error(
                conn,
                row,
                error_code="STALE_PROCESSING",
                message="stale PROCESSING: recovered by worker",
                response_body=None,
            )

            worker_stale_recovered_total.labels(
                result="failed" if will_fail else "retry"
            ).inc()

            recovered += 1

        if recovered:
            log.warning(
                "worker stale processing recovered",
                extra={
                    "component": "worker",
                    "operation": "worker.recover_stale_processing",
                    "status": "recovered",
                },
            )

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
        aq("""
        SELECT * FROM event_store
        WHERE (status = 'NEW')
           OR (status = 'RETRY' AND next_retry_at IS NOT NULL AND next_retry_at <= ? AND retries < 5)
        ORDER BY created_at
        LIMIT 1
        """),
        (now,),
    )
    row = cursor.fetchone()

    if not row:
        conn.close()
        return False

    event_id = row["id"]
    log.info(
        "worker event picked",
        extra=_event_extra(
            row,
            operation="worker.process_one_event",
            status="picked",
        ),
    )

    locked_at = int(time.time())
    cursor.execute(
        aq("""
        UPDATE event_store
        SET status = 'PROCESSING', updated_at = ?
        WHERE id = ? AND status IN ('NEW','RETRY')
        """),
        (locked_at, event_id),
    )

    if cursor.rowcount == 0:
        worker_events_processed_total.labels(result="cas_miss").inc()
        conn.close()
        return True

    conn.commit()
    worker_events_picked_total.inc()

    log.info(
        "worker event locked",
        extra=_event_extra(
            row,
            operation="worker.process_one_event",
            status="processing",
        ),
    )

    try:
        with observe_duration(worker_processing_duration_seconds):
            log.info(
                "worker event processing",
                extra=_event_extra(
                    row,
                    operation="worker.process_one_event",
                    status="processing",
                ),
            )

            result_ref = dispatch_event(row)
            now = int(time.time())

            cursor.execute(
                aq("""
                UPDATE event_store
                SET status = 'DONE', updated_at = ?
                WHERE id = ? AND status = 'PROCESSING' AND updated_at = ?
                """),
                (now, event_id, locked_at),
            )

            if cursor.rowcount == 0:
                log.warning(
                    "worker event cas miss on done",
                    extra=_event_extra(
                        row,
                        operation="worker.process_one_event",
                        status="cas_miss",
                    ),
                )
                conn.commit()
                worker_events_processed_total.labels(result="cas_miss").inc()
                return True

            cursor.execute(
                aq("""
                INSERT INTO processed_events (tenant_id, event_key, result_ref, processed_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (tenant_id, event_key) DO NOTHING
                """),
                (row["tenant_id"], row["event_key"], result_ref, now),
            )

            conn.commit()
            worker_events_processed_total.labels(result="done").inc()

            log.info(
                "worker event done",
                extra=_event_extra(
                    row,
                    operation="worker.process_one_event",
                    status="done",
                ),
            )
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
            "worker handler error",
            extra=_event_extra(
                row,
                operation="worker.process_one_event",
                status=decision.lower(),
                exception_type=type(e).__name__,
            ),
        )

        delay = 60 * (2 ** (new_retries - 1))
        go_failed = decision == FAILED or new_retries >= 5

        if go_failed:
            cursor.execute(
                aq("""
                UPDATE event_store
                SET status = 'FAILED',
                    retries = ?,
                    next_retry_at = NULL,
                    last_error_code = ?,
                    last_error_message = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'PROCESSING' AND updated_at = ?
                """),
                (new_retries, error_code, err, now, event_id, locked_at),
            )
        else:
            cursor.execute(
                aq("""
                UPDATE event_store
                SET status = 'RETRY',
                    retries = ?,
                    next_retry_at = ?,
                    last_error_code = ?,
                    last_error_message = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'PROCESSING' AND updated_at = ?
                """),
                (new_retries, now + delay, error_code, err, now, event_id, locked_at),
            )

        if cursor.rowcount == 0:
            log.warning(
                "worker event cas miss on error path",
                extra=_event_extra(
                    row,
                    operation="worker.process_one_event",
                    status="cas_miss",
                ),
            )
            conn.commit()
            worker_events_processed_total.labels(result="cas_miss").inc()
            return True

        insert_error(conn, row, error_code, err, response_body)
        conn.commit()

        if go_failed:
            worker_events_processed_total.labels(result="failed").inc()
            log.error(
                "worker event failed",
                extra=_event_extra(
                    row,
                    operation="worker.process_one_event",
                    status="failed",
                    exception_type=type(e).__name__,
                ),
            )
        else:
            worker_events_processed_total.labels(result="retry").inc()
            log.warning(
                "worker event scheduled for retry",
                extra=_event_extra(
                    row,
                    operation="worker.process_one_event",
                    status="retry",
                    exception_type=type(e).__name__,
                ),
            )

        return True

    finally:
        conn.close()


def runtime_db_smoke_check() -> dict:
    heartbeat_worker()

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'NEW' THEN 1 ELSE 0 END) AS new_count,
                SUM(CASE WHEN status = 'RETRY' THEN 1 ELSE 0 END) AS retry_count,
                SUM(CASE WHEN status = 'PROCESSING' THEN 1 ELSE 0 END) AS processing_count
            FROM event_store
            """
        )
        row = cur.fetchone() or {}
        return {
            "heartbeat_written": True,
            "new_count": int(row["new_count"] or 0),
            "retry_count": int(row["retry_count"] or 0),
            "processing_count": int(row["processing_count"] or 0),
        }
    finally:
        conn.close()


def main_loop():
    _start_metrics_exporter()
    log.info("Worker started")
    last_heartbeat_at = 0
    last_stale_check_at = 0

    while not _shutdown:
        worker_cycles_total.inc()
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
            worker_idle_cycles_total.inc()
            time.sleep(2)

    log.info("Worker stopped gracefully")


if __name__ == "__main__":
    main_loop()
