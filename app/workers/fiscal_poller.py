"""
fiscal_poller.py — фоновый воркер для polling статусов фискализации.

Периодически выбирает из fiscalization_checks все записи со статусом 1/2/5
(new / sent_to_device / accepted_by_device) и запрашивает актуальный статус
через GET /check/<uid> у fiscalization24.

Запуск:
    python -m app.workers.fiscal_poller

Переменные окружения:
    FISCAL_POLL_INTERVAL_SEC      — пауза между циклами (default: 60)
    FISCAL_POLL_STALE_SEC         — минимальный возраст записи для polling (default: 30)
    FISCAL_POLL_MAX_ATTEMPTS      — макс. попыток poll до перевода в status=9 (default: 20)
    FISCAL_BASE_URL               — base URL fiscalization24 (default: https://...)
    FISCAL_TIME_OFFSET_SEC        — компенсация clock skew (default: 0)
"""

import json
import logging
import os
import signal
import time

from app.observability.metrics import (
    fiscal_poller_cycles_total,
    fiscal_poller_pending_checks,
    fiscal_poller_polled_total,
    fiscal_poller_poll_duration_seconds,
    observe_duration,
    start_fiscal_poller_metrics_server,
)

from app.db import get_connection, adapt_query as aq
from app.logger import setup_logging

setup_logging()
log = logging.getLogger("fiscal_poller")


def _poll_extra(
    check: dict | None = None,
    *,
    uid: str | None = None,
    tenant_id: str | None = None,
    operation: str,
    status: str | None = None,
    exception_type: str | None = None,
    component: str = "fiscal_poller",
) -> dict:
    payload = {
        "component": component,
        "operation": operation,
    }

    if check is not None:
        if check.get("tenant_id") is not None:
            payload["tenant_id"] = check["tenant_id"]
        if check.get("uid") is not None:
            payload["uid"] = check["uid"]

    if uid is not None:
        payload["uid"] = uid
    if tenant_id is not None:
        payload["tenant_id"] = tenant_id
    if status is not None:
        payload["status"] = status
    if exception_type is not None:
        payload["exception_type"] = exception_type

    return payload


POLL_INTERVAL_SEC = int(os.getenv("FISCAL_POLL_INTERVAL_SEC", "60"))
POLL_STALE_SEC = int(os.getenv("FISCAL_POLL_STALE_SEC", "30"))
POLL_MAX_ATTEMPTS = int(os.getenv("FISCAL_POLL_MAX_ATTEMPTS", "20"))
FISCAL_POLLER_METRICS_HOST = os.getenv("FISCAL_POLLER_METRICS_HOST", "0.0.0.0")
FISCAL_POLLER_METRICS_PORT = int(os.getenv("FISCAL_POLLER_METRICS_PORT", "8002"))

PENDING_STATUSES = (1, 2, 5)

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Fiscal poller shutdown signal received (signum=%s)", signum)
    _shutdown = True


def _start_metrics_exporter() -> None:
    if FISCAL_POLLER_METRICS_PORT <= 0:
        log.info(
            "Fiscal poller Prometheus exporter disabled because FISCAL_POLLER_METRICS_PORT=%s",
            FISCAL_POLLER_METRICS_PORT,
        )
        return

    try:
        start_fiscal_poller_metrics_server(
            port=FISCAL_POLLER_METRICS_PORT,
            host=FISCAL_POLLER_METRICS_HOST,
        )
        log.info(
            "Fiscal poller Prometheus exporter listening on http://%s:%s/metrics",
            FISCAL_POLLER_METRICS_HOST,
            FISCAL_POLLER_METRICS_PORT,
        )
    except Exception:
        log.exception(
            "Failed to start fiscal poller Prometheus exporter host=%s port=%s",
            FISCAL_POLLER_METRICS_HOST,
            FISCAL_POLLER_METRICS_PORT,
        )


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _load_pending_checks() -> list[dict]:
    now = int(time.time())
    stale_before = now - POLL_STALE_SEC

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            SELECT fc.uid, fc.tenant_id, fc.ms_demand_id,
                   fc.status, fc.attempt,
                   fc.last_poll_at, fc.next_poll_at,
                   t.fiscal_token
            FROM fiscalization_checks fc
            JOIN tenants t ON t.id = fc.tenant_id
            WHERE fc.status IN (1, 2, 5)
              AND (fc.next_poll_at IS NULL OR fc.next_poll_at <= ?)
              AND fc.updated_at < ?
            ORDER BY fc.updated_at
            LIMIT 50
            """),
            (now, stale_before),
        )
        rows = cur.fetchall()
        result = [dict(r) for r in rows]
        fiscal_poller_pending_checks.set(len(result))
        return result
    finally:
        conn.close()


def _update_check(uid: str, new_status: int, state_dict: dict, attempt: int) -> None:
    now = int(time.time())
    backoff = min(30 * (2 ** attempt), 600)
    next_poll_at = now + backoff if new_status in PENDING_STATUSES else None

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            UPDATE fiscalization_checks
            SET status              = ?,
                description         = ?,
                error_code          = ?,
                error_message       = ?,
                response_json       = ?,
                attempt             = ?,
                last_poll_at        = ?,
                next_poll_at        = ?,
                updated_at          = ?
            WHERE uid = ?
            """),
            (
                new_status,
                state_dict.get("Description"),
                state_dict.get("Error"),
                state_dict.get("ErrorMessage"),
                json.dumps(state_dict, ensure_ascii=False),
                attempt,
                now,
                next_poll_at,
                now,
                uid,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _mark_transport_error(uid: str, error: str, attempt: int) -> None:
    now = int(time.time())
    backoff = min(30 * (2 ** attempt), 600)
    next_poll_at = now + backoff

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            UPDATE fiscalization_checks
            SET last_transport_error = ?,
                attempt              = ?,
                last_poll_at         = ?,
                next_poll_at         = ?,
                updated_at           = ?
            WHERE uid = ?
            """),
            (error, attempt, now, next_poll_at, now, uid),
        )
        conn.commit()
    finally:
        conn.close()


def _poll_one(check: dict) -> None:
    from app.clients.fiscalization_client import FiscalizationClient

    with observe_duration(fiscal_poller_poll_duration_seconds):
        uid = check["uid"]
        tenant_id = check["tenant_id"]
        fiscal_token = check.get("fiscal_token")
        attempt = (check.get("attempt") or 0) + 1

        if not fiscal_token:
            error_text = "fiscal_token missing for tenant"
            fiscal_poller_polled_total.labels(result="missing_token").inc()

            log.warning(
                "fiscal poll missing token",
                extra=_poll_extra(
                    check,
                    operation="fiscal_poller.poll",
                    status="missing_token",
                ),
            )
            _mark_transport_error(uid, error_text, attempt)

            if attempt >= POLL_MAX_ATTEMPTS:
                log.error(
                    "fiscal poll max attempts reached with missing token",
                    extra=_poll_extra(
                        check,
                        operation="fiscal_poller.poll",
                        status="error",
                    ),
                )
                _update_check(
                    uid,
                    9,
                    {"ErrorMessage": f"Max poll attempts ({POLL_MAX_ATTEMPTS}) exceeded: {error_text}"},
                    attempt,
                )
                fiscal_poller_polled_total.labels(result="error").inc()
            return

        log.info(
            "fiscal poll started",
            extra=_poll_extra(
                check,
                operation="fiscal_poller.poll",
                status="polling",
            ),
        )

        try:
            client = FiscalizationClient(fiscal_token)
            state = client.get_check_state(uid)
        except Exception as e:
            fiscal_poller_polled_total.labels(result="transport_error").inc()

            log.warning(
                "fiscal poll transport error",
                extra=_poll_extra(
                    check,
                    operation="fiscal_poller.poll",
                    status="transport_error",
                    exception_type=type(e).__name__,
                ),
            )
            _mark_transport_error(uid, str(e), attempt)

            if attempt >= POLL_MAX_ATTEMPTS:
                log.error(
                    "fiscal poll max attempts reached",
                    extra=_poll_extra(
                        check,
                        operation="fiscal_poller.poll",
                        status="error",
                    ),
                )
                _update_check(
                    uid,
                    9,
                    {"ErrorMessage": f"Max poll attempts ({POLL_MAX_ATTEMPTS}) exceeded: {e}"},
                    attempt,
                )
                fiscal_poller_polled_total.labels(result="error").inc()
            return

        new_status = int(state.get("State") or check["status"])

        log.info(
            "fiscal poll state updated",
            extra=_poll_extra(
                check,
                operation="fiscal_poller.poll",
                status=str(new_status),
            ),
        )

        _update_check(uid, new_status, state, attempt)

        if new_status == 10:
            fiscal_poller_polled_total.labels(result="fiscalized").inc()
            log.info(
                "fiscal poll check fiscalized",
                extra=_poll_extra(
                    check,
                    operation="fiscal_poller.poll",
                    status="fiscalized",
                ),
            )
        elif new_status == 9:
            fiscal_poller_polled_total.labels(result="error").inc()
            log.error(
                "fiscal poll check error",
                extra=_poll_extra(
                    check,
                    operation="fiscal_poller.poll",
                    status="error",
                ),
            )
        else:
            fiscal_poller_polled_total.labels(result="still_pending").inc()


def poll_cycle() -> int:
    fiscal_poller_cycles_total.inc()
    checks = _load_pending_checks()

    if not checks:
        return 0

    log.info(
        "fiscal poller found pending checks",
        extra={
            "component": "fiscal_poller",
            "operation": "fiscal_poller.poll_cycle",
            "status": "pending_found",
        },
    )

    for check in checks:
        if _shutdown:
            break
        try:
            _poll_one(check)
        except Exception:
            fiscal_poller_polled_total.labels(result="unexpected_exception").inc()
            log.exception(
                "fiscal poll unexpected error",
                extra=_poll_extra(
                    check,
                    operation="fiscal_poller.poll",
                    status="unexpected_error",
                ),
            )

    return len(checks)


def main_loop() -> None:
    _start_metrics_exporter()
    log.info(
        "Fiscal poller started interval=%ss stale=%ss max_attempts=%s",
        POLL_INTERVAL_SEC, POLL_STALE_SEC, POLL_MAX_ATTEMPTS,
    )

    while not _shutdown:
        try:
            processed = poll_cycle()
        except Exception:
            log.exception("Fiscal poll cycle failed")
            processed = 0

        if _shutdown:
            break

        time.sleep(5 if processed > 0 else POLL_INTERVAL_SEC)

    log.info("Fiscal poller stopped gracefully")


if __name__ == "__main__":
    main_loop()
