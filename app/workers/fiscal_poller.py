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

from app.db import get_connection
from app.logger import setup_logging

setup_logging()
log = logging.getLogger("fiscal_poller")

POLL_INTERVAL_SEC = int(os.getenv("FISCAL_POLL_INTERVAL_SEC", "60"))
POLL_STALE_SEC = int(os.getenv("FISCAL_POLL_STALE_SEC", "30"))
POLL_MAX_ATTEMPTS = int(os.getenv("FISCAL_POLL_MAX_ATTEMPTS", "20"))

PENDING_STATUSES = (1, 2, 5)

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Fiscal poller shutdown signal received (signum=%s)", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _load_pending_checks() -> list[dict]:
    now = int(time.time())
    stale_before = now - POLL_STALE_SEC

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
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
            """,
            (now, stale_before),
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _update_check(uid: str, new_status: int, state_dict: dict, attempt: int) -> None:
    now = int(time.time())
    backoff = min(30 * (2 ** attempt), 600)
    next_poll_at = now + backoff if new_status in PENDING_STATUSES else None

    conn = get_connection()
    try:
        conn.execute(
            """
            UPDATE fiscalization_checks
            SET status         = ?,
                description    = ?,
                error_code     = ?,
                error_message  = ?,
                response_json  = ?,
                attempt        = ?,
                last_poll_at   = ?,
                next_poll_at   = ?,
                updated_at     = ?
            WHERE uid = ?
            """,
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
        conn.execute(
            """
            UPDATE fiscalization_checks
            SET last_transport_error = ?,
                attempt              = ?,
                last_poll_at         = ?,
                next_poll_at         = ?,
                updated_at           = ?
            WHERE uid = ?
            """,
            (error, attempt, now, next_poll_at, now, uid),
        )
        conn.commit()
    finally:
        conn.close()


def _poll_one(check: dict) -> None:
    from app.clients.fiscalization_client import FiscalizationClient

    uid = check["uid"]
    tenant_id = check["tenant_id"]
    fiscal_token = check.get("fiscal_token")
    attempt = (check.get("attempt") or 0) + 1

    if not fiscal_token:
        error_text = "fiscal_token missing for tenant"
        log.warning(
            "fiscal_token missing tenant_id=%s uid=%s attempt=%s",
            tenant_id,
            uid,
            attempt,
        )
        _mark_transport_error(uid, error_text, attempt)

        if attempt >= POLL_MAX_ATTEMPTS:
            log.error(
                "Max poll attempts reached with missing fiscal_token uid=%s tenant_id=%s — marking as error (9)",
                uid,
                tenant_id,
            )
            _update_check(
                uid,
                9,
                {"ErrorMessage": f"Max poll attempts ({POLL_MAX_ATTEMPTS}) exceeded: {error_text}"},
                attempt,
            )
        return

    log.info(
        "Polling fiscal check uid=%s tenant_id=%s attempt=%s current_status=%s",
        uid,
        tenant_id,
        attempt,
        check["status"],
    )

    try:
        client = FiscalizationClient(fiscal_token)
        state = client.get_check_state(uid)
    except Exception as e:
        log.warning(
            "Transport error polling uid=%s tenant_id=%s attempt=%s err=%s",
            uid,
            tenant_id,
            attempt,
            e,
        )
        _mark_transport_error(uid, str(e), attempt)

        if attempt >= POLL_MAX_ATTEMPTS:
            log.error(
                "Max poll attempts reached uid=%s tenant_id=%s — marking as error (9)",
                uid,
                tenant_id,
            )
            _update_check(
                uid,
                9,
                {"ErrorMessage": f"Max poll attempts ({POLL_MAX_ATTEMPTS}) exceeded: {e}"},
                attempt,
            )
        return

    new_status = int(state.get("State") or check["status"])

    log.info(
        "Fiscal check uid=%s tenant_id=%s status: %s -> %s description=%s",
        uid,
        tenant_id,
        check["status"],
        new_status,
        state.get("Description"),
    )

    _update_check(uid, new_status, state, attempt)

    if new_status == 10:
        log.info("Fiscal check FISCALIZED uid=%s tenant_id=%s", uid, tenant_id)
    elif new_status == 9:
        log.error(
            "Fiscal check ERROR uid=%s tenant_id=%s error=%s message=%s",
            uid,
            tenant_id,
            state.get("Error"),
            state.get("ErrorMessage"),
        )


def poll_cycle() -> int:
    checks = _load_pending_checks()

    if not checks:
        return 0

    log.info("Fiscal poller: found %s pending check(s) to poll", len(checks))

    for check in checks:
        if _shutdown:
            break
        try:
            _poll_one(check)
        except Exception:
            log.exception("Unexpected error polling uid=%s", check.get("uid"))

    return len(checks)


def main_loop() -> None:
    log.info(
        "Fiscal poller started interval=%ss stale=%ss max_attempts=%s",
        POLL_INTERVAL_SEC,
        POLL_STALE_SEC,
        POLL_MAX_ATTEMPTS,
    )

    while not _shutdown:
        try:
            processed = poll_cycle()
        except Exception:
            log.exception("Fiscal poll cycle failed")
            processed = 0

        if _shutdown:
            break

        if processed == 0:
            time.sleep(POLL_INTERVAL_SEC)
        else:
            time.sleep(5)

    log.info("Fiscal poller stopped gracefully")


if __name__ == "__main__":
    main_loop()
