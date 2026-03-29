from dotenv import load_dotenv

load_dotenv()

import logging
import os
import signal
import time

from app.clients.email_client import EmailClient
from app.clients.telegram_client import TelegramClient
from app.db import get_connection
from app.logger import setup_logging
from app.services.alert_logic import build_alert_messages, build_alert_snapshot

setup_logging()
log = logging.getLogger("alert_worker")

SERVICE_NAME = "integration-bus"
WORKER_HEARTBEAT_NAME = "worker"
WORKER_STALE_AFTER_SEC = int(os.getenv("WORKER_STALE_AFTER_SEC", "30"))
ALERT_POLL_INTERVAL_SEC = max(1, int(os.getenv("ALERT_POLL_INTERVAL_SEC", "30")))

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Shutdown signal received (signum=%s), stopping alert worker...", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _parse_bool_env(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_email_recipients(raw_value: str) -> list[str]:
    return [item.strip() for item in (raw_value or "").split(",") if item.strip()]


def _build_telegram_client() -> TelegramClient | None:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not bot_token and not chat_id:
        log.info("Telegram alerts disabled: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID not configured")
        return None

    try:
        client = TelegramClient(bot_token=bot_token, chat_id=chat_id)
        log.info("Telegram alerts enabled")
        return client
    except Exception:
        log.exception("Telegram alerts disabled due to configuration error")
        return None


def _build_email_client() -> EmailClient | None:
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port_raw = os.getenv("SMTP_PORT", "").strip()
    smtp_username = os.getenv("SMTP_USERNAME", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")
    smtp_from = os.getenv("SMTP_FROM", "").strip()
    alert_email_to = os.getenv("ALERT_EMAIL_TO", "").strip()
    smtp_use_tls = _parse_bool_env("SMTP_USE_TLS", True)

    if not any([smtp_host, smtp_port_raw, smtp_username, smtp_password, smtp_from, alert_email_to]):
        log.info("Email alerts disabled: SMTP settings not configured")
        return None

    try:
        smtp_port = int(smtp_port_raw)
    except ValueError:
        log.exception("Email alerts disabled: SMTP_PORT must be an integer")
        return None

    try:
        client = EmailClient(
            host=smtp_host,
            port=smtp_port,
            from_address=smtp_from,
            to_addresses=_parse_email_recipients(alert_email_to),
            username=smtp_username,
            password=smtp_password,
            use_tls=smtp_use_tls,
        )
        log.info("Email alerts enabled")
        return client
    except Exception:
        log.exception("Email alerts disabled due to configuration error")
        return None


def _build_email_subject(message: str) -> str:
    return message.split(" | ", 1)[0]


def _collect_snapshot():
    now_ts = int(time.time())
    conn = get_connection()

    try:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT last_seen_at
            FROM service_heartbeats
            WHERE service_name = ?
            """,
            (WORKER_HEARTBEAT_NAME,),
        )
        heartbeat_row = cur.fetchone()
        worker_last_seen_at = heartbeat_row["last_seen_at"] if heartbeat_row else None

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM event_store
            WHERE status = 'FAILED'
            """
        )
        failed_row = cur.fetchone()
        failed_events_count = failed_row["cnt"] if failed_row else 0

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM event_store
            WHERE status = 'RETRY'
            """
        )
        retry_row = cur.fetchone()
        retry_events_count = retry_row["cnt"] if retry_row else 0

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM stock_sync_status
            WHERE status = 'error'
            """
        )
        stock_error_row = cur.fetchone()
        stock_sync_errors_count = stock_error_row["cnt"] if stock_error_row else 0

    finally:
        conn.close()

    return build_alert_snapshot(
        service_name=SERVICE_NAME,
        now_ts=now_ts,
        worker_last_seen_at=worker_last_seen_at,
        stale_after_sec=WORKER_STALE_AFTER_SEC,
        failed_events_count=failed_events_count,
        retry_events_count=retry_events_count,
        stock_sync_errors_count=stock_sync_errors_count,
    )


def main_loop():
    log.info(
        "Alert worker started poll_interval=%s stale_after=%s",
        ALERT_POLL_INTERVAL_SEC,
        WORKER_STALE_AFTER_SEC,
    )

    telegram_client = _build_telegram_client()
    email_client = _build_email_client()

    if telegram_client is None and email_client is None:
        log.error("Alert worker has no delivery channels configured")
        return

    previous_snapshot = None

    while not _shutdown:
        try:
            current_snapshot = _collect_snapshot()

            if previous_snapshot is None:
                previous_snapshot = current_snapshot
                log.info(
                    "Alert baseline set worker_problem=%s failed_events_present=%s failed_events_count=%s retry_events_present=%s retry_events_count=%s stock_sync_errors_present=%s stock_sync_errors_count=%s",
                    current_snapshot.worker_problem,
                    current_snapshot.failed_events_present,
                    current_snapshot.failed_events_count,
                    current_snapshot.retry_events_present,
                    current_snapshot.retry_events_count,
                    current_snapshot.stock_sync_errors_present,
                    current_snapshot.stock_sync_errors_count,
                )
            else:
                messages = build_alert_messages(previous_snapshot, current_snapshot)

                if not messages:
                    previous_snapshot = current_snapshot
                else:
                    all_messages_delivered = True

                    for message in messages:
                        subject = _build_email_subject(message)
                        message_delivered = False

                        if telegram_client is not None:
                            try:
                                telegram_client.send_message(message)
                                message_delivered = True
                                log.info("Telegram alert sent message=%s", message)
                            except Exception:
                                log.exception("Failed to send Telegram alert message=%s", message)

                        if email_client is not None:
                            try:
                                email_client.send_message(subject=subject, text=message)
                                message_delivered = True
                                log.info("Email alert sent subject=%s", subject)
                            except Exception:
                                log.exception("Failed to send email alert message=%s", message)

                        if not message_delivered:
                            all_messages_delivered = False

                    if all_messages_delivered:
                        previous_snapshot = current_snapshot
                    else:
                        log.warning(
                            "Alert state not advanced because at least one alert message was not delivered by any channel"
                        )

        except Exception:
            log.exception("Alert worker cycle failed")

        if _shutdown:
            break

        time.sleep(ALERT_POLL_INTERVAL_SEC)

    log.info("Alert worker stopped gracefully")


if __name__ == "__main__":
    main_loop()
