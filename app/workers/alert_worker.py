from dotenv import load_dotenv

load_dotenv()

import logging
import os
import signal
import time

from app.clients.email_client import EmailClient
from app.clients.telegram_client import TelegramClient
from app.db import adapt_query as aq, get_connection
from app.logger import setup_logging
from app.services.alert_logic import build_alert_messages, build_alert_snapshot
from app.services.action_log_service import log_action
from app.stores.notification_log_store import insert_notification_log

setup_logging()
log = logging.getLogger("alert_worker")


def _alert_extra(
    *,
    component: str,
    operation: str,
    status: str | None = None,
    exception_type: str | None = None,
) -> dict:
    payload = {
        "component": component,
        "operation": operation,
    }
    if status is not None:
        payload["status"] = status
    if exception_type is not None:
        payload["exception_type"] = exception_type
    return payload


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


def _get_telegram_bot_token() -> str:
    return os.getenv("TELEGRAM_BOT_TOKEN", "").strip()


def _get_system_telegram_chat_id() -> str:
    return os.getenv("TELEGRAM_CHAT_ID", "").strip()


def _get_email_transport_config() -> dict | None:
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port_raw = os.getenv("SMTP_PORT", "").strip()
    smtp_username = os.getenv("SMTP_USERNAME", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")
    smtp_from = os.getenv("SMTP_FROM", "").strip()
    smtp_use_tls = _parse_bool_env("SMTP_USE_TLS", True)
    smtp_use_ssl = _parse_bool_env("SMTP_USE_SSL", False)

    if not any([smtp_host, smtp_port_raw, smtp_username, smtp_password, smtp_from]):
        log.info("Email alerts disabled: SMTP settings not configured")
        return None

    if not smtp_host or not smtp_port_raw or not smtp_from:
        log.error("Email alerts disabled: SMTP_HOST, SMTP_PORT and SMTP_FROM are required")
        return None

    try:
        smtp_port = int(smtp_port_raw)
    except ValueError:
        log.exception("Email alerts disabled: SMTP_PORT must be an integer")
        return None

    if smtp_password and not smtp_username:
        log.error("Email alerts disabled: SMTP_USERNAME is required when SMTP_PASSWORD is set")
        return None

    return {
        "host": smtp_host,
        "port": smtp_port,
        "from_address": smtp_from,
        "username": smtp_username,
        "password": smtp_password,
        "use_tls": smtp_use_tls,
        "use_ssl": smtp_use_ssl,
    }


def _build_system_telegram_client() -> TelegramClient | None:
    bot_token = _get_telegram_bot_token()
    chat_id = _get_system_telegram_chat_id()

    if not bot_token and not chat_id:
        log.info("Telegram system alerts disabled: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID not configured")
        return None

    if not bot_token or not chat_id:
        log.info("Telegram system alerts disabled: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing")
        return None

    try:
        client = TelegramClient(bot_token=bot_token, chat_id=chat_id)
        log.info("Telegram system alerts enabled")
        return client
    except Exception:
        log.exception("Telegram system alerts disabled due to configuration error")
        return None


def _build_tenant_telegram_client(chat_id: str) -> TelegramClient | None:
    bot_token = _get_telegram_bot_token()
    chat_id = str(chat_id or "").strip()

    if not bot_token or not chat_id:
        return None

    try:
        return TelegramClient(bot_token=bot_token, chat_id=chat_id)
    except Exception:
        log.exception("Tenant Telegram alerts disabled due to configuration error chat_id=%s", chat_id)
        return None


def _build_system_email_client(email_transport_config: dict | None) -> EmailClient | None:
    alert_email_to = os.getenv("ALERT_EMAIL_TO", "").strip()
    recipients = _parse_email_recipients(alert_email_to)

    if email_transport_config is None:
        return None

    if not recipients:
        log.info("Email system alerts disabled: ALERT_EMAIL_TO not configured")
        return None

    try:
        client = EmailClient(
            host=email_transport_config["host"],
            port=email_transport_config["port"],
            from_address=email_transport_config["from_address"],
            to_addresses=recipients,
            username=email_transport_config["username"],
            password=email_transport_config["password"],
            use_tls=email_transport_config["use_tls"],
            use_ssl=email_transport_config["use_ssl"],
        )
        log.info("Email system alerts enabled")
        return client
    except Exception:
        log.exception("Email system alerts disabled due to configuration error")
        return None


def _build_tenant_email_client(email_transport_config: dict | None, to_address: str) -> EmailClient | None:
    recipient = (to_address or "").strip()
    if email_transport_config is None or not recipient:
        return None

    try:
        return EmailClient(
            host=email_transport_config["host"],
            port=email_transport_config["port"],
            from_address=email_transport_config["from_address"],
            to_addresses=[recipient],
            username=email_transport_config["username"],
            password=email_transport_config["password"],
            use_tls=email_transport_config["use_tls"],
            use_ssl=email_transport_config["use_ssl"],
        )
    except Exception:
        log.exception("Tenant email alerts disabled due to configuration error recipient=%s", recipient)
        return None


def _build_email_subject(message: str) -> str:
    return message.split(" | ", 1)[0]


def _infer_system_event_type(message: str) -> str:
    if "worker problem" in message or "worker ok" in message:
        return "system_worker_status"
    if "FAILED events" in message:
        return "system_failed_events"
    if "RETRY events" in message:
        return "system_retry_events"
    if "stock sync errors" in message:
        return "system_stock_sync_errors"
    return "system_alert"


def _infer_tenant_event_type(message: str) -> str:
    if "tenant FAILED events" in message:
        return "tenant_failed_events"
    if "tenant RETRY events" in message:
        return "tenant_retry_events"
    if "tenant stock sync" in message:
        return "tenant_stock_sync"
    return "tenant_alert"


def _write_notification_log(
    *,
    tenant_id: str | None,
    channel_type: str,
    destination: str,
    event_type: str,
    message: str,
    status: str,
    error_message: str | None = None,
) -> None:
    conn = get_connection()
    try:
        insert_notification_log(
            conn,
            tenant_id=tenant_id,
            channel_type=channel_type,
            destination=destination,
            event_type=event_type,
            message=message,
            status=status,
            error_message=error_message,
            sent_at=int(time.time()) if status == "sent" else None,
        )
        conn.commit()
        if tenant_id:
            log_action(
                tenant_id=tenant_id,
                action_type="notification",
                status=status,
                message=f"{channel_type} notification {status}: {event_type}",
                source="alert_worker",
                metadata={"channel_type": channel_type, "event_type": event_type},
            )
    except Exception:
        conn.rollback()
        log.exception(
            "failed to write notification log",
            extra=_alert_extra(
                component="notification_log",
                operation="alert_worker.notification_log",
                status="failed",
            ),
        )
    finally:
        conn.close()


def _load_tenant_alert_channels(conn) -> dict[str, dict]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            id,
            name,
            alert_email,
            alerts_email_enabled,
            telegram_chat_id,
            alerts_telegram_enabled
        FROM tenants
        """
    )

    channels: dict[str, dict] = {}
    for row in cur.fetchall():
        alert_email = (row["alert_email"] or "").strip() or None
        telegram_chat_id = str(row["telegram_chat_id"] or "").strip() or None
        channels[row["id"]] = {
            "tenant_id": row["id"],
            "tenant_name": row["name"],
            "alert_email": alert_email,
            "alerts_email_requested": bool(row["alerts_email_enabled"]),
            "alerts_email_enabled": bool(row["alerts_email_enabled"]) and bool(alert_email),
            "telegram_chat_id": telegram_chat_id,
            "alerts_telegram_requested": bool(row["alerts_telegram_enabled"]),
            "alerts_telegram_enabled": bool(row["alerts_telegram_enabled"]) and bool(telegram_chat_id),
        }
    return channels


def _shorten_text(text: str | None, limit: int = 160) -> str | None:
    if not text:
        return None
    normalized = " ".join(str(text).split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3] + "..."


def _empty_tenant_alert_state(tenant_id: str, channels: dict | None = None) -> dict:
    channels = channels or {}
    return {
        "tenant_id": tenant_id,
        "tenant_name": channels.get("tenant_name"),
        "alert_email": channels.get("alert_email"),
        "alerts_email_requested": bool(channels.get("alerts_email_requested")),
        "alerts_email_enabled": bool(channels.get("alerts_email_enabled")),
        "telegram_chat_id": channels.get("telegram_chat_id"),
        "alerts_telegram_requested": bool(channels.get("alerts_telegram_requested")),
        "alerts_telegram_enabled": bool(channels.get("alerts_telegram_enabled")),
        "failed_events_count": 0,
        "retry_events_count": 0,
        "stock_error_present": False,
        "stock_last_error": None,
        "synced_items_count": 0,
        "total_items_count": 0,
    }


def _collect_snapshot():
    now_ts = int(time.time())
    conn = get_connection()

    try:
        cur = conn.cursor()

        cur.execute(
            aq(
                """
            SELECT last_seen_at
            FROM service_heartbeats
            WHERE service_name = ?
            """
            ),
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


def runtime_db_smoke_check() -> dict:
    snapshot = _collect_snapshot()
    tenant_snapshot = _collect_tenant_alert_snapshot()
    return {
        "worker_status": snapshot.worker_status,
        "failed_events_count": snapshot.failed_events_count,
        "retry_events_count": snapshot.retry_events_count,
        "stock_sync_errors_count": snapshot.stock_sync_errors_count,
        "tenant_count": len(tenant_snapshot),
    }


def _collect_tenant_alert_snapshot() -> dict[str, dict]:
    conn = get_connection()

    try:
        cur = conn.cursor()
        tenant_channels = _load_tenant_alert_channels(conn)
        tenant_states = {
            tenant_id: _empty_tenant_alert_state(tenant_id, channels)
            for tenant_id, channels in tenant_channels.items()
        }

        cur.execute(
            """
            SELECT tenant_id, COUNT(*) AS cnt
            FROM event_store
            WHERE status = 'FAILED'
            GROUP BY tenant_id
            """
        )
        for row in cur.fetchall():
            tenant_id = row["tenant_id"]
            state = tenant_states.setdefault(
                tenant_id,
                _empty_tenant_alert_state(tenant_id, tenant_channels.get(tenant_id)),
            )
            state["failed_events_count"] = max(int(row["cnt"] or 0), 0)

        cur.execute(
            """
            SELECT tenant_id, COUNT(*) AS cnt
            FROM event_store
            WHERE status = 'RETRY'
            GROUP BY tenant_id
            """
        )
        for row in cur.fetchall():
            tenant_id = row["tenant_id"]
            state = tenant_states.setdefault(
                tenant_id,
                _empty_tenant_alert_state(tenant_id, tenant_channels.get(tenant_id)),
            )
            state["retry_events_count"] = max(int(row["cnt"] or 0), 0)

        cur.execute(
            """
            SELECT
                tenant_id,
                last_error,
                synced_items_count,
                total_items_count
            FROM stock_sync_status
            WHERE status = 'error'
            """
        )
        for row in cur.fetchall():
            tenant_id = row["tenant_id"]
            state = tenant_states.setdefault(
                tenant_id,
                _empty_tenant_alert_state(tenant_id, tenant_channels.get(tenant_id)),
            )
            state["stock_error_present"] = True
            state["stock_last_error"] = _shorten_text(row["last_error"])
            state["synced_items_count"] = max(int(row["synced_items_count"] or 0), 0)
            state["total_items_count"] = max(int(row["total_items_count"] or 0), 0)

    finally:
        conn.close()

    return tenant_states


def _build_tenant_failed_problem_message(state: dict) -> str:
    return (
        "ALERT: tenant FAILED events detected"
        f" | tenant_id={state['tenant_id']}"
        f" | failed_events_count={state['failed_events_count']}"
    )


def _build_tenant_failed_recovery_message(state: dict) -> str:
    return f"RECOVERY: tenant FAILED events cleared | tenant_id={state['tenant_id']}"


def _build_tenant_retry_problem_message(state: dict) -> str:
    return (
        "ALERT: tenant RETRY events detected"
        f" | tenant_id={state['tenant_id']}"
        f" | retry_events_count={state['retry_events_count']}"
    )


def _build_tenant_retry_recovery_message(state: dict) -> str:
    return f"RECOVERY: tenant RETRY events cleared | tenant_id={state['tenant_id']}"


def _build_tenant_stock_problem_message(state: dict) -> str:
    parts = [
        "ALERT: tenant stock sync error",
        f"tenant_id={state['tenant_id']}",
    ]
    if state["total_items_count"] > 0:
        parts.append(
            f"synced_items={state['synced_items_count']}/{state['total_items_count']}"
        )
    if state["stock_last_error"]:
        parts.append(f"summary={state['stock_last_error']}")
    return " | ".join(parts)


def _build_tenant_stock_recovery_message(state: dict) -> str:
    return f"RECOVERY: tenant stock sync ok | tenant_id={state['tenant_id']}"


def _build_tenant_alert_messages(previous_state: dict | None, current_state: dict) -> list[str]:
    previous_state = previous_state or _empty_tenant_alert_state(current_state["tenant_id"])
    messages: list[str] = []

    previous_failed_present = previous_state["failed_events_count"] > 0
    current_failed_present = current_state["failed_events_count"] > 0
    if not previous_failed_present and current_failed_present:
        messages.append(_build_tenant_failed_problem_message(current_state))
    elif previous_failed_present and not current_failed_present:
        messages.append(_build_tenant_failed_recovery_message(current_state))

    previous_retry_present = previous_state["retry_events_count"] > 0
    current_retry_present = current_state["retry_events_count"] > 0
    if not previous_retry_present and current_retry_present:
        messages.append(_build_tenant_retry_problem_message(current_state))
    elif previous_retry_present and not current_retry_present:
        messages.append(_build_tenant_retry_recovery_message(current_state))

    previous_stock_error_present = bool(previous_state["stock_error_present"])
    current_stock_error_present = bool(current_state["stock_error_present"])
    if not previous_stock_error_present and current_stock_error_present:
        messages.append(_build_tenant_stock_problem_message(current_state))
    elif previous_stock_error_present and not current_stock_error_present:
        messages.append(_build_tenant_stock_recovery_message(current_state))

    return messages


def _deliver_system_message(
    message: str,
    *,
    telegram_client: TelegramClient | None,
    email_client: EmailClient | None,
) -> bool:
    if telegram_client is None and email_client is None:
        log.info("system alert skipped: no global channels configured")
        return True

    subject = _build_email_subject(message)
    event_type = _infer_system_event_type(message)
    message_delivered = False

    if telegram_client is not None:
        try:
            telegram_client.send_message(message)
            _write_notification_log(
                tenant_id=None,
                channel_type="telegram",
                destination=telegram_client.chat_id,
                event_type=event_type,
                message=message,
                status="sent",
            )
            message_delivered = True
            log.info(
                "telegram alert sent",
                extra=_alert_extra(
                    component="telegram",
                    operation="alert_worker.deliver",
                    status="sent",
                ),
            )
        except Exception as exc:
            _write_notification_log(
                tenant_id=None,
                channel_type="telegram",
                destination=telegram_client.chat_id,
                event_type=event_type,
                message=message,
                status="failed",
                error_message=_shorten_text(f"{type(exc).__name__}: {exc}", limit=300),
            )
            log.exception(
                "telegram alert failed",
                extra=_alert_extra(
                    component="telegram",
                    operation="alert_worker.deliver",
                    status="failed",
                ),
            )

    if email_client is not None:
        try:
            email_client.send_message(subject=subject, text=message)
            _write_notification_log(
                tenant_id=None,
                channel_type="email",
                destination=",".join(email_client.to_addresses),
                event_type=event_type,
                message=message,
                status="sent",
            )
            message_delivered = True
            log.info(
                "email alert sent",
                extra=_alert_extra(
                    component="email",
                    operation="alert_worker.deliver",
                    status="sent",
                ),
            )
        except Exception as exc:
            _write_notification_log(
                tenant_id=None,
                channel_type="email",
                destination=",".join(email_client.to_addresses) or "<missing>",
                event_type=event_type,
                message=message,
                status="failed",
                error_message=_shorten_text(f"{type(exc).__name__}: {exc}", limit=300),
            )
            log.exception(
                "email alert failed",
                extra=_alert_extra(
                    component="email",
                    operation="alert_worker.deliver",
                    status="failed",
                ),
            )

    return message_delivered


def _deliver_tenant_message(
    tenant_state: dict,
    message: str,
    *,
    telegram_bot_token: str,
    email_transport_config: dict | None,
) -> bool:
    subject = _build_email_subject(message)
    event_type = _infer_tenant_event_type(message)
    had_requested_channel = False
    had_failed_channel = False

    if tenant_state["alerts_telegram_requested"]:
        had_requested_channel = True
        if not tenant_state["telegram_chat_id"]:
            _write_notification_log(
                tenant_id=tenant_state["tenant_id"],
                channel_type="telegram",
                destination="<missing>",
                event_type=event_type,
                message=message,
                status="skipped",
                error_message="Tenant Telegram channel enabled without chat_id",
            )
        elif not telegram_bot_token:
            _write_notification_log(
                tenant_id=tenant_state["tenant_id"],
                channel_type="telegram",
                destination=tenant_state["telegram_chat_id"],
                event_type=event_type,
                message=message,
                status="failed",
                error_message="TELEGRAM_BOT_TOKEN not configured",
            )
            had_failed_channel = True
            log.warning("tenant telegram alert skipped: TELEGRAM_BOT_TOKEN not configured tenant_id=%s", tenant_state["tenant_id"])
        else:
            telegram_client = _build_tenant_telegram_client(tenant_state["telegram_chat_id"])
            if telegram_client is None:
                _write_notification_log(
                    tenant_id=tenant_state["tenant_id"],
                    channel_type="telegram",
                    destination=tenant_state["telegram_chat_id"],
                    event_type=event_type,
                    message=message,
                    status="failed",
                    error_message="Invalid tenant Telegram configuration",
                )
                had_failed_channel = True
                log.warning("tenant telegram alert skipped: invalid tenant telegram config tenant_id=%s", tenant_state["tenant_id"])
            else:
                try:
                    telegram_client.send_message(message)
                    _write_notification_log(
                        tenant_id=tenant_state["tenant_id"],
                        channel_type="telegram",
                        destination=tenant_state["telegram_chat_id"],
                        event_type=event_type,
                        message=message,
                        status="sent",
                    )
                    log.info("tenant telegram alert sent tenant_id=%s", tenant_state["tenant_id"])
                except Exception as exc:
                    _write_notification_log(
                        tenant_id=tenant_state["tenant_id"],
                        channel_type="telegram",
                        destination=tenant_state["telegram_chat_id"],
                        event_type=event_type,
                        message=message,
                        status="failed",
                        error_message=_shorten_text(f"{type(exc).__name__}: {exc}", limit=300),
                    )
                    had_failed_channel = True
                    log.exception("tenant telegram alert failed tenant_id=%s", tenant_state["tenant_id"])

    if tenant_state["alerts_email_requested"]:
        had_requested_channel = True
        if not tenant_state["alert_email"]:
            _write_notification_log(
                tenant_id=tenant_state["tenant_id"],
                channel_type="email",
                destination="<missing>",
                event_type=event_type,
                message=message,
                status="skipped",
                error_message="Tenant email channel enabled without alert_email",
            )
        elif email_transport_config is None:
            _write_notification_log(
                tenant_id=tenant_state["tenant_id"],
                channel_type="email",
                destination=tenant_state["alert_email"],
                event_type=event_type,
                message=message,
                status="failed",
                error_message="SMTP transport not configured",
            )
            had_failed_channel = True
            log.warning("tenant email alert skipped: SMTP transport not configured tenant_id=%s", tenant_state["tenant_id"])
        else:
            email_client = _build_tenant_email_client(email_transport_config, tenant_state["alert_email"])
            if email_client is None:
                _write_notification_log(
                    tenant_id=tenant_state["tenant_id"],
                    channel_type="email",
                    destination=tenant_state["alert_email"],
                    event_type=event_type,
                    message=message,
                    status="failed",
                    error_message="Invalid tenant email configuration",
                )
                had_failed_channel = True
                log.warning("tenant email alert skipped: invalid tenant email config tenant_id=%s", tenant_state["tenant_id"])
            else:
                try:
                    email_client.send_message(subject=subject, text=message)
                    _write_notification_log(
                        tenant_id=tenant_state["tenant_id"],
                        channel_type="email",
                        destination=tenant_state["alert_email"],
                        event_type=event_type,
                        message=message,
                        status="sent",
                    )
                    log.info("tenant email alert sent tenant_id=%s", tenant_state["tenant_id"])
                except Exception as exc:
                    _write_notification_log(
                        tenant_id=tenant_state["tenant_id"],
                        channel_type="email",
                        destination=tenant_state["alert_email"],
                        event_type=event_type,
                        message=message,
                        status="failed",
                        error_message=_shorten_text(f"{type(exc).__name__}: {exc}", limit=300),
                    )
                    had_failed_channel = True
                    log.exception("tenant email alert failed tenant_id=%s", tenant_state["tenant_id"])

    if not had_requested_channel:
        log.info("tenant alert skipped: no tenant channels configured tenant_id=%s", tenant_state["tenant_id"])
        return True

    return not had_failed_channel


def main_loop():
    log.info(
        "alert worker started",
        extra=_alert_extra(
            component="alert_worker",
            operation="alert_worker.main_loop",
            status="started",
        ),
    )

    telegram_bot_token = _get_telegram_bot_token()
    email_transport_config = _get_email_transport_config()
    system_telegram_client = _build_system_telegram_client()
    system_email_client = _build_system_email_client(email_transport_config)

    if (
        system_telegram_client is None
        and system_email_client is None
        and not telegram_bot_token
        and email_transport_config is None
    ):
        log.warning(
            "alert worker has no delivery transports configured",
            extra=_alert_extra(
                component="alert_worker",
                operation="alert_worker.main_loop",
                status="no_channels",
            ),
        )

    previous_snapshot = None
    previous_tenant_snapshot: dict[str, dict] | None = None

    while not _shutdown:
        try:
            current_snapshot = _collect_snapshot()
            current_tenant_snapshot = _collect_tenant_alert_snapshot()

            if previous_snapshot is None:
                previous_snapshot = current_snapshot
                previous_tenant_snapshot = current_tenant_snapshot
                log.info(
                    "alert baseline set",
                    extra=_alert_extra(
                        component="alert_worker",
                        operation="alert_worker.main_loop",
                        status="baseline_set",
                    ),
                )
            else:
                messages = build_alert_messages(previous_snapshot, current_snapshot)

                if not messages:
                    previous_snapshot = current_snapshot
                else:
                    all_messages_delivered = True

                    for message in messages:
                        if not _deliver_system_message(
                            message,
                            telegram_client=system_telegram_client,
                            email_client=system_email_client,
                        ):
                            all_messages_delivered = False

                    if all_messages_delivered:
                        previous_snapshot = current_snapshot
                    else:
                        log.warning(
                            "system alert state not advanced",
                            extra=_alert_extra(
                                component="alert_worker",
                                operation="alert_worker.main_loop",
                                status="delivery_incomplete",
                            ),
                        )

                next_tenant_snapshot = dict(previous_tenant_snapshot or {})
                tenant_ids = sorted(
                    set(next_tenant_snapshot.keys()) | set(current_tenant_snapshot.keys())
                )

                for tenant_id in tenant_ids:
                    previous_tenant_state = (previous_tenant_snapshot or {}).get(tenant_id)
                    current_tenant_state = current_tenant_snapshot.get(
                        tenant_id,
                        _empty_tenant_alert_state(tenant_id),
                    )
                    tenant_messages = _build_tenant_alert_messages(
                        previous_tenant_state,
                        current_tenant_state,
                    )

                    if not tenant_messages:
                        next_tenant_snapshot[tenant_id] = current_tenant_state
                        continue

                    tenant_messages_delivered = True
                    for message in tenant_messages:
                        if not _deliver_tenant_message(
                            current_tenant_state,
                            message,
                            telegram_bot_token=telegram_bot_token,
                            email_transport_config=email_transport_config,
                        ):
                            tenant_messages_delivered = False

                    if tenant_messages_delivered:
                        next_tenant_snapshot[tenant_id] = current_tenant_state
                    else:
                        log.warning("tenant alert state not advanced tenant_id=%s", tenant_id)

                previous_tenant_snapshot = {
                    tenant_id: state
                    for tenant_id, state in next_tenant_snapshot.items()
                    if tenant_id in current_tenant_snapshot
                }

        except Exception:
            log.exception(
                "alert worker cycle failed",
                extra=_alert_extra(
                    component="alert_worker",
                    operation="alert_worker.main_loop",
                    status="failed",
                ),
            )

        if _shutdown:
            break

        time.sleep(ALERT_POLL_INTERVAL_SEC)

    log.info(
        "alert worker stopped gracefully",
        extra=_alert_extra(
            component="alert_worker",
            operation="alert_worker.main_loop",
            status="stopped",
        ),
    )


if __name__ == "__main__":
    main_loop()
