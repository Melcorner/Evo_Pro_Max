from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class AlertSnapshot:
    service_name: str
    worker_problem: bool
    worker_status: str
    worker_last_seen_at: int | None
    failed_events_present: bool
    failed_events_count: int
    retry_events_present: bool
    retry_events_count: int
    stock_sync_errors_present: bool
    stock_sync_errors_count: int


def _format_ts(ts: int | None) -> str:
    if ts is None:
        return "-"
    return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")


def build_alert_snapshot(
    service_name: str,
    now_ts: int,
    worker_last_seen_at: int | None,
    stale_after_sec: int,
    failed_events_count: int,
    retry_events_count: int,
    stock_sync_errors_count: int,
) -> AlertSnapshot:
    if worker_last_seen_at is None:
        worker_status = "stale"
        worker_problem = True
    elif (now_ts - int(worker_last_seen_at)) > stale_after_sec:
        worker_status = "stale"
        worker_problem = True
    else:
        worker_status = "ok"
        worker_problem = False

    failed_count = max(int(failed_events_count or 0), 0)
    retry_count = max(int(retry_events_count or 0), 0)
    stock_error_count = max(int(stock_sync_errors_count or 0), 0)

    return AlertSnapshot(
        service_name=service_name,
        worker_problem=worker_problem,
        worker_status=worker_status,
        worker_last_seen_at=worker_last_seen_at,
        failed_events_present=failed_count > 0,
        failed_events_count=failed_count,
        retry_events_present=retry_count > 0,
        retry_events_count=retry_count,
        stock_sync_errors_present=stock_error_count > 0,
        stock_sync_errors_count=stock_error_count,
    )


def format_worker_problem_message(snapshot: AlertSnapshot) -> str:
    parts = [
        f"ALERT: {snapshot.service_name} worker problem",
        f"status={snapshot.worker_status}",
    ]
    if snapshot.worker_last_seen_at is not None:
        parts.append(f"last_seen_at={_format_ts(snapshot.worker_last_seen_at)}")
    return " | ".join(parts)


def format_worker_recovery_message(snapshot: AlertSnapshot) -> str:
    parts = [
        f"RECOVERY: {snapshot.service_name} worker ok",
        f"status={snapshot.worker_status}",
    ]
    if snapshot.worker_last_seen_at is not None:
        parts.append(f"last_seen_at={_format_ts(snapshot.worker_last_seen_at)}")
    return " | ".join(parts)


def format_failed_events_problem_message(snapshot: AlertSnapshot) -> str:
    return (
        f"ALERT: {snapshot.service_name} FAILED events detected"
        f" | failed_events_count={snapshot.failed_events_count}"
    )


def format_failed_events_recovery_message(snapshot: AlertSnapshot) -> str:
    return (
        f"RECOVERY: {snapshot.service_name} FAILED events cleared"
        f" | failed_events_count={snapshot.failed_events_count}"
    )


def format_retry_events_problem_message(snapshot: AlertSnapshot) -> str:
    return (
        f"ALERT: {snapshot.service_name} RETRY events detected"
        f" | retry_events_count={snapshot.retry_events_count}"
    )


def format_retry_events_recovery_message(snapshot: AlertSnapshot) -> str:
    return (
        f"RECOVERY: {snapshot.service_name} RETRY events cleared"
        f" | retry_events_count={snapshot.retry_events_count}"
    )


def format_stock_sync_errors_problem_message(snapshot: AlertSnapshot) -> str:
    return (
        f"ALERT: {snapshot.service_name} stock sync errors detected"
        f" | stock_sync_errors_count={snapshot.stock_sync_errors_count}"
    )


def format_stock_sync_errors_recovery_message(snapshot: AlertSnapshot) -> str:
    return (
        f"RECOVERY: {snapshot.service_name} stock sync errors cleared"
        f" | stock_sync_errors_count={snapshot.stock_sync_errors_count}"
    )


def build_alert_messages(
    previous_snapshot: AlertSnapshot | None,
    current_snapshot: AlertSnapshot,
) -> list[str]:
    if previous_snapshot is None:
        return []

    messages: list[str] = []

    if not previous_snapshot.worker_problem and current_snapshot.worker_problem:
        messages.append(format_worker_problem_message(current_snapshot))
    elif previous_snapshot.worker_problem and not current_snapshot.worker_problem:
        messages.append(format_worker_recovery_message(current_snapshot))

    if not previous_snapshot.failed_events_present and current_snapshot.failed_events_present:
        messages.append(format_failed_events_problem_message(current_snapshot))
    elif previous_snapshot.failed_events_present and not current_snapshot.failed_events_present:
        messages.append(format_failed_events_recovery_message(current_snapshot))

    if not previous_snapshot.retry_events_present and current_snapshot.retry_events_present:
        messages.append(format_retry_events_problem_message(current_snapshot))
    elif previous_snapshot.retry_events_present and not current_snapshot.retry_events_present:
        messages.append(format_retry_events_recovery_message(current_snapshot))

    if not previous_snapshot.stock_sync_errors_present and current_snapshot.stock_sync_errors_present:
        messages.append(format_stock_sync_errors_problem_message(current_snapshot))
    elif previous_snapshot.stock_sync_errors_present and not current_snapshot.stock_sync_errors_present:
        messages.append(format_stock_sync_errors_recovery_message(current_snapshot))

    return messages