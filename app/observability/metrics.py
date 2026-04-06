from __future__ import annotations

import time
from contextlib import contextmanager

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    start_http_server,
)

API_REGISTRY = CollectorRegistry(auto_describe=True)
WORKER_REGISTRY = CollectorRegistry(auto_describe=True)
FISCAL_POLLER_REGISTRY = CollectorRegistry(auto_describe=True)

# ---------------- API ----------------

api_requests_total = Counter(
    "integration_api_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
    registry=API_REGISTRY,
)

api_request_duration_seconds = Histogram(
    "integration_api_request_duration_seconds",
    "HTTP request duration",
    ["method", "path"],
    registry=API_REGISTRY,
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)

api_exceptions_total = Counter(
    "integration_api_exceptions_total",
    "Unhandled API exceptions",
    ["method", "path", "exception_type"],
    registry=API_REGISTRY,
)

# ---------------- Worker ----------------

worker_cycles_total = Counter(
    "integration_worker_cycles_total",
    "Worker loop cycles",
    registry=WORKER_REGISTRY,
)

worker_idle_cycles_total = Counter(
    "integration_worker_idle_cycles_total",
    "Worker idle loop cycles",
    registry=WORKER_REGISTRY,
)

worker_events_picked_total = Counter(
    "integration_worker_events_picked_total",
    "Events picked by worker",
    registry=WORKER_REGISTRY,
)

worker_events_processed_total = Counter(
    "integration_worker_events_processed_total",
    "Events processed by worker",
    ["result"],
    registry=WORKER_REGISTRY,
)

worker_processing_duration_seconds = Histogram(
    "integration_worker_processing_duration_seconds",
    "Single event processing duration",
    registry=WORKER_REGISTRY,
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)

worker_last_heartbeat_unixtime = Gauge(
    "integration_worker_last_heartbeat_unixtime",
    "Last worker heartbeat Unix timestamp",
    registry=WORKER_REGISTRY,
)

worker_stale_recovered_total = Counter(
    "integration_worker_stale_recovered_total",
    "Recovered stale PROCESSING events",
    ["result"],
    registry=WORKER_REGISTRY,
)

# ---------------- Fiscal poller ----------------

fiscal_poller_cycles_total = Counter(
    "integration_fiscal_poller_cycles_total",
    "Fiscal poller cycles",
    registry=FISCAL_POLLER_REGISTRY,
)

fiscal_poller_pending_checks = Gauge(
    "integration_fiscal_poller_pending_checks",
    "Pending fiscalization checks available for polling",
    registry=FISCAL_POLLER_REGISTRY,
)

fiscal_poller_polled_total = Counter(
    "integration_fiscal_poller_polled_total",
    "Fiscal checks polled",
    ["result"],
    registry=FISCAL_POLLER_REGISTRY,
)

fiscal_poller_poll_duration_seconds = Histogram(
    "integration_fiscal_poller_poll_duration_seconds",
    "Fiscal poll duration per check",
    registry=FISCAL_POLLER_REGISTRY,
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)

# ---------------- Fiscalization API ----------------

fiscalization_requests_total = Counter(
    "integration_fiscalization_requests_total",
    "Fiscalization requests",
    ["result"],
    registry=API_REGISTRY,
)

fiscalization_request_duration_seconds = Histogram(
    "integration_fiscalization_request_duration_seconds",
    "Fiscalization request duration",
    registry=API_REGISTRY,
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)

fiscalization_status_checks_total = Counter(
    "integration_fiscalization_status_checks_total",
    "Fiscalization status checks",
    ["result"],
    registry=API_REGISTRY,
)

fiscalization_state_total = Counter(
    "integration_fiscalization_state_total",
    "Observed fiscalization states",
    ["state_label"],
    registry=API_REGISTRY,
)

# ---------------- DB-derived gauges ----------------

event_store_status_count = Gauge(
    "integration_event_store_status_count",
    "Event store rows by status",
    ["status"],
    registry=API_REGISTRY,
)

errors_count = Gauge(
    "integration_errors_count",
    "Total errors rows",
    registry=API_REGISTRY,
)

stock_sync_error_tenants = Gauge(
    "integration_stock_sync_error_tenants",
    "Tenants with stock sync error",
    registry=API_REGISTRY,
)


def _start_metrics_http_server(
    registry: CollectorRegistry,
    port: int,
    host: str = "0.0.0.0",
) -> None:
    start_http_server(port, addr=host, registry=registry)


def start_worker_metrics_server(port: int, host: str = "0.0.0.0") -> None:
    _start_metrics_http_server(WORKER_REGISTRY, port=port, host=host)


def start_fiscal_poller_metrics_server(port: int, host: str = "0.0.0.0") -> None:
    _start_metrics_http_server(FISCAL_POLLER_REGISTRY, port=port, host=host)


@contextmanager
def observe_duration(histogram: Histogram):
    start = time.perf_counter()
    try:
        yield
    finally:
        histogram.observe(time.perf_counter() - start)


def metrics_response() -> tuple[bytes, str]:
    payload = generate_latest(API_REGISTRY)
    return payload, CONTENT_TYPE_LATEST
