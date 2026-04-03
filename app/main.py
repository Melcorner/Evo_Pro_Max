from dotenv import load_dotenv

load_dotenv()

import logging
import os
import time

from fastapi import Depends, FastAPI

from app.logger import setup_logging
from app.db import get_connection
from app.security import require_admin_api_token

from app.api.tenants import router as tenants_router
from app.api.webhooks import router as webhooks_router
from app.api.events import router as events_router
from app.api.mappings import router as mappings_router
from app.api.errors import router as errors_router
from app.api.evotor import router as evotor_router
from app.api.sync import router as sync_router
from app.api.moysklad_webhooks import router as moysklad_webhooks_router
from app.api.monitoring import router as monitoring_router
from app.api.onboarding import router as onboarding_router

setup_logging()
log = logging.getLogger("api")

SERVICE_NAME = "integration-bus"
WORKER_HEARTBEAT_NAME = "worker"
WORKER_STALE_AFTER_SEC = int(os.getenv("WORKER_STALE_AFTER_SEC", "30"))

openapi_tags = [
    {"name": "Infrastructure", "description": "Служебные endpoint'ы приложения"},
    {"name": "Tenants", "description": "Tenant'ы и конфигурация интеграции"},
    {"name": "Sync", "description": "Синхронизация товаров, остатков и фискализации"},
    {"name": "Evotor Webhooks", "description": "Webhook'и от Эвотор"},
    {"name": "MoySklad Webhooks", "description": "Webhook'и от МойСклад"},
    {"name": "Events", "description": "Просмотр и повторная обработка событий"},
    {"name": "Errors", "description": "Журнал ошибок"},
    {"name": "Monitoring", "description": "Дашборд и snapshot мониторинга integration bus"},
    {"name": "Evotor Service", "description": "Служебные callback'и и endpoint'ы Эвотор"},
    {"name": "Mappings", "description": "Маппинги Evotor ↔ MoySklad"},
]

app = FastAPI(
    title="Evotor ↔ MoySklad Integration Bus",
    openapi_tags=openapi_tags,
)

admin_dependencies = [Depends(require_admin_api_token)]

# Protected admin/internal API
app.include_router(sync_router, dependencies=admin_dependencies)
app.include_router(tenants_router, dependencies=admin_dependencies)
app.include_router(events_router, dependencies=admin_dependencies)
app.include_router(mappings_router, dependencies=admin_dependencies)
app.include_router(errors_router, dependencies=admin_dependencies)
app.include_router(monitoring_router, dependencies=admin_dependencies)
app.include_router(onboarding_router)

# Public / external callbacks
app.include_router(webhooks_router)
app.include_router(evotor_router)
app.include_router(moysklad_webhooks_router)


def _health_error_response(now_ts: int, detail: str) -> dict:
    return {
        "status": "error",
        "service": SERVICE_NAME,
        "timestamp": now_ts,
        "checks": {
            "api": {"status": "ok"},
            "db": {"status": "error", "detail": detail},
            "worker": {
                "status": "unknown",
                "last_seen_at": None,
                "stale_after_sec": WORKER_STALE_AFTER_SEC,
            },
        },
        "events": {
            "new": None,
            "retry": None,
            "failed": None,
            "processing": None,
            "last_processed_at": None,
        },
        "stock_sync": {
            "tenants_with_error": None,
            "last_sync_at": None,
        },
    }


@app.get("/health", tags=["Infrastructure"])
def health():
    now_ts = int(time.time())

    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT 1 AS ok")
        cur.fetchone()

        cur.execute(
            """
            SELECT service_name, last_seen_at
            FROM service_heartbeats
            WHERE service_name = ?
            """,
            (WORKER_HEARTBEAT_NAME,),
        )
        heartbeat_row = cur.fetchone()

        worker_last_seen_at = heartbeat_row["last_seen_at"] if heartbeat_row else None
        worker_is_stale = (
            worker_last_seen_at is None
            or (now_ts - int(worker_last_seen_at)) > WORKER_STALE_AFTER_SEC
        )
        worker_status = "stale" if worker_is_stale else "ok"

        cur.execute(
            """
            SELECT status, COUNT(*) AS cnt
            FROM event_store
            GROUP BY status
            """
        )
        event_counts = {"NEW": 0, "RETRY": 0, "FAILED": 0, "PROCESSING": 0}
        for row in cur.fetchall():
            event_counts[row["status"]] = row["cnt"]

        cur.execute("SELECT MAX(processed_at) AS last_processed_at FROM processed_events")
        last_processed_row = cur.fetchone()
        last_processed_at = last_processed_row["last_processed_at"] if last_processed_row else None

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM stock_sync_status
            WHERE status = 'error'
            """
        )
        stock_error_row = cur.fetchone()
        stock_error_count = stock_error_row["cnt"] if stock_error_row else 0

        cur.execute("SELECT MAX(last_sync_at) AS last_sync_at FROM stock_sync_status")
        stock_last_sync_row = cur.fetchone()
        stock_last_sync_at = stock_last_sync_row["last_sync_at"] if stock_last_sync_row else None

    except Exception as e:
        log.exception("Health check failed: %s", e)
        return _health_error_response(now_ts, str(e))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    overall_status = "ok"
    if (
        worker_is_stale
        or event_counts["FAILED"] > 0
        or event_counts["RETRY"] > 0
        or stock_error_count > 0
    ):
        overall_status = "degraded"

    return {
        "status": overall_status,
        "service": SERVICE_NAME,
        "timestamp": now_ts,
        "checks": {
            "api": {"status": "ok"},
            "db": {"status": "ok"},
            "worker": {
                "status": worker_status,
                "last_seen_at": worker_last_seen_at,
                "stale_after_sec": WORKER_STALE_AFTER_SEC,
            },
        },
        "events": {
            "new": event_counts["NEW"],
            "retry": event_counts["RETRY"],
            "failed": event_counts["FAILED"],
            "processing": event_counts["PROCESSING"],
            "last_processed_at": last_processed_at,
        },
        "stock_sync": {
            "tenants_with_error": stock_error_count,
            "last_sync_at": stock_last_sync_at,
        },
    }
