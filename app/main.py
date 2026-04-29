from dotenv import load_dotenv

load_dotenv()

import logging
import os
import time

from fastapi import Depends, FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles
from starlette.responses import Response as StarletteResponse

from app.observability.metrics import (
    api_exceptions_total,
    api_request_duration_seconds,
    api_requests_total,
    errors_count,
    event_store_status_count,
    metrics_response,
    stock_sync_error_tenants,
)

from app.logger import setup_logging
from app.db import get_connection, adapt_query as aq
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
from app.api.vendor import router as vendor_router
from app.api.stores import router as stores_router

setup_logging()
log = logging.getLogger("api")

SERVICE_NAME = "integration-bus"
WORKER_HEARTBEAT_NAME = "worker"
WORKER_STALE_AFTER_SEC = int(os.getenv("WORKER_STALE_AFTER_SEC", "30"))
EVENT_STORE_METRIC_STATUSES = ("NEW", "PROCESSING", "DONE", "RETRY", "FAILED")

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
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(sync_router, dependencies=admin_dependencies)
app.include_router(tenants_router, dependencies=admin_dependencies)
app.include_router(stores_router, dependencies=admin_dependencies)
app.include_router(events_router, dependencies=admin_dependencies)
app.include_router(mappings_router, dependencies=admin_dependencies)
app.include_router(errors_router, dependencies=admin_dependencies)
app.include_router(monitoring_router, dependencies=admin_dependencies)
app.include_router(onboarding_router)

# Public / external callbacks
app.include_router(webhooks_router)
app.include_router(evotor_router)
app.include_router(moysklad_webhooks_router)

app.include_router(vendor_router)

@app.middleware("http")
async def prometheus_http_metrics(request: Request, call_next):
    method = request.method
    start = time.perf_counter()

    try:
        response = await call_next(request)
    except Exception as exc:
        route = request.scope.get("route")
        path = getattr(route, "path", None) or request.url.path

        api_exceptions_total.labels(
            method=method,
            path=path,
            exception_type=exc.__class__.__name__,
        ).inc()

        api_requests_total.labels(
            method=method,
            path=path,
            status="500",
        ).inc()

        api_request_duration_seconds.labels(
            method=method,
            path=path,
        ).observe(time.perf_counter() - start)

        raise

    route = request.scope.get("route")
    path = getattr(route, "path", None) or request.url.path
    status = str(response.status_code)

    api_requests_total.labels(
        method=method,
        path=path,
        status=status,
    ).inc()

    api_request_duration_seconds.labels(
        method=method,
        path=path,
    ).observe(time.perf_counter() - start)

    return response


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


def _refresh_prometheus_db_metrics() -> None:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT status, COUNT(*) AS cnt
            FROM event_store
            GROUP BY status
            """
        )
        counts = {status: 0 for status in EVENT_STORE_METRIC_STATUSES}
        for row in cur.fetchall():
            status = row["status"]
            if status in counts:
                counts[status] = row["cnt"]

        for status, count in counts.items():
            event_store_status_count.labels(status=status).set(count)

        cur.execute("SELECT COUNT(*) AS cnt FROM errors")
        errors_row = cur.fetchone()
        errors_count.set(errors_row["cnt"] if errors_row else 0)

        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM stock_sync_status
            WHERE status = 'error'
            """
        )
        stock_error_row = cur.fetchone()
        stock_sync_error_tenants.set(stock_error_row["cnt"] if stock_error_row else 0)
    except Exception:
        log.exception("Failed to refresh Prometheus DB gauges")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


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
            aq("""
            SELECT service_name, last_seen_at
            FROM service_heartbeats
            WHERE service_name = ?
            """),
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
            status = row["status"]
            if status in event_counts:
                event_counts[status] = row["cnt"]

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


@app.get("/metrics", tags=["Infrastructure"])
def metrics() -> Response:
    _refresh_prometheus_db_metrics()
    payload, content_type = metrics_response()
    return StarletteResponse(content=payload, media_type=content_type)


# ---------------------------------------------------------------------
# LK UI injection: wait overlay + toast notifications
# ---------------------------------------------------------------------
from starlette.responses import Response as _LkUiResponse

_LK_UI_HELPERS_HTML = r"""
<!-- LK injected wait overlay and toast -->
<div id="globalSyncWaitOverlay" style="
    display:none;
    position:fixed;
    inset:0;
    z-index:99999;
    background:rgba(15,23,42,.55);
    backdrop-filter:blur(4px);
    align-items:center;
    justify-content:center;
">
    <div style="
        width:min(420px, calc(100vw - 32px));
        background:#ffffff;
        border-radius:18px;
        box-shadow:0 24px 80px rgba(15,23,42,.28);
        padding:26px 28px;
        text-align:center;
        font-family:Inter, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    ">
        <div style="
            width:46px;
            height:46px;
            margin:0 auto 16px;
            border-radius:999px;
            border:4px solid #e5e7eb;
            border-top-color:#2563eb;
            animation:globalSyncWaitSpin .8s linear infinite;
        "></div>

        <div id="globalSyncWaitTitle" style="font-size:18px;font-weight:700;color:#0f172a;margin-bottom:8px;">
            Выполняем операцию...
        </div>

        <div id="globalSyncWaitText" style="font-size:14px;line-height:1.5;color:#475569;">
            Не закрывайте страницу. Операция может занять до нескольких минут.
        </div>
    </div>
</div>

<div id="lkGlobalToast" style="
    display:none;
    position:fixed;
    right:24px;
    top:24px;
    z-index:100000;
    max-width:min(520px, calc(100vw - 48px));
    border-radius:16px;
    box-shadow:0 20px 60px rgba(15,23,42,.22);
    padding:16px 18px;
    font-family:Inter, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
">
    <div style="display:flex;gap:12px;align-items:flex-start;">
        <div id="lkGlobalToastIcon" style="
            flex:0 0 auto;
            width:28px;
            height:28px;
            border-radius:999px;
            display:flex;
            align-items:center;
            justify-content:center;
            font-weight:800;
            font-size:16px;
        ">✓</div>

        <div style="min-width:0;flex:1;">
            <div id="lkGlobalToastTitle" style="font-size:15px;font-weight:800;margin-bottom:4px;">Готово</div>
            <div id="lkGlobalToastText" style="font-size:14px;line-height:1.45;word-break:break-word;"></div>
        </div>

        <button type="button" id="lkGlobalToastClose" style="
            border:0;background:transparent;cursor:pointer;color:inherit;opacity:.7;
            font-size:20px;line-height:1;padding:0 0 0 8px;
        ">×</button>
    </div>
</div>

<style>
@keyframes globalSyncWaitSpin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
}

@keyframes lkToastSlideIn {
    from { transform: translateY(-10px); opacity: 0; }
    to { transform: translateY(0); opacity: 1; }
}
</style>

<script>
(function () {
    function getWaitMessage(action) {
        action = String(action || '');

        if (action.includes('cleanup-stale-mappings')) {
            return {
                title: 'Очищаем устаревшие связи...',
                text: 'Проверяем товары МойСклад и удаляем только локальные связи, которые больше не актуальны.'
            };
        }

        if (action.includes('product-rollback')) {
            return {
                title: 'Восстанавливаем карточки товаров...',
                text: 'Откатываем карточки из последней точки восстановления. Остатки не перезаписываются.'
            };
        }

        if (action.includes('product-snapshot')) {
            return {
                title: 'Создаём точку восстановления...',
                text: 'Сохраняем текущие карточки товаров Эвотор перед синхронизацией.'
            };
        }

        if (action.includes('sync-ms-to-evotor') || action.includes('ms-to-evotor')) {
            return {
                title: 'Синхронизируем товары...',
                text: 'Выгружаем товары из МойСклад в Эвотор. Не закрывайте страницу.'
            };
        }

        if (action.includes('reconcile')) {
            return {
                title: 'Синхронизируем остатки...',
                text: 'Обновляем остатки товаров в Эвотор по данным МойСклад.'
            };
        }

        if (action.includes('/sync') || action.includes('/initial')) {
            return {
                title: 'Выполняем синхронизацию...',
                text: 'Обрабатываем товары и связи между Эвотор и МойСклад.'
            };
        }

        return {
            title: 'Выполняем операцию...',
            text: 'Не закрывайте страницу. Операция может занять до нескольких минут.'
        };
    }

    function shouldShowOverlay(form) {
        if (!form || !form.action) return false;

        var action = String(form.action);

        return (
            action.includes('/sync') ||
            action.includes('/reconcile') ||
            action.includes('/sync-ms-to-evotor') ||
            action.includes('/ms-to-evotor') ||
            action.includes('/initial') ||
            action.includes('/product-snapshot') ||
            action.includes('/product-rollback') ||
            action.includes('/cleanup-stale-mappings')
        );
    }

    function showOverlay(form) {
        var overlay = document.getElementById('globalSyncWaitOverlay');
        var title = document.getElementById('globalSyncWaitTitle');
        var text = document.getElementById('globalSyncWaitText');

        if (!overlay) return;

        var message = getWaitMessage(form.action);

        if (title) title.textContent = message.title;
        if (text) text.textContent = message.text;

        overlay.style.display = 'flex';

        var button = form.querySelector('button[type="submit"], button:not([type])');
        if (button) {
            button.disabled = true;
            button.textContent = '⏳ Выполняется...';
        }
    }

    function showToast(type, message) {
        var toast = document.getElementById('lkGlobalToast');
        var icon = document.getElementById('lkGlobalToastIcon');
        var title = document.getElementById('lkGlobalToastTitle');
        var text = document.getElementById('lkGlobalToastText');
        var close = document.getElementById('lkGlobalToastClose');

        if (!toast || !text) return;

        var isError = type === 'err';

        toast.style.display = 'block';
        toast.style.animation = 'lkToastSlideIn .22s ease-out';
        toast.style.background = isError ? '#fef2f2' : '#ecfdf5';
        toast.style.border = isError ? '1px solid #fecaca' : '1px solid #bbf7d0';
        toast.style.color = isError ? '#991b1b' : '#065f46';

        if (icon) {
            icon.textContent = isError ? '!' : '✓';
            icon.style.background = isError ? '#fee2e2' : '#d1fae5';
            icon.style.color = isError ? '#b91c1c' : '#047857';
        }

        if (title) title.textContent = isError ? 'Ошибка' : 'Готово';

        text.textContent = message || '';

        function hide() {
            toast.style.display = 'none';
        }

        if (close) close.onclick = hide;

        if (!isError) {
            setTimeout(hide, 8000);
        }
    }

    document.addEventListener('submit', function (event) {
        var form = event.target;

        if (!shouldShowOverlay(form)) return;

        if (form.dataset.submitting === '1') {
            event.preventDefault();
            return false;
        }

        form.dataset.submitting = '1';

        setTimeout(function () {
            showOverlay(form);
        }, 10);
    }, true);

    try {
        var params = new URLSearchParams(window.location.search);
        var ok = params.get('ok') || params.get('msg');
        var err = params.get('err');

        if (ok) showToast('ok', ok);
        else if (err) showToast('err', err);
    } catch (e) {}
})();
</script>

"""


@app.middleware("http")
async def lk_ui_injection_middleware(request, call_next):
    response = await call_next(request)

    path = request.url.path
    if not path.startswith("/onboarding/tenants/"):
        return response

    if response.status_code != 200:
        return response

    body = b""
    async for chunk in response.body_iterator:
        body += chunk

    html = body.decode("utf-8", errors="replace")

    if "<html" not in html.lower():
        return _LkUiResponse(
            content=body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )

    if "globalSyncWaitOverlay" not in html:
        if "</body>" in html:
            html = html.replace("</body>", _LK_UI_HELPERS_HTML + "\n</body>", 1)
        else:
            html = html + _LK_UI_HELPERS_HTML

    headers = dict(response.headers)
    headers.pop("content-length", None)

    return _LkUiResponse(
        content=html,
        status_code=response.status_code,
        headers=headers,
        media_type="text/html",
    )

