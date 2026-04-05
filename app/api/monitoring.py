import html
import os
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

from app.db import get_connection, adapt_query as aq

router = APIRouter(tags=["Monitoring"])

SERVICE_NAME = "integration-bus"
WORKER_HEARTBEAT_NAME = "worker"
WORKER_STALE_AFTER_SEC = int(os.getenv("WORKER_STALE_AFTER_SEC", "30"))
PROBLEM_EVENTS_LIMIT = 10
ERRORS_LIMIT = 10
LATENCY_SAMPLE_SIZE = 20


def _format_ts(ts: int | None) -> str:
    if ts is None:
        return "-"
    return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")


def _worker_status(last_seen_at: int | None, now_ts: int) -> str:
    if last_seen_at is None:
        return "stale"
    if (now_ts - int(last_seen_at)) > WORKER_STALE_AFTER_SEC:
        return "stale"
    return "ok"


def _load_dashboard_snapshot() -> dict:
    now_ts = int(time.time())
    conn = None

    try:
        conn = get_connection()
        cur = conn.cursor()

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
        worker_status = _worker_status(worker_last_seen_at, now_ts)

        cur.execute(
            """
            SELECT status, COUNT(*) AS cnt
            FROM event_store
            GROUP BY status
            """
        )
        event_counts = {
            "NEW": 0,
            "PROCESSING": 0,
            "DONE": 0,
            "RETRY": 0,
            "FAILED": 0,
        }
        for row in cur.fetchall():
            event_counts[row["status"]] = row["cnt"]

        cur.execute(
            aq("""
            SELECT
                id, tenant_id, event_type, event_key,
                status, retries, next_retry_at,
                last_error_message, created_at, updated_at
            FROM event_store
            WHERE status IN ('RETRY', 'FAILED')
            ORDER BY updated_at DESC
            LIMIT ?
            """),
            (PROBLEM_EVENTS_LIMIT,),
        )
        problem_events = [dict(row) for row in cur.fetchall()]

        cur.execute(
            aq("""
            SELECT id, event_id, tenant_id, error_code, message, created_at
            FROM errors
            ORDER BY created_at DESC
            LIMIT ?
            """),
            (ERRORS_LIMIT,),
        )
        errors = [dict(row) for row in cur.fetchall()]

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

        cur.execute(
            aq("""
            SELECT id, tenant_id, event_key, created_at, updated_at
            FROM event_store
            WHERE status = 'DONE'
            ORDER BY updated_at DESC
            LIMIT ?
            """),
            (LATENCY_SAMPLE_SIZE,),
        )
        done_rows = [dict(row) for row in cur.fetchall()]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build dashboard: {e}")
    finally:
        if conn is not None:
            conn.close()

    latencies = [
        max(0, int(row["updated_at"]) - int(row["created_at"]))
        for row in done_rows
        if row["created_at"] is not None and row["updated_at"] is not None
    ]

    latency = {
        "sample_size": len(latencies),
        "avg_latency_sec": round(sum(latencies) / len(latencies), 2) if latencies else None,
        "max_latency_sec": max(latencies) if latencies else None,
        "last_latency_sec": latencies[0] if latencies else None,
        "last_done_event": None,
    }

    if done_rows and latencies:
        last_done = dict(done_rows[0])
        last_done["latency_sec"] = latencies[0]
        latency["last_done_event"] = last_done

    overall_status = "ok"
    if (
        worker_status != "ok"
        or event_counts["FAILED"] > 0
        or event_counts["RETRY"] > 0
        or stock_error_count > 0
    ):
        overall_status = "degraded"

    return {
        "status": overall_status,
        "service": SERVICE_NAME,
        "timestamp": now_ts,
        "worker": {
            "status": worker_status,
            "last_seen_at": worker_last_seen_at,
            "stale_after_sec": WORKER_STALE_AFTER_SEC,
        },
        "events": {
            "counts": event_counts,
            "problem_events": problem_events,
        },
        "errors": errors,
        "latency": latency,
        "stock_sync": {
            "tenants_with_error": stock_error_count,
            "last_sync_at": stock_last_sync_at,
        },
    }


def _render_table(headers: list[str], rows: list[list[str]]) -> str:
    head_html = "".join(f"<th>{html.escape(header)}</th>" for header in headers)

    if not rows:
        return (
            "<table>"
            f"<thead><tr>{head_html}</tr></thead>"
            "<tbody><tr><td colspan='100%'>No data</td></tr></tbody>"
            "</table>"
        )

    body_html = []
    for row in rows:
        cells = "".join(f"<td>{html.escape(str(cell))}</td>" for cell in row)
        body_html.append(f"<tr>{cells}</tr>")

    return (
        "<table>"
        f"<thead><tr>{head_html}</tr></thead>"
        f"<tbody>{''.join(body_html)}</tbody>"
        "</table>"
    )


@router.get("/monitoring/dashboard")
def monitoring_dashboard():
    return _load_dashboard_snapshot()


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    snapshot = _load_dashboard_snapshot()

    counts = snapshot["events"]["counts"]
    worker = snapshot["worker"]
    latency = snapshot["latency"]

    cards = [
        ("NEW", counts["NEW"]),
        ("PROCESSING", counts["PROCESSING"]),
        ("DONE", counts["DONE"]),
        ("RETRY", counts["RETRY"]),
        ("FAILED", counts["FAILED"]),
        ("Worker", worker["status"]),
        ("Stock sync errors", snapshot["stock_sync"]["tenants_with_error"]),
        (
            "Avg latency, sec",
            latency["avg_latency_sec"] if latency["avg_latency_sec"] is not None else "-",
        ),
        (
            "Max latency, sec",
            latency["max_latency_sec"] if latency["max_latency_sec"] is not None else "-",
        ),
        (
            "Last latency, sec",
            latency["last_latency_sec"] if latency["last_latency_sec"] is not None else "-",
        ),
    ]

    cards_html = "".join(
        (
            "<div class='card'>"
            f"<div class='label'>{html.escape(str(label))}</div>"
            f"<div class='value'>{html.escape(str(value))}</div>"
            "</div>"
        )
        for label, value in cards
    )

    problem_rows = [
        [
            row["id"],
            row["tenant_id"],
            row["event_type"],
            row["event_key"],
            row["status"],
            row["retries"],
            row["last_error_message"] or "-",
            _format_ts(row["updated_at"]),
        ]
        for row in snapshot["events"]["problem_events"]
    ]

    error_rows = [
        [
            row["event_id"],
            row["tenant_id"],
            row["error_code"] or "-",
            row["message"],
            _format_ts(row["created_at"]),
        ]
        for row in snapshot["errors"]
    ]

    html_page = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <meta http-equiv="refresh" content="10">
        <title>Integration Bus Dashboard</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 24px;
                background: #f5f7fb;
                color: #172033;
            }}
            h1, h2 {{
                margin-bottom: 12px;
            }}
            .meta {{
                margin-bottom: 20px;
                color: #5b6475;
            }}
            .cards {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 12px;
                margin-bottom: 24px;
            }}
            .card {{
                background: #ffffff;
                border: 1px solid #d8deea;
                border-radius: 8px;
                padding: 12px;
            }}
            .label {{
                font-size: 12px;
                color: #5b6475;
                margin-bottom: 6px;
                text-transform: uppercase;
            }}
            .value {{
                font-size: 24px;
                font-weight: 700;
            }}
            .section {{
                margin-bottom: 24px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background: #ffffff;
                border: 1px solid #d8deea;
            }}
            th, td {{
                text-align: left;
                padding: 10px 12px;
                border-bottom: 1px solid #e8edf5;
                vertical-align: top;
                font-size: 14px;
            }}
            th {{
                background: #eef3fb;
            }}
            .links {{
                margin-top: 8px;
            }}
            a {{
                color: #2458d3;
                text-decoration: none;
            }}
        </style>
    </head>
    <body>
        <h1>Integration Bus Dashboard</h1>
        <div class="meta">
            Status: {html.escape(snapshot["status"])} |
            Updated: {html.escape(_format_ts(snapshot["timestamp"]))} |
            Worker last seen: {html.escape(_format_ts(worker["last_seen_at"]))}
        </div>

        <div class="cards">{cards_html}</div>

        <div class="section">
            <h2>Problem Events</h2>
            {_render_table(
                ["Event ID", "Tenant", "Type", "Event Key", "Status", "Retries", "Last Error", "Updated At"],
                problem_rows,
            )}
        </div>

        <div class="section">
            <h2>Recent Errors</h2>
            {_render_table(
                ["Event ID", "Tenant", "Code", "Message", "Created At"],
                error_rows,
            )}
        </div>

        <div class="links">
            <a href="/monitoring/dashboard">Open JSON snapshot</a>
        </div>
    </body>
    </html>
    """

    return HTMLResponse(content=html_page)