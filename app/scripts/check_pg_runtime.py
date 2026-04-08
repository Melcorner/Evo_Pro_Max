"""
app/scripts/check_pg_runtime.py

Smoke / integration check for the real PostgreSQL runtime contour.

Usage:
    python -m app.scripts.check_pg_runtime

Required env:
    DATABASE_URL=postgresql://user:password@host:5432/dbname

Optional env:
    SQLITE_PATH - used only by migrate_to_pg smoke; if not set, a temporary SQLite file is created.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from app import main as api_main
from app.db import (
    adapt_query as aq,
    db_backend,
    get_connection,
    inspect_postgres_dsn_components,
    safe_database_config_for_log,
    validate_database_url,
)
from app.migrations import migrate_fiscal_poller
from app.scripts import init_db as init_db_module
from app.scripts import migrate_to_pg as migrate_to_pg_module
from app.stores.notification_log_store import insert_notification_log, list_notification_log
from app.stores.telegram_link_token_store import (
    create_telegram_link_token,
    get_active_telegram_link_token,
    get_telegram_link_token_by_value,
)
from app.workers import alert_worker, fiscal_poller, worker

log = logging.getLogger("check_pg_runtime")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


@dataclass
class SmokeContext:
    prefix: str
    smoke_tenant_id: str
    migrate_tenant_id: str
    smoke_notification_prefix: str
    migrate_notification_message: str
    migrate_link_token: str
    fiscal_uid: str
    sqlite_temp_dir: Path | None = None


def _new_context() -> SmokeContext:
    prefix = f"pg_runtime_smoke_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    return SmokeContext(
        prefix=prefix,
        smoke_tenant_id=f"{prefix}_tenant",
        migrate_tenant_id=f"{prefix}_migrate_tenant",
        smoke_notification_prefix=f"{prefix}_notification",
        migrate_notification_message=f"{prefix}_migrate_notification",
        migrate_link_token=f"{prefix}_migrate_link_token",
        fiscal_uid=f"{prefix}_fiscal_uid",
    )


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _safe_pg_component_diagnostic() -> dict | None:
    if db_backend() != "postgresql":
        return None

    try:
        return inspect_postgres_dsn_components()
    except Exception as exc:
        return {
            "backend": "postgresql",
            "diagnostic_error": f"{type(exc).__name__}: {exc}",
        }


def _run_step(name: str, fn, results: list[dict]) -> dict:
    log.info("CHECK START: %s", name)
    started_at = time.time()
    try:
        details = fn() or {}
    except Exception as exc:
        duration_ms = int((time.time() - started_at) * 1000)
        log.exception("CHECK FAILED: %s", name)
        diagnostic_suffix = ""
        if db_backend() == "postgresql" and name == "connection_and_schema":
            diagnostic = _safe_pg_component_diagnostic()
            diagnostic_suffix = f" Safe PostgreSQL component diagnostic={diagnostic}"
        raise RuntimeError(
            f"{name} failed after {duration_ms}ms: {type(exc).__name__}: {exc}{diagnostic_suffix}"
        ) from exc

    duration_ms = int((time.time() - started_at) * 1000)
    result = {
        "name": name,
        "status": "ok",
        "duration_ms": duration_ms,
        "details": details,
    }
    results.append(result)
    log.info("CHECK OK: %s (%sms)", name, duration_ms)
    return result


def _require_postgresql() -> None:
    try:
        validate_database_url()
    except Exception as exc:
        diagnostic = _safe_pg_component_diagnostic()
        raise RuntimeError(
            f"DATABASE_URL validation failed: {exc}. Safe PostgreSQL component diagnostic={diagnostic}"
        ) from exc

    _assert(
        db_backend() == "postgresql",
        "DATABASE_URL must point to PostgreSQL. Example: postgresql://user:password@host:5432/dbname",
    )


def _fetch_required_tables() -> set[str]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = current_schema()
            """
        )
        return {row["table_name"] for row in cur.fetchall()}
    finally:
        conn.close()


def _delete_smoke_rows(ctx: SmokeContext) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        tenant_ids = (ctx.smoke_tenant_id, ctx.migrate_tenant_id)

        cur.execute(
            aq("DELETE FROM notification_log WHERE tenant_id IN (?, ?)"),
            tenant_ids,
        )
        cur.execute(
            aq("DELETE FROM notification_log WHERE message = ?"),
            (ctx.migrate_notification_message,),
        )
        cur.execute(
            aq("DELETE FROM telegram_link_tokens WHERE tenant_id IN (?, ?)"),
            tenant_ids,
        )
        cur.execute(
            aq("DELETE FROM telegram_link_tokens WHERE link_token = ?"),
            (ctx.migrate_link_token,),
        )
        cur.execute(
            aq("DELETE FROM fiscalization_checks WHERE uid = ?"),
            (ctx.fiscal_uid,),
        )
        cur.execute(
            aq("DELETE FROM processed_events WHERE tenant_id IN (?, ?)"),
            tenant_ids,
        )
        cur.execute(
            aq("DELETE FROM errors WHERE tenant_id IN (?, ?)"),
            tenant_ids,
        )
        cur.execute(
            aq("DELETE FROM stock_sync_status WHERE tenant_id IN (?, ?)"),
            tenant_ids,
        )
        cur.execute(
            aq("DELETE FROM event_store WHERE tenant_id IN (?, ?)"),
            tenant_ids,
        )
        cur.execute(
            aq("DELETE FROM tenants WHERE id IN (?, ?)"),
            tenant_ids,
        )
        conn.commit()
    finally:
        conn.close()

    if ctx.sqlite_temp_dir and ctx.sqlite_temp_dir.exists():
        shutil.rmtree(ctx.sqlite_temp_dir, ignore_errors=True)


def _ensure_smoke_tenant(ctx: SmokeContext) -> dict:
    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq(
                """
                INSERT INTO tenants (id, name, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE SET
                    name = excluded.name,
                    updated_at = excluded.updated_at
                """
            ),
            (ctx.smoke_tenant_id, f"PG Smoke Tenant {ctx.prefix}", now, now),
        )
        conn.commit()

        cur.execute(
            aq("SELECT id, name, created_at, updated_at FROM tenants WHERE id = ?"),
            (ctx.smoke_tenant_id,),
        )
        row = cur.fetchone()
        _assert(row is not None, "Failed to insert/read smoke tenant")
        return dict(row)
    finally:
        conn.close()


def _check_connection_and_schema() -> dict:
    init_db_module.init_db()

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq("SELECT ? AS answer"), (1,))
        row = cur.fetchone()
        _assert(row is not None and row["answer"] == 1, "SELECT 1 or named row access failed on PostgreSQL")

        _assert(
            init_db_module._col_exists(conn, "evotor_onboarding_sessions", "ms_data_json"),
            "init_db._col_exists failed to find evotor_onboarding_sessions.ms_data_json on PostgreSQL",
        )

        fiscal_columns = migrate_fiscal_poller._existing_columns(conn, "fiscalization_checks")
        _assert(
            "attempt" in fiscal_columns and "next_poll_at" in fiscal_columns,
            "migrate_fiscal_poller._existing_columns failed on PostgreSQL",
        )

        cur.execute(
            aq(
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = current_schema() AND indexname IN (?, ?)
                ORDER BY indexname
                """
            ),
            ("idx_tenants_evotor_store_id_unique", "idx_notification_log_tenant_created_at"),
        )
        indexes = {row["indexname"]: row["indexdef"] for row in cur.fetchall()}
        _assert(
            "idx_tenants_evotor_store_id_unique" in indexes,
            "Missing PostgreSQL tenant routing partial index idx_tenants_evotor_store_id_unique",
        )
        _assert(
            "WHERE" in indexes["idx_tenants_evotor_store_id_unique"].upper(),
            "idx_tenants_evotor_store_id_unique is expected to be a partial index on PostgreSQL",
        )
        _assert(
            "idx_notification_log_tenant_created_at" in indexes,
            "Missing PostgreSQL index idx_notification_log_tenant_created_at",
        )
    finally:
        conn.close()

    existing_tables = _fetch_required_tables()
    missing_tables = sorted(set(init_db_module.SCHEMA_TABLES) - existing_tables)
    _assert(not missing_tables, f"Schema issue: missing PostgreSQL tables {missing_tables}")

    return {
        "db_config": safe_database_config_for_log(),
        "table_count": len(existing_tables),
        "required_tables": list(init_db_module.SCHEMA_TABLES),
    }


def _check_crud_and_conflicts(ctx: SmokeContext) -> dict:
    tenant = _ensure_smoke_tenant(ctx)

    conn = get_connection()
    try:
        insert_notification_log(
            conn,
            tenant_id=ctx.smoke_tenant_id,
            channel_type="email",
            destination=f"{ctx.prefix}@example.com",
            event_type="pg_runtime_smoke",
            message=f"{ctx.smoke_notification_prefix}_message",
            status="sent",
            sent_at=int(time.time()),
        )
        conn.commit()

        notification_rows = list_notification_log(conn, tenant_id=ctx.smoke_tenant_id, limit=20, offset=0)
        matching_notifications = [
            row
            for row in notification_rows
            if row["message"] == f"{ctx.smoke_notification_prefix}_message"
        ]
        _assert(matching_notifications, "notification_log write/read failed on PostgreSQL")

        token_row = create_telegram_link_token(
            conn,
            tenant_id=ctx.smoke_tenant_id,
            ttl_sec=300,
        )
        conn.commit()

        active_token = get_active_telegram_link_token(conn, ctx.smoke_tenant_id)
        fetched_token = get_telegram_link_token_by_value(conn, token_row["link_token"])
        _assert(active_token is not None, "telegram_link_tokens active token lookup failed on PostgreSQL")
        _assert(fetched_token is not None, "telegram_link_tokens lookup by token failed on PostgreSQL")
        _assert(active_token["id"] == token_row["id"], "telegram_link_tokens returned unexpected active token")
        _assert(fetched_token["tenant_id"] == ctx.smoke_tenant_id, "telegram_link_tokens returned wrong tenant_id")

        processed_at = int(time.time())
        event_key = f"{ctx.prefix}_event_key"
        cur = conn.cursor()
        cur.execute(
            aq(
                """
                INSERT INTO processed_events (tenant_id, event_key, result_ref, processed_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (tenant_id, event_key) DO NOTHING
                """
            ),
            (ctx.smoke_tenant_id, event_key, "smoke-run-1", processed_at),
        )
        first_insert_count = cur.rowcount
        cur.execute(
            aq(
                """
                INSERT INTO processed_events (tenant_id, event_key, result_ref, processed_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (tenant_id, event_key) DO NOTHING
                """
            ),
            (ctx.smoke_tenant_id, event_key, "smoke-run-2", processed_at),
        )
        second_insert_count = cur.rowcount
        conn.commit()

        cur.execute(
            aq("SELECT COUNT(*) AS cnt FROM processed_events WHERE tenant_id = ? AND event_key = ?"),
            (ctx.smoke_tenant_id, event_key),
        )
        count_row = cur.fetchone()
        _assert(first_insert_count == 1, "First ON CONFLICT insert did not write a row on PostgreSQL")
        _assert(second_insert_count == 0, "Second ON CONFLICT insert should not duplicate rows on PostgreSQL")
        _assert(count_row is not None and count_row["cnt"] == 1, "processed_events duplicate protection failed on PostgreSQL")
    finally:
        conn.close()

    return {
        "tenant_id": tenant["id"],
        "notification_rows_found": len(matching_notifications),
        "telegram_token_id": token_row["id"],
        "processed_event_conflict": "ok",
    }


def _check_api_runtime() -> dict:
    _assert(api_main.app is not None, "FastAPI app failed to import")

    health_payload = api_main.health()
    _assert(
        health_payload["checks"]["db"]["status"] == "ok",
        f"API health() database check failed: {health_payload}",
    )

    metrics_response = api_main.metrics()
    metrics_body = metrics_response.body.decode("utf-8", errors="replace")
    _assert(metrics_response.status_code == 200, "API metrics() did not return HTTP 200")
    _assert(
        "event_store_status_count" in metrics_body,
        "API metrics() response does not contain Prometheus DB gauges",
    )

    return {
        "routes_loaded": len(api_main.app.routes),
        "health_status": health_payload["status"],
        "db_health": health_payload["checks"]["db"]["status"],
        "metrics_status": metrics_response.status_code,
    }


def _check_worker_runtime() -> dict:
    worker.heartbeat_worker()
    worker.heartbeat_worker()
    result = worker.runtime_db_smoke_check()

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("SELECT COUNT(*) AS cnt, MAX(last_seen_at) AS last_seen_at FROM service_heartbeats WHERE service_name = ?"),
            (worker.WORKER_HEARTBEAT_NAME,),
        )
        row = cur.fetchone()
    finally:
        conn.close()

    _assert(row is not None and row["cnt"] == 1, "worker heartbeat upsert failed on PostgreSQL")
    _assert(row["last_seen_at"] is not None, "worker heartbeat row was not updated on PostgreSQL")

    return {
        **result,
        "heartbeat_row_count": row["cnt"],
        "last_seen_at": row["last_seen_at"],
    }


def _check_alert_worker_runtime(ctx: SmokeContext) -> dict:
    conn = get_connection()
    try:
        cur = conn.cursor()
        now = int(time.time())

        cur.execute(
            aq(
                """
                INSERT INTO event_store (
                    id, tenant_id, event_type, event_key, payload_json,
                    status, retries, next_retry_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO NOTHING
                """
            ),
            (
                f"{ctx.prefix}_failed_event",
                ctx.smoke_tenant_id,
                "sale",
                f"{ctx.prefix}_failed_event_key",
                json.dumps({"smoke": True}),
                "FAILED",
                1,
                None,
                now,
                now,
            ),
        )
        cur.execute(
            aq(
                """
                INSERT INTO stock_sync_status (
                    tenant_id, status, started_at, updated_at, last_sync_at, last_error, synced_items_count, total_items_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (tenant_id) DO UPDATE SET
                    status = excluded.status,
                    started_at = excluded.started_at,
                    updated_at = excluded.updated_at,
                    last_sync_at = excluded.last_sync_at,
                    last_error = excluded.last_error,
                    synced_items_count = excluded.synced_items_count,
                    total_items_count = excluded.total_items_count
                """
            ),
            (
                ctx.smoke_tenant_id,
                "error",
                now,
                now,
                now,
                "pg runtime smoke stock error",
                1,
                2,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    result = alert_worker.runtime_db_smoke_check()
    tenant_snapshot = alert_worker._collect_tenant_alert_snapshot()
    tenant_state = tenant_snapshot.get(ctx.smoke_tenant_id)

    _assert(tenant_state is not None, "alert_worker tenant snapshot did not include smoke tenant")
    _assert(tenant_state["failed_events_count"] >= 1, "alert_worker did not see FAILED events on PostgreSQL")
    _assert(tenant_state["stock_error_present"] is True, "alert_worker did not see stock_sync_status errors on PostgreSQL")

    return {
        **result,
        "tenant_failed_events_count": tenant_state["failed_events_count"],
        "tenant_stock_error_present": tenant_state["stock_error_present"],
    }


def _check_fiscal_poller_runtime(ctx: SmokeContext) -> dict:
    conn = get_connection()
    try:
        cur = conn.cursor()
        now = int(time.time())
        updated_at = now - max(fiscal_poller.POLL_STALE_SEC, 30) - 5
        cur.execute(
            aq(
                """
                INSERT INTO fiscalization_checks (
                    uid, tenant_id, ms_demand_id, status, description,
                    created_at, updated_at, attempt, next_poll_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (uid) DO NOTHING
                """
            ),
            (
                ctx.fiscal_uid,
                ctx.smoke_tenant_id,
                f"{ctx.prefix}_ms_demand",
                1,
                "pg runtime smoke pending fiscal check",
                now,
                updated_at,
                0,
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    result = fiscal_poller.runtime_db_smoke_check()
    _assert(
        result["pending_checks"] >= 1,
        "fiscal_poller pending checks bootstrap/query failed on PostgreSQL",
    )
    return result


def _build_sqlite_fixture(ctx: SmokeContext) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="pg_runtime_smoke_", dir=os.getcwd()))
    sqlite_path = temp_dir / "migrate_smoke.db"
    conn = sqlite3.connect(sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE tenants (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE notification_log (
                id TEXT PRIMARY KEY,
                tenant_id TEXT,
                channel_type TEXT NOT NULL,
                destination TEXT NOT NULL,
                event_type TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                created_at INTEGER NOT NULL,
                sent_at INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE telegram_link_tokens (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                link_token TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                linked_chat_id TEXT,
                linked_at INTEGER
            )
            """
        )

        now = int(time.time())
        cur.execute(
            "INSERT INTO tenants (id, name, created_at) VALUES (?, ?, ?)",
            (ctx.migrate_tenant_id, f"PG Migrate Smoke Tenant {ctx.prefix}", now),
        )
        cur.execute(
            """
            INSERT INTO notification_log (
                id, tenant_id, channel_type, destination, event_type,
                message, status, error_message, created_at, sent_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"{ctx.prefix}_migrate_notification",
                ctx.migrate_tenant_id,
                "email",
                f"{ctx.prefix}@example.com",
                "pg_migrate_smoke",
                ctx.migrate_notification_message,
                "sent",
                None,
                now,
                now,
            ),
        )
        cur.execute(
            """
            INSERT INTO telegram_link_tokens (
                id, tenant_id, link_token, status, created_at, expires_at, linked_chat_id, linked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"{ctx.prefix}_migrate_tg_token",
                ctx.migrate_tenant_id,
                ctx.migrate_link_token,
                "linked",
                now,
                now + 300,
                "123456789",
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    ctx.sqlite_temp_dir = temp_dir
    return sqlite_path


def _check_migrate_to_pg(ctx: SmokeContext) -> dict:
    sqlite_path = _build_sqlite_fixture(ctx)
    previous_sqlite_env = os.getenv("SQLITE_PATH")
    previous_module_sqlite_path = getattr(migrate_to_pg_module, "SQLITE_PATH", None)

    try:
        os.environ["SQLITE_PATH"] = str(sqlite_path)
        migrate_to_pg_module.SQLITE_PATH = str(sqlite_path)

        migrate_to_pg_module.migrate()
        migrate_to_pg_module.migrate()

        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(aq("SELECT id FROM tenants WHERE id = ?"), (ctx.migrate_tenant_id,))
            tenant_row = cur.fetchone()
            cur.execute(
                aq("SELECT COUNT(*) AS cnt FROM notification_log WHERE message = ?"),
                (ctx.migrate_notification_message,),
            )
            notification_row = cur.fetchone()
            cur.execute(
                aq("SELECT COUNT(*) AS cnt FROM telegram_link_tokens WHERE link_token = ?"),
                (ctx.migrate_link_token,),
            )
            token_row = cur.fetchone()
        finally:
            conn.close()

        _assert(tenant_row is not None, "migrate_to_pg failed to migrate tenants row into PostgreSQL")
        _assert(notification_row is not None and notification_row["cnt"] == 1, "migrate_to_pg failed to migrate notification_log into PostgreSQL idempotently")
        _assert(token_row is not None and token_row["cnt"] == 1, "migrate_to_pg failed to migrate telegram_link_tokens into PostgreSQL idempotently")
    finally:
        if previous_sqlite_env is None:
            os.environ.pop("SQLITE_PATH", None)
        else:
            os.environ["SQLITE_PATH"] = previous_sqlite_env

        if previous_module_sqlite_path is None:
            try:
                delattr(migrate_to_pg_module, "SQLITE_PATH")
            except AttributeError:
                pass
        else:
            migrate_to_pg_module.SQLITE_PATH = previous_module_sqlite_path

    return {
        "sqlite_fixture": str(sqlite_path),
        "migrated_tenant_id": ctx.migrate_tenant_id,
        "notification_log_rows": 1,
        "telegram_link_tokens_rows": 1,
    }


def run_pg_runtime_smoke() -> dict:
    _require_postgresql()
    ctx = _new_context()
    results: list[dict] = []
    cleanup_needed = False

    log.info("Running PostgreSQL runtime smoke config=%s", safe_database_config_for_log())

    try:
        _run_step("connection_and_schema", _check_connection_and_schema, results)
        cleanup_needed = True
        _run_step("crud_and_conflicts", lambda: _check_crud_and_conflicts(ctx), results)
        _run_step("api_runtime", _check_api_runtime, results)
        _run_step("worker_runtime", _check_worker_runtime, results)
        _run_step("alert_worker_runtime", lambda: _check_alert_worker_runtime(ctx), results)
        _run_step("fiscal_poller_runtime", lambda: _check_fiscal_poller_runtime(ctx), results)
        _run_step("migrate_to_pg_runtime", lambda: _check_migrate_to_pg(ctx), results)
    finally:
        if cleanup_needed or ctx.sqlite_temp_dir:
            try:
                _delete_smoke_rows(ctx)
            except Exception:
                log.exception("Failed to cleanup PostgreSQL smoke artifacts")

    return {
        "status": "ok",
        "backend": "postgresql",
        "db_config": safe_database_config_for_log(),
        "checks": results,
    }


def main() -> None:
    try:
        summary = run_pg_runtime_smoke()
    except Exception as exc:
        log.error(
            "PostgreSQL runtime smoke failed. Required env: DATABASE_URL=postgresql://... "
            "Optional env: SQLITE_PATH for migrate_to_pg smoke. Error=%s",
            exc,
        )
        raise SystemExit(1) from exc

    log.info("PostgreSQL runtime smoke completed successfully")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
