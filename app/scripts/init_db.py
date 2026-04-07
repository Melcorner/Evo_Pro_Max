"""
app/scripts/init_db.py

Инициализация схемы БД.
Работает как с SQLite, так и с PostgreSQL.

Запуск:
    python -m app.scripts.init_db
"""

import logging
from app.db import get_connection, db_backend, adapt_query

log = logging.getLogger("init_db")
logging.basicConfig(level=logging.INFO, format="%(message)s")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col_exists(conn, table: str, column: str) -> bool:
    """Проверяет наличие колонки в таблице — совместимо с SQLite и PG."""
    backend = db_backend()
    cur = conn.cursor()
    if backend == "sqlite":
        cur.execute(f"PRAGMA table_info({table})")
        cols = {row[1] for row in cur.fetchall()}
    else:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
            """,
            (table, column),
        )
        cols = {row["column_name"] for row in cur.fetchall()}
    return column in cols


def _add_column_if_missing(conn, table: str, column: str, definition: str) -> None:
    if not _col_exists(conn, table, column):
        conn.cursor().execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        log.info("  + %s.%s", table, column)


def _index_exists(conn, index_name: str) -> bool:
    backend = db_backend()
    cur = conn.cursor()
    if backend == "sqlite":
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
            (index_name,),
        )
    else:
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE indexname = %s",
            (index_name,),
        )
    return cur.fetchone() is not None


def _create_index(conn, ddl: str, index_name: str) -> None:
    if not _index_exists(conn, index_name):
        conn.cursor().execute(ddl)
        log.info("  + index %s", index_name)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_db():
    backend = db_backend()
    log.info("Initializing DB backend=%s", backend)

    conn = get_connection()

    # PostgreSQL не знает PRAGMA — FK включены по умолчанию
    if backend == "sqlite":
        conn.execute("PRAGMA foreign_keys = ON;")

    cur = conn.cursor()

    # ------------------------------------------------------------------
    # tenants
    # ------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id                  TEXT PRIMARY KEY,
            name                TEXT NOT NULL,
            evotor_api_key      TEXT NOT NULL DEFAULT '',
            moysklad_token      TEXT NOT NULL DEFAULT '',
            created_at          INTEGER NOT NULL,
            evotor_user_id      TEXT,
            evotor_token        TEXT,
            evotor_store_id     TEXT,
            ms_organization_id  TEXT,
            ms_store_id         TEXT,
            ms_agent_id         TEXT,
            sync_completed_at   INTEGER,
            fiscal_token        TEXT,
            fiscal_client_uid   TEXT,
            fiscal_device_uid   TEXT,
            ms_account_id       TEXT,
            ms_status           TEXT DEFAULT 'active',
            updated_at          INTEGER
        )
        """
    )

    # ------------------------------------------------------------------
    # evotor_connections
    # ------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS evotor_connections (
            id              TEXT PRIMARY KEY,
            evotor_user_id  TEXT NOT NULL UNIQUE,
            evotor_token    TEXT NOT NULL,
            stores_json     TEXT NOT NULL,
            created_at      INTEGER NOT NULL,
            updated_at      INTEGER NOT NULL
        )
        """
    )

    # ------------------------------------------------------------------
    # evotor_onboarding_sessions
    # ------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS evotor_onboarding_sessions (
            id              TEXT PRIMARY KEY,
            evotor_token    TEXT NOT NULL,
            stores_json     TEXT NOT NULL,
            moysklad_token  TEXT,
            ms_data_json    TEXT,
            created_at      INTEGER NOT NULL,
            updated_at      INTEGER NOT NULL
        )
        """
    )

    # ------------------------------------------------------------------
    # event_store
    # ------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS event_store (
            id                  TEXT PRIMARY KEY,
            tenant_id           TEXT NOT NULL,
            event_type          TEXT NOT NULL CHECK (event_type IN ('sale','product','stock')),
            event_key           TEXT NOT NULL,
            payload_json        TEXT NOT NULL,
            status              TEXT NOT NULL CHECK (status IN ('NEW','PROCESSING','DONE','RETRY','FAILED')),
            retries             INTEGER NOT NULL DEFAULT 0,
            next_retry_at       INTEGER,
            last_error_code     TEXT,
            last_error_message  TEXT,
            created_at          INTEGER NOT NULL,
            updated_at          INTEGER NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants(id)
        )
        """
    )

    # ------------------------------------------------------------------
    # processed_events
    # ------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_events (
            tenant_id       TEXT NOT NULL,
            event_key       TEXT NOT NULL,
            result_ref      TEXT,
            processed_at    INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, event_key),
            FOREIGN KEY (tenant_id) REFERENCES tenants(id)
        )
        """
    )

    # ------------------------------------------------------------------
    # mappings
    # ------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mappings (
            tenant_id       TEXT NOT NULL,
            entity_type     TEXT NOT NULL,
            evotor_id       TEXT NOT NULL,
            ms_id           TEXT NOT NULL,
            created_at      INTEGER NOT NULL,
            updated_at      INTEGER NOT NULL,

            UNIQUE (tenant_id, entity_type, evotor_id),
            UNIQUE (tenant_id, entity_type, ms_id)
        )
        """
    )

    # ------------------------------------------------------------------
    # errors
    # ------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS errors (
            id                  TEXT PRIMARY KEY,
            event_id            TEXT NOT NULL,
            tenant_id           TEXT NOT NULL,
            error_code          TEXT,
            message             TEXT NOT NULL,
            payload_snapshot    TEXT,
            response_body       TEXT,
            created_at          INTEGER NOT NULL
        )
        """
    )

    # ------------------------------------------------------------------
    # stock_sync_status
    # ------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_sync_status (
            tenant_id           TEXT PRIMARY KEY,
            status              TEXT NOT NULL CHECK (status IN ('configured','in_progress','ok','error')),
            started_at          INTEGER,
            updated_at          INTEGER NOT NULL,
            last_sync_at        INTEGER,
            last_error          TEXT,
            synced_items_count  INTEGER NOT NULL DEFAULT 0,
            total_items_count   INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (tenant_id) REFERENCES tenants(id)
        )
        """
    )

    # ------------------------------------------------------------------
    # fiscalization_checks
    # ------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fiscalization_checks (
            uid                     TEXT PRIMARY KEY,
            tenant_id               TEXT NOT NULL,
            ms_demand_id            TEXT NOT NULL,
            fiscal_client_uid       TEXT,
            fiscal_device_uid       TEXT,
            status                  INTEGER NOT NULL DEFAULT 1,
            description             TEXT,
            error_code              INTEGER,
            error_message           TEXT,
            request_json            TEXT,
            response_json           TEXT,
            attempt                 INTEGER NOT NULL DEFAULT 0,
            last_poll_at            INTEGER,
            next_poll_at            INTEGER,
            last_transport_error    TEXT,
            created_at              INTEGER NOT NULL,
            updated_at              INTEGER NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants(id)
        )
        """
    )

    # ------------------------------------------------------------------
    # service_heartbeats
    # ------------------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS service_heartbeats (
            service_name    TEXT PRIMARY KEY,
            last_seen_at    INTEGER NOT NULL,
            meta_json       TEXT
        )
        """
    )

    # ------------------------------------------------------------------
    # Индексы
    # ------------------------------------------------------------------
    indexes = [
        (
            "idx_event_unique",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_event_unique ON event_store(tenant_id, event_key)",
        ),
        (
            "idx_event_status_retry",
            "CREATE INDEX IF NOT EXISTS idx_event_status_retry ON event_store(status, next_retry_at)",
        ),
        (
            "idx_fisc_tenant_demand",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_fisc_tenant_demand ON fiscalization_checks(tenant_id, ms_demand_id)",
        ),
        (
            "idx_fisc_tenant",
            "CREATE INDEX IF NOT EXISTS idx_fisc_tenant ON fiscalization_checks(tenant_id)",
        ),
        (
            "idx_fisc_demand",
            "CREATE INDEX IF NOT EXISTS idx_fisc_demand ON fiscalization_checks(ms_demand_id)",
        ),
        (
            "idx_fiscal_checks_pending",
            "CREATE INDEX IF NOT EXISTS idx_fiscal_checks_pending ON fiscalization_checks(status, next_poll_at, updated_at)",
        ),
        (
            "idx_errors_event_id",
            "CREATE INDEX IF NOT EXISTS idx_errors_event_id ON errors(event_id)",
        ),
        (
            "idx_errors_tenant_id",
            "CREATE INDEX IF NOT EXISTS idx_errors_tenant_id ON errors(tenant_id)",
        ),
        (
            "idx_errors_created_at",
            "CREATE INDEX IF NOT EXISTS idx_errors_created_at ON errors(created_at)",
        ),
        (
            "idx_mappings_evotor",
            "CREATE INDEX IF NOT EXISTS idx_mappings_evotor ON mappings(tenant_id, entity_type, evotor_id)",
        ),
        (
            "idx_mappings_ms",
            "CREATE INDEX IF NOT EXISTS idx_mappings_ms ON mappings(tenant_id, entity_type, ms_id)",
        ),
        (
            "idx_tenants_evotor_user_id",
            "CREATE INDEX IF NOT EXISTS idx_tenants_evotor_user_id ON tenants(evotor_user_id)",
        ),
        (
            "idx_tenants_evotor_store_id",
            "CREATE INDEX IF NOT EXISTS idx_tenants_evotor_store_id ON tenants(evotor_store_id)",
        ),
    ]

    for index_name, ddl in indexes:
        _create_index(conn, ddl, index_name)

    # ------------------------------------------------------------------
    # Миграции: добавляем колонки которых может не быть в старой БД
    # ------------------------------------------------------------------
    log.info("Applying migrations...")

    migrations = [
        # errors
        ("errors", "response_body", "TEXT"),
        # tenants
        ("tenants", "evotor_user_id", "TEXT"),
        ("tenants", "evotor_token", "TEXT"),
        ("tenants", "ms_organization_id", "TEXT"),
        ("tenants", "ms_store_id", "TEXT"),
        ("tenants", "ms_agent_id", "TEXT"),
        ("tenants", "sync_completed_at", "INTEGER"),
        ("tenants", "evotor_store_id", "TEXT"),
        ("tenants", "fiscal_token", "TEXT"),
        ("tenants", "fiscal_client_uid", "TEXT"),
        ("tenants", "fiscal_device_uid", "TEXT"),
        # stock_sync_status
        ("stock_sync_status", "started_at", "INTEGER"),
        ("stock_sync_status", "updated_at", "INTEGER NOT NULL DEFAULT 0"),
        ("stock_sync_status", "last_sync_at", "INTEGER"),
        ("stock_sync_status", "last_error", "TEXT"),
        ("stock_sync_status", "synced_items_count", "INTEGER NOT NULL DEFAULT 0"),
        ("stock_sync_status", "total_items_count", "INTEGER NOT NULL DEFAULT 0"),
        # fiscalization_checks
        ("fiscalization_checks", "attempt", "INTEGER NOT NULL DEFAULT 0"),
        ("fiscalization_checks", "last_poll_at", "INTEGER"),
        ("fiscalization_checks", "next_poll_at", "INTEGER"),
        ("fiscalization_checks", "last_transport_error", "TEXT"),
        # evotor_onboarding_sessions — новые поля онбординга
        ("evotor_onboarding_sessions", "moysklad_token", "TEXT"),
        ("evotor_onboarding_sessions", "ms_data_json", "TEXT"),
        # tenants — vendor API поля
        ("tenants", "ms_account_id", "TEXT"),
        ("tenants", "ms_status", "TEXT DEFAULT 'active'"),
        ("tenants", "updated_at", "INTEGER"),
    ]

    for table, column, definition in migrations:
        _add_column_if_missing(conn, table, column, definition)

    conn.commit()
    conn.close()
    log.info("DB initialized successfully backend=%s", backend)


if __name__ == "__main__":
    init_db()