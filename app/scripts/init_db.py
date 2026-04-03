from app.db import get_connection


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("PRAGMA foreign_keys = ON;")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            evotor_api_key TEXT NOT NULL,
            moysklad_token TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
    )
        
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS evotor_connections (
        id TEXT PRIMARY KEY,
        evotor_user_id TEXT NOT NULL UNIQUE,
        evotor_token TEXT NOT NULL,
        stores_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )
    """)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS event_store (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            event_type TEXT NOT NULL CHECK (event_type IN ('sale','product','stock')),
            event_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('NEW','PROCESSING','DONE','RETRY','FAILED')),
            retries INTEGER NOT NULL DEFAULT 0,
            next_retry_at INTEGER,
            last_error_code TEXT,
            last_error_message TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants(id)
        )
        """
    )

    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_event_unique
        ON event_store(tenant_id, event_key)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_event_status_retry
        ON event_store(status, next_retry_at)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_events (
            tenant_id TEXT NOT NULL,
            event_key TEXT NOT NULL,
            result_ref TEXT,
            processed_at INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, event_key),
            FOREIGN KEY (tenant_id) REFERENCES tenants(id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS mappings (
            tenant_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            evotor_id TEXT NOT NULL,
            ms_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,

            UNIQUE (tenant_id, entity_type, evotor_id),
            UNIQUE (tenant_id, entity_type, ms_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS errors (
            id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            error_code TEXT,
            message TEXT NOT NULL,
            payload_snapshot TEXT,
            response_body TEXT,
            created_at INTEGER NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_sync_status (
            tenant_id TEXT PRIMARY KEY,
            status TEXT NOT NULL CHECK (status IN ('configured','in_progress','ok','error')),
            started_at INTEGER,
            updated_at INTEGER NOT NULL,
            last_sync_at INTEGER,
            last_error TEXT,
            synced_items_count INTEGER NOT NULL DEFAULT 0,
            total_items_count INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (tenant_id) REFERENCES tenants(id)
        )
        """
    )

    # В базовую схему включаем поля poller'а, чтобы новая БД поднималась без отдельной миграции.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fiscalization_checks (
            uid TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            ms_demand_id TEXT NOT NULL,
            fiscal_client_uid TEXT,
            fiscal_device_uid TEXT,
            status INTEGER NOT NULL DEFAULT 1,
            description TEXT,
            error_code INTEGER,
            error_message TEXT,
            request_json TEXT,
            response_json TEXT,
            attempt INTEGER NOT NULL DEFAULT 0,
            last_poll_at INTEGER,
            next_poll_at INTEGER,
            last_transport_error TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants(id)
        )
        """
    )

    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_fisc_tenant_demand
        ON fiscalization_checks(tenant_id, ms_demand_id)
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fisc_tenant ON fiscalization_checks(tenant_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fisc_demand ON fiscalization_checks(ms_demand_id)")
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fiscal_checks_pending
        ON fiscalization_checks (status, next_poll_at, updated_at)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS service_heartbeats (
            service_name TEXT PRIMARY KEY,
            last_seen_at INTEGER NOT NULL,
            meta_json TEXT
        )
        """
    )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_errors_event_id ON errors(event_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_errors_tenant_id ON errors(tenant_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_errors_created_at ON errors(created_at)")

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mappings_evotor
            ON mappings(tenant_id, entity_type, evotor_id)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mappings_ms
            ON mappings(tenant_id, entity_type, ms_id)
        """
    )

    existing_errors = {row[1] for row in cursor.execute("PRAGMA table_info(errors)")}
    if "response_body" not in existing_errors:
        cursor.execute("ALTER TABLE errors ADD COLUMN response_body TEXT")

    existing_tenants = {row[1] for row in cursor.execute("PRAGMA table_info(tenants)")}
    if "evotor_user_id" not in existing_tenants:
        cursor.execute("ALTER TABLE tenants ADD COLUMN evotor_user_id TEXT")
    if "evotor_token" not in existing_tenants:
        cursor.execute("ALTER TABLE tenants ADD COLUMN evotor_token TEXT")
    if "ms_organization_id" not in existing_tenants:
        cursor.execute("ALTER TABLE tenants ADD COLUMN ms_organization_id TEXT")
    if "ms_store_id" not in existing_tenants:
        cursor.execute("ALTER TABLE tenants ADD COLUMN ms_store_id TEXT")
    if "ms_agent_id" not in existing_tenants:
        cursor.execute("ALTER TABLE tenants ADD COLUMN ms_agent_id TEXT")
    if "sync_completed_at" not in existing_tenants:
        cursor.execute("ALTER TABLE tenants ADD COLUMN sync_completed_at INTEGER")
    if "evotor_store_id" not in existing_tenants:
        cursor.execute("ALTER TABLE tenants ADD COLUMN evotor_store_id TEXT")
    if "fiscal_token" not in existing_tenants:
        cursor.execute("ALTER TABLE tenants ADD COLUMN fiscal_token TEXT")
    if "fiscal_client_uid" not in existing_tenants:
        cursor.execute("ALTER TABLE tenants ADD COLUMN fiscal_client_uid TEXT")
    if "fiscal_device_uid" not in existing_tenants:
        cursor.execute("ALTER TABLE tenants ADD COLUMN fiscal_device_uid TEXT")

    existing_stock_sync_status = {row[1] for row in cursor.execute("PRAGMA table_info(stock_sync_status)")}
    if existing_stock_sync_status:
        if "started_at" not in existing_stock_sync_status:
            cursor.execute("ALTER TABLE stock_sync_status ADD COLUMN started_at INTEGER")
        if "updated_at" not in existing_stock_sync_status:
            cursor.execute("ALTER TABLE stock_sync_status ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0")
        if "last_sync_at" not in existing_stock_sync_status:
            cursor.execute("ALTER TABLE stock_sync_status ADD COLUMN last_sync_at INTEGER")
        if "last_error" not in existing_stock_sync_status:
            cursor.execute("ALTER TABLE stock_sync_status ADD COLUMN last_error TEXT")
        if "synced_items_count" not in existing_stock_sync_status:
            cursor.execute("ALTER TABLE stock_sync_status ADD COLUMN synced_items_count INTEGER NOT NULL DEFAULT 0")
        if "total_items_count" not in existing_stock_sync_status:
            cursor.execute("ALTER TABLE stock_sync_status ADD COLUMN total_items_count INTEGER NOT NULL DEFAULT 0")

    existing_fiscal = {row[1] for row in cursor.execute("PRAGMA table_info(fiscalization_checks)")}
    if "attempt" not in existing_fiscal:
        cursor.execute("ALTER TABLE fiscalization_checks ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0")
    if "last_poll_at" not in existing_fiscal:
        cursor.execute("ALTER TABLE fiscalization_checks ADD COLUMN last_poll_at INTEGER")
    if "next_poll_at" not in existing_fiscal:
        cursor.execute("ALTER TABLE fiscalization_checks ADD COLUMN next_poll_at INTEGER")
    if "last_transport_error" not in existing_fiscal:
        cursor.execute("ALTER TABLE fiscalization_checks ADD COLUMN last_transport_error TEXT")

    # Lookup-индексы для мультитенант webhook routing.
    # Жёсткие UNIQUE-индексы лучше оставлять отдельной миграции.
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tenants_evotor_user_id ON tenants(evotor_user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tenants_evotor_store_id ON tenants(evotor_store_id)")

    conn.commit()
    conn.close()
    print("DB initialized")


if __name__ == "__main__":
    init_db()
