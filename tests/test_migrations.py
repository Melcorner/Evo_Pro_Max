import sqlite3

import pytest

from app.migrations import migrate_evotor_multitenant, migrate_evotor_onboarding_sessions, migrate_store_based_profiles
from app.scripts import init_db as init_db_module
from app.scripts import migrate_to_pg as migrate_to_pg_module


def test_migrate_to_pg_uses_current_schema_tables():
    assert migrate_to_pg_module.TABLES_TO_MIGRATE == list(init_db_module.SCHEMA_TABLES)


def test_migrate_to_pg_includes_notification_and_telegram_tables():
    assert "notification_log" in migrate_to_pg_module.TABLES_TO_MIGRATE
    assert "telegram_link_tokens" in migrate_to_pg_module.TABLES_TO_MIGRATE


def test_insert_pg_uses_on_conflict_do_nothing_for_idempotent_reruns():
    class FakeCursor:
        def __init__(self):
            self.rowcount = 1
            self.executed_sql = []

        def execute(self, sql, params):
            self.executed_sql.append((sql, params))

    class FakeConnection:
        def __init__(self):
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

    conn = FakeConnection()
    inserted = migrate_to_pg_module._insert_pg(
        conn,
        "notification_log",
        ["id", "message"],
        [("n1", "hello")],
    )

    assert inserted == 1
    assert "ON CONFLICT DO NOTHING" in conn.cursor_obj.executed_sql[0][0]


def test_multitenant_duplicate_reader_handles_dict_like_rows():
    class FakeCursor:
        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [{"duplicate_value": "store-1", "cnt": 2}]

    duplicates = migrate_evotor_multitenant._find_duplicates(FakeCursor(), "evotor_store_id")

    assert duplicates == [("store-1", 2)]


def test_store_based_profiles_explicitly_rejects_postgresql(monkeypatch):
    monkeypatch.setattr(migrate_store_based_profiles, "db_backend", lambda: "postgresql")

    with pytest.raises(RuntimeError) as exc_info:
        migrate_store_based_profiles.run()

    assert "SQLite-only" in str(exc_info.value)


def test_init_db_sqlite_creates_current_tables_and_indexes(monkeypatch, tmp_path):
    db_path = tmp_path / "app.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    init_db_module.init_db()

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        cur.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()

    assert "notification_log" in tables
    assert "telegram_link_tokens" in tables
    assert "action_log" in tables
    assert "sync_snapshots" in tables
    assert "sync_locks" in tables
    assert "idx_tenants_evotor_user_id_lookup" in indexes
    assert "idx_tenants_evotor_store_id_unique" in indexes
    assert "idx_action_log_tenant_created_at" in indexes
    assert "idx_sync_snapshots_tenant_created_at" in indexes


def test_onboarding_sessions_migration_adds_current_columns(monkeypatch, tmp_path):
    db_path = tmp_path / "onboarding.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE evotor_onboarding_sessions (
                id TEXT PRIMARY KEY,
                evotor_token TEXT NOT NULL,
                stores_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    migrate_evotor_onboarding_sessions.run()

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(evotor_onboarding_sessions)")
        columns = {row[1] for row in cur.fetchall()}
    finally:
        conn.close()

    assert "moysklad_token" in columns
    assert "ms_data_json" in columns
