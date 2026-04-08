"""
app/scripts/migrate_to_pg.py

Переносит все данные из SQLite в PostgreSQL.
"""

import logging
import os
import sqlite3

from app.db import db_backend, get_connection, safe_database_config_for_log, validate_database_url
from app.scripts.init_db import SCHEMA_TABLES

log = logging.getLogger("migrate_to_pg")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

SQLITE_PATH = os.getenv("SQLITE_PATH", "data/app.db")

# Таблицы в порядке, учитывающем FK-зависимости
TABLES_TO_MIGRATE = list(SCHEMA_TABLES)
TABLES = TABLES_TO_MIGRATE


def _get_sqlite_rows(sqlite_path: str, table: str) -> tuple[list[str], list[tuple]]:
    """Читает все строки из SQLite-таблицы. Возвращает (columns, rows)."""
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    try:
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
    except sqlite3.OperationalError as exc:
        log.warning("Table %s not found in SQLite: %s", table, exc)
        conn.close()
        return [], []

    if not rows:
        conn.close()
        return [], []

    columns = list(rows[0].keys())
    data = [tuple(row) for row in rows]
    conn.close()
    return columns, data


def _insert_pg(pg_conn, table: str, columns: list[str], rows: list[tuple]) -> int:
    """Вставляет строки в PostgreSQL. ON CONFLICT DO NOTHING для идемпотентности."""
    if not rows:
        return 0

    cols_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    sql = f"""
        INSERT INTO {table} ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """

    cur = pg_conn.cursor()
    inserted = 0
    for row in rows:
        cur.execute(sql, row)
        inserted += cur.rowcount

    return inserted


def migrate():
    validate_database_url()

    if db_backend() != "postgresql":
        raise RuntimeError(
            "DATABASE_URL должен указывать на PostgreSQL.\n"
            "Пример: postgresql://user:password@localhost:5432/evotor_ms"
        )

    if not os.path.exists(SQLITE_PATH):
        raise FileNotFoundError(f"SQLite база не найдена: {SQLITE_PATH}")

    log.info("Source SQLite: %s", SQLITE_PATH)
    log.info("Target PostgreSQL config: %s", safe_database_config_for_log())

    log.info("Initializing PostgreSQL schema...")
    from app.scripts.init_db import init_db

    init_db()

    pg_conn = get_connection()
    total_rows = 0

    try:
        for table in TABLES_TO_MIGRATE:
            columns, rows = _get_sqlite_rows(SQLITE_PATH, table)

            if not rows:
                log.info("  %-40s - empty or missing", table)
                continue

            inserted = _insert_pg(pg_conn, table, columns, rows)
            log.info("  %-40s - %d / %d rows migrated", table, inserted, len(rows))
            total_rows += inserted

        pg_conn.commit()
        log.info("Migration complete. Total rows migrated: %d", total_rows)

    except Exception as exc:
        pg_conn.rollback()
        log.error("Migration failed: %s", exc)
        raise

    finally:
        pg_conn.close()


if __name__ == "__main__":
    migrate()
