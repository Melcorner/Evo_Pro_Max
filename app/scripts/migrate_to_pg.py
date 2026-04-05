"""
app/scripts/migrate_to_pg.py

Переносит все данные из SQLite в PostgreSQL.

Использование:
    # 1. Убедись что PostgreSQL запущен и база создана
    # 2. Задай обе переменные окружения:
    export SQLITE_PATH=data/app.db
    export DATABASE_URL=postgresql://user:password@localhost:5432/evotor_ms

    # 3. Запусти миграцию:
    python -m app.scripts.migrate_to_pg

Скрипт:
  - сначала инициализирует схему в PG через init_db()
  - затем копирует данные таблица за таблицей
  - идемпотентен: при повторном запуске использует INSERT ... ON CONFLICT DO NOTHING
  - выводит количество перенесённых строк по каждой таблице
"""

import os
import sqlite3
import logging

log = logging.getLogger("migrate_to_pg")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

SQLITE_PATH = os.getenv("SQLITE_PATH", "data/app.db")

# Таблицы в порядке, учитывающем FK-зависимости
TABLES = [
    "tenants",
    "evotor_connections",
    "evotor_onboarding_sessions",
    "event_store",
    "processed_events",
    "mappings",
    "errors",
    "stock_sync_status",
    "fiscalization_checks",
    "service_heartbeats",
]


def _get_sqlite_rows(sqlite_path: str, table: str) -> tuple[list[str], list[tuple]]:
    """Читает все строки из SQLite-таблицы. Возвращает (columns, rows)."""
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    try:
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
    except sqlite3.OperationalError as e:
        log.warning("Table %s not found in SQLite: %s", table, e)
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
    # Проверяем что DATABASE_URL указывает на PG
    database_url = os.getenv("DATABASE_URL", "")
    if not (database_url.startswith("postgresql") or database_url.startswith("postgres")):
        raise RuntimeError(
            "DATABASE_URL должен указывать на PostgreSQL.\n"
            f"Текущее значение: '{database_url}'"
        )

    if not os.path.exists(SQLITE_PATH):
        raise FileNotFoundError(f"SQLite база не найдена: {SQLITE_PATH}")

    log.info("Source SQLite: %s", SQLITE_PATH)
    log.info("Target PostgreSQL: %s", database_url.split("@")[-1])  # скрываем credentials

    # Инициализируем схему в PG
    log.info("Initializing PostgreSQL schema...")
    from app.scripts.init_db import init_db
    init_db()

    # Подключаемся к PG
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        raise RuntimeError("psycopg2 не установлен. Выполни: pip install psycopg2-binary")

    pg_conn = psycopg2.connect(database_url)
    pg_conn.cursor_factory = psycopg2.extras.RealDictCursor

    total_rows = 0

    try:
        for table in TABLES:
            columns, rows = _get_sqlite_rows(SQLITE_PATH, table)

            if not rows:
                log.info("  %-40s — пусто или не существует", table)
                continue

            inserted = _insert_pg(pg_conn, table, columns, rows)
            log.info("  %-40s — %d / %d строк перенесено", table, inserted, len(rows))
            total_rows += inserted

        pg_conn.commit()
        log.info("Migration complete. Total rows migrated: %d", total_rows)

    except Exception as e:
        pg_conn.rollback()
        log.error("Migration failed: %s", e)
        raise

    finally:
        pg_conn.close()


if __name__ == "__main__":
    migrate()