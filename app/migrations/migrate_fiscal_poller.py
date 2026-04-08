"""
migrate_fiscal_poller.py — добавляет колонки для fiscal_poller в fiscalization_checks.

Запустить один раз перед деплоем fiscal_poller:
    python -m app.migrations.migrate_fiscal_poller

Миграция идемпотентна: повторный запуск безопасен.
"""

import logging
from app.db import adapt_query as aq, db_backend, get_connection
from app.logger import setup_logging

setup_logging()
log = logging.getLogger("migrate_fiscal_poller")

COLUMNS_TO_ADD = [
    ("attempt",             "INTEGER NOT NULL DEFAULT 0"),
    ("last_poll_at",        "INTEGER"),
    ("next_poll_at",        "INTEGER"),
    ("last_transport_error","TEXT"),
]


def _existing_columns(conn, table: str) -> set[str]:
    cur = conn.cursor()
    backend = db_backend()

    if backend == "sqlite":
        cur.execute(f"PRAGMA table_info({table})")
        return {row["name"] for row in cur.fetchall()}

    cur.execute(
        aq(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            """
        ),
        (table,),
    )
    return {row["column_name"] for row in cur.fetchall()}


def run() -> None:
    conn = get_connection()
    cur = conn.cursor()
    backend = db_backend()
    log.info("Running fiscal poller migration backend=%s", backend)

    existing_cols = _existing_columns(conn, "fiscalization_checks")

    added = []
    for col_name, col_def in COLUMNS_TO_ADD:
        if col_name not in existing_cols:
            sql = f"ALTER TABLE fiscalization_checks ADD COLUMN {col_name} {col_def}"
            log.info("Adding column: %s", sql)
            cur.execute(sql)
            added.append(col_name)
        else:
            log.info("Column already exists, skipping: %s", col_name)

    # Индекс для быстрого выбора pending записей поллером
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_fiscal_checks_pending
        ON fiscalization_checks (status, next_poll_at, updated_at)
        WHERE status IN (1, 2, 5)
    """)

    conn.commit()
    conn.close()

    if added:
        log.info("Migration complete. Added columns: %s", added)
    else:
        log.info("Migration complete. No changes needed.")


if __name__ == "__main__":
    run()
