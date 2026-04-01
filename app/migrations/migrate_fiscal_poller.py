"""
migrate_fiscal_poller.py — добавляет колонки для fiscal_poller в fiscalization_checks.

Запустить один раз перед деплоем fiscal_poller:
    python -m app.migrations.migrate_fiscal_poller

Миграция идемпотентна: повторный запуск безопасен.
"""

import logging
from app.db import get_connection
from app.logger import setup_logging

setup_logging()
log = logging.getLogger("migrate_fiscal_poller")

COLUMNS_TO_ADD = [
    ("attempt",             "INTEGER NOT NULL DEFAULT 0"),
    ("last_poll_at",        "INTEGER"),
    ("next_poll_at",        "INTEGER"),
    ("last_transport_error","TEXT"),
]


def run() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(fiscalization_checks)")
    existing_cols = {row["name"] for row in cur.fetchall()}

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
