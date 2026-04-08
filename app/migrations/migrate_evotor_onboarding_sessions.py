import logging

from app.db import adapt_query as aq, db_backend, get_connection
from app.logger import setup_logging

setup_logging()
log = logging.getLogger("migrate_evotor_onboarding_sessions")


def _column_exists(conn, table: str, column: str) -> bool:
    cur = conn.cursor()
    if db_backend() == "sqlite":
        cur.execute(f"PRAGMA table_info({table})")
        return column in {row["name"] for row in cur.fetchall()}

    cur.execute(
        aq(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ? AND column_name = ?
            """
        ),
        (table, column),
    )
    return cur.fetchone() is not None


def run() -> None:
    conn = get_connection()
    cur = conn.cursor()
    backend = db_backend()
    log.info("Running Evotor onboarding sessions migration backend=%s", backend)

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS evotor_onboarding_sessions (
            id TEXT PRIMARY KEY,
            evotor_token TEXT NOT NULL,
            stores_json TEXT NOT NULL,
            moysklad_token TEXT,
            ms_data_json TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )

    for column, definition in (
        ("moysklad_token", "TEXT"),
        ("ms_data_json", "TEXT"),
    ):
        if not _column_exists(conn, "evotor_onboarding_sessions", column):
            cur.execute(
                f"ALTER TABLE evotor_onboarding_sessions ADD COLUMN {column} {definition}"
            )
            log.info("Added column evotor_onboarding_sessions.%s", column)

    conn.commit()
    conn.close()
    log.info("Evotor onboarding sessions migration complete backend=%s", backend)


if __name__ == "__main__":
    run()
