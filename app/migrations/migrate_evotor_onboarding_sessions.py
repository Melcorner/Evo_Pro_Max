import logging
from app.db import get_connection
from app.logger import setup_logging

setup_logging()
log = logging.getLogger("migrate_evotor_onboarding_sessions")


def run() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS evotor_onboarding_sessions (
        id TEXT PRIMARY KEY,
        evotor_token TEXT NOT NULL,
        stores_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )
    """)

    conn.commit()
    conn.close()
    log.info("Evotor onboarding sessions migration complete")


if __name__ == "__main__":
    run()
