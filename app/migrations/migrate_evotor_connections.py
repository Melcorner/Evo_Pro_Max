import logging
from app.db import db_backend, get_connection
from app.logger import setup_logging

setup_logging()
log = logging.getLogger("migrate_evotor_connections")


def run() -> None:
    conn = get_connection()
    cur = conn.cursor()
    backend = db_backend()
    log.info("Running Evotor connections migration backend=%s", backend)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS evotor_connections (
        id TEXT PRIMARY KEY,
        evotor_user_id TEXT NOT NULL UNIQUE,
        evotor_token TEXT NOT NULL,
        stores_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )
    """)

    conn.commit()
    conn.close()
    log.info("Evotor connections migration complete backend=%s", backend)


if __name__ == "__main__":
    run()
