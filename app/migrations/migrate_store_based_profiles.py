import sqlite3
from app.db import get_connection
from app.logger import setup_logging
import logging

setup_logging()
log = logging.getLogger("migrate_store_based_profiles")


def run():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA index_list('tenants')")
    indexes = cur.fetchall()

    for idx in indexes:
        index_name = idx[1]
        is_unique = idx[2]

        cur.execute(f"PRAGMA index_info('{index_name}')")
        cols = [row[2] for row in cur.fetchall()]

        if is_unique and cols == ["evotor_user_id"]:
            log.info("Dropping unique index on evotor_user_id: %s", index_name)
            cur.execute(f'DROP INDEX IF EXISTS "{index_name}"')

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_tenants_evotor_user_id_lookup
        ON tenants(evotor_user_id)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_tenants_evotor_store_id_unique
        ON tenants(evotor_store_id)
        WHERE evotor_store_id IS NOT NULL
    """)

    conn.commit()
    conn.close()
    log.info("Store-based profiles migration complete")


if __name__ == "__main__":
    run()
