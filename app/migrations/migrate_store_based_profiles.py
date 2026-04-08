import logging

from app.db import db_backend, get_connection
from app.logger import setup_logging

setup_logging()
log = logging.getLogger("migrate_store_based_profiles")


def run() -> None:
    backend = db_backend()
    if backend != "sqlite":
        raise RuntimeError(
            "migrate_store_based_profiles is a legacy SQLite-only cleanup migration. "
            "For PostgreSQL use init_db + migrate_evotor_multitenant instead."
        )

    conn = get_connection()
    cur = conn.cursor()
    log.info("Running legacy store-based profiles migration backend=%s", backend)

    cur.execute("PRAGMA index_list('tenants')")
    indexes = cur.fetchall()

    for idx in indexes:
        index_name = idx[1]
        is_unique = idx[2]

        cur.execute(f"PRAGMA index_info('{index_name}')")
        cols = [row[2] for row in cur.fetchall()]

        if is_unique and cols == ["evotor_user_id"]:
            log.info("Dropping unique index on evotor_user_id: %s", index_name)
            try:
                cur.execute(f'DROP INDEX IF EXISTS "{index_name}"')
            except Exception as exc:
                raise RuntimeError(
                    "Found legacy UNIQUE constraint/index on tenants.evotor_user_id that cannot be dropped "
                    "automatically. Rebuild the SQLite tenants table or migrate via the current init_db schema."
                ) from exc

    for index_name in ("idx_tenants_evotor_user_id", "idx_tenants_evotor_store_id"):
        log.info("Dropping legacy tenant routing index if exists: %s", index_name)
        cur.execute(f'DROP INDEX IF EXISTS "{index_name}"')

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tenants_evotor_user_id_lookup
        ON tenants(evotor_user_id)
        """
    )

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_tenants_evotor_store_id_unique
        ON tenants(evotor_store_id)
        WHERE evotor_store_id IS NOT NULL AND TRIM(evotor_store_id) <> ''
        """
    )

    conn.commit()
    conn.close()
    log.info("Store-based profiles migration complete backend=%s", backend)


if __name__ == "__main__":
    run()
