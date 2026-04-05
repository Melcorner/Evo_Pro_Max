"""
Миграция под мультитенант webhook routing Эвотор.

Что делает:
- проверяет дубли по tenants.evotor_user_id и tenants.evotor_store_id
- если дублей нет, создаёт partial UNIQUE indexes для однозначного резолва tenant

Запуск:
    python -m app.migrations.migrate_evotor_multitenant
"""

import logging

from app.db import get_connection
from app.logger import setup_logging

setup_logging()
log = logging.getLogger("migrate_evotor_multitenant")


def _find_duplicates(cursor, column_name: str) -> list[tuple[str, int]]:
    cursor.execute(
        f"""
        SELECT {column_name}, COUNT(*) AS cnt
        FROM tenants
        WHERE {column_name} IS NOT NULL AND TRIM({column_name}) <> ''
        GROUP BY {column_name}
        HAVING COUNT(*) > 1
        """
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]


def run() -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()

        dup_users = _find_duplicates(cur, "evotor_user_id")
        dup_stores = _find_duplicates(cur, "evotor_store_id")

        if dup_users or dup_stores:
            if dup_users:
                log.error("Duplicate evotor_user_id found: %s", dup_users)
            if dup_stores:
                log.error("Duplicate evotor_store_id found: %s", dup_stores)
            raise RuntimeError(
                "Cannot create UNIQUE indexes for Evotor multitenant routing: resolve duplicates in tenants first."
            )

        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tenants_evotor_user_id_unique
            ON tenants(evotor_user_id)
            WHERE evotor_user_id IS NOT NULL AND TRIM(evotor_user_id) <> ''
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
        log.info("Evotor multitenant migration complete")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
