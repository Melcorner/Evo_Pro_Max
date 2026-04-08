"""
Миграция под store-based routing Evotor webhook'ов.

Что делает:
- проверяет дубли по tenants.evotor_store_id
- приводит tenant routing indexes к текущей схеме:
  - evotor_user_id -> lookup index
  - evotor_store_id -> partial UNIQUE index

Запуск:
    python -m app.migrations.migrate_evotor_multitenant
"""

import logging

from app.db import adapt_query as aq, db_backend, get_connection
from app.logger import setup_logging

setup_logging()
log = logging.getLogger("migrate_evotor_multitenant")

LEGACY_INDEXES_TO_DROP = (
    "idx_tenants_evotor_user_id_unique",
    "idx_tenants_evotor_user_id",
    "idx_tenants_evotor_store_id",
)


def _row_value(row, key: str, position: int):
    try:
        return row[key]
    except (KeyError, TypeError, IndexError):
        return row[position]


def _find_duplicates(cursor, column_name: str) -> list[tuple[str, int]]:
    cursor.execute(
        f"""
        SELECT {column_name} AS duplicate_value, COUNT(*) AS cnt
        FROM tenants
        WHERE {column_name} IS NOT NULL AND TRIM({column_name}) <> ''
        GROUP BY {column_name}
        HAVING COUNT(*) > 1
        """
    )
    return [
        (_row_value(row, "duplicate_value", 0), _row_value(row, "cnt", 1))
        for row in cursor.fetchall()
    ]


def _index_exists(conn, index_name: str) -> bool:
    cur = conn.cursor()
    if db_backend() == "sqlite":
        cur.execute(
            aq("SELECT name FROM sqlite_master WHERE type='index' AND name = ?"),
            (index_name,),
        )
    else:
        cur.execute(
            aq("SELECT indexname FROM pg_indexes WHERE indexname = ?"),
            (index_name,),
        )
    return cur.fetchone() is not None


def _drop_index_if_exists(conn, index_name: str) -> bool:
    if not _index_exists(conn, index_name):
        return False
    conn.cursor().execute(f'DROP INDEX IF EXISTS "{index_name}"')
    return True


def _discover_legacy_user_unique_indexes(conn) -> set[str]:
    names = set(LEGACY_INDEXES_TO_DROP)

    if db_backend() != "sqlite":
        return names

    cur = conn.cursor()
    cur.execute("PRAGMA index_list('tenants')")
    for row in cur.fetchall():
        index_name = _row_value(row, "name", 1)
        is_unique = _row_value(row, "unique", 2)
        if not is_unique:
            continue

        cur.execute(f"PRAGMA index_info('{index_name}')")
        columns = [_row_value(col, "name", 2) for col in cur.fetchall()]
        if columns == ["evotor_user_id"]:
            names.add(index_name)

    return names


def run() -> None:
    conn = get_connection()
    backend = db_backend()
    log.info("Running Evotor multitenant migration backend=%s", backend)

    try:
        cur = conn.cursor()

        dup_stores = _find_duplicates(cur, "evotor_store_id")
        if dup_stores:
            log.error("Duplicate evotor_store_id found: %s", dup_stores)
            raise RuntimeError(
                "Cannot create UNIQUE store routing index: resolve duplicate tenants.evotor_store_id first."
            )

        for index_name in sorted(_discover_legacy_user_unique_indexes(conn)):
            if _drop_index_if_exists(conn, index_name):
                log.info("Dropped legacy tenant routing index: %s", index_name)

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
        log.info("Evotor multitenant migration complete backend=%s", backend)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
