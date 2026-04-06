"""
app/db.py

Слой доступа к БД с поддержкой SQLite (dev) и PostgreSQL (prod).

Управляется переменной окружения DATABASE_URL:
  - не задана или начинается с "sqlite" → SQLite
  - начинается с "postgresql" / "postgres"  → PostgreSQL (через psycopg2)

Примеры DATABASE_URL:
  sqlite:///data/app.db
  postgresql://user:password@localhost:5432/evotor_ms
  postgresql://user:password@localhost:5432/evotor_ms?sslmode=require

Интерфейс get_connection() возвращает объект соединения с row_factory,
который позволяет обращаться к колонкам по имени: row["tenant_id"].
Это работает одинаково для SQLite (sqlite3.Row) и PostgreSQL
(psycopg2.extras.RealDictCursor / DictCursor).

Совместимость SQL:
  - Используй %s-плейсхолдеры если пишешь новый код, рассчитанный на PG.
  - Для совместимости со старым кодом на ? используй адаптер _adapt_query().
  - ON CONFLICT ... DO UPDATE — поддерживается и SQLite ≥ 3.24, и PG.
  - INTEGER PRIMARY KEY auto-increment — в PG замени на SERIAL/BIGSERIAL или
    оставь TEXT UUID (как сейчас).
  - PRAGMA — только SQLite, в PG не используются.
"""

import os
import logging

log = logging.getLogger("app.db")

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ---------------------------------------------------------------------------
# Определяем backend
# ---------------------------------------------------------------------------

def _is_postgres() -> bool:
    url = DATABASE_URL.strip()
    return url.startswith("postgresql") or url.startswith("postgres")


def _is_sqlite() -> bool:
    return not _is_postgres()


# ---------------------------------------------------------------------------
# SQLite backend
# ---------------------------------------------------------------------------

def _get_sqlite_path() -> str:
    url = DATABASE_URL.strip()
    if url.startswith("sqlite:///"):
        return url[len("sqlite:///"):]
    if url.startswith("sqlite://"):
        return url[len("sqlite://"):]
    # Если DATABASE_URL не задан — дефолт
    return "data/app.db"


def _get_sqlite_connection():
    import sqlite3

    path = _get_sqlite_path()
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)

    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


# ---------------------------------------------------------------------------
# PostgreSQL backend
# ---------------------------------------------------------------------------

def _get_pg_connection():
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        raise RuntimeError(
            "psycopg2 не установлен. Выполни: pip install psycopg2-binary"
        )

    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------

def get_connection():
    """
    Возвращает соединение с БД.

    SQLite:     строки доступны как row["column"] (sqlite3.Row)
    PostgreSQL: строки доступны как row["column"] (RealDictCursor)

    Не забывай вызывать conn.close() после использования,
    либо используй контекстный менеджер:

        conn = get_connection()
        try:
            ...
            conn.commit()
        finally:
            conn.close()
    """
    if _is_postgres():
        log.debug("Opening PostgreSQL connection")
        return _get_pg_connection()
    else:
        log.debug("Opening SQLite connection path=%s", _get_sqlite_path())
        return _get_sqlite_connection()


def adapt_query(sql: str) -> str:
    """
    Конвертирует SQLite-style placeholders (?) в PostgreSQL-style (%s).

    Используй при портировании старого кода:
        conn.execute(adapt_query("SELECT * FROM t WHERE id = ?"), (id,))

    В новом коде сразу пиши %s — они работают в обоих бэкендах
    через этот адаптер (? → %s для PG, %s → ? для SQLite не нужен
    т.к. SQLite тоже понимает %s через DBAPI2-совместимость).

    Примечание: SQLite НЕ поддерживает %s — поэтому для SQLite
    делаем обратную замену %s → ?.
    """
    if _is_postgres():
        return sql.replace("?", "%s")
    else:
        # На случай если код уже написан с %s
        return sql.replace("%s", "?")


# Короткий алиас
aq = adapt_query


def db_backend() -> str:
    """Возвращает 'postgresql' или 'sqlite' — для логов и диагностики."""
    return "postgresql" if _is_postgres() else "sqlite"