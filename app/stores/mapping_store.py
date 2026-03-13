# mapping_store.py
import logging
import time
from typing import Optional

from app.db import get_connection

log = logging.getLogger("mapping_store")


class MappingStore:
    """Хранилище маппинга Evotor ID <-> MS ID."""

    def __init__(self, db_path: Optional[str] = None):
        """
        Инициализация хранилища.
        
        Args:
            db_path: Путь к БД. Если None — используется стандартный get_connection().
                     Для тестов можно передать ":memory:" (in-memory БД).
        """
        self._db_path = db_path
        self._conn = None
        
        # Для in-memory БД держим коннект открытым (иначе БД исчезнет)
        if self._db_path == ":memory:":
            import sqlite3
            self._conn = sqlite3.connect(":memory:", check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        
        self._init_schema()

    def _get_conn(self):
        """Получить соединение."""
        if self._conn:
            return self._conn
        
        if self._db_path:
            import sqlite3
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn
        
        return get_connection()

    def _init_schema(self):
        """Создание таблицы и индексов (идемпотентно)."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mappings (
                tenant_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                evotor_id TEXT NOT NULL,
                ms_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,

                UNIQUE (tenant_id, entity_type, evotor_id),
                UNIQUE (tenant_id, entity_type, ms_id)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_mappings_evotor 
                ON mappings(tenant_id, entity_type, evotor_id)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_mappings_ms 
                 ON mappings(tenant_id, entity_type, ms_id)
        """)
        
        conn.commit()
        
        # Для in-memory не закрываем коннект!
        if not self._conn:
            conn.close()
        
        log.debug("Mapping schema initialized")

    def get_by_evotor_id(self, tenant_id: str, entity_type: str, evotor_id: str) -> Optional[str]:
        """Получить ms_id по evotor_id."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute(
            """
             SELECT ms_id
             FROM mappings
             WHERE tenant_id=? AND entity_type=? AND evotor_id=?
            """,
            (tenant_id, entity_type, evotor_id)
        )
        row = cursor.fetchone()
        
        if not self._conn:
            conn.close()
        
        return row["ms_id"] if row else None

    def get_by_ms_id(self, tenant_id: str, entity_type: str, ms_id: str) -> Optional[str]:
        """Получить evotor_id по ms_id."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT evotor_id
            FROM mappings
            WHERE tenant_id=? AND entity_type=? AND ms_id=?
            """,
            (tenant_id, entity_type, ms_id)
        )
        row = cursor.fetchone()
        
        if not self._conn:
            conn.close()
        
        return row["evotor_id"] if row else None

    def upsert_mapping(self, tenant_id: str, entity_type: str, evotor_id: str, ms_id: str) -> bool:
        """
        Создать или обновить маппинг.
        
        Returns:
            True если успешно, False если ms_id уже занят другим evotor_id.
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        now = int(time.time())
        try:
            cursor.execute(
                """
                SELECT evotor_id
                FROM mappings
                WHERE tenant_id=? AND entity_type=? AND ms_id=?
                """,
                (tenant_id, entity_type, ms_id)
            )
            existing = cursor.fetchone()
            
            if existing and existing["evotor_id"] != evotor_id:
                log.warning(
                    f"Upsert conflict: ms_id={ms_id} already mapped to evotor_id={existing['evotor_id']}"
                )
                return False

            cursor.execute("""
                INSERT INTO mappings (
                tenant_id,
                entity_type,
                evotor_id,
                ms_id,
                created_at,
                updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, entity_type, evotor_id)
                DO UPDATE SET
                ms_id=excluded.ms_id,
                updated_at=excluded.updated_at
                """, (
                    tenant_id,
                    entity_type,
                    evotor_id,
                    ms_id,
                    now,
                    now
                ))
            
            conn.commit()
            log.debug(f"Upserted mapping: {evotor_id} <-> {ms_id}")
            return True
            
        except Exception as e:
            conn.rollback()
            log.error(f"Upsert failed: {e}")
            return False
        finally:
            if not self._conn:
                conn.close()

    def close(self):
        """Закрыть хранилище (важно для in-memory БД)."""
        if self._conn:
            self._conn.close()
            self._conn = None


# ==============================================================================
# SELF-TEST (in-memory)
# ==============================================================================
if __name__ == "__main__":
    import sys

    print(">>> Running self-tests (in-memory DB)...")
    passed = 0

    store = MappingStore(db_path=":memory:")

    try:

        # ------------------------------------------------
        # Test 1: Базовое создание записи и чтение
        # ------------------------------------------------

        assert store.upsert_mapping("tenant1", "product", "evo_1", "ms_100") is True

        assert store.get_by_evotor_id("tenant1", "product", "evo_1") == "ms_100"
        assert store.get_by_ms_id("tenant1", "product", "ms_100") == "evo_1"

        passed += 1
        print("[OK] Test 1: Basic create & read")


        # ------------------------------------------------
        # Test 2: Обновить маппинг
        # ------------------------------------------------

        assert store.upsert_mapping("tenant1", "product", "evo_1", "ms_101") is True

        assert store.get_by_evotor_id("tenant1", "product", "evo_1") == "ms_101"
        assert store.get_by_ms_id("tenant1", "product", "ms_100") is None

        passed += 1
        print("[OK] Test 2: Update mapping")


        # ------------------------------------------------
        # Test 3: Проверка конфликта (один ms_id для разных evotor_id)
        # ------------------------------------------------

        assert store.upsert_mapping("tenant1", "product", "evo_2", "ms_200") is True

        # конфликт ms_id
        assert store.upsert_mapping("tenant1", "product", "evo_3", "ms_200") is False

        assert store.get_by_evotor_id("tenant1", "product", "evo_2") == "ms_200"
        assert store.get_by_evotor_id("tenant1", "product", "evo_3") is None

        passed += 1
        print("[OK] Test 3: ms_id conflict check")


        # ------------------------------------------------
        # Test 4: Разделение данных между тенантами (multi-tenant)
        # ------------------------------------------------

        assert store.upsert_mapping("tenant2", "product", "evo_2", "ms_200") is True

        assert store.get_by_evotor_id("tenant2", "product", "evo_2") == "ms_200"

        passed += 1
        print("[OK] Test 4: multi-tenant allowed")


        # ------------------------------------------------
        # Test 5: Чение несуществующего маппинга
        # ------------------------------------------------

        assert store.get_by_evotor_id("tenant1", "product", "unknown") is None
        assert store.get_by_ms_id("tenant1", "product", "unknown") is None

        passed += 1
        print("[OK] Test 5: Non-existent keys")


        # ------------------------------------------------
        # Test 6: Проверка использования индекса
        # ------------------------------------------------

        conn = store._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            "EXPLAIN QUERY PLAN SELECT ms_id FROM mappings WHERE tenant_id=? AND entity_type=? AND evotor_id=?",
            ("tenant1", "product", "evo_1")
        )

        plan = cursor.fetchone()[-1]

        assert "INDEX" in plan or "USING" in plan

        passed += 1
        print("[OK] Test 6: Index usage verified")


        print(f"\n>>> ALL {passed}/6 TESTS PASSED <<<")

    except AssertionError as e:
        print(f"\n>>> TEST FAILED: {e}")

        import traceback
        traceback.print_exc()

        sys.exit(1)

    finally:
        store.close()