# mapping_store.py
import logging
import time
from typing import Optional

from app.db import get_connection, adapt_query as aq

log = logging.getLogger("mapping_store")


class MappingStore:
    """Хранилище маппинга Evotor ID <-> MS ID с поддержкой store-level изоляции."""

    def get_by_evotor_id(
        self,
        tenant_id: str,
        entity_type: str,
        evotor_id: str,
        evotor_store_id: str | None = None,
    ) -> Optional[str]:
        """Получить ms_id по evotor_id."""
        conn = get_connection()
        try:
            cur = conn.cursor()
            if evotor_store_id:
                cur.execute(
                    aq("""
                    SELECT ms_id FROM mappings
                    WHERE tenant_id = ? AND evotor_store_id = ?
                      AND entity_type = ? AND evotor_id = ?
                    """),
                    (tenant_id, evotor_store_id, entity_type, evotor_id),
                )
            else:
                cur.execute(
                    aq("""
                    SELECT ms_id FROM mappings
                    WHERE tenant_id = ? AND entity_type = ? AND evotor_id = ?
                    LIMIT 1
                    """),
                    (tenant_id, entity_type, evotor_id),
                )
            row = cur.fetchone()
            return row["ms_id"] if row else None
        finally:
            conn.close()

    def get_by_ms_id(
        self,
        tenant_id: str,
        entity_type: str,
        ms_id: str,
        evotor_store_id: str | None = None,
    ) -> Optional[str]:
        """Получить evotor_id по ms_id."""
        conn = get_connection()
        try:
            cur = conn.cursor()
            if evotor_store_id:
                cur.execute(
                    aq("""
                    SELECT evotor_id FROM mappings
                    WHERE tenant_id = ? AND evotor_store_id = ?
                      AND entity_type = ? AND ms_id = ?
                    """),
                    (tenant_id, evotor_store_id, entity_type, ms_id),
                )
            else:
                cur.execute(
                    aq("""
                    SELECT evotor_id FROM mappings
                    WHERE tenant_id = ? AND entity_type = ? AND ms_id = ?
                    LIMIT 1
                    """),
                    (tenant_id, entity_type, ms_id),
                )
            row = cur.fetchone()
            return row["evotor_id"] if row else None
        finally:
            conn.close()

    def upsert_mapping(
        self,
        tenant_id: str,
        entity_type: str,
        evotor_id: str,
        ms_id: str,
        evotor_store_id: str | None = None,
    ) -> bool:
        """
        Создать или обновить маппинг.
        evotor_store_id обязателен для store-aware операций.
        Returns True если успешно, False при конфликте.
        """
        if not evotor_store_id:
            log.warning(
                "upsert_mapping called without evotor_store_id tenant_id=%s evotor_id=%s",
                tenant_id, evotor_id,
            )
            return False

        conn = get_connection()
        now = int(time.time())
        try:
            cur = conn.cursor()

            # Проверяем конфликт в рамках того же магазина
            cur.execute(
                aq("""
                SELECT evotor_id FROM mappings
                WHERE tenant_id = ? AND evotor_store_id = ?
                  AND entity_type = ? AND ms_id = ?
                """),
                (tenant_id, evotor_store_id, entity_type, ms_id),
            )
            existing = cur.fetchone()

            if existing and existing["evotor_id"] != evotor_id:
                log.warning(
                    "Upsert conflict: ms_id=%s already mapped to evotor_id=%s in store=%s",
                    ms_id, existing["evotor_id"], evotor_store_id,
                )
                return False

            cur.execute(
                aq("""
                INSERT INTO mappings (
                    tenant_id, evotor_store_id, entity_type,
                    evotor_id, ms_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (tenant_id, evotor_store_id, entity_type, evotor_id)
                DO UPDATE SET
                    ms_id = excluded.ms_id,
                    updated_at = excluded.updated_at
                """),
                (tenant_id, evotor_store_id, entity_type, evotor_id, ms_id, now, now),
            )

            conn.commit()
            log.debug(
                "Upserted mapping: %s <-> %s store=%s",
                evotor_id, ms_id, evotor_store_id,
            )
            return True

        except Exception as e:
            conn.rollback()
            log.error("Upsert failed: %s", e)
            return False
        finally:
            conn.close()

    def delete_by_ms_id(
        self,
        tenant_id: str,
        entity_type: str,
        ms_id: str,
        evotor_store_id: str | None = None,
    ) -> bool:
        """Удалить маппинг по ms_id."""
        conn = get_connection()
        try:
            cur = conn.cursor()
            if evotor_store_id:
                cur.execute(
                    aq("""
                    DELETE FROM mappings
                    WHERE tenant_id = ? AND evotor_store_id = ?
                      AND entity_type = ? AND ms_id = ?
                    """),
                    (tenant_id, evotor_store_id, entity_type, ms_id),
                )
            else:
                cur.execute(
                    aq("""
                    DELETE FROM mappings
                    WHERE tenant_id = ? AND entity_type = ? AND ms_id = ?
                    """),
                    (tenant_id, entity_type, ms_id),
                )
            conn.commit()
            return cur.rowcount > 0
        except Exception as e:
            conn.rollback()
            log.error("Delete mapping failed ms_id=%s err=%s", ms_id, e)
            return False
        finally:
            conn.close()

    def get_all_ms_ids(
        self,
        tenant_id: str,
        entity_type: str,
        evotor_store_id: str | None = None,
    ) -> list[dict]:
        """Получить все маппинги — можно фильтровать по магазину."""
        conn = get_connection()
        try:
            cur = conn.cursor()
            if evotor_store_id:
                cur.execute(
                    aq("""
                    SELECT ms_id, evotor_id, evotor_store_id FROM mappings
                    WHERE tenant_id = ? AND evotor_store_id = ? AND entity_type = ?
                    """),
                    (tenant_id, evotor_store_id, entity_type),
                )
            else:
                cur.execute(
                    aq("""
                    SELECT ms_id, evotor_id, evotor_store_id FROM mappings
                    WHERE tenant_id = ? AND entity_type = ?
                    """),
                    (tenant_id, entity_type),
                )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def list_by_store(
        self,
        tenant_id: str,
        evotor_store_id: str,
        entity_type: str = "product",
    ) -> list[dict]:
        """Получить все маппинги конкретного магазина."""
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                aq("""
                SELECT tenant_id, evotor_store_id, entity_type,
                       evotor_id, ms_id, created_at, updated_at
                FROM mappings
                WHERE tenant_id = ? AND evotor_store_id = ? AND entity_type = ?
                ORDER BY created_at ASC
                """),
                (tenant_id, evotor_store_id, entity_type),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def delete_by_store(self, tenant_id: str, evotor_store_id: str) -> int:
        """Удалить все маппинги магазина (для сброса синхронизации)."""
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                aq("""
                DELETE FROM mappings
                WHERE tenant_id = ? AND evotor_store_id = ?
                """),
                (tenant_id, evotor_store_id),
            )
            conn.commit()
            return cur.rowcount
        except Exception as e:
            conn.rollback()
            log.error("delete_by_store failed store=%s err=%s", evotor_store_id, e)
            return 0
        finally:
            conn.close()

    # Backward compat alias
    def upsert(self, **kwargs):
        return self.upsert_mapping(**kwargs)
