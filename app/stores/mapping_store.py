# mapping_store.py
import logging
import time
from typing import Optional

from app.db import get_connection, adapt_query as aq

log = logging.getLogger("mapping_store")


class MappingStore:
    """Хранилище маппинга Evotor ID <-> MS ID."""

    def get_by_evotor_id(self, tenant_id: str, entity_type: str, evotor_id: str) -> Optional[str]:
        """Получить ms_id по evotor_id."""
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                aq("""
                SELECT ms_id FROM mappings
                WHERE tenant_id = ? AND entity_type = ? AND evotor_id = ?
                """),
                (tenant_id, entity_type, evotor_id),
            )
            row = cur.fetchone()
            return row["ms_id"] if row else None
        finally:
            conn.close()

    def get_by_ms_id(self, tenant_id: str, entity_type: str, ms_id: str) -> Optional[str]:
        """Получить evotor_id по ms_id."""
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                aq("""
                SELECT evotor_id FROM mappings
                WHERE tenant_id = ? AND entity_type = ? AND ms_id = ?
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
    ) -> bool:
        """
        Создать или обновить маппинг.

        Returns:
            True если успешно, False если ms_id уже занят другим evotor_id.
        """
        conn = get_connection()
        now = int(time.time())
        try:
            cur = conn.cursor()

            # Проверяем конфликт: ms_id уже привязан к другому evotor_id
            cur.execute(
                aq("""
                SELECT evotor_id FROM mappings
                WHERE tenant_id = ? AND entity_type = ? AND ms_id = ?
                """),
                (tenant_id, entity_type, ms_id),
            )
            existing = cur.fetchone()

            if existing and existing["evotor_id"] != evotor_id:
                log.warning(
                    "Upsert conflict: ms_id=%s already mapped to evotor_id=%s",
                    ms_id,
                    existing["evotor_id"],
                )
                return False

            cur.execute(
                aq("""
                INSERT INTO mappings (
                    tenant_id, entity_type, evotor_id, ms_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (tenant_id, entity_type, evotor_id)
                DO UPDATE SET
                    ms_id = excluded.ms_id,
                    updated_at = excluded.updated_at
                """),
                (tenant_id, entity_type, evotor_id, ms_id, now, now),
            )

            conn.commit()
            log.debug("Upserted mapping: %s <-> %s", evotor_id, ms_id)
            return True

        except Exception as e:
            conn.rollback()
            log.error("Upsert failed: %s", e)
            return False
        finally:
            conn.close()