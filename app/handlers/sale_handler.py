import json
import logging

from app.clients.moysklad_client import MoySkladClient
from app.mappers.sale_mapper import map_sale_to_ms, SalePayloadError, MappingNotFoundError
from app.services.counterparty_resolver import resolve_counterparty_for_sale
from app.db import get_connection, adapt_query as aq

log = logging.getLogger("sale_handler")


def _extract_evotor_store_id(payload: dict) -> str | None:
    """Достаёт store_id из разных форматов webhook payload Эвотор."""
    store_id = (
        payload.get("store_id")
        or payload.get("storeId")
        or payload.get("evotor_store_id")
    )
    if store_id:
        return str(store_id)

    source_data = payload.get("source_data") or {}
    if isinstance(source_data, dict):
        source_store_id = source_data.get("storeId") or source_data.get("store_id")
        if source_store_id:
            return str(source_store_id)

    return None


def _load_ms_config(tenant_id: str, evotor_store_id: str | None = None) -> dict:
    """
    Загружает конфигурацию МойСклад.

    При мультимагазинности настройки склада/организации берутся из tenant_stores
    для конкретного evotor_store_id. Если store-level поля не заполнены, остаётся
    fallback на tenant-level настройки.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        if evotor_store_id:
            cursor.execute(
                aq("""
                SELECT
                    COALESCE(ts.ms_organization_id, t.ms_organization_id) AS ms_organization_id,
                    COALESCE(ts.ms_store_id, t.ms_store_id) AS ms_store_id,
                    COALESCE(ts.ms_agent_id, t.ms_agent_id) AS ms_agent_id
                FROM tenants t
                LEFT JOIN tenant_stores ts
                  ON ts.tenant_id = t.id
                 AND ts.evotor_store_id = ?
                WHERE t.id = ?
                """),
                (evotor_store_id, tenant_id),
            )
            row = cursor.fetchone()
            if row:
                return dict(row)

        cursor.execute(
            aq("""
            SELECT ms_organization_id, ms_store_id, ms_agent_id
            FROM tenants WHERE id = ?
            """),
            (tenant_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def handle_sale(event_row):
    log.info(f"Handle sale event_id={event_row['id']} event_key={event_row['event_key']}")

    payload = json.loads(event_row["payload_json"])
    tenant_id = event_row["tenant_id"]
    evotor_store_id = _extract_evotor_store_id(payload)

    ms_config = _load_ms_config(tenant_id, evotor_store_id)
    default_ms_agent_id = ms_config.get("ms_agent_id")

    resolved_ms_agent_id, resolution_source = resolve_counterparty_for_sale(
        payload=payload,
        tenant_id=tenant_id,
        default_ms_agent_id=default_ms_agent_id,
    )
    log.info(
        "Resolved counterparty event_id=%s source=%s agent_id=%s store=%s",
        event_row["id"],
        resolution_source,
        resolved_ms_agent_id,
        evotor_store_id,
    )

    try:
        ms_payload = map_sale_to_ms(
            payload,
            sync_id=event_row["id"],
            tenant_id=tenant_id,
            ms_organization_id=ms_config.get("ms_organization_id"),
            ms_store_id=ms_config.get("ms_store_id"),
            ms_agent_id=resolved_ms_agent_id,
            counterparty_resolution_source=resolution_source,
            evotor_store_id=evotor_store_id,
        )
    except SalePayloadError as e:
        log.error(f"Invalid sale payload event_id={event_row['id']} err={e}")
        raise
    except MappingNotFoundError as e:
        log.error(f"Mapping not found event_id={event_row['id']} tenant_id={tenant_id} err={e}")
        raise

    client = MoySkladClient(tenant_id)
    result = client.create_sale_document(ms_payload)

    result_ref = result["result_ref"]

    log.info(f"Sale sent to MoySklad event_id={event_row['id']} result_ref={result_ref}")

    return result_ref
