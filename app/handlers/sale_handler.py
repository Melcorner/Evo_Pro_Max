import json
import logging
import time
import requests

from app.clients.moysklad_client import MoySkladClient
from app.mappers.sale_mapper import map_sale_to_ms, map_sale_to_ms_retail_demand, SalePayloadError, MappingNotFoundError
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
                    COALESCE(ts.ms_agent_id, t.ms_agent_id) AS ms_agent_id,
                    COALESCE(NULLIF(ts.sale_document_mode, ''), 'demand') AS sale_document_mode,
                    ts.ms_retail_store_id AS ms_retail_store_id,
                    ts.ms_retail_shift_id AS ms_retail_shift_id,
                    ts.ms_cashier_id AS ms_cashier_id
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
            SELECT
                ms_organization_id,
                ms_store_id,
                ms_agent_id,
                'demand' AS sale_document_mode,
                NULL AS ms_retail_store_id,
                NULL AS ms_retail_shift_id,
                NULL AS ms_cashier_id
            FROM tenants WHERE id = ?
            """),
            (tenant_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()



def _ms_meta(base_url: str, entity_type: str, entity_id: str) -> dict:
    return {
        "meta": {
            "href": f"{base_url}/entity/{entity_type}/{entity_id}",
            "type": entity_type,
            "mediaType": "application/json",
        }
    }


def _save_retail_shift_id(tenant_id: str, evotor_store_id: str, shift_id: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("""
            UPDATE tenant_stores
            SET ms_retail_shift_id = ?,
                updated_at = ?
            WHERE tenant_id = ?
              AND evotor_store_id = ?
            """),
            (shift_id, int(time.time()), tenant_id, evotor_store_id),
        )
        conn.commit()
    finally:
        conn.close()


def _ensure_retail_shift(
    tenant_id: str,
    evotor_store_id: str,
    ms_config: dict,
) -> str:
    """
    Возвращает retailShift для создания retaildemand.

    Если ms_retail_shift_id уже сохранён — используем его.
    Если не сохранён — ищем смену по externalCode.
    Если не нашли — создаём новую смену и сохраняем её в tenant_stores.
    """
    current_shift_id = str(ms_config.get("ms_retail_shift_id") or "").strip()
    if current_shift_id:
        return current_shift_id

    retail_store_id = str(ms_config.get("ms_retail_store_id") or "").strip()
    organization_id = str(ms_config.get("ms_organization_id") or "").strip()
    store_id = str(ms_config.get("ms_store_id") or "").strip()

    if not retail_store_id:
        raise SalePayloadError("retaildemand mode requires ms_retail_store_id")
    if not organization_id:
        raise SalePayloadError("retaildemand mode requires ms_organization_id")
    if not store_id:
        raise SalePayloadError("retaildemand mode requires ms_store_id")

    client = MoySkladClient(tenant_id)
    base_url = client.BASE_URL
    headers = client._headers()

    base_external_code = f"evomspro-shift-{tenant_id[:8]}-{evotor_store_id[:8]}"
    external_code = base_external_code

    # 1. Ищем уже созданную смену.
    r = requests.get(
        f"{base_url}/entity/retailshift",
        headers=headers,
        params={"filter": f"externalCode={base_external_code}", "limit": 10},
        timeout=30,
    )
    client._handle_error(r)

    rows = r.json().get("rows", [])
    for row in rows:
        shift_id = row.get("id")
        close_date = row.get("closeDate")
        if shift_id and not close_date:
            _save_retail_shift_id(tenant_id, evotor_store_id, shift_id)
            ms_config["ms_retail_shift_id"] = shift_id
            log.info(
                "Retail shift found tenant_id=%s store=%s shift_id=%s externalCode=%s",
                tenant_id,
                evotor_store_id,
                shift_id,
                base_external_code,
            )
            return shift_id

    # Если нашли только закрытые смены с таким externalCode, создаём новую с уникальным externalCode.
    if rows:
        external_code = f"{base_external_code}-{int(time.time())}"

    payload = {
        "name": f"EvomsPro shift {int(time.time())}",
        "externalCode": external_code,
        "organization": _ms_meta(base_url, "organization", organization_id),
        "retailStore": _ms_meta(base_url, "retailstore", retail_store_id),
        "store": _ms_meta(base_url, "store", store_id),
        "vatEnabled": False,
        "vatIncluded": True,
    }

    r = requests.post(
        f"{base_url}/entity/retailshift",
        headers=headers,
        json=payload,
        timeout=30,
    )
    client._handle_error(r)

    data = r.json()
    shift_id = data.get("id")
    if not shift_id:
        raise SalePayloadError("MoySklad retailshift created without id")

    _save_retail_shift_id(tenant_id, evotor_store_id, shift_id)
    ms_config["ms_retail_shift_id"] = shift_id

    log.info(
        "Retail shift created tenant_id=%s store=%s shift_id=%s externalCode=%s",
        tenant_id,
        evotor_store_id,
        shift_id,
        external_code,
    )

    return shift_id


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

    sale_document_mode = str(ms_config.get("sale_document_mode") or "demand").strip().lower()
    client = MoySkladClient(tenant_id)

    try:
        if sale_document_mode == "retaildemand":
            ms_config["ms_retail_shift_id"] = _ensure_retail_shift(
                tenant_id=tenant_id,
                evotor_store_id=evotor_store_id,
                ms_config=ms_config,
            )

            ms_payload = map_sale_to_ms_retail_demand(
                payload,
                sync_id=event_row["id"],
                tenant_id=tenant_id,
                ms_organization_id=ms_config.get("ms_organization_id"),
                ms_store_id=ms_config.get("ms_store_id"),
                ms_agent_id=resolved_ms_agent_id,
                ms_retail_store_id=ms_config.get("ms_retail_store_id"),
                ms_retail_shift_id=ms_config.get("ms_retail_shift_id"),
                ms_cashier_id=ms_config.get("ms_cashier_id"),
                counterparty_resolution_source=resolution_source,
                evotor_store_id=evotor_store_id,
            )
            result = client.create_retail_demand(ms_payload)
        else:
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
            result = client.create_sale_document(ms_payload)

    except SalePayloadError as e:
        log.error(f"Invalid sale payload event_id={event_row['id']} mode={sale_document_mode} err={e}")
        raise
    except MappingNotFoundError as e:
        log.error(f"Mapping not found event_id={event_row['id']} tenant_id={tenant_id} mode={sale_document_mode} err={e}")
        raise

    result_ref = result["result_ref"]

    log.info(
        "Sale sent to MoySklad event_id=%s mode=%s result_ref=%s idempotent=%s",
        event_row["id"],
        sale_document_mode,
        result_ref,
        result.get("idempotent"),
    )

    return result_ref
