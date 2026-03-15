import json
import logging

from app.clients.moysklad_client import MoySkladClient
from app.mappers.sale_mapper import map_sale_to_ms, SalePayloadError, MappingNotFoundError

log = logging.getLogger("sale_handler")


def handle_sale(event_row):
    log.info(f"Handle sale event_id={event_row['id']} event_key={event_row['event_key']}")

    payload = json.loads(event_row["payload_json"])
    tenant_id = event_row["tenant_id"]

    try:
        ms_payload = map_sale_to_ms(payload, tenant_id=tenant_id)
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