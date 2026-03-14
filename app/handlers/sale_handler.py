import json
import logging

from app.clients.moysklad_client import MoySkladClient
from app.mappers.sale_mapper import map_sale_to_ms

log = logging.getLogger("sale_handler")


def handle_sale(event_row):
    log.info(f"Handle sale event_id={event_row['id']} event_key={event_row['event_key']}")

    payload = json.loads(event_row["payload_json"])
    ms_payload = map_sale_to_ms(payload)

    client = MoySkladClient(event_row["tenant_id"])
    result = client.create_sale_document(ms_payload)
    
    result_ref = result["result_ref"]

    log.info(f"Sale sent to MoySklad event_id={event_row['id']} result_ref={result_ref}")

    return result_ref