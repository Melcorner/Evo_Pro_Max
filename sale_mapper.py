import logging

log = logging.getLogger("sale_mapper")


def map_sale_to_ms(payload):

    log.info("Mapping sale payload")

    amount = payload.get("amount", 0)

    ms_payload = {
        "name": f"Sale {payload.get('event_id')}",
        "description": "Created from Evotor webhook",
        "sum": amount * 100
    }

    return ms_payload