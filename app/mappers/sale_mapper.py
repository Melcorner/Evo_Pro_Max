import logging
from app.stores.mapping_store import MappingStore

log = logging.getLogger("sale_mapper")


def map_sale_to_ms(payload):

    log.info("Mapping sale payload")

    amount = payload.get("amount", 0)
    product_id = payload.get("product_id")

    ms_product_id = None

    if product_id:
        store = MappingStore()

        mapping = store.get_by_evotor_id(
            tenant_id=payload.get("tenant_id"),
            entity_type="product",
            evotor_id=product_id
        )

        if mapping:
            ms_product_id = mapping["ms_id"]
            log.info(f"Mapping found product {product_id} -> {ms_product_id}")
        else:
            log.warning(f"TODO: no mapping for product {product_id}")

    ms_payload = {
        "name": f"Sale {payload.get('event_id')}",
        "description": "Created from Evotor webhook",
        "sum": amount * 100
    }

    if ms_product_id:
        ms_payload["positions"] = [
            {
                "quantity": 1,
                "assortment": {
                    "meta": {
                        "href": f"https://api.moysklad.ru/api/remap/1.2/entity/product/{ms_product_id}",
                        "type": "product"
                    }
                }
            }
        ]

    return ms_payload