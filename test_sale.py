import json
import logging
from mapping_store import MappingStore
from sale_mapper import map_sale_to_ms

# Логирование в консоль
logging.basicConfig(level=logging.INFO)

# --------------------------------------------------
# Dummy client вместо реального MoySklad
# --------------------------------------------------
class DummyClient:
    def __init__(self, tenant_id):
        self.tenant_id = tenant_id
    def create_document(self, payload):
        print("MS Document would be created:", payload)
        return {"id": "dummy_result"}

# --------------------------------------------------
# Обработчик sale с поддержкой mapping
# --------------------------------------------------
def handle_sale(event_row, store):
    payload = json.loads(event_row["payload_json"])
    product_id = payload.get("product_id")

    # ищем mapping
    ms_id = store.get_by_evotor_id(event_row["tenant_id"], "product", product_id)
    if not ms_id:
        print(f"TODO: no mapping for product {product_id}")

    # формируем MS payload
    ms_payload = map_sale_to_ms(payload)

    # используем dummy client
    client = DummyClient(event_row["tenant_id"])
    result = client.create_document(ms_payload)

    result_ref = result.get("id", "dummy:created")
    print(f"Sale processed, result_ref={result_ref}")
    return result_ref

# --------------------------------------------------
# SELF-TEST
# --------------------------------------------------
if __name__ == "__main__":
    store = MappingStore(db_path=":memory:")

    # Добавим тестовый mapping
    store.upsert_mapping("tenant1", "product", "evo_123", "ms_456")

    # Событие с существующим mapping
    event_with_mapping = {
        "id": "evt_1",
        "event_key": "sale_1",
        "tenant_id": "tenant1",
        "payload_json": json.dumps({
            "event_id": "sale_1",
            "product_id": "evo_123",
            "amount": 100
        })
    }

    # Событие без mapping
    event_without_mapping = {
        "id": "evt_2",
        "event_key": "sale_2",
        "tenant_id": "tenant1",
        "payload_json": json.dumps({
            "event_id": "sale_2",
            "product_id": "evo_999",
            "amount": 50
        })
    }

    print("--- Test with existing mapping ---")
    handle_sale(event_with_mapping, store)

    print("--- Test without mapping ---")
    handle_sale(event_without_mapping, store)

    print(">>> Self-test complete <<<")