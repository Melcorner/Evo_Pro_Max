import time
import uuid
import requests
import sqlite3

DB_PATH = "data/app.db"
API = "http://127.0.0.1:8000"


def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tenant():
    resp = requests.post(f"{API}/tenants", json={
        "name": "E2E Tenant",
        "evotor_api_key": "k",
        "moysklad_token": "t"
    })
    resp.raise_for_status()
    return resp.json()["id"]


def setup_mappings(tenant_id):
    for evotor_id, ms_id in [
        ("bbb5b5a8-6e3d-45ff-b16d-18b95926cbc9", "ms-product-001"),
        ("ccc5b5a8-6e3d-45ff-b16d-18b95926cbc9", "ms-product-002"),
    ]:
        resp = requests.post(f"{API}/mappings/", json={
            "tenant_id": tenant_id,
            "entity_type": "product",
            "evotor_id": evotor_id,
            "ms_id": ms_id,
        })
        resp.raise_for_status()


def send_webhook(tenant_id, event_id):
    """Отправляет webhook в формате Эвотор SELL."""
    resp = requests.post(f"{API}/webhooks/evotor/{tenant_id}", json={
        "type": "SELL",
        "id": event_id,
        "store_id": "20260314-3BF3-4021-8051-E3A278EE4974",
        "device_id": "20260314-65DA-40F1-80EE-5109AB6E49F6",
        "body": {
            "positions": [
                {
                    "product_id": "bbb5b5a8-6e3d-45ff-b16d-18b95926cbc9",
                    "product_name": "GP Alkaline AAx4",
                    "quantity": 2,
                    "price": 1.0,
                    "sum": 2.0
                },
                {
                    "product_id": "ccc5b5a8-6e3d-45ff-b16d-18b95926cbc9",
                    "product_name": "Test Product",
                    "quantity": 1,
                    "price": 3.0,
                    "sum": 3.0
                }
            ],
            "sum": 5.0
        }
    })
    resp.raise_for_status()
    return resp.json()


def wait_done(tenant_id, event_key, timeout_sec=15):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        conn = db_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT status FROM event_store
            WHERE tenant_id=? AND event_key=?
            ORDER BY created_at DESC
            LIMIT 1
        """, (tenant_id, event_key))
        row = cur.fetchone()
        conn.close()

        if row and row["status"] == "DONE":
            return True

        if row and row["status"] == "FAILED":
            raise SystemExit("❌ Event went to FAILED — check worker logs")

        time.sleep(1)
    return False


def check_processed(tenant_id, event_key):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM processed_events
        WHERE tenant_id=? AND event_key=?
    """, (tenant_id, event_key))
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def main():
    tenant_id = ensure_tenant()
    event_id = "e2e-" + str(uuid.uuid4())

    print("Tenant:", tenant_id)
    setup_mappings(tenant_id)
    print("Mappings registered: bbb5b5a8 -> ms-product-001, ccc5b5a8 -> ms-product-002")
    print("Sending webhook:", event_id)

    send_webhook(tenant_id, event_id)

    if not wait_done(tenant_id, event_id):
        raise SystemExit("❌ Timeout: event not DONE")

    if not check_processed(tenant_id, event_id):
        raise SystemExit("❌ Not found in processed_events")

    print("✅ E2E OK: DONE + processed_events")


if __name__ == "__main__":
    main()