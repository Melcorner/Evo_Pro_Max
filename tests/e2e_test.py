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
    for evotor_id, ms_id in [("p1", "ms-product-001"), ("p2", "ms-product-002")]:
        resp = requests.post(f"{API}/mappings/", json={
            "tenant_id": tenant_id,
            "entity_type": "product",
            "evotor_id": evotor_id,
            "ms_id": ms_id,
        })
        resp.raise_for_status()


def send_webhook(tenant_id, event_key):
    resp = requests.post(f"{API}/webhooks/evotor/{tenant_id}", json={
        "type": "sale",
        "event_id": event_key,
        "positions": [
            {"product_id": "p1", "quantity": 2, "price": 500},
            {"product_id": "p2", "quantity": 1, "price": 300}
        ]
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
            raise SystemExit(f"❌ Event went to FAILED — check worker logs")

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
    event_key = "e2e-" + str(uuid.uuid4())

    print("Tenant:", tenant_id)
    setup_mappings(tenant_id)
    print("Mappings registered: p1, p2")
    print("Sending webhook:", event_key)

    send_webhook(tenant_id, event_key)

    if not wait_done(tenant_id, event_key):
        raise SystemExit("❌ Timeout: event not DONE")

    if not check_processed(tenant_id, event_key):
        raise SystemExit("❌ Not found in processed_events")

    print("✅ E2E OK: DONE + processed_events")


if __name__ == "__main__":
    main()