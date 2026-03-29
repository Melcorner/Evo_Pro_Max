import sqlite3
import time

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()
now = int(time.time())

event_id = "evt-failed-test-1"

cur.execute(
    """
    INSERT INTO event_store (
        id, tenant_id, event_type, event_key, payload_json,
        status, retries, next_retry_at, last_error_code, last_error_message,
        created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (
        event_id,
        "test-tenant",
        "sale",
        "failed-test-1",
        '{"test": true}',
        "FAILED",
        5,
        None,
        "MAPPING_NOT_FOUND",
        "Product mapping not found",
        now - 20,
        now,
    )
)

cur.execute(
    """
    INSERT INTO errors (event_id, tenant_id, error_code, message, created_at)
    VALUES (?, ?, ?, ?, ?)
    """,
    (
        event_id,
        "test-tenant",
        "MAPPING_NOT_FOUND",
        "Product mapping not found",
        now,
    )
)

conn.commit()
conn.close()

print("FAILED test event inserted with error log")