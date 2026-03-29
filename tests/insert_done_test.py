import sqlite3
import time

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()
now = int(time.time())

cur.execute(
    """
    INSERT INTO event_store (
        id, tenant_id, event_type, event_key, payload_json,
        status, retries, next_retry_at, last_error_code, last_error_message,
        created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (
        "evt-done-test-1",
        "test-tenant",
        "sale",
        "done-test-1",
        '{"test": true}',
        "DONE",
        0,
        None,
        None,
        None,
        now - 42,
        now,
    )
)

conn.commit()
conn.close()

print("DONE test event inserted")