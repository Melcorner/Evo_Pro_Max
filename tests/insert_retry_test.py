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
        "evt-retry-test-1",
        "test-tenant",
        "sale",
        "retry-test-1",
        '{"test": true}',
        "RETRY",
        2,
        now + 120,
        "HTTP_503",
        "Temporary upstream error",
        now - 15,
        now,
    )
)

conn.commit()
conn.close()

print("RETRY test event inserted")