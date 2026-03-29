import sqlite3
import time

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()
now = int(time.time())

event_id = "evt-retry-alert-test-1"

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
        "retry-alert-test-1",
        '{"test": true}',
        "RETRY",
        2,
        now + 120,
        "TEST_RETRY",
        "Retry event for telegram alert test",
        now - 5,
        now,
    )
)

conn.commit()
conn.close()

print("RETRY alert test event inserted")