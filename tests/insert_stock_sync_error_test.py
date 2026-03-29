import sqlite3
import time

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()
now = int(time.time())

tenant_id = "test-tenant-stock"

cur.execute(
    """
    INSERT OR REPLACE INTO stock_sync_status (
        tenant_id,
        status,
        last_sync_at,
        last_error,
        updated_at
    ) VALUES (?, ?, ?, ?, ?)
    """,
    (
        tenant_id,
        "error",
        now,
        "Stock sync test error",
        now,
    )
)

conn.commit()
conn.close()

print("stock_sync_status test error inserted")