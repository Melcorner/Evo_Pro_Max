import sqlite3
import time

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()
now = int(time.time())

cur.execute(
    """
    UPDATE stock_sync_status
    SET status = ?, last_sync_at = ?, last_error = NULL, updated_at = ?
    WHERE tenant_id = ?
    """,
    (
        "ok",
        now,
        now,
        "test-tenant-stock",
    )
)

conn.commit()
conn.close()

print("stock_sync_status test error cleared")