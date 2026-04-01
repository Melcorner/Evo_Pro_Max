import time

from app.db import get_connection

TENANT_ID = "test-tenant-stock"


def ensure_test_tenant(conn, tenant_id: str) -> None:
    now = int(time.time())
    conn.execute(
        """
        INSERT OR IGNORE INTO tenants (
            id, name, evotor_api_key, moysklad_token, created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (tenant_id, "Stock Sync Test Tenant", "test-key", "test-token", now),
    )


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()
        now = int(time.time())

        ensure_test_tenant(conn, TENANT_ID)

        cur.execute(
            """
            INSERT OR REPLACE INTO stock_sync_status (
                tenant_id,
                status,
                started_at,
                updated_at,
                last_sync_at,
                last_error,
                synced_items_count,
                total_items_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                TENANT_ID,
                "error",
                now - 30,
                now,
                now,
                "Stock sync test error",
                0,
                10,
            ),
        )

        conn.commit()
        print("stock_sync_status test error inserted")
    finally:
        conn.close()


if __name__ == "__main__":
    main()