import time

from app.db import get_connection

TENANT_ID = "test-tenant"
EVENT_ID = "evt-done-test-1"
EVENT_KEY = "done-test-1"


def ensure_test_tenant(conn, tenant_id: str) -> None:
    now = int(time.time())
    conn.execute(
        """
        INSERT OR IGNORE INTO tenants (
            id, name, evotor_api_key, moysklad_token, created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (tenant_id, "Dashboard Test Tenant", "test-key", "test-token", now),
    )


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()
        now = int(time.time())

        ensure_test_tenant(conn, TENANT_ID)

        cur.execute("DELETE FROM event_store WHERE id = ?", (EVENT_ID,))

        cur.execute(
            """
            INSERT INTO event_store (
                id, tenant_id, event_type, event_key, payload_json,
                status, retries, next_retry_at, last_error_code, last_error_message,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                EVENT_ID,
                TENANT_ID,
                "sale",
                EVENT_KEY,
                '{"test": true}',
                "DONE",
                0,
                None,
                None,
                None,
                now - 42,
                now,
            ),
        )

        conn.commit()
        print("DONE test event inserted")
    finally:
        conn.close()


if __name__ == "__main__":
    main()