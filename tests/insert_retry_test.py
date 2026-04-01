import time
import uuid

from app.db import get_connection

TENANT_ID = "test-tenant"
EVENT_ID = "evt-retry-test-1"
EVENT_KEY = "retry-test-1"
ERROR_ID = "err-retry-test-1"


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

        cur.execute("DELETE FROM errors WHERE id = ?", (ERROR_ID,))
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
                "RETRY",
                2,
                now + 120,
                "HTTP_503",
                "Temporary upstream error",
                now - 15,
                now,
            ),
        )

        cur.execute(
            """
            INSERT INTO errors (
                id, event_id, tenant_id, error_code, message,
                payload_snapshot, response_body, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ERROR_ID,
                EVENT_ID,
                TENANT_ID,
                "HTTP_503",
                "Temporary upstream error",
                '{"test": true}',
                None,
                now,
            ),
        )

        conn.commit()
        print("RETRY test event inserted with error log")
    finally:
        conn.close()


if __name__ == "__main__":
    main()