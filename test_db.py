import time
import uuid

from db import get_connection


def main():
    conn = get_connection()
    cursor = conn.cursor()

    tenant_id = str(uuid.uuid4())

    cursor.execute("""
        INSERT INTO tenants (id, name, evotor_api_key, moysklad_token, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        tenant_id,
        "Test Tenant",
        "evotor_key",
        "ms_token",
        int(time.time())
    ))

    conn.commit()

    cursor.execute("SELECT * FROM tenants")
    rows = cursor.fetchall()

    for row in rows:
        print(dict(row))

    conn.close()


if __name__ == "__main__":
    main()