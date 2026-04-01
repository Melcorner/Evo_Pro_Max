from app.db import get_connection

EVENT_IDS = [
    "evt-done-test-1",
    "evt-retry-test-1",
    "evt-failed-test-1",
    "evt-retry-alert-test-1",
    "evt-failed-alert-test-1",
]

ERROR_IDS = [
    "err-retry-test-1",
    "err-failed-test-1",
]

TENANT_IDS = [
    "test-tenant",
    "test-tenant-stock",
]


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Удаляем ошибки по явным id
        for error_id in ERROR_IDS:
            cur.execute("DELETE FROM errors WHERE id = ?", (error_id,))

        # На случай старых версий тестов: чистим ошибки и по event_id / tenant_id
        for event_id in EVENT_IDS:
            cur.execute("DELETE FROM errors WHERE event_id = ?", (event_id,))
        for tenant_id in TENANT_IDS:
            cur.execute("DELETE FROM errors WHERE tenant_id = ?", (tenant_id,))

        # Удаляем тестовые события
        for event_id in EVENT_IDS:
            cur.execute("DELETE FROM event_store WHERE id = ?", (event_id,))

        # Удаляем тестовую ошибку синхронизации остатков
        cur.execute(
            "DELETE FROM stock_sync_status WHERE tenant_id = ?",
            ("test-tenant-stock",),
        )

        # Если хочешь полностью убрать тестовых tenant'ов,
        # раскомментируй блок ниже.
        #
        # Важно: он удаляет tenant'ов только после очистки зависимых записей.
        #
        # for tenant_id in TENANT_IDS:
        #     cur.execute("DELETE FROM tenants WHERE id = ?", (tenant_id,))

        conn.commit()
        print("Test data cleaned up")
    finally:
        conn.close()


if __name__ == "__main__":
    main()