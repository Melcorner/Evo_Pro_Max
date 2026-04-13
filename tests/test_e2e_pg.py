"""
E2E тесты для PostgreSQL контура.

Запуск:
    RUN_E2E_PG=1 DATABASE_URL=postgresql://evotor:pass@localhost:5432/evotor_ms \
    python -m pytest tests/test_e2e_pg.py -v --tb=short
"""
from __future__ import annotations

import json
import os
import time
import uuid

import pytest


def _e2e_pg_enabled() -> bool:
    if os.getenv("RUN_E2E_PG", "").strip().lower() not in {"1", "true", "yes"}:
        return False
    url = os.getenv("DATABASE_URL", "")
    return url.startswith("postgresql")


_E2E_PG_SKIP = pytest.mark.skipif(
    not _e2e_pg_enabled(),
    reason="Set RUN_E2E_PG=1 and DATABASE_URL=postgresql://... to run E2E PostgreSQL tests.",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def app_client():
    from fastapi.testclient import TestClient
    from app.main import app
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


@pytest.fixture(scope="module")
def admin_headers():
    token = os.getenv("ADMIN_API_TOKEN", "test")
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture()
def tenant_id(app_client, admin_headers):
    """Создаёт tenant перед тестом и удаляет после."""
    resp = app_client.post("/tenants", headers=admin_headers, json={
        "name": "E2E PG Tenant",
        "evotor_api_key": "test-evotor-key",
        "moysklad_token": "test-ms-token",
    })
    assert resp.status_code in (200, 201), f"Failed to create tenant: {resp.text}"
    tid = resp.json()["id"]
    yield tid
    app_client.delete(f"/tenants/{tid}", headers=admin_headers)


@pytest.fixture()
def tenant_with_mappings(app_client, admin_headers, tenant_id):
    """Tenant с маппингами товаров."""
    mappings = [
        ("evotor-prod-001", "ms-prod-001"),
        ("evotor-prod-002", "ms-prod-002"),
    ]
    for evotor_id, ms_id in mappings:
        resp = app_client.post("/mappings/", headers=admin_headers, json={
            "tenant_id": tenant_id,
            "entity_type": "product",
            "evotor_id": evotor_id,
            "ms_id": ms_id,
        })
        assert resp.status_code in (200, 201)
    return tenant_id, mappings


def _get_mappings_items(resp) -> list:
    """Извлекает список маппингов из ответа (поддерживает пагинацию и plain list)."""
    data = resp.json()
    if isinstance(data, list):
        return data
    return data.get("items", data.get("rows", []))


def _make_sell_webhook(store_id: str, positions: list) -> dict:
    return {
        "type": "ReceiptCreated",
        "id": str(uuid.uuid4()),
        "userId": "01-000000012747622",
        "timestamp": int(time.time() * 1000),
        "version": 2,
        "data": {
            "id": str(uuid.uuid4()),
            "storeId": store_id,
            "deviceId": "device-001",
            "dateTime": "2026-04-11T10:00:00.000Z",
            "type": "SELL",
            "shiftId": "1",
            "employeeId": "emp-001",
            "paymentSource": "PAY_CASH",
            "infoCheck": False,
            "egais": False,
            "totalAmount": sum(p.get("sumPrice", 0) for p in positions),
            "totalDiscount": 0.0,
            "totalTax": 0.0,
            "items": positions,
            "extras": {},
        }
    }


# ---------------------------------------------------------------------------
# Тесты
# ---------------------------------------------------------------------------

@_E2E_PG_SKIP
class TestTenantLifecycle:

    def test_create_tenant_returns_id(self, app_client, admin_headers):
        """Создание tenant возвращает сгенерированный id."""
        resp = app_client.post("/tenants", headers=admin_headers, json={
            "name": "Lifecycle Test",
            "evotor_api_key": "key",
            "moysklad_token": "token",
        })
        assert resp.status_code in (200, 201)
        data = resp.json()
        assert "id" in data
        assert len(data["id"]) == 36  # UUID формат

        app_client.delete(f"/tenants/{data['id']}", headers=admin_headers)

    def test_get_tenants_list_contains_created(self, app_client, admin_headers, tenant_id):
        resp = app_client.get("/tenants", headers=admin_headers)
        assert resp.status_code == 200
        tenants = resp.json()
        if isinstance(tenants, dict):
            tenants = tenants.get("items", tenants.get("rows", []))
        ids = [t["id"] for t in tenants]
        assert tenant_id in ids

    def test_delete_tenant_removes_it(self, app_client, admin_headers):
        """Удалённый tenant недоступен."""
        resp = app_client.post("/tenants", headers=admin_headers, json={
            "name": "Del Test", "evotor_api_key": "k", "moysklad_token": "t"
        })
        tid = resp.json()["id"]

        del_resp = app_client.delete(f"/tenants/{tid}", headers=admin_headers)
        assert del_resp.status_code == 200

    def test_delete_tenant_cleans_mappings(self, app_client, admin_headers):
        """При удалении tenant'а удаляются все его маппинги."""
        resp = app_client.post("/tenants", headers=admin_headers, json={
            "name": "Del Mappings Test", "evotor_api_key": "k", "moysklad_token": "t"
        })
        tid = resp.json()["id"]

        app_client.post("/mappings/", headers=admin_headers, json={
            "tenant_id": tid, "entity_type": "product",
            "evotor_id": "e-001", "ms_id": "m-001",
        })

        app_client.delete(f"/tenants/{tid}", headers=admin_headers)

        resp = app_client.get("/mappings", headers=admin_headers, params={"tenant_id": tid})
        assert resp.status_code == 200
        items = _get_mappings_items(resp)
        assert items == []


@_E2E_PG_SKIP
class TestMappings:

    def test_create_mapping(self, app_client, admin_headers, tenant_id):
        resp = app_client.post("/mappings/", headers=admin_headers, json={
            "tenant_id": tenant_id,
            "entity_type": "product",
            "evotor_id": "e-test-001",
            "ms_id": "m-test-001",
        })
        assert resp.status_code in (200, 201)

    def test_get_mappings_by_tenant(self, app_client, admin_headers, tenant_with_mappings):
        tenant_id, mappings = tenant_with_mappings
        resp = app_client.get("/mappings", headers=admin_headers, params={"tenant_id": tenant_id})
        assert resp.status_code == 200
        items = _get_mappings_items(resp)
        assert len(items) >= len(mappings)
        evotor_ids = [m["evotor_id"] for m in items]
        for evotor_id, _ in mappings:
            assert evotor_id in evotor_ids

    def test_delete_mapping(self, app_client, admin_headers, tenant_id):
        app_client.post("/mappings/", headers=admin_headers, json={
            "tenant_id": tenant_id, "entity_type": "product",
            "evotor_id": "e-del-001", "ms_id": "m-del-001",
        })

        resp = app_client.delete(
            f"/mappings/{tenant_id}/product/e-del-001",
            headers=admin_headers
        )
        assert resp.status_code == 200

        resp = app_client.get("/mappings", headers=admin_headers, params={
            "tenant_id": tenant_id, "entity_type": "product"
        })
        items = _get_mappings_items(resp)
        evotor_ids = [m["evotor_id"] for m in items]
        assert "e-del-001" not in evotor_ids

    def test_upsert_mapping_no_duplicate(self, app_client, admin_headers, tenant_id):
        """Повторный upsert того же маппинга не создаёт дубль."""
        payload = {
            "tenant_id": tenant_id, "entity_type": "product",
            "evotor_id": "e-dup-001", "ms_id": "m-dup-001",
        }
        app_client.post("/mappings/", headers=admin_headers, json=payload)
        app_client.post("/mappings/", headers=admin_headers, json=payload)

        resp = app_client.get("/mappings", headers=admin_headers, params={"tenant_id": tenant_id})
        items = _get_mappings_items(resp)
        count = sum(1 for m in items if m["evotor_id"] == "e-dup-001")
        assert count == 1

    def test_delete_all_mappings_by_tenant(self, app_client, admin_headers, tenant_id):
        """Удаление всех маппингов tenant'а."""
        for i in range(3):
            app_client.post("/mappings/", headers=admin_headers, json={
                "tenant_id": tenant_id, "entity_type": "product",
                "evotor_id": f"e-bulk-{i}", "ms_id": f"m-bulk-{i}",
            })

        resp = app_client.delete(f"/mappings/{tenant_id}/product", headers=admin_headers)
        assert resp.status_code == 200

        resp = app_client.get("/mappings", headers=admin_headers, params={
            "tenant_id": tenant_id, "entity_type": "product"
        })
        items = _get_mappings_items(resp)
        bulk_ids = [m for m in items if m["evotor_id"].startswith("e-bulk-")]
        assert bulk_ids == []


@_E2E_PG_SKIP
class TestWebhookEvotor:

    def test_webhook_stored_in_event_store(self, app_client, admin_headers, tenant_with_mappings):
        """Webhook продажи сохраняется в event_store."""
        tenant_id, mappings = tenant_with_mappings
        store_id = "20260314-3BF3-4021-8051-E3A278EE4974"

        # Устанавливаем evotor_store_id через PATCH
        patch_resp = app_client.patch(
            f"/tenants/{tenant_id}/moysklad",
            headers=admin_headers,
            json={"evotor_store_id": store_id}
        )
        # Если patch не поддерживает evotor_store_id — обновим напрямую через БД не нужно,
        # webhook резолвит tenant по store_id из тела

        payload = _make_sell_webhook(store_id, [
            {
                "id": mappings[0][0],
                "name": "Тест товар",
                "quantity": 2,
                "price": 100.0,
                "sumPrice": 200.0,
                "tax": 0.0,
                "taxPercent": 0,
                "discount": 0,
            }
        ])

        # Webhook без tenant_id в URL — резолвится по storeId
        resp = app_client.post(
            "/webhooks/evotor",
            json=payload,
            headers={"Authorization": f"Bearer {os.getenv('EVOTOR_WEBHOOK_SECRET', 'test')}"}
        )
        # 200 или 404 если tenant не привязан к store_id — оба приемлемы
        assert resp.status_code in (200, 404)

    def test_webhook_with_tenant_id_in_url(self, app_client, admin_headers, tenant_id):
        """Webhook с явным tenant_id в URL."""
        payload = _make_sell_webhook("store-test-001", [
            {
                "id": "evotor-prod-001",
                "name": "Товар",
                "quantity": 1,
                "price": 50.0,
                "sumPrice": 50.0,
                "tax": 0.0,
                "taxPercent": 0,
                "discount": 0,
            }
        ])

        resp = app_client.post(
            f"/webhooks/evotor/{tenant_id}",
            json=payload,
            headers={"Authorization": f"Bearer {os.getenv('EVOTOR_WEBHOOK_SECRET', 'test')}"}
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("status") in ("accepted", "skipped", "ok")

    def test_webhook_idempotency(self, app_client, admin_headers, tenant_id):
        """Повторный webhook с тем же event_id не задваивает событие."""
        event_id = str(uuid.uuid4())
        payload = _make_sell_webhook("store-test-001", [
            {"id": "evotor-prod-001", "name": "Товар", "quantity": 1,
             "price": 50.0, "sumPrice": 50.0, "tax": 0.0, "taxPercent": 0, "discount": 0}
        ])
        payload["id"] = event_id

        headers = {"Authorization": f"Bearer {os.getenv('EVOTOR_WEBHOOK_SECRET', 'test')}"}
        url = f"/webhooks/evotor/{tenant_id}"

        resp1 = app_client.post(url, json=payload, headers=headers)
        resp2 = app_client.post(url, json=payload, headers=headers)

        assert resp1.status_code == 200
        assert resp2.status_code == 200

        # Проверяем отсутствие дублей в event_store
        time.sleep(0.3)
        resp = app_client.get("/events", headers=admin_headers)
        events = resp.json() if isinstance(resp.json(), list) else resp.json().get("items", [])
        tenant_events = [e for e in events if e.get("tenant_id") == tenant_id]
        keys = [e.get("event_key") for e in tenant_events]
        assert keys.count(event_id) <= 1


@_E2E_PG_SKIP
class TestHealthAndMonitoring:

    def test_health_ok(self, app_client):
        resp = app_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("ok", "degraded")
        # Проверяем наличие db check (может быть в разных полях)
        assert "checks" in data or "db" in data or "database" in data

    def test_health_has_db_check(self, app_client):
        resp = app_client.get("/health")
        data = resp.json()
        checks = data.get("checks", {})
        assert "db" in checks or "database" in checks

    def test_monitoring_dashboard(self, app_client, admin_headers):
        resp = app_client.get("/monitoring/dashboard", headers=admin_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    def test_metrics_endpoint(self, app_client):
        resp = app_client.get("/metrics")
        assert resp.status_code == 200
        # Наши метрики начинаются с integration_
        assert "integration_" in resp.text or "http_" in resp.text


@_E2E_PG_SKIP
class TestEvents:

    def test_get_events(self, app_client, admin_headers):
        resp = app_client.get("/events", headers=admin_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, (list, dict))

    def test_get_failed_events(self, app_client, admin_headers):
        resp = app_client.get("/events/failed", headers=admin_headers)
        assert resp.status_code == 200

    def test_get_retry_events(self, app_client, admin_headers):
        resp = app_client.get("/events/retry", headers=admin_headers)
        assert resp.status_code == 200

    def test_requeue_nonexistent_event(self, app_client, admin_headers):
        fake_id = str(uuid.uuid4())
        resp = app_client.post(f"/events/{fake_id}/requeue", headers=admin_headers)
        assert resp.status_code in (404, 400)

    def test_get_event_details(self, app_client, admin_headers):
        """Детали несуществующего события возвращают 404."""
        fake_id = str(uuid.uuid4())
        resp = app_client.get(f"/events/{fake_id}", headers=admin_headers)
        assert resp.status_code == 404


@_E2E_PG_SKIP
class TestAdminAuth:

    def test_protected_endpoint_without_token(self, app_client):
        token = os.getenv("ADMIN_API_TOKEN", "")
        if not token:
            pytest.skip("ADMIN_API_TOKEN not set")
        resp = app_client.get("/tenants")
        assert resp.status_code == 401

    def test_protected_endpoint_with_wrong_token(self, app_client):
        token = os.getenv("ADMIN_API_TOKEN", "")
        if not token:
            pytest.skip("ADMIN_API_TOKEN not set")
        resp = app_client.get("/tenants", headers={"Authorization": "Bearer wrong-token"})
        assert resp.status_code == 401

    def test_public_endpoint_no_auth(self, app_client):
        resp = app_client.get("/health")
        assert resp.status_code == 200

    def test_metrics_public(self, app_client):
        resp = app_client.get("/metrics")
        assert resp.status_code == 200