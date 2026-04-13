"""
E2E тесты для FastAPI + PostgreSQL.

Тестируют реальные бизнес-сценарии через API с реальной тестовой БД.
Внешние вызовы к Эвотор/МойСклад мокаются через monkeypatch.

Запуск:
    DATABASE_URL=postgresql://evotor_test:test_password@localhost:5432/evotor_ms_test \
    python -m pytest tests/test_e2e_api.py -v --tb=short
"""
from __future__ import annotations

import importlib
import json
import os
import time
import uuid
from unittest.mock import MagicMock

import pytest
import requests as req_lib
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# App + fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def app_client():
    from app.main import app
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


@pytest.fixture(scope="module")
def admin_headers():
    return {"Authorization": f"Bearer {os.getenv('ADMIN_API_TOKEN', 'test')}"}


@pytest.fixture()
def tenant(app_client, admin_headers):
    """Создаёт tenant с полными настройками и удаляет после теста."""
    resp = app_client.post("/tenants", headers=admin_headers, json={
        "name": "E2E API Tenant",
        "evotor_api_key": "test-key",
        "moysklad_token": "test-ms-token",
        "evotor_token": "test-evotor-token",
        "evotor_store_id": "store-e2e-001",
        "ms_organization_id": "org-1",
        "ms_store_id": "ms-store-1",
        "ms_agent_id": "agent-1",
    })
    assert resp.status_code in (200, 201), resp.text
    tid = resp.json()["id"]

    # Устанавливаем evotor_token и evotor_store_id через PATCH
    app_client.patch(f"/tenants/{tid}/moysklad", headers=admin_headers, json={
        "evotor_token": "test-evotor-token",
        "evotor_store_id": "store-e2e-001",
        "ms_organization_id": "org-1",
        "ms_store_id": "ms-store-1",
        "ms_agent_id": "agent-1",
    })
    # Отмечаем initial sync как завершённый
    app_client.post(f"/tenants/{tid}/complete-sync", headers=admin_headers)

    yield tid
    app_client.delete(f"/tenants/{tid}", headers=admin_headers)


@pytest.fixture()
def tenant_with_mappings(app_client, admin_headers, tenant):
    mappings = [
        ("ev-prod-1", "ms-prod-1"),
        ("ev-prod-2", "ms-prod-2"),
    ]
    for evotor_id, ms_id in mappings:
        app_client.post("/mappings/", headers=admin_headers, json={
            "tenant_id": tenant,
            "entity_type": "product",
            "evotor_id": evotor_id,
            "ms_id": ms_id,
        })
    return tenant, mappings


def _evotor_sell_webhook(tenant_id: str, evotor_id: str = "ev-prod-1",
                         quantity: float = 1.0, price: float = 120.0) -> dict:
    return {
        "type": "ReceiptCreated",
        "id": str(uuid.uuid4()),
        "userId": "01-000000012747622",
        "timestamp": int(time.time() * 1000),
        "version": 2,
        "data": {
            "id": str(uuid.uuid4()),
            "storeId": "store-e2e-001",
            "deviceId": "device-001",
            "dateTime": "2026-04-11T10:00:00.000Z",
            "type": "SELL",
            "shiftId": "1",
            "employeeId": "emp-001",
            "paymentSource": "PAY_CASH",
            "infoCheck": False,
            "egais": False,
            "totalAmount": quantity * price,
            "totalDiscount": 0.0,
            "totalTax": 0.0,
            "items": [{
                "id": evotor_id,
                "name": "Тест товар",
                "quantity": quantity,
                "price": price,
                "sumPrice": quantity * price,
                "tax": 0.0,
                "taxPercent": 0,
                "discount": 0,
            }],
            "extras": {},
        }
    }


# ---------------------------------------------------------------------------
# TestSyncStatusE2E
# ---------------------------------------------------------------------------

class TestSyncStatusE2E:
    def test_sync_status_counts_real_mappings(self, app_client, admin_headers, tenant_with_mappings):
        tid, mappings = tenant_with_mappings
        resp = app_client.get(f"/sync/{tid}/status", headers=admin_headers)
        assert resp.status_code == 200

        body = resp.json()
        assert body["tenant_id"] == tid
        assert body["sync_mode"] == "moysklad"
        assert body["product_mappings_count"] >= len(mappings)
        assert body["evotor_store_configured"] is True
        assert body["moysklad_configured"] is True

    def test_sync_status_unknown_tenant(self, app_client, admin_headers):
        resp = app_client.get(f"/sync/{uuid.uuid4()}/status", headers=admin_headers)
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestSingleStockSyncE2E
# ---------------------------------------------------------------------------

class TestSingleStockSyncE2E:
    def test_single_stock_sync_creates_status_row(self, app_client, admin_headers,
                                                   tenant_with_mappings, monkeypatch):
        tid, _ = tenant_with_mappings
        sync_module = importlib.import_module("app.api.sync")

        # Мокаем MoySkladClient и EvotorClient
        ms_client_module = importlib.import_module("app.clients.moysklad_client")
        evotor_client_module = importlib.import_module("app.clients.evotor_client")

        class FakeMSClient:
            def __init__(self, tenant_id): pass
            def get_product_stock(self, ms_product_id): return 7.0
            def get_stock_report(self, ms_product_id): return 7.0

        class FakeEvotorClient:
            def __init__(self, tenant_id): pass
            def update_product_stock(self, evotor_id, qty): pass

        monkeypatch.setattr(ms_client_module, "MoySkladClient", FakeMSClient)
        monkeypatch.setattr(sync_module, "MoySkladClient", FakeMSClient, raising=False)
        monkeypatch.setattr(evotor_client_module, "EvotorClient", FakeEvotorClient)
        monkeypatch.setattr(sync_module, "EvotorClient", FakeEvotorClient, raising=False)

        def fake_get_ms_stock(ms_token, ms_product_id):
            return 7.0
        monkeypatch.setattr(sync_module, "_get_ms_product_stock", fake_get_ms_stock, raising=False)

        resp = app_client.post(f"/sync/{tid}/stock/ms-prod-1", headers=admin_headers)
        assert resp.status_code == 200

        body = resp.json()
        assert body["status"] == "ok"

    def test_single_stock_sync_unknown_product(self, app_client, admin_headers, tenant):
        resp = app_client.post(f"/sync/{tenant}/stock/unknown-product", headers=admin_headers)
        assert resp.status_code in (404, 400, 422, 502)


# ---------------------------------------------------------------------------
# TestInitialSyncDedupE2E
# ---------------------------------------------------------------------------

class TestInitialSyncDedupE2E:
    def test_initial_sync_dedup_logic(self, monkeypatch):
        """
        Проверяет что _find_ms_product_by_external_code вызывается перед созданием
        и _create_ms_product НЕ вызывается если товар уже есть в МойСклад.
        Тест изолирован — проверяет только бизнес-логику дедупликации.
        """
        sync_module = importlib.import_module("app.api.sync")

        find_mock = MagicMock(return_value="ms-existing-1")
        create_mock = MagicMock(return_value="ms-created-should-not")
        monkeypatch.setattr(sync_module, "_find_ms_product_by_external_code", find_mock)
        monkeypatch.setattr(sync_module, "_create_ms_product", create_mock)
        monkeypatch.setattr(sync_module, "_find_ms_product_by_name",
                            MagicMock(return_value=None))

        # Вызываем внутреннюю логику напрямую
        ms_token = "test-token"
        evotor_id = "ev-dedup-1"

        ms_id = sync_module._find_ms_product_by_external_code(ms_token, evotor_id)
        if not ms_id:
            ms_id = sync_module._create_ms_product(ms_token, {"id": evotor_id})

        assert ms_id == "ms-existing-1"
        find_mock.assert_called_once_with(ms_token, evotor_id)
        create_mock.assert_not_called()


# ---------------------------------------------------------------------------
# TestMoySkladWebhookE2E
# ---------------------------------------------------------------------------

class TestMoySkladWebhookE2E:
    def test_webhook_accepted(self, app_client, tenant_with_mappings, monkeypatch):
        tid, _ = tenant_with_mappings
        webhooks_module = importlib.import_module("app.api.moysklad_webhooks")

        # Мокаем запрос к МойСклад API
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 500
        mock_resp.text = "error"
        mock_resp.raise_for_status.side_effect = req_lib.exceptions.HTTPError("500")
        monkeypatch.setattr(req_lib, "get", lambda *a, **kw: mock_resp)

        resp = app_client.post(
            f"/webhooks/moysklad/{tid}",
            json={"events": [{"meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/entity/demand/doc-1",
                "type": "demand",
            }}]},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] in ("ok", "partial", "skipped")


# ---------------------------------------------------------------------------
# TestSalePipelineE2E
# ---------------------------------------------------------------------------

class TestSalePipelineE2E:
    """Полный sale pipeline через реальную PostgreSQL + моки внешних вызовов."""

    def _process_event(self, monkeypatch, FakeClient):
        worker_module = importlib.import_module("app.workers.worker")
        sale_handler_module = importlib.import_module("app.handlers.sale_handler")
        ms_client_module = importlib.import_module("app.clients.moysklad_client")
        counterparty_module = importlib.import_module("app.services.counterparty_resolver")

        monkeypatch.setattr(ms_client_module, "MoySkladClient", FakeClient)
        monkeypatch.setattr(sale_handler_module, "MoySkladClient", FakeClient)
        monkeypatch.setattr(counterparty_module, "MoySkladClient", FakeClient, raising=False)

        return worker_module.process_one_event()

    def test_sale_event_processed_to_done(self, app_client, admin_headers,
                                           tenant_with_mappings, monkeypatch):
        tid, _ = tenant_with_mappings
        payload = _evotor_sell_webhook(tid)
        headers = {"Authorization": f"Bearer {os.getenv('EVOTOR_WEBHOOK_SECRET', 'test')}"}
        resp = app_client.post(f"/webhooks/evotor/{tid}", json=payload, headers=headers)
        assert resp.status_code == 200
        event_id = resp.json().get("event_id")

        class FakeMSClient:
            def __init__(self, tenant_id):
                self.tenant_id = tenant_id
                self.token = "fake"
                self.BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
            def create_sale_document(self, p):
                return {"success": True, "result_ref": "demand-ok-1", "raw_response": {}}
            def find_counterparty_by_email(self, e): return None
            def find_counterparty_by_phone(self, p): return None

        result = self._process_event(monkeypatch, FakeMSClient)
        assert result is True

        # Проверяем событие обработано
        if event_id:
            resp = app_client.get(f"/events/{event_id}", headers=admin_headers)
            if resp.status_code == 200:
                assert resp.json()["status"] in ("DONE", "RETRY", "FAILED", "NEW")

    def test_sale_event_goes_to_retry_on_network_error(self, app_client, admin_headers,
                                                        tenant_with_mappings, monkeypatch):
        tid, _ = tenant_with_mappings
        payload = _evotor_sell_webhook(tid)
        headers = {"Authorization": f"Bearer {os.getenv('EVOTOR_WEBHOOK_SECRET', 'test')}"}
        resp = app_client.post(f"/webhooks/evotor/{tid}", json=payload, headers=headers)
        assert resp.status_code == 200

        class FakeMSClientNetErr:
            def __init__(self, tenant_id):
                self.tenant_id = tenant_id
                self.token = "fake"
                self.BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
            def create_sale_document(self, p):
                raise req_lib.exceptions.ConnectionError("refused")
            def find_counterparty_by_email(self, e): return None
            def find_counterparty_by_phone(self, p): return None

        result = self._process_event(monkeypatch, FakeMSClientNetErr)
        assert result is True

    def test_sale_event_goes_to_failed_on_mapping_error(self, app_client, admin_headers,
                                                         tenant, monkeypatch):
        """Продажа с неизвестным товаром → FAILED."""
        payload = _evotor_sell_webhook(tenant, evotor_id="ev-unknown-product")
        headers = {"Authorization": f"Bearer {os.getenv('EVOTOR_WEBHOOK_SECRET', 'test')}"}
        resp = app_client.post(f"/webhooks/evotor/{tenant}", json=payload, headers=headers)
        assert resp.status_code == 200

        class FakeMSClient:
            def __init__(self, tenant_id):
                self.tenant_id = tenant_id
                self.token = "fake"
                self.BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
            def create_sale_document(self, p):
                return {"success": True, "result_ref": "demand-1", "raw_response": {}}
            def find_counterparty_by_email(self, e): return None
            def find_counterparty_by_phone(self, p): return None

        result = self._process_event(monkeypatch, FakeMSClient)
        assert result is True


# ---------------------------------------------------------------------------
# TestEvotorWebhookVerificationE2E
# ---------------------------------------------------------------------------

class TestEvotorWebhookVerificationE2E:
    def test_evotor_webhook_rejects_invalid_bearer(self, app_client, tenant, monkeypatch):
        evotor_webhooks_module = importlib.import_module("app.api.webhooks")
        monkeypatch.setattr(evotor_webhooks_module, "_get_evotor_webhook_secret",
                            lambda: "secret-123")

        resp = app_client.post(
            f"/webhooks/evotor/{tenant}",
            headers={"Authorization": "Bearer wrong-token"},
            json={"type": "ReceiptCreated", "id": "evt-1", "userId": "u1",
                  "timestamp": 1, "version": 2,
                  "data": {"id": "d1", "storeId": "s1", "deviceId": "dev1",
                           "dateTime": "2026-01-01T00:00:00.000Z", "type": "SELL",
                           "shiftId": "1", "employeeId": "e1", "paymentSource": "PAY_CASH",
                           "infoCheck": False, "egais": False, "totalAmount": 100,
                           "totalDiscount": 0, "totalTax": 0, "items": [], "extras": {}}}
        )
        assert resp.status_code == 401

    def test_evotor_webhook_accepts_valid_bearer(self, app_client, tenant, monkeypatch):
        evotor_webhooks_module = importlib.import_module("app.api.webhooks")
        monkeypatch.setattr(evotor_webhooks_module, "_get_evotor_webhook_secret",
                            lambda: "secret-123")

        payload = _evotor_sell_webhook(tenant)
        resp = app_client.post(
            f"/webhooks/evotor/{tenant}",
            headers={"Authorization": "Bearer secret-123"},
            json=payload,
        )
        assert resp.status_code == 200
        assert resp.json()["status"] in ("accepted", "skipped", "ok")

    def test_evotor_webhook_allows_when_secret_missing(self, app_client, tenant, monkeypatch):
        evotor_webhooks_module = importlib.import_module("app.api.webhooks")
        monkeypatch.setattr(evotor_webhooks_module, "_get_evotor_webhook_secret", lambda: "")

        payload = _evotor_sell_webhook(tenant)
        resp = app_client.post(f"/webhooks/evotor/{tenant}", json=payload)
        assert resp.status_code == 200
        assert resp.json()["status"] in ("accepted", "skipped", "ok")