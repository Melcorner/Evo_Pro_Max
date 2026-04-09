"""
Небольшие E2E-тесты для реального FastAPI + SQLite контура.

Что здесь реально:
- FastAPI endpoints вызываются через TestClient
- SQLite база настоящая (временный файл)
- tenants / mappings / stock_sync_status читаются и пишутся по-настоящему

Что замокано:
- внешние интеграции Эвотор / МойСклад
- HTTP-запросы наружу

Если у вас app создаётся не в main.py и не в app.main,
поправьте функцию _load_app().
"""

from __future__ import annotations

import importlib
import sqlite3
import time
from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


# ============================================================================
# Загрузка приложения
# ============================================================================

def _load_app():
    from app.main import app
    return app

@pytest.fixture()
def client(temp_db_path: Path, db_getter, monkeypatch):
    sync_module = importlib.import_module("app.api.sync")
    webhooks_module = importlib.import_module("app.api.moysklad_webhooks")
    evotor_webhooks_module = importlib.import_module("app.api.webhooks")
    db_module = importlib.import_module("app.db")

    FakeMappingStore = _make_fake_mapping_store(temp_db_path)

    monkeypatch.setattr(db_module, "get_connection", db_getter, raising=False)
    monkeypatch.setattr(sync_module, "get_connection", db_getter, raising=False)
    monkeypatch.setattr(webhooks_module, "get_connection", db_getter, raising=False)
    monkeypatch.setattr(evotor_webhooks_module, "get_connection", db_getter, raising=False)

    monkeypatch.setattr(sync_module, "MappingStore", FakeMappingStore, raising=False)
    monkeypatch.setattr(webhooks_module, "MappingStore", FakeMappingStore, raising=False)

    app = _load_app()

    paths = {route.path for route in app.routes}
    assert "/webhooks/moysklad/{tenant_id}" in paths, paths

    return TestClient(app)
# ============================================================================
# SQLite helpers
# ============================================================================

def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _create_schema(db_path: Path) -> None:
    conn = _connect(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE tenants (
            id TEXT PRIMARY KEY,
            name TEXT,
            evotor_token TEXT,
            evotor_store_id TEXT,
            evotor_api_key TEXT,
            moysklad_token TEXT,
            sync_completed_at INTEGER,
            ms_organization_id TEXT,
            ms_store_id TEXT,
            ms_agent_id TEXT,
            created_at INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE mappings (
            tenant_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            evotor_id TEXT NOT NULL,
            ms_id TEXT NOT NULL,
            created_at INTEGER,
            updated_at INTEGER,
            UNIQUE(tenant_id, entity_type, evotor_id),
            UNIQUE(tenant_id, entity_type, ms_id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE stock_sync_status (
            tenant_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            started_at INTEGER,
            updated_at INTEGER,
            last_sync_at INTEGER,
            last_error TEXT,
            synced_items_count INTEGER DEFAULT 0,
            total_items_count INTEGER DEFAULT 0
        )
        """
    )


    cur.execute(
        """
        CREATE TABLE event_store (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            retries INTEGER DEFAULT 0,
            next_retry_at INTEGER,
            last_error_code TEXT,
            last_error_message TEXT,
            created_at INTEGER,
            updated_at INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE processed_events (
            tenant_id TEXT NOT NULL,
            event_key TEXT NOT NULL,
            result_ref TEXT,
            processed_at INTEGER,
            PRIMARY KEY (tenant_id, event_key)
        )
        """
    )

    conn.commit()
    conn.close()


# ============================================================================
# Fake MappingStore поверх реальной SQLite
# ============================================================================

def _make_fake_mapping_store(db_path: Path):
    class FakeMappingStore:
        def get_by_evotor_id(self, tenant_id: str, entity_type: str, evotor_id: str):
            conn = _connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT ms_id FROM mappings
                WHERE tenant_id = ? AND entity_type = ? AND evotor_id = ?
                """,
                (tenant_id, entity_type, evotor_id),
            )
            row = cur.fetchone()
            conn.close()
            return row["ms_id"] if row else None

        def get_by_ms_id(self, tenant_id: str, entity_type: str, ms_id: str):
            conn = _connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT evotor_id FROM mappings
                WHERE tenant_id = ? AND entity_type = ? AND ms_id = ?
                """,
                (tenant_id, entity_type, ms_id),
            )
            row = cur.fetchone()
            conn.close()
            return row["evotor_id"] if row else None

        def upsert_mapping(self, tenant_id: str, entity_type: str, evotor_id: str, ms_id: str):
            now = int(time.time())
            conn = _connect(db_path)
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    INSERT INTO mappings (tenant_id, entity_type, evotor_id, ms_id, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(tenant_id, entity_type, evotor_id)
                    DO UPDATE SET
                        ms_id = excluded.ms_id,
                        updated_at = excluded.updated_at
                    """,
                    (tenant_id, entity_type, evotor_id, ms_id, now, now),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False
            finally:
                conn.close()

    return FakeMappingStore


# ============================================================================
# Pytest fixtures
# ============================================================================

@pytest.fixture()
def temp_db_path(tmp_path: Path) -> Path:
    db_path = tmp_path / "test_app.db"
    _create_schema(db_path)
    return db_path


@pytest.fixture()
def db_getter(temp_db_path: Path) -> Callable[[], sqlite3.Connection]:
    def _get_connection():
        return _connect(temp_db_path)
    return _get_connection


@pytest.fixture()
def client(temp_db_path: Path, db_getter, monkeypatch):
    sync_module = importlib.import_module("app.api.sync")
    webhooks_module = importlib.import_module("app.api.moysklad_webhooks")
    evotor_webhooks_module = importlib.import_module("app.api.webhooks")
    db_module = importlib.import_module("app.db")

    FakeMappingStore = _make_fake_mapping_store(temp_db_path)

    # Все нужные модули переводим на временную SQLite.
    monkeypatch.setattr(db_module, "get_connection", db_getter, raising=False)
    monkeypatch.setattr(sync_module, "get_connection", db_getter, raising=False)
    monkeypatch.setattr(webhooks_module, "get_connection", db_getter, raising=False)
    monkeypatch.setattr(evotor_webhooks_module, "get_connection", db_getter, raising=False)

    # MappingStore тоже переводим на SQLite из фикстуры.
    monkeypatch.setattr(sync_module, "MappingStore", FakeMappingStore, raising=False)
    monkeypatch.setattr(webhooks_module, "MappingStore", FakeMappingStore, raising=False)

    app = _load_app()
    return TestClient(app)


# ============================================================================
# Seed helpers
# ============================================================================

def seed_tenant(
    db_path: Path,
    tenant_id: str = "tenant-1",
    *,
    sync_completed_at: int | None = None,
    evotor_token: str = "evotor-token",
    evotor_store_id: str = "store-1",
    moysklad_token: str = "ms-token",
):
    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO tenants (
            id, name, evotor_token, evotor_store_id, moysklad_token,
            sync_completed_at, ms_organization_id, ms_store_id, ms_agent_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tenant_id,
            "Test tenant",
            evotor_token,
            evotor_store_id,
            moysklad_token,
            sync_completed_at,
            "org-1",
            "ms-store-1",
            "agent-1",
            int(time.time()),
        ),
    )
    conn.commit()
    conn.close()



def seed_mapping(
    db_path: Path,
    *,
    tenant_id: str = "tenant-1",
    evotor_id: str,
    ms_id: str,
    entity_type: str = "product",
):
    now = int(time.time())
    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO mappings (tenant_id, entity_type, evotor_id, ms_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (tenant_id, entity_type, evotor_id, ms_id, now, now),
    )
    conn.commit()
    conn.close()


# ============================================================================
# E2E tests
# ============================================================================

class TestSyncStatusE2E:
    def test_sync_status_counts_real_mappings(self, client: TestClient, temp_db_path: Path):
        seed_tenant(temp_db_path, sync_completed_at=111111)
        seed_mapping(temp_db_path, evotor_id="ev-1", ms_id="ms-1")
        seed_mapping(temp_db_path, evotor_id="ev-2", ms_id="ms-2")

        response = client.get("/sync/tenant-1/status", headers={"Authorization": "Bearer 3kx1coO0KfD7gBzme5gvgnYTOFcbOsyh"})
        assert response.status_code == 200

        body = response.json()
        assert body["tenant_id"] == "tenant-1"
        assert body["sync_mode"] == "moysklad"
        assert body["product_mappings_count"] == 2
        assert body["evotor_store_configured"] is True
        assert body["moysklad_configured"] is True


class TestSingleStockSyncE2E:
    def test_single_stock_sync_creates_status_row(self, client: TestClient, temp_db_path: Path, monkeypatch):
        seed_tenant(temp_db_path, sync_completed_at=111111)
        seed_mapping(temp_db_path, evotor_id="ev-1", ms_id="ms-1")

        class FakeMoySkladClient:
            def __init__(self, tenant_id):
                self.tenant_id = tenant_id

            def get_product_stock(self, ms_product_id: str):
                assert ms_product_id == "ms-1"
                return 7.0

        class FakeEvotorClient:
            def __init__(self, tenant_id):
                self.tenant_id = tenant_id

            def update_product_stock(self, evotor_product_id: str, quantity: float):
                assert evotor_product_id == "ev-1"
                assert quantity == 7.0

        ms_client_module = importlib.import_module("app.clients.moysklad_client")
        evotor_client_module = importlib.import_module("app.clients.evotor_client")
        monkeypatch.setattr(ms_client_module, "MoySkladClient", FakeMoySkladClient, raising=False)
        monkeypatch.setattr(evotor_client_module, "EvotorClient", FakeEvotorClient, raising=False)

        response = client.post("/sync/tenant-1/stock/ms-1", headers={"Authorization": "Bearer 3kx1coO0KfD7gBzme5gvgnYTOFcbOsyh"})
        assert response.status_code == 200

        body = response.json()
        assert body["status"] == "ok"
        assert body["quantity"] == 7.0
        assert body["evotor_product_id"] == "ev-1"

        conn = _connect(temp_db_path)
        cur = conn.cursor()
        cur.execute("SELECT * FROM stock_sync_status WHERE tenant_id = ?", ("tenant-1",))
        row = cur.fetchone()
        conn.close()

        assert row is not None
        assert row["status"] == "ok"
        assert row["last_error"] is None
        assert row["synced_items_count"] == 1
        assert row["total_items_count"] == 1
        assert row["last_sync_at"] is not None


class TestInitialSyncDedupE2E:
    def test_initial_sync_reuses_existing_ms_product(self, client: TestClient, temp_db_path: Path, monkeypatch):
        seed_tenant(temp_db_path, sync_completed_at=None)

        sync_module = importlib.import_module("app.api.sync")

        monkeypatch.setattr(
            sync_module,
            "_get_evotor_products",
            lambda evotor_token, store_id: [
                {
                    "id": "ev-1",
                    "name": "МОЛОКО",
                    "price": 120.0,
                    "description": "test product",
                    "barcodes": ["2000000000053"],
                }
            ],
        )
        monkeypatch.setattr(sync_module, "_find_ms_product_by_external_code", lambda token, external_code: "ms-existing-1")

        create_mock = MagicMock(return_value="ms-created-should-not-happen")
        monkeypatch.setattr(sync_module, "_create_ms_product", create_mock)

        response = client.post("/sync/tenant-1/initial", headers={"Authorization": "Bearer 3kx1coO0KfD7gBzme5gvgnYTOFcbOsyh"})
        assert response.status_code == 200

        body = response.json()
        assert body["status"] == "ok"
        assert body["synced"] == 1
        assert body["failed"] == 0
        create_mock.assert_not_called()

        conn = _connect(temp_db_path)
        cur = conn.cursor()
        cur.execute(
            "SELECT ms_id FROM mappings WHERE tenant_id = ? AND entity_type = 'product' AND evotor_id = ?",
            ("tenant-1", "ev-1"),
        )
        row = cur.fetchone()
        conn.close()

        assert row is not None
        assert row["ms_id"] == "ms-existing-1"


class TestMoySkladWebhookE2E:
    def test_webhook_returns_partial_when_positions_fetch_fails(
        self,
        client: TestClient,
        temp_db_path: Path,
        monkeypatch,
    ):
        seed_tenant(temp_db_path, sync_completed_at=111111)
        seed_mapping(temp_db_path, evotor_id="ev-1", ms_id="ms-1")

        webhooks_module = importlib.import_module("app.api.moysklad_webhooks")

        class FakeMoySkladClient:
            BASE_URL = "https://api.moysklad.ru/api/remap/1.2"

            def __init__(self, tenant_id):
                self.tenant_id = tenant_id

            def _headers(self):
                return {"Authorization": "Bearer test"}

        monkeypatch.setattr(webhooks_module, "MoySkladClient", FakeMoySkladClient, raising=False)

        import requests

        bad_response = MagicMock()
        bad_response.ok = False
        bad_response.status_code = 500
        bad_response.text = "Internal Server Error"
        bad_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500")

        import requests
        monkeypatch.setattr(requests, "get", lambda *args, **kwargs: bad_response)

        response = client.post(
            "/webhooks/moysklad/tenant-1",
            json={
                "events": [
                    {
                        "meta": {
                            "href": "https://api.moysklad.ru/api/remap/1.2/entity/demand/doc-1",
                            "type": "demand",
                        }
                    }
                ]
            },
        )
        assert response.status_code == 200

        body = response.json()
        assert body["status"] == "partial"
        assert body["failed"] == 1
        assert body["docs_processed"] == 1


class TestSalePipelineE2E:
    """
    E2E тест полного sale pipeline:
    Webhook Эвотор → event_store → worker → DONE/RETRY/FAILED
    на реальной SQLite + замоканные внешние вызовы.
    """

    def _seed_event_store(self, db_path):
        conn = _connect(db_path)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS event_store (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                retries INTEGER NOT NULL DEFAULT 0,
                next_retry_at INTEGER,
                last_error_code TEXT,
                last_error_message TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS processed_events (
                tenant_id TEXT NOT NULL,
                event_key TEXT NOT NULL,
                result_ref TEXT,
                processed_at INTEGER NOT NULL,
                PRIMARY KEY (tenant_id, event_key)
            );
            CREATE TABLE IF NOT EXISTS errors (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                error_code TEXT,
                message TEXT NOT NULL,
                payload_snapshot TEXT,
                response_body TEXT,
                created_at INTEGER NOT NULL
            );
        """)
        conn.commit()
        conn.close()

    def _insert_event(self, db_path, *, event_id, tenant_id="tenant-1",
                      event_type="sale", event_key, payload, status="NEW"):
        import json
        now = int(time.time())
        conn = _connect(db_path)
        conn.execute(
            "INSERT INTO event_store (id, tenant_id, event_type, event_key, payload_json, "
            "status, retries, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)",
            (event_id, tenant_id, event_type, event_key, json.dumps(payload), status, now, now),
        )
        conn.commit()
        conn.close()

    def _get_event(self, db_path, event_id):
        conn = _connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT * FROM event_store WHERE id = ?", (event_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else {}

    def _get_processed(self, db_path, event_key):
        conn = _connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT * FROM processed_events WHERE event_key = ?", (event_key,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def _patch_all(self, monkeypatch, temp_db_path, FakeClient):
        import importlib
        db_module = importlib.import_module("app.db")
        worker_module = importlib.import_module("app.workers.worker")
        sale_handler_module = importlib.import_module("app.handlers.sale_handler")
        sale_mapper_module = importlib.import_module("app.mappers.sale_mapper")
        ms_client_module = importlib.import_module("app.clients.moysklad_client")
        counterparty_module = importlib.import_module("app.services.counterparty_resolver")

        def db_getter():
            return _connect(temp_db_path)

        monkeypatch.setattr(db_module, "get_connection", db_getter)
        monkeypatch.setattr(worker_module, "get_connection", db_getter)
        monkeypatch.setattr(sale_handler_module, "get_connection", db_getter)
        monkeypatch.setattr(sale_mapper_module, "MappingStore",
                            _make_fake_mapping_store(temp_db_path))

        # Патчим клиент в обоих местах: в модуле клиента и в sale_handler,
        # который импортирует его напрямую через "from ... import MoySkladClient"
        monkeypatch.setattr(ms_client_module, "MoySkladClient", FakeClient)
        monkeypatch.setattr(sale_handler_module, "MoySkladClient", FakeClient)
        monkeypatch.setattr(counterparty_module, "MoySkladClient", FakeClient)

    def test_sale_event_processed_to_done(self, temp_db_path, monkeypatch):
        """Happy path: NEW → worker → DONE + processed_events."""
        seed_tenant(temp_db_path, sync_completed_at=111111)
        seed_mapping(temp_db_path, evotor_id="ev-prod-1", ms_id="ms-prod-1")
        self._seed_event_store(temp_db_path)

        payload = {
            "id": "evt-sale-1", "type": "SELL", "customer": None,
            "body": {"positions": [{"product_id": "ev-prod-1", "product_name": "МОЛОКО",
                                    "quantity": 1, "price": 120.0, "sum": 120.0,
                                    "taxPercent": 10}], "sum": 120.0},
        }
        self._insert_event(temp_db_path, event_id="evt-sale-1",
                           event_key="receipt-001", payload=payload)

        class FakeMoySkladClient:
            def __init__(self, tenant_id):
                # Не вызываем _load_token — не ходим в БД
                self.tenant_id = tenant_id
                self.token = "fake-token"
                self.BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
            def create_sale_document(self, payload):
                return {"success": True, "result_ref": "demand-ref-1", "raw_response": {}}
            def find_counterparty_by_email(self, email): return None
            def find_counterparty_by_phone(self, phone): return None

        self._patch_all(monkeypatch, temp_db_path, FakeMoySkladClient)

        from app.workers.worker import process_one_event
        assert process_one_event() is True

        event = self._get_event(temp_db_path, "evt-sale-1")
        assert event["status"] == "DONE", f"Expected DONE, got {event['status']}"
        assert event["retries"] == 0

        processed = self._get_processed(temp_db_path, "receipt-001")
        assert processed is not None
        assert processed["result_ref"] == "demand-ref-1"

    def test_sale_event_goes_to_retry_on_network_error(self, temp_db_path, monkeypatch):
        """При ConnectionError → RETRY, retries=1, next_retry_at заполнен."""
        import requests as req_lib
        seed_tenant(temp_db_path, sync_completed_at=111111)
        seed_mapping(temp_db_path, evotor_id="ev-prod-1", ms_id="ms-prod-1")
        self._seed_event_store(temp_db_path)

        payload = {
            "id": "evt-sale-2", "type": "SELL", "customer": None,
            "body": {"positions": [{"product_id": "ev-prod-1", "product_name": "МОЛОКО",
                                    "quantity": 1, "price": 120.0, "sum": 120.0,
                                    "taxPercent": 10}], "sum": 120.0},
        }
        self._insert_event(temp_db_path, event_id="evt-sale-2",
                           event_key="receipt-002", payload=payload)

        class FakeMoySkladClientNetworkError:
            def __init__(self, tenant_id):
                self.tenant_id = tenant_id
                self.token = "fake-token"
                self.BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
            def create_sale_document(self, payload):
                raise req_lib.exceptions.ConnectionError("Connection refused")
            def find_counterparty_by_email(self, email): return None
            def find_counterparty_by_phone(self, phone): return None

        self._patch_all(monkeypatch, temp_db_path, FakeMoySkladClientNetworkError)

        from app.workers.worker import process_one_event
        assert process_one_event() is True

        event = self._get_event(temp_db_path, "evt-sale-2")
        assert event["status"] == "RETRY", f"Expected RETRY, got {event['status']}"
        assert event["retries"] == 1
        assert event["next_retry_at"] is not None

    def test_sale_event_goes_to_failed_on_mapping_error(self, temp_db_path, monkeypatch):
        """MappingNotFoundError → сразу FAILED, next_retry_at=None."""
        seed_tenant(temp_db_path, sync_completed_at=111111)
        # Маппинга намеренно нет
        self._seed_event_store(temp_db_path)

        payload = {
            "id": "evt-sale-3", "type": "SELL", "customer": None,
            "body": {"positions": [{"product_id": "ev-unknown", "product_name": "НЕИЗВЕСТНЫЙ",
                                    "quantity": 1, "price": 100.0, "sum": 100.0,
                                    "taxPercent": 0}], "sum": 100.0},
        }
        self._insert_event(temp_db_path, event_id="evt-sale-3",
                           event_key="receipt-003", payload=payload)

        class FakeMoySkladClient:
            def __init__(self, tenant_id):
                self.tenant_id = tenant_id
                self.token = "fake-token"
                self.BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
            def create_sale_document(self, payload):
                return {"success": True, "result_ref": "demand-1", "raw_response": {}}
            def find_counterparty_by_email(self, email): return None
            def find_counterparty_by_phone(self, phone): return None

        self._patch_all(monkeypatch, temp_db_path, FakeMoySkladClient)

        from app.workers.worker import process_one_event
        assert process_one_event() is True

        event = self._get_event(temp_db_path, "evt-sale-3")
        assert event["status"] == "FAILED", f"Expected FAILED, got {event['status']}"
        assert event["next_retry_at"] is None

# ============================================================================
# E2E для верификации webhook Эвотор
# ============================================================================

class TestEvotorWebhookVerificationE2E:
    def test_evotor_webhook_rejects_invalid_bearer(self, client: TestClient, temp_db_path: Path, monkeypatch):
        seed_tenant(temp_db_path, tenant_id="tenant-verify")
        evotor_webhooks_module = importlib.import_module("app.api.webhooks")
        monkeypatch.setattr(evotor_webhooks_module, "_get_evotor_webhook_secret", lambda: "secret-123")

        response = client.post(
            "/webhooks/evotor/tenant-verify",
            headers={"Authorization": "Bearer wrong-token"},
            json={
                "type": "SELL",
                "id": "evt-verify-1",
                "body": {
                    "positions": [
                        {
                            "product_id": "p1",
                            "quantity": 1,
                            "price": 100.0
                        }
                    ]
                }
            },
        )

        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid webhook signature"

    def test_evotor_webhook_accepts_valid_bearer(self, client: TestClient, temp_db_path: Path, monkeypatch):
        seed_tenant(temp_db_path, tenant_id="tenant-verify-ok")
        evotor_webhooks_module = importlib.import_module("app.api.webhooks")
        monkeypatch.setattr(evotor_webhooks_module, "_get_evotor_webhook_secret", lambda: "secret-123")

        response = client.post(
            "/webhooks/evotor/tenant-verify-ok",
            headers={"Authorization": "Bearer secret-123"},
            json={
                "type": "SELL",
                "id": "evt-verify-2",
                "body": {
                    "positions": [
                        {
                            "product_id": "p1",
                            "quantity": 1,
                            "price": 100.0
                        }
                    ]
                }
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        assert "event_id" in data

    def test_evotor_webhook_allows_requests_when_secret_missing(self, client: TestClient, temp_db_path: Path, monkeypatch):
        seed_tenant(temp_db_path, tenant_id="tenant-verify-open")
        evotor_webhooks_module = importlib.import_module("app.api.webhooks")
        monkeypatch.setattr(evotor_webhooks_module, "_get_evotor_webhook_secret", lambda: "")

        response = client.post(
            "/webhooks/evotor/tenant-verify-open",
            json={
                "type": "SELL",
                "id": "evt-verify-3",
                "body": {
                    "positions": [
                        {
                            "product_id": "p1",
                            "quantity": 1,
                            "price": 100.0
                        }
                    ]
                }
            },
        )

        assert response.status_code == 200
        assert response.json()["status"] == "accepted"
