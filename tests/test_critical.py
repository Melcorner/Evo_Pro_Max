"""
Автотесты критических сценариев интеграционной шины Эвотор ↔ МойСклад.

Покрывает:
- duplicate prevention в initial_sync
- retry/fail policy worker'а
- sale mapping на реальных payload'ах
- webhook stock sync при неуспешном чтении позиций
- fallback контрагента
- распознавание формата штрихкода
"""

import json
import time
import pytest
from unittest.mock import MagicMock, patch, call


# ===========================================================================
# 1. Распознавание формата штрихкода
# ===========================================================================

class TestDetectBarcodeFormat:
    def _fn(self):
        from app.api.sync import _detect_barcode_format
        return _detect_barcode_format

    def test_ean13(self):
        assert self._fn()("2000000000053") == "ean13"

    def test_ean8(self):
        assert self._fn()("12345678") == "ean8"

    def test_gtin(self):
        assert self._fn()("04607038235002") == "gtin"

    def test_code128_letters(self):
        assert self._fn()("ABC-12345") == "code128"

    def test_code128_short_digits(self):
        assert self._fn()("123") == "code128"

    def test_code128_long_digits(self):
        # 15 цифр — не EAN и не GTIN
        assert self._fn()("123456789012345") == "code128"


# ===========================================================================
# 2. sale_mapper — маппинг на реальных payload'ах
# ===========================================================================

SALE_PAYLOAD_PRODUCTION = {
    "id": "evt-001",
    "type": "SELL",
    "body": {
        "positions": [
            {
                "product_id": "evotor-prod-1",
                "product_name": "МОЛОКО",
                "quantity": 2,
                "price": 200.0,
                "sum": 400.0,
                "discount": 40.0,        # 10% скидка
                "taxPercent": 10,
            }
        ],
        "sum": 360.0,
    },
}

SALE_PAYLOAD_ENRICHED = {
    "id": "evt-002",
    "type": "SELL",
    "body": {
        "positions": [
            {
                "product_id": "evotor-prod-2",
                "product_name": "КЕФИР",
                "quantity": 1,
                "price": 100.0,
                "sum": 100.0,
                "resultSum": 80.0,
                "positionDiscount": {"discountPercent": 20},
                "taxPercent": 20,
            }
        ],
        "sum": 80.0,
    },
}


class TestSaleMapper:
    @patch("app.stores.mapping_store.MappingStore")
    def test_production_payload_discount(self, MockStore):
        from app.mappers.sale_mapper import map_sale_to_ms

        store_instance = MockStore.return_value
        store_instance.get_by_evotor_id.return_value = "ms-prod-1"

        result = map_sale_to_ms(
            SALE_PAYLOAD_PRODUCTION,
            sync_id="evt-001",
            tenant_id="t1",
            ms_organization_id="org-1",
            ms_store_id="store-1",
            ms_agent_id="agent-1",
        )

        pos = result["positions"][0]
        # Базовая цена не изменена
        assert pos["price"] == 200 * 100
        # Скидка 10% = discount/sum*100 = 40/400*100
        assert abs(pos["discount"] - 10.0) < 0.1
        # НДС из чека
        assert pos["vat"] == 10
        assert pos["vatEnabled"] is True

    @patch("app.stores.mapping_store.MappingStore")
    def test_enriched_payload_discount(self, MockStore):
        from app.mappers.sale_mapper import map_sale_to_ms

        store_instance = MockStore.return_value
        store_instance.get_by_evotor_id.return_value = "ms-prod-2"

        result = map_sale_to_ms(
            SALE_PAYLOAD_ENRICHED,
            sync_id="evt-002",
            tenant_id="t1",
            ms_organization_id="org-1",
            ms_store_id="store-1",
            ms_agent_id="agent-1",
        )

        pos = result["positions"][0]
        assert pos["discount"] == 20.0
        assert pos["vat"] == 20
        assert pos["vatEnabled"] is True

    def test_invalid_type_raises(self):
        from app.mappers.sale_mapper import map_sale_to_ms, SalePayloadError

        bad_payload = {**SALE_PAYLOAD_PRODUCTION, "type": "REFUND"}
        with pytest.raises(SalePayloadError):
            map_sale_to_ms(bad_payload)

    def test_empty_positions_raises(self):
        from app.mappers.sale_mapper import map_sale_to_ms, SalePayloadError

        payload = {**SALE_PAYLOAD_PRODUCTION, "body": {"positions": []}}
        with pytest.raises(SalePayloadError):
            map_sale_to_ms(payload)

    @patch("app.mappers.sale_mapper.MappingStore")
    def test_missing_mapping_raises(self, MockStore):
        from app.mappers.sale_mapper import map_sale_to_ms, MappingNotFoundError

        store_instance = MockStore.return_value
        store_instance.get_by_evotor_id.return_value = None

        with pytest.raises(MappingNotFoundError):
            map_sale_to_ms(SALE_PAYLOAD_PRODUCTION, tenant_id="t1")

    def test_no_vat(self):
        from app.mappers.sale_mapper import _extract_vat_fields

        item = {"taxPercent": 0}
        result = _extract_vat_fields(item)
        assert result == {"vat": 0, "vatEnabled": False}


# ===========================================================================
# 3. Fallback контрагента
# ===========================================================================

class TestCounterpartyResolver:
    def _resolve(self, payload, default_id=None):
        from app.services.counterparty_resolver import resolve_counterparty_for_sale
        return resolve_counterparty_for_sale(payload, "tenant-1", default_id)

    @patch("app.services.counterparty_resolver.MoySkladClient")
    def test_found_by_email(self, MockClient):
        client = MockClient.return_value
        client.find_counterparty_by_email.return_value = {"id": "cp-email-1"}
        client.find_counterparty_by_phone.return_value = None

        payload = {"customer": {"email": "test@example.com", "phone": "79001234567"}}
        agent_id, source = self._resolve(payload, "default-agent")

        assert agent_id == "cp-email-1"
        assert source == "found_by_email"

    @patch("app.services.counterparty_resolver.MoySkladClient")
    def test_found_by_phone_when_no_email(self, MockClient):
        client = MockClient.return_value
        client.find_counterparty_by_email.return_value = None
        client.find_counterparty_by_phone.return_value = {"id": "cp-phone-1"}

        payload = {"customer": {"phone": "79001234567"}}
        agent_id, source = self._resolve(payload, "default-agent")

        assert agent_id == "cp-phone-1"
        assert source == "found_by_phone"

    @patch("app.services.counterparty_resolver.MoySkladClient")
    def test_creates_counterparty_when_not_found(self, MockClient):
        client = MockClient.return_value
        client.find_counterparty_by_email.return_value = None
        client.find_counterparty_by_phone.return_value = None
        client.create_counterparty.return_value = {"id": "cp-new-1"}

        payload = {"customer": {"name": "Иван", "phone": "79001234567"}}
        agent_id, source = self._resolve(payload, "default-agent")

        assert agent_id == "cp-new-1"
        assert source == "created_counterparty"

    @patch("app.services.counterparty_resolver.MoySkladClient")
    def test_fallback_to_default_on_error(self, MockClient):
        client = MockClient.return_value
        client.find_counterparty_by_email.side_effect = Exception("network error")

        payload = {"customer": {"email": "test@example.com"}}
        agent_id, source = self._resolve(payload, "default-agent")

        assert agent_id == "default-agent"
        assert source == "default_agent_on_error"

    def test_no_customer_returns_default(self):
        payload = {"customer": {}}
        agent_id, source = self._resolve(payload, "default-agent")

        assert agent_id == "default-agent"
        assert source == "default_agent"

    @patch("app.services.counterparty_resolver.MoySkladClient")
    def test_fallback_name_when_no_name(self, MockClient):
        """При отсутствии имени используется email как fallback для create."""
        client = MockClient.return_value
        client.find_counterparty_by_email.return_value = None
        client.find_counterparty_by_phone.return_value = None
        client.create_counterparty.return_value = {"id": "cp-new-2"}

        payload = {"customer": {"email": "buyer@example.com"}}
        agent_id, source = self._resolve(payload, "default-agent")

        # create_counterparty должен получить name=email, а не None
        call_kwargs = client.create_counterparty.call_args
        assert call_kwargs.kwargs.get("name") == "buyer@example.com"
        assert source == "created_counterparty"


# ===========================================================================
# 4. Worker — retry/fail policy
# ===========================================================================

class TestWorkerPolicy:
    def _make_row(self, retries=0, status="NEW"):
        return {
            "id": "evt-w1",
            "tenant_id": "t1",
            "event_type": "sale",
            "event_key": "key-1",
            "payload_json": "{}",
            "status": status,
            "retries": retries,
        }

    @patch("app.workers.worker.insert_error")
    @patch("app.workers.worker.dispatch_event")
    @patch("app.workers.worker.get_connection")
    def test_retry_on_transient_error(self, mock_conn, mock_dispatch, mock_insert):
        from app.services.error_logic import RETRY
        mock_dispatch.side_effect = Exception("timeout")

        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = self._make_row(retries=0)
        cursor.rowcount = 1
        conn.cursor.return_value = cursor
        mock_conn.return_value = conn

        with patch("app.workers.worker.classify_error", return_value=RETRY):
            from app.workers.worker import process_one_event
            process_one_event()

        # Должен записать RETRY, не FAILED
        updates = [str(c) for c in cursor.execute.call_args_list]
        retry_calls = [u for u in updates if "RETRY" in u]
        failed_calls = [u for u in updates if "FAILED" in u]
        assert len(retry_calls) > 0
        assert len(failed_calls) == 0

    @patch("app.workers.worker.insert_error")
    @patch("app.workers.worker.dispatch_event")
    @patch("app.workers.worker.get_connection")
    def test_failed_after_max_retries(self, mock_conn, mock_dispatch, mock_insert):
        from app.services.error_logic import RETRY
        mock_dispatch.side_effect = Exception("timeout")

        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = self._make_row(retries=4)  # следующий = 5 → FAILED
        cursor.rowcount = 1
        conn.cursor.return_value = cursor
        mock_conn.return_value = conn

        with patch("app.workers.worker.classify_error", return_value=RETRY):
            from app.workers.worker import process_one_event
            process_one_event()

        updates = [str(c) for c in cursor.execute.call_args_list]
        failed_calls = [u for u in updates if "FAILED" in u]
        assert len(failed_calls) > 0

    @patch("app.workers.worker.insert_error")
    @patch("app.workers.worker.dispatch_event")
    @patch("app.workers.worker.get_connection")
    def test_conn_closed_on_exception(self, mock_conn, mock_dispatch, mock_insert):
        """Соединение закрывается даже при неожиданном исключении."""
        mock_dispatch.side_effect = RuntimeError("unexpected")

        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = self._make_row()
        cursor.rowcount = 1
        conn.cursor.return_value = cursor
        mock_conn.return_value = conn

        with patch("app.workers.worker.classify_error", return_value="FAILED"):
            from app.workers.worker import process_one_event
            process_one_event()

        conn.close.assert_called()


# ===========================================================================
# 5. Webhook МойСклад — ошибка чтения позиций
# ===========================================================================

class TestMoySkladWebhook:
    @pytest.mark.asyncio
    @patch("app.api.moysklad_webhooks._load_tenant")
    @patch("app.api.moysklad_webhooks.MoySkladClient")
    async def test_failed_positions_fetch_returns_partial(self, MockClient, mock_tenant):
        from app.api.moysklad_webhooks import moysklad_webhook, MoySkladWebhook

        mock_tenant.return_value = {
            "sync_completed_at": 1,
            "evotor_token": "tok",
            "evotor_store_id": "store",
            "moysklad_token": "tok",
        }

        client = MockClient.return_value
        # Симулируем HTTP-ошибку при чтении позиций
        import requests
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500")

        with patch("requests.get", return_value=mock_resp):
            body = MoySkladWebhook.model_validate({
                "events": [{
                    "meta": {
                        "href": "https://api.moysklad.ru/api/remap/1.2/entity/demand/some-uuid",
                        "type": "demand"
                    }
                }]
            })
            result = await moysklad_webhook("tenant-1", body)

        # Статус должен быть partial, не ok
        assert result["status"] == "partial"
        assert result["failed"] > 0


# ===========================================================================
# 6. initial_sync — защита от дублей
# ===========================================================================

class TestInitialSyncDedup:
    @patch("app.api.sync._upsert_stock_status")
    @patch("app.api.sync._get_evotor_products")
    @patch("app.api.sync._find_ms_product_by_external_code")
    @patch("app.api.sync._create_ms_product")
    @patch("app.api.sync.MappingStore")
    @patch("app.api.sync._load_tenant")
    @patch("app.api.sync.get_connection")
    def test_no_duplicate_when_ms_product_exists(
        self, mock_conn, mock_tenant, MockStore,
        mock_create, mock_find, mock_get_evotor, mock_upsert
    ):
        """Если товар уже есть в МойСклад по externalCode — create не вызывается."""
        mock_tenant.return_value = {
            "sync_completed_at": None,
            "evotor_token": "tok",
            "evotor_store_id": "store",
            "moysklad_token": "ms-tok",
        }
        mock_get_evotor.return_value = [{"id": "ev-1", "name": "МОЛОКО"}]

        store = MockStore.return_value
        store.get_by_evotor_id.return_value = None  # mapping ещё нет
        store.upsert_mapping.return_value = True

        mock_find.return_value = "ms-existing-1"  # товар уже есть в МС

        conn = MagicMock()
        mock_conn.return_value = conn

        from app.api.sync import initial_sync
        result = initial_sync("tenant-1")

        # create не должен вызываться
        mock_create.assert_not_called()
        # mapping должен сохраниться
        store.upsert_mapping.assert_called_once()
        assert result["synced"] == 1

    @patch("app.api.sync._get_evotor_products")
    @patch("app.api.sync._find_ms_product_by_external_code")
    @patch("app.api.sync.MappingStore")
    @patch("app.api.sync._load_tenant")
    def test_aborts_when_search_fails(
        self, mock_tenant, MockStore, mock_find, mock_get_evotor
    ):
        """Если поиск по externalCode вернул HTTP-ошибку — sync падает, не создаёт товар."""
        import requests
        mock_tenant.return_value = {
            "sync_completed_at": None,
            "evotor_token": "tok",
            "evotor_store_id": "store",
            "moysklad_token": "ms-tok",
        }
        mock_get_evotor.return_value = [{"id": "ev-1", "name": "МОЛОКО"}]

        store = MockStore.return_value
        store.get_by_evotor_id.return_value = None

        mock_find.side_effect = requests.exceptions.HTTPError("401")

        from app.api.sync import initial_sync
        result = initial_sync("tenant-1")

        # Должен упасть в errors, не создать товар
        assert result["failed"] == 1
        assert result["synced"] == 0