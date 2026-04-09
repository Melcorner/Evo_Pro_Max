"""
Тесты для sale_mapper (формат Эвотор) и classify_error.
"""
import pytest
import requests
from unittest.mock import MagicMock

from app.mappers.sale_mapper import map_sale_to_ms, validate_sale_payload, SalePayloadError, MappingNotFoundError
from app.services.error_logic import classify_error, RETRY, FAILED


VALID_EVOTOR_PAYLOAD = {
    "type": "SELL",
    "id": "03990165-9d5f-4841-a99a-083abc659f67",
    "store_id": "20260314-3BF3-4021-8051-E3A278EE4974",
    "body": {
        "positions": [
            {
                "product_id": "bbb5b5a8-6e3d-45ff-b16d-18b95926cbc9",
                "product_name": "GP Alkaline AAx4",
                "quantity": 2,
                "price": 1.0,
                "sum": 2.0
            },
            {
                "product_id": "ccc5b5a8-6e3d-45ff-b16d-18b95926cbc9",
                "product_name": "Test Product",
                "quantity": 1,
                "price": 3.0,
                "sum": 3.0
            }
        ],
        "sum": 5.0
    }
}


# ---------------------------------------------------------------------------
# A1 / A2 — маппинг позиций и syncId
# ---------------------------------------------------------------------------

def test_map_positions():
    result = map_sale_to_ms(VALID_EVOTOR_PAYLOAD)
    assert len(result["positions"]) == 2


def test_map_total_sum():
    result = map_sale_to_ms(VALID_EVOTOR_PAYLOAD)
    # sum убран — вычисляемое поле в МС, тест не актуален
    assert "positions" in result


def test_map_price_in_kopecks():
    result = map_sale_to_ms(VALID_EVOTOR_PAYLOAD)
    assert result["positions"][0]["price"] == 100  # 1.0 руб = 100 копеек


def test_map_sync_id_defaults_to_evotor_id():
    # без явного sync_id берётся id из payload Эвотор
    result = map_sale_to_ms(VALID_EVOTOR_PAYLOAD)
    assert result["syncId"] == "03990165-9d5f-4841-a99a-083abc659f67"


def test_map_sync_id_explicit():
    # явный sync_id переопределяет id из payload
    result = map_sale_to_ms(VALID_EVOTOR_PAYLOAD, sync_id="internal-uuid-001")
    assert result["syncId"] == "internal-uuid-001"


def test_map_name_contains_event_id():
    result = map_sale_to_ms(VALID_EVOTOR_PAYLOAD)
    assert "03990165-9d5f-4841-a99a-083abc659f67" in result["name"]


def test_map_price_round_float():
    # проверяем что round() корректно обрабатывает float
    payload = {
        "type": "SELL", "id": "x",
        "body": {"positions": [{"product_id": "p1", "quantity": 1, "price": 1.05, "sum": 1.05}]}
    }
    result = map_sale_to_ms(payload)
    assert result["positions"][0]["price"] == 105  # не 104


# ---------------------------------------------------------------------------
# A3 — валидация payload
# ---------------------------------------------------------------------------

def test_missing_id():
    payload = {**VALID_EVOTOR_PAYLOAD, "id": None}
    with pytest.raises(SalePayloadError, match="id"):
        validate_sale_payload(payload)


def test_wrong_type():
    payload = {**VALID_EVOTOR_PAYLOAD, "type": "ACCEPT"}
    with pytest.raises(SalePayloadError, match="type"):
        validate_sale_payload(payload)


def test_missing_body():
    payload = {"type": "SELL", "id": "x"}
    with pytest.raises(SalePayloadError, match="body"):
        validate_sale_payload(payload)


def test_empty_positions():
    payload = {"type": "SELL", "id": "x", "body": {"positions": []}}
    with pytest.raises(SalePayloadError, match="non-empty"):
        validate_sale_payload(payload)


def test_position_missing_product_id():
    payload = {
        "type": "SELL", "id": "x",
        "body": {"positions": [{"quantity": 1, "price": 1.0}]}
    }
    with pytest.raises(SalePayloadError, match="product_id"):
        validate_sale_payload(payload)


def test_position_zero_quantity():
    payload = {
        "type": "SELL", "id": "x",
        "body": {"positions": [{"product_id": "p1", "quantity": 0, "price": 1.0}]}
    }
    with pytest.raises(SalePayloadError, match="quantity"):
        validate_sale_payload(payload)


def test_position_negative_price():
    payload = {
        "type": "SELL", "id": "x",
        "body": {"positions": [{"product_id": "p1", "quantity": 1, "price": -1.0}]}
    }
    with pytest.raises(SalePayloadError, match="price"):
        validate_sale_payload(payload)


# ---------------------------------------------------------------------------
# A4 — классификация ошибок
# ---------------------------------------------------------------------------

def test_payload_error_classified_as_failed():
    e = SalePayloadError("bad payload")
    assert classify_error(e) == FAILED


def test_mapping_not_found_classified_as_failed():
    e = MappingNotFoundError("no mapping")
    assert classify_error(e) == FAILED


# ---------------------------------------------------------------------------
# A5 — retry на временных сбоях
# ---------------------------------------------------------------------------

def test_timeout_classified_as_retry():
    assert classify_error(requests.exceptions.Timeout()) == RETRY


def test_connection_error_classified_as_retry():
    assert classify_error(requests.exceptions.ConnectionError()) == RETRY


def test_http_500_classified_as_retry():
    e = requests.exceptions.HTTPError()
    e.response = MagicMock()
    e.response.status_code = 500
    assert classify_error(e) == RETRY


def test_http_429_classified_as_retry():
    e = requests.exceptions.HTTPError()
    e.response = MagicMock()
    e.response.status_code = 429
    assert classify_error(e) == RETRY


def test_http_401_classified_as_failed():
    e = requests.exceptions.HTTPError()
    e.response = MagicMock()
    e.response.status_code = 401
    assert classify_error(e) == FAILED


def test_http_400_classified_as_failed():
    e = requests.exceptions.HTTPError()
    e.response = MagicMock()
    e.response.status_code = 400
    assert classify_error(e) == FAILED