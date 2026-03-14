"""
Тесты для sale_mapper (A1-A3) и classify_error (A4-A5).
"""
import pytest
import requests
from unittest.mock import MagicMock

from app.mappers.sale_mapper import map_sale_to_ms, validate_sale_payload, SalePayloadError
from app.services.error_logic import classify_error, RETRY, FAILED


# ---------------------------------------------------------------------------
# A1 / A2 — маппинг позиций и syncId
# ---------------------------------------------------------------------------

VALID_PAYLOAD = {
    "event_id": "sale-001",
    "positions": [
        {"product_id": "p1", "quantity": 2, "price": 500},
        {"product_id": "p2", "quantity": 1, "price": 300},
    ]
}


def test_map_positions():
    result = map_sale_to_ms(VALID_PAYLOAD)
    assert len(result["positions"]) == 2
    assert result["positions"][0]["sum"] == 1000
    assert result["positions"][1]["sum"] == 300


def test_map_total_sum():
    result = map_sale_to_ms(VALID_PAYLOAD)
    assert result["sum"] == 1300


def test_map_sync_id():
    result = map_sale_to_ms(VALID_PAYLOAD)
    assert result["syncId"] == "sale-001"


def test_map_name_contains_event_id():
    result = map_sale_to_ms(VALID_PAYLOAD)
    assert "sale-001" in result["name"]


# ---------------------------------------------------------------------------
# A3 — валидация payload
# ---------------------------------------------------------------------------

def test_missing_event_id():
    with pytest.raises(SalePayloadError, match="event_id"):
        validate_sale_payload({"positions": [{"product_id": "p1", "quantity": 1, "price": 100}]})


def test_missing_positions():
    with pytest.raises(SalePayloadError, match="positions"):
        validate_sale_payload({"event_id": "x"})


def test_empty_positions():
    with pytest.raises(SalePayloadError, match="empty"):
        validate_sale_payload({"event_id": "x", "positions": []})


def test_position_missing_product_id():
    with pytest.raises(SalePayloadError, match="product_id"):
        validate_sale_payload({"event_id": "x", "positions": [{"quantity": 1, "price": 100}]})


def test_position_zero_quantity():
    with pytest.raises(SalePayloadError, match="quantity"):
        validate_sale_payload({"event_id": "x", "positions": [{"product_id": "p1", "quantity": 0, "price": 100}]})


def test_position_negative_price():
    with pytest.raises(SalePayloadError, match="price"):
        validate_sale_payload({"event_id": "x", "positions": [{"product_id": "p1", "quantity": 1, "price": -1}]})


# ---------------------------------------------------------------------------
# A4 — SalePayloadError классифицируется как FAILED
# ---------------------------------------------------------------------------

def test_payload_error_classified_as_failed():
    e = SalePayloadError("bad payload")
    assert classify_error(e) == FAILED


# ---------------------------------------------------------------------------
# A5 — retry на временных сбоях
# ---------------------------------------------------------------------------

def test_timeout_classified_as_retry():
    e = requests.exceptions.Timeout()
    assert classify_error(e) == RETRY


def test_connection_error_classified_as_retry():
    e = requests.exceptions.ConnectionError()
    assert classify_error(e) == RETRY


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