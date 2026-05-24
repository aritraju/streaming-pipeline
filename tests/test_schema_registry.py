"""
tests/test_schema_registry.py
Unit tests for schema validation and evolution detection.
No Kafka or Spark required.
"""

import pytest
import tempfile
import os
from src.schema_registry import SchemaRegistry


@pytest.fixture
def registry(tmp_path):
    db_path = str(tmp_path / "test_registry.db")
    return SchemaRegistry(db_path=db_path)


def make_valid_event():
    return {
        "event_id": "EVT-00000001",
        "schema_version": "v1",
        "symbol": "AAPL",
        "price": 189.50,
        "volume": 1000,
        "bid": 189.48,
        "ask": 189.52,
        "exchange": "NASDAQ",
        "event_timestamp": "2024-01-01T10:00:00+00:00",
        "producer_id": "test-producer"
    }


def test_valid_event_passes(registry):
    event = make_valid_event()
    is_valid, errors = registry.validate(event)
    assert is_valid
    assert errors == []


def test_null_symbol_fails(registry):
    event = make_valid_event()
    event["symbol"] = None
    is_valid, errors = registry.validate(event)
    assert not is_valid
    assert any("symbol" in e for e in errors)


def test_non_numeric_price_fails(registry):
    event = make_valid_event()
    event["price"] = "not-a-price"
    is_valid, errors = registry.validate(event)
    assert not is_valid
    assert any("price" in e for e in errors)


def test_missing_event_id_fails(registry):
    event = make_valid_event()
    del event["event_id"]
    is_valid, errors = registry.validate(event)
    assert not is_valid


def test_detect_new_field(registry):
    event = make_valid_event()
    event["analyst_rating"] = "BUY"
    changes = registry.detect_evolution(event)
    new_fields = [c for c in changes if c["type"] == "NEW_FIELD"]
    assert any(c["field"] == "analyst_rating" for c in new_fields)


def test_no_evolution_for_baseline(registry):
    event = make_valid_event()
    changes = registry.detect_evolution(event)
    assert changes == []


def test_evolution_log_persists(registry):
    event = make_valid_event()
    event["new_mystery_field"] = "surprise"
    changes = registry.detect_evolution(event)
    registry.log_evolution(event["event_id"], changes)
    log = registry.get_evolution_log()
    assert len(log) > 0
    assert any(r["field_name"] == "new_mystery_field" for r in log)


def test_schema_versions_tracked(registry):
    versions = registry.get_versions()
    assert any(v["version"] == "v1" for v in versions)
