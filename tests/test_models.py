"""Unit tests for discovery_engine.models."""

import pytest

from discovery_engine.models import (
    LotRecord,
    build_lot_record,
    build_lot_record_from_search_hit,
    parse_lot_detail_response,
    validate_search_payload,
)


class TestBuildLotRecord:
    def test_valid_full_record(self):
        raw = {
            "lotNumber": "12345678",
            "lotDescription": "2021 Toyota Camry",
            "vin": "JT1234567890123456",
            "odometer": 15000,
            "repairCost": 1200,
            "imagesList": ["https://example.com/image1.jpg", "https://example.com/image2.jpg"],
        }
        record = build_lot_record(raw, fetched_at="2026-04-24T12:00:00Z")
        assert record is not None
        assert isinstance(record, LotRecord)
        assert record.lotNumber == "12345678"
        assert record.lotDescription == "2021 Toyota Camry"
        assert record.vin == "JT1234567890123456"
        assert record.odometer == 15000.0
        assert record.repairCost == 1200.0
        assert len(record.imagesList) == 2
        assert record.fetched_at == "2026-04-24T12:00:00Z"

    def test_to_dict_is_json_serialisable(self):
        raw = {"lotNumber": "12345678", "odometer": 1000}
        record = build_lot_record(raw, fetched_at="2026-04-24T12:00:00Z")
        assert record is not None
        d = record.to_dict()
        assert isinstance(d, dict)
        assert d["lotNumber"] == "12345678"

    def test_missing_lot_number_returns_none(self):
        raw = {"lotDescription": "2021 Toyota Camry"}
        assert build_lot_record(raw) is None

    def test_non_dict_returns_none(self):
        assert build_lot_record([]) is None  # type: ignore[arg-type]
        assert build_lot_record(None) is None  # type: ignore[arg-type]

    def test_optional_fields_default_gracefully(self):
        raw = {"lotNumber": "99999999"}
        record = build_lot_record(raw)
        assert record is not None
        assert record.lotDescription == ""
        assert record.vin == ""
        assert record.odometer is None
        assert record.repairCost is None
        assert record.imagesList == []

    def test_images_filters_non_strings(self):
        raw = {"lotNumber": "11111111", "imagesList": ["https://a.com/img.jpg", 42, None, "https://b.com/img.jpg"]}
        record = build_lot_record(raw)
        assert record is not None
        assert record.imagesList == ["https://a.com/img.jpg", "https://b.com/img.jpg"]

    def test_numeric_lot_number_coerced(self):
        raw = {"lotNumber": 55555555}
        record = build_lot_record(raw)
        assert record is not None
        assert record.lotNumber == "55555555"

    def test_odometer_string_coerced(self):
        raw = {"lotNumber": "22222222", "odometer": "30000"}
        record = build_lot_record(raw)
        assert record is not None
        assert record.odometer == 30000.0

    def test_odometer_invalid_returns_none(self):
        raw = {"lotNumber": "33333333", "odometer": "not-a-number"}
        record = build_lot_record(raw)
        assert record is not None
        assert record.odometer is None

    def test_fetched_at_auto_populated(self):
        raw = {"lotNumber": "44444444"}
        record = build_lot_record(raw)
        assert record is not None
        assert "fetched_at" in record.to_dict()
        assert "T" in record.fetched_at

    def test_alternative_field_names(self):
        raw = {
            "lot_number": "77777777",
            "lot_description": "2020 Ford F150",
            "repair_cost": 500,
        }
        record = build_lot_record(raw)
        assert record is not None
        assert record.lotNumber == "77777777"
        assert record.lotDescription == "2020 Ford F150"
        assert record.repairCost == 500.0


class TestParseLotDetailResponse:
    def test_nested_lot_details(self):
        response = {
            "data": {
                "lotDetails": {
                    "lotNumber": "12345678",
                    "vin": "ABC123",
                }
            }
        }
        detail = parse_lot_detail_response(response)
        assert detail["lotNumber"] == "12345678"

    def test_flat_response(self):
        response = {"lotNumber": "87654321", "vin": "XYZ456"}
        detail = parse_lot_detail_response(response)
        assert detail["lotNumber"] == "87654321"

    def test_empty_response_returns_empty_dict(self):
        assert parse_lot_detail_response({}) == {}

    def test_malformed_response_returns_empty_dict(self):
        assert parse_lot_detail_response({"data": "bad"}) == {}


class TestBuildLotRecordFromSearchHit:
    def test_builds_partial_record_from_search_hit(self):
        hit = {
            "ln": 12345678,
            "ld": "2020 TOYOTA CAMRY",
            "thb": "https://img.example.com/123.jpg",
            "odometer": 50000,
        }
        record = build_lot_record_from_search_hit(hit, fetched_at="2026-04-27T00:00:00Z")

        assert record is not None
        assert record.lotNumber == "12345678"
        assert record.lotDescription == "2020 TOYOTA CAMRY"
        assert record.vin == ""
        assert record.odometer == 50000.0
        assert record.imagesList == ["https://img.example.com/123.jpg"]

    def test_requires_lot_number(self):
        assert build_lot_record_from_search_hit({"ld": "No lot"}) is None


class TestValidateSearchPayload:
    def test_valid_payload(self):
        payload = {"query": "*", "filter": {}}
        assert validate_search_payload(payload) is True

    def test_missing_query(self):
        payload = {"filter": {}}
        assert validate_search_payload(payload) is False

    def test_non_dict(self):
        assert validate_search_payload([]) is False  # type: ignore[arg-type]
        assert validate_search_payload(None) is False  # type: ignore[arg-type]
