"""Unit tests for discovery_engine.hydration."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from discovery_engine.hydration import _parse_lot_detail_response, hydrate_lots


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
        detail = _parse_lot_detail_response(response)
        assert detail["lotNumber"] == "12345678"

    def test_flat_response(self):
        response = {"lotNumber": "87654321", "vin": "XYZ456"}
        detail = _parse_lot_detail_response(response)
        assert detail["lotNumber"] == "87654321"

    def test_empty_response_returns_empty_dict(self):
        detail = _parse_lot_detail_response({})
        assert detail == {}

    def test_malformed_response_returns_empty_dict(self):
        detail = _parse_lot_detail_response({"data": "bad"})
        assert detail == {}


class TestHydrateLots:
    def _make_cache(self, cached_value=None):
        cache = MagicMock()
        cache.get_lot = AsyncMock(return_value=cached_value)
        cache.set_lot = AsyncMock()
        return cache

    def _make_http(self, response):
        http = MagicMock()
        http.get_json = AsyncMock(return_value=response)
        return http

    @pytest.mark.asyncio
    async def test_returns_cached_data_without_api_call(self):
        cached = {
            "lotNumber": "11111111",
            "lotDescription": "Cached Car",
            "vin": "",
            "odometer": None,
            "repairCost": None,
            "imagesList": [],
            "fetched_at": "2026-01-01T00:00:00Z",
        }
        cache = self._make_cache(cached_value=cached)
        http = self._make_http({})

        records = await hydrate_lots(["11111111"], http, cache)
        assert len(records) == 1
        assert records[0]["lotDescription"] == "Cached Car"
        http.get_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetches_from_api_on_cache_miss(self):
        cache = self._make_cache(cached_value=None)
        api_response = {
            "data": {
                "lotDetails": {
                    "lotNumber": "22222222",
                    "lotDescription": "API Car",
                    "vin": "VIN001",
                    "odometer": 50000,
                    "repairCost": 800,
                    "imagesList": ["https://img.com/1.jpg"],
                }
            }
        }
        http = self._make_http(api_response)

        records = await hydrate_lots(["22222222"], http, cache)
        assert len(records) == 1
        assert records[0]["lotNumber"] == "22222222"
        assert records[0]["vin"] == "VIN001"
        http.get_json.assert_called_once()
        cache.set_lot.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_failed_lots(self):
        cache = self._make_cache(cached_value=None)
        http = MagicMock()
        http.get_json = AsyncMock(side_effect=Exception("API error"))

        records = await hydrate_lots(["33333333"], http, cache)
        assert records == []

    @pytest.mark.asyncio
    async def test_empty_lot_list_returns_empty(self):
        cache = self._make_cache()
        http = self._make_http({})

        records = await hydrate_lots([], http, cache)
        assert records == []

    @pytest.mark.asyncio
    async def test_multiple_lots_concurrent(self):
        def make_cached(lot_number):
            return {
                "lotNumber": lot_number,
                "lotDescription": f"Car {lot_number}",
                "vin": "",
                "odometer": None,
                "repairCost": None,
                "imagesList": [],
                "fetched_at": "2026-01-01T00:00:00Z",
            }

        async def get_lot_side_effect(ln):
            return make_cached(ln)

        cache = MagicMock()
        cache.get_lot = AsyncMock(side_effect=get_lot_side_effect)
        cache.set_lot = AsyncMock()
        http = self._make_http({})

        lot_numbers = [str(i) for i in range(1, 6)]
        records = await hydrate_lots(lot_numbers, http, cache, concurrency=3)
        assert len(records) == 5
