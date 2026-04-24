"""Unit tests for discovery_engine.hydration."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from discovery_engine.hydration import HydrationStats, _parse_lot_detail_response, hydrate_lots


class TestParseLotDetailResponseAlias:
    """Verify the backward-compat alias still works."""

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
    def _make_cache(self, cached_map: dict | None = None):
        """Return a mock cache.  *cached_map* maps lot_number → cached dict."""
        cache = MagicMock()
        cache.get_lots_bulk = AsyncMock(return_value=cached_map or {})
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
        cache = self._make_cache(cached_map={"11111111": cached})
        http = self._make_http({})

        records, stats = await hydrate_lots(["11111111"], http, cache)
        assert len(records) == 1
        assert records[0]["lotDescription"] == "Cached Car"
        http.get_json.assert_not_called()
        assert stats.cache_hits == 1
        assert stats.cache_misses == 0

    @pytest.mark.asyncio
    async def test_fetches_from_api_on_cache_miss(self):
        cache = self._make_cache(cached_map={"22222222": None})
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

        records, stats = await hydrate_lots(["22222222"], http, cache)
        assert len(records) == 1
        assert records[0]["lotNumber"] == "22222222"
        assert records[0]["vin"] == "VIN001"
        http.get_json.assert_called_once()
        cache.set_lot.assert_called_once()
        assert stats.cache_hits == 0
        assert stats.cache_misses == 1
        assert stats.api_failures == 0

    @pytest.mark.asyncio
    async def test_skips_failed_lots(self):
        cache = self._make_cache(cached_map={"33333333": None})
        http = MagicMock()
        http.get_json = AsyncMock(side_effect=Exception("API error"))

        records, stats = await hydrate_lots(["33333333"], http, cache)
        assert records == []
        assert stats.api_failures == 1

    @pytest.mark.asyncio
    async def test_empty_lot_list_returns_empty(self):
        cache = self._make_cache()
        http = self._make_http({})

        records, stats = await hydrate_lots([], http, cache)
        assert records == []
        assert stats.total == 0
        assert stats.elapsed_seconds == 0.0

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

        lot_numbers = [str(i) for i in range(1, 6)]
        cached_map = {ln: make_cached(ln) for ln in lot_numbers}
        cache = self._make_cache(cached_map=cached_map)
        http = self._make_http({})

        records, stats = await hydrate_lots(lot_numbers, http, cache, concurrency=3)
        assert len(records) == 5
        assert stats.cache_hits == 5

    @pytest.mark.asyncio
    async def test_force_refresh_bypasses_cache(self):
        cached = {
            "lotNumber": "44444444",
            "lotDescription": "Stale Car",
            "vin": "",
            "odometer": None,
            "repairCost": None,
            "imagesList": [],
            "fetched_at": "2020-01-01T00:00:00Z",
        }
        # Cache has data, but force_refresh should ignore it.
        cache = self._make_cache(cached_map={"44444444": cached})
        api_response = {
            "data": {
                "lotDetails": {
                    "lotNumber": "44444444",
                    "lotDescription": "Fresh Car",
                    "vin": "FRESH01",
                }
            }
        }
        http = self._make_http(api_response)

        records, stats = await hydrate_lots(["44444444"], http, cache, force_refresh=True)
        assert len(records) == 1
        assert records[0]["lotDescription"] == "Fresh Car"
        http.get_json.assert_called_once()

    @pytest.mark.asyncio
    async def test_stats_success_rate(self):
        cache = self._make_cache(cached_map={"55555555": None, "66666666": None})
        # First lot succeeds; second returns empty detail (fails validation)
        responses = [
            {"data": {"lotDetails": {"lotNumber": "55555555", "lotDescription": "OK"}}},
            {},
        ]
        http = MagicMock()
        http.get_json = AsyncMock(side_effect=responses)

        records, stats = await hydrate_lots(["55555555", "66666666"], http, cache)
        assert len(records) == 1
        assert stats.api_failures == 1
        assert stats.success_rate == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_retry_on_api_failure_then_success(self):
        """A lot that fails on the first attempt succeeds on the second call."""
        cache = self._make_cache(cached_map={"77777777": None})
        good_response = {"data": {"lotDetails": {"lotNumber": "77777777", "lotDescription": "Retry Car"}}}
        http = MagicMock()
        http.get_json = AsyncMock(side_effect=[Exception("transient"), good_response])

        # First pass: API error → failure
        records_first, stats_first = await hydrate_lots(["77777777"], http, cache)
        assert records_first == []
        assert stats_first.api_failures == 1

        # Second pass (simulating retry at caller level): succeeds
        cache2 = self._make_cache(cached_map={"77777777": None})
        http2 = MagicMock()
        http2.get_json = AsyncMock(return_value=good_response)
        records_second, stats_second = await hydrate_lots(["77777777"], http2, cache2)
        assert len(records_second) == 1
