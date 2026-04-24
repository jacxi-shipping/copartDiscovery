"""Unit tests for discovery_engine.engine (DiscoveryEngine)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from discovery_engine.engine import DiscoveryEngine
from discovery_engine.hydration import HydrationStats


def _make_stats(**kwargs) -> HydrationStats:
    defaults = dict(total=1, cache_hits=0, cache_misses=1, api_failures=0, elapsed_seconds=0.01)
    return HydrationStats(**{**defaults, **kwargs})


def _mock_engine():
    """Return a DiscoveryEngine with mocked internals (no real Redis/HTTP)."""
    engine = DiscoveryEngine.__new__(DiscoveryEngine)
    engine._concurrency = 5
    engine._cache_ttl = 3600
    engine._last_stats = None

    cache = MagicMock()
    cache.connect = AsyncMock()
    cache.close = AsyncMock()
    cache.get_lots_bulk = AsyncMock(return_value={})
    cache.set_lot = AsyncMock()
    cache.ping = AsyncMock(return_value=True)
    engine._cache = cache

    http = MagicMock()
    http.start = AsyncMock()
    http.close = AsyncMock()
    http.post_json = AsyncMock(return_value={})
    engine._http = http

    return engine


class TestDiscoveryEngineRequestMode:
    @pytest.mark.asyncio
    async def test_request_mode_returns_records(self):
        engine = _mock_engine()
        record = {
            "lotNumber": "12345678",
            "lotDescription": "Test Car",
            "vin": "V001",
            "odometer": 10000.0,
            "repairCost": 500.0,
            "imagesList": [],
            "fetched_at": "2026-04-24T12:00:00Z",
        }

        with patch(
            "discovery_engine.engine.hydrate_lots",
            new=AsyncMock(return_value=([record], _make_stats(total=1))),
        ):
            results = await engine.request_mode(["12345678"])

        assert len(results) == 1
        assert results[0]["lotNumber"] == "12345678"

    @pytest.mark.asyncio
    async def test_request_mode_empty_lot_list(self):
        engine = _mock_engine()

        with patch(
            "discovery_engine.engine.hydrate_lots",
            new=AsyncMock(return_value=([], _make_stats(total=0))),
        ):
            results = await engine.request_mode([])

        assert results == []

    @pytest.mark.asyncio
    async def test_request_mode_stores_last_stats(self):
        engine = _mock_engine()
        stats = _make_stats(total=2, cache_hits=1, cache_misses=1)

        with patch(
            "discovery_engine.engine.hydrate_lots",
            new=AsyncMock(return_value=([{"lotNumber": "1"}, {"lotNumber": "2"}], stats)),
        ):
            await engine.request_mode(["1", "2"])

        assert engine.last_stats is stats

    @pytest.mark.asyncio
    async def test_request_mode_force_refresh_passed_through(self):
        engine = _mock_engine()

        with patch(
            "discovery_engine.engine.hydrate_lots",
            new=AsyncMock(return_value=([], _make_stats(total=0))),
        ) as mock_hydrate:
            await engine.request_mode(["99"], force_refresh=True)
            _, kwargs = mock_hydrate.call_args
            assert kwargs.get("force_refresh") is True


class TestDiscoveryEngineBulkMode:
    @pytest.mark.asyncio
    async def test_bulk_mode_returns_records(self):
        engine = _mock_engine()

        async def _fake_search(*args, **kwargs):
            for lot in [{"lotNumber": "11111111"}, {"lotNumber": "22222222"}]:
                yield lot

        record1 = {
            "lotNumber": "11111111", "lotDescription": "Car A",
            "vin": "", "odometer": None, "repairCost": None,
            "imagesList": [], "fetched_at": "2026-04-24T12:00:00Z",
        }
        record2 = {
            "lotNumber": "22222222", "lotDescription": "Car B",
            "vin": "", "odometer": None, "repairCost": None,
            "imagesList": [], "fetched_at": "2026-04-24T12:00:00Z",
        }

        with (
            patch("discovery_engine.engine.search_lots_bulk", side_effect=_fake_search),
            patch(
                "discovery_engine.engine.hydrate_lots",
                new=AsyncMock(return_value=([record1, record2], _make_stats(total=2))),
            ),
        ):
            results = await engine.bulk_mode(filters={"make": ["TOYOTA"]})

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_bulk_mode_empty_search(self):
        engine = _mock_engine()

        async def _empty_search(*args, **kwargs):
            return
            yield  # make it a generator

        with patch("discovery_engine.engine.search_lots_bulk", side_effect=_empty_search):
            results = await engine.bulk_mode()

        assert results == []

    @pytest.mark.asyncio
    async def test_bulk_mode_sort_passed_through(self):
        engine = _mock_engine()

        async def _fake_search(*args, **kwargs):
            assert kwargs.get("sort") == {"column": "repairCost", "order": "asc"}
            yield {"lotNumber": "11111111"}

        with (
            patch("discovery_engine.engine.search_lots_bulk", side_effect=_fake_search),
            patch(
                "discovery_engine.engine.hydrate_lots",
                new=AsyncMock(return_value=([], _make_stats(total=0))),
            ),
        ):
            await engine.bulk_mode(sort={"column": "repairCost", "order": "asc"})


class TestDiscoveryEngineHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_all_ok(self):
        engine = _mock_engine()
        engine._cache.ping = AsyncMock(return_value=True)
        engine._http.post_json = AsyncMock(return_value={})

        status = await engine.health_check()
        assert status["redis"] == "ok"
        assert status["api"] == "ok"

    @pytest.mark.asyncio
    async def test_health_check_redis_error(self):
        engine = _mock_engine()
        engine._cache.ping = AsyncMock(side_effect=Exception("connection refused"))
        engine._http.post_json = AsyncMock(return_value={})

        status = await engine.health_check()
        assert "error" in status["redis"]

    @pytest.mark.asyncio
    async def test_health_check_api_error(self):
        engine = _mock_engine()
        engine._cache.ping = AsyncMock(return_value=True)
        engine._http.post_json = AsyncMock(side_effect=Exception("timeout"))

        status = await engine.health_check()
        assert status["redis"] == "ok"
        assert "error" in status["api"]


class TestDiscoveryEngineNoCacheMode:
    def test_no_cache_uses_null_cache(self):
        from discovery_engine.cache import NullCache

        engine = DiscoveryEngine(use_cache=False)
        assert isinstance(engine._cache, NullCache)


class TestDiscoveryEngineSaveJson:
    def test_save_json(self, tmp_path):
        records = [{"lotNumber": "12345678", "fetched_at": "2026-04-24T12:00:00Z"}]
        path = str(tmp_path / "output.json")
        DiscoveryEngine.save_json(records, path)

        import json
        with open(path) as f:
            loaded = json.load(f)
        assert loaded == records


class TestDiscoveryEngineContextManager:
    @pytest.mark.asyncio
    async def test_context_manager_starts_and_shuts_down(self):
        engine = _mock_engine()

        async with engine:
            engine._cache.connect.assert_called_once()
            engine._http.start.assert_called_once()

        engine._http.close.assert_called_once()
        engine._cache.close.assert_called_once()
