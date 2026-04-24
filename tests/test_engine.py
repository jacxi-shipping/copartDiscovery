"""Unit tests for discovery_engine.engine (DiscoveryEngine)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from discovery_engine.engine import DiscoveryEngine


def _mock_engine():
    """Return a DiscoveryEngine with mocked internals (no real Redis/HTTP)."""
    engine = DiscoveryEngine.__new__(DiscoveryEngine)
    engine._concurrency = 5
    engine._cache_ttl = 3600

    cache = MagicMock()
    cache.connect = AsyncMock()
    cache.close = AsyncMock()
    cache.get_lot = AsyncMock(return_value=None)
    cache.set_lot = AsyncMock()
    engine._cache = cache

    http = MagicMock()
    http.start = AsyncMock()
    http.close = AsyncMock()
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
            new=AsyncMock(return_value=[record]),
        ):
            results = await engine.request_mode(["12345678"])

        assert len(results) == 1
        assert results[0]["lotNumber"] == "12345678"

    @pytest.mark.asyncio
    async def test_request_mode_empty_lot_list(self):
        engine = _mock_engine()

        with patch(
            "discovery_engine.engine.hydrate_lots",
            new=AsyncMock(return_value=[]),
        ):
            results = await engine.request_mode([])

        assert results == []


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
            patch("discovery_engine.engine.hydrate_lots", new=AsyncMock(return_value=[record1, record2])),
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
