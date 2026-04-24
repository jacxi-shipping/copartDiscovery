"""
Integration tests for the full request/bulk-mode flow.

Uses:
* ``fakeredis`` — in-process async Redis without a running server
* ``pytest-httpx`` — intercepts real httpx requests
"""

from __future__ import annotations

import json

import pytest
import pytest_httpx
from fakeredis.aioredis import FakeRedis

from discovery_engine.cache import RedisCache
from discovery_engine.client import HttpClient
from discovery_engine.config import LOT_DETAILS_URL, SEARCH_URL
from discovery_engine.hydration import hydrate_lots


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lot_detail_response(lot_number: str, description: str = "Test Car") -> dict:
    return {
        "data": {
            "lotDetails": {
                "lotNumber": lot_number,
                "lotDescription": description,
                "vin": f"VIN{lot_number}",
                "odometer": 10000,
                "repairCost": 500,
                "imagesList": [f"https://img.example.com/{lot_number}.jpg"],
            }
        }
    }


def _search_response(lot_numbers: list[str]) -> dict:
    return {
        "data": {
            "results": {
                "content": [{"lotNumber": ln} for ln in lot_numbers]
            }
        }
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
async def fake_cache():
    """A RedisCache backed by fakeredis (no real Redis server needed)."""
    cache = RedisCache(redis_url="redis://localhost:6379", ttl=3600)
    cache._client = FakeRedis(decode_responses=True)
    yield cache
    await cache._client.aclose()


@pytest.fixture
async def http_client(httpx_mock: pytest_httpx.HTTPXMock):
    """A started HttpClient whose requests are intercepted by pytest-httpx."""
    client = HttpClient(max_retries=1)
    await client.start()
    yield client, httpx_mock
    await client.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHydrateLotsFull:
    @pytest.mark.asyncio
    async def test_cache_miss_fetches_from_api_and_stores(
        self, fake_cache, httpx_mock: pytest_httpx.HTTPXMock
    ):
        lot_number = "12345678"
        httpx_mock.add_response(
            url=LOT_DETAILS_URL.format(lot_number=lot_number),
            json=_lot_detail_response(lot_number, "Integration Car"),
        )

        client = HttpClient(max_retries=1)
        await client.start()
        try:
            records, stats = await hydrate_lots([lot_number], client, fake_cache)
        finally:
            await client.close()

        assert len(records) == 1
        assert records[0]["lotNumber"] == lot_number
        assert records[0]["lotDescription"] == "Integration Car"
        assert stats.cache_misses == 1
        assert stats.cache_hits == 0

        # Verify the record was stored in fakeredis
        stored = await fake_cache.get_lot(lot_number)
        assert stored is not None
        assert stored["lotNumber"] == lot_number

    @pytest.mark.asyncio
    async def test_cache_hit_skips_api(
        self, fake_cache, httpx_mock: pytest_httpx.HTTPXMock
    ):
        lot_number = "87654321"
        cached_record = {
            "lotNumber": lot_number,
            "lotDescription": "Cached Car",
            "vin": "CACHEDVIN",
            "odometer": 5000.0,
            "repairCost": 200.0,
            "imagesList": [],
            "fetched_at": "2026-01-01T00:00:00Z",
        }
        # Pre-populate the cache
        await fake_cache.set_lot(lot_number, cached_record)

        client = HttpClient(max_retries=1)
        await client.start()
        try:
            records, stats = await hydrate_lots([lot_number], client, fake_cache)
        finally:
            await client.close()

        assert len(records) == 1
        assert records[0]["lotDescription"] == "Cached Car"
        assert stats.cache_hits == 1
        # No HTTP requests should have been made
        assert len(httpx_mock.get_requests()) == 0

    @pytest.mark.asyncio
    async def test_mixed_cache_hit_and_miss(
        self, fake_cache, httpx_mock: pytest_httpx.HTTPXMock
    ):
        hit_lot = "11111111"
        miss_lot = "22222222"

        # Pre-populate only the hit lot
        await fake_cache.set_lot(hit_lot, {
            "lotNumber": hit_lot, "lotDescription": "Hit Car",
            "vin": "", "odometer": None, "repairCost": None,
            "imagesList": [], "fetched_at": "2026-01-01T00:00:00Z",
        })

        httpx_mock.add_response(
            url=LOT_DETAILS_URL.format(lot_number=miss_lot),
            json=_lot_detail_response(miss_lot, "Miss Car"),
        )

        client = HttpClient(max_retries=1)
        await client.start()
        try:
            records, stats = await hydrate_lots([hit_lot, miss_lot], client, fake_cache)
        finally:
            await client.close()

        assert len(records) == 2
        descriptions = {r["lotNumber"]: r["lotDescription"] for r in records}
        assert descriptions[hit_lot] == "Hit Car"
        assert descriptions[miss_lot] == "Miss Car"
        assert stats.cache_hits == 1
        assert stats.cache_misses == 1

    @pytest.mark.asyncio
    async def test_force_refresh_bypasses_populated_cache(
        self, fake_cache, httpx_mock: pytest_httpx.HTTPXMock
    ):
        lot_number = "33333333"
        # Pre-populate stale data
        await fake_cache.set_lot(lot_number, {
            "lotNumber": lot_number, "lotDescription": "Stale",
            "vin": "", "odometer": None, "repairCost": None,
            "imagesList": [], "fetched_at": "2020-01-01T00:00:00Z",
        })
        httpx_mock.add_response(
            url=LOT_DETAILS_URL.format(lot_number=lot_number),
            json=_lot_detail_response(lot_number, "Fresh Car"),
        )

        client = HttpClient(max_retries=1)
        await client.start()
        try:
            records, stats = await hydrate_lots(
                [lot_number], client, fake_cache, force_refresh=True
            )
        finally:
            await client.close()

        assert records[0]["lotDescription"] == "Fresh Car"
        assert len(httpx_mock.get_requests()) == 1

    @pytest.mark.asyncio
    async def test_mget_single_round_trip(
        self, fake_cache, httpx_mock: pytest_httpx.HTTPXMock
    ):
        """Bulk-cache check uses MGET: only one round-trip for N cache-hit lots."""
        lot_numbers = [str(i) for i in range(1, 6)]
        for ln in lot_numbers:
            await fake_cache.set_lot(ln, {
                "lotNumber": ln, "lotDescription": f"Car {ln}",
                "vin": "", "odometer": None, "repairCost": None,
                "imagesList": [], "fetched_at": "2026-01-01T00:00:00Z",
            })

        client = HttpClient(max_retries=1)
        await client.start()
        try:
            records, stats = await hydrate_lots(lot_numbers, client, fake_cache)
        finally:
            await client.close()

        assert len(records) == 5
        assert stats.cache_hits == 5
        # All served from cache: no HTTP requests
        assert len(httpx_mock.get_requests()) == 0
