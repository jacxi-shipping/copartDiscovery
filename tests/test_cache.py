"""Unit tests for discovery_engine.cache (RedisCache and NullCache)."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from discovery_engine.cache import NullCache, RedisCache


@pytest.fixture
def cache():
    return RedisCache(redis_url="redis://localhost:6379", ttl=3600)


@pytest.fixture
def mock_redis_client():
    """Return a mock that mimics a redis.asyncio.Redis instance."""
    client = MagicMock()
    client.get = AsyncMock()
    client.mget = AsyncMock()
    client.set = AsyncMock()
    client.delete = AsyncMock()
    client.close = AsyncMock()
    client.ping = AsyncMock(return_value=True)
    client.scan_iter = MagicMock()
    return client


class TestRedisCacheGetLot:
    @pytest.mark.asyncio
    async def test_cache_hit(self, cache, mock_redis_client):
        data = {"lotNumber": "12345", "lotDescription": "Test Car"}
        mock_redis_client.get.return_value = json.dumps(data)
        cache._client = mock_redis_client

        result = await cache.get_lot("12345")
        assert result == data
        mock_redis_client.get.assert_awaited_once_with("copart:lot:12345")

    @pytest.mark.asyncio
    async def test_cache_miss(self, cache, mock_redis_client):
        mock_redis_client.get.return_value = None
        cache._client = mock_redis_client

        result = await cache.get_lot("99999")
        assert result is None

    @pytest.mark.asyncio
    async def test_invalid_json_returns_none(self, cache, mock_redis_client):
        mock_redis_client.get.return_value = "not-valid-json{"
        cache._client = mock_redis_client

        result = await cache.get_lot("12345")
        assert result is None

    @pytest.mark.asyncio
    async def test_redis_error_returns_none(self, cache, mock_redis_client):
        mock_redis_client.get.side_effect = Exception("connection refused")
        cache._client = mock_redis_client

        result = await cache.get_lot("12345")
        assert result is None

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, cache):
        with pytest.raises(RuntimeError, match="not connected"):
            await cache.get_lot("12345")


class TestRedisCacheGetLotsBulk:
    @pytest.mark.asyncio
    async def test_bulk_hit_and_miss(self, cache, mock_redis_client):
        data = {"lotNumber": "111", "lotDescription": "Car A"}
        mock_redis_client.mget.return_value = [json.dumps(data), None]
        cache._client = mock_redis_client

        result = await cache.get_lots_bulk(["111", "222"])
        assert result["111"] == data
        assert result["222"] is None

    @pytest.mark.asyncio
    async def test_bulk_invalid_json_treated_as_miss(self, cache, mock_redis_client):
        mock_redis_client.mget.return_value = ["bad-json{"]
        cache._client = mock_redis_client

        result = await cache.get_lots_bulk(["111"])
        assert result["111"] is None

    @pytest.mark.asyncio
    async def test_bulk_redis_error_returns_all_none(self, cache, mock_redis_client):
        mock_redis_client.mget.side_effect = Exception("network error")
        cache._client = mock_redis_client

        result = await cache.get_lots_bulk(["111", "222"])
        assert result == {"111": None, "222": None}

    @pytest.mark.asyncio
    async def test_empty_list_returns_empty_dict(self, cache, mock_redis_client):
        cache._client = mock_redis_client
        result = await cache.get_lots_bulk([])
        assert result == {}
        mock_redis_client.mget.assert_not_called()


class TestRedisCacheSetLot:
    @pytest.mark.asyncio
    async def test_set_uses_default_ttl(self, cache, mock_redis_client):
        cache._client = mock_redis_client
        data = {"lotNumber": "12345"}

        await cache.set_lot("12345", data)
        mock_redis_client.set.assert_awaited_once_with(
            "copart:lot:12345", json.dumps(data), ex=3600
        )

    @pytest.mark.asyncio
    async def test_set_uses_custom_ttl(self, cache, mock_redis_client):
        cache._client = mock_redis_client
        data = {"lotNumber": "12345"}

        await cache.set_lot("12345", data, ttl=7200)
        mock_redis_client.set.assert_awaited_once_with(
            "copart:lot:12345", json.dumps(data), ex=7200
        )

    @pytest.mark.asyncio
    async def test_set_redis_error_does_not_raise(self, cache, mock_redis_client):
        mock_redis_client.set.side_effect = Exception("write error")
        cache._client = mock_redis_client

        # Should not raise; errors are logged
        await cache.set_lot("12345", {"lotNumber": "12345"})


class TestRedisCacheDeleteLot:
    @pytest.mark.asyncio
    async def test_delete(self, cache, mock_redis_client):
        cache._client = mock_redis_client
        await cache.delete_lot("12345")
        mock_redis_client.delete.assert_awaited_once_with("copart:lot:12345")


class TestRedisCacheFlushPattern:
    @pytest.mark.asyncio
    async def test_flush_deletes_matching_keys(self, cache, mock_redis_client):
        async def _fake_scan_iter(pattern):
            for key in ["copart:lot:111", "copart:lot:222"]:
                yield key

        mock_redis_client.scan_iter = _fake_scan_iter
        mock_redis_client.delete = AsyncMock(return_value=1)
        cache._client = mock_redis_client

        count = await cache.flush_pattern()
        assert count == 2

    @pytest.mark.asyncio
    async def test_flush_empty_keyspace_returns_zero(self, cache, mock_redis_client):
        async def _empty_scan_iter(pattern):
            return
            yield  # noqa: unreachable

        mock_redis_client.scan_iter = _empty_scan_iter
        cache._client = mock_redis_client

        count = await cache.flush_pattern()
        assert count == 0


class TestRedisCachePing:
    @pytest.mark.asyncio
    async def test_ping_ok(self, cache, mock_redis_client):
        mock_redis_client.ping.return_value = True
        cache._client = mock_redis_client
        assert await cache.ping() is True

    @pytest.mark.asyncio
    async def test_ping_error_returns_false(self, cache, mock_redis_client):
        mock_redis_client.ping.side_effect = Exception("unreachable")
        cache._client = mock_redis_client
        assert await cache.ping() is False


class TestNullCache:
    @pytest.mark.asyncio
    async def test_connect_and_close_are_noop(self):
        nc = NullCache()
        await nc.connect()
        await nc.close()  # no exceptions

    @pytest.mark.asyncio
    async def test_get_lot_always_miss(self):
        nc = NullCache()
        assert await nc.get_lot("12345") is None

    @pytest.mark.asyncio
    async def test_get_lots_bulk_all_miss(self):
        nc = NullCache()
        result = await nc.get_lots_bulk(["111", "222"])
        assert result == {"111": None, "222": None}

    @pytest.mark.asyncio
    async def test_set_lot_is_noop(self):
        nc = NullCache()
        await nc.set_lot("12345", {"lotNumber": "12345"})  # no exception

    @pytest.mark.asyncio
    async def test_delete_lot_is_noop(self):
        nc = NullCache()
        await nc.delete_lot("12345")  # no exception

    @pytest.mark.asyncio
    async def test_flush_pattern_returns_zero(self):
        nc = NullCache()
        assert await nc.flush_pattern() == 0

    @pytest.mark.asyncio
    async def test_ping_returns_true(self):
        nc = NullCache()
        assert await nc.ping() is True
