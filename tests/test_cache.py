"""Unit tests for discovery_engine.cache (RedisCache)."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from discovery_engine.cache import RedisCache


@pytest.fixture
def cache():
    return RedisCache(redis_url="redis://localhost:6379", ttl=3600)


@pytest.fixture
def mock_redis_client():
    """Return a mock that mimics an aioredis.Redis instance."""
    client = MagicMock()
    client.get = AsyncMock()
    client.set = AsyncMock()
    client.delete = AsyncMock()
    client.close = AsyncMock()
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
