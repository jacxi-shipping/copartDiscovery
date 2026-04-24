"""Async Redis cache wrapper using redis-py (async)."""

from __future__ import annotations

import json
import logging
from typing import Any

from redis.asyncio import Redis

from .config import DEFAULT_CACHE_TTL, REDIS_URL

logger = logging.getLogger(__name__)


class RedisCache:
    """Thin async wrapper around redis-py (async) for lot-detail caching."""

    def __init__(self, redis_url: str = REDIS_URL, ttl: int = DEFAULT_CACHE_TTL) -> None:
        self._url = redis_url
        self._ttl = ttl
        self._client: Redis | None = None

    async def connect(self) -> None:
        """Open a connection pool to Redis."""
        self._client = Redis.from_url(
            self._url,
            encoding="utf-8",
            decode_responses=True,
        )
        logger.info("Redis connected: %s", self._url)

    async def close(self) -> None:
        """Close the connection pool."""
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("Redis connection closed")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _lot_key(self, lot_number: str) -> str:
        return f"copart:lot:{lot_number}"

    def _assert_connected(self) -> Redis:
        if self._client is None:
            raise RuntimeError("RedisCache is not connected. Call connect() first.")
        return self._client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_lot(self, lot_number: str) -> dict[str, Any] | None:
        """
        Return cached lot data, or ``None`` on cache-miss / error.
        Logs cache hit/miss at DEBUG level.
        """
        client = self._assert_connected()
        key = self._lot_key(lot_number)
        try:
            raw = await client.get(key)
        except Exception as exc:  # noqa: BLE001
            logger.error("Redis GET error for key %s: %s", key, exc)
            return None

        if raw is None:
            logger.debug("Cache MISS for lot %s", lot_number)
            return None

        try:
            data = json.loads(raw)
            logger.debug("Cache HIT for lot %s", lot_number)
            return data
        except json.JSONDecodeError as exc:
            logger.warning("Failed to decode cached data for lot %s: %s", lot_number, exc)
            return None

    async def set_lot(self, lot_number: str, data: dict[str, Any], ttl: int | None = None) -> None:
        """
        Store lot data in Redis with the configured TTL.
        A custom *ttl* (seconds) overrides the instance default.
        """
        client = self._assert_connected()
        key = self._lot_key(lot_number)
        effective_ttl = ttl if ttl is not None else self._ttl
        try:
            await client.set(key, json.dumps(data), ex=effective_ttl)
            logger.debug("Cached lot %s (TTL=%ds)", lot_number, effective_ttl)
        except Exception as exc:  # noqa: BLE001
            logger.error("Redis SET error for key %s: %s", key, exc)

    async def delete_lot(self, lot_number: str) -> None:
        """Evict a specific lot from the cache."""
        client = self._assert_connected()
        await client.delete(self._lot_key(lot_number))
        logger.debug("Evicted lot %s from cache", lot_number)
