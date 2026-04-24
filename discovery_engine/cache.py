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

    async def ping(self) -> bool:
        """Return True if Redis is reachable."""
        client = self._assert_connected()
        try:
            return bool(await client.ping())
        except Exception as exc:  # noqa: BLE001
            logger.error("Redis PING failed: %s", exc)
            return False

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

    async def get_lots_bulk(self, lot_numbers: list[str]) -> dict[str, dict[str, Any] | None]:
        """
        Fetch multiple lots in a single ``MGET`` round-trip.

        Returns a mapping of lot_number → cached dict (or ``None`` on miss/error).
        """
        if not lot_numbers:
            return {}

        client = self._assert_connected()
        keys = [self._lot_key(ln) for ln in lot_numbers]
        try:
            values = await client.mget(keys)
        except Exception as exc:  # noqa: BLE001
            logger.error("Redis MGET error: %s", exc)
            return {ln: None for ln in lot_numbers}

        result: dict[str, dict[str, Any] | None] = {}
        for lot_number, raw in zip(lot_numbers, values):
            if raw is None:
                logger.debug("Cache MISS for lot %s", lot_number)
                result[lot_number] = None
            else:
                try:
                    result[lot_number] = json.loads(raw)
                    logger.debug("Cache HIT for lot %s", lot_number)
                except json.JSONDecodeError as exc:
                    logger.warning("Failed to decode cached data for lot %s: %s", lot_number, exc)
                    result[lot_number] = None
        return result

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

    async def flush_pattern(self, prefix: str = "copart:lot:") -> int:
        """
        Delete all keys whose names begin with *prefix*.

        Uses ``SCAN`` to avoid blocking Redis on large key sets.
        Returns the number of keys deleted.
        """
        client = self._assert_connected()
        count = 0
        try:
            async for key in client.scan_iter(f"{prefix}*"):
                await client.delete(key)
                count += 1
        except Exception as exc:  # noqa: BLE001
            logger.error("Redis flush_pattern error (prefix=%s): %s", prefix, exc)
        logger.info("flush_pattern: deleted %d key(s) with prefix '%s'", count, prefix)
        return count


class NullCache:
    """
    Drop-in, no-op replacement for ``RedisCache``.

    Used when Redis is unavailable or caching is explicitly disabled
    (``use_cache=False`` on ``DiscoveryEngine``).  All reads return
    ``None`` (cache miss); writes are silently ignored.
    """

    async def connect(self) -> None:
        logger.debug("NullCache: connect() is a no-op")

    async def close(self) -> None:
        logger.debug("NullCache: close() is a no-op")

    async def ping(self) -> bool:
        return True

    async def get_lot(self, lot_number: str) -> None:
        return None

    async def get_lots_bulk(self, lot_numbers: list[str]) -> dict[str, None]:
        return {ln: None for ln in lot_numbers}

    async def set_lot(self, lot_number: str, data: dict[str, Any], ttl: int | None = None) -> None:
        pass

    async def delete_lot(self, lot_number: str) -> None:
        pass

    async def flush_pattern(self, prefix: str = "copart:lot:") -> int:
        return 0
