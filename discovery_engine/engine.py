"""
DiscoveryEngine — orchestrates Request Mode and Bulk Mode.

Request Mode
------------
Fetches and hydrates a specific list of lot numbers on demand.
Flow: Redis → API (if miss) → Redis (store) → return records

Bulk Mode
---------
Runs a paginated search, then hydrates only cache-missing lots.
Flow: search → Redis check → API (if miss) → Redis store → return records
"""

from __future__ import annotations

import json
import logging
from typing import Any

from .cache import RedisCache
from .client import HttpClient
from .config import (
    BULK_MAX_RESULTS,
    DEFAULT_CACHE_TTL,
    DEFAULT_CONCURRENCY,
    DEFAULT_PAGE_SIZE,
    REDIS_URL,
)
from .hydration import hydrate_lots
from .search import search_lots_bulk

logger = logging.getLogger(__name__)


class DiscoveryEngine:
    """
    High-level entry point for vehicle lot discovery.

    Parameters
    ----------
    redis_url:
        Connection string for Redis.
    cache_ttl:
        Default cache TTL in seconds (default 24 hours).
    concurrency:
        Max simultaneous hydration requests.
    http_headers:
        Extra headers to merge into all HTTP requests.
    """

    def __init__(
        self,
        redis_url: str = REDIS_URL,
        cache_ttl: int = DEFAULT_CACHE_TTL,
        concurrency: int = DEFAULT_CONCURRENCY,
        http_headers: dict[str, str] | None = None,
    ) -> None:
        self._cache = RedisCache(redis_url=redis_url, ttl=cache_ttl)
        self._http = HttpClient(headers=http_headers)
        self._concurrency = concurrency
        self._cache_ttl = cache_ttl

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------

    async def _startup(self) -> None:
        await self._cache.connect()
        await self._http.start()

    async def _shutdown(self) -> None:
        await self._http.close()
        await self._cache.close()

    # ------------------------------------------------------------------
    # Context-manager support (async with DiscoveryEngine() as eng: ...)
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "DiscoveryEngine":
        await self._startup()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self._shutdown()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def request_mode(
        self,
        lot_numbers: list[str],
        *,
        ttl: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        **Request Mode** — hydrate specific lot numbers.

        Parameters
        ----------
        lot_numbers:
            Lot numbers to fetch/hydrate.
        ttl:
            Optional TTL override (seconds).

        Returns
        -------
        list[dict]
            Flattened JSON-compatible records.
        """
        logger.info("Request Mode: hydrating %d lot(s)", len(lot_numbers))
        records = await hydrate_lots(
            lot_numbers,
            self._http,
            self._cache,
            concurrency=self._concurrency,
            ttl=ttl if ttl is not None else self._cache_ttl,
        )
        logger.info("Request Mode: returned %d record(s)", len(records))
        return records

    async def bulk_mode(
        self,
        *,
        filters: dict[str, Any] | None = None,
        sort: dict[str, str] | None = None,
        max_results: int = BULK_MAX_RESULTS,
        page_size: int = DEFAULT_PAGE_SIZE,
        ttl: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        **Bulk Mode** — search and hydrate up to *max_results* lots.

        Parameters
        ----------
        filters:
            Search filter dict (e.g. ``{"make": ["TOYOTA"]}``).
        sort:
            Sort dict (e.g. ``{"column": "auctionDate", "order": "desc"}``).
        max_results:
            Cap on total lots to retrieve (default 1,000).
        page_size:
            Lots per search page (default 100).
        ttl:
            Optional TTL override (seconds).

        Returns
        -------
        list[dict]
            Flattened JSON-compatible records.
        """
        logger.info(
            "Bulk Mode: max_results=%d, page_size=%d, filters=%s",
            max_results,
            page_size,
            filters,
        )

        # Collect lot numbers from search
        lot_numbers: list[str] = []
        async for raw_lot in search_lots_bulk(
            self._http,
            filters=filters,
            sort=sort,
            max_results=max_results,
            page_size=page_size,
        ):
            lot_num = str(
                raw_lot.get("lotNumber")
                or raw_lot.get("lot_number")
                or ""
            ).strip()
            if lot_num:
                lot_numbers.append(lot_num)

        logger.info("Bulk Mode: %d lot numbers collected from search", len(lot_numbers))

        if not lot_numbers:
            return []

        records = await hydrate_lots(
            lot_numbers,
            self._http,
            self._cache,
            concurrency=self._concurrency,
            ttl=ttl if ttl is not None else self._cache_ttl,
        )
        logger.info("Bulk Mode: returned %d record(s)", len(records))
        return records

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    @staticmethod
    def save_json(records: list[dict[str, Any]], path: str) -> None:
        """Write *records* to *path* as a formatted JSON file."""
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(records, fh, indent=2, ensure_ascii=False)
        logger.info("Saved %d records to %s", len(records), path)
