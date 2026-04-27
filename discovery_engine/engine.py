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

from .auth import authenticate_copart_session, parse_cookie_header
from .cache import NullCache, RedisCache
from .client import HttpClient
from .config import (
    BULK_MAX_RESULTS,
    COPART_AUTH_ENABLED,
    COPART_PASSWORD,
    COPART_SESSION_COOKIES,
    COPART_USERNAME,
    DEFAULT_CACHE_TTL,
    DEFAULT_CONCURRENCY,
    DEFAULT_PAGE_SIZE,
    REDIS_URL,
    SEARCH_URL,
)
from .hydration import HydrationStats, hydrate_lots
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
    use_cache:
        Set to ``False`` to disable Redis caching (useful for development
        or when Redis is unavailable).  A :class:`NullCache` is used instead.
    """

    def __init__(
        self,
        redis_url: str = REDIS_URL,
        cache_ttl: int = DEFAULT_CACHE_TTL,
        concurrency: int = DEFAULT_CONCURRENCY,
        http_headers: dict[str, str] | None = None,
        use_cache: bool = True,
        copart_username: str | None = None,
        copart_password: str | None = None,
        copart_session_cookies: str | None = None,
        auth_enabled: bool = COPART_AUTH_ENABLED,
    ) -> None:
        self._cache: RedisCache | NullCache = (
            RedisCache(redis_url=redis_url, ttl=cache_ttl) if use_cache else NullCache()
        )
        self._http = HttpClient(headers=http_headers)
        self._concurrency = concurrency
        self._cache_ttl = cache_ttl
        self._last_stats: HydrationStats | None = None
        self._copart_username = copart_username if copart_username is not None else COPART_USERNAME
        self._copart_password = copart_password if copart_password is not None else COPART_PASSWORD
        raw_cookies = (
            copart_session_cookies
            if copart_session_cookies is not None
            else COPART_SESSION_COOKIES
        )
        self._copart_session_cookies = parse_cookie_header(raw_cookies)
        self._auth_enabled = auth_enabled

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------

    async def _startup(self) -> None:
        await self._cache.connect()
        await self._http.start()
        if self._auth_enabled and self._copart_session_cookies:
            self._http.update_cookies(self._copart_session_cookies)
            logger.info("Copart session cookies loaded from environment")
            return

        if self._auth_enabled and self._copart_username and self._copart_password:
            auth_result = await authenticate_copart_session(
                self._copart_username,
                self._copart_password,
            )
            if auth_result.success:
                self._http.update_cookies(auth_result.cookies)
                logger.info("Copart authenticated session initialised")
            else:
                logger.warning("Copart auth unavailable: %s", auth_result.reason)

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

    @property
    def last_stats(self) -> HydrationStats | None:
        """Stats from the most recent :meth:`request_mode` / :meth:`bulk_mode` call."""
        return self._last_stats

    async def request_mode(
        self,
        lot_numbers: list[str],
        *,
        ttl: int | None = None,
        force_refresh: bool = False,
    ) -> list[dict[str, Any]]:
        """
        **Request Mode** — hydrate specific lot numbers.

        Parameters
        ----------
        lot_numbers:
            Lot numbers to fetch/hydrate.
        ttl:
            Optional TTL override (seconds).
        force_refresh:
            Skip cache reads and re-fetch every lot from the API.

        Returns
        -------
        list[dict]
            Flattened JSON-compatible records.
        """
        logger.info("Request Mode: hydrating %d lot(s)", len(lot_numbers))
        records, stats = await hydrate_lots(
            lot_numbers,
            self._http,
            self._cache,
            concurrency=self._concurrency,
            ttl=ttl if ttl is not None else self._cache_ttl,
            force_refresh=force_refresh,
        )
        self._last_stats = stats
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
        force_refresh: bool = False,
        fail_fast_search_errors: bool = False,
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
        force_refresh:
            Skip cache reads and re-fetch every lot from the API.
        fail_fast_search_errors:
            If ``True``, search API failures immediately raise and abort
            bulk mode. If ``False`` (default), failures stop pagination
            gracefully and return whatever was collected so far.

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
        search_hits_by_lot: dict[str, dict[str, Any]] = {}
        async for raw_lot in search_lots_bulk(
            self._http,
            filters=filters,
            sort=sort,
            max_results=max_results,
            page_size=page_size,
            fail_fast=fail_fast_search_errors,
        ):
            lot_num = str(
                raw_lot.get("lotNumber")
                or raw_lot.get("lot_number")
                or raw_lot.get("lotNumberStr")
                or raw_lot.get("ln")
                or ""
            ).strip()
            if lot_num:
                lot_numbers.append(lot_num)
                # Keep the full hit to allow partial-record fallback if detail API is blocked.
                search_hits_by_lot[lot_num] = raw_lot

        logger.info("Bulk Mode: %d lot numbers collected from search", len(lot_numbers))

        if not lot_numbers:
            return []

        records, stats = await hydrate_lots(
            lot_numbers,
            self._http,
            self._cache,
            concurrency=self._concurrency,
            ttl=ttl if ttl is not None else self._cache_ttl,
            force_refresh=force_refresh,
            search_fallback_map=search_hits_by_lot,
        )
        self._last_stats = stats
        logger.info("Bulk Mode: returned %d record(s)", len(records))
        return records

    async def health_check(self) -> dict[str, Any]:
        """
        Probe connectivity to Redis and the Copart search API.

        Returns a dict with ``"redis"`` and ``"api"`` keys, each set to
        ``"ok"`` or an error string.
        """
        status: dict[str, Any] = {"redis": "unknown", "api": "unknown"}

        # Redis probe
        try:
            ok = await self._cache.ping()
            status["redis"] = "ok" if ok else "error: ping returned False"
        except Exception as exc:  # noqa: BLE001
            status["redis"] = f"error: {exc}"

        # Copart API probe — minimal 1-result search
        try:
            await self._http.post_json(
                SEARCH_URL,
                {"query": "*", "filter": {}, "page": 0, "size": 1},
            )
            status["api"] = "ok"
        except Exception as exc:  # noqa: BLE001
            status["api"] = f"error: {exc}"

        return status

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    @staticmethod
    def save_json(records: list[dict[str, Any]], path: str) -> None:
        """Write *records* to *path* as a formatted JSON file."""
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(records, fh, indent=2, ensure_ascii=False)
        logger.info("Saved %d records to %s", len(records), path)
