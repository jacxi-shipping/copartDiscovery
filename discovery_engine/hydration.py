"""Concurrent lot-detail hydration with Redis caching and semaphore throttle."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from datetime import datetime, timezone
from typing import Any

from .cache import RedisCache
from .client import HttpClient
from .config import DEFAULT_CONCURRENCY, LOT_DETAILS_URL
from .models import build_lot_record, build_lot_record_from_search_hit, parse_lot_detail_response

logger = logging.getLogger(__name__)

# Keep the old private name as an alias so existing imports keep working.
_parse_lot_detail_response = parse_lot_detail_response


@dataclasses.dataclass
class HydrationStats:
    """Per-run metrics for a call to :func:`hydrate_lots`."""

    total: int
    cache_hits: int
    cache_misses: int
    api_failures: int
    elapsed_seconds: float

    @property
    def success_rate(self) -> float:
        """Fraction of lots that were successfully returned (0–1)."""
        if self.total == 0:
            return 0.0
        return (self.total - self.api_failures) / self.total


async def _fetch_lot_from_api(
    lot_number: str,
    http_client: HttpClient,
    cache: RedisCache,
    semaphore: asyncio.Semaphore,
    ttl: int | None = None,
    search_fallback_hit: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """
    Fetch a single lot from the Copart API, validate, normalise, and cache it.

    Returns the normalised dict on success, ``None`` on any failure.
    """
    def _fallback_record() -> dict[str, Any] | None:
        if not search_fallback_hit:
            return None
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        fallback = build_lot_record_from_search_hit(search_fallback_hit, fetched_at=fetched_at)
        if fallback is None:
            return None
        return fallback.to_dict()

    async with semaphore:
        url = LOT_DETAILS_URL.format(lot_number=lot_number)
        logger.info("Hydrating lot %s from API", lot_number)
        try:
            response = await http_client.get_json(url)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to hydrate lot %s: %s", lot_number, exc)
            fallback = _fallback_record()
            if fallback is None:
                return None
            logger.info("Using search fallback record for lot %s after API failure", lot_number)
            await cache.set_lot(lot_number, fallback, ttl=ttl)
            return fallback

    raw_detail = parse_lot_detail_response(response)
    if not raw_detail:
        logger.warning("Empty lot detail for %s", lot_number)
        fallback = _fallback_record()
        if fallback is None:
            return None
        logger.info("Using search fallback record for lot %s after empty detail payload", lot_number)
        await cache.set_lot(lot_number, fallback, ttl=ttl)
        return fallback

    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    record = build_lot_record(raw_detail, fetched_at=fetched_at)
    if record is None:
        logger.warning("Validation failed for lot %s", lot_number)
        fallback = _fallback_record()
        if fallback is None:
            return None
        logger.info("Using search fallback record for lot %s after validation failure", lot_number)
        await cache.set_lot(lot_number, fallback, ttl=ttl)
        return fallback

    record_dict = record.to_dict()
    await cache.set_lot(lot_number, record_dict, ttl=ttl)
    return record_dict


async def hydrate_lots(
    lot_numbers: list[str],
    http_client: HttpClient,
    cache: RedisCache,
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    ttl: int | None = None,
    force_refresh: bool = False,
    search_fallback_map: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], HydrationStats]:
    """
    Hydrate *lot_numbers* concurrently, consulting Redis first.

    Parameters
    ----------
    lot_numbers:
        Lot numbers to hydrate.
    http_client:
        Started ``HttpClient`` instance.
    cache:
        Connected cache instance (``RedisCache`` or ``NullCache``).
    concurrency:
        Maximum simultaneous in-flight API requests.
    ttl:
        Cache TTL override (seconds); ``None`` uses the cache default.
    force_refresh:
        When ``True``, skip cache reads and re-fetch everything from the API.
    search_fallback_map:
        Optional lot_number -> search-hit mapping used to build partial records
        if detail hydration fails due to blocked/changed endpoints.

    Returns
    -------
    tuple[list[dict], HydrationStats]
        Successfully hydrated records (failed lots are omitted) and run stats.
    """
    if not lot_numbers:
        empty_stats = HydrationStats(
            total=0, cache_hits=0, cache_misses=0, api_failures=0, elapsed_seconds=0.0
        )
        return [], empty_stats

    t0 = time.monotonic()
    records: list[dict[str, Any]] = []
    api_failures = 0

    if force_refresh:
        # Skip cache; fetch everything from the API.
        cached_map: dict[str, Any] = {ln: None for ln in lot_numbers}
    else:
        # Single MGET round-trip instead of N individual GETs.
        cached_map = await cache.get_lots_bulk(lot_numbers)

    cache_hits = sum(1 for v in cached_map.values() if v is not None)
    misses = [ln for ln, v in cached_map.items() if v is None]

    # Return cached records in original insertion order.
    for ln in lot_numbers:
        cached = cached_map.get(ln)
        if cached is not None:
            records.append(cached)

    # Fetch misses from the API concurrently.
    if misses:
        semaphore = asyncio.Semaphore(concurrency)
        tasks = [
            _fetch_lot_from_api(
                ln,
                http_client,
                cache,
                semaphore,
                ttl=ttl,
                search_fallback_hit=(search_fallback_map or {}).get(ln),
            )
            for ln in misses
        ]
        api_results = await asyncio.gather(*tasks, return_exceptions=False)
        for result in api_results:
            if result is not None:
                records.append(result)
            else:
                api_failures += 1

    elapsed = time.monotonic() - t0
    stats = HydrationStats(
        total=len(lot_numbers),
        cache_hits=cache_hits,
        cache_misses=len(misses),
        api_failures=api_failures,
        elapsed_seconds=round(elapsed, 3),
    )
    logger.info(
        "Hydration complete: %d/%d lots succeeded "
        "(hits=%d misses=%d failures=%d elapsed=%.3fs)",
        len(records),
        len(lot_numbers),
        stats.cache_hits,
        stats.cache_misses,
        stats.api_failures,
        stats.elapsed_seconds,
    )
    return records, stats
