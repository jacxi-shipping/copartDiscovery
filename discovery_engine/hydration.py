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
from .models import build_lot_record, parse_lot_detail_response

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
) -> dict[str, Any] | None:
    """
    Fetch a single lot from the Copart API, validate, normalise, and cache it.

    Returns the normalised dict on success, ``None`` on any failure.
    """
    async with semaphore:
        url = LOT_DETAILS_URL.format(lot_number=lot_number)
        logger.info("Hydrating lot %s from API", lot_number)
        try:
            response = await http_client.get_json(url)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to hydrate lot %s: %s", lot_number, exc)
            return None

    raw_detail = parse_lot_detail_response(response)
    if not raw_detail:
        logger.warning("Empty lot detail for %s", lot_number)
        return None

    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    record = build_lot_record(raw_detail, fetched_at=fetched_at)
    if record is None:
        logger.warning("Validation failed for lot %s", lot_number)
        return None

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
            _fetch_lot_from_api(ln, http_client, cache, semaphore, ttl=ttl)
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
