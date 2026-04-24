"""Concurrent lot-detail hydration with Redis caching and semaphore throttle."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from .cache import RedisCache
from .client import HttpClient
from .config import DEFAULT_CONCURRENCY, LOT_DETAILS_URL
from .models import build_lot_record

logger = logging.getLogger(__name__)


def _parse_lot_detail_response(response: dict[str, Any]) -> dict[str, Any]:
    """
    Extract the lot-detail payload from the API response wrapper.

    Returns an empty dict if the expected structure is absent.
    """
    try:
        # Common shape: {"data": {"lotDetails": {...}}}
        detail = response.get("data", {}).get("lotDetails") or {}
        if detail:
            return detail

        # Flat shape — the response itself is the detail
        if "lotNumber" in response or "lot_number" in response:
            return response
    except (AttributeError, TypeError):
        pass
    return {}


async def _hydrate_single(
    lot_number: str,
    http_client: HttpClient,
    cache: RedisCache,
    semaphore: asyncio.Semaphore,
    ttl: int | None = None,
) -> dict[str, Any] | None:
    """
    Hydrate a single lot, consulting Redis first.

    Steps:
    1. Check Redis cache.
    2. If miss, fetch from Copart API.
    3. Validate and normalise the response.
    4. Store in Redis.
    5. Return the normalised record.
    """
    # 1. Cache look-up
    cached = await cache.get_lot(lot_number)
    if cached is not None:
        return cached

    # 2. Fetch from API (throttled by semaphore)
    async with semaphore:
        url = LOT_DETAILS_URL.format(lot_number=lot_number)
        logger.info("Hydrating lot %s from API", lot_number)
        try:
            response = await http_client.get_json(url)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to hydrate lot %s: %s", lot_number, exc)
            return None

    # 3. Parse + validate
    raw_detail = _parse_lot_detail_response(response)
    if not raw_detail:
        logger.warning("Empty lot detail for %s", lot_number)
        return None

    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    record = build_lot_record(raw_detail, fetched_at=fetched_at)
    if record is None:
        logger.warning("Validation failed for lot %s", lot_number)
        return None

    # 4. Store in Redis
    await cache.set_lot(lot_number, record, ttl=ttl)

    return record


async def hydrate_lots(
    lot_numbers: list[str],
    http_client: HttpClient,
    cache: RedisCache,
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    ttl: int | None = None,
) -> list[dict[str, Any]]:
    """
    Hydrate *lot_numbers* concurrently.

    Parameters
    ----------
    lot_numbers:
        Lot numbers to hydrate.
    http_client:
        Started ``HttpClient`` instance.
    cache:
        Connected ``RedisCache`` instance.
    concurrency:
        Maximum simultaneous in-flight requests.
    ttl:
        Cache TTL override (seconds); ``None`` uses the cache default.

    Returns
    -------
    list[dict]
        Successfully hydrated records (failed lots are omitted).
    """
    if not lot_numbers:
        return []

    semaphore = asyncio.Semaphore(concurrency)
    tasks = [
        _hydrate_single(ln, http_client, cache, semaphore, ttl=ttl)
        for ln in lot_numbers
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    records = [r for r in results if r is not None]
    logger.info(
        "Hydration complete: %d/%d lots succeeded", len(records), len(lot_numbers)
    )
    return records
