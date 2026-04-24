"""Search API logic — paginated lot search against the Copart public API."""

from __future__ import annotations

import logging
from typing import Any, AsyncIterator

from .client import HttpClient
from .config import BULK_MAX_RESULTS, DEFAULT_PAGE_SIZE, SEARCH_URL

logger = logging.getLogger(__name__)


def _build_search_payload(
    filters: dict[str, Any] | None = None,
    sort: dict[str, str] | None = None,
    page: int = 0,
    size: int = DEFAULT_PAGE_SIZE,
) -> dict[str, Any]:
    """Assemble the JSON body for the search endpoint."""
    payload: dict[str, Any] = {
        "query": "*",
        "filter": filters or {},
        "sort": sort or {"column": "auctionDate", "order": "desc"},
        "page": page,
        "size": size,
    }
    return payload


def _extract_lots(response: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Pull the list of lot objects out of a search response.

    The Copart API wraps results in nested structures; we navigate
    common wrapper shapes and return an empty list on anything unexpected.
    """
    try:
        # Common response shape: {"data": {"results": {"content": [...]}}}
        if "data" in response:
            content = (
                response["data"]
                .get("results", {})
                .get("content", [])
            )
            if isinstance(content, list):
                return content

        # Alternative flat shapes
        items = response.get("lots") or response.get("content") or []
        if isinstance(items, list):
            return items
    except (AttributeError, TypeError):
        pass

    logger.warning("Unexpected search response structure; no lots extracted")
    return []


async def search_lots(
    http_client: HttpClient,
    *,
    filters: dict[str, Any] | None = None,
    sort: dict[str, str] | None = None,
    page: int = 0,
    size: int = DEFAULT_PAGE_SIZE,
) -> list[dict[str, Any]]:
    """
    Fetch a **single page** of search results.

    Returns a (possibly empty) list of raw lot dicts.
    """
    payload = _build_search_payload(filters=filters, sort=sort, page=page, size=size)
    logger.info("Searching lots: page=%d, size=%d, filters=%s", page, size, filters)
    try:
        response = await http_client.post_json(SEARCH_URL, payload)
    except Exception as exc:  # noqa: BLE001
        logger.error("Search request failed (page=%d): %s", page, exc)
        return []

    lots = _extract_lots(response)
    logger.info("Page %d returned %d lots", page, len(lots))
    return lots


async def search_lots_bulk(
    http_client: HttpClient,
    *,
    filters: dict[str, Any] | None = None,
    sort: dict[str, str] | None = None,
    max_results: int = BULK_MAX_RESULTS,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> AsyncIterator[dict[str, Any]]:
    """
    Async generator that yields individual lot dicts up to *max_results*.

    Iterates pages automatically and stops when:
    * *max_results* lots have been yielded, OR
    * a page returns fewer results than *page_size* (last page), OR
    * a page returns no results.
    """
    collected = 0
    page = 0

    while collected < max_results:
        remaining = max_results - collected
        size = min(page_size, remaining)
        lots = await search_lots(
            http_client,
            filters=filters,
            sort=sort,
            page=page,
            size=size,
        )

        if not lots:
            logger.info("No more lots at page %d; stopping bulk search", page)
            break

        for lot in lots:
            if collected >= max_results:
                break
            yield lot
            collected += 1

        if len(lots) < size:
            # Last page reached
            logger.info("Last page reached at page %d (%d lots)", page, len(lots))
            break

        page += 1

    logger.info("Bulk search complete: %d lots collected", collected)
