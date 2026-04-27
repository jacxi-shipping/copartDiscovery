"""Data models and validation for vehicle lot records."""

from __future__ import annotations

import dataclasses
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class LotRecord:
    """Canonical, type-safe representation of a single vehicle lot."""

    lotNumber: str
    lotDescription: str
    vin: str
    odometer: float | None
    repairCost: float | None
    imagesList: list[str]
    fetched_at: str

    def to_dict(self) -> dict[str, Any]:
        """Return a plain JSON-serialisable dict (no nested dataclasses)."""
        return dataclasses.asdict(self)


def _coerce_str(value: Any) -> str:
    """Return *value* as a str, or empty string on failure."""
    return str(value) if value is not None else ""


def _coerce_number(value: Any) -> float | None:
    """Return *value* as float, or None if it cannot be converted."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_images(value: Any) -> list[str]:
    """Return only string URLs from a list; ignore non-strings."""
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str) and item.strip()]


def parse_lot_detail_response(response: dict[str, Any]) -> dict[str, Any]:
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


def build_lot_record(raw: dict[str, Any], fetched_at: str | None = None) -> LotRecord | None:
    """
    Validate and normalise a raw lot-detail payload into a ``LotRecord``.

    Returns a ``LotRecord`` if the record is valid, or ``None`` if
    required fields are missing / invalid.
    """
    if not isinstance(raw, dict):
        logger.warning("Lot payload is not a dict: %r", type(raw))
        return None

    lot_number = raw.get("lotNumber") or raw.get("lot_number")
    if not lot_number:
        logger.warning("Lot record missing lotNumber; skipping")
        return None

    return LotRecord(
        lotNumber=_coerce_str(lot_number),
        lotDescription=_coerce_str(raw.get("lotDescription") or raw.get("lot_description")),
        vin=_coerce_str(raw.get("vin")),
        odometer=_coerce_number(raw.get("odometer")),
        repairCost=_coerce_number(raw.get("repairCost") or raw.get("repair_cost")),
        imagesList=_coerce_images(raw.get("imagesList") or raw.get("images_list") or []),
        fetched_at=fetched_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def build_lot_record_from_search_hit(
    search_hit: dict[str, Any],
    fetched_at: str | None = None,
) -> LotRecord | None:
    """
    Best-effort conversion from a search-result hit to a ``LotRecord``.

    Search payloads often expose summary fields (lot number/description/image)
    but not full dynamic details (VIN, repair cost, etc.). Missing fields are
    safely defaulted.
    """
    if not isinstance(search_hit, dict):
        return None

    lot_number = (
        search_hit.get("lotNumber")
        or search_hit.get("lot_number")
        or search_hit.get("lotNumberStr")
        or search_hit.get("ln")
    )
    if not lot_number:
        return None

    image_candidates: list[str] = []
    for key in ("imagesList", "images_list"):
        value = search_hit.get(key)
        if isinstance(value, list):
            image_candidates.extend([str(v) for v in value if isinstance(v, str)])

    for key in ("imgUrl", "imageUrl", "thumbUrl", "thb"):
        value = search_hit.get(key)
        if isinstance(value, str) and value.strip():
            image_candidates.append(value)

    lot_description = (
        search_hit.get("lotDescription")
        or search_hit.get("lot_description")
        or search_hit.get("ld")
        or search_hit.get("lDesc")
        or ""
    )

    return LotRecord(
        lotNumber=_coerce_str(lot_number),
        lotDescription=_coerce_str(lot_description),
        vin=_coerce_str(search_hit.get("vin") or search_hit.get("v")),
        odometer=_coerce_number(search_hit.get("odometer") or search_hit.get("odo")),
        repairCost=_coerce_number(search_hit.get("repairCost") or search_hit.get("repair_cost")),
        imagesList=_coerce_images(image_candidates),
        fetched_at=fetched_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def validate_search_payload(payload: dict[str, Any]) -> bool:
    """Return True if *payload* has the expected top-level structure."""
    if not isinstance(payload, dict):
        return False
    if "query" not in payload:
        return False
    return True
