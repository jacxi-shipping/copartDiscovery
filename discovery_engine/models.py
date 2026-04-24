"""Data models and validation for vehicle lot records."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Required fields and their expected Python types
_REQUIRED_FIELDS: dict[str, type] = {
    "lotNumber": str,
}

# Optional fields: (field_name -> expected_type)
_OPTIONAL_FIELDS: dict[str, type] = {
    "lotDescription": str,
    "vin": str,
    "odometer": (int, float),   # type: ignore[assignment]
    "repairCost": (int, float),  # type: ignore[assignment]
    "imagesList": list,
}


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


def build_lot_record(raw: dict[str, Any], fetched_at: str | None = None) -> dict[str, Any] | None:
    """
    Validate and normalise a raw lot-detail payload.

    Returns a flattened dict if the record is valid, or ``None`` if
    required fields are missing / invalid.
    """
    if not isinstance(raw, dict):
        logger.warning("Lot payload is not a dict: %r", type(raw))
        return None

    lot_number = raw.get("lotNumber") or raw.get("lot_number")
    if not lot_number:
        logger.warning("Lot record missing lotNumber; skipping")
        return None

    lot_number = _coerce_str(lot_number)

    record: dict[str, Any] = {
        "lotNumber": lot_number,
        "lotDescription": _coerce_str(raw.get("lotDescription") or raw.get("lot_description")),
        "vin": _coerce_str(raw.get("vin")),
        "odometer": _coerce_number(raw.get("odometer")),
        "repairCost": _coerce_number(raw.get("repairCost") or raw.get("repair_cost")),
        "imagesList": _coerce_images(raw.get("imagesList") or raw.get("images_list") or []),
        "fetched_at": fetched_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return record


def validate_search_payload(payload: dict[str, Any]) -> bool:
    """Return True if *payload* has the expected top-level structure."""
    if not isinstance(payload, dict):
        return False
    if "query" not in payload:
        return False
    return True
