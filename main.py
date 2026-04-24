#!/usr/bin/env python3
"""
Copart Vehicle Discovery Engine — CLI entry point.

Examples
--------
Request mode (hydrate specific lots):
    python main.py request 12345678 87654321

Bulk mode (search + hydrate, up to 200 lots):
    python main.py bulk --make TOYOTA --year 2020-2024 --max-results 200

Bulk mode with output file:
    python main.py bulk --make FORD --output results.json

Environment variables
---------------------
REDIS_URL          Redis connection string (default: redis://localhost:6379)
CACHE_TTL_SECONDS  Cache TTL in seconds    (default: 86400 = 24 h)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys

from discovery_engine import DiscoveryEngine
from discovery_engine.logging_config import configure_logging

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="copart-discovery",
        description="Copart Vehicle Discovery Engine",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://localhost:6379"),
        help="Redis connection URL",
    )
    parser.add_argument(
        "--ttl",
        type=int,
        default=int(os.getenv("CACHE_TTL_SECONDS", "86400")),
        help="Cache TTL in seconds (default: 86400)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Max concurrent hydration requests (default: 10)",
    )

    sub = parser.add_subparsers(dest="mode", required=True)

    # ---- request mode ----
    req = sub.add_parser("request", help="Hydrate specific lot numbers")
    req.add_argument("lot_numbers", nargs="+", help="One or more lot numbers")
    req.add_argument("--output", help="Write results to this JSON file")

    # ---- bulk mode ----
    bulk = sub.add_parser("bulk", help="Search and bulk-hydrate lots")
    bulk.add_argument(
        "--make",
        action="append",
        dest="makes",
        default=[],
        help="Filter by make (can repeat: --make TOYOTA --make FORD)",
    )
    bulk.add_argument(
        "--year",
        action="append",
        dest="years",
        default=[],
        help="Filter by year range, e.g. 2020-2024",
    )
    bulk.add_argument(
        "--max-results",
        type=int,
        default=1000,
        help="Max lots to retrieve (default: 1000)",
    )
    bulk.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Lots per search page (default: 100)",
    )
    bulk.add_argument("--output", help="Write results to this JSON file")

    return parser


async def run(args: argparse.Namespace) -> list[dict]:
    """Execute the requested mode and return records."""
    async with DiscoveryEngine(
        redis_url=args.redis_url,
        cache_ttl=args.ttl,
        concurrency=args.concurrency,
    ) as engine:
        if args.mode == "request":
            return await engine.request_mode(args.lot_numbers)

        # bulk mode
        filters: dict = {}
        if args.makes:
            filters["make"] = args.makes
        if args.years:
            filters["year"] = args.years

        return await engine.bulk_mode(
            filters=filters or None,
            max_results=args.max_results,
            page_size=args.page_size,
        )


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    configure_logging(args.log_level)

    records = asyncio.run(run(args))

    output = json.dumps(records, indent=2, ensure_ascii=False)
    print(output)

    if hasattr(args, "output") and args.output:
        DiscoveryEngine.save_json(records, args.output)
        logger.info("Results saved to %s", args.output)

    sys.exit(0)


if __name__ == "__main__":
    main()
