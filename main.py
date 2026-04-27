#!/usr/bin/env python3
"""
Copart Vehicle Discovery Engine — CLI entry point.

Examples
--------
Request mode (hydrate specific lots):
    python main.py request 12345678 87654321

Bulk mode (search + hydrate, up to 200 lots):
    python main.py bulk --make TOYOTA --year 2020-2024 --max-results 200

Bulk mode with custom sort and output file:
    python main.py bulk --make FORD --sort-column auctionDate --sort-order asc \\
        --output results.json

Force-refresh (bypass cache):
    python main.py bulk --make HONDA --force-refresh

Fail-fast search errors:
    python main.py bulk --make TOYOTA --strict-search-errors

Health check (probe Redis + Copart API):
    python main.py healthcheck

Auth check (validate cookie/credential auth context):
    python main.py authcheck

Environment variables
---------------------
REDIS_URL          Redis connection string (default: redis://localhost:6379)
CACHE_TTL_SECONDS  Cache TTL in seconds    (default: 86400 = 24 h)
COPART_SEARCH_URL  Search endpoint override
COPART_LOT_DETAILS_URL  Lot-details endpoint template override
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys

from discovery_engine import DiscoveryEngine
from discovery_engine.auth import check_copart_auth_session, parse_cookie_header
from discovery_engine.config import COPART_PASSWORD, COPART_SESSION_COOKIES, COPART_USERNAME
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
        "--json-logs",
        action="store_true",
        default=False,
        help="Emit structured JSON log lines (requires python-json-logger)",
    )
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://localhost:6379"),
        help="Redis connection URL",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        default=False,
        help="Disable Redis caching (useful for development / when Redis is unavailable)",
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
    req.add_argument(
        "--force-refresh",
        action="store_true",
        default=False,
        help="Bypass cache and re-fetch every lot from the API",
    )

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
        "--sort-column",
        default="auctionDate",
        help="Column to sort results by (default: auctionDate)",
    )
    bulk.add_argument(
        "--sort-order",
        default="desc",
        choices=["asc", "desc"],
        help="Sort direction (default: desc)",
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
    bulk.add_argument(
        "--force-refresh",
        action="store_true",
        default=False,
        help="Bypass cache and re-fetch every lot from the API",
    )
    bulk.add_argument(
        "--strict-search-errors",
        action="store_true",
        default=False,
        help="Fail fast on search API errors instead of returning partial/empty results",
    )

    # ---- healthcheck ----
    sub.add_parser("healthcheck", help="Probe Redis and Copart API connectivity")

    # ---- authcheck ----
    authcheck = sub.add_parser("authcheck", help="Validate Copart authenticated session context")
    authcheck.add_argument(
        "--auth-mode",
        default="auto",
        choices=["auto", "cookies", "credentials"],
        help="Which auth input to validate (default: auto)",
    )
    authcheck.add_argument(
        "--playwright-debug",
        action="store_true",
        default=False,
        help="Include Playwright frame/input diagnostics in auth failure reason",
    )
    authcheck.add_argument(
        "--playwright-pause-seconds",
        type=float,
        default=0.0,
        help="Pause on login page for N seconds before attempting credential fill",
    )
    authcheck.add_argument(
        "--playwright-headed",
        action="store_true",
        default=False,
        help="Force headed Playwright login for local interactive debugging",
    )
    authcheck.add_argument(
        "--playwright-artifact-dir",
        default="",
        help="Directory where authcheck writes screenshot/HTML artifacts on Playwright failure",
    )

    return parser


async def run(args: argparse.Namespace) -> list[dict]:
    """Execute the requested mode and return records."""
    if args.mode == "authcheck":
        raw_cookie = COPART_SESSION_COOKIES if args.auth_mode in {"auto", "cookies"} else ""
        cookie_map = parse_cookie_header(raw_cookie)
        use_credentials = args.auth_mode in {"auto", "credentials"}
        auth_status = await check_copart_auth_session(
            session_cookies=cookie_map,
            username=COPART_USERNAME if use_credentials else "",
            password=COPART_PASSWORD if use_credentials else "",
            playwright_debug=getattr(args, "playwright_debug", False),
            playwright_pause_seconds=getattr(args, "playwright_pause_seconds", 0.0),
            playwright_headless=False if getattr(args, "playwright_headed", False) else None,
            playwright_artifact_dir=getattr(args, "playwright_artifact_dir", "") or None,
        )
        print(
            json.dumps(
                {
                    "success": auth_status.success,
                    "reason": auth_status.reason,
                    "method": (
                        "cookies"
                        if cookie_map and auth_status.success
                        else "credentials"
                        if use_credentials
                        else "none"
                    ),
                },
                indent=2,
            )
        )
        return []

    async with DiscoveryEngine(
        redis_url=args.redis_url,
        cache_ttl=args.ttl,
        concurrency=args.concurrency,
        use_cache=not args.no_cache,
    ) as engine:
        if args.mode == "healthcheck":
            status = await engine.health_check()
            print(json.dumps(status, indent=2))
            return []

        if args.mode == "request":
            return await engine.request_mode(
                args.lot_numbers,
                force_refresh=args.force_refresh,
            )

        # bulk mode
        filters: dict = {}
        if args.makes:
            filters["make"] = args.makes
        if args.years:
            filters["year"] = args.years

        sort = {"column": args.sort_column, "order": args.sort_order}

        return await engine.bulk_mode(
            filters=filters or None,
            sort=sort,
            max_results=args.max_results,
            page_size=args.page_size,
            force_refresh=args.force_refresh,
            fail_fast_search_errors=args.strict_search_errors,
        )


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    configure_logging(args.log_level, json_logs=args.json_logs)

    records = asyncio.run(run(args))

    if args.mode not in {"healthcheck", "authcheck"}:
        output = json.dumps(records, indent=2, ensure_ascii=False)
        print(output)

        if hasattr(args, "output") and args.output:
            DiscoveryEngine.save_json(records, args.output)
            logger.info("Results saved to %s", args.output)

    sys.exit(0)


if __name__ == "__main__":
    main()
