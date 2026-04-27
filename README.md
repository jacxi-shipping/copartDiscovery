# copartDiscovery

Async Python engine for discovering and hydrating vehicle lots from the
[Copart](https://www.copart.com) public API.

---

## Features

| Feature | Detail |
|---|---|
| **Request Mode** | On-demand hydration of specific lot numbers (Redis-first) |
| **Bulk Mode** | Paginated search + concurrent hydration, up to 1 000 lots |
| **Redis caching** | Per-lot TTL, bulk MGET for minimal round-trips, `NullCache` for dev |
| **Retry / back-off** | Exponential back-off + `Retry-After` header awareness for 429s |
| **Force refresh** | `--force-refresh` bypasses cache and re-fetches from the API |
| **Structured logging** | Plain text or JSON (`--json-logs`) via `python-json-logger` |
| **Health check** | Probes Redis + Copart API connectivity |
| **Run statistics** | Cache hits, misses, failures, elapsed time per run |
| **Fallback records** | Bulk mode can build partial records from search hits if lot-detail calls fail |

---

## Requirements

* Python 3.12+
* Redis 6+ (or set `--no-cache` to skip Redis entirely)

---

## Installation

```bash
pip install -r requirements.txt
```

For reproducible installs (exact pinned versions):

```bash
pip install -r requirements-lock.txt
```

For credential login automation, install Playwright browser binaries:

```bash
python -m playwright install chromium
```

---

## Quick start

### Request Mode — hydrate specific lots

```bash
python main.py request 12345678 87654321
```

### Bulk Mode — search and hydrate up to N lots

```bash
# Filter by make, write results to a file
python main.py bulk --make TOYOTA --make HONDA --max-results 200 --output results.json

# Custom sort
python main.py bulk --make FORD --sort-column repairCost --sort-order asc

# Bypass cache and re-fetch everything
python main.py bulk --make BMW --force-refresh

# Fail fast on search API errors
python main.py bulk --make TOYOTA --strict-search-errors
```

### Health check

```bash
python main.py healthcheck
# {"redis": "ok", "api": "ok"}
```

### Auth check

```bash
python main.py authcheck --auth-mode auto
# {"success": false, "reason": "...", "method": "credentials|cookies|none"}

# Local interactive debug flow (headed browser + page pause + diagnostics)
python main.py authcheck --auth-mode credentials --playwright-headed --playwright-pause-seconds 8 --playwright-debug

# Save screenshot + HTML artifacts to a custom directory on Playwright failure
python main.py authcheck --auth-mode credentials --playwright-debug --playwright-artifact-dir tmp/copart-auth
```

---

## CLI reference

```
usage: copart-discovery [-h] [--log-level {DEBUG,INFO,WARNING,ERROR}]
                        [--json-logs] [--redis-url REDIS_URL] [--no-cache]
                        [--ttl TTL] [--concurrency CONCURRENCY]
                        {request,bulk,healthcheck} ...

Global options
  --log-level           Logging verbosity (default: INFO)
  --json-logs           Emit structured JSON log lines
  --redis-url           Redis connection URL (default: redis://localhost:6379)
  --no-cache            Disable Redis caching (NullCache mode)
  --ttl                 Cache TTL in seconds (default: 86400 = 24 h)
  --concurrency         Max concurrent hydration requests (default: 10)

request <lot_number> [lot_number …]
  --output FILE         Write results to this JSON file
  --force-refresh       Bypass cache and re-fetch from API

bulk
  --make MAKE           Filter by make (repeatable: --make TOYOTA --make FORD)
  --year YEAR           Filter by year range, e.g. 2020-2024 (repeatable)
  --sort-column COL     Column to sort by (default: auctionDate)
  --sort-order {asc,desc}
  --max-results N       Cap on total lots to retrieve (default: 1000)
  --page-size N         Lots per search page (default: 100)
  --output FILE         Write results to this JSON file
  --force-refresh       Bypass cache and re-fetch from API
  --strict-search-errors
                        Fail fast on search API errors
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `CACHE_TTL_SECONDS` | `86400` | Cache TTL in seconds (24 h) |
| `COPART_SEARCH_URL` | `https://www.copart.com/public/lots/search` | Search endpoint override |
| `COPART_LOT_DETAILS_URL` | `https://www.copart.com/api/v1/public/lotdetails/{lot_number}` | Lot details endpoint override |
| `COPART_AUTH_ENABLED` | `true` | Enable best-effort authenticated session bootstrap |
| `COPART_PLAYWRIGHT_HEADLESS` | `true` | Run Playwright in headless mode for credential login (set `false` for local interactive login) |
| `COPART_AUTH_DEBUG_DIR` | `.artifacts/copart-auth` | Default directory for Playwright screenshot/HTML failure artifacts |
| `COPART_USERNAME` | `` | Copart username/email for optional login |
| `COPART_PASSWORD` | `` | Copart password for optional login |
| `COPART_SESSION_COOKIES` | `` | Cookie header string (`name=value; name2=value2`) to reuse browser-authenticated session |
| `COPART_LOGIN_URL` | `https://www.copart.com/login` | Login page URL override |
| `COPART_AUTH_PROBE_URL` | `https://www.copart.com/public/data/member/account` | URL used to verify authenticated session |

When credentials are configured, the engine first attempts browser-based login
with Playwright. If Playwright cannot complete the flow (for example WAF /
captcha / anti-bot controls), the engine logs a warning and continues in
resilient fallback mode.

If automated username/password login is blocked, provide
`COPART_SESSION_COOKIES` from an already logged-in browser session to run the
engine with that authenticated context.

---

## Output schema

Each record in the returned JSON array has the following fields:

```json
{
  "lotNumber":      "12345678",
  "lotDescription": "2021 Toyota Camry",
  "vin":            "JT1234567890123456",
  "odometer":       15000.0,
  "repairCost":     1200.0,
  "imagesList":     ["https://cs.copart.com/..."],
  "fetched_at":     "2026-04-24T12:00:00Z"
}
```

---

## Library usage

```python
import asyncio
from discovery_engine import DiscoveryEngine

async def main():
    async with DiscoveryEngine(redis_url="redis://localhost:6379") as engine:
        # Request mode
        records = await engine.request_mode(["12345678", "87654321"])

        # Bulk mode
        records = await engine.bulk_mode(
            filters={"make": ["TOYOTA"]},
            sort={"column": "auctionDate", "order": "desc"},
            max_results=500,
        )

        # Inspect run stats
        stats = engine.last_stats
        print(f"Hits: {stats.cache_hits}, Misses: {stats.cache_misses}")

        # Health check
        status = await engine.health_check()
        print(status)  # {"redis": "ok", "api": "ok"}

asyncio.run(main())
```

### Disable Redis (development / testing)

```python
async with DiscoveryEngine(use_cache=False) as engine:
    records = await engine.request_mode(["12345678"])
```

---

## Docker Compose

A `docker-compose.yml` is included for local development:

```bash
docker compose up -d redis        # start Redis only
docker compose run --rm discovery bulk --make TOYOTA --max-results 50
```

---

## Running tests

```bash
pip install pytest pytest-asyncio fakeredis pytest-httpx
pytest
```

Tests use `fakeredis` (in-process Redis) and `pytest-httpx` (HTTP interception),
so no external services are required.

---

## Linting

```bash
pip install ruff
ruff check .
ruff format .
```
