"""Configuration constants and defaults for the discovery engine."""

import os
from pathlib import Path


def _load_local_env() -> None:
    """Load key=value pairs from workspace .env into process env (best effort)."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_local_env()

# API endpoints
SEARCH_URL = os.getenv(
    "COPART_SEARCH_URL",
    "https://www.copart.com/public/lots/search",
)
LOT_DETAILS_URL = os.getenv(
    "COPART_LOT_DETAILS_URL",
    "https://www.copart.com/api/v1/public/lotdetails/{lot_number}",
)
COPART_LOGIN_URL = os.getenv("COPART_LOGIN_URL", "https://www.copart.com/login")
COPART_AUTH_PROBE_URL = os.getenv(
    "COPART_AUTH_PROBE_URL",
    "https://www.copart.com/public/data/member/account",
)
COPART_USERNAME = os.getenv("COPART_USERNAME", "")
COPART_PASSWORD = os.getenv("COPART_PASSWORD", "")
COPART_SESSION_COOKIES = os.getenv("COPART_SESSION_COOKIES", "")
COPART_AUTH_ENABLED = os.getenv("COPART_AUTH_ENABLED", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
COPART_PLAYWRIGHT_HEADLESS = os.getenv("COPART_PLAYWRIGHT_HEADLESS", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
COPART_AUTH_DEBUG_DIR = os.getenv("COPART_AUTH_DEBUG_DIR", ".artifacts/copart-auth")

# Pagination
DEFAULT_PAGE_SIZE = 100
BULK_MAX_RESULTS = 1000

# Concurrency
DEFAULT_CONCURRENCY = 10  # semaphore limit for async hydration

# Redis
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DEFAULT_CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", 86400))  # 24 hours

# HTTP timeouts (seconds)
HTTP_TIMEOUT = 30.0

# Retry / backoff
MAX_RETRIES = 4
RETRY_WAIT_MIN = 1.0  # seconds
RETRY_WAIT_MAX = 16.0  # seconds

# Default request headers
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (compatible; CopartDiscoveryEngine/1.0)"
    ),
}
