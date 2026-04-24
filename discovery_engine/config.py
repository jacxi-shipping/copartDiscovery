"""Configuration constants and defaults for the discovery engine."""

import os

# API endpoints
SEARCH_URL = "https://www.copart.com/api/v1/public/lots/search"
LOT_DETAILS_URL = "https://www.copart.com/api/v1/public/lotdetails/{lot_number}"

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
