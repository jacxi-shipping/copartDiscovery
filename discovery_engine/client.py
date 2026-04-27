"""Async HTTP client with retry / exponential-backoff logic."""

from __future__ import annotations

import logging
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from .config import DEFAULT_HEADERS, HTTP_TIMEOUT, MAX_RETRIES, RETRY_WAIT_MAX, RETRY_WAIT_MIN

logger = logging.getLogger(__name__)

# Status codes that are worth retrying
_RETRYABLE_CODES = {429, 500, 502, 503, 504}


def _is_retryable_exception(exc: BaseException) -> bool:
    """Return True when *exc* should trigger a retry attempt."""
    if isinstance(exc, (httpx.TransportError, httpx.TimeoutException)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_CODES
    return False


def _compute_wait(retry_state: Any) -> float:
    """
    Return the number of seconds to wait before the next retry attempt.

    For 429 responses the server-supplied ``Retry-After`` header is
    respected; all other cases fall back to exponential back-off.
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429:
        retry_after = exc.response.headers.get("Retry-After", "")
        if retry_after.isdigit():
            delay = float(retry_after)
            logger.debug("Honouring Retry-After: %.1fs", delay)
            return delay
    return wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX)(retry_state)


class HttpClient:
    """
    Async HTTP client built on ``httpx.AsyncClient``.

    Features
    --------
    * HTTP/2 support (optional, enabled by default when h2 is installed).
    * Configurable headers.
    * Automatic exponential-backoff retry for transient failures.
    * ``Retry-After`` header awareness for 429 responses.
    """

    def __init__(
        self,
        headers: dict[str, str] | None = None,
        timeout: float = HTTP_TIMEOUT,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self._headers = {**DEFAULT_HEADERS, **(headers or {})}
        self._timeout = timeout
        self._max_retries = max_retries
        self._client: httpx.AsyncClient | None = None
        self._pending_cookies: dict[str, str] = {}

    async def start(self) -> None:
        """Create the underlying ``httpx.AsyncClient``."""
        self._client = httpx.AsyncClient(
            headers=self._headers,
            timeout=self._timeout,
            http2=True,
            follow_redirects=True,
        )
        if self._pending_cookies:
            self._client.cookies.update(self._pending_cookies)

    def update_cookies(self, cookies: dict[str, str]) -> None:
        """Merge cookies into the active client (or queue them before start)."""
        if not cookies:
            return
        self._pending_cookies.update(cookies)
        if self._client is not None:
            self._client.cookies.update(cookies)

    async def close(self) -> None:
        """Close the underlying client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "HttpClient":
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Core request helpers
    # ------------------------------------------------------------------

    def _assert_started(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("HttpClient not started. Call start() or use as async context manager.")
        return self._client

    async def _request(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        """
        Execute an HTTP request with automatic retry / back-off.

        Retries on transient network errors, timeouts, and retryable
        HTTP status codes (429, 5xx).
        """
        client = self._assert_started()

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_retries),
            wait=_compute_wait,
            retry=retry_if_exception(_is_retryable_exception),
            reraise=True,
        ):
            with attempt:
                logger.debug(
                    "%s %s (attempt %d)", method, url, attempt.retry_state.attempt_number
                )
                response = await client.request(method, url, **kwargs)
                if response.status_code in _RETRYABLE_CODES:
                    logger.warning("%s %s returned %d, will retry", method, url, response.status_code)
                    raise httpx.HTTPStatusError(
                        f"Retryable status {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return response.json()

        raise RuntimeError(f"{method} {url} failed after {self._max_retries} retries")  # pragma: no cover

    async def post_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST *payload* as JSON to *url* and return the parsed response."""
        return await self._request("POST", url, json=payload)

    async def get_json(self, url: str) -> dict[str, Any]:
        """GET *url* and return the parsed JSON response."""
        return await self._request("GET", url)
