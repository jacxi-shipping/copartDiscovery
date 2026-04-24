"""Async HTTP client with retry / exponential-backoff logic."""

from __future__ import annotations

import logging
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import DEFAULT_HEADERS, HTTP_TIMEOUT, MAX_RETRIES, RETRY_WAIT_MAX, RETRY_WAIT_MIN

logger = logging.getLogger(__name__)

# Status codes that are worth retrying
_RETRYABLE_CODES = {429, 500, 502, 503, 504}


class HttpClient:
    """
    Async HTTP client built on ``httpx.AsyncClient``.

    Features
    --------
    * HTTP/2 support (optional, enabled by default when h2 is installed).
    * Configurable headers.
    * Automatic exponential-backoff retry for transient failures.
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

    async def start(self) -> None:
        """Create the underlying ``httpx.AsyncClient``."""
        self._client = httpx.AsyncClient(
            headers=self._headers,
            timeout=self._timeout,
            http2=True,
            follow_redirects=True,
        )

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

    async def post_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        """
        POST *payload* as JSON to *url* and return the parsed response.
        Retries on transient HTTP errors / network errors with exponential back-off.
        """
        client = self._assert_started()

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
            retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
            reraise=True,
        ):
            with attempt:
                logger.debug("POST %s (attempt %d)", url, attempt.retry_state.attempt_number)
                response = await client.post(url, json=payload)
                if response.status_code in _RETRYABLE_CODES:
                    logger.warning(
                        "POST %s returned %d, will retry", url, response.status_code
                    )
                    raise httpx.HTTPStatusError(
                        f"Retryable status {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return response.json()

        raise RuntimeError(f"POST {url} failed after {self._max_retries} retries")  # pragma: no cover

    async def get_json(self, url: str) -> dict[str, Any]:
        """
        GET *url* and return the parsed JSON response.
        Retries on transient errors with exponential back-off.
        """
        client = self._assert_started()

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
            retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
            reraise=True,
        ):
            with attempt:
                logger.debug("GET %s (attempt %d)", url, attempt.retry_state.attempt_number)
                response = await client.get(url)
                if response.status_code in _RETRYABLE_CODES:
                    logger.warning(
                        "GET %s returned %d, will retry", url, response.status_code
                    )
                    raise httpx.HTTPStatusError(
                        f"Retryable status {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return response.json()

        raise RuntimeError(f"GET {url} failed after {self._max_retries} retries")  # pragma: no cover
