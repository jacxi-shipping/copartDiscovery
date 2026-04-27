"""Unit tests for discovery_engine.client retry behavior."""

from __future__ import annotations

import httpx
import pytest

from discovery_engine.client import HttpClient


class TestHttpClientRetryBehavior:
    @pytest.mark.asyncio
    async def test_retries_on_retryable_status_code(self):
        transport = httpx.MockTransport(
            lambda request: httpx.Response(503, request=request, json={"error": "temporary"})
        )

        client = HttpClient(max_retries=3)
        await client.start()
        assert client._client is not None
        client._client._transport = transport

        with pytest.raises(httpx.HTTPStatusError) as exc:
            await client.get_json("https://example.com/retryable")

        assert exc.value.response.status_code == 503
        await client.close()

    @pytest.mark.asyncio
    async def test_does_not_retry_non_retryable_status_code(self):
        call_count = 0

        def responder(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(404, request=request, json={"error": "not found"})

        transport = httpx.MockTransport(responder)

        client = HttpClient(max_retries=4)
        await client.start()
        assert client._client is not None
        client._client._transport = transport

        with pytest.raises(httpx.HTTPStatusError) as exc:
            await client.get_json("https://example.com/not-found")

        assert exc.value.response.status_code == 404
        assert call_count == 1
        await client.close()

    @pytest.mark.asyncio
    async def test_retries_on_transport_error_then_succeeds(self):
        attempts = 0

        def responder(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise httpx.ConnectError("boom", request=request)
            return httpx.Response(200, request=request, json={"ok": True})

        transport = httpx.MockTransport(responder)

        client = HttpClient(max_retries=3)
        await client.start()
        assert client._client is not None
        client._client._transport = transport

        result = await client.get_json("https://example.com/flaky")
        assert result == {"ok": True}
        assert attempts == 2
        await client.close()
