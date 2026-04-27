"""Tests for authentication helpers."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from discovery_engine.auth import CopartAuthResult, authenticate_copart_session, parse_cookie_header


def test_parse_cookie_header_splits_cookie_string():
	assert parse_cookie_header("a=1; b=2") == {"a": "1", "b": "2"}


@pytest.mark.asyncio
async def test_authenticate_uses_playwright_when_successful():
	with patch(
		"discovery_engine.auth._authenticate_with_playwright",
		new=AsyncMock(return_value=CopartAuthResult(success=True, reason="ok", cookies={"k": "v"})),
	) as mock_playwright:
		result = await authenticate_copart_session("user@example.com", "secret")

	assert result.success is True
	assert result.cookies == {"k": "v"}
	mock_playwright.assert_awaited_once()


@pytest.mark.asyncio
async def test_authenticate_returns_playwright_failure_without_http_fallback():
	with patch(
		"discovery_engine.auth._authenticate_with_playwright",
		new=AsyncMock(
			return_value=CopartAuthResult(
				success=False,
				reason="playwright login timed out",
				cookies={},
			)
		),
	) as mock_playwright:
		result = await authenticate_copart_session("user@example.com", "secret")

	assert result.success is False
	assert result.reason == "playwright login timed out"
	mock_playwright.assert_awaited_once()


@pytest.mark.asyncio
async def test_authenticate_reports_playwright_failure_reason():
	with patch(
		"discovery_engine.auth._authenticate_with_playwright",
		new=AsyncMock(return_value=CopartAuthResult(success=False, reason="pw failed", cookies={})),
	):
		result = await authenticate_copart_session("user@example.com", "secret")

	assert result.success is False
	assert "pw failed" in result.reason


@pytest.mark.asyncio
async def test_authenticate_passes_playwright_debug_options():
	with patch(
		"discovery_engine.auth._authenticate_with_playwright",
		new=AsyncMock(return_value=CopartAuthResult(success=False, reason="pw failed", cookies={})),
	) as mock_playwright:
		await authenticate_copart_session(
			"user@example.com",
			"secret",
			playwright_debug=True,
			playwright_pause_seconds=2.0,
			playwright_headless=False,
			playwright_artifact_dir="tmp/auth",
		)

	_, kwargs = mock_playwright.call_args
	assert kwargs["debug"] is True
	assert kwargs["pause_seconds"] == 2.0
	assert kwargs["headless"] is False
	assert kwargs["artifact_dir"] == "tmp/auth"
