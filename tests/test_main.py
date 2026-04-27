"""Tests for CLI wiring in main.py."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import main


def test_parser_includes_authcheck_mode():
    parser = main._build_parser()
    args = parser.parse_args(["authcheck"])
    assert args.mode == "authcheck"
    assert args.auth_mode == "auto"
    assert args.playwright_debug is False
    assert args.playwright_pause_seconds == 0.0
    assert args.playwright_headed is False
    assert args.playwright_artifact_dir == ""


@pytest.mark.asyncio
async def test_run_authcheck_uses_cookie_mode_without_engine_startup(monkeypatch):
    # Ensure no engine context is created for authcheck mode.
    args = SimpleNamespace(
        mode="authcheck",
        auth_mode="cookies",
        playwright_debug=False,
        playwright_pause_seconds=0.0,
        playwright_headed=False,
        playwright_artifact_dir="",
    )

    with (
        patch("main.COPART_SESSION_COOKIES", "foo=bar; baz=qux"),
        patch("main.check_copart_auth_session", new=AsyncMock(return_value=SimpleNamespace(success=True, reason="ok"))) as mock_check,
        patch("main.DiscoveryEngine") as mock_engine,
    ):
        result = await main.run(args)

    assert result == []
    mock_check.assert_awaited_once()
    _, kwargs = mock_check.call_args
    assert kwargs["session_cookies"] == {"foo": "bar", "baz": "qux"}
    mock_engine.assert_not_called()


@pytest.mark.asyncio
async def test_run_authcheck_passes_playwright_debug_flags():
    args = SimpleNamespace(
        mode="authcheck",
        auth_mode="credentials",
        playwright_debug=True,
        playwright_pause_seconds=3.5,
        playwright_headed=True,
        playwright_artifact_dir="debug/auth",
    )

    with (
        patch("main.COPART_USERNAME", "user@example.com"),
        patch("main.COPART_PASSWORD", "secret"),
        patch(
            "main.check_copart_auth_session",
            new=AsyncMock(return_value=SimpleNamespace(success=False, reason="blocked")),
        ) as mock_check,
    ):
        await main.run(args)

    _, kwargs = mock_check.call_args
    assert kwargs["playwright_debug"] is True
    assert kwargs["playwright_pause_seconds"] == 3.5
    assert kwargs["playwright_headless"] is False
    assert kwargs["playwright_artifact_dir"] == "debug/auth"
