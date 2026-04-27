"""Optional Copart authentication helpers."""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from .config import (
    COPART_AUTH_DEBUG_DIR,
    COPART_AUTH_PROBE_URL,
    COPART_LOGIN_URL,
    COPART_PLAYWRIGHT_HEADLESS,
)


@dataclasses.dataclass
class CopartAuthResult:
    success: bool
    reason: str
    cookies: dict[str, str]


def parse_cookie_header(raw_cookie: str) -> dict[str, str]:
    """Parse `name=value; name2=value2` into a cookie dict."""
    cookies: dict[str, str] = {}
    if not raw_cookie:
        return cookies

    for part in raw_cookie.split(";"):
        item = part.strip()
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            continue
        cookies[key] = value.strip()

    return cookies


def _format_failure_reason(
    base_reason: str,
    *,
    debug_snapshot: dict[str, Any] | None = None,
    artifact_paths: dict[str, str] | None = None,
) -> str:
    parts = [base_reason]
    if debug_snapshot:
        parts.append(f"diagnostics={json.dumps(debug_snapshot, ensure_ascii=False)}")
    if artifact_paths:
        parts.append(f"artifacts={json.dumps(artifact_paths, ensure_ascii=False)}")
    return "; ".join(parts)


async def _probe_auth_with_cookies(
    session_cookies: dict[str, str],
    *,
    timeout: float,
    probe_prefix: str,
) -> CopartAuthResult:
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=timeout,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; CopartDiscoveryEngine/1.0)",
            "Accept": "application/json,text/html",
        },
    ) as client:
        client.cookies.update(session_cookies)
        probe = await client.get(COPART_AUTH_PROBE_URL)
        if probe.status_code >= 400:
            return CopartAuthResult(
                success=False,
                reason=f"{probe_prefix} auth probe failed ({probe.status_code})",
                cookies={},
            )

        try:
            body: dict[str, Any] = probe.json()
        except (json.JSONDecodeError, ValueError):
            return CopartAuthResult(
                success=False,
                reason=f"{probe_prefix} auth probe returned non-JSON response",
                cookies={},
            )

        account_info = body.get("data") if isinstance(body, dict) else None
        is_anonymous = isinstance(account_info, dict) and account_info.get("anonymous") is True
        if is_anonymous:
            return CopartAuthResult(
                success=False,
                reason=f"{probe_prefix} auth probe returned anonymous",
                cookies={},
            )

        return CopartAuthResult(success=True, reason=f"{probe_prefix} auth valid", cookies=session_cookies)


async def _authenticate_with_playwright(
    username: str,
    password: str,
    *,
    timeout: float,
    debug: bool = False,
    pause_seconds: float = 0.0,
    headless: bool | None = None,
    artifact_dir: str | None = None,
) -> CopartAuthResult:
    try:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        from playwright.async_api import async_playwright
    except Exception:
        return CopartAuthResult(
            success=False,
            reason="playwright package unavailable",
            cookies={},
        )

    timeout_ms = int(timeout * 1000)
    debug_timeout_ms = max(timeout_ms, int(max(pause_seconds, 0.0) * 1000) + 5000)
    browser = None
    page: Any | None = None
    debug_snapshot: dict[str, Any] = {}

    async def _write_failure_artifacts(page: Any) -> dict[str, str]:
        root_dir = Path(artifact_dir or COPART_AUTH_DEBUG_DIR)
        root_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        screenshot_path = root_dir / f"copart-login-{stamp}.png"
        html_path = root_dir / f"copart-login-{stamp}.html"

        paths: dict[str, str] = {}
        try:
            await page.screenshot(path=str(screenshot_path), full_page=True)
            paths["screenshot"] = str(screenshot_path)
        except Exception:
            pass

        try:
            html_path.write_text(await page.content(), encoding="utf-8")
            paths["html"] = str(html_path)
        except Exception:
            pass

        return paths

    async def _failure(page: Any, message: str) -> CopartAuthResult:
        artifacts = await _write_failure_artifacts(page) if debug else {}
        return CopartAuthResult(
            success=False,
            reason=_format_failure_reason(
                message,
                debug_snapshot=debug_snapshot if debug else None,
                artifact_paths=artifacts,
            ),
            cookies={},
        )

    async def _collect_debug_snapshot(page: Any) -> dict[str, Any]:
        snapshot: dict[str, Any] = {
            "url": page.url,
            "frames": [],
        }
        try:
            snapshot["title"] = await page.title()
        except Exception:
            snapshot["title"] = ""

        for frame in page.frames:
            frame_info: dict[str, Any] = {
                "url": frame.url,
                "inputs": [],
            }
            try:
                inputs = await frame.eval_on_selector_all(
                    "input",
                    (
                        "els => els.slice(0, 20).map(e => ("
                        "{type: e.type || '', name: e.name || '', id: e.id || '', "
                        "placeholder: e.placeholder || '', autocomplete: e.autocomplete || ''}"
                        "))"
                    ),
                )
                if isinstance(inputs, list):
                    frame_info["inputs"] = inputs
            except Exception:
                frame_info["inputs"] = []
            snapshot["frames"].append(frame_info)

        return snapshot

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=COPART_PLAYWRIGHT_HEADLESS if headless is None else headless,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()

            await page.goto(COPART_LOGIN_URL, wait_until="domcontentloaded", timeout=debug_timeout_ms)
            await page.wait_for_timeout(1500)
            if pause_seconds > 0:
                await page.wait_for_timeout(int(pause_seconds * 1000))

            debug_snapshot = await _collect_debug_snapshot(page) if debug else {}

            if "_Incapsula_Resource" in page.url:
                return await _failure(page, "playwright blocked by Incapsula anti-bot")

            for frame in page.frames:
                if "_Incapsula_Resource" in frame.url:
                    return await _failure(page, "playwright blocked by Incapsula anti-bot")

            username_selectors = [
                "input[name='username']",
                "input[name='email']",
                "#username",
                "input[type='email']",
                "input[autocomplete='username']",
                "input[id*='user']",
                "input[name*='user']",
            ]
            password_selectors = [
                "input[name='password']",
                "#password",
                "input[type='password']",
                "input[autocomplete='current-password']",
            ]

            frames = [page.main_frame, *[f for f in page.frames if f != page.main_frame]]

            username_filled = False
            for frame in frames:
                for selector in username_selectors:
                    field = frame.locator(selector).first
                    if await field.count():
                        await field.fill(username)
                        username_filled = True
                        break
                if username_filled:
                    break

            # Some Copart variants render username as an unlabeled text input.
            # Fallback: find a non-search text/email input in the same frame as a password field.
            if not username_filled:
                for frame in frames:
                    password_in_frame = frame.locator("input[type='password']").first
                    if not await password_in_frame.count():
                        continue

                    candidates = frame.locator("input")
                    candidate_count = await candidates.count()
                    for idx in range(candidate_count):
                        candidate = candidates.nth(idx)
                        raw_type = (await candidate.get_attribute("type") or "text").lower()
                        if raw_type in {"password", "hidden", "checkbox", "radio", "submit", "button"}:
                            continue

                        id_name = (
                            (await candidate.get_attribute("id") or "")
                            + " "
                            + (await candidate.get_attribute("name") or "")
                        ).lower()
                        if "search" in id_name:
                            continue

                        await candidate.fill(username)
                        username_filled = True
                        break

                    if username_filled:
                        break

            if not username_filled:
                frame_urls = " ".join(frame.url for frame in page.frames).lower()
                if "incapsula" in frame_urls:
                    return await _failure(page, "playwright blocked by Incapsula anti-bot")
                return await _failure(page, "playwright could not find username field")

            password_filled = False
            for frame in frames:
                for selector in password_selectors:
                    field = frame.locator(selector).first
                    if await field.count():
                        await field.fill(password)
                        password_filled = True
                        break
                if password_filled:
                    break
            if not password_filled:
                return await _failure(page, "playwright could not find password field")

            submit_selectors = [
                "button[type='submit']",
                "input[type='submit']",
                "button:has-text('Login')",
                "button:has-text('Sign In')",
            ]
            clicked = False
            for frame in frames:
                for selector in submit_selectors:
                    btn = frame.locator(selector).first
                    if await btn.count():
                        await btn.click()
                        clicked = True
                        break
                if clicked:
                    break

            if not clicked:
                await page.keyboard.press("Enter")

            await page.wait_for_load_state("networkidle", timeout=timeout_ms)

            debug_snapshot = await _collect_debug_snapshot(page) if debug else {}

            cookie_entries = await context.cookies("https://www.copart.com")
            cookie_map = {
                item.get("name", ""): item.get("value", "")
                for item in cookie_entries
                if item.get("name") and item.get("value")
            }
            if not cookie_map:
                return await _failure(page, "playwright login produced no cookies")

            return await _probe_auth_with_cookies(
                cookie_map,
                timeout=timeout,
                probe_prefix="playwright",
            )
    except PlaywrightTimeoutError:
        artifacts = await _write_failure_artifacts(page) if debug and page is not None else {}
        return CopartAuthResult(
            success=False,
            reason=_format_failure_reason(
                "playwright login timed out",
                debug_snapshot=debug_snapshot if debug else None,
                artifact_paths=artifacts,
            ),
            cookies={},
        )
    except Exception as exc:
        artifacts = await _write_failure_artifacts(page) if debug and page is not None else {}
        return CopartAuthResult(
            success=False,
            reason=_format_failure_reason(
                f"playwright login failed: {exc}",
                debug_snapshot=debug_snapshot if debug else None,
                artifact_paths=artifacts,
            ),
            cookies={},
        )
    finally:
        if browser is not None:
            await browser.close()


async def authenticate_copart_session(
    username: str,
    password: str,
    *,
    timeout: float = 30.0,
    playwright_debug: bool = False,
    playwright_pause_seconds: float = 0.0,
    playwright_headless: bool | None = None,
    playwright_artifact_dir: str | None = None,
) -> CopartAuthResult:
    """Attempt login and return session cookies on success.

    Strategy: browser automation via Playwright only.
    """
    if not username or not password:
        return CopartAuthResult(success=False, reason="missing credentials", cookies={})

    return await _authenticate_with_playwright(
        username,
        password,
        timeout=timeout,
        debug=playwright_debug,
        pause_seconds=playwright_pause_seconds,
        headless=playwright_headless,
        artifact_dir=playwright_artifact_dir,
    )


async def check_copart_auth_session(
    *,
    session_cookies: dict[str, str] | None = None,
    username: str = "",
    password: str = "",
    timeout: float = 30.0,
    playwright_debug: bool = False,
    playwright_pause_seconds: float = 0.0,
    playwright_headless: bool | None = None,
    playwright_artifact_dir: str | None = None,
) -> CopartAuthResult:
    """Validate whether current auth inputs can access authenticated Copart APIs."""
    if session_cookies:
        return await _probe_auth_with_cookies(
            session_cookies,
            timeout=timeout,
            probe_prefix="cookie",
        )

    if username and password:
        return await authenticate_copart_session(
            username,
            password,
            timeout=timeout,
            playwright_debug=playwright_debug,
            playwright_pause_seconds=playwright_pause_seconds,
            playwright_headless=playwright_headless,
            playwright_artifact_dir=playwright_artifact_dir,
        )

    return CopartAuthResult(success=False, reason="no auth inputs provided", cookies={})
