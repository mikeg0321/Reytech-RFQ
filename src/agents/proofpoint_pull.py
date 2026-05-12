"""Proofpoint SecureMessage auto-pull — PR-A Step 7 (2026-05-11).

Auto-login to the Proofpoint Encryption portal and download the
encrypted RFQ attachments so the ingest pipeline can run its normal
Vision-primary parser pass against the real PDF.

The wrapper email (classified by `SHAPE_PROOFPOINT_SECUREMESSAGE` from
PR-A Step 6) carries a portal URL like:
  https://securereader.proofpoint.com/?u=<base64>&...

Public API:
  - is_available() -> bool        # creds + Playwright installed?
  - extract_portal_url(body)      # pull the secure-reader URL from
                                     the wrapper email HTML/text
  - pull_via_url(url, dir) -> [paths]   # auto-login + download

Caller (ingest_pipeline._dispatch_parser SecureMessage handler, Step 8)
falls back to `needs_manual_pull` status when this returns no files.

Design choices:
  - Persistent browser context lives under `data/proofpoint_profile/`
    so the Proofpoint session cookie survives across pulls. Mike's
    expected frequency is ~3 logins/month, but persistent state still
    helps when a single RFQ has multiple wrapper emails.
  - Every selector is broad (multiple fallback candidates) because
    Proofpoint occasionally tweaks the portal markup. Strict
    selectors would silently break the pull on a vendor UI update.
  - Hard 30-second timeout per stage so a hung portal can't block
    the ingest pipeline indefinitely. On timeout we return [] and
    the caller flips the record to `needs_manual_pull`.
  - Auto-login is gated by the `ingest.proofpoint_auto_login_enabled`
    flag (default off) AND the presence of both creds — fully
    opt-in so accidental Playwright execution can't happen.
"""
from __future__ import annotations

import logging
import os
import re
import uuid
from typing import List, Optional

log = logging.getLogger("reytech.proofpoint_pull")


# ── Module-cached Playwright availability check ──────────────────────────
# Mirrors the scprs_browser pattern: don't re-import playwright every call
# on containers where it isn't installed.

_PLAYWRIGHT_AVAILABLE: "Optional[bool]" = None


def _playwright_available() -> bool:
    global _PLAYWRIGHT_AVAILABLE
    if _PLAYWRIGHT_AVAILABLE is None:
        try:
            import playwright.async_api  # noqa: F401
            _PLAYWRIGHT_AVAILABLE = True
        except ImportError:
            _PLAYWRIGHT_AVAILABLE = False
            log.info(
                "playwright not installed — Proofpoint auto-pull disabled "
                "for this boot. Ingest will mark SecureMessage records as "
                "needs_manual_pull until creds + playwright are present."
            )
    return _PLAYWRIGHT_AVAILABLE


# ── Portal URL extractor ────────────────────────────────────────────────

# Proofpoint secure-reader URLs. The portal host varies by deployment
# (securereader.proofpoint.com is the public default; some agencies
# run a tenant-branded subdomain). We accept the canonical host AND
# any `securemail.<agency>.gov` variant we've seen in the wild.
_PORTAL_URL_PATTERNS = [
    r"https?://securereader\.proofpoint\.com[^\s\"'<>)]+",
    r"https?://[a-z0-9.-]*securemail\.[a-z0-9.-]+\.gov[^\s\"'<>)]+",
    r"https?://encrypt\.proofpoint\.com[^\s\"'<>)]+",
]


def extract_portal_url(email_body: str) -> Optional[str]:
    """Pull the Proofpoint secure-reader URL out of the wrapper email.

    Returns the first match (Proofpoint emails carry exactly one). The
    URL is unescaped from common HTML entities (`&amp;` → `&`) because
    Gmail's HTML body often passes through `&amp;` in the href that
    would otherwise break the query string when handed to Chromium.
    """
    if not email_body:
        return None
    body = email_body
    for pat in _PORTAL_URL_PATTERNS:
        m = re.search(pat, body, re.IGNORECASE)
        if m:
            url = m.group(0)
            # Strip trailing punctuation that often clings to URLs in
            # plain-text bodies ("...click here: <url>." → drop the dot).
            url = url.rstrip(".,;:)]>")
            # Common HTML entity decode — keep it tiny, don't import
            # html.unescape over a 6-char tweak.
            url = url.replace("&amp;", "&").replace("&#x2F;", "/")
            return url
    return None


# ── Credential gate ──────────────────────────────────────────────────────


def is_available() -> bool:
    """Return True iff:
      - playwright is importable
      - both Proofpoint creds are set in secrets
      - the auto-login feature flag is on

    Conservative gate — any False keeps the SecureMessage path on the
    manual-pull fallback. Mike's "opt-in" preference for auto-login
    (frequency ≈ 3/month, low blast radius) is the reason the flag
    is layered on top of cred presence: even after creds land in
    Railway env vars, the flag stays off until Mike flips it.
    """
    if not _playwright_available():
        return False
    try:
        from src.core.secrets import get_key
        if not get_key("proofpoint_email") or not get_key("proofpoint_password"):
            return False
    except Exception as e:
        log.debug("proofpoint_pull secrets check failed: %s", e)
        return False
    try:
        from src.core.flags import get_flag
        # Default False — auto-pull stays dark until explicitly enabled.
        return bool(get_flag("ingest.proofpoint_auto_login_enabled", False))
    except Exception as e:
        log.debug("proofpoint_pull flag check failed: %s", e)
        return False


# ── Public sync wrapper around the async pull ───────────────────────────


def pull_via_url(
    portal_url: str,
    download_dir: Optional[str] = None,
    timeout_s: int = 30,
) -> List[str]:
    """Auto-login to a Proofpoint portal URL and download every
    attachment. Returns list of local file paths.

    Returns [] on any failure (network, auth, no attachments, timeout) —
    the caller flips the record to `needs_manual_pull` so the operator
    knows to do it by hand. No exceptions propagate; every failure is
    a log entry.
    """
    if not portal_url:
        return []
    if not is_available():
        log.info("proofpoint_pull: not available (creds/flag/playwright)")
        return []

    if download_dir is None:
        download_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "..", "data", "proofpoint_downloads",
        )
    try:
        os.makedirs(download_dir, exist_ok=True)
    except Exception as e:
        log.warning("proofpoint_pull: download_dir create failed: %s", e)
        return []

    import asyncio
    # Handle the "called from inside an existing event loop" case
    # (e.g. Flask async adapter) by detecting it BEFORE asyncio.run
    # raises — otherwise we can't distinguish the nested-loop
    # RuntimeError from a real async-fn RuntimeError, and would
    # silently re-run the failing pull.
    try:
        asyncio.get_running_loop()
        in_loop = True
    except RuntimeError:
        in_loop = False

    try:
        if in_loop:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(
                    _pull_async(portal_url, download_dir, timeout_s=timeout_s)
                )
            finally:
                loop.close()
        return asyncio.run(
            _pull_async(portal_url, download_dir, timeout_s=timeout_s)
        )
    except Exception as e:
        log.error("proofpoint_pull: top-level error: %s", e, exc_info=True)
        return []


def _profile_dir() -> str:
    """Persistent browser profile so the Proofpoint session cookie
    survives across pulls. Same pattern as WolfPack's persistent
    Chrome profile."""
    base = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "..", "data", "proofpoint_profile",
    )
    os.makedirs(base, exist_ok=True)
    return base


async def _pull_async(
    portal_url: str,
    download_dir: str,
    timeout_s: int = 30,
) -> List[str]:
    """Async core: launch persistent-context Chromium, log in, download
    attachments. Returns absolute paths of downloaded files."""
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    from src.core.secrets import get_key

    email = get_key("proofpoint_email")
    password = get_key("proofpoint_password")
    if not email or not password:
        log.warning("proofpoint_pull: creds missing at async call time")
        return []

    downloaded: List[str] = []
    ms = int(timeout_s * 1000)

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            _profile_dir(),
            headless=True,
            accept_downloads=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        try:
            page = await context.new_page()
            page.set_default_timeout(ms)

            log.info("proofpoint_pull: navigating to %s", portal_url[:80])
            await page.goto(portal_url, wait_until="domcontentloaded", timeout=ms)

            # ── Login step (skipped when persistent session is fresh) ──
            try:
                email_input = await page.wait_for_selector(
                    "input[name='email'], input[type='email'], input#email",
                    timeout=5000,
                )
                if email_input is not None:
                    log.info("proofpoint_pull: login form present, signing in")
                    await email_input.fill(email)
                    # Continue / Next button (some Proofpoint deployments
                    # show a two-step email-then-password flow).
                    try:
                        cont = await page.wait_for_selector(
                            "button:has-text('Continue'), "
                            "button:has-text('Next'), "
                            "input[type='submit'][value*='Continue']",
                            timeout=2000,
                        )
                        if cont:
                            await cont.click()
                    except PWTimeout:
                        pass  # single-step form — password input is already visible
                    pw_input = await page.wait_for_selector(
                        "input[name='password'], input[type='password']",
                        timeout=ms,
                    )
                    await pw_input.fill(password)
                    submit = await page.wait_for_selector(
                        "button[type='submit'], input[type='submit'], "
                        "button:has-text('Sign In'), button:has-text('Continue'), "
                        "button:has-text('Read Message')",
                        timeout=ms,
                    )
                    await submit.click()
                    await page.wait_for_load_state("networkidle", timeout=ms)
            except PWTimeout:
                # No login form → session already authenticated; proceed.
                log.info("proofpoint_pull: no login form, session likely warm")

            # ── Enumerate + download attachments ──
            # Proofpoint's reader UI renders attachments as either:
            #   <a class="attachment-link" href="...">filename.pdf</a>
            #   <a download="filename.pdf" href="/download/...">filename.pdf</a>
            #   <button data-attachment-id="..."> ... </button>
            # We try the most-specific selector first, then broaden.
            selectors = [
                "a.attachment-link",
                "a[download]",
                "a[href*='/attachment/']",
                "a[href*='/download/']",
                "button[data-attachment-id]",
            ]
            elements = []
            for sel in selectors:
                try:
                    els = await page.query_selector_all(sel)
                    if els:
                        elements = els
                        log.info(
                            "proofpoint_pull: %d attachment(s) via selector %r",
                            len(els), sel,
                        )
                        break
                except Exception as _se:
                    log.debug("selector %r failed: %s", sel, _se)
            if not elements:
                log.warning(
                    "proofpoint_pull: no attachments detected on portal page"
                )
                return []

            for idx, el in enumerate(elements):
                try:
                    async with page.expect_download(timeout=ms) as dl_info:
                        await el.click()
                    download = await dl_info.value
                    suggested = (
                        download.suggested_filename
                        or f"proofpoint_attachment_{idx}_{uuid.uuid4().hex[:8]}.bin"
                    )
                    out_path = os.path.join(download_dir, suggested)
                    await download.save_as(out_path)
                    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                        downloaded.append(os.path.abspath(out_path))
                        log.info(
                            "proofpoint_pull: downloaded %s (%d bytes)",
                            suggested, os.path.getsize(out_path),
                        )
                except PWTimeout:
                    log.warning(
                        "proofpoint_pull: download #%d timed out", idx,
                    )
                except Exception as e:
                    log.warning(
                        "proofpoint_pull: download #%d failed: %s", idx, e,
                    )
        finally:
            try:
                await context.close()
            except Exception as _ce:
                log.debug("context close suppressed: %s", _ce)

    return downloaded
