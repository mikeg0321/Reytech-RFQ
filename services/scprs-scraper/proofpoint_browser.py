"""Proofpoint SecureMessage auto-pull — scraper-side implementation.

Runs in the scprs-scraper service (Playwright + Chromium pre-installed).
The web service calls `/proofpoint/pull` over the private Railway
network; this module handles the actual browser automation.

Returns attachments as base64-encoded bytes alongside their filenames
so the web service can decode + persist locally.

Mirrors the original `src/agents/proofpoint_pull.py` async core. The
web side became an HTTP client; the browser work lives here where
Chromium is already in the image.
"""
from __future__ import annotations

import base64
import logging
import os
import uuid
from typing import Dict, List

log = logging.getLogger("proofpoint_browser")


def _profile_dir() -> str:
    """Persistent browser profile so the Proofpoint session cookie
    survives across pulls. Lives under the scraper service's working
    directory (no shared volume yet — first-pull-per-restart will
    re-login). Mike's expected frequency is ~3 logins/month so the
    re-login cost is negligible."""
    base = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "proofpoint_profile",
    )
    os.makedirs(base, exist_ok=True)
    return base


async def _pull_async(
    portal_url: str,
    email: str,
    password: str,
    timeout_s: int = 30,
) -> List[Dict[str, str]]:
    """Async core: launch persistent-context Chromium, log in, download
    attachments. Returns a list of `{filename, content_b64}` dicts.

    Every failure returns []. The caller (HTTP endpoint) wraps the
    response and the web-side client treats empty as "manual-pull".
    """
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    if not portal_url or not email or not password:
        log.warning("proofpoint_browser: missing portal_url / email / password")
        return []

    ms = int(timeout_s * 1000)
    out: List[Dict[str, str]] = []
    download_tmp = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "tmp_downloads",
    )
    os.makedirs(download_tmp, exist_ok=True)

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

            log.info("proofpoint_browser: nav to %s", portal_url[:80])
            await page.goto(portal_url, wait_until="domcontentloaded", timeout=ms)

            # ── Login (skipped when persistent session is fresh) ──
            # Real DSH portal uses PrimeFaces with name='dialog:username'
            # and 'dialog:password' (NOT the generic 'email' / 'password'
            # names). Calibrated 2026-05-12 against
            # securemail.dsh.ca.gov/formpostdir/securereader — title
            # "Encrypted Email Login", form action="/securereader/login.jsf".
            # The portal pre-fills the username with the recipient's
            # email when arriving from a wrapper email link, so we
            # only really need to type the password.
            try:
                email_input = await page.wait_for_selector(
                    "input[name='email'], input[type='email'], input#email, "
                    "input[name='dialog:username'], input[name*='username'], "
                    "input[id*='username']",
                    timeout=5000,
                )
                if email_input is not None:
                    log.info("proofpoint_browser: login form present")
                    # Skip the email fill when the field is pre-filled
                    # (DSH portal does this when arriving from a wrapper
                    # email link) OR when it's read-only/disabled — both
                    # cases would otherwise time out for 30s on
                    # `.fill()` waiting for "element to be enabled".
                    # Diagnostic capture against the real DSH portal
                    # 2026-05-12 confirmed dialog:username is locked.
                    try:
                        existing = (await email_input.get_attribute("value")) or ""
                        readonly = await email_input.get_attribute("readonly")
                        disabled = await email_input.is_disabled()
                        if existing.strip() and (readonly is not None or disabled):
                            log.info(
                                "proofpoint_browser: username pre-filled + locked "
                                "(value=%r, readonly=%s, disabled=%s) — skipping fill",
                                existing[:40], readonly is not None, disabled,
                            )
                        else:
                            await email_input.fill(email)
                    except Exception as _fe:
                        log.debug("email fill suppressed: %s", _fe)
                    # Some deployments use a 2-step email-then-password
                    # flow; try the continue button first.
                    try:
                        cont = await page.wait_for_selector(
                            "button:has-text('Continue'), "
                            "button:has-text('Next'), "
                            "input[type='submit'][value*='Continue']",
                            timeout=2000,
                        )
                        if cont:
                            # Only click if password field NOT visible
                            # yet — otherwise it's a single-step form
                            # and clicking submit too early posts an
                            # empty password.
                            pw_test = await page.query_selector(
                                "input[type='password']"
                            )
                            if not pw_test:
                                await cont.click()
                    except PWTimeout:
                        pass
                    pw_input = await page.wait_for_selector(
                        "input[name='password'], input[type='password'], "
                        "input[name='dialog:password']",
                        timeout=ms,
                    )
                    await pw_input.fill(password)
                    # Submit-selector order matters — try the SPECIFIC
                    # DSH continue button first. Generic
                    # `input[type='submit']` matches multiple elements
                    # on the DSH page (the first being a HIDDEN Log Out
                    # button), causing `wait_for_selector` to lock onto
                    # the wrong one. Diagnostic capture 2026-05-12
                    # confirmed three submits exist: pfptEndSessionBtn
                    # (Log Out, hidden), pfptContinueSessionBtn (also
                    # hidden), and dialog:continueButton (the real one).
                    submit = await page.wait_for_selector(
                        "input[name='dialog:continueButton'], "
                        "button:has-text('Read Message'), "
                        "button:has-text('Sign In'), "
                        "button:has-text('Continue'), "
                        "button[type='submit']:visible, "
                        "input[type='submit']:visible",
                        timeout=ms,
                    )
                    await submit.click()
                    await page.wait_for_load_state("networkidle", timeout=ms)
            except PWTimeout:
                log.info("proofpoint_browser: no login form, session warm")

            # ── Enumerate + download attachments ──
            # Real DSH Proofpoint markup (calibrated 2026-05-12 via the
            # /proofpoint/inspect diagnostic): the wrapper email's
            # secure inbox auto-opens the message, rendering attachment
            # links inline as:
            #
            #   <span class="header-attachment-item">
            #     <a href="#" onclick="mojarra.jsfcljs(...,'_blank');return false">
            #       <img src="images/fileicons/pdf.gif">RFQ 25CB021.pdf
            #     </a>
            #   </span>
            #
            # The onclick fires a JSF postback that either streams a
            # download directly OR opens a `_blank` popup carrying the
            # bytes. We handle both via expect_download with a popup
            # fallback. Other selectors stay as fallbacks for non-DSH
            # Proofpoint deployments.
            selectors = [
                "span.header-attachment-item a",  # DSH/Proofpoint reader
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
                            "proofpoint_browser: %d attachment(s) via %r",
                            len(els), sel,
                        )
                        break
                except Exception as _se:
                    log.debug("selector %r failed: %s", sel, _se)
            if not elements:
                log.warning("proofpoint_browser: no attachments detected")
                return []

            # Dedupe by visible filename — DSH inbox renders the same
            # attachment 4x in the header strip (likely a rendering
            # quirk for multi-part MIME). Without dedupe we'd download
            # the same bytes multiple times.
            seen_filenames: set = set()
            unique_elements = []
            for el in elements:
                try:
                    name = (await el.inner_text()).strip()
                except Exception:
                    name = ""
                if name and name in seen_filenames:
                    continue
                if name:
                    seen_filenames.add(name)
                unique_elements.append((name, el))
            log.info(
                "proofpoint_browser: %d unique attachment(s) after dedupe",
                len(unique_elements),
            )

            for idx, (fname_hint, el) in enumerate(unique_elements):
                download = None
                try:
                    # JSF postback may stream the download directly OR
                    # open a `_blank` popup that carries it.
                    try:
                        async with page.expect_download(timeout=10000) as dl_info:
                            await el.click()
                        download = await dl_info.value
                    except PWTimeout:
                        # Fall back to popup capture — some JSF setups
                        # route the bytes through a new window.
                        try:
                            async with page.expect_popup(timeout=5000) as popup_info:
                                await el.click()
                            popup = await popup_info.value
                            try:
                                async with popup.expect_download(timeout=ms) as dl_info:
                                    pass  # download in flight
                                download = await dl_info.value
                            finally:
                                try:
                                    await popup.close()
                                except Exception:
                                    pass
                        except Exception as _pe:
                            log.warning(
                                "proofpoint_browser: popup fallback failed for #%d (%s): %s",
                                idx, fname_hint, _pe,
                            )
                            continue
                    if download is None:
                        continue
                    suggested = (
                        download.suggested_filename
                        or fname_hint
                        or f"proofpoint_attachment_{idx}_{uuid.uuid4().hex[:8]}.bin"
                    )
                    out_path = os.path.join(download_tmp, suggested)
                    await download.save_as(out_path)
                    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                        with open(out_path, "rb") as fh:
                            data = fh.read()
                        out.append({
                            "filename": suggested,
                            "content_b64": base64.b64encode(data).decode("ascii"),
                            "size": len(data),
                        })
                        log.info(
                            "proofpoint_browser: downloaded %s (%d bytes)",
                            suggested, len(data),
                        )
                        try:
                            os.unlink(out_path)
                        except Exception:
                            pass
                except PWTimeout:
                    log.warning(
                        "proofpoint_browser: download #%d (%s) timed out",
                        idx, fname_hint,
                    )
                except Exception as e:
                    log.warning(
                        "proofpoint_browser: download #%d (%s) failed: %s",
                        idx, fname_hint, e,
                    )
        finally:
            try:
                await context.close()
            except Exception as _ce:
                log.debug("context close suppressed: %s", _ce)

    return out


def pull(
    portal_url: str,
    email: str,
    password: str,
    timeout_s: int = 30,
) -> List[Dict[str, str]]:
    """Sync wrapper used by the HTTP endpoint. Returns [] on any error."""
    import asyncio
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
                    _pull_async(portal_url, email, password, timeout_s=timeout_s)
                )
            finally:
                loop.close()
        return asyncio.run(
            _pull_async(portal_url, email, password, timeout_s=timeout_s)
        )
    except Exception as e:
        log.error("proofpoint_browser.pull error: %s", e, exc_info=True)
        return []
