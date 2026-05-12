"""Proofpoint portal diagnostic capture — supports auto-pull v2 dev.

Same login flow as `proofpoint_browser._pull_async` but with a step-by-
step capture pass: at each stage (post-nav, post-fill, post-submit,
attachment-scan) we record the URL, title, HTML head, DOM inputs/
buttons/links/forms, and a base64-encoded screenshot.

Returns the full step list so a developer can curl this endpoint
(or the web-side helper) and see EXACTLY what the page looks like
at each step. Cuts iteration cost from ~3-5 min (code+deploy+test)
to ~seconds (curl + render).

Use a FRESH (non-persistent) browser context each call so cookies
from prior runs don't muddy the diagnostic.
"""
from __future__ import annotations

import base64
import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

log = logging.getLogger("proofpoint_diag")


async def _snapshot(page, name: str) -> Dict[str, Any]:
    """Capture one step's worth of diagnostic state from a Playwright page."""
    step: Dict[str, Any] = {"name": name}
    try:
        step["url"] = page.url
    except Exception as e:
        step["url_error"] = str(e)
    try:
        step["title"] = await page.title()
    except Exception as e:
        step["title_error"] = str(e)
    try:
        html = await page.content()
        step["html_head"] = html[:5000]
        step["html_len"] = len(html)
    except Exception as e:
        step["html_error"] = str(e)
    # Screenshot — PNG, full page, base64-encoded.
    try:
        png_bytes = await page.screenshot(full_page=True, timeout=10000)
        step["screenshot_b64"] = base64.b64encode(png_bytes).decode("ascii")
        step["screenshot_size"] = len(png_bytes)
    except Exception as e:
        step["screenshot_error"] = str(e)

    # Inputs — enumerate every <input> on the page.
    inputs: List[Dict[str, Any]] = []
    try:
        for inp in (await page.query_selector_all("input"))[:50]:
            try:
                inputs.append({
                    "type": await inp.get_attribute("type") or "",
                    "name": await inp.get_attribute("name") or "",
                    "id": await inp.get_attribute("id") or "",
                    "placeholder": await inp.get_attribute("placeholder") or "",
                    "value_preview": (await inp.get_attribute("value") or "")[:80],
                    "visible": await inp.is_visible(),
                })
            except Exception as e:
                inputs.append({"error": str(e)})
    except Exception as e:
        step["inputs_error"] = str(e)
    step["inputs"] = inputs

    # Buttons + submits.
    buttons: List[Dict[str, Any]] = []
    try:
        for btn in (await page.query_selector_all("button, input[type=submit]"))[:30]:
            try:
                txt = (await btn.inner_text())[:120].strip()
                buttons.append({
                    "text": txt,
                    "name": await btn.get_attribute("name") or "",
                    "type": await btn.get_attribute("type") or "",
                    "id": await btn.get_attribute("id") or "",
                    "visible": await btn.is_visible(),
                })
            except Exception as e:
                buttons.append({"error": str(e)})
    except Exception as e:
        step["buttons_error"] = str(e)
    step["buttons"] = buttons

    # Forms — action / method / number of fields.
    forms: List[Dict[str, Any]] = []
    try:
        for f in (await page.query_selector_all("form"))[:10]:
            try:
                forms.append({
                    "action": await f.get_attribute("action") or "",
                    "method": (await f.get_attribute("method") or "").upper(),
                    "id": await f.get_attribute("id") or "",
                })
            except Exception as e:
                forms.append({"error": str(e)})
    except Exception as e:
        step["forms_error"] = str(e)
    step["forms"] = forms

    # Links — first 30 (text + href). Useful for finding "download
    # attachment" anchors on the post-login message page.
    links: List[Dict[str, Any]] = []
    try:
        for lnk in (await page.query_selector_all("a"))[:30]:
            try:
                href = await lnk.get_attribute("href") or ""
                if not href or href == "#":
                    continue
                txt = (await lnk.inner_text())[:120].strip()
                links.append({
                    "href": href[:200],
                    "text": txt,
                    "download": await lnk.get_attribute("download") or "",
                })
            except Exception as e:
                links.append({"error": str(e)})
    except Exception as e:
        step["links_error"] = str(e)
    step["links"] = links

    return step


async def inspect_portal_async(
    portal_url: str,
    email: str,
    password: str,
    timeout_s: int = 30,
    persist_cookies: bool = False,
) -> Dict[str, Any]:
    """Walk the auto-pull flow capturing diagnostics at each step.

    Steps:
      1. nav         — landed on portal URL
      2. post_login  — after submitting login form (if found)
      3. attachments — after attempting to enumerate attachment links

    Login uses the same selector list as the production `_pull_async`
    so this diagnostic reproduces the same code path. If you find
    selectors that work via this endpoint, copy them straight into
    `proofpoint_browser._pull_async`.
    """
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    result: Dict[str, Any] = {
        "portal_url": portal_url,
        "email_used": email,
        "persist_cookies": persist_cookies,
        "steps": [],
        "login_attempted": False,
        "login_form_detected": False,
        "submit_clicked": False,
        "errors": [],
    }
    ms = int(timeout_s * 1000)

    profile_dir = None
    if persist_cookies:
        profile_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "proofpoint_profile",
        )
        os.makedirs(profile_dir, exist_ok=True)
    else:
        profile_dir = tempfile.mkdtemp(prefix="pp_diag_")

    async with async_playwright() as p:
        if persist_cookies:
            ctx = await p.chromium.launch_persistent_context(
                profile_dir,
                headless=True,
                accept_downloads=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
        else:
            # Fresh ephemeral context — no carryover from prior runs.
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = await browser.new_context(accept_downloads=True)

        try:
            page = await ctx.new_page()
            page.set_default_timeout(ms)

            # ── Step 1: nav ──
            try:
                await page.goto(portal_url, wait_until="domcontentloaded", timeout=ms)
                # Don't wait_for_load_state here — PrimeFaces never goes idle.
            except Exception as e:
                result["errors"].append(f"nav: {e}")
            result["steps"].append(await _snapshot(page, "nav"))

            # ── Step 2: login attempt ──
            # Use the same broadened selector list as _pull_async so the
            # diagnostic mirrors prod behavior. If the form isn't found
            # here, it won't be found in prod either.
            try:
                email_input = await page.wait_for_selector(
                    "input[name='email'], input[type='email'], input#email, "
                    "input[name='dialog:username'], input[name*='username'], "
                    "input[id*='username']",
                    timeout=5000,
                )
            except PWTimeout:
                email_input = None
            if email_input:
                result["login_form_detected"] = True
                result["login_attempted"] = True
                # Skip the email fill if the field is pre-filled AND
                # locked (DSH portal pattern). Mirrors the prod logic
                # in proofpoint_browser._pull_async.
                try:
                    existing = (await email_input.get_attribute("value")) or ""
                    readonly = await email_input.get_attribute("readonly")
                    disabled = await email_input.is_disabled()
                    if existing.strip() and (readonly is not None or disabled):
                        result["username_pre_filled"] = True
                    else:
                        await email_input.fill(email)
                except Exception as e:
                    result["errors"].append(f"email_fill: {e}")
                try:
                    pw_input = await page.wait_for_selector(
                        "input[name='password'], input[type='password'], "
                        "input[name='dialog:password']",
                        timeout=ms,
                    )
                    await pw_input.fill(password)
                except Exception as e:
                    result["errors"].append(f"password_fill: {e}")
                try:
                    # Specific selectors first so we don't match the
                    # hidden Log Out / Continue-Session buttons that
                    # also satisfy `input[type='submit']`.
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
                    result["submit_clicked"] = True
                    # Wait for navigation OR network response — don't use
                    # networkidle (PrimeFaces poll never stops). Give it
                    # 10s to settle, then snapshot regardless.
                    try:
                        await page.wait_for_load_state(
                            "domcontentloaded", timeout=10000,
                        )
                    except PWTimeout:
                        pass
                except Exception as e:
                    result["errors"].append(f"submit: {e}")
            result["steps"].append(await _snapshot(page, "post_login"))

            # ── Step 3: attachment scan ──
            attach_step: Dict[str, Any] = {"name": "attachments"}
            selectors_tried = []
            for sel in [
                "a.attachment-link",
                "a[download]",
                "a[href*='/attachment/']",
                "a[href*='/download/']",
                "a[href*='attachmentDownload']",
                "a[href*='downloadattachment']",
                "button[data-attachment-id]",
                "tr.attachment a",
                ".pfpt-attachment a",
                "li.attachment a",
            ]:
                try:
                    n = len(await page.query_selector_all(sel))
                    selectors_tried.append({"selector": sel, "count": n})
                except Exception as e:
                    selectors_tried.append({"selector": sel, "error": str(e)})
            attach_step["selectors_tried"] = selectors_tried
            # Plus a full snapshot of the current page so we can see what
            # attachment markup actually exists.
            attach_step.update(await _snapshot(page, "attachments"))
            result["steps"].append(attach_step)
        finally:
            try:
                await ctx.close()
            except Exception:
                pass
            if not persist_cookies:
                try:
                    await browser.close()  # noqa: F821 — defined when not persist
                except Exception:
                    pass

    return result


def inspect_portal(
    portal_url: str,
    email: str,
    password: str,
    timeout_s: int = 30,
    persist_cookies: bool = False,
) -> Dict[str, Any]:
    """Sync wrapper for the HTTP endpoint."""
    import asyncio
    try:
        asyncio.get_running_loop()
        in_loop = True
    except RuntimeError:
        in_loop = False
    if in_loop:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                inspect_portal_async(
                    portal_url, email, password,
                    timeout_s=timeout_s, persist_cookies=persist_cookies,
                )
            )
        finally:
            loop.close()
    return asyncio.run(
        inspect_portal_async(
            portal_url, email, password,
            timeout_s=timeout_s, persist_cookies=persist_cookies,
        )
    )
