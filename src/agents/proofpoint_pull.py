"""Proofpoint SecureMessage auto-pull — web-side HTTP client.

PR-A2 (2026-05-12): Playwright runs in the scprs-scraper service, NOT
the web service. The web side became a thin HTTP client that POSTs
the portal URL + creds to `${SCRAPER_SERVICE_URL}/proofpoint/pull`
and receives base64-encoded attachment bytes back.

Architecture moved from PR-A's local-Playwright model so the web
service stays light (no 150MB Chromium). The original PR-A pattern
is preserved on the scraper side at
`services/scprs-scraper/proofpoint_browser.py`.

Public API (unchanged for the SecureMessage handler):
  - is_available() -> bool        # scraper URL + creds + flag set?
  - extract_portal_url(body)      # pull the secure-reader URL from
                                     the wrapper email HTML/text
  - pull_via_url(url, dir) -> [paths]   # HTTP-call scraper, save
                                            attachments locally
"""
from __future__ import annotations

import base64
import logging
import os
import re
import uuid
from typing import List, Optional

log = logging.getLogger("reytech.proofpoint_pull")


# ── Portal URL extractor (unchanged from PR-A) ──────────────────────────

_PORTAL_URL_PATTERNS = [
    r"https?://securereader\.proofpoint\.com[^\s\"'<>)]+",
    r"https?://[a-z0-9.-]*securemail\.[a-z0-9.-]+\.gov[^\s\"'<>)]+",
    r"https?://encrypt\.proofpoint\.com[^\s\"'<>)]+",
]


def extract_portal_url(email_body: str) -> Optional[str]:
    """Pull the Proofpoint secure-reader URL out of the wrapper email."""
    if not email_body:
        return None
    body = email_body
    for pat in _PORTAL_URL_PATTERNS:
        m = re.search(pat, body, re.IGNORECASE)
        if m:
            url = m.group(0)
            url = url.rstrip(".,;:)]>")
            url = url.replace("&amp;", "&").replace("&#x2F;", "/")
            return url
    return None


# ── Availability gate ───────────────────────────────────────────────────


def is_available() -> bool:
    """Return True iff:
      - SCRAPER_SERVICE_URL env var is set (the scraper service is reachable)
      - Both Proofpoint creds are set in secrets
      - The `ingest.proofpoint_auto_login_enabled` flag is on
    """
    if not os.environ.get("SCRAPER_SERVICE_URL"):
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
        return bool(get_flag("ingest.proofpoint_auto_login_enabled", False))
    except Exception as e:
        log.debug("proofpoint_pull flag check failed: %s", e)
        return False


# ── HTTP client to the scraper service ──────────────────────────────────


def pull_via_url(
    portal_url: str,
    download_dir: Optional[str] = None,
    timeout_s: int = 60,
) -> List[str]:
    """POST `portal_url` + creds to the scraper service. Decode the
    returned base64 attachments to local files. Return list of paths.

    Returns [] on any failure mode (network, auth, no attachments,
    timeout). The SecureMessage handler flips `needs_manual_pull` on
    empty so the operator gets the portal link instead.

    `timeout_s` is the HTTP client timeout — the scraper-side Playwright
    pull has its own ~30s default, so 60s here gives headroom for the
    full request including network + base64 transit.
    """
    if not portal_url:
        return []
    if not is_available():
        log.info("proofpoint_pull: not available (scraper URL/creds/flag)")
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

    try:
        import requests
    except ImportError:
        log.error("proofpoint_pull: requests module unavailable")
        return []

    scraper_url = os.environ["SCRAPER_SERVICE_URL"].rstrip("/")
    secret = os.environ.get("SCRAPER_SECRET", "")
    headers = {"Content-Type": "application/json"}
    if secret:
        headers["X-Scraper-Secret"] = secret

    try:
        from src.core.secrets import get_key
        email = get_key("proofpoint_email")
        password = get_key("proofpoint_password")
    except Exception as e:
        log.error("proofpoint_pull: cred fetch failed: %s", e)
        return []

    # Build the scraper URL. SCRAPER_SERVICE_URL in Railway is the bare
    # host (e.g. http://scprs-scraper.railway.internal:8001) — append the
    # endpoint path here.
    endpoint = f"{scraper_url}/proofpoint/pull"
    payload = {
        "portal_url": portal_url,
        "email": email,
        "password": password,
        "timeout_s": 30,
    }

    try:
        log.info(
            "proofpoint_pull: POSTing to scraper service (%s)", endpoint,
        )
        resp = requests.post(
            endpoint, json=payload, headers=headers, timeout=timeout_s,
        )
    except requests.RequestException as e:
        log.error("proofpoint_pull: HTTP error: %s", e)
        return []

    if resp.status_code != 200:
        log.error(
            "proofpoint_pull: scraper returned %d: %s",
            resp.status_code, resp.text[:200],
        )
        return []

    try:
        body = resp.json()
    except ValueError as e:
        log.error("proofpoint_pull: JSON decode failed: %s", e)
        return []

    if not body.get("ok"):
        log.warning(
            "proofpoint_pull: scraper reported failure: %s",
            body.get("error", "unknown"),
        )
        return []

    attachments = body.get("data") or []
    if not attachments:
        log.info("proofpoint_pull: scraper returned no attachments")
        return []

    saved: List[str] = []
    for att in attachments:
        try:
            fname = att.get("filename") or f"proofpoint_{uuid.uuid4().hex[:8]}.bin"
            content_b64 = att.get("content_b64") or ""
            if not content_b64:
                continue
            data = base64.b64decode(content_b64)
            if not data:
                continue
            # Sanitize filename to prevent path traversal — same rule
            # as the upload-preview route's safe-fn pattern.
            safe_fn = re.sub(r"[^a-zA-Z0-9._-]", "_", os.path.basename(fname))
            out_path = os.path.join(download_dir, safe_fn)
            with open(out_path, "wb") as fh:
                fh.write(data)
            saved.append(os.path.abspath(out_path))
            log.info(
                "proofpoint_pull: saved %s (%d bytes)", safe_fn, len(data),
            )
        except Exception as e:
            log.warning("proofpoint_pull: attachment decode/save failed: %s", e)

    return saved
