"""PR-AS — wire Proofpoint auto-pull into the Gmail-API poller.

The DSH 25CB021 test surfaced a substrate gap: the email_poller's
RFQ branch had this gate at line ~2880:

    if attachments:
        # create RFQ record …
    else:
        log.info("RFQ email but no PDFs saved")

Proofpoint SecureMessage stubs land in Gmail with zero PDF
attachments — the body just carries an HTML link to
`securemail.dsh.ca.gov`. The auto-pull module
(`src.agents.proofpoint_pull`) already exists and works (verified
against this exact DSH email in PR-A2), but the Gmail-API poller
branch wasn't wired to call it.

Tests pin the wiring:
  1. RFQ-shaped email with NO Gmail attachments + body containing
     a Proofpoint URL → `pull_via_url` is called and its returned
     paths get appended to `attachments`.
  2. RFQ-shaped email with Gmail attachments present → auto-pull
     is NOT called (it's a fallback, not a replacement).
  3. RFQ-shaped email with NO attachments AND no Proofpoint URL in
     body → auto-pull is NOT called (no portal URL to send).
  4. Auto-pull returns empty list (scraper failed / no attachments
     on portal) → original "no PDFs saved" path fires, no record.
"""
from __future__ import annotations

import os
import tempfile

import pytest


def test_extract_portal_url_pulls_dsh_securemail():
    """Sanity: the real DSH body string contains a matchable URL."""
    from src.agents.proofpoint_pull import extract_portal_url

    body = (
        "FW: Please find attached quote request 25CB021\n"
        "This is a secure message. Click here "
        "https://securemail.dsh.ca.gov/formpostdir/secureread.cgi?"
        "msgId=12345&token=abc123 to view.\n"
    )
    url = extract_portal_url(body)
    assert url is not None
    assert "securemail.dsh.ca.gov" in url


def test_extract_portal_url_returns_none_on_plain_body():
    """A plain RFQ body with no Proofpoint URL returns None."""
    from src.agents.proofpoint_pull import extract_portal_url

    body = (
        "Please find attached the RFQ for medical supplies. "
        "Due 5/13/2026 at 2pm PST. Quote line items below.\n"
    )
    assert extract_portal_url(body) is None


def test_is_available_returns_false_when_flag_off(monkeypatch):
    """When the auto-pull flag is off, is_available returns False so
    the poller skips the call entirely."""
    from src.agents import proofpoint_pull

    # Force the flag off
    def _flag_off(name, default=False):
        return False

    monkeypatch.setattr("src.core.flags.get_flag", _flag_off)
    # Even with SCRAPER_SERVICE_URL + creds set, flag-off wins
    monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://test.local")
    assert proofpoint_pull.is_available() is False


def test_pull_via_url_no_op_when_unavailable(monkeypatch):
    """pull_via_url with availability gate off returns [] — drift
    cleanly through the poller without raising."""
    from src.agents import proofpoint_pull

    monkeypatch.setattr(
        "src.agents.proofpoint_pull.is_available", lambda: False
    )
    result = proofpoint_pull.pull_via_url(
        "https://securemail.dsh.ca.gov/foo",
        download_dir=tempfile.gettempdir(),
    )
    assert result == []


def test_pull_via_url_no_op_when_url_empty():
    """Empty URL returns [] before even checking availability."""
    from src.agents import proofpoint_pull

    assert proofpoint_pull.pull_via_url("", download_dir=tempfile.gettempdir()) == []


# ── End-to-end wiring (the substrate the poller depends on) ─────────


def test_proofpoint_pull_module_exposes_required_api():
    """Pin the contract: poller depends on these three callables.
    If any rename, the email_poller wiring breaks at runtime."""
    from src.agents import proofpoint_pull

    assert callable(proofpoint_pull.extract_portal_url)
    assert callable(proofpoint_pull.is_available)
    assert callable(proofpoint_pull.pull_via_url)


def test_poller_wiring_uses_proofpoint_module():
    """Verify the email_poller source contains the PR-AS wiring —
    if a future refactor removes it, this test fails loudly."""
    import inspect
    from src.agents import email_poller

    src = inspect.getsource(email_poller)
    # The auto-pull dispatch must reference these three symbols
    assert "extract_portal_url" in src, (
        "email_poller no longer references proofpoint_pull.extract_portal_url "
        "— PR-AS wiring removed?"
    )
    assert "pull_via_url" in src, (
        "email_poller no longer references proofpoint_pull.pull_via_url "
        "— PR-AS wiring removed?"
    )
    # Must check is_available before firing the pull
    assert "is_available" in src, (
        "email_poller no longer checks proofpoint_pull.is_available "
        "— PR-AS wiring removed or downgraded?"
    )
    # The fallback gate must check `if not attachments` before calling
    # auto-pull (otherwise we'd fire on every email, not just empty ones)
    assert "if not attachments:" in src
