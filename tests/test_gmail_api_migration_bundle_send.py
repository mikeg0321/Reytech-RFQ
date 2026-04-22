"""Regression: PC bundle-send migrated off smtplib.SMTP_SSL to gmail_api.

Why this migration matters:
  - smtplib.SMTP_SSL relies on GMAIL_ADDRESS + GMAIL_PASSWORD (app password).
    Google deprecates app passwords in stages — a silent deliverability
    regression is the failure mode.
  - The inbound side was migrated to Gmail API OAuth 2026-04-21 (IMAP ripped
    out entirely). Leaving outbound on SMTP means two auth stories in one
    codebase and a smoke hole where the outbound path breaks without the
    inbound-focused smoke catching it.
  - Gmail API send returns a message_id + thread_id on success so silent
    failures are structurally impossible — compare to smtplib.sendmail
    returning an empty dict on "sent to all recipients" even when the message
    never leaves the relay.

This is the first of the 6 remaining outbound smtplib sites. Migrating
one at a time lets us catch scope-grant / deliverability issues at a
single blast radius before touching the others.

Prior migration reference: routes_analytics.py IN-5 (send_quote_email).
Same pattern applied here.
"""
from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent


def _read(rel: str) -> str:
    return (_REPO / rel).read_text(encoding="utf-8")


# ── Migration guards on routes_pricecheck_gen.py api_bundle_send ──

def _strip_comments_and_docstrings(src: str) -> str:
    """Drop full-line comments + triple-quoted blocks so the regression
    guards match actual code, not explanatory text."""
    import re
    # Drop triple-quoted blocks (both single + double quoted)
    src = re.sub(r'"""[\s\S]*?"""', "", src)
    src = re.sub(r"'''[\s\S]*?'''", "", src)
    # Drop full-line comments (strip leading whitespace + # ...)
    out_lines = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        # Drop trailing inline comments too — split on first unquoted #
        # (approximation: just split on " # " to avoid string content).
        if " # " in line:
            line = line.split(" # ", 1)[0]
        out_lines.append(line)
    return "\n".join(out_lines)


def test_bundle_send_no_smtplib_smtp_ssl():
    """The smtplib.SMTP_SSL call is gone — no regression back to app-password."""
    body = _strip_comments_and_docstrings(
        _read("src/api/modules/routes_pricecheck_gen.py")
    )
    assert "smtplib.SMTP_SSL(" not in body, (
        "Gmail API regression: smtplib.SMTP_SSL( call is back in "
        "routes_pricecheck_gen.py — bundle send should route through "
        "src.core.gmail_api like the IN-5 send_quote_email migration."
    )


def test_bundle_send_no_smtplib_import():
    body = _strip_comments_and_docstrings(
        _read("src/api/modules/routes_pricecheck_gen.py")
    )
    assert "import smtplib" not in body, (
        "Gmail API regression: raw `import smtplib` is back — migration "
        "should have removed both the import and the send call."
    )


def test_bundle_send_no_gmail_password_dependency():
    """GMAIL_PASSWORD is the app-password env var. OAuth migration doesn't
    need it, so os.environ.get of it is a regression."""
    body = _strip_comments_and_docstrings(
        _read("src/api/modules/routes_pricecheck_gen.py")
    )
    assert 'os.environ.get("GMAIL_PASSWORD"' not in body, (
        "Gmail API regression: GMAIL_PASSWORD env var is being read in "
        "routes_pricecheck_gen.py — OAuth migration must not fall back "
        "to app-password auth."
    )


def test_bundle_send_uses_gmail_api_send_message():
    body = _read("src/api/modules/routes_pricecheck_gen.py")
    assert "gmail_api.send_message(" in body, (
        "Gmail API regression: gmail_api.send_message call missing from "
        "routes_pricecheck_gen.py — bundle send is the only send path "
        "in this module, so this is the only place to hook it."
    )


def test_bundle_send_checks_gmail_api_is_configured():
    """Fail-fast when OAuth credentials are missing, rather than letting
    the send call crash deep in googleapiclient."""
    body = _read("src/api/modules/routes_pricecheck_gen.py")
    assert "gmail_api.is_configured()" in body, (
        "Gmail API regression: missing is_configured() guard. Without it, "
        "an unconfigured prod will 500 from inside googleapiclient "
        "instead of returning a clean 400 with an actionable message."
    )


def test_bundle_send_preserves_reytech_attachment_name():
    """The buyer sees the PDF filename from Content-Disposition. gmail_api
    derives it from os.path.basename of the attachment path — so the
    migration must copy bundle_pdf to a temp file with the Reytech-branded
    attach_name first, or the buyer sees an internal bundle_<id>.pdf name."""
    body = _read("src/api/modules/routes_pricecheck_gen.py")
    # We look for the copy-to-named-temp pattern.
    assert "named_pdf" in body and "shutil.copy" in body, (
        "Gmail API regression: bundle PDF is no longer copied to a "
        "temp file with the Reytech-branded filename before send. "
        "Buyer will receive the attachment with the internal filename."
    )


def test_bundle_send_cleans_up_temp_dir():
    """Temp dirs must be cleaned up even if send raises — otherwise prod
    /tmp fills up across the week."""
    body = _read("src/api/modules/routes_pricecheck_gen.py")
    assert "shutil.rmtree(tmp_dir" in body, (
        "Gmail API regression: temp dir cleanup for bundle send is gone. "
        "Each failed send leaks a dir under /tmp until the worker restarts."
    )


# ── Sanity: gmail_api still exposes the contract this migration depends on ──

def test_gmail_api_send_message_signature_stable():
    body = _read("src/core/gmail_api.py")
    # Quick structural checks — full behavior is covered by gmail_api tests.
    for needed in (
        "def send_message(",
        "def is_configured()",
        "def get_send_service(",
    ):
        assert needed in body, f"gmail_api contract changed: {needed!r} missing"
