"""Regression: notify_agent._send_alert_email migrated off smtplib.SMTP to gmail_api.

Third of the 5 remaining outbound smtplib sites (after PR #365 bundle-send,
PR #411 PC send-quote, PR #417 RFQ resend-package). Unlike the other sites
which used SMTP_SSL on 465, this one used smtplib.SMTP on port 587 with
STARTTLS — same underlying app-password auth that Google is deprecating.

Fix: gmail_api.is_configured() gate → get_send_service() → send_message()
with body_plain + body_html (alert emails have both — HTML styled panel +
plain-text fallback). No attachments (this is an alert, not a package).
get_agent_status email_ok health check must also stop reading
GMAIL_PASSWORD so the status card doesn't go red after OAuth migration.

Prior migration references:
  - routes_pricecheck_gen.py (PR #365 bundle-send)
  - routes_pricecheck_admin.py (PR #411 PC send-quote)
  - routes_rfq.py (PR #417 RFQ resend)
"""
from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
_NOTIFY = _REPO / "src" / "agents" / "notify_agent.py"


def _read(rel_or_path) -> str:
    if isinstance(rel_or_path, Path):
        return rel_or_path.read_text(encoding="utf-8")
    return (_REPO / rel_or_path).read_text(encoding="utf-8")


def _strip_comments_and_docstrings(src: str) -> str:
    import re
    src = re.sub(r'"""[\s\S]*?"""', "", src)
    src = re.sub(r"'''[\s\S]*?'''", "", src)
    out = []
    for line in src.splitlines():
        s = line.lstrip()
        if s.startswith("#"):
            continue
        if " # " in line:
            line = line.split(" # ", 1)[0]
        out.append(line)
    return "\n".join(out)


def _alert_fn_body() -> str:
    """Slice _send_alert_email between its def and the next top-level def."""
    body = _read(_NOTIFY)
    start = body.find("def _send_alert_email(")
    assert start >= 0, "_send_alert_email not found in notify_agent.py"
    next_def = body.find("\ndef ", start + 1)
    return body[start:next_def] if next_def > 0 else body[start:]


def _status_fn_body() -> str:
    body = _read(_NOTIFY)
    start = body.find("def get_agent_status(")
    assert start >= 0, "get_agent_status not found"
    next_def = body.find("\ndef ", start + 1)
    return body[start:next_def] if next_def > 0 else body[start:]


# ── Migration guards ────────────────────────────────────────────────────────

def test_notify_no_smtplib_smtp_call():
    body = _strip_comments_and_docstrings(_read(_NOTIFY))
    for pat in ("smtplib.SMTP(", "smtplib.SMTP_SSL("):
        assert pat not in body, (
            f"Gmail API regression: {pat} is back in notify_agent.py. "
            "_send_alert_email must route through src.core.gmail_api like the "
            "bundle-send, PC send-quote, and RFQ resend migrations."
        )


def test_notify_no_smtplib_import_in_alert_fn():
    fn = _strip_comments_and_docstrings(_alert_fn_body())
    assert "import smtplib" not in fn, (
        "Gmail API regression: `import smtplib` is back inside "
        "_send_alert_email. Migration removed both the import and send."
    )


def test_notify_alert_has_no_gmail_password_guard():
    """The old guard was `all([GMAIL_ADDRESS, GMAIL_PASSWORD, NOTIFY_EMAIL])`.
    OAuth migration must not fall back to app-password auth or gate on it."""
    fn = _strip_comments_and_docstrings(_alert_fn_body())
    assert "GMAIL_PASSWORD" not in fn, (
        "Gmail API regression: _send_alert_email reads GMAIL_PASSWORD — "
        "OAuth migration must not fall back to app-password auth."
    )


def test_notify_alert_uses_gmail_api_send_message():
    fn = _alert_fn_body()
    assert "gmail_api.send_message(" in fn, (
        "Gmail API regression: gmail_api.send_message call missing from "
        "_send_alert_email."
    )


def test_notify_alert_checks_gmail_api_is_configured():
    fn = _alert_fn_body()
    assert "gmail_api.is_configured()" in fn, (
        "Gmail API regression: missing is_configured() guard in "
        "_send_alert_email. Unconfigured prod will 500 from inside "
        "googleapiclient instead of returning {ok: False, reason: ...}."
    )


def test_notify_alert_sends_both_plain_and_html():
    """Alert emails have both a plain-text fallback and an HTML panel.
    Dropping body_html means the nice dashboard-style alert degrades to
    plain text in every mail client."""
    fn = _alert_fn_body()
    assert "body_plain=" in fn and "body_html=" in fn, (
        "Gmail API regression: _send_alert_email no longer passes both "
        "body_plain and body_html to gmail_api.send_message. Alert emails "
        "will lose their HTML rendering."
    )


def test_notify_alert_preserves_urgency_subject_prefixes():
    """Sanity: the migration must not have dropped the emoji subject prefixes
    that tell Mike at a glance if the alert is ACTION/URGENT/WIN/etc."""
    fn = _alert_fn_body()
    assert "URGENCY_SUBJECT_PREFIX" in fn, (
        "Gmail API regression: URGENCY_SUBJECT_PREFIX dict dropped from "
        "_send_alert_email."
    )
    for key in ("cs_draft_ready", "rfq_arrived", "quote_won", "oracle_weekly"):
        assert f'"{key}"' in fn, (
            f"Gmail API regression: event_type {key!r} dropped from "
            "URGENCY_SUBJECT_PREFIX during migration."
        )


def test_notify_alert_preserves_oracle_custom_html_path():
    """Oracle weekly report passes context={'html_body': ...} to override the
    default alert panel. Migration must not silently drop that branch."""
    fn = _alert_fn_body()
    assert 'context.get("html_body")' in fn or "context.get('html_body')" in fn, (
        "Gmail API regression: Oracle weekly custom html_body branch dropped. "
        "Weekly reports will render as a generic 'Reytech: alert' panel."
    )


def test_notify_status_health_check_uses_gmail_api():
    """get_agent_status surfaces `email_alerts.enabled` on the agent status
    card. If it keeps reading GMAIL_PASSWORD after OAuth migration, the card
    will read red even when email alerts work fine."""
    fn = _strip_comments_and_docstrings(_status_fn_body())
    assert "GMAIL_PASSWORD" not in fn, (
        "Gmail API regression: get_agent_status still gates email_ok on "
        "GMAIL_PASSWORD. After OAuth migration the status card will show "
        "red even when alerts work."
    )
    assert "gmail_api.is_configured()" in fn, (
        "Gmail API regression: get_agent_status no longer consults "
        "gmail_api.is_configured() for email readiness."
    )


# ── Sanity: gmail_api contract stable

def test_gmail_api_send_message_signature_stable():
    body = _read("src/core/gmail_api.py")
    for needed in (
        "def send_message(",
        "def is_configured()",
        "def get_send_service(",
        "body_plain",
        "body_html",
    ):
        assert needed in body, f"gmail_api contract changed: {needed!r} missing"
