"""Regression: startup_checks._auto_alert_failures migrated off smtplib.

Sixth outbound smtplib site — discovered after the original 5-site migration
(PRs #365 bundle-send, #411 PC send-quote, #417 RFQ resend, #418 notify-agent,
#420 vendor-ordering, #422 email-poller) by verification grep. This one is a
cold-start deploy health alert: when startup checks fail on a fresh boot it
emails Mike so he finds out before the next form submission breaks.

Fix: gmail_api.is_configured() gate → get_send_service() → send_message().
Simpler shape than the other 5 — plain text only, no attachments, no threading,
sends to self. The only outbound fields are to/subject/body_plain.

Also drops the `GMAIL_PASSWORD` env read (was the smtplib app-password) and
the `import smtplib` / `from email.mime.text import MIMEText` inside the
try-block.
"""
from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
_CHECKS = _REPO / "src" / "core" / "startup_checks.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


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
    """Slice _auto_alert_failures between its def and the next top-level def."""
    body = _read(_CHECKS)
    start = body.find("def _auto_alert_failures(")
    assert start >= 0, "_auto_alert_failures not found"
    next_def = body.find("\ndef ", start + 1)
    return body[start:next_def] if next_def > 0 else body[start:]


# ── Migration guards ────────────────────────────────────────────────────────

def test_startup_checks_no_smtplib_anywhere():
    body = _strip_comments_and_docstrings(_read(_CHECKS))
    for pat in ("smtplib.SMTP(", "smtplib.SMTP_SSL(", "import smtplib"):
        assert pat not in body, (
            f"Gmail API regression: {pat} is back in startup_checks.py. "
            "Deploy health alert must route through src.core.gmail_api."
        )


def test_startup_checks_no_gmail_password_env_read():
    body = _strip_comments_and_docstrings(_read(_CHECKS))
    assert "GMAIL_PASSWORD" not in body, (
        "Gmail API regression: GMAIL_PASSWORD env read is back in "
        "startup_checks.py. The OAuth migration removed the app-password path."
    )


def test_startup_checks_no_mime_text_import_in_alert():
    fn = _alert_fn_body()
    assert "from email.mime.text import MIMEText" not in fn, (
        "Gmail API regression: legacy MIMEText import resurfaced in "
        "_auto_alert_failures. gmail_api.send_message builds MIME internally."
    )


def test_startup_checks_alert_uses_gmail_api_send_message():
    fn = _alert_fn_body()
    assert "gmail_api.send_message(" in fn, (
        "Gmail API regression: gmail_api.send_message call missing from "
        "_auto_alert_failures. Deploy health alerts will silently not send."
    )


def test_startup_checks_alert_checks_gmail_api_is_configured():
    fn = _alert_fn_body()
    assert "gmail_api.is_configured()" in fn, (
        "Gmail API regression: _auto_alert_failures missing is_configured() "
        "guard. Without it the function will 500 inside googleapiclient when "
        "token.json is absent (fresh dev env), crashing the cold-start path."
    )


def test_startup_checks_alert_preserves_subject_and_body():
    """Alert body is the only signal Mike has that deploy health failed —
    drop either and the alert becomes an empty-ish notification."""
    fn = _alert_fn_body()
    assert "subject=subject" in fn, (
        "Gmail API regression: subject dropped from send_message call."
    )
    assert "body_plain=body" in fn, (
        "Gmail API regression: body dropped from send_message call — alert "
        "would arrive with no content."
    )


def test_startup_checks_alert_sends_to_gmail_address():
    fn = _alert_fn_body()
    assert "to=gmail" in fn, (
        "Gmail API regression: `to=` recipient wiring broken. Alert must "
        "self-send to GMAIL_ADDRESS so Mike sees deploy health failures."
    )


# ── Sanity: gmail_api contract stable for the minimal send shape ────────────

def test_gmail_api_send_message_supports_minimal_fields_startup_needs():
    body = (_REPO / "src" / "core" / "gmail_api.py").read_text(encoding="utf-8")
    sig_start = body.find("def send_message(")
    assert sig_start >= 0
    sig_end = body.find("):", sig_start)
    sig = body[sig_start:sig_end]
    for needed in ("to", "subject", "body_plain"):
        assert needed in sig, (
            f"gmail_api.send_message signature missing {needed!r} — "
            f"_auto_alert_failures relies on it."
        )
