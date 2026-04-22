"""Regression: email_poller.EmailSender.send migrated off smtplib to gmail_api.

Fifth and final outbound smtplib site (after PR #365 bundle-send,
PR #411 PC send-quote, PR #417 RFQ resend-package, PR #418 notify-agent,
PR #420 vendor-ordering). This is the hot path — it's how every drafted
bid response in /agents/outbox actually leaves the building once Mike
clicks Send. Was smtplib.SMTP on port 587 with STARTTLS + app-password.

Fix: gmail_api.is_configured() gate → get_send_service() → send_message()
passing through cc/bcc/attachments/in_reply_to/references/from_name —
EmailSender has the richest draft shape of any of the 5 sites, and every
field must survive the translation. Threading in particular (in_reply_to
+ references) is load-bearing — drop it and the reply lands as a new
thread in the buyer's inbox and looks like spam.

Also drops EmailSender.smtp_host/smtp_port/password (dead after migration)
and EmailPoller.password (was only used by a since-removed IMAP path —
Gmail API handles inbound too).

Prior migration references:
  - routes_pricecheck_gen.py (PR #365 bundle-send)
  - routes_pricecheck_admin.py (PR #411 PC send-quote)
  - routes_rfq.py (PR #417 RFQ resend)
  - agents/notify_agent.py (PR #418 alert email)
  - agents/vendor_ordering_agent.py (PR #420 vendor PO)
"""
from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
_POLLER = _REPO / "src" / "agents" / "email_poller.py"


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


def _send_fn_body() -> str:
    """Slice EmailSender.send between its `def send(` and the next def."""
    body = _read(_POLLER)
    start = body.find("    def send(self, draft):")
    assert start >= 0, "EmailSender.send not found"
    next_def = body.find("\ndef ", start + 1)
    next_method = body.find("\n    def ", start + 1)
    next_class = body.find("\nclass ", start + 1)
    candidates = [x for x in (next_def, next_method, next_class) if x > 0]
    end = min(candidates) if candidates else -1
    return body[start:end] if end > 0 else body[start:]


# ── Migration guards ────────────────────────────────────────────────────────

def test_poller_no_smtplib_anywhere():
    body = _strip_comments_and_docstrings(_read(_POLLER))
    for pat in ("smtplib.SMTP(", "smtplib.SMTP_SSL(", "import smtplib"):
        assert pat not in body, (
            f"Gmail API regression: {pat} is back in email_poller.py. "
            "EmailSender.send must route through src.core.gmail_api."
        )


def test_poller_no_gmail_password_env_read():
    body = _strip_comments_and_docstrings(_read(_POLLER))
    assert "GMAIL_PASSWORD" not in body, (
        "Gmail API regression: GMAIL_PASSWORD references are back in "
        "email_poller.py. The OAuth migration should have removed the "
        "env var read from both EmailSender and EmailPoller __init__."
    )


def test_poller_send_uses_gmail_api_send_message():
    fn = _send_fn_body()
    assert "gmail_api.send_message(" in fn, (
        "Gmail API regression: gmail_api.send_message call missing from "
        "EmailSender.send."
    )


def test_poller_send_checks_gmail_api_is_configured():
    fn = _send_fn_body()
    assert "gmail_api.is_configured()" in fn, (
        "Gmail API regression: EmailSender.send missing is_configured() "
        "guard. Outbox sends will 500 from inside googleapiclient instead "
        "of raising a clean RuntimeError."
    )


def test_poller_send_preserves_threading_headers():
    """Buyer RFQ replies threaded via in_reply_to/references. Dropping
    those makes replies land as new threads — looks like spam, breaks
    dedup by thread_id, breaks the buyer's view of the conversation."""
    fn = _send_fn_body()
    assert "in_reply_to=" in fn, (
        "Gmail API regression: in_reply_to dropped from EmailSender.send. "
        "Replies will land as new threads in buyer inboxes."
    )
    assert "references=" in fn, (
        "Gmail API regression: references header dropped — same thread "
        "failure as in_reply_to."
    )


def test_poller_send_preserves_cc_bcc():
    fn = _send_fn_body()
    assert "cc=" in fn and "bcc=" in fn, (
        "Gmail API regression: cc/bcc dropped from EmailSender.send. "
        "Buyer CCs on original RFQ will miss the reply."
    )


def test_poller_send_preserves_attachments():
    """Attachment list is the whole point of EmailSender — it's what
    ships the quote PDF + 703B + 704B bid package."""
    fn = _send_fn_body()
    assert "attachments=" in fn, (
        "Gmail API regression: attachments dropped from EmailSender.send. "
        "Bid responses will send with no PDFs attached."
    )


def test_poller_send_preserves_from_name():
    """from_name = 'Michael Guadan - Reytech Inc.' is what buyers see in
    their inbox. Dropping it shows a bare email address."""
    fn = _send_fn_body()
    assert "from_name=" in fn, (
        "Gmail API regression: from_name dropped — buyer inbox will show "
        "a bare email address instead of 'Michael Guadan - Reytech Inc.'"
    )


def test_poller_email_sender_drops_password_field():
    """Dead-code check: EmailSender.password was only used for smtplib
    login. After migration it should be gone."""
    body = _read(_POLLER)
    # Scope: inside EmailSender class specifically
    cls_start = body.find("class EmailSender")
    assert cls_start >= 0
    next_class = body.find("\nclass ", cls_start + 1)
    cls = body[cls_start:next_class] if next_class > 0 else body[cls_start:]
    assert "self.password" not in cls, (
        "Gmail API regression: EmailSender.password lingers as dead code "
        "after OAuth migration — remove the field and its init read."
    )
    assert "self.smtp_host" not in cls and "self.smtp_port" not in cls, (
        "Gmail API regression: smtp_host/smtp_port lingers as dead code "
        "in EmailSender after OAuth migration."
    )


# ── Sanity: gmail_api contract stable for the rich send shape

def test_gmail_api_send_message_supports_all_fields_poller_needs():
    body = _read("src/core/gmail_api.py")
    sig_start = body.find("def send_message(")
    assert sig_start >= 0
    sig_end = body.find("):", sig_start)
    sig = body[sig_start:sig_end]
    for needed in (
        "to",
        "subject",
        "body_plain",
        "body_html",
        "cc",
        "bcc",
        "attachments",
        "in_reply_to",
        "references",
        "from_name",
        "from_addr",
    ):
        assert needed in sig, (
            f"gmail_api.send_message signature missing {needed!r} — "
            f"EmailSender.send relies on it."
        )
