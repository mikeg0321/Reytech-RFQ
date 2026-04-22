"""Regression: vendor_ordering_agent.send_email_po migrated off smtplib to gmail_api.

Fourth of the 5 remaining outbound smtplib sites (after PR #365 bundle-send,
PR #411 PC send-quote, PR #417 RFQ resend-package, PR #418 notify-agent).
Like notify_agent this one used smtplib.SMTP on port 587 with STARTTLS —
same deprecated app-password auth underneath.

Fix: gmail_api.is_configured() gate → get_send_service() → send_message()
with cc=GMAIL_ADDRESS so Mike keeps getting a copy of every vendor PO
(Sent-folder capture is built into Gmail API authenticated send, but the
cc preserves the legacy "BCC ourselves for records" semantics).

Also: 5 vendor-catalog entries and the agent-status health card used to
gate on bool(GMAIL_ADDRESS and GMAIL_PASSWORD). OAuth migration must
swap that for _GMAIL_API_READY so the vendor-ordering card doesn't read
red after the env var is pulled.
"""
from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
_VENDOR = _REPO / "src" / "agents" / "vendor_ordering_agent.py"


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


def _send_po_fn_body() -> str:
    body = _read(_VENDOR)
    start = body.find("def send_email_po(")
    assert start >= 0, "send_email_po not found"
    next_def = body.find("\ndef ", start + 1)
    return body[start:next_def] if next_def > 0 else body[start:]


# ── Migration guards ────────────────────────────────────────────────────────

def test_vendor_no_smtplib_anywhere():
    body = _strip_comments_and_docstrings(_read(_VENDOR))
    for pat in ("smtplib.SMTP(", "smtplib.SMTP_SSL(", "import smtplib"):
        assert pat not in body, (
            f"Gmail API regression: {pat} is back in vendor_ordering_agent.py. "
            "send_email_po must route through src.core.gmail_api."
        )


def test_vendor_no_gmail_password_env_read():
    """OAuth migration must pull GMAIL_PASSWORD from the module entirely —
    not just the send block. Every `configured`/`can_order` gate that
    previously read it must now consult gmail_api.is_configured()."""
    body = _strip_comments_and_docstrings(_read(_VENDOR))
    assert "GMAIL_PASSWORD" not in body, (
        "Gmail API regression: GMAIL_PASSWORD references are back in "
        "vendor_ordering_agent.py."
    )


def test_vendor_send_po_uses_gmail_api_send_message():
    fn = _send_po_fn_body()
    assert "gmail_api.send_message(" in fn, (
        "Gmail API regression: gmail_api.send_message call missing from "
        "send_email_po."
    )


def test_vendor_send_po_checks_gmail_api_is_configured():
    fn = _send_po_fn_body()
    assert "gmail_api.is_configured()" in fn, (
        "Gmail API regression: send_email_po missing is_configured() guard."
    )


def test_vendor_send_po_preserves_cc_to_self():
    """Legacy smtplib path CC'd GMAIL_ADDRESS so Mike kept a copy of every
    vendor PO. The migrated send must preserve that — Gmail API authenticated
    send copies to Sent, but a separate inbox copy is relied on downstream by
    the email_log reconciliation."""
    fn = _send_po_fn_body()
    assert "cc=GMAIL_ADDRESS" in fn, (
        "Gmail API regression: send_email_po no longer CCs GMAIL_ADDRESS. "
        "Mike loses the inbox copy of outbound POs."
    )


def test_vendor_catalog_entries_use_gmail_api_ready():
    """5 email_po vendors (curbell, integrated, echelon, tsi) plus
    email_po_active in get_agent_status — all used to call
    bool(GMAIL_ADDRESS and GMAIL_PASSWORD). Each must now use
    _GMAIL_API_READY so they stay green after the env var is pulled."""
    body = _read(_VENDOR)
    assert "_GMAIL_API_READY" in body, (
        "Gmail API regression: _GMAIL_API_READY flag missing — the vendor "
        "catalog health checks will fall back to hard-coded False after "
        "GMAIL_PASSWORD is pulled."
    )
    # The catalog has 4 email_po vendors × 2 fields (configured+can_order)
    # + email_po_active in status = 9 references. Allow 9+ to be safe if
    # a new email_po vendor is added later; block 0 hard.
    assert body.count("_GMAIL_API_READY") >= 9, (
        "Gmail API regression: _GMAIL_API_READY used fewer times than the "
        "original GMAIL_PASSWORD guard — one or more vendor catalog entries "
        "still has a hard-coded False status."
    )


def test_vendor_gmail_api_ready_imports_at_top():
    """The _GMAIL_API_READY flag must be computed at import time before
    VENDOR_CATALOG is constructed (VENDOR_CATALOG references it in dict
    literals). Otherwise import will NameError."""
    body = _read(_VENDOR)
    catalog_idx = body.find("VENDOR_CATALOG =")
    ready_idx = body.find("_GMAIL_API_READY")
    assert catalog_idx >= 0 and ready_idx >= 0, "markers missing"
    assert ready_idx < catalog_idx, (
        "Gmail API regression: _GMAIL_API_READY must be defined BEFORE "
        "VENDOR_CATALOG — the catalog dict literal references it."
    )


# ── Sanity: gmail_api contract stable

def test_gmail_api_send_message_supports_cc():
    body = _read("src/core/gmail_api.py")
    assert "def send_message(" in body
    assert "cc=" in body, (
        "gmail_api contract changed: send_message no longer supports cc= — "
        "vendor_ordering_agent relies on it for the inbox self-copy."
    )
