"""RE-AUDIT-10b regression guard — vendor_ordering_agent Gmail-API migration.

`send_email_po` in src/agents/vendor_ordering_agent.py previously sent via
`smtplib.SMTP("smtp.gmail.com", 587)` + STARTTLS + GMAIL_PASSWORD app
password. That was the third of six outbound smtplib sites the project
is migrating onto the OAuth gmail_api.send_message path.

This test gates the migrated shape so a hand-edit or revert can't
silently re-introduce the old smtplib path.
"""
from __future__ import annotations

import re
from pathlib import Path


VENDOR_AGENT = (
    Path(__file__).resolve().parents[1]
    / "src" / "agents" / "vendor_ordering_agent.py"
)


def _source() -> str:
    return VENDOR_AGENT.read_text(encoding="utf-8")


def _strip_comments(src: str) -> str:
    """Drop `#` comments AND triple-quoted docstrings so the guards match
    actual executable code, not prose that happens to mention the old
    pattern (e.g. a migration note in the function docstring)."""
    src = re.sub(r'"""[\s\S]*?"""', "", src)
    src = re.sub(r"'''[\s\S]*?'''", "", src)
    out = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        out.append(line)
    return "\n".join(out)


def _send_email_po_body(src: str) -> str:
    m = re.search(
        r"def send_email_po\([\s\S]*?\n(?=def [a-zA-Z_])",
        src,
    )
    assert m, "send_email_po function body not located"
    return m.group(0)


def test_send_email_po_uses_gmail_api():
    """send_email_po must route through gmail_api.send_message."""
    body = _send_email_po_body(_source())
    assert "send_message" in body, (
        "RE-AUDIT-10b regression: send_email_po does not call "
        "gmail_api.send_message. Vendor PO email was reverted back to "
        "smtplib — re-migrate."
    )
    assert "is_configured" in body, (
        "RE-AUDIT-10b regression: send_email_po does not guard on "
        "gmail_api.is_configured(). A misconfigured prod will 500 "
        "silently on every vendor PO send."
    )


def test_send_email_po_has_no_smtplib_call():
    """The send path must not instantiate smtplib.SMTP anywhere."""
    body = _send_email_po_body(_strip_comments(_source()))
    assert not re.search(r"smtplib\.SMTP\s*\(", body), (
        "RE-AUDIT-10b regression: send_email_po still constructs "
        "smtplib.SMTP(...). Outbound must go through "
        "gmail_api.send_message."
    )
    assert "starttls" not in body.lower(), (
        "RE-AUDIT-10b regression: send_email_po still references "
        "STARTTLS. Smtplib path must be fully removed."
    )


def test_no_smtplib_import_in_send_email_po():
    """The `import smtplib` inside send_email_po must be gone."""
    body = _send_email_po_body(_strip_comments(_source()))
    assert not re.search(r"^\s*import\s+smtplib\b", body, re.MULTILINE), (
        "RE-AUDIT-10b regression: send_email_po still imports "
        "smtplib in its body. The gmail_api migration left a stale "
        "import — clean it up."
    )
