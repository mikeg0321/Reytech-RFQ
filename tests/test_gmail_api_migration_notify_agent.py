"""RE-AUDIT-10 regression guard — notify_agent outbound Gmail-API migration.

`_send_alert_email` in src/agents/notify_agent.py previously sent via
`smtplib.SMTP("smtp.gmail.com", 587)` with STARTTLS + app-password
login. That pattern (a) required a GMAIL_PASSWORD env var separate
from the OAuth refresh token everything else now uses, and (b) was
the second of six outbound smtplib sites the project is migrating
onto the OAuth path.

This test gates the migrated shape so a hand-edit or revert can't
silently re-introduce the old smtplib code.
"""
from __future__ import annotations

import re
from pathlib import Path


NOTIFY_AGENT = (
    Path(__file__).resolve().parents[1]
    / "src" / "agents" / "notify_agent.py"
)


def _source() -> str:
    return NOTIFY_AGENT.read_text(encoding="utf-8")


def _strip_comments(src: str) -> str:
    """Drop `#` comments AND triple-quoted docstrings so the guards match
    actual executable code, not prose that happens to mention the old
    pattern (e.g. a migration note in the function docstring)."""
    # Strip triple-quoted blocks first (handles both `"""..."""` and `'''...'''`).
    src = re.sub(r'"""[\s\S]*?"""', "", src)
    src = re.sub(r"'''[\s\S]*?'''", "", src)
    out = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        out.append(line)
    return "\n".join(out)


def test_send_alert_email_uses_gmail_api():
    """_send_alert_email must go through gmail_api.send_message."""
    src = _source()
    m = re.search(
        r"def _send_alert_email\([\s\S]*?\n(?=def [a-zA-Z_])",
        src,
    )
    assert m, "_send_alert_email function body not located"
    body = m.group(0)
    assert "gmail_api.send_message" in body, (
        "RE-AUDIT-10 regression: _send_alert_email does not call "
        "gmail_api.send_message. Alert email was reverted back to "
        "smtplib — re-migrate."
    )
    assert "gmail_api.is_configured" in body, (
        "RE-AUDIT-10 regression: _send_alert_email does not guard on "
        "gmail_api.is_configured(). A misconfigured prod will 500 "
        "silently on every notify call."
    )


def test_send_alert_email_has_no_smtplib_call():
    """The send path must not instantiate smtplib.SMTP anywhere."""
    src = _strip_comments(_source())
    m = re.search(
        r"def _send_alert_email\([\s\S]*?\n(?=def [a-zA-Z_])",
        src,
    )
    assert m, "_send_alert_email function body not located"
    body = m.group(0)
    assert not re.search(r"smtplib\.SMTP\s*\(", body), (
        "RE-AUDIT-10 regression: _send_alert_email still constructs "
        "smtplib.SMTP(...). Outbound must go through "
        "gmail_api.send_message."
    )
    assert "starttls" not in body.lower(), (
        "RE-AUDIT-10 regression: _send_alert_email still references "
        "STARTTLS. Smtplib path must be fully removed."
    )


def test_no_smtplib_import_remains_in_function():
    """The `import smtplib` inside _send_alert_email must be gone."""
    src = _strip_comments(_source())
    m = re.search(
        r"def _send_alert_email\([\s\S]*?\n(?=def [a-zA-Z_])",
        src,
    )
    assert m, "_send_alert_email function body not located"
    body = m.group(0)
    assert not re.search(r"^\s*import\s+smtplib\b", body, re.MULTILINE), (
        "RE-AUDIT-10 regression: _send_alert_email still imports "
        "smtplib in its body. The gmail_api migration left a stale "
        "import — clean it up."
    )
