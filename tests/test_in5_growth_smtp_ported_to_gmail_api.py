"""IN-10 regression: growth send sites in routes_analytics.py must NOT use
raw smtplib.SMTP_SSL — they must go through src.core.gmail_api.send_message.

Before 2026-04-22, routes_analytics.py had three `smtplib.SMTP_SSL("smtp.gmail.com",
465)` call sites (send-quote, single follow-up, bulk follow-up). They used
the legacy app-password auth path and bypassed the Gmail API OAuth tokens,
so any future OAuth-only Gmail upgrade would have silently dropped outbound
quote email while the rest of the app kept running on the new token.

This guard locks the fix in: the file must no longer import smtplib or open
an SMTP_SSL connection, and each of the three sending blocks must reference
the Gmail API wrapper instead.
"""
from __future__ import annotations

from pathlib import Path


ROUTES_ANALYTICS = (
    Path(__file__).resolve().parents[1]
    / "src" / "api" / "modules" / "routes_analytics.py"
)


def _read():
    return ROUTES_ANALYTICS.read_text(encoding="utf-8")


def test_no_smtplib_import_in_routes_analytics():
    src = _read()
    assert "import smtplib" not in src, (
        "IN-5 regressed: routes_analytics.py re-introduced `import smtplib` — "
        "growth send paths must route through src.core.gmail_api."
    )


def test_no_smtp_ssl_call_in_routes_analytics():
    src = _read()
    # Tolerate the word appearing only inside comments documenting the migration.
    for lineno, line in enumerate(src.splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        assert 'smtplib.SMTP_SSL(' not in line, (
            f"IN-5 regressed: routes_analytics.py:{lineno} still calls "
            f"smtplib.SMTP_SSL. Use gmail_api.send_message() instead."
        )


def test_three_send_sites_use_gmail_api():
    """Each of the three send sites (send-quote, follow-up, bulk follow-up)
    must call gmail_api.send_message. Counting occurrences catches partial
    regressions (e.g., someone ports 2 of 3 back to smtplib)."""
    src = _read()
    count = src.count("gmail_api.send_message(")
    assert count >= 3, (
        f"IN-5 regressed: routes_analytics.py has only {count} "
        f"gmail_api.send_message() calls; expected >= 3 (send-quote, "
        f"single follow-up, bulk follow-up)."
    )


def test_send_sites_gate_on_gmail_api_is_configured():
    """Each block must guard with gmail_api.is_configured() so a missing
    refresh token returns a clean 400 instead of blowing up inside the
    OAuth refresh path."""
    src = _read()
    count = src.count("gmail_api.is_configured()")
    assert count >= 3, (
        f"IN-5 regressed: routes_analytics.py has {count} is_configured() "
        f"guards; expected >= 3 (one per send site)."
    )
