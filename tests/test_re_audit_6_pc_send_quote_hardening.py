"""RE-AUDIT-6 regression guard.

`api_pc_send_quote` in routes_pricecheck_admin.py had two defects:

1. **Raw SMTP path.** Every other outbound send site has been migrated to
   gmail_api.send_message (OAuth-scoped) under the Phase 3 rollout
   (see project_gmail_api_outbound_migration.md). PC send-quote still used
   smtplib.SMTP_SSL with GMAIL_PASSWORD app-password — the legacy path that
   breaks whenever Google rotates the app-password.

2. **No idempotency guard.** A PC already in status=sent had no guard.
   A double-click on the UI send button, a retry on a flaky network, or a
   bundle-send that overlapped with a single-PC send all shipped the buyer
   a duplicate quote. Bundle-send mirrors PCs to status=sent which means
   the single-PC endpoint should refuse to resend without an explicit
   force_resend override.

Fixes:
- Replace smtplib block with `gmail_api.send_message(...)` using
  get_send_service() + from_name="Reytech Inc." + threading via
  in_reply_to/references.
- Early-return HTTP 409 when pc.status=="sent" and the request body did
  not set force_resend=True.
"""
from __future__ import annotations

import re
from pathlib import Path


ROUTES = (
    Path(__file__).resolve().parents[1]
    / "src" / "api" / "modules" / "routes_pricecheck_admin.py"
)


def _route_body() -> str:
    src = ROUTES.read_text(encoding="utf-8")
    m = re.search(
        r"def api_pc_send_quote\(pcid\)[\s\S]*?(?=\n@bp\.route|\ndef [a-zA-Z_]|\Z)",
        src,
    )
    assert m, "api_pc_send_quote body not located"
    # Strip comment lines so assertions like "no smtplib" don't flag the
    # docstring sentence that explains what this code replaced.
    kept = []
    for line in m.group(0).splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        kept.append(line)
    return "\n".join(kept)


def test_smtplib_removed_from_send_quote_path():
    """api_pc_send_quote must not call smtplib or GMAIL_PASSWORD."""
    body = _route_body()
    assert "smtplib" not in body, (
        "RE-AUDIT-6 regression: api_pc_send_quote is using smtplib again. "
        "Every other outbound send site is migrated to gmail_api. Restore "
        "the OAuth path or this endpoint will silently fail whenever the "
        "legacy GMAIL_PASSWORD is rotated."
    )
    assert "GMAIL_PASSWORD" not in body, (
        "RE-AUDIT-6 regression: api_pc_send_quote references GMAIL_PASSWORD. "
        "The OAuth refresh-token path does not need it."
    )


def test_send_quote_uses_gmail_api():
    """api_pc_send_quote must route through gmail_api.send_message."""
    body = _route_body()
    assert re.search(r"gmail_api\.send_message\(", body), (
        "RE-AUDIT-6 regression: api_pc_send_quote no longer calls "
        "gmail_api.send_message — the migration was reverted or replaced."
    )
    assert re.search(r"gmail_api\.is_configured\(", body), (
        "RE-AUDIT-6 regression: api_pc_send_quote must gate on "
        "gmail_api.is_configured() — otherwise a missing OAuth token "
        "crashes inside send_message with an opaque RuntimeError."
    )


def test_send_quote_idempotency_guard_present():
    """Re-sending a status=sent PC must require force_resend."""
    body = _route_body()
    # Check for the idempotency gate (status == sent and not force_resend).
    assert re.search(
        r"pc\.get\(\s*[\"']status[\"']\s*\)\s*==\s*[\"']sent[\"']",
        body,
    ), (
        "RE-AUDIT-6 regression: api_pc_send_quote is missing the "
        "already-sent check. Without it a double-click on the send button "
        "ships the buyer a duplicate quote."
    )
    assert re.search(r"force_resend", body), (
        "RE-AUDIT-6 regression: api_pc_send_quote is missing the "
        "force_resend override. The idempotency guard must have an "
        "explicit caller opt-in for legitimate resends."
    )


def test_send_quote_preserves_threading():
    """Buyer email_message_id must still be threaded into the reply."""
    body = _route_body()
    assert re.search(r"email_message_id", body) and \
        re.search(r"in_reply_to\s*=", body), (
        "RE-AUDIT-6 regression: api_pc_send_quote dropped the buyer's "
        "email_message_id from the threading headers. Replies will show "
        "up as a new thread instead of in the buyer's existing RFQ thread."
    )
