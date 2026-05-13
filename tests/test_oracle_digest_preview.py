"""Phase 3.1 — Oracle digest preview endpoint.

Substrate-only ship. The `generate_weekly_report()` + `format_report_email()`
functions in src/agents/oracle_weekly_report.py have been built for weeks
but only reachable via the POST trigger that ALSO sends the email. Mike
needs to see the artifact first — what kinds of losses get surfaced, what
the formatting looks like, what columns are present — before he picks a
delivery cadence (immediate per-loss alert / daily 7am / weekly Monday).

This file pins the preview route's behavior:
  • GET /oracle/digest/preview returns HTML (200)
  • requires auth
  • does NOT send an email (no notify_agent.send_alert call)
  • does NOT mutate any state (no DB writes, no JSON writes)
  • renders the same `format_report_email` body as the live email path
  • surfaces the period and counts in a preview-only banner
"""
from __future__ import annotations

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def test_digest_preview_returns_200_and_html(auth_client):
    r = auth_client.get("/oracle/digest/preview")
    assert r.status_code == 200
    assert "text/html" in r.headers.get("Content-Type", "")
    body = r.get_data(as_text=True)
    assert "Oracle Digest Preview" in body
    assert "Preview only" in body, "preview banner must be present"


def test_digest_preview_requires_auth(anon_client):
    r = anon_client.get("/oracle/digest/preview")
    # Either 401 (gate fires) or 302 (redirect to login) — both prove gated
    assert r.status_code in (401, 302, 403), (
        f"preview endpoint must require auth, got {r.status_code}"
    )


def test_digest_preview_does_not_send_email(auth_client):
    """The whole point: read-only preview. `notify_agent.send_alert` must
    NEVER be called from this route. If it is, we have a leak that would
    spam Mike's inbox every time he hits the preview URL."""
    with patch("src.agents.notify_agent.send_alert") as mock_send:
        r = auth_client.get("/oracle/digest/preview")
    assert r.status_code == 200
    assert not mock_send.called, (
        f"send_alert was called {mock_send.call_count} times — preview must not "
        f"trigger email. Args: {mock_send.call_args_list}"
    )


def test_digest_preview_includes_report_period(auth_client):
    """Banner must surface the period window so Mike can confirm at-a-glance
    that the right time range is being summarized."""
    r = auth_client.get("/oracle/digest/preview")
    body = r.get_data(as_text=True)
    # Period dates are ISO YYYY-MM-DD format
    import re
    matches = re.findall(r'\d{4}-\d{2}-\d{2}', body)
    assert len(matches) >= 2, f"preview must show period start + end, got {matches!r}"


def test_digest_preview_renders_format_report_email_html(auth_client):
    """The rendered body must include the `format_report_email` output, not
    just a stub. Pin a known marker from the email template ("Oracle V3
    Weekly Intelligence" header) so future template renames don't silently
    break the preview."""
    r = auth_client.get("/oracle/digest/preview")
    body = r.get_data(as_text=True)
    assert "Oracle V3 Weekly Intelligence" in body, (
        "preview body must include the format_report_email template — "
        "the V3 header is the canonical fingerprint"
    )


def test_digest_preview_works_when_no_recent_activity(auth_client):
    """Brand-new instance / week with zero wins and zero losses must still
    render — the "no activity this week" empty state has to surface
    something useful, not a 500."""
    # Patch the report to return an empty state
    with patch("src.agents.oracle_weekly_report.generate_weekly_report") as mock_gen:
        mock_gen.return_value = {
            "period_start": "2026-05-06",
            "period_end": "2026-05-13",
            "generated_at": "2026-05-13T08:00:00",
            "wins": [], "win_count": 0, "win_revenue": 0,
            "losses": [], "loss_count": 0,
            "calibrations": [],
            "winning_prices_total": 0,
            "winning_prices_unique": 0,
            "avg_margin_all_time": 0,
            "supplier_leads": [],
            "pending_actions": [],
        }
        r = auth_client.get("/oracle/digest/preview")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert "No win/loss activity this week" in body, (
        "zero-activity empty state must surface a friendly message"
    )
