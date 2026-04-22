"""IN-8 / IN-9 regression: Intel/Oracle alert pipeline cleanup.

IN-8 — oracle_weekly_report.check_report_health previously returned silently
       when `oracle_report_log` had no successful-send rows ("might be first
       week"). That fails open on the two cases that most need alerting:
       a fresh DB after a migration glitch, and a prod box that has never
       successfully sent. Fix: alert with a distinct 'never_sent' event
       type so ops know the report has never run.

IN-9 — routes_growth_intel outreach reference-facility lookup compared
       agency strings with `==`. The institution resolver emits lowercase
       ("cchcs") while the RFQ form writes uppercase ("CCHCS"), so the
       lookup silently missed every case-opposite match. Fix: normalize
       both sides to lowercase before comparing.

Both guards are grep-level — they verify the fix is still in the code
and would catch a regression that stripped the alert branch or reverted
to a raw `==` compare.
"""
from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ORACLE_WEEKLY = ROOT / "src" / "agents" / "oracle_weekly_report.py"
GROWTH_INTEL = ROOT / "src" / "api" / "modules" / "routes_growth_intel.py"


# ── IN-8 ────────────────────────────────────────────────────────────────

def test_check_report_health_alerts_on_missing_row():
    """When oracle_report_log has no successful send, check_report_health
    must fire an alert (not silently return). The fix introduces a
    distinct 'never_sent' event type so it's distinguishable from the
    overdue path."""
    src = ORACLE_WEEKLY.read_text(encoding="utf-8")

    # Extract the check_report_health() body and assert it references
    # the never-sent alert. A regression that re-adds "might be first
    # week — return silently" will lose this token.
    idx = src.find("def check_report_health")
    assert idx != -1, "check_report_health function missing"
    # Grab a generous window — the function is < 4000 chars.
    fragment = src[idx : idx + 4000]
    assert "oracle_weekly_never_sent" in fragment, (
        "IN-8 regressed: check_report_health no longer alerts on the "
        "missing-row case. Fresh DBs + migration glitches will silently "
        "hide a broken feedback loop."
    )
    assert "send_alert" in fragment, (
        "IN-8 regressed: check_report_health lost its send_alert call."
    )


def test_check_report_health_distinguishes_never_sent_from_overdue():
    """Two code paths — 'never sent' vs 'overdue >9 days'. Must keep
    both alert events so dashboards can tell them apart."""
    src = ORACLE_WEEKLY.read_text(encoding="utf-8")
    assert "oracle_weekly_never_sent" in src, "never_sent event missing"
    assert "oracle_weekly_overdue" in src, "overdue event missing"


# ── IN-9 ────────────────────────────────────────────────────────────────

def test_outreach_reference_lookup_normalizes_agency_case():
    """The reference-facility lookup must compare agency strings
    case-insensitively. The raw `q.get("agency") == target.get("agency")`
    comparison regressed every case-opposite match."""
    src = GROWTH_INTEL.read_text(encoding="utf-8")

    # The fix must keep case-sensitive comparison out of the lookup.
    # Assert the raw case-sensitive line is gone.
    raw_pattern = re.compile(
        r'q\.get\("agency",\s*""\)\s*==\s*target\.get\("agency",\s*""\)'
    )
    assert not raw_pattern.search(src), (
        "IN-9 regressed: case-sensitive agency == agency compare came "
        "back. CCHCS vs cchcs matches will silently drop."
    )

    # Fix marker: the lowercased-both-sides form.
    assert ".strip().lower()" in src, (
        "IN-9 regressed: reference-facility lookup no longer lowercases "
        "both sides of the agency compare."
    )


def test_in9_marker_comment_explains_why():
    """The fix comment spells out the CCHCS/cchcs invariant. Keeping
    the comment alive reduces the chance someone 'simplifies' the
    normalization back out."""
    src = GROWTH_INTEL.read_text(encoding="utf-8")
    assert "IN-9" in src, (
        "IN-9 fix comment went missing — future readers will drop the "
        "normalization thinking it's redundant."
    )
