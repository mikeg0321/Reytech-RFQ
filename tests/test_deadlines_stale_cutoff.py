"""Regression: `_scan_deadlines` 72h stale-overdue cutoff.

Incident 2026-04-23: PC "Karaoke" (`pc_5db2709d`, CSP-SAC) was 36 days
(864h) past its due date and still firing critical bell notifications
every hour AND showing in the bottom-of-page deadline sidebar. The
home-page triage queue had already dropped it via
`quote_triage._STALE_OVERDUE_HOURS = 72`, but the sidebar +
notify_agent path went through `_scan_deadlines`, which had no such
cutoff — two different filters on the same data.

This regression locks in the cutoff so:
1. The deadline-sidebar fetches a clean list.
2. The bell-notification watcher stops re-alerting on stale records.
3. Operators can still see the full backlog via `include_stale=True`.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.api.modules.routes_deadlines import _scan_deadlines, _STALE_OVERDUE_HOURS


_PST = timezone(timedelta(hours=-8))


def _pc(due_offset_hours, *, status="open", desc="Test PC"):
    """Build a fake PC dict with due_date set N hours from now."""
    now = datetime.now(_PST)
    due_dt = now + timedelta(hours=due_offset_hours)
    return {
        "status": status,
        "due_date": due_dt.strftime("%m/%d/%Y"),
        "due_time": due_dt.strftime("%I:%M %p"),
        "header": {"institution": "Test Prison", "pc_number": desc},
        "items": [],
    }


def _stub_load(pcs=None, rfqs=None):
    """Patch the data-layer loaders for the duration of a test."""
    pcs = pcs or {}
    rfqs = rfqs or {}
    return patch("src.api.data_layer._load_price_checks", return_value=pcs), \
           patch("src.api.data_layer.load_rfqs", return_value=rfqs)


def test_recent_overdue_within_72h_is_kept():
    """A PC overdue by 24h should still appear — operator can act."""
    pcs = {"pc_recent": _pc(due_offset_hours=-24, desc="Recent")}
    p1, p2 = _stub_load(pcs=pcs)
    with p1, p2:
        out = _scan_deadlines()
    assert len(out) == 1
    assert out[0]["urgency"] == "overdue"


def test_stale_overdue_past_72h_is_dropped():
    """Karaoke at 864h past due (36 days) must NOT appear."""
    pcs = {"pc_karaoke": _pc(due_offset_hours=-864, desc="Karaoke")}
    p1, p2 = _stub_load(pcs=pcs)
    with p1, p2:
        out = _scan_deadlines()
    assert out == [], f"Stale-overdue Karaoke should be filtered, got: {out}"


def test_boundary_exactly_72h_overdue_is_kept():
    """The cutoff is `> 72h` (strict), so exactly 72h is still actionable."""
    # Use 71.5 to be safe across run-time drift; 72.5 is the first stale value.
    pcs = {"pc_edge": _pc(due_offset_hours=-71.5, desc="EdgeKept")}
    p1, p2 = _stub_load(pcs=pcs)
    with p1, p2:
        out = _scan_deadlines()
    assert len(out) == 1


def test_boundary_just_past_72h_is_dropped():
    pcs = {"pc_edge": _pc(due_offset_hours=-72.5, desc="EdgeDropped")}
    p1, p2 = _stub_load(pcs=pcs)
    with p1, p2:
        out = _scan_deadlines()
    assert out == []


def test_include_stale_true_returns_full_backlog():
    """Admin views can opt into the full list."""
    pcs = {
        "pc_recent": _pc(due_offset_hours=-24, desc="Recent"),
        "pc_karaoke": _pc(due_offset_hours=-864, desc="Karaoke"),
    }
    p1, p2 = _stub_load(pcs=pcs)
    with p1, p2:
        out = _scan_deadlines(include_stale=True)
    assert len(out) == 2


def test_future_due_is_unaffected_by_cutoff():
    """A future deadline should always pass the stale check."""
    pcs = {"pc_future": _pc(due_offset_hours=+48, desc="Future")}
    p1, p2 = _stub_load(pcs=pcs)
    with p1, p2:
        out = _scan_deadlines()
    assert len(out) == 1


def test_sent_status_still_skipped_independent_of_cutoff():
    pcs = {"pc_sent": _pc(due_offset_hours=-24, status="sent", desc="AlreadySent")}
    p1, p2 = _stub_load(pcs=pcs)
    with p1, p2:
        out = _scan_deadlines()
    assert out == []


def test_urgencies_filter_combines_with_cutoff():
    pcs = {
        "pc_recent_overdue": _pc(due_offset_hours=-24, desc="RecentOverdue"),
        "pc_stale": _pc(due_offset_hours=-200, desc="Stale"),
        "pc_critical": _pc(due_offset_hours=2, desc="Critical"),
    }
    p1, p2 = _stub_load(pcs=pcs)
    with p1, p2:
        out = _scan_deadlines(urgencies={"overdue", "critical"})
    descs = sorted(o["pc_number"] for o in out)
    assert descs == ["Critical", "RecentOverdue"]


def test_cutoff_constant_matches_quote_triage():
    """Sanity: the cutoff in routes_deadlines must equal the one in
    quote_triage so sidebar and queue agree on what's actionable."""
    from src.core.quote_triage import _STALE_OVERDUE_HOURS as TRIAGE_CUTOFF
    assert _STALE_OVERDUE_HOURS == TRIAGE_CUTOFF
