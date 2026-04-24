"""Regression: triage queue sort prefers real deadlines over defaults.

Incident 2026-04-23: Mike's home queue showed 11 PCs all stamped with
"16.8h left" because their `due_date_source = "default"` (deadline
extractor failed to find a buyer date in the email). A 12th PC with a
real header-extracted deadline at "24h left" was buried below the
fakes — operator couldn't tell which was the real-signal one.

Sort key changed from `(hours_left, loe_minutes)` to
`(is_default, hours_left, loe_minutes)` so real beats fake within
each urgency band. Defaults still sort by time among themselves.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.core.quote_triage import triage


def _row(*, hours_left, loe=15, source="header", pc_number="X"):
    return {
        "pc_number": pc_number,
        "hours_left": hours_left,
        "loe_minutes": loe,
        "due_date_source": source,
    }


def test_real_deadline_beats_default_at_same_hours():
    """Two future PCs at the same hours_left — header wins."""
    rows = [
        _row(hours_left=16.8, source="default", pc_number="DEF"),
        _row(hours_left=16.8, source="header",  pc_number="REAL"),
    ]
    out = triage(rows)
    queue = out["queue"]
    assert [q["pc_number"] for q in queue] == ["REAL", "DEF"]


def test_real_at_longer_horizon_still_beats_default_at_shorter():
    """A real deadline 24h out beats a default 16h out — Mike's pain
    case. The single real-signal record should never be buried below
    a sea of fake-stamped 'tomorrow 2pm' defaults."""
    rows = [
        _row(hours_left=16.8, source="default", pc_number="FAKE_A"),
        _row(hours_left=16.8, source="default", pc_number="FAKE_B"),
        _row(hours_left=16.8, source="default", pc_number="FAKE_C"),
        _row(hours_left=24.0, source="header",  pc_number="REAL"),
    ]
    out = triage(rows)
    # REAL should be at the top of the queue even though its
    # hours_left is larger.
    assert out["queue"][0]["pc_number"] == "REAL"


def test_email_source_treated_as_real():
    """`email` source is real-signal too — extracted from the buyer's
    own message, not a fallback."""
    rows = [
        _row(hours_left=10, source="default", pc_number="DEF"),
        _row(hours_left=20, source="email",   pc_number="EMAIL"),
    ]
    out = triage(rows)
    assert out["queue"][0]["pc_number"] == "EMAIL"


def test_defaults_among_themselves_still_sort_by_time():
    """Within the default group, time-asc still applies — no chaos."""
    rows = [
        _row(hours_left=40, source="default", pc_number="LATE"),
        _row(hours_left=10, source="default", pc_number="SOON"),
        _row(hours_left=25, source="default", pc_number="MID"),
    ]
    out = triage(rows)
    assert [q["pc_number"] for q in out["queue"]] == ["SOON", "MID", "LATE"]


def test_real_among_themselves_still_sort_by_time():
    rows = [
        _row(hours_left=40, source="header", pc_number="LATE"),
        _row(hours_left=10, source="email",  pc_number="SOON"),
        _row(hours_left=25, source="header", pc_number="MID"),
    ]
    out = triage(rows)
    assert [q["pc_number"] for q in out["queue"]] == ["SOON", "MID", "LATE"]


def test_loe_asc_still_breaks_ties_within_source_and_time_band():
    """Same source, same hours_left — easier (lower LOE) first."""
    rows = [
        _row(hours_left=12, loe=60, source="header", pc_number="HARD"),
        _row(hours_left=12, loe=15, source="header", pc_number="EASY"),
    ]
    out = triage(rows)
    assert out["queue"][0]["pc_number"] == "EASY"


def test_missing_source_treated_as_real():
    """Records without due_date_source (legacy data) shouldn't be
    penalized — only `default` is explicitly demoted."""
    rows = [
        _row(hours_left=10, source="default", pc_number="DEF"),
        {"pc_number": "LEGACY", "hours_left": 20, "loe_minutes": 15},
    ]
    out = triage(rows)
    # LEGACY (no source) sorts as real → first.
    assert out["queue"][0]["pc_number"] == "LEGACY"
