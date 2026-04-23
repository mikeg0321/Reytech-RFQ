"""LOE estimator + triage sorter — covers Mike's mental model.

Mental model documented in project_deadline_defaults_body_key_bug.md
follow-up: sort by (time_remaining, LOE), escalate to EMERGENCY when
hours_left * 60 < loe_minutes * 1.25.
"""
from __future__ import annotations

import pytest

from src.core.loe_estimator import estimate_loe_minutes, loe_label
from src.core.quote_triage import triage


# ── LOE estimator ───────────────────────────────────────────────────────────
# Mike's tiered model (2026-04-22): LOE = human review time assuming app works.
#   1–5 items  → 15 min
#   6–15       → 30 min
#   16–29      → 1 hour
#   30+        → 2 hours
# Agency/packet shape does NOT bump LOE (app automates assembly).

def test_loe_tier_small():
    """1–5 items → 15 min flat."""
    assert estimate_loe_minutes({"items": [{}]}) == 15
    assert estimate_loe_minutes({"items": [{}] * 5}) == 15


def test_loe_tier_medium():
    """6–15 items → 30 min flat."""
    assert estimate_loe_minutes({"items": [{}] * 6}) == 30
    assert estimate_loe_minutes({"items": [{}] * 15}) == 30


def test_loe_tier_large():
    """16–29 items → 60 min (1 hour)."""
    assert estimate_loe_minutes({"items": [{}] * 16}) == 60
    assert estimate_loe_minutes({"items": [{}] * 29}) == 60


def test_loe_tier_xl_30plus():
    """30+ items → 120 min (2 hours) — matches Mike's '30+ is prob 2 hours'."""
    assert estimate_loe_minutes({"items": [{}] * 30}) == 120
    assert estimate_loe_minutes({"items": [{}] * 35}) == 120
    assert estimate_loe_minutes({"items": [{}] * 100}) == 120


def test_loe_agency_does_not_bump():
    """If the app works, CCHCS review takes the same time as CDCR review."""
    cdcr = estimate_loe_minutes({"items": [{}] * 4, "institution": "CDCR"})
    cchcs = estimate_loe_minutes({"items": [{}] * 4, "institution": "CCHCS"})
    assert cdcr == cchcs == 15


def test_loe_parse_failed_adds_recovery():
    """Parse failure → +15 min manual recovery (the one remaining bump)."""
    doc = {"items": [{}], "institution": "CDCR", "_parse_failed": True}
    assert estimate_loe_minutes(doc) == 15 + 15


def test_loe_empty_doc_floors_to_smallest_tier():
    """Empty doc still lands in the 1–5 tier (15 min)."""
    assert estimate_loe_minutes({}) == 15
    assert estimate_loe_minutes({"items": []}) == 15


def test_loe_handles_non_dict():
    """Defensive — don't blow up on None or list inputs."""
    assert estimate_loe_minutes(None) == 15
    assert estimate_loe_minutes([]) == 15


def test_loe_line_items_key_also_counts():
    """RFQs use line_items, not items."""
    doc = {"line_items": [{}] * 8, "institution": "CDCR"}
    assert estimate_loe_minutes(doc) == 30  # 6–15 tier


def test_loe_label_formats():
    assert loe_label(15) == "15 min"
    assert loe_label(30) == "30 min"
    assert loe_label(45) == "45 min"
    assert loe_label(60) == "1.0 h"
    assert loe_label(120) == "2 h"


# ── Triage sorter ───────────────────────────────────────────────────────────

def _mk(hours_left, loe_minutes, **extra):
    """Build a minimal deadline dict."""
    d = {
        "hours_left": hours_left,
        "loe_minutes": loe_minutes,
        "doc_id": extra.pop("doc_id", f"{hours_left}h-{loe_minutes}m"),
    }
    d.update(extra)
    return d


def test_triage_normal_mode_sorts_due_asc_then_loe_asc():
    """Mike's "15 hours left, 2 quotes, easy first" case.

    Both items have 15h remaining, neither is tight. Easy (20 min) must come
    before hard (180 min) — LOE is the tiebreaker at same urgency.
    """
    easy = _mk(15.0, 20, doc_id="easy")
    hard = _mk(15.0, 180, doc_id="hard")
    out = triage([hard, easy])
    assert out["mode"] == "normal"
    assert out["queue"][0]["doc_id"] == "easy"
    assert out["queue"][1]["doc_id"] == "hard"


def test_triage_emergency_when_time_tight_vs_loe():
    """Mike's "hard due in 45 min, don't touch anything else" case.

    Hard: 0.75h (45 min) remaining, 180 min LOE → 45 < 180 * 1.25 = EMERGENCY.
    Easy: 15h remaining, 20 min LOE → plenty of slack, stays in queue.
    """
    hard = _mk(0.75, 180, doc_id="hard")
    easy = _mk(15.0, 20, doc_id="easy")
    out = triage([easy, hard])
    assert out["mode"] == "emergency"
    assert out["emergency"][0]["doc_id"] == "hard"
    assert out["queue"][0]["doc_id"] == "easy"


def test_triage_earlier_deadline_wins_when_both_comfortable():
    """If neither is tight, due-asc dominates LOE-asc."""
    soon_hard = _mk(3.0, 120, doc_id="soon_hard")   # 180m > 120*1.25=150 → comfy
    later_easy = _mk(8.0, 20, doc_id="later_easy")
    out = triage([later_easy, soon_hard])
    assert out["mode"] == "normal"
    assert out["queue"][0]["doc_id"] == "soon_hard"
    assert out["queue"][1]["doc_id"] == "later_easy"


def test_triage_recent_overdue_stays_actionable():
    """Overdue by < 72h → still in queue (Mike can still salvage)."""
    recent = _mk(-12.0, 30, doc_id="recent")   # 12h late
    out = triage([recent])
    assert out["queue"] == [recent]
    assert out["stale_overdue_count"] == 0


def test_triage_stale_overdue_drops_to_count():
    """Overdue > 72h → collapsed into a count, off the main view."""
    stale = _mk(-100.0, 30, doc_id="stale")
    out = triage([stale])
    assert out["queue"] == []
    assert out["emergency"] == []
    assert out["stale_overdue_count"] == 1


def test_triage_no_deadline_sinks_to_bottom():
    """Records with no due_date parsed still show, but after real deadlines."""
    dated = _mk(5.0, 20, doc_id="dated")
    undated = _mk(None, 20, doc_id="undated")
    out = triage([undated, dated])
    assert out["queue"][0]["doc_id"] == "dated"
    assert out["queue"][1]["doc_id"] == "undated"


def test_triage_empty_input():
    out = triage([])
    assert out == {
        "emergency": [],
        "queue": [],
        "stale_overdue_count": 0,
        "mode": "normal",
    }


def test_triage_emergency_sort_most_tight_first():
    """When multiple are in emergency mode, most-tight wins."""
    slightly = _mk(1.0, 60, doc_id="slightly")   # 60 < 60*1.25=75 → emergency
    very = _mk(0.25, 60, doc_id="very")          # 15 < 75 → emergency, much tighter
    out = triage([slightly, very])
    assert out["mode"] == "emergency"
    assert out["emergency"][0]["doc_id"] == "very"
    assert out["emergency"][1]["doc_id"] == "slightly"
