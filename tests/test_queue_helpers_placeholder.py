"""Tests for queue_helpers placeholder-hiding + needs_review status display.

2026-04-29 follow-up to PR #662 (kill subject[:40] fallback). The DB
flips were applied but the queue templates still rendered "PC #GOOD"
and "RFQ #WORKSHEET" because normalize_queue_item() passed the raw
pc_number / sol# through verbatim.

This test file locks the placeholder-hiding behavior + the new
'Needs Review' status display + color so future template changes
don't silently regress.
"""
from __future__ import annotations

import pytest


def _norm():
    from src.core.queue_helpers import normalize_queue_item
    return normalize_queue_item


def _is_ph():
    from src.core.queue_helpers import _is_placeholder_number
    return _is_placeholder_number


# ── Placeholder detection ──────────────────────────────────────────


def test_is_placeholder_flags_legacy_junk():
    fn = _is_ph()
    assert fn("GOOD") is True
    assert fn("WORKSHEET") is True
    assert fn("RFQ") is True
    assert fn("unknown") is True
    assert fn("(blank)") is True
    assert fn("") is True
    assert fn(None) is True


def test_is_placeholder_flags_auto_prefix():
    """AUTO_<id> is the new ingest_v2 placeholder. It's deterministic
    (so we can grep audit logs), but to a human looking at the queue
    it's still 'no real number' — render Pending."""
    fn = _is_ph()
    assert fn("AUTO_abcd1234") is True


def test_is_placeholder_passes_real_numbers():
    fn = _is_ph()
    assert fn("10840486") is False         # CDCR PR number
    assert fn("8955-00012345") is False    # CalVet 8955-prefixed
    assert fn("25-067MC") is False         # Solicitation with letters
    assert fn("RT Supplies") is False      # Filename-derived (mixed case)


# ── normalize_queue_item: number hiding ────────────────────────────


def test_pc_with_real_number_passes_through():
    raw = {
        "id": "pc_x",
        "pc_number": "10840486",
        "status": "parsed",
        "items": [{"qty": 1, "description": "Widget"}],
    }
    out = _norm()(raw, "pc", "pc_x")
    assert out["number"] == "10840486"


# ── needs_review status display + color ────────────────────────────


def test_needs_review_status_displays_as_needs_review():
    raw = {"id": "x", "pc_number": "", "status": "needs_review"}
    out = _norm()(raw, "pc", "x")
    assert out["display_status"] == "Needs Review"


def test_needs_review_status_uses_orange_color():
    """Orange (#f0883e) matches the existing 'critical' urgency color
    so it reads as 'triage-required' at a glance, not red ('done bad')
    or blue ('new and ready')."""
    raw = {"id": "x", "pc_number": "", "status": "needs_review"}
    out = _norm()(raw, "pc", "x")
    assert out["status_color"] == "#f0883e"


def test_unknown_status_falls_back_to_default():
    raw = {"id": "x", "pc_number": "", "status": "some_new_status_we_dont_know"}
    out = _norm()(raw, "pc", "x")
    assert out["display_status"] == "New"  # current fallback in STATUS_DISPLAY


# ── Combined: needs_review + placeholder ───────────────────────────


def test_keith_alsing_pair_renders_correctly():
    """End-to-end shape for the Keith Alsing pair after PR-A backfill:
    status=needs_review, pc_number=GOOD. Number-hiding for legacy junk now
    lives in routes_deadlines.py / home.html (operator-facing JSON paths) —
    queue normalize lets `_resolve_display_number` (PR #733) handle the
    AUTO_<hex>→title cascade."""
    pc_raw = {
        "id": "pc_a391db8f",
        "pc_number": "GOOD",
        "status": "needs_review",
        "items": [],
        "institution": "CalVet",
    }
    out = _norm()(pc_raw, "pc", "pc_a391db8f")
    assert out["display_status"] == "Needs Review"
    assert out["status_color"] == "#f0883e"
    assert out["item_count"] == 0
