"""Unit tests for award_tracker's calibration helpers.

These gate the runtime SCPRS→Oracle feedback loop. When SCPRS detects we
lost a quote, award_tracker.run_award_check calls calibrate_from_outcome
via these two pure helpers:

  - _loss_reason_for_calibration: maps award_tracker's richer
    loss_reason_class ('price_higher', 'margin_too_high',
    'relationship_incumbent', 'cost_too_high', 'unclear') down to the
    two-valued `loss_reason` enum calibrate_from_outcome expects
    ('price' | 'other'). Only 'price' bumps loss_on_price + feeds the
    avg_losing_delta EMA; 'other' only bumps loss_on_other.

  - _winner_prices_from_analysis: builds the {our_items_idx:
    competitor_unit_price} dict calibrate_from_outcome wants.
    _analyze_loss drops items without descriptions and stores
    `description[:80]` as `our_description`, so this matches on that
    same slice to keep indices aligned with the ORIGINAL our_items list
    that gets passed to calibrate.

Pure functions, no DB, no Oracle, no file I/O — fast and deterministic.
"""
from __future__ import annotations

from src.agents.award_tracker import (
    _loss_reason_for_calibration,
    _winner_prices_from_analysis,
)


# ── _loss_reason_for_calibration ─────────────────────────────────────────────

def test_loss_reason_price_higher_maps_to_price():
    assert _loss_reason_for_calibration("price_higher") == "price"


def test_loss_reason_margin_too_high_maps_to_price():
    # margin_too_high = we had cheaper cost but over-marked it up; still
    # a price-side loss signal for Oracle.
    assert _loss_reason_for_calibration("margin_too_high") == "price"


def test_loss_reason_relationship_incumbent_maps_to_other():
    # We were cheaper but lost anyway — not a price signal.
    assert _loss_reason_for_calibration("relationship_incumbent") == "other"


def test_loss_reason_cost_too_high_maps_to_other():
    # Our cost basis exceeds winner's sell — can't compete on price so
    # calibrate shouldn't pretend this is a markdown signal.
    assert _loss_reason_for_calibration("cost_too_high") == "other"


def test_loss_reason_unknown_class_defaults_to_other():
    # Defensive: any unclassified value must fall to 'other' so the EMA
    # never gets a bogus avg_losing_delta.
    assert _loss_reason_for_calibration("unclear") == "other"
    assert _loss_reason_for_calibration("") == "other"


# ── _winner_prices_from_analysis ─────────────────────────────────────────────

def test_winner_prices_builds_idx_price_dict():
    our_items = [
        {"description": "Widget A", "qty": 1, "unit_price": 50.0},
        {"description": "Gadget B", "qty": 2, "unit_price": 75.0},
    ]
    line_comparison = [
        {"matched": True, "our_description": "Widget A", "winner_unit_price": 40.0},
        {"matched": True, "our_description": "Gadget B", "winner_unit_price": 60.0},
    ]
    result = _winner_prices_from_analysis(our_items, line_comparison)
    assert result == {0: 40.0, 1: 60.0}


def test_winner_prices_skips_items_without_description():
    # _analyze_loss drops empty-description items from line_comparison,
    # but our_items keeps them. Keyed indices must align with original
    # our_items, so index 0 (no desc) gets skipped, index 1 gets price.
    our_items = [
        {"description": "", "qty": 1, "unit_price": 10.0},
        {"description": "Real Item", "qty": 1, "unit_price": 20.0},
    ]
    line_comparison = [
        {"matched": True, "our_description": "Real Item", "winner_unit_price": 15.0},
    ]
    result = _winner_prices_from_analysis(our_items, line_comparison)
    assert result == {1: 15.0}
    assert 0 not in result


def test_winner_prices_skips_unmatched_comps():
    # Unmatched comps (no PO line found) contribute no winner price.
    our_items = [
        {"description": "Widget A", "qty": 1},
        {"description": "Widget B", "qty": 1},
    ]
    line_comparison = [
        {"matched": True, "our_description": "Widget A", "winner_unit_price": 30.0},
        {"matched": False, "our_description": "Widget B", "winner_unit_price": 0},
    ]
    result = _winner_prices_from_analysis(our_items, line_comparison)
    assert result == {0: 30.0}
    assert 1 not in result


def test_winner_prices_handles_bad_winner_unit_price():
    # Defensive: bad values must not crash calibrate — skip them.
    our_items = [
        {"description": "Widget A"},
        {"description": "Widget B"},
        {"description": "Widget C"},
    ]
    line_comparison = [
        {"matched": True, "our_description": "Widget A", "winner_unit_price": None},
        {"matched": True, "our_description": "Widget B", "winner_unit_price": "abc"},
        {"matched": True, "our_description": "Widget C", "winner_unit_price": 42.0},
    ]
    result = _winner_prices_from_analysis(our_items, line_comparison)
    # None is falsy, skipped silently; "abc" raises ValueError, skipped;
    # 42.0 survives.
    assert result == {2: 42.0}


def test_winner_prices_empty_inputs():
    assert _winner_prices_from_analysis([], []) == {}
    assert _winner_prices_from_analysis(
        [{"description": "x"}], []
    ) == {}
    assert _winner_prices_from_analysis(
        [], [{"matched": True, "our_description": "x", "winner_unit_price": 5}]
    ) == {}


def test_winner_prices_uses_description_80_slice():
    # _analyze_loss stores description[:80] as our_description — helper
    # must match on the same slice, not the full string.
    long_desc = "A" * 100  # 100 chars
    our_items = [{"description": long_desc}]
    line_comparison = [
        {"matched": True, "our_description": "A" * 80, "winner_unit_price": 99.0},
    ]
    result = _winner_prices_from_analysis(our_items, line_comparison)
    assert result == {0: 99.0}


def test_winner_prices_falls_back_to_name_when_description_missing():
    # our_items may use 'name' instead of 'description' (PC/RFQ schema
    # drift). Helper must handle both.
    our_items = [{"name": "Fallback Item", "qty": 1}]
    line_comparison = [
        {"matched": True, "our_description": "Fallback Item", "winner_unit_price": 25.0},
    ]
    result = _winner_prices_from_analysis(our_items, line_comparison)
    assert result == {0: 25.0}
