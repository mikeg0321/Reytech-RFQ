"""Tests for `src.core.pc_rfq_linker.preview_pc_link` — the pure-function
preview that replaces the prior two-button + confirm() PC-link UX
(2026-05-26 rfq_0124647e screenshot substrate fix).

The preview is what the operator sees BEFORE clicking Apply. It must
mirror exactly what `promote_pc_to_rfq_in_place` would do — same match
order (mfg → upc → desc → positional), same qty-changed flagging.

Invariants under test:
  * Pure function — preview never mutates the RFQ
  * Matches are claimed at most once (a PC line can't be ported twice)
  * `qty_changed=True` when RFQ qty != PC qty (the re-quote trigger)
  * `subtotal_*_if_ported` uses RFQ qty (that's what the bid is for)
  * Order of match priority (mfg/upc → desc → positional)
"""
from __future__ import annotations

import copy

import pytest


def _make_pc(items):
    return {
        "id": "pc_test_001",
        "pc_number": "PC2026-0042",
        "pc_data": {
            "pc_number": "PC2026-0042",
            "items": items,
        },
    }


def _make_rfq(items):
    return {
        "id": "rfq_test_001",
        "line_items": items,
        "items": items,
    }


class TestPreviewPurity:
    """preview_pc_link must NEVER mutate inputs — this is what makes it
    safe to call in the suggestions endpoint without locking."""

    def test_preview_does_not_mutate_rfq(self):
        from src.core.pc_rfq_linker import preview_pc_link
        rfq = _make_rfq([{"description": "Widget A", "quantity": 10, "mfg_number": "W-1"}])
        pc = _make_pc([{
            "description": "Widget A", "quantity": 5, "mfg_number": "W-1",
            "supplier_cost": 5.00, "price_per_unit": 7.50,
        }])
        rfq_snapshot = copy.deepcopy(rfq)
        preview_pc_link(rfq, pc)
        assert rfq == rfq_snapshot, "preview must not mutate RFQ"

    def test_preview_does_not_mutate_pc(self):
        from src.core.pc_rfq_linker import preview_pc_link
        rfq = _make_rfq([{"description": "Widget A", "quantity": 10, "mfg_number": "W-1"}])
        pc = _make_pc([{
            "description": "Widget A", "quantity": 5, "mfg_number": "W-1",
            "supplier_cost": 5.00, "price_per_unit": 7.50,
        }])
        pc_snapshot = copy.deepcopy(pc)
        preview_pc_link(rfq, pc)
        assert pc == pc_snapshot, "preview must not mutate PC"


class TestMatchOrder:
    """MFG/UPC > desc > positional. Exact match wins regardless of order
    in the items array (the operator-confirm gate depends on this)."""

    def test_mfg_match_beats_position(self):
        from src.core.pc_rfq_linker import preview_pc_link
        rfq = _make_rfq([
            {"description": "Mystery Item", "quantity": 1, "mfg_number": "M-9"},
        ])
        # PC has the MFG match at index 1, a positional-similar item at 0
        pc = _make_pc([
            {"description": "Mystery Item", "quantity": 99, "mfg_number": "M-DIFFERENT",
             "supplier_cost": 100.0, "price_per_unit": 150.0},
            {"description": "Different Description", "quantity": 1, "mfg_number": "M-9",
             "supplier_cost": 5.0, "price_per_unit": 7.5},
        ])
        result = preview_pc_link(rfq, pc)
        line = result["lines"][0]
        assert line["match_kind"] == "mfg", line
        assert line["pc_idx"] == 1
        assert line["pc_unit_cost"] == 5.0


class TestQtyChangedFlag:
    """Drift detection — RFQ qty 10, PC qty 5 → qty_changed=True. The UI
    badge + the re-quote-drifted action both depend on this flag."""

    def test_qty_drift_flagged(self):
        from src.core.pc_rfq_linker import preview_pc_link
        rfq = _make_rfq([{"description": "Widget A", "quantity": 10, "mfg_number": "W-1"}])
        pc = _make_pc([{
            "description": "Widget A", "quantity": 5, "mfg_number": "W-1",
            "supplier_cost": 5.00, "price_per_unit": 7.50,
        }])
        result = preview_pc_link(rfq, pc)
        line = result["lines"][0]
        assert line["qty_changed"] is True
        assert line["pc_qty"] == 5
        assert line["rfq_qty"] == 10
        assert line["drift_pct"] == 100.0, "drift = (10-5)/5 = +100%"
        assert result["totals"]["qty_changed"] == 1

    def test_qty_match_not_flagged(self):
        from src.core.pc_rfq_linker import preview_pc_link
        rfq = _make_rfq([{"description": "Widget A", "quantity": 5, "mfg_number": "W-1"}])
        pc = _make_pc([{
            "description": "Widget A", "quantity": 5, "mfg_number": "W-1",
            "supplier_cost": 5.00, "price_per_unit": 7.50,
        }])
        result = preview_pc_link(rfq, pc)
        line = result["lines"][0]
        assert line["qty_changed"] is False
        assert line["drift_pct"] == 0.0
        assert result["totals"]["qty_changed"] == 0


class TestTotalsProjection:
    """Subtotals use RFQ qty — that's what the bid will be for, regardless
    of the PC qty. The footer is the operator's "Apply this would generate
    a $X bid" expectation."""

    def test_totals_use_rfq_qty(self):
        from src.core.pc_rfq_linker import preview_pc_link
        rfq = _make_rfq([
            {"description": "A", "quantity": 10, "mfg_number": "M-A"},
            {"description": "B", "quantity": 20, "mfg_number": "M-B"},
        ])
        pc = _make_pc([
            {"description": "A", "quantity": 5, "mfg_number": "M-A",
             "supplier_cost": 1.00, "price_per_unit": 2.00},
            {"description": "B", "quantity": 5, "mfg_number": "M-B",
             "supplier_cost": 3.00, "price_per_unit": 6.00},
        ])
        result = preview_pc_link(rfq, pc)
        # cost: 1*10 + 3*20 = 70; bid: 2*10 + 6*20 = 140
        assert result["totals"]["subtotal_cost_if_ported"] == 70.0
        assert result["totals"]["subtotal_bid_if_ported"] == 140.0
        assert result["totals"]["matched"] == 2


class TestNoMatch:
    """Lines with no PC counterpart still appear in preview with
    match_kind=None so the operator sees they exist and weren't covered."""

    def test_unmatched_rfq_line_shown(self):
        from src.core.pc_rfq_linker import preview_pc_link
        rfq = _make_rfq([
            {"description": "Widget Matched", "quantity": 1, "mfg_number": "M-MATCH"},
            {"description": "Completely Unrelated Thing Xyz", "quantity": 1, "mfg_number": "M-NO-PC"},
        ])
        pc = _make_pc([
            {"description": "Widget Matched", "quantity": 1, "mfg_number": "M-MATCH",
             "supplier_cost": 5.0, "price_per_unit": 7.0},
            # Note: second PC item is consumed by positional fallback for
            # the unmatched RFQ row — preview reflects what promote would do.
            {"description": "Unmatched PC widget", "quantity": 1, "mfg_number": "M-OTHER",
             "supplier_cost": 99.0, "price_per_unit": 99.0},
        ])
        result = preview_pc_link(rfq, pc)
        assert len(result["lines"]) == 2
        # First line: mfg match
        assert result["lines"][0]["match_kind"] == "mfg"
        # Second line: positional fallback (because PC has a second item
        # available at the same index). This mirrors promote_pc_to_rfq's
        # positional fallback — the operator sees "~ position" badge and
        # decides whether to accept.
        assert result["lines"][1]["match_kind"] == "positional"


class TestClaimedOnce:
    """A single PC line cannot be ported to two different RFQ lines.
    Without this, an exact-match in two places would double-port."""

    def test_pc_line_claimed_only_once(self):
        from src.core.pc_rfq_linker import preview_pc_link
        # Two RFQ lines with the same description, only one PC line
        rfq = _make_rfq([
            {"description": "Widget Same Desc", "quantity": 5},
            {"description": "Widget Same Desc", "quantity": 5},
        ])
        pc = _make_pc([{
            "description": "Widget Same Desc", "quantity": 5,
            "supplier_cost": 10.0, "price_per_unit": 15.0,
        }])
        result = preview_pc_link(rfq, pc)
        matched = [L for L in result["lines"] if L["match_kind"] is not None]
        # Only one of the two duplicate RFQ lines can claim the PC item
        assert len(matched) <= 1, (
            f"PC line was claimed more than once: {result['lines']}"
        )


class TestCurrentlyPricedFlag:
    """When the RFQ already has cost on a line, preview marks it so the
    operator sees `overwrite` tag in the banner UI."""

    def test_currently_priced_flag(self):
        from src.core.pc_rfq_linker import preview_pc_link
        rfq = _make_rfq([{
            "description": "Widget Priced",
            "quantity": 5,
            "mfg_number": "M-P",
            "supplier_cost": 99.99,  # already priced
        }])
        pc = _make_pc([{
            "description": "Widget Priced", "quantity": 5, "mfg_number": "M-P",
            "supplier_cost": 10.0, "price_per_unit": 15.0,
        }])
        result = preview_pc_link(rfq, pc)
        assert result["lines"][0]["currently_priced"] is True
