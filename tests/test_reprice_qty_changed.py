"""Tests for `reprice_qty_changed_lines` and `qty_change_summary`.

Enforces Mike's 2026-04-20 rule: PC prices are commitments used to publish
the RFQ for public bidding. The ONLY legal reason to re-derive a price is
that the RFQ qty moved away from the PC qty. Every other line must come
through untouched.

Covers:
  - Only qty_changed lines are passed to the pricer
  - Unchanged lines are NEVER repriced — even if the pricer would happily do it
  - Pricer returning None leaves the line alone but counts it separately
  - Only known price fields are accepted from the pricer (no description/qty override)
  - Audit marker `repriced_reason="qty_change"` set after repricing
  - qty_change_summary surfaces PC qty vs RFQ qty for operator review
"""
from __future__ import annotations

from src.core.pc_rfq_linker import (
    reprice_qty_changed_lines,
    qty_change_summary,
    promote_pc_to_rfq_in_place,
)


def _pricer_double_unit(line):
    """Test pricer: returns new prices derived from (implicit) qty delta.
    Real implementation would call oracle / catalog — tests inject this stub."""
    old = float(line.get("unit_price") or 0)
    return {
        "unit_price": old * 2,
        "bid_price": old * 2,
        "supplier_cost": old * 1.5,
        "markup_pct": 33,
    }


def _pricer_always_none(line):
    return None


def _pricer_tries_to_break_things(line):
    """Evil pricer: tries to change description, qty, MFG# via updates dict."""
    return {
        "unit_price": 999.0,
        "description": "HACKED",
        "quantity": 9999,
        "mfg_number": "FAKE-MFG",
    }


# ── reprice_qty_changed_lines ────────────────────────────────────────────────

def test_only_qty_changed_lines_are_repriced():
    rfq = {"line_items": [
        {"description": "A", "quantity": 5, "unit_price": 10.0,
         "qty_changed": False, "pc_original_qty": 5},
        {"description": "B", "quantity": 20, "unit_price": 10.0,
         "qty_changed": True, "pc_original_qty": 10},
        {"description": "C", "quantity": 3, "unit_price": 15.0,
         "qty_changed": False, "pc_original_qty": 3},
    ]}
    out = reprice_qty_changed_lines(rfq, _pricer_double_unit)
    assert out == {"repriced": 1, "skipped_no_change": 2, "skipped_no_price": 0}

    # A + C untouched (commitment prices preserved)
    assert rfq["line_items"][0]["unit_price"] == 10.0
    assert rfq["line_items"][2]["unit_price"] == 15.0
    assert "repriced_reason" not in rfq["line_items"][0]

    # B repriced, qty_changed flag cleared, audit marker set
    b = rfq["line_items"][1]
    assert b["unit_price"] == 20.0
    assert b["qty_changed"] is False
    assert b["repriced_reason"] == "qty_change"


def test_pricer_returning_none_skips_the_line():
    """When pricer can't find a fresh price (e.g. no catalog match), the line
    keeps its PC price. Counted as `skipped_no_price` so the operator knows."""
    rfq = {"line_items": [
        {"description": "A", "quantity": 20, "unit_price": 10.0,
         "qty_changed": True, "pc_original_qty": 10},
    ]}
    out = reprice_qty_changed_lines(rfq, _pricer_always_none)
    assert out["repriced"] == 0
    assert out["skipped_no_price"] == 1
    # Line preserved — still has the PC price, still flagged for follow-up
    assert rfq["line_items"][0]["unit_price"] == 10.0
    assert rfq["line_items"][0]["qty_changed"] is True


def test_pricer_cannot_overwrite_non_price_fields():
    """A buggy pricer must NOT be able to clobber description/qty/MFG#.
    The allowlist protects the line identity even if the pricer goes rogue."""
    rfq = {"line_items": [
        {"description": "original desc", "quantity": 20, "mfg_number": "REAL-MFG",
         "unit_price": 10.0, "qty_changed": True, "pc_original_qty": 10},
    ]}
    reprice_qty_changed_lines(rfq, _pricer_tries_to_break_things)
    line = rfq["line_items"][0]
    # Accepted price update
    assert line["unit_price"] == 999.0
    # Rejected everything else
    assert line["description"] == "original desc"
    assert line["quantity"] == 20
    assert line["mfg_number"] == "REAL-MFG"


def test_empty_rfq_returns_zeros():
    out = reprice_qty_changed_lines({"line_items": []}, _pricer_double_unit)
    assert out == {"repriced": 0, "skipped_no_change": 0, "skipped_no_price": 0}


def test_no_pricer_does_not_crash():
    """Defensive: if caller passes None, lines are counted as no_price but
    we don't explode."""
    rfq = {"line_items": [
        {"quantity": 20, "unit_price": 10.0, "qty_changed": True,
         "pc_original_qty": 10},
    ]}
    out = reprice_qty_changed_lines(rfq, None)
    assert out["skipped_no_price"] == 1
    assert rfq["line_items"][0]["unit_price"] == 10.0


# ── qty_change_summary ───────────────────────────────────────────────────────

def test_summary_surfaces_pc_vs_rfq_qty_for_each_line():
    rfq = {"line_items": [
        {"description": "Same qty item", "quantity": 5, "unit_price": 10.0,
         "promoted_from_pc": True, "pc_original_qty": 5, "qty_changed": False},
        {"description": "Doubled item", "quantity": 20, "unit_price": 10.0,
         "promoted_from_pc": True, "pc_original_qty": 10, "qty_changed": True},
    ]}
    summary = qty_change_summary(rfq)
    assert len(summary) == 2
    assert summary[0]["qty_changed"] is False
    assert summary[0]["pc_qty"] == 5
    assert summary[0]["rfq_qty"] == 5
    assert summary[1]["qty_changed"] is True
    assert summary[1]["pc_qty"] == 10
    assert summary[1]["rfq_qty"] == 20
    assert summary[1]["current_unit_price"] == 10.0


def test_summary_ignores_non_promoted_lines():
    """If a buyer adds a brand-new line after promote, it has no PC origin —
    the summary skips it (nothing to diff against)."""
    rfq = {"line_items": [
        {"description": "From PC", "quantity": 5, "promoted_from_pc": True,
         "pc_original_qty": 5, "qty_changed": False},
        {"description": "Buyer-added", "quantity": 2},  # no promoted_from_pc
    ]}
    summary = qty_change_summary(rfq)
    assert len(summary) == 1
    assert summary[0]["description"] == "From PC"


# ── End-to-end: promote → summary → reprice ──────────────────────────────────

def test_full_chain_promote_then_selective_reprice():
    """Integration: the three-step dance the route will perform.
    1. promote PC → RFQ (sets flags)
    2. qty_change_summary for operator review
    3. reprice only the flagged lines"""
    rfq = {"line_items": [
        {"mfg_number": "A1", "description": "unchanged", "quantity": 5},
        {"mfg_number": "B2", "description": "buyer bumped qty", "quantity": 30},
    ]}
    pc = {"agency": "CCHCS", "items": [
        {"mfg_number": "A1", "quantity": 5, "unit_price": 10.0, "bid_price": 15.0},
        {"mfg_number": "B2", "quantity": 10, "unit_price": 10.0, "bid_price": 15.0},
    ]}

    promote_result = promote_pc_to_rfq_in_place(rfq, "pc_1", pc)
    assert promote_result["qty_changed"] == 1

    summary = qty_change_summary(rfq)
    assert [s["qty_changed"] for s in summary] == [False, True]

    reprice_result = reprice_qty_changed_lines(rfq, _pricer_double_unit)
    assert reprice_result == {"repriced": 1, "skipped_no_change": 1, "skipped_no_price": 0}

    # A1 kept PC commitment verbatim (no qty change = no reprice)
    assert rfq["line_items"][0]["unit_price"] == 10.0
    assert rfq["line_items"][0]["bid_price"] == 15.0
    # B2 repriced (doubled by stub)
    assert rfq["line_items"][1]["unit_price"] == 20.0
    assert rfq["line_items"][1]["bid_price"] == 20.0
    assert rfq["line_items"][1]["repriced_reason"] == "qty_change"
    assert rfq["line_items"][1]["qty_changed"] is False
