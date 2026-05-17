"""The Spine — auto-pricer (cost carry-forward).

Pure-function tests on carry_forward_costs(). The ingest hookup that
calls this lives in a follow-up PR; this file covers the substrate's
contract: what carries, what doesn't, and what gets delta-flagged.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.spine.auto_pricer import (
    COST_CARRY_PREFIX,
    carry_forward_costs,
    parse_carry_note,
)
from src.spine.model import LineItem, Quote


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


def _fresh() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _stale() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=60)


def _line(
    line_no: int,
    *,
    mfg_number: str | None = "MFG-1",
    cost_cents: int = 5000,
    unit_price_cents: int = 6750,
    cost_source_url: str | None = "https://supplier.example.com/sku",
    cost_validated_at: datetime | None = None,
    cost_hand_validated_note: str | None = None,
    description: str | None = None,
) -> LineItem:
    return LineItem(
        line_no=line_no,
        description=description or f"Item {line_no}",
        mfg_number=mfg_number,
        qty=1,
        uom="EA",
        cost_cents=cost_cents,
        cost_source_url=cost_source_url,
        cost_validated_at=cost_validated_at or _fresh(),
        cost_hand_validated_note=cost_hand_validated_note,
        unit_price_cents=unit_price_cents,
    )


def _quote(quote_id: str, line_items: list[LineItem]) -> Quote:
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="SATF",
        solicitation_number="10847262",
        line_items=line_items,
        tax_rate_bps=825,
    )


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────


def test_carry_cost_on_mfg_match_with_zero_target_cost():
    pc = _quote("pc-prior-001", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
        _line(2, mfg_number="MFG-B", cost_cents=8800),
    ])
    rfq = _quote("rfq-new-001", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
        _line(2, mfg_number="MFG-B", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, summary = carry_forward_costs(rfq, pc)
    assert new_rfq.line_items[0].cost_cents == 4200
    assert new_rfq.line_items[1].cost_cents == 8800
    assert summary["source_quote_id"] == "pc-prior-001"
    assert len(summary["carried"]) == 2
    assert summary["carried"][0]["target_line_no"] == 1
    assert summary["carried"][0]["source_line_no"] == 1
    assert summary["carried"][0]["cost_cents"] == 4200


def test_carry_preserves_unit_price_field_not_touched():
    """unit_price_cents is operator-chosen — NEVER carried."""
    pc = _quote("pc-prior-002", [
        _line(1, mfg_number="MFG-A", cost_cents=4200, unit_price_cents=5500),
    ])
    rfq = _quote("rfq-new-002", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, _ = carry_forward_costs(rfq, pc)
    assert new_rfq.line_items[0].cost_cents == 4200
    assert new_rfq.line_items[0].unit_price_cents == 0  # untouched


def test_carry_copies_cost_source_url():
    pc = _quote("pc-prior-003", [
        _line(1, mfg_number="MFG-A", cost_cents=4200,
              cost_source_url="https://supplier-x.com/sku/A"),
    ])
    rfq = _quote("rfq-new-003", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, _ = carry_forward_costs(rfq, pc)
    assert new_rfq.line_items[0].cost_source_url == "https://supplier-x.com/sku/A"


def test_carry_copies_cost_validated_at_verbatim():
    """We preserve the source's validated_at so the freshness gate fires
    correctly on stale carries — operator has to re-validate before
    FINALIZED transition."""
    pc_ts = _stale()
    pc = _quote("pc-prior-004", [
        _line(1, mfg_number="MFG-A", cost_cents=4200, cost_validated_at=pc_ts),
    ])
    rfq = _quote("rfq-new-004", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, _ = carry_forward_costs(rfq, pc)
    assert new_rfq.line_items[0].cost_validated_at == pc_ts


# ──────────────────────────────────────────────────────────────────────
# Note stamping + provenance round-trip
# ──────────────────────────────────────────────────────────────────────


def test_carry_stamps_note_with_provenance():
    pc = _quote("pc-prior-005", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-005", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, _ = carry_forward_costs(rfq, pc)
    note = new_rfq.line_items[0].cost_hand_validated_note
    assert note is not None
    assert note.startswith(COST_CARRY_PREFIX)
    assert "pc-prior-005:1" in note


def test_carry_preserves_existing_operator_note_after_pipe():
    pc = _quote("pc-prior-006", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-006", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None,
              cost_hand_validated_note="operator-checked on phone with vendor"),
    ])
    new_rfq, _ = carry_forward_costs(rfq, pc)
    note = new_rfq.line_items[0].cost_hand_validated_note
    assert "operator-checked on phone with vendor" in note
    assert note.startswith(COST_CARRY_PREFIX)


def test_parse_carry_note_round_trip():
    pc = _quote("pc-prior-007", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-007", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, _ = carry_forward_costs(rfq, pc)
    parsed = parse_carry_note(new_rfq.line_items[0].cost_hand_validated_note)
    assert parsed is not None
    assert parsed["from_quote_id"] == "pc-prior-007"
    assert parsed["from_line_no"] == 1
    assert parsed["existing_note"] is None


def test_parse_carry_note_returns_none_for_operator_note():
    """Non-stamped operator notes don't parse — caller can distinguish
    "no provenance" from "operator-written note"."""
    assert parse_carry_note(None) is None
    assert parse_carry_note("") is None
    assert parse_carry_note("just an operator note") is None


def test_parse_carry_note_extracts_existing_note_segment():
    pc = _quote("pc-prior-008", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-008", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None,
              cost_hand_validated_note="prior op note"),
    ])
    new_rfq, _ = carry_forward_costs(rfq, pc)
    parsed = parse_carry_note(new_rfq.line_items[0].cost_hand_validated_note)
    assert parsed["existing_note"] == "prior op note"


# ──────────────────────────────────────────────────────────────────────
# Skip rules
# ──────────────────────────────────────────────────────────────────────


def test_skip_when_target_already_priced():
    """If the operator already typed a cost, don't overwrite."""
    pc = _quote("pc-prior-009", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-009", [
        _line(1, mfg_number="MFG-A", cost_cents=9999, unit_price_cents=15000),
    ])
    new_rfq, summary = carry_forward_costs(rfq, pc)
    assert new_rfq.line_items[0].cost_cents == 9999  # untouched
    assert summary["carried"] == []
    assert 1 in summary["skipped_already_priced"]


def test_skip_when_target_has_no_mfg_number():
    pc = _quote("pc-prior-010", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-010", [
        _line(1, mfg_number=None, cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, summary = carry_forward_costs(rfq, pc)
    assert new_rfq.line_items[0].cost_cents == 0
    assert summary["carried"] == []
    assert 1 in summary["skipped_no_mfg"]


def test_skip_when_no_mfg_match_in_source():
    pc = _quote("pc-prior-011", [
        _line(1, mfg_number="MFG-X", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-011", [
        _line(1, mfg_number="MFG-Y", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, summary = carry_forward_costs(rfq, pc)
    assert new_rfq.line_items[0].cost_cents == 0
    assert 1 in summary["skipped_no_match"]


def test_skip_when_source_line_has_zero_cost():
    """PC was a price-check that never got priced. Nothing to carry."""
    pc = _quote("pc-prior-012", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None),
    ])
    rfq = _quote("rfq-new-012", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, summary = carry_forward_costs(rfq, pc)
    assert new_rfq.line_items[0].cost_cents == 0
    assert summary["carried"] == []


# ──────────────────────────────────────────────────────────────────────
# Delta detection
# ──────────────────────────────────────────────────────────────────────


def test_delta_flagged_when_target_priced_differently_than_source():
    """Operator priced at 9999¢; PC charged 4200¢. Both numbers
    preserved in the delta summary so the editor can show "PC charged
    different price last time"."""
    pc = _quote("pc-prior-013", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-013", [
        _line(1, mfg_number="MFG-A", cost_cents=9999, unit_price_cents=15000),
    ])
    new_rfq, summary = carry_forward_costs(rfq, pc)
    assert len(summary["deltas"]) == 1
    d = summary["deltas"][0]
    assert d["target_line_no"] == 1
    assert d["source_line_no"] == 1
    assert d["target_cost_cents"] == 9999
    assert d["source_cost_cents"] == 4200


def test_no_delta_when_costs_match():
    pc = _quote("pc-prior-014", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-014", [
        _line(1, mfg_number="MFG-A", cost_cents=4200, unit_price_cents=6500),
    ])
    _, summary = carry_forward_costs(rfq, pc)
    assert summary["deltas"] == []


# ──────────────────────────────────────────────────────────────────────
# Idempotency
# ──────────────────────────────────────────────────────────────────────


def test_running_twice_no_op_on_second_call():
    """First call carries; second call sees the cost is non-zero and
    skips. The carried row remains stable."""
    pc = _quote("pc-prior-015", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-015", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    first, summary1 = carry_forward_costs(rfq, pc)
    assert summary1["carried"][0]["cost_cents"] == 4200

    second, summary2 = carry_forward_costs(first, pc)
    assert summary2["carried"] == []
    assert 1 in summary2["skipped_already_priced"]
    # The carry stamp + cost are stable.
    assert second.line_items[0].cost_cents == 4200
    assert second.line_items[0].cost_hand_validated_note == \
           first.line_items[0].cost_hand_validated_note


# ──────────────────────────────────────────────────────────────────────
# Edge cases
# ──────────────────────────────────────────────────────────────────────


def test_mfg_normalization_case_insensitive_and_whitespace():
    pc = _quote("pc-prior-016", [
        _line(1, mfg_number="mfg-a", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-016", [
        _line(1, mfg_number="  MFG-A.", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, summary = carry_forward_costs(rfq, pc)
    assert new_rfq.line_items[0].cost_cents == 4200
    assert len(summary["carried"]) == 1


def test_multiple_target_lines_with_same_mfg_all_get_same_source():
    """Target has 2 lines with same MFG#. Both should pick up the
    PC's cost (the source line is the same)."""
    pc = _quote("pc-prior-017", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-017", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
        _line(2, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, summary = carry_forward_costs(rfq, pc)
    assert new_rfq.line_items[0].cost_cents == 4200
    assert new_rfq.line_items[1].cost_cents == 4200
    assert len(summary["carried"]) == 2


def test_source_has_multiple_lines_with_same_mfg_pick_highest_cost():
    """When the PC has the same MFG# on multiple lines (different
    qty/uom), prefer the highest-cost line — most authoritative."""
    pc = _quote("pc-prior-018", [
        _line(1, mfg_number="MFG-A", cost_cents=3000),
        _line(2, mfg_number="MFG-A", cost_cents=5500),
        _line(3, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-018", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    new_rfq, summary = carry_forward_costs(rfq, pc)
    assert new_rfq.line_items[0].cost_cents == 5500
    assert summary["carried"][0]["source_line_no"] == 2


def test_does_not_mutate_input_target():
    """Pure-function contract: input target Quote is not mutated."""
    pc = _quote("pc-prior-019", [
        _line(1, mfg_number="MFG-A", cost_cents=4200),
    ])
    rfq = _quote("rfq-new-019", [
        _line(1, mfg_number="MFG-A", cost_cents=0, unit_price_cents=0,
              cost_source_url=None, cost_hand_validated_note=None),
    ])
    original_cost = rfq.line_items[0].cost_cents
    original_note = rfq.line_items[0].cost_hand_validated_note
    new_rfq, _ = carry_forward_costs(rfq, pc)
    assert rfq.line_items[0].cost_cents == original_cost  # unchanged
    assert rfq.line_items[0].cost_hand_validated_note == original_note
    assert new_rfq is not rfq
