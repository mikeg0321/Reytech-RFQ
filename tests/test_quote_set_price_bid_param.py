"""Tier 1b — `Quote.set_price(bid_price=...)` (audit 2026-05-07).

Audit's framing: the canonical model `Quote.set_price()` could not
represent "preserve operator bid when cost changes" because it had
no parameter for a target bid. The cost-change-recomputes-bid silent
overwrite shipped real prod incidents (Cortech mattress 2026-04-21).

This test file pins the new bid_price parameter so the model can hold
a bid constant across cost changes by deriving markup_pct on the fly,
mirroring pricing_math.reconcile_items semantics on the pydantic side.

Backward-compat tests confirm the old `set_price(line_no, cost, markup_pct)`
shape is unchanged when `bid_price` is omitted.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.core.quote_model import LineItem, Quote


def _quote_with_one_line(*, cost="10.00", markup="35", scprs="0",
                         catalog="0"):
    """Helper: create a single-line Quote in MANUAL price_source state."""
    item = LineItem(
        line_no=1,
        description="Widget",
        unit_cost=Decimal(cost),
        markup_pct=Decimal(markup),
        scprs_price=Decimal(scprs),
        catalog_cost=Decimal(catalog),
    )
    q = Quote(doc_type="pc", doc_id="test_pc_1", line_items=[item])
    return q


def test_set_price_old_signature_unchanged_when_bid_price_omitted():
    """Existing callers (no bid_price) get the same behavior as before."""
    q = _quote_with_one_line(cost="5.00", markup="20")
    q.set_price(line_no=1, unit_cost=Decimal("12.00"), markup_pct=Decimal("40"))
    item = q.line_items[0]
    assert item.unit_cost == Decimal("12.00")
    assert item.markup_pct == Decimal("40")
    # unit_price computed: 12 * 1.40 = 16.80
    assert item.unit_price == Decimal("16.80")
    assert item.price_source == "manual"


def test_set_price_with_bid_derives_markup():
    """When bid_price is provided, markup_pct is derived from (bid-cost)/cost."""
    q = _quote_with_one_line()
    # Operator wants bid $13.50 at cost $10.00 → markup 35.00%
    q.set_price(line_no=1, unit_cost=Decimal("10.00"),
                bid_price=Decimal("13.50"))
    item = q.line_items[0]
    assert item.unit_cost == Decimal("10.00")
    assert item.markup_pct == Decimal("35.00")
    # Round-trip: unit_price computed from cost × (1+markup/100) lands on
    # the operator's bid (within rounding).
    assert item.unit_price == Decimal("13.50")


def test_set_price_with_bid_preserves_bid_across_cost_change():
    """The fail shape this PR was written to close.

    Operator priced bid at $115.14 with cost $82.24 (markup ~40%).
    Catalog later auto-corrects cost down to $80.00. Without bid_price
    preservation, model would re-derive bid = 80 × 1.40 = $112.00 →
    silently lower bid → wrong PDF to buyer.

    With bid_price preservation, operator's $115.14 stays put; markup
    is derived to keep round-trip honest.
    """
    q = _quote_with_one_line(cost="82.24", markup="40")
    # New cost arrives, but operator wants bid preserved.
    q.set_price(
        line_no=1,
        unit_cost=Decimal("80.00"),
        bid_price=Decimal("115.14"),
    )
    item = q.line_items[0]
    assert item.unit_cost == Decimal("80.00")
    # Derived markup: (115.14 - 80) / 80 × 100 = 43.925 → 43.93%
    assert item.markup_pct == Decimal("43.93")
    # Round-trip lands within 1¢ of the operator's bid
    assert abs(item.unit_price - Decimal("115.14")) <= Decimal("0.01")


def test_set_price_bid_price_ignored_when_cost_zero():
    """Zero cost ⇒ no defined markup; bid_price is ignored, not crashy.

    A cost==0 bid-only quote is a separate model gap (`unit_price` is a
    computed field that returns 0 when cost is 0). Documented as out of
    scope for this PR — the helper just doesn't crash.
    """
    q = _quote_with_one_line(cost="0", markup="0")
    q.set_price(
        line_no=1,
        unit_cost=Decimal("0"),
        markup_pct=Decimal("25"),
        bid_price=Decimal("99.99"),
    )
    item = q.line_items[0]
    assert item.unit_cost == Decimal("0")
    # Falls back to explicit markup_pct since bid couldn't be honoured.
    assert item.markup_pct == Decimal("25")


def test_set_price_3x_sanity_still_caps_when_bid_price_given():
    """The 3x reference-price guard runs BEFORE bid derivation.

    Cost is capped to the reference, so the derived markup is calibrated
    against the *capped* cost — not the suspicious original.
    """
    q = _quote_with_one_line(cost="5.00", markup="20", scprs="10.00")
    # Operator passes cost=$50 (10x SCPRS reference). Should cap to $10.
    q.set_price(
        line_no=1,
        unit_cost=Decimal("50.00"),
        bid_price=Decimal("13.50"),
    )
    item = q.line_items[0]
    assert item.unit_cost == Decimal("10.00")  # capped to scprs ref
    # Derived markup uses CAPPED cost: (13.50 - 10) / 10 × 100 = 35.00%
    assert item.markup_pct == Decimal("35.00")


def test_set_price_with_bid_records_bid_in_audit_trail():
    """Audit log distinguishes preserve-bid mode so we can debug
    later why a particular line landed at a non-default markup."""
    q = _quote_with_one_line()
    q.set_price(
        line_no=1,
        unit_cost=Decimal("10.00"),
        bid_price=Decimal("13.50"),
    )
    # Audit messages from the model (`_audit`) — search for our entry
    audit_actions = [e.action for e in q.provenance.audit_trail]
    audit_text = " ".join(audit_actions)
    assert "set_price line 1" in audit_text
    assert "bid=$13.50" in audit_text
    assert "derived" in audit_text


def test_set_price_negative_bid_negative_markup():
    """When bid < cost, derived markup is negative (operator bidding at a
    loss). The model honours it without complaint — the QA layer is
    responsible for warning the operator. We just don't lose the bid."""
    q = _quote_with_one_line(cost="20", markup="10")
    q.set_price(
        line_no=1,
        unit_cost=Decimal("20.00"),
        bid_price=Decimal("18.00"),  # bid below cost on purpose
    )
    item = q.line_items[0]
    assert item.unit_cost == Decimal("20.00")
    # Derived: (18-20)/20 × 100 = -10.00%
    assert item.markup_pct == Decimal("-10.00")
    # Round-trip back to bid
    # unit_price = 20 × (1 + -10/100) = 20 × 0.9 = 18.00
    assert item.unit_price == Decimal("18.00")


def test_set_price_line_not_found_still_raises():
    """Missing line still raises ValueError regardless of bid_price."""
    q = _quote_with_one_line()
    with pytest.raises(ValueError, match="Line 99"):
        q.set_price(line_no=99, unit_cost=Decimal("5"),
                    bid_price=Decimal("10"))


def test_set_price_bid_price_with_str_input_is_decimal_safe():
    """Some callers may pass a float/str bid_price; the helper coerces."""
    q = _quote_with_one_line()
    # Pass as float (typical from JSON deserialization)
    q.set_price(line_no=1, unit_cost=Decimal("10"), bid_price=12.5)
    item = q.line_items[0]
    # (12.5 - 10) / 10 × 100 = 25.00%
    assert item.markup_pct == Decimal("25.00")
    assert item.unit_price == Decimal("12.50")
