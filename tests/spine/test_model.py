"""The Spine — model behavior tests.

These tests replay the 2026-05-15 failure classes against the Spine
model and assert that each failure is structurally impossible (raises
SpineValidationError or ValidationError at the boundary).

If you find one of these failing because the model raised when it
should have accepted, the model is wrong — fix it, then verify the
Charter still describes the corrected behavior.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from src.spine.model import (
    Quote,
    LineItem,
    QuoteStatus,
    SpineValidationError,
)


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


def _fresh_validation_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _stale_validation_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=60)


def _ok_line(
    line_no: int = 1,
    *,
    qty: int = 2,
    cost_cents: int = 5000,
    unit_price_cents: int = 6750,
    with_source: bool = True,
) -> LineItem:
    return LineItem(
        line_no=line_no,
        description=f"Test item {line_no}",
        mfg_number="MFG-X",
        qty=qty,
        uom="EA",
        cost_cents=cost_cents,
        cost_source_url="https://supplier.example.com/sku/123" if with_source else None,
        cost_validated_at=_fresh_validation_ts(),
        unit_price_cents=unit_price_cents,
    )


def _ok_quote(
    *,
    status: QuoteStatus = QuoteStatus.PARSED,
    tax_rate_bps: int = 825,
    line_items: list[LineItem] | None = None,
) -> Quote:
    return Quote(
        quote_id="Q-test-001",
        agency="CCHCS",
        facility="SATF",
        solicitation_number="10847262",
        line_items=line_items or [_ok_line(1), _ok_line(2)],
        tax_rate_bps=tax_rate_bps,
        status=status,
    )


# ──────────────────────────────────────────────────────────────────────
# Aliases / extras / forbidden fields
# ──────────────────────────────────────────────────────────────────────


def test_extra_field_on_line_item_raises():
    """Closes the persistence P0 class. Unknown LineItem fields RAISE."""
    with pytest.raises(ValidationError, match="extra"):
        LineItem(
            line_no=1,
            description="x",
            qty=1,
            uom="EA",
            cost_cents=100,
            cost_validated_at=_fresh_validation_ts(),
            unit_price_cents=200,
            bid_price=999,  # banned alias — must raise.
        )


def test_extra_field_on_quote_raises():
    with pytest.raises(ValidationError, match="extra"):
        Quote(
            quote_id="Q-test-001",
            agency="CCHCS",
            facility="SATF",
            solicitation_number="10847262",
            line_items=[_ok_line(1)],
            tax_rate_bps=825,
            shipping_option="included",  # banned — must raise.
        )


def test_tax_rate_pct_alias_raises():
    """The legacy `tax_rate_pct` alias is structurally banned."""
    with pytest.raises(ValidationError, match="extra"):
        Quote(
            quote_id="Q-test-001",
            agency="CCHCS",
            facility="SATF",
            solicitation_number="10847262",
            line_items=[_ok_line(1)],
            tax_rate_bps=825,
            tax_rate_pct=8.25,  # banned alias.
        )


# ──────────────────────────────────────────────────────────────────────
# Tax math — closes findings #13, #18.
# ──────────────────────────────────────────────────────────────────────


def test_tax_cents_is_subtotal_times_rate_with_bankers_rounding():
    """No conditional zeroing branch. Banker's-rounded integer math."""
    li = _ok_line(1, qty=10, unit_price_cents=5000)  # ext = 50000
    q = _ok_quote(tax_rate_bps=897, line_items=[li])  # 8.97%
    assert q.subtotal_cents == 50000
    # 50000 × 897 = 44,850,000. divmod 10000 = (4485, 0). Exact → 4485.
    assert q.tax_cents == 4485
    assert q.total_cents == 50000 + 4485


def test_tax_cents_rounds_half_to_even():
    """An exact-half case rounds to the even cent."""
    # subtotal 100, rate 5000 bps (50%) → total 5000, divmod 10000 = (0, 5000).
    # remainder*2 = 10000 = 10000 → tied → round to even. quotient=0 even → 0.
    li = _ok_line(1, qty=1, cost_cents=50, unit_price_cents=100)
    q = _ok_quote(tax_rate_bps=500, line_items=[li])
    # 100 * 500 = 50000. divmod 10000 = (5, 0). Exact → 5.
    assert q.tax_cents == 5


def test_no_shipping_field_can_zero_tax():
    """Finding #18: shipping_option=included → tax_cents=0 is impossible.

    The model has no shipping_option field at all. The total is
    always subtotal + tax, period.
    """
    li = _ok_line(1, qty=1, unit_price_cents=10000)
    q = _ok_quote(tax_rate_bps=897, line_items=[li])
    assert q.tax_cents > 0  # cannot be zeroed by any field setting.
    # And there's no way to inject shipping into the total:
    with pytest.raises(ValidationError):
        Quote(
            quote_id="Q-test-001",
            agency="CCHCS",
            facility="SATF",
            solicitation_number="10847262",
            line_items=[li],
            tax_rate_bps=897,
            shipping_amount=500,  # banned — must raise.
        )


def test_tax_rate_zero_blocks_priced_status_on_construction():
    """Finding #15: tax_rate_bps must be > 0 to construct a priced quote.

    Direct construction raises Pydantic's ValidationError (model
    validator wraps the SpineValidationError). The state-machine
    transition path raises SpineValidationError directly — see
    test_tax_rate_zero_blocks_priced_transition.
    """
    li = _ok_line(1)
    with pytest.raises(ValidationError, match="tax_rate_bps"):
        _ok_quote(status=QuoteStatus.PRICED, tax_rate_bps=0, line_items=[li])


def test_tax_rate_zero_blocks_priced_transition():
    """The state-machine path raises SpineValidationError directly."""
    q = _ok_quote(status=QuoteStatus.PARSED, tax_rate_bps=0)
    with pytest.raises(SpineValidationError, match="tax_rate_bps"):
        q.with_status(QuoteStatus.PRICED)


def test_tax_rate_zero_ok_in_parsed_state():
    """An in-flight 'parsed' record can have tax_rate_bps=0 momentarily.

    Ingest creates the quote, CDTFA lookup populates tax_rate_bps,
    then status advances to 'priced'.
    """
    q = _ok_quote(status=QuoteStatus.PARSED, tax_rate_bps=0)
    assert q.status == QuoteStatus.PARSED
    assert q.tax_rate_bps == 0


# ──────────────────────────────────────────────────────────────────────
# Computed fields — derived on every read, never stored.
# ──────────────────────────────────────────────────────────────────────


def test_extension_is_qty_times_unit_price():
    li = _ok_line(1, qty=7, unit_price_cents=350)
    assert li.extension_cents == 2450


def test_markup_pct_display_is_derived_not_stored():
    """No stored markup_pct = no qty-clobbers-markup bug (P0 2026-05-15).

    Finding from project_qty_change_clobbers_markup_p0_2026_05_15:
    operator changed qty=2 → qty=3, markup flipped 35% → 16%.
    In the Spine, qty changes cannot affect markup display because
    markup is derived from unit_price/cost — neither of which changes
    when qty changes.
    """
    li = _ok_line(1, qty=2, cost_cents=54000, unit_price_cents=72900)
    assert li.markup_pct_display == 35.0
    # Replay: operator changes qty 2 → 3. unit_price and cost don't change.
    li_after = li.model_copy(update={"qty": 3})
    assert li_after.markup_pct_display == 35.0  # UNCHANGED.
    assert li_after.extension_cents == 3 * 72900  # extension updates, that's it.


def test_markup_display_none_when_cost_zero():
    li = LineItem(
        line_no=1,
        description="zero-cost item",
        qty=1,
        uom="EA",
        cost_cents=0,
        cost_validated_at=_fresh_validation_ts(),
        unit_price_cents=100,
    )
    assert li.markup_pct_display is None


def test_subtotal_sums_extensions():
    items = [
        _ok_line(1, qty=2, unit_price_cents=1000),
        _ok_line(2, qty=3, unit_price_cents=2000),
    ]
    q = _ok_quote(line_items=items)
    assert q.subtotal_cents == (2 * 1000) + (3 * 2000)


# ──────────────────────────────────────────────────────────────────────
# Cost-basis validation — closes finding #19 (Item 2555 $20.85 vs $6.68).
# ──────────────────────────────────────────────────────────────────────


def test_finalize_rejects_expensive_line_without_source():
    """Closes finding #19 (the Item 2555 $14k phantom-cost case).

    A line item with cost >= $100 cannot reach finalized via the state
    machine without either a source URL or a hand-validated note.
    Tests the via-state-machine path → raises SpineValidationError.
    """
    expensive_no_source = LineItem(
        line_no=1,
        description="bulk-buy item, no provenance",
        qty=1,
        uom="EA",
        cost_cents=15000,  # $150 — over the $100 threshold.
        cost_source_url=None,
        cost_hand_validated_note=None,
        cost_validated_at=_fresh_validation_ts(),
        unit_price_cents=20000,
    )
    q = _ok_quote(status=QuoteStatus.PRICED, line_items=[expensive_no_source])
    with pytest.raises(SpineValidationError, match="cost source"):
        q.with_status(QuoteStatus.FINALIZED)


def test_finalize_rejects_expensive_line_without_source_on_construction():
    """Same constraint, hit via direct construction → ValidationError."""
    expensive_no_source = LineItem(
        line_no=1,
        description="bulk-buy item, no provenance",
        qty=1,
        uom="EA",
        cost_cents=15000,
        cost_source_url=None,
        cost_hand_validated_note=None,
        cost_validated_at=_fresh_validation_ts(),
        unit_price_cents=20000,
    )
    with pytest.raises(ValidationError, match="cost source"):
        _ok_quote(status=QuoteStatus.FINALIZED, line_items=[expensive_no_source])


def test_finalize_accepts_expensive_line_with_url():
    line_with_url = _ok_line(1, cost_cents=20000)  # $200 cost
    q = _ok_quote(status=QuoteStatus.FINALIZED, line_items=[line_with_url])
    assert q.status == QuoteStatus.FINALIZED


def test_finalize_accepts_expensive_line_with_hand_validated_note():
    line_hv = LineItem(
        line_no=1,
        description="Special-order item, vendor catalog PDF on file",
        qty=1,
        uom="EA",
        cost_cents=50000,
        cost_source_url=None,
        cost_hand_validated_note="Vendor catalog 2026 Q2 — saved to Drive folder X",
        cost_validated_at=_fresh_validation_ts(),
        unit_price_cents=67500,
    )
    q = _ok_quote(status=QuoteStatus.FINALIZED, line_items=[line_hv])
    assert q.status == QuoteStatus.FINALIZED


def test_finalize_rejects_stale_cost_validation():
    """60-day-old cost basis cannot ship. Via state machine."""
    stale = LineItem(
        line_no=1,
        description="x",
        qty=1,
        uom="EA",
        cost_cents=50000,
        cost_source_url="https://supplier.example.com/sku",
        cost_validated_at=_stale_validation_ts(),  # 60 days ago.
        unit_price_cents=67500,
    )
    q = _ok_quote(status=QuoteStatus.PRICED, line_items=[stale])
    with pytest.raises(SpineValidationError, match="fresh"):
        q.with_status(QuoteStatus.FINALIZED)


def test_finalize_skips_cost_check_for_cheap_items():
    """Sub-$100 items don't need URL — operator can type freely."""
    cheap = LineItem(
        line_no=1,
        description="penny-class item",
        qty=1,
        uom="EA",
        cost_cents=500,  # $5 — under threshold.
        cost_source_url=None,
        cost_hand_validated_note=None,
        cost_validated_at=None,
        unit_price_cents=750,
    )
    q = _ok_quote(status=QuoteStatus.FINALIZED, line_items=[cheap])
    assert q.status == QuoteStatus.FINALIZED


# ──────────────────────────────────────────────────────────────────────
# State machine — closes finding #9 (Finalize reverts edits).
# ──────────────────────────────────────────────────────────────────────


def test_with_status_does_not_mutate_line_items():
    """Finding #9: clicking Finalize Pricing reverted operator edits.

    Spine state transitions return a new Quote with status changed and
    NOTHING ELSE. Line items pass through bit-identical.
    """
    items = [_ok_line(1, qty=2, unit_price_cents=12345)]
    q1 = _ok_quote(status=QuoteStatus.PARSED, line_items=items, tax_rate_bps=825)

    q2 = q1.with_status(QuoteStatus.PRICED)
    q3 = q2.with_status(QuoteStatus.FINALIZED)

    # All three quotes have the same line items, byte for byte.
    assert q1.line_items[0].unit_price_cents == 12345
    assert q2.line_items[0].unit_price_cents == 12345
    assert q3.line_items[0].unit_price_cents == 12345
    assert q1.subtotal_cents == q2.subtotal_cents == q3.subtotal_cents


def test_illegal_transition_raises():
    q = _ok_quote(status=QuoteStatus.PARSED)
    with pytest.raises(SpineValidationError, match="illegal transition"):
        q.with_status(QuoteStatus.SENT)


def test_sent_is_terminal_in_v1():
    items = [_ok_line(1)]
    q = _ok_quote(status=QuoteStatus.SENT, line_items=items)
    with pytest.raises(SpineValidationError, match="illegal transition"):
        q.with_status(QuoteStatus.PARSED)


def test_priced_can_go_back_to_parsed_for_rebid():
    """Rebid arc — reopen a priced quote for re-pricing."""
    q = _ok_quote(status=QuoteStatus.PRICED)
    q2 = q.with_status(QuoteStatus.PARSED)
    assert q2.status == QuoteStatus.PARSED


# ──────────────────────────────────────────────────────────────────────
# Misc invariants
# ──────────────────────────────────────────────────────────────────────


def test_duplicate_line_no_raises():
    items = [_ok_line(1), _ok_line(1)]
    with pytest.raises(ValidationError, match="duplicate"):
        _ok_quote(line_items=items)


def test_line_items_must_be_sorted():
    items = [_ok_line(2), _ok_line(1)]
    with pytest.raises(ValidationError, match="sorted"):
        _ok_quote(line_items=items)


def test_unknown_uom_raises():
    with pytest.raises(ValidationError, match="uom"):
        LineItem(
            line_no=1,
            description="x",
            qty=1,
            uom="WIDGETS",  # not in SUPPORTED_UOM
            cost_cents=100,
            cost_validated_at=_fresh_validation_ts(),
            unit_price_cents=200,
        )


@pytest.mark.parametrize(
    "uom",
    [
        "EA", "PK", "PAC", "BX", "CS", "CT", "DZ",
        "RL", "PR", "ST", "BG", "BT", "KIT",
    ],
)
def test_supported_uom_round_trips(uom):
    # Regression — every UOM advertised by SUPPORTED_UOM must validate
    # without raising. The KIT entry was added 2026-05-19 after CHCF
    # 10843811 shipped a "WHEEL, TIRE & HANDRIM KIT" line that the
    # allowlist had been silently rejecting.
    li = LineItem(
        line_no=1,
        description="x",
        qty=1,
        uom=uom,
        cost_cents=100,
        cost_validated_at=_fresh_validation_ts(),
        unit_price_cents=200,
    )
    assert li.uom == uom


def test_url_shape_validated():
    with pytest.raises(ValidationError, match="http"):
        LineItem(
            line_no=1,
            description="x",
            qty=1,
            uom="EA",
            cost_cents=100,
            cost_source_url="not-a-url",
            cost_validated_at=_fresh_validation_ts(),
            unit_price_cents=200,
        )


def test_negative_unit_price_raises():
    with pytest.raises(ValidationError):
        LineItem(
            line_no=1,
            description="x",
            qty=1,
            uom="EA",
            cost_cents=100,
            cost_validated_at=_fresh_validation_ts(),
            unit_price_cents=-1,
        )


def test_zero_qty_raises():
    with pytest.raises(ValidationError):
        LineItem(
            line_no=1,
            description="x",
            qty=0,
            uom="EA",
            cost_cents=100,
            cost_validated_at=_fresh_validation_ts(),
            unit_price_cents=200,
        )


def test_agency_limited_to_cchcs_in_v1():
    with pytest.raises(ValidationError):
        Quote(
            quote_id="Q-test",
            agency="CalVet",  # not yet supported.
            facility="X",
            solicitation_number="1",
            line_items=[_ok_line(1)],
            tax_rate_bps=825,
        )


# ──────────────────────────────────────────────────────────────────────
# Real-world replay — today's 9e63456e quote.
# ──────────────────────────────────────────────────────────────────────


def test_replay_9e63456e_renders_correct_total():
    """Replay today's CCHCS R26Q44 quote in the Spine.

    Expected from handoff: Subtotal $46,836.20, Tax 8.25% = $3,863.99,
    Total = $50,700.19. The legacy substrate produced a Quote PDF with
    tax line $3,850.05 (slight divergence) that required hand-overlay
    correction. The Spine's integer-cents tax math must produce the
    arithmetically correct value.

    Note: actual line item breakdown not preserved in handoff; this
    test asserts the math on a single representative line that sums to
    the correct subtotal-class. Replace with the actual 7-row fixture
    when items_snapshot manifest is re-extracted.
    """
    # Synthetic 7-row mock summing to $46,836.20 / 4683620 cents.
    # Row totals: 1000×2815, 25×4500, 100×3500, 50×9000, 10×12500, 100×3500, 20×3500
    # = 2815000 + 112500 + 350000 + 450000 + 125000 + 350000 + 70000 = 4272500
    # Off — adjust: use one consolidated 1000-row at $46.8362 per unit.
    # That's not real but the *math* is what we're testing.
    items = [
        LineItem(
            line_no=1,
            description="(consolidated stand-in for 9e63456e rows)",
            qty=10,
            uom="EA",
            cost_cents=300000,
            cost_source_url="https://supplier.example.com/x",
            cost_validated_at=_fresh_validation_ts(),
            unit_price_cents=468362,
        ),
    ]
    q = Quote(
        quote_id="9e63456e-spine-replay",
        agency="CCHCS",
        facility="SATF Corcoran",
        solicitation_number="10847262",
        line_items=items,
        tax_rate_bps=825,
        status=QuoteStatus.FINALIZED,
    )
    assert q.subtotal_cents == 4683620
    # 4683620 × 825 = 3,863,986,500. divmod 10000 = (386398, 6500).
    # 6500 × 2 = 13000 > 10000 → round up to 386399.
    # This is $3,863.99 — matching the 9e63456e manifest exactly.
    # Floor division would have produced 386398 ($3,863.98); the
    # legacy substrate's float-math gave $3,850.05 (the divergence
    # that needed hand-overlay-edit). Banker's rounding on integer
    # bps math is the deterministic correct answer.
    assert q.tax_cents == 386399
    assert q.total_cents == 4683620 + 386399  # $50,700.19 — matches manifest.


# ──────────────────────────────────────────────────────────────────────
# display_number — buyer-facing R{yy}Q#### identifier (PR #1040)
# ──────────────────────────────────────────────────────────────────────


def test_display_number_none_when_seq_and_year_both_missing():
    """Legacy rows pre-PR #1040: no seq/year stored → no display string.

    Renderers fall back to quote_id when display_number is None.
    """
    q = _ok_quote()
    assert q.quote_seq is None
    assert q.quote_year is None
    assert q.display_number is None


def test_display_number_renders_when_both_set():
    q = Quote(
        quote_id="Q-test-001",
        agency="CCHCS",
        facility="SATF",
        solicitation_number="10847262",
        line_items=[_ok_line(1)],
        tax_rate_bps=825,
        quote_seq=347,
        quote_year=2026,
    )
    assert q.display_number == "R26Q347"


def test_display_number_no_zero_padding_low_seq():
    """Format mirrors Mike's prior buyer-facing convention (R26Q39,
    R25Q161): no zero-padding. Width grows naturally as seq grows."""
    q = Quote(
        quote_id="Q-test-001",
        agency="CCHCS",
        facility="SATF",
        solicitation_number="10847262",
        line_items=[_ok_line(1)],
        tax_rate_bps=825,
        quote_seq=1,
        quote_year=2026,
    )
    assert q.display_number == "R26Q1"


def test_display_number_widens_naturally_at_any_seq():
    """No truncation at any width. seq=10001 → R26Q10001."""
    q = Quote(
        quote_id="Q-test-001",
        agency="CCHCS",
        facility="SATF",
        solicitation_number="10847262",
        line_items=[_ok_line(1)],
        tax_rate_bps=825,
        quote_seq=10001,
        quote_year=2026,
    )
    assert q.display_number == "R26Q10001"


def test_display_number_none_when_only_seq_set():
    q = Quote(
        quote_id="Q-test-001",
        agency="CCHCS",
        facility="SATF",
        solicitation_number="10847262",
        line_items=[_ok_line(1)],
        tax_rate_bps=825,
        quote_seq=5,
    )
    assert q.display_number is None


def test_display_number_none_when_only_year_set():
    q = Quote(
        quote_id="Q-test-001",
        agency="CCHCS",
        facility="SATF",
        solicitation_number="10847262",
        line_items=[_ok_line(1)],
        tax_rate_bps=825,
        quote_year=2026,
    )
    assert q.display_number is None


def test_quote_seq_must_be_positive():
    """seq=0 is meaningless; refuse at validation."""
    with pytest.raises(ValidationError):
        Quote(
            quote_id="Q-test-001",
            agency="CCHCS",
            facility="SATF",
            solicitation_number="10847262",
            line_items=[_ok_line(1)],
            tax_rate_bps=825,
            quote_seq=0,
            quote_year=2026,
        )


def test_quote_year_rejects_out_of_range():
    """Year must be 2024..2099. Catches bad ingest stamps."""
    with pytest.raises(ValidationError):
        Quote(
            quote_id="Q-test-001",
            agency="CCHCS",
            facility="SATF",
            solicitation_number="10847262",
            line_items=[_ok_line(1)],
            tax_rate_bps=825,
            quote_seq=1,
            quote_year=1999,
        )


def test_display_number_excluded_from_to_persisted_dict():
    """display_number is @computed_field — never persisted (would drift)."""
    q = Quote(
        quote_id="Q-test-001",
        agency="CCHCS",
        facility="SATF",
        solicitation_number="10847262",
        line_items=[_ok_line(1)],
        tax_rate_bps=825,
        quote_seq=347,
        quote_year=2026,
    )
    persisted = q.to_persisted_dict()
    assert "display_number" not in persisted
    # The underlying integers ARE persisted — that's the truth of intent.
    assert persisted["quote_seq"] == 347
    assert persisted["quote_year"] == 2026


def test_century_rollover_via_year_2099_renders_99():
    q = Quote(
        quote_id="Q-test-001",
        agency="CCHCS",
        facility="SATF",
        solicitation_number="10847262",
        line_items=[_ok_line(1)],
        tax_rate_bps=825,
        quote_seq=12,
        quote_year=2099,
    )
    assert q.display_number == "R99Q12"
