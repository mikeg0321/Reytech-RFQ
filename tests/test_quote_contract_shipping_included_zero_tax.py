"""QuoteContract.shipping_option drives the Quote PDF tax line.

Mike's 3-month rule (codified PR mr-wolf substrate-pivot 2026-05-13):
when shipping is bundled INTO the vendor unit price (shipping_option
== "included"), the reseller eats sales tax inside the markup, so the
Quote PDF stamps $0 tax to the agency. Any other value ("fob_dest",
"custom", "separate", or the historic "" default) falls through to
the normal subtotal × rate math.

The canonical incident: RFQ e02b7fa6 PVSP 2026-05-13 — Quote PDF
stamped $322 tax against a $0 canonical because the renderer was
reading `tax_rate × subtotal` blind. This test pins the contract-side
fix so the renderer can never silently disagree with the contract
again.
"""
from __future__ import annotations

from src.core.quote_contract import LineItem, QuoteContract


def _make_contract(shipping_option: str, *, tax_rate_bps: int = 875) -> QuoteContract:
    """Build a 2-item contract with $4500 subtotal at the given shipping_option.

    2 items × $1500/ea = $3000 + 1 item × 1 × $1500/ea = $4500 keeps
    the integer math obvious — at 8.75% the tax_cents would round to
    39375 cents ($393.75) when included is OFF.
    """
    items = (
        LineItem(description="Sterile gauze pad",
                 quantity=2,
                 unit_price_cents=150000,
                 mfg_number="MG-2244",
                 uom="EA"),
        LineItem(description="Surgical drape",
                 quantity=1,
                 unit_price_cents=150000,
                 mfg_number="SD-9981",
                 uom="EA"),
    )
    return QuoteContract(
        facility=None,
        agency_code="CCHCS",
        agency_full="California Correctional Health Care Services",
        ship_to_raw="PVSP — Pleasant Valley State Prison",
        line_items=items,
        tax_rate_bps=tax_rate_bps,
        tax_rate=round(tax_rate_bps / 10000.0, 6),
        tax_jurisdiction="COALINGA",
        tax_source="facility_registry",
        tax_validated=True,
        shipping_option=shipping_option,
    )


def test_subtotal_cents_is_sum_of_extended_lines():
    """Sanity: subtotal math is what the tax assertions assume."""
    c = _make_contract("")
    assert c.subtotal_cents == 450000


def test_shipping_included_zero_tax_at_nonzero_rate():
    """The CANONICAL fix — shipping_option == 'included' → tax = 0."""
    c = _make_contract("included", tax_rate_bps=875)
    assert c.tax_cents == 0
    assert c.total_cents == c.subtotal_cents  # tax line nets to zero


def test_shipping_separate_charges_tax():
    """'separate' / 'fob_dest' / 'custom' all fall through to subtotal × rate."""
    for opt in ("separate", "fob_dest", "custom"):
        c = _make_contract(opt, tax_rate_bps=875)
        # 450000 cents × 875 bps / 10000 = 39375 cents
        assert c.tax_cents == 39375, (
            f"shipping_option={opt!r} should charge tax — got {c.tax_cents}"
        )
        assert c.total_cents == 450000 + 39375


def test_empty_shipping_option_falls_back_to_separate_math():
    """Pre-PR-AD records had no shipping_option field; treat absent as
    'separate' so historical quotes don't silently zero out."""
    c = _make_contract("", tax_rate_bps=875)
    assert c.tax_cents == 39375


def test_included_zero_tax_holds_when_rate_is_zero():
    """When BOTH shipping is included AND tax_rate_bps is 0 (DGS PO,
    out-of-state), the contract still returns 0 — neither code path
    surprises the renderer with a non-zero number."""
    c = _make_contract("included", tax_rate_bps=0)
    assert c.tax_cents == 0


def test_included_is_case_insensitive_at_assembly():
    """`assemble_from_rfq` lowercases the input; here we verify the
    field stored on the contract is the canonical lowercase token so
    no renderer has to repeat the normalization."""
    from src.core.quote_contract import assemble_from_rfq
    rfq = {
        "shipping_option": "Included",  # operator-set, mixed case
        "items": [
            {"description": "x", "quantity": 1, "unit_price": 100.0,
             "mfg_number": "X1", "uom": "EA"},
        ],
        "tax_rate": 0.0875,
    }
    c = assemble_from_rfq(rfq)
    assert c.shipping_option == "included"
    # And tax_cents respects it (resolved facility is None so tax_rate_bps=0
    # — but the included guard fires first and returns 0 regardless).
    assert c.tax_cents == 0
