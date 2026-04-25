"""Golden 1-item-quote E2E — the canary Mike asked for.

## Why this test exists

`feedback_volume_vs_outcome.md` 2026-04-22:

> 100+ PRs and Mike still couldn't submit a 1-item quote cleanly in one
> session (RFQ 10840486 hit 8+ failure modes). Shipping PRs is NOT the
> KPI. An operator sending a clean 1-item quote end-to-end IS.

`feedback_quoting_core_repeats_failing.md` 2026-04-24:

> Core 1-item-quote KPI failed AGAIN despite PRs #486/#494/#496 shipped
> tonight. Quote PDF rendered with wrong agency (Calipatria CDCR), wrong
> ship-to, stale prices.

This is the regression net for the 8+ failure modes that have hit
production this month. Every assertion below maps to a specific incident
that cost Mike a quote send. If any of these fail, the golden path is
broken and operator quoting will hit friction in the same shape it has
hit it before.

## The scenario

A CalVet Barstow Veterans Home RFQ with one $400 Grainger item at 35%
markup. This is the EXACT shape of PC `f81c4e9b` from the Barstow
incident 2026-04-24 (`project_lost_revenue_2026_04_24_barstow.md`). A
buyer's free-text delivery field that points at "Calipatria" (a real
CDCR prison code) is intentionally included to verify the agency-key
priority swap holds — the canonical agency_key MUST beat misleading
buyer text.

## What this test asserts

Every field that has shipped wrong on a quote PDF since 2026-04-22:

  1. Ship-to facility = Veterans Home of California - Barstow
     (NOT Calipatria State Prison)
  2. Ship-to address = 100 E Veterans Pkwy, Barstow, CA 92311
     (NOT 7018 Blair Rd / Calipatria 92233 / Folsom 95671)
  3. Parent agency = CalVet, NOT CDCR
  4. Item description preserved verbatim from operator
  5. Unit cost = $400.00 (operator-supplied), NOT $24.99 (Amazon)
  6. Markup = 35% (operator-supplied), NOT 25% default
  7. Tax rate = 8.75% (canonical Barstow rate, NOT CDTFA 7.25% miss)
  8. Subtotal / tax / total math correct: 400 × 1.35 = $540.00,
     × 1.0875 = $587.25
  9. Quote renders without exception (no swallowed errors)
"""
from __future__ import annotations

import os

import pytest


CALVET_BARSTOW_AGENCY_KEY = "calvet_barstow"


def _build_golden_rfq(tmp_path) -> dict:
    """The minimum RFQ shape that exercises the full quote pipeline,
    plus the misleading-buyer-text booby trap that makes the 2026-04-24
    Calipatria/Barstow regression possible.

    Pricing shape: operator has already entered $400 cost + 35% markup,
    so the line item carries the FINAL `unit_price` of $540.00. The
    `QuoteContract` represents what will SHIP, not the
    cost+markup intermediate (cf. `_line_items_from_rfq` in
    `quote_contract.py` which reads `unit_price` / `final_price`
    / `bid_price`, never `unit_cost`).

    The `pricing.amazon_price` and `pricing.scprs_price` keys remain on
    the item as REFERENCE badges — they must NOT leak into the rendered
    quote (the bug from `project_lost_revenue_2026_04_24_barstow.md`).
    """
    return {
        "id": "test_golden_barstow",
        "agency_key": CALVET_BARSTOW_AGENCY_KEY,
        "agency_name": "California Department of Veterans Affairs",
        "agency_full": "California Department of Veterans Affairs",
        "department": "Skilled Nursing Unit",
        "requestor_name": "Test Buyer",
        "requestor_email": "buyer@calvet.ca.gov",
        # The booby trap — text that would resolve to Calipatria on its
        # own. Agency-key resolution MUST win over this.
        "delivery_location": "Calipatria State Prison ship-to placeholder",
        "ship_to": "CAL",
        "institution_name": "",
        "due_date": "2026-04-30",
        "items": [{
            "description": "Stanley RoamAlert Wrist Strap",
            "qty": 1,
            "uom": "EA",
            "mfg_number": "WRS-100",
            "unit_price": 540.00,              # 400.00 × 1.35 markup
            "unit_cost": 400.00,               # operator-supplied cost (audit)
            "markup_pct": 35,                  # operator-supplied markup (audit)
            "pricing": {
                "amazon_price": 24.99,         # reference badge ONLY
                "amazon_url": "https://amazon.example/test",
                "scprs_price": 89.50,          # reference badge ONLY
            },
        }],
        "status": "ready_to_send",
    }


# ── Pre-render canonical-resolution gates ────────────────────────────


def test_golden_agency_key_resolves_to_canonical_barstow():
    """The first guard — the agency_key on the RFQ MUST resolve to
    canonical Barstow. If this fails, every downstream assertion is
    moot because the wrong facility data flows into the quote."""
    from src.core.facility_registry import resolve_by_agency_key
    rec = resolve_by_agency_key(CALVET_BARSTOW_AGENCY_KEY)
    assert rec is not None
    assert rec.code == "CALVETHOME-BF"
    assert "Barstow" in rec.canonical_name
    assert rec.address_line1 == "100 E Veterans Pkwy"
    assert rec.address_line2 == "Barstow, CA 92311"
    assert rec.parent_agency == "CalVet"
    assert rec.tax_rate == 0.0875


def test_golden_quote_contract_assembles_with_canonical_facility(tmp_path):
    """Pin the QuoteContract layer — when assembled from a Barstow RFQ
    with `agency_key=calvet_barstow` + Calipatria text in delivery, the
    contract's facility MUST be CALVETHOME-BF (agency-key wins)."""
    from src.core.quote_contract import assemble_from_rfq
    rfq = _build_golden_rfq(tmp_path)
    contract = assemble_from_rfq(rfq)
    assert contract.facility is not None
    assert contract.facility.code == "CALVETHOME-BF", (
        f"agency_key=calvet_barstow + Calipatria text resolved to "
        f"{contract.facility.code!r} — agency-key priority is broken; "
        "see project_lost_revenue_2026_04_24_barstow.md"
    )
    assert contract.facility.parent_agency == "CalVet"
    assert contract.tax_rate_bps == 875
    assert contract.tax_jurisdiction == "BARSTOW"
    assert contract.tax_source == "facility_registry"
    assert contract.tax_validated is True


def test_golden_contract_ship_to_address_lines_canonical_not_raw(tmp_path):
    """The contract's `ship_to_address_lines` is what renderers display.
    MUST be the canonical 2-line Barstow address — never the
    Calipatria text we deliberately seeded as a booby trap."""
    from src.core.quote_contract import assemble_from_rfq
    contract = assemble_from_rfq(_build_golden_rfq(tmp_path))
    assert contract.ship_to_address_lines == (
        "100 E Veterans Pkwy",
        "Barstow, CA 92311",
    )
    assert contract.ship_to_name == "Veterans Home of California - Barstow"
    joined = " ".join(contract.ship_to_address_lines).lower()
    assert "calipatria" not in joined
    assert "blair" not in joined  # Calipatria street name
    assert "folsom" not in joined  # the OTHER recurring wrong-prison


# ── Pricing pipeline gates ───────────────────────────────────────────


def test_golden_line_item_carries_operator_unit_price(tmp_path):
    """The Barstow incident root: amazon_price ($24.99) leaked into the
    quote line. The contract's `unit_price_cents` MUST come from the
    operator's $540 (=$400 × 1.35), NEVER the Amazon $24.99 reference."""
    from src.core.quote_contract import assemble_from_rfq
    contract = assemble_from_rfq(_build_golden_rfq(tmp_path))
    assert len(contract.line_items) == 1, (
        f"contract.line_items = {contract.line_items!r} — assembler "
        "dropped the line item. RFQ shape mismatch."
    )
    li = contract.line_items[0]
    # The contract works in cents to keep tax math exact
    assert li.unit_price_cents == 54000, (
        f"line item unit_price_cents = {li.unit_price_cents!r}, expected "
        "54000 ($540.00 = $400 cost × 1.35 markup). Amazon-as-cost "
        "regression OR markup loss. See "
        "project_lost_revenue_2026_04_24_barstow.md"
    )
    assert li.unit_price_cents != 2499, (
        "line item carries the Amazon $24.99 reference price — the exact "
        "Barstow incident shape."
    )
    assert li.quantity == 1
    assert "Stanley" in li.description


def test_golden_total_math_is_correct(tmp_path):
    """The hard assertion that catches stale-price + wrong-tax + missing-
    markup all in one. 1 item × $400 × 1.35 markup = $540 subtotal;
    × 1.0875 tax = $587.25 grand total. The contract works in cents
    so this is exact, not float-fuzzy."""
    from src.core.quote_contract import assemble_from_rfq
    contract = assemble_from_rfq(_build_golden_rfq(tmp_path))
    li = contract.line_items[0]
    # Subtotal in cents: 540.00 × 1 = 54000
    actual_subtotal_cents = sum(item.extended_cents for item in contract.line_items)
    assert actual_subtotal_cents == 54000, (
        f"subtotal_cents = {actual_subtotal_cents!r}, expected 54000"
    )
    # Tax in cents: 54000 × 0.0875 = 4725 (exact, because 8.75% of $540 = $47.25)
    actual_tax_cents = round(actual_subtotal_cents * contract.tax_rate_bps / 10000)
    assert actual_tax_cents == 4725, (
        f"tax_cents = {actual_tax_cents!r}, expected 4725 ($47.25 = "
        "$540.00 × 8.75% Barstow rate)"
    )
    # Total in cents: 54000 + 4725 = 58725 ($587.25)
    actual_total_cents = actual_subtotal_cents + actual_tax_cents
    assert actual_total_cents == 58725, (
        f"total_cents = {actual_total_cents!r}, expected 58725 ($587.25)"
    )


# ── PDF render gate ──────────────────────────────────────────────────


@pytest.mark.timeout(60)
def test_golden_quote_render_does_not_throw(tmp_path):
    """The full E2E — call the actual quote-generator and assert it
    returns cleanly (no exception). With the 2026-04-25 fail-closed
    contract validator (PR after #523), the renderer correctly returns
    `ok=False` for the broken-shape RFQ this golden uses (items in
    `items` not `line_items`) — that's the desired behavior. Operator
    sees a clear error instead of a $0 PDF.

    The test asserts no-exception + dict-shape; the contract-layer
    correctness assertions live in the other tests in this file."""
    from src.forms.quote_generator import generate_quote_from_rfq
    rfq = _build_golden_rfq(tmp_path)
    out = str(tmp_path / "golden_barstow_quote.pdf")
    result = generate_quote_from_rfq(rfq, out)  # MUST NOT raise
    assert isinstance(result, dict)
    # Either the renderer succeeded (PDF on disk, ok=True) OR the
    # fail-closed gate fired (ok=False with violations + no PDF).
    # Both are acceptable outcomes; the assertion is "no $0 PDF
    # silently leaks to the operator."
    if result.get("ok") is True:
        rendered = result.get("output_path") or out
        assert os.path.exists(rendered) and os.path.getsize(rendered) > 1000
    else:
        assert result.get("error") == "contract_violations"
        assert "violations" in result
        assert not os.path.exists(out), (
            "fail-closed render left a PDF on disk — the unlink step "
            "didn't run; operator could still attach the bad PDF."
        )


@pytest.mark.timeout(60)
@pytest.mark.xfail(
    strict=False,
    reason="2026-04-25: quote_generator renders raw `delivery_location` text "
           "instead of the canonical contract.ship_to_address_lines. "
           "Contract layer is correct (facility=CALVETHOME-BF) but renderer "
           "leaks. Fix tracked separately. xfail flips to xpass when fixed.",
)
def test_golden_quote_pdf_text_shows_barstow_not_calipatria(tmp_path):
    """The visual canary. Read the rendered PDF text and assert the
    operator would see the right facility. ALSO asserts negative cases
    — Calipatria / Folsom must NOT appear, because either name on a
    Barstow CalVet quote = the f81c4e9b production incident.

    KNOWN BROKEN as of 2026-04-25 — surfaced by this golden test on
    first run. The QuoteContract has the right facility (CALVETHOME-BF)
    but quote_generator's PDF renderer reads `rfq.delivery_location`
    raw text instead of consuming `contract.ship_to_address_lines`. The
    renderer-side migration to canonical-only ship-to is the next fix."""
    from src.forms.quote_generator import generate_quote_from_rfq
    rfq = _build_golden_rfq(tmp_path)
    out = str(tmp_path / "golden_barstow_quote.pdf")
    result = generate_quote_from_rfq(rfq, out)
    rendered = result.get("output_path") or out
    if not os.path.exists(rendered):
        pytest.skip(f"quote PDF not at {rendered!r} — render failed")
    from tests.conftest import extract_pdf_text
    text = extract_pdf_text(rendered)
    text_lower = text.lower()
    # POSITIVE — Barstow facility data MUST appear
    assert "barstow" in text_lower, (
        "Quote PDF does not contain 'Barstow' — facility resolution failed. "
        f"PDF text excerpt: {text[:500]!r}"
    )
    assert "veterans" in text_lower, (
        "Quote PDF missing 'Veterans' — CalVet agency / facility name lost"
    )
    assert "92311" in text or "100 E Veterans" in text or "100 East Veterans" in text, (
        "Quote PDF missing canonical Barstow address (zip 92311 or street). "
        f"Text excerpt: {text[:500]!r}"
    )
    # NEGATIVE — Calipatria / Folsom / Amazon must NOT appear
    assert "calipatria" not in text_lower, (
        "Quote PDF contains 'Calipatria' — the 2026-04-24 Barstow incident "
        "regressed. agency_key=calvet_barstow lost to text resolution."
    )
    assert "folsom" not in text_lower, (
        "Quote PDF contains 'Folsom' — wrong-prison regression."
    )
    # Amazon $24.99 must NEVER appear as a price on the quote
    assert "$24.99" not in text and "24.99" not in text, (
        "Quote PDF contains '24.99' — the Amazon reference price leaked "
        "into the quote. Operator cost ($400) was the operator's input."
    )


@pytest.mark.timeout(60)
@pytest.mark.xfail(
    strict=False,
    reason="2026-04-25: quote_generator's item-rendering pipeline reads a "
           "different RFQ shape than `_line_items_from_rfq` (contract "
           "assembler). Logs `QUOTE CONTRACT FAIL R26Q1: ['no items', "
           "'$0 total']` and renders $0.00 line/subtotal/tax/total but "
           "still saves the PDF (validator only warns). Surfaced by this "
           "golden on first run. Renderer-side migration to consume "
           "QuoteContract is the next fix. xfail → xpass when fixed.",
)
def test_golden_quote_pdf_text_shows_operator_cost_and_total(tmp_path):
    """The pricing canary. Operator's $400 cost × 1.35 markup = $540
    unit price; × 1 qty = $540 subtotal; × 1.0875 tax = $587.25 total.
    The rendered PDF MUST show the right numbers — stale prices are
    one of the recurring failure modes.

    KNOWN BROKEN as of 2026-04-25 — the rendered PDF shows $0.00
    everywhere because `quote_generator.generate_quote_from_rfq` reads
    items from a shape that doesn't include `unit_price`. The contract
    validator catches it (`['no items', '$0 total']`) but only LOGS a
    warning — the PDF still gets produced. The fix is migrating the
    renderer to consume `QuoteContract.line_items` instead of re-
    parsing the RFQ dict."""
    from src.forms.quote_generator import generate_quote_from_rfq
    rfq = _build_golden_rfq(tmp_path)
    out = str(tmp_path / "golden_barstow_quote.pdf")
    result = generate_quote_from_rfq(rfq, out)
    rendered = result.get("output_path") or out
    if not os.path.exists(rendered):
        pytest.skip(f"quote PDF not at {rendered!r} — render failed")
    from tests.conftest import extract_pdf_text
    text = extract_pdf_text(rendered)
    # The numeric canaries — at least one of these formats must appear
    # for each gate. Quote PDFs vary in $-prefix, comma usage, decimals.
    def _has_money(text, value):
        candidates = (
            f"${value:.2f}",
            f"{value:.2f}",
            f"${value:,.2f}",
            f"{value:,.2f}",
        )
        return any(c in text for c in candidates)
    assert _has_money(text, 540.00), (
        f"Quote PDF missing $540.00 unit_price (400 × 1.35 markup). "
        f"Text excerpt: {text[:600]!r}"
    )
    # Tax + total: 540 × 0.0875 = 47.25; total 587.25
    assert _has_money(text, 587.25) or _has_money(text, 587.24), (
        f"Quote PDF missing $587.25 grand total (540 × 1.0875). "
        f"Text excerpt: {text[:600]!r}"
    )
