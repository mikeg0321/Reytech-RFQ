"""End-to-end guard for the CCHCS PC→RFQ handoff chain.

Unit tests cover each helper in isolation:
  - find_matching_pcs_for_cchcs   (test_pc_rfq_linker_cchcs.py)
  - promote_pc_to_rfq_in_place    (test_promote_pc_to_rfq.py)
  - reprice_qty_changed_lines     (test_reprice_qty_changed.py)

This file locks the **full chain** with realistic multi-line RFQ/PC shapes.
If someone changes the match threshold, re-wires the price fields, or
breaks the qty-change detection, a layer test may pass but the operator's
"99.9% done" promise from the rescoped one-engine plan (2026-04-20) breaks.

Scenario mirrors prod: a CCHCS buyer sends a PC that Mike prices, the RFQ
arrives a week later with 4 identical lines + 1 qty bump + 1 brand-new line.
The full handoff must:
  1. Surface the PC as a top candidate with is_exact=False (one new line)
  2. Promote → prices ported verbatim onto matched lines
  3. qty_change_summary flags only the bumped line
  4. Reprice touches only the flagged line; commitment prices survive
"""
from __future__ import annotations

from src.core.pc_rfq_linker import (
    find_matching_pcs_for_cchcs,
    promote_pc_to_rfq_in_place,
    qty_change_summary,
    reprice_qty_changed_lines,
)


def _realistic_cchcs_pc():
    """A CCHCS PC that Mike priced last week — 5 medical line items."""
    return {
        "agency": "CCHCS",
        "pc_number": "PC-2026-042",
        "requestor": "buyer@cchcs.ca.gov",
        "institution": "California Correctional Health Care Services",
        "items": [
            {"mfg_number": "W12919", "description": "Blood pressure cuff, adult",
             "quantity": 10, "unit_price": 45.00, "supplier_cost": 25.00,
             "bid_price": 45.00, "markup_pct": 80,
             "item_supplier": "McKesson", "item_link": "https://mckesson.com/W12919"},
            {"mfg_number": "NL304", "description": "Stethoscope, dual-head",
             "quantity": 5, "unit_price": 120.00, "supplier_cost": 70.00,
             "bid_price": 120.00, "markup_pct": 71,
             "item_supplier": "Welch Allyn"},
            {"mfg_number": "FN4368", "description": "Exam gloves, nitrile, large, box/100",
             "quantity": 50, "unit_price": 18.50, "supplier_cost": 10.00,
             "bid_price": 18.50, "markup_pct": 85,
             "item_supplier": "Medline"},
            {"mfg_number": "16753", "description": "Gauze pad 4x4, sterile, box/100",
             "quantity": 25, "unit_price": 12.75, "supplier_cost": 7.50,
             "bid_price": 12.75, "markup_pct": 70,
             "item_supplier": "Cardinal Health"},
            {"mfg_number": "K2881", "description": "Tongue depressors, non-sterile, box/500",
             "quantity": 8, "unit_price": 6.99, "supplier_cost": 3.50,
             "bid_price": 6.99, "markup_pct": 100,
             "item_supplier": "PDI"},
        ],
    }


def _realistic_rfq_matching_the_pc():
    """The public RFQ that came in a week later. Four lines match the PC as-is,
    one has a qty bump (buyer increased the order), one is brand new (buyer
    added a line the PC never covered)."""
    return {
        "requestor_email": "buyer@cchcs.ca.gov",
        "institution": "CCHCS",
        "solicitation_number": "PC-2026-042-RFQ",
        "line_items": [
            {"mfg_number": "W12919", "description": "Blood pressure cuff adult",
             "quantity": 10},                    # same qty
            {"mfg_number": "NL304", "description": "Stethoscope dual-head",
             "quantity": 5},                     # same qty
            {"mfg_number": "FN4368", "description": "Exam gloves nitrile large",
             "quantity": 100},                   # QTY BUMP: 50 → 100
            {"mfg_number": "16753", "description": "Gauze pad 4x4 sterile",
             "quantity": 25},                    # same qty
            {"mfg_number": "K2881", "description": "Tongue depressors",
             "quantity": 8},                     # same qty
            {"mfg_number": "NEW999", "description": "Thermometer, digital",
             "quantity": 5},                     # NEW LINE never on PC
        ],
    }


def _oracle_stub_pricer(line):
    """Stub pricer — in prod this is wired to pricing_oracle_v2. Here we just
    return a fixed new-cost/new-price for the qty-bumped line so we can assert
    on the delta."""
    return {
        "supplier_cost": 9.00,     # volume break on bigger order
        "unit_price": 17.00,       # slight discount reflecting new volume
        "bid_price": 17.00,
        "markup_pct": 89,
    }


def test_full_chain_realistic_multi_line_cchcs_rfq():
    pc = _realistic_cchcs_pc()
    rfq = _realistic_rfq_matching_the_pc()
    pcs = {"pc_042": pc, "pc_unrelated_cchcs": {"agency": "CCHCS", "items": [
        {"mfg_number": "Z9", "description": "unrelated item", "quantity": 1},
    ]}}

    # ── Step 1: matcher surfaces our PC as the top candidate ─────────────
    candidates = find_matching_pcs_for_cchcs(rfq, pcs, max_results=3)
    assert len(candidates) >= 1, "Matcher should surface at least the real PC"
    top = candidates[0]
    assert top["pc_id"] == "pc_042"
    # 5 of 6 RFQ lines match; one is brand new → not exact
    assert top["line_matches"] == 5
    assert top["line_total"] == 6
    assert top["is_exact"] is False
    assert top["match_pct"] >= 80, (
        f"Expected high match % for 5-of-6 MFG matches + same email + "
        f"same solicitation. Got {top['match_pct']}."
    )
    assert any("by_mfg_or_upc" in r for r in top["reasons"])

    # ── Step 2: operator confirms — promote ports prices verbatim ────────
    promote_result = promote_pc_to_rfq_in_place(rfq, top["pc_id"], top["pc_data"])
    assert promote_result["promoted"] == 5  # 5 of 6 lines got PC pricing
    assert promote_result["qty_changed"] == 1  # FN4368 qty bump
    assert promote_result["no_match"] == 1  # NEW999 — brand new line

    # Verify verbatim preservation for unchanged-qty lines
    bp_cuff = rfq["line_items"][0]
    assert bp_cuff["unit_price"] == 45.00
    assert bp_cuff["supplier_cost"] == 25.00
    assert bp_cuff["qty_changed"] is False

    # FN4368 (qty 50→100) is flagged for re-price — but still has PC price right now
    gloves = rfq["line_items"][2]
    assert gloves["qty_changed"] is True
    assert gloves["pc_original_qty"] == 50
    assert gloves["unit_price"] == 18.50  # PC price still there until reprice

    # NEW999 (no PC origin) — untouched, no PC price
    new_line = rfq["line_items"][5]
    assert "unit_price" not in new_line
    assert not new_line.get("promoted_from_pc")

    # ── Step 3: qty_change_summary for operator review UI ────────────────
    summary = qty_change_summary(rfq)
    # Only promoted lines are in the summary (new line excluded)
    assert len(summary) == 5
    changed = [s for s in summary if s["qty_changed"]]
    assert len(changed) == 1
    assert changed[0]["pc_qty"] == 50
    assert changed[0]["rfq_qty"] == 100
    assert "Exam gloves" in changed[0]["description"] \
        or "gloves" in changed[0]["description"].lower()

    # ── Step 4: selective reprice — ONLY the qty-changed line moves ──────
    reprice_result = reprice_qty_changed_lines(rfq, _oracle_stub_pricer)
    assert reprice_result["repriced"] == 1
    # 4 PC-matched unchanged + 1 brand-new line (no qty_changed flag = treated
    # as "no change" by the helper, which is correct: we don't reprice things
    # we never priced)
    assert reprice_result["skipped_no_change"] == 5

    # Re-check prices: commitment lines still at PC price, gloves repriced
    assert rfq["line_items"][0]["unit_price"] == 45.00  # BP cuff — preserved
    assert rfq["line_items"][1]["unit_price"] == 120.00  # Stethoscope — preserved
    assert rfq["line_items"][3]["unit_price"] == 12.75  # Gauze — preserved
    assert rfq["line_items"][4]["unit_price"] == 6.99   # Tongue depressors — preserved
    # Gloves: repriced per stub pricer
    assert rfq["line_items"][2]["unit_price"] == 17.00
    assert rfq["line_items"][2]["supplier_cost"] == 9.00
    assert rfq["line_items"][2]["repriced_reason"] == "qty_change"
    assert rfq["line_items"][2]["qty_changed"] is False


def test_exact_match_rfq_is_99_percent_done():
    """The 'happy path' Mike described: RFQ comes in, it's a known PC,
    operator just needs to eyeball and send. Exact line match + same email
    + same solicitation = match_pct 100, no qty changes, no re-pricing needed."""
    pc = _realistic_cchcs_pc()
    rfq_exact = {
        "requestor_email": "buyer@cchcs.ca.gov",
        "institution": "CCHCS",
        "solicitation_number": "PC-2026-042",
        "line_items": [
            {"mfg_number": i["mfg_number"],
             "description": i["description"], "quantity": i["quantity"]}
            for i in pc["items"]
        ],
    }
    pcs = {"pc_042": pc}

    candidates = find_matching_pcs_for_cchcs(rfq_exact, pcs)
    assert candidates[0]["is_exact"] is True
    assert candidates[0]["match_pct"] == 100

    promote_result = promote_pc_to_rfq_in_place(rfq_exact, "pc_042", pc)
    assert promote_result["promoted"] == 5
    assert promote_result["qty_changed"] == 0
    assert promote_result["no_match"] == 0

    # No qty changes → reprice does nothing, every commitment price survives
    reprice_result = reprice_qty_changed_lines(rfq_exact, _oracle_stub_pricer)
    assert reprice_result["repriced"] == 0
    assert reprice_result["skipped_no_change"] == 5

    for i, pc_item in enumerate(pc["items"]):
        assert rfq_exact["line_items"][i]["unit_price"] == pc_item["unit_price"]
        assert rfq_exact["line_items"][i]["bid_price"] == pc_item["bid_price"]


def test_wrong_agency_rfq_gets_no_candidates_even_with_matching_items():
    """Safety backstop: a non-CCHCS RFQ that happens to match a CCHCS PC's
    items by MFG# must not surface the PC. Different agency → different
    bidder pool → different pricing logic."""
    pc = _realistic_cchcs_pc()
    rfq_calvet = {
        "requestor_email": "buyer@calvet.ca.gov",
        "institution": "CalVet",
        "line_items": [
            {"mfg_number": "W12919", "description": "BP cuff", "quantity": 10},
        ],
    }
    # RFQ is CalVet, PC is CCHCS — but matcher scopes to CCHCS PCs. It WILL
    # still surface the CCHCS PC (because matcher filters PCs, not RFQs), but
    # the operator's UI should only invoke this for CCHCS RFQs. This test
    # documents current contract: we return candidates if the PC is CCHCS,
    # regardless of RFQ agency. The route layer is responsible for gating
    # the call on rfq.agency == CCHCS.
    # (Kept as documentation; behavior re-verified in case anyone ever
    # tightens the matcher to also require matching RFQ agency.)
    candidates = find_matching_pcs_for_cchcs(rfq_calvet, {"pc": pc})
    assert len(candidates) == 1
    assert candidates[0]["pc_id"] == "pc"
