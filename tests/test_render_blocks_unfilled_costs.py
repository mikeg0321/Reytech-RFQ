"""Render aborts when any line item has cost_source='needs_lookup'.

## Why

PR #521 stopped auto-promoting Amazon/SCPRS reference prices into
`unit_cost`. Items now arrive with empty cost cells flagged
`pricing.cost_source = "needs_lookup"`. Without a blocking gate the
operator could (a) miss a flagged cell and (b) render a quote that
either renders the previous wrong price OR shows $0 in that line.

PR #525 (fail-closed validator) catches the `'$0 total'` case for the
WHOLE quote. This PR catches the case where ANY single item still
needs a supplier-cost lookup, even when other items have real costs
and the total is non-zero. Symmetric gate, same `ok=False` shape.

## What this test pins

* PC-side: `generate_quote_from_pc` aborts when any item has
  `pricing.cost_source = "needs_lookup"`
* RFQ-side: `generate_quote_from_rfq` aborts on the same condition
* Result carries `error="cost_source_unfilled"` so callers can branch
* Result includes `unfilled_items` (list of {line, description,
  ref_amazon, ref_scprs}) so the UI can show specifics
* Items without `cost_source` (legitimately empty / never auto-flagged)
  do NOT trip the gate
* Mixed items (some real costs, one needs_lookup) STILL trip the gate —
  partial-cost quotes cannot ship
"""
from __future__ import annotations


def _pc_with_needs_lookup() -> dict:
    """PC where item 2 still needs supplier lookup. Item 1 has real
    operator cost. The Barstow incident shape — 6 items, 5 filled, 1
    forgotten."""
    return {
        "id": "test_pc_needs_lookup",
        "agency_key": "calvet_barstow",
        "agency_name": "California Department of Veterans Affairs",
        "institution": "Veterans Home of California - Barstow",
        "requestor": "Test Buyer",
        "items": [
            {
                "description": "Stanley RoamAlert (real cost entered)",
                "qty": 1,
                "uom": "EA",
                "pricing": {
                    "unit_cost": 400.00,
                    "markup_pct": 35,
                    "cost_source": "operator_supplied",
                },
            },
            {
                "description": "Sedeo Pro Armrest Pad (operator forgot)",
                "qty": 2,
                "uom": "EA",
                "pricing": {
                    "amazon_price": 45.99,  # reference only
                    "cost_source": "needs_lookup",
                },
            },
        ],
    }


def _pc_all_filled() -> dict:
    """Positive case — every item has operator-supplied cost. Render
    must NOT be blocked."""
    return {
        "id": "test_pc_all_filled",
        "agency_key": "calvet_barstow",
        "agency_name": "California Department of Veterans Affairs",
        "institution": "Veterans Home of California - Barstow",
        "requestor": "Test Buyer",
        "items": [
            {
                "description": "Stanley RoamAlert",
                "qty": 1,
                "uom": "EA",
                "pricing": {
                    "unit_cost": 400.00,
                    "markup_pct": 35,
                    "cost_source": "operator_supplied",
                },
            },
        ],
    }


def _rfq_with_needs_lookup() -> dict:
    """RFQ-side equivalent of `_pc_with_needs_lookup`."""
    return {
        "id": "test_rfq_needs_lookup",
        "agency_key": "calvet_barstow",
        "agency_name": "California Department of Veterans Affairs",
        "department": "Skilled Nursing Unit",
        "requestor_email": "buyer@calvet.ca.gov",
        "due_date": "2026-04-30",
        "line_items": [
            {
                "description": "Real-cost item",
                "qty": 1,
                "uom": "EA",
                "unit_price": 540.00,
                "pricing": {
                    "unit_cost": 400.00,
                    "cost_source": "operator_supplied",
                },
            },
            {
                "description": "Forgotten item",
                "qty": 2,
                "uom": "EA",
                "pricing": {
                    "amazon_price": 12.99,
                    "cost_source": "needs_lookup",
                },
            },
        ],
    }


# ── PC-side gate ─────────────────────────────────────────────────────


def test_pc_render_blocks_when_any_item_needs_lookup(tmp_path):
    from src.forms.quote_generator import generate_quote_from_pc
    pc = _pc_with_needs_lookup()
    out = str(tmp_path / "blocked_pc.pdf")
    result = generate_quote_from_pc(pc, out)
    assert result.get("ok") is False
    assert result.get("error") == "cost_source_unfilled"


def test_pc_failure_lists_specific_unfilled_lines(tmp_path):
    from src.forms.quote_generator import generate_quote_from_pc
    pc = _pc_with_needs_lookup()
    out = str(tmp_path / "blocked_pc.pdf")
    result = generate_quote_from_pc(pc, out)
    unfilled = result.get("unfilled_items") or []
    assert len(unfilled) == 1, (
        f"Expected 1 unfilled line, got {len(unfilled)}: {unfilled!r}"
    )
    assert unfilled[0]["line"] == 2, "Unfilled line should be line 2 (1-indexed)"
    assert "Sedeo" in unfilled[0]["description"]
    assert unfilled[0].get("ref_amazon") == 45.99


def test_pc_render_succeeds_when_all_costs_filled(tmp_path):
    """Positive case — gate must not over-trip."""
    from src.forms.quote_generator import generate_quote_from_pc
    pc = _pc_all_filled()
    out = str(tmp_path / "valid_pc.pdf")
    result = generate_quote_from_pc(pc, out)
    # Gate did not fire — render proceeded normally
    assert result.get("error") != "cost_source_unfilled"


# ── RFQ-side gate ────────────────────────────────────────────────────


def test_rfq_render_blocks_when_any_item_needs_lookup(tmp_path):
    from src.forms.quote_generator import generate_quote_from_rfq
    rfq = _rfq_with_needs_lookup()
    out = str(tmp_path / "blocked_rfq.pdf")
    result = generate_quote_from_rfq(rfq, out)
    assert result.get("ok") is False
    assert result.get("error") == "cost_source_unfilled"


def test_rfq_failure_lists_specific_unfilled_lines(tmp_path):
    from src.forms.quote_generator import generate_quote_from_rfq
    rfq = _rfq_with_needs_lookup()
    out = str(tmp_path / "blocked_rfq.pdf")
    result = generate_quote_from_rfq(rfq, out)
    unfilled = result.get("unfilled_items") or []
    assert len(unfilled) == 1
    assert unfilled[0]["line"] == 2
    assert "Forgotten" in unfilled[0]["description"]


def test_rfq_render_proceeds_when_no_cost_source_set(tmp_path):
    """Items without `cost_source` (e.g. legitimately operator-blank,
    or never auto-processed) MUST NOT trip the gate. The gate fires on
    the explicit `"needs_lookup"` marker only — it doesn't second-guess
    items that were never auto-flagged."""
    from src.forms.quote_generator import generate_quote_from_rfq
    rfq = {
        "id": "test_rfq_no_marker",
        "agency_key": "calvet_barstow",
        "agency_name": "California Department of Veterans Affairs",
        "due_date": "2026-04-30",
        "line_items": [{
            "description": "Manual item (no cost_source marker)",
            "qty": 1,
            "uom": "EA",
            "unit_price": 540.00,
        }],
    }
    out = str(tmp_path / "no_marker_rfq.pdf")
    result = generate_quote_from_rfq(rfq, out)
    # Gate did not fire (different from contract validator gate which
    # might still fire on $0 total — that's an orthogonal check)
    assert result.get("error") != "cost_source_unfilled"


def test_pdf_is_not_written_when_gate_fires(tmp_path):
    """Gate fires BEFORE rendering, so no PDF should land on disk."""
    import os
    from src.forms.quote_generator import generate_quote_from_pc
    pc = _pc_with_needs_lookup()
    out = str(tmp_path / "should_not_exist.pdf")
    generate_quote_from_pc(pc, out)
    assert not os.path.exists(out), (
        f"Gate fired but PDF still landed at {out!r} — render proceeded "
        "past the gate."
    )


def test_helper_returns_empty_for_legitimate_no_cost_items():
    """Internal helper sanity — items with no `cost_source` key at all
    are NOT flagged as needs_lookup. Only the explicit marker counts."""
    from src.forms.quote_generator import _items_with_unfilled_costs
    items = [
        {"description": "no pricing dict at all"},
        {"description": "empty pricing dict", "pricing": {}},
        {"description": "real cost, no source",
         "pricing": {"unit_cost": 400.00}},
        {"description": "operator marked",
         "pricing": {"unit_cost": 400.00, "cost_source": "operator_supplied"}},
    ]
    assert _items_with_unfilled_costs(items) == []


def test_helper_flags_only_needs_lookup_marker():
    """Specifically check: only `cost_source = "needs_lookup"` trips
    the helper. Other source values (e.g. `"catalog_match"`,
    `"web_research"`) are operator-acceptable and don't fire."""
    from src.forms.quote_generator import _items_with_unfilled_costs
    items = [
        {"description": "needs lookup",
         "pricing": {"cost_source": "needs_lookup", "amazon_price": 24.99}},
        {"description": "from catalog",
         "pricing": {"cost_source": "catalog_match", "unit_cost": 400.00}},
        {"description": "from web",
         "pricing": {"cost_source": "web_research", "unit_cost": 400.00}},
    ]
    flagged = _items_with_unfilled_costs(items)
    assert len(flagged) == 1
    assert flagged[0]["line"] == 1
    assert "needs lookup" in flagged[0]["description"]
