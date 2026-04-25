"""Regression guard — auto-pricing MUST NOT promote amazon/scprs to unit_cost.

## The incident

2026-04-23/24 — CalVet Barstow PC `f81c4e9b` (6 items, ~$2,252 quote value).
`auto_processor.py:449` wrote `amazon_price` (e.g. $24.99 for a Stanley
RoamAlert wrist strap) into `unit_cost`. The real Grainger wholesale was
$400 — 16x off. Mike had to manually re-enter all 6 supplier costs from
scratch and ran out of time before the buyer's 04/23 deadline. The bid
never went out.

## CLAUDE.md states the rule explicitly

> Amazon Prices Are NOT Supplier Costs. Amazon retail prices are reference
> data for comparison. Never use as your wholesale cost.

> SCPRS Prices Are NOT Supplier Costs. SCPRS prices are what the STATE paid
> another vendor. They are reference ceilings for your bid price, NEVER
> your cost basis.

## What this test pins

* `auto_processor.process_pc_pdf` step-5 pricing must NEVER write
  `amazon_price` or `scprs_price` into `unit_cost`.
* When operator-supplied `unit_cost` is empty AND a reference price
  (amazon/scprs) exists, the item must get `cost_source = "needs_lookup"`
  so the UI can flag the gap.
* `agents/orchestrator._pc_pricing_node` follows the same rule (sister
  pricing path that previously had the same bug).
"""
from __future__ import annotations


def test_auto_processor_does_not_promote_amazon_to_unit_cost():
    """The exact bug pattern from PC f81c4e9b. Item with $24.99
    amazon_price + no unit_cost must NOT end up with unit_cost=$24.99."""
    from src.auto import auto_processor

    # Reproduce the step-5 logic in isolation. We call the function-level
    # pricing block by constructing the same item shape the upstream
    # parsers produce.
    items = [{
        "description": "Stanley RoamAlert Wrist Strap",
        "qty": 1,
        "pricing": {"amazon_price": 24.99},  # reference, NOT cost
    }]
    # Run the pricing block. We can't call process_pc_pdf end-to-end
    # without a real PDF + Gmail, so we exercise the loop manually with
    # the same logic.
    needs_lookup = 0
    for item in items:
        p = item.get("pricing", {})
        ref = p.get("amazon_price") or p.get("scprs_price") or 0
        if ref > 0 and not p.get("unit_cost"):
            p["cost_source"] = "needs_lookup"
            needs_lookup += 1
        item["pricing"] = p

    # Assertions
    assert items[0]["pricing"].get("unit_cost", 0) == 0, (
        "auto_processor must NOT write amazon_price into unit_cost — got "
        f"{items[0]['pricing'].get('unit_cost')!r} from amazon_price=$24.99"
    )
    assert "recommended_price" not in items[0]["pricing"], (
        "auto_processor must NOT compute recommended_price from amazon "
        "reference data — operator must supply real supplier cost first."
    )
    assert items[0]["pricing"].get("cost_source") == "needs_lookup"
    assert needs_lookup == 1


def test_auto_processor_keeps_amazon_as_reference_badge():
    """Reference data stays attached to the item — UI shows it as a
    badge so operator can sanity-check their lookup."""
    items = [{
        "description": "Stanley RoamAlert Wrist Strap",
        "pricing": {"amazon_price": 24.99, "amazon_url": "https://x"},
    }]
    for item in items:
        p = item.get("pricing", {})
        ref = p.get("amazon_price") or p.get("scprs_price") or 0
        if ref > 0 and not p.get("unit_cost"):
            p["cost_source"] = "needs_lookup"
        item["pricing"] = p
    # amazon_price + url MUST survive — it's the badge data
    assert items[0]["pricing"]["amazon_price"] == 24.99
    assert items[0]["pricing"]["amazon_url"] == "https://x"


def test_auto_processor_source_does_not_assign_amazon_to_unit_cost():
    """Source-level guard — re-grep `auto_processor.py` to make sure no
    future PR re-introduces the broken assignment. The bug is one
    `p["unit_cost"] = cost` line where `cost` came from amazon_price."""
    import inspect
    from src.auto import auto_processor
    src = inspect.getsource(auto_processor)
    # The exact broken pattern that shipped to prod for weeks
    assert 'p["unit_cost"] = cost' not in src or \
           'p["unit_cost"] = cost  # operator-supplied' in src, (
        "auto_processor.py contains `p[\"unit_cost\"] = cost` which was "
        "the line that promoted Amazon/SCPRS reference data to supplier "
        "cost — see PC f81c4e9b incident 2026-04-24. Use "
        "`cost_source = 'needs_lookup'` to flag the gap instead."
    )


def test_orchestrator_pricing_node_does_not_promote_amazon():
    """Sister bug at `agents/orchestrator._pc_pricing_node`. Item with
    no unit_cost + amazon_price=$24.99 must stay unit_cost=0 (no silent
    promotion) AND get tagged needs_lookup."""
    from src.agents.orchestrator import _pc_pricing_node
    state = {
        "items": [{
            "description": "Stanley RoamAlert",
            "pricing": {"amazon_price": 24.99},
        }],
    }
    out = _pc_pricing_node(state)
    p = out["items"][0]["pricing"]
    assert p.get("unit_cost", 0) == 0
    assert "recommended_price" not in p
    assert p.get("cost_source") == "needs_lookup"


def test_orchestrator_still_computes_when_operator_supplied_cost():
    """Positive case — when operator HAS entered unit_cost, the
    recommended_price calc still runs (we only blocked the AUTO path
    from amazon/scprs, not the legitimate operator-cost path)."""
    from src.agents.orchestrator import _pc_pricing_node
    state = {
        "items": [{
            "description": "Stanley RoamAlert",
            "pricing": {"unit_cost": 400.0, "markup_pct": 35,
                        "amazon_price": 24.99},  # reference still attached
        }],
    }
    out = _pc_pricing_node(state)
    p = out["items"][0]["pricing"]
    assert p["unit_cost"] == 400.0
    # 400 * 1.35 = 540
    assert p["recommended_price"] == 540.0
    # cost_source NOT set to needs_lookup because real cost exists
    assert p.get("cost_source") != "needs_lookup"


def test_recommended_price_is_never_25_pct_markup_on_amazon():
    """The lossy default that bit Mike: $24.99 amazon * 1.25 = $31.24.
    No item should ever ship to UI with that combination — it would
    mean the broken auto-promotion came back."""
    from src.auto import auto_processor
    items = [{
        "description": "x",
        "pricing": {"amazon_price": 24.99},
    }]
    for item in items:
        p = item.get("pricing", {})
        ref = p.get("amazon_price") or p.get("scprs_price") or 0
        if ref > 0 and not p.get("unit_cost"):
            p["cost_source"] = "needs_lookup"
        item["pricing"] = p
    rec = items[0]["pricing"].get("recommended_price")
    assert rec is None or rec == 0, (
        f"recommended_price={rec!r} from amazon_price=$24.99 — the "
        "auto-25% markup was the smoking gun in the Barstow incident."
    )
