"""Pricing math substrate tests — PR-1 (2026-05-06) + Mike P0 fix (2026-05-12).

`reconcile_line_item` is now intent-aware. The Heel Donut flow (PC SAVE
PRICES — operator types OUR PRICE, markup must follow) calls with
`prefer="price"`. All other callers — RFQ autosave, enrichment, ingest
reparse — use the default `prefer="markup"`, which makes the operator's
typed markup_pct sticky: a stale persisted price can no longer reverse-
derive markup and silently overwrite operator intent.

The 2026-05-12 incident this enforces: Mike was quoting `rfq_8efe9fae`
at 8% markup. Each RFQ autosave reverse-derived markup from a stale 35%
price snapshot, rewriting markup_pct to 35% on disk every save. Mike
saw the final quote at 35% and reported "i never once touched any of
those actions". The bug was the default — autosave inherited the PC's
price-wins semantic.
"""
from __future__ import annotations

import os

from src.core.pricing_math import (
    reconcile_line_item,
    reconcile_items,
    canonical_unit_price,
)


# ── prefer="price": Heel Donut (PC SAVE PRICES) ───────────────────


def test_price_wins_heel_donut_incident_8_to_16_yields_100pct():
    """2026-05-05 incident: PC SAVE PRICES path. Operator types $16
    against $8 cost; stored markup=20 must flip to 100."""
    item = {"vendor_cost": 8, "unit_price": 16, "markup_pct": 20, "qty": 1}
    reconcile_line_item(item, prefer="price")
    assert item["markup_pct"] == 100.0
    assert item["pricing"]["markup_pct"] == 100.0
    assert item["unit_price"] == 16
    assert item["price_per_unit"] == 16


def test_price_wins_skips_within_tolerance():
    """Stored markup within 0.5pt of derived → leave alone (no log spam)."""
    item = {"vendor_cost": 10, "unit_price": 12.50, "markup_pct": 25.1}
    reconcile_line_item(item, prefer="price")
    assert item["markup_pct"] == 25.1


def test_price_wins_overwrites_when_drift_exceeds_tolerance():
    item = {"vendor_cost": 10, "unit_price": 12.50, "markup_pct": 30}
    reconcile_line_item(item, prefer="price")
    # 30 vs derived 25.0 — diff 5.0, exceeds tolerance, overwrite
    assert item["markup_pct"] == 25.0


# ── prefer="markup" (default): autosave + enrichment + ingest ─────


def test_markup_wins_default_protects_operator_intent():
    """Mike P0 2026-05-12 (`rfq_8efe9fae`): operator set markup=8% on
    a record whose persisted unit_price still reflects the prior 35%
    snapshot. Default autosave reconcile must FORWARD-COMPUTE price
    from cost+markup — never reverse-derive markup back to 35%."""
    item = {"vendor_cost": 100, "unit_price": 135, "markup_pct": 8, "qty": 1}
    reconcile_line_item(item)  # default prefer="markup"
    # markup is sticky at the operator's typed 8%
    assert item["markup_pct"] == 8
    # price gets healed to match the markup intent
    assert item["unit_price"] == 108.0
    assert item["price_per_unit"] == 108.0


def test_markup_wins_idempotent_when_already_coherent():
    """Stable record (cost+markup+price all agree): default reconcile
    is a no-op. No log spam, no mutation."""
    item = {"vendor_cost": 100, "unit_price": 135, "markup_pct": 35}
    before_markup = item["markup_pct"]
    before_price = item["unit_price"]
    reconcile_line_item(item)
    assert item["markup_pct"] == before_markup
    assert item["unit_price"] == before_price


def test_markup_wins_fills_missing_markup_when_only_cost_price():
    """No markup present → back-fill from cost+price. Same as the
    legacy behavior when markup was None; nothing operator-typed gets
    overwritten."""
    item = {"supplier_cost": 100, "price_per_unit": 125}
    reconcile_line_item(item)  # default prefer="markup"
    assert item["markup_pct"] == 25.0
    assert item["vendor_cost"] == 100
    assert item["unit_price"] == 125


def test_markup_wins_forward_computes_when_price_missing():
    item = {"vendor_cost": 50, "markup_pct": 30}
    reconcile_line_item(item)  # default
    assert item["unit_price"] == 65.0
    assert item["price_per_unit"] == 65.0
    assert item["pricing"]["recommended_price"] == 65.0


def test_markup_wins_pricing_dict_input():
    item = {"pricing": {"unit_cost": 80, "markup_pct": 25}}
    reconcile_line_item(item)  # default
    assert item["unit_price"] == 100.0


# ── Insufficient signal — never mutate ────────────────────────────


def test_only_cost_leaves_price_alone():
    item = {"vendor_cost": 50}
    reconcile_line_item(item)
    assert "unit_price" not in item or item.get("unit_price") in (None, 0)
    assert "markup_pct" not in item


def test_no_bid_skipped():
    item = {"vendor_cost": 50, "unit_price": 200, "markup_pct": 10, "no_bid": True}
    reconcile_line_item(item, prefer="price")
    # markup left untouched even though it'd derive to 300%
    assert item["markup_pct"] == 10


def test_zero_cost_no_zero_division():
    item = {"vendor_cost": 0, "unit_price": 100, "markup_pct": 25}
    reconcile_line_item(item, prefer="price")
    assert item["markup_pct"] == 25


# ── PC and RFQ identical-math contract ────────────────────────────


def test_pc_alias_input_and_rfq_alias_input_produce_same_output_price_wins():
    """Heel Donut: both field-name shapes resolve to identical canonical
    output under prefer='price'."""
    pc_item = {"vendor_cost": 100, "unit_price": 130, "markup_pct": 20}
    rfq_item = {"supplier_cost": 100, "price_per_unit": 130, "markup_pct": 20}
    reconcile_line_item(pc_item, prefer="price")
    reconcile_line_item(rfq_item, prefer="price")
    assert pc_item["markup_pct"] == rfq_item["markup_pct"] == 30.0
    assert pc_item["unit_price"] == rfq_item["unit_price"] == 130
    assert pc_item["price_per_unit"] == rfq_item["price_per_unit"] == 130
    assert pc_item["vendor_cost"] == rfq_item["vendor_cost"] == 100
    assert pc_item["supplier_cost"] == rfq_item["supplier_cost"] == 100


def test_pc_alias_input_and_rfq_alias_input_produce_same_output_markup_wins():
    """Default autosave path: same shape on both sides, prefer='markup'.
    Markup stays at 20; price gets healed from 130 → cost*1.20 = 120."""
    pc_item = {"vendor_cost": 100, "unit_price": 130, "markup_pct": 20}
    rfq_item = {"supplier_cost": 100, "price_per_unit": 130, "markup_pct": 20}
    reconcile_line_item(pc_item)
    reconcile_line_item(rfq_item)
    assert pc_item["markup_pct"] == rfq_item["markup_pct"] == 20
    assert pc_item["unit_price"] == rfq_item["unit_price"] == 120.0
    assert pc_item["price_per_unit"] == rfq_item["price_per_unit"] == 120.0


def test_canonical_unit_price_agrees_after_reconcile_price_wins():
    item = {"vendor_cost": 465.40, "unit_price": 558.48, "markup_pct": 20}
    reconcile_line_item(item, prefer="price")
    derived = round((558.48 - 465.40) / 465.40 * 100, 1)
    assert item["markup_pct"] == derived
    assert abs(canonical_unit_price(item) - 558.48) < 0.01


# ── Call sites import the helper ──────────────────────────────────


def test_pc_save_prices_imports_reconcile():
    p = os.path.join(os.path.dirname(__file__), "..",
                     "src/api/modules/routes_pricecheck.py")
    with open(p, encoding="utf-8") as f:
        src = f.read()
    assert "from src.core.pricing_math import reconcile_items" in src


def test_pc_save_prices_uses_price_wins():
    """PC SAVE PRICES must pin `prefer='price'` so the Heel Donut flow
    works. If a refactor drops the kwarg, this test fails so the next
    operator who types OUR PRICE doesn't get stale markup downstream."""
    p = os.path.join(os.path.dirname(__file__), "..",
                     "src/api/modules/routes_pricecheck.py")
    with open(p, encoding="utf-8") as f:
        src = f.read()
    assert 'prefer="price"' in src or "prefer='price'" in src, (
        "PC SAVE PRICES must call reconcile_items with prefer='price'. "
        "Without it, operator-typed OUR PRICE won't propagate to markup_pct."
    )


def test_rfq_autosave_imports_reconcile():
    p = os.path.join(os.path.dirname(__file__), "..",
                     "src/api/modules/routes_rfq_gen.py")
    with open(p, encoding="utf-8") as f:
        src = f.read()
    assert "from src.core.pricing_math import reconcile_items" in src


def test_rfq_form_update_imports_reconcile():
    p = os.path.join(os.path.dirname(__file__), "..",
                     "src/api/modules/routes_rfq.py")
    with open(p, encoding="utf-8") as f:
        src = f.read()
    assert "from src.core.pricing_math import reconcile_items" in src


def test_no_inline_reverse_markup_remains_in_pc_save():
    p = os.path.join(os.path.dirname(__file__), "..",
                     "src/api/modules/routes_pricecheck.py")
    with open(p, encoding="utf-8") as f:
        src = f.read()
    assert "SAVE-PRICES reverse-markup" not in src, (
        "Inline reverse-markup block re-added to routes_pricecheck.py. "
        "Use src.core.pricing_math.reconcile_items instead."
    )


# ── reconcile_items list helper ────────────────────────────────────


def test_reconcile_items_returns_touched_count_price_wins():
    items = [
        {"vendor_cost": 10, "unit_price": 12, "markup_pct": 50},
        {"vendor_cost": 5, "markup_pct": 100},
        {"vendor_cost": 100, "unit_price": 125, "markup_pct": 25},
        {"no_bid": True, "vendor_cost": 1, "unit_price": 100, "markup_pct": 99},
    ]
    n = reconcile_items(items, prefer="price")
    assert n == 2
    assert items[0]["markup_pct"] == 20.0
    assert items[1]["unit_price"] == 10.0
    assert items[2]["markup_pct"] == 25
    assert items[3]["markup_pct"] == 99
    assert items[3]["unit_price"] == 100


def test_reconcile_items_default_markup_wins_protects_intent():
    """Default prefer='markup' on an autosave-shaped batch: markup is
    sticky on every item; price gets forward-computed where it drifted."""
    items = [
        {"vendor_cost": 100, "unit_price": 135, "markup_pct": 8},   # Mike's case
        {"vendor_cost": 50, "unit_price": 75, "markup_pct": 50},    # coherent
        {"vendor_cost": 200, "markup_pct": 10},                     # no price
    ]
    reconcile_items(items)  # default
    # Mike's case: 8% intent preserved, price healed
    assert items[0]["markup_pct"] == 8
    assert items[0]["unit_price"] == 108.0
    # Coherent: unchanged
    assert items[1]["markup_pct"] == 50
    assert items[1]["unit_price"] == 75.0
    # Missing price filled
    assert items[2]["unit_price"] == 220.0
