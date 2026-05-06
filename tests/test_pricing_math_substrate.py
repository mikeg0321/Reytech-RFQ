"""Pricing math substrate tests — PR-1, 2026-05-06 audit substrate fix.

Both PC and RFQ save paths now call `reconcile_line_item` /
`reconcile_items` from `src/core/pricing_math.py`. These tests pin
the math against the rules the audit named:

  Rule 1: cost + price → derive markup_pct (Mike P0 from 2026-05-05:
          typed OUR PRICE $16 with cost $8, expected markup=100%,
          got stale 20%).
  Rule 2: cost + markup → derive price (forward auto-compute when
          price was missing — was the legacy PC behavior).
  Rule 3: insufficient signal → leave alone.

The "PC and RFQ produce identical output" test is the load-bearing
one: it imports the pure helper from both call sites' perspective and
runs the same input through, asserting equality. If a future change
adds new pricing logic to only one side, this test fails.
"""
from __future__ import annotations

import os

from src.core.pricing_math import (
    reconcile_line_item,
    reconcile_items,
    canonical_unit_price,
)


# ── Rule 1: reverse-derive markup ─────────────────────────────────


def test_rule1_heel_donut_incident_8_to_16_yields_100pct():
    """Mike's exact 2026-05-05 incident reproduction."""
    item = {"vendor_cost": 8, "unit_price": 16, "markup_pct": 20, "qty": 1}
    reconcile_line_item(item)
    assert item["markup_pct"] == 100.0
    assert item["pricing"]["markup_pct"] == 100.0
    # Both PC alias and RFQ alias should match
    assert item["unit_price"] == 16
    assert item["price_per_unit"] == 16


def test_rule1_skips_within_tolerance():
    """Stored markup within 0.5pt of derived → leave alone (no log spam)."""
    item = {"vendor_cost": 10, "unit_price": 12.50, "markup_pct": 25.1}
    reconcile_line_item(item)
    # 25.1 vs derived 25.0 — diff 0.1, under 0.5 tolerance, leave alone
    assert item["markup_pct"] == 25.1


def test_rule1_overwrites_when_drift_exceeds_tolerance():
    item = {"vendor_cost": 10, "unit_price": 12.50, "markup_pct": 30}
    reconcile_line_item(item)
    # 30 vs derived 25.0 — diff 5.0, exceeds tolerance, overwrite
    assert item["markup_pct"] == 25.0


def test_rule1_fills_missing_markup():
    item = {"supplier_cost": 100, "price_per_unit": 125}  # RFQ aliases
    reconcile_line_item(item)
    assert item["markup_pct"] == 25.0
    # PC aliases also written so PC readers see the same data
    assert item["vendor_cost"] == 100
    assert item["unit_price"] == 125


# ── Rule 2: forward-compute price ─────────────────────────────────


def test_rule2_cost_plus_markup_derives_price():
    item = {"vendor_cost": 50, "markup_pct": 30}
    reconcile_line_item(item)
    assert item["unit_price"] == 65.0
    assert item["price_per_unit"] == 65.0
    assert item["pricing"]["recommended_price"] == 65.0


def test_rule2_pricing_dict_input():
    item = {"pricing": {"unit_cost": 80, "markup_pct": 25}}
    reconcile_line_item(item)
    assert item["unit_price"] == 100.0


# ── Rule 3: insufficient signal ───────────────────────────────────


def test_rule3_only_cost_leaves_price_alone():
    item = {"vendor_cost": 50}
    reconcile_line_item(item)
    assert "unit_price" not in item or item.get("unit_price") in (None, 0)
    assert "markup_pct" not in item


def test_no_bid_skipped():
    item = {"vendor_cost": 50, "unit_price": 200, "markup_pct": 10, "no_bid": True}
    reconcile_line_item(item)
    # markup left untouched even though it'd derive to 300%
    assert item["markup_pct"] == 10


def test_zero_cost_no_zero_division():
    item = {"vendor_cost": 0, "unit_price": 100, "markup_pct": 25}
    reconcile_line_item(item)
    # Cost is 0, no derivation possible — leave markup alone
    assert item["markup_pct"] == 25


# ── PC and RFQ identical-math contract ────────────────────────────


def test_pc_alias_input_and_rfq_alias_input_produce_same_output():
    """Critical contract: feed the same logical pricing into the PC
    field-name shape AND the RFQ field-name shape; reconciled output
    must be numerically identical. This is the load-bearing test for
    'one source of truth across both paths.'"""
    pc_item = {
        "vendor_cost": 100,
        "unit_price": 130,
        "markup_pct": 20,  # stale — should get rewritten to 30
    }
    rfq_item = {
        "supplier_cost": 100,
        "price_per_unit": 130,
        "markup_pct": 20,  # stale — should get rewritten to 30
    }
    reconcile_line_item(pc_item)
    reconcile_line_item(rfq_item)
    # Both items should now report the same canonical numbers under
    # both naming conventions.
    assert pc_item["markup_pct"] == rfq_item["markup_pct"] == 30.0
    assert pc_item["unit_price"] == rfq_item["unit_price"] == 130
    assert pc_item["price_per_unit"] == rfq_item["price_per_unit"] == 130
    assert pc_item["vendor_cost"] == rfq_item["vendor_cost"] == 100
    assert pc_item["supplier_cost"] == rfq_item["supplier_cost"] == 100


def test_canonical_unit_price_agrees_after_reconcile():
    """After reconcile, canonical_unit_price (read path) must agree with
    the persisted unit_price (write path). The 2026-04-23 stale-price
    incident was exactly this gap."""
    item = {"vendor_cost": 465.40, "unit_price": 558.48, "markup_pct": 20}
    reconcile_line_item(item)
    # Reverse-derived markup from cost+price overrides the stale 20%
    derived = round((558.48 - 465.40) / 465.40 * 100, 1)
    assert item["markup_pct"] == derived
    # The read-path canonical accessor recomputes from cost*markup, so
    # after reconciliation they MUST match within rounding.
    assert abs(canonical_unit_price(item) - 558.48) < 0.01


# ── Both call sites import it ─────────────────────────────────────


def test_pc_save_prices_imports_reconcile():
    p = os.path.join(os.path.dirname(__file__), "..",
                     "src/api/modules/routes_pricecheck.py")
    with open(p, encoding="utf-8") as f:
        src = f.read()
    assert "from src.core.pricing_math import reconcile_items" in src


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
    """The PR #765 inline block must be GONE from routes_pricecheck.py.
    If anyone re-adds inline reverse-markup math here, this test fails
    so they consolidate to pricing_math instead."""
    p = os.path.join(os.path.dirname(__file__), "..",
                     "src/api/modules/routes_pricecheck.py")
    with open(p, encoding="utf-8") as f:
        src = f.read()
    # The exact SAVE-PRICES log line from the inline block
    assert "SAVE-PRICES reverse-markup" not in src, (
        "Inline reverse-markup block re-added to routes_pricecheck.py. "
        "Use src.core.pricing_math.reconcile_items instead."
    )


# ── reconcile_items list helper ────────────────────────────────────


def test_reconcile_items_returns_touched_count():
    items = [
        {"vendor_cost": 10, "unit_price": 12, "markup_pct": 50},  # stale → fix
        {"vendor_cost": 5, "markup_pct": 100},                    # forward → fill
        {"vendor_cost": 100, "unit_price": 125, "markup_pct": 25},  # already right
        {"no_bid": True, "vendor_cost": 1, "unit_price": 100, "markup_pct": 99},  # skipped
    ]
    n = reconcile_items(items)
    # First two changed; third is no-op; fourth is no-bid (skipped, unchanged).
    assert n == 2
    assert items[0]["markup_pct"] == 20.0
    assert items[1]["unit_price"] == 10.0
    assert items[2]["markup_pct"] == 25
    # No-bid item must be untouched — markup stays at the bogus 99.
    assert items[3]["markup_pct"] == 99
    assert items[3]["unit_price"] == 100
