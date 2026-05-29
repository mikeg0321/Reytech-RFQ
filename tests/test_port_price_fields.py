"""Tests for the canonical PC→RFQ price porter `_port_price_fields`.

After the 2026-05-29 convergence, BOTH the operator-confirmed promote
(`promote_pc_to_rfq_in_place`) and the auto-link ingest path
(`dashboard._link_rfq_to_pc`) route price fields through this one function, so
field mapping and the "priced?" verdict can never drift apart again. The
`overwrite` flag is the only behavioral difference between the two callers:
  - overwrite=True  → operator-confirmed promote: PC commitment wins.
  - overwrite=False → auto-link during ingest: fill gaps, never clobber.
"""
from __future__ import annotations

from src.core.pc_rfq_linker import _port_price_fields


# ── Nesting + field-name mapping (the incident class) ────────────────────────

def test_nested_pricing_maps_to_render_fields():
    rfq = {"description": "MOUNT, IV POLE"}
    pc = {"description": "MOUNT, IV POLE",
          "pricing": {"unit_cost": 41.20, "recommended_price": 51.50}}
    assert _port_price_fields(rfq, pc) is True
    assert rfq["supplier_cost"] == 41.20      # YOUR COST cell
    assert rfq["price_per_unit"] == 51.50     # BID PRICE cell


def test_top_level_aliases_map_to_render_fields():
    rfq = {"description": "thing"}
    pc = {"vendor_cost": 12.58, "unit_price": 15.72}
    assert _port_price_fields(rfq, pc) is True
    assert rfq["supplier_cost"] == 12.58
    assert rfq["price_per_unit"] == 15.72


# ── overwrite=True (operator-confirmed promote) ──────────────────────────────

def test_overwrite_true_clobbers_stale_rfq_values():
    rfq = {"supplier_cost": 88.88, "price_per_unit": 99.99}
    pc = {"vendor_cost": 25.00, "unit_price": 45.00}
    _port_price_fields(rfq, pc, overwrite=True)
    assert rfq["supplier_cost"] == 25.00      # PC commitment wins
    assert rfq["price_per_unit"] == 45.00


# ── overwrite=False (auto-link ingest) ───────────────────────────────────────

def test_overwrite_false_preserves_existing_rfq_values():
    rfq = {"supplier_cost": 88.88, "price_per_unit": 99.99}
    pc = {"vendor_cost": 25.00, "unit_price": 45.00}
    _port_price_fields(rfq, pc, overwrite=False)
    assert rfq["supplier_cost"] == 88.88      # not clobbered
    assert rfq["price_per_unit"] == 99.99


def test_overwrite_false_fills_gaps():
    rfq = {"supplier_cost": 30.00}             # has cost, missing bid
    pc = {"vendor_cost": 25.00, "unit_price": 45.00}
    assert _port_price_fields(rfq, pc, overwrite=False) is True
    assert rfq["supplier_cost"] == 30.00       # kept
    assert rfq["price_per_unit"] == 45.00       # gap filled


# ── The "priced?" verdict (drives the honest counter) ────────────────────────

def test_returns_false_when_pc_has_no_cost_or_bid():
    rfq = {"description": "MOUNT, IV POLE"}
    pc = {"description": "MOUNT, IV POLE"}      # no pricing anywhere
    assert _port_price_fields(rfq, pc) is False
    assert not rfq.get("supplier_cost")
    assert not rfq.get("price_per_unit")


def test_scprs_only_is_a_ceiling_not_a_port():
    rfq = {"description": "gauze"}
    pc = {"description": "gauze", "pricing": {"scprs_price": 9.99}}
    assert _port_price_fields(rfq, pc) is False   # SCPRS != priced
    assert rfq["scprs_last_price"] == 9.99        # but ceiling still recorded
    assert not rfq.get("supplier_cost")


def test_amazon_price_is_never_read_as_cost():
    # Pricing Guard Rail: Amazon retail is reference, never cost basis.
    rfq = {"description": "widget"}
    pc = {"description": "widget", "pricing": {"amazon_price": 19.99}}
    assert _port_price_fields(rfq, pc) is False
    assert not rfq.get("supplier_cost")
