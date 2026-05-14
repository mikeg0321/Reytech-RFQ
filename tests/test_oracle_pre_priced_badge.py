"""PR-AK — operator-UI 🔮 Oracle badge for items pre-priced at ingest.

Companion to PR-AJ #991 (auto-price-on-ingest reference fields).
Pre-fix, an item flagged with `auto_priced_at_ingest=True` had no
visible operator-surface signal. The operator couldn't tell at a
glance which items Oracle pre-populated cost basis for vs. which
needed manual lookup. This adds a small 🔮 Oracle badge on both the
PC items table (via routes_pricecheck.py:_cat_chip_specs) and the
RFQ items table (via rfq_detail.html in the cost cell).

Tests pin:
  1. RFQ detail renders the 🔮 Oracle badge when item carries
     auto_priced_at_ingest=True. The badge is identified by
     data-testid="oracle-pre-priced-badge".
  2. RFQ detail does NOT render the badge when the flag is absent
     or falsy.
  3. PC detail renders the 🔮 icon in _intel_badges when an item
     carries auto_priced_at_ingest=True.
  4. PC detail does NOT render the 🔮 icon when the flag is absent.

CHROME-VERIFIED replacement: these template-render tests are the
gating signal because no production record yet carries
auto_priced_at_ingest=True (PR-AJ only enriches NEW inbound RFQs).
A live chrome walk would require seeding a record manually, which
this test does in pure-Python without the prod-data risk.
"""
from __future__ import annotations

import json


def _seed_rfq_with_item(client, rfq_id, item_overrides):
    """Write a minimal RFQ record directly to the JSON store so the
    detail page has something to render."""
    from src.api.dashboard import _save_single_rfq
    rfq = {
        "id": rfq_id,
        "rfq_number": "TEST-RFQ-AK",
        "status": "parsed",
        "agency": "cchcs",
        "institution": "CSP-SAC",
        "ship_to": "100 Prison Rd, Coalinga, CA 93210",
        "tax_rate": 8.975,
        "tax_source": "cdtfa_api",
        "tax_jurisdiction": "COALINGA",
        "tax_validated": True,
        "line_items": [dict({
            "description": "Test Widget",
            "qty": 5,
            "uom": "EA",
            "item_number": "WID-001",
            "supplier_cost": 0,
            "price_per_unit": 0,
        }, **item_overrides)],
    }
    _save_single_rfq(rfq_id, rfq)


def _seed_pc_with_item(pc_id, item_overrides):
    from src.api.dashboard import _save_single_pc
    pc = {
        "id": pc_id,
        "pc_number": "TEST-PC-AK",
        "status": "parsed",
        "agency": "cchcs",
        "institution": "CSP-SAC",
        "ship_to": "100 Prison Rd, Coalinga, CA 93210",
        "items": [dict({
            "description": "Test Widget",
            "qty": 5,
            "uom": "EA",
            "mfg_number": "WID-001",
            "pricing": {},  # template expects this sub-dict
        }, **item_overrides)],
    }
    _save_single_pc(pc_id, pc)


# ── RFQ ─────────────────────────────────────────────────────────────


def test_rfq_renders_oracle_badge_when_auto_priced_flag_set(client, temp_data_dir):
    """RFQ detail page shows the 🔮 Oracle badge when an item has
    auto_priced_at_ingest=True."""
    rfq_id = "rfq_ak_pos"
    _seed_rfq_with_item(client, rfq_id, {"auto_priced_at_ingest": True})
    resp = client.get(f"/rfq/{rfq_id}")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'data-testid="oracle-pre-priced-badge"' in body
    assert "🔮 Oracle" in body


def test_rfq_does_not_render_oracle_badge_when_flag_missing(client, temp_data_dir):
    """RFQ detail page does NOT show the 🔮 Oracle badge when the
    flag is absent or False."""
    rfq_id = "rfq_ak_neg"
    _seed_rfq_with_item(client, rfq_id, {})  # no flag
    resp = client.get(f"/rfq/{rfq_id}")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'data-testid="oracle-pre-priced-badge"' not in body


# ── PC ──────────────────────────────────────────────────────────────


def test_pc_renders_oracle_chip_when_auto_priced_flag_set(client, temp_data_dir):
    """PC detail page renders the 🔮 catalog chip when an item has
    auto_priced_at_ingest=True (via _cat_chip_specs in
    routes_pricecheck.py)."""
    pc_id = "pc_ak_pos"
    _seed_pc_with_item(pc_id, {"auto_priced_at_ingest": True})
    resp = client.get(f"/pricecheck/{pc_id}")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    # The chip uses the 🔮 emoji as its icon. Confirm presence + title.
    assert "🔮" in body
    assert "Oracle pre-priced at ingest" in body


def test_pc_does_not_render_oracle_chip_when_flag_missing(client, temp_data_dir):
    """PC detail page does NOT render the 🔮 chip when the flag is
    absent. Validates the negative case (no false positive)."""
    pc_id = "pc_ak_neg"
    _seed_pc_with_item(pc_id, {})
    resp = client.get(f"/pricecheck/{pc_id}")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    # The badge's tooltip text is unique; absent flag → no chip,
    # so no tooltip. (The 🔮 emoji could appear elsewhere on the
    # page for unrelated reasons; the tooltip text is the precise
    # signal.)
    assert "Oracle pre-priced at ingest" not in body
