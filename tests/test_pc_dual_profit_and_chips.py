"""Tests for the per-row dual-profit pills + inline catalog chips (D2 batch).

These render in the PC editor's items_html. Both features have a single source
of truth (routes_pricecheck.pc_detail), so we exercise them by hitting that
route with a seeded PC that has the relevant fields.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PRICECHECK_ROUTES = ROOT / "src" / "api" / "modules" / "routes_pricecheck.py"


# ── Static surface area ────────────────────────────────────────────────────

class TestRendererCode:
    """Confirm the renderer keeps the dual-pill + chip-spec contract.

    Catches accidental refactors that would silently drop a chip type or
    revert dual-profit back to a single inline string.
    """
    def setup_method(self):
        self.src = PRICECHECK_ROUTES.read_text(encoding="utf-8")

    def test_dual_profit_pill_classes_present(self):
        # Both pills must render — operator sees MSRP and discount side by side.
        assert "profit-pill profit-pill-msrp" in self.src
        assert "profit-pill profit-pill-disc" in self.src

    def test_dual_profit_only_when_discount_below_unit_cost(self):
        # Guard: don't show dual pills for every item — only those with a real
        # discount under unit_cost. discount_cost is set up that way at line ~558.
        assert "discount_cost = _sale_price if (_sale_price > 0 and _sale_price < unit_cost) else 0" in self.src

    def test_catalog_chip_specs_cover_five_signals(self):
        # If a future change drops one of these, an operator can't tell at a
        # glance what enrichment the row has.
        for label in ("Image stored", "Supplier URL", "UPC captured",
                      "ASIN captured", "MFG# present"):
            assert label in self.src

    def test_chips_only_emit_when_data_present(self):
        # The chip-spec list checks bool(item.get(...)) — that's how empty rows
        # stay clean. If someone changes this to "always emit", we'd flood the UI.
        assert "if _have" in self.src


# ── Live render through Flask client ───────────────────────────────────────

import os


def _write_pc_to_store(temp_data_dir, pc):
    """Write a PC dict directly to price_checks.json in the test data dir."""
    path = os.path.join(temp_data_dir, "price_checks.json")
    existing = {}
    if os.path.exists(path):
        with open(path) as f:
            try:
                existing = json.load(f)
            except Exception:
                existing = {}
    existing[pc["id"]] = pc
    with open(path, "w") as f:
        json.dump(existing, f, default=str)


def _make_pc_with_discount():
    """Seed a PC where the renderer will compute final_price=30 from cost=20 + markup=50%."""
    pc_id = "test_dual_profit_pc"
    return pc_id, {
        "pc_id": pc_id,
        "id": pc_id,
        "agency": "CCHCS",
        "status": "draft",
        "items": [{
            "line_num": 1,
            "mfg_number": "TEST-MFG-001",
            "qty": 5,
            "uom": "EA",
            "description": "Test item with sale discount",
            "vendor_cost": 20.00,    # → unit_cost = 20
            "sale_price": 12.00,     # → discount_cost = 12 (below unit_cost)
            "list_price": 20.00,
            "photo_url": "https://example.com/img.jpg",
            "item_link": "https://amazon.com/dp/B0TEST",
            "asin": "B0TEST",
            "upc": "012345678905",
            "pricing": {
                "unit_cost": 20.00,
                "markup_pct": 50,    # → final_price = 20 * 1.50 = 30
                "amazon_list_price": 20.00,
                "amazon_sale_price": 12.00,
            },
        }],
    }


@pytest.mark.usefixtures("auth_client", "temp_data_dir")
class TestDualProfitRendersInPage:
    def test_dual_pills_render_when_discount_present(self, auth_client, temp_data_dir):
        pc_id, pc = _make_pc_with_discount()
        _write_pc_to_store(temp_data_dir, pc)
        resp = auth_client.get(f"/pricecheck/{pc_id}")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "profit-pill-msrp" in body
        assert "profit-pill-disc" in body
        # MSRP profit: (30 - 20) * 5 = 50.00
        assert "$50.00" in body
        # Discount profit: (30 - 12) * 5 = 90.00
        assert "$90.00 disc" in body

    def test_catalog_chips_render_for_enriched_item(self, auth_client, temp_data_dir):
        pc_id, pc = _make_pc_with_discount()
        _write_pc_to_store(temp_data_dir, pc)
        resp = auth_client.get(f"/pricecheck/{pc_id}")
        body = resp.get_data(as_text=True)
        for title in ("Image stored", "Supplier URL", "UPC captured",
                      "ASIN captured", "MFG# present"):
            assert f'title="{title}"' in body, f"missing chip: {title}"

    def test_no_disc_pill_when_no_sale_price(self, auth_client, temp_data_dir):
        """Item without a sale price below unit_cost should NOT render disc pill."""
        pc_id = "test_no_disc_pc"
        pc = {
            "pc_id": pc_id,
            "id": pc_id,
            "agency": "CCHCS",
            "status": "draft",
            "items": [{
                "line_num": 1,
                "mfg_number": "TEST-MFG-002",
                "qty": 3,
                "uom": "EA",
                "description": "Test item, no discount",
                "vendor_cost": 15.00,
                "pricing": {
                    "unit_cost": 15.00,
                    "markup_pct": 67,   # 15 * 1.67 ≈ 25 → profit (25-15)*3 = 30
                },
            }],
        }
        _write_pc_to_store(temp_data_dir, pc)
        resp = auth_client.get(f"/pricecheck/{pc_id}")
        body = resp.get_data(as_text=True)
        assert "profit-pill-disc" not in body
        # MSRP-only single span; (25.05 - 15) * 3 = ~30
        assert "$30." in body
