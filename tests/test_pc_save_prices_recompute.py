"""Regression guard: /pricecheck/<pcid>/save-prices must recompute unit_price
when cost OR markup changes — not just when price changes directly.

Live incident 2026-04-21 (pc_f7ba7a6b, Cortech mattress, qty 16):
  - Operator edited cost to $465.40 with 22% markup.
  - UI rendered $567.79 (cost × 1.22, computed on the fly).
  - Email preview + PDF rendered **$558.48** — the STALE persisted unit_price
    from when the row was first auto-priced.
  - 16 × $558.48 = $8,935.68 shipped to customer.
  - 16 × $567.79 = $9,084.64 was what the operator saw.

Root cause in routes_pricecheck.py _do_save_prices:
  - `price` branch writes `unit_price` AND `pricing.recommended_price` ✓
  - `cost` branch writes `pricing.unit_cost` + `vendor_cost` — NEVER touches
    `unit_price` ✗
  - `markup` branch writes `pricing.markup_pct` + `markup_pct` — NEVER touches
    `unit_price` ✗

_build_item_summary + PDF writers read `unit_price` first (falling back to
`pricing.recommended_price`), so they ship the stale value.

Fix: _compute_unit_price(item) helper called at end of cost AND markup
branches, writing both unit_price AND pricing.recommended_price.
"""
from __future__ import annotations

import json
import os


def _seed_pc(temp_data_dir: str, pcid: str, *, vendor_cost: float,
             markup_pct: float, unit_price: float, qty: int = 16) -> None:
    """Write a single-line PC to price_checks.json."""
    pc = {
        "id": pcid,
        "pc_number": "TEST-RECOMPUTE",
        "institution": "CCHCS",
        "ship_to": "CCHCS, Elk Grove, CA",
        "status": "priced",
        "tax_enabled": False,
        "tax_rate": 0.0,
        "price_buffer": 0,
        "default_markup": 22,
        "parsed": {"header": {"institution": "CCHCS"}, "line_items": []},
        "items": [
            {
                "item_number": "1",
                "qty": qty,
                "uom": "EA",
                "description": "Cortech USA mattress C453075P",
                "no_bid": False,
                "vendor_cost": vendor_cost,
                "unit_price": unit_price,
                "markup_pct": markup_pct,
                "pricing": {
                    "unit_cost": vendor_cost,
                    "markup_pct": markup_pct,
                    "recommended_price": unit_price,
                    "price_source": "manual",
                },
            }
        ],
    }
    pc["parsed"]["line_items"] = pc["items"]
    path = os.path.join(temp_data_dir, "price_checks.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({pcid: pc}, f)


def _load_item0(pcid: str) -> dict:
    """Read the first item back through the authoritative load path."""
    from src.api.data_layer import _load_price_checks
    pcs = _load_price_checks()
    assert pcid in pcs, f"PC {pcid} missing from storage after save"
    return pcs[pcid]["items"][0]


class TestCostEditRecomputesUnitPrice:
    def test_cost_change_updates_persisted_unit_price(self, client, temp_data_dir):
        """Editing cost must re-derive unit_price via cost × (1+markup).
        Before fix: persisted unit_price stays at $567.79 even though UI shows
        the new derived value. Emails/PDFs ship stale price."""
        pcid = "test-pc-recompute-cost"
        _seed_pc(temp_data_dir, pcid,
                 vendor_cost=465.40, markup_pct=22, unit_price=567.79)

        # Operator changes cost from $465.40 → $500.00. Markup stays 22%.
        resp = client.post(
            f"/pricecheck/{pcid}/save-prices",
            json={"cost_0": 500.00},
            content_type="application/json",
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)
        assert resp.get_json()["ok"] is True

        item = _load_item0(pcid)

        # Expected: $500.00 × 1.22 = $610.00
        assert item["pricing"]["unit_cost"] == 500.00
        assert item["unit_price"] == 610.00, (
            f"unit_price should be re-derived (cost × 1+markup = $610.00), "
            f"but stayed at ${item['unit_price']}. Emails + PDFs ship stale "
            f"price to customer."
        )
        assert item["pricing"]["recommended_price"] == 610.00, (
            f"pricing.recommended_price must stay in sync with unit_price; "
            f"got ${item['pricing']['recommended_price']}"
        )

    def test_cost_change_updates_profit_using_new_price(self, client, temp_data_dir):
        """Profit recalc must use the NEW unit_price, not the stale one."""
        pcid = "test-pc-recompute-profit"
        _seed_pc(temp_data_dir, pcid,
                 vendor_cost=465.40, markup_pct=22, unit_price=567.79)

        resp = client.post(
            f"/pricecheck/{pcid}/save-prices",
            json={"cost_0": 500.00},
            content_type="application/json",
        )
        assert resp.status_code == 200

        item = _load_item0(pcid)

        # Expected: (610 - 500) = 110 profit_unit, × 16 qty = 1760 profit_total
        assert item["profit_unit"] == 110.00, (
            f"profit_unit should be $110.00, got ${item.get('profit_unit')}"
        )
        assert item["profit_total"] == 1760.00, (
            f"profit_total should be $1760.00, got ${item.get('profit_total')}"
        )


class TestMarkupEditRecomputesUnitPrice:
    def test_markup_change_updates_persisted_unit_price(self, client, temp_data_dir):
        """Editing markup must re-derive unit_price via cost × (1+markup)."""
        pcid = "test-pc-recompute-markup"
        _seed_pc(temp_data_dir, pcid,
                 vendor_cost=465.40, markup_pct=22, unit_price=567.79)

        # Cost stays $465.40; markup 22% → 30%.
        resp = client.post(
            f"/pricecheck/{pcid}/save-prices",
            json={"markup_0": 30},
            content_type="application/json",
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)

        item = _load_item0(pcid)

        # Expected: $465.40 × 1.30 = $605.02
        assert item["pricing"]["markup_pct"] == 30
        assert item["unit_price"] == 605.02, (
            f"unit_price should be re-derived to $605.02, got "
            f"${item['unit_price']}. Markup edit not recomputing price."
        )
        assert item["pricing"]["recommended_price"] == 605.02


class TestPriceEditStillWorks:
    """Direct price edits must continue writing unit_price (regression guard
    for the working branch — don't break it while fixing cost/markup)."""

    def test_price_change_updates_persisted_unit_price(self, client, temp_data_dir):
        pcid = "test-pc-recompute-price"
        _seed_pc(temp_data_dir, pcid,
                 vendor_cost=465.40, markup_pct=22, unit_price=567.79)

        resp = client.post(
            f"/pricecheck/{pcid}/save-prices",
            json={"price_0": 700.00},
            content_type="application/json",
        )
        assert resp.status_code == 200

        item = _load_item0(pcid)
        assert item["unit_price"] == 700.00
        assert item["pricing"]["recommended_price"] == 700.00


class TestCortechLiveIncident:
    """Exact reproduction of the 2026-04-21 Cortech mattress incident."""

    def test_live_incident_reproduction(self, client, temp_data_dir):
        """The scenario that shipped wrong: cost=465.40, markup=22.
        UI shows 567.79 (live-derived); persisted unit_price was stale at
        558.48 (original auto-price before markup was set to 22%).

        After fix: save must converge persisted unit_price to 567.79."""
        pcid = "pc_f7ba7a6b_repro"
        # Seed with KNOWN-STALE state: unit_price=558.48 lagging cost/markup.
        _seed_pc(temp_data_dir, pcid,
                 vendor_cost=465.40, markup_pct=22, unit_price=558.48)

        # Operator re-saves cost at the same value (no-op edit) — fix must
        # reconverge persisted price to cost × (1+markup).
        resp = client.post(
            f"/pricecheck/{pcid}/save-prices",
            json={"cost_0": 465.40},
            content_type="application/json",
        )
        assert resp.status_code == 200

        item = _load_item0(pcid)
        # $465.40 × 1.22 = $567.788 → $567.79 rounded.
        assert item["unit_price"] == 567.79, (
            f"Cortech incident guard: persisted unit_price should be "
            f"$567.79 after save (matches UI render), got "
            f"${item['unit_price']}"
        )
