"""EXACT badge is reserved for UPC/identifier-verified matches (conf >= 0.99).

Before this guardrail: any source with confidence > 0.95 rendered the green
EXACT badge — so a 96% description-match on the catalog would show "EXACT"
even though no identity (UPC, ASIN, MFG#) had been verified.

That's a user-trust bug. The operator reads EXACT and skips the row; the row
is actually a fuzzy description match that should have been glanced at.

Fix: raise the EXACT floor to 0.99 — that matches the PRD's ladder where
only UPC/barcode lookups hit 0.99, and where item-link and explicit
UPC-confirmed chips already live. STRONG matches (0.75-0.98) render as the
normal chip (no badge). FUZZY stays at 0.50-0.75 with the ~FUZZY tag.

We assert on the rendered HTML from `/pricecheck/<id>` because that is the
single source of truth for chip rendering (`routes_pricecheck.pc_detail`).
"""
from __future__ import annotations

import json
import os

import pytest


def _write_pc(temp_data_dir, pc):
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


def _pc_with_two_sources(pc_id: str) -> dict:
    """One item. Two competing sources:
      - catalog at 0.97 confidence (STRONG description match — NO EXACT)
      - item_link at 0.99 confidence (user-pasted URL — EXACT)
    Both render on the same row, so we can assert the badge fires once.
    """
    return {
        "id": pc_id,
        "pc_id": pc_id,
        "agency": "CCHCS",
        "status": "draft",
        "items": [{
            "line_num": 1,
            "mfg_number": "TEST-EXACT-001",
            "qty": 1,
            "uom": "EA",
            "description": "EXACT badge guard item",
            "vendor_cost": 10.00,
            # User-pasted item link → hardcoded 0.99 conf → must render EXACT
            "item_link": "https://supplier.example.com/product/123",
            "item_link_price": 9.95,
            "item_supplier": "SupplierX",
            "pricing": {
                "unit_cost": 10.00,
                "markup_pct": 30,
                # Catalog chip at 0.97 — STRONG, not EXACT (new rule).
                # Before the fix this would have rendered the EXACT badge.
                "catalog_cost": 8.50,
                "catalog_match": "strong description match",
                "catalog_confidence": 0.97,
                "catalog_best_supplier": "CatalogSup",
            },
        }],
    }


@pytest.mark.usefixtures("auth_client", "temp_data_dir")
class TestExactBadgeReservedForIdentifierMatch:
    def test_description_match_at_0_97_does_not_render_exact(
            self, auth_client, temp_data_dir):
        pc = _pc_with_two_sources("test_exact_gate_pc")
        _write_pc(temp_data_dir, pc)
        resp = auth_client.get(f"/pricecheck/{pc['id']}")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)

        # Exactly one EXACT tag on the row — the item-link chip at 0.99.
        # The catalog chip at 0.97 (STRONG) must not render the EXACT <b>.
        #
        # We count the bold-EXACT sentinel rather than the substring "EXACT"
        # because the word also appears in the route code comments and any
        # escaped alt-text. The <b ...>EXACT</b> block is the actual badge.
        badge_count = body.count(">EXACT</b>")
        assert badge_count == 1, (
            f"expected exactly 1 EXACT badge (from the 0.99 item-link chip); "
            f"got {badge_count}. If >1, the 0.97 catalog chip is bleeding "
            f"into EXACT — check the threshold in routes_pricecheck.py."
        )

    def test_source_0_99_still_renders_exact(
            self, auth_client, temp_data_dir):
        """Sanity: we didn't accidentally remove EXACT for real 0.99 matches."""
        pc = _pc_with_two_sources("test_exact_still_works_pc")
        _write_pc(temp_data_dir, pc)
        resp = auth_client.get(f"/pricecheck/{pc['id']}")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert ">EXACT</b>" in body, (
            "item-link chip at 0.99 confidence must still render EXACT; "
            "the badge appears to have been disabled entirely."
        )
