"""Mike P0 (2026-05-05): re-parse on buyer add-an-item email wipes pricing.

Repro from prod: PC pc_177b18e6 had Mike-priced unit_cost=$8.00, unit_price=$16.00
across 22 hours (revs 11-20). When buyer Valentina re-emailed asking to add 1
item, re-parse fired and rev 21 showed both items with unit_cost=None,
unit_price=None — pricing wiped.

Root cause: `_update_existing_record` at src/core/ingest_pipeline.py:897
destructively overwrites `pc["items"] = items` with no merge. Same for RFQ
at line 915.

Fix: merge new items into old, preserving operator-set fields
(pricing/vendor_cost/unit_price/markup_pct/item_link/notes/no_bid/
catalog_match) for items that match by MFG#, description token-Jaccard ≥ 0.85,
or row_index. New items not matching any existing are appended; existing items
not matched in the new parse are KEPT (don't delete on buyer-email re-parse —
operator must explicitly trim).
"""
from __future__ import annotations

import pytest


# ── Pure unit tests on the merger helper ─────────────────────────────


def test_match_old_to_new_by_mfg_number_case_insensitive():
    from src.core.ingest_pipeline import _match_old_to_new
    old_items = [
        {"mfg_number": "5CAIS1G9WZZC", "description": "Heel Donut Cushions"},
        {"mfg_number": "B000NQ4UGM", "description": "Different item"},
    ]
    new = {"mfg_number": "5cais1g9wzzc", "description": "anything"}
    matched = _match_old_to_new(old_items, new)
    assert matched is not None
    assert matched["mfg_number"] == "5CAIS1G9WZZC"


def test_match_old_to_new_by_description_token_similarity():
    from src.core.ingest_pipeline import _match_old_to_new
    old_items = [
        {"description": "Heel Donut Cushions, Heel Cups, Silicon Insoles, One Size Fits All - 1 Pair"},
    ]
    # Same item, slightly rephrased — token Jaccard well above threshold
    new = {"description": "Heel Donut Cushions Heel Cups Silicon Insoles One Size Fits All 1 Pair"}
    matched = _match_old_to_new(old_items, new)
    assert matched is not None


def test_match_old_to_new_returns_none_for_unrelated_item():
    from src.core.ingest_pipeline import _match_old_to_new
    old_items = [
        {"mfg_number": "5CAIS1G9WZZC", "description": "Heel Donut Cushions"},
    ]
    # Totally different item — buyer's new add
    new = {"mfg_number": "B000NQ4UGM", "description": "Rips-24 Adjustable Knee Brace"}
    assert _match_old_to_new(old_items, new) is None


def test_merge_preserves_pricing_when_buyer_adds_new_item():
    """Headline test: Mike's pc_177b18e6 scenario.

    Pre-fix: items[0] (Heel Donut, $8 cost) gets clobbered to None when
    buyer adds RIPS-24 item via email re-parse.
    """
    from src.core.ingest_pipeline import _merge_items_preserving_pricing
    old_items = [
        {
            "description": "Heel Donut Cushions, Heel Cups, Silicon Insoles, One Size Fits All - 1 Pair",
            "mfg_number": "5CAIS1G9WZZC",
            "qty": 10,
            "uom": "PR",
            "row_index": 1,
            "pricing": {"unit_cost": 8.0, "markup_pct": 35.0, "recommended_price": 16.0},
            "vendor_cost": 8.0,
            "unit_price": 16.0,
            "markup_pct": 35.0,
            "item_link": "https://www.amazon.com/Heel-Donut-Cushions-Silicon-Insoles/dp/B08TVK1JQS",
            "item_supplier": "Amazon",
            "notes": "operator-set notes",
        },
    ]
    new_items = [
        # Same item from the original (buyer left it in the body)
        {
            "description": "Heel Donut Cushions, Heel Cups, Silicon Insoles, One Size Fits All - 1 Pair",
            "mfg_number": "5CAIS1G9WZZC",
            "qty": 10,
            "uom": "PR",
            "row_index": 1,
            "pricing": {},  # fresh parse — no pricing yet
            "vendor_cost": None,
            "unit_price": None,
        },
        # New item — buyer added
        {
            "description": "Rips-24 Adjustable Knee Brace",
            "mfg_number": "B000NQ4UGM",
            "qty": 1,
            "uom": "EA",
            "row_index": 2,
            "pricing": {},
        },
    ]

    merged = _merge_items_preserving_pricing(old_items, new_items)

    assert len(merged) == 2

    # Item 0: original Heel Donut — pricing FULLY PRESERVED
    h = merged[0]
    assert h["mfg_number"] == "5CAIS1G9WZZC"
    assert h["pricing"]["unit_cost"] == 8.0, (
        f"Heel Donut unit_cost wiped: {h['pricing']}. This is the exact "
        f"pc_177b18e6 prod regression — operator-set $8 cost must survive "
        f"buyer add-an-item re-parse."
    )
    assert h["unit_price"] == 16.0
    assert h["vendor_cost"] == 8.0
    assert h["markup_pct"] == 35.0
    assert h["item_link"].startswith("https://www.amazon.com/Heel-Donut")
    assert h["item_supplier"] == "Amazon"
    assert h["notes"] == "operator-set notes"

    # Item 1: new RIPS-24 — fresh, no spurious pricing copied
    r = merged[1]
    assert r["mfg_number"] == "B000NQ4UGM"
    assert r["description"].startswith("Rips-24")


def test_merge_keeps_old_items_not_in_new_parse():
    """Buyer's "please add 1 item" email may not restate the original.
    The new parse may contain ONLY the new item. Original must persist —
    don't delete on buyer-email re-parse.
    """
    from src.core.ingest_pipeline import _merge_items_preserving_pricing
    old_items = [
        {
            "description": "Heel Donut Cushions",
            "mfg_number": "5CAIS1G9WZZC",
            "qty": 10,
            "pricing": {"unit_cost": 8.0},
            "unit_price": 16.0,
        },
    ]
    # Buyer's reply only mentions the new item
    new_items = [
        {
            "description": "Rips-24 Knee Brace",
            "mfg_number": "B000NQ4UGM",
            "qty": 1,
        },
    ]

    merged = _merge_items_preserving_pricing(old_items, new_items)

    # Both must survive
    assert len(merged) == 2
    descs = [m["description"] for m in merged]
    assert any("Heel Donut" in d for d in descs), "Original Heel Donut deleted"
    assert any("Rips-24" in d for d in descs), "New RIPS-24 not added"

    # Original's pricing intact
    heel = next(m for m in merged if "Heel Donut" in m["description"])
    assert heel["unit_price"] == 16.0
    assert heel["pricing"]["unit_cost"] == 8.0


def test_merge_qty_change_keeps_pricing():
    """Buyer says "change qty 10 → 20". New parse has qty=20, no pricing.
    Old qty=10 had cost=$8. After merge: qty=20 (buyer's update wins),
    cost=$8 (operator pricing preserved)."""
    from src.core.ingest_pipeline import _merge_items_preserving_pricing
    old_items = [
        {
            "description": "Heel Donut Cushions",
            "mfg_number": "5CAIS1G9WZZC",
            "qty": 10,
            "pricing": {"unit_cost": 8.0},
            "unit_price": 16.0,
            "vendor_cost": 8.0,
        },
    ]
    new_items = [
        {
            "description": "Heel Donut Cushions",
            "mfg_number": "5CAIS1G9WZZC",
            "qty": 20,  # buyer increased
        },
    ]

    merged = _merge_items_preserving_pricing(old_items, new_items)

    assert len(merged) == 1
    h = merged[0]
    assert h["qty"] == 20, "Buyer's qty update should win"
    assert h["pricing"]["unit_cost"] == 8.0, "Operator's cost should survive"
    assert h["unit_price"] == 16.0
    assert h["vendor_cost"] == 8.0


def test_merge_does_not_overwrite_new_description():
    """If new parse has a slightly tweaked description (buyer fixed a typo),
    the new description wins — only the OPERATOR-SET fields (pricing) come
    from old."""
    from src.core.ingest_pipeline import _merge_items_preserving_pricing
    old_items = [
        {
            "description": "Heel Donut Cushion",
            "mfg_number": "5CAIS1G9WZZC",
            "pricing": {"unit_cost": 8.0},
        },
    ]
    new_items = [
        {
            "description": "Heel Donut Cushions, Heel Cups",  # corrected
            "mfg_number": "5CAIS1G9WZZC",
        },
    ]
    merged = _merge_items_preserving_pricing(old_items, new_items)
    assert len(merged) == 1
    assert merged[0]["description"] == "Heel Donut Cushions, Heel Cups"
    assert merged[0]["pricing"]["unit_cost"] == 8.0


def test_merge_skips_old_pricing_when_value_is_none():
    """Old item with no pricing data (None / missing) shouldn't blow up
    the merge — new item just keeps its own (empty) values."""
    from src.core.ingest_pipeline import _merge_items_preserving_pricing
    old_items = [
        {"description": "Item A", "mfg_number": "X1", "pricing": None},
    ]
    new_items = [
        {"description": "Item A", "mfg_number": "X1"},
    ]
    merged = _merge_items_preserving_pricing(old_items, new_items)
    assert len(merged) == 1
    assert merged[0]["mfg_number"] == "X1"
    # No crash; pricing field is whatever new had (or absent)


def test_merge_one_old_to_many_new_only_matches_first():
    """If two new items both look like the same old item (rare — duplicate
    parse), only the first match should claim the old item's pricing.
    Otherwise we'd double-count operator pricing."""
    from src.core.ingest_pipeline import _merge_items_preserving_pricing
    old_items = [
        {
            "description": "Heel Donut",
            "mfg_number": "X1",
            "pricing": {"unit_cost": 8.0},
            "unit_price": 16.0,
        },
    ]
    new_items = [
        {"description": "Heel Donut", "mfg_number": "X1", "qty": 10},
        {"description": "Heel Donut", "mfg_number": "X1", "qty": 5},
    ]
    merged = _merge_items_preserving_pricing(old_items, new_items)
    assert len(merged) == 2
    # First new item gets the pricing
    assert merged[0]["pricing"]["unit_cost"] == 8.0
    assert merged[0]["unit_price"] == 16.0
    # Second new item does NOT also claim the same pricing (would be double)
    assert "pricing" not in merged[1] or not merged[1].get("pricing", {}).get("unit_cost")


# ── Integration test through _update_existing_record ─────────────────


def test_update_existing_pc_uses_merge_not_overwrite(temp_data_dir, monkeypatch):
    """End-to-end: _update_existing_record on a PC with operator pricing
    must preserve that pricing across re-parse with new items."""
    from src.api.dashboard import _save_single_pc, _load_price_checks
    from src.core.ingest_pipeline import _update_existing_record
    from dataclasses import dataclass

    @dataclass
    class FakeClassification:
        def to_dict(self): return {"shape": "pc", "agency": "calvet"}

    pc_id = "pc_test_repro_177b18e6"
    initial_pc = {
        "items": [
            {
                "description": "Heel Donut Cushions",
                "mfg_number": "5CAIS1G9WZZC",
                "qty": 10,
                "pricing": {"unit_cost": 8.0, "markup_pct": 35.0, "recommended_price": 16.0},
                "unit_price": 16.0,
                "vendor_cost": 8.0,
                "markup_pct": 35.0,
                "item_link": "https://www.amazon.com/Heel-Donut/dp/B08TVK1JQS",
                "item_supplier": "Amazon",
                "notes": "operator note",
            },
        ],
        "status": "sent",
    }
    _save_single_pc(pc_id, initial_pc)

    # Simulate buyer's re-emailed parse: same item + new item
    new_items = [
        {
            "description": "Heel Donut Cushions",
            "mfg_number": "5CAIS1G9WZZC",
            "qty": 10,
            "pricing": {},  # fresh parse, no pricing
        },
        {
            "description": "Rips-24 Knee Brace",
            "mfg_number": "B000NQ4UGM",
            "qty": 1,
        },
    ]

    _update_existing_record(
        pc_id, "pc",
        new_items,
        header={},
        classification=FakeClassification(),
        primary_path=None,
    )

    pc = _load_price_checks().get(pc_id)
    assert pc is not None
    items = pc.get("items", [])
    assert len(items) == 2

    # Heel Donut: ALL operator-set fields preserved
    heel = next(it for it in items if "Heel Donut" in it["description"])
    assert heel["pricing"]["unit_cost"] == 8.0
    assert heel["unit_price"] == 16.0
    assert heel["vendor_cost"] == 8.0
    assert heel["markup_pct"] == 35.0
    assert heel["item_link"].startswith("https://www.amazon.com/Heel-Donut")
    assert heel["item_supplier"] == "Amazon"
    assert heel["notes"] == "operator note"

    # New RIPS-24 present
    rips = next(it for it in items if "Rips-24" in it["description"])
    assert rips["mfg_number"] == "B000NQ4UGM"
