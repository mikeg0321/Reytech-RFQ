"""Mike P0 2026-05-11: supplier-quote upload must be verbatim.

The supplier quote PDF ECHQ1223525 from Echelon Distribution had 5 items
(Penlight, Razor, Shampoo, Fingernail Clippers, Laceration Tray). The RFQ
rfq_8efe9fae had 3 items extracted from email-body regex (the inbound
PDF was image-only and parser fell back). The supplier-quote upload route
matched 3 → 3 and silently dropped the 2 unmatched supplier items
(Shampoo + Laceration Tray) from the RFQ.

Mike: "if I upload a supplier quote I want it to be verbatim."

Fix: route now appends unmatched supplier items as new RFQ lines instead
of dropping them. Lines are tagged `_added_from_supplier_quote=True` so
downstream UI / audit can distinguish supplier-originated lines.

These tests pin the helper that builds the new line.
"""
from __future__ import annotations

from src.api.modules.routes_rfq_gen import (
    _apply_supplier_qty_uom,
    _build_unmatched_supplier_line,
)


def test_unmatched_line_carries_all_supplier_fields():
    """A new line built from a supplier-quote item must preserve
    description, item_number, qty, uom, supplier cost, and the
    supplier name."""
    rfq_items = []
    line = _build_unmatched_supplier_line(
        rfq_items,
        q_desc="Laceration Tray McKesson Sterile LACERATION TRAY CLTH TWL",
        q_pn="326543",
        q_qty=3,
        q_uom="EA",
        cost=15.03,
        supplier="Echelon Distribution",
    )
    assert line["description"].startswith("Laceration Tray")
    assert line["item_number"] == "326543"
    assert line["qty"] == 3
    assert line["uom"] == "EA"
    assert line["supplier_cost"] == 15.03
    assert line["cost_source"] == "Supplier Quote"
    assert line["cost_supplier_name"] == "Echelon Distribution"
    assert line["item_supplier"] == "Echelon Distribution"
    assert line["_desc_source"] == "supplier"
    assert line["_added_from_supplier_quote"] is True


def test_line_number_increments_from_existing_rfq_items():
    """When the RFQ already has N items, the new line's line_number
    must be N+1 so renderers see a contiguous sequence."""
    rfq_items = [
        {"line_number": 1, "description": "Penlight"},
        {"line_number": 2, "description": "Razor"},
        {"line_number": 3, "description": "Fingernail Clippers"},
    ]
    line = _build_unmatched_supplier_line(
        rfq_items, q_desc="Shampoo", q_pn="877018", q_qty=15,
        q_uom="CS", cost=67.58, supplier="Echelon",
    )
    assert line["line_number"] == 4


def test_qty_falls_back_to_1_when_zero_or_missing():
    """Supplier quote sometimes has qty=0 (parser stumble). The line
    must still be created with qty=1 rather than 0, so downstream
    pricing math doesn't divide-by-zero or stamp zero qty."""
    rfq_items = []
    line0 = _build_unmatched_supplier_line(
        rfq_items, q_desc="X", q_pn="Y", q_qty=0,
        q_uom="EA", cost=10.0, supplier="V",
    )
    assert line0["qty"] == 1
    line_none = _build_unmatched_supplier_line(
        rfq_items, q_desc="X", q_pn="Y", q_qty=None,
        q_uom="EA", cost=10.0, supplier="V",
    )
    assert line_none["qty"] == 1


def test_uom_uppercased():
    """UOM must be uppercased to match the rest of the codebase's
    canonical UOM tokens (EA, CS, BX, PK). A supplier may write 'ea'."""
    rfq_items = []
    line = _build_unmatched_supplier_line(
        rfq_items, q_desc="X", q_pn="Y", q_qty=5,
        q_uom="ea", cost=10.0, supplier="V",
    )
    assert line["uom"] == "EA"


def test_uom_defaults_to_EA_when_missing():
    """When the supplier item has no UOM, default to EA (each)."""
    rfq_items = []
    line = _build_unmatched_supplier_line(
        rfq_items, q_desc="X", q_pn="Y", q_qty=5,
        q_uom="", cost=10.0, supplier="V",
    )
    assert line["uom"] == "EA"


def test_supplier_cost_rounded_to_2_decimals():
    """Floating-point cost from vision parser may come in with 4+
    decimals; we display 2 in PDFs and dollars-line UI."""
    rfq_items = []
    line = _build_unmatched_supplier_line(
        rfq_items, q_desc="X", q_pn="Y", q_qty=1,
        q_uom="EA", cost=10.594999999, supplier="V",
    )
    assert line["supplier_cost"] == 10.59


def test_pricing_signal_fields_zeroed_for_supplier_only_lines():
    """Lines added from supplier quote have no SCPRS / Amazon /
    price_per_unit signal (those come from buyer-side enrichment).
    Initialize as 0 so renderers don't NPE on missing keys."""
    rfq_items = []
    line = _build_unmatched_supplier_line(
        rfq_items, q_desc="X", q_pn="Y", q_qty=1,
        q_uom="EA", cost=10.0, supplier="V",
    )
    assert line["scprs_last_price"] == 0
    assert line["amazon_price"] == 0
    assert line["price_per_unit"] == 0


# ─── Echelon ECHQ1223525 regression scenario ─────────────────────────────
#
# Reproduction of the exact case Mike flagged: supplier quote has 5 items,
# RFQ already has 3 (Penlight, Razor, Fingernail Clippers). Matcher
# matches those 3. The 2 unmatched (Shampoo, Laceration Tray) must end
# up as appended lines on the RFQ, not silently dropped.


def test_echelon_5_items_3_matched_2_appended_scenario():
    """End-to-end shape check using a minimal rfq_items + simulated
    matcher output to confirm `rfq_items` grows by 2 (the unmatched
    supplier items) after the route's unmatched-append loop runs."""
    rfq_items = [
        {"line_number": 1, "description": "Penlight White Light Disposable",
         "item_number": "161574"},
        {"line_number": 2, "description": "Personal Razor with Lubricating Strip",
         "item_number": "899539"},
        {"line_number": 3, "description": "Fingernail Clippers McKesson",
         "item_number": "475020"},
    ]
    # The 2 unmatched supplier items from ECHQ1223525:
    unmatched_supplier_items = [
        # Shampoo and Body Wash (line 3 of supplier quote)
        ("Shampoo and Body Wash McKesson 8oz", "877018", 15, "CS", 67.58),
        # Laceration Tray (line 5 of supplier quote)
        ("Laceration Tray McKesson Sterile", "326543", 3, "EA", 15.03),
    ]
    for q_desc, q_pn, q_qty, q_uom, cost in unmatched_supplier_items:
        new_line = _build_unmatched_supplier_line(
            rfq_items, q_desc, q_pn, q_qty, q_uom, cost, "Echelon Distribution",
        )
        rfq_items.append(new_line)

    # RFQ now has all 5 items
    assert len(rfq_items) == 5
    descs = [it["description"] for it in rfq_items]
    assert "Shampoo and Body Wash McKesson 8oz" in descs
    assert "Laceration Tray McKesson Sterile" in descs

    # Sanity: supplier-originated lines are tagged
    appended = [it for it in rfq_items if it.get("_added_from_supplier_quote")]
    assert len(appended) == 2

    # Buyer-originated lines stay untouched (no _added_from_supplier_quote)
    original = [it for it in rfq_items if not it.get("_added_from_supplier_quote")]
    assert len(original) == 3
    for it in original:
        assert "_added_from_supplier_quote" not in it


# ─── _apply_supplier_qty_uom — matched-line overwrite ────────────────────
#
# P0 2026-05-11 RFQ rfq_8efe9fae: supplier-quote upload matched lines but
# never overwrote qty/uom. The buyer-RFQ parser had put McKesson item
# numbers (161574) in the qty column; total ballooned to $1.6M instead
# of $2,627. These tests pin the helper that fixes it.


def test_apply_supplier_qty_uom_overwrites_bogus_qty():
    """The exact ECHQ1223525 incident: buyer RFQ row has qty=161574
    (a McKesson item number), supplier quote says qty=6. After the
    upload's match update, qty must be 6."""
    item = {"qty": 161574, "uom": "EA", "description": "Penlight"}
    _apply_supplier_qty_uom(item, q_qty=6, q_uom="PK")
    assert item["qty"] == 6
    assert item["uom"] == "PK"
    # Audit trail preserved
    assert item["_prior_qty_before_supplier"] == 161574
    assert item["_prior_uom_before_supplier"] == "EA"


def test_apply_supplier_qty_uom_records_no_audit_when_already_aligned():
    """No-op case: qty and uom already match the supplier. No audit
    fields written (avoids polluting the dict on idempotent re-runs)."""
    item = {"qty": 6, "uom": "PK", "description": "Penlight"}
    _apply_supplier_qty_uom(item, q_qty=6, q_uom="PK")
    assert item["qty"] == 6
    assert item["uom"] == "PK"
    assert "_prior_qty_before_supplier" not in item
    assert "_prior_uom_before_supplier" not in item


def test_apply_supplier_qty_uom_uppercases_uom():
    """Supplier may pass UOM as 'pk' or 'cs' — must normalize."""
    item = {"qty": 1, "uom": "EA"}
    _apply_supplier_qty_uom(item, q_qty=4, q_uom="cs")
    assert item["uom"] == "CS"


def test_apply_supplier_qty_uom_skips_qty_when_supplier_qty_zero_or_missing():
    """A supplier qty of 0 or None is a parser stumble — don't overwrite
    a real RFQ qty with zero/null. Mike's actual qty is preserved."""
    for bad in (0, None, "", "abc"):
        item = {"qty": 12, "uom": "EA"}
        _apply_supplier_qty_uom(item, q_qty=bad, q_uom="EA")
        assert item["qty"] == 12, f"qty should be preserved when supplier qty={bad!r}"


def test_apply_supplier_qty_uom_skips_uom_when_supplier_uom_empty():
    """Empty/None supplier UOM keeps the existing UOM intact."""
    item = {"qty": 1, "uom": "BX"}
    _apply_supplier_qty_uom(item, q_qty=5, q_uom="")
    assert item["uom"] == "BX"
    _apply_supplier_qty_uom(item, q_qty=5, q_uom=None)
    assert item["uom"] == "BX"


def test_apply_supplier_qty_uom_coerces_float_string_qty():
    """Some parsers emit qty as '6.0' or '6' strings — must coerce to int."""
    item = {"qty": 100}
    _apply_supplier_qty_uom(item, q_qty="6.0", q_uom="PK")
    assert item["qty"] == 6
    assert isinstance(item["qty"], int)


def test_apply_supplier_qty_uom_returns_same_item_for_chaining():
    """Helper mutates in place AND returns the dict so callers can chain."""
    item = {"qty": 99, "uom": "EA"}
    out = _apply_supplier_qty_uom(item, q_qty=4, q_uom="CS")
    assert out is item


def test_echelon_rfq_8efe9fae_three_row_overwrite_scenario():
    """End-to-end the specific incident: 3 buyer-RFQ rows with bogus
    qtys (item-number-as-qty pattern), supplier-quote upload runs the
    overwrite, all 3 rows now carry the supplier's qty/uom."""
    rfq_items = [
        # Row 1: Penlight — qty stuffed with McKesson item# 161574
        {"line_number": 1, "qty": 161574, "uom": "EA",
         "description": "Penlight", "item_number": "161574"},
        # Row 2: Fingernail Clippers — qty stuffed with another bogus number
        {"line_number": 2, "qty": 45, "uom": "EA",
         "description": "Fingernail Clippers", "item_number": "475020"},
        # Row 3: Personal Razor — qty stuffed with item-number-like 843
        {"line_number": 3, "qty": 843, "uom": "EA",
         "description": "Personal Razor", "item_number": "899539"},
    ]
    supplier_match_qtys = [(6, "PK"), (4, "CS"), (4, "CS")]
    for item, (q, u) in zip(rfq_items, supplier_match_qtys):
        _apply_supplier_qty_uom(item, q, u)

    # Per-row math now matches the supplier quote (cost × qty):
    # Penlight  $10.59 × 6  = $63.54
    # Razor     $215.81 × 4 = $863.24
    # Fingernail $160.51 × 4 = $642.04
    qtys = [it["qty"] for it in rfq_items]
    uoms = [it["uom"] for it in rfq_items]
    assert qtys == [6, 4, 4]
    assert uoms == ["PK", "CS", "CS"]
    # Audit trail captures the pre-overwrite values
    assert rfq_items[0]["_prior_qty_before_supplier"] == 161574
    assert rfq_items[2]["_prior_qty_before_supplier"] == 843
