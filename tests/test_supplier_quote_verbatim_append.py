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

from src.api.modules.routes_rfq_gen import _build_unmatched_supplier_line


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
