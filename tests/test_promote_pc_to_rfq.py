"""Tests for `promote_pc_to_rfq_in_place` — operator-confirmed PC→RFQ promote.

Business rule (Mike, 2026-04-20): "do not re-price unless QTY changes — PCs
are banking on the price submitted which allows the RFQ to go out to public
bidding." The promote step must port PC prices VERBATIM onto the RFQ and
flag qty-changed lines for a separate re-price pass; it must never recompute
the price itself.

Covers:
  - Verbatim price port (PC price wins even if RFQ has a stale/different price)
  - qty_changed flag fires only when qty differs
  - Context fields (supplier, URL, MFG#) fill gaps without clobbering
  - No-match lines are counted separately, not silently left blank
  - Double-claim guard: one PC line can only price one RFQ line
  - Linked-PC metadata set on the RFQ for audit
"""
from __future__ import annotations

from src.core.pc_rfq_linker import promote_pc_to_rfq_in_place


# ── Verbatim price preservation ──────────────────────────────────────────────

def test_pc_prices_port_verbatim_to_matched_rfq_line():
    rfq_data = {
        "line_items": [
            {"mfg_number": "W12919", "description": "BP cuff",
             "quantity": 5, "unit_price": 99.99, "supplier_cost": 88.88},
        ],
    }
    pc_data = {
        "agency": "CCHCS",
        "items": [
            {"mfg_number": "W12919", "description": "BP cuff adult",
             "quantity": 5, "unit_price": 45.00, "supplier_cost": 25.00,
             "bid_price": 45.00, "markup_pct": 80},
        ],
    }
    out = promote_pc_to_rfq_in_place(rfq_data, "pc_123", pc_data)
    assert out == {"promoted": 1, "qty_changed": 0, "no_match": 0}

    line = rfq_data["line_items"][0]
    # Verbatim: PC values overwrite the RFQ's stale ones
    assert line["unit_price"] == 45.00
    assert line["supplier_cost"] == 25.00
    assert line["bid_price"] == 45.00
    assert line["markup_pct"] == 80
    assert line["source_pc"] == "pc_123"
    assert line["promoted_from_pc"] is True
    assert line["qty_changed"] is False


def test_qty_unchanged_does_not_flag_for_reprice():
    rfq = {"line_items": [{"mfg_number": "A1", "quantity": 10}]}
    pc = {"agency": "CCHCS", "items": [
        {"mfg_number": "A1", "quantity": 10, "unit_price": 5.00, "bid_price": 7.50},
    ]}
    promote_pc_to_rfq_in_place(rfq, "pc", pc)
    line = rfq["line_items"][0]
    assert line["qty_changed"] is False
    assert line["pc_original_qty"] == 10
    # Price preserved
    assert line["unit_price"] == 5.00


def test_qty_changed_flags_line_for_reprice():
    """Mike's rule: re-pricing ONLY happens when qty changes. The flag must
    fire so the next stage (PR #3) can target this line and skip the rest."""
    rfq = {"line_items": [{"mfg_number": "A1", "quantity": 20}]}  # buyer doubled qty
    pc = {"agency": "CCHCS", "items": [
        {"mfg_number": "A1", "quantity": 10, "unit_price": 5.00, "bid_price": 7.50},
    ]}
    out = promote_pc_to_rfq_in_place(rfq, "pc", pc)
    line = rfq["line_items"][0]
    assert out["qty_changed"] == 1
    assert line["qty_changed"] is True
    assert line["pc_original_qty"] == 10
    # Price still ported verbatim; PR #3 will decide whether to refresh it.
    assert line["unit_price"] == 5.00


# ── Context fields (supplier, URL, identifiers) ──────────────────────────────

def test_context_fields_fill_gaps_without_clobbering_rfq_values():
    """Supplier / URL / MFG# should be filled if the RFQ is missing them,
    but must NOT overwrite a value the buyer already supplied."""
    rfq = {"line_items": [
        {"description": "thing", "quantity": 1,
         "item_supplier": "BuyerSuggestedSupplier"},
    ]}
    pc = {"agency": "CCHCS", "items": [
        {"description": "thing", "quantity": 1,
         "item_supplier": "Reytech's Choice",
         "item_link": "https://example.com/part",
         "mfg_number": "M-1"},
    ]}
    promote_pc_to_rfq_in_place(rfq, "pc", pc)
    line = rfq["line_items"][0]
    # Not overwritten
    assert line["item_supplier"] == "BuyerSuggestedSupplier"
    # Filled from PC
    assert line["item_link"] == "https://example.com/part"
    assert line["mfg_number"] == "M-1"


# ── No-match handling ────────────────────────────────────────────────────────

def test_unmatched_rfq_line_counted_and_left_unpriced():
    """If a buyer adds a completely new line the PC never had, we count it
    as no_match. We must NOT silently borrow another line's price."""
    rfq = {"line_items": [
        {"mfg_number": "A1", "description": "known item", "quantity": 1},
        {"description": "brand new item never on PC", "quantity": 2},
    ]}
    pc = {"agency": "CCHCS", "items": [
        {"mfg_number": "A1", "description": "known item", "quantity": 1,
         "unit_price": 10.00},
    ]}
    out = promote_pc_to_rfq_in_place(rfq, "pc", pc)
    # Positional fallback kicks in on the 2nd line only if the PC has a
    # corresponding positional slot — but pos=1 on PC doesn't exist here.
    assert out["promoted"] == 1
    assert out["no_match"] == 1
    assert rfq["line_items"][0].get("unit_price") == 10.00
    assert "unit_price" not in rfq["line_items"][1]


def test_one_pc_line_cannot_price_two_rfq_lines():
    """Double-claim guard — otherwise buyer duplicating a line would let us
    silently re-use a PC price for both, distorting the bid total."""
    rfq = {"line_items": [
        {"description": "widget alpha bravo charlie delta", "quantity": 1},
        {"description": "widget alpha bravo charlie delta", "quantity": 1},
    ]}
    pc = {"agency": "CCHCS", "items": [
        {"description": "widget alpha bravo charlie delta",
         "quantity": 1, "unit_price": 10.00},
    ]}
    out = promote_pc_to_rfq_in_place(rfq, "pc", pc)
    # Only one RFQ line gets priced; the other is no_match or positional.
    # Both landing with the same price would mean the guard failed.
    priced = [l for l in rfq["line_items"] if l.get("unit_price") is not None]
    assert len(priced) == 1


# ── Empty RFQ — pure carry-over ──────────────────────────────────────────────

def test_empty_rfq_gets_pc_items_copied_verbatim():
    """Manual-conversion path: operator starts a blank RFQ from a PC.
    All items come from the PC with full pricing preserved."""
    rfq = {"line_items": []}
    pc = {"agency": "CCHCS", "items": [
        {"mfg_number": "A1", "description": "one", "quantity": 3,
         "unit_price": 12.0},
        {"mfg_number": "B2", "description": "two", "quantity": 1,
         "unit_price": 20.0},
    ]}
    out = promote_pc_to_rfq_in_place(rfq, "pc_xyz", pc)
    assert out["promoted"] == 2
    assert len(rfq["line_items"]) == 2
    assert rfq["line_items"][0]["unit_price"] == 12.0
    assert rfq["line_items"][1]["unit_price"] == 20.0
    for line in rfq["line_items"]:
        assert line["source_pc"] == "pc_xyz"
        assert line["promoted_from_pc"] is True
        assert line["qty_changed"] is False


# ── Link metadata ────────────────────────────────────────────────────────────

def test_rfq_receives_pc_link_metadata_for_audit():
    rfq = {"line_items": [{"mfg_number": "A1", "quantity": 1}]}
    pc = {"agency": "CCHCS", "pc_number": "PC-2026-001",
          "bundle_id": "BUNDLE-7",
          "items": [{"mfg_number": "A1", "quantity": 1, "unit_price": 5.0}]}
    promote_pc_to_rfq_in_place(rfq, "pc_abc", pc)
    assert rfq["linked_pc_id"] == "pc_abc"
    assert rfq["linked_pc_number"] == "PC-2026-001"
    assert rfq["linked_pc_match_reason"] == "operator_confirmed"
    assert rfq["promoted_from_pc"] is True
    assert rfq["bundle_id"] == "BUNDLE-7"


def test_pc_data_wrapped_in_pc_data_key_unwraps():
    """Queue-loader shape: pc comes as {"pc_data": {...inner...}}."""
    rfq = {"line_items": [{"mfg_number": "A1", "quantity": 1}]}
    pc = {"agency": "CCHCS", "pc_data": {
        "pc_number": "PC-999",
        "items": [{"mfg_number": "A1", "quantity": 1, "unit_price": 9.99}],
    }}
    out = promote_pc_to_rfq_in_place(rfq, "pc", pc)
    assert out["promoted"] == 1
    assert rfq["line_items"][0]["unit_price"] == 9.99
    assert rfq["linked_pc_number"] == "PC-999"


def test_empty_pc_items_returns_zero_counts():
    rfq = {"line_items": [{"description": "x", "quantity": 1}]}
    pc = {"agency": "CCHCS", "items": []}
    out = promote_pc_to_rfq_in_place(rfq, "pc", pc)
    assert out == {"promoted": 0, "qty_changed": 0, "no_match": 0}
