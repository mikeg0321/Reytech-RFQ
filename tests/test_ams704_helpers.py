"""Unit tests for AMS 704 shared helpers.

Tests LineItem, FillStrategy, normalize_line_item, split_description,
and compute_line_totals. Pure unit tests — no Flask, no DB, no PDF I/O.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.forms.ams704_helpers import (
    LineItem,
    FillStrategy,
    normalize_line_item,
    split_description,
    compute_line_totals,
    TotalsResult,
    enrich_pc_description,
    build_pc_substitute_text,
)


# ═══════════════════════════════════════════════════════════════════════════
# LineItem.from_dict()
# ═══════════════════════════════════════════════════════════════════════════

class TestLineItemFromDict:
    """Test LineItem.from_dict() field alias normalization."""

    def test_basic_pc_format(self):
        raw = {
            "qty": 10,
            "uom": "EA",
            "description": "Nitrile gloves, large",
            "unit_price": 8.49,
            "supplier_cost": 6.50,
        }
        item = LineItem.from_dict(raw)
        assert item.qty == 10
        assert item.uom == "EA"
        assert item.description == "Nitrile gloves, large"
        assert item.unit_price == 8.49
        assert item.supplier_cost == 6.50

    def test_rfq_format_aliases(self):
        """RFQ data uses price_per_unit, item_number, quantity."""
        raw = {
            "quantity": 5,
            "UOM": "BOX",
            "description": "Copy paper",
            "price_per_unit": 42.99,
            "cost": 35.00,
            "item_number": "6500-001-430",
        }
        item = LineItem.from_dict(raw)
        assert item.qty == 5
        assert item.uom == "BOX"
        assert item.unit_price == 42.99
        assert item.supplier_cost == 35.00
        assert item.part_number == "6500-001-430"

    def test_dollar_signs_stripped(self):
        raw = {"unit_price": "$12.58", "supplier_cost": "$10.00", "qty": 1}
        item = LineItem.from_dict(raw)
        assert item.unit_price == 12.58
        assert item.supplier_cost == 10.00

    def test_comma_in_numbers(self):
        raw = {"unit_price": "1,234.56", "qty": "1,000"}
        item = LineItem.from_dict(raw)
        assert item.unit_price == 1234.56
        assert item.qty == 1000

    def test_missing_values_default_safely(self):
        item = LineItem.from_dict({})
        assert item.qty == 0
        assert item.unit_price == 0
        assert item.supplier_cost == 0
        assert item.uom == "EA"
        assert item.description == ""
        assert item.part_number == ""

    def test_no_bid_flag(self):
        raw = {"no_bid": True, "description": "Unavailable item"}
        item = LineItem.from_dict(raw)
        assert item.no_bid is True

    def test_extension_property(self):
        raw = {"qty": 3, "unit_price": 10.50}
        item = LineItem.from_dict(raw)
        assert item.extension == 31.50

    def test_has_price_property(self):
        assert LineItem.from_dict({"unit_price": 10}).has_price is True
        assert LineItem.from_dict({"unit_price": 0}).has_price is False
        assert LineItem.from_dict({}).has_price is False

    def test_roundtrip_to_dict(self):
        raw = {
            "qty": 5, "uom": "BOX", "description": "Gloves",
            "unit_price": 8.49, "supplier_cost": 6.50,
        }
        item = LineItem.from_dict(raw)
        d = item.to_dict()
        assert d["qty"] == 5
        assert d["unit_price"] == 8.49
        assert d["description"] == "Gloves"

    def test_qty_per_uom_coercion(self):
        raw = {"qty_per_uom": "12"}
        item = LineItem.from_dict(raw)
        assert item.qty_per_uom == 12

        raw2 = {"qty_per_uom": "invalid"}
        item2 = LineItem.from_dict(raw2)
        assert item2.qty_per_uom == 1  # fallback


# ═══════════════════════════════════════════════════════════════════════════
# FillStrategy
# ═══════════════════════════════════════════════════════════════════════════

class TestFillStrategy:

    def test_pc_full_strategy(self):
        s = FillStrategy.PC_FULL
        assert s.writes_descriptions is True
        assert s.writes_qty_uom is True
        assert s.writes_pricing is True
        assert s.writes_vendor_info is True
        assert s.writes_item_numbers is True

    def test_pc_original_strategy(self):
        """PC_ORIGINAL: buyer's descriptions untouched, only pricing + qty."""
        s = FillStrategy.PC_ORIGINAL
        assert s.writes_descriptions is False
        assert s.writes_qty_uom is True
        assert s.writes_pricing is True

    def test_rfq_prefilled_strategy(self):
        """RFQ_PREFILLED: only price per unit + subtotal."""
        s = FillStrategy.RFQ_PREFILLED
        assert s.writes_descriptions is False
        assert s.writes_qty_uom is False
        assert s.writes_pricing is True

    def test_for_pc_docx_source(self):
        assert FillStrategy.for_pc(is_prefilled=False, is_docx_source=True) == FillStrategy.PC_FULL

    def test_for_pc_prefilled(self):
        assert FillStrategy.for_pc(is_prefilled=True) == FillStrategy.PC_ORIGINAL

    def test_for_pc_blank(self):
        assert FillStrategy.for_pc(is_prefilled=False) == FillStrategy.PC_FULL

    def test_for_rfq_prefilled(self):
        assert FillStrategy.for_rfq(is_prefilled=True) == FillStrategy.RFQ_PREFILLED

    def test_for_rfq_blank(self):
        assert FillStrategy.for_rfq(is_prefilled=False) == FillStrategy.RFQ_FULL


# ═══════════════════════════════════════════════════════════════════════════
# normalize_line_item()
# ═══════════════════════════════════════════════════════════════════════════

class TestNormalizeLineItem:

    def test_pc_format(self):
        raw = {"qty": 10, "unit_price": 8.49, "uom": "EA", "description": "Gloves"}
        n = normalize_line_item(raw)
        assert n["qty"] == 10
        assert n["price_per_unit"] == 8.49
        assert n["uom"] == "EA"
        assert n["description"] == "Gloves"

    def test_rfq_format(self):
        raw = {"quantity": 5, "bid_price": "$42.99", "UOM": "BOX", "desc": " Paper "}
        n = normalize_line_item(raw)
        assert n["qty"] == 5
        assert n["price_per_unit"] == 42.99
        assert n["uom"] == "BOX"
        assert n["description"] == "Paper"

    def test_does_not_mutate_input(self):
        raw = {"description": "Original", "qty": 1}
        _ = normalize_line_item(raw)
        assert raw["description"] == "Original"

    def test_defaults_uom_to_ea(self):
        n = normalize_line_item({})
        assert n["uom"] == "EA"

    def test_strips_dollar_and_comma(self):
        n = normalize_line_item({"unit_price": "$1,234.56"})
        assert n["price_per_unit"] == 1234.56


# ═══════════════════════════════════════════════════════════════════════════
# split_description()
# ═══════════════════════════════════════════════════════════════════════════

class TestSplitDescription:

    def test_short_text_no_split(self):
        part1, part2 = split_description("Short text", 140)
        assert part1 == "Short text"
        assert part2 is None

    def test_empty_text(self):
        part1, part2 = split_description("", 140)
        assert part1 == ""
        assert part2 is None

    def test_splits_at_newline(self):
        text = "First line\nSecond line that goes on"
        part1, part2 = split_description(text, 15)
        assert "First line" in part1
        assert "Second" in part2

    def test_splits_at_comma_space(self):
        text = "Nitrile exam gloves, large, powder-free, box of 100, blue color for medical"
        part1, part2 = split_description(text, 50)
        assert len(part1) <= 60  # approximate, may break at comma
        assert part2 is not None

    def test_splits_at_space(self):
        text = "A" * 50 + " " + "B" * 50
        part1, part2 = split_description(text, 55)
        assert part2 is not None

    def test_exact_limit(self):
        text = "x" * 140
        part1, part2 = split_description(text, 140)
        assert part1 == text
        assert part2 is None


# ═══════════════════════════════════════════════════════════════════════════
# compute_line_totals()
# ═══════════════════════════════════════════════════════════════════════════

class TestComputeLineTotals:

    def test_basic_totals(self):
        items = [
            {"qty": 10, "price_per_unit": 8.49},
            {"qty": 5, "unit_price": 42.99},
        ]
        result = compute_line_totals(items)
        assert result.subtotal == 299.85  # 84.90 + 214.95
        assert result.tax == 0.0
        assert result.total == 299.85
        assert result.items_priced == 2
        assert result.items_total == 2

    def test_with_tax(self):
        items = [{"qty": 1, "unit_price": 100.00}]
        result = compute_line_totals(items, tax_rate=0.0775)
        assert result.subtotal == 100.00
        assert result.tax == 7.75
        assert result.total == 107.75

    def test_with_freight(self):
        items = [{"qty": 1, "unit_price": 100.00}]
        result = compute_line_totals(items, freight=15.00)
        assert result.total == 115.00

    def test_with_tax_and_freight(self):
        items = [{"qty": 2, "unit_price": 50.00}]
        result = compute_line_totals(items, tax_rate=0.10, freight=10.00)
        assert result.subtotal == 100.00
        assert result.tax == 10.00
        assert result.total == 120.00

    def test_empty_items(self):
        result = compute_line_totals([])
        assert result.subtotal == 0
        assert result.total == 0
        assert result.items_priced == 0
        assert result.items_total == 0

    def test_zero_price_items_not_counted(self):
        items = [
            {"qty": 10, "unit_price": 8.49},
            {"qty": 5, "unit_price": 0},
            {"qty": 3},  # missing price
        ]
        result = compute_line_totals(items)
        assert result.items_priced == 1
        assert result.items_total == 3
        assert result.subtotal == 84.90

    def test_returns_totals_result_type(self):
        result = compute_line_totals([{"qty": 1, "unit_price": 10}])
        assert isinstance(result, TotalsResult)


# ═══════════════════════════════════════════════════════════════════════════
# enrich_pc_description — buyer description + REF ASIN + QTY per UOM
# build_pc_substitute_text — MFG#/UPC always populates SUBSTITUTED column
# Regression for Mike's 2026-05-05 row-2 mangling: MFG# was being jammed into
# description (overflowing the form field, clipping leading "N" of "Nads" and
# the trailing digit of the UPC) AND substituted column was blank because
# build_pc_substitute_text gated on is_substitute=True.
# Per Mike's spec: "catalog contains all data, buyer output (704) or RFQ
# determines what gets published" + "apply MFG/item number if not provided
# and add REF ASIN: ... and QTY Per UOM: in description as well too."
# ═══════════════════════════════════════════════════════════════════════════

class TestEnrichPcDescription:

    def test_no_mfg_in_description(self):
        """MFG# now lives in SUBSTITUTED column — never inline-appended to description."""
        item = {"description": "Nads Hair Removal Body Wax Strips for Normal Skin",
                "mfg_number": "0063899500192"}
        out = enrich_pc_description(item)
        assert "MFG#" not in out, f"MFG# should not be in description, got: {out!r}"
        assert "0063899500192" not in out, f"UPC should not be in description, got: {out!r}"
        assert out.startswith("Nads Hair Removal Body Wax Strips")

    def test_qty_per_uom_appended_when_known(self):
        """Mike's 2026-05-05 spec: 'QTY per UOM: N' in description."""
        item = {"description": "Nads Hair Removal Body Wax Strips",
                "qty_per_uom": 24, "uom": "BX"}
        out = enrich_pc_description(item)
        assert "QTY per UOM: 24" in out, f"Should label as 'QTY per UOM: 24', got: {out!r}"

    def test_ref_asin_extracted_from_item_link(self):
        """REF ASIN comes from the operator-confirmed item_link, NOT from
        cached pricing.amazon_asin (which can carry a wrong-product ASIN
        from a prior bad match — Mike's Heel Donut → Echo Dot residue)."""
        item = {
            "description": "Nads Hair Removal Body Wax Strips",
            "item_link": "https://www.amazon.com/Nads-Body-Wax-Strips-24/dp/B000NQ4JGM?th=1",
            "pricing": {"amazon_asin": "B08TVK1JQS"},  # poisoned cache from prior mismatch
        }
        out = enrich_pc_description(item)
        assert "REF ASIN: B000NQ4JGM" in out, \
            f"Should use ASIN from item_link, got: {out!r}"
        assert "B08TVK1JQS" not in out, \
            f"Must NOT use cached pricing.amazon_asin (poisoned), got: {out!r}"

    def test_no_ref_asin_when_no_item_link(self):
        """REF ASIN omitted when there's no Amazon item_link to confirm it."""
        item = {"description": "Generic widget",
                "pricing": {"amazon_asin": "B0CHH87PT2"}}  # cache only, no link
        out = enrich_pc_description(item)
        assert "REF ASIN" not in out, \
            f"No item_link → no REF ASIN even if cached ASIN exists, got: {out!r}"

    def test_no_ref_asin_for_non_amazon_link(self):
        """item_link to a non-Amazon supplier shouldn't produce a REF ASIN line."""
        item = {"description": "Widget",
                "item_link": "https://www.grainger.com/product/12345"}
        out = enrich_pc_description(item)
        assert "REF ASIN" not in out, f"Grainger link → no REF ASIN, got: {out!r}"

    def test_buyer_description_preserved_verbatim(self):
        """Per Mike: 'you keep the buyer description.' Don't modify, don't
        prepend, don't trim — append decorations only."""
        buyer_text = "Heel Donut Cushions, Heel Cups, Silicon Insoles, One Size Fits All - 1 Pair"
        item = {"description": buyer_text}
        out = enrich_pc_description(item)
        assert out.startswith(buyer_text), \
            f"Buyer text must lead, got: {out!r}"

    def test_empty_description_returns_empty(self):
        assert enrich_pc_description({"qty_per_uom": 24}) == ""


class TestBuildPcSubstituteText:

    def test_mfg_populates_for_non_substitute(self):
        """The pc_177b18e6 row-2 regression: item with MFG# but is_substitute
        unset → SUBSTITUTED column was blank. Now it always populates."""
        item = {"description": "Nads Hair Removal Body Wax Strips",
                "mfg_number": "0063899500192"}
        assert build_pc_substitute_text(item) == "0063899500192"

    def test_mfg_populates_for_upc_style_identifier(self):
        """13-digit UPC must round-trip in full (the 2026-05-05 PDF render
        was clipping the trailing '2' off '0063899500192' because the value
        was crammed into description; here we verify the source string is
        what we claim before the PDF field even gets it)."""
        item = {"mfg_number": "0063899500192"}
        out = build_pc_substitute_text(item)
        assert out == "0063899500192"
        assert len(out) == 13

    def test_mfg_populates_for_alphanumeric_part(self):
        """Heel-Donut style mfg matches buyer's row-1 convention."""
        item = {"description": "Heel Donut Cushions",
                "mfg_number": "5CAIS1G9WZZC"}
        assert build_pc_substitute_text(item) == "5CAIS1G9WZZC"

    def test_buyer_substituted_item_field_takes_precedence(self):
        """When buyer's PDF parsed a substituted_item value, preserve it
        verbatim (don't replace with our MFG# lookup)."""
        item = {"substituted_item": "BUYER-PROVIDED-XYZ-001",
                "mfg_number": "INTERNAL-456"}
        assert build_pc_substitute_text(item) == "BUYER-PROVIDED-XYZ-001"

    def test_substitute_keeps_qualifying_format(self):
        """Substitutes still get 'MFG#: N\\nDescription' format so buyer sees
        what we substituted (preserves the previous behavior for is_substitute=True)."""
        item = {"description": "Heel Donut Cushions",
                "mfg_number": "5CAIS1G9WZZC",
                "is_substitute": True}
        out = build_pc_substitute_text(item, "Heel Donut Cushions")
        assert "5CAIS1G9WZZC" in out
        assert "MFG#:" in out

    def test_no_mfg_returns_empty(self):
        item = {"description": "Some product"}
        assert build_pc_substitute_text(item) == ""

    def test_capped_at_120_chars(self):
        """SUBSTITUTED field has limited width — never exceed 120 chars."""
        item = {"mfg_number": "X" * 200}
        out = build_pc_substitute_text(item)
        assert len(out) <= 120
