"""Tests for the canonical Quote model (Phase 1)."""
from decimal import Decimal

import pytest

from src.core.quote_model import (
    Quote, QuoteStatus, DocType, LineItem, QuoteHeader, BuyerInfo, Address,
)


class TestLineItemComputed:
    """Computed fields on LineItem."""

    def test_unit_price_computed(self):
        item = LineItem(unit_cost=Decimal("10.00"), markup_pct=Decimal("35"))
        assert item.unit_price == Decimal("13.50")

    def test_extension_computed(self):
        item = LineItem(unit_cost=Decimal("10.00"), markup_pct=Decimal("35"), qty=Decimal("5"))
        assert item.extension == Decimal("67.50")

    def test_zero_cost(self):
        item = LineItem(unit_cost=Decimal("0"), markup_pct=Decimal("35"))
        assert item.unit_price == Decimal("0")
        assert item.extension == Decimal("0")

    def test_rounding(self):
        # $82.24 * 1.40 = $115.136 → rounds to $115.14
        item = LineItem(unit_cost=Decimal("82.24"), markup_pct=Decimal("40"))
        assert item.unit_price == Decimal("115.14")

    def test_large_quantity(self):
        item = LineItem(unit_cost=Decimal("1.50"), markup_pct=Decimal("25"), qty=Decimal("1000"))
        assert item.unit_price == Decimal("1.88")
        assert item.extension == Decimal("1880.00")


class TestQuoteSubtotal:
    """Computed subtotal from line items."""

    def test_subtotal(self):
        q = Quote(line_items=[
            LineItem(line_no=1, unit_cost=Decimal("10"), markup_pct=Decimal("0"), qty=Decimal("1")),
            LineItem(line_no=2, unit_cost=Decimal("20"), markup_pct=Decimal("0"), qty=Decimal("2")),
        ])
        assert q.subtotal == Decimal("50.00")

    def test_subtotal_excludes_no_bid(self):
        q = Quote(line_items=[
            LineItem(line_no=1, unit_cost=Decimal("10"), markup_pct=Decimal("0"), qty=Decimal("1")),
            LineItem(line_no=2, unit_cost=Decimal("20"), markup_pct=Decimal("0"), qty=Decimal("1"), no_bid=True),
        ])
        assert q.subtotal == Decimal("10.00")

    def test_empty_items(self):
        q = Quote(line_items=[])
        assert q.subtotal == Decimal("0")
        assert q.item_count == 0


class TestSetPrice:
    """Typed mutation: set_price with 3x sanity check."""

    def test_basic_set_price(self):
        q = Quote(line_items=[
            LineItem(line_no=1, description="Test", unit_cost=Decimal("0")),
        ])
        q.set_price(1, Decimal("10.00"), Decimal("35"))
        assert q.line_items[0].unit_cost == Decimal("10.00")
        assert q.line_items[0].markup_pct == Decimal("35")
        assert q.line_items[0].unit_price == Decimal("13.50")

    def test_3x_sanity_caps(self):
        """Cost > 3x reference price gets capped to reference."""
        q = Quote(line_items=[
            LineItem(line_no=1, description="Test", scprs_price=Decimal("20.00")),
        ])
        q.set_price(1, Decimal("100.00"), Decimal("35"))  # 100 > 3*20
        assert q.line_items[0].unit_cost == Decimal("20.00")  # Capped

    def test_3x_sanity_allows_normal(self):
        """Cost within 3x passes through."""
        q = Quote(line_items=[
            LineItem(line_no=1, description="Test", scprs_price=Decimal("20.00")),
        ])
        q.set_price(1, Decimal("50.00"), Decimal("35"))  # 50 < 3*20=60
        assert q.line_items[0].unit_cost == Decimal("50.00")

    def test_set_price_not_found(self):
        q = Quote(line_items=[LineItem(line_no=1)])
        with pytest.raises(ValueError, match="Line 99 not found"):
            q.set_price(99, Decimal("10"))

    def test_audit_trail(self):
        q = Quote(line_items=[LineItem(line_no=1)])
        q.set_price(1, Decimal("10"), Decimal("25"))
        assert len(q.provenance.audit_trail) == 1
        assert "set_price" in q.provenance.audit_trail[0].action


class TestAddRemoveItem:
    """Item list mutations."""

    def test_add_item(self):
        q = Quote()
        q.add_item(LineItem(description="New item"))
        assert len(q.line_items) == 1
        assert q.line_items[0].line_no == 1

    def test_add_item_auto_numbers(self):
        q = Quote(line_items=[LineItem(line_no=5)])
        q.add_item(LineItem(description="New"))
        assert q.line_items[-1].line_no == 6

    def test_remove_item(self):
        q = Quote(line_items=[
            LineItem(line_no=1, description="Keep"),
            LineItem(line_no=2, description="Remove"),
        ])
        q.remove_item(2)
        assert len(q.line_items) == 1
        assert q.line_items[0].description == "Keep"


class TestFromLegacyPC:
    """Migration from legacy PC dicts."""

    def test_basic_pc(self, sample_pc):
        q = Quote.from_legacy_dict(sample_pc, doc_type="pc")
        assert q.doc_type == DocType.PC
        assert q.doc_id == "test-pc-001"
        assert q.header.institution_key == "CSP-Sacramento"
        assert q.status == QuoteStatus.PRICED
        assert len(q.line_items) == 2

    def test_pc_pricing_preserved(self, sample_pc):
        q = Quote.from_legacy_dict(sample_pc, doc_type="pc")
        assert q.line_items[0].unit_cost == Decimal("12.58")
        assert q.line_items[0].amazon_price == Decimal("12.58")
        assert q.line_items[0].price_source == "amazon"

    def test_pc_round_trip(self, sample_pc):
        q = Quote.from_legacy_dict(sample_pc, doc_type="pc")
        d = q.to_legacy_dict()
        assert d["id"] == "test-pc-001"
        assert d["institution"] == "CSP-Sacramento"
        assert len(d["items"]) == 2
        assert len(d["line_items"]) == 2
        assert d["items"][0]["supplier_cost"] == float(q.line_items[0].unit_cost)


class TestFromLegacyRFQ:
    """Migration from legacy RFQ dicts."""

    def test_basic_rfq(self, sample_rfq):
        q = Quote.from_legacy_dict(sample_rfq, doc_type="rfq")
        assert q.doc_type == DocType.RFQ
        assert q.doc_id == "test-rfq-001"
        assert q.header.solicitation_number == "RFQ-2026-TEST"

    def test_rfq_due_date_parsed(self, sample_rfq):
        q = Quote.from_legacy_dict(sample_rfq, doc_type="rfq")
        assert q.header.due_date is not None
        assert q.header.due_date.month == 3
        assert q.header.due_date.day == 15

    def test_rfq_scprs_preserved(self, sample_rfq):
        q = Quote.from_legacy_dict(sample_rfq, doc_type="rfq")
        assert q.line_items[0].scprs_price == Decimal("475.00")

    def test_rfq_round_trip(self, sample_rfq):
        q = Quote.from_legacy_dict(sample_rfq, doc_type="rfq")
        d = q.to_legacy_dict()
        assert d["id"] == "test-rfq-001"
        assert d["solicitation_number"] == "RFQ-2026-TEST"
        assert d["due_date"] == "03/15/2026"
        assert len(d["line_items"]) > 0


class TestStatusTransition:
    """Status transitions."""

    def test_basic_transition(self):
        q = Quote(status=QuoteStatus.DRAFT)
        q.transition(QuoteStatus.PRICED)
        assert q.status == QuoteStatus.PRICED
        assert len(q.provenance.audit_trail) == 1

    def test_legacy_status_mapping(self):
        d = {"id": "x", "status": "enriched", "items": []}
        q = Quote.from_legacy_dict(d)
        assert q.status == QuoteStatus.ENRICHED


class TestSerialization:
    """JSON serialization."""

    def test_model_dump(self):
        q = Quote(
            doc_id="test",
            line_items=[LineItem(line_no=1, unit_cost=Decimal("10"), markup_pct=Decimal("25"))],
        )
        data = q.model_dump(mode="json")
        assert data["doc_id"] == "test"
        assert data["subtotal"] == "12.50"
        assert data["line_items"][0]["unit_price"] == "12.50"

    def test_model_validate_round_trip(self):
        q = Quote(
            doc_id="test",
            line_items=[LineItem(line_no=1, unit_cost=Decimal("10"), markup_pct=Decimal("25"))],
        )
        data = q.model_dump(mode="json")
        q2 = Quote.model_validate(data)
        assert q2.doc_id == "test"
        assert q2.line_items[0].unit_cost == Decimal("10")
