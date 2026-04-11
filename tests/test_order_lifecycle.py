"""
test_order_lifecycle.py — Order & Pricing Pipeline Tests

Tests order creation, status transitions, winning price recording,
Oracle calibration, and V5 confidence-weighted pricing features.
"""

import json
import uuid
from datetime import datetime

import pytest


class TestOrderLifecycle:
    """Test order creation, status transitions, and revenue tracking."""

    def test_order_creation(self, temp_data_dir):
        """Orders can be created and read back from DB."""
        from src.core.db import get_db

        order_id = str(uuid.uuid4())[:12]
        now = datetime.now().isoformat()
        with get_db() as conn:
            conn.execute("""
                INSERT INTO orders
                (id, quote_number, agency, institution, po_number, status,
                 total, items, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (order_id, "R26Q999", "CDCR", "CSP-Sacramento",
                  "PO-TEST-001", "new", 814.23,
                  json.dumps([{"description": "Test item", "qty": 1, "unit_price": 814.23}]),
                  now, now))

        with get_db() as conn:
            row = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        assert row is not None, "Order not created"
        d = dict(row)
        assert d["po_number"] == "PO-TEST-001"
        assert d["status"] == "new"
        assert float(d["total"]) == 814.23

    def test_order_status_transitions(self, temp_data_dir):
        """Order status should transition through valid states."""
        from src.core.db import get_db

        order_id = str(uuid.uuid4())[:12]
        now = datetime.now().isoformat()
        with get_db() as conn:
            conn.execute("""
                INSERT INTO orders (id, quote_number, po_number, status, total, items, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?)
            """, (order_id, "R26Q888", "PO-002", "new", 500.0, json.dumps([]), now, now))

        for new_status in ["processing", "shipped", "completed"]:
            with get_db() as conn:
                conn.execute("UPDATE orders SET status=?, updated_at=? WHERE id=?",
                             (new_status, datetime.now().isoformat(), order_id))
                row = conn.execute("SELECT status FROM orders WHERE id=?", (order_id,)).fetchone()
            assert row[0] == new_status

    def test_winning_prices_recorded(self, temp_data_dir):
        """record_winning_prices should populate winning_prices table."""
        from src.knowledge.pricing_intel import record_winning_prices

        recorded = record_winning_prices({
            "order_id": "lifecycle-test",
            "quote_number": "R26QLFT",
            "agency": "CDCR",
            "institution": "CSP-Sacramento",
            "line_items": [
                {"description": "Lifecycle item A", "qty": 10,
                 "unit_price": 25.00, "cost": 18.00, "supplier": "Amazon"},
                {"description": "Lifecycle item B", "qty": 5,
                 "unit_price": 50.00, "cost": 35.00, "supplier": "Uline"},
            ],
        })
        assert recorded == 2, f"Expected 2 recorded, got {recorded}"

    def test_winning_prices_skips_empty(self, temp_data_dir):
        """Items with no price or description should be skipped."""
        from src.knowledge.pricing_intel import record_winning_prices

        recorded = record_winning_prices({
            "order_id": "empty-test",
            "line_items": [
                {"description": "", "qty": 1, "unit_price": 10.00},
                {"description": "Has desc", "qty": 1, "unit_price": 0},
            ],
        })
        assert recorded == 0, "Should skip items with empty desc or $0 price"

    def test_calibration_from_win(self, temp_data_dir):
        """Win outcomes should update oracle calibration table."""
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        from src.core.db import get_db

        items = [{
            "description": "Nitrile exam gloves powder free medium",
            "vendor_cost": 8.50,
            "unit_price": 12.75,
            "pricing": {"unit_cost": 8.50},
        }]
        calibrate_from_outcome(items, "won", agency="CCHCS")

        with get_db() as conn:
            row = conn.execute(
                "SELECT win_count, sample_size FROM oracle_calibration WHERE category='medical'"
            ).fetchone()
        assert row is not None, "Calibration row should exist for medical"
        assert row[0] >= 1, f"win_count should be >= 1, got {row[0]}"

    def test_calibration_from_loss(self, temp_data_dir):
        """Loss outcomes should update oracle calibration with loss reason."""
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        from src.core.db import get_db

        items = [{
            "description": "Copy paper white letter size ream 500 sheets",
            "vendor_cost": 30.00,
            "unit_price": 45.00,
            "pricing": {"unit_cost": 30.00},
        }]
        calibrate_from_outcome(items, "lost", agency="CDCR", loss_reason="price")

        with get_db() as conn:
            row = conn.execute(
                "SELECT loss_on_price FROM oracle_calibration WHERE category='office'"
            ).fetchone()
        assert row is not None
        assert row[0] >= 1, f"loss_on_price should be >= 1, got {row[0]}"


class TestPricingPipeline:
    """Test Oracle pricing, confidence tiers, and enrichment."""

    def test_oracle_returns_all_fields(self, temp_data_dir):
        """Oracle response should have all expected keys."""
        from src.core.pricing_oracle_v2 import get_pricing

        result = get_pricing("Copy paper white 8.5x11 500 sheets", quantity=10, cost=35.00)
        for key in ("recommendation", "market", "sources_used", "strategies", "tiers"):
            assert key in result, f"Missing key: {key}"

    def test_oracle_blind_tier(self, temp_data_dir):
        """No market data → blind tier with 30% markup."""
        from src.core.pricing_oracle_v2 import get_pricing

        r = get_pricing("Unique item no data XYZQ998877", quantity=1, cost=100.0)
        rec = r["recommendation"]
        assert rec.get("data_confidence") == "blind"
        assert rec.get("quote_price") == 130.0, \
            f"Expected $130.00, got {rec.get('quote_price')}"

    def test_oracle_floor_never_below_15pct(self, temp_data_dir):
        """Oracle should never go below 15% markup floor."""
        from src.core.pricing_oracle_v2 import get_pricing

        r = get_pricing("Generic item ABC", quantity=1, cost=100.0)
        rec = r["recommendation"]
        if rec.get("quote_price"):
            assert rec["quote_price"] >= 115.0, \
                f"Below 15% floor: {rec['quote_price']}"

    def test_category_classification(self, temp_data_dir):
        """Category classifier detects common item categories."""
        from src.core.pricing_oracle_v2 import _classify_item_category

        assert _classify_item_category("Nitrile exam gloves medium") == "medical"
        assert _classify_item_category("Copy paper white 8.5x11 ream") == "office"
        assert _classify_item_category("Trash bags heavy duty 33 gallon") == "janitorial"
        assert _classify_item_category("Coffee ground regular 2lb bag") == "food"

    def test_identifier_extraction_upc(self, temp_data_dir):
        """parse_identifiers extracts UPC codes."""
        from src.agents.item_enricher import parse_identifiers

        r = parse_identifiers("Monopoly Game - 195166217604")
        assert "195166217604" in r["identifiers"]["upc_codes"]

    def test_identifier_extraction_asin(self, temp_data_dir):
        """parse_identifiers extracts ASIN."""
        from src.agents.item_enricher import parse_identifiers

        r = parse_identifiers("Widget ASIN: B0A1B2C3D4")
        assert "B0A1B2C3D4" in r["identifiers"]["asins"]

    def test_identifier_extraction_mfg(self, temp_data_dir):
        """parse_identifiers extracts MFG numbers."""
        from src.agents.item_enricher import parse_identifiers

        r = parse_identifiers("Pen MFG# SAN80653 Fine Point")
        assert "SAN80653" in r["identifiers"]["mfg_numbers"]

    def test_supplier_sku_uline(self, temp_data_dir):
        """parse_identifiers detects Uline SKUs."""
        from src.agents.item_enricher import parse_identifiers

        r = parse_identifiers("Uline S-12345 bubble wrap")
        assert r["supplier_skus"].get("uline") == "S-12345"

    def test_supplier_sku_ssww(self, temp_data_dir):
        """parse_identifiers detects S&S Worldwide items."""
        from src.agents.item_enricher import parse_identifiers

        r = parse_identifiers("S&S Worldwide Item Model #: 60002")
        assert "ssww" in r["supplier_skus"]

    def test_institution_profile_created(self, temp_data_dir):
        """V5: calibration should create institution_pricing_profile."""
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        from src.core.db import get_db

        items = [{
            "description": "Bandage elastic wrap 4 inch",
            "vendor_cost": 3.00,
            "unit_price": 5.00,
            "pricing": {"unit_cost": 3.00},
        }]
        calibrate_from_outcome(items, "won", agency="CDCR-Folsom")

        with get_db() as conn:
            row = conn.execute(
                "SELECT win_count FROM institution_pricing_profile "
                "WHERE institution='CDCR-Folsom'"
            ).fetchone()
        assert row is not None, "Institution profile should exist"
        assert row[0] >= 1

    def test_quote_shape_recorded(self, temp_data_dir):
        """V5: calibration should record quote shapes."""
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        from src.core.db import get_db

        items = [
            {"description": "Item A paper", "vendor_cost": 10.0,
             "unit_price": 15.0, "pricing": {"unit_cost": 10.0}},
            {"description": "Item B gloves medical", "vendor_cost": 8.0,
             "unit_price": 12.0, "pricing": {"unit_cost": 8.0}},
        ]
        calibrate_from_outcome(items, "won", agency="CCHCS-Test")

        with get_db() as conn:
            row = conn.execute(
                "SELECT avg_markup FROM winning_quote_shapes WHERE institution='CCHCS-Test'"
            ).fetchone()
        assert row is not None, "Quote shape should be recorded"
        assert row[0] > 0

    def test_requote_triggers(self, temp_data_dir):
        """V5: check_requote_triggers returns list without error."""
        from src.core.pricing_oracle_v2 import check_requote_triggers

        triggers = check_requote_triggers()
        assert isinstance(triggers, list)

    def test_expiring_costs(self, temp_data_dir):
        """get_expiring_costs should return costs expiring within N days."""
        from src.core.pricing_oracle_v2 import get_expiring_costs
        result = get_expiring_costs(days=7)
        assert isinstance(result, list)

    def test_price_history_for_item(self, temp_data_dir):
        """Price history lookup should return results without error."""
        from src.core.pricing_oracle_v2 import get_price_history_for_item
        result = get_price_history_for_item("Nitrile exam gloves medium")
        assert isinstance(result, list)

    def test_speed_stats(self, temp_data_dir):
        """Speed stats should return metrics dict."""
        from src.core.pricing_oracle_v2 import get_speed_stats
        result = get_speed_stats()
        assert isinstance(result, dict)
        assert "pcs" in result or "quotes" in result

    def test_pricing_intel_summary(self, temp_data_dir):
        """Pricing intel summary should return dashboard stats."""
        from src.knowledge.pricing_intel import get_pricing_intelligence_summary
        result = get_pricing_intelligence_summary()
        assert isinstance(result, dict)
        assert "total_records" in result


class TestEmailClassification:
    """Test email classification functions for pipeline coverage."""

    def test_solicitation_number_extraction(self, mock_gmail):
        """Extract solicitation number from email subject/body."""
        from src.agents.email_poller import extract_solicitation_number
        result = extract_solicitation_number(
            subject="Price Check - CSP Sacramento - Office Supplies Q3",
            body="Please provide pricing per the attached 704 form.",
        )
        # Should return a string (may be empty if no number found)
        assert isinstance(result, str)

    def test_reply_followup_detection(self, mock_gmail):
        """Reply/follow-up emails should be detected."""
        from src.agents.email_poller import is_reply_followup
        msg = {"from": "buyer@cdcr.ca.gov", "subject": "Re: Price Check - Supplies"}
        result = is_reply_followup(
            msg=msg,
            subject="Re: Price Check - Supplies",
            body="Following up on the pricing request.",
            sender="buyer@cdcr.ca.gov",
            pdf_names=[],
        )
        # Returns truthy/falsy (may be None, dict, or bool)
        assert result is None or isinstance(result, (bool, dict))

    def test_sender_email_extraction(self, mock_gmail):
        """_extract_email_addr should parse email from sender string."""
        from src.agents.email_poller import _extract_email_addr
        assert _extract_email_addr("John Doe <john@example.com>") == "john@example.com"
        assert _extract_email_addr("plain@example.com") == "plain@example.com"


class TestQuoteLifecycle:
    """Test quote generation and lifecycle."""

    def test_quote_number_format(self, temp_data_dir):
        """Quote numbers follow R{YY}Q{seq} format."""
        from src.forms.quote_generator import _next_quote_number
        qn = _next_quote_number()
        assert qn.startswith("R")
        assert "Q" in qn

    def test_normalize_rfq_fields(self, temp_data_dir):
        """RFQ normalization should add alias fields."""
        from src.api.data_layer import _normalize_rfq_fields
        rfqs = {
            "rfq1": {"items": [{"desc": "test"}], "rfq_number": "SOL-001"},
        }
        result = _normalize_rfq_fields(rfqs)
        assert "line_items" in result["rfq1"]
        assert "solicitation_number" in result["rfq1"]

    def test_is_user_facing_pc(self, temp_data_dir):
        """User-facing filter should show PCs with items, hide dismissed."""
        from src.api.data_layer import _is_user_facing_pc
        assert _is_user_facing_pc({"items": [{"desc": "test"}], "status": "parsed"}) is True
        assert _is_user_facing_pc({"items": [], "status": "dismissed"}) is False
        assert _is_user_facing_pc({"items": [], "status": "new", "pc_number": "GP-001"}) is True
        assert _is_user_facing_pc({"items": [], "status": "new"}) is False

    def test_get_pc_items_from_items(self, temp_data_dir):
        """_get_pc_items should return items list."""
        from src.api.data_layer import _get_pc_items
        pc = {"items": [{"desc": "A"}, {"desc": "B"}]}
        assert len(_get_pc_items(pc)) == 2

    def test_get_pc_items_from_pc_data(self, temp_data_dir):
        """_get_pc_items should fall back to pc_data blob."""
        from src.api.data_layer import _get_pc_items
        import json
        pc = {"items": [], "pc_data": json.dumps({"items": [{"desc": "X"}]})}
        assert len(_get_pc_items(pc)) == 1

    def test_fingerprint_deterministic(self, temp_data_dir):
        """Same input always produces same fingerprint."""
        from src.knowledge.pricing_intel import _item_fingerprint

        fp1 = _item_fingerprint("Test item ABC")
        fp2 = _item_fingerprint("Test item ABC")
        fp3 = _item_fingerprint("Different item")
        assert fp1 == fp2
        assert fp1 != fp3
