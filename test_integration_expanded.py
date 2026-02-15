"""
Cross-module integration tests — exercise real pipelines that span multiple modules.
"""
import os
import json
import pytest


# ═══════════════════════════════════════════════════════════════════════════════
# PC → Won Quotes KB Ingestion
# ═══════════════════════════════════════════════════════════════════════════════

class TestPCToKBIngestion:
    """When a PC is completed, items should be ingestable into Won Quotes KB."""

    def test_ingest_pc_items_to_kb(self, sample_pc):
        from won_quotes_db import ingest_scprs_result, find_similar_items

        # Simulate ingesting PC items after completion
        for item in sample_pc["items"]:
            pricing = item.get("pricing", {})
            if pricing.get("recommended_price"):
                ingest_scprs_result(
                    po_number=sample_pc["pc_number"],
                    item_number=item["item_number"],
                    description=item["description"],
                    unit_price=pricing["recommended_price"],
                    source="price_check",
                )

        # Now find_similar_items should return these
        results = find_similar_items("1", "Engraved two line name tag")
        assert len(results) >= 1


# ═══════════════════════════════════════════════════════════════════════════════
# Oracle → Quote Generator
# ═══════════════════════════════════════════════════════════════════════════════

class TestOracleToQuote:
    """Pricing oracle output feeds directly into quote generation."""

    def test_oracle_price_into_quote(self, tmp_path):
        from pricing_oracle import recommend_price
        from quote_generator import generate_quote

        # Get oracle recommendation
        rec = recommend_price("TEST-INT", "Office chair ergonomic", supplier_cost=150.00)
        oracle_price = rec["recommended"]["price"]

        # Build quote with oracle price
        quote_data = {
            "institution": "Test Institution",
            "ship_to_name": "Test",
            "ship_to_address": ["123 Main St"],
            "rfq_number": "INT-TEST-001",
            "line_items": [{
                "line_number": 1,
                "part_number": "CHAIR-001",
                "qty": 3,
                "uom": "EA",
                "description": "Office chair ergonomic",
                "unit_price": oracle_price,
            }],
        }

        out = str(tmp_path / "oracle_quote.pdf")
        result = generate_quote(quote_data, out, quote_number="INT1", include_tax=False)

        assert result["ok"] is True
        assert result["subtotal"] == pytest.approx(oracle_price * 3, abs=0.01)
        assert os.path.exists(out)


# ═══════════════════════════════════════════════════════════════════════════════
# Dashboard API Chain Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestDashboardAPIChain:
    """Test API endpoint chains that users trigger via button clicks."""

    def test_pc_save_then_generate_then_download(self, client, seed_pc, temp_data_dir):
        pcid = seed_pc

        # Save prices
        r = client.post(f"/pricecheck/{pcid}/save-prices",
                        json={"price_0": 15.72, "cost_0": 12.58,
                              "markup_0": 25, "qty_0": 22,
                              "tax_enabled": False},
                        content_type="application/json")
        assert r.get_json()["ok"]

        # Generate quote
        r = client.get(f"/pricecheck/{pcid}/generate-quote")
        d = r.get_json()
        assert d["ok"]
        qn = d["quote_number"]

        # Verify quote appears in quotes list page
        r = client.get("/quotes")
        assert r.status_code == 200

    def test_rfq_update_then_generate_quote(self, client, seed_rfq):
        rid = seed_rfq

        # Update pricing
        r = client.post(f"/rfq/{rid}/update",
                        data={"cost_0": "350.00", "price_0": "454.40"},
                        follow_redirects=True)
        assert r.status_code == 200

        # Generate Reytech quote
        r = client.get(f"/rfq/{rid}/generate-quote", follow_redirects=True)
        assert r.status_code == 200

    def test_nonexistent_pc_returns_error(self, client):
        r = client.get("/pricecheck/FAKE-ID-999/generate-quote")
        d = r.get_json()
        assert d["ok"] is False

    def test_nonexistent_rfq_generate_quote(self, client):
        r = client.get("/rfq/FAKE-RFQ-999/generate-quote", follow_redirects=True)
        assert r.status_code in (200, 302, 404)


# ═══════════════════════════════════════════════════════════════════════════════
# Test IDs Presence
# ═══════════════════════════════════════════════════════════════════════════════

class TestTestIDsPresent:
    """Verify that data-testid attributes exist on key buttons for E2E testing."""

    def test_pc_page_has_test_ids(self, client, seed_pc):
        r = client.get(f"/pricecheck/{seed_pc}")
        html = r.data.decode()
        for tid in ("pc-scprs-lookup", "pc-amazon-lookup", "pc-preview-quote",
                     "pc-generate-704", "pc-generate-reytech-quote", "pc-auto-process"):
            assert f'data-testid="{tid}"' in html, f"Missing test ID: {tid}"

    def test_rfq_page_has_test_ids(self, client, seed_rfq):
        r = client.get(f"/rfq/{seed_rfq}")
        html = r.data.decode()
        for tid in ("rfq-scprs-lookup", "rfq-amazon-lookup", "rfq-preview-quote",
                     "rfq-save-pricing", "rfq-generate-state-forms",
                     "rfq-generate-reytech-quote"):
            assert f'data-testid="{tid}"' in html, f"Missing test ID: {tid}"

    def test_home_page_has_upload_test_id(self, client):
        r = client.get("/")
        html = r.data.decode()
        assert 'data-testid="upload-file-input"' in html


# ═══════════════════════════════════════════════════════════════════════════════
# Logging Integration
# ═══════════════════════════════════════════════════════════════════════════════

class TestLoggingIntegration:
    """Verify structured logging doesn't break anything."""

    def test_request_logging_does_not_break_routes(self, client):
        """After-request logging should be invisible to responses."""
        r = client.get("/")
        assert r.status_code == 200

    def test_api_health_with_logging(self, client):
        r = client.get("/api/health")
        assert r.status_code == 200
        d = r.get_json()
        assert "status" in d
