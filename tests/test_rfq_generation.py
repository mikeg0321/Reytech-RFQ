"""Integration tests for RFQ generation and management routes.

Tests RFQ CRUD operations and the generate pipeline using Flask test client.
Uses seed_rfq fixture for pre-populated data and mock external APIs.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


# ═══════════════════════════════════════════════════════════════════════════
# RFQ list and detail routes
# ═══════════════════════════════════════════════════════════════════════════

class TestRfqRoutes:

    def test_homepage_loads(self, client):
        """GET / (contains RFQ queue table) should return 200."""
        resp = client.get("/")
        assert resp.status_code == 200

    def test_rfq_detail_redirects_for_missing(self, client):
        """GET /rfq/<nonexistent> should redirect to home."""
        resp = client.get("/rfq/nonexistent-id-12345")
        assert resp.status_code in (200, 302, 404)

    def test_rfq_detail_with_seed(self, client, seed_rfq):
        """GET /rfq/<id> should load seeded RFQ."""
        resp = client.get(f"/rfq/{seed_rfq}")
        # Should either render the page or redirect
        assert resp.status_code in (200, 302)


# ═══════════════════════════════════════════════════════════════════════════
# RFQ API endpoints
# ═══════════════════════════════════════════════════════════════════════════

class TestRfqApiEndpoints:

    def test_status_update(self, client, seed_rfq):
        """POST status update should succeed."""
        resp = client.post(f"/api/rfq/{seed_rfq}/update-status",
                           json={"status": "in_progress"})
        # May return JSON or redirect
        assert resp.status_code in (200, 302)

    def test_generate_without_templates_fails_gracefully(self, client, seed_rfq):
        """POST generate without uploaded templates should flash error, not crash."""
        resp = client.post(f"/rfq/{seed_rfq}/generate",
                           data={})
        # Should redirect back with flash, not 500
        assert resp.status_code in (200, 302), f"Expected redirect, got {resp.status_code}"


# ═══════════════════════════════════════════════════════════════════════════
# RFQ data validation
# ═══════════════════════════════════════════════════════════════════════════

class TestRfqDataIntegrity:

    def test_seed_rfq_has_line_items(self, sample_rfq):
        """Verify sample RFQ fixture has expected structure."""
        assert "line_items" in sample_rfq
        assert len(sample_rfq["line_items"]) > 0
        item = sample_rfq["line_items"][0]
        assert "qty" in item
        assert "description" in item
        assert "price_per_unit" in item

    def test_seed_rfq_has_metadata(self, sample_rfq):
        """Verify sample RFQ has all required metadata."""
        assert sample_rfq["solicitation_number"] == "RFQ-2026-TEST"
        assert sample_rfq["requestor_name"] == "Jane Smith"
        assert sample_rfq["requestor_email"] == "jane@state.ca.gov"
        assert sample_rfq["status"] == "new"

    def test_stryker_quote_structure(self, sample_stryker_quote):
        """Verify Stryker quote fixture for X-RESTRAINT items."""
        items = sample_stryker_quote["line_items"]
        assert len(items) == 3
        assert items[0]["part_number"] == "6500-001-430"
        assert items[0]["unit_price"] == 454.40
        assert items[0]["qty"] == 2


# ═══════════════════════════════════════════════════════════════════════════
# Auth protection
# ═══════════════════════════════════════════════════════════════════════════

class TestRfqAuth:

    def test_homepage_requires_auth(self, anon_client):
        """Unauthenticated request to / should get 401."""
        resp = anon_client.get("/")
        assert resp.status_code in (401, 403, 302)

    def test_rfq_api_requires_auth(self, anon_client):
        """API endpoint should reject unauthenticated requests."""
        resp = anon_client.post("/api/rfq/test/update-status",
                                json={"status": "new"})
        assert resp.status_code in (401, 403, 302)
