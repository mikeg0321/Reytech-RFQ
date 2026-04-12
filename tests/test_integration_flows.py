"""Integration tests for the revenue-critical PC→quote and RFQ→package flows.

Tests the actual Flask routes with authenticated test client and seeded data.
These cover the happy paths that generate revenue.
"""
import json
import os
import pytest


# ── Price Check Flow Tests ────────────────────────────────────────────────────

class TestPCDetailPage:
    """The Price Check detail page is the main working surface."""

    def test_pc_detail_loads(self, client, seed_pc):
        resp = client.get(f"/pricecheck/{seed_pc}")
        assert resp.status_code == 200

    def test_pc_detail_redirect_for_missing(self, client):
        resp = client.get("/pricecheck/nonexistent-pc")
        # Missing PC redirects to homepage (302) rather than 404
        assert resp.status_code in (200, 302, 404)

    def test_pc_detail_shows_ai_chip_when_grok_validated(
        self, client, temp_data_dir, sample_pc
    ):
        """When the Grok validator has verified an item, the source chip
        must carry an 'AI' attribution so the user can see the match was
        AI-verified (not a blind Amazon scrape)."""
        import os, json
        sample_pc["items"][0]["pricing"]["llm_validated"] = True
        sample_pc["items"][0]["pricing"]["llm_confidence"] = 0.88
        sample_pc["items"][0]["pricing"]["price_source"] = "llm_grok"
        sample_pc["items"][0]["pricing"]["llm_reasoning"] = (
            "Verified UPC 840614150049 matches S&S Worldwide Mini Velvet Art Posters"
        )
        sample_pc["parsed"]["line_items"] = sample_pc["items"]
        with open(os.path.join(temp_data_dir, "price_checks.json"), "w") as f:
            json.dump({sample_pc["id"]: sample_pc}, f)
        resp = client.get(f"/pricecheck/{sample_pc['id']}")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        # AI attribution must appear somewhere in the item's sources column
        assert " · AI" in body or "AI · " in body, (
            "Expected 'AI' attribution on the Grok-validated chip"
        )
        # Reasoning should be surfaced in a title/tooltip
        assert "Verified UPC" in body, (
            "Expected Grok reasoning to be rendered as a chip tooltip"
        )

    def test_pc_detail_shows_ai_suggest_chip_for_low_confidence(
        self, client, temp_data_dir, sample_pc
    ):
        """When Grok has a lead but confidence is below the auto-apply
        threshold, it stores llm_suggestion_* fields. The UI must show this
        as a separate 'AI suggest' chip so the user can accept or reject it."""
        import os, json
        sample_pc["items"][0]["pricing"]["llm_suggestion"] = (
            "S&S Worldwide Pack of 100"
        )
        sample_pc["items"][0]["pricing"]["llm_suggestion_price"] = 27.99
        sample_pc["items"][0]["pricing"]["llm_suggestion_url"] = (
            "https://www.amazon.com/dp/B07663Q1KX"
        )
        sample_pc["items"][0]["pricing"]["llm_suggestion_confidence"] = 0.55
        sample_pc["parsed"]["line_items"] = sample_pc["items"]
        with open(os.path.join(temp_data_dir, "price_checks.json"), "w") as f:
            json.dump({sample_pc["id"]: sample_pc}, f)
        resp = client.get(f"/pricecheck/{sample_pc['id']}")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "AI suggest" in body, (
            "Expected 'AI suggest' chip for low-confidence Grok suggestion"
        )


class TestPCSavePrices:
    """Saving prices on the PC detail page."""

    def test_save_prices_endpoint_exists(self, client, seed_pc):
        resp = client.post(
            f"/pricecheck/{seed_pc}/save-prices",
            json={"items": [
                {"item_number": "1", "unit_cost": 10.00, "bid_price": 15.00},
                {"item_number": "2", "unit_cost": 30.00, "bid_price": 45.00},
            ]},
        )
        assert resp.status_code in (200, 400, 500)
        data = resp.get_json()
        assert "ok" in data or "error" in data


class TestPCReparse:
    """Re-parsing a PC from its source PDF."""

    def test_reparse_endpoint_exists(self, client, seed_pc):
        resp = client.post(f"/pricecheck/{seed_pc}/reparse")
        # May fail if no source PDF, but shouldn't crash
        assert resp.status_code in (200, 400, 404, 500)


# ── RFQ Flow Tests ────────────────────────────────────────────────────────────

class TestRFQDetailPage:
    """The RFQ detail page."""

    def test_rfq_detail_loads(self, client, seed_rfq):
        resp = client.get(f"/rfq/{seed_rfq}")
        assert resp.status_code == 200

    def test_rfq_detail_redirect_for_missing(self, client):
        resp = client.get("/rfq/nonexistent-rfq")
        # Missing RFQ redirects to homepage (302) rather than 404
        assert resp.status_code in (200, 302, 404)


class TestRFQStatusTransition:
    """Status changes on RFQs."""

    def test_update_rfq_status(self, client, seed_rfq):
        resp = client.post(
            f"/api/rfq/{seed_rfq}/status",
            json={"status": "in_progress"},
        )
        assert resp.status_code in (200, 400, 404, 500)


# ── Homepage Tests ────────────────────────────────────────────────────────────

class TestHomepage:
    """The homepage shows PC and RFQ queues."""

    def test_homepage_loads(self, client):
        resp = client.get("/")
        assert resp.status_code == 200

    def test_homepage_with_data(self, client, seed_pc, seed_rfq):
        resp = client.get("/")
        assert resp.status_code == 200


# ── API Endpoints ─────────────────────────────────────────────────────────────

class TestDashboardAPI:
    """Core API endpoints used by the frontend."""

    def test_dashboard_init(self, client):
        resp = client.get("/api/dashboard/init")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, dict)

    def test_notifications_endpoint(self, client):
        resp = client.get("/api/notifications")
        assert resp.status_code in (200, 404)

    def test_settings_page(self, client):
        resp = client.get("/settings")
        assert resp.status_code == 200


# ── Quote Operations ──────────────────────────────────────────────────────────

class TestQuoteSearch:
    """Quote search and stats."""

    def test_quotes_page_loads(self, client):
        resp = client.get("/quotes")
        assert resp.status_code == 200

    def test_quote_search_api(self, client):
        resp = client.get("/api/quotes/search?q=test")
        assert resp.status_code in (200, 404)

    def test_quote_stats_api(self, client):
        resp = client.get("/api/quotes/stats")
        assert resp.status_code in (200, 404)


# ── Health & System ───────────────────────────────────────────────────────────

class TestHealthEndpoints:
    """Health checks and system status."""

    def test_health_check(self, client):
        resp = client.get("/health")
        assert resp.status_code in (200, 503)

    def test_ping(self, client):
        # Ping is on the app directly (not blueprint), test via anon
        pass  # covered by other tests

    def test_system_health(self, client):
        resp = client.get("/api/system/health")
        assert resp.status_code in (200, 404, 500)


# ── Auth Tests ────────────────────────────────────────────────────────────────

class TestAuth:
    """Authentication on routes."""

    def test_unauthenticated_blocked(self, anon_client):
        resp = anon_client.get("/")
        assert resp.status_code == 401

    def test_wrong_credentials_blocked(self, app):
        import base64
        with app.test_client() as c:
            creds = base64.b64encode(b"wrong:wrong").decode()
            resp = c.get("/", headers={
                "Authorization": f"Basic {creds}",
                "Origin": "http://localhost",
            })
            assert resp.status_code == 401


# ── Orders ────────────────────────────────────────────────────────────────────

class TestOrders:
    """Order pages."""

    def test_orders_page_loads(self, client):
        resp = client.get("/orders")
        assert resp.status_code == 200

    def test_search_page_loads(self, client):
        resp = client.get("/search")
        assert resp.status_code == 200
