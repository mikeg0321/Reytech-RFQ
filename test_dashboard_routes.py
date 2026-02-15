"""
Integration tests for dashboard.py Flask routes.

Uses HTTP Basic Auth (DASH_USER/DASH_PASS env vars).
Tests every user-facing endpoint and button action.
"""
import pytest
import json
import os
import base64


def _auth_headers(user="reytech", pw="changeme"):
    creds = base64.b64encode(f"{user}:{pw}".encode()).decode()
    return {"Authorization": f"Basic {creds}"}


# ═══════════════════════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════════════════════

class TestAuth:

    def test_unauthenticated_returns_401(self, anon_client):
        r = anon_client.get("/")
        assert r.status_code == 401

    def test_wrong_password_401(self, anon_client):
        r = anon_client.get("/", headers=_auth_headers("bad", "wrong"))
        assert r.status_code == 401

    def test_correct_auth_200(self, client):
        r = client.get("/")
        assert r.status_code == 200


# ═══════════════════════════════════════════════════════════════════════════════
# HOME PAGE
# ═══════════════════════════════════════════════════════════════════════════════

class TestHomePage:

    def test_loads(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert b"Reytech" in r.data

    def test_has_upload_form(self, client):
        r = client.get("/")
        assert b"upload" in r.data.lower()

    def test_has_quotes_nav(self, client):
        r = client.get("/")
        assert b"/quotes" in r.data or b"Quotes" in r.data


# ═══════════════════════════════════════════════════════════════════════════════
# API HEALTH
# ═══════════════════════════════════════════════════════════════════════════════

class TestAPIHealth:

    def test_health(self, client):
        r = client.get("/api/health")
        assert r.status_code == 200
        d = r.get_json()
        assert d["status"] in ("ok", "degraded")  # degraded OK in test mode

    def test_status(self, client):
        r = client.get("/api/status")
        assert r.status_code == 200


# ═══════════════════════════════════════════════════════════════════════════════
# PRICE CHECK ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

class TestPriceCheckRoutes:

    def test_detail_page(self, client, seed_pc, sample_pc):
        r = client.get(f"/pricecheck/{seed_pc}")
        assert r.status_code == 200
        assert b"OS - Den - Feb" in r.data
        assert b"CSP-Sacramento" in r.data

    def test_detail_shows_asin(self, client, seed_pc, sample_pc):
        r = client.get(f"/pricecheck/{seed_pc}")
        assert b"B07TEST123" in r.data

    def test_save_prices(self, client, seed_pc):
        r = client.post(f"/pricecheck/{seed_pc}/save-prices",
                        json={"price_0": 15.72, "cost_0": 12.58,
                              "markup_0": 25, "qty_0": 22,
                              "tax_enabled": False},
                        content_type="application/json")
        assert r.status_code == 200
        assert r.get_json()["ok"] is True

    def test_generate_quote(self, client, seed_pc):
        r = client.get(f"/pricecheck/{seed_pc}/generate-quote")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert "quote_number" in d

    def test_nonexistent_pc(self, client):
        r = client.get("/pricecheck/no-such-id")
        # Should redirect or show error — not crash
        assert r.status_code in (200, 302, 404)


# ═══════════════════════════════════════════════════════════════════════════════
# RFQ ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

class TestRFQRoutes:

    def test_detail_page(self, client, seed_rfq, sample_rfq):
        r = client.get(f"/rfq/{seed_rfq}")
        assert r.status_code == 200
        assert b"RFQ-2026-TEST" in r.data

    def test_detail_has_upgraded_columns(self, client, seed_rfq):
        r = client.get(f"/rfq/{seed_rfq}")
        html = r.data.decode()
        for col in ("Your Cost", "SCPRS", "Amazon", "Bid Price", "Margin", "Profit"):
            assert col in html, f"Missing column: {col}"

    def test_update_pricing(self, client, seed_rfq):
        r = client.post(f"/rfq/{seed_rfq}/update",
                        data={"cost_0": "350.00", "price_0": "454.40"},
                        follow_redirects=True)
        assert r.status_code == 200

    def test_delete(self, client, seed_rfq, temp_data_dir):
        r = client.post(f"/rfq/{seed_rfq}/delete", follow_redirects=True)
        assert r.status_code == 200
        # RFQ should be gone
        with open(os.path.join(temp_data_dir, "rfqs.json")) as f:
            rfqs = json.load(f)
        assert seed_rfq not in rfqs

    def test_generate_reytech_quote(self, client, seed_rfq):
        r = client.get(f"/rfq/{seed_rfq}/generate-quote", follow_redirects=True)
        assert r.status_code == 200


# ═══════════════════════════════════════════════════════════════════════════════
# QUOTES PAGE
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuotesPage:

    def test_loads(self, client):
        r = client.get("/quotes")
        assert r.status_code == 200

    def test_has_search(self, client):
        r = client.get("/quotes")
        html = r.data.decode().lower()
        assert "search" in html

    def test_agency_filter(self, client):
        r = client.get("/quotes?agency=CDCR")
        assert r.status_code == 200

    def test_status_filter(self, client):
        r = client.get("/quotes?status=pending")
        assert r.status_code == 200

    def test_status_filter_won(self, client):
        r = client.get("/quotes?status=won")
        assert r.status_code == 200

    def test_has_logo_upload(self, client):
        r = client.get("/quotes")
        assert b"upload-logo" in r.data

    def test_has_status_column(self, client):
        r = client.get("/quotes")
        html = r.data.decode()
        assert "Status" in html

    def test_has_win_rate_stats(self, client):
        r = client.get("/quotes")
        html = r.data.decode()
        assert "Win Rate" in html

    def test_has_mark_buttons(self, client, seed_pc):
        # Generate a quote first so there's a row
        client.get(f"/pricecheck/{seed_pc}/generate-quote")
        r = client.get("/quotes")
        html = r.data.decode()
        assert "markQuote" in html


# ═══════════════════════════════════════════════════════════════════════════════
# WIN/LOSS STATUS API
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuoteStatusAPI:

    def test_mark_won(self, client, seed_pc):
        r = client.get(f"/pricecheck/{seed_pc}/generate-quote")
        qn = r.get_json()["quote_number"]
        r2 = client.post(f"/quotes/{qn}/status",
                         json={"status": "won", "po_number": "PO-TEST-001"},
                         content_type="application/json")
        assert r2.status_code == 200
        assert r2.get_json()["ok"] is True

    def test_mark_lost(self, client, seed_pc):
        r = client.get(f"/pricecheck/{seed_pc}/generate-quote")
        qn = r.get_json()["quote_number"]
        r2 = client.post(f"/quotes/{qn}/status",
                         json={"status": "lost"},
                         content_type="application/json")
        assert r2.get_json()["ok"] is True

    def test_invalid_status_rejected(self, client, seed_pc):
        r = client.get(f"/pricecheck/{seed_pc}/generate-quote")
        qn = r.get_json()["quote_number"]
        r2 = client.post(f"/quotes/{qn}/status",
                         json={"status": "bogus"},
                         content_type="application/json")
        assert r2.get_json()["ok"] is False

    def test_nonexistent_quote(self, client):
        r = client.post("/quotes/FAKE999/status",
                        json={"status": "won"},
                        content_type="application/json")
        assert r.get_json()["ok"] is False


# ═══════════════════════════════════════════════════════════════════════════════
# SCPRS API
# ═══════════════════════════════════════════════════════════════════════════════

class TestSCPRSRoutes:

    def test_won_quotes_stats(self, client):
        r = client.get("/api/won-quotes/stats")
        assert r.status_code == 200
        d = r.get_json()
        assert isinstance(d, dict)

    def test_won_quotes_search(self, client):
        r = client.get("/api/won-quotes/search?q=stryker")
        assert r.status_code == 200
