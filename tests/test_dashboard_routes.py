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

    def test_rt_confirm_helper_in_base(self, client):
        # ConfirmButton macro depends on window.rtConfirm being available
        # globally. Verify the helper is registered by base.html on every page.
        r = client.get("/")
        html = r.data.decode()
        assert "window.rtConfirm" in html
        assert "rt-confirm-toast" in html

    def test_mark_quote_helper_defined(self, client):
        # Regression: markQuote() was called from quote buttons but never
        # defined → silent no-op. Verify base.html now ships the helper.
        r = client.get("/")
        html = r.data.decode()
        assert "window.markQuote" in html

    def test_chartjs_self_hosted(self, client):
        # CSP fix: Chart.js must be served from /static/vendor, not jsdelivr.
        r = client.get("/")
        html = r.data.decode()
        assert "/static/vendor/chart.umd.min.js" in html
        assert "cdn.jsdelivr.net" not in html

    def test_fonts_self_hosted(self, client):
        # CSP fix: DM Sans + JetBrains Mono must come from /static/fonts,
        # not fonts.googleapis.com / fonts.gstatic.com.
        r = client.get("/")
        html = r.data.decode()
        assert "/static/fonts/fonts.css" in html
        assert "fonts.googleapis.com" not in html
        assert "fonts.gstatic.com" not in html
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
        r = client.post(f"/pricecheck/{seed_pc}/generate-quote")
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

    def test_update_get_redirects_not_405(self, client, seed_rfq):
        # Regression: GET /rfq/<id>/update used to 405 MethodNotAllowed.
        # Stray GETs should redirect to the RFQ detail page.
        r = client.get(f"/rfq/{seed_rfq}/update", follow_redirects=False)
        assert r.status_code in (301, 302, 303, 307, 308)
        assert f"/rfq/{seed_rfq}" in r.headers.get("Location", "")

    def test_qa_endpoint_returns_report(self, client, seed_rfq):
        # New endpoint reuses pc_qa_agent.run_qa via an RFQ→PC adapter.
        r = client.get(f"/api/rfq/{seed_rfq}/qa")
        assert r.status_code == 200
        body = r.get_json()
        # Either the agent returns a structured report (with issues) or
        # an explicit ok=False on a hard error — never a crash.
        assert isinstance(body, dict)
        assert "issues" in body or body.get("ok") is False

    def test_qa_endpoint_404_for_unknown_rfq(self, client):
        r = client.get("/api/rfq/rfq_does_not_exist_xyz/qa")
        assert r.status_code == 404

    def test_rfq_detail_has_qa_gate_script(self, client, seed_rfq):
        # The hard-block gate is wired via JS on rfq_detail.html.
        r = client.get(f"/rfq/{seed_rfq}")
        assert r.status_code == 200
        html = r.data.decode()
        assert "rfqQaGate" in html
        assert 'data-qa-gated="1"' in html

    def test_delete(self, client, seed_rfq, temp_data_dir):
        r = client.post(f"/rfq/{seed_rfq}/delete", follow_redirects=True)
        assert r.status_code == 200
        # RFQ should be gone — verify via route (data is in SQLite, not rfqs.json)
        r2 = client.get(f"/rfq/{seed_rfq}")
        assert r2.status_code in (302, 404) or b"not found" in r2.data.lower()

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
        # Logo upload moved to settings; quotes page should still load
        assert r.status_code == 200

    def test_has_status_column(self, client):
        r = client.get("/quotes")
        html = r.data.decode()
        assert "Status" in html

    def test_has_win_rate_stats(self, client):
        r = client.get("/quotes")
        html = r.data.decode()
        assert "Win Rate" in html

    def test_win_rate_no_double_percent(self, client):
        # Regression: stat_win_rate was rendering with literal '%%'
        r = client.get("/quotes")
        html = r.data.decode()
        assert "%%" not in html

    def test_ghost_quotes_hidden_from_list(self, client, seed_db_quote):
        # Regression: $0 + 0 items + no agency quotes (e.g. R26Q16) were polluting
        # the quotes list. They must be HIDDEN from the row list (not deleted).
        ghost_qn = "R26Q9901"
        seed_db_quote(ghost_qn, agency="", institution="", total=0.0, line_items=[])
        r = client.get("/quotes")
        assert r.status_code == 200
        html = r.data.decode()
        assert ghost_qn not in html

    def test_growth_redirects_to_growth_intel(self, client):
        # Regression: /growth used to redirect to /pipeline (wrong target).
        # Home dashboard advertises Growth Engine but /growth must land on
        # the actual Growth module page, not Pipeline.
        r = client.get("/growth", follow_redirects=False)
        assert r.status_code in (301, 302, 303, 307, 308)
        assert "/growth-intel" in r.headers.get("Location", "")

    def test_crm_redirects_to_contacts(self, client):
        # Regression: nav link to /crm must follow through to /contacts.
        r = client.get("/crm", follow_redirects=False)
        assert r.status_code in (301, 302, 303, 307, 308)
        assert "/contacts" in r.headers.get("Location", "")

    def test_real_quote_still_visible(self, client, seed_db_quote):
        # Inverse of ghost filter: a real quote with agency + total + items must
        # still render, so the filter doesn't accidentally hide everything.
        real_qn = "R26Q9902"
        seed_db_quote(real_qn, agency="CDCR", institution="CSP-Sacramento",
                      total=1234.56, line_items=[{"description": "Widget", "qty": 1, "unit_price": 1234.56}])
        r = client.get("/quotes")
        assert r.status_code == 200
        html = r.data.decode()
        assert real_qn in html

    def test_has_mark_buttons(self, client, seed_pc):
        # Generate a quote first so there's a row
        client.post(f"/pricecheck/{seed_pc}/generate-quote")
        r = client.get("/quotes")
        html = r.data.decode()
        assert "markQuote" in html


# ═══════════════════════════════════════════════════════════════════════════════
# WIN/LOSS STATUS API
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuoteStatusAPI:

    def test_mark_won(self, client, seed_pc):
        r = client.post(f"/pricecheck/{seed_pc}/generate-quote")
        qn = r.get_json()["quote_number"]
        r2 = client.post(f"/quotes/{qn}/status",
                         json={"status": "won", "po_number": "PO-TEST-001"},
                         content_type="application/json")
        assert r2.status_code == 200
        assert r2.get_json()["ok"] is True

    def test_mark_lost(self, client, seed_pc):
        r = client.post(f"/pricecheck/{seed_pc}/generate-quote")
        qn = r.get_json()["quote_number"]
        r2 = client.post(f"/quotes/{qn}/status",
                         json={"status": "lost"},
                         content_type="application/json")
        assert r2.get_json()["ok"] is True

    def test_invalid_status_rejected(self, client, seed_pc):
        r = client.post(f"/pricecheck/{seed_pc}/generate-quote")
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


# ═══════════════════════════════════════════════════════════════════════════════
# CUSTOMER CRM API
# ═══════════════════════════════════════════════════════════════════════════════

class TestCustomerCRM:

    def test_list_all(self, client):
        r = client.get("/api/customers")
        assert r.status_code == 200
        data = r.get_json()
        assert isinstance(data, list)

    def test_search(self, client):
        r = client.get("/api/customers?q=sacramento")
        assert r.status_code == 200

    def test_filter_agency(self, client):
        r = client.get("/api/customers?agency=CDCR")
        assert r.status_code == 200
        data = r.get_json()
        for c in data:
            assert c["agency"] == "CDCR"

    def test_hierarchy(self, client):
        r = client.get("/api/customers/hierarchy")
        assert r.status_code == 200
        data = r.get_json()
        assert isinstance(data, dict)

    def test_add_new(self, client):
        # Clean up any leftover from previous test runs (DB persists across sessions)
        try:
            from src.core.db import get_db
            db = get_db()
            db.execute("DELETE FROM customers WHERE display_name = 'Test Customer QA'")
            db.commit()
        except Exception:
            pass
        r = client.post("/api/customers",
                        json={"display_name": "Test Customer QA",
                              "agency": "CDCR", "city": "Test City"},
                        content_type="application/json")
        assert r.status_code == 200
        d = r.get_json()
        assert "ok" in d

    def test_add_duplicate_rejected(self, client):
        client.post("/api/customers",
                    json={"display_name": "Duplicate Test"},
                    content_type="application/json")
        r = client.post("/api/customers",
                        json={"display_name": "Duplicate Test"},
                        content_type="application/json")
        d = r.get_json()
        assert d["ok"] is False


# ═══════════════════════════════════════════════════════════════════════════════
# NAVIGATION — Home button
# ═══════════════════════════════════════════════════════════════════════════════

class TestNavigation:

    def test_home_link_on_quotes_page(self, client):
        r = client.get("/quotes")
        html = r.data.decode()
        assert '🏠 Home' in html

    def test_home_link_on_home_page(self, client):
        r = client.get("/")
        html = r.data.decode()
        assert '🏠 Home' in html

    def test_header_title_links_home(self, client):
        r = client.get("/quotes")
        html = r.data.decode()
        assert 'href="/"' in html


# ═══════════════════════════════════════════════════════════════════════════════
# PREVIEW — Should show Reytech format
# ═══════════════════════════════════════════════════════════════════════════════

class TestPreviewFormat:

    def test_preview_shows_704_format(self, client, seed_pc):
        r = client.get(f"/pricecheck/{seed_pc}")
        html = r.data.decode()
        # Preview JS should show AMS 704 Price Check format
        assert "Reytech Inc." in html
        assert "PRICE CHECK WORKSHEET" in html

    def test_preview_shows_institution(self, client, seed_pc):
        r = client.get(f"/pricecheck/{seed_pc}")
        html = r.data.decode()
        # Preview should reference institution from PC_META
        assert "PC_META.institution" in html


# ═══════════════════════════════════════════════════════════════════════════════
# CRM Match + Quote History APIs
# ═══════════════════════════════════════════════════════════════════════════════

class TestCRMMatchAPI:

    def test_exact_match(self, client):
        """Known institution returns matched=True."""
        r = client.get("/api/customers/match?q=Folsom%20State%20Prison")
        d = r.get_json()
        assert d.get("matched") is True
        assert d["customer"]["agency"] == "CDCR"

    def test_fuzzy_match(self, client):
        r = client.get("/api/customers/match?q=CSP-Sacramento")
        d = r.get_json()
        # Should match California State Prison, Sacramento
        assert d.get("matched") is True or len(d.get("candidates", [])) > 0

    def test_new_customer_flagged(self, client):
        r = client.get("/api/customers/match?q=Totally%20Unknown%20Agency")
        d = r.get_json()
        assert d.get("is_new") is True
        assert d.get("matched") is False

    def test_empty_query(self, client):
        r = client.get("/api/customers/match?q=")
        d = r.get_json()
        assert d.get("matched") is False


class TestQuoteHistoryAPI:

    def test_returns_list(self, client):
        r = client.get("/api/quotes/history?institution=CSP-Sacramento")
        assert r.status_code == 200
        d = r.get_json()
        assert isinstance(d, list)

    def test_empty_institution(self, client):
        r = client.get("/api/quotes/history?institution=")
        d = r.get_json()
        assert d == []

    def test_with_generated_quote(self, client, seed_pc):
        """After generating a quote, history should find it."""
        client.post(f"/pricecheck/{seed_pc}/generate-quote")
        r = client.get("/api/quotes/history?institution=CSP-Sacramento")
        d = r.get_json()
        assert len(d) >= 1
        assert d[0].get("status") in ("pending", "won", "lost")


class TestPCDetailCRMPanel:

    def test_crm_panel_present(self, client, seed_pc):
        r = client.get(f"/pricecheck/{seed_pc}")
        html = r.data.decode()
        assert 'id="crmPanel"' in html
        assert 'id="historyCard"' in html

    def test_add_customer_button_testid(self, client, seed_pc):
        r = client.get(f"/pricecheck/{seed_pc}")
        html = r.data.decode()
        assert 'crm-add-customer' in html

    def test_history_api_called(self, client, seed_pc):
        """CRM + History data should be embedded server-side (no JS fetch)."""
        r = client.get(f"/pricecheck/{seed_pc}")
        html = r.data.decode()
        # Server-side rendered: tax rate should be embedded, old fetch URLs gone
        assert 'cachedTaxRate' in html
        assert 'renderCRM' in html or 'renderHistory' in html
