"""Route-level smoke tests for modules with zero test coverage.

These catch silent crashes, missing imports, undefined variables, and
broken template rendering. Each test hits a route and asserts it doesn't
500. Not exhaustive — just the safety net that was missing.

Coverage targets:
- routes_crm.py (91 routes, 4464 lines, 0 prior tests)
- routes_catalog_finance.py (66 routes, 3533 lines, 0 prior tests)
- routes_voice_contacts.py (19 routes, 1012 lines, 0 prior tests)
"""

import pytest


# ═══════════════════════════════════════════════════════════════════════
# CRM / Core API Routes
# ═══════════════════════════════════════════════════════════════════════

class TestCRMCoreAPI:
    """Smoke tests for /api/* routes in routes_crm.py."""

    def test_health(self, client):
        r = client.get("/api/health")
        assert r.status_code == 200

    def test_build(self, client):
        r = client.get("/api/build")
        assert r.status_code == 200

    @pytest.mark.xfail(reason="Pre-existing: _rate_limiter NameError in exec'd module context")
    def test_metrics(self, client):
        r = client.get("/api/metrics")
        assert r.status_code == 200

    def test_db_info(self, client):
        r = client.get("/api/db")
        assert r.status_code == 200

    def test_customers_list(self, client):
        r = client.get("/api/customers")
        assert r.status_code == 200
        body = r.get_json()
        assert isinstance(body, (list, dict))

    def test_customers_hierarchy(self, client):
        r = client.get("/api/customers/hierarchy")
        assert r.status_code == 200

    def test_customers_match(self, client):
        r = client.get("/api/customers/match?q=Sacramento")
        assert r.status_code == 200

    def test_quote_counter(self, client):
        r = client.get("/api/quotes/counter")
        assert r.status_code == 200

    def test_quote_history(self, client):
        r = client.get("/api/quotes/history")
        assert r.status_code == 200

    def test_search_empty(self, client):
        r = client.get("/api/search?q=test")
        assert r.status_code == 200

    def test_tax_rate(self, client):
        r = client.get("/api/tax-rate?zip=95814")
        assert r.status_code == 200

    def test_supplier_profiles(self, client):
        r = client.get("/api/supplier-profiles")
        assert r.status_code == 200

    def test_research_status(self, client):
        r = client.get("/api/research/status")
        assert r.status_code == 200

    def test_research_cache_stats(self, client):
        r = client.get("/api/research/cache-stats")
        assert r.status_code == 200

    def test_debug_env_check(self, client):
        r = client.get("/api/debug/env-check")
        assert r.status_code == 200

    def test_notify_status(self, client):
        r = client.get("/api/notify/status")
        assert r.status_code == 200

    def test_email_log(self, client):
        r = client.get("/api/email-log")
        assert r.status_code == 200


class TestCRMAuthGates:
    """Verify auth is required on sensitive CRM endpoints."""

    def test_customers_requires_auth(self, anon_client):
        r = anon_client.get("/api/customers")
        assert r.status_code == 401

    def test_quote_counter_requires_auth(self, anon_client):
        r = anon_client.get("/api/quotes/counter")
        assert r.status_code == 401

    def test_db_requires_auth(self, anon_client):
        r = anon_client.get("/api/db")
        assert r.status_code == 401


# ═══════════════════════════════════════════════════════════════════════
# Catalog & Finance Routes
# ═══════════════════════════════════════════════════════════════════════

class TestCatalogPages:
    """Smoke tests for catalog page routes."""

    def test_catalog_page_loads(self, client):
        r = client.get("/catalog")
        assert r.status_code == 200
        assert b"Catalog" in r.data or b"catalog" in r.data

    def test_shipping_page_loads(self, client):
        r = client.get("/shipping")
        assert r.status_code == 200


class TestCatalogAPI:
    """Smoke tests for /api/catalog/* routes."""

    def test_catalog_lookup(self, client):
        r = client.get("/api/catalog/lookup?q=marker")
        assert r.status_code == 200

    def test_product_search(self, client):
        r = client.get("/api/products/search?q=pen")
        assert r.status_code == 200

    def test_catalog_opportunities(self, client):
        r = client.get("/api/catalog/opportunities")
        assert r.status_code == 200

    def test_catalog_audit_db(self, client):
        r = client.get("/api/catalog/audit/db")
        assert r.status_code == 200

    def test_orders_aging(self, client):
        r = client.get("/api/orders/aging")
        assert r.status_code == 200

    def test_orders_margins(self, client):
        r = client.get("/api/orders/margins")
        assert r.status_code == 200

    def test_orders_recurring(self, client):
        r = client.get("/api/orders/recurring")
        assert r.status_code == 200


class TestCatalogAuthGates:
    def test_catalog_requires_auth(self, anon_client):
        r = anon_client.get("/catalog")
        assert r.status_code == 401

    def test_catalog_lookup_requires_auth(self, anon_client):
        r = anon_client.get("/api/catalog/lookup?q=test")
        assert r.status_code == 401


# ═══════════════════════════════════════════════════════════════════════
# Voice & Contacts Routes
# ═══════════════════════════════════════════════════════════════════════

class TestContactsPages:
    """Smoke tests for contacts/campaigns pages."""

    def test_contacts_page_loads(self, client):
        r = client.get("/contacts")
        assert r.status_code == 200

    def test_crm_redirects_to_contacts(self, client):
        r = client.get("/crm", follow_redirects=False)
        assert r.status_code in (301, 302, 303)
        assert "/contacts" in r.headers.get("Location", "")

    def test_campaigns_page_loads(self, client):
        r = client.get("/campaigns")
        assert r.status_code == 200

    def test_intelligence_page_loads(self, client):
        r = client.get("/intelligence", follow_redirects=True)
        assert r.status_code == 200


class TestVoiceAPI:
    """Smoke tests for /api/voice/* routes."""

    def test_voice_status(self, client):
        r = client.get("/api/voice/status")
        assert r.status_code == 200

    def test_voice_scripts(self, client):
        r = client.get("/api/voice/scripts")
        assert r.status_code == 200

    def test_voice_log(self, client):
        r = client.get("/api/voice/log")
        assert r.status_code == 200

    def test_campaigns_list(self, client):
        r = client.get("/api/campaigns")
        assert r.status_code == 200

    def test_campaigns_stats(self, client):
        r = client.get("/api/campaigns/stats")
        assert r.status_code == 200


class TestVoiceAuthGates:
    def test_contacts_requires_auth(self, anon_client):
        r = anon_client.get("/contacts")
        assert r.status_code == 401

    def test_voice_status_requires_auth(self, anon_client):
        r = anon_client.get("/api/voice/status")
        assert r.status_code == 401

    def test_campaigns_requires_auth(self, anon_client):
        r = anon_client.get("/api/campaigns")
        assert r.status_code == 401
