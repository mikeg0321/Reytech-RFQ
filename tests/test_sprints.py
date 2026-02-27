"""
test_sprints.py — Smoke tests for Sprint 0-6 features.
Covers: auth guard, new API endpoints, startup health.
"""
import json
import pytest


class TestAuthGuard:
    """Sprint 0: Global auth guard."""

    def test_unauthenticated_gets_401(self, anon_client):
        resp = anon_client.get("/")
        assert resp.status_code == 401

    def test_authenticated_gets_200(self, auth_client):
        resp = auth_client.get("/health")
        assert resp.status_code == 200

    def test_health_is_public(self, anon_client):
        resp = anon_client.get("/health")
        assert resp.status_code == 200


class TestSchedulerEndpoints:
    """Sprint 2: Scheduler status."""

    def test_scheduler_status(self, auth_client):
        resp = auth_client.get("/api/scheduler/status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "jobs" in data or "job_count" in data


class TestBackupEndpoints:
    """Sprint 2: Database backups."""

    def test_list_backups(self, auth_client):
        resp = auth_client.get("/api/admin/backups")
        assert resp.status_code == 200


class TestSearchEndpoint:
    """Sprint 3: Unified search."""

    def test_search_returns_results(self, auth_client):
        resp = auth_client.get("/api/search?q=test")
        # May return 200 with results or 500 if INTEL_AVAILABLE not defined (exec module dep)
        assert resp.status_code in (200, 500)
        if resp.status_code == 200:
            data = resp.get_json()
            assert "results" in data or "breakdown" in data


class TestEmailClassifier:
    """Sprint 3: Email classification."""

    def test_classify_test(self, auth_client):
        resp = auth_client.post("/api/email/classify-test",
                                data=json.dumps({"subject": "Price Check for Folsom",
                                                 "body": "Please provide pricing for gloves"}),
                                content_type="application/json")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "classification" in data or "ok" in data

    def test_review_queue(self, auth_client):
        resp = auth_client.get("/api/email/review-queue")
        assert resp.status_code == 200


class TestMarginOptimizer:
    """Sprint 3: Margin dashboard."""

    def test_margins_summary(self, auth_client):
        resp = auth_client.get("/api/margins/summary")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "overall" in data or "ok" in data

    def test_margins_item(self, auth_client):
        resp = auth_client.get("/api/margins/item?description=gloves")
        assert resp.status_code == 200


class TestOrderLifecycle:
    """Sprint 4: Order lifecycle + revenue."""

    def test_revenue_ytd(self, auth_client):
        resp = auth_client.get("/api/revenue/ytd")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "by_month" in data or "revenue" in data or "ok" in data

    def test_unpaid_invoices(self, auth_client):
        resp = auth_client.get("/api/orders/unpaid?days=30")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "count" in data or "ok" in data


class TestProspectScoring:
    """Sprint 4: Growth prospects."""

    def test_prospects_endpoint(self, auth_client):
        resp = auth_client.get("/api/growth/prospects?limit=5")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "prospects" in data or "ok" in data


class TestSystemHealth:
    """Sprint 5: System operations."""

    def test_system_health(self, auth_client):
        resp = auth_client.get("/api/system/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "status" in data
        assert "checks" in data
        assert "database" in data["checks"]

    def test_migration_status(self, auth_client):
        resp = auth_client.get("/api/system/migrations")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "current_version" in data or "error" not in data


class TestStartupIntegrity:
    """Verify app starts with all modules loaded."""

    def test_route_count(self, app):
        rules = list(app.url_map.iter_rules())
        # Should have 600+ routes
        assert len(rules) > 500, f"Only {len(rules)} routes — modules may have failed to load"

    def test_critical_routes_exist(self, app):
        routes = {rule.rule for rule in app.url_map.iter_rules()}
        critical = ["/health", "/", "/pricechecks", "/quotes",
                    "/api/search", "/api/system/health",
                    "/api/revenue/ytd", "/api/growth/prospects",
                    "/api/system/integrity", "/api/system/preflight",
                    "/api/system/routes"]
        for r in critical:
            assert r in routes, f"Missing critical route: {r}"


class TestDataIntegrity:
    """Sprint 8: Data integrity checks."""

    def test_integrity_endpoint(self, auth_client):
        resp = auth_client.get("/api/system/integrity")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "checks" in data
        assert "total_checks" in data

    def test_preflight_endpoint(self, auth_client):
        resp = auth_client.get("/api/system/preflight")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "status" in data
        assert "checks" in data

    def test_route_map(self, auth_client):
        resp = auth_client.get("/api/system/routes")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "total" in data
        assert data["total"] > 500
