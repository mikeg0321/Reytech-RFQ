"""
E14: Integration tests for critical paths
Run: python -m pytest tests/test_critical_paths.py -v
"""
import json
import os
import sys
import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestHealthCheck:
    """Health endpoint should work without auth."""
    def test_health_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["status"] in ("ok", "degraded")
        assert "db" in data
        assert "disk" in data


class TestAuth:
    """All protected routes should require authentication."""
    def test_home_requires_auth(self, client):
        resp = client.get("/")
        assert resp.status_code == 401

    def test_home_with_auth(self, auth_client):
        resp = auth_client.get("/")
        assert resp.status_code == 200

    def test_analytics_requires_auth(self, client):
        resp = client.get("/analytics")
        assert resp.status_code == 401

    def test_settings_requires_auth(self, client):
        resp = client.get("/settings")
        assert resp.status_code == 401


class TestAPIv1:
    """API v1 endpoints should work with auth."""
    def test_list_rfqs(self, auth_client):
        resp = auth_client.get("/api/v1/rfqs")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["ok"] is True
        assert "rfqs" in data

    def test_stats(self, auth_client):
        resp = auth_client.get("/api/v1/stats")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert "rfqs" in data
        assert "pcs" in data

    def test_api_without_auth(self, client):
        resp = client.get("/api/v1/rfqs")
        assert resp.status_code == 401


class TestBulkActions:
    """Bulk action endpoint should validate input."""
    def test_bulk_missing_params(self, auth_client):
        resp = auth_client.post("/api/bulk/action",
            data=json.dumps({}),
            content_type="application/json")
        assert resp.status_code == 400

    def test_bulk_dismiss(self, auth_client):
        resp = auth_client.post("/api/bulk/action",
            data=json.dumps({"ids": ["nonexistent"], "action": "dismiss", "type": "rfq"}),
            content_type="application/json")
        data = json.loads(resp.data)
        assert data["ok"] is True


class TestSettings:
    """Settings should persist."""
    def test_save_setting(self, auth_client):
        resp = auth_client.post("/api/settings",
            data=json.dumps({"pricing.default_markup_pct": "25"}),
            content_type="application/json")
        data = json.loads(resp.data)
        assert data["ok"] is True
        assert data["saved"] == 1

    def test_read_settings(self, auth_client):
        resp = auth_client.get("/api/settings/data")
        data = json.loads(resp.data)
        assert data["ok"] is True
        assert "pricing.default_markup_pct" in data["settings"]


class TestMarginOptimizer:
    """Margin recommendations should work."""
    def test_scprs_undercut(self):
        """If SCPRS exists, recommend 2% undercut."""
        from src.api.modules.routes_analytics import _compute_recommended_price
        item = {"scprs_last_price": 100.00}
        rec = _compute_recommended_price(item)
        assert rec is not None
        assert rec["price"] == 98.00
        assert rec["confidence"] == "high"

    def test_amazon_markup(self):
        """If only Amazon, recommend 20% markup."""
        from src.api.modules.routes_analytics import _compute_recommended_price
        item = {"amazon_price": 50.00}
        rec = _compute_recommended_price(item)
        assert rec is not None
        assert rec["price"] == 60.00
        assert rec["confidence"] == "medium"

    def test_cost_markup(self):
        """If only cost, recommend 25% markup."""
        from src.api.modules.routes_analytics import _compute_recommended_price
        item = {"supplier_cost": 40.00}
        rec = _compute_recommended_price(item)
        assert rec is not None
        assert rec["price"] == 50.00
        assert rec["confidence"] == "low"

    def test_no_data_returns_none(self):
        """No pricing data returns None."""
        from src.api.modules.routes_analytics import _compute_recommended_price
        rec = _compute_recommended_price({})
        assert rec is None

    def test_scprs_floor_protection(self):
        """SCPRS undercut shouldn't go below cost + 5%."""
        from src.api.modules.routes_analytics import _compute_recommended_price
        item = {"scprs_last_price": 10.00, "supplier_cost": 9.80}
        rec = _compute_recommended_price(item)
        assert rec["price"] >= 9.80 * 1.05  # Minimum 5% above cost


class TestDuplicateDetection:
    """Duplicate/amendment detection."""
    def test_diff_line_items(self):
        from src.api.modules.routes_analytics import _diff_line_items
        old = [{"description": "Widget A"}, {"description": "Widget B"}]
        new = [{"description": "Widget A"}, {"description": "Widget C"}]
        diff = _diff_line_items(old, new)
        assert diff["changed"] is True
        assert len(diff["added"]) == 1
        assert len(diff["removed"]) == 1
        assert diff["unchanged"] == 1


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def app():
    """Create test Flask app."""
    os.environ["DASH_USER"] = "test"
    os.environ["DASH_PASS"] = "test"
    os.environ["REYTECH_DATA_DIR"] = "/tmp/reytech_test"
    os.makedirs("/tmp/reytech_test", exist_ok=True)
    
    from app import create_app
    app = create_app()
    app.config["TESTING"] = True
    yield app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def auth_client(app):
    c = app.test_client()
    import base64
    creds = base64.b64encode(b"test:test").decode()
    c.environ_base["HTTP_AUTHORIZATION"] = f"Basic {creds}"
    return c
