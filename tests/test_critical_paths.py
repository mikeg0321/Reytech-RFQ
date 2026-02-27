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
    def test_home_requires_auth(self, anon_client):
        resp = anon_client.get("/")
        assert resp.status_code == 401

    def test_home_with_auth(self, auth_client):
        resp = auth_client.get("/")
        assert resp.status_code == 200

    def test_analytics_requires_auth(self, anon_client):
        resp = anon_client.get("/analytics")
        assert resp.status_code == 401

    def test_settings_requires_auth(self, anon_client):
        resp = anon_client.get("/settings")
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

    def test_api_without_auth(self, anon_client):
        resp = anon_client.get("/api/v1/rfqs")
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
    def _get_fn(self, app):
        """Get _compute_recommended_price from loaded dashboard globals."""
        import src.api.dashboard as dash
        fn = getattr(dash, '_compute_recommended_price', None)
        if fn is None:
            pytest.skip("_compute_recommended_price not in dashboard globals (module load issue)")
        return fn

    def test_scprs_undercut(self, app):
        """If SCPRS exists, recommend 2% undercut."""
        fn = self._get_fn(app)
        item = {"scprs_last_price": 100.00}
        rec = fn(item)
        assert rec is not None
        assert rec["price"] == 98.00
        assert rec["confidence"] == "high"

    def test_amazon_markup(self, app):
        """If only Amazon, recommend 20% markup."""
        fn = self._get_fn(app)
        item = {"amazon_price": 50.00}
        rec = fn(item)
        assert rec is not None
        assert rec["price"] == 60.00
        assert rec["confidence"] == "medium"

    def test_cost_markup(self, app):
        """If only cost, recommend 25% markup."""
        fn = self._get_fn(app)
        item = {"supplier_cost": 40.00}
        rec = fn(item)
        assert rec is not None
        assert rec["price"] == 50.00
        assert rec["confidence"] == "low"

    def test_no_data_returns_none(self, app):
        """No pricing data returns None."""
        fn = self._get_fn(app)
        rec = fn({})
        assert rec is None

    def test_scprs_floor_protection(self, app):
        """SCPRS undercut shouldn't go below cost + 5%."""
        fn = self._get_fn(app)
        item = {"scprs_last_price": 10.00, "supplier_cost": 9.80}
        rec = fn(item)
        assert rec["price"] >= 9.80 * 1.05  # Minimum 5% above cost


class TestDuplicateDetection:
    """Duplicate/amendment detection."""
    def test_diff_line_items(self, app):
        import src.api.dashboard as dash
        fn = getattr(dash, '_diff_line_items', None)
        if fn is None:
            pytest.skip("_diff_line_items not in dashboard globals")
        old = [{"description": "Widget A"}, {"description": "Widget B"}]
        new = [{"description": "Widget A"}, {"description": "Widget C"}]
        diff = fn(old, new)
        assert diff["changed"] is True
        assert len(diff["added"]) == 1
        assert len(diff["removed"]) == 1
        assert diff["unchanged"] == 1


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def app(monkeypatch):
    """Create test Flask app."""
    monkeypatch.setenv("DASH_USER", "test")
    monkeypatch.setenv("DASH_PASS", "test")
    monkeypatch.setenv("SECRET_KEY", "test-secret-key")
    monkeypatch.setenv("REYTECH_DATA_DIR", "/tmp/reytech_test")
    os.makedirs("/tmp/reytech_test", exist_ok=True)
    
    from app import create_app
    app = create_app()
    app.config["TESTING"] = True
    
    # Patch dashboard module-level auth vars + rate limiter
    try:
        import src.api.dashboard as dash
        monkeypatch.setattr(dash, "DASH_USER", "test")
        monkeypatch.setattr(dash, "DASH_PASS", "test")
        monkeypatch.setattr(dash, "_check_rate_limit", lambda *a, **kw: True)
        monkeypatch.setattr(dash, "check_auth",
                            lambda u, p: u == "test" and p == "test")
    except Exception:
        pass
    # Also patch shared.py (auth guard lives there now)
    try:
        import src.api.shared as shared
        monkeypatch.setattr(shared, "DASH_USER", "test")
        monkeypatch.setattr(shared, "DASH_PASS", "test")
        monkeypatch.setattr(shared, "_check_rate_limit", lambda *a, **kw: True)
        monkeypatch.setattr(shared, "check_auth",
                            lambda u, p: u == "test" and p == "test")
    except Exception:
        pass
    
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


@pytest.fixture
def anon_client(app):
    return app.test_client()
