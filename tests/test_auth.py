"""Tests for X-API-Key authentication."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest


@pytest.fixture
def app():
    """Create test Flask app."""
    os.environ["SECRET_KEY"] = "test-secret"
    os.environ["API_KEY"] = "test-api-key-123"
    os.environ["DASH_USER"] = "testuser"
    os.environ["DASH_PASS"] = "testpass"
    from app import create_app
    app = create_app()
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(app):
    return app.test_client()


class TestAPIKeyAuth:
    def test_valid_api_key(self, client):
        resp = client.get("/api/health", headers={"X-API-Key": "test-api-key-123"})
        assert resp.status_code == 200

    def test_invalid_api_key(self, client):
        resp = client.get("/api/health", headers={"X-API-Key": "wrong-key"})
        assert resp.status_code == 401
        data = resp.get_json()
        assert data["ok"] is False
        assert "Invalid API key" in data["error"]

    def test_missing_key_falls_through_to_basic(self, client):
        # No key, no basic auth → 401 with Basic challenge
        resp = client.get("/api/health")
        assert resp.status_code == 401
        assert "Basic" in resp.headers.get("WWW-Authenticate", "")

    def test_basic_auth_still_works(self, client):
        from base64 import b64encode
        creds = b64encode(b"testuser:testpass").decode()
        resp = client.get("/api/health", headers={"Authorization": f"Basic {creds}"})
        assert resp.status_code == 200
