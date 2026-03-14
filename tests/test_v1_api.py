"""Tests for /api/v1/ endpoints."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src.core.db import init_db
from src.core.dal import save_rfq, save_pc, save_order


@pytest.fixture
def app():
    os.environ["SECRET_KEY"] = "test-secret"
    os.environ["API_KEY"] = "test-key"
    os.environ["DASH_USER"] = "testuser"
    os.environ["DASH_PASS"] = "testpass"
    from app import create_app
    app = create_app()
    app.config["TESTING"] = True
    init_db()
    return app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def headers():
    return {"X-API-Key": "test-key"}


class TestV1GetRFQ:
    def test_get_existing(self, client, headers):
        save_rfq({"id": "V1R1", "status": "new", "received_at": "2026-01-01",
                  "agency": "CDCR", "items": [{"desc": "Gloves"}]})
        resp = client.get("/api/v1/rfq/V1R1", headers=headers)
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert data["data"]["agency"] == "CDCR"

    def test_get_not_found(self, client, headers):
        resp = client.get("/api/v1/rfq/NONEXISTENT", headers=headers)
        assert resp.status_code == 404
        assert resp.get_json()["ok"] is False


class TestV1PriceRFQ:
    def test_price_existing(self, client, headers):
        save_rfq({"id": "V1P1", "status": "new", "received_at": "2026-01-01"})
        resp = client.post("/api/v1/rfq/V1P1/price", headers=headers,
                           json={"force": True})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert data["data"]["rfq_id"] == "V1P1"

    def test_price_not_found(self, client, headers):
        resp = client.post("/api/v1/rfq/NOPE/price", headers=headers)
        assert resp.status_code == 404


class TestV1Pipeline:
    def test_pipeline_returns_counts(self, client, headers):
        save_rfq({"id": "PIP1", "status": "new", "received_at": "2026-01-01"})
        save_pc({"id": "PIPC1", "status": "parsed", "created_at": "2026-01-01"})
        save_order({"id": "PIPO1", "status": "new", "created_at": "2026-01-01"})
        resp = client.get("/api/v1/pipeline", headers=headers)
        assert resp.status_code == 200
        data = resp.get_json()["data"]
        assert "rfqs" in data
        assert "pcs" in data
        assert "orders" in data
        assert "agents" in data
