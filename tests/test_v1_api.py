"""Tests for /api/v1/ endpoints."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src.core.db import init_db
# Canonical writers — legacy core.dal.save_* stubs deleted 2026-04-30
# (V1 DAL audit drift #1). _save_single_* takes (id, data); save_order
# takes (order_id, order, actor).
from src.api.data_layer import _save_single_rfq, _save_single_pc
from src.core.order_dal import save_order as _save_order


def save_rfq(data):
    _save_single_rfq(data["id"], data)


def save_pc(data):
    _save_single_pc(data["id"], data)


def save_order(data):
    _save_order(data["id"], data, actor="test")


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "test-secret")
    monkeypatch.setenv("API_KEY", "test-key")
    monkeypatch.setenv("DASH_USER", "testuser")
    monkeypatch.setenv("DASH_PASS", "testpass")
    # Patch module-level constants that were cached at import time
    from src.api import shared
    monkeypatch.setattr(shared, "API_KEY", "test-key")
    monkeypatch.setattr(shared, "DASH_USER", "testuser")
    monkeypatch.setattr(shared, "DASH_PASS", "testpass")
    monkeypatch.setattr(shared, "check_auth",
                        lambda u, p: u == "testuser" and p == "testpass")
    monkeypatch.setattr(shared, "_check_rate_limit", lambda *a, **kw: True)
    from app import create_app
    app = create_app()
    app.config["TESTING"] = True
    init_db()
    return app


@pytest.fixture
def client(app):
    with app.test_client() as c:
        yield c


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


class TestV1CreateRFQ:
    def test_create_via_json(self, client, headers):
        resp = client.post("/api/v1/rfq/create", headers=headers,
                           json={"solicitation_number": "TEST123", "agency": "CDCR",
                                 "requestor_name": "Test Buyer",
                                 "items": [{"description": "Gloves", "qty": 10, "uom": "BX"}]})
        assert resp.status_code == 201
        data = resp.get_json()
        assert data["ok"] is True
        assert "id" in data["data"]
        assert data["data"]["status"] == "new"
