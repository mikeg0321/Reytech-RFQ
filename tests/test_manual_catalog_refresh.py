"""Tests for manual catalog refresh endpoints.

POST /api/catalog/refresh-for-pc/<pcid>
POST /api/catalog/refresh-for-rfq/<rid>

These exist so operators can unblock QA-gated records (PCs/RFQs with
missing cost data) without re-parsing. The refresh runs async — the
endpoint returns immediately with the item count; prices settle in the
background.
"""
import pytest


class TestRefreshForPc:

    def test_returns_404_for_missing_pc(self, client):
        r = client.post("/api/catalog/refresh-for-pc/does-not-exist")
        assert r.status_code == 404
        body = r.get_json()
        assert body["ok"] is False

    def test_returns_400_for_pc_with_no_items(self, client, temp_data_dir):
        # Seed an empty PC
        import os, json
        pc_id = "empty_pc_test"
        path = os.path.join(temp_data_dir, "price_checks.json")
        with open(path, "w") as f:
            json.dump({pc_id: {"id": pc_id, "items": [], "pc_number": "EMPTY"}}, f)

        r = client.post(f"/api/catalog/refresh-for-pc/{pc_id}")
        assert r.status_code == 400
        body = r.get_json()
        assert body["ok"] is False
        assert "no items" in body["error"].lower()

    def test_happy_path_fires_async_refresh(self, client, seed_pc, monkeypatch):
        # Spy on the async call; verify the endpoint returns immediately
        # with the correct item count + doesn't block
        fired = {"count": 0, "items_len": 0, "context": None}

        def _spy(items, max_age_days=7, context="pc_parse"):
            fired["count"] += 1
            fired["items_len"] = len(items or [])
            fired["context"] = context

        import src.agents.product_catalog as _pc
        monkeypatch.setattr(_pc, "refresh_prices_for_items_async", _spy)

        r = client.post(f"/api/catalog/refresh-for-pc/{seed_pc}")
        assert r.status_code == 200, r.data
        body = r.get_json()
        assert body["ok"] is True
        assert body["items"] >= 1
        assert body["context"].startswith("manual_pc_")
        assert fired["count"] == 1
        assert fired["items_len"] >= 1
        assert fired["context"].startswith("manual_pc_")


class TestRefreshForRfq:

    def test_returns_404_for_missing_rfq(self, client):
        r = client.post("/api/catalog/refresh-for-rfq/does-not-exist")
        assert r.status_code == 404
        body = r.get_json()
        assert body["ok"] is False

    def test_happy_path_fires_async_refresh(self, client, seed_rfq, monkeypatch):
        fired = {"count": 0, "items_len": 0}

        def _spy(items, max_age_days=7, context="rfq_parse"):
            fired["count"] += 1
            fired["items_len"] = len(items or [])

        import src.agents.product_catalog as _pc
        monkeypatch.setattr(_pc, "refresh_prices_for_items_async", _spy)

        r = client.post(f"/api/catalog/refresh-for-rfq/{seed_rfq}")
        assert r.status_code == 200, r.data
        body = r.get_json()
        assert body["ok"] is True
        assert body["items"] >= 1
        assert body["context"].startswith("manual_rfq_")
        assert fired["count"] == 1
