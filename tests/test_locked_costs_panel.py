"""Tests for the locked-costs admin panel (Batch E2).

Covers:
- /admin/locked-costs renders rows + KPI tiles
- /api/admin/locked-costs returns rows with status classification
- /api/admin/locked-costs/unlock deletes the row
- /api/admin/locked-costs/extend pushes the expiry forward
- Cmd+K palette includes the panel
"""
import os
import sqlite3
from datetime import datetime, timedelta

import pytest


def _seed_lock(temp_data_dir, description, cost, supplier="", source="manual",
               expires_at=None, item_number=""):
    """Insert directly into supplier_costs."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    db = sqlite3.connect(db_path)
    db.execute(
        "INSERT OR REPLACE INTO supplier_costs "
        "(description, item_number, cost, supplier, source, source_url, "
        " confirmed_at, expires_at) "
        "VALUES (?,?,?,?,?,'',datetime('now'),?)",
        (description, item_number, cost, supplier, source, expires_at or ""),
    )
    db.commit()
    db.close()


class TestPanelPage:

    def test_page_renders_with_no_locks(self, auth_client):
        resp = auth_client.get("/admin/locked-costs")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "Locked Supplier Costs" in body
        assert "No costs are currently locked." in body

    def test_page_renders_locked_row(self, auth_client, temp_data_dir):
        future = (datetime.now() + timedelta(days=20)).isoformat()
        _seed_lock(temp_data_dir, "Vinyl Gloves M Box100", 11.99,
                   supplier="Uline", source="manual", expires_at=future,
                   item_number="VG-100M")
        resp = auth_client.get("/admin/locked-costs")
        body = resp.data.decode()
        assert "Vinyl Gloves M Box100" in body
        assert "$11.99" in body
        assert "Uline" in body
        assert "VG-100M" in body
        # "Active" pill
        assert ">Active<" in body

    def test_kpi_tiles_count_by_status(self, auth_client, temp_data_dir):
        now = datetime.now()
        _seed_lock(temp_data_dir, "Active Item", 5.0,
                   expires_at=(now + timedelta(days=20)).isoformat())
        _seed_lock(temp_data_dir, "Expiring Item", 5.0,
                   expires_at=(now + timedelta(days=3)).isoformat())
        _seed_lock(temp_data_dir, "Expired Item", 5.0,
                   expires_at=(now - timedelta(days=1)).isoformat())
        resp = auth_client.get("/api/admin/locked-costs")
        body = resp.get_json()
        assert body["ok"] is True
        assert body["summary"]["total"] == 3
        assert body["summary"]["active"] == 1
        assert body["summary"]["expiring"] == 1
        assert body["summary"]["expired"] == 1


class TestJsonApi:

    def test_status_classification(self, auth_client, temp_data_dir):
        now = datetime.now()
        _seed_lock(temp_data_dir, "A", 1.0,
                   expires_at=(now + timedelta(days=30)).isoformat())
        _seed_lock(temp_data_dir, "B", 2.0,
                   expires_at=(now + timedelta(days=2)).isoformat())
        _seed_lock(temp_data_dir, "C", 3.0,
                   expires_at=(now - timedelta(days=10)).isoformat())
        resp = auth_client.get("/api/admin/locked-costs")
        rows = {r["description"]: r for r in resp.get_json()["rows"]}
        assert rows["A"]["status"] == "active"
        assert rows["B"]["status"] == "expiring"
        assert rows["C"]["status"] == "expired"

    def test_blank_expiry_treated_as_active(self, auth_client, temp_data_dir):
        _seed_lock(temp_data_dir, "Forever", 9.0, expires_at="")
        rows = auth_client.get("/api/admin/locked-costs").get_json()["rows"]
        assert any(r["description"] == "Forever" and r["status"] == "active" for r in rows)


class TestUnlock:

    def test_unlock_deletes_row(self, auth_client, temp_data_dir):
        _seed_lock(temp_data_dir, "DeleteMe", 7.50, supplier="Amazon")
        resp = auth_client.post("/api/admin/locked-costs/unlock", json={
            "description": "DeleteMe", "supplier": "Amazon",
        })
        assert resp.status_code == 200
        assert resp.get_json()["deleted"] == 1
        # verify gone
        rows = auth_client.get("/api/admin/locked-costs").get_json()["rows"]
        assert not any(r["description"] == "DeleteMe" for r in rows)

    def test_unlock_requires_description(self, auth_client):
        resp = auth_client.post("/api/admin/locked-costs/unlock", json={"supplier": "x"})
        assert resp.status_code == 400


class TestExtend:

    def test_extend_pushes_expiry(self, auth_client, temp_data_dir):
        soon = (datetime.now() + timedelta(days=2)).isoformat()
        _seed_lock(temp_data_dir, "ExtMe", 5.0, supplier="Uline", expires_at=soon)
        resp = auth_client.post("/api/admin/locked-costs/extend", json={
            "description": "ExtMe", "supplier": "Uline", "days": 60,
        })
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["updated"] == 1
        # parse new expiry — should be ~60 days out
        new_dt = datetime.fromisoformat(body["new_expiry"].split("+")[0])
        delta = (new_dt - datetime.now()).days
        assert 58 <= delta <= 61

    def test_extend_rejects_silly_days(self, auth_client):
        resp = auth_client.post("/api/admin/locked-costs/extend", json={
            "description": "x", "days": 9999,
        })
        assert resp.status_code == 400

    def test_extend_requires_description(self, auth_client):
        resp = auth_client.post("/api/admin/locked-costs/extend", json={"days": 30})
        assert resp.status_code == 400


class TestCmdPalette:

    def test_locked_costs_in_palette(self, auth_client):
        """Cmd+K must surface the panel — discoverability matters."""
        resp = auth_client.get("/")
        body = resp.data.decode()
        assert "/admin/locked-costs" in body
        assert "Locked Costs" in body
