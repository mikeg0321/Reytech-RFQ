"""Phase 4.3: cost-alert scanner tests.

Walk price_history for items where the latest cost-source price differs
>threshold% from a prior known price for the same (mfg, source). The
scanner is idempotent on (mfg, source, new_found_at) so re-running
doesn't duplicate alerts.
"""

import json
from datetime import datetime, timedelta

import pytest

from src.core.db import get_db


def _seed_price_history(part_number, source, unit_price, found_at,
                        description="Test Item"):
    with get_db() as conn:
        conn.execute("""
            INSERT INTO price_history
            (found_at, description, part_number, unit_price, source)
            VALUES (?, ?, ?, ?, ?)
        """, (found_at, description, part_number, unit_price, source))
        conn.commit()


class TestScanCostAlerts:
    def test_no_history_returns_zero(self, client):
        r = client.post("/api/admin/scan-cost-alerts", json={})
        body = r.get_json()
        assert body["ok"] is True
        assert body["alerts_inserted"] == 0

    def test_detects_price_jump(self, client):
        # Older price + recent jump
        old = (datetime.now() - timedelta(days=30)).date().isoformat()
        new = datetime.now().date().isoformat()
        _seed_price_history("MFG-JUMP", "manual", 10.0, old)
        _seed_price_history("MFG-JUMP", "manual", 13.0, new)  # +30%
        r = client.post("/api/admin/scan-cost-alerts",
                        json={"threshold_pct": 10})
        body = r.get_json()
        assert body["alerts_inserted"] == 1
        # Check the alert row
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM cost_alerts WHERE mfg_number=?",
                ("MFG-JUMP",)
            ).fetchone()
        assert row is not None
        assert row["delta_pct"] == 30.0
        assert row["status"] == "pending"

    def test_below_threshold_skipped(self, client):
        old = (datetime.now() - timedelta(days=30)).date().isoformat()
        new = datetime.now().date().isoformat()
        _seed_price_history("MFG-SMALL", "manual", 10.0, old)
        _seed_price_history("MFG-SMALL", "manual", 10.5, new)  # +5%
        r = client.post("/api/admin/scan-cost-alerts",
                        json={"threshold_pct": 10})
        body = r.get_json()
        assert body["alerts_inserted"] == 0

    def test_amazon_source_excluded(self, client):
        """Amazon prices are reference, not cost — should NOT trigger."""
        old = (datetime.now() - timedelta(days=30)).date().isoformat()
        new = datetime.now().date().isoformat()
        _seed_price_history("MFG-AMZ", "amazon", 10.0, old)
        _seed_price_history("MFG-AMZ", "amazon", 20.0, new)
        r = client.post("/api/admin/scan-cost-alerts",
                        json={"threshold_pct": 10})
        body = r.get_json()
        assert body["alerts_inserted"] == 0

    def test_idempotent_rerun(self, client):
        old = (datetime.now() - timedelta(days=30)).date().isoformat()
        new = datetime.now().date().isoformat()
        _seed_price_history("MFG-IDEM", "manual", 10.0, old)
        _seed_price_history("MFG-IDEM", "manual", 15.0, new)
        r1 = client.post("/api/admin/scan-cost-alerts", json={"threshold_pct": 10})
        r2 = client.post("/api/admin/scan-cost-alerts", json={"threshold_pct": 10})
        assert r1.get_json()["alerts_inserted"] == 1
        assert r2.get_json()["alerts_inserted"] == 0
        assert r2.get_json()["alerts_skipped_dupe"] >= 1

    def test_dry_run_doesnt_write(self, client):
        old = (datetime.now() - timedelta(days=30)).date().isoformat()
        new = datetime.now().date().isoformat()
        _seed_price_history("MFG-DRY", "manual", 10.0, old)
        _seed_price_history("MFG-DRY", "manual", 14.0, new)
        r = client.post("/api/admin/scan-cost-alerts",
                        json={"threshold_pct": 10, "dry_run": True})
        body = r.get_json()
        assert body["alerts_inserted"] == 1
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM cost_alerts WHERE mfg_number=?",
                ("MFG-DRY",)
            ).fetchone()
        assert row is None


class TestCostAlertsList:
    def test_list_endpoint_returns_pending(self, client):
        old = (datetime.now() - timedelta(days=30)).date().isoformat()
        new = datetime.now().date().isoformat()
        _seed_price_history("MFG-LIST", "manual", 10.0, old)
        _seed_price_history("MFG-LIST", "manual", 14.0, new)
        client.post("/api/admin/scan-cost-alerts", json={"threshold_pct": 10})
        r = client.get("/api/admin/cost-alerts?status=pending")
        body = r.get_json()
        assert body["ok"] is True
        assert body["count_pending"] >= 1
        assert any(a["mfg_number"] == "MFG-LIST" for a in body["alerts"])

    def test_dismiss_alert(self, client):
        old = (datetime.now() - timedelta(days=30)).date().isoformat()
        new = datetime.now().date().isoformat()
        _seed_price_history("MFG-DISM", "manual", 10.0, old)
        _seed_price_history("MFG-DISM", "manual", 14.0, new)
        client.post("/api/admin/scan-cost-alerts", json={"threshold_pct": 10})
        r = client.get("/api/admin/cost-alerts?status=pending")
        alert = next((a for a in r.get_json()["alerts"]
                      if a["mfg_number"] == "MFG-DISM"), None)
        assert alert is not None
        r2 = client.post(
            f"/api/admin/cost-alerts/{alert['id']}/status",
            json={"status": "dismissed"},
        )
        assert r2.get_json()["status"] == "dismissed"


class TestSourceAllowlist:
    def test_only_cost_sources_trigger(self, client):
        """Mix of sources — only the cost-class one should produce an alert."""
        old = (datetime.now() - timedelta(days=30)).date().isoformat()
        new = datetime.now().date().isoformat()
        _seed_price_history("MFG-MIX", "scprs", 10.0, old)
        _seed_price_history("MFG-MIX", "scprs", 25.0, new)  # reference, skip
        _seed_price_history("MFG-MIX", "manual", 10.0, old)
        _seed_price_history("MFG-MIX", "manual", 25.0, new)  # cost, alert
        r = client.post("/api/admin/scan-cost-alerts",
                        json={"threshold_pct": 10})
        body = r.get_json()
        assert body["alerts_inserted"] == 1
        with get_db() as conn:
            row = conn.execute(
                "SELECT source FROM cost_alerts WHERE mfg_number=?",
                ("MFG-MIX",)
            ).fetchone()
        assert row["source"] == "manual"
