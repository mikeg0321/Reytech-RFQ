"""Phase 4.1: prove operator Mark Won/Lost wires through to oracle
calibration. Without this test the route silently regresses back to its
pre-2026-04-25 state where calibration only fired from background workers.
"""

import json

import pytest


def _seed_quote(quote_number, status="sent", agency="CDCR",
                items=None, total=100.0):
    from src.core.db import get_db
    from datetime import datetime
    items = items or [{
        "description": "Test Item Phase 4.1",
        "qty": 1, "unit_price": 25.0, "supplier_cost": 18.0,
    }]
    with get_db() as conn:
        conn.execute("""
            INSERT INTO quotes (quote_number, status, agency, line_items,
                                total, created_at, is_test)
            VALUES (?, ?, ?, ?, ?, ?, 0)
        """, (quote_number, status, agency, json.dumps(items), total,
              datetime.now().isoformat()))
        conn.commit()


class TestMarkWonCalibratesOracle:
    def test_won_post_returns_oracle_calibrated_flag(self, client):
        _seed_quote("R26Q-CAL-WON", status="sent")
        r = client.post(
            "/quotes/R26Q-CAL-WON/status",
            json={"status": "won", "po_number": "PO-CAL-1"},
        )
        body = r.get_json()
        assert body["ok"] is True
        # Either truly calibrated or at least attempted (oracle_error set
        # would still mean the wiring is in place).
        assert "oracle_calibrated" in body, (
            "POST /quotes/<qn>/status MUST attempt oracle calibration on "
            "win. If this assert fails, Phase 4.1 wiring has regressed."
        )

    def test_won_without_po_rejected(self, client):
        _seed_quote("R26Q-CAL-WON-2", status="sent")
        r = client.post(
            "/quotes/R26Q-CAL-WON-2/status",
            json={"status": "won"},  # missing po_number
        )
        body = r.get_json()
        assert body["ok"] is False
        assert "PO number required" in body.get("error", "")


class TestMarkLostCalibratesOracle:
    def test_lost_post_returns_oracle_calibrated_flag(self, client):
        _seed_quote("R26Q-CAL-LOST", status="sent")
        r = client.post(
            "/quotes/R26Q-CAL-LOST/status",
            json={"status": "lost", "notes": "lost on price"},
        )
        body = r.get_json()
        assert body["ok"] is True
        assert "oracle_calibrated" in body

    def test_lost_with_winner_price_passes_kwarg(self, client, monkeypatch):
        """Verify winner_price from JSON gets converted to winner_prices
        dict and passed into calibrate_from_outcome."""
        captured = {}

        def fake_cal(items, outcome, **kw):
            captured["items"] = items
            captured["outcome"] = outcome
            captured["kw"] = kw

        # Patch where the route imports it
        import src.core.pricing_oracle_v2 as oracle_mod
        monkeypatch.setattr(oracle_mod, "calibrate_from_outcome", fake_cal)

        _seed_quote("R26Q-CAL-LOST-2", status="sent")
        r = client.post(
            "/quotes/R26Q-CAL-LOST-2/status",
            json={"status": "lost", "winner_price": 18.50,
                  "notes": "competitor was cheaper"},
        )
        assert r.status_code == 200
        assert captured.get("outcome") == "lost"
        # winner_prices should be a dict keyed by item index
        wp = captured.get("kw", {}).get("winner_prices")
        assert wp is not None
        assert wp[0] == 18.50
        assert captured["kw"].get("loss_reason") == "price"


class TestPendingDoesntCalibrate:
    def test_pending_status_does_not_call_calibrate(self, client, monkeypatch):
        called = {"n": 0}

        def fake_cal(*a, **kw):
            called["n"] += 1

        import src.core.pricing_oracle_v2 as oracle_mod
        monkeypatch.setattr(oracle_mod, "calibrate_from_outcome", fake_cal)

        _seed_quote("R26Q-CAL-PEND", status="sent")
        r = client.post(
            "/quotes/R26Q-CAL-PEND/status",
            json={"status": "pending"},
        )
        assert r.status_code == 200
        assert called["n"] == 0, (
            "Calibration must only fire on terminal won/lost outcomes — "
            "intermediate transitions like 'pending' should be silent."
        )
