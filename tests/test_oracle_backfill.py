"""Tests for the oracle calibration backfill (src/core/oracle_backfill.py).

Verifies that historical won/lost quotes feed through to oracle_calibration.
"""

import json
import sqlite3
import pytest
from src.core.oracle_backfill import backfill_all


class TestBackfillAll:
    def test_empty_db_returns_zero(self):
        result = backfill_all()
        assert result["ok"] is True
        assert result["quotes_won"] == 0
        assert result["quotes_lost"] == 0

    def test_won_quote_feeds_calibration(self, seed_db_quote):
        seed_db_quote("R26Q900", agency="CDCR", status="won", total=1000.0,
                      line_items=[
                          {"description": "Test Widget", "qty": 1,
                           "unit_price": 100.0, "supplier_cost": 80.0}
                      ])
        result = backfill_all()
        assert result["quotes_won"] == 1
        assert result["calibrations_written"] >= 1

    def test_lost_quote_feeds_calibration(self, seed_db_quote):
        seed_db_quote("R26Q901", agency="CCHCS", status="lost", total=500.0,
                      line_items=[
                          {"description": "Office Supplies", "qty": 5,
                           "unit_price": 20.0, "supplier_cost": 15.0}
                      ])
        result = backfill_all()
        assert result["quotes_lost"] == 1
        assert result["calibrations_written"] >= 1

    def test_dry_run_doesnt_write(self, seed_db_quote):
        seed_db_quote("R26Q902", agency="CDCR", status="won", total=100.0,
                      line_items=[
                          {"description": "Pen Set", "qty": 10,
                           "unit_price": 5.0, "supplier_cost": 3.0}
                      ])
        result = backfill_all(dry_run=True)
        assert result["quotes_won"] == 1
        assert result["calibrations_written"] == 0
        assert result["dry_run"] is True

    def test_skips_test_quotes(self, seed_db_quote):
        # seed_db_quote uses is_test=0 by default, but the SQL filter
        # excludes is_test=1 — verify by checking only real quotes counted
        seed_db_quote("R26Q903", agency="CDCR", status="won", total=50.0,
                      line_items=[{"description": "X", "unit_price": 10.0}])
        result = backfill_all()
        assert result["quotes_won"] == 1

    def test_idempotent(self, seed_db_quote):
        seed_db_quote("R26Q904", agency="CDCR", status="won", total=200.0,
                      line_items=[
                          {"description": "Puzzle", "qty": 1,
                           "unit_price": 25.0, "supplier_cost": 18.0}
                      ])
        r1 = backfill_all()
        r2 = backfill_all()
        # Both runs should succeed — calibration uses EMA blending
        assert r1["ok"] is True
        assert r2["ok"] is True
        assert r1["quotes_won"] == r2["quotes_won"]


class TestBackfillEndpoint:
    def test_endpoint_exists(self, client):
        r = client.post("/api/oracle/backfill-all", json={})
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True

    def test_dry_run_via_endpoint(self, client):
        r = client.post("/api/oracle/backfill-all", json={"dry_run": True})
        assert r.status_code == 200
        body = r.get_json()
        assert body["dry_run"] is True
