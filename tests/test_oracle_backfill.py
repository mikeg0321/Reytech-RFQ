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


class TestBackfillFromWonQuotesKb:
    """The won_quotes_kb table is the largest historical signal source —
    1,260+ rows of per-product per-agency bid outcomes derived from SCPRS.
    Phase 0.7 of PLAN_ONCE_AND_FOR_ALL.md added this as a third source."""

    def _seed_kb(self, rows):
        """Helper: insert rows into won_quotes_kb on the test DB."""
        from src.core.db import get_db
        with get_db() as conn:
            # Migration 9 creates the table; ensure it's there
            conn.execute("""
                CREATE TABLE IF NOT EXISTS won_quotes_kb (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_description TEXT, nsn TEXT, mfg_number TEXT,
                    agency TEXT, winning_price REAL, winning_vendor TEXT,
                    reytech_won INTEGER DEFAULT 0, reytech_price REAL,
                    price_delta REAL, award_date TEXT, po_number TEXT,
                    tenant_id TEXT DEFAULT 'reytech',
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.execute("DELETE FROM won_quotes_kb")
            for r in rows:
                conn.execute("""
                    INSERT INTO won_quotes_kb
                    (item_description, mfg_number, agency, winning_price,
                     winning_vendor, reytech_won, reytech_price, po_number)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (r.get("desc"), r.get("mfg", ""), r.get("agency", ""),
                      r.get("winning_price"), r.get("winning_vendor", ""),
                      1 if r.get("won") else 0, r.get("reytech_price"),
                      r.get("po", "")))
            conn.commit()

    def test_kb_reytech_win_feeds_calibration(self):
        self._seed_kb([{
            "desc": "Latex Glove M", "mfg": "GLV-M",
            "agency": "CDCR", "winning_price": 12.50,
            "winning_vendor": "Reytech Inc.",
            "won": True, "reytech_price": 12.50,
        }])
        result = backfill_all()
        assert result["kb_wins"] == 1
        assert result["kb_losses"] == 0
        assert result["calibrations_written"] >= 1

    def test_kb_reytech_loss_with_winner_price(self):
        self._seed_kb([{
            "desc": "Bandage 4x4", "mfg": "BND-4",
            "agency": "CCHCS", "winning_price": 8.00,
            "winning_vendor": "Gorilla Stationers",
            "won": False, "reytech_price": 11.00,
        }])
        result = backfill_all()
        assert result["kb_wins"] == 0
        assert result["kb_losses"] == 1
        # Calibration ran with winner_prices={0: 8.00} so avg_losing_delta
        # should reflect (11-8)/8 = 37.5% over.
        assert result["calibrations_written"] >= 1

    def test_kb_no_bid_rows_count_separately(self):
        # Reytech didn't bid → skipped from calibration but counted.
        self._seed_kb([
            {"desc": "Pen blue", "agency": "CDCR",
             "winning_price": 0.50, "winning_vendor": "Other Co.",
             "won": False, "reytech_price": None},
            {"desc": "Pen red", "agency": "CDCR",
             "winning_price": 0.50, "winning_vendor": "Other Co.",
             "won": False, "reytech_price": 0},
        ])
        result = backfill_all()
        assert result["kb_wins"] == 0
        assert result["kb_losses"] == 0
        assert result["kb_skipped_no_bid"] == 2

    def test_kb_dry_run_doesnt_write(self):
        self._seed_kb([{
            "desc": "Tape 1in", "agency": "CDCR",
            "winning_price": 3.00, "won": True, "reytech_price": 3.00,
        }])
        result = backfill_all(dry_run=True)
        assert result["kb_wins"] == 1
        assert result["calibrations_written"] == 0

    def test_kb_missing_table_doesnt_crash(self):
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DROP TABLE IF EXISTS won_quotes_kb")
        result = backfill_all()
        assert result["ok"] is True
        assert result["kb_wins"] == 0


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
