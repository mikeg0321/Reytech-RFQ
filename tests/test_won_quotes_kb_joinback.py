"""Phase 0.7c: won_quotes_kb reytech_price join-back tests.

The joinback walks won_quotes_kb rows where reytech_price IS NULL and
populates them from the quotes table by matching agency + description +
date window. These tests pin the matching rules so a future regression
can't silently drop calibration signal.
"""

import json
from datetime import datetime, timedelta

from src.core.db import get_db
from src.core.oracle_backfill import (
    _agency_match,
    _description_match_score,
    joinback_won_quotes_kb,
)


# ── Helpers ──────────────────────────────────────────────────────────


def _ensure_kb_table():
    with get_db() as conn:
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


def _seed_kb(rows):
    _ensure_kb_table()
    with get_db() as conn:
        for r in rows:
            conn.execute("""
                INSERT INTO won_quotes_kb
                (item_description, agency, winning_price, winning_vendor,
                 award_date, reytech_won, reytech_price)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (r["desc"], r.get("agency", "CDCR"),
                  r.get("winning_price", 10.0),
                  r.get("winning_vendor", "Other Co"),
                  r.get("award_date", "2026-04-01"),
                  r.get("reytech_won", 0),
                  r.get("reytech_price")))


def _seed_quote(qnum, agency, status, items, created_at="2026-04-15"):
    with get_db() as conn:
        conn.execute("""
            INSERT INTO quotes (quote_number, agency, status, line_items,
                                created_at, is_test)
            VALUES (?, ?, ?, ?, ?, 0)
        """, (qnum, agency, status, json.dumps(items), created_at))


def _read_kb_row(idx=0):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM won_quotes_kb ORDER BY id"
        ).fetchall()
    return dict(rows[idx])


# ── Match-helper unit tests ──────────────────────────────────────────


class TestDescriptionMatch:
    def test_high_overlap_scores_well(self):
        score = _description_match_score(
            "Latex Glove Medium Powder Free",
            "Latex Glove Medium Powder-Free Box",
        )
        assert score >= 0.5

    def test_no_overlap_scores_zero(self):
        assert _description_match_score("apples", "wheelchair") == 0.0

    def test_empty_inputs_score_zero(self):
        assert _description_match_score("", "anything") == 0.0
        assert _description_match_score("anything", "") == 0.0

    def test_partial_overlap_scores_in_middle(self):
        score = _description_match_score(
            "Wheelchair Footrest Aluminum",
            "Wheelchair Cushion Foam",
        )
        assert 0.0 < score < 0.5


class TestAgencyMatch:
    def test_substring_either_direction(self):
        assert _agency_match("CDCR", "California Department of Corrections — CDCR")
        assert _agency_match("CDCR / Corrections", "CDCR")

    def test_case_insensitive(self):
        assert _agency_match("cdcr", "CDCR")

    def test_unrelated_agencies(self):
        assert not _agency_match("CDCR", "CalVet")

    def test_blank_inputs_dont_match(self):
        assert not _agency_match("", "CDCR")
        assert not _agency_match("CDCR", "")


# ── End-to-end joinback tests ────────────────────────────────────────


class TestJoinback:
    def test_match_populates_reytech_price_and_won(self):
        _seed_kb([{
            "desc": "Latex Glove Medium Powder Free",
            "agency": "CDCR", "winning_price": 12.50,
            "winning_vendor": "Reytech Inc.",
            "award_date": "2026-04-10",
        }])
        _seed_quote(
            "R26Q-JB-1", "CDCR", "won",
            [{"description": "Latex Glove Medium Powder-Free Box",
              "unit_price": 11.95}],
            created_at="2026-04-08",
        )
        result = joinback_won_quotes_kb()
        assert result["matched"] == 1
        assert result["updated"] == 1
        row = _read_kb_row()
        assert row["reytech_price"] == 11.95
        assert row["reytech_won"] == 1

    def test_loss_match_sets_won_to_zero(self):
        _seed_kb([{
            "desc": "Bandage 4x4 Sterile",
            "agency": "CCHCS", "winning_price": 8.0,
            "winning_vendor": "Gorilla Stationers",
            "award_date": "2026-04-10",
        }])
        _seed_quote(
            "R26Q-JB-2", "CCHCS", "lost",
            [{"description": "Bandage Sterile 4x4 Box",
              "unit_price": 11.0}],
            created_at="2026-04-12",
        )
        result = joinback_won_quotes_kb()
        assert result["matched"] == 1
        row = _read_kb_row()
        assert row["reytech_price"] == 11.0
        assert row["reytech_won"] == 0

    def test_no_match_leaves_row_alone(self):
        _seed_kb([{
            "desc": "Wheelchair Aluminum Footrest",
            "agency": "CDCR", "winning_price": 50.0,
            "award_date": "2026-04-10",
        }])
        _seed_quote(
            "R26Q-JB-3", "DSH", "won",  # different agency
            [{"description": "Wheelchair Aluminum Footrest",
              "unit_price": 45.0}],
            created_at="2026-04-08",
        )
        result = joinback_won_quotes_kb()
        assert result["matched"] == 0
        assert result["updated"] == 0

    def test_date_window_too_far_skips(self):
        _seed_kb([{
            "desc": "Adult Brief Large",
            "agency": "CalVet", "winning_price": 0.85,
            "award_date": "2026-01-10",
        }])
        _seed_quote(
            "R26Q-JB-4", "CalVet", "won",
            [{"description": "Adult Brief Large pack",
              "unit_price": 0.82}],
            created_at="2026-04-15",  # >90 days from award_date
        )
        result = joinback_won_quotes_kb(date_window_days=30)
        assert result["matched"] == 0

    def test_dry_run_doesnt_write(self):
        _seed_kb([{
            "desc": "Test Item",
            "agency": "CDCR", "winning_price": 10.0,
            "award_date": "2026-04-10",
        }])
        _seed_quote(
            "R26Q-JB-5", "CDCR", "won",
            [{"description": "Test Item", "unit_price": 9.5}],
            created_at="2026-04-08",
        )
        result = joinback_won_quotes_kb(dry_run=True)
        assert result["matched"] == 1
        assert result["updated"] == 0
        row = _read_kb_row()
        assert row["reytech_price"] is None

    def test_skips_already_populated_rows(self):
        _seed_kb([{
            "desc": "Already Done Item",
            "agency": "CDCR", "winning_price": 10.0,
            "award_date": "2026-04-10",
            "reytech_price": 9.0, "reytech_won": 1,
        }])
        _seed_quote(
            "R26Q-JB-6", "CDCR", "won",
            [{"description": "Already Done Item", "unit_price": 8.0}],
            created_at="2026-04-08",
        )
        result = joinback_won_quotes_kb()
        # Row was already populated, query filter excludes it
        assert result["kb_rows_examined"] == 0

    def test_won_status_preferred_over_lost(self):
        _seed_kb([{
            "desc": "Conflict Item Test",
            "agency": "CDCR", "winning_price": 10.0,
            "award_date": "2026-04-10",
        }])
        # Seed both a lost and a won quote that match
        _seed_quote(
            "R26Q-JB-LOSS", "CDCR", "lost",
            [{"description": "Conflict Item Test", "unit_price": 12.0}],
            created_at="2026-04-08",
        )
        _seed_quote(
            "R26Q-JB-WIN", "CDCR", "won",
            [{"description": "Conflict Item Test", "unit_price": 9.5}],
            created_at="2026-04-09",
        )
        result = joinback_won_quotes_kb()
        assert result["matched"] == 1
        row = _read_kb_row()
        assert row["reytech_won"] == 1
        assert row["reytech_price"] == 9.5


class TestJoinbackEndpoint:
    def test_endpoint_exists_and_dry_run(self, client):
        _ensure_kb_table()
        r = client.post(
            "/api/oracle/joinback-won-quotes-kb",
            json={"dry_run": True},
        )
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["dry_run"] is True
