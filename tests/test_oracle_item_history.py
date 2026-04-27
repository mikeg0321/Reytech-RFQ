"""Phase 4.2: buyer-product pricing history endpoint tests.

Confirms the read-side complement of Phase 4.1 (Mark Won/Lost calibration)
is wired and returns the right shape. Operator about to bid hits this
endpoint and sees prior bids + oracle recommendation.
"""

import json
from datetime import datetime

import pytest

from src.core.db import get_db
# Helpers live on oracle_backfill (the joinback uses the same heuristic);
# pulling them from there avoids re-importing the route module — which
# would re-register its @bp.route decorators against the test app and
# trigger 'View function mapping is overwriting an existing endpoint'.
from src.core.oracle_backfill import (
    _agency_match,
    _description_match_score,
)


def _seed_quote(qnum, status, agency, items, created_at="2026-04-15"):
    with get_db() as conn:
        conn.execute("""
            INSERT INTO quotes (quote_number, agency, status, line_items,
                                created_at, is_test, po_number)
            VALUES (?, ?, ?, ?, ?, 0, '')
        """, (qnum, agency, status, json.dumps(items), created_at))
        conn.commit()


def _seed_kb(rows):
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
        for r in rows:
            conn.execute("""
                INSERT INTO won_quotes_kb
                (item_description, agency, winning_price, winning_vendor,
                 award_date, reytech_won, reytech_price, po_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (r["desc"], r.get("agency", "CDCR"),
                  r.get("winning_price", 10.0),
                  r.get("winning_vendor", "Other Co"),
                  r.get("award_date", "2026-04-01"),
                  r.get("reytech_won", 0),
                  r.get("reytech_price"),
                  r.get("po_number", "")))
        conn.commit()


class TestEndpointBasics:
    def test_missing_args_returns_400(self, client):
        r = client.get("/api/oracle/item-history")
        assert r.status_code == 400

    def test_no_matches_returns_empty(self, client):
        r = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CDCR", "description": "Nonexistent Widget XYZ"},
        )
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["matches"]["quotes"] == []
        assert body["matches"]["kb"] == []
        assert body["stats"]["matches_total"] == 0


class TestEndpointMatching:
    def test_quote_match_returns_our_price(self, client):
        _seed_quote(
            "R26Q-IH-1", "won", "CDCR",
            [{"description": "Latex Glove Medium Powder Free",
              "unit_price": 11.95}],
            created_at="2026-04-08",
        )
        r = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CDCR",
                          "description": "Latex Glove Medium"},
        )
        assert r.status_code == 200
        body = r.get_json()
        assert len(body["matches"]["quotes"]) == 1
        m = body["matches"]["quotes"][0]
        assert m["quote_number"] == "R26Q-IH-1"
        assert m["status"] == "won"
        assert m["our_price"] == 11.95
        assert body["stats"]["wins"] >= 1

    def test_kb_match_returns_competitor_price(self, client):
        _seed_kb([{
            "desc": "Wheelchair Footrest Aluminum",
            "agency": "CalVet",
            "winning_price": 50.0,
            "winning_vendor": "Cardinal Health",
            "award_date": "2026-04-10",
        }])
        r = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CalVet",
                          "description": "Wheelchair Footrest"},
        )
        body = r.get_json()
        assert len(body["matches"]["kb"]) == 1
        m = body["matches"]["kb"][0]
        assert m["winning_vendor"] == "Cardinal Health"
        assert m["winning_price"] == 50.0
        assert m["reytech_won"] == 0

    def test_agency_must_match(self, client):
        _seed_quote(
            "R26Q-IH-2", "won", "DSH",
            [{"description": "Test Item Specific",
              "unit_price": 25.0}],
        )
        r = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CCHCS",   # different agency
                          "description": "Test Item Specific"},
        )
        body = r.get_json()
        assert body["matches"]["quotes"] == []

    def test_threshold_filter(self, client):
        _seed_quote(
            "R26Q-IH-3", "won", "CDCR",
            [{"description": "Apples Red Sweet Box Boxed Refresh",
              "unit_price": 5.00}],
        )
        # Loose threshold matches
        r1 = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CDCR", "description": "Apples Red",
                          "threshold": "0.1"},
        )
        assert len(r1.get_json()["matches"]["quotes"]) == 1
        # Strict threshold does not
        r2 = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CDCR", "description": "Apples Red",
                          "threshold": "0.95"},
        )
        assert r2.get_json()["matches"]["quotes"] == []


class TestStats:
    def test_winning_prices_aggregated(self, client):
        for q, p in [("R26Q-S1", 10.00), ("R26Q-S2", 11.00), ("R26Q-S3", 12.00)]:
            _seed_quote(
                q, "won", "CDCR",
                [{"description": "Stat Test Item", "unit_price": p}],
            )
        r = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CDCR", "description": "Stat Test Item"},
        )
        body = r.get_json()
        winning = body["stats"]["our_winning_prices"]
        assert winning is not None
        assert winning["min"] == 10.00
        assert winning["max"] == 12.00
        assert winning["median"] == 11.00
        assert winning["n"] == 3

    def test_win_rate_combines_quotes_and_kb(self, client):
        _seed_quote(
            "R26Q-WR-1", "won", "CDCR",
            [{"description": "WinRate Item", "unit_price": 10.0}],
        )
        _seed_kb([{
            "desc": "WinRate Item",
            "agency": "CDCR",
            "winning_vendor": "Other Co",
            "winning_price": 9.0,
            "reytech_won": 0,
            "reytech_price": 11.0,  # we lost
        }])
        r = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CDCR", "description": "WinRate Item"},
        )
        body = r.get_json()
        # 1 quote-win + 1 kb-loss = 50% win rate
        assert body["stats"]["wins"] == 1
        assert body["stats"]["losses"] == 1
        assert body["stats"]["win_rate_pct"] == 50.0


class TestOracleField:
    def test_oracle_recommendation_returned(self, client):
        # Even with no calibration data the field exists in the response shape
        r = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CDCR", "description": "Anything"},
        )
        body = r.get_json()
        assert "oracle" in body
        assert "markup_pct" in body["oracle"]
        assert "confidence" in body["oracle"]


class TestCategoryIntelField:
    """Phase 4.6 enrichment: item-history response now includes
    a category_intel sub-object so the existing PC-detail modal can
    render the loss/win bucket warning without a second fetch."""

    def test_category_intel_field_present(self, client):
        r = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CDCR",
                          "description": "Generic Office Pen"},
        )
        body = r.get_json()
        assert "category_intel" in body
        ci = body["category_intel"]
        assert "category" in ci
        assert "danger" in ci
        assert "warning_text" in ci

    def test_loss_bucket_fires_in_history_response(self, client):
        # Seed 6 footwear losses so the bucket clears the n>=5 floor
        # and the < 15% danger threshold.
        for i in range(6):
            _seed_quote(
                f"FW-LOSS-{i}", "lost", "CDCR Sacramento",
                [{"description": "Propet M3705 Walker Strap White"}],
            )
        r = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CDCR Sacramento",
                          "description": "Propet Walker"},
        )
        body = r.get_json()
        ci = body["category_intel"]
        assert ci["category"] == "footwear-orthopedic"
        assert ci["quotes"] >= 6
        assert ci["danger"] is True
        assert "LOSS BUCKET" in ci["warning_text"]

    def test_win_bucket_fires_in_history_response(self, client):
        # Seed 6 incontinence wins.
        for i in range(6):
            _seed_quote(
                f"INC-WIN-{i}", "won", "CDCR Sacramento",
                [{"description": "TENA ProSkin Adult Brief XL"}],
            )
        r = client.get(
            "/api/oracle/item-history",
            query_string={"agency": "CDCR Sacramento",
                          "description": "TENA Brief"},
        )
        body = r.get_json()
        ci = body["category_intel"]
        assert ci["category"] == "incontinence"
        assert ci["wins"] >= 6
        assert ci["danger"] is False
        assert "WIN BUCKET" in ci["warning_text"]


class TestMatchHelpers:
    def test_description_match_high(self):
        s = _description_match_score(
            "Latex Glove Medium Powder Free",
            "Latex Glove Medium Powder-Free Box",
        )
        assert s >= 0.5

    def test_agency_substring_either_direction(self):
        assert _agency_match("CDCR", "California CDCR Folsom")
        assert _agency_match("California CDCR Folsom", "CDCR")
        assert not _agency_match("CDCR", "CalVet")
