"""Oracle weekly email — verify the report builds against seeded data."""
import json
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest


def _seed(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_number TEXT,
            status TEXT,
            agency TEXT, institution TEXT, line_items TEXT,
            total REAL DEFAULT 0,
            sent_at TEXT, updated_at TEXT,
            is_test INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS oracle_calibration (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            agency TEXT DEFAULT '',
            sample_size INTEGER DEFAULT 0,
            win_count INTEGER DEFAULT 0,
            loss_on_price INTEGER DEFAULT 0,
            loss_on_other INTEGER DEFAULT 0,
            avg_winning_margin REAL DEFAULT 25,
            avg_losing_delta REAL DEFAULT 0,
            recommended_max_markup REAL DEFAULT 30,
            competitor_floor REAL DEFAULT 0,
            last_updated TEXT,
            UNIQUE(category, agency)
        );
        CREATE TABLE IF NOT EXISTS intel_acceptance_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT, agency TEXT, category TEXT, flavor TEXT,
            engine_markup_pct REAL, engine_price REAL,
            suggested_markup_pct REAL, suggested_price REAL,
            final_price REAL, accepted INTEGER NOT NULL DEFAULT 0,
            quote_number TEXT, pcid TEXT,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    now = datetime.now()
    yest = (now - timedelta(days=2)).isoformat()
    last = (now - timedelta(days=10)).isoformat()
    items = json.dumps([{"description": "Propet shoe diabetic"}])

    # 3 wins + 5 losses this week (under 7d)
    for i in range(3):
        conn.execute("INSERT INTO quotes (status, agency, line_items, "
                     "is_test, sent_at, updated_at) VALUES (?,?,?,0,?,?)",
                     ("won", "CCHCS", items, yest, yest))
    for i in range(5):
        conn.execute("INSERT INTO quotes (status, agency, line_items, "
                     "is_test, sent_at, updated_at) VALUES (?,?,?,0,?,?)",
                     ("lost", "CCHCS", items, yest, yest))
    # 4 wins + 6 losses prev week
    for i in range(4):
        conn.execute("INSERT INTO quotes (status, agency, line_items, "
                     "is_test, sent_at, updated_at) VALUES (?,?,?,0,?,?)",
                     ("won", "CCHCS", items, last, last))
    for i in range(6):
        conn.execute("INSERT INTO quotes (status, agency, line_items, "
                     "is_test, sent_at, updated_at) VALUES (?,?,?,0,?,?)",
                     ("lost", "CCHCS", items, last, last))

    # Calibration writes
    conn.execute("INSERT INTO oracle_calibration "
                 "(category, agency, sample_size, win_count, "
                 "avg_winning_margin, last_updated) VALUES (?,?,?,?,?,?)",
                 ("medical", "CCHCS", 8, 3, 22.5, yest))

    # Swap-link telemetry: 5 accepted, 3 rejected on footwear
    for i in range(5):
        conn.execute("INSERT INTO intel_acceptance_log "
                     "(description, category, flavor, accepted, recorded_at) "
                     "VALUES (?,?,?,1,?)",
                     ("shoe", "footwear-orthopedic", "B", yest))
    for i in range(3):
        conn.execute("INSERT INTO intel_acceptance_log "
                     "(description, category, flavor, accepted, recorded_at) "
                     "VALUES (?,?,?,0,?)",
                     ("shoe", "footwear-orthopedic", "B", yest))

    conn.commit()


@pytest.fixture
def fake_db(tmp_path):
    import sqlite3
    db_path = tmp_path / "weekly.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    _seed(conn)
    yield conn
    conn.close()


def test_build_weekly_report_aggregates_correctly(fake_db):
    from src.agents import oracle_weekly

    # Patch get_db to return our fake conn
    class _Ctx:
        def __enter__(_self): return fake_db
        def __exit__(_self, *a): return None

    with patch("src.core.db.get_db", return_value=_Ctx()), \
         patch("src.agents.oracle_weekly._live_buckets",
               return_value={"danger": [], "win": []}):
        report = oracle_weekly.build_weekly_report()

    assert report["ok"] is True
    assert report["this_week"]["won"] == 3
    assert report["this_week"]["lost"] == 5
    assert report["this_week"]["win_rate_pct"] == 37.5
    assert report["last_week"]["won"] == 4
    assert report["last_week"]["lost"] == 6
    assert report["last_week"]["win_rate_pct"] == 40.0
    assert report["wow_delta_pp"] == -2.5

    # Calibration row visible
    assert report["calibration"]["count"] == 1
    assert report["calibration"]["rows"][0]["category"] == "medical"

    # Swap-link telemetry
    assert report["swap_link"]["offered"] == 8
    assert report["swap_link"]["accepted"] == 5
    assert report["swap_link"]["rejected"] == 3
    assert report["swap_link"]["accept_rate_pct"] == 62.5

    # Body contains the rate + delta
    assert "37.5%" in report["body"]
    assert "-2.5pp" in report["body"]
    assert "Calibration" in report["body"] or "CALIBRATION" in report["body"]
    assert "5" in report["body"]  # accepted count somewhere


def test_send_weekly_email_dry_run_returns_body_no_send(fake_db):
    from src.agents import oracle_weekly

    class _Ctx:
        def __enter__(_self): return fake_db
        def __exit__(_self, *a): return None

    with patch("src.core.db.get_db", return_value=_Ctx()), \
         patch("src.agents.oracle_weekly._live_buckets",
               return_value={"danger": [], "win": []}):
        result = oracle_weekly.send_weekly_email(dry_run=True)

    assert result["ok"] is True
    assert result["dry_run"] is True
    assert "subject" in result and "body" in result
    # Confirm gmail_api.send_message was NOT called (no patch needed —
    # dry_run short-circuits before the gmail import)


def test_empty_window_returns_no_data_summary(tmp_path):
    """Empty DB should still build a report (no crash)."""
    import sqlite3
    p = tmp_path / "empty.db"
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    _seed(conn)
    # Wipe all rows
    conn.execute("DELETE FROM quotes")
    conn.execute("DELETE FROM oracle_calibration")
    conn.execute("DELETE FROM intel_acceptance_log")
    conn.commit()

    from src.agents import oracle_weekly

    class _Ctx:
        def __enter__(_self): return conn
        def __exit__(_self, *a): return None

    with patch("src.core.db.get_db", return_value=_Ctx()), \
         patch("src.agents.oracle_weekly._live_buckets",
               return_value={"danger": [], "win": []}):
        report = oracle_weekly.build_weekly_report()

    assert report["ok"] is True
    assert report["this_week"]["won"] == 0
    assert report["this_week"]["lost"] == 0
    assert report["this_week"]["win_rate_pct"] is None
    assert "No calibration writes" in report["body"]
    assert "No suggestions offered" in report["body"]
    conn.close()
