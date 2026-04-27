"""Plan §4.1 — operator_quote_sent KPI telemetry.

Until now we couldn't measure the <90s KPI. This module logs every
Mark Sent click + exposes get_kpi_stats() for analytics.
"""
from datetime import datetime, timedelta
from unittest.mock import patch
import sqlite3

import pytest


def _make_db(tmp_path):
    db = tmp_path / "kpi.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS operator_quote_sent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_id TEXT NOT NULL,
            quote_type TEXT NOT NULL DEFAULT 'pc',
            sent_at TEXT NOT NULL DEFAULT (datetime('now')),
            started_at TEXT,
            time_to_send_seconds INTEGER,
            item_count INTEGER DEFAULT 0,
            agency_key TEXT DEFAULT '',
            quote_total REAL DEFAULT 0
        );
    """)
    return conn


@pytest.fixture
def fake_db(tmp_path):
    conn = _make_db(tmp_path)
    yield conn
    conn.close()


def _patch_get_db(conn):
    class _Ctx:
        def __enter__(_self): return conn
        def __exit__(_self, *a): return None
    return patch("src.core.db.get_db", return_value=_Ctx())


class TestLogQuoteSent:
    def test_logs_with_computed_seconds(self, fake_db):
        from src.core.operator_kpi import log_quote_sent
        # 60 seconds ago
        started = (datetime.now() - timedelta(seconds=60)).isoformat()
        with _patch_get_db(fake_db):
            r = log_quote_sent(quote_id="pc-test", quote_type="pc",
                               started_at=started, item_count=3,
                               agency_key="cchcs", quote_total=420.50)
        assert r["ok"] is True
        # Allow ±2s for execution time
        assert 58 <= r["time_to_send_seconds"] <= 62

        rows = fake_db.execute("SELECT * FROM operator_quote_sent").fetchall()
        assert len(rows) == 1
        row = rows[0]
        assert row["quote_id"] == "pc-test"
        assert row["item_count"] == 3
        assert row["agency_key"] == "cchcs"
        assert abs(row["quote_total"] - 420.50) < 0.01

    def test_no_started_at_logs_null_seconds(self, fake_db):
        from src.core.operator_kpi import log_quote_sent
        with _patch_get_db(fake_db):
            r = log_quote_sent(quote_id="pc-X")
        assert r["ok"] is True
        assert r["time_to_send_seconds"] is None

    def test_invalid_started_at_doesnt_crash(self, fake_db):
        from src.core.operator_kpi import log_quote_sent
        with _patch_get_db(fake_db):
            r = log_quote_sent(quote_id="pc-Y", started_at="not-a-date")
        assert r["ok"] is True
        assert r["time_to_send_seconds"] is None

    def test_empty_quote_id_returns_error(self, fake_db):
        from src.core.operator_kpi import log_quote_sent
        with _patch_get_db(fake_db):
            r = log_quote_sent(quote_id="")
        assert r["ok"] is False

    def test_db_error_swallowed_returns_false_not_raise(self, fake_db, tmp_path):
        from src.core.operator_kpi import log_quote_sent
        # Drop the table so the insert fails
        fake_db.execute("DROP TABLE operator_quote_sent")
        fake_db.commit()
        with _patch_get_db(fake_db):
            r = log_quote_sent(quote_id="pc-Z")
        assert r["ok"] is False
        assert "error" in r


class TestGetKpiStats:
    def test_empty_db_returns_zero(self, fake_db):
        from src.core.operator_kpi import get_kpi_stats
        with _patch_get_db(fake_db):
            r = get_kpi_stats(window_days=7)
        assert r["ok"] is True
        assert r["count"] == 0
        assert r["median_seconds"] is None
        assert r["under_90_pct"] is None

    def test_aggregates_correctly(self, fake_db):
        from src.core.operator_kpi import get_kpi_stats
        # Seed 5 sends: 3 fast (under 90), 2 slow (over 90)
        now = datetime.now().isoformat()
        for sec in (45, 60, 75, 120, 180):
            fake_db.execute("""
                INSERT INTO operator_quote_sent
                (quote_id, sent_at, time_to_send_seconds, item_count, agency_key)
                VALUES (?, ?, ?, ?, ?)
            """, (f"q-{sec}", now, sec, 1, "cchcs"))
        fake_db.commit()

        with _patch_get_db(fake_db):
            r = get_kpi_stats(window_days=7)
        assert r["ok"] is True
        assert r["count"] == 5
        # Median of sorted [45,60,75,120,180] = 75
        assert r["median_seconds"] == 75
        assert r["under_90_count"] == 3
        assert r["under_90_pct"] == 60.0
        assert len(r["per_agency"]) == 1
        assert r["per_agency"][0]["agency_key"] == "cchcs"

    def test_one_item_only_filter(self, fake_db):
        from src.core.operator_kpi import get_kpi_stats
        now = datetime.now().isoformat()
        # 2 single-item, 1 multi-item
        fake_db.execute("INSERT INTO operator_quote_sent "
                        "(quote_id, sent_at, time_to_send_seconds, item_count) "
                        "VALUES ('q1', ?, 50, 1)", (now,))
        fake_db.execute("INSERT INTO operator_quote_sent "
                        "(quote_id, sent_at, time_to_send_seconds, item_count) "
                        "VALUES ('q2', ?, 60, 1)", (now,))
        fake_db.execute("INSERT INTO operator_quote_sent "
                        "(quote_id, sent_at, time_to_send_seconds, item_count) "
                        "VALUES ('q3', ?, 200, 5)", (now,))
        fake_db.commit()

        with _patch_get_db(fake_db):
            r1 = get_kpi_stats(window_days=7, one_item_only=True)
            r_all = get_kpi_stats(window_days=7, one_item_only=False)
        assert r1["count"] == 2
        assert r_all["count"] == 3
