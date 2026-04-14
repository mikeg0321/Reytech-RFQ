"""Regression guards for the win-rate bias fix (#74 follow-up).

The first version of _compute_outreach_triggers computed
`win_rate = won / total_quotes` where `total_quotes` counted 'sent'
and 'pending' statuses. On the 2026-04-14 prod DB, only 16% of
quotes had reached a terminal ('won'/'lost') status, so the naive
win_rate was systematically biased downward for every high-volume
buyer.

This file pins the debiased contract:
  - win_rate = won / (won + lost), None when both are zero
  - captured_quotes and unresolved_quotes are exposed on every row
  - opportunity_score uses the debiased win_rate in the formula
  - /api/oracle/capture-gap surfaces stuck 'sent' quotes
  - /api/oracle/capture-gap returns a summary with rate = captured/total
"""
import sqlite3
import pytest
from datetime import datetime, timedelta


def _seed_quote(institution: str, status: str, total: float,
                quote_number: str, created_at: str = None,
                agency: str = None, sent_at: str = None):
    from src.core.db import DB_PATH
    if created_at is None:
        created_at = datetime.now().isoformat()
    if agency is None:
        agency = institution
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute(
        """INSERT INTO quotes
           (quote_number, created_at, institution, agency, total, status,
            is_test, margin_pct, total_cost, items_count, sent_at)
           VALUES (?, ?, ?, ?, ?, ?, 0, 30, ?, 1, ?)""",
        (quote_number, created_at, institution, agency, total, status,
         total * 0.7, sent_at or ""),
    )
    conn.commit()
    conn.close()


class TestDebiasedWinRate:
    def test_win_rate_ignores_unresolved_quotes(self, auth_client, temp_data_dir):
        """Seed a buyer with 4 won, 2 lost, 10 sent — the old formula
        gave 4/16 = 25%, the new one must give 4/6 = 67%."""
        old = (datetime.now() - timedelta(days=90)).isoformat()
        for i in range(4):
            _seed_quote("bigwin", "won", 5000, f"BW-W-{i}", created_at=old)
        for i in range(2):
            _seed_quote("bigwin", "lost", 5000, f"BW-L-{i}", created_at=old)
        for i in range(10):
            _seed_quote("bigwin", "sent", 5000, f"BW-S-{i}", created_at=old)

        r = auth_client.get("/api/oracle/outreach-triggers?silence_days=60&min_quotes=5")
        d = r.get_json()
        bigwin = next(t for t in d["triggers"] if t["institution"] == "bigwin")
        # captured = 4 + 2 = 6, win_rate = 4/6 ≈ 0.667
        assert bigwin["captured_quotes"] == 6
        assert bigwin["unresolved_quotes"] == 10
        assert bigwin["win_rate"] is not None
        assert abs(bigwin["win_rate"] - 0.667) < 0.01
        # The naive formula would have given 4/16 = 0.25 — fail loudly
        # if someone reverts the fix
        assert bigwin["win_rate"] > 0.5

    def test_win_rate_none_when_no_captured(self, auth_client, temp_data_dir):
        """A buyer with 6 sent quotes and 0 won/lost should have
        win_rate = None, NOT 0.0. Zero implies "we tried and lost";
        None implies "we haven't actually resolved these yet"."""
        old = (datetime.now() - timedelta(days=90)).isoformat()
        for i in range(6):
            _seed_quote("allsent", "sent", 5000, f"AS-{i}", created_at=old)

        r = auth_client.get("/api/oracle/outreach-triggers?silence_days=60")
        d = r.get_json()
        allsent = next((t for t in d["triggers"] if t["institution"] == "allsent"), None)
        assert allsent is not None
        assert allsent["captured_quotes"] == 0
        assert allsent["win_rate"] is None

    def test_opportunity_uses_debiased_win_rate(self, auth_client, temp_data_dir):
        """A buyer with high captured win rate should rank above one
        with low captured win rate even when the unresolved counts
        swap the two. Seed A with 8 won + 2 lost + 20 unresolved and
        B with 2 won + 8 lost + 0 unresolved — both have 10 total
        captured, but A's debiased win rate is 0.8 vs B's 0.2."""
        old = (datetime.now() - timedelta(days=90)).isoformat()
        for i in range(8):
            _seed_quote("winner_a", "won", 5000, f"A-W-{i}", created_at=old)
        for i in range(2):
            _seed_quote("winner_a", "lost", 5000, f"A-L-{i}", created_at=old)
        for i in range(20):
            _seed_quote("winner_a", "sent", 5000, f"A-S-{i}", created_at=old)
        for i in range(2):
            _seed_quote("loser_b", "won", 5000, f"B-W-{i}", created_at=old)
        for i in range(8):
            _seed_quote("loser_b", "lost", 5000, f"B-L-{i}", created_at=old)

        r = auth_client.get("/api/oracle/outreach-triggers?silence_days=60")
        d = r.get_json()
        names = [t["institution"] for t in d["triggers"]]
        assert "winner_a" in names
        assert "loser_b" in names
        assert names.index("winner_a") < names.index("loser_b")


class TestCaptureRateSummary:
    def test_summary_captures_the_ratio(self, auth_client, temp_data_dir):
        """5 won + 5 lost + 10 sent = 10/20 = 50% capture rate."""
        now = datetime.now().isoformat()
        for i in range(5):
            _seed_quote("cchcs", "won", 5000, f"CW-{i}", created_at=now)
        for i in range(5):
            _seed_quote("cchcs", "lost", 5000, f"CL-{i}", created_at=now)
        for i in range(10):
            _seed_quote("cchcs", "sent", 5000, f"CS-{i}", created_at=now)

        r = auth_client.get("/api/oracle/capture-gap")
        d = r.get_json()
        assert d["ok"] is True
        assert d["summary"]["captured"] == 10
        assert d["summary"]["total"] == 20
        assert abs(d["summary"]["rate"] - 0.5) < 0.01

    def test_summary_handles_empty_db(self, auth_client, temp_data_dir):
        r = auth_client.get("/api/oracle/capture-gap")
        d = r.get_json()
        assert d["ok"] is True
        assert d["summary"]["captured"] == 0
        assert d["summary"]["total"] == 0
        assert d["summary"]["rate"] == 0.0


class TestCaptureGapAPI:
    def test_stuck_sent_quotes_surface(self, auth_client, temp_data_dir):
        old_sent = (datetime.now() - timedelta(days=45)).isoformat()
        for i in range(8):
            _seed_quote("stuckville", "sent", 2500, f"ST-{i}",
                         created_at=old_sent, sent_at=old_sent)

        r = auth_client.get("/api/oracle/capture-gap?min_age_days=30")
        d = r.get_json()
        assert d["ok"] is True
        gap = next((g for g in d["gap"] if g["institution"] == "stuckville"), None)
        assert gap is not None
        assert gap["stuck_count"] == 8
        assert gap["stuck_dollars"] == 20000.0  # 8 × 2500
        assert gap["oldest_age_days"] is not None
        assert 43 <= gap["oldest_age_days"] <= 47  # clock fuzz

    def test_recent_sent_does_not_surface(self, auth_client, temp_data_dir):
        """Quotes sent < min_age_days ago are not 'stuck' yet."""
        recent = (datetime.now() - timedelta(days=5)).isoformat()
        for i in range(6):
            _seed_quote("recentsend", "sent", 2500, f"RS-{i}",
                         created_at=recent, sent_at=recent)

        r = auth_client.get("/api/oracle/capture-gap?min_age_days=30")
        d = r.get_json()
        names = [g["institution"] for g in d["gap"]]
        assert "recentsend" not in names

    def test_non_sent_statuses_excluded(self, auth_client, temp_data_dir):
        """Won, lost, pending — none of these should show as 'stuck in sent'."""
        old = (datetime.now() - timedelta(days=90)).isoformat()
        for status in ("won", "lost", "pending", "draft"):
            _seed_quote(f"test_{status}", status, 1000, f"{status}-1",
                         created_at=old, sent_at=old)
        r = auth_client.get("/api/oracle/capture-gap")
        d = r.get_json()
        names = [g["institution"] for g in d["gap"]]
        for status in ("won", "lost", "pending", "draft"):
            assert f"test_{status}" not in names

    def test_min_age_days_clamp(self, auth_client):
        r = auth_client.get("/api/oracle/capture-gap?min_age_days=2")
        assert r.status_code == 200  # clamped to 7
        r = auth_client.get("/api/oracle/capture-gap?min_age_days=abc")
        assert r.status_code == 200


class TestBuyerIntelligencePageIntegration:
    def test_capture_summary_renders_on_page(self, auth_client, temp_data_dir):
        now = datetime.now().isoformat()
        for i in range(5):
            _seed_quote("cchcs", "won", 5000, f"CSP-W-{i}", created_at=now)
        for i in range(15):
            _seed_quote("cchcs", "sent", 5000, f"CSP-S-{i}", created_at=now)
        r = auth_client.get("/buyer-intelligence")
        assert r.status_code == 200
        assert b"capture rate" in r.data or b"Capture" in r.data

    def test_capture_gap_section_renders_when_nonempty(self, auth_client, temp_data_dir):
        old = (datetime.now() - timedelta(days=60)).isoformat()
        for i in range(5):
            _seed_quote("stuck_page_test", "sent", 3000, f"SP-{i}",
                         created_at=old, sent_at=old)
        r = auth_client.get("/buyer-intelligence")
        assert b"Capture gap" in r.data
        assert b"STUCK_PAGE_TEST" in r.data
