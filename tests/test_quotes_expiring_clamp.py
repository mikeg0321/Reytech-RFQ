"""Tests for the /api/quotes/expiring server-side clamp.

Regression for 2026-05-28 home audit P0: a quote that expired 70 days
ago (R26Q16) was rendering on the home banner as "0 days left" because
``max(0, (exp - now).days)`` floored the negative day count to 0. The
client-side ``renderExpiring`` filter hid these rows but they still
counted toward the payload. Server-side clamp drops rows older than
-7 days so they never leave the route.
"""
import os
import sqlite3
from datetime import datetime, timedelta


def _insert_quote(db_path, quote_number, expires_at, status="generated"):
    """Direct sqlite insert — the seed_db_quote fixture doesn't accept
    expires_at because that column is added by a runtime migration. We
    write it explicitly here so the route's WHERE clause sees a value.

    Uses INSERT OR REPLACE so the test is robust against any sample data
    the app fixture's init_db() may have seeded under the same number."""
    conn = sqlite3.connect(db_path)
    now = datetime.now().isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO quotes
           (quote_number, created_at, agency, institution, status,
            total, subtotal, tax, expires_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (quote_number, now, "CDCR", "CSP-Sacramento", status,
         100.0, 100.0, 0.0, expires_at, now),
    )
    conn.commit()
    conn.close()


class TestExpiringClamp:
    """The /api/quotes/expiring route filters out quotes that expired
    more than 7 days ago. Anything within ±7 days (or in the future
    within 7) is included."""

    def test_clamps_quote_expired_70_days_ago(self, auth_client, temp_data_dir):
        """The R26Q16 incident shape: a quote that expired 70 days back
        must NOT appear in the response. Before the clamp, it rendered
        with days_remaining=0 ("0 days left") which lied to the operator."""
        db_path = os.path.join(temp_data_dir, "reytech.db")
        long_expired = (datetime.now() - timedelta(days=70)).isoformat()
        _insert_quote(db_path, "R26Q16", long_expired)

        resp = auth_client.get("/api/quotes/expiring")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        qns = [q["quote_number"] for q in body["expiring"]]
        assert "R26Q16" not in qns
        assert body["count"] == 0

    def test_keeps_quote_expiring_tomorrow(self, auth_client, temp_data_dir):
        db_path = os.path.join(temp_data_dir, "reytech.db")
        tomorrow = (datetime.now() + timedelta(days=1)).isoformat()
        _insert_quote(db_path, "R26Q17", tomorrow)

        resp = auth_client.get("/api/quotes/expiring")
        body = resp.get_json()
        qns = [q["quote_number"] for q in body["expiring"]]
        assert "R26Q17" in qns

    def test_keeps_quote_expired_3_days_ago(self, auth_client, temp_data_dir):
        """Recently-expired quotes still surface — the operator may want
        to follow up. The cutoff is -7 days, not 0."""
        db_path = os.path.join(temp_data_dir, "reytech.db")
        three_days_ago = (datetime.now() - timedelta(days=3)).isoformat()
        _insert_quote(db_path, "R26Q18", three_days_ago)

        resp = auth_client.get("/api/quotes/expiring")
        body = resp.get_json()
        qns = [q["quote_number"] for q in body["expiring"]]
        assert "R26Q18" in qns

    def test_clamps_quote_expired_8_days_ago(self, auth_client, temp_data_dir):
        """Boundary: -8 days is just past the cutoff and must be dropped."""
        db_path = os.path.join(temp_data_dir, "reytech.db")
        eight_days_ago = (datetime.now() - timedelta(days=8)).isoformat()
        _insert_quote(db_path, "R26Q19", eight_days_ago)

        resp = auth_client.get("/api/quotes/expiring")
        body = resp.get_json()
        qns = [q["quote_number"] for q in body["expiring"]]
        assert "R26Q19" not in qns
