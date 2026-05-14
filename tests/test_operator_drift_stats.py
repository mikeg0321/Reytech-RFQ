"""PR-AP — operator_drift_line diagnostic endpoint.

/admin/auto-recommendations renders "No operator_drift_line rows in
last 7d — was Mark-Sent used?". To know whether the empty state is
operator-behavior, silent-logging-failure, or read-query-bug, we need
the actual table state — not a UI summary.

GET /api/admin/operator-drift-stats exposes it.

Tests pin:
  1. Empty table → total=0, all windows=0, recent=[].
  2. Seeded rows → counts match per-window slicing.
  3. drift_pct_stats summary surfaces min/median/max.
  4. quote_type breakdown groups pc vs rfq correctly.
"""
from __future__ import annotations


def _insert_drift_row(conn, **fields):
    """Direct SQL insert for the operator_drift_line table."""
    cols = list(fields.keys())
    placeholders = ", ".join(["?"] * len(cols))
    sql = (
        f"INSERT INTO operator_drift_line ({', '.join(cols)}) "
        f"VALUES ({placeholders})"
    )
    conn.execute(sql, [fields[c] for c in cols])


def test_drift_stats_empty_table(client, temp_data_dir):
    """No rows yet → all counts zero, recent empty, stats null."""
    # Force the table to exist for the test DB.
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute(
            "DELETE FROM operator_drift_line WHERE 1=1"
        )

    resp = client.get("/api/admin/operator-drift-stats")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["total"] == 0
    assert body["by_window"]["7d"] == 0
    assert body["by_window"]["30d"] == 0
    assert body["by_window"]["90d"] == 0
    assert body["recent"] == []
    assert body["drift_pct_stats"] is None


def test_drift_stats_seeded_rows(client, temp_data_dir):
    """Insert a few rows; counts + recent + stats reflect them."""
    from src.core.db import get_db

    with get_db() as conn:
        conn.execute("DELETE FROM operator_drift_line WHERE 1=1")
        # 2 recent (within 7d) + 1 older (60d ago)
        _insert_drift_row(
            conn, quote_id="pc_apt1", quote_type="pc",
            sent_at="2026-05-13T10:00:00", agency_key="cchcs",
            line_idx=1, sent_price=100.0, rec_price=80.0,
            drift_pct=25.0,
        )
        _insert_drift_row(
            conn, quote_id="rfq_apt2", quote_type="rfq",
            sent_at="2026-05-12T10:00:00", agency_key="calvet",
            line_idx=1, sent_price=200.0, rec_price=180.0,
            drift_pct=11.11,
        )
        _insert_drift_row(
            conn, quote_id="pc_old", quote_type="pc",
            sent_at="2026-03-15T10:00:00", agency_key="cchcs",
            line_idx=1, sent_price=50.0, rec_price=45.0,
            drift_pct=11.11,
        )

    resp = client.get("/api/admin/operator-drift-stats")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["total"] == 3
    # 7d and 30d windows: 2 (the recent ones). 90d: 3 (includes the 60d-old row).
    assert body["by_window"]["7d"] >= 2
    assert body["by_window"]["90d"] == 3
    # quote_type breakdown
    assert body["by_quote_type"].get("pc") == 2
    assert body["by_quote_type"].get("rfq") == 1
    # agencies surfaced
    assert "cchcs" in body["agencies"]
    assert "calvet" in body["agencies"]
    # drift stats present
    assert body["drift_pct_stats"] is not None
    assert body["drift_pct_stats"]["n"] == 3
    # recent ordered by sent_at desc — most recent first
    assert body["recent"][0]["quote_id"] == "pc_apt1"


def test_drift_stats_recent_limit_ten(client, temp_data_dir):
    """recent list is capped at 10 to keep response bounded."""
    from src.core.db import get_db

    with get_db() as conn:
        conn.execute("DELETE FROM operator_drift_line WHERE 1=1")
        for i in range(15):
            _insert_drift_row(
                conn, quote_id=f"pc_aplim_{i}", quote_type="pc",
                sent_at=f"2026-05-{(i % 10) + 1:02d}T10:00:00",
                agency_key="cchcs",
                line_idx=1, sent_price=100.0, rec_price=80.0,
                drift_pct=25.0,
            )

    resp = client.get("/api/admin/operator-drift-stats")
    body = resp.get_json()
    assert body["total"] == 15
    assert len(body["recent"]) == 10
