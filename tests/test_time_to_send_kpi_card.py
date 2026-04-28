"""Tests for `_build_time_to_send_kpi_card` — the §4.1 headline KPI on
/health/quoting.

The plan's KPI is "1 quote sent in <90 seconds." This card surfaces:
    median, p95, count, and %-under-90s for two windows (24h + 7d).

Status thresholds (locked here so a future tweak can't silently flip
the dashboard):
    error    < 30% under 90s in 7d
    warn     30-60% in 7d
    healthy  >= 60% in 7d (target)
    unknown  no rows in 7d (no signal yet)
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest


def _build():
    from src.api.modules.routes_health import _build_time_to_send_kpi_card
    return _build_time_to_send_kpi_card()


def _ensure_kpi_table(conn):
    """operator_quote_sent lives in migration #34 — same conftest gap as
    PR #619/#622 tests."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS operator_quote_sent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_id TEXT NOT NULL,
            quote_type TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            started_at TEXT,
            time_to_send_seconds INTEGER,
            item_count INTEGER DEFAULT 0,
            agency_key TEXT,
            quote_total REAL DEFAULT 0
        )
    """)


def _wipe(conn):
    _ensure_kpi_table(conn)
    conn.execute("DELETE FROM operator_quote_sent")
    conn.commit()


def _seed(conn, *, quote_id, time_to_send_seconds, days_ago=0,
          agency_key="CDCR", quote_total=100.0):
    """Seed via timedelta(hours=24*days_ago - 1) shaved by an hour so the
    boundary case (days_ago=1) lands inside the 24h window. SQLite's
    `datetime('now', '-1 days')` truncates to the second; a row seeded
    `now - timedelta(days=1)` lands microseconds outside the window
    after the SQL evaluation runs. Subtracting an hour avoids that."""
    sent_at = (datetime.now() - timedelta(hours=max(0, 24 * days_ago - 1))).isoformat() if days_ago else datetime.now().isoformat()
    conn.execute("""
        INSERT INTO operator_quote_sent
          (quote_id, quote_type, sent_at, started_at,
           time_to_send_seconds, item_count, agency_key, quote_total)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (quote_id, "pc", sent_at, sent_at,
          time_to_send_seconds, 1, agency_key, quote_total))


# ── Empty / unknown ─────────────────────────────────────────────────────


def test_unknown_when_no_rows():
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
    out = _build()
    assert out["status"] == "unknown"
    assert out["window_24h"]["count"] == 0
    assert out["window_7d"]["count"] == 0
    assert out["window_7d"]["under_90_pct"] is None


# ── Status thresholds (locked) ──────────────────────────────────────────


def test_healthy_when_60pct_or_more_under_90s():
    """6 of 10 quotes under 90s → 60% → healthy (right at target)."""
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        for i in range(6):
            _seed(c, quote_id=f"fast-{i}", time_to_send_seconds=45, days_ago=1)
        for i in range(4):
            _seed(c, quote_id=f"slow-{i}", time_to_send_seconds=300, days_ago=2)
        c.commit()
    out = _build()
    assert out["status"] == "healthy"
    assert out["window_7d"]["under_90_pct"] == 60.0
    assert out["window_7d"]["under_90_count"] == 6


def test_warn_when_30_to_60pct_under_90s():
    """4 of 10 → 40% → warn."""
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        for i in range(4):
            _seed(c, quote_id=f"fast-{i}", time_to_send_seconds=45, days_ago=1)
        for i in range(6):
            _seed(c, quote_id=f"slow-{i}", time_to_send_seconds=300, days_ago=2)
        c.commit()
    out = _build()
    assert out["status"] == "warn"
    assert out["window_7d"]["under_90_pct"] == 40.0


def test_error_when_under_30pct_under_90s():
    """2 of 10 → 20% → error (KPI broken)."""
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        for i in range(2):
            _seed(c, quote_id=f"fast-{i}", time_to_send_seconds=45, days_ago=1)
        for i in range(8):
            _seed(c, quote_id=f"slow-{i}", time_to_send_seconds=300, days_ago=2)
        c.commit()
    out = _build()
    assert out["status"] == "error"
    assert out["window_7d"]["under_90_pct"] == 20.0


# ── Window separation ──────────────────────────────────────────────────


def test_24h_window_excludes_older_quotes():
    """Quotes >24h old populate the 7d window but NOT the 24h window."""
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, quote_id="recent", time_to_send_seconds=45, days_ago=0)
        _seed(c, quote_id="three_days_ago", time_to_send_seconds=45, days_ago=3)
        c.commit()
    out = _build()
    assert out["window_24h"]["count"] == 1
    assert out["window_7d"]["count"] == 2


def test_7d_window_excludes_quotes_older_than_7d():
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, quote_id="ancient", time_to_send_seconds=45, days_ago=10)
        _seed(c, quote_id="recent", time_to_send_seconds=45, days_ago=2)
        c.commit()
    out = _build()
    assert out["window_7d"]["count"] == 1


# ── Median / p95 ────────────────────────────────────────────────────────


def test_median_and_p95_computed_correctly():
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        # Times: 30, 60, 90, 120, 600. median=90, p95=600 (5th at index 4).
        # days_ago=0 keeps these inside the 24h window without boundary risk.
        for i, t in enumerate([30, 60, 90, 120, 600]):
            _seed(c, quote_id=f"q{i}", time_to_send_seconds=t, days_ago=0)
        c.commit()
    out = _build()
    assert out["window_24h"]["median_seconds"] == 90
    assert out["window_24h"]["p95_seconds"] == 600


# ── Shape contract ──────────────────────────────────────────────────────


def test_response_shape_is_stable():
    """Templates rely on these keys — any rename is a breaking change."""
    out = _build()
    assert "status" in out
    assert "kpi_target_pct" in out
    for wkey in ("window_24h", "window_7d"):
        assert wkey in out
        for k in ("count", "median_seconds", "p95_seconds",
                  "under_90_pct", "under_90_count"):
            assert k in out[wkey], f"{wkey} missing {k}"


# ── Health endpoint integration ─────────────────────────────────────────


def test_api_health_quoting_exposes_time_to_send_kpi(auth_client):
    """The KPI card must be present on the JSON payload so monitors can
    alert when the headline KPI degrades."""
    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    kpi = data.get("time_to_send_kpi")
    assert kpi is not None
    assert kpi["status"] in ("healthy", "warn", "error", "unknown")
    assert "window_24h" in kpi and "window_7d" in kpi


def test_health_quoting_html_renders_time_to_send_kpi(auth_client):
    resp = auth_client.get("/health/quoting")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "Time-to-send KPI" in body
    assert "under 90s" in body
