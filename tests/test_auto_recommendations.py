"""PR-S — auto-recommendations digest section.

Reads operator_drift_line per-agency and emits markup-tuning suggestions.

Pinned guarantees:
  1. Insufficient data (< MIN_SAMPLE_LINES) → "insufficient_data" bucket
     with no markup change recommended.
  2. High drift (> DRIFT_HIGH_THRESHOLD) → "drift_high" bucket with a
     concrete -N% suggestion, capped at MAX_DELTA_PCT.
  3. Low/negative drift (< DRIFT_LOW_THRESHOLD) → "drift_low" bucket
     warning about possible stale cost basis.
  4. Cap binding > 50% of lines → "cap_working" bucket.
  5. Otherwise → "on_track" bucket.
  6. The format_for_digest renderer produces stable plain-text suitable
     for the weekly digest body.
  7. Empty drift table → summary "no recommendations possible".
  8. The /admin/auto-recommendations endpoint renders HTML without 500.
"""
from __future__ import annotations

import os
import sqlite3
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _seed_drift_line(db_path, agency, drift_pct, cap_sources="",
                     quote_id="q1", sent_at_offset_days=0):
    """Insert a single operator_drift_line row."""
    from datetime import datetime, timedelta
    sent_at = (datetime.now() - timedelta(days=sent_at_offset_days)).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO operator_drift_line "
            "(quote_id, quote_type, sent_at, agency_key, line_idx, "
            "sent_price, rec_price, drift_pct, cap_sources) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (quote_id, "pc", sent_at, agency, 1, 100.0, 100.0, drift_pct,
             cap_sources),
        )
        conn.commit()
    finally:
        conn.close()


# ── Classifier ───────────────────────────────────────────────────────


def test_insufficient_data_bucket():
    from src.agents.auto_recommendations import _classify_agency
    r = _classify_agency(line_count=3, median_drift_pct=20.0,
                         capped_lines=0, capped_pct=0)
    assert r["bucket"] == "insufficient_data"
    assert r["color"] == "neutral"


def test_drift_high_bucket():
    from src.agents.auto_recommendations import _classify_agency
    r = _classify_agency(line_count=20, median_drift_pct=22.0,
                         capped_lines=0, capped_pct=0)
    assert r["bucket"] == "drift_high"
    assert "tightening markup floor" in r["suggestion"]
    assert "-5.0%" in r["suggestion"] or "-5.5%" in r["suggestion"]


def test_drift_high_delta_capped_at_max():
    """Even with 100% drift, recommended delta caps at MAX_DELTA_PCT (5)."""
    from src.agents.auto_recommendations import _classify_agency, MAX_DELTA_PCT
    r = _classify_agency(line_count=20, median_drift_pct=100.0,
                         capped_lines=0, capped_pct=0)
    assert r["bucket"] == "drift_high"
    assert f"-{MAX_DELTA_PCT:.1f}%" in r["suggestion"]


def test_drift_low_bucket():
    from src.agents.auto_recommendations import _classify_agency
    r = _classify_agency(line_count=20, median_drift_pct=-10.0,
                         capped_lines=0, capped_pct=0)
    assert r["bucket"] == "drift_low"
    assert "BELOW oracle" in r["suggestion"]
    assert "stale" in r["suggestion"]


def test_cap_working_bucket():
    from src.agents.auto_recommendations import _classify_agency
    r = _classify_agency(line_count=20, median_drift_pct=2.0,
                         capped_lines=15, capped_pct=75.0)
    assert r["bucket"] == "cap_working"
    assert "doing the work" in r["suggestion"]


def test_on_track_bucket():
    from src.agents.auto_recommendations import _classify_agency
    r = _classify_agency(line_count=20, median_drift_pct=3.0,
                         capped_lines=2, capped_pct=10.0)
    assert r["bucket"] == "on_track"
    assert r["color"] == "good"


# ── Aggregator ──────────────────────────────────────────────────────


def test_build_recommendations_empty(temp_data_dir, monkeypatch):
    """Empty operator_drift_line → ok=True with empty recommendations."""
    from src.core.migrations import run_migrations
    run_migrations()
    from src.agents.auto_recommendations import build_auto_recommendations
    rep = build_auto_recommendations(window_days=7)
    assert rep["ok"] is True
    assert rep["recommendations"] == []
    assert rep["total_lines"] == 0
    assert "no operator_drift_line rows" in rep["summary_line"].lower()


def test_build_recommendations_with_drift_data(temp_data_dir, monkeypatch):
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    # Seed 15 lines for cchcs with high drift
    for i in range(15):
        _seed_drift_line(db_path, "cchcs", drift_pct=25.0, quote_id=f"q-cchcs-{i}")
    # Seed 12 lines for calvet on-track
    for i in range(12):
        _seed_drift_line(db_path, "calvet", drift_pct=2.0, quote_id=f"q-calvet-{i}")
    # Seed 4 lines for dsh — insufficient sample
    for i in range(4):
        _seed_drift_line(db_path, "dsh", drift_pct=50.0, quote_id=f"q-dsh-{i}")

    from src.agents.auto_recommendations import build_auto_recommendations
    rep = build_auto_recommendations(window_days=7)
    assert rep["ok"] is True
    assert rep["total_lines"] == 31
    assert len(rep["recommendations"]) == 3
    by_agency = {r["agency"]: r for r in rep["recommendations"]}
    assert by_agency["cchcs"]["bucket"] == "drift_high"
    assert by_agency["calvet"]["bucket"] == "on_track"
    assert by_agency["dsh"]["bucket"] == "insufficient_data"
    # Summary mentions cchcs (the actionable one)
    assert "cchcs" in rep["summary_line"].lower()


def test_build_recommendations_cap_active_bucket(temp_data_dir, monkeypatch):
    """Lines with cap_sources='scprs_rollup' get the cap_working bucket
    when >50% of agency's lines are capped."""
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    for i in range(12):
        _seed_drift_line(db_path, "cchcs",
                         drift_pct=3.0,
                         cap_sources="scprs_rollup",
                         quote_id=f"q-{i}")
    for i in range(4):
        _seed_drift_line(db_path, "cchcs",
                         drift_pct=2.0,
                         cap_sources="",
                         quote_id=f"q-uncap-{i}")
    from src.agents.auto_recommendations import build_auto_recommendations
    rep = build_auto_recommendations(window_days=7)
    cchcs = [r for r in rep["recommendations"] if r["agency"] == "cchcs"][0]
    assert cchcs["bucket"] == "cap_working"
    assert cchcs["capped_pct"] >= 50.0


def test_build_recommendations_ignores_old_rows(temp_data_dir, monkeypatch):
    """Rows outside the window should not appear."""
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    # 15 lines from 20 days ago → outside 7-day window
    for i in range(15):
        _seed_drift_line(db_path, "cchcs", drift_pct=25.0,
                         quote_id=f"q-old-{i}",
                         sent_at_offset_days=20)
    from src.agents.auto_recommendations import build_auto_recommendations
    rep = build_auto_recommendations(window_days=7)
    assert rep["total_lines"] == 0


# ── Digest formatting ───────────────────────────────────────────────


def test_format_for_digest_with_recommendations():
    from src.agents.auto_recommendations import format_for_digest
    report = {
        "summary_line": "1 of 1 agencies need attention: cchcs (drift_high)",
        "recommendations": [{
            "agency": "cchcs", "line_count": 20, "quote_count": 4,
            "median_drift_pct": 22.0, "capped_lines": 0, "capped_pct": 0,
            "bucket": "drift_high",
            "headline": "Pricing 22.0% above oracle",
            "suggestion": "tighten markup floor by -5%",
            "color": "warn",
        }],
    }
    lines = format_for_digest(report)
    body = "\n".join(lines)
    assert "AUTO-RECOMMENDATIONS" in body
    assert "cchcs" in body
    assert "Pricing 22.0% above oracle" in body
    assert "tighten markup" in body


def test_format_for_digest_empty():
    from src.agents.auto_recommendations import format_for_digest
    lines = format_for_digest({
        "summary_line": "no data", "recommendations": [],
    })
    assert "AUTO-RECOMMENDATIONS" in "\n".join(lines)


# ── Admin endpoint ──────────────────────────────────────────────────


def test_admin_auto_recommendations_endpoint(client):
    from src.core.migrations import run_migrations
    run_migrations()
    resp = client.get("/admin/auto-recommendations")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Auto-Recommendations" in body


def test_api_admin_auto_recommendations_json(client):
    from src.core.migrations import run_migrations
    run_migrations()
    resp = client.get("/api/admin/auto-recommendations")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert "recommendations" in data
    assert "summary_line" in data


# ── Oracle weekly integration ───────────────────────────────────────


def test_oracle_weekly_includes_auto_recommendations_section(temp_data_dir):
    """The auto-recommendations section must land in the weekly digest body."""
    from src.core.migrations import run_migrations
    run_migrations()
    from src.agents.oracle_weekly import build_weekly_report
    report = build_weekly_report()
    assert report["ok"] is True
    assert "AUTO-RECOMMENDATIONS" in report["body"]
