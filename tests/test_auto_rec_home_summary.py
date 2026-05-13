"""PR-W — auto-recommendations summary on /home.

The walkthrough audit found /admin/auto-recommendations dashboard had
zero links from /home or the main nav. The diagnostic shipped this
morning (PR-S) was orphaned. PR-W surfaces the top actionable
recommendation directly on the operator's main screen.

Pinned guarantees:
  1. /home route computes `auto_rec_summary` dict from
     build_auto_recommendations.
  2. Empty drift table → empty auto_rec_summary → panel does NOT render
     (no empty-state noise).
  3. drift_high bucket → panel renders + deep-link to
     /admin/auto-recommendations.
  4. Builder crash → panel skipped (auto_rec_summary stays empty),
     /home still renders.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _seed_drift_line(db_path, agency, drift_pct, cap_sources="",
                     quote_id="q1", sent_at_offset_days=0):
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


def test_home_renders_without_drift_data(client, temp_data_dir):
    """Empty drift table → panel does NOT render. /home still 200s."""
    from src.core.migrations import run_migrations
    run_migrations()
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'data-testid="auto-rec-summary"' not in body, \
        "Panel must NOT render when there's no actionable bucket"


def test_home_renders_panel_with_drift_high_bucket(client, temp_data_dir):
    """Seed drift_high agency → panel renders with deep-link."""
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    # 15 lines with high drift on cchcs → drift_high bucket
    for i in range(15):
        _seed_drift_line(db_path, "cchcs", drift_pct=25.0,
                         quote_id=f"q-{i}")
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'data-testid="auto-rec-summary"' in body
    assert 'data-testid="auto-rec-deeplink"' in body
    assert '/admin/auto-recommendations' in body
    assert "cchcs" in body.lower()


def test_home_panel_contains_actionable_bucket_text(client, temp_data_dir):
    """Panel shows the headline + suggestion of the top actionable bucket."""
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    for i in range(12):
        _seed_drift_line(db_path, "calvet", drift_pct=22.0,
                         quote_id=f"q-cv-{i}")
    resp = client.get("/")
    body = resp.get_data(as_text=True)
    # Headline string includes the median drift percentage
    assert "Pricing 22.0% above oracle" in body or "above oracle" in body
    # Suggestion mentions the markup-tuning action
    assert "tighten markup floor" in body.lower() or "markup floor" in body.lower()


def test_home_panel_skips_when_only_on_track(client, temp_data_dir):
    """When all agencies are on-track (drift within tolerance) → panel
    does not render (no actionable buckets)."""
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    for i in range(20):
        _seed_drift_line(db_path, "cchcs", drift_pct=3.0,
                         quote_id=f"q-ok-{i}")
    resp = client.get("/")
    body = resp.get_data(as_text=True)
    assert 'data-testid="auto-rec-summary"' not in body, \
        "Panel must NOT render when no agency has an actionable bucket"


def test_home_still_renders_when_builder_crashes(client, temp_data_dir,
                                                   monkeypatch):
    """Defensive: any error in build_auto_recommendations must not
    block /home rendering."""
    from src.core.migrations import run_migrations
    run_migrations()
    import src.agents.auto_recommendations as ar

    def _boom(window_days=7):
        raise RuntimeError("simulated agent crash")

    monkeypatch.setattr(ar, "build_auto_recommendations", _boom)
    resp = client.get("/")
    assert resp.status_code == 200
