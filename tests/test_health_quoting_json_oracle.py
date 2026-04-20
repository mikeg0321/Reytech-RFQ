"""Locks the `oracle_calibration` field shape on the JSON variant of
/health/quoting (the endpoint downstream monitors scrape).

The page route has unit coverage in test_oracle_health_card.py. This
file covers the HTTP contract: if someone renames a field, drops a key,
or breaks auth, external dashboards silently break — tests here catch
that before merge.

Scope is deliberately narrow: one happy-path shape assertion + one
populated-data shape assertion. Full status-logic coverage already
lives in the unit tests.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta


REQUIRED_KEYS = {
    "status", "rows", "wins", "losses_price", "losses_other",
    "losses_total", "win_rate_pct", "agencies", "is_stale",
    "days_since_update", "last_updated",
}


def test_api_health_quoting_exposes_oracle_calibration_shape(auth_client):
    """Default empty DB: endpoint must return ok=True and include the
    full oracle_calibration block with status='no_data'. External
    monitors key off the `status` and `rows` fields."""
    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True

    cal = data.get("oracle_calibration")
    assert cal is not None, (
        "oracle_calibration missing from JSON payload — downstream "
        "monitors will break. Check routes_health.quoting_health_json()."
    )
    missing = REQUIRED_KEYS - set(cal.keys())
    assert not missing, f"oracle_calibration missing keys: {missing}"

    # Empty DB → no_data status, None win_rate (not 0.0 — the distinction
    # lets monitors show '--' vs '0%').
    assert cal["status"] == "no_data"
    assert cal["rows"] == 0
    assert cal["win_rate_pct"] is None


def test_api_health_quoting_reports_populated_oracle_stats(auth_client, temp_data_dir):
    """With rows in oracle_calibration, the JSON payload reflects real
    aggregates. Guards against silent regressions where _build_oracle_
    calibration_card() stops aggregating (e.g., SQL error swallowed)."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    fresh = (datetime.now() - timedelta(hours=6)).isoformat()
    conn.execute("""
        INSERT INTO oracle_calibration
            (category, agency, sample_size, win_count,
             loss_on_price, loss_on_other, last_updated)
        VALUES (?,?,?,?,?,?,?)
    """, ("medical", "CDCR", 18, 5, 10, 3, fresh))
    conn.commit()
    conn.close()

    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    cal = resp.get_json()["oracle_calibration"]

    assert cal["status"] == "healthy"
    assert cal["rows"] == 1
    assert cal["wins"] == 5
    assert cal["losses_price"] == 10
    assert cal["losses_other"] == 3
    assert cal["losses_total"] == 13
    # 5 / (5 + 13) = 27.78%
    assert cal["win_rate_pct"] == 27.8
    assert cal["is_stale"] is False
