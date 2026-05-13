"""PR-Z — cap-inactive warning banner on /home.

The walkthrough audit (P2-9) flagged: pre-PR-Z the SCPRS rollup cap
auto-disabled silently when scprs_awards went stale. Exact failure
mode from the pre-PR-O 60-day awards freeze. If the bridge ever
regresses, we get unbounded pricing for weeks with no operator
signal. PR-Z surfaces a banner with the specific reason on /home.

Pinned guarantees:
  1. `scprs_rollup_cap_state` returns operator-readable reason +
     message + age_days context.
  2. ORACLE_USE_SCPRS_ROLLUP=on → enabled True, reason='env_on'.
  3. ORACLE_USE_SCPRS_ROLLUP=off → enabled False, reason='env_off'
     (intentional, banner does NOT render).
  4. Empty scprs_awards → enabled False, reason='no_data'.
  5. Stale scprs_awards (>30d) → enabled False, reason='stale_data'.
  6. Fresh scprs_awards → enabled True, reason='fresh_data'.
  7. /home renders banner when enabled=False AND reason != 'env_off'.
  8. /home suppresses banner when env_off (explicit operator choice).
"""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _seed_award(db_path, age_days):
    """Insert one scprs_awards row with created_at age_days old."""
    created = (datetime.now() - timedelta(days=age_days)).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO scprs_awards "
            "(id, po_number, agency, vendor_name, award_date, "
            "fiscal_year, total_value, item_count, source, tenant_id, "
            "created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (f"a-{age_days}", f"PO-{age_days}", "cchcs", "V",
             "01/01/2026", "FY26", 100, 1, "test", "reytech", created),
        )
        conn.commit()
    finally:
        conn.close()


# ── cap_state ──────────────────────────────────────────────────────


def test_cap_state_env_on(temp_data_dir, monkeypatch):
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "on")
    from src.core.pricing_oracle_v2 import scprs_rollup_cap_state
    s = scprs_rollup_cap_state()
    assert s["enabled"] is True
    assert s["reason"] == "env_on"


def test_cap_state_env_off(temp_data_dir, monkeypatch):
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "off")
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    _seed_award(db_path, age_days=1)  # even fresh data doesn't override env-off
    from src.core.pricing_oracle_v2 import scprs_rollup_cap_state
    s = scprs_rollup_cap_state()
    assert s["enabled"] is False
    assert s["reason"] == "env_off"


def test_cap_state_no_data(temp_data_dir, monkeypatch):
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    from src.core.migrations import run_migrations
    run_migrations()
    from src.core.pricing_oracle_v2 import scprs_rollup_cap_state
    s = scprs_rollup_cap_state()
    assert s["enabled"] is False
    assert s["reason"] == "no_data"
    assert "empty" in s["message"].lower()


def test_cap_state_fresh_data(temp_data_dir, monkeypatch):
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    _seed_award(db_path, age_days=2)
    from src.core.pricing_oracle_v2 import scprs_rollup_cap_state
    s = scprs_rollup_cap_state()
    assert s["enabled"] is True
    assert s["reason"] == "fresh_data"
    assert s["last_award_age_days"] is not None and s["last_award_age_days"] <= 3


def test_cap_state_stale_data(temp_data_dir, monkeypatch):
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    _seed_award(db_path, age_days=60)  # pre-PR-O failure mode
    from src.core.pricing_oracle_v2 import scprs_rollup_cap_state
    s = scprs_rollup_cap_state()
    assert s["enabled"] is False
    assert s["reason"] == "stale_data"
    assert s["last_award_age_days"] >= 60
    # Message names the threshold so operator knows what to fix
    assert "bridge" in s["message"].lower()


# ── /home banner ──────────────────────────────────────────────────


def test_home_renders_banner_when_no_data(client, temp_data_dir, monkeypatch):
    """Empty scprs_awards → banner renders with reason explanation."""
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    from src.core.migrations import run_migrations
    run_migrations()
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'data-testid="cap-inactive-banner"' in body
    assert "SCPRS rollup cap is INACTIVE" in body
    assert "no_data" in body


def test_home_renders_banner_when_stale(client, temp_data_dir, monkeypatch):
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    _seed_award(db_path, age_days=60)
    body = client.get("/").get_data(as_text=True)
    assert 'data-testid="cap-inactive-banner"' in body
    assert "stale_data" in body


def test_home_suppresses_banner_when_env_off(client, temp_data_dir, monkeypatch):
    """env_off is operator's explicit choice — banner does NOT render
    (otherwise we'd nag forever after the operator made a deliberate
    config decision)."""
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "off")
    from src.core.migrations import run_migrations
    run_migrations()
    body = client.get("/").get_data(as_text=True)
    assert 'data-testid="cap-inactive-banner"' not in body


def test_home_suppresses_banner_when_cap_active(client, temp_data_dir,
                                                  monkeypatch):
    """Fresh awards → cap enabled → banner does NOT render."""
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    from src.core.migrations import run_migrations
    run_migrations()
    db_path = os.path.join(temp_data_dir, "reytech.db")
    _seed_award(db_path, age_days=1)
    body = client.get("/").get_data(as_text=True)
    assert 'data-testid="cap-inactive-banner"' not in body


def test_existing_cap_enabled_helper_still_works(temp_data_dir, monkeypatch):
    """_scprs_rollup_cap_enabled delegates to scprs_rollup_cap_state —
    the original boolean API must still return correctly."""
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "on")
    from src.core.pricing_oracle_v2 import _scprs_rollup_cap_enabled
    assert _scprs_rollup_cap_enabled() is True
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "off")
    assert _scprs_rollup_cap_enabled() is False
