"""PR-R — SCPRS rollup cap binding (active mode for PR-J shadow).

The 2026-05-13 forensics showed we lost 94-105% over the winning
competitor on confirmed CCHCS losses. PR-J had been logging "would have
capped" rows in shadow for weeks. PR-R activates the cap: when the
oracle recommendation is above the agency-specific p75 of the SCPRS
rollup AND we have enough samples to trust the bucket, lower the
quote_price to p75.

Pinned guarantees:
  1. `_apply_scprs_rollup_cap` lowers `recommendation.quote_price` to
     p75 when current > p75 AND count >= MIN_SAMPLES AND p75 > 0.
  2. The pre-cap price is preserved on `recommendation.quote_price_pre_cap`.
  3. `caps_applied` is appended with structured cap metadata.
  4. Cap is a no-op when current_price <= p75 (already at or below cap).
  5. Cap is a no-op when count < SCPRS_CAP_MIN_SAMPLES (untrusted bucket).
  6. Cap is a no-op when scprs_rollup is missing/None.
  7. `_scprs_rollup_cap_enabled()` reads env first, falls back to
     auto-detect via scprs_awards freshness.
  8. ORACLE_USE_SCPRS_ROLLUP=off explicitly disables the cap even when
     awards are fresh.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── _scprs_rollup_cap_enabled ────────────────────────────────────────


def test_cap_enabled_when_env_explicit_on(temp_data_dir, monkeypatch):
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "on")
    from src.core.pricing_oracle_v2 import _scprs_rollup_cap_enabled
    assert _scprs_rollup_cap_enabled() is True


def test_cap_disabled_when_env_explicit_off(temp_data_dir, monkeypatch):
    """Off wins even when scprs_awards is fresh."""
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "off")
    # Seed fresh award (proves env precedence over auto-detect)
    import sqlite3
    db_path = os.path.join(temp_data_dir, "reytech.db")
    from src.core.migrations import run_migrations
    run_migrations()
    now = datetime.now().isoformat()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO scprs_awards "
            "(id, po_number, agency, vendor_name, award_date, "
            "fiscal_year, total_value, item_count, source, tenant_id, "
            "created_at) "
            "VALUES ('a1','PO1','cchcs','V','01/01/2026','FY26',100,1,"
            "'test','reytech',?)",
            (now,),
        )
        conn.commit()
    finally:
        conn.close()
    from src.core.pricing_oracle_v2 import _scprs_rollup_cap_enabled
    assert _scprs_rollup_cap_enabled() is False


def test_cap_auto_active_when_awards_fresh(temp_data_dir, monkeypatch):
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    from src.core.migrations import run_migrations
    run_migrations()
    import sqlite3
    db_path = os.path.join(temp_data_dir, "reytech.db")
    now = datetime.now().isoformat()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO scprs_awards "
            "(id, po_number, agency, vendor_name, award_date, "
            "fiscal_year, total_value, item_count, source, tenant_id, "
            "created_at) "
            "VALUES ('a1','PO1','cchcs','V','01/01/2026','FY26',100,1,"
            "'test','reytech',?)",
            (now,),
        )
        conn.commit()
    finally:
        conn.close()
    from src.core.pricing_oracle_v2 import _scprs_rollup_cap_enabled
    assert _scprs_rollup_cap_enabled() is True


def test_cap_auto_disabled_when_awards_stale(temp_data_dir, monkeypatch):
    """The pre-PR-O failure mode: awards frozen 60 days → auto-detect
    must keep the cap dormant (don't price against stale percentiles)."""
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    from src.core.migrations import run_migrations
    run_migrations()
    import sqlite3
    db_path = os.path.join(temp_data_dir, "reytech.db")
    stale = (datetime.now() - timedelta(days=60)).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO scprs_awards "
            "(id, po_number, agency, vendor_name, award_date, "
            "fiscal_year, total_value, item_count, source, tenant_id, "
            "created_at) "
            "VALUES ('a1','PO1','cchcs','V','01/01/2026','FY26',100,1,"
            "'test','reytech',?)",
            (stale,),
        )
        conn.commit()
    finally:
        conn.close()
    from src.core.pricing_oracle_v2 import _scprs_rollup_cap_enabled
    assert _scprs_rollup_cap_enabled() is False


def test_cap_auto_disabled_when_awards_empty(temp_data_dir, monkeypatch):
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    from src.core.migrations import run_migrations
    run_migrations()
    from src.core.pricing_oracle_v2 import _scprs_rollup_cap_enabled
    assert _scprs_rollup_cap_enabled() is False


# ── _apply_scprs_rollup_cap ──────────────────────────────────────────


def _make_result(quote_price, p75=None, count=10, p50=None, p90=None):
    """Build a minimal pricing_oracle_v2 result dict for cap testing."""
    return {
        "recommendation": {
            "quote_price": quote_price,
            "rationale": "test rec",
        },
        "scprs_rollup": (
            {"count": count, "p50": p50 or p75, "p75": p75,
             "p90": p90 or p75, "match_key": "MFG-X",
             "match_key_type": "mfg"}
            if p75 is not None else None
        ),
    }


def test_cap_lowers_price_to_p75_when_above():
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    result = _make_result(quote_price=100.0, p75=70.0, count=10)
    _apply_scprs_rollup_cap(result)
    rec = result["recommendation"]
    assert rec["quote_price"] == 70.0
    assert rec["quote_price_pre_cap"] == 100.0
    assert len(rec["caps_applied"]) == 1
    cap = rec["caps_applied"][0]
    assert cap["source"] == "scprs_rollup"
    assert cap["cap_price"] == 70.0
    assert cap["pre_cap_price"] == 100.0
    assert cap["sample_count"] == 10
    assert "p75" in rec["rationale"]


def test_cap_noop_when_already_at_or_below_p75():
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    result = _make_result(quote_price=65.0, p75=70.0, count=10)
    _apply_scprs_rollup_cap(result)
    rec = result["recommendation"]
    assert rec["quote_price"] == 65.0
    assert "quote_price_pre_cap" not in rec
    assert not rec.get("caps_applied")


def test_cap_noop_when_count_below_min_samples():
    """SCPRS_CAP_MIN_SAMPLES=5 — count=3 means bucket is too thin to trust."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    result = _make_result(quote_price=100.0, p75=20.0, count=3)
    _apply_scprs_rollup_cap(result)
    rec = result["recommendation"]
    assert rec["quote_price"] == 100.0  # untouched
    assert "quote_price_pre_cap" not in rec


def test_cap_noop_when_rollup_missing():
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    result = _make_result(quote_price=100.0, p75=None)
    _apply_scprs_rollup_cap(result)
    rec = result["recommendation"]
    assert rec["quote_price"] == 100.0


def test_cap_noop_when_p75_zero():
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    result = _make_result(quote_price=100.0, p75=0.0, count=10)
    _apply_scprs_rollup_cap(result)
    assert result["recommendation"]["quote_price"] == 100.0


def test_cap_records_delta_pct_in_caps_applied():
    """Delta = (pre - cap) / cap * 100 — used by the digest to surface
    the cap's actual impact per agency."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    result = _make_result(quote_price=150.0, p75=100.0, count=10)
    _apply_scprs_rollup_cap(result)
    cap = result["recommendation"]["caps_applied"][0]
    assert cap["delta_pct"] == 50.0  # 150 was 50% above 100


def test_cap_preserves_prior_rationale():
    """Operator-readable rationale shouldn't lose its prior content."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    result = _make_result(quote_price=100.0, p75=70.0, count=10)
    result["recommendation"]["rationale"] = "category-intel: PPE band"
    _apply_scprs_rollup_cap(result)
    rationale = result["recommendation"]["rationale"]
    assert "category-intel: PPE band" in rationale
    assert "SCPRS rollup cap" in rationale
