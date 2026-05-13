"""PR-J — shadow-mode cap evaluator (Generator-Critic).

At every Mark-Sent, walk items + log counterfactual "what would the
SCPRS p75 cap have done if enabled?" Lets Mike answer "should I flip
the cap flag on?" with data instead of a coin flip.

Pinned guarantees:
  1. `log_operator_drift_shadow` inserts one row per item with
     `oracle_audit.scprs_rollup.p75` data. Lines without rollup are
     skipped (skipped_no_rollup counter increments).
  2. shadow_action classification:
     - cap currently OFF + sent > p75 → 'would_cap'
     - cap currently OFF + sent ≤ p75 → 'no_cap'
     - cap currently ON                → 'cap_active'
  3. shadow_drift_pct = (sent - p75) / p75 × 100. Positive = operator
     above shadow cap (cap would have hurt margin).
  4. `get_shadow_stats` rolls up correctly: would/no/active counts +
     total $ savings extrapolation.
  5. Both Mark-Sent routes call `log_operator_drift_shadow`.
  6. Migration 46 lands the table on a fresh DB.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _audit(rec=60.0, p75=None, count=50):
    return {
        "rec_price": rec, "rec_pre_cap_price": None,
        "caps_applied": [],
        "scprs_rollup": (
            {"count": count, "p50": p75 - 5 if p75 else None, "p75": p75,
             "p90": p75 + 10 if p75 else None,
             "match_key": "X-1", "match_key_type": "mfg",
             "agency": "cchcs", "year": "*", "qty_band": "10-49"}
            if p75 else None
        ),
        "oracle_version": "v2.1",
        "snapshot_at": "2026-05-13T08:00:00",
    }


def _init_test_db(tmp_path, monkeypatch):
    tmp_db = tmp_path / "shadow_test.db"
    monkeypatch.setenv("REYTECH_DB_PATH", str(tmp_db))
    import importlib
    from src.core import db as _db_mod
    importlib.reload(_db_mod)
    _db_mod.init_db()
    from src.core.migrations import run_migrations
    run_migrations()
    return _db_mod


# ── Migration 46 ─────────────────────────────────────────────────────


def test_migration_46_creates_operator_drift_shadow(tmp_path, monkeypatch):
    _db = _init_test_db(tmp_path, monkeypatch)
    with _db.get_db() as conn:
        cols = [r["name"] for r in conn.execute(
            "PRAGMA table_info(operator_drift_shadow)").fetchall()]
        assert "shadow_cap_price" in cols
        assert "shadow_action" in cols
        assert "shadow_drift_pct" in cols
        assert "rollup_p75" in cols
        idxs = [r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name='operator_drift_shadow'").fetchall()]
        assert "idx_ods_shadow_action" in idxs


# ── Action classification ────────────────────────────────────────────


def test_shadow_action_would_cap_when_sent_above_p75(tmp_path, monkeypatch):
    """Cap flag OFF, operator sent $75 with p75=$60 → would_cap."""
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    _db = _init_test_db(tmp_path, monkeypatch)
    from src.core.operator_kpi import log_operator_drift_shadow
    result = log_operator_drift_shadow(
        quote_id="q1", quote_type="pc", agency_key="cchcs",
        items=[{"item_number": "1", "unit_price": 75.0,
                "oracle_audit": _audit(p75=60.0)}],
    )
    assert result["ok"]
    assert result["rows_logged"] == 1
    with _db.get_db() as conn:
        row = conn.execute(
            "SELECT shadow_action, shadow_drift_pct, shadow_cap_price, "
            "rollup_p75 FROM operator_drift_shadow"
        ).fetchone()
    assert row["shadow_action"] == "would_cap"
    assert row["shadow_cap_price"] == 60.0
    assert row["rollup_p75"] == 60.0
    # (75 - 60) / 60 * 100 = 25%
    assert row["shadow_drift_pct"] == 25.0


def test_shadow_action_no_cap_when_sent_at_or_below_p75(tmp_path, monkeypatch):
    """Cap flag OFF, operator sent $55 with p75=$60 → no_cap (cap
    would not have moved the line)."""
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    _db = _init_test_db(tmp_path, monkeypatch)
    from src.core.operator_kpi import log_operator_drift_shadow
    log_operator_drift_shadow(
        quote_id="q2", quote_type="pc",
        items=[{"item_number": "1", "unit_price": 55.0,
                "oracle_audit": _audit(p75=60.0)}],
    )
    with _db.get_db() as conn:
        row = conn.execute(
            "SELECT shadow_action, shadow_drift_pct FROM operator_drift_shadow"
        ).fetchone()
    assert row["shadow_action"] == "no_cap"
    # Operator was already below the shadow cap — negative drift
    assert row["shadow_drift_pct"] < 0


def test_shadow_action_cap_active_when_flag_on(tmp_path, monkeypatch):
    """Cap flag ON. Regardless of whether the live recommendation
    actually capped, the shadow row records 'cap_active' so digest
    can distinguish dormant-cap windows from live-cap windows."""
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    _db = _init_test_db(tmp_path, monkeypatch)
    from src.core.operator_kpi import log_operator_drift_shadow
    log_operator_drift_shadow(
        quote_id="q3", quote_type="pc",
        items=[{"item_number": "1", "unit_price": 75.0,
                "oracle_audit": _audit(p75=60.0)}],
    )
    with _db.get_db() as conn:
        row = conn.execute(
            "SELECT shadow_action FROM operator_drift_shadow"
        ).fetchone()
    assert row["shadow_action"] == "cap_active"


# ── Skip conditions ──────────────────────────────────────────────────


def test_lines_without_rollup_data_are_skipped(tmp_path, monkeypatch):
    """No rollup data → not logged (vs polluting the dataset with
    NULL shadow_cap_price). Same discipline as log_operator_drift
    skipping lines without oracle_audit."""
    _db = _init_test_db(tmp_path, monkeypatch)
    from src.core.operator_kpi import log_operator_drift_shadow
    result = log_operator_drift_shadow(
        quote_id="q_skip", quote_type="pc",
        items=[
            # Has audit but no rollup
            {"item_number": "1", "unit_price": 50.0,
             "oracle_audit": _audit(p75=None)},
            # No audit at all
            {"item_number": "2", "unit_price": 30.0},
            # Has audit + valid rollup
            {"item_number": "3", "unit_price": 75.0,
             "oracle_audit": _audit(p75=60.0)},
        ],
    )
    assert result["rows_logged"] == 1
    assert result["skipped_no_rollup"] == 2
    with _db.get_db() as conn:
        row = conn.execute(
            "SELECT item_number FROM operator_drift_shadow"
        ).fetchone()
    assert row["item_number"] == "3"


def test_p75_zero_or_invalid_skips_safely(tmp_path, monkeypatch):
    """Defensive: a malformed rollup with p75=0 must NOT raise
    ZeroDivisionError on the drift calc — it must just skip."""
    _db = _init_test_db(tmp_path, monkeypatch)
    from src.core.operator_kpi import log_operator_drift_shadow
    items = [{"item_number": "1", "unit_price": 50.0,
              "oracle_audit": _audit(p75=0)}]  # p75=0
    result = log_operator_drift_shadow(
        quote_id="q_p0", quote_type="pc", items=items)
    assert result["rows_logged"] == 0


# ── Aggregation ──────────────────────────────────────────────────────


def test_get_shadow_stats_rolls_up_actions_and_savings(tmp_path, monkeypatch):
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    _db = _init_test_db(tmp_path, monkeypatch)
    from src.core.operator_kpi import (
        log_operator_drift_shadow, get_shadow_stats,
    )
    log_operator_drift_shadow(
        quote_id="q1", quote_type="pc", agency_key="cchcs",
        items=[
            # would_cap, $15 over p75
            {"item_number": "1", "unit_price": 75.0,
             "oracle_audit": _audit(p75=60.0)},
            # would_cap, $10 over p75
            {"item_number": "2", "unit_price": 70.0,
             "oracle_audit": _audit(p75=60.0)},
            # no_cap (at p75)
            {"item_number": "3", "unit_price": 60.0,
             "oracle_audit": _audit(p75=60.0)},
            # no_cap (below)
            {"item_number": "4", "unit_price": 55.0,
             "oracle_audit": _audit(p75=60.0)},
        ],
    )
    s = get_shadow_stats(window_days=30)
    assert s["ok"]
    assert s["line_count"] == 4
    assert s["would_cap_count"] == 2
    assert s["no_cap_count"] == 2
    assert s["cap_active_count"] == 0
    # Average $ saved per capped line: ($15 + $10) / 2 = $12.50
    assert s["avg_savings_per_capped_line"] == 12.50
    # Total dollars left if cap had been live: $25
    assert s["total_savings_if_capped"] == 25.0


def test_get_shadow_stats_empty_window_returns_safe_zeroes(tmp_path, monkeypatch):
    _init_test_db(tmp_path, monkeypatch)
    from src.core.operator_kpi import get_shadow_stats
    s = get_shadow_stats(window_days=30)
    assert s["ok"]
    assert s["line_count"] == 0
    assert s["total_savings_if_capped"] == 0.0
    assert s["avg_savings_per_capped_line"] is None


# ── Route wire-up (static cover) ─────────────────────────────────────


def test_both_mark_sent_routes_call_shadow_logger():
    import inspect
    from src.api.modules import routes_rfq_admin, routes_pricecheck_admin
    for mod, label in (
        (routes_rfq_admin, "rfq mark-sent"),
        (routes_pricecheck_admin, "pc send-quote"),
    ):
        src = inspect.getsource(mod)
        assert "log_operator_drift_shadow" in src, (
            f"{label} must call log_operator_drift_shadow — without it "
            "the shadow surface is empty on this path"
        )


def test_drift_preview_renders_shadow_section_when_no_data(client):
    """When operator_drift_shadow is empty, preview must still render
    a 'no shadow-eligible lines yet' message (not 500). Same defensive
    discipline as digest preview's empty state."""
    from src.core.migrations import run_migrations
    run_migrations()
    resp = client.get("/oracle/drift/preview")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Shadow-mode cap evaluator" in body


def test_drift_preview_renders_would_cap_kpi_with_data(client):
    """With seeded shadow rows, preview shows the would_cap count +
    extrapolated savings."""
    from src.core.migrations import run_migrations
    run_migrations()
    from src.core.operator_kpi import log_operator_drift_shadow
    log_operator_drift_shadow(
        quote_id="q_preview", quote_type="pc",
        items=[{"item_number": "1", "unit_price": 80.0,
                "oracle_audit": _audit(p75=60.0)}],
    )
    resp = client.get("/oracle/drift/preview")
    body = resp.get_data(as_text=True)
    assert "would_cap" in body
    # $80 sent, $60 p75 → $20 left on the table
    assert "$20.00" in body
