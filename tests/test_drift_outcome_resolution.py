"""PR-K1 — outcome resolution on drift rows.

Joins drift signal → WR signal. Without this, the digest can show
drift distributions but can't answer "of lines with drift > 20%,
what's the WR?" — the actual leverage query for cap tuning.

Pinned guarantees:
  1. Migrations 47+48 land outcome/outcome_at/outcome_source/
     quote_number columns + indexes on a fresh DB.
  2. `resolve_drift_outcome` updates drift_line + drift_shadow rows
     matching quote_id OR quote_number.
  3. Re-detection (idempotency): a second resolve_drift_outcome call
     does NOT overwrite an earlier outcome.
  4. `resolve_drift_outcome` rejects invalid outcomes.
  5. `get_drift_wr_breakdown` correctly classifies lines into
     high/low drift cohorts and computes WR for each.
  6. award_monitor + quote_lifecycle both call resolve_drift_outcome.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _audit(rec=60.0, p75=None, caps=None):
    return {
        "rec_price": rec, "rec_pre_cap_price": None,
        "caps_applied": caps or [],
        "scprs_rollup": (
            {"count": 50, "p75": p75, "match_key": "X", "match_key_type": "mfg"}
            if p75 else None
        ),
        "oracle_version": "v2.1", "snapshot_at": "2026-05-13",
    }


def _init(tmp_path, monkeypatch):
    tmp_db = tmp_path / "k1_test.db"
    monkeypatch.setenv("REYTECH_DB_PATH", str(tmp_db))
    import importlib
    from src.core import db as _db_mod
    importlib.reload(_db_mod)
    _db_mod.init_db()
    from src.core.migrations import run_migrations
    run_migrations()
    return _db_mod


# ── Migrations 47+48 ─────────────────────────────────────────────────


def test_migration_47_adds_outcome_columns(tmp_path, monkeypatch):
    db = _init(tmp_path, monkeypatch)
    with db.get_db() as conn:
        cols_line = [r["name"] for r in conn.execute(
            "PRAGMA table_info(operator_drift_line)").fetchall()]
        cols_shadow = [r["name"] for r in conn.execute(
            "PRAGMA table_info(operator_drift_shadow)").fetchall()]
        for col in ("outcome", "outcome_at", "outcome_source", "quote_number"):
            assert col in cols_line, f"missing on drift_line: {col}"
            assert col in cols_shadow, f"missing on drift_shadow: {col}"


def test_migration_48_adds_outcome_indexes(tmp_path, monkeypatch):
    db = _init(tmp_path, monkeypatch)
    with db.get_db() as conn:
        idxs = [r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()]
        assert "idx_odl_outcome" in idxs
        assert "idx_odl_quote_number" in idxs
        assert "idx_ods_outcome" in idxs
        assert "idx_ods_quote_number" in idxs


# ── resolve_drift_outcome ────────────────────────────────────────────


def test_resolve_via_quote_id_updates_both_tables(tmp_path, monkeypatch):
    db = _init(tmp_path, monkeypatch)
    from src.core.operator_kpi import (
        log_operator_drift, log_operator_drift_shadow,
        resolve_drift_outcome,
    )
    items = [{"item_number": "1", "unit_price": 75.0,
              "oracle_audit": _audit(rec=60.0, p75=60.0)}]
    log_operator_drift(quote_id="pc_1", quote_type="pc",
                       items=items, quote_number="R26Q1")
    log_operator_drift_shadow(quote_id="pc_1", quote_type="pc",
                              items=items, quote_number="R26Q1")
    r = resolve_drift_outcome(quote_id="pc_1", outcome="won",
                              source="award_monitor")
    assert r["ok"]
    assert r["drift_rows_updated"] == 1
    assert r["shadow_rows_updated"] == 1
    with db.get_db() as conn:
        line_row = conn.execute(
            "SELECT outcome, outcome_source FROM operator_drift_line"
        ).fetchone()
        shadow_row = conn.execute(
            "SELECT outcome FROM operator_drift_shadow"
        ).fetchone()
    assert line_row["outcome"] == "won"
    assert line_row["outcome_source"] == "award_monitor"
    assert shadow_row["outcome"] == "won"


def test_resolve_via_quote_number_when_id_unknown(tmp_path, monkeypatch):
    """Reply-signal path knows quote_number but not pc.id — must still
    resolve."""
    db = _init(tmp_path, monkeypatch)
    from src.core.operator_kpi import (
        log_operator_drift, resolve_drift_outcome,
    )
    items = [{"item_number": "1", "unit_price": 50.0,
              "oracle_audit": _audit(rec=60.0)}]
    log_operator_drift(quote_id="pc_2", quote_type="pc",
                       items=items, quote_number="R26Q42")
    r = resolve_drift_outcome(quote_number="R26Q42", outcome="lost",
                              source="reply_signal")
    assert r["drift_rows_updated"] == 1
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT outcome FROM operator_drift_line"
        ).fetchone()
    assert row["outcome"] == "lost"


def test_resolve_is_idempotent_no_overwrite(tmp_path, monkeypatch):
    """A second resolve call (re-detected award, replayed signal) must
    NOT change the outcome — only NULL rows get backfilled. This pins
    against double-write race conditions."""
    db = _init(tmp_path, monkeypatch)
    from src.core.operator_kpi import (
        log_operator_drift, resolve_drift_outcome,
    )
    items = [{"item_number": "1", "unit_price": 75.0,
              "oracle_audit": _audit(rec=60.0)}]
    log_operator_drift(quote_id="pc_3", quote_type="pc", items=items)

    r1 = resolve_drift_outcome(quote_id="pc_3", outcome="won",
                               source="award_monitor")
    assert r1["drift_rows_updated"] == 1

    # Re-detect — different outcome shouldn't take. The first call wins.
    r2 = resolve_drift_outcome(quote_id="pc_3", outcome="lost",
                               source="duplicate_signal")
    assert r2["drift_rows_updated"] == 0

    with db.get_db() as conn:
        row = conn.execute(
            "SELECT outcome, outcome_source FROM operator_drift_line"
        ).fetchone()
    assert row["outcome"] == "won"
    assert row["outcome_source"] == "award_monitor"


def test_resolve_rejects_invalid_outcome(tmp_path, monkeypatch):
    _init(tmp_path, monkeypatch)
    from src.core.operator_kpi import resolve_drift_outcome
    r = resolve_drift_outcome(quote_id="pc_x", outcome="maybe")
    assert not r["ok"]
    assert "invalid" in r["error"].lower()


def test_resolve_requires_at_least_one_join_key(tmp_path, monkeypatch):
    _init(tmp_path, monkeypatch)
    from src.core.operator_kpi import resolve_drift_outcome
    r = resolve_drift_outcome(outcome="won", source="x")
    assert not r["ok"]


# ── get_drift_wr_breakdown ───────────────────────────────────────────


def test_drift_wr_breakdown_classifies_by_threshold(tmp_path, monkeypatch):
    """The leverage query: seed 4 lines (2 high drift, 2 low),
    resolve them with mixed outcomes, verify the WR split."""
    db = _init(tmp_path, monkeypatch)
    from src.core.operator_kpi import (
        log_operator_drift, resolve_drift_outcome,
        get_drift_wr_breakdown,
    )
    # High-drift line, won
    log_operator_drift(
        quote_id="q_hw", quote_type="pc",
        items=[{"item_number": "1", "unit_price": 80.0,
                "oracle_audit": _audit(rec=60.0)}],  # +33% drift
    )
    resolve_drift_outcome(quote_id="q_hw", outcome="won",
                          source="test")
    # High-drift line, lost
    log_operator_drift(
        quote_id="q_hl", quote_type="pc",
        items=[{"item_number": "1", "unit_price": 90.0,
                "oracle_audit": _audit(rec=60.0)}],  # +50% drift
    )
    resolve_drift_outcome(quote_id="q_hl", outcome="lost",
                          source="test")
    # Low-drift line, won
    log_operator_drift(
        quote_id="q_lw", quote_type="pc",
        items=[{"item_number": "1", "unit_price": 62.0,
                "oracle_audit": _audit(rec=60.0)}],  # +3% drift
    )
    resolve_drift_outcome(quote_id="q_lw", outcome="won",
                          source="test")
    # Low-drift line, won
    log_operator_drift(
        quote_id="q_lw2", quote_type="pc",
        items=[{"item_number": "1", "unit_price": 60.0,
                "oracle_audit": _audit(rec=60.0)}],  # 0% drift
    )
    resolve_drift_outcome(quote_id="q_lw2", outcome="won",
                          source="test")

    s = get_drift_wr_breakdown(window_days=30, drift_threshold_pct=20.0)
    assert s["ok"]
    assert s["resolved_lines"] == 4
    assert s["high_drift_lines"] == 2
    assert s["low_drift_lines"] == 2
    assert s["high_drift_won"] == 1
    assert s["high_drift_lost"] == 1
    assert s["high_drift_wr"] == 50.0
    assert s["low_drift_won"] == 2
    assert s["low_drift_lost"] == 0
    assert s["low_drift_wr"] == 100.0
    # wr_delta = high_wr - low_wr = 50 - 100 = -50 → high drift hurts
    assert s["wr_delta"] == -50.0


def test_drift_wr_breakdown_empty_returns_safe_nulls(tmp_path, monkeypatch):
    _init(tmp_path, monkeypatch)
    from src.core.operator_kpi import get_drift_wr_breakdown
    s = get_drift_wr_breakdown(window_days=30)
    assert s["ok"]
    assert s["resolved_lines"] == 0
    assert s["high_drift_wr"] is None
    assert s["low_drift_wr"] is None
    assert s["wr_delta"] is None


# ── Wire-up static cover ─────────────────────────────────────────────


def test_award_monitor_calls_resolve_drift_outcome():
    import inspect
    from src.agents import award_monitor
    src = inspect.getsource(award_monitor)
    assert "resolve_drift_outcome" in src, (
        "award_monitor must call resolve_drift_outcome on both "
        "win + loss branches — without it the drift→WR join is "
        "never populated from the SCPRS award path"
    )
    # Must fire on BOTH branches (won AND lost)
    assert src.count("resolve_drift_outcome") >= 2


def test_quote_lifecycle_calls_resolve_drift_outcome():
    import inspect
    from src.agents import quote_lifecycle
    src = inspect.getsource(quote_lifecycle)
    assert "resolve_drift_outcome" in src, (
        "quote_lifecycle.process_reply_signal must call "
        "resolve_drift_outcome — without it, reply-signal-driven "
        "outcomes never reach the drift tables"
    )


def test_mark_sent_routes_thread_quote_number(client):
    """Static cover that both Mark-Sent routes pass quote_number= to
    the drift loggers. Without this, quote_lifecycle's join key has
    nothing to match on."""
    import inspect
    from src.api.modules import routes_rfq_admin, routes_pricecheck_admin
    for mod, label in (
        (routes_rfq_admin, "rfq"),
        (routes_pricecheck_admin, "pc"),
    ):
        src = inspect.getsource(mod)
        assert "quote_number=" in src, (
            f"{label} mark-sent must pass quote_number= to drift "
            "loggers"
        )
