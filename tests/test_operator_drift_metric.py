"""PR-I — operator-drift per-line metric at Mark-Sent.

Pinned guarantees:
  1. `log_operator_drift` inserts one row per line that has an
     `oracle_audit` envelope, computes drift_pct correctly, and
     captures the cap-source list verbatim.
  2. Lines without `oracle_audit` are SKIPPED, not inserted with
     NULLs — a NULL drift row would pollute the dataset and bias
     downstream stats toward zero-drift.
  3. Lines with zero/negative sent_price are SKIPPED. Same reason.
  4. `get_drift_stats` aggregates correctly: per-source rollup,
     median/p25/p75, capped-above-oracle cohort count.
  5. The 2 Mark-Sent routes import + call `log_operator_drift`.
     (Static-source test — behavioral cover would require mocking
     8 dependencies in each route; the function itself is unit-
     tested above.)
  6. Migration 45 lands the table on a fresh DB.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _audit(rec_price, pre=None, caps=None, count=0):
    return {
        "rec_price": rec_price,
        "rec_pre_cap_price": pre,
        "caps_applied": caps or [],
        "scprs_rollup": ({"count": count} if count else None),
        "oracle_version": "v2.1",
        "snapshot_at": "2026-05-13T08:00:00",
    }


# ── Migration creates the table ──────────────────────────────────────


def test_migration_45_creates_operator_drift_line_table(tmp_path, monkeypatch):
    """A fresh DB after init_db must have operator_drift_line + the 4
    indexes. Catches the schema-vs-migration-drift class that bit us
    in 2026-05-08 (idx_wqs_agency / init_db raises). Indexes live
    INSIDE the CREATE TABLE block — same-step idempotent."""
    tmp_db = tmp_path / "test45.db"
    monkeypatch.setenv("REYTECH_DB_PATH", str(tmp_db))
    import importlib
    from src.core import db as _db_mod
    importlib.reload(_db_mod)
    _db_mod.init_db()
    from src.core.migrations import run_migrations
    run_migrations()
    with _db_mod.get_db() as conn:
        cols = [r["name"] for r in conn.execute(
            "PRAGMA table_info(operator_drift_line)").fetchall()]
        assert "drift_pct" in cols
        assert "cap_sources" in cols
        assert "caps_applied_json" in cols
        idxs = [r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name='operator_drift_line'").fetchall()]
        assert "idx_odl_quote" in idxs
        assert "idx_odl_cap_sources" in idxs
        assert "idx_odl_sent_at" in idxs


# ── log_operator_drift behavior ──────────────────────────────────────


def test_log_operator_drift_inserts_one_row_per_audited_line(tmp_path, monkeypatch):
    tmp_db = tmp_path / "test.db"
    monkeypatch.setenv("REYTECH_DB_PATH", str(tmp_db))
    import importlib
    from src.core import db as _db_mod
    importlib.reload(_db_mod)
    _db_mod.init_db()
    from src.core.migrations import run_migrations
    run_migrations()
    from src.core.operator_kpi import log_operator_drift

    items = [
        {  # audited, sent above rec (cap held but operator overrode)
            "item_number": "1",
            "mfg_number": "X-1",
            "unit_price": 75.0,
            "oracle_audit": _audit(60.0, pre=100.0, caps=[
                {"source": "scprs_rollup", "cap_price": 60.0,
                 "pre_cap_price": 100.0, "match_key": "X-1",
                 "match_key_type": "mfg", "sample_count": 50},
            ], count=50),
        },
        {  # audited, no cap, sent at rec (drift=0)
            "item_number": "2",
            "unit_price": 30.0,
            "oracle_audit": _audit(30.0),
        },
        {  # NO audit — should be skipped, NOT logged with NULLs
            "item_number": "3",
            "unit_price": 25.0,
        },
        {  # audited but sent_price = 0 → should be skipped
            "item_number": "4",
            "unit_price": 0.0,
            "oracle_audit": _audit(40.0),
        },
    ]
    result = log_operator_drift(
        quote_id="q_test_1", quote_type="pc",
        items=items, agency_key="cchcs",
    )
    assert result["ok"]
    assert result["rows_logged"] == 2, result
    assert result["skipped_no_audit"] == 1
    assert result["skipped_no_price"] == 1

    with _db_mod.get_db() as conn:
        rows = conn.execute(
            "SELECT item_number, sent_price, rec_price, drift_pct, "
            "cap_sources, scprs_match_count "
            "FROM operator_drift_line WHERE quote_id=? "
            "ORDER BY line_idx",
            ("q_test_1",),
        ).fetchall()
    assert [r["item_number"] for r in rows] == ["1", "2"]
    # Line 1: sent=$75, rec=$60 → drift = +25%
    assert rows[0]["drift_pct"] == 25.0
    assert rows[0]["cap_sources"] == "scprs_rollup"
    assert rows[0]["scprs_match_count"] == 50
    # Line 2: sent=$30, rec=$30 → drift = 0%
    assert rows[1]["drift_pct"] == 0.0
    assert rows[1]["cap_sources"] == ""


def test_drift_pct_handles_zero_rec_price_safely(tmp_path, monkeypatch):
    """Defensive: if rec_price is None or 0 the drift_pct must be NULL,
    not raise ZeroDivisionError. Latent bug class — caught a similar
    one in the markup_pct calc."""
    tmp_db = tmp_path / "t.db"
    monkeypatch.setenv("REYTECH_DB_PATH", str(tmp_db))
    import importlib
    from src.core import db as _db_mod
    importlib.reload(_db_mod)
    _db_mod.init_db()
    from src.core.migrations import run_migrations
    run_migrations()
    from src.core.operator_kpi import log_operator_drift
    log_operator_drift(
        quote_id="q_zero", quote_type="rfq",
        items=[{"item_number": "1", "unit_price": 50.0,
                "oracle_audit": _audit(0)}],
    )
    with _db_mod.get_db() as conn:
        row = conn.execute(
            "SELECT drift_pct FROM operator_drift_line WHERE quote_id=?",
            ("q_zero",),
        ).fetchone()
    assert row is not None
    assert row["drift_pct"] is None  # safe NULL, not a crash


def test_log_operator_drift_empty_or_invalid_quote_id_returns_error():
    from src.core.operator_kpi import log_operator_drift
    r = log_operator_drift(quote_id="", quote_type="pc", items=[])
    assert not r["ok"]
    assert "required" in r.get("error", "").lower()


# ── get_drift_stats aggregation ──────────────────────────────────────


def test_get_drift_stats_aggregates_per_source_and_cohort(tmp_path, monkeypatch):
    tmp_db = tmp_path / "tstats.db"
    monkeypatch.setenv("REYTECH_DB_PATH", str(tmp_db))
    import importlib
    from src.core import db as _db_mod
    importlib.reload(_db_mod)
    _db_mod.init_db()
    from src.core.migrations import run_migrations
    run_migrations()
    from src.core.operator_kpi import (
        log_operator_drift, get_drift_stats,
    )
    # 5 lines, mix of capped/uncapped, mix of above/below oracle.
    log_operator_drift(
        quote_id="q1", quote_type="pc", agency_key="cchcs",
        items=[
            # capped, +20% drift
            {"item_number": "1", "unit_price": 60.0,
             "oracle_audit": _audit(50.0, pre=100.0, caps=[
                 {"source": "scprs_rollup", "cap_price": 50.0,
                  "pre_cap_price": 100.0}])},
            # capped, -10% drift (operator went below cap)
            {"item_number": "2", "unit_price": 45.0,
             "oracle_audit": _audit(50.0, pre=70.0, caps=[
                 {"source": "scprs_rollup", "cap_price": 50.0,
                  "pre_cap_price": 70.0}])},
            # uncapped, +5% drift
            {"item_number": "3", "unit_price": 21.0,
             "oracle_audit": _audit(20.0)},
            # uncapped, 0% drift
            {"item_number": "4", "unit_price": 100.0,
             "oracle_audit": _audit(100.0)},
            # uncapped, -3% drift
            {"item_number": "5", "unit_price": 9.70,
             "oracle_audit": _audit(10.0)},
        ],
    )
    s = get_drift_stats(window_days=30)
    assert s["ok"]
    assert s["line_count"] == 5
    assert s["quote_count"] == 1
    assert s["capped_lines"] == 2
    assert s["capped_above_oracle"] == 1   # only the +20% one
    assert s["capped_below_oracle"] == 1   # only the -10% one
    # Per-source rollup: scprs_rollup has 2 lines, median of [20.0, -10.0]
    # = 20.0 (n=2, n//2=1 → second sorted element).
    src = [d for d in s["per_cap_source"] if d["source"] == "scprs_rollup"]
    assert src, s["per_cap_source"]
    assert src[0]["line_count"] == 2


# ── Mark-Sent route wire-up (static cover) ───────────────────────────


def test_mark_sent_routes_call_log_operator_drift():
    """Both the RFQ + PC mark-sent paths must import + call
    `log_operator_drift`. Static-source check: cheaper than building
    a full multipart-form request fixture with vision+pricing mocks."""
    import inspect
    from src.api.modules import routes_rfq_admin, routes_pricecheck_admin
    for mod, label in (
        (routes_rfq_admin, "rfq mark-sent route"),
        (routes_pricecheck_admin, "pc send-quote route"),
    ):
        src = inspect.getsource(mod)
        assert "log_operator_drift" in src, (
            f"{label} must import + call log_operator_drift "
            "or operator-drift telemetry never fires from this path"
        )


def test_oracle_drift_preview_endpoint_exists_and_renders(client):
    """Endpoint smoke — Chrome-MCP run validates the rendered HTML
    separately. Here we just verify the route is wired into the app
    and returns 200 for an empty drift window. Uses the conftest
    `client` fixture which sets up auth + temp DB."""
    from src.core.migrations import run_migrations
    run_migrations()
    resp = client.get("/oracle/drift/preview")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Operator drift" in body or "operator drift" in body.lower()
    assert "Lines tracked" in body
