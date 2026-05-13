"""PR-Q — QA/QC heartbeat tests.

Pinned guarantees:
  1. Migration 49 lands `qa_heartbeat` table + 3 indexes on a fresh DB.
  2. Each check function is pure: takes a conn, returns a dict with
     {status, value, threshold, message}. Never raises.
  3. `run_all_checks` isolates failures — one broken check doesn't
     kill the suite.
  4. `run_and_persist` writes one row per check + bounds the table
     (deletes rows older than 30d).
  5. `latest_status` returns the most-recent row per check_name.
  6. Threshold ladders work: scprs freshness, dedup rate, sent rate.
  7. Admin route + JSON endpoint exist and render.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _init(tmp_path, monkeypatch):
    tmp_db = tmp_path / "qa_test.db"
    monkeypatch.setenv("REYTECH_DB_PATH", str(tmp_db))
    import importlib
    from src.core import db as _db_mod
    importlib.reload(_db_mod)
    _db_mod.init_db()
    from src.core.migrations import run_migrations
    run_migrations()
    return _db_mod


# ── Migration ────────────────────────────────────────────────────────


def test_migration_49_creates_qa_heartbeat(tmp_path, monkeypatch):
    db = _init(tmp_path, monkeypatch)
    with db.get_db() as conn:
        cols = [r["name"] for r in conn.execute(
            "PRAGMA table_info(qa_heartbeat)").fetchall()]
        assert "check_name" in cols
        assert "status" in cols
        assert "value_json" in cols
        assert "ran_at" in cols
        assert "cycle_id" in cols
        idxs = [r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name='qa_heartbeat'").fetchall()]
        assert "idx_qa_check_ran" in idxs
        assert "idx_qa_status" in idxs


# ── Threshold ladders ────────────────────────────────────────────────


def test_scprs_awards_freshness_fail_when_stale(tmp_path, monkeypatch):
    """Inject a row whose created_at is 200h ago → fail rung."""
    db = _init(tmp_path, monkeypatch)
    stale = (datetime.utcnow() - timedelta(hours=200)).isoformat()
    with db.get_db() as conn:
        conn.execute("""
            INSERT INTO scprs_awards (id, po_number, agency, vendor_name,
              award_date, fiscal_year, total_value, item_count,
              source, tenant_id, created_at)
            VALUES (?, '1', 'CCHCS', 'X', '01/01/2026', '2026', 100, 1,
                    'test', 'reytech', ?)
        """, ("fixture-stale", stale))
    from src.core.qa_heartbeat import check_scprs_awards_freshness
    with db.get_db() as conn:
        r = check_scprs_awards_freshness(conn)
    assert r["status"] == "fail"
    assert r["value"]["row_count"] == 1
    assert r["value"]["hours_since"] >= 168  # 7-day threshold


def test_scprs_awards_freshness_pass_when_fresh(tmp_path, monkeypatch):
    db = _init(tmp_path, monkeypatch)
    fresh = (datetime.utcnow() - timedelta(hours=2)).isoformat()
    with db.get_db() as conn:
        conn.execute("""
            INSERT INTO scprs_awards (id, po_number, agency, vendor_name,
              award_date, fiscal_year, total_value, item_count,
              source, tenant_id, created_at)
            VALUES (?, '1', 'CCHCS', 'X', '01/01/2026', '2026', 100, 1,
                    'test', 'reytech', ?)
        """, ("fixture-fresh", fresh))
    from src.core.qa_heartbeat import check_scprs_awards_freshness
    with db.get_db() as conn:
        r = check_scprs_awards_freshness(conn)
    assert r["status"] == "pass"


def test_dedup_rate_warn_and_fail(tmp_path, monkeypatch):
    """Seed 10 PCs with 6 duplicates → 60% → fail (≥50%)."""
    db = _init(tmp_path, monkeypatch)
    now = datetime.utcnow().isoformat()
    with db.get_db() as conn:
        for i in range(6):
            conn.execute(
                "INSERT INTO price_checks (id, created_at, status) VALUES (?,?,?)",
                (f"d{i}", now, "duplicate"))
        for i in range(4):
            conn.execute(
                "INSERT INTO price_checks (id, created_at, status) VALUES (?,?,?)",
                (f"s{i}", now, "sent"))
    from src.core.qa_heartbeat import check_pc_dedup_rate
    with db.get_db() as conn:
        r = check_pc_dedup_rate(conn)
    assert r["status"] == "fail"
    assert r["value"]["duplicate_pct"] == 60.0


def test_dedup_rate_pass_when_low(tmp_path, monkeypatch):
    db = _init(tmp_path, monkeypatch)
    now = datetime.utcnow().isoformat()
    with db.get_db() as conn:
        for i in range(2):
            conn.execute(
                "INSERT INTO price_checks (id, created_at, status) VALUES (?,?,?)",
                (f"d{i}", now, "duplicate"))
        for i in range(10):
            conn.execute(
                "INSERT INTO price_checks (id, created_at, status) VALUES (?,?,?)",
                (f"s{i}", now, "sent"))
    from src.core.qa_heartbeat import check_pc_dedup_rate
    with db.get_db() as conn:
        r = check_pc_dedup_rate(conn)
    assert r["status"] == "pass"
    assert r["value"]["duplicate_pct"] < 30.0


def test_sent_rate_fail_when_low(tmp_path, monkeypatch):
    """Seed 10 PCs with 1 sent → 10% → fail (≤10%)."""
    db = _init(tmp_path, monkeypatch)
    now = datetime.utcnow().isoformat()
    with db.get_db() as conn:
        conn.execute(
            "INSERT INTO price_checks (id, created_at, status) VALUES (?,?,?)",
            ("s0", now, "sent"))
        for i in range(9):
            conn.execute(
                "INSERT INTO price_checks (id, created_at, status) VALUES (?,?,?)",
                (f"x{i}", now, "draft"))
    from src.core.qa_heartbeat import check_pc_sent_rate
    with db.get_db() as conn:
        r = check_pc_sent_rate(conn)
    assert r["status"] == "fail"


# ── Runners ──────────────────────────────────────────────────────────


def test_run_all_checks_isolates_failures(tmp_path, monkeypatch):
    """If one check raises, others still run + return fail-with-message."""
    _init(tmp_path, monkeypatch)
    from src.core import qa_heartbeat as qh

    def _boom(conn, ctx=None):
        raise RuntimeError("intentional test boom")

    monkeypatch.setattr(qh, "CHECKS", [
        ("boom_check", _boom),
        ("dedup_check", qh.check_pc_dedup_rate),
    ])
    results = qh.run_all_checks()
    assert "boom_check" in results
    assert results["boom_check"]["status"] == "fail"
    assert "intentional test boom" in results["boom_check"]["message"]
    assert "dedup_check" in results
    # dedup ran successfully despite boom_check exploding
    assert results["dedup_check"]["status"] in ("pass", "warn")


def test_run_and_persist_writes_one_row_per_check(tmp_path, monkeypatch):
    db = _init(tmp_path, monkeypatch)
    from src.core.qa_heartbeat import run_and_persist, CHECKS
    result = run_and_persist()
    assert result["cycle_id"]
    assert len(result["results"]) == len(CHECKS)
    with db.get_db() as conn:
        rows = conn.execute(
            "SELECT check_name, status FROM qa_heartbeat "
            "WHERE cycle_id=?", (result["cycle_id"],),
        ).fetchall()
    assert len(rows) == len(CHECKS)
    names = {r["check_name"] for r in rows}
    expected = {name for name, _ in CHECKS}
    assert names == expected


def test_run_and_persist_prunes_old_rows(tmp_path, monkeypatch):
    """Rows older than 30 days should get pruned on each persist."""
    db = _init(tmp_path, monkeypatch)
    old = (datetime.utcnow() - timedelta(days=40)).isoformat()
    with db.get_db() as conn:
        conn.execute(
            "INSERT INTO qa_heartbeat "
            "(ran_at, check_name, status, value_json, threshold, message, cycle_id) "
            "VALUES (?,?,?,?,?,?,?)",
            (old, "fixture_old", "pass", "{}", "—", "old", "old-cycle"))
    from src.core.qa_heartbeat import run_and_persist
    run_and_persist()
    with db.get_db() as conn:
        n_old = conn.execute(
            "SELECT COUNT(*) AS n FROM qa_heartbeat WHERE cycle_id='old-cycle'"
        ).fetchone()["n"]
    assert n_old == 0


def test_latest_status_returns_one_row_per_check(tmp_path, monkeypatch):
    _init(tmp_path, monkeypatch)
    from src.core.qa_heartbeat import run_and_persist, latest_status, CHECKS
    run_and_persist()
    run_and_persist()  # second cycle — same check_names, different cycle_id
    latest = latest_status()
    expected = {name for name, _ in CHECKS}
    assert set(latest.keys()) == expected
    for v in latest.values():
        assert "status" in v
        assert v["status"] in ("pass", "warn", "fail")


# ── Routes ───────────────────────────────────────────────────────────


def test_admin_qa_heartbeat_endpoint_renders(client):
    from src.core.migrations import run_migrations
    run_migrations()
    resp = client.get("/admin/qa/heartbeat?run=1")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "QA Heartbeat" in body or "QA / QC heartbeat" in body
    # The 7 check names should all appear in the table
    for name in ("scprs_awards_freshness", "pc_dedup_rate",
                 "oracle_audit_attach_rate"):
        assert name in body


def test_api_admin_qa_heartbeat_json(client):
    from src.core.migrations import run_migrations
    run_migrations()
    resp = client.post("/api/admin/qa/heartbeat")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert "cycle_id" in data
    assert "results" in data
    # Each check returns proper structure
    for name, r in data["results"].items():
        assert "status" in r
        assert "value" in r
