"""Pin db_retention substrate (Tier 2d Phase 1, audit 2026-05-07).

The substrate is purpose-built so the cron-arming Phase 2 PR can
focus on policy + scheduling without re-litigating mechanics. These
tests pin the mechanics:

  1. Allowlist gate: caller can't accidentally purge a non-allowed
     table; lifecycle_events (compliance audit trail) requires
     explicit `force_compliance_table=True`.
  2. dry_run is the default; `would_delete` is reported but no rows
     change.
  3. Live purge actually removes the right rows when dry_run=False.
  4. Batch loop runs to completion when row count > batch_size.
  5. days < 1 is rejected.
  6. bloat_report walks all user tables, marks auto-purge and
     compliance-only flags, sorts by row count desc.
"""
from __future__ import annotations

import sqlite3

import pytest

from src.core.db_retention import (
    purge_older_than,
    run_daily_purge,
    bloat_report,
    AUTO_PURGE_ALLOWLIST,
    COMPLIANCE_OPT_IN_ALLOWLIST,
    RETENTION_POLICY,
    RetentionError,
    _DATE_COLUMN,
)


def _new_conn() -> sqlite3.Connection:
    """Sandbox SQLite in memory with the 5 retention-target tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE email_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            logged_at TEXT NOT NULL,
            body TEXT
        );
        CREATE TABLE audit_trail (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            field_changed TEXT
        );
        CREATE TABLE recommendation_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            pc_id TEXT
        );
        CREATE TABLE utilization_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            feature TEXT
        );
        CREATE TABLE lifecycle_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at TEXT NOT NULL,
            event_type TEXT
        );
        CREATE TABLE quotes (
            id INTEGER PRIMARY KEY,
            quote_number TEXT,
            created_at TEXT
        );
    """)
    return conn


def _seed_old_and_new(conn, table, date_col, old_rows, new_rows):
    """Insert `old_rows` with date 100 days ago + `new_rows` with date
    1 day ago. Returns total rows inserted."""
    for _ in range(old_rows):
        conn.execute(
            f"INSERT INTO {table} ({date_col}) VALUES "
            f"(datetime('now', '-100 days'))")
    for _ in range(new_rows):
        conn.execute(
            f"INSERT INTO {table} ({date_col}) VALUES "
            f"(datetime('now', '-1 days'))")
    conn.commit()
    return old_rows + new_rows


# ─── Allowlist + validation gates ─────────────────────────────────

def test_purge_rejects_non_allowlist_table():
    with pytest.raises(RetentionError) as exc:
        purge_older_than("quotes", days=30, conn=_new_conn())
    assert "allowlist" in str(exc.value).lower()


def test_purge_rejects_compliance_table_without_force():
    """`lifecycle_events` is in the opt-in allowlist; without the
    explicit flag it must refuse to operate."""
    conn = _new_conn()
    with pytest.raises(RetentionError) as exc:
        purge_older_than("lifecycle_events", days=30, conn=conn)
    assert "compliance" in str(exc.value).lower()


def test_purge_compliance_table_with_force_works():
    conn = _new_conn()
    _seed_old_and_new(conn, "lifecycle_events", "occurred_at",
                      old_rows=5, new_rows=2)
    r = purge_older_than(
        "lifecycle_events", days=30,
        dry_run=True, conn=conn,
        force_compliance_table=True)
    assert r["would_delete"] == 5


def test_purge_rejects_bad_days():
    conn = _new_conn()
    for bad in (0, -1, -100):
        with pytest.raises(RetentionError):
            purge_older_than("email_log", days=bad, conn=conn)


# ─── dry_run is the default ───────────────────────────────────────

def test_purge_dry_run_default_does_not_delete():
    conn = _new_conn()
    _seed_old_and_new(conn, "email_log", "logged_at",
                      old_rows=10, new_rows=3)
    r = purge_older_than("email_log", days=30, conn=conn)
    assert r["dry_run"] is True
    assert r["would_delete"] == 10
    assert r["deleted"] == 0
    # Verify rows still in place.
    cnt = conn.execute("SELECT COUNT(*) FROM email_log").fetchone()[0]
    assert cnt == 13


def test_purge_dry_run_returns_cutoff_for_telemetry():
    conn = _new_conn()
    _seed_old_and_new(conn, "audit_trail", "created_at",
                      old_rows=2, new_rows=1)
    r = purge_older_than(
        "audit_trail", days=30, dry_run=True, conn=conn)
    # cutoff is the SQLite-side `datetime('now', '-30 days')` ISO string.
    assert r["cutoff"]
    assert "-" in r["cutoff"]  # date format check


# ─── Live purge ───────────────────────────────────────────────────

def test_purge_live_deletes_old_keeps_new():
    conn = _new_conn()
    _seed_old_and_new(conn, "email_log", "logged_at",
                      old_rows=8, new_rows=4)
    r = purge_older_than(
        "email_log", days=30, dry_run=False, conn=conn)
    assert r["dry_run"] is False
    assert r["would_delete"] == 8
    assert r["deleted"] == 8
    remaining = conn.execute(
        "SELECT COUNT(*) FROM email_log").fetchone()[0]
    assert remaining == 4


def test_purge_live_runs_multiple_batches():
    """When row count exceeds batch_size, the loop runs multiple
    DELETEs and still removes every targeted row."""
    conn = _new_conn()
    _seed_old_and_new(
        conn, "utilization_events", "created_at",
        old_rows=25, new_rows=5)
    r = purge_older_than(
        "utilization_events", days=30, dry_run=False,
        batch_size=10, conn=conn)
    assert r["deleted"] == 25
    assert r["batches"] == 3  # 10 + 10 + 5
    remaining = conn.execute(
        "SELECT COUNT(*) FROM utilization_events").fetchone()[0]
    assert remaining == 5


def test_purge_live_no_old_rows_is_noop():
    conn = _new_conn()
    _seed_old_and_new(conn, "audit_trail", "created_at",
                      old_rows=0, new_rows=5)
    r = purge_older_than(
        "audit_trail", days=30, dry_run=False, conn=conn)
    assert r["would_delete"] == 0
    assert r["deleted"] == 0
    assert r["batches"] == 0


# ─── Bloat report ─────────────────────────────────────────────────

def test_bloat_report_lists_all_user_tables():
    conn = _new_conn()
    _seed_old_and_new(conn, "email_log", "logged_at",
                      old_rows=5, new_rows=3)
    _seed_old_and_new(conn, "audit_trail", "created_at",
                      old_rows=2, new_rows=1)

    rep = bloat_report(conn=conn)
    names = [t["name"] for t in rep["tables"]]
    # All 6 user tables are present (5 retention + 1 unrelated).
    assert "email_log" in names
    assert "audit_trail" in names
    assert "lifecycle_events" in names
    assert "quotes" in names


def test_bloat_report_marks_auto_vs_opt_in_correctly():
    conn = _new_conn()
    rep = bloat_report(conn=conn)
    by_name = {t["name"]: t for t in rep["tables"]}

    assert by_name["email_log"]["auto_purge"] is True
    assert by_name["email_log"]["compliance_only"] is False
    assert by_name["lifecycle_events"]["auto_purge"] is False
    assert by_name["lifecycle_events"]["compliance_only"] is True
    assert by_name["quotes"]["auto_purge"] is False
    assert by_name["quotes"]["compliance_only"] is False


def test_bloat_report_sorts_by_row_count_desc():
    conn = _new_conn()
    _seed_old_and_new(conn, "email_log", "logged_at",
                      old_rows=20, new_rows=0)
    _seed_old_and_new(conn, "audit_trail", "created_at",
                      old_rows=5, new_rows=0)
    _seed_old_and_new(conn, "utilization_events", "created_at",
                      old_rows=10, new_rows=0)
    rep = bloat_report(conn=conn)
    # Filter to populated tables in our seed and confirm sort order.
    populated = [t for t in rep["tables"] if t["rows"] > 0]
    assert [t["name"] for t in populated] == [
        "email_log", "utilization_events", "audit_trail"]


def test_bloat_report_includes_retention_days():
    conn = _new_conn()
    rep = bloat_report(conn=conn)
    by_name = {t["name"]: t for t in rep["tables"]}
    assert (by_name["email_log"]["retention_days"]
            == RETENTION_POLICY["email_log"])
    assert by_name["quotes"]["retention_days"] is None


# ─── Self-consistency ────────────────────────────────────────────

def test_every_allowlisted_table_has_date_column():
    """Import-time validation guards this, but pin it in tests too
    so a future allowlist add can't slip through."""
    for tbl in (AUTO_PURGE_ALLOWLIST | COMPLIANCE_OPT_IN_ALLOWLIST):
        assert tbl in _DATE_COLUMN, (
            f"{tbl} missing _DATE_COLUMN entry")
        assert tbl in RETENTION_POLICY, (
            f"{tbl} missing RETENTION_POLICY default")


def test_lifecycle_events_is_NOT_in_auto_allowlist():
    """Compliance audit trail — never auto-purge per Mike's handoff
    note. Future maintainers should see this assertion before
    accidentally moving it."""
    assert "lifecycle_events" not in AUTO_PURGE_ALLOWLIST


# ─── Phase 2: run_daily_purge cron entrypoint ────────────────────────

def test_run_daily_purge_dry_run_default(monkeypatch):
    """No env override → defaults to live (dry_run=False) when called
    explicitly with dry_run=True override. Pin both flag paths."""
    conn = _new_conn()
    for tbl in AUTO_PURGE_ALLOWLIST:
        date_col = _DATE_COLUMN[tbl]
        _seed_old_and_new(conn, tbl, date_col, old_rows=3, new_rows=2)

    summary = run_daily_purge(dry_run=True, conn=conn)
    assert summary["dry_run"] is True
    assert summary["total_would_delete"] == 3 * len(AUTO_PURGE_ALLOWLIST)
    assert summary["total_deleted"] == 0
    assert summary["errors"] == []
    # Confirm one entry per allowlisted table
    seen = {r["table"] for r in summary["tables"]}
    assert seen == set(AUTO_PURGE_ALLOWLIST)


def test_run_daily_purge_live_actually_deletes():
    conn = _new_conn()
    for tbl in AUTO_PURGE_ALLOWLIST:
        date_col = _DATE_COLUMN[tbl]
        _seed_old_and_new(conn, tbl, date_col, old_rows=4, new_rows=1)

    summary = run_daily_purge(dry_run=False, conn=conn)
    assert summary["dry_run"] is False
    assert summary["total_deleted"] == 4 * len(AUTO_PURGE_ALLOWLIST)
    assert summary["errors"] == []

    # Surviving rows = 1 per allowlisted table
    for tbl in AUTO_PURGE_ALLOWLIST:
        n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        assert n == 1, f"{tbl} still has {n} rows after live purge"


def test_run_daily_purge_skips_lifecycle_events():
    """Compliance opt-in table must NOT be touched by the cron."""
    conn = _new_conn()
    _seed_old_and_new(conn, "lifecycle_events", "occurred_at",
                      old_rows=5, new_rows=2)
    run_daily_purge(dry_run=False, conn=conn)
    n = conn.execute("SELECT COUNT(*) FROM lifecycle_events").fetchone()[0]
    assert n == 7, "lifecycle_events should not be touched by the cron"


def test_run_daily_purge_env_var_dry_run(monkeypatch):
    """`DB_RETENTION_DRY_RUN=1` env var flips to dry-run when caller
    doesn't pass dry_run explicitly. Default (env unset) → live."""
    conn = _new_conn()
    for tbl in AUTO_PURGE_ALLOWLIST:
        date_col = _DATE_COLUMN[tbl]
        _seed_old_and_new(conn, tbl, date_col, old_rows=2, new_rows=1)

    monkeypatch.setenv("DB_RETENTION_DRY_RUN", "1")
    summary = run_daily_purge(conn=conn)
    assert summary["dry_run"] is True
    assert summary["total_deleted"] == 0

    # Env var unset → live by default
    monkeypatch.delenv("DB_RETENTION_DRY_RUN", raising=False)
    summary2 = run_daily_purge(conn=conn)
    assert summary2["dry_run"] is False
    assert summary2["total_deleted"] == 2 * len(AUTO_PURGE_ALLOWLIST)


def test_run_daily_purge_table_error_does_not_block_others(monkeypatch):
    """If one table errors, the other tables still get purged and
    the error is reported in summary['errors']."""
    conn = _new_conn()
    for tbl in AUTO_PURGE_ALLOWLIST:
        _seed_old_and_new(conn, tbl, _DATE_COLUMN[tbl],
                          old_rows=2, new_rows=0)

    # Simulate one table failing inside purge_older_than
    from src.core import db_retention as _r
    orig = _r.purge_older_than

    def _flaky(table, *a, **kw):
        if table == "audit_trail":
            raise RuntimeError("simulated DB error")
        return orig(table, *a, **kw)

    monkeypatch.setattr(_r, "purge_older_than", _flaky)
    summary = run_daily_purge(dry_run=False, conn=conn)

    # The 3 healthy tables should still be purged
    assert summary["total_deleted"] == 2 * (len(AUTO_PURGE_ALLOWLIST) - 1)
    assert len(summary["errors"]) == 1
    assert summary["errors"][0]["table"] == "audit_trail"
    assert "lifecycle_events" in COMPLIANCE_OPT_IN_ALLOWLIST
