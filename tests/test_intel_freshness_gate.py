"""Tests for the boot intelligence rebuild freshness gate shipped
2026-05-27 to close the deploy-cycle lock cascade Mr. Wolf caught in
prod log 06:01-06:08 (deployment 28d5adc7, sha f82f6025).

The gate skips the boot rebuild when `scprs_awards` already has 100+
rows and was rebuilt within the last 7 days. Pre-fix the boot rebuild
ran on every deploy, colliding with workflow_runs/upsert_quote/
record_price writers; WAL + 30s busy_timeout + 3-attempt retry
couldn't absorb the contention on a 2.2 GB DB.

These tests pin the gate logic. The boot-thread wiring is pinned via
source-grep in `test_boot_rebuild_uses_gate` below.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.core.intel_freshness_gate import intel_tables_fresh


REPO_ROOT = Path(__file__).resolve().parents[1]


def _build_db(path: str, rows: int, ts: str | None) -> None:
    """Create a minimal scprs_awards table with N rows. ts (ISO8601)
    is the timestamp for the most recent row's created_at — None means
    leave NULL."""
    with sqlite3.connect(path) as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS scprs_awards (
                id TEXT PRIMARY KEY,
                created_at TEXT
            )
        """)
        c.execute("DELETE FROM scprs_awards")
        for i in range(rows):
            # Last row carries the freshness timestamp; earlier rows
            # use an older timestamp so MAX() picks ts.
            row_ts = ts if i == rows - 1 else "2026-01-01T00:00:00+00:00"
            c.execute(
                "INSERT INTO scprs_awards (id, created_at) VALUES (?, ?)",
                (f"po_{i}", row_ts),
            )
        c.commit()


def test_returns_true_when_fresh_and_full(tmp_path):
    """Healthy state: 38k+ rows in prod, last rebuild < 7 days ago."""
    db = str(tmp_path / "rey.db")
    now = datetime.now(timezone.utc).isoformat()
    _build_db(db, rows=200, ts=now)
    assert intel_tables_fresh(db) is True


def test_returns_false_when_table_empty(tmp_path):
    """First deploy / empty volume — must rebuild to bootstrap."""
    db = str(tmp_path / "rey.db")
    _build_db(db, rows=0, ts=None)
    assert intel_tables_fresh(db) is False


def test_returns_false_when_below_min_rows(tmp_path):
    """Half-bootstrapped DB (< 100 rows) — keep rebuilding."""
    db = str(tmp_path / "rey.db")
    now = datetime.now(timezone.utc).isoformat()
    _build_db(db, rows=50, ts=now)
    assert intel_tables_fresh(db) is False


def test_returns_false_when_stale(tmp_path):
    """Post-outage: 200 rows but last rebuild > 7 days ago. Run rebuild
    to catch up — this is the case the docstring carves out."""
    db = str(tmp_path / "rey.db")
    stale = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
    _build_db(db, rows=200, ts=stale)
    assert intel_tables_fresh(db) is False


def test_returns_false_when_db_missing(tmp_path):
    """DB file missing → fail closed, rebuild runs."""
    assert intel_tables_fresh(str(tmp_path / "does_not_exist.db")) is False


def test_returns_false_when_table_missing(tmp_path):
    """DB exists but scprs_awards table absent → fail closed."""
    db = str(tmp_path / "rey.db")
    sqlite3.connect(db).close()  # empty file
    assert intel_tables_fresh(db) is False


def test_threshold_boundary_at_min_rows(tmp_path):
    """Exactly at min_rows is the inclusive boundary — passing 100 is
    fresh, 99 is not. Pin the >= comparison."""
    db = str(tmp_path / "rey.db")
    now = datetime.now(timezone.utc).isoformat()
    _build_db(db, rows=100, ts=now)
    assert intel_tables_fresh(db, min_rows=100) is True
    _build_db(db, rows=99, ts=now)
    assert intel_tables_fresh(db, min_rows=100) is False


def test_threshold_boundary_at_max_age(tmp_path):
    """Exactly at max_age_days boundary. Just-recently-rebuilt
    (< cutoff) is fresh; just-past (> cutoff) is stale."""
    db = str(tmp_path / "rey.db")
    # Build rows with timestamp = cutoff - 1s (fresh) and cutoff + 1s
    # (stale). The gate compares lex; both ISO8601 strings work.
    fresh_ts = (
        datetime.now(timezone.utc) - timedelta(days=6, hours=23)
    ).isoformat()
    _build_db(db, rows=200, ts=fresh_ts)
    assert intel_tables_fresh(db, max_age_days=7) is True

    stale_ts = (
        datetime.now(timezone.utc) - timedelta(days=7, hours=1)
    ).isoformat()
    _build_db(db, rows=200, ts=stale_ts)
    assert intel_tables_fresh(db, max_age_days=7) is False


def test_custom_thresholds_passed_through(tmp_path):
    """Caller can override min_rows / max_age_days (e.g. tests, manual
    admin endpoints). Pin pass-through."""
    db = str(tmp_path / "rey.db")
    ts_5d = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    _build_db(db, rows=50, ts=ts_5d)

    # Default thresholds: 50 rows + 5 days → False (< 100 min_rows)
    assert intel_tables_fresh(db) is False
    # Override min_rows=10 → True
    assert intel_tables_fresh(db, min_rows=10) is True
    # Override max_age_days=3 → False (5d > 3d)
    assert intel_tables_fresh(db, min_rows=10, max_age_days=3) is False


def test_default_path_resolves_to_data_dir(monkeypatch, tmp_path):
    """Calling with no args defaults to DATA_DIR/reytech.db. Lets the
    boot thread say `intel_tables_fresh()` without computing the path."""
    db = str(tmp_path / "reytech.db")
    now = datetime.now(timezone.utc).isoformat()
    _build_db(db, rows=200, ts=now)
    import src.core.intel_freshness_gate as gate_mod
    monkeypatch.setattr(gate_mod, "DATA_DIR", str(tmp_path))
    assert gate_mod.intel_tables_fresh() is True


def test_passes_code_patterns_startup_check():
    """The file MUST import `from src.core` to satisfy
    `startup_checks.check_code_patterns` Pattern C — the linter that
    allows bare sqlite3.connect only when the file imports init_db,
    get_db, or anything from src.core. Pin against a future refactor
    that removes the DATA_DIR import for 'cleanliness' — that would
    re-trigger the prod STARTUP FAIL caught 2026-05-27 on deployment
    9f0e8924 (`STARTUP FAIL: Code patterns — 1 code issues: core/
    intel_freshness_gate.py: direct sqlite3.connect without any db
    guard`)."""
    src = (
        REPO_ROOT / "src" / "core" / "intel_freshness_gate.py"
    ).read_text(encoding="utf-8")
    assert "sqlite3.connect(" in src, "Helper still uses sqlite3.connect"
    assert "from src.core" in src, (
        "intel_freshness_gate.py MUST import from src.core to satisfy "
        "startup_checks Pattern C."
    )


# ── Source-grep: the boot thread MUST use the gate ────────────────────


def test_boot_rebuild_uses_gate():
    """Pin that `_boot_rebuild_awards` actually calls `intel_tables_fresh`
    and returns early when it's True. Pre-fix this thread ran rebuild
    unconditionally."""
    src = (
        REPO_ROOT / "src" / "api" / "modules" / "routes_intel_ops.py"
    ).read_text(encoding="utf-8")
    # The boot thread body
    start = src.find("def _boot_rebuild_awards():")
    assert start > 0
    # Take the function body (up to the next blank-line dedent of 4 spaces)
    end = src.find("    threading.Thread(target=_boot_rebuild_awards", start)
    assert end > start
    body = src[start:end]

    assert "from src.core.intel_freshness_gate import intel_tables_fresh" in body, (
        "_boot_rebuild_awards must import intel_tables_fresh"
    )
    assert "if intel_tables_fresh(" in body, (
        "_boot_rebuild_awards must guard the rebuild on intel_tables_fresh()"
    )
    assert "Boot rebuild SKIPPED" in body, (
        "Skip path must log so operator sees the gate fired"
    )
