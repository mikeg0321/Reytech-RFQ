"""Pin: _quote_ingestion_freshness reads spine_quotes from Spine DB,
not from the legacy dashboard DB.

PR-A (chrome MCP audit 2026-05-26 anomaly #1): prod liveness probe
reported `spine_quotes: query failed (no such table: spine_quotes)`
because the check opened a SINGLE connection via `src.core.db.get_db()`
(the legacy dashboard DB at `data/reytech.db`) and queried both
tables against that one connection. The Spine table lives in
`data/spine.db` per §0 LAW 1 — same substrate-singleness class as
PRs #1076 / #1086 / #1088.

These tests fail under the pre-fix code (single get_db() conn) and
pass under the post-fix code (separate sqlite3.connect to
SPINE_DB_PATH).
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import datetime, timezone


def _now_iso(offset_minutes: int = 0) -> str:
    """ISO timestamp `offset_minutes` ago. Negative = future (we never use)."""
    from datetime import timedelta
    return (datetime.now(timezone.utc)
            - timedelta(minutes=offset_minutes)).isoformat()


def test_spine_quotes_read_from_separate_db(monkeypatch, tmp_path):
    """When spine_quotes lives in data/spine.db, the check finds it
    and reports its age — NOT 'no such table'."""
    # Build an isolated Spine DB with one row 30 minutes old
    spine_db = str(tmp_path / "spine.db")
    with sqlite3.connect(spine_db) as conn:
        conn.execute("""
            CREATE TABLE spine_quotes (
                quote_id TEXT PRIMARY KEY,
                state_json TEXT,
                event_log TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        conn.execute(
            "INSERT INTO spine_quotes "
            "(quote_id, state_json, event_log, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("q1", "{}", "[]", _now_iso(30), _now_iso(30)),
        )

    monkeypatch.setenv("SPINE_DB_PATH", spine_db)

    from src.core.liveness_checks import _quote_ingestion_freshness
    check = _quote_ingestion_freshness()
    ok, age, detail = check()

    # spine_quotes should be visible — detail must mention it with a
    # minute count, NOT "query failed".
    assert "spine_quotes:" in detail
    assert "query failed" not in detail, (
        f"check still hitting legacy DB for spine_quotes: {detail}"
    )
    # 30-minute row should report ~30min (allow drift)
    assert 1700 < age < 1900, f"unexpected age {age}, detail={detail}"


def test_spine_quotes_missing_does_not_mask_legacy(monkeypatch, tmp_path):
    """If Spine DB doesn't exist, the check reports that gracefully —
    legacy `quotes` reading still works (pre-fix masked legacy by
    binding both reads to the same broken conn)."""
    nonexistent = str(tmp_path / "does-not-exist-spine.db")
    monkeypatch.setenv("SPINE_DB_PATH", nonexistent)

    from src.core.liveness_checks import _quote_ingestion_freshness
    check = _quote_ingestion_freshness()
    ok, age, detail = check()

    # Both branches should be represented in detail — either with an age
    # or an explicit error string. The legacy branch must NOT be
    # collateral damage of the Spine branch's failure.
    assert "quotes:" in detail
    assert "spine_quotes:" in detail


def test_spine_canonical_when_newer_than_legacy(
    monkeypatch, tmp_path, auth_client
):
    """When Spine has a 5-min-old row and legacy `quotes` has a 24h-old
    row, the check reports the youngest (Spine) — proving §0 LAW 1
    (Spine canonical) is honored at the liveness layer."""
    from src.core.db import get_db

    # Seed legacy quotes 24h old
    with get_db() as conn:
        conn.execute("DELETE FROM quotes")
        conn.execute(
            "INSERT INTO quotes (quote_number, agency, created_at) "
            "VALUES (?, ?, ?)",
            ("L24H", "TEST", _now_iso(24 * 60)),
        )

    # Seed Spine 5 min old in a separate DB
    spine_db = str(tmp_path / "spine.db")
    with sqlite3.connect(spine_db) as conn:
        conn.execute("""
            CREATE TABLE spine_quotes (
                quote_id TEXT PRIMARY KEY,
                state_json TEXT,
                event_log TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        conn.execute(
            "INSERT INTO spine_quotes VALUES (?, ?, ?, ?, ?)",
            ("S5M", "{}", "[]", _now_iso(5), _now_iso(5)),
        )
    monkeypatch.setenv("SPINE_DB_PATH", spine_db)

    from src.core.liveness_checks import _quote_ingestion_freshness
    check = _quote_ingestion_freshness()
    ok, age, detail = check()

    # 5 min ≈ 300 sec; 24h would be 86400. Best should be the smaller.
    assert age < 600, (
        f"liveness picked legacy 24h over Spine 5min — "
        f"substrate-singleness regression. age={age} detail={detail}"
    )
