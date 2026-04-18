"""Regression test: migrations 19 → 21 must apply on a legacy workflow_runs shape.

The bug (2026-04-18): db.py boot DDL creates a LEGACY workflow_runs table
without `task_type`. Migration 19 then does CREATE INDEX ON workflow_runs(task_type)
which raises "no such column". That abort cascaded — migration 21 (which creates
`quote_audit_log`) never applied on prod, so the backfill endpoint's audit writes
failed silently via `_persist_audit`'s `except Exception: log.debug(...)`.

The fix is `_heal_workflow_runs_schema()` running unconditionally before the
migration loop. This test guards against regressions.
"""
from __future__ import annotations

import pytest


def _install_legacy_workflow_runs(conn) -> None:
    """Reproduce the exact legacy shape from src/core/db.py (pre-migration-19)."""
    conn.execute("DROP TABLE IF EXISTS workflow_runs")
    conn.executescript("""
        CREATE TABLE workflow_runs (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at        TEXT,
            finished_at       TEXT,
            type              TEXT DEFAULT 'workflow',
            status            TEXT DEFAULT 'completed',
            run_at            TEXT,
            score             INTEGER,
            grade             TEXT,
            passed            INTEGER,
            failed            INTEGER,
            warned            INTEGER,
            critical_failures TEXT,
            full_report       TEXT
        );
    """)


class TestWorkflowRunsHeal:
    def test_heal_adds_missing_columns(self):
        from src.core.db import get_db
        from src.core.migrations import _heal_workflow_runs_schema

        with get_db() as conn:
            _install_legacy_workflow_runs(conn)
            _heal_workflow_runs_schema(conn)
            cols = {r[1] for r in conn.execute("PRAGMA table_info(workflow_runs)").fetchall()}

        # Columns migration 19's indexes + migration 20's triggers depend on:
        for must_have in ("task_type", "running", "errors_json"):
            assert must_have in cols, f"heal did not add {must_have}"

    def test_heal_is_idempotent(self):
        from src.core.db import get_db
        from src.core.migrations import _heal_workflow_runs_schema

        with get_db() as conn:
            _install_legacy_workflow_runs(conn)
            _heal_workflow_runs_schema(conn)
            _heal_workflow_runs_schema(conn)  # second call must not raise
            cols = {r[1] for r in conn.execute("PRAGMA table_info(workflow_runs)").fetchall()}

        assert "task_type" in cols  # still there, not duplicated

    def test_heal_skips_when_table_absent(self):
        """Fresh DB: no workflow_runs yet. Heal must no-op (migration 19 will create it)."""
        from src.core.db import get_db
        from src.core.migrations import _heal_workflow_runs_schema

        with get_db() as conn:
            conn.execute("DROP TABLE IF EXISTS workflow_runs")
            _heal_workflow_runs_schema(conn)  # must not raise
            exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='workflow_runs'"
            ).fetchone()

        assert exists is None  # still absent — heal is not supposed to create


class TestMigrationLoopReachesQuoteAuditLog:
    """The whole point: after run_migrations(), quote_audit_log exists and is writable."""

    def test_migrations_create_quote_audit_log_on_legacy_db(self):
        from src.core.db import get_db
        from src.core.migrations import run_migrations

        with get_db() as conn:
            _install_legacy_workflow_runs(conn)
            # Reset schema_migrations so run_migrations replays the full chain.
            conn.execute("DROP TABLE IF EXISTS schema_migrations")

        run_migrations()

        with get_db() as conn:
            exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='quote_audit_log'"
            ).fetchone()
            assert exists is not None, "quote_audit_log missing — migration 21 did not apply"

            # Write path must succeed end-to-end (the very path _persist_audit uses).
            conn.execute(
                """INSERT INTO quote_audit_log
                   (quote_doc_id, doc_type, agency_key, stage_from, stage_to,
                    outcome, reasons_json, actor, at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                ("heal_probe", "pc", "cchcs", "draft", "parsed",
                 "advanced", "[]", "test", "2026-04-18T00:00:00"),
            )
            row = conn.execute(
                "SELECT outcome FROM quote_audit_log WHERE quote_doc_id='heal_probe'"
            ).fetchone()
            assert row is not None and row[0] == "advanced"
