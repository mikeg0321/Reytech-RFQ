"""Hotfix 2026-05-08 — audit_trail indexes must not boot before columns.

Production incident: PR #854 (Tier 2a audit_trail unification) added
`CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_trail(timestamp)`
inline in SCHEMA. On any prod DB that pre-dated Tier 2a:
  1. `CREATE TABLE IF NOT EXISTS audit_trail (...timestamp...)` → no-op
     (table already exists, with the OLD pre-Tier-2a column set).
  2. `CREATE INDEX ... ON audit_trail(timestamp)` → SQLite raises
     `no such column: timestamp` because timestamp hasn't been ALTER'd
     in yet.
  3. `executescript` aborts → init_db() raises → app.config["DB_DEGRADED"]
     = True → every page returns the maintenance screen.

This test pins:
  (a) The two new indexes are NOT created in SCHEMA (so `executescript`
      never touches them on a legacy table).
  (b) `_migrate_columns` creates them AFTER it ALTERs in the new columns
      (so they exist on every fresh boot regardless of DB age).
  (c) A simulated legacy DB (audit_trail with the OLD column set) boots
      cleanly through init_db() — no exception, no degraded mode.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read_db_py() -> str:
    import pathlib
    return pathlib.Path("src/core/db.py").read_text(encoding="utf-8")


class TestSchemaDoesNotIndexTier2aColumns:
    """idx_audit_timestamp / idx_audit_action must not appear in the
    SCHEMA string — they would run via executescript before the
    ALTER TABLE migration adds the columns."""

    def test_schema_omits_audit_timestamp_index(self):
        src = _read_db_py()
        # Grab the SCHEMA constant only (between the triple-quoted SCHEMA = """
        # opener and its closing """). The migrations live in _migrate_columns
        # which is plain Python, not inside SCHEMA.
        schema_start = src.find('SCHEMA = """')
        assert schema_start >= 0, "SCHEMA constant not found"
        # Closing triple-quote is the next standalone """ followed by newline
        schema_end = src.find('"""', schema_start + len('SCHEMA = """'))
        assert schema_end > schema_start
        schema = src[schema_start:schema_end]
        assert "idx_audit_timestamp" not in schema, (
            "SCHEMA must not CREATE INDEX on audit_trail(timestamp) — "
            "the column does not exist on legacy DBs until _migrate_columns "
            "runs ALTER TABLE."
        )
        assert "idx_audit_action" not in schema, (
            "SCHEMA must not CREATE INDEX on audit_trail(action) — same "
            "reason as idx_audit_timestamp."
        )

    def test_migrate_columns_creates_them(self):
        src = _read_db_py()
        # Look only inside _migrate_columns
        mc_start = src.find("def _migrate_columns()")
        assert mc_start >= 0
        mc_end = src.find("\ndef ", mc_start + 1)
        body = src[mc_start:mc_end if mc_end > mc_start else len(src)]
        assert "idx_audit_timestamp" in body, (
            "_migrate_columns must CREATE INDEX idx_audit_timestamp after "
            "the ALTER TABLE migrations have added the column."
        )
        assert "idx_audit_action" in body, (
            "_migrate_columns must CREATE INDEX idx_audit_action after "
            "the ALTER TABLE migrations have added the column."
        )


class TestLegacyAuditTrailBootsClean:
    """Simulate a legacy prod DB whose audit_trail table predates
    Tier 2a — the OLD column set, no timestamp/action. Boot must
    complete without raising."""

    def test_init_db_handles_legacy_audit_trail(self, monkeypatch):
        # Windows holds SQLite file locks past test exit; ignore cleanup errors.
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = os.path.join(tmp, "test.db")
            # Seed a legacy audit_trail (no timestamp/action columns).
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE audit_trail (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_description TEXT,
                    field_changed TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    source TEXT DEFAULT 'manual',
                    rfq_id TEXT,
                    part_number TEXT,
                    actor TEXT DEFAULT 'system',
                    notes TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                """
            )
            conn.commit()
            conn.close()

            # Point the db module at our legacy DB and run init_db.
            import src.core.db as dbmod
            monkeypatch.setattr(dbmod, "DB_PATH", db_path)

            # Should not raise — pre-fix this raised
            # `OperationalError: no such column: timestamp`.
            dbmod.init_db()

            # Verify columns + indexes both landed.
            conn = sqlite3.connect(db_path)
            cols = {r[1] for r in conn.execute("PRAGMA table_info(audit_trail)").fetchall()}
            assert "timestamp" in cols
            assert "action" in cols
            assert "details" in cols
            assert "ip_address" in cols
            assert "user_agent" in cols
            assert "metadata" in cols
            idxs = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='audit_trail'"
                ).fetchall()
            }
            assert "idx_audit_timestamp" in idxs
            assert "idx_audit_action" in idxs
            conn.close()
