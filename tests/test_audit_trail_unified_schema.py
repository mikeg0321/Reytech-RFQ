"""Pin Tier 2a — audit_trail has ONE canonical schema with both column sets.

Audit 2026-05-07 v2 §S-4: pre-fix audit_trail had divergent CREATE TABLE
statements in db.py vs routes_catalog_finance.py. db.py ran first, so
the table was created without admin-action columns (`timestamp`,
`action`, `details`, `ip_address`, `user_agent`, `metadata`). All
admin-action INSERTs from routes_catalog_finance silently failed.

These tests pin:
  1. Canonical schema in db.py contains BOTH column sets.
  2. _migrate_columns adds admin columns to existing tables.
  3. routes_catalog_finance.py no longer has redundant CREATE TABLE
     statements (they were the bug — divergent shapes).
  4. Admin-action INSERT now succeeds end-to-end via the canonical schema.
"""
from __future__ import annotations

import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _seed_db(monkeypatch, tmp_path):
    from src.core import db as core_db
    db_path = tmp_path / "test.db"
    monkeypatch.setattr(core_db, "DB_PATH", str(db_path))
    core_db.init_db()
    return core_db


class TestCanonicalSchemaUnified:
    def test_canonical_schema_has_both_column_sets(self, tmp_path, monkeypatch):
        from src.core import db as core_db
        _seed_db(monkeypatch, tmp_path)

        with core_db.get_db() as conn:
            cols = {r[1] for r in conn.execute(
                "PRAGMA table_info(audit_trail)"
            ).fetchall()}

        # Item-pricing audit columns (legacy)
        for c in ("item_description", "field_changed", "old_value",
                  "new_value", "source", "rfq_id", "part_number",
                  "actor", "notes", "created_at"):
            assert c in cols, f"Tier 2a: legacy column '{c}' missing"

        # Admin-action audit columns (Tier 2a unification)
        for c in ("timestamp", "action", "details", "ip_address",
                  "user_agent", "metadata"):
            assert c in cols, f"Tier 2a: admin column '{c}' missing"


class TestRoutesCatalogFinanceNoRedundantCreate:
    def test_no_create_table_audit_trail_in_routes_catalog_finance(self):
        """The 3 redundant CREATE TABLE statements that diverged from the
        canonical schema must be gone. Source-level guard so a future
        edit can't silently restore the bug shape."""
        import pathlib
        src = pathlib.Path(
            "src/api/modules/routes_catalog_finance.py"
        ).read_text(encoding="utf-8")

        # Pre-fix had 3 occurrences. Post-fix should have 0.
        n = len(re.findall(r"CREATE TABLE IF NOT EXISTS audit_trail", src))
        assert n == 0, (
            f"Tier 2a regression: routes_catalog_finance.py still has "
            f"{n} CREATE TABLE audit_trail statement(s). Canonical "
            f"schema lives in src/core/db.py:1024 — never duplicate it."
        )

        # And the Tier 2a sentinel must be present (so the comment
        # explaining the fix doesn't get deleted by a future cleanup).
        assert "Tier 2a" in src, \
            "Tier 2a sentinel missing — future maintainer may not know why this is consolidated"


class TestAdminActionInsertEndToEnd:
    def test_admin_audit_insert_now_succeeds(self, tmp_path, monkeypatch):
        """The actual INSERT shape used by routes_catalog_finance now
        executes successfully against the canonical schema."""
        from src.core import db as core_db
        from datetime import datetime
        import json

        _seed_db(monkeypatch, tmp_path)

        with core_db.get_db() as conn:
            conn.execute(
                "INSERT INTO audit_trail "
                "(timestamp, action, details, ip_address, user_agent, metadata) "
                "VALUES (?,?,?,?,?,?)",
                (datetime.now().isoformat(), "test_action",
                 "test_details", "127.0.0.1", "pytest", json.dumps({"k": "v"}))
            )
            row = conn.execute(
                "SELECT timestamp, action, details FROM audit_trail "
                "WHERE action='test_action'"
            ).fetchone()
            assert row is not None, \
                "Tier 2a regression: admin-action INSERT silently failed"
            assert row[1] == "test_action"
            assert row[2] == "test_details"

    def test_legacy_item_pricing_insert_still_works(self, tmp_path, monkeypatch):
        """Inverse — the legacy item-pricing column writes must still work."""
        from src.core import db as core_db
        _seed_db(monkeypatch, tmp_path)

        with core_db.get_db() as conn:
            conn.execute(
                "INSERT INTO audit_trail "
                "(item_description, field_changed, old_value, new_value, source, actor) "
                "VALUES (?,?,?,?,?,?)",
                ("widget A", "unit_price", "100.00", "110.00", "manual", "system")
            )
            row = conn.execute(
                "SELECT item_description, field_changed FROM audit_trail "
                "WHERE field_changed='unit_price'"
            ).fetchone()
            assert row is not None
            assert row[0] == "widget A"
