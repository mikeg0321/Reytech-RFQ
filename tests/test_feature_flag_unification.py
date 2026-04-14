"""Unification contract between src.core.flags and src.core.feature_flags.

Before PR #62 these were independent implementations against different tables
(`feature_flags` vs `app_settings` with a `flag:` prefix). A flag flipped via
the admin API (`/api/admin/flags` → `flags.set_flag`) was invisible to legacy
callers who imported `feature_flags.get_flag`. This file locks in the fix:

1. A value written via either import path is readable via the other.
2. A legacy row that still lives in `app_settings` (pre-migration state or a
   stray write from ops) is readable via `flags.get_flag` via the dual-read
   safety net.
3. The boot migration (`_migrate_feature_flags_from_app_settings`) copies
   legacy rows forward and is idempotent.
"""
import sqlite3

import pytest


@pytest.fixture(autouse=True)
def _clear_flag_cache():
    from src.core.flags import _cache_clear_all
    _cache_clear_all()
    yield
    _cache_clear_all()


class TestFacadeReadsWriteFromNewAPI:
    def test_new_set_visible_via_legacy_import(self):
        from src.core.flags import set_flag as new_set
        from src.core.feature_flags import get_flag as legacy_get
        assert new_set("ingest.classifier_v2_enabled", "true")
        assert legacy_get("ingest.classifier_v2_enabled", default=False) is True

    def test_legacy_set_visible_via_new_import(self):
        from src.core.feature_flags import set_flag as legacy_set
        from src.core.flags import get_flag as new_get, _cache_clear_all
        assert legacy_set("pricing_v2", "1")
        _cache_clear_all()
        assert new_get("pricing_v2", default=False) is True

    def test_legacy_delete_invalidates_new_read(self):
        from src.core.feature_flags import set_flag as legacy_set, delete_flag as legacy_del
        from src.core.flags import get_flag as new_get, _cache_clear_all
        legacy_set("nl_query_enabled", "true")
        _cache_clear_all()
        assert new_get("nl_query_enabled", default=False) is True
        assert legacy_del("nl_query_enabled")
        _cache_clear_all()
        assert new_get("nl_query_enabled", default=False) is False


class TestDualReadSafetyNet:
    """Ops (or a stray legacy writer) might still write to app_settings
    with the `flag:` prefix during the 1-week transition. flags.get_flag
    must fall back to that row when feature_flags has no match."""

    def _write_legacy(self, key: str, json_value: str):
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
            (f"flag:{key}", json_value),
        )
        conn.commit()
        conn.close()

    def test_legacy_only_write_is_visible_via_new_get(self):
        from src.core.flags import get_flag, _cache_clear_all
        self._write_legacy("compliance_matrix", "true")
        _cache_clear_all()
        assert get_flag("compliance_matrix", default=False) is True

    def test_legacy_quoted_string_is_unwrapped(self):
        from src.core.flags import get_flag, _cache_clear_all
        # Legacy writer used json.dumps, so "foo" was stored as "\"foo\""
        self._write_legacy("ingest.mode", '"v2"')
        _cache_clear_all()
        assert get_flag("ingest.mode", default="v1") == "v2"

    def test_new_write_takes_precedence_over_legacy(self):
        from src.core.flags import get_flag, set_flag, _cache_clear_all
        self._write_legacy("bid_scoring", "false")
        set_flag("bid_scoring", "true")  # new write wins
        _cache_clear_all()
        assert get_flag("bid_scoring", default=False) is True


class TestBootMigration:
    def test_copies_legacy_rows_into_feature_flags(self):
        from src.core.db import DB_PATH, _migrate_feature_flags_from_app_settings
        from src.core.flags import get_flag, _cache_clear_all

        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
            ("flag:docling_intake", "true"),
        )
        conn.execute("DELETE FROM feature_flags WHERE key = ?", ("docling_intake",))
        conn.commit()
        conn.close()

        _migrate_feature_flags_from_app_settings()

        conn = sqlite3.connect(DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT value FROM feature_flags WHERE key = ?", ("docling_intake",)
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "true"

        _cache_clear_all()
        assert get_flag("docling_intake", default=False) is True

    def test_migration_is_idempotent(self):
        from src.core.db import DB_PATH, _migrate_feature_flags_from_app_settings

        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
            ("flag:unspsc_enrichment", "false"),
        )
        conn.commit()
        conn.close()

        _migrate_feature_flags_from_app_settings()
        _migrate_feature_flags_from_app_settings()  # second run must not error or overwrite

        conn = sqlite3.connect(DB_PATH, timeout=10)
        rows = conn.execute(
            "SELECT COUNT(*) FROM feature_flags WHERE key = ?", ("unspsc_enrichment",)
        ).fetchone()
        conn.close()
        assert rows[0] == 1

    def test_migration_does_not_overwrite_new_values(self):
        """If the new table already has a value (e.g., written via admin API),
        the legacy copy must not clobber it."""
        from src.core.db import DB_PATH, _migrate_feature_flags_from_app_settings
        from src.core.flags import set_flag, _cache_clear_all

        set_flag("pipeline.delivery_threshold", "85")

        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
            ("flag:pipeline.delivery_threshold", "70"),
        )
        conn.commit()
        conn.close()

        _migrate_feature_flags_from_app_settings()

        conn = sqlite3.connect(DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT value FROM feature_flags WHERE key = ?", ("pipeline.delivery_threshold",)
        ).fetchone()
        conn.close()
        assert row[0] == "85"

        _cache_clear_all()
        from src.core.flags import get_flag
        assert get_flag("pipeline.delivery_threshold", default=0) == 85
