"""
test_platform_upgrade.py — Tests for platform upgrade features.

Surviving sections after the 2026-04-29 flag-sprint PR removed the
unbuilt-feature cluster (bid scoring, NL query, UNSPSC retag,
compliance matrix):
  - Migration M15 (idempotent column adds + new tables)
  - Quote expiry (expires_at column on quotes)
  - Ship-to auto-fill flag stays disabled
"""
import sqlite3
import pytest


# ═══════════════════════════════════════════════════════════════════════════
# MIGRATION M15
# ═══════════════════════════════════════════════════════════════════════════

class TestMigration15:
    def test_idempotent_columns(self, tmp_path):
        """_run_migration_15 adds columns to quotes and contacts."""
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE quotes (id INTEGER PRIMARY KEY, quote_number TEXT)")
        conn.execute("CREATE TABLE contacts (id TEXT PRIMARY KEY, buyer_email TEXT)")
        conn.commit()

        from src.core.migrations import _run_migration_15
        _run_migration_15(conn)

        q_cols = [r[1] for r in conn.execute("PRAGMA table_info(quotes)").fetchall()]
        assert "expires_at" in q_cols
        assert "expiry_notified" in q_cols

        c_cols = [r[1] for r in conn.execute("PRAGMA table_info(contacts)").fetchall()]
        assert "address" in c_cols
        assert "city" in c_cols
        assert "state" in c_cols
        assert "zip" in c_cols
        assert "ship_to_default" in c_cols

        # Idempotent — run again
        _run_migration_15(conn)
        conn.close()

    def test_new_tables(self, tmp_path):
        """M15 SQL creates bid_scores and agency_compliance_templates."""
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)

        from src.core.migrations import MIGRATIONS
        m15_sql = None
        for v, name, sql in MIGRATIONS:
            if v == 15:
                m15_sql = sql
                break

        assert m15_sql is not None
        conn.executescript(m15_sql)

        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        assert "bid_scores" in tables
        assert "agency_compliance_templates" in tables
        conn.close()


# BidScoring / NLQueryV2 / BatchRetag / ComplianceTemplates test classes
# removed 2026-04-29 — those features were gated on bid_scoring,
# nl_query_enabled, unspsc_enrichment, compliance_matrix flags (never
# enabled in prod). Backing modules + routes deleted in the same PR.


# ═══════════════════════════════════════════════════════════════════════════
# QUOTE EXPIRY
# ═══════════════════════════════════════════════════════════════════════════

class TestQuoteExpiry:
    def test_expiry_column_in_migration(self):
        """Migration M15 adds expires_at to quotes."""
        from src.core.migrations import _run_migration_15
        import sqlite3
        import tempfile, os
        db = os.path.join(tempfile.mkdtemp(), "test.db")
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE quotes (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE contacts (id TEXT PRIMARY KEY)")
        _run_migration_15(conn)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(quotes)").fetchall()]
        assert "expires_at" in cols
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# FEATURE FLAGS
# ═══════════════════════════════════════════════════════════════════════════

class TestPlatformFlags:
    def test_ship_to_disabled(self):
        from src.core.feature_flags import get_flag
        assert get_flag("ship_to_autofill", default=False) is False
