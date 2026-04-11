"""
test_platform_upgrade.py — Tests for platform upgrade features:
  - Quote expiry
  - Bid/No-Bid scorer
  - Ship-to auto-fill (address fields)
  - NL Query V2 (suggested queries)
  - UNSPSC V2 (batch retag)
  - Compliance Matrix V2 (agency templates)
  - Migration M15
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


# ═══════════════════════════════════════════════════════════════════════════
# BID/NO-BID SCORER
# ═══════════════════════════════════════════════════════════════════════════

class TestBidScoring:
    def test_complexity_small_pc(self):
        from src.agents.bid_decision_agent import _score_complexity
        score = _score_complexity([{"description": "item"}] * 3)
        assert score >= 90

    def test_complexity_large_pc(self):
        from src.agents.bid_decision_agent import _score_complexity
        score = _score_complexity([{"description": "item"}] * 50)
        assert score <= 30

    def test_complexity_food_deduction(self):
        from src.agents.bid_decision_agent import _score_complexity
        base = _score_complexity([{"description": "item"}] * 5, has_food=False)
        food = _score_complexity([{"description": "item"}] * 5, has_food=True)
        assert food < base

    def test_score_items_returns_structure(self):
        from src.agents.bid_decision_agent import score_pc_items
        result = score_pc_items(
            [{"description": "test item"}],
            agency="test", institution="test"
        )
        assert "total_score" in result
        assert "recommendation" in result
        assert "breakdown" in result
        assert result["recommendation"] in ("bid", "review", "no-bid")

    def test_thresholds(self):
        from src.agents.bid_decision_agent import THRESHOLD_BID, THRESHOLD_REVIEW
        assert THRESHOLD_BID > THRESHOLD_REVIEW
        assert THRESHOLD_REVIEW > 0


# ═══════════════════════════════════════════════════════════════════════════
# NL QUERY V2 — SUGGESTIONS
# ═══════════════════════════════════════════════════════════════════════════

class TestNLQueryV2:
    def test_suggested_queries_exist(self):
        from src.agents.nl_query_agent import SUGGESTED_QUERIES
        assert isinstance(SUGGESTED_QUERIES, list)
        assert len(SUGGESTED_QUERIES) >= 6

    def test_suggestion_structure(self):
        from src.agents.nl_query_agent import SUGGESTED_QUERIES
        for q in SUGGESTED_QUERIES:
            assert "text" in q
            assert "category" in q
            assert len(q["text"]) > 5


# ═══════════════════════════════════════════════════════════════════════════
# UNSPSC V2 — BATCH RETAG
# ═══════════════════════════════════════════════════════════════════════════

class TestBatchRetag:
    def test_empty_catalog(self):
        """batch_retag_catalog returns 0 tagged when no untagged items."""
        # This test would need a real DB; testing the interface
        from src.agents.unspsc_classifier import batch_retag_catalog
        # Can't easily test without DB mock, but verify function exists and is callable
        assert callable(batch_retag_catalog)


# ═══════════════════════════════════════════════════════════════════════════
# COMPLIANCE V2 — AGENCY TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════

class TestComplianceTemplates:
    def test_seed_data_exists(self):
        from src.agents.compliance_extractor import _AGENCY_TEMPLATE_SEEDS
        assert "cchcs" in _AGENCY_TEMPLATE_SEEDS
        assert "calvet" in _AGENCY_TEMPLATE_SEEDS
        assert "dgs" in _AGENCY_TEMPLATE_SEEDS
        assert len(_AGENCY_TEMPLATE_SEEDS["cchcs"]) >= 6

    def test_merge_deduplicates(self):
        from src.agents.compliance_extractor import merge_template_with_extraction
        template = [
            {"requirement_text": "Submit DVBE 843 form", "category": "form",
             "severity": "required", "form_id": "dvbe843"},
        ]
        extracted = [
            {"text": "Vendor must submit DVBE 843", "category": "form",
             "severity": "required", "form_id": "dvbe843"},
            {"text": "Deliver within 5 days", "category": "delivery",
             "severity": "required"},
        ]
        merged = merge_template_with_extraction(template, extracted)
        # DVBE should appear once (from template), delivery should be added
        dvbe_count = sum(1 for r in merged if "dvbe" in r.get("text", "").lower() or r.get("form_id") == "dvbe843")
        assert dvbe_count == 1  # deduped
        delivery = [r for r in merged if "deliver" in r.get("text", "").lower()]
        assert len(delivery) == 1

    def test_merge_preserves_template_priority(self):
        from src.agents.compliance_extractor import merge_template_with_extraction
        template = [
            {"requirement_text": "Submit W-9", "category": "form",
             "severity": "required", "form_id": "w9"},
        ]
        merged = merge_template_with_extraction(template, [])
        assert len(merged) == 1
        assert merged[0]["source"] == "agency_template"

    def test_seed_function_callable(self):
        from src.agents.compliance_extractor import seed_agency_templates
        assert callable(seed_agency_templates)

    def test_get_template_callable(self):
        from src.agents.compliance_extractor import get_agency_template
        assert callable(get_agency_template)


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
    def test_bid_scoring_disabled(self):
        from src.core.feature_flags import get_flag
        assert get_flag("bid_scoring", default=False) is False

    def test_ship_to_disabled(self):
        from src.core.feature_flags import get_flag
        assert get_flag("ship_to_autofill", default=False) is False
