"""
test_intelligence_layer.py — Tests for intelligence layer features:
  - UNSPSC classifier
  - NL query agent (guardrails)
  - Compliance extractor
  - Docling parser (fallback mode)
  - Migration M14
"""
import json
import os
import sqlite3
import tempfile
import pytest


# ═══════════════════════════════════════════════════════════════════════════
# UNSPSC CLASSIFIER
# ═══════════════════════════════════════════════════════════════════════════

class TestTAACompliance:
    def test_compliant_country(self):
        from src.agents.unspsc_classifier import check_taa_compliance
        assert check_taa_compliance("Malaysia") == 1
        assert check_taa_compliance("United States") == 1
        assert check_taa_compliance("Germany") == 1

    def test_non_compliant_country(self):
        from src.agents.unspsc_classifier import check_taa_compliance
        assert check_taa_compliance("China") == 0
        assert check_taa_compliance("china") == 0
        assert check_taa_compliance("  China  ") == 0
        assert check_taa_compliance("India") == 0
        assert check_taa_compliance("Russia") == 0
        assert check_taa_compliance("Iran") == 0

    def test_unknown_country(self):
        from src.agents.unspsc_classifier import check_taa_compliance
        assert check_taa_compliance("") == -1
        assert check_taa_compliance(None) == -1
        assert check_taa_compliance("  ") == -1


class TestUNSPSCParseResponse:
    def test_valid_json_array(self):
        from src.agents.unspsc_classifier import _parse_response
        text = '[{"unspsc_code": "42131500", "unspsc_description": "Gloves", "country_of_origin": "Malaysia"}]'
        results = _parse_response(text, 1)
        assert len(results) == 1
        assert results[0]["unspsc_code"] == "42131500"
        assert results[0]["country_of_origin"] == "Malaysia"

    def test_padded_to_count(self):
        from src.agents.unspsc_classifier import _parse_response
        text = '[{"unspsc_code": "42131500", "unspsc_description": "X", "country_of_origin": "US"}]'
        results = _parse_response(text, 3)
        assert len(results) == 3
        assert results[0]["unspsc_code"] == "42131500"
        assert results[1]["unspsc_code"] == ""  # padded

    def test_invalid_json(self):
        from src.agents.unspsc_classifier import _parse_response
        results = _parse_response("not json", 2)
        assert len(results) == 2
        assert all(r["unspsc_code"] == "" for r in results)

    def test_empty_response(self):
        from src.agents.unspsc_classifier import _parse_response
        results = _parse_response("", 1)
        assert len(results) == 1
        assert results[0]["unspsc_code"] == ""

    def test_json_with_surrounding_text(self):
        from src.agents.unspsc_classifier import _parse_response
        text = 'Here are the results:\n[{"unspsc_code":"44121500","unspsc_description":"Pens","country_of_origin":"China"}]\n\nDone.'
        results = _parse_response(text, 1)
        assert results[0]["unspsc_code"] == "44121500"


class TestClassifyBatch:
    def test_empty_list(self):
        from src.agents.unspsc_classifier import classify_batch
        assert classify_batch([]) == []


# ═══════════════════════════════════════════════════════════════════════════
# NL QUERY AGENT — GUARDRAILS
# ═══════════════════════════════════════════════════════════════════════════

class TestSQLValidation:
    def test_select_allowed(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("SELECT * FROM quotes LIMIT 10")
        assert ok is True

    def test_select_case_insensitive(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("select count(*) from quotes")
        assert ok is True

    def test_insert_blocked(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("INSERT INTO quotes VALUES (1, 'test')")
        assert ok is False
        assert "SELECT" in err

    def test_update_blocked(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("UPDATE quotes SET status='deleted'")
        assert ok is False

    def test_delete_blocked(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("DELETE FROM quotes")
        assert ok is False

    def test_drop_blocked(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("DROP TABLE quotes")
        assert ok is False

    def test_drop_in_select_blocked(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("SELECT * FROM quotes; DROP TABLE quotes")
        assert ok is False
        assert "Multiple" in err or "DROP" in err

    def test_attach_blocked(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("SELECT 1; ATTACH DATABASE '/tmp/evil.db' AS evil")
        assert ok is False

    def test_pragma_blocked(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("PRAGMA table_info(quotes)")
        assert ok is False

    def test_empty_blocked(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("")
        assert ok is False

    def test_multistatement_blocked(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("SELECT 1; SELECT 2")
        assert ok is False
        assert "Multiple" in err

    def test_create_blocked(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("CREATE TABLE evil (id int)")
        assert ok is False

    def test_replace_blocked(self):
        from src.agents.nl_query_agent import _validate_sql
        ok, err = _validate_sql("REPLACE INTO quotes VALUES (1)")
        assert ok is False


class TestLimitInjection:
    def test_adds_limit(self):
        from src.agents.nl_query_agent import _inject_limit
        sql = _inject_limit("SELECT * FROM quotes")
        assert "LIMIT 100" in sql

    def test_preserves_existing_limit(self):
        from src.agents.nl_query_agent import _inject_limit
        sql = _inject_limit("SELECT * FROM quotes LIMIT 5")
        assert "LIMIT 5" in sql
        assert sql.count("LIMIT") == 1


class TestRateLimit:
    def test_under_limit(self):
        from src.agents.nl_query_agent import _check_rate_limit, _rate_timestamps
        _rate_timestamps.clear()
        assert _check_rate_limit() is True

    def test_over_limit(self):
        import time
        from src.agents.nl_query_agent import _check_rate_limit, _rate_timestamps, _RATE_LIMIT
        _rate_timestamps.clear()
        now = time.time()
        for _ in range(_RATE_LIMIT):
            _rate_timestamps.append(now)
        assert _check_rate_limit() is False
        _rate_timestamps.clear()  # cleanup


class TestReadOnlyExecution:
    """Verify PRAGMA query_only = ON blocks writes."""

    def test_readonly_blocks_insert(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()

        # Enable read-only mode
        conn.execute("PRAGMA query_only = ON")

        # SELECT should work
        row = conn.execute("SELECT * FROM t").fetchone()
        assert row[0] == 1

        # INSERT should fail
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO t VALUES (2)")

        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# COMPLIANCE EXTRACTOR
# ═══════════════════════════════════════════════════════════════════════════

class TestFormKeywordMatch:
    def test_dvbe_match(self):
        from src.agents.compliance_extractor import _match_form_id
        assert _match_form_id("Vendor must submit DVBE 843 form") == "dvbe843"

    def test_sellers_permit(self):
        from src.agents.compliance_extractor import _match_form_id
        assert _match_form_id("Include a copy of your seller's permit") == "sellers_permit"

    def test_std204(self):
        from src.agents.compliance_extractor import _match_form_id
        assert _match_form_id("STD 204 payee data form required") == "std204"

    def test_w9(self):
        from src.agents.compliance_extractor import _match_form_id
        assert _match_form_id("Please provide W-9 form") == "w9"

    def test_no_match(self):
        from src.agents.compliance_extractor import _match_form_id
        assert _match_form_id("Deliver within 5 business days") == ""


class TestCrossReference:
    def test_form_met(self):
        from src.agents.compliance_extractor import _cross_reference_package
        reqs = [{"text": "Submit DVBE 843 certification", "category": "form", "severity": "required"}]
        result = _cross_reference_package(reqs, {}, ["dvbe843", "quote"])
        assert result[0]["met"] is True
        assert result[0]["met_by"] == "dvbe843"

    def test_form_not_met(self):
        from src.agents.compliance_extractor import _cross_reference_package
        reqs = [{"text": "Submit W-9 form", "category": "form", "severity": "required"}]
        result = _cross_reference_package(reqs, {}, ["dvbe843", "quote"])
        assert result[0]["met"] is False

    def test_dvbe_certification_auto_met(self):
        from src.agents.compliance_extractor import _cross_reference_package
        reqs = [{"text": "Vendor must be a certified DVBE", "category": "certification", "severity": "required"}]
        result = _cross_reference_package(reqs, {}, [])
        assert result[0]["met"] is True
        assert "DVBE" in result[0]["met_by"]

    def test_pricing_met(self):
        from src.agents.compliance_extractor import _cross_reference_package
        reqs = [{"text": "Include detailed pricing", "category": "pricing", "severity": "required"}]
        result = _cross_reference_package(reqs, {}, ["quote"])
        assert result[0]["met"] is True

    def test_ids_assigned(self):
        from src.agents.compliance_extractor import _cross_reference_package
        reqs = [
            {"text": "Req A", "category": "other", "severity": "required"},
            {"text": "Req B", "category": "other", "severity": "preferred"},
        ]
        result = _cross_reference_package(reqs, {}, [])
        assert result[0]["id"] == "REQ-001"
        assert result[1]["id"] == "REQ-002"


# ═══════════════════════════════════════════════════════════════════════════
# DOCLING PARSER — FALLBACK MODE
# ═══════════════════════════════════════════════════════════════════════════

class TestFileValidation:
    def test_missing_file(self):
        from src.agents.docling_parser import validate_file
        ok, err = validate_file("/nonexistent/file.pdf")
        assert ok is False
        assert "not found" in err.lower()

    def test_bad_extension(self, tmp_path):
        from src.agents.docling_parser import validate_file
        f = tmp_path / "test.exe"
        f.write_text("hello")
        ok, err = validate_file(str(f))
        assert ok is False
        assert "Unsupported" in err

    def test_empty_file(self, tmp_path):
        from src.agents.docling_parser import validate_file
        f = tmp_path / "empty.pdf"
        f.write_bytes(b"")
        ok, err = validate_file(str(f))
        assert ok is False
        assert "empty" in err.lower()

    def test_valid_pdf(self, tmp_path):
        from src.agents.docling_parser import validate_file
        f = tmp_path / "test.pdf"
        f.write_bytes(b"%PDF-1.4 test content that is not empty")
        ok, err = validate_file(str(f))
        assert ok is True


# ═══════════════════════════════════════════════════════════════════════════
# MIGRATION M14
# ═══════════════════════════════════════════════════════════════════════════

class TestMigration14:
    def test_idempotent_column_add(self, tmp_path):
        """_run_migration_14 should be idempotent — running twice is safe."""
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""CREATE TABLE products (
            id INTEGER PRIMARY KEY, name TEXT, sku TEXT)""")
        conn.execute("""CREATE TABLE product_catalog (
            id INTEGER PRIMARY KEY, name TEXT)""")
        conn.commit()

        from src.core.migrations import _run_migration_14

        # First run — adds columns
        _run_migration_14(conn)
        cols = [row[1] for row in conn.execute("PRAGMA table_info(products)").fetchall()]
        assert "unspsc_code" in cols
        assert "country_of_origin" in cols
        assert "taa_compliant" in cols

        # Second run — should not error
        _run_migration_14(conn)  # idempotent

        conn.close()

    def test_tables_created(self, tmp_path):
        """M14 SQL creates new intelligence tables."""
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)

        # Execute the SQL portion of M14
        from src.core.migrations import MIGRATIONS
        m14_sql = None
        for v, name, sql in MIGRATIONS:
            if v == 14:
                m14_sql = sql
                break

        assert m14_sql is not None, "Migration 14 not found in MIGRATIONS list"
        conn.executescript(m14_sql)

        # Verify tables exist
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        assert "parsed_documents" in tables
        assert "nl_query_log" in tables
        assert "compliance_matrices" in tables

        # Verify indexes
        indexes = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'").fetchall()]
        assert "idx_parsed_docs_rfq" in indexes
        assert "idx_compliance_rfq" in indexes

        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# FEATURE FLAGS
# ═══════════════════════════════════════════════════════════════════════════

class TestFeatureFlagsDefault:
    """All intelligence features should be disabled by default."""

    def test_unspsc_disabled(self):
        from src.core.feature_flags import get_flag
        # get_flag returns default when flag not set
        assert get_flag("unspsc_enrichment", default=False) is False

    def test_docling_disabled(self):
        from src.core.feature_flags import get_flag
        assert get_flag("docling_intake", default=False) is False

    def test_nl_query_disabled(self):
        from src.core.feature_flags import get_flag
        assert get_flag("nl_query_enabled", default=False) is False

    def test_compliance_disabled(self):
        from src.core.feature_flags import get_flag
        assert get_flag("compliance_matrix", default=False) is False
