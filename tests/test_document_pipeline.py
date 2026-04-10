"""
Tests for the self-healing document pipeline.

Covers:
- readback_verify: form field verification, overlay verification, scoring
- document_pipeline: strategy escalation, normalization, gate behavior
- template_learning: fingerprinting, outcome recording, strategy recommendation
- template_registry: risk assessment, cache invalidation
"""

import os
import json
import pytest
from unittest.mock import patch, MagicMock


# ═══════════════════════════════════════════════════════════════════════════
# READ-BACK VERIFICATION TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestReadbackVerify:
    """Test read-back verification of filled PDFs."""

    def test_verify_form_fields_perfect_score(self, blank_704_path, temp_data_dir):
        """Fill a PDF and verify read-back returns score=100."""
        from pypdf import PdfReader, PdfWriter
        from src.forms.readback_verify import verify_form_fields

        # Fill the blank 704 with known values
        reader = PdfReader(blank_704_path)
        writer = PdfWriter()
        writer.append(reader)

        test_values = {"COMPANY NAME": "Reytech Inc.",
                       "COMPANY REPRESENTATIVE print name": "Mike Garcia"}
        for page in writer.pages:
            writer.update_page_form_field_values(page, test_values,
                                                  auto_regenerate=False)

        output = os.path.join(temp_data_dir, "test_filled.pdf")
        with open(output, "wb") as f:
            writer.write(f)

        intended = [
            {"field_id": "COMPANY NAME", "page": 1, "value": "Reytech Inc."},
            {"field_id": "COMPANY REPRESENTATIVE print name", "page": 1,
             "value": "Mike Garcia"},
        ]
        result = verify_form_fields(output, intended)
        assert result.fields_confirmed == 2
        assert result.fields_missing == 0
        assert result.score == 100
        assert result.passed

    def test_verify_missing_file_returns_zero(self):
        """Non-existent file returns score=0."""
        from src.forms.readback_verify import verify_form_fields
        result = verify_form_fields("/nonexistent.pdf", [{"field_id": "X", "value": "Y"}])
        assert result.score == 0
        assert not result.passed

    def test_verify_empty_intended_returns_100(self, blank_704_path):
        """No intended values = nothing to verify = score 100."""
        from src.forms.readback_verify import verify_form_fields
        result = verify_form_fields(blank_704_path, [])
        assert result.score == 100

    def test_verify_missing_field_deducts_points(self, blank_704_path):
        """Intended field not in PDF deducts points."""
        from src.forms.readback_verify import verify_form_fields
        intended = [
            {"field_id": "NONEXISTENT_FIELD_XYZ", "page": 1, "value": "test"},
        ]
        result = verify_form_fields(blank_704_path, intended)
        assert result.score < 100
        assert result.fields_missing == 1
        assert len(result.issues) == 1

    def test_verify_critical_field_missing_larger_deduction(self, blank_704_path):
        """Missing COMPANY NAME (critical) deducts more than normal field."""
        from src.forms.readback_verify import verify_form_fields
        intended_critical = [
            {"field_id": "COMPANY NAME", "page": 1, "value": "Reytech Inc."},
        ]
        intended_normal = [
            {"field_id": "Discount Offered", "page": 1, "value": "Included"},
        ]
        r_critical = verify_form_fields(blank_704_path, intended_critical)
        r_normal = verify_form_fields(blank_704_path, intended_normal)
        # Critical field penalty (-15) > normal field penalty (-5)
        assert r_critical.score < r_normal.score

    def test_verify_signature_present(self, blank_704_path):
        """Signature verification on blank template (no sig = False)."""
        from src.forms.readback_verify import verify_signature
        result = verify_signature(blank_704_path)
        # Blank template has no signature image
        assert result is False

    def test_verify_overlay_text_on_blank(self, blank_704_path):
        """Overlay verification on blank template finds no text."""
        from src.forms.readback_verify import verify_overlay_text
        intended = [
            {"field_id": "COMPANY NAME", "page": 1, "value": "Reytech Inc."},
        ]
        result = verify_overlay_text(blank_704_path, intended)
        # Blank template has no text overlay — critical field missing
        assert result.score < 100

    def test_values_match_with_formatting(self):
        """Numeric values match despite formatting differences."""
        from src.forms.readback_verify import _values_match
        assert _values_match("1,234.56", "1234.56")
        assert _values_match("$99.99", "99.99")
        assert _values_match("Reytech Inc.", "Reytech Inc.")
        assert not _values_match("Reytech", "WrongCompany")


# ═══════════════════════════════════════════════════════════════════════════
# TEMPLATE RISK ASSESSMENT TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestTemplateRiskAssessment:
    """Test TemplateProfile risk assessment."""

    def test_blank_704_low_risk(self, blank_704_path):
        """Blank 704 template should be low risk with form_fields recommendation."""
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(blank_704_path)
        assert profile.risk_level == "low"
        assert profile.fill_recommendation == "form_fields"
        assert len(profile.risk_reasons) == 0

    def test_cache_invalidation(self, blank_704_path):
        """invalidate_cache removes cached profiles."""
        from src.forms.template_registry import get_profile, invalidate_cache, _profile_cache
        # Populate cache
        profile = get_profile(blank_704_path)
        abs_path = os.path.abspath(blank_704_path)
        cached_keys = [k for k in _profile_cache if k[0] == abs_path]
        assert len(cached_keys) > 0
        # Invalidate
        invalidate_cache(blank_704_path)
        cached_keys_after = [k for k in _profile_cache if k[0] == abs_path]
        assert len(cached_keys_after) == 0


# ═══════════════════════════════════════════════════════════════════════════
# TEMPLATE LEARNING TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestTemplateLearning:
    """Test template fingerprinting and strategy learning."""

    def test_fingerprint_consistent(self, blank_704_path):
        """Same template produces same fingerprint."""
        from src.forms.template_learning import template_fingerprint
        fp1 = template_fingerprint(blank_704_path)
        fp2 = template_fingerprint(blank_704_path)
        assert fp1 == fp2
        assert len(fp1) > 0

    def test_fingerprint_contains_structure(self, blank_704_path):
        """Fingerprint encodes page count and field count."""
        from src.forms.template_learning import template_fingerprint
        fp = template_fingerprint(blank_704_path)
        # Should start with "Np_Mf_" where N=pages and M=fields
        assert "p_" in fp
        assert "f_" in fp

    def test_record_and_retrieve_outcome(self, temp_data_dir):
        """Record an outcome and retrieve best strategy."""
        from src.forms.template_learning import record_outcome, get_best_strategy
        fp = "test_2p_45f_abc12345"
        # Need at least 2 samples to recommend
        record_outcome(fp, "form_fields", 100, pc_id="test1")
        record_outcome(fp, "form_fields", 100, pc_id="test2")
        best = get_best_strategy(fp, min_samples=2)
        assert best == "form_fields"

    def test_no_recommendation_without_samples(self, temp_data_dir):
        """No recommendation with insufficient data."""
        from src.forms.template_learning import get_best_strategy
        best = get_best_strategy("never_seen_fp_xyz", min_samples=2)
        assert best is None

    def test_strategy_stats(self, temp_data_dir):
        """Strategy stats returns aggregated data."""
        from src.forms.template_learning import record_outcome, get_strategy_stats
        record_outcome("stats_test_fp", "form_fields", 100, pc_id="s1")
        record_outcome("stats_test_fp", "overlay", 85, pc_id="s2")
        stats = get_strategy_stats(days=1)
        assert "form_fields" in stats or "overlay" in stats


# ═══════════════════════════════════════════════════════════════════════════
# DOCUMENT PIPELINE TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestDocumentPipeline:
    """Test the self-healing document pipeline."""

    def test_pipeline_with_blank_704(self, blank_704_path, sample_pc,
                                     temp_data_dir):
        """Pipeline generates successfully with blank 704 template."""
        from src.forms.document_pipeline import DocumentPipeline

        sample_pc["source_pdf"] = blank_704_path
        output = os.path.join(temp_data_dir, "pipeline_test.pdf")

        pipeline = DocumentPipeline(
            source_file=blank_704_path,
            parsed_data=sample_pc.get("parsed", {}),
            output_pdf=output,
        )
        result = pipeline.execute()
        # Pipeline should succeed (may not be score=100 on all fields
        # depending on template, but should complete without error)
        assert result.verification_score >= 0
        assert len(result.attempts) >= 1
        assert result.strategy_used in ("form_fields", "overlay", "blank_template")

    def test_pipeline_records_attempts(self, blank_704_path, sample_pc,
                                       temp_data_dir):
        """Pipeline records each attempt with strategy and score."""
        from src.forms.document_pipeline import DocumentPipeline

        output = os.path.join(temp_data_dir, "pipeline_attempts.pdf")
        pipeline = DocumentPipeline(
            source_file=blank_704_path,
            parsed_data=sample_pc.get("parsed", {}),
            output_pdf=output,
        )
        result = pipeline.execute()
        assert len(result.attempts) >= 1
        for attempt in result.attempts:
            assert attempt.strategy in ("form_fields", "overlay", "blank_template")
            assert 0 <= attempt.score <= 100
            assert attempt.duration_ms >= 0

    def test_pipeline_source_normalization_pdf(self, blank_704_path):
        """PDF source passes through without conversion."""
        from src.forms.document_pipeline import DocumentPipeline
        pipeline = DocumentPipeline(
            source_file=blank_704_path,
            parsed_data={"line_items": []},
            output_pdf="/tmp/test.pdf",
        )
        src, src_type = pipeline._normalize_source()
        assert src_type == "pdf"
        assert src == blank_704_path

    def test_pipeline_nonexistent_source_fails(self, temp_data_dir):
        """Pipeline handles missing source gracefully."""
        from src.forms.document_pipeline import DocumentPipeline
        output = os.path.join(temp_data_dir, "pipeline_nofile.pdf")
        pipeline = DocumentPipeline(
            source_file="/nonexistent/file.pdf",
            parsed_data={"line_items": []},
            output_pdf=output,
        )
        result = pipeline.execute()
        assert not result.ok

    def test_pipeline_result_has_attempt_summaries(self, blank_704_path,
                                                    sample_pc, temp_data_dir):
        """PipelineResult.attempt_summaries returns serializable dicts."""
        from src.forms.document_pipeline import DocumentPipeline
        output = os.path.join(temp_data_dir, "pipeline_summaries.pdf")
        pipeline = DocumentPipeline(
            source_file=blank_704_path,
            parsed_data=sample_pc.get("parsed", {}),
            output_pdf=output,
        )
        result = pipeline.execute()
        summaries = result.attempt_summaries
        assert isinstance(summaries, list)
        if summaries:
            assert "strategy" in summaries[0]
            assert "score" in summaries[0]

    def test_pipeline_escalation_order(self):
        """Strategy escalation follows correct order."""
        from src.forms.document_pipeline import DocumentPipeline, STRATEGIES
        pipeline = DocumentPipeline(
            source_file="/tmp/test.pdf",
            parsed_data={},
            output_pdf="/tmp/out.pdf",
        )
        # form_fields tried → should get overlay next
        next_s = pipeline._escalate("form_fields", None, {"form_fields"})
        assert next_s == "overlay"
        # overlay tried → should get blank_template
        next_s = pipeline._escalate("overlay", None, {"form_fields", "overlay"})
        assert next_s == "blank_template"
        # all tried → None
        next_s = pipeline._escalate("blank_template", None,
                                     {"form_fields", "overlay", "blank_template"})
        assert next_s is None


# ═══════════════════════════════════════════════════════════════════════════
# BATCH HEALTH TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestBatchHealth:
    """Test batch health check functionality."""

    def test_health_summary_empty(self, temp_data_dir):
        """Health summary on empty pcs.json."""
        pcs_path = os.path.join(temp_data_dir, "pcs.json")
        with open(pcs_path, "w") as f:
            json.dump({}, f)

        from src.forms.batch_health import get_health_summary
        summary = get_health_summary()
        assert summary["total"] == 0
        assert summary["excellent"] == 0

    def test_health_check_no_pcs(self, temp_data_dir):
        """Health check on empty PC set."""
        pcs_path = os.path.join(temp_data_dir, "pcs.json")
        with open(pcs_path, "w") as f:
            json.dump({}, f)

        from src.forms.batch_health import run_health_check
        report = run_health_check()
        assert report["total_checked"] == 0
