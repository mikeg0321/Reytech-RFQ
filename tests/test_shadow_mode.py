"""Tests for shadow mode (Phase 2)."""
import io
import json
import os
import tempfile

import pytest

from src.forms.shadow_mode import (
    _compare_pdfs, _log_diff, get_recent_diffs, get_diff_summary, _SHADOW_LOG,
)


@pytest.fixture(autouse=True)
def clean_shadow_log(tmp_path, monkeypatch):
    """Use a temp shadow log file for each test."""
    log_path = str(tmp_path / "shadow_diffs.jsonl")
    monkeypatch.setattr("src.forms.shadow_mode._SHADOW_LOG", log_path)
    return log_path


class TestPdfComparison:
    """Field-level PDF comparison."""

    def test_identical_pdfs_match(self, blank_704_path):
        with open(blank_704_path, "rb") as f:
            pdf_bytes = f.read()
        result = _compare_pdfs(pdf_bytes, pdf_bytes)
        assert result["verdict"] == "match"

    def test_different_pdfs_diverge(self, blank_704_path):
        """Fill one PDF, leave other blank — should diverge."""
        from PyPDFForm import PdfWrapper

        with open(blank_704_path, "rb") as f:
            blank_bytes = f.read()

        filled = PdfWrapper(blank_704_path).fill({"COMPANY NAME": "Reytech Inc."})
        filled_bytes = filled.read()

        result = _compare_pdfs(blank_bytes, filled_bytes)
        assert result["verdict"] == "diverge"
        assert len(result["field_diffs"]) > 0
        assert any(d["field"] == "COMPANY NAME" for d in result["field_diffs"])


class TestDiffLogging:
    """Shadow diff log I/O."""

    def test_log_and_read(self, clean_shadow_log):
        _log_diff("test-001", "pc", "match", "3/3 fields match", 42)
        _log_diff("test-002", "rfq", "diverge", "2/3 match, 1 diverge", 55)

        diffs = get_recent_diffs(limit=10)
        assert len(diffs) == 2
        assert diffs[0]["doc_id"] == "test-002"  # Most recent first
        assert diffs[1]["doc_id"] == "test-001"

    def test_summary(self, clean_shadow_log):
        _log_diff("a", "pc", "match", "ok", 10)
        _log_diff("b", "pc", "match", "ok", 10)
        _log_diff("c", "rfq", "diverge", "bad", 10)
        _log_diff("d", "pc", "match", "ok", 10)

        summary = get_diff_summary()
        assert summary["total"] == 4
        assert summary["matches"] == 3
        assert summary["divergences"] == 1
        assert summary["consecutive_matches"] == 1  # Only "d" after the diverge

    def test_empty_log(self, clean_shadow_log):
        diffs = get_recent_diffs()
        assert diffs == []
        summary = get_diff_summary()
        assert summary["total"] == 0


class TestGraduatedSeverity:
    """Pin the graduated-severity shipped 2026-05-27 after the
    rfq_0124 case: `SHADOW rfq rfq_0124: diverge — 3/102 match, 99
    divergences` was a WARNING in prod logs alongside 5%-divergence
    near-misses. Same line, very different meaning. Graduated severity
    surfaces the wildly-divergent cases as ERROR so they get attention,
    suppresses near-perfect cases as INFO."""

    def test_match_is_info_level(self, clean_shadow_log, caplog):
        import logging
        with caplog.at_level(logging.INFO, logger="src.forms.shadow_mode"):
            _log_diff("doc1", "pc", "match", "10/10 match",
                      42, match_count=10, total_compared=10)
        recs = [r for r in caplog.records if "SHADOW" in r.message]
        assert len(recs) == 1
        assert recs[0].levelno == logging.INFO

    def test_near_perfect_diverge_is_info(self, clean_shadow_log, caplog):
        """95% match → noise, log at INFO not WARNING."""
        import logging
        with caplog.at_level(logging.INFO, logger="src.forms.shadow_mode"):
            _log_diff("doc2", "pc", "diverge", "95/100 match, 5 diverge",
                      42, match_count=95, total_compared=100)
        recs = [r for r in caplog.records if "SHADOW" in r.message]
        assert recs and recs[-1].levelno == logging.INFO

    def test_mid_diverge_is_warning(self, clean_shadow_log, caplog):
        """50-89% match → WARNING (something drifting)."""
        import logging
        with caplog.at_level(logging.INFO, logger="src.forms.shadow_mode"):
            _log_diff("doc3", "pc", "diverge", "70/100 match, 30 diverge",
                      42, match_count=70, total_compared=100)
        recs = [r for r in caplog.records if "SHADOW" in r.message]
        assert recs and recs[-1].levelno == logging.WARNING

    def test_massive_diverge_is_error(self, clean_shadow_log, caplog):
        """<50% match → ERROR (substrate-class divergence, page someone).
        This is the prod rfq_0124 case: 3/102 match (2.9%) was logged
        as WARNING alongside 5%-divergence noise."""
        import logging
        with caplog.at_level(logging.INFO, logger="src.forms.shadow_mode"):
            _log_diff("rfq_0124", "rfq", "diverge",
                      "3/102 match, 99 divergences",
                      2329, match_count=3, total_compared=102)
        recs = [r for r in caplog.records if "SHADOW" in r.message]
        assert recs and recs[-1].levelno == logging.ERROR, (
            "3/102 match (2.9%) must log at ERROR — prod incident "
            "2026-05-27 rfq_0124 was logged as WARNING and ignored."
        )

    def test_error_verdict_stays_warning(self, clean_shadow_log, caplog):
        """Non-`diverge` error verdicts (no_profile, legacy_missing,
        error) keep the old WARNING level — they're operational signals,
        not substrate divergence."""
        import logging
        with caplog.at_level(logging.INFO, logger="src.forms.shadow_mode"):
            _log_diff("doc4", "pc", "no_profile", "No profile found", 0)
        recs = [r for r in caplog.records if "SHADOW" in r.message]
        assert recs and recs[-1].levelno == logging.WARNING

    def test_diverge_without_counts_falls_back_to_warning(
        self, clean_shadow_log, caplog,
    ):
        """If a legacy caller still passes diverge with no match_count /
        total_compared (back-compat), don't crash — fall back to WARNING."""
        import logging
        with caplog.at_level(logging.INFO, logger="src.forms.shadow_mode"):
            _log_diff("doc5", "pc", "diverge", "legacy summary", 10)
        recs = [r for r in caplog.records if "SHADOW" in r.message]
        assert recs and recs[-1].levelno == logging.WARNING


class TestShadowDashboard:
    """Admin dashboard routes."""

    def test_dashboard_page(self, auth_client):
        resp = auth_client.get("/admin/shadow-diffs")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert "Shadow Mode" in html

    def test_api_endpoint(self, auth_client):
        resp = auth_client.get("/api/admin/shadow-diffs")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert "diffs" in data
        assert "summary" in data

    def test_clear_endpoint(self, auth_client):
        resp = auth_client.post("/api/admin/shadow-diffs/clear")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True

    def test_auth_required(self, anon_client):
        assert anon_client.get("/admin/shadow-diffs").status_code == 401
        assert anon_client.get("/api/admin/shadow-diffs").status_code == 401
