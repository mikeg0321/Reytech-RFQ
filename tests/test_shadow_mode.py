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
