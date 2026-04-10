"""Tests for Email Contract System V2.

Tests: QA pre-send gate with requirements, re-extract endpoint,
template downloader URL validation and form classification.
"""
import json
import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from src.agents.template_downloader import (
    is_trusted_url,
    classify_form_type,
    _convert_drive_url,
    _domain_is_trusted,
    _sanitize_filename,
    TRUSTED_DOMAINS,
)


# ═══════════════════════════════════════════════════════════════════════════
# URL Trust Validation
# ═══════════════════════════════════════════════════════════════════════════

class TestUrlTrust:

    def test_ca_gov_trusted(self):
        assert is_trusted_url("https://www.dgs.ca.gov/forms/std204.pdf") is True

    def test_sharepoint_trusted(self):
        assert is_trusted_url("https://cdcr.sharepoint.com/sites/docs/file.pdf") is True

    def test_google_drive_trusted(self):
        assert is_trusted_url("https://drive.google.com/file/d/abc123/view") is True

    def test_onedrive_trusted(self):
        assert is_trusted_url("https://onedrive.live.com/download?id=abc") is True

    def test_random_site_rejected(self):
        assert is_trusted_url("https://evil-site.com/malware.exe") is False

    def test_typosquat_rejected(self):
        assert is_trusted_url("https://ca.gov.evil.com/fake.pdf") is False

    def test_empty_url(self):
        assert is_trusted_url("") is False

    def test_malformed_url(self):
        assert is_trusted_url("not-a-url") is False

    def test_subdomain_ca_gov(self):
        assert is_trusted_url("https://procurement.cchcs.ca.gov/rfq/download") is True

    def test_domain_helper(self):
        assert _domain_is_trusted("www.dgs.ca.gov") is True
        assert _domain_is_trusted("evil.com") is False


# ═══════════════════════════════════════════════════════════════════════════
# Google Drive URL Conversion
# ═══════════════════════════════════════════════════════════════════════════

class TestDriveUrlConversion:

    def test_drive_file_view_to_download(self):
        url = "https://drive.google.com/file/d/1a2b3c4d/view"
        converted = _convert_drive_url(url)
        assert "uc?export=download" in converted
        assert "1a2b3c4d" in converted

    def test_docs_to_docx_export(self):
        url = "https://docs.google.com/document/d/abc123/edit"
        converted = _convert_drive_url(url)
        assert "export?format=docx" in converted
        assert "abc123" in converted

    def test_non_drive_url_unchanged(self):
        url = "https://dgs.ca.gov/forms/std204.pdf"
        assert _convert_drive_url(url) == url


# ═══════════════════════════════════════════════════════════════════════════
# Form Type Classification
# ═══════════════════════════════════════════════════════════════════════════

class TestFormClassification:

    def test_703b_from_filename(self):
        assert classify_form_type("AMS_703B_template.pdf") == "703b"

    def test_704b_from_filename(self):
        assert classify_form_type("704B_Quote_Worksheet.pdf") == "704b"

    def test_bid_package(self):
        assert classify_form_type("CDCR_bid_package_2026.pdf") == "bidpkg"

    def test_std204_payee(self):
        assert classify_form_type("STD204_Payee_Data.pdf") == "std204"

    def test_unknown_generic(self):
        assert classify_form_type("random_document.pdf") == "unknown"

    def test_url_helps_classify(self):
        assert classify_form_type("form.pdf", "https://dgs.ca.gov/704b/download") == "704b"

    def test_calrecycle(self):
        assert classify_form_type("CalRecycle_074_form.pdf") == "calrecycle"

    def test_darfur(self):
        assert classify_form_type("Darfur_Act_Declaration.pdf") == "darfur"


# ═══════════════════════════════════════════════════════════════════════════
# Filename Sanitization
# ═══════════════════════════════════════════════════════════════════════════

class TestFilenameSanitization:

    def test_normal_filename(self):
        assert _sanitize_filename("AMS_704B.pdf") == "AMS_704B.pdf"

    def test_unsafe_chars_removed(self):
        result = _sanitize_filename("file<script>.pdf")
        assert "<" not in result
        assert ">" not in result

    def test_empty_returns_default(self):
        assert _sanitize_filename("") == "template.pdf"

    def test_spaces_preserved(self):
        result = _sanitize_filename("Bid Package 2026.pdf")
        assert "Bid Package" in result or "Bid_Package" in result


# ═══════════════════════════════════════════════════════════════════════════
# QA Gate with Requirements
# ═══════════════════════════════════════════════════════════════════════════

class TestQaGateRequirements:

    def test_qa_check_returns_requirement_gaps(self, client, seed_rfq, temp_data_dir):
        """QA check endpoint should include requirement_gaps in response."""
        # Seed an RFQ with requirements
        rfqs_path = os.path.join(temp_data_dir, "rfqs.json")
        with open(rfqs_path) as f:
            rfqs = json.load(f)
        rfq = rfqs[seed_rfq]
        rfq["requirements_json"] = json.dumps({
            "forms_required": ["obs_1600"],
            "confidence": 0.85,
        })
        rfq["line_items"] = [{"description": "Test", "qty": 1, "price_per_unit": 10}]
        rfqs[seed_rfq] = rfq
        with open(rfqs_path, "w") as f:
            json.dump(rfqs, f)

        resp = client.get(f"/api/rfq/{seed_rfq}/qa-check")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert "requirement_gaps" in data

    def test_qa_check_without_requirements(self, client, seed_rfq, temp_data_dir):
        """QA check with no requirements should still work."""
        # Seed minimal RFQ
        rfqs_path = os.path.join(temp_data_dir, "rfqs.json")
        with open(rfqs_path) as f:
            rfqs = json.load(f)
        rfq = rfqs[seed_rfq]
        rfq["line_items"] = [{"description": "Test", "qty": 1, "price_per_unit": 10}]
        rfqs[seed_rfq] = rfq
        with open(rfqs_path, "w") as f:
            json.dump(rfqs, f)

        resp = client.get(f"/api/rfq/{seed_rfq}/qa-check")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data.get("requirement_gaps") == [] or "requirement_gaps" in data


# ═══════════════════════════════════════════════════════════════════════════
# Re-Extract Endpoint
# ═══════════════════════════════════════════════════════════════════════════

class TestReExtractEndpoint:

    def test_re_extract_with_body_text(self, client, seed_rfq, temp_data_dir):
        """Re-extract should work when body_text exists."""
        rfqs_path = os.path.join(temp_data_dir, "rfqs.json")
        with open(rfqs_path) as f:
            rfqs = json.load(f)
        rfq = rfqs[seed_rfq]
        rfq["body_text"] = "Please complete STD 204 and DVBE 843. Due by 4/15/2026."
        rfq["email_subject"] = "RFQ for medical supplies"
        rfqs[seed_rfq] = rfq
        with open(rfqs_path, "w") as f:
            json.dump(rfqs, f)

        resp = client.post(f"/api/rfq/{seed_rfq}/re-extract-requirements")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["ok"] is True
        assert data["forms_found"] >= 2  # std204 + dvbe843 at minimum

    def test_re_extract_without_body(self, client, seed_rfq, temp_data_dir):
        """Re-extract without body_text should return gracefully."""
        rfqs_path = os.path.join(temp_data_dir, "rfqs.json")
        with open(rfqs_path) as f:
            rfqs = json.load(f)
        rfq = rfqs[seed_rfq]
        rfq.pop("body_text", None)
        rfq.pop("body_preview", None)
        rfq.pop("email_subject", None)
        rfqs[seed_rfq] = rfq
        with open(rfqs_path, "w") as f:
            json.dump(rfqs, f)

        resp = client.post(f"/api/rfq/{seed_rfq}/re-extract-requirements")
        assert resp.status_code == 200

    def test_re_extract_requires_auth(self, anon_client):
        """Unauthenticated re-extract should be rejected."""
        resp = anon_client.post("/api/rfq/fake-id/re-extract-requirements")
        assert resp.status_code in (401, 403, 302)


# ═══════════════════════════════════════════════════════════════════════════
# Template Downloader (Mocked HTTP)
# ═══════════════════════════════════════════════════════════════════════════

class TestTemplateDownloader:

    def test_download_skips_untrusted(self, tmp_path):
        from src.agents.template_downloader import download_templates
        results = download_templates(
            ["https://evil.com/malware.exe"],
            "test-rfq", str(tmp_path),
        )
        assert results == []

    def test_download_with_trusted_url_mocked(self, tmp_path):
        """Mock a successful download from ca.gov."""
        from src.agents.template_downloader import download_templates

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.url = "https://dgs.ca.gov/forms/STD204.pdf"
        mock_resp.headers = {
            "Content-Type": "application/pdf",
            "Content-Length": "1024",
            "Content-Disposition": 'attachment; filename="STD204.pdf"',
        }
        mock_resp.iter_content = MagicMock(return_value=[b"%PDF-fake-content"])

        with patch("src.agents.template_downloader.requests") as mock_req:
            mock_req.get.return_value = mock_resp
            results = download_templates(
                ["https://dgs.ca.gov/forms/STD204.pdf"],
                "test-rfq", str(tmp_path),
            )

        assert len(results) == 1
        assert results[0]["form_type"] == "std204"
        assert results[0]["filename"] == "STD204.pdf"

    def test_download_caps_at_5(self, tmp_path):
        """Should not download more than 5 URLs per RFQ."""
        from src.agents.template_downloader import download_templates
        urls = [f"https://dgs.ca.gov/form{i}.pdf" for i in range(10)]

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.url = "https://dgs.ca.gov/form.pdf"
        mock_resp.headers = {"Content-Type": "application/pdf", "Content-Length": "100"}
        mock_resp.iter_content = MagicMock(return_value=[b"%PDF"])

        with patch("src.agents.template_downloader.requests") as mock_req:
            mock_req.get.return_value = mock_resp
            results = download_templates(urls, "test-rfq", str(tmp_path))

        assert len(results) <= 5
