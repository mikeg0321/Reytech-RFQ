"""Tests for FormProfiler — blank-PDF → YAML draft.

Uses the real `ams_704_blank.pdf` fixture (187 AcroForm fields) so we're
exercising the classifier against a known form, not synthetic names.
The LLM path is NOT tested here — we stub `_classify_non_row_fields`
so the tests are hermetic and cheap.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest
import yaml

from src.agents.form_profiler import (
    profile_blank_pdf,
    _classify_row_fields,
    _match_stem_to_semantic,
    _CANONICAL_SEMANTICS,
)
from src.forms.profile_registry import load_profile, validate_profile


FIXTURE_704 = "tests/fixtures/ams_704_blank.pdf"


# ── Row-field derivation ────────────────────────────────────────────────────

class TestRowDetection:
    def test_derives_704_page_capacities(self):
        """704 blank has 11 rows on page 1, 8 rows on page 2."""
        if not os.path.exists(FIXTURE_704):
            pytest.skip("ams_704_blank.pdf fixture missing")

        from pypdf import PdfReader
        fields = sorted((PdfReader(FIXTURE_704).get_fields() or {}).keys())
        _tmpl, caps, non_row = _classify_row_fields(fields)

        assert caps == [11, 8], f"expected [11, 8], got {caps}"
        # Every non-row field must NOT match the Row<n> pattern.
        for f in non_row:
            assert "Row" not in f or not f.rstrip("_0123456789").endswith("Row"), (
                f"non_row leaked a row field: {f}"
            )

    def test_row_stem_matcher_is_deterministic(self):
        """Keyword rules are ordered — specific before generic."""
        # `SUBSTITUTED ITEM…` must NOT match `item` first.
        stems = [
            "ITEM ",
            "ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference number",
            "QTY",
            "UNIT OF MEASURE UOM",
            "QTY PER UOM",
            "PRICE PER UNIT",
            "EXTENSION",
            "SUBSTITUTED ITEM Include manufacturer part number andor reference number",
        ]
        mapping = _match_stem_to_semantic(stems)
        assert mapping["ITEM "] == "items[n].item_no"
        assert mapping["ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference number"] \
            == "items[n].description"
        assert mapping["QTY"] == "items[n].qty"
        assert mapping["UNIT OF MEASURE UOM"] == "items[n].uom"
        assert mapping["QTY PER UOM"] == "items[n].qty_per_uom"
        assert mapping["PRICE PER UNIT"] == "items[n].unit_price"
        assert mapping["EXTENSION"] == "items[n].extension"
        assert mapping["SUBSTITUTED ITEM Include manufacturer part number andor reference number"] \
            == "items[n].substituted"

    def test_empty_fields_input(self):
        tmpl, caps, non_row = _classify_row_fields([])
        assert tmpl == {}
        assert caps == []
        assert non_row == []


# ── End-to-end against real 704 blank (LLM stubbed) ────────────────────────

class TestProfileBlankPdf:
    def test_missing_pdf_returns_issue(self):
        result = profile_blank_pdf(
            "/does/not/exist.pdf",
            form_id="fake_form",
        )
        assert result.yaml_text == ""
        assert any("not found" in i for i in result.issues)

    @patch("src.agents.form_profiler._classify_non_row_fields")
    def test_generates_valid_yaml_against_704_blank(self, mock_llm, tmp_path):
        """With a stubbed LLM classifier that returns the canonical 704a
        header mappings, the profiler should emit a YAML that passes
        `validate_profile()` when reloaded."""
        if not os.path.exists(FIXTURE_704):
            pytest.skip("ams_704_blank.pdf fixture missing")

        # Canonical mappings pulled from 704a_reytech_standard.yaml —
        # this is the ground truth a well-trained LLM should emit.
        mock_llm.return_value = [
            {"pdf_field": "Text1", "semantic": "header.solicitation_number", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "Text2", "semantic": "header.due_date", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "Time", "semantic": "header.due_time", "field_type": "text", "confidence": 0.9},
            {"pdf_field": "AM PST", "semantic": "header.am_pst", "field_type": "checkbox", "confidence": 0.95},
            {"pdf_field": "PM PST", "semantic": "header.pm_pst", "field_type": "checkbox", "confidence": 0.95},
            {"pdf_field": "PRICE CHECK", "semantic": "header.price_check", "field_type": "checkbox", "confidence": 0.95},
            {"pdf_field": "Requestor", "semantic": "buyer.requestor_name", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "Institution or HQ Program", "semantic": "buyer.institution", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "Phone Number", "semantic": "buyer.phone", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "Date of Request", "semantic": "buyer.date_of_request", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "Ship to", "semantic": "ship_to.address", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "Delivery Zip Code", "semantic": "ship_to.zip_code", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "COMPANY NAME", "semantic": "vendor.name", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "SUPPLIER NAME", "semantic": "vendor.supplier_name", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "COMPANY REPRESENTATIVE print name", "semantic": "vendor.representative", "field_type": "text", "confidence": 0.9},
            {"pdf_field": "Address", "semantic": "vendor.address", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "Phone Number_2", "semantic": "vendor.phone", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "EMail Address", "semantic": "vendor.email", "field_type": "text", "confidence": 0.95},
            {"pdf_field": "Certified SBMB", "semantic": "vendor.sb_cert", "field_type": "text", "confidence": 0.9},
            {"pdf_field": "Certified DVBE", "semantic": "vendor.dvbe_cert", "field_type": "text", "confidence": 0.9},
            {"pdf_field": "Delivery Date and Time ARO", "semantic": "vendor.delivery", "field_type": "text", "confidence": 0.9},
            {"pdf_field": "Discount Offered", "semantic": "vendor.discount", "field_type": "text", "confidence": 0.9},
            {"pdf_field": "Date Price Check Expires", "semantic": "vendor.expires", "field_type": "text", "confidence": 0.9},
            {"pdf_field": "Signature and Date", "semantic": "vendor.signature", "field_type": "signature", "confidence": 0.95},
            {"pdf_field": "FOB Destination Freight Prepaid", "semantic": "shipping.fob_prepaid", "field_type": "checkbox", "confidence": 0.95},
            {"pdf_field": "FOB Destination PPADD", "semantic": "shipping.fob_ppadd", "field_type": "checkbox", "confidence": 0.95},
            {"pdf_field": "FOB Origin Freight Collect", "semantic": "shipping.fob_collect", "field_type": "checkbox", "confidence": 0.95},
            {"pdf_field": "fill_70", "semantic": "totals.subtotal", "field_type": "text", "confidence": 0.7},
            {"pdf_field": "fill_71", "semantic": "totals.freight", "field_type": "text", "confidence": 0.7},
            {"pdf_field": "fill_72", "semantic": "totals.tax", "field_type": "text", "confidence": 0.7},
            {"pdf_field": "fill_73", "semantic": "totals.total", "field_type": "text", "confidence": 0.7},
            {"pdf_field": "Supplier andor Requestor Notes", "semantic": "totals.notes", "field_type": "text", "confidence": 0.9},
            {"pdf_field": "Page", "semantic": "page.number", "field_type": "text", "confidence": 0.9},
            {"pdf_field": "of", "semantic": "page.of", "field_type": "text", "confidence": 0.9},
        ]

        result = profile_blank_pdf(
            FIXTURE_704,
            form_id="704a",
            profile_id="704a_profiler_test",
        )

        assert result.yaml_text, "expected YAML output"
        assert result.page_row_capacities == [11, 8]

        # Round-trip: write to a temp file and reload via the registry.
        out_path = tmp_path / "704a_profiler_test.yaml"
        out_path.write_text(result.yaml_text, encoding="utf-8")

        # YAML must at least parse cleanly.
        data = yaml.safe_load(out_path.read_text(encoding="utf-8"))
        assert data["id"] == "704a_profiler_test"
        assert data["page_row_capacities"] == [11, 8]
        assert "items[n].description" in data["fields"]
        assert "vendor.name" in data["fields"]

        # Validate through the production registry against the real blank PDF.
        profile = load_profile(str(out_path))
        issues = validate_profile(profile)
        # All canonical fields mapped above must exist in the blank PDF.
        assert issues == [], f"validation issues against real blank PDF: {issues}"

    @patch("src.agents.form_profiler._classify_non_row_fields", return_value=[])
    def test_llm_unavailable_still_emits_draft(self, _mock_llm):
        """When the LLM is unavailable, the profiler must still emit a YAML
        skeleton with TODO markers for every non-row field — not silently
        return nothing."""
        if not os.path.exists(FIXTURE_704):
            pytest.skip("ams_704_blank.pdf fixture missing")

        result = profile_blank_pdf(FIXTURE_704, form_id="704a")
        assert result.yaml_text, "should still produce a draft skeleton"
        assert any("TODO (auto)" in line for line in result.yaml_text.splitlines())
        assert any("unmapped" in i or "classification unavailable" in i for i in result.issues)


class TestCanonicalSchema:
    def test_canonical_list_matches_704a_ground_truth(self):
        """All semantic names used by 704a_reytech_standard.yaml must be
        in the canonical allowed list — otherwise the profiler could
        never re-derive that profile."""
        profile_path = "src/forms/profiles/704a_reytech_standard.yaml"
        if not os.path.exists(profile_path):
            pytest.skip("704a profile missing")

        with open(profile_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        row_allowed = {
            "items[n].item_no", "items[n].description", "items[n].qty",
            "items[n].uom", "items[n].qty_per_uom", "items[n].unit_price",
            "items[n].extension", "items[n].substituted",
        }
        for semantic in raw.get("fields", {}).keys():
            if semantic in row_allowed:
                continue
            assert semantic in _CANONICAL_SEMANTICS, (
                f"704a uses semantic {semantic!r} which is NOT in "
                f"_CANONICAL_SEMANTICS — the profiler cannot produce it"
            )
