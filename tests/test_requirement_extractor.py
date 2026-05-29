"""Tests for Email Requirements Extractor.

Tests regex extraction, dataclass serialization, Claude mock,
and validate_against_requirements(). All offline — no real API calls.
"""
import json
import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from src.agents.requirement_extractor import (
    RFQRequirements,
    extract_requirements,
    _extract_with_regex,
    _detect_forms,
    _extract_due_date,
    _extract_due_time,
    _extract_solicitation_number,
    _extract_phone,
    _detect_food_items,
    _filter_trusted_urls,
    _classify_attachments,
)


# ═══════════════════════════════════════════════════════════════════════════
# RFQRequirements Dataclass
# ═══════════════════════════════════════════════════════════════════════════

class TestRFQRequirements:

    def test_default_values(self):
        r = RFQRequirements()
        assert r.forms_required == []
        assert r.due_date == ""
        assert r.confidence == 0.0
        assert r.extraction_method == "none"
        assert r.has_requirements is False

    def test_to_dict_roundtrip(self):
        r = RFQRequirements(
            forms_required=["std204", "dvbe843"],
            due_date="2026-04-15",
            confidence=0.85,
        )
        d = r.to_dict()
        assert d["forms_required"] == ["std204", "dvbe843"]
        assert d["due_date"] == "2026-04-15"

    def test_from_dict(self):
        d = {"forms_required": ["703b"], "due_date": "2026-04-15", "confidence": 0.9}
        r = RFQRequirements.from_dict(d)
        assert r.forms_required == ["703b"]
        assert r.due_date == "2026-04-15"

    def test_from_dict_empty(self):
        r = RFQRequirements.from_dict({})
        assert r.forms_required == []

    def test_from_dict_none(self):
        r = RFQRequirements.from_dict(None)
        assert r.forms_required == []

    def test_json_roundtrip(self):
        r = RFQRequirements(
            forms_required=["std204"], due_date="2026-04-15",
            food_items_present=True, confidence=0.85,
        )
        j = json.dumps(r.to_dict())
        r2 = RFQRequirements.from_dict(json.loads(j))
        assert r2.forms_required == r.forms_required
        assert r2.food_items_present is True

    def test_has_requirements(self):
        assert RFQRequirements().has_requirements is False
        assert RFQRequirements(forms_required=["703b"]).has_requirements is True
        assert RFQRequirements(due_date="2026-04-15").has_requirements is True
        assert RFQRequirements(solicitation_number="RFQ-001").has_requirements is True


# ═══════════════════════════════════════════════════════════════════════════
# Regex Form Detection
# ═══════════════════════════════════════════════════════════════════════════

class TestDetectForms:

    def test_detects_std_204(self):
        forms = _detect_forms("Please complete the STD 204 and return with your bid")
        assert "std204" in forms

    def test_detects_dvbe_843(self):
        forms = _detect_forms("DVBE Declaration (DGS PD 843) required for all bids")
        assert "dvbe843" in forms

    def test_detects_obs_1600(self):
        forms = _detect_forms("Food items require OBS 1600 certification")
        assert "obs_1600" in forms

    def test_detects_multiple_forms(self):
        text = "Please complete STD 204, DVBE 843, Darfur Act, and CalRecycle 074"
        forms = _detect_forms(text)
        assert "std204" in forms
        assert "dvbe843" in forms
        assert "darfur_act" in forms
        assert "calrecycle74" in forms

    def test_detects_703b(self):
        forms = _detect_forms("Complete the AMS 703B and 704B forms")
        assert "703b" in forms
        assert "704b" in forms

    def test_empty_text(self):
        assert _detect_forms("") == []
        assert _detect_forms(None) == []


# ═══════════════════════════════════════════════════════════════════════════
# Regex Due Date Extraction
# ═══════════════════════════════════════════════════════════════════════════

class TestExtractDueDate:

    def test_by_date_slash_format(self):
        assert _extract_due_date("Please respond by 4/15/2026") == "2026-04-15"

    def test_by_date_with_eob(self):
        result = _extract_due_date("by End of Business Wednesday 4/8/2026")
        assert result == "2026-04-08"

    def test_by_date_written_month(self):
        result = _extract_due_date("Please respond by April 15, 2026")
        assert result == "2026-04-15"

    def test_due_keyword(self):
        result = _extract_due_date("due 4/15/2026")
        assert result == "2026-04-15"

    def test_no_date(self):
        assert _extract_due_date("No date mentioned here") == ""

    def test_empty(self):
        assert _extract_due_date("") == ""

    def test_two_digit_year(self):
        result = _extract_due_date("deadline 4/15/26")
        assert result == "2026-04-15"


class TestExtractDueTime:

    def test_time_with_am_pm(self):
        result = _extract_due_time("Please respond by 2:00 PM PST")
        assert "2:00 PM" in result

    def test_eob(self):
        result = _extract_due_time("by End of Business Friday")
        assert result == "COB"

    def test_no_time(self):
        assert _extract_due_time("No time mentioned") == ""


# ═══════════════════════════════════════════════════════════════════════════
# Other Regex Extractors
# ═══════════════════════════════════════════════════════════════════════════

class TestSolicitationNumber:

    def test_rfq_number(self):
        result = _extract_solicitation_number("RFQ #10838043 for Stryker items")
        assert "10838043" in result

    def test_solicitation_keyword(self):
        result = _extract_solicitation_number("Solicitation Number: RFQ-2026-TEST")
        assert "RFQ-2026-TEST" in result

    def test_no_sol_num(self):
        assert _extract_solicitation_number("No solicitation here") == ""


class TestPhone:

    def test_extracts_phone(self):
        result = _extract_phone("Contact me at Phone: (916) 555-1234")
        assert "916" in result and "1234" in result

    def test_no_phone(self):
        assert _extract_phone("No phone number") == ""


class TestFoodDetection:

    def test_food_items(self):
        assert _detect_food_items("Food items in this order require OBS 1600") is True

    def test_agricultural(self):
        assert _detect_food_items("Agricultural product certification needed") is True

    def test_perishable_alone_is_not_food(self):
        # "perishable" was removed — refrigerated meds/vaccines are perishable
        # but are NOT agricultural food (OBS 1600 does not apply).
        assert _detect_food_items("Perishable goods included") is False

    def test_nutritional_alone_is_not_food(self):
        # "nutritional supplement" = vitamins/medical, not OBS-1600 food.
        assert _detect_food_items("Nutritional supplement, 90 count") is False

    def test_expiration_boilerplate_is_not_food(self):
        # Incident 2026-05-28: standard CDCR dating clause flipped the food
        # flag on a durable IV-pole mount. This boilerplate must NOT trigger.
        clause = ("ALL dated materials must have a manufacturer's expiration "
                  "date at least 13 months after the material is received by "
                  "the institution's warehouse")
        assert _detect_food_items(clause) is False

    def test_no_food(self):
        assert _detect_food_items("Medical supplies order") is False

    def test_empty(self):
        assert _detect_food_items("") is False


class TestUrlFiltering:

    def test_trusted_ca_gov(self):
        urls = ["https://www.dgs.ca.gov/forms/std204.pdf"]
        assert len(_filter_trusted_urls(urls)) == 1

    def test_untrusted_rejected(self):
        urls = ["https://malicious-site.com/payload.exe"]
        assert len(_filter_trusted_urls(urls)) == 0

    def test_sharepoint_trusted(self):
        urls = ["https://cdcr.sharepoint.com/sites/procurement/doc.pdf"]
        assert len(_filter_trusted_urls(urls)) == 1

    def test_mixed(self):
        urls = [
            "https://dgs.ca.gov/form.pdf",
            "https://untrusted.com/bad",
            "https://drive.google.com/file/abc",
        ]
        assert len(_filter_trusted_urls(urls)) == 2


class TestClassifyAttachments:

    def test_704_attachment(self):
        types = _classify_attachments([{"filename": "AMS_704_April.pdf"}])
        assert "704" in types

    def test_703b_attachment(self):
        types = _classify_attachments([{"filename": "703B_template.pdf"}])
        assert "703b" in types

    def test_generic_pdf(self):
        types = _classify_attachments([{"filename": "document.pdf"}])
        assert "pdf" in types


# ═══════════════════════════════════════════════════════════════════════════
# Full Regex Extraction
# ═══════════════════════════════════════════════════════════════════════════

class TestFullRegexExtraction:

    def test_full_email(self):
        body = """
        Good afternoon,

        Please complete the attached 703B and 704B for the following items.
        Include STD 204 and DVBE 843 with your response.

        Due by End of Business 4/15/2026.

        Ship to: CIW 16756 Chino-Corona Road, Corona, CA 92880

        Phone: (951) 555-7890

        Thank you,
        Jane Smith
        """
        result = _extract_with_regex(body, [{"filename": "AMS_704B.pdf"}])
        assert "703b" in result.forms_required
        assert "704b" in result.forms_required
        assert "std204" in result.forms_required
        assert "dvbe843" in result.forms_required
        assert result.due_date == "2026-04-15"
        assert result.extraction_method == "regex"
        assert result.confidence >= 0.50

    def test_empty_email(self):
        result = _extract_with_regex("", [])
        assert result.forms_required == []
        assert result.confidence <= 0.50


# ═══════════════════════════════════════════════════════════════════════════
# Claude API Extraction (Mocked)
# ═══════════════════════════════════════════════════════════════════════════

class TestClaudeExtraction:

    def test_falls_back_to_regex_without_api_key(self):
        """No API key → regex fallback."""
        with patch.dict(os.environ, {}, clear=True):
            result = extract_requirements("Please complete STD 204", "Test RFQ")
            assert result.extraction_method == "regex"
            assert "std204" in result.forms_required

    def test_successful_claude_extraction(self):
        """Mock successful Claude API response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"type": "text", "text": json.dumps({
                "forms_required": ["std204", "dvbe843"],
                "due_date": "2026-04-20",
                "due_time": "5:00 PM PST",
                "special_instructions": ["Include food certification"],
                "delivery_location": "CSP-Sacramento",
                "buyer_name": "Jane Smith",
                "buyer_phone": "(916) 555-1234",
                "solicitation_number": "RFQ-2026-100",
                "food_items_present": True,
                "template_urls": [],
            })}],
        }

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("src.agents.requirement_extractor.requests") as mock_req:
                mock_req.post.return_value = mock_response
                mock_req.exceptions = type("E", (), {"Timeout": TimeoutError})
                result = extract_requirements("Complete STD 204 and DVBE", "RFQ Test")

        assert result.extraction_method == "claude"
        assert "std204" in result.forms_required
        assert result.due_date == "2026-04-20"
        assert result.food_items_present is True
        assert result.confidence >= 0.80

    def test_429_falls_back_to_regex(self):
        """Rate limited → regex fallback."""
        mock_response = MagicMock()
        mock_response.status_code = 429

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("src.agents.requirement_extractor.requests") as mock_req:
                mock_req.post.return_value = mock_response
                mock_req.exceptions = type("E", (), {"Timeout": TimeoutError})
                result = extract_requirements("Complete STD 204", "Test")

        assert result.extraction_method == "regex"


# ═══════════════════════════════════════════════════════════════════════════
# Validate Against Requirements
# ═══════════════════════════════════════════════════════════════════════════

class TestValidateAgainstRequirements:

    def test_missing_form_flagged(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({"forms_required": ["obs_1600"], "confidence": 0.85})
        result = validate_against_requirements(
            generated_files=["RFQ_703B_Reytech.pdf"],
            requirements_json=reqs,
        )
        gaps = [g["form_id"] for g in result["gaps"]]
        assert "obs_1600" in gaps

    def test_present_form_confirmed(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({"forms_required": ["703b"], "confidence": 0.85})
        result = validate_against_requirements(
            generated_files=["RFQ_703B_Reytech.pdf"],
            requirements_json=reqs,
        )
        assert "703b" in result["confirmed"]
        assert len(result["gaps"]) == 0

    def test_food_items_flag(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({"forms_required": [], "food_items_present": True, "confidence": 0.8})
        result = validate_against_requirements(
            generated_files=["RFQ_703B_Reytech.pdf"],
            requirements_json=reqs,
        )
        food_gaps = [g for g in result["gaps"] if "food" in g["msg"].lower()]
        assert len(food_gaps) > 0

    def test_due_date_mismatch(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({"forms_required": [], "due_date": "2026-04-15", "confidence": 0.8})
        result = validate_against_requirements(
            generated_files=[],
            requirements_json=reqs,
            rfq_data={"due_date": "TBD"},
        )
        due_gaps = [g for g in result["gaps"] if g["type"] == "due_date_missing"]
        assert len(due_gaps) > 0

    def test_empty_requirements(self):
        from src.forms.form_qa import validate_against_requirements
        result = validate_against_requirements([], "{}", {})
        assert result["gaps"] == []

    def test_malformed_json(self):
        from src.forms.form_qa import validate_against_requirements
        result = validate_against_requirements([], "not json", {})
        assert result["gaps"] == []


# ── LAW 6 substrate: attachment filenames are scanned for form patterns ────
# Added 2026-05-27 after Coleman sol# 10842771 surfaced the gap. The buyer's
# 703A revision marker lived in the attached PDF filename, NOT the email body.
# Without this, `_detect_forms()` missed 703a entirely on every CCHCS RFQ.
# Closes the LAW 6 spec: "ingest reads ALL attached documents".

class TestAttachmentFilenameScanning:
    """LAW 6 substrate: email body + attachment filenames are BOTH the
    source of truth for required forms. The buyer's email + their attached
    PDF filenames together are the canonical specification of the response.
    """

    def test_703a_from_attachment_filename(self):
        """Coleman sol# 10842771 scenario: 703A marker in attachment
        filename only. extract_requirements must pick it up."""
        from src.agents.requirement_extractor import extract_requirements
        result = extract_requirements(
            email_body="Please quote per attached.",
            subject="RFQ 10842771",
            attachments=[
                {"filename": "PR 10842771 - AMS 703A - REQUEST FOR QUOTATION.pdf"},
            ],
        )
        assert "703a" in result.forms_required, (
            "703a must be detected from attachment filename — pinned by "
            "Coleman sol# 10842771 substrate convergence 2026-05-27."
        )

    def test_703b_from_filename(self):
        from src.agents.requirement_extractor import extract_requirements
        result = extract_requirements(
            email_body="See attached.",
            subject="",
            attachments=[{"filename": "AMS 703B Rev 2024.pdf"}],
        )
        assert "703b" in result.forms_required

    def test_703c_from_filename(self):
        from src.agents.requirement_extractor import extract_requirements
        result = extract_requirements(
            email_body="See attached.",
            subject="",
            attachments=[{"filename": "AMS 703C - Fair_and_Reasonable.pdf"}],
        )
        assert "703c" in result.forms_required

    def test_704b_from_filename(self):
        from src.agents.requirement_extractor import extract_requirements
        result = extract_requirements(
            email_body="",
            subject="",
            attachments=[{"filename": "AMS 704B Quote Worksheet.pdf"}],
        )
        assert "704b" in result.forms_required

    def test_coleman_full_email_extracts_all_forms(self):
        """End-to-end Coleman scenario: body lists 6 forms, attachments
        carry 703A + 704B + Bid Package + Distribution List. Expected
        extraction: all 9 form_ids surface to requirements_json."""
        from src.agents.requirement_extractor import extract_requirements
        result = extract_requirements(
            email_body=(
                "Please attach the following required forms: "
                "CalRecycle 74 (Required), GSPD-05-105 BIDDER DECLARATION (Required), "
                "DGS PD 1 DARFUR (Required), CCHCS-MC-345 COMMERCIALLY USEFUL FUNCTION (Required), "
                "SELLERS PERMIT (Required), DGS PD 843 DVBE (Required, if applicable)"
            ),
            subject="AMS 703A — RFQ Coleman 10842771",
            attachments=[
                {"filename": "PR 10842771 - AMS 703A - REQUEST FOR QUOTATION.pdf"},
                {"filename": "PR 10842771 - AMS 704B - CCHCS Acquisition Quote Worksheet.pdf"},
                {"filename": "BID PACKAGE _ FORMS (Under 100k).pdf"},
                {"filename": "PR 10842771 - DELIVERY DISTRIBUTION LIST.pdf"},
            ],
        )
        # Every form the buyer asked for + every form the attachments name
        expected = {"703a", "704b", "bidder_decl", "calrecycle74", "cv012_cuf",
                    "darfur_act", "dvbe843", "sellers_permit"}
        missing = expected - set(result.forms_required)
        assert not missing, (
            f"Coleman LAW 6 extraction missed forms: {missing}. "
            f"Got: {sorted(result.forms_required)}"
        )

    def test_no_attachments_does_not_break(self):
        """No-attachment case must still work (body-only extraction)."""
        from src.agents.requirement_extractor import extract_requirements
        result = extract_requirements(
            email_body="Please submit STD 204 and DVBE 843.",
            subject="Bid request",
            attachments=None,
        )
        assert "std204" in result.forms_required
        assert "dvbe843" in result.forms_required

    def test_empty_everything_returns_empty(self):
        """LAW 6 guard: empty body + empty subject + empty attachments
        returns empty RFQRequirements (no false positives)."""
        from src.agents.requirement_extractor import extract_requirements
        result = extract_requirements("", "", [])
        assert result.forms_required == []
