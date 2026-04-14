"""Tests for request_classifier + QuoteRequest wrapper.

Pins the classifier behavior against real buyer fixtures that Mike
dropped into `tests/fixtures/unified_ingest/`. Any future drift in
the classifier shows up as a failing test with a clear message.
"""
import json
import os

import pytest


FIX_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "fixtures", "unified_ingest",
)

CCHCS_PACKET = os.path.join(FIX_DIR, "cchcs_packet_preq.pdf")
PC_DOCX_FOOD = os.path.join(FIX_DIR, "pc_docx_food.docx")
PC_DOCX_NON_FOOD = os.path.join(FIX_DIR, "pc_docx_non_food.docx")
PC_PDF_SCU_BLANK = os.path.join(FIX_DIR, "pc_pdf_scu_blank.pdf")
PC_PDF_SCU_FILLED = os.path.join(FIX_DIR, "pc_pdf_scu_reytech_filled.pdf")
RFQ_XLSX_MEDICAL = os.path.join(FIX_DIR, "rfq_xlsx_medical.xlsx")

ALL_FIXTURES = [
    CCHCS_PACKET, PC_DOCX_FOOD, PC_DOCX_NON_FOOD,
    PC_PDF_SCU_BLANK, PC_PDF_SCU_FILLED, RFQ_XLSX_MEDICAL,
]


# ─── Fixture sanity ────────────────────────────────────────────────────

class TestFixturesExist:
    def test_all_six_fixtures_present(self):
        missing = [f for f in ALL_FIXTURES if not os.path.exists(f)]
        assert not missing, f"missing fixtures: {missing}"


# ─── CCHCS packet classification ────────────────────────────────────────

class TestCCHCSPacketClassification:
    def test_cchcs_packet_shape(self):
        from src.core.request_classifier import (
            classify_request, SHAPE_CCHCS_PACKET,
        )
        r = classify_request(attachments=[CCHCS_PACKET])
        assert r.shape == SHAPE_CCHCS_PACKET

    def test_cchcs_packet_agency(self):
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[CCHCS_PACKET])
        assert r.agency == "cchcs"
        assert r.agency_name == "CCHCS / CDCR"

    def test_cchcs_packet_extracts_solicitation(self):
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[CCHCS_PACKET])
        # The fixture is PREQ10843276 — classifier should extract it
        assert r.solicitation_number in ("10843276", "PREQ10843276")

    def test_cchcs_packet_is_not_quote_only(self):
        """Full packet = full RFQ response, not just a price quote."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[CCHCS_PACKET])
        assert r.is_quote_only is False

    def test_cchcs_packet_has_confidence(self):
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[CCHCS_PACKET])
        assert r.confidence >= 0.70

    def test_cchcs_packet_no_sol_number_has_floor(self):
        """Regression: CCHCS packet + confirmed agency must score >=0.80
        even when the buyer forgot to include a solicitation number.
        Without the floor in _score_confidence this dipped to 0.70 and
        landed in manual review for our highest-volume buyer."""
        from src.core.request_classifier import (
            RequestClassification, SHAPE_CCHCS_PACKET, _score_confidence,
        )
        r = RequestClassification(
            shape=SHAPE_CCHCS_PACKET,
            agency="cchcs",
            agency_name="CCHCS / CDCR",
            solicitation_number="",  # missing — the edge case
            institution="CHCF",
        )
        assert _score_confidence(r, attachments=["packet.pdf"],
                                 agency_matches=["cchcs"]) >= 0.80

    def test_cchcs_packet_unknown_agency_does_not_get_floor(self):
        """Floor only applies when the agency is confirmed. A shape match
        alone is not enough to vault into high-confidence territory."""
        from src.core.request_classifier import (
            RequestClassification, SHAPE_CCHCS_PACKET, _score_confidence,
        )
        r = RequestClassification(
            shape=SHAPE_CCHCS_PACKET,
            agency="other",
            solicitation_number="",
        )
        assert _score_confidence(r, attachments=["packet.pdf"],
                                 agency_matches=[]) < 0.80

    def test_cchcs_packet_required_forms_from_agency(self):
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[CCHCS_PACKET])
        # CCHCS requires the 703b + 704b + bidpkg stack per agency_config
        assert "703b" in r.required_forms or "quote" in r.required_forms


# ─── DOCX AMS 704 (CCHCS variant) ───────────────────────────────────────

class TestPCDocx704Classification:
    def test_food_docx_shape(self):
        from src.core.request_classifier import (
            classify_request, SHAPE_PC_704_DOCX,
        )
        r = classify_request(attachments=[PC_DOCX_FOOD])
        assert r.shape == SHAPE_PC_704_DOCX

    def test_non_food_docx_shape(self):
        from src.core.request_classifier import (
            classify_request, SHAPE_PC_704_DOCX,
        )
        r = classify_request(attachments=[PC_DOCX_NON_FOOD])
        assert r.shape == SHAPE_PC_704_DOCX

    def test_food_docx_is_cchcs_agency(self):
        """The fixtures say 'CALIFORNIA CORRECTIONAL HEALTH CARE SERVICES'
        so agency must resolve to cchcs."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[PC_DOCX_FOOD])
        assert r.agency == "cchcs"

    def test_non_food_docx_is_cchcs_agency(self):
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[PC_DOCX_NON_FOOD])
        assert r.agency == "cchcs"

    def test_docx_is_quote_only(self):
        """AMS 704 worksheets are price-only — no compliance forms needed."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[PC_DOCX_FOOD])
        assert r.is_quote_only is True


# ─── DocuSign flat PDF 704 ──────────────────────────────────────────────

class TestPCDocuSignPDF:
    def test_blank_scu_pdf_shape(self):
        from src.core.request_classifier import (
            classify_request, SHAPE_PC_704_PDF_DOCUSIGN,
        )
        r = classify_request(attachments=[PC_PDF_SCU_BLANK])
        assert r.shape == SHAPE_PC_704_PDF_DOCUSIGN

    def test_docusign_needs_overlay_fill(self):
        """Flat DocuSign PDFs can't be form-filled — they need the
        reportlab overlay path. The classifier must flag this so
        Phase 2 dispatches to the right filler."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[PC_PDF_SCU_BLANK])
        assert r.needs_overlay_fill is True

    def test_docusign_producer_metadata_captured(self):
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[PC_PDF_SCU_BLANK])
        assert "docusign" in r.producer_signature.lower()

    def test_reytech_filled_scu_still_classifies_as_docusign_704(self):
        """The Reytech-filled copy of the same SCU PDF must classify
        the same way (shape-invariant across fill state)."""
        from src.core.request_classifier import (
            classify_request, SHAPE_PC_704_PDF_DOCUSIGN,
        )
        r = classify_request(attachments=[PC_PDF_SCU_FILLED])
        assert r.shape == SHAPE_PC_704_PDF_DOCUSIGN

    def test_scu_pdf_is_cchcs_agency(self):
        """The SCU PDF header has 'CALIFORNIA CORRECTIONAL HEALTH CARE SERVICES'."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[PC_PDF_SCU_BLANK])
        assert r.agency == "cchcs"


# ─── XLSX RFQ ───────────────────────────────────────────────────────────

class TestGenericXlsxRFQ:
    def test_medical_xlsx_shape(self):
        from src.core.request_classifier import (
            classify_request, SHAPE_GENERIC_RFQ_XLSX,
        )
        r = classify_request(attachments=[RFQ_XLSX_MEDICAL])
        assert r.shape == SHAPE_GENERIC_RFQ_XLSX

    def test_medical_xlsx_calvet_agency(self):
        """The XLSX has 'VHC-WLA' (Veterans Home of CA - West LA) so
        agency must resolve to calvet."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[RFQ_XLSX_MEDICAL])
        assert r.agency == "calvet"

    def test_xlsx_is_not_quote_only(self):
        """Generic RFQs need full packages, not just prices."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[RFQ_XLSX_MEDICAL])
        assert r.is_quote_only is False


# ─── Email-only (no attachments) ────────────────────────────────────────

class TestEmailOnlyClassification:
    def test_no_attachments_email_only_shape(self):
        from src.core.request_classifier import (
            classify_request, SHAPE_EMAIL_ONLY,
        )
        r = classify_request(
            attachments=[],
            email_body="Please quote 100 nitrile gloves. Thanks.",
            email_subject="Glove quote needed",
            email_sender="buyer@cchcs.ca.gov",
        )
        assert r.shape == SHAPE_EMAIL_ONLY

    def test_email_agency_from_sender_domain(self):
        from src.core.request_classifier import classify_request
        r = classify_request(
            attachments=[],
            email_body="Need pricing",
            email_subject="Quote",
            email_sender="buyer@cchcs.ca.gov",
        )
        assert r.agency == "cchcs"

    def test_email_calvet_from_body_reference(self):
        from src.core.request_classifier import classify_request
        r = classify_request(
            attachments=[],
            email_body="This is for VHC-WLA supply request",
            email_subject="RFQ for medical supplies",
            email_sender="buyer@state.ca.gov",
        )
        assert r.agency == "calvet"


# ─── Agency detection edge cases ────────────────────────────────────────

class TestAgencyDetectionEdges:
    def test_barstow_overrides_calvet(self):
        """Barstow is more specific than generic CalVet — must win."""
        from src.core.request_classifier import classify_request
        r = classify_request(
            attachments=[],
            email_body="Veterans Home Barstow quote request",
            email_subject="Quote",
            email_sender="buyer@calvet.ca.gov",
        )
        assert r.agency == "calvet_barstow"

    def test_cchcs_overrides_cdcr(self):
        """CCHCS is more specific than bare CDCR."""
        from src.core.request_classifier import classify_request
        r = classify_request(
            attachments=[],
            email_body="CCHCS health care services quote",
            email_subject="RFQ",
            email_sender="buyer@cdcr.ca.gov",
        )
        assert r.agency == "cchcs"

    def test_unknown_agency_falls_back_to_other(self):
        from src.core.request_classifier import classify_request
        r = classify_request(
            attachments=[],
            email_body="Generic RFQ text with no agency markers",
            email_subject="Quote",
            email_sender="someone@example.com",
        )
        assert r.agency == "other"


# ─── Confidence + reasons ───────────────────────────────────────────────

class TestConfidenceScoring:
    def test_cchcs_packet_high_confidence(self):
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[CCHCS_PACKET])
        assert r.confidence >= 0.70, \
            f"CCHCS packet should be high-confidence: {r.confidence} ({r.reasons})"

    def test_unknown_inputs_low_confidence(self):
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[])
        assert r.confidence < 0.5

    def test_reasons_are_populated(self):
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[CCHCS_PACKET])
        assert len(r.reasons) >= 1


# ─── Serialization ──────────────────────────────────────────────────────

class TestClassificationSerialization:
    def test_to_dict_round_trip(self):
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[PC_DOCX_FOOD])
        d = r.to_dict()
        assert d["shape"] == r.shape
        assert d["agency"] == r.agency
        # Must be JSON-serializable for storage on the record
        s = json.dumps(d)
        parsed = json.loads(s)
        assert parsed["shape"] == r.shape


# ─── QuoteRequest wrapper ───────────────────────────────────────────────

class TestQuoteRequestWrapper:
    def test_from_pc_reads_items_from_top_level(self):
        from src.core.quote_request import QuoteRequest
        pc = {"id": "pc_1", "items": [{"qty": 2, "description": "X"}]}
        qr = QuoteRequest.from_pc(pc)
        assert qr.kind == "pc"
        assert qr.record_id == "pc_1"
        assert len(qr.get_items()) == 1

    def test_from_pc_reads_items_from_pc_data_nested(self):
        """Legacy shape: pc.pc_data.items."""
        from src.core.quote_request import QuoteRequest
        pc = {
            "id": "pc_2",
            "pc_data": {"items": [{"qty": 3, "description": "Y"}]},
        }
        qr = QuoteRequest.from_pc(pc)
        assert len(qr.get_items()) == 1
        assert qr.get_items()[0]["qty"] == 3

    def test_from_pc_reads_items_from_pc_data_json_string(self):
        """Even older shape: pc.pc_data is a JSON string."""
        from src.core.quote_request import QuoteRequest
        pc = {
            "id": "pc_3",
            "pc_data": json.dumps({"items": [{"qty": 5, "description": "Z"}]}),
        }
        qr = QuoteRequest.from_pc(pc)
        items = qr.get_items()
        assert len(items) == 1
        assert items[0]["qty"] == 5

    def test_from_rfq_reads_line_items(self):
        from src.core.quote_request import QuoteRequest
        rfq = {
            "id": "rfq_1",
            "line_items": [{"qty": 1, "description": "A"}],
        }
        qr = QuoteRequest.from_rfq(rfq)
        assert qr.kind == "rfq"
        assert len(qr.get_items()) == 1

    def test_empty_record_returns_empty_items_safely(self):
        from src.core.quote_request import QuoteRequest
        qr = QuoteRequest.from_pc({})
        assert qr.get_items() == []

    def test_get_agency_from_classification(self):
        from src.core.quote_request import QuoteRequest
        rfq = {
            "id": "rfq_1",
            "_classification": {"agency": "calvet", "required_forms": ["std204"]},
        }
        qr = QuoteRequest.from_rfq(rfq)
        assert qr.get_agency() == "calvet"
        assert qr.get_required_forms() == ["std204"]

    def test_get_agency_fallback_to_legacy_field(self):
        from src.core.quote_request import QuoteRequest
        pc = {"id": "pc_1", "agency": "DSH"}
        qr = QuoteRequest.from_pc(pc)
        assert qr.get_agency() == "dsh"

    def test_get_solicitation_from_classification(self):
        from src.core.quote_request import QuoteRequest
        rfq = {"_classification": {"solicitation_number": "10843276"}}
        qr = QuoteRequest.from_rfq(rfq)
        assert qr.get_solicitation() == "10843276"

    def test_get_solicitation_legacy_fields(self):
        from src.core.quote_request import QuoteRequest
        qr_pc = QuoteRequest.from_pc({"pc_number": "PC-123"})
        assert qr_pc.get_solicitation() == "PC-123"
        qr_rfq = QuoteRequest.from_rfq({"solicitation_number": "SOL-456"})
        assert qr_rfq.get_solicitation() == "SOL-456"

    def test_is_quote_only_from_classification(self):
        from src.core.quote_request import QuoteRequest
        pc = {"_classification": {"is_quote_only": True}}
        qr = QuoteRequest.from_pc(pc)
        assert qr.is_quote_only() is True

    def test_is_quote_only_inferred_from_kind(self):
        """Without a classification, PCs default to quote_only and
        RFQs default to full package."""
        from src.core.quote_request import QuoteRequest
        assert QuoteRequest.from_pc({}).is_quote_only() is True
        assert QuoteRequest.from_rfq({}).is_quote_only() is False

    def test_summary_string(self):
        from src.core.quote_request import QuoteRequest
        rfq = {
            "id": "rfq_deadbeef",
            "solicitation_number": "10843276",
            "agency": "cchcs",
            "institution": "CA State Prison Sacramento",
            "line_items": [{"qty": 1}, {"qty": 2}],
        }
        s = QuoteRequest.from_rfq(rfq).summary()
        assert "10843276" in s
        assert "cchcs" in s
        assert "items=2" in s


# ─── End-to-end: classify + wrap in QuoteRequest ───────────────────────

class TestClassifyAndWrap:
    def test_cchcs_packet_full_pipeline(self):
        """The real user flow: operator uploads a file, classifier
        runs, result is stored on the record, QuoteRequest wraps it.
        Downstream code reads through the wrapper only."""
        from src.core.request_classifier import classify_request
        from src.core.quote_request import QuoteRequest

        classification = classify_request(attachments=[CCHCS_PACKET])

        # Simulate storing on a PC record (what Phase 2 will do)
        pc = {
            "id": "pc_new_123",
            "_classification": classification.to_dict(),
            "items": [],  # would be populated from the parse in Phase 2
        }
        qr = QuoteRequest.from_pc(pc)

        assert qr.get_agency() == "cchcs"
        assert qr.get_shape() == classification.shape
        assert qr.get_solicitation() in ("10843276", "PREQ10843276")
        assert qr.is_quote_only() is False  # full packet
