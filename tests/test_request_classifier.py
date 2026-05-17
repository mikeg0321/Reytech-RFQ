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


# ─── is_quote_only: PC-shape + RFQ companion → RFQ context ─────────────
#
# Mike 2026-05-17 SAC walkthrough: SAC came in with a fillable 704B
# alongside a 703B AMS form AND a bid package PDF — clearly an RFQ.
# Pre-fix classifier set is_quote_only=True because the 704B was the
# primary shape, ignoring the corroborating RFQ-only attachments. The
# downstream ID prefix landed as pc_ instead of rfq_ (cosmetic), and
# the future PC↔RFQ auto-link substrate would have skipped this as a
# PC. Closes that class.

class TestIsQuoteOnlyWithRfqCompanion:
    """When a PC-shaped 704 ships alongside 703B/703C/bidpkg attachments,
    it's an RFQ — the buyer wants the full package response."""

    def test_pc_docx_alone_is_quote_only_true(self):
        """Baseline: 704 worksheet alone = PC."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[PC_DOCX_FOOD])
        assert r.is_quote_only is True

    def test_pc_with_703b_companion_is_quote_only_false(self):
        """SAC pattern: AMS 703B alongside the 704 = full RFQ context."""
        from src.core.request_classifier import classify_request
        # Second path is filename-only; the classifier's filename scan
        # picks it up even though the file doesn't exist (the per-PDF
        # _classify_pdf call is what needs a real file; the companion
        # detection is filename-only by design).
        r = classify_request(attachments=[
            PC_DOCX_FOOD,
            "/tmp/AMS 703B - RFQ - Informal Competitive - RFQ 10847457.pdf",
        ])
        assert r.is_quote_only is False
        assert any("is_quote_only=False" in reason for reason in r.reasons)

    def test_pc_with_703c_companion_is_quote_only_false(self):
        """703C alternate (Fair-and-Reasonable pathway) → also RFQ."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[
            PC_DOCX_FOOD,
            "/tmp/AMS 703C - Sole Source - RFQ 99999.pdf",
        ])
        assert r.is_quote_only is False

    def test_pc_with_bidpkg_companion_is_quote_only_false(self):
        """SAC pattern: BID PACKAGE & FORMS PDF + 704 = RFQ."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[
            PC_DOCX_FOOD,
            "/tmp/BID PACKAGE & FORMS (Under 100k) - Attachment 3.pdf",
        ])
        assert r.is_quote_only is False

    def test_pc_with_lowercase_bidpkg_companion_is_quote_only_false(self):
        """Filename heuristic is case-insensitive."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[
            PC_DOCX_FOOD,
            "/tmp/some_bidpkg_attachment.pdf",
        ])
        assert r.is_quote_only is False

    def test_pc_with_704b_only_does_not_false_match_as_703(self):
        """The substring '703' in the filename triggers — but '704' must
        not. Critical: 'AMS 704B' filename alone is still PC."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[
            PC_DOCX_FOOD,
            "/tmp/AMS 704B - CCHCS Acquisition Quote Worksheet.pdf",
        ])
        # PC + another 704 sibling = still PC. No 703 anywhere.
        assert r.is_quote_only is True

    def test_pc_with_irrelevant_pdf_companion_is_quote_only_true(self):
        """Companion attachments that aren't 703/bidpkg don't flip the flag."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[
            PC_DOCX_FOOD,
            "/tmp/email_signature_logo.jpg",
            "/tmp/random_invoice_2024.pdf",
        ])
        assert r.is_quote_only is True

    def test_pc_with_ams_703_no_letter_suffix_is_quote_only_false(self):
        """Some buyers attach 'AMS 703' without B/C suffix — count it."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[
            PC_DOCX_FOOD,
            "/tmp/AMS 703 generic informal.pdf",
        ])
        assert r.is_quote_only is False

    def test_pc_with_non_it_rfq_packet_companion_is_quote_only_false(self):
        """CCHCS sometimes labels the bid package as 'Non-IT RFQ Packet'."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[
            PC_DOCX_FOOD,
            "/tmp/CCHCS Non-IT RFQ Packet 2024.pdf",
        ])
        assert r.is_quote_only is False


# ─── Multi-attachment pricing-page tiebreak (DSH-style bundles) ────────
#
# DSH (and similar) split a single RFQ across multiple PDFs:
#   - Cover letter (DSH 2010 with provisions/instructions)
#   - Attachment A — Bidder info & certifications
#   - Attachment B — Goods & services pricing (THE LINE ITEMS)
#   - Attachment C — Required forms list
#
# All four classify as SHAPE_GENERIC_RFQ_PDF. The classifier must pick
# Attachment B as `primary_file` so Vision parses the items, not the
# cover sheet (which has no item table → Vision returns 0 items).

def _make_pdf_with_text(path: str, text: str) -> None:
    """Synthesize a tiny single-page PDF containing the given text."""
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter

    c = canvas.Canvas(path, pagesize=letter)
    width, height = letter
    y = height - 60
    for line in text.splitlines():
        c.drawString(50, y, line[:90])
        y -= 14
        if y < 60:
            c.showPage()
            y = height - 60
    c.save()


class TestPricingPageTiebreak:
    def test_dsh_bundle_picks_attachment_b(self, tmp_path):
        """Classifier must prefer the pricing page over cover/cert/forms
        when all 4 PDFs in a buyer's RFQ bundle classify as generic."""
        from src.core.request_classifier import (
            classify_request, SHAPE_GENERIC_RFQ_PDF,
        )

        cover = tmp_path / "RFQ_25CB021.pdf"
        attA = tmp_path / "RFQ__25CB021.pdf"
        attB = tmp_path / "RFQ___25CB021.pdf"
        attC = tmp_path / "RFQ____25CB021.pdf"

        _make_pdf_with_text(str(cover), (
            "DEPARTMENT OF STATE HOSPITALS - ATASCADERO\n"
            "Solicitation Number: 25CB021\n"
            "Bidder Instructions\n"
            "General Provisions: DGS.ca.gov\n"
            "GENAI Disclosure Factsheet\n"
        ))
        _make_pdf_with_text(str(attA), (
            "ATTACHMENT A - BIDDER'S INFORMATION AND CERTIFICATIONS\n"
            "Solicitation Number: 25CB021\n"
            "Firm's Legal Name:\n"
            "BIDDER'S CERTIFICATIONS\n"
        ))
        _make_pdf_with_text(str(attB), (
            "ATTACHMENT B - GOODS AND SERVICES PRICING PAGE 1\n"
            "Solicitation #: 25CB021\n"
            "# DESCRIPTION OF GOODS / SERVICES QTY UOM UNIT PRICE EXTENSION\n"
            "1 POWER SUPPLY ADAM SCALE 12VDC 2 EACH\n"
            "2 PHILIPS NORELCO BLADES QP210/80 1500 EACH\n"
        ))
        _make_pdf_with_text(str(attC), (
            "ATTACHMENT C - REQUIRED FORMS\n"
            "Solicitation #: 25CB021\n"
            "Darfur Contracting Act Certification\n"
            "DVBE Declaration\n"
            "Bidder Declaration\n"
        ))

        # Mix the order so we don't accidentally win by alphabetical/insertion
        r = classify_request(attachments=[
            str(cover), str(attA), str(attC), str(attB),
        ])
        assert r.shape == SHAPE_GENERIC_RFQ_PDF
        assert r.primary_file == "RFQ___25CB021.pdf", (
            f"expected pricing page (attB) to win as primary, got "
            f"{r.primary_file!r}. reasons: {r.reasons}"
        )

    def test_pricing_score_scores_known_markers(self):
        """`_pricing_page_score` must non-zero-score known pricing-page
        text and return 0 for cover-letter text."""
        from src.core.request_classifier import _pricing_page_score

        cover_text = (
            "DEPARTMENT OF STATE HOSPITALS - ATASCADERO "
            "Solicitation Number: 25CB021 Bidder Instructions"
        )
        pricing_text = (
            "ATTACHMENT B - GOODS AND SERVICES PRICING PAGE 1 "
            "DESCRIPTION OF GOODS / SERVICES QTY UOM UNIT PRICE EXTENSION"
        )
        assert _pricing_page_score(cover_text) == 0
        assert _pricing_page_score(pricing_text) > 0
        # Pricing page should outscore cover by a wide margin
        assert _pricing_page_score(pricing_text) >= 4


# ─── Solicitation-number extractor (labeled patterns) ──────────────────
#
# PR substrate 2026-05-12: the pre-fix `_extract_solicitation` fell
# through to the 8-digit bare-numeric fallback for unlabeled corpora,
# which silently grabbed Adam Equipment part number `2010017786` as
# the DSH 25CB021 sol#. Labeled patterns now scan first.

class TestExtractSolicitation:
    def test_labeled_solicitation_number_wins_over_bare_numeric(self):
        from src.core.request_classifier import _extract_solicitation
        # The bare numeric would match 12345678 (8 digits) if it were
        # the only signal, but the labeled `Solicitation Number: X`
        # should win.
        s = _extract_solicitation(
            "Adam PN 12345678 — Solicitation Number: 25CB021 some text",
            [],
        )
        assert s == "25CB021"

    def test_subject_line_quote_request_pattern(self):
        """Catches Mike's real DSH subject:
        `FW: Please find attached quote request 25CB021`."""
        from src.core.request_classifier import _extract_solicitation
        assert _extract_solicitation(
            "FW: Please find attached quote request 25CB021", [],
        ) == "25CB021"

    def test_request_for_quotation_pattern(self):
        from src.core.request_classifier import _extract_solicitation
        assert _extract_solicitation(
            "Request for quotation 25CB021", [],
        ) == "25CB021"
        # And with a colon separator
        assert _extract_solicitation(
            "Request for Quote: ABC123", [],
        ) == "ABC123"

    def test_sol_hash_alphanumeric(self):
        from src.core.request_classifier import _extract_solicitation
        assert _extract_solicitation(
            "Sol# CALVET-2026-001", [],
        ) == "CALVET-2026-001"

    def test_preq_prefix_still_works(self):
        from src.core.request_classifier import _extract_solicitation
        assert _extract_solicitation(
            "Re: CCHCS PREQ10843276 attached", [],
        ) == "10843276"

    def test_eight_digit_bare_fallback(self):
        """Bare 8-digit numeric is the LAST fallback. Real CCHCS sol#s
        match this shape (e.g. 10843276)."""
        from src.core.request_classifier import _extract_solicitation
        assert _extract_solicitation(
            "Some text 10843276 attached", [],
        ) == "10843276"

    def test_ten_digit_bare_does_not_match(self):
        """10-digit bare numerics (Adam Equipment PN style) must NOT
        match the 8-digit fallback. Mike's DSH test showed
        `2010017786` was being mis-extracted as the sol# pre-fix."""
        from src.core.request_classifier import _extract_solicitation
        assert _extract_solicitation(
            "Adam Equipment Part Number 2010017786", [],
        ) == ""

    def test_does_not_capture_icitation_from_solicitation(self):
        """Regression for CI failure 2026-05-12: the `\\bSol\\s*[:#]?…`
        pattern matched `Sol` *inside* the word `SOLICITATION` and
        captured `ICITATION` as the sol#. The CCHCS packet fixture's
        page-1 caption is `SOLICITATION NUMBER: 10843276` — must
        extract `10843276`, never `ICITATION`."""
        from src.core.request_classifier import _extract_solicitation
        # Note: with the labeled `Solicitation Number:` pattern firing
        # first, the bare "SOLICITATION" without `#`/`Number:` should
        # NOT match anything in isolation.
        assert _extract_solicitation("SOLICITATION", []) == ""
        # With Number: present, the labeled pattern fires correctly.
        assert _extract_solicitation(
            "SOLICITATION NUMBER: 10843276", [],
        ) == "10843276"

    # PR-AB substrate 2026-05-13: placeholder-shaped captures
    # rfq_e02b7fa6 forensic: the buyer-issued PDF body contained
    # "Solicitation Number: PAYMENT" (or similar OCR'd label) which
    # the prior extractor accepted because [A-Z0-9][A-Z0-9-]{2,19}
    # has no digit requirement. Real sol#s have digits; pure-word
    # captures (PAYMENT / QUOTE / ATTACHED / …) must reject and fall
    # through so the synthesizer in ingest_pipeline can fire.
    def test_rejects_placeholder_word_payment(self):
        from src.core.request_classifier import _extract_solicitation
        # "Solicitation Number: PAYMENT" — placeholder must reject
        assert _extract_solicitation(
            "Solicitation Number: PAYMENT", []
        ) == ""

    def test_rejects_placeholder_word_quote(self):
        from src.core.request_classifier import _extract_solicitation
        assert _extract_solicitation(
            "Solicitation #: QUOTE", []
        ) == ""

    def test_rejects_placeholder_word_attached(self):
        from src.core.request_classifier import _extract_solicitation
        # "RFQ Number: ATTACHED" — must reject
        assert _extract_solicitation(
            "RFQ Number: ATTACHED", []
        ) == ""

    def test_falls_through_to_next_pattern_when_first_match_is_placeholder(self):
        """When pattern-1 captures a placeholder, the extractor must
        keep scanning later patterns / bare-numeric. Real-world: a
        body with `Solicitation Number: PAYMENT` further down + a
        valid `25CB021` earlier — DSH pattern should win."""
        from src.core.request_classifier import _extract_solicitation
        corpus = "Please quote 25CB021. Solicitation Number: PAYMENT"
        result = _extract_solicitation(corpus, [])
        # Either the labeled placeholder is rejected and DSH pattern
        # picks up `25CB021`, OR bare-8-digit fallback kicks in. Both
        # are acceptable; PAYMENT is not.
        assert result != "PAYMENT"
        assert result != ""

    def test_falls_through_to_bare_numeric_when_label_is_placeholder(self):
        """`Solicitation Number: PAYMENT … 10843276` — bare 8-digit
        fallback catches the CCHCS sol# after the label is rejected."""
        from src.core.request_classifier import _extract_solicitation
        corpus = "Solicitation Number: PAYMENT. PC# 10843276 attached."
        assert _extract_solicitation(corpus, []) == "10843276"

    def test_accepts_real_alphanumeric_solicitation(self):
        """Make sure the placeholder filter doesn't eat real sol#s
        that happen to have letters. `25CB021` (DSH) must pass."""
        from src.core.request_classifier import _extract_solicitation
        assert _extract_solicitation(
            "Solicitation Number: 25CB021", []
        ) == "25CB021"

    def test_is_sol_placeholder_capture_predicate(self):
        """Direct unit test of the placeholder predicate."""
        from src.core.request_classifier import _is_sol_placeholder_capture
        # placeholder words
        assert _is_sol_placeholder_capture("PAYMENT") is True
        assert _is_sol_placeholder_capture("QUOTE") is True
        assert _is_sol_placeholder_capture("ATTACHED") is True
        assert _is_sol_placeholder_capture("RFQ") is True
        # any pure-alpha all-caps word
        assert _is_sol_placeholder_capture("HELLO") is True
        # real sol#s pass
        assert _is_sol_placeholder_capture("25CB021") is False
        assert _is_sol_placeholder_capture("10843276") is False
        assert _is_sol_placeholder_capture("PREQ12345") is False
        # empty rejects
        assert _is_sol_placeholder_capture("") is True
        assert _is_sol_placeholder_capture("   ") is True


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

    def test_write_items_pc_sets_top_level_items(self):
        from src.core.quote_request import QuoteRequest
        pc = {"id": "pc_w1", "items": [{"qty": 1, "description": "old"}]}
        qr = QuoteRequest.from_pc(pc)
        qr.write_items([{"qty": 5, "description": "new"}])
        assert pc["items"][0]["qty"] == 5
        assert qr.get_items()[0]["qty"] == 5

    def test_write_items_pc_syncs_nested_pc_data(self):
        """When pc_data.items exists as a legacy mirror, write_items
        must keep it in sync so a reader that falls through to the
        nested path doesn't return stale data."""
        from src.core.quote_request import QuoteRequest
        pc = {
            "id": "pc_w2",
            "pc_data": {"items": [{"qty": 1, "description": "stale"}]},
        }
        qr = QuoteRequest.from_pc(pc)
        qr.write_items([{"qty": 9, "description": "fresh"}])
        assert pc["items"][0]["qty"] == 9
        assert pc["pc_data"]["items"][0]["qty"] == 9

    def test_write_items_pc_reserializes_pc_data_json_string(self):
        from src.core.quote_request import QuoteRequest
        pc = {
            "id": "pc_w3",
            "pc_data": json.dumps({"items": [{"qty": 2, "description": "a"}], "extra": "keep"}),
        }
        qr = QuoteRequest.from_pc(pc)
        qr.write_items([{"qty": 7, "description": "b"}])
        assert pc["items"][0]["qty"] == 7
        reparsed = json.loads(pc["pc_data"])
        assert reparsed["items"][0]["qty"] == 7
        assert reparsed["extra"] == "keep"  # preserved

    def test_write_items_drops_stale_data_json(self):
        """data_json is a whole-record snapshot and can't be patched
        in place — drop it so _save_single_pc re-emits a fresh copy."""
        from src.core.quote_request import QuoteRequest
        pc = {
            "id": "pc_w4",
            "items": [],
            "data_json": json.dumps({"items": [{"qty": 99}], "anything": 1}),
        }
        qr = QuoteRequest.from_pc(pc)
        qr.write_items([{"qty": 3}])
        assert "data_json" not in pc
        assert pc["items"][0]["qty"] == 3

    def test_write_items_rfq_sets_line_items(self):
        from src.core.quote_request import QuoteRequest
        rfq = {"id": "rfq_w1", "line_items": [{"qty": 1}]}
        qr = QuoteRequest.from_rfq(rfq)
        qr.write_items([{"qty": 4}, {"qty": 5}])
        assert len(rfq["line_items"]) == 2
        assert qr.get_items()[0]["qty"] == 4

    def test_write_items_rejects_non_list(self):
        """Defensive — never crash, but also never coerce silently."""
        from src.core.quote_request import QuoteRequest
        pc = {"id": "pc_w5", "items": [{"qty": 1}]}
        qr = QuoteRequest.from_pc(pc)
        qr.write_items("not a list")  # type: ignore[arg-type]
        # Original items preserved
        assert pc["items"][0]["qty"] == 1

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
