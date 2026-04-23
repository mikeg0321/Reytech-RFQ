"""Tests for src/core/ingest_pipeline.process_buyer_request.

Phase 2 of the PC↔RFQ unification. These tests are the regression
guards that make RFQ 6655f190 (the specific bug that triggered the
refactor) impossible to re-introduce.

Key assertions:
  1. Each fixture type flows to the right parser and produces items
  2. Record type is correct (pc for quote-only, rfq for full package)
  3. The classification is stored on the record for downstream use
  4. Triangulated linker requires >=2 anchors (agency + sol + items)
  5. Wrong PC link bug from 6655f190: can NOT happen with single-anchor
"""
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
RFQ_XLSX_MEDICAL = os.path.join(FIX_DIR, "rfq_xlsx_medical.xlsx")


# ─── End-to-end ingest: each fixture → correct record ─────────────────

class TestIngestEndToEnd:

    def test_cchcs_packet_creates_rfq_with_classification(self, temp_data_dir):
        """CCHCS packet = full RFQ response needed → creates RFQ record
        with classification stored, not a bare PC.
        Fixture is the 18-page, 183-field CCHCS packet (NOT an LPA — LPAs
        are 13 pages per 2026-04-22 RFQ 10840486). Classifies as cchcs_packet."""
        from src.core.ingest_pipeline import process_buyer_request

        result = process_buyer_request(
            files=[CCHCS_PACKET],
            email_subject="PREQ10843276 Quote Request",
            email_sender="ashley.russ@cdcr.ca.gov",
        )
        assert result.ok is True
        assert result.record_type == "rfq"
        assert result.record_id
        assert result.classification is not None
        assert result.classification["shape"] == "cchcs_packet"
        assert result.classification["agency"] == "cchcs"
        from src.api.dashboard import load_rfqs
        rfq = load_rfqs().get(result.record_id)
        assert rfq is not None
        assert rfq.get("_classification", {}).get("shape") == "cchcs_packet"

    def test_docx_704_food_creates_pc_record(self, temp_data_dir):
        """DOCX AMS 704 = quote-only → creates PC record."""
        from src.core.ingest_pipeline import process_buyer_request

        result = process_buyer_request(
            files=[PC_DOCX_FOOD],
            email_subject="AMS 704 Price Check - Food",
            email_sender="carolyn.montgomery@cdcr.ca.gov",
        )
        assert result.ok is True
        assert result.record_type == "pc"
        assert result.classification["shape"] == "pc_704_docx"
        assert result.classification["is_quote_only"] is True

    def test_xlsx_rfq_creates_rfq_with_calvet_agency(self, temp_data_dir):
        """XLSX with VHC-WLA header → CalVet RFQ."""
        from src.core.ingest_pipeline import process_buyer_request

        result = process_buyer_request(
            files=[RFQ_XLSX_MEDICAL],
            email_subject="Medical Supplies RFQ",
            email_sender="buyer@calvet.ca.gov",
        )
        assert result.ok is True
        assert result.record_type == "rfq"
        assert result.classification["agency"] == "calvet"
        assert result.classification["shape"] == "generic_rfq_xlsx"

    def test_docusign_pdf_creates_pc_record_with_overlay_flag(self, temp_data_dir):
        """DocuSign flat PDF → PC record with needs_overlay_fill=True
        so Phase 3 dispatcher knows to use the overlay filler."""
        from src.core.ingest_pipeline import process_buyer_request

        result = process_buyer_request(
            files=[PC_PDF_SCU_BLANK],
            email_subject="SCU Group Tx Materials",
            email_sender="buyer@cdcr.ca.gov",
        )
        assert result.ok is True
        assert result.record_type == "pc"
        assert result.classification["needs_overlay_fill"] is True


# ─── Triangulated linker ───────────────────────────────────────────────

class TestTriangulatedLinker:
    """The linker must require at least 2 anchors (agency/sol/items)
    before linking an RFQ to a PC. Single-anchor matches are rejected
    to prevent the 6655f190 "wrong PC linked" bug."""

    def _seed_pcs(self, temp_data_dir, sample_pc):
        """Seed 3 PCs with different agency/sol combos to test linker
        tiebreaking."""
        from src.api.dashboard import _save_single_pc
        pcs = []
        # PC A: cchcs, sol 10843276, matching items
        pc_a = dict(sample_pc)
        pc_a["id"] = "pc_a_cchcs_preq"
        pc_a["solicitation_number"] = "10843276"
        pc_a["agency"] = "cchcs"
        pc_a["institution"] = "ca state prison sacramento"
        pc_a["items"] = [{
            "description": "Handheld Scanner w/ USB cable and standard cradle",
            "qty": 15, "mfg_number": "DS8178",
        }]
        _save_single_pc(pc_a["id"], pc_a)
        pcs.append(pc_a["id"])

        # PC B: different agency (calvet), same solicitation (collision)
        pc_b = dict(sample_pc)
        pc_b["id"] = "pc_b_calvet_colliding_sol"
        pc_b["solicitation_number"] = "10843276"
        pc_b["agency"] = "calvet"
        pc_b["institution"] = "vhc-wla"
        pc_b["items"] = [{"description": "Completely different item", "qty": 1}]
        _save_single_pc(pc_b["id"], pc_b)
        pcs.append(pc_b["id"])

        # PC C: same agency, no solicitation, overlapping items
        pc_c = dict(sample_pc)
        pc_c["id"] = "pc_c_cchcs_items_only"
        pc_c["solicitation_number"] = ""
        pc_c["agency"] = "cchcs"
        pc_c["institution"] = "ca state prison sacramento"
        pc_c["items"] = [{
            "description": "Handheld Scanner w/ USB cable and standard cradle",
            "qty": 15, "mfg_number": "DS8178",
        }]
        _save_single_pc(pc_c["id"], pc_c)
        pcs.append(pc_c["id"])
        return pcs

    def test_triangulated_linker_picks_best_anchor_count(
        self, temp_data_dir, sample_pc
    ):
        """PC A has agency+sol+items (3 anchors); PC B has only sol
        (1 anchor, insufficient); PC C has agency+items+institution
        (3 anchors). Linker must pick A or C (both score 3), tie-break
        by most recent created_at. B must NEVER win."""
        self._seed_pcs(temp_data_dir, sample_pc)

        from src.core.ingest_pipeline import process_buyer_request
        result = process_buyer_request(
            files=[CCHCS_PACKET],
            email_subject="PREQ10843276 Quote Request",
            email_sender="ashley.russ@cdcr.ca.gov",
        )
        assert result.ok is True
        assert result.record_type == "rfq"
        # Must link to a PC that has >=2 anchors — not the sol-only PC
        assert result.linked_pc_id in ("pc_a_cchcs_preq", "pc_c_cchcs_items_only")
        assert result.linked_pc_id != "pc_b_calvet_colliding_sol"
        assert result.link_confidence >= 0.5

    def test_sol_number_wins_over_higher_anchor_count(
        self, temp_data_dir, sample_pc
    ):
        """Regression: a re-sent RFQ with a solicitation number must link
        to the PC that shares that solicitation, NOT to the most-recent
        PC from the same agency that happens to outscore it on anchor
        count. Before the fix the tie-break was (-score, -created_at)
        which routed re-sends to whichever PC was created most recently."""
        from src.api.dashboard import _save_single_pc
        from src.core.ingest_pipeline import _run_triangulated_linker
        from src.core.request_classifier import RequestClassification, SHAPE_CCHCS_PACKET

        # PC OLD: matches on agency + solicitation (2 anchors, the
        # actually-correct link even though created weeks ago).
        pc_old = dict(sample_pc)
        pc_old["id"] = "pc_old_correct"
        pc_old["solicitation_number"] = "PREQ99999"
        pc_old["agency"] = "cchcs"
        pc_old["institution"] = "some other prison"
        pc_old["items"] = [{"description": "Zebra Handheld Scanner DS8178", "qty": 5}]
        pc_old["created_at"] = "2026-03-01T10:00:00"
        _save_single_pc(pc_old["id"], pc_old)

        # PC NEW: matches on agency + institution + items (3 anchors, no
        # solicitation). Created more recently so the old tie-break
        # would have preferred it.
        pc_new = dict(sample_pc)
        pc_new["id"] = "pc_new_wrong"
        pc_new["solicitation_number"] = ""
        pc_new["agency"] = "cchcs"
        pc_new["institution"] = "ca state prison sacramento"
        pc_new["items"] = [{"description": "Zebra Handheld Scanner DS8178", "qty": 5}]
        pc_new["created_at"] = "2026-04-13T10:00:00"
        _save_single_pc(pc_new["id"], pc_new)

        classification = RequestClassification(
            shape=SHAPE_CCHCS_PACKET,
            agency="cchcs",
            institution="ca state prison sacramento",
            solicitation_number="PREQ99999",
        )
        rfq_items = [{"description": "Zebra Handheld Scanner DS8178", "qty": 5}]

        linked_pc_id, reason, confidence = _run_triangulated_linker(
            "rfq_test_resend", classification, rfq_items
        )
        assert linked_pc_id == "pc_old_correct", (
            f"expected sol-match to win, got {linked_pc_id} ({reason})"
        )
        assert "solicitation" in reason

    def test_linker_refuses_single_anchor_match(
        self, temp_data_dir, sample_pc
    ):
        """Only PC with exactly 1 anchor in common (solicitation only)
        must NOT link — no triangulation means no link."""
        from src.api.dashboard import _save_single_pc
        pc = dict(sample_pc)
        pc["id"] = "pc_sol_only"
        pc["solicitation_number"] = "10843276"
        pc["agency"] = "dgs"  # different agency
        pc["institution"] = "sacramento dgs office"  # different institution
        pc["items"] = [{"description": "Pencils", "qty": 100}]  # different items
        _save_single_pc(pc["id"], pc)

        from src.core.ingest_pipeline import process_buyer_request
        result = process_buyer_request(
            files=[CCHCS_PACKET],
            email_subject="PREQ10843276",
            email_sender="ashley.russ@cdcr.ca.gov",
        )
        assert result.linked_pc_id == "" or result.linked_pc_id != "pc_sol_only"

    def test_linker_no_matches_returns_empty_cleanly(
        self, temp_data_dir, sample_pc
    ):
        """No matching PCs at all → empty linked_pc_id, no crash."""
        # No PCs seeded
        from src.core.ingest_pipeline import process_buyer_request
        result = process_buyer_request(
            files=[CCHCS_PACKET],
            email_subject="PREQ10843276",
            email_sender="ashley.russ@cdcr.ca.gov",
        )
        assert result.ok is True
        assert result.linked_pc_id == ""


# ─── Strong item-match override (RFQ #10840486 regression, 2026-04-22) ─────
# A verbatim-identical 1-item RFQ must link to its prior 1-item PC even when
# the record-level anchors (agency/sol/institution) all fail. The prior PC
# was an informal price check with no agency classified — item identity is
# the only thing that ties them together, and that's strong enough.

class TestStrongItemMatchOverride:
    def test_verbatim_1item_links_across_mismatched_agency(
        self, temp_data_dir, sample_pc
    ):
        """RFQ #10840486 reproduction.

        RFQ: agency=cchcs, sol=10840486, institution="", 1 item "BLS …"
        PC:  agency="",    sol="",        institution="CSP-Sacramento",
             1 verbatim item "BLS …"

        Under the old 2-anchor rule this returned no link — only
        item-similarity fired. Strong verbatim match now promotes it.
        """
        from src.api.dashboard import _save_single_pc
        from src.core.ingest_pipeline import _run_triangulated_linker
        from src.core.request_classifier import RequestClassification, SHAPE_CCHCS_PACKET

        pc = dict(sample_pc)
        pc["id"] = "pc_bls_informal"
        pc["solicitation_number"] = ""
        pc["agency"] = ""
        pc["institution"] = "csp-sacramento"
        pc["items"] = [{"description": "BLS Provider Course Videos: USB", "qty": 2}]
        _save_single_pc(pc["id"], pc)

        classification = RequestClassification(
            shape=SHAPE_CCHCS_PACKET,
            agency="cchcs",
            institution="",
            solicitation_number="10840486",
        )
        rfq_items = [{"description": "BLS Provider Course Videos: USB", "qty": 2}]

        linked_pc_id, reason, confidence = _run_triangulated_linker(
            "rfq_test_bls", classification, rfq_items,
        )
        assert linked_pc_id == "pc_bls_informal", (
            f"verbatim item match must link; got {linked_pc_id!r} ({reason})"
        )
        assert "items" in reason  # either items(...) anchor or items-verbatim(...)

    def test_agency_fallback_via_institution_resolver(
        self, temp_data_dir, sample_pc
    ):
        """Legacy PC with blank agency but set institution must still land
        on the right agency via institution_resolver. Then agency + items
        = 2 anchors, link stands via the normal (non-strong-item) path."""
        from src.api.dashboard import _save_single_pc
        from src.core.ingest_pipeline import _run_triangulated_linker
        from src.core.request_classifier import RequestClassification, SHAPE_CCHCS_PACKET

        pc = dict(sample_pc)
        pc["id"] = "pc_legacy_no_agency"
        pc["solicitation_number"] = ""
        pc["agency"] = ""  # legacy PC — classifier hadn't run yet
        pc["institution"] = "csp-sacramento"  # resolves to cchcs
        # Item similar but not verbatim (sim ~0.76) — forces the 2-anchor
        # path, so the agency fallback must actually provide the 2nd anchor.
        pc["items"] = [{
            "description": "BLS Provider Course Videos USB flash drive",
            "qty": 2,
        }]
        _save_single_pc(pc["id"], pc)

        classification = RequestClassification(
            shape=SHAPE_CCHCS_PACKET,
            agency="cchcs",  # CSP-Sacramento resolves to cchcs
            institution="",  # RFQ institution blank (like the real RFQ #10840486)
            solicitation_number="",  # no sol anchor
        )
        rfq_items = [{"description": "BLS Provider Course Videos: USB", "qty": 2}]

        linked_pc_id, reason, _ = _run_triangulated_linker(
            "rfq_legacy", classification, rfq_items,
        )
        assert linked_pc_id == "pc_legacy_no_agency", (
            f"agency fallback failed; got {linked_pc_id!r} ({reason})"
        )
        assert "agency" in reason  # proves the fallback actually fired

    def test_strong_item_requires_count_symmetry(
        self, temp_data_dir, sample_pc
    ):
        """Safety: a 1-item RFQ must NOT link to a 10-item PC that happens
        to contain that item. The big PC was quoting a different batch that
        just includes this item — not the prior price-check for this RFQ."""
        from src.api.dashboard import _save_single_pc
        from src.core.ingest_pipeline import _run_triangulated_linker
        from src.core.request_classifier import RequestClassification, SHAPE_CCHCS_PACKET

        big_pc = dict(sample_pc)
        big_pc["id"] = "pc_big_unrelated"
        big_pc["solicitation_number"] = ""
        big_pc["agency"] = ""
        big_pc["institution"] = "unrelated"
        big_pc["items"] = (
            [{"description": "BLS Provider Course Videos: USB", "qty": 1}]
            + [{"description": f"Unrelated item {i}", "qty": 1} for i in range(9)]
        )
        _save_single_pc(big_pc["id"], big_pc)

        classification = RequestClassification(
            shape=SHAPE_CCHCS_PACKET,
            agency="cchcs",
            institution="",
            solicitation_number="99999",
        )
        rfq_items = [{"description": "BLS Provider Course Videos: USB", "qty": 2}]

        linked_pc_id, reason, _ = _run_triangulated_linker(
            "rfq_solo_item", classification, rfq_items,
        )
        assert linked_pc_id == "", (
            f"1-item RFQ must not link to 10-item unrelated PC; got {linked_pc_id!r} ({reason})"
        )

    def test_strong_item_rejects_low_similarity(
        self, temp_data_dir, sample_pc
    ):
        """Coverage might be 100% but if mean sim is <0.90, no strong match."""
        from src.api.dashboard import _save_single_pc
        from src.core.ingest_pipeline import _run_triangulated_linker
        from src.core.request_classifier import RequestClassification, SHAPE_CCHCS_PACKET

        pc = dict(sample_pc)
        pc["id"] = "pc_weak_sim"
        pc["solicitation_number"] = ""
        pc["agency"] = ""
        pc["institution"] = "nowhere"
        # ~0.77 sim — passes the coverage threshold but not the strong-item 0.90.
        pc["items"] = [{
            "description": "Videos USB BLS Provider Course training set for clinical staff",
            "qty": 2,
        }]
        _save_single_pc(pc["id"], pc)

        classification = RequestClassification(
            shape=SHAPE_CCHCS_PACKET,
            agency="cchcs",
            institution="",
            solicitation_number="",
        )
        rfq_items = [{"description": "BLS Provider Course Videos: USB", "qty": 2}]

        linked_pc_id, reason, _ = _run_triangulated_linker(
            "rfq_weak_sim", classification, rfq_items,
        )
        assert linked_pc_id == "", (
            f"weak-sim match must not promote; got {linked_pc_id!r} ({reason})"
        )


# ─── Re-parse existing record ───────────────────────────────────────────

class TestReparseExistingRecord:
    def test_reparse_updates_classification_on_existing_pc(
        self, temp_data_dir, sample_pc
    ):
        """Re-running the pipeline on an existing PC ID must update
        the _classification field without creating a duplicate."""
        from src.api.dashboard import _save_single_pc, _load_price_checks
        pc = dict(sample_pc)
        pc["id"] = "pc_to_reparse"
        _save_single_pc(pc["id"], pc)

        from src.core.ingest_pipeline import process_buyer_request
        result = process_buyer_request(
            files=[PC_DOCX_FOOD],
            existing_record_id="pc_to_reparse",
            existing_record_type="pc",
        )
        assert result.ok is True
        assert result.record_id == "pc_to_reparse"
        # Classification stored on the record
        pcs = _load_price_checks()
        assert pcs["pc_to_reparse"].get("_classification", {}).get("shape") == "pc_704_docx"

    def test_reparse_rfq_registers_704b_template(
        self, temp_data_dir, sample_rfq, tmp_path
    ):
        """Re-uploading a 704B PDF via process_buyer_request on an
        existing RFQ must also register the file under
        rfq["templates"]["704b"] so the package generator can find it.

        Regression: the classifier_v2 ingest path previously only wrote
        line_items, causing "Missing required templates: 704B" at
        package generation time on 2026-04-15 for RFQ
        20260413_215152_19d88d (CCHCS RFQ 10837703). The old
        /rfq/<rid>/upload-templates handler registered templates via
        identify_attachments; _update_existing_record must mirror that.
        """
        import shutil
        from src.api.dashboard import _save_single_rfq, load_rfqs

        rfq = dict(sample_rfq)
        rfq["id"] = "rfq_template_register"
        rfq["templates"] = {}
        _save_single_rfq(rfq["id"], rfq)

        # Stage a PDF under a filename identify_attachments will
        # classify as "704b" (the helper is purely filename-based).
        staged = tmp_path / "AMS_704B_Worksheet.pdf"
        shutil.copy(PC_PDF_SCU_BLANK, staged)

        from src.core.ingest_pipeline import process_buyer_request
        result = process_buyer_request(
            files=[str(staged)],
            existing_record_id="rfq_template_register",
            existing_record_type="rfq",
        )
        assert result.ok is True

        saved = load_rfqs()["rfq_template_register"]
        templates = saved.get("templates") or {}
        assert "704b" in templates, (
            f"704B template not registered after re-upload — package "
            f"generator will fail with 'Missing required templates: "
            f"704B'. templates={templates}"
        )
        assert templates["704b"] == str(staged)

    def test_reparse_rfq_merges_templates_across_uploads(
        self, temp_data_dir, sample_rfq, tmp_path
    ):
        """Two sequential uploads (703B then 704B) must both end up in
        rfq["templates"] — the second upload must NOT overwrite the
        first. Guards against future regressions where template
        registration replaces instead of merges."""
        import shutil
        from src.api.dashboard import _save_single_rfq, load_rfqs

        rfq = dict(sample_rfq)
        rfq["id"] = "rfq_template_merge"
        rfq["templates"] = {}
        _save_single_rfq(rfq["id"], rfq)

        staged_703 = tmp_path / "AMS_703B_RFQ.pdf"
        staged_704 = tmp_path / "AMS_704B_Worksheet.pdf"
        shutil.copy(PC_PDF_SCU_BLANK, staged_703)
        shutil.copy(PC_PDF_SCU_BLANK, staged_704)

        from src.core.ingest_pipeline import process_buyer_request
        process_buyer_request(
            files=[str(staged_703)],
            existing_record_id="rfq_template_merge",
            existing_record_type="rfq",
        )
        process_buyer_request(
            files=[str(staged_704)],
            existing_record_id="rfq_template_merge",
            existing_record_type="rfq",
        )

        saved = load_rfqs()["rfq_template_merge"]
        templates = saved.get("templates") or {}
        assert "703b" in templates, f"703B dropped by second upload: {templates}"
        assert "704b" in templates, f"704B missing from templates: {templates}"


# ─── Defensive: empty / malformed input ────────────────────────────────

class TestDefensive:
    def test_empty_files_returns_email_only(self, temp_data_dir):
        from src.core.ingest_pipeline import process_buyer_request
        result = process_buyer_request(
            files=[],
            email_body="Please quote 100 gloves",
            email_sender="buyer@cchcs.ca.gov",
        )
        assert result.ok is True
        assert result.classification["shape"] == "email_only"
        assert result.classification["agency"] == "cchcs"

    def test_nonexistent_file_doesnt_crash(self, temp_data_dir):
        from src.core.ingest_pipeline import process_buyer_request
        result = process_buyer_request(
            files=["/tmp/does_not_exist.pdf"],
            email_sender="buyer@cchcs.ca.gov",
        )
        # Must not crash — just classify as email_only / unknown
        assert result.ok is True
