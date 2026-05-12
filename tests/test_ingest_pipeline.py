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


# ─── Substrate: placeholder detector + CalVet sol# synthesizer ─────────

class TestPlaceholderDetector:
    def test_recognizes_known_sentinels(self):
        from src.core.ingest_pipeline import _looks_like_sol_placeholder
        for s in ("", "  ", "WORKSHEET", "GOOD", "RFQ", "QUOTE",
                  "worksheet", "Bid", "1", "42"):
            assert _looks_like_sol_placeholder(s), s

    def test_real_sol_numbers_pass(self):
        from src.core.ingest_pipeline import _looks_like_sol_placeholder
        for s in ("25CB021", "2010017786", "8955-0001234", "10843276",
                  "PREQ10843276"):
            assert not _looks_like_sol_placeholder(s), s

    def test_synthesized_RT_prefix_is_not_placeholder(self):
        """Reytech-synthesized `RT-CALVET-…` must NOT be flagged so the
        allocation gate (dashboard `is_ready_for_quote_allocation`)
        accepts it."""
        from src.core.ingest_pipeline import _looks_like_sol_placeholder
        from src.api.dashboard import _is_placeholder_number
        synth = "RT-CALVET-260512-2ff46f99"
        assert _looks_like_sol_placeholder(synth) is False
        # The dashboard-side gate uses its own copy of the predicate —
        # they must agree on the RT-prefix exemption.
        assert _is_placeholder_number(synth) is False

    def test_auto_id_is_placeholder_for_synthesizer(self):
        """Local helper treats `AUTO_<id>` as a placeholder so the
        synthesizer triggers on it. Dashboard's version differs (it
        does NOT flag AUTO_ because the operator-allocation gate has
        its own AUTO handling)."""
        from src.core.ingest_pipeline import _looks_like_sol_placeholder
        assert _looks_like_sol_placeholder("AUTO_abc12345") is True


class TestClassifierPerFileCache:
    """Performance follow-on 2026-05-12 — classifier publishes
    `_per_file_info` so the multi-attach Vision union doesn't have to
    re-call `_classify_pdf` per sibling. Pin: cache exists, has the
    expected shape, and is NOT serialized to record JSON."""

    def test_per_file_info_populated_for_pdf_attachments(self, tmp_path):
        from src.core.request_classifier import classify_request

        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter

        def make_pdf(p, text):
            c = canvas.Canvas(str(p), pagesize=letter)
            y = 750
            for line in text.splitlines():
                c.drawString(50, y, line[:90]); y -= 14
            c.save()

        cover = tmp_path / "cover.pdf"
        pricing = tmp_path / "pricing.pdf"
        make_pdf(cover, "COVER LETTER\nBidder Instructions")
        make_pdf(pricing, "DESCRIPTION OF GOODS / SERVICES UNIT PRICE EXTENSION")

        r = classify_request(attachments=[str(cover), str(pricing)])
        assert "cover.pdf" in r._per_file_info
        assert "pricing.pdf" in r._per_file_info
        assert r._per_file_info["pricing.pdf"]["info"]["pricing_page_score"] > 0
        assert r._per_file_info["cover.pdf"]["info"].get("pricing_page_score", 0) == 0

    def test_per_file_info_excluded_from_to_dict(self, tmp_path):
        """The cache is internal — it must not bloat the record JSON."""
        from src.core.request_classifier import classify_request
        r = classify_request(attachments=[], email_body="test")
        # Manually populate to confirm it would be filterable
        r._per_file_info["fake.pdf"] = {"shape": "x", "info": {"k": "v"}}
        d = r.to_dict()
        assert "_per_file_info" not in d


class TestMultiAttachCacheUsage:
    """Pin: `_multi_attachment_vision_union` reads from
    classification._per_file_info instead of re-calling _classify_pdf
    when the cache has entries for the sibling files."""

    def test_union_skips_reclassification_when_cache_present(self, tmp_path, monkeypatch):
        from src.core import ingest_pipeline as ip
        from src.core.request_classifier import (
            RequestClassification, SHAPE_GENERIC_RFQ_PDF,
        )

        # Two real (empty) PDFs so os.path.exists is True
        primary = tmp_path / "primary.pdf"
        sibling = tmp_path / "sibling.pdf"
        from reportlab.pdfgen import canvas
        for p in (primary, sibling):
            c = canvas.Canvas(str(p)); c.drawString(50, 750, "x"); c.save()

        # Force is_available True, _vision_primary_extract a stub that
        # returns a known item — and count _classify_pdf calls.
        calls = {"classify": 0, "vision": 0}

        def fake_classify(_path):
            calls["classify"] += 1
            return SHAPE_GENERIC_RFQ_PDF, {"pricing_page_score": 5, "page_count": 1}

        def fake_vision(path):
            calls["vision"] += 1
            return [{"description": "sibling item", "qty": 1}]

        monkeypatch.setattr(
            "src.forms.vision_parser.is_available", lambda: True,
        )
        # `_classify_pdf` is imported lazily inside the union via
        # `from src.core.request_classifier import …`, so patch the
        # source module. `_vision_primary_extract` is at module level
        # in ingest_pipeline so patch there.
        monkeypatch.setattr("src.core.request_classifier._classify_pdf", fake_classify)
        monkeypatch.setattr(ip, "_vision_primary_extract", fake_vision)

        # Pre-populate the cache for the sibling — the union should
        # NOT re-call _classify_pdf when it can read from cache.
        classification = RequestClassification(
            shape=SHAPE_GENERIC_RFQ_PDF,
            agency="dsh",
            primary_file="primary.pdf",
        )
        classification._per_file_info["sibling.pdf"] = {
            "shape": SHAPE_GENERIC_RFQ_PDF,
            "info": {"pricing_page_score": 5, "page_count": 1},
        }

        extras = ip._multi_attachment_vision_union(
            primary_path=str(primary),
            all_files=[str(primary), str(sibling)],
            classification=classification,
            primary_items=[],
        )
        assert calls["classify"] == 0, (
            f"_classify_pdf was called {calls['classify']} times — cache "
            f"should have served the sibling without re-classification"
        )
        assert calls["vision"] == 1
        assert len(extras) == 1
        assert extras[0]["description"] == "sibling item"
        assert extras[0]["_source_attachment"] == "sibling.pdf"
        assert extras[0]["_extraction_source"] == "vision_sibling"

    def test_union_falls_back_to_live_classify_on_cache_miss(self, tmp_path, monkeypatch):
        """Defensive: classification objects from older code paths may
        lack a populated `_per_file_info`. The union should still work
        by calling _classify_pdf live."""
        from src.core import ingest_pipeline as ip
        from src.core.request_classifier import (
            RequestClassification, SHAPE_GENERIC_RFQ_PDF,
        )
        from reportlab.pdfgen import canvas
        primary = tmp_path / "primary.pdf"
        sibling = tmp_path / "sibling.pdf"
        for p in (primary, sibling):
            c = canvas.Canvas(str(p)); c.drawString(50, 750, "x"); c.save()

        calls = {"classify": 0, "vision": 0}

        def fake_classify(_path):
            calls["classify"] += 1
            return SHAPE_GENERIC_RFQ_PDF, {"pricing_page_score": 5}

        def fake_vision(_path):
            calls["vision"] += 1
            return [{"description": "ok", "qty": 1}]

        monkeypatch.setattr(
            "src.forms.vision_parser.is_available", lambda: True,
        )
        # `_classify_pdf` is imported lazily inside the union via
        # `from src.core.request_classifier import …`, so patch the
        # source module. `_vision_primary_extract` is at module level
        # in ingest_pipeline so patch there.
        monkeypatch.setattr("src.core.request_classifier._classify_pdf", fake_classify)
        monkeypatch.setattr(ip, "_vision_primary_extract", fake_vision)

        # Classification with EMPTY cache
        classification = RequestClassification(
            shape=SHAPE_GENERIC_RFQ_PDF,
            agency="dsh",
            primary_file="primary.pdf",
        )
        extras = ip._multi_attachment_vision_union(
            primary_path=str(primary),
            all_files=[str(primary), str(sibling)],
            classification=classification,
            primary_items=[],
        )
        # Falls back to live classify since cache is empty
        assert calls["classify"] == 1
        assert calls["vision"] == 1
        assert len(extras) == 1


class TestItemSignature:
    """`_item_signature` is the dedup key for the multi-attachment
    Vision union. Two items that describe the same line in different
    case/whitespace MUST collide; two items that differ in qty or
    MFG# MUST NOT collide."""

    def test_case_and_whitespace_normalize(self):
        from src.core.ingest_pipeline import _item_signature
        a = {"description": "  POWER  SUPPLY ADAM SCALE  ", "qty": 2, "item_number": "WCA86920"}
        b = {"description": "power supply adam scale", "qty": 2.0, "item_number": "wca86920"}
        assert _item_signature(a) == _item_signature(b)

    def test_qty_difference_distinguishes(self):
        from src.core.ingest_pipeline import _item_signature
        a = {"description": "blades", "qty": 1500}
        b = {"description": "blades", "qty": 1000}
        assert _item_signature(a) != _item_signature(b)

    def test_mfg_difference_distinguishes(self):
        """Same description + qty but different MFG# = different items
        (size variants etc.)."""
        from src.core.ingest_pipeline import _item_signature
        a = {"description": "coverall", "qty": 4, "item_number": "MICROMAX-XL"}
        b = {"description": "coverall", "qty": 4, "item_number": "MICROMAX-XXL"}
        assert _item_signature(a) != _item_signature(b)


class TestLowDensityGate:
    """P000 substrate #2 — full form. The zero-items gate already fires
    on binary 0 cases; this one catches the silent failure class where
    e.g. 5 items get extracted from a 4-page items table."""

    def _reconcile(self, vision_items, base_items, page_count):
        """Helper: call _reconcile_vision_and_base with a fake PDF whose
        page count is controlled via a monkey-patched _pdf_page_count."""
        from src.core import ingest_pipeline as ip
        orig = ip._pdf_page_count
        ip._pdf_page_count = lambda _p: page_count
        try:
            return ip._reconcile_vision_and_base(
                vision_items=vision_items,
                base_items=base_items,
                base_parser_label="generic_rfq",
                path="/tmp/fake.pdf",
            )
        finally:
            ip._pdf_page_count = orig

    def test_low_density_on_multipage_fires_review(self):
        """3 items extracted from a 5-page PDF = 0.6 items/page,
        below the 2.0 floor. Must flag needs_review."""
        items = [{"description": f"item {i}", "qty": 1} for i in range(3)]
        primary, warnings, needs_review = self._reconcile(items, items, page_count=5)
        assert needs_review is True
        kinds = [w["kind"] for w in warnings]
        assert "low_item_density" in kinds

    def test_normal_density_does_not_fire(self):
        """12 items on a 2-page PDF = 6.0 items/page, well above the
        floor. No low-density warning."""
        items = [{"description": f"item {i}", "qty": 1} for i in range(12)]
        primary, warnings, needs_review = self._reconcile(items, items, page_count=2)
        kinds = [w["kind"] for w in warnings]
        assert "low_item_density" not in kinds

    def test_single_page_low_density_does_not_fire(self):
        """A 1-page RFQ with 1 item is legitimate (single-line RFQs
        exist). The density gate is for multi-page (>=2) only."""
        items = [{"description": "blades", "qty": 1500}]
        primary, warnings, needs_review = self._reconcile(items, items, page_count=1)
        kinds = [w["kind"] for w in warnings]
        assert "low_item_density" not in kinds

    def test_zero_items_takes_precedence(self):
        """When BOTH parsers return 0 items, zero_items_on_pdf fires
        (early-return path) — low_item_density does NOT also fire."""
        primary, warnings, needs_review = self._reconcile([], [], page_count=4)
        assert needs_review is True
        kinds = [w["kind"] for w in warnings]
        assert "zero_items_on_pdf" in kinds
        assert "low_item_density" not in kinds  # early-returned before the density check


class TestCalvetSolSynthesizer:
    def test_synthesizer_fires_when_calvet_no_sol(self, temp_data_dir, tmp_path):
        """When agency=calvet, sol# is junk, AND items exist, the
        ingest writes a synthesized `RT-CALVET-<date>-<id>` rfq_number
        so the allocation gate passes."""
        from src.core.ingest_pipeline import process_buyer_request

        # Use email-only path — fast, no fixture PDF needed. Agency
        # resolves to calvet via the sender domain.
        result = process_buyer_request(
            files=[],
            email_body="Please quote 100 medical supplies items",
            email_sender="grace.post@calvet.ca.gov",
            email_subject="WORKSHEET request",
        )
        assert result.ok is True
        assert result.classification["agency"] == "calvet"
        # No items extracted from a bare body line, so synthesizer should
        # NOT fire (the gate requires items > 0). pc_number will be
        # AUTO_<id> per the existing fallback.
        rid = result.record_id
        from src.api.dashboard import load_rfqs
        rfqs = load_rfqs()
        rec = rfqs.get(rid) if isinstance(rfqs, dict) else next(
            (r for r in rfqs if isinstance(r, dict) and r.get("id") == rid), None,
        )
        if rec:
            sol = rec.get("solicitation_number", "")
            # Either AUTO_ fallback (no items) OR synthesized RT- (with items).
            # Both are valid — the explicit anti-pattern is "WORKSHEET".
            assert "WORKSHEET" not in (sol or "").upper()
