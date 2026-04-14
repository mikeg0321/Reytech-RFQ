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
        with classification stored, not a bare PC."""
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
        # Classification must be stored ON the record
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
