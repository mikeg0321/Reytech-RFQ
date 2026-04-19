"""End-to-end proof for the CalVet path through the QuoteOrchestrator,
plus an agency-agnostic proof for the new generated quote letterhead profile.

Test A — `test_calvet_r25q86_orchestrator_drives_to_qa_pass`
    Loads the real R25Q86 CalVet golden fixture (4 items, Veterans Home of
    California - Fresno, May 2025 RFQ Briefs Due 6/2). Drives it through the
    orchestrator with `target_stage="qa_pass"` and proves:
      - All 10 CalVet `required_forms` resolve to a real profile
        (quote, calrecycle74, bidder_decl, dvbe843, darfur_act, cv012_cuf,
         std204, std205, std1000, sellers_permit)
      - Every form fills + qa_passes
      - ComplianceValidator returns zero blockers
      - Final stage = qa_pass

Test B — `test_quote_letterhead_paginates_with_28_items`
    Independent of agency. Loads the existing 28-item Test0321 fixture (CCHCS
    source) and calls `fill(quote, quote_reytech_letterhead)` directly.
    Proves the synthesized letterhead PDF:
      - Renders 3 pages (stress test for pagination)
      - Items 1, 11, and 28 all appear in the rendered text

The letterhead is intentionally agency-agnostic — the same template renders
for CCHCS, CalVet, CDCR, etc. Only the To:/Ship To:/Bill To: blocks change.
Test B uses the larger 28-item fixture because that's the multi-page stress;
Test A's 4-item CalVet quote fits one page.

Real data only — no mocks, no synthetic items. Per
`feedback_no_ghost_data.md`.
"""
from __future__ import annotations

import io
import json
import os

import pytest

from src.core.quote_model import Quote
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
    _stage_index,
)
from src.forms.fill_engine import fill
from src.forms.profile_registry import load_profiles


_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "golden")
_R25Q86_FIXTURE = os.path.join(_FIXTURES_DIR, "r25q86_real_calvet.json")
_TEST0321_FIXTURE = os.path.join(_FIXTURES_DIR, "test0321_real_cchcs.json")


# ── Test A: CalVet R25Q86 orchestrator E2E ──────────────────────────────────

class TestCalvetR25Q86OrchestratorE2E:
    """Drive the REAL R25Q86 CalVet golden fixture through QuoteOrchestrator
    and prove the full CalVet form set fills + qa_passes + clears compliance.

    This is the platform proof for CalVet — every required form has a profile,
    every profile fills under real-buyer data, no compliance blockers.
    """

    @pytest.fixture
    def golden(self):
        with open(_R25Q86_FIXTURE, "r", encoding="utf-8") as f:
            return json.load(f)

    def test_calvet_r25q86_orchestrator_drives_to_qa_pass(self, golden):
        orch = QuoteOrchestrator(persist_audit=False)
        result = orch.run(QuoteRequest(
            source=golden,
            doc_type="rfq",
            agency_key="calvet",
            solicitation_number="R26Q9986",
            target_stage="qa_pass",
        ))

        # No programming errors anywhere.
        errors = [a for a in result.stage_history if a.outcome == "error"]
        assert not errors, (
            f"unexpected error outcomes: {[(a.stage_to, a.reasons) for a in errors]}"
        )

        # State machine never skipped a stage.
        for attempt in result.stage_history:
            from_idx = _stage_index(attempt.stage_from)
            to_idx = _stage_index(attempt.stage_to)
            assert to_idx - from_idx <= 1, (
                f"orchestrator skipped stages: {attempt.stage_from} -> {attempt.stage_to}"
            )

        # Ingest loaded all 4 R25Q86 items.
        assert result.quote is not None
        assert len(result.quote.line_items) == 4

        # All 10 CalVet required forms resolved to profiles.
        expected = {
            "quote_reytech_letterhead",
            "calrecycle74_reytech_standard",
            "bidder_decl_reytech_standard",
            "dvbe843_reytech_standard",
            "darfur_reytech_standard",
            "cv012_cuf_reytech_standard",
            "std204_reytech_standard",
            "std205_reytech_standard",
            "std1000_reytech_standard",
            "sellers_permit_reytech",
        }
        used = set(result.profiles_used)
        missing = expected - used
        assert not missing, (
            f"CalVet required-form profiles not resolved: {missing} "
            f"(profiles_used={result.profiles_used}, warnings={result.warnings})"
        )

        # Every required profile filled + qa_passed.
        per_form = result.compliance_report.get("per_form", [])
        unfilled = [
            r for r in per_form
            if r.get("profile_id") in expected
            and not (r.get("filled") and r.get("qa_passed"))
        ]
        assert not unfilled, (
            f"some required CalVet forms did not fill+qa_pass: {unfilled}"
        )

        # Compliance validator returned zero blockers.
        gap = result.compliance_report.get("gap", {})
        compliance_blockers = gap.get("blockers", [])
        assert not compliance_blockers, (
            f"compliance blockers on CalVet R25Q86: {compliance_blockers}"
        )

        # Final stage advanced to qa_pass.
        assert result.ok, f"orchestrator returned not ok; blockers={result.blockers}"
        assert result.final_stage == "qa_pass", (
            f"expected final_stage=qa_pass; got {result.final_stage}"
        )


# ── Test B: quote letterhead paginates correctly ────────────────────────────

class TestQuoteLetterheadPaginates:
    """The Reytech quote letterhead is agency-agnostic. Whether the buyer is
    CCHCS, CalVet, or anyone else, the letterhead lays out identically — only
    the To:/Ship To:/Bill To: blocks vary. This test stresses the pagination
    path with a real 28-item dataset (CCHCS source) and asserts the rendered
    PDF spans 3 pages with first/middle/last items all present.
    """

    @pytest.fixture
    def golden(self):
        with open(_TEST0321_FIXTURE, "r", encoding="utf-8") as f:
            return json.load(f)

    def test_quote_letterhead_paginates_with_28_items(self, golden):
        quote = Quote.from_legacy_dict(golden, doc_type="rfq")
        # Pre-allocate the test quote number so the rendered PDF has a stable
        # header (avoids consuming the prod quote counter).
        quote.header.solicitation_number = "R26Q0321"

        profiles = load_profiles()
        assert "quote_reytech_letterhead" in profiles, (
            "quote_reytech_letterhead profile must be registered"
        )
        profile = profiles["quote_reytech_letterhead"]
        assert profile.fill_mode == "generated", (
            f"profile must be fill_mode=generated; got {profile.fill_mode}"
        )

        pdf_bytes = fill(quote, profile)
        assert pdf_bytes and len(pdf_bytes) > 1000, "letterhead PDF empty/tiny"

        # Page count: 28 items must paginate to 3 pages on this template.
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        assert len(reader.pages) == 3, (
            f"expected 3 pages for 28 items; got {len(reader.pages)}"
        )

        # Boundary items must all appear in the rendered text. We use
        # MFG numbers as the probe since they are unique and reliably
        # rendered (not subject to description wrapping).
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            full_text = "\n".join((p.extract_text() or "") for p in pdf.pages)

        items = golden["line_items"]
        for idx in (0, 10, 27):  # items 1, 11, 28 (0-indexed)
            mfg = items[idx]["mfg_number"]
            assert mfg in full_text, (
                f"item #{idx + 1} (mfg={mfg!r}) missing from rendered letterhead"
            )
