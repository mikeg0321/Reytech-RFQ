"""Tests for the unified quote engine orchestrator (src.core.quote_engine).

Covers ingestion, profile resolution, draft, finalize, and boot-time validation.
The pricing-oracle path is exercised separately because the oracle is lazily
imported and may not be available in CI.
"""
import io
from decimal import Decimal

import pytest
from pypdf import PdfReader

from src.core import quote_engine
from src.core.quote_engine import (
    DraftResult,
    boot_validate_profiles,
    draft,
    finalize,
    fill_one,
    get_profiles,
    ingest,
    pick_profile,
    sign,
    validate,
)
from src.core.quote_model import (
    Address,
    BuyerInfo,
    DocType,
    LineItem,
    Quote,
    QuoteHeader,
)
from src.forms.profile_registry import FormProfile


@pytest.fixture(autouse=True)
def _reset_profile_cache():
    """Each test gets a fresh profile cache so mutations don't leak."""
    quote_engine._PROFILES_CACHE = None
    yield
    quote_engine._PROFILES_CACHE = None


@pytest.fixture
def sample_quote():
    return Quote(
        doc_type="pc",
        doc_id="qe-test-001",
        header=QuoteHeader(
            solicitation_number="OS - Den - Feb 2026",
            institution_key="CSP-Sacramento",
        ),
        buyer=BuyerInfo(
            requestor_name="Jane Buyer",
            requestor_phone="916-555-0100",
        ),
        ship_to=Address(full="CSP-Sacramento, Represa, CA 95671", zip_code="95671"),
        line_items=[
            LineItem(line_no=1, description="Name tag black/white",
                     qty=Decimal("22"), uom="EA",
                     unit_cost=Decimal("12.58"), markup_pct=Decimal("25"),
                     item_no="NT-001"),
            LineItem(line_no=2, description="Copy paper 10 reams",
                     qty=Decimal("5"), uom="BOX",
                     unit_cost=Decimal("42.99"), markup_pct=Decimal("25")),
        ],
    )


# ── Profile registry plumbing ─────────────────────────────────────────────

class TestProfileResolution:

    def test_get_profiles_caches(self):
        first = get_profiles()
        second = get_profiles()
        assert first is second  # same dict object — cached
        assert "704a_reytech_standard" in first

    def test_get_profiles_refresh(self):
        first = get_profiles()
        second = get_profiles(refresh=True)
        assert first is not second  # fresh load
        assert set(first.keys()) == set(second.keys())

    def test_pick_profile_explicit_id(self, sample_quote):
        p = pick_profile(sample_quote, profile_id="704a_reytech_standard")
        assert p.id == "704a_reytech_standard"

    def test_pick_profile_doc_type_default_pc(self, sample_quote):
        p = pick_profile(sample_quote)
        assert p.id == "704a_reytech_standard"

    def test_pick_profile_doc_type_default_rfq(self):
        q = Quote(doc_type="rfq", doc_id="r1")
        p = pick_profile(q)
        assert p.id == "704a_reytech_standard"

    def test_pick_profile_unknown_id_falls_through_to_default(self, sample_quote):
        # Unknown profile_id should fall through to the doc-type default
        # (rather than raising) — keeps routes resilient to stale UI selections.
        p = pick_profile(sample_quote, profile_id="does_not_exist")
        assert p.id == "704a_reytech_standard"

    def test_pick_profile_no_default_raises(self, monkeypatch):
        monkeypatch.setitem(quote_engine._DEFAULT_PROFILE_BY_DOC_TYPE, DocType.PC, None)
        q = Quote(doc_type="pc", doc_id="x")
        with pytest.raises(KeyError):
            pick_profile(q, profile_id="missing")


# ── Ingest ────────────────────────────────────────────────────────────────

class TestIngest:

    def test_ingest_dict(self):
        legacy = {
            "id": "pc-123",
            "agency": "CDCR",
            "items": [
                {"description": "Widget", "quantity": 2,
                 "unit_cost": 5.00, "markup_pct": 25},
            ],
        }
        quote, warnings = ingest(legacy, doc_type="pc")
        assert isinstance(quote, Quote)
        assert quote.doc_type == DocType.PC
        assert len(quote.line_items) == 1
        assert warnings == []

    def test_ingest_dict_as_rfq(self):
        legacy = {"id": "rfq-9", "items": []}
        quote, warnings = ingest(legacy, doc_type="rfq")
        assert quote.doc_type == DocType.RFQ
        assert warnings == []

    def test_ingest_missing_pdf_returns_warning(self):
        quote, warnings = ingest("/no/such/file.pdf", doc_type="pc")
        assert isinstance(quote, Quote)
        assert quote.line_items == []
        assert len(warnings) == 1
        assert warnings[0].severity == "error"
        assert "not found" in warnings[0].message.lower()

    def test_ingest_unsupported_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported source type"):
            ingest(12345)  # type: ignore[arg-type]


# ── Fill / Validate / Sign (single-form path) ─────────────────────────────

class TestFillAndValidate:

    def test_fill_one_returns_pdf_bytes(self, sample_quote):
        pdf = fill_one(sample_quote)
        assert isinstance(pdf, bytes)
        assert pdf.startswith(b"%PDF")
        assert len(pdf) > 10_000

    def test_fill_one_explicit_profile(self, sample_quote):
        pdf = fill_one(sample_quote, profile_id="704a_reytech_standard")
        reader = PdfReader(io.BytesIO(pdf))
        fields = reader.get_fields()
        assert "Reytech" in str(fields.get("SUPPLIER NAME", {}).get("/V", ""))

    def test_validate_runs_on_filled_pdf(self, sample_quote):
        pdf = fill_one(sample_quote)
        report = validate(pdf, sample_quote)
        assert report.profile_id == "704a_reytech_standard"
        assert report.fields_checked > 0


# ── draft() one-shot ──────────────────────────────────────────────────────

class TestDraft:

    def test_draft_returns_editable_pdf_with_qa(self, sample_quote):
        result = draft(sample_quote)
        assert isinstance(result, DraftResult)
        assert result.profile_id == "704a_reytech_standard"
        assert result.pdf_bytes.startswith(b"%PDF")
        assert result.qa_report.fields_checked > 0
        # Editable — the PDF is not flattened. AcroForm root should remain.
        reader = PdfReader(io.BytesIO(result.pdf_bytes))
        assert reader.get_fields()  # form fields still present

    def test_draft_skips_qa_when_disabled(self, sample_quote):
        result = draft(sample_quote, run_qa=False)
        assert result.qa_report.passed is True
        assert result.qa_report.fields_checked == 0

    def test_draft_ok_property_reflects_qa(self, sample_quote):
        result = draft(sample_quote, run_qa=False)
        assert result.ok is True


# ── finalize() one-shot ───────────────────────────────────────────────────

class TestFinalize:

    def test_finalize_default_returns_signed_merged(self, sample_quote):
        result = finalize(sample_quote, sign_after=False)  # skip sig (no png in test env)
        assert result.merged_pdf is not None
        assert result.merged_pdf.startswith(b"%PDF")
        assert len(result.artifacts) == 1
        assert result.artifacts[0].profile_id == "704a_reytech_standard"

    def test_finalize_explicit_profile_list(self, sample_quote):
        result = finalize(
            sample_quote,
            profile_ids=["704a_reytech_standard"],
            sign_after=False,
        )
        assert result.ok is True
        assert len(result.artifacts) == 1

    def test_finalize_unknown_profile_skipped_with_warning(self, sample_quote):
        result = finalize(
            sample_quote,
            profile_ids=["704a_reytech_standard", "ghost_profile"],
            sign_after=False,
        )
        assert any("ghost_profile" in w for w in result.warnings)
        assert len(result.artifacts) == 1

    def test_finalize_all_profiles_unknown_raises(self, sample_quote):
        with pytest.raises(KeyError):
            finalize(sample_quote, profile_ids=["bogus"], sign_after=False)


# ── sign() ────────────────────────────────────────────────────────────────

class TestSign:

    def test_sign_returns_bytes(self, sample_quote):
        pdf = fill_one(sample_quote)
        signed = sign(pdf)  # no signature image — overlay still runs (date stamp)
        assert isinstance(signed, bytes)
        assert signed.startswith(b"%PDF")


# ── boot_validate_profiles ────────────────────────────────────────────────

class TestBootValidate:

    def test_boot_validate_returns_dict(self):
        results = boot_validate_profiles(strict=False)
        assert isinstance(results, dict)
        assert "704a_reytech_standard" in results

    def test_boot_validate_strict_raises_on_bad_profile(self, monkeypatch):
        # Inject a fake validator result with one failing profile
        from src.forms import profile_registry as pr

        def fake_validate_all(*a, **kw):
            return {"704a_reytech_standard": [], "broken": ["missing field FOO"]}

        monkeypatch.setattr(pr, "validate_all_profiles", fake_validate_all)
        with pytest.raises(RuntimeError, match="refusing to start"):
            boot_validate_profiles(strict=True)

    def test_boot_validate_non_strict_returns_failures(self, monkeypatch):
        from src.forms import profile_registry as pr

        monkeypatch.setattr(
            pr, "validate_all_profiles",
            lambda *a, **kw: {"a": [], "b": ["bad"]},
        )
        results = boot_validate_profiles(strict=False)
        assert results == {"a": [], "b": ["bad"]}
