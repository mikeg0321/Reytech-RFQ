"""Tests for the Package Engine."""
from decimal import Decimal

import pytest

from src.core.quote_model import Quote, LineItem, QuoteHeader, BuyerInfo, Address
from src.forms.profile_registry import load_profiles
from src.forms.package_engine import assemble, PackageResult, register_post_hook


@pytest.fixture
def sample_quote():
    return Quote(
        header=QuoteHeader(solicitation_number="PKG-TEST-001", institution_key="CIW"),
        buyer=BuyerInfo(requestor_name="Test Buyer", requestor_phone="555-0100"),
        ship_to=Address(zip_code="91710"),
        line_items=[
            LineItem(line_no=1, description="Test Item A", qty=Decimal("5"),
                     uom="EA", unit_cost=Decimal("10.00"), markup_pct=Decimal("25")),
            LineItem(line_no=2, description="Test Item B", qty=Decimal("3"),
                     uom="BOX", unit_cost=Decimal("20.00"), markup_pct=Decimal("30")),
        ],
    )


@pytest.fixture
def profile_704a():
    return load_profiles()["704a_reytech_standard"]


class TestAssemble:
    """Package assembly from Quote + profiles."""

    def test_single_profile(self, sample_quote, profile_704a):
        result = assemble(sample_quote, [profile_704a])
        assert result.ok
        assert len(result.artifacts) == 1
        assert result.artifacts[0].profile_id == "704a_reytech_standard"
        assert len(result.artifacts[0].pdf_bytes) > 10000

    def test_single_profile_with_qa(self, sample_quote, profile_704a):
        result = assemble(sample_quote, [profile_704a], run_qa=True)
        assert result.ok
        assert result.artifacts[0].qa_report is not None
        assert result.artifacts[0].qa_report.passed

    def test_single_profile_no_qa(self, sample_quote, profile_704a):
        result = assemble(sample_quote, [profile_704a], run_qa=False)
        assert result.ok
        assert result.artifacts[0].qa_report is None

    def test_merged_pdf_for_single(self, sample_quote, profile_704a):
        result = assemble(sample_quote, [profile_704a])
        assert result.merged_pdf is not None
        assert len(result.merged_pdf) > 10000

    def test_empty_profiles(self, sample_quote):
        result = assemble(sample_quote, [])
        assert result.ok
        assert len(result.artifacts) == 0

    def test_summary_string(self, sample_quote, profile_704a):
        result = assemble(sample_quote, [profile_704a])
        assert "1/1" in result.summary
        assert "PASS" in result.summary


class TestPostHooks:
    """Post-assembly hooks."""

    def test_hook_called(self, sample_quote, profile_704a):
        called = []
        def my_hook(quote, result):
            called.append((quote.header.solicitation_number, result.ok))

        register_post_hook(my_hook)
        try:
            assemble(sample_quote, [profile_704a])
            assert len(called) == 1
            assert called[0] == ("PKG-TEST-001", True)
        finally:
            # Clean up hook
            from src.forms.package_engine import _post_hooks
            _post_hooks.remove(my_hook)


class TestErrorHandling:
    """Error cases."""

    def test_bad_profile_fails_gracefully(self, sample_quote):
        from src.forms.profile_registry import FormProfile
        bad = FormProfile(id="bad", form_type="test", blank_pdf="/nonexistent.pdf", fill_mode="acroform")
        result = assemble(sample_quote, [bad])
        assert not result.ok
        assert len(result.errors) == 1
        assert "bad" in result.errors[0]
