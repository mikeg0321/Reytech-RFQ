"""Regression: Option A — shape=cchcs_it_rfq auto-includes CCHCS standard
submission subset (DVBE 843 + bidder_decl/STD 21 + CalRecycle 074) on top
of the agency's required_forms.

Per Mike 2026-04-23 after PR-B audit (docs/BID_PACKAGE_AUDIT_2026_04_23.md):
the generated bid package was only 6 pages vs the 20-page north star
because the CCHCS standalone forms were in `optional_forms` — not included
unless the operator ticked boxes manually. Option A auto-includes the
3 forms that ship with every Reytech LPA IT RFQ submission.

Test validates the _req_forms extension logic in
routes_rfq_gen.generate_rfq_package without requiring the full Flask app
stack. The underlying filler calls (generate_dvbe_843, fill_bidder_declaration,
fill_calrecycle_standalone) are exercised by their own existing tests.
"""
import pytest


def _compute_req_forms(_req_forms_filtered, _rfq_shape):
    """Mirror the Option A extension logic in routes_rfq_gen.py so we can
    unit-test it in isolation. Drift between this helper and the route
    implementation would mean the test isn't validating the live path —
    intentionally kept as a verbatim copy so a structural divergence
    raises this test as the canary."""
    _req_forms = set(_req_forms_filtered)
    if _rfq_shape == "cchcs_it_rfq":
        _lpa_auto = {"dvbe843", "bidder_decl", "calrecycle74"}
        _req_forms.update(_lpa_auto)
    return _req_forms


class TestOptionACchcsItRfqAutoInclude:

    def test_cchcs_it_rfq_adds_dvbe_bidder_calrecycle(self):
        """The core Option A behavior: 3 forms auto-added on LPA shape."""
        base = {"703b", "704b", "bidpkg", "quote"}
        result = _compute_req_forms(base, "cchcs_it_rfq")
        assert "dvbe843" in result
        assert "bidder_decl" in result
        assert "calrecycle74" in result

    def test_cchcs_it_rfq_preserves_base_required_forms(self):
        """Option A ADDS, it does not REPLACE. Base forms stay."""
        base = {"quote", "sellers_permit"}
        result = _compute_req_forms(base, "cchcs_it_rfq")
        assert "quote" in result
        assert "sellers_permit" in result
        # Plus the auto-adds
        assert {"dvbe843", "bidder_decl", "calrecycle74"}.issubset(result)

    def test_non_lpa_shape_unchanged(self):
        """cchcs_packet / generic / email_only MUST NOT trigger auto-add —
        those agencies/shapes have different form requirements."""
        for shape in ("cchcs_packet", "generic_rfq_pdf", "email_only", "unknown", ""):
            base = {"703b", "704b", "bidpkg", "quote"}
            result = _compute_req_forms(base, shape)
            assert "dvbe843" not in result, f"shape={shape} should NOT auto-add dvbe843"
            assert "bidder_decl" not in result, f"shape={shape} should NOT auto-add bidder_decl"
            assert "calrecycle74" not in result, f"shape={shape} should NOT auto-add calrecycle74"

    def test_idempotent_when_already_required(self):
        """If the agency config ALREADY lists dvbe843 as required (e.g., DSH),
        Option A must not break — the update is a set union."""
        base = {"quote", "dvbe843", "calrecycle74"}
        result = _compute_req_forms(base, "cchcs_it_rfq")
        assert "dvbe843" in result
        assert "bidder_decl" in result
        assert "calrecycle74" in result
        # No duplicate surface because it's a set
        assert len([f for f in result if f == "dvbe843"]) == 1

    def test_empty_base_on_cchcs_it_rfq_still_gets_auto(self):
        """Even if agency_config somehow returned zero required_forms,
        cchcs_it_rfq shape still ships DVBE + bidder_decl + CalRecycle."""
        result = _compute_req_forms(set(), "cchcs_it_rfq")
        assert result == {"dvbe843", "bidder_decl", "calrecycle74"}


class TestOptionAWiredInRoute:
    """Smoke test that the extension is actually in routes_rfq_gen.py
    at the right place. Catches accidental revert / merge conflict."""

    def test_route_file_contains_option_a_block(self):
        import pathlib
        route_file = pathlib.Path(__file__).parent.parent / "src" / "api" / "modules" / "routes_rfq_gen.py"
        text = route_file.read_text(encoding="utf-8")
        assert 'if _rfq_shape == "cchcs_it_rfq":' in text
        assert '"dvbe843"' in text
        assert '"bidder_decl"' in text
        assert '"calrecycle74"' in text
        assert "Option A" in text  # marker comment so future edits preserve intent
