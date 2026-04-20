"""Tests that _resolve_profiles distinguishes intentional non-profiles from
genuinely-missing profiles in its warning text.

CCHCS's required_forms list includes "bidpkg" — a container handled by
package_engine, NOT a profile. The mapping `_FORM_ID_TO_PROFILE_ID["bidpkg"] = ""`
encodes this intent: empty string means "this is not supposed to be a profile."

The previous warning lumped bidpkg together with genuinely-missing profiles
like 703b: "agency 'cchcs' requires forms with no profile yet: ['703b', 'bidpkg']".
Anyone reading that warning would conclude they need to BUILD a bidpkg profile
— wasted dev time. The fix: separate the two categories so the warning only
calls out forms that need a real profile.
"""
from __future__ import annotations

from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest


def _cchcs_dict_source() -> dict:
    return {
        "doc_id": "test_profile_warnings",
        "line_items": [
            {"line_no": 1, "description": "Gauze 4x4", "qty": 10, "unit_cost": "2.00"},
        ],
        "header": {"agency_key": "cchcs", "solicitation_number": "R26Q0042"},
        "buyer": {"requestor_email": "buyer@cchcs.ca.gov"},
    }


class TestProfileResolutionWarnings:
    def test_bidpkg_not_in_missing_profile_warning(self):
        """bidpkg is intentionally not a profile — must NOT appear in
        the 'no profile yet' warning."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_cchcs_dict_source(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="priced",  # stop early — we only care about profile resolution
        )
        result = orch.run(req)

        no_profile_warnings = [
            w for w in result.warnings if "no profile yet" in w
        ]
        # If 703b (or any other missing profile) is genuinely absent, that
        # warning should still surface — but bidpkg should not.
        for w in no_profile_warnings:
            assert "bidpkg" not in w, (
                f"bidpkg appears in 'no profile yet' warning — it's "
                f"intentionally a package_engine container, not a profile. "
                f"Warning: {w}"
            )

    def test_genuinely_missing_profile_still_warned(self, monkeypatch):
        """Sanity — a form_id with a non-empty mapping but no loaded profile
        SHOULD still be called out as missing. Simulates 703b being absent
        from the registry (at one point in history it really wasn't built)
        so the warning path stays covered even now that 703b ships."""
        from src.core import quote_engine

        original = quote_engine.get_profiles()
        stripped = {k: v for k, v in original.items() if k != "703b_reytech_standard"}
        monkeypatch.setattr(quote_engine, "get_profiles", lambda: stripped)

        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_cchcs_dict_source(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="priced",
        )
        result = orch.run(req)

        no_profile_warnings = [
            w for w in result.warnings if "no profile yet" in w
        ]
        assert any("703b" in w for w in no_profile_warnings), (
            f"Expected a warning calling out 703b. All warnings: {result.warnings}"
        )
