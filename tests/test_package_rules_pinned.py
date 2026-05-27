"""Regression test pinning Mike's hard package rules from CLAUDE.md.

These rules have caused multiple production incidents when violated.
The audit of `agency_config.py` against CLAUDE.md was previously
informal — this file makes the rules executable.

Rules pinned:

1. **CCHCS / CDCR**: required_forms = 703B + 704B + Bid Package + Quote.
   DVBE 843 and seller's permit are INSIDE the bid package — never
   generate as standalone required forms. (CLAUDE.md "Form Filling
   Guard Rails > Package Generation".)

2. **Optional forms are OPTIONAL.** Never auto-include based on item
   count or heuristics. Optional forms must live in `optional_forms`,
   not `required_forms`.

3. **CCHCS optional forms include 703c** (alternative to 703B when the
   buyer ships a 703C template).
"""
from __future__ import annotations


def test_cchcs_required_forms_match_claude_md_rule():
    """CCHCS package = 703B + 704B + Bid Package + Quote (4 forms only)."""
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    cchcs = DEFAULT_AGENCY_CONFIGS["cchcs"]
    required = cchcs["required_forms"]

    # Exact match — adding or removing a form here is a real package
    # change and must be intentional. Update both this test and
    # CLAUDE.md "Form Filling Guard Rails" together.
    assert set(required) == {"703b", "704b", "bidpkg", "quote"}, (
        f"CCHCS required_forms must be 703B + 704B + bidpkg + quote, "
        f"got: {required}. CLAUDE.md states 'CCHCS package = 703B/C + "
        f"704B + Bid Package + Quote ONLY. DVBE 843, seller's permit, "
        f"CalRecycle are INSIDE the bid package. Never generate "
        f"standalone.'"
    )


def test_cchcs_dvbe843_is_optional_not_required():
    """DVBE 843 is INSIDE the bid package PDF. Listing it in
    required_forms would generate it standalone — duplicate paperwork.
    """
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    cchcs = DEFAULT_AGENCY_CONFIGS["cchcs"]
    assert "dvbe843" not in cchcs["required_forms"], (
        "DVBE 843 must NOT be in CCHCS required_forms — it's inside "
        "the bid package PDF. Adding it generates standalone duplicate."
    )
    assert "dvbe843" in cchcs["optional_forms"], (
        "DVBE 843 must be in optional_forms so the operator can opt in "
        "if a non-CCHCS variant requires it"
    )


def test_cchcs_sellers_permit_is_optional_not_required():
    """Same as DVBE 843 — inside the bid package PDF."""
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    cchcs = DEFAULT_AGENCY_CONFIGS["cchcs"]
    assert "sellers_permit" not in cchcs["required_forms"], (
        "sellers_permit must NOT be in CCHCS required_forms"
    )


def test_cchcs_703c_is_optional_alternative_to_703b():
    """703C is an alternative supplier-info form some CCHCS orgs use.
    It must be in optional_forms so operators can swap when buyer ships
    a 703C template."""
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    cchcs = DEFAULT_AGENCY_CONFIGS["cchcs"]
    assert "703c" in cchcs["optional_forms"], (
        "703c must be in CCHCS optional_forms — buyer-shipped variant"
    )


def test_cchcs_primary_response_form_is_704b():
    """The 704B is Reytech's actual response form (priced). 703B/C is
    supplier registration. The primary_response_form drives the
    'generate quote' flow — must be 704b, not 703b."""
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    cchcs = DEFAULT_AGENCY_CONFIGS["cchcs"]
    assert cchcs["primary_response_form"] == "704b"
