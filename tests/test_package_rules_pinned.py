"""Regression test pinning Mike's hard package rules from CLAUDE.md.

These rules have caused multiple production incidents when violated.
The audit of `agency_config.py` against CLAUDE.md was previously
informal — this file makes the rules executable.

Rules pinned:

1. **CCHCS / CDCR**: required_forms = one of {703A, 703B, 703C}
   + 704B + Bid Package + Quote. The rev-aware filter at
   `routes_rfq_gen.py` picks the present 703 revision at render time
   (only the one the buyer attached renders). DVBE 843 and seller's
   permit are INSIDE the bid package — never generate as standalone
   required forms. (CLAUDE.md "Form Filling Guard Rails > Package
   Generation" + §0 Job #1 acceptance updated 2026-05-27 to admit 703A
   Rev. 03/2025 as the current revision.)

2. **Optional forms are OPTIONAL.** Never auto-include based on item
   count or heuristics. Optional forms must live in `optional_forms`,
   not `required_forms`.

3. **All three 703 revisions are required_forms candidates.** Reframed
   2026-05-27 from the prior "703B required + 703C optional" model
   after Coleman sol# 10842771 surfaced 703A Rev. 03/2025. The
   rev-aware filter handles the "never both" rule structurally —
   each revision is listed so the filter has something to pick from
   on a non-default buyer attachment. The prior "optional_forms must
   include 703c" pin is replaced by the rev-aware filter test below.
"""
from __future__ import annotations


def test_cchcs_required_forms_match_claude_md_rule():
    """CCHCS package = (703A | 703B | 703C) + 704B + Bid Package + Quote.

    All three 703 revisions appear in required_forms; rev-aware filter
    at the render seam drops the missing-revision siblings so only the
    present one ships in the package. The buyer chooses which revision
    to attach; we render only that one.
    """
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    cchcs = DEFAULT_AGENCY_CONFIGS["cchcs"]
    required = cchcs["required_forms"]

    # Required must be the rev-aware 703 trio + 704B + bidpkg + quote.
    # Adding or removing a form here is a real package change and must
    # be intentional. Update both this test and CLAUDE.md "Form
    # Filling Guard Rails" together.
    assert set(required) == {"703a", "703b", "703c", "704b", "bidpkg", "quote"}, (
        f"CCHCS required_forms must be the 703-revision trio "
        f"(703a/703b/703c) + 704b + bidpkg + quote, got: {required}. "
        f"CLAUDE.md states 'CCHCS package = 703B/C + 704B + Bid Package "
        f"+ Quote ONLY' and the §0 Job #1 amendment 2026-05-27 admitted "
        f"703A. The rev-aware filter at routes_rfq_gen.py picks the "
        f"buyer's attached revision; the others are silently dropped."
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


def test_cchcs_703_revisions_all_in_required_not_optional():
    """All three 703 revisions (A/B/C) live in required_forms.

    Reframed 2026-05-27 from prior "703B required + 703C optional"
    model after Coleman sol# 10842771 surfaced 703A Rev. 03/2025 as
    the current revision. The rev-aware filter at routes_rfq_gen.py
    picks the present revision at render time from `_uploaded_tmpls`
    and drops the siblings, so only the one the buyer attached
    actually renders — the "never both" guarantee is structural now,
    not enforced by separation into required vs optional.
    """
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    cchcs = DEFAULT_AGENCY_CONFIGS["cchcs"]
    required = set(cchcs["required_forms"])
    optional = set(cchcs.get("optional_forms", []))
    for slot in ("703a", "703b", "703c"):
        assert slot in required, (
            f"{slot} must be in CCHCS required_forms — buyer revisions "
            f"are picked by the rev-aware filter, not pre-screened by "
            f"required/optional separation"
        )
        assert slot not in optional, (
            f"{slot} must NOT also be in optional_forms — would duplicate "
            f"the iteration and break the rev-aware filter"
        )


def test_cchcs_rev_aware_filter_present_at_render_seam():
    """The render seam must have a rev-aware 703 filter that drops
    sibling revisions before iterating required_forms.

    Without this filter, listing 703a/703b/703c in required_forms
    would produce three empty 703 PDFs in the package (the buyer
    only attached one). The filter at routes_rfq_gen.py looks for
    the present revision in `_uploaded_tmpls` and prunes the
    others from `_req_forms_raw`.
    """
    from pathlib import Path
    REPO_ROOT = Path(__file__).resolve().parents[1]
    TARGET = REPO_ROOT / "src" / "api" / "modules" / "routes_rfq_gen.py"
    src = TARGET.read_text(encoding="utf-8")
    assert "_present_703" in src, (
        "rev-aware filter must compute the present 703 revision from "
        "_uploaded_tmpls — required after admitting 703A to required_forms"
    )
    assert 'for slot in ("703a", "703b", "703c")' in src, (
        "rev-aware filter must iterate all three 703 slots when picking "
        "the present revision"
    )


def test_cchcs_primary_response_form_is_704b():
    """The 704B is Reytech's actual response form (priced). 703B/C is
    supplier registration. The primary_response_form drives the
    'generate quote' flow — must be 704b, not 703b."""
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    cchcs = DEFAULT_AGENCY_CONFIGS["cchcs"]
    assert cchcs["primary_response_form"] == "704b"
