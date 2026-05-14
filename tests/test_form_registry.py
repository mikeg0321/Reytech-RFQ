"""form_registry — pin the per-form metadata contract.

PR mr-wolf #4 (Pattern 4 closure). The registry is the single source
of truth for `form_id → (prefix, code_filler, mirror_fallback)`. This
test fixes the invariants downstream consumers (the dispatcher,
mirror_fill, the classifier) depend on so a future PR can't silently
drop a form or flip a prefix without surfacing in the diff.
"""
from __future__ import annotations

from src.forms.form_registry import (
    FormDefinition,
    all_form_ids,
    field_prefix,
    get_form_definition,
    has_code_filler,
    mirror_fallback_form,
)


def test_703a_routes_through_mirror_fill_from_703b():
    """The canonical 703A gap (PVSP 2026-05-13 incident): no code-fill
    exists, mirror-fall-back from prior 703B is the substrate path."""
    fd = get_form_definition("703a")
    assert isinstance(fd, FormDefinition)
    assert fd.field_prefix == "703A_"
    assert fd.code_filler is None
    assert fd.mirror_fallback == "703b"


def test_703b_has_code_filler_and_canonical_prefix():
    fd = get_form_definition("703b")
    assert fd is not None
    assert fd.field_prefix == "703B_"
    assert fd.code_filler == "src.forms.reytech_filler_v4:fill_703b"
    assert fd.mirror_fallback is None  # 703B IS the canonical filler


def test_703c_falls_back_to_703b_when_code_filler_fails():
    """703C and 703B share field-name suffixes — when the 703C code
    filler crashes, mirror-fall-back from a prior 703B is viable."""
    fd = get_form_definition("703c")
    assert fd is not None
    assert fd.field_prefix == "703C_"
    assert fd.mirror_fallback == "703b"


def test_ams708_has_code_filler():
    fd = get_form_definition("ams708")
    assert fd is not None
    assert fd.field_prefix == "708_"
    assert fd.code_filler == "src.forms.reytech_filler_v4:fill_genai_708"


def test_704b_and_bidpkg_are_unprefixed():
    """704B uses Row1/Row1_2/etc. with no slot prefix. BidPackage
    aggregates many sub-forms; treat as unprefixed at the registry
    surface."""
    assert field_prefix("704b") == ""
    assert field_prefix("bidpkg") == ""


def test_field_prefix_accepts_case_insensitive_input():
    assert field_prefix("703A") == "703A_"
    assert field_prefix("  703a  ") == "703A_"
    assert field_prefix("703B") == "703B_"


def test_mirror_fallback_form_returns_none_for_canonical_forms():
    """Forms that ARE the canonical filler (703B, 704B, bidpkg, etc.)
    don't have a fallback — they are the fallback for siblings."""
    assert mirror_fallback_form("703b") is None
    assert mirror_fallback_form("704b") is None
    assert mirror_fallback_form("bidpkg") is None


def test_has_code_filler_distinguishes_code_path_from_mirror_path():
    """The dispatcher needs to know whether to call a code filler OR
    route through mirror-fill. This predicate is the load-bearing
    branch."""
    assert has_code_filler("703b") is True
    assert has_code_filler("704b") is True
    assert has_code_filler("703a") is False   # mirror-fill only
    assert has_code_filler("cv012_cuf") is False  # profile YAML, not code


def test_unregistered_form_returns_none_or_empty():
    """Unknown form IDs must NOT raise — callers downstream
    (classifier returning 'unknown' → dispatcher) need a defined
    sentinel for 'no metadata' rather than KeyError surprises."""
    assert get_form_definition("totally_made_up") is None
    assert get_form_definition("") is None
    assert get_form_definition(None) is None  # type: ignore[arg-type]
    assert field_prefix("totally_made_up") == ""
    assert mirror_fallback_form("totally_made_up") is None
    assert has_code_filler("totally_made_up") is False


def test_all_form_ids_are_stable():
    """The registry exports a stable enumeration — used by ratchet
    tests + the architecture-contract surface. Ids preserve the case
    convention `form_classifier.TEMPLATE_SLOTS` already established
    (`dsh_attA/B/C` mixed-case; everything else lowercase)."""
    ids = all_form_ids()
    assert ids == sorted(ids)  # alphabetical → diff-stable
    # The full set this PR ships with — adding/removing a form should
    # surface here so the diff shows the contract change explicitly.
    assert set(ids) == {
        "703a", "703b", "703c", "704b",
        "ams708", "bidpkg", "cchcs_it_rfq",
        "cv012_cuf", "dsh_attA", "dsh_attB", "dsh_attC",
        "quote",
    }


def test_every_form_with_mirror_fallback_points_to_a_real_registered_form():
    """Invariant: a `mirror_fallback` value must itself be a
    registered form. Catches typos like `mirror_fallback="703B"`
    (wrong case) or `mirror_fallback="ams_708"` (wrong separator) in
    code review."""
    ids = set(all_form_ids())
    for form_id in ids:
        fd = get_form_definition(form_id)
        if fd and fd.mirror_fallback:
            assert fd.mirror_fallback in ids, (
                f"{form_id!r}.mirror_fallback={fd.mirror_fallback!r} "
                f"is not in registry"
            )


def test_every_registered_form_id_has_a_human_label():
    """Telemetry / operator UI / audit logs surface `human_label`.
    Empty labels confuse downstream readers."""
    for form_id in all_form_ids():
        fd = get_form_definition(form_id)
        assert fd and fd.human_label, f"{form_id!r} missing human_label"
