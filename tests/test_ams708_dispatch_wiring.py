"""Anti-drift: the package generator must dispatch the GenAI/AMS 708 form on
EITHER id (`genai_708` or `ams708`), because the form lives in two id-spaces:

  - the form catalog / email-detection / std1000->ams708 swap use `ams708`
  - the generator + blank template + form_registry filler use `genai_708`
    (form_registry maps ams708 -> fill_genai_708)

Before this fix the generator dispatched ONLY `_include("genai_708")`, so a
required-forms list carrying `ams708` (the id email-detection and the swap
produce) silently dropped the 708 — exactly the form CCHCS IT buyer Ashley Russ
requires. This test pins the dual-id dispatch so a future edit can't regress to
the single-id form and re-introduce the silent drop.

Source-level assertion (not a full generate_rfq_package invocation, which needs
templates + DB + network). The dispatch line is the contract.
"""
import re
from pathlib import Path

_GEN = Path(__file__).resolve().parents[1] / "src" / "api" / "modules" / "routes_rfq_gen.py"


def _source() -> str:
    return _GEN.read_text(encoding="utf-8")


def test_708_dispatch_fires_on_either_id():
    src = _source()
    # The single dispatch condition must reference BOTH ids.
    assert '_include("genai_708") or _include("ams708")' in src, (
        "708 dispatch must fire on either genai_708 or ams708 — an ams708-only "
        "required-forms list would otherwise be silently dropped."
    )


def test_no_bare_genai_708_only_dispatch():
    """The pre-fix single-id condition must be gone (anti-regression)."""
    src = _source()
    assert "if _include(\"genai_708\"):" not in src, (
        "found the bare single-id 708 dispatch — it drops ams708 form requests."
    )


def test_708_block_still_records_missing_template_gap():
    """The 708 block must still surface a failure (not silently skip) so a
    missing template / fill error reaches the operator's errors list."""
    src = _source()
    assert 'errors.append(f"GenAI 708: {e}")' in src


def test_708_standalone_guarded_by_bidpkg_inclusion():
    """The 708 lives inside the bid package; a standalone copy must only be
    emitted when the bid package is NOT included, else it double-emits.
    Mirrors the sellers_permit / calrecycle74 guard."""
    src = _source()
    assert (
        '(_include("genai_708") or _include("ams708")) and not _bidpkg_included'
        in src
    ), "standalone 708 must be guarded by `not _bidpkg_included` (no double-emit)"
    assert (
        '(_include("genai_708") or _include("ams708")) and _bidpkg_included'
        in src
    ), "the bidpkg-present branch must log the skip (708 already inside bidpkg)"


def test_708_missing_template_is_not_silently_dropped():
    """A required ams708 that can't render standalone must append an error —
    the pre-fix `if os.path.exists(...)` with no else SILENTLY dropped it,
    the exact class #1263 set out to kill, one level down."""
    src = _source()
    # the old silent-skip pattern (existence-gate on a standalone blank, no else)
    assert 'os.path.join(DATA_DIR, "templates", "genai_708_blank.pdf")' not in src, (
        "the dead genai_708_blank.pdf existence-gate is the silent-drop path — "
        "the standalone 708 is now derived from the bid-package template."
    )
    # the render-failure path must reach the operator
    assert "AMS 708 required standalone but could not be rendered" in src
