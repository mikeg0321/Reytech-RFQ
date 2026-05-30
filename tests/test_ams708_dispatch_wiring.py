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
