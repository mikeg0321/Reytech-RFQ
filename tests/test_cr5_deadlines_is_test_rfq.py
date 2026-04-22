"""CR-5 regression guard: routes_deadlines.py api_deadlines and
api_deadlines_critical filtered `is_test` on the Price Check loop but
not on the RFQ loop. A test RFQ with a due date within 4 hours would
hit base.html's hard-alert modal, which blocks the entire UI.

Fix: mirror the PC is_test skip inside both RFQ loops.
"""
from __future__ import annotations

import re
from pathlib import Path


ROUTES_DEADLINES = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "api"
    / "modules"
    / "routes_deadlines.py"
)


def _strip_comment_lines(src: str) -> str:
    kept = []
    for line in src.splitlines():
        if line.lstrip().startswith("#"):
            continue
        kept.append(line)
    return "\n".join(kept)


def _endpoint_body(name: str) -> str:
    """Return the body of a named function up to the next top-level def."""
    src = ROUTES_DEADLINES.read_text(encoding="utf-8")
    m = re.search(
        rf"def {name}\(\)[\s\S]*?(?=\n@bp\.route|\ndef [a-zA-Z_]|\Z)",
        src,
    )
    assert m, f"{name}() body not located"
    return m.group(0)


def test_api_deadlines_rfq_loop_skips_is_test():
    """api_deadlines RFQ loop must skip `r.get('is_test')` records."""
    body = _strip_comment_lines(_endpoint_body("api_deadlines"))
    # Locate the RFQ loop and verify it contains an is_test skip.
    m = re.search(
        r"for\s+rid\s*,\s*r\s+in\s+rfqs\.items\(\)\s*:[\s\S]{0,400}?r\.get\(\s*[\"']is_test[\"']",
        body,
    )
    assert m, (
        "CR-5 regression: api_deadlines RFQ loop is missing the "
        "`if r.get('is_test'): continue` skip."
    )


def test_api_deadlines_critical_rfq_loop_skips_is_test():
    """api_deadlines_critical RFQ loop must also skip is_test rows."""
    body = _strip_comment_lines(_endpoint_body("api_deadlines_critical"))
    m = re.search(
        r"for\s+rid\s*,\s*r\s+in\s+rfqs\.items\(\)\s*:[\s\S]{0,400}?r\.get\(\s*[\"']is_test[\"']",
        body,
    )
    assert m, (
        "CR-5 regression: api_deadlines_critical RFQ loop is missing the "
        "`if r.get('is_test'): continue` skip — test RFQs can still fire "
        "the hard-alert modal."
    )


def test_pc_and_rfq_loops_have_symmetric_filters():
    """Both endpoints had symmetric PC filters; the RFQ loop must now
    mirror those filters (status-sent + is_test)."""
    src = ROUTES_DEADLINES.read_text(encoding="utf-8")
    stripped = _strip_comment_lines(src)
    # Count the is_test guard occurrences inside for-loops. Expect >= 4:
    # one PC + one RFQ per endpoint, across two endpoints.
    guards = re.findall(
        r"\.get\(\s*[\"']is_test[\"']\s*\)",
        stripped,
    )
    assert len(guards) >= 4, (
        f"CR-5 regression: only {len(guards)} `.get('is_test')` guard(s) "
        "present; expected >=4 (PC + RFQ in both api_deadlines and "
        "api_deadlines_critical)."
    )


def test_module_still_compiles():
    import py_compile
    py_compile.compile(str(ROUTES_DEADLINES), doraise=True)
