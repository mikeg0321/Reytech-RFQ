"""CR-5 regression guard: routes_deadlines.py api_deadlines and
api_deadlines_critical filtered `is_test` on the Price Check loop but
not on the RFQ loop. A test RFQ with a due date within 4 hours would
hit base.html's hard-alert modal, which blocks the entire UI.

Fix: mirror the PC is_test skip inside both RFQ loops.

GRILL-Q3 refactor (2026-04-22): the PC + RFQ loops were consolidated
into `_scan_deadlines()`, which is now shared by both endpoints AND
the new deadline-escalation watcher. The is_test filter still runs
on every RFQ, but it now lives in the shared helper instead of being
duplicated in each endpoint. Tests updated to match.
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


def _helper_body() -> str:
    """Return the body of `_scan_deadlines` — where CR-5 filters now live."""
    src = ROUTES_DEADLINES.read_text(encoding="utf-8")
    m = re.search(
        r"def _scan_deadlines\([^)]*\)[\s\S]*?(?=\n@bp\.route|\ndef [a-zA-Z_]|\Z)",
        src,
    )
    assert m, "_scan_deadlines() body not located — GRILL-Q3 refactor broken?"
    return m.group(0)


def test_scan_deadlines_rfq_loop_skips_is_test():
    """The shared scan helper's RFQ loop must skip `is_test` records."""
    body = _strip_comment_lines(_helper_body())
    m = re.search(
        r"for\s+rid\s*,\s*r\s+in\s+rfqs\.items\(\)\s*:[\s\S]{0,400}?r\.get\(\s*[\"']is_test[\"']",
        body,
    )
    assert m, (
        "CR-5 regression: _scan_deadlines RFQ loop is missing the "
        "`if r.get('is_test'): continue` skip — test RFQs can still fire "
        "the hard-alert modal and the new deadline-escalation SMS/email."
    )


def test_scan_deadlines_pc_loop_skips_is_test():
    """PC loop parity — test PCs must be skipped too."""
    body = _strip_comment_lines(_helper_body())
    m = re.search(
        r"for\s+pcid\s*,\s*pc\s+in\s+pcs\.items\(\)\s*:[\s\S]{0,400}?pc\.get\(\s*[\"']is_test[\"']",
        body,
    )
    assert m, (
        "CR-5 regression: _scan_deadlines PC loop is missing the "
        "`if pc.get('is_test'): continue` skip."
    )


def test_endpoints_delegate_to_scan_helper():
    """Both endpoints must delegate to _scan_deadlines — no duplicated
    loops (which would re-introduce the risk of one side drifting)."""
    src = _strip_comment_lines(ROUTES_DEADLINES.read_text(encoding="utf-8"))
    # api_deadlines and api_deadlines_critical must both call the helper.
    assert re.search(r"def api_deadlines\(\)[\s\S]*?_scan_deadlines\(", src), (
        "api_deadlines must call _scan_deadlines (CR-5 via shared filter)."
    )
    assert re.search(
        r"def api_deadlines_critical\(\)[\s\S]*?_scan_deadlines\(", src
    ), "api_deadlines_critical must call _scan_deadlines."


def test_module_still_compiles():
    import py_compile
    py_compile.compile(str(ROUTES_DEADLINES), doraise=True)
