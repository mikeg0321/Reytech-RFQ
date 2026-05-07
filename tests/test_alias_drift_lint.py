"""Codebase-wide ratcheting lint: every routes_*.py write to a PC items
alias (`pc["items"]`, `pc["line_items"]`, `pc["parsed"]["line_items"]`)
or an RFQ items alias (`r["line_items"]`, `r["items"]`, `rfq["line_items"]`,
`rfq["items"]`, `rfq_data["line_items"]`, `rfq_data["items"]`) must go
through the canonical sync helper.

**Bug class this closes:** the 2026-05-05 23:10Z pc_177b18e6 incident.
A handler updated `pc["items"]` (2 entries) but `pc["line_items"]`
stayed stale (1 entry). `quote_model_v2_enabled` adapter read the
stale alias → UI row blanked. Mike had to flip `quote_model_v2_enabled`
off prod 23:13Z to mitigate. Memory: feedback_pc_items_line_items_alias_drift.

**Substrate fix shape:** route every alias mutation through
`_sync_pc_items()` / `_sync_rfq_items()` (defined in
`src/api/modules/routes_pricecheck.py`, exec()-shared so they're
callable from any sibling module). Those helpers write all aliases
together so divergence becomes structurally impossible.

**Ratcheting baseline:** the 2026-05-06 audit found N existing
violations (frozen below). Same playbook as the RMW race ratchet (PR
#784): no new violations may land, and fixing one without removing
its baseline entry also fails CI — so the list can only shrink.

**To fix a violation:**
  1. Replace the direct write `pc["items"] = X` with `_sync_pc_items(pc, X)`.
     Replace `r["line_items"] = X` with `_sync_rfq_items(r, X)`.
  2. Remove the entry from KNOWN_VIOLATIONS below.
  3. Run `pytest tests/test_alias_drift_lint.py` — must stay green.

**Item-level mutations (`pc["items"][idx]["price"] = ...`) are NOT
in scope** — those don't replace the list, they edit a single item
in place, and item-level reads see the same object regardless of
which alias is used. Only whole-list replacements drift.
"""
from __future__ import annotations

import re
from pathlib import Path


# Functions exempt from the lint — each one is the canonical writer
# itself, or a defensive sync that lives in load/save infrastructure.
KNOWN_EXEMPTIONS: set[str] = {
    # The canonical helpers themselves. By definition they contain
    # direct alias writes — that's their job.
    "_sync_pc_items",
    "_sync_rfq_items",
}


# Frozen baseline. Each tuple is (filename, function_name) of a
# routes_*.py handler that contains one or more direct alias writes
# bypassing the canonical helpers. The list only shrinks: ship a fix,
# remove the entry. CI fails on additions OR on fixes shipped without
# entry removal — keeps the burndown queue accurate.
KNOWN_VIOLATIONS: frozenset[tuple[str, str]] = frozenset({
    # Captured 2026-05-06 from a fresh `_find_violations()` scan against
    # origin/main. Function names reflect main HEAD at that moment —
    # in-flight wrapper-rename PRs (e.g. RMW batches 5, 7) will rename a
    # subset of these once merged; the lint will then surface them as
    # "fixed but not removed" entries that need a baseline update.
    # 2026-05-07 baseline refresh: RMW lock-wrap PRs renamed three of the
    # original entries by moving the body into `_<name>_locked` inner
    # functions (the outer route is now just a `with _save_*_lock:` wrapper).
    # The inner still writes the alias directly, so the violation moved
    # rather than disappeared. Updated entries below — same handlers, new
    # function names per current main HEAD.
    ("routes_rfq.py", "_api_rfq_upload_parse_doc_locked"),
    ("routes_rfq.py", "detail"),
    ("routes_rfq.py", "upload"),
    ("routes_rfq_gen.py", "_api_rfq_screenshot_confirm_locked"),
    ("routes_rfq_gen.py", "rfq_add_item"),
    ("routes_rfq_gen.py", "rfq_duplicate_item"),
    ("routes_rfq_gen.py", "rfq_move_item"),
    ("routes_rfq_gen.py", "rfq_reset_items"),
    ("routes_rfq_gen.py", "_rfq_upload_supplier_quote_locked"),
    ("routes_rfq_gen.py", "upload_templates"),
})


# Detection regexes. Each matches a whole-list replacement (LHS = list).
# Item-level mutations like `pc["items"][idx]["price"] = ...` are
# excluded — they don't replace the list and don't cause alias drift.
_PC_WRITE_PATTERNS = (
    re.compile(r'\bpc\["items"\]\s*='),
    re.compile(r'\bpc\["line_items"\]\s*='),
    re.compile(r'\bpc\["parsed"\]\["line_items"\]\s*='),
)
_RFQ_WRITE_PATTERNS = (
    re.compile(r'\b(?:r|rfq|rfq_data)\["line_items"\]\s*='),
    re.compile(r'\b(?:r|rfq|rfq_data)\["items"\]\s*='),
)


def _is_item_level_mutation(line: str) -> bool:
    """Skip lines like `pc["items"][idx]["price"] = ...` — those are
    item-level edits, not whole-list replacement, so they don't cause
    alias drift."""
    return bool(re.search(r'\["(?:items|line_items)"\]\[', line))


def _split_into_functions(src: str) -> list[tuple[str, str]]:
    """Yield (function_name, function_body) pairs for top-level defs.

    Mirrors the RMW lint's splitter so the function-resolution logic
    stays consistent across substrate ratchets.
    """
    out = []
    matches = list(re.finditer(r"^def ([a-z_][a-z0-9_]*)\(", src, re.MULTILINE))
    for i, m in enumerate(matches):
        name = m.group(1)
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(src)
        out.append((name, src[start:end]))
    return out


def _function_has_alias_write(body: str) -> bool:
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if _is_item_level_mutation(line):
            continue
        for pat in _PC_WRITE_PATTERNS:
            if pat.search(line):
                return True
        for pat in _RFQ_WRITE_PATTERNS:
            if pat.search(line):
                return True
    return False


def _find_violations() -> set[tuple[str, str]]:
    """Scan routes_*.py and return the set of (filename, function_name)
    tuples whose body contains at least one direct alias write."""
    routes_dir = Path(__file__).parent.parent / "src" / "api" / "modules"
    out: set[tuple[str, str]] = set()
    for py_file in sorted(routes_dir.glob("routes_*.py")):
        text = py_file.read_text(encoding="utf-8")
        for name, body in _split_into_functions(text):
            if name in KNOWN_EXEMPTIONS:
                continue
            if _function_has_alias_write(body):
                out.add((py_file.name, name))
    return out


def test_no_new_alias_drift_handlers():
    """No NEW handlers may write to an items alias directly.

    Existing handlers are listed in KNOWN_VIOLATIONS as the cleanup
    backlog. Adding a handler not in that list (i.e., a new violation)
    fails this test. Removing one (i.e., shipping a fix) without also
    deleting the entry from KNOWN_VIOLATIONS also fails — keeps the
    backlog accurate.
    """
    found = _find_violations()
    new_violations = found - KNOWN_VIOLATIONS
    fixed_but_not_removed = KNOWN_VIOLATIONS - found

    msgs = []
    if new_violations:
        msgs.append(
            "NEW alias-drift violations introduced (handlers writing PC "
            "or RFQ items aliases directly — route the write through "
            "_sync_pc_items() / _sync_rfq_items()):"
        )
        for f, n in sorted(new_violations):
            msgs.append(f"  + {f}::{n}")

    if fixed_but_not_removed:
        msgs.append(
            "\nViolations FIXED but still listed in KNOWN_VIOLATIONS — "
            "delete these entries from the test to record progress:"
        )
        for f, n in sorted(fixed_but_not_removed):
            msgs.append(f"  - {f}::{n}")

    if msgs:
        msgs.insert(0, "")
        raise AssertionError("\n".join(msgs))


def test_baseline_violations_count():
    """Watchful info — surfaces the cleanup backlog size in test output."""
    backlog = len(KNOWN_VIOLATIONS)
    # Initial audit on 2026-05-06: ~38 violations across routes_*.py.
    # Floor at 50 leaves slack for the lint to pick up edge cases the
    # initial regex didn't catch; the assertion exists to make growth
    # visible if someone tries to add new entries.
    assert backlog <= 50, (
        f"Backlog should ratchet down toward 0 over time — got {backlog}. "
        f"If you've added entries, that's a regression: every new handler "
        f"must route alias writes through _sync_pc_items / _sync_rfq_items."
    )
