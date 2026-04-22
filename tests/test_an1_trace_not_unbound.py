"""AN-1 regression guard: the `trace` variable in rfq_relink_pc must be
bound BEFORE the inline-pricing loop, not after.

Audited 2026-04-22 — the inline-pricing loop's `except Exception as pe:
trace.append(...)` handler referenced `trace` before it was initialized
(the `trace = []` line was below the loop). Any inner-try failure on the
first iteration hit UnboundLocalError → outer except → 500.

Same class as the CLAUDE.md "Never Reference Variables Across try/except
Boundaries" rule from the 2026-04-03 incident.
"""
from __future__ import annotations

import re
from pathlib import Path


ROUTES_ANALYTICS = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "api"
    / "modules"
    / "routes_analytics.py"
)


def test_trace_initialized_before_inline_pricing_loop():
    """`trace = []` must come BEFORE any `trace.append(...)` inside the
    rfq_relink_pc outer try/except body."""
    src = ROUTES_ANALYTICS.read_text(encoding="utf-8")
    # Find the outer try block of the relink path.
    m = re.search(
        r"# Run the existing linkage function\s*\n\s*try:[\s\S]*?\n    except Exception as e:",
        src,
    )
    assert m, "rfq_relink_pc outer try block not found"
    body = m.group(0)

    # Look for the actual initializer line (not a substring inside a
    # comment). The initializer is at module indent +2 levels = 8 spaces.
    init_match = re.search(r"\n        trace = \[\]\n", body)
    # Match only actual code lines — exclude lines whose first
    # non-whitespace char is `#` (comments mentioning trace.append).
    # Iterate lines because regex-level comment exclusion backtracks.
    append_offset = -1
    offset = 0
    for line in body.split("\n"):
        stripped = line.lstrip()
        if (
            stripped
            and not stripped.startswith("#")
            and "trace.append(" in line
        ):
            append_offset = offset
            break
        offset += len(line) + 1  # +1 for the newline
    append_match = append_offset if append_offset >= 0 else None
    assert init_match, (
        "AN-1 regression: `trace = []` initializer is missing from the "
        "relink try block (looking for line at 8-space indent)."
    )
    assert append_match is not None, (
        "AN-1 sanity: `trace.append(...)` call not found — was the "
        "inline-pricing error handler removed?"
    )
    assert init_match.start() < append_match, (
        "AN-1 regression: `trace = []` must precede every "
        "`trace.append(...)` call in the relink try block. Otherwise the "
        "inline-pricing error path hits UnboundLocalError on trace."
    )


def test_no_duplicate_trace_init_after_loop():
    """The original code had two `trace = []` lines — one after the
    inline-pricing loop that re-set the accumulated trace back to [].
    The fix removes that second init; guard it never comes back."""
    src = ROUTES_ANALYTICS.read_text(encoding="utf-8")
    m = re.search(
        r"# Run the existing linkage function\s*\n\s*try:[\s\S]*?\n    except Exception as e:",
        src,
    )
    assert m, "rfq_relink_pc outer try block not found"
    body = m.group(0)
    # Count actual init lines, not substring matches inside comments.
    count = len(re.findall(r"\n        trace = \[\]\n", body))
    assert count == 1, (
        f"AN-1 regression: found {count} `trace = []` initializer lines in "
        "the relink try block (expected 1). A second one after the "
        "inline-pricing loop would wipe the error breadcrumbs the loop "
        "just recorded."
    )
