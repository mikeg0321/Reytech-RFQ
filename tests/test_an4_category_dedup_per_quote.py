"""AN-4 regression guard: api_win_loss_analysis() inflated by_category
outcome counters because they incremented per ITEM inside a quote loop.

Audited 2026-04-22. Bug shape:

    for q in quotes:
        status = q.get("status", "pending")
        ...
        for item in (q.get("items_detail") or []):
            ...decide cat...
            by_category[cat][outcome] += 1        # <- per-item, WRONG
            by_category[cat]["value"] += item.line_value

A won quote with 5 Medical items would contribute 5 wins to the Medical
bucket. Worse, if its 5 items spanned 3 categories, the SAME quote would
also contribute 1 win to EACH of those 3 buckets — sum(by_category[*].won)
exceeded the real won count. Same shape for lost/pending.

Fix: track categories already charged by this quote in a set; only
increment the outcome counter on first encounter. Value still accumulates
per-item, since line value IS the intended per-category aggregation.
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


def _strip_comment_lines(src: str) -> str:
    kept = []
    for line in src.splitlines():
        if line.lstrip().startswith("#"):
            continue
        kept.append(line)
    return "\n".join(kept)


def _win_loss_body() -> str:
    """Return the body of api_win_loss_analysis() as a single string."""
    src = ROUTES_ANALYTICS.read_text(encoding="utf-8")
    m = re.search(
        r"def api_win_loss_analysis\(\)[\s\S]*?\n(?=\n@bp\.route|\ndef [a-zA-Z_])",
        src,
    )
    assert m, "api_win_loss_analysis() body not located"
    return m.group(0)


def test_by_category_dedupes_per_quote():
    """After the fix, the items-loop must consult a per-quote set before
    incrementing by_category outcome counters."""
    body = _win_loss_body()
    stripped = _strip_comment_lines(body)
    # The fix introduces a set reset once per quote, then a membership
    # check before incrementing. Guard both halves.
    has_reset = re.search(
        r"_cats_counted_this_quote\s*=\s*set\(\s*\)",
        stripped,
    )
    assert has_reset, (
        "AN-4 regression: the per-quote dedup set `_cats_counted_this_quote "
        "= set()` is missing. Outcome counters will inflate again."
    )
    has_guard = re.search(
        r"if\s+cat\s+not\s+in\s+_cats_counted_this_quote\s*:",
        stripped,
    )
    assert has_guard, (
        "AN-4 regression: the `if cat not in _cats_counted_this_quote:` "
        "guard around the by_category outcome increment is missing."
    )


def test_by_category_value_still_per_item():
    """Value aggregation (line total per category) is intentional and must
    stay outside the dedup guard — each item adds its line value."""
    body = _win_loss_body()
    stripped = _strip_comment_lines(body)
    # Value accumulation line must still exist, and must NOT be indented
    # under the `if cat not in ...` guard (we test this by ensuring the
    # value line has the same leading indent as the guard line itself).
    m = re.search(
        r"(?m)^(?P<lead>\s+)if\s+cat\s+not\s+in\s+_cats_counted_this_quote\s*:\n"
        r"(?:\s*.*\n){1,3}"
        r"(?P<value_lead>\s+)by_category\[cat\]\[\"value\"\]\s*\+=",
        stripped,
    )
    assert m, (
        "AN-4: could not locate by_category value accumulation relative to "
        "the dedup guard."
    )
    assert m.group("lead") == m.group("value_lead"), (
        "AN-4 regression: `by_category[cat]['value'] += ...` is now indented "
        "under the dedup guard. Line value must accumulate per-item, not "
        "once-per-quote-per-category."
    )


def test_no_unguarded_by_category_outcome_increment():
    """The old unguarded `by_category[cat][outcome] = ... + 1` shape must
    be gone — increments must live under the dedup guard."""
    body = _win_loss_body()
    stripped = _strip_comment_lines(body)
    # Find each increment site and verify it appears AFTER a dedup guard.
    # Shape we banned: two-line assignment form with no preceding guard on
    # the same pass through the loop.
    # We just assert that every `by_category[cat][` outcome-read lives
    # within 6 lines below a `_cats_counted_this_quote` guard.
    outcome_increments = list(re.finditer(
        r"by_category\[cat\]\[(?:_outcome_key|status[^\]]*)\]\s*=",
        stripped,
    ))
    assert outcome_increments, (
        "Expected at least one by_category outcome increment after the fix."
    )
    for m in outcome_increments:
        window_start = max(0, m.start() - 400)
        window = stripped[window_start:m.end()]
        assert "_cats_counted_this_quote" in window, (
            "AN-4 regression: by_category outcome increment at offset "
            f"{m.start()} is not guarded by `_cats_counted_this_quote`."
        )


def test_an4_module_still_compiles():
    import py_compile
    py_compile.compile(str(ROUTES_ANALYTICS), doraise=True)
