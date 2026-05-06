"""Regression: Step 4b Grok must not overwrite operator-pasted item_link.

Sibling to the cost-overwrite fix in PR #781. Same failure class:
auto-source confidence describes the source's own answer, never licenses
overwriting an operator-supplied value.

Old buggy code (`pc_enrichment_pipeline.py`):
    if _url and (not it.get("item_link") or _grok_conf > _best_conf):
        it["item_link"] = _url
        ...

Failure: operator paste a Staples URL on row 3, Step 4b's Grok pass found
a different supplier (e.g. Amazon) with higher self-confidence than the
catalog/SCPRS match — the operator's pasted Staples URL silently became
the Amazon URL. Same shape that PR #777 closed for the cost cell.

Fix: gate purely on emptiness, like cost — `if _url and not it.get("item_link"):`
"""
from __future__ import annotations

import os


def _read_src(path: str) -> str:
    full = os.path.join(os.path.dirname(__file__), "..", path)
    with open(full, encoding="utf-8") as f:
        return f.read()


def test_step_4b_never_overwrites_operator_item_link():
    """The Step 4b apply-url block must only fill item_link when slot is
    empty. The old `_grok_conf > _best_conf` clause is gone."""
    src = _read_src("src/agents/pc_enrichment_pipeline.py")

    # Old buggy form must not exist
    assert 'not it.get("item_link") or _grok_conf > _best_conf' not in src, (
        "Step 4b item_link fill must not gate on confidence comparison — "
        "operator URL is ground truth, never overridden by AUTO confidence"
    )

    # The Step 4b apply-url block has a sentinel comment marking the
    # operator-protection rule. That comment plus the empty-slot gate
    # must appear together — distinguishes from the Step 1.6 UPC-resolved
    # block at line ~355 which already had the correct shape.
    assert "Same operator-protection rule as cost: only fill item_link" in src, (
        "Step 4b item_link gate must carry the operator-protection comment"
    )
    # And immediately after that comment, the empty-slot gate appears
    sentinel = "Same operator-protection rule as cost: only fill item_link"
    sentinel_idx = src.index(sentinel)
    next_300 = src[sentinel_idx:sentinel_idx + 500]
    assert 'if _url and not it.get("item_link"):' in next_300, (
        "Step 4b empty-slot gate must immediately follow the sentinel comment"
    )
