"""BUILD-11 P1 regression guard — process_reply_signal must calibrate.

`quote_lifecycle.process_reply_signal` is the fifth manual close path,
called by:
  1. `close_lost_to_competitor` (award_monitor → SCPRS competitor win)
  2. reply-analyzer agents that detect win/loss signals from buyer replies

Prior to BUILD-11 this path marked the quote `status='won'` or `'lost'`
but never fed `calibrate_from_outcome`. So the oracle EMAs over
sample_size / win_count / avg_winning_margin missed every reply-driven
close. BUILD-1 closed api_rfq_outcome (UI mark-won/lost), BUILD-6
closed the day-45 expired sweep, BUILD-9 closed 3 manual-won paths —
this was the remaining gap.

The fix follows the same shape as BUILD-9:
  * Capture `rowcount` from the status UPDATE (BUILD-6 idempotency)
  * Gate the UPDATE with `status NOT IN ('won','lost','expired','cancelled')`
    so replays can't re-fire calibrate into the EMA
  * Call calibrate AFTER the `with get_db() as conn:` block exits so the
    outer write lock releases before calibrate opens its own connection
    (BUILD-5 lock-contention pattern)
  * Calibrate on BOTH win AND loss branches — the loss branch is what
    feeds `loss_reason` into the oracle

This test locks the source-level guards.
"""
from __future__ import annotations

import re
from pathlib import Path


def _read(p: str) -> str:
    return Path(p).read_text(encoding="utf-8")


# ─── Calibrate presence ─────────────────────────────────────────────────

def test_reply_signal_calibrates_both_outcomes():
    """process_reply_signal must call calibrate_from_outcome with BOTH
    outcome='won' AND outcome='lost' — one per branch. Verified via the
    outcome variable stashed during the branch, then passed to
    calibrate_from_outcome positionally below."""
    src = _read("src/agents/quote_lifecycle.py")
    # The function must import + call calibrate_from_outcome
    assert "from src.core.pricing_oracle_v2 import calibrate_from_outcome" in src, (
        "BUILD-11: quote_lifecycle.py must import calibrate_from_outcome"
    )
    assert "calibrate_from_outcome(" in src, (
        "BUILD-11: quote_lifecycle.py must call calibrate_from_outcome"
    )
    # Both branches must stash _calibrate_outcome = "won" and = "lost"
    assert re.search(r'_calibrate_outcome\s*=\s*"won"', src), (
        "BUILD-11: win branch must stash _calibrate_outcome = \"won\""
    )
    assert re.search(r'_calibrate_outcome\s*=\s*"lost"', src), (
        "BUILD-11: loss branch must stash _calibrate_outcome = \"lost\" "
        "(note: calibrate_from_outcome uses \"lost\" not \"loss\" — see "
        "src/core/pricing_oracle_v2.py:1679 docstring and "
        "oracle_weekly_report.py:122)"
    )


# ─── Idempotency + lock-contention guards ───────────────────────────────

def test_reply_signal_rowcount_gated():
    """Each calibrate-stashing branch must be gated on `_rowcount > 0`
    so replays of an already-closed quote can't re-fire calibrate."""
    src = _read("src/agents/quote_lifecycle.py")
    # Count occurrences of `if _rowcount > 0:` near the stash lines
    gates = re.findall(r"if\s+_rowcount\s*>\s*0\s*:", src)
    assert len(gates) >= 2, (
        f"BUILD-11: expected at least 2 `if _rowcount > 0:` gates in "
        f"process_reply_signal (one per branch), found {len(gates)}. "
        f"Without the gate, replayed win/loss signals would double-count "
        f"into the oracle EMA."
    )


def test_reply_signal_update_gated_against_replay():
    """The status UPDATE on both win and loss branches must filter
    `status NOT IN ('won','lost','expired','cancelled')` so rowcount
    reflects only real transitions. This is the BUILD-6 idempotency
    pattern — without it, rowcount could be >0 on a no-op update."""
    src = _read("src/agents/quote_lifecycle.py")
    # Both UPDATE statements must include the status filter in their WHERE
    update_blocks = re.findall(
        r"UPDATE quotes\s+SET status = '(?:won|lost)'.*?WHERE quote_number = \?\s*"
        r"AND status NOT IN \('won','lost','expired','cancelled'\)",
        src, re.DOTALL,
    )
    assert len(update_blocks) >= 2, (
        f"BUILD-11: expected >=2 gated UPDATE statements (one per "
        f"branch), found {len(update_blocks)}. Pattern: "
        f"`UPDATE quotes SET status='won'|'lost' ... WHERE quote_number = ? "
        f"AND status NOT IN (...)`"
    )


def test_reply_signal_calibrate_outside_with_block():
    """The calibrate_from_outcome call must occur AFTER the
    `with get_db() as conn:` block exits, so the outer write lock
    is released before calibrate opens its own connection
    (BUILD-5 pattern).

    Structural proof: `_calibrate_outcome` is assigned INSIDE the
    with-block but used at the top level after it. Verify the
    calibrate call is preceded by the check `if _calibrate_outcome`,
    which structurally can only live outside the with-block."""
    src = _read("src/agents/quote_lifecycle.py")
    # Find the calibrate call
    m = re.search(r"calibrate_from_outcome\(\s*_calibrate_items", src)
    assert m, (
        "BUILD-11: calibrate_from_outcome call with _calibrate_items "
        "positional arg not found — refactor may have dropped it"
    )
    # Walk backwards 500 chars, must find the outer `if _calibrate_outcome`
    window_start = max(0, m.start() - 500)
    window = src[window_start:m.start()]
    assert re.search(
        r"if\s+_calibrate_outcome\s+and\s+_rowcount\s*>\s*0\s*:",
        window,
    ), (
        "BUILD-11: calibrate_from_outcome call must be preceded by "
        "`if _calibrate_outcome and _rowcount > 0:` — structural proof "
        "that it runs after the with-block exits (which is where "
        "_calibrate_outcome becomes readable at top scope)."
    )


# ─── Agency + items payload plumbing ────────────────────────────────────

def test_reply_signal_enriches_select_with_agency_and_items():
    """The initial SELECT from quotes must include agency + line_items +
    total columns — calibrate needs them for agency scoping (BUILD-8) +
    item-level EMA updates."""
    src = _read("src/agents/quote_lifecycle.py")
    # Match the SELECT ... FROM quotes WHERE quote_number = ? inside
    # process_reply_signal (multi-line OK).
    sel = re.search(
        r"def process_reply_signal.*?"
        r"SELECT\s+([^;]*?)FROM quotes WHERE quote_number = \?",
        src, re.DOTALL,
    )
    assert sel, (
        "BUILD-11: process_reply_signal's SELECT from quotes not found"
    )
    cols = sel.group(1)
    for required in ("status", "status_history", "agency", "line_items", "total"):
        assert required in cols, (
            f"BUILD-11: SELECT in process_reply_signal must include "
            f"`{required}` — needed by calibrate_from_outcome. "
            f"Current column list: {cols.strip()[:200]}"
        )
