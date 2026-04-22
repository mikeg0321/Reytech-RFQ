"""Cross-cutting regression guard: Oracle feedback loop must stay wired.

Background incident (Feb 17 → Apr 15): `markQuote()` was a silent no-op.
Every win/loss transition called it, operators saw "Quote marked won"
in logs, but the oracle feedback loop received zero data for 57 days.
When the bug was finally caught, Oracle had 0 real samples and had been
pricing off cold priors the entire time.

This file is the *prevention layer* for that class of bug. Unit tests
on individual layers (calibrate_from_outcome math, ledger dedup, card
rendering) still pass even when the pipe between entry-point and oracle
is cut — so those tests cannot catch a silent-no-op regression on their
own.

Guard strategy:
  1. Enumerate every known user-facing mark-won / mark-lost route and
     every runtime win/loss callback. For each: the function body MUST
     contain a `calibrate_from_outcome(` call. Deleting or unwiring the
     call triggers a named-callsite test failure with the incident
     context attached.
  2. The surrounding `except` block must log at `warning` or `error`
     severity. `log.debug` + swallow is the exact shape the Feb→Apr
     incident used to hide itself. Debug-only logging on the feedback
     loop is forbidden.
  3. A ban on `except Exception: pass` / bare `except: pass` bracketing
     a `calibrate_from_outcome(` call — silent swallow is the incident
     pattern and the reason unit tests passed while prod regressed.

If this file breaks, do NOT suppress the test. Re-wire the feedback
loop at the named callsite, OR — if the callsite was intentionally
removed — remove its entry from `_FEEDBACK_CALLSITES` with a commit
message explaining why the deletion is safe.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import List, Tuple

_REPO = Path(__file__).resolve().parent.parent


# ── Registry of every user-facing win/loss callsite ──────────────────
# (source_path, function_name, reason)
#
# Every tuple here represents a path that changes a quote's final
# disposition (won/lost/shipped) and therefore MUST feed the oracle.
# Adding a new win/loss route? Add it to this list.
_FEEDBACK_CALLSITES: List[Tuple[str, str, str]] = [
    ("src/api/modules/routes_rfq.py", "api_rfq_mark_won",
     "RFQ marked won — primary operator win path"),
    ("src/api/modules/routes_rfq.py", "api_rfq_mark_lost",
     "RFQ marked lost — primary operator loss path"),
    ("src/api/modules/routes_pricecheck_admin.py", "api_pricecheck_mark_won",
     "PC marked won — primary operator PC win path"),
    ("src/api/modules/routes_pricecheck_admin.py", "api_pricecheck_mark_lost",
     "PC marked lost — primary operator PC loss path"),
    ("src/api/modules/routes_pricecheck_admin.py", "api_backfill_wins",
     "PC historical wins backfill — bulk calibration path"),
    ("src/api/modules/routes_pricecheck_admin.py", "api_backfill_losses",
     "PC historical losses backfill — bulk calibration path"),
    ("src/api/modules/routes_growth_intel.py", "api_rfq_outcome",
     "IN-1 fix: growth-intel RFQ outcome wired 2026-04-21 "
     "(same incident shape — routes_growth_intel writes wl_log but "
     "was silent on the oracle side)"),
]


def _slurp(rel: str) -> str:
    return (_REPO / rel).read_text(encoding="utf-8")


def _extract_function_body(src: str, func_name: str) -> str:
    """Return the body text of `def func_name(...):` up to the next
    top-level `def ` or `@bp.route(` at column 0. Good enough to scope
    grep-style checks to one function at a time.
    """
    pattern = rf"^def {re.escape(func_name)}\(.*?\):\n(.*?)(?=^def |^@bp\.route|^@auth_required\ndef |\Z)"
    m = re.search(pattern, src, flags=re.DOTALL | re.MULTILINE)
    assert m is not None, (
        f"Could not locate `def {func_name}(` — function was renamed or "
        f"removed. Update _FEEDBACK_CALLSITES or restore the function."
    )
    return m.group(1)


# ── Guard 1: every known callsite still calls calibrate_from_outcome ─

def test_every_mark_outcome_callsite_calls_calibrate():
    """If a win/loss route exists but has no calibrate_from_outcome
    call in its body, the feedback loop is cut. This is the exact
    shape of the Feb→Apr incident."""
    failures = []
    for rel, func_name, reason in _FEEDBACK_CALLSITES:
        src = _slurp(rel)
        body = _extract_function_body(src, func_name)
        if "calibrate_from_outcome(" not in body:
            failures.append(f"  {rel}::{func_name}  ({reason})")
    assert not failures, (
        "FEEDBACK LOOP REGRESSION — calibrate_from_outcome missing from:\n"
        + "\n".join(failures)
        + "\n\nThis is the Feb→Apr markQuote silent-no-op pattern. "
          "The route logs success but the oracle never learns. "
          "Re-wire the call or remove the callsite from the registry."
    )


# ── Guard 2: no silent swallow around feedback calibration ───────────

_BARE_SWALLOW_PATTERNS = [
    # `except Exception: pass` bracketing a calibrate call
    re.compile(
        r"calibrate_from_outcome\([^)]*\)[^}]*?except\s+Exception[^:]*:\s*\n\s*pass",
        re.DOTALL,
    ),
    # bare `except:` bracketing a calibrate call
    re.compile(
        r"calibrate_from_outcome\([^)]*\)[^}]*?\n\s*except\s*:\s*\n\s*pass",
        re.DOTALL,
    ),
]


def test_no_silent_swallow_around_calibrate_call():
    """No callsite may wrap calibrate_from_outcome in except-pass. The
    whole point of failing calibration is to surface it — if we silence
    it, the next Feb→Apr goes 57 days unnoticed."""
    failures = []
    for rel, func_name, reason in _FEEDBACK_CALLSITES:
        body = _extract_function_body(_slurp(rel), func_name)
        for pat in _BARE_SWALLOW_PATTERNS:
            if pat.search(body):
                failures.append(f"  {rel}::{func_name}  (silent except-pass)")
                break
    assert not failures, (
        "FEEDBACK LOOP REGRESSION — calibrate failures silently swallowed in:\n"
        + "\n".join(failures)
        + "\n\nUse `except Exception as e: log.warning(...)` — never "
          "except-pass. Silent swallow is how the Feb→Apr bug hid."
    )


# ── Guard 3: except block must log at warning or error severity ──────

def test_calibrate_failures_logged_at_visible_severity():
    """A calibrate_from_outcome that fails must be visible in prod
    logs. `log.debug` is invisible at INFO level and hides incidents —
    the Feb→Apr bug would have been caught on day 1 if the no-op had
    logged at warning level."""

    # Allowlist for callsites that deliberately use debug-level logging
    # because the calibrate call is a bulk-loop side effect (e.g. the
    # backfill routes that iterate over hundreds of PCs — a single item
    # failing there is noise, but bulk failure surfaces via the outer
    # handler). Each entry must justify itself in the comment.
    _DEBUG_OK = {
        # Backfill loops: outer log.info reports total; per-item debug
        # is correct because a single bad PC shouldn't spam warn on
        # replay of 200+ historical items.
        ("src/api/modules/routes_pricecheck_admin.py", "api_backfill_wins"),
        ("src/api/modules/routes_pricecheck_admin.py", "api_backfill_losses"),
    }

    failures = []
    for rel, func_name, reason in _FEEDBACK_CALLSITES:
        body = _extract_function_body(_slurp(rel), func_name)

        # Find every (calibrate_from_outcome(...) ... except ... block)
        # and inspect the except body for log severity.
        calibrate_except_block = re.compile(
            r"calibrate_from_outcome\([^)]*\).*?except\s+[\w\s]*?(?:as\s+\w+)?\s*:\s*\n(?P<body>(?:[ \t]+[^\n]*\n)+)",
            re.DOTALL,
        )
        for m in calibrate_except_block.finditer(body):
            except_body = m.group("body")
            has_warning = re.search(r"log\.(warning|error)\b", except_body)
            has_debug_only = re.search(r"log\.debug\b", except_body) and not has_warning
            if has_debug_only and (rel, func_name) not in _DEBUG_OK:
                failures.append(
                    f"  {rel}::{func_name}  (except block uses log.debug only)"
                )
                break

    assert not failures, (
        "FEEDBACK LOOP REGRESSION — calibrate except block uses log.debug only:\n"
        + "\n".join(failures)
        + "\n\nUpgrade to log.warning or log.error. Debug is invisible "
          "at prod INFO level and hides feedback-loop outages."
    )


# ── Guard 4: calibrate_from_outcome signature hasn't been dropped ────

def test_calibrate_from_outcome_still_exists_and_writes_tables():
    """The destination function must still exist with its documented
    signature and still write both oracle_calibration and
    winning_quote_shapes. A refactor that drops the write side is
    equivalent to the Feb→Apr bug — callers fire, nothing persists."""
    src = _slurp("src/core/pricing_oracle_v2.py")

    # Signature check — a rename or signature change means every caller
    # needs to be audited, not just the definition.
    assert re.search(
        r"def calibrate_from_outcome\(items,\s*outcome,\s*agency=.*?loss_reason=.*?winner_prices=",
        src,
    ), (
        "calibrate_from_outcome signature changed. Every callsite in "
        "_FEEDBACK_CALLSITES must be audited for the new signature."
    )

    # Writes calibration table (the read side of the health card)
    assert "oracle_calibration" in src, (
        "pricing_oracle_v2 no longer references oracle_calibration — "
        "the card read path will show stale data."
    )

    # Writes shapes table (the read side of buyer intelligence)
    assert "winning_quote_shapes" in src, (
        "pricing_oracle_v2 no longer references winning_quote_shapes — "
        "buyer intelligence can't learn new shapes."
    )


# ── Guard 5: registry is not silently shrinking ──────────────────────

def test_feedback_callsite_registry_is_not_empty():
    """Paranoia check: if someone deletes entries from the registry
    wholesale to 'fix' a failing guard, this catches the gaming."""
    assert len(_FEEDBACK_CALLSITES) >= 7, (
        f"Registry dropped below 7 callsites ({len(_FEEDBACK_CALLSITES)}). "
        f"If a callsite was genuinely retired, the deletion commit must "
        f"explain why the feedback loop stays intact without it."
    )
