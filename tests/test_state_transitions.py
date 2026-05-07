"""Pin the RFQ status state machine against the silent-stuck bug
class that Mike P0 2026-05-06 RFQ a5b09b56 hit.

Symptom: package generator emits `_transition_status(r, "generated_incomplete")`
when QA fails or capacity overflow blocks fill. The state machine had
no row for `generated_incomplete` and no source state listed it as a
legal target → transition was silently blocked at
`_transition_status` → status stuck at the prior value (`ready` /
`generated`) while the underlying record IS incomplete.

Result: review-package screen shows "Unusual status change: ready ->
generated_incomplete" + the operator can't tell whether the package is
actually generated or just-pretend-generated. Fix: every state from
which the package generator can run must allow `generated_incomplete`
as a legal target, and `generated_incomplete` itself needs forward
transitions for the operator's recovery paths.
"""
from __future__ import annotations

import pytest

from src.core.quote_validator import VALID_TRANSITIONS, validate_transition


# ─── Pin every source-state that can reach generated_incomplete ───────


@pytest.mark.parametrize("from_status", [
    "draft", "parsed", "auto_priced", "priced", "ready", "generated",
])
def test_generated_incomplete_is_reachable_from_every_pre_send_state(from_status):
    """The package generator runs from any pre-send state and may emit
    `generated_incomplete` when the package fails QA or capacity. Each
    source state's allowed-targets list MUST include
    `generated_incomplete`."""
    result = validate_transition(from_status, "generated_incomplete")
    assert result["ok"], (
        f"State machine blocks {from_status} -> generated_incomplete: "
        f"{result.get('error')}"
    )


# ─── Forward paths from generated_incomplete ─────────────────────────


def test_generated_incomplete_has_a_row():
    """Without its own row, the operator can't transition OUT of
    generated_incomplete via the validator — every recovery path
    would fail."""
    assert "generated_incomplete" in VALID_TRANSITIONS, (
        "generated_incomplete missing from VALID_TRANSITIONS — operator "
        "has no validated recovery paths once a package lands here"
    )


@pytest.mark.parametrize("target", [
    "generated",       # regenerate succeeds
    "sent",            # mark sent manually
    "ready",           # back to edit
    "priced",          # back further
    "draft",           # back to draft
    "dismissed",       # cancel
])
def test_generated_incomplete_can_recover_to(target):
    result = validate_transition("generated_incomplete", target)
    assert result["ok"], (
        f"generated_incomplete cannot transition to {target}: "
        f"{result.get('error')} — operator stuck"
    )


# ─── Idempotent re-emit (regenerate again, still incomplete) ─────────


def test_generated_incomplete_idempotent_self():
    """If the operator regenerates and the package is still incomplete,
    the second generation re-emits `generated_incomplete`. Self-loop must
    be allowed or every retry would silently fail validation."""
    result = validate_transition("generated_incomplete", "generated_incomplete")
    assert result["ok"], result.get("error")


# ─── Regression — old paths still work ────────────────────────────────


def test_existing_paths_unchanged():
    """Sanity check: the new generated_incomplete entries are additive,
    not replacing existing paths."""
    # Original spec rows.
    assert validate_transition("priced", "ready")["ok"]
    assert validate_transition("ready", "generated")["ok"]
    assert validate_transition("generated", "sent")["ok"]
    assert validate_transition("sent", "won")["ok"]
    assert validate_transition("sent", "lost")["ok"]
    assert validate_transition("dismissed", "draft")["ok"]


def test_invalid_transitions_still_blocked():
    """Don't accidentally open up illegal transitions. e.g.
    `won → draft` is nonsensical."""
    assert not validate_transition("won", "draft")["ok"]
    assert not validate_transition("lost", "new")["ok"]
    assert not validate_transition("new", "sent")["ok"]


# ─── The exact log line from Mike's incident ──────────────────────────


def test_ready_to_generated_incomplete_was_the_bug():
    """The exact transition that fired BLOCKED in Mike's 2026-05-06
    log: `BLOCKED transition: ready -> generated_incomplete`. After this
    fix it must be allowed."""
    result = validate_transition("ready", "generated_incomplete")
    assert result["ok"], (
        "This is the exact incident transition. If this fails the fix "
        "is regressed."
    )


def test_generated_to_generated_incomplete_was_also_the_bug():
    """Second incident shape: `BLOCKED transition: generated ->
    generated_incomplete` (regenerate landed in incomplete)."""
    result = validate_transition("generated", "generated_incomplete")
    assert result["ok"]
