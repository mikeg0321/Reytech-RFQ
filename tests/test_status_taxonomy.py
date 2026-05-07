"""Tier 1c — `src/core/status_taxonomy.py` (audit 2026-05-07).

Pins the canonical PC + RFQ status sets and the `is_valid_status_for`
predicate. The 5 inline whitelists this PR collapsed are documented in
the module's docstring; each pre-collapse string is asserted to still
be valid (no behavior regression for any single accepted status).

The two PC broad whitelists used to disagree (admin route was missing
pending_award/archived/duplicate/completed/converted). After collapse,
both call into the same union — so the admin route's accepted set
*grows*. We pin that intentional widening explicitly.
"""
from __future__ import annotations

from src.core.status_taxonomy import (
    PC_DISMISSAL_STATUSES,
    PC_VALID_STATUSES,
    RFQ_VALID_STATUSES,
    is_valid_status_for,
    valid_statuses_for,
)


# ── Pin every pre-collapse whitelist string is still valid ───────────

# routes_pricecheck.py:385-387 (15 strings, broadest)
PC_CHANGE_STATUS_LEGACY = {
    "new", "draft", "sent", "pending_award", "won", "lost",
    "no_response", "archived", "duplicate", "completed", "converted",
    "expired", "parsed", "priced", "ready",
}

# routes_pricecheck_admin.py:5804 (10 strings, narrower)
PC_ADMIN_LEGACY = {
    "new", "parsed", "draft", "priced", "ready", "sent", "won", "lost",
    "expired", "no_response",
}

# routes_pricecheck_pricing.py:350 (7 strings, dismiss-only)
PC_DISMISS_LEGACY = {
    "not_responding", "dismissed", "archived", "duplicate",
    "no_response", "won", "lost",
}

# routes_rfq_admin.py:154/194 (9 strings, both endpoints same)
RFQ_ADMIN_LEGACY = {
    "new", "ready", "generated", "ready_to_send", "sent", "won", "lost",
    "no_bid", "cancelled",
}


def test_pc_change_status_legacy_all_still_valid():
    """Every status routes_pricecheck.py used to accept is still valid."""
    for s in PC_CHANGE_STATUS_LEGACY:
        assert is_valid_status_for("pc", s), f"PC change-status lost: {s}"


def test_pc_admin_legacy_all_still_valid():
    """Every status routes_pricecheck_admin.py used to accept is still valid."""
    for s in PC_ADMIN_LEGACY:
        assert is_valid_status_for("pc", s), f"PC admin lost: {s}"


def test_pc_dismiss_legacy_pinned_in_dismissal_set():
    """The narrow dismiss endpoint set is preserved verbatim."""
    assert PC_DISMISSAL_STATUSES == PC_DISMISS_LEGACY


def test_rfq_admin_legacy_all_still_valid():
    """Every status routes_rfq_admin.py used to accept is still valid."""
    for s in RFQ_ADMIN_LEGACY:
        assert is_valid_status_for("rfq", s), f"RFQ admin lost: {s}"


# ── Pin the canonical sets so silent shrinkage trips a test ─────────

def test_pc_valid_statuses_canonical_set():
    """Lock the canonical PC set so any future shrink is intentional.

    Adding a status is a one-line append to this test + the module.
    Removing one needs a deliberate review (might break a route).
    """
    expected = {
        "new", "parsed", "draft", "ready", "priced", "sent",
        "pending_award", "won", "lost", "no_response",
        "archived", "duplicate", "completed", "converted",
        "expired", "not_responding", "dismissed",
    }
    assert set(PC_VALID_STATUSES) == expected


def test_rfq_valid_statuses_canonical_set():
    """Lock the canonical RFQ set."""
    expected = {
        "new", "ready", "generated", "ready_to_send", "sent",
        "won", "lost", "no_bid", "cancelled",
    }
    assert set(RFQ_VALID_STATUSES) == expected


# ── Predicate edge cases ────────────────────────────────────────────

def test_invalid_strings_rejected():
    """Random or near-miss strings must not validate."""
    bad = ["", "  ", "WON", "ready_to_sent", "draft  ", "delivered",
           "Awarded", "in_progress", "n/a", None]
    for b in bad:
        # case + whitespace are normalized; "WON" → "won" is allowed.
        if isinstance(b, str) and b.strip().lower() in PC_VALID_STATUSES:
            continue
        assert not is_valid_status_for("pc", b), f"Should reject: {b!r}"
        assert not is_valid_status_for("rfq", b), f"Should reject: {b!r}"


def test_predicate_normalizes_case_and_whitespace():
    """Operator-typed strings can have stray case or padding."""
    assert is_valid_status_for("pc", "WON")
    assert is_valid_status_for("pc", "  won  ")
    assert is_valid_status_for("RFQ", "ready_to_send")


def test_predicate_rejects_unknown_record_type():
    """Unknown record types return False — never raise."""
    assert not is_valid_status_for("order", "won")
    assert not is_valid_status_for("", "won")
    assert not is_valid_status_for(None, "won")


def test_pc_dismissal_is_subset_of_pc_valid():
    """Every dismiss-status must also be a valid PC status overall."""
    assert PC_DISMISSAL_STATUSES.issubset(PC_VALID_STATUSES)


def test_valid_statuses_for_helper():
    """Helper returns frozenset for known types, empty for unknown."""
    assert valid_statuses_for("pc") == PC_VALID_STATUSES
    assert valid_statuses_for("rfq") == RFQ_VALID_STATUSES
    assert valid_statuses_for("order") == frozenset()
    assert valid_statuses_for("") == frozenset()


# ── Pin the intentional widening of the PC admin route ─────────────

def test_pc_admin_widening_after_collapse():
    """routes_pricecheck_admin.py used to reject these — now accepts.

    This is the audit's intent: collapse the two divergent broad PC
    whitelists into one canonical union. If a future migration wants
    to keep the admin route narrower, it should introduce a separate
    PC_ADMIN_STATUSES subset rather than fork the literal again.
    """
    newly_accepted = {
        "pending_award", "archived", "duplicate",
        "completed", "converted", "not_responding", "dismissed",
    }
    for s in newly_accepted:
        assert is_valid_status_for("pc", s)
        assert s in PC_VALID_STATUSES
        assert s not in PC_ADMIN_LEGACY  # was rejected pre-collapse
