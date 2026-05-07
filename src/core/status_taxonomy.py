"""Canonical status taxonomy for PC and RFQ records.

Tier 1c (audit 2026-05-07): collapse 5 inline `valid = {...}` whitelists
scattered across route modules into one predicate. Before this module:

  - routes_pricecheck.py:385         (PC change-status, 15 strings)
  - routes_pricecheck_admin.py:5804  (PC admin update-status, 10 strings)
  - routes_pricecheck_pricing.py:350 (PC dismiss, 7 strings, narrower)
  - routes_rfq_admin.py:154          (RFQ JSON update-status, 9 strings)
  - routes_rfq_admin.py:194          (RFQ form-post update-status, 9 strings)

All five literals were drifting independently. The two PC broad whitelists
already disagreed: the change-status route accepted `pending_award`,
`archived`, `duplicate`, `completed`, `converted` — the admin route did
not. Collapsing them to one canonical union is the audit's intent.

The dismiss endpoint (`PC_DISMISSAL_STATUSES`) is intentionally narrower
than the union — it routes "did not respond / archive / duplicate" UI
clicks, not arbitrary status changes. Keep it as a distinct subset.
"""
from __future__ import annotations

from typing import FrozenSet


# Union of all PC statuses any route currently accepts.
PC_VALID_STATUSES: FrozenSet[str] = frozenset({
    "new",
    "parsed",
    "draft",
    "ready",
    "priced",
    "sent",
    "pending_award",
    "won",
    "lost",
    "no_response",
    "archived",
    "duplicate",
    "completed",
    "converted",
    "expired",
    "not_responding",
    "dismissed",
})


# Narrow subset for the dismiss endpoint — preserves prior intent of
# routes_pricecheck_pricing.py:350. Operators reach this via the
# "Did not respond / Archive / Duplicate" UI buttons; arbitrary statuses
# (e.g. "priced", "sent") would be wrong here.
PC_DISMISSAL_STATUSES: FrozenSet[str] = frozenset({
    "not_responding",
    "dismissed",
    "archived",
    "duplicate",
    "no_response",
    "won",
    "lost",
})


# RFQ statuses — both admin endpoints already shared this set verbatim.
RFQ_VALID_STATUSES: FrozenSet[str] = frozenset({
    "new",
    "ready",
    "generated",
    "ready_to_send",
    "sent",
    "won",
    "lost",
    "no_bid",
    "cancelled",
})


def is_valid_status_for(record_type: str, status: str) -> bool:
    """Return True iff `status` is a recognized status for `record_type`.

    `record_type` is "pc" or "rfq" (case-insensitive). Unknown record
    types return False rather than raising — callers historically just
    rejected with a 4xx and we preserve that shape.
    """
    rt = (record_type or "").strip().lower()
    s = (status or "").strip().lower()
    if not s:
        return False
    if rt == "pc":
        return s in PC_VALID_STATUSES
    if rt == "rfq":
        return s in RFQ_VALID_STATUSES
    return False


def valid_statuses_for(record_type: str) -> FrozenSet[str]:
    """Return the canonical set for `record_type`, or empty if unknown.

    Useful for error messages: callers can include the sorted list so
    the operator sees what's actually accepted.
    """
    rt = (record_type or "").strip().lower()
    if rt == "pc":
        return PC_VALID_STATUSES
    if rt == "rfq":
        return RFQ_VALID_STATUSES
    return frozenset()
