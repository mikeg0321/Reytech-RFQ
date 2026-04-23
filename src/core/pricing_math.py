"""Pure pricing math helpers shared across read + write paths.

Audit PC-1 (project_pc_module_audit_2026_04_21) + confirmed live on prod
2026-04-23: PC save path writes `pricing.unit_cost` and `pricing.markup_pct`
on cost/markup edits. The derivation `unit_price = cost * (1 + markup/100)`
is performed on the fly by the UI but must also be persisted so email
previews + generated PDFs render the same number. PR #321 added
`_recompute_unit_price` on the write path; however records saved BEFORE
#321 shipped still carry a stale `unit_price`. Every time the operator
opens such a record, changes nothing, and clicks Send, the email ships the
stale value while the UI showed the live derivation.

Live evidence (pc_f7ba7a6b, Cortech mattress, 2026-04-23):
  UI:           cost $465.40 × 22% → $567.79/unit
  email body:   "Qty 16 @ $558.48"  ← persisted unit_price is 2-points stale
  gap: $9.31 × 16 = **$148.96 under-quote per send**

This module gives read-path code a canonical-price accessor that prefers
the live cost×markup derivation whenever both fields are present, and
falls back to the persisted field only when cost or markup is missing
(e.g., PO-imported items with a flat unit price and no cost).
"""
from __future__ import annotations

from typing import Any


def _coerce_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def canonical_unit_price(item: dict) -> float:
    """Return the unit price that should reach the buyer.

    Priority:
      1. If cost AND markup_pct are both known, return `cost * (1 + markup/100)`
         rounded to 2 decimals. This is what the UI displays on the fly and
         what the save path stores via `_recompute_unit_price`.
      2. Otherwise, fall back to persisted `unit_price` → `pricing.recommended_price`
         → `pricing.bid_price` → 0.

    Never raises. Always returns a float (0.0 when nothing usable is found).
    """
    if not isinstance(item, dict):
        return 0.0
    p = item.get("pricing") if isinstance(item.get("pricing"), dict) else {}

    # Read cost + markup from the priority order the UI uses at
    # routes_pricecheck.py:489: `unit_cost = p.get('unit_cost') or
    # item.get('vendor_cost')`. Use `or`-chaining (not `is None`) so an
    # explicit zero in `vendor_cost` falls through to the non-zero
    # `pricing.unit_cost`. This also covers the RFQ alias `supplier_cost`
    # set by routes_rfq_gen.py:671 on RFQ save. Hotfix 2026-04-23 after
    # pc_f7ba7a6b (Cortech) returned stale $558.48 in email body while UI
    # rendered the correct $567.79 — the flat `vendor_cost` was 0/missing
    # on that record's persisted item dict so the `is None` guard fell
    # through to the stale `unit_price` fallback instead of reading
    # `pricing.unit_cost` where the real $465.40 lived.
    cost = (_coerce_float(p.get("unit_cost"))
            or _coerce_float(item.get("vendor_cost"))
            or _coerce_float(item.get("supplier_cost"))
            or _coerce_float(item.get("cost")))
    markup = (_coerce_float(item.get("markup_pct"))
              or _coerce_float(p.get("markup_pct"))
              or _coerce_float(item.get("markup"))
              or _coerce_float(p.get("markup")))
    if cost and markup is not None and cost > 0:
        return round(cost * (1 + markup / 100.0), 2)
    # Fallback — persisted value, in reading-order preference
    for k in ("unit_price",):
        v = _coerce_float(item.get(k))
        if v and v > 0:
            return v
    for k in ("recommended_price", "bid_price"):
        v = _coerce_float(p.get(k))
        if v and v > 0:
            return v
    return 0.0


def is_unit_price_stale(item: dict, tolerance: float = 0.005) -> bool:
    """True iff the persisted `unit_price` disagrees with the cost×markup
    derivation by more than `tolerance` dollars.

    Tolerance defaults to half a cent so two-decimal rounding jitter
    doesn't flag a record. A record with no cost or no markup is never
    stale (there's nothing to derive from).

    Used by the backfill script to find records to heal.
    """
    if not isinstance(item, dict):
        return False
    stored = _coerce_float(item.get("unit_price"))
    p = item.get("pricing") if isinstance(item.get("pricing"), dict) else {}
    if stored is None:
        stored = _coerce_float(p.get("recommended_price"))
    if stored is None or stored <= 0:
        return False  # nothing persisted to compare against
    cost = (_coerce_float(p.get("unit_cost"))
            or _coerce_float(item.get("vendor_cost"))
            or _coerce_float(item.get("supplier_cost"))
            or _coerce_float(item.get("cost")))
    markup = _coerce_float(item.get("markup_pct"))
    if markup is None:
        markup = _coerce_float(p.get("markup_pct"))
    if cost is None or markup is None or cost <= 0:
        return False
    derived = round(cost * (1 + markup / 100.0), 2)
    return abs(derived - stored) > tolerance
