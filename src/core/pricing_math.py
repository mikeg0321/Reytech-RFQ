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


# ── Write-path reconciliation ──────────────────────────────────────────────
#
# 2026-05-06 audit (PR-1): both PC and RFQ save paths must call
# `reconcile_line_item(item)` after every write that changes cost,
# markup, or unit_price. This was the core drift instance in the
# session audit — PR #765 added reverse-markup derivation on the PC
# side only (`_do_save_prices`), so RFQ kept stale markup that
# poisoned catalog write-back.

import logging as _pm_logging

_pm_log = _pm_logging.getLogger("reytech.pricing_math")

# Tolerance in markup percentage points for "stale". Sub-tolerance
# differences are rounding noise from prior writes; we leave them alone.
_MARKUP_DRIFT_TOLERANCE_PCT = 0.5


def _read_cost(item: dict) -> float:
    """Pull cost from any alias. Priority matches `canonical_unit_price` so
    derivations use the same source field on read and write."""
    p = item.get("pricing") if isinstance(item.get("pricing"), dict) else {}
    raw = (_coerce_float(p.get("unit_cost"))
           or _coerce_float(item.get("vendor_cost"))
           or _coerce_float(item.get("supplier_cost"))
           or _coerce_float(item.get("cost"))
           or _coerce_float(p.get("cost")))
    return float(raw or 0)


def _read_price(item: dict) -> float:
    p = item.get("pricing") if isinstance(item.get("pricing"), dict) else {}
    raw = (_coerce_float(item.get("unit_price"))
           or _coerce_float(item.get("price_per_unit"))
           or _coerce_float(p.get("recommended_price"))
           or _coerce_float(p.get("unit_price")))
    return float(raw or 0)


def _read_markup(item: dict):
    """Returns markup_pct or None (None ≠ 0 — None means absent, 0 means free)."""
    raw = item.get("markup_pct")
    if raw is None:
        p = item.get("pricing") if isinstance(item.get("pricing"), dict) else {}
        raw = p.get("markup_pct")
    if raw is None:
        raw = item.get("markup")
    if raw is None:
        return None
    return _coerce_float(raw)


def _write_cost(item: dict, cost: float) -> None:
    """Mirror cost to all known aliases so PC and RFQ readers agree."""
    item["supplier_cost"] = cost
    item["vendor_cost"] = cost
    if not isinstance(item.get("pricing"), dict):
        item["pricing"] = {}
    item["pricing"]["unit_cost"] = cost


def _write_price(item: dict, price: float) -> None:
    item["unit_price"] = price
    item["price_per_unit"] = price
    if not isinstance(item.get("pricing"), dict):
        item["pricing"] = {}
    item["pricing"]["recommended_price"] = price


def _write_markup(item: dict, markup_pct: float) -> None:
    item["markup_pct"] = markup_pct
    if not isinstance(item.get("pricing"), dict):
        item["pricing"] = {}
    item["pricing"]["markup_pct"] = markup_pct


def _write_margin(item: dict, cost: float, price: float) -> None:
    if price > 0:
        item["margin_pct"] = round((price - cost) / price * 100, 1)


def reconcile_line_item(item: dict) -> dict:
    """Make cost / markup / price agree on a single line item.

    Mutates `item` in place and returns it. Skips no-bid items.

    Resolution rules, in order:

    1. **Both cost and price present, markup stale or missing**:
       reverse-derive markup_pct = (price - cost) / cost * 100.
       This is the case the 2026-05-05 incident exposed (Mike P0):
       operator types OUR PRICE directly, expects markup to follow.

    2. **Cost and markup present, price stale or missing**:
       forward-compute price = cost * (1 + markup_pct/100).
       Standard quote math — `_recompute_unit_price` legacy path.

    3. **Insufficient signal**: leave alone. Caller hasn't given us
       enough information to reconcile.

    Both PC and RFQ save paths MUST call this after any write that
    touches cost, markup, or price. New pricing rules land here once.
    """
    if item.get("no_bid"):
        return item

    cost = _read_cost(item)
    price = _read_price(item)
    markup = _read_markup(item)

    # Rule 1: cost + price → derive markup
    if cost > 0 and price > 0:
        try:
            derived_markup = round((price - cost) / cost * 100, 1)
        except ZeroDivisionError:  # cost > 0 above; defensive
            return item
        if markup is None or abs(markup - derived_markup) > _MARKUP_DRIFT_TOLERANCE_PCT:
            _write_markup(item, derived_markup)
            _pm_log.info(
                "reconcile reverse-markup: cost=%.2f price=%.2f → markup=%.1f%% (was %s)",
                cost, price, derived_markup, markup,
            )
        # Always echo cost/price to all aliases + recompute margin so
        # PC and RFQ readers see the same numbers regardless of which
        # field name they pick.
        _write_cost(item, cost)
        _write_price(item, price)
        _write_margin(item, cost, price)
        return item

    # Rule 2: cost + markup → derive price
    if cost > 0 and markup is not None and price <= 0:
        derived_price = round(cost * (1 + markup / 100.0), 2)
        _write_cost(item, cost)
        _write_price(item, derived_price)
        _write_margin(item, cost, derived_price)
        _pm_log.info(
            "reconcile forward-price: cost=%.2f markup=%.1f%% → price=%.2f",
            cost, markup, derived_price,
        )
        return item

    # Rule 3: insufficient signal — leave alone.
    return item


def reconcile_items(items: list) -> int:
    """Apply `reconcile_line_item` across a list. Returns count of items
    that changed (markup or price moved)."""
    if not isinstance(items, list):
        return 0
    touched = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        before_markup = _read_markup(it)
        before_price = _read_price(it)
        reconcile_line_item(it)
        if _read_markup(it) != before_markup or _read_price(it) != before_price:
            touched += 1
    return touched


# ── Subtotal invariant — single source of truth for billable items ────────
#
# 2026-05-08 audit incident: `csp-sac` PC printed Merchandise Subtotal
# $1,445.15 for a single $1,439.75 line item. Mike's two-item PC had
# item #1 marked Skip (`no_bid=True`) but its stored `unit_price` of
# $5.40 still hit the subtotal accumulator inside `fill_ams704()` —
# the row renderer dropped the row but the math summed it. Same shape
# was present in `quote_generator.py` for RFQ quote PDFs (no skip
# filter at all).
#
# Fix: every renderer derives subtotal from `subtotal_of(items)` AT
# THE END after rows are written. The accumulator-during-row-loop
# pattern is the bug-generating shape we are eliminating.


def is_billable(item: dict) -> bool:
    """Should this item contribute to subtotal?

    True iff the item is not marked Skip / no_bid. Render-side
    decisions (whether to draw a row at all) may further gate on
    price>0 / qty>0; those are separate questions and stay in the
    renderer. This predicate answers ONLY "is this item summed".
    Keeping the two filters separate prevents cross-contamination —
    a $0-price billable item still shows a row for operator
    visibility, but a no_bid item never contributes regardless of
    a stored stale price.
    """
    if not isinstance(item, dict):
        return False
    return not bool(item.get("no_bid"))


def extension_of(item: dict) -> float:
    """Single canonical price × qty calculation for one item.

    Reads price via `canonical_unit_price` (cost×markup priority +
    unit_price fallback) and additionally honors `price_per_unit`
    which is the RFQ/quote-side convention used by quote_generator.
    Returns 0.0 for non-billable items so callers can sum naively.
    """
    if not is_billable(item):
        return 0.0
    price = canonical_unit_price(item)
    if price <= 0:
        # quote_generator stores `price_per_unit` rather than `unit_price`.
        # canonical_unit_price's fallback chain checks unit_price only;
        # honor the quote convention here so subtotal_of works for both
        # PC dicts and quote dicts without requiring conversion.
        price = _coerce_float(item.get("price_per_unit")) or 0.0
    # PC items use `qty`, RFQ/quote inputs use `quantity` before
    # `_normalize_item` runs. Accept either — `subtotal_of` is called
    # by render paths that pass raw + normalized items; we must agree
    # with both shapes.
    qty = _coerce_float(item.get("qty"))
    if qty is None or qty == 0:
        qty = _coerce_float(item.get("quantity"))
    if qty is None:
        qty = 0.0
    if price <= 0 or qty <= 0:
        return 0.0
    return round(price * qty, 2)


def subtotal_of(items) -> float:
    """Sum extension_of() across all items.

    Use this AT THE END of any 704/quote render to derive the printed
    Merchandise Subtotal. Replaces the per-row `subtotal +=` accumulator
    pattern that drifted across `price_check.py` (two sites) and
    `quote_generator.py`.
    """
    if not items:
        return 0.0
    return round(sum(extension_of(it) for it in items if isinstance(it, dict)), 2)


def billable_items(items) -> list:
    """Filter to billable items in original order. Same filter
    `subtotal_of` applies, exposed for callers that need to count
    or iterate billable items (e.g., for tax application)."""
    if not items:
        return []
    return [it for it in items if isinstance(it, dict) and is_billable(it)]


def assert_subtotal_invariant(
    items,
    printed_subtotal: float,
    *,
    context: str = "",
    tolerance: float = 0.01,
) -> bool:
    """Verify printed subtotal equals subtotal_of(items).

    Logs WARNING (not raise) on drift so a render path with a stale-
    cache subtotal still ships output — but the warning surfaces drift
    loud enough that ops sees it. Returns True if invariant held,
    False if drift was detected.

    `context` is a free-form grep handle for the log line
    ("fill_ams704 pcid=X" or "quote_generator quote=Y").
    """
    expected = subtotal_of(items)
    try:
        printed = float(printed_subtotal)
    except (TypeError, ValueError):
        printed = 0.0
    if abs(printed - expected) > tolerance:
        item_list = list(items) if items else []
        _pm_log.warning(
            "PRICING-DRIFT %s: printed_subtotal=%.2f != billable_subtotal=%.2f "
            "(delta=%+.2f, total_items=%d, billable=%d, no_bid=%d)",
            context or "(unknown)",
            printed, expected, printed - expected,
            len(item_list), len(billable_items(item_list)),
            sum(1 for it in item_list if isinstance(it, dict) and it.get("no_bid")),
        )
        return False
    return True


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
