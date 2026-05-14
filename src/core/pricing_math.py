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
    """Coerce to float, returning None on failure. Strips `$` and `,`
    on string inputs so Excel-import / CSV-import / form-paste values
    like `"$10.00"` or `"1,250.50"` survive. Bools coerce to 0/1 by
    Python's int conversion — explicitly reject so a True-ish flag
    elsewhere on the item dict can't masquerade as $1.00."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # Strip leading currency, thousands separators. We intentionally
        # do NOT strip whitespace inside the number — `"1 250"` stays
        # ambiguous and fails the float() call below.
        if s.startswith("$"):
            s = s[1:]
        s = s.replace(",", "")
        try:
            return float(s)
        except (TypeError, ValueError):
            return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def cost_from_contract(item: dict) -> float:
    """Return the canonical cost for a line item — the SINGLE source of
    truth that every renderer / route / agent must use to read cost.

    Priority (operator-typed wins over scrape, mat-on-PVSP 2026-05-13):
      1. `item["supplier_cost"]`        — RFQ side, set by `validate_rfq_item`
      2. `item["vendor_cost"]`          — PC side, set by `_do_save_prices`
      3. `item["pricing"]["unit_cost"]` — URL-paste / catalog scrape
      4. `item["cost"]`                 — legacy flat alias
      5. `item["pricing"]["cost"]`      — legacy nested alias
      → 0.0 if none of the above coerce to a positive number.

    Returns float dollars (NOT cents). Pair with `_write_cost` so any
    write mirrors to all aliases and any read goes through this one
    function — closes the alias-priority drift bug class (PR #975 + #932
    + #952 were all symptoms of duplicated alias chains across renderers).

    PR mr-wolf #2 promotes the previously-private `_read_cost` to a
    public name so the architecture-contract ratchet can gate against
    direct alias reads in renderer / route / agent files. `_read_cost`
    remains as a deprecated thin alias for backwards compat; new code
    must call `cost_from_contract`.

    Never raises. Always returns a float.
    """
    if not isinstance(item, dict):
        return 0.0
    p = item.get("pricing") if isinstance(item.get("pricing"), dict) else {}
    raw = (_coerce_float(item.get("supplier_cost"))
           or _coerce_float(item.get("vendor_cost"))
           or _coerce_float(p.get("unit_cost"))
           or _coerce_float(item.get("cost"))
           or _coerce_float(p.get("cost")))
    return float(raw or 0)


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

    # Cost via the single canonical reader. PR #2 (mr-wolf substrate
    # pivot): previously this function inlined its own alias-priority
    # chain that duplicated `_read_cost`'s logic. The old PR #321
    # (Cortech mattress, pc_f7ba7a6b) plus PR #975 (mat-on-PVSP) both
    # patched the chain in TWO places. Now there's exactly one chain in
    # `cost_from_contract`; any future priority change touches one site.
    cost = cost_from_contract(item)
    # 2026-05-11: prior `or`-chain treated markup_pct=0.0 as missing
    # (Python falsy: `0.0 or X` evaluates to X). For a give-away or
    # cost-pass-through item with explicit markup=0, the function fell
    # through to the stale `unit_price` fallback. Now uses explicit None
    # check — None means "no markup recorded", 0.0 means "free / pass-
    # through" (legitimate canonical value: cost × 1.0 = cost).
    markup = None
    for src in (item.get("markup_pct"), p.get("markup_pct"),
                item.get("markup"), p.get("markup")):
        if src is None:
            continue
        coerced = _coerce_float(src)
        if coerced is not None:
            markup = coerced
            break
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
import os as _pm_os

_pm_log = _pm_logging.getLogger("reytech.pricing_math")

# Tolerance in markup percentage points for "stale". Sub-tolerance
# differences are rounding noise from prior writes; we leave them alone.
_MARKUP_DRIFT_TOLERANCE_PCT = 0.5


# ── Markup sanity gate ────────────────────────────────────────────────────
#
# 2026-05-12 macro audit (project_url_paste_substrate_macro_2026_05_12):
# Pre-Phase-1 PCs carry hallucinated markup_pct values (912%, 327%, 280%,
# -4.6%) computed against Amazon-hallucinated $2.50 costs. When a URL paste
# refreshes cost to the real $4 supplier price, `prefer="markup"` forward-
# computes price = $4 × 10.12 = $40.48 → $40 on a $4 eraser ships to buyer.
#
# The fix: a markup-sanity gate. An out-of-range markup_pct is treated as
# MISSING by the reconciler (so the price path falls back to reverse-derive
# from cost+price if a sane price exists, or leave the row alone for the
# operator). Bounds chosen to span all realistic Reytech bids:
#   - Mike's typical bids land 8-60% (catalog) and rarely 80-150% (premium)
#   - Loss-leader bids occasionally go slightly negative (-5% to 0%) but
#     never below -10% in known history
#   - >200% is always a hallucination from a pre-Phase-1 record or a
#     mis-scraped cost
#
# Bounds are env-overridable so we can tune from telemetry without a deploy.


def _markup_bounds() -> tuple[float, float]:
    """Return (min, max) markup_pct considered sane.

    Env knobs (default -10.0 / 200.0):
      PRICING_MARKUP_MIN_PCT  — lower bound (negative loss-leader floor)
      PRICING_MARKUP_MAX_PCT  — upper bound (above = hallucination)
    """
    try:
        lo = float(_pm_os.getenv("PRICING_MARKUP_MIN_PCT", "-10"))
    except (TypeError, ValueError):
        lo = -10.0
    try:
        hi = float(_pm_os.getenv("PRICING_MARKUP_MAX_PCT", "200"))
    except (TypeError, ValueError):
        hi = 200.0
    if lo > hi:
        # Misconfigured — fall back to defaults rather than invert the gate.
        return -10.0, 200.0
    return lo, hi


def _markup_is_sane(markup_pct: float | None) -> bool:
    """True if a markup_pct lies within the configured sanity window.

    `None` returns False (absent ≠ sane). A finite value inside
    `_markup_bounds()` returns True; outside returns False and the
    reconciler treats it as if it were absent.
    """
    if markup_pct is None:
        return False
    lo, hi = _markup_bounds()
    return lo <= markup_pct <= hi


def _read_cost(item: dict) -> float:
    """Deprecated alias for `cost_from_contract`. PR mr-wolf #2 promoted
    this to a public name; this thin alias stays for internal callers
    inside `pricing_math` until they migrate. New code (especially in
    `src/forms/`, `src/api/modules/routes_*`, `src/agents/*`) MUST call
    `cost_from_contract` directly — the architecture-contract ratchet
    test gates against bare alias-chain reads outside this module.
    """
    return cost_from_contract(item)


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


def reconcile_line_item(item: dict, prefer: str = "markup") -> dict:
    """Make cost / markup / price agree on a single line item.

    Mutates `item` in place and returns it. Skips no-bid items.

    `prefer` controls which field is treated as operator truth when
    cost + markup + price are all present but disagree:

    * `"markup"` (default) — markup_pct is sticky. If cost+markup are
      explicit, price is forward-computed from `cost * (1 + markup/100)`
      even if a stale price disagrees. Reverse-markup only fires when
      markup is absent. Used by autosave, enrichment, ingest reparse —
      paths where the operator's typed markup must not be silently
      overwritten by a stale persisted price. (Mike P0 2026-05-12
      `rfq_8efe9fae`: autosave was reverse-deriving 35% from a stale
      price snapshot and clobbering the operator's 8% intent.)

    * `"price"` — price is sticky. If cost+price are explicit, markup
      is reverse-derived from `(price - cost) / cost * 100` overriding
      any stale markup. Used by PC `_do_save_prices` where the
      operator's explicit interaction is typing OUR PRICE and expects
      markup_pct to follow. (Mike P0 2026-05-05 Heel Donut: cost=$8,
      stale markup=20%, operator types price=$16 → markup must flip
      to 100%.)

    In both modes, an item with only cost+price (markup absent) gets
    markup reverse-derived as a back-fill. An item with only cost+markup
    (price absent) gets price forward-computed. An item with only cost
    or no useful signal is left alone.
    """
    if item.get("no_bid"):
        return item

    cost = _read_cost(item)
    price = _read_price(item)
    markup = _read_markup(item)

    if cost <= 0:
        return item

    # Markup-sanity gate (2026-05-12 macro audit). An out-of-range
    # markup_pct is hallucination from a pre-Phase-1 record or scraping
    # error; we treat it as MISSING so the reconciler falls back to
    # reverse-derive from cost+price (if a sane price exists) instead
    # of forward-computing absurd prices like $4 × 10.12 = $40.48.
    # Log once per item so we can audit the gate's hit rate.
    if markup is not None and not _markup_is_sane(markup):
        lo, hi = _markup_bounds()
        _pm_log.warning(
            "reconcile markup-sanity: %.1f%% outside [%.1f%%, %.1f%%] — treating as MISSING "
            "(cost=%.2f, price=%.2f)",
            markup, lo, hi, cost, price,
        )
        markup = None

    if prefer == "price":
        # Price-wins semantic (PC `_do_save_prices` Heel Donut flow).
        if price > 0:
            try:
                derived_markup = round((price - cost) / cost * 100, 1)
            except ZeroDivisionError:
                return item
            if markup is None or abs(markup - derived_markup) > _MARKUP_DRIFT_TOLERANCE_PCT:
                _write_markup(item, derived_markup)
                _pm_log.info(
                    "reconcile reverse-markup [prefer=price]: cost=%.2f price=%.2f → markup=%.1f%% (was %s)",
                    cost, price, derived_markup, markup,
                )
            _write_cost(item, cost)
            _write_price(item, price)
            _write_margin(item, cost, price)
            return item
        if markup is not None:
            derived_price = round(cost * (1 + markup / 100.0), 2)
            _write_cost(item, cost)
            _write_price(item, derived_price)
            _write_margin(item, cost, derived_price)
            return item
        return item

    # prefer == "markup" (default) — markup is sticky.
    if markup is not None:
        derived_price = round(cost * (1 + markup / 100.0), 2)
        if price <= 0 or abs(price - derived_price) >= 0.01:
            if price > 0 and abs(price - derived_price) >= 0.01:
                _pm_log.info(
                    "reconcile forward-price [prefer=markup]: cost=%.2f markup=%.1f%% → price=%.2f (was %.2f)",
                    cost, markup, derived_price, price,
                )
            _write_price(item, derived_price)
            price = derived_price
        _write_cost(item, cost)
        _write_margin(item, cost, price)
        return item

    # markup absent + cost + price → back-fill markup (no operator
    # intent to overwrite). This keeps fresh-ingest records coherent.
    if price > 0:
        try:
            derived_markup = round((price - cost) / cost * 100, 1)
        except ZeroDivisionError:
            return item
        _write_markup(item, derived_markup)
        _write_cost(item, cost)
        _write_price(item, price)
        _write_margin(item, cost, price)
        return item

    return item


def reconcile_items(items: list, prefer: str = "markup") -> int:
    """Apply `reconcile_line_item` across a list. Returns count of items
    that changed (markup or price moved). See `reconcile_line_item` for
    `prefer` semantics."""
    if not isinstance(items, list):
        return 0
    touched = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        before_markup = _read_markup(it)
        before_price = _read_price(it)
        reconcile_line_item(it, prefer=prefer)
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


def profit_summary_of(items, *, landed_cost_fn=None) -> dict:
    """Compute the PC-level profit envelope from items, fresh.

    This is the substrate replacement for the `pc["profit_summary"]`
    cached snapshot (PR mr-wolf #3, Pattern 2). The cached field went
    stale on every cost edit and forced every renderer to either trust
    the cache (and ship stale numbers) or recompute locally (drifting
    from the cache). This function is the canonical computed view —
    every consumer that needs revenue / cost / margin reads this; no
    one reads `pc.get("profit_summary")` operationally.

    Shape (matches `routes_pricecheck.py`'s historical `_summary` dict
    exactly so consumers don't have to restructure):

      total_revenue       — Σ price × qty across billable items
      total_cost          — Σ cost × qty for items with cost > 0
      gross_profit        — total_revenue − total_cost
      margin_pct          — gross_profit / total_revenue × 100, rounded to 1
      total_landed_cost   — Σ landed_cost × qty if `landed_cost_fn` provided,
                            else falls back to total_cost (no shipping adder)
      true_profit         — total_revenue − total_landed_cost
      true_margin_pct     — true_profit / total_revenue × 100, rounded to 1
      costed_items        — count of billable items with cost > 0
      total_items         — count of billable (non-no_bid) items
      fully_costed        — bool: costed_items == total_items
      discount_items      — present iff any item has amazon_sale_price < list
      discount_total_cost / discount_gross_profit / discount_margin_pct /
      discount_profit_note — same conditional shape as the writer

    Money fields are float dollars rounded to 2 decimals. Counts are int.
    Costs read through `cost_from_contract` (PR #2 canonical reader).
    Prices read through `canonical_unit_price` with `price_per_unit`
    fallback for quote-side dicts. no_bid items contribute zero.

    `landed_cost_fn`: optional callable `(cost, qty, supplier) -> {"landed_cost": float}`
    matching the `src.core.db.calc_landed_cost` signature. Injected so this
    function stays pure for tests; the route layer wires the prod helper.
    """
    if not items:
        items_list = []
    elif isinstance(items, list):
        items_list = items
    else:
        items_list = list(items)

    total_revenue = 0.0
    total_cost = 0.0
    total_profit = 0.0
    total_landed_cost = 0.0
    costed_items = 0
    total_items = 0
    # Discount-scenario aggregates: when an item has a sale price below
    # the list price, the MSRP is the conservative cost basis but the
    # discount profit shows what we'd actually clear at PO time. Mike's
    # 2026-04-19 directive: surface both numbers.
    total_discount_cost = 0.0
    total_discount_profit = 0.0
    discount_items = 0

    for it in items_list:
        if not isinstance(it, dict) or not is_billable(it):
            continue
        total_items += 1

        # Price via the canonical reader (PR-1d Quote PDF subtotal lock-in).
        price = canonical_unit_price(it)
        if price <= 0:
            price = _coerce_float(it.get("price_per_unit")) or 0.0
        qty = _coerce_float(it.get("qty"))
        if qty is None or qty == 0:
            qty = _coerce_float(it.get("quantity"))
        if qty is None:
            qty = 0.0

        # Cost via cost_from_contract (PR #2 canonical reader). Prior
        # writer used a local `vendor_cost or pricing.unit_cost` chain
        # that ignored supplier_cost — operator-typed RFQ costs were
        # invisible to the cached summary even after PR #975.
        cost = cost_from_contract(it)

        if price > 0 and qty > 0:
            total_revenue += price * qty

        if cost > 0 and qty > 0:
            total_cost += cost * qty
            total_profit += (price - cost) * qty
            costed_items += 1
            # Landed cost: ask the injected helper if present, else
            # fall back to plain cost (route layer adds shipping when
            # the helper is wired).
            supplier = it.get("item_supplier") or ""
            if landed_cost_fn and supplier:
                try:
                    lc = landed_cost_fn(cost, int(qty), supplier)
                    total_landed_cost += float(lc["landed_cost"]) * qty
                except Exception:
                    total_landed_cost += cost * qty
            else:
                total_landed_cost += cost * qty

        # Discount scenario — only items where lookup_prices() recorded
        # a distinct sale_price below list. Falls back to MSRP cost.
        p = it.get("pricing") if isinstance(it.get("pricing"), dict) else {}
        sale = _coerce_float(p.get("amazon_sale_price"))
        list_p = _coerce_float(p.get("amazon_list_price"))
        if price > 0 and sale and list_p and sale < list_p:
            total_discount_cost += sale * qty
            total_discount_profit += (price - sale) * qty
            discount_items += 1
        elif price > 0 and cost > 0:
            total_discount_cost += cost * qty
            total_discount_profit += (price - cost) * qty

    true_profit = total_revenue - total_landed_cost
    summary = {
        "total_revenue":     round(total_revenue, 2),
        "total_cost":        round(total_cost, 2),
        "gross_profit":      round(total_profit, 2),
        "margin_pct":        round(total_profit / total_revenue * 100, 1) if total_revenue else 0,
        "total_landed_cost": round(total_landed_cost, 2),
        "true_profit":       round(true_profit, 2),
        "true_margin_pct":   round(true_profit / total_revenue * 100, 1) if total_revenue else 0,
        "costed_items":      costed_items,
        "total_items":       total_items,
        "fully_costed":      costed_items == total_items and total_items > 0,
    }
    if discount_items > 0:
        summary["discount_items"] = discount_items
        summary["discount_total_cost"] = round(total_discount_cost, 2)
        summary["discount_gross_profit"] = round(total_discount_profit, 2)
        summary["discount_margin_pct"] = (
            round(total_discount_profit / total_revenue * 100, 1)
            if total_revenue else 0
        )
        summary["discount_profit_note"] = (
            "if discount holds for profit calculation"
        )
    return summary


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
    # Cost via the canonical reader (PR mr-wolf #2 dedupe). Previously
    # this third site duplicated the alias chain alongside
    # `cost_from_contract` and `canonical_unit_price` — three places to
    # patch on every priority shift. Now one site.
    cost = cost_from_contract(item) or None
    markup = _coerce_float(item.get("markup_pct"))
    if markup is None:
        markup = _coerce_float(p.get("markup_pct"))
    if cost is None or markup is None or cost <= 0:
        return False
    derived = round(cost * (1 + markup / 100.0), 2)
    return abs(derived - stored) > tolerance
