"""Pre-send win-validation warnings (PR-C1, 2026-05-03).

Computes SOFT warnings for an RFQ before send so the operator can spot
likely-losing-or-leaking-margin pricing patterns. Never blocks send —
the existing `quote_validator.validate_ready_to_generate` is the only
hard gate, and stays that way (per `feedback_ten_minute_escape_valve`:
operator must always be able to ship).

Three warning families implemented (audit Gaps #1, #2, #5):

  • cost_above_last_won — operator's current cost is ≥ 1.5× our prior
    winning cost for THIS buyer + part. Catches the Barstow class
    (Amazon ghost cost on a $400 Grainger item).

  • line_low_margin — single line item priced below the strategic
    floor (markup < 15% OR absolute margin < $2/unit). Catches typos
    (missed decimal point) and accidental zero-margin lines.

  • quote_low_margin — total quote margin below 22%. Rolls up across
    items so a few high-margin lines can't mask a low-margin overall
    deal.

This module is read-only — no DB writes, no record mutation. Returning
warnings is the only side effect.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger("reytech.win_validation")


# ─── Thresholds ────────────────────────────────────────────────────────

# Gap #1: any cost > 1.5x last-won-buyer triggers a red warning.
COST_VS_LAST_WON_RATIO = 1.5

# Gap #2 line-level floors. Either condition triggers orange.
LINE_MIN_MARKUP_PCT = 15.0
LINE_MIN_MARGIN_USD = 2.0

# Gap #5 quote-level floor. Below this is orange.
QUOTE_MIN_MARGIN_PCT = 22.0


# ─── Helpers ──────────────────────────────────────────────────────────


def _f(v: Any) -> float:
    """Coerce a value to float, stripping currency formatting."""
    try:
        return float(str(v or 0).replace("$", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _cost_for(item: Dict[str, Any]) -> float:
    """Pick the cost field most readers use, with the standard fallback chain."""
    return _f(
        item.get("supplier_cost")
        or item.get("unit_cost")
        or item.get("cost")
    )


def _price_for(item: Dict[str, Any]) -> float:
    return _f(
        item.get("price_per_unit")
        or item.get("unit_price")
        or item.get("bid_price")
    )


def _qty_for(item: Dict[str, Any]) -> float:
    return _f(item.get("quantity") or item.get("qty"))


# ─── Public API ────────────────────────────────────────────────────────


def compute_win_warnings(rfq_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a list of soft win-risk warnings for the given RFQ/quote dict.

    Pure function — no DB calls, no I/O. Caller must pre-enrich each
    item with `last_won_price` (float) and `last_won_quote` (str) when
    a buyer-history lookup applies. The route module owns the DB; this
    module owns the threshold logic. Keeps the helper testable in
    isolation and avoids importing exec()-loaded route modules.

    Args:
      rfq_data: dict with "line_items" (or "items") list. Each item dict
        carries cost + price + qty + part_number + description, plus
        optional pre-resolved `last_won_price` / `last_won_quote`.

    Returns:
      A list of warning dicts, each with:
        line_no: int | None  (None for quote-level warnings)
        level:   "red" | "orange" | "yellow"
        code:    short identifier for grouping
        message: human-readable message
        meta:    dict with extra fields (last_won, ratio, margin_pct, …)

    Empty list = no warnings. Caller is free to ignore (operator
    overrides everything; nothing here blocks send).
    """
    items = rfq_data.get("line_items") or rfq_data.get("items") or []
    if not isinstance(items, list):
        return []

    warnings: List[Dict[str, Any]] = []

    # ── Per-item warnings ──
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        line_no = item.get("line_number") or item.get("line_no") or (idx + 1)
        cost = _cost_for(item)
        price = _price_for(item)
        qty = _qty_for(item)

        # Skip lines marked no_bid or with no cost set yet — operator
        # is still working on them; warning would just be noise.
        if item.get("no_bid"):
            continue
        if cost <= 0:
            continue

        # Gap #1: cost vs last-won-buyer (caller-provided)
        lw_price = _f(item.get("last_won_price"))
        if lw_price > 0 and cost > lw_price * COST_VS_LAST_WON_RATIO:
            ratio = cost / lw_price
            warnings.append({
                "line_no": line_no,
                "level": "red",
                "code": "cost_above_last_won",
                "message": (
                    f"Cost ${cost:.2f} is {ratio:.1f}× our last-won "
                    f"cost ${lw_price:.2f} for this buyer "
                    f"(quote {item.get('last_won_quote','?')})"
                ),
                "meta": {
                    "current_cost": cost,
                    "last_won_price": lw_price,
                    "last_won_quote": item.get("last_won_quote", ""),
                    "ratio": ratio,
                },
            })

        # Gap #2: line-level margin floor
        if price > 0 and cost > 0:
            margin = price - cost
            markup_pct = (margin / cost) * 100 if cost > 0 else 0
            below_pct = markup_pct < LINE_MIN_MARKUP_PCT
            below_abs = margin < LINE_MIN_MARGIN_USD
            if below_pct or below_abs:
                bits = []
                if below_pct:
                    bits.append(f"markup {markup_pct:.1f}% < {LINE_MIN_MARKUP_PCT}%")
                if below_abs:
                    bits.append(f"margin ${margin:.2f} < ${LINE_MIN_MARGIN_USD}")
                warnings.append({
                    "line_no": line_no,
                    "level": "orange",
                    "code": "line_low_margin",
                    "message": "Low margin: " + "; ".join(bits),
                    "meta": {
                        "unit_cost": cost,
                        "unit_price": price,
                        "qty": qty,
                        "margin_per_unit": margin,
                        "markup_pct": markup_pct,
                    },
                })

    # ── Quote-level warnings ──
    # Gap #5: aggregate margin floor across all items.
    total_cost = 0.0
    total_revenue = 0.0
    for item in items:
        if not isinstance(item, dict) or item.get("no_bid"):
            continue
        c = _cost_for(item)
        p = _price_for(item)
        q = _qty_for(item)
        if c > 0 and p > 0 and q > 0:
            total_cost += c * q
            total_revenue += p * q

    if total_cost > 0:
        total_margin_usd = total_revenue - total_cost
        total_markup_pct = (total_margin_usd / total_cost) * 100
        if total_markup_pct < QUOTE_MIN_MARGIN_PCT:
            warnings.append({
                "line_no": None,
                "level": "orange",
                "code": "quote_low_margin",
                "message": (
                    f"Total quote markup {total_markup_pct:.1f}% is below "
                    f"the {QUOTE_MIN_MARGIN_PCT:.0f}% floor "
                    f"(margin ${total_margin_usd:.2f} on cost ${total_cost:.2f})"
                ),
                "meta": {
                    "total_cost": total_cost,
                    "total_revenue": total_revenue,
                    "total_margin_usd": total_margin_usd,
                    "total_markup_pct": total_markup_pct,
                },
            })

    return warnings


__all__ = [
    "compute_win_warnings",
    "COST_VS_LAST_WON_RATIO",
    "LINE_MIN_MARKUP_PCT",
    "LINE_MIN_MARGIN_USD",
    "QUOTE_MIN_MARGIN_PCT",
]
