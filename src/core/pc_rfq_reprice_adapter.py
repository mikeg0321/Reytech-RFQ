"""Oracle adapter for the CCHCS PC→RFQ selective re-price path.

`reprice_qty_changed_lines` (in pc_rfq_linker.py) takes a `pricer` callable
as dependency injection so tests can stub it without touching the DB. This
module provides the production pricer: a thin wrapper around
`pricing_oracle_v2.get_pricing` that takes a single RFQ line dict and
returns either `{supplier_cost, unit_price, bid_price, markup_pct}` or
`None` when the oracle lacks enough data to price confidently.

Design constraints (Mike, 2026-04-20):

  - PC commitment prices on qty-unchanged lines must never reach this code.
    The allowlist in reprice_qty_changed_lines protects field identity,
    and pc_rfq_linker.py only calls the pricer on lines flagged qty_changed.

  - Returning None is preferred over fabricating a price. The reprice helper
    counts None returns as `skipped_no_price`, surfacing drifted lines for
    manual follow-up rather than silently locking in a bad number.

  - No field leakage: only the four price fields above are returned. If the
    oracle surfaces description or qty suggestions, they're discarded here
    (and the helper's own allowlist would discard them anyway).
"""
from __future__ import annotations

import logging

log = logging.getLogger("reytech")


def _pick_quote_price(rec: dict) -> float | None:
    """Extract the bid price from a get_pricing recommendation dict.

    Prefers `quote_price` (V3/V5 recommendation), falls back to the top
    strategy's price, then the ceiling. Returns None if nothing usable.
    """
    if not isinstance(rec, dict):
        return None
    qp = rec.get("quote_price")
    if qp and qp > 0:
        return float(qp)
    strategies = rec.get("strategies") or []
    if strategies and isinstance(strategies, list):
        for s in strategies:
            if isinstance(s, dict) and s.get("price"):
                return float(s["price"])
    return None


def _line_cost(line: dict) -> float | None:
    """Pull a usable cost from a line item — NOT SCPRS/Amazon (reference only)."""
    for key in ("supplier_cost", "catalog_cost", "web_cost", "vendor_cost",
                "cost", "unit_cost"):
        val = line.get(key)
        if val and float(val) > 0:
            return float(val)
    return None


def _build_oracle_audit(result: dict, rec: dict, snapshot_at: str) -> dict:
    """Build the snapshot that proves what the oracle saw at pricing time.

    PR-G (2026-05-13) — captures the cap audit + rollup data so the
    measurement loop can later answer "did capping this line improve
    the outcome?" The shape is a single JSON-friendly dict (vs adding
    5 separate fields) so it survives `_VERBATIM_PRICE_FIELDS` as one
    entry, and future cap types (floor, volume-ceiling, etc.) stack
    inside the same envelope without another allowlist edit.

    Always returns a dict — `caps_applied` may be `[]` and
    `scprs_rollup` may be `None`, but presence-of-snapshot itself is
    the signal that "the oracle considered the cap." Absence at read
    time means the pricer never ran.
    """
    return {
        "rec_price": rec.get("quote_price"),
        "rec_pre_cap_price": rec.get("quote_price_pre_cap"),
        "caps_applied": rec.get("caps_applied") or [],
        "scprs_rollup": result.get("scprs_rollup"),
        "oracle_version": "v2.1",
        "snapshot_at": snapshot_at,
    }


def oracle_pricer_for_line(line: dict, agency: str = "") -> dict | None:
    """Production pricer passed to `reprice_qty_changed_lines`.

    Called once per qty-changed line. Returns the allowlisted price fields
    the helper is willing to accept, or None to tell the helper to count
    this line as `skipped_no_price` and leave it with its PC commitment.

    Safety: if the oracle throws or returns nothing with quote_price > 0,
    we return None rather than propagating the exception. Getting a drifted
    line WRONG is worse than getting it un-repriced — the operator sees
    `skipped_no_price` in the summary and can follow up manually.

    2026-05-13 (PR-G): now passes `mfg_number=` + `unspsc=` to
    `get_pricing()` so the SCPRS rollup actually fires on this path,
    AND emits an `oracle_audit` field carrying the cap audit + rollup
    snapshot. The reprice adapter's allowlist (`_VERBATIM_PRICE_FIELDS`)
    is extended in pc_rfq_linker.py to pass that field through. Without
    both changes, every PC→RFQ qty-changed reprice silently discarded
    the cap audit and the loop's measurement substrate was empty.
    """
    desc = (line.get("description") or line.get("desc") or "").strip()
    if not desc:
        return None

    qty = line.get("quantity") or line.get("qty") or 1
    try:
        qty = int(float(qty))
    except (TypeError, ValueError):
        qty = 1
    if qty < 1:
        qty = 1

    mfg = (line.get("mfg_number") or line.get("part_number")
           or line.get("item_number") or "").strip()
    unspsc = (line.get("unspsc") or "").strip()
    cost = _line_cost(line)

    try:
        from src.core.pricing_oracle_v2 import get_pricing
        result = get_pricing(
            description=desc,
            quantity=qty,
            cost=cost,
            item_number=mfg,
            department=agency or "",
            upc=(line.get("upc") or "").strip(),
            mfg_number=mfg,    # PR-G: enable rollup lookup on reprice path
            unspsc=unspsc,
        )
    except Exception as e:
        log.warning("oracle_pricer_for_line: get_pricing failed for %r: %s",
                    desc[:60], e)
        return None

    rec = (result or {}).get("recommendation") or {}
    bid = _pick_quote_price(rec)
    if not bid or bid <= 0:
        return None

    # Cost for the new qty: prefer the oracle's locked/memory cost, fall back
    # to what the caller already had. Never fabricate a cost from market data.
    oracle_cost = None
    cost_block = (result or {}).get("cost") or {}
    for key in ("locked_cost", "provided_cost", "last_cost"):
        val = cost_block.get(key)
        if val and float(val) > 0:
            oracle_cost = float(val)
            break
    final_cost = oracle_cost or cost

    markup_pct = rec.get("markup_pct")
    if markup_pct is None and final_cost and final_cost > 0:
        markup_pct = round(((bid - final_cost) / final_cost) * 100, 1)

    from datetime import datetime as _dt
    snapshot_at = _dt.now().isoformat(timespec="seconds")

    out: dict = {
        "unit_price": round(float(bid), 2),
        "bid_price": round(float(bid), 2),
        # PR-G: oracle audit snapshot — passes through the allowlist as
        # a single envelope so future cap types don't force another edit.
        "oracle_audit": _build_oracle_audit(result, rec, snapshot_at),
    }
    if final_cost and final_cost > 0:
        out["supplier_cost"] = round(float(final_cost), 2)
    if markup_pct is not None:
        try:
            out["markup_pct"] = float(markup_pct)
        except (TypeError, ValueError):
            pass

    _caps_n = len(out["oracle_audit"]["caps_applied"])
    log.info("oracle_pricer: %r qty=%d → $%.2f (markup %s%%, caps=%d)",
             desc[:60], qty, bid, markup_pct, _caps_n)
    return out
