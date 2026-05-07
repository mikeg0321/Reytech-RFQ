"""Per-line pricing intelligence — buyer history + SCPRS ceiling.

Audit 2026-05-07 / Tier 1a (PR-D1, the highest-win-rate-lift item):

  > Pricing decisions are made without the data that should drive them.
  > The data exists (winning_prices, scprs_po_lines, quotes.line_items)
  > but is two clicks deep on /growth-intel/buyer. Grep `last won|scprs|
  > won_quote` against rfq_detail.html / pc_detail.html returns zero hits.

This module exposes the two queries that should ride next to every line
item the operator is pricing:

  1. `last_won_for_buyer(...)` — what did THIS buyer last pay for an item
     like this one (matched on part_number first, fuzzy description fallback).
     Helps operator anchor against "Mike sold them this last quarter for $X".

  2. `scprs_ceiling_for_item(...)` — what has the STATE paid (across all
     vendors) for items matching this description / part_number. SCPRS is
     the public price-paid registry; this number is the price ceiling
     beyond which a bid is unlikely to win.

Both are read-only one-shot queries that share a single db connection.

Compose with `compute_panel(...)` to get a `{by_line: {N: {...}}}` blob
suitable for JSON return.
"""
from __future__ import annotations

import logging
from typing import Iterable, Optional

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Buyer-last-won lookup
# ─────────────────────────────────────────────────────────────────────────────

def last_won_for_buyer(
    conn,
    contact_email: str,
    description: str,
    part_number: str = "",
    exclude_quote_number: str = "",
    days: int = 730,
) -> dict:
    """Find this buyer's last WON unit_price for an item matching by
    part_number (preferred) or fuzzy description match.

    Mirrors `routes_growth_intel._last_won_price_for_buyer` but as a
    public, importable helper so the rfq_detail / pc_detail pricing-intel
    panel can call into it without depending on a Flask blueprint module.

    Returns `{}` when no match exists in the window. Match priority:
      1. Exact part_number == part_number (when both non-empty)
      2. Description LIKE %first_3_words% (case-insensitive, all 3 must hit)

    Result shape on hit:
        {"price": float (per-unit, 2dp),
         "quote_number": str,
         "won_at": "YYYY-MM-DD"}
    """
    email = (contact_email or "").strip()
    if not email:
        return {}

    pn = (part_number or "").strip()
    desc = (description or "").strip()
    desc_words = [w for w in desc.split() if len(w) >= 3][:3]

    try:
        rows = conn.execute("""
            SELECT quote_number, sent_at, line_items
            FROM quotes
            WHERE COALESCE(is_test,0) = 0
              AND LOWER(contact_email) = LOWER(?)
              AND status = 'won'
              AND quote_number != ?
              AND COALESCE(NULLIF(sent_at,''), created_at) >= datetime('now', ?)
            ORDER BY COALESCE(NULLIF(sent_at,''), created_at) DESC
            LIMIT 50
        """, (email, exclude_quote_number or "",
              f"-{int(days)} days")).fetchall()
    except Exception as e:
        log.debug("last_won_for_buyer query failed: %s", e)
        return {}

    import json as _json
    for r in rows:
        try:
            items = _json.loads(r["line_items"] or "[]")
        except (ValueError, TypeError):
            continue
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            item_pn = (item.get("part_number") or "").strip()
            item_desc = (item.get("description") or "").strip().lower()

            matched = False
            if pn and item_pn and pn.lower() == item_pn.lower():
                matched = True
            elif desc_words:
                if all(w.lower() in item_desc for w in desc_words):
                    matched = True

            if matched:
                pricing = item.get("pricing") or {}
                if not isinstance(pricing, dict):
                    pricing = {}
                price = (pricing.get("unit_price")
                         or pricing.get("recommended_price")
                         or item.get("unit_price") or 0)
                if not price:
                    continue
                return {
                    "price": round(float(price), 2),
                    "quote_number": r["quote_number"] or "",
                    "won_at": (r["sent_at"] or "")[:10],
                }
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# SCPRS ceiling lookup
# ─────────────────────────────────────────────────────────────────────────────

def _scprs_per_unit(price, qty):
    """Normalize SCPRS-style prices to per-unit.

    Mirrors `pricing_oracle_v2._scprs_per_unit`. SCPRS stores line totals
    in `unit_price` for multi-qty rows. The `p > qty * 2` guard keeps
    small per-unit prices intact while normalizing obvious line totals.
    """
    try:
        p = float(price or 0)
        q = float(qty or 1) or 1
    except (TypeError, ValueError):
        return price
    if p <= 0:
        return price
    if q > 1 and p > q * 2:
        return p / q
    return p


def scprs_ceiling_for_item(conn, description: str, part_number: str = "") -> dict:
    """SCPRS ceiling = statewide median of `scprs_po_lines.unit_price`
    for items matching by part_number / mfg_number first, then by
    description fuzzy match (first 3 words of length >= 3).

    Uses `scprs_po_lines` directly — this is the table populated from
    SCPRS PO scraping. Exempts is_test rows.

    Returns `{}` when no match. On hit:
        {"ceiling": float (median per-unit),
         "low": float, "high": float,
         "sample_count": int}
    """
    desc = (description or "").strip()
    pn = (part_number or "").strip()
    if not desc and not pn:
        return {}

    desc_words = [w for w in desc.split() if len(w) >= 3][:3]

    # Try part-number match first; if zero rows, fall through to a
    # description-word match. SCPRS `item_id` is sparse on many older
    # PO imports — relying on it alone would silently drop matches the
    # operator can clearly see by description (the audit-flagged
    # "data exists but never reaches the eye" pattern).
    base_where = "COALESCE(is_test, 0) = 0"

    def _query(extra_where: str, extra_params: list):
        where = f"{base_where} AND {extra_where}"
        return conn.execute(f"""
            SELECT description, unit_price, quantity
            FROM scprs_po_lines
            WHERE {where}
              AND unit_price IS NOT NULL
              AND unit_price > 0
            ORDER BY id DESC
            LIMIT 100
        """, extra_params).fetchall()

    rows = []
    try:
        if pn:
            rows = _query("LOWER(item_id) = ?", [pn.lower()])
        if not rows and desc_words:
            desc_clauses = " AND ".join(["LOWER(description) LIKE ?" for _ in desc_words])
            rows = _query(desc_clauses, [f"%{w.lower()}%" for w in desc_words])
    except Exception as e:
        log.debug("scprs_ceiling_for_item query failed: %s", e)
        return {}

    if not rows:
        return {}

    per_units = []
    for r in rows:
        try:
            p = _scprs_per_unit(r["unit_price"], r["quantity"])
            if p and float(p) > 0:
                per_units.append(float(p))
        except (TypeError, ValueError):
            continue

    if not per_units:
        return {}

    per_units.sort()
    n = len(per_units)
    median = per_units[n // 2] if n % 2 else (per_units[n // 2 - 1] + per_units[n // 2]) / 2
    return {
        "ceiling": round(median, 2),
        "low": round(per_units[0], 2),
        "high": round(per_units[-1], 2),
        "sample_count": n,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Per-record panel composer
# ─────────────────────────────────────────────────────────────────────────────

def compute_panel(
    items: Iterable[dict],
    contact_email: str = "",
    exclude_quote_number: str = "",
    *,
    conn=None,
) -> dict:
    """Compute per-line buyer-history + SCPRS-ceiling intel for an
    iterable of items. One DB connection re-used across all lines.

    Each item is keyed by 1-based line_no in the result. Items missing
    a description AND a part_number get an empty dict (no signal to show).

    `conn` may be passed in by the caller (test isolation, transaction
    scoping); when omitted, opens a fresh `get_db()`.
    """
    items = list(items or [])
    if not items:
        return {"by_line": {}}

    if conn is None:
        from src.core.db import get_db
        with get_db() as _conn:
            return _compute_panel_inner(items, contact_email,
                                         exclude_quote_number, _conn)
    return _compute_panel_inner(items, contact_email,
                                 exclude_quote_number, conn)


def _compute_panel_inner(items, contact_email, exclude_quote_number, conn) -> dict:
    by_line = {}
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            by_line[idx] = {}
            continue
        desc = (item.get("description") or "").strip()
        pn = (item.get("part_number")
              or item.get("mfg_number")
              or item.get("item_number") or "").strip()
        if not desc and not pn:
            by_line[idx] = {}
            continue
        try:
            last_won = last_won_for_buyer(
                conn, contact_email, desc, pn,
                exclude_quote_number=exclude_quote_number,
            )
        except Exception as e:
            log.debug("last_won_for_buyer failed for line %d: %s", idx, e)
            last_won = {}
        try:
            scprs = scprs_ceiling_for_item(conn, desc, pn)
        except Exception as e:
            log.debug("scprs_ceiling_for_item failed for line %d: %s", idx, e)
            scprs = {}
        by_line[idx] = {"last_won": last_won, "scprs_ceiling": scprs}
    return {"by_line": by_line}
