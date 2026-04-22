"""
Unified Pricing Oracle v2
One function for all pricing. Merges all data sources,
applies time decay, returns the definitive answer.
"""
import logging
import re
import math
import json
import time
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Any, Dict

log = logging.getLogger("reytech.pricing_oracle")

# ─── Skip ledger ──────────────────────────────────────────────────────────────
# Each `_search_X` helper queries one historical-pricing data source. When
# the underlying table is missing, malformed, or its column shape drifts,
# the helper used to swallow the exception with `log.debug("X search: %s", e)`
# and return `[]`. The caller (`get_pricing`) then quietly omitted that
# source from `sources_used` — operators saw a thinner price recommendation
# with no signal that a data source had crashed.
#
# The ledger lets routes/orchestrator drain skips after a pricing call and
# surface the degraded data sources via the standard 3-channel envelope.
from src.core.dependency_check import Severity, SkipReason  # noqa: E402

_SKIP_LEDGER: list[SkipReason] = []


def _record_skip(skip: SkipReason) -> None:
    """Append a skip to the module ledger; routes/orchestrator drain later."""
    _SKIP_LEDGER.append(skip)
    log.warning(skip.format_for_log())


def drain_skips() -> list[SkipReason]:
    """Pop and return every skip recorded since the last drain. Destructive
    so two consecutive calls do not double-warn."""
    drained = list(_SKIP_LEDGER)
    _SKIP_LEDGER.clear()
    return drained


def get_pricing(description, quantity=1, cost=None, item_number="",
                department="", force_refresh=False, qty_per_uom=1,
                line_count=None):
    """THE pricing function. Call this for everything.
    qty_per_uom: pack size (e.g., 200 for a box of 200 markers). Used to
    normalize cost to per-unit for proper comparison with market data.
    line_count: total line count on the quote/PC (BUILD-2). When the
    caller knows this, the volume-aware band narrows by line-count
    bucket; missing it falls to the mid-density 'lc_4_15' default."""
    import sqlite3
    from src.core.db import DB_PATH

    result = {
        "description": description, "quantity": quantity,
        "matched_item": None, "confidence": 0,
        "cost": {}, "market": {}, "recommendation": {},
        "strategies": [], "tiers": [], "competitors": [],
        "cross_sell": [], "sources_used": [],
    }

    if not description or len(description.strip()) < 3:
        return result

    db = sqlite3.connect(DB_PATH, timeout=10)

    # Step 1: Check item memory
    memory = _check_item_memory(db, description, item_number)
    if memory:
        result["matched_item"] = memory
        result["confidence"] = memory.get("confidence", 0.95)
        result["sources_used"].append("item_memory")
        description = memory.get("canonical_description", description)

    # Step 2: Locked cost
    locked = _get_locked_cost(db, description, item_number)
    if locked:
        result["cost"] = locked
        result["sources_used"].append("supplier_costs")
        cost = locked.get("locked_cost") or cost
    if cost:
        result["cost"]["provided_cost"] = cost

    # Step 3: Gather market prices (won_quotes + winning_prices first — richest data)
    market_prices = []
    source_counts = {}
    for fn, name in [
        (_search_won_quotes, "won_quotes"),
        (_search_winning_prices, "winning_prices"),
        (_search_scprs_catalog, "scprs_catalog"),
        (_search_po_lines, "scprs_po_lines"),
        (_search_product_catalog, "product_catalog"),
    ]:
        hits = fn(db, description, item_number)
        if hits:
            source_counts[name] = len(hits)
            market_prices.extend(hits)
            result["sources_used"].append(name)
    result["source_counts"] = source_counts

    # Step 4: Analyze market
    result["market"] = _analyze_market_prices(market_prices, quantity)

    # Step 5: Competitors
    result["competitors"] = _get_competitor_breakdown(market_prices)

    # Step 6: Recommendation (V3: with calibration + win history)
    category = _classify_item_category(description)
    result["category"] = category
    rec = _calculate_recommendation(cost, result["market"], quantity,
                                     category=category, agency=department, _db=db,
                                     qty_per_uom=qty_per_uom,
                                     win_history=result.get("matched_item"),
                                     line_count=line_count)
    result["strategies"] = rec.pop("strategies", [])
    result["tiers"] = rec.pop("tiers", [])
    result["recommendation"] = rec

    # Step 6b: Volume-Aware band (Phase B). Historical line-margin band
    # for this (agency, qty_bucket). Surfaced as supplementary signal;
    # becomes a ceiling constraint when flag is on and sample is dense.
    try:
        from src.core.volume_aware_pricing import volume_aware_ceiling, get_volume_band
        va_cost = (cost if cost and cost > 0
                   else (result.get("cost", {}) or {}).get("locked_cost") or 0)
        va_band = get_volume_band(department or "", quantity, line_count)
        if va_band:
            result["volume_aware_band"] = va_band
        va = volume_aware_ceiling(va_cost, department or "", quantity,
                                   line_count) if va_cost else None
        if va:
            result["volume_aware"] = va
    except Exception as _vae:
        log.debug("volume_aware: %s", _vae)

    # Step 7: Cross-sell
    result["cross_sell"] = _get_cross_sell(db, description)

    db.close()
    return result


def _check_item_memory(db, description, item_number=""):
    """Return ALL stored fields for a matched item."""
    _cols = """canonical_description, canonical_item_number, product_url, mfg_number,
               supplier, last_cost, confidence, times_confirmed, asin, uom,
               supplier_url, last_sell_price, mfg_name"""

    def _row_to_dict(row, match_type):
        if not row:
            return None
        return {
            "canonical_description": row[0] or "", "canonical_item_number": row[1] or "",
            "product_url": row[2] or "", "mfg_number": row[3] or "",
            "supplier": row[4] or "", "last_cost": row[5] or 0,
            "confidence": row[6] or 0, "times_confirmed": row[7] or 0,
            "asin": row[8] or "" if len(row) > 8 else "",
            "uom": row[9] or "" if len(row) > 9 else "",
            "supplier_url": row[10] or "" if len(row) > 10 else "",
            "last_sell_price": row[11] or 0 if len(row) > 11 else 0,
            "mfg_name": row[12] or "" if len(row) > 12 else "",
            "match_type": match_type,
        }

    try:
        if item_number:
            row = db.execute(f"""
                SELECT {_cols} FROM item_mappings
                WHERE LOWER(original_item_number)=LOWER(?) AND confirmed=1 LIMIT 1
            """, (item_number,)).fetchone()
            result = _row_to_dict(row, "exact_item")
            if result:
                return result

        row = db.execute(f"""
            SELECT {_cols} FROM item_mappings WHERE confirmed=1
            AND (LOWER(original_description)=LOWER(?) OR LOWER(canonical_description)=LOWER(?)) LIMIT 1
        """, (description, description)).fetchone()
        result = _row_to_dict(row, "exact_desc")
        if result:
            return result
    except Exception as _e:
        log.debug("suppressed: %s", _e)
    return None


def _get_locked_cost(db, description, item_number=""):
    try:
        params = []
        where = "expires_at > datetime('now')"
        if item_number:
            where += " AND (item_number=? OR LOWER(description) LIKE ?)"
            params.extend([item_number, f"%{description.lower()[:50]}%"])
        else:
            where += " AND LOWER(description) LIKE ?"
            params.append(f"%{description.lower()[:50]}%")

        row = db.execute(f"""
            SELECT cost, supplier, source, confirmed_at, expires_at
            FROM supplier_costs WHERE {where} ORDER BY confirmed_at DESC LIMIT 1
        """, params).fetchone()
        if row:
            return {"locked_cost": row[0], "cost_source": row[2], "cost_supplier": row[1],
                    "cost_confirmed": row[3], "cost_expires": row[4], "cost_fresh": True}
    except Exception as _e:
        log.debug("suppressed: %s", _e)
    return None


def _scprs_per_unit(price, qty):
    """Normalize SCPRS-style prices to per-unit.

    SCPRS stores line totals in ``unit_price`` for multi-qty rows (a 5×$20 line
    shows unit_price=$100). Blindly dividing by qty corrupts rows that are
    already per-unit (qty=3, p=$5 → $1.67). The ``p > qty * 2`` guard keeps
    small per-unit prices intact while still normalizing obvious line totals.

    IN-2 (2026-04-21): ported from _search_winning_prices (the sibling that
    already got this right) into the won_quotes/scprs_catalog/po_lines
    searches that were dividing naively.
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


def _search_won_quotes(db, description, item_number=""):
    """Search won_quotes KB — SCPRS competitors' winning prices.
    This is the richest data source: actual prices that won contracts."""
    prices = []
    try:
        token_groups = _tokenize(description)[:4]
        if not token_groups:
            return prices
        where_parts = []
        params = []
        for group in token_groups:
            if len(group) == 1:
                where_parts.append("LOWER(normalized_description) LIKE ?")
                params.append(f"%{group[0]}%")
            else:
                or_clause = " OR ".join(["LOWER(normalized_description) LIKE ?" for _ in group])
                where_parts.append(f"({or_clause})")
                params.extend([f"%{v}%" for v in group])
        where = " AND ".join(where_parts)
        if item_number:
            where = f"({where}) OR LOWER(item_number) = ?"
            params.append(item_number.lower())
        rows = db.execute(f"""
            SELECT description, unit_price, quantity, supplier, department,
                   award_date, category, confidence
            FROM won_quotes WHERE {where} ORDER BY award_date DESC LIMIT 20
        """, params).fetchall()
        for r in rows:
            p = r[1]
            qty = r[2] or 1
            if p and p > 0:
                # IN-2: Normalize via shared helper (guards against dividing
                # already-per-unit rows into pennies).
                per_unit = _scprs_per_unit(p, qty)
                prices.append({"price": per_unit, "description": r[0], "quantity": qty,
                               "source": "won_quotes",
                               "is_reytech": "REYTECH" in (r[3] or "").upper()})
    except Exception as e:
        _record_skip(SkipReason(
            name="won_quotes",
            reason=f"{type(e).__name__}: {e}",
            severity=Severity.WARNING,
            where="pricing_oracle_v2._search_won_quotes",
        ))
    return prices


def _search_winning_prices(db, description, item_number=""):
    """Search winning_prices — OUR won order prices (items Reytech sold).
    These are the most authoritative: prices we actually won contracts at."""
    prices = []
    try:
        token_groups = _tokenize(description)[:3]
        if not token_groups:
            return prices
        where_parts = []
        params = []
        for group in token_groups:
            if len(group) == 1:
                where_parts.append("LOWER(description) LIKE ?")
                params.append(f"%{group[0]}%")
            else:
                or_clause = " OR ".join(["LOWER(description) LIKE ?" for _ in group])
                where_parts.append(f"({or_clause})")
                params.extend([f"%{v}%" for v in group])
        where = " AND ".join(where_parts)
        if item_number:
            where = f"({where}) OR LOWER(part_number) = ?"
            params.append(item_number.lower())
        rows = db.execute(f"""
            SELECT description, sell_price, qty, supplier, agency,
                   recorded_at, cost, margin_pct
            FROM winning_prices WHERE {where} ORDER BY recorded_at DESC LIMIT 15
        """, params).fetchall()
        for r in rows:
            p = r[1]
            qty = r[2] or 1
            if p and p > 0:
                # IN-11: use shared helper so all sites agree on the guard.
                per_unit = _scprs_per_unit(p, qty)
                prices.append({"price": per_unit, "description": r[0], "quantity": qty,
                               "supplier": r[3] or "", "department": r[4] or "",
                               "date": r[5] or "", "cost": r[6] or 0,
                               "margin": r[7] or 0, "source": "winning_prices",
                               "is_reytech": True})
    except Exception as e:
        _record_skip(SkipReason(
            name="winning_prices",
            reason=f"{type(e).__name__}: {e}",
            severity=Severity.WARNING,
            where="pricing_oracle_v2._search_winning_prices",
        ))
    return prices


def _search_scprs_catalog(db, description, item_number=""):
    prices = []
    try:
        token_groups = _tokenize(description)[:4]
        if not token_groups:
            return prices
        where_parts = []
        params = []
        for group in token_groups:
            if len(group) == 1:
                where_parts.append("LOWER(description) LIKE ?")
                params.append(f"%{group[0]}%")
            else:
                or_clause = " OR ".join(["LOWER(description) LIKE ?" for _ in group])
                where_parts.append(f"({or_clause})")
                params.extend([f"%{v}%" for v in group])
        where = " AND ".join(where_parts)
        if item_number:
            where = f"({where}) OR LOWER(mfg_number) = ?"
            params.append(item_number.lower())
        rows = db.execute(f"""
            SELECT description, last_unit_price, last_quantity, last_uom,
                   last_supplier, last_department, last_date, times_seen
            FROM scprs_catalog WHERE {where} ORDER BY times_seen DESC LIMIT 20
        """, params).fetchall()
        for r in rows:
            if r[1] and r[1] > 0:
                qty = r[2] or 1
                # IN-2: shared SCPRS per-unit normalization with false-positive guard.
                per_unit = _scprs_per_unit(r[1], qty)
                prices.append({"price": per_unit, "description": r[0], "quantity": qty,
                               "uom": r[3] or "", "supplier": r[4] or "", "department": r[5] or "",
                               "date": r[6] or "", "source": "scprs_catalog",
                               "is_reytech": "REYTECH" in (r[4] or "").upper()})
    except Exception as e:
        _record_skip(SkipReason(
            name="scprs_catalog",
            reason=f"{type(e).__name__}: {e}",
            severity=Severity.WARNING,
            where="pricing_oracle_v2._search_scprs_catalog",
        ))
    return prices


def _search_po_lines(db, description, item_number=""):
    prices = []
    try:
        token_groups = _tokenize(description)[:3]
        if not token_groups:
            return prices
        where_parts = []
        params = []
        for group in token_groups:
            if len(group) == 1:
                where_parts.append("LOWER(l.description) LIKE ?")
                params.append(f"%{group[0]}%")
            else:
                or_clause = " OR ".join(["LOWER(l.description) LIKE ?" for _ in group])
                where_parts.append(f"({or_clause})")
                params.extend([f"%{v}%" for v in group])
        where = " AND ".join(where_parts)
        rows = db.execute(f"""
            SELECT l.description, l.unit_price, l.quantity, l.uom,
                   m.supplier, m.dept_name, m.start_date, m.buyer_email
            FROM scprs_po_lines l JOIN scprs_po_master m ON l.po_number=m.po_number
            WHERE {where} ORDER BY m.start_date DESC LIMIT 25
        """, params).fetchall()
        for r in rows:
            try:
                p = float(str(r[1] or "0").replace("$", "").replace(",", ""))
            except (ValueError, TypeError):
                continue
            if p > 0:
                qty = float(str(r[2] or "1").replace(",", ""))
                # IN-2: SCPRS unit_price is sometimes a line total — shared guard.
                per_unit = _scprs_per_unit(p, qty)
                prices.append({"price": per_unit, "description": r[0],
                               "quantity": qty, "uom": r[3] or "",
                               "supplier": r[4] or "", "department": r[5] or "",
                               "date": r[6] or "", "buyer_email": r[7] or "",
                               "source": "scprs_po_lines",
                               "is_reytech": "REYTECH" in (r[4] or "").upper()})
    except Exception as e:
        _record_skip(SkipReason(
            name="scprs_po_lines",
            reason=f"{type(e).__name__}: {e}",
            severity=Severity.WARNING,
            where="pricing_oracle_v2._search_po_lines",
        ))
    return prices


def _search_product_catalog(db, description, item_number=""):
    prices = []
    try:
        token_groups = _tokenize(description)[:3]
        if not token_groups:
            return prices
        # Use OR across all variant groups for product catalog (broader match)
        all_variants = []
        for group in token_groups:
            all_variants.extend(group)
        where = " OR ".join(["LOWER(name) LIKE ?" for _ in all_variants])
        params = [f"%{v}%" for v in all_variants]
        rows = db.execute(f"""
            SELECT name, sell_price, cost, best_supplier FROM product_catalog WHERE {where} LIMIT 10
        """, params).fetchall()
        for r in rows:
            p = r[1] or r[2]
            if p and p > 0:
                prices.append({"price": p, "description": r[0], "supplier": r[3] or "",
                               "source": "product_catalog", "is_reytech": True})
    except Exception as e:
        _record_skip(SkipReason(
            name="product_catalog",
            reason=f"{type(e).__name__}: {e}",
            severity=Severity.WARNING,
            where="pricing_oracle_v2._search_product_catalog",
        ))
    return prices


def _normalize_to_per_unit(price, description, quantity=1, uom=""):
    """Normalize any price to per-unit (per-each) basis."""
    desc = (description or "").upper()
    total_units = 1
    detected = ""
    # X/BX Y BXS/CS
    m = re.search(r'(\d+)\s*/\s*(?:BX|BOX|PK|PKG)\s+(\d+)\s*(?:BXS?|BOXES|PKS?|PKGS?)\s*/\s*(?:CS|CASE|CT|CTN)', desc)
    if m:
        total_units = int(m.group(1)) * int(m.group(2))
        detected = m.group(0)
    if total_units == 1:
        m = re.search(r'(\d+)\s*/\s*(?:BX|BOX|PK|PKG|BAG|RL|ROLL|BT|BTL|EA|CT|CASE)', desc)
        if m:
            total_units = int(m.group(1))
            detected = m.group(0)
    if total_units == 1:
        m = re.search(r'(?:BOX|CASE|PACK|PKG|PACKAGE)\s+(?:OF\s+)?(\d+)', desc)
        if m:
            total_units = int(m.group(1))
            detected = m.group(0)
    if total_units == 1:
        m = re.search(r'(\d+)\s+PER\s+(?:BOX|BX|CASE|CS|PACK|PK)', desc)
        if m:
            total_units = int(m.group(1))
            detected = m.group(0)
    if total_units == 1:
        m = re.search(r'(\d+)\s*(?:CT|COUNT|EA)\b', desc)
        if m and int(m.group(1)) > 1:
            total_units = int(m.group(1))
            detected = m.group(0)
    if total_units > 10000:
        total_units = 1
        detected = ""
    per_unit = price / total_units if total_units > 0 else price
    return {"per_unit": round(per_unit, 6), "total_units": total_units,
            "detected": detected, "original_price": price}


def _analyze_market_prices(market_prices, request_qty):
    """Time-weighted market analysis with proper UOM normalization."""
    if not market_prices:
        return {"data_points": 0, "freshness": "none"}
    now = datetime.now()
    weighted = []
    for mp in market_prices:
        price = mp.get("price", 0)
        if not price or price <= 0:
            continue
        norm = _normalize_to_per_unit(price, mp.get("description", ""),
                                      mp.get("quantity", 1), mp.get("uom", ""))
        per_unit = norm["per_unit"]
        if per_unit < 0.001 or per_unit > 50000:
            continue
        date_str = mp.get("date", "")
        weight = 0.3
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
            try:
                dt = datetime.strptime(str(date_str).strip(), fmt)
                weight = math.pow(0.5, max(0, (now - dt).days) / 180)
                break
            except (ValueError, TypeError):
                continue
        covid_cats = ['glove', 'mask', 'gown', 'sanitiz', 'disinfect', 'ppe', 'n95']
        desc_lower = mp.get("description", "").lower()
        if any(c in desc_lower for c in covid_cats):
            for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(str(date_str).strip(), fmt)
                    if datetime(2020, 3, 1) <= dt <= datetime(2021, 12, 31):
                        weight *= 0.05
                    break
                except (ValueError, TypeError):
                    continue
        weighted.append({"price": per_unit, "raw_price": price,
                         "total_units": norm["total_units"], "pack_detected": norm["detected"],
                         "weight": round(weight, 4), "supplier": mp.get("supplier", ""),
                         "date": date_str, "is_reytech": mp.get("is_reytech", False),
                         "source": mp.get("source", "")})

    # Outlier removal: median-based clustering
    # Problem: $652/case and $8/box are in different UOMs.
    # If normalization couldn't detect pack size, raw price is an outlier.
    if len(weighted) >= 3:
        sorted_prices = sorted(w["price"] for w in weighted)
        median = sorted_prices[len(sorted_prices) // 2]
        if median > 0 and max(sorted_prices) / min(sorted_prices) > 50:
            filtered = [w for w in weighted
                        if median / 10 <= w["price"] <= median * 10]
            if len(filtered) >= 2:
                removed = len(weighted) - len(filtered)
                log.info("Oracle: removed %d outliers (median=$%.4f, "
                         "range kept: $%.4f-$%.4f)",
                         removed, median, median / 10, median * 10)
                weighted = filtered

    if not weighted:
        return {"data_points": 0, "freshness": "none"}
    total_w = sum(w["weight"] for w in weighted)
    wavg = sum(w["price"] * w["weight"] for w in weighted) / total_w if total_w > 0 else 0
    reytech = [w for w in weighted if w["is_reytech"]]
    comps = [w for w in weighted if not w["is_reytech"]]
    rt_wavg = None
    if reytech:
        rt_w = sum(w["weight"] for w in reytech)
        rt_wavg = sum(w["price"] * w["weight"] for w in reytech) / rt_w if rt_w > 0 else None
    comp_wavg = comp_low = comp_high = None
    if comps:
        c_w = sum(w["weight"] for w in comps)
        comp_wavg = sum(w["price"] * w["weight"] for w in comps) / c_w if c_w > 0 else None
        hw = [w for w in comps if w["weight"] > 0.3]
        if hw:
            comp_low = min(w["price"] for w in hw)
            comp_high = max(w["price"] for w in hw)
    best_w = max(w["weight"] for w in weighted)
    freshness = "stale" if best_w < 0.3 else ("aging" if best_w < 0.5 else "fresh")
    all_p = [w["price"] for w in weighted]
    return {
        "data_points": len(weighted), "freshness": freshness,
        "weighted_avg": round(wavg, 4) if wavg else None,
        "low": round(min(all_p), 4), "high": round(max(all_p), 4),
        "reytech_avg": round(rt_wavg, 4) if rt_wavg else None,
        "competitor_avg": round(comp_wavg, 4) if comp_wavg else None,
        "competitor_low": round(comp_low, 4) if comp_low else None,
        "competitor_high": round(comp_high, 4) if comp_high else None,
        "unique_suppliers": len(set(w["supplier"] for w in weighted if w["supplier"])),
        "normalization": "All prices normalized to per-unit",
    }


# BUILD-3: Dollar-floor profit guard. A %-based floor ($ cost × 1.15) gives
# pennies of gross profit on cheap items: a $0.80 swab at 15% markup = $0.12 GP
# per unit × 2 units = $0.24 for the whole line. That is less than the cost of
# a single email round-trip. This guard enforces an absolute-dollar floor for
# total line gross profit (price − cost × qty) so the system never recommends
# a line that's not worth quoting.
_DOLLAR_FLOOR_DEFAULT = 3.0


def _apply_win_probability(result, agency, _db):
    """Enrich `result` with P(win) evaluated at the FINAL recommended markup.

    The buyer_curve block (when present) reports P(win) at the EV-optimal
    markup — but the actual shipped markup_pct may differ after ceiling,
    floor, dollar-floor, and win-anchor adjustments. This helper evaluates
    `buyer_win_probability` at the *final* markup so the UI can surface
    the probability the quote we're about to send will actually win.

    Runs AFTER _apply_dollar_floor so it sees the bumped markup_pct.
    Falls through silently when the markup is missing or the curve lookup
    raises — the result just lacks the key, never dies.
    """
    try:
        from src.core.flags import get_flag
        if not get_flag("oracle.win_probability_in_result", True):
            return result
    except Exception:
        pass
    try:
        markup = result.get("markup_pct")
        if markup is None:
            return result
        # Pass the buyer_curve back through (already fetched upstream) so
        # this helper doesn't re-hit the DB. When absent, buyer_win_probability
        # still works via the cold-start prior (0.85 decaying to 0.30).
        cached = None
        bc = result.get("buyer_curve") or {}
        buckets = bc.get("buckets") or []
        ts = bc.get("total_samples", 0)
        if buckets and ts > 0:
            won = bc.get("won", 0)
            cached = {
                "buckets": buckets,
                "total_samples": ts,
                "won": won,
                "lost": bc.get("lost", 0),
                "global_win_rate": (won / ts) if ts > 0 else 0.0,
            }
        p = buyer_win_probability(agency or "", float(markup),
                                   db=_db, _curve=cached)
        result["win_probability"] = round(float(p), 3)
    except Exception as e:
        log.debug("win_probability enrich skipped: %s", e)
    return result


def _apply_dollar_floor(result, cost, qty):
    """Bump quote_price up so the line clears a minimum gross-profit floor in
    absolute dollars. No-op when cost/qty are unusable or the line already
    clears the floor. Idempotent. Feature-flag gated."""
    try:
        from src.core.flags import get_flag
        if not get_flag("oracle.dollar_floor", True):
            return result
        min_gp = float(get_flag("oracle.min_gross_profit_dollars",
                                _DOLLAR_FLOOR_DEFAULT))
    except Exception:
        min_gp = _DOLLAR_FLOOR_DEFAULT
    if min_gp <= 0:
        return result
    try:
        c = float(cost) if cost is not None else 0.0
        q = float(qty) if qty else 0.0
    except (TypeError, ValueError):
        return result
    if c <= 0 or q <= 0:
        return result
    qp = result.get("quote_price")
    if qp is None:
        return result
    try:
        qp = float(qp)
    except (TypeError, ValueError):
        return result
    gp_total = (qp - c) * q
    if gp_total >= min_gp:
        return result
    floor_price = round(c + (min_gp / q), 2)
    if floor_price <= qp:
        return result
    new_markup = ((floor_price - c) / c * 100) if c > 0 else 0
    result["dollar_floor_applied"] = {
        "original_price": round(qp, 2),
        "floor_price": floor_price,
        "min_gross_profit": round(min_gp, 2),
        "original_gp_total": round(gp_total, 2),
        "qty": q,
    }
    result["quote_price"] = floor_price
    result["markup_pct"] = round(new_markup, 1)
    note = (f" | Bumped to ${floor_price:.2f} to clear "
            f"${min_gp:.2f} min GP (was ${gp_total:.2f})")
    result["rationale"] = (result.get("rationale") or "") + note
    return result


def _calculate_recommendation(cost, market, quantity, category=None, agency=None,
                               _db=None, qty_per_uom=1, win_history=None,
                               line_count=None):
    result = {"strategies": [], "tiers": [], "rationale": "",
              "quote_price": None, "markup_pct": None, "confidence": "low",
              "calibration": None, "win_anchor": None}
    qty = float(quantity) if quantity else 1
    qpu = int(qty_per_uom) if qty_per_uom and qty_per_uom > 1 else 1
    comp_avg = market.get("competitor_avg")
    # Fallback: if no competitor-only avg, use our own past wins or weighted avg
    if not comp_avg and market.get("data_points", 0) > 0:
        comp_avg = market.get("reytech_avg") or market.get("weighted_avg")
    comp_low = market.get("competitor_low") or comp_avg
    has_cost = cost is not None and cost > 0
    has_market = market.get("data_points", 0) > 0 and comp_avg

    # WIN HISTORY ANCHOR — the strongest signal we have.
    # If we've won this exact item before (confirmed 3+ times), that price
    # is more reliable than any SCPRS/catalog estimate.
    win_price = None
    win_times = 0
    if win_history and isinstance(win_history, dict):
        _wp = win_history.get("last_sell_price", 0)
        _wt = win_history.get("times_confirmed", 0)
        if _wp and _wp > 0 and _wt >= 3:
            win_price = float(_wp)
            win_times = int(_wt)
            result["win_anchor"] = {"price": win_price, "times": win_times}
            log.info("Win anchor: $%.2f (%dx confirmed)", win_price, win_times)
            if has_cost and win_price < cost:
                log.warning("Win anchor $%.2f is BELOW current cost $%.2f — previous win price is now unprofitable", win_price, cost)

    # UOM normalization: SCPRS/market data is already per-line (not per-unit).
    # Cost is per-pack/per-UOM. Do NOT multiply market prices by qpu — that inflates
    # them by 100x for items with 100 units/pack. Market prices and cost are in the
    # same unit (what the buyer pays per UOM ordered).
    # (Removed: comp_avg *= qpu was causing 2000%+ markups on high-qpu items)

    # V3: Read calibration data if available
    cal = None
    if _db and category:
        cal = _get_calibration(_db, category, agency or "")
        if cal and cal.get("sample_size", 0) >= _CAL_MIN_SAMPLES:
            result["calibration"] = {
                "win_rate": round(cal["win_rate"] * 100),
                "sample_size": cal["sample_size"],
                "recommended_max_markup": round(cal["recommended_max_markup"], 1),
                "avg_winning_margin": round(cal["avg_winning_margin"], 1),
                "category": category,
            }

    # V5 Phase 2: Institution-specific pricing profile
    inst_profile = None
    if _db and agency:
        inst_profile = _get_institution_profile(_db, agency, category or "general")
        if inst_profile and inst_profile.get("total_quotes", 0) >= _CAL_MIN_SAMPLES:
            result["institution_profile"] = {
                "institution": agency,
                "price_sensitivity": inst_profile["price_sensitivity"],
                "win_rate": round(inst_profile["win_rate"] * 100),
                "avg_winning_markup": round(inst_profile["avg_winning_markup"], 1),
                "sample_size": inst_profile["total_quotes"],
                "source": "history",
            }
            # Blend institution markup with category calibration (50/50)
            if cal and cal.get("sample_size", 0) >= _CAL_MIN_SAMPLES:
                blended = (cal["recommended_max_markup"] + inst_profile["avg_winning_markup"]) / 2
                cal["recommended_max_markup"] = blended
                result["calibration"]["blended_with_institution"] = True
                result["calibration"]["recommended_max_markup"] = round(blended, 1)

    # Fallback: if no real history surfaced, use agency_config defaults so
    # the panel shows *something* (sensitivity, target markup, payment terms)
    # instead of being silently blank. This was the 2026-04-18 CalVet gap —
    # only CCHCS had enough quote history to populate the V5 block.
    if "institution_profile" not in result and agency:
        try:
            from src.core.agency_quote_profile import resolve_agency_profile
            fallback = resolve_agency_profile(agency)
            if fallback:
                result["institution_profile"] = fallback
        except Exception as e:
            log.debug("agency_quote_profile fallback skipped: %s", e)

    # V5.5: Per-buyer win-rate curve. When we have ≥ _CURVE_MIN_SAMPLES
    # won+lost quotes for this institution, fit a bucketed response
    # curve and pick the markup that maximizes markup × P(win). This
    # dominates the scalar avg_winning_markup above — a buyer who wins
    # 70% of 25%-markup quotes and 30% of 40%-markup quotes has an
    # *expected profit* peak at ~28%, not at their historical average.
    # Falls through silently when data is thin.
    buyer_opt = None
    if _db and agency:
        try:
            buyer_opt = optimal_markup_for_expected_profit(agency, _db)
        except Exception as e:
            log.debug("V5.5 buyer curve lookup error: %s", e)
            buyer_opt = None
    if buyer_opt and buyer_opt.get("sufficient"):
        curve_summary = buyer_opt.get("curve") or {}
        result["buyer_curve"] = {
            "optimal_markup_pct": buyer_opt["markup_pct"],
            "expected_value": buyer_opt["expected_value"],
            "win_probability": buyer_opt["win_probability"],
            "total_samples": curve_summary.get("total_samples", 0),
            "won": curve_summary.get("won", 0),
            "lost": curve_summary.get("lost", 0),
            "buckets": curve_summary.get("buckets", []),
        }
        # Override the calibration ceiling with the EV-maximizing markup
        # so the downstream `ceiling = cost * (1 + markup/100)` uses it.
        if cal:
            cal["recommended_max_markup"] = buyer_opt["markup_pct"]
            result["calibration"]["recommended_max_markup"] = buyer_opt["markup_pct"]
            result["calibration"]["source"] = "buyer_curve_v5_5"
        else:
            # No category calibration — synthesize one so the
            # downstream ceiling math picks it up.
            cal = {
                "recommended_max_markup": buyer_opt["markup_pct"],
                "sample_size": curve_summary.get("total_samples", 0),
                "win_rate": curve_summary.get("total_samples", 0) and
                            (curve_summary.get("won", 0) / curve_summary["total_samples"]),
                "avg_winning_margin": buyer_opt["markup_pct"],
            }
            result["calibration"] = {
                "win_rate": round((cal["win_rate"] or 0) * 100),
                "sample_size": cal["sample_size"],
                "recommended_max_markup": buyer_opt["markup_pct"],
                "avg_winning_margin": buyer_opt["markup_pct"],
                "category": category or "general",
                "source": "buyer_curve_v5_5",
            }

    # If we have a strong win history, that's the best ceiling — we KNOW this
    # price wins. Use it instead of SCPRS/market estimates.
    if has_cost and win_price and win_price > cost:
        ceiling = round(win_price, 2)
        ceiling_m = ((ceiling - cost) / cost * 100) if cost > 0 else 0
        # If market data would suggest a higher price, note it but don't use it
        market_ceiling = round(comp_avg * 0.98, 2) if has_market and comp_avg else 0
        result["strategies"] = [
            {"name": "Win Price", "price": ceiling, "markup_pct": round(ceiling_m, 1),
             "margin_per_unit": round(ceiling - cost, 2), "margin_total": round((ceiling - cost) * qty, 2)},
        ]
        if has_market and market_ceiling > ceiling:
            result["strategies"].append(
                {"name": "Market Ceiling", "price": market_ceiling,
                 "markup_pct": round(((market_ceiling - cost) / cost * 100) if cost > 0 else 0, 1),
                 "margin_per_unit": round(market_ceiling - cost, 2),
                 "margin_total": round((market_ceiling - cost) * qty, 2)})
        floor = round(cost * 1.15, 2)
        result["strategies"].append(
            {"name": "Floor", "price": floor, "markup_pct": 15,
             "margin_per_unit": round(floor - cost, 2), "margin_total": round((floor - cost) * qty, 2)})
        for pct in [15, 20, 25, 30, 35, 40, 45, 50]:
            tp = round(cost * (1 + pct / 100), 2)
            result["tiers"].append({"pct": pct, "price": tp, "margin_total": round((tp - cost) * qty, 2),
                                     "beats_avg": tp < (comp_avg or 99999), "beats_low": tp < (comp_low or 99999)})
        result.update({"quote_price": ceiling, "markup_pct": round(ceiling_m, 1), "confidence": "high",
                       "rationale": f"Won {win_times}x at ${ceiling:.2f} ({ceiling_m:.0f}% on ${cost:.2f})"})
        _apply_dollar_floor(result, cost, qty)
        _apply_win_probability(result, agency, _db)
        return result

    # Phase B: Volume-Aware band (agency × qty_bucket historical median margin).
    # When the flag is on and we have ≥ 10 historical line samples for this
    # (agency, qty_bucket), cap the ceiling at the p75 margin — this
    # reflects what has *historically* won for the same buyer+size combo.
    # Skipped silently when data is thin or flag is off.
    va_ceiling = None
    va_band_info = None
    try:
        from src.core.flags import get_flag
        from src.core.volume_aware_pricing import get_volume_band
        if has_cost and get_flag("oracle.volume_aware", True):
            vb = get_volume_band(agency or "", qty, line_count)
            if vb and vb.get("sample_size", 0) >= 10 and vb.get("p75_margin") is not None:
                va_ceiling = round(cost * (1 + float(vb["p75_margin"])), 2)
                va_band_info = {
                    "agency": vb["agency"],
                    "qty_bucket": vb["qty_bucket"],
                    "line_count_bucket": vb.get("line_count_bucket"),
                    "sample_size": vb["sample_size"],
                    "p25_margin_pct": round(float(vb["p25_margin"]) * 100, 1) if vb.get("p25_margin") is not None else None,
                    "p50_margin_pct": round(float(vb["p50_margin"]) * 100, 1) if vb.get("p50_margin") is not None else None,
                    "p75_margin_pct": round(float(vb["p75_margin"]) * 100, 1) if vb.get("p75_margin") is not None else None,
                    "used_fallback": vb.get("used_fallback", False),
                    "used_fallback_agency": vb.get("used_fallback_agency", False),
                    "used_fallback_lc": vb.get("used_fallback_lc", False),
                    "ceiling_price": va_ceiling,
                }
                result["volume_aware"] = va_band_info
    except Exception as _vae:
        log.debug("volume_aware ceiling: %s", _vae)

    if has_cost and has_market:
        # V5 Phase 5: Confidence-weighted pricing tiers based on data density
        dp = market.get("data_points", 0)
        if dp >= 50:
            data_tier = "aggressive"
            market_mult = 0.96  # tight band, price very close to market
        elif dp >= 10:
            data_tier = "moderate"
            market_mult = 0.98  # standard band
        elif dp >= 5:
            data_tier = "cautious"
            market_mult = 1.0   # don't undercut — match market
        elif dp >= 1:
            data_tier = "sparse"
            market_mult = 1.0
        else:
            data_tier = "blind"
            market_mult = 1.0
        result["data_confidence"] = data_tier

        # V3/V5: Use calibrated ceiling if available, otherwise data-tier multiplier
        if cal and cal.get("sample_size", 0) >= _CAL_MIN_SAMPLES:
            cal_ceiling = round(cost * (1 + cal["recommended_max_markup"] / 100), 2)
            market_ceiling = round(comp_avg * market_mult, 2)
            ceiling = min(cal_ceiling, market_ceiling)
        else:
            ceiling = round(comp_avg * market_mult, 2)

        # Phase B: Volume-Aware ceiling cap — prevents overpricing large orders
        # at small-order margins. Only applied when historical sample is dense
        # (≥10 lines for this agency × qty_bucket). Never raises the ceiling,
        # only caps it.
        if va_ceiling is not None and va_ceiling < ceiling:
            ceiling = va_ceiling
            if va_band_info:
                va_band_info["applied"] = True

        # For sparse data, enforce minimum markup of 25%
        if data_tier == "sparse" and cost > 0:
            sparse_floor = round(cost * 1.25, 2)
            if ceiling < sparse_floor:
                ceiling = sparse_floor

        ceiling_m = ((ceiling - cost) / cost * 100) if cost > 0 else 0
        competitive = round(comp_low * 0.98, 2)
        comp_m = ((competitive - cost) / cost * 100) if cost > 0 else 0
        floor = round(cost * 1.15, 2)
        result["strategies"] = [
            {"name": "Maximize Margin", "price": ceiling, "markup_pct": round(ceiling_m, 1),
             "margin_per_unit": round(ceiling - cost, 2), "margin_total": round((ceiling - cost) * qty, 2)},
            {"name": "Undercut All", "price": competitive, "markup_pct": round(comp_m, 1),
             "margin_per_unit": round(competitive - cost, 2), "margin_total": round((competitive - cost) * qty, 2)},
            {"name": "Floor", "price": floor, "markup_pct": 15,
             "margin_per_unit": round(floor - cost, 2), "margin_total": round((floor - cost) * qty, 2)},
        ]
        for pct in [15, 20, 25, 30, 35, 40, 45, 50]:
            tp = round(cost * (1 + pct / 100), 2)
            result["tiers"].append({"pct": pct, "price": tp, "margin_total": round((tp - cost) * qty, 2),
                                     "beats_avg": tp < comp_avg, "beats_low": tp < comp_low})
        if ceiling > floor:
            cal_note = ""
            if cal and cal.get("sample_size", 0) >= _CAL_MIN_SAMPLES:
                cal_note = f" | Win rate: {cal['win_rate']:.0%} on {cal['sample_size']} quotes"
            tier_note = f" [{data_tier}: {dp} pts]"
            confidence = "high" if dp >= 10 else "medium"
            result.update({"quote_price": ceiling, "markup_pct": round(ceiling_m, 1), "confidence": confidence,
                           "rationale": f"Cost ${cost:.2f} -> ${ceiling:.2f} ({ceiling_m:.0f}%). Margin ${(ceiling-cost)*qty:,.2f}{cal_note}{tier_note}"})
        elif competitive > floor:
            result.update({"quote_price": competitive, "markup_pct": round(comp_m, 1), "confidence": "medium",
                           "rationale": f"Tight market. ${competitive:.2f} ({comp_m:.0f}%) [{data_tier}]"})
        else:
            result.update({"quote_price": floor, "markup_pct": 15, "confidence": "low",
                           "rationale": f"Competitors below floor ${floor:.2f} [{data_tier}]"})
    elif has_cost:
        # V5: "blind" tier — no market data at all, conservative 30% markup
        result["data_confidence"] = "blind"
        # Phase B: if we have a dense volume-aware band for this agency+qty,
        # use the p50 margin instead of a blind 30% — this is still more
        # principled than a flat default.
        if va_ceiling is not None and va_band_info and va_band_info.get("p50_margin_pct") is not None:
            p50_price = round(cost * (1 + va_band_info["p50_margin_pct"] / 100), 2)
            va_band_info["applied"] = True
            result.update({
                "quote_price": p50_price,
                "markup_pct": va_band_info["p50_margin_pct"],
                "confidence": "medium",
                "rationale": f"No market; volume-aware p50 for {va_band_info['agency']}/{va_band_info['qty_bucket']} (n={va_band_info['sample_size']})",
            })
        else:
            result.update({"quote_price": round(cost * 1.30, 2), "markup_pct": 30, "confidence": "low",
                           "rationale": f"No market data. 30% on ${cost:.2f} [blind]"})
    elif has_market:
        result["data_confidence"] = "moderate" if market.get("data_points", 0) >= 10 else "sparse"
        result.update({"quote_price": round(comp_avg * 0.92, 2), "confidence": "medium",
                       "rationale": f"No cost. 8% under ${comp_avg:.2f}"})
    else:
        result["data_confidence"] = "blind"
        result["rationale"] = "No data. Manual research needed."
    _apply_dollar_floor(result, cost, qty)
    _apply_win_probability(result, agency, _db)
    return result


def _get_competitor_breakdown(market_prices):
    """Top competitors with normalized per-unit prices."""
    by_sup = {}
    for mp in market_prices:
        s = mp.get("supplier", "")
        if not s or mp.get("is_reytech"):
            continue
        norm = _normalize_to_per_unit(mp.get("price", 0), mp.get("description", ""),
                                      mp.get("quantity", 1), mp.get("uom", ""))
        if norm["per_unit"] < 0.001 or norm["per_unit"] > 50000:
            continue
        if s not in by_sup:
            by_sup[s] = {"supplier": s, "prices": [], "department": mp.get("department", ""),
                         "buyer_email": mp.get("buyer_email", "")}
        by_sup[s]["prices"].append(norm["per_unit"])
    result = []
    for s, data in by_sup.items():
        avg = sum(data["prices"]) / len(data["prices"])
        result.append({"supplier": s, "avg_price": round(avg, 4),
                       "low": round(min(data["prices"]), 4), "high": round(max(data["prices"]), 4),
                       "data_points": len(data["prices"]), "department": data["department"],
                       "buyer_email": data["buyer_email"]})
    result.sort(key=lambda x: x["avg_price"])
    return result[:8]


def _get_cross_sell(db, description):
    """Items commonly ordered on the same PO."""
    try:
        token_groups = _tokenize(description)[:2]
        if not token_groups:
            return []
        flat_kw = [g[0] for g in token_groups]
        where = " AND ".join(["LOWER(description) LIKE ?" for _ in flat_kw])
        params = [f"%{k}%" for k in flat_kw]
        po_rows = db.execute(f"""
            SELECT DISTINCT po_number FROM scprs_po_lines WHERE {where} LIMIT 50
        """, params).fetchall()
        if not po_rows:
            return []
        po_nums = [r[0] for r in po_rows]
        placeholders = ",".join(["?" for _ in po_nums])
        exclude = f"NOT (LOWER(description) LIKE ?)"
        rows = db.execute(f"""
            SELECT description, COUNT(DISTINCT po_number) c FROM scprs_po_lines
            WHERE po_number IN ({placeholders}) AND description!='' AND {exclude}
            GROUP BY description HAVING c>=2 ORDER BY c DESC LIMIT 5
        """, po_nums + [f"%{flat_kw[0]}%"]).fetchall()
        return [{"description": r[0][:100], "co_occurrence": r[1]} for r in rows]
    except Exception:
        return []


def _tokenize(text):
    """Extract keywords with abbreviation expansion. Returns list of lists."""
    stop = {'the', 'a', 'an', 'and', 'or', 'for', 'of', 'to', 'in', 'with',
            'item', 'items', 'each', 'per', 'ea', 'cs', 'bx', 'pk', 'box',
            'case', 'pack', 'qty', 'options', 'option', 'size', 'type'}
    expansions = {
        'medium': ['medium', 'med'], 'large': ['large', 'lg', 'lrg'],
        'small': ['small', 'sm', 'sml'], 'extra': ['extra', 'xtra'],
        'black': ['black', 'blk'], 'blue': ['blue', 'blu'],
        'white': ['white', 'wht'], 'powder': ['powder', 'pwdr', 'pwdrfree'],
        'nitrile': ['nitrile', 'nitrl'], 'gloves': ['gloves', 'glove', 'glov'],
        'exam': ['exam', 'examination'], 'wheelchair': ['wheelchair', 'whlchr'],
        'diabetic': ['diabetic', 'diab'],
    }
    words = [w for w in re.findall(r'[a-zA-Z0-9]{2,}', (text or "").lower()) if w not in stop]
    expanded = []
    for w in words:
        found = False
        for key, variants in expansions.items():
            if w in variants or w == key:
                expanded.append(variants)
                found = True
                break
        if not found:
            expanded.append([w])
    return expanded


# ── Item Memory ─────────────────────────────────────────────────

def _parse_float(val):
    """Safely parse a price/cost value to float."""
    try:
        return float(str(val or 0).replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        return 0


def _learn_item(db, item, source="backfill"):
    """Learn ALL fields from a single item dict into item_mappings + supplier_costs."""
    desc = item.get("description", item.get("desc", ""))
    if not desc or len(desc) < 3:
        return False

    item_num = item.get("item_number", item.get("part_number", item.get("mfg_number", "")))
    supplier = item.get("item_supplier", item.get("supplier", ""))
    cost_val = _parse_float(item.get("supplier_cost") or item.get("cost") or item.get("unit_cost"))
    sell_val = _parse_float(item.get("unit_price") or item.get("price") or item.get("bid_price") or
                            item.get("sell_price"))
    url = item.get("item_link", item.get("supplier_url", item.get("product_url", "")))
    uom = item.get("uom", item.get("unit_of_measure", ""))
    mfg = item.get("mfg_number", item.get("part_number", ""))
    asin = item.get("asin", "")

    if cost_val <= 0 and sell_val <= 0 and not url:
        return False

    try:
        db.execute("""
            INSERT INTO item_mappings
            (original_description, original_item_number, canonical_description,
             canonical_item_number, mfg_number, asin, product_url, supplier,
             last_cost, last_sell_price, uom, supplier_url,
             confidence, confirmed, times_confirmed, last_confirmed)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0.8,1,1,datetime('now'))
            ON CONFLICT(original_description, original_item_number) DO UPDATE SET
                last_cost = CASE WHEN excluded.last_cost > 0 THEN excluded.last_cost ELSE item_mappings.last_cost END,
                last_sell_price = CASE WHEN excluded.last_sell_price > 0 THEN excluded.last_sell_price ELSE item_mappings.last_sell_price END,
                supplier = COALESCE(NULLIF(excluded.supplier,''), item_mappings.supplier),
                mfg_number = COALESCE(NULLIF(excluded.mfg_number,''), item_mappings.mfg_number),
                asin = COALESCE(NULLIF(excluded.asin,''), item_mappings.asin),
                product_url = COALESCE(NULLIF(excluded.product_url,''), item_mappings.product_url),
                supplier_url = COALESCE(NULLIF(excluded.supplier_url,''), item_mappings.supplier_url),
                uom = COALESCE(NULLIF(excluded.uom,''), item_mappings.uom),
                times_confirmed = item_mappings.times_confirmed + 1,
                last_confirmed = datetime('now'),
                confidence = MIN(0.95, item_mappings.confidence + 0.05)
        """, (desc[:500], item_num, desc[:500], item_num, mfg, asin, url,
              supplier, cost_val, sell_val, uom, url))
    except Exception:
        return False

    # Also lock cost
    if cost_val > 0:
        try:
            expires = (datetime.now() + timedelta(days=60)).isoformat()
            db.execute("""
                INSERT INTO supplier_costs
                (description, item_number, cost, supplier, source, source_url, confirmed_at, expires_at)
                VALUES (?,?,?,?,?,?,datetime('now'),?)
                ON CONFLICT(description, supplier) DO UPDATE SET
                    cost = excluded.cost, source_url = COALESCE(NULLIF(excluded.source_url,''), supplier_costs.source_url),
                    confirmed_at = datetime('now'), expires_at = excluded.expires_at
            """, (desc[:500], item_num, cost_val, supplier, source, url, expires))
        except Exception as _e:
            log.debug("suppressed: %s", _e)

    return True


def backfill_item_memory():
    """Learn ALL fields from ALL existing priced PCs and quotes."""
    import json
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=30)
    learned = 0

    # From price_checks
    try:
        rows = db.execute("""
            SELECT id, pc_data, items FROM price_checks
            WHERE status IN ('sent','submitted','won','completed','priced')
        """).fetchall()
        for row in rows:
            try:
                pc_data = json.loads(row[1] or "{}") if row[1] else {}
                items = pc_data.get("items", pc_data.get("line_items", []))
                if isinstance(items, str):
                    items = json.loads(items)
                if not items:
                    items = json.loads(row[2] or "[]") if row[2] else []
                for item in (items or []):
                    if _learn_item(db, item, source="backfill_pc"):
                        learned += 1
            except Exception as _e:
                log.debug("suppressed: %s", _e)
    except Exception as e:
        log.warning("Backfill PCs: %s", e)

    # From quotes
    try:
        rows = db.execute("""
            SELECT id, items_detail, line_items FROM quotes
            WHERE status IN ('sent','submitted','won','completed')
        """).fetchall()
        for row in rows:
            try:
                items = json.loads(row[1] or "[]") if row[1] else []
                if not items:
                    items = json.loads(row[2] or "[]") if row[2] else []
                if isinstance(items, str):
                    items = json.loads(items)
                for item in (items or []):
                    if _learn_item(db, item, source="backfill_quote"):
                        learned += 1
            except Exception as _e:
                log.debug("suppressed: %s", _e)
    except Exception as e:
        log.warning("Backfill quotes: %s", e)

    db.commit()
    db.close()
    log.info("Item memory backfill: %d items learned (all fields)", learned)
    return learned


def confirm_item_mapping(original_description, canonical_description,
                          item_number="", mfg_number="", product_url="",
                          supplier="", cost=None):
    """User confirms item match. Persists forever."""
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.execute("""
        INSERT INTO item_mappings
        (original_description, original_item_number, canonical_description,
         canonical_item_number, mfg_number, product_url, supplier,
         last_cost, confidence, confirmed, times_confirmed, last_confirmed)
        VALUES (?,?,?,?,?,?,?,?,0.95,1,1,datetime('now'))
        ON CONFLICT(original_description, original_item_number) DO UPDATE SET
            canonical_description=excluded.canonical_description,
            mfg_number=COALESCE(NULLIF(excluded.mfg_number,''), item_mappings.mfg_number),
            product_url=COALESCE(NULLIF(excluded.product_url,''), item_mappings.product_url),
            supplier=COALESCE(NULLIF(excluded.supplier,''), item_mappings.supplier),
            last_cost=CASE WHEN excluded.last_cost>0 THEN excluded.last_cost ELSE item_mappings.last_cost END,
            confirmed=1, confidence=0.95, times_confirmed=item_mappings.times_confirmed+1,
            last_confirmed=datetime('now')
    """, (original_description[:500], item_number, canonical_description[:500],
          item_number, mfg_number, product_url, supplier, cost or 0))
    db.commit()
    db.close()
    log.info("Item confirmed: '%s' -> '%s'", original_description[:40], canonical_description[:40])


def auto_learn_mapping(original_description, matched_description,
                        item_number="", confidence=0.5):
    """System auto-learns a mapping."""
    import sqlite3
    from src.core.db import DB_PATH
    try:
        db = sqlite3.connect(DB_PATH, timeout=10)
        db.execute("""
            INSERT OR IGNORE INTO item_mappings
            (original_description, original_item_number, canonical_description,
             canonical_item_number, confidence, confirmed, times_confirmed)
            VALUES (?,?,?,?,?,0,1)
        """, (original_description[:500], item_number, matched_description[:500],
              item_number, confidence))
        db.commit()
        db.close()
    except Exception as _e:
        log.debug("suppressed: %s", _e)


# ── Supplier Cost Lock ──────────────────────────────────────────

def lock_cost(description, cost, supplier="", source="manual",
              source_url="", expires_days=30, item_number=""):
    """Lock a supplier cost with auto-expiry."""
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    expires = (datetime.now() + timedelta(days=expires_days)).isoformat()
    db.execute("""
        INSERT INTO supplier_costs
        (description, item_number, cost, supplier, source, source_url, confirmed_at, expires_at)
        VALUES (?,?,?,?,?,?,datetime('now'),?)
        ON CONFLICT(description, supplier) DO UPDATE SET
            cost=excluded.cost, source=excluded.source, source_url=excluded.source_url,
            confirmed_at=datetime('now'), expires_at=excluded.expires_at
    """, (description[:500], item_number, cost, supplier, source, source_url, expires))
    db.commit()
    db.close()
    log.info("Cost locked: '%s' = $%.2f from %s", description[:40], cost, supplier)


def get_expiring_costs(days=7):
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    rows = db.execute("""
        SELECT description, cost, supplier, source, expires_at FROM supplier_costs
        WHERE expires_at BETWEEN datetime('now') AND datetime('now', ?) ORDER BY expires_at
    """, (f"+{days} days",)).fetchall()
    db.close()
    return [{"description": r[0], "cost": r[1], "supplier": r[2], "source": r[3], "expires": r[4]} for r in rows]


# ── V5 Phase 6: Auto-Requote Triggers ──────────────────────────

def check_requote_triggers():
    """Scan pending/sent PCs for requote triggers.

    Returns list of {pc_id, pc_number, trigger_type, details} dicts.
    Triggers:
      - cost_dropped: supplier has lower cost → better margin
      - cost_spiked: supplier has higher cost → margin eroded
      - quote_expiring: sent > 35 days ago with no award
    """
    import sqlite3
    from src.core.db import DB_PATH

    triggers = []
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.row_factory = sqlite3.Row

    # Get sent PCs awaiting decision
    try:
        rows = db.execute("""
            SELECT id, pc_data FROM price_checks
            WHERE status IN ('sent', 'submitted')
            AND (award_status IS NULL OR award_status = 'pending' OR award_status = '')
            ORDER BY sent_at DESC LIMIT 100
        """).fetchall()
    except Exception:
        rows = []

    for row in rows:
        pcid = row["id"]
        try:
            pc_data = json.loads(row["pc_data"] or "{}") if row["pc_data"] else {}
        except Exception:
            continue

        pc_number = pc_data.get("pc_number", pcid)
        institution = pc_data.get("institution", "")
        items = pc_data.get("items", [])
        sent_at = pc_data.get("sent_at") or pc_data.get("generated_at") or ""

        # Trigger: quote expiring (sent > 35 days ago)
        if sent_at:
            try:
                sent_dt = datetime.fromisoformat(sent_at.replace("Z", "+00:00").split("+")[0])
                age_days = (datetime.now() - sent_dt).days
                if age_days >= 35:
                    triggers.append({
                        "pc_id": pcid, "pc_number": pc_number,
                        "trigger_type": "quote_expiring",
                        "institution": institution,
                        "details": {
                            "days_old": age_days,
                            "sent_at": sent_at[:10],
                            "item_count": len(items),
                        },
                    })
            except Exception as _e:
                log.debug("suppressed: %s", _e)

        # Trigger: cost changes since quote was sent
        for it in items:
            desc = (it.get("description") or "")[:200]
            if not desc:
                continue
            quoted_cost = float(it.get("vendor_cost") or (it.get("pricing") or {}).get("unit_cost") or 0)
            if quoted_cost <= 0:
                continue
            try:
                sc = db.execute("""
                    SELECT cost, confirmed_at FROM supplier_costs
                    WHERE description = ? AND confirmed_at > ?
                    ORDER BY confirmed_at DESC LIMIT 1
                """, (desc[:500], sent_at[:19] if sent_at else "2000-01-01")).fetchone()
                if sc:
                    new_cost = sc["cost"]
                    delta_pct = ((new_cost - quoted_cost) / quoted_cost) * 100
                    if delta_pct <= -10:  # cost dropped 10%+
                        triggers.append({
                            "pc_id": pcid, "pc_number": pc_number,
                            "trigger_type": "cost_dropped",
                            "institution": institution,
                            "details": {
                                "item": desc[:80],
                                "old_cost": round(quoted_cost, 2),
                                "new_cost": round(new_cost, 2),
                                "delta_pct": round(delta_pct, 1),
                            },
                        })
                    elif delta_pct >= 10:  # cost spiked 10%+
                        triggers.append({
                            "pc_id": pcid, "pc_number": pc_number,
                            "trigger_type": "cost_spiked",
                            "institution": institution,
                            "details": {
                                "item": desc[:80],
                                "old_cost": round(quoted_cost, 2),
                                "new_cost": round(new_cost, 2),
                                "delta_pct": round(delta_pct, 1),
                            },
                        })
            except Exception as _e:
                log.debug("suppressed: %s", _e)

    db.close()
    log.info("Requote triggers: %d found", len(triggers))
    return triggers


# ── Quote Speed Clock ───────────────────────────────────────────

def record_speed_event(record_type, record_id, event):
    """Record timing event: received, opened, priced, generated, sent."""
    import sqlite3
    from src.core.db import DB_PATH
    table = "price_checks" if record_type == "pc" else "quotes"
    col = f"{event}_at" if event != "opened" else "first_opened_at"
    try:
        db = sqlite3.connect(DB_PATH, timeout=10)
        current = db.execute(f"SELECT [{col}] FROM [{table}] WHERE id=?", (record_id,)).fetchone()
        if current and not current[0]:
            now = datetime.now().isoformat()
            db.execute(f"UPDATE [{table}] SET [{col}]=? WHERE id=?", (now, record_id))
            if event in ("priced", "sent"):
                received = db.execute(f"SELECT received_at, created_at FROM [{table}] WHERE id=?",
                                      (record_id,)).fetchone()
                start = received[0] or received[1] if received else None
                if start:
                    try:
                        elapsed = int((datetime.now() - datetime.fromisoformat(start)).total_seconds() / 60)
                        db.execute(f"UPDATE [{table}] SET time_to_{event}_mins=? WHERE id=?", (elapsed, record_id))
                    except Exception as _e:
                        log.debug("suppressed: %s", _e)
            db.commit()
        db.close()
    except Exception as _e:
        log.debug("suppressed: %s", _e)


def get_speed_stats():
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    stats = {}
    for table, name in [("price_checks", "pcs"), ("quotes", "quotes")]:
        try:
            rows = db.execute(f"""
                SELECT time_to_price_mins, time_to_send_mins FROM [{table}]
                WHERE time_to_send_mins > 0 ORDER BY sent_at DESC LIMIT 50
            """).fetchall()
            if rows:
                pt = [r[0] for r in rows if r[0] and r[0] > 0]
                st = [r[1] for r in rows if r[1] and r[1] > 0]
                stats[name] = {
                    "avg_to_price": round(sum(pt) / len(pt)) if pt else None,
                    "avg_to_send": round(sum(st) / len(st)) if st else None,
                    "fastest": min(st) if st else None, "slowest": max(st) if st else None,
                    "sample": len(rows),
                }
            else:
                stats[name] = {"sample": 0}
        except Exception:
            stats[name] = {"sample": 0}
    db.close()
    return stats


# ── Price History for Tooltips (P2.1) ─────────────────────────

def get_price_history_for_item(description, item_number="", agency="", limit=5):
    """Get historical quoted prices for a similar item.
    Returns list of {price, agency, date, outcome, quote_number} dicts."""
    import sqlite3
    from src.core.db import DB_PATH
    results = []
    try:
        db = sqlite3.connect(DB_PATH, timeout=10)
        db.row_factory = sqlite3.Row

        # Build WHERE clause from tokenized description
        token_groups = _tokenize(description)[:3]
        if not token_groups:
            db.close()
            return results
        where_parts = []
        params = []
        for group in token_groups:
            if len(group) == 1:
                where_parts.append("LOWER(description) LIKE ?")
                params.append(f"%{group[0]}%")
            else:
                or_clause = " OR ".join(["LOWER(description) LIKE ?" for _ in group])
                where_parts.append(f"({or_clause})")
                params.extend([f"%{v}%" for v in group])
        where = " AND ".join(where_parts)

        # Search winning_prices (our won orders)
        try:
            rows = db.execute(f"""
                SELECT description, sell_price, cost, agency, institution,
                       recorded_at, quote_number, margin_pct
                FROM winning_prices WHERE {where}
                ORDER BY recorded_at DESC LIMIT ?
            """, params + [limit]).fetchall()
            for r in rows:
                results.append({
                    "price": r["sell_price"], "cost": r["cost"] or 0,
                    "agency": r["agency"] or "", "institution": r["institution"] or "",
                    "date": (r["recorded_at"] or "")[:10],
                    "outcome": "won", "quote_number": r["quote_number"] or "",
                    "margin": r["margin_pct"] or 0,
                })
        except Exception as _e:
            log.debug("suppressed: %s", _e)

        # Search competitor_intel (our losses)
        try:
            rows = db.execute(f"""
                SELECT item_summary, our_price, competitor_price, agency, institution,
                       found_at, quote_number, competitor_name
                FROM competitor_intel WHERE {where.replace('description', 'item_summary')}
                ORDER BY found_at DESC LIMIT ?
            """, params + [limit]).fetchall()
            for r in rows:
                results.append({
                    "price": r["our_price"] or 0,
                    "competitor_price": r["competitor_price"] or 0,
                    "agency": r["agency"] or "", "institution": r["institution"] or "",
                    "date": (r["found_at"] or "")[:10],
                    "outcome": "lost", "quote_number": r["quote_number"] or "",
                    "competitor": r["competitor_name"] or "",
                })
        except Exception as _e:
            log.debug("suppressed: %s", _e)

        db.close()
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # Sort by date descending
    results.sort(key=lambda x: x.get("date", ""), reverse=True)
    return results[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
# ORACLE V3 — Self-Calibrating Feedback Loop
# ═══════════════════════════════════════════════════════════════════════════════

_CAL_ALPHA = 0.15  # EMA learning rate — higher = more responsive to recent data
_CAL_MIN_SAMPLES = 5  # Minimum quotes before calibration kicks in
_CAL_MARKUP_FLOOR = 15.0
_CAL_MARKUP_CEIL = 50.0


def _init_calibration_table(db):
    """Create oracle_calibration table if it doesn't exist."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS oracle_calibration (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            agency TEXT DEFAULT '',
            sample_size INTEGER DEFAULT 0,
            win_count INTEGER DEFAULT 0,
            loss_on_price INTEGER DEFAULT 0,
            loss_on_other INTEGER DEFAULT 0,
            avg_winning_margin REAL DEFAULT 25,
            avg_losing_delta REAL DEFAULT 0,
            recommended_max_markup REAL DEFAULT 30,
            competitor_floor REAL DEFAULT 0,
            last_updated TEXT,
            UNIQUE(category, agency)
        )
    """)
    db.commit()


def _get_calibration(db, category, agency=""):
    """Read calibration data for a category/agency combo."""
    try:
        _init_calibration_table(db)
        row = db.execute(
            "SELECT * FROM oracle_calibration WHERE category=? AND agency=?",
            (category, agency)
        ).fetchone()
        if not row:
            # Try category-only (any agency)
            row = db.execute(
                "SELECT * FROM oracle_calibration WHERE category=? AND agency=''",
                (category,)
            ).fetchone()
        if row:
            cols = [d[0] for d in db.execute(
                "SELECT * FROM oracle_calibration LIMIT 0").description]
            d = dict(zip(cols, row))
            d["win_rate"] = d["win_count"] / d["sample_size"] if d["sample_size"] > 0 else 0
            return d
    except Exception as e:
        log.debug("Calibration read error: %s", e)
    return None


def _get_institution_profile(db, institution, category="general"):
    """V5 Phase 2: Read institution pricing profile for buyer-specific pricing."""
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS institution_pricing_profile (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                institution TEXT NOT NULL, category TEXT DEFAULT 'general',
                avg_winning_markup REAL DEFAULT 25, avg_losing_markup REAL DEFAULT 0,
                win_count INTEGER DEFAULT 0, loss_count INTEGER DEFAULT 0,
                price_sensitivity TEXT DEFAULT 'normal', preferred_suppliers TEXT DEFAULT '',
                last_updated TEXT, UNIQUE(institution, category)
            )
        """)
        row = db.execute(
            "SELECT * FROM institution_pricing_profile WHERE institution=? AND category=?",
            (institution, category)
        ).fetchone()
        if not row:
            row = db.execute(
                "SELECT * FROM institution_pricing_profile WHERE institution=? AND category='general'",
                (institution,)
            ).fetchone()
        if row:
            cols = [d[0] for d in db.execute(
                "SELECT * FROM institution_pricing_profile LIMIT 0").description]
            d = dict(zip(cols, row))
            d["total_quotes"] = d["win_count"] + d["loss_count"]
            d["win_rate"] = d["win_count"] / d["total_quotes"] if d["total_quotes"] > 0 else 0
            return d
    except Exception as e:
        log.debug("Institution profile read error: %s", e)
    return None


def _classify_item_category(description):
    """Lightweight category detection from description text."""
    desc = (description or "").lower()
    cats = {
        "medical": ["glove", "bandage", "gauze", "syringe", "catheter", "wound", "medical",
                     "nitrile", "exam", "surgical", "sterile", "antiseptic", "thermometer"],
        "office": ["paper", "pen", "pencil", "folder", "binder", "staple", "tape", "marker",
                   "highlighter", "clipboard", "envelope", "notebook", "post-it", "toner", "ink"],
        "janitorial": ["trash", "garbage", "mop", "broom", "cleaner", "wipe", "towel",
                       "soap", "sanitizer", "disinfectant", "bleach", "deodorizer"],
        "food": ["food", "snack", "beverage", "coffee", "sugar", "creamer", "cup", "plate",
                 "napkin", "utensil", "fork", "spoon"],
        "safety": ["vest", "helmet", "goggles", "earplugs", "safety", "fire", "extinguisher",
                   "first aid", "ppe", "respirator"],
        "technology": ["computer", "laptop", "monitor", "keyboard", "mouse", "printer",
                       "cable", "usb", "hdmi", "battery", "charger", "adapter"],
        "arts_crafts": ["paint", "brush", "canvas", "crayon", "marker", "coloring",
                        "poster", "art", "craft", "sticker", "glue", "scissors"],
    }
    best_cat = "general"
    best_score = 0
    for cat, keywords in cats.items():
        score = sum(1 for kw in keywords if kw in desc)
        if score > best_score:
            best_score = score
            best_cat = cat
    return best_cat


def calibrate_from_outcome(items, outcome, agency="", loss_reason=None, winner_prices=None):
    """Update oracle_calibration table from a win/loss outcome.

    Args:
        items: list of item dicts from the quote (with pricing, cost, etc.)
        outcome: "won" or "lost"
        agency: agency code (CCHCS, CDCR, etc.)
        loss_reason: "price" or "other" (only for losses)
        winner_prices: dict of {idx: competitor_price} if available
    """
    import sqlite3
    from src.core.db import DB_PATH

    try:
        db = sqlite3.connect(DB_PATH, timeout=10)
        _init_calibration_table(db)

        # Group items by category
        category_items = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            desc = item.get("description", "")
            cat = _classify_item_category(desc)
            category_items.setdefault(cat, []).append(item)

        now = datetime.now().isoformat()

        for cat, cat_items in category_items.items():
            # Get current calibration row (or create)
            row = db.execute(
                "SELECT * FROM oracle_calibration WHERE category=? AND agency=?",
                (cat, agency)
            ).fetchone()

            if row:
                cols = [d[0] for d in db.execute(
                    "SELECT * FROM oracle_calibration LIMIT 0").description]
                cal = dict(zip(cols, row))
            else:
                cal = {"category": cat, "agency": agency, "sample_size": 0,
                       "win_count": 0, "loss_on_price": 0, "loss_on_other": 0,
                       "avg_winning_margin": 25.0, "avg_losing_delta": 0.0,
                       "recommended_max_markup": 30.0, "competitor_floor": 0.0}

            cal["sample_size"] += 1

            if outcome == "won":
                cal["win_count"] += 1
                # Calculate average winning margin from these items
                margins = []
                for it in cat_items:
                    p = it.get("pricing") or {}
                    cost = float(
                        it.get("vendor_cost") or it.get("supplier_cost") or it.get("catalog_cost")
                        or p.get("unit_cost") or p.get("cost") or 0
                    )
                    price = float(
                        it.get("unit_price") or it.get("bid_price")
                        or p.get("final_price") or p.get("bid_price") or p.get("unit_price") or 0
                    )
                    if cost > 0 and price > cost:
                        margins.append(((price - cost) / cost) * 100)
                if margins:
                    avg_m = sum(margins) / len(margins)
                    cal["avg_winning_margin"] = (
                        _CAL_ALPHA * avg_m + (1 - _CAL_ALPHA) * cal["avg_winning_margin"]
                    )

            elif outcome == "lost":
                if loss_reason == "price":
                    cal["loss_on_price"] += 1
                    # If we know competitor prices, calculate how much we were above
                    if winner_prices:
                        deltas = []
                        for it in cat_items:
                            p = it.get("pricing") or {}
                            our_price = float(
                                it.get("unit_price") or it.get("bid_price")
                                or p.get("final_price") or p.get("bid_price") or p.get("unit_price") or 0
                            )
                            idx = items.index(it) if it in items else -1
                            comp_price = winner_prices.get(idx, 0) if winner_prices else 0
                            if our_price > 0 and comp_price > 0:
                                deltas.append(((our_price - comp_price) / comp_price) * 100)
                        if deltas:
                            avg_delta = sum(deltas) / len(deltas)
                            cal["avg_losing_delta"] = (
                                _CAL_ALPHA * avg_delta + (1 - _CAL_ALPHA) * cal["avg_losing_delta"]
                            )
                else:
                    cal["loss_on_other"] += 1

            # Recalculate recommended max markup
            win_rate = cal["win_count"] / cal["sample_size"] if cal["sample_size"] > 0 else 0
            adjustment = (win_rate - 0.5) * 20  # -10% to +10%
            cal["recommended_max_markup"] = max(
                _CAL_MARKUP_FLOOR,
                min(_CAL_MARKUP_CEIL, cal["avg_winning_margin"] + adjustment)
            )
            cal["last_updated"] = now

            # Upsert
            db.execute("""
                INSERT INTO oracle_calibration
                    (category, agency, sample_size, win_count, loss_on_price, loss_on_other,
                     avg_winning_margin, avg_losing_delta, recommended_max_markup, competitor_floor,
                     last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(category, agency) DO UPDATE SET
                    sample_size=excluded.sample_size, win_count=excluded.win_count,
                    loss_on_price=excluded.loss_on_price, loss_on_other=excluded.loss_on_other,
                    avg_winning_margin=excluded.avg_winning_margin,
                    avg_losing_delta=excluded.avg_losing_delta,
                    recommended_max_markup=excluded.recommended_max_markup,
                    competitor_floor=excluded.competitor_floor,
                    last_updated=excluded.last_updated
            """, (cat, agency, cal["sample_size"], cal["win_count"],
                  cal["loss_on_price"], cal["loss_on_other"],
                  round(cal["avg_winning_margin"], 2), round(cal["avg_losing_delta"], 2),
                  round(cal["recommended_max_markup"], 2), cal["competitor_floor"], now))

        # ── V5 Phase 2: Institution pricing profile ──
        if agency:
            for cat, cat_items in category_items.items():
                try:
                    row = db.execute(
                        "SELECT * FROM institution_pricing_profile WHERE institution=? AND category=?",
                        (agency, cat)
                    ).fetchone()
                    if row:
                        cols = [d[0] for d in db.execute(
                            "SELECT * FROM institution_pricing_profile LIMIT 0").description]
                        ip = dict(zip(cols, row))
                    else:
                        ip = {"institution": agency, "category": cat,
                              "avg_winning_markup": 25.0, "avg_losing_markup": 0.0,
                              "win_count": 0, "loss_count": 0,
                              "price_sensitivity": "normal", "preferred_suppliers": ""}

                    if outcome == "won":
                        ip["win_count"] += 1
                        margins = []
                        for it in cat_items:
                            p = it.get("pricing") or {}
                            cost = float(it.get("vendor_cost") or p.get("unit_cost") or 0)
                            price = float(it.get("unit_price") or p.get("final_price") or p.get("bid_price") or 0)
                            if cost > 0 and price > cost:
                                margins.append(((price - cost) / cost) * 100)
                        if margins:
                            avg_m = sum(margins) / len(margins)
                            ip["avg_winning_markup"] = (
                                _CAL_ALPHA * avg_m + (1 - _CAL_ALPHA) * ip["avg_winning_markup"]
                            )
                    elif outcome == "lost":
                        ip["loss_count"] += 1
                        margins = []
                        for it in cat_items:
                            p = it.get("pricing") or {}
                            cost = float(it.get("vendor_cost") or p.get("unit_cost") or 0)
                            price = float(it.get("unit_price") or p.get("final_price") or p.get("bid_price") or 0)
                            if cost > 0 and price > cost:
                                margins.append(((price - cost) / cost) * 100)
                        if margins:
                            avg_m = sum(margins) / len(margins)
                            ip["avg_losing_markup"] = (
                                _CAL_ALPHA * avg_m + (1 - _CAL_ALPHA) * ip["avg_losing_markup"]
                            )

                    # Derive price sensitivity from win rate
                    total = ip["win_count"] + ip["loss_count"]
                    if total >= 3:
                        wr = ip["win_count"] / total
                        ip["price_sensitivity"] = "tight" if wr < 0.4 else ("loose" if wr > 0.6 else "normal")

                    db.execute("""
                        INSERT INTO institution_pricing_profile
                            (institution, category, avg_winning_markup, avg_losing_markup,
                             win_count, loss_count, price_sensitivity, preferred_suppliers, last_updated)
                        VALUES (?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(institution, category) DO UPDATE SET
                            avg_winning_markup=excluded.avg_winning_markup,
                            avg_losing_markup=excluded.avg_losing_markup,
                            win_count=excluded.win_count, loss_count=excluded.loss_count,
                            price_sensitivity=excluded.price_sensitivity,
                            last_updated=excluded.last_updated
                    """, (agency, cat, round(ip["avg_winning_markup"], 2),
                          round(ip["avg_losing_markup"], 2),
                          ip["win_count"], ip["loss_count"],
                          ip["price_sensitivity"], ip.get("preferred_suppliers", ""), now))
                except Exception as e:
                    log.debug("Institution profile update error: %s", e)

        # ── V5 Phase 4: Record quote shape ──
        try:
            all_markups = []
            cat_counts = {}
            for it in items:
                if not isinstance(it, dict):
                    continue
                p = it.get("pricing") or {}
                cost = float(it.get("vendor_cost") or p.get("unit_cost") or 0)
                price = float(it.get("unit_price") or p.get("final_price") or p.get("bid_price") or 0)
                if cost > 0 and price > cost:
                    markup = ((price - cost) / cost) * 100
                    all_markups.append(round(markup, 1))
                cat = _classify_item_category(it.get("description", ""))
                cat_counts[cat] = cat_counts.get(cat, 0) + 1
            if all_markups:
                avg_mu = sum(all_markups) / len(all_markups)
                stddev = (sum((m - avg_mu) ** 2 for m in all_markups) / len(all_markups)) ** 0.5
                # Bucket markups into 5% bands
                buckets = {}
                for m in all_markups:
                    band = int(m / 5) * 5
                    buckets[band] = buckets.get(band, 0) + 1
                dist = [{"pct": k, "count": v} for k, v in sorted(buckets.items())]
                # IN-10: write `agency` to both the legacy `institution` slot
                # (back-compat for consumers that predate the column split)
                # and the new dedicated `agency` column. Older rows are
                # back-filled by the `_migrate_columns` UPDATE so `agency`
                # is the canonical readable field going forward.
                db.execute("""
                    INSERT INTO winning_quote_shapes
                        (institution, agency, category_mix, total_items, avg_markup,
                         markup_stddev, markup_distribution, outcome, recorded_at)
                    VALUES (?,?,?,?,?,?,?,?,?)
                """, (agency, agency, json.dumps(cat_counts), len(items),
                      round(avg_mu, 2), round(stddev, 2),
                      json.dumps(dist), outcome, now))
        except Exception as e:
            log.debug("Quote shape recording error: %s", e)

        db.commit()
        log.info("Oracle V5 calibration updated: %s outcome=%s agency=%s categories=%s",
                 len(items), outcome, agency, list(category_items.keys()))
    except Exception as e:
        log.error("Oracle V5 calibration error: %s", e, exc_info=True)
    finally:
        try:
            db.close()
        except Exception as _e:
            log.debug("suppressed: %s", _e)


def calibrate_order_shipped_once(order_id: str, items: list, agency: str = "",
                                 order_total: float = 0.0) -> bool:
    """Fire calibrate_from_outcome('won') for a shipped/delivered/invoiced order,
    idempotently via `backfill_wins_ledger`.

    Prod 2026-04-20 had 0 wins / 47 losses because the only runtime win-path
    (PC/RFQ mark-won buttons) hadn't been used — Mike is mid-transition from
    Google Drive. Real shipped POs sat in `orders` invisible to Oracle. This
    helper is the runtime counterpart to `scripts/backfill_wins_from_orders.py`:
    order-status transitions into win-class now feed Oracle the same way the
    backfill does, gated on the same ledger so no double-count occurs.

    Args:
        order_id: `orders.id` — ledger primary key, dedupes re-entries.
        items: parsed line-items list from the order.
        agency: agency/institution label for calibration bucket.
        order_total: recorded on the ledger row for audit; no math done on it.

    Returns:
        True  — fired calibrate_from_outcome and wrote the ledger row.
        False — ledger already had this order_id; silently skipped.
    """
    import sqlite3
    from src.core.db import DB_PATH

    if not order_id:
        return False
    if not items:
        # Can't calibrate without items (single real prod win has items,
        # but 3/4 prod orders carry items=[] as artifacts). Skip quietly —
        # same contract as the backfill script.
        return False

    try:
        db = sqlite3.connect(DB_PATH, timeout=10)
        db.execute("""
            CREATE TABLE IF NOT EXISTS backfill_wins_ledger (
                order_id TEXT PRIMARY KEY,
                processed_at TEXT NOT NULL,
                win_total REAL,
                agency TEXT,
                items_count INTEGER
            )
        """)
        existing = db.execute(
            "SELECT 1 FROM backfill_wins_ledger WHERE order_id=?",
            (order_id,)
        ).fetchone()
        if existing:
            db.close()
            log.debug("Oracle win-calibration skipped (already in ledger): %s", order_id)
            return False

        db.execute("""
            INSERT INTO backfill_wins_ledger
                (order_id, processed_at, win_total, agency, items_count)
            VALUES (?, datetime('now'), ?, ?, ?)
        """, (order_id, float(order_total or 0), agency or "", len(items)))
        db.commit()
        db.close()
    except Exception as e:
        log.warning("calibrate_order_shipped_once ledger error (%s): %s", order_id, e)
        return False

    try:
        calibrate_from_outcome(items, "won", agency=agency)
        log.info("Oracle win-calibration fired from order ship: %s (agency=%s, items=%d)",
                 order_id, agency, len(items))
        return True
    except Exception as e:
        log.error("calibrate_order_shipped_once calibrate error (%s): %s", order_id, e)
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# ORACLE V5.5 — Per-Buyer Win-Rate Curves
# ═══════════════════════════════════════════════════════════════════════════════
#
# V5 Phase 2 stores a scalar `avg_winning_markup` per institution. That's a
# single number summarizing the *average* markup on winning quotes, but it
# can't answer the question that actually matters at quoting time:
#
#     "If I quote this buyer at 32% markup, what's my win probability?"
#
# V5.5 replaces the scalar with a bucketed win-rate curve fitted from every
# (markup_pct, outcome) pair in the `quotes` table, then finds the markup that
# maximizes expected profit = markup_pct × P(win | markup). This is a proper
# response-curve, not a scalar target.
#
# The curve is rebuilt on-demand (cached 10 min per institution) and falls
# back to the scalar V5 profile when sample size is below _CURVE_MIN_SAMPLES.
# Nothing breaks if this module is stripped — it's additive over V5.

_CURVE_MIN_SAMPLES = 8         # need ≥ this many won+lost quotes to trust the curve
_CURVE_BUCKETS = (15, 20, 25, 30, 35, 40, 45, 50, 60)  # left edges, in %
_CURVE_MARKUP_MIN = 15.0       # optimizer search floor
_CURVE_MARKUP_MAX = 60.0       # optimizer search ceiling
_CURVE_STEP = 1.0              # 1% resolution when searching for optimum
_CURVE_CACHE_TTL = 600         # 10 minutes

_curve_cache: Dict[str, Any] = {}  # type: ignore[name-defined]
_curve_cache_lock = None


def _curve_cache_init():
    global _curve_cache_lock
    if _curve_cache_lock is None:
        import threading
        _curve_cache_lock = threading.RLock()


def _curve_cache_get(key: str):
    _curve_cache_init()
    with _curve_cache_lock:
        entry = _curve_cache.get(key)
        if entry is None:
            return None
        if time.time() > entry["expires_at"]:
            _curve_cache.pop(key, None)
            return None
        return entry["value"]


def _curve_cache_put(key: str, value):
    _curve_cache_init()
    with _curve_cache_lock:
        _curve_cache[key] = {
            "value": value,
            "expires_at": time.time() + _CURVE_CACHE_TTL,
        }


def _curve_cache_clear():
    """Test helper — not exposed publicly."""
    _curve_cache_init()
    with _curve_cache_lock:
        _curve_cache.clear()


def _read_markup_outcomes(db, institution: str, category: str = None,
                          days: int = 365):
    """Return a list of (markup_pct, won_bool) from the quotes table for
    the given institution within the lookback window.

    The scalar V5 profile only tracks `avg_winning_markup`; we re-read
    from the raw quotes so we don't lose the shape of the distribution.
    `category` is accepted for forward compat but not yet used — the
    quotes table doesn't store a category column, so we'd have to
    reconstruct it from `items_detail` JSON. V5.5 keeps it simple and
    treats every institution as one curve.
    """
    try:
        since = (datetime.now() - timedelta(days=days)).isoformat()
        rows = db.execute(
            """SELECT margin_pct, status FROM quotes
                WHERE created_at >= ?
                  AND is_test = 0
                  AND (institution = ? OR agency = ?)
                  AND margin_pct IS NOT NULL
                  AND margin_pct > 0
                  AND status IN ('won', 'lost')""",
            (since, institution, institution),
        ).fetchall()
    except Exception as e:
        log.debug("V5.5 _read_markup_outcomes error: %s", e)
        return []

    out = []
    for row in rows:
        try:
            markup = float(row[0] if not isinstance(row, dict) else row["margin_pct"])
            status = row[1] if not isinstance(row, dict) else row["status"]
        except Exception as _e:
            log.debug("suppressed: %s", _e)
            continue
        if markup <= 0:
            continue
        out.append((markup, status == "won"))
    return out


def _bucket_markup(markup_pct: float) -> int:
    """Return the left edge of the bucket this markup falls into.
    Buckets are [_CURVE_BUCKETS[i], _CURVE_BUCKETS[i+1]); values below
    the first edge fall into the first bucket, values above the last
    edge fall into the last bucket."""
    if markup_pct < _CURVE_BUCKETS[0]:
        return _CURVE_BUCKETS[0]
    for i in range(len(_CURVE_BUCKETS) - 1):
        if _CURVE_BUCKETS[i] <= markup_pct < _CURVE_BUCKETS[i + 1]:
            return _CURVE_BUCKETS[i]
    return _CURVE_BUCKETS[-1]


def _fit_buyer_curve(db, institution: str, days: int = 365):
    """Build a bucketed win-rate curve for a single institution.

    Returns a dict:
        {
          "institution": "cchcs",
          "total_samples": 27,
          "won": 14,
          "lost": 13,
          "days": 365,
          "buckets": [
              {"markup_min": 15, "markup_max": 20, "samples": 3,
               "wins": 0, "win_rate": 0.0},
              ...
          ],
          "sufficient": True,   # >= _CURVE_MIN_SAMPLES
          "global_win_rate": 0.518,  # fallback when a bucket is empty
        }

    Never raises — returns a "sufficient: False" shell when the data
    is too thin to trust, so callers can fall through to scalar V5.
    """
    if not institution:
        return None
    cache_key = f"{institution.lower()}::{days}"
    cached = _curve_cache_get(cache_key)
    if cached is not None:
        return cached

    pairs = _read_markup_outcomes(db, institution, days=days)
    total = len(pairs)
    won = sum(1 for _m, w in pairs if w)
    lost = total - won

    curve = {
        "institution": institution,
        "total_samples": total,
        "won": won,
        "lost": lost,
        "days": days,
        "buckets": [],
        "sufficient": total >= _CURVE_MIN_SAMPLES,
        "global_win_rate": (won / total) if total > 0 else 0.0,
    }

    # Build bucket histogram
    counts: Dict[int, Dict[str, int]] = {b: {"wins": 0, "total": 0}
                                          for b in _CURVE_BUCKETS}
    for markup, is_win in pairs:
        b = _bucket_markup(markup)
        counts[b]["total"] += 1
        if is_win:
            counts[b]["wins"] += 1

    edges = list(_CURVE_BUCKETS) + [_CURVE_MARKUP_MAX + 1.0]
    for i, b in enumerate(_CURVE_BUCKETS):
        c = counts[b]
        win_rate = (c["wins"] / c["total"]) if c["total"] > 0 else None
        curve["buckets"].append({
            "markup_min": b,
            "markup_max": edges[i + 1],
            "samples": c["total"],
            "wins": c["wins"],
            "win_rate": round(win_rate, 4) if win_rate is not None else None,
        })

    _curve_cache_put(cache_key, curve)
    return curve


def buyer_win_probability(institution: str, markup_pct: float,
                           db=None, _curve=None) -> float:
    """P(win | institution, markup_pct) derived from the fitted curve.

    Interpolates linearly between adjacent buckets when both have data;
    falls back to the global institution win rate when the target bucket
    is empty; falls back to a 50/50 prior when there's no data at all.

    `_curve` is accepted so the recommender can pass a pre-fetched curve
    without re-hitting the DB.
    """
    curve = _curve
    if curve is None and db is not None:
        curve = _fit_buyer_curve(db, institution)
    if curve is None or curve.get("total_samples", 0) == 0:
        # IN-19: flat 0.5 prior makes EV = markup * 0.5 a strictly increasing
        # function of markup, so the optimizer always rides to markup_max for
        # unknown buyers — the opposite of Reytech's competitive posture.
        # Use a monotone decreasing prior so EV has an interior optimum:
        # P(win) = 0.85 at 0% markup, decaying linearly to 0.30 at max.
        # With that prior, EV = markup * P(win) peaks in the middle of the
        # range, which is a sane default for cold buyers.
        m = max(0.0, min(float(markup_pct), float(_CURVE_MARKUP_MAX)))
        return 0.85 - (0.55 * m / float(_CURVE_MARKUP_MAX))

    buckets = curve.get("buckets") or []
    if not buckets:
        return curve.get("global_win_rate", 0.5)

    target = _bucket_markup(float(markup_pct))
    # Find the target bucket
    target_idx = None
    for i, b in enumerate(buckets):
        if b["markup_min"] == target:
            target_idx = i
            break
    if target_idx is None:
        return curve.get("global_win_rate", 0.5)

    target_bucket = buckets[target_idx]
    if target_bucket.get("win_rate") is not None and target_bucket["samples"] >= 2:
        return float(target_bucket["win_rate"])

    # Target bucket empty or thin — interpolate linearly between the
    # nearest populated bucket on the LEFT and on the RIGHT. This
    # produces a smooth response curve instead of the step-function
    # you get from snapping to the nearest neighbor, which matters a
    # lot when a buyer has a clear peak at low markup and a flat tail
    # at high markup — snap-to-nearest picks the wrong side.
    def _find_neighbor(start_idx, step_dir):
        i = start_idx + step_dir
        while 0 <= i < len(buckets):
            b = buckets[i]
            if b.get("win_rate") is not None and b["samples"] >= 2:
                return i, float(b["win_rate"])
            i += step_dir
        return None, None

    left_idx, left_wr = _find_neighbor(target_idx, -1)
    right_idx, right_wr = _find_neighbor(target_idx, 1)
    global_wr = curve.get("global_win_rate", 0.5)

    if left_wr is not None and right_wr is not None:
        # Linear interpolation by index distance
        left_dist = target_idx - left_idx
        right_dist = right_idx - target_idx
        total = left_dist + right_dist
        return (left_wr * right_dist + right_wr * left_dist) / total
    if left_wr is not None:
        dist = target_idx - left_idx
        shrink = min(1.0, 0.2 * dist)
        return left_wr * (1 - shrink) + global_wr * shrink
    if right_wr is not None:
        dist = right_idx - target_idx
        shrink = min(1.0, 0.2 * dist)
        return right_wr * (1 - shrink) + global_wr * shrink
    return global_wr


def optimal_markup_for_expected_profit(institution: str, db,
                                        markup_min: float = _CURVE_MARKUP_MIN,
                                        markup_max: float = _CURVE_MARKUP_MAX,
                                        step: float = _CURVE_STEP) -> dict:
    """Search over [markup_min, markup_max] in `step` increments for the
    markup that maximizes markup_pct × P(win | markup), then return the
    optimum along with the curve's summary.

    Returns:
        {
          "markup_pct": 32.0,
          "expected_value": 17.6,   # markup_pct × P(win)
          "win_probability": 0.55,
          "curve": {...},           # same shape as _fit_buyer_curve()
          "sufficient": True,
        }

    Or `{"sufficient": False, "curve": ...}` when the curve has too few
    samples to trust — the caller should fall back to V5 scalar logic.
    """
    curve = _fit_buyer_curve(db, institution)
    if curve is None:
        return {"sufficient": False, "curve": None,
                "markup_pct": None, "win_probability": None,
                "expected_value": None}
    if not curve.get("sufficient"):
        return {"sufficient": False, "curve": curve,
                "markup_pct": None, "win_probability": None,
                "expected_value": None}

    best_markup = None
    best_ev = -1.0
    best_wp = 0.0
    m = markup_min
    # Use a small epsilon so the loop condition is stable in float
    while m <= markup_max + 1e-9:
        wp = buyer_win_probability(institution, m, _curve=curve)
        ev = m * wp
        if ev > best_ev:
            best_ev = ev
            best_markup = m
            best_wp = wp
        m += step

    return {
        "sufficient": True,
        "curve": curve,
        "markup_pct": round(best_markup, 1) if best_markup is not None else None,
        "win_probability": round(best_wp, 3),
        "expected_value": round(best_ev, 2),
    }


__all__ = __all__ if "__all__" in dir() else []
for _name in (
    "buyer_win_probability",
    "optimal_markup_for_expected_profit",
    "_fit_buyer_curve",
):
    if _name not in __all__:
        __all__.append(_name)
