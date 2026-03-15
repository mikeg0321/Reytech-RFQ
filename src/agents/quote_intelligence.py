"""
Quote Intelligence Engine
Matches RFQ line items against FI$Cal catalog data.
Provides pricing suggestions and competitor comparisons.
"""
import re
import logging
from difflib import SequenceMatcher

log = logging.getLogger("reytech.quote_intelligence")


def _parse_price_str(p):
    try:
        return float(str(p).replace("$", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _parse_qty_str(q):
    try:
        return float(str(q).replace(",", "").strip())
    except (ValueError, TypeError):
        return 1.0


def _extract_pack_info(description):
    """Extract packaging/UOM info from FI$Cal descriptions."""
    desc = description.upper()
    result = {
        "unit_count": 1, "pack_count": 1, "total_units": 1,
        "unit_type": "EA", "outer_type": "", "raw_match": "",
    }

    # Pattern 1: "100/BX 10 BXS/CS"
    m = re.search(
        r'(\d+)\s*/\s*(BX|BOX|PK|PACK|PAC|BT|BTL|RL|CS|CASE|CN|CAN|TB|TUBE)'
        r'[\s,]+(\d+)\s*(?:BXS?|PKS?|PCS?|PACKS?)?\s*/\s*'
        r'(CS|CASE|CTN|CARTON|PAC|PACK)', desc)
    if m:
        result["unit_count"] = int(m.group(1))
        result["unit_type"] = m.group(2)
        result["pack_count"] = int(m.group(3))
        result["outer_type"] = m.group(4)
        result["total_units"] = result["unit_count"] * result["pack_count"]
        result["raw_match"] = m.group(0)
        return result

    # Pattern 2: "CS/12" or "BX/100" (reversed)
    m = re.search(r'(CS|CASE|BX|BOX|PK|PACK|PAC)\s*/\s*(\d+)', desc)
    if m:
        result["unit_type"] = m.group(1)
        result["unit_count"] = int(m.group(2))
        result["total_units"] = result["unit_count"]
        result["raw_match"] = m.group(0)
        m2 = re.search(r'(\d+)\s*/\s*(BX|BOX|PK|PACK|PAC|RL|BTL)', desc)
        if m2 and m2.start() < m.start():
            inner = int(m2.group(1))
            result["pack_count"] = result["unit_count"]
            result["unit_count"] = inner
            result["total_units"] = inner * result["pack_count"]
        return result

    # Pattern 3: Simple "100/BX" or "50/CS"
    m = re.search(
        r'(\d+)\s*/\s*(BX|BOX|PK|PACK|PAC|CS|CASE|CTN|CN|RL|BTL|BT|TB|TUBE|EA|SET)', desc)
    if m:
        result["unit_count"] = int(m.group(1))
        result["unit_type"] = m.group(2)
        result["total_units"] = result["unit_count"]
        result["raw_match"] = m.group(0)
        return result

    # Pattern 4: "(50/CS)" with parens
    m = re.search(r'\((\d+)\s*/\s*(\w{2,4})\)', desc)
    if m:
        result["unit_count"] = int(m.group(1))
        result["unit_type"] = m.group(2)
        result["total_units"] = result["unit_count"]
        result["raw_match"] = m.group(0)
        return result

    # Pattern 5: Number before EA/EACH/PCS
    m = re.search(r'(\d+)\s*(EA|EACH|PCS|PIECES)', desc)
    if m:
        result["unit_count"] = int(m.group(1))
        result["total_units"] = result["unit_count"]
        result["raw_match"] = m.group(0)
        return result

    return result


def _normalize_unit_price(raw_price, description, quantity=1):
    """Normalize price to per-each unit price."""
    if not raw_price or raw_price <= 0:
        return None

    pack = _extract_pack_info(description)
    total = pack["total_units"]

    if total > 1:
        normalized = raw_price / total
        return {
            "raw_price": raw_price,
            "normalized_price": round(normalized, 4),
            "total_units": total,
            "pack_info": pack,
            "normalization": f"${raw_price:.2f} / {total} units ({pack['raw_match']})",
        }

    return {
        "raw_price": raw_price,
        "normalized_price": raw_price,
        "total_units": 1,
        "pack_info": pack,
        "normalization": f"${raw_price:.2f} / EA (no pack info detected)",
    }


def match_rfq_items(rfq_items):
    """Take RFQ line items, match against catalog, return enriched items."""
    results = []
    for item in rfq_items:
        desc = item.get("description", "")
        qty = item.get("quantity", 1)
        if not desc:
            continue
        matches = search_catalog(desc, limit=10)
        competitor_prices = get_competitor_prices(desc, limit=10)
        reytech_prices = get_reytech_prices(desc, limit=5)
        suggested = _calculate_suggested_price(
            reytech_prices=reytech_prices,
            competitor_prices=competitor_prices,
            quantity=qty,
        )
        results.append({
            "original_description": desc,
            "original_quantity": qty,
            "catalog_matches": matches,
            "reytech_history": reytech_prices,
            "competitor_prices": competitor_prices,
            "suggested_price": suggested.get("price"),
            "price_rationale": suggested.get("rationale"),
            "confidence": suggested.get("confidence"),
        })
    return results


def search_catalog(query, limit=10):
    """Fuzzy search the scprs_catalog for matching items."""
    try:
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=10)
        keywords = _tokenize(query)
        if not keywords:
            return []

        where_clauses = []
        params = []
        for kw in keywords[:5]:
            where_clauses.append("LOWER(description) LIKE ?")
            params.append(f"%{kw.lower()}%")

        sql = f"""
            SELECT description, unspsc, last_unit_price, last_quantity,
                   last_uom, last_supplier, last_department,
                   last_po_number, last_date, times_seen
            FROM scprs_catalog
            WHERE {' AND '.join(where_clauses)}
            ORDER BY times_seen DESC, last_date DESC
            LIMIT ?
        """
        params.append(limit)
        rows = db.execute(sql, params).fetchall()

        if not rows and len(keywords) > 1:
            where_any = " OR ".join(["LOWER(description) LIKE ?" for _ in keywords[:5]])
            params_any = [f"%{kw.lower()}%" for kw in keywords[:5]] + [limit * 2]
            rows = db.execute(f"""
                SELECT description, unspsc, last_unit_price, last_quantity,
                       last_uom, last_supplier, last_department,
                       last_po_number, last_date, times_seen
                FROM scprs_catalog WHERE {where_any}
                ORDER BY times_seen DESC, last_date DESC LIMIT ?
            """, params_any).fetchall()

        scored = []
        for row in rows:
            similarity = SequenceMatcher(None, query.lower(), row[0].lower()).ratio()
            keyword_hits = sum(1 for kw in keywords if kw.lower() in row[0].lower())
            score = (similarity * 0.6) + (keyword_hits / max(len(keywords), 1) * 0.4)
            norm = _normalize_unit_price(row[2], row[0], row[3])
            scored.append({
                "description": row[0], "unspsc": row[1],
                "last_unit_price": row[2],
                "normalized_unit_price": norm["normalized_price"] if norm else row[2],
                "normalization": norm["normalization"] if norm else "",
                "pack_info": norm["pack_info"] if norm else {},
                "last_quantity": row[3],
                "last_uom": row[4], "last_supplier": row[5],
                "last_department": row[6], "last_po_number": row[7],
                "last_date": row[8], "times_seen": row[9],
                "relevance_score": round(score, 3),
            })
        scored.sort(key=lambda x: x["relevance_score"], reverse=True)
        db.close()
        return scored[:limit]
    except Exception as e:
        log.error("Catalog search error: %s", e)
        return []


def get_competitor_prices(query, limit=10):
    """Get all suppliers' prices for items matching this query."""
    try:
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=10)
        keywords = _tokenize(query)
        if not keywords:
            return []

        where_clauses = []
        params = []
        for kw in keywords[:3]:
            where_clauses.append("LOWER(l.description) LIKE ?")
            params.append(f"%{kw.lower()}%")

        sql = f"""
            SELECT l.description, l.unit_price, l.quantity, l.uom,
                   m.supplier, m.dept_name, m.po_number, m.start_date,
                   m.buyer_name, m.buyer_email
            FROM scprs_po_lines l
            JOIN scprs_po_master m ON l.po_number = m.po_number
            WHERE {' AND '.join(where_clauses)}
            ORDER BY m.start_date DESC LIMIT ?
        """
        params.append(limit)
        rows = db.execute(sql, params).fetchall()
        db.close()
        results = []
        for r in rows:
            norm = _normalize_unit_price(_parse_price_str(r[1]), r[0], _parse_qty_str(r[2]))
            results.append({
                "description": r[0], "unit_price": r[1],
                "normalized_unit_price": norm["normalized_price"] if norm else None,
                "normalization": norm["normalization"] if norm else "",
                "quantity": r[2], "uom": r[3], "supplier": r[4],
                "department": r[5], "po_number": r[6], "date": r[7],
                "buyer_name": r[8], "buyer_email": r[9],
                "is_reytech": "REYTECH" in (r[4] or "").upper(),
            })
        return results
    except Exception as e:
        log.error("Competitor prices error: %s", e)
        return []


def get_reytech_prices(query, limit=5):
    """Get Reytech's historical prices for matching items."""
    try:
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=10)
        keywords = _tokenize(query)
        if not keywords:
            return []

        where_clauses = []
        params = []
        for kw in keywords[:3]:
            where_clauses.append("LOWER(l.description) LIKE ?")
            params.append(f"%{kw.lower()}%")

        sql = f"""
            SELECT l.description, l.unit_price, l.quantity, l.uom,
                   m.dept_name, m.po_number, m.start_date,
                   m.buyer_name, m.buyer_email
            FROM scprs_po_lines l
            JOIN scprs_po_master m ON l.po_number = m.po_number
            WHERE UPPER(m.supplier) LIKE '%REYTECH%'
            AND {' AND '.join(where_clauses)}
            ORDER BY m.start_date DESC LIMIT ?
        """
        params.append(limit)
        rows = db.execute(sql, params).fetchall()
        db.close()
        return [{
            "description": r[0], "unit_price": r[1], "quantity": r[2],
            "uom": r[3], "department": r[4], "po_number": r[5],
            "date": r[6], "buyer_name": r[7], "buyer_email": r[8],
        } for r in rows]
    except Exception as e:
        log.error("Reytech prices error: %s", e)
        return []


def _calculate_suggested_price(reytech_prices, competitor_prices, quantity):
    """Calculate optimal price with time decay, volume normalization, COVID adjustment."""
    import math
    from datetime import datetime, timedelta
    result = {"price": None, "rationale": "", "confidence": "low"}

    now = datetime.now()

    # COVID anomaly window for PPE/medical items
    COVID_START = datetime(2020, 3, 1)
    COVID_END = datetime(2021, 12, 31)
    COVID_CATEGORIES = ["glove", "mask", "gown", "sanitiz", "disinfect",
                        "ppe", "face shield", "respirator", "n95", "wipe"]

    def _parse_price(p):
        try:
            return float(str(p).replace("$", "").replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    def _parse_date(d):
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
            try:
                return datetime.strptime(str(d).strip(), fmt)
            except (ValueError, TypeError):
                continue
        return None

    def _parse_qty(q):
        try:
            return float(str(q).replace(",", "").strip())
        except (ValueError, TypeError):
            return 1.0

    def _time_weight(date_str):
        """6-month half-life exponential decay."""
        dt = _parse_date(date_str)
        if not dt:
            return 0.1
        age_days = max((now - dt).days, 0)
        return round(math.pow(0.5, age_days / 180), 4)

    def _covid_penalty(date_str, description=""):
        """Apply near-zero weight to COVID-inflated PPE prices."""
        dt = _parse_date(date_str)
        if not dt:
            return 1.0
        if COVID_START <= dt <= COVID_END:
            desc_lower = description.lower()
            if any(cat in desc_lower for cat in COVID_CATEGORIES):
                return 0.05
        return 1.0

    def _volume_weight(price_qty, request_qty):
        """Weight prices from similar quantities higher."""
        if not price_qty or price_qty <= 0:
            return 0.5
        if not request_qty or request_qty <= 0:
            return 0.5
        ratio = price_qty / request_qty
        if ratio > 1:
            ratio = 1 / ratio
        return max(0.2, math.pow(ratio, 0.3))

    def _score_prices(price_list, is_reytech=False):
        scored = []
        for p in price_list:
            raw_price = _parse_price(p.get("unit_price"))
            if not raw_price or raw_price <= 0:
                continue

            desc = p.get("description", "")
            date = p.get("date", "")
            qty = _parse_qty(p.get("quantity"))

            # Use normalized price if available
            norm = _normalize_unit_price(raw_price, desc, qty)
            price = norm["normalized_price"] if norm else raw_price

            time_w = _time_weight(date)
            covid_w = _covid_penalty(date, desc)
            vol_w = _volume_weight(qty, quantity)

            weight = time_w * covid_w * vol_w
            if is_reytech:
                weight *= 1.2

            scored.append({
                "price": price, "raw_price": raw_price,
                "weight": round(weight, 4),
                "date": date, "qty": qty,
                "normalization": norm["normalization"] if norm else "",
                "time_w": time_w, "covid_w": covid_w, "vol_w": vol_w,
            })
        return scored

    reytech_scored = _score_prices(reytech_prices, is_reytech=True)
    comp_scored = _score_prices(
        [p for p in competitor_prices if not p.get("is_reytech")],
        is_reytech=False
    )

    def _weighted_avg(scored_list):
        if not scored_list:
            return None
        total_weight = sum(s["weight"] for s in scored_list)
        if total_weight == 0:
            return None
        return sum(s["price"] * s["weight"] for s in scored_list) / total_weight

    reytech_wavg = _weighted_avg(reytech_scored)
    comp_wavg = _weighted_avg(comp_scored)

    comp_low = None
    if comp_scored:
        high_weight = [s for s in comp_scored if s["weight"] > 0.3]
        if high_weight:
            comp_low = min(s["price"] for s in high_weight)

    rationale_parts = []

    if reytech_wavg and comp_wavg:
        suggested = min(reytech_wavg, comp_wavg * 0.95)
        if comp_low:
            suggested = max(suggested, comp_low * 0.9)
        result["price"] = round(suggested, 2)
        result["confidence"] = "high"
        rationale_parts.append(f"Your weighted avg: ${reytech_wavg:.2f}")
        rationale_parts.append(f"Competitor weighted avg: ${comp_wavg:.2f}")
        if comp_low:
            rationale_parts.append(f"Competitor floor: ${comp_low:.2f}")
        rationale_parts.append(f"Suggested: ${suggested:.2f}")
        if reytech_scored:
            newest_r = max(reytech_scored, key=lambda x: x["time_w"])
            rationale_parts.append(f"Your most recent: ${newest_r['price']:.2f} ({newest_r['date']})")

    elif reytech_wavg:
        result["price"] = round(reytech_wavg, 2)
        result["confidence"] = "medium"
        rationale_parts.append(f"Based on your weighted history: ${reytech_wavg:.2f}")
        rationale_parts.append("No recent competitor data")

    elif comp_wavg:
        suggested = comp_wavg * 0.92
        result["price"] = round(suggested, 2)
        result["confidence"] = "medium"
        rationale_parts.append("No Reytech history")
        rationale_parts.append(f"Competitor weighted avg: ${comp_wavg:.2f}")
        rationale_parts.append(f"Suggested 8% undercut: ${suggested:.2f}")

    else:
        result["rationale"] = "No weighted pricing data. All available prices too old or mismatched. Manual research needed."
        return result

    # Flag stale data
    all_scored = reytech_scored + comp_scored
    if all_scored:
        max_weight = max(s["weight"] for s in all_scored)
        if max_weight < 0.3:
            rationale_parts.append("WARNING: All pricing data >6 months old. Verify before quoting.")
            result["confidence"] = "low"
        elif max_weight < 0.5:
            rationale_parts.append("Note: Best data is 3-6 months old")

    result["rationale"] = " | ".join(rationale_parts)
    return result


def _tokenize(text):
    """Extract meaningful keywords from a description."""
    stop_words = {'the', 'a', 'an', 'and', 'or', 'for', 'of', 'to', 'in', 'with',
                  'item', 'items', 'each', 'per', 'ea', 'cs', 'bx', 'pk', 'box',
                  'case', 'pack', 'qty', 'options', 'option'}
    words = re.findall(r'[a-zA-Z]{3,}', text.lower())
    return [w for w in words if w not in stop_words]


def enrich_quote_draft(rfq_data):
    """Take parsed RFQ data, return enriched quote draft with pricing."""
    items = rfq_data.get("line_items", rfq_data.get("items", []))
    enriched = match_rfq_items(items)

    total_estimated = 0
    high_confidence = 0

    for item in enriched:
        if item.get("suggested_price"):
            qty = item.get("original_quantity", 1) or 1
            total_estimated += item["suggested_price"] * float(qty)
        if item.get("confidence") == "high":
            high_confidence += 1

    return {
        "items": enriched,
        "total_estimated": round(total_estimated, 2),
        "items_with_pricing": sum(1 for i in enriched if i.get("suggested_price")),
        "items_total": len(enriched),
        "high_confidence_count": high_confidence,
        "pricing_coverage": f"{sum(1 for i in enriched if i.get('suggested_price'))}/{len(enriched)}",
    }


# ── PC/RFQ Item Enrichment ─────────────────────────────────────

def enrich_extracted_items(items):
    """Enrich extracted PC/RFQ line items with market intelligence."""
    enriched = []
    for item in items:
        desc = item.get("description", "")
        qty = item.get("quantity") or item.get("qty") or 1
        cost = item.get("unit_price") or item.get("cost") or item.get("pricing", {}).get("unit_cost")

        if not desc:
            enriched.append({**item, "intelligence": None})
            continue

        catalog_matches = search_catalog(desc, limit=5)
        competitor_prices = get_competitor_prices(desc, limit=15)
        reytech_prices = get_reytech_prices(desc, limit=5)

        # Normalize market prices
        market_prices = []
        for cp in competitor_prices:
            norm = _normalize_unit_price(
                _parse_price_str(cp.get("unit_price")),
                cp.get("description", ""),
                _parse_qty_str(cp.get("quantity"))
            )
            if norm and norm["normalized_price"] > 0:
                market_prices.append({**cp, "normalized_price": norm["normalized_price"],
                                      "normalization": norm["normalization"]})

        market_analysis = _analyze_market(market_prices, qty)
        recommendation = _calculate_recommendation(
            cost=float(cost) if cost else None,
            market=market_analysis,
            reytech_history=reytech_prices,
            quantity=qty, description=desc,
        )
        verification = _verify_item_match(desc, catalog_matches)

        enriched.append({
            **item,
            "intelligence": {
                "catalog_matches": catalog_matches[:3],
                "market_analysis": market_analysis,
                "reytech_history": reytech_prices[:3],
                "competitor_breakdown": market_prices[:8],
                "recommendation": recommendation,
                "verification": verification,
            }
        })

    return enriched


def _analyze_market(market_prices, request_qty):
    """Analyze what agencies actually pay for this item."""
    import math
    from datetime import datetime as _dt

    if not market_prices:
        return {"data_points": 0, "message": "No market data found"}

    now = _dt.now()
    weighted = []
    for mp in market_prices:
        price = mp.get("normalized_price", 0)
        if not price or price <= 0:
            continue
        date_str = mp.get("date", "")
        weight = 0.5
        for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
            try:
                dt = _dt.strptime(str(date_str).strip(), fmt)
                age_days = max(0, (now - dt).days)
                weight = math.pow(0.5, age_days / 180)
                break
            except (ValueError, TypeError):
                continue
        weighted.append({"price": price, "weight": weight,
                         "supplier": mp.get("supplier", ""), "date": date_str})

    if not weighted:
        return {"data_points": 0, "message": "No valid prices"}

    total_w = sum(w["weight"] for w in weighted)
    wavg = sum(w["price"] * w["weight"] for w in weighted) / total_w if total_w > 0 else 0
    prices = [w["price"] for w in weighted]

    reytech_entries = [w for w in weighted if "REYTECH" in (w.get("supplier", "") or "").upper()]
    reytech_avg = None
    if reytech_entries:
        rt_w = sum(w["weight"] for w in reytech_entries)
        reytech_avg = sum(w["price"] * w["weight"] for w in reytech_entries) / rt_w if rt_w > 0 else None

    comp_entries = [w for w in weighted if "REYTECH" not in (w.get("supplier", "") or "").upper()]
    comp_avg = comp_low = comp_high = None
    if comp_entries:
        ct_w = sum(w["weight"] for w in comp_entries)
        comp_avg = sum(w["price"] * w["weight"] for w in comp_entries) / ct_w if ct_w > 0 else None
        comp_low = min(w["price"] for w in comp_entries)
        comp_high = max(w["price"] for w in comp_entries)

    best_weight = max(w["weight"] for w in weighted)
    freshness = "fresh" if best_weight > 0.7 else ("aging" if best_weight > 0.4 else "stale")

    return {
        "data_points": len(weighted), "freshness": freshness,
        "market_weighted_avg": round(wavg, 4) if wavg else None,
        "market_low": round(min(prices), 4), "market_high": round(max(prices), 4),
        "reytech_weighted_avg": round(reytech_avg, 4) if reytech_avg else None,
        "competitor_weighted_avg": round(comp_avg, 4) if comp_avg else None,
        "competitor_low": round(comp_low, 4) if comp_low else None,
        "competitor_high": round(comp_high, 4) if comp_high else None,
        "unique_suppliers": len(set(w.get("supplier", "") for w in weighted)),
    }


def _calculate_recommendation(cost, market, reytech_history, quantity, description):
    """Calculate recommended quote price with margin tiers."""
    result = {"quote_price": None, "markup_pct": None, "margin_analysis": [],
              "rationale": "", "confidence": "low", "visual_indicator": "red"}

    has_cost = cost is not None and cost > 0
    has_market = market.get("data_points", 0) > 0
    comp_avg = market.get("competitor_weighted_avg")
    market_avg = market.get("market_weighted_avg")
    reytech_avg = market.get("reytech_weighted_avg")

    if has_cost and has_market and comp_avg:
        comp_low = market.get("competitor_low") or comp_avg
        comp_high = market.get("competitor_high") or comp_avg
        qty_float = float(quantity) if quantity else 1

        # Strategy: price just under the ceiling, not above the floor
        # Ceiling: 2% under competitor weighted avg (wins most bids at max margin)
        ceiling_price = round(comp_avg * 0.98, 2)
        ceiling_markup = ((ceiling_price - cost) / cost * 100) if cost > 0 else 0
        ceiling_margin_total = round((ceiling_price - cost) * qty_float, 2)

        # Competitive: 2% under competitor low (guaranteed win, lower margin)
        competitive_price = round(comp_low * 0.98, 2)
        competitive_markup = ((competitive_price - cost) / cost * 100) if cost > 0 else 0
        competitive_margin_total = round((competitive_price - cost) * qty_float, 2)

        # Floor: minimum acceptable (15%)
        floor_price = round(cost * 1.15, 2)
        floor_margin_total = round((floor_price - cost) * qty_float, 2)

        strategies = [
            {
                "name": "Maximize Margin",
                "price": ceiling_price,
                "markup_pct": round(ceiling_markup, 1),
                "margin_per_unit": round(ceiling_price - cost, 2),
                "margin_total": ceiling_margin_total,
                "rationale": f"2% under competitor avg (${comp_avg:.2f}). Wins most bids at highest margin.",
                "risk": "low",
            },
            {
                "name": "Undercut All",
                "price": competitive_price,
                "markup_pct": round(competitive_markup, 1),
                "margin_per_unit": round(competitive_price - cost, 2),
                "margin_total": competitive_margin_total,
                "rationale": f"2% under lowest competitor (${comp_low:.2f}). Guaranteed win, lower margin.",
                "risk": "none",
            },
            {
                "name": "Floor",
                "price": floor_price,
                "markup_pct": 15,
                "margin_per_unit": round(floor_price - cost, 2),
                "margin_total": floor_margin_total,
                "rationale": "Minimum 15% markup. Below this isn't worth the work.",
                "risk": "n/a",
            },
        ]

        # Markup tiers for reference
        tiers = []
        for pct in [15, 20, 25, 30, 35, 40, 45, 50]:
            tier_price = round(cost * (1 + pct / 100), 2)
            beats_avg = tier_price < comp_avg
            beats_low = tier_price < comp_low
            tiers.append({
                "pct": pct, "price": tier_price,
                "margin_per_unit": round(tier_price - cost, 2),
                "margin_total": round((tier_price - cost) * qty_float, 2),
                "beats_comp_avg": beats_avg, "beats_comp_low": beats_low,
                "tag": "WINS" if beats_avg else ("ABOVE AVG" if beats_low else "ABOVE ALL"),
            })

        result["strategies"] = strategies
        result["markup_tiers"] = tiers

        # Default: maximize margin (ceiling) unless below floor
        if ceiling_price > floor_price:
            result["quote_price"] = ceiling_price
            result["markup_pct"] = round(ceiling_markup, 1)
            result["confidence"] = "high"
            result["visual_indicator"] = "green"
            result["rationale"] = (
                f"Cost: ${cost:.2f} -> Quote: ${ceiling_price:.2f} "
                f"({round(ceiling_markup, 1)}% markup). "
                f"Just under market avg ${comp_avg:.2f}. "
                f"Margin: ${ceiling_price - cost:.2f}/unit x {int(qty_float)} = "
                f"${ceiling_margin_total:,.2f}"
            )
        elif competitive_price > floor_price:
            result["quote_price"] = competitive_price
            result["markup_pct"] = round(competitive_markup, 1)
            result["confidence"] = "medium"
            result["visual_indicator"] = "yellow"
            result["rationale"] = (
                f"Tight market. Cost: ${cost:.2f}. "
                f"Competitor low: ${comp_low:.2f}. "
                f"Best competitive: ${competitive_price:.2f} "
                f"({round(competitive_markup, 1)}% markup). "
                f"Margin: ${competitive_margin_total:,.2f} on {int(qty_float)} units."
            )
        else:
            result["quote_price"] = floor_price
            result["markup_pct"] = 15
            result["confidence"] = "low"
            result["visual_indicator"] = "red"
            result["rationale"] = (
                f"Competitors at ${comp_low:.2f} — below your "
                f"15% floor of ${floor_price:.2f}. "
                f"Negotiate better cost or pass."
            )

        # Highlight money difference between strategies
        if len(strategies) >= 2:
            money_diff = strategies[0]["margin_total"] - strategies[1]["margin_total"]
            if money_diff > 50:
                result["rationale"] += (
                    f" | Maximize vs Undercut = ${money_diff:,.2f} difference"
                )

    elif has_cost:
        tiers = []
        for pct in [15, 20, 25, 30]:
            price = round(cost * (1 + pct / 100), 2)
            tiers.append({"pct": pct, "price": price,
                          "margin_dollar": round(cost * pct / 100, 2),
                          "margin_on_qty": round(cost * pct / 100 * float(quantity), 2)})
        result["margin_analysis"] = tiers
        result["quote_price"] = round(cost * 1.25, 2)
        result["markup_pct"] = 25
        result["confidence"] = "low"
        result["visual_indicator"] = "yellow"
        result["rationale"] = f"Cost: ${cost:.2f} -> ${result['quote_price']:.2f} (25% default). No market data."

    elif has_market and comp_avg:
        suggested = round(comp_avg * 0.92, 2)
        result["quote_price"] = suggested
        result["confidence"] = "medium"
        result["visual_indicator"] = "yellow"
        result["rationale"] = f"No cost data. Competitor avg: ${comp_avg:.2f}. Suggested: ${suggested:.2f} (8% undercut)."
        if reytech_avg:
            result["rationale"] += f" Your history: ${reytech_avg:.2f}."

    else:
        result["rationale"] = "No cost or market data. Manual research required."

    return result


def _verify_item_match(description, catalog_matches):
    """Check confidence that we matched the right item."""
    if not catalog_matches:
        return {"status": "no_match", "confidence": 0,
                "message": "Item not in catalog — new item or check description"}

    best = catalog_matches[0]
    score = best.get("relevance_score", 0)
    if score > 0.7:
        return {"status": "high_match", "confidence": score,
                "matched_description": best["description"], "message": "Strong match"}
    elif score > 0.4:
        return {"status": "partial_match", "confidence": score,
                "matched_description": best["description"], "message": "Possible match — verify"}
    else:
        return {"status": "weak_match", "confidence": score,
                "matched_description": best["description"], "message": "Weak match — search manually"}


def learn_new_item(description, unit_price, quantity=1, uom="",
                   supplier="", department="", po_number="", date=""):
    """Add a newly-priced item to the catalog for future reference."""
    try:
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=10)
        db.execute("""
            INSERT INTO scprs_catalog
            (description, last_unit_price, last_quantity, last_uom,
             last_supplier, last_department, last_po_number,
             last_date, times_seen, updated_at)
            VALUES (?,?,?,?,?,?,?,?,1,datetime('now'))
            ON CONFLICT(description) DO UPDATE SET
                last_unit_price = excluded.last_unit_price,
                last_quantity = excluded.last_quantity,
                times_seen = scprs_catalog.times_seen + 1,
                updated_at = datetime('now')
        """, (description[:500], unit_price, quantity, uom,
              supplier, department, po_number, date))
        db.commit()
        db.close()
        log.info("Learned catalog item: %s @ $%s", description[:60], unit_price)
    except Exception as e:
        log.warning("Learn item failed: %s", e)
