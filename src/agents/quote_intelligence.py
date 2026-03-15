"""
Quote Intelligence Engine
Matches RFQ line items against FI$Cal catalog data.
Provides pricing suggestions and competitor comparisons.
"""
import re
import logging
from difflib import SequenceMatcher

log = logging.getLogger("reytech.quote_intelligence")


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
            scored.append({
                "description": row[0], "unspsc": row[1],
                "last_unit_price": row[2], "last_quantity": row[3],
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
        return [{
            "description": r[0], "unit_price": r[1], "quantity": r[2],
            "uom": r[3], "supplier": r[4], "department": r[5],
            "po_number": r[6], "date": r[7], "buyer_name": r[8],
            "buyer_email": r[9],
            "is_reytech": "REYTECH" in (r[4] or "").upper(),
        } for r in rows]
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
    """Calculate optimal price based on history and competition."""
    result = {"price": None, "rationale": "", "confidence": "low"}

    def _parse_price(p):
        try:
            return float(str(p).replace("$", "").replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    reytech_nums = [_parse_price(p["unit_price"]) for p in reytech_prices]
    reytech_nums = [n for n in reytech_nums if n and n > 0]

    comp_nums = [_parse_price(p["unit_price"]) for p in competitor_prices if not p.get("is_reytech")]
    comp_nums = [n for n in comp_nums if n and n > 0]

    if reytech_nums:
        latest_reytech = reytech_nums[0]
        if comp_nums:
            avg_comp = sum(comp_nums) / len(comp_nums)
            min_comp = min(comp_nums)
            suggested = min(latest_reytech, avg_comp * 0.95)
            suggested = max(suggested, min_comp * 0.9)
            result["price"] = round(suggested, 2)
            result["rationale"] = (
                f"Your last price: ${latest_reytech:.2f}. "
                f"Competitor avg: ${avg_comp:.2f} (low: ${min_comp:.2f}). "
                f"Suggested: ${suggested:.2f} (5% below competitor avg)"
            )
            result["confidence"] = "high"
        else:
            result["price"] = round(latest_reytech, 2)
            result["rationale"] = f"Based on your last winning price: ${latest_reytech:.2f}. No competitor data."
            result["confidence"] = "medium"
    elif comp_nums:
        avg_comp = sum(comp_nums) / len(comp_nums)
        suggested = avg_comp * 0.92
        result["price"] = round(suggested, 2)
        result["rationale"] = f"No Reytech history. Competitor avg: ${avg_comp:.2f}. Suggested: ${suggested:.2f} (8% undercut)"
        result["confidence"] = "medium"
    else:
        result["rationale"] = "No pricing data available. Research needed."

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
