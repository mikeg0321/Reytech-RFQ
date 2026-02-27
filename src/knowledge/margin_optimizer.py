"""
Margin Optimizer: Smart Pricing Dashboard (F7)

Consolidates all pricing data to help optimize margins.
Provides per-item analysis, category rollups, and "should have won" detection.
"""

import logging
from datetime import datetime, timezone

log = logging.getLogger("reytech.margins")


def get_margin_summary() -> dict:
    """Category-level margin stats from won/lost quotes and price history."""
    try:
        from src.core.db import get_db
        import json

        with get_db() as conn:
            # Overall margin stats from quotes
            overall = conn.execute("""
                SELECT COUNT(*) as total_quotes,
                       SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) as won,
                       SUM(CASE WHEN status='closed_lost' THEN 1 ELSE 0 END) as lost,
                       AVG(CASE WHEN margin_pct > 0 THEN margin_pct END) as avg_margin,
                       SUM(CASE WHEN status='won' THEN total ELSE 0 END) as won_revenue,
                       SUM(CASE WHEN status='won' THEN total_cost ELSE 0 END) as won_cost
                FROM quotes WHERE is_test = 0
            """).fetchone()

            # Low-margin items (< 15%) from recent quotes
            low_margin_items = []
            quotes_with_items = conn.execute("""
                SELECT quote_number, agency, items_detail, total, total_cost, margin_pct, status
                FROM quotes
                WHERE is_test = 0 AND items_detail IS NOT NULL
                  AND margin_pct > 0 AND margin_pct < 15
                ORDER BY created_at DESC LIMIT 30
            """).fetchall()

            for q in quotes_with_items:
                try:
                    items = json.loads(q[2]) if isinstance(q[2], str) else q[2]
                    for item in (items or []):
                        cost = item.get("vendor_cost") or item.get("unit_cost") or 0
                        sell = item.get("unit_price") or item.get("sell_price") or 0
                        if cost and sell and cost > 0:
                            margin = (sell - cost) / sell * 100
                            if margin < 15:
                                low_margin_items.append({
                                    "description": (item.get("description") or "")[:80],
                                    "cost": round(cost, 2),
                                    "sell": round(sell, 2),
                                    "margin_pct": round(margin, 1),
                                    "quote": q[0],
                                    "agency": q[1] or "",
                                    "status": q[6] or "",
                                })
                except (json.JSONDecodeError, TypeError):
                    pass

            # "Should have won" — lost quotes within 5% of competitor price
            should_have_won = []
            lost_with_notes = conn.execute("""
                SELECT quote_number, agency, total, status_notes, items_text
                FROM quotes
                WHERE status = 'closed_lost' AND status_notes LIKE '%SCPRS%'
                  AND is_test = 0
                ORDER BY updated_at DESC LIMIT 20
            """).fetchall()

            for lq in lost_with_notes:
                notes = lq[3] or ""
                our_total = lq[2] or 0
                # Try to extract competitor price from notes
                import re
                price_match = re.search(r'\$[\d,]+(?:\.\d{2})?', notes)
                if price_match and our_total > 0:
                    try:
                        their_price = float(price_match.group().replace("$", "").replace(",", ""))
                        if their_price > 0:
                            gap_pct = abs(our_total - their_price) / their_price * 100
                            if gap_pct <= 5:
                                should_have_won.append({
                                    "quote": lq[0],
                                    "agency": lq[1] or "",
                                    "our_price": round(our_total, 2),
                                    "their_price": round(their_price, 2),
                                    "gap_pct": round(gap_pct, 1),
                                    "notes": notes[:150],
                                })
                    except ValueError:
                        pass

            # Price source breakdown
            price_sources = conn.execute("""
                SELECT source, COUNT(*) as count, AVG(unit_price) as avg_price
                FROM price_history
                GROUP BY source
                ORDER BY count DESC
            """).fetchall()

            # Category margin rollup from price history
            category_margins = conn.execute("""
                SELECT ph.source, COUNT(DISTINCT ph.description) as items,
                       AVG(ph.unit_price) as avg_price,
                       MIN(ph.unit_price) as min_price,
                       MAX(ph.unit_price) as max_price
                FROM price_history ph
                WHERE ph.unit_price > 0
                GROUP BY ph.source
                ORDER BY items DESC
            """).fetchall()

            return {
                "ok": True,
                "overall": {
                    "total_quotes": overall[0] or 0,
                    "won": overall[1] or 0,
                    "lost": overall[2] or 0,
                    "avg_margin_pct": round(overall[3] or 0, 1),
                    "won_revenue": round(overall[4] or 0, 2),
                    "won_cost": round(overall[5] or 0, 2),
                    "win_rate": round((overall[1] or 0) / max(1, (overall[1] or 0) + (overall[2] or 0)) * 100, 1),
                },
                "low_margin_items": sorted(low_margin_items, key=lambda x: x["margin_pct"])[:20],
                "should_have_won": should_have_won[:10],
                "price_sources": [
                    {"source": r[0], "count": r[1], "avg_price": round(r[2] or 0, 2)}
                    for r in price_sources
                ],
                "category_margins": [
                    {"source": r[0], "items": r[1], "avg": round(r[2] or 0, 2),
                     "min": round(r[3] or 0, 2), "max": round(r[4] or 0, 2)}
                    for r in category_margins
                ],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

    except Exception as e:
        log.error("Margin summary error: %s", e)
        return {"ok": False, "error": str(e)}


def get_item_pricing(description: str) -> dict:
    """Get all pricing data for a specific item description."""
    try:
        from src.core.db import get_db
        desc_lower = description.lower().strip()

        with get_db() as conn:
            # All price history for this item
            prices = conn.execute("""
                SELECT found_at, unit_price, source, agency, quote_number, notes
                FROM price_history
                WHERE LOWER(description) LIKE ?
                ORDER BY found_at DESC LIMIT 50
            """, (f"%{desc_lower}%",)).fetchall()

            # Won/lost prices from quotes
            won_prices = conn.execute("""
                SELECT q.quote_number, q.agency, q.total, q.status, q.items_detail
                FROM quotes q
                WHERE q.is_test = 0
                  AND (LOWER(q.items_text) LIKE ? OR LOWER(q.items_detail) LIKE ?)
                  AND q.status IN ('won', 'closed_lost')
                ORDER BY q.created_at DESC LIMIT 20
            """, (f"%{desc_lower}%", f"%{desc_lower}%")).fetchall()

            return {
                "ok": True,
                "description": description,
                "price_history": [
                    {"date": r[0], "price": r[1], "source": r[2],
                     "agency": r[3], "quote": r[4], "notes": r[5]}
                    for r in prices
                ],
                "quote_history": [
                    {"quote": r[0], "agency": r[1], "total": r[2], "status": r[3]}
                    for r in won_prices
                ],
                "stats": {
                    "count": len(prices),
                    "avg_price": round(sum(r[1] for r in prices) / max(1, len(prices)), 2) if prices else 0,
                    "min_price": round(min((r[1] for r in prices), default=0), 2),
                    "max_price": round(max((r[1] for r in prices), default=0), 2),
                },
            }
    except Exception as e:
        return {"ok": False, "error": str(e)}
