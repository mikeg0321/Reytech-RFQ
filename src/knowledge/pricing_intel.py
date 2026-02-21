"""
Pricing Intelligence Engine
============================
Captures winning prices when orders are created from won quotes.
Builds a historical pricing database for smarter future quoting.

Key flows:
1. Quote wins → Order created → record_winning_prices() called
2. Pricing page queries historical data for recommendations
3. Quote generator uses historical wins for better pricing
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict

log = logging.getLogger("reytech.pricing_intel")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")


def _get_conn():
    import sqlite3
    conn = sqlite3.connect(os.path.join(DATA_DIR, "reytech.db"))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_pricing_intel_tables():
    """Create pricing intelligence tables."""
    conn = _get_conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS winning_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL,
                quote_number TEXT,
                po_number TEXT,
                order_id TEXT,
                agency TEXT,
                institution TEXT,
                description TEXT NOT NULL,
                part_number TEXT,
                sku TEXT,
                qty REAL DEFAULT 1,
                sell_price REAL NOT NULL,
                cost REAL DEFAULT 0,
                margin_pct REAL DEFAULT 0,
                supplier TEXT,
                category TEXT,
                catalog_product_id INTEGER,
                fingerprint TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wp_fingerprint ON winning_prices(fingerprint)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wp_part ON winning_prices(part_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wp_institution ON winning_prices(institution)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wp_recorded ON winning_prices(recorded_at)")
        conn.commit()
    except Exception as e:
        log.debug("Pricing intel table init: %s", e)
    finally:
        conn.close()


# Initialize on import
init_pricing_intel_tables()


def _item_fingerprint(desc: str, part_number: str = "") -> str:
    """Create a fingerprint for matching similar items across orders."""
    raw = (part_number.strip().lower() if part_number else "") or desc.strip().lower()[:80]
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _match_catalog_product(desc: str, part_number: str = "", sku: str = "") -> Optional[int]:
    """Try to match an item to a product_catalog entry."""
    conn = _get_conn()
    try:
        # Try by SKU first
        if sku:
            row = conn.execute("SELECT id FROM product_catalog WHERE sku=? LIMIT 1", (sku,)).fetchone()
            if row:
                return row[0]
        # Try by part number
        if part_number:
            row = conn.execute("SELECT id FROM product_catalog WHERE sku=? LIMIT 1", (part_number,)).fetchone()
            if row:
                return row[0]
        # Try by name fuzzy match
        if desc:
            words = desc.strip().lower().split()[:3]
            if words:
                like = f"%{words[0]}%"
                row = conn.execute(
                    "SELECT id FROM product_catalog WHERE LOWER(name) LIKE ? LIMIT 1", (like,)
                ).fetchone()
                if row:
                    return row[0]
        return None
    except Exception:
        return None
    finally:
        conn.close()


def record_winning_prices(order: dict):
    """Record all line item prices from a won order into pricing intelligence.
    
    Called when:
    - Quote status → "won"
    - Order created from PO email
    - Manual order creation
    """
    oid = order.get("order_id", "")
    qn = order.get("quote_number", "")
    po = order.get("po_number", "")
    agency = order.get("agency", "")
    institution = order.get("institution", "")
    items = order.get("line_items", [])
    
    if not items:
        return 0
    
    conn = _get_conn()
    recorded = 0
    try:
        now = datetime.now().isoformat()
        for it in items:
            desc = it.get("description", "") or ""
            pn = it.get("part_number", "") or ""
            sku = it.get("sku", "") or pn
            qty = it.get("qty", 0) or it.get("quantity", 0) or 1
            sell = it.get("unit_price", 0) or it.get("price", 0) or 0
            cost = it.get("cost", 0) or 0
            supplier = it.get("supplier", "") or ""
            
            if not sell or not desc:
                continue
            
            margin = round((sell - cost) / sell * 100, 1) if sell > 0 and cost > 0 else 0
            fp = _item_fingerprint(desc, pn)
            cat_id = _match_catalog_product(desc, pn, sku)
            
            conn.execute("""
                INSERT INTO winning_prices 
                (recorded_at, quote_number, po_number, order_id, agency, institution,
                 description, part_number, sku, qty, sell_price, cost, margin_pct,
                 supplier, catalog_product_id, fingerprint)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (now, qn, po, oid, agency, institution,
                  desc[:500], pn[:100], sku[:100], qty, sell, cost, margin,
                  supplier[:200], cat_id, fp))
            recorded += 1
            
            # Update catalog product win stats if matched
            if cat_id:
                try:
                    conn.execute("""
                        UPDATE product_catalog SET 
                            times_won = COALESCE(times_won, 0) + 1,
                            last_sold_price = ?,
                            last_sold_date = ?,
                            avg_margin_won = CASE 
                                WHEN COALESCE(avg_margin_won, 0) = 0 THEN ?
                                ELSE (COALESCE(avg_margin_won, 0) + ?) / 2
                            END,
                            updated_at = ?
                        WHERE id = ?
                    """, (sell, now[:10], margin, margin, now, cat_id))
                except Exception:
                    pass
        
        conn.commit()
        log.info("Recorded %d winning prices from order %s (quote=%s, po=%s)", recorded, oid, qn, po)
    except Exception as e:
        log.error("Error recording winning prices: %s", e)
    finally:
        conn.close()
    
    return recorded


def get_price_recommendation(description: str = "", part_number: str = "",
                              agency: str = "", institution: str = "") -> dict:
    """Get price recommendation based on historical winning prices.
    
    Returns:
        {avg_price, min_price, max_price, recent_price, count, recommendation, history[]}
    """
    conn = _get_conn()
    try:
        conditions = []
        params = []
        
        if part_number:
            fp = _item_fingerprint("", part_number)
            conditions.append("fingerprint = ?")
            params.append(fp)
        elif description:
            fp = _item_fingerprint(description)
            conditions.append("fingerprint = ?")
            params.append(fp)
        else:
            return {"count": 0, "recommendation": None}
        
        where = " AND ".join(conditions)
        rows = conn.execute(f"""
            SELECT sell_price, cost, margin_pct, agency, institution, qty, 
                   recorded_at, quote_number, po_number
            FROM winning_prices WHERE {where}
            ORDER BY recorded_at DESC LIMIT 50
        """, params).fetchall()
        
        if not rows:
            return {"count": 0, "recommendation": None}
        
        prices = [r["sell_price"] for r in rows if r["sell_price"]]
        costs = [r["cost"] for r in rows if r["cost"]]
        
        avg_price = sum(prices) / len(prices) if prices else 0
        min_price = min(prices) if prices else 0
        max_price = max(prices) if prices else 0
        recent_price = prices[0] if prices else 0
        avg_cost = sum(costs) / len(costs) if costs else 0
        
        # Recommendation: weighted average (recent prices matter more)
        weighted_sum = 0
        weight_total = 0
        for i, p in enumerate(prices[:10]):
            w = 10 - i  # Most recent = highest weight
            weighted_sum += p * w
            weight_total += w
        recommended = round(weighted_sum / weight_total, 2) if weight_total else avg_price
        
        # Agency-specific pricing
        agency_prices = {}
        for r in rows:
            ag = r["agency"] or "Unknown"
            agency_prices.setdefault(ag, []).append(r["sell_price"])
        
        if agency and agency in agency_prices:
            agency_avg = sum(agency_prices[agency]) / len(agency_prices[agency])
            recommended = round((recommended + agency_avg) / 2, 2)
        
        return {
            "count": len(rows),
            "avg_price": round(avg_price, 2),
            "min_price": round(min_price, 2),
            "max_price": round(max_price, 2),
            "recent_price": round(recent_price, 2),
            "avg_cost": round(avg_cost, 2),
            "recommended_price": recommended,
            "history": [
                {
                    "price": r["sell_price"],
                    "cost": r["cost"],
                    "margin": r["margin_pct"],
                    "agency": r["agency"],
                    "institution": r["institution"],
                    "qty": r["qty"],
                    "date": r["recorded_at"][:10],
                    "quote": r["quote_number"],
                    "po": r["po_number"],
                }
                for r in rows[:20]
            ],
            "agency_breakdown": {
                k: {"avg": round(sum(v)/len(v), 2), "count": len(v)}
                for k, v in agency_prices.items()
            },
        }
    except Exception as e:
        log.error("Price recommendation error: %s", e)
        return {"count": 0, "recommendation": None}
    finally:
        conn.close()


def get_pricing_intelligence_summary() -> dict:
    """Get overall pricing intelligence summary for the dashboard."""
    conn = _get_conn()
    try:
        total = conn.execute("SELECT COUNT(*) FROM winning_prices").fetchone()[0]
        unique_items = conn.execute("SELECT COUNT(DISTINCT fingerprint) FROM winning_prices").fetchone()[0]
        unique_agencies = conn.execute("SELECT COUNT(DISTINCT agency) FROM winning_prices WHERE agency != ''").fetchone()[0]
        total_revenue = conn.execute("SELECT SUM(sell_price * qty) FROM winning_prices").fetchone()[0] or 0
        avg_margin = conn.execute("SELECT AVG(margin_pct) FROM winning_prices WHERE margin_pct > 0").fetchone()[0] or 0
        
        # Recent wins (last 30 days)
        cutoff = (datetime.now() - timedelta(days=30)).isoformat()
        recent = conn.execute(
            "SELECT COUNT(*) FROM winning_prices WHERE recorded_at > ?", (cutoff,)
        ).fetchone()[0]
        
        # Top items by frequency
        top_items = conn.execute("""
            SELECT description, part_number, COUNT(*) as wins, 
                   ROUND(AVG(sell_price), 2) as avg_price,
                   ROUND(AVG(margin_pct), 1) as avg_margin,
                   MAX(recorded_at) as last_won
            FROM winning_prices
            GROUP BY fingerprint
            ORDER BY wins DESC LIMIT 15
        """).fetchall()
        
        # Top agencies by revenue
        top_agencies = conn.execute("""
            SELECT agency, COUNT(*) as wins, 
                   ROUND(SUM(sell_price * qty), 2) as total_revenue,
                   ROUND(AVG(margin_pct), 1) as avg_margin
            FROM winning_prices WHERE agency != ''
            GROUP BY agency
            ORDER BY total_revenue DESC LIMIT 10
        """).fetchall()
        
        # Margin distribution
        margin_dist = {
            "negative": conn.execute("SELECT COUNT(*) FROM winning_prices WHERE margin_pct < 0").fetchone()[0],
            "low": conn.execute("SELECT COUNT(*) FROM winning_prices WHERE margin_pct BETWEEN 0 AND 10").fetchone()[0],
            "mid": conn.execute("SELECT COUNT(*) FROM winning_prices WHERE margin_pct BETWEEN 10 AND 25").fetchone()[0],
            "high": conn.execute("SELECT COUNT(*) FROM winning_prices WHERE margin_pct > 25").fetchone()[0],
        }
        
        return {
            "total_records": total,
            "unique_items": unique_items,
            "unique_agencies": unique_agencies,
            "total_revenue": round(total_revenue, 2),
            "avg_margin": round(avg_margin, 1),
            "recent_wins_30d": recent,
            "top_items": [dict(r) for r in top_items],
            "top_agencies": [dict(r) for r in top_agencies],
            "margin_distribution": margin_dist,
        }
    except Exception as e:
        log.error("Pricing intel summary error: %s", e)
        return {"total_records": 0}
    finally:
        conn.close()


def get_item_price_trends(fingerprint: str = "", part_number: str = "",
                           description: str = "", limit: int = 50) -> list:
    """Get price trend for a specific item over time."""
    conn = _get_conn()
    try:
        if fingerprint:
            fp = fingerprint
        elif part_number:
            fp = _item_fingerprint("", part_number)
        elif description:
            fp = _item_fingerprint(description)
        else:
            return []
        
        rows = conn.execute("""
            SELECT sell_price, cost, margin_pct, agency, institution,
                   qty, recorded_at, quote_number, po_number
            FROM winning_prices WHERE fingerprint = ?
            ORDER BY recorded_at DESC LIMIT ?
        """, (fp, limit)).fetchall()
        
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()
