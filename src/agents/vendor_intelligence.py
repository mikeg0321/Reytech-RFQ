"""
src/agents/vendor_intelligence.py — Vendor Intelligence Engine (PRD-28 WI-5)

Provides:
  1. Vendor scorecard (price, reliability, speed, breadth)
  2. Preferred vendor matrix per product category
  3. Vendor comparison for quote building
  4. Basic enrichment from stored data
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from collections import defaultdict

log = logging.getLogger("vendor_intel")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import get_db
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")
    from contextlib import contextmanager
    @contextmanager
    def get_db():
        conn = sqlite3.connect(os.path.join(DATA_DIR, "reytech.db"), timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


# ── Vendor Loading ────────────────────────────────────────────────────────────

def _load_vendors() -> list:
    """Load vendors from DB (single source of truth)."""
    try:
        from src.core.dal import get_all_vendors
        return get_all_vendors()
    except Exception:
        try:
            with open(os.path.join(DATA_DIR, "vendors.json")) as f:
                return json.load(f)
        except Exception:
            return []


def _save_vendors(vendors: list):
    """Save vendors to DB (single source of truth)."""
    try:
        from src.core.dal import save_all_vendors
        save_all_vendors(vendors)
    except Exception:
        with open(os.path.join(DATA_DIR, "vendors.json"), "w") as f:
            json.dump(vendors, f, indent=2, default=str)


# ── Vendor Scoring ────────────────────────────────────────────────────────────

def score_all_vendors() -> dict:
    """Score all vendors based on price history, order data, and catalog coverage."""
    vendors = _load_vendors()
    now = datetime.now(timezone.utc).isoformat()
    scored = 0

    # Build pricing data from price_history
    price_data = _get_vendor_price_data()
    # Build order performance data
    order_data = _get_vendor_order_data()

    for vendor in vendors:
        name = vendor.get("name", "").strip()
        if not name:
            continue

        name_lower = name.lower()
        score = _calculate_vendor_score(name_lower, price_data, order_data)

        vendor["price_score"] = score["price_score"]
        vendor["reliability_score"] = score["reliability_score"]
        vendor["speed_score"] = score["speed_score"]
        vendor["breadth_score"] = score["breadth_score"]
        vendor["overall_score"] = score["overall_score"]
        vendor["scored_at"] = now
        vendor["categories_served"] = score.get("categories", [])
        scored += 1

        # Save to DB
        try:
            with get_db() as conn:
                conn.execute("""
                    INSERT INTO vendor_scores
                    (vendor_name, scored_at, price_score, reliability_score,
                     speed_score, breadth_score, overall_score, categories)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (name, now, score["price_score"], score["reliability_score"],
                      score["speed_score"], score["breadth_score"],
                      score["overall_score"], json.dumps(score.get("categories", []))))
        except Exception:
            pass

    _save_vendors(vendors)
    log.info("Scored %d vendors", scored)
    return {"ok": True, "scored": scored}


def _get_vendor_price_data() -> dict:
    """Get pricing data by vendor from price_history."""
    data = defaultdict(lambda: {"prices": [], "categories": set()})
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT source, description, unit_price, found_at
                FROM price_history
                WHERE unit_price > 0
            """).fetchall()
            for r in rows:
                source = (r["source"] or "").lower()
                data[source]["prices"].append(r["unit_price"])
                # Rough category from description
                desc = (r["description"] or "").lower()
                for cat in ["glove", "mask", "gown", "syringe", "bandage", "gauze",
                            "sanitizer", "thermometer", "dental", "surgical"]:
                    if cat in desc:
                        data[source]["categories"].add(cat)
    except Exception:
        pass
    return data


def _get_vendor_order_data() -> dict:
    """Get order performance data by vendor."""
    data = defaultdict(lambda: {"orders": 0, "delivered": 0, "total_value": 0})
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT vendor_name, status, total
                FROM vendor_orders
            """).fetchall()
            for r in rows:
                name = (r["vendor_name"] or "").lower()
                data[name]["orders"] += 1
                data[name]["total_value"] += r["total"] or 0
                if r["status"] in ("delivered", "shipped"):
                    data[name]["delivered"] += 1
    except Exception:
        pass
    return data


def _calculate_vendor_score(vendor_lower: str, price_data: dict, order_data: dict) -> dict:
    """Calculate composite vendor score."""
    # Price score: lower avg price = higher score (relative to other vendors)
    pd = price_data.get(vendor_lower, {})
    prices = pd.get("prices", [])
    categories = list(pd.get("categories", set()))

    # Check common name variations
    for key in price_data:
        if vendor_lower in key or key in vendor_lower:
            prices.extend(price_data[key].get("prices", []))
            categories.extend(list(price_data[key].get("categories", set())))

    price_score = 50  # default
    if prices:
        avg = sum(prices) / len(prices)
        # More data points = higher confidence
        price_score = min(80, 40 + len(prices) * 2)

    # Reliability: delivered / total orders
    od = order_data.get(vendor_lower, {})
    orders = od.get("orders", 0)
    delivered = od.get("delivered", 0)
    reliability_score = 50  # default
    if orders > 0:
        reliability_score = round(delivered / orders * 100)

    # Speed score: placeholder (would need timestamp tracking)
    speed_score = 50

    # Breadth: number of categories
    breadth_score = min(100, len(set(categories)) * 15)

    # Overall weighted
    overall = round(
        price_score * 0.35 +
        reliability_score * 0.30 +
        speed_score * 0.15 +
        breadth_score * 0.20
    )

    return {
        "price_score": price_score,
        "reliability_score": reliability_score,
        "speed_score": speed_score,
        "breadth_score": breadth_score,
        "overall_score": overall,
        "categories": list(set(categories)),
    }


# ── Preferred Vendor Matrix ───────────────────────────────────────────────────

def get_preferred_vendors() -> dict:
    """Get preferred vendor ranking per product category."""
    vendors = _load_vendors()
    scored_vendors = [v for v in vendors if v.get("overall_score", 0) > 0]

    # Group by category
    by_category = defaultdict(list)
    for v in scored_vendors:
        for cat in v.get("categories_served", []):
            by_category[cat].append({
                "name": v.get("name", ""),
                "overall_score": v.get("overall_score", 0),
                "price_score": v.get("price_score", 0),
                "reliability_score": v.get("reliability_score", 0),
            })

    # Sort each category by overall score
    matrix = {}
    for cat, vlist in by_category.items():
        vlist.sort(key=lambda x: x["overall_score"], reverse=True)
        matrix[cat] = vlist[:5]  # Top 5 per category

    return {"ok": True, "matrix": matrix, "categories": len(matrix)}


# ── Vendor Comparison ─────────────────────────────────────────────────────────

def compare_vendors(product_description: str) -> list:
    """Compare vendors for a specific product."""
    desc_lower = product_description.lower()
    results = []

    try:
        with get_db() as conn:
            # Get price history for this product from different sources
            rows = conn.execute("""
                SELECT source, unit_price, found_at, source_url
                FROM price_history
                WHERE LOWER(description) LIKE ?
                ORDER BY found_at DESC
            """, (f"%{desc_lower[:30]}%",)).fetchall()

            by_source = defaultdict(list)
            for r in rows:
                by_source[r["source"]].append({
                    "price": r["unit_price"],
                    "found_at": r["found_at"],
                    "url": r["source_url"],
                })

            for source, prices in by_source.items():
                latest = prices[0]
                avg = sum(p["price"] for p in prices) / len(prices)
                results.append({
                    "vendor": source,
                    "latest_price": latest["price"],
                    "avg_price": round(avg, 2),
                    "price_points": len(prices),
                    "last_checked": latest["found_at"],
                    "url": latest.get("url", ""),
                })

            # Sort by latest price
            results.sort(key=lambda x: x["latest_price"])

    except Exception as e:
        log.error("compare_vendors: %s", e)

    return results


# ── Vendor Enrichment Summary ─────────────────────────────────────────────────

def get_enrichment_status() -> dict:
    """Report on vendor data completeness."""
    vendors = _load_vendors()
    total = len(vendors)

    with_email = sum(1 for v in vendors if isinstance(v, dict) and (v.get("email") or "").strip())
    with_phone = sum(1 for v in vendors if isinstance(v, dict) and (v.get("phone") or "").strip())
    with_website = sum(1 for v in vendors if isinstance(v, dict) and (v.get("website") or "").strip())
    with_score = sum(1 for v in vendors if isinstance(v, dict) and v.get("overall_score", 0) > 0)

    return {
        "total_vendors": total,
        "with_email": with_email,
        "with_phone": with_phone,
        "with_website": with_website,
        "with_score": with_score,
        "email_pct": round(with_email / max(total, 1) * 100, 1),
        "scored_pct": round(with_score / max(total, 1) * 100, 1),
    }


# ── Agent Status ──────────────────────────────────────────────────────────────

def get_agent_status() -> dict:
    enrichment = get_enrichment_status()
    return {
        "name": "vendor_intelligence",
        "status": "ok",
        **enrichment,
    }
