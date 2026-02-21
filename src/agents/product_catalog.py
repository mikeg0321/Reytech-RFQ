"""
Product Catalog Agent — Import, Search, Dynamic Pricing
========================================================
Import QB products CSV, maintain catalog DB, predictive search,
dynamic pricing intelligence (SCPRS + competitor + web scraping).
"""
import csv
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("reytech.product_catalog")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")

DB_PATH = os.path.join(DATA_DIR, "reytech.db")

# ═══════════════════════════════════════════════════════════════════════
# Schema
# ═══════════════════════════════════════════════════════════════════════

CATALOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS product_catalog (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    sku TEXT,
    description TEXT,
    category TEXT DEFAULT '',
    item_type TEXT DEFAULT 'Non-Inventory',
    
    sell_price REAL DEFAULT 0,
    cost REAL DEFAULT 0,
    margin_pct REAL DEFAULT 0,
    
    qb_name TEXT,
    qb_item_type TEXT,
    qb_income_account TEXT,
    qb_expense_account TEXT,
    taxable INTEGER DEFAULT 1,
    
    last_sold_price REAL,
    last_sold_date TEXT,
    times_quoted INTEGER DEFAULT 0,
    times_won INTEGER DEFAULT 0,
    win_rate REAL DEFAULT 0,
    avg_margin_won REAL,
    
    scprs_last_price REAL,
    scprs_last_date TEXT,
    scprs_agency TEXT,
    competitor_low_price REAL,
    competitor_source TEXT,
    competitor_date TEXT,
    web_lowest_price REAL,
    web_lowest_source TEXT,
    web_lowest_date TEXT,
    
    recommended_price REAL,
    price_strategy TEXT,
    margin_opportunity REAL,
    
    tags TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    created_at TEXT,
    updated_at TEXT,
    
    UNIQUE(name)
);
"""

CATALOG_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_catalog_sku ON product_catalog(sku);
CREATE INDEX IF NOT EXISTS idx_catalog_category ON product_catalog(category);
CREATE INDEX IF NOT EXISTS idx_catalog_name ON product_catalog(name);
"""

PRICE_HISTORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS catalog_price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER,
    price_type TEXT,
    price REAL,
    source TEXT,
    agency TEXT,
    institution TEXT,
    quote_number TEXT,
    recorded_at TEXT
);
"""

PRICE_HISTORY_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_cat_ph_product ON catalog_price_history(product_id);
CREATE INDEX IF NOT EXISTS idx_cat_ph_type ON catalog_price_history(price_type);
"""


def _get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_catalog_db():
    """Create tables if they don't exist."""
    conn = _get_conn()
    conn.executescript(CATALOG_SCHEMA)
    conn.executescript(CATALOG_INDEXES)
    conn.executescript(PRICE_HISTORY_SCHEMA)
    conn.executescript(PRICE_HISTORY_INDEXES)
    conn.commit()
    conn.close()
    log.info("Product catalog DB initialized")


# ═══════════════════════════════════════════════════════════════════════
# Auto-Categorization
# ═══════════════════════════════════════════════════════════════════════

CATEGORY_RULES = [
    ("Gloves", r'\b(glove|nitrile|latex|vinyl.*glove)\b'),
    ("Paper/Towels", r'\b(paper\s*towel|tissue|napkin|toilet\s*paper|bath\s*tissue|roll\s*towel)\b'),
    ("Cleaning/Sanitation", r'\b(soap|sanitiz|disinfect|cleaner|bleach|detergent|wipe|germicid)\b'),
    ("Toner/Printer", r'\b(toner|cartridge|ink\s*jet|xerox|printer|drum\s*unit|fuser)\b'),
    ("Medical/Clinical", r'\b(medical|surgical|syringe|bandage|gauze|catheter|fistula|wound|exam|lancet|glucose|thermometer|stethoscope|iv\s|dressing|specimen|swab)\b'),
    ("Batteries", r'\b(battery|batteries|duracell|energizer|alkaline)\b'),
    ("Trash/Liners", r'\b(trash|liner|can\s*liner|refuse|garbage|waste\s*bag)\b'),
    ("Food Service", r'\b(coffee|sugar|cream|cup|plate|fork|spoon|napkin|food\s*serv|straw|lid|container|bowl|tray)\b'),
    ("Office Supplies", r'\b(pen\b|pencil|marker|tape|stapl|folder|binder|envelope|paper\s*clip|rubber\s*band|post-it|sticky\s*note|label)\b'),
    ("Personal Care", r'\b(razor|shave|shaving|hygiene|shampoo|deodorant|toothbrush|toothpaste|lotion|cream.*shave)\b'),
    ("Laundry", r'\b(laundry|detergent.*laundry|fabric\s*soften|bleach.*laundry|dryer\s*sheet)\b'),
    ("Safety/PPE", r'\b(safety|respirator|mask|goggles|hard\s*hat|vest|ear\s*plug|fire\s*ext)\b'),
    ("Furniture", r'\b(chair|desk|table|shelf|cabinet|locker|mattress|bed\b|cot\b)\b'),
    ("Janitorial", r'\b(mop|broom|bucket|dustpan|squeegee|vacuum|janitor)\b'),
    ("Electrical", r'\b(bulb|lamp|light|fluorescent|led\s|ballast|outlet|surge)\b'),
]


def auto_categorize(name: str, description: str) -> str:
    """Assign category based on product name + description keywords."""
    text = f"{name} {description}".lower()
    for cat, pattern in CATEGORY_RULES:
        if re.search(pattern, text, re.IGNORECASE):
            return cat
    return "General"


# ═══════════════════════════════════════════════════════════════════════
# Import from QuickBooks CSV
# ═══════════════════════════════════════════════════════════════════════

def import_qb_csv(csv_path: str, replace: bool = False) -> dict:
    """
    Import QuickBooks Products/Services CSV into catalog DB.
    
    Returns: {imported: N, updated: N, skipped: N, errors: [...]}
    """
    init_catalog_db()
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    
    stats = {"imported": 0, "updated": 0, "skipped": 0, "errors": [], "categories": {}}
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    for row in rows:
        try:
            name = (row.get("Product/Service Name", "") or "").strip()
            if not name:
                stats["skipped"] += 1
                continue
            
            # Skip test items and $1M placeholder
            desc = (row.get("Sales Description", "") or "").strip()
            price_str = (row.get("Price", "") or "").strip().replace(",", "")
            cost_str = (row.get("Cost", "") or "").strip().replace(",", "")
            
            try:
                sell_price = float(price_str) if price_str else 0
            except ValueError:
                sell_price = 0
            try:
                cost = float(cost_str) if cost_str else 0
            except ValueError:
                cost = 0
                
            # Skip obvious test/placeholder items
            if name.lower() in ("test",) or sell_price >= 999999:
                stats["skipped"] += 1
                continue
            
            # Calculate margin
            margin_pct = round((sell_price - cost) / sell_price * 100, 2) if sell_price > 0 and cost > 0 else 0
            
            # Auto-categorize
            category = auto_categorize(name, desc)
            stats["categories"][category] = stats["categories"].get(category, 0) + 1
            
            sku = (row.get("SKU", "") or "").strip()
            item_type = (row.get("Item type", "") or "Non-Inventory").strip()
            taxable = 1 if (row.get("Taxable", "") or "").lower() == "yes" else 0
            income_acct = (row.get("Income Account", "") or "").strip()
            expense_acct = (row.get("Expense Account", "") or "").strip()
            
            # Determine price strategy
            if margin_pct < 0:
                strategy = "loss_leader"
            elif margin_pct < 5:
                strategy = "margin_protect"
            elif margin_pct > 25:
                strategy = "premium"
            else:
                strategy = "competitive"
            
            # Check if exists
            existing = conn.execute(
                "SELECT id FROM product_catalog WHERE name = ?", (name,)
            ).fetchone()
            
            if existing and not replace:
                # Update pricing only
                conn.execute("""
                    UPDATE product_catalog SET
                        sell_price = ?, cost = ?, margin_pct = ?,
                        sku = COALESCE(NULLIF(?, ''), sku),
                        description = COALESCE(NULLIF(?, ''), description),
                        category = ?, price_strategy = ?,
                        qb_name = ?, qb_item_type = ?,
                        qb_income_account = ?, qb_expense_account = ?,
                        taxable = ?, updated_at = ?
                    WHERE id = ?
                """, (sell_price, cost, margin_pct, sku, desc,
                      category, strategy, name, item_type,
                      income_acct, expense_acct, taxable, now, existing["id"]))
                stats["updated"] += 1
                
                # Record price history
                conn.execute("""
                    INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                    VALUES (?, 'sell', ?, 'quickbooks', ?)
                """, (existing["id"], sell_price, now))
                if cost > 0:
                    conn.execute("""
                        INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                        VALUES (?, 'cost', ?, 'quickbooks', ?)
                    """, (existing["id"], cost, now))
            else:
                if existing and replace:
                    conn.execute("DELETE FROM product_catalog WHERE id = ?", (existing["id"],))
                
                cursor = conn.execute("""
                    INSERT INTO product_catalog (
                        name, sku, description, category, item_type,
                        sell_price, cost, margin_pct,
                        qb_name, qb_item_type, qb_income_account, qb_expense_account,
                        taxable, price_strategy, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (name, sku, desc, category, item_type,
                      sell_price, cost, margin_pct,
                      name, item_type, income_acct, expense_acct,
                      taxable, strategy, now, now))
                
                pid = cursor.lastrowid
                # Record initial price history
                conn.execute("""
                    INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                    VALUES (?, 'sell', ?, 'quickbooks_import', ?)
                """, (pid, sell_price, now))
                if cost > 0:
                    conn.execute("""
                        INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                        VALUES (?, 'cost', ?, 'quickbooks_import', ?)
                    """, (pid, cost, now))
                
                stats["imported"] += 1
                
        except Exception as e:
            stats["errors"].append(f"{name}: {e}")
    
    conn.commit()
    conn.close()
    
    log.info("Product catalog import: %d imported, %d updated, %d skipped, %d errors",
             stats["imported"], stats["updated"], stats["skipped"], len(stats["errors"]))
    return stats


# ═══════════════════════════════════════════════════════════════════════
# Search & Lookup
# ═══════════════════════════════════════════════════════════════════════

def search_products(query: str, limit: int = 20, category: str = "",
                    min_margin: float = None, max_margin: float = None,
                    strategy: str = "") -> list:
    """
    Predictive search across name, SKU, description.
    Returns list of product dicts sorted by relevance.
    """
    init_catalog_db()
    conn = _get_conn()
    
    conditions = []
    params = []
    
    if query:
        # Split into terms for AND matching
        terms = query.strip().split()
        for term in terms:
            conditions.append(
                "(name LIKE ? OR sku LIKE ? OR description LIKE ? OR category LIKE ?)"
            )
            wild = f"%{term}%"
            params.extend([wild, wild, wild, wild])
    
    if category:
        conditions.append("category = ?")
        params.append(category)
    
    if min_margin is not None:
        conditions.append("margin_pct >= ?")
        params.append(min_margin)
    
    if max_margin is not None:
        conditions.append("margin_pct <= ?")
        params.append(max_margin)
    
    if strategy:
        conditions.append("price_strategy = ?")
        params.append(strategy)
    
    where = " AND ".join(conditions) if conditions else "1=1"
    
    rows = conn.execute(f"""
        SELECT *, 
               CASE WHEN name LIKE ? THEN 3
                    WHEN sku LIKE ? THEN 2
                    ELSE 1 END as relevance
        FROM product_catalog
        WHERE {where}
        ORDER BY relevance DESC, times_quoted DESC, sell_price DESC
        LIMIT ?
    """, [f"%{query}%", f"%{query}%"] + params + [limit]).fetchall()
    
    conn.close()
    return [dict(r) for r in rows]


def get_product(product_id: int) -> Optional[dict]:
    """Get single product with price history."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM product_catalog WHERE id = ?", (product_id,)
    ).fetchone()
    if not row:
        conn.close()
        return None
    
    product = dict(row)
    
    # Get price history
    history = conn.execute("""
        SELECT * FROM catalog_price_history
        WHERE product_id = ?
        ORDER BY recorded_at DESC LIMIT 50
    """, (product_id,)).fetchall()
    product["price_history"] = [dict(h) for h in history]
    
    conn.close()
    return product


def get_product_by_name(name: str) -> Optional[dict]:
    """Find product by exact name or fuzzy match."""
    conn = _get_conn()
    # Exact match first
    row = conn.execute(
        "SELECT * FROM product_catalog WHERE name = ?", (name,)
    ).fetchone()
    if not row:
        # Fuzzy match
        row = conn.execute(
            "SELECT * FROM product_catalog WHERE name LIKE ? LIMIT 1",
            (f"%{name}%",)
        ).fetchone()
    conn.close()
    return dict(row) if row else None


def predictive_lookup(partial: str, limit: int = 10) -> list:
    """Fast prefix search for typeahead/autocomplete."""
    init_catalog_db()
    conn = _get_conn()
    rows = conn.execute("""
        SELECT id, name, sku, sell_price, cost, margin_pct, category, description
        FROM product_catalog
        WHERE name LIKE ? OR sku LIKE ? OR description LIKE ?
        ORDER BY times_quoted DESC, sell_price DESC
        LIMIT ?
    """, (f"%{partial}%", f"%{partial}%", f"%{partial}%", limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════════════
# Catalog Stats & Analytics
# ═══════════════════════════════════════════════════════════════════════

def get_catalog_stats() -> dict:
    """Dashboard-level stats for the catalog."""
    init_catalog_db()
    conn = _get_conn()
    
    stats = {}
    stats["total_products"] = conn.execute("SELECT COUNT(*) FROM product_catalog").fetchone()[0]
    stats["with_cost"] = conn.execute("SELECT COUNT(*) FROM product_catalog WHERE cost > 0").fetchone()[0]
    stats["with_price"] = conn.execute("SELECT COUNT(*) FROM product_catalog WHERE sell_price > 0").fetchone()[0]
    
    # Margin distribution
    stats["negative_margin"] = conn.execute(
        "SELECT COUNT(*) FROM product_catalog WHERE margin_pct < 0 AND cost > 0"
    ).fetchone()[0]
    stats["low_margin"] = conn.execute(
        "SELECT COUNT(*) FROM product_catalog WHERE margin_pct >= 0 AND margin_pct < 10 AND cost > 0"
    ).fetchone()[0]
    stats["mid_margin"] = conn.execute(
        "SELECT COUNT(*) FROM product_catalog WHERE margin_pct >= 10 AND margin_pct < 25 AND cost > 0"
    ).fetchone()[0]
    stats["high_margin"] = conn.execute(
        "SELECT COUNT(*) FROM product_catalog WHERE margin_pct >= 25 AND cost > 0"
    ).fetchone()[0]
    
    # Average margin
    row = conn.execute(
        "SELECT AVG(margin_pct) FROM product_catalog WHERE cost > 0 AND sell_price > 0 AND sell_price < 999999"
    ).fetchone()
    stats["avg_margin"] = round(row[0] or 0, 1)
    
    # Total catalog value
    row = conn.execute(
        "SELECT SUM(sell_price), SUM(cost) FROM product_catalog WHERE sell_price < 999999"
    ).fetchone()
    stats["total_sell_value"] = round(row[0] or 0, 2)
    stats["total_cost_value"] = round(row[1] or 0, 2)
    
    # By category
    cats = conn.execute("""
        SELECT category, COUNT(*) as cnt, 
               ROUND(AVG(margin_pct), 1) as avg_margin,
               ROUND(SUM(sell_price), 2) as total_value
        FROM product_catalog 
        WHERE cost > 0 AND sell_price < 999999
        GROUP BY category ORDER BY cnt DESC
    """).fetchall()
    stats["categories"] = [dict(c) for c in cats]
    
    # Strategy distribution
    strats = conn.execute("""
        SELECT price_strategy, COUNT(*) as cnt
        FROM product_catalog GROUP BY price_strategy ORDER BY cnt DESC
    """).fetchall()
    stats["strategies"] = {s["price_strategy"]: s["cnt"] for s in strats}
    
    # Top margin opportunity (low margin with high volume potential)
    opportunities = conn.execute("""
        SELECT id, name, sell_price, cost, margin_pct, category, times_quoted
        FROM product_catalog
        WHERE margin_pct < 10 AND margin_pct >= 0 AND cost > 0 AND sell_price > 5
        ORDER BY sell_price DESC LIMIT 20
    """).fetchall()
    stats["margin_opportunities"] = [dict(o) for o in opportunities]
    
    # Negative margin items (losing money)
    losers = conn.execute("""
        SELECT id, name, sell_price, cost, margin_pct, category
        FROM product_catalog
        WHERE margin_pct < 0 AND cost > 0
        ORDER BY margin_pct ASC
    """).fetchall()
    stats["negative_margin_items"] = [dict(l) for l in losers]
    
    conn.close()
    return stats


# ═══════════════════════════════════════════════════════════════════════
# Dynamic Pricing Engine
# ═══════════════════════════════════════════════════════════════════════

def calculate_recommended_price(product_id: int, target_margin: float = 15.0,
                                 agency: str = "", institution: str = "") -> dict:
    """
    Calculate recommended sell price based on:
    1. Cost basis + target margin
    2. SCPRS historical prices (what state paid before)
    3. Competitor pricing (what others charge)
    4. Win/loss history (what price wins)
    
    Returns pricing recommendation with justification.
    """
    conn = _get_conn()
    product = conn.execute(
        "SELECT * FROM product_catalog WHERE id = ?", (product_id,)
    ).fetchone()
    if not product:
        conn.close()
        return {"error": "Product not found"}
    
    product = dict(product)
    cost = product["cost"] or 0
    current_price = product["sell_price"] or 0
    
    recommendations = []
    
    # Strategy 1: Cost-plus target margin
    if cost > 0:
        cost_plus = round(cost / (1 - target_margin / 100), 2)
        recommendations.append({
            "strategy": "cost_plus",
            "price": cost_plus,
            "margin_pct": target_margin,
            "margin_dollars": round(cost_plus - cost, 2),
            "rationale": f"Cost ${cost:.2f} + {target_margin}% margin"
        })
    
    # Strategy 2: SCPRS-informed (what the state has paid)
    if product.get("scprs_last_price") and product["scprs_last_price"] > 0:
        scprs_price = product["scprs_last_price"]
        scprs_margin = round((scprs_price - cost) / scprs_price * 100, 1) if cost > 0 else 0
        recommendations.append({
            "strategy": "scprs_match",
            "price": scprs_price,
            "margin_pct": scprs_margin,
            "margin_dollars": round(scprs_price - cost, 2) if cost > 0 else 0,
            "rationale": f"SCPRS last price: ${scprs_price:.2f} ({product.get('scprs_agency', '')})"
        })
    
    # Strategy 3: Competitor-based
    if product.get("competitor_low_price") and product["competitor_low_price"] > 0:
        comp_price = product["competitor_low_price"]
        # Price slightly below competitor
        undercut = round(comp_price * 0.98, 2)
        undercut_margin = round((undercut - cost) / undercut * 100, 1) if cost > 0 else 0
        recommendations.append({
            "strategy": "competitive_undercut",
            "price": undercut,
            "margin_pct": undercut_margin,
            "margin_dollars": round(undercut - cost, 2) if cost > 0 else 0,
            "rationale": f"2% below competitor ${comp_price:.2f} ({product.get('competitor_source', '')})"
        })
    
    # Strategy 4: Web price check
    if product.get("web_lowest_price") and product["web_lowest_price"] > 0:
        web_price = product["web_lowest_price"]
        if web_price < cost:
            recommendations.append({
                "strategy": "cheaper_source",
                "price": current_price,
                "margin_pct": product["margin_pct"],
                "margin_dollars": round(current_price - web_price, 2),
                "rationale": f"⚠️ Web price ${web_price:.2f} is BELOW our cost ${cost:.2f} — find this supplier!",
                "source": product.get("web_lowest_source", ""),
                "savings_if_sourced": round(cost - web_price, 2)
            })
    
    # Strategy 5: Win history
    won_prices = conn.execute("""
        SELECT AVG(price) as avg_won, MIN(price) as min_won, MAX(price) as max_won, COUNT(*) as wins
        FROM catalog_price_history
        WHERE product_id = ? AND price_type = 'won_bid'
    """, (product_id,)).fetchone()
    
    if won_prices and won_prices["wins"] and won_prices["avg_won"]:
        avg_won = round(won_prices["avg_won"], 2)
        won_margin = round((avg_won - cost) / avg_won * 100, 1) if cost > 0 else 0
        recommendations.append({
            "strategy": "historical_wins",
            "price": avg_won,
            "margin_pct": won_margin,
            "margin_dollars": round(avg_won - cost, 2) if cost > 0 else 0,
            "rationale": f"Avg winning bid: ${avg_won:.2f} (from {won_prices['wins']} wins, range ${won_prices['min_won']:.2f}-${won_prices['max_won']:.2f})"
        })
    
    # Choose best recommendation
    best = None
    if recommendations:
        # Prefer: highest margin that's still competitive
        valid = [r for r in recommendations if r["margin_pct"] >= 0]
        if valid:
            best = max(valid, key=lambda r: r["margin_pct"])
    
    # Calculate margin opportunity vs current
    margin_opportunity = 0
    if best and cost > 0:
        margin_opportunity = round(best["price"] - current_price, 2)
    
    conn.close()
    
    return {
        "product_id": product_id,
        "product_name": product["name"],
        "current_price": current_price,
        "cost": cost,
        "current_margin": product["margin_pct"],
        "recommendations": recommendations,
        "best": best,
        "margin_opportunity": margin_opportunity,
    }


def update_product_pricing(product_id: int, **kwargs):
    """Update pricing fields on a product."""
    ALLOWED = {
        'sell_price', 'cost', 'scprs_last_price', 'scprs_last_date', 'scprs_agency',
        'competitor_low_price', 'competitor_source', 'competitor_date',
        'web_lowest_price', 'web_lowest_source', 'web_lowest_date',
        'recommended_price', 'price_strategy', 'margin_opportunity',
        'category', 'tags', 'notes', 'last_sold_price', 'last_sold_date',
        'times_quoted', 'times_won', 'win_rate', 'avg_margin_won',
    }
    updates = {k: v for k, v in kwargs.items() if k in ALLOWED}
    if not updates:
        return False
    
    conn = _get_conn()
    sets = ", ".join(f"{k} = ?" for k in updates)
    vals = list(updates.values()) + [datetime.now(timezone.utc).isoformat(), product_id]
    conn.execute(f"UPDATE product_catalog SET {sets}, updated_at = ? WHERE id = ?", vals)
    
    # Recalculate margin
    conn.execute("""
        UPDATE product_catalog SET margin_pct = 
            CASE WHEN sell_price > 0 AND cost > 0 
                 THEN ROUND((sell_price - cost) / sell_price * 100, 2)
                 ELSE 0 END
        WHERE id = ?
    """, (product_id,))
    
    conn.commit()
    conn.close()
    return True


def record_won_price(product_name: str, price: float, agency: str = "",
                     institution: str = "", quote_number: str = ""):
    """Record a winning bid price for future pricing intelligence."""
    conn = _get_conn()
    product = conn.execute(
        "SELECT id FROM product_catalog WHERE name LIKE ? LIMIT 1",
        (f"%{product_name}%",)
    ).fetchone()
    
    if product:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT INTO catalog_price_history (product_id, price_type, price, source, agency, institution, quote_number, recorded_at)
            VALUES (?, 'won_bid', ?, 'order', ?, ?, ?, ?)
        """, (product["id"], price, agency, institution, quote_number, now))
        
        # Update product stats
        conn.execute("""
            UPDATE product_catalog SET 
                times_won = times_won + 1,
                last_sold_price = ?,
                last_sold_date = ?,
                updated_at = ?
            WHERE id = ?
        """, (price, now, now, product["id"]))
        
        conn.commit()
    conn.close()


def bulk_margin_analysis(min_price: float = 5.0) -> list:
    """
    Find all products where margin can be increased.
    Compares current sell price vs SCPRS/competitor/web data.
    Returns sorted by opportunity size (largest $ first).
    """
    conn = _get_conn()
    rows = conn.execute("""
        SELECT id, name, sell_price, cost, margin_pct, category,
               scprs_last_price, competitor_low_price, web_lowest_price,
               times_quoted, times_won
        FROM product_catalog
        WHERE sell_price >= ? AND cost > 0
        ORDER BY sell_price DESC
    """, (min_price,)).fetchall()
    
    opportunities = []
    for r in rows:
        r = dict(r)
        current = r["sell_price"]
        cost = r["cost"]
        
        # Check if we can raise price
        ceiling = max(
            r.get("scprs_last_price") or 0,
            r.get("competitor_low_price") or 0,
            current
        )
        
        if ceiling > current and ceiling > cost:
            new_margin = round((ceiling - cost) / ceiling * 100, 1)
            dollar_gain = round(ceiling - current, 2)
            opportunities.append({
                **r,
                "ceiling_price": ceiling,
                "new_margin_pct": new_margin,
                "dollar_gain_per_unit": dollar_gain,
                "source": "scprs" if (r.get("scprs_last_price") or 0) >= ceiling else "competitor"
            })
        
        # Check if cheaper source available
        if (r.get("web_lowest_price") or 999999) < cost:
            opportunities.append({
                **r,
                "cheaper_source": True,
                "web_price": r["web_lowest_price"],
                "savings_per_unit": round(cost - r["web_lowest_price"], 2),
                "source": "web"
            })
    
    opportunities.sort(key=lambda x: x.get("dollar_gain_per_unit", 0) + x.get("savings_per_unit", 0), reverse=True)
    return opportunities


# ═══════════════════════════════════════════════════════════════════════
# Link to existing orders/quotes
# ═══════════════════════════════════════════════════════════════════════

def link_order_items_to_catalog(order: dict):
    """
    Match order line items to catalog products.
    Updates items with catalog_id, cost, margin info.
    """
    conn = _get_conn()
    linked = 0
    for item in order.get("line_items", []):
        desc = item.get("description", "")
        pn = item.get("part_number", "")
        
        # Try exact name match, then part number, then fuzzy
        product = None
        if pn:
            row = conn.execute(
                "SELECT * FROM product_catalog WHERE name = ? OR sku = ?",
                (pn, pn)
            ).fetchone()
            if row:
                product = dict(row)
        
        if not product and desc:
            # Try first significant word
            words = [w for w in desc.split() if len(w) > 3]
            if words:
                row = conn.execute(
                    "SELECT * FROM product_catalog WHERE description LIKE ? LIMIT 1",
                    (f"%{words[0]}%",)
                ).fetchone()
                if row:
                    product = dict(row)
        
        if product:
            item["catalog_id"] = product["id"]
            item["catalog_cost"] = product["cost"]
            item["catalog_margin"] = product["margin_pct"]
            item["catalog_category"] = product["category"]
            linked += 1
    
    conn.close()
    return linked
