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
    uom TEXT DEFAULT 'EA',

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
    times_lost INTEGER DEFAULT 0,
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

    best_cost REAL,
    best_supplier TEXT,

    recommended_price REAL,
    price_strategy TEXT,
    margin_opportunity REAL,

    photo_url TEXT,
    manufacturer TEXT,
    mfg_number TEXT,
    search_tokens TEXT,

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
CREATE INDEX IF NOT EXISTS idx_catalog_tokens ON product_catalog(search_tokens);
CREATE INDEX IF NOT EXISTS idx_catalog_mfg ON product_catalog(mfg_number);
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

# Multi-supplier pricing per product
SUPPLIER_SCHEMA = """
CREATE TABLE IF NOT EXISTS product_suppliers (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id      INTEGER NOT NULL,
    supplier_name   TEXT NOT NULL,
    supplier_url    TEXT,
    sku             TEXT,
    last_price      REAL,
    last_checked    TEXT,
    shipping_est    REAL DEFAULT 0,
    in_stock        INTEGER DEFAULT 1,
    reliability     REAL DEFAULT 0.5,
    notes           TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT,
    UNIQUE(product_id, supplier_name)
);
CREATE INDEX IF NOT EXISTS idx_prodsup_prod ON product_suppliers(product_id);
CREATE INDEX IF NOT EXISTS idx_prodsup_supplier ON product_suppliers(supplier_name);
"""

# ── UOM normalization map ────────────────────────────────────────────────
UOM_MAP = {
    "EA": "EA", "EACH": "EA", "BX": "BX", "BOX": "BX", "PK": "PK",
    "PACK": "PK", "CS": "CS", "CASE": "CS", "DZ": "DZ", "DOZEN": "DZ",
    "CT": "CT", "RL": "RL", "ROLL": "RL", "BG": "BG", "BAG": "BG",
    "PR": "PR", "PAIR": "PR", "SET": "SET", "BT": "BT", "BOTTLE": "BT",
    "TB": "TB", "TUBE": "TB", "GL": "GL", "GALLON": "GL", "OZ": "OZ",
    "LB": "LB", "KT": "KT", "KIT": "KT", "": "EA",
}

# ── Token generation for fuzzy matching ──────────────────────────────────
STOP_WORDS = {
    "the", "a", "an", "and", "or", "for", "of", "to", "in", "on", "by",
    "with", "is", "at", "from", "as", "per", "ea", "each", "set", "pkg",
    "package", "box", "case", "unit", "lot", "item", "no", "number",
    "non", "inventory", "single", "yes", "qty", "pack", "count",
}


def _tokenize(text: str) -> str:
    """Convert text to space-separated lowercase search tokens for fuzzy matching."""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^\w\s\-/]', ' ', text)
    tokens = text.split()
    tokens = [t for t in tokens if t not in STOP_WORDS and len(t) >= 2]
    return " ".join(sorted(set(tokens)))


def _parse_uom(sku_field: str) -> str:
    """QB SKU field is often the UOM (EA, BX, PK), not a real SKU."""
    val = (sku_field or "").strip().upper()
    return UOM_MAP.get(val, val if len(val) <= 4 else "EA")


def _clean_description(raw: str) -> str:
    """
    Clean QB Sales Description:
    - Remove leading part number line (often duplicated from name)
    - Strip delivery notes, lot numbers, expiry dates
    - Collapse whitespace
    """
    if not raw:
        return ""
    lines = [l.strip() for l in raw.replace('\r\n', '\n').split('\n') if l.strip()]
    if not lines:
        return ""

    desc_lines = []
    for line in lines:
        # Skip lines that are just a part number
        if re.match(r'^[A-Z0-9\-]{3,20}$', line, re.IGNORECASE):
            continue
        # Skip delivery/lot/expiry notes
        if re.match(r'^\*\*', line) or 'delivery' in line.lower():
            continue
        if re.match(r'^(Lot|LOT|Exp|EXP|REF|Ref)\b', line):
            continue
        desc_lines.append(line)

    if not desc_lines:
        return lines[0][:200]

    # Prefer the longest descriptive line
    desc_lines.sort(key=len, reverse=True)
    result = desc_lines[0]

    # Append packaging info from secondary line if useful
    if len(desc_lines) > 1 and len(desc_lines[1]) > 10:
        secondary = desc_lines[1]
        if any(kw in secondary.upper() for kw in ['BX', 'CS', 'PK', 'PACK', 'BOX', 'CASE', '/']):
            result += f" — {secondary}"

    return result[:300]


def _get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_catalog_db():
    """Create tables if they don't exist. Adds new columns on existing tables."""
    conn = _get_conn()
    conn.executescript(CATALOG_SCHEMA)
    conn.executescript(CATALOG_INDEXES)
    conn.executescript(PRICE_HISTORY_SCHEMA)
    conn.executescript(PRICE_HISTORY_INDEXES)
    conn.executescript(SUPPLIER_SCHEMA)
    # Migrate: add columns that may not exist on older DBs
    for col_def in [
        ("uom", "TEXT DEFAULT 'EA'"),
        ("times_lost", "INTEGER DEFAULT 0"),
        ("best_cost", "REAL"),
        ("best_supplier", "TEXT"),
        ("photo_url", "TEXT"),
        ("manufacturer", "TEXT"),
        ("mfg_number", "TEXT"),
        ("search_tokens", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE product_catalog ADD COLUMN {col_def[0]} {col_def[1]}")
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()
    conn.close()
    log.info("Product catalog DB initialized (with product_suppliers + search_tokens)")


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

    QB format quirks:
    - Product/Service Name = part number (e.g. "1019769", "0165SI22")
    - SKU field = usually UOM (EA, BX, PK) NOT a real SKU
    - Sales Description = multiline, often starts with part# repeated

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

            raw_desc = (row.get("Sales Description", "") or "").strip()
            desc = _clean_description(raw_desc)
            if not desc:
                desc = name

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

            # UOM from SKU field (QB puts UOM in SKU)
            raw_sku = (row.get("SKU", "") or "").strip()
            uom = _parse_uom(raw_sku)
            # If SKU is a real part# (>4 chars, not a known UOM), keep it
            sku = raw_sku if (len(raw_sku) > 4 and raw_sku.upper() not in UOM_MAP) else ""

            item_type = (row.get("Item type", "") or "Non-Inventory").strip()
            taxable = 1 if (row.get("Taxable", "") or "").lower() == "yes" else 0
            income_acct = (row.get("Income Account", "") or "").strip()
            expense_acct = (row.get("Expense Account", "") or "").strip()

            # Generate search tokens from name + description
            search_tokens = _tokenize(f"{name} {desc}")

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
                # Update pricing + new fields
                conn.execute("""
                    UPDATE product_catalog SET
                        sell_price = ?, cost = ?, margin_pct = ?,
                        sku = COALESCE(NULLIF(?, ''), sku),
                        description = COALESCE(NULLIF(?, ''), description),
                        category = ?, price_strategy = ?,
                        uom = COALESCE(NULLIF(?, ''), uom),
                        search_tokens = ?,
                        qb_name = ?, qb_item_type = ?,
                        qb_income_account = ?, qb_expense_account = ?,
                        taxable = ?, updated_at = ?
                    WHERE id = ?
                """, (sell_price, cost, margin_pct, sku, desc,
                      category, strategy, uom, search_tokens,
                      name, item_type,
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
                        name, sku, description, category, item_type, uom,
                        sell_price, cost, margin_pct, search_tokens,
                        qb_name, qb_item_type, qb_income_account, qb_expense_account,
                        taxable, price_strategy, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (name, sku, desc, category, item_type, uom,
                      sell_price, cost, margin_pct, search_tokens,
                      name, item_type, income_acct, expense_acct,
                      taxable, strategy, now, now))

                pid = cursor.lastrowid
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
        SELECT id, name, sku, sell_price, cost, margin_pct, category, description, uom
        FROM product_catalog
        WHERE name LIKE ? OR sku LIKE ? OR description LIKE ?
        ORDER BY times_quoted DESC, sell_price DESC
        LIMIT ?
    """, (f"%{partial}%", f"%{partial}%", f"%{partial}%", limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════════════
# Auto-Match Engine — Match PC/RFQ line items to catalog products
# ═══════════════════════════════════════════════════════════════════════

def match_item(description: str, part_number: str = "", top_n: int = 3) -> list:
    """
    Find best catalog matches for a line item from a Price Check or RFQ.
    Uses tiered strategy: exact part# → part# in description → token overlap.
    
    Returns list of dicts with match_confidence (0-1) and product data.
    """
    init_catalog_db()
    conn = _get_conn()
    matches = []
    seen_ids = set()

    # Strategy 1: Exact part number match (highest confidence)
    if part_number and part_number.strip():
        pn = part_number.strip()
        rows = conn.execute(
            "SELECT * FROM product_catalog WHERE name=? OR sku=? OR mfg_number=? LIMIT 5",
            (pn, pn, pn)
        ).fetchall()
        for r in rows:
            if r["id"] not in seen_ids:
                m = dict(r)
                m["match_confidence"] = 0.98
                m["match_reason"] = f"Exact part# match: {pn}"
                matches.append(m)
                seen_ids.add(r["id"])

    # Strategy 2: Part number extracted from description
    if not matches and description:
        potential_parts = re.findall(r'\b([A-Z0-9][\w\-]{3,19})\b', description, re.IGNORECASE)
        for pp in potential_parts[:5]:
            row = conn.execute(
                "SELECT * FROM product_catalog WHERE name=? LIMIT 1", (pp,)
            ).fetchone()
            if row and row["id"] not in seen_ids:
                m = dict(row)
                m["match_confidence"] = 0.92
                m["match_reason"] = f"Part# found in description: {pp}"
                matches.append(m)
                seen_ids.add(row["id"])
                break

    # Strategy 3: Token-based fuzzy matching using search_tokens
    if len(matches) < top_n and description:
        desc_tokens = set(_tokenize(description).split())
        if desc_tokens:
            # Use longest tokens for best selectivity
            search_terms = sorted(desc_tokens, key=len, reverse=True)[:3]
            conditions = " OR ".join(["search_tokens LIKE ?" for _ in search_terms])
            params = [f"%{t}%" for t in search_terms]
            candidates = conn.execute(
                f"SELECT * FROM product_catalog WHERE {conditions} LIMIT 50",
                params
            ).fetchall()

            for r in candidates:
                if r["id"] in seen_ids:
                    continue
                prod_tokens = set((r["search_tokens"] or "").split())
                if not prod_tokens:
                    # Fallback: tokenize description for old rows without search_tokens
                    prod_tokens = set(_tokenize(f"{r['name']} {r['description'] or ''}").split())
                if not prod_tokens:
                    continue
                # Jaccard similarity
                intersection = desc_tokens & prod_tokens
                union = desc_tokens | prod_tokens
                similarity = len(intersection) / len(union) if union else 0
                if similarity >= 0.25:
                    m = dict(r)
                    m["match_confidence"] = round(min(similarity * 1.3, 0.95), 2)
                    m["match_reason"] = f"Token match: {len(intersection)} shared ({similarity:.0%})"
                    matches.append(m)
                    seen_ids.add(r["id"])

    # Strategy 4: Description LIKE (broadest)
    if len(matches) < top_n and description and len(description) > 5:
        first_words = " ".join(description.split()[:3])
        rows = conn.execute(
            "SELECT * FROM product_catalog WHERE description LIKE ? LIMIT 10",
            (f"%{first_words}%",)
        ).fetchall()
        for r in rows:
            if r["id"] not in seen_ids:
                m = dict(r)
                m["match_confidence"] = 0.40
                m["match_reason"] = f"Description contains: '{first_words[:30]}'"
                matches.append(m)
                seen_ids.add(r["id"])

    conn.close()
    # Sort by confidence descending
    matches.sort(key=lambda x: x.get("match_confidence", 0), reverse=True)
    return matches[:top_n]


def match_items_batch(items: list) -> list:
    """
    Match multiple items at once (for PC detail page on load).
    items: [{idx, description, part_number}, ...]
    Returns: [{idx, matched, product_id, confidence, ...}, ...]
    """
    results = []
    for item in items[:30]:
        desc = (item.get("description") or "").strip()
        part = (item.get("part_number") or "").strip()
        matches = match_item(desc, part, top_n=1)
        if matches and matches[0].get("match_confidence", 0) >= 0.40:
            best = matches[0]
            results.append({
                "idx": item.get("idx", 0),
                "matched": True,
                "id": best["id"],
                "product_id": best["id"],
                "canonical_name": best.get("description") or best.get("name", ""),
                "part_number": best.get("name", ""),
                "uom": best.get("uom", "EA"),
                "last_cost": best.get("cost"),
                "last_sell": best.get("sell_price"),
                "best_cost": best.get("best_cost") or best.get("cost"),
                "best_supplier": best.get("best_supplier", ""),
                "category": best.get("category", ""),
                "confidence": best.get("match_confidence", 0),
                "reason": best.get("match_reason", ""),
                "times_quoted": best.get("times_quoted", 0),
                "times_won": best.get("times_won", 0),
                "times_lost": best.get("times_lost", 0),
                "win_rate": best.get("win_rate", 0),
                "avg_margin_won": best.get("avg_margin_won", 0),
                "margin_pct": best.get("margin_pct", 0),
                "last_checked": best.get("updated_at", ""),
                "scprs_last_price": best.get("scprs_last_price"),
                "competitor_low_price": best.get("competitor_low_price"),
                "web_lowest_price": best.get("web_lowest_price"),
                "photo_url": best.get("photo_url", ""),
            })
        else:
            results.append({"idx": item.get("idx", 0), "matched": False})
    return results


# ═══════════════════════════════════════════════════════════════════════
# Supplier Pricing — Multi-supplier price tracking per product
# ═══════════════════════════════════════════════════════════════════════

def add_supplier_price(product_id: int, supplier_name: str, price: float,
                       url: str = "", sku: str = "", shipping: float = 0,
                       in_stock: bool = True) -> bool:
    """Record/update a supplier's price for a product."""
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("""INSERT INTO product_suppliers
            (product_id, supplier_name, supplier_url, sku, last_price, shipping_est,
             in_stock, last_checked, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(product_id, supplier_name) DO UPDATE SET
                supplier_url=COALESCE(NULLIF(excluded.supplier_url,''),supplier_url),
                sku=COALESCE(NULLIF(excluded.sku,''),sku),
                last_price=excluded.last_price,
                shipping_est=excluded.shipping_est,
                in_stock=excluded.in_stock,
                last_checked=excluded.last_checked,
                updated_at=excluded.updated_at""",
            (product_id, supplier_name, url, sku, price, shipping,
             1 if in_stock else 0, now, now, now)
        )
        # Update best_cost on product if this is cheaper
        conn.execute("""UPDATE product_catalog SET
            best_cost = CASE WHEN ? < best_cost OR best_cost IS NULL THEN ? ELSE best_cost END,
            best_supplier = CASE WHEN ? < best_cost OR best_cost IS NULL THEN ? ELSE best_supplier END,
            updated_at = ?
            WHERE id = ?""",
            (price, price, price, supplier_name, now, product_id)
        )
        conn.commit()
    finally:
        conn.close()
    return True


def get_product_suppliers(product_id: int) -> list:
    """Get all known suppliers + prices for a product, cheapest first."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM product_suppliers WHERE product_id=? ORDER BY last_price ASC",
        (product_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def rebuild_search_tokens():
    """One-time migration: build search_tokens for all existing products."""
    init_catalog_db()
    conn = _get_conn()
    rows = conn.execute("SELECT id, name, description FROM product_catalog").fetchall()
    updated = 0
    for r in rows:
        tokens = _tokenize(f"{r['name']} {r['description'] or ''}")
        if tokens:
            conn.execute("UPDATE product_catalog SET search_tokens=? WHERE id=?",
                         (tokens, r["id"]))
            updated += 1
    conn.commit()
    conn.close()
    log.info("Rebuilt search tokens for %d products", updated)
    return updated


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
        'times_quoted', 'times_won', 'times_lost', 'win_rate', 'avg_margin_won',
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


def record_outcome_to_catalog(pc: dict, outcome: str = "won",
                              competitor_name: str = "", competitor_price: float = 0):
    """
    When a Price Check is marked won or lost, feed results back to catalog.
    This is the critical feedback loop that makes pricing smarter over time.
    
    outcome: "won" | "lost"
    """
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    updated = 0
    items = pc.get("items", [])
    agency = pc.get("institution", "") or pc.get("agency", "")
    quote_num = pc.get("reytech_quote_number", "") or pc.get("pc_number", "")

    for item in items:
        if item.get("no_bid"):
            continue
        desc = (item.get("description") or "").strip()
        pn = str(item.get("item_number") or "").strip()
        if not desc and not pn:
            continue

        # Match to catalog product
        matches = match_item(desc, pn, top_n=1)
        if not matches or matches[0].get("match_confidence", 0) < 0.55:
            continue

        pid = matches[0]["id"]
        product = conn.execute("SELECT * FROM product_catalog WHERE id = ?", (pid,)).fetchone()
        if not product:
            continue
        product = dict(product)

        unit_price = item.get("unit_price") or item.get("pricing", {}).get("recommended_price") or 0
        unit_cost = item.get("vendor_cost") or item.get("pricing", {}).get("unit_cost") or 0

        if outcome == "won":
            tw = (product.get("times_won") or 0) + 1
            tl = product.get("times_lost") or 0
            total = tw + tl
            win_rate = round(tw / total * 100, 1) if total > 0 else 100.0

            # Running average of won margins
            old_avg = product.get("avg_margin_won") or 0
            if unit_price > 0 and unit_cost > 0:
                this_margin = round((unit_price - unit_cost) / unit_price * 100, 2)
                new_avg = round(((old_avg * (tw - 1)) + this_margin) / tw, 2) if tw > 1 else this_margin
            else:
                new_avg = old_avg

            updates = {
                "times_won": tw, "win_rate": win_rate, "avg_margin_won": new_avg,
            }
            if unit_price > 0:
                updates["last_sold_price"] = float(unit_price)
                updates["last_sold_date"] = now

            # Record to price history
            if unit_price > 0:
                conn.execute("""
                    INSERT INTO catalog_price_history
                    (product_id, price_type, price, source, agency, institution, quote_number, recorded_at)
                    VALUES (?, 'won_bid', ?, 'pc_won', ?, ?, ?, ?)
                """, (pid, unit_price, agency, agency, quote_num, now))

        else:  # lost
            tw = product.get("times_won") or 0
            tl = (product.get("times_lost") or 0) + 1
            total = tw + tl
            win_rate = round(tw / total * 100, 1) if total > 0 else 0.0

            updates = {"times_lost": tl, "win_rate": win_rate}

            if competitor_price and competitor_price > 0:
                updates["competitor_low_price"] = float(competitor_price)
                updates["competitor_source"] = competitor_name
                updates["competitor_date"] = now

            # Record competitor price to history
            if competitor_price and competitor_price > 0:
                conn.execute("""
                    INSERT INTO catalog_price_history
                    (product_id, price_type, price, source, agency, institution, quote_number, recorded_at)
                    VALUES (?, 'competitor_bid', ?, ?, ?, ?, ?, ?)
                """, (pid, competitor_price, competitor_name or "unknown", agency, agency, quote_num, now))

        # Apply updates
        if updates:
            sets = ", ".join(f"{k} = ?" for k in updates)
            vals = list(updates.values()) + [now, pid]
            conn.execute(f"UPDATE product_catalog SET {sets}, updated_at = ? WHERE id = ?", vals)
            updated += 1

    conn.commit()
    conn.close()
    log.info("record_outcome: %s → updated %d/%d catalog items", outcome, updated, len(items))
    return {"outcome": outcome, "updated": updated, "total": len(items)}


def get_smart_price(product_id: int, qty: int = 1,
                    agency: str = "", cost_override: float = 0) -> dict:
    """
    Intelligent per-item pricing using all available data.
    Returns recommended/aggressive/safe prices with reasoning.
    """
    conn = _get_conn()
    p = conn.execute("SELECT * FROM product_catalog WHERE id = ?", (product_id,)).fetchone()
    if not p:
        conn.close()
        return {}
    p = dict(p)

    # Gather all pricing signals
    cost = cost_override or p.get("cost") or p.get("best_cost") or 0
    scprs_price = p.get("scprs_last_price") or 0
    web_price = p.get("web_lowest_price") or 0
    competitor_low = p.get("competitor_low_price") or 0
    win_rate = p.get("win_rate") or 0
    times_won = p.get("times_won") or 0
    times_lost = p.get("times_lost") or 0
    avg_margin_won = p.get("avg_margin_won") or 0
    last_sell = p.get("last_sold_price") or p.get("sell_price") or 0

    # Get recent price history for this product
    history = conn.execute("""
        SELECT price_type, price, source, recorded_at
        FROM catalog_price_history WHERE product_id = ?
        ORDER BY recorded_at DESC LIMIT 10
    """, (product_id,)).fetchall()
    conn.close()

    won_prices = [h["price"] for h in history if h["price_type"] == "won_bid" and h["price"]]
    lost_prices = [h["price"] for h in history if h["price_type"] == "competitor_bid" and h["price"]]

    if cost <= 0:
        return {"error": "no_cost", "message": "No cost data — enter unit cost first"}

    reasoning = []

    # === CEILING: What's the max we can charge? ===
    ceiling = None
    if scprs_price > 0:
        ceiling = scprs_price
        reasoning.append(f"SCPRS ceiling: ${scprs_price:.2f}")
    if competitor_low > 0 and (ceiling is None or competitor_low < ceiling):
        ceiling = competitor_low
        reasoning.append(f"Competitor floor: ${competitor_low:.2f}")

    # === FLOOR: Minimum acceptable price ===
    floor = cost * 1.08  # At least 8% margin
    if cost < 50:
        floor = max(floor, cost + 5)   # $5 minimum profit on cheap items
    elif cost < 200:
        floor = max(floor, cost + 15)  # $15 minimum on mid-range
    else:
        floor = max(floor, cost + 50)  # $50 minimum on expensive items
    reasoning.append(f"Cost: ${cost:.2f} → floor: ${floor:.2f}")

    # === RECOMMENDED: Best price based on all signals ===
    if times_won >= 3 and avg_margin_won > 0:
        # We have enough win data — use historical winning margin
        recommended = round(cost / (1 - avg_margin_won / 100), 2)
        reasoning.append(f"Historical win margin: {avg_margin_won:.1f}% (won {times_won}x)")
    elif ceiling:
        # Undercut SCPRS/competitor by 2%
        recommended = round(ceiling * 0.98, 2)
        reasoning.append("Undercutting known ceiling by 2%")
    elif last_sell > 0:
        recommended = last_sell
        reasoning.append(f"Using last sold price: ${last_sell:.2f}")
    else:
        # Default: 25% markup
        recommended = round(cost * 1.25, 2)
        reasoning.append("Default 25% markup (no history)")

    # Clamp to floor/ceiling
    recommended = max(recommended, floor)
    if ceiling and recommended > ceiling:
        recommended = round(ceiling * 0.98, 2)

    # === AGGRESSIVE: Win at all costs ===
    aggressive = round(floor * 1.02, 2)  # Just above floor
    if lost_prices:
        # Try to beat the competitor who beat us
        lowest_competitor = min(lost_prices)
        aggressive = min(aggressive, round(lowest_competitor * 0.97, 2))
    aggressive = max(aggressive, floor)

    # === SAFE: Maximize margin ===
    safe = round(cost * 1.40, 2)  # 40% markup
    if ceiling:
        safe = min(safe, round(ceiling * 0.95, 2))
    safe = max(safe, recommended)

    # === WIN PROBABILITY ===
    if ceiling and ceiling > 0:
        # How far below ceiling are we?
        pct_below = (ceiling - recommended) / ceiling * 100
        win_prob = min(95, max(20, 50 + pct_below * 5))
    elif times_won + times_lost >= 3:
        win_prob = win_rate
    else:
        win_prob = 60  # Unknown

    margin_pct = round((recommended - cost) / recommended * 100, 1) if recommended > 0 else 0

    return {
        "product_id": product_id,
        "cost": round(cost, 2),
        "recommended": round(recommended, 2),
        "aggressive": round(aggressive, 2),
        "safe": round(safe, 2),
        "margin_pct": margin_pct,
        "win_probability": round(win_prob, 1),
        "reasoning": reasoning,
        "signals": {
            "scprs_ceiling": scprs_price,
            "competitor_low": competitor_low,
            "web_lowest": web_price,
            "win_rate": win_rate,
            "times_won": times_won,
            "times_lost": times_lost,
            "avg_margin_won": avg_margin_won,
            "won_prices": won_prices[:5],
            "lost_prices": lost_prices[:5],
        }
    }


def bulk_smart_price(items: list, agency: str = "") -> list:
    """
    Get smart pricing for a batch of PC items.
    Returns list of per-item recommendations.
    """
    results = []
    for item in items:
        desc = (item.get("description") or "").strip()
        pn = str(item.get("item_number") or "").strip()
        idx = item.get("idx", 0)
        cost = item.get("cost") or item.get("vendor_cost") or 0

        if not desc and not pn:
            results.append({"idx": idx, "matched": False})
            continue

        matches = match_item(desc, pn, top_n=1)
        if not matches or matches[0].get("match_confidence", 0) < 0.55:
            # No catalog match — provide basic markup
            if cost > 0:
                results.append({
                    "idx": idx, "matched": False,
                    "recommended": round(cost * 1.25, 2),
                    "aggressive": round(cost * 1.12, 2),
                    "safe": round(cost * 1.40, 2),
                    "reasoning": ["No catalog match — using default markups"],
                    "cost": cost,
                })
            else:
                results.append({"idx": idx, "matched": False})
            continue

        m = matches[0]
        pricing = get_smart_price(
            m["id"], qty=item.get("qty", 1),
            agency=agency, cost_override=cost if cost > 0 else 0
        )
        pricing["idx"] = idx
        pricing["matched"] = True
        pricing["catalog_name"] = m.get("name", "")
        pricing["match_confidence"] = m.get("match_confidence", 0)
        results.append(pricing)

    return results


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
