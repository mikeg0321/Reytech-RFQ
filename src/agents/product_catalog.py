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
from datetime import datetime, timezone, timedelta
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
    quantity REAL,
    source TEXT,
    agency TEXT,
    institution TEXT,
    quote_number TEXT,
    pc_id TEXT,
    supplier_url TEXT,
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


# ── Match Quality Verification ────────────────────────────────────────────────

# Key product-type terms — if one has it and the other doesn't, it's a mismatch
_PRODUCT_TYPE_GROUPS = [
    {"set", "combo", "kit", "bundle", "system", "package"},
    {"top", "shirt", "blouse", "jersey", "tunic"},
    {"pants", "pant", "trousers", "bottoms", "drawstring"},
    {"glove", "gloves", "nitrile", "latex", "vinyl"},
    {"mask", "masks", "n95", "kn95", "respirator"},
    {"gown", "gowns", "isolation"},
    {"brief", "briefs", "diaper", "incontinence", "pullup"},
    {"catheter", "foley", "drainage"},
    {"syringe", "needle", "insulin"},
    {"bandage", "gauze", "dressing", "wound"},
    {"tape", "adhesive", "medical tape"},
    {"wipe", "wipes", "sanitizer", "disinfectant"},
    {"scrub", "scrubs"},  # clothing vs cleaning
    {"brush", "scrubber", "sponge"},
]

_SIZE_TERMS = {"small", "medium", "large", "xl", "xxl", "xs", "2xl", "3xl",
               "sm", "md", "lg", "s", "m", "l", "pediatric", "adult", "youth"}

_UOM_TERMS = {"each", "ea", "box", "bx", "case", "cs", "pack", "pk",
              "pair", "pr", "dozen", "dz", "set"}


def _verify_match_quality(query_desc: str, catalog_desc: str,
                          catalog_name: str = "", query_price: float = 0,
                          catalog_price: float = 0) -> tuple:
    """
    Post-match semantic verification. Returns (penalty: float 0-1, reasons: list).
    penalty=0 means match is good, penalty=1 means completely wrong.
    Applied AFTER initial matching to catch false positives.
    """
    penalty = 0.0
    reasons = []

    q = (query_desc or "").lower()
    c = ((catalog_desc or "") + " " + (catalog_name or "")).lower()
    q_words = set(q.split())
    c_words = set(c.split())

    # ── 1. Product type mismatch (SET vs TOP, gloves vs gowns) ──
    for group in _PRODUCT_TYPE_GROUPS:
        q_has = q_words & group
        c_has = c_words & group
        if q_has and c_has and q_has != c_has:
            # Both mention this category but different specific terms
            penalty += 0.4
            reasons.append(f"product_type_mismatch: query={q_has}, catalog={c_has}")
            break
        if q_has and not c_has and len(q_has) > 0:
            # Query specifies a type the catalog doesn't mention
            for other_group in _PRODUCT_TYPE_GROUPS:
                if other_group != group and c_words & other_group:
                    penalty += 0.5
                    reasons.append(f"category_mismatch: query={q_has}, catalog has {c_words & other_group}")
                    break

    # ── 2. Size mismatch ──
    q_sizes = q_words & _SIZE_TERMS
    c_sizes = c_words & _SIZE_TERMS
    if q_sizes and c_sizes and q_sizes != c_sizes:
        penalty += 0.3
        reasons.append(f"size_mismatch: query={q_sizes}, catalog={c_sizes}")

    # ── 3. SET/COMBO vs individual item ──
    q_is_set = bool(q_words & {"set", "combo", "kit", "bundle", "package", "system"})
    c_is_set = bool(c_words & {"set", "combo", "kit", "bundle", "package", "system"})
    if q_is_set != c_is_set:
        penalty += 0.3
        reasons.append(f"set_vs_single: query_set={q_is_set}, catalog_set={c_is_set}")

    # ── 4. Brand/manufacturer mismatch ──
    # If both mention a brand name but different ones
    q_brand_candidates = {w for w in q_words if len(w) > 4 and w[0].isalpha()
                          and w not in STOP_WORDS and w not in _SIZE_TERMS
                          and w not in _UOM_TERMS}
    c_brand_candidates = {w for w in c_words if len(w) > 4 and w[0].isalpha()
                          and w not in STOP_WORDS and w not in _SIZE_TERMS
                          and w not in _UOM_TERMS}
    # Check if first significant word (likely brand) differs
    q_first = q.split()[:1]
    c_first = c.split()[:1]
    if q_first and c_first and q_first[0] != c_first[0] and len(q_first[0]) > 3 and len(c_first[0]) > 3:
        # Different brand — moderate penalty
        if q_first[0] not in c and c_first[0] not in q:
            penalty += 0.2
            reasons.append(f"brand_mismatch: query_brand='{q_first[0]}', catalog_brand='{c_first[0]}'")

    # ── 5. Price magnitude check ──
    if query_price > 0 and catalog_price > 0:
        ratio = max(query_price, catalog_price) / min(query_price, catalog_price)
        if ratio > 5:
            penalty += 0.3
            reasons.append(f"price_5x_diff: query=${query_price:.2f}, catalog=${catalog_price:.2f}")
        elif ratio > 3:
            penalty += 0.15
            reasons.append(f"price_3x_diff: query=${query_price:.2f}, catalog=${catalog_price:.2f}")

    # ── 6. Part number present but doesn't match ──
    # Extract potential part numbers from both
    q_parts = set(re.findall(r'\b[A-Z0-9][\w\-]{4,19}\b', query_desc or "", re.IGNORECASE))
    c_parts = set(re.findall(r'\b[A-Z0-9][\w\-]{4,19}\b',
                              (catalog_desc or "") + " " + (catalog_name or ""), re.IGNORECASE))
    if q_parts and c_parts and not (q_parts & c_parts):
        # Both have part numbers but none match
        penalty += 0.2
        reasons.append(f"part_numbers_differ: query={list(q_parts)[:2]}, catalog={list(c_parts)[:2]}")

    return (min(penalty, 1.0), reasons)


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
    conn.executescript(PRICE_HISTORY_SCHEMA)
    conn.executescript(PRICE_HISTORY_INDEXES)
    conn.executescript(SUPPLIER_SCHEMA)

    # Migrate FIRST: add columns that may not exist on older DBs
    # This MUST run before index creation (indexes reference these columns)
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
            conn.execute("ALTER TABLE product_catalog ADD COLUMN " + re.sub(r"[^a-zA-Z0-9_]", "", col_def[0]) + " " + col_def[1])
            log.info("Added column %s to product_catalog", col_def[0])
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                pass  # Column already exists
            else:
                log.warning("Catalog migration for %s: %s", col_def[0], e)
    conn.commit()

    # Verify search_tokens column exists (belt-and-suspenders)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(product_catalog)").fetchall()}
    if "search_tokens" not in cols:
        log.error("CRITICAL: search_tokens column still missing after migration! Columns: %s", cols)
    else:
        log.debug("Verified: search_tokens column exists in product_catalog")

    # Create indexes AFTER migrations (indexes may reference migrated columns)
    try:
        conn.executescript(CATALOG_INDEXES)
    except sqlite3.OperationalError as e:
        log.warning("Index creation error (non-fatal): %s", e)
        # Try indexes one at a time so one failure doesn't block others
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_catalog_sku ON product_catalog(sku)",
            "CREATE INDEX IF NOT EXISTS idx_catalog_category ON product_catalog(category)",
            "CREATE INDEX IF NOT EXISTS idx_catalog_name ON product_catalog(name)",
            "CREATE INDEX IF NOT EXISTS idx_catalog_tokens ON product_catalog(search_tokens)",
            "CREATE INDEX IF NOT EXISTS idx_catalog_mfg ON product_catalog(mfg_number)",
        ]:
            try:
                conn.execute(idx_sql)
            except sqlite3.OperationalError:
                pass
        conn.commit()

    conn.close()
    log.info("Product catalog DB initialized (with product_suppliers + search_tokens)")

    # Migrate price_history: add columns for richer tracking
    conn = _get_conn()
    for tbl, col_def in [
        ("catalog_price_history", ("quantity", "REAL")),
        ("catalog_price_history", ("pc_id", "TEXT")),
        ("catalog_price_history", ("supplier_url", "TEXT")),
    ]:
        try:
            conn.execute("ALTER TABLE " + re.sub(r"[^a-zA-Z0-9_]", "", tbl) + " ADD COLUMN " + re.sub(r"[^a-zA-Z0-9_]", "", col_def[0]) + " " + col_def[1])
            log.info("Added column %s to %s", col_def[0], tbl)
        except sqlite3.OperationalError:
            pass  # Already exists
    conn.commit()
    conn.close()


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
# Import from QuoteWerks CSV / TSV / Open Export
# ═══════════════════════════════════════════════════════════════════════

# Column name patterns — QuoteWerks exports vary by method:
#   Data Manager: database column names (ItemDescription, ManufacturerPartNumber, etc.)
#   Open Export: sometimes shortened names
#   Report export: user-chosen labels
# We match flexibly by checking multiple patterns per field.

_QW_COL_MAP = {
    "description": [
        "ItemDescription", "Description", "Item Description", "Product",
        "ProductDescription", "Product Description", "LineItemDescription",
        "Sales Description", "SalesDescription", "Desc",
    ],
    "part_number": [
        "ManufacturerPartNumber", "Manufacturer Part Number", "MfgPartNumber",
        "PartNumber", "Part Number", "Part#", "ItemNumber", "Item Number",
        "SKU", "Item#", "ProductCode", "Product Code", "VendorPartNumber",
    ],
    "cost": [
        "ItemCost", "Item Cost", "Cost", "UnitCost", "Unit Cost",
        "VendorCost", "Vendor Cost", "PurchaseCost", "Purchase Cost",
        "CostEach", "Cost Each",
    ],
    "price": [
        "ItemPrice", "Item Price", "Price", "SellPrice", "Sell Price",
        "UnitPrice", "Unit Price", "SalesPrice", "Sales Price",
        "ExtendedPrice", "QuotePrice", "ListPrice", "List Price",
    ],
    "qty": [
        "Quantity", "Qty", "ItemQuantity", "Item Quantity", "OrderQty",
    ],
    "uom": [
        "UnitOfMeasure", "Unit Of Measure", "UOM", "Unit",
    ],
    "customer": [
        "SoldToCompany", "Sold To Company", "Company", "CompanyName",
        "Company Name", "Customer", "CustomerName", "Customer Name",
        "SoldToContact", "ContactName",
    ],
    "quote_number": [
        "DocNumber", "Document Number", "QuoteNumber", "Quote Number",
        "Quote#", "DocNo", "DocumentNumber",
    ],
    "date": [
        "DocDate", "Document Date", "QuoteDate", "Quote Date", "Date",
        "CreateDate", "Created", "OrderDate", "CreatedDate",
    ],
    "manufacturer": [
        "Manufacturer", "Mfg", "Brand", "MfgName", "Manufacturer Name",
    ],
}


def _find_qw_column(headers: list, field: str) -> str:
    """Find the best matching column header for a QuoteWerks field."""
    patterns = _QW_COL_MAP.get(field, [])
    header_lower = {h.lower().strip(): h for h in headers}
    for pat in patterns:
        if pat.lower() in header_lower:
            return header_lower[pat.lower()]
    return ""


def import_quotewerks_csv(csv_path: str, replace: bool = False) -> dict:
    """
    Import QuoteWerks exported data (CSV/TSV) into the product catalog.

    Handles multiple QuoteWerks export formats:
      - Data Manager export (DocumentItems dataset → CSV)
      - Open Export Module (tab-delimited or XML)
      - Report exports (CSV)
      - Clipboard/Excel exports (tab-delimited)

    The importer auto-detects column names by matching against known
    QuoteWerks field patterns. It deduplicates against existing catalog
    items by part number and description tokens.

    Returns: {imported, updated, skipped, errors, total_rows, columns_found}
    """
    init_catalog_db()
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()

    stats = {
        "imported": 0, "updated": 0, "skipped": 0,
        "errors": [], "categories": {},
        "total_rows": 0, "columns_found": {},
    }

    # Read file — detect delimiter (tab vs comma)
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        sample = f.read(2048)
    delimiter = '\t' if sample.count('\t') > sample.count(',') else ','

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        headers = reader.fieldnames or []
        rows = list(reader)

    stats["total_rows"] = len(rows)

    # Map QuoteWerks columns to our fields
    col_desc = _find_qw_column(headers, "description")
    col_pn = _find_qw_column(headers, "part_number")
    col_cost = _find_qw_column(headers, "cost")
    col_price = _find_qw_column(headers, "price")
    col_qty = _find_qw_column(headers, "qty")
    col_uom = _find_qw_column(headers, "uom")
    col_customer = _find_qw_column(headers, "customer")
    col_quote = _find_qw_column(headers, "quote_number")
    col_date = _find_qw_column(headers, "date")
    col_mfg = _find_qw_column(headers, "manufacturer")

    stats["columns_found"] = {
        "description": col_desc, "part_number": col_pn,
        "cost": col_cost, "price": col_price,
        "qty": col_qty, "uom": col_uom,
        "customer": col_customer, "quote_number": col_quote,
        "date": col_date, "manufacturer": col_mfg,
    }

    if not col_desc and not col_pn:
        stats["errors"].append(
            f"Could not find description or part number columns. "
            f"Headers found: {headers[:15]}"
        )
        conn.close()
        return stats

    log.info("QW import: %d rows, delimiter='%s', desc='%s', pn='%s', cost='%s', price='%s'",
             len(rows), 'tab' if delimiter == '\t' else 'comma',
             col_desc, col_pn, col_cost, col_price)

    for row in rows:
        try:
            desc = (row.get(col_desc, "") or "").strip() if col_desc else ""
            pn = (row.get(col_pn, "") or "").strip() if col_pn else ""

            if not desc and not pn:
                stats["skipped"] += 1
                continue

            # Parse numeric fields
            def _parse_num(col):
                if not col:
                    return 0
                val = (row.get(col, "") or "").strip()
                val = val.replace(",", "").replace("$", "").strip()
                try:
                    return float(val) if val else 0
                except ValueError:
                    return 0

            cost = _parse_num(col_cost)
            sell_price = _parse_num(col_price)
            qty = _parse_num(col_qty) or 1

            # If we have extended price but not unit price, divide by qty
            if sell_price > 0 and qty > 1:
                # Check if this looks like an extended price (> 2x reasonable unit)
                if "extended" in (col_price or "").lower() or "total" in (col_price or "").lower():
                    sell_price = round(sell_price / qty, 4)
                    if cost > 0:
                        cost = round(cost / qty, 4)

            # Skip zero-value rows and obvious garbage
            if sell_price <= 0 and cost <= 0:
                stats["skipped"] += 1
                continue

            uom = "EA"
            if col_uom:
                raw_uom = (row.get(col_uom, "") or "").strip().upper()
                uom = _parse_uom(raw_uom) if raw_uom else "EA"

            customer = (row.get(col_customer, "") or "").strip() if col_customer else ""
            quote_num = (row.get(col_quote, "") or "").strip() if col_quote else ""
            mfg = (row.get(col_mfg, "") or "").strip() if col_mfg else ""
            date_str = (row.get(col_date, "") or "").strip() if col_date else ""

            # Build catalog name: prefer part number, fall back to description
            name = pn if pn and len(pn) >= 3 else ""
            if not name:
                name = desc[:60].strip()

            if not name:
                stats["skipped"] += 1
                continue

            # Clean description
            clean_desc = _clean_description(desc) if desc else name

            # Margin
            margin_pct = round((sell_price - cost) / sell_price * 100, 2) \
                if sell_price > 0 and cost > 0 else 0

            # Category + tokens
            category = auto_categorize(name, clean_desc)
            stats["categories"][category] = stats["categories"].get(category, 0) + 1
            search_tokens = _tokenize(f"{name} {clean_desc} {pn} {mfg}")

            # Strategy
            if margin_pct < 0:
                strategy = "loss_leader"
            elif margin_pct < 5:
                strategy = "margin_protect"
            elif margin_pct > 25:
                strategy = "premium"
            else:
                strategy = "competitive"

            # Source tag for tracking
            source_tag = "quotewerks"
            notes_parts = []
            if customer:
                notes_parts.append(f"customer={customer}")
            if quote_num:
                notes_parts.append(f"qw_quote={quote_num}")
            if date_str:
                notes_parts.append(f"date={date_str}")
            notes = "; ".join(notes_parts)

            # ── Upsert logic ──
            existing = conn.execute(
                "SELECT id, cost, sell_price, times_quoted FROM product_catalog WHERE name = ?",
                (name,)
            ).fetchone()

            if existing:
                pid = existing["id"]
                old_cost = existing["cost"] or 0
                old_sell = existing["sell_price"] or 0
                tq = existing["times_quoted"] or 0

                # Update if new data is better (has price where old didn't, or is newer)
                update_cost = cost if cost > 0 and (old_cost == 0 or replace) else old_cost
                update_sell = sell_price if sell_price > 0 and (old_sell == 0 or replace) else old_sell
                update_margin = round((update_sell - update_cost) / update_sell * 100, 2) \
                    if update_sell > 0 and update_cost > 0 else 0

                conn.execute("""
                    UPDATE product_catalog SET
                        cost = ?, sell_price = ?, margin_pct = ?,
                        best_cost = CASE WHEN ? > 0 AND (best_cost IS NULL OR ? < best_cost) THEN ? ELSE best_cost END,
                        description = COALESCE(NULLIF(?, ''), description),
                        category = COALESCE(NULLIF(?, ''), category),
                        uom = COALESCE(NULLIF(?, ''), uom),
                        manufacturer = COALESCE(NULLIF(?, ''), manufacturer),
                        mfg_number = COALESCE(NULLIF(?, ''), mfg_number),
                        search_tokens = ?,
                        times_quoted = ?,
                        notes = CASE WHEN notes IS NULL OR notes = '' THEN ? ELSE notes || '; ' || ? END,
                        updated_at = ?
                    WHERE id = ?
                """, (update_cost, update_sell, update_margin,
                      cost, cost, cost,
                      clean_desc, category, uom, mfg, pn,
                      search_tokens, tq + 1,
                      notes, notes, now, pid))

                # Price history
                if sell_price > 0:
                    conn.execute("""
                        INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                        VALUES (?, 'sell', ?, ?, ?)
                    """, (pid, sell_price, source_tag, now))
                if cost > 0:
                    conn.execute("""
                        INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                        VALUES (?, 'cost', ?, ?, ?)
                    """, (pid, cost, source_tag, now))

                stats["updated"] += 1

            else:
                # Also check by token overlap to prevent near-dupes
                desc_tokens = set(search_tokens.split())
                is_dupe = False
                if desc_tokens and len(desc_tokens) >= 2:
                    try:
                        search_terms = sorted(desc_tokens, key=len, reverse=True)[:3]
                        conditions = " AND ".join(["search_tokens LIKE ?" for _ in search_terms])
                        params = [f"%{t}%" for t in search_terms]
                        candidate = conn.execute(
                            f"SELECT id, search_tokens, cost, sell_price, times_quoted FROM product_catalog WHERE {conditions} LIMIT 1",
                            params
                        ).fetchone()
                        if candidate:
                            prod_tokens = set((candidate["search_tokens"] or "").split())
                            overlap = len(desc_tokens & prod_tokens) / max(len(desc_tokens | prod_tokens), 1)
                            if overlap >= 0.60:
                                # Update existing
                                pid = candidate["id"]
                                tq = candidate["times_quoted"] or 0
                                if cost > 0:
                                    conn.execute(
                                        "UPDATE product_catalog SET cost=?, best_cost=CASE WHEN ? > 0 AND (best_cost IS NULL OR ? < best_cost) THEN ? ELSE best_cost END, times_quoted=?, updated_at=? WHERE id=?",
                                        (cost, cost, cost, cost, tq + 1, now, pid))
                                if sell_price > 0:
                                    conn.execute(
                                        "UPDATE product_catalog SET sell_price=?, times_quoted=?, updated_at=? WHERE id=? AND (sell_price IS NULL OR sell_price=0)",
                                        (sell_price, tq + 1, now, pid))
                                stats["updated"] += 1
                                is_dupe = True
                    except Exception:
                        pass

                if not is_dupe:
                    # Insert new
                    cursor = conn.execute("""
                        INSERT INTO product_catalog (
                            name, sku, description, category, item_type, uom,
                            sell_price, cost, margin_pct, search_tokens,
                            price_strategy, manufacturer, mfg_number,
                            best_cost, best_supplier,
                            times_quoted, notes,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (name, pn, clean_desc, category, "Non-Inventory", uom,
                          sell_price, cost, margin_pct, search_tokens,
                          strategy, mfg, pn,
                          cost if cost > 0 else None, None,
                          1, notes, now, now))

                    pid = cursor.lastrowid
                    if sell_price > 0:
                        conn.execute("""
                            INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                            VALUES (?, 'sell', ?, ?, ?)
                        """, (pid, sell_price, source_tag, now))
                    if cost > 0:
                        conn.execute("""
                            INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                            VALUES (?, 'cost', ?, ?, ?)
                        """, (pid, cost, source_tag, now))
                    stats["imported"] += 1

        except Exception as e:
            stats["errors"].append(f"Row {stats['imported'] + stats['updated'] + stats['skipped']}: {e}")
            if len(stats["errors"]) > 50:
                stats["errors"].append("... (truncated)")
                break

    conn.commit()
    conn.close()

    log.info("QuoteWerks import: %d imported, %d updated, %d skipped from %d rows (%d errors)",
             stats["imported"], stats["updated"], stats["skipped"],
             stats["total_rows"], len(stats["errors"]))
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
    
    rows = conn.execute("""
        SELECT *, 
               CASE WHEN name LIKE ? THEN 3
                    WHEN sku LIKE ? THEN 2
                    ELSE 1 END as relevance
        FROM product_catalog
        WHERE """ + where + """
        ORDER BY relevance DESC, times_quoted DESC, sell_price DESC
        LIMIT ?
    """, [f"%{query}%", f"%{query}%"] + params + [limit]).fetchall()
    
    conn.close()
    return [dict(r) for r in rows]


def get_product(product_id: int) -> Optional[dict]:
    """Get single product with price history and suppliers."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM product_catalog WHERE id = ?", (product_id,)
    ).fetchone()
    if not row:
        conn.close()
        return None
    
    product = dict(row)
    
    # Get price history (with full context)
    history = conn.execute("""
        SELECT * FROM catalog_price_history
        WHERE product_id = ?
        ORDER BY recorded_at DESC LIMIT 50
    """, (product_id,)).fetchall()
    product["price_history"] = [dict(h) for h in history]
    
    # Get suppliers
    suppliers = conn.execute("""
        SELECT * FROM product_suppliers
        WHERE product_id = ?
        ORDER BY last_price ASC
    """, (product_id,)).fetchall()
    product["suppliers"] = [dict(s) for s in suppliers]
    
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
            try:
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
            except Exception as e:
                log.debug("Token match failed (search_tokens column may be missing): %s", e)

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

    # ── Enrich matches with best supplier URL from product_suppliers ──
    for m in matches:
        try:
            sup_row = conn.execute("""
                SELECT supplier_name, supplier_url, last_price, sku
                FROM product_suppliers 
                WHERE product_id = ? 
                ORDER BY last_price DESC LIMIT 1
            """, (m["id"],)).fetchone()
            if sup_row:
                m["best_supplier_name"] = sup_row[0] or ""
                m["best_supplier_url"] = sup_row[1] or ""
                m["best_supplier_price"] = sup_row[2] or 0
                m["best_supplier_sku"] = sup_row[3] or ""
        except Exception:
            pass

    conn.close()
    # ── Post-match verification: penalize false positives ──
    for m in matches:
        if m.get("match_confidence", 0) < 0.90:  # Skip exact part# matches
            penalty, reasons = _verify_match_quality(
                query_desc=description,
                catalog_desc=m.get("description", ""),
                catalog_name=m.get("name", ""),
                catalog_price=m.get("sell_price") or m.get("cost") or 0,
            )
            if penalty > 0:
                original = m["match_confidence"]
                m["match_confidence"] = round(max(original - penalty, 0.05), 2)
                m["match_reason"] += f" | VERIFIED: penalty={penalty:.2f} ({', '.join(reasons)})"
                m["verification_penalty"] = penalty
                m["verification_reasons"] = reasons
                if m["match_confidence"] < 0.40:
                    m["match_confidence"] = 0.0  # Below threshold, mark as non-match

    # Sort by confidence descending
    matches.sort(key=lambda x: x.get("match_confidence", 0), reverse=True)
    return matches[:top_n]


def match_items_batch(items: list) -> list:
    """
    Match multiple items at once (for PC detail page on load).
    items: [{idx, description, part_number}, ...]
    Returns: [{idx, matched, product_id, confidence, freshness, ...}, ...]
    """
    now = datetime.now(timezone.utc)
    results = []
    for item in items[:30]:
        desc = (item.get("description") or "").strip()
        part = (item.get("part_number") or "").strip()
        matches = match_item(desc, part, top_n=1)
        if matches and matches[0].get("match_confidence", 0) >= 0.40:
            best = matches[0]

            # Calculate freshness
            updated = best.get("updated_at") or ""
            days_old = 999
            if updated:
                try:
                    updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                    days_old = (now - updated_dt).days
                except (ValueError, TypeError):
                    pass

            if days_old <= 7:
                freshness = "fresh"
                freshness_icon = "🟢"
            elif days_old <= 14:
                freshness = "recent"
                freshness_icon = "🟡"
            elif days_old <= 30:
                freshness = "stale"
                freshness_icon = "🟠"
            else:
                freshness = "expired"
                freshness_icon = "🔴"

            results.append({
                "idx": item.get("idx", 0),
                "matched": True,
                "id": best["id"],
                "product_id": best["id"],
                "canonical_name": best.get("description") or best.get("name", ""),
                "part_number": best.get("mfg_number") or best.get("name", ""),
                "mfg_number": best.get("mfg_number", ""),
                "sku": best.get("sku", ""),
                "manufacturer": best.get("manufacturer", ""),
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
                "recommended_price": best.get("recommended_price"),
                "price_strategy": best.get("price_strategy", ""),
                "freshness": freshness,
                "freshness_icon": freshness_icon,
                "days_old": days_old,
                "last_checked": best.get("updated_at", ""),
                "scprs_last_price": best.get("scprs_last_price"),
                "competitor_low_price": best.get("competitor_low_price"),
                "web_lowest_price": best.get("web_lowest_price"),
                "photo_url": best.get("photo_url", ""),
                "verification_penalty": best.get("verification_penalty", 0),
                "verification_reasons": best.get("verification_reasons", []),
            })
        else:
            results.append({"idx": item.get("idx", 0), "matched": False})
    return results



# ═══════════════════════════════════════════════════════════════════════
# Catalog Match Audit — DB-wide verification of match quality
# ═══════════════════════════════════════════════════════════════════════

def audit_catalog_matches(fix: bool = False) -> dict:
    """
    Scan all price check items that have catalog matches.
    Re-verify each match with semantic checks.
    Returns report of bad matches found (and optionally clears them).
    """
    import json as _json

    pcs_path = os.path.join(DATA_DIR, "price_checks.json")
    if not os.path.exists(pcs_path):
        return {"ok": True, "message": "No price checks to audit", "bad_matches": 0}

    with open(pcs_path) as f:
        pcs = _json.load(f)

    bad_matches = []
    good_matches = 0
    total_checked = 0

    for pcid, pc in pcs.items():
        for i, item in enumerate(pc.get("items", [])):
            pricing = item.get("pricing", {})
            cat_match = pricing.get("catalog_match", "")
            cat_confidence = pricing.get("catalog_confidence", 0)

            if not cat_match or cat_confidence <= 0:
                continue

            total_checked += 1
            item_desc = item.get("description", "") or item.get("name", "")
            item_price = item.get("pricing", {}).get("our_price", 0) or 0

            # Re-verify the match
            penalty, reasons = _verify_match_quality(
                query_desc=item_desc,
                catalog_desc=cat_match,
                query_price=item_price,
            )

            new_confidence = round(max(cat_confidence - penalty, 0), 2)

            if penalty >= 0.3:
                bad_matches.append({
                    "pc_id": pcid,
                    "item_index": i,
                    "item_description": item_desc[:60],
                    "catalog_match": cat_match[:60],
                    "original_confidence": cat_confidence,
                    "new_confidence": new_confidence,
                    "penalty": penalty,
                    "reasons": reasons,
                })

                if fix and new_confidence < 0.40:
                    # Clear the bad match
                    pricing.pop("catalog_match", None)
                    pricing.pop("catalog_confidence", None)
                    pricing.pop("catalog_price", None)
            else:
                good_matches += 1

    if fix and bad_matches:
        with open(pcs_path, "w") as f:
            _json.dump(pcs, f, indent=2, default=str)

    return {
        "ok": True,
        "total_checked": total_checked,
        "good_matches": good_matches,
        "bad_matches": len(bad_matches),
        "fixed": fix,
        "details": bad_matches[:50],
    }


def audit_catalog_db() -> dict:
    """
    Audit the product_catalog table itself for data quality issues.
    Returns structured report of problems found.
    """
    init_catalog_db()
    conn = _get_conn()
    issues = []

    # Duplicate names
    dupes = conn.execute("""
        SELECT LOWER(TRIM(name)) as n, COUNT(*) as c, GROUP_CONCAT(id) as ids
        FROM product_catalog WHERE name IS NOT NULL AND TRIM(name) != ''
        GROUP BY LOWER(TRIM(name)) HAVING c > 1
        ORDER BY c DESC LIMIT 20
    """).fetchall()
    for d in dupes:
        issues.append({
            "type": "duplicate_name",
            "name": d["n"][:60],
            "count": d["c"],
            "ids": d["ids"],
        })

    # Products with no cost AND no sell price
    no_price = conn.execute("""
        SELECT COUNT(*) FROM product_catalog
        WHERE (cost IS NULL OR cost = 0) AND (sell_price IS NULL OR sell_price = 0)
    """).fetchone()[0]

    # Products with no description or very short
    no_desc = conn.execute("""
        SELECT COUNT(*) FROM product_catalog
        WHERE description IS NULL OR LENGTH(TRIM(description)) < 10
    """).fetchone()[0]

    # Products with no search tokens (can't be matched)
    no_tokens = conn.execute("""
        SELECT COUNT(*) FROM product_catalog
        WHERE search_tokens IS NULL OR TRIM(search_tokens) = ''
    """).fetchone()[0]

    # Stale products (not updated in 90+ days)
    stale = conn.execute("""
        SELECT COUNT(*) FROM product_catalog
        WHERE updated_at IS NOT NULL AND updated_at < date('now', '-90 days')
    """).fetchone()[0]

    total = conn.execute("SELECT COUNT(*) FROM product_catalog").fetchone()[0]
    conn.close()

    return {
        "ok": True,
        "total_products": total,
        "duplicates": len(dupes),
        "no_pricing": no_price,
        "no_description": no_desc,
        "no_search_tokens": no_tokens,
        "stale_90d": stale,
        "issues": issues,
    }


# ═══════════════════════════════════════════════════════════════════════
# AI Product Finder — Use Claude API to identify & source products
# ═══════════════════════════════════════════════════════════════════════

def ai_find_product(description: str, quantity: int = 1, agency: str = "") -> dict:
    """
    Use Claude API to identify the exact product, find supplier options,
    and recommend pricing. Called when no catalog match is found.

    Returns:
        {
            ok: bool,
            product: {name, manufacturer, mfg_number, description, category, uom},
            suppliers: [{name, price, url, notes}],
            recommended_cost: float,
            recommended_sell: float,
            reasoning: str,
        }
    """
    import requests as _req

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"ok": False, "error": "ANTHROPIC_API_KEY not set"}

    prompt = f"""You are a procurement research assistant for a government medical supply reseller.

Identify this product and find the best sourcing options:

ITEM DESCRIPTION: {description}
QUANTITY NEEDED: {quantity}
AGENCY: {agency or 'California state agency'}

Respond ONLY with a JSON object (no markdown, no backticks, no extra text):
{{
  "product": {{
    "name": "Full canonical product name",
    "manufacturer": "Manufacturer/brand name",
    "mfg_number": "Manufacturer part number if identifiable",
    "description": "Clear 1-line product description",
    "category": "medical|office|janitorial|food_service|clothing|safety|other",
    "uom": "EA|BX|PK|CS|SET|PR"
  }},
  "suppliers": [
    {{
      "name": "Supplier name (e.g., Amazon, McKesson, Medline, Henry Schein)",
      "estimated_price": 0.00,
      "url": "Product URL if known, otherwise empty string",
      "notes": "Why this supplier (e.g., best price, fastest shipping, GSA schedule)"
    }}
  ],
  "recommended_cost": 0.00,
  "recommended_sell": 0.00,
  "margin_pct": 25,
  "reasoning": "Brief explanation of product identification and pricing rationale",
  "confidence": 0.85
}}

IMPORTANT:
- Identify the EXACT product, not a similar one
- For scrubs/clothing: note if it's a SET vs individual pieces
- Include at least 2 supplier options with realistic prices
- recommended_sell should have ~25% margin over cost for government contracts
- If you can't identify the product with confidence, set confidence < 0.5"""

    try:
        resp = _req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )

        if resp.status_code != 200:
            return {"ok": False, "error": f"API returned {resp.status_code}"}

        data = resp.json()
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block["text"]

        # Parse JSON response
        import json as _json
        text = text.strip()
        # Strip markdown fences if present
        if text.startswith("```"):
            text = re.sub(r'^```\w*\n?', '', text)
            text = re.sub(r'\n?```$', '', text)
        result = _json.loads(text)
        result["ok"] = True
        result["source"] = "claude_api"
        return result

    except _req.exceptions.Timeout:
        return {"ok": False, "error": "Claude API timeout (30s)"}
    except _json.JSONDecodeError as e:
        return {"ok": False, "error": f"Failed to parse AI response: {e}", "raw": text[:500]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def ai_find_products_batch(items: list, agency: str = "") -> list:
    """
    Find products for multiple unmatched items.
    items: [{idx, description, quantity}, ...]
    Returns list of results with idx preserved.
    """
    results = []
    for item in items[:10]:  # Cap at 10 to control API costs
        desc = item.get("description", "")
        qty = item.get("quantity") or item.get("qty") or 1
        if not desc or len(desc) < 5:
            results.append({"idx": item.get("idx", 0), "ok": False, "error": "Description too short"})
            continue

        result = ai_find_product(desc, qty, agency)
        result["idx"] = item.get("idx", 0)
        results.append(result)

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


def record_catalog_quote(product_id: int, price_type: str, price: float,
                         quantity: float = 1, source: str = "pc_save",
                         agency: str = "", institution: str = "",
                         quote_number: str = "", pc_id: str = "",
                         supplier_url: str = ""):
    """Record a price event with full context for history tracking.
    
    price_type: 'sell', 'cost', 'quoted', 'won_bid'
    Stores qty for volume pricing analysis, agency/institution for quote history,
    and supplier_url for re-ordering.
    """
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("""
            INSERT INTO catalog_price_history 
            (product_id, price_type, price, quantity, source, agency, institution,
             quote_number, pc_id, supplier_url, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (product_id, price_type, price, quantity, source, agency, institution,
              quote_number, pc_id, supplier_url, now))
        conn.commit()
    except Exception as e:
        log.debug("record_catalog_quote error: %s", e)
    finally:
        conn.close()


def get_product_suppliers(product_id: int) -> list:
    """Get all known suppliers + prices for a product, cheapest first."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM product_suppliers WHERE product_id=? ORDER BY last_price ASC",
        (product_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_supplier_reliability(product_id: int, supplier_name: str,
                                 success: bool = True):
    """
    Update supplier reliability score based on lookup success/failure.
    reliability = running success rate (0.0 to 1.0)
    """
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    row = conn.execute(
        "SELECT reliability, notes FROM product_suppliers WHERE product_id=? AND supplier_name=?",
        (product_id, supplier_name)
    ).fetchone()
    if row:
        old_rel = row["reliability"] or 0.9
        # Exponential moving average — recent results weighted more
        new_rel = round(old_rel * 0.8 + (1.0 if success else 0.0) * 0.2, 3)
        # Track check count in notes
        old_notes = row["notes"] or ""
        import re as _re
        check_match = _re.search(r'checks:(\d+)', old_notes)
        checks = int(check_match.group(1)) + 1 if check_match else 1
        notes_update = _re.sub(r'checks:\d+', f'checks:{checks}', old_notes) if check_match else f"{old_notes} checks:{checks}".strip()
        conn.execute("""
            UPDATE product_suppliers SET reliability=?, notes=?, updated_at=?
            WHERE product_id=? AND supplier_name=?
        """, (new_rel, notes_update, now, product_id, supplier_name))
        conn.commit()
    conn.close()


def get_stale_products(max_age_days: int = 14, limit: int = 50) -> list:
    """
    Find products with stale pricing data (not updated recently).
    Used by the freshness monitor to know what needs re-checking.
    """
    conn = _get_conn()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
    rows = conn.execute("""
        SELECT id, name, description, cost, sell_price, best_supplier,
               updated_at, times_quoted, times_won
        FROM product_catalog
        WHERE cost > 0 AND (updated_at < ? OR updated_at IS NULL)
        AND times_quoted > 0
        ORDER BY times_quoted DESC
        LIMIT ?
    """, (cutoff, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_freshness_summary() -> dict:
    """Get catalog freshness overview for dashboard."""
    conn = _get_conn()
    now = datetime.now(timezone.utc)
    d7 = (now - timedelta(days=7)).isoformat()
    d14 = (now - timedelta(days=14)).isoformat()
    d30 = (now - timedelta(days=30)).isoformat()

    total = conn.execute("SELECT COUNT(*) FROM product_catalog WHERE cost > 0").fetchone()[0]
    fresh = conn.execute("SELECT COUNT(*) FROM product_catalog WHERE cost > 0 AND updated_at >= ?", (d7,)).fetchone()[0]
    stale = conn.execute("SELECT COUNT(*) FROM product_catalog WHERE cost > 0 AND updated_at < ? AND updated_at >= ?", (d14, d30)).fetchone()[0]
    old = conn.execute("SELECT COUNT(*) FROM product_catalog WHERE cost > 0 AND (updated_at < ? OR updated_at IS NULL)", (d30,)).fetchone()[0]

    # Supplier stats
    supplier_count = conn.execute("SELECT COUNT(DISTINCT supplier_name) FROM product_suppliers").fetchone()[0]
    avg_reliability = conn.execute("SELECT AVG(reliability) FROM product_suppliers WHERE reliability IS NOT NULL").fetchone()[0]

    conn.close()
    return {
        "total_priced": total,
        "fresh_7d": fresh,
        "stale_14d": stale,
        "old_30d": old,
        "supplier_count": supplier_count,
        "avg_supplier_reliability": round(avg_reliability or 0, 2),
    }


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
    conn.execute("UPDATE product_catalog SET " + sets + ", updated_at = ? WHERE id = ?", vals)
    
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


def add_to_catalog(description: str, part_number: str = "", cost: float = 0,
                   sell_price: float = 0, supplier_url: str = "", supplier_name: str = "",
                   uom: str = "EA", manufacturer: str = "", mfg_number: str = "",
                   photo_url: str = "", source: str = "price_check") -> Optional[int]:
    """
    Add a NEW product to the catalog from a Price Check line item.
    This is the critical growth mechanism — every sourced item enriches the catalog.

    Returns: new product_id or None if duplicate/error.
    """
    init_catalog_db()
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()

    if not description and not part_number:
        conn.close()
        return None

    name = part_number if part_number and len(part_number) >= 3 else ""
    if not name:
        # Use first 60 chars of description as name
        name = description[:60].strip()

    # Check for duplicate by name or by description similarity
    existing = conn.execute(
        "SELECT id FROM product_catalog WHERE name = ? LIMIT 1", (name,)
    ).fetchone()
    if existing:
        pid = existing["id"]
        # Still update cost + URL even for existing items
        try:
            if cost > 0:
                conn.execute("UPDATE product_catalog SET cost=?, best_cost=?, updated_at=? WHERE id=? AND (cost IS NULL OR cost=0)",
                            (cost, cost, now, pid))
            if sell_price > 0:
                conn.execute("UPDATE product_catalog SET sell_price=?, updated_at=? WHERE id=? AND (sell_price IS NULL OR sell_price=0)",
                            (sell_price, sell_price, now, pid))
            if supplier_name:
                conn.execute("UPDATE product_catalog SET best_supplier=?, updated_at=? WHERE id=? AND (best_supplier IS NULL OR best_supplier='')",
                            (supplier_name, now, pid))
            conn.execute("UPDATE product_catalog SET times_quoted=times_quoted+1, updated_at=? WHERE id=?", (now, pid))
            conn.commit()
        except Exception:
            pass
        conn.close()
        # Save supplier URL for existing products too
        if supplier_url:
            try:
                add_supplier_price(pid, supplier_name or "Web", cost if cost > 0 else 0, url=supplier_url)
            except Exception:
                pass
        return pid

    # Also check by description tokens to prevent near-duplicates
    desc_tokens = set(_tokenize(description).split())
    if desc_tokens:
        try:
            search_terms = sorted(desc_tokens, key=len, reverse=True)[:3]
            conditions = " AND ".join(["search_tokens LIKE ?" for _ in search_terms])
            params = [f"%{t}%" for t in search_terms]
            candidate = conn.execute(
                f"SELECT id, search_tokens FROM product_catalog WHERE {conditions} LIMIT 1",
                params
            ).fetchone()
            if candidate:
                # High token overlap → likely duplicate
                prod_tokens = set((candidate["search_tokens"] or "").split())
                overlap = len(desc_tokens & prod_tokens) / max(len(desc_tokens | prod_tokens), 1)
                if overlap >= 0.60:
                    pid = candidate["id"]
                    try:
                        if cost > 0:
                            conn.execute("UPDATE product_catalog SET cost=?, best_cost=?, updated_at=? WHERE id=? AND (cost IS NULL OR cost=0)",
                                        (cost, cost, now, pid))
                        if sell_price > 0:
                            conn.execute("UPDATE product_catalog SET sell_price=?, updated_at=? WHERE id=? AND (sell_price IS NULL OR sell_price=0)",
                                        (sell_price, sell_price, now, pid))
                        conn.execute("UPDATE product_catalog SET times_quoted=times_quoted+1, updated_at=? WHERE id=?", (now, pid))
                        conn.commit()
                    except Exception:
                        pass
                    conn.close()
                    if supplier_url:
                        try:
                            add_supplier_price(pid, supplier_name or "Web", cost if cost > 0 else 0, url=supplier_url)
                        except Exception:
                            pass
                    return pid
        except Exception as e:
            log.debug("Token dedup check failed: %s", e)

    # Calculate fields
    margin_pct = round((sell_price - cost) / sell_price * 100, 2) if sell_price > 0 and cost > 0 else 0
    category = auto_categorize(name, description)
    search_tokens = _tokenize(f"{name} {description} {mfg_number} {manufacturer}")

    if margin_pct < 0:
        strategy = "loss_leader"
    elif margin_pct < 5:
        strategy = "margin_protect"
    elif margin_pct > 25:
        strategy = "premium"
    else:
        strategy = "competitive"

    try:
        cursor = conn.execute("""
            INSERT INTO product_catalog (
                name, sku, description, category, item_type, uom,
                sell_price, cost, margin_pct, search_tokens,
                price_strategy, manufacturer, mfg_number, photo_url,
                best_cost, best_supplier,
                times_quoted, times_won, times_lost, win_rate,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, part_number, description, category, "Non-Inventory", uom,
              sell_price, cost, margin_pct, search_tokens,
              strategy, manufacturer, mfg_number, photo_url,
              cost if cost > 0 else None, supplier_name or None,
              1, 0, 0, 0,  # times_quoted=1, haven't won/lost yet
              now, now))

        pid = cursor.lastrowid

        # Record price history
        if sell_price > 0:
            conn.execute("""
                INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                VALUES (?, 'sell', ?, ?, ?)
            """, (pid, sell_price, source, now))
        if cost > 0:
            conn.execute("""
                INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                VALUES (?, 'cost', ?, ?, ?)
            """, (pid, cost, source, now))

        # Add supplier if URL provided (URL is valuable even without cost)
        _needs_supplier = bool(supplier_url)
        _sup_name = supplier_name or "Web"

        conn.commit()
        conn.close()

        if _needs_supplier:
            try:
                add_supplier_price(pid, _sup_name, cost, url=supplier_url)
            except Exception:
                pass  # Non-critical

        log.info("add_to_catalog: created product #%d '%s' from %s", pid, name[:40], source)
        return pid

    except Exception as e:
        log.error("add_to_catalog error: %s", e)
        try:
            conn.close()
        except Exception:
            pass
        return None


def save_pc_items_to_catalog(pc: dict) -> dict:
    """
    On PC save: add ALL line items to catalog that don't already exist.
    This is how the catalog grows organically from daily quoting work.

    Returns: {added: N, existing: N, skipped: N}
    """
    init_catalog_db()
    result = {"added": 0, "existing": 0, "skipped": 0}

    for item in pc.get("items", []):
        desc = (item.get("description") or "").strip()
        pn = str(item.get("item_number") or "").strip()
        if not desc and not pn:
            result["skipped"] += 1
            continue

        cost = item.get("vendor_cost") or item.get("pricing", {}).get("unit_cost") or 0
        price = item.get("unit_price") or item.get("pricing", {}).get("recommended_price") or 0
        link = item.get("item_link") or item.get("link") or ""

        # Check if already in catalog
        matches = match_item(desc, pn, top_n=1)
        if matches and matches[0].get("match_confidence", 0) >= 0.55:
            # Update existing product's quote count
            pid = matches[0]["id"]
            conn = _get_conn()
            conn.execute(
                "UPDATE product_catalog SET times_quoted = times_quoted + 1, updated_at = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), pid)
            )
            conn.commit()
            conn.close()
            result["existing"] += 1
        else:
            # NEW item → add to catalog
            _supplier = (item.get("item_supplier") or "").strip()
            _mfg = str(item.get("item_number") or "").strip() if pn else ""
            pid = add_to_catalog(
                description=desc, part_number=pn,
                cost=float(cost) if cost else 0,
                sell_price=float(price) if price else 0,
                supplier_url=link,
                supplier_name=_supplier,
                uom=(item.get("uom") or "EA"),
                mfg_number=_mfg,
                source="price_check"
            )
            if pid and _supplier and cost:
                add_supplier_price(pid, _supplier, float(cost), url=link)
            if pid:
                result["added"] += 1
            else:
                result["skipped"] += 1

    log.info("save_pc_items_to_catalog: added=%d existing=%d skipped=%d",
             result["added"], result["existing"], result["skipped"])
    return result


def record_outcome_to_catalog(pc: dict, outcome: str = "won",
                              competitor_name: str = "", competitor_price: float = 0):
    """
    When a Price Check is marked won or lost, feed results back to catalog.
    This is the critical feedback loop that makes pricing smarter over time.
    
    For UNMATCHED items: creates new catalog entries (so the catalog grows).
    outcome: "won" | "lost"
    """
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    updated = 0
    created = 0
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
            # NEW ITEM → add to catalog so we remember it next time
            unit_price = item.get("unit_price") or item.get("pricing", {}).get("recommended_price") or 0
            unit_cost = item.get("vendor_cost") or item.get("pricing", {}).get("unit_cost") or 0
            link = item.get("item_link") or item.get("link") or ""
            pid = add_to_catalog(
                description=desc, part_number=pn,
                cost=float(unit_cost) if unit_cost else 0,
                sell_price=float(unit_price) if unit_price else 0,
                supplier_url=link, source=f"pc_{outcome}"
            )
            if pid and outcome == "won":
                # Mark the new product as having 1 win
                conn.execute(
                    "UPDATE product_catalog SET times_won=1, win_rate=100, last_sold_price=?, last_sold_date=?, updated_at=? WHERE id=?",
                    (float(unit_price) if unit_price else 0, now, now, pid)
                )
            elif pid and outcome == "lost":
                conn.execute(
                    "UPDATE product_catalog SET times_lost=1, win_rate=0, updated_at=? WHERE id=?",
                    (now, pid)
                )
            if pid:
                created += 1
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
            conn.execute("UPDATE product_catalog SET " + sets + ", updated_at = ? WHERE id = ?", vals)
            updated += 1

    conn.commit()
    conn.close()
    log.info("record_outcome: %s → updated %d, created %d / %d items", outcome, updated, created, len(items))
    return {"outcome": outcome, "updated": updated, "created": created, "total": len(items)}


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


def optimize_portfolio(items: list, agency: str = "",
                       target_win_pct: float = 70.0) -> dict:
    """
    Portfolio Pricing Optimizer — optimizes the WHOLE quote, not per-item.
    
    Strategy: Loss-leader on commodities + profit-center on specialty items.
    Government buyers evaluate total quote price, so we can take thin margins
    on competitive items and make it up on niche items.
    
    Returns: {items: [...], total_cost, total_revenue, blended_margin,
              estimated_win_pct, vs_flat: {revenue, margin, win_pct}}
    """
    conn = _get_conn()
    portfolio = []
    total_cost = 0
    total_revenue = 0

    for item in items:
        desc = (item.get("description") or "").strip()
        pn = str(item.get("item_number") or "").strip()
        idx = item.get("idx", 0)
        qty = max(item.get("qty", 1), 1)
        cost = float(item.get("cost") or item.get("vendor_cost") or 0)

        if cost <= 0:
            portfolio.append({"idx": idx, "strategy": "skip", "reason": "no cost"})
            continue

        # Match to catalog
        matches = match_item(desc, pn, top_n=1)
        product = None
        if matches and matches[0].get("match_confidence", 0) >= 0.50:
            pid = matches[0]["id"]
            row = conn.execute("SELECT * FROM product_catalog WHERE id = ?", (pid,)).fetchone()
            if row:
                product = dict(row)

        # Classify item competitiveness
        scprs = product.get("scprs_last_price", 0) if product else 0
        comp_low = product.get("competitor_low_price", 0) if product else 0
        web_low = product.get("web_lowest_price", 0) if product else 0
        times_won = product.get("times_won", 0) if product else 0
        times_lost = product.get("times_lost", 0) if product else 0
        win_rate = product.get("win_rate", 0) if product else 0

        # Determine competition level
        price_refs = [p for p in [scprs, comp_low, web_low] if p and p > 0]
        if len(price_refs) >= 2:
            competition = "high"  # Multiple price references = competitive
        elif len(price_refs) == 1:
            competition = "medium"
        elif times_lost > times_won:
            competition = "high"  # We lose more than win = competitive
        else:
            competition = "low"  # No price refs, likely niche/specialty

        # Set strategy based on competition
        if competition == "high":
            # LOSS LEADER: Thin margin, win on price
            markup_pct = 0.10  # 10%
            strategy = "loss_leader"
            reason = "Competitive item — thin margin to win"
            if scprs and scprs > 0:
                price = min(round(cost * (1 + markup_pct), 2), round(scprs * 0.97, 2))
            else:
                price = round(cost * (1 + markup_pct), 2)
        elif competition == "low":
            # PROFIT CENTER: Maximize margin on niche items
            markup_pct = 0.45  # 45%
            strategy = "profit_center"
            reason = "Niche/specialty — maximize margin"
            price = round(cost * (1 + markup_pct), 2)
            if scprs and scprs > 0:
                price = min(price, round(scprs * 0.95, 2))
        else:
            # BALANCED: Standard markup
            markup_pct = 0.25  # 25%
            strategy = "balanced"
            reason = "Standard competitive positioning"
            price = round(cost * (1 + markup_pct), 2)
            if scprs and scprs > 0:
                price = min(price, round(scprs * 0.97, 2))

        # Ensure minimum profit
        min_profit = max(5, cost * 0.05)
        price = max(price, round(cost + min_profit, 2))

        ext_cost = round(cost * qty, 2)
        ext_price = round(price * qty, 2)
        margin = round((price - cost) / price * 100, 1) if price > 0 else 0

        total_cost += ext_cost
        total_revenue += ext_price

        portfolio.append({
            "idx": idx,
            "strategy": strategy,
            "reason": reason,
            "competition": competition,
            "cost": round(cost, 2),
            "price": round(price, 2),
            "margin_pct": margin,
            "qty": qty,
            "ext_cost": ext_cost,
            "ext_price": ext_price,
            "markup_pct": round((price / cost - 1) * 100, 1) if cost > 0 else 0,
            "scprs_ceiling": scprs,
            "catalog_matched": product is not None,
        })

    conn.close()

    blended_margin = round((total_revenue - total_cost) / total_revenue * 100, 1) if total_revenue > 0 else 0
    total_profit = round(total_revenue - total_cost, 2)

    # Compare vs flat 25% markup
    flat_revenue = round(total_cost * 1.25, 2)
    flat_margin = round((flat_revenue - total_cost) / flat_revenue * 100, 1) if flat_revenue > 0 else 0
    flat_profit = round(flat_revenue - total_cost, 2)

    return {
        "items": portfolio,
        "total_cost": round(total_cost, 2),
        "total_revenue": round(total_revenue, 2),
        "total_profit": total_profit,
        "blended_margin": blended_margin,
        "estimated_win_pct": min(95, max(30, target_win_pct + (blended_margin - 25) * -1.5)),
        "vs_flat": {
            "revenue": round(flat_revenue, 2),
            "profit": flat_profit,
            "margin": flat_margin,
            "profit_delta": round(total_profit - flat_profit, 2),
        },
        "strategy_summary": {
            "loss_leaders": sum(1 for p in portfolio if p.get("strategy") == "loss_leader"),
            "profit_centers": sum(1 for p in portfolio if p.get("strategy") == "profit_center"),
            "balanced": sum(1 for p in portfolio if p.get("strategy") == "balanced"),
        }
    }


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


# ═══════════════════════════════════════════════════════════════════════
# Sprint 1: Foundation Fixes
# ═══════════════════════════════════════════════════════════════════════

# Known brands for government procurement (lowercase → display name)
KNOWN_BRANDS = {
    "3m": "3M", "kimberly-clark": "Kimberly-Clark", "kimberly clark": "Kimberly-Clark",
    "medline": "Medline", "mckesson": "McKesson", "cardinal": "Cardinal Health",
    "cardinal health": "Cardinal Health", "bd": "BD (Becton Dickinson)",
    "becton": "BD (Becton Dickinson)", "stryker": "Stryker",
    "baxter": "Baxter", "bard": "Bard", "bardex": "Bard",
    "dynarex": "Dynarex", "duracell": "Duracell", "energizer": "Energizer",
    "xerox": "Xerox", "hp": "HP", "hewlett": "HP",
    "brother": "Brother", "canon": "Canon", "epson": "Epson",
    "geri-care": "Geri-Care", "gericare": "Geri-Care",
    "procare": "ProCare", "jobst": "JOBST", "colgate": "Colgate",
    "clorox": "Clorox", "purell": "Purell", "lysol": "Lysol",
    "rubbermaid": "Rubbermaid", "diversey": "Diversey",
    "grainger": "Grainger", "uline": "Uline",
    "kellogg": "Kellogg's", "kellogg's": "Kellogg's",
    "folgers": "Folgers", "starbucks": "Starbucks",
    "crayola": "Crayola", "sharpie": "Sharpie",
    "alimed": "AliMed", "deroyal": "DeRoyal",
    "covidien": "Covidien", "teleflex": "Teleflex",
    "hollister": "Hollister", "coloplast": "Coloplast",
    "convatec": "ConvaTec", "smith+nephew": "Smith & Nephew",
    "smith & nephew": "Smith & Nephew", "molnlycke": "Mölnlycke",
    "polymem": "PolyMem", "aquacel": "Aquacel",
    "donjon": "DonJoy", "donjoy": "DonJoy", "aircast": "Aircast",
    "tranquility": "Tranquility", "prevail": "Prevail",
    "confiderm": "Confiderm", "halyard": "Halyard",
    "ansell": "Ansell", "kimberly": "Kimberly-Clark",
    "argyle": "Argyle (Cardinal)", "airlife": "AirLife",
    "philips": "Philips", "zoll": "ZOLL", "physio-control": "Physio-Control",
    "defibtech": "Defibtech", "heartsine": "HeartSine",
    "welch allyn": "Welch Allyn", "welchallyn": "Welch Allyn",
    "sani-cloth": "Sani-Cloth", "pdi": "PDI",
    "biopatch": "Biopatch (Ethicon)", "ethicon": "Ethicon",
    "depuy": "DePuy", "johnson": "Johnson & Johnson",
    "mic": "MIC (Halyard)", "princeton": "Princeton",
    "general pencil": "General Pencil", "magic": "Magic",
}


def _extract_manufacturer(description: str, name: str = "") -> str:
    """Extract manufacturer/brand from product description."""
    if not description:
        return ""
    text = f"{description} {name}".lower()

    # Check ® and ™ marked brands first (most reliable)
    tm_match = re.findall(r'(\w[\w\-]*)[®™]', description)
    for brand in tm_match:
        key = brand.lower()
        if key in KNOWN_BRANDS:
            return KNOWN_BRANDS[key]
        if len(brand) > 2:
            return brand  # Unknown but marked as trademark

    # Check known brands dictionary
    for key, display in KNOWN_BRANDS.items():
        if key in text:
            return display

    # Check "by <Brand>" pattern
    by_match = re.search(r'\bby\s+([A-Z][A-Za-z\-&]+(?:\s+[A-Z][A-Za-z\-&]+)?)', description)
    if by_match:
        return by_match.group(1).strip()

    # Check "Brand Name" at start if first word is capitalized and not a generic noun
    words = description.split()
    if words and words[0][0].isupper():
        generic = {"the", "a", "an", "for", "with", "heavy", "light", "large",
                    "small", "medium", "standard", "premium", "disposable",
                    "sterile", "non", "single", "multi", "replacement",
                    "contains", "includes", "compatible", "universal", "assorted"}
        if words[0].lower().rstrip('®™,') not in generic:
            candidate = words[0].rstrip('®™,')
            if len(candidate) > 2 and not candidate.isdigit():
                # Only return if it looks like a brand (not a product word)
                product_words = {"glove", "mask", "gown", "tape", "wrap", "pad",
                                 "tube", "bag", "soap", "wipe", "razor", "cream",
                                 "foley", "catheter", "syringe", "needle", "blade",
                                 "paper", "pen", "pencil", "toner", "ink", "chair",
                                 "table", "shelf", "cart", "probe", "cable", "eyewear",
                                 "chocolate", "candy", "cookie", "cashews", "peanuts",
                                 "salted", "coffee", "sugar", "chips", "water", "juice",
                                 "battery", "batteries", "towel", "tissue", "liner",
                                 "plate", "cup", "fork", "spoon", "napkin", "straw",
                                 "contains", "sterile", "disposable", "assorted",
                                 "replacement", "compatible", "universal", "heavy",
                                 "light", "large", "small", "medium", "standard"}
                if candidate.lower() not in product_words:
                    return candidate

    return ""


def _make_product_name(description: str, qb_name: str = "") -> str:
    """
    Create a clean, descriptive product name from QB data.
    QB names are usually part numbers; the description is the real name.
    """
    if not description:
        return qb_name or "Unknown Product"

    # Remove leading part number if description starts with one followed by a separator
    # Only strip if it looks like a standalone part number (no lowercase letters)
    cleaned = re.sub(r'^[A-Z0-9]{3,20}[\-\.\/]?[A-Z0-9]*\s*[\n—:]\s*', '', description, flags=re.IGNORECASE)
    # Don't strip if the "part number" contains lowercase (it's probably a word)
    if cleaned == description or not cleaned.strip() or len(cleaned.strip()) < 10:
        cleaned = description

    # Take first meaningful sentence/line, cap at 120 chars
    name = cleaned.strip().split('\n')[0].strip()

    # Remove trailing packaging info after " — " if name is long enough
    if ' — ' in name and len(name.split(' — ')[0]) > 20:
        name = name.split(' — ')[0].strip()

    # Cap length
    if len(name) > 120:
        # Cut at last word boundary
        name = name[:120].rsplit(' ', 1)[0] + '…'

    return name.strip() or qb_name or "Unknown Product"


def fix_catalog_names() -> dict:
    """
    Fix 837+ products that have part-number names instead of descriptive names.
    Moves current name → mfg_number, description → name.
    """
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    fixed = 0
    mfg_found = 0
    brand_found = 0

    rows = conn.execute(
        "SELECT id, name, description, qb_name, mfg_number, manufacturer FROM product_catalog"
    ).fetchall()

    for row in rows:
        row = dict(row)
        old_name = row["name"] or ""
        desc = row["description"] or ""

        # Skip if name is already descriptive (>30 chars, has spaces)
        if len(old_name) > 30 and ' ' in old_name:
            continue

        # Check if current name looks like a part number
        if not re.match(r'^[A-Z0-9\-\.\/\s]{2,35}$', old_name, re.IGNORECASE):
            continue

        updates = {}

        # Move current name to mfg_number if not already set
        if old_name and not row.get("mfg_number"):
            updates["mfg_number"] = old_name
            mfg_found += 1

        # Set new descriptive name from description
        new_name = _make_product_name(desc, old_name)
        if new_name != old_name and len(new_name) > len(old_name):
            updates["name"] = new_name

        # Extract manufacturer if not set
        if not row.get("manufacturer"):
            mfg = _extract_manufacturer(desc, old_name)
            if mfg:
                updates["manufacturer"] = mfg
                brand_found += 1

        # Rebuild search tokens with better name
        if "name" in updates or "manufacturer" in updates:
            token_text = f"{updates.get('name', old_name)} {desc} {updates.get('manufacturer', '')} {old_name}"
            updates["search_tokens"] = _tokenize(token_text)

        if updates:
            updates["updated_at"] = now
            sets = ", ".join(f"{k} = ?" for k in updates)
            vals = list(updates.values()) + [row["id"]]
            try:
                conn.execute("UPDATE product_catalog SET " + sets + " WHERE id = ?", vals)
                fixed += 1
            except Exception as dup_err:
                # Name collision — append part number to make unique
                if "UNIQUE" in str(dup_err) and "name" in updates:
                    updates["name"] = f"{updates['name']} [{old_name}]"
                    updates["search_tokens"] = _tokenize(
                        f"{updates['name']} {desc} {updates.get('manufacturer', '')} {old_name}"
                    )
                    sets = ", ".join(f"{k} = ?" for k in updates)
                    vals = list(updates.values()) + [row["id"]]
                    try:
                        conn.execute("UPDATE product_catalog SET " + sets + " WHERE id = ?", vals)
                        fixed += 1
                    except Exception:
                        pass  # Skip truly problematic rows

    conn.commit()
    conn.close()
    log.info("fix_catalog_names: fixed %d names, %d mfg_numbers, %d brands", fixed, mfg_found, brand_found)
    return {"fixed": fixed, "mfg_found": mfg_found, "brand_found": brand_found, "total": len(rows)}


def extract_manufacturers_bulk() -> dict:
    """Extract manufacturer/brand for ALL products missing it."""
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    found = 0

    rows = conn.execute(
        "SELECT id, name, description, qb_name FROM product_catalog WHERE manufacturer IS NULL OR manufacturer = ''"
    ).fetchall()

    for row in rows:
        row = dict(row)
        desc = row.get("description") or ""
        name = row.get("name") or ""
        qb = row.get("qb_name") or ""
        mfg = _extract_manufacturer(desc, f"{name} {qb}")
        if mfg:
            conn.execute(
                "UPDATE product_catalog SET manufacturer = ?, updated_at = ? WHERE id = ?",
                (mfg, now, row["id"])
            )
            found += 1

    conn.commit()
    conn.close()
    log.info("extract_manufacturers_bulk: found %d/%d", found, len(rows))
    return {"found": found, "total": len(rows)}


def bulk_calculate_recommended(default_margin: float = 25.0) -> dict:
    """
    Calculate recommended_price for ALL products that have cost but no recommended_price.
    Uses per-item intelligence when available, falls back to margin-based pricing.
    """
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    priced = 0
    skipped = 0

    rows = conn.execute("""
        SELECT id, name, cost, sell_price, margin_pct, category,
               times_won, times_lost, win_rate, avg_margin_won,
               scprs_last_price, competitor_low_price, web_lowest_price,
               best_cost, price_strategy
        FROM product_catalog
        WHERE cost > 0
    """).fetchall()

    for row in rows:
        p = dict(row)
        pid = p["id"]
        cost = p["cost"]

        # Determine best cost basis
        effective_cost = cost
        if p.get("best_cost") and p["best_cost"] < cost:
            effective_cost = p["best_cost"]

        # Smart margin based on available data
        margin = default_margin

        # If we have win data, use the winning margin
        if p.get("avg_margin_won") and p["avg_margin_won"] > 0 and (p.get("times_won") or 0) >= 2:
            margin = p["avg_margin_won"]

        # If we have SCPRS ceiling, price just below it
        if p.get("scprs_last_price") and p["scprs_last_price"] > effective_cost:
            scprs_ceiling = p["scprs_last_price"]
            # Price 2% below SCPRS
            scprs_target = round(scprs_ceiling * 0.98, 2)
            if scprs_target > effective_cost * 1.05:  # Must have 5% min margin
                recommended = scprs_target
            else:
                recommended = round(effective_cost * (1 + margin / 100), 2)
        else:
            # Category-based margin adjustments
            cat = (p.get("category") or "").lower()
            if cat == "medical_equipment":
                margin = max(margin, 20)
            elif cat == "janitorial":
                margin = max(margin, 30)
            elif cat == "office_supplies":
                margin = min(margin, 18)  # More competitive category

            recommended = round(effective_cost * (1 + margin / 100), 2)

        # Ensure minimum profit floor
        min_profit = 25 if effective_cost > 100 else 10 if effective_cost > 20 else 5
        if recommended - effective_cost < min_profit:
            recommended = round(effective_cost + min_profit, 2)

        # Determine strategy
        if p.get("scprs_last_price"):
            strategy = "scprs_guided"
        elif (p.get("times_won") or 0) >= 2:
            strategy = "win_history"
        elif p.get("competitor_low_price"):
            strategy = "competitor_aware"
        else:
            strategy = "margin_default"

        actual_margin = round((recommended - effective_cost) / recommended * 100, 2) if recommended > 0 else 0

        conn.execute("""
            UPDATE product_catalog SET
                recommended_price = ?, price_strategy = ?,
                margin_pct = ?, updated_at = ?
            WHERE id = ?
        """, (recommended, strategy, actual_margin, now, pid))
        priced += 1

    conn.commit()
    conn.close()
    log.info("bulk_calculate_recommended: priced %d/%d products", priced, len(rows))
    return {"priced": priced, "total": len(rows), "skipped": skipped}


def get_freshness_report(items: list) -> list:
    """
    For a list of PC items, return freshness indicators for their catalog matches.

    Returns list of dicts with freshness_status, days_old, last_checked, etc.
    """
    conn = _get_conn()
    now = datetime.now(timezone.utc)
    results = []

    for item in items:
        desc = (item.get("description") or "").strip()
        pn = str(item.get("item_number") or item.get("part_number") or "").strip()

        if not desc and not pn:
            results.append({"idx": item.get("idx", 0), "matched": False})
            continue

        matches = match_item(desc, pn, top_n=1)
        if not matches or matches[0].get("match_confidence", 0) < 0.5:
            results.append({"idx": item.get("idx", 0), "matched": False})
            continue

        m = matches[0]
        pid = m["id"]

        # Get latest price history entry
        latest = conn.execute("""
            SELECT recorded_at, price, price_type, source
            FROM catalog_price_history
            WHERE product_id = ?
            ORDER BY recorded_at DESC LIMIT 1
        """, (pid,)).fetchone()

        product = conn.execute(
            "SELECT updated_at, recommended_price, cost, sell_price, times_won, win_rate FROM product_catalog WHERE id = ?",
            (pid,)
        ).fetchone()

        if not product:
            results.append({"idx": item.get("idx", 0), "matched": True, "product_id": pid})
            continue

        product = dict(product)
        updated = product.get("updated_at") or ""
        days_old = 0
        if updated:
            try:
                updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                days_old = (now - updated_dt).days
            except (ValueError, TypeError):
                days_old = 999

        # Freshness levels
        if days_old <= 7:
            status = "fresh"
            icon = "🟢"
        elif days_old <= 14:
            status = "recent"
            icon = "🟡"
        elif days_old <= 30:
            status = "stale"
            icon = "🟠"
        else:
            status = "expired"
            icon = "🔴"

        results.append({
            "idx": item.get("idx", 0),
            "matched": True,
            "product_id": pid,
            "product_name": m.get("name", ""),
            "confidence": m.get("match_confidence", 0),
            "freshness": status,
            "freshness_icon": icon,
            "days_old": days_old,
            "recommended_price": product.get("recommended_price"),
            "cost": product.get("cost"),
            "sell_price": product.get("sell_price"),
            "times_won": product.get("times_won", 0),
            "win_rate": product.get("win_rate", 0),
            "last_price_source": dict(latest).get("source", "") if latest else "",
            "last_price_date": dict(latest).get("recorded_at", "") if latest else "",
        })

    conn.close()
    return results


def reimport_qb_csv(csv_path: str) -> dict:
    """
    Re-import QB CSV with improved name handling.
    Uses description as product name, extracts manufacturer, sets mfg_number.
    Updates existing products, adds new ones.
    """
    init_catalog_db()
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    stats = {"imported": 0, "updated": 0, "skipped": 0, "errors": [], "categories": {}}

    with open(csv_path, 'r', encoding='utf-8-sig', errors='replace') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        try:
            qb_name = (row.get("Product/Service Name", "") or "").strip()
            if not qb_name:
                stats["skipped"] += 1
                continue

            raw_desc = (row.get("Sales Description", "") or "").strip()
            desc = _clean_description(raw_desc)
            if not desc:
                desc = qb_name

            # NEW: Use description as the product name, not the QB name
            product_name = _make_product_name(desc, qb_name)

            # Extract manufacturer
            manufacturer = _extract_manufacturer(desc, qb_name)

            # QB name is likely the mfg/part number
            mfg_number = qb_name if re.match(r'^[A-Z0-9\-\.\/]{2,35}$', qb_name, re.IGNORECASE) else ""

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

            if qb_name.lower() in ("test",) or sell_price >= 999999:
                stats["skipped"] += 1
                continue

            margin_pct = round((sell_price - cost) / sell_price * 100, 2) if sell_price > 0 and cost > 0 else 0
            category = auto_categorize(product_name, desc)
            stats["categories"][category] = stats["categories"].get(category, 0) + 1

            raw_sku = (row.get("SKU", "") or "").strip()
            uom = _parse_uom(raw_sku)
            sku = raw_sku if (len(raw_sku) > 4 and raw_sku.upper() not in UOM_MAP) else ""

            item_type = (row.get("Item type", "") or "Non-Inventory").strip()
            taxable = 1 if (row.get("Taxable", "") or "").lower() == "yes" else 0
            income_acct = (row.get("Income Account", "") or "").strip()
            expense_acct = (row.get("Expense Account", "") or "").strip()

            search_tokens = _tokenize(f"{product_name} {desc} {manufacturer} {qb_name}")

            # Check if exists by qb_name (most reliable key)
            existing = conn.execute(
                "SELECT id FROM product_catalog WHERE qb_name = ?", (qb_name,)
            ).fetchone()

            if not existing:
                # Also check by old-style name match
                existing = conn.execute(
                    "SELECT id FROM product_catalog WHERE name = ?", (qb_name,)
                ).fetchone()

            if not existing:
                # Also check by the new product name (handles deduped products)
                existing = conn.execute(
                    "SELECT id FROM product_catalog WHERE name = ?", (product_name,)
                ).fetchone()

            if existing:
                conn.execute("""
                    UPDATE product_catalog SET
                        name = ?, description = ?, sell_price = ?, cost = ?,
                        margin_pct = ?, category = ?, manufacturer = COALESCE(NULLIF(?, ''), manufacturer),
                        mfg_number = COALESCE(NULLIF(?, ''), mfg_number),
                        sku = COALESCE(NULLIF(?, ''), sku),
                        uom = COALESCE(NULLIF(?, ''), uom),
                        search_tokens = ?, qb_name = ?, qb_item_type = ?,
                        qb_income_account = ?, qb_expense_account = ?,
                        taxable = ?, updated_at = ?
                    WHERE id = ?
                """, (product_name, desc, sell_price, cost, margin_pct, category,
                      manufacturer, mfg_number, sku, uom, search_tokens,
                      qb_name, item_type, income_acct, expense_acct, taxable, now,
                      existing["id"]))

                # Price history
                conn.execute("""
                    INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                    VALUES (?, 'sell', ?, 'quickbooks_update', ?)
                """, (existing["id"], sell_price, now))
                if cost > 0:
                    conn.execute("""
                        INSERT INTO catalog_price_history (product_id, price_type, price, source, recorded_at)
                        VALUES (?, 'cost', ?, 'quickbooks_update', ?)
                    """, (existing["id"], cost, now))

                stats["updated"] += 1
            else:
                try:
                    cursor = conn.execute("""
                        INSERT INTO product_catalog (
                            name, sku, description, category, item_type, uom,
                            sell_price, cost, margin_pct, search_tokens,
                            manufacturer, mfg_number,
                            qb_name, qb_item_type, qb_income_account, qb_expense_account,
                            taxable, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (product_name, sku, desc, category, item_type, uom,
                          sell_price, cost, margin_pct, search_tokens,
                          manufacturer, mfg_number,
                          qb_name, item_type, income_acct, expense_acct,
                          taxable, now, now))
                except Exception as _uniq_err:
                    if "UNIQUE" in str(_uniq_err):
                        # Name collision — append part number to differentiate
                        unique_name = f"{product_name} [{qb_name}]"
                        search_tokens = _tokenize(f"{unique_name} {desc} {manufacturer} {qb_name}")
                        cursor = conn.execute("""
                            INSERT INTO product_catalog (
                                name, sku, description, category, item_type, uom,
                                sell_price, cost, margin_pct, search_tokens,
                                manufacturer, mfg_number,
                                qb_name, qb_item_type, qb_income_account, qb_expense_account,
                                taxable, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (unique_name, sku, desc, category, item_type, uom,
                              sell_price, cost, margin_pct, search_tokens,
                              manufacturer, mfg_number,
                              qb_name, item_type, income_acct, expense_acct,
                              taxable, now, now))
                    else:
                        raise

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
            if "UNIQUE" in str(e):
                stats["skipped"] += 1  # Variant already exists via dedup merge
            else:
                stats["errors"].append(f"{qb_name}: {e}")

    conn.commit()
    conn.close()

    log.info("reimport_qb: %d imported, %d updated, %d skipped, %d errors",
             stats["imported"], stats["updated"], stats["skipped"], len(stats["errors"]))
    return stats


def run_sprint1_fixes() -> dict:
    """
    Run all Sprint 1 foundation fixes in order:
    1. Fix product names (part numbers → descriptive names)
    2. Extract manufacturers from descriptions
    3. Bulk calculate recommended prices
    Returns combined stats.
    """
    init_catalog_db()

    log.info("Sprint 1: Starting foundation fixes...")

    # 1. Fix names
    name_stats = fix_catalog_names()
    log.info("Sprint 1 [1/3]: Fixed %d product names", name_stats["fixed"])

    # 2. Extract manufacturers
    mfg_stats = extract_manufacturers_bulk()
    log.info("Sprint 1 [2/3]: Found %d manufacturers", mfg_stats["found"])

    # 3. Bulk pricing
    price_stats = bulk_calculate_recommended()
    log.info("Sprint 1 [3/3]: Calculated prices for %d products", price_stats["priced"])

    return {
        "names_fixed": name_stats["fixed"],
        "mfg_numbers_set": name_stats["mfg_found"],
        "brands_found": name_stats["brand_found"] + mfg_stats["found"],
        "prices_calculated": price_stats["priced"],
        "total_products": name_stats["total"],
    }


def dedup_catalog(dry_run: bool = False) -> dict:
    """
    Find and merge true duplicate products in the catalog.
    True dupe = same first-60-char description AND same sell_price.
    
    Merge strategy: keep the one with the most data (cost, win history),
    add other mfg_numbers as aliases in the notes field.
    """
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    
    rows = conn.execute("""
        SELECT id, name, description, mfg_number, qb_name, sell_price, cost,
               times_won, times_lost, recommended_price, manufacturer, best_cost
        FROM product_catalog
        ORDER BY id
    """).fetchall()
    
    from collections import defaultdict
    groups = defaultdict(list)
    for r in rows:
        d = dict(r)
        desc60 = (d["description"] or "")[:60].lower().strip()
        price = round(d["sell_price"] or 0, 2)
        if desc60:
            key = f"{desc60}|{price}"
            groups[key].append(d)
    
    dupes = {k: v for k, v in groups.items() if len(v) > 1}
    merged = 0
    deleted_ids = []
    merge_log = []
    
    for key, items in dupes.items():
        # Pick the "best" product to keep:
        # 1. Most win data, 2. Has recommended_price, 3. Has cost, 4. Lowest ID (oldest)
        items.sort(key=lambda x: (
            -(x.get("times_won") or 0),
            -(1 if x.get("recommended_price") else 0),
            -(1 if x.get("cost") and x["cost"] > 0 else 0),
            x["id"]
        ))
        
        keep = items[0]
        remove = items[1:]
        
        # Collect all mfg_numbers as aliases
        all_mfgs = set()
        all_qb_names = set()
        best_cost = keep.get("cost") or 0
        best_mfg = keep.get("manufacturer") or ""
        
        for item in items:
            if item.get("mfg_number"):
                all_mfgs.add(item["mfg_number"])
            if item.get("qb_name"):
                all_qb_names.add(item["qb_name"])
            if item.get("cost") and item["cost"] > 0:
                if item["cost"] < best_cost or best_cost == 0:
                    best_cost = item["cost"]
            if item.get("manufacturer") and not best_mfg:
                best_mfg = item["manufacturer"]
        
        # Build aliases note
        alias_note = f"Aliases: {', '.join(sorted(all_mfgs))}" if len(all_mfgs) > 1 else ""
        
        remove_ids = [r["id"] for r in remove]
        
        merge_log.append({
            "keep_id": keep["id"],
            "keep_name": keep["name"][:50],
            "remove_ids": remove_ids,
            "aliases": list(all_mfgs),
        })
        
        if not dry_run:
            # Update the keeper with combined data
            existing_notes = keep.get("notes") or ""
            new_notes = f"{existing_notes}\n{alias_note}".strip() if alias_note else existing_notes
            
            # Rebuild search tokens with all aliases
            token_text = f"{keep['name']} {keep.get('description','')} {' '.join(all_mfgs)} {best_mfg}"
            tokens = _tokenize(token_text)
            
            conn.execute("""
                UPDATE product_catalog SET
                    notes = ?, search_tokens = ?,
                    cost = CASE WHEN ? > 0 AND (cost IS NULL OR cost = 0 OR ? < cost) THEN ? ELSE cost END,
                    manufacturer = COALESCE(NULLIF(?, ''), manufacturer),
                    updated_at = ?
                WHERE id = ?
            """, (new_notes, tokens, best_cost, best_cost, best_cost, best_mfg, now, keep["id"]))
            
            # Move price history from removed items to keeper
            for rid in remove_ids:
                conn.execute(
                    "UPDATE catalog_price_history SET product_id = ? WHERE product_id = ?",
                    (keep["id"], rid)
                )
            
            # Delete duplicates
            conn.execute(
                f"DELETE FROM product_catalog WHERE id IN ({','.join('?' * len(remove_ids))})",
                remove_ids
            )
            
        deleted_ids.extend(remove_ids)
        merged += 1
    
    if not dry_run:
        conn.commit()
    conn.close()
    
    remaining = len(rows) - len(deleted_ids)
    log.info("dedup_catalog: merged %d groups, deleted %d products, %d remaining",
             merged, len(deleted_ids), remaining)
    
    return {
        "groups_merged": merged,
        "products_deleted": len(deleted_ids),
        "products_remaining": remaining,
        "merge_log": merge_log,
    }
