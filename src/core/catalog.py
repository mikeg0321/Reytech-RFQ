"""
Product Catalog — SKU database for Reytech
F31-01: Stores known products with vendor sourcing, typical cost, and category.
Grows automatically as new items are quoted.
"""
import sqlite3, json, os, logging, re
from datetime import datetime
from src.core.db import get_db

log = logging.getLogger("reytech")

CATALOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT,
    sku         TEXT UNIQUE,
    name        TEXT NOT NULL,
    description TEXT,
    category    TEXT,
    unit        TEXT DEFAULT 'each',
    typical_cost REAL DEFAULT 0,
    list_price   REAL DEFAULT 0,
    vendor_key  TEXT,
    manufacturer TEXT,
    part_number  TEXT,
    tags        TEXT DEFAULT '[]',
    source      TEXT DEFAULT 'manual',
    notes       TEXT
);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
CREATE INDEX IF NOT EXISTS idx_products_name ON products(name);
"""

P0_SKUS = [
    {"sku":"NIT-EXAM-XS","name":"Nitrile Exam Gloves, X-Small, Box/100","category":"Medical/PPE","unit":"box","typical_cost":7.50,"list_price":13.99,"vendor_key":"integrated_medical","manufacturer":"Medline","tags":["gloves","nitrile","medical","ppe"]},
    {"sku":"NIT-EXAM-SM","name":"Nitrile Exam Gloves, Small, Box/100","category":"Medical/PPE","unit":"box","typical_cost":7.50,"list_price":13.99,"vendor_key":"integrated_medical","manufacturer":"Medline","tags":["gloves","nitrile","medical","ppe"]},
    {"sku":"NIT-EXAM-MD","name":"Nitrile Exam Gloves, Medium, Box/100","category":"Medical/PPE","unit":"box","typical_cost":7.50,"list_price":12.99,"vendor_key":"integrated_medical","manufacturer":"Medline","tags":["gloves","nitrile","medical","ppe"]},
    {"sku":"NIT-EXAM-LG","name":"Nitrile Exam Gloves, Large, Box/100","category":"Medical/PPE","unit":"box","typical_cost":7.50,"list_price":13.99,"vendor_key":"integrated_medical","manufacturer":"Medline","tags":["gloves","nitrile","medical","ppe"]},
    {"sku":"NIT-EXAM-XL","name":"Nitrile Exam Gloves, X-Large, Box/100","category":"Medical/PPE","unit":"box","typical_cost":7.75,"list_price":14.49,"vendor_key":"integrated_medical","manufacturer":"Medline","tags":["gloves","nitrile","medical","ppe"]},
    {"sku":"NIT-BLK-MD","name":"Black Nitrile Gloves, Medium, Box/100 (Tactical)","category":"Medical/PPE","unit":"box","typical_cost":8.50,"list_price":15.99,"vendor_key":"integrated_medical","manufacturer":"GloveNation","tags":["gloves","nitrile","black","law enforcement","tactical","ppe"]},
    {"sku":"NIT-BLK-LG","name":"Black Nitrile Gloves, Large, Box/100 (Tactical)","category":"Medical/PPE","unit":"box","typical_cost":8.50,"list_price":15.99,"vendor_key":"integrated_medical","manufacturer":"GloveNation","tags":["gloves","nitrile","black","law enforcement","tactical","ppe"]},
    {"sku":"CHUX-23X36","name":"Disposable Underpads, 23x36 in, Case/100","category":"Medical/Incontinence","unit":"case","typical_cost":18.00,"list_price":28.99,"vendor_key":"cardinal_health","manufacturer":"Cardinal Health","tags":["chux","underpads","incontinence","medical","cchcs","calvet"]},
    {"sku":"CHUX-30X36","name":"Disposable Underpads, 30x36 in, Case/90","category":"Medical/Incontinence","unit":"case","typical_cost":24.00,"list_price":36.99,"vendor_key":"cardinal_health","manufacturer":"Cardinal Health","tags":["chux","underpads","incontinence","medical","cchcs","calvet"]},
    {"sku":"BRIEF-SM","name":"Adult Incontinence Briefs, Small, Case/80","category":"Medical/Incontinence","unit":"case","typical_cost":19.00,"list_price":29.99,"vendor_key":"cardinal_health","manufacturer":"Cardinal Health","tags":["briefs","incontinence","adult","medical","cchcs","calvet"]},
    {"sku":"BRIEF-MD","name":"Adult Incontinence Briefs, Medium, Case/80","category":"Medical/Incontinence","unit":"case","typical_cost":19.00,"list_price":29.99,"vendor_key":"cardinal_health","manufacturer":"Cardinal Health","tags":["briefs","incontinence","adult","medical","cchcs","calvet"]},
    {"sku":"BRIEF-LG","name":"Adult Incontinence Briefs, Large, Case/80","category":"Medical/Incontinence","unit":"case","typical_cost":20.00,"list_price":31.99,"vendor_key":"cardinal_health","manufacturer":"Cardinal Health","tags":["briefs","incontinence","adult","medical","cchcs","calvet"]},
    {"sku":"N95-3M-8210","name":"3M N95 Respirator 8210, Box/20 (NIOSH Approved)","category":"PPE/Respiratory","unit":"box","typical_cost":14.00,"list_price":22.99,"vendor_key":"grainger","manufacturer":"3M","part_number":"8210","tags":["n95","respirator","ppe","calfire","cdph","cchcs","safety"]},
    {"sku":"N95-3M-1860","name":"3M N95 Respirator 1860 Healthcare, Box/20","category":"PPE/Respiratory","unit":"box","typical_cost":18.00,"list_price":28.99,"vendor_key":"grainger","manufacturer":"3M","part_number":"1860","tags":["n95","respirator","healthcare","ppe","cdph","cchcs"]},
    {"sku":"HIVIZ-CL2-M","name":"Hi-Vis Safety Vest, ANSI Class 2, Medium","category":"Safety/PPE","unit":"each","typical_cost":8.50,"list_price":15.99,"vendor_key":"grainger","manufacturer":"ML Kishigo","tags":["safety","hi-vis","vest","ansi","caltrans","calfire","ppe"]},
    {"sku":"HIVIZ-CL2-L","name":"Hi-Vis Safety Vest, ANSI Class 2, Large","category":"Safety/PPE","unit":"each","typical_cost":8.50,"list_price":15.99,"vendor_key":"grainger","manufacturer":"ML Kishigo","tags":["safety","hi-vis","vest","ansi","caltrans","calfire","ppe"]},
    {"sku":"HIVIZ-CL2-XL","name":"Hi-Vis Safety Vest, ANSI Class 2, X-Large","category":"Safety/PPE","unit":"each","typical_cost":9.00,"list_price":16.99,"vendor_key":"grainger","manufacturer":"ML Kishigo","tags":["safety","hi-vis","vest","ansi","caltrans","calfire","ppe"]},
    {"sku":"FAK-ANSI-B","name":"First Aid Kit, ANSI Class B, 150-Person Vehicle/Field","category":"Safety/First Aid","unit":"each","typical_cost":28.00,"list_price":44.99,"vendor_key":"grainger","manufacturer":"Honeywell","tags":["first aid","kit","ansi","vehicle","chp","calfire","safety"]},
    {"sku":"FAK-ANSI-A","name":"First Aid Kit, ANSI Class A, 50-Person Office","category":"Safety/First Aid","unit":"each","typical_cost":18.00,"list_price":29.99,"vendor_key":"grainger","manufacturer":"Honeywell","tags":["first aid","kit","ansi","office","safety"]},
    {"sku":"CAT-TOURNIQUET","name":"Combat Application Tourniquet (CAT), Gen 7","category":"Safety/Trauma","unit":"each","typical_cost":28.00,"list_price":38.99,"vendor_key":"north_american_rescue","manufacturer":"North American Rescue","tags":["tourniquet","cat","bleed control","trauma","chp","calfire","stop the bleed"]},
    {"sku":"SANIT-8OZ","name":"Hand Sanitizer, 8oz Pump Bottle, 75% Alcohol","category":"Medical/PPE","unit":"each","typical_cost":3.50,"list_price":6.99,"vendor_key":"integrated_medical","manufacturer":"Purell","tags":["sanitizer","hand","ppe","medical"]},
    {"sku":"SANIT-GAL","name":"Hand Sanitizer, 1 Gallon Jug, 70% Alcohol","category":"Medical/PPE","unit":"gallon","typical_cost":12.00,"list_price":19.99,"vendor_key":"integrated_medical","manufacturer":"GOJO","tags":["sanitizer","hand","gallon","ppe","medical"]},
    {"sku":"GAUZE-4X4","name":"Gauze Pads, 4x4 in, Non-Sterile, Box/200","category":"Medical/Wound Care","unit":"box","typical_cost":6.00,"list_price":10.99,"vendor_key":"mckesson","manufacturer":"McKesson","tags":["gauze","wound care","medical","cchcs","calvet"]},
    {"sku":"ABD-8X10","name":"ABD Pads, 8x10 in, Sterile, Box/36","category":"Medical/Wound Care","unit":"box","typical_cost":9.00,"list_price":15.99,"vendor_key":"mckesson","manufacturer":"McKesson","tags":["abd","wound care","dressing","medical","cchcs"]},
    {"sku":"SHARPS-1QT","name":"Sharps Container, 1 Quart, Red Lid","category":"Medical/Sharps","unit":"each","typical_cost":2.50,"list_price":4.99,"vendor_key":"mckesson","manufacturer":"BD","tags":["sharps","container","biohazard","medical","cchcs","cdph"]},
    {"sku":"SHARPS-5QT","name":"Sharps Container, 5 Quart, Red Lid","category":"Medical/Sharps","unit":"each","typical_cost":5.50,"list_price":9.99,"vendor_key":"mckesson","manufacturer":"BD","tags":["sharps","container","biohazard","medical","cchcs","cdph"]},
    {"sku":"STRYKER-RESTRAINT-STD","name":"Stryker Patient Restraint Package, Standard","category":"Medical/Patient Care","unit":"set","typical_cost":45.00,"list_price":69.99,"vendor_key":"curbell_medical","manufacturer":"Stryker","tags":["restraint","patient","stryker","medical","cchcs","dsh"]},
]


def init_catalog():
    with get_db() as conn:
        conn.executescript(CATALOG_SCHEMA)
        count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        if count == 0:
            now = datetime.now().isoformat()
            loaded = 0
            for item in P0_SKUS:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO products
                          (created_at, updated_at, sku, name, description, category, unit,
                           typical_cost, list_price, vendor_key, manufacturer, part_number, tags, source)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        now, now,
                        item.get("sku"), item["name"], item.get("description",""),
                        item.get("category","General"), item.get("unit","each"),
                        item.get("typical_cost",0), item.get("list_price",0),
                        item.get("vendor_key",""), item.get("manufacturer",""),
                        item.get("part_number",""),
                        json.dumps(item.get("tags",[])),
                        "seed_p0"
                    ))
                    loaded += 1
                except Exception as e:
                    log.warning("catalog seed: %s -- %s", item.get("sku"), e)
            log.info("Catalog initialized: %d P0 SKUs loaded", loaded)
        return count or len(P0_SKUS)


def search_catalog(query: str, limit: int = 10) -> list:
    q = f"%{query.lower()}%"
    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, sku, name, category, unit, typical_cost, list_price,
                   vendor_key, manufacturer, part_number, tags, notes
            FROM products
            WHERE lower(name) LIKE ?
               OR lower(description) LIKE ?
               OR lower(tags) LIKE ?
               OR lower(manufacturer) LIKE ?
               OR lower(sku) LIKE ?
               OR lower(category) LIKE ?
            ORDER BY
                CASE WHEN lower(name) LIKE ? THEN 0 ELSE 1 END,
                list_price ASC
            LIMIT ?
        """, (q, q, q, q, q, q, q, limit)).fetchall()
        cols = ["id","sku","name","category","unit","typical_cost","list_price",
                "vendor_key","manufacturer","part_number","tags","notes"]
        results = []
        for row in rows:
            item = dict(zip(cols, row))
            try: item["tags"] = json.loads(item["tags"] or "[]")
            except: item["tags"] = []
            results.append(item)
        return results


def get_catalog(category=None, limit=200):
    with get_db() as conn:
        if category:
            rows = conn.execute(
                "SELECT * FROM products WHERE lower(category) LIKE ? ORDER BY category, name LIMIT ?",
                (f"%{category.lower()}%", limit)
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM products ORDER BY category, name LIMIT ?", (limit,)).fetchall()
        cols = [d[0] for d in conn.execute("PRAGMA table_info(products)").fetchall()]
        results = []
        for row in rows:
            item = dict(zip(cols, row))
            try: item["tags"] = json.loads(item["tags"] or "[]")
            except: item["tags"] = []
            results.append(item)
        return results


def get_categories():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT category, COUNT(*) as cnt FROM products GROUP BY category ORDER BY cnt DESC"
        ).fetchall()
        return [{"category": r[0], "count": r[1]} for r in rows]


def auto_ingest_item(description, unit_price=0, vendor_key="", manufacturer="", source="quote"):
    if not description or len(description.strip()) < 4:
        return {"added": False, "reason": "too_short"}
    name = description.strip()
    sku_candidate = re.sub(r"[^A-Z0-9]", "-", name.upper())[:30].strip("-")
    existing = search_catalog(name[:30], limit=3)
    if existing:
        return {"added": False, "reason": "exists", "matched": existing[0]["sku"]}
    nl = name.lower()
    if any(k in nl for k in ["glove","nitrile","latex","vinyl"]): cat = "Medical/PPE"
    elif any(k in nl for k in ["sanitizer","soap","hand wash"]): cat = "Medical/PPE"
    elif any(k in nl for k in ["n95","respirator","mask","kn95"]): cat = "PPE/Respiratory"
    elif any(k in nl for k in ["hi-vis","vest","ansi","high visibility"]): cat = "Safety/PPE"
    elif any(k in nl for k in ["first aid","kit","bandage","gauze"]): cat = "Safety/First Aid"
    elif any(k in nl for k in ["tourniquet","bleed","trauma","ifak"]): cat = "Safety/Trauma"
    elif any(k in nl for k in ["brief","diaper","incontinence","chux","underpad"]): cat = "Medical/Incontinence"
    elif any(k in nl for k in ["sharps","container","biohazard"]): cat = "Medical/Sharps"
    elif any(k in nl for k in ["trash","bag","liner","janitorial"]): cat = "Janitorial"
    elif any(k in nl for k in ["paper","towel","toilet","tissue"]): cat = "Janitorial"
    elif any(k in nl for k in ["office","pen","binder","folder","staple"]): cat = "Office Supplies"
    else: cat = "General"
    try:
        now = datetime.now().isoformat()
        with get_db() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO products
                  (created_at, updated_at, sku, name, category, unit, typical_cost,
                   list_price, vendor_key, manufacturer, tags, source)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (now, now, sku_candidate, name, cat, "each",
                  round(unit_price * 0.75, 2), unit_price,
                  vendor_key, manufacturer, "[]", source))
        return {"added": True, "sku": sku_candidate, "name": name, "category": cat}
    except Exception as e:
        return {"added": False, "reason": str(e)}


def get_catalog_stats():
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        cats = conn.execute("SELECT COUNT(DISTINCT category) FROM products").fetchone()[0]
        p0 = conn.execute("SELECT COUNT(*) FROM products WHERE source='seed_p0'").fetchone()[0]
        return {"total_skus": total, "categories": cats, "p0_skus_loaded": p0}
