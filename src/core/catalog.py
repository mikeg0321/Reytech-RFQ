"""Legacy shim — the real catalog lives in `src.agents.product_catalog`.

CP-5: this module previously maintained its own `products` table with 27
synthetic P0 SKUs (see `P0_SKUS` history in git). Callers treated those
fabricated rows as real catalog pricing, which poisoned every pricing
flow that went through `search_catalog` (bid_decision_agent, RFQ pricing
ingest, catalog UI endpoints, hit-rate analytics).

The real catalog — `product_catalog` table populated from actual quotes
and supplier feeds — lives in `src.agents.product_catalog` and contains
the 4500+ LOC search/pricing engine. This module now delegates every
public call to that engine and re-maps result dicts into the legacy
`search_catalog` shape so existing callers keep working without a
shotgun refactor.

`init_catalog()` is a no-op. The synthetic P0 seed is gone. The
`products` table is intentionally NOT dropped (existing production rows
live there); we simply stop writing into it.
"""
import json
import logging
import re
from typing import Optional

log = logging.getLogger("reytech")

# CP-5: the synthetic P0_SKUS seed list has been removed. The real
# catalog is driven by product_catalog, seeded from historical quotes.


def _row_to_legacy(row: dict) -> dict:
    """Map a product_catalog row into the shape the legacy callers expect.

    Legacy shape (src.core.catalog.search_catalog):
        id, sku, name, category, unit, typical_cost, list_price,
        vendor_key, manufacturer, part_number, tags, notes

    product_catalog shape (src.agents.product_catalog.search_products):
        id, sku, name, description, category, manufacturer, mfg_number,
        cost, sell_price, margin_pct, times_quoted, tags, ...
    """
    tags = row.get("tags") or []
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except Exception:
            tags = [t.strip() for t in tags.split(",") if t.strip()]
    return {
        "id": row.get("id"),
        "sku": row.get("sku") or "",
        "name": row.get("name") or "",
        "category": row.get("category") or "",
        "unit": row.get("unit") or "each",
        "typical_cost": float(row.get("cost") or 0),
        "list_price": float(row.get("sell_price") or 0),
        "vendor_key": row.get("preferred_supplier") or "",
        "manufacturer": row.get("manufacturer") or "",
        "part_number": row.get("mfg_number") or "",
        "tags": tags,
        "notes": row.get("notes") or "",
    }


def init_catalog():
    """CP-5 no-op. The real catalog is initialized by product_catalog.

    Retained only so legacy startup code (`app.py`) and legacy callers
    keep working. The previous implementation created a `products` table
    and seeded 27 synthetic SKUs; that behavior is gone.
    """
    try:
        from src.agents import product_catalog as _real
        # product_catalog._ensure_schema() is called lazily inside
        # _get_conn(); touching it here makes startup deterministic.
        _real._get_conn().close()
    except Exception as e:
        log.debug("CP-5 init_catalog delegate: %s", e)
    return 0


def search_catalog(query: str, limit: int = 10) -> list:
    """Delegates to product_catalog.search_products, returns legacy shape."""
    try:
        from src.agents.product_catalog import search_products
        rows = search_products(query, limit=limit)
        return [_row_to_legacy(r) for r in rows]
    except Exception as e:
        log.warning("CP-5 search_catalog delegate failed: %s", e)
        return []


def get_catalog(category: Optional[str] = None, limit: int = 200) -> list:
    """Browse catalog via product_catalog, returned in legacy shape."""
    try:
        from src.agents.product_catalog import _get_conn
        conn = _get_conn()
        try:
            if category:
                rows = conn.execute(
                    "SELECT * FROM product_catalog WHERE LOWER(category) LIKE ? "
                    "ORDER BY category, name LIMIT ?",
                    (f"%{category.lower()}%", limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM product_catalog ORDER BY category, name LIMIT ?",
                    (limit,),
                ).fetchall()
            return [_row_to_legacy(dict(r)) for r in rows]
        finally:
            conn.close()
    except Exception as e:
        log.warning("CP-5 get_catalog delegate failed: %s", e)
        return []


def get_categories() -> list:
    try:
        from src.agents.product_catalog import _get_conn
        conn = _get_conn()
        try:
            rows = conn.execute(
                "SELECT category, COUNT(*) as cnt FROM product_catalog "
                "WHERE category IS NOT NULL AND category <> '' "
                "GROUP BY category ORDER BY cnt DESC"
            ).fetchall()
            return [{"category": r[0], "count": r[1]} for r in rows]
        finally:
            conn.close()
    except Exception as e:
        log.warning("CP-5 get_categories delegate failed: %s", e)
        return []


def auto_ingest_item(description, unit_price=0, vendor_key="", manufacturer="", source="quote"):
    """Ingest a newly-quoted item into product_catalog.

    Delegates to product_catalog.record_item_from_quote (or its closest
    equivalent) when available; otherwise records a minimal row so the
    item is at least visible for future matching.
    """
    if not description or len(description.strip()) < 4:
        return {"added": False, "reason": "too_short"}
    name = description.strip()
    sku_candidate = re.sub(r"[^A-Z0-9]", "-", name.upper())[:30].strip("-")
    try:
        existing = search_catalog(name[:30], limit=3)
        if existing:
            return {"added": False, "reason": "exists", "matched": existing[0]["sku"]}
    except Exception:
        pass
    try:
        from src.agents.product_catalog import _get_conn
        from datetime import datetime
        now = datetime.now().isoformat()
        conn = _get_conn()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO product_catalog "
                "(sku, name, description, category, manufacturer, cost, sell_price, "
                " times_quoted, last_sold_date, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?)",
                (sku_candidate, name, name, _guess_category(name),
                 manufacturer, round(float(unit_price or 0) * 0.75, 2),
                 float(unit_price or 0), now),
            )
            conn.commit()
        finally:
            conn.close()
        return {"added": True, "sku": sku_candidate, "name": name,
                "category": _guess_category(name)}
    except Exception as e:
        return {"added": False, "reason": str(e)}


def _guess_category(name: str) -> str:
    nl = name.lower()
    if any(k in nl for k in ["glove", "nitrile", "latex", "vinyl"]):
        return "Medical/PPE"
    if any(k in nl for k in ["sanitizer", "soap", "hand wash"]):
        return "Medical/PPE"
    if any(k in nl for k in ["n95", "respirator", "mask", "kn95"]):
        return "PPE/Respiratory"
    if any(k in nl for k in ["hi-vis", "vest", "ansi", "high visibility"]):
        return "Safety/PPE"
    if any(k in nl for k in ["first aid", "kit", "bandage", "gauze"]):
        return "Safety/First Aid"
    if any(k in nl for k in ["tourniquet", "bleed", "trauma", "ifak"]):
        return "Safety/Trauma"
    if any(k in nl for k in ["brief", "diaper", "incontinence", "chux", "underpad"]):
        return "Medical/Incontinence"
    if any(k in nl for k in ["sharps", "container", "biohazard"]):
        return "Medical/Sharps"
    if any(k in nl for k in ["trash", "bag", "liner", "janitorial"]):
        return "Janitorial"
    if any(k in nl for k in ["paper", "towel", "toilet", "tissue"]):
        return "Janitorial"
    if any(k in nl for k in ["office", "pen", "binder", "folder", "staple"]):
        return "Office Supplies"
    return "General"


def get_catalog_stats() -> dict:
    try:
        from src.agents.product_catalog import _get_conn
        conn = _get_conn()
        try:
            total = conn.execute(
                "SELECT COUNT(*) FROM product_catalog"
            ).fetchone()[0]
            cats = conn.execute(
                "SELECT COUNT(DISTINCT category) FROM product_catalog "
                "WHERE category IS NOT NULL AND category <> ''"
            ).fetchone()[0]
            return {"total_skus": total, "categories": cats, "p0_skus_loaded": 0}
        finally:
            conn.close()
    except Exception as e:
        log.warning("CP-5 get_catalog_stats delegate failed: %s", e)
        return {"total_skus": 0, "categories": 0, "p0_skus_loaded": 0}
