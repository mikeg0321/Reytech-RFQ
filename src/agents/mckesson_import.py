"""McKesson catalog importer — XLSX → product_catalog.

Seeds the catalog with McKesson's approved-items list. The file the user
typically uploads has 5 columns:
    Type | Item | Description | Preferred Vendor | MPN
    ---- | ---- | ----------- | ---------------- | ---
    Inventory Part | 1001682 | Wheelchair Footrest FOOTREST, WHEEL | McKesson | 1001682
    Inventory Part | 1002774 | Tympanic Thermometer Probe Cover... | McKesson | 06000-005

This file carries no prices or URLs — it's a product-inventory sheet, not a
quote sheet. We seed rows (description + MPN + supplier) and leave
web_lowest_price empty. refresh_catalog_web_prices (#123) will fill current
MSRP on its next sweep for items within the 2-year lookback window.

Dedup: delegates to product_catalog.add_to_catalog(), which matches by name
(falls back to description prefix) and updates existing rows instead of
creating duplicates. Safe to re-run.
"""
import logging
from typing import Optional

log = logging.getLogger(__name__)


def import_mckesson_xlsx(xlsx_path: str, supplier_override: Optional[str] = None) -> dict:
    """Read a McKesson XLSX and ingest into product_catalog.

    Args:
        xlsx_path: absolute path to the .xlsx file on disk
        supplier_override: force this supplier name regardless of the
            "Preferred Vendor" column (useful when the sheet has mixed
            vendors but you want them all flagged as McKesson-approved)

    Returns:
        {
          "ok": bool,
          "total_rows": int,              — excluding header
          "imported": int,                — new product_catalog rows created
          "updated": int,                 — existing rows enriched (MPN/mfg etc.)
          "skipped": int,                 — rows with no usable data
          "errors": list[str],
          "supplier_counts": {name: n},   — breakdown by Preferred Vendor
        }
    """
    try:
        import openpyxl
    except ImportError:
        return {"ok": False, "error": "openpyxl not installed"}

    try:
        from src.agents.product_catalog import add_to_catalog, init_catalog_db
    except ImportError as e:
        return {"ok": False, "error": f"product_catalog not available: {e}"}

    try:
        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    except Exception as e:
        return {"ok": False, "error": f"Could not open XLSX: {e}"}

    init_catalog_db()

    stats = {
        "ok": True,
        "total_rows": 0,
        "imported": 0,
        "updated": 0,
        "skipped": 0,
        "errors": [],
        "supplier_counts": {},
    }

    sheet = wb.active  # first sheet
    rows_iter = sheet.iter_rows(values_only=True)

    header = next(rows_iter, None)
    if not header:
        return {"ok": False, "error": "XLSX has no rows"}

    # Build case-insensitive header → index mapping. Accept minor naming drift.
    header_map = {}
    for idx, col in enumerate(header):
        if col is None:
            continue
        key = str(col).strip().lower().replace(" ", "_")
        header_map[key] = idx

    def _col(row, *names):
        """Get the first populated column from a list of alternate header names."""
        for n in names:
            idx = header_map.get(n.lower().replace(" ", "_"))
            if idx is not None and idx < len(row):
                val = row[idx]
                if val is not None and str(val).strip():
                    return str(val).strip()
        return ""

    for row in rows_iter:
        if not row or not any(row):
            continue
        stats["total_rows"] += 1

        description = _col(row, "description", "desc", "item_name", "product_name")
        sku = _col(row, "item", "sku", "item_number", "part_number")
        mpn = _col(row, "mpn", "manufacturer_part_number", "mfg_part_number")
        vendor = supplier_override or _col(row, "preferred_vendor", "vendor", "supplier") or "McKesson"

        # Must have at least a description OR a part number to be useful
        if not description and not sku and not mpn:
            stats["skipped"] += 1
            continue

        # MPN is the true catalog identifier; fall back to SKU if MPN missing
        part_number = mpn or sku

        # Track vendor breakdown
        stats["supplier_counts"][vendor] = stats["supplier_counts"].get(vendor, 0) + 1

        try:
            # add_to_catalog handles dedup internally: matches by name, updates
            # existing rows, or inserts new. Returns product_id on insert, None
            # on match. We treat None as "updated existing" because the function
            # DOES enrich the existing row in that path.
            pid = add_to_catalog(
                description=description or part_number,
                part_number=part_number,
                cost=0,               # no prices in the McKesson sheet
                sell_price=0,
                supplier_name=vendor,
                uom="EA",
                manufacturer=vendor,  # McKesson IS the manufacturer for house brand
                mfg_number=mpn or part_number,
                source="mckesson_import",
            )
            if pid:
                stats["imported"] += 1
            else:
                stats["updated"] += 1
        except Exception as e:
            stats["errors"].append(f"row {stats['total_rows']}: {str(e)[:120]}")
            if len(stats["errors"]) > 30:
                stats["errors"].append("... (truncated)")
                break

    log.info(
        "McKesson import: %d rows, %d new, %d updated, %d skipped, %d errors",
        stats["total_rows"], stats["imported"], stats["updated"],
        stats["skipped"], len(stats["errors"]),
    )
    return stats
