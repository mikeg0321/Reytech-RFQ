"""Phase 1.7: import McKesson catalog CSV into supplier_skus.

The CSV at G:\\My Drive\\Reytech Inc\\Suppliers\\McKesson Items.csv has
~2,179 rows of (Type, Item, Description, Preferred Vendor, MPN). This
script normalizes each row into supplier_skus(supplier='mckesson',
supplier_sku=Item, mfg_number=MPN, description=Description).

Idempotent: UPSERT-style INSERT OR REPLACE keyed on the unique index
(supplier, supplier_sku). Re-running just refreshes timestamps and
descriptions — no duplicates.

This is enrichment data, NOT cost. McKesson costs come from the customer
portal which we don't scrape. The pricing oracle still resolves cost via
the existing tier cascade (catalog → web → vendor → SCPRS reference).

Usage (locally for testing):
    python scripts/import_mckesson_catalog.py --dry-run
    python scripts/import_mckesson_catalog.py

Usage (production via Railway):
    railway run python scripts/import_mckesson_catalog.py

Default CSV path is G:\\My Drive\\Reytech Inc\\Suppliers\\McKesson Items.csv
on Mike's machine. Override with --csv if running elsewhere.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from typing import Iterable

log = logging.getLogger("import_mckesson_catalog")


DEFAULT_CSV = r"G:\My Drive\Reytech Inc\Suppliers\McKesson Items.csv"
SUPPLIER_NAME = "mckesson"


def _clean_description(raw: str) -> str:
    """The McKesson CSV embeds '..McKesson #\\t1234..Manufacturer #\\t5678'
    inside the Description field. Strip those tail blocks so the catalog
    holds clean human-readable text."""
    if not raw:
        return ""
    body = raw
    for marker in ("..McKesson #", "..Manufacturer #", "..**", "..Old McKesson #"):
        idx = body.find(marker)
        if idx >= 0:
            body = body[:idx]
    return body.strip()


def _iter_rows(csv_path: str) -> Iterable[dict]:
    with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            sku = (row.get("Item") or "").strip()
            mpn = (row.get("MPN") or "").strip()
            desc = _clean_description(row.get("Description") or "")
            if not sku:
                continue
            yield {"sku": sku, "mpn": mpn or sku, "desc": desc}


def import_csv(csv_path: str, dry_run: bool = False) -> dict:
    """Import a McKesson CSV into supplier_skus.

    Returns: {ok, rows_read, rows_inserted, rows_updated, errors, dry_run}
    """
    from src.core.db import get_db

    result = {
        "ok": True,
        "rows_read": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "errors": [],
        "dry_run": dry_run,
    }

    rows = list(_iter_rows(csv_path))
    result["rows_read"] = len(rows)

    if dry_run:
        return result

    with get_db() as conn:
        # Make sure migration 30 has run; create defensively.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS supplier_skus (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier TEXT NOT NULL,
                supplier_sku TEXT NOT NULL,
                mfg_number TEXT,
                description TEXT,
                imported_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_supplier_skus_unique
            ON supplier_skus(supplier, supplier_sku)
        """)

        for r in rows:
            try:
                existing = conn.execute(
                    "SELECT id FROM supplier_skus WHERE supplier=? AND supplier_sku=?",
                    (SUPPLIER_NAME, r["sku"]),
                ).fetchone()

                if existing:
                    conn.execute("""
                        UPDATE supplier_skus
                        SET mfg_number=?, description=?, updated_at=datetime('now')
                        WHERE id=?
                    """, (r["mpn"], r["desc"], existing["id"]))
                    result["rows_updated"] += 1
                else:
                    conn.execute("""
                        INSERT INTO supplier_skus
                        (supplier, supplier_sku, mfg_number, description)
                        VALUES (?, ?, ?, ?)
                    """, (SUPPLIER_NAME, r["sku"], r["mpn"], r["desc"]))
                    result["rows_inserted"] += 1
            except Exception as e:
                result["errors"].append(f"{r.get('sku')}: {e}")

    log.info(
        "McKesson import: read=%d inserted=%d updated=%d errors=%d dry_run=%s",
        result["rows_read"], result["rows_inserted"],
        result["rows_updated"], len(result["errors"]), dry_run,
    )
    return result


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Import McKesson catalog CSV")
    p.add_argument("--csv", default=DEFAULT_CSV,
                   help="Path to the McKesson Items.csv file")
    p.add_argument("--dry-run", action="store_true",
                   help="Read the CSV but don't write to the DB")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = import_csv(args.csv, dry_run=args.dry_run)
    print(f"OK rows_read={result['rows_read']} "
          f"inserted={result['rows_inserted']} "
          f"updated={result['rows_updated']} "
          f"errors={len(result['errors'])} "
          f"dry_run={result['dry_run']}")
    if result["errors"][:5]:
        print("First errors:")
        for e in result["errors"][:5]:
            print(f"  {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
