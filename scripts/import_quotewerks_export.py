"""Phase 0.7d: import QuoteWerks export CSV into the `quotes` table.

The QuoteWerks export at C:\\Users\\mikeg\\OneDrive\\Desktop\\quotewerks_export_latest.csv
holds Reytech's full pre-app quote history. Format:
  - Wide CSV: DocumentHeaders_* columns (one row per item, headers repeat)
  - One quote = N rows (one per line item)
  - Group by DocumentHeaders_DocNo

Mike's win/loss rule (his words): "If you find them in SCPRS verbatim,
I won the PO. If you don't find the quote, I lost it." This script does
NOT do the SCPRS check — it imports every quote with status='sent'. A
follow-up step (verify_quotewerks_outcomes) walks each imported quote
and searches SCPRS to flip status to won/lost. Splitting these two jobs
lets the import be fast + idempotent without coupling to SCPRS latency.

Idempotent: keys on quote_number; INSERT OR REPLACE refreshes any prior
import. Re-running with a refreshed CSV is safe.

Usage (locally for testing):
    python scripts/import_quotewerks_export.py --dry-run
    python scripts/import_quotewerks_export.py

Production: the import runs via HTTP endpoint
POST /api/admin/import-quotewerks (CSV body) so it executes server-side.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Iterable

# Allow this script to be invoked outside the repo root.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

log = logging.getLogger("import_quotewerks_export")


DEFAULT_CSV = r"C:\Users\mikeg\OneDrive\Desktop\quotewerks_export_latest.csv"


def _parse_date(raw: str) -> str:
    """QuoteWerks dates like '2/23/2023' → ISO '2023-02-23'."""
    if not raw:
        return ""
    s = raw.strip().split()[0]
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def _f(raw: str) -> float:
    """Best-effort float parse. QuoteWerks uses bare numbers as strings."""
    if raw is None:
        return 0.0
    s = str(raw).strip().replace(",", "").replace("$", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except (TypeError, ValueError):
        return 0.0


def _initial_status(header: dict) -> tuple[str, str]:
    """Decide the import-time status. Mike's rule: SCPRS lookup is the
    real win/loss source — but that's a separate step. At import time the
    only signals we have are QuoteWerks's own:

    - DocType='ORDER' or DocStatus in ('Closed','Won') → already-won
      (skip SCPRS verify; QuoteWerks already converted to an order)
    - Otherwise → 'sent', awaiting SCPRS verify

    Returns (status, status_notes).
    """
    doc_type = (header.get("DocumentHeaders_DocType") or "").strip().upper()
    doc_status = (header.get("DocumentHeaders_DocStatus") or "").strip()
    converted_ref = (header.get("DocumentHeaders_ConvertedRef") or "").strip()
    sold_to_po = (header.get("DocumentHeaders_SoldToPONumber") or "").strip()

    if doc_type == "ORDER":
        return "won", f"QuoteWerks: converted to order {converted_ref or sold_to_po}"
    if doc_status in ("Closed", "Won"):
        return "won", f"QuoteWerks: DocStatus={doc_status} ConvertedRef={converted_ref}"
    return "sent", f"QuoteWerks: DocStatus={doc_status} (awaiting SCPRS verify)"


def _build_quote(rows: list[dict], doc_no: str) -> dict:
    """Group N item-rows for one quote into a quotes-table-shaped dict."""
    h = rows[0]
    items = []
    for r in rows:
        desc = (r.get("DocumentItems_Description") or "").strip()
        if not desc:
            continue
        items.append({
            "description": desc,
            "mfg_number": (r.get("DocumentItems_ManufacturerPartNumber") or "").strip(),
            "vendor_part_number": (r.get("DocumentItems_VendorPartNumber") or "").strip(),
            "qty": _f(r.get("DocumentItems_QtyTotal")) or 1,
            "unit_price": _f(r.get("DocumentItems_UnitPrice")),
            "supplier_cost": _f(r.get("DocumentItems_UnitCost")),
            "uom": (r.get("DocumentItems_UnitOfMeasure") or "EA").strip(),
            "manufacturer": (r.get("DocumentItems_Manufacturer") or "").strip(),
        })

    status, notes = _initial_status(h)
    institution = (h.get("DocumentHeaders_SoldToCompany") or "").strip()
    ship_to = (h.get("DocumentHeaders_ShipToCompany") or institution).strip()
    po_number = (h.get("DocumentHeaders_SoldToPONumber") or "").strip()
    converted_ref = (h.get("DocumentHeaders_ConvertedRef") or "").strip()
    doc_date = _parse_date(h.get("DocumentHeaders_DocDate", ""))

    return {
        "quote_number": doc_no,
        "status": status,
        "agency": institution,
        "institution": institution,
        "ship_to_name": ship_to,
        "line_items": json.dumps(items),
        "total": _f(h.get("DocumentHeaders_GrandTotal")),
        "po_number": po_number or converted_ref,
        "status_notes": notes,
        "created_at": doc_date or datetime.now().date().isoformat(),
        "is_test": 0,
        "_item_count": len(items),
    }


def _iter_quote_groups(csv_path: str):
    groups = defaultdict(list)
    with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            doc_no = (row.get("DocumentHeaders_DocNo") or "").strip()
            if not doc_no:
                continue
            groups[doc_no].append(row)
    for doc_no, rows in groups.items():
        yield doc_no, rows


def import_csv(csv_path: str, dry_run: bool = False) -> dict:
    """Import QuoteWerks export → quotes table.

    Returns: {ok, quotes_read, quotes_inserted, quotes_updated, items_total,
              by_status, errors, dry_run}
    """
    from src.core.db import get_db

    result = {
        "ok": True,
        "quotes_read": 0,
        "quotes_inserted": 0,
        "quotes_updated": 0,
        "items_total": 0,
        "by_status": {"won": 0, "sent": 0},
        "errors": [],
        "dry_run": dry_run,
    }

    if not dry_run:
        # Defensive: ensure target table exists.
        with get_db() as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='quotes'"
            ).fetchone()
            if not row:
                result["ok"] = False
                result["errors"].append("quotes table not found")
                return result

    for doc_no, rows in _iter_quote_groups(csv_path):
        result["quotes_read"] += 1
        try:
            q = _build_quote(rows, doc_no)
            result["items_total"] += q["_item_count"]
            result["by_status"][q["status"]] = result["by_status"].get(q["status"], 0) + 1

            if dry_run:
                continue

            with get_db() as conn:
                existing = conn.execute(
                    "SELECT quote_number FROM quotes WHERE quote_number=?",
                    (doc_no,),
                ).fetchone()

                if existing:
                    conn.execute("""
                        UPDATE quotes
                        SET status=?, agency=?, institution=?, line_items=?,
                            total=?, po_number=?, status_notes=?, created_at=?,
                            is_test=?
                        WHERE quote_number=?
                    """, (q["status"], q["agency"], q["institution"],
                          q["line_items"], q["total"], q["po_number"],
                          q["status_notes"], q["created_at"], q["is_test"],
                          doc_no))
                    result["quotes_updated"] += 1
                else:
                    conn.execute("""
                        INSERT INTO quotes
                        (quote_number, status, agency, institution, line_items,
                         total, po_number, status_notes, created_at, is_test)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (q["quote_number"], q["status"], q["agency"],
                          q["institution"], q["line_items"], q["total"],
                          q["po_number"], q["status_notes"], q["created_at"],
                          q["is_test"]))
                    result["quotes_inserted"] += 1

        except Exception as e:
            result["errors"].append(f"{doc_no}: {e}")

    log.info(
        "QuoteWerks import: read=%d inserted=%d updated=%d items=%d "
        "by_status=%s errors=%d dry_run=%s",
        result["quotes_read"], result["quotes_inserted"],
        result["quotes_updated"], result["items_total"],
        result["by_status"], len(result["errors"]), dry_run,
    )
    return result


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Import QuoteWerks export CSV")
    p.add_argument("--csv", default=DEFAULT_CSV)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = import_csv(args.csv, dry_run=args.dry_run)
    print(f"OK quotes_read={result['quotes_read']} "
          f"inserted={result['quotes_inserted']} "
          f"updated={result['quotes_updated']} "
          f"items={result['items_total']} "
          f"by_status={result['by_status']} "
          f"errors={len(result['errors'])} "
          f"dry_run={result['dry_run']}")
    if result["errors"][:5]:
        print("First errors:")
        for e in result["errors"][:5]:
            print(f"  {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
