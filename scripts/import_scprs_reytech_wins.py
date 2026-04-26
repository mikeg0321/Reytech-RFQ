"""Phase 0.7d: import the SCPRS "Reytech wins since 2022" HTML export.

Mike's SCPRS Detail-Information export is HTML masquerading as .xls.
Format: one big <table> with header + (PO summary row) + N (item row) per PO.
  - Summary row: Business Unit, Department Name, Purchase Document #,
    Start Date, End Date, Grand Total
  - Item row: same key cols blank-or-repeated, plus Line #, Item ID,
    Item Description, UNSPSC

This script parses the HTML and writes one row per PO into
scprs_reytech_wins (migration 31). That table becomes the
ground-truth "we won this PO" set for QuoteWerks-quote verification.

Idempotent on po_number — re-uploading replaces existing rows so a
refreshed export overwrites stale data without dupes.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Iterable

# Allow this script to be invoked outside the repo root.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

log = logging.getLogger("import_scprs_reytech_wins")


def _strip_apostrophe(s: str) -> str:
    """SCPRS exports IDs as `'8955` (leading apostrophe forces text in
    Excel). Strip it for clean storage."""
    if not s:
        return ""
    s = s.strip()
    if s.startswith("'"):
        s = s[1:]
    return s


def _f(raw) -> float:
    if raw is None:
        return 0.0
    s = str(raw).strip().replace(",", "").replace("$", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except (TypeError, ValueError):
        return 0.0


def _parse_date(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip().split()[0]
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def _parse_html(html_text: str) -> list[dict]:
    """Walk the SCPRS HTML and group rows into per-PO dicts."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    if not rows:
        return []

    # Header
    header = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
    idx = {name: i for i, name in enumerate(header)}

    def cell(row_cells, key):
        i = idx.get(key)
        if i is None or i >= len(row_cells):
            return ""
        return row_cells[i].get_text(strip=True)

    def cell_raw(row_cells, key):
        """Get cell text with newlines preserved (item descriptions
        are multi-line in the SCPRS export)."""
        i = idx.get(key)
        if i is None or i >= len(row_cells):
            return ""
        return row_cells[i].get_text(separator="\n", strip=True)

    # Build PO groups. Strategy: track current PO. A PO summary row has
    # Grand Total populated AND no Line #. Item rows have Line # populated.
    pos_by_num = {}
    current_po = None

    for r in rows[1:]:
        cells = r.find_all(["th", "td"])
        if not cells:
            continue
        po_num = _strip_apostrophe(cell(cells, "Purchase Document #"))
        line_num = cell(cells, "Line #").strip()
        grand_total_raw = cell(cells, "Grand Total")

        if po_num and grand_total_raw and not line_num:
            # PO summary row
            current_po = po_num
            pos_by_num.setdefault(po_num, {
                "po_number": po_num,
                "business_unit": _strip_apostrophe(cell(cells, "Business Unit")),
                "dept_name": cell(cells, "Department Name"),
                "associated_po": _strip_apostrophe(cell(cells, "Associated PO #")),
                "start_date": _parse_date(cell(cells, "Start Date")),
                "end_date": _parse_date(cell(cells, "End Date")),
                "grand_total": _f(grand_total_raw),
                "items": [],
            })
        elif line_num and current_po:
            # Item row for current PO
            entry = pos_by_num.get(current_po)
            if entry is None:
                continue
            entry["items"].append({
                "line_num": line_num,
                "item_id": _strip_apostrophe(cell(cells, "Item ID")),
                "description": cell_raw(cells, "Item Description"),
                "unspsc": _strip_apostrophe(cell(cells, "UNSPSC")),
            })
        # Ignore stray rows.

    return list(pos_by_num.values())


def _ensure_table():
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scprs_reytech_wins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                po_number TEXT NOT NULL,
                business_unit TEXT,
                dept_name TEXT,
                associated_po TEXT,
                start_date TEXT,
                end_date TEXT,
                grand_total REAL,
                items_json TEXT,
                imported_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_scprs_reytech_wins_po
            ON scprs_reytech_wins(po_number)
        """)


def import_html(html_text: str, dry_run: bool = False) -> dict:
    pos = _parse_html(html_text)
    result = {
        "ok": True,
        "pos_parsed": len(pos),
        "pos_inserted": 0,
        "pos_updated": 0,
        "items_total": sum(len(p.get("items", [])) for p in pos),
        "errors": [],
        "dry_run": dry_run,
    }

    if dry_run:
        return result

    _ensure_table()
    from src.core.db import get_db
    with get_db() as conn:
        for p in pos:
            try:
                existing = conn.execute(
                    "SELECT id FROM scprs_reytech_wins WHERE po_number = ?",
                    (p["po_number"],),
                ).fetchone()
                items_json = json.dumps(p.get("items", []))
                if existing:
                    conn.execute("""
                        UPDATE scprs_reytech_wins
                        SET business_unit=?, dept_name=?, associated_po=?,
                            start_date=?, end_date=?, grand_total=?,
                            items_json=?, imported_at=datetime('now')
                        WHERE id=?
                    """, (p["business_unit"], p["dept_name"], p["associated_po"],
                          p["start_date"], p["end_date"], p["grand_total"],
                          items_json, existing["id"]))
                    result["pos_updated"] += 1
                else:
                    conn.execute("""
                        INSERT INTO scprs_reytech_wins
                        (po_number, business_unit, dept_name, associated_po,
                         start_date, end_date, grand_total, items_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (p["po_number"], p["business_unit"], p["dept_name"],
                          p["associated_po"], p["start_date"], p["end_date"],
                          p["grand_total"], items_json))
                    result["pos_inserted"] += 1
            except Exception as e:
                result["errors"].append(f"{p.get('po_number', '?')}: {e}")

    log.info(
        "scprs_reytech_wins import: parsed=%d inserted=%d updated=%d "
        "items=%d errors=%d dry_run=%s",
        result["pos_parsed"], result["pos_inserted"], result["pos_updated"],
        result["items_total"], len(result["errors"]), dry_run,
    )
    return result


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Import SCPRS Reytech wins HTML")
    p.add_argument("--html",
                   default=r"C:\Users\mikeg\OneDrive\Desktop\Detail_Information_2026-04-25-21.42.27.000000.xls")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    with open(args.html, "r", encoding="utf-8", errors="replace") as fh:
        body = fh.read()
    result = import_html(body, dry_run=args.dry_run)
    print(f"OK pos_parsed={result['pos_parsed']} "
          f"inserted={result['pos_inserted']} "
          f"updated={result['pos_updated']} "
          f"items={result['items_total']} "
          f"errors={len(result['errors'])} "
          f"dry_run={result['dry_run']}")
    if result["errors"][:5]:
        for e in result["errors"][:5]:
            print(f"  {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
