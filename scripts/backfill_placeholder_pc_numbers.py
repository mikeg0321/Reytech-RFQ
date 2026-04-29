"""Backfill placeholder pc_number / solicitation_number rows to needs_review.

2026-04-29: Prod has rows where the legacy email-poller fallback chain ended
in `subject[:40]` (or filename-stem) and produced single-word junk like
"WORKSHEET" or "GOOD" in pc_number / solicitation_number. The classifier
stamped status='parsed', so the queue treated these as done when they
actually needed operator triage. Concrete: rfq_7813c4e1 + pc_a391db8f
from keith.alsing@calvet.ca.gov.

This script:
  1. Scans SQLite price_checks + rfqs tables (single source of truth)
  2. For rows with len(items) == 0 AND placeholder pc_number/sol#, flips
     status -> 'needs_review' so they re-surface on the operator queue
  3. Updates both the column AND the data_json blob (data_layer reads
     status from column-first when available)

Usage:
    python scripts/backfill_placeholder_pc_numbers.py --dry-run
    python scripts/backfill_placeholder_pc_numbers.py
    railway ssh "python scripts/backfill_placeholder_pc_numbers.py --dry-run"
    railway ssh "python scripts/backfill_placeholder_pc_numbers.py"
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _default_db_path() -> str:
    for candidate in ("/data/reytech.db", "data/reytech.db", "reytech.db"):
        if os.path.exists(candidate):
            return candidate
    return "data/reytech.db"


def _is_placeholder(value: str) -> bool:
    """Mirror src.api.dashboard._is_placeholder_number — kept inline so the
    backfill script doesn't drag in the full Flask import graph."""
    if not value:
        return True
    s = str(value).strip()
    if not s:
        return True
    if s.startswith("AUTO_"):
        return False
    if s.isupper() and s.isalpha() and 2 <= len(s) <= 20:
        return True
    if s.lower() in {"unknown", "rfq", "quote", "request", "worksheet", "good",
                     "bid", "vendor", "price", "check", "form"}:
        return True
    return False


def _items_count(items_raw, data_json_raw) -> int:
    """Count items from either the items column or the data_json blob.
    The data_json blob is authoritative for newer rows; older rows have
    items in the dedicated column."""
    # data_json wins when present
    if data_json_raw:
        try:
            blob = json.loads(data_json_raw)
            if isinstance(blob, dict):
                items = blob.get("items") or blob.get("line_items") or []
                if isinstance(items, list):
                    return len(items)
        except (json.JSONDecodeError, TypeError):
            pass
    if items_raw:
        try:
            parsed = json.loads(items_raw) if isinstance(items_raw, str) else items_raw
            if isinstance(parsed, list):
                return len(parsed)
        except (json.JSONDecodeError, TypeError):
            pass
    return 0


def _flip_data_json_status(data_json_raw: str, new_status: str) -> str:
    """Update the status field inside the data_json blob (if present)."""
    if not data_json_raw:
        return data_json_raw
    try:
        blob = json.loads(data_json_raw)
        if isinstance(blob, dict):
            blob["status"] = new_status
            return json.dumps(blob)
    except (json.JSONDecodeError, TypeError):
        pass
    return data_json_raw


_FLIPPABLE_STATUSES = {"parsed", "new", ""}


def _scan_price_checks(conn: sqlite3.Connection, dry_run: bool) -> int:
    flipped = 0
    cur = conn.execute(
        "SELECT id, pc_number, status, items, data_json FROM price_checks"
    )
    for row in cur.fetchall():
        rid, pc_num, status, items_raw, data_json_raw = row
        # Only flip in-queue rows. Never touch sent/won/lost/voided/etc —
        # those represent real actions that mustn't be reverted.
        if (status or "") not in _FLIPPABLE_STATUSES:
            continue
        n_items = _items_count(items_raw, data_json_raw)
        if n_items == 0 and _is_placeholder(pc_num):
            print(f"[pc]   {rid} pc_number={pc_num!r} status={status!r} items=0 -> needs_review")
            if not dry_run:
                new_blob = _flip_data_json_status(data_json_raw, "needs_review")
                conn.execute(
                    "UPDATE price_checks SET status=?, data_json=? WHERE id=?",
                    ("needs_review", new_blob, rid),
                )
            flipped += 1
    return flipped


def _scan_rfqs(conn: sqlite3.Connection, dry_run: bool) -> int:
    flipped = 0
    cur = conn.execute(
        "SELECT id, solicitation_number, rfq_number, status, items, data_json FROM rfqs"
    )
    for row in cur.fetchall():
        rid, sol_num, rfq_num, status, items_raw, data_json_raw = row
        if (status or "") not in _FLIPPABLE_STATUSES:
            continue
        n_items = _items_count(items_raw, data_json_raw)
        sol = sol_num or rfq_num or ""
        if n_items == 0 and _is_placeholder(sol):
            print(f"[rfq]  {rid} sol={sol!r} status={status!r} items=0 -> needs_review")
            if not dry_run:
                new_blob = _flip_data_json_status(data_json_raw, "needs_review")
                conn.execute(
                    "UPDATE rfqs SET status=?, data_json=? WHERE id=?",
                    ("needs_review", new_blob, rid),
                )
            flipped += 1
    return flipped


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would change, don't write")
    parser.add_argument("--db", default=None,
                        help="Override DB path (defaults to /data/reytech.db or data/reytech.db)")
    args = parser.parse_args()

    db_path = args.db or _default_db_path()
    print(f"db={db_path} dry_run={args.dry_run}")

    if not os.path.exists(db_path):
        print(f"[err] DB not found at {db_path}")
        return 1

    conn = sqlite3.connect(db_path)
    try:
        pc_count = _scan_price_checks(conn, args.dry_run)
        rfq_count = _scan_rfqs(conn, args.dry_run)
        if not args.dry_run:
            conn.commit()
    finally:
        conn.close()

    verb = "would flip" if args.dry_run else "flipped"
    print(f"\n{verb}: {pc_count} PCs, {rfq_count} RFQs -> needs_review")
    return 0


if __name__ == "__main__":
    sys.exit(main())
