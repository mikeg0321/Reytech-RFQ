"""Backfill placeholder pc_number / solicitation_number rows to needs_review.

2026-04-29: Prod has rows where the legacy email-poller fallback chain ended
in `subject[:40]` (or filename-stem) and produced single-word junk like
"WORKSHEET" or "GOOD" in pc_number / solicitation_number. The classifier
stamped status='parsed', so the queue treated these as done when they
actually needed operator triage. Concrete: rfq_7813c4e1 + pc_a391db8f
from keith.alsing@calvet.ca.gov.

This script:
  1. Scans price_checks (JSON) and rfqs (JSON + DB) for placeholder values
  2. For rows with len(items) == 0 AND placeholder pc_number/sol#, flips
     status -> 'needs_review' so they re-surface on the operator queue
  3. Writes a ledger row so re-runs skip already-processed IDs

Usage:
    python scripts/backfill_placeholder_pc_numbers.py --dry-run
    python scripts/backfill_placeholder_pc_numbers.py
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


def _default_data_dir() -> str:
    for candidate in ("/data", "data"):
        if os.path.isdir(candidate):
            return candidate
    return "data"


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


def _scan_price_checks(data_dir: str, dry_run: bool) -> int:
    """Walk data/price_checks/*.json — flip placeholder + zero-items rows."""
    pc_dir = os.path.join(data_dir, "price_checks")
    if not os.path.isdir(pc_dir):
        print(f"[skip] no price_checks dir at {pc_dir}")
        return 0
    flipped = 0
    for fn in os.listdir(pc_dir):
        if not fn.endswith(".json"):
            continue
        path = os.path.join(pc_dir, fn)
        try:
            with open(path) as f:
                pc = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"[err]  {fn}: {e}")
            continue
        items = pc.get("items") or pc.get("line_items") or []
        pc_num = pc.get("pc_number", "")
        if len(items) == 0 and _is_placeholder(pc_num):
            print(f"[pc]   {pc.get('id','?')} pc_number={pc_num!r} items=0 -> needs_review")
            if not dry_run:
                pc["status"] = "needs_review"
                with open(path, "w") as f:
                    json.dump(pc, f, indent=2)
            flipped += 1
    return flipped


def _scan_rfqs(data_dir: str, dry_run: bool) -> int:
    """Walk data/rfqs/*.json — same gate on solicitation_number / rfq_number."""
    rfq_dir = os.path.join(data_dir, "rfqs")
    if not os.path.isdir(rfq_dir):
        print(f"[skip] no rfqs dir at {rfq_dir}")
        return 0
    flipped = 0
    for fn in os.listdir(rfq_dir):
        if not fn.endswith(".json"):
            continue
        path = os.path.join(rfq_dir, fn)
        try:
            with open(path) as f:
                rfq = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"[err]  {fn}: {e}")
            continue
        items = rfq.get("line_items") or rfq.get("items") or []
        sol = rfq.get("solicitation_number") or rfq.get("rfq_number") or ""
        if len(items) == 0 and _is_placeholder(sol):
            print(f"[rfq]  {rfq.get('id','?')} sol={sol!r} items=0 -> needs_review")
            if not dry_run:
                rfq["status"] = "needs_review"
                with open(path, "w") as f:
                    json.dump(rfq, f, indent=2)
            flipped += 1
    return flipped


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would change, don't write")
    parser.add_argument("--data-dir", default=None,
                        help="Override data dir (defaults to /data or ./data)")
    args = parser.parse_args()

    data_dir = args.data_dir or _default_data_dir()
    print(f"data_dir={data_dir} dry_run={args.dry_run}")

    pc_count = _scan_price_checks(data_dir, args.dry_run)
    rfq_count = _scan_rfqs(data_dir, args.dry_run)

    verb = "would flip" if args.dry_run else "flipped"
    print(f"\n{verb}: {pc_count} PCs, {rfq_count} RFQs -> needs_review")
    return 0


if __name__ == "__main__":
    sys.exit(main())
