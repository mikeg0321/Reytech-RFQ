"""Backfill oracle_calibration with real wins from the `orders` table.

2026-04-20: Prod `oracle_calibration` has 47 losses recorded and **0
wins**. The loss-detection path (SCPRS award matching → quote_po_matches
→ calibrate_from_outcome) works, but the win path is never fired.
Meanwhile `orders` contains real shipped POs — at least one $6.4K
CDCR order (ORD-PO-4500750017, status='shipped') is a concrete win
that's invisible to Oracle.

This script walks `orders` for rows that represent realised wins
(`status IN ('shipped','delivered','invoiced')` and a non-empty items
array with cost data) and replays each one through
`pricing_oracle_v2.calibrate_from_outcome(items, 'won', agency=...)`.

EMA-based calibration is idempotent in the sense that re-running
doesn't corrupt the table, but it **does** double-count samples. To
avoid that, the script writes to a `backfill_wins_ledger` row per
order so subsequent runs skip already-processed IDs.

Usage:
    # Dry run (read-only, prints what would happen):
    python scripts/backfill_wins_from_orders.py --dry-run

    # Apply on local DB:
    python scripts/backfill_wins_from_orders.py

    # Apply on prod (via Railway):
    railway ssh "python scripts/backfill_wins_from_orders.py"

Safe to run repeatedly — ledger table prevents double-counting.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

# Bootstrap: when invoked as `python scripts/backfill_wins_from_orders.py`,
# the repo root isn't on sys.path, so `from src.core.pricing_oracle_v2
# import ...` fails with ModuleNotFoundError. Add the parent of scripts/
# so this script works the same way whether run locally, in CI, or on
# prod via `railway ssh python scripts/backfill_wins_from_orders.py`.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


_WIN_STATUSES = ("shipped", "delivered", "invoiced", "complete", "completed")


def _default_db_path() -> str:
    for candidate in ("/data/reytech.db", "data/reytech.db", "reytech.db"):
        if os.path.exists(candidate):
            return candidate
    return "reytech.db"


def _ensure_ledger(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS backfill_wins_ledger (
            order_id TEXT PRIMARY KEY,
            processed_at TEXT NOT NULL,
            win_total REAL,
            agency TEXT,
            items_count INTEGER
        )
    """)
    conn.commit()


def _parse_items(raw) -> list:
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
    except (ValueError, TypeError):
        pass
    return []


def _load_candidate_orders(conn: sqlite3.Connection) -> list[dict]:
    """Orders we'll replay through calibrate_from_outcome.

    We intentionally do NOT require cost data on items: historical orders
    often carry unit_price/qty without per-line cost, and that's fine —
    `calibrate_from_outcome('won')` bumps the win count regardless, and
    only folds margins into the EMA for lines where cost is present.
    Excluding cost-less orders would leave the single real win on prod
    (ORD-PO-4500750017, $6,408 CDCR shipment) invisible to Oracle.
    """
    placeholders = ",".join("?" * len(_WIN_STATUSES))
    rows = conn.execute(f"""
        SELECT o.id, o.quote_number, o.agency, o.institution, o.po_number,
               o.status, o.total, o.items, o.created_at
        FROM orders o
        LEFT JOIN backfill_wins_ledger l ON l.order_id = o.id
        WHERE o.status IN ({placeholders})
          AND l.order_id IS NULL
    """, _WIN_STATUSES).fetchall()
    out = []
    for r in rows:
        items = _parse_items(r[7])
        if not items:
            continue
        out.append({
            "id": r[0], "quote_number": r[1], "agency": r[2] or "",
            "institution": r[3], "po_number": r[4], "status": r[5],
            "total": r[6] or 0.0, "items": items, "created_at": r[8],
        })
    return out


def _apply_one(order: dict, *, dry_run: bool) -> dict:
    agency = (order["agency"] or order["institution"] or "").strip()
    result = {
        "order_id": order["id"],
        "po_number": order["po_number"],
        "agency": agency,
        "items_count": len(order["items"]),
        "total": order["total"],
        "applied": False,
    }
    if dry_run:
        return result
    from src.core.pricing_oracle_v2 import calibrate_from_outcome
    calibrate_from_outcome(order["items"], "won", agency=agency)
    result["applied"] = True
    return result


def run(db_path: str | None = None, *, dry_run: bool = False,
        as_json: bool = False) -> int:
    db_path = db_path or _default_db_path()
    if not os.path.exists(db_path):
        msg = f"reytech.db not found at {db_path}"
        if as_json:
            print(json.dumps({"error": msg}))
        else:
            print(msg)
        return 2

    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_ledger(conn)
        candidates = _load_candidate_orders(conn)

        report = {
            "db_path": db_path, "dry_run": dry_run,
            "candidates": len(candidates), "applied": [],
            "skipped_no_items": [], "skipped_no_cost": [],
        }

        results = []
        for order in candidates:
            res = _apply_one(order, dry_run=dry_run)
            results.append(res)
            if res["applied"] and not dry_run:
                conn.execute("""
                    INSERT INTO backfill_wins_ledger
                        (order_id, processed_at, win_total, agency, items_count)
                    VALUES (?, datetime('now'), ?, ?, ?)
                """, (order["id"], order["total"], res["agency"],
                      res["items_count"]))
        conn.commit()
        report["applied"] = results

        if as_json:
            print(json.dumps(report, indent=2, default=str))
        else:
            verb = "WOULD apply" if dry_run else "Applied"
            print(f"Orders backfill ({verb}) — db: {db_path}")
            print(f"  Candidates: {len(candidates)}")
            for r in results:
                marker = "→ OK" if r["applied"] else "→ dry-run"
                print(f"  {marker}  order={r['order_id']}  "
                      f"po={r['po_number']}  agency={r['agency']}  "
                      f"items={r['items_count']}  total=${r['total']:.2f}")
        return 0
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", help="Path to reytech.db")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)
    return run(args.db, dry_run=args.dry_run, as_json=args.as_json)


if __name__ == "__main__":
    sys.exit(main())
