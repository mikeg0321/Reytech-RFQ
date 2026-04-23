"""Heal stale `unit_price` on PC + RFQ line items.

PC-1 audit + 2026-04-23 prod re-audit: every line item saved BEFORE
PR #321 (recompute_unit_price on cost/markup edits) carries a stale
`unit_price` that disagrees with the live cost×markup derivation. The
UI renders the live number; the email body + generated PDF read the
stale one; the buyer gets a different price than what the operator saw.

Live evidence (pc_f7ba7a6b, 2026-04-23): UI $567.79, email $558.48 —
a $148.96 under-quote on that single 16-unit line.

This script scans every PC and RFQ, identifies items where the persisted
`unit_price` diverges from the canonical cost×markup derivation, and
overwrites the stale value. Never touches items missing cost or markup
(the derivation has nothing to stand on).

Safe defaults:
  * Dry-run by default — prints a report of what would change.
  * `--apply` required to commit.
  * Exit 0 = no-op / success, 1 = runtime error, 2 = missing DB.
  * Writes go through the same DAL writers the UI uses
    (`_save_single_pc` / `_save_single_rfq`) so SQLite + JSON stay in
    lock-step.
  * Limited to the PC + RFQ data paths — does NOT touch quote, order,
    revenue_log persisted prices (those are point-in-time snapshots).

Usage:
  python scripts/backfill_unit_price.py              # dry-run
  python scripts/backfill_unit_price.py --apply      # commit
  python scripts/backfill_unit_price.py --only pc    # scope to PCs
  python scripts/backfill_unit_price.py --only rfq   # scope to RFQs
  python scripts/backfill_unit_price.py --db /tmp/x.db  # override DB
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Iterable


_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.core.pricing_math import canonical_unit_price, is_unit_price_stale  # noqa: E402


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_db_path(override: str | None) -> str | None:
    if override:
        return override
    for p in ("/data/reytech.db", "data/reytech.db"):
        if os.path.exists(p):
            return p
    return None


def _load_items_from_row(row: sqlite3.Row) -> tuple[dict, list]:
    raw = row["data_json"] or "{}"
    try:
        blob = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except (ValueError, TypeError):
        blob = {}
    if not isinstance(blob, dict):
        blob = {}
    items = blob.get("items") or blob.get("line_items") or []
    if isinstance(items, str):
        try:
            items = json.loads(items)
        except (ValueError, TypeError):
            items = []
    if not isinstance(items, list):
        items = []
    return blob, items


def _scan(conn: sqlite3.Connection, kind: str) -> list[dict]:
    conn.row_factory = sqlite3.Row
    table = "price_checks" if kind == "pc" else "rfqs"
    num_col = "pc_number" if kind == "pc" else "rfq_number"
    rows = conn.execute(
        f"SELECT id, status, data_json, {num_col} AS num FROM {table}"
    ).fetchall()
    out: list[dict] = []
    for row in rows:
        blob, items = _load_items_from_row(row)
        stale_lines = []
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            if not is_unit_price_stale(item):
                continue
            stored = item.get("unit_price")
            derived = canonical_unit_price(item)
            qty = item.get("qty") or 1
            try:
                qty_f = float(qty)
            except (TypeError, ValueError):
                qty_f = 1.0
            try:
                stored_f = float(stored or 0)
            except (TypeError, ValueError):
                stored_f = 0.0
            gap = derived - stored_f
            stale_lines.append({
                "idx": idx,
                "desc": (item.get("description") or "")[:50],
                "qty": qty_f,
                "stored": stored_f,
                "derived": derived,
                "gap_per_unit": round(gap, 2),
                "gap_total": round(gap * qty_f, 2),
            })
        if stale_lines:
            out.append({
                "kind": kind, "id": row["id"], "num": row["num"],
                "status": row["status"],
                "blob": blob, "items": items, "stale_lines": stale_lines,
            })
    return out


def _heal(conn: sqlite3.Connection, rec: dict) -> None:
    blob = rec["blob"]
    items = rec["items"]
    now_iso = _utc_iso()
    for line in rec["stale_lines"]:
        idx = line["idx"]
        item = items[idx]
        item["unit_price"] = line["derived"]
        pricing = item.setdefault("pricing", {}) if isinstance(item.get("pricing"), dict) else {}
        if not isinstance(pricing, dict):
            item["pricing"] = {}
            pricing = item["pricing"]
        pricing["recommended_price"] = line["derived"]
        item["_unit_price_backfilled_at"] = now_iso
        item["_unit_price_backfilled_prior"] = line["stored"]
    blob["items"] = items
    if "line_items" in blob:
        blob["line_items"] = items
    blob["_unit_price_backfilled_at"] = now_iso
    table = "price_checks" if rec["kind"] == "pc" else "rfqs"
    conn.execute(
        f"UPDATE {table} SET data_json=?, updated_at=? WHERE id=?",
        (json.dumps(blob, default=str), now_iso, rec["id"]),
    )


def run(db_path: str | None, *, apply: bool = False,
        only: str | None = None) -> int:
    resolved = _resolve_db_path(db_path)
    if not resolved:
        print("ERROR: no reytech.db found (tried /data/reytech.db, data/reytech.db)",
              file=sys.stderr)
        return 2
    if not os.path.exists(resolved):
        print(f"ERROR: DB not found: {resolved}", file=sys.stderr)
        return 2

    kinds: Iterable[str] = ("pc", "rfq")
    if only:
        if only not in ("pc", "rfq"):
            print(f"ERROR: --only must be 'pc' or 'rfq', got {only!r}",
                  file=sys.stderr)
            return 1
        kinds = (only,)

    print(f"{'APPLY' if apply else 'DRY-RUN'} backfill_unit_price on {resolved}")
    conn = sqlite3.connect(resolved)
    try:
        total_records = 0
        total_lines = 0
        total_gap = 0.0
        for kind in kinds:
            try:
                records = _scan(conn, kind)
            except sqlite3.OperationalError as e:
                print(f"  skip {kind}: {e}")
                continue
            print(f"\n[{kind.upper()}] {len(records)} record(s) with stale unit_price:")
            for rec in records:
                line_count = len(rec["stale_lines"])
                rec_gap = sum(l["gap_total"] for l in rec["stale_lines"])
                total_gap += rec_gap
                total_lines += line_count
                print(f"  {kind.upper()} {rec['id'][:12]:12s} "
                      f"num={rec['num'] or '—':14s} "
                      f"status={rec['status']:12s} "
                      f"{line_count} line(s), total gap={rec_gap:+.2f}")
                for line in rec["stale_lines"][:5]:
                    print(f"      line {line['idx']+1}: stored=${line['stored']:.2f} "
                          f"derived=${line['derived']:.2f} "
                          f"gap={line['gap_per_unit']:+.2f}/unit × {line['qty']:g} "
                          f"= {line['gap_total']:+.2f}  "
                          f"{line['desc']}")
                if line_count > 5:
                    print(f"      ... {line_count - 5} more")
                if apply:
                    _heal(conn, rec)
            total_records += len(records)
        if apply:
            conn.commit()
            print(f"\n✓ Applied: healed {total_lines} line(s) across "
                  f"{total_records} record(s). Aggregate gap closed: "
                  f"${total_gap:+.2f}.")
        else:
            print(f"\nDry-run: would heal {total_lines} line(s) across "
                  f"{total_records} record(s). Aggregate gap: "
                  f"${total_gap:+.2f}. Pass --apply to commit.")
        return 0
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--apply", action="store_true",
                   help="Commit changes. Default is dry-run.")
    p.add_argument("--only", choices=("pc", "rfq"),
                   help="Limit scan to PCs or RFQs only.")
    p.add_argument("--db", default=None,
                   help="Override DB path (default auto-detects /data or ./data).")
    args = p.parse_args(argv)
    return run(args.db, apply=args.apply, only=args.only)


if __name__ == "__main__":
    sys.exit(main())
