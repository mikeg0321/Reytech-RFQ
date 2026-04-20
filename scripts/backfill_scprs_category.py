#!/usr/bin/env python3
"""One-shot backfill: repair won_quotes.category rows corrupted by the
pre-#225 SCPRS sync, which wrote p.start_date (ISO YYYY-MM-DD) into the
category column instead of l.category.

Dry run by default. Pass --apply to commit.

    python scripts/backfill_scprs_category.py           # dry run
    python scripts/backfill_scprs_category.py --apply   # write

On Railway:
    railway run python scripts/backfill_scprs_category.py --apply
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.knowledge.won_quotes_db import _get_db_conn, classify_category  # noqa: E402

ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def backfill(dry_run: bool = True) -> dict:
    stats = {"scanned": 0, "affected": 0, "fixed": 0, "source_missing": 0}
    conn = _get_db_conn()
    try:
        rows = conn.execute(
            "SELECT id, description, category FROM won_quotes "
            "WHERE source='scprs_sync' AND id LIKE 'wq_scprs_%'"
        ).fetchall()
        stats["scanned"] = len(rows)

        for r in rows:
            cat = r["category"] or ""
            if not ISO_DATE.match(cat):
                continue
            stats["affected"] += 1

            try:
                line_id = int(r["id"].replace("wq_scprs_", ""))
            except ValueError:
                continue

            src = conn.execute(
                "SELECT category FROM scprs_po_lines WHERE id=?", (line_id,)
            ).fetchone()

            new_cat = (src["category"] if src and src["category"] else None) \
                or classify_category(r["description"] or "")

            if not src:
                stats["source_missing"] += 1

            if not dry_run:
                conn.execute(
                    "UPDATE won_quotes SET category=? WHERE id=?",
                    (new_cat, r["id"]),
                )
            stats["fixed"] += 1

        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    return stats


def main() -> int:
    apply = "--apply" in sys.argv
    stats = backfill(dry_run=not apply)
    verb = "fixed" if apply else "would fix"
    print(
        f"scprs_sync rows scanned={stats['scanned']} "
        f"affected={stats['affected']} {verb}={stats['fixed']} "
        f"source_row_missing={stats['source_missing']}"
    )
    if not apply:
        print("dry run — pass --apply to write")
    return 0


if __name__ == "__main__":
    sys.exit(main())
