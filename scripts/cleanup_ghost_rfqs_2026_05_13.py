"""One-shot cleanup — mark the two ghost RFQs from 2026-05-13 as
status=duplicate retroactively.

Context: Mike marked sent RFQ e02b7fa6 (sol 10846357, Mohammad@CDCR,
CCHCS, PVSP) earlier today. The buyer's reply landed in the same
Gmail thread, but the ingest pipeline's RFQ branch had no dedup gate.
Two new RFQs spawned with RT-synthesized sol#s:

  - RT-CCHCS-260513-b283650e (7 items, 0/7 priced)
  - RT-CCHCS-260513-5caf8e6f (10 items, 0/10 priced)

The hotfix (fix/rfq-dedup-at-ingest-by-thread) adds
`_find_active_rfq_by_thread` + the dedup branch in `_create_record`.
That stops future buyer replies from spawning fresh RFQs but doesn't
retroactively clean the two ghosts already in prod's queue.

This script does the cleanup. Run ONCE on prod after the hotfix deploys.

Usage (prod):
    py scripts/cleanup_ghost_rfqs_2026_05_13.py --apply

Usage (dry-run, default):
    py scripts/cleanup_ghost_rfqs_2026_05_13.py

The script is idempotent — re-running after the first apply is a no-op
because the rows already carry status=duplicate.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

# The two RFQs to mark duplicate, with their dedup target (the canonical
# RFQ Mike actually shipped). Hard-coded — this is a surgical cleanup,
# not a generalized tool.
GHOST_TARGETS = [
    {
        "sol_number_prefix": "RT-CCHCS-260513-b283650e",
        "dedup_of": "rfq_e02b7fa6",  # the canonical RFQ Mike marked sent
        "reason": (
            "auto-dedup retroactive: buyer reply on thread of rfq_e02b7fa6 "
            "(sol 10846357, Mohammad@CDCR/PVSP) — spawned during pre-hotfix "
            "RFQ ingest gap. Cleaned 2026-05-13 EOD by "
            "scripts/cleanup_ghost_rfqs_2026_05_13.py."
        ),
    },
    {
        "sol_number_prefix": "RT-CCHCS-260513-5caf8e6f",
        "dedup_of": "rfq_e02b7fa6",
        "reason": (
            "auto-dedup retroactive: buyer reply on thread of rfq_e02b7fa6 "
            "(sol 10846357, Mohammad@CDCR/PVSP) — spawned during pre-hotfix "
            "RFQ ingest gap. Cleaned 2026-05-13 EOD by "
            "scripts/cleanup_ghost_rfqs_2026_05_13.py."
        ),
    },
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply", action="store_true",
        help="Commit the status flips (default: dry-run)",
    )
    args = parser.parse_args()

    from src.core.db import get_db
    now = datetime.now().isoformat()
    applied = 0
    skipped = 0

    with get_db() as conn:
        for target in GHOST_TARGETS:
            prefix = target["sol_number_prefix"]
            rows = conn.execute(
                "SELECT id, rfq_number, solicitation_number, status "
                "FROM rfqs "
                "WHERE rfq_number LIKE ? OR solicitation_number LIKE ?",
                (f"{prefix}%", f"{prefix}%"),
            ).fetchall()
            if not rows:
                print(f"  · no row found matching {prefix!r}")
                continue
            for r in rows:
                rid = r["id"]
                existing_status = (r["status"] or "").strip().lower()
                if existing_status in ("duplicate", "deleted", "archived"):
                    print(f"  · {rid} already {existing_status} — skipping")
                    skipped += 1
                    continue
                hist_row = conn.execute(
                    "SELECT data_json FROM rfqs WHERE id = ?", (rid,),
                ).fetchone()
                data = {}
                if hist_row and hist_row["data_json"]:
                    try:
                        data = json.loads(hist_row["data_json"])
                    except Exception:
                        data = {}
                hist = data.get("status_history") or []
                hist.append({
                    "from": existing_status or "parsed",
                    "to": "duplicate",
                    "at": now,
                    "actor": "cleanup_ghost_rfqs_2026_05_13",
                    "reason": target["reason"],
                })
                data["status_history"] = hist
                data["status"] = "duplicate"
                data["closed_reason"] = target["reason"]
                data["closed_at"] = now
                data["dedup_of"] = target["dedup_of"]

                if args.apply:
                    conn.execute(
                        "UPDATE rfqs SET status = ?, closed_at = ?, "
                        "data_json = ? WHERE id = ?",
                        ("duplicate", now, json.dumps(data, default=str), rid),
                    )
                    print(f"  ✓ {rid} flipped to duplicate (was {existing_status!r})")
                else:
                    print(f"  (dry-run) would flip {rid} → duplicate (was {existing_status!r})")
                applied += 1
        if args.apply:
            conn.commit()

    print()
    if args.apply:
        print(f"DONE — applied {applied} flip(s), {skipped} already-duplicate skipped.")
    else:
        print(f"DRY-RUN — {applied} flip(s) would apply, {skipped} already-duplicate.")
        print("Re-run with --apply to commit.")
    return 0 if applied + skipped > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
