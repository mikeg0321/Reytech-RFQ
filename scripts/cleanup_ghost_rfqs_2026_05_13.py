"""One-shot cleanup — mark the two ghost PCs from 2026-05-13 as
status=duplicate retroactively.

Context: Mike marked sent his Mohammad@CDCR / PVSP / CCHCS bid
(sol 10846357) earlier today. The buyer's reply landed in the same
Gmail thread, but the ingest pipeline had no thread-based dedup gate
for either PCs or RFQs. Two new PCs spawned with RT-synthesized
sol#s:

  - pc_b283650e  →  RT-CCHCS-260513-b283650e  (7 items, 0/7 priced)
  - pc_5caf8e6f  →  RT-CCHCS-260513-5caf8e6f  (10 items, 0/10 priced)

The hotfix (fix/rfq-dedup-at-ingest-by-thread) adds
`_find_active_record_by_thread` + dedup branches in BOTH the PC and
RFQ paths of `_create_record`. That stops future buyer replies from
spawning fresh PCs/RFQs but doesn't retroactively clean the two
ghosts already in prod's queue.

This script does the cleanup. Run ONCE on prod after the hotfix
deploys.

Usage (prod):
    py scripts/cleanup_ghost_rfqs_2026_05_13.py --apply

Usage (dry-run, default):
    py scripts/cleanup_ghost_rfqs_2026_05_13.py

The script is idempotent — re-running after the first apply is a
no-op because the rows already carry status=duplicate.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

# The two PCs to mark duplicate. Identified by their pc_id (visible
# in the prod /home HTML at hotfix time as
# `oncontextmenu="showQueueContextMenu('pc','pc_b283650e',event)"`
# and the same for `pc_5caf8e6f`). Hard-coded — this is a surgical
# cleanup, not a generalized tool.
GHOST_TARGETS = [
    {
        "pc_id": "pc_b283650e",
        "expected_pc_number_prefix": "RT-CCHCS-260513-b283650e",
        "dedup_of": None,  # No canonical PC for sol 10846357 was found
                            # in the visible queue; the canonical record
                            # for Mike's submitted bid is the rfq side
                            # (RFQ e02b7fa6, sol 10846357). Leave None
                            # to signal "no surviving PC, just bad data".
        "reason": (
            "auto-dedup retroactive: PC spawned from Mohammad@CDCR "
            "buyer-reply on thread of rfq_e02b7fa6 (sol 10846357, PVSP). "
            "Pre-hotfix PC-side ingest had no thread dedup; the "
            "RT-CCHCS-260513-* sol# is synthesized and unique per "
            "ingest, so PR-N's dedup-by-pc_number couldn't catch it. "
            "Cleaned 2026-05-13 EOD by "
            "scripts/cleanup_ghost_rfqs_2026_05_13.py."
        ),
    },
    {
        "pc_id": "pc_5caf8e6f",
        "expected_pc_number_prefix": "RT-CCHCS-260513-5caf8e6f",
        "dedup_of": None,
        "reason": (
            "auto-dedup retroactive: PC spawned from Mohammad@CDCR "
            "buyer-reply on thread of rfq_e02b7fa6 (sol 10846357, PVSP). "
            "Pre-hotfix PC-side ingest had no thread dedup; the "
            "RT-CCHCS-260513-* sol# is synthesized and unique per "
            "ingest, so PR-N's dedup-by-pc_number couldn't catch it. "
            "Cleaned 2026-05-13 EOD by "
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
            pc_id = target["pc_id"]
            row = conn.execute(
                "SELECT id, pc_number, status, data_json "
                "FROM price_checks WHERE id = ?",
                (pc_id,),
            ).fetchone()
            if not row:
                print(f"  · {pc_id} not found — skipping")
                continue
            existing_pc_number = (row["pc_number"] or "")
            existing_status = (row["status"] or "").strip().lower()
            if not existing_pc_number.startswith(target["expected_pc_number_prefix"]):
                print(
                    f"  ⚠ {pc_id} pc_number={existing_pc_number!r} does not "
                    f"start with {target['expected_pc_number_prefix']!r} — "
                    "refusing to flip (sanity check failed). Operator can "
                    "still mark-duplicate via the UI."
                )
                continue
            if existing_status in ("duplicate", "deleted", "archived"):
                print(f"  · {pc_id} already {existing_status!r} — skipping")
                skipped += 1
                continue

            data = {}
            if row["data_json"]:
                try:
                    data = json.loads(row["data_json"])
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
            if target["dedup_of"]:
                data["dedup_of"] = target["dedup_of"]

            if args.apply:
                conn.execute(
                    "UPDATE price_checks SET status = ?, closed_at = ?, "
                    "data_json = ? WHERE id = ?",
                    ("duplicate", now, json.dumps(data, default=str), pc_id),
                )
                print(f"  ✓ {pc_id} flipped to duplicate (was {existing_status!r})")
            else:
                print(f"  (dry-run) would flip {pc_id} → duplicate "
                      f"(pc_number={existing_pc_number}, was {existing_status!r})")
            applied += 1
        if args.apply:
            conn.commit()

    print()
    if args.apply:
        print(f"DONE — applied {applied} flip(s), {skipped} already-duplicate skipped.")
    else:
        print(f"DRY-RUN — {applied} flip(s) would apply, "
              f"{skipped} already-duplicate.")
        print("Re-run with --apply to commit.")
    return 0 if applied + skipped > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
