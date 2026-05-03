"""Read-only ghost scan over the `quotes` SQLite table.

Background — `project_session_2026_05_01_ghost_quote_arc.md` flagged
504 quote-table entries that hadn't been audited for ghost-bound
seqs. PR #675 + PR #699 stopped *new* ghost burns from happening but
don't backfill clean rows that were burned before the gates landed.

This script walks every non-test quote, classifies by ghost markers,
and prints a bucketed report. **It never writes.** Per
`project_orphan_orders_finding.md` and Mike's note in the session
memo: "Don't auto-delete — that touches financial history." Per-row
clearance needs Mike's review.

Usage:
    python scripts/quotes_ghost_scan.py
    python scripts/quotes_ghost_scan.py --json         # machine-readable
    python scripts/quotes_ghost_scan.py --verbose      # list every flagged row
    python scripts/quotes_ghost_scan.py --limit 50     # spot-check first 50
    python scripts/quotes_ghost_scan.py --include-test # include is_test=1 rows

Run on prod via:
    railway ssh "python scripts/quotes_ghost_scan.py"

Safe to run repeatedly — fully read-only.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# Bootstrap: when invoked as `python scripts/quotes_ghost_scan.py`,
# the repo root isn't on sys.path. Add the parent of scripts/ so this
# script works the same way locally, in CI, or via `railway ssh`.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _format_report(report: dict, verbose: bool) -> str:
    out: list[str] = []
    out.append(
        f"total_quotes={report['total_quotes']} "
        f"ghost={report['ghost_count']} "
        f"clean={report['clean_count']}"
    )

    bb = report["by_bucket"]
    out.append(
        f"  placeholder_source={len(bb['placeholder_source'])}  "
        f"orphaned_source={len(bb['orphaned_source'])}  "
        f"own_markers={len(bb['own_markers'])}  "
        f"no_source={len(bb['no_source'])}"
    )

    sk = report["by_source_kind"]
    if sk:
        kinds = ", ".join(f"{k}={v}" for k, v in sorted(sk.items()))
        out.append(f"  by source kind: {kinds}")

    if verbose:
        for bucket_name in ("placeholder_source", "orphaned_source",
                            "own_markers", "no_source"):
            entries = bb[bucket_name]
            if not entries:
                continue
            out.append(f"\n-- {bucket_name} ({len(entries)}):")
            for e in entries:
                line = (
                    f"  {e['quote_number']:14s}  "
                    f"src={e['source_kind']:10s}  "
                    f"agency={(e.get('agency') or '?')[:14]:14s}  "
                    f"total=${float(e.get('total') or 0):>9,.2f}  "
                    f"status={(e.get('status') or '?')[:12]:12s}"
                )
                out.append(line)
                for r in e["reasons"]:
                    out.append(f"      · {r}")

    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--json", action="store_true",
                        help="Emit the full report as JSON.")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="List every flagged quote with its reasons.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only walk the first N quotes (most recent first).")
    parser.add_argument("--include-test", action="store_true",
                        help="Include is_test=1 rows in the scan.")
    args = parser.parse_args()

    from src.core.quotes_ghost_scan import scan_quotes
    try:
        report = scan_quotes(
            include_test=args.include_test,
            limit=args.limit,
        )
    except Exception as e:
        # Most common failure on a fresh DB: tables not yet created.
        # Surface a one-line message rather than a stack trace.
        print(f"quotes_ghost_scan failed: {e}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(_format_report(report, verbose=args.verbose))

    return 0


if __name__ == "__main__":
    sys.exit(main())
