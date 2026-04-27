#!/usr/bin/env python
"""build_training_corpus.py — Phase 1.6 PR3g.

One-shot bootstrap: walks orders for last 365 days, joins to incoming
attachments + sent po_pdf_path, writes one labeled training pair per
won PO to data/training_pairs/<quote_id>/.

Usage:
    python scripts/build_training_corpus.py [--days 365] [--limit N] [--force]

Idempotent: re-runs skip already-built pairs unless --force is passed.
Per-buyer coverage report printed at the end + written to
data/training_pairs/_coverage_report.json.
"""

import argparse
import json
import os
import sys

# Allow running as a script from any cwd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.agents.training_corpus import bootstrap_from_orders, coverage_report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--days", type=int, default=365,
                   help="Lookback window in days (default 365)")
    p.add_argument("--limit", type=int, default=None,
                   help="Cap orders processed (for dev iteration)")
    p.add_argument("--force", action="store_true",
                   help="Rewrite existing manifests")
    args = p.parse_args()

    print(f"Bootstrapping training corpus (last {args.days} days)…")
    summary = bootstrap_from_orders(days=args.days, force=args.force,
                                    limit=args.limit)

    print()
    print("=" * 60)
    print("BOOTSTRAP SUMMARY")
    print("=" * 60)
    for k in ("scanned", "created", "skipped_exists",
              "skipped_no_data", "skipped_no_artifacts", "errors"):
        print(f"  {k:25s}: {summary.get(k, 0)}")
    print()
    print("PER-BUYER (top 15 by created):")
    by_agency = summary.get("by_agency", {})
    rows = sorted(by_agency.items(),
                  key=lambda x: -x[1].get("created", 0))[:15]
    for agency, s in rows:
        print(f"  {agency:30s}: {s.get('created',0):4d} created / "
              f"{s.get('scanned',0):4d} scanned")

    # On-disk coverage (cumulative — includes prior runs)
    cov = coverage_report()
    print()
    print(f"ON-DISK CORPUS: {cov['total_pairs']} total pairs at "
          f"{cov['training_root']}")

    # Write report file
    report_path = os.path.join(cov["training_root"], "_coverage_report.json")
    try:
        os.makedirs(cov["training_root"], exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump({"bootstrap": summary, "on_disk": cov}, f, indent=2)
        print(f"  report written: {report_path}")
    except OSError as e:
        print(f"  report write failed: {e}")


if __name__ == "__main__":
    main()
