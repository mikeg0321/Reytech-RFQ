"""One-time backfill: flip quotes paired with orders to status='won'.

Background — `project_orders_quotes_drift_inverse.md`. PR #664 wired the
forward-fix hook into `save_order` so every NEW order with a
`quote_number` flips its paired quote to 'won'. Older orders predating
PR #664 (and any orders whose quote was created via SCPRS reconcile
stubs from PRs #641-#644) still leave their paired quotes in 'sent' /
'open' / 'pending', under-reporting wins on recent-wins, win-rate-by-
agency, and oracle calibration.

This script runs the one-time historical backfill. Read-only by
default — dry-run prints the would-be flip list. `--apply` runs the
real flip via `ensure_quote_won_for_order`, which itself writes an
audit-log entry per flip.

Statuses NOT flipped:
  * 'won'                              — already done, no-op
  * 'lost' / 'cancelled' / 'voided' /  — operator decided otherwise;
    'deleted'                            don't second-guess

Usage:
    # Dry run (read-only):
    python scripts/backfill_orders_quotes_drift.py
    python scripts/backfill_orders_quotes_drift.py --json
    python scripts/backfill_orders_quotes_drift.py --verbose

    # Apply:
    python scripts/backfill_orders_quotes_drift.py --apply

    # Apply on prod via Railway:
    railway ssh "python scripts/backfill_orders_quotes_drift.py --apply"

Safe to run repeatedly — once a quote is 'won', subsequent runs skip
it. Quotes in final non-won states ('lost' etc.) are likewise skipped.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# Bootstrap: when invoked as `python scripts/...`, repo root isn't on
# sys.path. Add the parent of scripts/ so this works the same way
# locally, in CI, or via `railway ssh`.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _format_report(report: dict, verbose: bool) -> str:
    out: list[str] = []
    out.append(
        f"examined={report['examined']} "
        f"would_flip={len(report['flipped'])} "
        f"already_won={report['skipped_already_won']} "
        f"skipped_final={len(report['skipped_final'])} "
        f"errors={len(report['errors'])}"
    )
    if report.get("dry_run"):
        out.append("(dry-run — no writes)")
    else:
        out.append(f"flipped_count={len(report['flipped'])}")

    if verbose:
        if report["flipped"]:
            label = "Would flip" if report.get("dry_run") else "Flipped"
            out.append(f"\n-- {label} ({len(report['flipped'])}):")
            for q in report["flipped"]:
                out.append(f"  {q}")
        if report["skipped_final"]:
            out.append(
                f"\n-- Skipped (final non-won status): "
                f"{len(report['skipped_final'])}"
            )
            for qn, st in report["skipped_final"][:20]:
                out.append(f"  {qn}  status={st}")
            if len(report["skipped_final"]) > 20:
                out.append(f"  ... and {len(report['skipped_final']) - 20} more")
        if report["errors"]:
            out.append(f"\n-- Errors ({len(report['errors'])}):")
            for qn, err in report["errors"]:
                out.append(f"  {qn}: {err}")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Actually flip quotes. Default is dry-run.")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="List every quote_number flipped / skipped.")
    parser.add_argument("--actor", default="orders_quotes_drift_backfill",
                        help="Actor for the per-quote audit log entries.")
    parser.add_argument("--json", action="store_true",
                        help="Emit the full report as JSON.")
    args = parser.parse_args()

    from src.core.quotes_backfill import backfill_orders_quotes_drift
    try:
        report = backfill_orders_quotes_drift(
            dry_run=not args.apply, actor=args.actor,
        )
    except Exception as e:
        # Most common failure on a fresh DB: tables not yet created.
        # Surface a one-line message rather than a stack trace.
        print(f"backfill_orders_quotes_drift failed: {e}", file=sys.stderr)
        return 1

    if not report.get("ok", True):
        print(f"backfill failed: {report.get('error')}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(_format_report(report, verbose=args.verbose))

    return 0


if __name__ == "__main__":
    sys.exit(main())
