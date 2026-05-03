"""Link orphan orders (no quote_number) to their paired quote by PO.

Background — `project_orphan_orders_finding.md` flagged 67/167 prod
orders with empty `quote_number`. PR #664's `ensure_quote_won_for_order`
hook only fires when a quote_number is already set, so orphans stay
invisible to recent-wins, win-rate-by-agency, and oracle calibration.

This script walks the orphans, links each by exact `po_number` match
(high confidence; multi-quote POs and total±1% windows are reported
but not auto-linked). On `--apply`, each link goes through
`order_dal.save_order` so the PR #664 hook fires automatically and
the paired quote flips open → won.

Usage:
    # Dry run (read-only, prints what would happen):
    python scripts/link_orphan_orders.py

    # Apply on local DB:
    python scripts/link_orphan_orders.py --apply

    # Apply on prod (via Railway):
    railway ssh "python scripts/link_orphan_orders.py --apply"

Safe to run repeatedly — orders that gained a quote_number on a prior
pass are no longer orphans, so they're skipped.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# Bootstrap: when invoked as `python scripts/link_orphan_orders.py`,
# the repo root isn't on sys.path. Add the parent of scripts/ so this
# script works the same way locally, in CI, or via `railway ssh`.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _format_report(report: dict, verbose: bool) -> str:
    out: list[str] = []
    out.append(
        f"orphan_count={report['orphan_count']} "
        f"linkable={len(report['linked'])} "
        f"ambiguous={len(report['ambiguous'])} "
        f"no_po={len(report['no_po'])} "
        f"no_match={len(report['no_match'])}"
    )
    if report.get("dry_run"):
        out.append("(dry-run — no writes)")
    else:
        out.append(f"applied_count={report.get('applied_count', 0)}")
    if verbose:
        if report["linked"]:
            out.append("\n-- Linked (or would link):")
            for e in report["linked"]:
                out.append(
                    f"  {e['order_id']:30s} -> {e['quote_number']:20s} "
                    f"(po={e['po']})"
                )
        if report["ambiguous"]:
            out.append("\n-- Ambiguous (multi-quote PO, NOT auto-linked):")
            for e in report["ambiguous"]:
                out.append(
                    f"  {e['order_id']:30s} po={e['po']} "
                    f"matches={e['match_count']}"
                )
        if report["no_match"]:
            out.append(
                f"\n-- No-match (orphan PO has no paired quote): "
                f"{len(report['no_match'])} rows"
            )
        if report["no_po"]:
            out.append(
                f"-- No-po (orphan order has no PO either): "
                f"{len(report['no_po'])} rows"
            )
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Actually write updates. Default is dry-run.")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="List every linked / ambiguous order.")
    parser.add_argument("--actor", default="link_orphan_orders",
                        help="Actor string for audit log entries.")
    parser.add_argument("--json", action="store_true",
                        help="Emit the full report as JSON instead of "
                        "the human-readable summary.")
    args = parser.parse_args()

    from src.core.orders_link_orphans import link_orphan_orders
    try:
        report = link_orphan_orders(dry_run=not args.apply, actor=args.actor)
    except Exception as e:
        # Most common failure on a fresh DB: tables not yet created
        # (init_db hasn't run). Surface a one-line message rather than
        # a 30-line stack trace so the operator can redirect.
        print(f"link_orphan_orders failed: {e}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(_format_report(report, verbose=args.verbose))

    # Exit 0 always — the script's job is to report + (optionally) apply.
    # Failures inside apply are logged at WARN/ERROR but don't fail the
    # whole run, since we want to make as many safe links as we can.
    return 0


if __name__ == "__main__":
    sys.exit(main())
