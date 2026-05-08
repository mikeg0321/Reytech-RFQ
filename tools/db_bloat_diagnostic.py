#!/usr/bin/env python3
"""tools/db_bloat_diagnostic.py — Tier 2d Phase 1 (audit 2026-05-07).

Reports per-table row counts + retention-policy applicability for the
SQLite DB at `data/reytech.db`. Read-only.

Usage:
    python tools/db_bloat_diagnostic.py
        # prints a sorted table to stdout. Exits 0 always.

What it shows:
    * Total DB size (page_size * page_count).
    * Per-table row count, sorted descending.
    * Whether each retention-allowlist table is `auto_purge` or
      `compliance_only`.
    * The default retention-days from RETENTION_POLICY (or "—" if
      no policy).
    * A `would_delete` column for retention-allowlist tables, computed
      via `purge_older_than(table, RETENTION_POLICY[table], dry_run=True)`.

What it does NOT do:
    * Touch any rows. Read-only by construction.
    * Arm a cron. That's Phase 2.

Operator workflow (per handoff):
    1. Run this script in prod via `railway run` to see the bloat shape.
    2. Decide retention policy with Mike (defaults in RETENTION_POLICY
       are recommendations, not gospel).
    3. Open a follow-up PR that wires `purge_older_than` into the
       scheduler with the agreed-upon days, gated behind a feature
       flag for the first deploy.
"""
from __future__ import annotations

import sys
from pathlib import Path


def _format_bytes(n: int) -> str:
    if n < 0:
        return "?"
    if n < 1024:
        return f"{n}B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f}KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f}MB"
    return f"{n / (1024 * 1024 * 1024):.2f}GB"


def main(argv: list[str]) -> int:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))

    from src.core.db_retention import (
        bloat_report, purge_older_than,
        AUTO_PURGE_ALLOWLIST, COMPLIANCE_OPT_IN_ALLOWLIST,
        RETENTION_POLICY,
    )

    report = bloat_report()
    if "error" in report:
        print(f"ERROR: {report['error']}", file=sys.stderr)
        return 0  # never block — diagnostic is fail-open by design

    print()
    print("Reytech DB bloat diagnostic")
    print("=" * 78)
    print(f"  Total size: {_format_bytes(report.get('total_bytes', 0))} "
          f"({report.get('total_pages', 0):,} pages × "
          f"{report.get('page_size', 0)}B)")
    print()
    print(f"  {'TABLE':<32} {'ROWS':>10}  {'POLICY':<18} "
          f"{'WOULD_DELETE':>12}")
    print(f"  {'-'*32} {'-'*10}  {'-'*18} {'-'*12}")

    purge_targets = (AUTO_PURGE_ALLOWLIST
                     | COMPLIANCE_OPT_IN_ALLOWLIST)

    for tbl in report.get("tables", []):
        name = tbl["name"]
        rows = tbl["rows"]

        if tbl["auto_purge"]:
            policy = f"auto / {RETENTION_POLICY.get(name, '?')}d"
        elif tbl["compliance_only"]:
            policy = f"opt-in / {RETENTION_POLICY.get(name, '?')}d"
        else:
            policy = "—"

        # `would_delete` only meaningful for tables under retention.
        would = ""
        if name in purge_targets:
            try:
                kwargs = {"dry_run": True}
                if name in COMPLIANCE_OPT_IN_ALLOWLIST:
                    kwargs["force_compliance_table"] = True
                r = purge_older_than(
                    name, RETENTION_POLICY[name], **kwargs)
                would = f"{r.get('would_delete', 0):>12,}"
            except Exception as e:
                would = f"err: {type(e).__name__}"

        print(f"  {name[:32]:<32} {rows:>10,}  {policy:<18} {would:>12}")

    print()
    print("Notes:")
    print("  * `auto` tables are eligible for the cron when Phase 2 ships.")
    print("  * `opt-in` tables (lifecycle_events) require explicit")
    print("    confirmation from Mike/compliance before being scheduled.")
    print("  * `WOULD_DELETE` shows what a retention pass at the default")
    print("    policy days would remove TODAY. Re-run periodically.")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
