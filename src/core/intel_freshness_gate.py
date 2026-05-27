"""Boot-time intelligence rebuild freshness gate (2026-05-27).

Closes the deploy-cycle lock cascade caught by Mr. Wolf's audit of prod
log 2026-05-27 06:01-06:08 (deployment 28d5adc7, sha f82f6025):

  06:01:46 Boot rebuild_intelligence_tables failed: database is locked
  06:04:54 upsert_quote R26Q47: database is locked
  06:05:24 DB transient error (attempt 1/3): database is locked
  06:05:34 Could not persist workflow run: database is locked
  06:05:55 DB transient error (attempt 2/3): database is locked
  06:06:27 DB failed after 3 attempts: record_price 'INSOLES...'
  06:06:57 DB transient error (attempt 1/3): database is locked
  06:08:03 DB failed after 3 attempts: record_price 'WAX STRIPS...'
  06:08:33 Failed to stamp PDF metadata: database is locked

The boot rebuild at `routes_intel_ops._boot_rebuild_awards` fires 60s
after every deploy and writes 38k+ rows across 5 intel tables in ~45s.
On a 2.2 GB DB this collides with workflow_runs, upsert_quote, and
record_price writers — even WAL + 30s busy_timeout + 3-attempt retry
can't absorb the contention. Five page routes time out in `make smoke`
(Quotes / Orders / Agents / Pipeline / Growth) as symptoms.

This gate eliminates the collision when intel tables are already
current. The post-pull rebuild at
`scprs_intelligence_engine.py:1077` fires after every scheduled SCPRS
pull (Mon 7am + Wed 10am PT) so intel tables stay current at the
3-4-day cadence. Boot rebuild is only needed for:
  (a) first-deploy / empty-volume bootstrap
  (b) post-outage catch-up (no scheduled pull in 7+ days)

Otherwise skip and let the scheduler do its work on a quiet cycle.

Lives in `src/core/` (not the route module) so unit tests can import
it directly without exec() namespace setup. Imports are minimal — only
stdlib and `src.core.paths.DATA_DIR` for default-path resolution.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone

# `from src.core` import doubles as the marker that
# `startup_checks.check_code_patterns` Pattern C uses to allow bare
# `sqlite3.connect(` calls (see startup_checks.py:192-205). Without it,
# this file would trip "direct sqlite3.connect without any db guard" —
# happened on prod boot of deployment 9f0e8924 (the PR #1138 deploy):
# `STARTUP FAIL: Code patterns — 1 code issues: core/
#  intel_freshness_gate.py: direct sqlite3.connect without any db guard`.
# This file IS the db guard for the freshness probe (single read-only
# SELECT with a hard 5s timeout), but the linter heuristic is broad.
from src.core.paths import DATA_DIR


def intel_tables_fresh(
    db_path: str | None = None,
    min_rows: int = 100,
    max_age_days: int = 7,
) -> bool:
    """Return True iff `scprs_awards` has enough recently-written rows
    that boot rebuild can be skipped.

    Cheap read-only query (one `SELECT COUNT + MAX`). **Fails CLOSED**
    on any error: if we can't prove freshness, return False so the
    boot rebuild runs and bootstraps the tables.

    Args:
      db_path: absolute path to reytech.db. When None (the common
        case), defaults to DATA_DIR/reytech.db. Tests pass an
        explicit tmp_path.
      min_rows: minimum row count to consider tables non-empty.
        Default 100 — first-deploy / empty-volume has < 100, so
        rebuild always runs to bootstrap.
      max_age_days: how recent the latest rebuild must be. Default 7 —
        scheduled pulls run Mon 7am + Wed 10am PT, gap of at most ~4
        days, so 7 days gives one missed scheduled cycle before boot
        rebuild forces a catch-up.

    Returns:
      True  → skip boot rebuild (tables fresh enough)
      False → run boot rebuild (empty / stale / error)

    `scprs_awards.created_at` is the canonical freshness timestamp —
    `scripts/run_scprs_harvest.py:build_scprs_awards` uses
    `INSERT OR REPLACE`, so `created_at` bumps on every rebuild (not
    just first-seen). `MAX(created_at)` therefore reflects the last
    successful rebuild.
    """
    if db_path is None:
        db_path = os.path.join(DATA_DIR, "reytech.db")
    try:
        with sqlite3.connect(db_path, timeout=5) as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n, MAX(created_at) AS u "
                "FROM scprs_awards"
            ).fetchone()
    except Exception:
        return False
    if row is None:
        return False
    row_count = int(row[0] or 0)
    last_updated = row[1]
    if row_count < min_rows or not last_updated:
        return False
    # ISO8601 timestamps lex-compare correctly for date freshness.
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=max_age_days)
    ).isoformat()
    return str(last_updated) >= cutoff
