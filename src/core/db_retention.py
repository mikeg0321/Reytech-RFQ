"""Retention substrate for log-style tables.

Tier 2d Phase 1 (audit 2026-05-07). 548MB SQLite bloat as of the live
log 15:25:55Z. Six monotonically-growing tables, VACUUM disabled,
zero retention crons.

This module ships the **substrate only**: a tested, dry-run-default
`purge_older_than(table, days)` helper plus a per-table `RETENTION_POLICY`
default-policy dict. It does NOT arm any cron, does NOT auto-run on
boot, and `lifecycle_events` is intentionally absent from the auto-
purge allowlist (compliance audit trail — Mike or compliance may
want longer retention there per handoff).

Phase 2 (separate PR, after Mike reviews `tools/db_bloat_diagnostic.py`
output and chooses retention policy):
  * Wire `init_db_deferred()` to schedule a daily cron.
  * Optional weekly `PRAGMA incremental_vacuum`.
  * Decide whether `lifecycle_events` belongs in the policy at all.

Allowed tables (`AUTO_PURGE_ALLOWLIST`):
  * email_log              (date col: logged_at)
  * audit_trail            (date col: created_at)
  * recommendation_audit   (date col: recorded_at)
  * utilization_events     (date col: created_at)

Opt-in only (`COMPLIANCE_OPT_IN_ALLOWLIST`):
  * lifecycle_events       (date col: occurred_at) — caller must
                            pass the table explicitly AND set
                            `force_compliance_table=True`.

Design choices:
  * `dry_run=True` by default. Caller MUST pass `dry_run=False` to
    actually delete.
  * LIMIT-batched DELETE so a 500K-row purge can't lock the DB
    table for minutes. Each batch commits before the next one runs.
  * Returns a dict with `would_delete` (always populated, before any
    delete fires) AND `deleted` (only set when not dry_run). This
    makes the cron telemetry naturally observable: dry-run logs
    yesterday's `would_delete`; live runs log both.
  * Cutoff is computed as `datetime('now', '-N days')` in SQLite —
    avoids tz-vs-UTC drift between Python and SQLite clocks.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from typing import Dict, List, Optional

log = logging.getLogger(__name__)


# Table → date-column-name mapping. Each table in either allowlist
# MUST have an entry here (the policy validates this at import time
# so a typo can't silently disable retention).
_DATE_COLUMN: Dict[str, str] = {
    "email_log": "logged_at",
    "audit_trail": "created_at",
    "recommendation_audit": "recorded_at",
    "utilization_events": "created_at",
    "lifecycle_events": "occurred_at",
}


# Tables the cron may purge automatically. lifecycle_events is
# DELIBERATELY absent — compliance/audit-trail use case per handoff.
AUTO_PURGE_ALLOWLIST = frozenset({
    "email_log",
    "audit_trail",
    "recommendation_audit",
    "utilization_events",
})


# Tables that are valid retention targets but require an explicit
# `force_compliance_table=True` flag from the caller. Phase 2 may
# choose to leave this empty and never auto-purge.
COMPLIANCE_OPT_IN_ALLOWLIST = frozenset({
    "lifecycle_events",
})


# Default per-table retention in days. NOT enforced — these are
# RECOMMENDATIONS the cron-arming PR (Phase 2) can use as defaults.
# Operator can override per-call.
RETENTION_POLICY: Dict[str, int] = {
    "email_log": 30,            # Likely the dominant — 30d keeps ~1mo
                                # of full body for buyer-reply replay.
    "audit_trail": 90,          # 3mo of field-change history.
    "recommendation_audit": 90,  # 3mo of pricing-recommendation logs.
    "utilization_events": 30,   # Feature-use telemetry — 30d trend.
    "lifecycle_events": 365,    # Compliance trail — 1yr safety floor
                                # if Phase 2 ever opts it in.
}


# Validate at import time so a typo in any allowlist trips boot
# instead of silently disabling retention later.
for _tbl in (AUTO_PURGE_ALLOWLIST | COMPLIANCE_OPT_IN_ALLOWLIST):
    if _tbl not in _DATE_COLUMN:
        raise RuntimeError(
            f"db_retention: table {_tbl!r} in allowlist but has no "
            f"_DATE_COLUMN entry — this would silently fail at runtime")
    if _tbl not in RETENTION_POLICY:
        raise RuntimeError(
            f"db_retention: table {_tbl!r} in allowlist but has no "
            f"RETENTION_POLICY default")


class RetentionError(Exception):
    """Raised when a caller tries to purge a non-allowed table or
    asks for a non-positive `days` value."""


def purge_older_than(
    table: str,
    days: int,
    *,
    dry_run: bool = True,
    batch_size: int = 1000,
    conn: Optional[sqlite3.Connection] = None,
    force_compliance_table: bool = False,
) -> Dict:
    """Delete rows older than `days` from `table` in batches.

    Args:
      table: target table name. MUST be in `AUTO_PURGE_ALLOWLIST`,
             OR in `COMPLIANCE_OPT_IN_ALLOWLIST` with
             `force_compliance_table=True`.
      days: positive integer; rows where `<date_col> < now - days`
            are deleted.
      dry_run: when True (default), no rows are deleted. The result
               dict still reports `would_delete` so operators can
               size the cleanup before arming the cron.
      batch_size: rows per DELETE statement. Each batch commits
                  before the next runs to avoid long write locks.
      conn: optional sqlite3.Connection. When None, uses
            `src.core.db.get_db()` context manager.
      force_compliance_table: explicit opt-in for compliance tables.
                              Required for `lifecycle_events`.

    Returns:
      {
        "table": str,
        "days": int,
        "cutoff": str,           # ISO-formatted SQLite-side cutoff
        "would_delete": int,     # always populated
        "deleted": int,          # only when not dry_run; else 0
        "batches": int,          # number of DELETE statements run
        "dry_run": bool,
      }

    Raises:
      RetentionError on bad table / bad days / compliance gate.
    """
    if days < 1:
        raise RetentionError(
            f"days must be >= 1, got {days} — refusing to purge "
            f"the entire table by accident")

    if table in COMPLIANCE_OPT_IN_ALLOWLIST and not force_compliance_table:
        raise RetentionError(
            f"table {table!r} is a compliance audit trail. Pass "
            f"force_compliance_table=True only after confirming the "
            f"retention policy with Mike/compliance.")
    if (table not in AUTO_PURGE_ALLOWLIST
            and table not in COMPLIANCE_OPT_IN_ALLOWLIST):
        raise RetentionError(
            f"table {table!r} is not in db_retention allowlist. "
            f"Allowed: {sorted(AUTO_PURGE_ALLOWLIST)}; "
            f"opt-in: {sorted(COMPLIANCE_OPT_IN_ALLOWLIST)}")

    date_col = _DATE_COLUMN[table]
    cutoff_expr = f"datetime('now', '-{int(days)} days')"

    # Acquire connection: either passed-in or via the canonical helper.
    own_conn = False
    if conn is None:
        try:
            from src.core.db import get_db
        except Exception as e:
            raise RetentionError(f"db.get_db unavailable: {e}")
        ctx = get_db()
        conn = ctx.__enter__()
        own_conn = True

    result = {
        "table": table,
        "days": days,
        "cutoff": "",
        "would_delete": 0,
        "deleted": 0,
        "batches": 0,
        "dry_run": dry_run,
    }
    try:
        # Capture cutoff for telemetry. SQLite-side eval avoids tz drift.
        row = conn.execute(f"SELECT {cutoff_expr} as c").fetchone()
        result["cutoff"] = row["c"] if isinstance(row, sqlite3.Row) else row[0]

        # Count first — always cheap relative to delete, and gives us
        # the would_delete telemetry even when dry_run=True.
        # `f"... {table} ..."` is safe: `table` came from a frozenset
        # allowlist, no user input.
        cnt_row = conn.execute(
            f"SELECT COUNT(*) as n FROM {table} "
            f"WHERE {date_col} < {cutoff_expr}"
        ).fetchone()
        n = int(cnt_row["n"] if isinstance(cnt_row, sqlite3.Row) else cnt_row[0])
        result["would_delete"] = n

        if dry_run or n == 0:
            log.info(
                "db_retention: %s purge dry_run=%s would_delete=%d "
                "(cutoff=%s, days=%d)",
                table, dry_run, n, result["cutoff"], days)
            return result

        # LIMIT-batched DELETE. Commit per batch to release the
        # write lock so other writers don't stall.
        deleted = 0
        batches = 0
        while True:
            cur = conn.execute(
                f"DELETE FROM {table} WHERE rowid IN ("
                f"SELECT rowid FROM {table} "
                f"WHERE {date_col} < {cutoff_expr} "
                f"LIMIT {int(batch_size)})"
            )
            n_batch = cur.rowcount or 0
            conn.commit()
            batches += 1
            deleted += n_batch
            if n_batch < batch_size:
                break  # last partial batch — done
            if batches > 100_000:
                # Safety stop: shouldn't ever hit this but cheap to add.
                log.error(
                    "db_retention: %s — batch ceiling hit at %d batches, "
                    "stopping to avoid runaway loop", table, batches)
                break

        result["deleted"] = deleted
        result["batches"] = batches
        log.info(
            "db_retention: %s purged deleted=%d batches=%d "
            "(cutoff=%s, days=%d)",
            table, deleted, batches, result["cutoff"], days)
        return result
    finally:
        if own_conn:
            try:
                ctx.__exit__(None, None, None)
            except Exception as e:
                log.debug("db_retention: ctx exit suppressed: %s", e)


def run_daily_purge(
    *,
    dry_run: Optional[bool] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict:
    """Daily cron entrypoint — purges every table in `AUTO_PURGE_ALLOWLIST`
    using `RETENTION_POLICY` defaults.

    `lifecycle_events` is intentionally skipped (compliance opt-in).

    Args:
      dry_run: when None (default), reads `DB_RETENTION_DRY_RUN` env var
               ("1"/"true" → dry-run, anything else → real delete). When
               explicitly True or False, overrides the env var. The env
               override exists so a cautious first deploy can ship with
               `DB_RETENTION_DRY_RUN=1`, watch the `would_delete` numbers
               for a day, then flip the env var to "0" without re-deploy.
      conn: optional sqlite3.Connection; one is acquired per table when
            None so a stuck table can't block the others.

    Returns:
      {
        "dry_run": bool,
        "tables": [<purge_older_than result>, ...],
        "total_would_delete": int,
        "total_deleted": int,
        "errors": [{"table": str, "error": str}, ...],
      }
    """
    if dry_run is None:
        dry_run = os.environ.get("DB_RETENTION_DRY_RUN", "0").lower() in (
            "1", "true", "yes", "on")

    summary: Dict = {
        "dry_run": dry_run,
        "tables": [],
        "total_would_delete": 0,
        "total_deleted": 0,
        "errors": [],
    }

    for tbl in sorted(AUTO_PURGE_ALLOWLIST):
        days = RETENTION_POLICY[tbl]
        try:
            r = purge_older_than(tbl, days, dry_run=dry_run, conn=conn)
        except Exception as e:
            log.error("run_daily_purge: %s failed: %s", tbl, e)
            summary["errors"].append({"table": tbl, "error": str(e)})
            continue
        summary["tables"].append(r)
        summary["total_would_delete"] += r.get("would_delete", 0)
        summary["total_deleted"] += r.get("deleted", 0)

    log.info(
        "run_daily_purge: dry_run=%s would_delete=%d deleted=%d errors=%d",
        summary["dry_run"], summary["total_would_delete"],
        summary["total_deleted"], len(summary["errors"]))
    return summary


def bloat_report(conn: Optional[sqlite3.Connection] = None) -> Dict:
    """Per-table row count + page count + size estimate, sorted by
    size descending.

    Read-only. Phase 2 cron-arming PR can use this for before/after
    delta logging.

    Returns:
      {
        "page_size": int,
        "total_pages": int,
        "total_bytes": int,
        "tables": [
          {"name": str, "rows": int, "pages": int, "bytes": int,
           "auto_purge": bool, "compliance_only": bool,
           "retention_days": int | None}
          ...
        ],
      }
    """
    own_conn = False
    if conn is None:
        try:
            from src.core.db import get_db
        except Exception as e:
            return {"error": f"db.get_db unavailable: {e}", "tables": []}
        ctx = get_db()
        conn = ctx.__enter__()
        own_conn = True

    try:
        page_size_row = conn.execute("PRAGMA page_size").fetchone()
        page_size = int(
            page_size_row[0] if not isinstance(page_size_row, sqlite3.Row)
            else page_size_row["page_size"])

        total_pages_row = conn.execute("PRAGMA page_count").fetchone()
        total_pages = int(
            total_pages_row[0] if not isinstance(total_pages_row, sqlite3.Row)
            else total_pages_row["page_count"])

        # List user tables (skip sqlite_*).
        tables = []
        rows = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        ).fetchall()
        for r in rows:
            name = r["name"] if isinstance(r, sqlite3.Row) else r[0]
            try:
                cnt = conn.execute(
                    f"SELECT COUNT(*) as n FROM {name}").fetchone()
                n = int(cnt["n"] if isinstance(cnt, sqlite3.Row) else cnt[0])
            except sqlite3.OperationalError:
                n = -1  # virtual / unreadable

            # SQLite doesn't expose per-table page count without
            # `dbstat` (a compile-time vtable that's not in the
            # default Python build). Estimate via row count alone
            # and an average-row-bytes heuristic. Phase 2 can swap
            # to dbstat if it's available on the Railway image.
            tables.append({
                "name": name,
                "rows": n,
                "pages": -1,  # unknown without dbstat
                "bytes": -1,
                "auto_purge": name in AUTO_PURGE_ALLOWLIST,
                "compliance_only": name in COMPLIANCE_OPT_IN_ALLOWLIST,
                "retention_days": RETENTION_POLICY.get(name),
            })

        # Sort by row count desc — without dbstat, rows is the best
        # available bloat-proxy.
        tables.sort(key=lambda t: t["rows"], reverse=True)

        return {
            "page_size": page_size,
            "total_pages": total_pages,
            "total_bytes": page_size * total_pages,
            "tables": tables,
        }
    finally:
        if own_conn:
            try:
                ctx.__exit__(None, None, None)
            except Exception as e:
                log.debug("bloat_report: ctx exit suppressed: %s", e)
