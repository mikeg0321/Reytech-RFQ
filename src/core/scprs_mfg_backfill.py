"""SCPRS MFG# backfill — extract part numbers from descriptions and
write them to the per-table identifier columns the oracle searches.

## Why this exists

The pricing oracle (PR #487 MFG# partition) reads each SCPRS-style
search row's MFG# column to decide whether a comp is "MFG#-anchored"
(highest trust) or "description-only" (broader-market signal). When
≥2 MFG#-anchored rows exist for a quote item, the SCPRS Avg / Comp
Low are computed from MFG# rows only — defends against token-match
false positives dragging the average toward unrelated products.

But the *historical* SCPRS data we synced (88,265 rows in won_quotes
as of 2026-04-23) has **empty MFG# columns**. The CDTFA / SCPRS
public search portal doesn't expose a clean MFG# field, so the
ingest stored everything in `description` and left identifier columns
NULL. Result: even when Mike's PC carries a clean MFG# like "WL085P"
or "163353", the oracle's MFG# OR clause returns 0 rows because the
historical side has nothing to match against.

This module sweeps each table once, runs the same `_extract_part_number`
regex chain that `price_check.py` uses on incoming items, and writes
extracted MFG#s back to the column the oracle searches:

  Table              | Column oracle searches
  -------------------|-----------------------
  won_quotes         | item_number
  scprs_catalog      | mfg_number
  scprs_po_lines     | item_id
  winning_prices     | part_number

Idempotent — only fills empty/NULL columns. Safe to re-run.

## Usage

  from src.core.scprs_mfg_backfill import backfill_mfg_numbers
  stats = backfill_mfg_numbers()  # writes to DB
  stats = backfill_mfg_numbers(dry_run=True)  # report only

The admin route `POST /api/admin/scprs/backfill-mfg` exposes this
to operators. Returns per-table stats:

  {
    "ok": True,
    "stats": {
      "won_quotes":     {"scanned": 88265, "extracted": 12340, "written": 12340},
      "scprs_catalog":  {"scanned":  4560, "extracted":   890, "written":   890},
      "scprs_po_lines": {"scanned": 109000,"extracted": 25600, "written": 25600},
      "winning_prices": {"scanned":   320, "extracted":    78, "written":    78},
      "duration_sec": 12.3
    }
  }
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict

log = logging.getLogger("reytech.scprs_backfill")


# Per-table mapping: (table_name, target_column, description_column).
# `description_column` is the source we extract from; `target_column`
# is the identifier column the oracle's search function compares
# against `LOWER(...) = item_number_lc`.
_BACKFILL_TARGETS = [
    ("won_quotes",     "item_number",  "description"),
    ("scprs_catalog",  "mfg_number",   "description"),
    ("scprs_po_lines", "item_id",      "description"),
    ("winning_prices", "part_number",  "description"),
]

# Batch size for UPDATE writes — keeps a single transaction reasonable
# while still letting us commit progress periodically.
_BATCH_SIZE = 500


def _empty_stats() -> Dict[str, Any]:
    return {"scanned": 0, "extracted": 0, "written": 0, "errors": 0,
            "skipped_already_set": 0}


def backfill_mfg_numbers(dry_run: bool = False, limit_per_table: int = 0) -> Dict[str, Any]:
    """Sweep SCPRS tables and fill empty MFG# columns from description regex.

    Args:
        dry_run: when True, count extractions but don't write to the DB.
        limit_per_table: when > 0, stop after this many SCANNED rows per
                         table — useful for spot-checking on prod before
                         a full sweep.

    Returns a stats dict per table plus `duration_sec` and `ok`. Never
    raises — DB errors are caught per row and counted in `errors`. Safe
    to call from a Flask route without try/except wrapping.
    """
    t0 = time.monotonic()
    out: Dict[str, Any] = {"ok": True, "dry_run": dry_run, "stats": {}}

    # Lazy import — keeps module light when used as a script.
    try:
        import sqlite3
        from src.core.db import DB_PATH
        from src.forms.price_check import _extract_part_number
    except Exception as e:
        log.error("backfill_mfg_numbers: import error: %s", e)
        out["ok"] = False
        out["error"] = f"import: {e}"
        return out

    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    try:
        for table, target_col, desc_col in _BACKFILL_TARGETS:
            stats = _empty_stats()
            out["stats"][table] = stats
            try:
                # Pre-flight: confirm the table + columns exist. Some
                # installs may lack winning_prices / scprs_catalog
                # depending on which migrations ran. Skip cleanly.
                cols = {r[1] for r in db.execute(
                    f"PRAGMA table_info({table})"
                ).fetchall()}
                if target_col not in cols or desc_col not in cols:
                    log.info("backfill: %s missing columns (%s / %s) — skip",
                             table, target_col, desc_col)
                    stats["error"] = f"missing column {target_col}/{desc_col}"
                    continue

                # Use ROWID so we can update by primary key without
                # assuming the table has a unique non-rowid PK.
                where_empty = (
                    f"({target_col} IS NULL OR TRIM({target_col}) = '')"
                )
                limit_clause = f"LIMIT {int(limit_per_table)}" if limit_per_table else ""
                rows = db.execute(
                    f"SELECT ROWID AS _rid, {desc_col} AS _desc "
                    f"FROM {table} WHERE {where_empty} {limit_clause}"
                ).fetchall()

                pending: list = []
                for r in rows:
                    stats["scanned"] += 1
                    desc = (r["_desc"] or "").strip()
                    if not desc:
                        continue
                    try:
                        mfg = _extract_part_number(desc)
                    except Exception as _ee:
                        log.debug("extract failed on rowid=%s: %s", r["_rid"], _ee)
                        stats["errors"] += 1
                        continue
                    if not mfg:
                        continue
                    stats["extracted"] += 1
                    pending.append((mfg, r["_rid"]))
                    if len(pending) >= _BATCH_SIZE and not dry_run:
                        db.executemany(
                            f"UPDATE {table} SET {target_col} = ? WHERE ROWID = ?",
                            pending,
                        )
                        db.commit()
                        stats["written"] += len(pending)
                        pending.clear()
                if pending and not dry_run:
                    db.executemany(
                        f"UPDATE {table} SET {target_col} = ? WHERE ROWID = ?",
                        pending,
                    )
                    db.commit()
                    stats["written"] += len(pending)
                if dry_run:
                    # In dry run, written count mirrors extracted so the
                    # operator sees what *would* have been written.
                    stats["written"] = stats["extracted"]
                log.info(
                    "backfill %s: scanned=%d extracted=%d written=%d errors=%d (dry_run=%s)",
                    table, stats["scanned"], stats["extracted"],
                    stats["written"], stats["errors"], dry_run,
                )
            except Exception as e:
                log.error("backfill %s: %s", table, e, exc_info=True)
                stats["error"] = str(e)
                out["ok"] = False
    finally:
        db.close()

    out["duration_sec"] = round(time.monotonic() - t0, 2)
    return out


if __name__ == "__main__":  # pragma: no cover
    import json
    import sys
    dry = "--dry-run" in sys.argv or "--dry" in sys.argv
    limit = 0
    for arg in sys.argv:
        if arg.startswith("--limit="):
            limit = int(arg.split("=", 1)[1])
    result = backfill_mfg_numbers(dry_run=dry, limit_per_table=limit)
    print(json.dumps(result, indent=2))
