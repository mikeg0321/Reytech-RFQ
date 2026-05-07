"""Mark spurious thread-duplicate records as dismissed (Q1=A doctrine).

PR-D of the thread-aware-ingest arc (2026-05-07). The 2026-05-06 RFQ
a5b09b56 incident produced two known spurious records:

  1. `pc_93edc64e` — created when Valentina emailed back a question on
     the live quote thread; the email-poller dedup keyed only off
     solicitation_number so it spawned a new PC.
  2. RFQ #10838974 was reborn as a PC when the buyer replied after
     Mike's manual send.

Both are real buyer-source data — Reytech Law 22 says we DO NOT delete.
Instead we mark them with `gmail_thread_duplicate_of = <parent_id>`.
Effects:
  * Migration 40's `v_active_queue_*` views exclude rows where
    `gmail_thread_duplicate_of` is non-empty, so they stop appearing
    on the operator's home/queue.
  * The Python predicate `is_active_queue` (canonical_state.py)
    matches the view — both gates fire together.
  * The data, attachments, and audit log stay intact for QA, training,
    and possible future "buyer reply" routing once PR-E lands.

This is a one-off cleanup. The substrate fix (forward path) is the
ingest dedup logic that uses thread_id (PR-B/PR-E), so this script's
input list is only for known historical mistakes.

Usage:
  # Dry-run (default):
  python scripts/dismiss_thread_duplicates.py
  # Apply (commits column + audit entry):
  python scripts/dismiss_thread_duplicates.py --apply
  # Custom pair list (CSV: kind,duplicate_id,parent_id,reason):
  python scripts/dismiss_thread_duplicates.py --pairs my_list.csv --apply
  # Override DB path (defaults to /data or data/):
  python scripts/dismiss_thread_duplicates.py --db /tmp/test.db
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("dismiss_thread_duplicates")


# ─── Known historical duplicates ──────────────────────────────────────
# Each tuple: (kind, duplicate_id, parent_id, reason).
# kind ∈ {"rfq", "pc"} — names the duplicate's table.
# duplicate_id is the row to mark; parent_id is the original record
# the buyer was replying to.
DEFAULT_PAIRS: list[tuple[str, str, str, str]] = [
    # Valentina's question on RFQ a5b09b56 → spurious PC.
    ("pc", "pc_93edc64e", "rfq_a5b09b56",
     "Buyer reply on existing RFQ thread misclassified as new PC "
     "(2026-05-06 RFQ a5b09b56). Pre-PR-B dedup."),
    # 10838974 sent as RFQ, refired as PC after buyer reply. The exact
    # PC id depends on what the poller assigned — operator must confirm
    # before applying. This entry is informational; remove or replace
    # via --pairs if the prod row id differs from the slug below.
    ("pc", "pc_for_10838974_buyer_reply", "rfq_10838974",
     "RFQ #10838974 buyer-reply refired as PC (2026-05-06). "
     "Pre-PR-B dedup."),
]


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_db_path(override: Optional[str]) -> Optional[str]:
    if override:
        return override
    for p in ("/data/reytech.db", "data/reytech.db"):
        if os.path.exists(p):
            return p
    return None


def _load_pairs(path: Optional[str]) -> list[tuple[str, str, str, str]]:
    """Load (kind, duplicate_id, parent_id, reason) tuples from a CSV
    or fall back to the hard-coded DEFAULT_PAIRS."""
    if not path:
        return list(DEFAULT_PAIRS)
    out: list[tuple[str, str, str, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].lstrip().startswith("#"):
                continue
            if len(row) < 3:
                log.warning("skipping malformed row: %r", row)
                continue
            kind = row[0].strip().lower()
            dup_id = row[1].strip()
            parent_id = row[2].strip()
            reason = row[3].strip() if len(row) > 3 else "(no reason given)"
            if kind not in ("rfq", "pc"):
                log.warning("skipping row with bad kind=%r: %r", kind, row)
                continue
            out.append((kind, dup_id, parent_id, reason))
    return out


def _load_blob(row: sqlite3.Row) -> dict:
    raw = row["data_json"] or "{}"
    try:
        d = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except (ValueError, TypeError):
        d = {}
    if not isinstance(d, dict):
        d = {}
    return d


def _existing_dup_of(blob: dict, row: sqlite3.Row) -> str:
    try:
        col_val = row["gmail_thread_duplicate_of"]
    except (IndexError, KeyError):
        col_val = ""
    return (col_val or blob.get("gmail_thread_duplicate_of") or "").strip()


def _check_record_exists(conn: sqlite3.Connection,
                         table: str, rid: str) -> bool:
    cur = conn.execute(f"SELECT 1 FROM {table} WHERE id=? LIMIT 1", (rid,))
    return cur.fetchone() is not None


def _apply_one(conn: sqlite3.Connection, *,
               kind: str, duplicate_id: str,
               parent_id: str, reason: str) -> dict:
    """Return a result dict for this record, applying the column +
    blob mutation when called inside an `--apply` run."""
    table = "price_checks" if kind == "pc" else "rfqs"
    parent_table = table  # same kind as parent for the known cases;
    # if a future case crosses kinds, change to a 4-arg invocation.

    if not _check_record_exists(conn, table, duplicate_id):
        return {
            "kind": kind, "duplicate_id": duplicate_id,
            "parent_id": parent_id, "reason": reason,
            "status": "duplicate-not-found",
        }
    parent_kind = "rfq" if parent_id.startswith("rfq_") else (
        "pc" if parent_id.startswith("pc_") else "unknown")
    parent_lookup_table = "rfqs" if parent_kind == "rfq" else (
        "price_checks" if parent_kind == "pc" else None)
    if parent_lookup_table and not _check_record_exists(
            conn, parent_lookup_table, parent_id):
        return {
            "kind": kind, "duplicate_id": duplicate_id,
            "parent_id": parent_id, "reason": reason,
            "status": "parent-not-found",
        }

    conn.row_factory = sqlite3.Row
    row = conn.execute(
        f"SELECT * FROM {table} WHERE id=?", (duplicate_id,)
    ).fetchone()
    blob = _load_blob(row)
    existing = _existing_dup_of(blob, row)
    if existing:
        return {
            "kind": kind, "duplicate_id": duplicate_id,
            "parent_id": parent_id, "reason": reason,
            "status": "already-dismissed",
            "existing_parent": existing,
        }

    blob["gmail_thread_duplicate_of"] = parent_id
    blob["gmail_thread_duplicated_at"] = _utc_iso()
    blob["gmail_thread_duplicate_reason"] = reason
    audit = blob.setdefault("audit_log", [])
    audit.append({
        "at": _utc_iso(),
        "actor": "scripts.dismiss_thread_duplicates",
        "action": "thread-duplicate-dismiss",
        "parent_id": parent_id,
        "reason": reason,
    })

    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    sets = ["data_json=?"]
    params = [json.dumps(blob, default=str)]
    if "gmail_thread_duplicate_of" in cols:
        sets.append("gmail_thread_duplicate_of=?")
        params.append(parent_id)
    if "updated_at" in cols:
        sets.append("updated_at=?")
        params.append(_utc_iso())
    params.append(duplicate_id)
    conn.execute(f"UPDATE {table} SET {', '.join(sets)} WHERE id=?", params)

    return {
        "kind": kind, "duplicate_id": duplicate_id,
        "parent_id": parent_id, "reason": reason,
        "status": "dismissed",
    }


def run(db_path: Optional[str], *,
        apply: bool = False,
        pairs_csv: Optional[str] = None) -> dict:
    """Apply / dry-run the dismissal pass and return a structured result.

    Result keys:
      ok           : bool
      mode         : "apply" | "dry-run"
      db_path      : resolved DB path
      pairs_count  : number of pairs considered
      records      : per-pair status dicts
      summary      : counts by status
      error        : populated on hard failure
    """
    resolved = _resolve_db_path(db_path)
    if not resolved or not os.path.exists(resolved):
        return {
            "ok": False,
            "error": f"DB not found: {resolved or '/data/reytech.db'}",
            "mode": "apply" if apply else "dry-run",
        }

    pairs = _load_pairs(pairs_csv)
    conn = sqlite3.connect(resolved)
    try:
        records = []
        summary: dict[str, int] = {}
        for kind, dup_id, parent_id, reason in pairs:
            r = _apply_one(conn, kind=kind, duplicate_id=dup_id,
                           parent_id=parent_id, reason=reason)
            records.append(r)
            summary[r["status"]] = summary.get(r["status"], 0) + 1
        if apply:
            conn.commit()
        else:
            conn.rollback()
        return {
            "ok": True,
            "mode": "apply" if apply else "dry-run",
            "db_path": resolved,
            "pairs_count": len(pairs),
            "records": records,
            "summary": summary,
        }
    finally:
        conn.close()


def _print_report(result: dict) -> int:
    if not result.get("ok"):
        print(f"ERROR: {result.get('error', 'unknown')}", file=sys.stderr)
        return 2 if "DB not found" in (result.get("error") or "") else 1
    print(f"{result['mode'].upper()} dismiss_thread_duplicates "
          f"on {result['db_path']}")
    print(f"Pairs: {result['pairs_count']}")
    for r in result["records"]:
        print(f"  {r['kind'].upper():3s} {r['duplicate_id']:30s}"
              f" -> {r['parent_id']:30s} [{r['status']}]")
    print(f"\nSummary: {result['summary']}")
    if result["mode"] == "dry-run":
        print("Pass --apply to commit.")
    return 0


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--apply", action="store_true",
                   help="Commit dismissals. Default is dry-run.")
    p.add_argument("--db", default=None,
                   help="Override DB path (default auto-detects).")
    p.add_argument("--pairs", default=None,
                   help="CSV of (kind, duplicate_id, parent_id, reason). "
                   "Defaults to the hard-coded historical list.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    result = run(args.db, apply=args.apply, pairs_csv=args.pairs)
    return _print_report(result)


if __name__ == "__main__":
    sys.exit(main())
