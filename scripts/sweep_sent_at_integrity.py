"""Sweep `sent_at == created_at` rows across quotes / rfqs / price_checks.

The bug Mike caught on 2026-05-02: every row in the home-page Sent /
Awaiting Response table was stamped "today" because some writer was
filling `sent_at` with `created_at` instead of the real send moment.
The audit (PR-5 #695) found no current writer doing this — the bad
rows are historical residue from a defunct backfill or pre-formal
send path.

PR-3's `is_real_sent` / `is_awaiting_buyer` predicates already filter
these rows out at *read* time, so they don't pollute the home-page
widget. PR-5 cleans them up at the *write* layer too: any row where
`status='sent'` AND `sent_at == created_at` and the strings are
non-empty has its `sent_at` set to '' (NULL semantics), so the row
falls into the "missing sent timestamp" bucket where the operator
will see the data quality gap and either manually-mark or accept it.

Why clear instead of delete or backfill:
  - DELETE loses the row's existence record (we still want to know
    these existed and were sent at *some* point).
  - BACKFILL guesses; we'd rather force the operator to confirm than
    fabricate a moment in time we don't have.
  - SET '' (NULL) flips the row into a known-bad bucket the canonical
    predicate already handles. Operator sees "sent at: missing" and
    can act if it matters.

Tables swept: quotes, rfqs, price_checks. JSON blobs (data_json column)
are also rewritten on the same rows so the read paths agree.

Safe defaults: dry-run only. `--apply` required to commit. Always
exits 0 on no-op.

Usage:
  # Dry-run (default) — print counts + sample rows, write nothing:
  python scripts/sweep_sent_at_integrity.py

  # Apply the cleanup:
  python scripts/sweep_sent_at_integrity.py --apply

  # Limit scope:
  python scripts/sweep_sent_at_integrity.py --only rfqs

  # Override DB path (CI / test):
  python scripts/sweep_sent_at_integrity.py --db /tmp/test.db
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone


# Tables we sweep. Each entry:
#   (table, id_column, has_data_json, created_col_name)
# Heterogeneous shapes:
#   - quotes: column-level created_at + sent_at
#   - price_checks: column-level created_at + sent_at
#   - rfqs: NO column-level sent_at or created_at. Both live in
#     data_json blob; rfqs uses `received_at` as the creation field.
#     Sweep checks the blob.
TARGETS = [
    ("quotes", "quote_number", False, "created_at"),
    ("rfqs", "id", True, "received_at"),
    ("price_checks", "id", True, "created_at"),
]


def _default_db_path() -> str:
    """Resolve the DB path the same way the app does (Railway-friendly)."""
    try:
        from src.core.paths import DATA_DIR  # type: ignore
        return os.path.join(DATA_DIR, "reytech.db")
    except Exception:
        return os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "data", "reytech.db")


def _has_column(conn, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def find_bad_rows(conn, table: str, id_column: str,
                  created_col: str) -> list:
    """Return rows where status='sent' AND sent_at == created_at AND both non-empty.

    Limited to status='sent' so we don't accidentally mutate rows
    where the bug-shape is incidental (e.g. a draft row that happens
    to share strings is benign — only sent records pollute the
    Awaiting widget).

    Two paths depending on schema shape:
      A. Column-level: table has a `sent_at` column (quotes,
         price_checks). Compare directly.
      B. Blob-only: table has no `sent_at` column (rfqs); the
         field lives in data_json. Decode each row and compare
         blob fields. The created field name varies per-table
         (`received_at` for rfqs, `created_at` for the rest).
    """
    has_status = _has_column(conn, table, "status")
    if not has_status:
        return []

    if _has_column(conn, table, "sent_at") and _has_column(conn, table, created_col):
        # Path A — column-level sweep.
        rows = conn.execute(
            f"""
            SELECT {id_column} AS id, status, {created_col} AS created_at, sent_at
            FROM {table}
            WHERE LOWER(COALESCE(status, '')) = 'sent'
              AND sent_at IS NOT NULL AND TRIM(sent_at) != ''
              AND {created_col} IS NOT NULL AND TRIM({created_col}) != ''
              AND sent_at = {created_col}
            """
        ).fetchall()
        return [
            {"id": r[0], "status": r[1], "created_at": r[2], "sent_at": r[3]}
            for r in rows
        ]

    if not _has_column(conn, table, "data_json"):
        return []

    # Path B — blob-only sweep. Pre-filter on status when available.
    rows = conn.execute(
        f"SELECT {id_column} AS id, status, data_json FROM {table} "
        f"WHERE LOWER(COALESCE(status, '')) = 'sent'"
    ).fetchall()
    bad = []
    for r in rows:
        blob_raw = r[2] or ""
        try:
            blob = json.loads(blob_raw) if blob_raw else {}
        except json.JSONDecodeError:
            continue
        if not isinstance(blob, dict):
            continue
        sent_at = (blob.get("sent_at") or "").strip()
        if not sent_at:
            continue
        # The blob may carry either created_at or the table's
        # alternate creation field (received_at for rfqs). Compare
        # against whichever exists.
        created_at = (blob.get("created_at") or blob.get(created_col) or "").strip()
        if not created_at:
            continue
        if sent_at == created_at:
            bad.append({
                "id": r[0],
                "status": r[1],
                "created_at": created_at,
                "sent_at": sent_at,
            })
    return bad


def clear_sent_at(conn, table: str, id_column: str, ids: list[str],
                  has_data_json: bool, actor: str) -> int:
    """Set sent_at='' on rows by id. Also rewrites data_json blob if
    present. Records a sweep audit entry. Returns rows affected."""
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    now_iso = datetime.now(timezone.utc).isoformat()

    # Clear the column when one exists. Skip silently if not — for
    # blob-only tables (rfqs) the next block does the work.
    if _has_column(conn, table, "sent_at"):
        conn.execute(
            f"UPDATE {table} SET sent_at = '' "
            f"WHERE {id_column} IN ({placeholders})",
            ids,
        )

    # Rewrite the JSON blob too — read paths fall back to it.
    if has_data_json and _has_column(conn, table, "data_json"):
        rows = conn.execute(
            f"SELECT {id_column} AS id, data_json FROM {table} "
            f"WHERE {id_column} IN ({placeholders})",
            ids,
        ).fetchall()
        for r in rows:
            blob_raw = r[1] or ""
            try:
                blob = json.loads(blob_raw) if blob_raw else {}
            except json.JSONDecodeError:
                continue
            if not isinstance(blob, dict):
                continue
            sent_at = blob.get("sent_at") or ""
            created_at = blob.get("created_at") or blob.get("received_at") or ""
            if sent_at and sent_at == created_at:
                blob["sent_at"] = ""
                blob["sent_at_swept"] = now_iso
                blob["sent_at_swept_by"] = actor
                conn.execute(
                    f"UPDATE {table} SET data_json = ? "
                    f"WHERE {id_column} = ?",
                    (json.dumps(blob, ensure_ascii=False), r[0]),
                )
    return len(ids)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true",
                   help="Commit the cleanup. Without this flag, dry-run.")
    p.add_argument("--only",
                   choices=("quotes", "rfqs", "price_checks"),
                   help="Limit sweep to a single table.")
    p.add_argument("--db", default=_default_db_path(),
                   help="DB path (default: app's reytech.db).")
    p.add_argument("--actor", default="pr5_sweep",
                   help="Audit actor stamped onto data_json blobs.")
    args = p.parse_args()

    if not os.path.exists(args.db):
        print(f"ERR  no DB at {args.db}", file=sys.stderr)
        return 2

    targets = [t for t in TARGETS if not args.only or t[0] == args.only]
    grand = 0
    print(f"sweep_sent_at_integrity: db={args.db} apply={args.apply}")
    print(f"  targets: {[t[0] for t in targets]}")
    print()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        for table, id_col, has_blob, created_col in targets:
            bad = find_bad_rows(conn, table, id_col, created_col)
            print(f"  {table}: {len(bad)} bad rows "
                  f"(sent_at == {created_col} AND status='sent')")
            if bad[:3]:
                print(f"    sample: {bad[:3]}")
            if args.apply and bad:
                affected = clear_sent_at(
                    conn, table, id_col, [r["id"] for r in bad],
                    has_blob, args.actor)
                conn.commit()
                print(f"    cleared sent_at on {affected} rows in {table}")
            grand += len(bad)
    finally:
        conn.close()

    print()
    print(f"total bad rows: {grand}")
    if not args.apply and grand:
        print("dry-run complete; rerun with --apply to commit")
    return 0


if __name__ == "__main__":
    sys.exit(main())
