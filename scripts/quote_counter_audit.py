#!/usr/bin/env python3
"""Audit the quote counter for drift and duplicates.

Read-only. Reports:
  1. Drift between SQLite (authoritative) and `data/quote_counter.json` (legacy file)
  2. Duplicate `quote_number` values inside `price_checks`
  3. Duplicate `rfq_number` values inside `rfqs`
  4. Cross-table collisions (same number used by both a PC and an RFQ)
  5. Numbers in either table that are NOT in `quote_number_ledger`

Writes a JSON report to `data/quote_counter_audit_<ts>.json` and prints a console
summary. Exit code 0 = clean; exit code 2 = duplicates detected (install script
will refuse to add UNIQUE constraints).
"""
import json
import os
import sqlite3
import sys
from datetime import datetime

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.core.paths import DATA_DIR, QUOTE_COUNTER_PATH  # noqa: E402
from src.core.db import DB_PATH  # noqa: E402


def _read_json_counter():
    if not os.path.exists(QUOTE_COUNTER_PATH):
        return None
    try:
        with open(QUOTE_COUNTER_PATH) as f:
            return json.load(f)
    except Exception as e:
        return {"_error": str(e)}


def _read_sqlite_counter(conn):
    keys = ("quote_counter_seq", "quote_counter", "quote_counter_year", "quote_counter_last_good")
    out = {}
    for k in keys:
        row = conn.execute("SELECT value FROM app_settings WHERE key=?", (k,)).fetchone()
        out[k] = row[0] if row else None
    return out


def _find_dupes(conn, table, col):
    rows = conn.execute(
        f"SELECT {col}, COUNT(*) as n, GROUP_CONCAT(id) as ids "
        f"FROM {table} WHERE {col} IS NOT NULL AND {col} != '' "
        f"GROUP BY {col} HAVING n > 1 ORDER BY n DESC"
    ).fetchall()
    return [{"value": r[0], "count": r[1], "ids": (r[2] or "").split(",")} for r in rows]


def _find_cross_collisions(conn):
    rows = conn.execute(
        "SELECT pc.quote_number, pc.id AS pc_id, r.id AS rfq_id "
        "FROM price_checks pc JOIN rfqs r ON pc.quote_number = r.rfq_number "
        "WHERE pc.quote_number IS NOT NULL AND pc.quote_number != ''"
    ).fetchall()
    return [{"quote_number": r[0], "pc_id": r[1], "rfq_id": r[2]} for r in rows]


def _find_unledgered(conn):
    rows = conn.execute(
        "SELECT quote_number, 'price_check' AS kind, id FROM price_checks "
        "WHERE quote_number IS NOT NULL AND quote_number != '' "
        "  AND quote_number NOT IN (SELECT quote_number FROM quote_number_ledger) "
        "UNION ALL "
        "SELECT rfq_number, 'rfq', id FROM rfqs "
        "WHERE rfq_number IS NOT NULL AND rfq_number != '' "
        "  AND rfq_number NOT IN (SELECT quote_number FROM quote_number_ledger) "
        "ORDER BY 1"
    ).fetchall()
    return [{"quote_number": r[0], "kind": r[1], "id": r[2]} for r in rows]


def audit():
    if not os.path.exists(DB_PATH):
        print(f"ERROR: DB not found at {DB_PATH}")
        return 1

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    sqlite_counter = _read_sqlite_counter(conn)
    json_counter = _read_json_counter()

    drift = None
    if json_counter and not json_counter.get("_error"):
        sql_seq = sqlite_counter.get("quote_counter_seq")
        json_seq = json_counter.get("seq")
        if sql_seq is not None and json_seq is not None and str(sql_seq) != str(json_seq):
            drift = {"sqlite_seq": sql_seq, "json_seq": json_seq}

    pc_dupes = _find_dupes(conn, "price_checks", "quote_number")
    rfq_dupes = _find_dupes(conn, "rfqs", "rfq_number")
    cross = _find_cross_collisions(conn)
    unledgered = _find_unledgered(conn)

    conn.close()

    report = {
        "audited_at": datetime.now().isoformat(),
        "db_path": DB_PATH,
        "sqlite_counter": sqlite_counter,
        "json_counter": json_counter,
        "drift": drift,
        "price_check_duplicates": pc_dupes,
        "rfq_duplicates": rfq_dupes,
        "cross_table_collisions": cross,
        "unledgered_numbers": unledgered,
        "total_dupes": len(pc_dupes) + len(rfq_dupes) + len(cross),
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(DATA_DIR, f"quote_counter_audit_{ts}.json")
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)

    # Console summary
    print("=" * 60)
    print("QUOTE COUNTER AUDIT")
    print("=" * 60)
    print(f"SQLite counter:   {sqlite_counter}")
    print(f"JSON counter:     {json_counter}")
    if drift:
        print(f"⚠️  DRIFT detected: SQLite={drift['sqlite_seq']} JSON={drift['json_seq']}")
    else:
        print("✅ No drift between SQLite and JSON")
    print()
    print(f"Price-check dupes:        {len(pc_dupes)}")
    print(f"RFQ dupes:                {len(rfq_dupes)}")
    print(f"Cross-table collisions:   {len(cross)}")
    print(f"Unledgered numbers:       {len(unledgered)}")
    print()
    if pc_dupes:
        print("--- Price-check duplicates ---")
        for d in pc_dupes[:20]:
            print(f"  {d['value']}: {d['count']} rows ({', '.join(d['ids'][:3])}...)")
    if rfq_dupes:
        print("--- RFQ duplicates ---")
        for d in rfq_dupes[:20]:
            print(f"  {d['value']}: {d['count']} rows ({', '.join(d['ids'][:3])}...)")
    if cross:
        print("--- Cross-table collisions ---")
        for c in cross[:20]:
            print(f"  {c['quote_number']}: pc={c['pc_id']} rfq={c['rfq_id']}")
    print()
    print(f"📄 Report written to: {out_path}")
    print("=" * 60)

    if report["total_dupes"] > 0:
        print("❌ Duplicates found — UNIQUE constraint install will REFUSE.")
        print("   Resolve dupes manually, then re-run.")
        return 2
    print("✅ Clean — safe to install UNIQUE constraints.")
    return 0


if __name__ == "__main__":
    sys.exit(audit())
