"""Non-destructive `reytech.db` bloat diagnostic — 2026-04-20.

Prod `reytech.db` is ~513 MB, which is an order of magnitude larger
than a Flask-SQLite app usually needs. Per user direction ("diagnostic
first, no VACUUM until we know what's bloating"), this script reads
the DB in read-only mode and reports:

  - Total file size + free-page fraction (how much VACUUM would reclaim)
  - Per-table row count + approx logical size
  - Per-table physical size via sqlite_stat1 / dbstat when available
  - Top-N largest tables + indexes
  - Suggestions (truncatable log tables, missing indexes, VACUUM savings)

Usage:
    python scripts/db_bloat_diagnostic.py                  # local reytech.db
    python scripts/db_bloat_diagnostic.py --db /data/reytech.db
    python scripts/db_bloat_diagnostic.py --json           # machine-readable

Run on prod via Railway's shell:
    railway run python scripts/db_bloat_diagnostic.py

**This script never writes.** It opens the DB with `mode=ro` via URI
and explicitly does not run ANALYZE/VACUUM/DELETE. Safe to run against
a live prod DB.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys


_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)


def _default_db_path():
    # Railway mounts /data; local dev uses ./data/reytech.db.
    for candidate in ("/data/reytech.db",
                      os.path.join(_ROOT, "data", "reytech.db")):
        if os.path.exists(candidate):
            return candidate
    return os.path.join(_ROOT, "data", "reytech.db")


def _open_ro(db_path):
    uri = f"file:{db_path}?mode=ro"
    return sqlite3.connect(uri, uri=True, timeout=30)


def _pragma(conn, name):
    row = conn.execute(f"PRAGMA {name}").fetchone()
    return row[0] if row else None


def _file_level_stats(conn, db_path):
    size_bytes = os.path.getsize(db_path)
    page_size = _pragma(conn, "page_size") or 4096
    page_count = _pragma(conn, "page_count") or 0
    freelist = _pragma(conn, "freelist_count") or 0
    computed_size = page_size * page_count
    free_bytes = page_size * freelist
    return {
        "file_size_bytes": size_bytes,
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist,
        "computed_size_bytes": computed_size,
        "free_bytes": free_bytes,
        "free_pct": round(free_bytes / size_bytes * 100, 2) if size_bytes else 0,
    }


def _tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def _indexes(conn):
    rows = conn.execute(
        "SELECT name, tbl_name FROM sqlite_master "
        "WHERE type='index' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _table_row_counts(conn, tables):
    out = {}
    for t in tables:
        try:
            r = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()
            out[t] = r[0] if r else 0
        except sqlite3.DatabaseError as e:
            out[t] = f"ERR: {e}"
    return out


def _dbstat_sizes(conn):
    """Return physical bytes per object (table or index), or None if
    the `dbstat` virtual table is unavailable on this sqlite build."""
    try:
        rows = conn.execute(
            "SELECT name, SUM(pgsize) AS bytes "
            "FROM dbstat GROUP BY name ORDER BY bytes DESC"
        ).fetchall()
        return {name: bytes_ for name, bytes_ in rows}
    except sqlite3.OperationalError:
        return None


def _fmt_bytes(n):
    if not isinstance(n, (int, float)):
        return str(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


def _suggestions(file_stats, row_counts, sizes):
    hints = []

    # VACUUM savings
    if file_stats["free_pct"] >= 20:
        hints.append(
            f"⚠ {file_stats['free_pct']}% of the DB is free-page overhead "
            f"(~{_fmt_bytes(file_stats['free_bytes'])}). "
            f"A VACUUM would reclaim that, BUT requires as much free disk as "
            f"the current file size and locks writes while it runs. "
            f"Coordinate a maintenance window."
        )
    elif file_stats["free_pct"] >= 5:
        hints.append(
            f"ℹ Free-page overhead is {file_stats['free_pct']}% "
            f"(~{_fmt_bytes(file_stats['free_bytes'])}). Not worth a VACUUM "
            f"unless disk is tight."
        )

    # Log-shaped tables that often grow unbounded
    log_hints = []
    for t in ("audit_log", "api_calls", "task_log", "webhook_log",
              "api_usage", "quote_attempts", "email_log", "pull_history",
              "poll_history", "intel_events"):
        if t in row_counts and isinstance(row_counts[t], int) and row_counts[t] > 10000:
            log_hints.append(f"{t} ({row_counts[t]:,} rows)")
    if log_hints:
        hints.append(
            "ℹ Log-shaped tables with 10k+ rows (candidates for a "
            "retention window — e.g. keep 90 days): "
            + ", ".join(log_hints)
        )

    # Large tables without corresponding dbstat info
    if sizes:
        top = sorted(((n, b) for n, b in sizes.items() if b > 50 * 1024 * 1024),
                     key=lambda x: -x[1])
        if top:
            lines = [f"{n} ({_fmt_bytes(b)})" for n, b in top[:5]]
            hints.append(
                "⚠ Objects >50 MB on disk — these dominate the file size: "
                + ", ".join(lines)
            )

    return hints


def run(db_path, as_json=False):
    if not os.path.exists(db_path):
        print(f"ERROR: DB not found at {db_path}", file=sys.stderr)
        return 2

    conn = _open_ro(db_path)
    try:
        file_stats = _file_level_stats(conn, db_path)
        tables = _tables(conn)
        indexes = _indexes(conn)
        row_counts = _table_row_counts(conn, tables)
        sizes = _dbstat_sizes(conn)  # may be None
    finally:
        conn.close()

    report = {
        "db_path": db_path,
        "file": file_stats,
        "table_count": len(tables),
        "index_count": len(indexes),
        "rows_per_table": row_counts,
        "bytes_per_object": sizes,
        "suggestions": _suggestions(file_stats, row_counts, sizes),
    }

    if as_json:
        print(json.dumps(report, indent=2, default=str))
        return 0

    # Human-readable summary. ASCII-only so it works on Windows cp1252.
    print("=" * 68)
    print(f"reytech.db bloat diagnostic -- {db_path}")
    print("=" * 68)
    print(f"File size          : {_fmt_bytes(file_stats['file_size_bytes'])}")
    print(f"Page size / count  : {file_stats['page_size']} B x "
          f"{file_stats['page_count']:,} pages")
    print(f"Free pages         : {file_stats['freelist_count']:,} "
          f"({_fmt_bytes(file_stats['free_bytes'])}, "
          f"{file_stats['free_pct']}% of file)")
    print(f"Tables / indexes   : {len(tables)} / {len(indexes)}")
    print()

    # Top tables by row count
    numeric = [(t, c) for t, c in row_counts.items() if isinstance(c, int)]
    numeric.sort(key=lambda x: -x[1])
    print("Top 15 tables by row count")
    print("-" * 68)
    print(f"  {'table':<40}  {'rows':>14}")
    for t, c in numeric[:15]:
        print(f"  {t:<40}  {c:>14,}")
    print()

    if sizes:
        print("Top 15 objects by physical size (dbstat)")
        print("-" * 68)
        top_sizes = sorted(sizes.items(), key=lambda x: -x[1])[:15]
        print(f"  {'object':<40}  {'bytes':>14}  {'human':>10}")
        for name, b in top_sizes:
            print(f"  {name:<40}  {b:>14,}  {_fmt_bytes(b):>10}")
        print()
    else:
        print("(dbstat virtual table not available -- rebuild sqlite with "
              "SQLITE_ENABLE_DBSTAT_VTAB for per-object sizes)")
        print()

    if report["suggestions"]:
        print("Suggestions")
        print("-" * 68)
        for s in report["suggestions"]:
            # Strip non-ASCII for Windows console safety.
            safe = s.encode("ascii", "replace").decode("ascii")
            print(f"  {safe}")
        print()
    print("=" * 68)
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Read-only diagnostic for reytech.db bloat.")
    ap.add_argument("--db", default=None,
                    help="Path to reytech.db (default: /data/reytech.db "
                         "or <repo>/data/reytech.db, whichever exists).")
    ap.add_argument("--json", action="store_true",
                    help="Emit JSON report instead of human-readable text.")
    args = ap.parse_args(argv)

    db_path = args.db or _default_db_path()
    return run(db_path, as_json=args.json)


if __name__ == "__main__":
    sys.exit(main())
