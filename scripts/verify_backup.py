"""
verify_backup.py — Verify Google Drive backup is complete and restorable.

Downloads the latest backup from Google Drive, restores to a temp SQLite DB,
and verifies row counts, schema integrity, and critical data.

Usage:
    python scripts/verify_backup.py                    # Verify local backup
    python scripts/verify_backup.py --db /path/to.db   # Verify specific DB file
    python scripts/verify_backup.py --download         # Download from Drive first

Exit codes:
    0 = all checks passed
    1 = verification failed
    2 = backup not found
"""

import argparse
import os
import sqlite3
import sys
import tempfile
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Expected Tables ─────────────────────────────────────────────────────────
# Minimum set of tables that must exist in a valid backup.
REQUIRED_TABLES = [
    "quotes",
    "price_checks",
    "rfqs",
    "contacts",
    "price_history",
    "app_settings",
    "notifications",
    "email_log",
    "orders",
]

# Tables with expected minimum row counts (production should have more)
MIN_ROW_COUNTS = {
    "app_settings": 3,   # quote_counter_seq, quote_counter_year, etc.
}


def verify_database(db_path: str) -> dict:
    """Verify a SQLite database backup is valid and complete.

    Returns dict with:
        ok: bool
        checks: list of {name, status, detail}
        tables: dict of {table_name: row_count}
        errors: list of error strings
    """
    result = {
        "ok": True,
        "checks": [],
        "tables": {},
        "errors": [],
        "db_path": db_path,
        "verified_at": datetime.now().isoformat(),
    }

    def _check(name, passed, detail=""):
        status = "PASS" if passed else "FAIL"
        result["checks"].append({"name": name, "status": status, "detail": detail})
        if not passed:
            result["ok"] = False
            result["errors"].append(f"{name}: {detail}")

    # Check 1: File exists and is readable
    _check("file_exists", os.path.exists(db_path),
           f"File: {db_path}")
    if not os.path.exists(db_path):
        return result

    # Check 2: File size > 0
    size = os.path.getsize(db_path)
    _check("file_not_empty", size > 0, f"Size: {size:,} bytes")
    if size == 0:
        return result

    # Check 3: Valid SQLite header
    try:
        with open(db_path, "rb") as f:
            header = f.read(16)
        is_sqlite = header[:6] == b"SQLite"
        _check("sqlite_header", is_sqlite,
               f"Header: {header[:16]}")
        if not is_sqlite:
            return result
    except Exception as e:
        _check("sqlite_header", False, str(e))
        return result

    # Check 4: Can open and query
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("SELECT 1")
        _check("db_opens", True)
    except Exception as e:
        _check("db_opens", False, str(e))
        return result

    # Check 5: Required tables exist
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = {row[0] for row in cursor.fetchall()}

    for table in REQUIRED_TABLES:
        exists = table in existing_tables
        _check(f"table_{table}", exists,
               "exists" if exists else "MISSING")

    # Check 6: Row counts
    for table in existing_tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
            result["tables"][table] = count
        except Exception:
            result["tables"][table] = -1

    # Check 7: Minimum row counts
    for table, min_count in MIN_ROW_COUNTS.items():
        actual = result["tables"].get(table, 0)
        _check(f"rows_{table}", actual >= min_count,
               f"{actual} rows (min {min_count})")

    # Check 8: Quote counter is valid
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key='quote_counter_seq'"
        ).fetchone()
        if row:
            seq = int(row[0])
            _check("quote_counter", seq >= 0,
                   f"quote_counter_seq = {seq}")
        else:
            _check("quote_counter", False, "quote_counter_seq not found")
    except Exception as e:
        _check("quote_counter", False, str(e))

    # Check 9: Schema integrity (PRAGMA integrity_check)
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        _check("integrity_check", integrity == "ok",
               integrity[:100])
    except Exception as e:
        _check("integrity_check", False, str(e))

    # Check 10: WAL mode check
    try:
        journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
        _check("wal_mode", journal in ("wal", "delete"),
               f"journal_mode = {journal}")
    except Exception as e:
        _check("wal_mode", False, str(e))

    conn.close()
    return result


def download_latest_backup(dest_path: str) -> bool:
    """Download the latest backup from Google Drive.

    Uses the same credentials as the app's Drive integration.
    Returns True if downloaded successfully.
    """
    try:
        from src.core.gmail_api import get_drive_service
        service = get_drive_service()

        # Search for the most recent backup file
        results = service.files().list(
            q="name contains 'reytech' and name contains '.db' and mimeType='application/octet-stream'",
            orderBy="modifiedTime desc",
            pageSize=1,
            fields="files(id, name, modifiedTime, size)",
        ).execute()

        files = results.get("files", [])
        if not files:
            print("No backup files found on Google Drive")
            return False

        latest = files[0]
        print(f"Found backup: {latest['name']} "
              f"(modified: {latest['modifiedTime']}, "
              f"size: {int(latest.get('size', 0)):,} bytes)")

        # Download
        from io import BytesIO
        from googleapiclient.http import MediaIoBaseDownload

        request = service.files().get_media(fileId=latest["id"])
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        with open(dest_path, "wb") as f:
            f.write(fh.getvalue())

        print(f"Downloaded to: {dest_path}")
        return True

    except ImportError:
        print("Google Drive API not available (missing google-api-python-client)")
        return False
    except Exception as e:
        print(f"Download failed: {e}")
        return False


def print_report(result: dict):
    """Print a human-readable verification report."""
    print()
    print("═" * 60)
    print("  Backup Verification Report")
    print(f"  DB: {result['db_path']}")
    print(f"  Time: {result['verified_at']}")
    print("═" * 60)

    for check in result["checks"]:
        icon = "✅" if check["status"] == "PASS" else "❌"
        detail = f" — {check['detail']}" if check["detail"] else ""
        print(f"  {icon} {check['name']}{detail}")

    if result["tables"]:
        print()
        print("  Table Row Counts:")
        for table, count in sorted(result["tables"].items()):
            print(f"    {table}: {count:,}")

    print()
    if result["ok"]:
        print("  ✅ BACKUP VERIFIED — all checks passed")
    else:
        print("  ❌ BACKUP FAILED — issues found:")
        for err in result["errors"]:
            print(f"    • {err}")
    print("═" * 60)


def main():
    parser = argparse.ArgumentParser(description="Verify Reytech backup")
    parser.add_argument("--db", help="Path to DB file to verify")
    parser.add_argument("--download", action="store_true",
                        help="Download latest backup from Google Drive first")
    args = parser.parse_args()

    if args.download:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        if not download_latest_backup(db_path):
            sys.exit(2)
    elif args.db:
        db_path = args.db
    else:
        # Default: check the local production DB
        db_path = os.path.join(
            os.path.dirname(__file__), "..", "data", "reytech.db")
        if not os.path.exists(db_path):
            # Try Railway volume path
            db_path = "/data/reytech.db"

    result = verify_database(db_path)
    print_report(result)
    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()
