#!/usr/bin/env python3
"""Install UNIQUE indexes on issued quote numbers.

Refuses if `quote_counter_audit.py` finds duplicates. Adds:
  - `idx_unique_pc_quote_number` on `price_checks(quote_number)` WHERE NOT NULL
  - `idx_unique_rfq_number` on `rfqs(rfq_number)` WHERE NOT NULL

Rollback: drop the indexes (they're indexes, not constraints — no data is moved).

Usage:
  python scripts/quote_counter_install_unique.py            # install
  python scripts/quote_counter_install_unique.py --drop     # rollback
  python scripts/quote_counter_install_unique.py --force    # skip audit (DANGEROUS)
"""
import os
import sqlite3
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.core.db import DB_PATH  # noqa: E402

PC_INDEX = "idx_unique_pc_quote_number"
RFQ_INDEX = "idx_unique_rfq_number"

PC_INDEX_SQL = (
    f"CREATE UNIQUE INDEX IF NOT EXISTS {PC_INDEX} "
    "ON price_checks(quote_number) "
    "WHERE quote_number IS NOT NULL AND quote_number != ''"
)
RFQ_INDEX_SQL = (
    f"CREATE UNIQUE INDEX IF NOT EXISTS {RFQ_INDEX} "
    "ON rfqs(rfq_number) "
    "WHERE rfq_number IS NOT NULL AND rfq_number != ''"
)


def drop():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"DROP INDEX IF EXISTS {PC_INDEX}")
    conn.execute(f"DROP INDEX IF EXISTS {RFQ_INDEX}")
    conn.commit()
    conn.close()
    print(f"✅ Dropped {PC_INDEX} and {RFQ_INDEX}")
    return 0


def install(force=False):
    if not force:
        # Run the audit first; install only if zero dupes
        from scripts.quote_counter_audit import audit
        rc = audit()
        if rc != 0:
            print()
            print("❌ Audit failed — refusing to install UNIQUE constraints.")
            print("   Use --force to override (NOT recommended).")
            return rc

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(PC_INDEX_SQL)
        conn.execute(RFQ_INDEX_SQL)
        conn.commit()
    except sqlite3.IntegrityError as e:
        conn.close()
        print(f"❌ Index creation failed (dupes exist): {e}")
        return 2
    conn.close()
    print(f"✅ Installed {PC_INDEX} and {RFQ_INDEX}")
    return 0


if __name__ == "__main__":
    if "--drop" in sys.argv:
        sys.exit(drop())
    sys.exit(install(force="--force" in sys.argv))
