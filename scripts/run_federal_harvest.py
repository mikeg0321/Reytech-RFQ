#!/usr/bin/env python3
# DEPRECATED: use scripts/run_harvest.py instead. Will be removed in next cleanup sprint.
"""
run_federal_harvest.py — Federal procurement data harvest from USASpending.gov.

Pulls federal contract awards for Reytech and medical supply categories.
Normalizes to scprs_po_master schema with state='federal'.

Usage:
    python scripts/run_federal_harvest.py              # full harvest
    python scripts/run_federal_harvest.py --dry-run    # preview only
    python scripts/run_federal_harvest.py --reytech    # Reytech wins only
"""

import sys
import os
import json
import sqlite3
import logging
import argparse
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.paths import DATA_DIR
from src.core.db import DB_PATH

LOG_FILE = os.path.join(DATA_DIR, "federal_harvest.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ]
)
log = logging.getLogger("federal_harvest")


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def store_federal_awards(awards: list, conn, dry_run: bool = False) -> int:
    """Store normalized awards to scprs_po_master with state='federal'."""
    if dry_run:
        log.info("[DRY RUN] Would store %d federal awards", len(awards))
        return len(awards)

    count = 0
    now = datetime.now(timezone.utc).isoformat()
    for a in awards:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO scprs_po_master
                (id, pulled_at, po_number, dept_name, institution, supplier,
                 supplier_id, status, start_date, end_date, grand_total,
                 buyer_name, buyer_email, search_term, agency_key,
                 state, jurisdiction, source_system)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (a["id"], now, a.get("po_number", ""),
                  a.get("dept_name", ""), a.get("institution", ""),
                  a.get("supplier", ""), a.get("supplier_id", ""),
                  a.get("status", "Active"), a.get("start_date", ""),
                  a.get("end_date", ""), a.get("grand_total", 0),
                  a.get("buyer_name", ""), a.get("buyer_email", ""),
                  a.get("search_term", ""), a.get("agency_key", "federal"),
                  "federal", "federal", "usaspending"))
            count += 1
        except Exception as e:
            log.debug("Store federal award: %s", e)
    conn.commit()
    return count


def main():
    parser = argparse.ArgumentParser(description="Federal Procurement Harvest")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reytech", action="store_true", help="Reytech wins only")
    parser.add_argument("--days", type=int, default=730, help="Days back to search")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("FEDERAL HARVEST STARTING")
    log.info("=" * 60)

    try:
        from src.agents.usaspending_agent import (
            pull_reytech_federal, search_awards, REYTECH_KEYWORDS
        )
    except ImportError as e:
        log.error("USASpending agent not available: %s", e)
        return

    conn = get_conn()
    total_stored = 0

    # Pull Reytech's federal awards
    log.info("Pulling Reytech federal awards...")
    try:
        result = pull_reytech_federal(days_back=args.days)
        reytech_awards = result.get("reytech_awards", [])
        category_awards = result.get("category_awards", [])

        if reytech_awards:
            stored = store_federal_awards(reytech_awards, conn, args.dry_run)
            total_stored += stored
            log.info("Reytech federal: %d found, %d stored", len(reytech_awards), stored)

        if not args.reytech and category_awards:
            stored = store_federal_awards(category_awards, conn, args.dry_run)
            total_stored += stored
            log.info("Category awards: %d found, %d stored", len(category_awards), stored)

    except Exception as e:
        log.error("Federal harvest failed: %s", e, exc_info=True)

    # Log to harvest_log
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT INTO harvest_log (source_system, state, agency, pos_found,
                started_at, completed_at, tenant_id)
            VALUES ('usaspending', 'federal', 'all', ?, ?, ?, 'reytech')
        """, (total_stored, now, now))
        conn.commit()
    except Exception as e:
        log.debug("Harvest log: %s", e)

    conn.close()

    log.info("=" * 60)
    log.info("FEDERAL HARVEST COMPLETE")
    log.info("  Awards stored: %d", total_stored)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
