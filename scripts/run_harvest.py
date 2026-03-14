#!/usr/bin/env python3
"""
run_harvest.py — Unified procurement harvest entry point.

Usage:
    python scripts/run_harvest.py --vendor-search     # Find Reytech wins
    python scripts/run_harvest.py --connector ca_scprs # Run one connector
    python scripts/run_harvest.py --all               # Run all due connectors
    python scripts/run_harvest.py --health            # Health check only
    python scripts/run_harvest.py --dry-run           # Preview
"""
import sys
import os
import logging
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.paths import DATA_DIR

LOG_FILE = os.path.join(DATA_DIR, "harvest.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ]
)
log = logging.getLogger("harvest")


def main():
    parser = argparse.ArgumentParser(description="Unified Procurement Harvest")
    parser.add_argument("--vendor-search", action="store_true",
                        help="Find Reytech wins across all active connectors")
    parser.add_argument("--connector", type=str,
                        help="Run specific connector by ID")
    parser.add_argument("--all", action="store_true",
                        help="Run all due connectors")
    parser.add_argument("--from-date", type=str,
                        help="Override from_date (YYYY-MM-DD)")
    parser.add_argument("--health", action="store_true",
                        help="Health check only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen")
    args = parser.parse_args()

    from src.core.pull_orchestrator import PullOrchestrator
    orch = PullOrchestrator()

    if args.health:
        status = orch.get_status()
        for cid, s in status.items():
            log.info("  %s [%s] %s: grade=%s records=%s last=%s",
                     cid, s["status"], s["state"],
                     s.get("health_grade", "-"), s.get("record_count", 0),
                     s.get("last_pulled", "never"))
        return

    if args.dry_run:
        log.info("[DRY RUN] Would run:")
        if args.vendor_search:
            log.info("  Vendor search: reytech across all active connectors")
        if args.connector:
            log.info("  Connector: %s", args.connector)
        if args.all:
            from src.core.connector_registry import get_due_connectors
            due = get_due_connectors()
            log.info("  Due connectors: %s", [c["id"] for c in due])
        return

    log.info("=" * 60)
    log.info("PROCUREMENT HARVEST STARTING")
    log.info("=" * 60)

    from_date = None
    if args.from_date:
        from_date = datetime.strptime(args.from_date, "%Y-%m-%d")

    if args.vendor_search:
        log.info("Running vendor search (Reytech)...")
        result = orch.run_vendor_search()
        log.info("Vendor search: %s", result)

    if args.connector:
        log.info("Running connector %s...", args.connector)
        result = orch.run_connector(args.connector, from_date=from_date)
        log.info("Result: %s", result)

    if args.all:
        log.info("Running all due connectors...")
        results = orch.run_due_connectors()
        for r in results:
            log.info("  %s: %s", r.get("connector_id", "?"),
                     "OK" if r.get("ok") else r.get("error", "?"))

    # Rebuild intelligence tables from updated PO data
    try:
        log.info("Rebuilding intelligence tables...")
        # Import existing harvest functions
        sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
        from run_scprs_harvest import (
            build_vendor_intel, build_buyer_intel, build_competitors,
            build_won_quotes_kb, build_scprs_awards, get_conn
        )
        conn = get_conn()
        build_vendor_intel(conn)
        build_buyer_intel(conn)
        build_competitors(conn)
        build_won_quotes_kb(conn)
        build_scprs_awards(conn)
        conn.close()
    except Exception as e:
        log.warning("Intelligence rebuild: %s", e)

    log.info("=" * 60)
    log.info("HARVEST COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
