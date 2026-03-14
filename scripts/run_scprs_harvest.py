#!/usr/bin/env python3
# DEPRECATED: use scripts/run_harvest.py instead. Will be removed in next cleanup sprint.
"""
run_scprs_harvest.py — SCPRS Historical Harvest Runner

Processes raw PO data from scprs_po_master/scprs_po_lines into
intelligence tables: vendor_intel, buyer_intel, competitors,
won_quotes_kb, scprs_awards.

Safe to run multiple times (idempotent — uses INSERT OR REPLACE).

Usage:
    python scripts/run_scprs_harvest.py              # process existing data
    python scripts/run_scprs_harvest.py --pull        # pull new + process
    python scripts/run_scprs_harvest.py --dry-run     # show what would happen
"""

import sys
import os
import json
import sqlite3
import logging
import argparse
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.paths import DATA_DIR
from src.core.db import DB_PATH

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_FILE = os.path.join(DATA_DIR, "scprs_harvest.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ]
)
log = logging.getLogger("harvest")

REYTECH_PATTERNS = ["reytech", "rey tech", "rey-tech"]


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30); conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ── Step 1: Pull new data (optional) ────────────────────────────────────────

def pull_new_data(dry_run=False):
    """Run scprs_universal_pull to fetch new POs from SCPRS."""
    if dry_run:
        log.info("[DRY RUN] Would run scprs_universal_pull.run_universal_pull('all')")
        return {"ok": True, "dry_run": True}
    try:
        from src.agents.scprs_universal_pull import run_universal_pull
        log.info("Starting SCPRS universal pull (all priorities)...")
        result = run_universal_pull("all")
        log.info("Pull result: %s", json.dumps(result, default=str)[:500])
        return result
    except Exception as e:
        log.error("Pull failed: %s", e)
        return {"ok": False, "error": str(e)}


# ── Step 2: Build vendor_intel from scprs_po_master ─────────────────────────

def build_vendor_intel(conn, dry_run=False):
    """Aggregate vendor stats from PO data."""
    log.info("Building vendor_intel...")
    rows = conn.execute("""
        SELECT supplier, supplier_id,
               m.dept_name as agency,
               l.category,
               COUNT(DISTINCT m.po_number) as win_count,
               SUM(m.grand_total) as total_value,
               AVG(l.unit_price) as avg_price,
               MIN(m.start_date) as first_seen,
               MAX(m.start_date) as last_seen
        FROM scprs_po_master m
        JOIN scprs_po_lines l ON l.po_id = m.id
        WHERE m.supplier IS NOT NULL AND m.supplier != ''
        GROUP BY m.supplier, m.dept_name
    """).fetchall()

    if dry_run:
        log.info("[DRY RUN] Would insert %d vendor_intel rows", len(rows))
        return len(rows)

    count = 0
    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        conn.execute("""
            INSERT OR REPLACE INTO vendor_intel
            (vendor_name, vendor_code, agency, category, win_count, total_value,
             avg_price, first_seen, last_seen, tenant_id, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,'reytech',?)
        """, (r["supplier"], r["supplier_id"], r["agency"], r["category"],
              r["win_count"], r["total_value"], r["avg_price"],
              r["first_seen"], r["last_seen"], now))
        count += 1
    conn.commit()
    log.info("vendor_intel: %d rows written", count)
    return count


# ── Step 3: Build buyer_intel from scprs_po_master ──────────────────────────

def build_buyer_intel(conn, dry_run=False):
    """Aggregate buyer stats from PO data."""
    log.info("Building buyer_intel...")
    rows = conn.execute("""
        SELECT buyer_email, buyer_name, dept_name as agency, agency_key,
               COUNT(DISTINCT m.po_number) as rfq_count,
               SUM(grand_total) as total_spend,
               MAX(start_date) as last_purchase,
               GROUP_CONCAT(DISTINCT l.category) as categories
        FROM scprs_po_master m
        LEFT JOIN scprs_po_lines l ON l.po_id = m.id
        WHERE buyer_email IS NOT NULL AND buyer_email != ''
        GROUP BY buyer_email
    """).fetchall()

    if dry_run:
        log.info("[DRY RUN] Would insert %d buyer_intel rows", len(rows))
        return len(rows)

    count = 0
    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        # Collect items purchased for this buyer
        items = conn.execute("""
            SELECT DISTINCT l.description FROM scprs_po_lines l
            JOIN scprs_po_master m ON l.po_id = m.id
            WHERE m.buyer_email = ? LIMIT 20
        """, (r["buyer_email"],)).fetchall()
        items_json = json.dumps([i["description"][:100] for i in items])

        conn.execute("""
            INSERT OR REPLACE INTO buyer_intel
            (buyer_name, buyer_email, agency, agency_code, items_purchased,
             categories, total_spend, rfq_count, last_purchase, tenant_id, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,'reytech',?)
        """, (r["buyer_name"], r["buyer_email"], r["agency"], r["agency_key"],
              items_json, r["categories"], r["total_spend"],
              r["rfq_count"], r["last_purchase"], now))
        count += 1
    conn.commit()
    log.info("buyer_intel: %d rows written", count)
    return count


# ── Step 4: Build competitors from vendor_intel ─────────────────────────────

def build_competitors(conn, dry_run=False):
    """Build competitor profiles from vendor_intel aggregates."""
    log.info("Building competitors...")
    rows = conn.execute("""
        SELECT vendor_name, vendor_code,
               GROUP_CONCAT(DISTINCT agency) as primary_agencies,
               GROUP_CONCAT(DISTINCT category) as primary_categories,
               SUM(win_count) as total_wins,
               SUM(total_value) as total_value,
               MAX(last_seen) as last_win
        FROM vendor_intel
        WHERE vendor_name NOT LIKE '%reytech%'
          AND vendor_name NOT LIKE '%rey tech%'
        GROUP BY vendor_name
        HAVING total_wins >= 2
        ORDER BY total_wins DESC
    """).fetchall()

    if dry_run:
        log.info("[DRY RUN] Would insert %d competitor rows", len(rows))
        return len(rows)

    count = 0
    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        conn.execute("""
            INSERT OR REPLACE INTO competitors
            (vendor_name, vendor_code, primary_agencies, primary_categories,
             win_rate, last_win, tenant_id, updated_at)
            VALUES (?,?,?,?,?,?,'reytech',?)
        """, (r["vendor_name"], r["vendor_code"], r["primary_agencies"],
              r["primary_categories"], r["total_wins"], r["last_win"], now))
        count += 1
    conn.commit()
    log.info("competitors: %d rows written", count)
    return count


# ── Step 5: Build won_quotes_kb from scprs_po_lines ────────────────────────

def build_won_quotes_kb(conn, dry_run=False):
    """Build knowledge base of winning prices per item."""
    log.info("Building won_quotes_kb...")
    rows = conn.execute("""
        SELECT l.description, l.item_id, m.dept_name as agency,
               l.unit_price, m.supplier, m.po_number, m.start_date,
               l.quantity
        FROM scprs_po_lines l
        JOIN scprs_po_master m ON l.po_id = m.id
        WHERE l.unit_price > 0 AND l.description IS NOT NULL
              AND l.description != ''
    """).fetchall()

    if dry_run:
        log.info("[DRY RUN] Would insert %d won_quotes_kb rows", len(rows))
        return len(rows)

    count = 0
    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        is_reytech = any(p in (r["supplier"] or "").lower() for p in REYTECH_PATTERNS)
        import hashlib
        row_id = hashlib.md5(
            f"{r['po_number']}:{r['description'][:50]}:{r['unit_price']}".encode()
        ).hexdigest()[:16]

        conn.execute("""
            INSERT OR IGNORE INTO won_quotes_kb
            (item_description, nsn, agency, winning_price, winning_vendor,
             reytech_won, award_date, po_number, tenant_id, created_at)
            VALUES (?,?,?,?,?,?,?,?,'reytech',?)
        """, (r["description"][:200], r["item_id"], r["agency"],
              r["unit_price"], r["supplier"], 1 if is_reytech else 0,
              r["start_date"], r["po_number"], now))
        count += 1
    conn.commit()
    log.info("won_quotes_kb: %d rows written", count)
    return count


# ── Step 6: Build scprs_awards from scprs_po_master ────────────────────────

def build_scprs_awards(conn, dry_run=False):
    """Build awards table from PO master data."""
    log.info("Building scprs_awards...")
    rows = conn.execute("""
        SELECT m.po_number, dept_name as agency, agency_key,
               supplier, supplier_id, start_date, grand_total,
               COUNT(l.id) as item_count
        FROM scprs_po_master m
        LEFT JOIN scprs_po_lines l ON l.po_id = m.id
        GROUP BY m.po_number
    """).fetchall()

    if dry_run:
        log.info("[DRY RUN] Would insert %d scprs_awards rows", len(rows))
        return len(rows)

    count = 0
    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        # Derive fiscal year from start_date (MM/DD/YYYY format)
        fy = ""
        try:
            sd = r["start_date"] or ""
            if "/" in sd:
                parts = sd.split("/")
                year = int(parts[2])
                month = int(parts[0])
                fy = f"FY{year}-{year+1}" if month >= 7 else f"FY{year-1}-{year}"
        except Exception:
            pass

        conn.execute("""
            INSERT OR IGNORE INTO scprs_awards
            (id, po_number, agency, agency_code, vendor_name, vendor_code,
             award_date, fiscal_year, total_value, item_count, tenant_id, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,'reytech',?)
        """, (r["po_number"], r["po_number"], r["agency"], r["agency_key"],
              r["supplier"], r["supplier_id"], r["start_date"],
              fy, r["grand_total"], r["item_count"], now))
        count += 1
    conn.commit()
    log.info("scprs_awards: %d rows written", count)
    return count


# ── Agency pull via intelligence engine ──────────────────────────────────────

def pull_agency_scprs(agency_code: str, days_back: int = 730, dry_run: bool = False) -> dict:
    """Pull one agency from SCPRS via the intelligence engine."""
    if dry_run:
        log.info("[DRY RUN] Would pull %s (%d days back)", agency_code, days_back)
        return {"ok": True, "dry_run": True, "agency": agency_code}
    t0 = time.time()
    try:
        from src.agents.scprs_intelligence_engine import pull_agency
        result = pull_agency(agency_code)
        duration = round(time.time() - t0, 1)
        pos = result.get("new_pos", 0)
        lines = result.get("new_lines", 0)
        log.info("Agency %s: %d POs, %d lines in %.1fs", agency_code, pos, lines, duration)
        # Log to harvest_log + run health check
        try:
            conn = get_conn()
            now = datetime.now(timezone.utc).isoformat()
            conn.execute("""
                INSERT INTO harvest_log (source_system, state, agency, pos_found,
                    lines_found, started_at, completed_at, duration_seconds, tenant_id)
                VALUES ('scprs', 'CA', ?, ?, ?, ?, ?, ?, 'reytech')
            """, (agency_code, pos, lines, now, now, duration))
            conn.commit()
            # Health contract validation
            from src.core.harvest_health import validate_pull
            health = validate_pull(agency_code, "scprs", "CA", conn)
            result["health_grade"] = health["grade"]
            result["health_issues"] = health["issues"]
            conn.close()
        except Exception as e:
            log.debug("Harvest log/health: %s", e)
        return result
    except Exception as e:
        duration = round(time.time() - t0, 1)
        log.error("Agency %s failed in %.1fs: %s", agency_code, duration, e)
        return {"ok": False, "error": str(e), "agency": agency_code}


def pull_all_agencies(priority: str = "P1", days_back: int = 730,
                      max_workers: int = 1, dry_run: bool = False) -> dict:
    """Pull all CA agencies up to a priority level. Returns summary."""
    from src.core.ca_agencies import get_agencies_by_priority, HIGH_VALUE_AGENCIES

    agencies = get_agencies_by_priority(priority)
    log.info("Pulling %d CA agencies (priority <= %s, %d workers)",
             len(agencies), priority, max_workers)

    results = {}
    total_pos = 0
    errors = []

    def _pull_one(code):
        db = HIGH_VALUE_AGENCIES if hasattr(HIGH_VALUE_AGENCIES, '__contains__') else set()
        days = 1095 if code in db else days_back
        return code, pull_agency_scprs(code, days_back=days, dry_run=dry_run)

    if max_workers <= 1:
        for code in agencies:
            c, r = _pull_one(code)
            results[c] = r
            if r.get("ok"):
                total_pos += r.get("new_pos", 0)
            else:
                errors.append(c)
            time.sleep(2)  # Rate limit
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_pull_one, code): code for code in agencies}
            for future in as_completed(futures):
                try:
                    c, r = future.result()
                    results[c] = r
                    if r.get("ok"):
                        total_pos += r.get("new_pos", 0)
                    else:
                        errors.append(c)
                except Exception as e:
                    code = futures[future]
                    results[code] = {"ok": False, "error": str(e)}
                    errors.append(code)

    return {"total_agencies": len(agencies), "total_pos": total_pos,
            "errors": errors, "results": results}


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SCPRS Historical Harvest Runner")
    parser.add_argument("--pull", action="store_true", help="Pull new data from SCPRS first")
    parser.add_argument("--pull-all", action="store_true", help="Pull ALL CA agencies from SCPRS")
    parser.add_argument("--priority", default="P1", help="Max priority level (P0/P1/P2)")
    parser.add_argument("--workers", type=int, default=1, help="Parallel pull workers (1-4)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen")
    parser.add_argument("--health", action="store_true", help="Run health check on existing data only")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCPRS HARVEST STARTING")
    log.info("DB: %s", DB_PATH)
    log.info("=" * 60)

    # Health-only mode
    if args.health:
        conn = get_conn()
        from src.core.harvest_health import validate_all_agencies
        summary = validate_all_agencies(conn, "scprs", "CA")
        log.info("Health check: %d agencies", summary["agencies_checked"])
        log.info("Grades: %s", summary["grades"])
        for agency, report in summary["reports"].items():
            status = "PASS" if report["grade"] in ("A", "B") else "WARN" if report["grade"] == "C" else "FAIL"
            log.info("  [%s] %s: %s (%d/%d checks) %s",
                     report["grade"], agency, status,
                     report["passed"], report["total_checks"],
                     " — " + "; ".join(report["issues"]) if report["issues"] else "")
        conn.close()
        return

    # Seed agency registry
    try:
        from src.core.ca_agencies import seed_agency_registry
        conn = get_conn()
        seed_agency_registry(conn)
        conn.close()
        log.info("CA agency registry seeded")
    except Exception as e:
        log.warning("Agency registry seed: %s", e)

    # Step 1: Pull (single or all agencies)
    if args.pull_all:
        pull_result = pull_all_agencies(
            priority=args.priority,
            max_workers=min(args.workers, 4),
            dry_run=args.dry_run)
        log.info("Pull-all result: %d agencies, %d new POs, %d errors",
                 pull_result["total_agencies"], pull_result["total_pos"],
                 len(pull_result["errors"]))
        if pull_result["errors"]:
            log.warning("Failed agencies: %s", pull_result["errors"])
    elif args.pull:
        pull_result = pull_new_data(dry_run=args.dry_run)
        if not pull_result.get("ok") and not args.dry_run:
            log.warning("Pull had issues: %s", pull_result)

    # Step 2-6: Process existing data into intelligence tables
    conn = get_conn()

    po_count = conn.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]
    line_count = conn.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]
    log.info("Baseline: %d POs, %d lines in scprs_po_master/lines", po_count, line_count)

    if po_count == 0:
        log.warning("No PO data to process. Run with --pull or --pull-all first.")
        conn.close()
        return

    try:
        vendor_count = build_vendor_intel(conn, dry_run=args.dry_run)
        buyer_count = build_buyer_intel(conn, dry_run=args.dry_run)
        competitor_count = build_competitors(conn, dry_run=args.dry_run)
        kb_count = build_won_quotes_kb(conn, dry_run=args.dry_run)
        award_count = build_scprs_awards(conn, dry_run=args.dry_run)
    except Exception as e:
        log.error("Harvest processing failed: %s", e, exc_info=True)
        conn.close()
        return

    # Tag Reytech wins
    reytech_wins = 0
    if not args.dry_run:
        for pattern in REYTECH_PATTERNS:
            r = conn.execute(
                "SELECT COUNT(*) FROM scprs_po_master WHERE LOWER(supplier) LIKE ?",
                (f"%{pattern}%",)).fetchone()[0]
            reytech_wins += r

    conn.close()

    # Print summary
    log.info("=" * 60)
    log.info("HARVEST COMPLETE%s", " (DRY RUN)" if args.dry_run else "")
    log.info("  scprs_po_master: %d rows", po_count)
    log.info("  scprs_po_lines:  %d rows", line_count)
    log.info("  scprs_awards:    %d rows", award_count)
    log.info("  vendor_intel:    %d vendors", vendor_count)
    log.info("  won_quotes_kb:   %d items", kb_count)
    log.info("  buyer_intel:     %d buyers", buyer_count)
    log.info("  competitors:     %d vendors tracked", competitor_count)
    log.info("  Reytech wins:    %d POs", reytech_wins)

    # Health contract validation
    if not args.dry_run:
        try:
            health_conn = get_conn()
            from src.core.harvest_health import validate_all_agencies
            summary = validate_all_agencies(health_conn, "scprs", "CA")
            log.info("  Health: %s", summary["grades"])
            for agency, report in summary["reports"].items():
                if report["grade"] not in ("A", "B"):
                    log.warning("  [%s] %s: %s", report["grade"], agency,
                                "; ".join(report["issues"]))
            health_conn.close()
        except Exception as e:
            log.debug("Health check: %s", e)

    log.info("=" * 60)


if __name__ == "__main__":
    main()
