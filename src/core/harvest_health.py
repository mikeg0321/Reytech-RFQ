"""
harvest_health.py — Health contract validation for procurement data pulls.

Every harvest pull must pass these checks before data is considered trustworthy.
Failed checks are logged as warnings and returned to the caller — they do NOT
block data insertion (the data may still be partially useful).

Usage:
    from src.core.harvest_health import validate_pull
    report = validate_pull("CCHCS", "scprs", "CA", conn)
    if report["grade"] != "A":
        log.warning("Pull health: %s", report)
"""

import logging
import sqlite3
from datetime import datetime, timedelta

log = logging.getLogger("reytech.harvest_health")

# ── Minimum expected POs per pull per agency ─────────────────────────────────

MINIMUM_POS_PER_AGENCY = {
    "CCHCS": 50,
    "CDCR": 30,
    "CalVet": 20,
    "DSH": 10,
}
DEFAULT_MINIMUM_POS = 5

# ── Required fields — no NULLs allowed above 10% threshold ──────────────────

SCHEMA_REQUIRED_FIELDS = [
    "supplier",       # vendor_name in po_master
    "dept_name",      # agency
    "start_date",     # award_date
    "grand_total",    # total_value
    "state",
    "source_system",
]


def validate_pull(agency: str, source_system: str, state: str,
                  conn, rows_before: int = 0) -> dict:
    """Run all 5 health checks on a completed pull.

    Input:
        agency: agency code (e.g. 'CCHCS')
        source_system: 'scprs' or 'usaspending'
        state: 'CA' or 'federal'
        conn: open sqlite3 connection
        rows_before: row count before this pull (for check 5)
    Output:
        dict with {grade, checks: [{name, passed, detail}], issues: [str]}
    """
    checks = []
    issues = []

    # Scope query to this agency + source
    where = "WHERE agency_key = ? AND source_system = ? AND state = ?"
    params = (agency, source_system, state)

    try:
        total = conn.execute(
            f"SELECT COUNT(*) FROM scprs_po_master {where}", params
        ).fetchone()[0]
    except Exception as e:
        return {"grade": "F", "checks": [], "issues": [f"Query failed: {e}"],
                "total_rows": 0}

    # ── Check 1: Row count >= minimum ────────────────────────────────────
    minimum = MINIMUM_POS_PER_AGENCY.get(agency, DEFAULT_MINIMUM_POS)
    passed = total >= minimum
    detail = f"{total} rows (minimum: {minimum})"
    checks.append({"name": "row_count", "passed": passed, "detail": detail})
    if not passed:
        issues.append(f"Low row count: {detail}")

    # ── Check 2: No NULLs in required fields > 10% ──────────────────────
    null_issues = []
    for field in SCHEMA_REQUIRED_FIELDS:
        try:
            nulls = conn.execute(
                f"SELECT COUNT(*) FROM scprs_po_master {where} "
                f"AND ({field} IS NULL OR {field} = '')", params
            ).fetchone()[0]
            pct = (nulls / total * 100) if total > 0 else 0
            if pct > 10:
                null_issues.append(f"{field}: {nulls}/{total} ({pct:.0f}%) NULL")
        except Exception as _e:
            log.debug("suppressed: %s", _e)
    passed = len(null_issues) == 0
    detail = "All required fields < 10% NULL" if passed else "; ".join(null_issues)
    checks.append({"name": "null_check", "passed": passed, "detail": detail})
    if not passed:
        issues.append(f"NULL fields: {detail}")

    # ── Check 3: award_date within expected range ────────────────────────
    date_ok = True
    date_detail = ""
    try:
        dates = conn.execute(
            f"SELECT start_date FROM scprs_po_master {where} "
            f"AND start_date IS NOT NULL AND start_date != '' LIMIT 500", params
        ).fetchall()
        if dates:
            distinct_dates = set(r[0] for r in dates)
            if len(distinct_dates) <= 1 and len(dates) > 5:
                date_ok = False
                date_detail = f"All {len(dates)} rows have same date: {dates[0][0]}"
            # Check for future dates (handle MM/DD/YYYY and YYYY-MM-DD)
            now = datetime.now()
            future_count = 0
            for r in dates:
                if not r[0]:
                    continue
                try:
                    d = r[0]
                    if "/" in d:
                        parts = d.split("/")
                        dt = datetime(int(parts[2]), int(parts[0]), int(parts[1]))
                    elif "-" in d:
                        dt = datetime.fromisoformat(d[:10])
                    else:
                        continue
                    if dt > now + timedelta(days=30):  # 30-day grace period
                        future_count += 1
                except Exception as _e:
                    log.debug("suppressed: %s", _e)
            if future_count > len(dates) * 0.1:
                date_ok = False
                date_detail = f"{future_count} future dates out of {len(dates)}"
            if date_ok:
                date_detail = f"{len(distinct_dates)} distinct dates across {len(dates)} rows"
        else:
            date_detail = "No date data"
    except Exception as e:
        date_detail = f"Date check error: {e}"
    checks.append({"name": "date_range", "passed": date_ok, "detail": date_detail})
    if not date_ok:
        issues.append(f"Date issue: {date_detail}")

    # ── Check 4: total_value numeric and > 0 on > 80% of rows ───────────
    value_ok = True
    value_detail = ""
    try:
        positive = conn.execute(
            f"SELECT COUNT(*) FROM scprs_po_master {where} "
            f"AND grand_total > 0", params
        ).fetchone()[0]
        pct = (positive / total * 100) if total > 0 else 0
        value_ok = pct >= 80
        value_detail = f"{positive}/{total} ({pct:.0f}%) have total_value > 0"
    except Exception as e:
        value_detail = f"Value check error: {e}"
    checks.append({"name": "value_positive", "passed": value_ok, "detail": value_detail})
    if not value_ok:
        issues.append(f"Value issue: {value_detail}")

    # ── Check 5: New rows inserted (not all duplicates) ──────────────────
    new_rows = total - rows_before
    insert_ok = new_rows > 0 or rows_before == 0
    insert_detail = f"{new_rows} new rows (was {rows_before}, now {total})"
    checks.append({"name": "new_inserts", "passed": insert_ok, "detail": insert_detail})
    if not insert_ok:
        issues.append(f"No new data: {insert_detail}")

    # ── Grade ────────────────────────────────────────────────────────────
    passed_count = sum(1 for c in checks if c["passed"])
    if passed_count == 5:
        grade = "A"
    elif passed_count >= 4:
        grade = "B"
    elif passed_count >= 3:
        grade = "C"
    else:
        grade = "F"

    report = {
        "grade": grade,
        "agency": agency,
        "source_system": source_system,
        "state": state,
        "total_rows": total,
        "checks": checks,
        "issues": issues,
        "passed": passed_count,
        "total_checks": len(checks),
    }

    level = logging.INFO if grade in ("A", "B") else logging.WARNING
    log.log(level, "Harvest health [%s] %s/%s/%s: %d/%d checks passed — %s",
            grade, agency, source_system, state, passed_count, len(checks),
            "; ".join(issues) if issues else "all clear")

    return report


def validate_all_agencies(conn, source_system: str = "scprs",
                          state: str = "CA") -> dict:
    """Run health checks for every agency that has data. Returns summary."""
    rows = conn.execute(
        "SELECT DISTINCT agency_key FROM scprs_po_master "
        "WHERE source_system = ? AND state = ?",
        (source_system, state)
    ).fetchall()

    reports = {}
    for r in rows:
        agency = r[0]
        if agency:
            reports[agency] = validate_pull(agency, source_system, state, conn)

    grades = [r["grade"] for r in reports.values()]
    return {
        "agencies_checked": len(reports),
        "grades": {g: grades.count(g) for g in set(grades)},
        "reports": reports,
    }
