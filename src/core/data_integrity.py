"""
data_integrity.py — Cross-table data consistency checks.
Verifies referential integrity, orphaned records, and data quality
issues that would otherwise cause silent failures in the pipeline.
"""
import json
import logging
from datetime import datetime

log = logging.getLogger("reytech.integrity")


def _get_db():
    from src.core.db import get_db
    return get_db()


def run_integrity_checks() -> dict:
    """Run all data integrity checks. Returns summary with issues."""
    checks = []
    
    def _check(name, fn):
        try:
            result = fn()
            checks.append({"name": name, "ok": result.get("ok", True), **result})
        except Exception as e:
            checks.append({"name": name, "ok": False, "error": str(e)})

    _check("orphaned_order_quotes", _check_orphaned_order_quotes)
    _check("duplicate_quote_numbers", _check_duplicate_quote_numbers)
    _check("missing_order_items", _check_missing_order_items)
    _check("stale_pending_quotes", _check_stale_pending_quotes)
    _check("revenue_log_consistency", _check_revenue_consistency)
    _check("table_health", _check_table_health)

    failed = [c for c in checks if not c["ok"]]
    return {
        "ok": len(failed) == 0,
        "total_checks": len(checks),
        "passed": len(checks) - len(failed),
        "failed": len(failed),
        "checks": checks,
        "checked_at": datetime.now().isoformat(),
    }


def _check_orphaned_order_quotes() -> dict:
    """Orders referencing quotes that don't exist."""
    with _get_db() as conn:
        rows = conn.execute("""
            SELECT o.id, o.quote_number 
            FROM orders o
            WHERE o.quote_number IS NOT NULL 
              AND o.quote_number != ''
              AND o.quote_number NOT IN (SELECT quote_number FROM quotes WHERE quote_number IS NOT NULL)
        """).fetchall()
        orphans = [{"order_id": r["id"], "quote_number": r["quote_number"]} for r in rows]
        return {"ok": len(orphans) == 0, "count": len(orphans), "orphans": orphans[:10]}


def _check_duplicate_quote_numbers() -> dict:
    """Quote numbers that appear more than once."""
    with _get_db() as conn:
        rows = conn.execute("""
            SELECT quote_number, COUNT(*) as cnt
            FROM quotes
            WHERE quote_number IS NOT NULL AND quote_number != ''
            GROUP BY quote_number HAVING cnt > 1
            ORDER BY cnt DESC LIMIT 10
        """).fetchall()
        dupes = [{"quote_number": r["quote_number"], "count": r["cnt"]} for r in rows]
        return {"ok": len(dupes) == 0, "count": len(dupes), "duplicates": dupes}


def _check_missing_order_items() -> dict:
    """Orders with total > 0 but no line items."""
    with _get_db() as conn:
        rows = conn.execute("""
            SELECT id, total, items
            FROM orders
            WHERE total > 0 AND (items IS NULL OR items = '' OR items = '[]')
        """).fetchall()
        missing = [{"order_id": r["id"], "total": r["total"]} for r in rows]
        return {"ok": len(missing) == 0, "count": len(missing), "orders": missing[:10]}


def _check_stale_pending_quotes() -> dict:
    """Quotes in 'pending' or 'draft' status older than 90 days."""
    with _get_db() as conn:
        rows = conn.execute("""
            SELECT COUNT(*) as cnt, 
                   MIN(created_at) as oldest,
                   COALESCE(SUM(total), 0) as total_value
            FROM quotes
            WHERE status IN ('pending', 'draft', 'sent')
              AND is_test = 0
              AND created_at < date('now', '-90 days')
        """).fetchone()
        count = rows["cnt"] if rows else 0
        return {
            "ok": count < 20,  # warn if > 20 stale quotes
            "count": count,
            "oldest": rows["oldest"] if rows else None,
            "total_value": round(rows["total_value"], 2) if rows else 0,
            "message": f"{count} quotes older than 90 days still open" if count else "No stale quotes"
        }


def _check_revenue_consistency() -> dict:
    """Verify revenue_log entries match order totals."""
    with _get_db() as conn:
        # Revenue log total
        rl = conn.execute("""
            SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as cnt
            FROM revenue_log WHERE amount > 0
        """).fetchone()
        
        # Orders total  
        ot = conn.execute("""
            SELECT COALESCE(SUM(total), 0) as total, COUNT(*) as cnt
            FROM orders WHERE total > 0
        """).fetchone()
        
        rl_total = round(rl["total"], 2) if rl else 0
        ot_total = round(ot["total"], 2) if ot else 0
        diff = abs(rl_total - ot_total)
        
        return {
            "ok": True,  # informational, not a failure
            "revenue_log_total": rl_total,
            "revenue_log_entries": rl["cnt"] if rl else 0,
            "orders_total": ot_total,
            "orders_count": ot["cnt"] if ot else 0,
            "difference": round(diff, 2),
        }


def _check_table_health() -> dict:
    """Verify all expected tables exist and have reasonable row counts."""
    expected = [
        "quotes", "contacts", "orders", "rfqs", "revenue_log",
        "price_history", "price_checks", "activity_log",
        "audit_trail",
    ]
    
    with _get_db() as conn:
        results = {}
        missing = []
        for table in expected:
            try:
                row = conn.execute(
                    f"SELECT COUNT(*) as cnt FROM {table}"
                ).fetchone()
                results[table] = row["cnt"]
            except Exception:
                missing.append(table)
                results[table] = -1
        
        return {
            "ok": len(missing) == 0,
            "tables": results,
            "missing": missing,
        }
