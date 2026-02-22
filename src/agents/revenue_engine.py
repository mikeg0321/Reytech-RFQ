"""
src/agents/revenue_engine.py — Revenue Dashboard Engine (PRD-28 WI-4)

Provides:
  1. Revenue reconciliation (sync DB + JSON)
  2. Pipeline forecast (quote totals × win probability)
  3. Margin analysis per deal
  4. Monthly/quarterly breakdowns
  5. Goal tracking ($2M target)
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from collections import defaultdict

log = logging.getLogger("revenue_engine")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import get_db
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")
    from contextlib import contextmanager
    @contextmanager
    def get_db():
        conn = sqlite3.connect(os.path.join(DATA_DIR, "reytech.db"), timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


# ── Config ────────────────────────────────────────────────────────────────────
ANNUAL_GOAL = 2_000_000  # $2M target
FISCAL_YEAR_START = "2025-07-01"  # CA fiscal year


# ── Revenue Reconciliation ────────────────────────────────────────────────────

def reconcile_revenue() -> dict:
    """Sync revenue from multiple sources into revenue_log."""
    now = datetime.now(timezone.utc).isoformat()
    synced = 0

    try:
        with get_db() as conn:
            existing_ids = set(r[0] for r in conn.execute(
                "SELECT id FROM revenue_log"
            ).fetchall())

            # Source 1: Won quotes
            won = conn.execute("""
                SELECT quote_number, total, agency, institution, po_number,
                       updated_at, total_cost, gross_profit, margin_pct
                FROM quotes
                WHERE status = 'won' AND total > 0
            """).fetchall()

            for q in won:
                rev_id = f"rev-{q['quote_number']}"
                if rev_id in existing_ids:
                    continue
                conn.execute("""
                    INSERT INTO revenue_log
                    (id, logged_at, amount, description, source, quote_number,
                     po_number, agency, institution, cost, margin_pct, category)
                    VALUES (?, ?, ?, ?, 'quote_won', ?, ?, ?, ?, ?, ?, 'product_sales')
                """, (rev_id, q["updated_at"] or now, q["total"],
                      f"Quote {q['quote_number']} won",
                      q["quote_number"], q["po_number"] or "",
                      q["agency"], q["institution"],
                      q["total_cost"] or 0, q["margin_pct"] or 0))
                synced += 1

            # Source 2: JSON revenue file
            rev_path = os.path.join(DATA_DIR, "intel_revenue.json")
            try:
                with open(rev_path) as f:
                    rev_data = json.load(f)
                entries = rev_data.get("entries", [])
                for entry in entries:
                    eid = entry.get("id", f"json-{hash(json.dumps(entry, default=str)) % 100000}")
                    if eid in existing_ids:
                        continue
                    conn.execute("""
                        INSERT OR IGNORE INTO revenue_log
                        (id, logged_at, amount, description, source, quote_number,
                         po_number, agency, category)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'manual')
                    """, (eid, entry.get("date", now), entry.get("amount", 0),
                          entry.get("description", "Manual entry"),
                          entry.get("source", "manual"),
                          entry.get("quote_number", ""), entry.get("po_number", ""),
                          entry.get("agency", "")))
                    synced += 1
            except Exception:
                pass

    except Exception as e:
        log.error("reconcile_revenue: %s", e)
        return {"ok": False, "error": str(e)}

    log.info("Revenue reconciliation: synced %d new entries", synced)
    return {"ok": True, "synced": synced}


# ── Pipeline Forecast ─────────────────────────────────────────────────────────

def forecast_pipeline() -> dict:
    """Calculate weighted pipeline value from open quotes."""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT quote_number, agency, institution, total, status,
                       created_at, win_probability, margin_pct
                FROM quotes
                WHERE status IN ('pending', 'sent') AND total > 0 AND is_test = 0
            """).fetchall()

            pipeline_items = []
            total_raw = 0
            total_weighted = 0

            for r in rows:
                total_val = r["total"] or 0
                # Use stored win_probability, or estimate from status
                win_prob = r["win_probability"] or (0.15 if r["status"] == "pending" else 0.35)
                weighted = total_val * win_prob

                pipeline_items.append({
                    "quote_number": r["quote_number"],
                    "agency": r["agency"],
                    "institution": r["institution"],
                    "total": total_val,
                    "win_probability": win_prob,
                    "weighted_value": round(weighted, 2),
                    "status": r["status"],
                    "margin_pct": r["margin_pct"] or 0,
                    "created_at": r["created_at"],
                })

                total_raw += total_val
                total_weighted += weighted

            # Sort by weighted value descending
            pipeline_items.sort(key=lambda x: x["weighted_value"], reverse=True)

            return {
                "ok": True,
                "items": pipeline_items,
                "total_raw": round(total_raw, 2),
                "total_weighted": round(total_weighted, 2),
                "count": len(pipeline_items),
            }
    except Exception as e:
        log.error("forecast_pipeline: %s", e)
        return {"ok": False, "error": str(e)}


# ── Monthly Breakdown ─────────────────────────────────────────────────────────

def get_monthly_revenue(months: int = 12) -> dict:
    """Get revenue breakdown by month."""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT id, logged_at, amount, agency, source, margin_pct, cost
                FROM revenue_log
                WHERE amount > 0
                ORDER BY logged_at DESC
            """).fetchall()

            monthly = defaultdict(lambda: {"revenue": 0, "cost": 0, "count": 0, "agencies": set()})
            for r in rows:
                try:
                    dt = datetime.fromisoformat(r["logged_at"].replace("Z", "+00:00"))
                    key = dt.strftime("%Y-%m")
                    monthly[key]["revenue"] += r["amount"] or 0
                    monthly[key]["cost"] += r["cost"] or 0
                    monthly[key]["count"] += 1
                    if r["agency"]:
                        monthly[key]["agencies"].add(r["agency"])
                except Exception:
                    pass

            # Build sorted monthly series
            result = []
            now = datetime.now()
            for i in range(months - 1, -1, -1):
                dt = now - timedelta(days=30 * i)
                key = dt.strftime("%Y-%m")
                data = monthly.get(key, {"revenue": 0, "cost": 0, "count": 0, "agencies": set()})
                profit = data["revenue"] - data["cost"]
                result.append({
                    "month": key,
                    "revenue": round(data["revenue"], 2),
                    "cost": round(data["cost"], 2),
                    "profit": round(profit, 2),
                    "margin_pct": round(profit / data["revenue"] * 100, 1) if data["revenue"] else 0,
                    "deals": data["count"],
                    "agencies": len(data["agencies"]),
                })

            return {"ok": True, "months": result}
    except Exception as e:
        log.error("get_monthly_revenue: %s", e)
        return {"ok": False, "error": str(e)}


# ── Top Customers ─────────────────────────────────────────────────────────────

def get_top_customers(limit: int = 10) -> list:
    """Top customers by revenue."""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT agency, SUM(amount) as total_revenue, COUNT(*) as deal_count,
                       AVG(margin_pct) as avg_margin
                FROM revenue_log
                WHERE amount > 0 AND agency IS NOT NULL AND agency != ''
                GROUP BY agency
                ORDER BY total_revenue DESC
                LIMIT ?
            """, (limit,)).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


# ── Margin Analysis ───────────────────────────────────────────────────────────

def get_margin_analysis() -> dict:
    """Analyze margins across all quotes and orders."""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT quote_number, agency, total, total_cost, gross_profit, margin_pct,
                       status, items_costed
                FROM quotes
                WHERE total > 0 AND total_cost > 0 AND is_test = 0
                ORDER BY margin_pct ASC
            """).fetchall()

            items = [dict(r) for r in rows]
            if not items:
                return {"ok": True, "items": [], "avg_margin": 0, "low_margin_count": 0}

            avg_margin = sum(r["margin_pct"] for r in items) / len(items)
            low_margin = [r for r in items if r["margin_pct"] < 15]
            critical_margin = [r for r in items if r["margin_pct"] < 10]

            return {
                "ok": True,
                "items": items,
                "avg_margin": round(avg_margin, 1),
                "low_margin_count": len(low_margin),
                "critical_margin_count": len(critical_margin),
                "total_quotes_with_cost": len(items),
            }
    except Exception as e:
        log.error("get_margin_analysis: %s", e)
        return {"ok": False, "error": str(e)}


# ── Goal Tracking ─────────────────────────────────────────────────────────────

def get_goal_progress() -> dict:
    """Track progress toward annual revenue goal."""
    try:
        # Get YTD revenue from DB
        with get_db() as conn:
            ytd = conn.execute("""
                SELECT COALESCE(SUM(amount), 0) as total
                FROM revenue_log
                WHERE logged_at >= ? AND amount > 0
            """, (FISCAL_YEAR_START,)).fetchone()
            total_ytd = ytd["total"] if ytd else 0

        # Pipeline forecast (separate DB call — avoid lock deadlock)
        forecast = forecast_pipeline()
        weighted_pipeline = forecast.get("total_weighted", 0)

        # Calculate projected annual
        now = datetime.now()
        fy_start = datetime.fromisoformat(FISCAL_YEAR_START)
        days_elapsed = max((now - fy_start).days, 1)
        daily_rate = total_ytd / days_elapsed
        projected_annual = daily_rate * 365

        pct_of_goal = round(total_ytd / ANNUAL_GOAL * 100, 1) if ANNUAL_GOAL else 0

        if daily_rate > 0:
            days_to_goal = (ANNUAL_GOAL - total_ytd) / daily_rate
            projected_hit = (now + timedelta(days=days_to_goal)).strftime("%Y-%m-%d")
        else:
            projected_hit = "N/A"

        return {
            "ok": True,
            "goal": ANNUAL_GOAL,
            "ytd_revenue": round(total_ytd, 2),
            "pct_of_goal": pct_of_goal,
            "weighted_pipeline": round(weighted_pipeline, 2),
            "projected_with_pipeline": round(total_ytd + weighted_pipeline, 2),
            "projected_annual": round(projected_annual, 2),
            "daily_rate": round(daily_rate, 2),
            "projected_goal_date": projected_hit,
        }
    except Exception as e:
        log.error("get_goal_progress: %s", e)
        return {"ok": False, "error": str(e)}


# ── Full Dashboard Data ───────────────────────────────────────────────────────

def get_revenue_dashboard() -> dict:
    """All data needed for the revenue dashboard page."""
    try:
        reconcile_revenue()  # Ensure data is fresh
    except Exception as e:
        log.warning("reconcile in dashboard: %s", e)

    goal = get_goal_progress()
    monthly = get_monthly_revenue(12)
    pipeline = forecast_pipeline()
    margins = get_margin_analysis()
    top_customers = get_top_customers(10)

    return {
        "ok": True,
        "goal": goal,
        "monthly": monthly.get("months", []),
        "pipeline": pipeline,
        "margins": margins,
        "top_customers": top_customers,
    }


# ── Agent Status ──────────────────────────────────────────────────────────────

def get_agent_status() -> dict:
    goal = get_goal_progress()
    return {
        "name": "revenue_engine",
        "status": "ok",
        "ytd_revenue": goal.get("ytd_revenue", 0),
        "pct_of_goal": goal.get("pct_of_goal", 0),
        "weighted_pipeline": goal.get("weighted_pipeline", 0),
    }
