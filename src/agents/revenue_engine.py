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
    """Sync revenue from multiple sources into revenue_log.
    
    Revenue is based on ORDERS (confirmed purchases), not quote status.
    A quote is temporary — revenue is real when a PO/order exists.
    """
    now = datetime.now(timezone.utc).isoformat()
    synced = 0

    try:
        with get_db() as conn:
            existing_ids = set(r[0] for r in conn.execute(
                "SELECT id FROM revenue_log"
            ).fetchall())

            # ── Source 1: Orders (primary revenue source) ─────────────
            # An order = confirmed purchase (PO received). This IS revenue.
            # BUILD-10: exclude test orders from revenue sync, and inherit
            # is_test into the derived revenue_log rows so analytics stays
            # clean end-to-end.
            orders = conn.execute("""
                SELECT id, quote_number, agency, institution, po_number,
                       total, status, created_at, is_test
                FROM orders
                WHERE total > 0 AND is_test = 0
            """).fetchall()

            for o in orders:
                rev_id = f"rev-ord-{o['id']}"
                if rev_id in existing_ids:
                    continue
                desc = f"Order {o['id']}"
                if o["po_number"]:
                    desc = f"PO {o['po_number']}"
                if o["agency"]:
                    desc += f" — {o['agency']}"

                # Look up cost/margin from linked quote if available
                cost = 0
                margin = 0
                if o["quote_number"]:
                    qrow = conn.execute(
                        "SELECT total_cost, margin_pct FROM quotes WHERE quote_number = ?",
                        (o["quote_number"],)
                    ).fetchone()
                    if qrow:
                        cost = qrow["total_cost"] or 0
                        margin = qrow["margin_pct"] or 0

                conn.execute("""
                    INSERT INTO revenue_log
                    (id, logged_at, amount, description, source, quote_number,
                     po_number, agency, institution, cost, margin_pct, category, is_test)
                    VALUES (?, ?, ?, ?, 'order', ?, ?, ?, ?, ?, ?, 'product_sales', ?)
                """, (rev_id, o["created_at"] or now, o["total"], desc,
                      o["quote_number"] or "", o["po_number"] or "",
                      o["agency"] or "", o["institution"] or "",
                      cost, margin,
                      1 if (o["is_test"] if "is_test" in o.keys() else 0) else 0))
                synced += 1
                existing_ids.add(rev_id)

                # Auto-mark linked quote as 'won' if it has an order
                if o["quote_number"]:
                    qstatus = conn.execute(
                        "SELECT status FROM quotes WHERE quote_number = ?",
                        (o["quote_number"],)
                    ).fetchone()
                    if qstatus and qstatus["status"] not in ("won", "lost", "cancelled"):
                        conn.execute("""
                            UPDATE quotes SET status = 'won',
                                po_number = COALESCE(NULLIF(?, ''), po_number),
                                updated_at = ?
                            WHERE quote_number = ?
                        """, (o["po_number"] or "", now, o["quote_number"]))
                        log.info("Auto-marked quote %s as 'won' (has order %s)",
                                 o["quote_number"], o["id"])

            # ── Source 2: Won quotes WITHOUT orders (manual wins) ─────
            won = conn.execute("""
                SELECT quote_number, total, agency, institution, po_number,
                       updated_at, total_cost, gross_profit, margin_pct
                FROM quotes
                WHERE status = 'won' AND total > 0 AND is_test = 0
            """).fetchall()

            for q in won:
                rev_id = f"rev-{q['quote_number']}"
                if rev_id in existing_ids:
                    continue
                # Skip if we already have an order-based entry for this quote
                ord_id = f"rev-ord-ORD-{q['quote_number']}"
                if ord_id in existing_ids:
                    continue
                # Also check if ANY order-based entry references this quote
                has_order_rev = conn.execute(
                    "SELECT 1 FROM revenue_log WHERE source='order' AND quote_number=? LIMIT 1",
                    (q["quote_number"],)
                ).fetchone()
                if has_order_rev:
                    continue

                # BUILD-10: source query already filters is_test=0, so
                # every row written here is real (is_test=0). Write
                # explicitly to lock in intent for future column reorders.
                conn.execute("""
                    INSERT INTO revenue_log
                    (id, logged_at, amount, description, source, quote_number,
                     po_number, agency, institution, cost, margin_pct, category, is_test)
                    VALUES (?, ?, ?, ?, 'quote_won', ?, ?, ?, ?, ?, ?, 'product_sales', 0)
                """, (rev_id, q["updated_at"] or now, q["total"],
                      f"Quote {q['quote_number']} won",
                      q["quote_number"], q["po_number"] or "",
                      q["agency"], q["institution"],
                      q["total_cost"] or 0, q["margin_pct"] or 0))
                synced += 1
                existing_ids.add(rev_id)

            # ── Source 3: JSON revenue file (manual entries) ──────────
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
            except Exception as _e:
                log.debug("suppressed: %s", _e)

            # ── Cleanup: remove old duplicate/orphan revenue entries ──
            # 1. Remove quote_won entries that have a corresponding order-based entry
            try:
                conn.execute("""
                    DELETE FROM revenue_log WHERE source = 'quote_won'
                    AND quote_number != ''
                    AND quote_number IN (
                        SELECT quote_number FROM revenue_log 
                        WHERE source = 'order' AND quote_number != ''
                    )
                """)
            except Exception as _e:
                log.debug("suppressed: %s", _e)
            # 2. Remove quote_won entries where quote is no longer won and has no order
            try:
                orphans = conn.execute("""
                    SELECT r.id, r.quote_number FROM revenue_log r
                    WHERE r.source = 'quote_won' AND r.quote_number != ''
                    AND NOT EXISTS (
                        SELECT 1 FROM quotes q 
                        WHERE q.quote_number = r.quote_number AND q.status = 'won'
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM orders o
                        WHERE o.quote_number = r.quote_number
                    )
                """).fetchall()
                for orph in orphans:
                    conn.execute("DELETE FROM revenue_log WHERE id = ?", (orph["id"],))
                    log.info("Cleaned orphan revenue entry %s (quote %s no longer won)",
                             orph["id"], orph["quote_number"])
            except Exception as _e:
                log.debug("suppressed: %s", _e)

    except Exception as e:
        log.error("reconcile_revenue: %s", e)
        return {"ok": False, "error": str(e)}

    log.info("Revenue reconciliation: synced %d new entries", synced)

    # Backfill margins on quotes that lack cost data
    try:
        costed = _backfill_margins()
        if costed > 0:
            log.info("Margin backfill: updated %d quotes with cost data", costed)
    except Exception as _me:
        log.debug("Margin backfill: %s", _me)

    return {"ok": True, "synced": synced}


def _backfill_margins() -> int:
    """Backfill total_cost / gross_profit / margin_pct on quotes using product catalog costs."""
    updated = 0
    try:
        with get_db() as conn:
            # Build cost lookup: part_number/name → cost
            catalog_costs = {}
            for row in conn.execute(
                "SELECT name, cost FROM product_catalog WHERE cost > 0"
            ).fetchall():
                catalog_costs[row["name"].strip().upper()] = row["cost"]

            # Quotes missing cost data
            quotes = conn.execute("""
                SELECT quote_number, line_items, total
                FROM quotes
                WHERE is_test = 0 AND line_items IS NOT NULL AND line_items != ''
                  AND (total_cost IS NULL OR total_cost = 0)
                  AND total > 0
            """).fetchall()

            for q in quotes:
                try:
                    items = json.loads(q["line_items"]) if isinstance(q["line_items"], str) else q["line_items"]
                except Exception:
                    continue
                if not isinstance(items, list):
                    continue

                total_cost = 0
                items_costed = 0
                for item in items:
                    pn = (item.get("part_number") or "").strip().upper()
                    qty = item.get("qty", 1) or 1

                    # Try matching by part number
                    cost_per = catalog_costs.get(pn, 0)

                    if not cost_per:
                        # Try fuzzy: check if part number appears in any catalog name
                        if pn and len(pn) >= 4:
                            for cat_name, cat_cost in catalog_costs.items():
                                if pn in cat_name or cat_name in pn:
                                    cost_per = cat_cost
                                    break

                    if cost_per > 0:
                        total_cost += cost_per * qty
                        items_costed += 1

                if items_costed > 0 and total_cost > 0:
                    total = q["total"] or 0
                    gross_profit = total - total_cost
                    margin_pct = (gross_profit / total * 100) if total > 0 else 0

                    conn.execute("""
                        UPDATE quotes
                        SET total_cost = ?, gross_profit = ?, margin_pct = ?, items_costed = ?
                        WHERE quote_number = ?
                    """, (round(total_cost, 2), round(gross_profit, 2),
                          round(margin_pct, 1), items_costed, q["quote_number"]))
                    updated += 1

    except Exception as e:
        log.warning("_backfill_margins: %s", e)

    return updated


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
                except Exception as _e:
                    log.debug("suppressed: %s", _e)

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
