"""
api_quota.py — API usage tracking and daily/monthly quota enforcement.

Tracks every paid API call (Grok, Claude) with cost estimates.
Provides soft/hard quota enforcement to prevent runaway spend.

Usage:
    from src.core.api_quota import api_quota

    # Before making an API call:
    if not api_quota.can_call("grok"):
        return {"ok": False, "error": "Daily quota exceeded"}

    # After making an API call:
    api_quota.log_call("grok", agent="product_research", pc_id="pc_123",
                       tokens_in=500, tokens_out=200, response_time_ms=1200)
"""

import logging
import time
from datetime import datetime

log = logging.getLogger("reytech.api_quota")

# Default cost estimates per call (conservative, based on typical token usage)
DEFAULT_COSTS = {
    "grok": 0.001,      # ~$0.001 per Grok call (grok-3-mini, ~500 tokens)
    "claude": 0.001,    # ~$0.001 per Claude Haiku call (~500 tokens)
}


class APIQuota:
    """Track API usage and enforce daily/monthly quotas."""

    @staticmethod
    def log_call(service, agent="", pc_id="", tokens_in=0, tokens_out=0,
                 error="", response_time_ms=0, model=""):
        """Log an API call to the database."""
        try:
            from src.core.db import get_db
            cost = APIQuota._estimate_cost(service, tokens_in, tokens_out)
            with get_db() as conn:
                conn.execute("""
                    INSERT INTO api_usage
                    (service, agent, pc_id, call_date, called_at,
                     request_tokens, response_tokens, estimated_cost,
                     response_time_ms, error, model)
                    VALUES (?, ?, ?, date('now'), datetime('now'),
                            ?, ?, ?, ?, ?, ?)
                """, (service, agent, pc_id, tokens_in, tokens_out,
                      cost, response_time_ms, error or "", model))
        except Exception as e:
            log.debug("api_quota.log_call failed (non-fatal): %s", e)

    @staticmethod
    def _estimate_cost(service, tokens_in=0, tokens_out=0):
        """Estimate cost of an API call based on tokens."""
        if tokens_in or tokens_out:
            # Token-based cost (per million tokens)
            rates = {
                "grok": {"in": 0.10, "out": 0.30},      # grok-3-mini
                "claude": {"in": 0.80, "out": 2.40},     # claude-haiku
            }
            r = rates.get(service, {"in": 0.10, "out": 0.30})
            return (tokens_in * r["in"] / 1_000_000) + (tokens_out * r["out"] / 1_000_000)
        # Flat estimate if no token data
        return DEFAULT_COSTS.get(service, 0.001)

    @staticmethod
    def check_quota(service):
        """Check remaining budget for a service. Returns status dict."""
        try:
            from src.core.db import get_db
            with get_db() as conn:
                # Get today's spend
                row = conn.execute("""
                    SELECT COALESCE(SUM(estimated_cost), 0) as today_cost,
                           COUNT(*) as today_calls
                    FROM api_usage
                    WHERE service = ? AND call_date = date('now')
                """, (service,)).fetchone()
                today_cost = row["today_cost"] if row else 0
                today_calls = row["today_calls"] if row else 0

                # Get monthly spend
                month_row = conn.execute("""
                    SELECT COALESCE(SUM(estimated_cost), 0) as month_cost,
                           COUNT(*) as month_calls
                    FROM api_usage
                    WHERE service = ? AND call_date >= date('now', 'start of month')
                """, (service,)).fetchone()
                month_cost = month_row["month_cost"] if month_row else 0
                month_calls = month_row["month_calls"] if month_row else 0

                # Get quota limits
                quota = conn.execute("""
                    SELECT daily_limit_dollars, monthly_limit_dollars, per_pc_limit, enabled
                    FROM api_quotas WHERE service = ?
                """, (service,)).fetchone()

                if not quota:
                    return {
                        "service": service, "ok": True, "status": "no_quota",
                        "today_cost": today_cost, "today_calls": today_calls,
                        "month_cost": month_cost, "month_calls": month_calls,
                    }

                daily_limit = quota["daily_limit_dollars"]
                monthly_limit = quota["monthly_limit_dollars"]
                enabled = bool(quota["enabled"])

                daily_remaining = daily_limit - today_cost
                monthly_remaining = monthly_limit - month_cost
                daily_pct = (today_cost / daily_limit * 100) if daily_limit > 0 else 0

                if not enabled:
                    status = "disabled"
                elif daily_remaining <= 0:
                    status = "daily_exceeded"
                elif monthly_remaining <= 0:
                    status = "monthly_exceeded"
                elif daily_pct >= 80:
                    status = "warning"
                else:
                    status = "ok"

                return {
                    "service": service,
                    "ok": status in ("ok", "warning", "disabled", "no_quota"),
                    "status": status,
                    "enabled": enabled,
                    "today_cost": round(today_cost, 4),
                    "today_calls": today_calls,
                    "daily_limit": daily_limit,
                    "daily_remaining": round(max(0, daily_remaining), 4),
                    "daily_pct": round(daily_pct, 1),
                    "month_cost": round(month_cost, 4),
                    "month_calls": month_calls,
                    "monthly_limit": monthly_limit,
                    "monthly_remaining": round(max(0, monthly_remaining), 4),
                }
        except Exception as e:
            log.debug("check_quota failed (non-fatal): %s", e)
            return {"service": service, "ok": True, "status": "error", "error": str(e)}

    @staticmethod
    def can_call(service, pc_id=""):
        """Return True if quota allows another call. Non-blocking on errors."""
        try:
            status = APIQuota.check_quota(service)
            if status.get("status") in ("daily_exceeded", "monthly_exceeded"):
                log.warning("API quota exceeded for %s: %s", service, status.get("status"))
                return False
            return True
        except Exception:
            return True  # Fail open — don't block business on tracking errors

    @staticmethod
    def get_daily_summary():
        """Return today's costs per service."""
        try:
            from src.core.db import get_db
            with get_db() as conn:
                rows = conn.execute("""
                    SELECT service,
                           COUNT(*) as calls,
                           COALESCE(SUM(estimated_cost), 0) as total_cost,
                           COALESCE(AVG(response_time_ms), 0) as avg_response_ms,
                           SUM(CASE WHEN error != '' THEN 1 ELSE 0 END) as errors
                    FROM api_usage
                    WHERE call_date = date('now')
                    GROUP BY service
                    ORDER BY total_cost DESC
                """).fetchall()

                services = {}
                total_cost = 0
                for r in rows:
                    s = dict(r)
                    s["total_cost"] = round(s["total_cost"], 4)
                    s["avg_response_ms"] = round(s["avg_response_ms"], 0)
                    total_cost += s["total_cost"]
                    quota = APIQuota.check_quota(s["service"])
                    s["daily_limit"] = quota.get("daily_limit", 0)
                    s["daily_pct"] = quota.get("daily_pct", 0)
                    services[s["service"]] = s

                return {
                    "ok": True,
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "total_cost": round(total_cost, 4),
                    "services": services,
                }
        except Exception as e:
            log.debug("get_daily_summary failed: %s", e)
            return {"ok": False, "error": str(e), "services": {}}

    @staticmethod
    def get_monthly_summary():
        """Return this month's costs per service."""
        try:
            from src.core.db import get_db
            with get_db() as conn:
                rows = conn.execute("""
                    SELECT service,
                           COUNT(*) as calls,
                           COALESCE(SUM(estimated_cost), 0) as total_cost
                    FROM api_usage
                    WHERE call_date >= date('now', 'start of month')
                    GROUP BY service
                """).fetchall()
                return {
                    "ok": True,
                    "month": datetime.now().strftime("%Y-%m"),
                    "services": {r["service"]: {
                        "calls": r["calls"],
                        "total_cost": round(r["total_cost"], 4),
                    } for r in rows},
                }
        except Exception as e:
            return {"ok": False, "error": str(e)}


# Module-level singleton
api_quota = APIQuota()
