"""Unified metrics — single source of truth for dashboard numbers.

Every page that shows pending-count, pipeline-$, win-rate, order-count,
or inbox-count MUST call the functions in this module instead of inlining
its own SQL or JSON computation. This eliminates the cross-page divergence
flagged in the 2026-04-14 UX audit (P0.12).

Data source: SQLite only (the authoritative store per Orders V2).
No JSON fallbacks — if the DB is unavailable, return zero-safe defaults
so pages degrade gracefully without lying.

Status filters are documented per-function so the definitions are auditable.
"""

import logging
from typing import Optional

log = logging.getLogger("metrics")


def _db():
    """Get a DB connection. Resolves dynamically for test isolation."""
    from src.core.db import get_db
    return get_db()


# ═══════════════════════════════════════════════════════════════════════
# 1. Pipeline value — total $ of quotes not yet decided
# ═══════════════════════════════════════════════════════════════════════

# A quote is "in the pipeline" if it's been created but not yet won, lost,
# or expired. Draft quotes ARE in the pipeline (they represent work in
# progress). Test quotes are never counted.
PIPELINE_STATUSES = ("pending", "sent", "draft", "generated")


def get_pipeline_value() -> dict:
    """Total dollar value of quotes in the active pipeline.

    Returns: {pipeline_value, quote_count, by_status: {status: {count, value}}}
    """
    result = {"pipeline_value": 0.0, "quote_count": 0, "by_status": {}}
    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT status, COUNT(*) as c, COALESCE(SUM(total), 0) as v
                FROM quotes
                WHERE is_test = 0
                  AND status IN (?, ?, ?, ?)
                GROUP BY status
            """, PIPELINE_STATUSES).fetchall()
            for r in rows:
                result["by_status"][r["status"]] = {
                    "count": r["c"], "value": round(r["v"], 2)
                }
                result["pipeline_value"] += r["v"]
                result["quote_count"] += r["c"]
            result["pipeline_value"] = round(result["pipeline_value"], 2)
    except Exception as e:
        log.debug("get_pipeline_value: %s", e)
    return result


# ═══════════════════════════════════════════════════════════════════════
# 2. Win rate — won / (won + lost), excluding undecided
# ═══════════════════════════════════════════════════════════════════════

def get_win_rate() -> dict:
    """Quote win/loss statistics.

    Win rate = won / (won + lost) × 100. Pending, draft, sent, expired,
    and generated quotes are NOT included in the denominator — only
    quotes with a terminal decision count.

    Returns: {won, lost, pending, sent, expired, total, decided,
              won_total, lost_total, pending_total, rate}
    """
    result = {
        "won": 0, "lost": 0, "pending": 0, "sent": 0,
        "expired": 0, "draft": 0, "generated": 0,
        "total": 0, "decided": 0,
        "won_total": 0.0, "lost_total": 0.0, "pending_total": 0.0,
        "rate": 0.0,
    }
    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT status, COUNT(*) as c, COALESCE(SUM(total), 0) as v
                FROM quotes WHERE is_test = 0
                GROUP BY status
            """).fetchall()
            for r in rows:
                s = r["status"] or "pending"
                result[s] = result.get(s, 0) + r["c"]
                result["total"] += r["c"]
                if s == "won":
                    result["won_total"] = round(r["v"], 2)
                elif s == "lost":
                    result["lost_total"] = round(r["v"], 2)
                elif s in PIPELINE_STATUSES:
                    result["pending_total"] += r["v"]
            result["pending_total"] = round(result["pending_total"], 2)
            result["decided"] = result["won"] + result["lost"]
            if result["decided"] > 0:
                result["rate"] = round(
                    result["won"] / result["decided"] * 100, 1
                )
    except Exception as e:
        log.debug("get_win_rate: %s", e)
    return result


# ═══════════════════════════════════════════════════════════════════════
# 3. Active orders — real orders excluding test/cancelled
# ═══════════════════════════════════════════════════════════════════════

# Consistent exclusion list: test, cancelled, deleted. And PO numbers
# containing "TEST" (belt + suspenders with is_test column).
ORDER_EXCLUDE_STATUSES = ("cancelled", "test", "deleted")


def get_active_orders() -> dict:
    """Order counts and value.

    Returns: {total, active, closed, total_value, invoiced_value}
    """
    result = {
        "total": 0, "active": 0, "closed": 0,
        "total_value": 0.0, "invoiced_value": 0.0,
    }
    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT status, COUNT(*) as c, COALESCE(SUM(total), 0) as v
                FROM orders
                WHERE status NOT IN (?, ?, ?)
                  AND COALESCE(po_number, '') NOT LIKE '%TEST%'
                GROUP BY status
            """, ORDER_EXCLUDE_STATUSES).fetchall()
            for r in rows:
                result["total"] += r["c"]
                result["total_value"] += r["v"]
                if r["status"] in ("closed", "invoiced"):
                    result["closed"] += r["c"]
                    result["invoiced_value"] += r["v"]
                else:
                    result["active"] += r["c"]
            result["total_value"] = round(result["total_value"], 2)
            result["invoiced_value"] = round(result["invoiced_value"], 2)
    except Exception as e:
        log.debug("get_active_orders: %s", e)
    return result


# ═══════════════════════════════════════════════════════════════════════
# 4. Inbox / open PCs+RFQs — work waiting to be processed
# ═══════════════════════════════════════════════════════════════════════

# "Inbox" = items that need human attention. A PC or RFQ is in the inbox
# if it's been parsed but not yet priced, or if it's brand new / errored.
PC_INBOX_STATUSES = ("parsed", "new", "parse_error")
RFQ_INBOX_STATUSES = ("new", "pending", "parsed")

# "Priced" = ready for quote generation but not yet generated.
PC_PRICED_STATUSES = ("priced", "ready", "auto_drafted")
RFQ_PRICED_STATUSES = ("priced", "ready")

# "Sent" / completed.
PC_SENT_STATUSES = ("sent", "completed")
RFQ_SENT_STATUSES = ("sent",)


def get_inbox_counts() -> dict:
    """Funnel counts: inbox → priced → quoted → sent → won.

    Uses the price_checks and rfqs SQLite tables. Status definitions are
    explicit (no inverted filters) so adding new statuses doesn't silently
    change the numbers.

    Returns: {inbox, priced, quoted, sent, won, won_value, pipeline_value,
              orders, pc_inbox, rfq_inbox}
    """
    result = {
        "inbox": 0, "priced": 0, "quoted": 0, "sent": 0,
        "won": 0, "won_value": 0.0, "pipeline_value": 0.0,
        "orders": 0, "pc_inbox": 0, "rfq_inbox": 0,
    }
    try:
        with _db() as conn:
            # PC counts by status bucket
            def _count_pcs(statuses):
                placeholders = ",".join("?" * len(statuses))
                row = conn.execute(
                    f"SELECT COUNT(*) FROM price_checks WHERE status IN ({placeholders})",
                    statuses
                ).fetchone()
                return row[0] if row else 0

            # RFQ counts by status bucket
            def _count_rfqs(statuses):
                placeholders = ",".join("?" * len(statuses))
                row = conn.execute(
                    f"SELECT COUNT(*) FROM rfqs WHERE status IN ({placeholders})",
                    statuses
                ).fetchone()
                return row[0] if row else 0

            result["pc_inbox"] = _count_pcs(PC_INBOX_STATUSES)
            result["rfq_inbox"] = _count_rfqs(RFQ_INBOX_STATUSES)
            result["inbox"] = result["pc_inbox"] + result["rfq_inbox"]

            result["priced"] = (
                _count_pcs(PC_PRICED_STATUSES)
                + _count_rfqs(RFQ_PRICED_STATUSES)
            )

            result["quoted"] = (
                _count_pcs(("quoted", "generated"))
                + _count_rfqs(("generated", "quoted"))
            )

            result["sent"] = (
                _count_pcs(PC_SENT_STATUSES)
                + _count_rfqs(RFQ_SENT_STATUSES)
            )

            # Won + pipeline from unified quote stats
            wr = get_win_rate()
            result["won"] = wr["won"]
            result["won_value"] = wr["won_total"]

            pv = get_pipeline_value()
            result["pipeline_value"] = pv["pipeline_value"]

            ao = get_active_orders()
            result["orders"] = ao["total"]

    except Exception as e:
        log.debug("get_inbox_counts: %s", e)
    return result


# ═══════════════════════════════════════════════════════════════════════
# 5. Pending email drafts — outbox items awaiting review
# ═══════════════════════════════════════════════════════════════════════

def get_pending_drafts() -> dict:
    """Email draft counts from the outbox.

    Returns: {sales_drafts, cs_drafts, total}
    """
    result = {"sales_drafts": 0, "cs_drafts": 0, "total": 0}
    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT status, COUNT(*) as c FROM email_outbox
                WHERE status IN ('draft', 'cs_draft')
                GROUP BY status
            """).fetchall()
            for r in rows:
                if r["status"] == "draft":
                    result["sales_drafts"] = r["c"]
                elif r["status"] == "cs_draft":
                    result["cs_drafts"] = r["c"]
            result["total"] = result["sales_drafts"] + result["cs_drafts"]
    except Exception as e:
        log.debug("get_pending_drafts: %s", e)
    return result


# ═══════════════════════════════════════════════════════════════════════
# Convenience: all metrics in one call (for /api/dashboard/init)
# ═══════════════════════════════════════════════════════════════════════

def get_all_metrics() -> dict:
    """All dashboard metrics in a single dict. Each sub-key uses the
    canonical helper above so numbers are guaranteed consistent."""
    return {
        "pipeline": get_pipeline_value(),
        "win_rate": get_win_rate(),
        "orders": get_active_orders(),
        "inbox": get_inbox_counts(),
        "drafts": get_pending_drafts(),
    }
