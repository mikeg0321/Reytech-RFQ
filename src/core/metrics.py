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

def get_active_orders() -> dict:
    """Order counts and value, by canonical sourceable definition.

    Headline `total` / `total_value` are the canonical sourceable PO
    count (`is_sourceable_po` from canonical_state — excludes
    invoiced / paid / closed / cancelled, sentinel po_numbers, test
    rows, already-quoted orders). PR-4 (#694) introduced the cutover
    with Scientist-style dual-emit; PR-6 (#696) removed the legacy
    transition fields after the canonical numbers settled.

    `closed` and `invoiced_value` still count separately so the
    orders dashboard can render its "completed" badge alongside
    the active backlog.

    Returns: {total, active, closed, total_value, invoiced_value}
    """
    result = {
        "total": 0, "active": 0, "closed": 0,
        "total_value": 0.0, "invoiced_value": 0.0,
    }
    try:
        from src.core.canonical_state import (
            INVOICED_OR_PAID_STATUSES,
            is_sourceable_po,
        )
        with _db() as conn:
            # Pull every order, apply canonical predicate. Python-side
            # rather than via v_sourceable_pos so the number stays
            # correct even when migration 36 hasn't run on a brand-new
            # test DB. The view is an optimization, not source of
            # truth — that lives in canonical_state.
            rows = conn.execute("""
                SELECT id, status, total, po_number, quote_number, is_test
                FROM orders
            """).fetchall()
            sourceable_total = 0
            sourceable_value = 0.0
            invoiced_count = 0
            invoiced_value = 0.0
            for r in rows:
                rec = {
                    "status": r["status"],
                    "po_number": r["po_number"],
                    "quote_number": r["quote_number"],
                    "is_test": r["is_test"],
                }
                amount = float(r["total"] or 0)
                if is_sourceable_po(rec):
                    sourceable_total += 1
                    sourceable_value += amount
                else:
                    norm = (rec.get("status") or "").strip().lower()
                    if norm in INVOICED_OR_PAID_STATUSES:
                        invoiced_count += 1
                        invoiced_value += amount
            result["total"] = sourceable_total
            result["active"] = sourceable_total
            result["total_value"] = round(sourceable_value, 2)
            result["closed"] = invoiced_count
            result["invoiced_value"] = round(invoiced_value, 2)
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
# 6. Time-bucketed revenue / volume + top institutions
#    (canonical replacements for the per-page quotes_log.json / rfqs-JSON
#     derivations — ISSUE-4, 2026-05-29 sweep). All read the `quotes`
#     table so /api/manager/metrics and /analytics report the SAME numbers
#     as Home/pipeline instead of three independent substrates.
# ═══════════════════════════════════════════════════════════════════════

def _won_date(row) -> str:
    """When a quote became won. `quotes` has no status_updated column;
    updated_at is set on the won transition, created_at is the floor."""
    return (row["updated_at"] or row["created_at"] or "")


def get_month_revenue(year_month: Optional[str] = None) -> dict:
    """Won revenue + count for a single calendar month (default: current).

    `year_month` is "YYYY-MM"; when None the caller passes the current
    month (computed PST upstream). Returns {revenue, won_count}.
    """
    result = {"revenue": 0.0, "won_count": 0}
    if not year_month:
        return result
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT total, updated_at, created_at FROM quotes "
                "WHERE is_test = 0 AND status = 'won'"
            ).fetchall()
            for r in rows:
                if _won_date(r)[:7] == year_month:
                    result["revenue"] += float(r["total"] or 0)
                    result["won_count"] += 1
            result["revenue"] = round(result["revenue"], 2)
    except Exception as e:
        log.debug("get_month_revenue: %s", e)
    return result


def get_revenue_by_month() -> dict:
    """Won revenue + count keyed by YYYY-MM (all time).

    Returns {month: {revenue, won_count}} — drives the analytics
    monthly-revenue chart from the canonical quotes table instead of
    re-summing rfqs JSON line-items.
    """
    out: dict = {}
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT total, updated_at, created_at FROM quotes "
                "WHERE is_test = 0 AND status = 'won'"
            ).fetchall()
            for r in rows:
                month = _won_date(r)[:7]
                if not month:
                    continue
                b = out.setdefault(month, {"revenue": 0.0, "won_count": 0})
                b["revenue"] += float(r["total"] or 0)
                b["won_count"] += 1
            for b in out.values():
                b["revenue"] = round(b["revenue"], 2)
    except Exception as e:
        log.debug("get_revenue_by_month: %s", e)
    return out


def get_weekly_volume(weeks: int = 4, now=None) -> list:
    """Quote count + value per week for the last `weeks` weeks.

    `now` is injected (PST) by the caller so the bucketing matches the
    rest of the dashboard. Returns oldest→newest list of
    {label, quotes, value}. Reads the quotes table (was quotes_log.json).
    """
    from datetime import datetime, timedelta
    if now is None:
        now = datetime.now()
    buckets = []
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT total, created_at FROM quotes WHERE is_test = 0"
            ).fetchall()
            parsed = []
            for r in rows:
                ts = r["created_at"] or ""
                if not ts:
                    continue
                try:
                    dt = datetime.fromisoformat(
                        ts.replace("Z", "+00:00")).replace(tzinfo=None)
                except (ValueError, TypeError):
                    continue
                parsed.append((dt, float(r["total"] or 0)))
            for w in range(weeks):
                week_end = now - timedelta(weeks=w)
                week_start = week_end - timedelta(weeks=1)
                count = sum(1 for dt, _v in parsed if week_start <= dt < week_end)
                value = sum(v for dt, v in parsed if week_start <= dt < week_end)
                label = f"Week {weeks - w}" if w > 0 else "This Week"
                buckets.append({"label": label, "quotes": count,
                                "value": round(value, 2)})
            buckets.reverse()
    except Exception as e:
        log.debug("get_weekly_volume: %s", e)
    return buckets


def get_top_institutions(limit: int = 5) -> list:
    """Top institutions by won revenue, aggregated on the CANONICAL
    facility name so spelling variants of the same place collapse into
    one row. Returns [{name, revenue}]. (Reads the quotes table — was
    a raw-string defaultdict over quotes_log.json.)"""
    from collections import defaultdict
    try:
        from src.core.quote_contract import canonical_name
    except Exception:
        def canonical_name(x):
            return x or "Unknown"
    inst_rev: dict = defaultdict(float)
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT institution, total FROM quotes "
                "WHERE is_test = 0 AND status = 'won'"
            ).fetchall()
            for r in rows:
                inst_rev[canonical_name(r["institution"] or "Unknown")] += \
                    float(r["total"] or 0)
    except Exception as e:
        log.debug("get_top_institutions: %s", e)
    top = sorted(inst_rev.items(), key=lambda x: x[1], reverse=True)[:limit]
    return [{"name": n, "revenue": round(v, 2)} for n, v in top]


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
