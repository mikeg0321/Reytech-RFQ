"""Canonical predicates over Reytech business state.

This module is the **single source of truth** for the questions every
panel, agent, and route asks about a record:

    - Is this RFQ in the active queue?
    - Has this quote actually been delivered to the buyer?
    - Is this PO still pending sourcing?
    - Did this revenue land in the current calendar year?

Before this module existed, every consumer (home page, revenue card,
Awaiting Responses panel, agent loops, manager brief, etc.) wrote its
own inline `WHERE status IN ...` filter. Numbers drifted. Mike saw
"Queue (5)" hide 68 stale rows, all 10 sent-table rows stamped today,
"99 new POs" while only 1 was real. The fix isn't to patch each panel
— it's to make every panel ask the same question.

## Glossary (locked-in definitions, 2026-05-02)

These came from Mike directly. Treat them as binding contracts; don't
bend them per-caller. If a consumer needs a stricter or looser variant,
add a new predicate here, don't shadow the existing one.

    Revenue year:
        Calendar Jan 1 – Dec 31. The previous fiscal year split
        (`FISCAL_YEAR_START='2025-07-01'`) is being retired.

    Active queue:
        An RFQ or Price Check whose status is *not* in the closed set
        {sent, won, lost, no_bid, cancelled}. These are records the
        operator still owes work on.

    Sent (integrity):
        Got to the buyer properly and on time, whether through the app
        or marked manually. Requires status='sent' AND a real sent_at
        timestamp — not empty, not the same as created_at (which would
        mean the column is reading creation time, not send time — the
        bug Mike caught in the Sent / Completed table).

    Sourceable PO:
        An active purchase order that is not yet invoiced AND not yet
        paid. (Future: also "still has items pending delivery" once
        the line-tracking layer ships.)

## How to use

    from src.core.canonical_state import (
        is_active_queue, is_real_sent, is_sourceable_po, is_year_revenue,
        revenue_year_start, revenue_year_end, REVENUE_YEAR,
        ACTIVE_QUEUE_EXCLUDED_STATUSES,
    )

    active = [r for r in rfqs.values() if is_active_queue(r)]
    sent_real = [q for q in quotes if is_real_sent(q)]
    pos_to_source = [o for o in orders if is_sourceable_po(o)]
    ytd_orders = [o for o in orders if is_year_revenue(o, REVENUE_YEAR)]

For SQL surfaces, prefer the matching VIEWs created by migration 36:
`v_active_queue_rfqs`, `v_real_sent`, `v_sourceable_pos`,
`v_revenue_year_2026`. They share the same definitions.

## Why predicates over inline SQL

A `WHERE status IN ('new','priced')` scattered across 12 files is 12
chances to drift. A `is_active_queue(r)` import + lint guard against
inline status filters keeps the definition in one place. When the
business changes ("we now treat 'awaiting_buyer_response' as active
even though it's post-send"), one edit here cascades everywhere.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional

# ─────────────────────────────────────────────────────────────────────────
# Constants — locked-in glossary
# ─────────────────────────────────────────────────────────────────────────

#: The calendar year that "YTD revenue" and the $2M goal apply to.
#: Per Mike (2026-05-02): explicit calendar year, not fiscal. Bumped
#: annually on Jan 1 of the new year.
REVENUE_YEAR: int = 2026

#: Statuses that take an RFQ/PC out of the active operator queue. Once
#: a record reaches any of these, the operator is no longer expected
#: to act on it as part of "today's work". Sent → buyer's turn; the
#: rest are terminal outcomes.
ACTIVE_QUEUE_EXCLUDED_STATUSES: frozenset[str] = frozenset({
    "sent", "won", "lost", "no_bid", "cancelled",
})

#: Subset of ACTIVE_QUEUE_EXCLUDED_STATUSES that are *terminal*
#: (no further state transitions expected). 'sent' is excluded because
#: a sent quote still moves to won/lost as the buyer decides.
TERMINAL_STATUSES: frozenset[str] = frozenset({
    "won", "lost", "no_bid", "cancelled",
})

#: Order statuses that mean money has effectively cleared (or won't):
#: a sourceable PO is one whose status is *not* in this set.
INVOICED_OR_PAID_STATUSES: frozenset[str] = frozenset({
    "invoiced", "paid", "closed", "cancelled",
})

#: PO numbers that should never count as real (operator typos, stub
#: rows, sentinel placeholders that escaped the cleaner). The full
#: cleaner lives in `core.order_dal.clean_po_number`; this mirror
#: exists so canonical predicates don't have to import order_dal
#: (which imports a lot).
_SENTINEL_PO_TOKENS: frozenset[str] = frozenset({
    "", "n/a", "na", "tbd", "pending", "?", "x", "xx", "xxx",
    "none", "null", "test",
})


# ─────────────────────────────────────────────────────────────────────────
# Year boundaries
# ─────────────────────────────────────────────────────────────────────────

def revenue_year_start(year: int = REVENUE_YEAR) -> str:
    """ISO date for the first day of the revenue year (inclusive).

    Returns 'YYYY-01-01' so it sorts lexically against ISO timestamps
    in `created_at`/`sent_at`/`logged_at` columns.
    """
    return f"{year:04d}-01-01"


def revenue_year_end(year: int = REVENUE_YEAR) -> str:
    """ISO date for the day *after* the revenue year ends (exclusive).

    Use as `< revenue_year_end()` so the comparison is half-open
    `[start, end)` and there's no off-by-one on Dec 31 23:59:59.
    """
    return f"{year + 1:04d}-01-01"


def _parse_iso(value: Any) -> Optional[datetime]:
    """Best-effort ISO-8601 parse. Returns None on anything unparseable.

    Handles the assortment of timestamp shapes that show up in this
    codebase: bare dates, with seconds, with microseconds, with or
    without timezone, with a trailing 'Z'. Returning None on failure
    lets callers treat malformed strings as "no real timestamp"
    rather than raising — the disease being treated here is *missing*
    or *fake* timestamps, not parse errors.
    """
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    # Python 3.11+ fromisoformat handles "Z" suffix; older versions don't.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        # Fallback: date-only.
        try:
            return datetime.fromisoformat(s[:10])
        except ValueError:
            return None


def _normalize_status(record: Mapping[str, Any]) -> str:
    """Lower-case status string, defaulting to '' for missing/None."""
    return (record.get("status") or "").strip().lower()


# ─────────────────────────────────────────────────────────────────────────
# Active queue
# ─────────────────────────────────────────────────────────────────────────

def is_active_queue(record: Mapping[str, Any]) -> bool:
    """Should this RFQ/PC appear in the operator's active work queue?

    True when the record's status is *not* in the excluded set
    (sent / won / lost / no_bid / cancelled). Test/cancelled rows
    are also excluded via `is_test`.

    Works for both RFQ and Price Check records — they share the
    `status` and `is_test` shape. PCs that became RFQs (via convert)
    are still queryable as PCs but the queue panel renders the RFQ
    side; that de-dup is the consumer's job, not this predicate.
    """
    if record.get("is_test"):
        return False
    return _normalize_status(record) not in ACTIVE_QUEUE_EXCLUDED_STATUSES


# ─────────────────────────────────────────────────────────────────────────
# Sent integrity
# ─────────────────────────────────────────────────────────────────────────

def is_real_sent(record: Mapping[str, Any]) -> bool:
    """Did this quote actually get to the buyer?

    Three conditions must all hold:

      1. status == 'sent'.
      2. sent_at is non-empty.
      3. sent_at is not equal to created_at — the bug Mike caught:
         when the Sent table column rendered `created_at` instead of
         the real send timestamp, every row stamped "today" because
         creation and rendering happened on the same day. If sent_at
         literally equals created_at (string match), we treat it as
         a misconfigured writer, not a real send moment, and return
         False so the panel surfaces "missing" instead of lying.

    Test/cancelled rows are filtered out via `is_test`.
    """
    if record.get("is_test"):
        return False
    if _normalize_status(record) != "sent":
        return False
    sent_at = (record.get("sent_at") or "").strip()
    if not sent_at:
        return False
    created_at = (record.get("created_at") or "").strip()
    # If both timestamps exist and match exactly, the writer is
    # almost certainly stamping created_at into sent_at. Suspect.
    if created_at and sent_at == created_at:
        return False
    # Sanity check: must parse as a real timestamp.
    return _parse_iso(sent_at) is not None


# ─────────────────────────────────────────────────────────────────────────
# Sourceable PO
# ─────────────────────────────────────────────────────────────────────────

def _is_sentinel_po_number(po_number: Any) -> bool:
    """True if this PO number is a placeholder, not a real order ref."""
    if not po_number:
        return True
    s = str(po_number).strip().lower()
    if s in _SENTINEL_PO_TOKENS:
        return True
    # "TEST..." and "??" variants
    if s.startswith("test"):
        return True
    if set(s) <= {"?", "x", "-", " "}:
        return True
    return False


def is_sourceable_po(record: Mapping[str, Any]) -> bool:
    """Should this order count as "PO awaiting sourcing"?

    Per Mike (2026-05-02): "an active PO that has not been invoiced
    yet and paid". Future-state: also "items still pending delivery"
    once line-status tracking lands.

    Excludes:
      - Test rows (is_test or sentinel po_number with TEST prefix).
      - Invoiced / paid / closed / cancelled (money has cleared or
        won't, so not actionable for sourcing).
      - Sentinel PO numbers (N/A, TBD, ?, etc.) — these are operator
        typos or stub rows, not real orders.
      - Already-quoted orders (`quote_number` populated): the source
        is identified; sourcing the line items is a separate workflow,
        not "find me the right vendor for this PO".

    Open question once delivery-tracking lands: should `delivered`
    drop out of the sourceable set? For now, kept in — items can be
    delivered without invoice/payment (e.g. terms 30 days), and Mike
    explicitly named only invoice + payment as the gates.
    """
    if record.get("is_test"):
        return False
    if _is_sentinel_po_number(record.get("po_number")):
        return False
    status = _normalize_status(record)
    if status in INVOICED_OR_PAID_STATUSES:
        return False
    quote_number = (record.get("quote_number") or "").strip()
    if quote_number:
        # Already linked to a quote → already sourced (by us). The
        # vendor-search workflow doesn't need to revisit this PO.
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────
# Revenue year membership
# ─────────────────────────────────────────────────────────────────────────

def is_year_revenue(
    record: Mapping[str, Any],
    year: int = REVENUE_YEAR,
    *,
    timestamp_field: str = "created_at",
) -> bool:
    """Did this record land in the given revenue year?

    Half-open interval `[YYYY-01-01, YYYY+1-01-01)` against the
    record's timestamp field. Default field is `created_at` (orders,
    quotes, RFQs); revenue_log rows use `logged_at`.

    Excludes test rows.

    Returns False on missing or unparseable timestamps. The previous
    behavior — treating malformed timestamps as "current year" via
    `now()` fallback — is exactly the disease this module exists to
    cure: it inflates YTD by counting bad data as good data.
    """
    if record.get("is_test"):
        return False
    ts = (record.get(timestamp_field) or "").strip()
    if not ts:
        return False
    parsed = _parse_iso(ts)
    if parsed is None:
        return False
    # Compare in UTC to avoid timezone-shift edge cases at year boundaries.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.year == year


# ─────────────────────────────────────────────────────────────────────────
# Convenience: SQL fragments for callers that must build queries
# ─────────────────────────────────────────────────────────────────────────
#
# Direct SQL is allowed inside core/ for performance-sensitive paths.
# Outside core/, prefer the VIEWs from migration 36. These constants
# exist so the SQL stays in lockstep with the Python predicates above.

#: SQL fragment placed after WHERE for active-queue records (RFQs/PCs).
#: Uses parameter substitution for the excluded-status list.
SQL_ACTIVE_QUEUE_FRAGMENT = (
    "is_test = 0 AND LOWER(COALESCE(status, '')) NOT IN "
    "({placeholders})"
)

#: SQL fragment for "real sent" quotes — status=sent AND has real sent_at.
SQL_REAL_SENT_FRAGMENT = (
    "is_test = 0 "
    "AND LOWER(COALESCE(status, '')) = 'sent' "
    "AND sent_at IS NOT NULL AND sent_at != '' "
    "AND sent_at != created_at"
)

#: SQL fragment for sourceable POs.
SQL_SOURCEABLE_PO_FRAGMENT = (
    "is_test = 0 "
    "AND po_number IS NOT NULL AND TRIM(po_number) != '' "
    "AND LOWER(TRIM(po_number)) NOT IN ('n/a','na','tbd','pending','none','null','test') "
    "AND LOWER(COALESCE(status, '')) NOT IN ('invoiced','paid','closed','cancelled') "
    "AND (quote_number IS NULL OR TRIM(quote_number) = '')"
)


def active_queue_sql_clause() -> tuple[str, tuple[str, ...]]:
    """Returns (clause, params) for an active-queue WHERE filter.

    Use as:
        clause, params = active_queue_sql_clause()
        cur.execute(f"SELECT * FROM rfqs WHERE {clause}", params)
    """
    statuses = tuple(sorted(ACTIVE_QUEUE_EXCLUDED_STATUSES))
    placeholders = ",".join("?" for _ in statuses)
    return (
        SQL_ACTIVE_QUEUE_FRAGMENT.format(placeholders=placeholders),
        statuses,
    )


def revenue_year_sql_clause(
    year: int = REVENUE_YEAR,
    *,
    timestamp_field: str = "created_at",
) -> tuple[str, tuple[str, str]]:
    """Returns (clause, params) for a revenue-year WHERE filter.

    Use as:
        clause, params = revenue_year_sql_clause()
        cur.execute(f"SELECT SUM(total) FROM orders WHERE {clause}", params)
    """
    # Whitelist the field name to prevent injection; only known columns.
    if timestamp_field not in ("created_at", "logged_at", "sent_at",
                                "received_at", "po_date"):
        raise ValueError(f"unsafe timestamp_field: {timestamp_field!r}")
    clause = (
        f"is_test = 0 "
        f"AND {timestamp_field} >= ? AND {timestamp_field} < ?"
    )
    return clause, (revenue_year_start(year), revenue_year_end(year))


__all__ = [
    "REVENUE_YEAR",
    "ACTIVE_QUEUE_EXCLUDED_STATUSES",
    "TERMINAL_STATUSES",
    "INVOICED_OR_PAID_STATUSES",
    "revenue_year_start",
    "revenue_year_end",
    "is_active_queue",
    "is_real_sent",
    "is_sourceable_po",
    "is_year_revenue",
    "active_queue_sql_clause",
    "revenue_year_sql_clause",
    "SQL_ACTIVE_QUEUE_FRAGMENT",
    "SQL_REAL_SENT_FRAGMENT",
    "SQL_SOURCEABLE_PO_FRAGMENT",
]
