"""Deadline default + backfill helper.

Every active PC/RFQ should carry a due_date. Priority:
    1. Header (from PDF vision parse or structured extraction)
    2. Email-body regex extract
    3. Default = now + 2 business days @ 2:00 PM PST

Used at ingest (src/api/dashboard.py) and as a one-time startup backfill
pass for pre-existing records with blank due_date.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)

_PST = timezone(timedelta(hours=-8))

# Default close time matches _parse_due_datetime() in routes_deadlines.py —
# 2:00 PM PST is the earliest common CA agency close time.
_DEFAULT_CLOSE_TIME = "02:00 PM"
_DEFAULT_BIZ_DAYS = 2

# Inlined from routes_deadlines._SENT_STATUSES — duplicated intentionally to
# avoid triggering dashboard's exec()-loaded route module from a background
# thread (see backfill_missing_deadlines). Keep in sync with routes_deadlines.py.
_SENT_STATUSES = {"sent", "won", "lost", "dismissed", "archived", "expired", "pending_award"}


def add_business_days(start: datetime, n: int) -> datetime:
    """Return `start + n` business days (Mon-Fri), skipping weekends.

    Intentionally does NOT handle CA state holidays — keep it simple; a
    2-day default that lands on MLK day one day out of 365 is acceptable.
    Can be extended later if needed.
    """
    if n <= 0:
        return start
    d = start
    added = 0
    while added < n:
        d += timedelta(days=1)
        if d.weekday() < 5:  # Mon..Fri
            added += 1
    return d


def compute_default_deadline(
    now: datetime | None = None,
    biz_days: int = _DEFAULT_BIZ_DAYS,
) -> tuple[str, str]:
    """Return (date_str_mdy, time_str) for the fallback deadline.

    Format matches what `_parse_due_datetime` in routes_deadlines.py
    already accepts — mm/dd/YYYY + "02:00 PM".
    """
    now = now or datetime.now(_PST)
    target = add_business_days(now, biz_days)
    return target.strftime("%m/%d/%Y"), _DEFAULT_CLOSE_TIME


def resolve_or_default(
    header_date: str,
    header_time: str,
    email_body: str = "",
    now: datetime | None = None,
) -> tuple[str, str, str]:
    """Resolve a deadline (date_str, time_str, source).

    source ∈ {"header", "email", "default"}. Caller is expected to persist
    all three fields so downstream UI can show *why* a given due date is set.
    """
    if header_date and header_date.strip():
        return header_date.strip(), (header_time or "").strip(), "header"

    if email_body:
        try:
            from src.agents.requirement_extractor import _extract_due_date, _extract_due_time
            ext_date = _extract_due_date(email_body)
            ext_time = _extract_due_time(email_body) if ext_date else ""
        except Exception as e:
            log.debug("email extractor unavailable: %s", e)
            ext_date = ""
            ext_time = ""
        if ext_date:
            # _extract_due_date returns YYYY-MM-DD; leave as-is, parser accepts it.
            return ext_date, ext_time, "email"

    d, t = compute_default_deadline(now=now)
    return d, t, "default"


def apply_default_if_missing(doc: dict, email_body: str = "") -> str | None:
    """In-place: ensure `doc` has due_date/due_time/due_date_source.

    Returns the source that was applied (or None if already present).
    Safe to call on a PC or RFQ dict at any stage.
    """
    if not isinstance(doc, dict):
        return None
    if doc.get("due_date") and str(doc["due_date"]).strip():
        # Already set; just stamp the source if not recorded.
        doc.setdefault("due_date_source", "header")
        return None
    header = doc.get("header") or {}
    date, time_, source = resolve_or_default(
        header.get("due_date", "") or doc.get("due_date", ""),
        header.get("due_time", "") or doc.get("due_time", ""),
        email_body=email_body,
    )
    doc["due_date"] = date
    if time_:
        doc["due_time"] = time_
    doc["due_date_source"] = source
    return source


def backfill_missing_deadlines() -> dict:
    """Sweep active PCs/RFQs and apply the default where due_date is blank.

    Returns a summary dict. Called once at app startup.
    Skips records with status in the sent/archived set (same list
    routes_deadlines uses).
    """
    stats = {"pc_filled": 0, "rfq_filled": 0, "errors": 0}

    try:
        from src.api.data_layer import _load_price_checks, _save_single_pc, load_rfqs, _save_single_rfq
    except Exception as e:
        log.warning("backfill: data layer unavailable: %s", e)
        return stats

    try:
        pcs = _load_price_checks()
        for pcid, pc in pcs.items():
            if pc.get("status", "") in _SENT_STATUSES:
                continue
            if pc.get("is_test"):
                continue
            if pc.get("due_date") and str(pc["due_date"]).strip():
                continue
            try:
                src = apply_default_if_missing(pc, email_body=pc.get("email_body", ""))
                if src:
                    _save_single_pc(pcid, pc)
                    stats["pc_filled"] += 1
            except Exception as e:
                stats["errors"] += 1
                log.debug("backfill PC %s failed: %s", pcid, e)
    except Exception as e:
        log.warning("backfill PC pass failed: %s", e)

    try:
        rfqs = load_rfqs()
        for rid, r in rfqs.items():
            if r.get("status", "") in _SENT_STATUSES:
                continue
            if r.get("is_test"):
                continue
            if r.get("due_date") and str(r["due_date"]).strip():
                continue
            try:
                src = apply_default_if_missing(r, email_body=r.get("email_body", ""))
                if src:
                    _save_single_rfq(rid, r)
                    stats["rfq_filled"] += 1
            except Exception as e:
                stats["errors"] += 1
                log.debug("backfill RFQ %s failed: %s", rid, e)
    except Exception as e:
        log.warning("backfill RFQ pass failed: %s", e)

    if stats["pc_filled"] or stats["rfq_filled"]:
        log.info("Deadline backfill: filled %d PC + %d RFQ with default (+ %d errors)",
                 stats["pc_filled"], stats["rfq_filled"], stats["errors"])
    return stats
