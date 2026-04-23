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

# Precedence matches dashboard.py ingest (`body_text` → `body` → `body_preview`),
# extended with `email_body` because some admin/edit paths store it under that
# key. Checked in `_doc_email_body` when caller doesn't pass an explicit body.
_BODY_KEYS = ("email_body", "body_text", "body", "body_preview", "original_email_body")


def _doc_email_body(doc: dict) -> str:
    """Pick the first non-empty body field on a PC/RFQ dict.

    Ingest stores the buyer's email under a handful of different keys depending
    on which parser ran; without this helper, `apply_default_if_missing` silently
    misses the email-extracted due date and the 2-biz-day fallback wins.
    """
    for k in _BODY_KEYS:
        v = doc.get(k)
        if v and str(v).strip():
            return str(v)
    return ""


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


def apply_default_if_missing(doc: dict, email_body: str | None = None) -> str | None:
    """In-place: ensure `doc` has due_date/due_time/due_date_source.

    Returns the source that was applied (or None if already present).
    Safe to call on a PC or RFQ dict at any stage.

    `email_body` is optional — when omitted, the body is picked off `doc`
    itself via `_doc_email_body`, which knows the handful of keys ingest
    uses (body_text/body/body_preview/…). Callers that already have the
    raw text in hand can still pass it explicitly.
    """
    if not isinstance(doc, dict):
        return None
    if doc.get("due_date") and str(doc["due_date"]).strip():
        # Already set; just stamp the source if not recorded.
        doc.setdefault("due_date_source", "header")
        return None
    header = doc.get("header") or {}
    body = email_body if email_body is not None else _doc_email_body(doc)
    date, time_, source = resolve_or_default(
        header.get("due_date", "") or doc.get("due_date", ""),
        header.get("due_time", "") or doc.get("due_time", ""),
        email_body=body,
    )
    doc["due_date"] = date
    if time_:
        doc["due_time"] = time_
    doc["due_date_source"] = source
    return source


def re_resolve_default(doc: dict) -> str | None:
    """Re-run deadline resolution on a record currently stamped `default`.

    A `default` source means "app didn't know yet" — so if a buyer email body
    (from a late re-ingest, admin edit, or Gmail fetch after the initial stamp)
    later becomes available, that should win. Only this helper is authorized
    to overwrite an existing deadline; `apply_default_if_missing` never does.

    Behavior:
      - Returns None if `doc` isn't a dict or `due_date_source != "default"`.
      - Clears due_date/due_time, calls `apply_default_if_missing`.
      - If the new source is still `default` (no header, no email body),
        RESTORES the prior due_date/due_time and returns None. This avoids
        "walking" the default anchor forward by 2 biz days on every pass —
        a dormant record with no body text should stay anchored to its
        original stamp, not slide rightward every boot.
      - If the new source is `header` or `email`, returns that source.
        Caller should persist the updated doc.
    """
    if not isinstance(doc, dict):
        return None
    if doc.get("due_date_source") != "default":
        return None

    orig_due_date = doc.get("due_date")
    orig_due_time = doc.get("due_time")

    doc.pop("due_date", None)
    doc.pop("due_time", None)

    new_src = apply_default_if_missing(doc)

    if new_src == "default":
        # Still default — restore prior anchor so we don't drift the date.
        if orig_due_date is not None:
            doc["due_date"] = orig_due_date
        if orig_due_time is not None:
            doc["due_time"] = orig_due_time
        return None

    return new_src


def backfill_missing_deadlines() -> dict:
    """Sweep active PCs/RFQs — fill blank deadlines and re-resolve stale defaults.

    Two behaviors per record:
      1. No due_date at all → stamp via `apply_default_if_missing` (header →
         email body → 2-biz-day default). Counted as `pc_filled`/`rfq_filled`.
      2. due_date exists but `due_date_source == "default"` → re-run resolution
         via `re_resolve_default`. If a real header or email body is now
         available, the record is upgraded. Counted as `pc_re_resolved`/
         `rfq_re_resolved`.

    Records with `due_date_source == "header"` or `"email"` are left untouched.
    Sent/archived/test records are always skipped.

    Called once at app startup via routes_intel_ops._scprs_autostart.
    """
    stats = {
        "pc_filled": 0, "rfq_filled": 0,
        "pc_re_resolved": 0, "rfq_re_resolved": 0,
        "errors": 0,
    }

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
            has_due = bool(pc.get("due_date") and str(pc["due_date"]).strip())
            if not has_due:
                try:
                    src = apply_default_if_missing(pc)
                    if src:
                        _save_single_pc(pcid, pc)
                        stats["pc_filled"] += 1
                except Exception as e:
                    stats["errors"] += 1
                    log.debug("backfill PC %s failed: %s", pcid, e)
            elif pc.get("due_date_source") == "default":
                try:
                    new_src = re_resolve_default(pc)
                    if new_src:
                        _save_single_pc(pcid, pc)
                        stats["pc_re_resolved"] += 1
                except Exception as e:
                    stats["errors"] += 1
                    log.debug("backfill re-resolve PC %s failed: %s", pcid, e)
    except Exception as e:
        log.warning("backfill PC pass failed: %s", e)

    try:
        rfqs = load_rfqs()
        for rid, r in rfqs.items():
            if r.get("status", "") in _SENT_STATUSES:
                continue
            if r.get("is_test"):
                continue
            has_due = bool(r.get("due_date") and str(r["due_date"]).strip())
            if not has_due:
                try:
                    src = apply_default_if_missing(r)
                    if src:
                        _save_single_rfq(rid, r)
                        stats["rfq_filled"] += 1
                except Exception as e:
                    stats["errors"] += 1
                    log.debug("backfill RFQ %s failed: %s", rid, e)
            elif r.get("due_date_source") == "default":
                try:
                    new_src = re_resolve_default(r)
                    if new_src:
                        _save_single_rfq(rid, r)
                        stats["rfq_re_resolved"] += 1
                except Exception as e:
                    stats["errors"] += 1
                    log.debug("backfill re-resolve RFQ %s failed: %s", rid, e)
    except Exception as e:
        log.warning("backfill RFQ pass failed: %s", e)

    touched = (stats["pc_filled"] + stats["rfq_filled"]
               + stats["pc_re_resolved"] + stats["rfq_re_resolved"])
    if touched:
        log.info(
            "Deadline backfill: filled %d PC + %d RFQ, re-resolved %d PC + %d RFQ (+ %d errors)",
            stats["pc_filled"], stats["rfq_filled"],
            stats["pc_re_resolved"], stats["rfq_re_resolved"],
            stats["errors"],
        )
    return stats
