"""Deadline tracking API + hard-alert system for due dates.

Routes:
    GET /api/deadlines           — All active PCs/RFQs with due dates, urgency, countdown
    GET /api/deadlines/critical  — Only items due within 4 hours (for hard alert)

Used by:
    - base.html sidebar strip (persistent "Next due" on every page)
    - home.html countdown widget (deadline cards with live timers)
    - base.html hard-alert modal (blocks UI when any bid is <4h out and unsent)
"""
import logging
from datetime import datetime, timedelta, timezone

from flask import jsonify

from src.api.shared import bp, auth_required

log = logging.getLogger(__name__)

_PST = timezone(timedelta(hours=-8))
_PDT = timezone(timedelta(hours=-7))

_DATE_FMTS = ["%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"]
_TIME_FMTS = ["%I:%M %p", "%I:%M%p", "%H:%M", "%I %p", "%I%p"]

# Statuses that mean "already sent" — exclude from deadline alerts
_SENT_STATUSES = {"sent", "won", "lost", "dismissed", "archived", "expired", "pending_award"}


def _parse_due_datetime(due_date_str, due_time_str=""):
    """Parse due date + optional time into a PST datetime.

    Returns (datetime_pst, time_was_explicit) or (None, False).
    """
    if not due_date_str or not due_date_str.strip():
        return None, False

    dt = None
    for fmt in _DATE_FMTS:
        try:
            dt = datetime.strptime(due_date_str.strip(), fmt)
            break
        except ValueError:
            continue
    if dt is None:
        return None, False

    time_explicit = False
    if due_time_str and due_time_str.strip():
        for tfmt in _TIME_FMTS:
            try:
                t = datetime.strptime(due_time_str.strip(), tfmt)
                dt = dt.replace(hour=t.hour, minute=t.minute)
                time_explicit = True
                break
            except ValueError:
                continue

    if not time_explicit:
        # Default to 2:00 PM PST if no time specified (earliest common CA agency close)
        dt = dt.replace(hour=14, minute=0)

    # Assume PST
    dt = dt.replace(tzinfo=_PST)
    return dt, time_explicit


def _now_pst():
    return datetime.now(_PST)


def _build_deadline_item(doc_type, doc_id, doc):
    """Build a deadline dict from a PC or RFQ record."""
    header = doc.get("header") or {}
    due_date_str = header.get("due_date") or doc.get("due_date") or ""
    due_time_str = header.get("due_time") or doc.get("due_time") or ""

    due_dt, time_explicit = _parse_due_datetime(due_date_str, due_time_str)
    if due_dt is None:
        return None

    now = _now_pst()
    remaining = due_dt - now
    total_seconds = remaining.total_seconds()
    hours_left = total_seconds / 3600

    if total_seconds < 0:
        urgency = "overdue"
    elif hours_left <= 4:
        urgency = "critical"
    elif hours_left <= 24:
        urgency = "urgent"
    elif hours_left <= 72:
        urgency = "soon"
    else:
        urgency = "normal"

    # Human-readable countdown
    if total_seconds < 0:
        abs_hrs = abs(total_seconds) / 3600
        if abs_hrs < 1:
            countdown_text = f"{int(abs(total_seconds) / 60)}m overdue"
        elif abs_hrs < 24:
            countdown_text = f"{abs_hrs:.1f}h overdue"
        else:
            countdown_text = f"{int(abs_hrs / 24)}d overdue"
    else:
        if hours_left < 1:
            countdown_text = f"{int(total_seconds / 60)}m remaining"
        elif hours_left < 24:
            countdown_text = f"{hours_left:.1f}h remaining"
        else:
            countdown_text = f"{hours_left / 24:.1f}d remaining"

    institution = header.get("institution") or doc.get("institution") or ""
    pc_number = (header.get("pc_number") or doc.get("solicitation_number")
                 or doc.get("rfq_number") or doc.get("pc_number") or doc_id[:8])

    items = doc.get("line_items") or doc.get("items") or []
    item_count = len(items) if isinstance(items, list) else 0

    try:
        from src.core.loe_estimator import estimate_loe_minutes, loe_label
        loe_minutes = estimate_loe_minutes(doc)
        loe_text = loe_label(loe_minutes)
    except Exception as _e:
        log.debug("loe estimator unavailable: %s", _e)
        loe_minutes = 0
        loe_text = ""

    return {
        "doc_type": doc_type,
        "doc_id": doc_id,
        "pc_number": pc_number,
        "institution": institution,
        "due_date": due_date_str,
        "due_time": due_time_str,
        "due_iso": due_dt.isoformat(),
        "time_explicit": time_explicit,
        "hours_left": round(hours_left, 2),
        "total_seconds": round(total_seconds),
        "countdown_text": countdown_text,
        "urgency": urgency,
        "status": doc.get("status", ""),
        "item_count": item_count,
        "loe_minutes": loe_minutes,
        "loe_text": loe_text,
        "url": f"/pricecheck/{doc_id}" if doc_type == "pc" else f"/rfq/{doc_id}",
    }


def _scan_deadlines(urgencies=None):
    """Scan all active PCs/RFQs and return deadline items.

    Shared by /api/deadlines, /api/deadlines/critical, and the background
    deadline-escalation watcher in notify_agent.py.

    Args:
        urgencies: optional set of urgency levels to include
                   (e.g. {"overdue","critical"}). None = include all.
    """
    from src.api.data_layer import _load_price_checks, load_rfqs

    out = []

    pcs = _load_price_checks()
    for pcid, pc in pcs.items():
        if pc.get("status", "") in _SENT_STATUSES:
            continue
        if pc.get("is_test"):
            continue
        dl = _build_deadline_item("pc", pcid, pc)
        if dl and (urgencies is None or dl["urgency"] in urgencies):
            out.append(dl)

    rfqs = load_rfqs()
    for rid, r in rfqs.items():
        if r.get("status", "") in _SENT_STATUSES:
            continue
        # CR-5: test RFQs inside 4h were triggering the base.html
        # hard-alert modal. PC loop already filtered is_test; parity it here.
        if r.get("is_test"):
            continue
        dl = _build_deadline_item("rfq", rid, r)
        if dl and (urgencies is None or dl["urgency"] in urgencies):
            out.append(dl)

    return out


@bp.route("/api/deadlines")
@auth_required
def api_deadlines():
    """All active PCs/RFQs with due dates, sorted by urgency."""
    try:
        deadlines = _scan_deadlines()
        urgency_order = {"overdue": 0, "critical": 1, "urgent": 2, "soon": 3, "normal": 4}
        deadlines.sort(key=lambda d: (urgency_order.get(d["urgency"], 5), d["hours_left"]))

        return jsonify({
            "ok": True,
            "deadlines": deadlines,
            "count": len(deadlines),
            "critical_count": sum(1 for d in deadlines if d["urgency"] in ("overdue", "critical")),
        })

    except Exception as e:
        log.error("Deadlines API error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/triage")
@auth_required
def api_triage():
    """Operator-view triage: NEXT UP + queue, sorted by (time, LOE).

    Returns the grouped buckets the home dashboard consumes:
      {
        ok: True,
        mode: "emergency" | "normal",
        emergency: [...],          # items where time_remaining < LOE * 1.25
        next_up: {...} | None,     # the single card: emergency[0] or queue[0]
        queue: [...],              # rest of the actionable queue (next_up excluded)
        stale_overdue_count: N,    # > 72h past due, collapsed off the main view
        total: N,                  # all actionable items
      }
    """
    try:
        from src.api.data_layer import _load_price_checks, load_rfqs
        from src.core.quote_triage import triage

        deadlines = []
        pcs = _load_price_checks()
        for pcid, pc in pcs.items():
            if pc.get("status", "") in _SENT_STATUSES:
                continue
            if pc.get("is_test"):
                continue
            dl = _build_deadline_item("pc", pcid, pc)
            if dl:
                deadlines.append(dl)

        rfqs = load_rfqs()
        for rid, r in rfqs.items():
            if r.get("status", "") in _SENT_STATUSES:
                continue
            if r.get("is_test"):
                continue
            dl = _build_deadline_item("rfq", rid, r)
            if dl:
                deadlines.append(dl)

        t = triage(deadlines)
        if t["emergency"]:
            next_up = t["emergency"][0]
            remaining_emergency = t["emergency"][1:]
            queue = t["queue"]
        elif t["queue"]:
            next_up = t["queue"][0]
            remaining_emergency = []
            queue = t["queue"][1:]
        else:
            next_up = None
            remaining_emergency = []
            queue = []

        return jsonify({
            "ok": True,
            "mode": t["mode"],
            "next_up": next_up,
            "emergency": remaining_emergency,
            "queue": queue,
            "stale_overdue_count": t["stale_overdue_count"],
            "total": len(deadlines),
        })

    except Exception as e:
        log.error("Triage API error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/deadlines/critical")
@auth_required
def api_deadlines_critical():
    """Only items due within 4 hours or overdue — for the hard-alert modal."""
    try:
        critical = _scan_deadlines(urgencies={"overdue", "critical"})
        critical.sort(key=lambda d: d["hours_left"])

        return jsonify({
            "ok": True,
            "critical": critical,
            "count": len(critical),
        })

    except Exception as e:
        log.error("Critical deadlines API error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
