"""Grouped /notifications view + /api/notifications/grouped API.

PR-C (back-window audit 2026-05-26): Mike's bell archive grows
quickly. Without a grouped view, he can't tell "this fires daily,
ignore" from "new today, investigate" without scrolling everything.

The grouped view collapses N notifications of the same event_type
into ONE row showing count + first_seen + last_seen + latest_detail
+ resolved indicator (set when the most recent observation for the
event's base name is a `_recovered` event, courtesy of PR-B's
liveness recovery close-out).

Read-only — no mutations to the notifications table; this is a
projection over the existing data.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from flask import jsonify, render_template, request

from src.api.shared import auth_required, bp

log = logging.getLogger("reytech.notifications")


_DEFAULT_WINDOW_DAYS = 7
_MAX_WINDOW_DAYS = 90


def _window_days() -> int:
    """Read `?days=N` query param. Clamped to [1, 90]."""
    raw = (request.args.get("days") or "").strip()
    if not raw:
        return _DEFAULT_WINDOW_DAYS
    try:
        v = int(raw)
        if v < 1:
            return 1
        if v > _MAX_WINDOW_DAYS:
            return _MAX_WINDOW_DAYS
        return v
    except (TypeError, ValueError):
        return _DEFAULT_WINDOW_DAYS


def _query_grouped(days: int) -> list[dict]:
    """Query notifications table, return per-event-type rollup.

    Each event_type collapses to:
      {
        event_type:    'gmail_oauth_expired'
        base_event:    'gmail_oauth_expired'        (strips _recovered)
        count:         5
        first_seen:    ISO
        last_seen:     ISO
        latest_title:  '⚠️ Gmail inbound poller: silent 96h'
        latest_body:   <truncated>
        latest_urgency: 'warning'
        latest_deep_link: '/api/notify/status'      (nullable)
        resolved:      bool                          (most recent obs is _recovered)
      }

    `resolved` is computed pair-wise: for each base event name (e.g.
    `gmail_oauth_expired`), if a `_recovered` notification exists AND
    its created_at > the latest stale notification's created_at, the
    base event is resolved. Both rows are still returned (so Mike sees
    the close-out + the prior alarm history); the base event's row
    just carries resolved=True.
    """
    from src.core.db import get_db

    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    out: dict[str, dict] = {}

    with get_db() as conn:
        rows = conn.execute(
            "SELECT event_type, urgency, title, body, deep_link, created_at "
            "FROM notifications "
            "WHERE created_at >= ? "
            "ORDER BY created_at DESC",
            (cutoff,),
        ).fetchall()

    for row in rows:
        if hasattr(row, "keys"):
            event_type = row["event_type"] or "unknown"
            urgency = row["urgency"] or "info"
            title = row["title"] or ""
            body = row["body"] or ""
            deep_link = row["deep_link"] or ""
            created_at = row["created_at"] or ""
        else:
            event_type, urgency, title, body, deep_link, created_at = row
            event_type = event_type or "unknown"
            urgency = urgency or "info"
            title = title or ""
            body = body or ""
            deep_link = deep_link or ""
            created_at = created_at or ""

        bucket = out.get(event_type)
        if bucket is None:
            base = event_type[:-len("_recovered")] if event_type.endswith("_recovered") else event_type
            out[event_type] = {
                "event_type": event_type,
                "base_event": base,
                "is_recovered_event": event_type.endswith("_recovered"),
                "count": 1,
                "first_seen": created_at,  # ORDER BY DESC → first encountered is newest
                "last_seen": created_at,
                "latest_title": title,
                "latest_body": (body[:300] + "…") if len(body) > 300 else body,
                "latest_urgency": urgency,
                "latest_deep_link": deep_link,
            }
        else:
            bucket["count"] += 1
            # first_seen should track the OLDEST observation; since rows
            # arrive newest-first, every additional row is older.
            bucket["first_seen"] = created_at

    # Resolved pairing: a base event is resolved if its companion
    # _recovered event has a last_seen strictly newer than the base
    # event's last_seen.
    for ev, bucket in out.items():
        if bucket["is_recovered_event"]:
            continue
        recovered = out.get(f"{ev}_recovered")
        if recovered and recovered["last_seen"] > bucket["last_seen"]:
            bucket["resolved"] = True
            bucket["resolved_at"] = recovered["last_seen"]
        else:
            bucket["resolved"] = False
            bucket["resolved_at"] = None

    # Sort: unresolved first (urgency desc), then resolved, then
    # _recovered-only events. Within each, newest last_seen first.
    _URGENCY_RANK = {"urgent": 0, "warning": 1, "info": 2}
    def _key(b):
        ur = _URGENCY_RANK.get(b.get("latest_urgency"), 3)
        is_recovered_only = b.get("is_recovered_event")
        is_resolved = b.get("resolved")
        # Tier 0: unresolved alerts, urgent → warning → info
        # Tier 1: resolved alerts
        # Tier 2: _recovered-event rows (close-out cards)
        if is_recovered_only:
            tier = 2
        elif is_resolved:
            tier = 1
        else:
            tier = 0
        return (tier, ur, -1 * _last_seen_sort_key(b.get("last_seen", "")))

    return sorted(out.values(), key=_key)


def _last_seen_sort_key(ts: str) -> int:
    """ISO timestamp → comparable int for sort. Bad/missing → 0."""
    try:
        return int(datetime.fromisoformat(
            ts.replace("Z", "+00:00") if "Z" in ts else ts,
        ).timestamp())
    except (ValueError, AttributeError):
        return 0


# ─── API ─────────────────────────────────────────────────────────────


@bp.route("/api/notifications/grouped")
@auth_required
def api_notifications_grouped():
    """Return bell archive grouped by event_type. Query param: days=N
    (default 7, capped at 90)."""
    days = _window_days()
    try:
        groups = _query_grouped(days)
    except Exception as e:
        log.error("notifications grouped query failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e), "groups": []}), 500
    return jsonify({"ok": True, "days": days, "groups": groups})


@bp.route("/api/notifications/mark-event-read", methods=["POST"])
@auth_required
def api_notifications_mark_event_read():
    """Mark every unread row for one `event_type` as read.

    Bell-side counterpart to PR #1102's Telegram supersede primitive.
    Pre-existing `/api/notifications/mark-read` accepts a list of IDs
    or no-args ("mark everything"); neither matches the grouped-view
    workflow ("dismiss this whole class — the underlying alarm is
    moot"). Today's volume driver — 1,313 `deadline_critical` events
    for the Coleman NON-IT misparse — is exactly the shape this
    endpoint targets: one click clears one class without nuking the
    rest of the archive.

    Body: `{"event_type": "deadline_critical"}`. Returns count updated
    so the UI can decrement its per-group counter without a refetch.
    """
    payload = request.get_json(silent=True) or {}
    event_type = (payload.get("event_type") or "").strip()
    if not event_type:
        return jsonify({"ok": False, "error": "event_type required"}), 400
    try:
        from src.agents.notify_agent import mark_event_type_read
        result = mark_event_type_read(event_type)
    except Exception as e:
        log.error("mark-event-read failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify(result)


# ─── Page ────────────────────────────────────────────────────────────


@bp.route("/notifications")
@auth_required
def notifications_page():
    """Grouped notifications view — one row per event_type with counts,
    first/last seen, resolved indicator. PR-C 2026-05-26."""
    days = _window_days()
    try:
        groups = _query_grouped(days)
    except Exception as e:
        log.error("notifications page query failed: %s", e, exc_info=True)
        groups = []
    return render_template(
        "notifications.html",
        groups=groups,
        days=days,
        total_count=sum(g.get("count", 0) for g in groups),
        active_page="Notifications",
    )
