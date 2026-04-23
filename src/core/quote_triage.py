"""Triage sorter — the dashboard's "what do I do next" engine.

Mike's mental model (confirmed 2026-04-22):
    primary sort:   time-remaining ASC
    secondary sort: LOE ASC (easy-first within same time band)
    emergency rule: if any item's time_remaining < LOE * 1.25, that item
                    takes over — the operator should not touch anything else.

Inputs are deadline dicts as produced by routes_deadlines._build_deadline_item,
extended with `loe_minutes` from loe_estimator. Returns grouped buckets so the
UI can render the emergency banner, the "NEXT UP" card, and the queue.
"""
from __future__ import annotations

_EMERGENCY_SAFETY_FACTOR = 1.25  # if hours_left * 60 < loe * this, escalate
_STALE_OVERDUE_HOURS = 72  # past due by > 72h → collapse off the main view


def triage(deadlines: list[dict]) -> dict:
    """Split a deadline list into emergency / queue / stale_overdue_count.

    `deadlines` must already carry `hours_left` (float) and `loe_minutes` (int).

    Returns:
        {
            "emergency":  [items where time < LOE * 1.25 and not yet past due],
            "queue":      [everything actionable, sorted (hours_left, loe_minutes)],
            "stale_overdue_count": N,   # items past due by > 72h, collapsed
            "mode":       "emergency" | "normal",
        }
    """
    emergency: list[dict] = []
    queue: list[dict] = []
    stale_count = 0

    for d in deadlines:
        hl = d.get("hours_left")
        loe = d.get("loe_minutes") or 0

        if hl is None:
            # No deadline parsed — floor-of-queue. Still show so it's visible.
            queue.append(d)
            continue

        if hl < 0:
            # Already past due. Recent overdue stays actionable; stale drops.
            if abs(hl) > _STALE_OVERDUE_HOURS:
                stale_count += 1
                continue
            queue.append(d)
            continue

        # Future-dated. Emergency if LOE doesn't fit in remaining time.
        if loe > 0 and (hl * 60.0) < (loe * _EMERGENCY_SAFETY_FACTOR):
            emergency.append(d)
        else:
            queue.append(d)

    emergency.sort(key=lambda x: x.get("hours_left") or 0)
    # Queue: due-asc primary, LOE-asc within same hour-band (easy first).
    # Items with no hours_left sink to the bottom.
    queue.sort(key=lambda x: (
        x.get("hours_left") if x.get("hours_left") is not None else 1e9,
        x.get("loe_minutes") or 0,
    ))

    return {
        "emergency": emergency,
        "queue": queue,
        "stale_overdue_count": stale_count,
        "mode": "emergency" if emergency else "normal",
    }
