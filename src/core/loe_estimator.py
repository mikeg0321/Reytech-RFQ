"""Level-of-Effort estimator for PCs/RFQs.

Mike's model (2026-04-22): LOE represents **human review time** assuming the
app did its job on parsing, pricing, and form-filling. It is NOT "how long
to do this from scratch by hand." Tiered by item count:

    1–5 items    →  15 min
    6–15 items   →  30 min
    16–29 items  →  1 hour
    30+ items    →  2 hours

Agency/form shape does NOT bump LOE — if the app is doing its job, reviewing
a CCHCS packet takes the same time as reviewing a plain CDCR RFQ at the same
item count. Packet assembly, sig pages, 703B/C, etc. are all automated.

The one bump kept: parse failure adds +15 min for manual re-parse / data
cleanup, because that is human work the app failed to eliminate.
"""
from __future__ import annotations

# Tier thresholds (inclusive upper bound on items)
_TIERS = (
    (5,  15),   # 1–5 items:    15 min review
    (15, 30),   # 6–15 items:   30 min review
    (29, 60),   # 16–29 items:  1 hour review
)
_LARGE_TIER_MIN = 120   # 30+ items: 2 hours review
_PARSE_FAIL_MIN = 15    # manual re-parse / data cleanup
_MIN_FLOOR_MIN = 15     # every quote carries at least the 1–5 tier cost


def _item_count(doc: dict) -> int:
    items = doc.get("line_items") or doc.get("items") or []
    if isinstance(items, str):
        try:
            import json
            items = json.loads(items)
        except Exception:
            items = []
    return len(items) if isinstance(items, list) else 0


def _is_parse_failed(doc: dict) -> bool:
    if doc.get("_parse_failed"):
        return True
    if (doc.get("status") or "").lower() == "parse_error":
        return True
    if _item_count(doc) == 0 and (doc.get("status") or "").lower() in ("new", "parsed"):
        return True
    return False


def _tier_minutes(n_items: int) -> int:
    """Pick the tier minutes for a given item count."""
    for upper, mins in _TIERS:
        if n_items <= upper:
            return mins
    return _LARGE_TIER_MIN


def estimate_loe_minutes(doc: dict) -> int:
    """Return minutes-of-effort estimate for a PC/RFQ dict.

    Pure function — no DB, no I/O. Safe to call on any queue row.
    """
    if not isinstance(doc, dict):
        return _MIN_FLOOR_MIN

    minutes = _tier_minutes(_item_count(doc))
    if _is_parse_failed(doc):
        minutes += _PARSE_FAIL_MIN

    return max(minutes, _MIN_FLOOR_MIN)


def loe_label(minutes: int) -> str:
    """Human-readable LOE badge: '20 min', '1.5 h', '3 h'."""
    if minutes < 60:
        return f"{int(round(minutes))} min"
    hours = minutes / 60.0
    if hours < 2:
        return f"{hours:.1f} h"
    return f"{int(round(hours))} h"
