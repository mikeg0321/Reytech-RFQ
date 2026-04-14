"""Carrier tracking framework — Orders V2 phase 5.

Centralizes carrier detection + tracking-URL generation + status
lookup for orders_line_items rows. Consolidates the scattered logic
that previously lived in three places:

    src/agents/order_digest.py     — TRACKING_PATTERNS regex list
    src/agents/cs_agent.py         — hardcoded URL strings
    src/api/modules/routes_*       — per-route manual URL building

Anyone who needs to act on a tracking number should go through this
module. The `check_tracking_status()` function is the extension
point for a future UPS/FedEx API integration — it currently returns
the manually-entered status from the DB row, but the function
signature is stable so the API call can land as a single internal
change without touching any caller.
"""
from __future__ import annotations

import logging
import re
from typing import Optional, Tuple

log = logging.getLogger("reytech.carrier_tracking")


# ── Carrier identification ───────────────────────────────────────────────
#
# Patterns are ordered most-specific to most-generic. detect_carrier
# returns on the first match. Letter-only prefixes (TBA, 1Z) are
# checked before digit-only patterns because they're unambiguous.

_CARRIER_PATTERNS: list[Tuple[str, re.Pattern]] = [
    # Amazon — TBA followed by 10-25 digits
    ("Amazon", re.compile(r"^TBA\d{10,25}$", re.IGNORECASE)),
    # UPS — 1Z followed by 16-18 alphanumeric chars
    ("UPS", re.compile(r"^1Z[A-Z0-9]{16,18}$", re.IGNORECASE)),
    # USPS — 20-22 digit tracking starting with 94/93/92/91/95
    ("USPS", re.compile(r"^(?:94|93|92|91|95)\d{18,20}$")),
    # USPS certified/priority 13-char format: 2 alpha + 9 digit + 2 alpha
    ("USPS", re.compile(r"^[A-Z]{2}\d{9}[A-Z]{2}$")),
    # FedEx — 12 or 15-digit numeric
    ("FedEx", re.compile(r"^\d{12}$")),
    ("FedEx", re.compile(r"^\d{15}$")),
    # FedEx Ground — 22 digits
    ("FedEx", re.compile(r"^\d{22}$")),
    # DHL — 10-11 digit numeric (lower priority — overlaps FedEx)
    ("DHL", re.compile(r"^\d{10,11}$")),
    # OnTrac — 15 alphanum starting with C or D
    ("OnTrac", re.compile(r"^[CD]\d{14}$", re.IGNORECASE)),
]


def detect_carrier(tracking_number: str) -> str:
    """Return a carrier name ('UPS', 'FedEx', 'USPS', 'Amazon',
    'DHL', 'OnTrac') inferred from the tracking number's shape, or
    'Unknown' if no pattern matches. Never raises.

    The returned name is what `tracking_url()` and the frontend
    expect. Callers that already know the carrier (e.g., from a
    vendor email header) should pass it through directly instead
    of round-tripping through detection.
    """
    if not tracking_number:
        return "Unknown"
    tn = str(tracking_number).strip().upper().replace(" ", "").replace("-", "")
    for carrier, pattern in _CARRIER_PATTERNS:
        if pattern.match(tn):
            return carrier
    return "Unknown"


# ── Tracking URL generation ──────────────────────────────────────────────

_TRACKING_URLS = {
    "UPS": "https://www.ups.com/track?tracknum={tn}",
    "FedEx": "https://www.fedex.com/fedextrack/?tracknumbers={tn}",
    "USPS": "https://tools.usps.com/go/TrackConfirmAction?tLabels={tn}",
    "Amazon": "https://www.amazon.com/progress-tracker/package/ref=oh_aui_st_{tn}",
    "DHL": "https://www.dhl.com/us-en/home/tracking/tracking-parcel.html?submit=1&tracking-id={tn}",
    "OnTrac": "https://www.ontrac.com/tracking/?number={tn}",
}


def tracking_url(carrier: str, tracking_number: str) -> str:
    """Return a public tracking URL for the given carrier + tracking
    number, or empty string if the carrier is unknown.

    Accepts 'Unknown' carrier and will try to auto-detect. This is
    the one-stop function the UI should use when rendering a "Track"
    link — it handles the auto-detect fallback so callers don't
    need to sprinkle `detect_carrier` calls everywhere.
    """
    if not tracking_number:
        return ""
    tn = str(tracking_number).strip()
    if not carrier or carrier == "Unknown":
        carrier = detect_carrier(tn)
    url_template = _TRACKING_URLS.get(carrier)
    if not url_template:
        return ""
    return url_template.format(tn=tn)


def carrier_and_url(tracking_number: str,
                     known_carrier: str = "") -> Tuple[str, str]:
    """Convenience wrapper: given a tracking number (and optionally a
    pre-known carrier), return a (carrier, url) tuple. Both values
    are safe to embed in templates — the URL is empty on unknown
    carriers, never None, so Jinja's default filter is enough.
    """
    carrier = known_carrier or detect_carrier(tracking_number)
    url = tracking_url(carrier, tracking_number)
    return carrier, url


# ── Status lookup (extension point) ──────────────────────────────────────

def check_tracking_status(order_id: str, line_id: int) -> dict:
    """Return the current known tracking status for a specific order
    line item. Shape:

        {
          "ok": True|False,
          "order_id": "ord_...",
          "line_id": 42,
          "tracking_number": "1Z999...",
          "carrier": "UPS",
          "carrier_url": "https://...",
          "status": "shipped" | "delivered" | "in_transit" | "pending",
          "ship_date": "2026-04-10",
          "delivery_date": "2026-04-14",
          "source": "manual" | "carrier_api",
          "last_checked_at": "2026-04-14T12:00:00",
          "needs_api_check": True|False,
        }

    Current implementation: reads whatever the user manually entered
    on the order_line_items row and fills in the carrier + URL. When
    a UPS/FedEx API integration lands, it'll call the API here and
    update the row in place — no caller changes needed.

    `needs_api_check` is True when the line has a tracking number
    but status is still "shipped" or "pending" — i.e., a candidate
    for the future auto-status background job.
    """
    from src.core.db import get_db
    try:
        with get_db() as conn:
            row = conn.execute(
                """SELECT id, order_id, line_number, tracking_number,
                          carrier, ship_date, delivery_date, sourcing_status,
                          description
                     FROM order_line_items
                    WHERE order_id = ? AND (id = ? OR line_number = ?)""",
                (order_id, line_id, line_id),
            ).fetchone()
    except Exception as e:
        log.warning("check_tracking_status DB error: %s", e)
        return {"ok": False, "error": str(e)}

    if not row:
        return {
            "ok": False,
            "error": "line not found",
            "order_id": order_id,
            "line_id": line_id,
        }

    d = dict(row) if hasattr(row, "keys") else {
        "id": row[0], "order_id": row[1], "line_number": row[2],
        "tracking_number": row[3], "carrier": row[4],
        "ship_date": row[5], "delivery_date": row[6],
        "sourcing_status": row[7], "description": row[8],
    }

    tracking = d.get("tracking_number") or ""
    carrier, url = carrier_and_url(tracking, d.get("carrier") or "")
    status = d.get("sourcing_status") or "pending"

    # The future auto-status job will target this subset: there's a
    # tracking number but we haven't yet marked the item delivered.
    needs_api_check = bool(
        tracking and status in ("shipped", "in_transit", "pending", "ordered")
        and not d.get("delivery_date")
    )

    return {
        "ok": True,
        "order_id": d.get("order_id") or order_id,
        "line_id": d.get("id"),
        "line_number": d.get("line_number"),
        "description": (d.get("description") or "")[:120],
        "tracking_number": tracking,
        "carrier": carrier,
        "carrier_url": url,
        "status": status,
        "ship_date": d.get("ship_date") or "",
        "delivery_date": d.get("delivery_date") or "",
        "source": "manual",
        "needs_api_check": needs_api_check,
    }


__all__ = [
    "detect_carrier",
    "tracking_url",
    "carrier_and_url",
    "check_tracking_status",
]
