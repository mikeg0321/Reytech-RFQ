# po_email_v2.py — V2 PO email poller logic
#
# Processes inbound PO-related emails by writing through order_dal to the
# unified `orders` + `order_line_items` tables. Replaces the legacy
# `_process_po_email` path that wrote directly to `purchase_orders` /
# `po_line_items` / `po_emails` / `po_status_history`.
#
# Gated by feature flag `orders_v2.poller_unified` in routes_order_tracking.
# See docs/PRD_ORDERS_V2_POLLER_MIGRATION.md.
#
# Pure module — importable directly (no exec-only globals).

import json
import logging
import re
import sqlite3
from datetime import datetime

log = logging.getLogger("reytech.po_email_v2")


# ── Email parsing (extracted from routes_order_tracking) ─────────────────

# Patterns updated 2026-04-28 (PR #636) to capture canonical agency
# prefixes including dashes (CalVet `8955-NNNN`, DSH `4440-NNNN`).
# Previously digits-only `(\d{4,12})` stripped the prefix off CalVet
# and DSH POs; PR #635's po_prefix card surfaced 57% of prod POs as
# "unidentified" because of this. Token allows a single optional
# dashed suffix.
_PO_PATTERNS = [
    r'PO[#:\s]*(\d{4,}(?:-\d{4,})?)',
    r'Purchase Order[#:\s]*(\d{4,}(?:-\d{4,})?)',
    r'P\.?O\.?\s*(\d{4,}(?:-\d{4,})?)',
    r'Order[#:\s]*(\d{4,}(?:-\d{4,})?)',
    r'#(\d{4,}(?:-\d{4,})?)',
]


def extract_po_numbers(text):
    """Return unique PO numbers found in text.

    Updated PR #636 to capture canonical dashed prefixes via the
    shared `_PO_PATTERNS` set above. Empty-string and pure-digit
    fragments are filtered out so we don't return `8955` and
    `0000044935` as two separate matches when the input was
    `8955-0000044935` (a single PO).
    """
    found = set()
    for pattern in _PO_PATTERNS:
        for m in re.finditer(pattern, text, re.IGNORECASE):
            tok = m.group(1)
            if tok:
                found.add(tok)
    return list(found)


def extract_status_updates(body):
    """Parse update events out of an email body. Returns a list of dicts.

    Same keyword set as the legacy poller — we only changed the sink, not the parser.
    """
    updates = []
    body_lower = body.lower()

    tracking_patterns = [
        (r'tracking[#:\s]*([A-Z0-9]{10,30})', "tracking"),
        (r'1Z[A-Z0-9]{16}', "ups"),
        (r'\b\d{12,22}\b', "fedex"),
        (r'94\d{20,22}', "usps"),
    ]
    for pattern, carrier in tracking_patterns:
        for m in re.finditer(pattern, body, re.IGNORECASE):
            tracking = m.group(1) if m.lastindex else m.group(0)
            updates.append({"type": "tracking", "tracking_number": tracking, "carrier": carrier})

    if any(kw in body_lower for kw in ("shipped", "dispatched", "in transit", "out for delivery")):
        updates.append({"type": "status_change", "new_status": "shipped"})
    if any(kw in body_lower for kw in ("delivered", "received", "signed for")):
        updates.append({"type": "status_change", "new_status": "delivered"})
    if any(kw in body_lower for kw in ("backorder", "back order", "out of stock", "delayed")):
        updates.append({"type": "status_change", "new_status": "backordered"})
    if any(kw in body_lower for kw in ("invoice", "billing", "payment due")):
        updates.append({"type": "status_change", "new_status": "invoiced"})
    if any(kw in body_lower for kw in ("order confirmed", "confirmation", "processing your order")):
        updates.append({"type": "status_change", "new_status": "confirmed"})

    return updates


# ── Status mapping: email keyword → V2 order status ─────────────────────

_STATUS_MAP = {
    "shipped": "shipped",
    "delivered": "delivered",
    "invoiced": "invoiced",
    "confirmed": "sourcing",   # V2 has no 'confirmed' — sourcing is closest
    "backordered": None,        # handled per-line, not via transition_order
}


# ── Direct order status update ───────────────────────────────────────────
#
# We bypass order_dal.transition_order because it reads a `status_history`
# column on the orders table that isn't part of the production schema or
# migrations. (transition_order swallows the error and returns ok=False, so
# the latent bug is invisible — fixing it is out of scope for this PR.)

def _set_order_status(order_id, new_status, notes):
    """Set orders.status + write an audit_log row. No status_history needed."""
    from src.core.db import get_db
    try:
        with get_db() as conn:
            old = conn.execute("SELECT status FROM orders WHERE id=?",
                               (order_id,)).fetchone()
            old_status = (old[0] if old else "") or ""
            if old_status == new_status:
                return
            conn.execute("UPDATE orders SET status=?, updated_at=? WHERE id=?",
                         (new_status, datetime.now().isoformat(), order_id))
            conn.execute(
                """INSERT INTO order_audit_log
                   (order_id, action, field, old_value, new_value, actor, details, created_at)
                   VALUES (?, 'status_transition', 'status', ?, ?, 'email_poller', ?, ?)""",
                (order_id, old_status, new_status, notes, datetime.now().isoformat()))
    except Exception as e:
        log.warning("_set_order_status(%s, %s) failed: %s", order_id, new_status, e)


# ── Apply one update through order_dal ──────────────────────────────────

def apply_update(order_dal, order_id, open_line_ids, update):
    """Apply one parsed update to an order. Pass `order_dal` in to keep this testable.

    Returns True if line statuses were touched (so the caller knows to roll up
    via compute_order_status). False if the update only set orders.status.
    """
    today = datetime.now().isoformat()[:10]
    utype = update.get("type")

    if utype == "tracking":
        tracking = update.get("tracking_number", "")
        carrier = update.get("carrier", "")
        for lid in open_line_ids:
            order_dal.update_line_status(order_id, lid, "tracking_number",
                                         tracking, actor="email_poller")
            if carrier:
                order_dal.update_line_status(order_id, lid, "carrier",
                                             carrier, actor="email_poller")
            order_dal.update_line_status(order_id, lid, "ship_date",
                                         today, actor="email_poller")
            order_dal.update_line_status(order_id, lid, "sourcing_status",
                                         "shipped", actor="email_poller")
        return True

    if utype == "status_change":
        new_status = update.get("new_status", "")

        if new_status == "backordered":
            for lid in open_line_ids:
                order_dal.update_line_status(order_id, lid, "sourcing_status",
                                             "backordered", actor="email_poller")
            _set_order_status(order_id, "new", "email keyword: backordered")
            return True

        v2_status = _STATUS_MAP.get(new_status)
        if v2_status is None:
            log.debug("unknown email status keyword '%s' — skipping", new_status)
            return False

        if new_status in ("shipped", "delivered"):
            for lid in open_line_ids:
                order_dal.update_line_status(order_id, lid, "sourcing_status",
                                             new_status, actor="email_poller")
            _set_order_status(order_id, v2_status, f"email keyword: {new_status}")
            return True

        # invoiced / confirmed: only touch orders.status (lines stay as-is)
        _set_order_status(order_id, v2_status, f"email keyword: {new_status}")
        return False

    return False


# ── Top-level: process one inbound email ────────────────────────────────

def process_email(subject, sender, body, email_uid, get_db=None, order_dal=None):
    """Match an inbound email to a V2 order, log it, and apply parsed updates.

    `get_db` and `order_dal` are injected so tests can stub them. Production
    callers pass the real `src.core.db.get_db` and `src.core.order_dal`.
    """
    if get_db is None:
        from src.core.db import get_db as _get_db
        get_db = _get_db
    if order_dal is None:
        from src.core import order_dal as _order_dal
        order_dal = _order_dal

    result = {"matched": False, "po_id": None, "updates": []}

    po_numbers = extract_po_numbers(subject + " " + body)
    if not po_numbers:
        return result

    with get_db() as conn:
        conn.row_factory = sqlite3.Row
        for po_num in po_numbers:
            row = conn.execute(
                "SELECT id, status FROM orders WHERE po_number = ? LIMIT 1",
                (po_num,)
            ).fetchone()
            if not row:
                continue

            order_id = row["id"]
            result["matched"] = True
            result["po_id"] = order_id

            updates = extract_status_updates(body)

            try:
                conn.execute(
                    """INSERT INTO order_audit_log
                       (order_id, action, actor, details, created_at)
                       VALUES (?, 'inbound_email', 'email_poller', ?, ?)""",
                    (order_id,
                     json.dumps({
                         "subject": subject,
                         "sender": sender,
                         "email_uid": email_uid,
                         "body_preview": body[:500],
                         "parsed_updates": updates,
                     }, default=str),
                     datetime.now().isoformat()))
            except Exception as e:
                log.warning("audit log write failed for %s: %s", order_id, e)

            open_lines = conn.execute(
                """SELECT id FROM order_line_items
                   WHERE order_id = ? AND COALESCE(sourcing_status,'pending')
                       IN ('pending','ordered')""",
                (order_id,)
            ).fetchall()
            open_line_ids = [r["id"] for r in open_lines]

            touched_lines = False
            for update in updates:
                if apply_update(order_dal, order_id, open_line_ids, update):
                    touched_lines = True
                result["updates"].append(update)

            # Roll up only when at least one update changed line statuses.
            # invoiced/confirmed-only emails set orders.status directly and
            # would be clobbered back to 'new' by the rollup since no line
            # status reflects them.
            if touched_lines:
                try:
                    order_dal.compute_order_status(order_id, actor="email_poller")
                except Exception as e:
                    log.warning("compute_order_status(%s) failed: %s", order_id, e)

            log.info("po_email_v2.process_email: %s → %s with %d updates",
                     po_num, order_id, len(updates))
            break

    return result
