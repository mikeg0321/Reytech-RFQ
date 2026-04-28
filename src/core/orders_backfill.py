"""Idempotent order materialization for any won quote.

Closes the gap PR #629's orders-drift card surfaced: 102/102 won quotes
on prod had NO matching `orders` row because the three background
workers that auto-mark wins (award_tracker, email_poller,
revenue_engine) update `quotes.status='won'` directly without ever
calling _create_order_from_quote.

The operator-driven Mark Won path in routes_crm.py already creates an
order. This helper exists for the BACKGROUND paths so they don't
silently leave orphan wins.

Idempotent: if an orders row already exists for the quote, returns
its order_id without writing. Safe to call from any path that just
flipped a quote to 'won'.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

log = logging.getLogger("reytech.orders_backfill")


def ensure_order_for_won_quote(quote_number: str, po_number: str = "",
                               actor: str = "system") -> dict:
    """Make sure an orders row exists for a won quote.

    Args:
        quote_number: The quote that was just marked won.
        po_number: PO number from buyer (optional; some background
                   detection paths know it, others don't).
        actor: Who created the order (for the audit log).

    Returns:
        {"ok": True/False, "order_id": "...", "created": True/False, "error": "..."}

        - created=True if we wrote a new row.
        - created=False with ok=True if the row already existed.
        - ok=False with error set if the quote couldn't be loaded or
          the write failed.
    """
    if not quote_number:
        return {"ok": False, "error": "quote_number is required",
                "order_id": "", "created": False}

    from src.core.db import get_db
    try:
        with get_db() as conn:
            # Idempotency check: any existing orders row referencing this quote?
            existing = conn.execute(
                "SELECT id FROM orders WHERE quote_number = ? LIMIT 1",
                (quote_number,)
            ).fetchone()
            if existing:
                return {"ok": True, "order_id": existing["id"],
                        "created": False, "error": ""}

            # Load the quote row + its line_items JSON for the order's
            # initial line-item snapshot.
            qrow = conn.execute("""
                SELECT quote_number, agency, institution, total, subtotal,
                       tax, contact_name, contact_email, line_items,
                       ship_to_name, ship_to_address, is_test
                FROM quotes
                WHERE quote_number = ?
                LIMIT 1
            """, (quote_number,)).fetchone()
    except Exception as e:
        log.warning("ensure_order: existence check failed for %s: %s",
                    quote_number, e)
        return {"ok": False, "error": str(e),
                "order_id": "", "created": False}

    if not qrow:
        return {"ok": False, "error": f"quote {quote_number} not found",
                "order_id": "", "created": False}

    # Build the order. order_id mirrors _create_order_from_quote's
    # convention so a backfill collision (where a future operator-side
    # Mark Won runs on the same quote) just overwrites — ON CONFLICT
    # in save_order makes that an UPDATE, not a duplicate row.
    order_id = f"ORD-{quote_number}"

    try:
        line_items = json.loads(qrow["line_items"] or "[]")
    except (ValueError, TypeError):
        line_items = []
    if not isinstance(line_items, list):
        line_items = []

    try:
        ship_to_address = json.loads(qrow["ship_to_address"] or "[]")
    except (ValueError, TypeError):
        ship_to_address = []

    now_iso = datetime.now().isoformat()
    order = {
        "order_id": order_id,
        "quote_number": qrow["quote_number"] or quote_number,
        "po_number": po_number or "",
        "agency": qrow["agency"] or "",
        "institution": qrow["institution"] or "",
        "buyer_name": qrow["contact_name"] or "",
        "buyer_email": qrow["contact_email"] or "",
        "ship_to": qrow["ship_to_name"] or "",
        "ship_to_address": ship_to_address,
        "total": float(qrow["total"] or 0),
        "subtotal": float(qrow["subtotal"] or 0),
        "tax": float(qrow["tax"] or 0),
        "line_items": line_items,
        "items": line_items,
        "status": "new",
        "is_test": bool(qrow["is_test"]),
        "created_at": now_iso,
        "updated_at": now_iso,
        # Field that lets ops trace which background path created the
        # order — useful when the drift card flips green so we know
        # which worker fixed itself.
        "notes": f"auto-created from won quote by {actor}",
    }

    try:
        from src.core.order_dal import save_order, save_line_items_batch
        save_order(order_id, order, actor=actor)
        if line_items:
            save_line_items_batch(order_id, line_items)
    except Exception as e:
        log.warning("ensure_order: save_order failed for %s: %s",
                    quote_number, e)
        return {"ok": False, "error": str(e),
                "order_id": "", "created": False}

    log.info("ensure_order: created %s for won quote %s (actor=%s)",
             order_id, quote_number, actor)
    return {"ok": True, "order_id": order_id, "created": True, "error": ""}
