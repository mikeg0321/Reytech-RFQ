"""
Order Lifecycle + Revenue Tracking (F8)

Manages order status transitions from PO receipt through payment.
Statuses: received → processing → ordered_from_vendor → shipped → delivered → invoiced → paid

Also provides YTD revenue data from completed orders.
"""

import json
import logging
from datetime import datetime, timezone, timedelta

log = logging.getLogger("reytech.orders")

ORDER_STATUSES = [
    "received", "processing", "ordered_from_vendor",
    "shipped", "delivered", "invoiced", "paid",
]

STATUS_LABELS = {
    "received": "📥 PO Received",
    "processing": "⚙️ Processing",
    "ordered_from_vendor": "🏭 Ordered from Vendor",
    "shipped": "🚚 Shipped",
    "delivered": "✅ Delivered",
    "invoiced": "📄 Invoiced",
    "paid": "💰 Paid",
    "cancelled": "❌ Cancelled",
}


def _ensure_lifecycle_columns():
    """Add lifecycle columns to orders table if missing."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Add columns that may not exist yet
            new_cols = [
                ("vendor_order_id", "TEXT"),
                ("vendor_name", "TEXT"),
                ("tracking_number", "TEXT"),
                ("ship_date", "TEXT"),
                ("delivery_date", "TEXT"),
                ("invoice_number", "TEXT"),
                ("invoice_date", "TEXT"),
                ("payment_date", "TEXT"),
                ("payment_amount", "REAL"),
                ("buyer_name", "TEXT"),
                ("buyer_email", "TEXT"),
                ("status_history", "TEXT"),  # JSON array of transitions
            ]
            existing = {r[1] for r in conn.execute("PRAGMA table_info(orders)").fetchall()}
            for col_name, col_type in new_cols:
                if col_name not in existing:
                    conn.execute(f"ALTER TABLE orders ADD COLUMN {col_name} {col_type}")
                    log.debug("Added column orders.%s", col_name)
    except Exception as e:
        log.debug("Lifecycle columns: %s", e)


def transition_order(order_id: str, new_status: str, actor: str = "system",
                     notes: str = "", **kwargs) -> dict:
    """
    Move an order to a new status with audit logging.

    kwargs can include: tracking_number, ship_date, invoice_number, payment_amount, etc.
    """
    if new_status not in ORDER_STATUSES and new_status != "cancelled":
        return {"ok": False, "error": f"Invalid status: {new_status}"}

    _ensure_lifecycle_columns()

    try:
        from src.core.db import get_db
        now = datetime.now(timezone.utc).isoformat()

        with get_db() as conn:
            order = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
            if not order:
                return {"ok": False, "error": f"Order {order_id} not found"}

            order_dict = dict(order)
            old_status = order_dict.get("status", "unknown")

            # Build status history
            history = json.loads(order_dict.get("status_history") or "[]")
            history.append({
                "from": old_status,
                "to": new_status,
                "at": now,
                "actor": actor,
                "notes": notes,
            })

            # Build update fields
            updates = {
                "status": new_status,
                "updated_at": now,
                "status_history": json.dumps(history),
            }

            # Set milestone fields based on status
            if new_status == "shipped":
                updates["ship_date"] = kwargs.get("ship_date", now[:10])
                if kwargs.get("tracking_number"):
                    updates["tracking_number"] = kwargs["tracking_number"]
            elif new_status == "delivered":
                updates["delivery_date"] = kwargs.get("delivery_date", now[:10])
            elif new_status == "invoiced":
                if kwargs.get("invoice_number"):
                    updates["invoice_number"] = kwargs["invoice_number"]
                updates["invoice_date"] = kwargs.get("invoice_date", now[:10])
            elif new_status == "paid":
                updates["payment_date"] = kwargs.get("payment_date", now[:10])
                if kwargs.get("payment_amount"):
                    updates["payment_amount"] = kwargs["payment_amount"]
            elif new_status == "ordered_from_vendor":
                if kwargs.get("vendor_name"):
                    updates["vendor_name"] = kwargs["vendor_name"]
                if kwargs.get("vendor_order_id"):
                    updates["vendor_order_id"] = kwargs["vendor_order_id"]

            # Apply any extra kwargs
            for k, v in kwargs.items():
                if k in ("buyer_name", "buyer_email", "notes", "tracking_number"):
                    updates[k] = v

            # Execute update
            set_clause = ", ".join(f"{k}=?" for k in updates)
            values = list(updates.values()) + [order_id]
            conn.execute(f"UPDATE orders SET {set_clause} WHERE id=?", values)

            log.info("Order %s: %s → %s (by %s)", order_id, old_status, new_status, actor)

            return {
                "ok": True,
                "order_id": order_id,
                "old_status": old_status,
                "new_status": new_status,
                "transition_count": len(history),
            }

    except Exception as e:
        log.error("Order transition error: %s", e)
        return {"ok": False, "error": str(e)}


def get_order_detail(order_id: str) -> dict:
    """Full order detail with lifecycle timeline."""
    _ensure_lifecycle_columns()
    try:
        from src.core.db import get_db
        with get_db() as conn:
            order = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
            if not order:
                return {"ok": False, "error": "Not found"}
            d = dict(order)
            d["status_history"] = json.loads(d.get("status_history") or "[]")
            d["items"] = json.loads(d.get("items") or "[]")
            d["status_label"] = STATUS_LABELS.get(d.get("status", ""), d.get("status", ""))
            return {"ok": True, "order": d}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_revenue_ytd() -> dict:
    """YTD revenue from paid/invoiced orders."""
    _ensure_lifecycle_columns()
    try:
        from src.core.db import get_db
        now = datetime.now(timezone.utc)
        year_start = f"{now.year}-01-01"

        with get_db() as conn:
            # Revenue from orders
            order_rev = conn.execute("""
                SELECT COUNT(*) as count,
                       SUM(CASE WHEN status IN ('paid','invoiced','delivered') THEN total ELSE 0 END) as revenue,
                       SUM(CASE WHEN status = 'paid' THEN COALESCE(payment_amount, total) ELSE 0 END) as collected,
                       SUM(CASE WHEN status = 'invoiced' AND invoice_date < ? THEN total ELSE 0 END) as overdue
                FROM orders
                WHERE created_at >= ?
            """, ((now - timedelta(days=30)).isoformat(), year_start)).fetchone()

            # Revenue from revenue_log as well
            logged_rev = conn.execute("""
                SELECT SUM(amount) as total FROM revenue_log
                WHERE logged_at >= ?
            """, (year_start,)).fetchone()

            # Monthly breakdown
            monthly = conn.execute("""
                SELECT strftime('%Y-%m', created_at) as month,
                       COUNT(*) as orders,
                       SUM(total) as revenue
                FROM orders
                WHERE created_at >= ? AND status IN ('paid','invoiced','delivered','shipped')
                GROUP BY month ORDER BY month
            """, (year_start,)).fetchall()

            # Unpaid invoices > 30 days
            overdue_invoices = conn.execute("""
                SELECT id, quote_number, agency, institution, total,
                       invoice_number, invoice_date
                FROM orders
                WHERE status = 'invoiced'
                  AND invoice_date < ?
                ORDER BY invoice_date ASC
            """, ((now - timedelta(days=30)).strftime("%Y-%m-%d"),)).fetchall()

            return {
                "ok": True,
                "ytd": {
                    "total_orders": order_rev[0] or 0,
                    "revenue": round(order_rev[1] or 0, 2),
                    "collected": round(order_rev[2] or 0, 2),
                    "logged_revenue": round((logged_rev[0] or 0), 2),
                },
                "monthly": [{"month": r[0], "orders": r[1], "revenue": round(r[2] or 0, 2)} for r in monthly],
                "overdue_invoices": [
                    {"id": r[0], "quote": r[1], "agency": r[2], "institution": r[3],
                     "total": r[4], "invoice": r[5], "invoice_date": r[6]}
                    for r in overdue_invoices
                ],
                "overdue_count": len(overdue_invoices),
                "year": now.year,
            }
    except Exception as e:
        return {"ok": False, "error": str(e)}
