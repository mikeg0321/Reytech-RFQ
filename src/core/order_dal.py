# order_dal.py — Unified Order Data Access Layer (V2)
#
# Single source of truth for ALL order operations. Replaces:
# - dashboard.py: _load_orders, _save_single_order, _save_orders, _update_order_status
# - routes_orders_enhance.py: _load_orders (local copy)
# - order_digest.py: _load_orders (local copy)
# - invoice_processor.py: _load_orders (local copy)
# - data_layer.py: _load_orders (delegation)
#
# Storage: SQLite `orders` table (header) + `order_line_items` table (normalized lines).
# During transition, also dual-writes `data_json` blob for backward compatibility.

import json
import logging
from datetime import datetime, timezone

log = logging.getLogger("reytech.order_dal")

# ═══════════════════════════════════════════════════════════════════════
# Status definitions
# ═══════════════════════════════════════════════════════════════════════

ORDER_STATUSES = ["new", "sourcing", "shipped", "partial_delivery",
                  "delivered", "invoiced", "paid", "closed", "cancelled"]

LINE_STATUSES = ["pending", "ordered", "shipped", "delivered", "backordered"]

STATUS_LABELS = {
    "new": "🆕 New",
    "sourcing": "🛒 Sourcing",
    "shipped": "🚚 Shipped",
    "partial_delivery": "📦 Partial Delivery",
    "delivered": "✅ Delivered",
    "invoiced": "💰 Invoiced",
    "paid": "💰 Paid",
    "closed": "🏁 Closed",
    "cancelled": "❌ Cancelled",
}

LINE_STATUS_LABELS = {
    "pending": "⏳ Pending",
    "ordered": "🛒 Ordered",
    "shipped": "🚚 Shipped",
    "delivered": "✅ Delivered",
    "backordered": "⚠️ Backordered",
}

# Old lifecycle statuses → V2 mapping (for backward compatibility)
LEGACY_STATUS_MAP = {
    "received": "new",
    "processing": "sourcing",
    "ordered_from_vendor": "sourcing",
    "active": "new",
}


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _safe_json(val, default=None):
    """Parse JSON string, return default on failure."""
    if default is None:
        default = []
    if val is None:
        return default
    if isinstance(val, (list, dict)):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return default


# ═══════════════════════════════════════════════════════════════════════
# READ operations
# ═══════════════════════════════════════════════════════════════════════

def get_order(order_id: str) -> dict | None:
    """Load a single order with its line items from normalized tables.

    Returns dict with 'order_id', 'line_items', and all order fields.
    Falls back to data_json blob if normalized line items don't exist yet.
    """
    from src.core.db import get_db
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
            if not row:
                return None
            order = dict(row)
            order["order_id"] = order.get("id", order_id)

            # Try normalized line items first
            line_rows = conn.execute(
                "SELECT * FROM order_line_items WHERE order_id=? ORDER BY line_number",
                (order_id,)
            ).fetchall()

            if line_rows:
                order["line_items"] = [_row_to_line_item(lr) for lr in line_rows]
            else:
                # Fallback: parse from data_json blob
                blob = order.pop("data_json", None)
                if blob:
                    try:
                        full = json.loads(blob)
                        order["line_items"] = full.get("line_items", full.get("items", []))
                        # Merge any extra fields from blob that aren't in structured columns
                        for k in ("ship_to_name", "ship_to_address", "subtotal", "tax",
                                   "payment_terms", "source", "sender_email", "po_pdf",
                                   "draft_invoice", "qb_invoice_id", "qb_invoice_number",
                                   "qb_invoice_total", "qb_invoice_due", "invoice_status",
                                   "invoice_pdf", "invoice_pdf_enhanced", "invoice_pdf_raw",
                                   "invoice_sent_to", "invoice_sent_at", "delivered_at",
                                   "qb_customer_id", "status_history"):
                            if k not in order or not order[k]:
                                order[k] = full.get(k, "")
                    except (json.JSONDecodeError, TypeError):
                        order["line_items"] = _safe_json(order.get("items"), [])
                else:
                    order["line_items"] = _safe_json(order.get("items"), [])

            # Normalize legacy status
            st = order.get("status", "new")
            order["status"] = LEGACY_STATUS_MAP.get(st, st)
            order["status_label"] = STATUS_LABELS.get(order["status"], order["status"])

            return order
    except Exception as e:
        log.error("get_order(%s) failed: %s", order_id, e, exc_info=True)
        return None


def list_orders(status: str = None, agency: str = None,
                search: str = None, limit: int = 500) -> list[dict]:
    """Load orders list with line item summaries. Returns list of dicts.

    Each order includes computed fields: item_count, sourced_count,
    shipped_count, delivered_count, pct_complete.
    """
    from src.core.db import get_db
    try:
        with get_db() as conn:
            # Build query
            where_clauses = []
            params = []
            if status:
                where_clauses.append("o.status = ?")
                params.append(status)
            if agency:
                where_clauses.append("LOWER(o.agency) LIKE ?")
                params.append(f"%{agency.lower()}%")

            where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
            params.append(limit)

            rows = conn.execute(
                f"SELECT * FROM orders o{where_sql} ORDER BY o.created_at DESC LIMIT ?",
                params
            ).fetchall()

            orders = []
            for row in rows:
                order = dict(row)
                oid = order.get("id", "")
                order["order_id"] = oid

                # Normalize legacy status
                st = order.get("status", "new")
                order["status"] = LEGACY_STATUS_MAP.get(st, st)

                # Load line items — try normalized table first
                line_rows = conn.execute(
                    "SELECT * FROM order_line_items WHERE order_id=? ORDER BY line_number",
                    (oid,)
                ).fetchall()

                if line_rows:
                    items = [_row_to_line_item(lr) for lr in line_rows]
                else:
                    # Fallback to blob
                    blob = order.pop("data_json", None)
                    if blob:
                        try:
                            full = json.loads(blob)
                            items = full.get("line_items", full.get("items", []))
                            # Carry forward extra blob fields
                            for k in ("ship_to_name", "subtotal", "tax", "draft_invoice",
                                       "qb_invoice_id", "qb_invoice_number", "invoice_status",
                                       "delivered_at", "status_history"):
                                if k not in order or not order[k]:
                                    order[k] = full.get(k, "")
                        except (json.JSONDecodeError, TypeError):
                            items = _safe_json(order.get("items"), [])
                    else:
                        items = _safe_json(order.get("items"), [])

                order["line_items"] = items

                # Compute summaries
                n = len(items)
                order["item_count"] = n
                order["sourced_count"] = sum(1 for it in items
                    if _item_status(it) in ("ordered", "shipped", "delivered"))
                order["shipped_count"] = sum(1 for it in items
                    if _item_status(it) in ("shipped", "delivered"))
                order["delivered_count"] = sum(1 for it in items
                    if _item_status(it) == "delivered")
                order["pct_complete"] = round(order["delivered_count"] / n * 100) if n else 0

                # Search filter (after loading items for searchable content)
                if search:
                    q = search.lower()
                    searchable = " ".join([
                        order.get("po_number", "") or "",
                        order.get("institution", "") or "",
                        oid,
                        order.get("quote_number", "") or "",
                        order.get("agency", "") or "",
                    ]).lower()
                    if q not in searchable:
                        continue

                orders.append(order)

            return orders
    except Exception as e:
        log.error("list_orders failed: %s", e, exc_info=True)
        return []


def load_orders_dict() -> dict:
    """Load all orders as {order_id: order_dict}. Backward compat for _load_orders().

    This is the transitional bridge — callers that used _load_orders() get the same
    dict-of-dicts shape they expect while we migrate them to list_orders().
    """
    orders = list_orders(limit=5000)
    return {o["order_id"]: o for o in orders}


def get_order_margins(order_id: str) -> dict:
    """Calculate margin data from unit_cost/unit_price on normalized line items."""
    from src.core.db import get_db
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT description, qty_ordered, unit_price, unit_cost,
                       extended_price, extended_cost, supplier_name
                FROM order_line_items WHERE order_id=? ORDER BY line_number
            """, (order_id,)).fetchall()

            lines = []
            total_revenue = 0
            total_cost = 0
            for r in rows:
                rev = (r["qty_ordered"] or 0) * (r["unit_price"] or 0)
                cost = (r["qty_ordered"] or 0) * (r["unit_cost"] or 0)
                margin = rev - cost
                pct = round(margin / rev * 100, 1) if rev > 0 else 0
                total_revenue += rev
                total_cost += cost
                lines.append({
                    "description": r["description"],
                    "qty": r["qty_ordered"],
                    "unit_price": r["unit_price"],
                    "unit_cost": r["unit_cost"],
                    "revenue": round(rev, 2),
                    "cost": round(cost, 2),
                    "margin": round(margin, 2),
                    "margin_pct": pct,
                    "supplier": r["supplier_name"],
                    "has_cost": bool(r["unit_cost"]),
                })

            total_margin = total_revenue - total_cost
            return {
                "ok": True,
                "lines": lines,
                "total_revenue": round(total_revenue, 2),
                "total_cost": round(total_cost, 2),
                "total_margin": round(total_margin, 2),
                "margin_pct": round(total_margin / total_revenue * 100, 1) if total_revenue > 0 else 0,
                "items_with_cost": sum(1 for l in lines if l["has_cost"]),
                "items_total": len(lines),
            }
    except Exception as e:
        log.error("get_order_margins(%s): %s", order_id, e)
        return {"ok": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════
# WRITE operations
# ═══════════════════════════════════════════════════════════════════════

def save_order(order_id: str, order: dict, actor: str = "system") -> bool:
    """Save/update order header. Also dual-writes data_json blob for backward compat.

    Does NOT save line items — use save_line_item() or save_line_items_batch() for that.
    """
    from src.core.db import get_db, db_retry

    def _do():
        with get_db() as conn:
            # Normalize status
            st = order.get("status", "new")
            st = LEGACY_STATUS_MAP.get(st, st)

            # Build the data_json blob for backward compat
            blob_dict = dict(order)
            blob_dict["order_id"] = order_id
            if "line_items" not in blob_dict:
                # Pull from normalized table
                line_rows = conn.execute(
                    "SELECT * FROM order_line_items WHERE order_id=? ORDER BY line_number",
                    (order_id,)
                ).fetchall()
                if line_rows:
                    blob_dict["line_items"] = [_row_to_line_item(lr) for lr in line_rows]

            conn.execute("""
                INSERT INTO orders
                (id, quote_number, po_number, agency, institution,
                 total, status, items, created_at, updated_at, data_json,
                 buyer_name, buyer_email, ship_to, ship_to_address,
                 total_cost, margin_pct, po_pdf_path, fulfillment_type, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    quote_number=excluded.quote_number,
                    po_number=excluded.po_number,
                    agency=excluded.agency,
                    institution=excluded.institution,
                    total=excluded.total,
                    status=excluded.status,
                    items=excluded.items,
                    updated_at=excluded.updated_at,
                    data_json=excluded.data_json,
                    buyer_name=excluded.buyer_name,
                    buyer_email=excluded.buyer_email,
                    ship_to=excluded.ship_to,
                    ship_to_address=excluded.ship_to_address,
                    total_cost=excluded.total_cost,
                    margin_pct=excluded.margin_pct,
                    po_pdf_path=excluded.po_pdf_path,
                    fulfillment_type=excluded.fulfillment_type,
                    notes=excluded.notes
            """, (
                order_id,
                order.get("quote_number", ""),
                order.get("po_number", ""),
                order.get("agency", ""),
                order.get("institution", order.get("customer", "")),
                order.get("total", 0),
                st,
                json.dumps(order.get("line_items", order.get("items", [])), default=str),
                order.get("created_at", _now_iso()),
                _now_iso(),
                json.dumps(blob_dict, default=str),
                order.get("buyer_name", ""),
                order.get("buyer_email", ""),
                order.get("ship_to", order.get("ship_to_name", "")),
                json.dumps(order.get("ship_to_address", []), default=str) if isinstance(order.get("ship_to_address"), (list, dict)) else order.get("ship_to_address", ""),
                order.get("total_cost", 0),
                order.get("margin_pct", 0),
                order.get("po_pdf_path", order.get("po_pdf", "")),
                order.get("fulfillment_type", "dropship"),
                order.get("notes", ""),
            ))

            # Audit log
            conn.execute("""
                INSERT INTO order_audit_log (order_id, action, actor, details, created_at)
                VALUES (?, 'save', ?, ?, ?)
            """, (order_id, actor, f"Order saved by {actor}", _now_iso()))

            log.info("order_dal.save_order(%s) by %s — status=%s total=$%.2f",
                     order_id, actor, st, order.get("total", 0))

    try:
        db_retry(_do, max_retries=3, delay=1.0)
        return True
    except Exception as e:
        log.error("save_order(%s) failed: %s", order_id, e, exc_info=True)
        return False


def save_line_item(order_id: str, item: dict) -> int | None:
    """Insert or update a single line item in order_line_items. Returns row id."""
    from src.core.db import get_db
    try:
        with get_db() as conn:
            qty = item.get("qty", item.get("qty_ordered", 0)) or 0
            price = item.get("unit_price", 0) or 0
            cost = item.get("unit_cost", item.get("cost", 0)) or 0

            existing_id = item.get("id")
            if existing_id:
                conn.execute("""
                    UPDATE order_line_items SET
                        description=?, part_number=?, mfg_number=?, asin=?, uom=?,
                        qty_ordered=?, unit_price=?, unit_cost=?,
                        extended_price=?, extended_cost=?,
                        sourcing_status=?, supplier_name=?, supplier_url=?,
                        vendor_order_id=?, vendor_order_ref=?,
                        tracking_number=?, carrier=?, ship_date=?,
                        expected_delivery=?, delivery_date=?,
                        invoice_status=?, invoice_number=?, notes=?,
                        updated_at=?
                    WHERE id=?
                """, (
                    item.get("description", ""),
                    item.get("part_number", ""),
                    item.get("mfg_number", ""),
                    item.get("asin", ""),
                    item.get("uom", "EA"),
                    qty, price, cost,
                    round(qty * price, 2), round(qty * cost, 2),
                    item.get("sourcing_status", "pending"),
                    item.get("supplier_name", item.get("supplier", "")),
                    item.get("supplier_url", ""),
                    item.get("vendor_order_id"),
                    item.get("vendor_order_ref", ""),
                    item.get("tracking_number", ""),
                    item.get("carrier", ""),
                    item.get("ship_date", ""),
                    item.get("expected_delivery", ""),
                    item.get("delivery_date", ""),
                    item.get("invoice_status", "pending"),
                    item.get("invoice_number", ""),
                    item.get("notes", ""),
                    _now_iso(),
                    existing_id,
                ))
                return existing_id
            else:
                # Determine next line number
                max_ln = conn.execute(
                    "SELECT MAX(line_number) as m FROM order_line_items WHERE order_id=?",
                    (order_id,)
                ).fetchone()
                next_ln = (max_ln["m"] or 0) + 1 if max_ln else 1

                cursor = conn.execute("""
                    INSERT INTO order_line_items
                    (order_id, line_number, description, part_number, mfg_number,
                     asin, uom, qty_ordered, unit_price, unit_cost,
                     extended_price, extended_cost,
                     sourcing_status, supplier_name, supplier_url,
                     tracking_number, carrier, ship_date, delivery_date,
                     invoice_status, invoice_number, notes,
                     created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    order_id, next_ln,
                    item.get("description", ""),
                    item.get("part_number", ""),
                    item.get("mfg_number", ""),
                    item.get("asin", ""),
                    item.get("uom", "EA"),
                    qty, price, cost,
                    round(qty * price, 2), round(qty * cost, 2),
                    item.get("sourcing_status", "pending"),
                    item.get("supplier_name", item.get("supplier", "")),
                    item.get("supplier_url", ""),
                    item.get("tracking_number", ""),
                    item.get("carrier", ""),
                    item.get("ship_date", ""),
                    item.get("delivery_date", ""),
                    item.get("invoice_status", "pending"),
                    item.get("invoice_number", ""),
                    item.get("notes", ""),
                    _now_iso(), _now_iso(),
                ))
                new_id = cursor.lastrowid
                log.info("order_dal.save_line_item(%s) → new line #%d (id=%d)",
                         order_id, next_ln, new_id)
                return new_id
    except Exception as e:
        log.error("save_line_item(%s) failed: %s", order_id, e, exc_info=True)
        return None


def save_line_items_batch(order_id: str, items: list[dict]) -> bool:
    """Replace all line items for an order. Used during PO import / order creation."""
    from src.core.db import get_db
    try:
        with get_db() as conn:
            conn.execute("DELETE FROM order_line_items WHERE order_id=?", (order_id,))
            for i, item in enumerate(items):
                qty = item.get("qty", item.get("qty_ordered", 0)) or 0
                price = item.get("unit_price", 0) or 0
                cost = item.get("unit_cost", item.get("cost", 0)) or 0
                conn.execute("""
                    INSERT INTO order_line_items
                    (order_id, line_number, description, part_number, mfg_number,
                     asin, uom, qty_ordered, unit_price, unit_cost,
                     extended_price, extended_cost,
                     sourcing_status, supplier_name, supplier_url,
                     tracking_number, carrier, ship_date, delivery_date,
                     invoice_status, invoice_number, notes,
                     created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    order_id, i + 1,
                    item.get("description", ""),
                    item.get("part_number", ""),
                    item.get("mfg_number", ""),
                    item.get("asin", ""),
                    item.get("uom", "EA"),
                    qty, price, cost,
                    round(qty * price, 2), round(qty * cost, 2),
                    item.get("sourcing_status", "pending"),
                    item.get("supplier_name", item.get("supplier", "")),
                    item.get("supplier_url", ""),
                    item.get("tracking_number", ""),
                    item.get("carrier", ""),
                    item.get("ship_date", ""),
                    item.get("delivery_date", ""),
                    item.get("invoice_status", "pending"),
                    item.get("invoice_number", ""),
                    item.get("notes", ""),
                    _now_iso(), _now_iso(),
                ))
            log.info("order_dal.save_line_items_batch(%s) — %d items", order_id, len(items))
            return True
    except Exception as e:
        log.error("save_line_items_batch(%s) failed: %s", order_id, e, exc_info=True)
        return False


def update_line_status(order_id: str, line_id: str, field: str, value,
                       actor: str = "user") -> bool:
    """Update a single field on a line item. Handles both normalized table and blob sync.

    line_id: either the integer id from order_line_items, or the legacy 'L001' string.
    field: one of sourcing_status, tracking_number, carrier, supplier_name, supplier_url, etc.
    """
    from src.core.db import get_db
    try:
        with get_db() as conn:
            # Find the line item row
            row = _find_line_item(conn, order_id, line_id)
            if not row:
                log.warning("update_line_status: line %s not found in order %s", line_id, order_id)
                return False

            item_db_id = row["id"]
            old_value = row.get(field, "")

            # Whitelist of updatable fields
            allowed = {"sourcing_status", "tracking_number", "carrier", "ship_date",
                        "delivery_date", "expected_delivery", "supplier_name", "supplier_url",
                        "invoice_status", "invoice_number", "unit_cost", "notes",
                        "vendor_order_id", "vendor_order_ref", "asin", "part_number",
                        "qty_ordered", "unit_price", "fulfillment_type", "qty_backordered"}
            if field not in allowed:
                log.warning("update_line_status: field '%s' not in allowed set", field)
                return False

            conn.execute(
                f"UPDATE order_line_items SET {field}=?, updated_at=? WHERE id=?",
                (value, _now_iso(), item_db_id)
            )

            # Recompute extended if price/cost/qty changed
            if field in ("unit_price", "unit_cost", "qty_ordered"):
                updated = conn.execute(
                    "SELECT qty_ordered, unit_price, unit_cost FROM order_line_items WHERE id=?",
                    (item_db_id,)
                ).fetchone()
                if updated:
                    q = updated["qty_ordered"] or 0
                    p = updated["unit_price"] or 0
                    c = updated["unit_cost"] or 0
                    conn.execute("""
                        UPDATE order_line_items SET
                            extended_price=?, extended_cost=?, updated_at=?
                        WHERE id=?
                    """, (round(q * p, 2), round(q * c, 2), _now_iso(), item_db_id))

            # Audit log
            conn.execute("""
                INSERT INTO order_audit_log
                (order_id, action, field, old_value, new_value, actor, created_at)
                VALUES (?, 'line_update', ?, ?, ?, ?, ?)
            """, (order_id, field, str(old_value), str(value), actor, _now_iso()))

            log.info("order_dal.update_line_status(%s, %s, %s=%s) by %s",
                     order_id, line_id, field, value, actor)

            # Sync data_json blob
            _sync_blob(conn, order_id)

            return True
    except Exception as e:
        log.error("update_line_status(%s, %s) failed: %s", order_id, line_id, e, exc_info=True)
        return False


def compute_order_status(order_id: str, actor: str = "system") -> str:
    """Recompute and save order status from line item statuses.

    Returns the new status string.
    """
    from src.core.db import get_db
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT sourcing_status, invoice_status FROM order_line_items WHERE order_id=?",
                (order_id,)
            ).fetchall()

            if not rows:
                # No normalized items — try blob fallback
                order = get_order(order_id)
                if order:
                    items = order.get("line_items", [])
                    rows = [{"sourcing_status": it.get("sourcing_status", "pending"),
                             "invoice_status": it.get("invoice_status", "pending")}
                            for it in items]

            if not rows:
                return "new"

            statuses = [r["sourcing_status"] or "pending" for r in rows]
            inv_statuses = [r["invoice_status"] or "pending" for r in rows]

            if all(s == "delivered" for s in statuses):
                if all(s == "invoiced" for s in inv_statuses):
                    new_status = "closed"
                elif any(s == "invoiced" for s in inv_statuses):
                    new_status = "invoiced"
                else:
                    new_status = "delivered"
            elif any(s == "delivered" for s in statuses):
                new_status = "partial_delivery"
            elif any(s == "shipped" for s in statuses):
                new_status = "shipped"
            elif any(s == "ordered" for s in statuses):
                new_status = "sourcing"
            else:
                new_status = "new"

            # Get old status for comparison
            old_row = conn.execute(
                "SELECT status FROM orders WHERE id=?", (order_id,)
            ).fetchone()
            old_status = old_row["status"] if old_row else "new"
            old_status = LEGACY_STATUS_MAP.get(old_status, old_status)

            if new_status != old_status:
                conn.execute(
                    "UPDATE orders SET status=?, updated_at=? WHERE id=?",
                    (new_status, _now_iso(), order_id)
                )
                conn.execute("""
                    INSERT INTO order_audit_log
                    (order_id, action, field, old_value, new_value, actor, created_at)
                    VALUES (?, 'status_change', 'status', ?, ?, ?, ?)
                """, (order_id, old_status, new_status, actor, _now_iso()))

                # Sync blob
                _sync_blob(conn, order_id)

                log.info("order_dal.compute_order_status(%s): %s → %s",
                         order_id, old_status, new_status)

            return new_status
    except Exception as e:
        log.error("compute_order_status(%s) failed: %s", order_id, e, exc_info=True)
        return "new"


def confirm_delivery(order_id: str, line_id: str, delivery_date: str = "",
                     tracking_number: str = "", carrier: str = "",
                     notes: str = "", actor: str = "user") -> bool:
    """Confirm delivery of a line item (dropship model). Creates delivery_log entry."""
    from src.core.db import get_db
    try:
        with get_db() as conn:
            row = _find_line_item(conn, order_id, line_id)
            if not row:
                return False

            item_db_id = row["id"]
            d_date = delivery_date or _now_iso()[:10]

            conn.execute("""
                UPDATE order_line_items SET
                    sourcing_status='delivered', delivery_date=?, updated_at=?
                WHERE id=?
            """, (d_date, _now_iso(), item_db_id))

            conn.execute("""
                INSERT INTO delivery_log
                (order_id, line_item_id, confirmed_at, delivery_date,
                 confirmation_source, tracking_number, carrier, notes, confirmed_by)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (order_id, item_db_id, _now_iso(), d_date,
                  "manual", tracking_number or row.get("tracking_number", ""),
                  carrier or row.get("carrier", ""), notes, actor))

            conn.execute("""
                INSERT INTO order_audit_log
                (order_id, action, field, new_value, actor, details, created_at)
                VALUES (?, 'delivery_confirmed', 'sourcing_status', 'delivered', ?, ?, ?)
            """, (order_id, actor,
                  f"Line {line_id} delivered on {d_date}" + (f" — {notes}" if notes else ""),
                  _now_iso()))

            _sync_blob(conn, order_id)
            log.info("order_dal.confirm_delivery(%s, %s) on %s", order_id, line_id, d_date)

        # Recompute order status after confirming delivery
        compute_order_status(order_id, actor=actor)
        return True
    except Exception as e:
        log.error("confirm_delivery(%s, %s) failed: %s", order_id, line_id, e, exc_info=True)
        return False


def delete_order(order_id: str, actor: str = "user", reason: str = "") -> bool:
    """Delete an order and its line items."""
    from src.core.db import get_db
    try:
        with get_db() as conn:
            conn.execute("DELETE FROM order_line_items WHERE order_id=?", (order_id,))
            conn.execute("DELETE FROM delivery_log WHERE order_id=?", (order_id,))
            conn.execute("DELETE FROM orders WHERE id=?", (order_id,))
            conn.execute("""
                INSERT INTO order_audit_log
                (order_id, action, actor, details, created_at)
                VALUES (?, 'delete', ?, ?, ?)
            """, (order_id, actor, reason or "Order deleted", _now_iso()))
            log.info("order_dal.delete_order(%s) by %s: %s", order_id, actor, reason)
            return True
    except Exception as e:
        log.error("delete_order(%s) failed: %s", order_id, e, exc_info=True)
        return False


# ═══════════════════════════════════════════════════════════════════════
# Revenue & Lifecycle (migrated from order_lifecycle.py)
# ═══════════════════════════════════════════════════════════════════════

def get_revenue_ytd() -> dict:
    """YTD revenue from paid/invoiced orders + revenue_log."""
    from src.core.db import get_db
    from datetime import timedelta
    try:
        now = datetime.now(timezone.utc)
        year_start = f"{now.year}-01-01"

        with get_db() as conn:
            order_rev = conn.execute("""
                SELECT COUNT(*) as count,
                       SUM(CASE WHEN status IN ('paid','invoiced','delivered') THEN total ELSE 0 END) as revenue,
                       SUM(CASE WHEN status = 'paid' THEN COALESCE(payment_amount, total) ELSE 0 END) as collected,
                       SUM(CASE WHEN status = 'invoiced' AND invoice_date < ? THEN total ELSE 0 END) as overdue
                FROM orders
                WHERE created_at >= ?
            """, ((now - timedelta(days=30)).isoformat(), year_start)).fetchone()

            logged_rev = conn.execute("""
                SELECT SUM(amount) as total FROM revenue_log
                WHERE logged_at >= ?
            """, (year_start,)).fetchone()

            monthly = conn.execute("""
                SELECT strftime('%Y-%m', created_at) as month,
                       COUNT(*) as orders,
                       SUM(total) as revenue
                FROM orders
                WHERE created_at >= ? AND status IN ('paid','invoiced','delivered','shipped')
                GROUP BY month ORDER BY month
            """, (year_start,)).fetchall()

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
        log.error("get_revenue_ytd: %s", e)
        return {"ok": False, "error": str(e)}


def transition_order(order_id: str, new_status: str, actor: str = "system",
                     notes: str = "", **kwargs) -> dict:
    """Move an order to a new status with audit logging. V2 replacement for order_lifecycle.transition_order."""
    # Map legacy statuses to V2
    new_status = LEGACY_STATUS_MAP.get(new_status, new_status)
    if new_status not in ORDER_STATUSES:
        return {"ok": False, "error": f"Invalid status: {new_status}"}

    from src.core.db import get_db
    try:
        with get_db() as conn:
            row = conn.execute("SELECT status, status_history FROM orders WHERE id=?", (order_id,)).fetchone()
            if not row:
                return {"ok": False, "error": f"Order {order_id} not found"}

            old_status = row["status"] or "new"
            old_status = LEGACY_STATUS_MAP.get(old_status, old_status)

            history = _safe_json(row.get("status_history"), [])
            history.append({
                "from": old_status, "to": new_status,
                "at": _now_iso(), "actor": actor, "notes": notes,
            })

            updates = {"status": new_status, "updated_at": _now_iso(),
                        "status_history": json.dumps(history)}

            # Set milestone fields
            if new_status == "shipped" and kwargs.get("tracking_number"):
                updates["tracking_number"] = kwargs["tracking_number"]
            if new_status == "invoiced" and kwargs.get("invoice_number"):
                updates["invoice_number"] = kwargs["invoice_number"]

            for k, v in kwargs.items():
                if k in ("buyer_name", "buyer_email", "notes", "tracking_number",
                          "ship_date", "delivery_date", "invoice_date",
                          "payment_date", "payment_amount", "vendor_name"):
                    updates[k] = v

            set_clause = ", ".join(f"{k}=?" for k in updates)
            values = list(updates.values()) + [order_id]
            conn.execute("UPDATE orders SET " + set_clause + " WHERE id=?", values)

            conn.execute("""
                INSERT INTO order_audit_log
                (order_id, action, field, old_value, new_value, actor, details, created_at)
                VALUES (?, 'status_transition', 'status', ?, ?, ?, ?, ?)
            """, (order_id, old_status, new_status, actor, notes, _now_iso()))

            log.info("transition_order(%s): %s → %s by %s", order_id, old_status, new_status, actor)
            return {"ok": True, "order_id": order_id, "old_status": old_status,
                    "new_status": new_status, "transition_count": len(history)}
    except Exception as e:
        log.error("transition_order(%s) failed: %s", order_id, e, exc_info=True)
        return {"ok": False, "error": str(e)}


def get_order_detail(order_id: str) -> dict:
    """Full order detail with lifecycle timeline. V2 replacement for order_lifecycle.get_order_detail."""
    order = get_order(order_id)
    if not order:
        return {"ok": False, "error": "Not found"}
    order["status_label"] = STATUS_LABELS.get(order.get("status", ""), order.get("status", ""))
    order["status_history"] = _safe_json(order.get("status_history"), [])
    return {"ok": True, "order": order}


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def _item_status(item: dict) -> str:
    """Get sourcing status from either normalized row or blob dict."""
    return item.get("sourcing_status", "pending")


def _row_to_line_item(row) -> dict:
    """Convert a SQLite Row from order_line_items to a dict matching the legacy line_item shape."""
    d = dict(row)
    # Map normalized column names to legacy field names for template compat
    d["line_id"] = f"L{d.get('line_number', 0):03d}"
    d["db_id"] = d.get("id")  # preserve the actual DB id
    d["qty"] = d.get("qty_ordered", 0)
    d["extended"] = d.get("extended_price", 0)
    d["supplier"] = d.get("supplier_name", "")
    d["cost"] = d.get("unit_cost", 0)
    return d


def _find_line_item(conn, order_id: str, line_id) -> dict | None:
    """Find a line item by ID (int db id) or legacy line_id string (L001)."""
    # Try as integer DB id first
    try:
        db_id = int(line_id)
        row = conn.execute(
            "SELECT * FROM order_line_items WHERE id=? AND order_id=?",
            (db_id, order_id)
        ).fetchone()
        if row:
            return dict(row)
    except (ValueError, TypeError):
        pass

    # Try as legacy line_id string (L001 → line_number 1)
    if isinstance(line_id, str) and line_id.startswith("L"):
        try:
            ln = int(line_id[1:])
            row = conn.execute(
                "SELECT * FROM order_line_items WHERE order_id=? AND line_number=?",
                (order_id, ln)
            ).fetchone()
            if row:
                return dict(row)
        except (ValueError, TypeError):
            pass

    # Last resort: try as string match on line_number
    row = conn.execute(
        "SELECT * FROM order_line_items WHERE order_id=? ORDER BY line_number LIMIT 1",
        (order_id,)
    ).fetchone()
    return dict(row) if row else None


def _sync_blob(conn, order_id: str):
    """Re-sync data_json blob from normalized line items. Backward compat during transition."""
    try:
        order_row = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        if not order_row:
            return
        order = dict(order_row)

        line_rows = conn.execute(
            "SELECT * FROM order_line_items WHERE order_id=? ORDER BY line_number",
            (order_id,)
        ).fetchall()

        if line_rows:
            items = [_row_to_line_item(lr) for lr in line_rows]
        else:
            items = _safe_json(order.get("items"), [])

        # Merge existing blob data with updated items
        existing_blob = {}
        if order.get("data_json"):
            try:
                existing_blob = json.loads(order["data_json"])
            except (json.JSONDecodeError, TypeError):
                pass

        existing_blob["line_items"] = items
        existing_blob["status"] = order.get("status", "new")
        existing_blob["total"] = order.get("total", 0)
        existing_blob["updated_at"] = _now_iso()
        existing_blob["order_id"] = order_id

        conn.execute(
            "UPDATE orders SET data_json=?, items=?, updated_at=? WHERE id=?",
            (json.dumps(existing_blob, default=str),
             json.dumps(items, default=str),
             _now_iso(), order_id)
        )
    except Exception as e:
        log.debug("_sync_blob(%s): %s", order_id, e)
