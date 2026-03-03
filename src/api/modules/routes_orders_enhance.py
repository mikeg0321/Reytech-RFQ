"""
routes_orders_enhance.py — Order Enhancement Features (Sprint)

F1: Order Timeline (visual activity log)
F2: Margin Tracker (cost/sell/margin per line)
F3: Smart Filters + Search
F4: Auto-Tracking Import (enhanced)
F5: Aging Badges
F6: Email Thread Panel
F7: Structured Audit Log
F8: One-Click Reorder
F9: Delivery Photo/Proof upload
F10: Order KPI Dashboard
"""

from flask import request, jsonify, redirect, flash
from src.api.shared import bp, auth_required
from src.api.render import render_page
import logging
import os
import json
import re
from datetime import datetime, timedelta

log = logging.getLogger("reytech")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

ORDERS_FILE = os.path.join(DATA_DIR, "orders.json")
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")


def _load_orders():
    try:
        with open(ORDERS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_orders(orders):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(ORDERS_FILE, "w") as f:
        json.dump(orders, f, indent=2, default=str)


@bp.route("/api/order/<oid>/delivery-update", methods=["POST"])
@auth_required
def api_order_delivery_update(oid):
    """Send delivery status update email for selected line items.
    Reply-all to the original PO sender group."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})

    data = request.get_json(silent=True) or {}
    selected_items = data.get("items", [])
    note = data.get("note", "")

    if not selected_items:
        return jsonify({"ok": False, "error": "No items selected"})

    po = order.get("po_number", "")
    institution = order.get("institution", "")
    agency = order.get("agency", "")

    # Build item status table for email
    lines = []
    for sel in selected_items:
        lid = sel.get("line_id", "")
        for it in order.get("line_items", []):
            if it.get("line_id") == lid:
                status_labels = {"pending": "Pending", "ordered": "Ordered",
                                 "shipped": "Shipped", "delivered": "Delivered"}
                s = status_labels.get(it.get("sourcing_status", "pending"), "Pending")
                tracking = it.get("tracking_number", "")
                carrier = it.get("carrier", "")
                desc = it.get("description", "")[:60]
                qty = it.get("qty", 0)
                track_str = f" — {carrier} {tracking}" if tracking else ""
                lines.append(f"• {desc} (Qty: {qty}) — {s}{track_str}")
                break

    subject = f"Delivery Update: PO {po} — {institution}"

    lines_text = "\n".join(lines)
    body = f"""Hello,

Please see below for the latest delivery status update on PO {po} for {institution}:

{lines_text}
"""
    if note:
        body += f"\nNote: {note}\n"

    body += f"""
If you have any questions about this order, please don't hesitate to reach out.

Best regards,
Mike Gonzales
Reytech Inc.
(949) 229-1575
mike@reytechinc.com
"""

    # Find original PO sender emails for reply-all
    recipients = []
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT DISTINCT sender FROM processed_emails
                WHERE (subject LIKE ? OR body LIKE ?) AND sender != ''
                LIMIT 10
            """, (f"%{po}%", f"%{po}%")).fetchall()
            recipients = [r["sender"] for r in rows if r["sender"]]
    except Exception:
        pass

    # Also check order metadata for sender
    order_sender = order.get("sender_email", "") or order.get("requestor_email", "")
    if order_sender and order_sender not in recipients:
        recipients.insert(0, order_sender)

    if not recipients:
        recipients = ["(no recipient found — add manually)"]

    # Try to send via Gmail
    draft_url = ""
    try:
        import urllib.parse
        gmail_to = ",".join(recipients)
        params = urllib.parse.urlencode({
            "to": gmail_to,
            "su": subject,
            "body": body,
        })
        draft_url = f"https://mail.google.com/mail/?view=cm&{params}"
    except Exception:
        pass

    log_order_event(oid, "delivery_update_sent", "email", "",
                    f"{len(selected_items)} items",
                    "user", f"Recipients: {', '.join(recipients[:3])}")

    return jsonify({
        "ok": True,
        "recipients": recipients,
        "items_count": len(selected_items),
        "subject": subject,
        "draft_url": draft_url,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Supplier Record Page
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/supplier/<name>")
@auth_required
def supplier_record_page(n):
    """Supplier record page — shows all orders, items, and activity for a supplier."""
    import urllib.parse
    supplier_name = urllib.parse.unquote_plus(n)
    orders = _load_orders()

    # Find all line items for this supplier across all orders
    supplier_items = []
    order_ids = set()
    total_spend = 0
    for oid, order in orders.items():
        for it in order.get("line_items", []):
            s = (it.get("supplier", "") or "").strip()
            if s.lower() == supplier_name.lower():
                supplier_items.append({
                    "order_id": oid,
                    "line_id": it.get("line_id", ""),
                    "description": it.get("description", "")[:80],
                    "part_number": it.get("part_number", ""),
                    "qty": it.get("qty", 0),
                    "unit_price": it.get("unit_price", 0),
                    "sourcing_status": it.get("sourcing_status", "pending"),
                    "tracking": it.get("tracking_number", ""),
                    "carrier": it.get("carrier", ""),
                    "supplier_url": it.get("supplier_url", ""),
                    "po_number": order.get("po_number", ""),
                    "institution": order.get("institution", ""),
                })
                order_ids.add(oid)
                total_spend += (it.get("unit_price", 0) or 0) * (it.get("qty", 0) or 0)

    # Build HTML rows
    rows = ""
    for si in supplier_items:
        ss = si["sourcing_status"]
        s_colors = {"pending": "var(--tx2)", "ordered": "#58a6ff", "shipped": "#bc8cff", "delivered": "#3fb950"}
        clr = s_colors.get(ss, "var(--tx2)")
        track = f'{si["carrier"]} {si["tracking"]}' if si["tracking"] else "—"
        rows += f"""<tr>
         <td><a href="/order/{si['order_id']}" style="color:var(--ac);font-size:13px">{si['order_id']}</a></td>
         <td style="font-size:13px">{si['institution']}</td>
         <td style="font-size:14px">{si['description']}</td>
         <td class="mono" style="font-size:13px">{si['part_number']}</td>
         <td class="mono" style="text-align:center;font-size:14px">{si['qty']}</td>
         <td class="mono" style="text-align:right;font-size:14px">${si['unit_price']:,.2f}</td>
         <td style="color:{clr};font-size:13px;font-weight:600">{ss.title()}</td>
         <td style="font-size:13px">{track}</td>
        </tr>"""

    return render_page("supplier_record.html", active_page="Orders",
        supplier_name=supplier_name, items=supplier_items,
        rows=rows, order_count=len(order_ids),
        item_count=len(supplier_items),
        total_spend=total_spend)


# ═══════════════════════════════════════════════════════════════════════════════
# F7: Structured Audit Log
# ═══════════════════════════════════════════════════════════════════════════════

def log_order_event(order_id: str, action: str, field: str = "",
                    old_value: str = "", new_value: str = "",
                    actor: str = "system", details: str = ""):
    """Log any order event to SQLite audit log. Non-blocking."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT INTO order_audit_log
                (order_id, action, field, old_value, new_value, actor, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (order_id, action, field,
                  str(old_value)[:500] if old_value else "",
                  str(new_value)[:500] if new_value else "",
                  actor, details[:1000] if details else "",
                  datetime.now().isoformat()))
    except Exception as e:
        log.debug("Audit log: %s", e)


@bp.route("/api/order/<oid>/log")
@auth_required
def api_order_log(oid):
    """Get full audit log for an order."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT id, action, field, old_value, new_value, actor, details, created_at
                FROM order_audit_log WHERE order_id = ?
                ORDER BY created_at DESC LIMIT 200
            """, (oid,)).fetchall()
            return jsonify({"ok": True, "log": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# F1: Order Timeline API (used by detail page)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/order/<oid>/timeline")
@auth_required
def api_order_timeline(oid):
    """Build merged timeline: audit log + status history + emails."""
    events = []

    # 1. Audit log entries
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT action, field, old_value, new_value, actor, details, created_at
                FROM order_audit_log WHERE order_id = ?
                ORDER BY created_at ASC
            """, (oid,)).fetchall()
            for r in rows:
                events.append({
                    "type": "audit",
                    "action": r["action"],
                    "field": r["field"] or "",
                    "old": r["old_value"] or "",
                    "new": r["new_value"] or "",
                    "actor": r["actor"] or "system",
                    "details": r["details"] or "",
                    "ts": r["created_at"],
                })
    except Exception:
        pass

    # 2. Status history from order JSON
    orders = _load_orders()
    order = orders.get(oid, {})
    for h in order.get("status_history", []):
        events.append({
            "type": "status",
            "action": "status_change",
            "field": "status",
            "old": h.get("from", ""),
            "new": h.get("status", h.get("to", "")),
            "actor": h.get("actor", "system"),
            "details": "",
            "ts": h.get("timestamp", h.get("at", "")),
        })

    # 3. Related emails
    try:
        from src.core.db import get_db
        po = order.get("po_number", "")
        qn = order.get("quote_number", "")
        with get_db() as conn:
            email_rows = conn.execute("""
                SELECT subject, sender, received_at, classification, id
                FROM processed_emails
                WHERE (subject LIKE ? OR subject LIKE ? OR body LIKE ? OR body LIKE ?)
                ORDER BY received_at ASC LIMIT 50
            """, (f"%{po}%", f"%{qn}%", f"%{po}%", f"%{qn}%")).fetchall()
            for r in email_rows:
                events.append({
                    "type": "email",
                    "action": r["classification"] or "email",
                    "field": "",
                    "old": "",
                    "new": r["subject"] or "",
                    "actor": r["sender"] or "",
                    "details": "",
                    "ts": r["received_at"] or "",
                })
    except Exception:
        pass

    # Sort all events chronologically
    events.sort(key=lambda e: e.get("ts", ""))

    return jsonify({"ok": True, "events": events, "count": len(events)})


# ═══════════════════════════════════════════════════════════════════════════════
# F2: Margin Tracker
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/order/<oid>/margins")
@auth_required
def api_order_line_margins(oid):
    """Calculate margins for all line items in an order."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Not found"})

    items = order.get("line_items", [])
    total_revenue = 0
    total_cost = 0
    line_margins = []

    for it in items:
        qty = it.get("qty", 0) or 0
        sell = it.get("unit_price", 0) or 0
        cost = it.get("cost", 0) or 0
        revenue = qty * sell
        cost_total = qty * cost
        margin = revenue - cost_total
        margin_pct = round((margin / revenue * 100), 1) if revenue > 0 else 0

        total_revenue += revenue
        total_cost += cost_total

        line_margins.append({
            "line_id": it.get("line_id", ""),
            "description": (it.get("description", "") or "")[:60],
            "qty": qty,
            "sell_price": sell,
            "cost_price": cost,
            "revenue": round(revenue, 2),
            "cost_total": round(cost_total, 2),
            "margin": round(margin, 2),
            "margin_pct": margin_pct,
            "has_cost": cost > 0,
            "alert": margin_pct < 15 and cost > 0,  # Alert if margin < 15%
        })

    total_margin = total_revenue - total_cost
    total_margin_pct = round((total_margin / total_revenue * 100), 1) if total_revenue > 0 else 0
    items_with_cost = sum(1 for m in line_margins if m["has_cost"])

    return jsonify({
        "ok": True,
        "order_id": oid,
        "total_revenue": round(total_revenue, 2),
        "total_cost": round(total_cost, 2),
        "total_margin": round(total_margin, 2),
        "total_margin_pct": total_margin_pct,
        "items_with_cost": items_with_cost,
        "items_total": len(items),
        "low_margin_alerts": sum(1 for m in line_margins if m["alert"]),
        "lines": line_margins,
    })


@bp.route("/api/order/<oid>/line/<lid>/cost", methods=["POST"])
@auth_required
def api_order_line_cost(oid, lid):
    """Update cost for a single line item."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Not found"})

    data = request.get_json(silent=True) or {}
    cost = data.get("cost", 0)

    for it in order.get("line_items", []):
        if it.get("line_id") == lid:
            old_cost = it.get("cost", 0)
            it["cost"] = float(cost)
            sell = it.get("unit_price", 0) or 0
            it["margin_pct"] = round(((sell - float(cost)) / sell * 100), 1) if sell > 0 else 0

            log_order_event(oid, "cost_updated", "cost",
                            f"${old_cost:.2f}", f"${float(cost):.2f}",
                            "user", f"Line {lid}: {it.get('description', '')[:40]}")

            orders[oid] = order
            _save_orders(orders)
            return jsonify({"ok": True, "margin_pct": it["margin_pct"]})

    return jsonify({"ok": False, "error": "Line not found"})


# ═══════════════════════════════════════════════════════════════════════════════
# F5: Aging Calculations
# ═══════════════════════════════════════════════════════════════════════════════

def calc_order_aging(order: dict) -> dict:
    """Calculate aging metrics for an order."""
    now = datetime.now()
    created = order.get("created_at", "")
    updated = order.get("updated_at", "")
    status = order.get("status", "new")

    try:
        created_dt = datetime.fromisoformat(created) if created else now
    except (ValueError, TypeError):
        created_dt = now
    try:
        updated_dt = datetime.fromisoformat(updated or created) if (updated or created) else now
    except (ValueError, TypeError):
        updated_dt = now

    age_days = (now - created_dt).days
    stale_days = (now - updated_dt).days

    # Severity based on status + age
    if status in ("closed", "invoiced"):
        severity = "ok"
        badge = "🟢"
    elif status == "delivered" and stale_days >= 3:
        severity = "warning"  # Delivered but no invoice
        badge = "🟡"
    elif status == "new" and age_days >= 3:
        severity = "critical"
        badge = "🔴"
    elif stale_days >= 5 and status not in ("closed", "invoiced"):
        severity = "warning"
        badge = "🟡"
    elif stale_days >= 10:
        severity = "critical"
        badge = "🔴"
    else:
        severity = "ok"
        badge = "🟢"

    return {
        "age_days": age_days,
        "stale_days": stale_days,
        "severity": severity,
        "badge": badge,
        "created_at": created,
        "updated_at": updated,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# F6: Email Thread Panel
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/order/<oid>/emails")
@auth_required
def api_order_emails(oid):
    """Get all emails related to this order (by PO#, quote#, or institution)."""
    orders = _load_orders()
    order = orders.get(oid, {})
    po = order.get("po_number", "")
    qn = order.get("quote_number", "")
    sender = order.get("sender_email", "")

    emails = []
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Search by PO, quote number, or sender
            conditions = []
            params = []
            if po:
                conditions.append("(subject LIKE ? OR body LIKE ?)")
                params.extend([f"%{po}%", f"%{po}%"])
            if qn:
                conditions.append("(subject LIKE ? OR body LIKE ?)")
                params.extend([f"%{qn}%", f"%{qn}%"])
            if sender:
                conditions.append("sender LIKE ?")
                params.append(f"%{sender}%")

            if not conditions:
                return jsonify({"ok": True, "emails": [], "count": 0})

            where = " OR ".join(conditions)
            rows = conn.execute(f"""
                SELECT id, subject, sender, received_at, classification,
                       substr(body, 1, 200) as preview
                FROM processed_emails
                WHERE {where}
                ORDER BY received_at DESC LIMIT 30
            """, params).fetchall()
            emails = [dict(r) for r in rows]
    except Exception as e:
        log.debug("Email thread: %s", e)

    return jsonify({"ok": True, "emails": emails, "count": len(emails)})


# ═══════════════════════════════════════════════════════════════════════════════
# F8: One-Click Reorder (Clone)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/order/<oid>/clone", methods=["POST"])
@auth_required
def api_order_clone(oid):
    """Clone an existing order with new PO number and fresh dates."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})

    data = request.get_json(silent=True) or {}
    new_po = data.get("po_number", "")
    if not new_po:
        return jsonify({"ok": False, "error": "PO number required"})

    now = datetime.now().isoformat()
    new_oid = f"ORD-PO-{new_po}"

    if new_oid in orders:
        return jsonify({"ok": False, "error": f"Order {new_oid} already exists"})

    # Clone line items — reset statuses
    new_items = []
    for it in order.get("line_items", []):
        new_it = dict(it)
        new_it["sourcing_status"] = "pending"
        new_it["tracking_number"] = ""
        new_it["carrier"] = ""
        new_it["ship_date"] = ""
        new_it["delivery_date"] = ""
        new_it["invoice_status"] = "pending"
        new_it["invoice_number"] = ""
        new_items.append(new_it)

    new_order = {
        "order_id": new_oid,
        "quote_number": "",
        "po_number": new_po,
        "agency": order.get("agency", ""),
        "institution": order.get("institution", ""),
        "ship_to_name": order.get("ship_to_name", ""),
        "ship_to_address": order.get("ship_to_address", []),
        "total": order.get("total", 0),
        "subtotal": order.get("subtotal", 0),
        "tax": order.get("tax", 0),
        "payment_terms": order.get("payment_terms", "Net 45"),
        "line_items": new_items,
        "status": "new",
        "source": "reorder",
        "cloned_from": oid,
        "created_at": now,
        "updated_at": now,
        "status_history": [{"status": "new", "timestamp": now, "actor": "user"}],
    }

    orders[new_oid] = new_order
    _save_orders(orders)

    log_order_event(new_oid, "order_created", "", "", "",
                    "user", f"Cloned from {oid} with new PO #{new_po}")
    log_order_event(oid, "order_cloned", "", "", new_oid,
                    "user", f"Cloned to {new_oid} PO #{new_po}")

    return jsonify({"ok": True, "order_id": new_oid, "items": len(new_items)})


# ═══════════════════════════════════════════════════════════════════════════════
# F9: Delivery Proof Upload
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/order/<oid>/upload-proof", methods=["POST"])
@auth_required
def api_order_upload_proof(oid):
    """Upload delivery proof (photo, BOL PDF) for an order or line item."""
    orders = _load_orders()
    if oid not in orders:
        return jsonify({"ok": False, "error": "Order not found"})

    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded"})

    f = request.files["file"]
    if not f.filename:
        return jsonify({"ok": False, "error": "Empty filename"})

    line_id = request.form.get("line_id", "")
    file_type = request.form.get("type", "delivery_proof")

    # Save file
    proof_dir = os.path.join(UPLOAD_DIR, "proofs", oid)
    os.makedirs(proof_dir, exist_ok=True)
    safe_name = re.sub(r'[^\w\-.]', '_', f.filename)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{ts}_{safe_name}"
    filepath = os.path.join(proof_dir, filename)
    f.save(filepath)

    # Log to DB
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT INTO order_attachments
                (order_id, line_id, file_type, file_name, file_path, uploaded_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (oid, line_id, file_type, safe_name, filepath, "user", datetime.now().isoformat()))
    except Exception as e:
        log.debug("Attachment DB: %s", e)

    log_order_event(oid, "proof_uploaded", "attachment", "", filename,
                    "user", f"Delivery proof: {safe_name}" + (f" for line {line_id}" if line_id else ""))

    return jsonify({"ok": True, "filename": filename, "path": filepath})


@bp.route("/api/order/<oid>/attachments")
@auth_required
def api_order_attachments(oid):
    """List all attachments for an order."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT id, line_id, file_type, file_name, file_path, uploaded_by, created_at
                FROM order_attachments WHERE order_id = ?
                ORDER BY created_at DESC
            """, (oid,)).fetchall()
            return jsonify({"ok": True, "attachments": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"ok": True, "attachments": []})


# ═══════════════════════════════════════════════════════════════════════════════
# F10: Order KPI Dashboard
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/orders/kpi")
@auth_required
def api_orders_kpi():
    """Order KPI metrics: monthly trends, avg fulfillment, top agencies, margins."""
    orders = _load_orders()
    now = datetime.now()

    # Monthly breakdown
    monthly = {}
    agency_totals = {}
    fulfillment_times = []
    total_revenue = 0
    total_cost = 0
    has_cost_data = False
    status_counts = {}

    for oid, order in orders.items():
        status = order.get("status", "new")
        if status in ("cancelled", "test", "deleted"):
            continue
        # Skip test orders
        if "TEST" in (order.get("po_number", "") or "").upper() or order.get("is_test"):
            continue

        status_counts[status] = status_counts.get(status, 0) + 1
        total = order.get("total", 0)
        total_revenue += total

        # Monthly
        created = order.get("created_at", "")[:7]  # YYYY-MM
        if created:
            if created not in monthly:
                monthly[created] = {"month": created, "orders": 0, "value": 0}
            monthly[created]["orders"] += 1
            monthly[created]["value"] += total

        # Agency
        agency = order.get("agency", "") or "Unknown"
        if agency not in agency_totals:
            agency_totals[agency] = {"agency": agency, "orders": 0, "value": 0}
        agency_totals[agency]["orders"] += 1
        agency_totals[agency]["value"] += total

        # Fulfillment time (created → delivered)
        delivered_at = order.get("delivered_at", "")
        if delivered_at and created:
            try:
                c = datetime.fromisoformat(order["created_at"])
                d = datetime.fromisoformat(delivered_at)
                fulfillment_times.append((d - c).days)
            except (ValueError, TypeError):
                pass

        # Line-item costs for margin — only count if real cost data exists
        for it in order.get("line_items", []):
            cost = it.get("cost", 0) or 0
            qty = it.get("qty", 0) or 0
            if cost > 0:
                has_cost_data = True
            total_cost += cost * qty

    avg_fulfillment = round(sum(fulfillment_times) / len(fulfillment_times), 1) if fulfillment_times else None

    # Sort monthly
    monthly_sorted = sorted(monthly.values(), key=lambda m: m["month"])

    # Top agencies
    top_agencies = sorted(agency_totals.values(), key=lambda a: a["value"], reverse=True)[:10]

    # Margin: only calculate if we have actual cost data from line items or QB
    # If no cost data, return null so UI shows "—" instead of misleading 100%
    if has_cost_data and total_cost > 0:
        total_margin = total_revenue - total_cost
        margin_pct = round((total_margin / total_revenue * 100), 1) if total_revenue > 0 else 0
    else:
        total_margin = None
        margin_pct = None

    return jsonify({
        "ok": True,
        "total_orders": len([o for o in orders.values()
                            if o.get("status") not in ("cancelled", "test", "deleted")
                            and "TEST" not in (o.get("po_number", "") or "").upper()
                            and not o.get("is_test")]),
        "total_revenue": round(total_revenue, 2),
        "total_cost": round(total_cost, 2) if has_cost_data else None,
        "total_margin": round(total_margin, 2) if total_margin is not None else None,
        "margin_pct": margin_pct,
        "has_cost_data": has_cost_data,
        "avg_fulfillment_days": avg_fulfillment,
        "status_counts": status_counts,
        "monthly": monthly_sorted,
        "top_agencies": top_agencies,
    })
