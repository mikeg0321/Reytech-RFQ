"""
order_digest.py — Daily Order Digest + Shipping Tracker

1. Daily digest: surfaces items not yet ordered, orders delivered but not invoiced
2. Tracking scanner: detects shipping/tracking info from vendor emails
3. Feeds CS agent with live order context for draft responses

Runs on a background thread, checks every 4 hours.
Daily digest fires once per day around 8am PST.
"""

import os
import json
import re
import logging
import threading
import time
from datetime import datetime, timedelta

log = logging.getLogger("reytech.order_digest")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

DIGEST_STATE_FILE = os.path.join(DATA_DIR, "order_digest_state.json")


# ═══════════════════════════════════════════════════════════════════════════════
# Order Digest — surfaces action items
# ═══════════════════════════════════════════════════════════════════════════════

def _load_orders() -> dict:
    """Load orders — delegates to order_dal (V2)."""
    try:
        from src.core.order_dal import load_orders_dict
        return load_orders_dict()
    except Exception as e:
        log.warning("_load_orders via order_dal failed: %s", e)
        return {}


def get_order_health() -> dict:
    """Full order health report. Used by home dashboard AND CS agent."""
    orders = _load_orders()
    if not orders:
        return {"ok": True, "total": 0, "issues": [], "summary": {}}

    now = datetime.now()
    issues = []
    summary = {
        "total_orders": 0,
        "total_value": 0,
        "new_unactioned": [],        # POs received, nothing ordered yet
        "items_not_ordered": [],     # Individual line items still pending
        "items_ordered_no_tracking": [],  # Ordered but no tracking
        "items_shipped": [],         # In transit
        "items_delivered": [],       # Delivered
        "orders_delivered_no_invoice": [],  # All items delivered, no invoice
        "orders_invoiced_unpaid": [],  # Invoiced but not marked paid
        "orders_stale": [],          # No status change in 5+ days
    }

    for oid, order in orders.items():
        status = order.get("status", "new")
        if status in ("cancelled", "test", "deleted"):
            continue
        # Skip test orders
        if "TEST" in (order.get("po_number", "") or "").upper():
            continue
        if order.get("is_test"):
            continue

        summary["total_orders"] += 1
        total = order.get("total", 0)
        summary["total_value"] += total
        inst = order.get("institution", "")
        po = order.get("po_number", "")
        items = order.get("line_items", [])
        updated = order.get("updated_at", "")
        created = order.get("created_at", "")

        # Days since last update
        try:
            last_touch = datetime.fromisoformat(updated or created or "2025-01-01")
            days_stale = (now - last_touch).days
        except (ValueError, TypeError):
            days_stale = 99

        # New / unactioned orders (no items ordered yet)
        if status == "new":
            pending_items = [it for it in items if it.get("sourcing_status") == "pending"]
            if pending_items:
                summary["new_unactioned"].append({
                    "order_id": oid, "po": po, "institution": inst,
                    "total": total, "items": len(pending_items),
                    "days_old": days_stale,
                })
                if days_stale >= 1:
                    issues.append({
                        "severity": "high" if days_stale >= 3 else "medium",
                        "type": "not_ordered",
                        "msg": f"PO #{po} ({inst}) — {len(pending_items)} items not ordered ({days_stale}d old)",
                        "order_id": oid, "link": f"/order/{oid}",
                    })

        # Pending items in active orders (sourcing/shipped/partial — still have unordered items)
        elif status not in ("closed", "delivered", "invoiced"):
            pending_items = [it for it in items if it.get("sourcing_status") == "pending"]
            if pending_items:
                issues.append({
                    "severity": "high" if days_stale >= 3 else "medium",
                    "type": "not_ordered",
                    "msg": f"PO #{po} ({inst}) — {len(pending_items)}/{len(items)} items still not ordered ({status})",
                    "order_id": oid, "link": f"/order/{oid}",
                })

        # Per-item analysis
        for it in items:
            ss = it.get("sourcing_status", "pending")
            lid = it.get("line_id", "")
            desc = (it.get("description", "") or "")[:50]

            if ss == "pending":
                summary["items_not_ordered"].append({
                    "order_id": oid, "line_id": lid, "desc": desc,
                    "qty": it.get("qty", 0), "po": po, "institution": inst,
                })
            elif ss == "ordered":
                if not it.get("tracking_number"):
                    summary["items_ordered_no_tracking"].append({
                        "order_id": oid, "line_id": lid, "desc": desc,
                        "po": po, "institution": inst,
                    })
            elif ss == "shipped":
                summary["items_shipped"].append({
                    "order_id": oid, "line_id": lid, "desc": desc,
                    "tracking": it.get("tracking_number", ""), "carrier": it.get("carrier", ""),
                    "po": po, "institution": inst,
                })
            elif ss == "delivered":
                summary["items_delivered"].append({
                    "order_id": oid, "line_id": lid, "desc": desc,
                    "po": po, "institution": inst,
                })

        # Issues for items ordered but no tracking (across all active orders)
        no_track_items = [it for it in items if it.get("sourcing_status") == "ordered" and not it.get("tracking_number")]
        if no_track_items and days_stale >= 2:
            issues.append({
                "severity": "medium",
                "type": "no_tracking",
                "msg": f"PO #{po} ({inst}) — {len(no_track_items)} items ordered, no tracking yet",
                "order_id": oid, "link": f"/order/{oid}",
            })

        # Issues for shipped but not delivered after 7+ days
        shipped_items = [it for it in items if it.get("sourcing_status") == "shipped"]
        if shipped_items and days_stale >= 7:
            issues.append({
                "severity": "medium",
                "type": "shipping_delay",
                "msg": f"PO #{po} ({inst}) — {len(shipped_items)} items shipped {days_stale}d ago, not delivered",
                "order_id": oid, "link": f"/order/{oid}",
            })

        # Delivered but no invoice
        if status == "delivered" and not order.get("draft_invoice"):
            summary["orders_delivered_no_invoice"].append({
                "order_id": oid, "po": po, "institution": inst, "total": total,
                "days_since_delivery": days_stale,
            })
            issues.append({
                "severity": "high",
                "type": "no_invoice",
                "msg": f"PO #{po} ({inst}) — ${total:,.0f} delivered, no invoice created",
                "order_id": oid, "link": f"/order/{oid}",
            })

        # Stale orders (no updates in 5+ days, not closed)
        if status not in ("closed", "invoiced") and days_stale >= 5:
            summary["orders_stale"].append({
                "order_id": oid, "po": po, "institution": inst,
                "status": status, "days_stale": days_stale,
            })
            if status not in ("delivered",):
                issues.append({
                    "severity": "medium",
                    "type": "stale",
                    "msg": f"PO #{po} ({inst}) — stuck in '{status}' for {days_stale}d",
                    "order_id": oid, "link": f"/order/{oid}",
                })

    # Sort issues by severity
    sev_rank = {"high": 0, "medium": 1, "low": 2}
    issues.sort(key=lambda x: sev_rank.get(x.get("severity", "low"), 2))

    return {"ok": True, "total": summary["total_orders"], "issues": issues, "summary": summary}


def run_daily_digest(force: bool = False):
    """Generate and send daily order digest via SMS + email."""
    # Check if we already sent today
    state = {}
    try:
        with open(DIGEST_STATE_FILE) as f:
            state = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as _e:
        log.debug("suppressed: %s", _e)

    today = datetime.now().strftime("%Y-%m-%d")
    if not force and state.get("last_digest") == today:
        return {"ok": True, "skipped": True, "reason": "already sent today"}

    health = get_order_health()
    if not health.get("ok") or health.get("total", 0) == 0:
        return {"ok": True, "skipped": True, "reason": "no orders"}

    s = health["summary"]
    issues = health.get("issues", [])

    # Build digest message
    lines = [f"📋 Daily Order Digest — {today}"]
    lines.append(f"Active orders: {s['total_orders']} · ${s['total_value']:,.0f}")

    if s["new_unactioned"]:
        lines.append(f"\n🔴 {len(s['new_unactioned'])} POs need sourcing:")
        for o in s["new_unactioned"][:5]:
            lines.append(f"  PO #{o['po']} {o['institution'][:25]} — {o['items']} items (${o['total']:,.0f})")

    if s["items_not_ordered"]:
        lines.append(f"\n⏳ {len(s['items_not_ordered'])} line items not yet ordered")

    if s["items_ordered_no_tracking"]:
        lines.append(f"\n📦 {len(s['items_ordered_no_tracking'])} items ordered, awaiting tracking")

    if s["items_shipped"]:
        lines.append(f"\n🚚 {len(s['items_shipped'])} items in transit")

    if s["orders_delivered_no_invoice"]:
        lines.append(f"\n💰 {len(s['orders_delivered_no_invoice'])} orders delivered — NEED INVOICE:")
        for o in s["orders_delivered_no_invoice"][:5]:
            lines.append(f"  PO #{o['po']} {o['institution'][:25]} — ${o['total']:,.0f}")

    if s["orders_stale"]:
        lines.append(f"\n⚠️ {len(s['orders_stale'])} stale orders (5+ days no update)")

    body = "\n".join(lines)

    alert_result = None
    try:
        from src.agents.notify_agent import send_alert
        alert_result = send_alert(
            event_type="order_digest",
            title=f"📋 Order Digest: {len(issues)} action items",
            body=body,
            urgency="warning" if any(i["severity"] == "high" for i in issues) else "info",
            cooldown_key=f"digest:{today}:{datetime.now().strftime('%H%M')}",
            run_async=False,
        )
    except Exception as e:
        log.error("Digest send failed: %s", e)
        return {"ok": False, "error": str(e)}

    # Save state
    state["last_digest"] = today
    os.makedirs(os.path.dirname(DIGEST_STATE_FILE), exist_ok=True)
    with open(DIGEST_STATE_FILE, "w") as f:
        json.dump(state, f)

    log.info("Daily digest sent: %d issues, %d orders", len(issues), s["total_orders"])
    return {"ok": True, "issues": len(issues), "sent": True, "alert_result": alert_result}


# ═══════════════════════════════════════════════════════════════════════════════
# Shipping / Tracking Email Scanner
# ═══════════════════════════════════════════════════════════════════════════════

# Patterns for extracting tracking numbers from vendor emails
TRACKING_PATTERNS = [
    # Amazon TBA
    (r'(TBA\d{10,25})', "Amazon"),
    # UPS 1Z
    (r'(1Z[A-Z0-9]{16,18})', "UPS"),
    # FedEx 12-22 digits
    (r'(?:tracking|track)[\s#:]*(\d{12,22})', "FedEx/USPS"),
    # USPS 20-22 digits
    (r'((?:94|93|92|91)\d{18,22})', "USPS"),
    # Generic "Tracking Number: XXX"
    (r'tracking\s*(?:number|#|no\.?)?\s*[:=]\s*([A-Z0-9]{8,30})', "Unknown"),
    # "has shipped" + number nearby
    (r'(?:shipped|dispatched|in transit).*?([A-Z0-9]{10,25})', "Unknown"),
]

# PO number extraction from shipping emails
PO_IN_SHIPPING = [
    r'(?:PO|P\.O\.|purchase\s*order)\s*#?\s*:?\s*(\d{7,13})',
    r'(?:PO\s*Distribution)\s*:?\s*(\d{7,13})',
    r'(?:order|ref)\s*#?\s*:?\s*([A-Z0-9\-]{5,20})',
]


def scan_email_for_tracking(subject: str, body: str, sender: str = "") -> dict:
    """
    Scan an email for shipping/tracking info and match to existing orders.
    Returns: {has_tracking, tracking_numbers, carrier, matched_orders, po_numbers}
    """
    combined = f"{subject} {body or ''}"[:5000]
    combined_lower = combined.lower()

    # Is this a shipping notification?
    shipping_keywords = ["shipped", "tracking", "in transit", "dispatched",
                         "delivery", "shipment confirmation", "out for delivery",
                         "your order has shipped", "carrier", "package"]
    is_shipping = any(kw in combined_lower for kw in shipping_keywords)
    if not is_shipping:
        return {"has_tracking": False}

    # Extract tracking numbers
    found_tracking = []
    for pattern, carrier in TRACKING_PATTERNS:
        for m in re.finditer(pattern, combined, re.IGNORECASE):
            tn = m.group(1).strip()
            if len(tn) >= 8 and tn not in [t["number"] for t in found_tracking]:
                found_tracking.append({"number": tn, "carrier": carrier})

    # Extract PO numbers
    po_numbers = []
    for pattern in PO_IN_SHIPPING:
        for m in re.finditer(pattern, combined, re.IGNORECASE):
            po = m.group(1).strip()
            if po not in po_numbers:
                po_numbers.append(po)

    # Match to existing orders
    matched = []
    if po_numbers or found_tracking:
        orders = _load_orders()
        for oid, order in orders.items():
            if order.get("status") in ("cancelled", "test", "deleted", "closed"):
                continue
            # Match by PO number
            if order.get("po_number") in po_numbers:
                matched.append(oid)
                continue
            # Match by order ID reference
            if oid in combined or oid.replace("ORD-", "") in combined:
                matched.append(oid)

    # Is this a delivery confirmation?
    delivery_keywords = ["delivered", "has been delivered", "delivery complete",
                         "package delivered", "your package was delivered",
                         "left at front door", "signed by", "delivery confirmed"]
    is_delivered = any(kw in combined_lower for kw in delivery_keywords)

    return {
        "has_tracking": bool(found_tracking),
        "tracking_numbers": found_tracking,
        "po_numbers": po_numbers,
        "matched_orders": matched,
        "is_shipping_email": is_shipping,
        "is_delivery_confirmation": is_delivered,
    }


def apply_tracking_to_order(oid: str, tracking_number: str, carrier: str = ""):
    """Apply a tracking number to untracked items in an order.
    Also updates order-level status to 'shipped' or 'partial_delivery'."""
    try:
        with open(ORDERS_FILE) as f:
            orders = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"ok": False, "error": "No orders"}

    order = orders.get(oid)
    if not order:
        return {"ok": False, "error": f"Order {oid} not found"}

    # Don't re-apply same tracking number
    existing_tracking = set(it.get("tracking_number", "") for it in order.get("line_items", []))
    if tracking_number in existing_tracking:
        return {"ok": True, "applied_to": [], "skipped": "already_applied"}

    applied_to = []
    for it in order.get("line_items", []):
        ss = it.get("sourcing_status", "pending")
        it_tracking = it.get("tracking_number", "")

        # Apply to items that are ordered/pending without tracking
        if ss in ("ordered", "pending") and not it_tracking:
            it["tracking_number"] = tracking_number
            it["carrier"] = carrier or it.get("carrier", "")
            it["sourcing_status"] = "shipped"
            it["ship_date"] = datetime.now().strftime("%Y-%m-%d")
            applied_to.append(it.get("line_id", ""))

    if applied_to:
        # Update order-level status
        all_items = order.get("line_items", [])
        all_shipped = all(i.get("sourcing_status") in ("shipped", "delivered") for i in all_items)
        any_shipped = any(i.get("sourcing_status") in ("shipped", "delivered") for i in all_items)
        if all_shipped:
            order["status"] = "shipped"
        elif any_shipped:
            order["status"] = "shipped"  # at least one shipped

        order["updated_at"] = datetime.now().isoformat()
        orders[oid] = order
        # Save via order_dal (V2) — no longer writes to JSON file
        try:
            from src.core.order_dal import save_order, save_line_items_batch
            save_order(oid, order, actor="tracking_auto")
            save_line_items_batch(oid, order.get("line_items", []))
        except Exception as _save_err:
            log.error("apply_tracking save failed: %s", _save_err)

        log.info("Applied tracking %s (%s) to order %s: %d items",
                 tracking_number, carrier, oid, len(applied_to))

        try:
            from src.agents.notify_agent import send_alert
            inst = order.get("institution", "")
            po = order.get("po_number", "")
            send_alert(
                event_type="line_shipped",
                title=f"🚚 Tracking auto-applied: {po or oid}",
                body=f"PO #{po} ({inst})\nTracking: {tracking_number} ({carrier})\n"
                     f"Applied to {len(applied_to)} item(s)",
                urgency="info",
                cooldown_key=f"track_apply:{oid}:{tracking_number}",
            )
        except Exception as _e:
            log.debug("suppressed: %s", _e)

    return {"ok": True, "applied_to": applied_to}


# ═══════════════════════════════════════════════════════════════════════════════
# CS Agent Context — provides live order data for draft responses
# ═══════════════════════════════════════════════════════════════════════════════

def get_order_context_for_cs(po_number: str = "", sender_email: str = "") -> dict:
    """
    Build rich order context for CS agent to draft accurate responses.
    Called from cs_agent.build_cs_response_draft().
    """
    orders = _load_orders()
    matched = []

    for oid, order in orders.items():
        if order.get("status") in ("cancelled", "test", "deleted"):
            continue
        # Match by PO
        if po_number and order.get("po_number") == po_number:
            matched.append(order)
            continue
        # Match by sender email
        if sender_email and order.get("sender_email", "").lower() == sender_email.lower():
            matched.append(order)
            continue
        if sender_email and order.get("buyer_email", "").lower() == sender_email.lower():
            matched.append(order)

    if not matched:
        return {"found": False}

    # Build rich context for each matched order
    contexts = []
    for order in matched:
        items = order.get("line_items", [])
        pending = [it for it in items if it.get("sourcing_status") == "pending"]
        ordered = [it for it in items if it.get("sourcing_status") == "ordered"]
        shipped = [it for it in items if it.get("sourcing_status") == "shipped"]
        delivered = [it for it in items if it.get("sourcing_status") == "delivered"]

        tracking_numbers = list(set(
            it.get("tracking_number", "") for it in items if it.get("tracking_number")
        ))

        ctx = {
            "order_id": order.get("order_id", ""),
            "po_number": order.get("po_number", ""),
            "status": order.get("status", ""),
            "institution": order.get("institution", ""),
            "total": order.get("total", 0),
            "created_at": order.get("created_at", ""),
            "item_summary": {
                "total": len(items),
                "pending": len(pending),
                "ordered": len(ordered),
                "shipped": len(shipped),
                "delivered": len(delivered),
            },
            "tracking_numbers": tracking_numbers,
            "has_invoice": bool(order.get("draft_invoice")),
            "invoice_number": (order.get("draft_invoice") or {}).get("invoice_number", ""),
            # Detailed items for specific inquiries
            "items": [
                {
                    "description": it.get("description", "")[:60],
                    "qty": it.get("qty", 0),
                    "status": it.get("sourcing_status", "pending"),
                    "tracking": it.get("tracking_number", ""),
                    "carrier": it.get("carrier", ""),
                }
                for it in items
            ],
        }
        contexts.append(ctx)

    return {"found": True, "orders": contexts, "count": len(contexts)}


def format_order_context_for_cs_draft(po_number: str = "", sender_email: str = "") -> str:
    """Format order context as human-readable text for CS agent prompts."""
    ctx = get_order_context_for_cs(po_number=po_number, sender_email=sender_email)
    if not ctx.get("found"):
        return ""

    lines = []
    for order in ctx.get("orders", []):
        s = order["item_summary"]
        lines.append(f"── Order {order['order_id']} (PO #{order['po_number']}) ──")
        lines.append(f"  Status: {order['status']} | Institution: {order['institution']}")
        lines.append(f"  Total: ${order['total']:,.2f} | Created: {order['created_at'][:10]}")
        lines.append(f"  Items: {s['total']} total — {s['pending']} pending, {s['ordered']} ordered, "
                      f"{s['shipped']} shipped, {s['delivered']} delivered")
        if order["tracking_numbers"]:
            lines.append(f"  Tracking: {', '.join(order['tracking_numbers'])}")
        if order["has_invoice"]:
            lines.append(f"  Invoice: {order['invoice_number']}")
        # Item details
        for it in order.get("items", []):
            status_icon = {"pending": "⏳", "ordered": "🛒", "shipped": "🚚", "delivered": "✅"}.get(it["status"], "?")
            lines.append(f"    {status_icon} {it['description']} x{it['qty']} — {it['status']}"
                         + (f" [tracking: {it['tracking']}]" if it["tracking"] else ""))

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# Background Scheduler
# ═══════════════════════════════════════════════════════════════════════════════

_digest_started = False

def start_order_digest_scheduler(interval_hours: int = 4):
    """Start background thread that runs daily digest and tracking checks."""
    global _digest_started
    if _digest_started:
        return
    _digest_started = True

    def _loop():
        from src.core.scheduler import _shutdown_event, heartbeat
        _shutdown_event.wait(60)  # Initial delay
        while not _shutdown_event.is_set():
            try:
                now = datetime.now()
                if 7 <= now.hour <= 10:
                    run_daily_digest()
                heartbeat("order-digest", success=True)
            except Exception as e:
                log.error("Order digest scheduler error: %s", e, exc_info=True)
                heartbeat("order-digest", success=False, error=str(e)[:200])
            _shutdown_event.wait(interval_hours * 3600)
        log.info("Order digest scheduler shutting down")

    t = threading.Thread(target=_loop, daemon=True, name="order-digest")
    t.start()
    log.info("Order digest scheduler started (every %dh)", interval_hours)
