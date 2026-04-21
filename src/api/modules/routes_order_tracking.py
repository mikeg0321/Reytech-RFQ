# routes_order_tracking.py — Purchase Order Tracking via Separate Email Inbox
# Monitors a dedicated email address for PO-related communications
# Auto-evaluates line items, builds statuses, manages PO lifecycle
#
# ARCHITECTURE:
#   Separate email → parse PO# → match to existing orders → extract updates
#   → auto-update line item statuses → trigger notifications
#
# PO LIFECYCLE:
#   📋 Received → 🔄 Processing → 📦 Shipped (partial/full) → ✅ Delivered → 💰 Invoiced
#
# LINE ITEM LIFECYCLE:
#   pending → confirmed → backordered → shipped → delivered → invoiced

# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify
from src.api.shared import bp, auth_required
import os
import logging
from datetime import datetime
log = logging.getLogger("reytech")
from flask import redirect, flash
from src.api.render import render_page

import json
import re as _re
import imaplib as _imaplib
import email as _email_lib
from email.header import decode_header as _decode_header
import threading as _threading
import time as _time
import sqlite3 as _sqlite3

# ═══════════════════════════════════════════════════════════════════════════════
# DB Schema for PO Tracking
# ═══════════════════════════════════════════════════════════════════════════════

_PO_TRACKING_SCHEMA = """
CREATE TABLE IF NOT EXISTS purchase_orders (
    id              TEXT PRIMARY KEY,
    po_number       TEXT NOT NULL,
    vendor_name     TEXT,
    vendor_email    TEXT,
    buyer_name      TEXT,
    buyer_email     TEXT,
    institution     TEXT,
    order_date      TEXT,
    expected_delivery TEXT,
    actual_delivery TEXT,
    total_amount    REAL DEFAULT 0,
    status          TEXT DEFAULT 'received',
    rfq_id          TEXT,
    quote_number    TEXT,
    source_email_uid TEXT,
    notes           TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    metadata        TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_po_number ON purchase_orders(po_number);
CREATE INDEX IF NOT EXISTS idx_po_status ON purchase_orders(status);
CREATE INDEX IF NOT EXISTS idx_po_vendor ON purchase_orders(vendor_name);

CREATE TABLE IF NOT EXISTS po_line_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id           TEXT NOT NULL,
    line_number     INTEGER,
    description     TEXT,
    item_number     TEXT,
    mfg_number      TEXT,
    qty_ordered     INTEGER DEFAULT 0,
    qty_shipped     INTEGER DEFAULT 0,
    qty_received    INTEGER DEFAULT 0,
    qty_backordered INTEGER DEFAULT 0,
    unit_price      REAL DEFAULT 0,
    extended_price  REAL DEFAULT 0,
    uom             TEXT DEFAULT 'EA',
    status          TEXT DEFAULT 'pending',
    tracking_number TEXT,
    carrier         TEXT,
    ship_date       TEXT,
    delivery_date   TEXT,
    notes           TEXT,
    updated_at      TEXT,
    FOREIGN KEY (po_id) REFERENCES purchase_orders(id)
);
CREATE INDEX IF NOT EXISTS idx_poli_po ON po_line_items(po_id);
CREATE INDEX IF NOT EXISTS idx_poli_status ON po_line_items(status);

CREATE TABLE IF NOT EXISTS po_emails (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id           TEXT,
    email_uid       TEXT,
    direction       TEXT DEFAULT 'inbound',
    sender          TEXT,
    recipient       TEXT,
    subject         TEXT,
    body_preview    TEXT,
    parsed_updates  TEXT DEFAULT '{}',
    received_at     TEXT,
    processed_at    TEXT,
    FOREIGN KEY (po_id) REFERENCES purchase_orders(id)
);
CREATE INDEX IF NOT EXISTS idx_poemail_po ON po_emails(po_id);

CREATE TABLE IF NOT EXISTS po_status_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id           TEXT NOT NULL,
    line_item_id    INTEGER,
    old_status      TEXT,
    new_status      TEXT,
    changed_by      TEXT DEFAULT 'system',
    change_reason   TEXT,
    created_at      TEXT NOT NULL,
    FOREIGN KEY (po_id) REFERENCES purchase_orders(id)
);
CREATE INDEX IF NOT EXISTS idx_pohistory_po ON po_status_history(po_id);
"""

def _init_po_tracking_db():
    """Initialize PO tracking tables."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.executescript(_PO_TRACKING_SCHEMA)
            log.info("PO tracking tables initialized")
    except Exception as e:
        log.warning("Failed to init PO tracking tables: %s", e)

# Init on module load
_init_po_tracking_db()


# ═══════════════════════════════════════════════════════════════════════════════
# PO Email Poller — Separate Inbox
# ═══════════════════════════════════════════════════════════════════════════════

_PO_POLL_STATUS = {"running": False, "last_poll": None, "emails_processed": 0, "errors": []}
_PO_POLL_INTERVAL = 300  # 5 minutes

def _get_po_email_config():
    """Get PO inbox credentials. Falls back to main Gmail (POs come to same inbox)."""
    return {
        "email": os.environ.get("PO_GMAIL_ADDRESS", "") or os.environ.get("GMAIL_ADDRESS", ""),
        "password": os.environ.get("PO_GMAIL_PASSWORD", "") or os.environ.get("GMAIL_PASSWORD", ""),
        "imap_server": os.environ.get("PO_IMAP_SERVER", "imap.gmail.com"),
    }


def _poll_po_inbox():
    """Poll the PO-dedicated email inbox for order updates."""
    cfg = _get_po_email_config()
    if not cfg["email"] or not cfg["password"]:
        return {"ok": False, "error": "PO email not configured"}

    try:
        mail = _imaplib.IMAP4_SSL(cfg["imap_server"])
        mail.login(cfg["email"], cfg["password"])
        mail.select("INBOX")

        # Search for unread messages
        status, messages = mail.search(None, "UNSEEN")
        if status != "OK":
            return {"ok": False, "error": "IMAP search failed"}

        msg_nums = messages[0].split()
        processed = 0

        for num in msg_nums[-50:]:  # Process last 50 unread
            try:
                status, data = mail.fetch(num, "(BODY.PEEK[])")
                if status != "OK":
                    continue

                msg = _email_lib.message_from_bytes(data[0][1])
                subject = _decode_email_header(msg.get("Subject", ""))
                sender = _decode_email_header(msg.get("From", ""))
                body = _extract_email_body(msg)
                email_uid = msg.get("Message-ID", str(num))

                # Parse and process
                result = _process_po_email(subject, sender, body, email_uid)
                if result.get("matched"):
                    processed += 1
                    # Mark as read
                    mail.store(num, "+FLAGS", "\\Seen")

            except Exception as e:
                log.warning("PO email processing error: %s", e)

        mail.logout()
        _PO_POLL_STATUS["last_poll"] = datetime.now().isoformat()
        _PO_POLL_STATUS["emails_processed"] += processed
        return {"ok": True, "processed": processed, "total_checked": len(msg_nums)}

    except Exception as e:
        _PO_POLL_STATUS["errors"].append(str(e))
        log.error("PO inbox poll failed: %s", e)
        return {"ok": False, "error": str(e)}


def _decode_email_header(header):
    """Decode email header."""
    if not header:
        return ""
    parts = _decode_header(header)
    return "".join(
        part.decode(enc or "utf-8") if isinstance(part, bytes) else part
        for part, enc in parts
    )


def _extract_email_body(msg):
    """Extract plain text body from email."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode("utf-8", errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode("utf-8", errors="replace")
    return ""


def _process_po_email(subject, sender, body, email_uid):
    """Dispatch to V2 or legacy path based on `orders_v2.poller_unified` flag.

    Hard cut: when the flag is on, ALL writes go to V2 (orders + order_line_items
    + order_audit_log). When off, the legacy purchase_orders / po_line_items /
    po_emails / po_status_history tables get the writes (pre-migration behavior).

    Flip via `/api/admin/flags` — see docs/PRD_ORDERS_V2_POLLER_MIGRATION.md.
    """
    try:
        from src.core.flags import get_flag
        if get_flag("orders_v2.poller_unified", False):
            return _process_po_email_v2(subject, sender, body, email_uid)
    except Exception as e:
        log.warning("poller flag read failed, falling back to legacy: %s", e)
    return _process_po_email_legacy(subject, sender, body, email_uid)


def _process_po_email_legacy(subject, sender, body, email_uid):
    """Legacy poller — writes to purchase_orders + po_line_items + po_emails.

    Kept verbatim from pre-migration code so flipping the FF off resumes the
    exact prior behavior. Will be deleted with the legacy tables in a future PR.
    """
    result = {"matched": False, "po_id": None, "updates": []}

    # Extract PO number from subject or body
    po_numbers = _extract_po_numbers(subject + " " + body)
    if not po_numbers:
        return result

    # Match to existing POs
    from src.core.db import get_db
    with get_db() as conn:
        conn.row_factory = _sqlite3.Row
        for po_num in po_numbers:
            row = conn.execute(
                "SELECT id, status FROM purchase_orders WHERE po_number = ? LIMIT 1",
                (po_num,)
            ).fetchone()

            if not row:
                continue

            po_id = row["id"]
            result["matched"] = True
            result["po_id"] = po_id

            # Extract status updates from email content
            updates = _extract_status_updates(body)

            # Record the email
            conn.execute("""INSERT INTO po_emails
                (po_id, email_uid, direction, sender, subject, body_preview, parsed_updates, received_at, processed_at)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (po_id, email_uid, "inbound", sender, subject, body[:500],
                 json.dumps(updates), datetime.now().isoformat(), datetime.now().isoformat()))

            # Apply updates
            for update in updates:
                _apply_po_update(conn, po_id, update)
                result["updates"].append(update)

            # Recalculate PO status based on line items
            _recalculate_po_status(conn, po_id)

            log.info("PO email matched: %s → %s with %d updates", po_num, po_id, len(updates))
            break

    return result


def _process_po_email_v2(subject, sender, body, email_uid):
    """V2 poller — delegates to src.core.po_email_v2.process_email.

    Lives here so the FF dispatcher can call it; real logic is in the
    pure module so tests can import without dragging in dashboard's exec()
    namespace.
    """
    from src.core import po_email_v2
    return po_email_v2.process_email(subject, sender, body, email_uid)


def _extract_po_numbers(text):
    """Extract PO numbers from text. Supports common formats."""
    patterns = [
        r'PO[#:\s]*(\d{4,12})',           # PO#12345, PO: 12345
        r'Purchase Order[#:\s]*(\d{4,12})', # Purchase Order #12345
        r'P\.?O\.?\s*(\d{4,12})',          # P.O. 12345
        r'Order[#:\s]*(\d{4,12})',         # Order #12345
        r'#(\d{4,12})',                     # #12345 (generic)
    ]
    found = set()
    for pattern in patterns:
        for match in _re.finditer(pattern, text, _re.IGNORECASE):
            found.add(match.group(1))
    return list(found)


def _extract_status_updates(body):
    """Extract status updates from email body text."""
    updates = []
    body_lower = body.lower()

    # Tracking number detection
    tracking_patterns = [
        (r'tracking[#:\s]*([A-Z0-9]{10,30})', "tracking"),
        (r'1Z[A-Z0-9]{16}', "ups"),           # UPS
        (r'\b\d{12,22}\b', "fedex"),            # FedEx
        (r'94\d{20,22}', "usps"),               # USPS
    ]
    for pattern, carrier in tracking_patterns:
        for match in _re.finditer(pattern, body, _re.IGNORECASE):
            tracking = match.group(1) if match.lastindex else match.group(0)
            updates.append({"type": "tracking", "tracking_number": tracking, "carrier": carrier})

    # Ship date detection
    if any(kw in body_lower for kw in ["shipped", "dispatched", "in transit", "out for delivery"]):
        updates.append({"type": "status_change", "new_status": "shipped"})

    # Delivery confirmation
    if any(kw in body_lower for kw in ["delivered", "received", "signed for"]):
        updates.append({"type": "status_change", "new_status": "delivered"})

    # Backorder detection
    if any(kw in body_lower for kw in ["backorder", "back order", "out of stock", "delayed"]):
        updates.append({"type": "status_change", "new_status": "backordered"})

    # Invoice detection
    if any(kw in body_lower for kw in ["invoice", "billing", "payment due"]):
        updates.append({"type": "status_change", "new_status": "invoiced"})

    # Confirmation
    if any(kw in body_lower for kw in ["order confirmed", "confirmation", "processing your order"]):
        updates.append({"type": "status_change", "new_status": "confirmed"})

    return updates


def _apply_po_update(conn, po_id, update):
    """Apply a single update to a PO and its line items."""
    now = datetime.now().isoformat()

    if update["type"] == "tracking":
        # Apply tracking number to all unshipped line items
        conn.execute("""UPDATE po_line_items 
            SET tracking_number = ?, carrier = ?, status = 'shipped', ship_date = ?, updated_at = ?
            WHERE po_id = ? AND status IN ('pending', 'confirmed')""",
            (update["tracking_number"], update.get("carrier", ""), now, now, po_id))

        conn.execute("""INSERT INTO po_status_history (po_id, old_status, new_status, change_reason, created_at)
            VALUES (?, 'processing', 'shipped', ?, ?)""",
            (po_id, f"Tracking: {update['tracking_number']}", now))

    elif update["type"] == "status_change":
        new_status = update["new_status"]
        # Update PO-level status
        old_status = conn.execute("SELECT status FROM purchase_orders WHERE id = ?", (po_id,)).fetchone()
        old = old_status[0] if old_status else "unknown"
        conn.execute("UPDATE purchase_orders SET status = ?, updated_at = ? WHERE id = ?",
                     (new_status, now, po_id))
        # Update all line items
        conn.execute("UPDATE po_line_items SET status = ?, updated_at = ? WHERE po_id = ? AND status != ?",
                     (new_status, now, po_id, new_status))
        # Record history
        conn.execute("""INSERT INTO po_status_history (po_id, old_status, new_status, changed_by, created_at)
            VALUES (?, ?, ?, 'email_auto', ?)""", (po_id, old, new_status, now))


def _recalculate_po_status(conn, po_id):
    """Recalculate PO status based on line item statuses.

    Returns the computed `new_status` so the caller can forward it to
    `_mirror_status_to_orders_v2` after the connection context closes.
    Returns None if there are no line items to reduce over.
    """
    rows = conn.execute(
        "SELECT status, COUNT(*) as cnt FROM po_line_items WHERE po_id = ? GROUP BY status",
        (po_id,)
    ).fetchall()

    if not rows:
        return None

    status_counts = {r[0]: r[1] for r in rows}
    total = sum(status_counts.values())

    # Determine overall PO status
    if status_counts.get("invoiced", 0) == total:
        new_status = "invoiced"
    elif status_counts.get("delivered", 0) == total:
        new_status = "delivered"
    elif status_counts.get("delivered", 0) + status_counts.get("invoiced", 0) == total:
        new_status = "delivered"
    elif status_counts.get("shipped", 0) + status_counts.get("delivered", 0) + status_counts.get("invoiced", 0) > 0:
        if status_counts.get("pending", 0) + status_counts.get("confirmed", 0) > 0:
            new_status = "partial_shipped"
        else:
            new_status = "shipped"
    elif status_counts.get("backordered", 0) > 0:
        new_status = "backordered"
    elif status_counts.get("confirmed", 0) > 0:
        new_status = "processing"
    else:
        new_status = "received"

    now = datetime.now().isoformat()
    conn.execute("UPDATE purchase_orders SET status = ?, updated_at = ? WHERE id = ?",
                 (new_status, now, po_id))
    return new_status


# ═══════════════════════════════════════════════════════════════════════════════
# Background Poller Thread
# ═══════════════════════════════════════════════════════════════════════════════

_po_poller_thread = None

def _po_poller_loop():
    """Background thread that polls PO inbox periodically."""
    while True:
        try:
            _PO_POLL_STATUS["running"] = True
            result = _poll_po_inbox()
            if result.get("error"):
                log.debug("PO poll: %s", result["error"])
        except Exception as e:
            log.warning("PO poller error: %s", e)
        _time.sleep(_PO_POLL_INTERVAL)


def _start_po_poller():
    """Start the PO email poller background thread."""
    global _po_poller_thread
    cfg = _get_po_email_config()
    if not cfg["email"]:
        log.info("PO email poller: not configured (set PO_GMAIL_ADDRESS env var)")
        return

    if _po_poller_thread and _po_poller_thread.is_alive():
        return

    _po_poller_thread = _threading.Thread(target=_po_poller_loop, daemon=True)
    _po_poller_thread.start()
    log.info("PO email poller started for %s (interval=%ds)", cfg["email"], _PO_POLL_INTERVAL)


# PO poller started from dashboard.py boot sequence — not here
# _start_po_poller()


# ═══════════════════════════════════════════════════════════════════════════════
# API Routes
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/po-tracking")
@auth_required
@safe_page
def po_tracking_dashboard():
    """DEPRECATED (V2): Redirects to unified /orders page."""
    return redirect("/orders", code=301)


@bp.route("/po-tracking-legacy")
@auth_required
@safe_page
def po_tracking_dashboard_legacy():
    """Purchase Order tracking dashboard (legacy — kept for reference)."""
    from src.core.db import get_db
    pos = []
    try:
        with get_db() as conn:
            conn.row_factory = _sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM purchase_orders ORDER BY created_at DESC LIMIT 200"
            ).fetchall()
            pos = [dict(r) for r in rows]
    except Exception as e:
        log.warning("PO tracking query failed: %s", e)

    # Compute summary stats
    stats = {
        "total": len(pos),
        "received": sum(1 for p in pos if p.get("status") == "received"),
        "processing": sum(1 for p in pos if p.get("status") == "processing"),
        "shipped": sum(1 for p in pos if p.get("status") in ("shipped", "partial_shipped")),
        "delivered": sum(1 for p in pos if p.get("status") == "delivered"),
        "invoiced": sum(1 for p in pos if p.get("status") == "invoiced"),
        "backordered": sum(1 for p in pos if p.get("status") == "backordered"),
    }

    poll_status = dict(_PO_POLL_STATUS)
    return render_page("po_tracking.html", active_page="Orders", pos=pos, stats=stats, poll_status=poll_status)


@bp.route("/po-tracking/<po_id>")
@auth_required
@safe_page
def po_detail(po_id):
    """PO detail page with line items, email history, status timeline."""
    from src.core.db import get_db
    with get_db() as conn:
        conn.row_factory = _sqlite3.Row

        po = conn.execute("SELECT * FROM purchase_orders WHERE id = ?", (po_id,)).fetchone()
        if not po:
            flash("PO not found", "error")
            return redirect("/po-tracking")
        po = dict(po)

        line_items = [dict(r) for r in conn.execute(
            "SELECT * FROM po_line_items WHERE po_id = ? ORDER BY line_number", (po_id,)
        ).fetchall()]

        emails = [dict(r) for r in conn.execute(
            "SELECT * FROM po_emails WHERE po_id = ? ORDER BY received_at DESC LIMIT 50", (po_id,)
        ).fetchall()]

        history = [dict(r) for r in conn.execute(
            "SELECT * FROM po_status_history WHERE po_id = ? ORDER BY created_at DESC LIMIT 50", (po_id,)
        ).fetchall()]

    return render_page("po_detail.html", active_page="Orders",
        po=po, line_items=line_items, emails=emails, history=history)


def _mirror_po_to_orders_v2(po_id, po_number, vendor_name, buyer_name, buyer_email,
                            institution, total, status, rfq_id, quote_number,
                            line_items, now):
    """Eagerly mirror a newly-created PO into the Orders V2 schema.

    The boot migration already does this on deploy; this writes the same row
    immediately so the V2 tables don't go stale until the next restart. Idempotent
    via INSERT OR IGNORE on the deterministic id `ORD-PO-<po_number>`. Failures
    here MUST NOT break the legacy write (best-effort propagation).
    """
    try:
        from src.core.db import get_db
        oid = f"ORD-PO-{po_number}"
        with get_db() as conn:
            existing = conn.execute("SELECT id FROM orders WHERE id=?", (oid,)).fetchone()
            if existing:
                return False  # boot migration or prior call already covered it
            conn.execute("""
                INSERT OR IGNORE INTO orders
                (id, quote_number, po_number, agency, institution, total, status,
                 buyer_name, buyer_email, created_at, updated_at, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (oid, quote_number or "", po_number, "", institution or "",
                  total or 0, status or "received",
                  buyer_name or "", buyer_email or "", now, now, ""))
            for li in line_items:
                qty = li.get("qty_ordered", 0) or 0
                price = li.get("unit_price", 0) or 0
                conn.execute("""
                    INSERT INTO order_line_items
                    (order_id, line_number, description, part_number, mfg_number,
                     uom, qty_ordered, unit_price, extended_price,
                     sourcing_status, created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (oid, li.get("line_number", 1),
                      li.get("description", ""), li.get("item_number", ""),
                      li.get("mfg_number", ""), li.get("uom", "EA"),
                      qty, price, round(qty * price, 2),
                      li.get("status", "pending"), now, now))
        log.info("Orders V2 mirror: %s (%d items) → %s", po_number, len(line_items), oid)
        return True
    except Exception as e:
        log.warning("V2 mirror failed for %s (legacy write still succeeded): %s", po_number, e)
        return False


def _mirror_status_to_orders_v2(po_number, new_status, now):
    """Mirror an overall PO status change to the Orders V2 row.

    Looks up the order by po_number first, then falls back to the deterministic
    id `ORD-PO-<po_number>`. Best-effort — never raises.
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT id FROM orders WHERE po_number=? LIMIT 1", (po_number,)
            ).fetchone()
            oid = row["id"] if row else f"ORD-PO-{po_number}"
            cur = conn.execute(
                "UPDATE orders SET status=?, updated_at=? WHERE id=?",
                (new_status, now, oid),
            )
            if cur.rowcount == 0:
                log.debug("V2 mirror: no orders row for po_number=%s", po_number)
                return False
        return True
    except Exception as e:
        log.warning("V2 status mirror failed for %s: %s", po_number, e)
        return False


@bp.route("/api/po/create", methods=["POST"])
@auth_required
@safe_route
def create_po():
    """Create a new PO from an RFQ/quote that was won."""
    data = request.get_json(silent=True) or {}
    po_number = data.get("po_number", "")
    rfq_id = data.get("rfq_id", "")

    if not po_number:
        return jsonify({"ok": False, "error": "PO number required"}), 400

    import uuid
    po_id = f"po_{str(uuid.uuid4())[:8]}"
    now = datetime.now().isoformat()

    # If linked to RFQ, pull line items
    line_items = []
    rfq_data = {}
    if rfq_id:
        rfqs = load_rfqs()
        rfq_data = rfqs.get(rfq_id, {})
        for i, item in enumerate(rfq_data.get("line_items", [])):
            line_items.append({
                "line_number": i + 1,
                "description": item.get("description", ""),
                "item_number": item.get("item_number", ""),
                "qty_ordered": item.get("qty", 0) or 0,
                "unit_price": item.get("price_per_unit", 0) or 0,
                "extended_price": (item.get("qty", 0) or 0) * (item.get("price_per_unit", 0) or 0),
                "uom": item.get("uom", "EA"),
                "status": "pending",
            })

    total = sum(li.get("extended_price", 0) for li in line_items)

    from src.core.db import get_db
    with get_db() as conn:
        conn.execute("""INSERT INTO purchase_orders
            (id, po_number, vendor_name, buyer_name, buyer_email, institution,
             order_date, total_amount, status, rfq_id, quote_number, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (po_id, po_number,
             data.get("vendor_name", ""),
             rfq_data.get("requestor_name", data.get("buyer_name", "")),
             rfq_data.get("requestor_email", data.get("buyer_email", "")),
             rfq_data.get("delivery_location", data.get("institution", "")),
             now, total, "received", rfq_id,
             data.get("quote_number", rfq_data.get("reytech_quote_number", "")),
             now, now))

        for li in line_items:
            conn.execute("""INSERT INTO po_line_items
                (po_id, line_number, description, item_number, qty_ordered, unit_price, extended_price, uom, status, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (po_id, li["line_number"], li["description"], li["item_number"],
                 li["qty_ordered"], li["unit_price"], li["extended_price"],
                 li["uom"], li["status"], now))

        conn.execute("""INSERT INTO po_status_history (po_id, new_status, changed_by, change_reason, created_at)
            VALUES (?, 'received', 'user', 'PO created', ?)""", (po_id, now))

    # Eagerly propagate to Orders V2 so the new PO is visible in /orders
    # without waiting for the next deploy's boot migration to run.
    # Best-effort: mirror failure must NEVER fail the legacy write — the
    # caller has already received their PO id and the boot migration will
    # backfill on next deploy.
    try:
        _mirror_po_to_orders_v2(
            po_id=po_id, po_number=po_number,
            vendor_name=data.get("vendor_name", ""),
            buyer_name=rfq_data.get("requestor_name", data.get("buyer_name", "")),
            buyer_email=rfq_data.get("requestor_email", data.get("buyer_email", "")),
            institution=rfq_data.get("delivery_location", data.get("institution", "")),
            total=total, status="received", rfq_id=rfq_id,
            quote_number=data.get("quote_number", rfq_data.get("reytech_quote_number", "")),
            line_items=line_items, now=now,
        )
    except Exception as _e:
        log.warning("V2 mirror raised for %s — legacy write succeeded: %s", po_number, _e)

    log.info("PO created: %s (%s) with %d items", po_number, po_id, len(line_items))
    return jsonify({"ok": True, "po_id": po_id, "items": len(line_items)})


@bp.route("/api/po/<po_id>/update-item", methods=["POST"])
@auth_required
@safe_route
def update_po_item(po_id):
    """Update a single PO line item status.

    If a tracking_number is supplied without an explicit status, the
    line is auto-promoted to ``shipped`` (with detected carrier and a
    fresh ``ship_date``). This keeps the operator from having to make
    two separate clicks every time a vendor sends a tracking link.
    """
    data = request.get_json(silent=True) or {}
    item_id = data.get("item_id")
    new_status = data.get("status")
    tracking = data.get("tracking_number")

    if not item_id:
        return jsonify({"ok": False, "error": "item_id required"}), 400

    now = datetime.now().isoformat()
    from src.core.db import get_db
    with get_db() as conn:
        conn.row_factory = _sqlite3.Row
        old = conn.execute(
            "SELECT status, carrier FROM po_line_items WHERE id = ? AND po_id = ?",
            (item_id, po_id)
        ).fetchone()
        if not old:
            return jsonify({"ok": False, "error": "Item not found"}), 404

        carrier = None
        if tracking and not new_status:
            from src.core.carrier_tracking import auto_promote_status_for_tracking
            promoted, carrier = auto_promote_status_for_tracking(
                old["status"], tracking, old["carrier"] or ""
            )
            if promoted:
                new_status = promoted

        if not new_status:
            return jsonify({"ok": False, "error": "status required (or send tracking_number to auto-promote)"}), 400

        updates = {"status": new_status, "updated_at": now}
        if tracking:
            updates["tracking_number"] = tracking
            if carrier and not old["carrier"]:
                updates["carrier"] = carrier
        if new_status == "shipped":
            updates["ship_date"] = now
        elif new_status == "delivered":
            updates["delivery_date"] = now

        sets = ", ".join(f"{k}=?" for k in updates)
        conn.execute("UPDATE po_line_items SET " + sets + " WHERE id = ?",
                     list(updates.values()) + [item_id])

        conn.execute("""INSERT INTO po_status_history
            (po_id, line_item_id, old_status, new_status, changed_by, created_at)
            VALUES (?,?,?,?,?,?)""",
            (po_id, item_id, old["status"], new_status, "user", now))

        recalc_status = _recalculate_po_status(conn, po_id)
        # Capture po_number for V2 mirror while we still hold the conn.
        po_num_row = conn.execute(
            "SELECT po_number FROM purchase_orders WHERE id = ?", (po_id,)
        ).fetchone()
        po_num = po_num_row[0] if po_num_row else ""

    # Mirror the recalculated status to Orders V2 so margin/analytics
    # views reflect the line-item change without waiting for next boot
    # migration. Best-effort — failures MUST NOT fail the legacy write.
    if po_num and recalc_status:
        try:
            _mirror_status_to_orders_v2(po_num, recalc_status, now)
        except Exception as _e:
            log.warning("V2 recalc mirror raised for %s — legacy write succeeded: %s",
                        po_num, _e)

    return jsonify({"ok": True})


@bp.route("/api/po/<po_id>/update-status", methods=["POST"])
@auth_required
@safe_route
def update_po_status(po_id):
    """Update overall PO status."""
    data = request.get_json(silent=True) or {}
    new_status = data.get("status")
    if not new_status:
        return jsonify({"ok": False, "error": "status required"}), 400

    now = datetime.now().isoformat()
    from src.core.db import get_db
    with get_db() as conn:
        old = conn.execute("SELECT status FROM purchase_orders WHERE id = ?", (po_id,)).fetchone()
        if not old:
            return jsonify({"ok": False, "error": "PO not found"}), 404

        conn.execute("UPDATE purchase_orders SET status = ?, updated_at = ? WHERE id = ?",
                     (new_status, now, po_id))
        conn.execute("""INSERT INTO po_status_history
            (po_id, old_status, new_status, changed_by, change_reason, created_at)
            VALUES (?,?,?,?,?,?)""",
            (po_id, old[0], new_status, "user", data.get("reason", "Manual update"), now))
        # Look up po_number while we still hold the connection
        po_num_row = conn.execute(
            "SELECT po_number FROM purchase_orders WHERE id = ?", (po_id,)
        ).fetchone()
        po_num = po_num_row[0] if po_num_row else ""

    if po_num:
        try:
            _mirror_status_to_orders_v2(po_num, new_status, now)
        except Exception as _e:
            log.warning("V2 status mirror raised for %s — legacy write succeeded: %s",
                        po_num, _e)

    return jsonify({"ok": True})


@bp.route("/api/po/migration-parity")
@auth_required
@safe_route
def po_migration_parity():
    """Report parity between legacy purchase_orders and Orders V2 rows.

    Used to monitor the user-CRUD V2 mirror and decide when it is safe to
    drop the legacy tables. `unmirrored` is the count of legacy POs whose
    po_number is not present as an order — those are the rows the boot
    migration will pick up on next deploy (or that the new mirror has
    failed to write).
    """
    from src.core.db import get_db
    with get_db() as conn:
        try:
            legacy = conn.execute("SELECT COUNT(*) FROM purchase_orders").fetchone()[0]
        except Exception:
            legacy = 0
        v2 = conn.execute(
            "SELECT COUNT(*) FROM orders WHERE po_number != '' AND po_number IS NOT NULL"
        ).fetchone()[0]
        try:
            unmirrored = conn.execute("""
                SELECT COUNT(*) FROM purchase_orders
                WHERE po_number NOT IN (SELECT po_number FROM orders WHERE po_number != '')
            """).fetchone()[0]
        except Exception:
            unmirrored = 0
    return jsonify({
        "ok": True,
        "legacy_count": legacy,
        "v2_count": v2,
        "unmirrored": unmirrored,
        "parity": legacy == 0 or unmirrored == 0,
    })


@bp.route("/api/po/poll", methods=["POST"])
@auth_required
@safe_route
def trigger_po_poll():
    """Manually trigger PO email polling."""
    result = _poll_po_inbox()
    return jsonify(result)


@bp.route("/api/po/status")
@auth_required
@safe_route
def po_poll_status():
    """Get PO poller status."""
    cfg = _get_po_email_config()
    return jsonify({
        "configured": bool(cfg["email"]),
        "email": cfg["email"][:5] + "..." if cfg["email"] else "",
        **_PO_POLL_STATUS,
    })


@bp.route("/api/po/stats")
@auth_required
@safe_route
def po_stats():
    """Get PO tracking statistics."""
    from src.core.db import get_db
    stats = {}
    try:
        with get_db() as conn:
            # Overall counts
            total = conn.execute("SELECT COUNT(*) FROM purchase_orders").fetchone()[0]
            stats["total_pos"] = total

            # By status
            rows = conn.execute("SELECT status, COUNT(*) FROM purchase_orders GROUP BY status").fetchall()
            stats["by_status"] = {r[0]: r[1] for r in rows}

            # Total value
            val = conn.execute("SELECT COALESCE(SUM(total_amount), 0) FROM purchase_orders").fetchone()[0]
            stats["total_value"] = val

            # Overdue (expected_delivery < today, not delivered)
            today = datetime.now().strftime("%Y-%m-%d")
            overdue = conn.execute("""SELECT COUNT(*) FROM purchase_orders 
                WHERE expected_delivery < ? AND status NOT IN ('delivered', 'invoiced')""",
                (today,)).fetchone()[0]
            stats["overdue"] = overdue

            # Line items
            li_rows = conn.execute("SELECT status, COUNT(*) FROM po_line_items GROUP BY status").fetchall()
            stats["line_items_by_status"] = {r[0]: r[1] for r in li_rows}

    except Exception as e:
        stats["error"] = str(e)

    return jsonify({"ok": True, **stats})
