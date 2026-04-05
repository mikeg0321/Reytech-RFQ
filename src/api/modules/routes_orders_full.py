# routes_orders_full.py — Order Management, Supplier Lookup, Quote-Order Link
# Extracted from routes_intel.py for maintainability

# ═══════════════════════════════════════════════════════════════════════
# Order Management (Phase 17)
# ═══════════════════════════════════════════════════════════════════════

# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from src.core.error_handler import safe_route
from flask import redirect, flash, send_file
from src.core.paths import DATA_DIR
from src.api.render import render_page

@bp.route("/orders")
@auth_required
@safe_route
def orders_page():
    """Orders dashboard — track sourcing, shipping, delivery, invoicing."""
    orders = _load_orders()

    # ── Filtering ──
    filter_status = request.args.get("status", "")
    filter_agency = request.args.get("agency", "")
    search_q = request.args.get("q", "").lower()

    order_list = sorted(orders.values(), key=lambda o: o.get("created_at", ""), reverse=True)

    if filter_status:
        order_list = [o for o in order_list if o.get("status") == filter_status]
    if filter_agency:
        order_list = [o for o in order_list if filter_agency.lower() in (o.get("agency", "") or "").lower()]
    if search_q:
        order_list = [o for o in order_list if
                      search_q in (o.get("po_number", "") or "").lower() or
                      search_q in (o.get("institution", "") or "").lower() or
                      search_q in (o.get("order_id", "") or "").lower() or
                      search_q in (o.get("quote_number", "") or "").lower() or
                      search_q in (o.get("agency", "") or "").lower()]

    status_cfg = {
        "new":              ("🆕 New",              "#58a6ff", "rgba(88,166,255,.1)"),
        "sourcing":         ("🛒 Sourcing",         "#d29922", "rgba(210,153,34,.1)"),
        "shipped":          ("🚚 Shipped",          "#bc8cff", "rgba(188,140,255,.1)"),
        "partial_delivery": ("📦 Partial",          "#d29922", "rgba(210,153,34,.1)"),
        "delivered":        ("✅ Delivered",         "#3fb950", "rgba(52,211,153,.1)"),
        "invoiced":         ("💰 Invoiced",         "#58a6ff", "rgba(88,166,255,.1)"),
        "closed":           ("🏁 Closed",           "#8b949e", "rgba(139,148,160,.1)"),
    }

    # ── Aggregate stats for template ──
    all_items = []
    for o in order_list:
        for it in o.get("line_items", []):
            all_items.append(it)

    total_line_items = len(all_items)
    delivered_items = sum(1 for it in all_items if it.get("sourcing_status") == "delivered")

    stats = {
        "total_orders": len(order_list),
        "active": sum(1 for o in order_list if o.get("status") not in ("closed",)),
        "total_value": sum(o.get("total", 0) for o in order_list),
        "invoiced_value": sum(o.get("invoice_total", 0) for o in order_list),
        "total_line_items": total_line_items,
        "pending_items": sum(1 for it in all_items if it.get("sourcing_status") == "pending"),
        "ordered_items": sum(1 for it in all_items if it.get("sourcing_status") == "ordered"),
        "shipped_items": sum(1 for it in all_items if it.get("sourcing_status") == "shipped"),
        "delivered_items": delivered_items,
        "pct_complete": round(delivered_items / total_line_items * 100) if total_line_items else 0,
        "orders_needing_action": sum(1 for o in order_list if o.get("status") == "new" and o.get("line_items")),
        "orders_ready_invoice": sum(1 for o in order_list if o.get("status") == "delivered"),
    }

    # ── Enrich each order with aging badge + computed counts for template ──
    for o in order_list:
        items = o.get("line_items", [])
        o["delivered_count"] = sum(1 for it in items if it.get("sourcing_status") == "delivered")
        try:
            from src.api.modules.routes_orders_enhance import calc_order_aging
            aging = calc_order_aging(o)
            o["age_badge"] = aging["badge"]
            o["age_title"] = f"{aging['age_days']}d old, {aging['stale_days']}d since update"
        except Exception:
            o["age_badge"] = ""
            o["age_title"] = ""

    all_agencies = sorted(set(o.get("agency", "") for o in orders.values() if o.get("agency")))

    return render_page("orders.html", active_page="Orders",
        order_list=order_list, stats=stats, status_cfg=status_cfg,
        all_agencies=all_agencies, filter_status=filter_status,
        filter_agency=filter_agency, search_q=request.args.get("q", ""))


@bp.route("/order/<oid>")
@auth_required
@safe_page
def order_detail(oid):
    """Order detail page — line item sourcing, tracking, invoicing."""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        flash(f"Order {oid} not found", "error")
        return redirect("/orders")

    try:
        return _render_order_detail(order, oid)
    except Exception as e:
        import traceback
        log.error("Order detail render error for %s: %s\n%s", oid, e, traceback.format_exc())
        return _wrap_page(f"""
        <div class="card" style="padding:24px">
         <h2 style="color:var(--rd)">⚠️ Error rendering order {oid}</h2>
         <pre style="color:var(--tx2);font-size:14px;overflow:auto;max-height:400px">{type(e).__name__}: {e}</pre>
         <a href="/orders" class="btn btn-s" style="margin-top:12px">← Back to Orders</a>
        </div>""", f"Error: {oid}")


def _render_order_detail(order, oid):
    """Order detail rendering — V2: data only, all HTML in Jinja2 template."""
    st = order.get("status", "new")
    items = order.get("line_items", [])
    qn = order.get("quote_number", "")
    institution = order.get("institution", "")

    status_cfg = {
        "new": "🆕 New", "sourcing": "🛒 Sourcing", "shipped": "🚚 Shipped",
        "partial_delivery": "📦 Partial Delivery", "delivered": "✅ Delivered",
        "invoiced": "💰 Invoiced", "closed": "🏁 Closed"
    }

    sourced_count = sum(1 for i in items if i.get('sourcing_status') in ('ordered','shipped','delivered'))
    shipped_count = sum(1 for i in items if i.get('sourcing_status') in ('shipped','delivered'))
    delivered_count = sum(1 for i in items if i.get('sourcing_status') == 'delivered')
    total_count = len(items)

    return render_page("order_detail.html", active_page="Orders",
        oid=oid, order=order, items=items,
        qn=qn, institution=institution, st=st, status_cfg=status_cfg,
        sourced_count=sourced_count, shipped_count=shipped_count,
        delivered_count=delivered_count, total_count=total_count)


# ─── Order API Routes ──────────────────────────────────────────────────────

@bp.route("/order/new")
@auth_required
@safe_page
def order_create_page():
    """V2: Manual order creation form with line items, PO PDF import, quote lookup."""
    return render_page("order_create.html", active_page="Orders")


@bp.route("/po-upload")
@auth_required
@safe_page
def po_upload_page():
    """Manual PO upload page — parse PDF, preview items, create order."""
    return render_page("po_upload.html", active_page="Orders")


@bp.route("/api/po/upload-parse", methods=["POST"])
@auth_required
@safe_route
def api_po_upload_parse():
    """Upload a PO PDF, parse it, return extracted data for preview."""
    import tempfile, os as _os, re as _re
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"ok": False, "error": "Upload a PDF file"})
    if not f.filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "error": "File must be a PDF"})

    # Save to temp
    safe_fn = _re.sub(r'[^a-zA-Z0-9._-]', '_', _os.path.basename(f.filename))
    upload_dir = _os.path.join(DATA_DIR, "uploads", "po_uploads")
    _os.makedirs(upload_dir, exist_ok=True)
    pdf_path = _os.path.join(upload_dir, f"po_{safe_fn}")
    f.save(pdf_path)

    # Parse
    try:
        from src.agents.email_poller import _parse_po_pdf
        result = _parse_po_pdf(pdf_path)
        return jsonify({
            "ok": True,
            "po_number": result.get("po_number", ""),
            "agency": result.get("agency", ""),
            "institution": result.get("institution", ""),
            "items": result.get("items", []),
            "total": result.get("total", 0),
            "subtotal": result.get("subtotal", 0),
            "tax": result.get("tax", 0),
            "pdf_path": pdf_path,
        })
    except Exception as e:
        log.error("PO parse error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e), "pdf_path": pdf_path})


@bp.route("/api/order/create", methods=["POST"])
@auth_required
@safe_route
@rate_limit("api")
@audit_action("order_create")
def api_order_create():
    """Create a new order manually (for POs received outside the system).
    POST JSON: {po_number, agency, institution, total, items: [{description, qty, unit_price, part_number, supplier, supplier_url}]}
    """
    data = request.get_json(force=True, silent=True) or {}
    po_number = data.get("po_number", "").strip()
    if not po_number:
        return jsonify({"ok": False, "error": "PO number required"})
    
    from src.api.dashboard import _create_order_from_po_email
    order = _create_order_from_po_email({
        "po_number": po_number,
        "agency": data.get("agency", ""),
        "institution": data.get("institution", ""),
        "items": data.get("items", []),
        "total": data.get("total", 0),
        "matched_quote": data.get("quote_number", ""),
        "sender_email": "",
        "subject": f"Manual order — PO {po_number}",
        "po_pdf_path": "",
    })
    if order.get("skipped"):
        return jsonify({"ok": False, "error": "Order rejected: no items or value. Add items or a total."})
    
    # ── Google Drive: create PO folder ──
    try:
        from src.agents.drive_triggers import on_po_received
        on_po_received(order)
    except Exception as _gde:
        log.debug("Drive trigger (po_received): %s", _gde)
    
    return jsonify({"ok": True, "order_id": order.get("order_id"), "items": len(order.get("line_items", []))})


@bp.route("/api/order/<oid>/add-line", methods=["POST"])
@auth_required
@safe_route
def api_order_add_line(oid):
    """Add a line item to an existing order.
    POST JSON: {description, qty, unit_price, part_number, supplier, supplier_url}
    """
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(force=True, silent=True) or {}
    items = order.get("line_items", [])
    next_num = len(items) + 1
    pn = data.get("part_number", "")
    sup_url = data.get("supplier_url", "")
    supplier = data.get("supplier", "")
    if pn and (pn.startswith("B0") or pn.startswith("b0")) and not sup_url:
        sup_url = f"https://amazon.com/dp/{pn}"
        supplier = supplier or "Amazon"
    new_item = {
        "line_id": f"L{next_num:03d}",
        "description": data.get("description", ""),
        "part_number": pn,
        "qty": data.get("qty", 1),
        "unit_price": data.get("unit_price", 0),
        "extended": round(data.get("qty", 1) * data.get("unit_price", 0), 2),
        "supplier": supplier,
        "supplier_url": sup_url,
        "sourcing_status": "pending",
        "tracking_number": "",
        "carrier": "",
        "ship_date": "",
        "delivery_date": "",
        "invoice_status": "pending",
        "invoice_number": "",
        "notes": data.get("notes", ""),
    }
    items.append(new_item)
    order["line_items"] = items
    order["total"] = sum(it.get("extended", 0) for it in items)
    order["updated_at"] = datetime.now().isoformat()
    _save_single_order(oid, order)
    return jsonify({"ok": True, "line_id": new_item["line_id"], "total_items": len(items)})


@bp.route("/api/order/<oid>/import-po", methods=["POST"])
@auth_required
@safe_route
def api_order_import_po(oid):
    """Upload and parse a PO PDF to populate order line items.
    Multipart form: file=<pdf>
    Returns: {ok, items_added, total, raw_text (on failure for debugging)}
    """
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})

    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"ok": False, "error": "No file uploaded"})

    # Save the PDF
    po_dir = os.path.join(DATA_DIR, "po_documents")
    os.makedirs(po_dir, exist_ok=True)
    safe_name = f"{oid}_{re.sub(r'[^\\w.\\-]', '_', f.filename or 'po.pdf')}"
    pdf_path = os.path.join(po_dir, safe_name)
    f.save(pdf_path)

    # Parse
    from src.agents.email_poller import _parse_po_pdf
    parsed = _parse_po_pdf(pdf_path)

    if not parsed:
        # Return raw text for debugging — use pypdf since pdftotext may not be installed
        raw_text = ""
        try:
            from pypdf import PdfReader
            reader = PdfReader(pdf_path)
            for page in reader.pages:
                raw_text += (page.extract_text() or "") + "\n"
            raw_text = raw_text[:3000]
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        return jsonify({"ok": False, "error": "Could not parse PDF — see console for raw text",
                        "raw_text": raw_text})

    items_parsed = parsed.get("items", [])

    if not items_parsed:
        # Return raw text so Mike can see what the parser saw
        raw_text = parsed.get("_raw_text", "")
        return jsonify({"ok": False, "error": f"PDF parsed but 0 line items found. PO#={parsed.get('po_number','?')}, Agency={parsed.get('agency','?')}",
                        "raw_text": raw_text, "parsed_meta": {
                            "po_number": parsed.get("po_number"),
                            "agency": parsed.get("agency"),
                            "institution": parsed.get("institution"),
                            "total": parsed.get("total"),
                        }})

    # Build line items from parsed data
    new_items = []
    for i, it in enumerate(items_parsed):
        pn = it.get("part_number", "")
        sup_url = ""
        supplier = ""
        if pn and (pn.startswith("B0") or pn.startswith("b0")):
            sup_url = f"https://amazon.com/dp/{pn}"
            supplier = "Amazon"
        new_items.append({
            "line_id": f"L{i+1:03d}",
            "description": it.get("description", ""),
            "part_number": pn,
            "qty": it.get("qty", 0) or it.get("quantity", 0),
            "unit_price": it.get("unit_price", 0) or it.get("price", 0),
            "extended": it.get("extended", 0) or round(
                (it.get("qty", 0) or it.get("quantity", 0)) * (it.get("unit_price", 0) or it.get("price", 0)), 2),
            "supplier": supplier,
            "supplier_url": sup_url,
            "sourcing_status": "pending",
            "tracking_number": "",
            "carrier": "",
            "ship_date": "",
            "delivery_date": "",
            "invoice_status": "pending",
            "invoice_number": "",
            "notes": "",
        })

    # Replace order data (items, total, metadata)
    order["line_items"] = new_items
    total = parsed.get("total", 0) or sum(it.get("extended", 0) for it in new_items)
    order["total"] = total
    order["subtotal"] = parsed.get("subtotal", 0) or sum(it.get("extended", 0) for it in new_items)
    order["tax"] = parsed.get("tax", 0)

    # Update metadata from PO if better than what we had
    if parsed.get("po_number") and not order.get("po_number"):
        order["po_number"] = parsed["po_number"]
    if parsed.get("agency") and not order.get("agency"):
        order["agency"] = parsed["agency"]
    if parsed.get("institution"):
        order["institution"] = parsed["institution"]
        order["ship_to_name"] = parsed["institution"]
    if parsed.get("ship_to_address"):
        order["ship_to_address"] = parsed["ship_to_address"]

    order["po_pdf"] = pdf_path
    order["updated_at"] = datetime.now().isoformat()
    _save_single_order(oid, order)

    log.info("PO PDF imported for %s: %d items, $%.2f, po=%s",
             oid, len(new_items), total, parsed.get("po_number", "?"))

    return jsonify({
        "ok": True,
        "items_added": len(new_items),
        "total": total,
        "po_number": parsed.get("po_number", ""),
        "agency": parsed.get("agency", ""),
        "institution": parsed.get("institution", ""),
    })


@bp.route("/api/order/<oid>/line/<lid>", methods=["POST"])
@auth_required
@safe_route
def api_order_update_line(oid, lid):
    """Update a single line item. POST JSON with any fields to update."""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(force=True, silent=True) or {}
    # Support V2 field/value pattern (from margins editor)
    if "field" in data and "value" in data:
        data[data["field"]] = data["value"]
    updated = False
    for it in order.get("line_items", []):
        if it.get("line_id") == lid:
            for field in ("sourcing_status", "tracking_number", "carrier",
                          "ship_date", "delivery_date", "invoice_status",
                          "invoice_number", "supplier", "supplier_url", "notes",
                          "unit_cost", "cost", "asin", "part_number"):
                if field in data:
                    old_val = it.get(field, "")
                    it[field] = data[field]
                    # V2: sync unit_cost ↔ cost (legacy name)
                    if field == "unit_cost":
                        it["cost"] = data[field]
                    elif field == "cost":
                        it["unit_cost"] = data[field]
                    if field == "sourcing_status" and old_val != data[field]:
                        _log_crm_activity(order.get("quote_number",""), f"line_{data[field]}",
                                          f"Order {oid} line {lid}: {old_val} → {data[field]} — {it.get('description','')[:60]}",
                                          actor="user", metadata={"order_id": oid})
                    # Audit log every field change
                    try:
                        from src.api.modules.routes_orders_enhance import log_order_event
                        log_order_event(oid, f"line_{field}_changed", field,
                                        str(old_val), str(data[field]),
                                        "user", f"Line {lid}: {it.get('description','')[:40]}")
                    except Exception:
                        pass
            updated = True
            break
    if not updated:
        return jsonify({"ok": False, "error": "Line item not found"})
    order["updated_at"] = datetime.now().isoformat()
    _save_single_order(oid, order)
    _update_order_status(oid)
    
    # ── Line-item level notifications ──
    if "sourcing_status" in data:
        new_ss = data["sourcing_status"]
        desc_short = it.get("description", "")[:50]
        inst = order.get("institution", "")
        po = order.get("po_number", "")
        try:
            from src.agents.notify_agent import send_alert
            if new_ss == "shipped":
                tracking = it.get("tracking_number", "") or data.get("tracking_number", "")
                send_alert(
                    event_type="line_shipped",
                    title=f"🚚 Shipped: {desc_short}",
                    body=f"PO #{po} → {inst}\n{desc_short} x{it.get('qty',0)}"
                         + (f"\nTracking: {tracking}" if tracking else ""),
                    urgency="info",
                    context={"order_id": oid, "line_id": lid, "po_number": po},
                    cooldown_key=f"line_ship:{oid}:{lid}",
                )
            elif new_ss == "delivered":
                # Check if ALL items now delivered
                all_delivered = all(i.get("sourcing_status") == "delivered"
                                   for i in order.get("line_items", []))
                send_alert(
                    event_type="line_delivered",
                    title=f"✅ Delivered: {desc_short}" + (" — ALL ITEMS DONE" if all_delivered else ""),
                    body=f"PO #{po} → {inst}\n{desc_short} x{it.get('qty',0)}"
                         + (f"\n🏁 All {len(order.get('line_items',[]))} items delivered — create invoice!" if all_delivered else ""),
                    urgency="deal" if all_delivered else "info",
                    context={"order_id": oid, "line_id": lid, "po_number": po},
                    cooldown_key=f"line_del:{oid}:{lid}",
                )
        except Exception as _ne:
            log.debug("Line item notify: %s", _ne)
    
    # ── Catalog Learning: when supplier info changes, teach the catalog ──
    if any(f in data for f in ("supplier", "supplier_url", "unit_price")):
        try:
            _learn_supplier_from_order_line(it, order)
        except Exception as _e:
            log.debug("Catalog learning from order line: %s", _e)
    
    return jsonify({"ok": True})


@bp.route("/api/order/<oid>/bulk", methods=["POST"])
@auth_required
@safe_route
def api_order_bulk_update(oid):
    """Bulk update all line items. POST JSON with fields to set on all items."""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(force=True, silent=True) or {}
    for it in order.get("line_items", []):
        for field in ("sourcing_status", "carrier", "invoice_status"):
            if field in data:
                it[field] = data[field]
    order["updated_at"] = datetime.now().isoformat()
    _save_single_order(oid, order)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), "order_bulk_update",
                      f"Order {oid}: bulk update — {data}",
                      actor="user", metadata={"order_id": oid})
    return jsonify({"ok": True})


@bp.route("/api/order/<oid>/bulk-tracking", methods=["POST"])
@auth_required
@safe_route
def api_order_bulk_tracking(oid):
    """Add tracking to all pending/ordered items. POST: {tracking, carrier}"""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(force=True, silent=True) or {}
    tracking = data.get("tracking", "")
    carrier = data.get("carrier", "")
    updated = 0
    for it in order.get("line_items", []):
        if it.get("sourcing_status") in ("pending", "ordered"):
            it["tracking_number"] = tracking
            it["carrier"] = carrier
            it["sourcing_status"] = "shipped"
            it["ship_date"] = datetime.now().strftime("%Y-%m-%d")
            updated += 1
    order["updated_at"] = datetime.now().isoformat()
    _save_single_order(oid, order)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), "tracking_added",
                      f"Order {oid}: tracking {tracking} ({carrier}) added to {updated} items",
                      actor="user", metadata={"order_id": oid, "tracking": tracking})
    return jsonify({"ok": True, "updated": updated})


@bp.route("/api/order/<oid>/invoice", methods=["POST"])
@auth_required
@safe_route
def api_order_invoice(oid):
    """Create partial or full invoice. POST: {type: 'partial'|'full', invoice_number}"""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(force=True, silent=True) or {}
    inv_type = data.get("type", "full")
    inv_num = data.get("invoice_number", "")

    if inv_type == "full":
        # Mark all items as invoiced
        for it in order.get("line_items", []):
            it["invoice_status"] = "invoiced"
            it["invoice_number"] = inv_num
        order["invoice_type"] = "full"
        order["invoice_total"] = order.get("total", 0)
    elif inv_type == "partial":
        # Mark only delivered items as invoiced
        partial_total = 0
        for it in order.get("line_items", []):
            if it.get("sourcing_status") == "delivered":
                it["invoice_status"] = "invoiced"
                it["invoice_number"] = inv_num
                partial_total += it.get("extended", 0)
            elif it.get("sourcing_status") in ("shipped", "ordered"):
                it["invoice_status"] = "partial"
        order["invoice_type"] = "partial"
        order["invoice_total"] = partial_total

    order["invoice_number"] = inv_num
    order["updated_at"] = datetime.now().isoformat()
    order["status_history"].append({
        "status": f"invoice_{inv_type}",
        "timestamp": datetime.now().isoformat(),
        "actor": "user",
        "invoice_number": inv_num,
    })
    _save_single_order(oid, order)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), f"invoice_{inv_type}",
                      f"Order {oid}: {inv_type} invoice #{inv_num} — ${order.get('invoice_total',0):,.2f}",
                      actor="user", metadata={"order_id": oid, "invoice": inv_num})
    return jsonify({"ok": True, "invoice_type": inv_type, "invoice_total": order.get("invoice_total", 0)})


@bp.route("/api/order/<oid>/invoice-pdf", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_order_invoice_pdf(oid):
    """Generate a branded invoice PDF from order's draft_invoice data.
    Returns the PDF download URL."""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    
    if not order.get("draft_invoice"):
        return jsonify({"ok": False, "error": "No draft invoice — trigger invoice creation first"})
    
    try:
        from src.forms.invoice_generator import generate_invoice_pdf
        pdf_path = generate_invoice_pdf(order)
        if not pdf_path:
            return jsonify({"ok": False, "error": "PDF generation failed — check line items"})
        
        # Store path on order
        order["draft_invoice"]["pdf_path"] = pdf_path
        order["updated_at"] = datetime.now().isoformat()
        _save_single_order(oid, order)
        
        fname = os.path.basename(pdf_path)
        return jsonify({
            "ok": True,
            "pdf_path": pdf_path,
            "download_url": f"/api/order/{oid}/invoice-pdf/download",
            "filename": fname,
        })
    except Exception as e:
        log.error("Invoice PDF generation error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/order/<oid>/invoice-pdf/download")
@auth_required
@safe_route
def api_order_invoice_pdf_download(oid):
    """Download the generated invoice PDF."""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return "Order not found", 404
    inv = order.get("draft_invoice", {})
    pdf_path = inv.get("pdf_path", "")
    if not pdf_path or not os.path.exists(pdf_path):
        return "Invoice PDF not generated yet", 404
    return send_file(pdf_path, as_attachment=True, download_name=os.path.basename(pdf_path))


# ═══════════════════════════════════════════════════════════════════════
# Supplier Link Auto-Lookup (#6) — search Amazon/Grainger/Uline by part#
# ═══════════════════════════════════════════════════════════════════════

def _build_supplier_urls(part_number: str, description: str = "") -> list:
    """Build direct search URLs for major suppliers from a part number or description."""
    urls = []
    query = part_number or description[:60]
    if not query:
        return urls
    from urllib.parse import quote_plus
    q = quote_plus(query)

    # Amazon — ASIN shortcut or search
    if part_number and (part_number.startswith("B0") or part_number.startswith("b0")):
        urls.append({"supplier": "Amazon", "url": f"https://www.amazon.com/dp/{part_number}", "type": "direct"})
    else:
        urls.append({"supplier": "Amazon", "url": f"https://www.amazon.com/s?k={q}", "type": "search"})

    # Grainger
    urls.append({"supplier": "Grainger", "url": f"https://www.grainger.com/search?searchQuery={q}", "type": "search"})
    # Uline
    urls.append({"supplier": "Uline", "url": f"https://www.uline.com/BL/Search?keywords={q}", "type": "search"})
    # Staples
    urls.append({"supplier": "Staples", "url": f"https://www.staples.com/search?query={q}", "type": "search"})
    # Global Industrial
    urls.append({"supplier": "Global Industrial", "url": f"https://www.globalindustrial.com/g/search?q={q}", "type": "search"})

    return urls


@bp.route("/api/order/<oid>/lookup-suppliers", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_order_lookup_suppliers(oid):
    """Auto-lookup supplier links + prices for all line items with part numbers.
    POST: {line_id: 'L001'} for single item, or {} for all items.
    Uses product_research.research_product() for Amazon SerpApi lookup.
    """
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})

    data = request.get_json(force=True, silent=True) or {}
    target_lid = data.get("line_id", "")  # empty = all items

    results = []
    updated = 0

    for it in order.get("line_items", []):
        lid = it.get("line_id", "")
        if target_lid and lid != target_lid:
            continue

        pn = it.get("part_number", "")
        desc = it.get("description", "")
        if not pn and not desc:
            continue

        # Already has supplier link? Skip unless forced
        if it.get("supplier_url") and not data.get("force"):
            results.append({"line_id": lid, "status": "already_linked", "supplier_url": it["supplier_url"]})
            continue

        # Try research_product for Amazon SerpApi lookup
        item_result = {"line_id": lid, "part_number": pn, "description": desc[:60]}
        try:
            from src.agents.product_research import research_product
            research = research_product(item_number=pn, description=desc)
            if research.get("found"):
                item_result["amazon"] = {
                    "price": research["price"],
                    "title": research.get("title", ""),
                    "url": research.get("url", ""),
                    "asin": research.get("asin", ""),
                }
                # Auto-populate if no supplier set
                if not it.get("supplier_url"):
                    it["supplier_url"] = research.get("url", "")
                    it["supplier"] = "Amazon"
                if not it.get("cost") and research.get("price"):
                    it["cost"] = research["price"]
                    sell = it.get("unit_price", 0) or 0
                    if sell > 0:
                        it["margin_pct"] = round((sell - research["price"]) / sell * 100, 1)
                updated += 1
                item_result["status"] = "found"
            else:
                item_result["status"] = "not_found"
        except Exception as e:
            item_result["status"] = "error"
            item_result["error"] = str(e)[:100]
            log.debug("Supplier lookup error for %s: %s", pn or desc[:30], e)

        # Always add search URLs as fallback
        item_result["search_urls"] = _build_supplier_urls(pn, desc)
        results.append(item_result)

    if updated > 0:
        order["updated_at"] = datetime.now().isoformat()
        _save_single_order(oid, order)

    return jsonify({"ok": True, "results": results, "updated": updated, "total": len(results)})


@bp.route("/api/supplier/search")
@auth_required
@safe_route
def api_supplier_search():
    """Quick supplier URL lookup for a part number or description. GET: ?q=B0xxx or ?q=nitrile+gloves"""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"ok": False, "error": "?q= parameter required"})

    urls = _build_supplier_urls(query)

    # Also try Amazon API if available
    amazon_result = None
    try:
        from src.agents.product_research import research_product
        research = research_product(item_number=query, description=query)
        if research.get("found"):
            amazon_result = {
                "price": research["price"],
                "title": research.get("title", ""),
                "url": research.get("url", ""),
                "asin": research.get("asin", ""),
                "source": research.get("source", ""),
            }
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    return jsonify({"ok": True, "query": query, "search_urls": urls, "amazon": amazon_result})


# ═══════════════════════════════════════════════════════════════════════
# Quote → Order Auto-Link (#11) — link PO to existing quote
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/order/<oid>/link-quote", methods=["POST"])
@auth_required
@safe_route
def api_order_link_quote(oid):
    """Link an order to a quote. Auto-populates line items with quote prices/suppliers.
    POST: {quote_number} or {} to auto-detect from PO number.
    """
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})

    data = request.get_json(force=True, silent=True) or {}
    qn = data.get("quote_number", "")

    # Auto-detect: try to find matching quote by PO reference
    if not qn:
        qn = _auto_find_quote_for_order(order)
        if not qn:
            return jsonify({"ok": False, "error": "No matching quote found. Provide quote_number."})

    # Load quote data
    quote_data = None
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute("SELECT * FROM quotes WHERE quote_number=?", (qn,)).fetchone()
            if row:
                quote_data = dict(row)
                items_detail = json.loads(row["items_detail"] or "[]") if row["items_detail"] else []
                line_items_raw = json.loads(row["line_items"] or "[]") if row["line_items"] else []
                quote_data["items_detail"] = items_detail or line_items_raw
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    if not quote_data:
        # Fallback: search via get_all_quotes() (SQLite-primary)
        try:
            from src.forms.quote_generator import get_all_quotes
            for q in get_all_quotes():
                if q.get("quote_number") == qn:
                    quote_data = q
                    break
        except Exception as _e:
            log.debug("Quote fallback search: %s", _e)

    if not quote_data:
        return jsonify({"ok": False, "error": f"Quote {qn} not found"})

    # Enrich order line items from quote
    quote_items = quote_data.get("items_detail", [])
    enriched = 0
    for oi in order.get("line_items", []):
        oi_desc = (oi.get("description", "") or "").lower()[:30]
        oi_pn = (oi.get("part_number", "") or "").lower()
        for qi in quote_items:
            qi_desc = (qi.get("description", "") or qi.get("name", "")).lower()[:30]
            qi_pn = (qi.get("part_number", "") or qi.get("sku", "")).lower()

            matched = False
            if oi_pn and qi_pn and oi_pn == qi_pn:
                matched = True
            elif oi_desc and qi_desc and oi_desc == qi_desc:
                matched = True

            if matched:
                if not oi.get("unit_price"):
                    oi["unit_price"] = qi.get("unit_price", 0) or qi.get("our_price", 0) or qi.get("price", 0)
                if not oi.get("cost"):
                    oi["cost"] = qi.get("cost", 0) or qi.get("supplier_price", 0)
                if not oi.get("supplier"):
                    oi["supplier"] = qi.get("supplier", "")
                if not oi.get("supplier_url"):
                    oi["supplier_url"] = qi.get("supplier_url", "") or qi.get("url", "")
                # Recalculate margin
                sell = oi.get("unit_price", 0) or 0
                cost = oi.get("cost", 0) or 0
                if sell > 0 and cost > 0:
                    oi["margin_pct"] = round((sell - cost) / sell * 100, 1)
                oi["extended"] = round((oi.get("qty", 0) or 1) * sell, 2)
                enriched += 1
                break

    order["quote_number"] = qn
    order["agency"] = order.get("agency") or quote_data.get("agency", "")
    order["institution"] = order.get("institution") or quote_data.get("institution", "") or quote_data.get("ship_to_name", "")
    order["ship_to_name"] = order.get("ship_to_name") or quote_data.get("ship_to_name", "")
    order["total"] = order.get("total") or quote_data.get("total", 0)
    order["updated_at"] = datetime.now().isoformat()
    _save_single_order(oid, order)

    # Record pricing intelligence
    try:
        from src.knowledge.pricing_intel import record_winning_prices
        record_winning_prices(order)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    _log_crm_activity(qn, "order_linked",
                      f"Order {oid} linked to quote {qn} — enriched {enriched} items",
                      actor="user", metadata={"order_id": oid, "quote": qn, "enriched": enriched})

    return jsonify({"ok": True, "quote_number": qn, "enriched": enriched,
                     "total_items": len(order.get("line_items", []))})


def _auto_find_quote_for_order(order: dict) -> str:
    """Try to auto-detect which quote an order belongs to.
    Checks: PO number references, institution match, total match, date proximity.
    """
    po = order.get("po_number", "")
    inst = (order.get("institution", "") or "").lower()
    total = order.get("total", 0)

    try:
        from src.core.db import get_db
        with get_db() as conn:
            # First: check if PO is referenced in any quote
            if po:
                row = conn.execute("SELECT quote_number FROM quotes WHERE po_number=? LIMIT 1", (po,)).fetchone()
                if row:
                    return row["quote_number"]

            # Second: check quotes with matching institution + similar total (within 10%)
            if inst and total > 0:
                rows = conn.execute("""
                    SELECT quote_number, total, institution FROM quotes 
                    WHERE is_test=0 AND status IN ('sent','pending','won') 
                    ORDER BY created_at DESC LIMIT 50
                """).fetchall()
                for r in rows:
                    q_inst = (r["institution"] or "").lower()
                    q_total = r["total"] or 0
                    if q_inst and inst in q_inst or q_inst in inst:
                        if q_total > 0 and abs(q_total - total) / q_total < 0.10:
                            return r["quote_number"]
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    return ""


@bp.route("/api/order/<oid>/delete", methods=["POST"])
@auth_required
@safe_route
def api_order_delete(oid):
    """Delete/dismiss a duplicate or erroneous order. POST: {reason}"""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(force=True, silent=True) or {}
    reason = data.get("reason", "duplicate")

    # Log before deleting
    _log_crm_activity(order.get("quote_number", "") or order.get("po_number", ""),
                      "order_deleted",
                      f"Order {oid} deleted. Reason: {reason}. PO: {order.get('po_number','')} Total: ${order.get('total',0):,.2f}",
                      actor="user", metadata={"order_id": oid, "reason": reason})

    from src.core.order_dal import delete_order as _delete_order
    _delete_order(oid, actor="user", reason=reason)
    log.info("Order %s deleted. Reason: %s", oid, reason)
    return jsonify({"ok": True, "deleted": oid, "reason": reason})


@bp.route("/api/order/<oid>/reply-all")
@auth_required
@safe_route
def api_order_reply_all(oid):
    """Draft PO confirmation reply-all email → saved to outbox for review + send."""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        flash("Order not found", "error")
        return redirect("/orders")

    qn = order.get("quote_number", "")
    institution = order.get("institution", "")
    po_num = order.get("po_number", "")
    total = order.get("total", 0)
    subtotal = order.get("subtotal", 0) or total
    tax = order.get("tax", 0)
    items = order.get("line_items", [])
    sender_email = order.get("sender_email", "")

    # Build items table HTML
    items_html = ""
    for it in items[:20]:
        items_html += f"""<tr>
         <td style="padding:4px 8px;border-bottom:1px solid #eee;font-size:13px">{it.get('description','')[:65]}</td>
         <td style="padding:4px 8px;border-bottom:1px solid #eee;text-align:center;font-size:13px">{it.get('qty',0)}</td>
         <td style="padding:4px 8px;border-bottom:1px solid #eee;text-align:right;font-size:13px">${it.get('unit_price',0):,.2f}</td>
         <td style="padding:4px 8px;border-bottom:1px solid #eee;text-align:right;font-size:13px">${it.get('extended',0):,.2f}</td>
        </tr>"""

    # Plain text body
    items_plain = "\n".join(
        f"  - {it.get('description','')[:60]} (Qty {it.get('qty',0)}) — ${it.get('extended',0):,.2f}"
        for it in items[:20]
    )

    po_display = f"PO {po_num}" if po_num else "your purchase order"
    subject = f"RE: PO Distribution: {po_num}" if po_num else f"RE: Order Confirmation — {institution}"

    body_plain = f"""Hello,

This email confirms receipt of {po_display} for {institution}.

Order Summary:
{items_plain}

Subtotal: ${subtotal:,.2f}
Tax: ${tax:,.2f}
Total: ${total:,.2f}

We will begin processing this order immediately and provide tracking information as items ship.

Should you have any questions, please don't hesitate to reach out.

Respectfully,

Michael Guadan
Reytech Inc.
30 Carnoustie Way, Trabuco Canyon, CA 92679
949-229-1575
sales@reytechinc.com
SB/DVBE Cert #2002605"""

    # HTML body with styled table + signature
    body_html = f"""<div style="font-family:'Segoe UI',Arial,sans-serif;font-size:14px;color:#222;line-height:1.6">
<p>Hello,</p>
<p>This email confirms receipt of <strong>{po_display}</strong> for <strong>{institution}</strong>.</p>

<table style="border-collapse:collapse;width:100%;max-width:600px;margin:16px 0;font-family:'Segoe UI',Arial,sans-serif">
 <thead>
  <tr style="background:#f5f5f5">
   <th style="padding:6px 8px;text-align:left;border-bottom:2px solid #ddd;font-size:14px">Description</th>
   <th style="padding:6px 8px;text-align:center;border-bottom:2px solid #ddd;font-size:14px">Qty</th>
   <th style="padding:6px 8px;text-align:right;border-bottom:2px solid #ddd;font-size:14px">Unit Price</th>
   <th style="padding:6px 8px;text-align:right;border-bottom:2px solid #ddd;font-size:14px">Extended</th>
  </tr>
 </thead>
 <tbody>
  {items_html}
 </tbody>
 <tfoot>
  <tr><td colspan="3" style="padding:4px 8px;text-align:right;font-weight:600;font-size:13px">Subtotal:</td>
      <td style="padding:4px 8px;text-align:right;font-size:13px">${subtotal:,.2f}</td></tr>
  <tr><td colspan="3" style="padding:4px 8px;text-align:right;font-weight:600;font-size:13px">Tax:</td>
      <td style="padding:4px 8px;text-align:right;font-size:13px">${tax:,.2f}</td></tr>
  <tr style="border-top:2px solid #333"><td colspan="3" style="padding:6px 8px;text-align:right;font-weight:700;font-size:14px">Total:</td>
      <td style="padding:6px 8px;text-align:right;font-weight:700;font-size:14px;color:#2563eb">${total:,.2f}</td></tr>
 </tfoot>
</table>

<p>We will begin processing this order immediately and provide tracking information as items ship.</p>
<p>Should you have any questions, please don't hesitate to reach out.</p>

<br>
<div style="border-top:1px solid #ddd;padding-top:12px;margin-top:12px">
 <table cellpadding="0" cellspacing="0" style="font-family:'Segoe UI',Arial,sans-serif">
  <tr>
   <td style="padding-right:16px;vertical-align:top">
    <img src="https://reytechinc.com/logo.png" alt="Reytech Inc." style="width:80px;height:auto" onerror="this.style.display='none'">
   </td>
   <td style="vertical-align:top">
    <div style="font-weight:700;font-size:14px;color:#1a1a2e">Michael Guadan</div>
    <div style="font-size:14px;color:#666">Reytech Inc.</div>
    <div style="font-size:14px;color:#666">30 Carnoustie Way, Trabuco Canyon, CA 92679</div>
    <div style="font-size:14px;margin-top:4px">
     <a href="tel:9492291575" style="color:#2563eb;text-decoration:none">949-229-1575</a> |
     <a href="mailto:sales@reytechinc.com" style="color:#2563eb;text-decoration:none">sales@reytechinc.com</a>
    </div>
    <div style="font-size:14px;color:#888;margin-top:2px">SB/DVBE Cert #2002605 · <a href="https://reytechinc.com" style="color:#2563eb;text-decoration:none">reytechinc.com</a></div>
   </td>
  </tr>
 </table>
</div>
</div>"""

    # Find original email thread info from processed emails
    in_reply_to = ""
    references = ""
    original_cc = ""
    try:
        from src.core.paths import DATA_DIR as _dd
        processed_path = os.path.join(_dd, "processed_emails.json")
        if os.path.exists(processed_path):
            with open(processed_path) as f:
                processed = json.load(f)
            # Find the PO email by PO number in subject
            for uid, info in processed.items():
                subj = info.get("subject", "")
                if po_num and po_num in subj:
                    in_reply_to = info.get("message_id", "")
                    references = info.get("references", "")
                    original_cc = info.get("cc", "")
                    if not subject.lower().startswith("re:"):
                        subject = f"Re: {subj}"
                    break
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Build CC list from original thread
    cc_addrs = set()
    if original_cc:
        import re as _re
        cc_addrs.update(_re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', original_cc))
    # Remove our own address
    cc_addrs = {a for a in cc_addrs if not a.endswith("@reytechinc.com")}

    # Save as DRAFT to outbox
    draft = {
        "id": f"po_confirm_{po_num or oid}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "to": sender_email,
        "cc": ", ".join(cc_addrs),
        "subject": subject,
        "body": body_plain,
        "body_html": body_html,
        "in_reply_to": in_reply_to,
        "references": references,
        "attachments": [],
        "status": "draft",
        "source": "po_confirmation",
        "po_number": po_num,
        "order_id": oid,
        "created_at": datetime.now().isoformat(),
        "priority": "high",
    }

    try:
        from src.core.dal import upsert_outbox_email
        upsert_outbox_email(draft)
    except Exception as e:
        log.error("Failed to save PO confirmation draft: %s", e)
        flash(f"Error saving draft: {e}", "error")
        return redirect(f"/order/{oid}")

    _log_crm_activity(qn, "draft_created",
                      f"PO confirmation draft for {oid} → {sender_email}",
                      actor="user", metadata={"order_id": oid, "po_number": po_num})

    flash(f"📧 PO confirmation draft saved to outbox — review and send from Agents page", "success")
    return redirect(f"/order/{oid}")


# ═══════════════════════════════════════════════════════════════════════
# Supplier Purchase URLs — group by supplier, build cart links
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/order/<oid>/purchase-urls")
@auth_required
@safe_route
def api_order_purchase_urls(oid):
    """Get purchase URLs grouped by supplier for an order."""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    
    groups = build_supplier_purchase_urls(order)
    return jsonify({"ok": True, "suppliers": groups, "order_id": oid})


# ═══════════════════════════════════════════════════════════════════════
# Catalog Learning — orders teach the catalog about suppliers & prices
# ═══════════════════════════════════════════════════════════════════════

# Supplier ordering config — how to build purchase URLs per supplier
# Future ordering agent uses this to auto-generate cart/PO links
SUPPLIER_ORDER_CONFIG = {
    "amazon": {
        "name": "Amazon",
        "cart_url": "https://www.amazon.com/gp/aws/cart/add.html",
        "product_url": "https://amazon.com/dp/{sku}",
        "search_url": "https://amazon.com/s?k={query}",
        "id_field": "asin",   # B0xxxxxxxxx
        "id_pattern": r"^B0[A-Z0-9]{8,}$",
        "supports_bulk_cart": True,
        "account_type": "business",  # Amazon Business for tax exempt
    },
    "grainger": {
        "name": "Grainger",
        "product_url": "https://www.grainger.com/product/{sku}",
        "search_url": "https://www.grainger.com/search?searchQuery={query}",
        "id_field": "grainger_item",
        "id_pattern": r"^\d{3,8}[A-Z]?\d*$",
        "supports_bulk_cart": False,
        "account_type": "business",
    },
    "uline": {
        "name": "Uline",
        "product_url": "https://www.uline.com/{sku}",
        "search_url": "https://www.uline.com/BL/Search?keywords={query}",
        "id_field": "uline_model",
        "id_pattern": r"^[SH]-\d+",
        "supports_bulk_cart": False,
        "account_type": "business",
    },
    "mckesson": {
        "name": "McKesson",
        "search_url": "https://mms.mckesson.com/search?q={query}",
        "id_field": "mck_number",
        "supports_bulk_cart": False,
        "account_type": "medical",
    },
    "medline": {
        "name": "Medline",
        "search_url": "https://www.medline.com/search?q={query}",
        "id_field": "medline_number",
        "supports_bulk_cart": False,
        "account_type": "medical",
    },
    "cardinal_health": {
        "name": "Cardinal Health",
        "id_field": "cardinal_cat",
        "supports_bulk_cart": False,
        "account_type": "medical",
    },
}


def _detect_supplier(part_number: str, supplier_url: str = "", supplier_name: str = "") -> str:
    """Detect which supplier an item belongs to based on PN/URL/name."""
    import re
    pn = (part_number or "").strip()
    url = (supplier_url or "").lower()
    name = (supplier_name or "").lower()
    
    # URL-based detection
    if "amazon" in url: return "amazon"
    if "grainger" in url: return "grainger"
    if "uline" in url: return "uline"
    if "mckesson" in url or "mms.mckesson" in url: return "mckesson"
    if "medline" in url: return "medline"
    if "cardinal" in url: return "cardinal_health"
    
    # Name-based
    if "amazon" in name: return "amazon"
    if "grainger" in name: return "grainger"
    if "uline" in name: return "uline"
    if "mckesson" in name: return "mckesson"
    if "medline" in name: return "medline"
    if "cardinal" in name: return "cardinal_health"
    
    # Part number pattern detection
    for key, cfg in SUPPLIER_ORDER_CONFIG.items():
        pattern = cfg.get("id_pattern")
        if pattern and pn and re.match(pattern, pn, re.IGNORECASE):
            return key
    
    return ""


def _learn_supplier_from_order_line(line_item: dict, order: dict):
    """When an order line item has supplier info, teach the catalog.
    
    This is called when:
    1. User updates supplier/URL on a line item
    2. Order is created from quote with supplier data
    3. Bulk order completes and items have confirmed suppliers
    
    The catalog learns:
    - Which suppliers carry this product
    - What price was paid (cost tracking)
    - The supplier URL for re-ordering
    - Reliability (based on delivery success)
    """
    from src.agents.product_catalog import (
        match_item, add_supplier_price, add_to_catalog, record_catalog_quote
    )
    
    desc = line_item.get("description", "")
    pn = line_item.get("part_number", "")
    supplier = line_item.get("supplier", "")
    supplier_url = line_item.get("supplier_url", "")
    cost = line_item.get("cost", 0) or line_item.get("unit_price", 0)
    qty = line_item.get("qty", 0)
    
    if not desc and not pn:
        return
    
    # Detect supplier key
    supplier_key = _detect_supplier(pn, supplier_url, supplier)
    supplier_name = supplier or (SUPPLIER_ORDER_CONFIG.get(supplier_key, {}).get("name", ""))
    
    if not supplier_name:
        return  # Can't learn without knowing the supplier
    
    # Match to existing catalog product (or create new entry)
    matches = match_item(desc, part_number=pn)
    product_id = matches[0].get("product_id") if matches else None
    
    if not product_id and pn:
        # Try to add to catalog as new product
        try:
            product_id = add_to_catalog(
                description=desc[:200],
                part_number=pn,
                cost=cost,
                sell_price=line_item.get("unit_price", 0),
                supplier_url=supplier_url,
                supplier_name=supplier_name,
                uom="EA",
                source="order_sourcing",
            )
        except Exception:
            pass
    
    if product_id and cost > 0:
        # Record this supplier + price in product_suppliers
        add_supplier_price(
            product_id=product_id,
            supplier_name=supplier_name,
            price=cost,
            url=supplier_url,
            sku=pn,
            in_stock=True,
        )
        
        # Record in price history for trend analysis
        record_catalog_quote(
            product_id=product_id,
            price_type="cost",
            price=cost,
            quantity=qty,
            source="order_sourcing",
            agency=order.get("agency", ""),
            institution=order.get("institution", ""),
            quote_number=order.get("quote_number", ""),
            supplier_url=supplier_url,
        )
        
        log.info("Catalog learned: %s → %s @ $%.2f (product_id=%s)",
                 pn or desc[:40], supplier_name, cost, product_id)


def learn_from_completed_order(oid: str):
    """When an order reaches delivered/closed, learn from ALL its line items.
    
    Called from _update_order_status when status changes to delivered/closed.
    Updates supplier reliability based on delivery performance.
    """
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return
    
    learned = 0
    for it in order.get("line_items", []):
        supplier = it.get("supplier", "")
        if supplier:
            try:
                _learn_supplier_from_order_line(it, order)
                learned += 1
                
                # Update reliability if delivered successfully
                if it.get("sourcing_status") == "delivered":
                    try:
                        from src.agents.product_catalog import (
                            match_item, update_supplier_reliability
                        )
                        matches = match_item(
                            it.get("description", ""),
                            part_number=it.get("part_number", "")
                        )
                        if matches and matches[0].get("product_id"):
                            update_supplier_reliability(matches[0]["product_id"], supplier,
                                         reliability=0.8,  # Confirmed delivery
                                         notes=f"Delivered on order {oid}")
                    except Exception:
                        pass
            except Exception as _e:
                log.debug("Learn from order line: %s", _e)
    
    if learned:
        log.info("Order %s completed: taught catalog %d supplier records", oid, learned)


def build_supplier_purchase_urls(order: dict) -> dict:
    """Group order items by supplier and generate purchase URLs.
    
    Returns: {
        "amazon": {
            "name": "Amazon",
            "items": [...],
            "cart_url": "https://amazon.com/gp/aws/cart/add.html?ASIN.1=...",
            "total_items": 5,
            "total_cost": 234.50,
        },
        "grainger": {
            "name": "Grainger", 
            "items": [...],
            "search_urls": ["https://grainger.com/..."],
            "total_items": 2,
            "total_cost": 89.00,
        },
        "unknown": {
            "items": [...],  # Items without identified supplier
        }
    }
    """
    import urllib.parse
    
    groups = {}
    for it in order.get("line_items", []):
        if it.get("sourcing_status") not in ("pending", "ordered"):
            continue  # Skip already shipped/delivered
        
        pn = it.get("part_number", "")
        supplier_key = _detect_supplier(pn, it.get("supplier_url", ""), it.get("supplier", ""))
        if not supplier_key:
            supplier_key = "unknown"
        
        if supplier_key not in groups:
            cfg = SUPPLIER_ORDER_CONFIG.get(supplier_key, {})
            groups[supplier_key] = {
                "name": cfg.get("name", supplier_key.title()),
                "items": [],
                "total_items": 0,
                "total_cost": 0,
            }
        
        groups[supplier_key]["items"].append(it)
        groups[supplier_key]["total_items"] += 1
        groups[supplier_key]["total_cost"] += it.get("cost", 0) or it.get("unit_price", 0) * it.get("qty", 0)
    
    # Generate purchase URLs per supplier
    for key, group in groups.items():
        cfg = SUPPLIER_ORDER_CONFIG.get(key, {})
        
        if key == "amazon" and cfg.get("supports_bulk_cart"):
            # Amazon bulk cart URL
            params = []
            for i, it in enumerate(group["items"]):
                asin = it.get("part_number", "")
                if asin and (asin.startswith("B0") or len(asin) == 10):
                    params.append(f"ASIN.{i+1}={asin}")
                    params.append(f"Quantity.{i+1}={it.get('qty', 1)}")
            if params:
                group["cart_url"] = f"{cfg['cart_url']}?{'&'.join(params)}"
        
        # Search URLs for all suppliers
        search_tmpl = cfg.get("search_url", "")
        if search_tmpl:
            group["search_urls"] = []
            for it in group["items"]:
                q = it.get("part_number", "") or it.get("description", "")[:40]
                group["search_urls"].append(
                    search_tmpl.format(query=urllib.parse.quote_plus(q))
                )
    
    return groups



# ─── Order Health + Digest API Routes ──────────────────────────────────────

@bp.route("/api/qb/health")
@auth_required
@safe_route
def api_qb_health():
    """Check QuickBooks API connectivity and auth status."""
    try:
        from src.agents.quickbooks_agent import (
            is_configured, QB_CLIENT_ID, QB_REALM_ID, QB_SANDBOX,
        )
        result = {
            "configured": is_configured(),
            "client_id": QB_CLIENT_ID[:8] + "..." if QB_CLIENT_ID else "(not set)",
            "realm_id": QB_REALM_ID or "(not set)",
            "sandbox": QB_SANDBOX,
        }
        if is_configured():
            try:
                from src.agents.quickbooks_agent import _qb_query
                customers = _qb_query("SELECT COUNT(*) FROM Customer")
                result["connected"] = True
                result["customer_count"] = customers[0] if customers else "?"
            except Exception as e:
                result["connected"] = False
                result["auth_error"] = str(e)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/drive/health")
@auth_required
@safe_route
def api_drive_health():
    """Check Google Drive integration status and backup health."""
    try:
        from src.core.gdrive import GOOGLE_DRIVE_ROOT_FOLDER_ID
        from src.agents.drive_backup import get_backup_health
        health = get_backup_health()
        health["root_folder_id"] = GOOGLE_DRIVE_ROOT_FOLDER_ID[:12] + "..." if GOOGLE_DRIVE_ROOT_FOLDER_ID else "(not set)"
        return jsonify({"ok": True, **health})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/drive/backup-now", methods=["POST", "GET"])
@auth_required
@safe_route
def api_drive_backup_now():
    """Trigger an immediate backup to Google Drive."""
    try:
        from src.agents.drive_backup import run_nightly_backup
        result = run_nightly_backup(force=True)
        return jsonify(result)
    except Exception as e:
        import traceback
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/drive/restore", methods=["POST"])
@auth_required
@safe_route
def api_drive_restore():
    """Manually trigger disaster recovery from Drive backup."""
    try:
        from src.agents.drive_backup import check_and_restore
        result = check_and_restore()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/drive/search")
@auth_required
@safe_route
def api_drive_search():
    """Search the local Drive file index."""
    q = request.args.get("q", "")
    if not q:
        return jsonify({"ok": False, "error": "No query"})
    try:
        from src.core.gdrive import search_index
        results = search_index(q)
        return jsonify({"ok": True, "results": results, "count": len(results)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/orders/health")
@auth_required
@safe_route
def api_orders_health():
    """Return full order health report for dashboard."""
    try:
        from src.agents.order_digest import get_order_health
        return jsonify(get_order_health())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/orders/digest", methods=["GET", "POST"])
@auth_required
@safe_route
def api_orders_digest():
    """Trigger daily digest manually (always sends, bypasses daily limit)."""
    try:
        from src.agents.order_digest import run_daily_digest
        result = run_daily_digest(force=True)
        return jsonify(result)
    except Exception as e:
        import traceback
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/orders/context/<po_number>")
@auth_required
@safe_route
def api_order_context(po_number):
    """Get rich order context (used by CS agent and order status pages)."""
    try:
        from src.agents.order_digest import get_order_context_for_cs
        return jsonify(get_order_context_for_cs(po_number=po_number))
    except Exception as e:
        return jsonify({"found": False, "error": str(e)})


@bp.route("/api/orders/test-sms")
@auth_required
@safe_route
def api_test_sms():
    """Test SMS delivery directly — no async, returns full error."""
    try:
        from src.agents.notify_agent import _send_sms, TWILIO_SID, TWILIO_TOKEN, TWILIO_FROM, NOTIFY_PHONE
        diag = {
            "twilio_sid": TWILIO_SID[:8] + "..." if TWILIO_SID else "(not set)",
            "twilio_token": TWILIO_TOKEN[:4] + "..." if TWILIO_TOKEN else "(not set)",
            "twilio_from": TWILIO_FROM or "(not set)",
            "notify_phone": NOTIFY_PHONE or "(not set)",
        }
        result = _send_sms("Test from Reytech", "If you see this, SMS is working!", {})
        return jsonify({"diag": diag, "sms_result": result})
    except Exception as e:
        import traceback
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# Orders Diagnostic — Debug phantom order issues
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/orders/diagnostic")
@auth_required
@safe_route
def api_orders_diagnostic():
    """Full diagnostic of all order data sources.
    Shows: SQLite orders, DB orders via DAL, what shows as urgent, and why.
    """
    import json
    results = {"ok": True, "orders_sqlite": [], "db_orders": [], "urgent_analysis": []}

    # 1. SQLite orders (single source of truth)
    try:
        all_orders = _load_orders()
        for oid, o in all_orders.items():
            items = o.get("line_items", [])
            has_real_items = any(
                (li.get("description", "") or "").strip() or (li.get("part_number", "") or "").strip()
                for li in items
            )
            entry = {
                "order_id": oid,
                "status": o.get("status"),
                "total": o.get("total", 0),
                "po_number": o.get("po_number"),
                "quote_number": o.get("quote_number"),
                "institution": o.get("institution"),
                "source": o.get("source", ""),
                "line_items_count": len(items),
                "has_real_items": has_real_items,
                "is_test": o.get("is_test"),
                "created_at": o.get("created_at"),
            }
            results["orders_sqlite"].append(entry)

            # Urgent analysis
            if o.get("status") == "new":
                is_test = ("TEST" in (o.get("po_number", "") or "").upper() or o.get("is_test"))
                is_phantom = not has_real_items and (o.get("total", 0) or 0) == 0
                shows_urgent = not is_test and not is_phantom and \
                               o.get("status") not in ("cancelled", "test", "deleted")
                results["urgent_analysis"].append({
                    "order_id": oid,
                    "total": o.get("total", 0),
                    "has_real_items": has_real_items,
                    "is_test": is_test,
                    "is_phantom": is_phantom,
                    "would_show_urgent": shows_urgent,
                    "reason": "SHOWS" if shows_urgent else
                              ("filtered: test" if is_test else
                               "filtered: phantom ($0 + no items)" if is_phantom else
                               "filtered: status"),
                })
    except Exception as e:
        results["orders_sqlite_error"] = str(e)

    # 2. DB orders via DAL
    try:
        from src.core.dal import list_orders as _dal_list_orders
        for d in _dal_list_orders(limit=20):
            results["db_orders"].append({
                "id": d.get("id"),
                "status": d.get("status"),
                "total": d.get("total"),
                "po_number": d.get("po_number"),
                "created_at": d.get("created_at"),
            })
    except Exception as e:
        results["db_orders_error"] = str(e)

    results["summary"] = {
        "total_orders_sqlite": len(results["orders_sqlite"]),
        "total_db_orders": len(results["db_orders"]),
        "showing_urgent": sum(1 for a in results["urgent_analysis"] if a["would_show_urgent"]),
        "filtered_phantom": sum(1 for a in results["urgent_analysis"] if a.get("is_phantom")),
        "filtered_test": sum(1 for a in results["urgent_analysis"] if a.get("is_test")),
    }
    return jsonify(results)


# ─── QuickBooks Integration for Orders ─────────────────────────────────────

@bp.route("/api/order/<oid>/items")
@auth_required
@safe_route
def api_order_items(oid):
    """Return order line items for PO Builder."""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    items = order.get("line_items", [])
    return jsonify({"ok": True, "items": items, "count": len(items)})


@bp.route("/api/order/<oid>/create-qb-po", methods=["POST"])
@auth_required
@safe_route
def api_order_create_qb_po(oid):
    """Create QB Purchase Orders from order items grouped by vendor."""
    try:
        from src.agents.quickbooks_agent import is_configured, create_purchase_order
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})

        from src.core.order_dal import get_order as _get_order
        order = _get_order(oid)
        if not order:
            return jsonify({"ok": False, "error": "Order not found"})

        data = request.get_json(force=True, silent=True) or {}
        vendor_groups = data.get("vendor_groups", {})
        if not vendor_groups:
            return jsonify({"ok": False, "error": "No vendor groups provided"})

        ship_to = order.get("ship_to", "") or order.get("institution", "")

        created = []
        failed = []
        for vendor_id, group in vendor_groups.items():
            vendor_name = group.get("name", "Unknown")
            items = group.get("items", [])
            if not items:
                continue
            result = create_purchase_order(
                vendor_id=vendor_id,
                items=items,
                memo=f"Reytech Order {oid} — {order.get('institution', '')} — PO: {order.get('po_number', '')}",
                ship_to=ship_to,
                po_number=order.get("po_number", "")[:20],
            )
            if result and result.get("qb_id"):
                created.append({"supplier": vendor_name, "qb_id": result["qb_id"],
                               "doc_number": result.get("doc_number", ""), "total": result.get("total", 0)})
            else:
                failed.append({"supplier": vendor_name, "error": "QB API error"})

        # Store on order
        if created:
            order.setdefault("qb_pos", []).extend(created)
            _save_single_order(oid, order)

        return jsonify({"ok": True, "created": created, "failed": failed,
                       "message": f"Created {len(created)} PO(s)" + (f", {len(failed)} failed" if failed else "")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})



@bp.route("/api/order/<oid>/create-qb-invoice", methods=["POST"])
@auth_required
@safe_route
def api_order_create_qb_invoice(oid):
    """Create QB invoice and have QB email it to sales@reytechinc.com.
    
    Flow: Create in QB → QB emails PDF to us → we poll, enhance, forward.
    QB does NOT email the customer. We do, after adding UOM + PO#.
    """
    try:
        from src.agents.quickbooks_agent import (
            is_configured, create_invoice, find_customer, create_customer,
            send_invoice_email,
        )
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})

        from src.core.order_dal import get_order as _get_order
        order = _get_order(oid)
        if not order:
            return jsonify({"ok": False, "error": "Order not found"})

        institution = order.get("institution", "")

        # Check for selected line items (partial invoice)
        data = request.get_json(force=True, silent=True) or {}
        selected_ids = data.get("line_ids")  # None = all items

        # Find or create customer in QB
        customer = find_customer(institution) if institution else None
        if not customer:
            customer = create_customer(name=institution)
            if not customer:
                return jsonify({"ok": False,
                    "error": f"Customer '{institution}' not in QB. Add them in QuickBooks first."})

        # Build line items — filter by selected_ids if provided
        inv_items = []
        all_items = order.get("line_items", [])
        for it in all_items:
            # Skip if line_ids specified and this item not selected
            if selected_ids is not None:
                lid = it.get("line_id", "")
                if lid not in selected_ids:
                    continue
            
            price = it.get("unit_price") or it.get("sell_price") or it.get("price") or 0
            price = float(price) if price else 0
            if price <= 0:
                continue
            mfg = it.get("part_number", "") or it.get("mfg_number", "") or ""
            desc = it.get("description", "") or ""
            if mfg and not desc.startswith(mfg):
                desc = f"{mfg}\n{desc}"
            inv_items.append({
                "description": desc,
                "qty": int(it.get("qty", 1)),
                "unit_price": price,
                "uom": it.get("uom", "EA") or "EA",
                "mfg_number": mfg,
            })

        if not inv_items:
            return jsonify({"ok": False, "error": "No items with sell prices"})

        # Create invoice in QB
        result = create_invoice(
            customer_id=customer["Id"],
            items=inv_items,
            po_number=order.get("po_number", ""),
        )
        if not result:
            return jsonify({"ok": False, "error": "QB invoice creation failed"})

        # Have QB email the invoice to sales@reytechinc.com (NOT customer)
        our_email = os.environ.get("GMAIL_ADDRESS", "sales@reytechinc.com")
        email_sent = send_invoice_email(result["id"], to_email=our_email)

        # Store on order — items with UOM for later PDF enhancement
        order["qb_invoice_id"] = result["id"]
        order["qb_invoice_number"] = result.get("doc_number", "")
        order["qb_invoice_total"] = result.get("total", 0)
        order["qb_invoice_due"] = result.get("due_date", "")
        order["invoice_status"] = "created"
        order["invoice_items_uom"] = [{
            "mfg": it.get("mfg_number", ""),
            "uom": it.get("uom", "EA"),
            "qty": it.get("qty", 1),
        } for it in inv_items]
        order["invoice_po_number"] = order.get("po_number", "")

        # Mark invoiced items on order line_items
        invoiced_descs = set(it.get("description", "")[:50] for it in inv_items)
        for it in all_items:
            lid = it.get("line_id", "")
            desc = it.get("description", "")[:50]
            if selected_ids is not None:
                if lid in selected_ids:
                    it["invoice_status"] = "invoiced"
                    it["qb_invoice_number"] = result.get("doc_number", "")
            else:
                if desc in invoiced_descs:
                    it["invoice_status"] = "invoiced"
                    it["qb_invoice_number"] = result.get("doc_number", "")

        if email_sent:
            order["invoice_status"] = "awaiting_email"
            log.info("QB Invoice #%s created, emailed to %s. Awaiting pickup.",
                     result.get("doc_number"), our_email)

        _save_single_order(oid, order)

        return jsonify({
            "ok": True,
            "invoice_id": result["id"],
            "invoice_number": result.get("doc_number", ""),
            "total": result.get("total", 0),
            "due_date": result.get("due_date", ""),
            "emailed_to": our_email if email_sent else None,
            "status": order["invoice_status"],
            "items_count": len(inv_items),
            "next_step": "Invoice will arrive in your inbox. App will auto-detect it, add UOM + PO#, then you can send to customer.",
        })
    except Exception as e:
        log.error("QB invoice error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/order/<oid>/send-invoice", methods=["POST"])
@auth_required
@safe_route
def api_order_send_invoice(oid):
    """Send the enhanced invoice (with UOM + PO#) to the customer.
    Called after the invoice PDF has been received and enhanced."""
    try:
        from src.core.order_dal import get_order as _get_order
        order = _get_order(oid)
        if not order:
            return jsonify({"ok": False, "error": "Order not found"})

        invoice_pdf = order.get("invoice_pdf_enhanced") or order.get("invoice_pdf")
        if not invoice_pdf or not os.path.exists(invoice_pdf):
            return jsonify({"ok": False, "error": "Enhanced invoice PDF not found. Wait for QB email to arrive."})

        data = request.get_json(force=True, silent=True) or {}
        to_email = data.get("to_email", "")
        if not to_email:
            return jsonify({"ok": False, "error": "Provide customer email address"})

        # Send via existing email sender
        from src.agents.email_poller import EmailSender
        sender = EmailSender()
        inv_num = order.get("qb_invoice_number", oid)
        po_num = order.get("po_number", "")
        institution = order.get("institution", "")

        subject = f"Invoice #{inv_num} — Reytech Inc."
        if po_num:
            subject += f" (PO: {po_num})"

        body = f"Please find attached Invoice #{inv_num} for {institution}.\n\n"
        if po_num:
            body += f"Reference PO: {po_num}\n"
        body += f"Total: ${order.get('qb_invoice_total', 0):,.2f}\n"
        if order.get("qb_invoice_due"):
            body += f"Due: {order['qb_invoice_due']}\n"
        body += "\nThank you for your business.\n\nReytech Inc.\n(949) 229-1575\nsales@reytechinc.com"

        result = sender.send_email(
            to=to_email,
            subject=subject,
            body=body,
            attachments=[invoice_pdf],
        )

        if result.get("ok"):
            order["invoice_status"] = "sent"
            order["invoice_sent_to"] = to_email
            order["invoice_sent_at"] = __import__("datetime").datetime.now().isoformat()
            _save_single_order(oid, order)
            return jsonify({"ok": True, "sent_to": to_email, "status": "sent"})
        return jsonify({"ok": False, "error": result.get("error", "Email send failed")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/invoices/poll-now", methods=["POST"])
@auth_required
@safe_route
def api_invoices_poll_now():
    """Manually trigger QB invoice email poll."""
    try:
        from src.agents.invoice_processor import poll_for_qb_invoices
        result = poll_for_qb_invoices()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/order/<oid>/download-invoice")
@auth_required
@safe_route
def api_order_download_invoice(oid):
    """Download the enhanced invoice PDF."""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})

    pdf_path = order.get("invoice_pdf_enhanced") or order.get("invoice_pdf") or order.get("invoice_pdf_raw")
    if pdf_path and os.path.exists(pdf_path):
        inv_num = order.get("qb_invoice_number", oid)
        return send_file(pdf_path, as_attachment=True,
                        download_name=f"Invoice_{inv_num}.pdf")

    return jsonify({"ok": False, "error": "Invoice PDF not found. Check if QB email has arrived."})


# ══ Consolidated from routes_features*.py ══════════════════════════════════

import os, json
from datetime import datetime, timedelta
from collections import defaultdict


# ── From routes_features.py ─────────────────────────────────────────────────

@bp.route("/api/pipeline/quote-to-cash", methods=["GET"])
@auth_required
@safe_route
def api_quote_to_cash():
    """Quote-to-cash pipeline: track RFQs from quote through order to payment."""
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")

        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
        except Exception:
            rfqs = {}

        orders = _load_orders()

        stages = {
            "draft": [], "priced": [], "sent": [],
            "ordered": [], "invoiced": [], "paid": []
        }

        for rid, r in rfqs.items():
            status = (r.get("status") or "").lower()
            entry = {
                "id": rid,
                "solicitation": (r.get("solicitation_number") or rid)[:30],
                "agency": r.get("institution", "?"),
                "total": r.get("total_price", 0),
                "created": r.get("created", r.get("received_date", "")),
            }
            if status in ("new", "draft", "inbox"):
                stages["draft"].append(entry)
            elif status == "priced":
                stages["priced"].append(entry)
            elif status in ("sent", "quoted"):
                stages["sent"].append(entry)
            elif status in ("ordered", "won"):
                stages["ordered"].append(entry)

        for oid, o in orders.items():
            entry = {
                "id": oid,
                "solicitation": o.get("po_number", oid)[:30],
                "agency": o.get("institution", o.get("agency", "?")),
                "total": o.get("total", 0),
                "created": o.get("created_at", ""),
            }
            status = (o.get("status") or "").lower()
            if status in ("invoiced",):
                stages["invoiced"].append(entry)
            elif status in ("paid", "closed"):
                stages["paid"].append(entry)
            else:
                stages["ordered"].append(entry)

        totals = {k: sum(e.get("total", 0) for e in v) for k, v in stages.items()}

        return jsonify({
            "ok": True,
            "stages": {k: {"count": len(v), "total": round(totals[k], 2), "items": v[:10]} for k, v in stages.items()},
            "pipeline_total": round(sum(totals.values()), 2),
        })
    except Exception as e:
        log.error("quote-to-cash error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/quotes/stale", methods=["GET"])
@auth_required
@safe_route
def api_stale_quotes():
    """Identify quotes that have gone stale (no activity in 14+ days)."""
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
        except Exception:
            rfqs = {}

        threshold = int(request.args.get("days", 14))
        cutoff = (datetime.now() - timedelta(days=threshold)).strftime("%Y-%m-%d")
        stale = []

        for rid, r in rfqs.items():
            status = (r.get("status") or "").lower()
            if status not in ("sent", "quoted", "priced"):
                continue
            last_activity = r.get("sent_date") or r.get("updated") or r.get("created") or ""
            if last_activity and last_activity[:10] < cutoff:
                days_stale = (datetime.now() - datetime.strptime(last_activity[:10], "%Y-%m-%d")).days
                stale.append({
                    "id": rid,
                    "solicitation": (r.get("solicitation_number") or rid)[:30],
                    "agency": r.get("institution", "?"),
                    "status": status,
                    "last_activity": last_activity[:10],
                    "days_stale": days_stale,
                    "total": r.get("total_price", 0),
                })

        stale.sort(key=lambda x: x["days_stale"], reverse=True)

        return jsonify({
            "ok": True,
            "stale_quotes": stale[:25],
            "count": len(stale),
            "threshold_days": threshold,
            "total_at_risk": round(sum(s.get("total", 0) for s in stale), 2),
        })
    except Exception as e:
        log.error("stale quotes error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pipeline/follow-up-queue", methods=["GET"])
@auth_required
@safe_route
def api_follow_up_queue():
    """Return prioritised follow-up queue from follow_up_state.json."""
    try:
        fu_path = os.path.join(DATA_DIR, "follow_up_state.json")
        try:
            with open(fu_path) as f:
                fu = json.load(f)
        except Exception:
            fu = {}

        today = datetime.now().strftime("%Y-%m-%d")
        queue = []

        for fid, f_data in fu.items():
            if not isinstance(f_data, dict):
                continue
            next_date = f_data.get("next_follow_up", "")
            status = f_data.get("status", "pending")
            if status in ("completed", "cancelled"):
                continue
            overdue = bool(next_date and next_date[:10] <= today)
            queue.append({
                "id": fid,
                "rfq_id": f_data.get("rfq_id", fid),
                "contact": f_data.get("contact", f_data.get("buyer_name", "?")),
                "next_follow_up": next_date[:10] if next_date else "TBD",
                "status": status,
                "overdue": overdue,
                "attempts": f_data.get("attempts", 0),
                "notes": (f_data.get("notes") or "")[:80],
            })

        queue.sort(key=lambda x: (not x["overdue"], x["next_follow_up"] or "9999"))

        return jsonify({
            "ok": True,
            "queue": queue[:30],
            "total": len(queue),
            "overdue_count": len([q for q in queue if q["overdue"]]),
        })
    except Exception as e:
        log.error("follow-up queue error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pipeline/revenue-goal", methods=["GET"])
@auth_required
@safe_route
def api_pipeline_revenue_goal():
    """Track progress toward monthly / quarterly revenue goals."""
    try:
        now = datetime.now()
        month_start = now.replace(day=1).strftime("%Y-%m-%d")
        q_month = ((now.month - 1) // 3) * 3 + 1
        quarter_start = now.replace(month=q_month, day=1).strftime("%Y-%m-%d")

        orders = _load_orders()

        monthly_rev = 0.0
        quarterly_rev = 0.0
        for o in orders.values():
            created = (o.get("created_at") or "")[:10]
            total = o.get("total", 0)
            if isinstance(total, (int, float)):
                if created >= month_start:
                    monthly_rev += total
                if created >= quarter_start:
                    quarterly_rev += total

        monthly_goal = float(request.args.get("monthly_goal", 50000))
        quarterly_goal = float(request.args.get("quarterly_goal", monthly_goal * 3))

        return jsonify({
            "ok": True,
            "monthly": {
                "revenue": round(monthly_rev, 2),
                "goal": monthly_goal,
                "pct": round(monthly_rev / monthly_goal * 100, 1) if monthly_goal > 0 else 0,
                "remaining": round(max(0, monthly_goal - monthly_rev), 2),
            },
            "quarterly": {
                "revenue": round(quarterly_rev, 2),
                "goal": quarterly_goal,
                "pct": round(quarterly_rev / quarterly_goal * 100, 1) if quarterly_goal > 0 else 0,
                "remaining": round(max(0, quarterly_goal - quarterly_rev), 2),
            },
            "period": now.strftime("%B %Y"),
        })
    except Exception as e:
        log.error("revenue goal error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pipeline/conversion-funnel", methods=["GET"])
@auth_required
@safe_route
def api_conversion_funnel():
    """Show conversion rates at each pipeline stage."""
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
        except Exception:
            rfqs = {}

        counts = defaultdict(int)
        for r in rfqs.values():
            status = (r.get("status") or "").lower()
            if status in ("new", "inbox", "draft"):
                counts["received"] += 1
            if status in ("priced",):
                counts["priced"] += 1
            if status in ("sent", "quoted"):
                counts["quoted"] += 1
            if status in ("won", "ordered"):
                counts["won"] += 1
            if status in ("lost",):
                counts["lost"] += 1

        total = counts["received"] + counts["priced"] + counts["quoted"] + counts["won"] + counts["lost"]
        funnel = []
        for stage, label in [("received", "Received"), ("priced", "Priced"),
                             ("quoted", "Quoted"), ("won", "Won")]:
            funnel.append({
                "stage": label,
                "count": counts[stage],
                "pct_of_total": round(counts[stage] / total * 100, 1) if total > 0 else 0,
            })

        win_rate = round(counts["won"] / (counts["won"] + counts["lost"]) * 100, 1) if (counts["won"] + counts["lost"]) > 0 else None

        return jsonify({
            "ok": True,
            "funnel": funnel,
            "total_rfqs": total,
            "win_rate": win_rate,
            "lost": counts["lost"],
        })
    except Exception as e:
        log.error("conversion funnel error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pipeline/avg-deal-size", methods=["GET"])
@auth_required
@safe_route
def api_avg_deal_size():
    """Calculate average deal size from won quotes and orders."""
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        amounts = []

        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
            for r in rfqs.values():
                if (r.get("status") or "").lower() in ("won", "ordered"):
                    total = r.get("total_price", 0)
                    if isinstance(total, (int, float)) and total > 0:
                        amounts.append(total)
        except Exception:
            pass

        try:
            orders = _load_orders()
            for o in orders.values():
                total = o.get("total", 0)
                if isinstance(total, (int, float)) and total > 0:
                    amounts.append(total)
        except Exception:
            pass

        avg = round(sum(amounts) / len(amounts), 2) if amounts else 0
        median = sorted(amounts)[len(amounts) // 2] if amounts else 0

        return jsonify({
            "ok": True,
            "avg_deal_size": avg,
            "median_deal_size": round(median, 2),
            "min_deal": round(min(amounts), 2) if amounts else 0,
            "max_deal": round(max(amounts), 2) if amounts else 0,
            "deals_counted": len(amounts),
        })
    except Exception as e:
        log.error("avg deal size error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pipeline/daily-summary", methods=["GET"])
@auth_required
@safe_route
def api_pipeline_daily_summary():
    """Daily pipeline summary: new RFQs, quotes sent, orders received today."""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
        except Exception:
            rfqs = {}

        orders = _load_orders()

        new_today = [r for r in rfqs.values() if (r.get("created") or r.get("received_date") or "")[:10] == today]
        sent_today = [r for r in rfqs.values() if (r.get("sent_date") or "")[:10] == today]
        orders_today = [o for o in orders.values() if (o.get("created_at") or "")[:10] == today]

        return jsonify({
            "ok": True,
            "date": today,
            "new_rfqs": len(new_today),
            "quotes_sent": len(sent_today),
            "orders_received": len(orders_today),
            "new_rfq_value": round(sum(r.get("total_price", 0) for r in new_today), 2),
            "sent_value": round(sum(r.get("total_price", 0) for r in sent_today), 2),
            "orders_value": round(sum(o.get("total", 0) for o in orders_today), 2),
        })
    except Exception as e:
        log.error("pipeline daily summary error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# ── From routes_features2.py ────────────────────────────────────────────────

@bp.route("/api/pipeline/sales-velocity", methods=["GET"])
@auth_required
@safe_route
def api_sales_velocity():
    """Measure sales velocity: deals * avg_value * win_rate / cycle_time."""
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
        except Exception:
            rfqs = {}

        active_deals = 0
        won_deals = 0
        lost_deals = 0
        total_value = 0.0
        cycle_days = []

        for r in rfqs.values():
            status = (r.get("status") or "").lower()
            if status in ("new", "draft", "priced", "sent", "quoted"):
                active_deals += 1
                total_value += r.get("total_price", 0) or 0
            elif status in ("won", "ordered"):
                won_deals += 1
                created = r.get("created") or r.get("received_date") or ""
                closed = r.get("won_date") or r.get("sent_date") or ""
                if created and closed:
                    try:
                        c = datetime.strptime(created[:10], "%Y-%m-%d")
                        d = datetime.strptime(closed[:10], "%Y-%m-%d")
                        days = (d - c).days
                        if 0 < days <= 180:
                            cycle_days.append(days)
                    except Exception:
                        pass
            elif status == "lost":
                lost_deals += 1

        avg_value = total_value / active_deals if active_deals > 0 else 0
        win_rate = won_deals / (won_deals + lost_deals) if (won_deals + lost_deals) > 0 else 0
        avg_cycle = sum(cycle_days) / len(cycle_days) if cycle_days else 30

        velocity = (active_deals * avg_value * win_rate) / avg_cycle if avg_cycle > 0 else 0

        return jsonify({
            "ok": True,
            "velocity": round(velocity, 2),
            "active_deals": active_deals,
            "avg_deal_value": round(avg_value, 2),
            "win_rate_pct": round(win_rate * 100, 1),
            "avg_cycle_days": round(avg_cycle, 1),
            "won": won_deals,
            "lost": lost_deals,
        })
    except Exception as e:
        log.error("sales velocity error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pipeline/weekly-summary", methods=["GET"])
@auth_required
@safe_route
def api_pipeline_weekly_summary():
    """Weekly pipeline summary: activity over the past 7 days."""
    try:
        now = datetime.now()
        week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")

        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")

        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
        except Exception:
            rfqs = {}

        orders = _load_orders()

        new_rfqs = [r for r in rfqs.values()
                    if (r.get("created") or r.get("received_date") or "")[:10] >= week_ago]
        sent_rfqs = [r for r in rfqs.values()
                     if (r.get("sent_date") or "")[:10] >= week_ago]
        new_orders = [o for o in orders.values()
                      if (o.get("created_at") or "")[:10] >= week_ago]

        return jsonify({
            "ok": True,
            "period": f"{week_ago} to {now.strftime('%Y-%m-%d')}",
            "new_rfqs": len(new_rfqs),
            "quotes_sent": len(sent_rfqs),
            "orders": len(new_orders),
            "rfq_value": round(sum(r.get("total_price", 0) for r in new_rfqs), 2),
            "sent_value": round(sum(r.get("total_price", 0) for r in sent_rfqs), 2),
            "order_value": round(sum(o.get("total", 0) for o in new_orders), 2),
        })
    except Exception as e:
        log.error("pipeline weekly summary error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pipeline/po-match", methods=["GET"])
@auth_required
@safe_route
def api_po_match():
    """Match POs to quotes — find orders that reference known RFQ IDs."""
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")

        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
        except Exception:
            rfqs = {}

        orders = _load_orders()

        matched = []
        unmatched_orders = []

        for oid, o in orders.items():
            rfq_id = o.get("rfq_id") or o.get("quote_id") or ""
            po = o.get("po_number", oid)
            if rfq_id and rfq_id in rfqs:
                matched.append({
                    "order_id": oid,
                    "po_number": po,
                    "rfq_id": rfq_id,
                    "solicitation": (rfqs[rfq_id].get("solicitation_number") or "")[:30],
                    "order_total": o.get("total", 0),
                    "quote_total": rfqs[rfq_id].get("total_price", 0),
                })
            else:
                unmatched_orders.append({
                    "order_id": oid,
                    "po_number": po,
                    "total": o.get("total", 0),
                    "agency": o.get("institution", o.get("agency", "?")),
                })

        return jsonify({
            "ok": True,
            "matched": matched[:20],
            "unmatched": unmatched_orders[:20],
            "matched_count": len(matched),
            "unmatched_count": len(unmatched_orders),
        })
    except Exception as e:
        log.error("po-match error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pipeline/auto-follow-up", methods=["POST"])
@auth_required
@safe_route
def api_auto_follow_up():
    """Generate follow-up entries for stale quotes automatically."""
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        fu_path = os.path.join(DATA_DIR, "follow_up_state.json")

        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
        except Exception:
            rfqs = {}

        try:
            with open(fu_path) as f:
                fu = json.load(f)
        except Exception:
            fu = {}

        threshold = int(request.args.get("days", 7))
        cutoff = (datetime.now() - timedelta(days=threshold)).strftime("%Y-%m-%d")
        created = 0

        for rid, r in rfqs.items():
            status = (r.get("status") or "").lower()
            if status not in ("sent", "quoted"):
                continue
            last_activity = r.get("sent_date") or r.get("updated") or r.get("created") or ""
            if not last_activity or last_activity[:10] >= cutoff:
                continue
            if rid in fu:
                continue

            fu[rid] = {
                "rfq_id": rid,
                "contact": r.get("requestor", r.get("buyer_name", "Unknown")),
                "next_follow_up": datetime.now().strftime("%Y-%m-%d"),
                "status": "pending",
                "attempts": 0,
                "created": datetime.now().isoformat(),
                "notes": f"Auto-created: quote stale since {last_activity[:10]}",
            }
            created += 1

        with open(fu_path, "w") as f:
            json.dump(fu, f, indent=2)

        return jsonify({
            "ok": True,
            "created": created,
            "total_follow_ups": len(fu),
            "message": f"Created {created} new follow-up entries",
        })
    except Exception as e:
        log.error("auto follow-up error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/quotes/expiring", methods=["GET"])
@auth_required
@safe_route
def api_expiring_quotes():
    """Find quotes expiring within N days (default 7)."""
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
        except Exception:
            rfqs = {}

        days = int(request.args.get("days", 7))
        now = datetime.now()
        cutoff = (now + timedelta(days=days)).strftime("%Y-%m-%d")
        today = now.strftime("%Y-%m-%d")
        expiring = []

        for rid, r in rfqs.items():
            status = (r.get("status") or "").lower()
            if status not in ("sent", "quoted", "priced"):
                continue
            due = r.get("due_date") or r.get("deadline") or ""
            if not due:
                continue
            due_str = due[:10]
            if due_str <= cutoff and due_str >= today:
                days_left = (datetime.strptime(due_str, "%Y-%m-%d") - now).days
                expiring.append({
                    "id": rid,
                    "solicitation": (r.get("solicitation_number") or rid)[:30],
                    "agency": r.get("institution", "?"),
                    "status": status,
                    "due_date": due_str,
                    "days_left": days_left,
                    "total": r.get("total_price", 0),
                })

        expiring.sort(key=lambda x: x["days_left"])

        return jsonify({
            "ok": True,
            "expiring": expiring[:25],
            "count": len(expiring),
            "window_days": days,
        })
    except Exception as e:
        log.error("expiring quotes error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pipeline/draft-follow-up", methods=["POST"])
@auth_required
@safe_route
def api_draft_follow_up():
    """Draft a follow-up email for a specific RFQ."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        rfq_id = data.get("rfq_id", "")
        if not rfq_id:
            return jsonify({"ok": False, "error": "rfq_id is required"})

        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
        except Exception:
            rfqs = {}

        rfq = rfqs.get(rfq_id)
        if not rfq:
            return jsonify({"ok": False, "error": f"RFQ {rfq_id} not found"})

        contact = rfq.get("requestor") or rfq.get("buyer_name") or "there"
        sol = rfq.get("solicitation_number") or rfq_id
        total = rfq.get("total_price", 0)

        subject = f"Follow-up: Quote for {sol}"
        body = (
            f"Hi {contact},\n\n"
            f"I wanted to follow up on our quote for solicitation {sol}"
            f"{' (${:,.2f})'.format(total) if total else ''}.\n\n"
            f"Please let me know if you have any questions or if there's "
            f"anything I can help with.\n\n"
            f"Best regards,\nReytech Inc."
        )

        return jsonify({
            "ok": True,
            "draft": {
                "to": rfq.get("email", rfq.get("buyer_email", "")),
                "subject": subject,
                "body": body,
                "rfq_id": rfq_id,
            },
        })
    except Exception as e:
        log.error("draft follow-up error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# ── From routes_features3.py ────────────────────────────────────────────────

@bp.route("/api/pipeline/velocity")
@auth_required
@safe_route
def api_pipeline_velocity():
    """Measure how quickly quotes move from inbox to sent to won."""
    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")

    try:
        with open(rfqs_path) as f:
            rfqs = json.load(f)
    except Exception:
        return jsonify({"ok": True, "message": "No RFQ data", "avg_days_to_quote": None})

    quote_times = []

    for r in rfqs.values():
        created = r.get("created") or r.get("received_date")
        sent = r.get("sent_date")

        if created and sent:
            try:
                c = datetime.strptime(created[:10], "%Y-%m-%d")
                s = datetime.strptime(sent[:10], "%Y-%m-%d")
                days = (s - c).days
                if 0 <= days <= 90:
                    quote_times.append(days)
            except Exception:
                pass

    avg_quote_days = round(sum(quote_times) / len(quote_times), 1) if quote_times else None

    return jsonify({
        "ok": True,
        "avg_days_to_quote": avg_quote_days,
        "fastest_quote_days": min(quote_times) if quote_times else None,
        "slowest_quote_days": max(quote_times) if quote_times else None,
        "quotes_measured": len(quote_times),
        "target_days": 2,
        "on_target": avg_quote_days is not None and avg_quote_days <= 2,
    })


@bp.route("/api/quote/lookup")
@auth_required
@safe_route
def api_quote_lookup():
    """Lookup a quote by number, solicitation, or keyword."""
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"ok": False, "error": "Provide ?q=<quote_number>"})

    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    results = []

    if os.path.exists(rfqs_path):
        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
            for rid, r in rfqs.items():
                sol = r.get("solicitation_number", "")
                qn = r.get("quote_number", "")
                buyer = r.get("requestor", r.get("buyer_name", ""))
                if (q.lower() in rid.lower() or q.lower() in sol.lower()
                        or q.lower() in (qn or "").lower()
                        or q.lower() in buyer.lower()):
                    results.append({
                        "id": rid, "solicitation": sol[:30],
                        "quote_number": qn, "status": r.get("status", "?"),
                        "requestor": buyer, "institution": r.get("institution", "?"),
                        "total": r.get("total_price", 0),
                        "created": r.get("created", r.get("received_date", "?")),
                    })
        except Exception:
            pass

    return jsonify({
        "ok": True,
        "query": q,
        "results": results[:20],
        "count": len(results),
    })


@bp.route("/api/order/<oid>/check-amazon-urls", methods=["POST"])
@auth_required
@safe_route
def api_check_amazon_urls(oid):
    """Check Amazon URLs for all items with amazon.com supplier links."""
    from src.core.order_dal import get_order as _get_order
    order = _get_order(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})

    results = []
    for item in order.get("line_items", []):
        url = item.get("supplier_url", "")
        if "amazon.com" not in url:
            results.append({"desc": item.get("description", "")[:40], "status": "skip", "reason": "not Amazon"})
            continue

        # Extract ASIN from URL
        import re
        asin_match = re.search(r'/dp/([A-Z0-9]{10})', url) or re.search(r'/gp/product/([A-Z0-9]{10})', url)
        asin = asin_match.group(1) if asin_match else item.get("asin", "")

        if not asin:
            results.append({"desc": item.get("description", "")[:40], "status": "error", "reason": "no ASIN found in URL"})
            continue

        # Check if in catalog
        try:
            from src.core.db import get_db
            with get_db() as conn:
                cat = conn.execute("SELECT * FROM catalog WHERE asin = ? OR part_number = ?", (asin, asin)).fetchone()
                if cat:
                    results.append({
                        "desc": item.get("description", "")[:40],
                        "asin": asin,
                        "status": "in_catalog",
                        "catalog_price": cat["price"] if "price" in cat.keys() else None,
                        "supplier": cat["supplier"] if "supplier" in cat.keys() else None,
                    })
                else:
                    results.append({
                        "desc": item.get("description", "")[:40],
                        "asin": asin,
                        "status": "not_in_catalog",
                        "url": url,
                    })
        except Exception as e:
            results.append({"desc": item.get("description", "")[:40], "asin": asin, "status": "error", "reason": str(e)})

    return jsonify({
        "ok": True,
        "results": results,
        "in_catalog": sum(1 for r in results if r["status"] == "in_catalog"),
        "not_in_catalog": sum(1 for r in results if r["status"] == "not_in_catalog"),
    })
