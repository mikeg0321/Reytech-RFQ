# routes_orders_full.py — Order Management, Supplier Lookup, Quote-Order Link
# Extracted from routes_intel.py for maintainability

# ═══════════════════════════════════════════════════════════════════════
# Order Management (Phase 17)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/orders")
@auth_required
def orders_page():
    """Orders dashboard — track sourcing, shipping, delivery, invoicing."""
    orders = _load_orders()
    order_list = sorted(orders.values(), key=lambda o: o.get("created_at", ""), reverse=True)

    status_cfg = {
        "new":              ("🆕 New",              "#58a6ff", "rgba(88,166,255,.1)"),
        "sourcing":         ("🛒 Sourcing",         "#d29922", "rgba(210,153,34,.1)"),
        "shipped":          ("🚚 Shipped",          "#bc8cff", "rgba(188,140,255,.1)"),
        "partial_delivery": ("📦 Partial",          "#d29922", "rgba(210,153,34,.1)"),
        "delivered":        ("✅ Delivered",         "#3fb950", "rgba(52,211,153,.1)"),
        "invoiced":         ("💰 Invoiced",         "#58a6ff", "rgba(88,166,255,.1)"),
        "closed":           ("🏁 Closed",           "#8b949e", "rgba(139,148,160,.1)"),
    }

    # Stats — aggregate across all orders
    total_orders = len(order_list)
    active = sum(1 for o in order_list if o.get("status") not in ("closed",))
    total_value = sum(o.get("total", 0) for o in order_list)
    invoiced_value = sum(o.get("invoice_total", 0) for o in order_list)

    # Line item level metrics
    all_items = []
    for o in order_list:
        for it in o.get("line_items", []):
            it["_order_id"] = o.get("order_id", "")
            it["_order_status"] = o.get("status", "")
            all_items.append(it)

    total_line_items = len(all_items)
    pending_items = sum(1 for it in all_items if it.get("sourcing_status") == "pending")
    ordered_items = sum(1 for it in all_items if it.get("sourcing_status") == "ordered")
    shipped_items = sum(1 for it in all_items if it.get("sourcing_status") == "shipped")
    delivered_items = sum(1 for it in all_items if it.get("sourcing_status") == "delivered")
    
    orders_needing_action = sum(1 for o in order_list 
                                 if o.get("status") == "new" and o.get("line_items"))
    orders_ready_invoice = sum(1 for o in order_list if o.get("status") == "delivered")
    orders_with_drafts = sum(1 for o in order_list if o.get("draft_invoice"))

    # Pipeline progress
    pct_complete = round(delivered_items / total_line_items * 100) if total_line_items else 0
    pct_shipped = round((shipped_items + delivered_items) / total_line_items * 100) if total_line_items else 0
    pct_ordered = round((ordered_items + shipped_items + delivered_items) / total_line_items * 100) if total_line_items else 0

    macro_html = f"""
    <div class="bento bento-4" style="margin-bottom:16px">
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--ac)">{total_orders}</div>
      <div style="font-size:11px;color:var(--tx2);margin-top:2px">Total Orders</div>
      <div style="font-size:11px;color:var(--tx2);margin-top:4px">{active} active</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#3fb950">${total_value:,.0f}</div>
      <div style="font-size:11px;color:var(--tx2);margin-top:2px">Total Value</div>
      <div style="font-size:11px;color:var(--tx2);margin-top:4px">${invoiced_value:,.0f} invoiced</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#d29922">{total_line_items}</div>
      <div style="font-size:11px;color:var(--tx2);margin-top:2px">Line Items</div>
      <div style="font-size:11px;color:var(--tx2);margin-top:4px">{pct_complete}% delivered</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:{'#f85149' if orders_needing_action else '#3fb950'}">{orders_needing_action}</div>
      <div style="font-size:11px;color:var(--tx2);margin-top:2px">Need Action</div>
      <div style="font-size:11px;color:var(--tx2);margin-top:4px">{orders_ready_invoice} ready to invoice</div>
     </div>
    </div>

    <div class="card" style="margin-bottom:16px;padding:12px 16px">
     <div style="display:flex;gap:24px;flex-wrap:wrap;align-items:center;font-size:12px">
      <div style="display:flex;align-items:center;gap:6px"><span style="width:10px;height:10px;border-radius:50%;background:#d29922;display:inline-block"></span> <b>{pending_items}</b> pending</div>
      <div style="display:flex;align-items:center;gap:6px"><span style="width:10px;height:10px;border-radius:50%;background:#58a6ff;display:inline-block"></span> <b>{ordered_items}</b> ordered</div>
      <div style="display:flex;align-items:center;gap:6px"><span style="width:10px;height:10px;border-radius:50%;background:#bc8cff;display:inline-block"></span> <b>{shipped_items}</b> shipped</div>
      <div style="display:flex;align-items:center;gap:6px"><span style="width:10px;height:10px;border-radius:50%;background:#3fb950;display:inline-block"></span> <b>{delivered_items}</b> delivered</div>
      <div style="flex:1;min-width:200px">
       <div style="background:var(--sf);border-radius:8px;height:14px;overflow:hidden;display:flex">
        <div style="width:{pct_complete}%;background:#3fb950;transition:width 0.3s" title="{delivered_items} delivered"></div>
        <div style="width:{round(shipped_items/total_line_items*100) if total_line_items else 0}%;background:#bc8cff" title="{shipped_items} shipped"></div>
        <div style="width:{round(ordered_items/total_line_items*100) if total_line_items else 0}%;background:#58a6ff" title="{ordered_items} ordered"></div>
       </div>
      </div>
     </div>
    </div>
    """

    rows = ""
    for o in order_list:
        oid = o.get("order_id", "")
        st = o.get("status", "new")
        lbl, clr, bg = status_cfg.get(st, status_cfg["new"])
        items = o.get("line_items", [])
        sourced = sum(1 for it in items if it.get("sourcing_status") in ("ordered", "shipped", "delivered"))
        shipped = sum(1 for it in items if it.get("sourcing_status") in ("shipped", "delivered"))
        delivered = sum(1 for it in items if it.get("sourcing_status") == "delivered")
        has_tracking = sum(1 for it in items if it.get("tracking_number"))
        has_suppliers = sum(1 for it in items if it.get("supplier_url"))
        n = len(items)
        pct = round(delivered / n * 100) if n else 0

        # Progress bar for this order
        progress_bar = f"""<div style="display:flex;align-items:center;gap:4px;min-width:80px">
         <div style="flex:1;background:var(--sf);border-radius:4px;height:6px;overflow:hidden">
          <div style="width:{pct}%;background:#3fb950;height:100%"></div>
         </div>
         <span style="font-size:10px;color:var(--tx2);white-space:nowrap">{delivered}/{n}</span>
        </div>"""

        # Indicators
        indicators = ""
        if has_suppliers:
            indicators += f'<span title="{has_suppliers}/{n} items linked to suppliers" style="font-size:10px;margin-left:2px">🔗{has_suppliers}</span>'
        if has_tracking:
            indicators += f'<span title="{has_tracking} tracking numbers" style="font-size:10px;margin-left:2px">📦{has_tracking}</span>'
        if o.get("draft_invoice"):
            indicators += '<span title="Draft invoice ready" style="font-size:10px;margin-left:2px">📄</span>'

        rows += f"""<tr style="{'opacity:0.5' if st == 'closed' else ''}">
         <td><a href="/order/{oid}" style="color:var(--ac);text-decoration:none;font-family:'JetBrains Mono',monospace;font-weight:700">{oid}</a></td>
         <td class="mono" style="white-space:nowrap">{o.get('created_at','')[:10]}</td>
         <td>{o.get('agency','')}</td>
         <td style="max-width:250px;word-wrap:break-word;white-space:normal;font-weight:500">{o.get('institution','')}</td>
         <td class="mono">{o.get('po_number','') or o.get('quote_number','')}</td>
         <td style="text-align:right;font-weight:600;font-family:'JetBrains Mono',monospace">${o.get('total',0):,.2f}</td>
         <td>{progress_bar}</td>
         <td style="text-align:center"><span style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;color:{clr};background:{bg}">{lbl}</span>{indicators}</td>
         <td style="text-align:center"><button onclick="deleteOrder('{oid}')" style="background:none;border:none;cursor:pointer;font-size:12px;color:var(--tx2)" title="Delete order">🗑️</button></td>
        </tr>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:14px">
     <h2 style="margin:0;font-size:20px;font-weight:700">📦 Orders & Fulfillment</h2>
     <div style="display:flex;gap:8px;margin-top:6px;flex-wrap:wrap">
       <a href="/shipping" style="font-size:11px;color:var(--ac);text-decoration:none;background:var(--sf2);padding:3px 10px;border-radius:12px;border:1px solid var(--bd)">🚚 Shipping</a>
       <a href="/margins" style="font-size:11px;color:var(--ac);text-decoration:none;background:var(--sf2);padding:3px 10px;border-radius:12px;border:1px solid var(--bd)">📊 Margins</a>
       <a href="/payments" style="font-size:11px;color:var(--ac);text-decoration:none;background:var(--sf2);padding:3px 10px;border-radius:12px;border:1px solid var(--bd)">💳 Payments</a>
       <a href="/recurring" style="font-size:11px;color:var(--ac);text-decoration:none;background:var(--sf2);padding:3px 10px;border-radius:12px;border:1px solid var(--bd)">🔄 Recurring</a>
       <a href="/pricing" style="font-size:11px;color:var(--ac);text-decoration:none;background:var(--sf2);padding:3px 10px;border-radius:12px;border:1px solid var(--bd)">💰 Pricing Intel</a>
       <a href="/audit" style="font-size:11px;color:var(--ac);text-decoration:none;background:var(--sf2);padding:3px 10px;border-radius:12px;border:1px solid var(--bd)">📋 Audit Trail</a>
     </div>
     <div style="display:flex;gap:10px;align-items:center">
      <button onclick="createFromPO()" class="btn btn-g" style="font-size:13px;white-space:nowrap">📄 Import PO PDF</button>
      <button onclick="createOrder()" class="btn btn-s" style="font-size:13px;white-space:nowrap">+ Manual Order</button>
     </div>
    </div>
    {macro_html}
    <div class="card" style="padding:0;overflow-x:auto">
     <table class="home-tbl" style="min-width:800px">
      <thead><tr>
       <th style="width:130px">Order</th><th style="width:90px">Date</th><th style="width:60px">Agency</th>
       <th>Institution</th><th style="width:100px">PO / Quote</th>
       <th style="text-align:right;width:90px">Total</th><th style="width:70px;text-align:center">Delivery</th>
       <th style="width:100px;text-align:center">Status</th>
       <th style="width:40px"></th>
      </tr></thead>
      <tbody>{rows if rows else '<tr><td colspan="9" style="text-align:center;padding:24px;color:var(--tx2)">No orders yet — mark a quote as Won or import a PO PDF</td></tr>'}</tbody>
     </table>
    </div>

    <!-- Pending Invoices from QuickBooks -->
    <script>
    function createOrder() {{
      const po = prompt('PO Number (from state purchase order):');
      if (!po) return;
      const agency = prompt('Agency (CDCR, CCHCS, CalVet, DSH, etc):', '');
      const inst = prompt('Institution / Ship-To name:', '');
      fetch('/api/order/create', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{po_number: po, agency: agency || '', institution: inst || '', items: [], total: 0}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{
          location.href = '/order/' + d.order_id;
        }} else alert('Error: ' + (d.error || 'unknown'));
      }});
    }}
    function createFromPO() {{
      document.getElementById('po-upload-input').click();
    }}
    function handlePOUpload(input) {{
      const file = input.files[0];
      if (!file) return;
      // First create a skeleton order, then upload the PDF to populate it
      fetch('/api/order/create', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{po_number: 'PENDING', agency: '', institution: '', items: [], total: 0}})
      }}).then(r => r.json()).then(d => {{
        if (!d.ok) {{ alert('Error: ' + d.error); return; }}
        const oid = d.order_id;
        const formData = new FormData();
        formData.append('file', file);
        return fetch('/api/order/' + oid + '/import-po', {{method: 'POST', body: formData}}).then(r => r.json()).then(r => {{
          if (r.ok) {{
            location.href = '/order/' + oid;
          }} else {{
            alert('Order created but PDF parse issue: ' + (r.error || 'unknown') + '\\nRedirecting to order — upload PO again from detail page.');
            if (r.raw_text) console.log('PO raw text:', r.raw_text);
            location.href = '/order/' + oid;
          }}
        }});
      }});
    }}
    function deleteOrder(oid) {{
      const reasons = ['Duplicate', 'Created in error', 'PO cancelled', 'Test order'];
      const reason = prompt('Delete order ' + oid + '?\\n\\nReason:\\n1. Duplicate\\n2. Created in error\\n3. PO cancelled\\n4. Test order\\n\\nEnter number or custom reason:', '1');
      if (reason === null) return;
      const reasonMap = {{'1':'Duplicate','2':'Created in error','3':'PO cancelled','4':'Test order'}};
      const finalReason = reasonMap[reason] || reason;
      fetch('/api/order/' + oid + '/delete', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{reason: finalReason}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) location.reload();
        else alert('Error: ' + (d.error || 'unknown'));
      }});
    }}
    </script>
    <input type="file" id="po-upload-input" accept=".pdf" style="display:none" onchange="handlePOUpload(this)">
    <div id="qb-invoices" class="card" style="margin-top:14px;padding:16px;display:none">
     <div class="card-t" style="margin-bottom:10px">💰 QuickBooks — Pending Invoices</div>
     <div id="inv-stats" style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px"></div>
     <div id="inv-table"></div>
    </div>
    <script>
    fetch('/api/qb/financial-context').then(r=>r.json()).then(d=>{{
     if(!d.ok) return;
     document.getElementById('qb-invoices').style.display='block';
     const s=document.getElementById('inv-stats');
     const mkStat=(label,val,color)=>'<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">'+label+'</div><div style="font-size:18px;font-weight:700;color:'+(color||'var(--tx)')+'">'+val+'</div></div>';
     s.innerHTML=mkStat('Open','$'+(d.total_receivable||0).toLocaleString(),'var(--yl)')
      +mkStat('Overdue','$'+(d.overdue_amount||0).toLocaleString(),'var(--rd)')
      +mkStat('Collected','$'+(d.total_collected||0).toLocaleString(),'var(--gn)')
      +mkStat('Invoices',d.invoice_count||0);
     const inv=d.pending_invoices||[];
     if(inv.length){{
      let t='<table class="tbl" style="width:100%"><thead><tr><th>Invoice</th><th>Customer</th><th style="text-align:right">Total</th><th style="text-align:right">Balance</th><th>Due</th><th>Days Out</th><th>Status</th></tr></thead><tbody>';
      inv.forEach(i=>{{
       const st=i.status==='overdue'?'<span style="color:var(--rd);font-weight:600">⚠️ OVERDUE</span>':'<span style="color:var(--yl)">Open</span>';
       t+='<tr><td class="mono">'+i.doc_number+'</td><td>'+i.customer+'</td><td style="text-align:right;font-weight:600" class="mono">$'+i.total.toLocaleString()+'</td><td style="text-align:right;color:var(--yl)" class="mono">$'+i.balance.toLocaleString()+'</td><td class="mono">'+i.due_date+'</td><td style="text-align:center">'+i.days_outstanding+'</td><td>'+st+'</td></tr>';
      }});
      t+='</tbody></table>';
      document.getElementById('inv-table').innerHTML=t;
     }} else {{
      document.getElementById('inv-table').innerHTML='<div style="color:var(--tx2);text-align:center;padding:12px">No pending invoices</div>';
     }}
    }}).catch(()=>{{}});
    </script>"""
    return render(content, title="Orders")


@bp.route("/order/<oid>")
@auth_required
def order_detail(oid):
    """Order detail page — line item sourcing, tracking, invoicing."""
    orders = _load_orders()
    order = orders.get(oid)
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
         <pre style="color:var(--tx2);font-size:12px;overflow:auto;max-height:400px">{traceback.format_exc()}</pre>
         <a href="/orders" class="btn btn-s" style="margin-top:12px">← Back to Orders</a>
        </div>""", f"Error: {oid}")


def _render_order_detail(order, oid):
    """Actual order detail rendering (separated for error handling)."""
    st = order.get("status", "new")
    items = order.get("line_items", [])
    qn = order.get("quote_number", "")
    institution = order.get("institution", "")

    sourcing_cfg = {
        "pending":   ("⏳ Pending",   "#d29922", "rgba(210,153,34,.1)"),
        "ordered":   ("🛒 Ordered",   "#58a6ff", "rgba(88,166,255,.1)"),
        "shipped":   ("🚚 Shipped",   "#bc8cff", "rgba(188,140,255,.1)"),
        "delivered": ("✅ Delivered", "#3fb950", "rgba(52,211,153,.1)"),
    }
    inv_cfg = {
        "pending":  ("⏳", "#d29922"),
        "partial":  ("½", "#58a6ff"),
        "invoiced": ("✅", "#3fb950"),
    }

    # Line items table
    items_rows = ""
    for it in items:
        lid = it.get("line_id", "")
        desc = it.get("description", "")[:80]
        pn = it.get("part_number", "")
        sup_url = it.get("supplier_url", "")
        supplier_name = it.get("supplier", "") or "—"
        if sup_url:
            sup_link = f'<a href="{sup_url}" target="_blank" style="color:var(--ac);font-size:11px" title="{sup_url}">🛒 {supplier_name}</a>'
        else:
            sup_link = f'<span style="color:var(--tx2);font-size:11px">{supplier_name}</span>'
        # Edit link button
        sup_edit = f'<button onclick="editSupplier(\'{oid}\',\'{lid}\')" style="background:none;border:none;cursor:pointer;font-size:10px;color:var(--tx2);padding:0" title="Edit supplier/link">✏️</button>'

        ss = it.get("sourcing_status", "pending")
        s_lbl, s_clr, s_bg = sourcing_cfg.get(ss, sourcing_cfg["pending"])
        tracking = it.get("tracking_number", "")
        # Auto-detect tracking URL based on carrier
        carrier = it.get("carrier", "")
        if tracking:
            carrier_low = carrier.lower()
            if "amazon" in carrier_low or tracking.startswith("TBA"):
                track_url = f"https://www.amazon.com/progress-tracker/package/ref=ppx_yo_dt_b_track_package?itemId=&shipmentId={tracking}"
            elif "ups" in carrier_low or tracking.startswith("1Z"):
                track_url = f"https://www.ups.com/track?tracknum={tracking}"
            elif "fedex" in carrier_low:
                track_url = f"https://www.fedex.com/fedextrack/?trknbr={tracking}"
            elif "usps" in carrier_low:
                track_url = f"https://tools.usps.com/go/TrackConfirmAction?tLabels={tracking}"
            else:
                track_url = f"https://track.aftership.com/{tracking}"
            tracking_html = f'<a href="{track_url}" target="_blank" style="color:var(--ac);font-size:10px">{tracking[:20]}</a>'
        else:
            tracking_html = '<button onclick="addTracking(\'' + oid + '\',\'' + lid + '\')" style="background:none;border:none;cursor:pointer;font-size:10px;color:var(--tx2)">+ tracking</button>'

        is_lbl, is_clr = inv_cfg.get(it.get("invoice_status","pending"), inv_cfg["pending"])

        items_rows += f"""<tr data-lid="{lid}">
         <td style="color:var(--tx2);font-size:11px">{lid}</td>
         <td style="max-width:300px;word-wrap:break-word;white-space:normal">{desc}</td>
         <td class="mono" style="font-size:11px">{pn or '—'}</td>
         <td>{sup_link} {sup_edit}</td>
         <td class="mono" style="text-align:center">{it.get('qty',0)}</td>
         <td class="mono" style="text-align:right">${it.get('unit_price',0):,.2f}</td>
         <td style="text-align:center">
          <select onchange="updateLine('{oid}','{lid}','sourcing_status',this.value)" style="background:var(--sf);border:1px solid var(--bd);border-radius:4px;color:{s_clr};font-size:11px;padding:2px">
           <option value="pending" {"selected" if ss=="pending" else ""}>⏳ Pending</option>
           <option value="ordered" {"selected" if ss=="ordered" else ""}>🛒 Ordered</option>
           <option value="shipped" {"selected" if ss=="shipped" else ""}>🚚 Shipped</option>
           <option value="delivered" {"selected" if ss=="delivered" else ""}>✅ Delivered</option>
          </select>
         </td>
         <td style="font-size:10px">{carrier} {tracking_html}</td>
         <td style="text-align:center;font-size:12px;color:{is_clr}" title="{it.get('invoice_status','pending')}">{is_lbl}</td>
        </tr>"""

    status_cfg = {
        "new": "🆕 New", "sourcing": "🛒 Sourcing", "shipped": "🚚 Shipped",
        "partial_delivery": "📦 Partial Delivery", "delivered": "✅ Delivered",
        "invoiced": "💰 Invoiced", "closed": "🏁 Closed"
    }

    # Upload PO prompt (prominent when no items)
    if not items:
        upload_section = f"""
    <div class="card" style="margin-bottom:14px;border:2px dashed var(--ac);text-align:center;padding:32px">
     <div style="font-size:18px;font-weight:700;margin-bottom:8px">📄 Upload PO PDF to populate line items</div>
     <div style="color:var(--tx2);font-size:13px;margin-bottom:16px">The PO document has everything — items, quantities, prices, ship-to. Upload it and we'll parse all fields automatically.</div>
     <input type="file" id="po-pdf" accept=".pdf" style="display:none" onchange="uploadPO('{oid}',this)">
     <button onclick="document.getElementById('po-pdf').click()" class="btn btn-g" style="font-size:14px;padding:10px 24px">📄 Upload PO PDF</button>
     <div id="upload-status" style="margin-top:12px;font-size:12px;color:var(--tx2)"></div>
    </div>"""
    else:
        upload_section = f"""
    <div style="margin-bottom:8px;display:flex;justify-content:flex-end">
     <input type="file" id="po-pdf" accept=".pdf" style="display:none" onchange="uploadPO('{oid}',this)">
     <button onclick="document.getElementById('po-pdf').click()" class="btn btn-s" style="font-size:11px">📄 Re-import from PO PDF</button>
     <div id="upload-status" style="margin-left:8px;font-size:11px;color:var(--tx2);line-height:28px"></div>
    </div>"""

    content = f"""
    <div style="display:flex;gap:10px;align-items:center;margin-bottom:16px">
     <a href="/orders" class="btn btn-s" style="font-size:13px">← Orders</a>
     {f'<a href="/quote/{qn}" class="btn btn-s" style="font-size:13px">📋 Quote {qn}</a>' if qn else ''}
     <button onclick="if(confirm('Delete order {oid}?'))fetch('/api/order/{oid}/delete',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{reason:'manual delete'}})}}).then(r=>r.json()).then(d=>{{if(d.ok)location.href='/orders'}})" class="btn btn-s" style="font-size:12px;margin-left:auto;color:var(--rd)">🗑️ Delete</button>
    </div>

    <div class="bento bento-2" style="margin-bottom:14px">
     <div class="card" style="margin:0">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px">
       <div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:700">{oid}</div>
        <div style="color:var(--tx2);font-size:12px;margin-top:4px">{order.get('agency','')}{' · ' if order.get('agency') else ''}Created {order.get('created_at','')[:10]}</div>
       </div>
       <span style="padding:4px 12px;border-radius:12px;font-size:12px;font-weight:600;background:var(--sf2)">{status_cfg.get(st, st)}</span>
      </div>
      <div class="meta-g" style="margin:0">
       <div class="meta-i"><div class="meta-l">Institution</div><div class="meta-v">{institution}</div></div>
       <div class="meta-i"><div class="meta-l">PO Number</div><div class="meta-v" style="color:var(--gn);font-weight:600">{order.get('po_number','—')}</div></div>
       <div class="meta-i"><div class="meta-l">Quote</div><div class="meta-v">{qn or '—'}</div></div>
       <div class="meta-i"><div class="meta-l">Items</div><div class="meta-v">{len(items)}</div></div>
      </div>
     </div>
     <div class="card" style="margin:0">
      <div style="text-align:center;padding:12px 0">
       <div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px">Order Total</div>
       <div style="font-family:'JetBrains Mono',monospace;font-size:32px;font-weight:700;color:var(--gn);margin:8px 0">${order.get('total',0):,.2f}</div>
       {f'<div style="font-size:11px;color:var(--tx2)">Subtotal: ${order.get("subtotal",0):,.2f} · Tax: ${order.get("tax",0):,.2f}</div>' if order.get('tax') else ''}
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-top:10px">
       <div style="background:var(--sf2);padding:8px;border-radius:8px;text-align:center">
        <div style="font-size:10px;color:var(--tx2)">Sourced</div>
        <div style="font-size:18px;font-weight:700;color:#58a6ff">{sum(1 for i in items if i.get('sourcing_status') in ('ordered','shipped','delivered'))}/{len(items)}</div>
       </div>
       <div style="background:var(--sf2);padding:8px;border-radius:8px;text-align:center">
        <div style="font-size:10px;color:var(--tx2)">Shipped</div>
        <div style="font-size:18px;font-weight:700;color:#bc8cff">{sum(1 for i in items if i.get('sourcing_status') in ('shipped','delivered'))}/{len(items)}</div>
       </div>
       <div style="background:var(--sf2);padding:8px;border-radius:8px;text-align:center">
        <div style="font-size:10px;color:var(--tx2)">Delivered</div>
        <div style="font-size:18px;font-weight:700;color:#3fb950">{sum(1 for i in items if i.get('sourcing_status') == 'delivered')}/{len(items)}</div>
       </div>
      </div>
      <div style="margin-top:12px;display:flex;gap:8px;justify-content:center">
       <button onclick="invoiceOrder('{oid}','partial')" class="btn btn-s" style="font-size:12px">½ Partial Invoice</button>
       <button onclick="invoiceOrder('{oid}','full')" class="btn btn-g" style="font-size:12px">💰 Full Invoice</button>
      </div>
     </div>
    </div>

    {upload_section}

    <!-- Line Items with sourcing controls -->
    <div class="card">
     <div class="card-t">Line Items — Sourcing & Tracking</div>
     <div style="overflow-x:auto">
     <table class="home-tbl" style="min-width:900px">
      <thead><tr>
       <th style="width:40px">#</th><th>Description</th><th style="width:80px">Part #</th>
       <th style="width:80px">Supplier</th><th style="width:40px;text-align:center">Qty</th>
       <th style="width:80px;text-align:right">Price</th><th style="width:100px;text-align:center">Status</th>
       <th style="width:140px">Tracking</th><th style="width:30px;text-align:center">Inv</th>
      </tr></thead>
      <tbody>{items_rows}</tbody>
     </table>
     </div>
    </div>

    <!-- Bulk actions -->
    <div class="card" style="margin-top:14px">
     <div class="card-t">Quick Actions</div>
     <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button onclick="bulkAddTracking('{oid}')" class="btn btn-s" style="font-size:12px">📋 Bulk Add Tracking</button>
      <button onclick="markAllOrdered('{oid}')" class="btn btn-s" style="font-size:12px">🛒 Mark All Ordered</button>
      <button onclick="markAllDelivered('{oid}')" class="btn btn-s" style="font-size:12px">✅ Mark All Delivered</button>
      <button onclick="addLineItem('{oid}')" class="btn btn-s" style="font-size:12px">➕ Add Line Item</button>
      <button onclick="lookupSuppliers('{oid}')" class="btn btn-s" style="font-size:12px" id="lookup-btn">🔍 Lookup Suppliers</button>
      <button onclick="linkQuote('{oid}')" class="btn btn-s" style="font-size:12px" id="link-quote-btn">{f'🔗 Linked: {qn}' if qn else '🔗 Link Quote'}</button>
      <a href="/api/order/{oid}/reply-all" class="btn btn-s" style="font-size:12px">📧 Draft PO Confirmation</a>
     </div>
    </div>
    """

    # Draft Invoice section (auto-generated when all items delivered)
    draft_inv = order.get("draft_invoice", {})
    if draft_inv:
        inv_items_rows = ""
        for di in draft_inv.get("items", []):
            inv_items_rows += f"""<tr>
             <td style="max-width:300px;word-wrap:break-word;white-space:normal;font-size:12px">{di.get('description','')[:70]}</td>
             <td class="mono" style="font-size:11px">{di.get('part_number','')}</td>
             <td class="mono" style="text-align:center">{di.get('qty',0)}</td>
             <td class="mono" style="text-align:right">${di.get('unit_price',0):,.2f}</td>
             <td class="mono" style="text-align:right">${di.get('extended',0):,.2f}</td>
            </tr>"""

        content += f"""
    <div class="card" style="margin-top:14px;border:2px solid var(--gn)">
     <div class="card-t" style="color:var(--gn)">📄 Draft Invoice — {draft_inv.get('invoice_number','')}</div>
     <div style="margin-bottom:12px;font-size:12px;color:var(--tx2)">
      Auto-generated {draft_inv.get('created_at','')[:10]} when all items marked delivered.
      {f'<span style="color:var(--ac)">QB Synced: {draft_inv.get("qb_synced_at","")[:10]}</span>' if draft_inv.get('qb_invoice_id') else 'Review line items, then finalize → push to QuickBooks.'}
     </div>
     <div class="meta-g" style="margin-bottom:12px">
      <div class="meta-i"><div class="meta-l">Bill To</div><div class="meta-v">{draft_inv.get('bill_to_name','')}</div></div>
      <div class="meta-i"><div class="meta-l">Email</div><div class="meta-v">{draft_inv.get('bill_to_email','')}</div></div>
      <div class="meta-i"><div class="meta-l">PO #</div><div class="meta-v">{draft_inv.get('po_number','')}</div></div>
      <div class="meta-i"><div class="meta-l">Terms</div><div class="meta-v">{draft_inv.get('terms','Net 45')}</div></div>
     </div>
     <div style="overflow-x:auto">
     <table class="home-tbl" style="min-width:600px">
      <thead><tr>
       <th>Description</th><th style="width:80px">Part #</th>
       <th style="width:50px;text-align:center">Qty</th>
       <th style="width:80px;text-align:right">Unit Price</th>
       <th style="width:90px;text-align:right">Extended</th>
      </tr></thead>
      <tbody>{inv_items_rows}</tbody>
      <tfoot>
       <tr style="border-top:2px solid var(--bd)">
        <td colspan="4" style="text-align:right;font-weight:600">Subtotal:</td>
        <td class="mono" style="text-align:right">${draft_inv.get('subtotal',0):,.2f}</td>
       </tr>
       <tr><td colspan="4" style="text-align:right;font-weight:600">Tax ({draft_inv.get('tax_rate',7.75)}%):</td>
        <td class="mono" style="text-align:right">${draft_inv.get('tax',0):,.2f}</td>
       </tr>
       <tr><td colspan="4" style="text-align:right;font-weight:700;font-size:14px">Grand Total:</td>
        <td class="mono" style="text-align:right;font-weight:700;font-size:14px;color:var(--gn)">${draft_inv.get('total',0):,.2f}</td>
       </tr>
      </tfoot>
     </table>
     </div>
     <div style="margin-top:12px;display:flex;gap:8px;justify-content:flex-end;align-items:center">
      <button onclick="genInvoicePdf('{oid}')" class="btn btn-s" style="font-size:12px" id="inv-pdf-btn">📄 Generate PDF</button>
      {f'<a href="/api/order/{oid}/invoice-pdf/download" class="btn btn-s" style="font-size:12px;color:var(--gn)" download>⬇️ Download PDF</a>' if draft_inv.get('pdf_path') else ''}
      <button onclick="invoiceOrder('{oid}','full')" class="btn btn-g" style="font-size:13px">💰 Finalize Invoice</button>
      {f'<span style="font-size:11px;color:var(--tx2);line-height:30px">QB #{draft_inv.get("qb_invoice_id")}</span>' if draft_inv.get('qb_invoice_id') else '<span style="font-size:11px;color:var(--tx2);line-height:30px">QuickBooks API: not yet connected</span>'}
     </div>
    </div>
    """
    else:
        content += ""
    content += f"""

    <script>
    function updateLine(oid, lid, field, value) {{
      fetch('/api/order/' + oid + '/line/' + lid, {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{[field]: value}})
      }}).then(r => r.json()).then(d => {{
        if (!d.ok) alert('Error: ' + (d.error||'unknown'));
        else location.reload();
      }});
    }}

    function invoiceOrder(oid, type) {{
      const num = prompt('Invoice number:');
      if (!num) return;
      fetch('/api/order/' + oid + '/invoice', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{type: type, invoice_number: num}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) location.reload();
        else alert('Error: ' + (d.error||'unknown'));
      }});
    }}

    function genInvoicePdf(oid) {{
      var btn = document.getElementById('inv-pdf-btn');
      if (btn) {{ btn.disabled=true; btn.textContent='⏳ Generating...'; }}
      fetch('/api/order/' + oid + '/invoice-pdf', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{
          window.open(d.download_url, '_blank');
          if (btn) {{ btn.textContent='✅ PDF Ready'; }}
          setTimeout(function() {{ location.reload(); }}, 1000);
        }} else {{
          alert('Error: ' + (d.error||'unknown'));
          if (btn) {{ btn.disabled=false; btn.textContent='📄 Generate PDF'; }}
        }}
      }}).catch(function(e) {{
        alert('Error: ' + e);
        if (btn) {{ btn.disabled=false; btn.textContent='📄 Generate PDF'; }}
      }});
    }}

    function markAllOrdered(oid) {{
      fetch('/api/order/' + oid + '/bulk', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{sourcing_status: 'ordered'}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); }});
    }}

    function markAllDelivered(oid) {{
      fetch('/api/order/' + oid + '/bulk', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{sourcing_status: 'delivered'}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); }});
    }}

    function bulkAddTracking(oid) {{
      const tracking = prompt('Tracking number(s) — comma separated for multiple shipments:');
      if (!tracking) return;
      const carrier = prompt('Carrier (UPS/FedEx/USPS/Amazon):', 'Amazon');
      fetch('/api/order/' + oid + '/bulk-tracking', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{tracking: tracking, carrier: carrier}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); }});
    }}

    function editSupplier(oid, lid) {{
      const name = prompt('Supplier name (Amazon, Grainger, Curbell, etc):');
      if (name === null) return;
      const url = prompt('Product URL (paste Amazon/Grainger/supplier link):');
      if (url === null) return;
      fetch('/api/order/' + oid + '/line/' + lid, {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{supplier: name, supplier_url: url}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); else alert(d.error); }});
    }}

    function addTracking(oid, lid) {{
      const tracking = prompt('Tracking number:');
      if (!tracking) return;
      const carrier = prompt('Carrier (Amazon/UPS/FedEx/USPS):', 'Amazon');
      fetch('/api/order/' + oid + '/line/' + lid, {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{tracking_number: tracking, carrier: carrier, sourcing_status: 'shipped'}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); else alert(d.error); }});
    }}

    function addLineItem(oid) {{
      const desc = prompt('Item description:');
      if (!desc) return;
      const qty = parseInt(prompt('Quantity:', '1')) || 1;
      const price = parseFloat(prompt('Unit price ($):', '0')) || 0;
      const pn = prompt('Part number / ASIN (optional):', '') || '';
      const supplier = prompt('Supplier (Amazon, Grainger, etc):', '') || '';
      const url = prompt('Product URL (optional):', '') || '';
      fetch('/api/order/' + oid + '/add-line', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{description: desc, qty: qty, unit_price: price, part_number: pn, supplier: supplier, supplier_url: url}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); else alert(d.error || 'Failed'); }});
    }}

    function uploadPO(oid, input) {{
      const file = input.files[0];
      if (!file) return;
      const status = document.getElementById('upload-status');
      status.innerHTML = '⏳ Parsing PO PDF...';
      const formData = new FormData();
      formData.append('file', file);
      fetch('/api/order/' + oid + '/import-po', {{
        method: 'POST',
        body: formData
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{
          status.innerHTML = '✅ Imported ' + d.items_added + ' items · $' + (d.total||0).toLocaleString();
          setTimeout(() => location.reload(), 800);
        }} else {{
          status.innerHTML = '❌ ' + (d.error || 'Parse failed');
          if (d.raw_text) {{
            console.log('PO raw text:', d.raw_text);
            status.innerHTML += ' — raw text logged to console';
          }}
        }}
      }}).catch(e => {{ status.innerHTML = '❌ Upload failed: ' + e; }});
    }}

    function lookupSuppliers(oid) {{
      var btn = document.getElementById('lookup-btn');
      if (btn) {{ btn.disabled=true; btn.textContent='⏳ Searching...'; }}
      fetch('/api/order/' + oid + '/lookup-suppliers', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{
          var msg = 'Searched ' + d.total + ' items. Updated ' + d.updated + ' with supplier links.';
          if (d.updated > 0) {{
            alert(msg);
            location.reload();
          }} else {{
            var detail = '';
            (d.results||[]).forEach(function(r) {{
              if (r.search_urls && r.search_urls.length > 0) {{
                detail += '\\n' + (r.part_number || r.description) + ': ';
                detail += r.search_urls.map(function(u) {{ return u.supplier; }}).join(', ');
              }}
            }});
            alert(msg + '\\n\\nManual search links available:' + detail);
          }}
        }} else {{
          alert('Error: ' + (d.error||'unknown'));
        }}
        if (btn) {{ btn.disabled=false; btn.textContent='🔍 Lookup Suppliers'; }}
      }}).catch(function(e) {{
        alert('Error: ' + e);
        if (btn) {{ btn.disabled=false; btn.textContent='🔍 Lookup Suppliers'; }}
      }});
    }}
    function linkQuote(oid) {{
      var qn = prompt('Enter quote number to link (e.g., Q-2024-001)\\nLeave blank to auto-detect:');
      if (qn === null) return;
      var btn = document.getElementById('link-quote-btn');
      if (btn) {{ btn.disabled=true; btn.textContent='⏳ Linking...'; }}
      fetch('/api/order/' + oid + '/link-quote', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{quote_number: qn}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{
          alert('Linked to ' + d.quote_number + '. Enriched ' + d.enriched + ' of ' + d.total_items + ' items with quote data.');
          location.reload();
        }} else {{
          alert('Error: ' + (d.error||'unknown'));
          if (btn) {{ btn.disabled=false; btn.textContent='🔗 Link Quote'; }}
        }}
      }}).catch(function(e) {{
        alert('Error: ' + e);
        if (btn) {{ btn.disabled=false; btn.textContent='🔗 Link Quote'; }}
      }});
    }}
    </script>
    """
    return render(content, title=f"Order {oid}")


# ─── Order API Routes ──────────────────────────────────────────────────────

@bp.route("/api/order/create", methods=["POST"])
@auth_required
@rate_limit("api")
@audit_action("order_create")
def api_order_create():
    """Create a new order manually (for POs received outside the system).
    POST JSON: {po_number, agency, institution, total, items: [{description, qty, unit_price, part_number, supplier, supplier_url}]}
    """
    data = request.get_json(silent=True) or {}
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
    return jsonify({"ok": True, "order_id": order.get("order_id"), "items": len(order.get("line_items", []))})


@bp.route("/api/order/<oid>/add-line", methods=["POST"])
@auth_required
def api_order_add_line(oid):
    """Add a line item to an existing order.
    POST JSON: {description, qty, unit_price, part_number, supplier, supplier_url}
    """
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
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
    orders[oid] = order
    _save_orders(orders)
    return jsonify({"ok": True, "line_id": new_item["line_id"], "total_items": len(items)})


@bp.route("/api/order/<oid>/import-po", methods=["POST"])
@auth_required
def api_order_import_po(oid):
    """Upload and parse a PO PDF to populate order line items.
    Multipart form: file=<pdf>
    Returns: {ok, items_added, total, raw_text (on failure for debugging)}
    """
    orders = _load_orders()
    order = orders.get(oid)
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
        except Exception:
            pass
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
    orders[oid] = order
    _save_orders(orders)

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
def api_order_update_line(oid, lid):
    """Update a single line item. POST JSON with any fields to update."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
    updated = False
    for it in order.get("line_items", []):
        if it.get("line_id") == lid:
            for field in ("sourcing_status", "tracking_number", "carrier",
                          "ship_date", "delivery_date", "invoice_status",
                          "invoice_number", "supplier", "supplier_url", "notes"):
                if field in data:
                    old_val = it.get(field, "")
                    it[field] = data[field]
                    if field == "sourcing_status" and old_val != data[field]:
                        _log_crm_activity(order.get("quote_number",""), f"line_{data[field]}",
                                          f"Order {oid} line {lid}: {old_val} → {data[field]} — {it.get('description','')[:60]}",
                                          actor="user", metadata={"order_id": oid})
            updated = True
            break
    if not updated:
        return jsonify({"ok": False, "error": "Line item not found"})
    order["updated_at"] = datetime.now().isoformat()
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    return jsonify({"ok": True})


@bp.route("/api/order/<oid>/bulk", methods=["POST"])
@auth_required
def api_order_bulk_update(oid):
    """Bulk update all line items. POST JSON with fields to set on all items."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
    for it in order.get("line_items", []):
        for field in ("sourcing_status", "carrier", "invoice_status"):
            if field in data:
                it[field] = data[field]
    order["updated_at"] = datetime.now().isoformat()
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), "order_bulk_update",
                      f"Order {oid}: bulk update — {data}",
                      actor="user", metadata={"order_id": oid})
    return jsonify({"ok": True})


@bp.route("/api/order/<oid>/bulk-tracking", methods=["POST"])
@auth_required
def api_order_bulk_tracking(oid):
    """Add tracking to all pending/ordered items. POST: {tracking, carrier}"""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
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
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), "tracking_added",
                      f"Order {oid}: tracking {tracking} ({carrier}) added to {updated} items",
                      actor="user", metadata={"order_id": oid, "tracking": tracking})
    return jsonify({"ok": True, "updated": updated})


@bp.route("/api/order/<oid>/invoice", methods=["POST"])
@auth_required
def api_order_invoice(oid):
    """Create partial or full invoice. POST: {type: 'partial'|'full', invoice_number}"""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
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
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), f"invoice_{inv_type}",
                      f"Order {oid}: {inv_type} invoice #{inv_num} — ${order.get('invoice_total',0):,.2f}",
                      actor="user", metadata={"order_id": oid, "invoice": inv_num})
    return jsonify({"ok": True, "invoice_type": inv_type, "invoice_total": order.get("invoice_total", 0)})


@bp.route("/api/order/<oid>/invoice-pdf", methods=["POST"])
@auth_required
@rate_limit("heavy")
def api_order_invoice_pdf(oid):
    """Generate a branded invoice PDF from order's draft_invoice data.
    Returns the PDF download URL."""
    orders = _load_orders()
    order = orders.get(oid)
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
        orders[oid] = order
        _save_orders(orders)
        
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
def api_order_invoice_pdf_download(oid):
    """Download the generated invoice PDF."""
    orders = _load_orders()
    order = orders.get(oid)
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
@rate_limit("heavy")
def api_order_lookup_suppliers(oid):
    """Auto-lookup supplier links + prices for all line items with part numbers.
    POST: {line_id: 'L001'} for single item, or {} for all items.
    Uses product_research.research_product() for Amazon SerpApi lookup.
    """
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})

    data = request.get_json(silent=True) or {}
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
        orders[oid] = order
        _save_orders(orders)

    return jsonify({"ok": True, "results": results, "updated": updated, "total": len(results)})


@bp.route("/api/supplier/search")
@auth_required
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
    except Exception:
        pass

    return jsonify({"ok": True, "query": query, "search_urls": urls, "amazon": amazon_result})


# ═══════════════════════════════════════════════════════════════════════
# Quote → Order Auto-Link (#11) — link PO to existing quote
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/order/<oid>/link-quote", methods=["POST"])
@auth_required
def api_order_link_quote(oid):
    """Link an order to a quote. Auto-populates line items with quote prices/suppliers.
    POST: {quote_number} or {} to auto-detect from PO number.
    """
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})

    data = request.get_json(silent=True) or {}
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
    except Exception:
        pass

    if not quote_data:
        # Try quotes_log.json
        try:
            ql_path = os.path.join(DATA_DIR, "quotes_log.json")
            with open(ql_path) as f:
                for q in json.load(f):
                    if q.get("quote_number") == qn:
                        quote_data = q
                        break
        except Exception:
            pass

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
    orders[oid] = order
    _save_orders(orders)

    # Record pricing intelligence
    try:
        from src.knowledge.pricing_intel import record_winning_prices
        record_winning_prices(order)
    except Exception:
        pass

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
                    WHERE status IN ('sent','pending','won') 
                    ORDER BY created_at DESC LIMIT 50
                """).fetchall()
                for r in rows:
                    q_inst = (r["institution"] or "").lower()
                    q_total = r["total"] or 0
                    if q_inst and inst in q_inst or q_inst in inst:
                        if q_total > 0 and abs(q_total - total) / q_total < 0.10:
                            return r["quote_number"]
    except Exception:
        pass
    return ""


@bp.route("/api/order/<oid>/delete", methods=["POST"])
@auth_required
def api_order_delete(oid):
    """Delete/dismiss a duplicate or erroneous order. POST: {reason}"""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
    reason = data.get("reason", "duplicate")

    # Log before deleting
    _log_crm_activity(order.get("quote_number", "") or order.get("po_number", ""),
                      "order_deleted",
                      f"Order {oid} deleted. Reason: {reason}. PO: {order.get('po_number','')} Total: ${order.get('total',0):,.2f}",
                      actor="user", metadata={"order_id": oid, "reason": reason})

    del orders[oid]
    _save_orders(orders)
    log.info("Order %s deleted. Reason: %s", oid, reason)
    return jsonify({"ok": True, "deleted": oid, "reason": reason})


@bp.route("/api/order/<oid>/reply-all")
@auth_required
def api_order_reply_all(oid):
    """Draft PO confirmation reply-all email → saved to outbox for review + send."""
    orders = _load_orders()
    order = orders.get(oid)
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
   <th style="padding:6px 8px;text-align:left;border-bottom:2px solid #ddd;font-size:12px">Description</th>
   <th style="padding:6px 8px;text-align:center;border-bottom:2px solid #ddd;font-size:12px">Qty</th>
   <th style="padding:6px 8px;text-align:right;border-bottom:2px solid #ddd;font-size:12px">Unit Price</th>
   <th style="padding:6px 8px;text-align:right;border-bottom:2px solid #ddd;font-size:12px">Extended</th>
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
    <div style="font-size:12px;color:#666">Reytech Inc.</div>
    <div style="font-size:12px;color:#666">30 Carnoustie Way, Trabuco Canyon, CA 92679</div>
    <div style="font-size:12px;margin-top:4px">
     <a href="tel:9492291575" style="color:#2563eb;text-decoration:none">949-229-1575</a> |
     <a href="mailto:sales@reytechinc.com" style="color:#2563eb;text-decoration:none">sales@reytechinc.com</a>
    </div>
    <div style="font-size:11px;color:#888;margin-top:2px">SB/DVBE Cert #2002605 · <a href="https://reytechinc.com" style="color:#2563eb;text-decoration:none">reytechinc.com</a></div>
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
    except Exception:
        pass

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
        outbox_path = os.path.join(DATA_DIR, "email_outbox.json")
        try:
            with open(outbox_path) as f:
                outbox = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            outbox = []
        outbox.append(draft)
        with open(outbox_path, "w") as f:
            json.dump(outbox, f, indent=2, default=str)
    except Exception as e:
        log.error("Failed to save PO confirmation draft: %s", e)
        flash(f"Error saving draft: {e}", "error")
        return redirect(f"/order/{oid}")

    _log_crm_activity(qn, "draft_created",
                      f"PO confirmation draft for {oid} → {sender_email}",
                      actor="user", metadata={"order_id": oid, "po_number": po_num})

    flash(f"📧 PO confirmation draft saved to outbox — review and send from Agents page", "success")
    return redirect(f"/order/{oid}")


