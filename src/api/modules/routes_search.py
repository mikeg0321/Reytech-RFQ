"""
Universal Search — finds PCs, RFQs, Quotes, Orders, Buyers,
and Catalog items from a single query.
"""
import logging
import json
import os
from flask import request, jsonify
from src.api.shared import bp, auth_required, api_response
from src.api.render import render_page
from src.api.dashboard import load_rfqs

log = logging.getLogger("reytech.search")

STATUS_CHIPS = {
    "sent":      ("📤 Sent",    "all", "rgba(79,140,255,.1)",  "rgba(79,140,255,.3)",  "var(--ac)"),
    "won":       ("🏆 Won",     "all", "rgba(52,211,153,.1)",  "rgba(52,211,153,.3)",  "#3fb950"),
    "draft":     ("📝 Draft",   "all", "rgba(139,148,158,.1)", "rgba(139,148,158,.3)", "#8b949e"),
    "generated": ("📦 Package", "rfq", "rgba(167,139,250,.1)", "rgba(167,139,250,.3)", "#a78bfa"),
    "ready":     ("✅ Ready",   "rfq", "rgba(52,211,153,.1)",  "rgba(52,211,153,.3)",  "#3fb950"),
    "lost":      ("❌ Lost",    "all", "rgba(248,81,73,.1)",   "rgba(248,81,73,.3)",   "#f85149"),
}

TYPE_COLORS = {
    "pc":           ("#38bdf8", "rgba(56,189,248,.1)"),
    "rfq":          ("#58a6ff", "rgba(88,166,255,.1)"),
    "quote":        ("#3fb950", "rgba(63,185,80,.1)"),
    "buyer":        ("#a78bfa", "rgba(167,139,250,.1)"),
    "catalog":      ("#fb923c", "rgba(251,146,60,.1)"),
    "order":        ("#d29922", "rgba(210,153,34,.1)"),
    "contact":      ("#c084fc", "rgba(192,132,252,.1)"),
    "vendor":       ("#f472b6", "rgba(244,114,182,.1)"),
    "vendor_order": ("#fbbf24", "rgba(251,191,36,.1)"),
    "email":        ("#67e8f9", "rgba(103,232,249,.1)"),
    "lead":         ("#86efac", "rgba(134,239,172,.1)"),
    "customer":     ("#fca5a5", "rgba(252,165,165,.1)"),
    "scprs_po":     ("#cbd5e1", "rgba(203,213,225,.1)"),
}

TYPE_LABELS = {
    "pc": "Price Checks", "rfq": "RFQs", "quote": "Quotes",
    "buyer": "Buyers", "catalog": "Catalog", "order": "Orders",
    "contact": "Contacts", "vendor": "Vendors", "vendor_order": "Supplier Orders",
    "email": "Emails", "lead": "Leads", "customer": "Customers",
    "scprs_po": "SCPRS POs",
}


@bp.route("/search")
@auth_required
@safe_page
def search_page():
    q = (request.args.get("q", "") or "").strip()
    type_filter = (request.args.get("type", "") or "").strip().lower()

    # Status-chip shortcut: ?q=sent → filter by status, not text
    status_filter = ""
    if q.lower() in STATUS_CHIPS:
        status_filter = q.lower()
        q = ""

    results = universal_search(q, status=status_filter, type_filter=type_filter) if (q and len(q) >= 1) or status_filter else []

    # Type counts for filter bar
    type_counts = {}
    for r in results:
        t = r["type"]
        type_counts[t] = type_counts.get(t, 0) + 1

    # Build rows HTML
    rows_html = ""
    for r in results:
        color, bg = TYPE_COLORS.get(r["type"], ("#8b949e", "rgba(139,148,158,.1)"))
        status_dot = _status_dot(r.get("status", ""))
        meta_parts = []
        if r.get("items"): meta_parts.append(f"{r['items']} items")
        if r.get("status"): meta_parts.append(r["status"])
        if r.get("due_date"): meta_parts.append(f"due {r['due_date'][:10]}")
        if r.get("total") and r["total"] > 0: meta_parts.append(f"${r['total']:,.0f}")
        meta_str = " · ".join(meta_parts)

        rows_html += f'''<a href="{r['url']}" class="search-result">
  <div style="display:flex;align-items:flex-start;gap:12px">
    <span style="font-size:20px;flex-shrink:0;margin-top:1px">{r["type_icon"]}</span>
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        <span class="sr-type" style="color:{color};background:{bg}">{r["type_label"]}</span>
        {status_dot}
        <span class="sr-title">{r["title"]}</span>
      </div>
      <div class="sr-detail">{r["subtitle"]}</div>
      {f'<div style="font-size:12px;color:var(--tx2);margin-top:2px">{meta_str}</div>' if meta_str else ""}
    </div>
    <span style="font-size:12px;color:var(--tx2);flex-shrink:0;padding-top:4px">→</span>
  </div>
</a>'''

    # Filter bar
    display_q = status_filter or q
    filter_html = ""
    if type_counts:
        filter_html = '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px;align-items:center">'
        filter_html += '<span style="font-size:13px;color:var(--tx2)">Filter:</span>'
        all_active = not type_filter
        filter_html += f'<a href="/search?q={display_q}" style="font-size:13px;padding:4px 12px;border-radius:16px;border:1px solid {"var(--ac)" if all_active else "var(--bd)"};background:{"rgba(79,140,255,.15)" if all_active else "var(--sf2)"};color:{"var(--ac)" if all_active else "var(--tx2)"};text-decoration:none;font-weight:{"600" if all_active else "400"}">All ({len(results)})</a>'
        for t, count in sorted(type_counts.items()):
            tc, tbg = TYPE_COLORS.get(t, ("#8b949e", "rgba(139,148,158,.1)"))
            active = type_filter == t
            label = TYPE_LABELS.get(t, t.title())
            filter_html += f'<a href="/search?q={display_q}&type={t}" style="font-size:13px;padding:4px 12px;border-radius:16px;border:1px solid {"rgba(79,140,255,.4)" if active else "var(--bd)"};background:{"rgba(79,140,255,.15)" if active else "var(--sf2)"};color:{"var(--ac)" if active else tc};text-decoration:none;font-weight:{"600" if active else "400"}">{label} ({count})</a>'
        filter_html += '</div>'

    # Empty state
    empty_state = ""
    if (q or status_filter) and not results:
        empty_state = f'''<div style="text-align:center;padding:40px;color:var(--tx2)">
  <div style="font-size:36px;margin-bottom:10px">🔍</div>
  <div style="font-size:15px;font-weight:600;margin-bottom:4px">No results for "{display_q}"</div>
  <div style="font-size:13px">Try a different keyword — searches PCs, RFQs, quotes, orders, contacts, vendors, emails, leads, catalog, PO numbers, item descriptions</div>
</div>'''

    # Type badges
    _badge_items = [
        ("📋 PCs", "pc"), ("📄 RFQs", "rfq"), ("💰 Quotes", "quote"),
        ("📦 Catalog", "catalog"), ("👤 Buyers", "buyer"), ("🛒 Orders", "order"),
        ("📇 Contacts", "contact"), ("🏭 Vendors", "vendor"),
        ("📧 Emails", "email"), ("📬 Leads", "lead"),
    ]
    type_badges = ""
    for badge_label, badge_key in _badge_items:
        tc, tbg = TYPE_COLORS.get(badge_key, ("#8b949e", "rgba(139,148,158,.1)"))
        type_badges += f'<span style="font-size:13px;padding:4px 10px;border-radius:6px;background:{tbg};color:{tc}">{badge_label}</span>'

    # Quick chips
    quick_chips_html = '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px"><span style="font-size:14px;color:var(--tx2);padding:6px 0;align-self:center">Quick:</span>'
    for chip_key, (chip_label, _, chip_bg, chip_border, chip_color) in STATUS_CHIPS.items():
        active = status_filter == chip_key
        quick_chips_html += f'<a href="/search?q={chip_key}" style="font-size:14px;padding:6px 14px;border-radius:8px;background:{"rgba(79,140,255,.2)" if active else chip_bg};border:1px solid {chip_border};color:{chip_color};text-decoration:none;font-weight:{"700" if active else "400"}">{chip_label}</a>'
    for agency in ["CDCR", "CCHCS", "CalVet", "DSH"]:
        quick_chips_html += f'<a href="/search?q={agency}" style="font-size:14px;padding:6px 14px;border-radius:8px;background:var(--sf2);border:1px solid var(--bd);color:var(--tx);text-decoration:none">{agency}</a>'
    quick_chips_html += '</div>'

    return render_page("search.html", active_page="Search",
        q=display_q, q_escaped=(display_q).replace('"', '&quot;'),
        results=results, total=len(results),
        rows_html=rows_html, breakdown_html=filter_html,
        empty_state=empty_state, type_badges=type_badges,
        quick_chips_html=quick_chips_html,
        status_filter=status_filter, type_filter=type_filter,
        error=None)


@bp.route("/api/v1/search")
@auth_required
@safe_route
def api_v1_search():
    q = (request.args.get("q", "") or "").strip()
    status = (request.args.get("status", "") or "").strip()
    type_filter = (request.args.get("type", "") or "").strip()
    limit = min(int(request.args.get("limit", 50)), 200)
    if not q and not status:
        return api_response({"results": [], "query": q, "total": 0})
    results = universal_search(q, limit=limit, status=status, type_filter=type_filter)
    type_counts = {}
    for r in results:
        type_counts[r["type"]] = type_counts.get(r["type"], 0) + 1
    return api_response({"query": q, "results": results, "total": len(results), "by_type": type_counts})


def universal_search(query, limit=50, status="", type_filter=""):
    results = []
    q = (query or "").strip().lower()

    # 1. Price Checks
    if not type_filter or type_filter == "pc":
        try:
            from src.api.dashboard import _load_price_checks
            pcs = _load_price_checks()
            for pcid, pc in pcs.items():
                if status and pc.get("status", "") != status:
                    continue
                if q and not _matches_pc(pc, pcid, q):
                    continue
                if not q and not status:
                    continue
                results.append({
                    "type": "pc", "type_icon": "📋", "type_label": "Price Check",
                    "id": pcid,
                    "title": f"PC #{pc.get('pc_number', pcid[:12])}",
                    "subtitle": pc.get("institution") or pc.get("requestor") or pc.get("requestor_name") or "",
                    "url": f"/pricecheck/{pcid}",
                    "status": pc.get("status", ""),
                    "items": len(pc.get("items", [])),
                    "due_date": pc.get("due_date", ""),
                    "total": 0,
                    "relevance": 10 if q == (pc.get("pc_number", "") or "").lower() else 5,
                })
        except Exception as e:
            log.debug("Search PCs: %s", e)

    # 2. RFQs
    if not type_filter or type_filter == "rfq":
        try:
            rfqs = load_rfqs()
            for rid, r in rfqs.items():
                if status and r.get("status", "") != status:
                    continue
                if q and not _matches_rfq(r, rid, q):
                    continue
                if not q and not status:
                    continue
                items = r.get("line_items", r.get("items", []))
                results.append({
                    "type": "rfq", "type_icon": "📄", "type_label": "RFQ",
                    "id": rid,
                    "title": f"RFQ #{r.get('solicitation_number', r.get('rfq_number', rid[:12]))}",
                    "subtitle": r.get("agency_name") or r.get("institution") or r.get("requestor_name") or "",
                    "url": f"/rfq/{rid}",
                    "status": r.get("status", ""),
                    "items": len(items),
                    "due_date": r.get("due_date", ""),
                    "total": 0,
                    "relevance": 10 if q in (r.get("solicitation_number", "") or "").lower() else 5,
                })
        except Exception as e:
            log.debug("Search RFQs: %s", e)

    # 3. Quotes (SQLite)
    if not type_filter or type_filter == "quote":
        try:
            from src.core.db import get_db
            with get_db() as conn:
                where_clauses = ["status != 'void'"]
                params = []
                if q:
                    where_clauses.append("""(quote_number LIKE ? OR institution LIKE ? OR
                         requestor LIKE ? OR rfq_number LIKE ? OR items_text LIKE ?)""")
                    params.extend([f"%{query}%"] * 5)
                if status:
                    where_clauses.append("status = ?")
                    params.append(status)
                where_sql = " AND ".join(where_clauses)
                rows = conn.execute(
                    f"SELECT quote_number, institution, requestor, rfq_number, total, items_count, status, items_text FROM quotes WHERE {where_sql} ORDER BY created_at DESC LIMIT ?",
                    params + [limit]).fetchall()
            for r in rows:
                _qn = r[0] or ""
                results.append({
                    "type": "quote", "type_icon": "💰", "type_label": "Quote",
                    "id": _qn, "title": f"Quote {_qn}",
                    "subtitle": f"{r[1] or ''} — {r[2] or ''}".strip(" —"),
                    "url": f"/quotes?q={_qn}" if _qn else "/quotes",
                    "status": r[6] or "", "items": r[5] or 0,
                    "total": r[4] or 0, "due_date": "",
                    "relevance": 10 if q == _qn.lower() else 5,
                })
        except Exception as e:
            log.debug("Search quotes: %s", e)

    # 4. Orders (SQLite)
    if not type_filter or type_filter == "order":
        try:
            from src.core.db import get_db
            with get_db() as conn:
                _oclauses = []
                _oparams = []
                if q:
                    _oclauses.append("(po_number LIKE ? OR quote_number LIKE ? OR agency LIKE ? OR institution LIKE ? OR id LIKE ?)")
                    _oparams.extend([f"%{query}%"] * 5)
                if status:
                    _oclauses.append("status = ?")
                    _oparams.append(status)
                if not _oclauses:
                    _oclauses.append("1=0")
                _owhere = " AND ".join(_oclauses)
                _orows = conn.execute(
                    f"SELECT id, po_number, quote_number, agency, institution, total, status FROM orders WHERE {_owhere} ORDER BY created_at DESC LIMIT ?",
                    _oparams + [limit]).fetchall()
            for o in _orows:
                _po = o[1] or ""
                _qq = o[2] or ""
                results.append({
                    "type": "order", "type_icon": "🛒", "type_label": "Order",
                    "id": o[0], "title": f"PO {_po}" if _po else f"Order {o[0][:12]}",
                    "subtitle": f"{o[3] or ''} — {o[4] or ''}{(' — Quote ' + _qq) if _qq else ''}".strip(" —"),
                    "url": f"/order/{o[0]}",
                    "status": o[6] or "", "total": o[5] or 0, "due_date": "",
                    "relevance": 10 if q == _po.lower() or q == _qq.lower() else 7,
                })
        except Exception as e:
            log.debug("Search orders: %s", e)

    # 5. Contacts (SQLite)
    if not type_filter or type_filter == "contact":
        if q:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    _crows = conn.execute("""
                        SELECT id, buyer_name, buyer_email, agency, department,
                               total_spend, po_count, outreach_status
                        FROM contacts
                        WHERE buyer_name LIKE ? OR buyer_email LIKE ? OR agency LIKE ?
                              OR department LIKE ? OR id LIKE ?
                        ORDER BY total_spend DESC LIMIT ?
                    """, (f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", limit)).fetchall()
                for c in _crows:
                    _cname = c[1] or "Unknown"
                    _cemail = c[2] or ""
                    _cagency = c[3] or ""
                    _cdept = c[4] or ""
                    _sub_parts = [x for x in [_cagency, _cdept, _cemail] if x]
                    results.append({
                        "type": "contact", "type_icon": "📇", "type_label": "Contact",
                        "id": c[0], "title": _cname,
                        "subtitle": " · ".join(_sub_parts),
                        "url": f"/contacts?q={_cname}",
                        "status": c[7] or "", "total": c[5] or 0,
                        "items": c[6] or 0, "due_date": "",
                        "relevance": 8 if q == (_cemail or "").lower() else 5,
                    })
            except Exception as e:
                log.debug("Search contacts: %s", e)

    # 6. Buyers (SCPRS) — only if no contacts matched or type-filtered
    if not type_filter or type_filter == "buyer":
        if q:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    buyers = conn.execute("""
                        SELECT buyer_name, buyer_email, department,
                               COUNT(*) as po_count, SUM(grand_total) as total_spend
                        FROM scprs_po_master
                        WHERE buyer_name LIKE ? OR buyer_email LIKE ? OR department LIKE ?
                        GROUP BY buyer_name, buyer_email
                        ORDER BY po_count DESC LIMIT ?
                    """, (f"%{query}%", f"%{query}%", f"%{query}%", limit)).fetchall()
                for b in buyers:
                    results.append({
                        "type": "buyer", "type_icon": "👤", "type_label": "Buyer",
                        "id": b[1] or b[0],
                        "title": b[0] or "Unknown",
                        "subtitle": f"{b[2] or ''} · {b[3]} POs · ${(b[4] or 0):,.0f}",
                        "url": f"/contacts?q={b[0] or b[1]}",
                        "status": "", "total": b[4] or 0, "due_date": "",
                        "relevance": 6,
                    })
            except Exception as e:
                log.debug("Search buyers: %s", e)

    # 7. Vendors (SQLite)
    if not type_filter or type_filter == "vendor":
        if q:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    _vrows = conn.execute("""
                        SELECT id, name, company, email, phone, overall_score, categories_served
                        FROM vendors
                        WHERE name LIKE ? OR company LIKE ? OR email LIKE ? OR categories_served LIKE ?
                        ORDER BY overall_score DESC LIMIT ?
                    """, (f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", limit)).fetchall()
                for v in _vrows:
                    _vname = v[1] or ""
                    _vcompany = v[2] or ""
                    _vemail = v[3] or ""
                    _vscore = v[5] or 0
                    _sub = f"{_vcompany}{(' — ' + _vemail) if _vemail else ''}{(' — score ' + str(round(_vscore, 1))) if _vscore else ''}"
                    results.append({
                        "type": "vendor", "type_icon": "🏭", "type_label": "Vendor",
                        "id": str(v[0]), "title": _vname,
                        "subtitle": _sub,
                        "url": f"/vendors?q={_vname}",
                        "status": "", "total": 0, "due_date": "",
                        "relevance": 7 if q == _vname.lower() else 4,
                    })
            except Exception as e:
                log.debug("Search vendors: %s", e)

    # 8. Vendor Orders / Supplier Orders (SQLite)
    if not type_filter or type_filter == "vendor_order":
        if q:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    _vorows = conn.execute("""
                        SELECT id, vendor_name, po_number, order_number, quote_number,
                               total, status, submitted_at
                        FROM vendor_orders
                        WHERE po_number LIKE ? OR order_number LIKE ? OR quote_number LIKE ?
                              OR vendor_name LIKE ?
                        ORDER BY submitted_at DESC LIMIT ?
                    """, (f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", limit)).fetchall()
                for vo in _vorows:
                    _vpo = vo[2] or ""
                    _vord = vo[3] or ""
                    _vqt = vo[4] or ""
                    _vtitle = f"PO {_vpo}" if _vpo else f"Order #{vo[0]}"
                    _vsub = f"{vo[1] or ''}{(' — vendor #' + _vord) if _vord else ''}{(' — quote ' + _vqt) if _vqt else ''}"
                    results.append({
                        "type": "vendor_order", "type_icon": "📋", "type_label": "Supplier Order",
                        "id": str(vo[0]), "title": _vtitle,
                        "subtitle": _vsub,
                        "url": "/orders",
                        "status": vo[6] or "", "total": vo[5] or 0, "due_date": "",
                        "relevance": 8 if q == _vpo.lower() or q == _vord.lower() else 5,
                    })
            except Exception as e:
                log.debug("Search vendor_orders: %s", e)

    # 9. Email Log (SQLite)
    if not type_filter or type_filter == "email":
        if q:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    _erows = conn.execute("""
                        SELECT id, sender, recipient, subject, quote_number, po_number,
                               direction, logged_at, status
                        FROM email_log
                        WHERE sender LIKE ? OR recipient LIKE ? OR subject LIKE ?
                              OR quote_number LIKE ? OR po_number LIKE ?
                        ORDER BY logged_at DESC LIMIT ?
                    """, (f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", limit)).fetchall()
                for e in _erows:
                    _esubj = e[3] or "No subject"
                    _edir = e[6] or "out"
                    _eicon = "📤" if _edir == "out" else "📥"
                    _edate = (e[7] or "")[:10]
                    _esub = f"{_eicon} {e[1] or ''} → {e[2] or ''}{(' — ' + _edate) if _edate else ''}"
                    _eurl = f"/contacts?q={e[2] or e[1]}"
                    results.append({
                        "type": "email", "type_icon": "📧", "type_label": "Email",
                        "id": str(e[0]), "title": _esubj[:80],
                        "subtitle": _esub,
                        "url": _eurl,
                        "status": e[8] or "", "total": 0, "due_date": "",
                        "relevance": 5,
                    })
            except Exception as e_err:
                log.debug("Search email_log: %s", e_err)

    # 10. Leads (SQLite)
    if not type_filter or type_filter == "lead":
        if q:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    _lrows = conn.execute("""
                        SELECT id, buyer_name, buyer_email, agency, po_number, po_value, status
                        FROM leads
                        WHERE buyer_name LIKE ? OR buyer_email LIKE ? OR agency LIKE ?
                              OR po_number LIKE ?
                        ORDER BY po_value DESC LIMIT ?
                    """, (f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", limit)).fetchall()
                for ld in _lrows:
                    _lname = ld[1] or "Unknown"
                    _lagency = ld[3] or ""
                    _lpo = ld[4] or ""
                    _lval = ld[5] or 0
                    _lsub = f"{_lagency}{(' — PO ' + _lpo) if _lpo else ''}{(' — $' + f'{_lval:,.0f}') if _lval else ''}"
                    results.append({
                        "type": "lead", "type_icon": "📬", "type_label": "Lead",
                        "id": str(ld[0]), "title": _lname,
                        "subtitle": _lsub,
                        "url": f"/contacts?q={_lname}",
                        "status": ld[6] or "", "total": _lval, "due_date": "",
                        "relevance": 5,
                    })
            except Exception as e:
                log.debug("Search leads: %s", e)

    # 11. Customers (SQLite)
    if not type_filter or type_filter == "customer":
        if q:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    _curows = conn.execute("""
                        SELECT id, display_name, email, qb_name, agency, open_balance
                        FROM customers
                        WHERE display_name LIKE ? OR email LIKE ? OR qb_name LIKE ? OR agency LIKE ?
                        ORDER BY open_balance DESC LIMIT ?
                    """, (f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", limit)).fetchall()
                for cu in _curows:
                    _cuname = cu[1] or cu[3] or "Unknown"
                    _cuemail = cu[2] or ""
                    _cuagency = cu[4] or ""
                    _cubal = cu[5] or 0
                    _cusub = f"{_cuagency}{(' — ' + _cuemail) if _cuemail else ''}{(' — bal $' + f'{_cubal:,.0f}') if _cubal else ''}"
                    results.append({
                        "type": "customer", "type_icon": "🏢", "type_label": "Customer",
                        "id": str(cu[0]), "title": _cuname,
                        "subtitle": _cusub,
                        "url": f"/contacts?q={_cuname}",
                        "status": "", "total": _cubal, "due_date": "",
                        "relevance": 5,
                    })
            except Exception as e:
                log.debug("Search customers: %s", e)

    # 12. SCPRS POs (SQLite)
    if not type_filter or type_filter == "scprs_po":
        if q:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    _sprows = conn.execute("""
                        SELECT po_number, buyer_name, agency, grand_total, po_date
                        FROM scprs_po_master
                        WHERE po_number LIKE ? OR buyer_name LIKE ? OR agency LIKE ?
                        ORDER BY po_date DESC LIMIT ?
                    """, (f"%{query}%", f"%{query}%", f"%{query}%", limit)).fetchall()
                for sp in _sprows:
                    _sppo = sp[0] or ""
                    _spbuyer = sp[1] or ""
                    _spagency = sp[2] or ""
                    _sptotal = sp[3] or 0
                    _spdate = (sp[4] or "")[:10]
                    results.append({
                        "type": "scprs_po", "type_icon": "🏛️", "type_label": "SCPRS PO",
                        "id": _sppo, "title": f"PO {_sppo}",
                        "subtitle": f"{_spbuyer} — {_spagency}{(' — ' + _spdate) if _spdate else ''}",
                        "url": f"/contacts?q={_spbuyer}",
                        "status": "", "total": _sptotal, "due_date": "",
                        "relevance": 4,
                    })
            except Exception as e:
                log.debug("Search scprs_po: %s", e)

    # 13. Catalog (increased limit)
    if not type_filter or type_filter == "catalog":
        if q:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    catalog = conn.execute("""
                        SELECT description, item_number, last_unit_price, last_supplier,
                               last_uom, times_seen
                        FROM scprs_catalog
                        WHERE description LIKE ? OR item_number LIKE ?
                        ORDER BY times_seen DESC LIMIT ?
                    """, (f"%{query}%", f"%{query}%", limit)).fetchall()
                for c in catalog:
                    results.append({
                        "type": "catalog", "type_icon": "📦", "type_label": "Catalog",
                        "id": c[1] or c[0][:30],
                        "title": (c[0] or "")[:80],
                        "subtitle": f"${c[2] or 0:.2f}/{c[4] or 'EA'} — {c[3] or ''} ({c[5] or 0}x seen)",
                        "url": f"/catalog?q={query}",
                        "status": "", "total": c[2] or 0, "due_date": "",
                        "relevance": 3,
                    })
            except Exception as e:
                log.debug("Search catalog: %s", e)

    results.sort(key=lambda x: (-x.get("relevance", 0), x.get("type", "")))
    return results[:limit]


def _matches_pc(pc, pcid, q):
    """Search PC record including all item descriptions."""
    top_fields = [
        pc.get("pc_number", ""), pc.get("requestor", ""), pc.get("requestor_name", ""),
        pc.get("institution", ""), pc.get("email_subject", ""), pc.get("requestor_email", ""),
        pc.get("ship_to", ""), pcid,
    ]
    if any(q in (f or "").lower() for f in top_fields):
        return True
    for item in pc.get("items", []):
        if not isinstance(item, dict):
            continue
        desc = (item.get("description") or item.get("desc") or
                item.get("item_desc") or item.get("name") or
                item.get("item_description") or "").lower()
        part = (item.get("item_number") or item.get("part_number") or
                item.get("mfg_number") or "").lower()
        if q in desc or (part and q in part):
            return True
    return False


def _matches_rfq(r, rid, q):
    """Search RFQ record including all line item descriptions."""
    top_fields = [
        r.get("solicitation_number", ""), r.get("rfq_number", ""),
        r.get("requestor_name", ""), r.get("requestor_email", ""),
        r.get("agency_name", ""), r.get("institution", ""),
        r.get("email_subject", ""), rid,
    ]
    if any(q in (f or "").lower() for f in top_fields):
        return True
    for item in r.get("line_items", r.get("items", [])):
        if not isinstance(item, dict):
            continue
        desc = (item.get("description") or item.get("desc") or item.get("name") or "").lower()
        part = (item.get("item_number") or item.get("part_number") or "").lower()
        if q in desc or (part and q in part):
            return True
    return False


def _status_dot(status):
    colors = {
        "sent": "#58a6ff", "won": "#3fb950", "generated": "#a78bfa",
        "ready": "#3fb950", "lost": "#f85149", "draft": "#8b949e",
        "new": "#d29922", "parsed": "#d29922",
    }
    color = colors.get(status, "#8b949e")
    if not status:
        return ""
    return f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{color};flex-shrink:0"></span>'
