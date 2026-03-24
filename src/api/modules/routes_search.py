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
    "pc":      ("#38bdf8", "rgba(56,189,248,.1)"),
    "rfq":     ("#58a6ff", "rgba(88,166,255,.1)"),
    "quote":   ("#3fb950", "rgba(63,185,80,.1)"),
    "buyer":   ("#a78bfa", "rgba(167,139,250,.1)"),
    "catalog": ("#fb923c", "rgba(251,146,60,.1)"),
    "order":   ("#d29922", "rgba(210,153,34,.1)"),
}


@bp.route("/search")
@auth_required
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
            label = {"pc": "Price Checks", "rfq": "RFQs", "quote": "Quotes",
                     "buyer": "Buyers", "catalog": "Catalog", "order": "Orders"}.get(t, t.title())
            filter_html += f'<a href="/search?q={display_q}&type={t}" style="font-size:13px;padding:4px 12px;border-radius:16px;border:1px solid {"rgba(79,140,255,.4)" if active else "var(--bd)"};background:{"rgba(79,140,255,.15)" if active else "var(--sf2)"};color:{"var(--ac)" if active else tc};text-decoration:none;font-weight:{"600" if active else "400"}">{label} ({count})</a>'
        filter_html += '</div>'

    # Empty state
    empty_state = ""
    if (q or status_filter) and not results:
        empty_state = f'''<div style="text-align:center;padding:40px;color:var(--tx2)">
  <div style="font-size:36px;margin-bottom:10px">🔍</div>
  <div style="font-size:15px;font-weight:600;margin-bottom:4px">No results for "{display_q}"</div>
  <div style="font-size:13px">Try a different keyword — searches PC #, item descriptions, buyer name, institution, solicitation #</div>
</div>'''

    # Type badges
    type_badges = ""
    for label, tc, tbg in [
        ("📋 Price Checks", "#38bdf8", "rgba(56,189,248,.1)"),
        ("📄 RFQs", "#58a6ff", "rgba(88,166,255,.1)"),
        ("💰 Quotes", "#3fb950", "rgba(63,185,80,.1)"),
        ("📦 Catalog", "#fb923c", "rgba(251,146,60,.1)"),
        ("👤 Buyers", "#a78bfa", "rgba(167,139,250,.1)"),
    ]:
        type_badges += f'<span style="font-size:13px;padding:4px 10px;border-radius:6px;background:{tbg};color:{tc}">{label}</span>'

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
                results.append({
                    "type": "quote", "type_icon": "💰", "type_label": "Quote",
                    "id": r[0], "title": f"Quote {r[0]}",
                    "subtitle": f"{r[1] or ''} — {r[2] or ''}".strip(" —"),
                    "url": f"/quotes",
                    "status": r[6] or "", "items": r[5] or 0,
                    "total": r[4] or 0, "due_date": "",
                    "relevance": 10 if q == (r[0] or "").lower() else 5,
                })
        except Exception as e:
            log.debug("Search quotes: %s", e)

    # 4. Buyers (SCPRS)
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
                        ORDER BY po_count DESC LIMIT 10
                    """, (f"%{query}%", f"%{query}%", f"%{query}%")).fetchall()
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

    # 5. Catalog
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
                        ORDER BY times_seen DESC LIMIT 10
                    """, (f"%{query}%", f"%{query}%")).fetchall()
                for c in catalog:
                    results.append({
                        "type": "catalog", "type_icon": "📦", "type_label": "Catalog",
                        "id": c[1] or c[0][:30],
                        "title": (c[0] or "")[:80],
                        "subtitle": f"${c[2] or 0:.2f}/{c[4] or 'EA'} — {c[3] or ''} ({c[5] or 0}× seen)",
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
