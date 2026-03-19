"""
Universal Search — finds PCs, RFQs, Quotes, Orders, Buyers,
and Catalog items from a single query.
"""
import logging
import json
import os
from flask import request, jsonify
from src.api.shared import bp, auth_required, api_response
from src.api.dashboard import load_rfqs

log = logging.getLogger("reytech.search")


@bp.route("/search")
@auth_required
def search_page():
    q = (request.args.get("q", "") or "").strip()
    results = universal_search(q) if q and len(q) >= 2 else []
    return render_page("search.html", active_page="Search",
                      query=q, results=results, total=len(results))


@bp.route("/api/v1/search")
@auth_required
def api_v1_search():
    q = (request.args.get("q", "") or "").strip()
    limit = min(int(request.args.get("limit", 50)), 200)
    if not q or len(q) < 2:
        return api_response({"results": [], "query": q, "total": 0})
    results = universal_search(q, limit=limit)
    groups = {}
    for r in results:
        t = r["type"]
        if t not in groups:
            groups[t] = 0
        groups[t] += 1
    return api_response({"query": q, "results": results, "total": len(results), "by_type": groups})


def universal_search(query, limit=50):
    results = []
    q = query.strip().lower()

    # 1. Price Checks
    try:
        from src.api.dashboard import _load_price_checks
        pcs = _load_price_checks()
        for pcid, pc in pcs.items():
            if _matches(pc, pcid, q, ["pc_number", "requestor", "institution",
                                       "email_subject", "requestor_email"]):
                results.append({
                    "type": "pc", "type_icon": "📋", "type_label": "Price Check",
                    "id": pcid, "title": f"PC #{pc.get('pc_number', pcid)}",
                    "subtitle": pc.get("institution", pc.get("requestor", "")),
                    "url": f"/pricecheck/{pcid}",
                    "status": pc.get("status", ""),
                    "items": len(pc.get("items", [])),
                    "relevance": 10 if q == (pc.get("pc_number", "") or "").lower() else 5,
                })
    except Exception as e:
        log.debug("Search PCs: %s", e)

    # 2. RFQs
    try:
        rfqs = load_rfqs()
        for rid, r in rfqs.items():
            if _matches(r, rid, q, ["solicitation_number", "rfq_number", "requestor_name",
                                     "requestor_email", "institution", "email_subject"]):
                items = r.get("line_items", r.get("items", []))
                results.append({
                    "type": "rfq", "type_icon": "📄", "type_label": "RFQ",
                    "id": rid, "title": f"RFQ #{r.get('solicitation_number', r.get('rfq_number', rid))}",
                    "subtitle": r.get("institution", r.get("requestor_name", "")),
                    "url": f"/rfq/{rid}",
                    "status": r.get("status", ""),
                    "items": len(items),
                    "due_date": r.get("due_date", ""),
                    "relevance": 10 if q == (r.get("solicitation_number", "") or "").lower() else 5,
                })
    except Exception as e:
        log.debug("Search RFQs: %s", e)

    # 3. Quotes (SQLite)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT quote_number, institution, requestor, rfq_number,
                       total, items_count, status, items_text, pdf_path
                FROM quotes
                WHERE status != 'void'
                AND (quote_number LIKE ? OR institution LIKE ? OR
                     requestor LIKE ? OR rfq_number LIKE ? OR items_text LIKE ?)
                ORDER BY created_at DESC LIMIT ?
            """, (f"%{query}%", f"%{query}%", f"%{query}%",
                  f"%{query}%", f"%{query}%", limit)).fetchall()
        for r in rows:
            results.append({
                "type": "quote", "type_icon": "💰", "type_label": "Quote",
                "id": r[0], "title": f"Quote {r[0]}",
                "subtitle": f"{r[1] or ''} — {r[2] or ''}".strip(" —"),
                "url": f"/quote/{r[0]}",
                "status": r[6] or "", "items": r[5] or 0,
                "total": r[4] or 0, "has_pdf": bool(r[8]),
                "items_preview": (r[7] or "")[:100],
                "relevance": 10 if q == (r[0] or "").lower() else 5,
            })
    except Exception as e:
        log.debug("Search quotes: %s", e)

    # 4. Buyers (SCPRS)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            buyers = conn.execute("""
                SELECT buyer_name, buyer_email, dept_name,
                       COUNT(*) as po_count, SUM(grand_total) as total_spend
                FROM scprs_po_master
                WHERE buyer_name LIKE ? OR buyer_email LIKE ?
                GROUP BY buyer_name, buyer_email
                ORDER BY po_count DESC LIMIT 10
            """, (f"%{query}%", f"%{query}%")).fetchall()
        for b in buyers:
            results.append({
                "type": "buyer", "type_icon": "👤", "type_label": "Buyer",
                "id": b[1] or b[0],
                "title": b[0] or "Unknown",
                "subtitle": f"{b[2] or ''} — {b[3]} POs, ${(b[4] or 0):,.0f}",
                "url": f"/contacts?q={b[0] or b[1]}",
                "status": "", "total": b[4] or 0, "relevance": 6,
            })
    except Exception as e:
        log.debug("Search buyers: %s", e)

    # 5. Catalog items
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
                "subtitle": f"${c[2] or 0:.2f}/{c[4] or 'EA'} — {c[3] or ''} ({c[5] or 0}x seen)",
                "url": f"/catalog?q={query}",
                "status": "", "total": c[2] or 0, "relevance": 3,
            })
    except Exception as e:
        log.debug("Search catalog: %s", e)

    results.sort(key=lambda x: -x.get("relevance", 0))
    return results[:limit]


def _matches(record, rid, q, fields):
    if q in rid.lower():
        return True
    for f in fields:
        val = (record.get(f, "") or "").lower()
        if q in val:
            return True
    # Check item descriptions
    for item in record.get("items", record.get("line_items", [])):
        desc = (item.get("description", item.get("desc", "")) or "").lower()
        if q in desc:
            return True
    return False
