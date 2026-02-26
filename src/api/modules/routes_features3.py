"""
routes_features3.py — 15+ New Endpoints for Agents Page v3
Focused on: actionable intelligence, combined dashboards, button reliability
"""
import os, json, glob, time, logging, traceback
from datetime import datetime, timedelta
from collections import defaultdict

log = logging.getLogger("features3")
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")


# ═══════════════════════════════════════════════════════════════════════
# 1. Batch Endpoint Tester — test ALL critical endpoints at once
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/agents/batch-test")
@auth_required
def api_agents_batch_test():
    """Test all critical endpoints and return their status."""
    import requests as _req
    from flask import request as flask_req
    
    endpoints = [
        ("/api/health", "Health"),
        ("/api/qb/test-connection", "QuickBooks"),
        ("/api/qa/health", "QA Agent"),
        ("/api/agents/status", "Fleet Status"),
        ("/api/scanner/status", "Email Scanner"),
        ("/api/catalog/stats", "Catalog"),
        ("/api/pipeline/stats", "Pipeline"),
        ("/api/crm/activity?limit=1", "CRM Activity"),
        ("/api/price-alerts?limit=1", "Price Alerts"),
        ("/api/follow-ups/summary", "Follow-Ups"),
        ("/api/manager/brief", "Daily Brief"),
        ("/api/qb/financial-context", "QB Finance"),
        ("/api/win-loss-analytics", "Win/Loss"),
        ("/api/revenue/dashboard", "Revenue"),
    ]
    
    results = []
    passed = 0
    auth = flask_req.authorization
    
    for path, name in endpoints:
        t0 = time.time()
        try:
            base = flask_req.host_url.rstrip("/")
            resp = _req.get(f"{base}{path}", 
                          auth=(auth.username, auth.password) if auth else None,
                          timeout=8,
                          verify=False)
            ms = int((time.time() - t0) * 1000)
            ok = resp.status_code == 200
            try:
                data = resp.json()
                ok = data.get("ok", True) != False and resp.status_code == 200
                err = data.get("error", "") if not ok else ""
            except:
                err = f"HTTP {resp.status_code}" if not ok else ""
            
            if ok:
                passed += 1
            results.append({
                "name": name, "endpoint": path, "ok": ok,
                "ms": ms, "status": resp.status_code,
                "error": err[:100] if err else None
            })
        except Exception as e:
            ms = int((time.time() - t0) * 1000)
            results.append({
                "name": name, "endpoint": path, "ok": False,
                "ms": ms, "status": "timeout", "error": str(e)[:100]
            })
    
    return jsonify({
        "ok": True,
        "passed": passed,
        "total": len(endpoints),
        "grade": "A" if passed == len(endpoints) else "B" if passed >= len(endpoints) * 0.8 else "C" if passed >= len(endpoints) * 0.6 else "F",
        "results": results,
        "tested_at": datetime.now().isoformat()
    })


# ═══════════════════════════════════════════════════════════════════════
# 2. QB Quick Dashboard — everything in ONE call
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/qb/quick-dashboard")
@auth_required
def api_qb_quick_dashboard():
    """Combined QB dashboard — invoices, payments, overdue, customers in one call."""
    try:
        from src.agents.quickbooks_agent import (
            is_configured, fetch_invoices, fetch_customers, 
            fetch_vendors, fetch_payments, get_company_info
        )
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured. Click 'Connect QuickBooks' first."})
        
        company = get_company_info() or {}
        invoices = fetch_invoices() or []
        customers = fetch_customers() or []
        vendors = fetch_vendors() or []
        payments = fetch_payments() or []
        
        open_inv = [i for i in invoices if i.get("Balance", 0) > 0]
        overdue = [i for i in open_inv if i.get("DueDate") and i["DueDate"] < datetime.now().strftime("%Y-%m-%d")]
        
        total_receivable = sum(i.get("Balance", 0) for i in open_inv)
        total_overdue = sum(i.get("Balance", 0) for i in overdue)
        total_paid = sum(p.get("TotalAmt", 0) for p in payments)
        
        return jsonify({
            "ok": True,
            "company": company.get("CompanyName", "Unknown"),
            "summary": {
                "total_receivable": round(total_receivable, 2),
                "total_overdue": round(total_overdue, 2),
                "total_collected_30d": round(total_paid, 2),
                "open_invoices": len(open_inv),
                "overdue_invoices": len(overdue),
                "customers": len(customers),
                "vendors": len(vendors),
            },
            "top_overdue": [
                {"customer": i.get("CustomerRef", {}).get("name", "?"),
                 "amount": i.get("Balance", 0),
                 "due": i.get("DueDate", "?"),
                 "invoice": i.get("DocNumber", "?")}
                for i in sorted(overdue, key=lambda x: x.get("Balance", 0), reverse=True)[:5]
            ],
            "recent_payments": [
                {"customer": p.get("CustomerRef", {}).get("name", "?"),
                 "amount": p.get("TotalAmt", 0),
                 "date": p.get("TxnDate", "?")}
                for p in sorted(payments, key=lambda x: x.get("TxnDate", ""), reverse=True)[:5]
            ]
        })
    except ImportError:
        return jsonify({"ok": False, "error": "QuickBooks agent not available"})
    except Exception as e:
        log.error(f"QB quick dashboard error: {e}")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════
# 3. Daily Wins — what went right today
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/daily-wins")
@auth_required 
def api_daily_wins():
    """Today's wins: new quotes, orders, payments, won bids."""
    today = datetime.now().strftime("%Y-%m-%d")
    wins = []
    
    # Check for new orders
    orders_path = os.path.join(DATA_DIR, "orders.json")
    if os.path.exists(orders_path):
        try:
            with open(orders_path) as f:
                orders = json.load(f)
            for oid, o in orders.items():
                if o.get("created", "").startswith(today):
                    wins.append({"type": "🎉 New Order", "detail": f"PO {o.get('po_number', oid)}", 
                                "value": o.get("total", 0), "time": o.get("created", "")})
        except: pass
    
    # Check for won quotes
    wl_path = os.path.join(DATA_DIR, "win_loss_log.json")
    if os.path.exists(wl_path):
        try:
            with open(wl_path) as f:
                wl = json.load(f)
            for entry in wl.get("entries", []):
                if entry.get("outcome") == "won" and entry.get("date", "").startswith(today):
                    wins.append({"type": "🏆 Won Quote", "detail": entry.get("rfq_id", "?"),
                                "value": entry.get("amount", 0), "time": entry.get("date", "")})
        except: pass
    
    # Check for sent quotes
    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    if os.path.exists(rfqs_path):
        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
            for rid, r in rfqs.items():
                if r.get("status") == "sent" and r.get("sent_date", "").startswith(today):
                    wins.append({"type": "📤 Sent Quote", "detail": f"Sol# {r.get('solicitation_number', rid)[:20]}",
                                "value": r.get("total_price", 0), "time": r.get("sent_date", "")})
        except: pass
    
    total_value = sum(w.get("value", 0) for w in wins if isinstance(w.get("value"), (int, float)))
    
    return jsonify({
        "ok": True,
        "date": today,
        "wins": sorted(wins, key=lambda w: w.get("time", ""), reverse=True),
        "total_wins": len(wins),
        "total_value": round(total_value, 2),
        "message": f"🎉 {len(wins)} wins today!" if wins else "No wins yet today — keep pushing!"
    })


# ═══════════════════════════════════════════════════════════════════════
# 4. RFQs Ready to Quote — what needs attention NOW  
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/ready-to-quote")
@auth_required
def api_rfq_ready_to_quote():
    """RFQs that need pricing/quoting — prioritized by deadline."""
    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    if not os.path.exists(rfqs_path):
        return jsonify({"ok": True, "rfqs": [], "count": 0})
    
    try:
        with open(rfqs_path) as f:
            rfqs = json.load(f)
    except:
        return jsonify({"ok": True, "rfqs": [], "count": 0})
    
    today = datetime.now().strftime("%Y-%m-%d")
    ready = []
    
    for rid, r in rfqs.items():
        status = (r.get("status") or "").lower()
        if status in ("new", "draft", "priced", "inbox"):
            due = r.get("due_date") or r.get("deadline") or ""
            sol = r.get("solicitation_number", rid)
            items = r.get("line_items") or r.get("items_detail") or []
            if isinstance(items, str):
                try: items = json.loads(items)
                except: items = []
            
            overdue = due and due < today
            days_left = None
            if due:
                try:
                    dd = datetime.strptime(due[:10], "%Y-%m-%d")
                    days_left = (dd - datetime.now()).days
                except: pass
            
            ready.append({
                "id": rid,
                "solicitation": sol[:30],
                "requestor": r.get("requestor", r.get("buyer_name", "?")),
                "institution": r.get("institution", "?"),
                "status": status.upper(),
                "items": len(items) if isinstance(items, list) else 0,
                "due": due[:10] if due else "TBD",
                "days_left": days_left,
                "overdue": overdue,
                "total": r.get("total_price", 0),
            })
    
    # Sort: overdue first, then by days_left
    ready.sort(key=lambda x: (not x["overdue"], x["days_left"] if x["days_left"] is not None else 999))
    
    return jsonify({
        "ok": True,
        "rfqs": ready[:20],
        "count": len(ready),
        "overdue": len([r for r in ready if r["overdue"]]),
        "due_this_week": len([r for r in ready if r.get("days_left") is not None and 0 <= r["days_left"] <= 7])
    })


# ═══════════════════════════════════════════════════════════════════════
# 5. Quick Quote Lookup — search by quote number or sol number
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/quote/lookup")
@auth_required
def api_quote_lookup():
    """Lookup a quote by number, solicitation, or keyword."""
    from flask import request as req
    q = (req.args.get("q") or "").strip()
    if not q:
        return jsonify({"ok": False, "error": "Provide ?q=<quote_number>"})
    
    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    quotes_dir = os.path.join(DATA_DIR, "quotes")
    results = []
    
    if os.path.exists(rfqs_path):
        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
            for rid, r in rfqs.items():
                sol = r.get("solicitation_number", "")
                qn = r.get("quote_number", "")
                buyer = r.get("requestor", r.get("buyer_name", ""))
                if q.lower() in rid.lower() or q.lower() in sol.lower() or q.lower() in (qn or "").lower() or q.lower() in buyer.lower():
                    results.append({
                        "id": rid, "solicitation": sol[:30],
                        "quote_number": qn, "status": r.get("status", "?"),
                        "requestor": buyer, "institution": r.get("institution", "?"),
                        "total": r.get("total_price", 0),
                        "created": r.get("created", r.get("received_date", "?")),
                    })
        except: pass
    
    return jsonify({
        "ok": True,
        "query": q,
        "results": results[:20],
        "count": len(results),
    })


# ═══════════════════════════════════════════════════════════════════════
# 6. Business Health Score — overall health metric
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/business/health-score")
@auth_required
def api_business_health_score():
    """Calculate overall business health score (0-100)."""
    score = 0
    factors = []
    
    # Factor 1: Active pipeline (0-20 pts)
    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    try:
        with open(rfqs_path) as f:
            rfqs = json.load(f)
        active = [r for r in rfqs.values() if (r.get("status") or "").lower() in ("new", "draft", "priced", "sent", "quoted")]
        pts = min(20, len(active) * 4)
        score += pts
        factors.append({"name": "Active Pipeline", "score": pts, "max": 20, "detail": f"{len(active)} active RFQs"})
    except:
        factors.append({"name": "Active Pipeline", "score": 0, "max": 20, "detail": "No data"})
    
    # Factor 2: Win rate (0-20 pts)
    wl_path = os.path.join(DATA_DIR, "win_loss_log.json")
    try:
        with open(wl_path) as f:
            wl = json.load(f)
        entries = wl.get("entries", [])
        won = len([e for e in entries if e.get("outcome") == "won"])
        lost = len([e for e in entries if e.get("outcome") == "lost"])
        if won + lost > 0:
            rate = won / (won + lost) * 100
            pts = min(20, int(rate / 5))
            factors.append({"name": "Win Rate", "score": pts, "max": 20, "detail": f"{rate:.0f}% ({won}W/{lost}L)"})
        else:
            pts = 10
            factors.append({"name": "Win Rate", "score": pts, "max": 20, "detail": "No outcomes tracked"})
        score += pts
    except:
        score += 10
        factors.append({"name": "Win Rate", "score": 10, "max": 20, "detail": "No data"})
    
    # Factor 3: Catalog completeness (0-15 pts)
    cat_path = os.path.join(DATA_DIR, "product_catalog.json")
    try:
        with open(cat_path) as f:
            cat = json.load(f)
        products = cat.get("products", {})
        with_pricing = len([p for p in products.values() if p.get("last_quoted_price", 0) > 0])
        pts = min(15, len(products) // 50)
        score += pts
        factors.append({"name": "Catalog", "score": pts, "max": 15, "detail": f"{len(products)} products, {with_pricing} priced"})
    except:
        factors.append({"name": "Catalog", "score": 0, "max": 15, "detail": "No data"})
    
    # Factor 4: QuickBooks connected (0-15 pts)
    try:
        from src.agents.quickbooks_agent import is_configured, get_access_token
        if is_configured() and get_access_token():
            score += 15
            factors.append({"name": "QuickBooks", "score": 15, "max": 15, "detail": "Connected"})
        elif is_configured():
            score += 5
            factors.append({"name": "QuickBooks", "score": 5, "max": 15, "detail": "Configured but token expired"})
        else:
            factors.append({"name": "QuickBooks", "score": 0, "max": 15, "detail": "Not connected"})
    except:
        factors.append({"name": "QuickBooks", "score": 0, "max": 15, "detail": "Module not available"})
    
    # Factor 5: Follow-up discipline (0-15 pts)
    fu_path = os.path.join(DATA_DIR, "follow_up_state.json")
    try:
        with open(fu_path) as f:
            fu = json.load(f)
        overdue = len([f for f in fu.values() if isinstance(f, dict) and f.get("status") == "overdue"])
        pending = len([f for f in fu.values() if isinstance(f, dict) and f.get("status") == "pending"])
        pts = max(0, 15 - overdue * 3)
        score += pts
        factors.append({"name": "Follow-Ups", "score": pts, "max": 15, "detail": f"{pending} pending, {overdue} overdue"})
    except:
        score += 10
        factors.append({"name": "Follow-Ups", "score": 10, "max": 15, "detail": "No data"})
    
    # Factor 6: CRM contacts (0-15 pts)
    crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
    try:
        with open(crm_path) as f:
            crm = json.load(f)
        contacts = crm.get("contacts", [])
        with_email = len([c for c in contacts if c.get("email")])
        pts = min(15, len(contacts) // 5)
        score += pts
        factors.append({"name": "CRM Contacts", "score": pts, "max": 15, "detail": f"{len(contacts)} contacts, {with_email} with email"})
    except:
        factors.append({"name": "CRM Contacts", "score": 0, "max": 15, "detail": "No data"})
    
    grade = "A+" if score >= 90 else "A" if score >= 80 else "B" if score >= 65 else "C" if score >= 50 else "D" if score >= 35 else "F"
    
    return jsonify({
        "ok": True,
        "score": min(100, score),
        "grade": grade,
        "factors": factors,
        "recommendations": _get_health_recommendations(factors),
        "calculated_at": datetime.now().isoformat()
    })


def _get_health_recommendations(factors):
    """Generate recommendations based on health score factors."""
    recs = []
    for f in factors:
        if f["score"] < f["max"] * 0.5:
            if f["name"] == "Active Pipeline":
                recs.append("📋 Pipeline is thin — check inbox for new RFQs or run SCPRS deep pull")
            elif f["name"] == "Win Rate":
                recs.append("📊 Track win/loss outcomes on RFQ detail pages to improve your rate")
            elif f["name"] == "Catalog":
                recs.append("📦 Run 'Rebuild Catalog from History' to auto-populate from quotes")
            elif f["name"] == "QuickBooks":
                recs.append("💰 Connect QuickBooks for financial tracking & invoice creation")
            elif f["name"] == "Follow-Ups":
                recs.append("📧 You have overdue follow-ups — check Follow-Up page")
            elif f["name"] == "CRM Contacts":
                recs.append("👤 Sync QB customers → CRM to build your contact database")
    return recs


# ═══════════════════════════════════════════════════════════════════════
# 7. Competitor Price Intel — what are competitors charging?
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/competitor/price-intel")
@auth_required
def api_competitor_price_intel():
    """Analyze competitor pricing from won/lost data."""
    wl_path = os.path.join(DATA_DIR, "win_loss_log.json")
    intel = {"competitors": {}, "insights": []}
    
    try:
        with open(wl_path) as f:
            wl = json.load(f)
        
        for entry in wl.get("entries", []):
            comp = entry.get("competitor_name") or entry.get("notes", "")
            price = entry.get("competitor_price", 0)
            outcome = entry.get("outcome", "")
            our_price = entry.get("our_price", 0)
            
            if comp and comp != "Unknown":
                if comp not in intel["competitors"]:
                    intel["competitors"][comp] = {"won_against": 0, "lost_to": 0, "avg_price_diff": []}
                
                if outcome == "won":
                    intel["competitors"][comp]["won_against"] += 1
                elif outcome == "lost":
                    intel["competitors"][comp]["lost_to"] += 1
                
                if price and our_price and price > 0:
                    diff = ((our_price - price) / price) * 100
                    intel["competitors"][comp]["avg_price_diff"].append(diff)
        
        # Calculate averages
        for comp, data in intel["competitors"].items():
            diffs = data.pop("avg_price_diff", [])
            data["avg_price_diff_pct"] = round(sum(diffs) / len(diffs), 1) if diffs else None
            data["total_encounters"] = data["won_against"] + data["lost_to"]
            data["win_rate_vs"] = round(data["won_against"] / data["total_encounters"] * 100, 1) if data["total_encounters"] > 0 else None
    except:
        pass
    
    return jsonify({"ok": True, **intel})


# ═══════════════════════════════════════════════════════════════════════
# 8. Email Draft Queue Status
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/email/queue-status")
@auth_required
def api_email_queue_status():
    """Status of email drafts: pending, approved, sent."""
    outbox_path = os.path.join(DATA_DIR, "outbox.json")
    try:
        with open(outbox_path) as f:
            outbox = json.load(f)
    except:
        outbox = []
    
    if isinstance(outbox, dict):
        outbox = list(outbox.values())
    
    draft = [e for e in outbox if (e.get("status") or "").lower() in ("draft", "pending")]
    approved = [e for e in outbox if (e.get("status") or "").lower() == "approved"]
    sent = [e for e in outbox if (e.get("status") or "").lower() == "sent"]
    
    return jsonify({
        "ok": True,
        "drafts": len(draft),
        "approved": len(approved),
        "sent": len(sent),
        "total": len(outbox),
        "needs_review": len(draft),
        "ready_to_send": len(approved),
        "recent_drafts": [
            {"to": e.get("to", "?"), "subject": e.get("subject", "?")[:50], 
             "created": e.get("created", "?"), "type": e.get("type", "?")}
            for e in sorted(draft, key=lambda x: x.get("created", ""), reverse=True)[:5]
        ]
    })


# ═══════════════════════════════════════════════════════════════════════
# 9. Vendor Performance — which suppliers are reliable?
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/vendor/performance")
@auth_required
def api_vendor_performance():
    """Score vendors by response time, pricing accuracy, fill rate."""
    cat_path = os.path.join(DATA_DIR, "product_catalog.json")
    vendors = defaultdict(lambda: {"quotes": 0, "products": 0, "avg_markup": [], "urls": set()})
    
    try:
        with open(cat_path) as f:
            cat = json.load(f)
        
        for pid, p in cat.get("products", {}).items():
            for url in p.get("supplier_urls", []):
                domain = url.split("/")[2] if "/" in url and len(url.split("/")) > 2 else url
                domain = domain.replace("www.", "")
                vendors[domain]["products"] += 1
                vendors[domain]["urls"].add(url)
            
            if p.get("supplier_cost") and p.get("last_quoted_price"):
                cost = p["supplier_cost"]
                price = p["last_quoted_price"]
                if cost > 0:
                    markup = ((price - cost) / cost) * 100
                    for url in p.get("supplier_urls", []):
                        domain = url.split("/")[2] if "/" in url and len(url.split("/")) > 2 else url
                        domain = domain.replace("www.", "")
                        vendors[domain]["avg_markup"].append(markup)
    except:
        pass
    
    result = []
    for name, data in vendors.items():
        markups = data.pop("avg_markup", [])
        data["urls"] = list(data["urls"])[:3]
        data["avg_markup_pct"] = round(sum(markups) / len(markups), 1) if markups else None
        data["name"] = name
        result.append(data)
    
    result.sort(key=lambda x: x["products"], reverse=True)
    
    return jsonify({"ok": True, "vendors": result[:20], "total": len(result)})


# ═══════════════════════════════════════════════════════════════════════
# 10. Smart Notifications — what needs attention RIGHT NOW
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/notifications/smart")
@auth_required
def api_smart_notifications():
    """AI-generated notifications based on current state."""
    notifs = []
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    
    # Check overdue RFQs
    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    try:
        with open(rfqs_path) as f:
            rfqs = json.load(f)
        for rid, r in rfqs.items():
            due = r.get("due_date") or r.get("deadline") or ""
            status = (r.get("status") or "").lower()
            if due and due[:10] < today and status in ("new", "draft", "priced"):
                notifs.append({
                    "severity": "high",
                    "type": "overdue_rfq",
                    "message": f"RFQ {r.get('solicitation_number', rid)[:20]} is OVERDUE (due {due[:10]})",
                    "action_url": f"/rfq/{rid}",
                    "action_label": "Open RFQ"
                })
            elif due and due[:10] == today and status in ("new", "draft", "priced"):
                notifs.append({
                    "severity": "high",
                    "type": "due_today",
                    "message": f"RFQ {r.get('solicitation_number', rid)[:20]} is DUE TODAY",
                    "action_url": f"/rfq/{rid}",
                    "action_label": "Open RFQ"
                })
    except: pass
    
    # Check email drafts needing review
    outbox_path = os.path.join(DATA_DIR, "outbox.json")
    try:
        with open(outbox_path) as f:
            outbox = json.load(f)
        if isinstance(outbox, dict): outbox = list(outbox.values())
        drafts = [e for e in outbox if (e.get("status") or "").lower() in ("draft", "pending")]
        if len(drafts) > 0:
            notifs.append({
                "severity": "medium",
                "type": "drafts_pending",
                "message": f"{len(drafts)} email drafts need review",
                "action_url": "/outbox",
                "action_label": "Review Drafts"
            })
    except: pass
    
    # Check follow-ups due
    fu_path = os.path.join(DATA_DIR, "follow_up_state.json")
    try:
        with open(fu_path) as f:
            fu = json.load(f)
        overdue_fu = len([f for f in fu.values() if isinstance(f, dict) and 
                         f.get("next_follow_up", "") and f["next_follow_up"][:10] <= today])
        if overdue_fu > 0:
            notifs.append({
                "severity": "medium",
                "type": "follow_ups_due",
                "message": f"{overdue_fu} follow-ups are due today or overdue",
                "action_url": "/follow-up",
                "action_label": "View Follow-Ups"
            })
    except: pass
    
    # Sort by severity
    sev_order = {"high": 0, "medium": 1, "low": 2}
    notifs.sort(key=lambda n: sev_order.get(n.get("severity"), 9))
    
    return jsonify({
        "ok": True,
        "notifications": notifs[:15],
        "count": len(notifs),
        "high": len([n for n in notifs if n["severity"] == "high"]),
        "medium": len([n for n in notifs if n["severity"] == "medium"]),
    })


# ═══════════════════════════════════════════════════════════════════════
# 11. Agency Leaderboard — which agencies bring most revenue
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/agency/leaderboard")
@auth_required
def api_agency_leaderboard():
    """Rank agencies by revenue, order count, and growth."""
    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    orders_path = os.path.join(DATA_DIR, "orders.json")
    agencies = defaultdict(lambda: {"quotes": 0, "orders": 0, "revenue": 0, "rfqs": 0})
    
    try:
        with open(rfqs_path) as f:
            rfqs = json.load(f)
        for r in rfqs.values():
            agency = r.get("institution") or r.get("agency") or "Unknown"
            agencies[agency]["rfqs"] += 1
            if (r.get("status") or "").lower() in ("sent", "quoted"):
                agencies[agency]["quotes"] += 1
    except: pass
    
    try:
        with open(orders_path) as f:
            orders = json.load(f)
        for o in orders.values():
            agency = o.get("institution") or o.get("agency") or "Unknown"
            agencies[agency]["orders"] += 1
            agencies[agency]["revenue"] += o.get("total", 0)
    except: pass
    
    result = [{"agency": k, **v} for k, v in agencies.items()]
    result.sort(key=lambda x: x["revenue"], reverse=True)
    
    return jsonify({"ok": True, "agencies": result[:20], "total": len(result)})


# ═══════════════════════════════════════════════════════════════════════
# 12. Product Quick Search from Agents page
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/product/search")
@auth_required
def api_product_search():
    """Quick product search in catalog."""
    from flask import request as req
    q = (req.args.get("q") or "").strip().lower()
    if not q:
        return jsonify({"ok": False, "error": "Provide ?q=<search_term>"})
    
    cat_path = os.path.join(DATA_DIR, "product_catalog.json")
    results = []
    
    try:
        with open(cat_path) as f:
            cat = json.load(f)
        
        for pid, p in cat.get("products", {}).items():
            name = (p.get("name") or "").lower()
            desc = (p.get("description") or "").lower()
            sku = (p.get("sku") or p.get("item_number") or "").lower()
            
            if q in name or q in desc or q in sku:
                results.append({
                    "id": pid,
                    "name": p.get("name", "?"),
                    "sku": p.get("sku") or p.get("item_number") or "",
                    "last_price": p.get("last_quoted_price", 0),
                    "cost": p.get("supplier_cost", 0),
                    "margin_pct": round(((p.get("last_quoted_price", 0) - p.get("supplier_cost", 0)) / p.get("last_quoted_price", 1)) * 100, 1) if p.get("last_quoted_price", 0) > 0 else None,
                    "times_quoted": p.get("times_quoted", 0),
                    "category": p.get("category", ""),
                })
    except: pass
    
    results.sort(key=lambda x: x.get("times_quoted", 0), reverse=True)
    
    return jsonify({"ok": True, "query": q, "results": results[:20], "count": len(results)})


# ═══════════════════════════════════════════════════════════════════════
# 13. Quote Pipeline Velocity — how fast quotes move through stages
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/pipeline/velocity")
@auth_required
def api_pipeline_velocity():
    """Measure how quickly quotes move from inbox to sent to won."""
    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    
    try:
        with open(rfqs_path) as f:
            rfqs = json.load(f)
    except:
        return jsonify({"ok": True, "message": "No RFQ data", "avg_days_to_quote": None})
    
    quote_times = []
    decision_times = []
    
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
            except: pass
    
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


# ═══════════════════════════════════════════════════════════════════════
# 14. Auto-Pricing Suggestion — AI-powered price recommendation
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/catalog/pricing-suggestion")
@auth_required
def api_pricing_suggestion():
    """Get AI pricing suggestions for catalog items."""
    from flask import request as req
    product_name = req.args.get("product", "").strip()
    
    cat_path = os.path.join(DATA_DIR, "product_catalog.json")
    suggestions = []
    
    try:
        with open(cat_path) as f:
            cat = json.load(f)
        
        for pid, p in cat.get("products", {}).items():
            if product_name and product_name.lower() not in (p.get("name") or "").lower():
                continue
            
            cost = p.get("supplier_cost", 0)
            last_price = p.get("last_quoted_price", 0)
            prices = p.get("price_history", [])
            
            if cost > 0 and last_price > 0:
                current_margin = ((last_price - cost) / last_price) * 100
                
                # Suggest based on margin targets
                suggested_low = round(cost * 1.15, 2)   # 15% margin
                suggested_mid = round(cost * 1.25, 2)    # 25% margin  
                suggested_high = round(cost * 1.35, 2)   # 35% margin
                
                suggestions.append({
                    "product": p.get("name", "?")[:50],
                    "current_cost": cost,
                    "current_price": last_price,
                    "current_margin": round(current_margin, 1),
                    "suggested_competitive": suggested_low,
                    "suggested_balanced": suggested_mid,
                    "suggested_premium": suggested_high,
                    "times_quoted": p.get("times_quoted", 0),
                    "flag": "⚠️ Low margin" if current_margin < 10 else "✅ Healthy" if current_margin < 40 else "💰 High margin"
                })
        
        suggestions.sort(key=lambda x: x.get("current_margin", 50))
    except: pass
    
    return jsonify({
        "ok": True,
        "suggestions": suggestions[:20],
        "count": len(suggestions),
    })


# ═══════════════════════════════════════════════════════════════════════
# 15. Export Any Result as CSV
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/export/csv", methods=["POST"])
@auth_required
def api_export_csv():
    """Export JSON data as downloadable CSV."""
    from flask import request as req
    import csv, io
    
    data = req.json or {}
    rows = data.get("rows") or data.get("data") or data.get("results") or []
    filename = data.get("filename", "export.csv")
    
    if not rows or not isinstance(rows, list):
        return jsonify({"ok": False, "error": "No data to export. Provide {rows: [...]} or {results: [...]}"})
    
    # Flatten if needed
    if isinstance(rows[0], dict):
        headers = list(rows[0].keys())
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: str(v) for k, v in row.items()})
    else:
        output = io.StringIO()
        writer = csv.writer(output)
        for row in rows:
            writer.writerow(row if isinstance(row, list) else [row])
    
    from flask import Response
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
