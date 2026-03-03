"""
routes_features.py — 24 New Feature Endpoints for Agents Page
Batch 2: QB Actions, Quote-to-Cash Pipeline, Catalog Intelligence,
         System Dashboard, Data Quality
"""
# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from src.core.paths import DATA_DIR
from src.core.db import get_db

import os, json, glob, time, logging, sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

log = logging.getLogger("features")

# ── QB Action Endpoints ─────────────────────────────────────────────────────

@bp.route("/api/qb/sync-customers", methods=["POST"])
@auth_required
def api_qb_sync_customers():
    """Import QB customers into CRM contacts."""
    try:
        from src.agents.quickbooks_agent import fetch_customers, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        customers = fetch_customers(force_refresh=True)
        if not customers:
            return jsonify({"ok": True, "message": "No customers found in QB", "synced": 0})

        # Load CRM contacts
        crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
        try:
            with open(crm_path) as f:
                crm = json.load(f)
        except Exception:
            crm = {"contacts": []}

        existing_emails = {c.get("email", "").lower() for c in crm.get("contacts", []) if c.get("email")}
        synced = 0
        for cust in customers:
            email = (cust.get("PrimaryEmailAddr", {}) or {}).get("Address", "")
            name = cust.get("DisplayName", "") or cust.get("CompanyName", "")
            if not name:
                continue
            if email and email.lower() in existing_emails:
                continue
            contact = {
                "display_name": name,
                "qb_name": name,
                "email": email,
                "phone": (cust.get("PrimaryPhone", {}) or {}).get("FreeFormNumber", ""),
                "source": "quickbooks_sync",
                "qb_id": cust.get("Id", ""),
                "balance": float(cust.get("Balance", 0)),
                "synced_at": datetime.now().isoformat(),
            }
            crm.setdefault("contacts", []).append(contact)
            if email:
                existing_emails.add(email.lower())
            synced += 1

        with open(crm_path, "w") as f:
            json.dump(crm, f, indent=2)

        return jsonify({"ok": True, "synced": synced, "total_qb_customers": len(customers),
                        "total_crm_contacts": len(crm.get("contacts", []))})
    except Exception as e:
        log.exception("QB sync customers failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/collection-alerts")
@auth_required
def api_qb_collection_alerts():
    """Show overdue invoices with aging brackets and collection priority."""
    try:
        from src.agents.quickbooks_agent import fetch_invoices, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        invoices = fetch_invoices(status="overdue")
        alerts = []
        now = datetime.now()
        for inv in invoices:
            due_str = inv.get("DueDate", "")
            try:
                due = datetime.strptime(due_str, "%Y-%m-%d")
                days_late = (now - due).days
            except Exception:
                days_late = 0
            amount = float(inv.get("Balance", inv.get("TotalAmt", 0)))
            cust = inv.get("CustomerRef", {}).get("name", "Unknown")
            bracket = "1-30 days" if days_late <= 30 else "31-60 days" if days_late <= 60 else "61-90 days" if days_late <= 90 else "90+ days"
            priority = "🔴 CRITICAL" if days_late > 60 or amount > 5000 else "🟡 HIGH" if days_late > 30 else "🟢 NORMAL"
            alerts.append({
                "invoice": inv.get("DocNumber", "?"),
                "customer": cust,
                "amount": amount,
                "due_date": due_str,
                "days_late": days_late,
                "bracket": bracket,
                "priority": priority,
            })
        alerts.sort(key=lambda x: (-x["days_late"], -x["amount"]))
        total_overdue = sum(a["amount"] for a in alerts)
        return jsonify({"ok": True, "alerts": alerts, "count": len(alerts),
                        "total_overdue": round(total_overdue, 2),
                        "brackets": {b: sum(1 for a in alerts if a["bracket"] == b)
                                     for b in ["1-30 days", "31-60 days", "61-90 days", "90+ days"]}})
    except Exception as e:
        log.exception("Collection alerts failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/cash-flow")
@auth_required
def api_qb_cash_flow():
    """30-day cash flow projection from open invoices + pipeline."""
    try:
        from src.agents.quickbooks_agent import fetch_invoices, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        invoices = fetch_invoices(status="open")
        now = datetime.now()

        # Expected inflows from invoices
        inflows = []
        for inv in invoices:
            due_str = inv.get("DueDate", "")
            amount = float(inv.get("Balance", inv.get("TotalAmt", 0)))
            try:
                due = datetime.strptime(due_str, "%Y-%m-%d")
                days_until = (due - now).days
            except Exception:
                days_until = 30
            if days_until <= 30:
                inflows.append({"source": f"Invoice #{inv.get('DocNumber', '?')}", "amount": amount,
                                "due": due_str, "days_until": days_until,
                                "customer": inv.get("CustomerRef", {}).get("name", "?")})

        # Pipeline value
        # rfq.db migrated to reytech.db via get_db()
        pipeline_value = 0
        if True:  # migrated to reytech.db
            try:
                from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
                cur = conn.execute("SELECT SUM(total) FROM quotes WHERE status IN ('sent','quoted') AND total > 0")
                row = cur.fetchone()
                pipeline_value = float(row[0] or 0)
                conn.close()
            except Exception:
                pass

        total_expected = sum(i["amount"] for i in inflows)
        return jsonify({
            "ok": True,
            "30_day_forecast": {
                "expected_collections": round(total_expected, 2),
                "pipeline_pending": round(pipeline_value, 2),
                "total_potential": round(total_expected + pipeline_value * 0.3, 2),
            },
            "inflows": sorted(inflows, key=lambda x: x.get("days_until", 99)),
            "count": len(inflows),
        })
    except Exception as e:
        log.exception("Cash flow forecast failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/vendor-spend")
@auth_required
def api_qb_vendor_spend():
    """Top vendors by spending."""
    try:
        from src.agents.quickbooks_agent import get_recent_purchase_orders, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        pos = get_recent_purchase_orders(days_back=365)
        spend = defaultdict(lambda: {"total": 0, "count": 0, "last_po": ""})
        for po in pos:
            vendor = po.get("VendorRef", {}).get("name", "Unknown")
            amount = float(po.get("TotalAmt", 0))
            spend[vendor]["total"] += amount
            spend[vendor]["count"] += 1
            spend[vendor]["last_po"] = po.get("DocNumber", "")
        result = [{"vendor": k, "total_spend": round(v["total"], 2), "po_count": v["count"],
                    "last_po": v["last_po"]} for k, v in spend.items()]
        result.sort(key=lambda x: -x["total_spend"])
        return jsonify({"ok": True, "vendors": result[:20], "total_vendors": len(result),
                        "total_spend": round(sum(v["total_spend"] for v in result), 2)})
    except Exception as e:
        log.exception("Vendor spend failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/invoice-from-quote", methods=["POST"])
@auth_required
def api_qb_invoice_from_quote():
    """Create QB invoice from a won quote."""
    try:
        from src.agents.quickbooks_agent import create_invoice, find_customer, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        data = request.get_json(silent=True) or {}
        qnum = data.get("quote_number", "")
        if not qnum:
            return jsonify({"ok": False, "error": "quote_number required"})

        # rfq.db migrated to reytech.db via get_db()
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        quote = conn.execute("SELECT * FROM quotes WHERE quote_number=?", (qnum,)).fetchone()
        if not quote:
            conn.close()
            return jsonify({"ok": False, "error": f"Quote {qnum} not found"})

        institution = quote["institution"] or ""
        customer = find_customer(institution)
        if not customer:
            conn.close()
            return jsonify({"ok": False, "error": f"No QB customer match for '{institution}'. Create customer in QB first."})

        items_rows = conn.execute("SELECT * FROM quote_items WHERE quote_number=?", (qnum,)).fetchall()
        items = []
        for it in items_rows:
            items.append({
                "description": it["description"] or "",
                "quantity": int(it["quantity"] or 1),
                "unit_price": float(it["unit_price"] or 0),
            })
        conn.close()

        if not items:
            return jsonify({"ok": False, "error": f"No line items in quote {qnum}"})

        result = create_invoice(
            customer_id=customer["Id"],
            items=items,
            po_number=qnum,
            memo=f"Created from Reytech quote {qnum}",
        )
        if result:
            return jsonify({"ok": True, "invoice": result, "quote": qnum, "customer": institution})
        return jsonify({"ok": False, "error": "Failed to create invoice in QB"})
    except Exception as e:
        log.exception("Invoice from quote failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/revenue-by-month")
@auth_required
def api_qb_revenue_by_month():
    """Monthly revenue breakdown from QB payments."""
    try:
        from src.agents.quickbooks_agent import get_recent_payments, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        payments = get_recent_payments(days_back=365)
        monthly = defaultdict(float)
        for p in payments:
            date_str = p.get("TxnDate", "")
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                key = dt.strftime("%Y-%m")
            except Exception:
                continue
            monthly[key] += float(p.get("TotalAmt", 0))
        result = [{"month": k, "revenue": round(v, 2)} for k, v in sorted(monthly.items())]
        return jsonify({"ok": True, "months": result, "ytd_total": round(sum(v["revenue"] for v in result if v["month"].startswith(str(datetime.now().year))), 2)})
    except Exception as e:
        log.exception("Revenue by month failed")
        return jsonify({"ok": False, "error": str(e)})


# ── QB Draft Reminders, Profit Margins, Expense Summary ─────────────────────

@bp.route("/api/qb/draft-reminders", methods=["POST"])
@auth_required
def api_qb_draft_reminders():
    """Draft payment reminder emails for overdue invoices."""
    try:
        from src.agents.quickbooks_agent import fetch_invoices, fetch_customers, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        invoices = fetch_invoices(status="overdue")
        if not invoices:
            return jsonify({"ok": True, "message": "No overdue invoices found", "drafts": []})
        customers = {c.get("Id"): c for c in fetch_customers()}
        drafts = []
        for inv in invoices[:10]:
            cust_ref = inv.get("CustomerRef", {})
            cust_id = cust_ref.get("value", "")
            cust_name = cust_ref.get("name", "Customer")
            cust = customers.get(cust_id, {})
            email = cust.get("PrimaryEmailAddr", {}).get("Address", "") if isinstance(cust.get("PrimaryEmailAddr"), dict) else ""
            balance = float(inv.get("Balance", 0))
            inv_num = inv.get("DocNumber", "?")
            due_date = inv.get("DueDate", "?")
            days_overdue = 0
            try:
                due_dt = datetime.strptime(due_date, "%Y-%m-%d")
                days_overdue = (datetime.now() - due_dt).days
            except Exception:
                pass
            drafts.append({
                "to": email or f"(no email for {cust_name})",
                "customer": cust_name, "invoice_number": inv_num,
                "amount": balance, "due_date": due_date, "days_overdue": days_overdue,
                "subject": f"Payment Reminder — Invoice #{inv_num} (${balance:,.2f})",
                "body": (f"Dear {cust_name},\n\nThis is a friendly reminder that Invoice #{inv_num} "
                         f"for ${balance:,.2f} was due on {due_date} ({days_overdue} days ago).\n\n"
                         f"Please arrange payment at your earliest convenience.\n\nThank you,\nReytech Inc."),
            })
        return jsonify({"ok": True, "drafts": drafts, "count": len(drafts),
                        "total_overdue": sum(d["amount"] for d in drafts)})
    except Exception as e:
        log.exception("Draft reminders failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/profit-margins")
@auth_required
def api_qb_profit_margins():
    """Calculate profit margins from QB invoice and purchase data."""
    try:
        from src.agents.quickbooks_agent import fetch_invoices, get_recent_purchase_orders, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        invoices = fetch_invoices(status="all", days_back=180)
        pos = get_recent_purchase_orders(days_back=180)
        cust_revenue = defaultdict(float)
        total_revenue = 0
        for inv in invoices:
            cust = inv.get("CustomerRef", {}).get("name", "Unknown")
            amt = float(inv.get("TotalAmt", 0))
            cust_revenue[cust] += amt
            total_revenue += amt
        total_cost = sum(float(po.get("TotalAmt", 0)) for po in pos)
        gross_margin = total_revenue - total_cost
        margin_pct = (gross_margin / total_revenue * 100) if total_revenue > 0 else 0
        top_customers = sorted(cust_revenue.items(), key=lambda x: -x[1])[:10]
        return jsonify({
            "ok": True, "total_revenue_180d": round(total_revenue, 2),
            "total_cost_180d": round(total_cost, 2),
            "gross_margin": round(gross_margin, 2),
            "margin_percent": round(margin_pct, 1),
            "top_customers": [{"customer": c, "revenue": round(r, 2)} for c, r in top_customers],
            "invoice_count": len(invoices), "po_count": len(pos),
        })
    except Exception as e:
        log.exception("Profit margins failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/expense-summary")
@auth_required
def api_qb_expense_summary():
    """Expense breakdown from QB purchase orders and bills."""
    try:
        from src.agents.quickbooks_agent import get_recent_purchase_orders, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        pos = get_recent_purchase_orders(days_back=90)
        vendor_spend = defaultdict(float)
        total = 0
        for po in pos:
            vendor = po.get("VendorRef", {}).get("name", "Unknown")
            amt = float(po.get("TotalAmt", 0))
            vendor_spend[vendor] += amt
            total += amt
        top_vendors = sorted(vendor_spend.items(), key=lambda x: -x[1])[:15]
        # Try QB bills query
        bills, bill_total = [], 0
        try:
            from src.agents.quickbooks_agent import _qb_query
            bills = _qb_query("SELECT * FROM Bill WHERE TxnDate >= '{}' MAXRESULTS 100".format(
                (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")))
            bill_total = sum(float(b.get("TotalAmt", 0)) for b in bills)
        except Exception:
            pass
        return jsonify({
            "ok": True, "po_total_90d": round(total, 2),
            "bill_total_90d": round(bill_total, 2),
            "combined_expenses": round(total + bill_total, 2),
            "top_vendors": [{"vendor": v, "amount": round(a, 2)} for v, a in top_vendors],
            "po_count": len(pos), "bill_count": len(bills),
        })
    except Exception as e:
        log.exception("Expense summary failed")
        return jsonify({"ok": False, "error": str(e)})


# ── Pipeline Endpoints ──────────────────────────────────────────────────────

@bp.route("/api/pipeline/quote-to-cash")
@auth_required
def api_pipeline_quote_to_cash():
    """Full quote-to-cash pipeline with every quote's current stage."""
    # rfq.db migrated to reytech.db via get_db()
    if not os.path.exists(db_path):
        return jsonify({"ok": False, "error": "No database"})
    try:
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        quotes = conn.execute("""
            SELECT quote_number, institution, total, status, created_date, sent_date
            FROM quotes ORDER BY created_date DESC LIMIT 100
        """).fetchall()
        pipeline = []
        for q in quotes:
            stage = q["status"] or "draft"
            pipeline.append({
                "quote": q["quote_number"],
                "institution": q["institution"],
                "total": float(q["total"] or 0),
                "stage": stage,
                "created": q["created_date"],
                "sent": q["sent_date"],
            })
        conn.close()
        stages = defaultdict(lambda: {"count": 0, "value": 0})
        for p in pipeline:
            stages[p["stage"]]["count"] += 1
            stages[p["stage"]]["value"] += p["total"]
        return jsonify({"ok": True, "pipeline": pipeline[:50],
                        "stages": {k: {"count": v["count"], "value": round(v["value"], 2)} for k, v in stages.items()},
                        "total_quotes": len(pipeline)})
    except Exception as e:
        log.exception("Quote-to-cash failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pipeline/stale-quotes")
@auth_required
def api_pipeline_stale_quotes():
    """Quotes with no activity in 7+ days."""
    # rfq.db migrated to reytech.db via get_db()
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "stale": [], "count": 0})
    try:
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        quotes = conn.execute("""
            SELECT quote_number, institution, total, status, created_date, sent_date
            FROM quotes WHERE status IN ('sent','quoted','draft')
            AND created_date < ? ORDER BY created_date ASC
        """, (cutoff,)).fetchall()
        stale = []
        for q in quotes:
            created = q["created_date"] or ""
            try:
                days_old = (datetime.now() - datetime.strptime(created[:10], "%Y-%m-%d")).days
            except Exception:
                days_old = 0
            stale.append({
                "quote": q["quote_number"], "institution": q["institution"],
                "total": float(q["total"] or 0), "status": q["status"],
                "created": created, "days_old": days_old,
                "action": "🔴 Call now" if days_old > 14 else "🟡 Send follow-up",
            })
        conn.close()
        return jsonify({"ok": True, "stale": stale, "count": len(stale),
                        "total_value_at_risk": round(sum(s["total"] for s in stale), 2)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pipeline/follow-up-queue")
@auth_required
def api_pipeline_follow_up_queue():
    """Prioritized follow-up queue based on value and age."""
    # rfq.db migrated to reytech.db via get_db()
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "queue": []})
    try:
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        quotes = conn.execute("""
            SELECT quote_number, institution, total, status, created_date, sent_date
            FROM quotes WHERE status IN ('sent','quoted','priced')
            ORDER BY total DESC
        """).fetchall()
        queue = []
        for q in quotes:
            sent = q["sent_date"] or q["created_date"] or ""
            try:
                days = (datetime.now() - datetime.strptime(sent[:10], "%Y-%m-%d")).days
            except Exception:
                days = 0
            priority = "🔴 HIGH" if days > 7 and float(q["total"] or 0) > 1000 else "🟡 MEDIUM" if days > 3 else "🟢 LOW"
            queue.append({
                "quote": q["quote_number"], "institution": q["institution"],
                "total": float(q["total"] or 0), "status": q["status"],
                "days_since_sent": days, "priority": priority,
                "suggested_action": "Phone call" if days > 7 else "Email follow-up" if days > 3 else "Wait",
            })
        conn.close()
        queue.sort(key=lambda x: (0 if "HIGH" in x["priority"] else 1 if "MEDIUM" in x["priority"] else 2, -x["total"]))
        return jsonify({"ok": True, "queue": queue[:30], "total": len(queue)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pipeline/revenue-goal")
@auth_required
def api_pipeline_revenue_goal():
    """$2M annual revenue goal tracker with monthly projections."""
    goal = 2_000_000
    # rfq.db migrated to reytech.db via get_db()
    now = datetime.now()
    months_passed = now.month + (now.day / 30.0)
    months_left = 12 - months_passed

    won_total = 0
    pipeline_value = 0
    monthly_data = defaultdict(float)

    if True:  # migrated to reytech.db
        try:
            from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
            cur = conn.execute("SELECT SUM(total) FROM quotes WHERE status='won' AND total > 0")
            row = cur.fetchone()
            won_total = float(row[0] or 0)

            cur = conn.execute("SELECT SUM(total) FROM quotes WHERE status IN ('sent','quoted') AND total > 0")
            row = cur.fetchone()
            pipeline_value = float(row[0] or 0)

            for row in conn.execute("SELECT created_date, total, status FROM quotes WHERE status='won' AND total > 0"):
                try:
                    dt = datetime.strptime(row[0][:10], "%Y-%m-%d")
                    monthly_data[dt.strftime("%Y-%m")] += float(row[1])
                except Exception:
                    pass
            conn.close()
        except Exception:
            pass

    pct = (won_total / goal * 100) if goal else 0
    monthly_needed = (goal - won_total) / max(months_left, 0.1)
    avg_monthly = won_total / max(months_passed, 0.1)
    projected_annual = avg_monthly * 12

    return jsonify({
        "ok": True,
        "goal": goal,
        "won_total": round(won_total, 2),
        "pipeline_value": round(pipeline_value, 2),
        "pct_achieved": round(pct, 1),
        "monthly_needed": round(monthly_needed, 2),
        "avg_monthly_revenue": round(avg_monthly, 2),
        "projected_annual": round(projected_annual, 2),
        "on_track": projected_annual >= goal,
        "months_left": round(months_left, 1),
        "monthly_breakdown": [{"month": k, "revenue": round(v, 2)} for k, v in sorted(monthly_data.items())],
    })


@bp.route("/api/pipeline/conversion-funnel")
@auth_required
def api_pipeline_conversion_funnel():
    """Stage-by-stage conversion funnel."""
    # rfq.db migrated to reytech.db via get_db()
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "funnel": {}})
    try:
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        stages = {}
        for row in conn.execute("SELECT status, COUNT(*), SUM(total) FROM quotes GROUP BY status"):
            stages[row[0] or "unknown"] = {"count": row[1], "value": round(float(row[2] or 0), 2)}

        # PC pipeline
        pc_count = conn.execute("SELECT COUNT(*) FROM price_checks").fetchone()[0]
        rfq_count = conn.execute("SELECT COUNT(*) FROM rfqs").fetchone()[0]
        conn.close()

        funnel_order = ["draft", "priced", "quoted", "sent", "won", "lost"]
        ordered = []
        for s in funnel_order:
            if s in stages:
                ordered.append({"stage": s, **stages[s]})

        return jsonify({
            "ok": True,
            "price_checks": pc_count, "rfqs": rfq_count,
            "funnel": ordered, "all_stages": stages,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pipeline/avg-deal-size")
@auth_required
def api_pipeline_avg_deal_size():
    """Average deal size by agency and overall."""
    # rfq.db migrated to reytech.db via get_db()
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "overall": 0, "by_agency": {}})
    try:
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        overall = conn.execute("SELECT AVG(total), COUNT(*) FROM quotes WHERE total > 0").fetchone()
        by_agency = {}
        for row in conn.execute("""
            SELECT institution, AVG(total), COUNT(*), SUM(total)
            FROM quotes WHERE total > 0 GROUP BY institution ORDER BY SUM(total) DESC LIMIT 20
        """):
            by_agency[row[0] or "Unknown"] = {
                "avg_deal": round(float(row[1] or 0), 2),
                "quote_count": row[2],
                "total_value": round(float(row[3] or 0), 2),
            }
        conn.close()
        return jsonify({
            "ok": True,
            "overall_avg": round(float(overall[0] or 0), 2),
            "total_quotes": overall[1],
            "by_agency": by_agency,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Catalog Intelligence Endpoints ──────────────────────────────────────────

@bp.route("/api/catalog/margin-analysis")
@auth_required
def api_catalog_margin_analysis():
    """Analyze catalog products by margin tier."""
    db_path = os.path.join(DATA_DIR, "catalog.db")
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "tiers": {}, "total": 0})
    try:
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        products = conn.execute("""
            SELECT name, sell_price, cost_price, margin_pct, times_quoted, category
            FROM products WHERE sell_price > 0 AND cost_price > 0
            ORDER BY margin_pct ASC
        """).fetchall()
        conn.close()

        tiers = {"🔴 Negative (<0%)": [], "🟡 Low (0-10%)": [], "🟢 Mid (10-25%)": [], "🔵 High (>25%)": []}
        for p in products:
            margin = float(p["margin_pct"] or 0)
            item = {"name": p["name"][:60], "sell": float(p["sell_price"]), "cost": float(p["cost_price"]),
                    "margin": round(margin, 1), "quoted": p["times_quoted"] or 0, "category": p["category"] or ""}
            if margin < 0:
                tiers["🔴 Negative (<0%)"].append(item)
            elif margin < 10:
                tiers["🟡 Low (0-10%)"].append(item)
            elif margin < 25:
                tiers["🟢 Mid (10-25%)"].append(item)
            else:
                tiers["🔵 High (>25%)"].append(item)

        summary = {k: {"count": len(v), "avg_margin": round(sum(i["margin"] for i in v) / max(len(v), 1), 1)}
                   for k, v in tiers.items()}
        return jsonify({"ok": True, "summary": summary, "total": len(products),
                        "worst_margins": tiers["🔴 Negative (<0%)"][:10],
                        "best_margins": tiers["🔵 High (>25%)"][:10]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/top-quoted")
@auth_required
def api_catalog_top_quoted():
    """Top 20 most-quoted catalog items."""
    db_path = os.path.join(DATA_DIR, "catalog.db")
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "items": []})
    try:
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        items = conn.execute("""
            SELECT name, sell_price, cost_price, margin_pct, times_quoted, category, last_quoted
            FROM products WHERE times_quoted > 0 ORDER BY times_quoted DESC LIMIT 20
        """).fetchall()
        conn.close()
        result = [{"name": i["name"][:60], "sell": float(i["sell_price"] or 0),
                    "cost": float(i["cost_price"] or 0), "margin": round(float(i["margin_pct"] or 0), 1),
                    "times_quoted": i["times_quoted"], "category": i["category"] or "",
                    "last_quoted": i["last_quoted"] or ""} for i in items]
        return jsonify({"ok": True, "items": result, "count": len(result)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/quick-quote")
@auth_required
def api_catalog_quick_quote():
    """Search catalog for quick quote pricing."""
    q = request.args.get("q", "")
    if not q:
        return jsonify({"ok": False, "error": "?q= search term required"})
    db_path = os.path.join(DATA_DIR, "catalog.db")
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "matches": []})
    try:
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        items = conn.execute("""
            SELECT name, sell_price, cost_price, margin_pct, category, item_number,
                   times_quoted, last_quoted
            FROM products WHERE name LIKE ? OR item_number LIKE ? OR category LIKE ?
            ORDER BY times_quoted DESC LIMIT 10
        """, (f"%{q}%", f"%{q}%", f"%{q}%")).fetchall()
        conn.close()
        matches = [{"name": i["name"], "price": float(i["sell_price"] or 0),
                     "cost": float(i["cost_price"] or 0), "margin": round(float(i["margin_pct"] or 0), 1),
                     "item_number": i["item_number"] or "", "category": i["category"] or "",
                     "times_quoted": i["times_quoted"] or 0} for i in items]
        return jsonify({"ok": True, "query": q, "matches": matches, "count": len(matches)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── System Dashboard Endpoints ──────────────────────────────────────────────

@bp.route("/api/system/dashboard")
@auth_required
def api_system_dashboard():
    """System health: disk, memory, uptime, data stats."""
    import platform
    try:
        sys_info = {
            "platform": platform.platform(),
            "python": platform.python_version(),
        }
        disk_info = {}
        mem_info = {}
        if HAS_PSUTIL:
            disk = psutil.disk_usage("/")
            mem = psutil.virtual_memory()
            boot = datetime.fromtimestamp(psutil.boot_time())
            uptime = datetime.now() - boot
            sys_info["uptime"] = str(uptime).split(".")[0]
            disk_info = {"total_gb": round(disk.total / (1024**3), 1), "used_gb": round(disk.used / (1024**3), 1),
                         "free_gb": round(disk.free / (1024**3), 1), "pct_used": disk.percent}
            mem_info = {"total_gb": round(mem.total / (1024**3), 2), "used_gb": round(mem.used / (1024**3), 2),
                        "pct_used": mem.percent}
        else:
            # Fallback using os.statvfs
            try:
                st = os.statvfs("/")
                disk_info = {"total_gb": round(st.f_blocks * st.f_frsize / (1024**3), 1),
                             "free_gb": round(st.f_bavail * st.f_frsize / (1024**3), 1)}
            except Exception:
                disk_info = {"note": "psutil not available"}
            mem_info = {"note": "psutil not available"}

        # Count data files
        data_files = glob.glob(os.path.join(DATA_DIR, "*"))
        db_files = [f for f in data_files if f.endswith(".db")]
        json_files = [f for f in data_files if f.endswith(".json")]

        return jsonify({
            "ok": True,
            "system": sys_info,
            "disk": disk_info,
            "memory": mem_info,
            "data": {
                "total_files": len(data_files),
                "databases": len(db_files),
                "json_files": len(json_files),
                "total_size_mb": round(sum(os.path.getsize(f) for f in data_files if os.path.isfile(f)) / (1024**2), 1),
            },
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/system/error-log")
@auth_required
def api_system_error_log():
    """Recent application errors from logs."""
    errors = []
    # Check gunicorn/app logs
    log_paths = [
        "/tmp/gunicorn_error.log",
        os.path.join(DATA_DIR, "error.log"),
        os.path.join(DATA_DIR, "app.log"),
    ]
    for path in log_paths:
        if os.path.exists(path):
            try:
                with open(path) as f:
                    lines = f.readlines()
                for line in lines[-100:]:
                    lower = line.lower()
                    if "error" in lower or "exception" in lower or "traceback" in lower:
                        errors.append({"file": os.path.basename(path), "line": line.strip()[:200]})
            except Exception:
                pass

    # Check QA history for failures
    qa_path = os.path.join(DATA_DIR, "qa_history.json")
    if os.path.exists(qa_path):
        try:
            with open(qa_path) as f:
                qa = json.load(f)
            recent = qa[-5:] if isinstance(qa, list) else []
            for run in recent:
                if isinstance(run, dict) and run.get("score", 100) < 70:
                    errors.append({"file": "qa_history", "line": f"QA score {run.get('score')}: {run.get('grade', '?')}"})
        except Exception:
            pass

    return jsonify({"ok": True, "errors": errors[-30:], "count": len(errors)})


@bp.route("/api/system/route-map")
@auth_required
def api_system_route_map():
    """List all registered API routes."""
    from flask import current_app
    routes = []
    for rule in current_app.url_map.iter_rules():
        if rule.endpoint == "static":
            continue
        routes.append({
            "path": rule.rule,
            "methods": sorted([m for m in rule.methods if m not in ("HEAD", "OPTIONS")]),
            "endpoint": rule.endpoint,
        })
    routes.sort(key=lambda x: x["path"])
    api_routes = [r for r in routes if r["path"].startswith("/api/")]
    page_routes = [r for r in routes if not r["path"].startswith("/api/")]
    return jsonify({"ok": True, "api_routes": len(api_routes), "page_routes": len(page_routes),
                    "total": len(routes), "routes": routes})


@bp.route("/api/system/data-sizes")
@auth_required
def api_system_data_sizes():
    """Show sizes of all data files."""
    files = []
    for f in sorted(glob.glob(os.path.join(DATA_DIR, "*"))):
        if os.path.isfile(f):
            size = os.path.getsize(f)
            files.append({
                "file": os.path.basename(f),
                "size_kb": round(size / 1024, 1),
                "size_mb": round(size / (1024**2), 2),
                "modified": datetime.fromtimestamp(os.path.getmtime(f)).strftime("%Y-%m-%d %H:%M"),
            })
    files.sort(key=lambda x: -x["size_kb"])
    total = sum(f["size_kb"] for f in files)
    return jsonify({"ok": True, "files": files, "total_mb": round(total / 1024, 1), "count": len(files)})


@bp.route("/api/system/batch-health", methods=["POST"])
@auth_required
def api_system_batch_health():
    """Test all critical endpoints at once."""
    from flask import current_app
    test_endpoints = [
        ("/api/qa/health", "QA Health"),
        ("/api/agents/status", "Agent Fleet"),
        ("/api/catalog/stats", "Catalog"),
        ("/api/crm/activity?limit=1", "CRM Activity"),
        ("/api/pipeline/revenue-goal", "Revenue Goal"),
    ]
    results = []
    for path, name in test_endpoints:
        start = time.time()
        try:
            with current_app.test_client() as client:
                resp = client.get(path, headers={"Authorization": request.headers.get("Authorization", "")})
                elapsed = round((time.time() - start) * 1000)
                ok = resp.status_code == 200
                results.append({"name": name, "path": path, "status": resp.status_code,
                                "ok": ok, "ms": elapsed})
        except Exception as e:
            results.append({"name": name, "path": path, "status": 500, "ok": False,
                            "ms": 0, "error": str(e)})

    healthy = sum(1 for r in results if r["ok"])
    return jsonify({"ok": True, "results": results, "healthy": healthy,
                    "total": len(results), "grade": "A" if healthy == len(results) else "B" if healthy >= 3 else "F"})


@bp.route("/api/system/env-check")
@auth_required
def api_system_env_check():
    """Check which environment variables are configured."""
    checks = {
        "DASH_USER": bool(os.environ.get("DASH_USER")),
        "DASH_PASS": os.environ.get("DASH_PASS", "changeme") != "changeme",
        "GMAIL_ADDRESS": bool(os.environ.get("GMAIL_ADDRESS")),
        "GMAIL_PASSWORD": bool(os.environ.get("GMAIL_PASSWORD")),
        "QB_CLIENT_ID": bool(os.environ.get("QB_CLIENT_ID")),
        "QB_CLIENT_SECRET": bool(os.environ.get("QB_CLIENT_SECRET")),
        "QB_REALM_ID": bool(os.environ.get("QB_REALM_ID")),
        "QB_REFRESH_TOKEN": bool(os.environ.get("QB_REFRESH_TOKEN")),
        "OPENAI_API_KEY": bool(os.environ.get("OPENAI_API_KEY")),
        "ANTHROPIC_API_KEY": bool(os.environ.get("ANTHROPIC_API_KEY")),
        "VAPI_API_KEY": bool(os.environ.get("VAPI_API_KEY")),
        "TWILIO_ACCOUNT_SID": bool(os.environ.get("TWILIO_ACCOUNT_SID")),
        "TWILIO_AUTH_TOKEN": bool(os.environ.get("TWILIO_AUTH_TOKEN")),
        "RAILWAY_ENVIRONMENT": os.environ.get("RAILWAY_ENVIRONMENT", "unknown"),
    }
    configured = sum(1 for k, v in checks.items() if v and k != "RAILWAY_ENVIRONMENT")
    return jsonify({"ok": True, "env_vars": checks, "configured": configured,
                    "total_checked": len(checks) - 1})


# ── Data Quality Endpoints ──────────────────────────────────────────────────

@bp.route("/api/data-quality/duplicates")
@auth_required
def api_data_quality_duplicates():
    """Find duplicate contacts/vendors in CRM."""
    crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
    if not os.path.exists(crm_path):
        return jsonify({"ok": True, "duplicates": [], "count": 0})
    try:
        with open(crm_path) as f:
            crm = json.load(f)
        contacts = crm.get("contacts", [])

        # Group by email
        by_email = defaultdict(list)
        for c in contacts:
            email = (c.get("email") or "").lower().strip()
            if email:
                by_email[email].append(c.get("display_name") or c.get("qb_name") or "Unknown")

        # Group by normalized name
        by_name = defaultdict(list)
        for c in contacts:
            name = (c.get("display_name") or c.get("qb_name") or "").lower().strip()
            if name and len(name) > 3:
                by_name[name].append(c.get("email") or "no email")

        email_dupes = [{"email": k, "names": v, "count": len(v)} for k, v in by_email.items() if len(v) > 1]
        name_dupes = [{"name": k, "emails": v, "count": len(v)} for k, v in by_name.items() if len(v) > 1]

        return jsonify({
            "ok": True,
            "email_duplicates": email_dupes[:20],
            "name_duplicates": name_dupes[:20],
            "total_contacts": len(contacts),
            "duplicate_emails": len(email_dupes),
            "duplicate_names": len(name_dupes),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/data-quality/missing-data")
@auth_required
def api_data_quality_missing_data():
    """Find records with incomplete data."""
    issues = []
    # Check quotes with no total
    # rfq.db migrated to reytech.db via get_db()
    if True:  # migrated to reytech.db
        try:
            from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
            no_total = conn.execute("SELECT COUNT(*) FROM quotes WHERE total IS NULL OR total = 0").fetchone()[0]
            no_inst = conn.execute("SELECT COUNT(*) FROM quotes WHERE institution IS NULL OR institution = ''").fetchone()[0]
            no_items = conn.execute("""
                SELECT COUNT(*) FROM quotes q
                WHERE NOT EXISTS (SELECT 1 FROM quote_items qi WHERE qi.quote_number = q.quote_number)
            """).fetchone()[0]
            conn.close()
            if no_total: issues.append({"type": "quotes", "issue": f"{no_total} quotes with $0 total"})
            if no_inst: issues.append({"type": "quotes", "issue": f"{no_inst} quotes missing institution"})
            if no_items: issues.append({"type": "quotes", "issue": f"{no_items} quotes with no line items"})
        except Exception:
            pass

    # Check CRM contacts
    crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
    if os.path.exists(crm_path):
        try:
            with open(crm_path) as f:
                crm = json.load(f)
            contacts = crm.get("contacts", [])
            no_email = sum(1 for c in contacts if not c.get("email"))
            no_phone = sum(1 for c in contacts if not c.get("phone"))
            if no_email: issues.append({"type": "crm", "issue": f"{no_email} contacts missing email"})
            if no_phone: issues.append({"type": "crm", "issue": f"{no_phone} contacts missing phone"})
        except Exception:
            pass

    return jsonify({"ok": True, "issues": issues, "count": len(issues)})


@bp.route("/api/data-quality/orphaned-quotes")
@auth_required
def api_data_quality_orphaned_quotes():
    """Find quotes not linked to any CRM contact."""
    # rfq.db migrated to reytech.db via get_db()
    crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "orphaned": []})
    try:
        # Get CRM institution names
        crm_institutions = set()
        if os.path.exists(crm_path):
            with open(crm_path) as f:
                crm = json.load(f)
            for c in crm.get("contacts", []):
                name = (c.get("display_name") or c.get("qb_name") or "").lower()
                if name:
                    crm_institutions.add(name)

        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        quotes = conn.execute("SELECT quote_number, institution, total, status FROM quotes").fetchall()
        conn.close()

        orphaned = []
        for q in quotes:
            inst = (q["institution"] or "").lower()
            if inst and inst not in crm_institutions:
                orphaned.append({
                    "quote": q["quote_number"], "institution": q["institution"],
                    "total": float(q["total"] or 0), "status": q["status"],
                })

        return jsonify({"ok": True, "orphaned": orphaned[:30], "count": len(orphaned),
                        "total_quotes": len(quotes), "crm_contacts": len(crm_institutions)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Feature Enhancement Routes (15+ new capabilities) ───────────────────────

# 1. QB Test Connection — quick ping to verify QB is working
@bp.route("/api/qb/test-connection")
@auth_required
def api_qb_test_connection():
    """Quick QB connection test — tries to fetch company info."""
    try:
        from src.agents.quickbooks_agent import is_configured, get_access_token, get_company_info, _load_tokens
        tokens = _load_tokens()
        has_token = bool(tokens.get("access_token"))
        configured = is_configured()
        result = {
            "ok": True,
            "has_token_file": has_token,
            "is_configured": configured,
            "realm_id": tokens.get("realm_id", "")[:6] + "..." if tokens.get("realm_id") else "",
            "connected_at": tokens.get("connected_at", ""),
            "last_refreshed": tokens.get("refreshed_at", ""),
        }
        if configured:
            token = get_access_token()
            result["has_valid_access_token"] = bool(token)
            if token:
                info = get_company_info()
                result["company"] = info.get("name", "") if info else "FAILED"
                result["api_reachable"] = bool(info)
            else:
                result["api_reachable"] = False
                result["hint"] = "Token refresh failed — try reconnecting via Connect QuickBooks"
        else:
            missing = []
            if not os.environ.get("QB_CLIENT_ID"): missing.append("QB_CLIENT_ID")
            if not os.environ.get("QB_CLIENT_SECRET"): missing.append("QB_CLIENT_SECRET")
            if not tokens.get("refresh_token") and not os.environ.get("QB_REFRESH_TOKEN"): missing.append("refresh_token (connect QB first)")
            if not tokens.get("realm_id") and not os.environ.get("QB_REALM_ID"): missing.append("realm_id (connect QB first)")
            result["missing"] = missing
            result["hint"] = "Missing: " + ", ".join(missing)
        return jsonify(result)
    except ImportError:
        return jsonify({"ok": False, "error": "QuickBooks agent module not available"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# 2. QB Force Refresh — force token refresh
@bp.route("/api/qb/force-refresh", methods=["POST"])
@auth_required
def api_qb_force_refresh():
    """Force-refresh the QB access token."""
    try:
        from src.agents.quickbooks_agent import _refresh_access_token, _load_tokens
        token = _refresh_access_token()
        if token:
            tokens = _load_tokens()
            return jsonify({"ok": True, "message": "Token refreshed successfully",
                            "expires_at": tokens.get("expires_at", 0),
                            "realm_id": tokens.get("realm_id", "")[:6] + "..."})
        return jsonify({"ok": False, "error": "Token refresh failed — check credentials or reconnect"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# 3. Dashboard KPIs — key business metrics in one call
@bp.route("/api/dashboard/kpis")
@auth_required
def api_dashboard_kpis():
    """Key performance indicators — single-call business health."""
    try:
        conn = _get_db()
        kpis = {}
        # Quotes
        kpis["total_quotes"] = conn.execute("SELECT COUNT(*) FROM quotes").fetchone()[0]
        kpis["quotes_this_month"] = conn.execute(
            "SELECT COUNT(*) FROM quotes WHERE created_date >= date('now','start of month')").fetchone()[0]
        # Revenue
        won = conn.execute("SELECT SUM(total) FROM quotes WHERE status='won'").fetchone()[0]
        kpis["revenue_won"] = float(won or 0)
        pipeline = conn.execute("SELECT SUM(total) FROM quotes WHERE status IN ('sent','draft','priced','quoted')").fetchone()[0]
        kpis["pipeline_value"] = float(pipeline or 0)
        # Orders
        kpis["total_orders"] = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        # Price checks
        kpis["total_pcs"] = conn.execute("SELECT COUNT(*) FROM price_checks").fetchone()[0]
        kpis["open_pcs"] = conn.execute("SELECT COUNT(*) FROM price_checks WHERE status NOT IN ('priced','completed','cancelled')").fetchone()[0]
        # RFQs
        try:
            kpis["total_rfqs"] = conn.execute("SELECT COUNT(*) FROM rfqs").fetchone()[0]
            kpis["new_rfqs"] = conn.execute("SELECT COUNT(*) FROM rfqs WHERE status='new'").fetchone()[0]
        except Exception:
            kpis["total_rfqs"] = 0
            kpis["new_rfqs"] = 0
        # Contacts
        try:
            kpis["crm_contacts"] = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
        except Exception:
            kpis["crm_contacts"] = 0
        # Win rate
        won_count = conn.execute("SELECT COUNT(*) FROM quotes WHERE status='won'").fetchone()[0]
        lost_count = conn.execute("SELECT COUNT(*) FROM quotes WHERE status='lost'").fetchone()[0]
        total_decided = (won_count or 0) + (lost_count or 0)
        kpis["win_rate"] = round((won_count or 0) / total_decided * 100, 1) if total_decided > 0 else 0
        kpis["$2m_goal_pct"] = round(kpis["revenue_won"] / 2000000 * 100, 2)
        conn.close()
        return jsonify({"ok": True, **kpis})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# 4. Pipeline Daily Summary — today's activity
@bp.route("/api/pipeline/daily-summary")
@auth_required
def api_pipeline_daily_summary():
    """Today's pipeline activity — new quotes, PCs, orders."""
    try:
        conn = _get_db()
        today = datetime.now().strftime("%Y-%m-%d")
        summary = {
            "date": today,
            "new_quotes_today": conn.execute(
                "SELECT COUNT(*) FROM quotes WHERE created_date >= ?", (today,)).fetchone()[0],
            "new_pcs_today": conn.execute(
                "SELECT COUNT(*) FROM price_checks WHERE created_date >= ?", (today,)).fetchone()[0],
        }
        # Recent quotes
        recent = conn.execute(
            "SELECT quote_number, institution, total, status FROM quotes ORDER BY created_date DESC LIMIT 5"
        ).fetchall()
        summary["recent_quotes"] = [dict(r) for r in recent]
        # Overdue items
        overdue_pcs = conn.execute(
            "SELECT pc_number, institution, due_date FROM price_checks WHERE due_date < ? AND status NOT IN ('priced','completed','cancelled')",
            (today,)).fetchall()
        summary["overdue_pcs"] = [dict(r) for r in overdue_pcs]
        conn.close()
        return jsonify({"ok": True, **summary})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# 5. Contact Search — search CRM contacts
@bp.route("/api/crm/search")
@auth_required
def api_crm_contact_search():
    """Search contacts by name, email, or institution. ?q=keyword"""
    try:
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify({"ok": False, "error": "Provide ?q=search_term"})
        conn = _get_db()
        like = f"%{q}%"
        rows = conn.execute(
            "SELECT * FROM contacts WHERE name LIKE ? OR email LIKE ? OR institution LIKE ? LIMIT 20",
            (like, like, like)).fetchall()
        conn.close()
        return jsonify({"ok": True, "contacts": [dict(r) for r in rows], "count": len(rows), "query": q})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# 6. System Metrics — uptime, request patterns
@bp.route("/api/system/metrics")
@auth_required
def api_system_metrics():
    """System performance metrics."""
    import platform
    metrics = {
        "ok": True,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "uptime_seconds": int(time.time() - _plt_start) if '_plt_start' in dir() else None,
    }
    if HAS_PSUTIL:
        try:
            metrics["cpu_percent"] = psutil.cpu_percent(interval=0.1)
            mem = psutil.virtual_memory()
            metrics["memory_used_mb"] = round(mem.used / 1024 / 1024)
            metrics["memory_total_mb"] = round(mem.total / 1024 / 1024)
            metrics["memory_percent"] = mem.percent
            disk = psutil.disk_usage("/")
            metrics["disk_used_gb"] = round(disk.used / 1024 / 1024 / 1024, 1)
            metrics["disk_total_gb"] = round(disk.total / 1024 / 1024 / 1024, 1)
        except Exception:
            pass
    # Count data files
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
    try:
        files = os.listdir(data_dir)
        metrics["data_files"] = len(files)
        total_size = sum(os.path.getsize(os.path.join(data_dir, f)) for f in files if os.path.isfile(os.path.join(data_dir, f)))
        metrics["data_size_mb"] = round(total_size / 1024 / 1024, 2)
    except Exception:
        pass
    return jsonify(metrics)


# 7. Recent Errors — parse gunicorn/app logs for errors
@bp.route("/api/system/recent-errors")
@auth_required
def api_system_recent_errors_trace():
    """Recent application errors with context."""
    errors = []
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
    # Check for error_log.json
    err_file = os.path.join(log_dir, "error_log.json")
    try:
        if os.path.exists(err_file):
            with open(err_file) as f:
                data = json.load(f)
                if isinstance(data, list):
                    errors = data[-20:]
                elif isinstance(data, dict):
                    errors = data.get("errors", [])[-20:]
    except Exception:
        pass
    # Also check QA reports for failures
    qa_file = os.path.join(log_dir, "qa_reports.json")
    qa_errors = []
    try:
        if os.path.exists(qa_file):
            with open(qa_file) as f:
                reports = json.load(f)
                if isinstance(reports, list) and reports:
                    latest = reports[-1]
                    for r in latest.get("results", []):
                        if r.get("status") == "fail":
                            qa_errors.append({"source": "qa", "test": r.get("test"), "message": r.get("message"), "fix": r.get("fix")})
    except Exception:
        pass
    return jsonify({"ok": True, "errors": errors, "qa_failures": qa_errors,
                     "total": len(errors), "qa_total": len(qa_errors)})


# 8. Agent Health Batch — test all major endpoints
@bp.route("/api/agents/health-sweep")
@auth_required
def api_agents_health_sweep():
    """Quick health sweep of all agent subsystems."""
    results = {}
    endpoints = {
        "database": lambda: bool(_get_db().execute("SELECT 1").fetchone()),
        "catalog": lambda: bool(_get_db().execute("SELECT COUNT(*) FROM catalog").fetchone()),
        "quotes": lambda: bool(_get_db().execute("SELECT COUNT(*) FROM quotes").fetchone()),
        "orders": lambda: bool(_get_db().execute("SELECT COUNT(*) FROM orders").fetchone()),
        "price_checks": lambda: bool(_get_db().execute("SELECT COUNT(*) FROM price_checks").fetchone()),
    }
    # QB check
    try:
        from src.agents.quickbooks_agent import is_configured, get_access_token
        results["quickbooks"] = {"configured": is_configured(), "has_token": bool(get_access_token()) if is_configured() else False}
    except Exception:
        results["quickbooks"] = {"configured": False, "error": "module unavailable"}
    # Test each
    for name, check in endpoints.items():
        try:
            start = time.time()
            ok = check()
            dur = round((time.time() - start) * 1000)
            results[name] = {"ok": ok, "ms": dur}
        except Exception as e:
            results[name] = {"ok": False, "error": str(e)}
    # Summary
    total = len(results)
    healthy = sum(1 for v in results.values() if v.get("ok") or v.get("configured"))
    return jsonify({"ok": True, "results": results, "healthy": healthy, "total": total,
                     "grade": "A" if healthy == total else "B" if healthy >= total - 1 else "C"})


# 9. Price History for Item — trend data for a catalog item
@bp.route("/api/catalog/price-history")
@auth_required
def api_catalog_price_history():
    """Get price history for a catalog item. ?q=description or ?id=catalog_id"""
    try:
        conn = _get_db()
        q = request.args.get("q", "")
        cid = request.args.get("id", "")
        if cid:
            row = conn.execute("SELECT * FROM catalog WHERE id = ?", (cid,)).fetchone()
        elif q:
            row = conn.execute("SELECT * FROM catalog WHERE description LIKE ? LIMIT 1", (f"%{q}%",)).fetchone()
        else:
            return jsonify({"ok": False, "error": "Provide ?q=keyword or ?id=catalog_id"})
        if not row:
            return jsonify({"ok": False, "error": "Item not found"})
        item = dict(row)
        # Parse price history from JSON field
        history = []
        try:
            ph = json.loads(item.get("price_history", "[]"))
            if isinstance(ph, list):
                history = ph
        except Exception:
            pass
        conn.close()
        return jsonify({"ok": True, "item": item.get("description", ""),
                         "current_price": float(item.get("unit_price", 0) or 0),
                         "cost": float(item.get("supplier_cost", 0) or 0),
                         "margin_pct": float(item.get("margin_pct", 0) or 0),
                         "times_quoted": int(item.get("times_quoted", 0) or 0),
                         "history": history[-20:], "history_points": len(history)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# 10. Action Log — server-side action tracking
_action_log = []

@bp.route("/api/agents/log-action", methods=["POST"])
@auth_required
def api_log_action():
    """Log a button action for audit trail."""
    data = request.get_json(silent=True) or {}
    entry = {
        "action": data.get("action", "unknown"),
        "url": data.get("url", ""),
        "timestamp": datetime.now().isoformat(),
        "result": data.get("result", ""),
    }
    _action_log.append(entry)
    if len(_action_log) > 200:
        _action_log.pop(0)
    return jsonify({"ok": True})


@bp.route("/api/agents/action-log")
@auth_required
def api_action_log():
    """Get recent action log."""
    return jsonify({"ok": True, "actions": _action_log[-50:], "count": len(_action_log)})


# 11. QB Summary Card — formatted data for dashboard display
@bp.route("/api/qb/summary-card")
@auth_required
def api_qb_summary_card():
    """Pre-formatted QB financial summary for dashboard cards."""
    try:
        from src.agents.quickbooks_agent import is_configured, get_financial_context
        if not is_configured():
            return jsonify({"ok": False, "connected": False, "error": "QB not configured"})
        ctx = get_financial_context()
        return jsonify({
            "ok": True, "connected": True,
            "receivable": ctx.get("total_receivable", 0),
            "overdue": ctx.get("overdue_amount", 0),
            "collected": ctx.get("total_collected", 0),
            "open_invoices": ctx.get("open_invoices", 0),
            "customers": ctx.get("customer_count", 0),
            "vendors": ctx.get("vendor_count", 0),
            "last_updated": datetime.now().isoformat(),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# 12. Workflow History — recent workflow runs
@bp.route("/api/workflow/history")
@auth_required
def api_workflow_history():
    """Recent workflow execution history."""
    history = []
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
    wf_file = os.path.join(data_dir, "workflow_runs.json")
    try:
        if os.path.exists(wf_file):
            with open(wf_file) as f:
                runs = json.load(f)
                history = runs[-20:] if isinstance(runs, list) else []
    except Exception:
        pass
    return jsonify({"ok": True, "runs": history, "count": len(history)})


# 13. Export Last Result — save to file for download
@bp.route("/api/export/json", methods=["POST"])
@auth_required
def api_export_json():
    """Save JSON data to downloadable file."""
    data = request.get_json(silent=True) or {}
    content = data.get("content", "")
    filename = data.get("filename", f"reytech-export-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json")
    export_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "exports")
    os.makedirs(export_dir, exist_ok=True)
    filepath = os.path.join(export_dir, filename)
    with open(filepath, "w") as f:
        f.write(content if isinstance(content, str) else json.dumps(content, indent=2))
    return jsonify({"ok": True, "file": filename, "path": filepath})


# 14. Favorites — persist user's favorite buttons
_favorites = []

@bp.route("/api/agents/favorites", methods=["GET", "POST"])
@auth_required
def api_agent_favorites():
    """Get or set favorite agent buttons."""
    global _favorites
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        _favorites = data.get("favorites", [])[:10]
        # Persist to file
        fav_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "agent_favorites.json")
        try:
            with open(fav_file, "w") as f:
                json.dump(_favorites, f)
        except Exception:
            pass
        return jsonify({"ok": True, "favorites": _favorites})
    # GET
    if not _favorites:
        fav_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "agent_favorites.json")
        try:
            with open(fav_file) as f:
                _favorites = json.load(f)
        except Exception:
            pass
    return jsonify({"ok": True, "favorites": _favorites})


# 15. Diagnostic Sweep — comprehensive system check
@bp.route("/api/system/diagnostic-sweep")
@auth_required
def api_diagnostic_sweep():
    """Comprehensive diagnostic sweep of all systems."""
    results = {"ok": True, "timestamp": datetime.now().isoformat(), "checks": {}}
    
    # Database
    try:
        conn = _get_db()
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        results["checks"]["database"] = {"ok": True, "tables": len(tables), "table_list": tables}
        # Row counts
        counts = {}
        for t in tables[:20]:
            try:
                counts[t] = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
            except Exception:
                counts[t] = "error"
        results["checks"]["row_counts"] = counts
        conn.close()
    except Exception as e:
        results["checks"]["database"] = {"ok": False, "error": str(e)}
    
    # File system
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
    try:
        files = os.listdir(data_dir)
        json_files = [f for f in files if f.endswith(".json")]
        db_files = [f for f in files if f.endswith(".db") or f.endswith(".sqlite")]
        results["checks"]["filesystem"] = {"ok": True, "total_files": len(files), "json_files": len(json_files), "db_files": len(db_files)}
    except Exception as e:
        results["checks"]["filesystem"] = {"ok": False, "error": str(e)}
    
    # Environment
    env_vars = ["DASH_USER", "DASH_PASS", "QB_CLIENT_ID", "QB_CLIENT_SECRET",
                "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "VAPI_API_KEY",
                "SMTP_USER", "IMAP_USER", "QB_REALM_ID"]
    env_status = {}
    for v in env_vars:
        val = os.environ.get(v, "")
        env_status[v] = "✅ set" if val else "❌ missing"
    results["checks"]["env_vars"] = env_status
    
    # QB
    try:
        from src.agents.quickbooks_agent import is_configured, _load_tokens
        tokens = _load_tokens()
        results["checks"]["quickbooks"] = {
            "configured": is_configured(),
            "token_file_exists": bool(tokens),
            "has_access_token": bool(tokens.get("access_token")),
            "has_refresh_token": bool(tokens.get("refresh_token")),
            "has_realm_id": bool(tokens.get("realm_id")),
            "connected_at": tokens.get("connected_at", ""),
        }
    except Exception:
        results["checks"]["quickbooks"] = {"configured": False, "error": "module unavailable"}
    
    # Summary
    total = len(results["checks"])
    ok_count = sum(1 for v in results["checks"].values() if isinstance(v, dict) and v.get("ok", v.get("configured", False)))
    results["summary"] = f"{ok_count}/{total} checks passed"
    results["grade"] = "A" if ok_count >= total - 1 else "B" if ok_count >= total - 2 else "C"
    
    return jsonify(results)
