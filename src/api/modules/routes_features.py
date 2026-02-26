"""
routes_features.py — 24 New Feature Endpoints for Agents Page
Batch 2: QB Actions, Quote-to-Cash Pipeline, Catalog Intelligence,
         System Dashboard, Data Quality
"""
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
        except:
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
            except:
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
            except:
                days_until = 30
            if days_until <= 30:
                inflows.append({"source": f"Invoice #{inv.get('DocNumber', '?')}", "amount": amount,
                                "due": due_str, "days_until": days_until,
                                "customer": inv.get("CustomerRef", {}).get("name", "?")})

        # Pipeline value
        db_path = os.path.join(DATA_DIR, "rfq.db")
        pipeline_value = 0
        if os.path.exists(db_path):
            try:
                conn = sqlite3.connect(db_path)
                cur = conn.execute("SELECT SUM(total) FROM quotes WHERE status IN ('sent','quoted') AND total > 0")
                row = cur.fetchone()
                pipeline_value = float(row[0] or 0)
                conn.close()
            except:
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

        db_path = os.path.join(DATA_DIR, "rfq.db")
        conn = sqlite3.connect(db_path)
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
            except:
                continue
            monthly[key] += float(p.get("TotalAmt", 0))
        result = [{"month": k, "revenue": round(v, 2)} for k, v in sorted(monthly.items())]
        return jsonify({"ok": True, "months": result, "ytd_total": round(sum(v["revenue"] for v in result if v["month"].startswith(str(datetime.now().year))), 2)})
    except Exception as e:
        log.exception("Revenue by month failed")
        return jsonify({"ok": False, "error": str(e)})


# ── Pipeline Endpoints ──────────────────────────────────────────────────────

@bp.route("/api/pipeline/quote-to-cash")
@auth_required
def api_pipeline_quote_to_cash():
    """Full quote-to-cash pipeline with every quote's current stage."""
    db_path = os.path.join(DATA_DIR, "rfq.db")
    if not os.path.exists(db_path):
        return jsonify({"ok": False, "error": "No database"})
    try:
        conn = sqlite3.connect(db_path)
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
    db_path = os.path.join(DATA_DIR, "rfq.db")
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "stale": [], "count": 0})
    try:
        conn = sqlite3.connect(db_path)
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
            except:
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
    db_path = os.path.join(DATA_DIR, "rfq.db")
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "queue": []})
    try:
        conn = sqlite3.connect(db_path)
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
            except:
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
    db_path = os.path.join(DATA_DIR, "rfq.db")
    now = datetime.now()
    months_passed = now.month + (now.day / 30.0)
    months_left = 12 - months_passed

    won_total = 0
    pipeline_value = 0
    monthly_data = defaultdict(float)

    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
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
                except:
                    pass
            conn.close()
        except:
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
    db_path = os.path.join(DATA_DIR, "rfq.db")
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "funnel": {}})
    try:
        conn = sqlite3.connect(db_path)
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
    db_path = os.path.join(DATA_DIR, "rfq.db")
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "overall": 0, "by_agency": {}})
    try:
        conn = sqlite3.connect(db_path)
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
        conn = sqlite3.connect(db_path)
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
        conn = sqlite3.connect(db_path)
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
        conn = sqlite3.connect(db_path)
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
            except:
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
            except:
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
        except:
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
    db_path = os.path.join(DATA_DIR, "rfq.db")
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
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
        except:
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
        except:
            pass

    return jsonify({"ok": True, "issues": issues, "count": len(issues)})


@bp.route("/api/data-quality/orphaned-quotes")
@auth_required
def api_data_quality_orphaned_quotes():
    """Find quotes not linked to any CRM contact."""
    db_path = os.path.join(DATA_DIR, "rfq.db")
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

        conn = sqlite3.connect(db_path)
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
