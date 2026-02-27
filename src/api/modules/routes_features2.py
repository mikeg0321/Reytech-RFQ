"""
routes_features2.py — 15+ New Business Intelligence Endpoints
Customer health, sales velocity, agency penetration, competitive pricing,
auto-follow-up, revenue trends, morning brief, and more.
"""
import os, json, glob, time, logging, sqlite3
from datetime import datetime, timedelta
from collections import defaultdict

log = logging.getLogger("features2")
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Customer Health Score
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/qb/customer-health")
@auth_required
def api_qb_customer_health():
    """Score customers by payment reliability, order frequency, and value."""
    try:
        from src.agents.quickbooks_agent import fetch_customers, fetch_invoices, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})

        customers = fetch_customers() or []
        invoices = fetch_invoices(status="all", days_back=365) or []

        # Build per-customer stats
        cust_stats = {}
        for inv in invoices:
            cid = (inv.get("CustomerRef") or {}).get("value", "")
            cname = (inv.get("CustomerRef") or {}).get("name", "Unknown")
            if cid not in cust_stats:
                cust_stats[cid] = {"name": cname, "total": 0, "paid": 0, "overdue": 0,
                                   "invoices": 0, "total_amount": 0, "avg_days_to_pay": []}
            cust_stats[cid]["invoices"] += 1
            bal = float(inv.get("Balance", 0))
            total = float(inv.get("TotalAmt", 0))
            cust_stats[cid]["total_amount"] += total

            due = inv.get("DueDate", "")
            if bal <= 0:
                cust_stats[cid]["paid"] += 1
                # Calculate days to pay
                created = inv.get("MetaData", {}).get("CreateTime", "")[:10]
                if created and due:
                    try:
                        d1 = datetime.strptime(created, "%Y-%m-%d")
                        d2 = datetime.strptime(due, "%Y-%m-%d")
                        days = (d2 - d1).days
                        cust_stats[cid]["avg_days_to_pay"].append(max(days, 0))
                    except:
                        pass
            elif due:
                try:
                    if datetime.strptime(due, "%Y-%m-%d") < datetime.now():
                        cust_stats[cid]["overdue"] += 1
                except:
                    pass

        # Score each customer
        scored = []
        for cid, st in cust_stats.items():
            score = 50  # base
            # Payment reliability (0-30)
            if st["invoices"] > 0:
                pay_rate = st["paid"] / st["invoices"]
                score += int(pay_rate * 30)
            # No overdue (0-20)
            if st["overdue"] == 0:
                score += 20
            elif st["overdue"] == 1:
                score += 10
            # Order volume bonus
            if st["total_amount"] > 10000:
                score = min(100, score + 10)
            elif st["total_amount"] > 5000:
                score = min(100, score + 5)

            avg_days = round(sum(st["avg_days_to_pay"]) / max(len(st["avg_days_to_pay"]), 1))
            grade = "A" if score >= 85 else "B" if score >= 70 else "C" if score >= 55 else "D" if score >= 40 else "F"

            scored.append({
                "customer_id": cid, "name": st["name"],
                "score": min(score, 100), "grade": grade,
                "invoices": st["invoices"], "paid": st["paid"],
                "overdue": st["overdue"],
                "total_revenue": round(st["total_amount"], 2),
                "avg_days_to_pay": avg_days
            })

        scored.sort(key=lambda x: x["score"], reverse=True)
        return jsonify({"ok": True, "customers": scored, "count": len(scored)})
    except Exception as e:
        log.exception("customer-health")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Customer Lifetime Value
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/qb/customer-lifetime-value")
@auth_required
def api_qb_customer_ltv():
    """Calculate customer lifetime value from invoice history."""
    try:
        from src.agents.quickbooks_agent import fetch_invoices, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})

        invoices = fetch_invoices(status="all", days_back=730) or []  # 2 years
        cust_data = {}
        for inv in invoices:
            cid = (inv.get("CustomerRef") or {}).get("value", "")
            cname = (inv.get("CustomerRef") or {}).get("name", "Unknown")
            total = float(inv.get("TotalAmt", 0))
            created = inv.get("MetaData", {}).get("CreateTime", "")[:10]
            if cid not in cust_data:
                cust_data[cid] = {"name": cname, "revenue": 0, "orders": 0,
                                  "first_order": created, "last_order": created}
            cust_data[cid]["revenue"] += total
            cust_data[cid]["orders"] += 1
            if created < cust_data[cid]["first_order"]:
                cust_data[cid]["first_order"] = created
            if created > cust_data[cid]["last_order"]:
                cust_data[cid]["last_order"] = created

        results = []
        for cid, d in cust_data.items():
            # Annualized revenue
            try:
                first = datetime.strptime(d["first_order"], "%Y-%m-%d")
                last = datetime.strptime(d["last_order"], "%Y-%m-%d")
                span_months = max((last - first).days / 30, 1)
                monthly_avg = d["revenue"] / span_months
                annual_projected = monthly_avg * 12
            except:
                annual_projected = d["revenue"]

            results.append({
                "customer_id": cid, "name": d["name"],
                "total_revenue": round(d["revenue"], 2),
                "orders": d["orders"],
                "first_order": d["first_order"],
                "last_order": d["last_order"],
                "avg_order_value": round(d["revenue"] / max(d["orders"], 1), 2),
                "annual_projected": round(annual_projected, 2),
                "ltv_3yr": round(annual_projected * 3, 2)
            })
        results.sort(key=lambda x: x["ltv_3yr"], reverse=True)
        total_ltv = sum(r["ltv_3yr"] for r in results)
        return jsonify({"ok": True, "customers": results, "count": len(results),
                        "total_portfolio_ltv": round(total_ltv, 2)})
    except Exception as e:
        log.exception("customer-ltv")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Sales Velocity
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/pipeline/sales-velocity")
@auth_required
def api_pipeline_sales_velocity():
    """Measure how fast deals move through the pipeline."""
    try:
        # rfq.db migrated to reytech.db via get_db()
        if not os.path.exists(db):
            return jsonify({"ok": False, "error": "No database"})
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT id, status, created_at, quoted_at, sent_at, won_at, lost_at
            FROM rfq_records ORDER BY created_at DESC LIMIT 200
        """).fetchall()
        conn.close()

        metrics = {"total": 0, "quoted": 0, "sent": 0, "won": 0, "lost": 0,
                   "days_to_quote": [], "days_to_send": [], "days_to_close": []}
        for r in rows:
            metrics["total"] += 1
            created = r["created_at"] or ""
            if r["quoted_at"]:
                metrics["quoted"] += 1
                try:
                    d1 = datetime.fromisoformat(created[:19])
                    d2 = datetime.fromisoformat(r["quoted_at"][:19])
                    metrics["days_to_quote"].append((d2 - d1).days)
                except: pass
            if r["sent_at"]:
                metrics["sent"] += 1
                try:
                    d1 = datetime.fromisoformat(created[:19])
                    d2 = datetime.fromisoformat(r["sent_at"][:19])
                    metrics["days_to_send"].append((d2 - d1).days)
                except: pass
            if r["won_at"]:
                metrics["won"] += 1
                try:
                    d1 = datetime.fromisoformat(created[:19])
                    d2 = datetime.fromisoformat(r["won_at"][:19])
                    metrics["days_to_close"].append((d2 - d1).days)
                except: pass
            if r["lost_at"]:
                metrics["lost"] += 1

        def avg(lst): return round(sum(lst) / max(len(lst), 1), 1)

        # Sales velocity = (deals * avg_value * win_rate) / avg_cycle
        win_rate = metrics["won"] / max(metrics["won"] + metrics["lost"], 1) * 100
        avg_cycle = avg(metrics["days_to_close"]) if metrics["days_to_close"] else 14

        return jsonify({
            "ok": True,
            "total_deals": metrics["total"],
            "quoted": metrics["quoted"],
            "sent": metrics["sent"],
            "won": metrics["won"],
            "lost": metrics["lost"],
            "win_rate": round(win_rate, 1),
            "avg_days_to_quote": avg(metrics["days_to_quote"]),
            "avg_days_to_send": avg(metrics["days_to_send"]),
            "avg_days_to_close": avg(metrics["days_to_close"]),
            "avg_cycle_days": avg_cycle,
            "velocity_score": round(win_rate / max(avg_cycle, 1), 2)
        })
    except Exception as e:
        log.exception("sales-velocity")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Weekly Summary
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/pipeline/weekly-summary")
@auth_required
def api_pipeline_weekly_summary():
    """This week's pipeline activity — quotes, wins, losses, revenue."""
    try:
        # rfq.db migrated to reytech.db via get_db()
        if not os.path.exists(db):
            return jsonify({"ok": False, "error": "No database"})
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        week_start = (datetime.now() - timedelta(days=datetime.now().weekday())).strftime("%Y-%m-%d")

        new_rfqs = conn.execute("SELECT COUNT(*) as c FROM rfq_records WHERE created_at >= ?", (week_start,)).fetchone()["c"]
        quoted = conn.execute("SELECT COUNT(*) as c FROM rfq_records WHERE quoted_at >= ?", (week_start,)).fetchone()["c"]

        # Check for won/lost this week from win_loss_log
        won_this_week = 0
        lost_this_week = 0
        wl_path = os.path.join(DATA_DIR, "win_loss_log.json")
        if os.path.exists(wl_path):
            with open(wl_path) as f:
                wl = json.load(f)
            for entry in wl.get("entries", []):
                ts = entry.get("timestamp", "")[:10]
                if ts >= week_start:
                    if entry.get("outcome") == "won":
                        won_this_week += 1
                    elif entry.get("outcome") == "lost":
                        lost_this_week += 1

        conn.close()
        return jsonify({
            "ok": True, "week_start": week_start,
            "new_rfqs": new_rfqs,
            "quotes_created": quoted,
            "won": won_this_week,
            "lost": lost_this_week,
            "pending": new_rfqs - won_this_week - lost_this_week
        })
    except Exception as e:
        log.exception("weekly-summary")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Top Products
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/catalog/top-products-report")
@auth_required
def api_catalog_top_products_report():
    """Most quoted, highest margin, and most won products."""
    try:
        cat_path = os.path.join(DATA_DIR, "product_catalog.json")
        if not os.path.exists(cat_path):
            return jsonify({"ok": False, "error": "No catalog"})
        with open(cat_path) as f:
            catalog = json.load(f)

        products = catalog.get("products", [])
        # Top quoted
        by_quoted = sorted(products, key=lambda p: p.get("times_quoted", 0), reverse=True)[:10]
        # Best margin
        by_margin = sorted(
            [p for p in products if p.get("avg_sell_price") and p.get("avg_cost")],
            key=lambda p: (p["avg_sell_price"] - p["avg_cost"]) / max(p["avg_sell_price"], 0.01) * 100,
            reverse=True
        )[:10]
        # Most recently won (from win_loss_log)
        won_items = []
        wl_path = os.path.join(DATA_DIR, "win_loss_log.json")
        if os.path.exists(wl_path):
            with open(wl_path) as f:
                wl = json.load(f)
            for entry in wl.get("entries", []):
                if entry.get("outcome") == "won":
                    won_items.append(entry.get("rfq_id", ""))

        return jsonify({
            "ok": True,
            "total_products": len(products),
            "top_quoted": [{"name": p.get("description", "")[:60], "times_quoted": p.get("times_quoted", 0),
                            "avg_price": p.get("avg_sell_price")} for p in by_quoted],
            "best_margin": [{"name": p.get("description", "")[:60],
                             "margin_pct": round((p["avg_sell_price"] - p["avg_cost"]) / max(p["avg_sell_price"], 0.01) * 100, 1),
                             "avg_price": p.get("avg_sell_price"),
                             "avg_cost": p.get("avg_cost")} for p in by_margin],
            "recent_wins": len(won_items)
        })
    except Exception as e:
        log.exception("top-products")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Agency Penetration
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/intel/agency-penetration")
@auth_required
def api_intel_agency_penetration():
    """How deep we've penetrated each agency — facilities, contacts, quotes."""
    try:
        crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
        crm = {}
        if os.path.exists(crm_path):
            with open(crm_path) as f:
                crm = json.load(f)

        agencies = {}
        for contact in crm.get("contacts", []):
            agency = contact.get("agency") or contact.get("organization") or "Unknown"
            if agency not in agencies:
                agencies[agency] = {"contacts": 0, "facilities": set(), "emailed": 0, "has_phone": 0}
            agencies[agency]["contacts"] += 1
            fac = contact.get("facility") or contact.get("institution") or ""
            if fac:
                agencies[agency]["facilities"].add(fac)
            if contact.get("status") == "emailed":
                agencies[agency]["emailed"] += 1
            if contact.get("phone"):
                agencies[agency]["has_phone"] += 1

        # Count quotes per agency from DB
        # rfq.db migrated to reytech.db via get_db()
        quote_counts = {}
        if True:  # migrated to reytech.db
            from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT agency, COUNT(*) as c FROM rfq_records WHERE agency IS NOT NULL GROUP BY agency").fetchall()
            conn.close()
            for r in rows:
                quote_counts[r["agency"]] = r["c"]

        results = []
        for ag, data in agencies.items():
            quotes = quote_counts.get(ag, 0)
            fac_count = len(data["facilities"])
            # Penetration score: contacts + facilities + quotes
            score = min(100, data["contacts"] * 5 + fac_count * 15 + quotes * 10)
            results.append({
                "agency": ag, "contacts": data["contacts"],
                "facilities": fac_count, "facility_names": sorted(data["facilities"]),
                "emailed": data["emailed"], "quotes": quotes,
                "penetration_score": score,
                "grade": "A" if score >= 80 else "B" if score >= 50 else "C" if score >= 25 else "D"
            })
        results.sort(key=lambda x: x["penetration_score"], reverse=True)
        return jsonify({"ok": True, "agencies": results, "count": len(results)})
    except Exception as e:
        log.exception("agency-penetration")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Competitive Pricing Suggestions
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/intel/competitive-pricing")
@auth_required
def api_intel_competitive_pricing():
    """Suggest prices based on win/loss history and catalog data."""
    try:
        cat_path = os.path.join(DATA_DIR, "product_catalog.json")
        if not os.path.exists(cat_path):
            return jsonify({"ok": False, "error": "No catalog"})
        with open(cat_path) as f:
            catalog = json.load(f)

        wl_path = os.path.join(DATA_DIR, "win_loss_log.json")
        wl_entries = []
        if os.path.exists(wl_path):
            with open(wl_path) as f:
                wl_entries = json.load(f).get("entries", [])

        suggestions = []
        for prod in catalog.get("products", []):
            if not prod.get("avg_sell_price"):
                continue
            sell = prod["avg_sell_price"]
            cost = prod.get("avg_cost", 0)
            margin_pct = ((sell - cost) / max(sell, 0.01)) * 100 if cost else None

            suggestion = {
                "product": prod.get("description", "")[:60],
                "current_price": sell,
                "cost": cost,
                "margin_pct": round(margin_pct, 1) if margin_pct else None,
                "times_quoted": prod.get("times_quoted", 0)
            }

            # If margin is very high (>40%) and we're losing, suggest lower
            if margin_pct and margin_pct > 40:
                suggestion["recommendation"] = "Consider lowering price — high margin may be losing deals"
                suggestion["suggested_price"] = round(cost * 1.25 if cost else sell * 0.85, 2)
            elif margin_pct and margin_pct < 10:
                suggestion["recommendation"] = "Margin too thin — raise price or find cheaper supplier"
                suggestion["suggested_price"] = round(cost * 1.20 if cost else sell * 1.10, 2)
            else:
                suggestion["recommendation"] = "Price looks competitive"
                suggestion["suggested_price"] = sell

            suggestions.append(suggestion)

        suggestions.sort(key=lambda x: x.get("times_quoted", 0), reverse=True)
        return jsonify({"ok": True, "suggestions": suggestions[:30], "total": len(suggestions)})
    except Exception as e:
        log.exception("competitive-pricing")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 8. Auto-Follow-Up Queue
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/quotes/auto-follow-up")
@auth_required
def api_quotes_auto_follow_up():
    """Quotes sent 3+ days ago with no response — need follow-up."""
    try:
        # rfq.db migrated to reytech.db via get_db()
        if not os.path.exists(db):
            return jsonify({"ok": False, "error": "No database"})
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.now() - timedelta(days=3)).isoformat()
        rows = conn.execute("""
            SELECT id, solicitation_number, institution, requestor, requestor_email,
                   status, sent_at, total_amount
            FROM rfq_records
            WHERE status IN ('sent','quoted') AND (sent_at IS NOT NULL AND sent_at < ?)
            ORDER BY sent_at ASC
        """, (cutoff,)).fetchall()
        conn.close()

        needs_follow_up = []
        for r in rows:
            days_since = 0
            try:
                sent = datetime.fromisoformat(r["sent_at"][:19])
                days_since = (datetime.now() - sent).days
            except: pass

            urgency = "high" if days_since > 7 else "medium" if days_since > 4 else "low"
            needs_follow_up.append({
                "rfq_id": r["id"],
                "solicitation": r["solicitation_number"],
                "institution": r["institution"],
                "requestor": r["requestor"],
                "email": r["requestor_email"],
                "amount": r["total_amount"],
                "sent_at": r["sent_at"],
                "days_since_sent": days_since,
                "urgency": urgency
            })

        return jsonify({
            "ok": True, "needs_follow_up": needs_follow_up,
            "count": len(needs_follow_up),
            "high_urgency": sum(1 for f in needs_follow_up if f["urgency"] == "high")
        })
    except Exception as e:
        log.exception("auto-follow-up")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 9. Quote Expiration Alert
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/quotes/expiring")
@auth_required
def api_quotes_expiring():
    """Quotes expiring within the next 7 days."""
    try:
        quotes_dir = os.path.join(DATA_DIR, "quotes")
        expiring = []
        if os.path.isdir(quotes_dir):
            for qf in glob.glob(os.path.join(quotes_dir, "*.json")):
                try:
                    with open(qf) as f:
                        q = json.load(f)
                    exp_date = q.get("expiration_date") or q.get("valid_until", "")
                    if exp_date:
                        exp = datetime.strptime(exp_date[:10], "%Y-%m-%d")
                        days_left = (exp - datetime.now()).days
                        if -7 <= days_left <= 7:  # Include recently expired too
                            expiring.append({
                                "quote_number": q.get("quote_number", os.path.basename(qf)),
                                "institution": q.get("institution", ""),
                                "amount": q.get("total", 0),
                                "expires": exp_date[:10],
                                "days_left": days_left,
                                "status": "expired" if days_left < 0 else "expiring_soon"
                            })
                except:
                    pass

        expiring.sort(key=lambda x: x["days_left"])
        return jsonify({"ok": True, "expiring": expiring, "count": len(expiring)})
    except Exception as e:
        log.exception("quotes-expiring")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 10. Revenue by Agency
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/intel/revenue-by-agency")
@auth_required
def api_intel_revenue_by_agency():
    """Revenue breakdown by agency from quotes and QB data."""
    try:
        # rfq.db migrated to reytech.db via get_db()
        if not os.path.exists(db):
            return jsonify({"ok": False, "error": "No database"})
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT agency, status, total_amount
            FROM rfq_records WHERE agency IS NOT NULL
        """).fetchall()
        conn.close()

        agencies = {}
        for r in rows:
            ag = r["agency"] or "Unknown"
            if ag not in agencies:
                agencies[ag] = {"quoted": 0, "quoted_value": 0, "won": 0, "won_value": 0,
                                "lost": 0, "pending": 0}
            amt = float(r["total_amount"] or 0)
            agencies[ag]["quoted"] += 1
            agencies[ag]["quoted_value"] += amt
            status = (r["status"] or "").lower()
            if status in ("won", "ordered", "po_received"):
                agencies[ag]["won"] += 1
                agencies[ag]["won_value"] += amt
            elif status in ("lost",):
                agencies[ag]["lost"] += 1
            else:
                agencies[ag]["pending"] += 1

        results = []
        for ag, d in agencies.items():
            win_rate = d["won"] / max(d["won"] + d["lost"], 1) * 100
            results.append({
                "agency": ag,
                "total_quoted": d["quoted"],
                "quoted_value": round(d["quoted_value"], 2),
                "won": d["won"],
                "won_value": round(d["won_value"], 2),
                "lost": d["lost"],
                "pending": d["pending"],
                "win_rate": round(win_rate, 1)
            })
        results.sort(key=lambda x: x["won_value"], reverse=True)
        total_won = sum(r["won_value"] for r in results)
        return jsonify({"ok": True, "agencies": results, "total_won_value": round(total_won, 2)})
    except Exception as e:
        log.exception("revenue-by-agency")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 11. Morning Brief (Consolidated)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/dashboard/morning-brief")
@auth_required
def api_dashboard_morning_brief():
    """One-call consolidated morning briefing: key metrics, alerts, and actions."""
    brief = {"ok": True, "generated": datetime.now().isoformat(), "sections": {}}

    # Pipeline stats
    try:
        # rfq.db migrated to reytech.db via get_db()
        if True:  # migrated to reytech.db
            from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
            conn.row_factory = sqlite3.Row
            today = datetime.now().strftime("%Y-%m-%d")
            week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

            total = conn.execute("SELECT COUNT(*) as c FROM rfq_records").fetchone()["c"]
            new_this_week = conn.execute("SELECT COUNT(*) as c FROM rfq_records WHERE created_at >= ?", (week_ago,)).fetchone()["c"]
            pending = conn.execute("SELECT COUNT(*) as c FROM rfq_records WHERE status IN ('new','draft','priced','quoted')").fetchone()["c"]
            overdue = conn.execute("SELECT COUNT(*) as c FROM rfq_records WHERE due_date < ? AND status NOT IN ('sent','won','lost','ordered')", (today,)).fetchone()["c"]
            conn.close()

            brief["sections"]["pipeline"] = {
                "total_rfqs": total, "new_this_week": new_this_week,
                "pending_action": pending, "overdue": overdue
            }
    except Exception as e:
        brief["sections"]["pipeline"] = {"error": str(e)}

    # QB summary
    try:
        from src.agents.quickbooks_agent import is_configured, get_financial_context
        if is_configured():
            ctx = get_financial_context()
            brief["sections"]["financial"] = {
                "receivable": ctx.get("total_receivable", 0),
                "overdue": ctx.get("overdue_amount", 0),
                "collected_30d": ctx.get("total_collected", 0),
                "open_invoices": ctx.get("open_invoices", 0)
            }
    except:
        pass

    # Price alerts count
    try:
        cat_path = os.path.join(DATA_DIR, "product_catalog.json")
        if os.path.exists(cat_path):
            with open(cat_path) as f:
                cat = json.load(f)
            brief["sections"]["catalog"] = {
                "total_products": len(cat.get("products", [])),
                "with_pricing": sum(1 for p in cat.get("products", []) if p.get("avg_sell_price"))
            }
    except:
        pass

    # Outreach stats
    try:
        oq_path = os.path.join(DATA_DIR, "outreach_queue.json")
        if os.path.exists(oq_path):
            with open(oq_path) as f:
                oq = json.load(f)
            sent = len(oq.get("emails", []))
            follow_ups = sum(1 for e in oq.get("emails", [])
                             if e.get("status") == "sent" and
                             e.get("follow_up_date", "") <= datetime.now().strftime("%Y-%m-%d"))
            brief["sections"]["outreach"] = {"sent": sent, "follow_ups_due": follow_ups}
    except:
        pass

    # Actions needed
    actions = []
    pipeline = brief["sections"].get("pipeline", {})
    if pipeline.get("overdue", 0) > 0:
        actions.append(f"⚠️ {pipeline['overdue']} overdue RFQs need attention")
    if pipeline.get("pending_action", 0) > 5:
        actions.append(f"📋 {pipeline['pending_action']} quotes pending action")
    financial = brief["sections"].get("financial", {})
    if financial.get("overdue", 0) > 0:
        actions.append(f"💰 ${financial['overdue']:,.0f} in overdue invoices")
    outreach = brief["sections"].get("outreach", {})
    if outreach.get("follow_ups_due", 0) > 0:
        actions.append(f"📧 {outreach['follow_ups_due']} outreach follow-ups due")

    brief["actions_needed"] = actions
    brief["action_count"] = len(actions)
    return jsonify(brief)


# ═══════════════════════════════════════════════════════════════════════════════
# 12. PO Match
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/pipeline/po-match")
@auth_required
def api_pipeline_po_match():
    """Match POs to quotes — find quotes that became orders."""
    try:
        # rfq.db migrated to reytech.db via get_db()
        if not os.path.exists(db):
            return jsonify({"ok": False, "error": "No database"})
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row

        # Get won/ordered quotes
        quotes = conn.execute("""
            SELECT id, solicitation_number, institution, requestor, total_amount,
                   status, won_at, po_number
            FROM rfq_records
            WHERE status IN ('won','ordered','po_received') OR po_number IS NOT NULL
            ORDER BY won_at DESC
        """).fetchall()

        # Get QB POs for matching
        po_list = []
        try:
            from src.agents.quickbooks_agent import get_recent_purchase_orders, is_configured
            if is_configured():
                po_list = get_recent_purchase_orders(days_back=90) or []
        except:
            pass

        matches = []
        unmatched_quotes = []
        for q in quotes:
            matched = False
            for po in po_list:
                po_num = po.get("DocNumber", "")
                if q["po_number"] and po_num and q["po_number"].lower() in po_num.lower():
                    matches.append({
                        "rfq_id": q["id"],
                        "solicitation": q["solicitation_number"],
                        "institution": q["institution"],
                        "quote_amount": q["total_amount"],
                        "po_number": po_num,
                        "po_amount": float(po.get("TotalAmt", 0)),
                        "po_date": po.get("TxnDate", ""),
                        "status": "matched"
                    })
                    matched = True
                    break
            if not matched:
                unmatched_quotes.append({
                    "rfq_id": q["id"],
                    "solicitation": q["solicitation_number"],
                    "institution": q["institution"],
                    "amount": q["total_amount"],
                    "status": q["status"]
                })

        conn.close()
        return jsonify({
            "ok": True,
            "matched": matches, "matched_count": len(matches),
            "unmatched": unmatched_quotes[:20], "unmatched_count": len(unmatched_quotes)
        })
    except Exception as e:
        log.exception("po-match")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 13. Price Comparison (Our price vs market)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/catalog/price-comparison")
@auth_required
def api_catalog_price_comparison():
    """Compare our catalog prices vs supplier costs and market rates."""
    try:
        cat_path = os.path.join(DATA_DIR, "product_catalog.json")
        if not os.path.exists(cat_path):
            return jsonify({"ok": False, "error": "No catalog"})
        with open(cat_path) as f:
            catalog = json.load(f)

        comparisons = []
        for prod in catalog.get("products", []):
            sell = prod.get("avg_sell_price", 0)
            cost = prod.get("avg_cost", 0)
            if not sell:
                continue

            comp = {
                "product": prod.get("description", "")[:60],
                "our_price": sell,
                "cost": cost,
                "margin_pct": round(((sell - cost) / max(sell, 0.01)) * 100, 1) if cost else None,
                "markup_pct": round(((sell - cost) / max(cost, 0.01)) * 100, 1) if cost else None,
                "times_quoted": prod.get("times_quoted", 0)
            }

            # Check price history for trends
            history = prod.get("price_history", [])
            if len(history) >= 2:
                recent = history[-1].get("price", sell)
                oldest = history[0].get("price", sell)
                comp["price_trend"] = "up" if recent > oldest * 1.05 else "down" if recent < oldest * 0.95 else "stable"
                comp["price_change_pct"] = round(((recent - oldest) / max(oldest, 0.01)) * 100, 1)
            else:
                comp["price_trend"] = "insufficient_data"

            comparisons.append(comp)

        comparisons.sort(key=lambda x: abs(x.get("margin_pct") or 0))
        return jsonify({
            "ok": True, "comparisons": comparisons[:50],
            "total": len(comparisons),
            "avg_margin": round(sum(c.get("margin_pct") or 0 for c in comparisons) / max(len(comparisons), 1), 1)
        })
    except Exception as e:
        log.exception("price-comparison")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 14. Payment Aging Trend
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/qb/payment-aging-trend")
@auth_required
def api_qb_payment_aging_trend():
    """Track how quickly customers pay invoices over time."""
    try:
        from src.agents.quickbooks_agent import fetch_invoices, get_recent_payments, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})

        invoices = fetch_invoices(status="all", days_back=365) or []
        payments = get_recent_payments(days_back=365) or []

        # Bucket by month
        monthly = defaultdict(lambda: {"paid": 0, "total": 0, "days": [], "amount": 0})
        for inv in invoices:
            created = (inv.get("MetaData", {}).get("CreateTime", "") or "")[:7]  # YYYY-MM
            if not created:
                continue
            bal = float(inv.get("Balance", 0))
            total = float(inv.get("TotalAmt", 0))
            monthly[created]["total"] += 1
            monthly[created]["amount"] += total
            if bal <= 0:
                monthly[created]["paid"] += 1

        months_sorted = sorted(monthly.keys())
        trend = []
        for m in months_sorted[-12:]:
            d = monthly[m]
            pay_rate = d["paid"] / max(d["total"], 1) * 100
            trend.append({
                "month": m,
                "invoices": d["total"],
                "paid": d["paid"],
                "payment_rate": round(pay_rate, 1),
                "total_amount": round(d["amount"], 2)
            })

        return jsonify({"ok": True, "trend": trend, "months": len(trend)})
    except Exception as e:
        log.exception("payment-aging-trend")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 15. Reorder Alerts
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/catalog/reorder-alerts")
@auth_required
def api_catalog_reorder_alerts():
    """Items frequently ordered that may need restocking or re-quoting."""
    try:
        cat_path = os.path.join(DATA_DIR, "product_catalog.json")
        if not os.path.exists(cat_path):
            return jsonify({"ok": False, "error": "No catalog"})
        with open(cat_path) as f:
            catalog = json.load(f)

        alerts = []
        for prod in catalog.get("products", []):
            quoted = prod.get("times_quoted", 0)
            if quoted < 2:
                continue
            last_seen = prod.get("last_seen", "")
            days_since = 999
            if last_seen:
                try:
                    days_since = (datetime.now() - datetime.fromisoformat(last_seen[:19])).days
                except: pass

            # Frequently quoted but not seen recently = may need re-quote
            if days_since > 30 and quoted >= 3:
                alerts.append({
                    "product": prod.get("description", "")[:60],
                    "times_quoted": quoted,
                    "last_seen": last_seen[:10] if last_seen else "unknown",
                    "days_since_last": days_since,
                    "avg_price": prod.get("avg_sell_price"),
                    "suppliers": prod.get("supplier_urls", [])[:3],
                    "alert": "Frequently quoted item not seen in 30+ days — re-check pricing"
                })

        alerts.sort(key=lambda x: x["times_quoted"], reverse=True)
        return jsonify({"ok": True, "alerts": alerts[:20], "count": len(alerts)})
    except Exception as e:
        log.exception("reorder-alerts")
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# 16. Full System Heartbeat
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/system/heartbeat")
@auth_required
def api_system_heartbeat():
    """Quick health check of all major subsystems in one call."""
    results = {"ok": True, "timestamp": datetime.now().isoformat(), "systems": {}}

    # Database
    try:
        # rfq.db migrated to reytech.db via get_db()
        if True:  # migrated to reytech.db
            from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
            ct = conn.execute("SELECT COUNT(*) FROM rfq_records").fetchone()[0]
            conn.close()
            results["systems"]["database"] = {"status": "ok", "rfq_count": ct}
        else:
            results["systems"]["database"] = {"status": "missing"}
    except Exception as e:
        results["systems"]["database"] = {"status": "error", "error": str(e)}

    # Catalog
    try:
        cat_path = os.path.join(DATA_DIR, "product_catalog.json")
        if os.path.exists(cat_path):
            with open(cat_path) as f:
                cat = json.load(f)
            results["systems"]["catalog"] = {"status": "ok", "products": len(cat.get("products", []))}
        else:
            results["systems"]["catalog"] = {"status": "empty"}
    except Exception as e:
        results["systems"]["catalog"] = {"status": "error", "error": str(e)}

    # CRM
    try:
        crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
        if os.path.exists(crm_path):
            with open(crm_path) as f:
                crm = json.load(f)
            results["systems"]["crm"] = {"status": "ok", "contacts": len(crm.get("contacts", []))}
        else:
            results["systems"]["crm"] = {"status": "empty"}
    except Exception as e:
        results["systems"]["crm"] = {"status": "error", "error": str(e)}

    # QuickBooks
    try:
        from src.agents.quickbooks_agent import is_configured, get_access_token
        if is_configured():
            token = get_access_token()
            results["systems"]["quickbooks"] = {"status": "connected" if token else "token_expired",
                                                "configured": True}
        else:
            results["systems"]["quickbooks"] = {"status": "not_configured"}
    except:
        results["systems"]["quickbooks"] = {"status": "unavailable"}

    # Email
    try:
        results["systems"]["email"] = {
            "status": "configured" if os.environ.get("EMAIL_USER") else "not_configured"
        }
    except:
        pass

    all_ok = all(s.get("status") in ("ok", "connected", "configured") for s in results["systems"].values())
    results["overall"] = "healthy" if all_ok else "degraded"
    return jsonify(results)


# ═══════════════════════════════════════════════════════════════════════════════
# 17. Draft Follow-Up Email
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/quotes/draft-follow-up", methods=["POST"])
@auth_required
def api_quotes_draft_follow_up():
    """Draft a follow-up email for a stale quote."""
    try:
        data = request.get_json(silent=True) or {}
        rfq_id = data.get("rfq_id", "")
        if not rfq_id:
            return jsonify({"ok": False, "error": "rfq_id required"})

        # rfq.db migrated to reytech.db via get_db()
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM rfq_records WHERE id = ?", (rfq_id,)).fetchone()
        conn.close()

        if not row:
            return jsonify({"ok": False, "error": f"RFQ {rfq_id} not found"})

        sol = row["solicitation_number"] or rfq_id
        inst = row["institution"] or "your facility"
        name = row["requestor"] or "Procurement"
        email = row["requestor_email"] or ""

        subject = f"Following Up — Reytech Quote for {sol}"
        body = f"""Hi {name},

I wanted to follow up on the quote we submitted for solicitation {sol} at {inst}.

Please let me know if you have any questions about our pricing or if there's anything else we can provide to assist with your decision.

We appreciate the opportunity to serve {inst} and look forward to hearing from you.

Best regards,
Reytech Inc."""

        return jsonify({
            "ok": True,
            "draft": {
                "to": email,
                "subject": subject,
                "body": body,
                "rfq_id": rfq_id,
                "institution": inst
            }
        })
    except Exception as e:
        log.exception("draft-follow-up")
        return jsonify({"ok": False, "error": str(e)})
