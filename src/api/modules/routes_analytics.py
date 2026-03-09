# routes_analytics.py — Pipeline Analytics, Buyer Intelligence, Margin Optimizer,
# Settings, API v1 endpoints, SSE Progress
# PRD v29 Enhancements: E3, E6, E7, E10, E13, E15

# NOTE: This module is exec'd in dashboard.py globals — bp, load_rfqs, save_rfqs,
# _load_price_checks, _save_price_checks, render_page, auth_required, CONFIG,
# DATA_DIR, jsonify, request, redirect, flash, datetime, os, json, log are all available.
# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from flask import redirect, flash
from src.core.paths import DATA_DIR
from src.core.db import get_db
from src.api.render import render_page

import time as _time
import threading as _threading
import sqlite3 as _sqlite3
from collections import defaultdict as _defaultdict
from datetime import timedelta as _timedelta

# ═══════════════════════════════════════════════════════════════════════════════
# E3: Progress Tracking for Long-Running Operations (file-based for multi-worker)
# ═══════════════════════════════════════════════════════════════════════════════

def _progress_file(task_id):
    """Progress file path — uses DATA_DIR so it works on Railway persistent volume."""
    _pdir = os.path.join(DATA_DIR, "progress")
    os.makedirs(_pdir, exist_ok=True)
    return os.path.join(_pdir, f"{task_id}.json")

def _emit_progress(task_id, step, detail="", done=False):
    """Write progress to a file so any gunicorn worker can read it."""
    pf = _progress_file(task_id)
    try:
        with open(pf, "r") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"steps": [], "done": False}
    data["steps"].append({"step": step, "detail": detail, "ts": _time.time()})
    if done:
        data["done"] = True
    with open(pf, "w") as f:
        json.dump(data, f)

@bp.route("/api/progress/<task_id>")
@auth_required
def poll_progress(task_id):
    """JSON polling endpoint — returns progress steps since last_idx."""
    last_idx = int(request.args.get("since", 0))
    pf = _progress_file(task_id)
    try:
        with open(pf, "r") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return jsonify({"steps": [], "done": False, "next_idx": last_idx})
    steps = data.get("steps", [])
    new_steps = steps[last_idx:]
    is_done = data.get("done", False)
    # Cleanup old progress files (>5 min old)
    if is_done:
        try:
            os.remove(pf)
        except OSError:
            pass
    return jsonify({"steps": new_steps, "done": is_done, "next_idx": len(steps)})


@bp.route("/api/rfq/<rid>/auto-lookup", methods=["POST"])
@auth_required
def rfq_auto_lookup(rid):
    """Run SCPRS + Amazon + Catalog lookup with real-time progress via SSE."""
    import uuid
    task_id = str(uuid.uuid4())[:8]

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404

    def _run():
        try:
            items = r.get("line_items", [])
            total = len(items)

            # Step 1: SCPRS
            _emit_progress(task_id, "scprs", f"Checking SCPRS for {total} items...")
            scprs_found = 0
            try:
                from src.agents.scprs_lookup import bulk_lookup
                r["line_items"] = bulk_lookup(items)
                scprs_found = sum(1 for i in r["line_items"] if i.get("scprs_last_price"))
                _emit_progress(task_id, "scprs_done", f"SCPRS: {scprs_found}/{total} found")
            except Exception as e:
                log.error("Auto-lookup SCPRS error: %s", e, exc_info=True)
                _emit_progress(task_id, "scprs_error", f"SCPRS error: {str(e)[:80]}")

            # Step 2: Amazon
            _emit_progress(task_id, "amazon", f"Checking Amazon for {total} items...")
            amazon_found = 0
            try:
                from src.agents.web_price_research import research_items
                r["line_items"] = research_items(r["line_items"])
                amazon_found = sum(1 for i in r["line_items"] if i.get("amazon_price"))
                _emit_progress(task_id, "amazon_done", f"Amazon: {amazon_found}/{total} found")
            except Exception as e:
                log.error("Auto-lookup Amazon error: %s", e, exc_info=True)
                _emit_progress(task_id, "amazon_error", f"Amazon error: {str(e)[:80]}")

            # Step 3: Catalog
            _emit_progress(task_id, "catalog", "Checking internal catalog...")
            catalog_found = 0
            try:
                from src.agents.product_catalog import match_item, init_catalog_db
                init_catalog_db()
                for item in r["line_items"]:
                    matches = match_item(item.get("description", ""), item.get("item_number", ""))
                    if matches and isinstance(matches, list) and len(matches) > 0:
                        best = matches[0]
                        if best.get("confidence", 0) > 0.5:
                            item["catalog_match"] = best
                            catalog_found += 1
                    elif matches and isinstance(matches, dict) and matches.get("confidence", 0) > 0.5:
                        item["catalog_match"] = matches
                        catalog_found += 1
                _emit_progress(task_id, "catalog_done", f"Catalog: {catalog_found}/{total} found")
            except Exception as e:
                log.error("Auto-lookup Catalog error: %s", e, exc_info=True)
                _emit_progress(task_id, "catalog_error", f"Catalog error: {str(e)[:80]}")

            # Step 4: Apply margin recommendations (E7)
            _emit_progress(task_id, "margins", "Computing recommended prices...")
            priced = 0
            try:
                for item in r["line_items"]:
                    rec = _compute_recommended_price(item)
                    if rec:
                        item["recommended_price"] = rec["price"]
                        item["recommended_reason"] = rec["reason"]
                        item["recommended_confidence"] = rec["confidence"]
                        priced += 1
                _emit_progress(task_id, "margins_done", f"Recommendations: {priced}/{total} items")
            except Exception as e:
                log.error("Auto-lookup margins error: %s", e, exc_info=True)
                _emit_progress(task_id, "margins_error", f"Margins error: {str(e)[:80]}")

            # Save results
            r["auto_lookup_results"] = {
                "scprs_found": sum(1 for i in r["line_items"] if i.get("scprs_last_price")),
                "amazon_found": sum(1 for i in r["line_items"] if i.get("amazon_price")),
                "catalog_found": catalog_found,
                "priced": priced,
                "total": total,
                "ran_at": datetime.now().isoformat(),
            }
            rfqs_fresh = load_rfqs()
            rfqs_fresh[rid] = r
            save_rfqs(rfqs_fresh)
            _emit_progress(task_id, "saved", "Results saved", done=True)
        except Exception as e:
            log.error("Auto-lookup FATAL error: %s", e, exc_info=True)
            _emit_progress(task_id, "fatal_error", f"Fatal: {str(e)[:100]}", done=True)

    _threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "task_id": task_id})


# ═══════════════════════════════════════════════════════════════════════════════
# E7: Margin Optimizer — Auto-recommend prices
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_recommended_price(item):
    """Compute recommended bid price for a line item using intelligence stack."""
    scprs = item.get("scprs_last_price") or 0
    amazon = item.get("amazon_price") or 0
    cost = item.get("supplier_cost") or 0
    catalog = (item.get("catalog_match") or {}).get("sell_price") or 0

    # Priority 1: Undercut SCPRS by 2%
    if scprs > 0:
        price = round(scprs * 0.98, 2)
        if cost > 0 and price < cost * 1.05:
            price = round(cost * 1.10, 2)  # Minimum 10% margin on cost
        return {"price": price, "reason": f"SCPRS ${scprs:.2f} - 2%", "confidence": "high"}

    # Priority 2: Historical winning price
    try:
        from src.knowledge.won_quotes_db import find_similar_wins
        wins = find_similar_wins(item.get("description", ""), item.get("item_number", ""))
        if wins:
            avg_win = sum(w.get("price", 0) for w in wins[:3]) / min(3, len(wins))
            if avg_win > 0:
                return {"price": round(avg_win, 2), "reason": f"Won avg ${avg_win:.2f} ({len(wins)} wins)", "confidence": "high"}
    except Exception:
        pass

    # Priority 3: Catalog sell price
    if catalog > 0:
        return {"price": round(catalog, 2), "reason": f"Catalog ${catalog:.2f}", "confidence": "medium"}

    # Priority 4: Markup from Amazon wholesale
    if amazon > 0:
        price = round(amazon * 1.20, 2)
        return {"price": price, "reason": f"Amazon ${amazon:.2f} + 20%", "confidence": "medium"}

    # Priority 5: Markup from known cost
    if cost > 0:
        price = round(cost * 1.25, 2)
        return {"price": price, "reason": f"Cost ${cost:.2f} + 25%", "confidence": "low"}

    return None


@bp.route("/api/rfq/<rid>/apply-recommendations", methods=["POST"])
@auth_required
def apply_recommendations(rid):
    """Apply all recommended prices to RFQ line items."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "Not found"}), 404

    applied = 0
    for item in r.get("line_items", []):
        rec = _compute_recommended_price(item)
        if rec:
            item["price_per_unit"] = rec["price"]
            item["recommended_price"] = rec["price"]
            item["recommended_reason"] = rec["reason"]
            applied += 1

    has_prices = sum(1 for i in r["line_items"] if i.get("price_per_unit"))
    r["status"] = "priced" if has_prices > 0 else r.get("status", "draft")
    save_rfqs(rfqs)
    return jsonify({"ok": True, "applied": applied, "total": len(r["line_items"])})


# ═══════════════════════════════════════════════════════════════════════════════
# E6: Buyer Intelligence Dashboard
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/buyer/<path:buyer_key>")
@auth_required
def buyer_profile(buyer_key):
    """Buyer profile page — aggregates all RFQs, PCs, win/loss, spend."""
    buyer_key_lower = buyer_key.lower().strip()

    # Gather all RFQs for this buyer
    rfqs = load_rfqs()
    buyer_rfqs = []
    for rid, r in rfqs.items():
        rname = (r.get("requestor_name") or "").lower()
        remail = (r.get("requestor_email") or "").lower()
        if buyer_key_lower in rname or buyer_key_lower in remail or buyer_key_lower == rname or buyer_key_lower == remail:
            buyer_rfqs.append({"id": rid, **r})

    # Gather all PCs for this buyer
    pcs = _load_price_checks()
    buyer_pcs = []
    for pid, pc in pcs.items():
        requestor = (pc.get("requestor") or "").lower()
        if buyer_key_lower in requestor:
            buyer_pcs.append({"id": pid, **pc})

    # Compute stats
    total_rfqs = len(buyer_rfqs)
    won = sum(1 for r in buyer_rfqs if r.get("status") == "won")
    lost = sum(1 for r in buyer_rfqs if r.get("status") == "lost")
    win_rate = round((won / (won + lost) * 100), 1) if (won + lost) > 0 else 0

    total_revenue = 0
    for r in buyer_rfqs:
        if r.get("status") == "won":
            for item in r.get("line_items", []):
                qty = item.get("qty", 0) or 0
                price = item.get("price_per_unit", 0) or 0
                total_revenue += qty * price

    # Get contact info from CRM
    contact = None
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.row_factory = _sqlite3.Row
            row = conn.execute(
                "SELECT * FROM contacts WHERE LOWER(buyer_email) LIKE ? OR LOWER(buyer_name) LIKE ? LIMIT 1",
                (f"%{buyer_key_lower}%", f"%{buyer_key_lower}%")
            ).fetchone()
            if row:
                contact = dict(row)
    except Exception:
        pass

    # Activity log
    activities = []
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.row_factory = _sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM activity_log WHERE LOWER(details) LIKE ? ORDER BY created_at DESC LIMIT 50",
                (f"%{buyer_key_lower}%",)
            ).fetchall()
            activities = [dict(r) for r in rows]
    except Exception:
        pass

    buyer_name = ""
    buyer_email = ""
    agency = ""
    for r in buyer_rfqs:
        buyer_name = buyer_name or r.get("requestor_name", "")
        buyer_email = buyer_email or r.get("requestor_email", "")
        agency = agency or r.get("department", "") or r.get("institution", "")

    return render_page("buyer_profile.html",
        active_page="CRM",
        buyer_key=buyer_key,
        buyer_name=buyer_name,
        buyer_email=buyer_email,
        agency=agency,
        contact=contact,
        buyer_rfqs=sorted(buyer_rfqs, key=lambda x: x.get("created_at", ""), reverse=True),
        buyer_pcs=sorted(buyer_pcs, key=lambda x: x.get("created_at", ""), reverse=True),
        stats={
            "total_rfqs": total_rfqs,
            "total_pcs": len(buyer_pcs),
            "won": won, "lost": lost, "win_rate": win_rate,
            "total_revenue": total_revenue,
        },
        activities=activities[:20],
    )


# ═══════════════════════════════════════════════════════════════════════════════
# E10: Pipeline Analytics Dashboard
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/analytics")
@auth_required
def analytics_dashboard():
    """Pipeline analytics — conversion funnel, revenue trends, win rates."""
    rfqs = load_rfqs()
    pcs = _load_price_checks()

    # Conversion funnel
    statuses = _defaultdict(int)
    for r in rfqs.values():
        statuses[r.get("status", "unknown")] += 1
    for pc in pcs.values():
        statuses[f"pc_{pc.get('status', 'unknown')}"] += 1

    funnel = {
        "imported": len(rfqs) + len(pcs),
        "parsed": sum(1 for r in rfqs.values() if r.get("status") not in ("dismissed",)),
        "priced": statuses.get("priced", 0) + statuses.get("pc_priced", 0),
        "sent": statuses.get("sent", 0) + statuses.get("pc_sent", 0),
        "won": statuses.get("won", 0) + statuses.get("pc_won", 0),
        "lost": statuses.get("lost", 0) + statuses.get("pc_lost", 0),
    }

    # Revenue by month
    monthly_revenue = _defaultdict(float)
    monthly_won = _defaultdict(int)
    for r in rfqs.values():
        if r.get("status") == "won":
            created = r.get("created_at", "")[:7]  # YYYY-MM
            for item in r.get("line_items", []):
                monthly_revenue[created] += (item.get("qty", 0) or 0) * (item.get("price_per_unit", 0) or 0)
            monthly_won[created] += 1

    # Win rate by institution
    inst_stats = _defaultdict(lambda: {"won": 0, "lost": 0, "revenue": 0})
    for r in rfqs.values():
        inst = r.get("department") or r.get("institution") or r.get("delivery_location", "")[:30] or "Unknown"
        if r.get("status") == "won":
            inst_stats[inst]["won"] += 1
            for item in r.get("line_items", []):
                inst_stats[inst]["revenue"] += (item.get("qty", 0) or 0) * (item.get("price_per_unit", 0) or 0)
        elif r.get("status") == "lost":
            inst_stats[inst]["lost"] += 1

    # Time to quote (created → sent)
    quote_times = []
    for r in rfqs.values():
        created = r.get("created_at", "")
        sent = r.get("sent_at", "")
        if created and sent:
            try:
                c = datetime.fromisoformat(created.replace("Z", "+00:00"))
                s = datetime.fromisoformat(sent.replace("Z", "+00:00"))
                hours = (s - c).total_seconds() / 3600
                if 0 < hours < 720:  # Max 30 days
                    quote_times.append(hours)
            except Exception:
                pass
    avg_quote_time = round(sum(quote_times) / len(quote_times), 1) if quote_times else 0

    # Growth metrics integration
    growth_kpis = {}
    growth_top = []
    try:
        from src.agents.growth_agent import get_growth_kpis, get_campaign_performance, get_agency_intelligence
        growth_kpis = get_growth_kpis()
        campaign_perf = get_campaign_performance()
        agency_intel = get_agency_intelligence()
        # Top 5 agencies by engagement
        growth_top = sorted(agency_intel, key=lambda a: a.get("responded", 0), reverse=True)[:5]
    except Exception:
        campaign_perf = {}

    return render_page("analytics.html",
        active_page="Pipeline",
        funnel=funnel,
        statuses=dict(statuses),
        monthly_revenue=dict(sorted(monthly_revenue.items())),
        monthly_won=dict(sorted(monthly_won.items())),
        inst_stats=dict(inst_stats),
        avg_quote_time=avg_quote_time,
        total_rfqs=len(rfqs),
        total_pcs=len(pcs),
        growth_kpis=growth_kpis,
        campaign_perf=campaign_perf,
        growth_top=growth_top,
    )


@bp.route("/api/analytics/data")
@auth_required
def analytics_data():
    """JSON API for analytics charts."""
    rfqs = load_rfqs()
    pcs = _load_price_checks()

    # Daily activity for last 30 days
    daily = _defaultdict(lambda: {"created": 0, "priced": 0, "sent": 0, "won": 0})
    cutoff = (datetime.now() - _timedelta(days=30)).isoformat()
    for r in rfqs.values():
        created = r.get("created_at", "")[:10]
        if created >= cutoff[:10]:
            daily[created]["created"] += 1

    return jsonify({
        "ok": True,
        "total_rfqs": len(rfqs),
        "total_pcs": len(pcs),
        "daily": dict(daily),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# E8: Email Send Integration from Detail Page
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/<rid>/send-quote", methods=["POST"])
@auth_required
def send_quote_email(rid):
    """Send the generated quote PDF via email directly from the detail page."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404

    data = request.get_json(silent=True) or {}
    to_email = data.get("to") or r.get("requestor_email", "")
    subject = data.get("subject") or f"Quote — Solicitation #{r.get('solicitation_number', rid)}"
    body = data.get("body") or _default_quote_email_body(r)
    pdf_path = data.get("pdf_path") or ""

    if not to_email:
        return jsonify({"ok": False, "error": "No recipient email"}), 400

    # Find the latest generated PDF
    if not pdf_path:
        sol = r.get("solicitation_number", "")
        output_dir = os.path.join(DATA_DIR, "output", sol)
        if os.path.isdir(output_dir):
            pdfs = sorted([f for f in os.listdir(output_dir) if f.endswith(".pdf")], reverse=True)
            if pdfs:
                pdf_path = os.path.join(output_dir, pdfs[0])

    # Send via Gmail
    try:
        email_cfg = CONFIG.get("email", {})
        gmail_user = email_cfg.get("email") or os.environ.get("GMAIL_ADDRESS", "")
        gmail_pass = email_cfg.get("email_password") or os.environ.get("GMAIL_PASSWORD", "")

        if not gmail_user or not gmail_pass:
            return jsonify({"ok": False, "error": "Gmail not configured"}), 400

        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders

        msg = MIMEMultipart()
        msg["From"] = gmail_user
        msg["To"] = to_email
        msg["Subject"] = subject
        msg["Reply-To"] = gmail_user
        msg.attach(MIMEText(body, "html"))

        # Attach PDF if available
        if pdf_path and os.path.exists(pdf_path):
            with open(pdf_path, "rb") as f:
                part = MIMEBase("application", "pdf")
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(pdf_path)}"')
                msg.attach(part)

        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(gmail_user, gmail_pass)
        server.send_message(msg)
        server.quit()

        # Record in DB
        r["status"] = "sent"
        r["sent_at"] = datetime.now().isoformat()
        r["sent_to"] = to_email
        rfqs[rid] = r
        save_rfqs(rfqs)

        # Log to email_log
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("""INSERT INTO email_log
                    (direction, sender, recipient, subject, body_preview, status, logged_at)
                    VALUES (?,?,?,?,?,?,?)""",
                    ("outbound", gmail_user, to_email, subject, body[:200], "sent",
                     datetime.now().isoformat()))
        except Exception:
            pass

        log.info("Quote sent for RFQ %s to %s", rid, to_email)
        return jsonify({"ok": True, "sent_to": to_email})

    except Exception as e:
        log.error("Failed to send quote email for %s: %s", rid, e)
        return jsonify({"ok": False, "error": str(e)}), 500


def _default_quote_email_body(r):
    sol = r.get("solicitation_number", "")
    items = r.get("line_items", [])
    total = sum((i.get("qty", 0) or 0) * (i.get("price_per_unit", 0) or 0) for i in items)
    return f"""<div style="font-family:Arial,sans-serif;color:#333">
<p>Dear {r.get('requestor_name', 'Procurement Officer')},</p>
<p>Please find attached our quote for <strong>Solicitation #{sol}</strong>.</p>
<p><strong>Summary:</strong> {len(items)} line items · Total: ${total:,.2f}</p>
<p>Please don't hesitate to reach out with any questions.</p>
<br>
<p>Best regards,<br>
<strong>Reytech Inc.</strong><br>
Michael Guadan<br>
949-229-1575 · sales@reytechinc.com<br>
SB/MB #2002605 · DVBE #2002605</p>
</div>"""


# ═══════════════════════════════════════════════════════════════════════════════
# E11: Bulk Operations on Queue
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/bulk/action", methods=["POST"])
@auth_required
def bulk_action():
    """Apply bulk action to multiple RFQs/PCs."""
    data = request.get_json(silent=True) or {}
    ids = data.get("ids", [])
    action = data.get("action", "")
    entity_type = data.get("type", "rfq")  # rfq or pc

    if not ids or not action:
        return jsonify({"ok": False, "error": "Missing ids or action"}), 400

    results = {"applied": 0, "errors": []}

    if entity_type == "rfq":
        rfqs = load_rfqs()
        for rid in ids:
            r = rfqs.get(rid)
            if not r:
                results["errors"].append(f"{rid}: not found")
                continue
            if action == "dismiss":
                r["status"] = "dismissed"
            elif action == "archive":
                r["status"] = "archived"
            elif action == "markup":
                pct = data.get("markup_pct", 20) / 100
                for item in r.get("line_items", []):
                    cost = item.get("supplier_cost") or item.get("scprs_last_price") or item.get("amazon_price") or 0
                    if cost > 0:
                        item["price_per_unit"] = round(cost * (1 + pct), 2)
                r["status"] = "priced"
            elif action == "lookup":
                # Queue for background lookup
                pass
            results["applied"] += 1
        save_rfqs(rfqs)

    elif entity_type == "pc":
        pcs = _load_price_checks()
        for pid in ids:
            pc = pcs.get(pid)
            if not pc:
                results["errors"].append(f"{pid}: not found")
                continue
            if action == "dismiss":
                pc["status"] = "dismissed"
            elif action == "archive":
                pc["status"] = "archived"
            elif action == "markup":
                pct = data.get("markup_pct", 20) / 100
                for item in pc.get("items", []):
                    cost = item.get("pricing", {}).get("your_cost") or 0
                    if cost > 0:
                        item["pricing"]["recommended_price"] = round(cost * (1 + pct), 2)
                pc["status"] = "priced"
            results["applied"] += 1
        _save_price_checks(pcs)

    return jsonify({"ok": True, **results})


# ═══════════════════════════════════════════════════════════════════════════════
# E12: Duplicate Detection
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/<rid>/check-duplicate")
@auth_required
def check_duplicate(rid):
    """Check if this RFQ is a duplicate or amendment of an existing one."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "Not found"}), 404

    sol = r.get("solicitation_number", "").strip()
    matches = []

    for oid, other in rfqs.items():
        if oid == rid:
            continue
        other_sol = other.get("solicitation_number", "").strip()
        if sol and other_sol == sol:
            # Same solicitation number — likely amendment
            diff = _diff_line_items(other.get("line_items", []), r.get("line_items", []))
            matches.append({
                "id": oid,
                "solicitation": other_sol,
                "status": other.get("status"),
                "created_at": other.get("created_at"),
                "item_count": len(other.get("line_items", [])),
                "diff": diff,
                "type": "amendment" if diff.get("changed") else "duplicate",
            })

    return jsonify({"ok": True, "matches": matches, "has_duplicates": len(matches) > 0})


def _diff_line_items(old_items, new_items):
    """Diff two sets of line items for amendment detection."""
    old_descs = {i.get("description", "").lower().strip() for i in old_items}
    new_descs = {i.get("description", "").lower().strip() for i in new_items}

    added = new_descs - old_descs
    removed = old_descs - new_descs
    unchanged = old_descs & new_descs

    return {
        "added": list(added)[:10],
        "removed": list(removed)[:10],
        "unchanged": len(unchanged),
        "changed": len(added) > 0 or len(removed) > 0,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# E15: Settings / Configuration Management
# ═══════════════════════════════════════════════════════════════════════════════

_DEFAULT_SETTINGS = {
    "pricing.default_markup_pct": 20,
    "pricing.scprs_undercut_pct": 2,
    "pricing.minimum_margin_pct": 5,
    "pricing.auto_recommend": True,
    "email.quote_template": "default",
    "email.auto_send_on_generate": False,
    "import.auto_process_emails": True,
    "import.auto_price_on_import": True,
    "notifications.new_rfq": True,
    "notifications.price_found": True,
    "notifications.deadline_warning_hours": 24,
    "company.name": "Reytech Inc.",
    "company.phone": "949-229-1575",
    "company.email": "sales@reytechinc.com",
    "company.sb_number": "2002605",
}


def _load_settings():
    """Load settings from SQLite, falling back to defaults."""
    settings = dict(_DEFAULT_SETTINGS)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT,
                updated_by TEXT DEFAULT 'system'
            )""")
            rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
            for row in rows:
                k, v = row[0], row[1]
                # Type coercion
                if k in settings:
                    orig = settings[k]
                    if isinstance(orig, bool):
                        settings[k] = v.lower() in ("true", "1", "yes")
                    elif isinstance(orig, int):
                        try: settings[k] = int(v)
                        except Exception: pass
                    elif isinstance(orig, float):
                        try: settings[k] = float(v)
                        except Exception: pass
                    else:
                        settings[k] = v
                else:
                    settings[k] = v
    except Exception:
        pass
    return settings


def _save_setting(key, value):
    """Save a single setting to SQLite."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY, value TEXT, updated_at TEXT, updated_by TEXT DEFAULT 'system'
            )""")
            conn.execute(
                "INSERT OR REPLACE INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
                (key, str(value), datetime.now().isoformat())
            )
    except Exception as e:
        log.warning("Failed to save setting %s: %s", key, e)


@bp.route("/settings")
@auth_required
def settings_page():
    """Configuration management UI."""
    settings = _load_settings()
    # Group settings by category
    groups = _defaultdict(dict)
    for k, v in settings.items():
        cat = k.split(".")[0]
        groups[cat][k] = v
    return render_page("settings.html", active_page="Agents", settings=settings, groups=dict(groups))


@bp.route("/api/settings", methods=["POST"])
@auth_required
def save_settings():
    """Save settings from the configuration page."""
    data = request.get_json(silent=True) or request.form.to_dict()
    saved = 0
    for key, value in data.items():
        if key.startswith("_"):
            continue
        _save_setting(key, value)
        saved += 1
    return jsonify({"ok": True, "saved": saved})


@bp.route("/api/settings/data")
@auth_required
def get_settings_data():
    """Return all settings as JSON."""
    return jsonify({"ok": True, "settings": _load_settings()})


@bp.route("/api/settings/get")
@auth_required
def get_single_setting():
    """Return a single setting value by key."""
    key = request.args.get("key", "")
    if not key:
        return jsonify({"ok": False, "error": "key required"})
    settings = _load_settings()
    value = settings.get(key, "")
    return jsonify({"ok": True, "key": key, "value": value})


# ═══════════════════════════════════════════════════════════════════════════════
# E13: API v1 Endpoints (Token Auth)
# ═══════════════════════════════════════════════════════════════════════════════

def _api_auth():
    """Validate API token or fall back to Basic Auth."""
    token = request.headers.get("X-API-Key", "")
    if token:
        expected = os.environ.get("REYTECH_API_KEY", "")
        if expected and token == expected:
            return True
    # Fall back to Basic Auth
    auth = request.authorization
    if auth:
        from src.api.dashboard import DASH_USER, DASH_PASS
        return auth.username == DASH_USER and auth.password == DASH_PASS
    return False


@bp.route("/api/v1/rfqs")
def api_v1_list_rfqs():
    """API v1: List RFQs with filtering."""
    if not _api_auth():
        return jsonify({"error": "Unauthorized"}), 401
    rfqs = load_rfqs()
    status = request.args.get("status")
    limit = int(request.args.get("limit", 50))

    items = []
    for rid, r in sorted(rfqs.items(), key=lambda x: x[1].get("created_at", ""), reverse=True):
        if status and r.get("status") != status:
            continue
        items.append({
            "id": rid,
            "solicitation_number": r.get("solicitation_number"),
            "status": r.get("status"),
            "requestor_name": r.get("requestor_name"),
            "requestor_email": r.get("requestor_email"),
            "due_date": r.get("due_date"),
            "item_count": len(r.get("line_items", [])),
            "created_at": r.get("created_at"),
            "total": sum((i.get("qty", 0) or 0) * (i.get("price_per_unit", 0) or 0) for i in r.get("line_items", [])),
        })
        if len(items) >= limit:
            break

    return jsonify({"ok": True, "rfqs": items, "total": len(items)})


@bp.route("/api/v1/rfqs/<rid>")
def api_v1_get_rfq(rid):
    """API v1: Get RFQ detail."""
    if not _api_auth():
        return jsonify({"error": "Unauthorized"}), 401
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"ok": True, "rfq": r})


@bp.route("/api/v1/stats")
def api_v1_stats():
    """API v1: Dashboard KPIs."""
    if not _api_auth():
        return jsonify({"error": "Unauthorized"}), 401
    rfqs = load_rfqs()
    pcs = _load_price_checks()

    return jsonify({
        "ok": True,
        "rfqs": {"total": len(rfqs), "active": sum(1 for r in rfqs.values() if r.get("status") not in ("dismissed", "archived", "deleted"))},
        "pcs": {"total": len(pcs), "active": sum(1 for p in pcs.values() if p.get("status") not in ("dismissed", "archived", "deleted"))},
        "won": sum(1 for r in rfqs.values() if r.get("status") == "won"),
        "lost": sum(1 for r in rfqs.values() if r.get("status") == "lost"),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Enhancement 2: Quick-Price Panel — Price items without leaving queue
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/quick-price/<entity_type>/<eid>")
@auth_required
def quick_price_data(entity_type, eid):
    """Return items for inline quick-price panel on home queue."""
    if entity_type == "pc":
        pcs = _load_price_checks()
        pc = pcs.get(eid)
        if not pc:
            return jsonify({"ok": False, "error": "Not found"}), 404
        items = []
        for i, item in enumerate(pc.get("items", [])):
            pricing = item.get("pricing", {})
            rec = _compute_recommended_price({
                "scprs_last_price": pricing.get("scprs_price"),
                "amazon_price": pricing.get("amazon_cost"),
                "supplier_cost": pricing.get("your_cost"),
            })
            hist = _find_won_history(item.get("description", ""), item.get("mfg_number", ""))
            items.append({
                "idx": i,
                "description": item.get("description", "")[:60],
                "mfg": item.get("mfg_number", ""),
                "qty": item.get("qty", 0),
                "uom": item.get("uom", "EA"),
                "cost": pricing.get("your_cost", 0),
                "scprs": pricing.get("scprs_price"),
                "amazon": pricing.get("amazon_cost"),
                "current_price": pricing.get("recommended_price", 0),
                "recommended": rec,
                "won_history": hist,
            })
        return jsonify({"ok": True, "items": items, "entity": "pc", "id": eid,
                        "institution": pc.get("institution", ""), "pc_number": pc.get("pc_number", "")})

    elif entity_type == "rfq":
        rfqs = load_rfqs()
        r = rfqs.get(eid)
        if not r:
            return jsonify({"ok": False, "error": "Not found"}), 404
        items = []
        for i, item in enumerate(r.get("line_items", [])):
            rec = _compute_recommended_price(item)
            hist = _find_won_history(item.get("description", ""), item.get("item_number", ""))
            items.append({
                "idx": i,
                "description": item.get("description", "")[:60],
                "part": item.get("item_number", ""),
                "qty": item.get("qty", 0),
                "uom": item.get("uom", "EA"),
                "cost": item.get("supplier_cost", 0),
                "scprs": item.get("scprs_last_price"),
                "amazon": item.get("amazon_price"),
                "current_price": item.get("price_per_unit", 0),
                "recommended": rec,
                "won_history": hist,
            })
        return jsonify({"ok": True, "items": items, "entity": "rfq", "id": eid,
                        "solicitation": r.get("solicitation_number", "")})

    return jsonify({"ok": False, "error": "Unknown entity type"}), 400


@bp.route("/api/quick-price/<entity_type>/<eid>/save", methods=["POST"])
@auth_required
def quick_price_save(entity_type, eid):
    """Save prices from the quick-price panel without navigating to detail."""
    data = request.get_json(silent=True) or {}
    prices = data.get("prices", {})  # {idx: price}

    if entity_type == "pc":
        pcs = _load_price_checks()
        pc = pcs.get(eid)
        if not pc:
            return jsonify({"ok": False, "error": "Not found"}), 404
        for idx_str, price in prices.items():
            idx = int(idx_str)
            if 0 <= idx < len(pc.get("items", [])):
                pc["items"][idx].setdefault("pricing", {})["recommended_price"] = float(price)
                pc["items"][idx]["pricing"]["unit_price"] = float(price)
        pc["status"] = "priced"
        pc["quick_priced_at"] = datetime.now().isoformat()
        _save_price_checks(pcs)
        return jsonify({"ok": True, "priced": len(prices)})

    elif entity_type == "rfq":
        rfqs = load_rfqs()
        r = rfqs.get(eid)
        if not r:
            return jsonify({"ok": False, "error": "Not found"}), 404
        for idx_str, price in prices.items():
            idx = int(idx_str)
            if 0 <= idx < len(r.get("line_items", [])):
                r["line_items"][idx]["price_per_unit"] = float(price)
        r["status"] = "priced"
        save_rfqs(rfqs)
        return jsonify({"ok": True, "priced": len(prices)})

    return jsonify({"ok": False, "error": "Unknown entity type"}), 400


# ═══════════════════════════════════════════════════════════════════════════════
# Enhancement 3: Won History Intelligence — Surface past wins on every item
# ═══════════════════════════════════════════════════════════════════════════════

def _find_won_history(description, item_number=""):
    """Find past winning prices for similar items."""
    results = []
    try:
        from src.knowledge.won_quotes_db import find_similar_wins
        wins = find_similar_wins(description, item_number)
        if wins:
            for w in wins[:3]:
                results.append({
                    "price": w.get("price", 0),
                    "institution": w.get("institution", ""),
                    "date": w.get("date", ""),
                    "qty": w.get("qty", 0),
                    "quote_number": w.get("quote_number", ""),
                })
    except Exception:
        pass

    # Also check recent PCs that were marked won
    try:
        pcs = _load_price_checks()
        desc_lower = (description or "").lower().strip()[:30]
        if desc_lower and len(desc_lower) > 5:
            for pid, pc in pcs.items():
                if pc.get("status") != "won":
                    continue
                for item in pc.get("items", []):
                    item_desc = (item.get("description", "") or "").lower().strip()
                    if desc_lower in item_desc or item_desc in desc_lower:
                        price = item.get("pricing", {}).get("recommended_price") or item.get("pricing", {}).get("unit_price", 0)
                        if price and price > 0:
                            results.append({
                                "price": price,
                                "institution": pc.get("institution", ""),
                                "date": pc.get("won_at", pc.get("created_at", ""))[:10],
                                "qty": item.get("qty", 0),
                                "quote_number": pc.get("reytech_quote_number", ""),
                                "source": "pc_won",
                            })
    except Exception:
        pass

    # Deduplicate and sort by recency
    seen = set()
    unique = []
    for r in results:
        key = f"{r.get('price', 0)}-{r.get('institution', '')}"
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return sorted(unique, key=lambda x: x.get("date", ""), reverse=True)[:5]


@bp.route("/api/won-history/search")
@auth_required
def won_history_search():
    """Search won history for a description or part number."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"ok": False, "error": "Query required"}), 400
    results = _find_won_history(q)
    return jsonify({"ok": True, "results": results, "query": q})


# ═══════════════════════════════════════════════════════════════════════════════
# Enhancement 4: Stale Quote Follow-Up Tracker
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/stale-quotes")
@auth_required
def stale_quotes():
    """Find quotes sent but with no response after configurable days."""
    days_threshold = int(request.args.get("days", 3))
    cutoff = (datetime.now() - _timedelta(days=days_threshold)).isoformat()

    stale = []

    # Check RFQs
    rfqs = load_rfqs()
    for rid, r in rfqs.items():
        if r.get("status") != "sent":
            continue
        sent_at = r.get("sent_at", "")
        if sent_at and sent_at < cutoff:
            try:
                days_since = (datetime.now() - datetime.fromisoformat(
                    sent_at.replace("Z", "+00:00").split("+")[0]
                )).days
            except Exception:
                days_since = days_threshold
            stale.append({
                "type": "rfq", "id": rid,
                "number": r.get("solicitation_number", ""),
                "institution": r.get("delivery_location", r.get("department", "")),
                "requestor": r.get("requestor_name", ""),
                "email": r.get("requestor_email", ""),
                "sent_at": sent_at,
                "days_since": days_since,
                "total": sum(
                    (i.get("qty", 0) or 0) * (i.get("price_per_unit", 0) or 0)
                    for i in r.get("line_items", [])
                ),
                "link": f"/rfq/{rid}",
            })

    # Check PCs
    pcs = _load_price_checks()
    for pid, pc in pcs.items():
        if pc.get("status") not in ("sent", "pending_award"):
            continue
        sent_at = pc.get("sent_at", pc.get("completed_at", ""))
        if sent_at and sent_at < cutoff:
            try:
                days_since = (datetime.now() - datetime.fromisoformat(
                    sent_at.replace("Z", "+00:00").split("+")[0]
                )).days
            except Exception:
                days_since = days_threshold

            # Extract email from requestor or parsed header
            requestor = pc.get("requestor", "")
            email = ""
            if "@" in requestor:
                email = requestor
            elif pc.get("contact_email"):
                email = pc["contact_email"]
            elif pc.get("parsed", {}).get("header", {}).get("buyer_email"):
                email = pc["parsed"]["header"]["buyer_email"]

            # Calculate PC total value
            pc_total = sum(
                (it.get("unit_price") or it.get("pricing", {}).get("recommended_price", 0) or 0)
                * it.get("qty", 1)
                for it in pc.get("items", [])
            )

            stale.append({
                "type": "pc", "id": pid,
                "number": pc.get("pc_number", ""),
                "institution": pc.get("institution", ""),
                "requestor": requestor,
                "email": email,
                "sent_at": sent_at,
                "days_since": days_since,
                "total": round(pc_total, 2),
                "link": f"/pricecheck/{pid}",
                "follow_up_count": pc.get("follow_up_count", 0),
                "last_follow_up": pc.get("last_follow_up_at", ""),
            })

    stale.sort(key=lambda x: x.get("days_since", 0), reverse=True)
    return jsonify({"ok": True, "stale": stale, "threshold_days": days_threshold,
                    "count": len(stale)})


@bp.route("/api/stale-quotes/<entity_type>/<eid>/follow-up", methods=["POST"])
@auth_required
def send_follow_up(entity_type, eid):
    """Send a follow-up email for a stale quote."""
    data = request.get_json(silent=True) or {}

    if entity_type == "rfq":
        rfqs = load_rfqs()
        r = rfqs.get(eid)
        if not r:
            return jsonify({"ok": False, "error": "Not found"}), 404
        to_email = data.get("to") or r.get("requestor_email", "")
        name = r.get("requestor_name", "")
        sol = r.get("solicitation_number", "")
    elif entity_type == "pc":
        pcs = _load_price_checks()
        pc = pcs.get(eid)
        if not pc:
            return jsonify({"ok": False, "error": "Not found"}), 404
        to_email = data.get("to") or ""
        name = pc.get("requestor", "")
        sol = pc.get("pc_number", "")
    else:
        return jsonify({"ok": False, "error": "Unknown type"}), 400

    if not to_email:
        return jsonify({"ok": False, "error": "No recipient email"}), 400

    subject = data.get("subject") or f"Follow Up — Quote for #{sol}"
    body = data.get("body") or f"""<div style="font-family:Arial,sans-serif;color:#333">
<p>Dear {name or 'Procurement Officer'},</p>
<p>I'm following up on our quote submitted for <strong>#{sol}</strong>.
Please let us know if you have any questions or need any revisions.</p>
<p>We remain ready to support your procurement needs.</p>
<br>
<p>Best regards,<br><strong>Reytech Inc.</strong><br>
Michael Guadan · 949-229-1575 · sales@reytechinc.com</p>
</div>"""

    # Send via Gmail
    try:
        email_cfg = CONFIG.get("email", {})
        gmail_user = email_cfg.get("email") or os.environ.get("GMAIL_ADDRESS", "")
        gmail_pass = email_cfg.get("email_password") or os.environ.get("GMAIL_PASSWORD", "")
        if not gmail_user or not gmail_pass:
            return jsonify({"ok": False, "error": "Gmail not configured"}), 400

        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        msg = MIMEMultipart()
        msg["From"] = gmail_user
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))

        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(gmail_user, gmail_pass)
        server.send_message(msg)
        server.quit()

        # Record follow-up
        if entity_type == "rfq":
            rfqs = load_rfqs()
            r = rfqs.get(eid, {})
            r.setdefault("follow_ups", []).append({
                "sent_at": datetime.now().isoformat(),
                "to": to_email,
            })
            r["last_follow_up"] = datetime.now().isoformat()
            save_rfqs(rfqs)
        elif entity_type == "pc":
            pcs = _load_price_checks()
            pc = pcs.get(eid, {})
            pc.setdefault("follow_ups", []).append({
                "sent_at": datetime.now().isoformat(),
                "to": to_email,
            })
            pc["last_follow_up"] = datetime.now().isoformat()
            _save_price_checks(pcs)

        log.info("Follow-up sent for %s/%s to %s", entity_type, eid, to_email)
        return jsonify({"ok": True, "sent_to": to_email})
    except Exception as e:
        log.error("Follow-up send failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/stale-quotes/bulk-follow-up", methods=["POST"])
@auth_required
def bulk_follow_up():
    """Send follow-up emails to all stale quotes that have email addresses."""
    data = request.get_json(silent=True) or {}
    days = int(data.get("days", 3))

    # Re-use existing stale quotes logic
    stale_list = _get_stale_list(days)
    sent_count = 0
    errors = []

    for s in stale_list:
        if not s.get("email"):
            continue
        try:
            # Trigger individual follow-up via internal call
            from flask import current_app
            with current_app.test_request_context(
                f"/api/stale-quotes/{s['type']}/{s['id']}/follow-up",
                method="POST",
                json={"to": s["email"]},
            ):
                # Just call the send function directly
                email_cfg = CONFIG.get("email", {})
                gmail_user = email_cfg.get("email") or os.environ.get("GMAIL_ADDRESS", "")
                gmail_pass = email_cfg.get("email_password") or os.environ.get("GMAIL_PASSWORD", "")
                if not gmail_user or not gmail_pass:
                    return jsonify({"ok": False, "error": "Gmail not configured"}), 400

                import smtplib
                from email.mime.multipart import MIMEMultipart
                from email.mime.text import MIMEText

                name = s.get("requestor", "Procurement Officer")
                sol = s.get("number", "?")
                body = f"""<div style="font-family:Arial,sans-serif;color:#333">
<p>Dear {name},</p>
<p>I'm following up on our quote submitted for <strong>#{sol}</strong>.
Please let us know if you have any questions or need revisions.</p>
<p>We remain ready to support your procurement needs.</p>
<br>
<p>Best regards,<br><strong>Reytech Inc.</strong><br>
Michael Guadan · 949-229-1575 · sales@reytechinc.com</p>
</div>"""

                msg = MIMEMultipart()
                msg["From"] = gmail_user
                msg["To"] = s["email"]
                msg["Subject"] = f"Follow Up — Quote #{sol}"
                msg.attach(MIMEText(body, "html"))

                server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
                server.login(gmail_user, gmail_pass)
                server.send_message(msg)
                server.quit()
                sent_count += 1
                log.info("Bulk follow-up sent to %s for #%s", s["email"], sol)
        except Exception as e:
            errors.append(f"{s.get('number', '?')}: {str(e)[:60]}")
            log.error("Bulk follow-up error for %s: %s", s.get("number"), e)

    return jsonify({
        "ok": True,
        "sent": sent_count,
        "errors": len(errors),
        "error_detail": errors[:5] if errors else [],
    })


def _get_stale_list(days):
    """Return stale quotes as a flat list for bulk operations."""
    from datetime import datetime, timedelta
    cutoff = datetime.now() - timedelta(days=days)
    result = []

    try:
        pcs = _load_price_checks()
        for pid, pc in pcs.items():
            if pc.get("status") not in ("sent",):
                continue
            sent_at = pc.get("sent_at") or pc.get("updated_at") or ""
            if not sent_at:
                continue
            try:
                sent_dt = datetime.fromisoformat(sent_at.replace("Z", "+00:00").split("+")[0])
            except Exception:
                continue
            if sent_dt > cutoff:
                continue
            result.append({
                "type": "pc", "id": pid,
                "number": pc.get("pc_number", ""),
                "email": pc.get("requestor_email", ""),
                "requestor": pc.get("requestor", ""),
                "days_since": (datetime.now() - sent_dt).days,
                "total": pc.get("total", 0),
            })
    except Exception:
        pass

    try:
        rfqs = load_rfqs()
        for rid, r in rfqs.items():
            if r.get("status") not in ("sent",):
                continue
            sent_at = r.get("sent_at") or r.get("updated_at") or ""
            if not sent_at:
                continue
            try:
                sent_dt = datetime.fromisoformat(sent_at.replace("Z", "+00:00").split("+")[0])
            except Exception:
                continue
            if sent_dt > cutoff:
                continue
            result.append({
                "type": "rfq", "id": rid,
                "number": r.get("solicitation_number", ""),
                "email": r.get("requestor_email", ""),
                "requestor": r.get("requestor_name", ""),
                "days_since": (datetime.now() - sent_dt).days,
                "total": r.get("total", 0),
            })
    except Exception:
        pass

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Enhancement 5: PC→RFQ Linkage — Track when a PC becomes a formal RFQ
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/pc/<pcid>/link-rfq", methods=["POST"])
@auth_required
def link_pc_to_rfq(pcid):
    """Link a Price Check to an RFQ (PC became formal solicitation)."""
    data = request.get_json(silent=True) or {}
    rfq_id = data.get("rfq_id", "")

    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"}), 404

    if rfq_id:
        rfqs = load_rfqs()
        if rfq_id not in rfqs:
            return jsonify({"ok": False, "error": "RFQ not found"}), 404
        pc["linked_rfq_id"] = rfq_id
        pc["linked_rfq_at"] = datetime.now().isoformat()
        # Also backlink on the RFQ
        rfqs[rfq_id]["linked_pc_id"] = pcid
        rfqs[rfq_id]["linked_pc_number"] = pc.get("pc_number", "")
        save_rfqs(rfqs)
    else:
        # Auto-match: find RFQ with same solicitation/PC number
        sol = pc.get("pc_number", "").strip()
        rfqs = load_rfqs()
        match = None
        for rid, r in rfqs.items():
            if r.get("solicitation_number", "").strip() == sol and sol:
                match = rid
                break
        if not match:
            # Try matching by institution + similar items
            inst = (pc.get("institution", "") or "").lower().strip()
            pc_items = {(i.get("description", "") or "").lower()[:30] for i in pc.get("items", []) if i.get("description")}
            for rid, r in rfqs.items():
                r_inst = (r.get("delivery_location", "") or r.get("department", "") or "").lower().strip()
                if inst and inst in r_inst:
                    r_items = {(i.get("description", "") or "").lower()[:30] for i in r.get("line_items", []) if i.get("description")}
                    overlap = pc_items & r_items
                    if len(overlap) >= max(1, len(pc_items) * 0.5):
                        match = rid
                        break
        if match:
            pc["linked_rfq_id"] = match
            pc["linked_rfq_at"] = datetime.now().isoformat()
            rfqs[match]["linked_pc_id"] = pcid
            rfqs[match]["linked_pc_number"] = pc.get("pc_number", "")
            save_rfqs(rfqs)
        else:
            _save_price_checks(pcs)
            return jsonify({"ok": True, "matched": False, "message": "No matching RFQ found"})

    _save_price_checks(pcs)
    return jsonify({"ok": True, "matched": True, "rfq_id": pc.get("linked_rfq_id")})


@bp.route("/api/pc/<pcid>/convert-to-rfq", methods=["POST"])
@auth_required
def convert_pc_to_rfq(pcid):
    """Convert a Price Check into a new RFQ, carrying over all item data and pricing."""
    import uuid as _uuid
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"}), 404

    rfq_id = str(_uuid.uuid4())[:8]
    now = datetime.now().isoformat()

    # Convert PC items to RFQ line items, preserving all pricing intelligence
    line_items = []
    for i, item in enumerate(pc.get("items", [])):
        pricing = item.get("pricing", {})
        line_items.append({
            "row_index": i,
            "description": item.get("description", ""),
            "item_number": item.get("mfg_number", "") or item.get("item_number", ""),
            "qty": item.get("qty", 0),
            "uom": item.get("uom", "EA"),
            "price_per_unit": pricing.get("recommended_price") or pricing.get("unit_price", 0),
            "supplier_cost": pricing.get("your_cost", 0),
            "scprs_last_price": pricing.get("scprs_price", 0),
            "amazon_price": pricing.get("amazon_cost", 0),
            "catalog_match": pricing.get("catalog_match"),
            "extension": (item.get("qty", 0) or 0) * (pricing.get("recommended_price") or pricing.get("unit_price", 0) or 0),
            "_from_pc": pcid,
        })

    rfq_data = {
        "id": rfq_id,
        "solicitation_number": pc.get("pc_number", ""),
        "status": "priced" if any(li.get("price_per_unit") for li in line_items) else "new",
        "source": "pc_conversion",
        "requestor_name": pc.get("requestor", ""),
        "requestor_email": "",
        "department": pc.get("institution", ""),
        "delivery_location": pc.get("ship_to", ""),
        "due_date": pc.get("due_date", ""),
        "line_items": line_items,
        "created_at": now,
        "linked_pc_id": pcid,
        "linked_pc_number": pc.get("pc_number", ""),
        "reytech_quote_number": pc.get("reytech_quote_number", ""),
    }

    rfqs = load_rfqs()
    rfqs[rfq_id] = rfq_data
    save_rfqs(rfqs)

    # Update PC with link
    pc["linked_rfq_id"] = rfq_id
    pc["linked_rfq_at"] = now
    pc["converted_to_rfq"] = True
    _save_price_checks(pcs)

    log.info("PC %s converted to RFQ %s with %d items", pcid, rfq_id, len(line_items))
    return jsonify({"ok": True, "rfq_id": rfq_id, "items": len(line_items)})


# ═══════════════════════════════════════════════════════════════════════════════
# Enhancement 5b: Follow-Up Dashboard Page
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/follow-ups")
@auth_required
def follow_ups_page():
    """Dashboard showing all stale quotes needing follow-up."""
    return render_page("follow_ups.html", active_page="Pipeline")


# ═══════════════════════════════════════════════════════════════════════════════
# Win/Loss Analysis Dashboard (PRD-v32 F9)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/analytics/win-loss")
@auth_required
def win_loss_page():
    """Win/loss analysis page with charts and trends."""
    return render_page("win_loss.html", active_page="Analytics")


@bp.route("/api/analytics/win-loss")
@auth_required
def api_win_loss_analysis():
    """Comprehensive win/loss data for charts and tables."""
    from collections import defaultdict

    quotes = [q for q in get_all_quotes() if not q.get("is_test")]
    pcs = _load_price_checks()

    # Aggregate by status
    by_status = defaultdict(lambda: {"count": 0, "total_value": 0})
    by_agency = defaultdict(lambda: {"won": 0, "lost": 0, "pending": 0, "won_value": 0, "lost_value": 0})
    by_month = defaultdict(lambda: {"won": 0, "lost": 0, "pending": 0, "sent": 0})
    by_category = defaultdict(lambda: {"won": 0, "lost": 0, "value": 0})
    recent_won = []
    recent_lost = []

    for q in quotes:
        status = q.get("status", "pending")
        total = q.get("total", 0) or 0
        agency = q.get("agency", "") or "Unknown"
        if agency == "DEFAULT":
            agency = "Unknown"
        created = q.get("created_at", "") or ""
        month_key = created[:7] if created else "unknown"

        by_status[status]["count"] += 1
        by_status[status]["total_value"] += total

        by_agency[agency][status if status in ("won", "lost") else "pending"] += 1
        if status == "won":
            by_agency[agency]["won_value"] += total
        elif status == "lost":
            by_agency[agency]["lost_value"] += total

        by_month[month_key][status if status in ("won", "lost", "sent") else "pending"] += 1

        # Categorize by items
        for item in (q.get("items_detail") or []):
            desc = (item.get("description", "") or "").lower()
            cat = "Other"
            if any(k in desc for k in ["medical", "surgical", "pharma", "rx", "iv "]):
                cat = "Medical"
            elif any(k in desc for k in ["office", "paper", "toner", "pen"]):
                cat = "Office"
            elif any(k in desc for k in ["janitorial", "clean", "trash", "sanitiz"]):
                cat = "Janitorial"
            elif any(k in desc for k in ["food", "beverage", "kitchen", "meal"]):
                cat = "Food Service"
            elif any(k in desc for k in ["cloth", "uniform", "garment", "boot"]):
                cat = "Clothing"
            by_category[cat][status if status in ("won", "lost") else "pending"] = \
                by_category[cat].get(status if status in ("won", "lost") else "pending", 0) + 1
            by_category[cat]["value"] += item.get("qty", 1) * (item.get("unit_price", 0) or 0)

        if status == "won":
            recent_won.append({
                "quote_number": q.get("quote_number", ""),
                "agency": agency,
                "institution": q.get("institution", "") or q.get("ship_to_name", ""),
                "total": total,
                "created_at": created,
                "po_number": q.get("po_number", ""),
            })
        elif status == "lost":
            recent_lost.append({
                "quote_number": q.get("quote_number", ""),
                "agency": agency,
                "institution": q.get("institution", "") or q.get("ship_to_name", ""),
                "total": total,
                "created_at": created,
                "close_reason": q.get("close_reason", ""),
            })

    # PC conversion rate
    total_pcs = len(pcs)
    pcs_with_quotes = sum(1 for p in pcs.values() if p.get("reytech_quote_number"))
    pcs_sent = sum(1 for p in pcs.values() if p.get("status") in ("sent", "won", "pending_award"))
    pcs_no_response = sum(1 for p in pcs.values()
                          if p.get("status") in ("not_responding", "no_response", "expired", "lost"))

    total_quotes = len(quotes)
    total_won = by_status.get("won", {}).get("count", 0)
    total_lost = by_status.get("lost", {}).get("count", 0)
    total_pending = total_quotes - total_won - total_lost
    win_rate = round(total_won / max(total_won + total_lost, 1) * 100, 1)
    won_revenue = by_status.get("won", {}).get("total_value", 0)
    lost_revenue = by_status.get("lost", {}).get("total_value", 0)
    avg_deal = round(won_revenue / max(total_won, 1), 2)

    # Conversion funnel
    funnel = [
        {"stage": "Price Checks Received", "count": total_pcs},
        {"stage": "Quotes Generated", "count": pcs_with_quotes},
        {"stage": "Quotes Sent", "count": pcs_sent + sum(1 for q in quotes if q.get("status") in ("sent",))},
        {"stage": "Won", "count": total_won},
    ]

    return jsonify({
        "ok": True,
        "summary": {
            "total_quotes": total_quotes,
            "won": total_won,
            "lost": total_lost,
            "pending": total_pending,
            "win_rate": win_rate,
            "won_revenue": round(won_revenue, 2),
            "lost_revenue": round(lost_revenue, 2),
            "avg_deal_size": avg_deal,
        },
        "by_agency": [
            {"agency": k, **v, "win_rate": round(v["won"] / max(v["won"] + v["lost"], 1) * 100, 1)}
            for k, v in sorted(by_agency.items(), key=lambda x: x[1]["won_value"], reverse=True)
        ],
        "by_month": [
            {"month": k, **v}
            for k, v in sorted(by_month.items())
        ],
        "by_category": [
            {"category": k, **v}
            for k, v in sorted(by_category.items(), key=lambda x: x[1]["value"], reverse=True)
        ],
        "funnel": funnel,
        "pc_stats": {
            "total": total_pcs,
            "quoted": pcs_with_quotes,
            "sent": pcs_sent,
            "no_response": pcs_no_response,
            "conversion_rate": round(pcs_with_quotes / max(total_pcs, 1) * 100, 1),
        },
        "recent_won": sorted(recent_won, key=lambda x: x.get("created_at", ""), reverse=True)[:10],
        "recent_lost": sorted(recent_lost, key=lambda x: x.get("created_at", ""), reverse=True)[:10],
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Supplier Performance Dashboard (PRD-v32 F4)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/suppliers/performance")
@auth_required
def supplier_performance_page():
    """Supplier performance rankings and analytics."""
    return render_page("supplier_performance.html", active_page="Vendors")


@bp.route("/api/suppliers/performance")
@auth_required
def api_supplier_performance():
    """Aggregate supplier performance data from orders, catalog, and vendor records."""
    from collections import defaultdict

    suppliers = defaultdict(lambda: {
        "name": "",
        "order_count": 0,
        "line_items": 0,
        "total_cost": 0,
        "total_revenue": 0,
        "on_time": 0,
        "late": 0,
        "pending": 0,
        "avg_lead_days": 0,
        "lead_days_list": [],
        "categories": set(),
        "price_score": 50,
        "reliability_score": 50,
        "speed_score": 50,
        "overall_score": 40,
    })

    # From orders
    orders = _load_orders()
    for oid, order in orders.items():
        if order.get("is_test"):
            continue
        for li in order.get("line_items", []):
            supplier = li.get("supplier", "").strip()
            if not supplier:
                continue
            s = suppliers[supplier]
            s["name"] = supplier
            s["line_items"] += 1
            s["total_cost"] += (li.get("cost") or 0) * (li.get("qty") or 1)
            s["total_revenue"] += (li.get("unit_price") or 0) * (li.get("qty") or 1)

            status = li.get("sourcing_status", "pending")
            if status in ("delivered",):
                s["on_time"] += 1
                # Calculate lead time if dates exist
                if li.get("ship_date") and li.get("delivery_date"):
                    try:
                        ship = datetime.fromisoformat(li["ship_date"][:19])
                        deliv = datetime.fromisoformat(li["delivery_date"][:19])
                        s["lead_days_list"].append((deliv - ship).days)
                    except Exception:
                        pass
            elif status in ("shipped",):
                s["pending"] += 1
            elif status == "pending":
                s["pending"] += 1

    # From vendor records
    vendors_path = os.path.join(DATA_DIR, "vendors.json")
    try:
        with open(vendors_path) as f:
            vendors = json.load(f)
        for v in vendors:
            name = v.get("name") or v.get("company", "")
            if not name:
                continue
            s = suppliers[name]
            s["name"] = name
            s["price_score"] = v.get("price_score", 50)
            s["reliability_score"] = v.get("reliability_score", 50)
            s["speed_score"] = v.get("speed_score", 50)
            s["overall_score"] = v.get("overall_score", 40)
            for cat in (v.get("categories_served") or []):
                s["categories"].add(cat)
    except Exception:
        pass

    # From catalog supplier pricing
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT supplier, COUNT(*) as products, AVG(cost) as avg_cost
                FROM product_catalog WHERE supplier IS NOT NULL AND supplier != ''
                GROUP BY supplier ORDER BY products DESC LIMIT 50
            """).fetchall()
            for r in rows:
                sup = r["supplier"]
                s = suppliers[sup]
                s["name"] = sup
                s["catalog_products"] = r["products"]
                s["avg_catalog_cost"] = round(r["avg_cost"] or 0, 2)
    except Exception:
        pass

    # Build results
    results = []
    for name, s in suppliers.items():
        if not s["name"]:
            continue
        avg_lead = round(sum(s["lead_days_list"]) / len(s["lead_days_list"]), 1) if s["lead_days_list"] else 0
        total_fulfilled = s["on_time"] + s["late"]
        fulfillment_rate = round(s["on_time"] / max(total_fulfilled, 1) * 100, 1)

        # Calculate composite score
        composite = round((s["price_score"] * 0.3 + s["reliability_score"] * 0.3 +
                          s["speed_score"] * 0.2 + min(s["line_items"] * 5, 100) * 0.2), 1)

        results.append({
            "name": s["name"],
            "order_count": s["order_count"],
            "line_items": s["line_items"],
            "total_cost": round(s["total_cost"], 2),
            "total_revenue": round(s["total_revenue"], 2),
            "margin": round(s["total_revenue"] - s["total_cost"], 2) if s["total_revenue"] else 0,
            "on_time": s["on_time"],
            "late": s["late"],
            "pending": s["pending"],
            "avg_lead_days": avg_lead,
            "fulfillment_rate": fulfillment_rate,
            "categories": sorted(list(s["categories"]))[:5],
            "price_score": s["price_score"],
            "reliability_score": s["reliability_score"],
            "speed_score": s["speed_score"],
            "overall_score": s["overall_score"],
            "composite_score": composite,
            "catalog_products": s.get("catalog_products", 0),
        })

    # Sort by composite score (highest first)
    results.sort(key=lambda x: x["composite_score"], reverse=True)

    # Summary stats
    total_suppliers = len(results)
    active_suppliers = sum(1 for r in results if r["line_items"] > 0)
    avg_score = round(sum(r["composite_score"] for r in results) / max(total_suppliers, 1), 1)

    return jsonify({
        "ok": True,
        "suppliers": results[:100],
        "summary": {
            "total": total_suppliers,
            "active": active_suppliers,
            "avg_score": avg_score,
            "top_by_revenue": sorted(results, key=lambda x: x["total_revenue"], reverse=True)[:5],
        },
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Webhook Integration Hub (PRD-v32 F10)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/webhooks/config")
@auth_required
def api_webhooks_config():
    """Get webhook configuration."""
    try:
        from src.core.webhooks import get_config
        return jsonify({"ok": True, **get_config()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/webhooks/save", methods=["POST"])
@auth_required
def api_webhooks_save():
    """Save or update a webhook endpoint."""
    try:
        from src.core.webhooks import save_webhook
        data = request.get_json(silent=True) or {}
        name = data.get("name", "").strip()
        url = data.get("url", "").strip()
        events = data.get("events", [])
        fmt = data.get("format", "json")
        if not name or not url:
            return jsonify({"ok": False, "error": "Name and URL required"})
        result = save_webhook(name, url, events, fmt)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/webhooks/delete", methods=["POST"])
@auth_required
def api_webhooks_delete():
    """Delete a webhook endpoint."""
    try:
        from src.core.webhooks import delete_webhook
        data = request.get_json(silent=True) or {}
        name = data.get("name", "")
        if not name:
            return jsonify({"ok": False, "error": "Name required"})
        return jsonify(delete_webhook(name))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/webhooks/test", methods=["POST"])
@auth_required
def api_webhooks_test():
    """Send a test webhook to verify connectivity."""
    try:
        from src.core.webhooks import fire_event
        data = request.get_json(silent=True) or {}
        fire_event("new_rfq", {
            "rfq_id": "TEST-001",
            "agency": "Test Agency",
            "items": 3,
            "message": "This is a test webhook from Reytech RFQ",
        })
        return jsonify({"ok": True, "message": "Test webhook fired"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/rfq/<rid>/retry-auto-price", methods=["POST", "GET"])
@auth_required
def rfq_retry_auto_price(rid):
    """Inline auto-price for an RFQ — runs synchronously, returns results."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    items = r.get("line_items", [])
    if not items:
        return jsonify({"ok": False, "error": "RFQ has no line items"})

    found = 0
    errors = []

    # 1. SCPRS
    try:
        from src.agents.scprs_lookup import bulk_lookup
        r["line_items"] = bulk_lookup(items)
        items = r["line_items"]
        scprs = sum(1 for i in items if i.get("scprs_last_price"))
        found += scprs
    except Exception as e:
        errors.append(f"scprs: {e}")

    # 2. Catalog
    try:
        from src.agents.product_catalog import match_item, init_catalog_db
        init_catalog_db()
        for item in items:
            matches = match_item(item.get("description", ""), item.get("item_number", ""), top_n=1)
            if matches and matches[0].get("match_confidence", 0) >= 0.50:
                best = matches[0]
                item["catalog_match"] = best
                if best.get("recommended_price") and not item.get("recommended_price"):
                    item["recommended_price"] = best["recommended_price"]
                    found += 1
    except Exception as e:
        errors.append(f"catalog: {e}")

    # 3. Compute recommended prices
    try:
        for item in items:
            cost = item.get("scprs_last_price") or item.get("amazon_price") or 0
            if cost and not item.get("recommended_price"):
                item["recommended_price"] = round(float(cost) * 1.25, 2)
                found += 1
    except Exception as e:
        errors.append(f"pricing: {e}")

    # Save
    try:
        rfqs_fresh = load_rfqs()
        rfqs_fresh[rid] = r
        save_rfqs(rfqs_fresh)
    except Exception as e:
        errors.append(f"save: {e}")

    return jsonify({
        "ok": True,
        "items": len(items),
        "priced": found,
        "errors": errors,
        "message": f"Priced {found}/{len(items)} RFQ items" + (f" (errors: {errors})" if errors else ""),
    })


@bp.route("/api/rfq/<rid>/relink-pc", methods=["POST", "GET"])
@auth_required
def rfq_relink_pc(rid):
    """Re-run PC linkage for an existing RFQ. 
    Clears stale link + zero prices first, then re-matches and ports."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    # Clear previous link so _link_rfq_to_pc re-runs matching
    r.pop("linked_pc_id", None)
    r.pop("linked_pc_number", None)
    r.pop("linked_pc_match_reason", None)
    r.pop("pc_diff", None)
    
    # Clear zero/empty prices on items so _port overwrites them
    for it in r.get("line_items", []):
        for field in ("supplier_cost", "price_per_unit", "scprs_last_price", "amazon_price"):
            val = it.get(field)
            if val is not None:
                try:
                    if float(val) <= 0:
                        it.pop(field, None)
                except (ValueError, TypeError):
                    if not val or val.strip() in ("", "0", "0.00", "—"):
                        it.pop(field, None)
        it.pop("_from_pc", None)

    # Run the existing linkage function
    try:
        from src.api.dashboard import _link_rfq_to_pc, _load_price_checks, _save_price_checks
        
        # First: ensure matching PC actually has prices
        # If PC items have empty pricing, price them NOW before porting
        pcs = _load_price_checks()
        for pid, pc in pcs.items():
            pc_items = pc.get("items", [])
            has_prices = any(
                (it.get("pricing") or {}).get("recommended_price") or
                (it.get("pricing") or {}).get("unit_cost") or
                (it.get("pricing") or {}).get("scprs_price")
                for it in pc_items if isinstance(it, dict)
            )
            if pc_items and not has_prices:
                # Price this PC inline
                try:
                    from src.agents.product_catalog import match_item, init_catalog_db
                    init_catalog_db()
                    priced = 0
                    for item in pc_items:
                        desc = item.get("description", "")
                        pn = str(item.get("item_number", "") or item.get("mfg_number", "") or "")
                        if not desc and not pn: continue
                        matches = match_item(desc, pn, top_n=1)
                        if matches and matches[0].get("match_confidence", 0) >= 0.40:
                            best = matches[0]
                            if not item.get("pricing"): item["pricing"] = {}
                            cat_price = best.get("recommended_price") or best.get("sell_price", 0)
                            cat_cost = best.get("cost", 0)
                            if cat_price > 0:
                                item["pricing"]["catalog_match"] = best.get("name", "")[:60]
                                item["pricing"]["recommended_price"] = round(cat_price, 2)
                                if cat_cost > 0:
                                    item["pricing"]["unit_cost"] = cat_cost
                                priced += 1
                    if priced == 0:
                        # Try SCPRS
                        try:
                            from src.knowledge.pricing_oracle import find_similar_items
                            for item in pc_items:
                                if (item.get("pricing") or {}).get("recommended_price"): continue
                                desc = item.get("description", "")
                                pn = str(item.get("item_number", "") or "")
                                smatches = find_similar_items(item_number=pn, description=desc)
                                if smatches:
                                    best = smatches[0]
                                    quote = best.get("quote", best)
                                    price = quote.get("unit_price", 0)
                                    if price and price > 0:
                                        if not item.get("pricing"): item["pricing"] = {}
                                        item["pricing"]["scprs_price"] = price
                                        item["pricing"]["unit_cost"] = price
                                        item["pricing"]["recommended_price"] = round(price * 1.25, 2)
                                        priced += 1
                        except Exception:
                            pass
                    if priced > 0:
                        pc["items"] = pc_items
                        pcs[pid] = pc
                        _save_price_checks(pcs)
                except Exception as pe:
                    trace.append(f"Inline pricing error: {pe}")

        trace = []
        linked = _link_rfq_to_pc(r, trace)
        
        if linked:
            # Check if prices actually ported (the _port function may have skipped)
            items = r.get("line_items", [])
            priced = sum(1 for it in items if it.get("supplier_cost") or it.get("price_per_unit") or it.get("scprs_last_price"))
            
            # FALLBACK: if link matched but 0 prices ported, manually copy
            if priced == 0:
                pc_id = r.get("linked_pc_id", "")
                pcs2 = _load_price_checks()
                pc2 = pcs2.get(pc_id, {})
                pc_items = pc2.get("items", [])
                for ri in items:
                    rd = (ri.get("description") or "").lower()[:40]
                    for pci in pc_items:
                        pd = (pci.get("description") or "").lower()[:40]
                        if rd == pd or (len(rd) > 8 and rd in pd) or (len(pd) > 8 and pd in rd):
                            p = pci.get("pricing") or {}
                            cost = p.get("unit_cost") or p.get("scprs_price") or p.get("catalog_cost") or p.get("web_price") or p.get("amazon_price") or 0
                            bid = p.get("recommended_price") or 0
                            if cost:
                                ri["supplier_cost"] = float(cost)
                                priced += 1
                            if bid:
                                ri["price_per_unit"] = float(bid)
                            elif cost:
                                ri["price_per_unit"] = round(float(cost) * 1.25, 2)
                            ri["_from_pc"] = pc2.get("pc_number", "")
                            break
                trace.append(f"Fallback direct copy: {priced} items priced")
            
            # Save
            rfqs[rid] = r
            save_rfqs(rfqs)
            diff = r.get("pc_diff", {})
            
            return jsonify({
                "ok": True,
                "linked_pc": r.get("linked_pc_number", ""),
                "linked_pc_id": r.get("linked_pc_id", ""),
                "match_reason": r.get("linked_pc_match_reason", ""),
                "items_priced": priced,
                "items_total": len(items),
                "diff": diff,
                "trace": trace,
                "message": f"Linked to PC #{r.get('linked_pc_number','')} — {priced}/{len(items)} items priced",
            })
        else:
            # No match — show why
            from src.api.dashboard import _load_price_checks
            pcs = _load_price_checks()
            pc_summary = []
            for pid, pc in list(pcs.items())[:10]:
                pc_summary.append({
                    "id": pid[:20], "pc_number": pc.get("pc_number", ""),
                    "institution": (pc.get("institution", "") or "")[:30],
                    "items": len(pc.get("items", [])),
                    "status": pc.get("status", ""),
                })
            
            return jsonify({
                "ok": False,
                "error": "No matching PC found",
                "rfq_sol": r.get("solicitation_number", ""),
                "rfq_sender": r.get("email_sender", ""),
                "rfq_items": len(r.get("line_items", [])),
                "rfq_item_descs": [(it.get("description", "") or "")[:50] for it in r.get("line_items", [])[:5]],
                "available_pcs": pc_summary,
                "trace": trace,
            })
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "traceback": traceback.format_exc()[:500]})


@bp.route("/api/rfqs/relink-all", methods=["POST", "GET"])
@auth_required
def rfqs_relink_all():
    """Re-run PC linkage for ALL unlinked RFQs. Fixes historical gaps."""
    rfqs = load_rfqs()
    from src.api.dashboard import _link_rfq_to_pc
    
    linked = 0
    already = 0
    failed = 0
    details = []
    
    for rid, r in rfqs.items():
        if r.get("linked_pc_id"):
            already += 1
            continue
        if r.get("status") in ("dismissed", "cancelled"):
            continue
        if not r.get("line_items"):
            continue
        
        trace = []
        try:
            ok = _link_rfq_to_pc(r, trace)
            if ok:
                linked += 1
                details.append(f"✅ {rid[:20]} → PC #{r.get('linked_pc_number','')}")
            else:
                failed += 1
        except Exception as e:
            failed += 1
            details.append(f"❌ {rid[:20]}: {e}")
    
    if linked > 0:
        save_rfqs(rfqs)
    
    return jsonify({
        "ok": True,
        "linked": linked,
        "already_linked": already,
        "no_match": failed,
        "total_rfqs": len(rfqs),
        "details": details[:20],
        "message": f"Linked {linked} RFQs to PCs ({already} already linked, {failed} no match)",
    })


@bp.route("/api/rfq/<rid>/debug-link")
@auth_required
def rfq_debug_link(rid):
    """Show EXACTLY what PC data exists and why porting fails."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    from src.api.dashboard import _load_price_checks
    pcs = _load_price_checks()
    
    # RFQ items
    rfq_items = []
    for it in r.get("line_items", []):
        rfq_items.append({
            "desc": (it.get("description") or "")[:60],
            "supplier_cost": it.get("supplier_cost"),
            "price_per_unit": it.get("price_per_unit"),
            "scprs": it.get("scprs_last_price"),
            "_from_pc": it.get("_from_pc"),
        })

    # Find linked or matching PC
    linked_pc_id = r.get("linked_pc_id")
    sol = r.get("solicitation_number", "")
    
    pc_match = None
    match_reason = "none"
    
    if linked_pc_id and linked_pc_id in pcs:
        pc_match = pcs[linked_pc_id]
        match_reason = "already_linked"
    else:
        # Try to find by sol#
        for pid, pc in pcs.items():
            pc_num = (pc.get("pc_number") or "").strip()
            if sol and pc_num == sol:
                pc_match = pc
                match_reason = f"sol_match:{pc_num}"
                break
    
    if not pc_match:
        # Show all PCs for manual review
        all_pcs = []
        for pid, pc in list(pcs.items())[:20]:
            all_pcs.append({
                "id": pid[:25],
                "pc_number": pc.get("pc_number", ""),
                "institution": (pc.get("institution") or "")[:30],
                "items": len(pc.get("items", [])),
                "status": pc.get("status", ""),
                "has_pricing": any(
                    it.get("pricing", {}).get("recommended_price") or it.get("pricing", {}).get("unit_cost")
                    for it in pc.get("items", []) if isinstance(it, dict)
                ),
            })
        return jsonify({
            "ok": False,
            "error": "No PC match found",
            "rfq_sol": sol,
            "rfq_items": rfq_items,
            "available_pcs": all_pcs,
        })

    # Show PC items with their FULL pricing dict
    pc_items = []
    for it in pc_match.get("items", []):
        pc_items.append({
            "desc": (it.get("description") or "")[:60],
            "mfg": it.get("mfg_number") or it.get("item_number", ""),
            "pricing": it.get("pricing", {}),
            "qty": it.get("qty"),
        })

    # Show what would match
    matches = []
    for ri in r.get("line_items", []):
        rd = (ri.get("description") or "").lower()[:40]
        matched_pc_item = None
        for pci in pc_match.get("items", []):
            pd = (pci.get("description") or "").lower()[:40]
            if rd == pd or (len(rd) > 10 and rd in pd) or (len(pd) > 10 and pd in rd):
                matched_pc_item = pci
                break
        matches.append({
            "rfq_desc": rd,
            "matched": bool(matched_pc_item),
            "pc_desc": (matched_pc_item.get("description") or "")[:40] if matched_pc_item else None,
            "pc_pricing": matched_pc_item.get("pricing", {}) if matched_pc_item else None,
        })

    return jsonify({
        "ok": True,
        "rfq_sol": sol,
        "match_reason": match_reason,
        "pc_number": pc_match.get("pc_number", ""),
        "rfq_items": rfq_items,
        "pc_items": pc_items,
        "item_matches": matches,
    })


@bp.route("/api/pcs/list")
@auth_required
def api_pcs_list():
    """List all PCs with basic info for import picker."""
    from src.api.dashboard import _load_price_checks
    pcs = _load_price_checks()
    result = []
    for pid, pc in pcs.items():
        items = pc.get("items", [])
        priced = sum(1 for it in items if isinstance(it, dict) and (
            it.get("pricing", {}).get("recommended_price") or
            it.get("pricing", {}).get("unit_cost") or
            it.get("pricing", {}).get("scprs_price") or
            it.get("pricing", {}).get("amazon_price")
        ))
        result.append({
            "id": pid,
            "pc_number": pc.get("pc_number", "unknown"),
            "institution": (pc.get("institution") or "")[:40],
            "status": pc.get("status", ""),
            "items": len(items),
            "priced": priced,
            "created_at": pc.get("created_at", ""),
        })
    # Sort: most recently created first
    result.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return jsonify({"ok": True, "pcs": result})


@bp.route("/api/rfq/<rid>/import-from-pc", methods=["POST"])
@auth_required
def api_rfq_import_from_pc(rid):
    """Import items + prices from a PC directly into an RFQ. No matching logic.
    Just copies PC items as RFQ line_items with all pricing fields."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    data = request.get_json(silent=True) or {}
    pc_id = data.get("pc_id", "")
    if not pc_id:
        return jsonify({"ok": False, "error": "No pc_id provided"})

    from src.api.dashboard import _load_price_checks
    pcs = _load_price_checks()
    pc = pcs.get(pc_id)
    if not pc:
        return jsonify({"ok": False, "error": f"PC {pc_id} not found"})

    pc_items = pc.get("items", [])
    if not pc_items:
        return jsonify({"ok": False, "error": "PC has no items"})

    # Build RFQ line items from PC items — copy EVERYTHING
    imported = []
    for it in pc_items:
        if not isinstance(it, dict):
            continue
        pricing = it.get("pricing", {})

        # Get best cost from any source
        cost = (pricing.get("unit_cost") or pricing.get("your_cost") or
                pricing.get("catalog_cost") or pricing.get("scprs_price") or
                pricing.get("amazon_price") or pricing.get("web_price") or 0)
        try:
            cost = round(float(cost), 2) if cost else 0
        except:
            cost = 0

        # Get bid price
        bid = pricing.get("recommended_price") or pricing.get("unit_price") or 0
        try:
            bid = round(float(bid), 2) if bid else 0
        except:
            bid = 0
        if not bid and cost > 0:
            bid = round(cost * 1.25, 2)

        rfq_item = {
            "description": it.get("description", ""),
            "qty": it.get("qty", 1),
            "uom": it.get("uom", "EA"),
            "item_number": it.get("mfg_number") or it.get("item_number", ""),
            "supplier_cost": cost,
            "price_per_unit": bid,
            "scprs_last_price": pricing.get("scprs_price"),
            "amazon_price": pricing.get("amazon_price") or pricing.get("amazon_cost"),
            "item_link": it.get("item_link") or pricing.get("web_url") or pricing.get("catalog_url") or pricing.get("amazon_url", ""),
            "item_supplier": it.get("item_supplier") or pricing.get("catalog_best_supplier") or pricing.get("web_source", ""),
            "_from_pc": pc.get("pc_number", pc_id),
        }
        imported.append(rfq_item)

    # Replace RFQ line items with PC items
    r["line_items"] = imported
    r["linked_pc_id"] = pc_id
    r["linked_pc_number"] = pc.get("pc_number", "")
    r["linked_pc_match_reason"] = "manual_import"

    save_rfqs(rfqs)

    return jsonify({
        "ok": True,
        "items_imported": len(imported),
        "pc_number": pc.get("pc_number", ""),
        "priced": sum(1 for it in imported if it.get("supplier_cost") or it.get("price_per_unit")),
    })


@bp.route("/api/rfq/<rid>/upload-pc", methods=["POST"])
@auth_required
def api_rfq_upload_pc(rid):
    """Upload a filled PC PDF → parse → verify/save to catalog → populate RFQ.
    
    Handles edge case: PC was filled outside app or data was lost.
    The filled PDF has all the pricing data — extract it, save to catalog,
    and populate the RFQ line items.
    """
    import os, shutil
    from src.core.paths import DATA_DIR

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    f = request.files.get("file")
    if not f or not f.filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "error": "Upload a PDF file"})

    # Save uploaded file
    upload_dir = os.path.join(DATA_DIR, "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    pdf_path = os.path.join(upload_dir, f"pc_import_{rid}_{f.filename}")
    f.save(pdf_path)

    # Parse the 704
    try:
        from src.forms.price_check import parse_ams704
        parsed = parse_ams704(pdf_path)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Parse failed: {e}"})

    if parsed.get("error"):
        return jsonify({"ok": False, "error": f"Parse error: {parsed['error']}"})

    items = parsed.get("line_items", [])
    prices = parsed.get("existing_prices", {})
    header = parsed.get("header", {})
    pc_number = header.get("price_check_number", "")
    institution = header.get("institution", "")

    if not items:
        return jsonify({"ok": False, "error": "No items found in PDF"})

    # Build RFQ line items from parsed PC + merge existing prices
    catalog_added = 0
    catalog_updated = 0
    imported = []

    for it in items:
        row_idx = it.get("row_index", 0)
        price = prices.get(row_idx) or prices.get(str(row_idx)) or 0
        try:
            price = float(price) if price else 0
        except:
            price = 0

        desc = it.get("description", "")
        qty = it.get("qty", 1) or 1
        uom = it.get("uom", "EA") or "EA"
        mfg = it.get("mfg_number", "") or it.get("item_number", "")

        rfq_item = {
            "description": desc,
            "qty": qty,
            "uom": uom,
            "item_number": mfg,
            "supplier_cost": price if price > 0 else None,
            "price_per_unit": price if price > 0 else None,
            "_from_pc": pc_number or "uploaded",
        }
        imported.append(rfq_item)

        # Write to catalog
        if desc and price > 0:
            try:
                from src.agents.product_catalog import (
                    match_item, add_to_catalog, update_product_pricing,
                    add_supplier_price, init_catalog_db
                )
                init_catalog_db()
                matches = match_item(desc, mfg, top_n=1)
                if matches and matches[0].get("match_confidence", 0) >= 0.55:
                    # Update existing
                    pid = matches[0]["id"]
                    update_product_pricing(pid,
                        sell_price=price, cost=price,
                        last_sold_price=price,
                        times_quoted=(matches[0].get("times_quoted") or 0) + 1,
                    )
                    catalog_updated += 1
                else:
                    # Add new
                    add_to_catalog(
                        description=desc, part_number=mfg, mfg_number=mfg,
                        cost=price, sell_price=price, uom=uom,
                        source=f"pc_upload_{pc_number}",
                    )
                    catalog_added += 1
            except Exception as e:
                log.debug("Catalog write from PC upload: %s", e)

    # Replace RFQ line items
    r["line_items"] = imported
    r["linked_pc_number"] = pc_number
    r["linked_pc_match_reason"] = "pdf_upload"
    r["uploaded_pc_pdf"] = pdf_path
    save_rfqs(rfqs)

    return jsonify({
        "ok": True,
        "items_imported": len(imported),
        "pc_number": pc_number,
        "institution": institution,
        "catalog_added": catalog_added,
        "catalog_updated": catalog_updated,
        "priced": sum(1 for it in imported if it.get("price_per_unit")),
    })


@bp.route("/api/quote-audit")
@auth_required
def api_quote_audit():
    """Show all quote numbers, counter state, and gaps."""
    from src.core.db import get_db
    result = {"quotes": [], "counter": None, "pcs_with_quotes": [], "gaps": [], "duplicates": []}
    
    try:
        with get_db() as conn:
            # All quotes
            rows = conn.execute("SELECT quote_number, status, total, created_at, agency FROM quotes ORDER BY created_at").fetchall()
            for r in rows:
                result["quotes"].append(dict(r))
            
            # Counter
            try:
                row = conn.execute("SELECT value FROM app_settings WHERE key='quote_counter'").fetchone()
                result["counter"] = int(row[0]) if row else "NOT SET (default=16)"
            except:
                result["counter"] = "app_settings table missing"
            
            # PCs with quote numbers
            try:
                pcs = conn.execute("SELECT id, quote_number, pc_number, status FROM price_checks WHERE quote_number IS NOT NULL AND quote_number != ''").fetchall()
                for p in pcs:
                    result["pcs_with_quotes"].append(dict(p))
            except:
                pass
    except Exception as e:
        result["error"] = str(e)
    
    # Find gaps and duplicates
    import re
    nums = []
    for q in result["quotes"]:
        m = re.search(r'R\d{2}Q(\d+)', q.get("quote_number", ""))
        if m:
            nums.append(int(m.group(1)))
    
    nums_set = set(nums)
    if nums:
        for i in range(1, max(nums) + 1):
            if i not in nums_set:
                result["gaps"].append(f"R26Q{i}")
        
        from collections import Counter
        dupes = [f"R26Q{n}" for n, count in Counter(nums).items() if count > 1]
        result["duplicates"] = dupes
    
    result["max_used"] = max(nums) if nums else 0
    result["expected_next"] = f"R26Q{max(nums)+1}" if nums else "R26Q1"
    
    return jsonify(result)


@bp.route("/api/rfq/<rid>/package-diag")
@auth_required
def api_rfq_package_diag(rid):
    """Diagnose RFQ package: what templates, what agency, what forms."""
    import os
    from src.core.paths import DATA_DIR
    
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    
    result = {
        "rfq_id": rid,
        "solicitation": r.get("solicitation_number", ""),
        "agency": r.get("agency", ""),
        "agency_name": r.get("agency_name", ""),
        "form_type": r.get("form_type", ""),
        "quote_type": r.get("quote_type", ""),
        "templates_on_rfq": r.get("templates", {}),
        "output_files": r.get("output_files", []),
        "draft_email": bool(r.get("draft_email")),
    }
    
    # Check template files on disk
    tmpl = r.get("templates", {})
    result["template_files_exist"] = {}
    for ttype, path in tmpl.items():
        exists = os.path.exists(path) if path else False
        result["template_files_exist"][ttype] = {
            "path": path,
            "exists": exists,
            "size_kb": round(os.path.getsize(path) / 1024, 1) if exists else 0,
        }
    
    # Check rfq_files in DB
    from src.api.dashboard import list_rfq_files
    db_files = list_rfq_files(rid)
    result["db_files"] = []
    for f in db_files:
        result["db_files"].append({
            "id": f.get("id"),
            "filename": f.get("filename"),
            "file_type": f.get("file_type"),
            "category": f.get("category"),
            "size": f.get("file_size"),
            "created_at": f.get("created_at"),
        })
    
    # Check what source PDFs came in with this RFQ
    attachments = r.get("attachments", [])
    result["original_attachments"] = [
        {"filename": a.get("filename", ""), "path": a.get("path", "")} 
        for a in attachments
    ] if attachments else "none"
    
    # Check output directory
    sol = r.get("solicitation_number", "unknown")
    out_dir = os.path.join(DATA_DIR, "output", sol)
    if os.path.exists(out_dir):
        result["output_dir_files"] = os.listdir(out_dir)
    else:
        result["output_dir_files"] = f"directory not found: {out_dir}"
    
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════════════════════
# AGENCY PACKAGE SETTINGS — Configure which forms each agency requires
# ═══════════════════════════════════════════════════════════════════════════════

# All available forms that can be included in a package
AVAILABLE_FORMS = [
    {"id": "703b", "name": "AMS 703B — Request for Quotation", "source": "email", "description": "State RFQ form — comes with email"},
    {"id": "704b", "name": "AMS 704B — Quote Worksheet", "source": "email", "description": "Pricing worksheet — comes with email"},
    {"id": "bidpkg", "name": "Bid Package", "source": "email", "description": "Agency bid package — comes with email"},
    {"id": "quote", "name": "Company Quote (Letterhead)", "source": "generated", "description": "Reytech quote on company letterhead"},
    {"id": "std204", "name": "STD 204 — Payee Data Record", "source": "template", "description": "Standard vendor info form"},
    {"id": "sellers_permit", "name": "Seller's Permit", "source": "static", "description": "California seller's permit copy"},
    {"id": "dvbe843", "name": "DVBE 843 — DVBE Declarations", "source": "generated", "description": "Disabled Veteran Business Enterprise declarations"},
    {"id": "cv012_cuf", "name": "CV 012 — CUF Certification", "source": "template", "description": "Cal Vet Commercially Useful Function certification"},
    {"id": "barstow_cuf", "name": "Barstow CUF", "source": "generated", "description": "VHC-Barstow facility-specific CUF"},
    {"id": "bidder_decl", "name": "GSPD-05-106 — Bidder Declaration", "source": "generated", "description": "DGS bidder declaration form"},
    {"id": "darfur_act", "name": "DGS PD 1 — Darfur Act", "source": "generated", "description": "Darfur Contracting Act certification"},
    {"id": "calrecycle74", "name": "CalRecycle 74", "source": "template", "description": "CalRecycle recycled content certification"},
    {"id": "std1000", "name": "STD 1000 — GenAI Reporting", "source": "template", "description": "Generative AI usage disclosure"},
    {"id": "std205", "name": "STD 205 — Payee Supplement", "source": "generated", "description": "Payee data record supplement"},
    {"id": "drug_free", "name": "STD 21 — Drug-Free Workplace", "source": "generated", "description": "Drug-free workplace certification"},
    {"id": "food_cert", "name": "Food Safety Certification", "source": "generated", "description": "Food handling/safety certification"},
]

# Default agency configs (used to seed the DB)
DEFAULT_AGENCY_CONFIGS = {
    "cchcs": {
        "name": "CCHCS / CDCR",
        "match_patterns": ["CDCR", "CCHCS", "CORRECTIONS", "CORRECTIONAL"],
        "required_forms": ["703b", "704b", "bidpkg", "quote", "std204", "sellers_permit", "dvbe843"],
        "optional_forms": ["calrecycle74"],
        "notes": "California Correctional Health Care Services. Standard AMS 704/703 workflow.",
    },
    "calvet": {
        "name": "Cal Vet / DVA",
        "match_patterns": ["CALVET", "CAL VET", "CVA", "VHC", "VETERANS"],
        "required_forms": ["703b", "704b", "bidpkg", "quote", "std204", "sellers_permit", "dvbe843", "cv012_cuf", "bidder_decl", "darfur_act"],
        "optional_forms": ["barstow_cuf"],
        "notes": "California Department of Veterans Affairs. Requires CUF + bidder declarations.",
    },
    "dgs": {
        "name": "DGS",
        "match_patterns": ["DGS", "GENERAL SERVICES"],
        "required_forms": ["quote", "std204", "sellers_permit", "dvbe843", "bidder_decl", "darfur_act"],
        "optional_forms": ["std1000"],
        "notes": "Department of General Services. No AMS forms — uses their own bid format.",
    },
    "calfire": {
        "name": "CAL FIRE",
        "match_patterns": ["CALFIRE", "CAL FIRE", "FORESTRY"],
        "required_forms": ["quote", "std204", "sellers_permit", "dvbe843"],
        "optional_forms": [],
        "notes": "California Department of Forestry and Fire Protection.",
    },
    "other": {
        "name": "Other / Unknown",
        "match_patterns": [],
        "required_forms": ["quote", "std204", "sellers_permit"],
        "optional_forms": ["dvbe843"],
        "notes": "Default config for unrecognized agencies. Minimal forms.",
    },
}


def _load_agency_configs():
    """Load agency package configs from DB, seeding defaults if empty."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS agency_package_configs (
                agency_key   TEXT PRIMARY KEY,
                agency_name  TEXT NOT NULL,
                match_patterns TEXT DEFAULT '[]',
                required_forms TEXT DEFAULT '[]',
                optional_forms TEXT DEFAULT '[]',
                notes        TEXT DEFAULT '',
                updated_at   TEXT,
                updated_by   TEXT DEFAULT 'system'
            )""")
            
            rows = conn.execute("SELECT * FROM agency_package_configs ORDER BY agency_name").fetchall()
            configs = {}
            for r in rows:
                d = dict(r)
                d["match_patterns"] = json.loads(d.get("match_patterns") or "[]")
                d["required_forms"] = json.loads(d.get("required_forms") or "[]")
                d["optional_forms"] = json.loads(d.get("optional_forms") or "[]")
                configs[d["agency_key"]] = d
            
            # Seed defaults if empty
            if not configs:
                for key, cfg in DEFAULT_AGENCY_CONFIGS.items():
                    conn.execute("""INSERT OR IGNORE INTO agency_package_configs 
                        (agency_key, agency_name, match_patterns, required_forms, optional_forms, notes, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
                        (key, cfg["name"], json.dumps(cfg["match_patterns"]),
                         json.dumps(cfg["required_forms"]), json.dumps(cfg["optional_forms"]),
                         cfg.get("notes", "")))
                conn.commit()
                configs = DEFAULT_AGENCY_CONFIGS.copy()
                log.info("Seeded %d default agency package configs", len(configs))
            else:
                # Patch stale rows: add missing required_forms from defaults without
                # removing forms the user has customized.
                patched = 0
                for key, default_cfg in DEFAULT_AGENCY_CONFIGS.items():
                    if key not in configs:
                        conn.execute("""INSERT OR IGNORE INTO agency_package_configs
                            (agency_key, agency_name, match_patterns, required_forms, optional_forms, notes, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
                            (key, default_cfg["name"], json.dumps(default_cfg["match_patterns"]),
                             json.dumps(default_cfg["required_forms"]), json.dumps(default_cfg["optional_forms"]),
                             default_cfg.get("notes", "")))
                        configs[key] = default_cfg.copy()
                        patched += 1
                    else:
                        existing_req = set(configs[key].get("required_forms", []))
                        default_req = set(default_cfg["required_forms"])
                        missing = default_req - existing_req
                        if missing:
                            # Preserve order: default order first, then any user additions
                            ordered = [f for f in default_cfg["required_forms"]]
                            ordered += [f for f in existing_req if f not in default_req]
                            conn.execute("""UPDATE agency_package_configs
                                SET required_forms=?, updated_at=datetime('now'), updated_by='auto_patch'
                                WHERE agency_key=?""",
                                (json.dumps(ordered), key))
                            configs[key]["required_forms"] = ordered
                            log.info("Patched agency config '%s': added %s", key, missing)
                            patched += 1
                if patched:
                    conn.commit()
            
            return configs
    except Exception as e:
        log.warning("Failed to load agency configs: %s", e)
        return DEFAULT_AGENCY_CONFIGS.copy()


def _match_agency(rfq_data):
    """Match an RFQ to an agency config based on agency field + match patterns."""
    configs = _load_agency_configs()
    agency = (rfq_data.get("agency", "") or "").upper()
    agency_name = (rfq_data.get("agency_name", "") or "").upper()
    sender = (rfq_data.get("email_sender", "") or "").upper()
    combined = f"{agency} {agency_name} {sender}"
    
    for key, cfg in configs.items():
        if key == "other":
            continue
        patterns = cfg.get("match_patterns", [])
        if isinstance(patterns, str):
            patterns = json.loads(patterns)
        for pattern in patterns:
            if pattern.upper() in combined:
                return key, cfg
    
    return "other", configs.get("other", DEFAULT_AGENCY_CONFIGS["other"])


@bp.route("/settings/packages")
@auth_required
def agency_package_settings():
    """Agency RFQ Package Settings page."""
    configs = _load_agency_configs()
    return render_page("agency_packages.html", active_page="Agents",
                       configs=configs, available_forms=AVAILABLE_FORMS)


@bp.route("/api/agency-configs")
@auth_required
def api_agency_configs():
    """Get all agency package configs."""
    return jsonify({"ok": True, "configs": _load_agency_configs(), "available_forms": AVAILABLE_FORMS})


@bp.route("/api/agency-config/<key>", methods=["POST"])
@auth_required
def api_save_agency_config(key):
    """Save or create an agency package config."""
    data = request.get_json(silent=True) or {}
    
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS agency_package_configs (
                agency_key TEXT PRIMARY KEY, agency_name TEXT NOT NULL,
                match_patterns TEXT DEFAULT '[]', required_forms TEXT DEFAULT '[]',
                optional_forms TEXT DEFAULT '[]', notes TEXT DEFAULT '',
                updated_at TEXT, updated_by TEXT DEFAULT 'system'
            )""")
            
            conn.execute("""INSERT INTO agency_package_configs 
                (agency_key, agency_name, match_patterns, required_forms, optional_forms, notes, updated_at, updated_by)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'), 'user')
                ON CONFLICT(agency_key) DO UPDATE SET
                    agency_name=excluded.agency_name,
                    match_patterns=excluded.match_patterns,
                    required_forms=excluded.required_forms,
                    optional_forms=excluded.optional_forms,
                    notes=excluded.notes,
                    updated_at=excluded.updated_at,
                    updated_by=excluded.updated_by""",
                (key,
                 data.get("agency_name", key),
                 json.dumps(data.get("match_patterns", [])),
                 json.dumps(data.get("required_forms", [])),
                 json.dumps(data.get("optional_forms", [])),
                 data.get("notes", "")))
        
        return jsonify({"ok": True, "key": key})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/agency-config/<key>", methods=["DELETE"])
@auth_required
def api_delete_agency_config(key):
    """Delete an agency config."""
    if key in ("cchcs", "other"):
        return jsonify({"ok": False, "error": "Cannot delete core agency configs"})
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM agency_package_configs WHERE agency_key=?", (key,))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/agency-config-reset", methods=["POST"])
@auth_required
def api_agency_config_reset():
    """Force-wipe and re-seed all agency configs from DEFAULT_AGENCY_CONFIGS.
    Use this after a DB rebuild to ensure configs match current defaults."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM agency_package_configs")
            for key, cfg in DEFAULT_AGENCY_CONFIGS.items():
                conn.execute("""INSERT INTO agency_package_configs
                    (agency_key, agency_name, match_patterns, required_forms, optional_forms, notes, updated_at, updated_by)
                    VALUES (?, ?, ?, ?, ?, ?, datetime('now'), 'reset')""",
                    (key, cfg["name"], json.dumps(cfg["match_patterns"]),
                     json.dumps(cfg["required_forms"]), json.dumps(cfg["optional_forms"]),
                     cfg.get("notes", "")))
            conn.commit()
        log.info("Agency configs reset to defaults (%d configs)", len(DEFAULT_AGENCY_CONFIGS))
        return jsonify({"ok": True, "message": f"Reset {len(DEFAULT_AGENCY_CONFIGS)} agency configs to defaults",
                        "configs": list(DEFAULT_AGENCY_CONFIGS.keys())})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})
