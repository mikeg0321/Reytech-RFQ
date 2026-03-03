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
        if pc.get("status") != "sent":
            continue
        sent_at = pc.get("sent_at", pc.get("completed_at", ""))
        if sent_at and sent_at < cutoff:
            try:
                days_since = (datetime.now() - datetime.fromisoformat(
                    sent_at.replace("Z", "+00:00").split("+")[0]
                )).days
            except Exception:
                days_since = days_threshold
            stale.append({
                "type": "pc", "id": pid,
                "number": pc.get("pc_number", ""),
                "institution": pc.get("institution", ""),
                "requestor": pc.get("requestor", ""),
                "email": "",
                "sent_at": sent_at,
                "days_since": days_since,
                "total": 0,
                "link": f"/pricecheck/{pid}",
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
