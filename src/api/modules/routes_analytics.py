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
from src.core.paths import DATA_DIR
from src.api.render import render_page

import time as _time
import threading as _threading
import sqlite3 as _sqlite3
from collections import defaultdict as _defaultdict
from datetime import timedelta as _timedelta

import csv, io, glob, os, json, platform, re, sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

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
@safe_route
def poll_progress(task_id):
    """JSON polling endpoint — returns progress steps since last_idx."""
    try:
        last_idx = max(0, min(int(request.args.get("since", 0)), 999999999))
    except (ValueError, TypeError, OverflowError):
        last_idx = 0
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
@safe_route
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
                # Carry SCPRS cost to vendor_cost — NEVER overwrite supplier quote costs
                for _item in r["line_items"]:
                    _sp = _item.get("scprs_last_price") or 0
                    if _sp and not _item.get("vendor_cost") and not _item.get("supplier_cost") and _item.get("cost_source") != "Supplier Quote":
                        try:
                            _item["vendor_cost"] = float(_sp)
                            _item["cost_source"] = "SCPRS"
                            _item["cost_supplier_name"] = _item.get("scprs_vendor", "")
                            if not _item.get("pricing"): _item["pricing"] = {}
                            _item["pricing"]["unit_cost"] = float(_sp)
                        except (ValueError, TypeError):
                            pass
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
                # Carry Amazon cost to vendor_cost — NEVER overwrite supplier quote costs
                for _item in r["line_items"]:
                    _ap = _item.get("amazon_price") or 0
                    if _ap and not _item.get("vendor_cost") and not _item.get("supplier_cost") and _item.get("cost_source") != "Supplier Quote":
                        try:
                            _item["vendor_cost"] = float(_ap)
                            _item["cost_source"] = "Amazon"
                            if not _item.get("pricing"): _item["pricing"] = {}
                            _item["pricing"]["unit_cost"] = float(_ap)
                        except (ValueError, TypeError):
                            pass
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
                    best = None
                    if matches and isinstance(matches, list) and len(matches) > 0:
                        if matches[0].get("confidence", 0) > 0.5:
                            best = matches[0]
                    elif matches and isinstance(matches, dict) and matches.get("confidence", 0) > 0.5:
                        best = matches
                    if best:
                        item["catalog_match"] = best
                        catalog_found += 1
                        # Auto-fill supplier URL from catalog if not already set
                        if not item.get("item_link"):
                            for url_field in ["best_supplier_url", "product_url", "url", "amazon_url", "item_link"]:
                                cat_url = best.get(url_field, "")
                                if cat_url:
                                    item["item_link"] = cat_url
                                    break
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
            _save_single_rfq(rid, r)
            _emit_progress(task_id, "saved", "Results saved", done=True)
            try:
                from src.core.dal import log_lifecycle_event
                log_lifecycle_event("rfq", rid, "price_lookup",
                    f"Auto-lookup: SCPRS {scprs_found}/{total}, Amazon {amazon_found}/{total}, Catalog {catalog_found}/{total}",
                    actor="system", detail={"scprs": scprs_found, "amazon": amazon_found, "catalog": catalog_found, "total": total})
            except Exception:
                pass
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
@safe_route
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
    _save_single_rfq(rid, r)
    return jsonify({"ok": True, "applied": applied, "total": len(r["line_items"])})


# ═══════════════════════════════════════════════════════════════════════════════
# E6: Buyer Intelligence Dashboard
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/buyer/<path:buyer_key>")
@auth_required
@safe_page
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
@safe_page
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
@safe_route
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
@safe_route
def send_quote_email(rid):
    """Send the generated quote PDF via email directly from the detail page."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404

    data = request.get_json(silent=True) or {}
    # Prefer original buyer email (from forwarded emails) over requestor fields
    to_email = data.get("to") or r.get("original_sender") or r.get("requestor_email", "")
    # Build reply subject from original email subject
    import re as _re_subj
    orig_subject = r.get("email_subject", "")
    if orig_subject and not data.get("subject"):
        clean_subj = _re_subj.sub(r'^(Re:\s*|Fwd?:\s*|FW:\s*)*', '', orig_subject, flags=_re_subj.IGNORECASE).strip()
        subject = f"Re: {clean_subj}" if clean_subj else f"Quote — Solicitation #{r.get('solicitation_number', rid)}"
    else:
        subject = data.get("subject") or f"Quote — Solicitation #{r.get('solicitation_number', rid)}"
    body = data.get("body") or _default_quote_email_body(r)
    pdf_path = data.get("pdf_path") or ""

    if not to_email:
        return jsonify({"ok": False, "error": "No recipient email"}), 400

    # Find the latest generated PDF — broader search across multiple locations
    if not pdf_path:
        sol = r.get("solicitation_number", "")

        # Priority 1: stored paths on the RFQ record
        for _stored_key in ("reytech_quote_pdf", "output_pdf"):
            _sp = r.get(_stored_key, "")
            if _sp and os.path.exists(_sp):
                pdf_path = _sp
                break

        # Priority 2: output_files list
        if not pdf_path and r.get("output_files"):
            for _of in r["output_files"]:
                for _base in [os.path.join(DATA_DIR, "output", sol), os.path.join(DATA_DIR, "output", rid)]:
                    _fp = os.path.join(_base, _of)
                    if os.path.exists(_fp):
                        pdf_path = _fp
                        break
                if pdf_path:
                    break

        # Priority 3: scan output directory by solicitation number
        if not pdf_path and sol:
            output_dir = os.path.join(DATA_DIR, "output", sol)
            if os.path.isdir(output_dir):
                pdfs = sorted([f for f in os.listdir(output_dir) if f.endswith(".pdf")], reverse=True)
                if pdfs:
                    pdf_path = os.path.join(output_dir, pdfs[0])

        # Priority 4: scan output directory by RFQ ID
        if not pdf_path:
            output_dir_rid = os.path.join(DATA_DIR, "output", rid)
            if os.path.isdir(output_dir_rid):
                pdfs = sorted([f for f in os.listdir(output_dir_rid) if f.endswith(".pdf")], reverse=True)
                if pdfs:
                    pdf_path = os.path.join(output_dir_rid, pdfs[0])

        # Priority 5: rfq_files DB
        if not pdf_path:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    row = conn.execute(
                        "SELECT id, filename FROM rfq_files WHERE rfq_id = ? AND filename LIKE '%.pdf' ORDER BY uploaded_at DESC LIMIT 1",
                        (rid,)).fetchone()
                    if row:
                        from src.api.modules.routes_rfq import get_rfq_file
                        f = get_rfq_file(row[0])
                        if f and f.get("data"):
                            import tempfile
                            _tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False, prefix="rfq_send_")
                            _tmp.write(f["data"])
                            _tmp.close()
                            pdf_path = _tmp.name
            except Exception as _db_err:
                log.debug("rfq_files PDF lookup: %s", _db_err)

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

        msg = MIMEMultipart("mixed")
        msg["From"] = gmail_user
        msg["To"] = to_email
        msg["Subject"] = subject
        msg["Reply-To"] = gmail_user

        # Threading — reply in buyer's email thread
        email_message_id = r.get("email_message_id", "")
        if email_message_id:
            msg["In-Reply-To"] = email_message_id
            msg["References"] = email_message_id

        # Plain text only — Gmail auto-appends the configured signature
        msg.attach(MIMEText(body, "plain"))

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
        _save_single_rfq(rid, r)

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
        
        # ── Google Drive: archive sent quote email ──
        try:
            from src.agents.drive_triggers import on_quote_sent
            on_quote_sent(r, body, to_email)
        except Exception as _gde:
            log.debug("Drive trigger (quote_sent): %s", _gde)
        
        return jsonify({"ok": True, "sent_to": to_email})

    except Exception as e:
        log.error("Failed to send quote email for %s: %s", rid, e)
        return jsonify({"ok": False, "error": str(e)}), 500


def _default_quote_email_body(r):
    sol = r.get("solicitation_number", "")
    requestor = r.get("requestor_name", "").split(",")[0].split("@")[0].strip()
    if not requestor or "@" in requestor:
        requestor = "Procurement Officer"
    # Use first name only
    first_name = requestor.split()[0] if requestor and " " in requestor else requestor
    
    # Plain text only — Gmail auto-appends the configured signature
    return (f"Dear {first_name},\n\n"
            f"Please find attached our bid response for Solicitation #{sol}.\n\n"
            f"Please let us know if you have any questions.\n\n"
            f"Thank you for the opportunity.")


# ═══════════════════════════════════════════════════════════════════════════════
# E11: Bulk Operations on Queue
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/bulk/action", methods=["POST"])
@auth_required
@safe_route
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
@safe_route
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
    "pricing.markup_buttons": "10,15,20,25,30",
    "pricing.markup_step": 1,
    "pricing.undercut_buttons": "1,2,5",
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
    "email.sender_blocklist": "",
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
@safe_page
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
@safe_route
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
@safe_route
def get_settings_data():
    """Return all settings as JSON."""
    return jsonify({"ok": True, "settings": _load_settings()})


@bp.route("/api/settings/get")
@auth_required
@safe_route
def get_single_setting():
    """Return a single setting value by key."""
    key = request.args.get("key", "")
    if not key:
        return jsonify({"ok": False, "error": "key required"})
    settings = _load_settings()
    value = settings.get(key, "")
    return jsonify({"ok": True, "key": key, "value": value})


@bp.route("/api/settings/markup-config")
@auth_required
@safe_route
def get_markup_config():
    """Return markup button configuration for RFQ/PC pages."""
    settings = _load_settings()
    buttons_str = settings.get("pricing.markup_buttons", "10,15,20,25,30")
    undercut_str = settings.get("pricing.undercut_buttons", "1,2,5")
    step = settings.get("pricing.markup_step", 1)
    try:
        buttons = [float(x.strip()) for x in str(buttons_str).split(",") if x.strip()]
    except Exception:
        buttons = [10, 15, 20, 25, 30]
    try:
        undercuts = [float(x.strip()) for x in str(undercut_str).split(",") if x.strip()]
    except Exception:
        undercuts = [1, 2, 5]
    try:
        step = float(step)
    except Exception:
        step = 1
    return jsonify({
        "ok": True,
        "markup_buttons": buttons,
        "undercut_buttons": undercuts,
        "step": step,
        "default_markup": settings.get("pricing.default_markup_pct", 20),
    })



# (Legacy v1 endpoints removed — replaced by routes_v1.py with proper auth)


# ═══════════════════════════════════════════════════════════════════════════════
# Enhancement 2: Quick-Price Panel — Price items without leaving queue
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/quick-price/<entity_type>/<eid>")
@auth_required
@safe_route
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
@safe_route
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
        _save_single_pc(eid, pc)
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
        _save_single_rfq(eid, r)
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
@safe_route
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

_stale_cache = {"data": None, "ts": 0}

@bp.route("/api/stale-quotes")
@auth_required
@safe_route
def stale_quotes():
    """Find quotes sent but with no response after configurable days."""
    import time as _time
    global _stale_cache
    if _stale_cache["data"] and (_time.time() - _stale_cache["ts"]) < 120:
        return jsonify(_stale_cache["data"])
    try:
        days_threshold = max(1, min(int(request.args.get("days", 3)), 365))
    except (ValueError, TypeError, OverflowError):
        days_threshold = 3
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
    _stale_result = {"ok": True, "stale": stale, "threshold_days": days_threshold,
                     "count": len(stale)}
    _stale_cache["data"] = _stale_result
    _stale_cache["ts"] = _time.time()
    return jsonify(_stale_result)


@bp.route("/api/stale-quotes/<entity_type>/<eid>/follow-up", methods=["POST"])
@auth_required
@safe_route
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
            _save_single_rfq(eid, r)
        elif entity_type == "pc":
            pcs = _load_price_checks()
            pc = pcs.get(eid, {})
            pc.setdefault("follow_ups", []).append({
                "sent_at": datetime.now().isoformat(),
                "to": to_email,
            })
            pc["last_follow_up"] = datetime.now().isoformat()
            _save_single_pc(eid, pc)

        log.info("Follow-up sent for %s/%s to %s", entity_type, eid, to_email)
        return jsonify({"ok": True, "sent_to": to_email})
    except Exception as e:
        log.error("Follow-up send failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/stale-quotes/bulk-follow-up", methods=["POST"])
@auth_required
@safe_route
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
@safe_route
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
        _save_single_rfq(rfq_id, rfqs[rfq_id])
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
            _save_single_rfq(match, rfqs[match])
        else:
            _save_single_pc(pcid, pc)
            return jsonify({"ok": True, "matched": False, "message": "No matching RFQ found"})

    _save_single_pc(pcid, pc)
    return jsonify({"ok": True, "matched": True, "rfq_id": pc.get("linked_rfq_id")})


def _convert_single_pc_to_rfq(pcid, pc, extra_fields=None):
    """Core logic: convert a single PC into an RFQ dict. Returns (rfq_id, rfq_data, files_copied, agency_info).
    Does NOT save to DB — caller handles persistence.
    extra_fields: optional dict of additional fields to merge into rfq_data (e.g., bundle_id).
    """
    import uuid as _uuid
    import copy as _copy
    rfq_id = str(_uuid.uuid4())[:8]
    now = datetime.now().isoformat()

    # ── Take the PC record as-is — no field remapping ────────────────────
    rfq_data = _copy.deepcopy(pc)

    # Stamp with RFQ identity
    rfq_data["id"] = rfq_id
    rfq_data["source"] = "pc_conversion"
    rfq_data["created_at"] = now
    rfq_data["converted_at"] = now
    rfq_data["status"] = "priced" if pc.get("status") in ("priced", "quoted", "sent") else "new"

    # Link back to source PC
    rfq_data["linked_pc_id"] = pcid
    rfq_data["linked_pc_number"] = pc.get("pc_number") or pc.get("solicitation_number") or pcid[:12]
    rfq_data["source_pc"] = pcid
    rfq_data["source_pc_number"] = pc.get("pc_number", "")
    rfq_data["source_pc_status"] = pc.get("status", "")
    rfq_data["source_pc_requestor"] = pc.get("requestor", "")

    # Ensure key RFQ fields exist (use PC values, no translation)
    rfq_data.setdefault("solicitation_number", pc.get("pc_number", ""))
    rfq_data.setdefault("line_items", pc.get("items", []))
    rfq_data.setdefault("requestor_name", pc.get("requestor", ""))
    rfq_data.setdefault("requestor_email", pc.get("requestor_email") or pc.get("email", ""))
    rfq_data.setdefault("agency", pc.get("agency") or pc.get("institution", ""))
    rfq_data.setdefault("institution", pc.get("institution", ""))
    rfq_data.setdefault("delivery_location", pc.get("ship_to", ""))
    rfq_data.setdefault("reytech_quote_number", pc.get("reytech_quote_number", ""))

    # ── Infer agency and required forms ──────────────────────────────────
    _agency_key = "other"
    _agency_cfg = {}
    _conversion_warnings = []
    try:
        from src.core.agency_config import match_agency as _match_agency, get_agency_config as _get_cfg
        _agency_key, _agency_cfg = _match_agency(rfq_data)
        rfq_data["agency_key"] = _agency_key
        rfq_data["agency_name"] = _agency_cfg.get("name", _agency_key)
        log.info("PC→RFQ agency inference: %s (matched_by: %s)",
                 _agency_key, _agency_cfg.get("matched_by", "?"))
    except Exception as _ae:
        log.warning("Agency inference failed for PC→RFQ %s: %s", pcid, _ae)
        _conversion_warnings.append("Could not auto-detect agency — set it manually on the RFQ page.")

    # Check which buyer-provided templates are needed but missing
    _req_forms = _agency_cfg.get("required_forms", [])
    _buyer_templates = {"703b", "703c", "704b"}  # must come from buyer's RFQ email
    for _ft in _req_forms:
        if _ft in _buyer_templates:
            _conversion_warnings.append(
                f"Upload {_ft.upper()} template before generating package "
                f"(must come from the buyer's RFQ email)."
            )

    # Propagate bundle_id from PC
    if pc.get("bundle_id"):
        rfq_data["bundle_id"] = pc["bundle_id"]

    if extra_fields:
        rfq_data.update(extra_fields)

    source_file = pc.get("source_file", "")
    if source_file and os.path.exists(source_file):
        rfq_data["source_file"] = source_file
        rfq_data["pc_pdf_path"] = source_file

    # Copy attachments/files from PC to RFQ
    files_copied = 0
    try:
        from src.api.dashboard import list_rfq_files, get_rfq_file, save_rfq_file
        pc_files = list_rfq_files(pcid)
        for pf in pc_files:
            full = get_rfq_file(pf["id"])
            if full and full.get("data"):
                save_rfq_file(
                    rfq_id=rfq_id, filename=pf["filename"],
                    file_type=pf.get("file_type", ""),
                    data=full["data"],
                    category=pf.get("category", "attachment"),
                    uploaded_by="pc_conversion",
                )
                files_copied += 1
    except Exception as _e:
        log.warning("File copy from PC %s to RFQ %s: %s", pcid, rfq_id, _e)

    return rfq_id, rfq_data, files_copied, {"agency_key": _agency_key, "agency_cfg": _agency_cfg, "req_forms": _req_forms, "warnings": _conversion_warnings}


@bp.route("/api/pc/<pcid>/convert-to-rfq", methods=["POST"])
@auth_required
@safe_route
def convert_pc_to_rfq(pcid):
    """Convert a Price Check into a new RFQ.

    This is a status + DB change — the PC record IS the RFQ data.
    No field remapping. Same items, same prices, same everything.
    """
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"}), 404

    rfq_id, rfq_data, files_copied, agency_info = _convert_single_pc_to_rfq(pcid, pc)
    _agency_key = agency_info["agency_key"]
    _agency_cfg = agency_info["agency_cfg"]
    _req_forms = agency_info["req_forms"]
    _conversion_warnings = agency_info["warnings"]

    # ── Save to RFQ store ────────────────────────────────────────────────
    rfqs = load_rfqs()
    rfqs[rfq_id] = rfq_data
    _save_single_rfq(rfq_id, rfq_data)

    # ── Update PC with link ──────────────────────────────────────────────
    now = datetime.now().isoformat()
    pc["linked_rfq_id"] = rfq_id
    pc["linked_rfq_at"] = now
    pc["converted_to_rfq"] = True
    _save_single_pc(pcid, pc)

    # ── Audit logging ────────────────────────────────────────────────────
    item_count = len(rfq_data.get("line_items", rfq_data.get("items", [])))
    log.info("PC→RFQ: %s → %s (%d items, %d files, status=%s)",
             pcid, rfq_id, item_count, files_copied, rfq_data["status"])

    # Log to activity/CRM
    try:
        from src.api.dashboard import _log_crm_activity
        _log_crm_activity(
            rfq_data.get("reytech_quote_number") or rfq_id,
            "pc_to_rfq_conversion",
            f"PC {pc.get('pc_number', pcid)} converted to RFQ {rfq_id}. "
            f"{item_count} items, status={rfq_data['status']}.",
            actor="user",
            metadata={
                "pc_id": pcid,
                "rfq_id": rfq_id,
                "pc_number": pc.get("pc_number", ""),
                "item_count": item_count,
                "files_copied": files_copied,
                "source_status": pc.get("status", ""),
            },
        )
    except Exception as _e:
        log.debug("CRM activity log for PC→RFQ: %s", _e)

    # Log to SQLite audit
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT INTO activity_log (event_type, subject, body, logged_at, actor, metadata_json)
                VALUES (?,?,?,?,?,?)
            """, (
                "pc_to_rfq",
                f"PC {pc.get('pc_number', pcid)} → RFQ {rfq_id}",
                f"Converted with {item_count} items, {files_copied} files. "
                f"PC status was '{pc.get('status', '')}', RFQ status set to '{rfq_data['status']}'.",
                now, "user",
                __import__("json").dumps({
                    "pc_id": pcid, "rfq_id": rfq_id,
                    "pc_number": pc.get("pc_number", ""),
                    "item_count": item_count,
                }, default=str),
            ))
    except Exception as _e:
        log.debug("Activity log for PC→RFQ: %s", _e)

    return jsonify({
        "ok": True, "rfq_id": rfq_id, "items": item_count,
        "files_copied": files_copied,
        "agency_key": _agency_key,
        "agency_name": _agency_cfg.get("name", _agency_key),
        "required_forms": _req_forms,
        "warnings": _conversion_warnings,
    })


@bp.route("/api/pc/<pcid>/reclassify-as-rfq", methods=["POST"])
@auth_required
@safe_route
def reclassify_pc_as_rfq(pcid):
    """Reclassify a misclassified PC as an RFQ.

    Unlike convert_pc_to_rfq (which means "PC is done, now make an RFQ"),
    reclassify means "this was never a PC — route it to the RFQ queue."
    Archives the PC and creates a proper RFQ with email context.
    """
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"}), 404
    if pc.get("status") == "reclassified":
        return jsonify({"ok": False, "error": "Already reclassified"}), 400

    # Reuse existing conversion logic for the heavy lifting
    rfq_id, rfq_data, files_copied, agency_info = _convert_single_pc_to_rfq(pcid, pc)
    _agency_key = agency_info["agency_key"]
    _agency_cfg = agency_info["agency_cfg"]
    _conversion_warnings = agency_info["warnings"]

    # Mark as reclassification, not conversion
    rfq_data["source"] = "pc_reclassification"

    # ── Extract ship-to / institution from email body ──
    _body = (pc.get("body_text") or pc.get("email_body") or
             pc.get("body_preview") or pc.get("body") or "")
    _subj = pc.get("email_subject", "")
    _combined = f"{_subj} {_body}"
    if _combined.strip():
        import re as _re_rcl
        # CalVet / Veterans Home pattern
        _calvet_m = _re_rcl.search(
            r'(?:Veterans?\s+Home\s+of\s+California|Cal\s*Vet|CALVET)'
            r'[\s,\-\u2013\u2014:]+([A-Za-z][\w\s.]+?)(?:\n|$|;|\s{2,})',
            _combined, _re_rcl.IGNORECASE
        )
        if _calvet_m:
            _cv_loc = _calvet_m.group(1).strip().rstrip(",.- ")
            if _cv_loc and len(_cv_loc) >= 2:
                try:
                    from src.core.institution_resolver import resolve as _resolve_inst
                    _resolved = _resolve_inst(_cv_loc)
                    if _resolved and _resolved.get("canonical"):
                        rfq_data["institution"] = _resolved["canonical"]
                        rfq_data["agency"] = _resolved["canonical"]
                        rfq_data["agency_key"] = _resolved.get("agency", "calvet")
                        _agency_key = rfq_data["agency_key"]
                    else:
                        rfq_data["institution"] = f"Veterans Home of California, {_cv_loc}"
                        rfq_data["agency"] = rfq_data["institution"]
                except Exception:
                    rfq_data["institution"] = f"Veterans Home of California, {_cv_loc}"
                    rfq_data["agency"] = rfq_data["institution"]
                rfq_data["delivery_location"] = rfq_data["institution"]

        # Generic ship-to / deliver-to patterns
        if not rfq_data.get("delivery_location"):
            _ship_m = _re_rcl.search(
                r'(?:ship\s+to|deliver\s+to|following\s+location)\s*:?\s*\n*\s*'
                r'([A-Z][A-Za-z\s,.\-]+?)(?:\n|$|;|\s{3,})',
                _body, _re_rcl.IGNORECASE
            )
            if _ship_m:
                _ship = _ship_m.group(1).strip().rstrip(",.- ")
                if _ship and len(_ship) >= 5:
                    rfq_data["delivery_location"] = _ship
                    if not rfq_data.get("institution"):
                        rfq_data["institution"] = _ship

    # Carry email context to RFQ
    if _body:
        rfq_data["body_text"] = _body
    if _subj:
        rfq_data["email_subject"] = _subj

    # ── Save RFQ ──
    rfqs = load_rfqs()
    rfqs[rfq_id] = rfq_data
    _save_single_rfq(rfq_id, rfq_data)

    # ── Archive PC as reclassified ──
    now = datetime.now().isoformat()
    pc["status"] = "reclassified"
    pc["reclassified_to_rfq"] = rfq_id
    pc["reclassified_at"] = now
    pc["converted_to_rfq"] = True
    pc["linked_rfq_id"] = rfq_id
    _save_single_pc(pcid, pc)

    # ── Audit log ──
    item_count = len(rfq_data.get("line_items", rfq_data.get("items", [])))
    log.info("PC RECLASSIFIED: %s → RFQ %s (%d items, agency=%s, ship_to=%s)",
             pcid, rfq_id, item_count, _agency_key,
             rfq_data.get("delivery_location", "unknown"))

    try:
        from src.api.dashboard import _log_crm_activity
        _log_crm_activity(
            rfq_data.get("reytech_quote_number") or rfq_id,
            "pc_reclassified_to_rfq",
            f"PC {pc.get('pc_number', pcid)} reclassified as RFQ {rfq_id}. "
            f"{item_count} items. Was never a PC — misclassified by email pipeline.",
            actor="user",
            metadata={"pc_id": pcid, "rfq_id": rfq_id, "pc_number": pc.get("pc_number", "")},
        )
    except Exception:
        pass

    return jsonify({
        "ok": True, "rfq_id": rfq_id, "items": item_count,
        "files_copied": files_copied,
        "agency_key": _agency_key,
        "agency_name": _agency_cfg.get("name", _agency_key),
        "warnings": _conversion_warnings,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Enhancement 5b: Follow-Up Dashboard Page
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/follow-ups")
@auth_required
@safe_page
def follow_ups_page():
    """Dashboard showing all stale quotes needing follow-up."""
    return render_page("follow_ups.html", active_page="Pipeline")


# ═══════════════════════════════════════════════════════════════════════════════
# Win/Loss Analysis Dashboard (PRD-v32 F9)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/analytics/win-loss")
@auth_required
@safe_page
def win_loss_page():
    """Win/loss analysis page with charts and trends."""
    return render_page("win_loss.html", active_page="Analytics")


@bp.route("/loss-intelligence")
@auth_required
@safe_page
def loss_intelligence_page():
    """Loss intelligence dashboard — why we lose, margin analysis, competitor deep dive."""
    return render_page("loss_intelligence.html", active_page="Analytics")


@bp.route("/loss-detail/<quote_number>")
@auth_required
@safe_page
def loss_detail_page(quote_number):
    """Detailed loss analysis for a specific quote."""
    import json as _json

    context = {"quote_number": quote_number}

    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.row_factory = sqlite3.Row

            # Get competitor_intel record
            ci = conn.execute("""
                SELECT * FROM competitor_intel WHERE quote_number=? LIMIT 1
            """, (quote_number,)).fetchone()
            if ci:
                context["loss"] = dict(ci)
                # Parse items_detail JSON
                try:
                    context["items"] = _json.loads(ci["items_detail"]) if ci["items_detail"] else []
                except (ValueError, TypeError, KeyError):
                    context["items"] = []
            else:
                context["loss"] = None
                context["items"] = []

            # Get quote info
            q = conn.execute("""
                SELECT quote_number, agency, institution, total, sent_at, status, close_reason
                FROM quotes WHERE quote_number=? LIMIT 1
            """, (quote_number,)).fetchone()
            context["quote"] = dict(q) if q else {}

            # Run supplier inference on each item
            if context["items"]:
                try:
                    from src.agents.supplier_inference import infer_supply_chain
                    for item in context["items"]:
                        item["inference"] = infer_supply_chain(
                            competitor_name=context["loss"].get("competitor_name", "") if context["loss"] else "",
                            competitor_price=item.get("winner_unit_price", 0),
                            our_cost=item.get("our_cost", 0),
                            our_supplier=item.get("our_supplier", ""),
                            item_description=item.get("our_description", item.get("description", "")),
                        )
                except Exception as _si_e:
                    log.debug("loss-detail supplier inference: %s", _si_e)
    except Exception as e:
        log.exception("loss-detail error for %s", quote_number)
        context["error"] = str(e)

    return render_page("loss_detail.html", active_page="Analytics", **context)


@bp.route("/api/analytics/win-loss")
@auth_required
@safe_route
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
@safe_page
def supplier_performance_page():
    """Supplier performance rankings and analytics."""
    return render_page("supplier_performance.html", active_page="Vendors")


@bp.route("/api/suppliers/performance")
@auth_required
@safe_route
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
@safe_route
def api_webhooks_config():
    """Get webhook configuration."""
    try:
        from src.core.webhooks import get_config
        return jsonify({"ok": True, **get_config()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/webhooks/save", methods=["POST"])
@auth_required
@safe_route
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
@safe_route
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
@safe_route
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


@bp.route("/api/rfq/<rid>/retry-auto-price", methods=["POST"])
@auth_required
@safe_route
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
        _save_single_rfq(rid, r)
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
@safe_route
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
        from src.api.dashboard import _link_rfq_to_pc, _load_price_checks, _save_price_checks, _save_single_pc
        
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
                        _save_single_pc(pid, pc)
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
            _save_single_rfq(rid, r)
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
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/rfqs/relink-all", methods=["POST", "GET"])
@auth_required
@safe_route
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
@safe_route
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
@safe_route
def api_pcs_list():
    """List PCs for import picker. Pass ?rfq_id=<rid> for smart ranking."""
    from src.api.dashboard import _load_price_checks, _save_price_checks, _save_single_pc, _save_single_rfq
    pcs = _load_price_checks()
    rfq_id = request.args.get("rfq_id", "")
    rfq = {}
    rfq_sol = ""
    rfq_institution = ""

    if rfq_id:
        try:
            _rfqs = load_rfqs()
            rfq = _rfqs.get(rfq_id, {})
            rfq_sol = (rfq.get("solicitation_number") or "").strip()
            rfq_institution = (rfq.get("agency_name") or rfq.get("department") or "").lower()
        except Exception:
            pass

    result = []
    for pid, pc in pcs.items():
        if pc.get("status") in ("converted", "deleted", "archived"):
            continue
        items = pc.get("items", [])
        priced = sum(1 for it in items if isinstance(it, dict) and (
            it.get("pricing", {}).get("recommended_price") or
            it.get("pricing", {}).get("unit_cost") or
            it.get("pricing", {}).get("scprs_price") or
            it.get("supplier_cost") or it.get("price_per_unit")
        ))
        pc_sol = (pc.get("pc_number") or pc.get("solicitation_hint") or "").strip()
        pc_inst = (pc.get("institution") or pc.get("department") or "").lower()

        score = 0
        match_reason = ""
        if rfq_sol and pc_sol and rfq_sol == pc_sol:
            score += 100; match_reason = "sol_match"
        elif rfq_sol and pc_sol and (rfq_sol in pc_sol or pc_sol in rfq_sol):
            score += 60; match_reason = "sol_partial"
        if rfq_institution and pc_inst and len(rfq_institution) >= 4 and (rfq_institution[:8] in pc_inst or pc_inst[:8] in rfq_institution):
            score += 20; match_reason = match_reason or "institution_match"
        if priced > 0:
            score += 5
        if pc.get("linked_rfq_id") == rfq_id:
            score += 50; match_reason = "linked"

        result.append({
            "id": pid, "pc_number": pc_sol or pid,
            "institution": (pc.get("institution") or "")[:50],
            "requestor": (pc.get("requestor") or pc.get("requestor_name") or "")[:40],
            "status": pc.get("status", ""), "items": len(items), "priced": priced,
            "created_at": pc.get("created_at", ""),
            "score": score, "match_reason": match_reason,
            "source_pdf": bool(pc.get("source_pdf") or pc.get("source_file")),
        })

    result.sort(key=lambda x: (x["score"], x["priced"], x["created_at"]), reverse=True)

    # Auto-link if exact sol match and not already linked
    auto_linked = None
    if rfq_sol and rfq_id:
        exact = [r for r in result if r["match_reason"] == "sol_match"]
        if exact and rfq and not rfq.get("linked_pc_id"):
            try:
                _rfqs = load_rfqs()
                _r = _rfqs.get(rfq_id, {})
                _r["linked_pc_id"] = exact[0]["id"]
                _r["linked_pc_number"] = exact[0]["pc_number"]
                _save_single_rfq(rfq_id, _r)
                _pc_obj = pcs.get(exact[0]["id"], {})
                _pc_obj["linked_rfq_id"] = rfq_id
                _save_single_pc(exact[0]["id"], _pc_obj)
                auto_linked = exact[0]["id"]
                log.info("Auto-linked RFQ %s ↔ PC %s (sol=%s)", rfq_id, exact[0]["id"], rfq_sol)
            except Exception as _ale:
                log.debug("Auto-link failed: %s", _ale)

    return jsonify({"ok": True, "pcs": result, "auto_linked": auto_linked, "rfq_sol": rfq_sol})


@bp.route("/api/rfq/<rid>/import-from-pc", methods=["POST"])
@auth_required
@safe_route
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

    # Build RFQ line items from PC items — copy ALL fields, don't cherry-pick
    imported = []
    for it in pc_items:
        if not isinstance(it, dict):
            continue

        # Copy ALL fields — don't cherry-pick
        rfq_item = {}
        for key, val in it.items():
            rfq_item[key] = val

        # Then normalize field names for RFQ compatibility
        rfq_item.setdefault("description", it.get("desc", ""))
        rfq_item.setdefault("quantity", it.get("qty", 1))
        rfq_item.setdefault("uom", it.get("uom", "EACH"))
        rfq_item.setdefault("item_number", it.get("part_number", ""))
        rfq_item.setdefault("supplier_cost",
            it.get("cost", it.get("unit_cost", it.get("unit_price"))))
        rfq_item.setdefault("price_per_unit",
            it.get("bid_price", it.get("sell_price")))
        rfq_item.setdefault("item_supplier", it.get("supplier", ""))
        rfq_item.setdefault("item_link",
            it.get("url", it.get("product_url",
            it.get("amazon_url", ""))))

        # Tag the source
        rfq_item["source_pc"] = pc_id
        rfq_item["imported_from_pc"] = True
        rfq_item["imported_at"] = datetime.now().isoformat()
        rfq_item["_from_pc"] = pc.get("pc_number", pc_id)

        # Also check for PO screenshots linked to this PC
        try:
            po_num = pc.get("po_number", "")
            if po_num:
                for ext in [".png", ".html"]:
                    po_path = os.path.join(DATA_DIR, "po_records", f"{po_num}{ext}")
                    if os.path.exists(po_path):
                        rfq_item["po_screenshot"] = po_path
        except Exception:
            pass

        imported.append(rfq_item)

    # Replace RFQ line items with PC items
    r["line_items"] = imported
    r["linked_pc_id"] = pc_id
    r["linked_pc_number"] = pc.get("pc_number", "")
    r["linked_pc_match_reason"] = "manual_import"

    # Copy PC-level metadata to the RFQ
    r["source_pc"] = pc_id
    r["source_pc_number"] = pc.get("pc_number", "")
    r["source_pc_status"] = pc.get("status", "")
    r["source_pc_requestor"] = pc.get("requestor", "")

    # Copy source PDF if it exists
    source_file = pc.get("source_file", "")
    if source_file and os.path.exists(source_file):
        r["source_file"] = source_file
        r["pc_pdf_path"] = source_file

    _save_single_rfq(rid, r)

    # Copy attachments/files from PC to RFQ
    files_copied = 0
    try:
        from src.api.dashboard import list_rfq_files, get_rfq_file, save_rfq_file
        pc_files = list_rfq_files(pc_id)
        for pf in pc_files:
            full = get_rfq_file(pf["id"])
            if full and full.get("data"):
                save_rfq_file(
                    rfq_id=rid,
                    filename=pf["filename"],
                    file_type=pf.get("file_type", ""),
                    data=full["data"],
                    category=pf.get("category", "attachment"),
                    uploaded_by="import_from_pc",
                )
                files_copied += 1
        log.info("Copied %d files from PC %s to RFQ %s", len(pc_files), pc_id, rid)
    except Exception as _e:
        log.warning("File copy from PC %s to RFQ %s: %s", pc_id, rid, _e)

    return jsonify({
        "ok": True,
        "items_imported": len(imported),
        "files_copied": files_copied,
        "pc_number": pc.get("pc_number", ""),
        "priced": sum(1 for it in imported if it.get("supplier_cost") or it.get("price_per_unit")),
    })


@bp.route("/api/rfq/<rid>/import-from-catalog", methods=["POST"])
@auth_required
@safe_route
def api_rfq_import_from_catalog(rid):
    """Match RFQ line items against the product catalog and auto-fill pricing.
    For each item: search by description + part number → fill cost, bid, supplier, link.
    Does NOT replace items, only enriches existing ones with catalog data."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    items = r.get("line_items", [])
    if not items:
        return jsonify({"ok": False, "error": "RFQ has no line items"})

    try:
        from src.agents.product_catalog import match_item, init_catalog_db
        init_catalog_db()
    except Exception as e:
        return jsonify({"ok": False, "error": f"Catalog unavailable: {e}"})

    enriched = 0
    details = []
    for idx, item in enumerate(items):
        desc = (item.get("description") or "").strip()
        pn = str(item.get("item_number") or "").strip()
        if not desc and not pn:
            details.append({"idx": idx, "matched": False, "reason": "no description"})
            continue

        matches = match_item(desc, pn, top_n=1)
        if not matches or matches[0].get("match_confidence", 0) < 0.3:
            details.append({"idx": idx, "matched": False, "reason": "no catalog match",
                           "description": desc[:60]})
            continue

        best = matches[0]
        conf = best.get("match_confidence", 0)

        # Fill pricing from catalog — don't overwrite user-entered values
        catalog_cost = best.get("best_cost") or best.get("cost") or 0
        catalog_price = best.get("recommended_price") or best.get("sell_price") or 0
        catalog_supplier = best.get("best_supplier", "")
        catalog_pn = best.get("mfg_number") or best.get("sku") or ""

        changed = []
        if catalog_cost and not item.get("supplier_cost"):
            item["supplier_cost"] = round(float(catalog_cost), 2)
            changed.append(f"cost=${catalog_cost}")
        if catalog_price and not item.get("price_per_unit"):
            item["price_per_unit"] = round(float(catalog_price), 2)
            changed.append(f"bid=${catalog_price}")
        elif catalog_cost and not item.get("price_per_unit"):
            # Auto-calculate 25% markup if we have cost but no sell price
            item["price_per_unit"] = round(float(catalog_cost) * 1.25, 2)
            changed.append(f"bid=${item['price_per_unit']}(+25%)")
        if catalog_supplier and not item.get("item_supplier"):
            item["item_supplier"] = catalog_supplier
            changed.append(f"supplier={catalog_supplier}")
        if catalog_pn and not item.get("item_number"):
            item["item_number"] = catalog_pn
            changed.append(f"pn={catalog_pn}")

        # Copy product URL from catalog
        if not item.get("item_link"):
            for url_field in ["best_supplier_url", "product_url", "url", "amazon_url", "item_link"]:
                cat_url = best.get(url_field, "")
                if cat_url:
                    item["item_link"] = cat_url
                    changed.append(f"url={cat_url[:40]}")
                    break

        # Tag with catalog source
        item["_catalog_match"] = best.get("name", "")[:60]
        item["_catalog_confidence"] = round(conf, 2)
        item["_catalog_product_id"] = best.get("id")

        if changed:
            enriched += 1
            details.append({"idx": idx, "matched": True, "confidence": round(conf, 2),
                           "catalog_name": best.get("name", "")[:60], "filled": changed})
        else:
            details.append({"idx": idx, "matched": True, "confidence": round(conf, 2),
                           "catalog_name": best.get("name", "")[:60],
                           "filled": [], "note": "already had pricing"})

    _save_single_rfq(rid, r)

    return jsonify({
        "ok": True,
        "enriched": enriched,
        "total": len(items),
        "details": details,
    })


@bp.route("/api/rfq/<rid>/upload-pc", methods=["POST"])
@auth_required
@safe_route
def api_rfq_upload_pc(rid):
    """Upload a filled PC PDF → parse → verify/save to catalog → populate RFQ.
    
    Handles edge case: PC was filled outside app or data was lost.
    The filled PDF has all the pricing data — extract it, save to catalog,
    and populate the RFQ line items.
    """
    import os
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
    import re as _re_safe
    _safe_fn = _re_safe.sub(r'[^a-zA-Z0-9._-]', '_', os.path.basename(f.filename or 'upload.pdf'))
    pdf_path = os.path.join(upload_dir, f"pc_import_{rid}_{_safe_fn}")
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
        except (ValueError, TypeError):
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
                    init_catalog_db
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
    _save_single_rfq(rid, r)

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
@safe_route
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
            except Exception:
                result["counter"] = "app_settings table missing"

            # PCs with quote numbers
            try:
                pcs = conn.execute("SELECT id, quote_number, pc_number, status FROM price_checks WHERE quote_number IS NOT NULL AND quote_number != ''").fetchall()
                for p in pcs:
                    result["pcs_with_quotes"].append(dict(p))
            except Exception:
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
@safe_route
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
    sol = r.get("solicitation_number", "") or "RFQ"
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
        "required_forms": [
            # ── Separate standalone attachments ──────────────────────
            "703b",           # AMS 703B Bidder Info form
            "704b",           # AMS 704B Pricing worksheet
            "quote",          # Reytech formal quote on letterhead
            # ── RFQ Package: BidPackage template + extras ────────────
            # NOTE: bidpkg already contains CDCR Terms, CalRecycle 74,
            # CUF MC-345, DVBE 843, GenAI AMS 708, Drug-Free STD 21,
            # Voluntary Stats PD 802.
            # Darfur Act is also embedded in bidpkg — no standalone needed.
            # Bidder Declaration (GSPD-05-105) is embedded in bidpkg — no standalone GSPD-05-106.
            "bidpkg",         # CDCR combined template (all forms above)
            "sellers_permit", # CA Seller's Permit (static copy)
        ],
        "optional_forms": [],
        # RULE: std204 is a SEPARATE standalone attachment, NOT in this package.
        # RULE: dvbe843/calrecycle74/drug_free/cuf_cchcs are inside bidpkg — never add here.
        "notes": "California Correctional Health Care Services. AMS 703B/704B + full supporting docs.",
    },
    "calvet": {
        "name": "Cal Vet / DVA",
        "match_patterns": ["CALVET", "CAL VET", "CVA", "VHC", "VETERANS"],
        "required_forms": ["quote", "calrecycle74", "bidder_decl", "dvbe843", "darfur_act", "cv012_cuf", "std204", "std1000", "sellers_permit"],
        "optional_forms": ["barstow_cuf"],
        "notes": "California Department of Veterans Affairs. No AMS 703B/704B — uses Reytech quote + compliance forms.",
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
        "optional_forms": ["bidder_decl", "darfur_act"],
        "notes": "California Department of Forestry and Fire Protection.",
    },
    "dsh": {
        "name": "DSH — Dept of State Hospitals",
        "match_patterns": ["DSH", "STATE HOSPITAL", "NAPA STATE",
                          "ATASCADERO", "PATTON", "COALINGA",
                          "METROPOLITAN"],
        "required_forms": ["703b", "704b", "quote", "bidpkg", "sellers_permit", "genai_708"],
        "optional_forms": ["std205", "w9"],
        "notes": "Uses same AMS forms as CCHCS/CDCR.",
    },
    "cdfa": {
        "name": "CDFA — Dept of Food & Agriculture",
        "match_patterns": ["CDFA", "FOOD AND AGRICULTURE", "FOOD & AGRICULTURE"],
        "required_forms": ["quote", "std204", "sellers_permit", "bidder_decl", "darfur_act"],
        "optional_forms": ["dvbe843", "obs_1600", "w9"],
        "notes": "DGS-style forms.",
    },
    "dca": {
        "name": "DCA — Dept of Consumer Affairs",
        "match_patterns": ["DCA", "CONSUMER AFFAIRS"],
        "required_forms": ["quote", "std204", "sellers_permit", "bidder_decl", "darfur_act"],
        "optional_forms": ["dvbe843", "w9"],
        "notes": "Standard DGS forms.",
    },
    "chp": {
        "name": "CHP — CA Highway Patrol",
        "match_patterns": ["CHP", "HIGHWAY PATROL"],
        "required_forms": ["quote", "std204", "sellers_permit", "dvbe843", "bidder_decl", "darfur_act"],
        "optional_forms": ["w9"],
        "notes": "DGS-standard procurement forms.",
    },
    "edd": {
        "name": "EDD — Employment Development Dept",
        "match_patterns": ["EDD", "EMPLOYMENT DEVELOPMENT"],
        "required_forms": ["quote", "std204", "sellers_permit", "bidder_decl"],
        "optional_forms": ["dvbe843", "darfur_act", "w9"],
        "notes": "Standard DGS forms.",
    },
    "judicial": {
        "name": "Judicial Branch",
        "match_patterns": ["JUDICIAL", "SUPERIOR COURT", "COURTS"],
        "required_forms": ["quote", "sellers_permit"],
        "optional_forms": ["std204", "w9"],
        "notes": "Judicial branch has own procurement. Minimal forms.",
    },
    "other": {
        "name": "Other / Unknown",
        "match_patterns": [],
        "required_forms": ["quote", "std204", "sellers_permit"],
        "optional_forms": ["dvbe843", "bidder_decl"],
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
@safe_page
def agency_package_settings():
    """Agency RFQ Package Settings page."""
    configs = _load_agency_configs()
    return render_page("agency_packages.html", active_page="Agents",
                       configs=configs, available_forms=AVAILABLE_FORMS)


@bp.route("/api/agency-configs")
@auth_required
@safe_route
def api_agency_configs():
    """Get all agency package configs."""
    return jsonify({"ok": True, "configs": _load_agency_configs(), "available_forms": AVAILABLE_FORMS})


@bp.route("/api/agency-config/<key>", methods=["POST"])
@auth_required
@safe_route
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
@safe_route
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


@bp.route("/api/agency-config-reset", methods=["GET", "POST"])
@auth_required
@safe_route
def api_agency_config_reset():
    """Force-wipe and re-seed all agency configs from DEFAULT_AGENCY_CONFIGS.
    Use this after a DB rebuild to ensure configs match current defaults."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS agency_package_configs (
                agency_key TEXT PRIMARY KEY, agency_name TEXT NOT NULL,
                match_patterns TEXT DEFAULT '[]', required_forms TEXT DEFAULT '[]',
                optional_forms TEXT DEFAULT '[]', notes TEXT DEFAULT '',
                updated_at TEXT, updated_by TEXT DEFAULT 'system'
            )""")
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


# ══ Consolidated from routes_features*.py ══════════════════════════════════


# ── System Dashboard (from routes_features.py) ────────────────────────────

@bp.route("/api/system/dashboard")
@auth_required
@safe_route
def api_system_dashboard():
    """System health: disk, memory, uptime, data stats."""
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
            try:
                st = os.statvfs("/")
                disk_info = {"total_gb": round(st.f_blocks * st.f_frsize / (1024**3), 1),
                             "free_gb": round(st.f_bavail * st.f_frsize / (1024**3), 1)}
            except Exception:
                disk_info = {"note": "psutil not available"}
            mem_info = {"note": "psutil not available"}

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
@safe_route
def api_system_error_log():
    """Recent application errors from logs."""
    errors = []
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
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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


@bp.route("/api/system/metrics")
@auth_required
@safe_route
def api_system_metrics():
    """System performance metrics."""
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
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
    try:
        files = os.listdir(data_dir)
        metrics["data_files"] = len(files)
        total_size = sum(os.path.getsize(os.path.join(data_dir, f)) for f in files if os.path.isfile(os.path.join(data_dir, f)))
        metrics["data_size_mb"] = round(total_size / 1024 / 1024, 2)
    except Exception:
        pass
    return jsonify(metrics)


@bp.route("/api/system/recent-errors")
@auth_required
@safe_route
def api_system_recent_errors_trace():
    """Recent application errors with context."""
    errors = []
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
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


@bp.route("/api/system/diagnostic-sweep")
@auth_required
@safe_route
def api_diagnostic_sweep():
    """Comprehensive diagnostic sweep of all systems."""
    results = {"ok": True, "timestamp": datetime.now().isoformat(), "checks": {}}

    # Database
    try:
        import sqlite3 as _sq; from src.core.db import DB_PATH as _dbp; conn = _sq.connect(_dbp, timeout=30); conn.row_factory = _sq.Row
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        results["checks"]["database"] = {"ok": True, "tables": len(tables), "table_list": tables}
        counts = {}
        for t in tables[:20]:
            try:
                counts[t] = conn.execute("SELECT COUNT(*) FROM [" + re.sub(r"[^a-zA-Z0-9_]", "", t) + "]").fetchone()[0]
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
        env_status[v] = "set" if val else "missing"
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


@bp.route("/api/dashboard/kpis")
@auth_required
@safe_route
def api_dashboard_kpis():
    """Key performance indicators -- single-call business health."""
    try:
        import sqlite3 as _sq; from src.core.db import DB_PATH as _dbp; conn = _sq.connect(_dbp, timeout=30); conn.row_factory = _sq.Row
        kpis = {}
        kpis["total_quotes"] = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_test=0").fetchone()[0]
        kpis["quotes_this_month"] = conn.execute(
            "SELECT COUNT(*) FROM quotes WHERE is_test=0 AND created_at >= date('now','start of month')").fetchone()[0]
        won = conn.execute("SELECT SUM(total) FROM quotes WHERE is_test=0 AND status='won'").fetchone()[0]
        kpis["revenue_won"] = float(won or 0)
        pipeline = conn.execute("SELECT SUM(total) FROM quotes WHERE is_test=0 AND status IN ('sent','draft','priced','quoted')").fetchone()[0]
        kpis["pipeline_value"] = float(pipeline or 0)
        kpis["total_orders"] = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        kpis["total_pcs"] = conn.execute("SELECT COUNT(*) FROM price_checks").fetchone()[0]
        kpis["open_pcs"] = conn.execute("SELECT COUNT(*) FROM price_checks WHERE status NOT IN ('priced','completed','cancelled')").fetchone()[0]
        try:
            kpis["total_rfqs"] = conn.execute("SELECT COUNT(*) FROM rfqs").fetchone()[0]
            kpis["new_rfqs"] = conn.execute("SELECT COUNT(*) FROM rfqs WHERE status='new'").fetchone()[0]
        except Exception:
            kpis["total_rfqs"] = 0
            kpis["new_rfqs"] = 0
        try:
            kpis["crm_contacts"] = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
        except Exception:
            kpis["crm_contacts"] = 0
        won_count = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_test=0 AND status='won'").fetchone()[0]
        lost_count = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_test=0 AND status='lost'").fetchone()[0]
        total_decided = (won_count or 0) + (lost_count or 0)
        kpis["win_rate"] = round((won_count or 0) / total_decided * 100, 1) if total_decided > 0 else 0
        kpis["$2m_goal_pct"] = round(kpis["revenue_won"] / 2000000 * 100, 2)
        conn.close()
        return jsonify({"ok": True, **kpis})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/business-intel")
@auth_required
@safe_page
def business_intel_page():
    """Business Intelligence dashboard — visual metrics page."""
    from datetime import datetime as _dt
    _empty = {"bid_to_win": {"revenue_won": 0, "win_rate_pct": 0, "pipeline_value": 0, "avg_deal_size": 0, "total_bids": 0, "won": 0, "lost": 0, "cost_per_quote_est": 0},
              "customer_ltv": [], "competitors": [], "top_products": [], "head_to_head": [],
              "time_to_quote": {"avg_days": 0, "min_days": 0, "max_days": 0, "sample_size": 0},
              "monthly_trend": []}
    try:
        import sqlite3 as _bsq
        from src.core.db import DB_PATH as _bdbp
        _bconn = _bsq.connect(_bdbp, timeout=30)
        _bconn.row_factory = _bsq.Row
        bi = _build_bi_data(_bconn)
        _bconn.close()
    except Exception as _bi_err:
        log.error("BI page failed: %s", _bi_err, exc_info=True)
        bi = _empty
    return render_page("business_intel.html", active_page="BI", bi=bi, now=_dt.now().strftime("%Y-%m-%d %H:%M"))


def _build_bi_data(conn):
    """Build BI metrics dict from a DB connection. Used by both page and API."""
    bi = {}

    # ── 1. Cost of Sales / Bid-to-Win Ratio ──
    # Revenue from BOTH quotes (status=won) AND orders (paid/invoiced/delivered)
    total_quotes = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_test=0").fetchone()[0] or 0
    won_quotes = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_test=0 AND status='won'").fetchone()[0] or 0
    lost_quotes = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_test=0 AND status='lost'").fetchone()[0] or 0
    won_revenue_quotes = float(conn.execute("SELECT COALESCE(SUM(total),0) FROM quotes WHERE is_test=0 AND status='won'").fetchone()[0] or 0)
    # Also check orders table (primary revenue source)
    won_revenue_orders = 0
    try:
        won_revenue_orders = float(conn.execute(
            "SELECT COALESCE(SUM(total),0) FROM orders WHERE status IN ('paid','invoiced','delivered','shipped','active')"
        ).fetchone()[0] or 0)
    except Exception:
        pass
    # Also check revenue_log
    revenue_logged = 0
    try:
        revenue_logged = float(conn.execute(
            "SELECT COALESCE(SUM(amount),0) FROM revenue_log WHERE logged_at >= strftime('%Y-01-01', 'now')"
        ).fetchone()[0] or 0)
    except Exception:
        pass
    won_revenue = max(won_revenue_quotes, won_revenue_orders, revenue_logged)
    # Count won from orders too
    try:
        orders_count = conn.execute("SELECT COUNT(*) FROM orders WHERE status NOT IN ('cancelled','')").fetchone()[0] or 0
        won_quotes = max(won_quotes, orders_count)
    except Exception:
        pass
    pipeline_revenue = float(conn.execute("SELECT COALESCE(SUM(total),0) FROM quotes WHERE is_test=0 AND status IN ('sent','draft','priced')").fetchone()[0] or 0)
    bi["bid_to_win"] = {
        "total_bids": total_quotes,
        "won": won_quotes,
        "lost": lost_quotes,
        "win_rate_pct": round(won_quotes / (won_quotes + lost_quotes) * 100, 1) if (won_quotes + lost_quotes) > 0 else 0,
        "revenue_won": won_revenue,
        "pipeline_value": pipeline_revenue,
        "avg_deal_size": round(won_revenue / won_quotes, 2) if won_quotes > 0 else 0,
        "cost_per_quote_est": round(won_revenue * 0.05 / total_quotes, 2) if total_quotes > 0 else 0,  # ~5% overhead estimate
    }

    # ── 2. Customer Lifetime Value (by agency/institution) ──
    agency_rows = conn.execute("""
        SELECT agency, COUNT(*) as cnt, SUM(total) as rev,
               MIN(created_at) as first_quote, MAX(created_at) as last_quote,
               SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) as losses
        FROM quotes WHERE is_test=0 AND agency IS NOT NULL AND agency != ''
        GROUP BY agency ORDER BY rev DESC LIMIT 20
    """).fetchall()
    bi["customer_ltv"] = []
    for r in agency_rows:
        decided = (r[5] or 0) + (r[6] or 0)
        bi["customer_ltv"].append({
            "agency": r[0],
            "total_quotes": r[1],
            "total_revenue": float(r[2] or 0),
            "first_quote": r[3],
            "last_quote": r[4],
            "wins": r[5] or 0,
            "losses": r[6] or 0,
            "win_rate_pct": round((r[5] or 0) / decided * 100, 1) if decided > 0 else 0,
            "avg_deal": round(float(r[2] or 0) / r[1], 2) if r[1] > 0 else 0,
        })

    # ── 3. Competitor Profiles (from SCPRS vendor data) ──
    bi["competitors"] = []
    try:
        comp_rows = conn.execute("""
            SELECT supplier, COUNT(*) as po_count,
                   SUM(quantity * unit_price) as total_value,
                   COUNT(DISTINCT department) as agencies_served,
                   MIN(award_date) as first_seen, MAX(award_date) as last_seen,
                   ROUND(AVG(unit_price), 2) as avg_price
            FROM won_quotes
            WHERE supplier IS NOT NULL AND supplier != '' AND source != 'pc_vendor_cost'
            GROUP BY supplier ORDER BY total_value DESC LIMIT 15
        """).fetchall()
        for r in comp_rows:
            bi["competitors"].append({
                "vendor": r[0],
                "po_count": r[1],
                "total_value": float(r[2] or 0),
                "agencies_served": r[3],
                "avg_price": float(r[6] or 0),
                "first_seen": r[4],
                "last_seen": r[5],
            })
        # Fallback: if won_quotes has no competitor data, try scprs_po_master directly
        if not bi["competitors"]:
            try:
                comp_rows2 = conn.execute("""
                    SELECT supplier, COUNT(DISTINCT po_number) as po_count,
                           SUM(grand_total) as total_value,
                           COUNT(DISTINCT agency_key) as agencies_served,
                           MIN(start_date) as first_seen, MAX(start_date) as last_seen
                    FROM scprs_po_master
                    WHERE supplier IS NOT NULL AND supplier != ''
                      AND LOWER(supplier) NOT LIKE '%reytech%'
                    GROUP BY LOWER(supplier) ORDER BY total_value DESC LIMIT 15
                """).fetchall()
                for r in comp_rows2:
                    bi["competitors"].append({
                        "vendor": r[0],
                        "po_count": r[1],
                        "total_value": float(r[2] or 0),
                        "agencies_served": r[3],
                        "avg_price": 0,
                        "first_seen": r[4],
                        "last_seen": r[5],
                    })
            except Exception:
                pass
    except Exception:
        pass

    # ── 3b. Win/Loss Against Competitors ──
    # Cross-reference our lost quotes with SCPRS awards to find who beat us
    bi["head_to_head"] = []
    try:
        # Find departments where we lost, then see who won the award
        h2h_rows = conn.execute("""
            SELECT wq.supplier as winner, COUNT(DISTINCT q.quote_number) as times_beat_us,
                   ROUND(AVG(wq.unit_price), 2) as their_avg_price,
                   ROUND(AVG(q.total / NULLIF(q.items_count, 0)), 2) as our_avg_price,
                   GROUP_CONCAT(DISTINCT wq.department) as departments
            FROM quotes q
            JOIN won_quotes wq ON (
                wq.department = q.agency
                AND wq.award_date >= q.created_at
                AND wq.award_date <= date(q.created_at, '+60 days')
                AND wq.source != 'pc_vendor_cost'
            )
            WHERE q.is_test = 0 AND q.status = 'lost'
              AND wq.supplier IS NOT NULL AND wq.supplier != ''
              AND wq.supplier NOT LIKE '%reytech%'
            GROUP BY wq.supplier
            HAVING times_beat_us >= 1
            ORDER BY times_beat_us DESC LIMIT 10
        """).fetchall()
        for r in h2h_rows:
            bi["head_to_head"].append({
                "competitor": r[0],
                "times_beat_us": r[1],
                "their_avg_price": float(r[2] or 0),
                "our_avg_price": float(r[3] or 0),
                "departments": (r[4] or "")[:60],
            })
    except Exception:
        pass

    # ── 4. Time-to-Quote SLA ──
    ttq_rows = conn.execute("""
        SELECT
            ROUND(AVG(JULIANDAY(sent_at) - JULIANDAY(created_at)), 1) as avg_days,
            ROUND(MIN(JULIANDAY(sent_at) - JULIANDAY(created_at)), 1) as min_days,
            ROUND(MAX(JULIANDAY(sent_at) - JULIANDAY(created_at)), 1) as max_days,
            COUNT(*) as count
        FROM quotes
        WHERE is_test=0 AND sent_at IS NOT NULL AND sent_at != ''
          AND created_at IS NOT NULL AND created_at != ''
    """).fetchone()
    bi["time_to_quote"] = {
        "avg_days": float(ttq_rows[0] or 0),
        "min_days": float(ttq_rows[1] or 0),
        "max_days": float(ttq_rows[2] or 0),
        "sample_size": ttq_rows[3] or 0,
    }

    # ── 5. Monthly Revenue Trend ──
    trend_rows = conn.execute("""
        SELECT strftime('%Y-%m', created_at) as month,
               COUNT(*) as quotes,
               SUM(CASE WHEN status='won' THEN total ELSE 0 END) as won_rev,
               SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) as losses
        FROM quotes WHERE is_test=0 AND created_at >= date('now', '-12 months')
        GROUP BY month ORDER BY month
    """).fetchall()
    bi["monthly_trend"] = [
        {"month": r[0], "quotes": r[1], "won_revenue": float(r[2] or 0),
         "wins": r[3] or 0, "losses": r[4] or 0}
        for r in trend_rows
    ]

    # ── 6. Top Products (from won quotes) ──
    try:
        prod_rows = conn.execute("""
            SELECT description, COUNT(*) as times_won,
                   ROUND(AVG(unit_price), 2) as avg_price,
                   SUM(quantity) as total_qty
            FROM won_quotes
            WHERE unit_price > 0 AND source = 'pc_vendor_cost'
            GROUP BY SUBSTR(description, 1, 60)
            ORDER BY times_won DESC LIMIT 10
        """).fetchall()
        bi["top_products"] = [
            {"description": r[0][:60], "times_won": r[1],
             "avg_price": float(r[2] or 0), "total_qty": r[3] or 0}
            for r in prod_rows
        ]
    except Exception:
        bi["top_products"] = []

    return bi


@bp.route("/api/analytics/business-intel")
@auth_required
@safe_route
def api_business_intel():
    """Comprehensive business intelligence metrics API."""
    try:
        import sqlite3 as _bi_sq
        from src.core.db import DB_PATH as _bi_dbp
        conn = _bi_sq.connect(_bi_dbp, timeout=30)
        conn.row_factory = _bi_sq.Row
        bi = _build_bi_data(conn)
        conn.close()
        return jsonify({"ok": True, **bi})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/workflow/history")
@auth_required
@safe_route
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


@bp.route("/api/export/json", methods=["POST"])
@auth_required
@safe_route
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


# ── Data Quality (from routes_features.py) ─────────────────────────────────

@bp.route("/api/data-quality/duplicates")
@auth_required
@safe_route
def api_data_quality_duplicates():
    """Find duplicate contacts/vendors in CRM."""
    try:
        from src.core.db import get_all_contacts
        _crm_dict = get_all_contacts()
        contacts = list(_crm_dict.values()) if _crm_dict else []
    except Exception:
        return jsonify({"ok": True, "duplicates": [], "count": 0})
    try:

        by_email = defaultdict(list)
        for c in contacts:
            email = (c.get("email") or "").lower().strip()
            if email:
                by_email[email].append(c.get("display_name") or c.get("qb_name") or "Unknown")

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
@safe_route
def api_data_quality_missing_data():
    """Find records with incomplete data."""
    issues = []
    if True:
        try:
            from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
            no_total = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_test=0 AND (total IS NULL OR total = 0)").fetchone()[0]
            no_inst = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_test=0 AND (institution IS NULL OR institution = '')").fetchone()[0]
            no_items = conn.execute("""
                SELECT COUNT(*) FROM quotes q
                WHERE q.is_test=0 AND NOT EXISTS (SELECT 1 FROM quote_items qi WHERE qi.quote_number = q.quote_number)
            """).fetchone()[0]
            conn.close()
            if no_total: issues.append({"type": "quotes", "issue": f"{no_total} quotes with $0 total"})
            if no_inst: issues.append({"type": "quotes", "issue": f"{no_inst} quotes missing institution"})
            if no_items: issues.append({"type": "quotes", "issue": f"{no_items} quotes with no line items"})
        except Exception:
            pass

    try:
        from src.core.db import get_all_contacts
        _contacts = list(get_all_contacts().values())
        no_email = sum(1 for c in _contacts if not c.get("buyer_email"))
        no_phone = sum(1 for c in _contacts if not c.get("buyer_phone"))
        if no_email: issues.append({"type": "crm", "issue": f"{no_email} contacts missing email"})
        if no_phone: issues.append({"type": "crm", "issue": f"{no_phone} contacts missing phone"})
    except Exception:
        pass

    return jsonify({"ok": True, "issues": issues, "count": len(issues)})


@bp.route("/api/data-quality/orphaned-quotes")
@auth_required
@safe_route
def api_data_quality_orphaned_quotes():
    """Find quotes not linked to any CRM contact."""
    from src.core.db import get_db
    try:
        crm_institutions = set()
        try:
            from src.core.db import get_all_contacts
            for c in get_all_contacts().values():
                name = (c.get("buyer_name") or "").lower()
                if name:
                    crm_institutions.add(name)
        except Exception:
            pass

        with get_db() as conn:
            quotes = conn.execute("SELECT quote_number, institution, total, status FROM quotes WHERE is_test=0").fetchall()

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


# ── System Heartbeat & Morning Brief (from routes_features2.py) ────────────

@bp.route("/api/system/heartbeat")
@auth_required
@safe_route
def api_system_heartbeat():
    """Quick health check of all major subsystems in one call."""
    results = {"ok": True, "timestamp": datetime.now().isoformat(), "systems": {}}

    try:
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        ct = conn.execute("SELECT COUNT(*) FROM rfq_records").fetchone()[0]
        conn.close()
        results["systems"]["database"] = {"status": "ok", "rfq_count": ct}
    except Exception as e:
        results["systems"]["database"] = {"status": "error", "error": str(e)}

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

    try:
        from src.core.db import get_all_contacts
        _crm = get_all_contacts()
        results["systems"]["crm"] = {"status": "ok", "contacts": len(_crm)} if _crm else {"status": "empty"}
    except Exception as e:
        results["systems"]["crm"] = {"status": "error", "error": str(e)}

    try:
        from src.agents.quickbooks_agent import is_configured, get_access_token
        if is_configured():
            token = get_access_token()
            results["systems"]["quickbooks"] = {"status": "connected" if token else "token_expired",
                                                "configured": True}
        else:
            results["systems"]["quickbooks"] = {"status": "not_configured"}
    except Exception:
        results["systems"]["quickbooks"] = {"status": "unavailable"}

    try:
        results["systems"]["email"] = {
            "status": "configured" if os.environ.get("EMAIL_USER") else "not_configured"
        }
    except Exception:
        pass

    all_ok = all(s.get("status") in ("ok", "connected", "configured") for s in results["systems"].values())
    results["overall"] = "healthy" if all_ok else "degraded"
    return jsonify(results)


@bp.route("/api/dashboard/morning-brief")
@auth_required
@safe_route
def api_dashboard_morning_brief():
    """One-call consolidated morning briefing: key metrics, alerts, and actions."""
    brief = {"ok": True, "generated": datetime.now().isoformat(), "sections": {}}

    try:
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
    except Exception:
        pass

    try:
        cat_path = os.path.join(DATA_DIR, "product_catalog.json")
        if os.path.exists(cat_path):
            with open(cat_path) as f:
                cat = json.load(f)
            brief["sections"]["catalog"] = {
                "total_products": len(cat.get("products", [])),
                "with_pricing": sum(1 for p in cat.get("products", []) if p.get("avg_sell_price"))
            }
    except Exception:
        pass

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
    except Exception:
        pass

    actions = []
    pipeline = brief["sections"].get("pipeline", {})
    if pipeline.get("overdue", 0) > 0:
        actions.append(f"{pipeline['overdue']} overdue RFQs need attention")
    if pipeline.get("pending_action", 0) > 5:
        actions.append(f"{pipeline['pending_action']} quotes pending action")
    financial = brief["sections"].get("financial", {})
    if financial.get("overdue", 0) > 0:
        actions.append(f"${financial['overdue']:,.0f} in overdue invoices")
    outreach = brief["sections"].get("outreach", {})
    if outreach.get("follow_ups_due", 0) > 0:
        actions.append(f"{outreach['follow_ups_due']} outreach follow-ups due")

    brief["actions_needed"] = actions
    brief["action_count"] = len(actions)
    return jsonify(brief)


# ── Daily Wins, Health Score, Notifications, CSV Export (from routes_features3.py) ──

@bp.route("/api/daily-wins")
@auth_required
@safe_route
def api_daily_wins():
    """Today's wins: new quotes, orders, payments, won bids."""
    today = datetime.now().strftime("%Y-%m-%d")
    wins = []

    try:
        orders = _load_orders()
        for oid, o in orders.items():
            if (o.get("created_at") or o.get("created", "")).startswith(today):
                wins.append({"type": "New Order", "detail": f"PO {o.get('po_number', oid)}",
                            "value": o.get("total", 0), "time": o.get("created_at") or o.get("created", "")})
    except Exception: pass

    wl_path = os.path.join(DATA_DIR, "win_loss_log.json")
    if os.path.exists(wl_path):
        try:
            with open(wl_path) as f:
                wl = json.load(f)
            for entry in wl.get("entries", []):
                if entry.get("outcome") == "won" and entry.get("date", "").startswith(today):
                    wins.append({"type": "Won Quote", "detail": entry.get("rfq_id", "?"),
                                "value": entry.get("amount", 0), "time": entry.get("date", "")})
        except Exception: pass

    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    if os.path.exists(rfqs_path):
        try:
            with open(rfqs_path) as f:
                rfqs = json.load(f)
            for rid, r in rfqs.items():
                if r.get("status") == "sent" and r.get("sent_date", "").startswith(today):
                    wins.append({"type": "Sent Quote", "detail": f"Sol# {r.get('solicitation_number', rid)[:20]}",
                                "value": r.get("total_price", 0), "time": r.get("sent_date", "")})
        except Exception: pass

    total_value = sum(w.get("value", 0) for w in wins if isinstance(w.get("value"), (int, float)))

    return jsonify({
        "ok": True,
        "date": today,
        "wins": sorted(wins, key=lambda w: w.get("time", ""), reverse=True),
        "total_wins": len(wins),
        "total_value": round(total_value, 2),
        "message": f"{len(wins)} wins today!" if wins else "No wins yet today -- keep pushing!"
    })


def _get_health_recommendations(factors):
    """Generate recommendations based on health score factors."""
    recs = []
    for f in factors:
        if f["score"] < f["max"] * 0.5:
            if f["name"] == "Active Pipeline":
                recs.append("Pipeline is thin -- check inbox for new RFQs or run SCPRS deep pull")
            elif f["name"] == "Win Rate":
                recs.append("Track win/loss outcomes on RFQ detail pages to improve your rate")
            elif f["name"] == "Catalog":
                recs.append("Run 'Rebuild Catalog from History' to auto-populate from quotes")
            elif f["name"] == "QuickBooks":
                recs.append("Connect QuickBooks for financial tracking & invoice creation")
            elif f["name"] == "Follow-Ups":
                recs.append("You have overdue follow-ups -- check Follow-Up page")
            elif f["name"] == "CRM Contacts":
                recs.append("Sync QB customers to CRM to build your contact database")
    return recs


@bp.route("/api/business/health-score")
@auth_required
@safe_route
def api_business_health_score():
    """Calculate overall business health score (0-100)."""
    score = 0
    factors = []

    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    try:
        with open(rfqs_path) as f:
            rfqs = json.load(f)
        active = [r for r in rfqs.values() if (r.get("status") or "").lower() in ("new", "draft", "priced", "sent", "quoted")]
        pts = min(20, len(active) * 4)
        score += pts
        factors.append({"name": "Active Pipeline", "score": pts, "max": 20, "detail": f"{len(active)} active RFQs"})
    except Exception:
        factors.append({"name": "Active Pipeline", "score": 0, "max": 20, "detail": "No data"})

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
    except Exception:
        score += 10
        factors.append({"name": "Win Rate", "score": 10, "max": 20, "detail": "No data"})

    cat_path = os.path.join(DATA_DIR, "product_catalog.json")
    try:
        with open(cat_path) as f:
            cat = json.load(f)
        products = cat.get("products", {})
        with_pricing = len([p for p in products.values() if p.get("last_quoted_price", 0) > 0])
        pts = min(15, len(products) // 50)
        score += pts
        factors.append({"name": "Catalog", "score": pts, "max": 15, "detail": f"{len(products)} products, {with_pricing} priced"})
    except Exception:
        factors.append({"name": "Catalog", "score": 0, "max": 15, "detail": "No data"})

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
    except Exception:
        factors.append({"name": "QuickBooks", "score": 0, "max": 15, "detail": "Module not available"})

    fu_path = os.path.join(DATA_DIR, "follow_up_state.json")
    try:
        with open(fu_path) as f:
            fu = json.load(f)
        overdue = len([f for f in fu.values() if isinstance(f, dict) and f.get("status") == "overdue"])
        pending = len([f for f in fu.values() if isinstance(f, dict) and f.get("status") == "pending"])
        pts = max(0, 15 - overdue * 3)
        score += pts
        factors.append({"name": "Follow-Ups", "score": pts, "max": 15, "detail": f"{pending} pending, {overdue} overdue"})
    except Exception:
        score += 10
        factors.append({"name": "Follow-Ups", "score": 10, "max": 15, "detail": "No data"})

    try:
        from src.core.db import get_all_contacts
        _contacts = list(get_all_contacts().values())
        with_email = len([c for c in _contacts if c.get("buyer_email")])
        pts = min(15, len(_contacts) // 5)
        score += pts
        factors.append({"name": "CRM Contacts", "score": pts, "max": 15, "detail": f"{len(_contacts)} contacts, {with_email} with email"})
    except Exception:
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


@bp.route("/api/notifications/smart")
@auth_required
@safe_route
def api_smart_notifications():
    """AI-generated notifications based on current state."""
    notifs = []
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")

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
    except Exception: pass

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
    except Exception: pass

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
    except Exception: pass

    sev_order = {"high": 0, "medium": 1, "low": 2}
    notifs.sort(key=lambda n: sev_order.get(n.get("severity"), 9))

    return jsonify({
        "ok": True,
        "notifications": notifs[:15],
        "count": len(notifs),
        "high": len([n for n in notifs if n["severity"] == "high"]),
        "medium": len([n for n in notifs if n["severity"] == "medium"]),
    })


@bp.route("/api/export/csv", methods=["POST"])
@auth_required
@safe_route
def api_export_csv():
    """Export JSON data as downloadable CSV."""
    data = request.get_json(force=True, silent=True) or {}
    rows = data.get("rows") or data.get("data") or data.get("results") or []
    filename = data.get("filename", "export.csv")

    if not rows or not isinstance(rows, list):
        return jsonify({"ok": False, "error": "No data to export. Provide {rows: [...]} or {results: [...]}"})

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

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ── API Key Management ───────────────────────────────────────────────────────

@bp.route("/api/settings/api-keys")
@auth_required
@safe_route
def api_list_keys():
    """List all API keys (admin only)."""
    from src.core.db import list_api_keys
    return jsonify({"ok": True, "keys": list_api_keys()})


@bp.route("/api/settings/api-keys", methods=["POST"])
@auth_required
@safe_route
def api_create_key():
    """Generate a new API key. Returns the raw key (shown once)."""
    data = request.get_json(silent=True) or {}
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"ok": False, "error": "name is required"})
    from src.core.db import generate_api_key
    actor = getattr(request, '_api_key', {}).get('name', request.authorization.username if request.authorization else 'system')
    raw_key = generate_api_key(name, created_by=actor)
    return jsonify({"ok": True, "key": raw_key, "name": name,
                    "warning": "Save this key now — it cannot be retrieved again"})


@bp.route("/api/settings/api-keys/<int:key_id>/revoke", methods=["POST"])
@auth_required
@safe_route
def api_revoke_key(key_id):
    """Revoke an API key."""
    from src.core.db import revoke_api_key
    revoke_api_key(key_id)
    return jsonify({"ok": True, "revoked": key_id})


# ── Incoming Webhook Receiver ────────────────────────────────────────────────

@bp.route("/api/webhook/inbound", methods=["POST"])
def api_webhook_inbound():
    """Generic incoming webhook for external integrations (n8n, Zapier, etc.).
    Validates HMAC signature if WEBHOOK_SECRET env var is set.
    POST JSON: {action: "submit_rfq"|"trigger_price_check"|..., data: {...}}
    """
    import hmac, hashlib
    secret = os.environ.get("WEBHOOK_SECRET", "")
    if not secret:
        log.warning("Webhook rejected — WEBHOOK_SECRET not configured (from %s)", request.remote_addr)
        return jsonify({"ok": False, "error": "Webhook not configured"}), 403
    sig = request.headers.get("X-Webhook-Signature", "")
    body = request.get_data()
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        log.warning("Webhook HMAC mismatch from %s", request.remote_addr)
        return jsonify({"ok": False, "error": "Invalid signature"}), 403

    data = request.get_json(silent=True) or {}
    action = data.get("action", "")
    payload = data.get("data", {})
    actor = data.get("actor", "webhook")

    if not action:
        return jsonify({"ok": False, "error": "action field required"})

    ACTIONS = {
        "submit_rfq": "_webhook_submit_rfq",
        "trigger_price_check": "_webhook_trigger_pc",
        "update_order_status": "_webhook_update_order",
        "get_pipeline_status": "_webhook_pipeline_status",
        "mark_order_shipped": "_webhook_mark_shipped",
    }

    handler_name = ACTIONS.get(action)
    if not handler_name:
        return jsonify({"ok": False, "error": f"Unknown action: {action}",
                        "available": list(ACTIONS.keys())})

    try:
        handler = globals().get(handler_name)
        if not handler:
            return jsonify({"ok": False, "error": f"Handler not implemented: {action}"})
        result = handler(payload, actor)
        return jsonify({"ok": True, "action": action, "result": result})
    except Exception as e:
        log.error("Webhook action %s failed: %s", action, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


def _webhook_pipeline_status(payload, actor):
    """Return current pipeline summary."""
    rfqs = {}
    try:
        rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
        if os.path.exists(rfqs_path):
            with open(rfqs_path) as f:
                rfqs = json.load(f)
    except Exception:
        pass
    statuses = {}
    for r in rfqs.values():
        s = (r.get("status") or "unknown").lower()
        statuses[s] = statuses.get(s, 0) + 1
    return {"total_rfqs": len(rfqs), "by_status": statuses}


def _webhook_submit_rfq(payload, actor):
    """Create a new RFQ from webhook data."""
    from src.core.task_queue import enqueue
    task_id = enqueue("submit_rfq", payload, actor=actor)
    return {"queued": True, "task_id": task_id}


def _webhook_trigger_pc(payload, actor):
    """Trigger a price check."""
    from src.core.task_queue import enqueue
    task_id = enqueue("trigger_price_check", payload, actor=actor)
    return {"queued": True, "task_id": task_id}


def _webhook_update_order(payload, actor):
    """Update order status via webhook."""
    from src.core.task_queue import enqueue
    task_id = enqueue("update_order_status", payload, actor=actor)
    return {"queued": True, "task_id": task_id}


def _webhook_mark_shipped(payload, actor):
    """Mark order as shipped via webhook."""
    from src.core.task_queue import enqueue
    task_id = enqueue("mark_order_shipped", payload, actor=actor)
    return {"queued": True, "task_id": task_id}


# ═══════════════════════════════════════════════════════════════════════
# Integration Settings (Layer 3 — webhook / SMS / base URL config)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/settings/integrations")
@auth_required
@safe_route
def api_get_integrations():
    """Get integration settings (webhook URLs, SMS config)."""
    import os
    from src.core.db import get_setting
    return jsonify({"ok": True, "settings": {
        "webhook_rfq_created_url": get_setting("webhook_rfq_created_url", ""),
        "webhook_order_updated_url": get_setting("webhook_order_updated_url", ""),
        "notify_phone": os.environ.get("NOTIFY_PHONE", get_setting("notify_phone", "")),
        "base_url": os.environ.get("BASE_URL", get_setting("base_url", "")),
    }})


@bp.route("/api/settings/integrations", methods=["POST"])
@auth_required
@safe_route
def api_save_integrations():
    """Save integration settings."""
    import os
    from src.core.db import set_setting
    data = request.get_json(silent=True) or {}
    for key in ("webhook_rfq_created_url", "webhook_order_updated_url", "notify_phone", "base_url"):
        if key in data:
            set_setting(key, data[key])
            # Also set as env var for current process
            env_key = key.upper()
            os.environ[env_key] = data[key]
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
# Pricing Intelligence Dashboard (Phase 5)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/pricing-intelligence")
@auth_required
@safe_page
def pricing_intelligence_page():
    """Pricing Intelligence Dashboard — 4-tab analytics."""
    from src.api.render import render_page
    return render_page("pricing_intelligence.html", active_page="Pricing Intel")


@bp.route("/api/pricing-intelligence/data")
@auth_required
@safe_route
def api_pricing_intelligence():
    """Combined data for all 4 tabs of the pricing intelligence dashboard."""
    from src.core.db import get_db
    result = {"ok": True}

    try:
        with get_db() as conn:
            # Tab 1: Scorecard
            scorecard = {}
            # Win rate from quotes
            row = conn.execute(
                "SELECT COUNT(*) as total, "
                "SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) as won, "
                "SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) as lost "
                "FROM quotes WHERE status IN ('won','lost','sent','pending_award')"
            ).fetchone()
            if row:
                total = row[0] or 0
                won = row[1] or 0
                lost = row[2] or 0
                decided = won + lost
                scorecard["total_quotes"] = total
                scorecard["won"] = won
                scorecard["lost"] = lost
                scorecard["win_rate"] = round(won / decided * 100, 1) if decided > 0 else 0

            # Win rate trend (monthly, last 12 months)
            trends = conn.execute("""
                SELECT strftime('%Y-%m', created_at) as month,
                       SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) as won,
                       SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) as lost
                FROM quotes WHERE status IN ('won','lost')
                AND created_at >= date('now', '-12 months')
                GROUP BY month ORDER BY month
            """).fetchall()
            scorecard["monthly_trends"] = [
                {"month": r[0], "won": r[1], "lost": r[2],
                 "win_rate": round(r[1] / max(r[1] + r[2], 1) * 100, 1)}
                for r in trends
            ]

            # Avg margin on wins
            margins = conn.execute("""
                SELECT AVG(CAST(json_extract(data_json, '$.profit_summary.margin_pct') AS REAL))
                FROM price_checks WHERE status='won' AND data_json IS NOT NULL
            """).fetchone()
            scorecard["avg_margin"] = round(margins[0], 1) if margins and margins[0] else 0

            result["scorecard"] = scorecard

            # Tab 2: Competitive Position
            position = []
            pos_rows = conn.execute("""
                SELECT name, category, sell_price, scprs_last_price, cost,
                       times_quoted, times_won, times_lost, win_rate
                FROM product_catalog
                WHERE sell_price > 0 AND scprs_last_price > 0
                ORDER BY ((sell_price - scprs_last_price) / NULLIF(scprs_last_price, 0)) DESC
                LIMIT 50
            """).fetchall()
            for r in pos_rows:
                gap = round((r[2] - r[3]) / r[3] * 100, 1) if r[3] > 0 else 0
                position.append({
                    "name": r[0], "category": r[1] or "",
                    "our_price": r[2], "market_price": r[3], "cost": r[4] or 0,
                    "gap_pct": gap, "times_quoted": r[5] or 0,
                    "times_won": r[6] or 0, "times_lost": r[7] or 0,
                    "win_rate": r[8] or 0,
                })
            below = sum(1 for p in position if p["gap_pct"] < -5)
            at_market = sum(1 for p in position if -5 <= p["gap_pct"] <= 5)
            above = sum(1 for p in position if p["gap_pct"] > 5)
            result["competitive_position"] = {
                "items": position, "below_market": below,
                "at_market": at_market, "above_market": above,
            }

            # Tab 3: Recommendation Accuracy
            accuracy = {}
            try:
                acc_row = conn.execute("""
                    SELECT COUNT(*) as total,
                           SUM(followed) as followed_count,
                           SUM(CASE WHEN outcome='won' AND followed=1 THEN 1 ELSE 0 END) as followed_won,
                           SUM(CASE WHEN outcome='won' AND followed=0 THEN 1 ELSE 0 END) as override_won,
                           SUM(CASE WHEN outcome='lost' AND followed=0 THEN 1 ELSE 0 END) as override_lost,
                           SUM(CASE WHEN outcome='lost' AND oracle_price < outcome_price THEN 1 ELSE 0 END) as missed_wins
                    FROM recommendation_audit WHERE outcome IN ('won','lost')
                """).fetchone()
                if acc_row and acc_row[0]:
                    t = acc_row[0]
                    followed = acc_row[1] or 0
                    accuracy["total"] = t
                    accuracy["follow_rate"] = round(followed / t * 100, 1) if t > 0 else 0
                    f_total = followed
                    f_won = acc_row[2] or 0
                    o_total = t - followed
                    o_won = acc_row[3] or 0
                    accuracy["followed_win_rate"] = round(f_won / max(f_total, 1) * 100, 1)
                    accuracy["override_win_rate"] = round(o_won / max(o_total, 1) * 100, 1)
                    accuracy["missed_wins"] = acc_row[5] or 0

                # Missed wins detail
                missed = conn.execute("""
                    SELECT description, oracle_price, user_price, outcome_price, pc_id
                    FROM recommendation_audit
                    WHERE outcome='lost' AND oracle_price > 0 AND outcome_price > 0
                    AND oracle_price < outcome_price
                    ORDER BY recorded_at DESC LIMIT 20
                """).fetchall()
                accuracy["missed_wins_detail"] = [
                    {"description": r[0], "oracle_price": r[1], "user_price": r[2],
                     "winner_price": r[3], "pc_id": r[4]}
                    for r in missed
                ]
            except Exception:
                accuracy = {"total": 0, "note": "No recommendation data yet — re-enrich PCs to start tracking"}
            result["accuracy"] = accuracy

            # Tab 4: Data Source Attribution
            attribution = {}
            try:
                # Count items by price_source from price_checks
                src_rows = conn.execute("""
                    SELECT json_extract(value, '$.pricing.price_source') as src,
                           COUNT(*) as cnt
                    FROM price_checks, json_each(json_extract(data_json, '$.items'))
                    WHERE data_json IS NOT NULL AND status IN ('won','lost','sent')
                    GROUP BY src ORDER BY cnt DESC
                """).fetchall()
                attribution["by_source"] = [{"source": r[0] or "manual", "count": r[1]} for r in src_rows]
            except Exception:
                attribution["by_source"] = []
            result["attribution"] = attribution

    except Exception as e:
        result["error"] = str(e)

    return jsonify(result)
