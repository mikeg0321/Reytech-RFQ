# routes_growth_prospects.py — Growth strategy, prospect management, campaigns
# Split from routes_intel.py for maintainability

# ── Explicit imports ──────────────────────────────────────────────────────────
from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from flask import redirect, flash
from src.core.paths import DATA_DIR
from src.core.db import get_db
from src.api.render import render_page

import os, json, threading
from datetime import datetime, timedelta
from collections import defaultdict

# ── Growth agent imports (duplicated from routes_intel.py — both exec into dashboard namespace) ──
try:
    from src.agents.growth_agent import (
        pull_reytech_history, find_category_buyers, launch_outreach,
        launch_distro_campaign,
        check_follow_ups, launch_voice_follow_up,
        get_growth_status, PULL_STATUS, BUYER_STATUS,
        get_prospect, update_prospect, add_prospect_note, mark_responded,
        process_bounceback, scan_inbox_for_bounces, detect_bounceback,
        get_campaign_dashboard, start_scheduler,
        generate_recommendations, full_report, lead_funnel,
    )
    GROWTH_AVAILABLE = True
except ImportError:
    GROWTH_AVAILABLE = False

try:
    from src.agents.sales_intel import (
        deep_pull_all_buyers, push_to_growth_prospects,
        DEEP_PULL_STATUS,
    )
    INTEL_AVAILABLE = True
except ImportError:
    INTEL_AVAILABLE = False

# ─── Growth Strategy Routes (v2.0 — SCPRS-driven) ──────────────────────────


@bp.route("/growth/prospect/<prospect_id>")
@auth_required
def growth_prospect_detail(prospect_id):
    """Full CRM contact detail — timeline, contact info, SCPRS data, activity log."""
    if not GROWTH_AVAILABLE:
        flash("Growth agent not available", "error"); return redirect("/contacts")
    result = get_prospect(prospect_id)
    
    # Fallback: check CRM contacts if not in growth prospects
    if not result.get("ok"):
        crm_contacts = _load_crm_contacts()
        crm_c = crm_contacts.get(prospect_id)
        if crm_c:
            # Synthesize a prospect object from CRM contact
            result = {
                "ok": True,
                "prospect": {
                    "id": prospect_id,
                    "buyer_name": crm_c.get("buyer_name", ""),
                    "buyer_email": crm_c.get("buyer_email", ""),
                    "buyer_phone": crm_c.get("buyer_phone", ""),
                    "agency": crm_c.get("agency", ""),
                    "categories": crm_c.get("categories", {}),
                    "items_purchased": crm_c.get("items_purchased", []),
                    "total_spend": crm_c.get("total_spend", 0),
                    "score": crm_c.get("score", 0),
                    "outreach_status": crm_c.get("outreach_status", "new"),
                    "po_count": crm_c.get("po_count", 0),
                    "last_purchase": crm_c.get("last_purchase", ""),
                    "purchase_orders": crm_c.get("purchase_orders", []),
                    "notes": crm_c.get("notes", ""),
                },
                "timeline": [],
                "outreach_records": [],
            }
    
    if not result.get("ok"):
        flash("Contact not found", "error"); return redirect("/contacts")

    pr = result["prospect"]
    timeline = result.get("timeline", [])
    outreach_recs = result.get("outreach_records", [])

    # Merge CRM activity log for this contact
    contact_email = pr.get("buyer_email", "")
    crm_events = _get_crm_activity(ref_id=prospect_id, limit=100)
    if contact_email:
        crm_events += _get_crm_activity(ref_id=contact_email, limit=50)
    crm_events = sorted(crm_events, key=lambda x: x.get("timestamp",""), reverse=True)[:100]

    # Combine all events
    event_icons = {
        "status_change":"🔄","email_sent":"📧","email_received":"📨","email_bounced":"⛔",
        "voice_called":"📞","sms_sent":"💬","chat":"💬","note":"📝","updated":"✏️",
        "response_received":"✅","won":"🏆","lost":"💀","follow_up":"⏰","meeting":"🤝",
        "quote_sent":"📋","quote_won":"✅","lead_scored":"⭐",
    }
    all_events = []
    for ev in timeline[:50]:
        all_events.append({"ts":ev.get("timestamp",""),"type":ev.get("type","event"),
                           "detail":ev.get("detail",""),"actor":"system","source":"growth"})
    for ev in crm_events:
        all_events.append({"ts":ev.get("timestamp",""),"type":ev.get("event_type","event"),
                           "detail":ev.get("description",""),"actor":ev.get("actor","system"),
                           "source":"crm","metadata":ev.get("metadata",{})})
    all_events.sort(key=lambda x: x.get("ts",""), reverse=True)

    tl_html = ""
    for ev in all_events[:80]:
        ts = ev.get("ts","")[:16].replace("T"," ")
        icon = event_icons.get(ev.get("type",""), "•")
        etype = ev.get("type","").replace("_"," ").title()
        detail = ev.get("detail","")
        actor = ev.get("actor","")
        actor_badge = f'<span style="font-size:13px;padding:1px 6px;border-radius:8px;background:rgba(79,140,255,.15);color:var(--ac);margin-left:4px">{actor}</span>' if actor and actor != "system" else ""
        meta = ev.get("metadata",{})
        meta_html = ""
        if meta.get("amount"): meta_html += f' · <span style="color:#3fb950">${float(meta["amount"]):,.0f}</span>'
        if meta.get("subject"): meta_html += f' · <i style="color:var(--tx2)">{str(meta["subject"])[:50]}</i>'
        tl_html += f'<div style="display:flex;gap:10px;padding:10px 0;border-bottom:1px solid rgba(46,51,69,.5)"><span style="font-size:18px;flex-shrink:0;width:24px;text-align:center">{icon}</span><div style="flex:1;min-width:0"><div style="font-size:14px;font-weight:600;display:flex;align-items:center;gap:4px">{etype}{actor_badge}</div><div style="font-size:14px;color:var(--tx2);margin-top:2px;word-break:break-word">{detail}{meta_html}</div></div><span style="font-size:13px;color:var(--tx2);font-family:monospace;white-space:nowrap;flex-shrink:0">{ts}</span></div>'
    if not tl_html:
        tl_html = '<div style="color:var(--tx2);font-size:13px;padding:16px;text-align:center">No activity yet — log a call, email, or note above</div>'

    # PO history
    po_html = ""
    for po in pr.get("purchase_orders",[]):
        po_html += f'<tr><td class="mono" style="color:var(--ac)">{po.get("po_number","—")}</td><td class="mono">{po.get("date","—")}</td><td style="font-size:14px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{str(po.get("items","—"))[:80]}</td><td style="font-size:14px">{po.get("category","—")}</td><td class="mono" style="color:#3fb950;text-align:right">${po.get("total_num",0) or po.get("total",0) or 0:,.0f}</td></tr>'

    # Items purchased
    items_html = ""
    cat_colors = {"Medical":"#f87171","Janitorial":"#3fb950","Office":"#4f8cff","IT":"#a78bfa","Facility":"#fb923c","Safety":"#fbbf24"}
    for it in pr.get("items_purchased",[])[:20]:
        cc = cat_colors.get(it.get("category",""),"#8b90a0")
        up = f'<span style="font-size:14px;font-family:monospace;color:#3fb950">${float(it["unit_price"]):,.2f}</span>' if it.get("unit_price") else ""
        items_html += f'<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid rgba(46,51,69,.4)"><span style="font-size:13px;padding:2px 7px;border-radius:8px;background:{cc}22;color:{cc};border:1px solid {cc}44;white-space:nowrap">{it.get("category","General")}</span><span style="font-size:14px;flex:1">{it.get("description","")}</span>{up}</div>'

    # Categories breakdown
    cats_dict = pr.get("categories",{})
    # Handle categories_matched list format (from sales_intel source)
    if not cats_dict and pr.get("categories_matched"):
        cats_list = pr.get("categories_matched", [])
        total_spend_val = pr.get("total_spend", 0) or 0
        if isinstance(cats_list, list) and cats_list:
            per_cat = total_spend_val / len(cats_list) if total_spend_val else 0
            cats_dict = {c: per_cat for c in cats_list}
    cats_html = ""
    total_cat = sum(cats_dict.values()) or 1
    for cat, spend in sorted(cats_dict.items(), key=lambda x: x[1], reverse=True):
        pct = round(spend/total_cat*100)
        cc = cat_colors.get(cat,"#8b90a0")
        cats_html += f'<div style="margin-bottom:8px"><div style="display:flex;justify-content:space-between;font-size:14px;margin-bottom:3px"><span style="color:{cc};font-weight:600">{cat}</span><span class="mono">${spend:,.0f} ({pct}%)</span></div><div style="background:var(--sf2);border-radius:4px;height:6px;overflow:hidden"><div style="width:{pct}%;height:100%;background:{cc};border-radius:4px"></div></div></div>'

    # Outreach records
    or_html = ""
    for o in outreach_recs:
        flags = (''.join([
            '<span style="color:#3fb950">✅ Sent</span> ' if o.get("email_sent") else '<span style="color:var(--tx2)">⏳ Draft</span> ',
            '<span style="color:#f85149">⛔ Bounced</span> ' if o.get("bounced") else '',
            '<span style="color:#3fb950">✅ Replied</span> ' if o.get("response_received") else '',
            '<span style="color:#fb923c">📞 Called</span>' if o.get("voice_called") else '',
        ]))
        or_html += f'<div style="padding:10px;background:var(--sf2);border-radius:8px;margin-bottom:8px;font-size:14px"><div style="font-weight:600;margin-bottom:4px">{o.get("email_subject","—")}</div><div style="color:var(--tx2);display:flex;gap:12px;flex-wrap:wrap"><span>To: {o.get("email","—")}</span>{flags}</div></div>'

    stat = pr.get("outreach_status","new")
    sc = {"new":"#4f8cff","emailed":"#fbbf24","called":"#fb923c","responded":"#a78bfa","won":"#3fb950","lost":"#f87171","dead":"#8b90a0","bounced":"#f85149","follow_up_due":"#d29922"}
    stat_color = sc.get(stat,"#8b90a0")
    pid = pr.get("id","")
    agency = pr.get("agency","Unknown")
    total_spend = pr.get("total_spend",0) or 0
    po_count = pr.get("po_count",0) or len(pr.get("purchase_orders",[]))
    score = pr.get("score",0) or 0
    score_pct = round(score*100) if score<=1 else round(score)
    last_purchase = (pr.get("last_purchase","") or pr.get("last_po_date","") or "—")[:10]

    # Merge CRM contact data for tags, follow-up, etc.
    crm_contacts = _load_crm_contacts()
    crm_c = crm_contacts.get(pid, {})
    tags = crm_c.get("tags", []) or pr.get("tags", []) or []
    follow_up_date = crm_c.get("follow_up_date", "") or ""
    preferred_contact = crm_c.get("preferred_contact", "") or ""
    source = crm_c.get("source", pr.get("source", "auto"))
    created_at = crm_c.get("created_at", pr.get("created_at", ""))
    contact_email = pr.get("buyer_email", "")

    # Find linked quotes and PCs for this contact
    linked_quotes = []
    linked_pcs = []
    try:
        from src.forms.quote_generator import get_all_quotes
        all_quotes = get_all_quotes()
        for q in all_quotes:
            q_agency = (q.get("agency","") or q.get("institution","")).lower()
            q_email = (q.get("email","") or "").lower()
            if (contact_email and q_email == contact_email.lower()) or \
               (agency and agency.lower() != "unknown" and q_agency == agency.lower()):
                linked_quotes.append(q)
    except Exception:
        pass
    try:
        pcs = _load_price_checks() if '_load_price_checks' in dir() else {}
        if not pcs:
            from src.api.dashboard import _load_price_checks as _lpc2
            pcs = _lpc2()
        for pcid, pc in pcs.items():
            pc_inst = (pc.get("institution","") or "").lower()
            pc_req = (pc.get("requestor","") or "").lower()
            if (agency and agency.lower() != "unknown" and pc_inst == agency.lower()) or \
               (contact_email and contact_email.lower() in pc_req):
                linked_pcs.append({"id": pcid, **pc})
    except Exception:
        pass

    # Calculate response rate from activity
    activity_list = crm_c.get("activity", [])
    emails_sent = sum(1 for a in activity_list if a.get("event_type") == "email_sent")
    emails_received = sum(1 for a in activity_list if a.get("event_type") in ("email_received","response_received"))
    response_rate = round(emails_received / max(emails_sent, 1) * 100) if emails_sent else None

    # Last contacted
    last_contacted = ""
    for ev in sorted(activity_list, key=lambda x: x.get("timestamp",""), reverse=True):
        if ev.get("event_type") in ("email_sent","voice_called","chat","sms_sent"):
            last_contacted = ev.get("timestamp","")[:10]
            break

    # V3+: Win probability + workflow + calendar
    win_data = {}
    wf_state = {}
    prospect_cal = []
    try:
        from src.agents.growth_agent import get_win_probability, get_reytech_credentials, get_calendar_events
        _creds = get_reytech_credentials()
        win_data = get_win_probability(pr, _creds)
        wf_state = pr.get("workflow", {})
        _all_cal = get_calendar_events(upcoming_only=True)
        prospect_cal = [e for e in _all_cal if e.get("prospect_id") == pid][:5]
    except Exception:
        pass

    return render_page("prospect_detail.html", active_page="CRM",
        pr=pr, pid=pid, agency=agency, total_spend=total_spend,
        po_count=po_count, score=score, score_pct=score_pct,
        last_purchase=last_purchase, stat=stat, stat_color=stat_color,
        tl_html=tl_html, po_html=po_html, items_html=items_html,
        cats_html=cats_html, or_html=or_html, all_events=all_events,
        tags=tags, follow_up_date=follow_up_date, preferred_contact=preferred_contact,
        source=source, created_at=created_at, linked_quotes=linked_quotes,
        linked_pcs=linked_pcs, response_rate=response_rate,
        last_contacted=last_contacted, emails_sent=emails_sent,
        win_data=win_data, wf_state=wf_state, prospect_cal=prospect_cal)
@bp.route("/api/growth/status")
@auth_required
def api_growth_status():
    """Full growth agent status — history, categories, prospects, outreach."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify(get_growth_status())


@bp.route("/api/growth/pull-history")
@auth_required
def api_growth_pull_history():
    """Step 1: Pull ALL Reytech POs from SCPRS (2022-present).
    Long-running — check /api/growth/pull-status for progress."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from_date = request.args.get("from", "01/01/2022")
    to_date = request.args.get("to", "")

    # Run in background thread
    import threading
    def _run():
        pull_reytech_history(from_date=from_date, to_date=to_date)
    t = threading.Thread(target=_run, daemon=True, name="growth-pull")
    t.start()
    return jsonify({"ok": True, "message": f"Pulling Reytech history from SCPRS ({from_date} → present). Check /api/growth/pull-status for progress."})


@bp.route("/api/growth/pull-status")
@auth_required
def api_growth_pull_status():
    """Check progress of Reytech history pull."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify({"ok": True, **PULL_STATUS})


@bp.route("/api/growth/find-buyers")
@auth_required
def api_growth_find_buyers():
    """Step 2: Search SCPRS for all buyers of Reytech's item categories.
    Requires Step 1 first. Long-running."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    try:
        max_cats = max(1, min(int(request.args.get("max_categories", 10)), 100))
    except (ValueError, TypeError, OverflowError):
        max_cats = 10
    from_date = request.args.get("from", "01/01/2024")

    import threading
    def _run():
        find_category_buyers(max_categories=max_cats, from_date=from_date)
    t = threading.Thread(target=_run, daemon=True, name="growth-buyers")
    t.start()
    return jsonify({"ok": True, "message": f"Searching SCPRS for buyers (top {max_cats} categories from {from_date}). Check /api/growth/buyer-status."})


@bp.route("/api/growth/intel-scrape", methods=["POST"])
@auth_required
def api_growth_intel_scrape():
    """3-Phase buyer intelligence scrape.
    POST params: year_from, year_to, max_per_phase"""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})

    from src.agents.growth_agent import run_buyer_intelligence, INTEL_STATUS
    from datetime import datetime as _dt
    if INTEL_STATUS.get("running"):
        return jsonify({"ok": False, "error": "Already running", "status": INTEL_STATUS})

    data = request.get_json(silent=True) or {}
    year_from = int(data.get("year_from", request.args.get("year_from", 2024)))
    year_to = int(data.get("year_to", request.args.get("year_to", _dt.now().year)))
    max_per = int(data.get("max_per_phase", request.args.get("max_per_phase", 30)))

    import threading
    def _run():
        run_buyer_intelligence(year_from=year_from, year_to=year_to, max_per_phase=max_per)
    t = threading.Thread(target=_run, daemon=True, name="growth-intel")
    t.start()

    return jsonify({
        "ok": True,
        "message": f"3-Phase intelligence scrape started ({year_from}–{year_to}). Check /api/growth/intel-status.",
    })


@bp.route("/api/growth/intel-status")
@auth_required
def api_growth_intel_status():
    """Check progress of buyer intelligence scrape."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_intel_status
    return jsonify({"ok": True, **get_intel_status()})


@bp.route("/api/growth/intel-results")
@auth_required
def api_growth_intel_results():
    """Get full intelligence results."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_intel_results
    return jsonify({"ok": True, "results": get_intel_results()})


@bp.route("/api/growth/buyer-status")
@auth_required
def api_growth_buyer_status():
    """Check progress of buyer search."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify({"ok": True, **BUYER_STATUS})


@bp.route("/api/growth/outreach")
@auth_required
def api_growth_outreach():
    """Step 3: Launch email outreach to prospects.
    ?dry_run=true (default) previews without sending.
    ?dry_run=false sends live emails."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    dry_run = request.args.get("dry_run", "true").lower() != "false"
    try:
        max_p = max(1, min(int(request.args.get("max", 50)), 500))
    except (ValueError, TypeError, OverflowError):
        max_p = 50
    return jsonify(launch_outreach(max_prospects=max_p, dry_run=dry_run))


@bp.route("/api/growth/follow-ups")
@auth_required
def api_growth_follow_ups():
    """Check which prospects need voice follow-up (3-5 days no response)."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify(check_follow_ups())


# ── PRD Feature 4.3 + Growth Campaign: Distro List Email Campaign ────────────
@bp.route("/api/growth/distro-campaign", methods=["GET", "POST"])
@auth_required
def api_growth_distro_campaign():
    """Phase 1 Growth Campaign — email CA state buyers to get on RFQ distro lists.

    GET: Preview campaign (dry_run=true, shows emails without sending)
    POST: Execute campaign
      Body: { dry_run: bool, max: int, template: str, source_filter: str }

    Templates: distro_list (default) | initial_outreach | follow_up
    """
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})

    from src.agents.growth_agent import launch_distro_campaign

    if request.method == "GET":
        dry_run = True
    else:
        body = request.get_json(silent=True) or {}
        dry_run = body.get("dry_run", True)

    args = request.get_json(silent=True) or {} if request.method == "POST" else {}
    try:
        max_c = max(1, min(int(request.args.get("max", args.get("max", 100))), 500))
    except (ValueError, TypeError, OverflowError):
        max_c = 100
    template = request.args.get("template", args.get("template", "distro_list"))
    source_filter = request.args.get("source", args.get("source_filter", ""))

    result = launch_distro_campaign(
        max_contacts=max_c,
        dry_run=dry_run,
        template=template,
        source_filter=source_filter,
    )
    return jsonify(result)


@bp.route("/api/growth/campaign-status")
@auth_required
def api_growth_campaign_status():
    """Get status of all growth campaigns including distro list campaign."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    try:
        from src.agents.growth_agent import get_campaign_dashboard, _load_json, OUTREACH_FILE
        dashboard = get_campaign_dashboard()
        outreach = _load_json(OUTREACH_FILE)
        if not isinstance(outreach, dict):
            outreach = {}
        campaigns = outreach.get("campaigns", [])
        distro_campaigns = [c for c in campaigns if c.get("type") == "distro_list_phase1"]
        total_distro_staged = sum(len(c.get("outreach", [])) for c in distro_campaigns)
        total_distro_sent = sum(
            sum(1 for o in c.get("outreach", []) if o.get("email_sent"))
            for c in distro_campaigns
        )
        return jsonify({
            "ok": True,
            "dashboard": dashboard,
            "distro_campaigns": {
                "count": len(distro_campaigns),
                "total_staged": total_distro_staged,
                "total_sent": total_distro_sent,
                "last_campaign": distro_campaigns[-1]["id"] if distro_campaigns else None,
            },
            "total_sent_all_campaigns": outreach.get("total_sent", 0),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/growth/voice-follow-up")
@auth_required
def api_growth_voice_follow_up():
    """Step 4: Auto-dial non-responders."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    try:
        max_calls = max(1, min(int(request.args.get("max", 10)), 100))
    except (ValueError, TypeError, OverflowError):
        max_calls = 10
    return jsonify(launch_voice_follow_up(max_calls=max_calls))


# Legacy growth routes (redirect to new status)
@bp.route("/api/growth/report")
@auth_required
def api_growth_report():
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify(get_growth_status())


@bp.route("/api/growth/recommendations")
@auth_required
def api_growth_recommendations():
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify(get_growth_status())


# ─── Growth CRM Routes ────────────────────────────────────────────────────

@bp.route("/api/growth/prospect/<prospect_id>")
@auth_required
def api_growth_prospect(prospect_id):
    """Get prospect detail with full timeline."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify(get_prospect(prospect_id))


@bp.route("/api/growth/prospect/<prospect_id>", methods=["POST"])
@auth_required
def api_growth_prospect_update(prospect_id):
    """Update prospect. POST JSON: {buyer_name, buyer_phone, outreach_status, notes, title, linkedin}"""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    data = request.get_json(silent=True) or {}

    # Sync CRM-only fields (title, linkedin, notes) to crm_contacts store
    crm_fields = {k: data[k] for k in ("title","linkedin","notes","buyer_name","buyer_phone","outreach_status") if k in data}
    if crm_fields:
        try:
            contacts = _load_crm_contacts()
            if prospect_id not in contacts:
                # Hydrate from prospect
                pr_result = get_prospect(prospect_id)
                if pr_result.get("ok"):
                    _get_or_create_crm_contact(prospect_id, pr_result["prospect"])
                    contacts = _load_crm_contacts()
            if prospect_id in contacts:
                for k, v in crm_fields.items():
                    contacts[prospect_id][k] = v
                contacts[prospect_id]["updated_at"] = datetime.now().isoformat()
                _save_crm_contacts(contacts)
        except Exception as e:
            log.warning(f"CRM sync for {prospect_id} failed: {e}")

    # Pass all fields to growth agent (it ignores unknown keys gracefully)
    return jsonify(update_prospect(prospect_id, data))


@bp.route("/api/growth/prospect/<prospect_id>/note", methods=["POST"])
@auth_required
def api_growth_prospect_note(prospect_id):
    """Add note to prospect. POST JSON: {note: "..."}"""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    data = request.get_json(silent=True) or {}
    return jsonify(add_prospect_note(prospect_id, data.get("note", "")))


@bp.route("/api/growth/prospect/<prospect_id>/responded", methods=["POST"])
@auth_required
def api_growth_prospect_responded(prospect_id):
    """Mark prospect as responded. POST JSON: {response_type, detail}"""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    data = request.get_json(silent=True) or {}
    return jsonify(mark_responded(prospect_id, data.get("response_type", "email_reply"), data.get("detail", "")))


@bp.route("/api/growth/bounceback", methods=["POST"])
@auth_required
def api_growth_bounceback():
    """Process a bounceback. POST JSON: {email, reason}"""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    data = request.get_json(silent=True) or {}
    email = data.get("email", "")
    if not email:
        return jsonify({"ok": False, "error": "email required"})
    return jsonify(process_bounceback(email, data.get("reason", "")))


@bp.route("/api/growth/scan-bounces")
@auth_required
def api_growth_scan_bounces():
    """Scan inbox for bounceback emails and auto-process them."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify(scan_inbox_for_bounces())


@bp.route("/api/growth/campaigns")
@auth_required
def api_growth_campaigns():
    """Campaign dashboard with metrics breakdown."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify(get_campaign_dashboard())


@bp.route("/api/growth/create-campaign", methods=["POST"])
@auth_required
def api_growth_create_campaign():
    """Create Campaign — full pipeline: pull history → find buyers → push to growth → preview emails.
    
    This is the one-button workflow that chains everything together.
    Runs steps in background thread so it doesn't block.
    """
    if not GROWTH_AVAILABLE or not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth + Intel agents required"})

    import threading

    # Campaign config from request
    data = request.get_json(silent=True) or {}
    max_prospects = data.get("max_prospects", 50)
    from_date = data.get("from_date", "01/01/2019")
    dry_run = data.get("dry_run", True)  # Default to preview mode

    def run_campaign():
        """Background: Pull → Mine → Push → Outreach."""
        try:
            from src.agents.growth_agent import (
                pull_reytech_history, find_category_buyers,
                launch_outreach, PULL_STATUS, BUYER_STATUS,
            )
            from src.agents.sales_intel import (
                deep_pull_all_buyers, push_to_growth_prospects,
                DEEP_PULL_STATUS,
            )

            # Step 1: Pull Reytech purchase history from SCPRS
            PULL_STATUS["running"] = True
            PULL_STATUS["progress"] = "Step 1/4: Pulling Reytech purchase history..."
            pull_reytech_history(from_date=from_date)
            PULL_STATUS["progress"] = "Step 1 done."
            PULL_STATUS["running"] = False

            # Step 2: Find all buyers who buy same items from competitors
            BUYER_STATUS["running"] = True
            BUYER_STATUS["progress"] = "Step 2/4: Mining SCPRS for all buyers..."
            find_category_buyers(from_date=from_date)
            BUYER_STATUS["progress"] = "Step 2 done."
            BUYER_STATUS["running"] = False

            # Step 3: Deep pull from Sales Intel for scoring + agency data
            DEEP_PULL_STATUS["running"] = True
            DEEP_PULL_STATUS["progress"] = "Step 3/4: Deep pull — scoring buyers & agencies..."
            deep_pull_all_buyers(from_date=from_date)
            DEEP_PULL_STATUS["running"] = False

            # Step 4: Push top prospects to growth pipeline + preview outreach
            PULL_STATUS["running"] = True
            PULL_STATUS["progress"] = f"Step 4/4: Pushing top {max_prospects} prospects to growth pipeline..."
            push_to_growth_prospects(top_n=max_prospects)

            if not dry_run:
                PULL_STATUS["progress"] = "Step 4/4: Sending outreach emails..."
                launch_outreach(max_prospects=max_prospects, dry_run=False)

            PULL_STATUS["progress"] = "✅ Campaign complete! Refresh page to see results."
            PULL_STATUS["running"] = False
            log.info("CREATE CAMPAIGN: Complete (dry_run=%s, max=%d)", dry_run, max_prospects)

        except Exception as e:
            log.error("CREATE CAMPAIGN failed: %s", e)
            PULL_STATUS["running"] = False
            PULL_STATUS["progress"] = f"❌ Campaign error: {e}"
            try:
                BUYER_STATUS["running"] = False
                DEEP_PULL_STATUS["running"] = False
            except Exception as _e:
                log.debug("Suppressed: %s", _e)

    t = threading.Thread(target=run_campaign, daemon=True)
    t.start()

    mode = "LIVE — emails will send" if not dry_run else "PREVIEW — dry run, no emails sent"
    return jsonify({
        "ok": True,
        "message": f"🚀 Campaign started ({mode}). Check progress on Growth page.",
        "mode": "live" if not dry_run else "preview",
        "max_prospects": max_prospects,
        "steps": [
            "1. Pull Reytech purchase history from SCPRS",
            "2. Find all buyers of same items (competitors' customers)",
            "3. Deep pull — score buyers & agencies by opportunity",
            f"4. Push top {max_prospects} to growth pipeline" + (" + send emails" if not dry_run else " (preview only)"),
        ],
    })

@bp.route("/api/growth/kpis")
@auth_required
def api_growth_kpis():
    """Real-time KPI metrics for growth dashboard."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_growth_kpis
    return jsonify(get_growth_kpis())


@bp.route("/api/growth/competitor-intel")
@auth_required
def api_growth_competitor_intel():
    """Competitor analysis from SCPRS + lost quotes."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_competitor_intel
    return jsonify(get_competitor_intel())


@bp.route("/api/growth/lost-analysis")
@auth_required
def api_growth_lost_analysis():
    """Deep analysis of lost purchase orders."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_lost_po_analysis
    return jsonify(get_lost_po_analysis())


@bp.route("/api/growth/win-probability/<prospect_id>")
@auth_required
def api_growth_win_probability(prospect_id):
    """Get win probability for a specific prospect."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_prospect, get_win_probability, get_reytech_credentials
    p = get_prospect(prospect_id)
    if not p:
        return jsonify({"ok": False, "error": "Prospect not found"})
    creds = get_reytech_credentials()
    return jsonify(get_win_probability(p, creds))


@bp.route("/api/growth/export")
@auth_required
def api_growth_export():
    """Export growth data in various formats."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import export_growth_report
    fmt = request.args.get("format", "csv")
    result = export_growth_report(fmt)
    if result.get("ok") and fmt == "csv":
        from flask import Response
        return Response(
            result["data"],
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={result['filename']}"}
        )
    return jsonify(result)


@bp.route("/api/growth/audit-log")
@auth_required
def api_growth_audit_log():
    """Retrieve growth action audit log."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_audit_log
    try:
        limit = max(1, min(int(request.args.get("limit", 50)), 500))
    except (ValueError, TypeError, OverflowError):
        limit = 50
    action_filter = request.args.get("action")
    return jsonify({"entries": get_audit_log(limit, action_filter)})


@bp.route("/api/growth/ab-stats")
@auth_required
def api_growth_ab_stats():
    """Get A/B template test results."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_ab_stats
    return jsonify(get_ab_stats())


@bp.route("/api/growth/enrich/<prospect_id>")
@auth_required
def api_growth_enrich(prospect_id):
    """Enrich a prospect with SCPRS data."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_prospect, enrich_prospect_scprs
    p = get_prospect(prospect_id)
    if not p:
        return jsonify({"ok": False, "error": "Prospect not found"})
    return jsonify(enrich_prospect_scprs(p))


@bp.route("/api/growth/personalize/<prospect_id>")
@auth_required
def api_growth_personalize(prospect_id):
    """Generate personalized content for a prospect."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_prospect, generate_personalized_content
    p = get_prospect(prospect_id)
    if not p:
        return jsonify({"ok": False, "error": "Prospect not found"})
    return jsonify(generate_personalized_content(p))


# ═══════════════════════════════════════════════════════════════════════
# V3 — Automation & Outreach API
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/growth/workflows")
@auth_required
def api_growth_workflows():
    """Get available workflow definitions."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_workflows, get_workflow_queue
    return jsonify({"workflows": get_workflows(), "queue": get_workflow_queue()})


@bp.route("/api/growth/workflow/assign", methods=["POST"])
@auth_required
def api_growth_workflow_assign():
    """Assign a workflow to a prospect."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import assign_workflow
    data = request.get_json(silent=True) or {}
    pid = data.get("prospect_id", "")
    wf_id = data.get("workflow_id", "standard_outreach")
    return jsonify(assign_workflow(pid, wf_id))


@bp.route("/api/growth/workflow/advance", methods=["POST"])
@auth_required
def api_growth_workflow_advance():
    """Advance a prospect's workflow to next step."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import advance_workflow_step
    data = request.get_json(silent=True) or {}
    pid = data.get("prospect_id", "")
    result = data.get("result", "completed")
    return jsonify(advance_workflow_step(pid, result))


@bp.route("/api/growth/sms", methods=["POST"])
@auth_required
def api_growth_sms():
    """Send SMS outreach via Twilio."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import send_sms_outreach, get_prospect
    data = request.get_json(silent=True) or {}
    phone = data.get("phone", "")
    template = data.get("template", "sms_follow_up")
    dry_run = data.get("dry_run", True)
    prospect = None
    if data.get("prospect_id"):
        prospect = get_prospect(data["prospect_id"])
    return jsonify(send_sms_outreach(phone, template, prospect, dry_run))


@bp.route("/api/growth/notifications")
@auth_required
def api_growth_notifications():
    """Get notifications for notification center."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_notifications
    unread = request.args.get("unread", "false").lower() == "true"
    return jsonify({"notifications": get_notifications(unread_only=unread)})


@bp.route("/api/growth/notifications/read", methods=["POST"])
@auth_required
def api_growth_notifications_read():
    """Mark notification(s) as read."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import mark_notification_read, dismiss_all_notifications
    data = request.get_json(silent=True) or {}
    if data.get("all"):
        dismiss_all_notifications()
    elif data.get("id"):
        mark_notification_read(data["id"])
    return jsonify({"ok": True})


@bp.route("/api/growth/webhook-test", methods=["POST"])
@auth_required
def api_growth_webhook_test():
    """Test webhook integration."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import fire_webhook
    return jsonify(fire_webhook("test", {"summary": "Growth Engine webhook test", "source": "manual"}))


# ═══════════════════════════════════════════════════════════════════════
# Feature #7 — RBAC
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/growth/roles")
@auth_required
def api_growth_roles():
    """List roles and permissions."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import list_roles
    return jsonify(list_roles())


@bp.route("/api/growth/roles/set", methods=["POST"])
@auth_required
def api_growth_roles_set():
    """Set a user's role."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import set_user_role
    data = request.get_json(silent=True) or {}
    return jsonify(set_user_role(data.get("user_id", "default"), data.get("role", "viewer")))


@bp.route("/api/growth/roles/check")
@auth_required
def api_growth_roles_check():
    """Check if user has a permission."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import check_permission, get_user_role
    user_id = request.args.get("user_id", "default")
    permission = request.args.get("permission", "view")
    return jsonify({
        "user_id": user_id,
        "role": get_user_role(user_id),
        "permission": permission,
        "allowed": check_permission(user_id, permission),
    })


# ═══════════════════════════════════════════════════════════════════════
# Feature #9 — Multi-Format Export (PDF + Excel)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/growth/export/pdf")
@auth_required
def api_growth_export_pdf():
    """Export growth report as PDF."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import export_growth_pdf
    result = export_growth_pdf()
    if result.get("ok"):
        from flask import send_file
        return send_file(result["filepath"], as_attachment=True, download_name=result["filename"])
    return jsonify(result)


@bp.route("/api/growth/export/excel")
@auth_required
def api_growth_export_excel():
    """Export growth data as Excel workbook."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import export_growth_excel
    result = export_growth_excel()
    if result.get("ok"):
        from flask import send_file
        return send_file(result["filepath"], as_attachment=True, download_name=result["filename"])
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════════════
# Feature #12 — Calendar Sync
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/growth/calendar")
@auth_required
def api_growth_calendar():
    """Get scheduled follow-up events."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_calendar_events, get_todays_agenda
    return jsonify({
        "upcoming": get_calendar_events(upcoming_only=True),
        "today": get_todays_agenda(),
    })


@bp.route("/api/growth/calendar/schedule", methods=["POST"])
@auth_required
def api_growth_calendar_schedule():
    """Schedule a follow-up on the calendar."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import schedule_follow_up
    data = request.get_json(silent=True) or {}
    return jsonify(schedule_follow_up(
        prospect_id=data.get("prospect_id", ""),
        date=data.get("date", ""),
        time=data.get("time", "09:00"),
        notes=data.get("notes", ""),
        reminder_type=data.get("type", "email"),
    ))


@bp.route("/api/growth/calendar/complete", methods=["POST"])
@auth_required
def api_growth_calendar_complete():
    """Mark a calendar event as completed."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import complete_calendar_event
    data = request.get_json(silent=True) or {}
    return jsonify(complete_calendar_event(data.get("event_id", "")))


# ═══════════════════════════════════════════════════════════════════════
# SCPRS Search Proxy + Loss Reasons + Startup
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/growth/scprs-search")
@auth_required
def api_growth_scprs_search():
    """SCPRS search proxy with caching."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import scprs_search_proxy
    query = request.args.get("query", request.args.get("q", ""))
    search_type = request.args.get("type", "item")
    if not query:
        return jsonify({"ok": False, "error": "Missing query parameter"})
    return jsonify(scprs_search_proxy(query, search_type))


@bp.route("/api/growth/loss-reason", methods=["POST"])
@auth_required
def api_growth_loss_reason():
    """Record reason for a lost PO/quote."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import add_loss_reason
    data = request.get_json(silent=True) or {}
    return jsonify(add_loss_reason(
        quote_id=data.get("quote_id", ""),
        reason=data.get("reason", ""),
        competitor=data.get("competitor", ""),
        price_delta=float(data.get("price_delta", 0)),
        notes=data.get("notes", ""),
    ))


@bp.route("/api/growth/startup-check")
@auth_required
def api_growth_startup_check():
    """Run growth module startup validation."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import growth_startup_check
    return jsonify(growth_startup_check())


# ═══════════════════════════════════════════════════════════════════════
# V4 — 12 New Feature API Routes
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/growth/kanban")
@auth_required
def api_growth_kanban():
    """Kanban board view of prospect pipeline."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_kanban_board
    return jsonify(get_kanban_board())


@bp.route("/api/growth/funnel")
@auth_required
def api_growth_funnel():
    """Outreach conversion funnel analytics."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_outreach_funnel
    return jsonify(get_outreach_funnel())


@bp.route("/api/growth/agency-intel")
@auth_required
def api_growth_agency_intel():
    """Agency intelligence ranking."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_agency_intelligence
    return jsonify(get_agency_intelligence())


@bp.route("/api/growth/batch-workflow", methods=["POST"])
@auth_required
def api_growth_batch_workflow():
    """Assign workflow to multiple prospects."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import batch_assign_workflow
    data = request.get_json(silent=True) or {}
    return jsonify(batch_assign_workflow(data.get("prospect_ids", []), data.get("workflow_id", "standard_outreach")))


@bp.route("/api/growth/prospect/<prospect_id>/timeline")
@auth_required
def api_growth_prospect_timeline(prospect_id):
    """Unified activity timeline for a prospect."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_prospect_timeline
    return jsonify({"events": get_prospect_timeline(prospect_id)})


@bp.route("/api/growth/quick-wins")
@auth_required
def api_growth_quick_wins():
    """Surface highest probability quick wins."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_quick_wins
    try:
        _max_qw = max(1, min(int(request.args.get("max", 10)), 100))
    except (ValueError, TypeError, OverflowError):
        _max_qw = 10
    return jsonify({"quick_wins": get_quick_wins(_max_qw)})


@bp.route("/api/growth/campaign-performance")
@auth_required
def api_growth_campaign_perf():
    """Per-campaign performance stats."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import get_campaign_performance
    return jsonify({"campaigns": get_campaign_performance()})


@bp.route("/api/growth/daily-brief")
@auth_required
def api_growth_daily_brief():
    """Auto-generated daily growth brief."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import generate_daily_brief
    return jsonify(generate_daily_brief())


@bp.route("/api/growth/bulk-import", methods=["POST"])
@auth_required
def api_growth_bulk_import():
    """Import prospects from CSV."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import bulk_import_prospects
    csv_text = ""
    if request.content_type and "multipart" in request.content_type:
        f = request.files.get("file")
        if f:
            csv_text = f.read().decode("utf-8", errors="ignore")
    else:
        data = request.get_json(silent=True) or {}
        csv_text = data.get("csv", "")
    if not csv_text:
        return jsonify({"ok": False, "error": "No CSV data provided"})
    return jsonify(bulk_import_prospects(csv_text))


@bp.route("/api/growth/auto-tag", methods=["POST"])
@auth_required
def api_growth_auto_tag():
    """Auto-tag all prospects."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import auto_tag_prospects
    return jsonify(auto_tag_prospects())


@bp.route("/api/growth/price-compare")
@auth_required
def api_growth_price_compare():
    """Compare pricing vs SCPRS market data."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import compare_pricing
    item = request.args.get("item", "")
    if not item:
        return jsonify({"ok": False, "error": "Missing item parameter"})
    return jsonify(compare_pricing(item))


@bp.route("/api/growth/duplicates")
@auth_required
def api_growth_duplicates():
    """Find duplicate prospects."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import find_duplicate_prospects
    return jsonify(find_duplicate_prospects())


@bp.route("/api/growth/merge", methods=["POST"])
@auth_required
def api_growth_merge():
    """Merge duplicate prospects."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    from src.agents.growth_agent import merge_prospects
    data = request.get_json(silent=True) or {}
    return jsonify(merge_prospects(data.get("keep_id", ""), data.get("remove_ids", [])))


