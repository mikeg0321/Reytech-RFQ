# routes_intel_ops.py — Operations, admin, test mode, scheduler, pipeline API

from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
from src.core.paths import DATA_DIR
from src.core.db import get_db
from src.api.render import render_page
from markupsafe import escape as esc
import os, json
from datetime import datetime, timedelta, timezone

log = logging.getLogger("reytech")

try:
    from src.core.security import rate_limit, audit_action, _log_audit_internal
    _HAS_SECURITY = True
except ImportError:
    _HAS_SECURITY = False
    def rate_limit(tier="default"):
        def decorator(f): return f
        return decorator
    def audit_action(name):
        def decorator(f): return f
        return decorator

# ═══════════════════════════════════════════════════════════════════════
# Test Mode — QA/QC Infrastructure (Phase 12 Ready)
# ═══════════════════════════════════════════════════════════════════════

# Standard test fixture — realistic PC that exercises the full pipeline
TEST_PC_FIXTURE = {
    "header": {
        "institution": "CSP-Sacramento (TEST)",
        "requestor": "QA Tester",
        "phone": "916-555-0100",
    },
    "ship_to": "CSP-Sacramento, 100 Prison Road, Represa, CA 95671",
    "pc_number": "TEST-001",
    "due_date": "",  # Will be set to 30 days from now
    "items": [
        {"item_number": "1", "description": "Nitrile Exam Gloves, Medium, Box/100",
         "qty": 50, "uom": "BX", "pricing": {}},
        {"item_number": "2", "description": "Hand Sanitizer, 8oz Pump Bottle",
         "qty": 100, "uom": "EA", "pricing": {}},
        {"item_number": "3", "description": "Stryker Patient Restraint Package, Standard",
         "qty": 10, "uom": "KT", "pricing": {}},
    ],
}


@bp.route("/api/test/create-pc")
@auth_required
@safe_route
def api_test_create_pc():
    """Create a test Price Check with fixture data. Flagged as is_test=True."""
    from copy import deepcopy

    fixture = deepcopy(TEST_PC_FIXTURE)
    pc_id = f"test_{uuid.uuid4().hex[:8]}"
    now = datetime.now()
    fixture["due_date"] = (now + timedelta(days=30)).strftime("%m/%d/%Y")

    # Auto-assign TEST quote number (never uses real counter)
    import random
    draft_qn = f"TEST-Q{random.randint(100,999)}"

    pcs = _load_price_checks()
    pc_record = {
        "id": pc_id,
        "pc_number": fixture["pc_number"],
        "institution": fixture["header"]["institution"],
        "due_date": fixture["due_date"],
        "requestor": fixture["header"]["requestor"],
        "ship_to": fixture["ship_to"],
        "items": fixture["items"],
        "source_pdf": "",
        "status": "parsed",
        "status_history": [{"from": "", "to": "parsed", "timestamp": now.isoformat(), "actor": "test"}],
        "created_at": now.isoformat(),
        "parsed": fixture,
        "reytech_quote_number": draft_qn,
        "is_test": True,
    }
    pcs[pc_id] = pc_record
    _save_price_checks(pcs)
    log.info("TEST: Created test PC %s (%s)", pc_id, fixture["pc_number"])
    return jsonify({"ok": True, "pc_id": pc_id, "url": f"/pricecheck/{pc_id}",
                    "message": f"Test PC created: {fixture['pc_number']} with {len(fixture['items'])} items"})


@bp.route("/api/test/cleanup")
@auth_required
@safe_route
def api_test_cleanup():
    """Remove all test records and optionally reset quote counter."""
    reset_counter = request.args.get("reset_counter", "false").lower() == "true"

    # Clean PCs
    pcs = _load_price_checks()
    test_pcs = [k for k, v in pcs.items() if v.get("is_test")]
    for k in test_pcs:
        pcs[k]["status"] = "dismissed"  # Law 22: never delete
    _save_price_checks(pcs)

    # Clean RFQs
    rfqs = load_rfqs()
    test_rfqs = [k for k, v in rfqs.items() if v.get("is_test")]
    for k in test_rfqs:
        del rfqs[k]
    if test_rfqs:
        save_rfqs(rfqs)

    # Clean quotes
    test_quotes = 0
    if QUOTE_GEN_AVAILABLE:
        quotes = get_all_quotes()
        original_len = len(quotes)
        clean_quotes = [q for q in quotes if not q.get("source_pc_id", "").startswith("test_")]
        test_quotes = original_len - len(clean_quotes)
        if test_quotes > 0:
            # Use quote_generator's save
            from src.forms.quote_generator import _save_all_quotes
            _save_all_quotes(clean_quotes)

    # Reset quote counter
    counter_reset = ""
    if reset_counter and QUOTE_GEN_AVAILABLE:
        # Find highest non-test quote number
        quotes = get_all_quotes()
        if quotes:
            nums = [q.get("quote_number", "") for q in quotes]
            # Parse R26Q15 → 15
            max_n = 0
            for n in nums:
                try:
                    max_n = max(max_n, int(n.split("Q")[-1]))
                except Exception as e:
                    log.debug("Suppressed: %s", e)
                    pass
            set_quote_counter(max_n)
            counter_reset = f"Counter reset to {max_n}"
        else:
            set_quote_counter(0)
            counter_reset = "Counter reset to 0"

    log.info("TEST CLEANUP: %d PCs, %d RFQs, %d quotes removed. %s",
             len(test_pcs), len(test_rfqs), test_quotes, counter_reset)
    return jsonify({
        "ok": True,
        "removed": {"pcs": len(test_pcs), "rfqs": len(test_rfqs), "quotes": test_quotes},
        "counter_reset": counter_reset,
        "message": f"Cleaned {len(test_pcs)} test PCs, {len(test_rfqs)} RFQs, {test_quotes} quotes. {counter_reset}",
    })


@bp.route("/api/test/status")
@auth_required
@safe_route
def api_test_status():
    """Show current test data in the system."""
    pcs = _load_price_checks()
    test_pcs = {k: {"pc_number": v.get("pc_number"), "status": v.get("status"), "institution": v.get("institution")}
                for k, v in pcs.items() if v.get("is_test")}
    test_quotes = []
    if QUOTE_GEN_AVAILABLE:
        for q in get_all_quotes():
            if q.get("source_pc_id", "").startswith("test_"):
                test_quotes.append({"quote_number": q.get("quote_number"), "total": q.get("total", 0)})
    return jsonify({
        "test_pcs": test_pcs,
        "test_quotes": test_quotes,
        "counts": {"pcs": len(test_pcs), "quotes": len(test_quotes)},
    })


# ─── Item Identification Agent ───────────────────────────────────────────────

try:
    from src.agents.item_identifier import (identify_item, identify_pc_items,
                                            get_agent_status as item_id_agent_status)
    ITEM_ID_AVAILABLE = True
except ImportError:
    ITEM_ID_AVAILABLE = False

# ─── Lead Generation Agent ──────────────────────────────────────────────────

try:
    from src.agents.lead_gen_agent import (
        evaluate_po, add_lead, get_leads, update_lead_status,
        draft_outreach_email, get_agent_status as leadgen_agent_status,
        get_lead_analytics,
    )
    LEADGEN_AVAILABLE = True
except ImportError:
    LEADGEN_AVAILABLE = False

try:
    from src.agents.scprs_scanner import (
        get_scanner_status, start_scanner, stop_scanner, manual_scan,
    )
    SCANNER_AVAILABLE = True
except ImportError:
    SCANNER_AVAILABLE = False

try:
    from src.agents.predictive_intel import (
        predict_win_probability, log_competitor_intel, get_competitor_insights,
        detect_shipping_email, match_tracking_to_order, update_order_from_tracking,
    )
    PREDICT_AVAILABLE = True
except ImportError:
    PREDICT_AVAILABLE = False

try:
    from src.agents.reply_analyzer import analyze_reply, find_quote_from_reply
    REPLY_ANALYZER_AVAILABLE = True
except ImportError:
    REPLY_ANALYZER_AVAILABLE = False

try:
    from src.agents.quickbooks_agent import (
        fetch_vendors as qb_fetch_vendors, find_vendor, create_purchase_order,
        get_recent_purchase_orders, get_agent_status as qb_agent_status,
        is_configured as qb_configured,
        fetch_invoices, get_invoice_summary, create_invoice, create_invoice as qb_create_invoice,
        fetch_customers, find_customer, find_customer as qb_find_customer,
        get_customer_balance_summary,
        get_financial_context,
        get_company_info as qb_company_info,
        get_profit_loss as qb_profit_loss,
        get_ar_aging as qb_ar_aging,
        get_recent_payments as qb_recent_payments,
        diagnose_connection as qb_diagnose,
    )
    QB_AVAILABLE = True
except ImportError:
    QB_AVAILABLE = False

try:
    from src.agents.email_outreach import (
        draft_for_pc, draft_for_lead, get_outbox, approve_email,
        update_draft, send_email as outreach_send, send_approved,
        delete_from_outbox, get_sent_log, get_agent_status as outreach_agent_status,
    )
    OUTREACH_AVAILABLE = True
except ImportError:
    OUTREACH_AVAILABLE = False

try:
    from src.agents.growth_agent import (
        pull_reytech_history, find_category_buyers, launch_outreach,
        launch_distro_campaign,
        check_follow_ups, launch_voice_follow_up,
        get_growth_status, PULL_STATUS, BUYER_STATUS,
        # CRM layer
        get_prospect, update_prospect, add_prospect_note, mark_responded,
        process_bounceback, scan_inbox_for_bounces, detect_bounceback,
        get_campaign_dashboard, start_scheduler,
        # Legacy compat
        generate_recommendations, full_report, lead_funnel,
    )
    GROWTH_AVAILABLE = True
    if os.environ.get("ENABLE_BACKGROUND_AGENTS", "true").lower() not in ("false", "0", "off"):
        start_scheduler()  # Background: bounce scan + follow-up status updates every hour
except ImportError:
    GROWTH_AVAILABLE = False

try:
    from src.agents.sales_intel import (
        deep_pull_all_buyers, get_priority_queue, push_to_growth_prospects,
        get_intel_status, update_revenue_tracker, add_manual_revenue,
        get_sb_admin, find_sb_admin_for_agencies,
        add_manual_buyer, import_buyers_csv, seed_demo_data, delete_buyer,
        sync_buyers_to_crm,
        DEEP_PULL_STATUS, REVENUE_GOAL,
        BUYERS_FILE as INTEL_BUYERS_FILE, AGENCIES_FILE as INTEL_AGENCIES_FILE,
    )
    INTEL_AVAILABLE = True
except ImportError:
    INTEL_AVAILABLE = False

try:
    from src.agents.voice_agent import (
        place_call, get_call_log, get_agent_status as voice_agent_status,
        is_configured as voice_configured, SCRIPTS as VOICE_SCRIPTS,
        verify_credentials as voice_verify,
        import_twilio_to_vapi, get_vapi_call_details, get_vapi_calls,
    )
    VOICE_AVAILABLE = True
except ImportError:
    VOICE_AVAILABLE = False

try:
    from src.agents.voice_campaigns import (
        create_campaign, get_campaigns, get_campaign,
        execute_campaign_call, update_call_outcome,
        get_campaign_stats, list_scripts as campaign_list_scripts,
    )
    CAMPAIGNS_AVAILABLE = True
except ImportError:
    CAMPAIGNS_AVAILABLE = False

try:
    from src.agents.manager_agent import (
        generate_brief, get_agent_status as manager_agent_status,
    )
    MANAGER_AVAILABLE = True
except ImportError:
    MANAGER_AVAILABLE = False

try:
    from src.agents.orchestrator import (
        run_workflow, get_workflow_status, get_workflow_graph_viz,
    )
    ORCHESTRATOR_AVAILABLE = True
except ImportError:
    ORCHESTRATOR_AVAILABLE = False

try:
    from src.agents.qa_agent import (
        full_scan, scan_html, agent_status as qa_agent_status,
        run_health_check, get_qa_history, get_health_trend, start_qa_monitor,
    )
    QA_AVAILABLE = True
    if os.environ.get("ENABLE_BACKGROUND_AGENTS", "true").lower() not in ("false", "0", "off"):
        try:
            start_qa_monitor()
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
except ImportError:
    QA_AVAILABLE = False

try:
    from src.agents.workflow_tester import (
        run_workflow_tests, get_latest_run as get_latest_wf_run,
        get_run_history as get_wf_history, start_workflow_monitor,
    )
    if os.environ.get("ENABLE_BACKGROUND_AGENTS", "true").lower() not in ("false", "0", "off"):
        start_workflow_monitor()
    _WF_AVAILABLE = True
except Exception:
    _WF_AVAILABLE = False


@bp.route("/api/identify", methods=["POST"])
@auth_required
@safe_route
def api_identify_item():
    """Identify a single item. POST JSON: {"description": "...", "qty": 22, "uom": "EA"}"""
    if not ITEM_ID_AVAILABLE:
        return jsonify({"ok": False, "error": "Item identifier agent not available"})
    data = request.get_json(silent=True) or {}
    desc = data.get("description", "").strip()
    if not desc:
        return jsonify({"ok": False, "error": "No description provided"})
    result = identify_item(desc, qty=data.get("qty", 0), uom=data.get("uom", ""))
    return jsonify({"ok": True, **result})


@bp.route("/api/identify/pc/<pcid>")
@auth_required
@safe_route
def api_identify_pc(pcid):
    """Run item identification on all items in a Price Check."""
    if not ITEM_ID_AVAILABLE:
        return jsonify({"ok": False, "error": "Item identifier agent not available"})
    pcs = _load_price_checks()
    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"})
    pc = pcs[pcid]
    items = pc.get("items", [])
    if not items:
        return jsonify({"ok": False, "error": "No items in PC"})

    identified = identify_pc_items(items)
    # Save back
    pc["items"] = identified
    _save_price_checks(pcs)

    return jsonify({
        "ok": True,
        "items": len(identified),
        "identified": sum(1 for it in identified if it.get("identification")),
        "mode": identified[0].get("identification", {}).get("method", "none") if identified else "none",
        "results": [
            {
                "description": it.get("description", "")[:60],
                "search_term": it.get("_search_query", ""),
                "category": it.get("_category", ""),
                "method": it.get("identification", {}).get("method", ""),
            }
            for it in identified
        ],
    })


@bp.route("/api/agents/status")
@auth_required
@safe_route
def api_agents_status():
    """Status of all agents."""
    agents = {
        "item_identifier": item_id_agent_status() if ITEM_ID_AVAILABLE else {"status": "not_available"},
        "lead_gen": leadgen_agent_status() if LEADGEN_AVAILABLE else {"status": "not_available"},
        "scprs_scanner": get_scanner_status() if SCANNER_AVAILABLE else {"status": "not_available"},
        "quickbooks": qb_agent_status() if QB_AVAILABLE else {"status": "not_available"},
        "email_outreach": outreach_agent_status() if OUTREACH_AVAILABLE else {"status": "not_available"},
        "growth_strategy": get_growth_status() if GROWTH_AVAILABLE else {"status": "not_available"},
        "voice_calls": voice_agent_status() if VOICE_AVAILABLE else {"status": "not_available"},
        "manager": manager_agent_status() if MANAGER_AVAILABLE else {"status": "not_available"},
        "orchestrator": get_workflow_status() if ORCHESTRATOR_AVAILABLE else {"status": "not_available"},
        "qa": qa_agent_status() if QA_AVAILABLE else {"status": "not_available"},
        "predictive_intel": {"status": "ready", "version": "1.0.0", "features": ["win_prediction", "competitor_intel", "shipping_monitor"]} if PREDICT_AVAILABLE else {"status": "not_available"},
    }
    try:
        from src.agents.product_research import get_research_cache_stats
        agents["product_research"] = get_research_cache_stats()
    except Exception as e:
        log.debug("Suppressed: %s", e)
        agents["product_research"] = {"status": "not_available"}

    # PRD-28 agents
    try:
        from src.agents.quote_lifecycle import get_agent_status as _ql_status
        agents["quote_lifecycle"] = _ql_status()
    except Exception:
        agents["quote_lifecycle"] = {"status": "not_available"}
    try:
        from src.agents.email_lifecycle import get_agent_status as _el_status
        agents["email_lifecycle"] = _el_status()
    except Exception:
        agents["email_lifecycle"] = {"status": "not_available"}
    try:
        from src.agents.lead_nurture_agent import get_agent_status as _ln_status
        agents["lead_nurture"] = _ln_status()
    except Exception:
        agents["lead_nurture"] = {"status": "not_available"}
    try:
        from src.agents.revenue_engine import get_agent_status as _re_status
        agents["revenue_engine"] = _re_status()
    except Exception:
        agents["revenue_engine"] = {"status": "not_available"}
    try:
        from src.agents.vendor_intelligence import get_agent_status as _vi_status
        agents["vendor_intelligence"] = _vi_status()
    except Exception:
        agents["vendor_intelligence"] = {"status": "not_available"}

    return jsonify({"ok": True, "agents": agents,
                    "total": len(agents),
                    "active": sum(1 for a in agents.values() if a.get("status") != "not_available")})


@bp.route("/api/qa/workflow", methods=["GET","POST"])
@auth_required
@safe_route
def api_qa_workflow_run():
    if not _WF_AVAILABLE:
        return jsonify({"ok": False, "error": "workflow_tester not available"}), 503
    # Force-reload to pick up source changes (Railway volume caches stale bytecode)
    import importlib, src.agents.workflow_tester as _wt
    importlib.reload(_wt)
    report = _wt.run_workflow_tests()
    return jsonify(report)


@bp.route("/api/qa/workflow/latest")
@auth_required
@safe_route
def api_qa_workflow_latest():
    if not _WF_AVAILABLE:
        return jsonify({"ok": False, "error": "workflow_tester not available"}), 503
    return jsonify(get_latest_wf_run())


@bp.route("/api/qa/workflow/history")
@auth_required
@safe_route
def api_qa_workflow_history():
    if not _WF_AVAILABLE:
        return jsonify({"ok": False, "error": "workflow_tester not available"}), 503
    try:
        n = max(1, min(int(request.args.get("n", 20)), 200))
    except (ValueError, TypeError, OverflowError):
        n = 20
    return jsonify(get_wf_history(n))


@bp.route("/qa/workflow")
@auth_required
@safe_page
def qa_workflow_page():
    return """<!DOCTYPE html><html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Workflow Tests — Reytech</title>
<style>
:root{--bg:#0d1117;--sf:#161b22;--sf2:#21262d;--bd:#30363d;--tx:#e6edf3;--tx2:#8b949e;--gn:#34d399;--yl:#fbbf24;--rd:#f87171;--ac:#4f8cff}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--tx);padding:20px;max-width:960px;margin:auto}
.nav{display:flex;gap:8px;margin-bottom:20px;flex-wrap:wrap}
.nav a{padding:5px 12px;background:var(--sf2);border:1px solid var(--bd);border-radius:6px;font-size:13px;color:var(--tx);text-decoration:none}
.card{background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:16px;margin-bottom:14px}
.row{display:flex;align-items:flex-start;gap:12px;padding:10px 0;border-bottom:1px solid var(--bd)}
.row:last-child{border-bottom:none}
.badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.5px}
.pass{background:rgba(52,211,153,.15);color:var(--gn)}.warn{background:rgba(251,191,36,.15);color:var(--yl)}.fail{background:rgba(248,113,113,.15);color:var(--rd)}
.fix{font-size:14px;color:var(--yl);margin-top:4px;font-style:italic}
h1{font-size:22px;margin-bottom:4px}.sub{color:var(--tx2);font-size:13px;margin-bottom:20px}
.run-btn{padding:10px 24px;background:var(--ac);color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:14px;font-weight:600;margin-bottom:16px}
</style></head><body>
<div class="nav"><a href="/">🏠 Home</a><a href="/agents">🤖 Agents</a><a href="/qa/intelligence">📊 QA Intel</a></div>
<h1>🔬 Workflow Tests</h1><p class="sub">End-to-end data flow validation — runs every 10 minutes automatically</p>
<div id="score-card" class="card" style="text-align:center"><div style="color:var(--tx2)">Loading…</div></div>
<button class="run-btn" onclick="runTests()" id="run-btn">▶ Run Tests Now</button>
<div id="results"></div>
<div id="history"><div class="card"><div style="color:var(--tx2);font-size:13px">Loading history…</div></div></div>
<script>
function loadLatest(){
  fetch('/api/qa/workflow/latest',{credentials:'same-origin'}).then(r=>r.json()).then(function(d){
    if(d&&d.full_report&&d.full_report.results){renderReport(d.full_report);}else{runTests();}
  }).catch(function(){runTests();});
}
function runTests(){
  var btn=document.getElementById('run-btn');btn.disabled=true;btn.textContent='⏳ Running…';
  fetch('/api/qa/workflow',{credentials:'same-origin'}).then(r=>r.json()).then(function(d){
    renderReport(d);loadHistory();btn.disabled=false;btn.textContent='▶ Run Tests Now';
  }).catch(function(){btn.disabled=false;btn.textContent='▶ Run Tests Now';});
}
function renderReport(d){
  var sc=d.score||0;var col=sc>=90?'var(--gn)':sc>=70?'var(--yl)':'var(--rd)';
  document.getElementById('score-card').innerHTML=
    '<div style="font-size:52px;font-weight:700;color:'+col+';font-family:monospace">'+sc+'/100</div>'+
    '<div style="font-size:18px;margin:4px 0;color:'+col+'">Grade '+d.grade+'</div>'+
    '<div style="font-size:14px;color:var(--tx2)">'+d.summary.passed+' pass · '+d.summary.warned+' warn · '+d.summary.failed+' fail · '+d.duration_s+'s</div>'+
    '<div style="font-size:14px;color:var(--tx2);margin-top:4px">Last run: '+new Date(d.run_at).toLocaleString()+'</div>';
  var html='<div class="card"><div style="font-weight:600;margin-bottom:8px">Test Results</div>';
  (d.results||[]).forEach(function(r){
    var icon=r.status==='pass'?'✅':r.status==='warn'?'⚠️':'❌';
    html+='<div class="row"><span style="font-size:18px">'+icon+'</span>'+
      '<div style="flex:1"><div style="font-size:13px;font-weight:600">'+r.test+'<span class="badge '+r.status+'" style="margin-left:8px">'+r.status+'</span></div>'+
      '<div style="font-size:14px;color:var(--tx2);margin-top:2px">'+r.message+'</div>'+
      (r.detail?'<div style="font-size:14px;color:var(--tx2);margin-top:2px;font-family:monospace">'+r.detail+'</div>':'')+
      (r.fix&&r.status!=='pass'?'<div class="fix">💡 '+r.fix+'</div>':'')+
      '</div></div>';
  });
  html+='</div>';
  document.getElementById('results').innerHTML=html;
}
function loadHistory(){
  fetch('/api/qa/workflow/history?n=10',{credentials:'same-origin'}).then(r=>r.json()).then(function(rows){
    if(!rows.length)return;
    var html='<div class="card"><div style="font-weight:600;margin-bottom:8px">Run History (last 10)</div>';
    rows.forEach(function(r){
      var col=r.score>=90?'var(--gn)':r.score>=70?'var(--yl)':'var(--rd)';
      var fails=[];try{fails=JSON.parse(r.critical_failures||'[]');}catch(e){}
      html+='<div style="display:flex;align-items:center;gap:12px;padding:8px 0;border-bottom:1px solid var(--bd)">'+
        '<div style="font-family:monospace;font-size:18px;font-weight:700;color:'+col+';width:60px">'+r.score+'</div>'+
        '<div style="flex:1"><div style="font-size:14px;color:var(--tx2)">'+new Date(r.run_at).toLocaleString()+'</div>'+
        (fails.length?'<div style="font-size:14px;color:var(--rd);margin-top:2px">'+fails[0]+'</div>':'<div style="font-size:14px;color:var(--gn)">All clear</div>')+'</div>'+
        '<div style="font-size:14px;color:var(--tx2)">'+r.passed+'P '+r.warned+'W '+r.failed+'F</div>'+
        '</div>';
    });
    html+='</div>';
    document.getElementById('history').innerHTML=html;
  });
}
loadLatest();loadHistory();
</script></body></html>"""


@bp.route("/api/qa/scan")
@auth_required
@safe_route
def api_qa_scan():
    """Run full QA scan across all pages and source files."""
    if not QA_AVAILABLE:
        return jsonify({"ok": False, "error": "QA agent not available"})
    try:
        report = full_scan(current_app)
        return jsonify({"ok": True, **report})
    except Exception as e:
        log.exception("QA scan failed: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qa/health")
@auth_required
@safe_route
def api_qa_health():
    """Run health check — routes, data, agents, env, code metrics.
    ?checks=routes,data,agents"""
    if not QA_AVAILABLE:
        return jsonify({"ok": False, "error": "QA agent not available"})
    checks = request.args.get("checks", "").split(",") if request.args.get("checks") else None
    checks = [c.strip() for c in checks] if checks else None
    report = run_health_check(checks=checks)
    try:
        from src.agents.qa_agent import save_qa_run_to_db
        save_qa_run_to_db(report)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    return jsonify({"ok": True, **report})




@bp.route("/api/qa/trend")
@auth_required
@safe_route
def api_qa_trend():
    """Health score trend over time."""
    if not QA_AVAILABLE:
        return jsonify({"ok": False, "error": "QA agent not available"})
    return jsonify({"ok": True, **get_health_trend()})


# ─── Manager Brief Routes ───────────────────────────────────────────────────

@bp.route("/api/manager/brief/debug")
@auth_required
@safe_route
def api_manager_brief_debug():
    """Debug endpoint — shows exactly what throws in generate_brief() on this environment."""
    import traceback
    results = {}
    from src.agents.manager_agent import (
        _get_pending_approvals, _get_activity_feed, _get_pipeline_summary,
        _check_all_agents, _get_revenue_status, get_scprs_brief_section,
        generate_brief,
    )
    for name, fn, kw in [
        ("_get_pending_approvals", _get_pending_approvals, {}),
        ("_get_activity_feed",     _get_activity_feed,     {"limit": 5}),
        ("_get_pipeline_summary",  _get_pipeline_summary,  {}),
        ("_check_all_agents",      _check_all_agents,      {}),
        ("_get_revenue_status",    _get_revenue_status,    {}),
        ("get_scprs_brief_section",get_scprs_brief_section,{}),
    ]:
        try:
            val = fn(**kw)
            results[name] = {"ok": True, "type": type(val).__name__}
        except Exception as e:
            results[name] = {"ok": False, "error": str(e)}
    try:
        generate_brief()
        results["generate_brief"] = {"ok": True}
    except Exception as e:
        results["generate_brief"] = {"ok": False, "error": str(e)}
    all_ok = all(v.get("ok") for v in results.values())
    return jsonify({"ok": all_ok, "results": results})


_brief_cache = {"data": None, "ts": 0}
_BRIEF_TTL = 120  # seconds (was 30 — too short for 6.5s avg generation)

@bp.route("/api/manager/brief")
@auth_required
@safe_route
def api_manager_brief():
    """Manager brief — everything you need to know right now.  Server-side 30s TTL cache."""
    import time as _time
    if not MANAGER_AVAILABLE:
        return jsonify({"ok": False, "error": "Manager agent not available"})
    # Serve from cache unless ?nocache=1 (Refresh button) or stale
    nocache = request.args.get("nocache") == "1"
    now = _time.time()
    if not nocache and _brief_cache["data"] and (now - _brief_cache["ts"]) < _BRIEF_TTL:
        return jsonify(_brief_cache["data"])
    try:
        brief = generate_brief()
        # Sanitize any None values that could crash the JS
        if isinstance(brief.get("scprs_intel"), dict):
            si = brief["scprs_intel"]
            si.setdefault("top_action", None)
            si.setdefault("recommendations", [])
            si.setdefault("recent_losses", [])
        result = {"ok": True, **brief}
        _brief_cache["data"] = result
        _brief_cache["ts"] = now
        return jsonify(result)
    except Exception as e:
        import traceback
        err_detail = traceback.format_exc()
        log.error("manager brief error: %s\n%s", e, err_detail)
        # Fallback: build a real brief from individual resilient calls
        # Each call is individually guarded — one crash must NOT kill the whole fallback
        from datetime import datetime
        try:
            from src.agents.manager_agent import (
                _get_pipeline_summary, _get_activity_feed,
                _get_pending_approvals, _get_revenue_status, _check_all_agents,
            )
            _empty_summary = {"price_checks": {}, "rfqs": {}, "quotes": {}, "leads": {}, "orders": {}, "outbox": {"drafts": 0}, "growth": {}}
            try: summary   = _get_pipeline_summary()
            except Exception as _se: log.warning("fallback _get_pipeline_summary: %s", _se); summary = _empty_summary
            try: activity  = _get_activity_feed(limit=8)
            except Exception as _ae: log.warning("fallback _get_activity_feed: %s", _ae); activity = []
            try: approvals = _get_pending_approvals()
            except Exception as _ape: log.warning("fallback _get_pending_approvals: %s", _ape); approvals = []
            try: revenue   = _get_revenue_status()
            except Exception as _re: log.warning("fallback _get_revenue_status: %s", _re); revenue = {}
            try: agents    = _check_all_agents()
            except Exception as _age: log.warning("fallback _check_all_agents: %s", _age); agents = []
            agents_ok = sum(1 for a in agents if a["status"] in ("active","ready","connected"))
            agents_down = sum(1 for a in agents if a["status"] in ("unavailable","error"))
            q = summary.get("quotes", {})
            headlines = []
            if approvals:
                headlines.append(f"{len(approvals)} item{'s' if len(approvals)!=1 else ''} need your attention")
            if q.get("total", 0) > 0:
                headlines.append(f"{q.get('total',0)} quote{'s' if q.get('total',0)!=1 else ''} in pipeline")
            if not headlines:
                headlines.append("Pipeline clear — upload a PC to get started")
            return jsonify({
                "ok": True,
                "generated_at": datetime.now().isoformat(),
                "headline": headlines[0],
                "headlines": headlines,
                "pending_approvals": approvals,
                "approval_count": len(approvals),
                "activity": activity,
                "summary": summary,
                "agents": agents,
                "agents_summary": {"total": len(agents), "healthy": agents_ok, "down": agents_down, "needs_config": 0},
                "revenue": {
                    "closed": revenue.get("closed_revenue", 0),
                    "goal":   revenue.get("goal", 2000000),
                    "pct":    revenue.get("pct_to_goal", 0),
                    "gap":    revenue.get("gap_to_goal", 2000000),
                    "on_track": revenue.get("on_track", False),
                    "run_rate": revenue.get("run_rate_annual", 0),
                    "monthly_needed": revenue.get("monthly_needed", 181818),
                },
                "scprs_intel": {"available": False},
                "growth_campaign": {},
                "db_context": {},
                "auto_closed_today": 0,
                "_error": str(e),
                "_fallback": True,
            })
        except Exception as e2:
            log.error("manager brief fallback also failed: %s", e2)
            return jsonify({
                "ok": True,
                "generated_at": datetime.now().isoformat(),
                "headline": "Dashboard active",
                "headlines": [],
                "pending_approvals": [], "approval_count": 0,
                "activity": [],
                "summary": {"quotes": {}, "price_checks": {}, "outbox": {"drafts": 0}, "leads": {}, "growth": {}},
                "agents": [], "agents_summary": {"total": 0, "healthy": 0, "down": 0, "needs_config": 0},
                "revenue": {"closed": 0, "goal": 2000000, "pct": 0, "gap": 2000000, "on_track": False, "run_rate": 0, "monthly_needed": 181818},
                "scprs_intel": {"available": False},
                "growth_campaign": {}, "db_context": {}, "auto_closed_today": 0,
                "_error": f"{e} / {e2}", "_fallback": True,
            })


@bp.route("/api/manager/metrics")
@auth_required
@safe_route
def api_manager_metrics():
    """Power BI-style metrics for dashboard KPIs."""
    if not MANAGER_AVAILABLE:
        return jsonify({"ok": False, "error": "Manager agent not available"})

    from datetime import timedelta
    from collections import defaultdict

    quotes = []
    try:
        qpath = os.path.join(DATA_DIR, "quotes_log.json")
        with open(qpath) as f:
            quotes = [q for q in json.load(f) if not q.get("is_test")]
    except Exception as e:
        log.debug("Suppressed: %s", e)
        pass

    pcs = _load_price_checks()
    # Filter test PCs
    if isinstance(pcs, dict):
        pcs = {k: v for k, v in pcs.items() if not v.get("is_test")}
    now = datetime.now()

    # Revenue metrics
    won = [q for q in quotes if q.get("status") == "won"]
    lost = [q for q in quotes if q.get("status") == "lost"]
    pending = [q for q in quotes if q.get("status") in ("pending", "sent")]
    total_revenue = sum(q.get("total", 0) for q in won)
    pipeline_value = sum(q.get("total", 0) for q in pending)

    # Monthly goal aligned with $2M annual
    monthly_goal = float(os.environ.get("MONTHLY_REVENUE_GOAL", "166667"))

    # This month's revenue
    month_start = now.replace(day=1, hour=0, minute=0, second=0)
    month_revenue = 0
    month_quotes = 0
    for q in won:
        ts = q.get("status_updated", q.get("created_at", ""))
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
                if dt >= month_start:
                    month_revenue += q.get("total", 0)
                    month_quotes += 1
            except (ValueError, TypeError) as _e:
                log.debug("suppressed: %s", _e)

    # Weekly quote volume (last 4 weeks)
    weekly_volume = []
    for w in range(4):
        week_end = now - timedelta(weeks=w)
        week_start = week_end - timedelta(weeks=1)
        count = 0
        value = 0
        for q in quotes:
            ts = q.get("created_at", q.get("generated_at", ""))
            if ts:
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
                    if week_start <= dt < week_end:
                        count += 1
                        value += q.get("total", 0)
                except (ValueError, TypeError) as _e:
                    log.debug("suppressed: %s", _e)
        label = f"Week {4-w}" if w > 0 else "This Week"
        weekly_volume.append({"label": label, "quotes": count, "value": round(value, 2)})
    weekly_volume.reverse()

    # Win rate trend (rolling - all time)
    decided = len(won) + len(lost)
    win_rate = round(len(won) / max(decided, 1) * 100)

    # Average deal size
    avg_deal = round(total_revenue / max(len(won), 1), 2)

    # Pipeline funnel — ACTIVE items only (exclude dismissed/archived/deleted)
    _active_statuses = {"parsed", "new", "parse_error", "priced", "ready", "auto_drafted",
                        "quoted", "generated", "sent", "completed", "converted"}
    _inactive_statuses = {"dismissed", "archived", "deleted", "duplicate", "no_response"}
    active_pcs = {pid: p for pid, p in (pcs.items() if isinstance(pcs, dict) else [])
                  if p.get("status", "parsed") not in _inactive_statuses}
    pc_count = len(active_pcs)
    pc_parsed = sum(1 for p in active_pcs.values() if p.get("status") in ("parsed", "new", "parse_error"))
    pc_priced = sum(1 for p in active_pcs.values() if p.get("status") in ("priced", "ready", "auto_drafted"))
    pc_completed = sum(1 for p in active_pcs.values() if p.get("status") in ("completed", "converted"))

    # Quote-stage counts for funnel
    quoted_count = len([q for q in quotes if q.get("status") in ("pending", "draft")])
    sent_count = len([q for q in quotes if q.get("status") == "sent"])
    won_count = len(won)

    # Orders count
    orders_active = 0
    try:
        from src.core.db import get_db
        with get_db() as _oconn:
            orders_active = _oconn.execute(
                "SELECT COUNT(*) FROM orders WHERE status IN ('active', 'processing', 'shipped')"
            ).fetchone()[0]
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Response time (avg hours from PC upload to priced)
    response_times = []
    for pcid, pc in (pcs.items() if isinstance(pcs, dict) else []):
        history = pc.get("status_history", [])
        created_ts = None
        priced_ts = None
        for h in history:
            if h.get("to") == "parsed" and not created_ts:
                created_ts = h.get("timestamp")
            if h.get("to") == "priced" and not priced_ts:
                priced_ts = h.get("timestamp")
        if created_ts and priced_ts:
            try:
                c = datetime.fromisoformat(created_ts.replace("Z", "+00:00")).replace(tzinfo=None)
                p = datetime.fromisoformat(priced_ts.replace("Z", "+00:00")).replace(tzinfo=None)
                hours = (p - c).total_seconds() / 3600
                if 0 < hours < 720:  # Sanity: under 30 days
                    response_times.append(hours)
            except (ValueError, TypeError) as _e:
                log.debug("suppressed: %s", _e)
    avg_response = round(sum(response_times) / max(len(response_times), 1), 1)

    # Top institutions by revenue
    inst_rev = defaultdict(float)
    for q in won:
        inst_rev[q.get("institution", "Unknown")] += q.get("total", 0)
    top_institutions = sorted(inst_rev.items(), key=lambda x: x[1], reverse=True)[:5]

    return jsonify({
        "ok": True,
        "revenue": {
            "total": round(total_revenue, 2),
            "this_month": round(month_revenue, 2),
            "monthly_goal": monthly_goal,
            "goal_pct": round(month_revenue / max(monthly_goal, 1) * 100),
            "pipeline_value": round(pipeline_value, 2),
            "avg_deal": avg_deal,
        },
        "quotes": {
            "total": len(quotes),
            "won": len(won),
            "lost": len(lost),
            "pending": len(pending),
            "win_rate": win_rate,
            "this_month_won": month_quotes,
        },
        "funnel": {
            "pcs_total": pc_count,
            "parsed": pc_parsed,
            "priced": pc_priced,
            "completed": pc_completed,
            "quoted": quoted_count,
            "sent": sent_count,
            "won": won_count,
            "orders_active": orders_active,
            "quotes_generated": len(quotes),
            "quotes_won": len(won),
        },
        "weekly_volume": weekly_volume,
        "response_time_hours": avg_response,
        "top_institutions": [{"name": n, "revenue": round(v, 2)} for n, v in top_institutions],
    })


# ─── Orchestrator / Workflow Routes ──────────────────────────────────────────

@bp.route("/api/workflow/run", methods=["POST"])
@auth_required
@safe_route
def api_workflow_run():
    """Execute a named workflow pipeline."""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({"ok": False, "error": "Orchestrator not available"})
    data = request.get_json(force=True, silent=True) or {}
    name = data.get("workflow", "")
    inputs = data.get("inputs", {})
    if not name:
        return jsonify({"ok": False, "error": "Missing 'workflow' field"})
    result = run_workflow(name, inputs)
    return jsonify({"ok": not bool(result.get("error")), **result})


@bp.route("/api/workflow/status")
@auth_required
@safe_route
def api_workflow_status():
    """Orchestrator status and run history."""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({"ok": False, "error": "Orchestrator not available"})
    return jsonify({"ok": True, **get_workflow_status()})


@bp.route("/api/workflow/graph/<name>")
@auth_required
@safe_route
def api_workflow_graph(n):
    """Get workflow graph structure for visualization."""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({"ok": False, "error": "Orchestrator not available"})
    return jsonify({"ok": True, **get_workflow_graph_viz(n)})


# ─── SCPRS Scanner Routes ───────────────────────────────────────────────────

@bp.route("/api/scanner/start", methods=["POST"])
@auth_required
@safe_route
def api_scanner_start():
    """Start the SCPRS opportunity scanner."""
    if not SCANNER_AVAILABLE:
        return jsonify({"ok": False, "error": "Scanner not available"})
    data = request.get_json(silent=True) or {}
    interval = data.get("interval", 60)
    start_scanner(interval)
    return jsonify({"ok": True, "status": get_scanner_status()})


@bp.route("/api/scanner/stop", methods=["POST"])
@auth_required
@safe_route
def api_scanner_stop():
    """Stop the SCPRS opportunity scanner."""
    if not SCANNER_AVAILABLE:
        return jsonify({"ok": False, "error": "Scanner not available"})
    stop_scanner()
    return jsonify({"ok": True, "status": get_scanner_status()})


@bp.route("/api/scanner/scan", methods=["POST"])
@auth_required
@safe_route
def api_scanner_manual():
    """Run a single scan manually."""
    if not SCANNER_AVAILABLE:
        return jsonify({"ok": False, "error": "Scanner not available"})
    results = manual_scan()
    return jsonify({"ok": True, **results})


@bp.route("/api/scanner/status")
@auth_required
@safe_route
def api_scanner_status():
    """Get scanner status."""
    if not SCANNER_AVAILABLE:
        return jsonify({"ok": False, "error": "Scanner not available"})
    return jsonify({"ok": True, **get_scanner_status()})


# ─── QuickBooks Routes ──────────────────────────────────────────────────────

@bp.route("/api/qb/connect")
@auth_required
@safe_route
def api_qb_connect():
    """Start QuickBooks OAuth2 flow — redirects to Intuit login."""
    if not QB_AVAILABLE:
        return jsonify({"ok": False, "error": "QuickBooks agent not available"})
    from src.agents.quickbooks_agent import QB_CLIENT_ID, QB_SANDBOX
    if not QB_CLIENT_ID:
        return jsonify({"ok": False, "error": "Set QB_CLIENT_ID env var first"})
    # Build OAuth URL
    redirect_uri = request.url_root.rstrip("/").replace("http://", "https://") + "/api/qb/callback"
    scope = "com.intuit.quickbooks.accounting"
    auth_url = (
        f"https://appcenter.intuit.com/connect/oauth2?"
        f"client_id={QB_CLIENT_ID}&response_type=code&scope={scope}"
        f"&redirect_uri={redirect_uri}&state=reytech"
    )
    return redirect(auth_url)


@bp.route("/api/qb/callback")
def api_qb_callback():
    """QuickBooks OAuth2 callback — exchange code for tokens."""
    if not QB_AVAILABLE:
        flash("QuickBooks agent not available", "error")
        return redirect("/agents")
    code = request.args.get("code")
    realm_id = request.args.get("realmId")
    if not code:
        flash(f"QB OAuth failed: {request.args.get('error', 'no code')}", "error")
        return redirect("/agents")
    try:
        from src.agents.quickbooks_agent import (
            QB_CLIENT_ID, QB_CLIENT_SECRET, TOKEN_URL, _save_tokens
        )
        import base64 as _b64
        redirect_uri = request.url_root.rstrip("/").replace("http://", "https://") + "/api/qb/callback"
        auth = _b64.b64encode(f"{QB_CLIENT_ID}:{QB_CLIENT_SECRET}".encode()).decode()
        import requests as _req
        resp = _req.post(TOKEN_URL, headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        }, data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        import time as _time
        _save_tokens({
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "expires_at": _time.time() + data.get("expires_in", 3600),
            "realm_id": realm_id,
            "connected_at": datetime.now().isoformat(),
        })
        # Also save realm_id + refresh_token to env for cross-worker persistence
        os.environ["QB_REALM_ID"] = realm_id or ""
        os.environ["QB_REFRESH_TOKEN"] = data.get("refresh_token", "")
        flash(f"QuickBooks connected! Realm: {realm_id}", "success")
        _log_crm_activity("system", "qb_connected", f"QuickBooks Online connected (realm {realm_id})", actor="user")
    except Exception as e:
        flash(f"QB OAuth error: {e}", "error")
    return redirect("/agents")


@bp.route("/api/qb/status")
@auth_required
@safe_route
def api_qb_status():
    """QuickBooks connection status."""
    if not QB_AVAILABLE:
        return jsonify({"ok": False, "error": "QuickBooks agent not available"})
    return jsonify({"ok": True, **qb_agent_status()})


@bp.route("/api/qb/vendors")
@auth_required
@safe_route
def api_qb_vendors():
    """List QuickBooks vendors."""
    if not QB_AVAILABLE:
        return jsonify({"ok": False, "error": "QuickBooks agent not available"})
    if not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured. Set QB_CLIENT_ID, QB_CLIENT_SECRET, QB_REFRESH_TOKEN, QB_REALM_ID"})
    force = request.args.get("refresh", "").lower() in ("true", "1")
    vendors = qb_fetch_vendors(force_refresh=force)
    return jsonify({"ok": True, "vendors": vendors, "count": len(vendors)})


@bp.route("/api/qb/vendors/find")
@auth_required
@safe_route
def api_qb_vendor_find():
    """Find a vendor by name. ?name=Amazon"""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    name = request.args.get("name", "")
    if not name:
        return jsonify({"ok": False, "error": "Provide ?name= parameter"})
    vendor = find_vendor(name)
    if vendor:
        return jsonify({"ok": True, "vendor": vendor})
    return jsonify({"ok": True, "vendor": None, "message": f"No vendor matching '{name}'"})


@bp.route("/api/qb/vendors/create", methods=["POST"])
@auth_required
@safe_route
def api_qb_vendor_create():
    """Create a new vendor in QuickBooks. POST {name, email, phone}"""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "Vendor name is required"})
    # Check if already exists
    existing = find_vendor(name)
    if existing:
        return jsonify({"ok": True, "vendor": existing, "message": "Vendor already exists in QB"})
    try:
        from src.agents.quickbooks_agent import create_vendor
        result = create_vendor(name, email=data.get("email", ""), phone=data.get("phone", ""))
        if result:
            # Also add to local vendors DB
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    conn.execute("""
                        INSERT OR IGNORE INTO vendors (name, qb_vendor_id, email, status, created_at)
                        VALUES (?, ?, ?, 'active', datetime('now'))
                    """, (name, result.get("Id", ""), data.get("email", "")))
            except Exception as _e:
                log.debug("suppressed: %s", _e)
            return jsonify({"ok": True, "vendor": result})
        return jsonify({"ok": False, "error": "QuickBooks API error — check connection"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/po/create", methods=["POST"])
@auth_required
@safe_route
def api_qb_create_po():
    """Create a Purchase Order in QuickBooks."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    data = request.get_json(silent=True) or {}
    vendor_id = data.get("vendor_id", "")
    items = data.get("items", [])
    if not vendor_id or not items:
        return jsonify({"ok": False, "error": "Provide vendor_id and items"})
    result = create_purchase_order(vendor_id, items,
                                   memo=data.get("memo", ""),
                                   ship_to=data.get("ship_to", ""))
    if result:
        return jsonify({"ok": True, "po": result})
    return jsonify({"ok": False, "error": "PO creation failed — check QB credentials"})


@bp.route("/api/qb/pos")
@auth_required
@safe_route
def api_qb_recent_pos():
    """Get recent Purchase Orders from QuickBooks."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    try:
        days = max(1, min(int(request.args.get("days", 30)), 365))
    except (ValueError, TypeError, OverflowError):
        days = 30
    pos = get_recent_purchase_orders(days_back=days)
    return jsonify({"ok": True, "purchase_orders": pos, "count": len(pos)})


@bp.route("/api/qb/invoices")
@auth_required
@safe_route
def api_qb_invoices():
    """Get invoices from QuickBooks. ?status=open|overdue|paid|all"""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    status = request.args.get("status", "all")
    force = request.args.get("refresh", "").lower() in ("true", "1")
    invoices = fetch_invoices(status=status, force_refresh=force)
    return jsonify({"ok": True, "invoices": invoices, "count": len(invoices)})


@bp.route("/api/qb/invoices/summary")
@auth_required
@safe_route
def api_qb_invoice_summary():
    """Get invoice metrics: open, overdue, paid counts and totals."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    return jsonify({"ok": True, **get_invoice_summary()})


@bp.route("/api/qb/invoices/create", methods=["POST"])
@auth_required
@safe_route
def api_qb_create_invoice():
    """Create an invoice in QuickBooks.
    POST: {customer_id, items: [{description, qty, unit_price}], po_number, memo}"""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    data = request.get_json(silent=True) or {}
    cid = data.get("customer_id", "")
    items = data.get("items", [])
    if not cid or not items:
        return jsonify({"ok": False, "error": "Provide customer_id and items"})
    result = create_invoice(cid, items, po_number=data.get("po_number", ""), memo=data.get("memo", ""))
    if result:
        return jsonify({"ok": True, "invoice": result})
    return jsonify({"ok": False, "error": "Invoice creation failed"})


@bp.route("/api/qb/customers")
@auth_required
@safe_route
def api_qb_customers():
    """List QuickBooks customers with balances."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    force = request.args.get("refresh", "").lower() in ("true", "1")
    customers = fetch_customers(force_refresh=force)
    return jsonify({"ok": True, "customers": customers, "count": len(customers)})


@bp.route("/api/qb/customers/create", methods=["POST"])
@auth_required
@safe_route
def api_qb_customer_create():
    """Create a new customer in QuickBooks. POST {name, email, bill_address}"""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "Customer name is required"})
    existing = qb_find_customer(name)
    if existing:
        return jsonify({"ok": True, "customer": existing, "message": "Customer already exists in QB"})
    try:
        from src.agents.quickbooks_agent import create_customer
        result = create_customer(name, email=data.get("email", ""),
                                  bill_address=data.get("bill_address", ""))
        if result:
            return jsonify({"ok": True, "customer": result})
        return jsonify({"ok": False, "error": "QuickBooks API error"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/customers/balances")
@auth_required
@safe_route
def api_qb_customer_balances():
    """Customer balance summary: total AR, top balances."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    return jsonify({"ok": True, **get_customer_balance_summary()})


@bp.route("/api/qb/financial-context")
@auth_required
@safe_route
def api_qb_financial_context():
    """Comprehensive financial snapshot for all agents.
    Pulls invoices, customers, vendors — cached 1 hour."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    force = request.args.get("refresh", "").lower() in ("true", "1")
    ctx = get_financial_context(force_refresh=force)
    return jsonify(ctx)


@bp.route("/api/qb/company")
@auth_required
@safe_route
def api_qb_company_info():
    """Get QuickBooks company information."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    info = qb_company_info()
    if info:
        return jsonify({"ok": True, "company": info})
    return jsonify({"ok": False, "error": "Failed to fetch company info — check connection"})


@bp.route("/api/qb/pnl")
@auth_required
@safe_route
def api_qb_profit_loss():
    """Get Profit & Loss report. ?start=2026-01-01&end=2026-12-31"""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    start = request.args.get("start")
    end = request.args.get("end")
    pnl = qb_profit_loss(start_date=start, end_date=end)
    if pnl:
        return jsonify({"ok": True, "profit_loss": pnl})
    return jsonify({"ok": False, "error": "Failed to fetch P&L report"})


@bp.route("/api/qb/aging")
@auth_required
@safe_route
def api_qb_ar_aging():
    """Get AR Aging Summary report."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    aging = qb_ar_aging()
    if aging:
        return jsonify({"ok": True, **aging})
    return jsonify({"ok": False, "error": "Failed to fetch aging report"})


@bp.route("/api/qb/payments")
@auth_required
@safe_route
def api_qb_recent_payments():
    """Get recent payments received. ?days=30"""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    days = int(request.args.get("days", 30))
    payments = qb_recent_payments(days_back=days)
    return jsonify({"ok": True, "payments": payments, "count": len(payments)})


@bp.route("/api/qb/diagnose")
@auth_required
@safe_route
def api_qb_diagnose():
    """Full diagnostic of QB connection — token status, API reachability."""
    if not QB_AVAILABLE:
        return jsonify({"ok": False, "error": "QuickBooks agent module not available"})
    diag = qb_diagnose()
    return jsonify({"ok": True, "diagnostic": diag})


@bp.route("/api/qb/sync-vendors", methods=["POST"])
@auth_required
@safe_route
def api_qb_sync_vendors():
    """Pull QB vendors and merge into product catalog as suppliers."""
    if not QB_AVAILABLE:
        return jsonify({"ok": False, "error": "QB not available"})
    try:
        vendors = qb_fetch_vendors(force_refresh=True)
        synced = 0
        from src.core.db import get_db
        with get_db() as conn:
            for v in vendors:
                name = v.get("CompanyName") or v.get("DisplayName", "")
                if not name: continue
                conn.execute("""
                    INSERT OR IGNORE INTO vendors (name, qb_vendor_id, email, phone, status, created_at)
                    VALUES (?, ?, ?, ?, 'active', datetime('now'))
                """, (name, v.get("Id", ""),
                      (v.get("PrimaryEmailAddr") or {}).get("Address", ""),
                      (v.get("PrimaryPhone") or {}).get("FreeFormNumber", "")))
                synced += 1
        return jsonify({"ok": True, "vendors_pulled": len(vendors), "synced_to_db": synced})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/auto-invoice", methods=["POST"])
@auth_required
@safe_route
def api_qb_auto_invoice():
    """Create QB invoice from a won PC/quote."""
    if not QB_AVAILABLE:
        return jsonify({"ok": False, "error": "QB not available"})
    data = request.get_json(silent=True) or {}
    pc_id = data.get("pc_id", "")
    try:
        pcs = _load_price_checks()
        pc = pcs.get(pc_id)
        if not pc:
            return jsonify({"ok": False, "error": "PC not found"})

        items_for_inv = []
        for it in pc.get("items", []):
            if it.get("no_bid"): continue
            price = it.get("unit_price") or it.get("pricing", {}).get("recommended_price") or 0
            if price <= 0: continue
            items_for_inv.append({
                "description": (it.get("description") or "")[:200],
                "quantity": it.get("qty", 1),
                "unit_price": float(price),
            })

        if not items_for_inv:
            return jsonify({"ok": False, "error": "No priced items to invoice"})

        # Find customer in QB
        institution = pc.get("institution", "")
        customer = qb_find_customer(institution) if institution else None
        customer_id = customer["Id"] if customer else None

        result = qb_create_invoice(
            customer_id=customer_id,
            items=items_for_inv,
            po_number=pc.get("pc_number", ""),
            memo=f"Reytech Quote #{pc.get('reytech_quote_number', '')} — {institution}",
        )
        if result and result.get("Id"):
            pc["qb_invoice_id"] = result["Id"]
            pc["qb_invoice_number"] = result.get("DocNumber", "")
            _save_price_checks(pcs)
            return jsonify({"ok": True, "invoice_id": result["Id"],
                          "invoice_number": result.get("DocNumber", "")})
        return jsonify({"ok": False, "error": "QB returned empty result"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/quickbooks")
@auth_required
@safe_page
def quickbooks_dashboard():
    """QuickBooks integration dashboard."""
    from src.api.render import render_page
    return render_page("quickbooks.html", title="QuickBooks")


# ─── CRM Activity Routes (Phase 16) ───────────────────────────────────────

@bp.route("/api/crm/activity")
@auth_required
@safe_route
def api_crm_activity():
    """Get CRM activity feed. ?ref_id=R26Q1&type=quote_won&institution=CSP&limit=50"""
    ref_id = request.args.get("ref_id")
    event_type = request.args.get("type")
    institution = request.args.get("institution")
    limit = min(int(request.args.get("limit", 50)), 200)
    activity = _get_crm_activity(ref_id=ref_id, event_type=event_type,
                                  institution=institution, limit=limit)
    return jsonify({"ok": True, "activity": activity, "count": len(activity)})


@bp.route("/api/crm/activity", methods=["POST"])
@auth_required
@safe_route
def api_crm_log_activity():
    """Manually log a CRM activity. POST JSON {ref_id, event_type, description}"""
    data = request.get_json(silent=True) or {}
    ref_id = data.get("ref_id", "")
    event_type = data.get("event_type", "note")
    description = data.get("description", "")
    if not description:
        return jsonify({"ok": False, "error": "description required"})
    _log_crm_activity(ref_id, event_type, description, actor="user",
                       metadata=data.get("metadata", {}))
    return jsonify({"ok": True})


@bp.route("/api/crm/agency/<agency_name>")
@auth_required
@safe_route
def api_crm_agency_summary(agency_name):
    """Agency CRM summary — quotes, win rate, recent activity, last contact."""
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"})

    quotes = get_all_quotes()
    agency_quotes = [q for q in quotes
                     if q.get("agency", "").lower() == agency_name.lower()
                     or q.get("institution", "").lower().startswith(agency_name.lower())]

    won = [q for q in agency_quotes if q.get("status") == "won"]
    lost = [q for q in agency_quotes if q.get("status") == "lost"]
    pending = [q for q in agency_quotes if q.get("status") in ("pending", "sent")]
    expired = [q for q in agency_quotes if q.get("status") == "expired"]

    total_won = sum(q.get("total", 0) for q in won)
    total_quoted = sum(q.get("total", 0) for q in agency_quotes)
    decided = len(won) + len(lost)
    win_rate = round(len(won) / decided * 100, 1) if decided else 0

    # Unique institutions
    institutions = list(set(q.get("institution", "") for q in agency_quotes if q.get("institution")))

    # Recent activity for this agency
    activity = _get_crm_activity(institution=agency_name, limit=20)

    # Last contact date
    last_contact = None
    for a in activity:
        if a.get("event_type") in ("email_sent", "voice_call", "quote_sent"):
            last_contact = a.get("timestamp")
            break

    return jsonify({
        "ok": True,
        "agency": agency_name,
        "total_quotes": len(agency_quotes),
        "won": len(won), "lost": len(lost),
        "pending": len(pending), "expired": len(expired),
        "total_won_value": total_won,
        "total_quoted_value": total_quoted,
        "win_rate": win_rate,
        "institutions": sorted(institutions),
        "last_contact": last_contact,
        "recent_activity": activity[:10],
    })


# ─── CRM Contact Routes (contact-level activity, persisted separately) ────────

CRM_CONTACTS_FILE = os.path.join(DATA_DIR, "crm_contacts.json")

def _load_crm_contacts() -> dict:
    """Load CRM contacts. SQLite is authoritative, JSON fallback."""
    try:
        from src.core.db import get_all_contacts
        contacts = get_all_contacts()
        if contacts:
            return contacts
    except Exception as _e:
        log.debug("suppressed: %s", _e)
    return _cached_json_load(CRM_CONTACTS_FILE, fallback={})

def _save_crm_contacts(contacts: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CRM_CONTACTS_FILE, "w") as f:
        json.dump(contacts, f, indent=2, default=str)
    _invalidate_cache(CRM_CONTACTS_FILE)
    # ── Also persist to SQLite ──
    try:
        from src.core.db import upsert_contact
        for cid, c in contacts.items():
            c_copy = dict(c)
            c_copy["id"] = cid
            upsert_contact(c_copy)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

def _get_or_create_crm_contact(prospect_id: str, prospect: dict = None) -> dict:
    """Get or create a CRM contact record, merging SCPRS intel data."""
    contacts = _load_crm_contacts()
    if prospect_id not in contacts:
        pr = prospect or {}
        contacts[prospect_id] = {
            "id": prospect_id,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "buyer_name": pr.get("buyer_name",""),
            "buyer_email": pr.get("buyer_email",""),
            "buyer_phone": pr.get("buyer_phone",""),
            "agency": pr.get("agency",""),
            "title": "",
            "linkedin": "",
            "notes": "",
            "tags": [],
            "preferred_contact": "",  # email, phone, text, in_person
            "follow_up_date": "",     # ISO date for next follow-up
            "source": pr.get("source", "auto"),  # auto, manual, email, import
            "hidden": False,
            "ignored": False,
            # SCPRS intel snapshot (updated on each sync)
            "total_spend": pr.get("total_spend", 0),
            "po_count": pr.get("po_count", 0),
            "categories": pr.get("categories", {}),
            "items_purchased": pr.get("items_purchased", []),
            "purchase_orders": pr.get("purchase_orders", []),
            "last_purchase": pr.get("last_purchase",""),
            "score": pr.get("score", 0),
            "outreach_status": pr.get("outreach_status","new"),
            # Activity log (all emails, calls, chats, notes)
            "activity": [],
        }
        _save_crm_contacts(contacts)
    return contacts[prospect_id]


@bp.route("/api/crm/contact/<contact_id>/log", methods=["POST"])
@auth_required
@safe_route
def api_crm_contact_log(contact_id):
    """Log an activity (email, call, chat, note) for a contact.
    POST JSON: {event_type, detail, actor, subject?, direction?, outcome?, channel?, duration?}
    """
    data = request.get_json(silent=True) or {}
    event_type = data.get("event_type") or data.get("type") or "note"
    detail = data.get("detail","").strip()
    actor = data.get("actor","mike")

    if not detail:
        return jsonify({"ok": False, "error": "detail is required"})

    # Build activity entry
    entry = {
        "id": f"act-{datetime.now().strftime('%Y%m%d%H%M%S')}-{contact_id[:6]}",
        "event_type": event_type,
        "detail": detail,
        "actor": actor,
        "timestamp": datetime.now().isoformat(),
    }
    # Attach extra fields based on type
    for field in ("subject","direction","outcome","channel","duration","amount"):
        if data.get(field):
            entry[field] = data[field]

    # Persist to CRM contacts store
    contacts = _load_crm_contacts()
    if contact_id not in contacts:
        # Try to fetch prospect data to hydrate
        try:
            if GROWTH_AVAILABLE:
                pr_result = get_prospect(contact_id)
                pr = pr_result.get("prospect", {}) if pr_result.get("ok") else {}
            else:
                pr = {}
        except Exception:
            pr = {}
        contacts[contact_id] = _get_or_create_crm_contact(contact_id, pr)
        contacts = _load_crm_contacts()  # reload after create

    if contact_id in contacts:
        contacts[contact_id].setdefault("activity", []).append(entry)
        # Keep newest 500 entries per contact
        contacts[contact_id]["activity"] = contacts[contact_id]["activity"][-500:]
        _save_crm_contacts(contacts)

    # Also write to global CRM activity log (for cross-contact views)
    metadata = {k: v for k, v in entry.items() if k not in ("id","event_type","detail","actor","timestamp")}
    _log_crm_activity(
        ref_id=contact_id,
        event_type=event_type,
        description=detail,
        actor=actor,
        metadata=metadata,
    )

    # ── Persist to SQLite activity_log ──
    try:
        from src.core.db import log_activity
        log_activity(
            contact_id=contact_id,
            event_type=event_type,
            subject=entry.get("subject",""),
            body=detail,
            outcome=entry.get("outcome",""),
            actor=actor,
            metadata=metadata,
        )
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Auto-update prospect status on meaningful interactions
    if GROWTH_AVAILABLE and event_type in ("email_sent","voice_called","chat","meeting"):
        try:
            update_prospect(contact_id, {"outreach_status": "emailed" if event_type=="email_sent" else "called"})
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

    return jsonify({"ok": True, "entry": entry, "contact_id": contact_id})


@bp.route("/api/crm/contact/<contact_id>")
@auth_required
@safe_route
def api_crm_contact_get(contact_id):
    """Get full CRM contact record including all logged activity."""
    contacts = _load_crm_contacts()
    if contact_id not in contacts:
        # Hydrate from prospect if available
        if GROWTH_AVAILABLE:
            try:
                pr_result = get_prospect(contact_id)
                if pr_result.get("ok"):
                    contacts[contact_id] = _get_or_create_crm_contact(contact_id, pr_result["prospect"])
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
    contact = contacts.get(contact_id)
    if not contact:
        return jsonify({"ok": False, "error": "Contact not found"})
    # Merge global CRM activity
    global_events = _get_crm_activity(ref_id=contact_id, limit=200)
    contact["global_activity"] = global_events
    return jsonify({"ok": True, "contact": contact})


@bp.route("/api/crm/contact/<contact_id>", methods=["PATCH"])
@auth_required
@safe_route
def api_crm_contact_update(contact_id):
    """Update manual contact fields: name, phone, title, linkedin, notes, tags."""
    data = request.get_json(silent=True) or {}
    contacts = _load_crm_contacts()
    if contact_id not in contacts:
        if GROWTH_AVAILABLE:
            try:
                pr_result = get_prospect(contact_id)
                if pr_result.get("ok"):
                    _get_or_create_crm_contact(contact_id, pr_result["prospect"])
                    contacts = _load_crm_contacts()
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
    if contact_id not in contacts:
        return jsonify({"ok": False, "error": "Contact not found"})

    allowed = {"buyer_name","buyer_phone","buyer_email","title","linkedin","notes","tags",
               "outreach_status","agency","preferred_contact","follow_up_date","source",
               "hidden","ignored"}
    for k, v in data.items():
        if k in allowed:
            contacts[contact_id][k] = v
    contacts[contact_id]["updated_at"] = datetime.now().isoformat()
    _save_crm_contacts(contacts)
    # Sync name/phone back to growth prospects too
    if GROWTH_AVAILABLE:
        try:
            sync = {k: data[k] for k in ("buyer_name","buyer_phone") if k in data}
            if sync:
                update_prospect(contact_id, sync)
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    return jsonify({"ok": True, "contact_id": contact_id})


@bp.route("/api/crm/contact/<contact_id>/delete", methods=["POST"])
@auth_required
@safe_route
def api_crm_contact_delete(contact_id):
    """Delete or hide a CRM contact. POST JSON: {mode: 'hide'|'delete'}"""
    data = request.get_json(silent=True) or {}
    mode = data.get("mode", "hide")
    contacts = _load_crm_contacts()
    if contact_id not in contacts:
        return jsonify({"ok": False, "error": "Contact not found"})
    if mode == "delete":
        del contacts[contact_id]
        _save_crm_contacts(contacts)
        # Also remove from SQLite
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("DELETE FROM contacts WHERE id=?", (contact_id,))
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        # Add to ignore list if email present
        email = data.get("email", "")
        if email and data.get("ignore", False):
            _add_to_ignore_list(email, reason="deleted_contact")
        log.info("CRM contact %s permanently deleted", contact_id)
        return jsonify({"ok": True, "deleted": contact_id})
    else:
        contacts[contact_id]["hidden"] = True
        contacts[contact_id]["hidden_at"] = datetime.now().isoformat()
        _save_crm_contacts(contacts)
        log.info("CRM contact %s hidden", contact_id)
        return jsonify({"ok": True, "hidden": contact_id})


@bp.route("/api/crm/contact/<contact_id>/tags", methods=["POST"])
@auth_required
@safe_route
def api_crm_contact_tags(contact_id):
    """Add or remove tags. POST JSON: {action:'add'|'remove', tag:'Buyer'}"""
    data = request.get_json(silent=True) or {}
    action = data.get("action", "add")
    tag = data.get("tag", "").strip()
    if not tag:
        return jsonify({"ok": False, "error": "Tag required"})
    contacts = _load_crm_contacts()
    if contact_id not in contacts:
        return jsonify({"ok": False, "error": "Contact not found"})
    tags = contacts[contact_id].get("tags", [])
    if not isinstance(tags, list):
        tags = []
    if action == "add" and tag not in tags:
        tags.append(tag)
    elif action == "remove" and tag in tags:
        tags.remove(tag)
    contacts[contact_id]["tags"] = tags
    contacts[contact_id]["updated_at"] = datetime.now().isoformat()
    _save_crm_contacts(contacts)
    return jsonify({"ok": True, "tags": tags})


# ── Email ignore list ──
IGNORE_LIST_FILE = os.path.join(DATA_DIR, "crm_ignore_list.json")

def _load_ignore_list() -> list:
    return _cached_json_load(IGNORE_LIST_FILE, fallback=[])

def _save_ignore_list(entries: list):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(IGNORE_LIST_FILE, "w") as f:
        json.dump(entries, f, indent=2, default=str)
    _invalidate_cache(IGNORE_LIST_FILE)

def _add_to_ignore_list(email: str, reason: str = ""):
    entries = _load_ignore_list()
    if not any(e.get("email","").lower() == email.lower() for e in entries):
        entries.append({
            "email": email.lower().strip(),
            "reason": reason,
            "added_at": datetime.now().isoformat(),
        })
        _save_ignore_list(entries)

def is_ignored_email(email: str) -> bool:
    """Check if an email is on the ignore list. Used by email poller."""
    entries = _load_ignore_list()
    return any(e.get("email","").lower() == email.lower().strip() for e in entries)


@bp.route("/api/crm/ignore-list")
@auth_required
@safe_route
def api_crm_ignore_list():
    """Get the email ignore list."""
    return jsonify({"ok": True, "entries": _load_ignore_list()})


@bp.route("/api/crm/ignore-list", methods=["POST"])
@auth_required
@safe_route
def api_crm_ignore_list_add():
    """Add/remove from ignore list. POST JSON: {email, action:'add'|'remove', reason?}"""
    data = request.get_json(silent=True) or {}
    email = data.get("email", "").strip().lower()
    action = data.get("action", "add")
    if not email:
        return jsonify({"ok": False, "error": "Email required"})
    entries = _load_ignore_list()
    if action == "add":
        _add_to_ignore_list(email, data.get("reason", "manual"))
        entries = _load_ignore_list()
    elif action == "remove":
        entries = [e for e in entries if e.get("email","").lower() != email]
        _save_ignore_list(entries)
    return jsonify({"ok": True, "entries": _load_ignore_list()})


@bp.route("/api/crm/contacts")
@auth_required
@safe_route
def api_crm_contacts_list():
    """List all CRM contacts with activity counts and last interaction."""
    contacts = _load_crm_contacts()
    show_hidden = request.args.get("show_hidden", "0") == "1"
    result = []
    for cid, c in contacts.items():
        if c.get("hidden") and not show_hidden:
            continue
        activity = c.get("activity", [])
        last_act = activity[-1].get("timestamp","") if activity else ""
        result.append({
            "id": cid,
            "buyer_name": c.get("buyer_name",""),
            "buyer_email": c.get("buyer_email",""),
            "agency": c.get("agency",""),
            "outreach_status": c.get("outreach_status","new"),
            "total_spend": c.get("total_spend",0),
            "categories": list(c.get("categories",{}).keys()),
            "activity_count": len(activity),
            "last_activity": last_act,
            "score": c.get("score",0),
            "tags": c.get("tags",[]),
            "follow_up_date": c.get("follow_up_date",""),
            "hidden": c.get("hidden", False),
        })
    result.sort(key=lambda x: x.get("last_activity",""), reverse=True)
    return jsonify({"ok": True, "contacts": result, "total": len(result)})


@bp.route("/api/crm/sync-intel", methods=["POST"])
@auth_required
@safe_route
def api_crm_sync_intel():
    """Sync all intel buyers into CRM contacts store.
    Preserves manual fields (phone, title, linkedin, notes, activity).
    Updates SCPRS intel fields (spend, categories, items, POs).
    """
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Intel not available"})
    result = sync_buyers_to_crm()
    _invalidate_cache(os.path.join(DATA_DIR, "crm_contacts.json"))
    return jsonify(result)


# ─── Lead Generation Routes ─────────────────────────────────────────────────

@bp.route("/api/shipping/detected")
@auth_required
@safe_route
def api_shipping_detected():
    """Get recently detected shipping emails."""
    ship_file = os.path.join(DATA_DIR, "detected_shipments.json")
    try:
        with open(ship_file) as f:
            shipments = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        shipments = []
    limit = min(int(request.args.get("limit", 20)), 200)
    shipments = sorted(shipments, key=lambda s: s.get("detected_at", ""), reverse=True)[:limit]
    return jsonify({"ok": True, "shipments": shipments, "count": len(shipments)})



_wp_cache = {"value": 0, "ts": 0}

def _get_weighted_pipeline_cached() -> float:
    """Return probability-weighted pipeline value. Cached 60s to avoid slowing funnel stats."""
    import time as _time
    now = _time.time()
    if now - _wp_cache["ts"] < 60:
        return _wp_cache["value"]
    try:
        from src.core.forecasting import score_all_quotes
        result = score_all_quotes()
        val = result.get("weighted_pipeline", 0)
    except Exception:
        val = 0
    _wp_cache["value"] = val
    _wp_cache["ts"] = now
    return val

@bp.route("/api/funnel/stats")
@auth_required
@safe_route
def api_funnel_stats():
    """Pipeline funnel stats — full business pipeline including Price Checks.
    
    Workflow stages:
      Inbox (new PCs + RFQs) → Priced (PC priced) → Quoted (quote generated) → 
      Sent (quote emailed) → Won (PO received) → Orders (fulfilling) → Pipeline $
    """
    # ── Price Checks ──
    all_pcs = _load_price_checks()
    from src.api.dashboard import _is_user_facing_pc
    user_pcs = {pid: pc for pid, pc in all_pcs.items() if _is_user_facing_pc(pc)}
    pcs_new = sum(1 for pc in user_pcs.values() if pc.get("status") in ("parsed", "new", "parse_error"))
    pcs_priced = sum(1 for pc in user_pcs.values() if pc.get("status") in ("priced", "ready", "auto_drafted"))
    pcs_quoted = sum(1 for pc in user_pcs.values() if pc.get("status") in ("quoted", "generated"))
    pcs_sent = sum(1 for pc in user_pcs.values() if pc.get("status") in ("sent", "completed"))
    pcs_total = len(user_pcs)

    # ── RFQs (704B formal packages) ──
    rfqs = load_rfqs()
    rfqs_non_test = {k: v for k, v in rfqs.items() if not v.get("is_test")}
    rfqs_new = sum(1 for r in rfqs_non_test.values() if r.get("status") in ("new", "pending", "parsed"))
    rfqs_priced = sum(1 for r in rfqs_non_test.values() if r.get("status") in ("priced", "ready"))
    rfqs_quoted = sum(1 for r in rfqs_non_test.values() if r.get("status") in ("generated", "quoted"))
    rfqs_sent = sum(1 for r in rfqs_non_test.values() if r.get("status") == "sent")

    # ── Combined inbox = new PCs + new RFQs ──
    inbox_count = pcs_new + rfqs_new
    priced_count = pcs_priced + rfqs_priced
    
    # ── Quotes — use best available data source (SQLite or JSON) ──
    # Try SQLite first (most accurate totals), fall back to JSON if SQLite is empty
    quotes_pending = 0
    quotes_sent = 0
    quotes_won = 0
    quotes_lost = 0
    total_quoted = 0
    total_won = 0
    pipeline_value_db = 0
    _q_source = "none"
    try:
        from src.core.db import get_db
        with get_db() as _conn:
            # Count ALL non-test quotes (don't filter by total>0 for counts)
            db_total = _conn.execute(
                "SELECT COUNT(*) FROM quotes WHERE is_test = 0"
            ).fetchone()[0]
            if db_total > 0:
                _q_source = "sqlite"
                for r in _conn.execute("""
                    SELECT status, COUNT(*) as c, COALESCE(SUM(total), 0) as t
                    FROM quotes WHERE is_test = 0
                    GROUP BY status
                """).fetchall():
                    s = r["status"]
                    if s in ("pending", "draft"):
                        quotes_pending = r["c"]
                        pipeline_value_db += r["t"]
                    elif s == "sent":
                        quotes_sent = r["c"]
                        pipeline_value_db += r["t"]
                    elif s == "won":
                        quotes_won = r["c"]
                        total_won = r["t"]
                    elif s == "lost":
                        quotes_lost = r["c"]
                    total_quoted += r["t"]
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Fallback to JSON if SQLite had no data
    if _q_source == "none":
        _q_source = "json"
        quotes = [q for q in get_all_quotes() if not q.get("is_test")]
        quotes_pending = sum(1 for q in quotes if q.get("status") in ("pending", "draft"))
        quotes_sent = sum(1 for q in quotes if q.get("status") == "sent")
        quotes_won = sum(1 for q in quotes if q.get("status") == "won")
        quotes_lost = sum(1 for q in quotes if q.get("status") == "lost")
        total_quoted = sum(q.get("total", 0) for q in quotes)
        total_won = sum(q.get("total", 0) for q in quotes if q.get("status") == "won")
        pipeline_value_db = sum(q.get("total", 0) for q in quotes
                                if q.get("status") in ("pending", "sent", "draft"))

    # Quoted = formal quotes generated (pending/draft status = not yet sent)
    quoted_count = quotes_pending + pcs_quoted + rfqs_quoted
    # Sent = quotes actually sent + PCs/RFQs marked sent
    sent_count = quotes_sent + pcs_sent + rfqs_sent

    # ── Orders ──
    all_orders = _load_orders()
    orders = {k: v for k, v in all_orders.items() if not v.get("is_test")}
    orders_active = sum(1 for o in orders.values() if o.get("status") not in ("closed",))
    orders_total = len(orders)
    items_shipped = 0
    items_delivered = 0
    for o in orders.values():
        for it in o.get("line_items", []):
            if it.get("sourcing_status") in ("shipped", "delivered"):
                items_shipped += 1
            if it.get("sourcing_status") == "delivered":
                items_delivered += 1
    order_value = sum(o.get("total", 0) for o in orders.values())
    invoiced_value = sum(o.get("invoice_total", 0) for o in orders.values())

    # Won = MAX(quotes_won, orders_total) — if we have orders, we won at least that many
    won_count = max(quotes_won, orders_total)

    # ── Leads ──
    try:
        from src.core.dal import get_all_leads
        leads = get_all_leads()
        leads_count = len(leads)
        hot_leads = sum(1 for l in leads
                        if isinstance(l, dict) and l.get("score", 0) >= 0.7)
    except Exception:
        leads_count = 0
        hot_leads = 0

    # Win rate
    decided = won_count + quotes_lost
    win_rate = round(won_count / decided * 100) if decided > 0 else 0

    # Pipeline value = already computed from SQLite above
    pipeline_value = pipeline_value_db

    # QuickBooks financial data
    qb_receivable = 0
    qb_overdue = 0
    qb_collected = 0
    qb_open_invoices = 0
    if QB_AVAILABLE and qb_configured():
        try:
            ctx = get_financial_context()
            if ctx.get("ok"):
                qb_receivable = ctx.get("total_receivable", 0)
                qb_overdue = ctx.get("overdue_amount", 0)
                qb_collected = ctx.get("total_collected", 0)
                qb_open_invoices = ctx.get("open_invoices", 0)
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

    # Next quote number + CRM stats
    next_quote = ""
    crm_contacts_count = 0
    intel_buyers_count = 0
    try:
        next_quote = peek_next_quote_number() if QUOTE_GEN_AVAILABLE else ""
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    try:
        crm_contacts_count = len(_load_crm_contacts())
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    if INTEL_AVAILABLE:
        try:
            from src.agents.sales_intel import _load_json as _il, BUYERS_FILE as _BF
            bd = _il(_BF)
            intel_buyers_count = bd.get("total_buyers", 0) if isinstance(bd, dict) else 0
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

    return jsonify({
        "ok": True,
        "next_quote": next_quote,
        # New combined pipeline stages
        "inbox": inbox_count,         # New PCs + new RFQs awaiting pricing
        "priced": priced_count,       # Priced, awaiting quote generation
        "quoted": quoted_count,       # Quote generated, not yet sent
        "sent": sent_count,           # Quote sent to customer
        "won": won_count,            # PO received (max of quotes_won, orders)
        "orders_active": orders_active,
        "pipeline_value": pipeline_value,
        # Legacy fields (keep for backward compat)
        "rfqs_active": inbox_count,
        "quotes_pending": quoted_count,
        "quotes_sent": sent_count,
        "quotes_won": won_count,
        "quotes_lost": quotes_lost,
        "orders_total": orders_total,
        "items_shipped": items_shipped,
        "items_delivered": items_delivered,
        "leads_count": leads_count,
        "hot_leads": hot_leads,
        "total_quoted": total_quoted,
        "total_won": total_won,
        "order_value": order_value,
        "invoiced_value": invoiced_value,
        "win_rate": win_rate,
        "crm_contacts": crm_contacts_count,
        "intel_buyers": intel_buyers_count,
        "qb_receivable": qb_receivable,
        "qb_overdue": qb_overdue,
        "qb_collected": qb_collected,
        "qb_open_invoices": qb_open_invoices,
        # PC breakdown for debugging
        "pcs_total": pcs_total,
        "pcs_new": pcs_new,
        "pcs_priced": pcs_priced,
        "pcs_quoted": pcs_quoted,
        "pcs_sent": pcs_sent,
        # PRD Feature 4.4 — weighted pipeline (probability-adjusted)
        "weighted_pipeline": _get_weighted_pipeline_cached(),
    })



@bp.route("/api/leads")
@auth_required
@safe_route
def api_leads_list():
    """Get leads, optionally filtered. ?status=new&min_score=0.6&limit=20"""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    status = request.args.get("status")
    min_score = float(request.args.get("min_score", 0))
    limit = min(int(request.args.get("limit", 50)), 200)
    leads = get_leads(status=status, min_score=min_score, limit=limit)
    return jsonify({"ok": True, "leads": leads, "count": len(leads)})


@bp.route("/api/leads/evaluate", methods=["POST"])
@auth_required
@safe_route
def api_leads_evaluate():
    """Evaluate a PO as a potential lead. POST JSON with PO data."""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    data = request.get_json(silent=True) or {}
    # Load won history for matching
    won_history = []
    try:
        from src.knowledge.won_quotes_db import get_all_items
        won_history = get_all_items()
    except Exception as e:
        log.debug("Suppressed: %s", e)
        pass
    lead = evaluate_po(data, won_history)
    if not lead:
        return jsonify({"ok": True, "qualified": False,
                        "reason": "Below confidence threshold or out of value range"})
    result = add_lead(lead)
    return jsonify({"ok": True, "qualified": True, "lead": lead, **result})


@bp.route("/api/leads/<lead_id>/status", methods=["POST"])
@auth_required
@safe_route
def api_leads_update_status(lead_id):
    """Update lead status. POST JSON: {"status": "contacted", "notes": "..."}"""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    data = request.get_json(silent=True) or {}
    return jsonify(update_lead_status(
        lead_id, data.get("status", ""), data.get("notes", "")))


@bp.route("/api/leads/<lead_id>/draft")
@auth_required
@safe_route
def api_leads_draft(lead_id):
    """Get outreach email draft for a lead."""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    leads = get_leads()
    lead = next((l for l in leads if l["id"] == lead_id), None)
    if not lead:
        return jsonify({"ok": False, "error": "Lead not found"})
    draft = draft_outreach_email(lead)
    return jsonify({"ok": True, **draft})


@bp.route("/api/leads/analytics")
@auth_required
@safe_route
def api_leads_analytics():
    """Lead conversion analytics."""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    return jsonify({"ok": True, **get_lead_analytics()})


# ─── Email Outreach Routes ──────────────────────────────────────────────────

@bp.route("/api/outbox")
@auth_required
@safe_route
def api_outbox_list():
    """Get email outbox. ?status=draft"""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    status = request.args.get("status")
    emails = get_outbox(status=status)
    return jsonify({"ok": True, "emails": emails, "count": len(emails)})


@bp.route("/api/outbox/draft/pc/<pcid>", methods=["POST"])
@auth_required
@safe_route
def api_outbox_draft_pc(pcid):
    """Draft a buyer email for a completed PC."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(silent=True) or {}
    email = draft_for_pc(pc,
                         quote_number=data.get("quote_number", pc.get("quote_number", "")),
                         pdf_path=data.get("pdf_path", ""))
    return jsonify({"ok": True, "email": email})


@bp.route("/api/outbox/draft/lead/<lead_id>", methods=["POST"])
@auth_required
@safe_route
def api_outbox_draft_lead(lead_id):
    """Draft outreach email for a lead."""
    if not OUTREACH_AVAILABLE or not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Required agents not available"})
    leads = get_leads()
    lead = next((l for l in leads if l["id"] == lead_id), None)
    if not lead:
        return jsonify({"ok": False, "error": "Lead not found"})
    email = draft_for_lead(lead)
    return jsonify({"ok": True, "email": email})


@bp.route("/api/outbox/<email_id>/approve", methods=["POST"])
@auth_required
@safe_route
def api_outbox_approve(email_id):
    """Approve a draft email for sending."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify(approve_email(email_id))


@bp.route("/api/outbox/<email_id>/edit", methods=["POST"])
@auth_required
@safe_route
def api_outbox_edit(email_id):
    """Edit a draft. POST JSON: {"to": "...", "subject": "...", "body": "..."}"""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    data = request.get_json(silent=True) or {}
    return jsonify(update_draft(email_id, data))


@bp.route("/api/outbox/<email_id>/send", methods=["POST"])
@auth_required
@safe_route
def api_outbox_send(email_id):
    """Send a specific email."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify(outreach_send(email_id))


@bp.route("/api/outbox/send-approved", methods=["POST"])
@auth_required
@safe_route
def api_outbox_send_all():
    """Send all approved emails.

    UX Audit 2026-04-14 §9.2: hard-disabled until the CS-reply agent
    rewrite ships. Bulk-approving canned text is the class of action
    that can damage real customer relationships, so the endpoint
    rejects with 423 (Locked) instead of just hiding the UI button.
    Runtime override via `outbox.send_approved_enabled=true` feature
    flag for when the rewrite lands.
    """
    try:
        from src.core.flags import get_flag
        if not get_flag("outbox.send_approved_enabled", False):
            return jsonify({
                "ok": False,
                "error": "BLOCKED: Send All Approved is disabled until the "
                         "CS-reply agent rewrite ships. Review drafts "
                         "individually in /outbox.",
                "blocked_reason": "ux_audit_p0_2",
            }), 423
    except Exception as e:
        log.debug("send-approved flag check failed: %s", e)
        # If the flag layer is broken, default-deny — this is a
        # destructive bulk action.
        return jsonify({
            "ok": False,
            "error": "BLOCKED: feature flag layer unavailable, "
                     "defaulting to deny for send-approved.",
        }), 423
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify({"ok": True, **send_approved()})


@bp.route("/api/outbox/<email_id>", methods=["DELETE"])
@auth_required
@safe_route
def api_outbox_delete(email_id):
    """Delete an email from outbox."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify(delete_from_outbox(email_id))


@bp.route("/api/outbox/sent")
@auth_required
@safe_route
def api_outbox_sent_log():
    """Get sent email log."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    limit = min(int(request.args.get("limit", 50)), 200)
    return jsonify({"ok": True, "sent": get_sent_log(limit=limit)})


# ── Growth routes moved to routes_growth_prospects.py ────────────────────────





# ── Notifications API ─────────────────────────────────────────────────────────
@bp.route("/api/notifications")
@auth_required
@safe_route
def api_notifications():
    """Get dashboard notifications (auto-draft alerts, etc.)."""
    unread = [n for n in _notifications if not n.get("read")]
    return jsonify({"ok": True, "notifications": list(_notifications),
                    "unread_count": len(unread)})



# ── _create_quote_from_pc helper (used by email auto-draft) ──────────────────
def _create_quote_from_pc(pc_id: str, status: str = "draft") -> dict:
    """Create a quote from a price check. Wrapper used by Feature 4.2 auto-draft.
    If a draft quote was already created by _handle_price_check_upload, updates it
    with priced items instead of creating a duplicate."""
    try:
        pcs = _load_price_checks()
        pc = pcs.get(pc_id)
        if not pc:
            return {"ok": False, "error": "PC not found"}
        items = pc.get("items", [])
        priced = [i for i in items if i.get("our_price") or i.get("unit_cost")]
        if not priced:
            return {"ok": False, "error": "no_prices"}

        from src.forms.quote_generator import (
            create_quote, peek_next_quote_number, increment_quote_counter,
            get_all_quotes, _save_all_quotes,
        )

        existing_qn = pc.get("linked_quote_number") or pc.get("reytech_quote_number") or ""

        # If a draft quote with this number already exists, UPDATE it instead of creating new
        if existing_qn:
            all_quotes = get_all_quotes()
            for idx, q in enumerate(all_quotes):
                if q.get("quote_number") == existing_qn:
                    # Update the existing bare draft with priced items
                    line_items = []
                    for it in priced:
                        price = it.get("our_price") or it.get("unit_cost") or 0
                        qty = it.get("qty") or 1
                        line_items.append({
                            "description": it.get("description", ""),
                            "qty": qty,
                            "unit_price": price,
                            "total": round(price * qty, 2),
                        })
                    total = sum(i["total"] for i in line_items)
                    all_quotes[idx]["total"] = total
                    all_quotes[idx]["subtotal"] = total
                    all_quotes[idx]["items_count"] = len(line_items)
                    all_quotes[idx]["status"] = status
                    all_quotes[idx]["items"] = line_items
                    _save_all_quotes(all_quotes)
                    log.info("Updated existing draft %s with %d priced items ($%.2f)",
                             existing_qn, len(line_items), total)
                    return {"ok": True, "quote_number": existing_qn, "updated": True}
            # If we got here, linked number exists but no quote found — fall through to create

        # No existing draft — create new (consume a quote number)
        quote_number = existing_qn or peek_next_quote_number()
        agency = pc.get("agency") or pc.get("institution") or ""
        line_items = []
        for it in priced:
            price = it.get("our_price") or it.get("unit_cost") or 0
            qty = it.get("qty") or 1
            line_items.append({
                "description": it.get("description",""),
                "qty": qty,
                "unit_price": price,
                "total": round(price * qty, 2),
            })
        total = sum(i["total"] for i in line_items)
        result = create_quote({
            "quote_number": quote_number,
            "agency": agency,
            "total": total,
            "items": line_items,
            "status": status,
            "source_pc_id": pc_id,
            "feature": "PRD 4.2",
        })
        if result.get("ok") and not existing_qn:
            # Only increment counter if we consumed a new number (not reusing linked)
            increment_quote_counter()
        if result.get("ok"):
            pcs[pc_id]["linked_quote_number"] = quote_number
            _save_price_checks(pcs)
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════════
# SCPRS DEEP PULL SCHEDULER  (PRD Feature 4.5 — P2)
# ════════════════════════════════════════════════════════════════════════════════
import threading as _threading
_scprs_scheduler_thread = None
_scprs_scheduler_state = {"running": False, "cron": "", "last_run": None, "next_run": None}


def _parse_simple_cron(cron_expr: str) -> dict:
    """Parse simple cron: 'sunday 2am', '0 2 * * 0', 'weekly', 'daily', etc."""
    expr = cron_expr.lower().strip()
    if "sunday" in expr or "weekly" in expr or expr == "0 2 * * 0":
        return {"day_of_week": 6, "hour": 2, "minute": 0, "label": "Sundays at 2:00 AM"}
    if "monday" in expr: return {"day_of_week": 0, "hour": 8, "minute": 0, "label": "Mondays at 8:00 AM"}
    if "daily" in expr or "everyday" in expr:
        return {"day_of_week": -1, "hour": 3, "minute": 0, "label": "Daily at 3:00 AM"}
    # Try standard cron: minute hour day month weekday
    parts = expr.split()
    if len(parts) == 5:
        try:
            return {"day_of_week": int(parts[4]) % 7, "hour": int(parts[1]),
                    "minute": int(parts[0]), "label": f"cron: {expr}"}
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    return {"day_of_week": 6, "hour": 2, "minute": 0, "label": "Sundays at 2:00 AM"}


# Default dual schedule (PRD spec: Monday 7am + Wednesday 10am PST)
_SCPRS_DEFAULT_SCHEDULES = [
    {"day_of_week": 0, "hour": 7, "minute": 0,  "label": "Monday 7:00 AM PST"},
    {"day_of_week": 2, "hour": 10, "minute": 0, "label": "Wednesday 10:00 AM PST"},
]


def _scprs_scheduler_loop(cron_expr: str = "", run_now: bool = False, schedules: list = None):
    """Background thread: SCPRS deep pull on dual schedule.

    Defaults: Monday 7:00 AM PST + Wednesday 10:00 AM PST.
    PST applied via UTC-8 offset (configurable via PST_OFFSET_HOURS env var).
    """
    import time as _time
    from datetime import timezone, timedelta as _tds
    _scprs_scheduler_state["running"] = True
    _scprs_scheduler_state["cron"] = cron_expr or "Mon 7am + Wed 10am PST"

    sched_list = schedules or ([_parse_simple_cron(cron_expr)] if cron_expr else _SCPRS_DEFAULT_SCHEDULES)
    _scprs_scheduler_state["schedules"] = [s["label"] for s in sched_list]
    log.info("SCPRS Scheduler started: %s", [s["label"] for s in sched_list])

    if run_now:
        _run_scheduled_scprs_pull()

    while _scprs_scheduler_state["running"]:
        tz_offset = int(os.environ.get("PST_OFFSET_HOURS", "-8"))
        now_pst = datetime.now(timezone.utc) + _tds(hours=tz_offset)
        dow = now_pst.weekday()  # 0=Mon, 6=Sun

        for sched in sched_list:
            if ((sched["day_of_week"] == -1 or dow == sched["day_of_week"])
                    and now_pst.hour == sched["hour"]
                    and now_pst.minute == sched.get("minute", 0)):
                log.info("SCPRS Scheduler: triggering pull (%s)", sched["label"])
                _run_scheduled_scprs_pull()
                _time.sleep(70)
                break
        _time.sleep(30)


def _run_scheduled_scprs_pull():
    """Execute a SCPRS deep pull and auto-sync to CRM."""
    _scprs_scheduler_state["last_run"] = datetime.now().isoformat()
    try:
        if INTEL_AVAILABLE:
            from src.agents.sales_intel import run_deep_pull
            result = run_deep_pull(max_items=200)
            log.info("Scheduled SCPRS pull: %s", result)
            # Auto-sync to CRM
            if result.get("ok"):
                sync_buyers_to_crm()
                log.info("Scheduled SCPRS pull: CRM sync complete")
            _scprs_scheduler_state["result"] = result
    except Exception as e:

        _scprs_scheduler_state["error"] = str(e)


# ── SCPRS Scheduler auto-start on module load ────────────────────────────────
# Defaults to Monday 7am + Wednesday 10am PST.
# Set SCPRS_PULL_SCHEDULE env var to override (or "off" to disable).
def _scprs_autostart():
    _env = os.environ.get("SCPRS_PULL_SCHEDULE", "auto")
    if _env.lower() in ("off", "disable", "disabled", "false", "0"):
        log.info("SCPRS scheduler disabled via SCPRS_PULL_SCHEDULE=off")
        return
    _cron = "" if _env == "auto" else _env
    _label = "Mon 7am PST + Wed 10am PST" if _env == "auto" else _env
    threading.Thread(target=_full_scprs_scheduler_loop, daemon=True, name="scprs-intel").start()
    threading.Thread(target=_scprs_scheduler_loop, args=(_cron,), daemon=True, name="scprs-sched").start()
    log.info("SCPRS schedulers started: %s", _label)

    # Auto-backfill: if SCPRS tables are empty, start 2025 backfill
    def _auto_backfill():
        import time as _t2
        _t2.sleep(300)  # Wait 5 min for app to stabilize
        try:
            from src.core.db import get_db
            with get_db() as _conn:
                po_count = _conn.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]
            if po_count < 500:
                log.info("SCPRS has only %d POs — starting 2025 backfill automatically", po_count)
                try:
                    from src.agents.scprs_intelligence_engine import backfill_historical
                    result = backfill_historical(year=2025, notify_fn=_notify_wrapper, force=True)
                    log.info("Auto-backfill result: %s", result)
                except Exception as e:
                    log.error("Auto-backfill FAILED: %s", e)
                    import traceback
                    log.error("Auto-backfill traceback: %s", traceback.format_exc())
            else:
                log.info("SCPRS has %d POs — skipping auto-backfill", po_count)
        except Exception as e:
            log.error("Auto-backfill check error: %s", e)
            import traceback
            log.error("Auto-backfill traceback: %s", traceback.format_exc())

    threading.Thread(target=_auto_backfill, daemon=True, name="scprs-auto-backfill").start()

    try:
        from src.agents.notify_agent import start_stale_watcher
        start_stale_watcher()
    except Exception as _sw:
        log.debug("Stale watcher: %s", _sw)

def _full_scprs_scheduler_loop():
    """
    Master SCPRS intelligence scheduler.
    Runs in background — pulls all agencies on schedule, runs PO monitor daily.
    """
    import time as _time
    from datetime import datetime as _dt, timezone as _tz

    log.info("SCPRS Intelligence Scheduler started")

    # Wait 3 min after startup before first pull
    _time.sleep(180)

    while True:
        try:
            now = _dt.now(_tz.utc)
            hour = now.hour
            weekday = now.weekday()  # 0=Mon

            # Run scheduled agency pulls
            try:
                from src.agents.scprs_intelligence_engine import run_scheduled_pulls
                run_scheduled_pulls(notify_fn=_notify_wrapper)
            except Exception as e:
                log.error(f"Scheduled pull error: {e}")

            # Run PO award monitor daily at 8am
            if hour == 8 and now.minute < 30:
                try:
                    from src.agents.scprs_intelligence_engine import run_po_award_monitor
                    result = run_po_award_monitor(notify_fn=_notify_wrapper)
                    if result.get("auto_closed_lost", 0) > 0:
                        log.info(f"PO Monitor: {result['auto_closed_lost']} quotes auto-closed")
                except Exception as e:
                    log.error(f"PO monitor scheduled: {e}")

            # Monthly full pull — 1st of each month at 2am
            if now.day == 1 and hour == 2 and now.minute < 30:
                try:
                    from src.agents.scprs_intelligence_engine import run_monthly_full_pull
                    log.info("Monthly full SCPRS pull starting...")
                    run_monthly_full_pull(notify_fn=_notify_wrapper)
                except Exception as e:
                    log.error(f"Monthly full pull: {e}")

        except Exception as e:
            log.error(f"SCPRS scheduler: {e}")

        # Check every 30 minutes
        _time.sleep(1800)

if os.environ.get("ENABLE_BACKGROUND_AGENTS", "true").lower() not in ("false", "0", "off"):
    _scprs_autostart()


@bp.route("/api/intel/pull/schedule", methods=["GET", "POST"])
@auth_required
@safe_route
def api_intel_pull_schedule():
    """Configure SCPRS auto-pull schedule.

    GET: Return current schedule status
    POST { cron: "sunday 2am", run_now: false }: Set schedule
         cron examples: "sunday 2am", "daily", "0 2 * * 0" (standard cron)
    """
    try:
        global _scprs_scheduler_thread

        if request.method == "GET":
            return jsonify({
                "ok": True,
                "scheduler": _scprs_scheduler_state,
                "hint": "POST {cron: 'sunday 2am'} to enable. Also set SCPRS_PULL_SCHEDULE env var for persistence.",
            })

        body = request.get_json(silent=True) or {}
        cron = body.get("cron", os.environ.get("SCPRS_PULL_SCHEDULE", ""))
        run_now = body.get("run_now", False)
        # Support custom schedule list or use the default dual schedule
        custom_schedules = body.get("schedules")

        # Stop existing thread
        _scprs_scheduler_state["running"] = False
        if _scprs_scheduler_thread and _scprs_scheduler_thread.is_alive():
            _scprs_scheduler_thread = None

        # Start new thread with dual schedule by default
        _scprs_scheduler_thread = _threading.Thread(
            target=_scprs_scheduler_loop,
            args=(cron,),
            kwargs={"run_now": run_now, "schedules": custom_schedules},
            daemon=True, name="scprs-scheduler"
        )
        _scprs_scheduler_thread.start()

        labels = custom_schedules or (_SCPRS_DEFAULT_SCHEDULES if not cron else [_parse_simple_cron(cron)])
        label_str = " + ".join(s if isinstance(s, str) else s["label"] for s in labels)
        _scprs_scheduler_state["schedule_label"] = label_str
        _scprs_scheduler_state["next_run"] = label_str

        log.info("SCPRS Scheduler: enabled (%s, run_now=%s)", label_str, run_now)
        return jsonify({
            "ok": True,
            "message": f"SCPRS scheduler enabled: {label_str}",
            "schedules": [s if isinstance(s, str) else s["label"] for s in labels],
            "run_now": run_now,
            "hint": "Default: Monday 7am + Wednesday 10am PST. Set SCPRS_PULL_SCHEDULE to override.",
        })


    # ════════════════════════════════════════════════════════════════════════════════
    # DEAL FORECASTING + WIN PROBABILITY  (PRD Feature 4.4 — P1)
    # ════════════════════════════════════════════════════════════════════════════════
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/quotes/win-probability")
@auth_required
@safe_route
def api_win_probability():
    """Score all open quotes with win probability (0-100).

    Returns per-quote scores + weighted pipeline total.
    Scoring: agency relationship (30%), category match (20%),
             contact engagement (20%), price competitiveness (20%),
             time recency (10%).
    """
    try:
        from src.core.forecasting import score_all_quotes
        result = score_all_quotes()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "scores": [], "weighted_pipeline": 0})


@bp.route("/api/quotes/<qn>/win-probability")
@auth_required
@safe_route
def api_quote_win_probability(qn):
    """Score a single quote."""
    try:
        from src.core.forecasting import score_quote
        from src.forms.quote_generator import get_all_quotes
        from src.core.agent_context import get_context

        quotes = get_all_quotes()
        q = next((x for x in quotes if x.get("quote_number") == qn), None)
        if not q:
            return jsonify({"ok": False, "error": "Quote not found"})
        ctx = get_context(include_contacts=True)
        result = score_quote(q, contacts=ctx.get("contacts", []))
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/agent/context")
@auth_required
@safe_route
def api_agent_context():
    """Return full DB context snapshot for any agent to consume.
    Implements Anthropic Skills Guide Pattern 5: Domain-Specific Intelligence.
    ?prices=query&focus=all|crm|quotes|revenue|intel
    """
    try:
        from src.core.agent_context import get_context, format_context_for_agent
        price_q = request.args.get("prices", "")
        focus = request.args.get("focus", "all")
        ctx = get_context(
            include_prices=bool(price_q),
            price_query=price_q,
            include_contacts=True,
            include_quotes=True,
            include_revenue=True,
        )
        return jsonify({
            "ok": True,
            "context": ctx,
            "formatted": format_context_for_agent(ctx, focus=focus),
            "summary": {
                "contacts": len(ctx.get("contacts", [])),
                "quote_pipeline": ctx.get("quotes", {}).get("pipeline_value", 0),
                "revenue_pct": ctx.get("revenue", {}).get("pct", 0),
                "intel_buyers": ctx.get("intel", {}).get("total_buyers", 0),
            },
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})






# ─── Sales Intelligence Routes ─────────────────────────────────────────────

@bp.route("/api/intel/status")
@auth_required
@safe_route
def api_intel_status():
    """Full intelligence status — buyers, agencies, revenue tracker."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    st = get_intel_status()
    # Quick SCPRS connectivity probe
    scprs_error = None
    try:
        import requests as _req
        r = _req.get("https://suppliers.fiscal.ca.gov/psc/psfpd1/SUPPLIER/ERP/c/ZZ_PO.ZZ_SCPRS1_CMP.GBL",
                     timeout=5, allow_redirects=True)
        if r.status_code >= 400:
            scprs_error = f"HTTP {r.status_code}"
    except Exception as e:
        scprs_error = str(e)[:120]
    st["scprs_reachable"] = scprs_error is None
    st["scprs_error"] = scprs_error
    return jsonify(st)


@bp.route("/api/intel/scprs-test")
@auth_required
@safe_route
def api_intel_scprs_test():
    """Test SCPRS connectivity from Railway and return detailed result."""
    try:
        import requests as _req
        import time as _time
        t0 = _time.time()
        r = _req.get("https://suppliers.fiscal.ca.gov/psc/psfpd1/SUPPLIER/ERP/c/ZZ_PO.ZZ_SCPRS1_CMP.GBL",
                     timeout=10, allow_redirects=True)
        elapsed = round((_time.time() - t0) * 1000)
        return jsonify({
            "ok": r.status_code < 400,
            "status_code": r.status_code,
            "elapsed_ms": elapsed,
            "reachable": True,
            "content_length": len(r.content),
            "is_html": "text/html" in r.headers.get("content-type", ""),
        })
    except Exception as e:
        return jsonify({"ok": False, "reachable": False, "error": str(e),
                        "hint": "Railway static IP must be enabled and whitelisted. Check Railway settings → Networking → Static IP."})


@bp.route("/api/intel/deep-pull")
@auth_required
@safe_route
def api_intel_deep_pull():
    """Deep pull ALL buyers from SCPRS across all product categories. Long-running."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})

    # If already running, return current status
    if DEEP_PULL_STATUS.get("running"):
        return jsonify({"ok": True, "message": "Already running", "status": DEEP_PULL_STATUS})

    from_date = request.args.get("from", "01/01/2019")
    max_q = request.args.get("max_queries")
    max_q = int(max_q) if max_q else None

    def _run():
        deep_pull_all_buyers(from_date=from_date, max_queries=max_q)

    t = threading.Thread(target=_run, daemon=True, name="intel-deep-pull")
    t.start()
    # Give the thread 1.5s to fail fast on SCPRS init, so we can surface the error now
    t.join(timeout=1.5)

    if not DEEP_PULL_STATUS.get("running") and DEEP_PULL_STATUS.get("phase") == "error":
        err = DEEP_PULL_STATUS.get("progress", "SCPRS connection failed")
        return jsonify({
            "ok": False,
            "error": err,
            "hint": "Enable Railway static IP: railway.app → your project → Settings → Networking → Static IP. Then retry.",
            "railway_guide": "https://docs.railway.app/reference/static-outbound-ips",
        })

    return jsonify({"ok": True, "message": f"Deep pull started (from {from_date}). Polling /api/intel/pull-status…"})


@bp.route("/api/intel/pull-status")
@auth_required
@safe_route
def api_intel_pull_status():
    """Check deep pull progress."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    return jsonify({"ok": True, **DEEP_PULL_STATUS})


@bp.route("/api/intel/priority-queue")
@auth_required
@safe_route
def api_intel_priority_queue():
    """Get prioritized outreach queue — highest opportunity buyers first."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    limit = min(int(request.args.get("limit", 25)), 200)
    result = get_priority_queue(limit=limit)
    if not result.get("ok") and "No buyer data" in str(result.get("error", "")):
        return jsonify({"ok": False,
                        "error": "No buyer data yet",
                        "hint": "Run 🔍 Deep Pull All Buyers first to mine SCPRS for buyer contacts, categories, and spend data."})
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════════════
# V2 — Analytics & Intelligence API
# ═══════════════════════════════════════════════════════════════════════


@bp.route("/api/intel/push-prospects")
@auth_required
@safe_route
def api_intel_push_prospects():
    """Push top priority buyers into Growth Agent prospect pipeline."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    top_n = int(request.args.get("top", 50))
    return jsonify(push_to_growth_prospects(top_n=top_n))


@bp.route("/api/intel/revenue")
@auth_required
@safe_route
def api_intel_revenue():
    """Revenue tracker — YTD vs $2M goal."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    return jsonify(update_revenue_tracker())


@bp.route("/api/intel/revenue", methods=["POST"])
@auth_required
@safe_route
def api_intel_add_revenue():
    """Add manual revenue entry. POST JSON: {amount, description, date}"""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    data = request.get_json(silent=True) or {}
    amount = data.get("amount", 0)
    desc = data.get("description", "")
    if not amount or not desc:
        return jsonify({"ok": False, "error": "amount and description required"})
    return jsonify(add_manual_revenue(float(amount), desc, data.get("date", "")))


@bp.route("/api/intel/sb-admin/<agency>")
@auth_required
@safe_route
def api_intel_sb_admin(agency):
    """Find the SB admin/liaison for an agency."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    return jsonify(get_sb_admin(agency))


@bp.route("/api/intel/sb-admin-match")
@auth_required
@safe_route
def api_intel_sb_admin_match():
    """Match SB admin contacts to all agencies in the database."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    return jsonify(find_sb_admin_for_agencies())


@bp.route("/api/intel/buyers/add", methods=["POST"])
@auth_required
@safe_route
def api_intel_buyer_add():
    """Manually add a buyer. POST JSON: {agency, email, name, phone, categories[], annual_spend, notes}"""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    data = request.get_json(silent=True) or {}
    return jsonify(add_manual_buyer(
        agency=_sanitize_input(data.get("agency","")),
        buyer_email=_sanitize_input(data.get("email","")),
        buyer_name=_sanitize_input(data.get("name","") or data.get("buyer_name","")),
        buyer_phone=_sanitize_input(data.get("phone","") or data.get("buyer_phone","")),
        categories=data.get("categories", []),
        annual_spend=float(data.get("annual_spend", 0) or 0),
        notes=_sanitize_input(data.get("notes","")),
    ))


@bp.route("/api/intel/buyers/import-csv", methods=["POST"])
@auth_required
@safe_route
def api_intel_buyers_import_csv():
    """Import buyers from CSV. POST raw CSV text as body, or JSON {csv: '...'}.
    Columns: agency, email, name, phone, categories, annual_spend, notes
    """
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    if request.content_type and "json" in request.content_type:
        data = request.get_json(silent=True) or {}
        csv_text = data.get("csv", "")
    else:
        csv_text = request.get_data(as_text=True)
    if not csv_text.strip():
        return jsonify({"ok": False, "error": "No CSV data provided"})
    return jsonify(import_buyers_csv(csv_text))


@bp.route("/api/intel/seed-demo", methods=["POST"])
@auth_required
@safe_route
def api_intel_seed_demo():
    """Seed the intel DB with realistic CA agency demo data (for testing/demo when SCPRS is unreachable)."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    return jsonify(seed_demo_data())


@bp.route("/api/intel/buyers/delete", methods=["POST"])
@auth_required
@safe_route
def api_intel_buyer_delete():
    """Delete a buyer by id or email. POST JSON: {buyer_id} or {email}"""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    data = request.get_json(silent=True) or {}
    return jsonify(delete_buyer(
        buyer_id=data.get("buyer_id"),
        buyer_email=data.get("email"),
    ))


@bp.route("/api/intel/buyers/clear", methods=["POST"])
@auth_required
@safe_route
def api_intel_buyers_clear():
    """Clear all buyer data (start fresh). Requires confirm=true in body."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    data = request.get_json(silent=True) or {}
    if not data.get("confirm"):
        return jsonify({"ok": False, "error": "Send {confirm: true} to clear all buyer data"})
    import os as _os
    for f in [INTEL_BUYERS_FILE, INTEL_AGENCIES_FILE]:
        if _os.path.exists(f):
            _os.remove(f)
            _invalidate_cache(f)
    return jsonify({"ok": True, "message": "Buyer database cleared. Run Deep Pull or seed demo data."})




@bp.route("/api/test/cleanup-duplicates")
@auth_required
@safe_route
def api_cleanup_duplicates():
    """ONE-TIME: Deduplicate quotes_log.json and reset counter.

    What it does:
      1. Backs up quotes_log.json → quotes_log_backup_{timestamp}.json
      2. Deduplicates: keeps only the LAST entry per quote number
      3. Resets quote_counter.json to highest quote number + 1
      4. Returns full before/after report

    Safe to run multiple times — idempotent after first run.
    Hit: /api/test/cleanup-duplicates?dry_run=true to preview without writing.
    """
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "quote_generator not available"})

    dry_run = request.args.get("dry_run", "false").lower() == "true"
    quotes = get_all_quotes()
    original_count = len(quotes)

    # Deduplicate: walk forward, keep last occurrence of each quote number
    seen = {}
    for i, q in enumerate(quotes):
        qn = q.get("quote_number", "")
        if qn:
            seen[qn] = i  # last index wins

    # Build clean list preserving order of last occurrence
    clean = []
    used_indices = set(seen.values())
    for i in sorted(used_indices):
        clean.append(quotes[i])

    removed = original_count - len(clean)

    # Find highest quote number for counter reset
    max_num = 0
    for q in clean:
        qn = q.get("quote_number", "")
        try:
            n = int(qn.split("Q")[-1])
            max_num = max(max_num, n)
        except (ValueError, IndexError) as _e:
            log.debug("suppressed: %s", _e)

    # Build report
    from collections import Counter
    old_counts = Counter(q.get("quote_number", "") for q in quotes)
    dupes = {k: v for k, v in old_counts.items() if v > 1}

    report = {
        "dry_run": dry_run,
        "before": {"total_entries": original_count, "unique_quotes": len(seen)},
        "after": {"total_entries": len(clean), "unique_quotes": len(seen)},
        "removed": removed,
        "duplicates_found": dupes,
        "counter_will_be": max_num,
        "next_quote": f"R{str(datetime.now().year)[2:]}Q{max_num + 1}",
        "clean_quotes": [
            {"quote_number": q.get("quote_number"), "total": q.get("total", 0),
             "institution": q.get("institution", "")[:40], "status": q.get("status", "")}
            for q in clean
        ],
    }

    if not dry_run:
        # Backup
        backup_name = f"quotes_log_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        backup_path = os.path.join(DATA_DIR, backup_name)
        import shutil
        src_path = os.path.join(DATA_DIR, "quotes_log.json")
        if os.path.exists(src_path):
            shutil.copy2(src_path, backup_path)
            report["backup"] = backup_name

        # Write clean data
        from src.forms.quote_generator import _save_all_quotes, _detect_agency

        # Fix DEFAULT agencies using all available data
        agencies_fixed = 0
        for q in clean:
            if q.get("agency", "DEFAULT") == "DEFAULT":
                detected = _detect_agency(q)
                if detected != "DEFAULT":
                    q["agency"] = detected
                    agencies_fixed += 1
        report["agencies_fixed"] = agencies_fixed

        _save_all_quotes(clean)

        # Reset counter
        set_quote_counter(max_num)

        log.info("CLEANUP: %d → %d quotes (%d duplicates removed, %d agencies fixed). Counter → %d. Backup: %s",
                 original_count, len(clean), removed, agencies_fixed, max_num, backup_name)
        report["message"] = f"Done. {removed} duplicates removed, {agencies_fixed} agencies fixed. Counter reset to {max_num}. Backup: {backup_name}"
    else:
        report["message"] = f"DRY RUN: Would remove {removed} duplicates and reset counter to {max_num}. Add ?dry_run=false to execute."

    return jsonify(report)


@bp.route("/api/data/sync-clean")
@auth_required
@safe_route
def api_data_sync_clean():
    """Deep clean production data — remove test/orphaned records, keep all real data.
    
    Keeps: all non-test quotes, real PCs, real leads, customers, vendors, caches.
    Removes: test data, batch-generated leads, stale logs.
    
    ?dry_run=true to preview. Default is dry_run.
    ?confirm=yes to actually execute.
    """
    dry_run = request.args.get("confirm", "no").lower() != "yes"
    report = {"dry_run": dry_run, "actions": []}

    # 1. Clean quotes — keep real ones, remove test
    # ?keep=R26Q16,R26Q17 to explicitly specify which to keep
    try:
        qpath = os.path.join(DATA_DIR, "quotes_log.json")
        with open(qpath) as f:
            quotes = json.load(f)
        keep_list = request.args.get("keep", "").split(",") if request.args.get("keep") else None
        if keep_list:
            # Explicit keep list provided
            keep_list = [k.strip() for k in keep_list if k.strip()]
            keep = [q for q in quotes if q.get("quote_number") in keep_list]
        else:
            # Auto: remove is_test or TEST- prefix
            keep = [q for q in quotes if not q.get("is_test")
                    and not str(q.get("quote_number", "")).startswith("TEST-")]
        removed_q = len(quotes) - len(keep)
        report["quotes"] = {"before": len(quotes), "after": len(keep), "removed": removed_q,
                            "kept": [q.get("quote_number") for q in keep]}
        if removed_q > 0:
            report["actions"].append(f"Remove {removed_q} quotes (keep {[q.get('quote_number') for q in keep]})")
        if not dry_run and removed_q > 0:
            with open(qpath, "w") as f:
                json.dump(keep, f, indent=2, default=str)
    except Exception as e:
        report["quotes_error"] = str(e)

    # 2. Clean price checks — remove any with is_test or no real data
    try:
        pcpath = os.path.join(DATA_DIR, "price_checks.json")
        if os.path.exists(pcpath):
            with open(pcpath) as f:
                pcs = json.load(f)
            if isinstance(pcs, dict):
                clean_pcs = {k: v for k, v in pcs.items()
                             if not v.get("is_test") and v.get("institution")}
                removed_pc = len(pcs) - len(clean_pcs)
                report["price_checks"] = {"before": len(pcs), "after": len(clean_pcs), "removed": removed_pc}
                if removed_pc > 0:
                    report["actions"].append(f"Remove {removed_pc} stale/test PCs")
                if not dry_run and removed_pc > 0:
                    with open(pcpath, "w") as f:
                        json.dump(clean_pcs, f, indent=2, default=str)
    except Exception as e:
        report["pc_error"] = str(e)

    # 3. Clean leads — remove test leads + batch-generated
    try:
        lpath = os.path.join(DATA_DIR, "leads.json")
        try:
            from src.core.dal import get_all_leads
            leads = get_all_leads()
        except Exception:
            leads = []
        if leads:
            clean_leads = [l for l in leads
                           if not l.get("is_test")
                           and l.get("match_type") != "test"
                           and not str(l.get("po_number", "")).startswith("PO-ADD-")]
            removed_l = len(leads) - len(clean_leads)
            report["leads"] = {"before": len(leads), "after": len(clean_leads), "removed": removed_l}
            if removed_l > 0:
                report["actions"].append(f"Remove {removed_l} test/batch leads")
            if not dry_run and removed_l > 0:
                with open(lpath, "w") as f:
                    json.dump(clean_leads, f, indent=2, default=str)
    except Exception as e:
        report["leads_error"] = str(e)

    # 4. Clear stale outbox, CRM, email logs
    stale_files = ["email_outbox.json", "crm_activity.json", "email_sent_log.json",
                   "lead_history.json", "workflow_runs.json", "scan_log.json"]
    for fname in stale_files:
        fpath = os.path.join(DATA_DIR, fname)
        if os.path.exists(fpath):
            try:
                with open(fpath) as f:
                    data = json.load(f)
                count = len(data) if isinstance(data, (list, dict)) else 0
                if count > 0:
                    report["actions"].append(f"Clear {fname} ({count} entries)")
                    if not dry_run:
                        empty = [] if isinstance(data, list) else {}
                        with open(fpath, "w") as f:
                            json.dump(empty, f, indent=2)
            except Exception as _e:
                log.debug("Suppressed: %s", _e)

    # 5. Ensure quote counter matches highest quote number
    # ?counter=16 to force a specific value
    try:
        cpath = os.path.join(DATA_DIR, "quote_counter.json")
        qpath2 = os.path.join(DATA_DIR, "quotes_log.json")
        if os.path.exists(cpath):
            with open(cpath) as f:
                counter = json.load(f)
            force_counter = request.args.get("counter", type=int)
            if force_counter:
                target = force_counter
            elif os.path.exists(qpath2):
                with open(qpath2) as f:
                    all_q = json.load(f)
                max_num = 0
                for q in all_q:
                    qn = q.get("quote_number", "")
                    import re
                    m = re.search(r'(\d+)$', qn)
                    if m:
                        max_num = max(max_num, int(m.group(1)))
                target = max_num
            else:
                target = 0
            current = counter.get("counter", 0)
            if current != target and target > 0:
                report["actions"].append(f"Sync quote counter: {current} → {target}")
                if not dry_run:
                    counter["counter"] = target
                    with open(cpath, "w") as f:
                        json.dump(counter, f, indent=2)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # 6. Clear orders (SQLite)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            cnt = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            if cnt > 0:
                report["actions"].append(f"Clear {cnt} orders from SQLite")
                if not dry_run:
                    conn.execute("DELETE FROM orders")
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    if not report["actions"]:
        report["message"] = "Data is already clean — nothing to do"
    elif dry_run:
        report["message"] = f"DRY RUN: {len(report['actions'])} actions needed. Hit /api/data/sync-clean?confirm=yes to execute."
    else:
        report["message"] = f"DONE: {len(report['actions'])} cleanup actions executed"
        log.info("DATA SYNC: %d actions executed", len(report["actions"]))

    return jsonify({"ok": True, **report})

@bp.route("/api/test/renumber-quote")
@auth_required
@safe_route
def api_renumber_quote():
    """Renumber a quote. Usage: ?old=R26Q1&new=R26Q16
    
    Also updates any PC that references the old quote number.
    """
    old = request.args.get("old", "").strip()
    new = request.args.get("new", "").strip()
    dry_run = request.args.get("dry_run", "false").lower() == "true"

    if not old or not new:
        return jsonify({"ok": False, "error": "Provide ?old=R26Q1&new=R26Q16"})

    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "quote_generator not available"})

    quotes = get_all_quotes()
    found = False
    for q in quotes:
        if q.get("quote_number") == old:
            if not dry_run:
                q["quote_number"] = new
                q["renumbered_from"] = old
                q["renumbered_at"] = datetime.now().isoformat()
            found = True
            break

    if not found:
        return jsonify({"ok": False, "error": f"Quote {old} not found"})

    # Update linked PCs
    pc_updated = ""
    pcs = _load_price_checks()
    for pid, pc in pcs.items():
        if pc.get("reytech_quote_number") == old:
            if not dry_run:
                pc["reytech_quote_number"] = new
            pc_updated = pid

    # Update counter if new number is higher
    try:
        new_num = int(new.split("Q")[-1])
    except (ValueError, IndexError):
        new_num = 0

    if not dry_run:
        from src.forms.quote_generator import _save_all_quotes
        _save_all_quotes(quotes)
        if pc_updated:
            _save_price_checks(pcs)
        if new_num > 0:
            set_quote_counter(new_num)
        log.info("RENUMBER: %s → %s (PC: %s, counter: %d)", old, new, pc_updated or "none", new_num)

    return jsonify({
        "ok": True,
        "dry_run": dry_run,
        "old": old,
        "new": new,
        "pc_updated": pc_updated or None,
        "counter_set_to": new_num,
        "next_quote": f"R{str(datetime.now().year)[2:]}Q{new_num + 1}",
        "message": f"{'DRY RUN: Would renumber' if dry_run else 'Renumbered'} {old} → {new}",
    })


@bp.route("/api/test/delete-quotes")
@auth_required
@safe_route
def api_delete_quotes():
    """Delete specific quotes by number. Usage: ?numbers=R26Q2,R26Q3,R26Q4

    Backs up before deleting. Also cleans linked PCs.
    """
    numbers_str = request.args.get("numbers", "").strip()
    dry_run = request.args.get("dry_run", "false").lower() == "true"

    if not numbers_str:
        return jsonify({"ok": False, "error": "Provide ?numbers=R26Q2,R26Q3,R26Q4"})

    to_delete = set(n.strip() for n in numbers_str.split(",") if n.strip())

    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "quote_generator not available"})

    quotes = get_all_quotes()
    original_count = len(quotes)
    deleted = []
    kept = []

    for q in quotes:
        qn = q.get("quote_number", "")
        if qn in to_delete:
            deleted.append({"quote_number": qn, "total": q.get("total", 0),
                           "institution": q.get("institution", "")})
        else:
            kept.append(q)

    # Clean linked PCs
    pcs_cleaned = []
    pcs = _load_price_checks()
    for pid, pc in pcs.items():
        if pc.get("reytech_quote_number") in to_delete:
            if not dry_run:
                pc["reytech_quote_number"] = ""
                pc["reytech_quote_pdf"] = ""
                _transition_status(pc, "parsed", actor="cleanup", notes=f"Quote deleted")
            pcs_cleaned.append(pid)

    if not dry_run and deleted:
        # Backup
        backup_name = f"quotes_log_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        import shutil
        src_path = os.path.join(DATA_DIR, "quotes_log.json")
        if os.path.exists(src_path):
            shutil.copy2(src_path, os.path.join(DATA_DIR, backup_name))
        from src.forms.quote_generator import _save_all_quotes
        _save_all_quotes(kept)
        if pcs_cleaned:
            _save_price_checks(pcs)
        log.info("DELETE QUOTES: %s removed (%d → %d). PCs cleaned: %s",
                 [d["quote_number"] for d in deleted], original_count, len(kept), pcs_cleaned)

    return jsonify({
        "ok": True,
        "dry_run": dry_run,
        "deleted": deleted,
        "remaining": len(kept),
        "pcs_cleaned": pcs_cleaned,
        "message": f"{'DRY RUN: Would delete' if dry_run else 'Deleted'} {len(deleted)} quotes: {[d['quote_number'] for d in deleted]}",
    })


# ══ Consolidated from routes_features*.py ══════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════
# Competitor Price Intel — what are competitors charging?
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/competitor/price-intel")
@auth_required
@safe_route
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
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    return jsonify({"ok": True, **intel})


# ═══════════════════════════════════════════════════════════════════════
# Agency Leaderboard — which agencies bring most revenue
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/agency/leaderboard")
@auth_required
@safe_route
def api_agency_leaderboard():
    """Rank agencies by revenue, order count, and growth."""
    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    agencies = defaultdict(lambda: {"quotes": 0, "orders": 0, "revenue": 0, "rfqs": 0})

    try:
        with open(rfqs_path) as f:
            rfqs = json.load(f)
        for r in rfqs.values():
            agency = r.get("institution") or r.get("agency") or "Unknown"
            agencies[agency]["rfqs"] += 1
            if (r.get("status") or "").lower() in ("sent", "quoted"):
                agencies[agency]["quotes"] += 1
    except Exception: pass

    try:
        orders = _load_orders()
        for o in orders.values():
            agency = o.get("institution") or o.get("agency") or "Unknown"
            agencies[agency]["orders"] += 1
            agencies[agency]["revenue"] += o.get("total", 0)
    except Exception: pass

    result = [{"agency": k, **v} for k, v in agencies.items()]
    result.sort(key=lambda x: x["revenue"], reverse=True)

    return jsonify({"ok": True, "agencies": result[:20], "total": len(result)})


# ═══════════════════════════════════════════════════════════════════════
# Product Quick Search from Agents page
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/product/search")
@auth_required
@safe_route
def api_product_search():
    """Quick product search in catalog."""
    q = (request.args.get("q") or "").strip().lower()
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
    except Exception: pass

    results.sort(key=lambda x: x.get("times_quoted", 0), reverse=True)

    return jsonify({"ok": True, "query": q, "results": results[:20], "count": len(results)})


# ── Intel routes from routes_features2.py ────────────────────────────────────

@bp.route("/api/intel/agency-penetration")
@auth_required
@safe_route
def api_intel_agency_penetration():
    """How deep we've penetrated each agency — facilities, contacts, quotes."""
    try:
        import sqlite3
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
        quote_counts = {}
        from src.core.db import DB_PATH as _DB_PATH
        conn = sqlite3.connect(_DB_PATH, timeout=10)
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


@bp.route("/api/intel/competitive-pricing")
@auth_required
@safe_route
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


@bp.route("/api/intel/revenue-by-agency")
@auth_required
@safe_route
def api_intel_revenue_by_agency():
    """Revenue breakdown by agency from quotes and QB data."""
    try:
        import sqlite3
        from src.core.db import DB_PATH as _DB_PATH
        conn = sqlite3.connect(_DB_PATH, timeout=10)
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


# ══════════════════════════════════════════════════════════════════════════════
# AWARD INTELLIGENCE & COMPETITIVE PRICING ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/intel/loss-analysis")
@auth_required
@safe_route
def api_loss_analysis():
    """Full loss analysis dashboard data — recent losses, patterns, margin insights."""
    try:
        days = int(request.args.get("days", 90))

        with get_db() as conn:
            conn.row_factory = __import__("sqlite3").Row

            # Recent losses with classification
            recent_losses = [dict(r) for r in conn.execute("""
                SELECT found_at, quote_number, our_price, competitor_name,
                       competitor_price, price_delta, price_delta_pct,
                       po_number, agency, institution, loss_reason_class,
                       margin_too_high, our_cost, our_margin_pct, item_summary
                FROM competitor_intel
                WHERE outcome='lost'
                ORDER BY found_at DESC LIMIT 25
            """).fetchall()]

            # Loss classification breakdown
            cutoff = (__import__("datetime").datetime.now()
                      - __import__("datetime").timedelta(days=days)).isoformat()
            class_breakdown = [dict(r) for r in conn.execute("""
                SELECT loss_reason_class,
                       COUNT(*) as count,
                       AVG(price_delta_pct) as avg_delta_pct,
                       SUM(our_price) as total_lost_value
                FROM competitor_intel
                WHERE outcome='lost' AND found_at >= ?
                GROUP BY loss_reason_class
                ORDER BY count DESC
            """, (cutoff,)).fetchall()]

            # Margin too high items
            mth_losses = [dict(r) for r in conn.execute("""
                SELECT found_at, quote_number, competitor_name,
                       our_price, competitor_price, price_delta_pct,
                       our_cost, our_margin_pct, agency, item_summary
                FROM competitor_intel
                WHERE outcome='lost' AND margin_too_high = 1
                ORDER BY found_at DESC LIMIT 15
            """).fetchall()]

            # Overall stats
            stats = conn.execute("""
                SELECT COUNT(*) as total_losses,
                       AVG(price_delta_pct) as avg_delta_pct,
                       COUNT(DISTINCT competitor_name) as unique_competitors,
                       SUM(CASE WHEN margin_too_high = 1 THEN 1 ELSE 0 END) as margin_too_high_count,
                       SUM(CASE WHEN loss_reason_class = 'relationship_incumbent' THEN 1 ELSE 0 END) as relationship_losses
                FROM competitor_intel WHERE outcome='lost' AND found_at >= ?
            """, (cutoff,)).fetchone()
            stats = dict(stats) if stats else {}

        return jsonify({
            "ok": True,
            "recent_losses": recent_losses,
            "class_breakdown": class_breakdown,
            "margin_too_high_losses": mth_losses,
            "stats": stats,
            "days": days,
        })
    except Exception as e:
        log.exception("loss-analysis")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/loss-patterns")
@auth_required
@safe_route
def api_loss_patterns():
    """Detected competitive patterns and recommendations."""
    try:
        from src.agents.pricing_feedback import (
            detect_margin_patterns, get_unacknowledged_patterns
        )
        days = int(request.args.get("days", 90))
        patterns = detect_margin_patterns(days)
        unacked = get_unacknowledged_patterns(limit=20)
        return jsonify({
            "ok": True,
            "patterns": patterns,
            "unacknowledged": unacked,
            "days": days,
        })
    except Exception as e:
        log.exception("loss-patterns")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/loss-patterns/acknowledge", methods=["POST"])
@auth_required
@safe_route
def api_acknowledge_pattern():
    """Mark a loss pattern as reviewed."""
    try:
        from src.agents.pricing_feedback import acknowledge_pattern
        pattern_id = request.json.get("pattern_id")
        if not pattern_id:
            return jsonify({"ok": False, "error": "pattern_id required"}), 400
        result = acknowledge_pattern(int(pattern_id))
        return jsonify({"ok": result})
    except Exception as e:
        log.exception("acknowledge-pattern")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/pricing-recommendation", methods=["POST"])
@auth_required
@safe_route
def api_pricing_recommendation():
    """Get pricing recommendation for items about to be quoted."""
    try:
        from src.agents.pricing_feedback import get_pricing_recommendation
        data = request.json or {}
        description = data.get("description", "")
        agency = data.get("agency", "")
        cost = float(data.get("cost", 0) or 0)
        quantity = int(data.get("quantity", 1) or 1)

        if not description:
            return jsonify({"ok": False, "error": "description required"}), 400

        rec = get_pricing_recommendation(description, agency, cost, quantity)
        return jsonify({"ok": True, **rec})
    except Exception as e:
        log.exception("pricing-recommendation")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/margin-analysis")
@auth_required
@safe_route
def api_margin_analysis():
    """Margin trends — where are we too high, where too thin."""
    try:
        from src.agents.pricing_feedback import (
            get_category_loss_trends, detect_margin_patterns
        )
        days = int(request.args.get("days", 90))
        category_trends = get_category_loss_trends(days)
        patterns = detect_margin_patterns(days)
        margin_patterns = [p for p in patterns if p.get("pattern_type") in
                           ("margin_trend", "loss_class_trend")]
        return jsonify({
            "ok": True,
            "category_trends": category_trends,
            "margin_patterns": margin_patterns,
            "days": days,
        })
    except Exception as e:
        log.exception("margin-analysis")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/competitor/<name>")
@auth_required
@safe_route
def api_competitor_detail(name):
    """Deep dive on a specific competitor's pricing patterns."""
    try:
        from src.agents.pricing_feedback import get_competitor_price_trends
        days = int(request.args.get("days", 180))
        trends = get_competitor_price_trends(competitor_name=name, days=days)
        competitor_data = trends.get(name, {})

        # Also get recent losses to this competitor
        with get_db() as conn:
            conn.row_factory = __import__("sqlite3").Row
            recent = [dict(r) for r in conn.execute("""
                SELECT found_at, quote_number, our_price, competitor_price,
                       price_delta_pct, po_number, agency, institution,
                       loss_reason_class, margin_too_high, item_summary
                FROM competitor_intel
                WHERE competitor_name = ? AND outcome='lost'
                ORDER BY found_at DESC LIMIT 15
            """, (name,)).fetchall()]

        return jsonify({
            "ok": True,
            "competitor": name,
            "trends": competitor_data,
            "recent_losses": recent,
            "days": days,
        })
    except Exception as e:
        log.exception("competitor-detail")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/award-tracker/status")
@auth_required
@safe_route
def api_award_tracker_status():
    """Award tracker status with schedule information."""
    try:
        from src.agents.award_tracker import get_status
        status = get_status()

        # Add schedule config
        try:
            from src.core.scprs_schedule import (
                SCPRS_UPDATE_TIMES_PT, DAILY_CHECK_PHASE_DAYS, EXPIRY_DAYS,
                seconds_until_next_window, is_scprs_check_time,
                current_scprs_window,
            )
            status["schedule"] = {
                "scprs_update_times": [t.strftime("%H:%M") for t in SCPRS_UPDATE_TIMES_PT],
                "daily_phase_days": DAILY_CHECK_PHASE_DAYS,
                "expiry_days": EXPIRY_DAYS,
                "in_check_window": is_scprs_check_time(),
                "current_window": current_scprs_window(),
                "seconds_until_next": seconds_until_next_window(),
            }
        except ImportError:
            status["schedule"] = {"error": "scprs_schedule module not available"}

        return jsonify({"ok": True, **status})
    except Exception as e:
        log.exception("award-tracker-status")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/award-tracker/run", methods=["POST"])
@auth_required
@safe_route
def api_award_tracker_run():
    """Manually trigger an award check cycle."""
    try:
        from src.agents.award_tracker import run_award_check
        force = request.json.get("force", False) if request.json else False
        result = run_award_check(force=force)
        return jsonify(result)
    except Exception as e:
        log.exception("award-tracker-run")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/oracle/backfill-all", methods=["POST"])
@auth_required
@safe_route
def api_oracle_backfill_all():
    """Feed all historical won/lost quotes + award-tracker losses through
    oracle calibration. Idempotent — safe to re-run.

    Body: {"dry_run": true} to preview without writing.
    """
    try:
        from src.core.oracle_backfill import backfill_all
        dry_run = (request.json or {}).get("dry_run", False)
        result = backfill_all(dry_run=dry_run)
        return jsonify(result)
    except Exception as e:
        log.exception("oracle-backfill-all")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/win-rate-trends")
@auth_required
@safe_route
def api_win_rate_trends():
    """Win rate trends over time — by period, agency, competitor."""
    try:
        from src.agents.pricing_feedback import get_win_rate_trends
        days = int(request.args.get("days", 180))
        result = get_win_rate_trends(days=days)
        return jsonify({"ok": True, **result})
    except Exception as e:
        log.exception("win-rate-trends")
        return jsonify({"ok": False, "error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# SUPPLIER AUTH STATUS & TESTING
# ══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/intel/supplier-auth/status")
@auth_required
@safe_route
def api_supplier_auth_status():
    """Get authentication status for all login-required suppliers."""
    try:
        from src.agents.supplier_auth_scraper import get_supplier_auth_status
        return jsonify({"ok": True, "suppliers": get_supplier_auth_status()})
    except Exception as e:
        log.exception("supplier-auth-status")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/supplier-auth/test", methods=["POST"])
@auth_required
@safe_route
def api_supplier_auth_test():
    """Test login for a specific supplier."""
    try:
        from src.agents.supplier_auth_scraper import test_supplier_login
        supplier_key = (request.json or {}).get("supplier", "")
        if not supplier_key:
            return jsonify({"ok": False, "error": "supplier key required"}), 400
        result = test_supplier_login(supplier_key)
        return jsonify(result)
    except Exception as e:
        log.exception("supplier-auth-test")
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Award Monitor Dashboard ──────────────────────────────────────────────────

@bp.route("/award-monitor")
@auth_required
@safe_page
def award_monitor_page():
    """Live SCPRS award monitoring dashboard."""
    return render_page("award_monitor.html", active_page="Awards")


@bp.route("/api/intel/award-tracker/queue")
@auth_required
@safe_route
def api_award_tracker_queue():
    """Get monitoring queue for all sent quotes."""
    try:
        from src.agents.award_tracker import get_monitoring_queue, get_status
        queue = get_monitoring_queue()
        status = get_status()
        return jsonify({"ok": True, "queue": queue, "status": status})
    except Exception as e:
        log.exception("award-tracker-queue")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/intel/action-items")
@auth_required
@safe_route
def api_action_items():
    """Get pending action items from loss analysis."""
    try:
        import sqlite3
        with get_db() as conn:
            conn.row_factory = sqlite3.Row
            items = conn.execute("""
                SELECT * FROM action_items WHERE status='pending'
                ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                         created_at DESC LIMIT 50
            """).fetchall()
            return jsonify({"ok": True, "items": [dict(r) for r in items]})
    except Exception as e:
        log.debug("action_items query: %s", e)
        return jsonify({"ok": True, "items": [], "note": str(e)})


@bp.route("/api/intel/action-items/<int:item_id>/complete", methods=["POST"])
@auth_required
@safe_route
def api_action_item_complete(item_id):
    """Mark an action item as completed."""
    try:
        with get_db() as conn:
            conn.execute("UPDATE action_items SET status='done', completed_at=datetime('now') WHERE id=?", (item_id,))
        return jsonify({"ok": True})
    except Exception as e:
        log.error("action-item-complete %d: %s", item_id, e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/action-items/<int:item_id>/dismiss", methods=["POST"])
@auth_required
@safe_route
def api_action_item_dismiss(item_id):
    """Dismiss an action item."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        with get_db() as conn:
            conn.execute("UPDATE action_items SET status='dismissed', notes=?, completed_at=datetime('now') WHERE id=?",
                        (data.get("reason", ""), item_id))
        return jsonify({"ok": True})
    except Exception as e:
        log.error("action-item-dismiss %d: %s", item_id, e)
        return jsonify({"ok": False, "error": str(e)})
