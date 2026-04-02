# PRD-28 Routes — Quote Lifecycle, Email Overhaul, Lead Nurture, Revenue, Vendor Intel
# Loaded by dashboard.py via load_module()

# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from flask import redirect

from datetime import datetime as _dt, timezone as _tz

# ══════════════════════════════════════════════════════════════════════════════
# Work Item 1: Quote Lifecycle APIs
# ══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/quote-lifecycle/status")
@auth_required
def api_quote_lifecycle_status():
    from src.agents.quote_lifecycle import get_agent_status
    return jsonify(get_agent_status())

@bp.route("/api/quote-lifecycle/pipeline")
@auth_required
def api_quote_pipeline():
    from src.agents.quote_lifecycle import get_pipeline_summary
    return jsonify(get_pipeline_summary())

@bp.route("/api/quote-lifecycle/expiring")
@auth_required
def api_quote_expiring():
    days = request.args.get("days", 7, type=int)
    from src.agents.quote_lifecycle import get_expiring_soon
    return jsonify({"ok": True, "quotes": get_expiring_soon(days)})

@bp.route("/api/quote-lifecycle/check-expirations", methods=["POST"])
@auth_required
def api_check_expirations():
    from src.agents.quote_lifecycle import check_expirations
    return jsonify(check_expirations())

@bp.route("/api/quote-lifecycle/process-reply", methods=["POST"])
@auth_required
def api_process_reply_signal():
    data = request.get_json(force=True, silent=True) or {}
    from src.agents.quote_lifecycle import process_reply_signal
    return jsonify(process_reply_signal(
        quote_number=data.get("quote_number", ""),
        signal=data.get("signal", ""),
        confidence=data.get("confidence", 0.5),
        po_number=data.get("po_number", ""),
        reason=data.get("reason", ""),
    ))

@bp.route("/api/quote-lifecycle/revisions/<qn>")
@auth_required
def api_quote_revisions(qn):
    from src.agents.quote_lifecycle import get_revisions
    return jsonify({"ok": True, "revisions": get_revisions(qn)})

@bp.route("/api/quote-lifecycle/save-revision", methods=["POST"])
@auth_required
def api_save_revision():
    try:
        data = request.get_json(force=True, silent=True) or {}
        from src.agents.quote_lifecycle import save_revision
        return jsonify(save_revision(data.get("quote_number", ""), data.get("reason", "manual edit")))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/quote-lifecycle/close-competitor", methods=["POST"])
@auth_required
def api_close_competitor():
    try:
        data = request.get_json(force=True, silent=True) or {}
        from src.agents.quote_lifecycle import close_lost_to_competitor
        return jsonify(close_lost_to_competitor(
            data.get("quote_number", ""),
            data.get("competitor", ""),
            data.get("competitor_price", 0),
            data.get("po_number", ""),
        ))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ══════════════════════════════════════════════════════════════════════════════
# Work Item 2: Email Outbox Overhaul APIs
# ══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/outbox/bulk-approve", methods=["POST"])
@auth_required
def api_outbox_bulk_approve():
    try:
        data = request.get_json(force=True, silent=True) or {}
        from src.agents.email_lifecycle import bulk_approve
        return jsonify(bulk_approve(data.get("email_ids")))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/outbox/bulk-delete", methods=["POST"])
@auth_required
def api_outbox_bulk_delete():
    try:
        data = request.get_json(force=True, silent=True) or {}
        from src.agents.email_lifecycle import bulk_delete
        return jsonify(bulk_delete(data.get("email_ids"), data.get("status_filter", "draft")))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/outbox/retry-failed", methods=["POST"])
@auth_required
def api_outbox_retry_failed():
    try:
        from src.agents.email_lifecycle import retry_failed_emails
        return jsonify(retry_failed_emails())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/outbox/engagement-stats")
@auth_required
def api_outbox_engagement():
    from src.agents.email_lifecycle import get_engagement_stats
    return jsonify(get_engagement_stats())

@bp.route("/api/outbox/summary")
@auth_required
def api_outbox_summary():
    from src.agents.email_lifecycle import get_outbox_summary
    return jsonify(get_outbox_summary())

# Email tracking pixel (no auth — hit from email client)
@bp.route("/api/email/track/<tracking_id>/open")
def api_email_track_open(tracking_id):
    from src.agents.email_lifecycle import record_engagement
    record_engagement(
        tracking_id=tracking_id,
        event_type="open",
        ip_address=request.remote_addr or "",
        user_agent=request.headers.get("User-Agent", ""),
    )
    # Return 1x1 transparent GIF
    import base64 as _b64
    gif = _b64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")
    return Response(gif, mimetype="image/gif", headers={"Cache-Control": "no-store"})

@bp.route("/api/email/track/<tracking_id>/click")
def api_email_track_click(tracking_id):
    url = request.args.get("url", "/")
    from src.agents.email_lifecycle import record_engagement
    record_engagement(
        tracking_id=tracking_id,
        event_type="click",
        ip_address=request.remote_addr or "",
        user_agent=request.headers.get("User-Agent", ""),
        link_url=url,
    )
    # Prevent open redirect — only allow same-origin or whitelisted domains
    from urllib.parse import urlparse as _urlparse
    _parsed = _urlparse(url)
    if _parsed.scheme and _parsed.netloc:
        _allowed = ("reytechinc.com", "railway.app", "amazon.com", "grainger.com")
        if not any(_parsed.netloc.endswith(d) for d in _allowed):
            url = "/"
    return redirect(url)


# ══════════════════════════════════════════════════════════════════════════════
# Work Item 3: Lead Nurture APIs
# ══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/leads/nurture/start", methods=["POST"])
@auth_required
def api_start_nurture():
    try:
        data = request.get_json(force=True, silent=True) or {}
        from src.agents.lead_nurture_agent import start_nurture
        return jsonify(start_nurture(data.get("lead_id", ""), data.get("sequence", "")))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/leads/nurture/pause", methods=["POST"])
@auth_required
def api_pause_nurture():
    try:
        data = request.get_json(force=True, silent=True) or {}
        from src.agents.lead_nurture_agent import pause_nurture
        return jsonify(pause_nurture(data.get("lead_id", ""), data.get("reason", "")))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/leads/nurture/process", methods=["POST"])
@auth_required
def api_process_nurture():
    try:
        from src.agents.lead_nurture_agent import process_nurture_queue
        return jsonify(process_nurture_queue())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/leads/nurture/auto-start", methods=["POST"])
@auth_required
def api_auto_start_nurture():
    try:
        from src.agents.lead_nurture_agent import auto_start_nurture_new_leads
        return jsonify(auto_start_nurture_new_leads())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/leads/rescore", methods=["POST"])
@auth_required
def api_rescore_leads():
    try:
        from src.agents.lead_nurture_agent import rescore_all_leads
        return jsonify(rescore_all_leads())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/leads/convert", methods=["POST"])
@auth_required
def api_convert_lead():
    try:
        data = request.get_json(force=True, silent=True) or {}
        from src.agents.lead_nurture_agent import convert_lead_to_customer
        return jsonify(convert_lead_to_customer(data.get("lead_id", "")))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/leads/pipeline")
@auth_required
def api_leads_pipeline():
    from src.agents.lead_nurture_agent import get_unified_pipeline
    return jsonify(get_unified_pipeline())

@bp.route("/api/leads/nurture/status")
@auth_required
def api_nurture_status():
    from src.agents.lead_nurture_agent import get_agent_status
    return jsonify(get_agent_status())


# ══════════════════════════════════════════════════════════════════════════════
# Work Item 4: Revenue Dashboard
# ══════════════════════════════════════════════════════════════════════════════

@bp.route("/revenue")
@auth_required
def revenue_page():
    from src.api.render import render_page
    return render_page("revenue.html", active_page="Revenue")

@bp.route("/api/revenue/dashboard")
@auth_required
def api_revenue_dashboard():
    from src.agents.revenue_engine import get_revenue_dashboard
    return jsonify(get_revenue_dashboard())

@bp.route("/api/revenue/goal")
@auth_required
def api_revenue_goal():
    from src.agents.revenue_engine import get_goal_progress
    return jsonify(get_goal_progress())

@bp.route("/api/revenue/monthly")
@auth_required
def api_revenue_monthly():
    months = request.args.get("months", 12, type=int)
    from src.agents.revenue_engine import get_monthly_revenue
    return jsonify(get_monthly_revenue(months))

@bp.route("/api/revenue/pipeline-forecast")
@auth_required
def api_revenue_pipeline():
    from src.agents.revenue_engine import forecast_pipeline
    return jsonify(forecast_pipeline())

@bp.route("/api/revenue/margins")
@auth_required
def api_revenue_margins():
    from src.agents.revenue_engine import get_margin_analysis
    return jsonify(get_margin_analysis())

@bp.route("/api/revenue/top-customers")
@auth_required
def api_revenue_top_customers():
    from src.agents.revenue_engine import get_top_customers
    return jsonify({"ok": True, "customers": get_top_customers(10)})

@bp.route("/api/revenue/reconcile", methods=["POST"])
@auth_required
def api_revenue_reconcile():
    try:
        from src.agents.revenue_engine import reconcile_revenue
        return jsonify(reconcile_revenue())


    # ══════════════════════════════════════════════════════════════════════════════
    # Work Item 5: Vendor Intelligence APIs
    # ══════════════════════════════════════════════════════════════════════════════
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/vendor/intelligence")
@auth_required
def api_vendor_intelligence():
    from src.agents.vendor_intelligence import get_agent_status
    return jsonify(get_agent_status())

@bp.route("/api/vendor/score-all", methods=["POST"])
@auth_required
def api_vendor_score_all():
    from src.agents.vendor_intelligence import score_all_vendors
    return jsonify(score_all_vendors())

@bp.route("/api/vendor/preferred")
@auth_required
def api_vendor_preferred():
    from src.agents.vendor_intelligence import get_preferred_vendors
    return jsonify(get_preferred_vendors())

@bp.route("/api/vendor/compare-product")
@auth_required
def api_vendor_compare_product():
    desc = request.args.get("description", "")
    if not desc:
        return jsonify({"ok": False, "error": "description required"})
    from src.agents.vendor_intelligence import compare_vendors
    return jsonify({"ok": True, "comparisons": compare_vendors(desc)})

@bp.route("/api/vendor/enrichment")
@auth_required
def api_vendor_enrichment():
    from src.agents.vendor_intelligence import get_enrichment_status
    return jsonify(get_enrichment_status())


# ══════════════════════════════════════════════════════════════════════════════
# Work Item 6: Action-Oriented Dashboard API
# ══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/dashboard/actions")
@auth_required
def api_dashboard_actions():
    """Build the action-oriented dashboard data."""
    urgent = []
    action_needed = []
    progress = []

    _now = _dt.now(_tz.utc).isoformat()

    # ── URGENT (red) ─────────────────────────────────────
    # Failed emails
    try:
        from src.agents.email_lifecycle import get_outbox_summary
        ob = get_outbox_summary()
        if ob.get("permanently_failed", 0) > 0:
            urgent.append({
                "icon": "🔴", "label": f"{ob['permanently_failed']} emails permanently failed",
                "link": "/outbox", "type": "failed_emails"
            })
        if ob.get("failed", 0) > 0:
            urgent.append({
                "icon": "🟠", "label": f"{ob['failed']} emails failed (retrying)",
                "link": "/outbox", "type": "retrying_emails"
            })
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Expiring quotes
    try:
        from src.agents.quote_lifecycle import get_expiring_soon
        exp = get_expiring_soon(3)
        if exp:
            urgent.append({
                "icon": "🔴", "label": f"{len(exp)} quotes expiring in < 3 days",
                "link": "/quotes", "type": "expiring_quotes",
                "items": [{"qn": q["quote_number"], "agency": q.get("agency",""), "total": q.get("total",0)} for q in exp[:5]]
            })
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # ── ACTION NEEDED (yellow) ────────────────────────────
    # Draft emails awaiting review
    try:
        from src.agents.email_lifecycle import get_outbox_summary
        ob = get_outbox_summary()
        if ob.get("drafts", 0) > 0:
            action_needed.append({
                "icon": "📧", "label": f"{ob['drafts']} email drafts to review",
                "link": "/outbox", "type": "draft_emails", "count": ob['drafts']
            })
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # New leads needing attention
    try:
        from src.agents.lead_nurture_agent import get_unified_pipeline
        pipe = get_unified_pipeline()
        new_count = pipe.get("by_status", {}).get("new", 0)
        if new_count > 0:
            action_needed.append({
                "icon": "🎯", "label": f"{new_count} new leads to review",
                "link": "/growth", "type": "new_leads", "count": new_count
            })
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Quotes needing follow-up (sent > 7d ago, no follow-up yet)
    try:
        from src.agents.quote_lifecycle import get_expiring_soon
        exp_7 = get_expiring_soon(14)
        sent_need_followup = [q for q in exp_7 if q.get("status") == "sent" and (q.get("follow_up_count", 0) or 0) == 0]
        if sent_need_followup:
            action_needed.append({
                "icon": "📋", "label": f"{len(sent_need_followup)} sent quotes need follow-up",
                "link": "/quotes", "type": "followup_quotes"
            })
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # ── PROGRESS (green) ──────────────────────────────────
    # Revenue goal
    try:
        from src.agents.revenue_engine import get_goal_progress
        goal = get_goal_progress()
        if goal.get("ok"):
            progress.append({
                "icon": "💰", "label": f"${goal['ytd_revenue']:,.0f} revenue YTD ({goal['pct_of_goal']}% of $2M goal)",
                "link": "/revenue", "type": "revenue",
                "value": goal["pct_of_goal"]
            })
            progress.append({
                "icon": "📊", "label": f"${goal['weighted_pipeline']:,.0f} weighted pipeline",
                "link": "/revenue", "type": "pipeline",
            })
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Quote pipeline
    try:
        from src.agents.quote_lifecycle import get_pipeline_summary
        ps = get_pipeline_summary()
        if ps.get("ok"):
            won = ps.get("pipeline", {}).get("won", {})
            if won.get("count", 0) > 0:
                progress.append({
                    "icon": "🏆", "label": f"{won['count']} quotes won (${won['value']:,.0f})",
                    "link": "/quotes", "type": "won_quotes"
                })
            conv = ps.get("conversion_rate", 0)
            progress.append({
                "icon": "📈", "label": f"{conv}% quote conversion rate",
                "link": "/pipeline", "type": "conversion"
            })
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Email engagement
    try:
        from src.agents.email_lifecycle import get_engagement_stats
        eng = get_engagement_stats()
        if eng.get("total_sent", 0) > 0:
            progress.append({
                "icon": "📬", "label": f"{eng['open_rate']}% email open rate ({eng['total_sent']} sent)",
                "link": "/outbox", "type": "email_engagement"
            })
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Vendor intelligence
    try:
        from src.agents.vendor_intelligence import get_enrichment_status
        ve = get_enrichment_status()
        if ve.get("total_vendors", 0) > 0:
            scored_pct = ve.get("scored_pct", 0)
            progress.append({
                "icon": "🏭", "label": f"{ve['total_vendors']} vendors tracked ({scored_pct}% scored)",
                "link": "/vendors", "type": "vendor_intel"
            })
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    return jsonify({
        "ok": True,
        "urgent": urgent,
        "action_needed": action_needed,
        "progress": progress,
        "generated_at": _now,
    })


# ══════════════════════════════════════════════════════════════════════════════
# Revenue Dashboard Page HTML
# ══════════════════════════════════════════════════════════════════════════════

_REVENUE_PAGE_HTML = """
<main class="ctr" role="main">
<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:24px">
 <h2 style="margin:0;font-size:22px">💰 Revenue Dashboard</h2>
 <div style="display:flex;gap:8px">
  <button onclick="reconcileRevenue()" class="hdr-btn" style="padding:6px 14px;font-size:14px">🔄 Reconcile</button>
  <a href="/pipeline" class="hdr-btn" style="padding:6px 14px;font-size:14px;text-decoration:none">🔄 Pipeline</a>
 </div>
</div>

<!-- Goal Tracker -->
<div id="goal-section" style="background:var(--crd);border:1px solid var(--bd);border-radius:12px;padding:20px;margin-bottom:20px">
 <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
  <h3 style="margin:0;font-size:16px">🎯 Annual Goal: $2M</h3>
  <span id="goal-pct" style="font-size:24px;font-weight:700;color:var(--ac)">--</span>
 </div>
 <div style="background:var(--bg);border-radius:8px;height:24px;overflow:hidden;margin-bottom:12px">
  <div id="goal-bar" style="height:100%;background:linear-gradient(90deg,#2ecc71,#27ae60);border-radius:8px;transition:width 0.8s;width:0%"></div>
 </div>
 <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;text-align:center">
  <div><div style="font-size:14px;color:var(--tx2)">YTD Revenue</div><div id="goal-ytd" style="font-size:18px;font-weight:600">$--</div></div>
  <div><div style="font-size:14px;color:var(--tx2)">Weighted Pipeline</div><div id="goal-pipe" style="font-size:18px;font-weight:600;color:var(--yl)">$--</div></div>
  <div><div style="font-size:14px;color:var(--tx2)">Projected Annual</div><div id="goal-proj" style="font-size:18px;font-weight:600">$--</div></div>
  <div><div style="font-size:14px;color:var(--tx2)">Daily Run Rate</div><div id="goal-daily" style="font-size:18px;font-weight:600">$--</div></div>
 </div>
</div>

<!-- Monthly Chart + Pipeline -->
<div style="display:grid;grid-template-columns:2fr 1fr;gap:16px;margin-bottom:20px">
 <div style="background:var(--crd);border:1px solid var(--bd);border-radius:12px;padding:20px">
  <h3 style="margin:0 0 12px;font-size:15px">📊 Monthly Revenue</h3>
  <canvas id="rev-chart" height="220"></canvas>
 </div>
 <div style="background:var(--crd);border:1px solid var(--bd);border-radius:12px;padding:20px">
  <h3 style="margin:0 0 12px;font-size:15px">🔄 Pipeline Forecast</h3>
  <div id="pipeline-list" style="max-height:240px;overflow-y:auto"></div>
 </div>
</div>

<!-- Margins + Top Customers -->
<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px">
 <div style="background:var(--crd);border:1px solid var(--bd);border-radius:12px;padding:20px">
  <h3 style="margin:0 0 12px;font-size:15px">📉 Margin Analysis</h3>
  <div id="margin-summary" style="margin-bottom:12px"></div>
  <div id="margin-list" style="max-height:200px;overflow-y:auto"></div>
 </div>
 <div style="background:var(--crd);border:1px solid var(--bd);border-radius:12px;padding:20px">
  <h3 style="margin:0 0 12px;font-size:15px">🏢 Top Customers</h3>
  <div id="top-customers" style="max-height:240px;overflow-y:auto"></div>
 </div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script>
let revChart;
async function loadRevenue() {
  try {
    const r = await fetch('/api/revenue/dashboard');
    const d = await r.json();
    if (!d.ok) return;

    // Goal section
    const g = d.goal || {};
    document.getElementById('goal-pct').textContent = (g.pct_of_goal||0).toFixed(1) + '%';
    document.getElementById('goal-bar').style.width = Math.min(g.pct_of_goal||0, 100) + '%';
    document.getElementById('goal-ytd').textContent = '$' + (g.ytd_revenue||0).toLocaleString(undefined,{maximumFractionDigits:0});
    document.getElementById('goal-pipe').textContent = '$' + (g.weighted_pipeline||0).toLocaleString(undefined,{maximumFractionDigits:0});
    document.getElementById('goal-proj').textContent = '$' + (g.projected_annual||0).toLocaleString(undefined,{maximumFractionDigits:0});
    document.getElementById('goal-daily').textContent = '$' + (g.daily_rate||0).toLocaleString(undefined,{maximumFractionDigits:0});

    // Monthly chart
    const months = d.monthly || [];
    const labels = months.map(m => m.month);
    const revenues = months.map(m => m.revenue);
    const profits = months.map(m => m.profit);

    if (revChart) revChart.destroy();
    const ctx = document.getElementById('rev-chart');
    if (ctx) {
      revChart = new Chart(ctx, {
        type: 'bar',
        data: {
          labels,
          datasets: [
            {label: 'Revenue', data: revenues, backgroundColor: '#2ecc7180', borderColor: '#2ecc71', borderWidth: 1},
            {label: 'Profit', data: profits, backgroundColor: '#3498db80', borderColor: '#3498db', borderWidth: 1}
          ]
        },
        options: {
          responsive: true,
          plugins: {legend: {position: 'top', labels: {color: '#e0e0e0', font: {size: 11}}}},
          scales: {
            x: {ticks: {color: '#999'}, grid: {display: false}},
            y: {ticks: {color: '#999', callback: v => '$'+v.toLocaleString()}, grid: {color: '#333'}}
          }
        }
      });
    }

    // Pipeline list
    const pipe = d.pipeline?.items || [];
    const pEl = document.getElementById('pipeline-list');
    if (pEl) {
      pEl.innerHTML = pipe.length ? pipe.slice(0,10).map(p =>
        `<div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--bd);font-size:14px">
          <div><a href="/quote/${p.quote_number}" style="color:var(--ac)">${p.quote_number}</a>
           <span style="color:var(--tx2);margin-left:4px">${p.agency||''}</span></div>
          <div style="text-align:right">
            <div style="font-weight:600">$${(p.weighted_value||0).toLocaleString()}</div>
            <div style="font-size:13px;color:var(--tx2)">${(p.win_probability*100).toFixed(0)}% × $${(p.total||0).toLocaleString()}</div>
          </div>
        </div>`
      ).join('') : '<div style="color:var(--tx2);font-size:14px">No open quotes in pipeline</div>';
    }

    // Margins
    const m = d.margins || {};
    const mSummary = document.getElementById('margin-summary');
    if (mSummary) {
      mSummary.innerHTML = m.total_quotes_with_cost ?
        `<div style="display:flex;gap:16px;font-size:13px">
          <span>Avg margin: <b style="color:${m.avg_margin>=15?'#2ecc71':'#e74c3c'}">${(m.avg_margin||0).toFixed(1)}%</b></span>
          <span>Low margin: <b style="color:#e67e22">${m.low_margin_count||0}</b></span>
          <span>Critical: <b style="color:#e74c3c">${m.critical_margin_count||0}</b></span>
        </div>` : '<span style="font-size:14px;color:var(--tx2)">No cost data yet</span>';
    }
    const mList = document.getElementById('margin-list');
    if (mList && m.items) {
      mList.innerHTML = m.items.slice(0,10).map(i =>
        `<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid var(--bd);font-size:14px">
          <span>${i.quote_number} — ${i.agency||''}</span>
          <span style="color:${i.margin_pct>=15?'#2ecc71':i.margin_pct>=10?'#e67e22':'#e74c3c'};font-weight:600">${i.margin_pct?.toFixed(1)}%</span>
        </div>`
      ).join('');
    }

    // Top customers
    const tc = d.top_customers || [];
    const tcEl = document.getElementById('top-customers');
    if (tcEl) {
      tcEl.innerHTML = tc.length ? tc.map((c,i) =>
        `<div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--bd);font-size:14px">
          <span>${i+1}. ${c.agency||'Unknown'} <span style="color:var(--tx2)">(${c.deal_count} deals)</span></span>
          <span style="font-weight:600">$${(c.total_revenue||0).toLocaleString(undefined,{maximumFractionDigits:0})}</span>
        </div>`
      ).join('') : '<div style="color:var(--tx2);font-size:14px">No revenue data yet</div>';
    }
  } catch(e) { console.error('Revenue load:', e); }
}

async function reconcileRevenue() {
  const r = await fetch('/api/revenue/reconcile', {method:'POST'});
  const d = await r.json();
  alert(d.ok ? `Reconciled ${d.synced} entries` : 'Failed');
  loadRevenue();
}

loadRevenue();
</script>
"""


_dash_init_cache = {"data": None, "ts": 0}

@bp.route("/api/dashboard/init")
@auth_required
def api_dashboard_init():
    """Combined endpoint — returns ALL home page widget data in one call.
    Replaces 6 separate fetch() calls that each loaded the same JSON files."""
    import time as _time
    t0 = _time.time()
    # 90-second cache — this is the most expensive home page call
    global _dash_init_cache
    if _dash_init_cache["data"] and (_time.time() - _dash_init_cache["ts"]) < 90:
        return jsonify(_dash_init_cache["data"])
    result = {"ok": True}

    # ── 1. Actions (urgent / action_needed / progress) ──
    try:
        # Inline the lightweight parts of api_dashboard_actions
        urgent = []
        action_needed = []
        progress_items = []

        try:
            from src.agents.email_lifecycle import get_outbox_summary
            ob = get_outbox_summary()
            if ob.get("permanently_failed", 0) > 0:
                urgent.append({"icon": "🔴", "label": f"{ob['permanently_failed']} emails permanently failed", "link": "/outbox", "type": "failed_emails"})
            if ob.get("drafts", 0) > 0:
                action_needed.append({"icon": "📧", "label": f"{ob['drafts']} email drafts to review", "link": "/outbox", "type": "draft_emails", "count": ob['drafts']})
        except Exception:
            pass

        try:
            from src.agents.revenue_engine import get_goal_progress
            goal = get_goal_progress()
            if goal.get("ok"):
                progress_items.append({"icon": "💰", "label": f"${goal['ytd_revenue']:,.0f} revenue YTD ({goal['pct_of_goal']}% of $2M goal)", "link": "/revenue", "type": "revenue", "value": goal["pct_of_goal"]})
                progress_items.append({"icon": "📊", "label": f"${goal['weighted_pipeline']:,.0f} weighted pipeline", "link": "/revenue", "type": "pipeline"})
        except Exception:
            pass

        try:
            from src.agents.quote_lifecycle import get_pipeline_summary
            ps = get_pipeline_summary()
            if ps.get("ok"):
                conv = ps.get("conversion_rate", 0)
                progress_items.append({"icon": "📈", "label": f"{conv}% quote conversion rate", "link": "/pipeline", "type": "conversion"})
        except Exception:
            pass

        try:
            from src.core.db import get_db
            with get_db() as conn:
                vendor_count = conn.execute("SELECT COUNT(DISTINCT company) FROM contacts WHERE tags LIKE '%vendor%'").fetchone()[0]
                scored = conn.execute("SELECT COUNT(DISTINCT company) FROM contacts WHERE tags LIKE '%vendor%' AND score > 0").fetchone()[0]
                progress_items.append({"icon": "🏭", "label": f"{vendor_count} vendors tracked ({scored/max(vendor_count,1)*100:.0f}% scored)", "link": "/crm", "type": "vendors"})
        except Exception:
            pass

        # ── Orders needing action ──
        try:
            orders_path = os.path.join(DATA_DIR, "orders.json")
            if os.path.exists(orders_path):
                with open(orders_path) as f:
                    all_orders = json.load(f)
                # Filter out test orders everywhere
                real_orders = {k: o for k, o in all_orders.items()
                               if "TEST" not in (o.get("po_number", "") or "").upper()
                               and not o.get("is_test")
                               and o.get("status") not in ("cancelled", "test", "deleted")}

                # New orders: must have either total > 0 OR at least one line item
                # Skip phantom $0 orders with no items (auto-created from bad PO emails)
                new_orders = []
                stale_orders = []
                for o in real_orders.values():
                    if o.get("status") != "new":
                        continue
                    has_value = (o.get("total", 0) or 0) > 0
                    has_items = len(o.get("line_items", [])) > 0 and any(
                        (li.get("description", "") or "").strip() for li in o.get("line_items", [])
                    )
                    if not has_value and not has_items:
                        continue  # Skip phantom orders with no value AND no real items

                    # Check age — orders older than 30 days in "new" are stale
                    age_days = 0
                    try:
                        created = o.get("created_at", "")
                        if created:
                            from datetime import datetime as _dt
                            created_dt = _dt.fromisoformat(created[:19])
                            age_days = (_dt.now() - created_dt).days
                    except Exception:
                        pass

                    if age_days > 30:
                        stale_orders.append(o)
                    else:
                        new_orders.append(o)

                if new_orders:
                    total_val = sum(o.get("total", 0) or 0 for o in new_orders)
                    urgent.append({"icon": "🏆", "label": f"{len(new_orders)} new PO{'s' if len(new_orders) > 1 else ''} — ${total_val:,.0f} to source", "link": "/orders", "type": "new_orders", "count": len(new_orders)})
                if stale_orders:
                    action_needed.append({"icon": "⏰", "label": f"{len(stale_orders)} stale order{'s' if len(stale_orders) > 1 else ''} (30d+ in New)", "link": "/orders", "type": "stale_orders", "count": len(stale_orders)})

                delivered_orders = [o for o in real_orders.values() if o.get("status") == "delivered"]
                if delivered_orders:
                    action_needed.append({"icon": "💰", "label": f"{len(delivered_orders)} order{'s' if len(delivered_orders) > 1 else ''} ready to invoice", "link": "/orders", "type": "invoice_ready", "count": len(delivered_orders)})
        except Exception:
            pass

        result["actions"] = {"ok": True, "urgent": urgent, "action_needed": action_needed, "progress": progress_items}
    except Exception as e:
        result["actions"] = {"ok": False, "error": str(e), "urgent": [], "action_needed": [], "progress": []}

    # ── 2. Funnel stats ──
    try:
        from src.api.dashboard import _load_price_checks, load_rfqs, _is_user_facing_pc
        all_pcs = _load_price_checks()
        user_pcs = {pid: pc for pid, pc in all_pcs.items() if _is_user_facing_pc(pc)}
        rfqs = load_rfqs()
        rfqs_nt = {k: v for k, v in rfqs.items() if not v.get("is_test")}

        inbox = sum(1 for pc in user_pcs.values() if pc.get("status") in ("parsed","new","parse_error")) + \
                sum(1 for r in rfqs_nt.values() if r.get("status") in ("new","pending","parsed"))
        priced = sum(1 for pc in user_pcs.values() if pc.get("status") in ("priced","ready","auto_drafted")) + \
                 sum(1 for r in rfqs_nt.values() if r.get("status") in ("priced","ready"))
        quoted = sum(1 for pc in user_pcs.values() if pc.get("status") in ("quoted","generated")) + \
                 sum(1 for r in rfqs_nt.values() if r.get("status") in ("generated","quoted"))
        sent = sum(1 for pc in user_pcs.values() if pc.get("status") in ("sent","completed")) + \
               sum(1 for r in rfqs_nt.values() if r.get("status") == "sent")

        # Quick quote stats from DB
        won_count = 0; won_value = 0; orders_count = 0; pipeline_val = 0
        try:
            from src.core.db import get_db
            with get_db() as conn:
                for row in conn.execute("SELECT status, COUNT(*) as c, COALESCE(SUM(total),0) as t FROM quotes WHERE is_test=0 GROUP BY status").fetchall():
                    s = row["status"]
                    if s == "won": won_count = row["c"]; won_value = row["t"]
                    elif s in ("pending","sent","draft"): pipeline_val += row["t"]
                orders_count = conn.execute("SELECT COUNT(*) FROM orders WHERE status NOT IN ('cancelled','test','deleted') AND po_number NOT LIKE '%TEST%'").fetchone()[0]
        except Exception:
            pass

        # Include orders.json in won_value if orders exist without won quotes
        try:
            orders_path = os.path.join(DATA_DIR, "orders.json")
            if os.path.exists(orders_path):
                with open(orders_path) as f:
                    json_orders = json.load(f)
                real_orders = {k: o for k, o in json_orders.items()
                               if o.get("status") not in ("cancelled", "test", "deleted")
                               and "TEST" not in (o.get("po_number", "") or "").upper()
                               and not o.get("is_test")}
                order_total = sum(o.get("total", 0) for o in real_orders.values())
                orders_count = max(orders_count, len(real_orders))
                if order_total > won_value:
                    won_value = order_total
        except Exception:
            pass

        result["funnel"] = {
            "ok": True, "inbox": inbox, "priced": priced, "quoted": quoted,
            "sent": sent, "won": won_count, "won_value": won_value,
            "orders": orders_count, "pipeline_value": pipeline_val,
        }
    except Exception as e:
        result["funnel"] = {"ok": False, "error": str(e)}

    # ── 3. Revenue ──
    try:
        from src.agents.sales_intel import update_revenue_tracker
        result["revenue"] = update_revenue_tracker()
    except Exception as e:
        result["revenue"] = {"ok": False, "error": str(e)}

    # ── 4. QA workflow (lightweight) ──
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute("SELECT * FROM workflow_runs ORDER BY id DESC LIMIT 1").fetchone()
            result["qa"] = dict(row) if row else {"status": "none"}
    except Exception:
        result["qa"] = {"status": "none"}

    # ── 5. Manager metrics (skip the heavy agent calls, use cached data) ──
    try:
        from src.core.db import get_db
        with get_db() as conn:
            total_quotes = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_test=0").fetchone()[0]
            total_revenue = conn.execute("SELECT COALESCE(SUM(total),0) FROM quotes WHERE is_test=0 AND status='won'").fetchone()[0]
            pipeline = conn.execute("SELECT COALESCE(SUM(total),0) FROM quotes WHERE is_test=0 AND status IN ('pending','sent')").fetchone()[0]
        # Also include orders.json revenue (POs may exist without won quotes)
        try:
            orders_path = os.path.join(DATA_DIR, "orders.json")
            if os.path.exists(orders_path):
                with open(orders_path) as f:
                    json_orders = json.load(f)
                order_revenue = sum(o.get("total", 0) for o in json_orders.values()
                                   if o.get("status") not in ("cancelled", "test", "deleted")
                                   and "TEST" not in (o.get("po_number", "") or "").upper()
                                   and not o.get("is_test"))
                if order_revenue > total_revenue:
                    total_revenue = order_revenue
        except Exception:
            pass
        result["metrics"] = {"ok": True, "total_quotes": total_quotes, "total_revenue": total_revenue, "pipeline": pipeline}
    except Exception as e:
        result["metrics"] = {"ok": False, "error": str(e)}

    # ── 6. Order health (progress board data) ──
    try:
        from src.agents.order_digest import get_order_health
        result["order_health"] = get_order_health()
    except Exception as e:
        result["order_health"] = {"ok": False, "error": str(e)}

    # ── 7. Growth Engine summary ──
    try:
        from src.agents.growth_agent import get_growth_kpis, get_quick_wins, generate_daily_brief
        growth_kpis = get_growth_kpis()
        quick_wins = get_quick_wins(max_results=3)
        brief = generate_daily_brief()
        result["growth"] = {
            "ok": True,
            "kpis": growth_kpis,
            "quick_wins": quick_wins,
            "brief_summary": brief.get("summary", ""),
            "priority_count": len(brief.get("priorities", [])),
            "critical_count": sum(1 for p in brief.get("priorities", []) if p.get("level") == "critical"),
        }
    except Exception as e:
        result["growth"] = {"ok": False, "error": str(e)}

    # ── 8. Award Intel (SCPRS monitoring) ──
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # SCPRS data volume
            po_count = conn.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]
            line_count = conn.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]
            wq_count = conn.execute("SELECT COUNT(*) FROM won_quotes").fetchone()[0]
            # Recent award tracker activity
            recent_wins = conn.execute("SELECT COUNT(*) FROM award_tracker_log WHERE outcome='won' AND checked_at > datetime('now', '-30 days')").fetchone()[0]
            recent_losses = conn.execute("SELECT COUNT(*) FROM award_tracker_log WHERE outcome='lost' AND checked_at > datetime('now', '-30 days')").fetchone()[0]
            # SCPRS freshness
            freshness = conn.execute("SELECT agency_key, last_pull FROM scprs_pull_schedule ORDER BY last_pull DESC").fetchall()
        result["award_intel"] = {
            "po_count": po_count, "line_count": line_count, "wq_count": wq_count,
            "recent_wins": recent_wins, "recent_losses": recent_losses,
            "scprs_freshness": [{"agency": r[0], "last_pull": r[1]} for r in freshness] if freshness else [],
        }
    except Exception:
        result["award_intel"] = {}

    # ── 9. Parse error count (last 48h) ──
    try:
        from src.core.db import get_db
        with get_db() as conn:
            parse_errors = conn.execute("""
                SELECT COUNT(*) FROM price_checks
                WHERE status='parse_error' AND created_at >= datetime('now', '-2 days')
            """).fetchone()[0]
        result["parse_errors"] = parse_errors
    except Exception:
        result["parse_errors"] = 0

    result["_ms"] = round((_time.time() - t0) * 1000)
    _dash_init_cache["data"] = result
    _dash_init_cache["ts"] = _time.time()
    return jsonify(result)


# ── Activity Feed: unified recent events across the system ──────────
@bp.route("/api/activity-feed")
@auth_required
def api_activity_feed():
    """Aggregated activity stream: CRM logs, quote/PC changes, orders, outreach."""
    import time as _time
    t0 = _time.time()
    limit = min(int(request.args.get("limit", 30)), 100)
    events = []

    # 1) CRM activity_log (emails, calls, notes)
    try:
        from src.core.db import get_db
        db = get_db()
        rows = db.execute(
            "SELECT * FROM activity_log ORDER BY logged_at DESC LIMIT ?", (limit,)
        ).fetchall()
        for r in rows:
            rdict = dict(r)
            events.append({
                "ts": rdict.get("logged_at", ""),
                "type": rdict.get("event_type") or rdict.get("type", "note"),
                "icon": {"email_sent": "📧", "email_received": "📬", "voice_called": "📞",
                         "note": "📝", "chat": "💬", "lead_converted": "🎯",
                         "status_change": "🔄"}.get(rdict.get("event_type", ""), "📋"),
                "title": _fmt_activity(rdict),
                "detail": (rdict.get("detail") or "")[:120],
                "link": f"/growth/prospect/{rdict.get('contact_id', '')}" if rdict.get("contact_id") else None,
                "source": "crm",
            })
    except Exception:
        pass

    # 1b) RFQ/PC/Quote events from crm_activity.json
    try:
        _crm_events = _load_crm_activity()
        _icon_map = {"rfq_created":"📄","rfq_pricing_finalized":"💰","rfq_package_generated":"📦","rfq_email_sent":"📧","rfq_field_updated":"✏️","quote_generated":"💰","quote_sent":"📤","quote_won":"🏆","quote_lost":"📉","pc_follow_up":"🔄","order_created":"📦","email_sent":"📧"}
        if isinstance(_crm_events, list):
            for _evt in reversed(_crm_events[-limit:]):
                _et = _evt.get("event_type", "")
                if _et in ("scprs_lookup",):
                    continue
                _src = "crm"
                for _pfx, _s in [("rfq_", "rfq"), ("quote_", "quotes"), ("pc_", "pc"), ("order_", "orders")]:
                    if _et.startswith(_pfx):
                        _src = _s
                        break
                events.append({
                    "ts": _evt.get("timestamp", ""),
                    "type": _et,
                    "icon": _icon_map.get(_et, "📋"),
                    "title": (_evt.get("description", "") or "")[:120],
                    "detail": "",
                    "link": f"/rfq/{_evt.get('ref_id', '')}" if _et.startswith("rfq_") else (
                        f"/pricecheck/{_evt.get('ref_id', '')}" if _et.startswith("pc_") else None),
                    "source": _src,
                    "ref_id": _evt.get("ref_id", ""),
                })
    except Exception as _cfe:
        log.debug("CRM activity feed: %s", _cfe)

    # 2) Recent quote status changes
    try:
        from src.core.paths import data_path
        import json
        ql_path = data_path("quotes_log.json")
        if ql_path.exists():
            quotes = json.loads(ql_path.read_text())
            if isinstance(quotes, dict):
                quotes = list(quotes.values())
            for q in sorted(quotes, key=lambda x: x.get("updated_at") or x.get("created_at", ""), reverse=True)[:limit]:
                status = q.get("status", "")
                if status in ("sent", "won", "lost"):
                    events.append({
                        "ts": q.get("updated_at") or q.get("created_at", ""),
                        "type": f"quote_{status}",
                        "icon": {"sent": "📤", "won": "🏆", "lost": "📉"}.get(status, "📋"),
                        "title": f"Quote #{q.get('number', '?')} → {status.upper()}",
                        "detail": q.get("institution", "")[:80],
                        "link": f"/quote/{q.get('id', '')}",
                        "source": "quotes",
                    })
    except Exception:
        pass

    # 3) Recent orders
    try:
        from src.core.paths import data_path
        import json
        op = data_path("orders.json")
        if op.exists():
            orders = json.loads(op.read_text())
            if isinstance(orders, dict):
                orders = list(orders.values())
            for o in sorted(orders, key=lambda x: x.get("updated_at") or x.get("created_at", ""), reverse=True)[:limit]:
                events.append({
                    "ts": o.get("updated_at") or o.get("created_at", ""),
                    "type": f"order_{o.get('status', 'new')}",
                    "icon": {"new": "📦", "sourcing": "🛒", "shipped": "🚚",
                             "delivered": "✅", "invoiced": "💰", "closed": "🏁"}.get(o.get("status", ""), "📦"),
                    "title": f"Order {o.get('po_number', '?')} — {(o.get('status') or 'new').replace('_', ' ').title()}",
                    "detail": o.get("institution", "")[:80],
                    "link": f"/order/{o.get('id', '')}",
                    "source": "orders",
                })
    except Exception:
        pass

    # 4) Growth outreach
    try:
        from src.core.paths import data_path
        import json
        gp = data_path("growth_outreach.json")
        if gp.exists():
            outreach = json.loads(gp.read_text())
            if isinstance(outreach, list):
                for out in sorted(outreach, key=lambda x: x.get("sent_at", ""), reverse=True)[:limit]:
                    events.append({
                        "ts": out.get("sent_at", ""),
                        "type": "outreach",
                        "icon": "🚀",
                        "title": f"Outreach → {out.get('agency', '?')}",
                        "detail": (out.get("buyer_name") or out.get("to", ""))[:80],
                        "link": f"/growth/prospect/{out.get('prospect_id', '')}" if out.get("prospect_id") else None,
                        "source": "growth",
                    })
    except Exception:
        pass

    # Sort all by timestamp descending, take top N
    events.sort(key=lambda e: e.get("ts", ""), reverse=True)
    events = events[:limit]

    return jsonify({"ok": True, "events": events, "_ms": round((_time.time() - t0) * 1000)})


def _fmt_activity(row):
    """Format a CRM activity_log row into a readable title."""
    etype = row.get("event_type") or row.get("type", "note")
    contact = row.get("contact_name") or row.get("contact_id", "")[:12]
    titles = {
        "email_sent": f"Email sent to {contact}",
        "email_received": f"Email from {contact}",
        "voice_called": f"Called {contact}",
        "note": f"Note on {contact}",
        "chat": f"Chat with {contact}",
        "lead_converted": f"Converted {contact} to customer",
        "status_change": f"Status change: {contact}",
    }
    return titles.get(etype, f"{etype}: {contact}")
