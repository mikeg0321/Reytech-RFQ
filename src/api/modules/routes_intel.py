# routes_intel.py

# ── JSON→SQLite compatibility (Phase 32c migration) ──────────────────────────
try:
    from src.core.db import (
        get_all_customers, get_all_price_checks, get_price_check, upsert_price_check,
        get_outbox, upsert_outbox_email, update_outbox_status, get_email_templates,
        get_market_intelligence, upsert_market_intelligence, get_intel_agencies,
        get_all_vendors, get_vendor_registrations, get_qa_reports, save_qa_report,
        get_growth_outreach, save_growth_campaign,
    )
    _HAS_DB_DAL = True
except ImportError:
    _HAS_DB_DAL = False
# ─────────────────────────────────────────────────────────────────────────────
# 170 routes, 7596 lines
# Loaded by dashboard.py via load_module()

# GROWTH INTELLIGENCE — Full SCPRS pull + Gap analysis + Auto close-lost
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/intel/scprs/pull-all", methods=["POST"])
@auth_required
def api_intel_pull_all():
    """Trigger full SCPRS pull for ALL agencies in background."""
    try:
        from src.agents.scprs_intelligence_engine import pull_all_agencies_background
        priority = (request.json or {}).get("priority", "P0")
        result = pull_all_agencies_background(notify_fn=_push_notification, priority_filter=priority)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/scprs/engine-status")
@auth_required
def api_intel_engine_status():
    """Full SCPRS engine status — pull progress, record counts, schedule."""
    try:
        from src.agents.scprs_intelligence_engine import get_engine_status
        return jsonify({"ok": True, **get_engine_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/scprs/po-monitor", methods=["POST"])
@auth_required
def api_intel_po_monitor():
    """Run PO award monitor — check open quotes against SCPRS, auto close-lost."""
    try:
        from src.agents.scprs_intelligence_engine import run_po_award_monitor
        result = run_po_award_monitor(notify_fn=_push_notification)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/growth")
@auth_required
def api_intel_growth():
    """Full growth intelligence JSON — gaps, win-back, competitors, recs."""
    try:
        from src.agents.growth_agent import get_scprs_growth_intelligence
        return jsonify(get_scprs_growth_intelligence())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/intel/growth")
@auth_required
def page_intel_growth():
    """Growth Intelligence Dashboard — the main 'what should I do?' page."""
    import json as _j
    from src.agents.scprs_intelligence_engine import get_engine_status, get_growth_intelligence
    from src.agents.scprs_intelligence_engine import _engine_status as eng_st

    try:
        engine = get_engine_status()
        intel = get_growth_intelligence()
    except Exception as e:
        engine = {"running": False, "by_agency": [], "total_line_items": 0, "total_gap_items": 0, "quotes_auto_closed": 0}
        intel = {"recommendations": [], "top_gaps": [], "win_back": [], "competitors": [], "by_agency": [], "recent_losses": []}

    running = eng_st.get("running", False)
    current_agency = eng_st.get("current_agency", "")
    total_lines = engine.get("total_line_items", 0)
    total_gaps = engine.get("total_gap_items", 0)
    auto_closed = engine.get("quotes_auto_closed", 0)
    agencies_data = engine.get("by_agency", [])
    recs = intel.get("recommendations", [])
    gaps = intel.get("top_gaps", [])
    win_back = intel.get("win_back", [])
    competitors = intel.get("competitors", [])
    losses = intel.get("recent_losses", [])

    # Compute totals
    total_gap_spend = sum(g.get("total_spend") or 0 for g in gaps)
    total_wb_spend = sum(w.get("total_spend") or 0 for w in win_back)
    agencies_loaded = len([a for a in agencies_data if (a.get("pos") or 0) > 0])

    no_data = total_lines == 0

    # ── Render recommendations ──────────────────────────────────────────────
    def rec_color(priority):
        return {"P0": "var(--rd)", "P1": "var(--ac)", "P2": "var(--tx2)"}.get(priority, "var(--tx)")

    def rec_icon(rec_type):
        return {"add_product": "📦", "win_back": "⚔️", "pricing": "💲",
                "expand_agency": "🏛️", "dvbe_displace": "🏅", 
                "dvbe_partner": "🤝", "source_anything": "🔍"}.get(rec_type, "🎯")
    
    def rec_badge(rec):
        badges = []
        if rec.get("dvbe_angle"):
            badges.append('<span style="background:rgba(22,163,74,.15);color:var(--gn);padding:2px 7px;border-radius:4px;font-size:10px;font-weight:700">DVBE</span>')
        if rec.get("partner_model"):
            badges.append('<span style="background:rgba(37,99,235,.15);color:var(--ac);padding:2px 7px;border-radius:4px;font-size:10px;font-weight:700">PARTNER</span>')
        return " ".join(badges)

    rec_html = ""
    for i, rec in enumerate(recs[:8]):
        val = rec.get("estimated_annual_value", 0) or 0
        prio = rec.get("priority", "P1")
        rtype = rec.get("type", "")
        agencies = ", ".join(rec.get("agencies", []))
        rec_html += f"""
<div style="border:1px solid var(--bd);border-left:4px solid {rec_color(prio)};border-radius:8px;padding:14px 18px;margin-bottom:10px;background:var(--bg2)">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px">
    <div style="flex:1">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;flex-wrap:wrap">
        <span style="font-size:16px">{rec_icon(rtype)}</span>
        <span style="font-size:14px;font-weight:700;color:var(--tx)">{rec.get("action","")}</span>
        <span style="font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px;background:rgba(0,0,0,.08);color:{rec_color(prio)}">{prio}</span>
        {rec_badge(rec)}
      </div>
      <div style="font-size:12px;color:var(--tx2);margin-bottom:6px;line-height:1.5">{rec.get("why","")}</div>
      <div style="font-size:12px;color:var(--ac);font-weight:500">▶ {rec.get("how","")[:120]}</div>
      {'<div style="font-size:11px;color:var(--tx2);margin-top:4px">📍 ' + agencies + '</div>' if agencies else ''}
    </div>
    <div style="text-align:right;flex-shrink:0">
      <div style="font-size:20px;font-weight:800;color:var(--gn)">${val:,.0f}</div>
      <div style="font-size:10px;color:var(--tx2)">est/yr</div>
    </div>
  </div>
</div>"""

    # ── Gap items table ─────────────────────────────────────────────────────
    gap_rows = ""
    for g in gaps[:20]:
        spend = g.get("total_spend") or 0
        ags = (g.get("agencies") or "").split(",")
        gap_rows += f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:7px 12px;font-size:12px">{g.get("description","")[:50]}</td>
  <td style="padding:7px 12px;font-size:11px;color:var(--tx2)">{(g.get("category") or "").replace("_"," ").title()}</td>
  <td style="padding:7px 12px;font-size:11px;text-align:center">{", ".join(ags[:3])}</td>
  <td style="padding:7px 12px;font-size:11px;text-align:center">{g.get("order_count",0)}</td>
  <td style="padding:7px 12px;font-size:12px;text-align:right">${g.get("avg_price") or 0:.2f}</td>
  <td style="padding:7px 12px;font-size:13px;font-weight:700;text-align:right;color:var(--rd)">${spend:,.0f}</td>
</tr>"""

    # ── Win-back table ──────────────────────────────────────────────────────
    wb_rows = ""
    for w in win_back[:15]:
        spend = w.get("total_spend") or 0
        vendors = (w.get("incumbent_vendors") or "Unknown").split(",")[:2]
        ags = (w.get("agencies") or "").split(",")
        wb_rows += f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:7px 12px;font-size:12px">{w.get("description","")[:45]}</td>
  <td style="padding:7px 12px;font-size:11px;font-weight:600;color:var(--ac)">{w.get("reytech_sku","—")}</td>
  <td style="padding:7px 12px;font-size:11px;color:var(--rd)">{vendors[0][:25] if vendors else "?"}</td>
  <td style="padding:7px 12px;font-size:11px;color:var(--tx2)">{", ".join(ags[:3])}</td>
  <td style="padding:7px 12px;font-size:12px;text-align:right">${w.get("avg_price") or 0:.2f}</td>
  <td style="padding:7px 12px;font-size:13px;font-weight:700;text-align:right;color:var(--gn)">${spend:,.0f}</td>
</tr>"""

    # ── Agency coverage ─────────────────────────────────────────────────────
    schedule = intel.get("pull_schedule", [])
    sch_map = {s.get("agency_key"): s for s in schedule}
    ag_rows = ""
    all_agencies = ["CCHCS", "CalVet", "DSH", "CalFire", "CDPH", "CalTrans", "CHP", "DGS"]
    for ak in all_agencies:
        ag_data = next((a for a in agencies_data if a.get("agency_key") == ak), {})
        pos = ag_data.get("pos") or 0
        sch = sch_map.get(ak, {})
        last = (sch.get("last_pull") or "Never")[:10]
        status_color = "var(--gn)" if pos > 0 else "var(--rd)"
        status_txt = f"✅ {pos} POs" if pos > 0 else "⬜ No data"
        ag_rows += f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:7px 12px;font-size:13px;font-weight:600">{ak}</td>
  <td style="padding:7px 12px;font-size:12px;color:{status_color}">{status_txt}</td>
  <td style="padding:7px 12px;font-size:11px;color:var(--tx2)">{last}</td>
  <td style="padding:7px 12px;font-size:11px;color:var(--tx2)">Every {sch.get("pull_interval_hours",168)}h</td>
  <td style="padding:7px 12px">
    <button onclick="pullAgency('{ak}')" style="font-size:10px;padding:3px 10px;border:1px solid var(--bd);border-radius:4px;background:none;color:var(--tx);cursor:pointer">Pull Now</button>
  </td>
</tr>"""

    # ── Lost quotes ─────────────────────────────────────────────────────────
    loss_rows = ""
    for l in losses[:6]:
        our = l.get("total") or 0
        theirs = l.get("scprs_total") or 0
        delta = our - theirs
        loss_rows += f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:7px 12px;font-size:12px;font-weight:600">{l.get("quote_number","")}</td>
  <td style="padding:7px 12px;font-size:12px">{l.get("agency","")} — {l.get("institution","")[:25]}</td>
  <td style="padding:7px 12px;font-size:12px;color:var(--rd)">{l.get("scprs_supplier","")[:25]}</td>
  <td style="padding:7px 12px;font-size:12px;text-align:right">${theirs:,.0f}</td>
  <td style="padding:7px 12px;font-size:12px;text-align:right">${our:,.0f}</td>
  <td style="padding:7px 12px;font-size:12px;font-weight:700;text-align:right;color:{'var(--rd)' if delta > 0 else 'var(--gn)'}">${abs(delta):,.0f} {'over' if delta > 0 else 'under'}</td>
</tr>"""

    pull_banner = ""
    if running:
        pull_banner = f'<div style="background:rgba(37,99,235,.1);border:1px solid var(--ac);border-radius:8px;padding:10px 16px;margin-bottom:16px;font-size:13px;font-weight:600;color:var(--ac)">⏳ Pulling {current_agency}... Auto-refreshing every 10s</div>'
    elif no_data:
        pull_banner = '<div style="background:rgba(220,38,38,.06);border:1px solid var(--rd);border-radius:8px;padding:14px 18px;margin-bottom:16px"><div style="font-size:14px;font-weight:700;color:var(--rd)">📡 No SCPRS data yet</div><div style="font-size:13px;color:var(--tx2);margin-top:4px">Click "Pull All Agencies" to search public SCPRS records for what every agency is buying. Takes 10-15 min. Runs automatically after that.</div></div>'

    return _header("Growth Intel") + f"""
<style>
.card{{background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px}}
th{{padding:7px 12px;font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid var(--bd);white-space:nowrap}}
table{{width:100%;border-collapse:collapse}}
.tab{{padding:6px 14px;border-radius:6px;border:1px solid var(--bd);cursor:pointer;font-size:12px;background:none;color:var(--tx)}}
.tab.active{{background:var(--ac);color:white;border-color:var(--ac)}}
</style>

<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
  <div>
    <h2 style="font-size:22px;font-weight:700">📈 Growth Intelligence</h2>
    <p style="color:var(--tx2);font-size:13px;margin-top:2px">SCPRS-powered gap analysis · What to sell · Who to beat · Why we lost</p>
  </div>
  <div style="display:flex;gap:8px">
    <button onclick="pullAll('P0')" style="padding:7px 16px;background:var(--ac);color:white;border:none;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer">{'⏳ Running...' if running else '📡 Pull All Agencies'}</button>
    <button onclick="pullAll('all')" style="padding:7px 14px;border:1px solid var(--bd);background:none;color:var(--tx);border-radius:6px;font-size:12px;cursor:pointer">Deep Pull (all products)</button>
    <button onclick="runMonitor()" style="padding:7px 14px;border:1px solid var(--bd);background:none;color:var(--tx);border-radius:6px;font-size:12px;cursor:pointer">🔍 Check Lost Quotes</button>
  </div>
</div>

{pull_banner}

<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:20px">
  <div class="card" style="text-align:center">
    <div style="font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Agencies Loaded</div>
    <div style="font-size:28px;font-weight:800;color:var(--ac)">{agencies_loaded}<span style="font-size:14px;color:var(--tx2)">/8</span></div>
  </div>
  <div class="card" style="text-align:center">
    <div style="font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Line Items</div>
    <div style="font-size:28px;font-weight:800;color:var(--tx)">{total_lines:,}</div>
  </div>
  <div class="card" style="text-align:center">
    <div style="font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Gap Spend Found</div>
    <div style="font-size:26px;font-weight:800;color:var(--rd)">${total_gap_spend:,.0f}</div>
    <div style="font-size:10px;color:var(--tx2)">buying from others</div>
  </div>
  <div class="card" style="text-align:center">
    <div style="font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Win-Back</div>
    <div style="font-size:26px;font-weight:800;color:var(--gn)">${total_wb_spend:,.0f}</div>
    <div style="font-size:10px;color:var(--tx2)">we sell, they buy elsewhere</div>
  </div>
  <div class="card" style="text-align:center">
    <div style="font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Quotes Auto-Closed</div>
    <div style="font-size:28px;font-weight:800;color:var(--tx)">{auto_closed}</div>
    <div style="font-size:10px;color:var(--tx2)">lost via PO monitor</div>
  </div>
</div>

<!-- DVBE Insight Banner -->
<div style="background:linear-gradient(135deg,rgba(22,163,74,.08),rgba(37,99,235,.08));border:1px solid rgba(22,163,74,.3);border-radius:10px;padding:14px 20px;margin-bottom:20px;display:flex;align-items:center;gap:16px">
  <div style="font-size:28px">🏅</div>
  <div style="flex:1">
    <div style="font-size:14px;font-weight:700;color:var(--gn)">Your DVBE Cert is a Revenue Engine</div>
    <div style="font-size:12px;color:var(--tx2);margin-top:3px;line-height:1.6">
      CA law mandates that state agencies allocate <strong>3% of spend to DVBE suppliers</strong>. 
      Cardinal Health, McKesson, Grainger — none of them have this cert. When you quote against them, 
      lead with DVBE. Agencies NEED your cert to hit their quota. You can be priced higher and still win.
      <span style="color:var(--ac);font-weight:600"> Also consider approaching these large distributors as their DVBE subcontractor on state bids.</span>
    </div>
  </div>
  <div style="text-align:right;flex-shrink:0">
    <div style="font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px">DVBE Mandate</div>
    <div style="font-size:22px;font-weight:800;color:var(--gn)">3%</div>
    <div style="font-size:11px;color:var(--tx2)">of all state spend</div>
  </div>
</div>

<!-- RECOMMENDATIONS — the heart of it -->
<div style="margin-bottom:24px">
  <div style="font-size:13px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">
    🎯 What To Do Next — Ranked by Revenue Impact
  </div>
  {rec_html if rec_html else '<div class="card" style="text-align:center;padding:32px;color:var(--tx2)">Pull SCPRS data to generate recommendations</div>'}
</div>

<!-- TABS -->
<div style="display:flex;gap:8px;margin-bottom:14px">
  <button class="tab active" onclick="showTab(this,'gaps')">🚨 Gaps ({len(gaps)})</button>
  <button class="tab" onclick="showTab(this,'winback')">⚔️ Win-Back ({len(win_back)})</button>
  <button class="tab" onclick="showTab(this,'losses')">📉 Lost Quotes ({len(losses)})</button>
  <button class="tab" onclick="showTab(this,'coverage')">📡 Agency Coverage</button>
</div>

<div id="tab-gaps" class="card" style="padding:0;overflow-x:auto">
  <table>
    <thead><tr><th>Item CCHCS/Agencies Buy</th><th>Category</th><th>Agencies</th><th style="text-align:center">Orders</th><th style="text-align:right">Avg Price</th><th style="text-align:right">Annual Spend</th></tr></thead>
    <tbody>{gap_rows if gap_rows else '<tr><td colspan="6" style="padding:24px;text-align:center;color:var(--tx2)">Pull data to see gaps →</td></tr>'}</tbody>
  </table>
</div>

<div id="tab-winback" class="card" style="padding:0;overflow-x:auto;display:none">
  <table>
    <thead><tr><th>Item We Sell</th><th>Our SKU</th><th>Current Vendor</th><th>Agencies</th><th style="text-align:right">Their Price</th><th style="text-align:right">Spend</th></tr></thead>
    <tbody>{wb_rows if wb_rows else '<tr><td colspan="6" style="padding:16px;text-align:center;color:var(--tx2)">Pull data to see win-back opportunities</td></tr>'}</tbody>
  </table>
</div>

<div id="tab-losses" class="card" style="padding:0;overflow-x:auto;display:none">
  <div style="padding:10px 14px;font-size:12px;color:var(--tx2);border-bottom:1px solid var(--bd)">
    Auto-detected via SCPRS PO monitor. When SCPRS shows another vendor won a PO you quoted, the quote is automatically closed-lost and their price is saved to your pricing intelligence.
  </div>
  <table>
    <thead><tr><th>Quote</th><th>Agency / Facility</th><th>Who Won</th><th style="text-align:right">Winner Price</th><th style="text-align:right">Our Quote</th><th style="text-align:right">Diff</th></tr></thead>
    <tbody>{loss_rows if loss_rows else '<tr><td colspan="6" style="padding:16px;text-align:center;color:var(--tx2)">No auto-closed quotes yet — run "Check Lost Quotes"</td></tr>'}</tbody>
  </table>
</div>

<div id="tab-coverage" class="card" style="padding:0;overflow-x:auto;display:none">
  <div style="padding:10px 14px;font-size:12px;color:var(--tx2);border-bottom:1px solid var(--bd)">
    Pull schedule — SCPRS data freshness per agency
  </div>
  <table>
    <thead><tr><th>Agency</th><th>Status</th><th>Last Pull</th><th>Frequency</th><th>Action</th></tr></thead>
    <tbody>{ag_rows}</tbody>
  </table>
</div>

<script>
function showTab(btn, name) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  ['gaps','winback','losses','coverage'].forEach(t => {{
    document.getElementById('tab-' + t).style.display = t === name ? '' : 'none';
  }});
}}
function pullAll(priority) {{
  fetch('/api/intel/scprs/pull-all', {{
    method:'POST', headers:{{'Content-Type':'application/json'}},
    credentials:'same-origin', body: JSON.stringify({{priority}})
  }}).then(r=>r.json()).then(()=>{{ startPolling(); }});
}}
function pullAgency(agency) {{
  fetch('/api/cchcs/intel/pull', {{
    method:'POST', headers:{{'Content-Type':'application/json'}},
    credentials:'same-origin', body: JSON.stringify({{priority:'all', agency}})
  }}).then(r=>r.json()).then(()=>{{ startPolling(); }});
}}
function runMonitor() {{
  fetch('/api/intel/scprs/po-monitor', {{
    method:'POST', credentials:'same-origin'
  }}).then(r=>r.json()).then(d=>{{
    alert('Monitor complete: ' + (d.auto_closed_lost||0) + ' quotes auto-closed, ' + (d.matches_found||0) + ' matches found');
    location.reload();
  }});
}}
let pollTimer;
function startPolling() {{
  clearInterval(pollTimer);
  pollTimer = setInterval(()=>{{
    fetch('/api/intel/scprs/engine-status', {{credentials:'same-origin'}})
      .then(r=>r.json()).then(d=>{{
        if (!d.running) {{ clearInterval(pollTimer); location.reload(); }}
      }});
  }}, 10000);
}}
{'startPolling();' if running else ''}
</script>
</div></body></html>"""



# ════════════════════════════════════════════════════════════════════════════════
# UNIVERSAL SCPRS INTELLIGENCE — All agencies, auto-close, price intel
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/intel/scprs/pull", methods=["POST"])
@auth_required
def api_scprs_universal_pull():
    """Trigger full SCPRS pull for all agencies."""
    try:
        from src.agents.scprs_universal_pull import pull_background
        priority = (request.json or {}).get("priority", "P0")
        result = pull_background(priority=priority)
        _push_notification("bell", f"SCPRS universal pull started ({priority})", "info")
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/scprs/status")
@auth_required
def api_scprs_universal_status():
    try:
        from src.agents.scprs_universal_pull import get_pull_status
        return jsonify({"ok": True, **get_pull_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/scprs/intelligence")
@auth_required
def api_scprs_intelligence():
    try:
        from src.agents.scprs_universal_pull import get_universal_intelligence
        agency = request.args.get("agency")
        return jsonify({"ok": True, **get_universal_intelligence(agency_code=agency)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/intel/scprs/close-lost", methods=["POST"])
@auth_required
def api_scprs_check_close_lost():
    """Run quote auto-close check against SCPRS now."""
    try:
        from src.agents.scprs_universal_pull import check_quotes_against_scprs
        result = check_quotes_against_scprs()
        if result["auto_closed"] > 0:
            _push_notification("bell", f"SCPRS: {result['auto_closed']} quotes auto-closed lost", "warn")
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/manager/recommendations")
@auth_required
def api_manager_recommendations():
    """Intelligent action recommendations from manager agent."""
    try:
        from src.agents.manager_agent import get_intelligent_recommendations
        return jsonify(get_intelligent_recommendations())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/intel/scprs")
@auth_required
def page_intel_scprs():
    """Universal SCPRS Intelligence Dashboard — all agencies, all products."""
    import json as _j
    try:
        from src.agents.scprs_universal_pull import get_universal_intelligence, get_pull_status
        intel = get_universal_intelligence()
        status = get_pull_status()
    except Exception as e:
        intel = {"summary": {}, "gap_items": [], "win_back": [], "by_agency": [],
                 "competitors": [], "auto_closed_quotes": []}
        status = {"pos_stored": 0, "lines_stored": 0, "running": False, "progress": ""}

    try:
        from src.agents.manager_agent import get_intelligent_recommendations
        recs = get_intelligent_recommendations()
    except Exception:
        recs = {"actions": [], "summary": {}}

    summary = intel.get("summary", {})
    pos = status.get("pos_stored", 0)
    lines = status.get("lines_stored", 0)
    running = status.get("running", False)
    gap_opp = summary.get("gap_opportunity", 0) or 0
    win_opp = summary.get("win_back_opportunity", 0) or 0
    total_mkt = summary.get("total_market_spend", 0) or 0
    auto_closed = len(intel.get("auto_closed_quotes", []))

    no_data = pos == 0

    # Build recommendation cards
    rec_cards = ""
    urgency_color = {"RIGHT NOW": "var(--rd)", "THIS WEEK": "#D97706", "NEXT 30 DAYS": "var(--ac)"}
    type_icon = {
        "collect_ar": "💰", "follow_up_quote": "📞", "add_product": "📦",
        "displace_competitor": "⚔️", "expand_existing_customer": "🏥",
        "reprice_analysis": "📊", "pull_data": "📡",
    }
    for action in recs.get("actions", [])[:12]:
        icon = type_icon.get(action.get("type",""), "•")
        urg = action.get("urgency", "")
        urg_col = urgency_color.get(urg, "var(--tx2)")
        dv = action.get("dollar_value", 0) or 0
        rec_cards += f"""
<div style="border:1px solid var(--bd);border-radius:8px;padding:14px 16px;background:var(--bg2)">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px">
    <div style="font-size:14px;font-weight:700">{icon} {action.get("title","")}</div>
    <div style="display:flex;gap:6px;align-items:center;flex-shrink:0;margin-left:12px">
      <span style="font-size:11px;font-weight:700;color:{urg_col}">{urg}</span>
      {f'<span style="font-size:13px;font-weight:700;color:var(--gn)">${dv:,.0f}</span>' if dv > 0 else ""}
    </div>
  </div>
  <div style="font-size:12px;color:var(--tx2);margin-bottom:8px;line-height:1.5">{action.get("why","")[:180]}</div>
  <div style="font-size:12px;background:rgba(37,99,235,.07);border-radius:4px;padding:6px 10px;color:var(--ac);font-weight:500">
    → {action.get("action","")[:200]}
  </div>
</div>"""

    # Gap items table
    gap_rows = ""
    for item in intel.get("gap_items", [])[:20]:
        spend = item.get("total_spend") or 0
        agencies_ct = item.get("agencies_buying", 1) or 1
        gap_rows += f"""<tr>
  <td style="padding:7px 10px;font-size:12px">{item.get("description","")[:55]}</td>
  <td style="padding:7px 10px;font-size:11px;color:var(--tx2)">{(item.get("category") or "").replace("_"," ").title()}</td>
  <td style="padding:7px 10px;font-size:11px;text-align:center">{agencies_ct}</td>
  <td style="padding:7px 10px;font-size:11px;text-align:center">{item.get("times_ordered",0)}</td>
  <td style="padding:7px 10px;font-size:12px;text-align:right;font-weight:600;color:var(--rd)">${spend:,.0f}</td>
  <td style="padding:7px 10px"><span style="background:rgba(220,38,38,.1);color:var(--rd);padding:2px 7px;border-radius:3px;font-size:10px;font-weight:700">ADD PRODUCT</span></td>
</tr>"""

    # Win-back table
    wb_rows = ""
    for item in intel.get("win_back", [])[:15]:
        spend = item.get("total_spend") or 0
        their_price = item.get("their_price") or 0
        beat_price = their_price * 0.96 if their_price else 0
        wb_rows += f"""<tr>
  <td style="padding:7px 10px;font-size:12px">{item.get("description","")[:50]}</td>
  <td style="padding:7px 10px;font-size:12px;color:var(--rd)">{item.get("incumbent_vendor","")[:30]}</td>
  <td style="padding:7px 10px;font-size:11px;text-align:right">${their_price:.2f}</td>
  <td style="padding:7px 10px;font-size:11px;text-align:right;color:var(--gn);font-weight:600">${beat_price:.2f}</td>
  <td style="padding:7px 10px;font-size:12px;text-align:right;font-weight:700;color:var(--gn)">${spend:,.0f}</td>
</tr>"""

    # Agency breakdown
    agency_rows = ""
    for ag in intel.get("by_agency", [])[:10]:
        gap = ag.get("gap_spend") or 0
        we_sell = ag.get("we_sell_spend") or 0
        total = ag.get("total_spend") or 0
        pct_gap = int(gap / total * 100) if total > 0 else 0
        agency_rows += f"""<tr>
  <td style="padding:7px 10px;font-size:12px;font-weight:600">{ag.get("dept_name","")[:35]}</td>
  <td style="padding:7px 10px;font-size:12px;text-align:right">${total:,.0f}</td>
  <td style="padding:7px 10px;font-size:12px;text-align:right;color:var(--gn)">${we_sell:,.0f}</td>
  <td style="padding:7px 10px;font-size:12px;text-align:right;color:var(--rd)">${gap:,.0f}</td>
  <td style="padding:7px 10px">
    <div style="background:var(--bd);border-radius:2px;height:6px;width:100%">
      <div style="background:var(--rd);border-radius:2px;height:6px;width:{pct_gap}%"></div>
    </div>
  </td>
</tr>"""

    # Auto-closed quotes
    ac_rows = ""
    for q in intel.get("auto_closed_quotes", [])[:5]:
        ac_rows += f"""<div style="padding:8px 12px;border-bottom:1px solid var(--bd);font-size:12px">
  <span style="font-weight:600">{q.get("quote_number","")}</span>
  <span style="color:var(--tx2);margin:0 8px">{q.get("agency","")}</span>
  <span style="color:var(--rd)">{(q.get("status_notes",""))[:100]}</span>
</div>"""

    rec_summary = recs.get("summary", {})

    return _header("SCPRS Intelligence") + f"""
<style>
.card{{background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px}}
th{{padding:7px 10px;font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid var(--bd);text-align:left}}
table{{width:100%;border-collapse:collapse}}
tr:hover td{{background:rgba(255,255,255,.03)}}
</style>

<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
  <div>
    <h2 style="font-size:22px;font-weight:700">📡 SCPRS Market Intelligence</h2>
    <p style="color:var(--tx2);font-size:13px;margin-top:4px">All CA agencies · What they buy · Who from · What you're missing · What to do about it</p>
  </div>
  <div style="display:flex;gap:8px">
    <button onclick="triggerPull('P0')" style="padding:7px 16px;background:var(--ac);color:white;border:none;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer">
      {"⏳ Running..." if running else "📡 Pull P0 Now (5min)"}
    </button>
    <button onclick="triggerPull('all')" style="padding:7px 14px;border:1px solid var(--bd);background:none;color:var(--tx);border-radius:6px;font-size:12px;cursor:pointer">Full Pull (all 40 terms)</button>
    <button onclick="runCloseLost()" style="padding:7px 14px;border:1px solid var(--bd);background:none;color:var(--tx);border-radius:6px;font-size:12px;cursor:pointer">⚡ Check Lost Quotes</button>
  </div>
</div>

{"<div style='background:rgba(37,99,235,.08);border:1px solid var(--ac);border-radius:10px;padding:16px 20px;margin-bottom:20px;display:flex;align-items:center;gap:14px'><div style='font-size:28px'>📡</div><div><div style='font-weight:700;color:var(--ac)'>No data yet — click Pull P0 Now to start</div><div style='font-size:12px;color:var(--tx2);margin-top:4px'>Searches SCPRS public records for nitrile gloves, adult briefs, N95s, chux, first aid kits across CCHCS, CalVet, CalFire, CDPH, CalTrans, CHP. Takes ~5 minutes. No login required — it&#39;s public data.</div></div></div>" if no_data else ""}

<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:24px">
  <div class="card"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px">POs Captured</div>
    <div style="font-size:26px;font-weight:800;color:var(--ac);margin-top:4px">{pos:,}</div>
    <div style="font-size:11px;color:var(--tx2)">{lines:,} line items</div></div>
  <div class="card"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px">Market Spend</div>
    <div style="font-size:26px;font-weight:800;margin-top:4px">${total_mkt:,.0f}</div>
    <div style="font-size:11px;color:var(--tx2)">captured from SCPRS</div></div>
  <div class="card"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px">Gap Items</div>
    <div style="font-size:26px;font-weight:800;color:var(--rd);margin-top:4px">${gap_opp:,.0f}</div>
    <div style="font-size:11px;color:var(--rd)">products we don't sell</div></div>
  <div class="card"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px">Win-Back</div>
    <div style="font-size:26px;font-weight:800;color:var(--gn);margin-top:4px">${win_opp:,.0f}</div>
    <div style="font-size:11px;color:var(--gn)">displace competitors</div></div>
  <div class="card"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px">Auto-Closed</div>
    <div style="font-size:26px;font-weight:800;color:var(--tx2);margin-top:4px">{auto_closed}</div>
    <div style="font-size:11px;color:var(--tx2)">quotes closed-lost by SCPRS</div></div>
</div>

{"<div style='background:rgba(22,163,74,.07);border:1px solid var(--gn);border-radius:8px;padding:10px 14px;margin-bottom:16px;font-size:13px;color:var(--gn);font-weight:600'>⏳ Pull running... &nbsp;<span style='font-weight:400;color:var(--tx2)'>" + (status.get("progress","")) + "</span></div>" if running else ""}

<div style="display:grid;grid-template-columns:420px 1fr;gap:20px;margin-bottom:20px">
  <div>
    <div style="font-size:11px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">🎯 What To Do Next <span style="color:var(--tx2);font-weight:400">— {len(recs.get("actions",[]))} actions · ${rec_summary.get("revenue_opportunity",0):,.0f} opportunity</span></div>
    <div style="display:flex;flex-direction:column;gap:10px;max-height:640px;overflow-y:auto;padding-right:4px">
      {rec_cards if rec_cards else '<div style="padding:20px;text-align:center;color:var(--tx2);font-size:13px">Pull SCPRS data to generate recommendations</div>'}
    </div>
  </div>
  <div>
    <div style="font-size:11px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">⚔️ Win-Back — We sell these, they buy from competitors</div>
    <div class="card" style="padding:0;margin-bottom:16px">
      <table>
        <thead><tr><th>Item</th><th>Their Vendor</th><th style="text-align:right">Their Price</th><th style="text-align:right">Beat At</th><th style="text-align:right">Spend</th></tr></thead>
        <tbody>{wb_rows if wb_rows else '<tr><td colspan="5" style="padding:16px;text-align:center;color:var(--tx2)">Pull data →</td></tr>'}</tbody>
      </table>
    </div>
    <div style="font-size:11px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">📊 By Agency</div>
    <div class="card" style="padding:0">
      <table>
        <thead><tr><th>Agency</th><th style="text-align:right">Total Spend</th><th style="text-align:right">We Sell</th><th style="text-align:right">Gap</th><th>% Gap</th></tr></thead>
        <tbody>{agency_rows if agency_rows else '<tr><td colspan="5" style="padding:16px;text-align:center;color:var(--tx2)">Pull data →</td></tr>'}</tbody>
      </table>
    </div>
  </div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px">
  <div>
    <div style="font-size:11px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">🚨 Gap Items — They buy these, you don't carry them yet</div>
    <div class="card" style="padding:0">
      <table>
        <thead><tr><th>Item Description</th><th>Category</th><th style="text-align:center">Agencies</th><th style="text-align:center">Orders</th><th style="text-align:right">Spend</th><th>Action</th></tr></thead>
        <tbody>{gap_rows if gap_rows else '<tr><td colspan="6" style="padding:16px;text-align:center;color:var(--tx2)">Pull data →</td></tr>'}</tbody>
      </table>
    </div>
  </div>
  <div>
    <div style="font-size:11px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">🔴 Auto-Closed Quotes (SCPRS found winner)</div>
    <div class="card" style="padding:0;margin-bottom:16px">
      {ac_rows if ac_rows else '<div style="padding:16px;text-align:center;color:var(--tx2);font-size:12px">No auto-closes yet — quotes are checked after each SCPRS pull</div>'}
    </div>
  </div>
</div>

<script>
let pollTimer;
function triggerPull(priority) {{
  fetch('/api/intel/scprs/pull', {{
    method:'POST', headers:{{'Content-Type':'application/json'}},
    credentials:'same-origin', body: JSON.stringify({{priority}})
  }}).then(r=>r.json()).then(d=>{{ if(d.ok) pollStatus(); }});
}}
function runCloseLost() {{
  fetch('/api/intel/scprs/close-lost', {{method:'POST',credentials:'same-origin'}})
    .then(r=>r.json()).then(d=>{{ alert(d.auto_closed+' quotes auto-closed lost'); location.reload(); }});
}}
function pollStatus() {{
  clearTimeout(pollTimer);
  fetch('/api/intel/scprs/status',{{credentials:'same-origin'}})
    .then(r=>r.json()).then(d=>{{
      if(d.running) {{ pollTimer=setTimeout(pollStatus,6000); }}
      else if(d.pos_stored > 0) {{ location.reload(); }}
    }});
}}
{"pollStatus();" if running else ""}
</script>
</div></body></html>"""


# ════════════════════════════════════════════════════════════════════════════════
# CCHCS PURCHASING INTELLIGENCE — What are they buying? Who from? At what price?
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/cchcs/intel/pull", methods=["POST"])
@auth_required
def api_cchcs_intel_pull():
    """Trigger CCHCS SCPRS purchasing data pull in background."""
    try:
        from src.agents.cchcs_intel_puller import pull_in_background
        priority = request.json.get("priority", "P0") if request.is_json else "P0"
        result = pull_in_background(priority=priority)
        _push_notification("bell", f"CCHCS intel pull started (priority={priority})", "info")
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cchcs/intel/status")
@auth_required
def api_cchcs_intel_status():
    """Check CCHCS intel pull status and DB record counts."""
    try:
        from src.agents.cchcs_intel_puller import get_pull_status, _pull_status
        status = get_pull_status()
        status["pull_running"] = _pull_status.get("running", False)
        status["last_result"] = _pull_status.get("last_result")
        return jsonify({"ok": True, **status})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cchcs/intel/data")
@auth_required
def api_cchcs_intel_data():
    """Full CCHCS purchasing intelligence: gaps, win-backs, suppliers, facilities."""
    try:
        from src.agents.cchcs_intel_puller import get_cchcs_intelligence
        intel = get_cchcs_intelligence()
        return jsonify({"ok": True, **intel})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/cchcs/intel")
@auth_required
def page_cchcs_intel():
    """CCHCS Purchasing Intelligence Dashboard."""
    from src.agents.cchcs_intel_puller import get_cchcs_intelligence, get_pull_status, _pull_status
    import json as _j

    try:
        intel = get_cchcs_intelligence()
        status = get_pull_status()
    except Exception as e:
        intel = {"summary": {}, "gap_items": [], "win_back_items": [],
                 "by_category": [], "suppliers": [], "facilities": [], "top_items": []}
        status = {"pos_stored": 0, "lines_stored": 0}

    pull_running = _pull_status.get("running", False)
    pos_stored = status.get("pos_stored", 0)
    lines_stored = status.get("lines_stored", 0)
    summary = intel.get("summary", {})
    gap_spend = summary.get("gap_spend_not_selling", 0) or 0
    win_back = summary.get("win_back_spend", 0) or 0
    total_captured = summary.get("total_po_value_captured", 0) or 0

    # Build gap items table
    gap_rows = ""
    for item in intel.get("gap_items", [])[:25]:
        spend = item.get("total_spend") or 0
        cat = (item.get("category") or "").replace("_", " ").title()
        gap_rows += f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:8px 12px;font-size:13px">{item.get("description","")[:60]}</td>
  <td style="padding:8px 12px;font-size:12px;color:var(--tx2)">{cat}</td>
  <td style="padding:8px 12px;font-size:12px;text-align:center">{item.get("times_purchased",0)}</td>
  <td style="padding:8px 12px;font-size:12px;text-align:right">${item.get("avg_price") or 0:.2f}</td>
  <td style="padding:8px 12px;font-size:13px;text-align:right;font-weight:700;color:var(--rd)">${spend:,.0f}</td>
  <td style="padding:8px 12px"><span style="background:rgba(220,38,38,.1);color:var(--rd);padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600">MISSING</span></td>
</tr>"""

    # Build win-back table
    wb_rows = ""
    for item in intel.get("win_back_items", [])[:15]:
        spend = item.get("total_spend") or 0
        wb_rows += f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:8px 12px;font-size:13px">{item.get("description","")[:60]}</td>
  <td style="padding:8px 12px;font-size:12px;color:var(--tx2)">{(item.get("category") or "").replace("_"," ").title()}</td>
  <td style="padding:8px 12px;font-size:12px;font-weight:600;color:var(--ac)">{item.get("reytech_sku","—")}</td>
  <td style="padding:8px 12px;font-size:12px;color:var(--tx2)">{item.get("supplier","Unknown")[:30]}</td>
  <td style="padding:8px 12px;font-size:12px;text-align:right">${item.get("avg_price") or 0:.2f}</td>
  <td style="padding:8px 12px;font-size:13px;text-align:right;font-weight:700;color:var(--gn)">${spend:,.0f}</td>
</tr>"""

    # Supplier table
    sup_rows = ""
    for s in intel.get("suppliers", [])[:15]:
        cats_raw = s.get("categories", "[]")
        try: cats = ", ".join(_j.loads(cats_raw))[:50]
        except: cats = str(cats_raw)[:50]
        is_comp = s.get("is_competitor", 0)
        supplier_lower = (s.get("supplier_name") or "").lower()
        # Import check — is this a known non-DVBE we can displace?
        from src.agents.scprs_intelligence_engine import KNOWN_NON_DVBE_INCUMBENTS, DVBE_PARTNER_TARGETS
        is_dvbe_target = any(inc in supplier_lower for inc in KNOWN_NON_DVBE_INCUMBENTS)
        is_partner = any(p in supplier_lower for p in DVBE_PARTNER_TARGETS)
        action_cell = ""
        if is_dvbe_target:
            if is_partner:
                action_cell = '<span style="color:var(--rd);font-size:11px;font-weight:600">⚔️ Displace</span> · <span style="color:var(--ac);font-size:11px;font-weight:600">🤝 Partner</span>'
            else:
                action_cell = '<span style="color:var(--rd);font-size:11px;font-weight:600">⚔️ Displace (DVBE)</span>'
        sup_rows += f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:8px 12px;font-size:13px;font-weight:{'600' if is_dvbe_target else '400'};color:{'var(--rd)' if is_dvbe_target else 'var(--tx)'}">{s.get("supplier_name","")[:40]}{' 🏅' if is_dvbe_target else ''}</td>
  <td style="padding:8px 12px;font-size:12px;text-align:center">{s.get("po_count",0)}</td>
  <td style="padding:8px 12px;font-size:13px;text-align:right;font-weight:700">${(s.get("total_po_value") or 0):,.0f}</td>
  <td style="padding:8px 12px;font-size:11px;color:var(--tx2)">{cats}</td>
  <td style="padding:8px 12px;font-size:11px">{action_cell if action_cell else '—'}</td>
</tr>"""

    # Category chart data
    cat_labels = _j.dumps([r.get("category","").replace("_"," ").title() for r in intel.get("by_category",[])])
    cat_values = _j.dumps([round(r.get("total_spend") or 0) for r in intel.get("by_category",[])])
    cat_colors = _j.dumps(["#DC2626" if r.get("gap_spend",0) > r.get("reytech_sells_spend",0) else "#16A34A"
                           for r in intel.get("by_category",[])])

    no_data_msg = ""
    if pos_stored == 0:
        no_data_msg = f"""
<div style="background:rgba(37,99,235,.08);border:1px solid var(--ac);border-radius:10px;padding:20px 24px;margin-bottom:24px;display:flex;align-items:center;gap:16px">
  <div style="font-size:32px">📡</div>
  <div>
    <div style="font-size:15px;font-weight:700;color:var(--ac)">No data yet — Pull Required</div>
    <div style="font-size:13px;color:var(--tx2);margin-top:4px">Click "Pull CCHCS Data Now" to search SCPRS for all CDCR/CCHCS purchase orders. Takes 3-5 minutes. Runs on Railway — no login needed, it's public data.</div>
  </div>
</div>"""

    return _header("CCHCS Intel") + f"""
<style>
.card{{background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px}}
th{{padding:8px 12px;font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;text-align:left;border-bottom:2px solid var(--bd)}}
table{{width:100%;border-collapse:collapse}}
.stat{{display:flex;flex-direction:column;gap:4px}}
.stat .label{{font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px}}
.stat .value{{font-size:28px;font-weight:800}}
</style>

<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
  <div>
    <h2 style="font-size:22px;font-weight:700">🔬 CCHCS Purchasing Intelligence</h2>
    <p style="color:var(--tx2);font-size:13px;margin-top:4px">What is CDCR/CCHCS buying · From whom · At what price · What you're missing</p>
  </div>
  <div style="display:flex;gap:8px;align-items:center">
    {'<div style="background:rgba(22,163,74,.1);color:var(--gn);border:1px solid var(--gn);padding:5px 14px;border-radius:6px;font-size:12px;font-weight:600">⏳ Pull Running...</div>' if pull_running else ''}
    <button onclick="startPull('P0')" style="padding:6px 16px;background:var(--ac);color:white;border:none;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer">
      {'⏳ Pulling...' if pull_running else '📡 Pull CCHCS Data Now'}
    </button>
    <button onclick="startPull('all')" style="padding:6px 14px;border:1px solid var(--bd);background:none;color:var(--tx);border-radius:6px;font-size:12px;cursor:pointer">Full Pull (all categories)</button>
    <a href="/" style="padding:5px 12px;border:1px solid var(--bd);border-radius:6px;font-size:12px;text-decoration:none">🏠</a>
  </div>
</div>

{no_data_msg}

<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:14px;margin-bottom:24px">
  <div class="card"><div class="stat">
    <div class="label">POs Captured</div>
    <div class="value" style="color:var(--ac)">{pos_stored:,}</div>
    <div style="font-size:11px;color:var(--tx2)">{lines_stored:,} line items</div>
  </div></div>
  <div class="card"><div class="stat">
    <div class="label">Total PO Value</div>
    <div class="value" style="color:var(--tx)">${total_captured:,.0f}</div>
    <div style="font-size:11px;color:var(--tx2)">spend captured</div>
  </div></div>
  <div class="card"><div class="stat">
    <div class="label">Gap Spend</div>
    <div class="value" style="color:var(--rd)">${gap_spend:,.0f}</div>
    <div style="font-size:11px;color:var(--rd)">buying from others</div>
  </div></div>
  <div class="card"><div class="stat">
    <div class="label">Win-Back</div>
    <div class="value" style="color:var(--gn)">${win_back:,.0f}</div>
    <div style="font-size:11px;color:var(--gn)">items we sell</div>
  </div></div>
  <div class="card"><div class="stat">
    <div class="label">Last Pull</div>
    <div class="value" style="font-size:16px;color:var(--tx2)">{"Live" if pull_running else (summary.get("data_freshness","Never")[:10] if summary.get("data_freshness") else "Never")}</div>
    <div style="font-size:11px;color:var(--tx2)">SCPRS public data</div>
  </div></div>
</div>

<div style="display:grid;grid-template-columns:1fr 340px;gap:20px;margin-bottom:24px">
  <div>
    <div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">
      🚨 GAP ITEMS — CCHCS buys these but Reytech doesn't sell them
    </div>
    <div class="card" style="padding:0">
      <table>
        <thead><tr>
          <th>Item Description</th><th>Category</th><th style="text-align:center">Orders</th>
          <th style="text-align:right">Avg Price</th><th style="text-align:right">Total Spend</th><th>Status</th>
        </tr></thead>
        <tbody>{gap_rows if gap_rows else '<tr><td colspan="6" style="padding:24px;text-align:center;color:var(--tx2);font-size:13px">Pull CCHCS data to see gap items →</td></tr>'}</tbody>
      </table>
    </div>
  </div>
  <div>
    <div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Spend by Category</div>
    <div class="card"><canvas id="catChart" height="300"></canvas></div>
  </div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px">
  <div>
    <div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">
      ✅ WIN-BACK ITEMS — We sell these, but they're buying from someone else
    </div>
    <div class="card" style="padding:0">
      <table>
        <thead><tr>
          <th>Item</th><th>Category</th><th>Our SKU</th><th>Their Vendor</th>
          <th style="text-align:right">Their Price</th><th style="text-align:right">Spend</th>
        </tr></thead>
        <tbody>{wb_rows if wb_rows else '<tr><td colspan="6" style="padding:16px;text-align:center;color:var(--tx2)">Pull data to see win-back opportunities</td></tr>'}</tbody>
      </table>
    </div>
  </div>
  <div>
    <div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">
      ⚔️ Incumbent Suppliers (Your Competition at CCHCS)
    </div>
    <div class="card" style="padding:0">
      <table>
        <thead><tr><th>Supplier</th><th style="text-align:center">POs</th><th style="text-align:right">Total $</th><th>Categories</th><th>Action</th></tr></thead>
        <tbody>{sup_rows if sup_rows else '<tr><td colspan="5" style="padding:16px;text-align:center;color:var(--tx2)">Pull data to see suppliers</td></tr>'}</tbody>
      </table>
    </div>
  </div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<script>
const catLabels = {cat_labels};
const catValues = {cat_values};
const catColors = {cat_colors};

if (catLabels.length > 0) {{
  new Chart(document.getElementById('catChart'), {{
    type: 'bar',
    data: {{
      labels: catLabels,
      datasets: [{{ label: 'Spend ($)', data: catValues, backgroundColor: catColors, borderRadius: 4 }}]
    }},
    options: {{
      indexAxis: 'y',
      plugins: {{ legend: {{ display: false }} }},
      scales: {{ x: {{ ticks: {{ callback: v => '$' + (v/1000).toFixed(0) + 'K' }} }},
                 y: {{ ticks: {{ font: {{ size: 10 }} }} }} }},
      responsive: true, maintainAspectRatio: false
    }}
  }});
}}

let pollTimer = null;

function startPull(priority) {{
  fetch('/api/cchcs/intel/pull', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    credentials: 'same-origin',
    body: JSON.stringify({{priority}})
  }}).then(r => r.json()).then(d => {{
    if (d.ok) {{
      console.log('Pull started');
      pollStatus();
    }}
  }});
}}

function pollStatus() {{
  clearTimeout(pollTimer);
  fetch('/api/cchcs/intel/status', {{credentials:'same-origin'}})
    .then(r => r.json()).then(d => {{
      if (d.pull_running) {{
        pollTimer = setTimeout(pollStatus, 8000);
      }} else if (d.pos_stored > 0) {{
        location.reload();
      }}
    }});
}}

// Auto-poll if pull is running
{f"pollStatus();" if pull_running else ""}
</script>
</div></body></html>"""

# ════════════════════════════════════════════════════════════════════════════════
# VENDOR ORDERING ROUTES
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/vendors")
@auth_required
def page_vendors():
    """Vendor management — API status, enriched list, ordering capabilities."""
    from src.agents.vendor_ordering_agent import get_enriched_vendor_list, get_agent_status as _voas, get_vendor_orders
    vendors = get_enriched_vendor_list()
    vs = _voas()
    recent_orders = get_vendor_orders(limit=20)
    
    active = [v for v in vendors if v.get("can_order")]
    email_po = [v for v in vendors if v.get("integration_status") == "email_po"]
    setup_needed = [v for v in vendors if v.get("integration_status") == "setup_needed"]
    manual = [v for v in vendors if v.get("integration_status") == "manual_only"]

    STATUS_BADGE = {
        "active": ("<span style='color:var(--gn);font-size:11px;font-weight:600'>● ACTIVE</span>", "var(--gn)"),
        "email_po": ("<span style='color:var(--ac);font-size:11px;font-weight:600'>✉ EMAIL PO</span>", "var(--ac)"),
        "setup_needed": ("<span style='color:var(--yl);font-size:11px;font-weight:600'>⚙ SETUP</span>", "var(--yl)"),
        "ready": ("<span style='color:var(--or);font-size:11px;font-weight:600'>◑ PARTIAL</span>", "var(--or)"),
        "manual_only": ("<span style='color:var(--tx2);font-size:11px'>— MANUAL</span>", "var(--tx2)"),
    }
    
    def vendor_row(v):
        name = v.get("name","")
        status = v.get("integration_status","manual_only")
        badge_html, color = STATUS_BADGE.get(status, STATUS_BADGE["manual_only"])
        email = v.get("email","") or v.get("contact_email","")
        phone = v.get("phone","")
        balance = v.get("open_balance","")
        cats = ", ".join(v.get("categories",[])[:3]) or "—"
        note = v.get("note","") or v.get("action","")
        return f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:10px 12px;font-weight:500;color:{color}">{name}</td>
  <td style="padding:10px 12px;font-size:12px">{badge_html}</td>
  <td style="padding:10px 12px;font-size:11px;color:var(--tx2)">{cats}</td>
  <td style="padding:10px 12px;font-size:11px;color:var(--ac)">{email}</td>
  <td style="padding:10px 12px;font-size:11px;color:var(--tx2)">{phone}</td>
  <td style="padding:10px 12px;font-size:11px;color:var(--yl)">{f"${float(balance):,.2f}" if balance else ""}</td>
  <td style="padding:10px 12px;font-size:11px;color:var(--tx2);max-width:200px">{note[:80] if note else ""}</td>
</tr>"""
    
    # Priority vendors first
    priority_vendors = [v for v in vendors if v.get("integration_status") in ("active","email_po","setup_needed","ready")]
    other_vendors = [v for v in vendors if v.get("integration_status") == "manual_only"]
    all_rows = "".join(vendor_row(v) for v in priority_vendors + other_vendors)
    
    # Recent orders
    orders_html = ""
    if recent_orders:
        for o in recent_orders[:10]:
            ts = (o.get("submitted_at","")[:16] or "").replace("T"," ")
            status_color = {"submitted":"var(--ac)","confirmed":"var(--gn)","shipped":"var(--yl)","failed":"var(--rd)"}.get(o.get("status",""),("var(--tx2)"))
            orders_html += f"""<tr>
  <td style="padding:8px 12px;font-size:12px">{ts}</td>
  <td style="padding:8px 12px;font-size:12px;font-weight:500">{o.get("vendor_name","")}</td>
  <td style="padding:8px 12px;font-size:12px;font-family:'JetBrains Mono',monospace">{o.get("po_number","")}</td>
  <td style="padding:8px 12px;font-size:12px">{o.get("quote_number","")}</td>
  <td style="padding:8px 12px;font-size:12px">${o.get("total",0):,.2f}</td>
  <td style="padding:8px 12px;font-size:12px;color:{status_color}">{o.get("status","").upper()}</td>
</tr>"""
    else:
        orders_html = '<tr><td colspan="6" style="padding:20px;text-align:center;color:var(--tx2)">No vendor orders yet — orders appear here when quotes are won</td></tr>'

    html = _header("Vendors") + f"""
<style>
.btn{{padding:5px 12px;border:1px solid var(--bd);border-radius:6px;cursor:pointer;font-family:'DM Sans',sans-serif;transition:.15s;text-decoration:none;font-size:12px;font-weight:500}}
.btn:hover{{opacity:.8}}
table{{width:100%;border-collapse:collapse}}
th{{padding:8px 12px;font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;text-align:left;border-bottom:1px solid var(--bd)}}
</style>

<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
  <div>
    <h2 style="font-size:22px;font-weight:700">🏭 Vendor Management</h2>
    <p style="color:var(--tx2);font-size:13px;margin-top:4px">{len(vendors)} vendors · {len(active)+len(email_po)} API-ready · {len(setup_needed)} need setup</p>
  </div>
  <div style="display:flex;gap:8px">
    <a href="/" class="btn">🏠 Home</a>
    <a href="/api/vendor/status" class="btn" target="_blank">⚙️ API Status</a>
    <button class="btn" onclick="testGrainger(this)" style="border-color:var(--ac);color:var(--ac)">🔍 Test Grainger Search</button>
  </div>
</div>

<!-- Setup guide cards -->
<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:24px">
  <div class="card" style="border-color:{('var(--gn)' if vs.get('grainger_can_order') else 'var(--yl)')}">
    <div style="font-size:11px;color:var(--tx2);margin-bottom:8px">GRAINGER REST API</div>
    <div style="font-size:20px;font-weight:700;color:{('var(--gn)' if vs.get('grainger_can_order') else 'var(--yl)')}">
      {'✅ Active' if vs.get('grainger_can_order') else '⚙ Setup Needed'}
    </div>
    <div style="font-size:11px;color:var(--tx2);margin-top:6px">Free public API · industrial + medical</div>
    {'<div style="font-size:11px;color:var(--yl);margin-top:8px">→ Set GRAINGER_CLIENT_ID/SECRET/ACCOUNT_NUMBER in Railway</div>' if not vs.get('grainger_can_order') else ''}
  </div>
  <div class="card" style="border-color:{('var(--gn)' if vs.get('amazon_configured') else 'var(--yl)')}">
    <div style="font-size:11px;color:var(--tx2);margin-bottom:8px">AMAZON BUSINESS SP-API</div>
    <div style="font-size:20px;font-weight:700;color:{('var(--gn)' if vs.get('amazon_configured') else 'var(--yl)')}">
      {'✅ Active' if vs.get('amazon_configured') else '⚙ Setup Needed'}
    </div>
    <div style="font-size:11px;color:var(--tx2);margin-top:6px">Search via SerpApi ✅ · ordering via SP-API</div>
    {'<div style="font-size:11px;color:var(--yl);margin-top:8px">→ Set AMZN_ACCESS_KEY/SECRET/REFRESH_TOKEN in Railway</div>' if not vs.get('amazon_configured') else ''}
  </div>
  <div class="card" style="border-color:{('var(--gn)' if vs.get('email_po_active') else 'var(--bd)')}">
    <div style="font-size:11px;color:var(--tx2);margin-bottom:8px">EMAIL PO VENDORS</div>
    <div style="font-size:20px;font-weight:700;color:var(--gn)">
      {len(vs.get('email_po_vendors',[]))} Active
    </div>
    <div style="font-size:11px;color:var(--tx2);margin-top:6px">Curbell · IMS · Echelon · TSI</div>
    <div style="font-size:11px;color:var(--gn);margin-top:6px">POs sent automatically on quote won</div>
  </div>
</div>

<!-- Vendor table -->
<div class="card" style="margin-bottom:20px">
  <div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:14px">
    All Vendors ({len(vendors)})
  </div>
  <div style="overflow-x:auto">
    <table>
      <thead><tr>
        <th>Vendor</th><th>Integration</th><th>Categories</th>
        <th>Email</th><th>Phone</th><th>Balance</th><th>Notes</th>
      </tr></thead>
      <tbody>{all_rows}</tbody>
    </table>
  </div>
</div>

<!-- Recent vendor orders -->
<div class="card">
  <div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:14px">
    Vendor Order History
  </div>
  <table>
    <thead><tr>
      <th>Date</th><th>Vendor</th><th>PO Number</th><th>Quote</th><th>Total</th><th>Status</th>
    </tr></thead>
    <tbody>{orders_html}</tbody>
  </table>
</div>

<div id="grainger-results" style="margin-top:16px"></div>

<script>
function testGrainger(btn){{
  var q=prompt("Search Grainger catalog (e.g. 'nitrile gloves medium 100 box'):");
  if(!q)return;
  btn.disabled=true;btn.textContent='Searching...';
  fetch('/api/vendor/search?vendor=grainger&q='+encodeURIComponent(q),{{credentials:'same-origin'}})
  .then(r=>r.json()).then(d=>{{
    btn.disabled=false;btn.textContent='🔍 Test Grainger Search';
    var el=document.getElementById('grainger-results');
    if(!d.results||!d.results.length){{el.innerHTML='<p style="color:var(--yl)">No results (configure GRAINGER_CLIENT_ID/SECRET for full access)</p>';return;}}
    var rows=d.results.map(r=>'<tr><td style="padding:6px 10px">'+r.item_number+'</td><td style="padding:6px 10px">'+r.title.substring(0,60)+'</td><td style="padding:6px 10px;color:var(--gn)">$'+(r.price||0).toFixed(2)+'</td><td style="padding:6px 10px;color:var(--tx2)">'+r.availability+'</td></tr>').join('');
    el.innerHTML='<div class="card"><div style="font-size:12px;font-weight:600;color:var(--tx2);margin-bottom:10px">Grainger Results: '+d.results.length+' found</div><table><thead><tr><th>Item#</th><th>Title</th><th>Price</th><th>Availability</th></tr></thead><tbody>'+rows+'</tbody></table></div>';
  }}).catch(()=>{{btn.disabled=false;btn.textContent='🔍 Test Grainger Search';alert('Search failed')}});
}}
</script>
</div></body></html>"""
    return html


@bp.route("/api/vendor/status")
@auth_required
def api_vendor_status():
    """Vendor ordering agent status + setup guide."""
    from src.agents.vendor_ordering_agent import get_agent_status as _voas
    return jsonify({"ok": True, **_voas()})


@bp.route("/api/vendor/search")
@auth_required
def api_vendor_search():
    """Search a vendor catalog.
    ?vendor=grainger&q=nitrile+gloves
    """
    vendor = request.args.get("vendor", "grainger")
    q = request.args.get("q", "")
    if not q:
        return jsonify({"ok": False, "error": "q required"})
    try:
        from src.agents.vendor_ordering_agent import grainger_search, amazon_search_catalog, compare_vendor_prices
        if vendor == "grainger":
            results = grainger_search(q, max_results=10)
        elif vendor == "amazon":
            results = amazon_search_catalog(q, max_results=10)
        elif vendor == "compare":
            qty = int(request.args.get("qty", 1))
            return jsonify(compare_vendor_prices(q, qty))
        else:
            return jsonify({"ok": False, "error": f"Unknown vendor: {vendor}"})
        return jsonify({"ok": True, "vendor": vendor, "query": q, "count": len(results), "results": results})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/vendor/compare")
@auth_required
def api_vendor_compare():
    """Compare prices across all vendors for a product.
    ?q=nitrile+gloves+medium&qty=10
    """
    q = request.args.get("q", "")
    qty = int(request.args.get("qty", 1))
    if not q:
        return jsonify({"ok": False, "error": "q required"})
    try:
        from src.agents.vendor_ordering_agent import compare_vendor_prices
        return jsonify({"ok": True, **compare_vendor_prices(q, qty)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/vendor/order", methods=["POST"])
@auth_required
def api_vendor_order():
    """Place a vendor order or email PO.
    POST {vendor_key, items: [{description, quantity, unit_price}], po_number, quote_number}
    """
    data = request.get_json(silent=True) or {}
    vendor_key = data.get("vendor_key", "")
    items = data.get("items", [])
    po_number = data.get("po_number", "")
    quote_number = data.get("quote_number", "")

    if not vendor_key or not items or not po_number:
        return jsonify({"ok": False, "error": "vendor_key, items, and po_number required"})

    try:
        from src.agents.vendor_ordering_agent import VENDOR_CATALOG, grainger_place_order, send_email_po
        vendor = VENDOR_CATALOG.get(vendor_key, {})
        
        if not vendor:
            return jsonify({"ok": False, "error": f"Unknown vendor: {vendor_key}"})
        if not vendor.get("can_order"):
            return jsonify({"ok": False, "error": f"Vendor {vendor_key} not configured for ordering", "setup": vendor.get("env_needed", [])})
        
        api_type = vendor.get("api_type", "")
        if api_type == "rest":
            result = grainger_place_order(items, po_number)
        elif api_type == "email_po":
            result = send_email_po(vendor_key, items, po_number, quote_number)
        else:
            return jsonify({"ok": False, "error": f"Vendor type {api_type} not supported for ordering"})
        
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/vendor/orders")
@auth_required
def api_vendor_orders():
    """Get vendor order history."""
    status_filter = request.args.get("status")
    limit = int(request.args.get("limit", 50))
    try:
        from src.agents.vendor_ordering_agent import get_vendor_orders
        orders = get_vendor_orders(limit=limit, status=status_filter)
        return jsonify({"ok": True, "count": len(orders), "orders": orders})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/vendor/enrich", methods=["POST"])
@auth_required
def api_vendor_enrich():
    """Get enriched vendor list with API metadata."""
    try:
        from src.agents.vendor_ordering_agent import get_enriched_vendor_list
        return jsonify({"ok": True, "vendors": get_enriched_vendor_list()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

# ════════════════════════════════════════════════════════════════════════════════
# CS AGENT ROUTES — Inbound Customer Service
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/cs/classify", methods=["POST"])
@auth_required
def api_cs_classify():
    """Classify an email as an update request and get its intent.
    POST {subject, body, sender}
    """
    body = request.get_json(silent=True) or {}
    try:
        from src.agents.cs_agent import classify_inbound_email
        result = classify_inbound_email(
            subject=body.get("subject",""),
            body=body.get("body",""),
            sender=body.get("sender",""),
        )
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cs/draft", methods=["POST"])
@auth_required
def api_cs_draft():
    """Build a CS response draft for an inbound email.
    POST {subject, body, sender}
    Returns a draft ready for review in the outbox.
    """
    body = request.get_json(silent=True) or {}
    try:
        from src.agents.cs_agent import classify_inbound_email, build_cs_response_draft
        subject = body.get("subject","")
        email_body = body.get("body","")
        sender = body.get("sender","")
        classification = classify_inbound_email(subject, email_body, sender)
        result = build_cs_response_draft(classification, subject, email_body, sender)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cs/drafts", methods=["GET"])
@auth_required
def api_cs_drafts():
    """Get all pending CS drafts from the outbox."""
    try:
        from src.agents.cs_agent import get_cs_drafts
        drafts = get_cs_drafts(limit=50)
        return jsonify({"ok": True, "count": len(drafts), "drafts": drafts})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cs/call", methods=["POST"])
@auth_required
def api_cs_call():
    """Place a CS follow-up call via Vapi.
    POST {phone_number, context: {intent, po_number, quote_number, institution, buyer_name}}
    """
    body = request.get_json(silent=True) or {}
    phone = body.get("phone_number","")
    if not phone:
        return jsonify({"ok": False, "error": "phone_number required"})
    try:
        from src.agents.cs_agent import place_cs_call
        result = place_cs_call(phone, context=body.get("context",{}))
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cs/status", methods=["GET"])
@auth_required
def api_cs_status():
    """Get CS agent status."""
    try:
        from src.agents.cs_agent import get_agent_status
        return jsonify({"ok": True, **get_agent_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/debug")
@auth_required
def debug_agent():
    """Live debug + monitoring agent — system health, data flow, automation status."""
    return render(DEBUG_PAGE_HTML, title="Debug Agent")


@bp.route("/api/debug/run")
@auth_required
def api_debug_run():
    """Run all debug checks and return JSON results. Used by /debug page."""
    results = {}
    start = time.time()

    # 1. DB health
    try:
        from src.core.db import get_db_stats, _is_railway_volume, DB_PATH
        db = get_db_stats()
        results["db"] = {
            "ok": True, "path": DB_PATH,
            "size_kb": db.get("db_size_kb", 0),
            "is_volume": _is_railway_volume(),
            "tables": {k: v for k, v in db.items() if k not in ("db_path","db_size_kb")},
        }
    except Exception as e:
        results["db"] = {"ok": False, "error": str(e)}

    # 2. Data files
    data_files = {}
    for fname in ["quotes_log.json","crm_contacts.json","intel_buyers.json",
                  "intel_agencies.json","quote_counter.json","orders.json"]:
        fp = os.path.join(DATA_DIR, fname)
        if os.path.exists(fp):
            try:
                d = json.load(open(fp))
                n = len(d) if isinstance(d, (list, dict)) else 0
                data_files[fname] = {"exists": True, "records": n, "size_kb": round(os.path.getsize(fp)/1024,1)}
            except Exception:
                data_files[fname] = {"exists": True, "records": "parse_error"}
        else:
            data_files[fname] = {"exists": False, "records": 0}
    results["data_files"] = data_files

    # 3. Quote counter
    try:
        nxt = peek_next_quote_number() if QUOTE_GEN_AVAILABLE else "N/A"
        results["quote_counter"] = {"ok": True, "next": nxt}
    except Exception as e:
        results["quote_counter"] = {"ok": False, "error": str(e)}

    # 4. Intel + CRM sync state
    try:
        intel_buyers = 0
        crm_count = len(_load_crm_contacts())
        if INTEL_AVAILABLE:
            from src.agents.sales_intel import _load_json as _il2, BUYERS_FILE as _BF2
            bd = _il2(_BF2)
            intel_buyers = bd.get("total_buyers", 0) if isinstance(bd, dict) else 0
        in_sync = intel_buyers == crm_count or crm_count >= intel_buyers
        results["sync"] = {
            "ok": in_sync,
            "intel_buyers": intel_buyers,
            "crm_contacts": crm_count,
            "delta": abs(crm_count - intel_buyers),
        }
    except Exception as e:
        results["sync"] = {"ok": False, "error": str(e)}

    # 5. Auto-seed check
    try:
        crm_count = results.get("sync", {}).get("crm_contacts", 0)
        results["auto_seed"] = {
            "needed": crm_count == 0,
            "crm_contacts": crm_count,
            "status": "empty — run Load Demo Data" if crm_count == 0 else f"ok ({crm_count} contacts)",
        }
    except Exception as e:
        results["auto_seed"] = {"ok": False, "error": str(e)}

    # 6. Funnel stats
    try:
        quotes = [q for q in get_all_quotes() if not q.get("is_test")]
        results["funnel"] = {
            "ok": True,
            "quotes_total": len(quotes),
            "quotes_sent": sum(1 for q in quotes if q.get("status") == "sent"),
            "quotes_won": sum(1 for q in quotes if q.get("status") == "won"),
            "orders": len(_load_orders()),
        }
    except Exception as e:
        results["funnel"] = {"ok": False, "error": str(e)}

    # 7. Module availability
    results["modules"] = {
        "quote_gen": QUOTE_GEN_AVAILABLE,
        "price_check": PRICE_CHECK_AVAILABLE,
        "intel": INTEL_AVAILABLE,
        "growth": GROWTH_AVAILABLE,
        "qb": QB_AVAILABLE,
        "predict": PREDICT_AVAILABLE,
        "auto_processor": AUTO_PROCESSOR_AVAILABLE,
    }

    # 8. Recent errors from QA
    try:
        if QA_AVAILABLE:
            hist = get_qa_history(5)
            last = hist[0] if hist else {}
            results["qa"] = {
                "score": last.get("health_score", 0),
                "grade": last.get("grade", "?"),
                "critical_issues": last.get("critical_issues", []),
                "last_run": last.get("timestamp", "never"),
            }
    except Exception as e:
        results["qa"] = {"error": str(e)}

    # 9. Railway environment
    results["railway"] = {
        "environment": os.environ.get("RAILWAY_ENVIRONMENT", "local"),
        "volume_name": os.environ.get("RAILWAY_VOLUME_NAME", "not mounted"),
        "volume_path": os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "not mounted"),
        "deployment_id": os.environ.get("RAILWAY_DEPLOYMENT_ID", "local")[:16] if os.environ.get("RAILWAY_DEPLOYMENT_ID") else "local",
    }

    results["elapsed_ms"] = round((time.time() - start) * 1000)
    results["ok"] = True
    results["timestamp"] = datetime.now().isoformat()
    return jsonify(results)


@bp.route("/api/debug/fix/<fix_name>", methods=["POST"])
@auth_required
def api_debug_fix(fix_name):
    """Run an automated fix. fix_name: seed_demo | sync_crm | clear_cache | reset_counter"""
    if fix_name == "seed_demo":
        if INTEL_AVAILABLE:
            from src.agents.sales_intel import seed_demo_data
            r = seed_demo_data()
            return jsonify({"ok": True, "result": r})
        return jsonify({"ok": False, "error": "Intel not available"})

    elif fix_name == "sync_crm":
        if INTEL_AVAILABLE:
            from src.agents.sales_intel import sync_buyers_to_crm
            r = sync_buyers_to_crm()
            return jsonify({"ok": True, "result": r})
        return jsonify({"ok": False, "error": "Intel not available"})

    elif fix_name == "clear_cache":
        with _json_cache_lock:
            count = len(_json_cache)
            _json_cache.clear()
        return jsonify({"ok": True, "cleared": count})

    elif fix_name == "migrate_db":
        try:
            from src.core.db import migrate_json_to_db
            r = migrate_json_to_db()
            return jsonify({"ok": True, "result": r})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)})

    return jsonify({"ok": False, "error": f"Unknown fix: {fix_name}"})


@bp.route("/search")
@auth_required
def universal_search_page():
    """Universal search page — searches all data types: quotes, contacts, intel buyers, orders, RFQs."""
    q = (_sanitize_input(request.args.get("q", "")) or "").strip()

    # Run search if query provided
    results = []
    breakdown = {}
    error = None

    if q and len(q) >= 2:
        try:
            # Reuse the API logic directly
            from flask import g as _g
            # Call search inline to avoid HTTP round-trip
            ql = q.lower()
            limit = 50

            # ── Quotes ──
            if QUOTE_GEN_AVAILABLE:
                try:
                    for qt in search_quotes(query=ql, limit=20):
                        qn = qt.get("quote_number", "")
                        inst = qt.get("institution","") or qt.get("ship_to_name","") or "—"
                        ag   = qt.get("agency","") or "—"
                        results.append({
                            "type": "quote", "icon": "📋",
                            "title": qn,
                            "subtitle": f"{ag} · {inst[:50]}",
                            "meta": f"${qt.get('total',0):,.0f} · {qt.get('status','')} · {str(qt.get('created_at',''))[:10]}",
                            "url": f"/quote/{qn}",
                        })
                except Exception:
                    pass

            # ── CRM Contacts ──
            try:
                contacts = _load_crm_contacts()
                for cid, c in contacts.items():
                    fields = " ".join([
                        c.get("buyer_name",""), c.get("buyer_email",""),
                        c.get("agency",""), c.get("title",""), c.get("notes",""),
                        c.get("buyer_phone",""),
                        " ".join(str(k) for k in c.get("categories",{}).keys()),
                        " ".join(i.get("description","") for i in c.get("items_purchased",[])[:5]),
                    ]).lower()
                    if ql in fields:
                        spend = c.get("total_spend",0)
                        results.append({
                            "type": "contact", "icon": "👤",
                            "title": c.get("buyer_name","") or c.get("buyer_email",""),
                            "subtitle": f"{c.get('agency','')} · {c.get('buyer_email','')}",
                            "meta": f"${spend:,.0f} spend · {c.get('outreach_status','new')} · {len(c.get('activity',[]))} interactions",
                            "url": f"/growth/prospect/{cid}",
                        })
            except Exception:
                pass

            # ── Intel Buyers (not yet in CRM) ──
            if INTEL_AVAILABLE:
                try:
                    from src.agents.sales_intel import _load_json as _il, BUYERS_FILE as _BF
                    buyers_data = _il(_BF)
                    crm_ids = set(_load_crm_contacts().keys())
                    if isinstance(buyers_data, dict):
                        for b in buyers_data.get("buyers", []):
                            if b.get("id","") in crm_ids:
                                continue
                            email = (b.get("email","") or b.get("buyer_email","")).lower()
                            fields = " ".join([
                                b.get("name","") or b.get("buyer_name",""),
                                email, b.get("agency",""),
                                " ".join(b.get("categories",{}).keys()),
                                " ".join(i.get("description","") for i in b.get("items_purchased",[])[:5]),
                            ]).lower()
                            if ql in fields:
                                results.append({
                                    "type": "intel_buyer", "icon": "🧠",
                                    "title": b.get("name","") or b.get("buyer_name","") or email,
                                    "subtitle": f"{b.get('agency','')} · {email}",
                                    "meta": f"${b.get('total_spend',0):,.0f} spend · score {b.get('opportunity_score',0)} · not in CRM",
                                    "url": f"/growth/prospect/{b.get('id','')}",
                                })
                except Exception:
                    pass

            # ── Orders ──
            try:
                orders = _load_orders()
                for oid, o in orders.items():
                    fields = " ".join([
                        o.get("quote_number",""), o.get("agency",""),
                        o.get("institution",""), o.get("po_number",""), oid,
                    ]).lower()
                    if ql in fields:
                        results.append({
                            "type": "order", "icon": "📦",
                            "title": oid,
                            "subtitle": f"{o.get('agency','')} · {o.get('institution','')}",
                            "meta": f"PO {o.get('po_number','—')} · {o.get('status','')}",
                            "url": f"/order/{oid}",
                        })
            except Exception:
                pass

            # ── RFQs ──
            try:
                rfqs = load_rfqs()
                for rid, r in rfqs.items():
                    fields = " ".join([
                        r.get("rfq_number",""), r.get("requestor_name",""),
                        r.get("institution",""), r.get("agency",""), rid,
                        " ".join(str(i.get("description","")) for i in r.get("items",[])),
                    ]).lower()
                    if ql in fields:
                        results.append({
                            "type": "rfq", "icon": "📄",
                            "title": r.get("rfq_number","") or rid[:12],
                            "subtitle": f"{r.get('agency','')} · {r.get('requestor_name','')}",
                            "meta": f"{len(r.get('items',[]))} items · {r.get('status','')}",
                            "url": f"/rfq/{rid}",
                        })
            except Exception:
                pass

            # Dedupe by URL
            seen = set()
            deduped = []
            for r in results:
                if r["url"] not in seen:
                    seen.add(r["url"])
                    deduped.append(r)
            results = deduped[:limit]

            breakdown = {t: sum(1 for r in results if r["type"]==t)
                         for t in ("quote","contact","intel_buyer","order","rfq")}
        except Exception as e:
            error = str(e)

    # Build type badge colors
    type_styles = {
        "quote":       ("#58a6ff", "rgba(88,166,255,.12)",  "📋 Quote"),
        "contact":     ("#a78bfa", "rgba(167,139,250,.12)", "👤 Contact"),
        "intel_buyer": ("#3fb950", "rgba(52,211,153,.12)",  "🧠 Intel Buyer"),
        "order":       ("#fbbf24", "rgba(251,191,36,.12)",  "📦 Order"),
        "rfq":         ("#f87171", "rgba(248,113,113,.12)", "📄 RFQ"),
    }

    rows_html = ""
    for r in results:
        color, bg, lbl = type_styles.get(r["type"], ("#8b949e","rgba(139,148,160,.12)","?"))
        rows_html += f"""
        <a href="{r['url']}" style="display:block;text-decoration:none;padding:14px 16px;border-bottom:1px solid var(--bd);transition:background .1s" onmouseover="this.style.background='rgba(79,140,255,.06)'" onmouseout="this.style.background=''">
         <div style="display:flex;align-items:center;gap:12px">
          <span style="font-size:11px;padding:3px 8px;border-radius:10px;color:{color};background:{bg};white-space:nowrap;font-weight:600">{lbl}</span>
          <div style="flex:1;min-width:0">
           <div style="font-weight:600;font-size:14px;color:var(--tx);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{r['title']}</div>
           <div style="font-size:12px;color:var(--tx2);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{r['subtitle']}</div>
          </div>
          <div style="font-size:11px;color:var(--tx2);white-space:nowrap;text-align:right">{r['meta']}</div>
          <span style="color:var(--ac);font-size:16px">→</span>
         </div>
        </a>"""

    breakdown_html = ""
    if breakdown:
        for t, count in breakdown.items():
            if count:
                color, bg, lbl = type_styles.get(t, ("#8b949e","rgba(139,148,160,.12)",t))
                breakdown_html += f'<span style="font-size:12px;padding:3px 10px;border-radius:10px;color:{color};background:{bg}">{lbl}: {count}</span>'

    empty_state = ""
    if q and len(q) >= 2 and not results:
        empty_state = f"""
        <div style="text-align:center;padding:48px 24px;color:var(--tx2)">
         <div style="font-size:40px;margin-bottom:12px">🔍</div>
         <div style="font-size:16px;font-weight:600;margin-bottom:6px">No results for "{q}"</div>
         <div style="font-size:13px;margin-bottom:20px">Try a name, agency, email, item description, or quote number</div>
         <div style="display:flex;gap:8px;justify-content:center;flex-wrap:wrap">
          <a href="/quotes" style="padding:8px 16px;background:var(--sf2);border:1px solid var(--bd);border-radius:7px;color:var(--tx);font-size:13px;text-decoration:none">📋 Browse Quotes</a>
          <a href="/contacts" style="padding:8px 16px;background:var(--sf2);border:1px solid var(--bd);border-radius:7px;color:var(--tx);font-size:13px;text-decoration:none">👥 Browse CRM</a>
          <a href="/intelligence" style="padding:8px 16px;background:var(--sf2);border:1px solid var(--bd);border-radius:7px;color:var(--tx);font-size:13px;text-decoration:none">🧠 Sales Intel</a>
         </div>
        </div>"""

    q_escaped = q.replace('"','&quot;')
    return render(f"""
     <!-- Search header -->
     <div style="display:flex;align-items:center;gap:12px;margin-bottom:18px;flex-wrap:wrap">
      <h2 style="margin:0;font-size:20px;font-weight:700">🔍 Search</h2>
      {'<div style="font-size:13px;color:var(--tx2)">' + str(len(results)) + ' results for <b style="color:var(--tx)">"' + q + '"</b></div>' if q else ''}
     </div>

     <!-- Search form -->
     <form method="get" action="/search" style="display:flex;gap:10px;margin-bottom:16px">
      <div style="flex:1;display:flex;background:var(--sf);border:1.5px solid var(--ac);border-radius:10px;overflow:hidden">
       <span style="padding:0 14px;font-size:18px;display:flex;align-items:center;color:var(--tx2)">🔍</span>
       <input name="q" value="{q_escaped}" placeholder="Search quotes, contacts, buyers, orders, RFQs..." autofocus
              style="flex:1;padding:14px 4px 14px 0;background:transparent;border:none;color:var(--tx);font-size:15px;outline:none" autocomplete="off">
       <button type="submit" style="padding:14px 22px;background:var(--ac);border:none;color:#fff;font-size:14px;font-weight:700;cursor:pointer">Search</button>
      </div>
     </form>

     <!-- Breakdown badges -->
     {('<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px">' + breakdown_html + '</div>') if breakdown_html else ''}

     <!-- Results -->
     <div style="background:var(--sf);border:1px solid var(--bd);border-radius:10px;overflow:hidden">
      {rows_html if rows_html else empty_state if q else '<div style="text-align:center;padding:48px;color:var(--tx2)"><div style="font-size:40px;margin-bottom:12px">🔍</div><div style="font-size:15px">Type a name, agency, quote number, or email above</div></div>'}
     </div>

     {'<div style="margin-top:10px;font-size:12px;color:var(--rd);padding:8px 12px;background:rgba(248,113,113,.1);border-radius:6px">Search error: ' + error + '</div>' if error else ''}

     <!-- Data sources key -->
     <div style="margin-top:14px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
      <span style="font-size:11px;color:var(--tx2)">Searches:</span>
      {''.join(f'<span style="font-size:11px;padding:2px 8px;border-radius:8px;color:{c};background:{bg}">{lbl}</span>' for t,(c,bg,lbl) in type_styles.items())}
     </div>
    """, title=f'Search{" — " + q if q else ""}')


@bp.route("/quotes")
@auth_required
def quotes_list():
    """Browse / search all generated Reytech quotes with win/loss tracking."""
    if not QUOTE_GEN_AVAILABLE:
        flash("Quote generator not available", "error")
        return redirect("/")
    q = request.args.get("q", "")
    agency_filter = request.args.get("agency", "")
    status_filter = request.args.get("status", "")
    quotes = search_quotes(query=q, agency=agency_filter, status=status_filter, limit=100)
    next_num = peek_next_quote_number()
    stats = get_quote_stats()

    # Check if logo exists
    logo_exists = any(os.path.exists(os.path.join(DATA_DIR, f"reytech_logo.{e}"))
                      for e in ("png", "jpg", "jpeg", "gif"))

    # Status badge colors
    status_cfg = {
        "won":     ("✅ Won",     "#3fb950", "rgba(52,211,153,.08)"),
        "lost":    ("❌ Lost",    "#f85149", "rgba(248,113,113,.08)"),
        "pending": ("⏳ Pending", "#d29922", "rgba(210,153,34,.08)"),
        "draft":   ("📝 Draft",   "#8b949e", "rgba(139,148,160,.08)"),
        "sent":    ("📤 Sent",    "#58a6ff", "rgba(88,166,255,.08)"),
        "expired": ("⏰ Expired", "#8b949e", "rgba(139,148,160,.08)"),
    }

    rows_html = ""
    for qt in quotes:
        fname = os.path.basename(qt.get("pdf_path", ""))
        dl = f'<a href="/api/pricecheck/download/{fname}" title="Download PDF" style="font-size:14px">📥</a>' if fname else ""
        st = qt.get("status", "pending")

        # Derive institution from ship_to if empty/missing
        institution = qt.get("institution", "")
        if not institution or institution.strip() == "":
            ship_name = qt.get("ship_to_name", "")
            if ship_name:
                institution = ship_name
            else:
                # Try from items_text or rfq_number as last resort
                institution = qt.get("rfq_number", "") or "—"

        # Fix DEFAULT agency using ALL available data
        agency = qt.get("agency", "")
        if agency in ("DEFAULT", "", None) and QUOTE_GEN_AVAILABLE:
            try:
                agency = _detect_agency(qt)
            except Exception as e:
                log.debug("Suppressed: %s", e)
                agency = ""
        if agency == "DEFAULT":
            agency = ""

        lbl, color, bg = status_cfg.get(st, status_cfg["pending"])
        po = qt.get("po_number", "")
        po_html = f'<br><span style="font-size:10px;color:#8b949e">PO: {po}</span>' if po else ""
        qn = qt.get("quote_number", "")
        items_detail = qt.get("items_detail", [])
        items_text = qt.get("items_text", "")

        # Build expandable detail row
        detail_rows = ""
        if items_detail:
            for it in items_detail[:10]:
                desc = str(it.get("description", ""))[:80]
                pn = it.get("part_number", "")
                pn_link = f'<a href="https://amazon.com/dp/{pn}" target="_blank" style="color:#58a6ff;font-size:10px">{pn}</a>' if pn and pn.startswith("B0") else (f'<span style="color:#8b949e;font-size:10px">{pn}</span>' if pn else "")
                detail_rows += f'<div style="display:flex;gap:8px;align-items:baseline;padding:2px 0"><span style="color:var(--tx2);font-size:11px;flex:1">{desc}</span>{pn_link}<span style="font-family:monospace;font-size:11px;color:#d29922">${it.get("unit_price",0):.2f} × {it.get("qty",0)}</span></div>'
        elif items_text:
            detail_rows = f'<div style="color:var(--tx2);font-size:11px;padding:2px 0">{items_text[:200]}</div>'

        detail_id = f"detail-{qn.replace(' ','')}"
        toggle = f"""<button onclick="document.getElementById('{detail_id}').style.display=document.getElementById('{detail_id}').style.display==='none'?'table-row':'none'" style="background:none;border:none;cursor:pointer;font-size:10px;color:var(--tx2);padding:0" title="Show items">▶ {qt.get('items_count',0)}</button>""" if (items_detail or items_text) else str(qt.get('items_count', 0))

        # Quote number links to dedicated detail page
        test_badge = ' <span style="background:#d29922;color:#000;font-size:9px;padding:1px 5px;border-radius:4px;font-weight:700">TEST</span>' if qt.get("is_test") or qt.get("source_pc_id", "").startswith("test_") else ""
        qn_cell = f'<a href="/quote/{qn}" style="color:var(--ac);text-decoration:none;font-family:\'JetBrains Mono\',monospace;font-weight:700" title="View quote details">{qn}</a>{test_badge}'

        # Decided rows get subtle opacity
        row_style = "opacity:0.5" if st in ("won", "lost", "expired") else ""

        rows_html += f"""<tr data-qn="{qn}" style="{row_style}">
         <td>{qn_cell}</td>
         <td class="mono" style="white-space:nowrap">{qt.get('date','')}</td>
         <td>{agency}</td>
         <td style="max-width:300px;word-wrap:break-word;white-space:normal;font-weight:500">{institution}</td>
         <td class="mono">{qt.get('rfq_number','')}</td>
         <td style="text-align:right;font-weight:600;font-family:'JetBrains Mono',monospace">${qt.get('total',0):,.2f}</td>
         <td style="text-align:center">{toggle}</td>
         <td style="text-align:center">
          <span style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;color:{color};background:{bg}">{lbl}</span>{po_html}
         </td>
         <td style="text-align:center;white-space:nowrap">
          {"<a href=\"/order/ORD-" + qn + "\" style=\"font-size:11px;color:#3fb950;text-decoration:none;padding:2px 6px\" title=\"View order\">📦 Order</a>" if st == "won" else "<span style=\"font-size:11px;color:#8b949e;padding:2px 6px\">lost</span>" if st == "lost" else f"<button onclick=\"markQuote('{qn}','won')\" class=\"btn btn-sm\" style=\"background:rgba(52,211,153,.15);color:#3fb950;border:1px solid rgba(52,211,153,.3);padding:2px 6px;font-size:11px;cursor:pointer\" title=\"Mark Won\">✅</button><button onclick=\"markQuote('{qn}','lost')\" class=\"btn btn-sm\" style=\"background:rgba(248,113,113,.15);color:#f85149;border:1px solid rgba(248,113,113,.3);padding:2px 6px;font-size:11px;cursor:pointer\" title=\"Mark Lost\">❌</button>" if st not in ("expired",) else "<span style=\"font-size:11px;color:#8b949e\">expired</span>"}
          {dl}
         </td>
        </tr>
        <tr id="{detail_id}" style="display:none"><td colspan="9" style="background:var(--sf2);padding:8px 16px;border-left:3px solid var(--ac)">{detail_rows if detail_rows else '<span style="color:var(--tx2);font-size:11px">No item details available</span>'}</td></tr>"""

    # Win rate stats bar
    wr = stats.get("win_rate", 0)
    wr_color = "#3fb950" if wr >= 50 else ("#d29922" if wr >= 30 else "#f85149")
    expired_count = sum(1 for qt in quotes if qt.get("status") == "expired")
    stats_html = f"""
     <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;font-size:13px;font-family:'JetBrains Mono',monospace">
      <span><b>{stats['total']}</b> total</span>
      <span style="color:#3fb950"><b>{stats['won']}</b> won (${stats['won_total']:,.0f})</span>
      <span style="color:#f85149"><b>{stats['lost']}</b> lost</span>
      <span style="color:#d29922"><b>{stats['pending']}</b> pending</span>
      {f'<span style="color:#8b949e"><b>{expired_count}</b> expired</span>' if expired_count else ''}
      <span>WR: <b style="color:{wr_color}">{wr}%</b></span>
      <span style="color:#8b949e">Next: <b style="color:var(--tx)">{next_num}</b></span>
     </div>
    """

    return render(build_quotes_page_content(
        stats_html=stats_html, q=q, agency_filter=agency_filter,
        status_filter=status_filter, logo_exists=logo_exists, rows_html=rows_html
    ), title="Quotes Database")


@bp.route("/quote/<qn>")
def quote_detail(qn):
    """Dedicated quote detail page."""
    if not QUOTE_GEN_AVAILABLE:
        flash("Quote generator not available", "error")
        return redirect("/")
    quotes = get_all_quotes()
    qt = None
    for q in quotes:
        if q.get("quote_number") == qn:
            qt = q
            break
    if not qt:
        flash(f"Quote {qn} not found", "error")
        return redirect("/quotes")

    # Derive institution from ship_to if empty
    institution = qt.get("institution", "")
    if not institution or institution.strip() == "":
        institution = qt.get("ship_to_name", "") or qt.get("rfq_number", "") or "—"

    # Fix DEFAULT agency using all available data
    agency = qt.get("agency", "")
    if agency in ("DEFAULT", "", None):
        try:
            agency = _detect_agency(qt)
        except Exception:
            agency = ""
    if agency == "DEFAULT":
        agency = ""

    st = qt.get("status", "pending")
    fname = os.path.basename(qt.get("pdf_path", ""))
    # items_detail is canonical; line_items is the field in quotes_log.json
    items = qt.get("items_detail") or qt.get("line_items") or []
    source_link = ""
    source_label = ""
    if qt.get("source_pc_id"):
        source_link = f'/pricecheck/{qt["source_pc_id"]}'
        source_label = f"PC # {qt.get('rfq_number', 'Price Check')}"
    elif qt.get("source_rfq_id"):
        source_link = f'/rfq/{qt["source_rfq_id"]}'
        source_label = "RFQ"
    elif not source_link and qt.get("notes") and "PC#" in str(qt.get("notes", "")):
        import re as _re2
        _m = _re2.search(r"PC#\s*([^|\n]+)", str(qt.get("notes", "")))
        if _m:
            source_label = f"PC # {_m.group(1).strip()}"
            # Search for the PC in price_checks.json by pc_number match
            try:
                import json as _j2
                _pcs = _j2.load(open(os.path.join(DATA_DIR, "price_checks.json")))
                _pc_num = _m.group(1).strip().lower().replace(" ", "").replace("-", "")
                for _pid, _pc in _pcs.items():
                    _pnum = str(_pc.get("pc_number","")).lower().replace(" ", "").replace("-", "")
                    if _pnum == _pc_num:
                        source_link = f"/pricecheck/{_pid}"
                        break
            except Exception:
                pass

    # Status config
    status_cfg = {
        "won":     ("✅ Won",     "var(--gn)", "rgba(52,211,153,.1)"),
        "lost":    ("❌ Lost",    "var(--rd)", "rgba(248,113,113,.1)"),
        "pending": ("⏳ Pending", "var(--yl)", "rgba(251,191,36,.1)"),
        "draft":   ("📝 Draft",   "var(--tx2)", "rgba(139,148,160,.1)"),
        "sent":    ("📤 Sent",    "var(--ac)", "rgba(79,140,255,.1)"),
        "expired": ("⏰ Expired", "var(--tx2)", "rgba(139,148,160,.1)"),
    }
    lbl, color, bg = status_cfg.get(st, status_cfg["pending"])

    # Items table rows
    items_html = ""
    for it in items:
        desc = str(it.get("description", ""))
        pn = it.get("part_number", "")
        pn_cell = f'<a href="https://amazon.com/dp/{pn}" target="_blank" style="color:var(--ac)">{pn}</a>' if pn and pn.startswith("B0") else (pn or "—")
        up = it.get("unit_price", 0)
        qty = it.get("qty", 0)
        items_html += f"""<tr>
         <td style="color:var(--tx2)">{it.get('line_number', '')}</td>
         <td style="max-width:400px;word-wrap:break-word;white-space:normal">{desc}</td>
         <td class="mono">{pn_cell}</td>
         <td class="mono" style="text-align:center">{qty}</td>
         <td class="mono" style="text-align:right">${up:,.2f}</td>
         <td class="mono" style="text-align:right;font-weight:600">${up*qty:,.2f}</td>
        </tr>"""

    # Status history
    history = qt.get("status_history", [])
    history_html = ""
    for h in reversed(history[-10:]):
        history_html += f'<div style="font-size:11px;color:var(--tx2);padding:3px 0"><span class="mono">{h.get("timestamp","")[:16]}</span> → <b>{h.get("status","")}</b>{" by " + h.get("actor","") if h.get("actor") else ""}{" (PO: " + h["po_number"] + ")" if h.get("po_number") else ""}</div>'

    # Build action buttons separately to avoid f-string escaping
    if st in ('pending', 'sent'):
        action_btns = '<div style="border-top:1px solid var(--bd);margin-top:14px;padding-top:14px;display:flex;gap:8px;justify-content:center">'
        action_btns += f'<button onclick="markQuote(&quot;{qn}&quot;,&quot;won&quot;)" class="btn btn-g" style="font-size:13px">✅ Mark Won</button>'
        action_btns += f'<button onclick="markQuote(&quot;{qn}&quot;,&quot;lost&quot;)" class="btn" style="background:rgba(248,113,113,.15);color:var(--rd);border:1px solid rgba(248,113,113,.3);font-size:13px">❌ Mark Lost</button>'
        action_btns += '</div>'
    else:
        action_btns = ""

    content = f"""
    <div style="display:flex;gap:10px;align-items:center;margin-bottom:16px">
     <a href="/quotes" class="btn btn-s" style="font-size:13px">← Quotes</a>
     {f'<a href="{source_link}" class="btn btn-s" style="font-size:13px">📎 {source_label}</a>' if source_link else ''}
     {f'<a href="/api/pricecheck/download/{fname}" class="btn btn-s" style="font-size:13px">📥 Download PDF</a>' if fname else ''}
     {f'<a href="/api/pricecheck/view-pdf/{fname}" target="_blank" class="btn btn-s" style="font-size:13px">📄 View PDF</a>' if fname else ''}
     <a href="/outbox?filter=cs" class="btn btn-s" style="font-size:13px;background:rgba(251,191,36,.12);color:var(--yl);border:1px solid rgba(251,191,36,.3)">💬 CS Inbox</a>
    </div>

    <!-- Header -->
    <div class="bento bento-2" style="margin-bottom:14px">
     <div class="card" style="margin:0">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px">
       <div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:28px;font-weight:700">{qn}</div>
        <div style="color:var(--tx2);font-size:12px;margin-top:4px">{agency}{' · ' if agency else ''}Generated {qt.get('date','')}</div>
       </div>
       <span style="padding:4px 12px;border-radius:12px;font-size:12px;font-weight:600;color:{color};background:{bg}">{lbl}</span>
      </div>
      <div class="meta-g" style="margin:0">
       <div class="meta-i"><div class="meta-l">Institution</div><div class="meta-v">{institution}</div></div>
       <div class="meta-i"><div class="meta-l">RFQ / PC #</div><div class="meta-v">{qt.get('rfq_number','—')}</div></div>
       <div class="meta-i"><div class="meta-l">Items</div><div class="meta-v">{qt.get('items_count',0)}</div></div>
       <div class="meta-i"><div class="meta-l">Expiry</div><div class="meta-v">{qt.get('expiry','—')}</div></div>
       {'<div class="meta-i"><div class="meta-l">PO Number</div><div class="meta-v" style="color:var(--gn);font-weight:600">' + qt.get("po_number","") + '</div></div>' if qt.get("po_number") else ''}
      </div>
     </div>
     <div class="card" style="margin:0">
      <div style="text-align:center;padding:12px 0">
       <div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px">Quote Total</div>
       <div style="font-family:'JetBrains Mono',monospace;font-size:36px;font-weight:700;color:var(--gn);margin:8px 0">${qt.get('total',0):,.2f}</div>
       <div style="display:flex;justify-content:center;gap:16px;font-size:12px;color:var(--tx2)">
        <span>Subtotal: <b>${qt.get('subtotal',0):,.2f}</b></span>
        <span>Tax: <b>${qt.get('tax',0):,.2f}</b></span>
       </div>
      </div>
      {'<div style="border-top:1px solid var(--bd);margin-top:14px;padding-top:14px"><div style="font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px">Status History</div>' + history_html + '</div>' if history_html else ''}
      {action_btns}
     </div>
    </div>

    <!-- PDF Preview -->
    {f'''<div class="card" style="margin-bottom:14px">
     <div class="card-t">📄 Quote PDF Preview</div>
     <iframe src="/api/pricecheck/view-pdf/{fname}" style="width:100%;height:700px;border:none;border-radius:8px;background:var(--sf2)" title="Quote PDF"></iframe>
    </div>''' if fname else ''}

    <!-- Line Items -->
    <div class="card">
     <div class="card-t">Line Items ({len(items)} item{"s" if len(items)!=1 else ""})</div>
     <div style="overflow-x:auto">
     <table class="home-tbl">
      <thead><tr>
       <th style="width:40px">#</th><th>Description</th><th style="width:120px">Part #</th>
       <th style="width:60px;text-align:center">Qty</th><th style="width:90px;text-align:right">Unit Price</th><th style="width:90px;text-align:right">Extended</th>
      </tr></thead>
      <tbody>{items_html if items_html else '<tr><td colspan="6" style="text-align:center;padding:16px;color:var(--tx2)">No item details stored</td></tr>'}</tbody>
     </table>
     </div>
    </div>

    <!-- CRM Section: Agency Intel + Activity Timeline -->
    <div class="bento bento-2" style="margin-top:14px">
     <div class="card" style="margin:0">
      <div class="card-t">🏢 Agency Intel</div>
      <div id="win-prediction" style="padding:6px 0;font-size:12px"></div>
      <div id="agency-intel" style="color:var(--tx2);font-size:12px;padding:4px 0">Loading agency data...</div>
     </div>
     <div class="card" style="margin:0">
      <div class="card-t">📋 Activity Timeline</div>
      <div id="crm-activity" style="max-height:320px;overflow-y:auto;font-size:12px">Loading...</div>
      <div style="margin-top:10px;border-top:1px solid var(--bd);padding-top:10px;display:flex;gap:6px">
       <input id="crm-note" placeholder="Add a note..." style="flex:1;padding:8px 10px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);font-size:12px">
       <button onclick="addNote()" class="btn btn-p" style="padding:8px 12px;font-size:12px">Add</button>
      </div>
     </div>
    </div>

    <script>
    function markQuote(qn, status) {{
      let po = '';
      if (status === 'won') {{
        po = prompt('PO number (optional):', '') || '';
      }}
      fetch('/quotes/' + qn + '/status', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{status: status, po_number: po}})
      }})
      .then(r => r.json())
      .then(d => {{
        if (d.ok) {{ location.reload(); }}
        else {{ alert('Error: ' + (d.error || 'unknown')); }}
      }});
    }}

    function addNote() {{
      const note = document.getElementById('crm-note').value.trim();
      if (!note) return;
      fetch('/api/crm/activity', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{ref_id: '{qn}', event_type: 'note', description: note,
                              metadata: {{institution: '{institution}', agency: '{agency}'}} }})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{
          document.getElementById('crm-note').value = '';
          loadActivity();
        }}
      }});
    }}

    const eventIcons = {{
      'quote_won': '✅', 'quote_lost': '❌', 'quote_sent': '📤',
      'quote_generated': '📋', 'qb_po_created': '💰', 'email_sent': '📧',
      'email_received': '📨', 'voice_call': '📞', 'scprs_lookup': '🔍',
      'price_check': '📊', 'lead_scored': '🎯', 'follow_up': '🔔', 'note': '📝'
    }};

    function loadActivity() {{
      fetch('/api/crm/activity?ref_id={qn}&limit=30').then(r => r.json()).then(d => {{
        const el = document.getElementById('crm-activity');
        if (!d.ok || !d.activity.length) {{
          el.innerHTML = '<div style="color:var(--tx2);padding:12px">No activity yet</div>';
          return;
        }}
        el.innerHTML = d.activity.map(a => {{
          const icon = eventIcons[a.event_type] || '•';
          const ts = a.timestamp ? a.timestamp.substring(0,16).replace('T',' ') : '';
          const actor = a.actor && a.actor !== 'system' ? ' <span style="color:var(--ac)">' + a.actor + '</span>' : '';
          return '<div style="padding:6px 0;border-bottom:1px solid var(--bd);display:flex;gap:8px;align-items:baseline">' +
            '<span>' + icon + '</span>' +
            '<div style="flex:1"><div>' + a.description + actor + '</div>' +
            '<div style="font-size:10px;color:var(--tx2);font-family:monospace">' + ts + '</div></div></div>';
        }}).join('');
      }}).catch(() => {{
        document.getElementById('crm-activity').innerHTML = '<div style="color:var(--rd)">Failed to load</div>';
      }});
    }}

    function loadAgencyIntel() {{
      const agency = '{agency}' || '{institution}'.split('-')[0].split(' ')[0];
      if (!agency) {{
        document.getElementById('agency-intel').innerHTML = '<div style="color:var(--tx2)">No agency detected</div>';
        return;
      }}
      fetch('/api/crm/agency/' + encodeURIComponent(agency)).then(r => r.json()).then(d => {{
        if (!d.ok) {{ document.getElementById('agency-intel').innerHTML = '<div>No data</div>'; return; }}
        const wrColor = d.win_rate >= 50 ? 'var(--gn)' : (d.win_rate >= 30 ? 'var(--yl)' : 'var(--rd)');
        let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px">';
        html += '<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Quotes</div><div style="font-size:22px;font-weight:700">' + d.total_quotes + '</div></div>';
        html += '<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Win Rate</div><div style="font-size:22px;font-weight:700;color:' + wrColor + '">' + d.win_rate + '%</div></div>';
        html += '<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Won Value</div><div style="font-size:16px;font-weight:700;color:var(--gn)">$' + d.total_won_value.toLocaleString() + '</div></div>';
        html += '<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Pending</div><div style="font-size:22px;font-weight:700;color:var(--yl)">' + d.pending + '</div></div>';
        html += '</div>';
        if (d.institutions && d.institutions.length) {{
          html += '<div style="font-size:11px;color:var(--tx2);margin-bottom:6px"><b>Facilities:</b></div>';
          html += '<div style="display:flex;flex-wrap:wrap;gap:4px">';
          d.institutions.forEach(inst => {{
            html += '<span style="background:var(--sf2);padding:2px 8px;border-radius:10px;font-size:10px">' + inst + '</span>';
          }});
          html += '</div>';
        }}
        if (d.last_contact) {{
          const days = Math.floor((Date.now() - new Date(d.last_contact).getTime()) / 86400000);
          const color = days > 14 ? 'var(--rd)' : (days > 7 ? 'var(--yl)' : 'var(--gn)');
          html += '<div style="margin-top:10px;font-size:11px">Last contact: <b style="color:' + color + '">' + days + ' days ago</b></div>';
        }}
        document.getElementById('agency-intel').innerHTML = html;
      }}).catch(() => {{
        document.getElementById('agency-intel').innerHTML = '<div>Failed to load</div>';
      }});
    }}

    // Load on page ready
    loadActivity();
    loadAgencyIntel();

    // Win prediction
    fetch('/api/predict/win?institution={institution}&agency={agency}&value={qt.get("total",0)}')
      .then(r => r.json()).then(d => {{
        if (!d.ok) return;
        const pct = Math.round(d.probability * 100);
        const clr = pct >= 60 ? 'var(--gn)' : (pct >= 40 ? 'var(--yl)' : 'var(--rd)');
        const bar = '<div style="background:var(--sf2);border-radius:6px;height:8px;margin:6px 0;overflow:hidden"><div style="width:' + pct + '%;height:100%;background:' + clr + ';border-radius:6px;transition:width .5s"></div></div>';
        document.getElementById('win-prediction').innerHTML =
          '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">' +
          '<span style="font-weight:600;font-size:11px">🎯 Win Prediction</span>' +
          '<span style="font-size:18px;font-weight:700;color:' + clr + '">' + pct + '%</span></div>' + bar +
          '<div style="color:var(--tx2);font-size:10px">' + d.recommendation + ' <span style="opacity:.5">(' + d.confidence + ' confidence)</span></div>';
      }}).catch(() => {{}});
    </script>
    """
    return render(content, title=f"Quote {qn}")


# ═══════════════════════════════════════════════════════════════════════
# Order Management (Phase 17)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/orders")
@auth_required
def orders_page():
    """Orders dashboard — track sourcing, shipping, delivery, invoicing."""
    orders = _load_orders()
    order_list = sorted(orders.values(), key=lambda o: o.get("created_at", ""), reverse=True)

    status_cfg = {
        "new":              ("🆕 New",              "#58a6ff", "rgba(88,166,255,.1)"),
        "sourcing":         ("🛒 Sourcing",         "#d29922", "rgba(210,153,34,.1)"),
        "shipped":          ("🚚 Shipped",          "#bc8cff", "rgba(188,140,255,.1)"),
        "partial_delivery": ("📦 Partial",          "#d29922", "rgba(210,153,34,.1)"),
        "delivered":        ("✅ Delivered",         "#3fb950", "rgba(52,211,153,.1)"),
        "invoiced":         ("💰 Invoiced",         "#58a6ff", "rgba(88,166,255,.1)"),
        "closed":           ("🏁 Closed",           "#8b949e", "rgba(139,148,160,.1)"),
    }

    # Stats
    total_orders = len(order_list)
    active = sum(1 for o in order_list if o.get("status") not in ("closed",))
    total_value = sum(o.get("total", 0) for o in order_list)
    invoiced_value = sum(o.get("invoice_total", 0) for o in order_list)

    stats_html = f"""
    <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;font-size:13px;font-family:'JetBrains Mono',monospace">
     <span><b>{total_orders}</b> orders</span>
     <span style="color:#58a6ff"><b>{active}</b> active</span>
     <span style="color:#3fb950">value: <b>${total_value:,.0f}</b></span>
     <span style="color:#d29922">invoiced: <b>${invoiced_value:,.0f}</b></span>
    </div>"""

    rows = ""
    for o in order_list:
        oid = o.get("order_id", "")
        st = o.get("status", "new")
        lbl, clr, bg = status_cfg.get(st, status_cfg["new"])
        items = o.get("line_items", [])
        sourced = sum(1 for it in items if it.get("sourcing_status") in ("ordered", "shipped", "delivered"))
        shipped = sum(1 for it in items if it.get("sourcing_status") in ("shipped", "delivered"))
        delivered = sum(1 for it in items if it.get("sourcing_status") == "delivered")
        progress = f"{delivered}/{len(items)}" if items else "0/0"

        rows += f"""<tr style="{'opacity:0.5' if st == 'closed' else ''}">
         <td><a href="/order/{oid}" style="color:var(--ac);text-decoration:none;font-family:'JetBrains Mono',monospace;font-weight:700">{oid}</a></td>
         <td class="mono" style="white-space:nowrap">{o.get('created_at','')[:10]}</td>
         <td>{o.get('agency','')}</td>
         <td style="max-width:250px;word-wrap:break-word;white-space:normal;font-weight:500">{o.get('institution','')}</td>
         <td class="mono">{o.get('po_number','') or o.get('quote_number','')}</td>
         <td style="text-align:right;font-weight:600;font-family:'JetBrains Mono',monospace">${o.get('total',0):,.2f}</td>
         <td style="text-align:center">{progress}</td>
         <td style="text-align:center"><span style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;color:{clr};background:{bg}">{lbl}</span></td>
        </tr>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:14px">
     <h2 style="margin:0;font-size:20px;font-weight:700">📦 Orders</h2>
     <div>{stats_html}</div>
    </div>
    <div class="card" style="padding:0;overflow-x:auto">
     <table class="home-tbl" style="min-width:800px">
      <thead><tr>
       <th style="width:130px">Order</th><th style="width:90px">Date</th><th style="width:60px">Agency</th>
       <th>Institution</th><th style="width:100px">PO / Quote</th>
       <th style="text-align:right;width:90px">Total</th><th style="width:70px;text-align:center">Delivery</th>
       <th style="width:100px;text-align:center">Status</th>
      </tr></thead>
      <tbody>{rows if rows else '<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--tx2)">No orders yet — mark a quote as Won to create one</td></tr>'}</tbody>
     </table>
    </div>

    <!-- Pending Invoices from QuickBooks -->
    <div id="qb-invoices" class="card" style="margin-top:14px;padding:16px;display:none">
     <div class="card-t" style="margin-bottom:10px">💰 QuickBooks — Pending Invoices</div>
     <div id="inv-stats" style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px"></div>
     <div id="inv-table"></div>
    </div>
    <script>
    fetch('/api/qb/financial-context').then(r=>r.json()).then(d=>{{
     if(!d.ok) return;
     document.getElementById('qb-invoices').style.display='block';
     const s=document.getElementById('inv-stats');
     const mkStat=(label,val,color)=>'<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">'+label+'</div><div style="font-size:18px;font-weight:700;color:'+(color||'var(--tx)')+'">'+val+'</div></div>';
     s.innerHTML=mkStat('Open','$'+(d.total_receivable||0).toLocaleString(),'var(--yl)')
      +mkStat('Overdue','$'+(d.overdue_amount||0).toLocaleString(),'var(--rd)')
      +mkStat('Collected','$'+(d.total_collected||0).toLocaleString(),'var(--gn)')
      +mkStat('Invoices',d.invoice_count||0);
     const inv=d.pending_invoices||[];
     if(inv.length){{
      let t='<table class="tbl" style="width:100%"><thead><tr><th>Invoice</th><th>Customer</th><th style="text-align:right">Total</th><th style="text-align:right">Balance</th><th>Due</th><th>Days Out</th><th>Status</th></tr></thead><tbody>';
      inv.forEach(i=>{{
       const st=i.status==='overdue'?'<span style="color:var(--rd);font-weight:600">⚠️ OVERDUE</span>':'<span style="color:var(--yl)">Open</span>';
       t+='<tr><td class="mono">'+i.doc_number+'</td><td>'+i.customer+'</td><td style="text-align:right;font-weight:600" class="mono">$'+i.total.toLocaleString()+'</td><td style="text-align:right;color:var(--yl)" class="mono">$'+i.balance.toLocaleString()+'</td><td class="mono">'+i.due_date+'</td><td style="text-align:center">'+i.days_outstanding+'</td><td>'+st+'</td></tr>';
      }});
      t+='</tbody></table>';
      document.getElementById('inv-table').innerHTML=t;
     }} else {{
      document.getElementById('inv-table').innerHTML='<div style="color:var(--tx2);text-align:center;padding:12px">No pending invoices</div>';
     }}
    }}).catch(()=>{{}});
    </script>"""
    return render(content, title="Orders")


@bp.route("/order/<oid>")
@auth_required
def order_detail(oid):
    """Order detail page — line item sourcing, tracking, invoicing."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        flash(f"Order {oid} not found", "error")
        return redirect("/orders")

    st = order.get("status", "new")
    items = order.get("line_items", [])
    qn = order.get("quote_number", "")
    institution = order.get("institution", "")

    sourcing_cfg = {
        "pending":   ("⏳ Pending",   "#d29922", "rgba(210,153,34,.1)"),
        "ordered":   ("🛒 Ordered",   "#58a6ff", "rgba(88,166,255,.1)"),
        "shipped":   ("🚚 Shipped",   "#bc8cff", "rgba(188,140,255,.1)"),
        "delivered": ("✅ Delivered", "#3fb950", "rgba(52,211,153,.1)"),
    }
    inv_cfg = {
        "pending":  ("⏳", "#d29922"),
        "partial":  ("½", "#58a6ff"),
        "invoiced": ("✅", "#3fb950"),
    }

    # Line items table
    items_rows = ""
    for it in items:
        lid = it.get("line_id", "")
        desc = it.get("description", "")[:80]
        pn = it.get("part_number", "")
        sup_url = it.get("supplier_url", "")
        sup_link = f'<a href="{sup_url}" target="_blank" style="color:var(--ac);font-size:11px">🛒 {it.get("supplier","Amazon")}</a>' if sup_url else (it.get("supplier","") or "—")

        ss = it.get("sourcing_status", "pending")
        s_lbl, s_clr, s_bg = sourcing_cfg.get(ss, sourcing_cfg["pending"])
        tracking = it.get("tracking_number", "")
        tracking_html = f'<a href="https://track.aftership.com/{tracking}" target="_blank" style="color:var(--ac);font-size:10px">{tracking[:20]}</a>' if tracking else ""
        carrier = it.get("carrier", "")

        is_lbl, is_clr = inv_cfg.get(it.get("invoice_status","pending"), inv_cfg["pending"])

        items_rows += f"""<tr data-lid="{lid}">
         <td style="color:var(--tx2);font-size:11px">{lid}</td>
         <td style="max-width:300px;word-wrap:break-word;white-space:normal">{desc}</td>
         <td class="mono" style="font-size:11px">{pn or '—'}</td>
         <td>{sup_link}</td>
         <td class="mono" style="text-align:center">{it.get('qty',0)}</td>
         <td class="mono" style="text-align:right">${it.get('unit_price',0):,.2f}</td>
         <td style="text-align:center">
          <select onchange="updateLine('{oid}','{lid}','sourcing_status',this.value)" style="background:var(--sf);border:1px solid var(--bd);border-radius:4px;color:{s_clr};font-size:11px;padding:2px">
           <option value="pending" {"selected" if ss=="pending" else ""}>⏳ Pending</option>
           <option value="ordered" {"selected" if ss=="ordered" else ""}>🛒 Ordered</option>
           <option value="shipped" {"selected" if ss=="shipped" else ""}>🚚 Shipped</option>
           <option value="delivered" {"selected" if ss=="delivered" else ""}>✅ Delivered</option>
          </select>
         </td>
         <td style="font-size:10px">{carrier} {tracking_html}</td>
         <td style="text-align:center;font-size:12px;color:{is_clr}" title="{it.get('invoice_status','pending')}">{is_lbl}</td>
        </tr>"""

    status_cfg = {
        "new": "🆕 New", "sourcing": "🛒 Sourcing", "shipped": "🚚 Shipped",
        "partial_delivery": "📦 Partial Delivery", "delivered": "✅ Delivered",
        "invoiced": "💰 Invoiced", "closed": "🏁 Closed"
    }

    content = f"""
    <div style="display:flex;gap:10px;align-items:center;margin-bottom:16px">
     <a href="/orders" class="btn btn-s" style="font-size:13px">← Orders</a>
     {f'<a href="/quote/{qn}" class="btn btn-s" style="font-size:13px">📋 Quote {qn}</a>' if qn else ''}
    </div>

    <div class="bento bento-2" style="margin-bottom:14px">
     <div class="card" style="margin:0">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px">
       <div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:700">{oid}</div>
        <div style="color:var(--tx2);font-size:12px;margin-top:4px">{order.get('agency','')}{' · ' if order.get('agency') else ''}Created {order.get('created_at','')[:10]}</div>
       </div>
       <span style="padding:4px 12px;border-radius:12px;font-size:12px;font-weight:600;background:var(--sf2)">{status_cfg.get(st, st)}</span>
      </div>
      <div class="meta-g" style="margin:0">
       <div class="meta-i"><div class="meta-l">Institution</div><div class="meta-v">{institution}</div></div>
       <div class="meta-i"><div class="meta-l">PO Number</div><div class="meta-v" style="color:var(--gn);font-weight:600">{order.get('po_number','—')}</div></div>
       <div class="meta-i"><div class="meta-l">Quote</div><div class="meta-v">{qn or '—'}</div></div>
       <div class="meta-i"><div class="meta-l">Items</div><div class="meta-v">{len(items)}</div></div>
      </div>
     </div>
     <div class="card" style="margin:0">
      <div style="text-align:center;padding:12px 0">
       <div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px">Order Total</div>
       <div style="font-family:'JetBrains Mono',monospace;font-size:32px;font-weight:700;color:var(--gn);margin:8px 0">${order.get('total',0):,.2f}</div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-top:10px">
       <div style="background:var(--sf2);padding:8px;border-radius:8px;text-align:center">
        <div style="font-size:10px;color:var(--tx2)">Sourced</div>
        <div style="font-size:18px;font-weight:700;color:#58a6ff">{sum(1 for i in items if i.get('sourcing_status') in ('ordered','shipped','delivered'))}/{len(items)}</div>
       </div>
       <div style="background:var(--sf2);padding:8px;border-radius:8px;text-align:center">
        <div style="font-size:10px;color:var(--tx2)">Shipped</div>
        <div style="font-size:18px;font-weight:700;color:#bc8cff">{sum(1 for i in items if i.get('sourcing_status') in ('shipped','delivered'))}/{len(items)}</div>
       </div>
       <div style="background:var(--sf2);padding:8px;border-radius:8px;text-align:center">
        <div style="font-size:10px;color:var(--tx2)">Delivered</div>
        <div style="font-size:18px;font-weight:700;color:#3fb950">{sum(1 for i in items if i.get('sourcing_status') == 'delivered')}/{len(items)}</div>
       </div>
      </div>
      <div style="margin-top:12px;display:flex;gap:8px;justify-content:center">
       <button onclick="invoiceOrder('{oid}','partial')" class="btn btn-s" style="font-size:12px">½ Partial Invoice</button>
       <button onclick="invoiceOrder('{oid}','full')" class="btn btn-g" style="font-size:12px">💰 Full Invoice</button>
      </div>
     </div>
    </div>

    <!-- Line Items with sourcing controls -->
    <div class="card">
     <div class="card-t">Line Items — Sourcing & Tracking</div>
     <div style="overflow-x:auto">
     <table class="home-tbl" style="min-width:900px">
      <thead><tr>
       <th style="width:40px">#</th><th>Description</th><th style="width:80px">Part #</th>
       <th style="width:80px">Supplier</th><th style="width:40px;text-align:center">Qty</th>
       <th style="width:80px;text-align:right">Price</th><th style="width:100px;text-align:center">Status</th>
       <th style="width:140px">Tracking</th><th style="width:30px;text-align:center">Inv</th>
      </tr></thead>
      <tbody>{items_rows}</tbody>
     </table>
     </div>
    </div>

    <!-- Bulk actions -->
    <div class="card" style="margin-top:14px">
     <div class="card-t">Quick Actions</div>
     <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button onclick="bulkAddTracking('{oid}')" class="btn btn-s" style="font-size:12px">📋 Bulk Add Tracking</button>
      <button onclick="markAllOrdered('{oid}')" class="btn btn-s" style="font-size:12px">🛒 Mark All Ordered</button>
      <button onclick="markAllDelivered('{oid}')" class="btn btn-s" style="font-size:12px">✅ Mark All Delivered</button>
      <a href="/api/order/{oid}/reply-all" class="btn btn-s" style="font-size:12px">📧 Reply-All Confirmation</a>
     </div>
    </div>

    <script>
    function updateLine(oid, lid, field, value) {{
      fetch('/api/order/' + oid + '/line/' + lid, {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{[field]: value}})
      }}).then(r => r.json()).then(d => {{
        if (!d.ok) alert('Error: ' + (d.error||'unknown'));
        else location.reload();
      }});
    }}

    function invoiceOrder(oid, type) {{
      const num = prompt('Invoice number:');
      if (!num) return;
      fetch('/api/order/' + oid + '/invoice', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{type: type, invoice_number: num}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) location.reload();
        else alert('Error: ' + (d.error||'unknown'));
      }});
    }}

    function markAllOrdered(oid) {{
      fetch('/api/order/' + oid + '/bulk', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{sourcing_status: 'ordered'}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); }});
    }}

    function markAllDelivered(oid) {{
      fetch('/api/order/' + oid + '/bulk', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{sourcing_status: 'delivered'}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); }});
    }}

    function bulkAddTracking(oid) {{
      const tracking = prompt('Tracking number(s) — comma separated for multiple shipments:');
      if (!tracking) return;
      const carrier = prompt('Carrier (UPS/FedEx/USPS/Amazon):', 'Amazon');
      fetch('/api/order/' + oid + '/bulk-tracking', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{tracking: tracking, carrier: carrier}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); }});
    }}
    </script>
    """
    return render(content, title=f"Order {oid}")


# ─── Order API Routes ──────────────────────────────────────────────────────

@bp.route("/api/order/<oid>/line/<lid>", methods=["POST"])
@auth_required
def api_order_update_line(oid, lid):
    """Update a single line item. POST JSON with any fields to update."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
    updated = False
    for it in order.get("line_items", []):
        if it.get("line_id") == lid:
            for field in ("sourcing_status", "tracking_number", "carrier",
                          "ship_date", "delivery_date", "invoice_status",
                          "invoice_number", "supplier", "supplier_url", "notes"):
                if field in data:
                    old_val = it.get(field, "")
                    it[field] = data[field]
                    if field == "sourcing_status" and old_val != data[field]:
                        _log_crm_activity(order.get("quote_number",""), f"line_{data[field]}",
                                          f"Order {oid} line {lid}: {old_val} → {data[field]} — {it.get('description','')[:60]}",
                                          actor="user", metadata={"order_id": oid})
            updated = True
            break
    if not updated:
        return jsonify({"ok": False, "error": "Line item not found"})
    order["updated_at"] = datetime.now().isoformat()
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    return jsonify({"ok": True})


@bp.route("/api/order/<oid>/bulk", methods=["POST"])
@auth_required
def api_order_bulk_update(oid):
    """Bulk update all line items. POST JSON with fields to set on all items."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
    for it in order.get("line_items", []):
        for field in ("sourcing_status", "carrier", "invoice_status"):
            if field in data:
                it[field] = data[field]
    order["updated_at"] = datetime.now().isoformat()
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), "order_bulk_update",
                      f"Order {oid}: bulk update — {data}",
                      actor="user", metadata={"order_id": oid})
    return jsonify({"ok": True})


@bp.route("/api/order/<oid>/bulk-tracking", methods=["POST"])
@auth_required
def api_order_bulk_tracking(oid):
    """Add tracking to all pending/ordered items. POST: {tracking, carrier}"""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
    tracking = data.get("tracking", "")
    carrier = data.get("carrier", "")
    updated = 0
    for it in order.get("line_items", []):
        if it.get("sourcing_status") in ("pending", "ordered"):
            it["tracking_number"] = tracking
            it["carrier"] = carrier
            it["sourcing_status"] = "shipped"
            it["ship_date"] = datetime.now().strftime("%Y-%m-%d")
            updated += 1
    order["updated_at"] = datetime.now().isoformat()
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), "tracking_added",
                      f"Order {oid}: tracking {tracking} ({carrier}) added to {updated} items",
                      actor="user", metadata={"order_id": oid, "tracking": tracking})
    return jsonify({"ok": True, "updated": updated})


@bp.route("/api/order/<oid>/invoice", methods=["POST"])
@auth_required
def api_order_invoice(oid):
    """Create partial or full invoice. POST: {type: 'partial'|'full', invoice_number}"""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
    inv_type = data.get("type", "full")
    inv_num = data.get("invoice_number", "")

    if inv_type == "full":
        # Mark all items as invoiced
        for it in order.get("line_items", []):
            it["invoice_status"] = "invoiced"
            it["invoice_number"] = inv_num
        order["invoice_type"] = "full"
        order["invoice_total"] = order.get("total", 0)
    elif inv_type == "partial":
        # Mark only delivered items as invoiced
        partial_total = 0
        for it in order.get("line_items", []):
            if it.get("sourcing_status") == "delivered":
                it["invoice_status"] = "invoiced"
                it["invoice_number"] = inv_num
                partial_total += it.get("extended", 0)
            elif it.get("sourcing_status") in ("shipped", "ordered"):
                it["invoice_status"] = "partial"
        order["invoice_type"] = "partial"
        order["invoice_total"] = partial_total

    order["invoice_number"] = inv_num
    order["updated_at"] = datetime.now().isoformat()
    order["status_history"].append({
        "status": f"invoice_{inv_type}",
        "timestamp": datetime.now().isoformat(),
        "actor": "user",
        "invoice_number": inv_num,
    })
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), f"invoice_{inv_type}",
                      f"Order {oid}: {inv_type} invoice #{inv_num} — ${order.get('invoice_total',0):,.2f}",
                      actor="user", metadata={"order_id": oid, "invoice": inv_num})
    return jsonify({"ok": True, "invoice_type": inv_type, "invoice_total": order.get("invoice_total", 0)})


@bp.route("/api/order/<oid>/reply-all")
@auth_required
def api_order_reply_all(oid):
    """Generate reply-all confirmation email for the won quote's original thread."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        flash("Order not found", "error")
        return redirect("/orders")

    qn = order.get("quote_number", "")
    institution = order.get("institution", "")
    po_num = order.get("po_number", "")
    total = order.get("total", 0)
    items = order.get("line_items", [])

    items_list = "\n".join(
        f"  - {it.get('description','')[:60]} (Qty {it.get('qty',0)}) — ${it.get('extended',0):,.2f}"
        for it in items[:15]
    )

    subject = f"RE: Reytech Quote {qn}" + (f" — PO {po_num}" if po_num else "") + " — Order Confirmation"
    body = f"""Thank you for your order!

We are pleased to confirm receipt of {"PO " + po_num if po_num else "your order"} for {institution}.

Quote: {qn}
Order Total: ${total:,.2f}
Items ({len(items)}):
{items_list}

We will process your order promptly and provide tracking information as items ship.

Please don't hesitate to reach out with any questions.

Best regards,
Mike Gonzalez
Reytech Inc.
949-229-1575
sales@reytechinc.com"""

    # Store as draft and redirect to a mailto link
    mailto = f"mailto:?subject={subject}&body={body}".replace("\n", "%0A").replace(" ", "%20")

    _log_crm_activity(qn, "email_sent", f"Order confirmation reply-all for {oid}",
                      actor="user", metadata={"order_id": oid})

    return redirect(mailto)


# ═══════════════════════════════════════════════════════════════════════
# Pipeline Dashboard (Phase 20)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/pipeline")
@auth_required
def pipeline_page():
    """Autonomous pipeline dashboard — full funnel visibility."""
    quotes = [q for q in get_all_quotes() if not q.get("is_test")]
    orders = {k: v for k, v in _load_orders().items() if not v.get("is_test")}
    crm = _load_crm_activity()
    leads = []
    try:
        import json as _json
        with open(os.path.join(DATA_DIR, "leads.json")) as f:
            leads = _json.load(f)
    except Exception:
        pass

    # ── Funnel Counts ──
    total_leads = len(leads)
    total_quotes = len(quotes)
    sent = sum(1 for q in quotes if q.get("status") in ("sent",))
    pending = sum(1 for q in quotes if q.get("status") in ("pending",))
    won = sum(1 for q in quotes if q.get("status") == "won")
    lost = sum(1 for q in quotes if q.get("status") == "lost")
    expired = sum(1 for q in quotes if q.get("status") == "expired")
    total_orders = len(orders)
    invoiced = sum(1 for o in orders.values() if o.get("status") in ("invoiced", "closed"))

    # ── Revenue ──
    total_quoted = sum(q.get("total", 0) for q in quotes)
    total_won = sum(q.get("total", 0) for q in quotes if q.get("status") == "won")
    total_pending = sum(q.get("total", 0) for q in quotes if q.get("status") in ("pending", "sent"))
    total_invoiced = sum(o.get("invoice_total", 0) for o in orders.values())

    # ── Conversion Rates ──
    def rate(a, b): return round(a/b*100) if b > 0 else 0
    lead_to_quote = rate(total_quotes, total_leads) if total_leads else "—"
    quote_to_sent = rate(sent + won + lost, total_quotes) if total_quotes else "—"
    sent_to_won = rate(won, won + lost) if (won + lost) else "—"
    won_to_invoiced = rate(invoiced, total_orders) if total_orders else "—"

    # ── Funnel bars ──
    max_count = max(total_leads, total_quotes, 1)
    def bar(count, color, label, sublabel=""):
        pct = max(5, round(count / max_count * 100))
        return f"""<div style="margin-bottom:8px">
         <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
          <span style="font-size:12px;font-weight:600">{label}</span>
          <span style="font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;color:{color}">{count}</span>
         </div>
         <div style="background:var(--sf2);border-radius:6px;height:24px;overflow:hidden">
          <div style="width:{pct}%;height:100%;background:{color};border-radius:6px;display:flex;align-items:center;padding-left:8px">
           <span style="font-size:10px;color:#fff;font-weight:600">{sublabel}</span>
          </div>
         </div>
        </div>"""

    funnel = (
        bar(total_leads, "#58a6ff", "🔍 Leads (SCPRS)", f"{total_leads} opportunities") +
        bar(total_quotes, "#bc8cff", "📋 Quotes Generated", f"${total_quoted:,.0f} total") +
        bar(sent + won + lost, "#d29922", "📤 Sent to Buyer", f"{sent} active") +
        bar(won, "#3fb950", "✅ Won", f"${total_won:,.0f} revenue") +
        bar(total_orders, "#58a6ff", "📦 Orders", f"{total_orders} active") +
        bar(invoiced, "#3fb950", "💰 Invoiced", f"${total_invoiced:,.0f}")
    )

    # ── Recent CRM events ──
    recent = sorted(crm, key=lambda e: e.get("timestamp", ""), reverse=True)[:15]
    evt_icons = {
        "quote_won": "✅", "quote_lost": "❌", "quote_sent": "📤", "quote_generated": "📋",
        "order_created": "📦", "voice_call": "📞", "email_sent": "📧", "note": "📝",
        "shipping_detected": "🚚", "invoice_full": "💰", "invoice_partial": "½",
    }
    events_html = ""
    for e in recent:
        icon = evt_icons.get(e.get("event_type", ""), "●")
        ts = e.get("timestamp", "")[:16].replace("T", " ")
        events_html += f"""<div style="padding:6px 0;border-bottom:1px solid var(--bd);font-size:12px;display:flex;gap:8px;align-items:flex-start">
         <span style="flex-shrink:0">{icon}</span>
         <div style="flex:1"><div>{e.get('description','')[:100]}</div><div style="color:var(--tx2);font-size:10px;margin-top:2px">{ts}</div></div>
        </div>"""

    # ── Prediction leaderboard for pending quotes ──
    predictions_html = ""
    if PREDICT_AVAILABLE:
        preds = []
        for q in quotes:
            if q.get("status") in ("pending", "sent"):
                p = predict_win_probability(
                    institution=q.get("institution", ""),
                    agency=q.get("agency", ""),
                    po_value=q.get("total", 0),
                )
                preds.append({**q, "win_prob": p["probability"], "rec": p["recommendation"]})
        preds.sort(key=lambda x: x["win_prob"], reverse=True)
        for q in preds[:10]:
            prob = round(q["win_prob"] * 100)
            clr = "#3fb950" if prob >= 60 else ("#d29922" if prob >= 40 else "#f85149")
            predictions_html += f"""<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--bd);font-size:12px">
             <span style="font-family:'JetBrains Mono',monospace;font-weight:700;color:{clr};min-width:36px">{prob}%</span>
             <a href="/quote/{q.get('quote_number','')}" style="color:var(--ac);text-decoration:none;font-weight:600">{q.get('quote_number','')}</a>
             <span style="color:var(--tx2);flex:1">{q.get('institution','')[:30]}</span>
             <span style="font-family:'JetBrains Mono',monospace">${q.get('total',0):,.0f}</span>
            </div>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
     <h2 style="margin:0;font-size:20px;font-weight:700">🔄 Pipeline Dashboard</h2>
     <div style="font-size:12px;font-family:'JetBrains Mono',monospace;display:flex;gap:16px">
      <span>📊 Leads: <b>{total_leads}</b></span>
      <span>📋 Quotes: <b>{total_quotes}</b></span>
      <span style="color:var(--gn)">💰 Pipeline: <b>${total_pending:,.0f}</b></span>
      <span style="color:var(--gn)">🏆 Won: <b>${total_won:,.0f}</b></span>
     </div>
    </div>

    <div class="bento bento-2">
     <div class="card" style="margin:0">
      <div class="card-t">📊 Sales Funnel</div>
      {funnel}
      <div style="margin-top:14px;padding-top:10px;border-top:1px solid var(--bd);display:grid;grid-template-columns:repeat(4,1fr);gap:8px">
       <div style="text-align:center;background:var(--sf2);padding:8px;border-radius:8px">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Lead→Quote</div>
        <div style="font-size:16px;font-weight:700">{lead_to_quote}{'%' if isinstance(lead_to_quote,int) else ''}</div>
       </div>
       <div style="text-align:center;background:var(--sf2);padding:8px;border-radius:8px">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Quote→Sent</div>
        <div style="font-size:16px;font-weight:700">{quote_to_sent}{'%' if isinstance(quote_to_sent,int) else ''}</div>
       </div>
       <div style="text-align:center;background:var(--sf2);padding:8px;border-radius:8px">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Win Rate</div>
        <div style="font-size:16px;font-weight:700;color:var(--gn)">{sent_to_won}{'%' if isinstance(sent_to_won,int) else ''}</div>
       </div>
       <div style="text-align:center;background:var(--sf2);padding:8px;border-radius:8px">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Won→Invoiced</div>
        <div style="font-size:16px;font-weight:700">{won_to_invoiced}{'%' if isinstance(won_to_invoiced,int) else ''}</div>
       </div>
      </div>
     </div>

     <div class="card" style="margin:0">
      <div class="card-t">⏱️ Recent Activity</div>
      <div style="max-height:400px;overflow-y:auto">
       {events_html if events_html else '<div style="color:var(--tx2);font-size:12px;padding:12px">No activity yet</div>'}
      </div>
     </div>
    </div>

    {'<div class="card" style="margin-top:14px"><div class="card-t">🎯 Win Prediction Leaderboard — Active Quotes</div><div style="max-height:320px;overflow-y:auto">' + predictions_html + '</div></div>' if predictions_html else ''}

    <div class="card" style="margin-top:14px">
     <div class="card-t">⚡ Quick Actions</div>
     <div style="display:flex;gap:8px;flex-wrap:wrap">
      <a href="/quotes?status=pending" class="btn btn-s" style="font-size:12px">📋 Pending Quotes ({pending})</a>
      <a href="/quotes?status=sent" class="btn btn-s" style="font-size:12px">📤 Sent Quotes ({sent})</a>
      <a href="/orders" class="btn btn-s" style="font-size:12px">📦 Active Orders ({total_orders})</a>
      <button onclick="fetch('/api/poll-now').then(r=>r.json()).then(d=>alert(JSON.stringify(d,null,2)))" class="btn btn-p" style="font-size:12px">⚡ Check Inbox</button>
     </div>
    </div>
    """
    # BI Revenue bar (secondary — data layer only)
    try:
        rev = update_revenue_tracker() if INTEL_AVAILABLE else {}
        if rev.get("ok"):
            rv_pct = min(100, rev.get("pct_to_goal", 0))
            rv_closed = rev.get("closed_revenue", 0)
            rv_goal = rev.get("goal", 2000000)
            rv_gap = rev.get("gap_to_goal", 0)
            rv_rate = rev.get("run_rate_annual", 0)
            rv_on = rev.get("on_track", False)
            rv_color = "#3fb950" if rv_pct >= 50 else "#d29922" if rv_pct >= 25 else "#f85149"
            content += f"""
    <div style="margin-top:14px;padding:12px 16px;background:var(--sf);border:1px solid var(--bd);border-radius:10px">
     <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px">
      <span style="font-size:11px;color:var(--tx2);font-weight:600">📈 ANNUAL GOAL</span>
      <div style="flex:1;background:var(--sf2);border-radius:8px;height:18px;overflow:hidden;position:relative">
       <div style="background:{rv_color};height:100%;width:{rv_pct}%;border-radius:8px"></div>
       <span style="position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);font-size:10px;font-weight:600">${rv_closed:,.0f} / ${rv_goal/1e6:.0f}M ({rv_pct:.0f}%)</span>
      </div>
      <span style="font-size:11px;color:var(--tx2)">Gap: <b style="color:#f85149">${rv_gap:,.0f}</b></span>
      <span style="font-size:11px;color:var(--tx2)">Run rate: <b style="color:{'#3fb950' if rv_on else '#f85149'}">${rv_rate:,.0f}</b></span>
     </div>
    </div>"""
    except Exception:
        pass
    content += ""
    return render(content, title="Pipeline")


# ═══════════════════════════════════════════════════════════════════════
# Pipeline API (Phase 20)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/pipeline/stats")
@auth_required
def api_pipeline_stats():
    """Full pipeline statistics as JSON."""
    quotes = [q for q in get_all_quotes() if not q.get("is_test")]
    orders = {k: v for k, v in _load_orders().items() if not v.get("is_test")}

    statuses = {}
    for q in quotes:
        s = q.get("status", "pending")
        statuses[s] = statuses.get(s, 0) + 1

    return jsonify({
        "ok": True,
        "quotes": {
            "total": len(quotes),
            "by_status": statuses,
            "total_value": sum(q.get("total", 0) for q in quotes),
            "won_value": sum(q.get("total", 0) for q in quotes if q.get("status") == "won"),
            "pending_value": sum(q.get("total", 0) for q in quotes if q.get("status") in ("pending", "sent")),
        },
        "orders": {
            "total": len(orders),
            "total_value": sum(o.get("total", 0) for o in orders.values()),
            "invoiced_value": sum(o.get("invoice_total", 0) for o in orders.values()),
        },
        "conversion": {
            "win_rate": round(statuses.get("won", 0) / max(statuses.get("won", 0) + statuses.get("lost", 0), 1) * 100, 1),
            "quote_count": len(quotes),
            "decided": statuses.get("won", 0) + statuses.get("lost", 0),
        },
        "annual_goal": update_revenue_tracker() if INTEL_AVAILABLE else None,
    })


@bp.route("/api/pipeline/analyze-reply", methods=["POST"])
@auth_required
def api_analyze_reply():
    """Analyze an email reply for win/loss/question signals.
    POST: {subject, body, sender}"""
    if not REPLY_ANALYZER_AVAILABLE:
        return jsonify({"ok": False, "error": "Reply analyzer not available"})
    data = request.get_json(silent=True) or {}
    quotes = get_all_quotes()
    result = find_quote_from_reply(
        data.get("subject", ""), data.get("body", ""),
        data.get("sender", ""), quotes)
    result["ok"] = True

    # Auto-flag quote if high confidence win/loss
    if result.get("matched_quote") and result.get("confidence", 0) >= 0.6:
        signal = result.get("signal")
        qn = result["matched_quote"]
        if signal == "win":
            _log_crm_activity(qn, "win_signal_detected",
                              f"Email reply signals WIN for {qn} — {result.get('summary', '')}",
                              actor="system", metadata=result)
        elif signal == "loss":
            _log_crm_activity(qn, "loss_signal_detected",
                              f"Email reply signals LOSS for {qn} — {result.get('summary', '')}",
                              actor="system", metadata=result)
        elif signal == "question":
            _log_crm_activity(qn, "question_detected",
                              f"Buyer question detected for {qn} — follow up needed",
                              actor="system", metadata=result)

    return jsonify(result)


# ═══════════════════════════════════════════════════════════════════════
# Predictive Intelligence & Shipping Monitor (Phase 19)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/predict/win")
@auth_required
def api_predict_win():
    """Predict win probability for an institution/agency.
    GET ?institution=CSP-Sacramento&agency=CDCR&value=5000"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive module not available"})
    inst = request.args.get("institution", "")
    agency = request.args.get("agency", "")
    value = float(request.args.get("value", 0) or 0)
    result = predict_win_probability(institution=inst, agency=agency, po_value=value)
    return jsonify({"ok": True, **result})


@bp.route("/api/predict/batch", methods=["POST"])
@auth_required
def api_predict_batch():
    """Batch predict for multiple opportunities. POST JSON: [{institution, agency, value}, ...]"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive module not available"})
    data = request.get_json(silent=True) or []
    results = []
    for opp in data[:50]:
        pred = predict_win_probability(
            institution=opp.get("institution", ""),
            agency=opp.get("agency", ""),
            po_value=opp.get("value", 0),
        )
        results.append({**opp, **pred})
    results.sort(key=lambda r: r.get("probability", 0), reverse=True)
    return jsonify({"ok": True, "predictions": results})


@bp.route("/api/intel/competitors")
@bp.route("/api/competitor/insights")
@auth_required
def api_competitor_insights():
    """Competitor intelligence summary.
    GET ?institution=...&agency=...&limit=20"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive module not available"})
    inst = request.args.get("institution", "")
    agency = request.args.get("agency", "")
    limit = int(request.args.get("limit", 20))
    result = get_competitor_insights(institution=inst, agency=agency, limit=limit)
    return jsonify({"ok": True, **result})


@bp.route("/api/shipping/scan-email", methods=["POST"])
@bp.route("/api/shipping/detect", methods=["POST"])
@auth_required
def api_shipping_scan():
    """Scan an email for shipping/tracking info. POST: {subject, body, sender}"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Shipping monitor not available"})
    data = request.get_json(silent=True) or {}
    tracking_info = detect_shipping_email(
        data.get("subject", ""), data.get("body", ""), data.get("sender", ""))

    if not tracking_info.get("is_shipping"):
        return jsonify({"ok": True, "is_shipping": False})

    # Try to match to an order
    orders = _load_orders()
    matched_oid = match_tracking_to_order(tracking_info, orders)
    result = {"ok": True, **tracking_info, "matched_order": matched_oid}

    # Auto-update order if matched
    if matched_oid:
        update = update_order_from_tracking(matched_oid, tracking_info, orders)
        _save_orders(orders)
        _update_order_status(matched_oid)
        result["update"] = update
        _log_crm_activity(matched_oid, "shipping_detected",
                          f"Shipping email detected — {tracking_info.get('carrier','')} "
                          f"tracking {', '.join(tracking_info.get('tracking_numbers',[])) or 'N/A'} — "
                          f"status: {tracking_info.get('delivery_status','')}",
                          actor="system", metadata=tracking_info)

    return jsonify(result)


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
def api_test_cleanup():
    """Remove all test records and optionally reset quote counter."""
    reset_counter = request.args.get("reset_counter", "false").lower() == "true"

    # Clean PCs
    pcs = _load_price_checks()
    test_pcs = [k for k, v in pcs.items() if v.get("is_test")]
    for k in test_pcs:
        del pcs[k]
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
        fetch_vendors, find_vendor, create_purchase_order,
        get_recent_purchase_orders, get_agent_status as qb_agent_status,
        is_configured as qb_configured,
        fetch_invoices, get_invoice_summary, create_invoice,
        fetch_customers, find_customer, get_customer_balance_summary,
        get_financial_context,
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
    try:
        start_qa_monitor()
    except Exception:
        pass
except ImportError:
    QA_AVAILABLE = False

try:
    from src.agents.workflow_tester import (
        run_workflow_tests, get_latest_run as get_latest_wf_run,
        get_run_history as get_wf_history, start_workflow_monitor,
    )
    start_workflow_monitor()
    _WF_AVAILABLE = True
except Exception:
    _WF_AVAILABLE = False


@bp.route("/api/identify", methods=["POST"])
@auth_required
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

    return jsonify({"ok": True, "agents": agents,
                    "total": len(agents),
                    "active": sum(1 for a in agents.values() if a.get("status") != "not_available")})


@bp.route("/api/qa/workflow", methods=["GET","POST"])
@auth_required
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
def api_qa_workflow_latest():
    if not _WF_AVAILABLE:
        return jsonify({"ok": False, "error": "workflow_tester not available"}), 503
    return jsonify(get_latest_wf_run())


@bp.route("/api/qa/workflow/history")
@auth_required
def api_qa_workflow_history():
    if not _WF_AVAILABLE:
        return jsonify({"ok": False, "error": "workflow_tester not available"}), 503
    n = int(request.args.get("n", 20))
    return jsonify(get_wf_history(n))


@bp.route("/qa/workflow")
@auth_required
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
.badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px}
.pass{background:rgba(52,211,153,.15);color:var(--gn)}.warn{background:rgba(251,191,36,.15);color:var(--yl)}.fail{background:rgba(248,113,113,.15);color:var(--rd)}
.fix{font-size:11px;color:var(--yl);margin-top:4px;font-style:italic}
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
    '<div style="font-size:12px;color:var(--tx2)">'+d.summary.passed+' pass · '+d.summary.warned+' warn · '+d.summary.failed+' fail · '+d.duration_s+'s</div>'+
    '<div style="font-size:11px;color:var(--tx2);margin-top:4px">Last run: '+new Date(d.run_at).toLocaleString()+'</div>';
  var html='<div class="card"><div style="font-weight:600;margin-bottom:8px">Test Results</div>';
  (d.results||[]).forEach(function(r){
    var icon=r.status==='pass'?'✅':r.status==='warn'?'⚠️':'❌';
    html+='<div class="row"><span style="font-size:18px">'+icon+'</span>'+
      '<div style="flex:1"><div style="font-size:13px;font-weight:600">'+r.test+'<span class="badge '+r.status+'" style="margin-left:8px">'+r.status+'</span></div>'+
      '<div style="font-size:12px;color:var(--tx2);margin-top:2px">'+r.message+'</div>'+
      (r.detail?'<div style="font-size:11px;color:var(--tx2);margin-top:2px;font-family:monospace">'+r.detail+'</div>':'')+
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
        '<div style="flex:1"><div style="font-size:12px;color:var(--tx2)">'+new Date(r.run_at).toLocaleString()+'</div>'+
        (fails.length?'<div style="font-size:11px;color:var(--rd);margin-top:2px">'+fails[0]+'</div>':'<div style="font-size:11px;color:var(--gn)">All clear</div>')+'</div>'+
        '<div style="font-size:11px;color:var(--tx2)">'+r.passed+'P '+r.warned+'W '+r.failed+'F</div>'+
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
    except Exception:
        pass
    return jsonify({"ok": True, **report})


@bp.route("/api/qa/history")
@auth_required
def api_qa_history():
    """Get QA report history."""
    if not QA_AVAILABLE:
        return jsonify({"ok": False, "error": "QA agent not available"})
    limit = int(request.args.get("limit", 20))
    return jsonify({"ok": True, "reports": get_qa_history(limit)})


@bp.route("/api/qa/trend")
@auth_required
def api_qa_trend():
    """Health score trend over time."""
    if not QA_AVAILABLE:
        return jsonify({"ok": False, "error": "QA agent not available"})
    return jsonify({"ok": True, **get_health_trend()})


# ─── Manager Brief Routes ───────────────────────────────────────────────────

@bp.route("/api/manager/brief/debug")
@auth_required
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
            results[name] = {"ok": False, "error": str(e), "trace": traceback.format_exc()[-500:]}
    try:
        generate_brief()
        results["generate_brief"] = {"ok": True}
    except Exception as e:
        results["generate_brief"] = {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}
    all_ok = all(v.get("ok") for v in results.values())
    return jsonify({"ok": all_ok, "results": results})


_brief_cache = {"data": None, "ts": 0}
_BRIEF_TTL = 30  # seconds

@bp.route("/api/manager/brief")
@auth_required
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
            except (ValueError, TypeError):
                pass

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
                except (ValueError, TypeError):
                    pass
        label = f"Week {4-w}" if w > 0 else "This Week"
        weekly_volume.append({"label": label, "quotes": count, "value": round(value, 2)})
    weekly_volume.reverse()

    # Win rate trend (rolling - all time)
    decided = len(won) + len(lost)
    win_rate = round(len(won) / max(decided, 1) * 100)

    # Average deal size
    avg_deal = round(total_revenue / max(len(won), 1), 2)

    # Pipeline funnel
    pc_count = len(pcs) if isinstance(pcs, dict) else 0
    pc_parsed = sum(1 for p in (pcs.values() if isinstance(pcs, dict) else []) if p.get("status") == "parsed")
    pc_priced = sum(1 for p in (pcs.values() if isinstance(pcs, dict) else []) if p.get("status") == "priced")
    pc_completed = sum(1 for p in (pcs.values() if isinstance(pcs, dict) else []) if p.get("status") == "completed")

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
            except (ValueError, TypeError):
                pass
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
def api_workflow_run():
    """Execute a named workflow pipeline."""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({"ok": False, "error": "Orchestrator not available"})
    data = request.json or {}
    name = data.get("workflow", "")
    inputs = data.get("inputs", {})
    if not name:
        return jsonify({"ok": False, "error": "Missing 'workflow' field"})
    result = run_workflow(name, inputs)
    return jsonify({"ok": not bool(result.get("error")), **result})


@bp.route("/api/workflow/status")
@auth_required
def api_workflow_status():
    """Orchestrator status and run history."""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({"ok": False, "error": "Orchestrator not available"})
    return jsonify({"ok": True, **get_workflow_status()})


@bp.route("/api/workflow/graph/<name>")
@auth_required
def api_workflow_graph(n):
    """Get workflow graph structure for visualization."""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({"ok": False, "error": "Orchestrator not available"})
    return jsonify({"ok": True, **get_workflow_graph_viz(n)})


# ─── SCPRS Scanner Routes ───────────────────────────────────────────────────

@bp.route("/api/scanner/start", methods=["POST"])
@auth_required
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
def api_scanner_stop():
    """Stop the SCPRS opportunity scanner."""
    if not SCANNER_AVAILABLE:
        return jsonify({"ok": False, "error": "Scanner not available"})
    stop_scanner()
    return jsonify({"ok": True, "status": get_scanner_status()})


@bp.route("/api/scanner/scan", methods=["POST"])
@auth_required
def api_scanner_manual():
    """Run a single scan manually."""
    if not SCANNER_AVAILABLE:
        return jsonify({"ok": False, "error": "Scanner not available"})
    results = manual_scan()
    return jsonify({"ok": True, **results})


@bp.route("/api/scanner/status")
@auth_required
def api_scanner_status():
    """Get scanner status."""
    if not SCANNER_AVAILABLE:
        return jsonify({"ok": False, "error": "Scanner not available"})
    return jsonify({"ok": True, **get_scanner_status()})


# ─── QuickBooks Routes ──────────────────────────────────────────────────────

@bp.route("/api/qb/connect")
@auth_required
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
        # Also save realm_id to env for future use
        os.environ["QB_REALM_ID"] = realm_id or ""
        flash(f"QuickBooks connected! Realm: {realm_id}", "success")
        _log_crm_activity("system", "qb_connected", f"QuickBooks Online connected (realm {realm_id})", actor="user")
    except Exception as e:
        flash(f"QB OAuth error: {e}", "error")
    return redirect("/agents")


@bp.route("/api/qb/status")
@auth_required
def api_qb_status():
    """QuickBooks connection status."""
    if not QB_AVAILABLE:
        return jsonify({"ok": False, "error": "QuickBooks agent not available"})
    return jsonify({"ok": True, **qb_agent_status()})


@bp.route("/api/qb/vendors")
@auth_required
def api_qb_vendors():
    """List QuickBooks vendors."""
    if not QB_AVAILABLE:
        return jsonify({"ok": False, "error": "QuickBooks agent not available"})
    if not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured. Set QB_CLIENT_ID, QB_CLIENT_SECRET, QB_REFRESH_TOKEN, QB_REALM_ID"})
    force = request.args.get("refresh", "").lower() in ("true", "1")
    vendors = fetch_vendors(force_refresh=force)
    return jsonify({"ok": True, "vendors": vendors, "count": len(vendors)})


@bp.route("/api/qb/vendors/find")
@auth_required
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


@bp.route("/api/qb/po/create", methods=["POST"])
@auth_required
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
def api_qb_recent_pos():
    """Get recent Purchase Orders from QuickBooks."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    days = int(request.args.get("days", 30))
    pos = get_recent_purchase_orders(days_back=days)
    return jsonify({"ok": True, "purchase_orders": pos, "count": len(pos)})


@bp.route("/api/qb/invoices")
@auth_required
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
def api_qb_invoice_summary():
    """Get invoice metrics: open, overdue, paid counts and totals."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    return jsonify({"ok": True, **get_invoice_summary()})


@bp.route("/api/qb/invoices/create", methods=["POST"])
@auth_required
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
def api_qb_customers():
    """List QuickBooks customers with balances."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    force = request.args.get("refresh", "").lower() in ("true", "1")
    customers = fetch_customers(force_refresh=force)
    return jsonify({"ok": True, "customers": customers, "count": len(customers)})


@bp.route("/api/qb/customers/balances")
@auth_required
def api_qb_customer_balances():
    """Customer balance summary: total AR, top balances."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    return jsonify({"ok": True, **get_customer_balance_summary()})


@bp.route("/api/qb/financial-context")
@auth_required
def api_qb_financial_context():
    """Comprehensive financial snapshot for all agents.
    Pulls invoices, customers, vendors — cached 1 hour."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    force = request.args.get("refresh", "").lower() in ("true", "1")
    ctx = get_financial_context(force_refresh=force)
    return jsonify(ctx)


# ─── CRM Activity Routes (Phase 16) ───────────────────────────────────────

@bp.route("/api/crm/activity")
@auth_required
def api_crm_activity():
    """Get CRM activity feed. ?ref_id=R26Q1&type=quote_won&institution=CSP&limit=50"""
    ref_id = request.args.get("ref_id")
    event_type = request.args.get("type")
    institution = request.args.get("institution")
    limit = int(request.args.get("limit", 50))
    activity = _get_crm_activity(ref_id=ref_id, event_type=event_type,
                                  institution=institution, limit=limit)
    return jsonify({"ok": True, "activity": activity, "count": len(activity)})


@bp.route("/api/crm/activity", methods=["POST"])
@auth_required
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
    """Load persisted CRM contact enhancements (manual fields + activity)."""
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
    except Exception:
        pass

def _get_or_create_crm_contact(prospect_id: str, prospect: dict = None) -> dict:
    """Get or create a CRM contact record, merging SCPRS intel data."""
    contacts = _load_crm_contacts()
    if prospect_id not in contacts:
        pr = prospect or {}
        contacts[prospect_id] = {
            "id": prospect_id,
            "created_at": datetime.now().isoformat(),
            "buyer_name": pr.get("buyer_name",""),
            "buyer_email": pr.get("buyer_email",""),
            "buyer_phone": pr.get("buyer_phone",""),
            "agency": pr.get("agency",""),
            "title": "",
            "linkedin": "",
            "notes": "",
            "tags": [],
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
    except Exception:
        pass

    # Auto-update prospect status on meaningful interactions
    if GROWTH_AVAILABLE and event_type in ("email_sent","voice_called","chat","meeting"):
        try:
            update_prospect(contact_id, {"outreach_status": "emailed" if event_type=="email_sent" else "called"})
        except Exception:
            pass

    return jsonify({"ok": True, "entry": entry, "contact_id": contact_id})


@bp.route("/api/crm/contact/<contact_id>")
@auth_required
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
            except Exception:
                pass
    contact = contacts.get(contact_id)
    if not contact:
        return jsonify({"ok": False, "error": "Contact not found"})
    # Merge global CRM activity
    global_events = _get_crm_activity(ref_id=contact_id, limit=200)
    contact["global_activity"] = global_events
    return jsonify({"ok": True, "contact": contact})


@bp.route("/api/crm/contact/<contact_id>", methods=["PATCH"])
@auth_required
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
            except Exception:
                pass
    if contact_id not in contacts:
        return jsonify({"ok": False, "error": "Contact not found"})

    allowed = {"buyer_name","buyer_phone","title","linkedin","notes","tags","outreach_status"}
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
        except Exception:
            pass
    return jsonify({"ok": True, "contact_id": contact_id})


@bp.route("/api/crm/contacts")
@auth_required
def api_crm_contacts_list():
    """List all CRM contacts with activity counts and last interaction."""
    contacts = _load_crm_contacts()
    result = []
    for cid, c in contacts.items():
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
        })
    result.sort(key=lambda x: x.get("last_activity",""), reverse=True)
    return jsonify({"ok": True, "contacts": result, "total": len(result)})


@bp.route("/api/crm/sync-intel", methods=["POST"])
@auth_required
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
def api_shipping_detected():
    """Get recently detected shipping emails."""
    ship_file = os.path.join(DATA_DIR, "detected_shipments.json")
    try:
        with open(ship_file) as f:
            shipments = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        shipments = []
    limit = int(request.args.get("limit", 20))
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
def api_funnel_stats():
    """Pipeline funnel stats — aggregated view of the full business pipeline."""
    # RFQs (exclude test)
    rfqs = load_rfqs()
    rfqs_active = sum(1 for r in rfqs.values()
                      if r.get("status") not in ("completed", "won", "lost") and not r.get("is_test"))

    # Quotes (exclude test)
    quotes = [q for q in get_all_quotes() if not q.get("is_test")]
    quotes_pending = sum(1 for q in quotes if q.get("status") in ("pending", "draft"))
    quotes_sent = sum(1 for q in quotes if q.get("status") == "sent")
    quotes_won = sum(1 for q in quotes if q.get("status") == "won")
    quotes_lost = sum(1 for q in quotes if q.get("status") == "lost")
    total_quoted = sum(q.get("total", 0) for q in quotes)
    total_won = sum(q.get("total", 0) for q in quotes if q.get("status") == "won")

    # Orders (exclude test)
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

    # Leads
    try:
        with open(os.path.join(DATA_DIR, "leads.json")) as f:
            leads = json.load(f)
        leads_count = len(leads) if isinstance(leads, list) else 0
        hot_leads = sum(1 for l in (leads if isinstance(leads, list) else [])
                        if isinstance(l, dict) and l.get("score", 0) >= 0.7)
    except (FileNotFoundError, json.JSONDecodeError):
        leads_count = 0
        hot_leads = 0

    # Win rate
    decided = quotes_won + quotes_lost
    win_rate = round(quotes_won / decided * 100) if decided > 0 else 0

    # Pipeline value = pending + sent quote totals
    pipeline_value = sum(q.get("total", 0) for q in quotes
                         if q.get("status") in ("pending", "sent", "draft"))

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
        except Exception:
            pass

    # Next quote number + CRM stats
    next_quote = ""
    crm_contacts_count = 0
    intel_buyers_count = 0
    try:
        next_quote = peek_next_quote_number() if QUOTE_GEN_AVAILABLE else ""
    except Exception:
        pass
    try:
        crm_contacts_count = len(_load_crm_contacts())
    except Exception:
        pass
    if INTEL_AVAILABLE:
        try:
            from src.agents.sales_intel import _load_json as _il, BUYERS_FILE as _BF
            bd = _il(_BF)
            intel_buyers_count = bd.get("total_buyers", 0) if isinstance(bd, dict) else 0
        except Exception:
            pass

    return jsonify({
        "ok": True,
        "next_quote": next_quote,
        "rfqs_active": rfqs_active,
        "quotes_pending": quotes_pending,
        "quotes_sent": quotes_sent,
        "quotes_won": quotes_won,
        "quotes_lost": quotes_lost,
        "orders_active": orders_active,
        "orders_total": orders_total,
        "items_shipped": items_shipped,
        "items_delivered": items_delivered,
        "leads_count": leads_count,
        "hot_leads": hot_leads,
        "total_quoted": total_quoted,
        "total_won": total_won,
        "pipeline_value": pipeline_value,
        "order_value": order_value,
        "invoiced_value": invoiced_value,
        "win_rate": win_rate,
        "crm_contacts": crm_contacts_count,
        "intel_buyers": intel_buyers_count,
        "qb_receivable": qb_receivable,
        "qb_overdue": qb_overdue,
        "qb_collected": qb_collected,
        "qb_open_invoices": qb_open_invoices,
        # PRD Feature 4.4 — weighted pipeline (probability-adjusted)
        "weighted_pipeline": _get_weighted_pipeline_cached(),
    })



@bp.route("/api/leads")
@auth_required
def api_leads_list():
    """Get leads, optionally filtered. ?status=new&min_score=0.6&limit=20"""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    status = request.args.get("status")
    min_score = float(request.args.get("min_score", 0))
    limit = int(request.args.get("limit", 50))
    leads = get_leads(status=status, min_score=min_score, limit=limit)
    return jsonify({"ok": True, "leads": leads, "count": len(leads)})


@bp.route("/api/leads/evaluate", methods=["POST"])
@auth_required
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
def api_leads_update_status(lead_id):
    """Update lead status. POST JSON: {"status": "contacted", "notes": "..."}"""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    data = request.get_json(silent=True) or {}
    return jsonify(update_lead_status(
        lead_id, data.get("status", ""), data.get("notes", "")))


@bp.route("/api/leads/<lead_id>/draft")
@auth_required
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
def api_leads_analytics():
    """Lead conversion analytics."""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    return jsonify({"ok": True, **get_lead_analytics()})


# ─── Email Outreach Routes ──────────────────────────────────────────────────

@bp.route("/api/outbox")
@auth_required
def api_outbox_list():
    """Get email outbox. ?status=draft"""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    status = request.args.get("status")
    emails = get_outbox(status=status)
    return jsonify({"ok": True, "emails": emails, "count": len(emails)})


@bp.route("/api/outbox/draft/pc/<pcid>", methods=["POST"])
@auth_required
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
def api_outbox_approve(email_id):
    """Approve a draft email for sending."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify(approve_email(email_id))


@bp.route("/api/outbox/<email_id>/edit", methods=["POST"])
@auth_required
def api_outbox_edit(email_id):
    """Edit a draft. POST JSON: {"to": "...", "subject": "...", "body": "..."}"""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    data = request.get_json(silent=True) or {}
    return jsonify(update_draft(email_id, data))


@bp.route("/api/outbox/<email_id>/send", methods=["POST"])
@auth_required
def api_outbox_send(email_id):
    """Send a specific email."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify(outreach_send(email_id))


@bp.route("/api/outbox/send-approved", methods=["POST"])
@auth_required
def api_outbox_send_all():
    """Send all approved emails."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify({"ok": True, **send_approved()})


@bp.route("/api/outbox/<email_id>", methods=["DELETE"])
@auth_required
def api_outbox_delete(email_id):
    """Delete an email from outbox."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify(delete_from_outbox(email_id))


@bp.route("/api/outbox/sent")
@auth_required
def api_outbox_sent_log():
    """Get sent email log."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    limit = int(request.args.get("limit", 50))
    return jsonify({"ok": True, "sent": get_sent_log(limit=limit)})


# ─── Growth Strategy Routes (v2.0 — SCPRS-driven) ──────────────────────────

@bp.route("/growth")
@auth_required
def growth_page():
    """Growth Engine Dashboard — full funnel view."""
    if not GROWTH_AVAILABLE:
        flash("Growth agent not available", "error")
        return redirect("/")
    from src.agents.growth_agent import (
        get_growth_status, PULL_STATUS, BUYER_STATUS,
        HISTORY_FILE, CATEGORIES_FILE, PROSPECTS_FILE, OUTREACH_FILE,
        _load_json,
    )
    st = get_growth_status()
    h = st.get("history", {})
    c = st.get("categories", {})
    p = st.get("prospects", {})
    o = st.get("outreach", {})
    pull = st.get("pull_status", {})
    buyer = st.get("buyer_status", {})

    # Load prospect details for table
    prospect_data = _load_json(PROSPECTS_FILE)
    prospects = prospect_data.get("prospects", []) if isinstance(prospect_data, dict) else []

    # Load outreach details + campaign metrics
    outreach_data = _load_json(OUTREACH_FILE)
    campaigns = outreach_data.get("campaigns", []) if isinstance(outreach_data, dict) else []
    total_emailed = sum(1 for c_ in campaigns for o_ in c_.get("outreach", []) if o_.get("email_sent"))
    total_bounced = sum(1 for c_ in campaigns for o_ in c_.get("outreach", []) if o_.get("bounced"))
    total_responded = sum(1 for c_ in campaigns for o_ in c_.get("outreach", []) if o_.get("response_received"))
    total_called = sum(1 for c_ in campaigns for o_ in c_.get("outreach", []) if o_.get("voice_called"))
    total_no_response = total_emailed - total_bounced - total_responded - total_called

    # Category summary
    cat_data = _load_json(CATEGORIES_FILE)
    cat_rows = ""
    if isinstance(cat_data, dict) and cat_data.get("categories"):
        for cat_name, info in sorted(cat_data["categories"].items(), key=lambda x: x[1].get("total_value", 0), reverse=True):
            cat_rows += f"""<tr>
             <td style="font-weight:600">{cat_name}</td>
             <td class="mono">{info.get('item_count', 0)}</td>
             <td class="mono">{info.get('po_count', 0)}</td>
             <td class="mono" style="color:#3fb950">${info.get('total_value', 0):,.2f}</td>
             <td style="font-size:11px;color:var(--tx2)">{', '.join(info.get('sample_items', [])[:2])[:80]}</td>
            </tr>"""
    elif prospects:
        # Derive categories from prospect data when SCPRS hasn't been pulled
        cat_agg = {}
        for pr_ in prospects:
            for cat_ in (pr_.get("categories_matched") or []):
                if cat_ not in cat_agg:
                    cat_agg[cat_] = {"spend": 0, "buyers": 0}
                cat_agg[cat_]["spend"] += (pr_.get("total_spend") or 0)
                cat_agg[cat_]["buyers"] += 1
        for cat_name, info_ in sorted(cat_agg.items(), key=lambda x: x[1]["spend"], reverse=True):
            cat_rows += f"""<tr>
             <td style="font-weight:600">{cat_name}</td>
             <td class="mono">{info_['buyers']}</td>
             <td class="mono">—</td>
             <td class="mono" style="color:#3fb950">${info_['spend']:,.0f}</td>
             <td style="font-size:11px;color:var(--tx2)">from prospect data</td>
            </tr>"""

    # Prospect table rows with CRM actions
    prospect_rows = ""
    status_cfg = {
        "new": ("⬜ New", "#d29922", "rgba(210,153,34,.08)"),
        "emailed": ("📧 Emailed", "#58a6ff", "rgba(88,166,255,.08)"),
        "follow_up_due": ("⏰ Follow-Up Due", "#f0883e", "rgba(240,136,62,.08)"),
        "called": ("📞 Called", "#bc8cff", "rgba(188,140,255,.08)"),
        "responded": ("✅ Responded", "#3fb950", "rgba(52,211,153,.08)"),
        "bounced": ("⛔ Bounced", "#f85149", "rgba(248,113,113,.08)"),
        "dead": ("💀 Dead", "#8b949e", "rgba(139,148,160,.08)"),
        "won": ("🏆 Won", "#3fb950", "rgba(52,211,153,.15)"),
    }
    for pr in prospects[:100]:
        pid = pr.get("id", "")
        cats = ", ".join(pr.get("categories_matched", [])[:2])
        po_count = len(pr.get("purchase_orders", []))
        phone = pr.get("buyer_phone", "") or "—"
        email = pr.get("buyer_email", "") or "—"
        name = pr.get("buyer_name", "") or "—"
        stat = pr.get("outreach_status", "new")
        lbl, clr, bg = status_cfg.get(stat, status_cfg["new"])
        badge = f'<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;color:{clr};background:{bg}">{lbl}</span>'

        # Action buttons based on status
        actions = ""
        if stat in ("emailed", "follow_up_due"):
            actions = f'<button onclick="markResponded(\'{pid}\')" class="act-btn" title="Mark responded" style="color:#3fb950">✅</button>'
            actions += f'<button onclick="markBounced(\'{pid}\',\'{email}\')" class="act-btn" title="Mark bounced" style="color:#f85149">⛔</button>'
        elif stat == "new":
            actions = f'<span style="color:var(--tx2);font-size:10px">awaiting email</span>'
        elif stat == "responded":
            actions = f'<button onclick="markWon(\'{pid}\')" class="act-btn" title="Mark won" style="color:#3fb950">🏆</button>'

        prospect_rows += f"""<tr data-pid="{pid}">
         <td style="font-weight:500"><a href="/growth/prospect/{pid}" style="color:var(--ac);text-decoration:none">{pr.get('agency', '—')}</a></td>
         <td>{name}</td>
         <td style="font-size:12px"><a href="mailto:{email}" style="color:var(--ac);text-decoration:none" title="Open email to {name}">{email}</a></td>
         <td style="font-size:12px">{phone}</td>
         <td class="mono">{po_count}</td>
         <td class="mono" style="color:#3fb950">${pr.get('total_spend', 0):,.0f}</td>
         <td style="font-size:11px">{cats}</td>
         <td>{badge}</td>
         <td style="white-space:nowrap">{actions}</td>
        </tr>"""

    # Step progress indicators
    def step_tag(done, label):
        c = "#3fb950" if done else "#8b949e"
        bg = "rgba(52,211,153,.1)" if done else "rgba(139,148,160,.05)"
        icon = "✅" if done else "⬜"
        return f'<span style="display:inline-block;padding:3px 10px;border-radius:12px;font-size:12px;font-weight:600;color:{c};background:{bg}">{icon} {label}</span>'

    step1 = step_tag(h.get("total_pos", 0) > 0, f"History: {h.get('total_pos', 0)} POs")
    step2 = step_tag(c.get("total", 0) > 0, f"Categories: {c.get('total', 0)}")
    step3 = step_tag(p.get("total", 0) > 0, f"Prospects: {p.get('total', 0)}")
    step4 = step_tag(o.get("total_sent", 0) > 0, f"Emailed: {o.get('total_sent', 0)}")

    pull_running = pull.get("running", False)
    buyer_running = buyer.get("running", False)
    pull_progress = pull.get("progress", "") if pull_running else ""
    buyer_progress = buyer.get("progress", "") if buyer_running else ""

    return f"""{_header('Growth Engine')}
    <style>
     .card {{background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:16px;margin-bottom:16px}}
     .card h3 {{font-size:15px;margin-bottom:12px;display:flex;align-items:center;gap:8px}}
     .g-btn {{padding:8px 16px;border-radius:8px;border:1px solid var(--bd);background:var(--sf2);color:var(--tx);cursor:pointer;font-size:13px;font-weight:600;transition:all .15s}}
     .g-btn:hover {{background:var(--ac);color:#000;border-color:var(--ac)}}
     .g-btn-go {{background:rgba(52,211,153,.12);color:#3fb950;border-color:rgba(52,211,153,.3)}}
     .g-btn-warn {{background:rgba(210,153,34,.12);color:#d29922;border-color:rgba(210,153,34,.3)}}
     .g-btn-red {{background:rgba(248,113,113,.12);color:#f85149;border-color:rgba(248,113,113,.3)}}
     .act-btn {{background:none;border:none;cursor:pointer;font-size:14px;padding:2px 4px;opacity:.7;transition:opacity .15s}}
     .act-btn:hover {{opacity:1}}
     table {{width:100%;border-collapse:collapse;font-size:12px}}
     th {{text-align:left;padding:6px 8px;border-bottom:2px solid var(--bd);font-size:11px;color:var(--tx2);text-transform:uppercase}}
     td {{padding:6px 8px;border-bottom:1px solid var(--bd)}}
     .mono {{font-family:'JetBrains Mono',monospace}}
     #progress-bar {{display:{'block' if (pull_running or buyer_running) else 'none'};background:var(--sf2);padding:10px;border-radius:8px;margin-bottom:12px;font-size:12px}}
    </style>

    <h1>🚀 Growth Engine</h1>
    <div style="color:var(--tx2);font-size:13px;margin-bottom:16px">
     SCPRS-driven proactive outreach — mine Reytech history → find all buyers → email → voice follow-up
    </div>

    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px">{step1} → {step2} → {step3} → {step4}</div>

    <div id="progress-bar">
     <span id="progress-text">{pull_progress or buyer_progress or 'Idle'}</span>
    </div>

    <div class="card">
     <h3>⚡ Actions</h3>
     <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px">
      <button class="g-btn g-btn-go" style="font-size:14px;padding:10px 20px" onclick="createCampaign()">🚀 Create Campaign</button>
     </div>
     <div style="font-size:11px;color:var(--tx2);margin-bottom:12px">
      Mines SCPRS for all buyers → scores by opportunity → emails top prospects → auto-schedules voice follow-up in 3-5 days
     </div>
     <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button class="g-btn" onclick="runStep('/api/growth/pull-history')">📥 Pull Reytech History</button>
      <button class="g-btn" onclick="runStep('/api/growth/find-buyers')">🔍 Find Buyers</button>
      <button class="g-btn" onclick="runStep('/api/growth/outreach?dry_run=true')">👁️ Preview Emails</button>
      <button class="g-btn g-btn-warn" onclick="if(confirm('Send real emails to prospects?')) runStep('/api/growth/outreach?dry_run=false')">📧 Send Emails</button>
      <button class="g-btn" onclick="runStep('/api/growth/follow-ups')">📋 Follow-Ups</button>
      <button class="g-btn" onclick="if(confirm('Auto-dial non-responders?')) runStep('/api/growth/voice-follow-up')">📞 Voice Follow-Up</button>
      <button class="g-btn" onclick="runStep('/api/growth/scan-bounces')">🔍 Scan Bounces</button>
      <button class="g-btn" onclick="runStep('/api/growth/campaigns')">📊 Stats</button>
     </div>
    </div>

    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">
     <div class="card" style="text-align:center">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px">Reytech POs</div>
      <div style="font-size:28px;font-weight:700;color:var(--ac)">{h.get('total_pos', 0)}</div>
      <div style="font-size:10px;color:var(--tx2)">{h.get('total_items', 0)} items</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px">Categories</div>
      <div style="font-size:28px;font-weight:700;color:#bc8cff">{c.get('total', 0)}</div>
      <div style="font-size:10px;color:var(--tx2)">product groups</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px">Prospects</div>
      <div style="font-size:28px;font-weight:700;color:#d29922">{p.get('total', 0)}</div>
      <div style="font-size:10px;color:var(--tx2)">buyers found</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px">Outreach</div>
      <div style="font-size:28px;font-weight:700;color:#3fb950">{o.get('total_sent', 0)}</div>
      <div style="font-size:10px;color:var(--tx2)">{total_no_response} pending follow-up</div>
     </div>
    </div>

    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">
     <div class="card" style="text-align:center">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">📧 Emailed</div>
      <div style="font-size:22px;font-weight:700;color:#58a6ff">{total_emailed}</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">✅ Responded</div>
      <div style="font-size:22px;font-weight:700;color:#3fb950">{total_responded}</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">⛔ Bounced</div>
      <div style="font-size:22px;font-weight:700;color:#f85149">{total_bounced}</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">📞 Called</div>
      <div style="font-size:22px;font-weight:700;color:#bc8cff">{total_called}</div>
     </div>
    </div>

    {'<div class="card"><h3>📂 Item Categories</h3><table><thead><tr><th>Category</th><th>Items</th><th>POs</th><th>Total Value</th><th>Sample Items</th></tr></thead><tbody>' + cat_rows + '</tbody></table></div>' if cat_rows else ''}

    {'<div class="card"><h3>🎯 Prospect Pipeline (' + str(len(prospects)) + ')</h3><div style="max-height:500px;overflow:auto"><table><thead><tr><th>Agency</th><th>Buyer</th><th>Email</th><th>Phone</th><th>POs</th><th>Spend</th><th>Categories</th><th>Status</th><th>Actions</th></tr></thead><tbody>' + prospect_rows + '</tbody></table></div></div>' if prospect_rows else ''}

    <div id="result" style="display:none;background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:12px;margin-top:12px;max-height:600px;overflow:auto">
     <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
      <span style="font-weight:600;font-size:13px" id="result-title">Result</span>
      <button onclick="document.getElementById('result').style.display='none'" style="background:none;border:none;color:var(--tx2);cursor:pointer">✕</button>
     </div>
     <div id="result-emails" style="display:none"></div>
     <pre id="result-content" style="font-size:11px;white-space:pre-wrap;word-break:break-word;margin:0"></pre>
    </div>

    <!-- Existing Email Drafts -->
    <div class="card" id="drafts-section">
     <h3>📨 Email Drafts from Campaigns</h3>
     <div id="drafts-container">
      <div style="color:var(--tx2);font-size:12px">Loading drafts...</div>
     </div>
    </div>

    <script>
    function createCampaign() {{
      const mode = confirm('Send real emails to prospects?\\n\\nOK = Send emails (live)\\nCancel = Preview only (dry run)');
      const body = {{ dry_run: !mode, max_prospects: 50 }};
      fetch('/api/growth/create-campaign', {{
        method: 'POST', credentials: 'same-origin',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify(body)
      }}).then(r => r.json()).then(data => {{
        showResult(data);
        if (data.ok) pollProgress();
      }}).catch(e => showResultRaw('Error: ' + e));
    }}

    function runStep(url) {{
      fetch(url, {{credentials:'same-origin'}}).then(r=>r.json()).then(data => {{
        showResult(data);
        if (data.message && data.message.includes('Check')) pollProgress();
      }}).catch(e => showResultRaw('Error: ' + e));
    }}

    function showResult(data) {{
      const el = document.getElementById('result');
      const emails = document.getElementById('result-emails');
      const pre = document.getElementById('result-content');
      const title = document.getElementById('result-title');
      el.style.display = 'block';

      // If response has preview emails, render them as cards
      if (data.preview && data.preview.length > 0) {{
        title.textContent = (data.dry_run ? '👁️ Preview' : '📧 Sent') + ': ' + data.emails_built + ' emails';
        emails.style.display = 'block';
        pre.style.display = 'none';
        emails.innerHTML = data.preview.map((e, i) => {{
          const gmailUrl = 'https://mail.google.com/mail/?view=cm&to=' + encodeURIComponent(e.to) + '&su=' + encodeURIComponent(e.subject) + '&body=' + encodeURIComponent(e.body || '');
          const bodyContent = e.body_html || e.body || '(no body)';
          const isHtml = !!e.body_html;
          return `
          <div style="background:var(--sf2);border:1px solid var(--bd);border-radius:8px;padding:12px;margin-bottom:10px">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
              <span style="font-size:11px;color:var(--tx2)">#${{i+1}}</span>
              <div style="display:flex;gap:6px;align-items:center">
                <a href="${{gmailUrl}}" target="_blank" style="font-size:10px;padding:3px 8px;border-radius:6px;background:rgba(79,140,255,.12);color:var(--ac);text-decoration:none;font-weight:600;border:1px solid rgba(79,140,255,.3)">📧 Open in Gmail</a>
                <a href="mailto:${{e.to}}?subject=${{encodeURIComponent(e.subject)}}&body=${{encodeURIComponent(e.body || '')}}" style="font-size:10px;padding:3px 8px;border-radius:6px;background:rgba(139,148,160,.1);color:var(--tx2);text-decoration:none;border:1px solid var(--bd)">✉️ mailto</a>
                <span style="font-size:10px;padding:2px 8px;border-radius:10px;background:${{data.dry_run?'rgba(210,153,34,.1)':'rgba(52,211,153,.1)'}};color:${{data.dry_run?'#d29922':'#3fb950'}};font-weight:600">${{data.dry_run?'DRAFT':'SENT'}}</span>
              </div>
            </div>
            <div style="font-size:12px;margin-bottom:4px"><strong>To:</strong> <a href="mailto:${{e.to}}" style="color:var(--ac)">${{e.to}}</a> <span style="color:var(--tx2)">(${{e.agency}})</span></div>
            <div style="font-size:12px;margin-bottom:8px;color:var(--ac)"><strong>Subject:</strong> ${{e.subject}}</div>
            <div style="font-size:12px;line-height:1.5;color:var(--tx);background:var(--sf);padding:10px;border-radius:6px;border:1px solid var(--bd);max-height:200px;overflow:auto">${{isHtml ? bodyContent : '<pre style="white-space:pre-wrap;margin:0;font-family:inherit">' + bodyContent + '</pre>'}}</div>
          </div>
        `}}).join('');
        // Also reload drafts section
        loadDrafts();
      }} else {{
        emails.style.display = 'none';
        pre.style.display = 'block';
        title.textContent = 'Result';
        pre.textContent = JSON.stringify(data, null, 2);
      }}
    }}

    function showResultRaw(text) {{
      const el = document.getElementById('result');
      el.style.display = 'block';
      document.getElementById('result-emails').style.display = 'none';
      document.getElementById('result-content').style.display = 'block';
      document.getElementById('result-content').textContent = text;
    }}

    function crmPost(url, body) {{
      return fetch(url, {{method:'POST', credentials:'same-origin', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify(body)}}).then(r=>r.json());
    }}

    function markResponded(pid) {{
      const detail = prompt('Response details (optional):','Email reply received');
      if (detail === null) return;
      crmPost('/api/growth/prospect/'+pid+'/responded', {{response_type:'email_reply', detail:detail}}).then(d => {{
        if (d.ok) {{ alert('Marked as responded'); location.reload(); }}
        else alert(d.error || 'Failed');
      }});
    }}

    function markBounced(pid, email) {{
      if (!confirm('Mark ' + email + ' as bounced?')) return;
      const reason = prompt('Bounce reason:', 'Mailbox not found');
      if (reason === null) return;
      crmPost('/api/growth/bounceback', {{email:email, reason:reason}}).then(d => {{
        if (d.ok) {{ alert('Marked as bounced'); location.reload(); }}
        else alert(d.error || 'Failed');
      }});
    }}

    function markWon(pid) {{
      crmPost('/api/growth/prospect/'+pid, {{outreach_status:'won'}}).then(d => {{
        if (d.ok) {{ alert('Marked as won!'); location.reload(); }}
        else alert(d.error || 'Failed');
      }});
    }}

    // Load existing email drafts from all campaigns
    function loadDrafts() {{
      fetch('/api/growth/campaigns', {{credentials:'same-origin'}}).then(r=>r.json()).then(data => {{
        const container = document.getElementById('drafts-container');
        if (!data.ok || !data.campaigns || data.campaigns.length === 0) {{
          container.innerHTML = '<div style="color:var(--tx2);font-size:12px;padding:8px">No campaigns yet. Click 👁️ Preview Emails or 🚀 Create Campaign.</div>';
          return;
        }}
        let html = '';
        data.campaigns.forEach((camp, ci) => {{
          const entries = camp.outreach || [];
          const sentCount = entries.filter(e => e.email_sent).length;
          const draftCount = entries.length - sentCount;
          const isDry = camp.dry_run;
          html += `<div style="margin-bottom:12px">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
              <span style="font-weight:600;font-size:12px">${{camp.id || 'Campaign '+(ci+1)}}</span>
              <span style="font-size:10px;padding:2px 6px;border-radius:8px;background:${{isDry?'rgba(210,153,34,.1)':'rgba(52,211,153,.1)'}};color:${{isDry?'#d29922':'#3fb950'}}">${{isDry?'DRY RUN':'LIVE'}}</span>
              <span style="font-size:10px;color:var(--tx2)">${{entries.length}} emails (${{sentCount}} sent, ${{draftCount}} draft)</span>
            </div>`;
          entries.slice(0, 5).forEach((e, ei) => {{
            const subj = e.subject || e.email_subject || '(no subject)';
            const body = e.body_html || e.body || e.email_body || '';
            const bodyPlain = e.body || e.email_body || '';
            const isHtml = !!e.body_html;
            const gmailUrl = 'https://mail.google.com/mail/?view=cm&to=' + encodeURIComponent(e.email || '') + '&su=' + encodeURIComponent(subj) + '&body=' + encodeURIComponent(bodyPlain);
            html += `<details style="margin-bottom:4px;font-size:12px">
              <summary style="cursor:pointer;padding:4px 8px;border-radius:4px;background:var(--sf2);display:flex;align-items:center;gap:6px;flex-wrap:wrap">
                <span style="color:var(--tx2)">${{e.email || ''}}</span> — <span style="color:var(--ac)">${{subj.substring(0,60)}}</span>
                ${{e.email_sent ? '<span style="color:#3fb950;font-size:10px">✅ SENT</span>' : '<span style="color:#d29922;font-size:10px">📝 DRAFT</span>'}}
                <a href="${{gmailUrl}}" target="_blank" onclick="event.stopPropagation()" style="font-size:9px;padding:2px 6px;border-radius:4px;background:rgba(79,140,255,.12);color:var(--ac);text-decoration:none;border:1px solid rgba(79,140,255,.3);margin-left:auto">📧 Gmail</a>
              </summary>
              <div style="padding:8px;margin:4px 0 4px 16px;font-size:12px;line-height:1.4;background:var(--sf);border-radius:4px;border:1px solid var(--bd);max-height:180px;overflow:auto">${{isHtml ? body : '<pre style="white-space:pre-wrap;margin:0;font-family:inherit">' + body + '</pre>'}}</div>
            </details>`;
          }});
          if (entries.length > 5) html += `<div style="font-size:10px;color:var(--tx2);padding-left:8px">...and ${{entries.length-5}} more</div>`;
          html += '</div>';
        }});
        container.innerHTML = html;
      }}).catch(() => {{
        document.getElementById('drafts-container').innerHTML = '<div style="color:#f85149;font-size:12px">Failed to load drafts</div>';
      }});
    }}

    let pollTimer = null;
    function pollProgress() {{
      if (pollTimer) clearInterval(pollTimer);
      const bar = document.getElementById('progress-bar');
      const txt = document.getElementById('progress-text');
      bar.style.display = 'block';
      pollTimer = setInterval(() => {{
        Promise.all([
          fetch('/api/growth/pull-status',{{credentials:'same-origin'}}).then(r=>r.json()),
          fetch('/api/growth/buyer-status',{{credentials:'same-origin'}}).then(r=>r.json())
        ]).then(([pull, buyer]) => {{
          const running = pull.running || buyer.running;
          txt.textContent = pull.running ? pull.progress : buyer.running ? buyer.progress : 'Complete — refresh page to see results';
          if (!running) {{
            clearInterval(pollTimer);
            setTimeout(() => location.reload(), 2000);
          }}
        }});
      }}, 3000);
    }}
    // Load drafts on page load
    loadDrafts();
    {('pollProgress();' if (pull_running or buyer_running) else '')}
    </script>
    </body></html>"""


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
        actor_badge = f'<span style="font-size:9px;padding:1px 6px;border-radius:8px;background:rgba(79,140,255,.15);color:var(--ac);margin-left:4px">{actor}</span>' if actor and actor != "system" else ""
        meta = ev.get("metadata",{})
        meta_html = ""
        if meta.get("amount"): meta_html += f' · <span style="color:#3fb950">${float(meta["amount"]):,.0f}</span>'
        if meta.get("subject"): meta_html += f' · <i style="color:var(--tx2)">{str(meta["subject"])[:50]}</i>'
        tl_html += f'<div style="display:flex;gap:10px;padding:10px 0;border-bottom:1px solid rgba(46,51,69,.5)"><span style="font-size:18px;flex-shrink:0;width:24px;text-align:center">{icon}</span><div style="flex:1;min-width:0"><div style="font-size:12px;font-weight:600;display:flex;align-items:center;gap:4px">{etype}{actor_badge}</div><div style="font-size:12px;color:var(--tx2);margin-top:2px;word-break:break-word">{detail}{meta_html}</div></div><span style="font-size:10px;color:var(--tx2);font-family:monospace;white-space:nowrap;flex-shrink:0">{ts}</span></div>'
    if not tl_html:
        tl_html = '<div style="color:var(--tx2);font-size:13px;padding:16px;text-align:center">No activity yet — log a call, email, or note above</div>'

    # PO history
    po_html = ""
    for po in pr.get("purchase_orders",[]):
        po_html += f'<tr><td class="mono" style="color:var(--ac)">{po.get("po_number","—")}</td><td class="mono">{po.get("date","—")}</td><td style="font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{str(po.get("items","—"))[:80]}</td><td style="font-size:11px">{po.get("category","—")}</td><td class="mono" style="color:#3fb950;text-align:right">${po.get("total_num",0) or po.get("total",0) or 0:,.0f}</td></tr>'

    # Items purchased
    items_html = ""
    cat_colors = {"Medical":"#f87171","Janitorial":"#3fb950","Office":"#4f8cff","IT":"#a78bfa","Facility":"#fb923c","Safety":"#fbbf24"}
    for it in pr.get("items_purchased",[])[:20]:
        cc = cat_colors.get(it.get("category",""),"#8b90a0")
        up = f'<span style="font-size:11px;font-family:monospace;color:#3fb950">${float(it["unit_price"]):,.2f}</span>' if it.get("unit_price") else ""
        items_html += f'<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid rgba(46,51,69,.4)"><span style="font-size:10px;padding:2px 7px;border-radius:8px;background:{cc}22;color:{cc};border:1px solid {cc}44;white-space:nowrap">{it.get("category","General")}</span><span style="font-size:12px;flex:1">{it.get("description","")}</span>{up}</div>'

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
        cats_html += f'<div style="margin-bottom:8px"><div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px"><span style="color:{cc};font-weight:600">{cat}</span><span class="mono">${spend:,.0f} ({pct}%)</span></div><div style="background:var(--sf2);border-radius:4px;height:6px;overflow:hidden"><div style="width:{pct}%;height:100%;background:{cc};border-radius:4px"></div></div></div>'

    # Outreach records
    or_html = ""
    for o in outreach_recs:
        flags = (''.join([
            '<span style="color:#3fb950">✅ Sent</span> ' if o.get("email_sent") else '<span style="color:var(--tx2)">⏳ Draft</span> ',
            '<span style="color:#f85149">⛔ Bounced</span> ' if o.get("bounced") else '',
            '<span style="color:#3fb950">✅ Replied</span> ' if o.get("response_received") else '',
            '<span style="color:#fb923c">📞 Called</span>' if o.get("voice_called") else '',
        ]))
        or_html += f'<div style="padding:10px;background:var(--sf2);border-radius:8px;margin-bottom:8px;font-size:12px"><div style="font-weight:600;margin-bottom:4px">{o.get("email_subject","—")}</div><div style="color:var(--tx2);display:flex;gap:12px;flex-wrap:wrap"><span>To: {o.get("email","—")}</span>{flags}</div></div>'

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

    page_html = f"""{_header('CRM Contact')}
    <style>
     .card{{background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:18px;margin-bottom:14px}}
     .card h3{{font-size:11px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:14px}}
     .g-btn{{padding:8px 14px;border-radius:7px;border:1px solid var(--bd);background:var(--sf2);color:var(--tx);cursor:pointer;font-size:13px;font-weight:600;transition:.15s;text-decoration:none;display:inline-flex;align-items:center;gap:5px}}
     .g-btn:hover{{border-color:var(--ac);background:rgba(79,140,255,.1)}}
     .g-btn-go{{background:rgba(52,211,153,.1);color:#3fb950;border-color:rgba(52,211,153,.3)}}
     .g-btn-warn{{background:rgba(251,191,36,.1);color:#fbbf24;border-color:rgba(251,191,36,.3)}}
     .g-btn-red{{background:rgba(248,113,113,.1);color:#f87171;border-color:rgba(248,113,113,.3)}}
     .g-btn-purple{{background:rgba(167,139,250,.1);color:#a78bfa;border-color:rgba(167,139,250,.3)}}
     table{{width:100%;border-collapse:collapse;font-size:12px}}
     th{{text-align:left;padding:8px;font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--bd)}}
     td{{padding:8px;border-bottom:1px solid rgba(46,51,69,.4);vertical-align:middle}}
     .mono{{font-family:'JetBrains Mono',monospace}}
     .field-row{{display:flex;align-items:flex-start;padding:9px 0;border-bottom:1px solid rgba(46,51,69,.4)}}
     .field-lbl{{font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;width:80px;flex-shrink:0;padding-top:2px}}
     .field-val{{font-size:13px;font-weight:500;flex:1}}
     .modal-bg{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:1000;align-items:center;justify-content:center}}
     .modal-box{{background:var(--sf);border:1px solid var(--bd);border-radius:12px;padding:24px;width:480px;max-width:95vw;max-height:90vh;overflow-y:auto}}
     .form-lbl{{font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;display:block;margin-bottom:4px}}
     .form-input{{width:100%;padding:10px 12px;background:var(--sf2);border:1px solid var(--bd);border-radius:7px;color:var(--tx);font-size:13px;font-family:'DM Sans',sans-serif;box-sizing:border-box;margin-bottom:12px}}
     .form-input:focus{{outline:none;border-color:var(--ac)}}
     textarea.form-input{{resize:vertical;min-height:80px}}
    </style>

    <div style="display:flex;align-items:center;gap:8px;margin-bottom:16px;font-size:13px">
     <a href="/contacts" style="color:var(--ac)">👥 CRM</a>
     <span style="color:var(--tx2)">›</span>
     <span style="color:var(--tx)">{agency}</span>
    </div>

    <div style="display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:18px">
     <div>
      <h1 style="font-size:22px;font-weight:700;margin-bottom:6px">{pr.get('buyer_name') or agency}</h1>
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
       <span style="font-size:13px;color:var(--tx2)">{agency}</span>
       <span style="padding:3px 12px;border-radius:12px;font-size:11px;font-weight:700;text-transform:uppercase;background:{stat_color}22;color:{stat_color};border:1px solid {stat_color}44">{stat}</span>
       <span style="font-size:12px;color:var(--tx2)">Score <b style="color:var(--ac)">{score_pct}%</b></span>
       <span style="font-size:12px;color:var(--tx2)">Spend <b style="color:#3fb950">${total_spend:,.0f}</b></span>
       <span style="font-size:12px;color:var(--tx2)">{po_count} POs · Last {last_purchase}</span>
      </div>
     </div>
     <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button class="g-btn g-btn-go" onclick="openLog('email')">📧 Log Email</button>
      <button class="g-btn g-btn-go" onclick="openLog('call')">📞 Log Call</button>
      <button class="g-btn g-btn-purple" onclick="openLog('chat')">💬 Log Chat</button>
      <button class="g-btn" onclick="openLog('note')">📝 Note</button>
      <button class="g-btn" onclick="openEdit()">✏️ Edit</button>
     </div>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1.5fr 0.9fr;gap:14px;margin-bottom:14px">
     <div class="card">
      <h3>👤 Contact Info</h3>
      <div class="field-row"><span class="field-lbl">Name</span><span class="field-val">{pr.get('buyer_name') or '—'}</span></div>
      <div class="field-row"><span class="field-lbl">Email</span><span class="field-val"><a href="mailto:{pr.get('buyer_email','')}" style="color:var(--ac);font-family:monospace;font-size:12px">{pr.get('buyer_email') or '—'}</a></span></div>
      <div class="field-row"><span class="field-lbl">Phone</span><span class="field-val">{pr.get('buyer_phone') or '—'}</span></div>
      <div class="field-row"><span class="field-lbl">Title</span><span class="field-val">{pr.get('title') or '—'}</span></div>
      <div class="field-row"><span class="field-lbl">Agency</span><span class="field-val">{agency}</span></div>
      <div class="field-row"><span class="field-lbl">LinkedIn</span><span class="field-val">{"<a href='"+str(pr.get('linkedin',''))+"' target='_blank' style='color:var(--ac)'>View Profile</a>" if pr.get('linkedin') else '—'}</span></div>
      <div class="field-row"><span class="field-lbl">Notes</span><span class="field-val" style="font-size:12px;color:var(--tx2);white-space:pre-wrap">{pr.get('notes') or pr.get('contact_notes') or '—'}</span></div>
      <div style="margin-top:14px;display:grid;grid-template-columns:1fr 1fr;gap:6px">
       <button class="g-btn g-btn-go" onclick="setStatus('responded')" style="justify-content:center">✅ Responded</button>
       <button class="g-btn g-btn-warn" onclick="setStatus('follow_up_due')" style="justify-content:center">⏰ Follow Up</button>
       <button class="g-btn g-btn-go" onclick="setStatus('won')" style="justify-content:center">🏆 Won</button>
       <button class="g-btn g-btn-red" onclick="setStatus('dead')" style="justify-content:center">💀 Dead</button>
      </div>
     </div>

     <div class="card">
      <h3>📅 Activity Log <span style="font-weight:400;color:var(--tx2);font-size:10px;text-transform:none;letter-spacing:0">({len(all_events)} events)</span></h3>
      <div style="max-height:420px;overflow-y:auto;padding-right:4px">{tl_html}</div>
     </div>

     <div class="card">
      <h3>📊 SCPRS Intel</h3>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px">
       <div style="background:var(--sf2);border-radius:8px;padding:10px;text-align:center">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Annual Spend</div>
        <div style="font-size:18px;font-weight:700;color:#3fb950;font-family:monospace">${total_spend:,.0f}</div>
       </div>
       <div style="background:var(--sf2);border-radius:8px;padding:10px;text-align:center">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">PO Count</div>
        <div style="font-size:18px;font-weight:700;color:var(--ac);font-family:monospace">{po_count}</div>
       </div>
       <div style="background:var(--sf2);border-radius:8px;padding:10px;text-align:center">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Opp Score</div>
        <div style="font-size:18px;font-weight:700;color:#a78bfa;font-family:monospace">{score_pct}%</div>
       </div>
       <div style="background:var(--sf2);border-radius:8px;padding:10px;text-align:center">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Last Buy</div>
        <div style="font-size:12px;font-weight:600;font-family:monospace;color:var(--tx)">{last_purchase}</div>
       </div>
      </div>
      {('<div style="font-size:10px;color:var(--tx2);font-weight:600;text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Spend by Category</div>' + cats_html) if cats_html else '<div style="color:var(--tx2);font-size:12px">Run Deep Pull for category data</div>'}
     </div>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1.8fr;gap:14px">
     <div class="card">
      <h3>🛒 Items Purchased</h3>
      {('<div>' + items_html + '</div>') if items_html else '<div style="color:var(--tx2);font-size:13px;padding:8px 0">No item data yet — run Deep Pull to mine line items</div>'}
     </div>
     <div class="card">
      <h3>📋 PO History ({po_count})</h3>
      {('<div style="overflow-x:auto"><table><thead><tr><th>PO #</th><th>Date</th><th>Items</th><th>Category</th><th style="text-align:right">Total</th></tr></thead><tbody>' + po_html + '</tbody></table></div>') if po_html else '<div style="color:var(--tx2);font-size:13px;padding:8px 0">No PO history — run Deep Pull to fetch SCPRS purchase orders</div>'}
     </div>
    </div>

    {('<div class="card"><h3>📧 Outreach Campaigns</h3>' + or_html + '</div>') if or_html else ''}

    <!-- Log Activity Modal -->
    <div class="modal-bg" id="log-modal" onclick="if(event.target===this)closeModal()">
     <div class="modal-box">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:18px">
       <span id="modal-title" style="font-size:16px;font-weight:700">Log Activity</span>
       <button onclick="closeModal()" style="background:none;border:none;color:var(--tx2);cursor:pointer;font-size:20px">✕</button>
      </div>
      <div id="modal-body"></div>
     </div>
    </div>

    <!-- Edit Contact Modal -->
    <div class="modal-bg" id="edit-modal" onclick="if(event.target===this)closeEditModal()">
     <div class="modal-box">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:18px">
       <span style="font-size:16px;font-weight:700">✏️ Edit Contact</span>
       <button onclick="closeEditModal()" style="background:none;border:none;color:var(--tx2);cursor:pointer;font-size:20px">✕</button>
      </div>
      <label class="form-lbl">Full Name</label>
      <input id="edit-name" class="form-input" value="{pr.get('buyer_name','')}" placeholder="Full name">
      <label class="form-lbl">Phone</label>
      <input id="edit-phone" class="form-input" value="{pr.get('buyer_phone','')}" placeholder="+1 (xxx) xxx-xxxx">
      <label class="form-lbl">Title / Role</label>
      <input id="edit-title" class="form-input" value="{pr.get('title','')}" placeholder="e.g. Procurement Officer">
      <label class="form-lbl">LinkedIn URL</label>
      <input id="edit-linkedin" class="form-input" value="{pr.get('linkedin','')}" placeholder="https://linkedin.com/in/...">
      <label class="form-lbl">Notes</label>
      <textarea id="edit-notes" class="form-input">{pr.get('notes','') or pr.get('contact_notes','')}</textarea>
      <button onclick="saveContact()" class="g-btn g-btn-go" style="width:100%;justify-content:center;padding:12px;font-size:14px">💾 Save Contact</button>
     </div>
    </div>

    <script>
    const PID = '{pid}';
    const EMAIL = '{pr.get("buyer_email","")}';
    function crmPost(u,b){{return fetch(u,{{method:'POST',credentials:'same-origin',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(b)}}).then(r=>r.json())}}
    function setStatus(s){{crmPost('/api/growth/prospect/'+PID,{{outreach_status:s}}).then(r=>{{if(r.ok)location.reload();else alert(r.error)}})}}
    function setBounced(){{if(!confirm('Mark as bounced?'))return;crmPost('/api/growth/bounceback',{{email:EMAIL,reason:'Manual bounce'}}).then(r=>{{if(r.ok)location.reload();else alert(r.error)}})}}

    var logType='';
    function openLog(type){{
      logType=type;
      var titles={{email:'📧 Log Email',call:'📞 Log Call',note:'📝 Add Note',chat:'💬 Log Interaction'}};
      document.getElementById('modal-title').textContent=titles[type]||'Log Activity';
      var bodies={{
        email:'<label class="form-lbl">Direction</label><select id="log-dir" class="form-input"><option value="sent">Sent (outbound)</option><option value="received">Received (inbound reply)</option></select><label class="form-lbl">Subject</label><input id="log-subject" class="form-input" placeholder="Email subject..."><label class="form-lbl">Notes / Summary</label><textarea id="log-detail" class="form-input" placeholder="What was the email about?"></textarea>',
        call:'<label class="form-lbl">Outcome</label><select id="log-outcome" class="form-input"><option value="reached">Reached — had conversation</option><option value="voicemail">Left voicemail</option><option value="no_answer">No answer</option><option value="callback">Requested callback</option><option value="not_interested">Not interested</option></select><label class="form-lbl">Duration (minutes)</label><input id="log-duration" class="form-input" type="number" placeholder="e.g. 5"><label class="form-lbl">Notes</label><textarea id="log-detail" class="form-input" placeholder="What was discussed?"></textarea>',
        note:'<label class="form-lbl">Note</label><textarea id="log-detail" class="form-input" rows="6" placeholder="Add a note about this contact..."></textarea>',
        chat:'<label class="form-lbl">Channel</label><select id="log-channel" class="form-input"><option value="in_person">In-person meeting</option><option value="teams">Teams / Zoom</option><option value="linkedin">LinkedIn message</option><option value="text">Text / SMS</option><option value="other">Other</option></select><label class="form-lbl">Summary</label><textarea id="log-detail" class="form-input" placeholder="What was discussed?"></textarea>',
      }};
      document.getElementById('modal-body').innerHTML=(bodies[type]||'')+'<div style="display:flex;gap:8px;margin-top:4px"><button onclick="submitLog()" class="g-btn g-btn-go" style="flex:1;justify-content:center;padding:12px">✅ Save</button><button onclick="closeModal()" class="g-btn" style="padding:12px 20px">Cancel</button></div>';
      document.getElementById('log-modal').style.display='flex';
      setTimeout(()=>{{var d=document.getElementById('log-detail');if(d)d.focus();}},100);
    }}
    function closeModal(){{document.getElementById('log-modal').style.display='none';}}
    function submitLog(){{
      var detail=document.getElementById('log-detail')?.value||'';
      if(!detail.trim()){{alert('Please add a note or summary');return;}}
      var payload={{type:logType,detail:detail,actor:'mike'}};
      if(logType==='email'){{payload.direction=document.getElementById('log-dir')?.value;payload.subject=document.getElementById('log-subject')?.value;payload.event_type=payload.direction==='sent'?'email_sent':'email_received';}}
      else if(logType==='call'){{payload.outcome=document.getElementById('log-outcome')?.value;payload.duration=document.getElementById('log-duration')?.value;payload.event_type='voice_called';}}
      else if(logType==='chat'){{payload.channel=document.getElementById('log-channel')?.value;payload.event_type='chat';}}
      else{{payload.event_type='note';}}
      crmPost('/api/crm/contact/'+PID+'/log',payload).then(r=>{{if(r.ok){{closeModal();location.reload();}}else alert('Error: '+(r.error||'Failed'));}});
    }}
    function openEdit(){{document.getElementById('edit-modal').style.display='flex';}}
    function closeEditModal(){{document.getElementById('edit-modal').style.display='none';}}
    function saveContact(){{
      var data={{buyer_name:document.getElementById('edit-name').value,buyer_phone:document.getElementById('edit-phone').value,title:document.getElementById('edit-title').value,linkedin:document.getElementById('edit-linkedin').value,notes:document.getElementById('edit-notes').value}};
      crmPost('/api/growth/prospect/'+PID,data).then(r=>{{if(r.ok){{closeEditModal();location.reload();}}else alert('Error: '+(r.error||'Failed'));}});
    }}
    </script></body></html>"""
    return page_html


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
    max_cats = int(request.args.get("max_categories", 10))
    from_date = request.args.get("from", "01/01/2024")

    import threading
    def _run():
        find_category_buyers(max_categories=max_cats, from_date=from_date)
    t = threading.Thread(target=_run, daemon=True, name="growth-buyers")
    t.start()
    return jsonify({"ok": True, "message": f"Searching SCPRS for buyers (top {max_cats} categories from {from_date}). Check /api/growth/buyer-status."})


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
    max_p = int(request.args.get("max", 50))
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
    max_c = int(request.args.get("max", args.get("max", 100)))
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





# ── Notifications API ─────────────────────────────────────────────────────────
@bp.route("/api/notifications")
@auth_required
def api_notifications():
    """Get dashboard notifications (auto-draft alerts, etc.)."""
    unread = [n for n in _notifications if not n.get("read")]
    return jsonify({"ok": True, "notifications": list(_notifications),
                    "unread_count": len(unread)})

@bp.route("/api/notifications/mark-read", methods=["POST"])
@auth_required
def api_notifications_mark_read():
    for n in _notifications:
        n["read"] = True
    return jsonify({"ok": True})


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
        except Exception:
            pass
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
                run_scheduled_pulls(notify_fn=_push_notification)
            except Exception as e:
                log.error(f"Scheduled pull error: {e}")

            # Run PO award monitor daily at 8am
            if hour == 8 and now.minute < 30:
                try:
                    from src.agents.scprs_intelligence_engine import run_po_award_monitor
                    result = run_po_award_monitor(notify_fn=_push_notification)
                    if result.get("auto_closed_lost", 0) > 0:
                        log.info(f"PO Monitor: {result['auto_closed_lost']} quotes auto-closed")
                except Exception as e:
                    log.error(f"PO monitor scheduled: {e}")

        except Exception as e:
            log.error(f"SCPRS scheduler: {e}")

        # Check every 30 minutes
        _time.sleep(1800)

_scprs_autostart()


@bp.route("/api/intel/pull/schedule", methods=["GET", "POST"])
@auth_required
def api_intel_pull_schedule():
    """Configure SCPRS auto-pull schedule.

    GET: Return current schedule status
    POST { cron: "sunday 2am", run_now: false }: Set schedule
         cron examples: "sunday 2am", "daily", "0 2 * * 0" (standard cron)
    """
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
@bp.route("/api/quotes/win-probability")
@auth_required
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



@bp.route("/api/growth/voice-follow-up")
@auth_required
def api_growth_voice_follow_up():
    """Step 4: Auto-dial non-responders."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    max_calls = int(request.args.get("max", 10))
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
            except Exception:
                pass

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


# ─── Sales Intelligence Routes ─────────────────────────────────────────────

@bp.route("/api/intel/status")
@auth_required
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
def api_intel_pull_status():
    """Check deep pull progress."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    return jsonify({"ok": True, **DEEP_PULL_STATUS})


@bp.route("/api/intel/priority-queue")
@auth_required
def api_intel_priority_queue():
    """Get prioritized outreach queue — highest opportunity buyers first."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    limit = int(request.args.get("limit", 25))
    result = get_priority_queue(limit=limit)
    if not result.get("ok") and "No buyer data" in str(result.get("error", "")):
        return jsonify({"ok": False,
                        "error": "No buyer data yet",
                        "hint": "Run 🔍 Deep Pull All Buyers first to mine SCPRS for buyer contacts, categories, and spend data."})
    return jsonify(result)


@bp.route("/api/intel/push-prospects")
@auth_required
def api_intel_push_prospects():
    """Push top priority buyers into Growth Agent prospect pipeline."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    top_n = int(request.args.get("top", 50))
    return jsonify(push_to_growth_prospects(top_n=top_n))


@bp.route("/api/intel/revenue")
@auth_required
def api_intel_revenue():
    """Revenue tracker — YTD vs $2M goal."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    return jsonify(update_revenue_tracker())


@bp.route("/api/intel/revenue", methods=["POST"])
@auth_required
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
def api_intel_sb_admin(agency):
    """Find the SB admin/liaison for an agency."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    return jsonify(get_sb_admin(agency))


@bp.route("/api/intel/sb-admin-match")
@auth_required
def api_intel_sb_admin_match():
    """Match SB admin contacts to all agencies in the database."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    return jsonify(find_sb_admin_for_agencies())


@bp.route("/api/intel/buyers/add", methods=["POST"])
@auth_required
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
def api_intel_seed_demo():
    """Seed the intel DB with realistic CA agency demo data (for testing/demo when SCPRS is unreachable)."""
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel not available"})
    return jsonify(seed_demo_data())


@bp.route("/api/intel/buyers/delete", methods=["POST"])
@auth_required
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




# ─── Intelligence Dashboard Page ──────────────────────────────────────────

@bp.route("/intelligence")
@auth_required
def intelligence_page():
    """Sales Intelligence Dashboard — $2M revenue command center."""
    if not INTEL_AVAILABLE:
        flash("Sales Intelligence not available", "error")
        return redirect("/")

    from src.agents.sales_intel import _load_json as il, BUYERS_FILE as BF, AGENCIES_FILE as AF

    st = get_intel_status()
    rev = st.get("revenue", {})
    top_opps = st.get("top_opportunity_agencies", [])
    pull = st.get("pull_status", {})

    # Revenue bar
    pct = min(100, rev.get("pct_to_goal", 0))
    closed = rev.get("closed_revenue", 0)
    gap = rev.get("gap_to_goal", 0)
    pipeline = rev.get("pipeline_value", 0)
    monthly = rev.get("monthly_needed", 0)
    on_track = rev.get("on_track", False)
    run_rate = rev.get("run_rate_annual", 0)
    bar_color = "#3fb950" if pct >= 50 else "#d29922" if pct >= 25 else "#f85149"

    # Load buyer + agency data
    buyers_data = il(BF)
    agencies_data = il(AF)
    buyers = buyers_data.get("buyers", [])[:100] if isinstance(buyers_data, dict) else []
    agencies = agencies_data.get("agencies", [])[:50] if isinstance(agencies_data, dict) else []
    total_buyers = buyers_data.get("total_buyers", 0) if isinstance(buyers_data, dict) else 0
    total_agencies = agencies_data.get("total_agencies", 0) if isinstance(agencies_data, dict) else 0

    # Opportunity agencies (not our customer, sorted by score)
    opp_rows = ""
    for ag in agencies:
        if ag.get("is_customer"):
            continue
        cats = ", ".join(list(ag.get("categories", {}).keys())[:3])
        sb = ag.get("sb_admin")
        sb_cell = f'<span style="color:#3fb950">{sb.get("email","") or sb.get("name","")}</span>' if sb else '<span style="color:var(--tx2)">—</span>'
        buyer_count = len(ag.get("buyers", {}))
        opp_rows += f"""<tr>
         <td style="font-weight:600">{ag.get('dept_code','—')}</td>
         <td class="mono" style="color:#3fb950">${ag.get('total_spend',0):,.0f}</td>
         <td class="mono">{ag.get('opportunity_score',0)}</td>
         <td class="mono">{buyer_count}</td>
         <td style="font-size:11px">{cats}</td>
         <td style="font-size:11px">{sb_cell}</td>
        </tr>"""
        if len(opp_rows) > 20000:
            break

    # Top buyers (not our customers)
    buyer_rows = ""
    for b in buyers:
        if b.get("is_reytech_customer"):
            continue
        cats = ", ".join(list(b.get("categories", {}).keys())[:2])
        items = ", ".join([i.get("description","")[:40] for i in b.get("items_purchased",[])[:2]])
        buyer_rows += f"""<tr>
         <td style="font-weight:500">{b.get('agency','—')}</td>
         <td>{b.get('name','—')}</td>
         <td style="font-size:12px">{b.get('email','—')}</td>
         <td class="mono" style="color:#3fb950">${b.get('total_spend',0):,.0f}</td>
         <td class="mono">{b.get('opportunity_score',0)}</td>
         <td style="font-size:11px">{cats}</td>
         <td style="font-size:10px;color:var(--tx2)">{items[:60]}</td>
        </tr>"""
        if len(buyer_rows) > 25000:
            break

    # Existing customer spend (agencies we do sell to)
    customer_rows = ""
    for ag in agencies:
        if not ag.get("is_customer"):
            continue
        upsell = ag.get("total_spend", 0) - ag.get("reytech_spend", 0)
        customer_rows += f"""<tr>
         <td style="font-weight:600">{ag.get('dept_code','—')}</td>
         <td class="mono" style="color:#3fb950">${ag.get('reytech_spend',0):,.0f}</td>
         <td class="mono">${ag.get('total_spend',0):,.0f}</td>
         <td class="mono" style="color:#d29922">${upsell:,.0f}</td>
         <td style="font-size:11px">{', '.join(list(ag.get('categories',{}).keys())[:3])}</td>
        </tr>"""

    pull_running = pull.get("running", False)

    # Check scprs connectivity status
    scprs_ok = st.get("scprs_reachable", False)
    scprs_err = st.get("scprs_error", "")
    has_buyers = total_buyers > 0

    return f"""{_header('Sales Intelligence')}
    <style>
     .card{{background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:16px;margin-bottom:14px}}
     .card h3{{font-size:11px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px}}
     .g-btn{{padding:8px 14px;border-radius:7px;border:1px solid var(--bd);background:var(--sf2);color:var(--tx);cursor:pointer;font-size:13px;font-weight:600;transition:.15s;display:inline-flex;align-items:center;gap:5px}}
     .g-btn:hover{{border-color:var(--ac);background:rgba(79,140,255,.1)}}
     .g-btn-go{{background:rgba(52,211,153,.1);color:#3fb950;border-color:rgba(52,211,153,.3)}}
     .g-btn-warn{{background:rgba(251,191,36,.1);color:#fbbf24;border-color:rgba(251,191,36,.3)}}
     .g-btn-red{{background:rgba(248,113,113,.1);color:#f87171;border-color:rgba(248,113,113,.3)}}
     .g-btn-purple{{background:rgba(167,139,250,.1);color:#a78bfa;border-color:rgba(167,139,250,.3)}}
     table{{width:100%;border-collapse:collapse;font-size:12px}}
     th{{text-align:left;padding:8px;font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--bd)}}
     td{{padding:8px;border-bottom:1px solid rgba(46,51,69,.4);vertical-align:middle}}
     tr:hover td{{background:rgba(79,140,255,.04)}}
     .mono{{font-family:'JetBrains Mono',monospace}}
     .modal-bg{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:1000;align-items:center;justify-content:center}}
     .modal-box{{background:var(--sf);border:1px solid var(--bd);border-radius:12px;padding:24px;width:520px;max-width:95vw;max-height:90vh;overflow-y:auto}}
     .form-lbl{{font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;display:block;margin-bottom:4px}}
     .form-input{{width:100%;padding:10px 12px;background:var(--sf2);border:1px solid var(--bd);border-radius:7px;color:var(--tx);font-size:13px;box-sizing:border-box;margin-bottom:12px;font-family:'DM Sans',sans-serif}}
     .form-input:focus{{outline:none;border-color:var(--ac)}}
     textarea.form-input{{resize:vertical;min-height:120px}}
    </style>

    <!-- Header -->
    <div style="display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:18px">
     <div>
      <h1 style="font-size:22px;font-weight:700;margin-bottom:4px">🧠 Sales Intelligence</h1>
      <div style="font-size:13px;color:var(--tx2)">SCPRS buyer database — contacts, spend, categories, opportunities</div>
     </div>
     <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
      <span id="scprs-dot" style="font-size:12px;padding:4px 10px;border-radius:12px;background:{'rgba(52,211,153,.15)' if scprs_ok else 'rgba(248,113,113,.15)'};color:{'#3fb950' if scprs_ok else '#f87171'};border:1px solid {'rgba(52,211,153,.3)' if scprs_ok else 'rgba(248,113,113,.3)'}">
       {'✅ SCPRS Connected' if scprs_ok else '⚠️ SCPRS Offline'}
      </span>
      <button class="g-btn" onclick="testSCPRS(this)">🔌 Test Connection</button>
     </div>
    </div>

    <!-- SCPRS offline banner -->
    {'<div id="scprs-banner" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.3);border-radius:8px;padding:12px 16px;margin-bottom:14px;font-size:13px"><b style=\'color:#f87171\'>⚠️ SCPRS Unreachable</b> — Deep Pull requires Railway static IP.<br><span style=\'color:var(--tx2);font-size:12px\'>Fix: railway.app → your project → Settings → Networking → Static IP → Enable. Then retry Deep Pull.</span><br><span style=\'color:var(--tx2);font-size:12px\'>In the meantime, use <b style=\'color:#fbbf24\'>Load Demo Data</b> to see the full UI, or <b style=\'color:#3fb950\'>Add Buyer Manually</b> to enter real contacts.</span></div>' if not scprs_ok else ''}

    <!-- Stats bar -->
    <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:14px">
     <div class="card" style="text-align:center;padding:12px">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Buyers</div>
      <div style="font-size:26px;font-weight:700;color:var(--ac);font-family:monospace">{total_buyers}</div>
     </div>
     <div class="card" style="text-align:center;padding:12px">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Agencies</div>
      <div style="font-size:26px;font-weight:700;color:#a78bfa;font-family:monospace">{total_agencies}</div>
     </div>
     <div class="card" style="text-align:center;padding:12px">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Addressable</div>
      <div style="font-size:22px;font-weight:700;color:#fbbf24;font-family:monospace">${sum(b.get('total_spend',0) for b in buyers):,.0f}</div>
     </div>
     <div class="card" style="text-align:center;padding:12px">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Revenue Closed</div>
      <div style="font-size:22px;font-weight:700;color:#3fb950;font-family:monospace">${closed:,.0f}</div>
     </div>
     <div class="card" style="text-align:center;padding:12px">
      <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Goal Progress</div>
      <div style="font-size:22px;font-weight:700;color:{'#3fb950' if pct>=50 else '#d29922'};font-family:monospace">{pct:.0f}%</div>
     </div>
    </div>

    <!-- 2-col layout -->
    <div style="display:grid;grid-template-columns:1fr 340px;gap:14px;align-items:start">
     <div>

      <!-- Deep Pull Actions -->
      <div class="card">
       <h3>⚡ Data Collection</h3>
       <div id="pull-progress-wrap" style="display:{'block' if pull_running else 'none'};margin-bottom:12px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
         <span style="font-size:12px;font-weight:600;color:var(--tx2)" id="pull-phase-label">Deep Pull Running...</span>
         <span style="font-size:11px;font-family:monospace;color:var(--ac)" id="pull-counts"></span>
        </div>
        <div style="background:var(--sf2);border-radius:8px;height:22px;overflow:hidden;position:relative;border:1px solid var(--bd)">
         <div id="pull-bar-fill" style="height:100%;border-radius:8px;transition:width .5s;background:linear-gradient(90deg,#4f8cff,#34d399);width:0%"></div>
         <span id="pull-bar-text" style="position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);font-size:11px;font-weight:600;color:#fff;white-space:nowrap">Starting...</span>
        </div>
        <div style="margin-top:6px;font-size:11px;color:var(--tx2)" id="pull-detail-text"></div>
        <div id="pull-errors" style="margin-top:6px;font-size:11px;color:#f87171;display:none"></div>
       </div>
       <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="g-btn g-btn-go" id="deep-pull-btn" onclick="startDeepPull()">🔍 Deep Pull SCPRS</button>
        <button class="g-btn g-btn-warn" onclick="seedDemo(this)">🌱 Load Demo Data</button>
        <button class="g-btn g-btn-purple" onclick="openAddBuyer()">➕ Add Buyer</button>
        <button class="g-btn" onclick="openImportCSV()">📥 Import CSV</button>
        <button class="g-btn" onclick="syncCRM(this)">👥 Sync → CRM</button>
        <button class="g-btn" onclick="pushProspects(this)">🚀 Push → Growth</button>
        <button class="g-btn" onclick="showPriorityQueue(this)">📊 Priority Queue</button>
       </div>
      </div>

      <!-- Buyer table -->
      <div class="card">
       <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <h3 style="margin:0">🔥 Buyer Database ({total_buyers})</h3>
        <input id="buyer-search" placeholder="Filter buyers..." style="padding:6px 10px;background:var(--sf2);border:1px solid var(--bd);border-radius:6px;color:var(--tx);font-size:12px;width:180px" oninput="filterBuyers()">
       </div>
       {'<div style="overflow-x:auto"><table id="buyer-table"><thead><tr><th>Agency</th><th>Name</th><th>Email</th><th>Categories</th><th>Spend</th><th>Score</th><th>Status</th><th></th></tr></thead><tbody id="buyer-tbody">' + ''.join(
           f'<tr data-search="{b.get("agency","").lower()} {b.get("name","").lower()} {b.get("email","").lower()}">'
           f'<td style="font-weight:600">{b.get("agency","—")}</td>'
           f'<td>{b.get("name") or b.get("buyer_name","—")}</td>'
           f'<td style="font-family:monospace;font-size:11px"><a href="mailto:{b.get("email","")}" style="color:var(--ac)">{b.get("email","—")}</a></td>'
           f'<td style="font-size:11px">{", ".join(list(b.get("categories",{}).keys())[:2])}</td>'
           f'<td class="mono" style="color:#3fb950">${b.get("total_spend",0):,.0f}</td>'
           f'<td class="mono" style="color:#a78bfa">{b.get("opportunity_score",0) or int((b.get("score",0) or 0)*100)}</td>'
           f'<td><span style="font-size:10px;padding:2px 8px;border-radius:8px;background:rgba(79,140,255,.15);color:var(--ac)">{b.get("outreach_status","new")}</span></td>'
           f'<td><a href="/growth/prospect/{b.get("id","")}" style="color:var(--ac);font-size:11px">View →</a></td>'
           f'</tr>'
           for b in buyers
       ) + '</tbody></table></div>' if has_buyers else '<div style="text-align:center;padding:32px;color:var(--tx2)"><div style="font-size:32px;margin-bottom:10px">📭</div><div style="font-size:14px;font-weight:600;margin-bottom:6px">No buyers yet</div><div style="font-size:13px;margin-bottom:16px">Use the buttons above to pull from SCPRS, import CSV, or add manually</div><button class="g-btn g-btn-warn" onclick="seedDemo(this)" style="margin:0 auto">🌱 Load Demo Data (15 CA agencies)</button></div>'}
      </div>

      <!-- Opportunity Agencies -->
      {'<div class="card"><h3>🎯 Opportunity Agencies (' + str(sum(1 for a in agencies if not a.get("is_customer"))) + ')</h3><div style="overflow-x:auto"><table><thead><tr><th>Agency</th><th>Total Spend</th><th>Score</th><th>Buyers</th><th>Categories</th></tr></thead><tbody>' + opp_rows + '</tbody></table></div></div>' if opp_rows else ''}

      <!-- Existing Customers -->
      {'<div class="card"><h3>🏆 Existing Customers — Upsell View</h3><table><thead><tr><th>Agency</th><th>Our Revenue</th><th>Their Total</th><th>Upsell Gap</th><th>Categories</th></tr></thead><tbody>' + customer_rows + '</tbody></table></div>' if customer_rows else ''}

     </div>

     <!-- Right column -->
     <div>

      <!-- Revenue Goal -->
      <div class="card">
       <h3>📈 Revenue Goal — 2026</h3>
       <div style="background:var(--sf2);border-radius:8px;height:22px;overflow:hidden;position:relative;margin-bottom:10px">
        <div style="background:{bar_color};height:100%;width:{pct}%;border-radius:8px;transition:width .5s"></div>
        <span style="position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);font-size:11px;font-weight:700;color:#fff">${closed:,.0f} / $2M</span>
       </div>
       <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:12px;margin-bottom:12px">
        <div style="background:var(--sf2);border-radius:6px;padding:8px;text-align:center"><div style="color:var(--tx2);font-size:9px;text-transform:uppercase">Gap</div><div style="font-weight:700;color:#f85149;font-family:monospace">${gap:,.0f}</div></div>
        <div style="background:var(--sf2);border-radius:6px;padding:8px;text-align:center"><div style="color:var(--tx2);font-size:9px;text-transform:uppercase">Mo. Needed</div><div style="font-weight:700;color:#fbbf24;font-family:monospace">${monthly:,.0f}</div></div>
        <div style="background:var(--sf2);border-radius:6px;padding:8px;text-align:center"><div style="color:var(--tx2);font-size:9px;text-transform:uppercase">Run Rate</div><div style="font-weight:700;color:{'#3fb950' if on_track else '#f87171'};font-family:monospace">${run_rate:,.0f}</div></div>
        <div style="background:var(--sf2);border-radius:6px;padding:8px;text-align:center"><div style="color:var(--tx2);font-size:9px;text-transform:uppercase">Pipeline</div><div style="font-weight:700;color:#58a6ff;font-family:monospace">${pipeline:,.0f}</div></div>
       </div>
       <div style="display:flex;gap:6px">
        <button class="g-btn g-btn-go" onclick="openLogRevenue()" style="flex:1;justify-content:center">💰 Log Revenue</button>
        <button class="g-btn" onclick="refreshRevenue(this)" style="padding:8px 10px">🔄</button>
       </div>
      </div>

      <!-- Pull status -->
      <div class="card">
       <h3>📡 Pull Status</h3>
       <div id="pull-status-card" style="font-size:12px">
        {'<div style="color:#f87171">⚠️ Last pull failed: ' + pull.get("progress","")[:80] + '</div>' if pull.get("phase") == "error" else '<div style="color:var(--tx2)">No pull run yet</div>' if not pull.get("phase") else '<div style="color:#3fb950">✅ ' + str(pull.get("progress",""))[:80] + '</div>'}
        {f'<div style="font-size:11px;color:var(--tx2);margin-top:6px">{pull.get("total_buyers",0)} buyers · {pull.get("total_agencies",0)} agencies · {pull.get("total_pos",0)} POs scanned</div>' if pull.get("total_buyers") else ''}
        {f'<div style="font-size:11px;color:var(--tx2);margin-top:4px">Finished: {str(pull.get("finished_at",""))[:16].replace("T"," ")}</div>' if pull.get("finished_at") else ''}
       </div>
      </div>

      <!-- Result output box -->
      <div id="result-wrap" style="display:none" class="card">
       <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <h3 style="margin:0" id="result-title">Result</h3>
        <button onclick="document.getElementById('result-wrap').style.display='none'" style="background:none;border:none;color:var(--tx2);cursor:pointer;font-size:16px">✕</button>
       </div>
       <div id="result-content" style="font-size:12px;line-height:1.6"></div>
      </div>

      <!-- CSV template -->
      <div class="card">
       <h3>📋 CSV Import Format</h3>
       <div style="font-size:11px;color:var(--tx2);margin-bottom:8px">Copy this template, fill it out, and click Import CSV:</div>
       <pre style="font-size:10px;background:var(--sf2);padding:10px;border-radius:6px;overflow-x:auto;color:var(--tx);line-height:1.4">agency,email,name,phone,categories,annual_spend,notes
CDCR,j.smith@cdcr.ca.gov,John Smith,916-445-1000,"Medical,Safety",125000,High priority
CalTrans,m.jones@dot.ca.gov,Mary Jones,916-654-2000,Office,45000,</pre>
       <button class="g-btn" onclick="copyTemplate(this)" style="margin-top:6px;font-size:11px;padding:5px 10px">📋 Copy Template</button>
      </div>

     </div>
    </div>

    <!-- Add Buyer Modal -->
    <div class="modal-bg" id="add-buyer-modal" onclick="if(event.target===this)closeModal('add-buyer-modal')">
     <div class="modal-box">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:18px">
       <span style="font-size:16px;font-weight:700">➕ Add Buyer Manually</span>
       <button onclick="closeModal('add-buyer-modal')" style="background:none;border:none;color:var(--tx2);cursor:pointer;font-size:20px">✕</button>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0 12px">
       <div><label class="form-lbl">Agency *</label><input id="ab-agency" class="form-input" placeholder="e.g. CDCR, CalTrans"></div>
       <div><label class="form-lbl">Email *</label><input id="ab-email" class="form-input" placeholder="buyer@agency.ca.gov"></div>
       <div><label class="form-lbl">Full Name</label><input id="ab-name" class="form-input" placeholder="First Last"></div>
       <div><label class="form-lbl">Phone</label><input id="ab-phone" class="form-input" placeholder="916-xxx-xxxx"></div>
      </div>
      <label class="form-lbl">Categories (comma-separated)</label>
      <input id="ab-categories" class="form-input" placeholder="e.g. Medical, Safety, Janitorial">
      <label class="form-lbl">Annual Spend ($)</label>
      <input id="ab-spend" class="form-input" type="number" placeholder="e.g. 75000">
      <label class="form-lbl">Notes</label>
      <textarea id="ab-notes" class="form-input" rows="2" placeholder="Any context about this buyer..."></textarea>
      <button onclick="submitAddBuyer()" class="g-btn g-btn-go" style="width:100%;justify-content:center;padding:12px;font-size:14px">✅ Add Buyer</button>
     </div>
    </div>

    <!-- Import CSV Modal -->
    <div class="modal-bg" id="csv-modal" onclick="if(event.target===this)closeModal('csv-modal')">
     <div class="modal-box">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:18px">
       <span style="font-size:16px;font-weight:700">📥 Import Buyers CSV</span>
       <button onclick="closeModal('csv-modal')" style="background:none;border:none;color:var(--tx2);cursor:pointer;font-size:20px">✕</button>
      </div>
      <div style="font-size:12px;color:var(--tx2);margin-bottom:10px">Paste CSV with headers: agency, email, name, phone, categories, annual_spend, notes</div>
      <textarea id="csv-input" class="form-input" rows="10" placeholder="agency,email,name,phone,categories,annual_spend,notes&#10;CDCR,j.smith@cdcr.ca.gov,John Smith,916-445-1000,&quot;Medical,Safety&quot;,125000,"></textarea>
      <div style="display:flex;gap:8px;margin-top:4px">
       <button onclick="submitCSV()" class="g-btn g-btn-go" style="flex:1;justify-content:center;padding:12px">📥 Import</button>
       <button onclick="closeModal('csv-modal')" class="g-btn" style="padding:12px 20px">Cancel</button>
      </div>
     </div>
    </div>

    <!-- Log Revenue Modal -->
    <div class="modal-bg" id="rev-modal" onclick="if(event.target===this)closeModal('rev-modal')">
     <div class="modal-box">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:18px">
       <span style="font-size:16px;font-weight:700">💰 Log Revenue</span>
       <button onclick="closeModal('rev-modal')" style="background:none;border:none;color:var(--tx2);cursor:pointer;font-size:20px">✕</button>
      </div>
      <label class="form-lbl">Amount ($) *</label>
      <input id="rev-amount" class="form-input" type="number" placeholder="e.g. 12500">
      <label class="form-lbl">Description *</label>
      <input id="rev-desc" class="form-input" placeholder="e.g. PO#12345 CDCR nitrile gloves">
      <label class="form-lbl">Date (optional)</label>
      <input id="rev-date" class="form-input" type="date">
      <button onclick="submitRevenue()" class="g-btn g-btn-go" style="width:100%;justify-content:center;padding:12px;font-size:14px">💰 Log Revenue</button>
     </div>
    </div>

    <script>
    // ── Utility ──
    function showResult(title, content, isError) {{
      document.getElementById('result-wrap').style.display = 'block';
      document.getElementById('result-title').textContent = title;
      const el = document.getElementById('result-content');
      el.style.color = isError ? '#f87171' : 'var(--tx)';
      if(typeof content === 'object') {{
        if(content.error) {{
          el.innerHTML = '<b style="color:#f87171">❌ ' + content.error + '</b>' +
            (content.hint ? '<br><br>💡 ' + content.hint : '') +
            (content.railway_guide ? '<br><a href="' + content.railway_guide + '" target="_blank" style="color:var(--ac)">📖 Railway guide →</a>' : '');
        }} else {{
          const lines = [];
          if(content.message) lines.push('✅ ' + content.message);
          if(content.created !== undefined) lines.push('Created: ' + content.created);
          if(content.updated !== undefined) lines.push('Updated: ' + content.updated);
          if(content.total_in_queue !== undefined) lines.push('In queue: ' + content.total_in_queue);
          if(content.queue) {{
            lines.push('');
            content.queue.slice(0,10).forEach(q => {{
              lines.push('• ' + (q.agency||'') + ' — ' + (q.email||'') + ' ($' + (q.total_spend||0).toLocaleString() + ')');
            }});
          }}
          if(content.errors && content.errors.length) lines.push('Errors: ' + content.errors.join(', '));
          el.innerHTML = lines.join('<br>') || JSON.stringify(content, null, 2);
        }}
      }} else {{
        el.textContent = content;
      }}
    }}

    function closeModal(id) {{ document.getElementById(id).style.display='none'; }}
    function crmPost(u,b){{return fetch(u,{{method:'POST',credentials:'same-origin',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(b)}}).then(r=>r.json())}}

    // ── Deep Pull ──
    function startDeepPull() {{
      const btn = document.getElementById('deep-pull-btn');
      btn.disabled = true; btn.textContent = '⏳ Starting...';
      fetch('/api/intel/deep-pull', {{credentials:'same-origin'}}).then(r=>r.json()).then(data => {{
        if(!data.ok) {{
          btn.disabled = false; btn.textContent = '🔍 Deep Pull SCPRS';
          showResult('Deep Pull Failed', data, true);
          // Show banner if SCPRS blocked
          if(data.error && (data.error.includes('static IP') || data.error.includes('blocked') || data.error.includes('proxy'))) {{
            document.getElementById('scprs-banner') && (document.getElementById('scprs-banner').style.display='block');
          }}
          return;
        }}
        document.getElementById('pull-progress-wrap').style.display = 'block';
        pollPull();
      }}).catch(e => {{
        btn.disabled = false; btn.textContent = '🔍 Deep Pull SCPRS';
        showResult('Error', 'Network error: ' + e, true);
      }});
    }}

    let pullTimer = null;
    function pollPull() {{
      if(pullTimer) clearInterval(pullTimer);
      pullTimer = setInterval(() => {{
        fetch('/api/intel/pull-status', {{credentials:'same-origin'}}).then(r=>r.json()).then(d => {{
          const total = d.queries_total || 1;
          const done = d.queries_done || 0;
          const pct = Math.min(99, Math.round((done/total)*100));
          document.getElementById('pull-bar-fill').style.width = pct + '%';
          document.getElementById('pull-bar-text').textContent = pct + '% (' + done + '/' + total + ')';
          const phaseMap = {{
            'init':'🔌 Connecting...','reytech_history':'📥 Phase 1: Reytech history',
            'category_scan':'🔍 Phase 2: Category scan','scoring':'📊 Scoring buyers',
            'saving':'💾 Saving','complete':'✅ Complete','error':'❌ Error'
          }};
          document.getElementById('pull-phase-label').textContent = phaseMap[d.phase] || d.phase || 'Running...';
          document.getElementById('pull-detail-text').textContent = d.progress || '';
          const counts = [];
          if(d.total_pos) counts.push(d.total_pos + ' POs');
          if(d.total_buyers) counts.push(d.total_buyers + ' buyers');
          if(d.total_agencies) counts.push(d.total_agencies + ' agencies');
          document.getElementById('pull-counts').textContent = counts.join(' · ');
          if(d.errors && d.errors.length) {{
            const e = document.getElementById('pull-errors');
            e.style.display='block'; e.textContent=d.errors.slice(-2).join('\\n');
          }}
          if(!d.running) {{
            clearInterval(pullTimer);
            document.getElementById('pull-bar-fill').style.width='100%';
            const btn = document.getElementById('deep-pull-btn');
            btn.disabled=false; btn.textContent='🔍 Deep Pull SCPRS';
            if(d.phase==='error') {{
              document.getElementById('pull-bar-fill').style.background='#f85149';
              document.getElementById('pull-bar-text').textContent='❌ Failed';
              showResult('Deep Pull Failed', {{error: d.progress||'Unknown error', hint: 'Enable Railway static IP, then retry.', railway_guide: 'https://docs.railway.app/reference/static-outbound-ips'}}, true);
            }} else {{
              document.getElementById('pull-bar-fill').style.background='#34d399';
              document.getElementById('pull-bar-text').textContent='✅ Done — syncing...';
              fetch('/api/crm/sync-intel',{{method:'POST',credentials:'same-origin'}}).then(r=>r.json()).then(sync => {{
                document.getElementById('pull-bar-text').textContent='✅ ' + (sync.created||0) + ' new contacts';
                setTimeout(()=>location.reload(), 1500);
              }}).catch(()=>setTimeout(()=>location.reload(),1500));
            }}
          }}
        }}).catch(()=>{{}});
      }}, 2000);
    }}

    // ── SCPRS Test ──
    function testSCPRS(btn) {{
      btn.disabled=true; btn.textContent='⏳ Testing...';
      fetch('/api/intel/scprs-test',{{credentials:'same-origin'}}).then(r=>r.json()).then(d => {{
        btn.disabled=false; btn.textContent='🔌 Test Connection';
        const dot = document.getElementById('scprs-dot');
        if(d.reachable) {{
          dot.textContent='✅ SCPRS Connected'; dot.style.color='#3fb950';
          dot.style.background='rgba(52,211,153,.15)';
          showResult('SCPRS Connection', '✅ Connected! ' + d.status_code + ' ' + d.elapsed_ms + 'ms', false);
        }} else {{
          dot.textContent='⚠️ SCPRS Offline'; dot.style.color='#f87171';
          dot.style.background='rgba(248,113,113,.15)';
          showResult('SCPRS Connection', {{error: d.error || 'Cannot reach SCPRS', hint: 'Enable Railway static IP to allow outbound connections to suppliers.fiscal.ca.gov', railway_guide: 'https://docs.railway.app/reference/static-outbound-ips'}}, true);
        }}
      }}).catch(e=>{{btn.disabled=false;btn.textContent='🔌 Test Connection';showResult('Error','Network error: '+e,true);}});
    }}

    // ── Seed Demo ──
    function seedDemo(btn) {{
      if(!confirm('Load 15 realistic CA agency contacts as demo data? This will add to any existing data.')) return;
      btn.disabled=true; btn.textContent='⏳ Loading...';
      crmPost('/api/intel/seed-demo',{{}}).then(d => {{
        btn.disabled=false; btn.textContent='🌱 Load Demo Data';
        if(d.ok) {{ showResult('Demo Data Loaded', d, false); setTimeout(()=>location.reload(), 1200); }}
        else showResult('Error', d, true);
      }}).catch(e=>{{btn.disabled=false;btn.textContent='🌱 Load Demo Data';showResult('Error',''+e,true);}});
    }}

    // ── Sync CRM ──
    function syncCRM(btn) {{
      btn.disabled=true; btn.textContent='⏳ Syncing...';
      crmPost('/api/crm/sync-intel',{{}}).then(d => {{
        btn.disabled=false; btn.textContent='👥 Sync → CRM';
        showResult('CRM Sync', d, !d.ok);
      }}).catch(e=>{{btn.disabled=false;btn.textContent='👥 Sync → CRM';showResult('Error',''+e,true);}});
    }}

    // ── Push Prospects ──
    function pushProspects(btn) {{
      btn.disabled=true; btn.textContent='⏳ Pushing...';
      fetch('/api/intel/push-prospects?top=50',{{credentials:'same-origin'}}).then(r=>r.json()).then(d => {{
        btn.disabled=false; btn.textContent='🚀 Push → Growth';
        showResult('Push to Growth', d, !d.ok);
        if(d.ok) setTimeout(()=>{{if(confirm('Pushed! Go to Growth page?')) location.href='/growth';}}, 500);
      }}).catch(e=>{{btn.disabled=false;btn.textContent='🚀 Push → Growth';showResult('Error',''+e,true);}});
    }}

    // ── Priority Queue ──
    function showPriorityQueue(btn) {{
      btn.disabled=true; btn.textContent='⏳ Loading...';
      fetch('/api/intel/priority-queue',{{credentials:'same-origin'}}).then(r=>r.json()).then(d => {{
        btn.disabled=false; btn.textContent='📊 Priority Queue';
        showResult('Priority Queue', d, !d.ok);
      }}).catch(e=>{{btn.disabled=false;btn.textContent='📊 Priority Queue';showResult('Error',''+e,true);}});
    }}

    // ── Add Buyer ──
    function openAddBuyer() {{ document.getElementById('add-buyer-modal').style.display='flex'; setTimeout(()=>document.getElementById('ab-agency').focus(),100); }}
    function submitAddBuyer() {{
      const agency = document.getElementById('ab-agency').value.trim();
      const email = document.getElementById('ab-email').value.trim();
      if(!agency||!email) {{ alert('Agency and Email are required'); return; }}
      const cats = document.getElementById('ab-categories').value.split(',').map(s=>s.trim()).filter(Boolean);
      crmPost('/api/intel/buyers/add', {{
        agency, email,
        name: document.getElementById('ab-name').value,
        phone: document.getElementById('ab-phone').value,
        categories: cats,
        annual_spend: parseFloat(document.getElementById('ab-spend').value||'0'),
        notes: document.getElementById('ab-notes').value,
      }}).then(d => {{
        if(d.ok) {{ closeModal('add-buyer-modal'); showResult('Buyer Added', d, false); setTimeout(()=>location.reload(),1000); }}
        else showResult('Error', d, true);
      }});
    }}

    // ── Import CSV ──
    function openImportCSV() {{ document.getElementById('csv-modal').style.display='flex'; setTimeout(()=>document.getElementById('csv-input').focus(),100); }}
    function submitCSV() {{
      const csv = document.getElementById('csv-input').value.trim();
      if(!csv) {{ alert('Paste CSV data first'); return; }}
      crmPost('/api/intel/buyers/import-csv', {{csv}}).then(d => {{
        if(d.ok) {{ closeModal('csv-modal'); showResult('CSV Import', d, false); setTimeout(()=>location.reload(),1000); }}
        else showResult('Error', d, true);
      }});
    }}

    // ── Revenue ──
    function openLogRevenue() {{ document.getElementById('rev-modal').style.display='flex'; setTimeout(()=>document.getElementById('rev-amount').focus(),100); }}
    function submitRevenue() {{
      const amount = parseFloat(document.getElementById('rev-amount').value||'0');
      const desc = document.getElementById('rev-desc').value.trim();
      if(!amount||!desc) {{ alert('Amount and Description required'); return; }}
      crmPost('/api/intel/revenue', {{amount, description:desc, date:document.getElementById('rev-date').value}}).then(d => {{
        if(d.ok) {{ closeModal('rev-modal'); showResult('Revenue Logged', d, false); setTimeout(()=>location.reload(),800); }}
        else showResult('Error', d, true);
      }});
    }}
    function refreshRevenue(btn) {{
      btn.disabled=true; btn.textContent='⏳';
      fetch('/api/intel/revenue',{{credentials:'same-origin'}}).then(r=>r.json()).then(d => {{
        btn.disabled=false; btn.textContent='🔄';
        if(d.ok) location.reload(); else showResult('Error', d, true);
      }}).catch(e=>{{btn.disabled=false;btn.textContent='🔄';}});
    }}

    // ── Buyer filter ──
    function filterBuyers() {{
      const q = document.getElementById('buyer-search').value.toLowerCase();
      document.querySelectorAll('#buyer-tbody tr').forEach(r => {{
        r.style.display = !q || (r.dataset.search||'').includes(q) ? '' : 'none';
      }});
    }}

    // ── Copy CSV template ──
    function copyTemplate(btn) {{
      navigator.clipboard.writeText('agency,email,name,phone,categories,annual_spend,notes\\nCDCR,j.smith@cdcr.ca.gov,John Smith,916-445-1000,"Medical,Safety",125000,High priority\\nCalTrans,m.jones@dot.ca.gov,Mary Jones,916-654-2000,Office,45000,').then(()=>{{btn.textContent='✅ Copied!';setTimeout(()=>btn.textContent='📋 Copy Template',2000);}});
    }}

    {f'pollPull();' if pull_running else ''}
    </script>
    </body></html>"""


# ─── Voice Agent Routes ─────────────────────────────────────────────────────

@bp.route("/api/voice/call", methods=["POST"])
@auth_required
def api_voice_call():
    """Place an outbound call. POST JSON: {"phone": "+19165550100", "script": "lead_intro", "variables": {...}}"""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    data = request.get_json(silent=True) or {}
    phone = data.get("phone", "")
    if not phone:
        return jsonify({"ok": False, "error": "Provide phone number in E.164 format"})
    # Inject server URL for Vapi function calling webhook
    variables = data.get("variables", {})
    variables["server_url"] = request.url_root.rstrip("/").replace("http://", "https://") + "/api/voice/webhook"
    result = place_call(phone, script_key=data.get("script", "lead_intro"),
                        variables=variables)
    # CRM: log call
    ref_id = data.get("variables", {}).get("quote_number", "") or data.get("variables", {}).get("po_number", "")
    _log_crm_activity(ref_id or "outbound", "voice_call",
                      f"Outbound call to {phone} ({data.get('script','lead_intro')})" +
                      (" — " + result.get("call_sid", "") if result.get("ok") else " — FAILED"),
                      actor="user", metadata={"phone": phone, "script": data.get("script",""),
                                               "institution": data.get("variables",{}).get("institution","")})
    return jsonify(result)


@bp.route("/api/voice/log")
@auth_required
def api_voice_log():
    """Get call log."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    limit = int(request.args.get("limit", 50))
    return jsonify({"ok": True, "calls": get_call_log(limit=limit)})


@bp.route("/api/voice/scripts")
@auth_required
def api_voice_scripts():
    """Get available call scripts."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    return jsonify({"ok": True, "scripts": VOICE_SCRIPTS})


@bp.route("/api/voice/status")
@auth_required
def api_voice_status():
    """Voice agent status + setup instructions."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    return jsonify({"ok": True, **voice_agent_status()})


@bp.route("/api/voice/verify")
@auth_required
def api_voice_verify():
    """Verify Twilio credentials are valid by pinging the API."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    return jsonify(voice_verify())


@bp.route("/api/voice/import-twilio", methods=["POST"])
@auth_required
def api_voice_import_twilio():
    """Import Twilio phone number into Vapi for Reytech caller ID."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    return jsonify(import_twilio_to_vapi())


@bp.route("/api/voice/webhook", methods=["POST"])
def api_voice_vapi_webhook():
    """Vapi server URL webhook — handles function calls during live conversations.
    No auth required — Vapi calls this endpoint during active calls."""
    data = request.get_json(silent=True) or {}
    msg_type = data.get("message", {}).get("type", "")

    if msg_type == "function-call":
        fn = data.get("message", {}).get("functionCall", {})
        fn_name = fn.get("name", "")
        fn_params = fn.get("parameters", {})

        try:
            from src.agents.voice_knowledge import handle_tool_call
            result = handle_tool_call(fn_name, fn_params)
            return jsonify({"results": [{"result": result}]})
        except Exception as e:
            log.error("Vapi webhook tool call failed: %s", e)
            return jsonify({"results": [{"result": "I couldn't look that up right now."}]})

    elif msg_type == "end-of-call-report":
        # Log transcript to CRM
        call = data.get("message", {}).get("call", {})
        transcript = data.get("message", {}).get("transcript", "")
        summary = data.get("message", {}).get("summary", "")
        call_id = call.get("id", "")
        phone = call.get("customer", {}).get("number", "")

        if call_id:
            _log_crm_activity(call_id, "voice_call_completed",
                              f"Call to {phone} completed" + (f" — {summary[:200]}" if summary else ""),
                              actor="system", metadata={
                                  "call_id": call_id,
                                  "phone": phone,
                                  "transcript": transcript[:2000] if transcript else "",
                                  "summary": summary[:500] if summary else "",
                                  "duration": data.get("message", {}).get("durationSeconds", 0),
                              })
        return jsonify({"ok": True})

    return jsonify({"ok": True})


@bp.route("/api/voice/vapi-calls")
@auth_required
def api_voice_vapi_calls():
    """List recent Vapi calls with transcripts."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    limit = int(request.args.get("limit", 20))
    calls = get_vapi_calls(limit=limit)
    return jsonify({"ok": True, "calls": calls, "count": len(calls)})


@bp.route("/api/voice/call/<call_id>/details")
@auth_required
def api_voice_call_details(call_id):
    """Get Vapi call details including transcript."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    details = get_vapi_call_details(call_id)
    return jsonify({"ok": not bool(details.get("error")), **details})


# ─── CRM / Contacts Route ────────────────────────────────────────────────────

@bp.route("/contacts")
@auth_required
def contacts_page():
    """CRM — Persistent buyer/contact database with activity tracking."""
    contacts_dict = _load_crm_contacts()

    # Also pull from growth prospects if contacts store is empty
    if not contacts_dict and GROWTH_AVAILABLE:
        try:
            from src.agents.growth_agent import _load_json, PROSPECTS_FILE
            pd = _load_json(PROSPECTS_FILE)
            prospects = pd.get("prospects",[]) if isinstance(pd,dict) else []
            for p in prospects[:200]:
                cid = p.get("id","")
                if cid:
                    contacts_dict[cid] = {
                        "id": cid, "buyer_name": p.get("buyer_name",""),
                        "buyer_email": p.get("buyer_email",""), "buyer_phone": p.get("buyer_phone",""),
                        "agency": p.get("agency",""), "title":"", "linkedin":"", "notes":"", "tags":[],
                        "total_spend": p.get("total_spend",0), "po_count": p.get("po_count",0),
                        "categories": p.get("categories",{}), "items_purchased": p.get("items_purchased",[]),
                        "purchase_orders": p.get("purchase_orders",[]),
                        "last_purchase": p.get("last_purchase",""),
                        "score": p.get("score",0), "outreach_status": p.get("outreach_status","new"), "activity":[],
                    }
        except Exception:
            pass

    contacts = list(contacts_dict.values())
    total = len(contacts)
    has_data = total > 0

    # Aggregate stats
    total_spend = sum(c.get("total_spend",0) for c in contacts)
    agencies = len(set(c.get("agency","") for c in contacts if c.get("agency")))
    in_outreach = sum(1 for c in contacts if c.get("outreach_status") not in ("new",""))
    total_activity = sum(len(c.get("activity",[])) for c in contacts)
    won_count = sum(1 for c in contacts if c.get("outreach_status")=="won")

    # Collect all categories + statuses for filters
    all_cats = sorted(set(cat for c in contacts for cat in c.get("categories",{}).keys()))
    all_statuses = sorted(set(c.get("outreach_status","new") for c in contacts if c.get("outreach_status")))

    # Sort by score desc
    contacts.sort(key=lambda x: (x.get("score",0) or 0), reverse=True)

    def fmt_spend(v):
        if not v: return "$0"
        if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
        if v >= 1_000: return f"${v/1_000:.0f}K"
        return f"${v:,.0f}"

    stat_colors = {"new":"#4f8cff","emailed":"#fbbf24","called":"#fb923c","responded":"#a78bfa",
                   "won":"#3fb950","lost":"#f87171","dead":"#8b90a0","bounced":"#f85149","follow_up_due":"#d29922"}
    cat_colors = {"Medical":"#f87171","Janitorial":"#3fb950","Office":"#4f8cff","IT":"#a78bfa","Facility":"#fb923c","Safety":"#fbbf24"}

    rows_html = ""
    for c in contacts[:500]:
        cid = c.get("id","")
        name = c.get("buyer_name") or "—"
        email = c.get("buyer_email","")
        agency = c.get("agency","—")
        stat = c.get("outreach_status","new")
        sc = stat_colors.get(stat,"#8b90a0")
        spend = c.get("total_spend",0) or 0
        po_count = c.get("po_count",0) or len(c.get("purchase_orders",[]))
        score = c.get("score",0) or 0
        score_pct = round(score*100) if score<=1 else round(score)
        last = (c.get("last_purchase","") or "")[:10] or "—"
        act_count = len(c.get("activity",[]))
        categories = c.get("categories",{})
        items = c.get("items_purchased",[])

        # Category tags (top 3)
        cat_tags = ""
        for cat in list(categories.keys())[:3]:
            cc = cat_colors.get(cat,"#8b90a0")
            cat_tags += f'<span style="font-size:10px;padding:2px 7px;border-radius:8px;background:{cc}22;color:{cc};border:1px solid {cc}44;white-space:nowrap">{cat}</span> '

        # Items (first 2)
        items_text = ", ".join(it.get("description","")[:30] for it in items[:2])
        if len(items) > 2: items_text += f" +{len(items)-2}"

        # Score bar
        sp_color = "#3fb950" if score_pct>=70 else "#fbbf24" if score_pct>=40 else "#f87171"
        score_bar = f'<div style="display:flex;align-items:center;gap:6px"><div style="background:var(--sf2);border-radius:3px;height:6px;width:50px;overflow:hidden"><div style="width:{score_pct}%;height:100%;background:{sp_color};border-radius:3px"></div></div><span style="font-size:11px;font-family:monospace">{score_pct}%</span></div>'

        # Activity badge
        act_badge = f'<span style="font-size:11px;background:rgba(79,140,255,.15);color:var(--ac);padding:2px 8px;border-radius:8px">{act_count} 📋</span>' if act_count > 0 else '<span style="font-size:11px;color:var(--tx2)">—</span>'

        rows_html += f'''<tr data-agency="{agency.lower()}" data-name="{name.lower()}" data-email="{email.lower()}" data-cats="{','.join(categories.keys()).lower()}" data-status="{stat}" data-items="{items_text.lower()}" style="cursor:pointer" onclick="location.href='/growth/prospect/{cid}'">
         <td><div style="font-weight:600;font-size:13px">{agency}</div><div style="font-size:11px;color:var(--tx2)">{name}</div></td>
         <td style="font-size:12px"><a href="mailto:{email}" style="color:var(--ac);font-family:monospace" onclick="event.stopPropagation()">{email or '—'}</a></td>
         <td><div style="display:flex;flex-wrap:wrap;gap:3px">{cat_tags}</div></td>
         <td style="font-size:11px;color:var(--tx2);max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{items_text or '—'}</td>
         <td class="mono" style="color:#3fb950;font-weight:700">{fmt_spend(spend)}</td>
         <td class="mono" style="color:var(--tx2)">{po_count} · {last}</td>
         <td>{score_bar}</td>
         <td><span style="padding:3px 10px;border-radius:10px;font-size:11px;font-weight:600;background:{sc}22;color:{sc};border:1px solid {sc}44">{stat}</span></td>
         <td>{act_badge}</td>
         <td><a href="/growth/prospect/{cid}" style="color:var(--ac);font-size:12px;text-decoration:none">View →</a></td>
        </tr>'''

    cat_options = "".join(f'<option value="{c}">{c}</option>' for c in all_cats)
    status_options = "".join(f'<option value="{s}">{s}</option>' for s in all_statuses)

    empty_html = """<div style="text-align:center;padding:60px 20px;color:var(--tx2)">
      <div style="font-size:48px;margin-bottom:16px">👥</div>
      <div style="font-size:18px;font-weight:600;margin-bottom:8px">No contacts yet</div>
      <div style="font-size:14px;margin-bottom:24px">Run a Deep Pull on the Intelligence page to mine all SCPRS buyers into CRM</div>
      <a href="/intelligence" style="padding:12px 24px;background:rgba(52,211,153,.15);color:#3fb950;border:1px solid rgba(52,211,153,.3);border-radius:8px;text-decoration:none;font-weight:600">🧠 Go to Intelligence → Run Deep Pull</a>
     </div>""" if not has_data else ""

    return f"""{_header('CRM Contacts')}
    <style>
     .card{{background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:14px}}
     table{{width:100%;border-collapse:collapse;font-size:12px}}
     th{{text-align:left;padding:9px 10px;font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--bd);white-space:nowrap;cursor:pointer;user-select:none}}
     th:hover{{color:var(--tx)}}
     td{{padding:9px 10px;border-bottom:1px solid rgba(46,51,69,.4);vertical-align:middle}}
     tr:hover td{{background:rgba(79,140,255,.04)}}
     .mono{{font-family:'JetBrains Mono',monospace}}
     .filter-input{{padding:8px 12px;background:var(--sf2);border:1px solid var(--bd);border-radius:7px;color:var(--tx);font-size:13px;font-family:'DM Sans',sans-serif}}
     .filter-input:focus{{outline:none;border-color:var(--ac)}}
    </style>

    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:18px;flex-wrap:wrap;gap:12px">
     <div>
      <h1 style="font-size:22px;font-weight:700;margin-bottom:4px">👥 CRM Contacts</h1>
      <div style="font-size:13px;color:var(--tx2)">All buyers from SCPRS — tagged, scored, with full activity history</div>
     </div>
     <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button onclick="syncFromIntel(this)" style="padding:8px 16px;border-radius:7px;border:1px solid rgba(52,211,153,.3);background:rgba(52,211,153,.1);color:#3fb950;cursor:pointer;font-size:13px;font-weight:600">🔄 Sync from Intel</button>
      <a href="/intelligence" style="padding:8px 16px;border-radius:7px;border:1px solid var(--bd);background:var(--sf2);color:var(--tx);text-decoration:none;font-size:13px;font-weight:600">🧠 Run Deep Pull</a>
     </div>
    </div>

    <!-- Stats bar -->
    <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:18px">
     <div class="card" style="text-align:center"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Contacts</div><div style="font-size:26px;font-weight:700;color:var(--ac);font-family:monospace">{total}</div></div>
     <div class="card" style="text-align:center"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Agencies</div><div style="font-size:26px;font-weight:700;color:#a78bfa;font-family:monospace">{agencies}</div></div>
     <div class="card" style="text-align:center"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Total Spend</div><div style="font-size:22px;font-weight:700;color:#fbbf24;font-family:monospace">{fmt_spend(total_spend)}</div></div>
     <div class="card" style="text-align:center"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Interactions</div><div style="font-size:26px;font-weight:700;color:#fb923c;font-family:monospace">{total_activity}</div></div>
     <div class="card" style="text-align:center"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase;margin-bottom:4px">Won</div><div style="font-size:26px;font-weight:700;color:#3fb950;font-family:monospace">{won_count}</div></div>
    </div>

    <!-- Filters -->
    <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px;align-items:center">
     <input id="search" class="filter-input" placeholder="🔍  Search agency, name, email, items..." oninput="filterTable()" style="flex:1;min-width:220px">
     <select id="cat-filter" class="filter-input" onchange="filterTable()">
      <option value="">All Categories</option>{cat_options}
     </select>
     <select id="status-filter" class="filter-input" onchange="filterTable()">
      <option value="">All Statuses</option>{status_options}
     </select>
     <span id="count-label" style="font-size:13px;color:var(--tx2);white-space:nowrap">{total} contacts</span>
    </div>

    <!-- Table -->
    <div class="card" style="overflow-x:auto;padding:0">
     {empty_html if not has_data else f'''<table id="crm-table">
      <thead><tr>
       <th onclick="sortTable(0)">Agency / Buyer ↕</th>
       <th>Email</th>
       <th>Categories</th>
       <th>Items Bought</th>
       <th onclick="sortTable(4)">Spend ↕</th>
       <th onclick="sortTable(5)">POs · Last Buy ↕</th>
       <th onclick="sortTable(6)">Score ↕</th>
       <th>Status</th>
       <th>Activity</th>
       <th></th>
      </tr></thead>
      <tbody id="crm-tbody">{rows_html}</tbody>
     </table>'''}
    </div>

    <script>
    function filterTable() {{
      const q = document.getElementById('search').value.toLowerCase();
      const cat = document.getElementById('cat-filter').value.toLowerCase();
      const status = document.getElementById('status-filter').value;
      const rows = document.querySelectorAll('#crm-tbody tr');
      let visible = 0;
      rows.forEach(r => {{
        const agency = r.dataset.agency||'';
        const name = r.dataset.name||'';
        const email = r.dataset.email||'';
        const cats = r.dataset.cats||'';
        const stat = r.dataset.status||'';
        const items = r.dataset.items||'';
        const matchQ = !q || agency.includes(q) || name.includes(q) || email.includes(q) || items.includes(q);
        const matchCat = !cat || cats.includes(cat);
        const matchStat = !status || stat === status;
        const show = matchQ && matchCat && matchStat;
        r.style.display = show ? '' : 'none';
        if(show) visible++;
      }});
      document.getElementById('count-label').textContent = visible + ' contacts';
    }}

    let sortDir = {{}};
    function sortTable(col) {{
      const tbody = document.getElementById('crm-tbody');
      if(!tbody) return;
      const rows = Array.from(tbody.querySelectorAll('tr'));
      const dir = sortDir[col] = -(sortDir[col]||1);
      rows.sort((a,b) => {{
        const av = a.cells[col]?.textContent?.trim()||'';
        const bv = b.cells[col]?.textContent?.trim()||'';
        const an = parseFloat(av.replace(/[$KMk,]/g,''));
        const bn = parseFloat(bv.replace(/[$KMk,]/g,''));
        if(!isNaN(an)&&!isNaN(bn)) return (an-bn)*dir;
        return av.localeCompare(bv)*dir;
      }});
      rows.forEach(r => tbody.appendChild(r));
    }}

    function syncFromIntel(btn) {{
      btn.disabled = true; btn.textContent = '⏳ Syncing...';
      fetch('/api/crm/sync-intel', {{method:'POST',credentials:'same-origin'}}).then(r=>r.json()).then(d => {{
        if(d.ok) {{
          btn.textContent = '✅ ' + (d.message||'Synced');
          setTimeout(() => location.reload(), 1500);
        }} else {{
          btn.disabled = false; btn.textContent = '🔄 Sync from Intel';
          alert(d.error||'Sync failed');
        }}
      }}).catch(e => {{
        btn.disabled = false; btn.textContent = '🔄 Sync from Intel';
        alert('Error: '+e);
      }});
    }}
    </script>
    </body></html>"""


# ─── Campaign Routes ────────────────────────────────────────────────────────

@bp.route("/campaigns")
@auth_required
def campaigns_page():
    """Campaigns management page."""
    campaigns = get_campaigns() if CAMPAIGNS_AVAILABLE else []
    stats = get_campaign_stats() if CAMPAIGNS_AVAILABLE else {}
    scripts = list(VOICE_SCRIPTS.items()) if VOICE_AVAILABLE else []

    # Script options for dropdowns
    script_options = ""
    for key, sc in scripts:
        cat = sc.get("category", "other")
        script_options += f'<option value="{key}">[{cat}] {sc["name"]}</option>'

    # Source options
    source_options = """
    <option value="manual">Manual (add contacts)</option>
    <option value="hot_leads">🔥 Hot Leads (score ≥ 70%)</option>
    <option value="pending_quotes">📋 Pending Quotes (follow-up)</option>
    <option value="lost_quotes">❌ Lost Quotes (recovery)</option>
    <option value="won_customers">✅ Won Customers (thank you)</option>
    <option value="dormant">💤 Dormant Accounts (reactivation)</option>
    """

    # Campaign rows
    camp_rows = ""
    for c in campaigns[:20]:
        st = c.get("status", "draft")
        st_color = {"draft": "var(--tx2)", "active": "var(--gn)", "paused": "var(--yl)", "completed": "var(--ac)"}
        called = c["stats"]["called"]
        total = c["stats"]["total"]
        reached = c["stats"]["reached"]
        pct = round(called / total * 100) if total > 0 else 0
        camp_rows += f"""<tr>
         <td><a href="/campaign/{c['id']}" style="color:var(--ac);text-decoration:none;font-weight:600">{c['name']}</a></td>
         <td style="color:{st_color.get(st,'var(--tx2)')};font-weight:600">{st}</td>
         <td>{c.get('script_key','?')}</td>
         <td style="text-align:center">{total}</td>
         <td style="text-align:center">{called}/{total} ({pct}%)</td>
         <td style="text-align:center">{reached}</td>
         <td class="mono" style="font-size:11px">{c.get('created_at','')[:10]}</td>
        </tr>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
     <h1 style="margin:0">📞 Voice Campaigns</h1>
     <button class="btn btn-p" onclick="document.getElementById('new-camp').style.display='block'" style="padding:8px 16px">+ New Campaign</button>
    </div>

    <!-- Stats bar -->
    <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:16px">
     <div class="card" style="text-align:center;padding:12px"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Campaigns</div><div style="font-size:24px;font-weight:700">{stats.get('total_campaigns',0)}</div></div>
     <div class="card" style="text-align:center;padding:12px"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Total Calls</div><div style="font-size:24px;font-weight:700">{stats.get('total_called',0)}</div></div>
     <div class="card" style="text-align:center;padding:12px"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Connect Rate</div><div style="font-size:24px;font-weight:700;color:var(--gn)">{stats.get('connect_rate',0)}%</div></div>
     <div class="card" style="text-align:center;padding:12px"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Interested</div><div style="font-size:24px;font-weight:700;color:var(--ac)">{stats.get('total_interested',0)}</div></div>
     <div class="card" style="text-align:center;padding:12px"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Est. Cost</div><div style="font-size:24px;font-weight:700">${stats.get('estimated_cost',0):.2f}</div></div>
    </div>

    <!-- New Campaign Form (hidden) -->
    <div id="new-camp" class="card" style="display:none;margin-bottom:16px;padding:16px">
     <div class="card-t" style="margin-bottom:12px">New Campaign</div>
     <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
      <div>
       <label style="font-size:11px;color:var(--tx2)">Campaign Name</label>
       <input id="camp-name" placeholder="Feb CDCR Outreach" style="width:100%;padding:8px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);margin-top:4px">
      </div>
      <div>
       <label style="font-size:11px;color:var(--tx2)">Contact Source</label>
       <select id="camp-source" style="width:100%;padding:8px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);margin-top:4px">{source_options}</select>
      </div>
      <div>
       <label style="font-size:11px;color:var(--tx2)">Default Script</label>
       <select id="camp-script" style="width:100%;padding:8px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);margin-top:4px">{script_options}</select>
      </div>
      <div>
       <label style="font-size:11px;color:var(--tx2)">Filter (agency)</label>
       <input id="camp-filter" placeholder="CDCR, CCHCS, etc." style="width:100%;padding:8px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);margin-top:4px">
      </div>
     </div>
     <div style="margin-top:12px;display:flex;gap:8px">
      <button class="btn btn-p" onclick="createCampaign()" style="padding:8px 20px">Create Campaign</button>
      <button class="btn" onclick="document.getElementById('new-camp').style.display='none'" style="padding:8px 20px">Cancel</button>
     </div>
    </div>

    <!-- Available Scripts -->
    <div class="card" style="margin-bottom:16px;padding:16px">
     <div class="card-t" style="margin-bottom:10px">📜 {len(scripts)} Call Scripts Available</div>
     <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:8px">
      {''.join(f'<div style="padding:8px;background:var(--sf2);border-radius:8px;font-size:12px"><span style="color:var(--ac);font-weight:600">{sc["name"]}</span><br><span style="color:var(--tx2);font-size:10px">[{sc.get("category","?")}] {key}</span></div>' for key, sc in scripts)}
     </div>
    </div>

    <!-- Campaigns Table -->
    <div class="card" style="padding:16px">
     <div class="card-t" style="margin-bottom:10px">Campaigns</div>
     <table class="tbl" style="width:100%">
      <thead><tr>
       <th>Campaign</th><th>Status</th><th>Script</th><th>Contacts</th><th>Progress</th><th>Reached</th><th>Created</th>
      </tr></thead>
      <tbody>{camp_rows if camp_rows else '<tr><td colspan="7" style="text-align:center;color:var(--tx2);padding:20px">No campaigns yet — create one above</td></tr>'}</tbody>
     </table>
    </div>

    <script>
    function createCampaign() {{
      const name = document.getElementById('camp-name').value;
      if (!name) {{ alert('Enter a campaign name'); return; }}
      const source = document.getElementById('camp-source').value;
      const script = document.getElementById('camp-script').value;
      const agency = document.getElementById('camp-filter').value;
      fetch('/api/campaigns', {{
        method: 'POST', credentials: 'same-origin',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{name, script_key: script, target_type: source, filters: {{agency: agency}}}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{ location.reload(); }} else {{ alert(d.error || 'Failed'); }}
      }});
    }}
    </script>
    """
    return render(content, title="Voice Campaigns")


@bp.route("/campaign/<cid>")
@auth_required
def campaign_detail(cid):
    """Campaign detail page with contact list and dialer."""
    if not CAMPAIGNS_AVAILABLE:
        return redirect("/campaigns")
    camp = get_campaign(cid)
    if not camp:
        flash("Campaign not found", "error")
        return redirect("/campaigns")

    contacts = camp.get("contacts", [])
    stats = camp.get("stats", {})
    pending = [c for c in contacts if c.get("status") == "pending"]
    called = [c for c in contacts if c.get("status") == "called"]

    # Contact rows
    contact_rows = ""
    for i, c in enumerate(contacts):
        outcome = c.get("outcome", "")
        outcome_color = {"reached": "var(--gn)", "voicemail": "var(--yl)", "interested": "var(--ac)",
                         "no_answer": "var(--tx2)", "callback": "var(--warn)", "not_interested": "var(--rd)"}.get(outcome, "var(--tx2)")
        phone = c.get("phone", "")
        dial_btn = f'<button class="btn btn-sm" onclick="dialContact({i})" style="background:rgba(52,211,153,.15);color:var(--gn);border:1px solid rgba(52,211,153,.3);padding:2px 8px;font-size:10px">📞 Dial</button>' if c["status"] == "pending" and phone else ""
        outcome_btn = f'<select onchange="logOutcome(\'{phone}\',this.value)" style="font-size:10px;padding:2px;background:var(--sf);border:1px solid var(--bd);border-radius:4px;color:var(--tx)"><option value="">Log outcome...</option><option value="reached">✅ Reached</option><option value="voicemail">📱 Voicemail</option><option value="no_answer">❌ No Answer</option><option value="callback">📞 Callback</option><option value="interested">🎯 Interested</option><option value="not_interested">👎 Not Interested</option><option value="gatekeeper">🚪 Gatekeeper</option></select>' if c["status"] == "pending" or (c["status"] == "called" and not outcome) else ""

        contact_rows += f"""<tr>
         <td style="font-weight:500">{c.get('name','?')}</td>
         <td class="mono" style="font-size:11px">{phone or '<span style=\"color:var(--rd)\">no phone</span>'}</td>
         <td style="font-size:11px">{c.get('institution','')}</td>
         <td style="font-size:11px">{c.get('script', camp.get('script_key',''))}</td>
         <td style="text-align:center"><span style="color:{outcome_color};font-weight:600;font-size:11px">{outcome or c.get('status','')}</span></td>
         <td style="text-align:center;white-space:nowrap">{dial_btn} {outcome_btn}</td>
        </tr>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
     <div>
      <h1 style="margin:0">{camp['name']}</h1>
      <div style="color:var(--tx2);font-size:12px;margin-top:4px">{camp.get('contact_source', camp.get('target_type',''))} • {camp.get('script_key','')} • {len(contacts)} contacts</div>
     </div>
     <div style="display:flex;gap:8px">
      <a href="/campaigns" class="btn" style="padding:8px 16px">← Back</a>
      <button class="btn btn-p" onclick="dialNext()" style="padding:8px 16px" {'disabled' if not pending else ''}>📞 Dial Next ({len(pending)} remaining)</button>
     </div>
    </div>

    <!-- Stats -->
    <div style="display:grid;grid-template-columns:repeat(6,1fr);gap:8px;margin-bottom:16px">
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Total</div><div style="font-size:20px;font-weight:700">{stats.get('total',0)}</div></div>
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Called</div><div style="font-size:20px;font-weight:700">{stats.get('called',0)}</div></div>
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Reached</div><div style="font-size:20px;font-weight:700;color:var(--gn)">{stats.get('reached',0)}</div></div>
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Voicemail</div><div style="font-size:20px;font-weight:700;color:var(--yl)">{stats.get('voicemail',0)}</div></div>
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Interested</div><div style="font-size:20px;font-weight:700;color:var(--ac)">{stats.get('interested',0)}</div></div>
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Callback</div><div style="font-size:20px;font-weight:700;color:var(--warn)">{stats.get('callback',0)}</div></div>
    </div>

    <!-- Contact List -->
    <div class="card" style="padding:16px">
     <div class="card-t" style="margin-bottom:10px">Contact List</div>
     <table class="tbl" style="width:100%">
      <thead><tr><th>Name</th><th>Phone</th><th>Institution</th><th>Script</th><th>Outcome</th><th>Actions</th></tr></thead>
      <tbody>{contact_rows}</tbody>
     </table>
    </div>

    <script>
    function dialContact(idx) {{
      fetch('/api/campaigns/{cid}/call', {{
        method: 'POST', credentials: 'same-origin',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{target_index: idx}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{ alert('Call placed: ' + (d.call_id||d.call_sid||'queued')); location.reload(); }}
        else {{ alert(d.error || 'Call failed'); }}
      }});
    }}
    function dialNext() {{
      fetch('/api/campaigns/{cid}/call', {{
        method: 'POST', credentials: 'same-origin',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{ alert('Calling: ' + (d.to||'next contact')); location.reload(); }}
        else {{ alert(d.error || 'No more contacts'); }}
      }});
    }}
    function logOutcome(phone, outcome) {{
      if (!outcome) return;
      fetch('/api/campaigns/{cid}/outcome', {{
        method: 'POST', credentials: 'same-origin',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{phone, outcome}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) location.reload();
      }});
    }}
    </script>
    """
    return render(content, title=f"Campaign: {camp['name']}")


@bp.route("/api/campaigns", methods=["GET", "POST"])
@auth_required
def api_campaigns():
    """List or create campaigns."""
    if not CAMPAIGNS_AVAILABLE:
        return jsonify({"ok": False, "error": "Campaigns not available"})
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        result = create_campaign(
            name=data.get("name", "Untitled"),
            script_key=data.get("script_key", "lead_intro"),
            target_type=data.get("target_type", "manual"),
            filters=data.get("filters", {}),
        )
        return jsonify({"ok": True, **result})
    return jsonify({"ok": True, "campaigns": get_campaigns()})


@bp.route("/api/campaigns/<cid>/call", methods=["POST"])
@auth_required
def api_campaign_call(cid):
    """Execute next call in campaign."""
    if not CAMPAIGNS_AVAILABLE or not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice/campaigns not available"})
    data = request.get_json(silent=True) or {}
    target_index = data.get("target_index")
    result = execute_campaign_call(cid, target_index=target_index)
    return jsonify(result)


@bp.route("/api/campaigns/<cid>/outcome", methods=["POST"])
@auth_required
def api_campaign_outcome(cid):
    """Log call outcome for a campaign contact."""
    if not CAMPAIGNS_AVAILABLE:
        return jsonify({"ok": False, "error": "Campaigns not available"})
    data = request.get_json(silent=True) or {}
    result = update_call_outcome(cid, phone=data.get("phone", ""), outcome=data.get("outcome", ""))
    return jsonify(result)


@bp.route("/api/campaigns/<cid>")
@auth_required
def api_campaign_detail(cid):
    """Get campaign details."""
    if not CAMPAIGNS_AVAILABLE:
        return jsonify({"ok": False, "error": "Campaigns not available"})
    camp = get_campaign(cid)
    if not camp:
        return jsonify({"ok": False, "error": "Not found"})
    return jsonify({"ok": True, **camp})


@bp.route("/api/campaigns/stats")
@auth_required
def api_campaign_stats():
    """Aggregate campaign analytics."""
    if not CAMPAIGNS_AVAILABLE:
        return jsonify({"ok": False, "error": "Campaigns not available"})
    return jsonify({"ok": True, **get_campaign_stats()})


@bp.route("/api/test/cleanup-duplicates")
@auth_required
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
        except (ValueError, IndexError):
            pass

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
        if os.path.exists(lpath):
            with open(lpath) as f:
                leads = json.load(f)
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
            except Exception:
                pass

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
    except Exception:
        pass

    # 6. Clear orders
    try:
        opath = os.path.join(DATA_DIR, "orders.json")
        if os.path.exists(opath):
            with open(opath) as f:
                orders = json.load(f)
            if isinstance(orders, dict) and len(orders) > 0:
                report["actions"].append(f"Clear {len(orders)} orders")
                if not dry_run:
                    with open(opath, "w") as f:
                        json.dump({}, f, indent=2)
    except Exception:
        pass

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


# Start polling on import (for gunicorn) and on direct run
start_polling()
