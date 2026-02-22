# routes_voice_contacts.py — Intelligence page, Voice APIs, Contacts, Campaigns
# Extracted from routes_intel.py for maintainability

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
    """ + _page_footer()


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
    Auth via shared secret header (VAPI_WEBHOOK_SECRET) or falls back to open."""
    # Verify webhook secret if configured
    webhook_secret = os.environ.get("VAPI_WEBHOOK_SECRET", "")
    if webhook_secret:
        auth_header = request.headers.get("X-Vapi-Secret", "")
        if auth_header != webhook_secret:
            return jsonify({"error": "Unauthorized"}), 401

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
    """ + _page_footer()


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


