# routes_catalog_finance.py — Catalog, Shipping, Pricing, Margins, Payments, Audit
# Extracted from routes_intel.py for maintainability

# ═══════════════════════════════════════════════════════════════════════
# Product Catalog & Dynamic Pricing
# ═══════════════════════════════════════════════════════════════════════

# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify, redirect, flash
from markupsafe import escape as esc
from src.api.shared import bp, auth_required
import logging
import os
import re
from src.core.security import rate_limit
log = logging.getLogger("reytech")
from src.core.paths import DATA_DIR
from src.api.render import render_page

try:
    from src.agents.product_catalog import (
        import_qb_csv, search_products, get_product, predictive_lookup,
        get_catalog_stats, calculate_recommended_price, update_product_pricing,
        record_won_price, bulk_margin_analysis, init_catalog_db,
        match_item, match_items_batch, add_supplier_price, get_product_suppliers,
        rebuild_search_tokens,
        audit_catalog_matches, audit_catalog_db,
        ai_find_product, ai_find_products_batch,
        # Sprint 1 additions
        reimport_qb_csv, run_sprint1_fixes, fix_catalog_names,
        extract_manufacturers_bulk, bulk_calculate_recommended,
        get_freshness_report, dedup_catalog,
        # QuoteWerks import
        import_quotewerks_csv,
        import_qw_documents_report,
    )
    CATALOG_AVAILABLE = True
except ImportError:
    CATALOG_AVAILABLE = False


@bp.route("/catalog")
@auth_required
@safe_page
def catalog_page():
    """Product catalog with search, pricing intelligence, margin analysis."""
    tab = request.args.get("tab", "products")

    # ── Vendors tab ──────────────────────────────────────────────────────
    if tab == "vendors":
        from src.agents.vendor_ordering_agent import get_enriched_vendor_list, get_agent_status as _voas, get_vendor_orders
        vendors = get_enriched_vendor_list()
        vs = _voas()
        recent_orders = get_vendor_orders(limit=20)

        active = [v for v in vendors if v.get("can_order")]
        email_po = [v for v in vendors if v.get("integration_status") == "email_po"]
        setup_needed = [v for v in vendors if v.get("integration_status") == "setup_needed"]

        STATUS_BADGE = {
            "active": ("<span style='color:var(--gn);font-size:14px;font-weight:600'>● ACTIVE</span>", "var(--gn)"),
            "email_po": ("<span style='color:var(--ac);font-size:14px;font-weight:600'>✉ EMAIL PO</span>", "var(--ac)"),
            "setup_needed": ("<span style='color:var(--yl);font-size:14px;font-weight:600'>⚙ SETUP</span>", "var(--yl)"),
            "ready": ("<span style='color:var(--or);font-size:14px;font-weight:600'>◑ PARTIAL</span>", "var(--or)"),
            "manual_only": ("<span style='color:var(--tx2);font-size:14px'>— MANUAL</span>", "var(--tx2)"),
        }

        def vendor_row(v):
            name = esc(v.get("name",""))
            status = v.get("integration_status","manual_only")
            badge_html, color = STATUS_BADGE.get(status, STATUS_BADGE["manual_only"])
            email = esc(v.get("email","") or v.get("contact_email",""))
            phone = esc(v.get("phone",""))
            balance = v.get("open_balance","")
            cats = esc(", ".join(v.get("categories",[])[:3]) or "—")
            note = esc(v.get("note","") or v.get("action",""))
            oscore = v.get("overall_score", 0) or 0
            score_color = "var(--gn)" if oscore >= 70 else "var(--yl)" if oscore >= 40 else "var(--rd)" if oscore > 0 else "var(--tx2)"
            score_html = f'<div style="display:flex;align-items:center;gap:5px"><div style="background:var(--sf2);border-radius:3px;height:6px;width:40px;overflow:hidden"><div style="width:{oscore}%;height:100%;background:{score_color};border-radius:3px"></div></div><span style="font-size:14px;font-family:monospace">{oscore:.0f}</span></div>' if oscore > 0 else '<span style="font-size:13px;color:var(--tx2)">—</span>'
            return f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:10px 12px;font-weight:500;color:{color}">{name}</td>
  <td style="padding:10px 12px;font-size:14px">{badge_html}</td>
  <td style="padding:10px 12px;font-size:14px">{score_html}</td>
  <td style="padding:10px 12px;font-size:14px;color:var(--tx2)">{cats}</td>
  <td style="padding:10px 12px;font-size:14px;color:var(--ac)">{email}</td>
  <td style="padding:10px 12px;font-size:14px;color:var(--tx2)">{phone}</td>
  <td style="padding:10px 12px;font-size:14px;color:var(--yl)">{f"${float(balance):,.2f}" if balance else ""}</td>
  <td style="padding:10px 12px;font-size:14px;color:var(--tx2);max-width:200px">{note[:80] if note else ""}</td>
</tr>"""

        priority_vendors = [v for v in vendors if v.get("integration_status") in ("active","email_po","setup_needed","ready")]
        other_vendors = [v for v in vendors if v.get("integration_status") == "manual_only"]
        all_rows = "".join(vendor_row(v) for v in priority_vendors + other_vendors)

        orders_html = ""
        if recent_orders:
            for o in recent_orders[:10]:
                ts = (o.get("submitted_at","")[:16] or "").replace("T"," ")
                status_color = {"submitted":"var(--ac)","confirmed":"var(--gn)","shipped":"var(--yl)","failed":"var(--rd)"}.get(o.get("status",""),("var(--tx2)"))
                orders_html += f"""<tr>
  <td style="padding:8px 12px;font-size:14px">{ts}</td>
  <td style="padding:8px 12px;font-size:14px;font-weight:500">{esc(o.get("vendor_name",""))}</td>
  <td style="padding:8px 12px;font-size:14px;font-family:'JetBrains Mono',monospace">{esc(o.get("po_number",""))}</td>
  <td style="padding:8px 12px;font-size:14px">{esc(o.get("quote_number",""))}</td>
  <td style="padding:8px 12px;font-size:14px">${o.get("total",0):,.2f}</td>
  <td style="padding:8px 12px;font-size:14px;color:{status_color}">{o.get("status","").upper()}</td>
</tr>"""
        else:
            orders_html = '<tr><td colspan="6" style="padding:20px;text-align:center;color:var(--tx2)">No vendor orders yet — orders appear here when quotes are won</td></tr>'

        return render_page("catalog.html", active_page="Catalog",
            tab="vendors",
            all_rows=all_rows,
            orders_html=orders_html,
            vs=vs,
            vendors=vendors,
            active=active,
            setup_needed=setup_needed,
            api_ready_count=len(active)+len(email_po))

    # ── Products tab (default) ───────────────────────────────────────────
    if not CATALOG_AVAILABLE:
        return render_page("catalog.html", active_page="Catalog", tab="products",
            content="<div class='card'><p>Product catalog module not available.</p></div>")

    init_catalog_db()
    stats = get_catalog_stats()
    q = request.args.get("q", "")
    cat_filter = request.args.get("category", "")
    margin_filter = request.args.get("margin", "")
    
    products = []
    if q or cat_filter or margin_filter:
        # Build search with margin filter
        min_m = None
        max_m = None
        if margin_filter == "negative":
            max_m = 0
        elif margin_filter == "low":
            min_m = 0
            max_m = 10
        elif margin_filter == "mid":
            min_m = 10
            max_m = 25
        elif margin_filter == "high":
            min_m = 25
        products = search_products(q, limit=100, category=cat_filter, min_margin=min_m, max_margin=max_m)
    else:
        # Default: show all products sorted by times_quoted desc
        try:
            products = search_products("", limit=100)
        except Exception:
            products = []

    # Enrich products with primary supplier URL + last_checked
    if products:
        try:
            from src.agents.product_catalog import _get_conn as _cat_conn
            _conn = _cat_conn()
            pids = [p["id"] for p in products]
            placeholders = ",".join("?" * len(pids))
            url_rows = _conn.execute(f"""
                SELECT product_id, supplier_url, last_checked, supplier_name
                FROM product_suppliers
                WHERE product_id IN ({placeholders}) AND supplier_url IS NOT NULL AND supplier_url != ''
                ORDER BY last_checked DESC
            """, pids).fetchall()
            _conn.close()
            url_map = {}
            for r in url_rows:
                pid = r["product_id"]
                if pid not in url_map:
                    url_map[pid] = {"url": r["supplier_url"], "last_checked": r["last_checked"], "supplier": r["supplier_name"]}
            for p in products:
                info = url_map.get(p["id"], {})
                p["primary_url"] = info.get("url", "")
                p["last_price_checked"] = info.get("last_checked", "")
        except Exception as _e:
            log.debug('suppressed in vendor_row: %s', _e)

    # Macro stats bento
    tp = stats["total_products"]
    am = stats["avg_margin"]
    neg = stats["negative_margin"]
    low = stats["low_margin"]
    mid = stats["mid_margin"]
    high = stats["high_margin"]

    # Margin bar
    total_with_cost = neg + low + mid + high
    pct_neg = round(neg / total_with_cost * 100) if total_with_cost else 0
    pct_low = round(low / total_with_cost * 100) if total_with_cost else 0
    pct_mid = round(mid / total_with_cost * 100) if total_with_cost else 0
    pct_high = round(high / total_with_cost * 100) if total_with_cost else 0

    # Category options
    cat_options = "".join(
        f'<option value="{c["category"]}" {"selected" if c["category"]==cat_filter else ""}>{c["category"]} ({c["cnt"]})</option>'
        for c in stats.get("categories", [])
    )

    # Product rows
    rows = ""
    for p in products:
        margin = p.get("margin_pct", 0)
        mc = "#f85149" if margin < 0 else "#d29922" if margin < 10 else "#3fb950" if margin < 25 else "#58a6ff"
        strat = p.get("price_strategy", "")
        strat_badge = {"loss_leader": "🔴", "margin_protect": "🟡", "competitive": "🟢", "premium": "🔵"}.get(strat, "")
        desc_short = (p.get("description", "") or "")[:60].replace("\n", " ")
        p_url = p.get("primary_url", "")
        url_icon = f'<a href="{p_url}" target="_blank" style="color:var(--ac)" onclick="event.stopPropagation()">🔗</a>' if p_url else '<span style="color:var(--tx2)">—</span>'
        checked_date = (p.get("last_price_checked", "") or "")[:10]
        rows += f"""<tr onclick="location.href='/catalog/{p['id']}'" style="cursor:pointer">
         <td class="mono" style="font-weight:600;color:var(--ac)">{p.get('name','')[:25]}</td>
         <td style="font-size:14px;color:var(--tx2)">{desc_short}</td>
         <td class="mono">{p.get('sku','')}</td>
         <td style="font-size:14px">{p.get('category','')}</td>
         <td class="mono" style="text-align:right">${p.get('sell_price',0):,.2f}</td>
         <td class="mono" style="text-align:right">${p.get('cost',0):,.2f}</td>
         <td class="mono" style="text-align:right;color:{mc};font-weight:700">{margin:.1f}%</td>
         <td style="text-align:center">{strat_badge}</td>
         <td style="text-align:center">{url_icon}</td>
         <td class="mono" style="font-size:13px;color:var(--tx2)">{checked_date}</td>
        </tr>"""

    # Negative margin alerts
    neg_alerts = ""
    for ni in stats.get("negative_margin_items", [])[:5]:
        neg_alerts += f"""<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid var(--bd)">
         <span style="font-weight:600">{ni['name'][:30]}</span>
         <span style="color:#f85149;font-weight:700;font-family:'JetBrains Mono',monospace">{ni['margin_pct']:.1f}% (sell ${ni['sell_price']:.2f} / cost ${ni['cost']:.2f})</span>
        </div>"""

    # Top opportunities
    opp_rows = ""
    for o in stats.get("margin_opportunities", [])[:8]:
        opp_rows += f"""<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid var(--bd)">
         <span style="font-size:14px">{o['name'][:35]}</span>
         <span class="mono" style="font-size:14px">${o['sell_price']:,.2f} @ {o['margin_pct']:.1f}%</span>
        </div>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
     <h2 style="margin:0;font-size:20px;font-weight:700">📦 Product Catalog</h2>
     <div style="display:flex;gap:8px;align-items:center">
      <span class="mono" style="font-size:14px;color:var(--tx2)">{tp} products</span>
      <button onclick="document.getElementById('import-csv').click()" class="btn btn-s" style="font-size:14px">📥 Import QB CSV</button>
      <input type="file" id="import-csv" accept=".csv" style="display:none" onchange="importCSV(this)">
      <button onclick="document.getElementById('import-qw').click()" class="btn btn-s" style="font-size:14px;background:#21262d;color:#58a6ff;border:1px solid #58a6ff44">📋 Import QuoteWerks</button>
      <input type="file" id="import-qw" accept=".csv,.tsv,.txt" style="display:none" onchange="importQW(this)">
      <button onclick="runCatalogFixes(this)" class="btn btn-s" style="font-size:14px;background:#21262d;color:#d2a8ff;border:1px solid #d2a8ff44">🔧 Run Fixes</button>
      <button onclick="bulkCheckPrices(this)" class="btn btn-s" style="font-size:14px;background:#21262d;color:#3fb950;border:1px solid #3fb95044">🔄 Check All Prices</button>
      <a href="/catalog/price-alerts" class="btn btn-s" style="font-size:14px;background:#21262d;color:#d29922;border:1px solid #d2992244;text-decoration:none">💰 Price Alerts</a>
     </div>
    </div>

    <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:16px">
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--ac)">{tp}</div>
      <div style="font-size:14px;color:var(--tx2)">Products</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:{'#f85149' if am < 10 else '#d29922' if am < 15 else '#3fb950'}">{am}%</div>
      <div style="font-size:14px;color:var(--tx2)">Avg Margin</div>
     </div>
     <div class="card" style="text-align:center">
      <a href="/catalog?margin=negative" style="text-decoration:none"><div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#f85149">{neg + low}</div></a>
      <div style="font-size:14px;color:var(--tx2)">Need Pricing Review</div>
      <div style="font-size:13px"><a href="/catalog?margin=negative" style="color:#f85149">{neg} losing money</a></div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#3fb950">${stats['total_sell_value']:,.0f}</div>
      <div style="font-size:14px;color:var(--tx2)">Catalog Value</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#58a6ff">{stats.get('products_with_urls', 0)}</div>
      <div style="font-size:14px;color:var(--tx2)">With URLs</div>
      <div style="font-size:13px;color:#d29922">{stats.get('stale_price_checks', 0)} need check</div>
     </div>
    </div>

    <!-- Margin distribution bar -->
    <div class="card" style="margin-bottom:16px;padding:12px 16px">
     <div style="font-size:14px;font-weight:600;margin-bottom:8px">Margin Distribution</div>
     <div style="display:flex;gap:16px;align-items:center;font-size:14px;margin-bottom:6px">
      <span><span style="color:#f85149">●</span> {neg} negative</span>
      <span><span style="color:#d29922">●</span> {low} low (&lt;10%)</span>
      <span><span style="color:#3fb950">●</span> {mid} mid (10-25%)</span>
      <span><span style="color:#58a6ff">●</span> {high} high (&gt;25%)</span>
     </div>
     <div style="background:var(--sf);border-radius:8px;height:16px;overflow:hidden;display:flex">
      <div style="width:{pct_neg}%;background:#f85149" title="{neg} negative margin"></div>
      <div style="width:{pct_low}%;background:#d29922" title="{low} low margin"></div>
      <div style="width:{pct_mid}%;background:#3fb950" title="{mid} mid margin"></div>
      <div style="width:{pct_high}%;background:#58a6ff" title="{high} high margin"></div>
     </div>
    </div>

    <div class="bento bento-2" style="margin-bottom:16px">
     <div class="card" style="padding:12px">
      <a href="/catalog?margin=negative" style="text-decoration:none;font-weight:600;font-size:13px;margin-bottom:8px;color:#f85149;display:block">⚠️ Losing Money ({neg} items)</a>
      {neg_alerts if neg_alerts else '<div style="font-size:14px;color:var(--tx2)">No negative margin items ✅</div>'}
     </div>
     <div class="card" style="padding:12px">
      <div style="font-weight:600;font-size:13px;margin-bottom:8px;color:#d29922">💡 Margin Opportunities</div>
      {opp_rows if opp_rows else '<div style="font-size:14px;color:var(--tx2)">Connect SCPRS pricing to find opportunities</div>'}
     </div>
    </div>

    <!-- Search -->
    <div class="card" style="padding:12px;margin-bottom:12px">
     <form method="GET" action="/catalog" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
      <input type="text" name="q" value="{q}" placeholder="Search products, SKU, description..." 
             style="flex:1;min-width:200px;padding:6px 10px;border:1px solid var(--bd);border-radius:6px;background:var(--sf);color:var(--tx);font-size:13px"
             id="catalog-search" autocomplete="off">
      <select name="category" style="padding:6px;border:1px solid var(--bd);border-radius:6px;background:var(--sf);color:var(--tx);font-size:14px">
       <option value="">All Categories</option>
       {cat_options}
      </select>
      <select name="margin" style="padding:6px;border:1px solid var(--bd);border-radius:6px;background:var(--sf);color:var(--tx);font-size:14px">
       <option value="">All Margins</option>
       <option value="negative" {"selected" if margin_filter=="negative" else ""}>🔴 Negative (&lt;0%)</option>
       <option value="low" {"selected" if margin_filter=="low" else ""}>🟡 Low (0-10%)</option>
       <option value="mid" {"selected" if margin_filter=="mid" else ""}>🟢 Mid (10-25%)</option>
       <option value="high" {"selected" if margin_filter=="high" else ""}>🔵 High (&gt;25%)</option>
      </select>
      <button type="submit" class="btn btn-s" style="font-size:14px">🔍 Search</button>
      {'<a href="/catalog" class="btn" style="font-size:14px">Clear</a>' if (q or cat_filter or margin_filter) else ''}
     </form>
    </div>

    <!-- Predictive search dropdown -->
    <div id="search-results-dropdown" style="display:none;position:absolute;z-index:100;background:var(--bg2);border:1px solid var(--bd);border-radius:8px;max-height:300px;overflow-y:auto;width:400px;box-shadow:0 4px 12px rgba(0,0,0,0.3)"></div>

    {f'''<div class="card" style="padding:0;overflow-x:auto">
     <div style="padding:8px 12px;font-size:14px;color:var(--tx2);border-bottom:1px solid var(--bd)">Showing {len(products)} product{"s" if len(products)!=1 else ""}{f" matching '{q}'" if q else ""}{f" in {cat_filter}" if cat_filter else ""}{f" — margin: {margin_filter}" if margin_filter else ""}</div>
     <table class="home-tbl" style="min-width:700px">
      <thead><tr>
       <th style="width:150px">Name</th><th>Description</th><th style="width:80px">SKU</th>
       <th style="width:100px">Category</th>
       <th style="width:80px;text-align:right">Price</th><th style="width:80px;text-align:right">Cost</th>
       <th style="width:70px;text-align:right">Margin</th><th style="width:30px"></th>
       <th style="width:30px">URL</th><th style="width:80px">Checked</th>
      </tr></thead>
      <tbody>{rows}</tbody>
     </table>
    </div>''' if products else '<div class="card" style="padding:24px;text-align:center;color:var(--tx2)">No products found{" matching your search" if q else ". Import a QB CSV or rebuild from history on the <a href=\\"/growth-intel\\" style=\\"color:var(--ac)\\">Growth Intel</a> page"}.</div>'}

    <script>
    function importCSV(input) {{
      const file = input.files[0]; if (!file) return;
      const fd = new FormData(); fd.append('file', file);
      const btn = input.previousElementSibling;
      if(btn) {{ btn.textContent='⏳ Importing...'; btn.disabled=true; }}
      fetch('/api/catalog/reimport', {{method:'POST', body:fd}})
        .then(r=>r.json()).then(d=>{{
          if(btn) {{ btn.textContent='📥 Import QB CSV'; btn.disabled=false; }}
          if(d.ok) {{ alert('✅ Import complete!\\nImported: '+d.imported+'\\nUpdated: '+d.updated+'\\nNames fixed: '+(d.names_fixed||0)+'\\nBrands found: '+(d.brands_found||0)+'\\nPrices calculated: '+(d.prices_calculated||0)+'\\nDupes merged: '+(d.dupes_merged||0)+' (deleted '+(d.dupes_deleted||0)+')'); location.reload(); }}
          else alert('Error: '+(d.error||'unknown'));
        }}).catch(e=>{{
          if(btn) {{ btn.textContent='📥 Import QB CSV'; btn.disabled=false; }}
          alert('Import failed: '+e.message);
        }});
    }}
    function runCatalogFixes(btn) {{
      btn.disabled=true; btn.textContent='⏳ Running fixes...';
      fetch('/api/catalog/run-fixes', {{method:'POST'}})
        .then(r=>r.json()).then(d=>{{
          btn.disabled=false; btn.textContent='🔧 Run Fixes';
          if(d.ok) {{ alert('✅ Fixes applied!\\nNames: '+d.names_fixed+'\\nPart#s: '+d.mfg_numbers_set+'\\nBrands: '+d.brands_found+'\\nPrices: '+d.prices_calculated+'\\nDupes merged: '+(d.dupes_merged||0)+'\\nDupes deleted: '+(d.dupes_deleted||0)+'\\nProducts remaining: '+(d.products_remaining||'?')); location.reload(); }}
          else alert('Error: '+(d.error||'unknown'));
        }}).catch(e=>{{
          btn.disabled=false; btn.textContent='🔧 Run Fixes';
          alert('Fix failed: '+e.message);
        }});
    }}
    function importQW(input) {{
      const file = input.files[0]; if (!file) return;
      const fd = new FormData(); fd.append('file', file);
      const btn = input.previousElementSibling;
      if(btn) {{ btn.textContent='⏳ Importing...'; btn.disabled=true; }}
      fetch('/api/catalog/import-quotewerks', {{method:'POST', body:fd}})
        .then(r=>r.json()).then(d=>{{
          if(btn) {{ btn.textContent='📋 Import QuoteWerks'; btn.disabled=false; }}
          if(d.ok) {{
            let msg = '✅ QuoteWerks Import Complete!\\n\\n';
            msg += 'Rows processed: '+d.total_rows+'\\n';
            msg += 'New products: '+d.imported+'\\n';
            msg += 'Updated existing: '+d.updated+'\\n';
            msg += 'Skipped: '+d.skipped+'\\n';
            if(d.urls_stored) msg += 'Supplier URLs stored: '+d.urls_stored+'\\n';
            if(d.dupes_merged) msg += 'Dupes merged: '+d.dupes_merged+'\\n';
            if(d.qa_flags && d.qa_flags.length) msg += 'QA flags: '+d.qa_flags.length+'\\n';
            if(d.dedup_stats) msg += 'Dedup: '+JSON.stringify(d.dedup_stats)+'\\n';
            if(d.errors && d.errors.length) msg += '\\nErrors: '+d.errors.length;
            const cols = d.columns_found || {{}};
            msg += '\\n\\nColumns matched:\\n';
            for(const [k,v] of Object.entries(cols)) {{ if(v) msg += '  '+k+' → '+v+'\\n'; }}
            alert(msg); location.reload();
          }} else alert('Error: '+(d.error||'unknown'));
        }}).catch(e=>{{
          if(btn) {{ btn.textContent='📋 Import QuoteWerks'; btn.disabled=false; }}
          alert('Import failed: '+e.message);
        }});
    }}
    function bulkCheckPrices(btn) {{
      btn.disabled=true; btn.textContent='⏳ Starting...';
      fetch('/api/catalog/bulk-check-prices', {{method:'POST'}})
        .then(r=>r.json()).then(d=>{{
          if(!d.ok) {{ btn.disabled=false; btn.textContent='🔄 Check All Prices'; alert('Error: '+(d.error||'unknown')); return; }}
          btn.textContent='⏳ Checking '+d.total+' URLs...';
          var poll = setInterval(function() {{
            fetch('/api/catalog/bulk-check-status').then(r=>r.json()).then(s=>{{
              btn.textContent='⏳ '+s.checked+'/'+s.total+' checked ('+s.price_changes+' changes)';
              if(!s.running) {{
                clearInterval(poll);
                btn.disabled=false; btn.textContent='🔄 Check All Prices';
                alert('✅ Done! Checked '+s.checked+' URLs.\\n'+s.price_changes+' price changes found.\\n'+s.errors+' errors.');
                location.reload();
              }}
            }});
          }}, 3000);
        }}).catch(function(e) {{
          btn.disabled=false; btn.textContent='🔄 Check All Prices';
          alert('Error: '+e.message);
        }});
    }}
    // Predictive search
    let searchTimeout;
    const searchInput = document.getElementById('catalog-search');
    const dropdown = document.getElementById('search-results-dropdown');
    if (searchInput) {{
      searchInput.addEventListener('input', function() {{
        clearTimeout(searchTimeout);
        const q = this.value.trim();
        if (q.length < 2) {{ dropdown.style.display='none'; return; }}
        searchTimeout = setTimeout(()=>{{
          fetch('/api/catalog/lookup?q='+encodeURIComponent(q))
            .then(r=>r.json()).then(items=>{{
              if (!items.length) {{ dropdown.style.display='none'; return; }}
              const rect = searchInput.getBoundingClientRect();
              dropdown.style.left = rect.left+'px';
              dropdown.style.top = (rect.bottom+2)+'px';
              dropdown.style.width = Math.max(rect.width, 400)+'px';
              dropdown.innerHTML = items.map(p=>
                `<a href="/catalog/${{p.id}}" style="display:flex;justify-content:space-between;padding:8px 12px;text-decoration:none;color:var(--tx);border-bottom:1px solid var(--bd);font-size:14px">
                  <span style="font-weight:600">${{p.name.substring(0,30)}}</span>
                  <span style="color:var(--tx2)">${{p.category}} · $${{(p.sell_price||0).toFixed(2)}} · ${{(p.margin_pct||0).toFixed(1)}}%</span>
                </a>`
              ).join('');
              dropdown.style.display='block';
            }});
        }}, 200);
      }});
      document.addEventListener('click', e=>{{ if(!dropdown.contains(e.target)&&e.target!==searchInput) dropdown.style.display='none'; }});
    }}
    </script>
    """
    return render_page("catalog.html", active_page="Catalog", tab="products", content=content)


@bp.route("/catalog/<int:pid>")
@auth_required
@safe_page
def catalog_product_detail(pid):
    """Product detail with pricing intelligence."""
    if not CATALOG_AVAILABLE:
        return redirect("/catalog")

    product = get_product(pid)
    if not product:
        flash("Product not found", "error")
        return redirect("/catalog")

    p = product
    margin_color = "#f85149" if p["margin_pct"] < 0 else "#d29922" if p["margin_pct"] < 10 else "#3fb950"
    strat_map = {"loss_leader": "🔴 Loss Leader", "margin_protect": "🟡 Margin Protect", "competitive": "🟢 Competitive", "premium": "🔵 Premium"}

    # ── Feature 2: Price Trend Sparkline ──
    sparkline_svg = ""
    price_history = p.get("price_history", [])
    sell_prices = [h.get("price", 0) for h in reversed(price_history) if h.get("price_type") in ("sell", "quoted", "web_check") and h.get("price", 0) > 0]
    if len(sell_prices) >= 2:
        # Build SVG sparkline (120x30)
        min_p = min(sell_prices)
        max_p = max(sell_prices)
        rng = max_p - min_p if max_p != min_p else 1
        w, h_svg = 120, 30
        step = w / max(len(sell_prices) - 1, 1)
        points = []
        for i, price in enumerate(sell_prices):
            x = round(i * step, 1)
            y = round(h_svg - ((price - min_p) / rng) * (h_svg - 4) - 2, 1)
            points.append(f"{x},{y}")
        polyline = " ".join(points)
        trend_color = "#3fb950" if sell_prices[-1] <= sell_prices[0] else "#f85149"
        sparkline_svg = f'<svg width="{w}" height="{h_svg}" style="vertical-align:middle"><polyline points="{polyline}" fill="none" stroke="{trend_color}" stroke-width="1.5"/></svg>'

    # ── Feature 8: Reorder link ──
    reorder_btn = ""
    primary_url = ""
    for s in p.get("suppliers", []):
        url = s.get("supplier_url", "") or ""
        if url:
            primary_url = url
            supplier_name = s.get("supplier_name", "Supplier")
            reorder_btn = f'<a href="{url}" target="_blank" class="btn btn-s" style="font-size:14px;background:#238636;color:#fff;text-decoration:none">🛒 Order from {supplier_name}</a>'
            break

    # Price history rows (with qty + institution context)
    ph_rows = ""
    for h in p.get("price_history", [])[:30]:
        qty_str = f"{h.get('quantity',0):g}" if h.get('quantity') else "—"
        inst_str = h.get('institution', '') or h.get('agency', '') or ''
        pc_str = h.get('quote_number', '') or h.get('pc_id', '') or ''
        url_str = h.get('supplier_url', '') or ''
        url_link = f'<a href="{url_str}" target="_blank" style="color:var(--ac);font-size:13px">🔗 link</a>' if url_str else ''
        ph_rows += f"""<tr>
         <td class="mono" style="font-size:14px">{h.get('recorded_at','')[:10]}</td>
         <td style="font-size:14px"><span style="padding:1px 6px;border-radius:3px;font-size:13px;background:{'#238636' if h.get('price_type')=='quoted' else '#1a3a5c' if h.get('price_type')=='cost' else '#6e40c9'}20;color:{'#3fb950' if h.get('price_type')=='quoted' else '#58a6ff' if h.get('price_type')=='cost' else '#bc8cff'}">{h.get('price_type','')}</span></td>
         <td class="mono" style="text-align:right">${h.get('price',0):,.2f}</td>
         <td class="mono" style="text-align:center">{qty_str}</td>
         <td style="font-size:14px">{inst_str}</td>
         <td style="font-size:14px;color:var(--tx2)">{pc_str}</td>
         <td style="font-size:14px;color:var(--tx2)">{h.get('source','')}</td>
         <td>{url_link}</td>
        </tr>"""

    # Supplier rows
    sup_rows = ""
    for s in p.get("suppliers", []):
        url = s.get('supplier_url', '') or ''
        url_display = url[:50] + '...' if len(url) > 50 else url
        url_cell = f'<a href="{url}" target="_blank" style="color:var(--ac);word-break:break-all;font-size:14px">{url_display}</a>' if url else '<span style="color:var(--tx2)">—</span>'
        rel_pct = int((s.get('reliability', 0.5) or 0.5) * 100)
        rel_color = '#3fb950' if rel_pct >= 80 else '#d29922' if rel_pct >= 50 else '#f85149'
        sid = s.get('id', '')
        safe_url = url.replace("'", "\\'").replace('"', '&quot;')
        check_btn = f'''<button onclick="checkSupplierPrice({sid},'{safe_url}',{pid})" class="btn btn-s" style="font-size:12px;padding:2px 8px">Check</button>''' if url else ''
        sup_rows += f"""<tr id="sup-row-{sid}">
         <td style="font-size:14px;font-weight:600">{s.get('supplier_name','')}</td>
         <td class="mono" style="text-align:right" id="sup-price-{sid}">${s.get('last_price',0) or 0:,.2f}</td>
         <td style="font-size:14px">{url_cell}</td>
         <td class="mono" style="font-size:14px" id="sup-checked-{sid}">{(s.get('last_checked','') or '')[:10]}</td>
         <td style="text-align:center"><span style="color:{rel_color}">{rel_pct}%</span></td>
         <td style="text-align:center">{'✅' if s.get('in_stock') else '❌'}</td>
         <td id="sup-delta-{sid}"></td>
         <td>{check_btn}</td>
        </tr>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
     <div>
      <a href="/catalog" style="color:var(--tx2);text-decoration:none;font-size:14px">← Catalog</a>
      <h2 style="margin:4px 0 0;font-size:18px;font-weight:700">{p['name']}</h2>
      <div style="font-size:14px;color:var(--tx2);margin-top:2px">{(p.get('description','') or '')[:200]}</div>
      {f'<div style="margin-top:6px">{sparkline_svg} <span style="font-size:12px;color:var(--tx2)">Price trend ({len(sell_prices)} data points)</span></div>' if sparkline_svg else ''}
     </div>
     <div style="display:flex;flex-direction:column;gap:6px;align-items:flex-end">
      <span style="padding:4px 12px;border-radius:12px;font-size:14px;font-weight:600;background:var(--sf)">{strat_map.get(p.get('price_strategy',''), p.get('price_strategy',''))}</span>
      {reorder_btn}
     </div>
    </div>

    <div class="bento bento-4" style="margin-bottom:16px">
     <div class="card" style="text-align:center">
      <div style="font-size:24px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--ac)">${p['sell_price']:,.2f}</div>
      <div style="font-size:14px;color:var(--tx2)">Sell Price</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:24px;font-weight:800;font-family:'JetBrains Mono',monospace">${p['cost']:,.2f}</div>
      <div style="font-size:14px;color:var(--tx2)">Cost</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:24px;font-weight:800;font-family:'JetBrains Mono',monospace;color:{margin_color}">{p['margin_pct']:.1f}%</div>
      <div style="font-size:14px;color:var(--tx2)">Margin</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:24px;font-weight:800;font-family:'JetBrains Mono',monospace">${p['sell_price'] - p['cost']:,.2f}</div>
      <div style="font-size:14px;color:var(--tx2)">Margin $</div>
     </div>
    </div>

    <div class="bento bento-2" style="margin-bottom:16px">
     <div class="card" style="padding:12px">
      <div class="card-t">Product Details</div>
      <div style="display:grid;grid-template-columns:110px 1fr;gap:4px;font-size:14px">
       <span style="color:var(--tx2)">MFG#</span><span class="mono" style="font-weight:600">{p.get('mfg_number','—') or '—'}</span>
       <span style="color:var(--tx2)">SKU</span><span class="mono">{p.get('sku','—')}</span>
       <span style="color:var(--tx2)">UOM</span><span class="mono" style="font-weight:600">{p.get('uom','EA')}</span>
       <span style="color:var(--tx2)">Category</span><span>{p.get('category','—')}</span>
       <span style="color:var(--tx2)">Manufacturer</span><span>{p.get('manufacturer','—') or '—'}</span>
       <span style="color:var(--tx2)">Item Type</span><span>{p.get('item_type','')}</span>
       <span style="color:var(--tx2)">Taxable</span><span>{'Yes' if p.get('taxable') else 'No'}</span>
       <span style="color:var(--tx2)">Times Quoted</span><span class="mono">{p.get('times_quoted',0)}</span>
       <span style="color:var(--tx2)">Times Won</span><span class="mono">{p.get('times_won',0)}</span>
       <span style="color:var(--tx2)">Last Sold</span><span class="mono">${p.get('last_sold_price',0) or 0:,.2f} ({(p.get('last_sold_date') or '—')[:10]})</span>
       <span style="color:var(--tx2)">Best Cost</span><span class="mono">${p.get('best_cost',0) or 0:,.2f} <span style="font-size:13px">({p.get('best_supplier','') or '—'})</span></span>
       <span style="color:var(--tx2)">Tags</span><span>{p.get('tags','')}</span>
      </div>
     </div>

     <div class="card" style="padding:12px">
      <div class="card-t">💰 Pricing Intelligence</div>
      <div style="display:grid;grid-template-columns:120px 1fr;gap:4px;font-size:14px">
       <span style="color:var(--tx2)">SCPRS Price</span><span class="mono">${p.get('scprs_last_price',0) or 0:,.2f} <span style="font-size:13px;color:var(--tx2)">{p.get('scprs_agency','')}</span></span>
       <span style="color:var(--tx2)">Competitor Low</span><span class="mono">${p.get('competitor_low_price',0) or 0:,.2f} <span style="font-size:13px;color:var(--tx2)">{p.get('competitor_source','')}</span></span>
       <span style="color:var(--tx2)">Web Lowest</span><span class="mono">${p.get('web_lowest_price',0) or 0:,.2f} <span style="font-size:13px;color:var(--tx2)">{p.get('web_lowest_source','')}</span></span>
       <span style="color:var(--tx2)">Recommended</span><span class="mono" style="color:#3fb950;font-weight:700">${p.get('recommended_price',0) or 0:,.2f}</span>
      </div>
      <div style="margin-top:12px;display:flex;gap:6px;flex-wrap:wrap">
       <button onclick="runPricingAnalysis({pid})" class="btn btn-s" style="font-size:14px">🧮 Run Pricing Analysis</button>
       <button onclick="updatePrice({pid})" class="btn btn-s" style="font-size:14px">✏️ Update Pricing</button>
       {'<button onclick="enrichFromUrl()" class="btn btn-s" style="font-size:14px;background:#21262d;color:#58a6ff;border:1px solid #58a6ff44">🔍 Enrich from URL</button>' if primary_url else ''}
      </div>
     </div>
    </div>

    {f'<div class="card" style="margin-bottom:12px;padding:10px 16px;background:#f8514915;border:1px solid #f8514944"><span style="font-weight:700;color:#f85149">⚠️ Competitive Risk:</span> <span style="font-size:14px">A supplier has this at <b>${min(s.get("last_price",0) or 9999999 for s in p.get("suppliers",[]) if s.get("last_price")):.2f}</b> — below your sell price of <b>${p["sell_price"]:.2f}</b></span></div>' if p.get("suppliers") and p["sell_price"] > 0 and any(s.get("last_price") and s["last_price"] < p["sell_price"] for s in p.get("suppliers", [])) else ''}

    {f'''<div class="card" style="margin-bottom:16px;padding:0;overflow-x:auto">
     <div style="padding:10px 12px;font-weight:600;font-size:13px;border-bottom:1px solid var(--bd);display:flex;justify-content:space-between;align-items:center">
      <span>🏪 Suppliers & Source URLs</span>
      <button onclick="checkAllSupplierPrices()" class="btn btn-s" style="font-size:12px;background:#21262d;color:#3fb950;border:1px solid #3fb95044">Check All Prices</button>
     </div>
     <table class="home-tbl"><thead><tr>
      <th>Supplier</th><th style="text-align:right">Price</th><th>URL</th><th>Last Checked</th><th>Reliability</th><th>Stock</th><th>Change</th><th></th>
     </tr></thead><tbody>{sup_rows}</tbody></table>
    </div>''' if sup_rows else ''}

    {f'''<div class="card" style="margin-bottom:16px;padding:0;overflow-x:auto">
     <div style="padding:10px 12px;font-weight:600;font-size:13px;border-bottom:1px solid var(--bd)">📊 Quote & Price History</div>
     <table class="home-tbl"><thead><tr>
      <th>Date</th><th>Type</th><th style="text-align:right">Price</th><th style="text-align:center">Qty</th><th>Institution</th><th>PC#</th><th>Source</th><th>Link</th>
     </tr></thead><tbody>{ph_rows}</tbody></table>
    </div>''' if ph_rows else ''}

    <style>
    @keyframes flashGreen {{ 0%{{background:#3fb95030}} 50%{{background:#3fb95060}} 100%{{background:#3fb95020}} }}
    @keyframes flashRed {{ 0%{{background:#f8514930}} 50%{{background:#f8514960}} 100%{{background:#f8514920}} }}
    .price-flash-green {{ animation: flashGreen 1.5s ease-in-out; border-radius:4px; padding:2px 8px; }}
    .price-flash-red {{ animation: flashRed 1.5s ease-in-out; border-radius:4px; padding:2px 8px; }}
    </style>
    <script>
    function checkSupplierPrice(supId, url, pid) {{
      var btn = event.target;
      btn.disabled = true; btn.textContent = '...';
      var deltaEl = document.getElementById('sup-delta-' + supId);
      fetch('/api/catalog/check-price', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{supplier_id: supId, url: url, product_id: pid}})
      }})
      .then(function(r) {{ return r.json(); }})
      .then(function(d) {{
        btn.disabled = false; btn.textContent = 'Check';
        if (!d.ok) {{ deltaEl.innerHTML = '<span style="color:#f85149;font-size:13px">'+d.error+'</span>'; return; }}
        var oldP = d.old_price, newP = d.new_price;
        var delta = newP - oldP;
        var pctC = oldP > 0 ? ((delta / oldP) * 100).toFixed(1) : '0';
        var priceEl = document.getElementById('sup-price-' + supId);
        if (priceEl) priceEl.textContent = '$' + newP.toFixed(2);
        var checkedEl = document.getElementById('sup-checked-' + supId);
        if (checkedEl) checkedEl.textContent = new Date().toISOString().slice(0,10);
        if (Math.abs(delta) > 0.01) {{
          var color = delta < 0 ? '#3fb950' : '#f85149';
          var cls = delta < 0 ? 'price-flash-green' : 'price-flash-red';
          var sign = delta < 0 ? '' : '+';
          deltaEl.innerHTML = '<span class="'+cls+'" style="color:'+color+';font-weight:700;font-size:13px">'
            + sign + '$' + delta.toFixed(2) + ' (' + sign + pctC + '%)</span>';
        }} else {{
          deltaEl.innerHTML = '<span style="color:var(--tx2);font-size:13px">No change</span>';
        }}
      }})
      .catch(function() {{
        btn.disabled = false; btn.textContent = 'Check';
        deltaEl.innerHTML = '<span style="color:#f85149;font-size:13px">Error</span>';
      }});
    }}
    function checkAllSupplierPrices() {{
      var btns = document.querySelectorAll('[id^="sup-row-"] button');
      var delay = 0;
      btns.forEach(function(btn) {{ if (btn.textContent === 'Check') {{ setTimeout(function() {{ btn.click(); }}, delay); delay += 2000; }} }});
    }}
    var ENRICH_PID = {pid};
    var ENRICH_URL = '{primary_url.replace(chr(39), "")}';
    function enrichFromUrl() {{
      var btn = event.target;
      btn.disabled = true; btn.textContent = '⏳ Enriching...';
      fetch('/api/catalog/'+ENRICH_PID+'/enrich-from-url', {{
        method: 'POST', headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{url: ENRICH_URL}})
      }}).then(r=>r.json()).then(d=>{{
        btn.disabled = false; btn.textContent = '🔍 Enrich from URL';
        if(d.ok) {{
          let msg = '✅ Enriched from URL\\n\\n';
          msg += 'Fields updated: ' + (d.fields_updated||[]).join(', ') + '\\n';
          if(d.scraped) {{
            if(d.scraped.title) msg += 'Title: ' + d.scraped.title + '\\n';
            if(d.scraped.mfg_number) msg += 'MFG#: ' + d.scraped.mfg_number + '\\n';
            if(d.scraped.manufacturer) msg += 'Manufacturer: ' + d.scraped.manufacturer + '\\n';
            if(d.scraped.price) msg += 'Price: $' + d.scraped.price + '\\n';
          }}
          alert(msg);
          location.reload();
        }} else alert('Error: ' + (d.error||'unknown'));
      }}).catch(function() {{
        btn.disabled = false; btn.textContent = '🔍 Enrich from URL';
      }});
    }}
    function runPricingAnalysis(pid) {{
      fetch('/api/catalog/'+pid+'/pricing').then(r=>r.json()).then(d=>{{
        if (d.error) {{ alert(d.error); return; }}
        let msg = 'Current: $'+d.current_price.toFixed(2)+' ('+d.current_margin.toFixed(1)+'% margin)\\n\\nRecommendations:\\n';
        (d.recommendations||[]).forEach(r=>{{
          msg += '\\n'+r.strategy+': $'+r.price.toFixed(2)+' ('+r.margin_pct.toFixed(1)+'%) — '+r.rationale;
        }});
        if(d.best) msg += '\\n\\n✅ Best: $'+d.best.price.toFixed(2)+' ('+d.best.margin_pct.toFixed(1)+'%)';
        alert(msg);
      }});
    }}
    function updatePrice(pid) {{
      const price = prompt('New sell price:');
      if (!price) return;
      const cost = prompt('New cost (leave blank to keep current):');
      const body = {{sell_price: parseFloat(price)}};
      if (cost) body.cost = parseFloat(cost);
      fetch('/api/catalog/'+pid+'/update', {{
        method:'POST', headers:{{'Content-Type':'application/json'}},
        body: JSON.stringify(body)
      }}).then(r=>r.json()).then(d=>{{
        if(d.ok) location.reload();
        else alert('Error: '+(d.error||'unknown'));
      }});
    }}
    </script>"""
    return _wrap_page(content, f"Product: {p['name'][:40]}")


@bp.route("/api/catalog/import", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_catalog_import():
    """Import QB products CSV."""
    try:
        if not CATALOG_AVAILABLE:
            return jsonify({"ok": False, "error": "Catalog not available"})
        f = request.files.get("file")
        if not f:
            return jsonify({"ok": False, "error": "No file"})
        safe = re.sub(r'[^\w.\-]', '_', f.filename or 'import.csv')
        path = os.path.join(DATA_DIR, f"catalog_import_{safe}")
        f.save(path)
        result = import_qb_csv(path)
        return jsonify({"ok": True, **result})
    except Exception as e:
        log.error("api_catalog_import error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/catalog/reimport", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_catalog_reimport():
    """Re-import QB CSV with improved name/manufacturer extraction."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "No file uploaded"})
    safe = re.sub(r'[^\w.\-]', '_', f.filename or 'reimport.csv')
    path = os.path.join(DATA_DIR, f"catalog_reimport_{safe}")
    f.save(path)
    # Also save as the canonical import file for future deploys
    canonical = os.path.join(DATA_DIR, "product_catalog_import.csv")
    import shutil
    shutil.copy2(path, canonical)
    try:
        init_catalog_db()
        result = reimport_qb_csv(path)
        # Run Sprint 1 fixes on reimported data
        fix_result = run_sprint1_fixes()
        result["names_fixed"] = fix_result.get("names_fixed", 0)
        result["brands_found"] = fix_result.get("brands_found", 0)
        result["prices_calculated"] = fix_result.get("prices_calculated", 0)
        # Dedup
        dedup_result = dedup_catalog()
        result["dupes_merged"] = dedup_result.get("groups_merged", 0)
        result["dupes_deleted"] = dedup_result.get("products_deleted", 0)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/dedup", methods=["POST"])
@auth_required
@safe_route
def api_catalog_dedup():
    """Find and merge duplicate products."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    try:
        dry_run = request.args.get("dry_run", "false").lower() in ("true", "1", "yes")
        result = dedup_catalog(dry_run=dry_run)
        return jsonify({"ok": True, **result})
    except Exception as e:
        log.error("Catalog dedup error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/catalog/import-quotewerks", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_catalog_import_quotewerks():
    """Import QuoteWerks exported CSV/TSV into the product catalog.

    Accepts Data Manager exports, Open Export Module files, report CSVs,
    and clipboard tab-delimited pastes. Auto-detects column mapping.
    """
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog module not available"})

    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "No file uploaded. Use Data Manager → Export in QuoteWerks."})

    safe = re.sub(r'[^\w.\-]', '_', f.filename or 'qw_import.csv')
    path = os.path.join(DATA_DIR, f"qw_import_{safe}")
    f.save(path)

    # Also save a copy for reference / re-import
    import shutil
    canonical = os.path.join(DATA_DIR, "quotewerks_import_latest.csv")
    shutil.copy2(path, canonical)

    try:
        init_catalog_db()
        replace = request.form.get("replace", "").lower() in ("true", "1", "yes")

        # Auto-detect Documents Report format (DocumentItems_ / DocumentHeaders_ columns)
        with open(path, 'r', encoding='utf-8-sig') as _f:
            header_line = _f.readline()
        is_documents_report = 'DocumentItems_' in header_line or 'DocumentHeaders_' in header_line

        if is_documents_report:
            result = import_qw_documents_report(path, replace=replace)
        else:
            result = import_quotewerks_csv(path, replace=replace)

        # Run dedup after import
        try:
            dedup_result = dedup_catalog()
            result["dupes_merged"] = dedup_result.get("groups_merged", 0)
            result["dupes_deleted"] = dedup_result.get("products_deleted", 0)
        except Exception as _e:
            log.debug('suppressed in api_catalog_import_quotewerks: %s', _e)

        return jsonify({"ok": True, **result})
    except Exception as e:
        log.exception("QuoteWerks import error")
        return jsonify({"ok": False, "error": str(e)})
    try:
        init_catalog_db()
        dry = request.args.get("dry_run", "").lower() in ("1", "true", "yes")
        result = dedup_catalog(dry_run=dry)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/run-fixes", methods=["POST"])
@auth_required
@safe_route
def api_catalog_run_fixes():
    """Run Sprint 1 foundation fixes: names, manufacturers, pricing, dedup."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    try:
        init_catalog_db()
        result = run_sprint1_fixes()
        # Also dedup
        dedup_result = dedup_catalog()
        result["dupes_merged"] = dedup_result.get("groups_merged", 0)
        result["dupes_deleted"] = dedup_result.get("products_deleted", 0)
        result["products_remaining"] = dedup_result.get("products_remaining", 0)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/freshness-report", methods=["POST"])
@auth_required
@safe_route
def api_catalog_freshness_report():
    """Get freshness indicators for PC items."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    try:
        init_catalog_db()
        items = (request.get_json(force=True, silent=True) or {}).get("items", [])
        report = get_freshness_report(items)
        return jsonify({"ok": True, "items": report})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/lookup")
@auth_required
@safe_route
def api_catalog_lookup():
    """Predictive typeahead search."""
    if not CATALOG_AVAILABLE:
        return jsonify([])
    q = request.args.get("q", "")
    if len(q) < 2:
        return jsonify([])
    results = predictive_lookup(q, limit=10)
    return jsonify(results)


@bp.route("/api/products/search")
@auth_required
@safe_route
def api_products_search():
    """Full search with filters."""
    if not CATALOG_AVAILABLE:
        return jsonify([])
    q = request.args.get("q", "")
    cat = request.args.get("category", "")
    strat = request.args.get("strategy", "")
    try:
        limit = min(max(1, int(request.args.get("limit", 50))), 200)
    except (ValueError, TypeError, OverflowError):
        limit = 50
    results = search_products(q, limit=limit, category=cat, strategy=strat)
    return jsonify(results)


@bp.route("/api/catalog/<int:pid>/pricing")
@auth_required
@safe_route
def api_catalog_pricing(pid):
    """Calculate recommended pricing for a product."""
    if not CATALOG_AVAILABLE:
        return jsonify({"error": "Catalog not available"})
    agency = request.args.get("agency", "")
    result = calculate_recommended_price(pid, target_margin=15.0, agency=agency)
    return jsonify(result)


@bp.route("/api/catalog/<int:pid>/update", methods=["POST"])
@auth_required
@safe_route
def api_catalog_update(pid):
    """Update product pricing/metadata."""
    try:
        if not CATALOG_AVAILABLE:
            return jsonify({"ok": False, "error": "Catalog not available"})
        data = request.get_json() or {}
        ok = update_product_pricing(pid, **data)
        return jsonify({"ok": ok})
    except Exception as e:
        log.error("api_catalog_update error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/catalog/opportunities")
@auth_required
@safe_route
def api_catalog_opportunities():
    """Bulk margin analysis — find pricing opportunities."""
    if not CATALOG_AVAILABLE:
        return jsonify([])
    results = bulk_margin_analysis()
    return jsonify(results[:50])


@bp.route("/api/catalog/match", methods=["POST"])
@auth_required
@safe_route
def api_catalog_match():
    """
    POST {description: "...", part_number: "..."}
    Returns best catalog matches for a line item.
    """
    try:
        if not CATALOG_AVAILABLE:
            return jsonify({"ok": False, "error": "Catalog not available"})
        data = request.get_json(silent=True) or {}
        desc = (data.get("description") or "").strip()
        part = (data.get("part_number") or "").strip()
        if not desc and not part:
            return jsonify({"ok": True, "matches": []})
        matches = match_item(desc, part, top_n=3)
        clean = []
        for m in matches:
            clean.append({
                "id": m["id"], "name": m.get("name", ""),
                "description": (m.get("description") or "")[:120],
                "category": m.get("category", ""), "uom": m.get("uom", "EA"),
                "sell_price": m.get("sell_price"), "cost": m.get("cost"),
                "margin_pct": m.get("margin_pct", 0),
                "best_cost": m.get("best_cost"), "best_supplier": m.get("best_supplier", ""),
                "mfg_number": m.get("mfg_number", ""),
                "sku": m.get("sku", ""),
                "part_number": m.get("mfg_number") or m.get("sku") or m.get("name", ""),
                "manufacturer": m.get("manufacturer", ""),
                "recommended_price": m.get("recommended_price"),
                "win_rate": m.get("win_rate", 0),
                "confidence": m.get("match_confidence", 0),
                "reason": m.get("match_reason", ""),
                "times_quoted": m.get("times_quoted", 0),
                "times_won": m.get("times_won", 0),
            })
        return jsonify({"ok": True, "matches": clean})
    except Exception as e:
        log.error("api_catalog_match error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/catalog/match-batch", methods=["POST"])
@auth_required
@safe_route
def api_catalog_match_batch():
    """
    POST {items: [{idx, description, part_number}, ...]}
    Match multiple PC line items at once against the catalog.
    Called by PC detail page on load for auto-fill.
    """
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    try:
        data = request.get_json(silent=True) or {}
        items = data.get("items", [])
        results = match_items_batch(items)
        matched_count = sum(1 for r in results if r.get("matched"))
        return jsonify({
            "ok": True, "results": results,
            "matched": matched_count, "total": len(results),
        })
    except Exception as e:
        log.error("Catalog match-batch error: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/<int:pid>/suppliers")
@auth_required
@safe_route
def api_catalog_product_suppliers(pid):
    """GET all suppliers and prices for a product."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    suppliers = get_product_suppliers(pid)
    return jsonify({"ok": True, "suppliers": suppliers})


@bp.route("/api/catalog/<int:pid>/add-supplier", methods=["POST"])
@auth_required
@safe_route
def api_catalog_add_supplier(pid):
    """POST {supplier_name, price, url, sku, shipping, in_stock}"""
    try:
        if not CATALOG_AVAILABLE:
            return jsonify({"ok": False, "error": "Catalog not available"})
        data = request.get_json(silent=True) or {}
        supplier = (data.get("supplier_name") or "").strip()
        price = float(data.get("price") or 0)
        if not supplier or price <= 0:
            return jsonify({"ok": False, "error": "supplier_name and price required"})
        add_supplier_price(
            pid, supplier, price,
            url=data.get("url", ""), sku=data.get("sku", ""),
            shipping=float(data.get("shipping") or 0),
            in_stock=data.get("in_stock", True),
        )
        return jsonify({"ok": True, "msg": f"Supplier {supplier} price ${price:.2f} recorded"})
    except Exception as e:
        log.error("api_catalog_add_supplier error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/catalog/check-price", methods=["POST"])
@auth_required
@safe_route
def api_catalog_check_price():
    """Scrape a supplier URL for current pricing, update catalog, return delta."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    product_id = data.get("product_id")
    supplier_id = data.get("supplier_id")

    if not url:
        return jsonify({"ok": False, "error": "No URL provided"})

    try:
        from src.agents.item_link_lookup import lookup_from_url
    except ImportError:
        return jsonify({"ok": False, "error": "Link lookup module not available"})

    result = lookup_from_url(url)

    if not result.get("price"):
        error_msg = result.get("error", "Could not scrape price from URL")
        if result.get("login_required"):
            error_msg = f"{result.get('supplier', 'Supplier')} requires login"
        return jsonify({"ok": False, "error": error_msg})

    new_price = float(result["price"])
    old_price = 0

    # Get old price from product_suppliers
    if supplier_id:
        try:
            from src.agents.product_catalog import _get_conn
            conn = _get_conn()
            row = conn.execute("SELECT last_price FROM product_suppliers WHERE id=?", (supplier_id,)).fetchone()
            if row:
                old_price = row["last_price"] or 0
            conn.close()
        except Exception as _e:
            log.debug('suppressed in api_catalog_check_price: %s', _e)

    # Update supplier record + record price history
    if product_id:
        try:
            supplier_name = result.get("supplier", "Web")
            add_supplier_price(product_id, supplier_name, new_price, url=url)
            from src.agents.product_catalog import record_catalog_quote
            record_catalog_quote(product_id, "web_check", new_price,
                                 source="price_check_button", supplier_url=url)
        except Exception as e:
            log.debug("check-price update error: %s", e)

    return jsonify({
        "ok": True,
        "old_price": old_price,
        "new_price": new_price,
        "supplier": result.get("supplier", ""),
        "title": result.get("title", ""),
    })


# Background status for bulk price checks
_BULK_CHECK_STATUS = {"running": False, "checked": 0, "total": 0, "price_changes": 0, "errors": 0}


@bp.route("/api/catalog/bulk-check-prices", methods=["POST"])
@auth_required
@safe_route
def api_catalog_bulk_check_prices():
    """Check all supplier URLs for current pricing. Runs in background thread."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})

    if _BULK_CHECK_STATUS["running"]:
        return jsonify({"ok": False, "error": "Bulk check already running",
                        "status": _BULK_CHECK_STATUS})

    try:
        from src.agents.item_link_lookup import lookup_from_url, _is_login_required
    except ImportError:
        return jsonify({"ok": False, "error": "Link lookup module not available"})

    from src.agents.product_catalog import _get_conn, record_catalog_quote
    import threading, time

    conn = _get_conn()
    suppliers = conn.execute(
        "SELECT id, product_id, supplier_name, supplier_url, last_price FROM product_suppliers "
        "WHERE supplier_url IS NOT NULL AND supplier_url != '' ORDER BY last_checked ASC NULLS FIRST"
    ).fetchall()
    conn.close()
    suppliers = [dict(s) for s in suppliers]

    _BULK_CHECK_STATUS.update({"running": True, "checked": 0, "total": len(suppliers),
                                "price_changes": 0, "errors": 0})

    def _run_bulk():
        for s in suppliers:
            url = s["supplier_url"]
            try:
                if _is_login_required(url):
                    _BULK_CHECK_STATUS["checked"] += 1
                    continue
                result = lookup_from_url(url)
                if result.get("price"):
                    new_price = float(result["price"])
                    old_price = s.get("last_price") or 0
                    add_supplier_price(s["product_id"], s["supplier_name"], new_price, url=url)
                    if old_price > 0 and abs(new_price - old_price) > 0.01:
                        _BULK_CHECK_STATUS["price_changes"] += 1
                    record_catalog_quote(s["product_id"], "web_check", new_price,
                                         source="bulk_price_check", supplier_url=url)
                else:
                    _BULK_CHECK_STATUS["errors"] += 1
            except Exception:
                _BULK_CHECK_STATUS["errors"] += 1
            _BULK_CHECK_STATUS["checked"] += 1
            time.sleep(1.5)  # Rate limit
        _BULK_CHECK_STATUS["running"] = False

    t = threading.Thread(target=_run_bulk, daemon=True)
    t.start()

    return jsonify({"ok": True, "msg": f"Bulk check started for {len(suppliers)} suppliers",
                    "total": len(suppliers)})


@bp.route("/api/catalog/bulk-check-status")
@auth_required
@safe_route
def api_catalog_bulk_check_status():
    """Poll bulk price check progress."""
    return jsonify({"ok": True, **_BULK_CHECK_STATUS})


@bp.route("/api/catalog/rebuild-tokens", methods=["POST"])
@auth_required
@safe_route
def api_catalog_rebuild_tokens():
    """Rebuild search tokens for all products (migration utility)."""
    try:
        if not CATALOG_AVAILABLE:
            return jsonify({"ok": False, "error": "Catalog not available"})
        count = rebuild_search_tokens()
        return jsonify({"ok": True, "updated": count})
    except Exception as e:
        log.error("api_catalog_rebuild_tokens error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Catalog Match Audit ──────────────────────────────────────────────────────

@bp.route("/api/catalog/audit", methods=["GET", "POST"])
@auth_required
@safe_route
def api_catalog_audit():
    """Run DB-wide catalog match quality audit.
    GET: dry run (report only). GET ?fix=true or POST {fix: true} to auto-clear bad matches."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    try:
        if request.method == "POST":
            data = request.get_json(silent=True) or {}
            fix = data.get("fix", False)
        else:
            fix = request.args.get("fix", "").lower() in ("true", "1", "yes")
        result = audit_catalog_matches(fix=fix)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/audit/db")
@auth_required
@safe_route
def api_catalog_audit_db():
    """Audit the product catalog table itself for quality issues."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    try:
        return jsonify(audit_catalog_db())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── AI Product Finder ────────────────────────────────────────────────────────

@bp.route("/api/catalog/ai-find", methods=["POST"])
@auth_required
@safe_route
def api_catalog_ai_find():
    """Use Claude API to identify & source a single product.
    POST {description: "...", quantity: 1, agency: "CDCR"}"""
    try:
        data = request.get_json(silent=True) or {}
        desc = (data.get("description") or "").strip()
        if not desc:
            return jsonify({"ok": False, "error": "description required"})
        result = ai_find_product(
            desc,
            quantity=data.get("quantity", 1),
            agency=data.get("agency", ""),
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/ai-find-batch", methods=["POST"])
@auth_required
@safe_route
def api_catalog_ai_find_batch():
    """Use Claude API to identify & source multiple unmatched products.
    POST {items: [{idx, description, quantity}], agency: "CDCR"}"""
    try:
        data = request.get_json(silent=True) or {}
        items = data.get("items", [])
        if not items:
            return jsonify({"ok": False, "error": "items required"})
        results = ai_find_products_batch(items, agency=data.get("agency", ""))
        return jsonify({"ok": True, "results": results})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# Auto-import product catalog on startup if DB empty
try:
    if CATALOG_AVAILABLE:
        init_catalog_db()
        _cat_count = 0
        try:
            import sqlite3 as _sql3
            _conn = _sql3.connect(os.path.join(DATA_DIR, "reytech.db"), timeout=5)
            _cat_count = _conn.execute("SELECT COUNT(*) FROM product_catalog").fetchone()[0]
            _conn.close()
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        if _cat_count == 0:
            csv_path = os.path.join(DATA_DIR, "product_catalog_import.csv")
            if os.path.exists(csv_path):
                _result = import_qb_csv(csv_path)
                log.info("🏗️ Auto-imported product catalog: %d products from QB CSV", _result.get("imported", 0))
                # Run Sprint 1 fixes on fresh import
                try:
                    _fix_result = run_sprint1_fixes()
                    log.info("🔧 Sprint 1 fixes applied: names=%d, brands=%d, prices=%d",
                             _fix_result.get("names_fixed", 0), _fix_result.get("brands_found", 0),
                             _fix_result.get("prices_calculated", 0))
                except Exception as _fx:
                    log.warning("Sprint 1 fixes error: %s", _fx)
        elif _cat_count > 0:
            # Check if fixes have been applied (recommended_price populated)
            try:
                import sqlite3 as _sql3b
                _conn2 = _sql3b.connect(os.path.join(DATA_DIR, "reytech.db"), timeout=5)
                _unfixed = _conn2.execute(
                    "SELECT COUNT(*) FROM product_catalog WHERE recommended_price IS NULL AND cost > 0"
                ).fetchone()[0]
                _conn2.close()
                if _unfixed > 50:  # More than 50 unpriced products = fixes haven't run
                    log.info("🔧 Found %d unpriced products — running Sprint 1 fixes...", _unfixed)
                    _fix_result = run_sprint1_fixes()
                    log.info("🔧 Sprint 1 fixes: names=%d, brands=%d, prices=%d",
                             _fix_result.get("names_fixed", 0), _fix_result.get("brands_found", 0),
                             _fix_result.get("prices_calculated", 0))
            except Exception as _fx2:
                log.debug("Sprint 1 fix check: %s", _fx2)
except Exception as _e:
    log.warning("Product catalog auto-import failed: %s", _e)

# ═══════════════════════════════════════════════════════════════════════════════
# Shipping Dashboard (#7) — aggregate tracking across all orders
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/shipping")
@auth_required
@safe_page
def shipping_dashboard():
    """Shipping dashboard — all tracking numbers, carrier links, delivery status."""
    orders = _load_orders()
    
    shipments = []
    for oid, order in orders.items():
        if order.get("status") == "dismissed":
            continue
        for it in order.get("line_items", []):
            tracking = it.get("tracking_number", "")
            if not tracking:
                continue
            carrier = it.get("carrier", "")
            carrier_low = carrier.lower() if carrier else ""
            
            # Auto-detect carrier from tracking format
            if not carrier:
                if tracking.startswith("TBA"):
                    carrier = "Amazon"
                elif tracking.startswith("1Z"):
                    carrier = "UPS"
                elif len(tracking) in (12, 15, 20, 22) and tracking.isdigit():
                    carrier = "FedEx"
                elif len(tracking) in (20, 22, 26, 30, 34) and tracking.isdigit():
                    carrier = "USPS"
                carrier_low = carrier.lower()
            
            # Build tracking URL
            track_url = ""
            if "amazon" in carrier_low or tracking.startswith("TBA"):
                track_url = f"https://www.amazon.com/gp/your-account/order-history?search={tracking}"
            elif "ups" in carrier_low or tracking.startswith("1Z"):
                track_url = f"https://www.ups.com/track?tracknum={tracking}"
            elif "fedex" in carrier_low:
                track_url = f"https://www.fedex.com/fedextrack/?trknbr={tracking}"
            elif "usps" in carrier_low:
                track_url = f"https://tools.usps.com/go/TrackConfirmAction?tLabels={tracking}"
            
            status = it.get("sourcing_status", "")
            ship_date = it.get("ship_date", "")
            delivery_date = it.get("delivery_date", "")
            
            shipments.append({
                "order_id": oid,
                "po_number": order.get("po_number", ""),
                "institution": order.get("institution", ""),
                "description": it.get("description", "")[:50],
                "tracking": tracking,
                "carrier": carrier,
                "track_url": track_url,
                "status": status,
                "ship_date": ship_date,
                "delivery_date": delivery_date,
            })
    
    # Sort: undelivered first, then by ship date desc
    shipments.sort(key=lambda s: (s["status"] == "delivered", s.get("ship_date", "") or ""), reverse=True)
    
    # Stats
    total = len(shipments)
    in_transit = sum(1 for s in shipments if s["status"] in ("shipped", "ordered"))
    delivered_count = sum(1 for s in shipments if s["status"] == "delivered")
    carriers = {}
    for s in shipments:
        c = s["carrier"] or "Unknown"
        carriers[c] = carriers.get(c, 0) + 1
    
    return render_page("shipping.html", active_page="Shipping",
        shipments=shipments, total=total, in_transit=in_transit,
        delivered=delivered_count, carriers=carriers)


# ═══════════════════════════════════════════════════════════════════════════════
# Cross-Inbox Dedup (#10) — shared fingerprint table
# ═══════════════════════════════════════════════════════════════════════════════

def _init_dedup_table():
    """Create cross-inbox dedup table if not exists."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS email_fingerprints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fingerprint TEXT UNIQUE NOT NULL,
                    inbox TEXT NOT NULL,
                    subject TEXT,
                    sender TEXT,
                    message_id TEXT,
                    processed_at TEXT NOT NULL,
                    result_type TEXT,
                    result_id TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fp_fingerprint ON email_fingerprints(fingerprint)")
            conn.commit()
    except Exception as e:
        log.debug("Dedup table init: %s", e)

_init_dedup_table()


def _email_fingerprint(subject: str, sender: str, date_str: str = "") -> str:
    import hashlib
    raw = f"{(subject or '').strip().lower()}|{(sender or '').strip().lower()}|{(date_str or '')[:16]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def check_email_fingerprint(subject: str, sender: str, date_str: str = "",
                            message_id: str = "", inbox: str = "sales") -> bool:
    """Return True only if this email was previously processed AND produced
    a record (result_type non-empty).

    History note (2026-04-12): the prior implementation recorded the
    fingerprint on the very first call regardless of whether downstream
    processing succeeded — and there was no separate `record_*` call wired
    up. Effect: any classifier rejection or attachment download error
    permanently poisoned the dedup table for that email, and on the next
    poll cycle the cross-inbox dedup gate at email_poller.py:1846 silently
    dropped the same email forever. Kevin Jensen's RFQ from 2026-04-10
    was lost this way.

    Now this function is strictly read-only and only treats fingerprints
    as duplicates when result_type is set, i.e. the upstream caller actually
    invoked record_email_fingerprint after a successful pipeline run.
    """
    fp = _email_fingerprint(subject, sender, date_str)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT result_type, result_id FROM email_fingerprints WHERE fingerprint=?",
                (fp,)
            ).fetchone()
            if not row:
                return False
            # Treat as duplicate only when a downstream record was created.
            # Tentative rows from the legacy code path have empty result_type
            # and must NOT block reprocessing.
            try:
                rt = row["result_type"] if hasattr(row, "keys") else row[0]
            except Exception:
                rt = ""
            return bool((rt or "").strip())
    except Exception:
        return False


def record_email_fingerprint(subject: str, sender: str, date_str: str = "",
                             inbox: str = "sales", result_type: str = "",
                             result_id: str = "", message_id: str = ""):
    """Lock in a fingerprint after successful pipeline processing.

    Call this from the poll loop only when process_rfq_email (or the PO
    routing path) actually produced a record. Without this call, the
    fingerprint stays soft and check_email_fingerprint returns False on
    re-poll, giving the email another chance.
    """
    fp = _email_fingerprint(subject, sender, date_str)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO email_fingerprints
                (fingerprint, inbox, subject, sender, message_id, processed_at, result_type, result_id)
                VALUES (?,?,?,?,?,?,?,?)
            """, (fp, inbox, (subject or "")[:200], (sender or "")[:200],
                  (message_id or "")[:200],
                  datetime.now().isoformat(), result_type, result_id))
            conn.commit()
    except Exception as _e:
        log.debug("Suppressed: %s", _e)


def clear_tentative_fingerprints() -> int:
    """Delete every email_fingerprints row that has no result_type — i.e.
    a leftover from the buggy old check_email_fingerprint code path. Safe
    to run multiple times. Returns the number of rows removed."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM email_fingerprints "
                "WHERE result_type IS NULL OR result_type = ''"
            ).fetchone()
            count = int(row[0]) if row else 0
            if count > 0:
                conn.execute(
                    "DELETE FROM email_fingerprints "
                    "WHERE result_type IS NULL OR result_type = ''"
                )
                conn.commit()
            return count
    except Exception as e:
        log.debug("clear_tentative_fingerprints: %s", e)
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# Recurring Order Detection (#12)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/orders/recurring")
@auth_required
@safe_route
def api_recurring_orders():
    """Detect repeat buyers — same institution + similar items across multiple orders."""
    orders = _load_orders()
    institution_orders = {}
    for oid, order in orders.items():
        if order.get("status") == "dismissed":
            continue
        inst = (order.get("institution") or "").strip().lower()
        if inst:
            institution_orders.setdefault(inst, []).append(order)
    
    recurring = []
    for inst, inst_orders in institution_orders.items():
        if len(inst_orders) < 2:
            continue
        total_value = sum(o.get("total", 0) for o in inst_orders)
        all_items = set()
        for o in inst_orders:
            for it in o.get("line_items", []):
                all_items.add((it.get("description", "")[:40] or "").lower())
        recurring.append({
            "institution": inst_orders[0].get("institution", ""),
            "order_count": len(inst_orders),
            "total_value": total_value,
            "avg_value": total_value / len(inst_orders),
            "unique_items": len(all_items),
            "orders": [{"id": o["order_id"], "po": o.get("po_number",""), 
                         "total": o.get("total",0), "date": o.get("created_at","")[:10]}
                        for o in sorted(inst_orders, key=lambda x: x.get("created_at",""), reverse=True)],
        })
    
    recurring.sort(key=lambda r: r["total_value"], reverse=True)
    return jsonify({"ok": True, "recurring": recurring, "count": len(recurring)})


# ═══════════════════════════════════════════════════════════════════════════════
# Margin Calculator (#13) — cost vs sell per item per order
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/orders/margins")
@auth_required
@safe_route
def api_order_margins():
    """Calculate margins across all orders — cost (supplier) vs sell (quote) per item."""
    orders = _load_orders()
    margin_data = []
    total_revenue = 0
    total_cost = 0
    
    for oid, order in orders.items():
        if order.get("status") == "dismissed":
            continue
        for it in order.get("line_items", []):
            sell = it.get("unit_price", 0) or 0
            cost = it.get("cost", 0) or 0
            qty = it.get("qty", 0) or 1
            if sell > 0:
                margin = (sell - cost) / sell * 100 if cost else 0
                revenue = sell * qty
                cost_total = cost * qty
                profit = revenue - cost_total
                total_revenue += revenue
                total_cost += cost_total
                margin_data.append({
                    "order_id": oid,
                    "po_number": order.get("po_number", ""),
                    "institution": order.get("institution", ""),
                    "description": it.get("description", "")[:60],
                    "sell_price": sell,
                    "cost": cost,
                    "qty": qty,
                    "margin_pct": round(margin, 1),
                    "profit": round(profit, 2),
                    "revenue": round(revenue, 2),
                })
    
    margin_data.sort(key=lambda m: m["profit"], reverse=True)
    overall_margin = round((total_revenue - total_cost) / total_revenue * 100, 1) if total_revenue else 0
    
    return jsonify({
        "ok": True,
        "items": margin_data[:100],
        "summary": {
            "total_revenue": round(total_revenue, 2),
            "total_cost": round(total_cost, 2),
            "total_profit": round(total_revenue - total_cost, 2),
            "overall_margin_pct": overall_margin,
            "items_with_cost": sum(1 for m in margin_data if m["cost"] > 0),
            "items_without_cost": sum(1 for m in margin_data if m["cost"] == 0),
        }
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Payment Tracking (#14) — post-invoice payment aging
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/order/<oid>/payment", methods=["POST"])
@auth_required
@safe_route
def api_order_payment(oid):
    """Record payment received. POST: {amount, date, method, reference}"""
    try:
        orders = _load_orders()
        order = orders.get(oid)
        if not order:
            return jsonify({"ok": False, "error": "Order not found"})

        data = request.get_json(silent=True) or {}
        payment = {
            "amount": float(data.get("amount", 0)),
            "date": data.get("date", datetime.now().strftime("%Y-%m-%d")),
            "method": data.get("method", "check"),
            "reference": data.get("reference", ""),
            "recorded_at": datetime.now().isoformat(),
        }

        if "payments" not in order:
            order["payments"] = []
        order["payments"].append(payment)
        order["total_paid"] = sum(p["amount"] for p in order["payments"])
        order["payment_status"] = "paid" if order["total_paid"] >= order.get("total", 0) else "partial"
        order["updated_at"] = datetime.now().isoformat()

        orders[oid] = order
        _save_orders(orders)

        _log_crm_activity(order.get("quote_number", ""), "payment_received",
                          f"Payment ${payment['amount']:,.2f} on order {oid} via {payment['method']}",
                          actor="user", metadata={"order_id": oid, "payment": payment})

        return jsonify({"ok": True, "total_paid": order["total_paid"], "payment_status": order["payment_status"]})
    except Exception as e:
        log.error("api_order_payment error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/orders/aging")
@auth_required
@safe_route
def api_orders_aging():
    """Invoice aging report — how long since invoice, payment status."""
    orders = _load_orders()
    aging = []
    now = datetime.now()
    
    for oid, order in orders.items():
        if order.get("status") == "dismissed":
            continue
        inv = order.get("draft_invoice") or {}
        if not inv.get("invoice_number"):
            continue
        
        inv_date = inv.get("created_at", order.get("created_at", ""))
        try:
            from dateutil.parser import parse as _dp
            dt = _dp(inv_date)
            days = (now - dt.replace(tzinfo=None)).days
        except Exception:
            days = 0
        
        total = order.get("total", 0)
        paid = order.get("total_paid", 0)
        balance = total - paid
        
        if balance <= 0:
            bucket = "Paid"
        elif days <= 30:
            bucket = "Current"
        elif days <= 45:
            bucket = "31-45 Days"
        elif days <= 60:
            bucket = "46-60 Days"
        elif days <= 90:
            bucket = "61-90 Days"
        else:
            bucket = "90+ Days"
        
        aging.append({
            "order_id": oid,
            "invoice_number": inv.get("invoice_number", ""),
            "institution": order.get("institution", ""),
            "po_number": order.get("po_number", ""),
            "invoice_date": inv_date[:10],
            "days_outstanding": days,
            "total": total,
            "paid": paid,
            "balance": balance,
            "bucket": bucket,
        })
    
    aging.sort(key=lambda a: a["days_outstanding"], reverse=True)
    
    # Summary by bucket
    buckets = {}
    for a in aging:
        b = a["bucket"]
        buckets[b] = buckets.get(b, {"count": 0, "total": 0})
        buckets[b]["count"] += 1
        buckets[b]["total"] += a["balance"]
    
    return jsonify({"ok": True, "invoices": aging, "buckets": buckets, "total_outstanding": sum(a["balance"] for a in aging)})


# ═══════════════════════════════════════════════════════════════════════════════
# Audit Trail (#16) — every admin action logged
# ═══════════════════════════════════════════════════════════════════════════════

def _log_audit(action: str, details: str = "", metadata: dict = None):
    """Log an admin action to the audit trail."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_trail (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT,
                    ip_address TEXT,
                    user_agent TEXT,
                    metadata TEXT
                )
            """)
            conn.execute(
                "INSERT INTO audit_trail (timestamp, action, details, ip_address, user_agent, metadata) VALUES (?,?,?,?,?,?)",
                (datetime.now().isoformat(), action, details[:500],
                 request.remote_addr if request else "",
                 (request.user_agent.string[:200] if request and request.user_agent else ""),
                 json.dumps(metadata or {}, default=str)[:1000])
            )
            conn.commit()
    except Exception as e:
        log.debug("Audit log error: %s", e)


@bp.route("/api/audit")
@auth_required
@safe_route
def api_audit_trail():
    """View audit trail — last 100 actions."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_trail (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT,
                    ip_address TEXT,
                    user_agent TEXT,
                    metadata TEXT
                )
            """)
            rows = conn.execute(
                "SELECT * FROM audit_trail ORDER BY timestamp DESC LIMIT 100"
            ).fetchall()
            return jsonify({"ok": True, "entries": [dict(r) for r in rows]})
    except Exception:
        return jsonify({"ok": True, "entries": []})


# ═══════════════════════════════════════════════════════════════════════════════
# Pricing Intelligence (#5) — Historical winning prices
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/pricing/intel")
@auth_required
@safe_route
def api_pricing_intel():
    """Get pricing intelligence summary."""
    try:
        from src.knowledge.pricing_intel import get_pricing_intelligence_summary
        return jsonify({"ok": True, **get_pricing_intelligence_summary()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricing/recommend-price")
@auth_required
@safe_route
def api_pricing_recommend_price():
    """Get price recommendation for an item.
    Query params: description, part_number, agency, institution"""
    desc = request.args.get("description", "")
    pn = request.args.get("part_number", "")
    agency = request.args.get("agency", "")
    institution = request.args.get("institution", "")
    
    if not desc and not pn:
        return jsonify({"ok": False, "error": "description or part_number required"})
    
    try:
        from src.knowledge.pricing_intel import get_price_recommendation
        result = get_price_recommendation(desc, pn, agency, institution)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricing/trends")
@auth_required
@safe_route
def api_pricing_trends():
    """Get price trends for a specific item.
    Query params: part_number, description, limit"""
    pn = request.args.get("part_number", "")
    desc = request.args.get("description", "")
    try:
        limit = max(1, min(int(request.args.get("limit", 50)), 500))
    except (ValueError, TypeError, OverflowError):
        limit = 50

    try:
        from src.knowledge.pricing_intel import get_item_price_trends
        trends = get_item_price_trends(part_number=pn, description=desc, limit=limit)
        return jsonify({"ok": True, "trends": trends, "count": len(trends)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/pricing")
@auth_required
def pricing_intel_page():
    """Redirect to unified analytics tab."""
    return redirect("/analytics?tab=pricing")


# ═══════════════════════════════════════════════════════════════════════════════
# Recurring Orders UI (#12) — detect repeat buyers
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/recurring")
@auth_required
@safe_page
def recurring_orders_page():
    """Recurring orders page — detect repeat buyers for template reuse."""
    orders = _load_orders()
    institution_orders = {}
    for oid, order in orders.items():
        if order.get("status") == "dismissed":
            continue
        inst = (order.get("institution") or "").strip()
        if inst:
            institution_orders.setdefault(inst.lower(), {"name": inst, "orders": []})
            institution_orders[inst.lower()]["orders"].append(order)

    recurring = []
    for inst_key, data in institution_orders.items():
        inst_orders = data["orders"]
        if len(inst_orders) < 2:
            continue
        total_value = sum(o.get("total", 0) for o in inst_orders)
        all_items = set()
        for o in inst_orders:
            for it in o.get("line_items", []):
                d = (it.get("description", "")[:40] or "").lower()
                if d:
                    all_items.add(d)
        recurring.append({
            "institution": data["name"],
            "order_count": len(inst_orders),
            "total_value": total_value,
            "avg_value": total_value / len(inst_orders),
            "unique_items": len(all_items),
            "orders": sorted(inst_orders, key=lambda x: x.get("created_at", ""), reverse=True),
        })
    recurring.sort(key=lambda r: r["total_value"], reverse=True)

    # Stats
    total_recurring = len(recurring)
    total_value_recurring = sum(r["total_value"] for r in recurring)
    total_orders_recurring = sum(r["order_count"] for r in recurring)

    return render_page("recurring.html", active_page="Orders",
        recurring=recurring, total_recurring=total_recurring,
        total_orders=total_orders_recurring, total_value=total_value_recurring)


# ═══════════════════════════════════════════════════════════════════════════════
# Margin Calculator UI (#13) — order-level profitability
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/margins")
@auth_required
@safe_page
def margins_page():
    """Margin calculator dashboard — cost vs sell per item per order."""
    orders = _load_orders()
    margin_data = []
    total_revenue = 0
    total_cost = 0

    for oid, order in orders.items():
        if order.get("status") == "dismissed":
            continue
        for it in order.get("line_items", []):
            sell = it.get("unit_price", 0) or 0
            cost = it.get("cost", 0) or 0
            qty = it.get("qty", 0) or 1
            if sell > 0:
                margin = (sell - cost) / sell * 100 if cost else 0
                revenue = sell * qty
                cost_total = cost * qty
                profit = revenue - cost_total
                total_revenue += revenue
                total_cost += cost_total
                margin_data.append({
                    "order_id": oid,
                    "po": order.get("po_number", ""),
                    "institution": order.get("institution", ""),
                    "description": it.get("description", "")[:50],
                    "sell": sell, "cost": cost, "qty": qty,
                    "margin": round(margin, 1),
                    "profit": round(profit, 2),
                    "revenue": round(revenue, 2),
                })

    margin_data.sort(key=lambda m: m["profit"], reverse=True)
    overall_margin = round((total_revenue - total_cost) / total_revenue * 100, 1) if total_revenue else 0
    total_profit = total_revenue - total_cost
    costed = sum(1 for m in margin_data if m["cost"] > 0)
    uncosted = sum(1 for m in margin_data if m["cost"] == 0)
    negative = sum(1 for m in margin_data if m["margin"] < 0)

    return render_page("margins.html", active_page="Pricing",
        margin_data=margin_data, total_revenue=total_revenue, total_cost=total_cost,
        total_profit=total_profit, overall_margin=overall_margin,
        costed=costed, uncosted=uncosted, negative=negative)


# ═══════════════════════════════════════════════════════════════════════════════
# Payment Tracking + Aging UI (#14)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/payments")
@auth_required
@safe_page
def payments_page():
    """Payment tracking dashboard — invoice aging, payment recording."""
    orders = _load_orders()
    now = datetime.now()
    invoices = []
    total_outstanding = 0
    total_paid_amt = 0
    buckets = {"Paid": 0, "Current": 0, "31-45 Days": 0, "46-60 Days": 0, "61-90 Days": 0, "90+ Days": 0}

    for oid, order in orders.items():
        if order.get("status") == "dismissed":
            continue
        inv = order.get("draft_invoice") or {}
        if not inv.get("invoice_number"):
            continue
        inv_date = inv.get("created_at", order.get("created_at", ""))
        try:
            from dateutil.parser import parse as _dp
            dt = _dp(inv_date)
            days = (now - dt.replace(tzinfo=None)).days
        except Exception:
            days = 0

        total = order.get("total", 0)
        paid = order.get("total_paid", 0)
        balance = total - paid
        total_paid_amt += paid

        if balance <= 0:
            bucket = "Paid"
        elif days <= 30:
            bucket = "Current"
        elif days <= 45:
            bucket = "31-45 Days"
        elif days <= 60:
            bucket = "46-60 Days"
        elif days <= 90:
            bucket = "61-90 Days"
        else:
            bucket = "90+ Days"

        buckets[bucket] = buckets.get(bucket, 0) + balance
        if balance > 0:
            total_outstanding += balance

        invoices.append({
            "oid": oid,
            "inv_num": inv.get("invoice_number", ""),
            "institution": order.get("institution", ""),
            "po": order.get("po_number", ""),
            "inv_date": inv_date[:10],
            "days": days,
            "total": total,
            "paid": paid,
            "balance": balance,
            "bucket": bucket,
            "payment_status": order.get("payment_status", "unpaid"),
        })

    invoices.sort(key=lambda a: a["days"], reverse=True)

    return render_page("payments.html", active_page="Pricing",
        invoices=invoices, total_outstanding=total_outstanding,
        total_paid=total_paid_amt, buckets=buckets)


# ═══════════════════════════════════════════════════════════════════════════════
# Audit Trail UI (#16)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/audit")
@auth_required
@safe_page
def audit_trail_page():
    """Audit trail dashboard — every admin action logged."""
    entries = []
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_trail (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT,
                    ip_address TEXT,
                    user_agent TEXT,
                    metadata TEXT
                )
            """)
            # Pull from audit_trail first
            rows = conn.execute(
                "SELECT * FROM audit_trail ORDER BY timestamp DESC LIMIT 200"
            ).fetchall()
            entries = [dict(r) for r in rows]

            # Also pull from activity_log (the table that actually has data)
            # activity_log uses different column names — normalize them
            try:
                al_rows = conn.execute("""
                    SELECT id, created_at as timestamp, action, detail as details,
                           '' as ip_address, '' as user_agent, '' as metadata
                    FROM activity_log
                    ORDER BY created_at DESC LIMIT 200
                """).fetchall()
                entries.extend([dict(r) for r in al_rows])
            except Exception:
                pass  # activity_log may not exist on fresh installs

            # Sort combined entries by timestamp descending, keep top 200
            entries.sort(key=lambda e: e.get("timestamp") or "", reverse=True)
            entries = entries[:200]
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Group by action type for stats
    action_counts = {}
    for e in entries:
        a = e.get("action", "unknown")
        action_counts[a] = action_counts.get(a, 0) + 1

    return render_page("audit.html", active_page="Home",
        entries=entries, action_counts=action_counts)


# ══ Consolidated from routes_features*.py ══════════════════════════════════

# ── QB Action Endpoints ─────────────────────────────────────────────────────

@bp.route("/api/qb/sync-customers", methods=["POST"])
@auth_required
@safe_route
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
        except Exception:
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
@safe_route
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
            except Exception:
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
@safe_route
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
            except Exception:
                days_until = 30
            if days_until <= 30:
                inflows.append({"source": f"Invoice #{inv.get('DocNumber', '?')}", "amount": amount,
                                "due": due_str, "days_until": days_until,
                                "customer": inv.get("CustomerRef", {}).get("name", "?")})

        # Pipeline value
        pipeline_value = 0
        try:
            from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
            cur = conn.execute("SELECT SUM(total) FROM quotes WHERE is_test=0 AND status IN ('sent','quoted') AND total > 0")
            row = cur.fetchone()
            pipeline_value = float(row[0] or 0)
            conn.close()
        except Exception as _e:
            log.debug('suppressed in api_qb_cash_flow: %s', _e)

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
@safe_route
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
@safe_route
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

        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
        conn.row_factory = sqlite3.Row
        quote = conn.execute("SELECT * FROM quotes WHERE is_test=0 AND quote_number=?", (qnum,)).fetchone()
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
@safe_route
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
            except Exception:
                continue
            monthly[key] += float(p.get("TotalAmt", 0))
        result = [{"month": k, "revenue": round(v, 2)} for k, v in sorted(monthly.items())]
        return jsonify({"ok": True, "months": result, "ytd_total": round(sum(v["revenue"] for v in result if v["month"].startswith(str(datetime.now().year))), 2)})
    except Exception as e:
        log.exception("Revenue by month failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/draft-reminders", methods=["POST"])
@auth_required
@safe_route
def api_qb_draft_reminders():
    """Draft payment reminder emails for overdue invoices."""
    try:
        from src.agents.quickbooks_agent import fetch_invoices, fetch_customers, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        invoices = fetch_invoices(status="overdue")
        if not invoices:
            return jsonify({"ok": True, "message": "No overdue invoices found", "drafts": []})
        customers = {c.get("Id"): c for c in fetch_customers()}
        drafts = []
        for inv in invoices[:10]:
            cust_ref = inv.get("CustomerRef", {})
            cust_id = cust_ref.get("value", "")
            cust_name = cust_ref.get("name", "Customer")
            cust = customers.get(cust_id, {})
            email = cust.get("PrimaryEmailAddr", {}).get("Address", "") if isinstance(cust.get("PrimaryEmailAddr"), dict) else ""
            balance = float(inv.get("Balance", 0))
            inv_num = inv.get("DocNumber", "?")
            due_date = inv.get("DueDate", "?")
            days_overdue = 0
            try:
                due_dt = datetime.strptime(due_date, "%Y-%m-%d")
                days_overdue = (datetime.now() - due_dt).days
            except Exception as _e:
                log.debug('suppressed in api_qb_draft_reminders: %s', _e)
            drafts.append({
                "to": email or f"(no email for {cust_name})",
                "customer": cust_name, "invoice_number": inv_num,
                "amount": balance, "due_date": due_date, "days_overdue": days_overdue,
                "subject": f"Payment Reminder — Invoice #{inv_num} (${balance:,.2f})",
                "body": (f"Dear {cust_name},\n\nThis is a friendly reminder that Invoice #{inv_num} "
                         f"for ${balance:,.2f} was due on {due_date} ({days_overdue} days ago).\n\n"
                         f"Please arrange payment at your earliest convenience.\n\nThank you,\nReytech Inc."),
            })
        return jsonify({"ok": True, "drafts": drafts, "count": len(drafts),
                        "total_overdue": sum(d["amount"] for d in drafts)})
    except Exception as e:
        log.exception("Draft reminders failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/profit-margins")
@auth_required
@safe_route
def api_qb_profit_margins():
    """Calculate profit margins from QB invoice and purchase data."""
    try:
        from src.agents.quickbooks_agent import fetch_invoices, get_recent_purchase_orders, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        invoices = fetch_invoices(status="all", days_back=180)
        pos = get_recent_purchase_orders(days_back=180)
        cust_revenue = defaultdict(float)
        total_revenue = 0
        for inv in invoices:
            cust = inv.get("CustomerRef", {}).get("name", "Unknown")
            amt = float(inv.get("TotalAmt", 0))
            cust_revenue[cust] += amt
            total_revenue += amt
        total_cost = sum(float(po.get("TotalAmt", 0)) for po in pos)
        gross_margin = total_revenue - total_cost
        margin_pct = (gross_margin / total_revenue * 100) if total_revenue > 0 else 0
        top_customers = sorted(cust_revenue.items(), key=lambda x: -x[1])[:10]
        return jsonify({
            "ok": True, "total_revenue_180d": round(total_revenue, 2),
            "total_cost_180d": round(total_cost, 2),
            "gross_margin": round(gross_margin, 2),
            "margin_percent": round(margin_pct, 1),
            "top_customers": [{"customer": c, "revenue": round(r, 2)} for c, r in top_customers],
            "invoice_count": len(invoices), "po_count": len(pos),
        })
    except Exception as e:
        log.exception("Profit margins failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/expense-summary")
@auth_required
@safe_route
def api_qb_expense_summary():
    """Expense breakdown from QB purchase orders and bills."""
    try:
        from src.agents.quickbooks_agent import get_recent_purchase_orders, is_configured
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured"})
        pos = get_recent_purchase_orders(days_back=90)
        vendor_spend = defaultdict(float)
        total = 0
        for po in pos:
            vendor = po.get("VendorRef", {}).get("name", "Unknown")
            amt = float(po.get("TotalAmt", 0))
            vendor_spend[vendor] += amt
            total += amt
        top_vendors = sorted(vendor_spend.items(), key=lambda x: -x[1])[:15]
        # Try QB bills query
        bills, bill_total = [], 0
        try:
            from src.agents.quickbooks_agent import _qb_query
            bills = _qb_query("SELECT * FROM Bill WHERE TxnDate >= '{}' MAXRESULTS 100".format(
                (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")))
            bill_total = sum(float(b.get("TotalAmt", 0)) for b in bills)
        except Exception as _e:
            log.debug('suppressed in api_qb_expense_summary: %s', _e)
        return jsonify({
            "ok": True, "po_total_90d": round(total, 2),
            "bill_total_90d": round(bill_total, 2),
            "combined_expenses": round(total + bill_total, 2),
            "top_vendors": [{"vendor": v, "amount": round(a, 2)} for v, a in top_vendors],
            "po_count": len(pos), "bill_count": len(bills),
        })
    except Exception as e:
        log.exception("Expense summary failed")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/test-connection")
@auth_required
@safe_route
def api_qb_test_connection():
    """Quick QB connection test — tries to fetch company info."""
    try:
        from src.agents.quickbooks_agent import is_configured, get_access_token, get_company_info, _load_tokens
        tokens = _load_tokens()
        has_token = bool(tokens.get("access_token"))
        configured = is_configured()
        result = {
            "ok": True,
            "has_token_file": has_token,
            "is_configured": configured,
            "realm_id": tokens.get("realm_id", "")[:6] + "..." if tokens.get("realm_id") else "",
            "connected_at": tokens.get("connected_at", ""),
            "last_refreshed": tokens.get("refreshed_at", ""),
        }
        if configured:
            token = get_access_token()
            result["has_valid_access_token"] = bool(token)
            if token:
                info = get_company_info()
                result["company"] = info.get("name", "") if info else "FAILED"
                result["api_reachable"] = bool(info)
            else:
                result["api_reachable"] = False
                result["hint"] = "Token refresh failed — try reconnecting via Connect QuickBooks"
        else:
            missing = []
            if not os.environ.get("QB_CLIENT_ID"): missing.append("QB_CLIENT_ID")
            if not os.environ.get("QB_CLIENT_SECRET"): missing.append("QB_CLIENT_SECRET")
            if not tokens.get("refresh_token") and not os.environ.get("QB_REFRESH_TOKEN"): missing.append("refresh_token (connect QB first)")
            if not tokens.get("realm_id") and not os.environ.get("QB_REALM_ID"): missing.append("realm_id (connect QB first)")
            result["missing"] = missing
            result["hint"] = "Missing: " + ", ".join(missing)
        return jsonify(result)
    except ImportError:
        return jsonify({"ok": False, "error": "QuickBooks agent module not available"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/force-refresh", methods=["POST"])
@auth_required
@safe_route
def api_qb_force_refresh():
    """Force-refresh the QB access token."""
    try:
        from src.agents.quickbooks_agent import _refresh_access_token, _load_tokens
        token = _refresh_access_token()
        if token:
            tokens = _load_tokens()
            return jsonify({"ok": True, "message": "Token refreshed successfully",
                            "expires_at": tokens.get("expires_at", 0),
                            "realm_id": tokens.get("realm_id", "")[:6] + "..."})
        return jsonify({"ok": False, "error": "Token refresh failed — check credentials or reconnect"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qb/summary-card")
@auth_required
@safe_route
def api_qb_summary_card():
    """Pre-formatted QB financial summary for dashboard cards."""
    try:
        from src.agents.quickbooks_agent import is_configured, get_financial_context
        if not is_configured():
            return jsonify({"ok": False, "connected": False, "error": "QB not configured"})
        ctx = get_financial_context()
        return jsonify({
            "ok": True, "connected": True,
            "receivable": ctx.get("total_receivable", 0),
            "overdue": ctx.get("overdue_amount", 0),
            "collected": ctx.get("total_collected", 0),
            "open_invoices": ctx.get("open_invoices", 0),
            "customers": ctx.get("customer_count", 0),
            "vendors": ctx.get("vendor_count", 0),
            "last_updated": datetime.now().isoformat(),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Catalog Intelligence Endpoints ──────────────────────────────────────────

@bp.route("/api/catalog/margin-analysis")
@auth_required
@safe_route
def api_catalog_margin_analysis():
    """Analyze catalog products by margin tier."""
    db_path = os.path.join(DATA_DIR, "catalog.db")
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "tiers": {}, "total": 0})
    try:
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
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
@safe_route
def api_catalog_top_quoted():
    """Top 20 most-quoted catalog items."""
    db_path = os.path.join(DATA_DIR, "catalog.db")
    if not os.path.exists(db_path):
        return jsonify({"ok": True, "items": []})
    try:
        from src.core.db import DB_PATH as _DB_PATH; conn = sqlite3.connect(_DB_PATH, timeout=10); conn.row_factory = sqlite3.Row
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
@safe_route
def api_catalog_quick_quote():
    """Search catalog for quick quote pricing."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    q = request.args.get("q", "")
    if not q:
        return jsonify({"ok": False, "error": "?q= search term required"})
    try:
        init_catalog_db()
        items = search_products(q, limit=10)
        matches = [{"name": i["name"], "price": float(i.get("sell_price") or 0),
                     "cost": float(i.get("cost") or 0),
                     "margin": round(float(i.get("margin_pct") or 0), 1),
                     "sku": i.get("sku") or i.get("mfg_number") or "",
                     "category": i.get("category") or "",
                     "times_quoted": i.get("times_quoted") or 0,
                     "recommended_price": float(i.get("recommended_price") or 0)}
                    for i in items]
        return jsonify({"ok": True, "query": q, "matches": matches, "count": len(matches)})
    except Exception as e:
        log.error("Quick quote error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/catalog/price-history")
@auth_required
@safe_route
def api_catalog_price_history():
    """Get price history for a catalog item. ?pid=product_id or ?q=keyword"""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    try:
        import sqlite3 as _sql
        from src.agents.product_catalog import DB_PATH as _CAT_DB, init_catalog_db
        init_catalog_db()
        conn = _sql.connect(_CAT_DB, timeout=10)
        conn.row_factory = _sql.Row

        pid = request.args.get("pid", "")
        q = request.args.get("q", "")
        days = int(request.args.get("days", 90))

        if pid:
            product = conn.execute("SELECT * FROM product_catalog WHERE id = ?", (pid,)).fetchone()
        elif q:
            product = conn.execute(
                "SELECT * FROM product_catalog WHERE name LIKE ? OR sku LIKE ? OR mfg_number LIKE ? LIMIT 1",
                (f"%{q}%", f"%{q}%", f"%{q}%")).fetchone()
        else:
            conn.close()
            return jsonify({"ok": False, "error": "Provide ?pid=product_id or ?q=keyword"})

        if not product:
            conn.close()
            return jsonify({"ok": False, "error": "Item not found"})

        p = dict(product)
        history = conn.execute("""
            SELECT price_type, price, quantity, source, agency, institution,
                   quote_number, pc_id, supplier_url, recorded_at
            FROM catalog_price_history
            WHERE product_id = ?
            ORDER BY recorded_at DESC LIMIT 50
        """, (p["id"],)).fetchall()
        conn.close()

        return jsonify({
            "ok": True,
            "product_id": p["id"],
            "name": p.get("name", ""),
            "current_price": float(p.get("sell_price") or 0),
            "cost": float(p.get("cost") or 0),
            "margin_pct": float(p.get("margin_pct") or 0),
            "times_quoted": int(p.get("times_quoted") or 0),
            "times_won": int(p.get("times_won") or 0),
            "history": [dict(h) for h in history],
            "history_points": len(history),
        })
    except Exception as e:
        log.error("Price history error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# ── From routes_features2.py ───────────────────────────────────────────────

@bp.route("/api/qb/customer-health")
@auth_required
@safe_route
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
                    except Exception as _e:
                        log.debug('suppressed in api_qb_customer_health: %s', _e)
            elif due:
                try:
                    if datetime.strptime(due, "%Y-%m-%d") < datetime.now():
                        cust_stats[cid]["overdue"] += 1
                except Exception as _e:
                    log.debug('suppressed in api_qb_customer_health: %s', _e)

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


@bp.route("/api/qb/customer-lifetime-value")
@auth_required
@safe_route
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
            except Exception:
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


@bp.route("/api/qb/payment-aging-trend")
@auth_required
@safe_route
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


@bp.route("/api/qb/top-products-report")
@auth_required
@safe_route
def api_qb_top_products_report():
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


@bp.route("/api/qb/price-comparison")
@auth_required
@safe_route
def api_qb_price_comparison():
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


@bp.route("/api/qb/reorder-alerts")
@auth_required
@safe_route
def api_qb_reorder_alerts():
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
                except Exception: pass

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


# ── From routes_features3.py ───────────────────────────────────────────────

@bp.route("/api/qb/quick-dashboard")
@auth_required
@safe_route
def api_qb_quick_dashboard():
    """Combined QB dashboard — invoices, payments, overdue, customers in one call."""
    try:
        from src.agents.quickbooks_agent import (
            is_configured, fetch_invoices, fetch_customers,
            fetch_vendors, fetch_payments, get_company_info
        )
        if not is_configured():
            return jsonify({"ok": False, "error": "QuickBooks not configured. Click 'Connect QuickBooks' first."})

        company = get_company_info() or {}
        invoices = fetch_invoices() or []
        customers = fetch_customers() or []
        vendors = fetch_vendors() or []
        payments = fetch_payments() or []

        open_inv = [i for i in invoices if i.get("Balance", 0) > 0]
        overdue = [i for i in open_inv if i.get("DueDate") and i["DueDate"] < datetime.now().strftime("%Y-%m-%d")]

        total_receivable = sum(i.get("Balance", 0) for i in open_inv)
        total_overdue = sum(i.get("Balance", 0) for i in overdue)
        total_paid = sum(p.get("TotalAmt", 0) for p in payments)

        return jsonify({
            "ok": True,
            "company": company.get("CompanyName", "Unknown"),
            "summary": {
                "total_receivable": round(total_receivable, 2),
                "total_overdue": round(total_overdue, 2),
                "total_collected_30d": round(total_paid, 2),
                "open_invoices": len(open_inv),
                "overdue_invoices": len(overdue),
                "customers": len(customers),
                "vendors": len(vendors),
            },
            "top_overdue": [
                {"customer": i.get("CustomerRef", {}).get("name", "?"),
                 "amount": i.get("Balance", 0),
                 "due": i.get("DueDate", "?"),
                 "invoice": i.get("DocNumber", "?")}
                for i in sorted(overdue, key=lambda x: x.get("Balance", 0), reverse=True)[:5]
            ],
            "recent_payments": [
                {"customer": p.get("CustomerRef", {}).get("name", "?"),
                 "amount": p.get("TotalAmt", 0),
                 "date": p.get("TxnDate", "?")}
                for p in sorted(payments, key=lambda x: x.get("TxnDate", ""), reverse=True)[:5]
            ]
        })
    except ImportError:
        return jsonify({"ok": False, "error": "QuickBooks agent not available"})
    except Exception as e:
        log.error(f"QB quick dashboard error: {e}")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/pricing-suggestion")
@auth_required
@safe_route
def api_pricing_suggestion():
    """Get AI pricing suggestions for catalog items."""
    product_name = request.args.get("product", "").strip()

    cat_path = os.path.join(DATA_DIR, "product_catalog.json")
    suggestions = []

    try:
        with open(cat_path) as f:
            cat = json.load(f)

        for pid, p in cat.get("products", {}).items():
            if product_name and product_name.lower() not in (p.get("name") or "").lower():
                continue

            cost = p.get("supplier_cost", 0)
            last_price = p.get("last_quoted_price", 0)
            prices = p.get("price_history", [])

            if cost > 0 and last_price > 0:
                current_margin = ((last_price - cost) / last_price) * 100

                # Suggest based on margin targets
                suggested_low = round(cost * 1.15, 2)   # 15% margin
                suggested_mid = round(cost * 1.25, 2)    # 25% margin
                suggested_high = round(cost * 1.35, 2)   # 35% margin

                suggestions.append({
                    "product": p.get("name", "?")[:50],
                    "current_cost": cost,
                    "current_price": last_price,
                    "current_margin": round(current_margin, 1),
                    "suggested_competitive": suggested_low,
                    "suggested_balanced": suggested_mid,
                    "suggested_premium": suggested_high,
                    "times_quoted": p.get("times_quoted", 0),
                    "flag": "⚠️ Low margin" if current_margin < 10 else "✅ Healthy" if current_margin < 40 else "💰 High margin"
                })

        suggestions.sort(key=lambda x: x.get("current_margin", 50))
    except Exception: pass

    return jsonify({
        "ok": True,
        "suggestions": suggestions[:20],
        "count": len(suggestions),
    })


# ═══════════════════════════════════════════════════════════════════════
# Feature 1: Price Change Dashboard
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/catalog/price-alerts")
@auth_required
@safe_page
def catalog_price_alerts():
    """Dashboard showing products where web price differs from catalog price."""
    if not CATALOG_AVAILABLE:
        return _wrap_page("<div class='card'><p>Catalog not available.</p></div>", "Price Alerts")

    from src.agents.product_catalog import _get_conn as _cat_conn
    conn = _cat_conn()

    # Find products with supplier price != catalog sell/cost, ordered by dollar impact
    alerts = conn.execute("""
        SELECT pc.id, pc.name, pc.sell_price, pc.cost, pc.margin_pct, pc.category,
               pc.mfg_number, pc.manufacturer,
               ps.supplier_name, ps.last_price as web_price, ps.supplier_url,
               ps.last_checked, ps.id as supplier_id
        FROM product_suppliers ps
        JOIN product_catalog pc ON pc.id = ps.product_id
        WHERE ps.supplier_url IS NOT NULL AND ps.supplier_url != ''
          AND ps.last_price IS NOT NULL AND ps.last_price > 0
          AND (pc.cost > 0 OR pc.sell_price > 0)
        ORDER BY ABS(ps.last_price - CASE WHEN pc.cost > 0 THEN pc.cost ELSE pc.sell_price END) DESC
        LIMIT 200
    """).fetchall()
    conn.close()

    # Split into: price increased, price decreased, competitive risk
    increased = []
    decreased = []
    competitive_risk = []
    for a in alerts:
        a = dict(a)
        ref_price = a["cost"] if a["cost"] > 0 else a["sell_price"]
        a["delta"] = a["web_price"] - ref_price
        a["delta_pct"] = round(a["delta"] / ref_price * 100, 1) if ref_price > 0 else 0
        if abs(a["delta"]) < 0.02:
            continue
        if a["delta"] > 0:
            increased.append(a)
        else:
            decreased.append(a)
        # Competitive risk: web price < our sell price
        if a["sell_price"] > 0 and a["web_price"] < a["sell_price"]:
            competitive_risk.append(a)

    def _alert_rows(items, show_action=True):
        rows = ""
        for a in items[:50]:
            dc = "#f85149" if a["delta"] > 0 else "#3fb950"
            sign = "+" if a["delta"] > 0 else ""
            url_short = (a.get("supplier_url", "") or "")[:45]
            action = ""
            if show_action:
                action = f'''<td><button onclick="updateCostFromAlert({a['id']},{a['web_price']:.2f})" class="btn btn-s" style="font-size:12px;padding:2px 8px">Update Cost</button></td>'''
            rows += f"""<tr>
             <td><a href="/catalog/{a['id']}" style="color:var(--ac);font-weight:600">{a['name'][:30]}</a></td>
             <td class="mono" style="text-align:right">${a.get('cost',0):,.2f}</td>
             <td class="mono" style="text-align:right">${a.get('sell_price',0):,.2f}</td>
             <td class="mono" style="text-align:right">${a['web_price']:,.2f}</td>
             <td class="mono" style="text-align:right;color:{dc};font-weight:700">{sign}${a['delta']:,.2f} ({sign}{a['delta_pct']}%)</td>
             <td style="font-size:13px">{a.get('supplier_name','')}</td>
             <td style="font-size:13px"><a href="{a.get('supplier_url','')}" target="_blank" style="color:var(--ac)">{url_short}</a></td>
             {action}
            </tr>"""
        return rows

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
     <h2 style="margin:0;font-size:20px;font-weight:700">💰 Price Change Alerts</h2>
     <a href="/catalog" class="btn btn-s" style="font-size:14px">← Catalog</a>
    </div>

    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px">
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#f85149">{len(increased)}</div>
      <div style="font-size:14px;color:var(--tx2)">Price Increased</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#3fb950">{len(decreased)}</div>
      <div style="font-size:14px;color:var(--tx2)">Price Decreased</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#d29922">{len(competitive_risk)}</div>
      <div style="font-size:14px;color:var(--tx2)">Competitive Risk</div>
      <div style="font-size:13px;color:#d29922">Web price &lt; our sell price</div>
     </div>
    </div>

    {f'''<div class="card" style="margin-bottom:16px;padding:0;overflow-x:auto">
     <div style="padding:10px 12px;font-weight:600;font-size:13px;border-bottom:1px solid var(--bd);color:#d29922">⚠️ Competitive Risk — Web Price Below Our Sell Price ({len(competitive_risk)})</div>
     <table class="home-tbl"><thead><tr>
      <th>Product</th><th style="text-align:right">Our Cost</th><th style="text-align:right">Our Sell</th><th style="text-align:right">Web Price</th><th style="text-align:right">Delta</th><th>Supplier</th><th>URL</th><th></th>
     </tr></thead><tbody>{_alert_rows(competitive_risk)}</tbody></table>
    </div>''' if competitive_risk else ''}

    {f'''<div class="card" style="margin-bottom:16px;padding:0;overflow-x:auto">
     <div style="padding:10px 12px;font-weight:600;font-size:13px;border-bottom:1px solid var(--bd);color:#f85149">📈 Price Increased ({len(increased)})</div>
     <table class="home-tbl"><thead><tr>
      <th>Product</th><th style="text-align:right">Our Cost</th><th style="text-align:right">Our Sell</th><th style="text-align:right">Web Price</th><th style="text-align:right">Delta</th><th>Supplier</th><th>URL</th><th></th>
     </tr></thead><tbody>{_alert_rows(increased)}</tbody></table>
    </div>''' if increased else ''}

    {f'''<div class="card" style="margin-bottom:16px;padding:0;overflow-x:auto">
     <div style="padding:10px 12px;font-weight:600;font-size:13px;border-bottom:1px solid var(--bd);color:#3fb950">📉 Price Decreased — Opportunities ({len(decreased)})</div>
     <table class="home-tbl"><thead><tr>
      <th>Product</th><th style="text-align:right">Our Cost</th><th style="text-align:right">Our Sell</th><th style="text-align:right">Web Price</th><th style="text-align:right">Delta</th><th>Supplier</th><th>URL</th><th></th>
     </tr></thead><tbody>{_alert_rows(decreased)}</tbody></table>
    </div>''' if decreased else ''}

    {'<div class="card" style="padding:24px;text-align:center;color:var(--tx2)">No price alerts. Run "Check All Prices" from the catalog page first.</div>' if not increased and not decreased else ''}

    <script>
    function updateCostFromAlert(pid, newCost) {{
      if(!confirm('Update product cost to $'+newCost.toFixed(2)+'?')) return;
      fetch('/api/catalog/'+pid+'/update', {{
        method:'POST', headers:{{'Content-Type':'application/json'}},
        body: JSON.stringify({{cost: newCost}})
      }}).then(r=>r.json()).then(d=>{{
        if(d.ok) location.reload();
        else alert('Error: '+(d.error||'unknown'));
      }});
    }}
    </script>
    """
    return _wrap_page(content, "Price Change Alerts")


# ═══════════════════════════════════════════════════════════════════════
# Feature 3: Auto-enrich product from URL scrape
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/catalog/<int:pid>/enrich-from-url", methods=["POST"])
@auth_required
@safe_route
def api_catalog_enrich_from_url(pid):
    """Scrape supplier URL and fill missing catalog fields (title, description, MFG#, manufacturer)."""
    try:
        if not CATALOG_AVAILABLE:
            return jsonify({"ok": False, "error": "Catalog not available"})

        data = request.get_json(force=True, silent=True) or {}
        url = (data.get("url") or "").strip()
        if not url:
            return jsonify({"ok": False, "error": "No URL provided"})

        try:
            from src.agents.item_link_lookup import lookup_from_url
        except ImportError:
            return jsonify({"ok": False, "error": "Link lookup module not available"})

        result = lookup_from_url(url)
        if not result.get("ok"):
            return jsonify({"ok": False, "error": result.get("error", "Scrape failed")})

        # Update product catalog with enriched data (only fill gaps, don't overwrite)
        from src.agents.product_catalog import _get_conn as _cat_conn
        from datetime import datetime, timezone
        conn = _cat_conn()
        now = datetime.now(timezone.utc).isoformat()

        fields_updated = []
        updates = []
        params = []

        scraped_desc = (result.get("description") or result.get("title") or "").strip()
        scraped_mfg = (result.get("mfg_number") or result.get("part_number") or "").strip()
        scraped_manufacturer = (result.get("manufacturer") or "").strip()

        if scraped_desc:
            updates.append("description = CASE WHEN description IS NULL OR description = '' OR LENGTH(description) < 10 THEN ? ELSE description END")
            params.append(scraped_desc[:500])
            fields_updated.append("description")
        if scraped_mfg:
            updates.append("mfg_number = COALESCE(NULLIF(mfg_number, ''), ?)")
            params.append(scraped_mfg)
            fields_updated.append("mfg_number")
        if scraped_manufacturer:
            updates.append("manufacturer = COALESCE(NULLIF(manufacturer, ''), ?)")
            params.append(scraped_manufacturer)
            fields_updated.append("manufacturer")
        if result.get("price"):
            new_price = float(result["price"])
            updates.append("best_cost = CASE WHEN ? > 0 AND (best_cost IS NULL OR ? < best_cost) THEN ? ELSE best_cost END")
            params.extend([new_price, new_price, new_price])
            updates.append("best_supplier = CASE WHEN ? > 0 AND (best_cost IS NULL OR ? < best_cost) THEN ? ELSE best_supplier END")
            supplier_name = result.get("supplier", "Web")
            params.extend([new_price, new_price, supplier_name])

        if updates:
            updates.append("updated_at = ?")
            params.append(now)
            params.append(pid)
            sql = f"UPDATE product_catalog SET {', '.join(updates)} WHERE id = ?"
            conn.execute(sql, params)
            conn.commit()

        conn.close()

        return jsonify({
            "ok": True,
            "fields_updated": fields_updated,
            "scraped": {
                "title": result.get("title", ""),
                "description": scraped_desc[:100],
                "mfg_number": scraped_mfg,
                "manufacturer": scraped_manufacturer,
                "price": result.get("price"),
                "supplier": result.get("supplier", ""),
            }
        })
    except Exception as e:
        log.error("api_catalog_enrich_from_url error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════
# Feature 4: Scheduled Weekly Price Checks with Digest
# ═══════════════════════════════════════════════════════════════════════

_PRICE_CHECK_SCHEDULE = {"last_run": None, "interval_hours": 168}  # 168 = weekly


def _run_scheduled_price_check():
    """Background task: check all supplier URLs, send digest of changes."""
    import time as _time
    try:
        from src.agents.item_link_lookup import lookup_from_url, _is_login_required
        from src.agents.product_catalog import _get_conn as _cat_conn, record_catalog_quote
    except ImportError:
        log.error("Scheduled price check: missing dependencies")
        return

    conn = _cat_conn()
    suppliers = conn.execute(
        "SELECT id, product_id, supplier_name, supplier_url, last_price FROM product_suppliers "
        "WHERE supplier_url IS NOT NULL AND supplier_url != '' ORDER BY last_checked ASC NULLS FIRST"
    ).fetchall()
    conn.close()
    suppliers = [dict(s) for s in suppliers]

    changes = []
    checked = 0
    errors = 0

    for s in suppliers:
        url = s["supplier_url"]
        try:
            if _is_login_required(url):
                checked += 1
                continue
            result = lookup_from_url(url)
            if result.get("price"):
                new_price = float(result["price"])
                old_price = s.get("last_price") or 0
                add_supplier_price(s["product_id"], s["supplier_name"], new_price, url=url)
                if old_price > 0 and abs(new_price - old_price) > 0.01:
                    delta = new_price - old_price
                    delta_pct = round(delta / old_price * 100, 1)
                    changes.append({
                        "product_id": s["product_id"],
                        "supplier": s["supplier_name"],
                        "old": old_price, "new": new_price,
                        "delta": delta, "delta_pct": delta_pct,
                    })
                record_catalog_quote(s["product_id"], "web_check", new_price,
                                     source="scheduled_price_check", supplier_url=url)
            else:
                errors += 1
        except Exception:
            errors += 1
        checked += 1
        _time.sleep(1.5)

    _PRICE_CHECK_SCHEDULE["last_run"] = datetime.now(timezone.utc).isoformat()

    # Send digest via notify_agent
    if changes:
        try:
            from src.agents.notify_agent import send_alert
            increases = [c for c in changes if c["delta"] > 0]
            decreases = [c for c in changes if c["delta"] < 0]
            body = f"Checked {checked} supplier URLs.\n\n"
            if increases:
                body += f"📈 {len(increases)} PRICE INCREASES:\n"
                for c in increases[:10]:
                    body += f"  • {c['supplier']}: ${c['old']:.2f} → ${c['new']:.2f} (+{c['delta_pct']}%)\n"
            if decreases:
                body += f"\n📉 {len(decreases)} PRICE DECREASES:\n"
                for c in decreases[:10]:
                    body += f"  • {c['supplier']}: ${c['old']:.2f} → ${c['new']:.2f} ({c['delta_pct']}%)\n"
            body += f"\n{errors} errors. View details: /catalog/price-alerts"

            send_alert(
                event_type="price_check_digest",
                title=f"💰 Price Check: {len(changes)} changes found",
                body=body,
                urgency="info",
                channels=["email", "bell"],
                cooldown_key="price_check_digest_weekly",
            )
        except Exception as e:
            log.error("Price check digest notification error: %s", e)

    log.info("Scheduled price check: %d checked, %d changes, %d errors", checked, len(changes), errors)


def _price_check_scheduler():
    """Background thread that runs price checks on schedule."""
    import time as _time
    while True:
        try:
            interval = _PRICE_CHECK_SCHEDULE.get("interval_hours", 168) * 3600
            _time.sleep(interval)
            log.info("Starting scheduled price check...")
            _run_scheduled_price_check()
        except Exception as e:
            log.error("Price check scheduler error: %s", e)
            _time.sleep(3600)  # Wait 1h on error


@bp.route("/api/catalog/schedule-price-check", methods=["POST"])
@auth_required
@safe_route
def api_catalog_schedule_price_check():
    """Manually trigger a scheduled price check, or update the schedule interval."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        if data.get("run_now"):
            import threading
            t = threading.Thread(target=_run_scheduled_price_check, daemon=True, name="manual-price-check")
            t.start()
            return jsonify({"ok": True, "msg": "Price check started in background"})

        if data.get("interval_hours"):
            _PRICE_CHECK_SCHEDULE["interval_hours"] = int(data["interval_hours"])
            return jsonify({"ok": True, "interval_hours": _PRICE_CHECK_SCHEDULE["interval_hours"]})

        return jsonify({"ok": True, "schedule": _PRICE_CHECK_SCHEDULE})
    except Exception as e:
        log.error("api_catalog_schedule_price_check error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# Start the scheduler thread on app boot (skip in tests)
import threading as _sched_threading
if os.environ.get("TESTING") != "1":
    _sched_t = _sched_threading.Thread(target=_price_check_scheduler, daemon=True, name="price-check-scheduler")
    _sched_t.start()
    # Start polling on import (for gunicorn) and on direct run
    start_polling()
