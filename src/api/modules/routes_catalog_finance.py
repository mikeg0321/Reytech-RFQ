# routes_catalog_finance.py — Catalog, Shipping, Pricing, Margins, Payments, Audit
# Extracted from routes_intel.py for maintainability

# ═══════════════════════════════════════════════════════════════════════
# Product Catalog & Dynamic Pricing
# ═══════════════════════════════════════════════════════════════════════

# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from flask import redirect, flash
from src.core.paths import DATA_DIR
from src.core.db import get_db
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
    )
    CATALOG_AVAILABLE = True
except ImportError:
    CATALOG_AVAILABLE = False


@bp.route("/catalog")
@auth_required
def catalog_page():
    """Product catalog with search, pricing intelligence, margin analysis."""
    if not CATALOG_AVAILABLE:
        return _wrap_page("<div class='card'><p>Product catalog module not available.</p></div>", "Catalog")

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
        rows += f"""<tr onclick="location.href='/catalog/{p['id']}'" style="cursor:pointer">
         <td class="mono" style="font-weight:600;color:var(--ac)">{p.get('name','')[:25]}</td>
         <td style="font-size:14px;color:var(--tx2)">{desc_short}</td>
         <td class="mono">{p.get('sku','')}</td>
         <td style="font-size:14px">{p.get('category','')}</td>
         <td class="mono" style="text-align:right">${p.get('sell_price',0):,.2f}</td>
         <td class="mono" style="text-align:right">${p.get('cost',0):,.2f}</td>
         <td class="mono" style="text-align:right;color:{mc};font-weight:700">{margin:.1f}%</td>
         <td style="text-align:center">{strat_badge}</td>
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
      <button onclick="runCatalogFixes(this)" class="btn btn-s" style="font-size:14px;background:#21262d;color:#d2a8ff;border:1px solid #d2a8ff44">🔧 Run Fixes</button>
     </div>
    </div>

    <div class="bento bento-4" style="margin-bottom:16px">
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
    return _wrap_page(content, "Product Catalog")


@bp.route("/catalog/<int:pid>")
@auth_required
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
        sup_rows += f"""<tr>
         <td style="font-size:14px;font-weight:600">{s.get('supplier_name','')}</td>
         <td class="mono" style="text-align:right">${s.get('last_price',0) or 0:,.2f}</td>
         <td style="font-size:14px">{url_cell}</td>
         <td class="mono" style="font-size:14px">{(s.get('last_checked','') or '')[:10]}</td>
         <td style="text-align:center"><span style="color:{rel_color}">{rel_pct}%</span></td>
         <td style="text-align:center">{'✅' if s.get('in_stock') else '❌'}</td>
        </tr>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
     <div>
      <a href="/catalog" style="color:var(--tx2);text-decoration:none;font-size:14px">← Catalog</a>
      <h2 style="margin:4px 0 0;font-size:18px;font-weight:700">{p['name']}</h2>
      <div style="font-size:14px;color:var(--tx2);margin-top:2px">{(p.get('description','') or '')[:200]}</div>
     </div>
     <span style="padding:4px 12px;border-radius:12px;font-size:14px;font-weight:600;background:var(--sf)">{strat_map.get(p.get('price_strategy',''), p.get('price_strategy',''))}</span>
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
      </div>
     </div>
    </div>

    {f'''<div class="card" style="margin-bottom:16px;padding:0;overflow-x:auto">
     <div style="padding:10px 12px;font-weight:600;font-size:13px;border-bottom:1px solid var(--bd)">🏪 Suppliers & Source URLs</div>
     <table class="home-tbl"><thead><tr>
      <th>Supplier</th><th style="text-align:right">Price</th><th>URL</th><th>Last Checked</th><th>Reliability</th><th>Stock</th>
     </tr></thead><tbody>{sup_rows}</tbody></table>
    </div>''' if sup_rows else ''}

    {f'''<div class="card" style="margin-bottom:16px;padding:0;overflow-x:auto">
     <div style="padding:10px 12px;font-weight:600;font-size:13px;border-bottom:1px solid var(--bd)">📊 Quote & Price History</div>
     <table class="home-tbl"><thead><tr>
      <th>Date</th><th>Type</th><th style="text-align:right">Price</th><th style="text-align:center">Qty</th><th>Institution</th><th>PC#</th><th>Source</th><th>Link</th>
     </tr></thead><tbody>{ph_rows}</tbody></table>
    </div>''' if ph_rows else ''}

    <script>
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
def api_catalog_import():
    """Import QB products CSV."""
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


@bp.route("/api/catalog/reimport", methods=["POST"])
@auth_required
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
def api_catalog_dedup():
    """Find and merge duplicate products."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    try:
        init_catalog_db()
        dry = request.args.get("dry_run", "").lower() in ("1", "true", "yes")
        result = dedup_catalog(dry_run=dry)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/run-fixes", methods=["POST"])
@auth_required
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
def api_catalog_freshness_report():
    """Get freshness indicators for PC items."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    try:
        init_catalog_db()
        items = request.json.get("items", [])
        report = get_freshness_report(items)
        return jsonify({"ok": True, "items": report})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/lookup")
@auth_required
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
def api_products_search():
    """Full search with filters."""
    if not CATALOG_AVAILABLE:
        return jsonify([])
    q = request.args.get("q", "")
    cat = request.args.get("category", "")
    strat = request.args.get("strategy", "")
    limit = min(int(request.args.get("limit", 50)), 200)
    results = search_products(q, limit=limit, category=cat, strategy=strat)
    return jsonify(results)


@bp.route("/api/catalog/<int:pid>/pricing")
@auth_required
def api_catalog_pricing(pid):
    """Calculate recommended pricing for a product."""
    if not CATALOG_AVAILABLE:
        return jsonify({"error": "Catalog not available"})
    agency = request.args.get("agency", "")
    result = calculate_recommended_price(pid, target_margin=15.0, agency=agency)
    return jsonify(result)


@bp.route("/api/catalog/<int:pid>/update", methods=["POST"])
@auth_required
def api_catalog_update(pid):
    """Update product pricing/metadata."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    data = request.get_json() or {}
    ok = update_product_pricing(pid, **data)
    return jsonify({"ok": ok})


@bp.route("/api/catalog/opportunities")
@auth_required
def api_catalog_opportunities():
    """Bulk margin analysis — find pricing opportunities."""
    if not CATALOG_AVAILABLE:
        return jsonify([])
    results = bulk_margin_analysis()
    return jsonify(results[:50])


@bp.route("/api/catalog/match", methods=["POST"])
@auth_required
def api_catalog_match():
    """
    POST {description: "...", part_number: "..."}
    Returns best catalog matches for a line item.
    """
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


@bp.route("/api/catalog/match-batch", methods=["POST"])
@auth_required
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
def api_catalog_product_suppliers(pid):
    """GET all suppliers and prices for a product."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    suppliers = get_product_suppliers(pid)
    return jsonify({"ok": True, "suppliers": suppliers})


@bp.route("/api/catalog/<int:pid>/add-supplier", methods=["POST"])
@auth_required
def api_catalog_add_supplier(pid):
    """POST {supplier_name, price, url, sku, shipping, in_stock}"""
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


@bp.route("/api/catalog/rebuild-tokens", methods=["POST"])
@auth_required
def api_catalog_rebuild_tokens():
    """Rebuild search tokens for all products (migration utility)."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    count = rebuild_search_tokens()
    return jsonify({"ok": True, "updated": count})


# ── Catalog Match Audit ──────────────────────────────────────────────────────

@bp.route("/api/catalog/audit", methods=["GET", "POST"])
@auth_required
def api_catalog_audit():
    """Run DB-wide catalog match quality audit.
    GET: dry run (report only). POST {fix: true} to auto-clear bad matches."""
    if not CATALOG_AVAILABLE:
        return jsonify({"ok": False, "error": "Catalog not available"})
    try:
        data = request.get_json(silent=True) or {}
        fix = data.get("fix", False) if request.method == "POST" else False
        result = audit_catalog_matches(fix=fix)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/audit/db")
@auth_required
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
                track_url = f"https://www.amazon.com/progress-tracker/package/?itemId={tracking}"
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


def check_email_fingerprint(subject: str, sender: str, date_str: str = "",
                            message_id: str = "", inbox: str = "sales") -> bool:
    """Check if email was already processed by ANY inbox. Returns True if duplicate."""
    import hashlib
    raw = f"{subject.strip().lower()}|{sender.strip().lower()}|{date_str[:16]}"
    fp = hashlib.sha256(raw.encode()).hexdigest()[:32]
    
    try:
        from src.core.db import get_db
        with get_db() as conn:
            existing = conn.execute(
                "SELECT inbox, processed_at FROM email_fingerprints WHERE fingerprint=?", (fp,)
            ).fetchone()
            if existing:
                return True
            # Not a dupe — record it
            conn.execute(
                "INSERT OR IGNORE INTO email_fingerprints (fingerprint, inbox, subject, sender, message_id, processed_at) VALUES (?,?,?,?,?,?)",
                (fp, inbox, subject[:200], sender[:200], message_id[:200], datetime.now().isoformat())
            )
            conn.commit()
            return False
    except Exception:
        return False


def record_email_fingerprint(subject: str, sender: str, date_str: str = "",
                             inbox: str = "sales", result_type: str = "",
                             result_id: str = "", message_id: str = ""):
    """Record an email fingerprint after successful processing."""
    import hashlib
    raw = f"{subject.strip().lower()}|{sender.strip().lower()}|{date_str[:16]}"
    fp = hashlib.sha256(raw.encode()).hexdigest()[:32]
    
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO email_fingerprints 
                (fingerprint, inbox, subject, sender, message_id, processed_at, result_type, result_id)
                VALUES (?,?,?,?,?,?,?,?)
            """, (fp, inbox, subject[:200], sender[:200], message_id[:200],
                  datetime.now().isoformat(), result_type, result_id))
            conn.commit()
    except Exception as _e:
        log.debug("Suppressed: %s", _e)


# ═══════════════════════════════════════════════════════════════════════════════
# Recurring Order Detection (#12)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/orders/recurring")
@auth_required
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
def api_order_payment(oid):
    """Record payment received. POST: {amount, date, method, reference}"""
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


@bp.route("/api/orders/aging")
@auth_required
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
def api_pricing_intel():
    """Get pricing intelligence summary."""
    try:
        from src.knowledge.pricing_intel import get_pricing_intelligence_summary
        return jsonify({"ok": True, **get_pricing_intelligence_summary()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricing/recommend-price")
@auth_required
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
def api_pricing_trends():
    """Get price trends for a specific item.
    Query params: part_number, description, limit"""
    pn = request.args.get("part_number", "")
    desc = request.args.get("description", "")
    limit = int(request.args.get("limit", 50))
    
    try:
        from src.knowledge.pricing_intel import get_item_price_trends
        trends = get_item_price_trends(part_number=pn, description=desc, limit=limit)
        return jsonify({"ok": True, "trends": trends, "count": len(trends)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/pricing")
@auth_required
def pricing_intel_page():
    """Pricing Intelligence dashboard — historical win data + recommendations."""
    try:
        from src.knowledge.pricing_intel import get_pricing_intelligence_summary
        data = get_pricing_intelligence_summary()
    except Exception:
        data = {"total_records": 0, "unique_items": 0, "unique_agencies": 0,
                "total_revenue": 0, "avg_margin": 0, "recent_wins_30d": 0,
                "top_items": [], "top_agencies": [], "margin_distribution": {}}

    return render_page("pricing.html", active_page="Pricing", data=data)


# ═══════════════════════════════════════════════════════════════════════════════
# Recurring Orders UI (#12) — detect repeat buyers
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/recurring")
@auth_required
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
            rows = conn.execute(
                "SELECT * FROM audit_trail ORDER BY timestamp DESC LIMIT 200"
            ).fetchall()
            entries = [dict(r) for r in rows]
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Group by action type for stats
    action_counts = {}
    for e in entries:
        a = e.get("action", "unknown")
        action_counts[a] = action_counts.get(a, 0) + 1

    return render_page("audit.html", active_page="Home",
        entries=entries, action_counts=action_counts)


# Start polling on import (for gunicorn) and on direct run
start_polling()
