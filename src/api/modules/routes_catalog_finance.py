# routes_catalog_finance.py — Catalog, Shipping, Pricing, Margins, Payments, Audit
# Extracted from routes_intel.py for maintainability

# ═══════════════════════════════════════════════════════════════════════
# Product Catalog & Dynamic Pricing
# ═══════════════════════════════════════════════════════════════════════

try:
    from src.agents.product_catalog import (
        import_qb_csv, search_products, get_product, predictive_lookup,
        get_catalog_stats, calculate_recommended_price, update_product_pricing,
        record_won_price, bulk_margin_analysis, init_catalog_db,
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
    strategy_filter = request.args.get("strategy", "")

    products = []
    if q or cat_filter or strategy_filter:
        products = search_products(q, limit=50, category=cat_filter, strategy=strategy_filter)

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
         <td style="font-size:11px;color:var(--tx2)">{desc_short}</td>
         <td class="mono">{p.get('sku','')}</td>
         <td style="font-size:11px">{p.get('category','')}</td>
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
         <span style="font-size:12px">{o['name'][:35]}</span>
         <span class="mono" style="font-size:12px">${o['sell_price']:,.2f} @ {o['margin_pct']:.1f}%</span>
        </div>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
     <h2 style="margin:0;font-size:20px;font-weight:700">📦 Product Catalog</h2>
     <div style="display:flex;gap:8px;align-items:center">
      <span class="mono" style="font-size:12px;color:var(--tx2)">{tp} products</span>
      <button onclick="document.getElementById('import-csv').click()" class="btn btn-s" style="font-size:12px">📥 Import QB CSV</button>
      <input type="file" id="import-csv" accept=".csv" style="display:none" onchange="importCSV(this)">
     </div>
    </div>

    <div class="bento bento-4" style="margin-bottom:16px">
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--ac)">{tp}</div>
      <div style="font-size:11px;color:var(--tx2)">Products</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:{'#f85149' if am < 10 else '#d29922' if am < 15 else '#3fb950'}">{am}%</div>
      <div style="font-size:11px;color:var(--tx2)">Avg Margin</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#f85149">{neg + low}</div>
      <div style="font-size:11px;color:var(--tx2)">Need Pricing Review</div>
      <div style="font-size:10px;color:var(--tx2)">{neg} losing money</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:28px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#3fb950">${stats['total_sell_value']:,.0f}</div>
      <div style="font-size:11px;color:var(--tx2)">Catalog Value</div>
     </div>
    </div>

    <!-- Margin distribution bar -->
    <div class="card" style="margin-bottom:16px;padding:12px 16px">
     <div style="font-size:12px;font-weight:600;margin-bottom:8px">Margin Distribution</div>
     <div style="display:flex;gap:16px;align-items:center;font-size:11px;margin-bottom:6px">
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
      <div style="font-weight:600;font-size:13px;margin-bottom:8px;color:#f85149">⚠️ Losing Money ({neg} items)</div>
      {neg_alerts if neg_alerts else '<div style="font-size:12px;color:var(--tx2)">No negative margin items ✅</div>'}
     </div>
     <div class="card" style="padding:12px">
      <div style="font-weight:600;font-size:13px;margin-bottom:8px;color:#d29922">💡 Margin Opportunities</div>
      {opp_rows if opp_rows else '<div style="font-size:12px;color:var(--tx2)">Connect SCPRS pricing to find opportunities</div>'}
     </div>
    </div>

    <!-- Search -->
    <div class="card" style="padding:12px;margin-bottom:12px">
     <form method="GET" action="/catalog" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
      <input type="text" name="q" value="{q}" placeholder="Search products, SKU, description..." 
             style="flex:1;min-width:200px;padding:6px 10px;border:1px solid var(--bd);border-radius:6px;background:var(--sf);color:var(--tx);font-size:13px"
             id="catalog-search" autocomplete="off">
      <select name="category" style="padding:6px;border:1px solid var(--bd);border-radius:6px;background:var(--sf);color:var(--tx);font-size:12px">
       <option value="">All Categories</option>
       {cat_options}
      </select>
      <select name="strategy" style="padding:6px;border:1px solid var(--bd);border-radius:6px;background:var(--sf);color:var(--tx);font-size:12px">
       <option value="">All Strategies</option>
       <option value="loss_leader" {"selected" if strategy_filter=="loss_leader" else ""}>🔴 Loss Leader</option>
       <option value="margin_protect" {"selected" if strategy_filter=="margin_protect" else ""}>🟡 Margin Protect</option>
       <option value="competitive" {"selected" if strategy_filter=="competitive" else ""}>🟢 Competitive</option>
       <option value="premium" {"selected" if strategy_filter=="premium" else ""}>🔵 Premium</option>
      </select>
      <button type="submit" class="btn btn-s" style="font-size:12px">🔍 Search</button>
      {'<a href="/catalog" class="btn" style="font-size:12px">Clear</a>' if (q or cat_filter or strategy_filter) else ''}
     </form>
    </div>

    <!-- Predictive search dropdown -->
    <div id="search-results-dropdown" style="display:none;position:absolute;z-index:100;background:var(--bg2);border:1px solid var(--bd);border-radius:8px;max-height:300px;overflow-y:auto;width:400px;box-shadow:0 4px 12px rgba(0,0,0,0.3)"></div>

    {f'''<div class="card" style="padding:0;overflow-x:auto">
     <table class="home-tbl" style="min-width:700px">
      <thead><tr>
       <th style="width:150px">Name</th><th>Description</th><th style="width:80px">SKU</th>
       <th style="width:100px">Category</th>
       <th style="width:80px;text-align:right">Price</th><th style="width:80px;text-align:right">Cost</th>
       <th style="width:70px;text-align:right">Margin</th><th style="width:30px"></th>
      </tr></thead>
      <tbody>{rows}</tbody>
     </table>
    </div>''' if products else '<div class="card" style="padding:24px;text-align:center;color:var(--tx2)">Search above to browse products, or <a href="/catalog?strategy=loss_leader" style="color:#f85149">view items losing money</a></div>' if not q else '<div class="card" style="padding:24px;text-align:center;color:var(--tx2)">No products match your search</div>'}

    <script>
    function importCSV(input) {{
      const file = input.files[0]; if (!file) return;
      const fd = new FormData(); fd.append('file', file);
      fetch('/api/catalog/import', {{method:'POST', body:fd}})
        .then(r=>r.json()).then(d=>{{
          if(d.ok) {{ alert('Imported: '+d.imported+' Updated: '+d.updated); location.reload(); }}
          else alert('Error: '+(d.error||'unknown'));
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
                `<a href="/catalog/${{p.id}}" style="display:flex;justify-content:space-between;padding:8px 12px;text-decoration:none;color:var(--tx);border-bottom:1px solid var(--bd);font-size:12px">
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

    # Price history rows
    ph_rows = ""
    for h in p.get("price_history", [])[:20]:
        ph_rows += f"""<tr>
         <td class="mono" style="font-size:11px">{h.get('recorded_at','')[:10]}</td>
         <td style="font-size:12px">{h.get('price_type','')}</td>
         <td class="mono" style="text-align:right">${h.get('price',0):,.2f}</td>
         <td style="font-size:11px;color:var(--tx2)">{h.get('source','')}</td>
        </tr>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
     <div>
      <a href="/catalog" style="color:var(--tx2);text-decoration:none;font-size:12px">← Catalog</a>
      <h2 style="margin:4px 0 0;font-size:18px;font-weight:700">{p['name']}</h2>
     </div>
     <span style="padding:4px 12px;border-radius:12px;font-size:12px;font-weight:600;background:var(--sf)">{strat_map.get(p.get('price_strategy',''), p.get('price_strategy',''))}</span>
    </div>

    <div class="bento bento-4" style="margin-bottom:16px">
     <div class="card" style="text-align:center">
      <div style="font-size:24px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--ac)">${p['sell_price']:,.2f}</div>
      <div style="font-size:11px;color:var(--tx2)">Sell Price</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:24px;font-weight:800;font-family:'JetBrains Mono',monospace">${p['cost']:,.2f}</div>
      <div style="font-size:11px;color:var(--tx2)">Cost</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:24px;font-weight:800;font-family:'JetBrains Mono',monospace;color:{margin_color}">{p['margin_pct']:.1f}%</div>
      <div style="font-size:11px;color:var(--tx2)">Margin</div>
     </div>
     <div class="card" style="text-align:center">
      <div style="font-size:24px;font-weight:800;font-family:'JetBrains Mono',monospace">${p['sell_price'] - p['cost']:,.2f}</div>
      <div style="font-size:11px;color:var(--tx2)">Margin $</div>
     </div>
    </div>

    <div class="bento bento-2" style="margin-bottom:16px">
     <div class="card" style="padding:12px">
      <div class="card-t">Product Details</div>
      <div style="display:grid;grid-template-columns:100px 1fr;gap:4px;font-size:12px">
       <span style="color:var(--tx2)">SKU</span><span class="mono">{p.get('sku','—')}</span>
       <span style="color:var(--tx2)">Category</span><span>{p.get('category','—')}</span>
       <span style="color:var(--tx2)">Item Type</span><span>{p.get('item_type','')}</span>
       <span style="color:var(--tx2)">Taxable</span><span>{'Yes' if p.get('taxable') else 'No'}</span>
       <span style="color:var(--tx2)">Times Quoted</span><span class="mono">{p.get('times_quoted',0)}</span>
       <span style="color:var(--tx2)">Times Won</span><span class="mono">{p.get('times_won',0)}</span>
       <span style="color:var(--tx2)">Last Sold</span><span class="mono">${p.get('last_sold_price',0) or 0:,.2f} ({(p.get('last_sold_date') or '—')[:10]})</span>
       <span style="color:var(--tx2)">Tags</span><span>{p.get('tags','')}</span>
      </div>
      <div style="margin-top:8px;font-size:12px;color:var(--tx2);white-space:pre-wrap">{(p.get('description','') or '')[:300]}</div>
     </div>

     <div class="card" style="padding:12px">
      <div class="card-t">💰 Pricing Intelligence</div>
      <div style="display:grid;grid-template-columns:120px 1fr;gap:4px;font-size:12px">
       <span style="color:var(--tx2)">SCPRS Price</span><span class="mono">${p.get('scprs_last_price',0) or 0:,.2f} <span style="font-size:10px;color:var(--tx2)">{p.get('scprs_agency','')}</span></span>
       <span style="color:var(--tx2)">Competitor Low</span><span class="mono">${p.get('competitor_low_price',0) or 0:,.2f} <span style="font-size:10px;color:var(--tx2)">{p.get('competitor_source','')}</span></span>
       <span style="color:var(--tx2)">Web Lowest</span><span class="mono">${p.get('web_lowest_price',0) or 0:,.2f} <span style="font-size:10px;color:var(--tx2)">{p.get('web_lowest_source','')}</span></span>
       <span style="color:var(--tx2)">Recommended</span><span class="mono" style="color:#3fb950;font-weight:700">${p.get('recommended_price',0) or 0:,.2f}</span>
      </div>
      <div style="margin-top:12px;display:flex;gap:6px;flex-wrap:wrap">
       <button onclick="runPricingAnalysis({pid})" class="btn btn-s" style="font-size:11px">🧮 Run Pricing Analysis</button>
       <button onclick="updatePrice({pid})" class="btn btn-s" style="font-size:11px">✏️ Update Pricing</button>
      </div>
     </div>
    </div>

    {f'''<div class="card" style="margin-bottom:16px;padding:0;overflow-x:auto">
     <div style="padding:10px 12px;font-weight:600;font-size:13px;border-bottom:1px solid var(--bd)">📊 Price History</div>
     <table class="home-tbl"><thead><tr>
      <th>Date</th><th>Type</th><th style="text-align:right">Price</th><th>Source</th>
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
        except Exception:
            pass
        if _cat_count == 0:
            csv_path = os.path.join(DATA_DIR, "product_catalog_import.csv")
            if os.path.exists(csv_path):
                _result = import_qb_csv(csv_path)
                log.info("🏗️ Auto-imported product catalog: %d products from QB CSV", _result.get("imported", 0))
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
    delivered = sum(1 for s in shipments if s["status"] == "delivered")
    carriers = {}
    for s in shipments:
        c = s["carrier"] or "Unknown"
        carriers[c] = carriers.get(c, 0) + 1
    
    rows = ""
    for s in shipments:
        st_color = {"delivered": "var(--gn)", "shipped": "var(--ac)", "ordered": "var(--yl)"}.get(s["status"], "var(--tx2)")
        link = f'<a href="{s["track_url"]}" target="_blank" style="color:var(--ac)">{s["tracking"]}</a>' if s["track_url"] else s["tracking"]
        rows += f"""<tr onclick="location.href='/order/{s['order_id']}'" style="cursor:pointer">
         <td><a href="/order/{s['order_id']}" style="color:var(--ac);font-size:12px">{s['order_id'][:20]}</a></td>
         <td style="font-size:12px">{s['po_number']}</td>
         <td style="font-size:12px">{s['institution'][:30]}</td>
         <td style="font-size:12px">{s['description']}</td>
         <td style="font-family:'JetBrains Mono',monospace;font-size:11px">{link}</td>
         <td style="font-size:12px">{s['carrier']}</td>
         <td style="font-size:12px">{s['ship_date']}</td>
         <td style="font-size:12px">{s['delivery_date']}</td>
         <td style="color:{st_color};font-weight:600;font-size:12px">{s['status']}</td>
        </tr>"""
    
    carrier_chips = " ".join(
        f'<span style="background:var(--sf2);border:1px solid var(--bd);border-radius:6px;padding:4px 10px;font-size:11px">{c}: <b>{n}</b></span>'
        for c, n in sorted(carriers.items(), key=lambda x: -x[1])
    )
    
    content = f"""
    <h2 style="margin-bottom:4px">🚚 Shipping Dashboard</h2>
    <p style="font-size:13px;color:var(--tx2);margin-bottom:16px">All tracking numbers across orders with carrier links</p>
    
    <div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <div class="card" style="text-align:center;padding:12px 20px;min-width:100px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--ac)">{total}</div>
        <div style="font-size:10px;color:var(--tx2)">SHIPMENTS</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:100px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--yl)">{in_transit}</div>
        <div style="font-size:10px;color:var(--tx2)">IN TRANSIT</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:100px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--gn)">{delivered}</div>
        <div style="font-size:10px;color:var(--tx2)">DELIVERED</div></div>
    </div>
    
    <div style="margin-bottom:12px">{carrier_chips}</div>
    
    <div class="card" style="overflow-x:auto">
     <table class="home-tbl" style="min-width:900px">
      <thead><tr>
       <th>Order</th><th>PO #</th><th>Institution</th><th>Item</th>
       <th>Tracking</th><th>Carrier</th><th>Shipped</th><th>Delivered</th><th>Status</th>
      </tr></thead>
      <tbody>{rows or '<tr><td colspan="9" style="text-align:center;color:var(--tx2);padding:20px">No shipments tracked yet — tracking numbers auto-detected from shipping emails</td></tr>'}</tbody>
     </table>
    </div>"""
    
    return render(content, title="Shipping")


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
    except Exception:
        pass


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

    # Stats cards
    stats = f"""
    <div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--ac)">{data.get('total_records',0)}</div>
        <div style="font-size:10px;color:var(--tx2)">WINNING PRICES</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--gn)">{data.get('unique_items',0)}</div>
        <div style="font-size:10px;color:var(--tx2)">UNIQUE ITEMS</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--yl)">{data.get('unique_agencies',0)}</div>
        <div style="font-size:10px;color:var(--tx2)">AGENCIES</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--gn)">${data.get('total_revenue',0):,.0f}</div>
        <div style="font-size:10px;color:var(--tx2)">WIN REVENUE</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--ac)">{data.get('avg_margin',0):.1f}%</div>
        <div style="font-size:10px;color:var(--tx2)">AVG MARGIN</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--ac)">{data.get('recent_wins_30d',0)}</div>
        <div style="font-size:10px;color:var(--tx2)">LAST 30 DAYS</div></div>
    </div>"""

    # Margin distribution bar
    md = data.get("margin_distribution", {})
    md_total = sum(md.values()) or 1
    md_bar = f"""
    <div class="card" style="margin-bottom:16px">
      <h3 style="margin-bottom:8px;font-size:14px">Margin Distribution</h3>
      <div style="display:flex;height:24px;border-radius:6px;overflow:hidden;font-size:10px;font-weight:600">
        <div style="background:#e74c3c;width:{md.get('negative',0)/md_total*100:.1f}%;display:flex;align-items:center;justify-content:center;color:#fff" title="Negative margin">{md.get('negative',0)}</div>
        <div style="background:#f39c12;width:{md.get('low',0)/md_total*100:.1f}%;display:flex;align-items:center;justify-content:center;color:#fff" title="0-10% margin">{md.get('low',0)}</div>
        <div style="background:#27ae60;width:{md.get('mid',0)/md_total*100:.1f}%;display:flex;align-items:center;justify-content:center;color:#fff" title="10-25% margin">{md.get('mid',0)}</div>
        <div style="background:#2980b9;width:{md.get('high',0)/md_total*100:.1f}%;display:flex;align-items:center;justify-content:center;color:#fff" title="25%+ margin">{md.get('high',0)}</div>
      </div>
      <div style="display:flex;gap:16px;margin-top:6px;font-size:10px;color:var(--tx2)">
        <span>🔴 Negative: {md.get('negative',0)}</span>
        <span>🟡 Low (0-10%): {md.get('low',0)}</span>
        <span>🟢 Mid (10-25%): {md.get('mid',0)}</span>
        <span>🔵 High (25%+): {md.get('high',0)}</span>
      </div>
    </div>"""

    # Price lookup tool
    lookup = """
    <div class="card" style="margin-bottom:16px">
      <h3 style="margin-bottom:8px;font-size:14px">🔍 Price Lookup</h3>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <input id="pl-desc" placeholder="Description or part number" style="flex:1;min-width:200px;padding:8px;border-radius:6px;border:1px solid var(--bd);background:var(--sf2);color:var(--tx)">
        <input id="pl-agency" placeholder="Agency (optional)" style="width:160px;padding:8px;border-radius:6px;border:1px solid var(--bd);background:var(--sf2);color:var(--tx)">
        <button onclick="priceLookup()" style="padding:8px 16px;background:var(--ac);color:#fff;border:none;border-radius:6px;cursor:pointer;font-weight:600">Lookup</button>
      </div>
      <div id="pl-result" style="margin-top:12px"></div>
    </div>
    <script>
    function priceLookup(){
      var desc=document.getElementById('pl-desc').value;
      var agency=document.getElementById('pl-agency').value;
      if(!desc){alert('Enter description or part number');return}
      var url='/api/pricing/recommend-price?description='+encodeURIComponent(desc);
      if(agency) url+='&agency='+encodeURIComponent(agency);
      fetch(url,{credentials:'same-origin'}).then(r=>r.json()).then(d=>{
        var el=document.getElementById('pl-result');
        if(!d.ok||d.count===0){el.innerHTML='<div style="color:var(--tx2);padding:8px">No pricing history found for this item</div>';return}
        var h='<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:8px">';
        h+='<div style="background:var(--sf2);border-radius:6px;padding:8px 14px"><div style="font-size:10px;color:var(--tx2)">RECOMMENDED</div><div style="font-size:20px;font-weight:800;color:var(--gn)">$'+d.recommended_price.toFixed(2)+'</div></div>';
        h+='<div style="background:var(--sf2);border-radius:6px;padding:8px 14px"><div style="font-size:10px;color:var(--tx2)">AVG WIN</div><div style="font-size:20px;font-weight:700">$'+d.avg_price.toFixed(2)+'</div></div>';
        h+='<div style="background:var(--sf2);border-radius:6px;padding:8px 14px"><div style="font-size:10px;color:var(--tx2)">RANGE</div><div style="font-size:14px;font-weight:600">$'+d.min_price.toFixed(2)+' – $'+d.max_price.toFixed(2)+'</div></div>';
        h+='<div style="background:var(--sf2);border-radius:6px;padding:8px 14px"><div style="font-size:10px;color:var(--tx2)">WINS</div><div style="font-size:20px;font-weight:700">'+d.count+'</div></div>';
        h+='</div>';
        if(d.history&&d.history.length){
          h+='<table class="home-tbl" style="font-size:11px"><thead><tr><th>Date</th><th>Price</th><th>Cost</th><th>Margin</th><th>Agency</th><th>Quote</th></tr></thead><tbody>';
          d.history.slice(0,10).forEach(function(r){
            h+='<tr><td>'+r.date+'</td><td style="font-weight:600">$'+r.price.toFixed(2)+'</td><td>'+(r.cost?'$'+r.cost.toFixed(2):'-')+'</td><td>'+(r.margin?r.margin.toFixed(1)+'%':'-')+'</td><td>'+r.agency+'</td><td>'+(r.quote||'-')+'</td></tr>';
          });
          h+='</tbody></table>';
        }
        el.innerHTML=h;
      });
    }
    </script>"""

    # Top winning items table
    top_rows = ""
    for it in data.get("top_items", []):
        top_rows += f"""<tr>
         <td style="font-size:12px">{it['description'][:50]}</td>
         <td style="font-family:monospace;font-size:11px">{it['part_number'] or '-'}</td>
         <td style="font-weight:600">{it['wins']}</td>
         <td style="font-weight:600">${it['avg_price']:,.2f}</td>
         <td>{it['avg_margin']:.1f}%</td>
         <td style="font-size:11px">{it['last_won'][:10] if it['last_won'] else '-'}</td>
        </tr>"""

    top_items_html = f"""
    <div class="card" style="margin-bottom:16px;overflow-x:auto">
      <h3 style="margin-bottom:8px;font-size:14px">📊 Top Winning Items</h3>
      <table class="home-tbl">
       <thead><tr><th>Description</th><th>Part #</th><th>Wins</th><th>Avg Price</th><th>Avg Margin</th><th>Last Won</th></tr></thead>
       <tbody>{top_rows or '<tr><td colspan="6" style="text-align:center;color:var(--tx2);padding:16px">Pricing data builds as quotes are won and orders created</td></tr>'}</tbody>
      </table>
    </div>"""

    # Top agencies table
    agency_rows = ""
    for ag in data.get("top_agencies", []):
        agency_rows += f"""<tr>
         <td style="font-size:12px;font-weight:600">{ag['agency']}</td>
         <td>{ag['wins']}</td>
         <td style="font-weight:600">${ag['total_revenue']:,.2f}</td>
         <td>{ag['avg_margin']:.1f}%</td>
        </tr>"""

    agencies_html = f"""
    <div class="card" style="overflow-x:auto">
      <h3 style="margin-bottom:8px;font-size:14px">🏛️ Top Agencies by Revenue</h3>
      <table class="home-tbl">
       <thead><tr><th>Agency</th><th>Wins</th><th>Revenue</th><th>Avg Margin</th></tr></thead>
       <tbody>{agency_rows or '<tr><td colspan="4" style="text-align:center;color:var(--tx2);padding:16px">Agency data populates from won orders</td></tr>'}</tbody>
      </table>
    </div>"""

    content = f"""
    <h2 style="margin-bottom:4px">💰 Pricing Intelligence</h2>
    <p style="font-size:13px;color:var(--tx2);margin-bottom:16px">Historical winning prices captured from orders — use for smarter quoting</p>
    {stats}
    {md_bar}
    {lookup}
    {top_items_html}
    {agencies_html}
    """
    return render(content, title="Pricing Intel")


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

    rows = ""
    for r in recurring:
        order_links = " ".join(
            f'<a href="/order/{o.get("order_id","")}" style="color:var(--ac);font-size:10px">{o.get("po_number","") or o.get("order_id","")[:12]}</a>'
            for o in r["orders"][:5]
        )
        rows += f"""<tr>
         <td style="font-weight:600;font-size:12px">{r['institution']}</td>
         <td style="font-weight:700;color:var(--ac)">{r['order_count']}</td>
         <td style="font-weight:600">${r['total_value']:,.2f}</td>
         <td>${r['avg_value']:,.2f}</td>
         <td>{r['unique_items']}</td>
         <td>{order_links}</td>
        </tr>"""

    content = f"""
    <h2 style="margin-bottom:4px">🔄 Recurring Orders</h2>
    <p style="font-size:13px;color:var(--tx2);margin-bottom:16px">Repeat buyers detected — use for quote template reuse and proactive outreach</p>
    <div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--ac)">{total_recurring}</div>
        <div style="font-size:10px;color:var(--tx2)">REPEAT BUYERS</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--gn)">{total_orders_recurring}</div>
        <div style="font-size:10px;color:var(--tx2)">TOTAL ORDERS</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--gn)">${total_value_recurring:,.0f}</div>
        <div style="font-size:10px;color:var(--tx2)">TOTAL VALUE</div></div>
    </div>
    <div class="card" style="overflow-x:auto">
     <table class="home-tbl">
      <thead><tr><th>Institution</th><th>Orders</th><th>Total Value</th><th>Avg Value</th><th>Unique Items</th><th>Recent Orders</th></tr></thead>
      <tbody>{rows or '<tr><td colspan="6" style="text-align:center;color:var(--tx2);padding:20px">Need 2+ orders from same institution to detect patterns</td></tr>'}</tbody>
     </table>
    </div>"""
    return render(content, title="Recurring Orders")


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

    rows = ""
    for m in margin_data[:50]:
        mc = "var(--rn)" if m["margin"] < 0 else ("var(--yl)" if m["margin"] < 10 else "var(--gn)")
        rows += f"""<tr onclick="location.href='/order/{m['order_id']}'" style="cursor:pointer">
         <td style="font-size:11px"><a href="/order/{m['order_id']}" style="color:var(--ac)">{m['po'] or m['order_id'][:15]}</a></td>
         <td style="font-size:11px">{m['institution'][:25]}</td>
         <td style="font-size:11px">{m['description']}</td>
         <td style="text-align:right">{m['qty']}</td>
         <td style="text-align:right">${m['sell']:,.2f}</td>
         <td style="text-align:right">{f"${m['cost']:,.2f}" if m['cost'] else '<span style="color:var(--tx2)">—</span>'}</td>
         <td style="text-align:right;font-weight:600;color:{mc}">{m['margin']:.1f}%</td>
         <td style="text-align:right;font-weight:600">${m['profit']:,.2f}</td>
        </tr>"""

    content = f"""
    <h2 style="margin-bottom:4px">📊 Margin Calculator</h2>
    <p style="font-size:13px;color:var(--tx2);margin-bottom:16px">Cost vs sell price per item — profitability across all orders</p>
    <div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--gn)">${total_revenue:,.0f}</div>
        <div style="font-size:10px;color:var(--tx2)">REVENUE</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--yl)">${total_cost:,.0f}</div>
        <div style="font-size:10px;color:var(--tx2)">COSTS</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--gn)">${total_profit:,.0f}</div>
        <div style="font-size:10px;color:var(--tx2)">PROFIT</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:{'var(--gn)' if overall_margin > 15 else 'var(--yl)'}">{overall_margin:.1f}%</div>
        <div style="font-size:10px;color:var(--tx2)">OVERALL MARGIN</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--ac)">{costed}</div>
        <div style="font-size:10px;color:var(--tx2)">COSTED ITEMS</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:{'var(--rn)' if negative > 0 else 'var(--gn)'}">{negative}</div>
        <div style="font-size:10px;color:var(--tx2)">NEGATIVE MARGIN</div></div>
    </div>
    <div class="card" style="overflow-x:auto">
     <table class="home-tbl" style="min-width:800px">
      <thead><tr><th>Order</th><th>Institution</th><th>Item</th><th>Qty</th><th>Sell</th><th>Cost</th><th>Margin</th><th>Profit</th></tr></thead>
      <tbody>{rows or '<tr><td colspan="8" style="text-align:center;color:var(--tx2);padding:20px">Margin data populates when orders have cost and sell prices</td></tr>'}</tbody>
     </table>
    </div>
    <p style="font-size:11px;color:var(--tx2);margin-top:8px">{uncosted} items missing cost data — use Supplier Lookup to auto-populate costs</p>
    """
    return render(content, title="Margins")


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

    # Aging bar
    aging_total = max(total_outstanding, 1)
    aging_bar = '<div style="display:flex;height:24px;border-radius:6px;overflow:hidden;font-size:10px;font-weight:600;margin-bottom:8px">'
    aging_colors = {"Current": "#27ae60", "31-45 Days": "#f39c12", "46-60 Days": "#e67e22", "61-90 Days": "#e74c3c", "90+ Days": "#c0392b"}
    for b, color in aging_colors.items():
        val = buckets.get(b, 0)
        pct = val / aging_total * 100 if aging_total > 0 and val > 0 else 0
        if pct > 0:
            aging_bar += f'<div style="background:{color};width:{pct:.1f}%;display:flex;align-items:center;justify-content:center;color:#fff" title="{b}: ${val:,.0f}">{b}</div>'
    aging_bar += '</div>'

    rows = ""
    for inv in invoices:
        bc = "var(--gn)" if inv["bucket"] == "Paid" else ("var(--rn)" if "90" in inv["bucket"] or "61" in inv["bucket"] else "var(--yl)")
        pay_btn = f"""<button onclick="recordPayment('{inv['oid']}')" class="btn btn-s" style="font-size:10px;padding:2px 8px">💳 Pay</button>""" if inv["balance"] > 0 else '<span style="color:var(--gn);font-size:11px">✅ Paid</span>'
        rows += f"""<tr>
         <td><a href="/order/{inv['oid']}" style="color:var(--ac);font-size:11px">{inv['inv_num']}</a></td>
         <td style="font-size:11px">{inv['institution'][:30]}</td>
         <td style="font-size:11px">{inv['po']}</td>
         <td style="font-size:11px">{inv['inv_date']}</td>
         <td style="text-align:right">{inv['days']}d</td>
         <td style="text-align:right;font-weight:600">${inv['total']:,.2f}</td>
         <td style="text-align:right;color:var(--gn)">${inv['paid']:,.2f}</td>
         <td style="text-align:right;font-weight:700;color:{bc}">${inv['balance']:,.2f}</td>
         <td style="color:{bc};font-size:11px">{inv['bucket']}</td>
         <td>{pay_btn}</td>
        </tr>"""

    content = f"""
    <h2 style="margin-bottom:4px">💳 Payments & Aging</h2>
    <p style="font-size:13px;color:var(--tx2);margin-bottom:16px">Invoice aging report — track payments, outstanding balances</p>
    <div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--rn)">${total_outstanding:,.0f}</div>
        <div style="font-size:10px;color:var(--tx2)">OUTSTANDING</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--gn)">${total_paid_amt:,.0f}</div>
        <div style="font-size:10px;color:var(--tx2)">COLLECTED</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--ac)">{len(invoices)}</div>
        <div style="font-size:10px;color:var(--tx2)">INVOICES</div></div>
    </div>
    <div class="card" style="margin-bottom:16px">
      <h3 style="margin-bottom:8px;font-size:14px">Aging Distribution</h3>
      {aging_bar}
      <div style="display:flex;gap:12px;font-size:10px;color:var(--tx2);flex-wrap:wrap">
        {''.join(f'<span style="color:{c}">● {b}: ${buckets.get(b,0):,.0f}</span>' for b, c in aging_colors.items())}
      </div>
    </div>
    <div class="card" style="overflow-x:auto">
     <table class="home-tbl" style="min-width:900px">
      <thead><tr><th>Invoice</th><th>Institution</th><th>PO</th><th>Date</th><th>Age</th><th>Total</th><th>Paid</th><th>Balance</th><th>Bucket</th><th>Action</th></tr></thead>
      <tbody>{rows or '<tr><td colspan="10" style="text-align:center;color:var(--tx2);padding:20px">No invoices generated yet — invoices auto-create when all items delivered</td></tr>'}</tbody>
     </table>
    </div>
    <script>
    function recordPayment(oid) {{
      var amt = prompt('Payment amount received:');
      if (!amt) return;
      var method = prompt('Payment method (check/ach/wire/card):', 'check');
      var ref = prompt('Reference/check number:', '');
      fetch('/api/order/' + oid + '/payment', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        credentials: 'same-origin',
        body: JSON.stringify({{amount: parseFloat(amt), method: method||'check', reference: ref||''}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{
          alert('Payment recorded. Total paid: $' + d.total_paid.toFixed(2) + ' — Status: ' + d.payment_status);
          location.reload();
        }} else {{
          alert('Error: ' + (d.error||'unknown'));
        }}
      }});
    }}
    </script>
    """
    return render(content, title="Payments")


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
    except Exception:
        pass

    # Group by action type for stats
    action_counts = {}
    for e in entries:
        a = e.get("action", "unknown")
        action_counts[a] = action_counts.get(a, 0) + 1

    action_chips = " ".join(
        f'<span style="background:var(--sf2);border:1px solid var(--bd);border-radius:6px;padding:4px 10px;font-size:11px">{a}: <b>{n}</b></span>'
        for a, n in sorted(action_counts.items(), key=lambda x: -x[1])[:12]
    )

    rows_html = ""
    action_colors = {
        "order_create": "var(--gn)", "order_linked": "var(--ac)", "order_deleted": "var(--rn)",
        "payment_received": "var(--gn)", "quote_sent": "var(--ac)", "email_sent": "var(--ac)",
        "login": "var(--yl)", "rate_limited": "var(--rn)", "csrf_failed": "var(--rn)",
    }
    for e in entries[:100]:
        ts = e.get("timestamp", "")[:19].replace("T", " ")
        action = e.get("action", "")
        color = action_colors.get(action, "var(--tx2)")
        ip = e.get("ip_address", "")
        details = (e.get("details", "") or "")[:80]
        rows_html += f"""<tr>
         <td style="font-size:11px;font-family:monospace;color:var(--tx2)">{ts}</td>
         <td style="font-weight:600;color:{color};font-size:12px">{action}</td>
         <td style="font-size:11px">{details}</td>
         <td style="font-size:10px;color:var(--tx2);font-family:monospace">{ip}</td>
        </tr>"""

    content = f"""
    <h2 style="margin-bottom:4px">📋 Audit Trail</h2>
    <p style="font-size:13px;color:var(--tx2);margin-bottom:16px">Every admin action logged with timestamp and IP — last 200 entries</p>
    <div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--ac)">{len(entries)}</div>
        <div style="font-size:10px;color:var(--tx2)">TOTAL EVENTS</div></div>
      <div class="card" style="text-align:center;padding:12px 20px;min-width:110px;margin:0">
        <div style="font-size:28px;font-weight:800;color:var(--yl)">{len(action_counts)}</div>
        <div style="font-size:10px;color:var(--tx2)">ACTION TYPES</div></div>
    </div>
    <div style="margin-bottom:12px;display:flex;flex-wrap:wrap;gap:6px">{action_chips}</div>
    <div class="card" style="overflow-x:auto">
     <table class="home-tbl" style="min-width:700px">
      <thead><tr><th>Timestamp</th><th>Action</th><th>Details</th><th>IP</th></tr></thead>
      <tbody>{rows_html or '<tr><td colspan="4" style="text-align:center;color:var(--tx2);padding:20px">Audit trail populates as actions are performed</td></tr>'}</tbody>
     </table>
    </div>
    <p style="font-size:11px;color:var(--tx2);margin-top:8px">API: <a href="/api/audit" style="color:var(--ac)">/api/audit</a> — JSON feed of last 100 audit events</p>
    """
    return render(content, title="Audit Trail")


# Start polling on import (for gunicorn) and on direct run
start_polling()
