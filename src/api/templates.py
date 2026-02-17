"""
Reytech RFQ Dashboard — HTML Templates
Extracted from dashboard.py for maintainability.
"""

BASE_CSS = """
:root{--bg:#0f1117;--sf:#1a1d27;--sf2:#242836;--bd:#2e3345;--tx:#e4e6ed;--tx2:#8b90a0;
--ac:#4f8cff;--ac2:#3b6fd4;--gn:#34d399;--yl:#fbbf24;--rd:#f87171;--or:#fb923c;--r:10px}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--tx);min-height:100vh}
a{color:var(--ac);text-decoration:none}
.hdr{background:var(--sf);border-bottom:1px solid var(--bd);padding:14px 28px;display:flex;justify-content:space-between;align-items:center}
.hdr h1{font-size:19px;font-weight:700;letter-spacing:-0.5px}.hdr h1 span{color:var(--ac)}
.hdr-right{display:flex;align-items:center;gap:16px;font-size:12px;font-family:'JetBrains Mono',monospace;color:var(--tx2)}
.poll-dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:4px}
.poll-on{background:var(--gn);box-shadow:0 0 6px var(--gn)}.poll-off{background:var(--rd)}
.poll-wait{background:var(--yl)}
.ctr{max-width:1200px;margin:0 auto;padding:20px}
.card{background:var(--sf);border:1px solid var(--bd);border-radius:var(--r);padding:20px;margin-bottom:16px}
.card-t{font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:14px}
.upl{border:2px dashed var(--bd);border-radius:var(--r);padding:36px;text-align:center;cursor:pointer;transition:.2s}
.upl:hover{border-color:var(--ac);background:rgba(79,140,255,.05)}
.upl h3{font-size:16px;margin-bottom:4px}.upl p{color:var(--tx2);font-size:13px}
.rfq-i{background:var(--sf2);border:1px solid var(--bd);border-radius:var(--r);padding:14px 18px;display:grid;grid-template-columns:auto 1fr auto auto;gap:14px;align-items:center;text-decoration:none;color:var(--tx);transition:.15s;margin-bottom:8px}
.rfq-i:hover{border-color:var(--ac);transform:translateY(-1px)}
.sol{font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:600;color:var(--ac)}
.det{font-size:12px;color:var(--tx2)}.det b{color:var(--tx)}
.badge{padding:3px 9px;border-radius:16px;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.5px}
.b-new{background:rgba(251,191,36,.15);color:var(--yl)}.b-pending{background:rgba(251,191,36,.15);color:var(--yl)}
.b-ready{background:rgba(52,211,153,.15);color:var(--gn)}.b-generated{background:rgba(79,140,255,.15);color:var(--ac)}
.b-sent{background:rgba(52,211,153,.2);color:var(--gn)}
.meta-g{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-bottom:20px}
.meta-i{background:var(--sf2);border-radius:8px;padding:10px 12px}
.meta-l{font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px}
.meta-v{font-size:13px;font-weight:500;margin-top:3px}
table.it{width:100%;border-collapse:collapse;font-size:12px}
table.it th{text-align:left;padding:8px;font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--bd)}
table.it td{padding:8px;border-bottom:1px solid var(--bd);vertical-align:middle}
table.it input[type=number]{background:var(--sf2);border:1px solid var(--bd);color:var(--tx);padding:5px 8px;border-radius:6px;width:88px;font-family:'JetBrains Mono',monospace;font-size:12px}
table.it input:focus{outline:none;border-color:var(--ac)}
.mono{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--tx2)}
.btn{display:inline-flex;align-items:center;gap:6px;padding:8px 16px;border-radius:7px;font-size:13px;font-weight:600;cursor:pointer;border:none;transition:.15s;text-decoration:none}
.btn-p{background:var(--ac);color:#fff}.btn-p:hover{background:var(--ac2)}
.btn-s{background:var(--sf2);color:var(--tx);border:1px solid var(--bd)}.btn-s:hover{border-color:var(--ac)}
.btn-g{background:var(--gn);color:#0f1117}.btn-g:hover{opacity:.9}
.btn-o{background:var(--or);color:#0f1117}.btn-o:hover{opacity:.9}
.btn-sm{padding:5px 10px;font-size:11px;border-radius:5px}
.bg{display:flex;gap:8px;margin-top:16px;flex-wrap:wrap}
.alert{padding:10px 14px;border-radius:8px;font-size:12px;margin-bottom:12px}
.al-s{background:rgba(52,211,153,.1);border:1px solid rgba(52,211,153,.3);color:var(--gn)}
.al-e{background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.3);color:var(--rd)}
.al-i{background:rgba(79,140,255,.1);border:1px solid rgba(79,140,255,.3);color:var(--ac)}
.markup-bar{display:flex;gap:6px;align-items:center;margin-bottom:12px;flex-wrap:wrap}
.markup-bar span{font-size:11px;color:var(--tx2);margin-right:4px}
.g-good{color:var(--gn)}.g-low{color:var(--yl)}.g-bad{color:var(--rd)}
.empty{text-align:center;padding:48px 20px;color:var(--tx2)}
.draft-box{background:var(--sf2);border:1px solid var(--bd);border-radius:8px;padding:16px;margin-top:16px;font-size:13px;white-space:pre-wrap;line-height:1.6}
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:1000;overflow-y:auto;padding:20px;justify-content:center;align-items:flex-start}
.scprs-tag{font-size:9px;padding:2px 5px;border-radius:3px;margin-left:4px;font-weight:600}
.scprs-hi{background:rgba(52,211,153,.15);color:var(--gn)}
.scprs-med{background:rgba(251,191,36,.15);color:var(--yl)}
.qh-link{transition:all .15s;position:relative}
.qh-link:hover{color:#79c0ff !important;text-decoration:underline !important}
.qh-row{transition:background .15s}
.qh-row:hover{background:rgba(56,139,253,.06);border-radius:6px;margin:0 -6px;padding-left:6px !important;padding-right:6px !important}
.qh-row a:hover{color:#79c0ff !important;text-decoration:underline !important}
[title]{cursor:pointer}
#historyCard [title]:hover{filter:brightness(1.15)}
"""

PAGE_HOME = """
<div class="card">
 <div class="card-t">New RFQ / Price Check</div>
 <form method="POST" action="/upload" enctype="multipart/form-data" id="uf">
  <div class="upl" id="dz" onclick="document.getElementById('fi').click()">
   <h3>Drop PDF attachments here</h3>
   <p>Upload RFQ (703B, 704B, Bid Package) or AMS 704 Price Check</p>
   <input type="file" id="fi" data-testid="upload-file-input" name="files" multiple accept=".pdf" style="display:none">
  </div>
 </form>
</div>
<!-- Search + Quick Actions -->
<div class="card" style="padding:12px 16px">
 <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
  <form method="get" action="/quotes" style="display:flex;gap:6px;flex:1;min-width:200px">
   <input name="q" placeholder="Search quotes, institutions, RFQ #..." style="flex:1;padding:8px 12px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);font-size:14px">
   <button type="submit" class="btn btn-p" style="padding:8px 16px">🔍 Search</button>
  </form>
  <a href="/quotes" class="btn btn-sm" style="background:var(--sf2);color:var(--tx);border:1px solid var(--bd);font-size:12px;padding:5px 12px">📋 Quotes DB</a>
 </div>
</div>
<div class="card">
 <div class="card-t">RFQ Queue ({{rfqs|length}})</div>
 {% for id, r in rfqs|dictsort(reverse=true) %}
 <a href="/rfq/{{id}}" class="rfq-i" style="{% if r.status in ('sent','generated') %}opacity:0.55{% endif %}">
  <div class="sol">#{{r.solicitation_number}}</div>
  <div class="det"><b>{{r.requestor_name}}</b> · Due {{r.due_date}}{% if r.source == 'email' %} · 📧{% endif %}</div>
  <div class="mono">{{r.line_items|length}} items</div>
  <span class="badge b-{{r.status}}">{{r.status}}</span>
 </a>
 {% else %}
 <div class="empty">No RFQs yet — upload files above or configure email polling</div>
 {% endfor %}
</div>
{% if price_checks %}
<div class="card">
 <div class="card-t">Price Checks ({{price_checks|length}})</div>
 {% for id, pc in price_checks|dictsort(reverse=true) %}
 <a href="/pricecheck/{{id}}" class="rfq-i" style="{% if pc.status in ('completed','converted') %}opacity:0.55{% endif %}">
  <div class="sol">#{{pc.pc_number}}</div>
  <div class="det"><b>{{pc.institution}}</b> · Due {{pc.due_date}}{% if pc.requestor %} · {{pc.requestor}}{% endif %}{% if pc.reytech_quote_number %} · <span style="color:#3fb950">{{pc.reytech_quote_number}}</span>{% endif %}</div>
  <div class="mono">{{pc.get('items',[])|length}} items</div>
  <span class="badge b-{{pc.status}}">{{pc.status}}</span>
 </a>
 {% endfor %}
</div>
{% endif %}
<script>
const dz=document.getElementById('dz'),fi=document.getElementById('fi'),f=document.getElementById('uf');
['dragover','dragenter'].forEach(e=>dz.addEventListener(e,ev=>{ev.preventDefault();dz.style.borderColor='var(--ac)'}));
['dragleave','drop'].forEach(e=>dz.addEventListener(e,ev=>{ev.preventDefault();dz.style.borderColor='var(--bd)'}));
dz.addEventListener('drop',ev=>{fi.files=ev.dataTransfer.files;f.submit()});
fi.addEventListener('change',()=>{if(fi.files.length)f.submit()});
</script>
"""

PAGE_DETAIL = """
<a href="/" class="btn btn-s" style="margin-bottom:16px">← Queue</a>
<form method="POST" action="/rfq/{{rid}}/delete" style="display:inline;margin-left:8px;margin-bottom:16px">
 <button type="submit" class="btn btn-sm" style="background:var(--rd);color:#fff;padding:4px 10px;font-size:11px" onclick="return confirm('Delete this RFQ?')">Delete</button>
</form>

<!-- Preview Modal -->
<div class="modal-overlay" id="previewModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:1000;overflow-y:auto;padding:20px;justify-content:center;align-items:flex-start">
 <div style="background:#fff;color:#1a1a1a;border-radius:10px;max-width:850px;width:100%;margin:20px auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">
  <div style="display:flex;justify-content:space-between;align-items:center;padding:14px 20px;border-bottom:2px solid #1a1a1a;background:#f5f5f0;border-radius:10px 10px 0 0">
   <h2 style="margin:0;font-size:16px;color:#1a1a1a">📋 Quote Preview — <span id="rfqPreviewType">704B Quote Worksheet</span></h2>
   <div>
    <button class="btn btn-sm" style="background:var(--gn);color:#fff;margin-right:8px;font-size:12px" onclick="window.print()">🖨️ Print</button>
    <button style="background:none;border:none;font-size:24px;cursor:pointer;color:#666" onclick="document.getElementById('previewModal').style.display='none'">×</button>
   </div>
  </div>
  <div id="rfqPreviewBody" style="padding:0"></div>
 </div>
</div>

<div class="card">
 <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px">
  <div>
   <div style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:700">#{{r.solicitation_number}}</div>
   <div style="color:var(--tx2);font-size:12px;margin-top:2px">{{r.get('award_method','all_or_none')|replace('_',' ')|title}}{% if r.source=='email' %} · 📧 Auto-imported{% endif %}</div>
  </div>
  <span class="badge b-{{r.status}}">{{r.status}}</span>
 </div>
 <div class="meta-g">
  <div class="meta-i"><div class="meta-l">Requestor</div><div class="meta-v">{{r.requestor_name}}</div></div>
  <div class="meta-i"><div class="meta-l">Email</div><div class="meta-v">{{r.requestor_email}}</div></div>
  <div class="meta-i"><div class="meta-l">Due Date</div><div class="meta-v">{{r.due_date}}</div></div>
  <div class="meta-i"><div class="meta-l">Delivery</div><div class="meta-v" style="font-size:11px">{{r.get('delivery_location','N/A')[:55]}}</div></div>
 </div>
</div>

<div class="card">
 <div class="card-t">Line Items & Pricing</div>
 
 <!-- Markup Buttons -->
 <div class="markup-bar">
  <span>Quick Markup:</span>
  <button class="btn btn-sm btn-s" onclick="applyMarkup(0.10)">+10%</button>
  <button class="btn btn-sm btn-s" onclick="applyMarkup(0.15)">+15%</button>
  <button class="btn btn-sm btn-s" onclick="applyMarkup(0.20)">+20%</button>
  <button class="btn btn-sm btn-s" onclick="applyMarkup(0.25)">+25%</button>
  <button class="btn btn-sm btn-s" onclick="applyMarkup(0.30)">+30%</button>
  <span style="margin-left:8px">SCPRS Undercut:</span>
  <button class="btn btn-sm btn-s" onclick="applyScprsUndercut(0.01)">-1%</button>
  <button class="btn btn-sm btn-s" onclick="applyScprsUndercut(0.02)">-2%</button>
  <button class="btn btn-sm btn-s" onclick="applyScprsUndercut(0.05)">-5%</button>
  <button class="btn btn-sm btn-p" data-testid="rfq-scprs-lookup" onclick="lookupScprs()" style="margin-left:8px">🔍 SCPRS Lookup</button>
  <button class="btn btn-sm btn-o" data-testid="rfq-amazon-lookup" onclick="researchPrices()" style="margin-left:4px">🔬 Amazon Lookup</button>
 </div>

 <form method="POST" action="/rfq/{{rid}}/update" id="pf">
 <table class="it">
  <thead><tr>
   <th>#</th><th>Qty</th><th style="min-width:180px">Description</th><th>Part #</th>
   <th>Your Cost</th><th>SCPRS</th><th>Amazon</th><th>Bid Price</th><th>Subtotal</th><th>Margin</th><th>Profit</th>
  </tr></thead>
  <tbody>
  {% for i in r.line_items %}
  <tr>
   <td>{{i.line_number}}</td>
   <td style="white-space:nowrap">{{i.qty}} {{i.uom}}</td>
   <td style="max-width:220px;font-size:12px"><input type="text" name="desc_{{loop.index0}}" value="{{i.description.split('\n')[0]}}" class="text-in" style="width:100%;font-size:12px" title="{{i.description}}"></td>
   <td class="mono" style="font-size:11px">{{i.item_number}}</td>
   <td><input type="number" step="0.01" name="cost_{{loop.index0}}" value="{{i.supplier_cost or ''}}" placeholder="0.00" class="num-in" style="width:80px;font-size:14px;font-weight:600" oninput="recalc()"></td>
   <td style="font-size:13px;font-weight:600">
    {% if i.scprs_last_price %}${{'{:.2f}'.format(i.scprs_last_price)}}{% else %}—{% endif %}
    {% if i.scprs_source %}<span class="scprs-tag scprs-{{'hi' if i.scprs_confidence=='high' else 'med'}}" title="{{i.scprs_vendor|default('')}}">{{i.scprs_source|replace('_',' ')}}</span>{% endif %}
   </td>
   <td style="font-size:13px;font-weight:600">
    {% if i.get('amazon_price') %}${{'{:.2f}'.format(i.amazon_price)}}{% elif i.get('supplier_cost') %}${{'{:.2f}'.format(i.supplier_cost)}}{% else %}—{% endif %}
   </td>
   <td><input type="number" step="0.01" name="price_{{loop.index0}}" value="{{i.price_per_unit or ''}}" placeholder="0.00" class="num-in" style="width:80px;font-size:14px;font-weight:600" oninput="recalc()"></td>
   <td class="mono" style="font-size:14px;font-weight:600" id="sub_{{loop.index0}}">—</td>
   <td id="mg_{{loop.index0}}" style="font-weight:700;font-size:13px">—</td>
   <td id="pf_{{loop.index0}}" style="font-weight:600;font-size:13px">—</td>
  </tr>
  {% endfor %}
  </tbody>
 </table>

 <div style="display:flex;justify-content:space-between;align-items:center;margin-top:14px;padding-top:14px;border-top:1px solid var(--bd)">
  <div>
   <span style="color:var(--tx2);font-size:13px">Revenue: </span><span id="tot" style="font-family:'JetBrains Mono',monospace;font-size:20px;font-weight:700">$0</span>
  </div>
  <div id="pft" style="font-family:'JetBrains Mono',monospace;font-size:15px;font-weight:600">—</div>
 </div>

 <div class="bg">
  <button type="submit" class="btn btn-p" data-testid="rfq-save-pricing">💾 Save Pricing</button>
  <button type="button" class="btn" data-testid="rfq-preview-quote" style="background:var(--sf2);color:var(--tx);border:1px solid var(--bd)" onclick="showRfqPreview()">👁️ Preview Quote</button>
  <button type="submit" formaction="/rfq/{{rid}}/generate" data-testid="rfq-generate-state-forms" class="btn btn-g">📄 Generate State Forms (704B + Package)</button>
  <a href="/rfq/{{rid}}/generate-quote" class="btn" data-testid="rfq-generate-reytech-quote" style="background:#1a3a5c;color:#fff">📋 Generate Reytech Quote</a>
 </div>
 </form>
</div>

<!-- Template Status — shows what forms are available for generation -->
<div class="card">
 <div class="card-t">📋 Form Templates</div>
 <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px">
  {% set t = r.get('templates', {}) %}
  <div style="padding:8px 14px;border-radius:6px;font-size:13px;border:1px solid {{ 'rgba(52,211,153,.3)' if t.get('703b') else 'rgba(248,113,113,.3)' }};background:{{ 'rgba(52,211,153,.06)' if t.get('703b') else 'rgba(248,113,113,.06)' }}">
   {{ '✅' if t.get('703b') else '❌' }} 703B (RFQ)
  </div>
  <div style="padding:8px 14px;border-radius:6px;font-size:13px;border:1px solid {{ 'rgba(52,211,153,.3)' if t.get('704b') else 'rgba(248,113,113,.3)' }};background:{{ 'rgba(52,211,153,.06)' if t.get('704b') else 'rgba(248,113,113,.06)' }}">
   {{ '✅' if t.get('704b') else '❌' }} 704B (Quote Worksheet)
  </div>
  <div style="padding:8px 14px;border-radius:6px;font-size:13px;border:1px solid {{ 'rgba(52,211,153,.3)' if t.get('bidpkg') else 'rgba(251,191,36,.3)' }};background:{{ 'rgba(52,211,153,.06)' if t.get('bidpkg') else 'rgba(251,191,36,.06)' }}">
   {{ '✅' if t.get('bidpkg') else '⚠️' }} Bid Package {{ '(optional)' if not t.get('bidpkg') else '' }}
  </div>
 </div>
 {% if not t.get('704b') %}
 <div style="background:rgba(248,113,113,.08);border:1px solid rgba(248,113,113,.2);border-radius:6px;padding:12px 16px;margin-bottom:12px;font-size:13px">
  ⚠️ <b>704B template missing</b> — Upload the blank 704B form from the RFQ package to generate a filled bid. 
  {% if r.get('source') == 'price_check' %}This RFQ was converted from a Price Check and needs the original RFQ forms.{% endif %}
 </div>
 <form method="POST" action="/rfq/{{rid}}/upload-templates" enctype="multipart/form-data">
  <div style="display:flex;gap:8px;align-items:center">
   <input type="file" name="templates" data-testid="rfq-upload-templates" multiple accept=".pdf" style="font-size:13px">
   <button type="submit" class="btn btn-sm btn-p" data-testid="rfq-upload-templates-btn">📎 Upload Templates</button>
  </div>
  <div style="font-size:11px;color:var(--tx2);margin-top:4px">Upload the 703B, 704B, and/or Bid Package PDFs from the RFQ email</div>
 </form>
 {% endif %}
</div>

<!-- SCPRS Results Panel (populated by JS after lookup) -->
<div class="card" id="scprs-panel" style="display:none">
 <div style="display:flex;justify-content:space-between;align-items:center">
  <div class="card-t" style="margin-bottom:0">🔍 SCPRS Search Results</div>
  <div style="display:flex;gap:8px;align-items:center">
   <span id="scprs-ts" style="font-size:10px;color:var(--tx2);font-family:'JetBrains Mono',monospace"></span>
   <button class="btn btn-sm btn-s" onclick="document.getElementById('scprs-panel').style.display='none'" style="padding:2px 8px;font-size:10px">✕ Hide</button>
  </div>
 </div>
 <div id="scprs-body" style="margin-top:12px"></div>
</div>

{% if r.status in ('generated','sent') and r.output_files %}
<div class="card">
 <div class="card-t">📦 Bid Package</div>
 <div class="bg">
  {% for f in r.output_files %}<a href="/dl/{{rid}}/{{f}}" class="btn btn-s">📄 {{f}}</a>{% endfor %}
 </div>
</div>

<div class="card">
 <div class="card-t">📧 Draft Response Email</div>
 {% if r.draft_email %}
 <div style="font-size:12px;color:var(--tx2);margin-bottom:8px">
  <b>To:</b> {{r.draft_email.to}} · <b>Subject:</b> {{r.draft_email.subject}}
 </div>
 <div class="draft-box">{{r.draft_email.body}}</div>
 <div class="bg">
  <form method="POST" action="/rfq/{{rid}}/send" style="display:inline">
   <button type="submit" class="btn btn-o">📤 Send Email</button>
  </form>
  <a href="mailto:{{r.draft_email.to}}?subject={{r.draft_email.subject|urlencode}}&body={{r.draft_email.body|urlencode}}" class="btn btn-s">📋 Open in Mail App</a>
 </div>
 {% endif %}
</div>
{% endif %}

<script>
const items={{r.line_items|tojson}};
const n=items.length;

function recalc(){
 let tb=0,tc=0;
 for(let i=0;i<n;i++){
  const q=items[i].qty||0;
  const c=parseFloat(document.querySelector(`[name=cost_${i}]`).value)||0;
  const p=parseFloat(document.querySelector(`[name=price_${i}]`).value)||0;
  const s=p*q; tb+=s; tc+=c*q;
  document.getElementById(`sub_${i}`).textContent=s?`$${s.toFixed(2)}`:'—';
  const m=p>0&&c>0?((p-c)/p*100):null;
  const el=document.getElementById(`mg_${i}`);
  if(m!==null){el.textContent=m.toFixed(1)+'%';el.style.color=m>=20?'#3fb950':m>=10?'#d29922':'#f85149'}
  else{el.textContent='—';el.style.color='#8b949e'}
  // Per-item profit
  const pf=document.getElementById(`pf_${i}`);
  if(pf){
   const ip=(p-c)*q;
   if(p>0&&c>0){pf.textContent=`$${ip.toFixed(2)}`;pf.style.color=ip>0?'#3fb950':'#f85149'}
   else{pf.textContent='—';pf.style.color='#8b949e'}
  }
 }
 document.getElementById('tot').textContent=`$${tb.toFixed(2)}`;
 const pr=tb-tc;
 const pe=document.getElementById('pft');
 if(tb>0&&tc>0){const pp=(pr/tb*100).toFixed(1);pe.textContent=`💰 Profit: $${pr.toFixed(2)} (${pp}%)`;pe.style.color=pr>0?'#3fb950':'#f85149'}
 else{pe.textContent='—';pe.style.color='#8b949e'}
}

function applyMarkup(pct){
 for(let i=0;i<n;i++){
  const c=parseFloat(document.querySelector(`[name=cost_${i}]`).value)||0;
  if(c>0){document.querySelector(`[name=price_${i}]`).value=(c*(1+pct)).toFixed(2)}
 }
 recalc();
}

function applyScprsUndercut(pct){
 for(let i=0;i<n;i++){
  const s=parseFloat(document.querySelector(`[name=scprs_${i}]`).value)||0;
  if(s>0){document.querySelector(`[name=price_${i}]`).value=(s*(1-pct)).toFixed(2)}
 }
 recalc();
}

function lookupScprs(){
 const btn=event.target;btn.disabled=true;btn.textContent='⏳ Searching FI$Cal...';
 fetch('/api/scprs/{{rid}}').then(r=>r.json()).then(d=>{
  let found=0,total=0;
  if(d.results){
   total=d.results.length;
   d.results.forEach((r,i)=>{
    if(r.price){document.querySelector(`[name=scprs_${i}]`).value=r.price.toFixed(2);found++}
   });
   recalc();
  }
  btn.disabled=false;
  if(found>0){
   btn.textContent=`✅ ${found}/${total} prices found`;
   setTimeout(()=>{btn.textContent='🔍 SCPRS Lookup'},4000);
  } else {
   btn.textContent=`⚠️ 0/${total} found`;
   setTimeout(()=>{btn.textContent='🔍 SCPRS Lookup'},3000);
  }
  // Populate persistent results panel
  showScprsResults(d);
 }).catch(e=>{btn.disabled=false;btn.textContent='❌ Lookup failed';console.error(e)});
}

function researchPrices(){
 const btn=event.target;btn.disabled=true;btn.textContent='⏳ Searching Amazon...';
 fetch('/api/research/rfq/{{rid}}').then(r=>r.json()).then(d=>{
  if(!d.ok){btn.textContent='❌ '+d.message;btn.disabled=false;return;}
  // Poll for results
  const poll=setInterval(()=>{
   fetch('/api/research/status').then(r=>r.json()).then(s=>{
    btn.textContent=`⏳ ${s.items_done}/${s.items_total} items (${s.prices_found} found)`;
    if(!s.running){
     clearInterval(poll);
     btn.disabled=false;
     if(s.prices_found>0){
      btn.textContent=`✅ ${s.prices_found} prices found — reloading...`;
      setTimeout(()=>location.reload(),1000);
     } else {
      btn.textContent='⚠️ 0 prices found';
      setTimeout(()=>{btn.textContent='🔬 Amazon Lookup'},3000);
     }
    }
   });
  },3000);
 }).catch(e=>{btn.disabled=false;btn.textContent='❌ Research failed';console.error(e)});
}

function showScprsResults(d){
 const panel=document.getElementById('scprs-panel');
 const body=document.getElementById('scprs-body');
 const ts=document.getElementById('scprs-ts');
 if(!d.results||!d.results.length){panel.style.display='none';return;}
 panel.style.display='block';
 ts.textContent=new Date().toLocaleTimeString();
 let html='<table class="it"><thead><tr><th>#</th><th>Status</th><th>Price</th><th>Source</th><th>Vendor</th><th>PO#</th><th>Date</th><th>Searched</th></tr></thead><tbody>';
 d.results.forEach((r,i)=>{
  const price=r.price?`$${r.price.toFixed(2)}`:'<span style="color:var(--rd)">Not found</span>';
  const status=r.price?'<span style="color:var(--gn)">✅</span>':'<span style="color:var(--rd)">❌</span>';
  const src=r.source||r.note||r.error||'—';
  const vendor=r.vendor||'—';
  const po=r.po_number||'—';
  const dt=r.date||'—';
  const searched=(r.searched||[]).join(', ')||'—';
  html+=`<tr><td>${i+1}</td><td>${status}</td><td style="font-family:'JetBrains Mono',monospace;font-weight:600">${price}</td><td><span class="scprs-tag scprs-${r.confidence=='high'?'hi':'med'}">${src.replace(/_/g,' ')}</span></td><td style="font-size:11px">${vendor}</td><td class="mono">${po}</td><td class="mono">${dt}</td><td style="font-size:10px;color:var(--tx2);max-width:200px">${searched}</td></tr>`;
 });
 html+='</tbody></table>';
 if(d.errors&&d.errors.length){
  html+='<div style="margin-top:8px;font-size:11px;color:var(--rd)">Errors: '+d.errors.join(', ')+'</div>';
 }
 body.innerHTML=html;
}

function showRfqPreview(){
 let rowsHtml='';
 let total=0;
 for(let i=0;i<n;i++){
  const q=items[i].qty||0;
  const desc=items[i].description||'';
  const itemNo=items[i].line_number||items[i].item_number||(i+1);
  const uom=items[i].uom||'ea';
  const c=parseFloat(document.querySelector(`[name=cost_${i}]`).value)||0;
  const p=parseFloat(document.querySelector(`[name=price_${i}]`).value)||0;
  const ext=p*q; total+=ext;
  rowsHtml+=`<tr>
   <td style="text-align:center;border:1px solid #000;padding:4px">${itemNo}</td>
   <td style="text-align:center;border:1px solid #000;padding:4px">${q} ${uom}</td>
   <td style="font-size:12px;border:1px solid #000;padding:4px">${desc}</td>
   <td style="text-align:right;border:1px solid #000;padding:4px">$${c.toFixed(2)}</td>
   <td style="text-align:right;font-weight:600;border:1px solid #000;padding:4px">$${p.toFixed(2)}</td>
   <td style="text-align:right;border:1px solid #000;padding:4px">$${ext.toFixed(2)}</td>
  </tr>`;
 }
 const sol='{{r.solicitation_number}}';
 const dept='{{r.get("department","")}}';
 const reqName='{{r.requestor_name}}';
 const due='{{r.due_date}}';
 const delivery='{{r.get("delivery_location","")}}';
 const shipTo='{{r.get("ship_to","")}}';

 const html=`<div style="font-family:'Times New Roman',Times,serif;font-size:13px;color:#000;line-height:1.4;padding:20px">
  <div style="display:flex;justify-content:space-between;font-size:10px;text-transform:uppercase;letter-spacing:1px;color:#444">
   <span>STATE OF CALIFORNIA</span><span>${dept||'CALIFORNIA CORRECTIONAL HEALTH CARE SERVICES'}</span>
  </div>
  <div style="text-align:center;padding:8px;border-bottom:2px solid #000">
   <h3 style="margin:4px 0;font-size:14px;text-transform:uppercase;letter-spacing:1px">ACQUISITION QUOTE WORKSHEET — 704B</h3>
   <div style="font-size:12px">Solicitation #${sol}</div>
  </div>

  <table style="width:100%;border-collapse:collapse;margin-top:10px">
   <tr>
    <td style="border:1px solid #000;padding:4px 8px;width:50%"><span style="font-size:9px;color:#555;text-transform:uppercase;display:block">Requestor</span>${reqName}</td>
    <td style="border:1px solid #000;padding:4px 8px"><span style="font-size:9px;color:#555;text-transform:uppercase;display:block">Due Date</span><b>${due}</b></td>
   </tr>
   <tr>
    <td style="border:1px solid #000;padding:4px 8px" colspan="2"><span style="font-size:9px;color:#555;text-transform:uppercase;display:block">Delivery Location</span>${delivery||shipTo||'—'}</td>
   </tr>
  </table>

  <div style="background:#e8e8e0;text-align:center;font-weight:700;font-size:12px;padding:4px;margin-top:10px;border:1px solid #000;letter-spacing:2px">SUPPLIER: REYTECH INC.</div>
  <table style="width:100%;border-collapse:collapse;margin-top:0">
   <tr>
    <td style="border:1px solid #000;padding:3px 8px;font-size:11px"><b>30 Carnoustie Way, Trabuco Canyon, CA 92679</b></td>
    <td style="border:1px solid #000;padding:3px 8px;font-size:11px">949-229-1575</td>
    <td style="border:1px solid #000;padding:3px 8px;font-size:11px">sales@reytechinc.com</td>
    <td style="border:1px solid #000;padding:3px 8px;font-size:11px">SB/MB: 2002605</td>
   </tr>
  </table>

  <table style="width:100%;border-collapse:collapse;margin-top:12px">
   <thead><tr style="background:#e8e8e0">
    <th style="border:1px solid #000;padding:4px 6px;font-size:10px;text-transform:uppercase;width:50px">#</th>
    <th style="border:1px solid #000;padding:4px 6px;font-size:10px;text-transform:uppercase;width:60px">Qty</th>
    <th style="border:1px solid #000;padding:4px 6px;font-size:10px;text-transform:uppercase">Description</th>
    <th style="border:1px solid #000;padding:4px 6px;font-size:10px;text-transform:uppercase;width:80px;text-align:right">Your Cost</th>
    <th style="border:1px solid #000;padding:4px 6px;font-size:10px;text-transform:uppercase;width:80px;text-align:right">Bid Price</th>
    <th style="border:1px solid #000;padding:4px 6px;font-size:10px;text-transform:uppercase;width:90px;text-align:right">Extension</th>
   </tr></thead>
   <tbody>${rowsHtml}</tbody>
  </table>

  <div style="text-align:right;margin-top:8px;font-size:15px;font-weight:700;padding:8px;border:2px solid #000;display:inline-block;float:right">
   TOTAL: $${total.toFixed(2)}
  </div>
  <div style="clear:both"></div>

  <div style="margin-top:12px;font-size:10px;text-align:center;color:#555;border-top:1px solid #999;padding-top:6px">
   Reytech Inc. · Michael Guadan · SB/MB #2002605 · DVBE #2002605
  </div>
 </div>`;

 document.getElementById('rfqPreviewBody').innerHTML=html;
 document.getElementById('rfqPreviewType').textContent='704B — '+(dept||sol);
 const modal=document.getElementById('previewModal');
 modal.style.display='flex';
 modal.onclick=function(e){if(e.target===modal) modal.style.display='none';};
}
document.addEventListener('keydown',function(e){if(e.key==='Escape'){const m=document.getElementById('previewModal');if(m)m.style.display='none';}});

recalc();
</script>
"""


def build_pc_detail_html(pcid, pc, items, items_html, download_html, 
                         expiry_date, header, custom_val, custom_display,
                         del_sel, next_quote_preview=""):
    """Build the Price Check detail page HTML.
    
    Extracted from dashboard.py to keep the main module lean.
    All parameters are pre-computed by the route handler.
    """
    return f"""<!doctype html><html><head><title>PC #{pc.get('pc_number','')}</title>
    <style>
     body{{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;padding:20px;font-size:15px;line-height:1.5}}
     .card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:12px 0}}
     table{{width:100%;border-collapse:collapse}}
     th,td{{padding:8px 10px;border:1px solid #30363d;text-align:left;font-size:14px}}
     th{{background:#21262d;font-size:12px;text-transform:uppercase;color:#8b949e;white-space:nowrap;letter-spacing:0.3px}}
     .btn{{padding:9px 18px;border-radius:6px;border:none;cursor:pointer;font-weight:600;text-decoration:none;display:inline-block;margin:4px;font-size:14px}}
     .btn-p{{background:#1f6feb;color:#fff}} .btn-g{{background:#238636;color:#fff}} .btn-o{{background:#da3633;color:#fff}}
     .btn-y{{background:#9e6a03;color:#fff}} .btn-v{{background:#8957e5;color:#fff}} .btn-sm{{padding:6px 14px;font-size:13px}}
     a{{color:#58a6ff}} h1{{margin:0;font-size:24px}} .meta{{color:#8b949e;font-size:15px}}
     .status{{padding:3px 8px;border-radius:4px;font-size:13px;font-weight:600;vertical-align:middle}}
     .status-parsed{{background:#1f6feb33;color:#58a6ff}} .status-priced{{background:#23863633;color:#3fb950}}
     .status-completed{{background:#23863633;color:#3fb950}} .status-converted{{background:#8957e533;color:#bc8cff}}
     .totals{{text-align:right;font-size:16px;margin-top:12px}} .totals b{{color:#3fb950}}
     .num-in{{width:82px;background:#0d1117;border:1px solid #484f58;color:#e6edf3;padding:6px 8px;border-radius:5px;text-align:right;font-size:15px;font-weight:500;font-family:'Segoe UI',system-ui,sans-serif}}
     .num-in:focus{{border-color:#58a6ff;outline:none;box-shadow:0 0 0 2px #1f6feb44}}
     .num-in.sm{{width:52px}}
     input[type=number]{{-moz-appearance:textfield}}
     input[type=number]::-webkit-outer-spin-button,input[type=number]::-webkit-inner-spin-button{{-webkit-appearance:none;margin:0}}
     .text-in{{background:#0d1117;border:1px solid #484f58;color:#e6edf3;padding:6px 8px;border-radius:5px;font-size:14px;font-family:'Segoe UI',system-ui,sans-serif}}
     .text-in:focus{{border-color:#58a6ff;outline:none;box-shadow:0 0 0 2px #1f6feb44}}
     textarea.text-in{{resize:vertical;min-height:38px;line-height:1.4}}
     .msg{{padding:10px 14px;border-radius:6px;margin:8px 0;font-size:14px}}
     .msg-ok{{background:#23863622;color:#3fb950;border:1px solid #23863655}}
     .msg-warn{{background:#9e6a0322;color:#d29922;border:1px solid #9e6a0355}}
     .msg-err{{background:#da363322;color:#f85149;border:1px solid #da363355}}
     .tier-btn{{background:#21262d;border:2px solid #30363d;color:#8b949e;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:14px;font-weight:600;line-height:1.3;text-align:center;transition:all .15s}}
     .tier-btn:hover{{border-color:#58a6ff;color:#c9d1d9;background:#21262d}}
     .tier-active{{background:#1f6feb22;border-color:#1f6feb;color:#58a6ff}}
     .desc-raw{{font-size:11px;color:#6e7681;display:block;margin-top:2px;font-style:italic}}
     /* Preview Modal */
     .modal-overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:1000;overflow-y:auto;padding:20px}}
     .modal-overlay.active{{display:flex;justify-content:center;align-items:flex-start}}
     .modal-content{{background:#fff;color:#1a1a1a;border-radius:10px;max-width:850px;width:100%;margin:20px auto;box-shadow:0 20px 60px rgba(0,0,0,.5)}}
     .modal-header{{display:flex;justify-content:space-between;align-items:center;padding:14px 20px;border-bottom:2px solid #1a1a1a;background:#f5f5f0;border-radius:10px 10px 0 0}}
     .modal-header h2{{margin:0;font-size:16px;color:#1a1a1a}}
     .modal-close{{background:none;border:none;font-size:24px;cursor:pointer;color:#666;padding:0 8px}}
     .modal-body{{padding:0}}
     /* Quote Preview Styles (print-like) */
     .q-preview{{font-family:'Times New Roman',Times,serif;font-size:13px;color:#000;line-height:1.4}}
     .q-preview table{{width:100%;border-collapse:collapse}}
     .q-preview th,.q-preview td{{border:1px solid #000;padding:4px 6px;text-align:left;font-size:12px}}
     .q-preview th{{background:#e8e8e0;font-size:10px;text-transform:uppercase;font-weight:700}}
     .q-header{{text-align:center;padding:10px;border-bottom:2px solid #000}}
     .q-header h3{{margin:0;font-size:14px;text-transform:uppercase;letter-spacing:1px}}
     .q-header small{{color:#444;font-size:11px}}
     .q-supplier{{display:grid;grid-template-columns:1fr 1fr;gap:0;border:1px solid #000;margin:0}}
     .q-supplier div{{padding:4px 8px;border:1px solid #000;font-size:12px}}
     .q-supplier .q-lbl{{font-size:9px;color:#555;text-transform:uppercase;display:block}}
     .q-total{{text-align:right;font-size:14px;font-weight:700;padding:8px 12px}}
     @media print{{
      body>*{{display:none!important}}
      .modal-overlay,.modal-overlay *{{display:block!important}}
      .modal-overlay{{position:static!important;background:none!important;padding:0!important}}
      .modal-content{{box-shadow:none!important;margin:0!important;max-width:none!important}}
      .modal-header button,.modal-close{{display:none!important}}
     }}
    </style></head><body>
    <div style="display:flex;gap:8px;align-items:center;margin-bottom:12px">
     <a href="/" style="color:#58a6ff;font-size:13px;text-decoration:none;padding:4px 10px;background:#21262d;border:1px solid #30363d;border-radius:6px">🏠 Home</a>
     <a href="/quotes" style="color:#fff;font-size:13px;text-decoration:none;padding:4px 10px;background:#1a3a5c;border-radius:6px">📋 Quotes</a>
    </div>

    <!-- Preview Modal -->
    <div class="modal-overlay" id="previewModal">
     <div class="modal-content">
      <div class="modal-header">
       <h2>📋 Quote Preview — <span id="previewFormType">AMS 704</span></h2>
       <div>
        <button class="btn btn-sm btn-g" onclick="window.print()" style="margin-right:8px;font-size:12px">🖨️ Print</button>
        <button class="modal-close" onclick="closePreview()">×</button>
       </div>
      </div>
      <div class="modal-body" id="previewBody"></div>
     </div>
    </div>

    <div class="card">
     <h1>Price Check #{pc.get('pc_number','unknown')}
      <span class="status status-{pc.get('status','parsed')}">{pc.get('status','parsed').upper()}</span></h1>
     {"<div style='margin:10px 0;padding:10px 16px;border-radius:8px;font-size:13px;display:flex;align-items:center;gap:10px;background:rgba(52,211,153,.08);border:1px solid rgba(52,211,153,.25);color:#3fb950'><span style=font-size:18px>✅</span><div><b>Quote Generated</b> — " + pc.get('reytech_quote_number','') + "<br><span style=color:#8b949e;font-size:12px>Prices locked. Download below or regenerate to update.</span></div></div>" if pc.get('status') in ('completed','converted') and pc.get('reytech_quote_number') else "<div style='margin:10px 0;padding:10px 16px;border-radius:8px;font-size:13px;display:flex;align-items:center;gap:10px;background:rgba(88,166,255,.08);border:1px solid rgba(88,166,255,.25);color:#58a6ff'><span style=font-size:18px>🔵</span><div><b>Priced</b> — ready to generate quote<br><span style=color:#8b949e;font-size:12px>Review prices below, then click Generate Quote.</span></div></div>" if pc.get('status') == 'priced' else ""}
     <div class="meta" style="margin-top:8px">
      <b>Institution:</b> {header.get('institution',pc.get('institution',''))} &nbsp;|&nbsp;
      <b>Requestor:</b> {header.get('requestor',pc.get('requestor',''))} &nbsp;|&nbsp;
      <b>Due:</b> {pc.get('due_date','')} <span id="dueUrgency"></span> &nbsp;|&nbsp;
      <b>Ship to:</b> {pc.get('ship_to','')}
     </div>

     <!-- CRM Customer Card + Quote History -->
     <div id="crmPanel" style="margin-top:10px;display:flex;gap:12px;flex-wrap:wrap">
      <div id="crmCard" style="flex:1;min-width:260px;background:var(--sf2);border:1px solid var(--bd);border-radius:8px;padding:12px;font-size:13px">
       <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
        <span style="font-size:11px;color:#8b949e;text-transform:uppercase;font-weight:600;letter-spacing:.5px">📇 Customer</span>
        <span id="crmBadge" style="font-size:10px;padding:2px 8px;border-radius:10px;font-weight:600"></span>
       </div>
       <div id="crmBody" style="color:var(--tx2)">Loading...</div>
      </div>
      <div id="historyCard" style="flex:1;min-width:320px;background:var(--sf2);border:1px solid var(--bd);border-radius:8px;padding:16px;font-size:13px">
       <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
        <span style="font-size:12px;color:#8b949e;text-transform:uppercase;font-weight:600;letter-spacing:.5px">📊 Quote History</span>
        <span id="historyBadge" style="font-size:11px;padding:2px 10px;border-radius:10px;font-weight:600;background:#1a3a5c;color:#58a6ff;display:none"></span>
       </div>
       <div id="historyBody" style="color:var(--tx2);font-size:13px">Loading...</div>
      </div>
     </div>

     <div style="margin-top:12px;display:flex;gap:6px;flex-wrap:wrap;align-items:center">
      <button class="btn btn-p" data-testid="pc-scprs-lookup" onclick="runScprs(this)">🔍 SCPRS Lookup</button>
      <button class="btn btn-y" data-testid="pc-amazon-lookup" onclick="runLookup(this)">🔬 Amazon Lookup</button>
      <span style="border-left:2px solid #30363d;height:28px;margin:0 8px"></span>
      <button class="btn" data-testid="pc-preview-quote" style="background:#21262d;color:#c9d1d9;border:1px solid #484f58" onclick="showPreview()">👁️ Preview Quote</button>
      <button class="btn btn-g" data-testid="pc-generate-704" onclick="saveAndGenerate(this)">{"♻️ Regenerate 704" if pc.get('status') in ('completed','converted') else "📄 Generate Completed 704"}</button>
      <button class="btn" data-testid="pc-generate-reytech-quote" style="background:#1a3a5c;color:#fff" onclick="generateReytechQuote(this)">{"♻️ Regenerate Quote" if pc.get('reytech_quote_number') else "📋 Reytech Quote PDF"}</button>
      {download_html}
      <span style="border-left:2px solid #30363d;height:28px;margin:0 8px"></span>
      {"<button class='btn' disabled style='opacity:0.4;cursor:not-allowed;background:#21262d;color:#8b949e;border:1px solid #30363d'>✅ Converted</button>" if pc.get('status')=='converted' else "<button class='btn btn-v' onclick=\"convertToQuote(this)\">🔄 Convert to Full Quote (704A/B + Package)</button>"}
     </div>
     <div style="margin-top:8px">
      {"<button class='btn' disabled style='opacity:0.4;cursor:not-allowed;background:#4a3000;color:#8b949e;border:1px solid #4a3000'>⚡ Already Processed</button>" if pc.get('status') in ('completed','converted') else "<button class='btn' data-testid='pc-auto-process' style='background:#f0883e;color:#fff' onclick='autoProcess(this)'>⚡ Auto-Process (SCPRS + Amazon + Price + Generate — one click)</button>"}
     </div>
     <div id="statusMsg"></div>
     <div id="confidenceBar"></div>
    </div>

    <div class="card" style="padding:16px 20px">
     <div style="display:flex;gap:28px;align-items:flex-start;flex-wrap:wrap">
      <div>
       <label style="font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:0.5px;font-weight:600">Delivery Time</label><br>
       <select id="deliverySelect" class="text-in" style="padding:8px 12px;font-size:15px;min-width:210px;margin-top:4px;cursor:pointer">
        <option value="3-5 business days" {del_sel["3-5 business days"]}>3-5 business days</option>
        <option value="5-7 business days" {del_sel["5-7 business days"]}>5-7 business days</option>
        <option value="7-14 business days" {del_sel["7-14 business days"]}>7-14 business days</option>
        <option value="custom" {del_sel["custom"]}>Custom...</option>
       </select>
       <input type="text" id="deliveryCustom" placeholder="e.g. 2-3 weeks" value="{custom_val}" class="text-in" style="display:{custom_display};padding:8px 12px;font-size:15px;width:160px;margin-left:6px;margin-top:4px">
      </div>
      <div>
       <label style="font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:0.5px;font-weight:600">Price Protection
        <span style="font-size:11px;text-transform:none;cursor:help" title="Amazon prices fluctuate. Buffer pads your cost basis so a temporary discount doesn't eat your margin when prices revert.">ⓘ</span>
       </label>
       <div style="display:flex;gap:6px;margin-top:6px" id="pricingTiers">
        <button class="tier-btn tier-active" onclick="applyTier(0,this)" data-buffer="0">Current<br><span style="font-size:11px;opacity:0.7">No buffer</span></button>
        <button class="tier-btn" onclick="applyTier(10,this)" data-buffer="10">Light<br><span style="font-size:11px;opacity:0.7">+10%</span></button>
        <button class="tier-btn" onclick="applyTier(15,this)" data-buffer="15">Standard<br><span style="font-size:11px;opacity:0.7">+15%</span></button>
        <button class="tier-btn" onclick="applyTier(20,this)" data-buffer="20">Safe<br><span style="font-size:11px;opacity:0.7">+20%</span></button>
       </div>
       <div id="tierComparison" style="margin-top:10px;font-size:14px;background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:10px 12px;display:none"></div>
      </div>
      <div>
       <label style="font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:0.5px;font-weight:600">Default Markup</label>
       <div style="display:flex;align-items:center;gap:6px;margin-top:6px">
        <input type="number" id="markupDefault" value="{pc.get('default_markup', 25)}" min="0" max="200" class="num-in" style="width:60px;text-align:center;font-size:16px;font-weight:700;padding:8px" onchange="applyTier(getCurrentBuffer(),document.querySelector('.tier-active'))">
        <span style="font-size:15px;color:#8b949e;font-weight:600">%</span>
       </div>
      </div>
      <div style="flex:1;min-width:250px">
       <label style="font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:0.5px;font-weight:600">Supplier Notes <span style="font-size:11px;text-transform:none;font-weight:400">(prints on 704)</span></label><br>
       <input type="text" id="supplierNotes" value="{pc.get('custom_notes','').replace('"','&quot;')}" placeholder="Optional — leave blank for no notes" class="text-in" style="width:100%;padding:8px 12px;font-size:15px;margin-top:4px">
      </div>
     </div>
    </div>

    <div class="card">
     <h3 style="margin-top:0;font-size:18px">Line Items <span id="itemCount" style="font-weight:normal;color:#8b949e;font-size:15px">({len(items)} items)</span></h3>
     <table id="itemsTable">
      <tr><th style="width:28px">Bid</th><th>#</th><th>Qty</th><th>UOM</th><th style="min-width:280px">Description</th><th>SCPRS $</th><th>Amazon $</th><th>Amazon Match</th><th>Unit Cost</th><th>Markup</th><th>Our Price</th><th>Extension</th><th>Profit</th><th>Conf</th></tr>
      {items_html}
     </table>
     <div style="margin-top:8px">
      <button class="btn btn-sm" style="background:#21262d;color:#8b949e;border:1px solid #30363d" onclick="addRow()">+ Add Item</button>
     </div>
     <div class="totals" id="totals"></div>
     <div style="margin-top:12px;display:flex;align-items:center;gap:20px;font-size:14px">
      <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
       <input type="checkbox" id="taxToggle" onchange="recalcPC()" style="width:18px;height:18px">
       <span>Include CA Sales Tax</span>
       <span id="taxRateDisplay" style="color:#8b949e;font-size:13px">(fetching rate...)</span>
      </label>
      <span style="color:#30363d">|</span>
      <span style="color:#8b949e">📅 Pricing valid through: <b id="expiryDate" style="color:#d29922">{expiry_date}</b> (45 days)</span>
     </div>
    </div>

    <script>
    let cachedTaxRate = null;

    // Fetch CA tax rate from CDTFA on load
    (function fetchTaxRate() {{
     fetch('/api/tax-rate').then(r=>r.json()).then(d=>{{
      if(d.rate) {{
       cachedTaxRate = d.rate;
       document.getElementById('taxRateDisplay').textContent = '(' + (d.rate*100).toFixed(3) + '% — ' + (d.jurisdiction||'CA') + ')';
      }} else {{
       cachedTaxRate = 0.0725;
       document.getElementById('taxRateDisplay').textContent = '(7.25% — default CA)';
      }}
     }}).catch(()=>{{
      cachedTaxRate = 0.0725;
      document.getElementById('taxRateDisplay').textContent = '(7.25% — default CA)';
     }});
    }})();

    // Delivery dropdown: show custom input when "Custom..." selected
    document.getElementById('deliverySelect').addEventListener('change', function() {{
     const custom=document.getElementById('deliveryCustom');
     if(this.value==='custom') {{
      custom.style.display='inline-block';
      custom.focus();
     }} else {{
      custom.style.display='none';
      custom.value='';
     }}
    }});

    function showMsg(text, type) {{
     const el=document.getElementById('statusMsg');
     el.innerHTML='<div class="msg msg-'+type+'">'+text+'</div>';
     if(type==='ok') setTimeout(()=>el.innerHTML='',5000);
    }}

    function getCurrentBuffer() {{
     const active=document.querySelector('.tier-active');
     return active?parseInt(active.getAttribute('data-buffer'))||0:0;
    }}

    function applyTier(buffer, btn) {{
     // Update active button
     document.querySelectorAll('.tier-btn').forEach(b=>b.classList.remove('tier-active'));
     if(btn) btn.classList.add('tier-active');
     const markup=parseInt(document.getElementById('markupDefault').value)||25;
     // Apply to all rows with Amazon data
     document.querySelectorAll('tr[data-row]').forEach((row,i)=>{{
      const amazonCell=row.querySelector('td[data-amazon]');
      if(!amazonCell) return;
      const amazonPrice=parseFloat(amazonCell.getAttribute('data-amazon'))||0;
      if(amazonPrice<=0) return;
      const protectedCost=Math.round(amazonPrice*(1+buffer/100)*100)/100;
      const costInp=row.querySelector('[name=cost_'+i+']');
      const markupInp=row.querySelector('[name=markup_'+i+']');
      if(costInp) costInp.value=protectedCost.toFixed(2);
      if(markupInp) markupInp.value=markup;
      recalcRow(i);
     }});
     showTierComparison();
    }}

    function showTierComparison() {{
     // Gather all Amazon-priced items to compute totals per tier
     const markup=parseInt(document.getElementById('markupDefault').value)||25;
     let amazonItems=[];
     document.querySelectorAll('tr[data-row]').forEach((row,i)=>{{
      const amazonCell=row.querySelector('td[data-amazon]');
      if(!amazonCell) return;
      const ap=parseFloat(amazonCell.getAttribute('data-amazon'))||0;
      if(ap<=0) return;
      const qtyInp=row.querySelector('[name=qty_'+i+']');
      const qty=parseInt(qtyInp?.value)||1;
      amazonItems.push({{ap,qty}});
     }});
     if(amazonItems.length===0) {{ document.getElementById('tierComparison').style.display='none'; return; }}

     const tiers=[
      {{name:'Current',buf:0,color:'#8b949e'}},
      {{name:'Light +10%',buf:10,color:'#58a6ff'}},
      {{name:'Standard +15%',buf:15,color:'#d29922'}},
      {{name:'Safe +20%',buf:20,color:'#3fb950'}}
     ];
     const activeBuf=getCurrentBuffer();
     let html='<table style="width:100%;border:none;font-size:14px"><tr>';
     html+='<th style="border:none;color:#8b949e;font-size:12px;padding:3px 8px;text-align:left">Tier</th>';
     html+='<th style="border:none;color:#8b949e;font-size:12px;padding:3px 8px;text-align:right">Cost Basis</th>';
     html+='<th style="border:none;color:#8b949e;font-size:12px;padding:3px 8px;text-align:right">Revenue</th>';
     html+='<th style="border:none;color:#8b949e;font-size:12px;padding:3px 8px;text-align:right">Profit</th>';
     html+='<th style="border:none;color:#8b949e;font-size:12px;padding:3px 8px;text-align:right">Margin</th></tr>';
     tiers.forEach(t=>{{
      let totalCost=0,totalRev=0;
      amazonItems.forEach(item=>{{
       let cost=item.ap*(1+t.buf/100);
       let price=cost*(1+markup/100);
       totalCost+=cost*item.qty;
       totalRev+=price*item.qty;
      }});
      let profit=totalRev-totalCost;
      let margin=totalRev>0?((profit/totalRev)*100):0;
      let isActive=t.buf===activeBuf;
      let rowStyle=isActive?'background:#1f6feb22;border-radius:4px;font-weight:600':'';
      let arrow=isActive?' ◀':'';
      html+='<tr style="'+rowStyle+'">';
      html+='<td style="border:none;padding:4px 8px;color:'+t.color+';font-size:14px">'+t.name+arrow+'</td>';
      html+='<td style="border:none;padding:4px 8px;text-align:right;font-size:14px">$'+totalCost.toFixed(2)+'</td>';
      html+='<td style="border:none;padding:4px 8px;text-align:right;font-size:14px">$'+totalRev.toFixed(2)+'</td>';
      html+='<td style="border:none;padding:4px 8px;text-align:right;font-size:14px;font-weight:600;color:'+(profit>0?'#3fb950':'#f85149')+'">$'+profit.toFixed(2)+'</td>';
      html+='<td style="border:none;padding:4px 8px;text-align:right;font-size:14px;font-weight:600;color:'+(margin>=20?'#3fb950':(margin>=10?'#d29922':'#f85149'))+'">'+margin.toFixed(1)+'%</td>';
      html+='</tr>';
     }});
     html+='</table>';
     const comp=document.getElementById('tierComparison');
     comp.innerHTML=html;
     comp.style.display='block';
    }}
    // Show comparison on load
    setTimeout(showTierComparison, 200);

    function recalcRow(idx) {{
     const cost=parseFloat(document.querySelector('[name=cost_'+idx+']').value)||0;
     const markup=parseFloat(document.querySelector('[name=markup_'+idx+']').value)||0;
     const priceField=document.querySelector('[name=price_'+idx+']');
     priceField.value=(cost*(1+markup/100)).toFixed(2);
     recalcPC();
    }}

    function toggleBid(idx, cb) {{
     const row=document.querySelector('tr[data-row="'+idx+'"]');
     if(row) row.style.opacity=cb.checked?'1':'0.4';
     recalcPC();
    }}

    function addRow() {{
     const table=document.getElementById('itemsTable');
     const rows=table.querySelectorAll('tr[data-row]');
     const idx=rows.length;
     const tr=document.createElement('tr');
     tr.setAttribute('data-row',idx);
     tr.innerHTML='<td style="text-align:center"><input type="checkbox" name="bid_'+idx+'" checked onchange="toggleBid('+idx+',this)" style="width:18px;height:18px;cursor:pointer"></td>'
      +'<td><input type="number" name="itemnum_'+idx+'" value="'+(idx+1)+'" class="num-in sm" style="width:40px"></td>'
      +'<td><input type="number" name="qty_'+idx+'" value="1" class="num-in sm" style="width:55px" onchange="recalcPC()"></td>'
      +'<td><input type="text" name="uom_'+idx+'" value="EA" class="text-in" style="width:45px;text-transform:uppercase;text-align:center;font-weight:600"></td>'
      +'<td><textarea name="desc_'+idx+'" class="text-in" style="width:100%;min-height:38px;resize:vertical;font-size:13px;line-height:1.4;padding:6px 8px" placeholder="Enter description"></textarea></td>'
      +'<td>—</td><td>—</td><td>—</td>'
      +'<td><input type="number" step="0.01" min="0" name="cost_'+idx+'" value="" class="num-in" onchange="recalcRow('+idx+')"></td>'
      +'<td><input type="number" step="1" min="0" max="200" name="markup_'+idx+'" value="25" class="num-in sm" style="width:48px" onchange="recalcRow('+idx+')"><span style="color:#8b949e;font-size:13px">%</span></td>'
      +'<td><input type="number" step="0.01" min="0" name="price_'+idx+'" value="" class="num-in" onchange="recalcPC()"></td>'
      +'<td class="ext">—</td><td class="profit">—</td><td style="text-align:center">—</td>';
     table.appendChild(tr);
     recalcPC();
    }}

    function recalcPC() {{
     let sub=0, totalCost=0, totalProfit=0, bidCount=0, totalCount=0;
     const priceInputs=document.querySelectorAll('input[name^=price_]');
     totalCount=priceInputs.length;
     priceInputs.forEach((inp,i)=>{{
      const bidCb=document.querySelector('[name=bid_'+i+']');
      const isBid=bidCb?bidCb.checked:true;
      let p=parseFloat(inp.value)||0;
      let c=parseFloat(document.querySelector('[name=cost_'+i+']')?.value)||0;
      let qtyInp=document.querySelector('[name=qty_'+i+']');
      let qty=parseInt(qtyInp?.value)||1;

      if(!isBid) {{
       // No-bid: zero out extension and profit display
       let extCells=document.querySelectorAll('.ext');
       if(extCells[i]) extCells[i].textContent='N/B';
       let profitCells=document.querySelectorAll('.profit');
       if(profitCells[i]) profitCells[i].innerHTML='<span style="color:#8b949e">N/B</span>';
       return;
      }}

      if(p>0) bidCount++;
      let ext=p*qty;
      let costExt=c*qty;
      let profit=ext-costExt;
      sub+=ext;
      totalCost+=costExt;
      totalProfit+=profit;

      let extCells=document.querySelectorAll('.ext');
      if(extCells[i]) extCells[i].textContent=ext>0?'$'+ext.toFixed(2):'—';

      let profitCells=document.querySelectorAll('.profit');
      if(profitCells[i]) {{
       if(c>0 && p>0) {{
        let pColor=profit>0?'#3fb950':(profit<0?'#f85149':'#8b949e');
        profitCells[i].innerHTML='<span style="color:'+pColor+'">$'+profit.toFixed(2)+'</span>';
       }} else {{
        profitCells[i].innerHTML='—';
       }}
      }}
     }});

     // Update item count
     document.getElementById('itemCount').textContent='(quoting '+bidCount+'/'+totalCount+' items)';

     // Tax
     let taxOn=document.getElementById('taxToggle').checked;
     let taxRate=taxOn?(cachedTaxRate||0.0725):0;
     let tax=sub*taxRate;
     let total=sub+tax;

     // Margin
     let margin=sub>0?((totalProfit/sub)*100):0;
     let marginColor=margin>=20?'#3fb950':(margin>=10?'#d29922':'#f85149');

     // Build totals display — two columns: left = financials, right = profit summary
     let html='<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:40px">';

     // Left: revenue totals
     html+='<div style="text-align:right;flex:1">';
     html+='<div><span style="color:#8b949e">Subtotal:</span> <b>$'+sub.toFixed(2)+'</b></div>';
     if(taxOn) {{
      html+='<div><span style="color:#8b949e">Tax ('+((taxRate*100).toFixed(3))+'%):</span> <b>$'+tax.toFixed(2)+'</b></div>';
     }}
     html+='<div style="font-size:16px;margin-top:4px"><span style="color:#8b949e">Total:</span> <b style="color:#3fb950">$'+total.toFixed(2)+'</b></div>';
     html+='</div>';

     // Right: profit summary
     html+='<div style="text-align:right;min-width:240px;background:#21262d;padding:12px 16px;border-radius:8px;border:1px solid #30363d">';
     html+='<div style="font-size:12px;text-transform:uppercase;color:#8b949e;margin-bottom:6px;letter-spacing:0.5px;font-weight:600">Profit Summary</div>';
     html+='<div style="font-size:15px"><span style="color:#8b949e">Total Cost:</span> <b>$'+totalCost.toFixed(2)+'</b></div>';
     html+='<div style="font-size:15px"><span style="color:#8b949e">Total Revenue:</span> <b>$'+sub.toFixed(2)+'</b></div>';
     let profitColor=totalProfit>0?'#3fb950':(totalProfit<0?'#f85149':'#8b949e');
     html+='<div style="font-size:16px;margin-top:2px"><span style="color:#8b949e">Profit:</span> <b style="color:'+profitColor+'">$'+totalProfit.toFixed(2)+'</b>';
     html+=' <span style="color:'+marginColor+';font-size:14px">('+margin.toFixed(1)+'% margin)</span></div>';
     html+='</div>';

     html+='</div>';
     document.getElementById('totals').innerHTML=html;
     // Also refresh tier comparison
     if(typeof showTierComparison==='function') showTierComparison();
    }}

    // Due date urgency
    (function calcUrgency() {{
     const dueStr='{pc.get("due_date","")}'.trim();
     if(!dueStr) return;
     // Try parsing common formats: M/D/YY, M/D/YYYY, YYYY-MM-DD
     let due=new Date(dueStr);
     if(isNaN(due)) {{
      // Try M/D/YY format
      const parts=dueStr.split('/');
      if(parts.length===3) {{
       let yr=parseInt(parts[2]);
       if(yr<100) yr+=2000;
       due=new Date(yr,parseInt(parts[0])-1,parseInt(parts[1]));
      }}
     }}
     if(isNaN(due)) return;
     const now=new Date();
     const diffMs=due-now;
     const diffDays=Math.ceil(diffMs/(1000*60*60*24));
     const el=document.getElementById('dueUrgency');
     if(diffDays<0) {{
      el.innerHTML='<span style="background:#da363344;color:#f85149;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600">OVERDUE ('+Math.abs(diffDays)+'d ago)</span>';
     }} else if(diffDays<=2) {{
      el.innerHTML='<span style="background:#da363344;color:#f85149;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600">🔥 '+diffDays+'d left</span>';
     }} else if(diffDays<=5) {{
      el.innerHTML='<span style="background:#9e6a0333;color:#d29922;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600">'+diffDays+'d left</span>';
     }} else {{
      el.innerHTML='<span style="background:#23863633;color:#3fb950;padding:2px 8px;border-radius:4px;font-size:11px">'+diffDays+'d left</span>';
     }}
    }})();

    recalcPC();

    function runScprs(btn) {{
     btn.disabled=true;btn.textContent='⏳ Searching SCPRS...';
     showMsg('Searching SCPRS Won Quotes knowledge base...','warn');
     fetch('/pricecheck/{pcid}/scprs-lookup').then(r=>r.json()).then(d=>{{
      btn.disabled=false;
      if(d.error){{
       btn.textContent='🔍 SCPRS Lookup';
       showMsg('❌ SCPRS error: '+d.error,'err');
      }} else if(d.found>0){{
       btn.textContent='🔍 SCPRS Lookup';
       showMsg('✅ SCPRS: Found '+d.found+'/'+d.total+' matches in Won Quotes KB. Reloading...','ok');
       setTimeout(()=>location.reload(),1200);
      }} else {{
       btn.textContent='🔍 SCPRS Lookup';
       showMsg('ℹ️ SCPRS: Searched '+d.total+' items — no matches found in Won Quotes KB. Try Amazon next.','warn');
      }}
     }}).catch(e=>{{
      btn.textContent='🔍 SCPRS Lookup';btn.disabled=false;
      showMsg('❌ SCPRS request failed: '+e,'err');
     }});
    }}

    function runLookup(btn) {{
     btn.disabled=true;btn.textContent='⏳ Searching Amazon...';
     showMsg('Searching Amazon via SerpApi...','warn');
     fetch('/pricecheck/{pcid}/lookup').then(r=>r.json()).then(d=>{{
      btn.disabled=false;
      if(d.error){{
       btn.textContent='🔬 Amazon Lookup';
       showMsg('❌ Amazon error: '+d.error,'err');
      }} else if(d.found>0){{
       btn.textContent='🔬 Amazon Lookup';
       showMsg('✅ Amazon: Found prices for '+d.found+'/'+d.total+' items. Reloading...','ok');
       setTimeout(()=>location.reload(),1200);
      }} else {{
       btn.textContent='🔬 Amazon Lookup';
       showMsg('ℹ️ Amazon: Searched '+d.total+' items — no results. These may need manual cost entry.','warn');
      }}
     }}).catch(e=>{{
      btn.textContent='🔬 Amazon Lookup';btn.disabled=false;
      showMsg('❌ Amazon request failed: '+e,'err');
     }});
    }}

    function collectPrices() {{
     let data={{}};
     document.querySelectorAll('input[name^=price_]').forEach(inp=>{{
      data[inp.name]=parseFloat(inp.value)||0;
     }});
     document.querySelectorAll('input[name^=cost_]').forEach(inp=>{{
      data[inp.name]=parseFloat(inp.value)||0;
     }});
     document.querySelectorAll('input[name^=markup_]').forEach(inp=>{{
      data[inp.name]=parseFloat(inp.value)||0;
     }});
     document.querySelectorAll('input[name^=qty_]').forEach(inp=>{{
      data[inp.name]=parseInt(inp.value)||1;
     }});
     document.querySelectorAll('input[name^=uom_],input[name^=itemno_]').forEach(inp=>{{
      data[inp.name]=inp.value;
     }});
     document.querySelectorAll('textarea[name^=desc_],input[name^=desc_]').forEach(inp=>{{
      data[inp.name]=inp.value;
     }});
     document.querySelectorAll('input[name^=bid_]').forEach(inp=>{{
      data[inp.name]=inp.checked;
     }});
     data['tax_enabled']=document.getElementById('taxToggle').checked;
     data['tax_rate']=cachedTaxRate||0;
     data['price_buffer']=getCurrentBuffer();
     data['default_markup']=parseInt(document.getElementById('markupDefault').value)||25;
     // Delivery option
     let delSel=document.getElementById('deliverySelect');
     data['delivery_option']=delSel.value==='custom'?document.getElementById('deliveryCustom').value:delSel.value;
     // Supplier notes
     data['custom_notes']=document.getElementById('supplierNotes').value;
     return data;
    }}

    // ── Quote Preview ──────────────────────────────────
    const REYTECH={{
     name:'Reytech Inc.',
     rep:'Michael Guadan',
     address:'30 Carnoustie Way, Trabuco Canyon, CA 92679',
     phone:'949-229-1575',
     email:'sales@reytechinc.com',
     sbmb:'2002605',
     dvbe:'2002605'
    }};
    const PC_META={{
     pcNum:'{pc.get("pc_number","").replace("'","\\'")}',
     institution:'{header.get("institution",pc.get("institution","")).replace("'","\\'")}',
     requestor:'{header.get("requestor",pc.get("requestor","")).replace("'","\\'")}',
     dueDate:'{pc.get("due_date","").replace("'","\\'")}',
     shipTo:'{pc.get("ship_to","").replace("'","\\'")}',
     phone:'{header.get("phone","").replace("'","\\'")}',
     quoteNum:'{pc.get("reytech_quote_number","").replace("'","\\'")}',
     expiry:document.getElementById('expiryDate')?.textContent||''
    }};
    const peek_next='{next_quote_preview}';

    function showPreview() {{
     // Build preview matching the Reytech quote PDF format
     const rows=document.querySelectorAll('tr[data-row]');
     let itemsHtml='';
     let subtotal=0;
     let itemCount=0;
     rows.forEach((row,i)=>{{
      const bid=row.querySelector('[name=bid_'+i+']');
      if(bid&&!bid.checked) return;
      const itemNo=row.querySelector('[name=itemnum_'+i+']')?.value||i+1;
      const qty=parseInt(row.querySelector('[name=qty_'+i+']')?.value)||1;
      const uom=row.querySelector('[name=uom_'+i+']')?.value||'EA';
      const desc=row.querySelector('[name=desc_'+i+']')?.value||'';
      const price=parseFloat(row.querySelector('[name=price_'+i+']')?.value)||0;
      const cost=parseFloat(row.querySelector('[name=cost_'+i+']')?.value)||0;
      const asin=row.querySelector('.asin-tag')?.textContent?.replace('ASIN:','').trim()||'';
      const ext=Math.round(price*qty*100)/100;
      subtotal+=ext;
      itemCount++;
      const descWithAsin=asin?desc+'\\nRef ASIN: '+asin:desc;
      const mfgPart=asin||'—';
      itemsHtml+=`<tr>
       <td style="text-align:center;border:1px solid #ccc;padding:6px">${{itemNo}}</td>
       <td style="border:1px solid #ccc;padding:6px;font-size:11px;max-width:280px;white-space:pre-wrap">${{descWithAsin}}</td>
       <td style="text-align:center;border:1px solid #ccc;padding:6px;font-family:monospace;font-size:10px">${{mfgPart}}</td>
       <td style="text-align:center;border:1px solid #ccc;padding:6px">${{qty}}</td>
       <td style="text-align:center;border:1px solid #ccc;padding:6px">${{uom.toUpperCase()}}</td>
       <td style="text-align:right;border:1px solid #ccc;padding:6px">$${{price.toFixed(2)}}</td>
       <td style="text-align:right;border:1px solid #ccc;padding:6px;font-weight:600">$${{ext.toFixed(2)}}</td>
      </tr>`;
     }});
     const taxOn=document.getElementById('taxToggle')?.checked;
     const taxRate=cachedTaxRate||0.0725;
     const tax=taxOn?Math.round(subtotal*taxRate*100)/100:0;
     const total=Math.round((subtotal+tax)*100)/100;
     const delivery=document.getElementById('deliverySelect')?.value==='custom'
       ?document.getElementById('deliveryCustom')?.value
       :document.getElementById('deliverySelect')?.value||'5-7 business days';
     const expiry=document.getElementById('expiryDate')?.textContent||'';
     const qNum=PC_META.quoteNum||peek_next||'(Next)';

     const today=new Date();
     const dateStr=today.toLocaleDateString('en-US',{{month:'short',day:'numeric',year:'numeric'}});

     const html=`<div style="padding:28px;font-family:'DM Sans',Helvetica,sans-serif;color:#1a1a1a;background:#fff">
      <!-- Header: Logo area + Quote # box -->
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:4px">
       <div>
        <div style="font-size:18px;font-weight:700;color:#1a2744">▲ Reytech Inc.</div>
        <div style="font-size:11px;color:#444;line-height:1.6;margin-top:4px">
         30 Carnoustie Way<br>Trabuco Canyon, CA 92679<br>
         Michael Guadan, Owner<br>sales@reytechinc.com<br>(714) 501-3530
        </div>
       </div>
       <div style="text-align:right">
        <div style="font-size:22px;font-weight:700;color:#1a2744;letter-spacing:1px">QUOTE</div>
        <table style="margin-left:auto;margin-top:6px;border-collapse:collapse">
         <tr><td style="background:#1a2744;color:#fff;font-size:10px;font-weight:600;padding:4px 10px;text-transform:uppercase">Quote #</td>
             <td style="border:1px solid #ccc;padding:4px 10px;font-weight:700">${{qNum}}</td></tr>
         <tr><td style="background:#1a2744;color:#fff;font-size:10px;font-weight:600;padding:4px 10px;text-transform:uppercase">Date</td>
             <td style="border:1px solid #ccc;padding:4px 10px">${{dateStr}}</td></tr>
        </table>
       </div>
      </div>

      <!-- To / Ship To -->
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0;margin-top:12px">
       <div style="background:#1a2744;color:#fff;font-size:10px;font-weight:600;padding:4px 10px;text-transform:uppercase">To:</div>
       <div style="background:#1a2744;color:#fff;font-size:10px;font-weight:600;padding:4px 10px;text-transform:uppercase">Ship To:</div>
       <div style="border:1px solid #ccc;padding:8px 10px;font-size:12px">${{PC_META.institution}}<br>${{PC_META.shipTo}}</div>
       <div style="border:1px solid #ccc;padding:8px 10px;font-size:12px">${{PC_META.institution}}<br>${{PC_META.shipTo}}</div>
      </div>

      <!-- Terms bar -->
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:0;margin-top:8px">
       <div><div style="background:#f0f0ec;font-size:9px;font-weight:600;padding:3px 8px;text-transform:uppercase;border:1px solid #ccc">Delivery</div>
            <div style="border:1px solid #ccc;padding:4px 8px;font-size:12px">${{delivery}}</div></div>
       <div><div style="background:#f0f0ec;font-size:9px;font-weight:600;padding:3px 8px;text-transform:uppercase;border:1px solid #ccc">Terms</div>
            <div style="border:1px solid #ccc;padding:4px 8px;font-size:12px">Net 45</div></div>
       <div><div style="background:#f0f0ec;font-size:9px;font-weight:600;padding:3px 8px;text-transform:uppercase;border:1px solid #ccc">Valid Until</div>
            <div style="border:1px solid #ccc;padding:4px 8px;font-size:12px">${{expiry}}</div></div>
      </div>

      <!-- Items table -->
      <table style="width:100%;border-collapse:collapse;margin-top:12px">
       <thead><tr style="background:#1a2744;color:#fff">
        <th style="padding:6px 8px;font-size:10px;text-transform:uppercase;width:40px">#</th>
        <th style="padding:6px 8px;font-size:10px;text-transform:uppercase;text-align:left">Description</th>
        <th style="padding:6px 8px;font-size:10px;text-transform:uppercase">MFG Part #</th>
        <th style="padding:6px 8px;font-size:10px;text-transform:uppercase;width:40px">Qty</th>
        <th style="padding:6px 8px;font-size:10px;text-transform:uppercase;width:40px">UOM</th>
        <th style="padding:6px 8px;font-size:10px;text-transform:uppercase;text-align:right">Unit Price</th>
        <th style="padding:6px 8px;font-size:10px;text-transform:uppercase;text-align:right">Extension</th>
       </tr></thead>
       <tbody>${{itemsHtml}}</tbody>
      </table>

      <!-- Totals -->
      <div style="display:flex;justify-content:flex-end;margin-top:8px">
       <table style="border-collapse:collapse;min-width:220px">
        <tr><td style="text-align:right;padding:4px 10px;font-size:12px;border:1px solid #ccc">Subtotal</td>
            <td style="text-align:right;padding:4px 10px;font-size:13px;border:1px solid #ccc;font-weight:600">$${{subtotal.toFixed(2)}}</td></tr>
        ${{taxOn?`<tr><td style="text-align:right;padding:4px 10px;font-size:12px;border:1px solid #ccc">Tax (${{(taxRate*100).toFixed(2)}}%)</td>
            <td style="text-align:right;padding:4px 10px;font-size:13px;border:1px solid #ccc">$${{tax.toFixed(2)}}</td></tr>`:''}}
        <tr style="background:#1a2744;color:#fff"><td style="text-align:right;padding:6px 10px;font-size:13px;font-weight:700">TOTAL</td>
            <td style="text-align:right;padding:6px 10px;font-size:15px;font-weight:700">$${{total.toFixed(2)}}</td></tr>
       </table>
      </div>

      <div style="margin-top:16px;font-size:10px;color:#666;text-align:center;border-top:1px solid #ddd;padding-top:8px">
       Sellers Permit: 245652416 &nbsp;|&nbsp; SB/MB Cert: 2023764 &nbsp;|&nbsp; ${{itemCount}} item(s) quoted
      </div>
     </div>`;

     document.getElementById('previewBody').innerHTML=html;
     document.getElementById('previewFormType').textContent='Reytech Quote — '+PC_META.institution;
     document.getElementById('previewModal').classList.add('active');
    }}

    function closePreview() {{
     document.getElementById('previewModal').classList.remove('active');
    }}
    // Close on Esc or click outside
    document.getElementById('previewModal').addEventListener('click',function(e){{
     if(e.target===this) closePreview();
    }});
    document.addEventListener('keydown',function(e){{if(e.key==='Escape') closePreview();}});

    function saveAndGenerate(btn) {{
     btn.disabled=true;btn.textContent='⏳ Saving prices...';
     showMsg('Saving prices and generating completed AMS 704...','warn');
     fetch('/pricecheck/{pcid}/save-prices',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(collectPrices())}})
     .then(r=>r.json()).then(d=>{{
      if(!d.ok){{btn.textContent='📄 Generate Completed 704';btn.disabled=false;showMsg('❌ Save failed','err');return;}}
      btn.textContent='⏳ Generating PDF...';
      return fetch('/pricecheck/{pcid}/generate');
     }}).then(r=>r.json()).then(d=>{{
      btn.disabled=false;btn.textContent='📄 Generate Completed 704';
      if(d&&d.ok){{showMsg('✅ Completed 704 generated! Reloading...','ok');setTimeout(()=>location.reload(),1200)}}
      else{{showMsg('❌ Generation failed: '+(d?.error||'unknown'),'err')}}
     }}).catch(e=>{{btn.textContent='📄 Generate Completed 704';btn.disabled=false;showMsg('❌ Error: '+e,'err')}});
    }}

    function generateReytechQuote(btn) {{
     btn.disabled=true;btn.textContent='⏳ Saving...';
     showMsg('Saving prices and generating Reytech Quote PDF...','warn');
     fetch('/pricecheck/{pcid}/save-prices',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(collectPrices())}})
     .then(r=>r.json()).then(d=>{{
      if(!d.ok){{btn.textContent='📋 Reytech Quote PDF';btn.disabled=false;showMsg('❌ Save failed','err');return;}}
      btn.textContent='⏳ Generating quote...';
      return fetch('/pricecheck/{pcid}/generate-quote');
     }}).then(r=>r.json()).then(d=>{{
      btn.disabled=false;btn.textContent='📋 Reytech Quote PDF';
      if(d&&d.ok){{showMsg('✅ Reytech Quote #'+d.quote_number+' generated! Reloading...','ok');setTimeout(()=>location.reload(),1200)}}
      else{{showMsg('❌ Quote generation failed: '+(d?.error||'unknown'),'err')}}
     }}).catch(e=>{{btn.textContent='📋 Reytech Quote PDF';btn.disabled=false;showMsg('❌ Error: '+e,'err')}});
    }}

    function convertToQuote(btn) {{
     if(!confirm('Convert this Price Check into a full quote?\\n\\nThis will create an RFQ entry with 704A, 704B, and Bid Package forms pre-filled with the pricing from this Price Check.'))return;
     btn.disabled=true;btn.textContent='⏳ Converting...';
     showMsg('Saving prices first...','warn');
     fetch('/pricecheck/{pcid}/save-prices',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(collectPrices())}})
     .then(r=>r.json()).then(d=>{{
      return fetch('/pricecheck/{pcid}/convert-to-quote');
     }}).then(r=>r.json()).then(d=>{{
      btn.disabled=false;btn.textContent='🔄 Convert to Full Quote (704A/B + Package)';
      if(d&&d.ok){{showMsg('✅ Quote created! Redirecting...','ok');setTimeout(()=>window.location='/rfq/'+d.rfq_id,1000)}}
      else{{showMsg('❌ Conversion failed: '+(d?.error||'unknown'),'err')}}
     }}).catch(e=>{{btn.textContent='🔄 Convert to Full Quote (704A/B + Package)';btn.disabled=false;showMsg('❌ Error: '+e,'err')}});
    }}

    function autoProcess(btn) {{
     btn.disabled=true;btn.textContent='⚡ Running full pipeline...';
     showMsg('⚡ Auto-processing: Parse → SCPRS → Amazon → Price → Generate PDF...','warn');
     fetch('/pricecheck/{pcid}/auto-process').then(r=>r.json()).then(d=>{{
      btn.disabled=false;btn.textContent='⚡ Auto-Process (SCPRS + Amazon + Price + Generate — one click)';
      if(d.ok){{
       let t=d.timing||{{}};
       let c=d.confidence||{{}};
       let grade=c.overall_grade||'?';
       let gradeColor={{'A':'#3fb950','B':'#58a6ff','C':'#d29922','F':'#f85149'}}[grade]||'#8b949e';
       let msg='✅ Auto-processed in '+t.total+'s! ';
       msg+='Confidence: <b style="color:'+gradeColor+'">'+grade+' ('+((c.overall_score||0)*100).toFixed(0)+'%)</b>';
       if(c.recommendation) msg+=' — '+c.recommendation;
       if(d.draft_email) msg+='<br>📧 Email draft ready.';
       showMsg(msg,'ok');
       // Show confidence bar
       let bar=document.getElementById('confidenceBar');
       let pct=((c.overall_score||0)*100).toFixed(0);
       bar.innerHTML='<div style="margin-top:8px;background:#21262d;border-radius:4px;overflow:hidden;height:24px;position:relative"><div style="width:'+pct+'%;background:'+gradeColor+';height:100%;transition:width 1s"></div><span style="position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);font-size:12px;font-weight:bold">Confidence: '+grade+' ('+pct+'%)</span></div>';
       // Show timing breakdown
       let steps=(d.steps||[]).map(s=>s.step+': '+(s.found!==undefined?s.found+' found':'ok')).join(' → ');
       bar.innerHTML+='<div style="font-size:11px;color:#8b949e;margin-top:4px">Pipeline: '+steps+' | Total: '+t.total+'s</div>';
       setTimeout(()=>location.reload(),3000);
      }} else {{
       showMsg('❌ Auto-process failed: '+(d.error||'unknown'),'err');
      }}
     }}).catch(e=>{{btn.textContent='⚡ Auto-Process';btn.disabled=false;showMsg('❌ Error: '+e,'err')}});
    }}

    // ── CRM Customer Card + Quote History (loads on page) ─────────────────────
    // Timeout wrapper for fetch — Railway cold starts can hang
    function fetchWithTimeout(url, ms=8000) {{
      const ctrl = new AbortController();
      const timer = setTimeout(()=>ctrl.abort(), ms);
      return fetch(url, {{signal:ctrl.signal}}).finally(()=>clearTimeout(timer));
    }}
    function retryFetch(url, attempts=2, ms=8000) {{
      return fetchWithTimeout(url, ms).catch(err => {{
        if (attempts <= 1) throw err;
        return new Promise(r=>setTimeout(r,1500)).then(()=>retryFetch(url,attempts-1,ms));
      }});
    }}

    (function loadCrmAndHistory() {{
     const inst = PC_META.institution;
     if (!inst) {{
       document.getElementById('crmBody').innerHTML='<span style="color:#8b949e">No institution detected</span>';
       document.getElementById('historyBody').innerHTML='<span style="color:#8b949e">No institution — upload a valid PC</span>';
       return;
     }}

     // CRM match — with retry
     document.getElementById('crmBody').innerHTML='<span style="color:#8b949e">🔄 Loading CRM...</span>';
     retryFetch('/api/customers/match?q=' + encodeURIComponent(inst))
     .then(r=>r.json())
     .then(d=>{{
      const card = document.getElementById('crmBody');
      const badge = document.getElementById('crmBadge');
      if (d.matched && d.customer) {{
       const c = d.customer;
       const agColors = {{CDCR:'#3fb950',CCHCS:'#58a6ff',CalVet:'#d29922',DSH:'#bc8cff',DGS:'#f0883e',DEFAULT:'#8b949e'}};
       const ag = c.agency || 'DEFAULT';
       badge.textContent = ag;
       badge.style.background = (agColors[ag]||'#8b949e') + '22';
       badge.style.color = agColors[ag]||'#8b949e';
       let html = '<div style="font-weight:600;font-size:14px;color:var(--tx)">' + (c.display_name||'') + '</div>';
       if (c.parent) html += '<div style="font-size:11px;color:#8b949e;margin-top:2px">↳ ' + c.parent + '</div>';
       if (c.email) html += '<div style="margin-top:4px">📧 <a href="mailto:' + c.email + '" style="color:#58a6ff">' + c.email + '</a></div>';
       if (c.phone) html += '<div>📞 ' + c.phone + '</div>';
       if (c.address) html += '<div style="margin-top:4px;font-size:11px;color:var(--tx2)">' + c.address.replace(/\\n/g,'<br>') + '</div>';
       if (c.city) html += '<div style="font-size:11px;color:var(--tx2)">' + c.city + ', ' + (c.state||'CA') + ' ' + (c.zip||'') + '</div>';
       html += '<div style="margin-top:4px;font-size:10px;color:#8b949e">Source: ' + (c.source||'manual') + '</div>';
       card.innerHTML = html;
      }} else {{
       badge.textContent = '⚠️ NEW';
       badge.style.background = 'rgba(210,153,34,.15)';
       badge.style.color = '#d29922';
       let html = '<div style="color:#d29922;font-weight:600">"' + inst + '" not in CRM</div>';
       const suggested = d.suggested_agency || 'DEFAULT';
       html += '<div style="font-size:11px;margin-top:4px;color:var(--tx2)">Suggested agency: <b>' + suggested + '</b></div>';
       if (d.candidates && d.candidates.length > 0) {{
        html += '<div style="margin-top:6px;font-size:11px;color:var(--tx2)">Did you mean:</div>';
        d.candidates.forEach(c => {{
         html += '<div style="margin-left:8px;font-size:11px">• ' + c.display_name + ' <span style="color:#8b949e">(' + (c.agency||'') + ')</span></div>';
        }});
       }}
       html += '<button onclick="addNewCustomer()" class="btn btn-sm" data-testid="crm-add-customer" style="margin-top:8px;background:rgba(210,153,34,.15);color:#d29922;border:1px solid rgba(210,153,34,.3);font-size:11px;padding:3px 10px;cursor:pointer">+ Add to CRM</button>';
       card.innerHTML = html;
      }}
     }}).catch((err)=>{{
      document.getElementById('crmBody').innerHTML='<span style="color:#f85149">⚠️ CRM load failed</span><br><span style="color:#8b949e;font-size:11px">' + (err.name==='AbortError'?'Timeout — server may be starting up':'Network error') + '</span><br><button onclick="location.reload()" style="margin-top:6px;font-size:11px;background:none;border:1px solid var(--bd);color:var(--tx2);padding:3px 10px;border-radius:4px;cursor:pointer">↻ Retry</button>';
     }});

     // Quote history for this institution — with hyperlinks and hover previews
     document.getElementById('historyBody').innerHTML='<span style="color:#8b949e">🔄 Loading history...</span>';
     retryFetch('/api/quotes/history?institution=' + encodeURIComponent(inst))
     .then(r=>r.json())
     .then(history=>{{
      const el = document.getElementById('historyBody');
      const badge = document.getElementById('historyBadge');
      if (!history || history.length === 0) {{
       el.innerHTML = '<span style="color:#8b949e;font-size:13px">No previous quotes for this institution</span>';
       return;
      }}
      const stCfg = {{
        won:['✅','#3fb950','Won — PO received'],
        lost:['❌','#f85149','Lost — not awarded'],
        pending:['⏳','#d29922','Pending — awaiting decision'],
        draft:['📝','#8b949e','Draft — not yet sent'],
      }};

      // Summary badge
      badge.style.display = 'inline';
      badge.textContent = history.length + ' quote' + (history.length>1?'s':'');

      // Summary stats row
      const won = history.filter(h=>h.status==='won').length;
      const lost = history.filter(h=>h.status==='lost').length;
      const pending = history.filter(h=>['pending','draft'].includes(h.status)).length;
      const wonTotal = history.filter(h=>h.status==='won').reduce((s,h)=>s+(h.total||0),0);
      let html = '<div style="display:flex;gap:12px;padding:8px 0 10px;border-bottom:1px solid var(--bd);font-size:13px;font-weight:600">';
      if (won) html += '<span style="color:#3fb950">' + won + ' won · $' + wonTotal.toLocaleString('en',{{minimumFractionDigits:2}}) + '</span>';
      if (lost) html += '<span style="color:#f85149">' + lost + ' lost</span>';
      if (pending) html += '<span style="color:#d29922">' + pending + ' pending</span>';
      html += '</div>';

      // Quote rows — each entity hyperlinked with hover preview
      history.forEach(h => {{
       const [icon,color,statusTip] = stCfg[h.status] || stCfg.pending;
       const items = (h.items_text||'').substring(0,100);
       const itemsList = (h.items_detail||[]).map(it => 
         it.description.substring(0,60) + ' (x' + it.qty + ') $' + (it.unit_price||0).toFixed(2)
       ).join('\\n');
       const tooltipContent = h.quote_number + ' — ' + statusTip 
         + '\\n$' + (h.total||0).toFixed(2) + ' · ' + (h.items_count||0) + ' items'
         + (h.po_number ? '\\nPO: ' + h.po_number : '')
         + (h.days_ago ? '\\n' + h.days_ago : '')
         + (itemsList ? '\\n───\\n' + itemsList : '');

       html += '<div class="qh-row" style="padding:8px 0;border-bottom:1px solid rgba(48,54,61,0.5)">';
       
       // Row 1: Quote number + status + total
       html += '<div style="display:flex;gap:10px;align-items:center">';
       // Quote number — hyperlinked
       html += '<a href="' + (h.quote_url||'/quotes') + '" class="qh-link" title="' + tooltipContent.replace(/"/g,'&quot;') + '" '
         + 'style="font-family:\'JetBrains Mono\',monospace;font-weight:700;font-size:13px;color:#58a6ff;text-decoration:none">'
         + h.quote_number + '</a>';
       // Status badge — links to source PC if available
       const statusLink = h.source_pc_url || h.source_rfq_url || '#';
       const statusTitle = statusLink !== '#' ? 'View source document' : statusTip;
       html += '<a href="' + statusLink + '" title="' + statusTitle + '" '
         + 'style="color:' + color + ';font-size:12px;text-decoration:none;padding:1px 8px;border-radius:10px;'
         + 'background:' + color + '18;font-weight:600;white-space:nowrap">'
         + icon + ' ' + h.status + '</a>';
       // Date
       html += '<span style="font-size:12px;color:#8b949e;flex:1">' + (h.date||'') + '</span>';
       // Total
       html += '<span style="font-family:\'JetBrains Mono\',monospace;font-weight:600;font-size:13px;color:var(--tx)">$' 
         + (h.total||0).toLocaleString('en',{{minimumFractionDigits:2}}) + '</span>';
       html += '</div>';
       
       // Row 2: Items preview + PO + source link
       html += '<div style="display:flex;gap:8px;margin-top:4px;align-items:center">';
       if (items) html += '<span style="font-size:11px;color:#8b949e;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' 
         + (h.items_count||0) + ' items: ' + items + '</span>';
       if (h.po_number) html += '<span style="font-size:11px;padding:1px 6px;background:#238636;color:#fff;border-radius:4px;font-weight:600">PO: ' + h.po_number + '</span>';
       if (h.source_pc_url) html += '<a href="' + h.source_pc_url + '" title="View source Price Check" '
         + 'style="font-size:10px;color:#58a6ff;text-decoration:none">📄 PC</a>';
       if (h.source_rfq_url) html += '<a href="' + h.source_rfq_url + '" title="View source RFQ" '
         + 'style="font-size:10px;color:#58a6ff;text-decoration:none">📋 RFQ</a>';
       html += '</div>';
       
       html += '</div>';
      }});
      el.innerHTML = html;
     }}).catch((err)=>{{
      document.getElementById('historyBody').innerHTML='<span style="color:#f85149">⚠️ History load failed</span><br><span style="color:#8b949e;font-size:11px">' + (err.name==='AbortError'?'Timeout — server may be starting up':'Network error') + '</span><br><button onclick="location.reload()" style="margin-top:6px;font-size:11px;background:none;border:1px solid var(--bd);color:var(--tx2);padding:3px 10px;border-radius:4px;cursor:pointer">↻ Retry</button>';
     }});
    }})();

    function addNewCustomer() {{
     const inst = PC_META.institution;
     const ag = prompt('Agency for "' + inst + '"?\\n(CDCR, CCHCS, CalVet, DGS, DSH, DEFAULT)', 'CDCR');
     if (!ag) return;
     const parent = prompt('Parent organization?\\n(e.g. "Dept of Corrections and Rehabilitation")', '');
     fetch('/api/customers', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{display_name: inst, agency: ag, parent: parent||'',
                            city: '', state: 'CA', source: 'user_validated'}})
     }})
     .then(r=>r.json())
     .then(d=>{{
      if (d.ok) {{ alert('✅ Added "' + inst + '" to CRM'); location.reload(); }}
      else {{ alert('⚠️ ' + (d.error||'Failed')); }}
     }});
    }}
    </script>
    </body></html>
"""


def build_quotes_page_content(stats_html, q, agency_filter, status_filter,
                               logo_exists, rows_html):
    """Build the Quotes Database page content HTML.
    
    Extracted from dashboard.py. Returns content string for the render() wrapper.
    """
    return f"""
     <!-- Header: Logo + Title + Stats — all one row -->
     <div class="card" style="margin-bottom:10px;padding:14px 18px">
      <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap">
       {"<div style='background:#fff;padding:4px 8px;border-radius:6px;display:flex;align-items:center'><img src=/api/logo alt=Reytech style=height:36px></div>" if logo_exists else ""}
       <h2 style="margin:0;font-size:20px">Quotes Database</h2>
       <div style="margin-left:auto">{stats_html}</div>
      </div>
     </div>

     <!-- Search + Filters + Logo Upload — one compact bar -->
     <div class="card" style="margin-bottom:10px;padding:10px 14px">
      <form method="get" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
       <input name="q" value="{q}" placeholder="Search quotes, institutions, RFQ #..." style="flex:1;min-width:160px;padding:7px 10px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);font-size:13px">
       <select name="agency" style="padding:7px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);font-size:13px">
        <option value="">All Agencies</option>
        <option value="CDCR" {"selected" if agency_filter=="CDCR" else ""}>CDCR</option>
        <option value="CCHCS" {"selected" if agency_filter=="CCHCS" else ""}>CCHCS</option>
        <option value="CalVet" {"selected" if agency_filter=="CalVet" else ""}>CalVet</option>
        <option value="DGS" {"selected" if agency_filter=="DGS" else ""}>DGS</option>
        <option value="DSH" {"selected" if agency_filter=="DSH" else ""}>DSH</option>
       </select>
       <select name="status" style="padding:7px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);font-size:13px">
        <option value="">All Status</option>
        <option value="pending" {"selected" if status_filter=="pending" else ""}>⏳ Pending</option>
        <option value="won" {"selected" if status_filter=="won" else ""}>✅ Won</option>
        <option value="lost" {"selected" if status_filter=="lost" else ""}>❌ Lost</option>
       </select>
       <button type="submit" class="btn btn-p" style="padding:7px 14px;font-size:13px">Search</button>
       <span style="border-left:1px solid #30363d;height:22px;margin:0 2px"></span>
       <span style="font-size:11px;color:#8b949e">Logo:</span>
       {"<span style='font-size:11px;color:#3fb950'>✅</span>" if logo_exists else "<span style='font-size:11px;color:#f85149'>❌</span>"}
       <form method="post" action="/settings/upload-logo" enctype="multipart/form-data" style="display:contents">
        <input type="file" name="logo" accept=".png,.jpg,.jpeg,.gif" style="font-size:11px;max-width:140px">
        <button type="submit" class="btn btn-sm" style="font-size:11px;padding:3px 8px;background:var(--sf2);color:var(--tx2);border:1px solid var(--bd)">Upload</button>
       </form>
      </form>
     </div>

     <!-- Quotes Table — flush, no extra padding -->
     <div class="card" style="padding:0;overflow-x:auto">
      <table style="min-width:900px">
       <thead><tr>
        <th style="width:80px">Quote #</th><th style="width:100px">Date</th><th style="width:70px">Agency</th><th style="min-width:180px">Institution</th><th style="width:90px">RFQ #</th>
        <th style="text-align:right;width:90px">Total</th><th style="width:50px">Items</th><th style="width:80px">Status</th><th style="width:100px">Actions</th>
       </tr></thead>
       <tbody>{rows_html if rows_html else '<tr><td colspan="9" style="text-align:center;padding:24px;color:var(--tx2)">No quotes yet — generate your first from a Price Check or RFQ</td></tr>'}</tbody>
      </table>
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
     </script>
"""
