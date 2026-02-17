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
<div class="card">
 <div class="card-t">RFQ Queue ({{rfqs|length}})</div>
 {% for id, r in rfqs|dictsort(reverse=true) %}
 <a href="/rfq/{{id}}" class="rfq-i">
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
 <a href="/pricecheck/{{id}}" class="rfq-i">
  <div class="sol">#{{pc.pc_number}}</div>
  <div class="det"><b>{{pc.institution}}</b> · Due {{pc.due_date}}{% if pc.requestor %} · {{pc.requestor}}{% endif %}</div>
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
