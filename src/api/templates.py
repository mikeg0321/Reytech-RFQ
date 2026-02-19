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
.hdr{background:var(--sf);border-bottom:2px solid var(--bd);padding:14px 28px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;min-height:68px}
.hdr h1{font-size:17px;font-weight:600;letter-spacing:-0.3px;color:var(--tx2)}
.hdr-btn{padding:6px 14px;font-size:12px;font-weight:600;border-radius:6px;border:1px solid var(--bd);background:var(--sf2);color:var(--tx);cursor:pointer;text-decoration:none;transition:.15s;font-family:'DM Sans',sans-serif;display:inline-flex;align-items:center;gap:4px}
.hdr-btn:hover{border-color:var(--ac);background:rgba(79,140,255,.1);color:#fff}
.hdr-active{border-color:var(--ac);background:rgba(79,140,255,.12)}
.hdr-warn{border-color:var(--or);color:var(--or)}
.hdr-warn:hover{background:rgba(251,146,60,.1)}
.hdr-status{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--tx2);text-align:right;line-height:1.5}
.hdr-time{font-size:10px;opacity:0.7}
.hdr-right{display:flex;align-items:center;gap:16px;font-size:12px;font-family:'JetBrains Mono',monospace;color:var(--tx2)}
.poll-dot{width:10px;height:10px;border-radius:50%;display:inline-block;margin-right:4px}
.poll-on{background:var(--gn);box-shadow:0 0 8px var(--gn),0 0 16px rgba(52,211,153,.3);animation:pulse 2s infinite}.poll-off{background:var(--rd);box-shadow:0 0 6px var(--rd)}
.poll-wait{background:var(--yl);box-shadow:0 0 6px var(--yl)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.ctr{max-width:1600px;margin:0 auto;padding:20px 28px}
.bento{display:grid;gap:14px}
.bento-2{grid-template-columns:1.2fr 0.8fr}
.bento-4{grid-template-columns:repeat(4,1fr)}
.bento-2e{grid-template-columns:1fr 1fr}
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
.b-priced{background:rgba(79,140,255,.15);color:var(--ac)}
.b-completed{background:rgba(52,211,153,.15);color:var(--gn)}
.b-converted{background:rgba(52,211,153,.2);color:var(--gn)}
.b-parsed{background:rgba(251,191,36,.15);color:var(--yl)}
.b-won{background:rgba(52,211,153,.2);color:var(--gn)}
.b-lost{background:rgba(248,113,113,.15);color:var(--rd)}
.b-expired{background:rgba(139,144,160,.15);color:var(--tx2)}
.b-draft{background:rgba(251,191,36,.25);color:#f59e0b;border:1px solid rgba(251,191,36,.4)}
.home-tbl{width:100%;border-collapse:collapse;font-size:13px}
.home-tbl thead th{text-align:left;padding:8px 10px;font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--bd);font-weight:600}
.home-tbl tbody td{padding:10px;border-bottom:1px solid rgba(46,51,69,.5);vertical-align:middle}
.home-row{cursor:pointer;transition:background .12s}
.home-row:hover{background:rgba(79,140,255,.06)}
.home-row a{text-decoration:none}
.brief-item{display:flex;justify-content:space-between;align-items:flex-start;padding:8px 10px;border-radius:8px;transition:background .12s;margin-bottom:2px}
.brief-item:hover{background:var(--sf2)}
.brief-item-left{display:flex;gap:10px;align-items:flex-start;min-width:0}
.brief-icon{font-size:16px;flex-shrink:0;margin-top:1px}
.brief-title{font-size:13px;font-weight:500;line-height:1.4}
.brief-detail{font-size:11px;color:var(--tx2);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:280px}
.brief-age{font-size:10px;color:var(--tx2);font-family:'JetBrains Mono',monospace;white-space:nowrap;flex-shrink:0;margin-top:3px}
.brief-empty{font-size:12px;color:var(--tx2);padding:12px 10px;text-align:center;font-style:italic}
.brief-count{font-size:10px;padding:1px 7px;border-radius:10px;background:rgba(251,191,36,.2);color:#fbbf24;font-weight:600;font-family:'JetBrains Mono',monospace}
.stat-chip{background:var(--sf2);border:1px solid var(--bd);border-radius:8px;padding:8px 14px;min-width:85px;text-align:center}
.stat-val{font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1.2}
.stat-label{font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-top:3px}
.kpi-card{padding:16px 18px;text-align:center}
.kpi-card-label{font-size:10px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px}
.kpi-card-value{font-size:28px;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1.3;margin:4px 0}
.kpi-card-sub{font-size:11px;color:var(--tx2)}
.kpi-panel{min-height:120px}
.kpi-panel-title{font-size:11px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px}
.kpi-big{font-size:26px;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1}.funnel-link:hover{background:rgba(79,140,255,.08)!important}
.kpi-sub{font-size:10px;color:var(--tx2);font-family:'JetBrains Mono',monospace}
.progress-track{height:10px;background:var(--sf2);border-radius:5px;overflow:hidden}
.progress-fill{height:100%;border-radius:5px;transition:width .8s ease}
@media(max-width:768px){
 #kpi-cards{grid-template-columns:repeat(2,1fr)!important}
 #brief-grid{grid-template-columns:1fr!important}
}
@media(max-width:1200px){
 .pc-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch}
 .pc-table-wrap table{min-width:900px}
 .bento-2,.bento-2e{grid-template-columns:1fr}
 .bento-4{grid-template-columns:repeat(2,1fr)}
}
@media(max-width:900px){
 .hdr-bar{flex-wrap:wrap;gap:6px}
 .hdr-bar .nav-btn{font-size:11px;padding:4px 8px}
 .meta-g{grid-template-columns:1fr 1fr!important}
 .action-bar{flex-wrap:wrap;gap:6px}
 .action-bar .btn{font-size:12px;padding:5px 10px}
 .sidebar-cards{flex-direction:column!important}
 .sidebar-cards>div{width:100%!important;min-width:unset!important}
}
@media(max-width:600px){
 #kpi-cards{grid-template-columns:1fr!important}
 .meta-g{grid-template-columns:1fr!important}
 .hdr-bar{padding:6px 10px}
 body{padding:0 4px}
 .card{padding:10px}
 .action-bar{flex-direction:column}
 .action-bar .btn{width:100%}
 .bento-4{grid-template-columns:1fr}
 /* Feature 6: Core mobile improvements */
 .home-tbl thead{display:none}
 .home-tbl tr{display:block;border:1px solid var(--bd);border-radius:6px;margin-bottom:8px;padding:8px}
 .home-tbl td{display:flex;justify-content:space-between;padding:4px 0;border:none;font-size:13px}
 .home-tbl td:before{content:attr(data-label);color:var(--tx2);font-size:11px;text-transform:uppercase;letter-spacing:.3px}
 #pipeline-bar{flex-wrap:wrap;gap:6px}
 #pipeline-bar .bar-item{font-size:12px}
 .pc-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch}
 .pc-table-wrap table{min-width:700px}
 .quote-gen-banner{flex-direction:column!important;gap:8px!important}
 .quote-gen-banner button{width:100%}
 h1{font-size:18px}
 .hdr-bar .nav-btn{font-size:10px;padding:3px 6px}
}
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

/* ── Mobile Responsive (PRD Feature P1) ───────────────────────────── */
@media(max-width:768px){
  body{padding:10px!important;font-size:14px!important}
  .card{padding:12px!important}
  table{font-size:12px}
  th,td{padding:5px 6px!important}
  .btn{padding:8px 12px!important;font-size:13px!important}
  h1{font-size:18px!important}
  h2{font-size:16px!important}
  .pipeline-bar,.kpi-row{flex-direction:column!important;gap:8px!important}
  .nav-tabs,.tab-bar{overflow-x:auto;white-space:nowrap}
  .sidebar{display:none!important}
  .home-row td:nth-child(5),.home-row td:nth-child(6){display:none}
  .action-bar{flex-direction:column!important}
  #quote-gen-banner{flex-direction:column!important;gap:10px!important}
  .pc-table-wrap{overflow-x:auto}
  .modal-box{padding:16px!important;margin:0!important}
}
@media(max-width:480px){
  .btn-group{flex-direction:column;width:100%}
  .btn-group .btn{width:100%;text-align:center}
  h1{font-size:16px!important}
}

/* Notification bell */
.notif-bell{position:relative;cursor:pointer;padding:6px 10px;border-radius:6px;border:1px solid var(--bd);background:var(--sf2);color:var(--tx);display:inline-flex;align-items:center;gap:5px;font-size:13px;transition:.15s}
.notif-bell:hover{border-color:var(--ac);background:rgba(79,140,255,.1)}
.notif-badge{position:absolute;top:-6px;right:-6px;background:var(--rd);color:#fff;border-radius:10px;font-size:10px;font-weight:700;min-width:18px;height:18px;display:flex;align-items:center;justify-content:center;padding:0 4px;border:2px solid var(--bg);display:none}
.notif-badge.show{display:flex}
.notif-panel{position:fixed;top:76px;right:20px;width:360px;background:var(--sf);border:1px solid var(--bd);border-radius:12px;box-shadow:0 8px 32px rgba(0,0,0,.5);z-index:999;display:none;flex-direction:column;max-height:520px;overflow:hidden}
.notif-panel.open{display:flex}
.notif-panel-hdr{padding:14px 16px;border-bottom:1px solid var(--bd);display:flex;justify-content:space-between;align-items:center}
.notif-panel-hdr h4{font-size:13px;font-weight:600;color:var(--tx)}
.notif-panel-body{overflow-y:auto;flex:1}
.notif-item{padding:12px 16px;border-bottom:1px solid rgba(46,51,69,.5);cursor:pointer;transition:.1s}
.notif-item:hover{background:var(--sf2)}
.notif-item.unread{border-left:3px solid var(--ac)}
.notif-item.urgency-urgent{border-left:3px solid var(--rd)}
.notif-item.urgency-deal{border-left:3px solid var(--gn)}
.notif-item.urgency-warning{border-left:3px solid var(--yl)}
.notif-item-title{font-size:13px;font-weight:500;color:var(--tx);margin-bottom:3px}
.notif-item-body{font-size:11px;color:var(--tx2);line-height:1.4}
.notif-item-time{font-size:10px;color:var(--tx2);margin-top:4px;font-family:'JetBrains Mono',monospace}
.notif-empty{padding:32px;text-align:center;color:var(--tx2);font-size:13px}
.notif-footer{padding:10px 16px;border-top:1px solid var(--bd);display:flex;gap:8px}
"""

PAGE_HOME = """
<!-- ═══ Bar 1: Pipeline Funnel — full-width stat strip ═══ -->
<div class="card" style="margin-bottom:12px;padding:18px 24px">
 <div style="display:flex;align-items:center;gap:6px;margin-bottom:14px">
  <span style="font-size:11px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:1px">📊 Pipeline</span>
  <span style="flex:1;height:1px;background:var(--bd)"></span>
  <span id="next-quote-badge" style="font-size:11px;color:var(--tx2)">—</span>
  <span style="width:1px;height:14px;background:var(--bd);margin:0 8px"></span>
  <a href="/pipeline" style="font-size:11px;color:var(--ac);font-weight:600">View Full Pipeline →</a>
 </div>
 <div id="funnel-row" style="display:grid;grid-template-columns:repeat(7,1fr);gap:0">
  <!-- loading skeleton -->
  <div style="text-align:center;padding:10px 0"><div style="font-size:28px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--tx2)">—</div><div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-top:4px">Loading</div></div>
 </div>
</div>
<script>
fetch('/api/funnel/stats').then(r=>r.json()).then(d=>{
 if(!d.ok) return;
 const items = [
  {icon:'📥', val:d.rfqs_active||0, label:'RFQs', href:'/rfq/', color:'var(--ac)', fmt:'n'},
  {icon:'💰', val:d.quotes_pending||0, label:'Pending', href:'/quotes?status=pending', color:'var(--yl)', fmt:'n'},
  {icon:'📤', val:d.quotes_sent||0, label:'Sent', href:'/quotes?status=sent', color:'#a78bfa', fmt:'n'},
  {icon:'✅', val:d.quotes_won||0, label:'Won', href:'/quotes?status=won', color:'var(--gn)', fmt:'n'},
  {icon:'📦', val:d.orders_active||0, label:'Orders', href:'/orders', color:'var(--or)', fmt:'n'},
  {icon:'👥', val:d.crm_contacts||0, label:'Contacts', href:'/contacts', color:'#a78bfa', fmt:'n'},
  {icon:'💵', val:d.pipeline_value||0, label:'Pipeline $', href:'/pipeline', color:'var(--gn)', fmt:'$'},
 ];
 const cols = items.map((it,i)=>{
  const fmtVal = it.fmt==='$' ? '$'+(it.val>=1e6?(it.val/1e6).toFixed(1)+'M':it.val>=1e3?(it.val/1e3).toFixed(0)+'K':it.val.toLocaleString()) : it.val;
  const sep = i<items.length-1 ? '<div style="position:absolute;right:0;top:50%;transform:translateY(-50%);color:var(--bd);font-size:20px;font-weight:300">&rsaquo;</div>' : '';
  return '<a href="'+it.href+'" class="funnel-link" style="text-align:center;padding:10px 4px;display:block;text-decoration:none;border-radius:8px;transition:background .15s;position:relative">'+
   '<div style="font-size:18px;margin-bottom:4px">'+it.icon+'</div>'+
   '<div class="kpi-big" style="color:'+it.color+'">'+fmtVal+'</div>'+
   '<div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-top:5px;font-weight:600">'+it.label+'</div>'+
   sep+'</a>';
 });
 document.getElementById('funnel-row').style.gridTemplateColumns='repeat(7,1fr)';
 document.getElementById('funnel-row').innerHTML = cols.join('');
 if(d.next_quote){const nb=document.getElementById('next-quote-badge');if(nb){nb.innerHTML='';var b=document.createElement('b');b.className='kpi-small';b.style.color='var(--ac)';b.textContent=d.next_quote;nb.appendChild(document.createTextNode('🎯 Next: '));nb.appendChild(b);if(d.win_rate){nb.innerHTML+=' · Win Rate: <b style="color:var(--gn)">'+d.win_rate+'%</b>';};}}
}).catch(()=>{const nb=document.getElementById('next-quote-badge');if(nb)nb.textContent='';});
</script>

<!-- ═══ Bar 2: Annual Revenue Goal ═══ -->
<div id="rev-bi" class="card" style="margin-bottom:12px;padding:18px 24px;display:block">
 <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;flex-wrap:wrap;gap:8px">
  <span style="font-size:11px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:1px">📈 Annual Revenue Goal</span>
  <div style="display:flex;gap:20px;align-items:center" id="rev-meta">
   <span style="font-size:12px;color:var(--tx2)">Gap: <b id="rev-gap" style="color:var(--rd);font-family:'JetBrains Mono',monospace">$2,000,000</b></span>
   <span style="font-size:12px;color:var(--tx2)">Run Rate: <b id="rev-rate" style="font-family:'JetBrains Mono',monospace;color:var(--rd)">$0</b></span>
   <span style="font-size:12px;color:var(--tx2)">Status: <b id="rev-track" style="color:var(--rd)">Behind</b></span>
  </div>
 </div>
 <div style="display:flex;align-items:center;gap:16px">
  <span id="rev-closed" style="font-size:28px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--gn);white-space:nowrap">$0</span>
  <div style="flex:1">
   <div style="background:var(--sf2);border-radius:10px;height:28px;overflow:hidden;position:relative;border:1px solid var(--bd)">
    <div id="rev-bar" style="height:100%;border-radius:10px;transition:width .8s ease;background:var(--rd);min-width:2px"></div>
    <span id="rev-label" style="position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);font-size:12px;font-weight:700;color:#fff;white-space:nowrap;text-shadow:0 1px 3px rgba(0,0,0,.5)">$0 / $2M (0%)</span>
   </div>
  </div>
  <span style="font-size:14px;color:var(--tx2);white-space:nowrap">$2M Goal</span>
 </div>
</div>
<script>
fetch('/api/intel/revenue',{credentials:'same-origin'}).then(r=>r.json()).then(d=>{
 if(!d.ok) return;
 const pct = Math.min(100, d.pct_to_goal||0);
 const color = pct>=50?'#3fb950':pct>=25?'#d29922':'#f85149';
 const bar = document.getElementById('rev-bar');
 bar.style.width = Math.max(pct,0.5)+'%';
 bar.style.background = 'linear-gradient(90deg, '+color+', '+color+'aa)';
 document.getElementById('rev-label').textContent = '$'+(d.closed_revenue||0).toLocaleString()+' / $'+(d.goal/1e6).toFixed(0)+'M ('+pct.toFixed(1)+'%)';
 document.getElementById('rev-closed').textContent = '$'+(d.closed_revenue||0).toLocaleString();
 document.getElementById('rev-closed').style.color = color;
 document.getElementById('rev-gap').textContent = '$'+(d.gap_to_goal||0).toLocaleString();
 document.getElementById('rev-rate').textContent = '$'+(d.run_rate_annual||0).toLocaleString()+'/yr';
 document.getElementById('rev-rate').style.color = d.on_track?'#3fb950':'#f85149';
 document.getElementById('rev-track').textContent = d.on_track?'On Track ✅':'Behind 🔴';
 document.getElementById('rev-track').style.color = d.on_track?'#3fb950':'#f85149';
 document.getElementById('rev-gap').style.color = d.gap_to_goal>0?'#f85149':'#3fb950';
}).catch(()=>{});
</script>

<!-- ═══ Bar 3: Search + Quick Nav ═══ -->
<div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:14px">
 <form method="get" action="/search" style="display:flex;gap:0;flex:1;min-width:320px;background:var(--sf);border:1.5px solid var(--bd);border-radius:10px;overflow:hidden;transition:border-color .2s" onfocusin="this.style.borderColor='var(--ac)'" onfocusout="this.style.borderColor='var(--bd)'">
  <span style="padding:0 14px;font-size:18px;display:flex;align-items:center;color:var(--tx2)">🔍</span>
  <input name="q" placeholder="Search quotes, agencies, PO numbers, items, contacts..." style="flex:1;padding:14px 4px 14px 0;background:transparent;border:none;color:var(--tx);font-size:15px;outline:none" autocomplete="off">
  <button type="submit" style="padding:14px 22px;background:var(--ac);border:none;color:#fff;font-size:14px;font-weight:700;cursor:pointer;transition:.15s;letter-spacing:.3px" onmouseover="this.style.background='var(--ac2)'" onmouseout="this.style.background='var(--ac)'">Search</button>
 </form>
 <a href="/quotes" class="btn btn-s" style="padding:14px 18px;font-size:14px;font-weight:600;white-space:nowrap">📋 Quotes DB</a>
 <a href="/contacts" class="btn btn-s" style="padding:14px 18px;font-size:14px;font-weight:600;white-space:nowrap;border-color:rgba(167,139,250,.4);color:#a78bfa" onmouseover="this.style.borderColor='#a78bfa'" onmouseout="this.style.borderColor='rgba(167,139,250,.4)'">👥 CRM</a>
 <a href="/orders" class="btn btn-s" style="padding:14px 18px;font-size:14px;font-weight:600;white-space:nowrap">📦 Orders</a>
 <a href="/growth" class="btn btn-s" style="padding:14px 18px;font-size:14px;font-weight:600;white-space:nowrap">🚀 Growth</a>
 <a href="/agents" class="btn btn-s" style="padding:14px 18px;font-size:14px;font-weight:600;white-space:nowrap">🤖 Agents</a>
 <a href="/qa/workflow" id="qa-nav-btn" class="btn btn-s" style="padding:14px 18px;font-size:14px;font-weight:600;white-space:nowrap;display:none" title="QA Score">🔬 <span id="qa-nav-score">—</span></a>
</div>

<!-- Manager Brief — loads via AJAX -->
<div id="brief-section" class="card" style="margin-bottom:14px;display:none">
 <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px">
  <div>
   <div class="card-t" style="margin:0;display:flex;align-items:center;gap:8px">
    🧠 Manager Brief
    <span id="brief-badge" style="font-size:10px;padding:2px 8px;border-radius:10px;background:var(--sf2);color:var(--tx2);font-weight:500"></span>
   </div>
   <div id="brief-headline" style="font-size:15px;font-weight:600;margin-top:8px;line-height:1.4"></div>
  </div>
  <div style="display:flex;gap:6px;align-items:center">
   <a href="/agents" class="btn btn-sm btn-s" style="font-size:10px;padding:4px 10px;white-space:nowrap">📊 Full Report</a>
   <button onclick="loadBrief()" id="brief-refresh-btn" style="font-size:10px;padding:4px 8px;background:rgba(79,140,255,.1);border:1px solid rgba(79,140,255,.3);color:var(--ac);border-radius:6px;cursor:pointer;white-space:nowrap">🔄 Refresh</button>
  </div>
 </div>

 <!-- Two-column: Approvals | Activity -->
 <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px" id="brief-grid">
  <!-- Pending Approvals -->
  <div>
   <div style="font-size:11px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;display:flex;align-items:center;gap:6px">
    Needs Your Attention <span id="approval-count" class="brief-count"></span>
   </div>
   <div id="approvals-list"></div>
  </div>
  <!-- Activity Feed -->
  <div>
   <div style="font-size:11px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">
    Recent Activity
   </div>
   <div id="activity-list"></div>
  </div>
 </div>

 <!-- Pipeline Stats Bar -->
 <div id="pipeline-bar" style="display:flex;gap:12px;flex-wrap:wrap;margin-top:16px;padding-top:14px;border-top:1px solid var(--bd)"></div>

 <!-- Agent Health Row — prominent 3x sizing -->
 <div id="agents-row" style="display:none;margin-top:16px;padding-top:14px;border-top:1px solid var(--bd)">
  <div style="font-size:10px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Agent Status</div>
  <div id="agents-list" style="display:flex;gap:10px;flex-wrap:wrap"></div>
 </div>
</div>

<!-- ═══ QA Status Banner (hidden when all clear) ═══ -->
<div id="qa-banner" style="display:none;margin-bottom:10px;padding:10px 14px;border-radius:8px;border:1px solid rgba(248,113,113,.4);background:rgba(248,113,113,.06);align-items:center;gap:10px;font-size:13px">
 <span style="font-size:18px">⚠️</span>
 <div style="flex:1">
  <strong id="qa-banner-title" style="color:#f87171">Workflow tests failing</strong>
  <span id="qa-banner-detail" style="color:var(--tx2);margin-left:8px;font-size:12px"></span>
 </div>
 <a href="/qa/workflow" style="padding:4px 10px;background:rgba(248,113,113,.15);color:#f87171;border:1px solid rgba(248,113,113,.3);border-radius:6px;font-size:11px;font-weight:600;text-decoration:none;white-space:nowrap">View Tests →</a>
</div>

<!-- ═══ Work Queues — Primary Bento Row ═══ -->
<div class="bento bento-2" style="margin-bottom:14px">

 <!-- Price Checks — primary work queue (wider column) -->
 <div class="card" style="margin:0;overflow:hidden">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
   <div class="card-t" style="margin:0">Price Checks <span style="font-size:11px;font-weight:400;color:var(--tx2)">(AMS 704 — price your response)</span> <span style="font-family:'JetBrains Mono',monospace;font-weight:700">{{price_checks|length}}</span></div>
  </div>
  {% if price_checks %}
  <div style="overflow-x:auto;-webkit-overflow-scrolling:touch">
  <table class="home-tbl">
   <thead>
    <tr>
     <th>PC Number</th>
     <th>Institution</th>
     <th>Requestor</th>
     <th style="width:90px">Due</th>
     <th style="width:56px;text-align:center">Items</th>
     <th style="width:74px;text-align:center">Quote</th>
     <th style="width:80px;text-align:center">Status</th>
     <th style="width:32px"></th>
    </tr>
   </thead>
   <tbody>
    {% for id, pc in price_checks.items() %}
    <tr class="home-row" id="pc-row-{{id}}" onclick="location.href='/pricecheck/{{id}}'">
     <td><a href="/pricecheck/{{id}}" class="sol">#{{pc.pc_number or '(blank)'}}</a></td>
     <td style="font-weight:600">{{pc.institution or '—'}}</td>
     <td style="color:var(--tx2)">{{pc.requestor or '—'}}</td>
     <td class="mono">{{pc.due_date or '—'}}</td>
     <td style="text-align:center" class="mono">{{pc.get('items',[])|length}}</td>
     <td style="text-align:center" onclick="event.stopPropagation()">{% if pc.reytech_quote_number %}<span style="color:var(--gn);font-weight:600;font-family:'JetBrains Mono',monospace;font-size:12px">{{pc.reytech_quote_number}}</span>{% else %}—{% endif %}</td>
     <td style="text-align:center"><span class="badge b-{{pc.status}}">{{pc.status}}</span></td>
     <td style="text-align:center" onclick="event.stopPropagation()">
      <button onclick="deletePC('{{id}}',this)" title="Delete this Price Check" style="background:rgba(248,81,73,.08);border:1px solid rgba(248,81,73,.2);color:#f85149;cursor:pointer;font-size:10px;padding:2px 8px;border-radius:4px;font-weight:600" onmouseover="this.style.background='rgba(248,81,73,.2)'" onmouseout="this.style.background='rgba(248,81,73,.08)'">Delete</button>
     </td>
    </tr>
    {% endfor %}
   </tbody>
  </table>
  </div>
  {% else %}
  <div class="empty" style="padding:32px 16px">No Price Checks yet — upload a 704 or configure email polling</div>
  {% endif %}
 </div>

 <!-- RFQ Queue (narrower column) -->
 <div class="card" style="margin:0;overflow:hidden">
  <div class="card-t">RFQ Queue <span style="font-size:11px;font-weight:400;color:var(--tx2)">(704A/704B + formal quote package)</span> <span style="font-family:'JetBrains Mono',monospace;font-weight:700">{{rfqs|length}}</span></div>
  {% if rfqs %}
  <div style="overflow-x:auto;-webkit-overflow-scrolling:touch">
  <table class="home-tbl">
   <thead>
    <tr>
     <th>Solicitation</th>
     <th>Requestor</th>
     <th style="width:90px">Due</th>
     <th style="width:56px;text-align:center">Items</th>
     <th style="width:80px;text-align:center">Status</th>
    </tr>
   </thead>
   <tbody>
    {% for id, r in rfqs|dictsort(reverse=true) %}
    <tr class="home-row" onclick="location.href='/rfq/{{id}}'" style="{% if r.status in ('sent','generated') %}opacity:0.55{% endif %}">
     <td><a href="/rfq/{{id}}" class="sol">#{{r.solicitation_number}}</a></td>
     <td style="font-weight:600">{{r.requestor_name}}</td>
     <td class="mono">{{r.due_date}}</td>
     <td style="text-align:center" class="mono">{{r.line_items|length}}</td>
     <td style="text-align:center"><span class="badge b-{{r.status}}">{{r.status}}</span></td>
    </tr>
    {% endfor %}
   </tbody>
  </table>
  </div>
  {% else %}
  <div class="empty" style="padding:32px 16px">No RFQs — configure email polling or upload below</div>
  {% endif %}
 </div>

</div>

<!-- ═══ KPI Dashboard — Bento analytics ═══ -->
<div id="kpi-section" style="display:none">
 <!-- Row 1: Big KPI cards -->
 <div class="bento bento-4" style="margin-bottom:14px" id="kpi-cards"></div>
 <!-- Row 2: Goal + Funnel side by side -->
 <div class="bento bento-2e" style="margin-bottom:14px">
  <div class="card kpi-panel" style="padding:16px;margin:0">
   <div class="kpi-panel-title">Monthly Revenue Goal</div>
   <div style="display:flex;align-items:flex-end;gap:12px;margin:12px 0 8px">
    <span id="goal-current" class="kpi-big" style="color:var(--gn)">$0</span>
    <span style="font-size:13px;color:var(--tx2)">of <span id="goal-target">$25,000</span></span>
   </div>
   <div class="progress-track"><div class="progress-fill" id="goal-bar" style="width:0%"></div></div>
   <div style="display:flex;justify-content:space-between;margin-top:6px">
    <span class="kpi-sub" id="goal-pct">0%</span>
    <span class="kpi-sub" id="goal-remaining">$25,000 remaining</span>
   </div>
  </div>
  <div class="card kpi-panel" style="padding:16px;margin:0">
   <div class="kpi-panel-title">Pipeline Funnel</div>
   <div id="funnel-bars" style="margin-top:12px"></div>
  </div>
 </div>
 <!-- Row 3: Weekly volume + Top institutions -->
 <div class="bento bento-2e" style="margin-bottom:14px">
  <div class="card kpi-panel" style="padding:16px;margin:0">
   <div class="kpi-panel-title">Weekly Quote Volume</div>
   <div id="weekly-chart" style="display:flex;align-items:flex-end;gap:8px;height:100px;margin-top:12px;padding-top:8px"></div>
  </div>
  <div class="card kpi-panel" style="padding:16px;margin:0">
   <div class="kpi-panel-title">Top Institutions by Revenue</div>
   <div id="top-inst" style="margin-top:12px"></div>
  </div>
 </div>
</div>

<!-- Manual Upload — collapsed fallback -->
<details class="card" style="cursor:default">
 <summary style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;cursor:pointer;list-style:none;display:flex;align-items:center;gap:8px">
  <span style="font-size:10px;transition:transform .2s" class="upload-arrow">▶</span>
  Manual Upload
  <span style="font-weight:400;font-size:11px;color:var(--tx2);margin-left:auto">Use if email automation doesn't catch it</span>
 </summary>
 <form method="POST" action="/upload" enctype="multipart/form-data" id="uf" style="margin-top:14px">
  <div class="upl" id="dz" onclick="document.getElementById('fi').click()" style="padding:24px;border-width:1px">
   <h3 style="font-size:14px;margin-bottom:2px">Drop PDF here</h3>
   <p style="font-size:12px">AMS 704 Price Check or RFQ (703B, 704B, Bid Package)</p>
   <input type="file" id="fi" data-testid="upload-file-input" name="files" multiple accept=".pdf" style="display:none">
  </div>
 </form>
</details>

<script>
// ── Upload drag/drop ──
const dz=document.getElementById('dz'),fi=document.getElementById('fi'),uf=document.getElementById('uf');
if(dz){
 ['dragover','dragenter'].forEach(e=>dz.addEventListener(e,ev=>{ev.preventDefault();dz.style.borderColor='var(--ac)'}));
 ['dragleave','drop'].forEach(e=>dz.addEventListener(e,ev=>{ev.preventDefault();dz.style.borderColor='var(--bd)'}));
 dz.addEventListener('drop',ev=>{fi.files=ev.dataTransfer.files;uf.submit()});
 fi.addEventListener('change',()=>{if(fi.files.length)uf.submit()});
}
// ── Details arrow ──
document.querySelectorAll('details').forEach(d=>{
 d.addEventListener('toggle',()=>{
  var arr=d.querySelector('.upload-arrow');
  if(arr) arr.style.transform=d.open?'rotate(90deg)':'rotate(0)';
 });
});

    // ── Clear stale quote number from PC ──────────────────────────────────
    function clearPCQuote(id, btn) {
      if (!confirm('Clear the stale quote number from this PC? The PC will return to "parsed" status so you can generate a fresh quote.')) return;
      btn.textContent = '⏳';
      fetch('/api/pricecheck/' + id + '/clear-quote', {method: 'POST', credentials: 'same-origin'})
        .then(r => r.json())
        .then(d => {
          if (d.ok) {
            // Reload to show updated state
            location.reload();
          } else {
            btn.textContent = '✕';
            alert('Error: ' + (d.error || 'unknown'));
          }
        })
        .catch(() => { btn.textContent = '✕'; });
    }

    // ── Delete Price Check ─────────────────────────────────────────────────
    function deletePC(id, btn) {
      if (!confirm('Are you sure you want to delete this Price Check?')) return;
      if (!confirm('This will permanently remove the PC, its linked draft quote, and reclaim the quote number. Continue?')) return;
      btn.textContent = '⏳';
      btn.disabled = true;
      fetch('/api/pricecheck/' + id + '/delete', {method: 'POST', credentials: 'same-origin'})
        .then(r => r.json())
        .then(d => {
          if (d.ok) {
            const row = document.getElementById('pc-row-' + id);
            if (row) row.remove();
            // Update count
            const hdr = document.querySelector('.card-t');
            if (hdr && hdr.textContent.includes('Price Checks')) {
              const remaining = document.querySelectorAll('[id^=pc-row-]').length;
              hdr.textContent = 'Price Checks (' + remaining + ')';
            }
            // Show what was cleaned up
            let msg = '✅ Price Check deleted.';
            if (d.quote_removed) msg += '\\nQuote ' + d.quote_removed + ' removed.';
            if (d.counter_reset) msg += '\\nQuote counter adjusted — ' + d.counter_reset;
            alert(msg);
          } else {
            btn.textContent = 'Delete';
            btn.disabled = false;
            alert('Delete failed: ' + (d.error || 'unknown'));
          }
        })
        .catch(() => { btn.textContent = 'Delete'; btn.disabled = false; });
    }


// ── Manager Brief (loads async) ──
function loadBrief() {
  var btn = document.getElementById('brief-refresh-btn');
  if(btn) { btn.disabled=true; btn.textContent='⏳'; }
fetch('/api/manager/brief',{credentials:'same-origin'}).then(function(r){
 if(!r.ok) throw new Error('HTTP '+r.status);
 return r.json();
}).then(function(data){
 if(!data.ok) return;
 var sec=document.getElementById('brief-section');
 sec.style.display='block';

 // Headline
 document.getElementById('brief-headline').textContent=data.headline||'All clear';

 // Badge
 var badge=document.getElementById('brief-badge');
 if(data.approval_count>0){
  badge.textContent=data.approval_count+' pending';
  badge.style.background='rgba(251,191,36,.15)';badge.style.color='#fbbf24';
 } else {
  badge.textContent='all clear';
  badge.style.background='rgba(52,211,153,.15)';badge.style.color='#34d399';
 }

 // Approval count
 var ac=document.getElementById('approval-count');
 if(data.approval_count>0){
  ac.textContent=data.approval_count;
 }

 // Approvals list
 var al=document.getElementById('approvals-list');
 if(data.pending_approvals && data.pending_approvals.length>0){
  al.innerHTML=data.pending_approvals.map(function(a){
   return '<div class="brief-item">'
    +'<div class="brief-item-left">'
    +'<span class="brief-icon">'+a.icon+'</span>'
    +'<div><div class="brief-title">'+a.title+'</div>'
    +(a.detail?'<div class="brief-detail">'+a.detail+'</div>':'')
    +'</div></div>'
    +(a.age?'<span class="brief-age">'+a.age+'</span>':'')
    +'</div>';
  }).join('');
 } else {
  al.innerHTML='<div class="brief-empty">Nothing pending — all caught up</div>';
 }

 // Activity feed
 var actList=document.getElementById('activity-list');
 if(data.activity && data.activity.length>0){
  actList.innerHTML=data.activity.map(function(a){
   return '<div class="brief-item">'
    +'<div class="brief-item-left">'
    +'<span class="brief-icon">'+a.icon+'</span>'
    +'<div><div class="brief-title">'+a.text+'</div>'
    +(a.detail?'<div class="brief-detail">'+a.detail+'</div>':'')
    +'</div></div>'
    +(a.age?'<span class="brief-age">'+a.age+'</span>':'')
    +'</div>';
  }).join('');
 } else {
  actList.innerHTML='<div class="brief-empty">No recent activity</div>';
 }

 // Pipeline stats bar
 var bar=document.getElementById('pipeline-bar');
 var s=data.summary||{};
 var pc=s.price_checks||{};var q=s.quotes||{};var l=s.leads||{};var ob=s.outbox||{};
 var gr=s.growth||{};var rv=data.revenue||{};var ag=data.agents_summary||{};
 var stats=[
  {label:'Quotes',value:q.total||0,color:'var(--ac)'},
  {label:'Pipeline $',value:'$'+(q.pipeline_value||0).toLocaleString(),color:'var(--yl)'},
  {label:'Won $',value:'$'+(q.won_total||0).toLocaleString(),color:'var(--gn)'},
  {label:'Win Rate',value:(q.win_rate||0)+'%',color:q.win_rate>=50?'var(--gn)':'var(--yl)'},
  {label:'Growth',value:gr.total_prospects||0,color:'#bc8cff'},
  {label:'Agents',value:(ag.healthy||0)+'/'+(ag.total||0),color:ag.down>0?'var(--rd)':'var(--gn)'},
  {label:'Goal',value:rv.pct?rv.pct.toFixed(0)+'%':'0%',color:rv.pct>=50?'var(--gn)':rv.pct>=25?'var(--yl)':'var(--rd)'},
  {label:'Drafts',value:ob.drafts||0,color:ob.drafts>0?'var(--yl)':'var(--tx2)'},
 ];
 bar.innerHTML=stats.map(function(s){
  return '<div class="stat-chip">'
   +'<div class="stat-val" style="color:'+s.color+'">'+s.value+'</div>'
   +'<div class="stat-label">'+s.label+'</div></div>';
 }).join('');

 // Agent health row
 if(data.agents && data.agents.length>0){
  document.getElementById('agents-row').style.display='block';
  document.getElementById('agents-list').innerHTML=data.agents.map(function(a){
   var isOk=a.status==='active'||a.status==='ready'||a.status==='connected';
   var isWait=a.status==='not configured'||a.status==='waiting';
   var color=isOk?'#3fb950':isWait?'#d29922':'#f85149';
   var bg=isOk?'rgba(52,211,153,.08)':isWait?'rgba(251,191,36,.08)':'rgba(248,113,113,.08)';
   var border=isOk?'rgba(52,211,153,.25)':isWait?'rgba(251,191,36,.25)':'rgba(248,113,113,.25)';
   return '<span style="font-size:13px;padding:8px 14px;border-radius:8px;background:'+bg+';border:1px solid '+border+';display:inline-flex;align-items:center;gap:7px;font-weight:500">'
    +'<span style="width:10px;height:10px;border-radius:50%;background:'+color+';display:inline-block;flex-shrink:0;box-shadow:0 0 6px '+color+'66"></span>'
    +'<span style="font-size:15px">'+a.icon+'</span>'
    +'<span>'+a.name+'</span>'
    +'</span>';
  }).join('');
 }
}).catch(function(err){
 console.error('Manager brief failed:',err);
 var sec=document.getElementById('brief-section');
 sec.style.display='block';
 var badge=document.getElementById('brief-badge');
 badge.textContent='retry';
 badge.style.background='rgba(251,191,36,.15)';
 badge.style.color='#fbbf24';
 badge.style.cursor='pointer';
 badge.onclick=function(){loadBrief();};
 var hdr=document.getElementById('brief-headline');
 hdr.textContent='Dashboard loaded — click Refresh to reload brief';
 hdr.style.color='#8b949e';
}).finally(function(){
 var btn=document.getElementById('brief-refresh-btn');
 if(btn){btn.disabled=false;btn.textContent='🔄 Refresh';}
});
}
loadBrief();

// ── QA Status Banner (surfaces failures to main dashboard) ──
(function loadQABanner(){
  fetch('/api/qa/workflow/latest',{credentials:'same-origin'}).then(function(r){return r.json();}).then(function(d){
    var report=d&&d.full_report?d.full_report:d;
    if(!report||!report.summary)return;
    var failed=report.summary.failed||0;
    var warned=report.summary.warned||0;
    var score=report.score||0;
    var banner=document.getElementById('qa-banner');
    // Update nav QA button
    var navBtn=document.getElementById('qa-nav-btn');
    var navScore=document.getElementById('qa-nav-score');
    if(navBtn&&navScore){
      navScore.textContent=score+'/100';
      navBtn.style.display='';
      if(failed>0){navBtn.style.borderColor='rgba(248,113,113,.5)';navBtn.style.color='#f87171';}
      else if(score<80){navBtn.style.borderColor='rgba(251,191,36,.5)';navBtn.style.color='#fbbf24';}
      else{navBtn.style.borderColor='rgba(52,211,153,.3)';navBtn.style.color='#34d399';}
    }
    if(!banner)return;
    if(failed>0){
      banner.style.display='flex';
      document.getElementById('qa-banner-title').textContent=failed+' workflow test'+(failed>1?'s':'')+' failing';
      var cf=(report.critical_failures||[]).slice(0,2).map(function(f){return f.message||f;});
      document.getElementById('qa-banner-detail').textContent=cf.join(' · ').substring(0,120);
    } else if(score<70&&warned>0){
      banner.style.display='flex';
      banner.style.borderColor='rgba(251,191,36,.4)';
      banner.style.background='rgba(251,191,36,.04)';
      document.getElementById('qa-banner-title').style.color='#fbbf24';
      document.getElementById('qa-banner-title').textContent=warned+' warning'+(warned>1?'s':'')+' — score '+score+'/100';
      var anchor=banner.querySelector('a');if(anchor){anchor.style.color='#fbbf24';anchor.style.borderColor='rgba(251,191,36,.3)';anchor.style.background='rgba(251,191,36,.1)';}
    } else {
      banner.style.display='none';
    }
  }).catch(function(){});
})();
fetch('/api/manager/metrics',{credentials:'same-origin'}).then(function(r){
 if(!r.ok) throw new Error('HTTP '+r.status);
 return r.json();
}).then(function(data){
 if(!data.ok) return;
 document.getElementById('kpi-section').style.display='block';
 var rev=data.revenue||{};var q=data.quotes||{};var fn=data.funnel||{};

 // Big KPI cards
 var cards=[
  {label:'Total Revenue',value:'$'+(rev.total||0).toLocaleString(undefined,{maximumFractionDigits:0}),color:'var(--gn)',sub:q.won+' quotes won'},
  {label:'Win Rate',value:(q.win_rate||0)+'%',color:q.win_rate>=50?'var(--gn)':q.win_rate>0?'var(--yl)':'var(--tx2)',sub:(q.won+q.lost)+' decided'},
  {label:'Pipeline Value',value:'$'+(rev.pipeline_value||0).toLocaleString(undefined,{maximumFractionDigits:0}),color:'var(--ac)',sub:q.pending+' quotes pending'},
  {label:'Avg Response',value:(data.response_time_hours||0)+'h',color:data.response_time_hours<24?'var(--gn)':data.response_time_hours<48?'var(--yl)':'var(--rd)',sub:'PC upload → priced'},
 ];
 document.getElementById('kpi-cards').innerHTML=cards.map(function(c){
  return '<div class="card kpi-card"><div class="kpi-card-label">'+c.label+'</div>'
   +'<div class="kpi-card-value" style="color:'+c.color+'">'+c.value+'</div>'
   +'<div class="kpi-card-sub">'+c.sub+'</div></div>';
 }).join('');

 // Goal progress
 document.getElementById('goal-current').textContent='$'+(rev.this_month||0).toLocaleString(undefined,{maximumFractionDigits:0});
 document.getElementById('goal-target').textContent='$'+(rev.monthly_goal||25000).toLocaleString(undefined,{maximumFractionDigits:0});
 document.getElementById('goal-pct').textContent=(rev.goal_pct||0)+'%';
 var remaining=Math.max((rev.monthly_goal||25000)-(rev.this_month||0),0);
 document.getElementById('goal-remaining').textContent='$'+remaining.toLocaleString(undefined,{maximumFractionDigits:0})+' remaining';
 var bar=document.getElementById('goal-bar');
 bar.style.width=Math.min(rev.goal_pct||0,100)+'%';
 bar.style.background=rev.goal_pct>=100?'var(--gn)':rev.goal_pct>=50?'var(--ac)':'var(--yl)';

 // Funnel
 var funnelData=[
  {label:'Price Checks',value:fn.pcs_total||0,color:'var(--ac)'},
  {label:'Priced',value:fn.priced||0,color:'var(--yl)'},
  {label:'Completed',value:fn.completed||0,color:'var(--or)'},
  {label:'Quoted',value:fn.quotes_generated||0,color:'var(--ac)'},
  {label:'Won',value:fn.quotes_won||0,color:'var(--gn)'},
 ];
 var fMax=Math.max.apply(null,funnelData.map(function(f){return f.value}))||1;
 document.getElementById('funnel-bars').innerHTML=funnelData.map(function(f){
  var pct=Math.max(f.value/fMax*100,4);
  return '<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px">'
   +'<div style="width:80px;font-size:11px;color:var(--tx2);text-align:right">'+f.label+'</div>'
   +'<div style="flex:1;height:20px;background:var(--sf2);border-radius:4px;overflow:hidden">'
   +'<div style="height:100%;width:'+pct+'%;background:'+f.color+';border-radius:4px;transition:width .6s"></div></div>'
   +'<div style="width:30px;font-size:12px;font-weight:600;font-family:JetBrains Mono,monospace">'+f.value+'</div></div>';
 }).join('');

 // Weekly chart (bar chart)
 var wv=data.weekly_volume||[];
 var wMax=Math.max.apply(null,wv.map(function(w){return w.quotes}))||1;
 document.getElementById('weekly-chart').innerHTML=wv.map(function(w){
  var h=Math.max(w.quotes/wMax*80,4);
  return '<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:4px">'
   +'<div style="font-size:10px;font-family:JetBrains Mono,monospace;color:var(--tx2)">'+w.quotes+'</div>'
   +'<div style="width:100%;height:'+h+'px;background:var(--ac);border-radius:4px 4px 0 0;transition:height .4s"></div>'
   +'<div style="font-size:9px;color:var(--tx2)">'+w.label+'</div></div>';
 }).join('');

 // Top institutions
 var ti=data.top_institutions||[];
 if(ti.length>0){
  var tiMax=ti[0].revenue||1;
  document.getElementById('top-inst').innerHTML=ti.map(function(t){
   var pct=Math.max(t.revenue/tiMax*100,4);
   return '<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px">'
    +'<div style="min-width:120px;max-width:160px;font-size:11px;color:var(--tx);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+t.name+'</div>'
    +'<div style="flex:1;height:16px;background:var(--sf2);border-radius:4px;overflow:hidden">'
    +'<div style="height:100%;width:'+pct+'%;background:var(--gn);border-radius:4px;transition:width .6s"></div></div>'
    +'<div style="width:70px;font-size:11px;font-weight:600;font-family:JetBrains Mono,monospace;text-align:right;color:var(--gn)">$'+t.revenue.toLocaleString(undefined,{maximumFractionDigits:0})+'</div></div>';
  }).join('');
 } else {
  document.getElementById('top-inst').innerHTML='<div class="brief-empty">Win quotes to see top institutions here</div>';
 }
}).catch(function(err){ console.error('Manager metrics failed:',err); });
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
   <th style="min-width:200px">Item Link</th>
   <th>Your Cost</th><th>SCPRS</th><th>Amazon</th><th>Bid Price</th><th>Subtotal</th><th>Margin</th><th>Profit</th>
  </tr></thead>
  <tbody>
  {% for i in r.line_items %}
  <tr>
   <td>{{i.line_number}}</td>
   <td style="white-space:nowrap">{{i.qty}} {{i.uom}}</td>
   <td style="max-width:220px;font-size:12px"><input type="text" name="desc_{{loop.index0}}" value="{{i.description.split('\n')[0]}}" class="text-in" style="width:100%;font-size:12px" title="{{i.description}}"></td>
   <td class="mono" style="font-size:11px">{{i.item_number}}</td>
   <td style="min-width:200px">
    <div style="display:flex;align-items:center;gap:4px">
     <input type="text" name="link_{{loop.index0}}" value="{{i.get('item_link','')}}" placeholder="Paste supplier URL..." class="text-in" style="width:100%;font-size:12px;color:#58a6ff" oninput="handleRfqLinkInput({{loop.index0}}, this)">
     {% if i.get('item_supplier') %}<span style="font-size:10px;color:#8b949e;white-space:nowrap">{{i.item_supplier}}</span>{% endif %}
    </div>
   </td>
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
 fetch('/api/scprs/{{rid}}',{credentials:'same-origin'}).then(r=>r.json()).then(d=>{
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
 fetch('/api/research/rfq/{{rid}}',{credentials:'same-origin'}).then(r=>r.json()).then(d=>{
  if(!d.ok){btn.textContent='❌ '+d.message;btn.disabled=false;return;}
  // Poll for results
  const poll=setInterval(()=>{
   fetch('/api/research/status',{credentials:'same-origin'}).then(r=>r.json()).then(s=>{
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
                         del_sel, next_quote_preview="", today_date="",
                         profit_summary_json="null"):
    """Build the Price Check detail page HTML.
    
    Extracted from dashboard.py to keep the main module lean.
    All parameters are pre-computed by the route handler.
    """
    # Build pipeline status tracker
    _status = pc.get('status', 'parsed')
    _steps = [
        ('parsed', '📥', 'Parsed'),
        ('priced', '💰', 'Priced'),
        ('completed', '📄', '704 Filled'),
    ]
    _reached = {'parsed': 0, 'priced': 1, 'completed': 2, 'converted': 2}.get(_status, 0)
    _pip_parts = []
    for i, (step, icon, label) in enumerate(_steps):
        if i <= _reached:
            style = "padding:4px 10px;border-radius:6px;background:rgba(52,211,153,.12);color:#3fb950"
        else:
            style = "padding:4px 10px;border-radius:6px;background:#21262d;color:#484f58"
        _pip_parts.append(f"<span style=\"{style}\">{icon} {label}</span>")
        if i < len(_steps) - 1:
            _pip_parts.append("<span style=\"color:#484f58;margin:0 4px\">→</span>")
    pipeline_html = "".join(_pip_parts)

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
     @keyframes slideIn{{from{{opacity:0;transform:translateY(-8px)}}to{{opacity:1;transform:translateY(0)}}}}
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
     <a href="/agents" style="color:#fff;font-size:13px;text-decoration:none;padding:4px 10px;background:#21262d;border:1px solid #30363d;border-radius:6px">🤖 Agents</a>
    </div>

    <!-- Preview Modal -->
    <div class="modal-overlay" id="previewModal">
     <div class="modal-content">
      <div class="modal-header">
       <h2>📋 Preview — <span id="previewFormType">AMS 704 Price Check</span></h2>
       <div>
        <button class="btn btn-sm btn-g" onclick="window.print()" style="margin-right:8px;font-size:12px">🖨️ Print</button>
        <button class="modal-close" onclick="closePreview()">×</button>
       </div>
      </div>
      <div class="modal-body" id="previewBody"></div>
     </div>
    </div>

    <div class="card">
     <h1 style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
      <span>Price Check #</span>
      <span id="pcNumDisplay" style="cursor:pointer" title="Click to rename" onclick="document.getElementById('pcNumEdit').style.display='inline-flex';this.style.display='none'">{pc.get('pc_number','unknown')}</span>
      <span id="pcNumEdit" style="display:none;align-items:center;gap:4px">
       <input type="text" id="pcNumInput" value="{pc.get('pc_number','')}" style="background:#0d1117;border:1px solid #58a6ff;color:#c9d1d9;padding:4px 8px;border-radius:4px;font-size:16px;font-weight:700;width:200px;font-family:inherit">
       <button onclick="renamePc()" style="background:#238636;color:#fff;border:none;padding:4px 10px;border-radius:4px;font-size:12px;cursor:pointer">Save</button>
       <button onclick="document.getElementById('pcNumEdit').style.display='none';document.getElementById('pcNumDisplay').style.display='inline'" style="background:none;border:1px solid #30363d;color:#8b949e;padding:4px 8px;border-radius:4px;font-size:12px;cursor:pointer">✕</button>
      </span>
      <span class="status status-{pc.get('status','parsed')}">{pc.get('status','parsed').upper()}</span>
      {f"<span style='margin-left:12px;font-family:JetBrains Mono,monospace;font-size:16px;color:#58a6ff;font-weight:700'>{pc.get('reytech_quote_number','')}</span>" if pc.get('reytech_quote_number') else ""}</h1>

     <!-- Pipeline Status Tracker -->
     <div style="margin:12px 0;display:flex;align-items:center;gap:0;font-size:12px;font-weight:600">
      {pipeline_html}
     </div>

     {"<div style='padding:10px 16px;border-radius:8px;font-size:13px;display:flex;align-items:center;gap:10px;background:rgba(52,211,153,.08);border:1px solid rgba(52,211,153,.25);color:#3fb950;margin-bottom:10px'><span style=font-size:18px>✅</span><div><b>704 Complete</b> — Download below or re-fill if prices changed.</div></div>" if pc.get('status') in ('completed','converted') else "<div style='padding:10px 16px;border-radius:8px;font-size:13px;display:flex;align-items:center;gap:10px;background:rgba(88,166,255,.08);border:1px solid rgba(88,166,255,.25);color:#58a6ff;margin-bottom:10px'><span style=font-size:18px>💰</span><div><b>Priced</b> — review costs below, then Save & Fill 704.</div></div>" if pc.get('status') == 'priced' else "<div style='padding:10px 16px;border-radius:8px;font-size:13px;display:flex;align-items:center;gap:10px;background:rgba(210,153,34,.08);border:1px solid rgba(210,153,34,.25);color:#d29922;margin-bottom:10px'><span style=font-size:18px>📥</span><div><b>Parsed</b> — awaiting pricing agent. Click Process to run manually.</div></div>" if pc.get('status') == 'parsed' else ""}

     <div class="meta" style="margin-top:8px">
      <b>Institution:</b> {header.get('institution',pc.get('institution',''))} &nbsp;|&nbsp;
      <b>Requestor:</b> {header.get('requestor',pc.get('requestor',''))} &nbsp;|&nbsp;
      <b>Due:</b> {pc.get('due_date','')} <span id="dueUrgency"></span> &nbsp;|&nbsp;
      <b>Ship to:</b> {pc.get('ship_to','')} &nbsp;|&nbsp;
      <b>Today:</b> {today_date}
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

     <!-- Actions: Save + Preview + Fill/Download -->
     <!-- 1-click Generate Quote banner (PRD Feature 3.2.1) -->
     <div id="quote-gen-banner" style="background:linear-gradient(135deg,#1a3a5c,#0d2137);border:1px solid #1f6feb;border-radius:10px;padding:14px 20px;margin-bottom:10px;display:flex;align-items:center;justify-content:space-between;gap:16px">
      <div>
       <div style="font-size:13px;font-weight:700;color:#58a6ff;margin-bottom:3px">🎯 Generate Reytech Quote</div>
       <div style="font-size:12px;color:#8b949e" id="quote-gen-meta">Prices saved → 1 click to PDF + logged to CRM + DB</div>
      </div>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
       <span style="font-size:11px;color:#3fb950;font-family:monospace" id="next-qn-badge">Next: {next_quote_preview or '—'}</span>
       <button class="btn" id="genQuoteBtn" data-testid="pc-generate-quote-1click" onclick="generateQuote1Click(this)" style="background:#1f6feb;color:#fff;font-weight:700;font-size:14px;padding:10px 24px;border-radius:8px;transition:all .2s;white-space:nowrap">📋 Generate Quote →</button>
      </div>
     </div>

     <div class="action-bar" style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap;align-items:center" id="actionBar">
      <button class="btn btn-p" onclick="savePrices(this)" id="saveBtn" style="font-size:14px;padding:8px 20px">💾 Save</button>
      <button class="btn" onclick="showPreview()" style="background:#21262d;color:#c9d1d9;border:1px solid #484f58;font-size:14px;padding:8px 20px">👁️ Preview</button>
      {"" if pc.get('status') in ('completed','converted') else "<button class='btn btn-g' id='submitBtn' onclick='saveAndGenerate(this)' style='font-size:14px;padding:8px 20px'>📄 Save &amp; Fill 704</button>"}
      {"<button class='btn' data-testid='pc-auto-process' style='background:#f0883e;color:#fff;font-size:14px;padding:8px 20px' onclick='autoProcess(this)'>⚡ Process Now</button>" if pc.get('status') == 'parsed' else ""}
      {download_html}
      <details style="position:relative;display:inline-block">
       <summary class="btn btn-sm" style="background:#21262d;color:#8b949e;border:1px solid #30363d;font-size:12px;padding:4px 10px;cursor:pointer;list-style:none">⋯ More</summary>
       <div style="position:absolute;top:100%;left:0;z-index:50;margin-top:4px;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:6px;min-width:220px;box-shadow:0 8px 24px rgba(0,0,0,.4)">
        <button class="btn btn-sm" style="width:100%;text-align:left;background:none;color:#c9d1d9;border:none;padding:8px 10px;font-size:12px;border-radius:4px" onmouseover="this.style.background='#21262d'" onmouseout="this.style.background='none'" onclick="runScprs(this)" data-testid="pc-scprs-lookup">🔍 Re-run SCPRS Lookup</button>
        <button class="btn btn-sm" style="width:100%;text-align:left;background:none;color:#c9d1d9;border:none;padding:8px 10px;font-size:12px;border-radius:4px" onmouseover="this.style.background='#21262d'" onmouseout="this.style.background='none'" onclick="runLookup(this)" data-testid="pc-amazon-lookup">🔬 Re-run Amazon Lookup</button>
        {"<button class='btn btn-sm' style='width:100%;text-align:left;background:none;color:#c9d1d9;border:none;padding:8px 10px;font-size:12px;border-radius:4px' onmouseover=\"this.style.background='#21262d'\" onmouseout=\"this.style.background='none'\" onclick='saveAndGenerate(this)'>♻️ Re-fill 704</button>" if pc.get('status') in ('completed','converted') else ""}
        <button class="btn btn-sm" style="width:100%;text-align:left;background:none;color:#c9d1d9;border:none;padding:8px 10px;font-size:12px;border-radius:4px" onmouseover="this.style.background='#21262d'" onmouseout="this.style.background='none'" onclick="window.print()">🖨️ Print Page</button>
       </div>
      </details>
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
     <div class="pc-table-wrap">
     <table id="itemsTable">
      <tr><th style="width:28px">Bid</th><th>#</th><th>Qty</th><th>UOM</th><th style="min-width:280px">Description</th><th style="min-width:220px">Item Link</th><th>SCPRS $</th><th>Amazon $</th><th>Amazon Match</th><th>Unit Cost</th><th>Markup</th><th>Our Price</th><th>Extension</th><th>Profit</th><th>Conf</th></tr>
      {items_html}
     </table>
     </div>
     <div style="margin-top:8px">
      <button class="btn btn-sm" style="background:#21262d;color:#8b949e;border:1px solid #30363d" onclick="addRow()">+ Add Item</button>
     </div>

     <!-- PRD Feature 10: Price History Intelligence Panel -->
     <div id="price-history-panel" style="margin-top:16px">
       <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
         <span style="font-size:13px;font-weight:700;color:#8b949e;text-transform:uppercase;letter-spacing:.5px">📊 Price History Intelligence</span>
         <span style="font-size:11px;color:#484f58">Historical prices from all prior quotes & lookups</span>
         <button onclick="loadPriceHistory()" id="phLoadBtn" class="btn btn-sm" style="background:#21262d;color:#8b949e;border:1px solid #30363d;margin-left:auto;font-size:11px">Load History</button>
       </div>
       <div id="ph-content" style="display:none">
         <div id="ph-rows"></div>
       </div>
     </div>

     <div class="totals" id="totals"></div>
     <div id="server-profit-panel" style="display:none;background:#21262d;border:1px solid #30363d;border-radius:8px;padding:12px 16px;margin-top:8px;text-align:right"></div>
     <div style="margin-top:12px;display:flex;align-items:center;gap:20px;font-size:14px">
      <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
       <input type="checkbox" id="taxToggle" onchange="recalcPC()" style="width:18px;height:18px">
       <span>Include CA Sales Tax</span>
       <span id="taxRateDisplay" style="color:#8b949e;font-size:13px">(fetching rate...)</span>
      </label>
      <span style="color:#30363d">|</span>
      <span style="color:#8b949e">📅 Date: <b style="color:#c9d1d9">{today_date}</b> · Valid through: <b id="expiryDate" style="color:#d29922">{expiry_date}</b> (45 days)</span>
     </div>
    </div>

    <script>
    let cachedTaxRate = null;

    // Fetch CA tax rate from CDTFA on load — uses ship-to zip if available
    (function fetchTaxRate() {{
     cachedTaxRate = 0.0725; // Default immediately so it's never null
     document.getElementById('taxRateDisplay').textContent = '(7.25% — CA Default)';
     // Extract zip from ship-to address
     const shipTo = '{pc.get("ship_to","").replace("'","\\'")}';
     const zipMatch = shipTo.match(/\\b(\\d{{5}})\\b/);
     const zip = zipMatch ? zipMatch[1] : '';
     fetch('/api/tax-rate' + (zip ? '?zip='+zip : ''),{{credentials:'same-origin'}}).then(r=>{{
      if(!r.ok) throw new Error('HTTP '+r.status);
      return r.json();
     }}).then(d=>{{
      if(d.rate) {{
       cachedTaxRate = d.rate;
       document.getElementById('taxRateDisplay').textContent = '(' + (d.rate*100).toFixed(3) + '% — ' + (d.jurisdiction||'CA') + ')';
      }}
     }}).catch(()=>{{
      console.log('Tax rate fetch failed, using 7.25% default');
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
     el.innerHTML='<div class="msg msg-'+type+'" style="animation:slideIn .3s ease">'+text+'</div>';
     el.scrollIntoView({{behavior:'smooth',block:'nearest'}});
     if(type==='ok') setTimeout(()=>el.innerHTML='',5000);
    }}

    // ── Standalone Save with feedback ──
    function savePrices(btn) {{
     const origText=btn.textContent;
     btn.disabled=true;btn.textContent='⏳ Saving...';
     fetch('/pricecheck/{pcid}/save-prices',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(collectPrices())}})
     .then(r=>r.json()).then(d=>{{
      btn.disabled=false;
      if(d.ok){{
       if (d.profit_summary) updateProfitPanel(d.profit_summary);
       btn.textContent='✅ Saved!';btn.style.background='#238636';
       const ps = d.profit_summary;
       const profitMsg = ps && ps.gross_profit
         ? ' · Profit: $' + ps.gross_profit.toFixed(2) + ' (' + ps.margin_pct.toFixed(1) + '% margin)'
         : '';
       showMsg('✅ Prices saved.' + profitMsg,'ok');
       setTimeout(()=>{{btn.textContent=origText;btn.style.background=''}},2500);
      }} else {{
       btn.textContent=origText;
       showMsg('❌ Save failed: '+(d.error||'unknown'),'err');
      }}
     }}).catch(e=>{{btn.disabled=false;btn.textContent=origText;showMsg('❌ Error: '+e,'err')}});
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


    // ── Item Link Autofill ──────────────────────────────────────────────────
    const _linkDebounce = {{}};
    const _KNOWN_DOMAINS = {{'amazon.com':'Amazon','grainger.com':'Grainger',
      'mcmaster.com':'McMaster-Carr','fishersci.com':'Fisher Scientific',
      'medline.com':'Medline','boundtree.com':'Bound Tree Medical',
      'henryschein.com':'Henry Schein','uline.com':'Uline','zoro.com':'Zoro',
      'staples.com':'Staples','waxie.com':'Waxie','fastenal.com':'Fastenal',
      'globalindustrial.com':'Global Industrial','homedepot.com':'Home Depot',
      'officedepot.com':'Office Depot'
    }};

    function _detectSupplierJS(url) {{
      try {{
        const host = new URL(url).hostname.replace(/^www\./,'');
        for (const [dom, name] of Object.entries(_KNOWN_DOMAINS)) {{
          if (host.includes(dom)) return name;
        }}
        const parts = host.split('.');
        return parts.length >= 2
          ? parts[parts.length-2].charAt(0).toUpperCase()+parts[parts.length-2].slice(1)
          : host;
      }} catch(e) {{ return ''; }}
    }}

    function _isUrl(v) {{
      return /^https?:\/\//i.test(v) || /^[a-z0-9-]+\.[a-z]{{2,}}\//i.test(v);
    }}

    function handleLinkInput(idx, inp) {{
      const url = inp.value.trim();
      const metaEl = document.getElementById('link_meta_'+idx);
      if (!url) {{ if(metaEl) metaEl.innerHTML=''; return; }}
      const supplier = _detectSupplierJS(url);
      if (metaEl && supplier) metaEl.innerHTML='<span style="color:#8b949e">'+supplier+'</span>';
      if (!_isUrl(url)) return;
      clearTimeout(_linkDebounce[idx]);
      _linkDebounce[idx] = setTimeout(() => _fireLinkLookup(idx, url, 'pc'), 800);
    }}

    function handleRfqLinkInput(idx, inp) {{
      const url = inp.value.trim();
      if (!url || !_isUrl(url)) return;
      clearTimeout(_linkDebounce['rfq_'+idx]);
      _linkDebounce['rfq_'+idx] = setTimeout(() => _fireLinkLookup(idx, url, 'rfq'), 800);
    }}

    function _fireLinkLookup(idx, url, mode) {{
      const metaEl = mode==='pc' ? document.getElementById('link_meta_'+idx) : null;
      if (metaEl) metaEl.innerHTML='<span style="color:#d29922">\u23F3 Looking up…</span>';
      fetch('/api/item-link/lookup',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{url}})}})
      .then(r=>r.json()).then(d=>{{
        if (!d.ok && d.error) {{
          if (metaEl) metaEl.innerHTML='<span style="color:#f85149">\u26A0\uFE0F '+d.error+'</span>';
          return;
        }}
        _applyLinkData(idx, d, mode);
      }}).catch(()=>{{ if(metaEl) metaEl.innerHTML='<span style="color:#f85149">Lookup failed</span>'; }});
    }}

    function _applyLinkData(idx, d, mode) {{
      const filled = [];
      if (mode==='pc') {{
        const descEl = document.querySelector('[name=desc_'+idx+']');
        if (descEl && !descEl.value.trim() && d.description) {{ descEl.value=d.description; filled.push('description'); }}
        const costEl = document.querySelector('[name=cost_'+idx+']');
        if (costEl && d.price) {{ costEl.value=d.price.toFixed(2); filled.push('cost $'+d.price.toFixed(2)); recalcRow(idx); }}
        const metaEl = document.getElementById('link_meta_'+idx);
        if (metaEl) {{
          const supStr = d.supplier ? '<b style="color:#c9d1d9">'+d.supplier+'</b>' : '';
          const shipStr = d.shipping===0 ? ' \u00B7 <span style="color:#3fb950">Free shipping</span>'
                        : d.shipping>0 ? ' \u00B7 Ship $'+d.shipping.toFixed(2) : '';
          const partStr = d.part_number ? ' \u00B7 Part: <span style="font-family:monospace;color:#58a6ff">'+d.part_number+'</span>' : '';
          const fillStr = filled.length ? ' <span style="color:#3fb950">\u2713 '+filled.join(', ')+'</span>' : '';
          metaEl.innerHTML = supStr+shipStr+partStr+fillStr;
        }}
      }} else {{
        const descEl = document.querySelector('[name=desc_'+idx+']');
        if (descEl && !descEl.value.trim() && d.description) descEl.value=d.description;
        const costEl = document.querySelector('[name=cost_'+idx+']');
        if (costEl && d.price) {{ costEl.value=d.price.toFixed(2); typeof recalc==='function' && recalc(); }}
      }}
      if (filled.length > 0 && typeof showMsg==='function') {{
        showMsg('🔗 Auto-filled from '+d.supplier+': '+filled.join(', '),'ok');
      }}
    }}

    function relookupLink(idx) {{
      const inp = document.querySelector('[name=link_'+idx+']');
      if (!inp||!inp.value.trim()) {{ typeof showMsg==='function'&&showMsg('Paste a supplier URL first','warn'); return; }}
      _fireLinkLookup(idx, inp.value.trim(), 'pc');
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

    // Load saved profit summary from server if it exists
    (function loadSavedProfit() {{
      const saved = {profit_summary_json};
      if (saved && saved.gross_profit != null) {{
        updateProfitPanel(saved);
      }}
    }})();

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
     document.querySelectorAll('input[name^=link_]').forEach(inp=>{{
      data[inp.name]=inp.value.trim();
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

    function renamePc() {{
      const newName = document.getElementById('pcNumInput').value.trim();
      if (!newName) return;
      fetch('/pricecheck/{pcid}/rename', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{pc_number: newName}})
      }})
      .then(r=>r.json())
      .then(d=>{{
        if (d.ok) {{
          document.getElementById('pcNumDisplay').textContent = newName;
          document.getElementById('pcNumDisplay').style.display = 'inline';
          document.getElementById('pcNumEdit').style.display = 'none';
          document.title = 'PC #' + newName;
          showMsg('✅ Renamed to ' + newName, 'ok');
        }} else {{
          showMsg('❌ ' + (d.error||'Rename failed'), 'err');
        }}
      }}).catch(e=>showMsg('❌ Error: '+e, 'err'));
    }}

    function showPreview() {{
     // Build preview matching the AMS 704 Price Check format
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
      const ext=Math.round(price*qty*100)/100;
      subtotal+=ext;
      itemCount++;
      itemsHtml+=`<tr>
       <td style="text-align:center;border:1px solid #999;padding:6px;font-size:12px">${{itemNo}}</td>
       <td style="text-align:center;border:1px solid #999;padding:6px;font-size:12px">${{qty}}</td>
       <td style="text-align:center;border:1px solid #999;padding:6px;font-size:12px">${{uom.toUpperCase()}}</td>
       <td style="border:1px solid #999;padding:6px;font-size:11px;max-width:300px">${{desc}}</td>
       <td style="text-align:right;border:1px solid #999;padding:6px;font-size:12px">$${{price.toFixed(2)}}</td>
       <td style="text-align:right;border:1px solid #999;padding:6px;font-size:12px;font-weight:600">$${{ext.toFixed(2)}}</td>
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
     const notes=document.getElementById('supplierNotes')?.value||'';
     const today=new Date();
     const dateStr=today.toLocaleDateString('en-US',{{month:'2-digit',day:'2-digit',year:'numeric'}});

     const html=`<div style="padding:24px 28px;font-family:Arial,Helvetica,sans-serif;color:#000;background:#fff;font-size:13px">
      <div style="text-align:center;border-bottom:2px solid #000;padding-bottom:10px;margin-bottom:16px">
       <div style="font-size:11px;color:#444;margin-bottom:2px">STATE OF CALIFORNIA — DEPARTMENT OF GENERAL SERVICES</div>
       <div style="font-size:18px;font-weight:700;letter-spacing:1px">AMS 704 — PRICE CHECK WORKSHEET</div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0;margin-bottom:14px;border:1px solid #999">
       <div style="padding:6px 10px;border:1px solid #999"><span style="font-size:10px;color:#666;text-transform:uppercase">Price Check #</span><br><b>${{PC_META.pcNum}}</b></div>
       <div style="padding:6px 10px;border:1px solid #999"><span style="font-size:10px;color:#666;text-transform:uppercase">Date</span><br><b>${{dateStr}}</b></div>
       <div style="padding:6px 10px;border:1px solid #999"><span style="font-size:10px;color:#666;text-transform:uppercase">Institution</span><br><b>${{PC_META.institution}}</b></div>
       <div style="padding:6px 10px;border:1px solid #999"><span style="font-size:10px;color:#666;text-transform:uppercase">Ship To</span><br>${{PC_META.shipTo}}</div>
       <div style="padding:6px 10px;border:1px solid #999"><span style="font-size:10px;color:#666;text-transform:uppercase">Company Name</span><br><b>Reytech Inc.</b></div>
       <div style="padding:6px 10px;border:1px solid #999"><span style="font-size:10px;color:#666;text-transform:uppercase">Delivery</span><br>${{delivery}}</div>
      </div>
      <table style="width:100%;border-collapse:collapse;margin-bottom:12px">
       <thead><tr style="background:#e8e8e0">
        <th style="padding:6px;font-size:10px;text-transform:uppercase;border:1px solid #999;width:50px">Item #</th>
        <th style="padding:6px;font-size:10px;text-transform:uppercase;border:1px solid #999;width:50px">Qty</th>
        <th style="padding:6px;font-size:10px;text-transform:uppercase;border:1px solid #999;width:50px">UOM</th>
        <th style="padding:6px;font-size:10px;text-transform:uppercase;border:1px solid #999">Description</th>
        <th style="padding:6px;font-size:10px;text-transform:uppercase;border:1px solid #999;text-align:right;width:90px">Price/Unit</th>
        <th style="padding:6px;font-size:10px;text-transform:uppercase;border:1px solid #999;text-align:right;width:90px">Extension</th>
       </tr></thead>
       <tbody>${{itemsHtml}}</tbody>
      </table>
      <div style="display:flex;justify-content:flex-end">
       <table style="border-collapse:collapse;min-width:220px">
        <tr><td style="text-align:right;padding:4px 10px;font-size:12px;border:1px solid #999">Subtotal</td>
            <td style="text-align:right;padding:4px 10px;font-size:13px;border:1px solid #999;font-weight:600">$${{subtotal.toFixed(2)}}</td></tr>
        ${{taxOn?`<tr><td style="text-align:right;padding:4px 10px;font-size:12px;border:1px solid #999">Tax (${{(taxRate*100).toFixed(2)}}%)</td>
            <td style="text-align:right;padding:4px 10px;font-size:13px;border:1px solid #999">$${{tax.toFixed(2)}}</td></tr>`:''}}
        <tr style="background:#1a2744;color:#fff"><td style="text-align:right;padding:6px 10px;font-size:13px;font-weight:700">TOTAL</td>
            <td style="text-align:right;padding:6px 10px;font-size:15px;font-weight:700">$${{total.toFixed(2)}}</td></tr>
       </table>
      </div>
      ${{notes?`<div style="margin-top:12px;padding:8px 10px;border:1px solid #ccc;border-radius:4px;font-size:11px;color:#444"><b>Notes:</b> ${{notes}}</div>`:''}}
      <div style="margin-top:14px;padding-top:8px;border-top:1px solid #ccc;font-size:10px;color:#666;text-align:center">
       Reytech Inc. — 30 Carnoustie Way, Trabuco Canyon, CA 92679 — sales@reytechinc.com — (714) 501-3530<br>
       ${{itemCount}} item(s) · Valid until ${{expiry}}
      </div>
     </div>`;

     document.getElementById('previewBody').innerHTML=html;
     document.getElementById('previewFormType').textContent='AMS 704 — '+PC_META.institution;
     var modal=document.getElementById('previewModal');
     modal.style.display='flex';
     modal.style.justifyContent='center';
     modal.style.alignItems='flex-start';
    }}

    function closePreview() {{
     document.getElementById('previewModal').style.display='none';
    }}
    // Close on Esc or click outside
    document.getElementById('previewModal').addEventListener('click',function(e){{
     if(e.target===this) closePreview();
    }});
    document.addEventListener('keydown',function(e){{if(e.key==='Escape') closePreview();}});

    function saveAndGenerate(btn) {{
     btn.disabled=true;btn.textContent='⏳ Saving prices...';
     showMsg('Saving prices and filling AMS 704 PDF...','warn');
     fetch('/pricecheck/{pcid}/save-prices',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(collectPrices())}})
     .then(r=>r.json()).then(d=>{{
      if(!d.ok){{btn.textContent='📄 Save & Fill 704';btn.disabled=false;showMsg('❌ Save failed','err');return;}}
      btn.textContent='⏳ Generating PDF...';
      return fetch('/pricecheck/{pcid}/generate');
     }}).then(r=>r.json()).then(d=>{{
      btn.disabled=false;
      if(d&&d.ok){{
       showMsg('✅ AMS 704 filled! Reloading...','ok');
       btn.textContent='📥 Download 704';btn.className='btn btn-g';
       btn.onclick=function(){{window.location=d.download}};
       setTimeout(()=>location.reload(),2000);
      }}
      else{{btn.textContent='📄 Save & Fill 704';showMsg('❌ Generation failed: '+(d?.error||'unknown'),'err')}}
     }}).catch(e=>{{btn.textContent='📄 Save & Fill 704';btn.disabled=false;showMsg('❌ Error: '+e,'err')}});
    }}

    // ═══ PRD Feature 3.2.1: 1-click Price Check → Quote ═══
    function generateQuote1Click(btn) {{
     // Pre-flight: check for items missing cost
     let missingCost = [];
     document.querySelectorAll('input[name^=price_]').forEach((inp, i) => {{
      const bidCb = document.querySelector('[name=bid_' + i + ']');
      const isBid = bidCb ? bidCb.checked : true;
      if (!isBid) return;
      const price = parseFloat(inp.value) || 0;
      const cost = parseFloat(document.querySelector('[name=cost_' + i + ']')?.value) || 0;
      if (price > 0 && cost === 0) {{
       const desc = document.querySelector('[name=desc_' + i + ']')?.value || ('Item ' + (i+1));
       missingCost.push(desc.substring(0, 40));
      }}
     }});
     if (missingCost.length > 0) {{
      const proceed = confirm(
       '⚠️ ' + missingCost.length + ' item(s) have a price but no vendor cost entered:\\n\\n' +
       missingCost.map((d,i) => (i+1)+'. ' + d).join('\\n') +
       '\\n\\nProfit cannot be calculated for these items.\\n\\nProceed anyway?'
      );
      if (!proceed) return;
     }}

     const banner = document.getElementById('quote-gen-banner');
     const meta = document.getElementById('quote-gen-meta');
     btn.disabled = true;
     btn.textContent = '⏳ Saving prices...';
     banner.style.borderColor = '#e3b341';
     meta.textContent = 'Step 1/3: Persisting prices to DB...';

     // Step 1: Save current prices
     fetch('/pricecheck/{pcid}/save-prices', {{
       method: 'POST',
       headers: {{'Content-Type': 'application/json'}},
       body: JSON.stringify(collectPrices())
     }})
     .then(r => r.json())
     .then(d => {{
       if (!d.ok) throw new Error('Save failed: ' + (d.error || 'unknown'));
       // Update profit panel from server response immediately
       if (d.profit_summary) updateProfitPanel(d.profit_summary);
       meta.textContent = 'Step 2/3: Generating Reytech Quote PDF...';
       btn.textContent = '⏳ Generating PDF...';

       // Step 2: Generate quote via enhanced endpoint with full logging
       return fetch('/api/quote/from-price-check', {{
         method: 'POST',
         headers: {{'Content-Type': 'application/json'}},
         body: JSON.stringify({{pc_id: '{pcid}'}})
       }});
     }})
     .then(r => r.json())
     .then(d => {{
       btn.disabled = false;
       if (d && d.ok) {{
         const qn = d.quote_number || '?';
         const total = d.total ? ' — $' + parseFloat(d.total).toLocaleString('en-US', {{minimumFractionDigits:2}}) : '';
         const profitStr = d.gross_profit != null
           ? ' · <span style="color:#3fb950">Profit: $' + parseFloat(d.gross_profit).toFixed(2) + ' (' + parseFloat(d.margin_pct||0).toFixed(1) + '% margin)</span>'
           + (d.fully_costed === false ? ' <span style="color:#d29922;font-size:12px">(partial — enter costs for remaining items)</span>' : '')
           : ' · <span style="color:#8b949e;font-size:13px">Profit unknown — enter vendor costs to track margin</span>';
         banner.style.borderColor = '#3fb950';
         banner.style.background = 'linear-gradient(135deg,#0d2a0d,#0d1117)';
         meta.innerHTML = `✅ Quote <b style="color:#3fb950">${{qn}}</b>${{total}} generated · <a href="${{d.download}}" style="color:#58a6ff" download>Download PDF</a> · <a href="/quotes" style="color:#58a6ff">View in Quotes →</a>` + profitStr;
         btn.textContent = '✅ ' + qn + ' Generated';
         btn.style.background = '#238636';
         document.getElementById('next-qn-badge').textContent = 'Next: ' + (d.next_quote || '—');
         const profitLog = d.gross_profit != null ? ' · Profit $' + parseFloat(d.gross_profit).toFixed(2) : '';
         showMsg('✅ Quote ' + qn + total + profitLog + ' generated and logged to CRM + DB', 'ok');
       }} else {{
         banner.style.borderColor = '#f85149';
         meta.textContent = '❌ ' + (d?.error || 'Generation failed');
         btn.textContent = '📋 Generate Quote →';
         btn.style.background = '#1f6feb';
         showMsg('❌ Quote generation failed: ' + (d?.error || 'unknown'), 'err');
       }}
     }})
     .catch(e => {{
       btn.disabled = false;
       btn.textContent = '📋 Generate Quote →';
       btn.style.background = '#1f6feb';
       banner.style.borderColor = '#f85149';
       meta.textContent = '❌ Error: ' + e.message;
       showMsg('❌ Error: ' + e, 'err');
     }});
    }}

    // Update the server-side profit summary panel from save response
    function updateProfitPanel(ps) {{
     if (!ps) return;
     const costed = ps.costed_items || 0;
     const total = ps.total_items || 0;
     const uncostd = total - costed;
     const warn = uncostd > 0
       ? '<div style="color:#d29922;font-size:12px;margin-top:4px">⚠️ ' + uncostd + ' item(s) missing vendor cost</div>'
       : '<div style="color:#3fb950;font-size:12px;margin-top:4px">✅ All items costed</div>';
     const profitColor = ps.gross_profit > 0 ? '#3fb950' : (ps.gross_profit < 0 ? '#f85149' : '#8b949e');
     const marginColor = ps.margin_pct >= 20 ? '#3fb950' : (ps.margin_pct >= 10 ? '#d29922' : '#f85149');
     const panel = document.getElementById('server-profit-panel');
     if (panel) {{
      panel.innerHTML =
       '<div style="font-size:12px;text-transform:uppercase;color:#8b949e;margin-bottom:6px;letter-spacing:0.5px;font-weight:600">Saved Profit Summary</div>' +
       '<div style="font-size:14px"><span style="color:#8b949e">Revenue:</span> <b>$' + ps.total_revenue.toFixed(2) + '</b></div>' +
       '<div style="font-size:14px"><span style="color:#8b949e">Cost:</span> <b>$' + ps.total_cost.toFixed(2) + '</b></div>' +
       '<div style="font-size:15px;margin-top:4px"><span style="color:#8b949e">Gross Profit:</span> ' +
       '<b style="color:' + profitColor + '">$' + ps.gross_profit.toFixed(2) + '</b>' +
       ' <span style="color:' + marginColor + ';font-size:13px">(' + ps.margin_pct.toFixed(1) + '% margin)</span></div>' +
       warn;
      panel.style.display = 'block';
     }}
    }}

    function generateReytechQuote(btn) {{
     btn.disabled=true;btn.textContent='⏳ Saving...';
     showMsg('Saving prices and generating Reytech Quote PDF...','warn');
     fetch('/pricecheck/{pcid}/save-prices',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(collectPrices())}})
     .then(r=>r.json()).then(d=>{{
      if (d.profit_summary) updateProfitPanel(d.profit_summary);
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
      btn.disabled=false;btn.textContent='⚡ Process Now';
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
     }}).catch(e=>{{btn.textContent='⚡ Process Now';btn.disabled=false;showMsg('❌ Error: '+e,'err')}});
    }}

    // ── CRM Customer Card + Quote History (loads on page) ─────────────────────
    // Timeout wrapper for fetch — Railway cold starts can hang
    function fetchWithTimeout(url, ms=8000) {{
      const ctrl = new AbortController();
      const timer = setTimeout(()=>ctrl.abort(), ms);
      return fetch(url, {{signal:ctrl.signal, credentials:'same-origin'}}).finally(()=>clearTimeout(timer));
    }}
    function retryFetch(url, attempts=2, ms=8000) {{
      return fetchWithTimeout(url, ms).catch(err => {{
        if (attempts <= 1) throw err;
        return new Promise(r=>setTimeout(r,1500)).then(()=>retryFetch(url,attempts-1,ms));
      }});
    }}

    // Fallback: clear Loading... after 12s no matter what
    setTimeout(()=>{{
      const crm=document.getElementById('crmBody');
      const hist=document.getElementById('historyBody');
      if(crm&&crm.textContent.includes('Loading'))crm.innerHTML='<span style="color:#8b949e">CRM timed out — <button onclick="location.reload()" style="background:none;border:none;color:#58a6ff;cursor:pointer;text-decoration:underline;font-size:inherit">reload</button></span>';
      if(hist&&hist.textContent.includes('Loading'))hist.innerHTML='<span style="color:#8b949e">History timed out — <button onclick="location.reload()" style="background:none;border:none;color:#58a6ff;cursor:pointer;text-decoration:underline;font-size:inherit">reload</button></span>';
    }},12000);

    document.addEventListener('DOMContentLoaded',function(){{try{{
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
         + 'style="font-family:JetBrains Mono,monospace;font-weight:700;font-size:13px;color:#58a6ff;text-decoration:none">'
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
       html += '<span style="font-family:JetBrains Mono,monospace;font-weight:600;font-size:13px;color:var(--tx)">$' 
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
    }}catch(e){{console.error('CRM/History load error:',e);
      const crm=document.getElementById('crmBody');
      if(crm)crm.innerHTML='<span style="color:#f85149">⚠️ JS error — check console</span>';
    }}}});

    function addNewCustomer() {{
     const inst = PC_META.institution;
     const ag = prompt('Agency for "' + inst + '"?\\n(CDCR, CCHCS, CalVet, DGS, DSH, DEFAULT)', 'CDCR');
     if (!ag) return;
     const parent = prompt('Parent organization?\\n(e.g. "Dept of Corrections and Rehabilitation")', '');
     fetch('/api/customers', {{
      method: 'POST',
      credentials: 'same-origin',
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

    // ── PRD Feature 10: Price History Intelligence ──────────────────────────
    function loadPriceHistory() {{
      const btn = document.getElementById('phLoadBtn');
      const panel = document.getElementById('ph-content');
      const rows = document.getElementById('ph-rows');
      btn.textContent = 'Loading...'; btn.disabled = true;
      const descs = [];
      document.querySelectorAll('#itemsTable tr').forEach(tr => {{
        const inputs = tr.querySelectorAll('input[type=text]');
        inputs.forEach(inp => {{ if(inp.name && inp.name.startsWith('desc_') && inp.value.trim()) descs.push(inp.value.trim().substring(0,80)); }});
      }});
      if (!descs.length) {{
        rows.innerHTML = '<div style="color:#484f58;font-size:13px;padding:8px">No items found.</div>';
        panel.style.display='block'; btn.textContent='Load History'; btn.disabled=false; return;
      }}
      let loaded = 0; rows.innerHTML = ''; panel.style.display = 'block';
      const seen = new Set();
      descs.filter(d=>!seen.has(d)&&seen.add(d)).forEach(desc => {{
        fetch('/api/prices/best?q=' + encodeURIComponent(desc.substring(0,50)))
          .then(r=>r.json()).then(d=>{{
            loaded++;
            if (d.ok && d.found) {{
              rows.innerHTML += `<div style="display:flex;align-items:center;gap:12px;padding:8px 12px;background:#161b22;border:1px solid #30363d;border-radius:6px;margin-bottom:6px;flex-wrap:wrap">
                <div style="flex:1;min-width:0"><div style="font-size:13px;color:#c9d1d9;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${{desc}}">${{desc.substring(0,70)}}</div>
                <div style="font-size:11px;color:#484f58">via ${{d.best_source||'?'}} · ${{(d.best_found_at||'').substring(0,10)}} · ${{d.observations}} observation${{d.observations!==1?'s':''}}</div></div>
                <div style="text-align:right;white-space:nowrap"><div style="color:#3fb950;font-weight:700;font-size:15px">Best: $${{d.best_price.toFixed(2)}}</div>
                <div style="color:#8b949e;font-size:12px">Avg: $${{d.avg_price.toFixed(2)}}</div></div></div>`;
            }}
            if (loaded >= seen.size) {{
              if (!rows.innerHTML) rows.innerHTML='<div style="color:#484f58;font-size:13px;padding:8px">No price history yet — builds as quotes are generated.</div>';
              btn.textContent='Refresh'; btn.disabled=false;
            }}
          }}).catch(()=>{{loaded++;}});
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
     <!-- Header: Title + Stats — compact row -->
     <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:14px">
      <h2 style="margin:0;font-size:20px;font-weight:700">📋 Quotes Database</h2>
      <div>{stats_html}</div>
     </div>

     <!-- Search + Filters — one compact bar -->
     <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:14px">
      <form method="get" style="display:contents">
       <input name="q" value="{q}" placeholder="Search quotes, institutions, items, part numbers, RFQ #..." style="flex:1;min-width:240px;padding:10px 14px;background:var(--sf);border:1px solid var(--bd);border-radius:8px;color:var(--tx);font-size:13px">
       <select name="agency" style="padding:10px;background:var(--sf);border:1px solid var(--bd);border-radius:8px;color:var(--tx);font-size:13px">
        <option value="">All Agencies</option>
        <option value="CDCR" {"selected" if agency_filter=="CDCR" else ""}>CDCR</option>
        <option value="CCHCS" {"selected" if agency_filter=="CCHCS" else ""}>CCHCS</option>
        <option value="CalVet" {"selected" if agency_filter=="CalVet" else ""}>CalVet</option>
        <option value="DGS" {"selected" if agency_filter=="DGS" else ""}>DGS</option>
        <option value="DSH" {"selected" if agency_filter=="DSH" else ""}>DSH</option>
       </select>
       <select name="status" style="padding:10px;background:var(--sf);border:1px solid var(--bd);border-radius:8px;color:var(--tx);font-size:13px">
        <option value="">All Status</option>
        <option value="pending" {"selected" if status_filter=="pending" else ""}>⏳ Pending</option>
        <option value="won" {"selected" if status_filter=="won" else ""}>✅ Won</option>
        <option value="lost" {"selected" if status_filter=="lost" else ""}>❌ Lost</option>
        <option value="sent" {"selected" if status_filter=="sent" else ""}>📤 Sent</option>
        <option value="expired" {"selected" if status_filter=="expired" else ""}>⏰ Expired</option>
       </select>
       <button type="submit" class="btn btn-p" style="padding:10px 18px;font-size:13px">🔍 Search</button>
      </form>
     </div>

     <!-- Quotes Table — flush, full width -->
     <div class="card" style="padding:0;overflow-x:auto">
      <table class="home-tbl" style="min-width:900px">
       <thead><tr>
        <th style="width:80px">Quote #</th><th style="width:100px">Date</th><th style="width:70px">Agency</th><th>Institution</th><th style="width:100px">RFQ #</th>
        <th style="text-align:right;width:90px">Total</th><th style="width:50px;text-align:center">Items</th><th style="width:80px;text-align:center">Status</th><th style="width:100px;text-align:center">Actions</th>
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


def render_agents_page():
    """Render the Agent Control Panel — buttons for all agent operations."""
    return f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Agent Control Panel — Reytech</title>
<style>
:root {{ --bg:#0d1117; --sf:#161b22; --sf2:#21262d; --bd:#30363d; --tx:#e6edf3; --tx2:#8b949e;
  --ok:#238636; --warn:#d29922; --err:#da3633; --blue:#58a6ff; --purple:#bc8cff; }}
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:var(--bg); color:var(--tx); padding:16px; max-width:1000px; margin:auto; }}
a {{ color:var(--blue); text-decoration:none; }}
.nav {{ display:flex; gap:8px; align-items:center; margin-bottom:20px; flex-wrap:wrap; }}
.nav a {{ padding:5px 12px; background:var(--sf2); border:1px solid var(--bd); border-radius:6px; font-size:13px; color:var(--tx); }}
h1 {{ font-size:22px; margin-bottom:4px; }}
.sub {{ color:var(--tx2); font-size:13px; margin-bottom:20px; }}
.section {{ background:var(--sf); border:1px solid var(--bd); border-radius:10px; padding:16px; margin-bottom:16px; }}
.section h2 {{ font-size:16px; margin-bottom:12px; display:flex; align-items:center; gap:8px; }}
.section h2 .tag {{ font-size:11px; padding:2px 8px; border-radius:10px; font-weight:normal; }}
.tag-ok {{ background:#238636; color:#fff; }}
.tag-warn {{ background:#d29922; color:#000; }}
.tag-off {{ background:var(--sf2); color:var(--tx2); border:1px solid var(--bd); }}
.grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(200px,1fr)); gap:10px; }}
.btn {{ padding:10px 16px; border:1px solid var(--bd); border-radius:8px; background:var(--sf2); color:var(--tx);
  cursor:pointer; font-size:13px; text-align:left; transition:all 0.15s; display:flex; flex-direction:column; gap:4px; }}
.btn:hover {{ border-color:var(--blue); background:#1a2332; }}
.btn .label {{ font-weight:600; font-size:14px; }}
.btn .desc {{ font-size:11px; color:var(--tx2); }}
.btn-go {{ background:#1a3a5c; border-color:#2563eb; }}
.btn-go:hover {{ background:#1e4a7c; }}
.btn-danger {{ border-color:var(--err); }}
.btn-danger:hover {{ background:#3a1515; }}
#result {{ background:#0a0e14; border:1px solid var(--bd); border-radius:8px; padding:16px; margin-top:16px;
  font-family:'SF Mono',Consolas,monospace; font-size:12px; line-height:1.6; white-space:pre-wrap;
  max-height:500px; overflow-y:auto; display:none; position:relative; }}
#result .close {{ position:absolute; top:8px; right:12px; cursor:pointer; color:var(--tx2); font-size:16px; }}
#result .close:hover {{ color:var(--tx); }}
.fleet {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(180px,1fr)); gap:8px; margin-bottom:16px; }}
.agent-card {{ padding:10px; border:1px solid var(--bd); border-radius:8px; background:var(--sf2); font-size:12px; }}
.agent-card .name {{ font-weight:600; font-size:13px; margin-bottom:4px; }}
.agent-card .mode {{ color:var(--tx2); }}
.loading {{ color:var(--warn); }}
</style>
</head><body>

<div class="nav">
 <a href="/">🏠 Home</a>
 <a href="/quotes">📋 Quotes</a>
 <a href="/agents" style="border-color:var(--blue)">🤖 Agents</a>
</div>

<h1>🤖 Agent Control Panel</h1>
<div class="sub">Click any button to run it. Results appear below.</div>

<div id="fleet" class="section">
 <h2>Fleet Status <span class="tag tag-off" id="fleet-tag">loading...</span></h2>
 <div class="fleet" id="fleet-grid"><div class="loading">Loading agent status...</div></div>
</div>

<div class="section">
 <h2>🔀 Workflow Orchestrator <span class="tag tag-ok">LangGraph</span></h2>
 <p style="color:#8b949e;font-size:12px;margin-bottom:10px">LangGraph-powered pipelines — chains your agents into executable workflows with audit trails.</p>
 <div class="grid">
  <button class="btn btn-go" onclick="apiGet('/api/workflow/status')">
   <span class="label">📊 Orchestrator Status</span><span class="desc">Run history + available workflows</span>
  </button>
  <button class="btn btn-go" onclick="runWorkflow('pc_pipeline')">
   <span class="label">⚡ PC Pipeline</span><span class="desc">SCPRS → Amazon → Price → 704</span>
  </button>
  <button class="btn btn-go" onclick="runWorkflow('lead_pipeline')">
   <span class="label">🎯 Lead Pipeline</span><span class="desc">Scan → Score → Draft → Approve</span>
  </button>
  <button class="btn btn-go" onclick="runWorkflow('quote_pipeline')">
   <span class="label">📋 Quote Pipeline</span><span class="desc">Quote PDF → Email → Review</span>
  </button>
  <button class="btn btn-go" onclick="apiGet('/api/workflow/graph/pc_pipeline')">
   <span class="label">🗺️ View PC Graph</span><span class="desc">Node → edge structure</span>
  </button>
  <button class="btn btn-go" onclick="apiGet('/api/workflow/graph/lead_pipeline')">
   <span class="label">🗺️ View Lead Graph</span><span class="desc">Node → edge structure</span>
  </button>
 </div>
</div>

<div class="section">
 <h2>🧠 Manager Brief <span class="tag tag-ok">Active</span></h2>
 <div class="grid">
  <button class="btn btn-go" onclick="apiGet('/api/manager/brief')">
   <span class="label">📋 Daily Brief</span><span class="desc">What needs attention right now</span>
  </button>
 </div>
</div>

<div class="section">
 <h2>🚀 Growth Engine <span class="tag tag-ok" id="growth-tag">v2.0</span></h2>
 <p style="color:#8b949e;font-size:12px;margin-bottom:8px">Proactive SCPRS-driven outreach — pull Reytech history → categorize → find all buyers → email → voice follow-up.</p>
 <div id="growth-status" style="display:none;margin-bottom:12px;padding:10px;background:var(--sf2);border-radius:8px;font-size:12px"></div>

 <div style="margin-bottom:8px;font-size:11px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:1px">Step 1 — Pull Reytech History</div>
 <div class="grid" style="margin-bottom:12px">
  <button class="btn btn-go" onclick="apiGet('/api/growth/pull-history')">
   <span class="label">📥 Pull SCPRS History</span><span class="desc">All Reytech POs from 2022</span>
  </button>
  <button class="btn" onclick="apiGet('/api/growth/pull-status')">
   <span class="label">⏳ Pull Progress</span><span class="desc">Check scrape status</span>
  </button>
 </div>

 <div style="margin-bottom:8px;font-size:11px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:1px">Step 2 — Find All Buyers</div>
 <div class="grid" style="margin-bottom:12px">
  <button class="btn btn-go" onclick="apiGet('/api/growth/find-buyers')">
   <span class="label">🔍 Find Category Buyers</span><span class="desc">Search SCPRS for all buyers of our items</span>
  </button>
  <button class="btn" onclick="apiGet('/api/growth/buyer-status')">
   <span class="label">⏳ Buyer Search Progress</span><span class="desc">Check buyer scrape status</span>
  </button>
 </div>

 <div style="margin-bottom:8px;font-size:11px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:1px">Step 3 — Outreach Campaign</div>
 <div class="grid" style="margin-bottom:12px">
  <button class="btn btn-go" onclick="apiGet('/api/growth/outreach?dry_run=true')">
   <span class="label">👁️ Preview Emails</span><span class="desc">Dry run — see what would send</span>
  </button>
  <button class="btn" onclick="if(confirm('Send real emails to prospects?')) apiGet('/api/growth/outreach?dry_run=false')" style="border-color:rgba(52,211,153,.3)">
   <span class="label">📧 Send Outreach</span><span class="desc">Live emails to prospects</span>
  </button>
 </div>

 <div style="margin-bottom:8px;font-size:11px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:1px">Step 4 — Voice Follow-Up</div>
 <div class="grid" style="margin-bottom:12px">
  <button class="btn" onclick="apiGet('/api/growth/follow-ups')">
   <span class="label">📋 Check Follow-Ups</span><span class="desc">Who hasn't responded in 3-5 days?</span>
  </button>
  <button class="btn" onclick="if(confirm('Auto-dial non-responders?')) apiGet('/api/growth/voice-follow-up')">
   <span class="label">📞 Voice Follow-Up</span><span class="desc">Auto-call non-responders</span>
  </button>
 </div>

 <div class="grid">
  <button class="btn" onclick="apiGet('/api/growth/status')">
   <span class="label">📊 Full Status</span><span class="desc">History, categories, prospects, outreach</span>
  </button>
 </div>
</div>

<div class="section">
 <h2>🧠 Sales Intelligence <span class="tag tag-ok">$2M Goal</span></h2>
 <p style="color:#8b949e;font-size:12px;margin-bottom:8px">Deep SCPRS mining → buyer prioritization → SB admin outreach → revenue tracking.</p>
 <div class="grid">
  <button class="btn btn-go" onclick="apiGet('/api/intel/deep-pull')">
   <span class="label">🔍 Deep Pull All Buyers</span><span class="desc">Mine SCPRS for every buyer across all categories</span>
  </button>
  <button class="btn" onclick="apiGet('/api/intel/pull-status')">
   <span class="label">⏳ Pull Progress</span><span class="desc">Check deep pull status</span>
  </button>
  <button class="btn btn-go" onclick="apiGet('/api/intel/priority-queue')">
   <span class="label">📊 Priority Queue</span><span class="desc">Ranked buyers by opportunity score</span>
  </button>
  <button class="btn" onclick="apiGet('/api/intel/push-prospects?top=50')">
   <span class="label">📥 Push to Growth</span><span class="desc">Send top 50 → Growth outreach pipeline</span>
  </button>
  <button class="btn" onclick="apiGet('/api/intel/sb-admin-match')">
   <span class="label">🏛️ Match SB Admins</span><span class="desc">Find SB liaisons for agencies</span>
  </button>
  <button class="btn" onclick="apiGet('/api/intel/revenue')">
   <span class="label">💰 Revenue Tracker</span><span class="desc">YTD vs $2M goal</span>
  </button>
  <button class="btn" onclick="apiGet('/api/intel/status')">
   <span class="label">📊 Full Status</span><span class="desc">Buyers, agencies, revenue, pull status</span>
  </button>
 </div>
</div>

<div class="section" id="cs-agent-section" style="border:2px solid rgba(251,191,36,.4);background:rgba(251,191,36,.04)">
 <h2>💬 Customer Support Agent <span class="tag tag-warn" id="cs-badge">loading…</span></h2>
 <p style="color:#8b949e;font-size:12px;margin-bottom:12px">
   Auto-drafts replies to inbound customer emails (order status, invoice questions, PC follow-ups).
   Drafts sit here for review before sending. <a href="/outbox" style="color:#fbbf24;margin-left:4px">→ Open CS Inbox</a>
 </p>
 <div id="cs-drafts-preview" style="margin-bottom:12px"></div>
 <div class="grid">
  <button class="btn btn-go" onclick="location.href='/outbox'" style="border-color:rgba(251,191,36,.4)">
   <span class="label">💬 CS Inbox</span><span class="desc">Review &amp; approve CS replies</span>
  </button>
  <button class="btn" onclick="apiPost('/api/cs/classify',{{}})" style="border-color:rgba(251,191,36,.4)">
   <span class="label">🔍 Classify Inbound</span><span class="desc">Auto-route new customer emails</span>
  </button>
  <button class="btn" onclick="apiGet('/api/cs/status')">
   <span class="label">📊 CS Status</span><span class="desc">Agent health &amp; stats</span>
  </button>
  <button class="btn" onclick="apiGet('/api/cs/drafts')">
   <span class="label">📋 List Drafts</span><span class="desc">All pending CS reply drafts</span>
  </button>
 </div>
</div>


<div class="section" id="wf-section" style="border:2px solid rgba(79,140,255,.3);background:rgba(79,140,255,.04)">
 <h2>🔬 Workflow QA Tests <span class="tag" id="wf-badge" style="background:var(--sf2);color:var(--tx2)">loading…</span></h2>
 <p style="color:#8b949e;font-size:12px;margin-bottom:12px">
   End-to-end data flow tests — runs every 10 minutes. Tests queue isolation, manager brief accuracy, DB consistency, CS visibility, email routing.
   <a href="/qa/workflow" style="color:var(--blue);margin-left:4px">→ Full Dashboard</a>
 </p>
 <div id="wf-latest-preview" style="margin-bottom:12px"></div>
 <div class="grid">
  <button class="btn btn-go" onclick="location.href='/qa/workflow'" style="border-color:rgba(79,140,255,.4)">
   <span class="label">🔬 Test Dashboard</span><span class="desc">Full run history & results</span>
  </button>
  <button class="btn" onclick="runWorkflowTests()" id="wf-run-btn" style="border-color:rgba(79,140,255,.4)">
   <span class="label">▶ Run Now</span><span class="desc">Force immediate test run</span>
  </button>
  <button class="btn" onclick="apiGet('/api/qa/health')">
   <span class="label">📊 Code QA</span><span class="desc">Static code quality scan</span>
  </button>
  <button class="btn" onclick="location.href='/qa/intelligence'">
   <span class="label">📈 QA History</span><span class="desc">Score trends over time</span>
  </button>
 </div>
</div>


<div class="section">
 <h2>📧 Email Outreach</h2>
 <div class="grid">
  <button class="btn btn-go" onclick="apiGet('/api/outbox')">
   <span class="label">📬 View Outbox</span><span class="desc">All drafts & sent emails</span>
  </button>
  <button class="btn" onclick="apiGet('/api/outbox?status=draft')">
   <span class="label">📝 Drafts Only</span><span class="desc">Emails awaiting approval</span>
  </button>
  <button class="btn" onclick="apiGet('/api/outbox/sent')">
   <span class="label">✅ Sent Log</span><span class="desc">Delivered emails</span>
  </button>
  <button class="btn btn-go" onclick="draftForPc()">
   <span class="label">✉️ Draft PC Email</span><span class="desc">Enter PC ID → auto-draft</span>
  </button>
  <button class="btn btn-danger" onclick="sendApproved()">
   <span class="label">🚀 Send All Approved</span><span class="desc">Send every approved email</span>
  </button>
 </div>
</div>

<div class="section">
 <h2>🎯 Lead Generation</h2>
 <div class="grid">
  <button class="btn btn-go" onclick="apiGet('/api/leads')">
   <span class="label">📋 All Leads</span><span class="desc">Scored opportunities</span>
  </button>
  <button class="btn" onclick="apiGet('/api/leads?status=new')">
   <span class="label">🆕 New Leads</span><span class="desc">Not yet contacted</span>
  </button>
  <button class="btn" onclick="apiGet('/api/leads/analytics')">
   <span class="label">📈 Lead Analytics</span><span class="desc">Funnel conversion rates</span>
  </button>
 </div>
</div>

<div class="section">
 <h2>🔍 SCPRS Scanner</h2>
 <div class="grid">
  <button class="btn" onclick="apiGet('/api/scanner/status')">
   <span class="label">📡 Scanner Status</span><span class="desc">Is it running?</span>
  </button>
  <button class="btn btn-go" onclick="apiPost('/api/scanner/start',{{interval:120}})">
   <span class="label">▶️ Start Scanner</span><span class="desc">Poll every 2 min</span>
  </button>
  <button class="btn btn-danger" onclick="apiPost('/api/scanner/stop')">
   <span class="label">⏹️ Stop Scanner</span><span class="desc">Stop polling</span>
  </button>
  <button class="btn" onclick="apiPost('/api/scanner/scan')">
   <span class="label">🔎 Scan Now</span><span class="desc">One manual scan</span>
  </button>
 </div>
</div>

<div class="section">
 <h2>🔑 Item Identifier</h2>
 <div class="grid">
  <button class="btn btn-go" onclick="identifyItem()">
   <span class="label">🔍 Identify Item</span><span class="desc">Enter description → get search terms</span>
  </button>
  <button class="btn" onclick="identifyPc()">
   <span class="label">📦 Identify PC Items</span><span class="desc">Run ID on all items in a PC</span>
  </button>
 </div>
</div>

<div class="section">
 <h2>📞 Voice Agent (Vapi AI) <span class="tag tag-off" id="voice-tag">needs setup</span></h2>
 <p style="color:#8b949e;font-size:12px;margin-bottom:10px">Conversational AI outbound calls — real conversations, not script reading. Powered by Vapi.ai with natural voice.</p>
 <div class="grid">
  <button class="btn btn-go" onclick="apiGet('/api/voice/status')">
   <span class="label">📞 Voice Status</span><span class="desc">Engine + config check</span>
  </button>
  <button class="btn btn-go" onclick="apiGet('/api/voice/verify')">
   <span class="label">🔑 Verify Keys</span><span class="desc">Test Vapi + Twilio</span>
  </button>
  <button class="btn" onclick="apiGet('/api/voice/scripts')">
   <span class="label">📜 Call Scripts</span><span class="desc">Lead intro, follow-up, cold, thank you</span>
  </button>
  <button class="btn" onclick="apiGet('/api/voice/log')">
   <span class="label">📋 Call Log</span><span class="desc">Recent calls</span>
  </button>
  <button class="btn" onclick="apiGet('/api/voice/vapi-calls')">
   <span class="label">🎙️ Transcripts</span><span class="desc">Vapi calls + AI transcripts</span>
  </button>
  <button class="btn btn-go" onclick="testCall()">
   <span class="label">📱 Test Call</span><span class="desc">Call to test conversation</span>
  </button>
  <button class="btn" onclick="importTwilio()">
   <span class="label">🔗 Import Twilio #</span><span class="desc">Use Reytech caller ID</span>
  </button>
 </div>
 <div style="margin-top:10px;padding:10px;background:#0d1117;border-radius:8px;font-size:11px;font-family:monospace;color:#8b949e">
  <div style="color:var(--warn);margin-bottom:4px">Railway env vars:</div>
  <div>VAPI_API_KEY=... <span style="color:#3fb950">(required — conversational AI)</span></div>
  <div>TWILIO_ACCOUNT_SID=AC... <span style="color:#8b949e">(optional — for Reytech caller ID)</span></div>
  <div>TWILIO_AUTH_TOKEN=... <span style="color:#8b949e">(optional)</span></div>
  <div>TWILIO_PHONE_NUMBER=+1... <span style="color:#8b949e">(optional)</span></div>
 </div>
</div>

<div class="section">
 <h2>🧾 QuickBooks Online <span class="tag tag-off" id="qb-tag">not connected</span></h2>
 <p style="color:#8b949e;font-size:12px;margin-bottom:10px">Full financial context — invoices, customers, vendors, AR. All agents use this data for informed decisions.</p>
 <div id="qb-finance" style="display:none;margin-bottom:12px"></div>
 <div class="grid">
  <button class="btn btn-go" onclick="window.location='/api/qb/connect'">
   <span class="label">🔗 Connect QuickBooks</span><span class="desc">Start OAuth2 flow</span>
  </button>
  <button class="btn" onclick="apiGet('/api/qb/financial-context')">
   <span class="label">📊 Financial Snapshot</span><span class="desc">Full context for agents</span>
  </button>
  <button class="btn" onclick="apiGet('/api/qb/invoices?status=open')">
   <span class="label">💰 Open Invoices</span><span class="desc">Unpaid receivables</span>
  </button>
  <button class="btn" onclick="apiGet('/api/qb/invoices?status=overdue')">
   <span class="label">⚠️ Overdue</span><span class="desc">Past-due invoices</span>
  </button>
  <button class="btn" onclick="apiGet('/api/qb/customers')">
   <span class="label">👤 Customers</span><span class="desc">QB customer list</span>
  </button>
  <button class="btn" onclick="apiGet('/api/qb/customers/balances')">
   <span class="label">💳 AR Balances</span><span class="desc">Top receivables</span>
  </button>
  <button class="btn" onclick="apiGet('/api/qb/vendors')">
   <span class="label">👥 Vendors</span><span class="desc">Pull vendor list</span>
  </button>
  <button class="btn" onclick="apiGet('/api/qb/pos')">
   <span class="label">📄 Recent POs</span><span class="desc">Last 30 days</span>
  </button>
 </div>
 <div style="margin-top:10px;padding:10px;background:#0d1117;border-radius:8px;font-size:11px;font-family:monospace;color:#8b949e">
  <div style="color:var(--warn);margin-bottom:4px">Railway env vars needed:</div>
  <div>QB_CLIENT_ID=...</div>
  <div>QB_CLIENT_SECRET=...</div>
  <div>QB_REALM_ID=... (or auto-detected on connect)</div>
  <div>QB_REFRESH_TOKEN=... (or auto-set on connect)</div>
 </div>
</div>

<div class="section">
 <h2>📋 CRM Activity <span class="tag tag-ok">Phase 16</span></h2>
 <p style="color:#8b949e;font-size:12px;margin-bottom:10px">Activity timeline — tracks quotes, emails, calls, POs across all agencies.</p>
 <div class="grid">
  <button class="btn btn-go" onclick="apiGet('/api/crm/activity?limit=20')">
   <span class="label">📋 Recent Activity</span><span class="desc">Last 20 events</span>
  </button>
  <button class="btn" onclick="apiGet('/api/crm/agency/CDCR')">
   <span class="label">🏢 CDCR Intel</span><span class="desc">Agency summary</span>
  </button>
  <button class="btn" onclick="apiGet('/api/crm/agency/CCHCS')">
   <span class="label">🏥 CCHCS Intel</span><span class="desc">Agency summary</span>
  </button>
 </div>
</div>

<div class="section">
 <h2>🎯 Predictive Intel <span class="tag tag-ok">Phase 19</span></h2>
 <p style="color:#8b949e;font-size:12px;margin-bottom:10px">Win probability prediction, competitor intelligence, shipping monitor. AI learns from every won and lost quote.</p>
 <div class="grid">
  <button class="btn btn-go" onclick="predictWin()">
   <span class="label">🎯 Predict Win</span><span class="desc">Score any opportunity</span>
  </button>
  <button class="btn" onclick="apiGet('/api/intel/competitors')">
   <span class="label">🕵️ Competitor Intel</span><span class="desc">Lost quote patterns</span>
  </button>
  <button class="btn" onclick="apiGet('/api/intel/competitors?institution=CSP-Sacramento')">
   <span class="label">🏢 CSP Intel</span><span class="desc">Sacramento competitor data</span>
  </button>
  <button class="btn" onclick="testShipping()">
   <span class="label">📦 Test Shipping Scan</span><span class="desc">Detect tracking in email</span>
  </button>
 </div>
</div>

<div class="section">
 <h2>🔍 QA Agent <span class="tag tag-ok" id="qa-tag">Active</span></h2>
 <p style="color:#8b949e;font-size:12px;margin-bottom:10px">Autonomous health monitor — runs every 5 min. Checks routes, data, agents, code, env, sales.</p>
 <div id="qa-health" style="display:none;margin-bottom:12px"></div>
 <div class="grid">
  <button class="btn btn-go" onclick="apiGet('/api/qa/health')">
   <span class="label">🏥 Health Check</span><span class="desc">Full system health</span>
  </button>
  <button class="btn btn-go" onclick="apiGet('/api/qa/health?checks=sales')">
   <span class="label">💰 Sales QA</span><span class="desc">Quote totals, PC profit, $2M goal</span>
  </button>
  <button class="btn" onclick="apiGet('/api/qa/scan')">
   <span class="label">🔍 Code Scan</span><span class="desc">JS/HTML/Python quality</span>
  </button>
  <button class="btn" onclick="apiGet('/api/qa/history')">
   <span class="label">📊 History</span><span class="desc">Past health reports</span>
  </button>
  <button class="btn" onclick="apiGet('/api/qa/trend')">
   <span class="label">📈 Trend</span><span class="desc">Health score over time</span>
  </button>
 </div>
</div>

<div class="section">
 <h2>🧪 Testing <span class="tag tag-warn">Sandbox</span></h2>
 <p style="color:#8b949e;font-size:12px;margin-bottom:10px">Test data is labeled <span style="background:#d29922;color:#000;font-size:9px;padding:1px 5px;border-radius:4px;font-weight:700">TEST</span> and excluded from pipeline stats, funnel, and auto-assignment.</p>
 <div class="grid">
  <button class="btn btn-go" onclick="apiGet('/api/test/create-pc')">
   <span class="label">📝 Create Test RFQ</span><span class="desc">CSP-Sacramento test PC with 3 items</span>
  </button>
  <button class="btn" onclick="apiGet('/api/test/status')">
   <span class="label">📊 Test Status</span><span class="desc">View active test records</span>
  </button>
  <button class="btn" onclick="if(confirm('Remove all test data?')) apiGet('/api/test/cleanup?reset_counter=false')" style="border-color:rgba(248,113,113,.3)">
   <span class="label">🗑️ Clean Test Data</span><span class="desc">Remove all TEST records</span>
  </button>
 </div>
</div>

<div id="result"><span class="close" onclick="closeResult()">✕</span><pre id="result-content"></pre></div>

<script>
const R = document.getElementById('result');
const RC = document.getElementById('result-content');

function showResult(data) {{
  RC.textContent = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
  R.style.display = 'block';
  R.scrollIntoView({{ behavior:'smooth', block:'nearest' }});
}}

function closeResult() {{ R.style.display = 'none'; }}

function apiGet(url) {{
  RC.textContent = 'Loading...'; R.style.display = 'block';
  fetch(url).then(r => r.json()).then(showResult).catch(e => showResult('Error: ' + e));
}}

// ── CS Agent — load draft count + preview ───────────────────────────────
fetch('/api/cs/drafts',{{credentials:'same-origin'}}).then(r=>r.json()).then(function(d){{
  var badge=document.getElementById('cs-badge');
  var preview=document.getElementById('cs-drafts-preview');
  var drafts=Array.isArray(d.drafts)?d.drafts:(d.cs_drafts||[]);
  if(badge){{
    if(drafts.length>0){{
      badge.textContent=drafts.length+' draft'+(drafts.length>1?'s':'')+' pending';
      badge.className='tag tag-warn';
    }}else{{
      badge.textContent='inbox clear';
      badge.className='tag tag-ok';
    }}
  }}
  if(preview&&drafts.length>0){{
    preview.innerHTML=drafts.slice(0,3).map(function(dr){{
      var meta={{}};try{{meta=JSON.parse(dr.metadata||'{{}}')}}catch(e){{}}
      var from=meta.original_sender||dr.to||dr.to_address||'?';
      var origSubj=meta.original_subject||dr.subject||dr.label||'Customer email';
      return '<div style="background:rgba(251,191,36,.08);border:1px solid rgba(251,191,36,.25);border-radius:8px;padding:10px 12px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center;gap:10px">'+
        '<div>'+
          '<div style="font-size:12px;font-weight:600;color:#fbbf24">📬 '+origSubj+'</div>'+
          '<div style="font-size:11px;color:#8b949e;margin-top:2px">From: '+from+'</div>'+
        '</div>'+
        '<a href="/outbox" style="font-size:12px;font-weight:600;color:#fbbf24;white-space:nowrap;border:1px solid rgba(251,191,36,.4);padding:4px 10px;border-radius:6px;background:rgba(251,191,36,.1)">Review →</a>'+
      '</div>';
    }}).join('');
  }}
}}).catch(function(){{
  var badge=document.getElementById('cs-badge');
  if(badge){{badge.textContent='unavailable';badge.className='tag tag-off';}}
}});
// ── Workflow Tester — load latest score ─────────────────────────────────
fetch('/api/qa/workflow/latest',{{credentials:'same-origin'}}).then(r=>r.json()).then(function(d){{
  var badge=document.getElementById('wf-badge');
  var preview=document.getElementById('wf-latest-preview');
  if(!d||!d.score){{
    if(badge)badge.textContent='no runs yet';
    return;
  }}
  var sc=d.score||0;
  var col=sc>=90?'#34d399':sc>=70?'#fbbf24':'#f87171';
  var bg=sc>=90?'rgba(52,211,153,.15)':sc>=70?'rgba(251,191,36,.15)':'rgba(248,113,113,.15)';
  if(badge){{badge.textContent=sc+'/100 '+d.grade;badge.style.background=bg;badge.style.color=col;}}
  if(preview&&d.full_report){{
    var fr=d.full_report;
    var fails=(fr.critical_failures||[]);
    var warns=(fr.results||[]).filter(function(r){{return r.status==='warn';}});
    if(fails.length+warns.length>0){{
      preview.innerHTML=(fails.concat(warns)).slice(0,3).map(function(r){{
        var ic=r.status==='fail'?'❌':'⚠️';
        return '<div style="background:rgba('+( r.status==='fail'?'248,113,113':'251,191,36')+', .08);border:1px solid rgba('+( r.status==='fail'?'248,113,113':'251,191,36')+', .25);border-radius:8px;padding:8px 12px;margin-bottom:6px;font-size:12px">'+
          ic+' <b>'+r.test+'</b>: '+r.message+
          (r.fix?'<div style="color:#fbbf24;font-size:11px;margin-top:3px">💡 '+r.fix+'</div>':'')+
          '</div>';
      }}).join('');
    }} else {{
      preview.innerHTML='<div style="color:#34d399;font-size:12px;padding:8px">✅ All workflow tests passing</div>';
    }}
  }}
}}).catch(function(){{
  var badge=document.getElementById('wf-badge');
  if(badge){{badge.textContent='unavailable';badge.style.color='var(--tx2)';}}
}});

function runWorkflowTests(){{
  var btn=document.getElementById('wf-run-btn');
  if(btn){{btn.disabled=true;btn.querySelector('.label').textContent='⏳ Running…';}}
  fetch('/api/qa/workflow',{{method:'POST',credentials:'same-origin'}}).then(r=>r.json()).then(function(d){{
    var sc=d.score||0;
    var col=sc>=90?'#34d399':sc>=70?'#fbbf24':'#f87171';
    var bg=sc>=90?'rgba(52,211,153,.15)':sc>=70?'rgba(251,191,36,.15)':'rgba(248,113,113,.15)';
    var badge=document.getElementById('wf-badge');
    if(badge){{badge.textContent=sc+'/100 '+d.grade;badge.style.background=bg;badge.style.color=col;}}
    if(btn){{btn.disabled=false;btn.querySelector('.label').textContent='▶ Run Now';}}
    location.href='/qa/workflow';
  }}).catch(function(){{if(btn){{btn.disabled=false;btn.querySelector('.label').textContent='▶ Run Now';}}}} );
}}

function apiPost(url, body) {{
  RC.textContent = 'Loading...'; R.style.display = 'block';
  fetch(url, {{
    method: 'POST',
    headers: {{ 'Content-Type': 'application/json' }},
    body: body ? JSON.stringify(body) : '{{}}'
  }}).then(r => r.json()).then(showResult).catch(e => showResult('Error: ' + e));
}}

function runWorkflow(name) {{
  let inputs = {{}};
  if (name === 'pc_pipeline') {{
    const pcid = prompt('Enter Price Check ID (e.g. pc_abc123):');
    if (!pcid) return;
    inputs = {{ pc_id: pcid }};
  }} else if (name === 'quote_pipeline') {{
    const pcid = prompt('Enter Price Check ID for quote:');
    if (!pcid) return;
    inputs = {{ pc_id: pcid }};
  }}
  apiPost('/api/workflow/run', {{ workflow: name, inputs: inputs }});
}}

function draftForPc() {{
  const pcid = prompt('Enter Price Check ID (e.g. pc_abc123):');
  if (!pcid) return;
  const qnum = prompt('Quote number (e.g. R26Q16):', '');
  apiPost('/api/outbox/draft/pc/' + pcid, {{ quote_number: qnum || '' }});
}}

function identifyItem() {{
  const desc = prompt('Item description (e.g. "Engraved two line name tag, black/white"):');
  if (!desc) return;
  apiPost('/api/identify', {{ description: desc }});
}}

function identifyPc() {{
  const pcid = prompt('Enter Price Check ID:');
  if (!pcid) return;
  apiGet('/api/identify/pc/' + pcid);
}}

function sendApproved() {{
  if (!confirm('Send ALL approved emails in outbox?')) return;
  apiPost('/api/outbox/send-approved');
}}

function testCall() {{
  const phone = prompt('Phone number to call (E.164 format, e.g. +19491234567):');
  if (!phone) return;
  const script = prompt('Script: lead_intro, follow_up, intro_cold, thank_you (default: lead_intro):', 'lead_intro') || 'lead_intro';
  apiPost('/api/voice/call', {{ phone: phone, script: script, variables: {{ po_number: 'PO-TEST', institution: 'CSP-Sacramento', quote_number: 'R26Q1' }} }});
}}

function importTwilio() {{
  if (!confirm('Import your Twilio phone number into Vapi? This lets outbound calls show your Reytech caller ID.')) return;
  apiPost('/api/voice/import-twilio', {{}});
}}

function predictWin() {{
  const inst = prompt('Institution (e.g. CSP-Sacramento):');
  if (!inst) return;
  const agency = prompt('Agency (e.g. CDCR):', '') || '';
  const value = prompt('Estimated PO value ($):', '5000') || '0';
  apiGet('/api/predict/win?institution=' + encodeURIComponent(inst) + '&agency=' + encodeURIComponent(agency) + '&value=' + value);
}}

function testShipping() {{
  const subject = prompt('Email subject:', 'Your Amazon order has shipped');
  if (!subject) return;
  const body = prompt('Email body (paste tracking #):', 'Your order has shipped. Tracking: TBA123456789012. Estimated delivery: Feb 20.');
  apiPost('/api/shipping/scan-email', {{subject: subject, body: body, sender: 'ship-confirm@amazon.com'}});
}}

// Load fleet status on page load
fetch('/api/agents/status',{{credentials:'same-origin'}}).then(r => r.json()).then(data => {{
  const grid = document.getElementById('fleet-grid');
  const tag = document.getElementById('fleet-tag');
  if (!data.ok) {{ grid.innerHTML = '<div>Failed to load</div>'; return; }}
  tag.textContent = data.active + '/' + data.total + ' active';
  tag.className = 'tag ' + (data.active > 5 ? 'tag-ok' : 'tag-warn');

  let html = '';
  for (const [name, info] of Object.entries(data.agents)) {{
    const isOff = info.status === 'not_available';
    const mode = info.mode || info.status || (info.configured === false ? 'not configured' : 'ready');
    const dot = isOff ? '⚫' : (info.configured === false || info.api_key_set === false ? '🟡' : '🟢');
    html += '<div class="agent-card"><div class="name">' + dot + ' ' + name.replace(/_/g,' ') + '</div>';
    html += '<div class="mode">' + mode + '</div></div>';
  }}
  grid.innerHTML = html;

  // Update voice tag
  const vt = document.getElementById('voice-tag');
  const voice = data.agents.voice_calls || {{}};
  if (voice.vapi_configured) {{ vt.textContent = 'Vapi AI'; vt.className = 'tag tag-ok'; }}
  else if (voice.twilio_configured) {{ vt.textContent = 'Twilio TTS'; vt.className = 'tag tag-warn'; }}
  else if (voice.status !== 'not_available') {{ vt.textContent = 'needs setup'; vt.className = 'tag tag-warn'; }}

  // Update QB tag
  const qt = document.getElementById('qb-tag');
  const qb = data.agents.quickbooks || {{}};
  if (qb.has_valid_token) {{ qt.textContent = 'connected'; qt.className = 'tag tag-ok'; }}
  else if (qb.configured) {{ qt.textContent = 'token expired'; qt.className = 'tag tag-warn'; }}
  else if (qb.status !== 'not_available') {{ qt.textContent = 'not connected'; qt.className = 'tag tag-warn'; }}

  // Update QA tag with health score
  const qaInfo = data.agents.qa || {{}};
  const qaTag = document.getElementById('qa-tag');
  if (qaInfo.last_grade && qaInfo.last_grade !== '—') {{
    const gColor = {{'A':'tag-ok','B':'tag-ok','C':'tag-warn','D':'tag-warn','F':'tag-off'}}[qaInfo.last_grade] || 'tag-warn';
    qaTag.textContent = 'Grade: ' + qaInfo.last_grade + ' (' + qaInfo.last_score + ')';
    qaTag.className = 'tag ' + gColor;
  }}

  // Load QB financial snapshot
  if (qb.has_valid_token) {{
    fetch('/api/qb/financial-context',{{credentials:'same-origin'}}).then(r=>r.json()).then(fc=>{{
      if(!fc.ok) return;
      const el = document.getElementById('qb-finance');
      el.style.display = 'grid';
      el.style.gridTemplateColumns = 'repeat(4,1fr)';
      el.style.gap = '8px';
      const mkC = (l,v,c) => '<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">'+l+'</div><div style="font-size:16px;font-weight:700;color:'+(c||'var(--tx)')+'">'+v+'</div></div>';
      el.innerHTML = mkC('Receivable','$'+(fc.total_receivable||0).toLocaleString(),'var(--yl)')
        + mkC('Overdue','$'+(fc.overdue_amount||0).toLocaleString(),'var(--rd)')
        + mkC('Collected','$'+(fc.total_collected||0).toLocaleString(),'var(--gn)')
        + mkC('Open Inv',fc.open_invoices||0,'var(--ac)');
    }}).catch(()=>{{}});
  }}
}}).catch(() => {{
  document.getElementById('fleet-grid').innerHTML = '<div>Failed to load fleet status</div>';
}});
</script>
</body></html>"""

# ── CRM / Contacts Page ──────────────────────────────────────────────────────
PAGE_CRM = """
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;flex-wrap:wrap;gap:10px">
 <div>
  <h2 style="font-size:20px;font-weight:700;margin-bottom:4px">👥 CRM — Buyers & Contacts</h2>
  <p style="font-size:13px;color:var(--tx2)">SCPRS-sourced buyers tagged by category, items purchased, and annual spend. Correlates to growth pipeline.</p>
 </div>
 <div style="display:flex;gap:8px;flex-wrap:wrap">
  <a href="/growth" class="btn btn-s" style="padding:10px 16px;font-size:13px">🚀 Growth Engine</a>
  <a href="/intelligence" class="btn btn-s" style="padding:10px 16px;font-size:13px">🧠 Intel / Deep Pull</a>
  <a href="/" class="btn btn-s" style="padding:10px 16px;font-size:13px">← Home</a>
 </div>
</div>

<!-- CRM Summary Stats -->
<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:20px" id="crm-stats">
 <div class="card" style="text-align:center;padding:16px;margin:0">
  <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px">Total Buyers</div>
  <div id="stat-total" style="font-size:28px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--ac)">—</div>
 </div>
 <div class="card" style="text-align:center;padding:16px;margin:0">
  <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px">Agencies</div>
  <div id="stat-agencies" style="font-size:28px;font-weight:700;font-family:'JetBrains Mono',monospace;color:#a78bfa">—</div>
 </div>
 <div class="card" style="text-align:center;padding:16px;margin:0">
  <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px">Total SCPRS Spend</div>
  <div id="stat-spend" style="font-size:28px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--yl)">—</div>
 </div>
 <div class="card" style="text-align:center;padding:16px;margin:0">
  <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px">In Outreach</div>
  <div id="stat-outreach" style="font-size:28px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--or)">—</div>
 </div>
 <div class="card" style="text-align:center;padding:16px;margin:0">
  <div style="font-size:9px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px">Won</div>
  <div id="stat-won" style="font-size:28px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--gn)">—</div>
 </div>
</div>

<!-- Filters -->
<div class="card" style="padding:14px 18px;margin-bottom:16px">
 <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
  <input id="crm-search" placeholder="Search agency, buyer name, email, item..." style="flex:1;min-width:240px;padding:10px 14px;background:var(--sf2);border:1px solid var(--bd);border-radius:8px;color:var(--tx);font-size:14px" oninput="filterTable()">
  <select id="crm-cat" style="padding:10px 12px;background:var(--sf2);border:1px solid var(--bd);border-radius:8px;color:var(--tx);font-size:13px" onchange="filterTable()">
   <option value="">All Categories</option>
   <option value="Medical">Medical</option>
   <option value="Janitorial">Janitorial</option>
   <option value="Office">Office</option>
   <option value="IT">IT</option>
   <option value="Facility">Facility</option>
   <option value="Safety">Safety</option>
  </select>
  <select id="crm-status" style="padding:10px 12px;background:var(--sf2);border:1px solid var(--bd);border-radius:8px;color:var(--tx);font-size:13px" onchange="filterTable()">
   <option value="">All Statuses</option>
   <option value="new">New</option>
   <option value="emailed">Emailed</option>
   <option value="called">Called</option>
   <option value="responded">Responded</option>
   <option value="won">Won</option>
   <option value="lost">Lost</option>
  </select>
  <span id="crm-count" style="font-size:12px;color:var(--tx2);white-space:nowrap;font-family:'JetBrains Mono',monospace"></span>
 </div>
</div>

<!-- Buyers Table -->
<div class="card" style="padding:0;overflow:hidden">
 <div style="overflow-x:auto">
  <table class="home-tbl" id="crm-table">
   <thead>
    <tr>
     <th style="padding:12px 14px">Agency</th>
     <th style="padding:12px 14px">Buyer</th>
     <th style="padding:12px 14px">Email</th>
     <th style="padding:12px 14px">Categories</th>
     <th style="padding:12px 14px">Items Bought (SCPRS)</th>
     <th style="padding:12px 14px;text-align:right">Annual Spend</th>
     <th style="padding:12px 14px">Score</th>
     <th style="padding:12px 14px;text-align:center">Status</th>
     <th style="padding:12px 14px;text-align:center">Action</th>
    </tr>
   </thead>
   <tbody id="crm-body">
    <tr><td colspan="9" style="text-align:center;padding:40px;color:var(--tx2)">Loading buyer data...</td></tr>
   </tbody>
  </table>
 </div>
</div>

<!-- No Data State -->
<div id="crm-empty" style="display:none" class="card">
 <div style="text-align:center;padding:48px 20px">
  <div style="font-size:40px;margin-bottom:12px">🔍</div>
  <div style="font-size:16px;font-weight:600;margin-bottom:8px">No buyers yet</div>
  <p style="font-size:13px;color:var(--tx2);margin-bottom:16px">Run a Deep Pull on the Intelligence page to discover all SCPRS buyers who purchase the same items as Reytech. The CRM will auto-populate with buyer names, emails, categories, items, and spend data.</p>
  <a href="/intelligence" class="btn btn-p" style="padding:12px 24px;font-size:14px">🧠 Run Deep Pull → Populate CRM</a>
 </div>
</div>

<script>
var ALL_BUYERS = [];

// Status badge colors
var STATUS_COLORS = {
 'new': {bg:'rgba(79,140,255,.15)',color:'#4f8cff'},
 'emailed': {bg:'rgba(251,191,36,.15)',color:'#fbbf24'},
 'called': {bg:'rgba(251,146,60,.15)',color:'#fb923c'},
 'responded': {bg:'rgba(167,139,250,.15)',color:'#a78bfa'},
 'won': {bg:'rgba(52,211,153,.2)',color:'#34d399'},
 'lost': {bg:'rgba(248,113,113,.15)',color:'#f87171'},
 'bounced': {bg:'rgba(139,144,160,.15)',color:'#8b90a0'},
};

function statusBadge(s) {
 var c = STATUS_COLORS[s] || {bg:'rgba(139,144,160,.15)',color:'#8b90a0'};
 return '<span style="padding:3px 10px;border-radius:12px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;background:'+c.bg+';color:'+c.color+'">'+(s||'new')+'</span>';
}

function catTag(cat) {
 var colors = {'Medical':'#f87171','Janitorial':'#3fb950','Office':'#4f8cff','IT':'#a78bfa','Facility':'#fb923c','Safety':'#fbbf24'};
 var c = colors[cat] || '#8b90a0';
 return '<span style="font-size:10px;padding:2px 8px;border-radius:10px;background:'+c+'22;color:'+c+';font-weight:600;border:1px solid '+c+'44;white-space:nowrap">'+cat+'</span>';
}

function scoreBar(score) {
 var pct = Math.round((score||0)*100);
 var color = pct>=70?'#3fb950':pct>=40?'#fbbf24':'#f87171';
 return '<div style="display:flex;align-items:center;gap:6px"><div style="width:52px;background:var(--sf2);border-radius:4px;height:6px;overflow:hidden"><div style="width:'+pct+'%;height:100%;background:'+color+';border-radius:4px"></div></div><span style="font-size:11px;font-family:JetBrains Mono,monospace;color:'+color+'">'+pct+'%</span></div>';
}

function fmtSpend(v) {
 if (!v) return '<span style="color:var(--tx2)">—</span>';
 if (v >= 1e6) return '<span style="color:var(--yl);font-weight:700;font-family:JetBrains Mono,monospace">$'+(v/1e6).toFixed(1)+'M</span>';
 if (v >= 1e3) return '<span style="color:var(--yl);font-weight:700;font-family:JetBrains Mono,monospace">$'+(v/1e3).toFixed(0)+'K</span>';
 return '<span style="font-family:JetBrains Mono,monospace">$'+v.toLocaleString()+'</span>';
}

function renderTable(buyers) {
 var tbody = document.getElementById('crm-body');
 document.getElementById('crm-count').textContent = buyers.length + ' buyers';
 if (!buyers.length) {
  tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:32px;color:var(--tx2)">No buyers match filter</td></tr>';
  return;
 }
 tbody.innerHTML = buyers.map(function(b) {
  var cats = (b.categories||[]).map(catTag).join(' ');
  var items = (b.items||[]).slice(0,3).map(function(it){
   return '<span style="font-size:11px;color:var(--tx2)">'+it+'</span>';
  }).join(', ') + ((b.items||[]).length>3?' <span style="font-size:10px;color:var(--ac)">+'+((b.items||[]).length-3)+' more</span>':'');
  var pid = b.id || b.prospect_id || '';
  var detailLink = pid ? '/growth/prospect/'+pid : '#';
  return '<tr class="home-row" onclick="location.href=\\\''+detailLink+'\'">'
   + '<td style="padding:12px 14px;font-weight:600;font-size:13px">'+b.agency+'</td>'
   + '<td style="padding:12px 14px;font-size:13px">'+(b.buyer_name||'—')+'</td>'
   + '<td style="padding:12px 14px"><a href="mailto:'+(b.buyer_email||'')
     +'" onclick="event.stopPropagation()" style="font-size:12px;font-family:JetBrains Mono,monospace;color:var(--ac)">'
     +(b.buyer_email||'—')+'</a></td>'
   + '<td style="padding:12px 14px"><div style="display:flex;gap:4px;flex-wrap:wrap">'+cats+'</div></td>'
   + '<td style="padding:12px 14px;max-width:220px;overflow:hidden"><div style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+items+'</div></td>'
   + '<td style="padding:12px 14px;text-align:right">'+fmtSpend(b.annual_spend||b.spend||0)+'</td>'
   + '<td style="padding:12px 14px">'+scoreBar(b.score||0)+'</td>'
   + '<td style="padding:12px 14px;text-align:center">'+statusBadge(b.outreach_status||'new')+'</td>'
   + '<td style="padding:12px 14px;text-align:center"><a href="'+detailLink+'" onclick="event.stopPropagation()" class="btn btn-sm btn-s">View</a></td>'
   + '</tr>';
 }).join('');
}

function filterTable() {
 var q = document.getElementById('crm-search').value.toLowerCase();
 var cat = document.getElementById('crm-cat').value;
 var status = document.getElementById('crm-status').value;
 var filtered = ALL_BUYERS.filter(function(b) {
  var matchQ = !q ||
   (b.agency||'').toLowerCase().includes(q) ||
   (b.buyer_name||'').toLowerCase().includes(q) ||
   (b.buyer_email||'').toLowerCase().includes(q) ||
   (b.items||[]).some(function(it){return it.toLowerCase().includes(q);});
  var matchCat = !cat || (b.categories||[]).includes(cat);
  var matchStatus = !status || (b.outreach_status||'new')===status;
  return matchQ && matchCat && matchStatus;
 });
 renderTable(filtered);
}

// Load buyers from growth prospects API
fetch('/api/growth/status', {credentials:'same-origin'}).then(r=>r.json()).then(function(d) {
 var buyers = d.prospects || d.pipeline || [];
 ALL_BUYERS = buyers;

 if (!buyers.length) {
  document.getElementById('crm-table').style.display = 'none';
  document.getElementById('crm-empty').style.display = 'block';
  document.getElementById('stat-total').textContent = '0';
  document.getElementById('stat-agencies').textContent = '0';
  document.getElementById('stat-spend').textContent = '$0';
  document.getElementById('stat-outreach').textContent = '0';
  document.getElementById('stat-won').textContent = '0';
  return;
 }

 // Stats
 var agencies = new Set(buyers.map(function(b){return b.agency})).size;
 var totalSpend = buyers.reduce(function(s,b){return s+(b.annual_spend||b.spend||0)}, 0);
 var inOutreach = buyers.filter(function(b){return b.outreach_status&&b.outreach_status!=='new'}).length;
 var won = buyers.filter(function(b){return b.outreach_status==='won'}).length;

 document.getElementById('stat-total').textContent = buyers.length;
 document.getElementById('stat-agencies').textContent = agencies;
 document.getElementById('stat-spend').textContent = totalSpend>=1e6?'$'+(totalSpend/1e6).toFixed(1)+'M':totalSpend>=1e3?'$'+(totalSpend/1e3).toFixed(0)+'K':'$'+totalSpend;
 document.getElementById('stat-outreach').textContent = inOutreach;
 document.getElementById('stat-won').textContent = won;

 // Populate category filter with actual categories
 var cats = new Set();
 buyers.forEach(function(b){(b.categories||[]).forEach(function(c){cats.add(c)})});
 var sel = document.getElementById('crm-cat');
 sel.innerHTML = '<option value="">All Categories</option>';
 Array.from(cats).sort().forEach(function(c){
  sel.innerHTML += '<option value="'+c+'">'+c+'</option>';
 });

 renderTable(buyers);
}).catch(function() {
 document.getElementById('crm-table').style.display = 'none';
 document.getElementById('crm-empty').style.display = 'block';
});
</script>
"""

DEBUG_PAGE_HTML = """
<style>
:root{--ok:#3fb950;--warn:#e3b341;--fail:#f85149;--info:#58a6ff;--neu:#8b949e}
.dbg-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px;margin-top:16px}
.dbg-card{background:var(--bg2);border:1px solid var(--bd);border-radius:12px;padding:20px;position:relative;overflow:hidden}
.dbg-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:var(--card-color,var(--ac))}
.dbg-title{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1.2px;color:var(--tx2);margin-bottom:14px;display:flex;align-items:center;gap:8px}
.dbg-title .dot{width:8px;height:8px;border-radius:50%;background:var(--card-color,var(--ac));flex-shrink:0}
.dbg-row{display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid var(--bd);font-size:13px}
.dbg-row:last-child{border-bottom:none}
.dbg-key{color:var(--tx2);font-size:12px}
.dbg-val{font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:600}
.badge{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:600}
.badge-ok{background:rgba(63,185,80,.15);color:var(--ok)}
.badge-warn{background:rgba(227,179,65,.15);color:var(--warn)}
.badge-fail{background:rgba(248,81,73,.15);color:var(--fail)}
.badge-info{background:rgba(88,166,255,.15);color:var(--info)}
.badge-neu{background:rgba(139,148,158,.12);color:var(--neu)}
.fix-btn{display:inline-flex;align-items:center;gap:6px;padding:6px 14px;border-radius:8px;font-size:12px;font-weight:600;border:1px solid var(--bd);background:var(--bg);color:var(--tx);cursor:pointer;transition:all .15s}
.fix-btn:hover{border-color:var(--ac);color:var(--ac);background:rgba(79,140,255,.06)}
.fix-btn:disabled{opacity:.4;cursor:not-allowed}
.fix-btn.running{border-color:var(--warn);color:var(--warn)}
.pulse{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--ok);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(1.3)}}
.section-hdr{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--tx2);margin:24px 0 12px;display:flex;align-items:center;gap:8px}
.section-hdr::after{content:'';flex:1;height:1px;background:var(--bd)}
.log-panel{background:var(--bg);border:1px solid var(--bd);border-radius:8px;padding:14px;font-family:'JetBrains Mono',monospace;font-size:11px;max-height:220px;overflow-y:auto;color:var(--tx2);line-height:1.6}
.log-ok{color:var(--ok)}.log-warn{color:var(--warn)}.log-fail{color:var(--fail)}.log-info{color:var(--info)}
.refresh-bar{display:flex;align-items:center;gap:12px;margin-bottom:16px}
.last-run{font-size:11px;color:var(--tx2)}
#score-ring{position:relative;display:inline-flex;align-items:center;justify-content:center}
.table-sm{width:100%;border-collapse:collapse;font-size:12px}
.table-sm td,.table-sm th{padding:5px 8px;border-bottom:1px solid var(--bd);text-align:left}
.table-sm th{color:var(--tx2);font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px}
.table-sm tr:last-child td{border-bottom:none}
</style>

<div class="refresh-bar">
  <h2 style="margin:0;font-size:18px;font-weight:700">🔬 Debug Agent</h2>
  <span style="flex:1"></span>
  <span id="last-run-ts" class="last-run">Loading...</span>
  <button class="fix-btn" onclick="runDebug()" id="refresh-btn">⟳ Refresh</button>
  <button class="fix-btn" onclick="runFix('seed_demo')" id="btn-seed">🌱 Load Demo</button>
  <button class="fix-btn" onclick="runFix('sync_crm')" id="btn-sync">🔄 Sync CRM</button>
  <button class="fix-btn" onclick="runFix('clear_cache')" id="btn-cache">🗑 Clear Cache</button>
</div>

<!-- Health Score Banner -->
<div id="health-banner" class="card" style="padding:16px 24px;margin-bottom:8px;display:flex;align-items:center;gap:20px">
  <div style="text-align:center;min-width:80px">
    <div id="qa-score" style="font-size:42px;font-weight:800;font-family:'JetBrains Mono',monospace;line-height:1">—</div>
    <div style="font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-top:2px">Health Score</div>
  </div>
  <div style="width:1px;height:50px;background:var(--bd)"></div>
  <div style="flex:1">
    <div id="qa-grade" style="font-size:13px;font-weight:600;margin-bottom:6px">—</div>
    <div id="qa-issues" style="font-size:12px;color:var(--tx2)">Running checks...</div>
  </div>
  <div id="persistence-badge" style="text-align:right">
    <div style="font-size:11px;color:var(--tx2);margin-bottom:4px">Persistence</div>
    <div id="vol-status">—</div>
  </div>
</div>

<div class="dbg-grid" id="dbg-grid">
  <div class="dbg-card" style="--card-color:var(--ac)">
    <div class="dbg-title"><span class="dot"></span>Loading checks...</div>
  </div>
</div>

<div class="section-hdr">📋 QA Check Log</div>
<div class="log-panel" id="qa-log">Fetching QA report...</div>

<div class="section-hdr">🔧 Available Fixes</div>
<div style="display:flex;flex-wrap:wrap;gap:10px;margin-bottom:24px" id="fixes-panel">
  <button class="fix-btn" onclick="runFix('seed_demo')">🌱 Seed Demo Data</button>
  <button class="fix-btn" onclick="runFix('sync_crm')">🔄 Sync Intel → CRM</button>
  <button class="fix-btn" onclick="runFix('clear_cache')">🗑 Clear JSON Cache</button>
  <button class="fix-btn" onclick="runFix('migrate_db')">🗄 Migrate JSON → DB</button>
  <button class="fix-btn" onclick="window.open('/api/qa/health','_blank')">📊 Full QA Health</button>
  <button class="fix-btn" onclick="window.open('/api/metrics','_blank')">📈 Metrics API</button>
  <button class="fix-btn" onclick="window.open('/api/db','_blank')">🗃 DB Status</button>
  <button class="fix-btn" onclick="window.open('/api/search?q=test','_blank')">🔍 Test Search</button>
</div>

<div id="fix-result" style="display:none" class="card" style="padding:12px 16px;margin-bottom:16px;font-family:'JetBrains Mono',monospace;font-size:12px"></div>

<script>
let lastData = null;

function fmt(v){
  if(v===null||v===undefined) return '<span style="color:var(--tx2)">—</span>';
  if(typeof v==='boolean') return v?'<span class="badge badge-ok">✓ yes</span>':'<span class="badge badge-fail">✗ no</span>';
  if(typeof v==='number') return '<b>'+v.toLocaleString()+'</b>';
  return '<span>'+v+'</span>';
}

function badge(ok, label){
  const cls = ok==='ok'||ok===true ? 'badge-ok' : ok==='warn' ? 'badge-warn' : ok==='fail'||ok===false ? 'badge-fail' : 'badge-info';
  return '<span class="badge '+cls+'">'+label+'</span>';
}

function renderDebug(d){
  lastData = d;
  document.getElementById('last-run-ts').textContent = 'Updated '+new Date(d.timestamp).toLocaleTimeString()+' ('+d.elapsed_ms+'ms)';

  // Health banner
  fetch('/api/qa/health').then(r=>r.json()).then(qa=>{
    const score = qa.health_score||0;
    const el = document.getElementById('qa-score');
    el.textContent = score;
    el.style.color = score>=90?'var(--ok)':score>=70?'var(--warn)':'var(--fail)';
    document.getElementById('qa-grade').innerHTML = badge(score>=90?'ok':score>=70?'warn':'fail', 'Grade '+qa.grade) + ' &nbsp; ' + qa.summary.passed+' pass / '+qa.summary.warned+' warn / '+qa.summary.failed+' fail';
    document.getElementById('qa-issues').textContent = qa.critical_issues.length ? '⚠️ '+qa.critical_issues.join(' · ') : qa.recommendations.join(' · ') || 'No critical issues';
    
    // Log panel
    const log = document.getElementById('qa-log');
    log.innerHTML = qa.results.map(r=>{
      const cls = r.status==='pass'?'log-ok':r.status==='warn'?'log-warn':r.status==='fail'?'log-fail':'log-info';
      const icon = r.status==='pass'?'✓':r.status==='warn'?'⚠':r.status==='fail'?'✗':'ℹ';
      return '<div class="'+cls+'">'+icon+' ['+r.check.toUpperCase()+'] '+r.message+'</div>';
    }).join('');
  }).catch(()=>{});

  // Volume/persistence badge
  const vol = d.db.is_volume;
  document.getElementById('vol-status').innerHTML = vol 
    ? '<span class="badge badge-ok">💾 Volume ✓</span>'
    : '<span class="badge badge-warn">⚠ No Volume</span>';

  // Build cards
  const cards = [];

  // DB Card
  const dbTables = d.db.tables||{};
  const dbOk = d.db.ok;
  cards.push(`<div class="dbg-card" style="--card-color:${dbOk?'var(--ok)':'var(--fail)'}">
    <div class="dbg-title"><span class="dot"></span>SQLite Database</div>
    <div class="dbg-row"><span class="dbg-key">Status</span><span>${badge(dbOk?'ok':'fail', dbOk?'Online':'Error')}</span></div>
    <div class="dbg-row"><span class="dbg-key">Path</span><span class="dbg-val" style="font-size:10px;max-width:200px;overflow:hidden;text-overflow:ellipsis" title="${d.db.path}">${d.db.path.split('/').slice(-3).join('/')}</span></div>
    <div class="dbg-row"><span class="dbg-key">Size</span><span class="dbg-val">${d.db.size_kb} KB</span></div>
    <div class="dbg-row"><span class="dbg-key">Persistent</span>${fmt(d.db.is_volume)}</div>
    <div style="margin-top:10px">
      <table class="table-sm"><tr><th>Table</th><th>Rows</th></tr>
      ${Object.entries(dbTables).map(([t,n])=>`<tr><td>${t}</td><td><b>${n}</b></td></tr>`).join('')}
      </table>
    </div>
  </div>`);

  // Data Files Card
  const files = d.data_files||{};
  cards.push(`<div class="dbg-card" style="--card-color:var(--ac)">
    <div class="dbg-title"><span class="dot"></span>Data Files</div>
    <table class="table-sm"><tr><th>File</th><th>Records</th><th>KB</th></tr>
    ${Object.entries(files).map(([f,v])=>`<tr>
      <td style="font-size:11px">${f.replace('.json','')}</td>
      <td>${badge(v.exists&&v.records>0?'ok':v.records===0?'warn':'fail', v.records||0)}</td>
      <td style="color:var(--tx2)">${v.size_kb||0}</td>
    </tr>`).join('')}
    </table>
  </div>`);

  // Sync Card
  const sync = d.sync||{};
  const syncOk = sync.delta === 0;
  cards.push(`<div class="dbg-card" style="--card-color:${syncOk?'var(--ok)':'var(--warn)'}">
    <div class="dbg-title"><span class="dot"></span>Intel ↔ CRM Sync</div>
    <div class="dbg-row"><span class="dbg-key">Intel Buyers</span><span class="dbg-val">${sync.intel_buyers||0}</span></div>
    <div class="dbg-row"><span class="dbg-key">CRM Contacts</span><span class="dbg-val">${sync.crm_contacts||0}</span></div>
    <div class="dbg-row"><span class="dbg-key">Delta</span><span>${badge(syncOk?'ok':'warn', sync.delta===0?'✓ In Sync':'+'+sync.delta+' drift')}</span></div>
    <div class="dbg-row"><span class="dbg-key">Auto-Seed</span><span>${badge(d.auto_seed?.needed?'warn':'ok', d.auto_seed?.status||'—')}</span></div>
    ${sync.delta>0?`<button class="fix-btn" style="margin-top:12px;width:100%" onclick="runFix('sync_crm')">🔄 Sync Now</button>`:''}
  </div>`);

  // Quote Counter Card
  cards.push(`<div class="dbg-card" style="--card-color:#a78bfa">
    <div class="dbg-title"><span class="dot"></span>Quote Engine</div>
    <div class="dbg-row"><span class="dbg-key">Next Quote</span><span class="dbg-val" style="color:#a78bfa;font-size:15px">${d.quote_counter?.next||'—'}</span></div>
    <div class="dbg-row"><span class="dbg-key">Total Quotes</span><span class="dbg-val">${d.funnel?.quotes_total||0}</span></div>
    <div class="dbg-row"><span class="dbg-key">Sent</span><span class="dbg-val">${d.funnel?.quotes_sent||0}</span></div>
    <div class="dbg-row"><span class="dbg-key">Won</span><span class="dbg-val" style="color:var(--ok)">${d.funnel?.quotes_won||0}</span></div>
    <div class="dbg-row"><span class="dbg-key">Orders</span><span class="dbg-val">${d.funnel?.orders||0}</span></div>
  </div>`);

  // Modules Card
  const mods = d.modules||{};
  cards.push(`<div class="dbg-card" style="--card-color:var(--gn)">
    <div class="dbg-title"><span class="dot"></span>Agent Modules</div>
    ${Object.entries(mods).map(([m,v])=>`<div class="dbg-row">
      <span class="dbg-key">${m.replace(/_/g,' ')}</span>
      ${badge(v?'ok':'fail', v?'✓ loaded':'✗ missing')}
    </div>`).join('')}
  </div>`);

  // Railway Card
  const rw = d.railway||{};
  const isRailway = rw.environment !== 'local';
  cards.push(`<div class="dbg-card" style="--card-color:${isRailway?'var(--ok)':'var(--neu)'}">
    <div class="dbg-title"><span class="dot"></span>Railway Environment</div>
    <div class="dbg-row"><span class="dbg-key">Environment</span><span>${badge(isRailway?'ok':'info', rw.environment||'local')}</span></div>
    <div class="dbg-row"><span class="dbg-key">Volume</span><span>${badge(d.db.is_volume?'ok':'warn', rw.volume_name||'not mounted')}</span></div>
    <div class="dbg-row"><span class="dbg-key">Mount Path</span><span class="dbg-val" style="font-size:11px">${rw.volume_path||'—'}</span></div>
    <div class="dbg-row"><span class="dbg-key">Deploy ID</span><span class="dbg-val" style="font-size:10px">${rw.deployment_id||'local'}</span></div>
    ${!d.db.is_volume?`<div style="margin-top:10px;padding:8px 10px;background:rgba(227,179,65,.1);border-radius:6px;font-size:11px;color:var(--warn)">⚠ Add Railway Volume → Mount: /app/data → Redeploy</div>`:''}
  </div>`);

  document.getElementById('dbg-grid').innerHTML = cards.join('');
}

function runDebug(){
  const btn = document.getElementById('refresh-btn');
  btn.textContent='⟳ Running...'; btn.disabled=true;
  fetch('/api/debug/run').then(r=>r.json()).then(d=>{
    renderDebug(d);
    btn.textContent='⟳ Refresh'; btn.disabled=false;
  }).catch(e=>{
    btn.textContent='⟳ Refresh'; btn.disabled=false;
    console.error(e);
  });
}

function runFix(name){
  const panel = document.getElementById('fix-result');
  panel.style.display='block';
  panel.innerHTML='⏳ Running '+name+'...';
  panel.style.borderLeft='3px solid var(--warn)';
  fetch('/api/debug/fix/'+name,{method:'POST'}).then(r=>r.json()).then(d=>{
    panel.style.borderLeft='3px solid '+(d.ok?'var(--ok)':'var(--fail)');
    panel.innerHTML = (d.ok?'✓ ':'✗ ') + name + ': ' + JSON.stringify(d.result||d.error||d,null,2).substring(0,300);
    setTimeout(()=>runDebug(),500);
  }).catch(e=>{
    panel.innerHTML='✗ Error: '+e;
    panel.style.borderLeft='3px solid var(--fail)';
  });
}

// Auto-refresh every 30 seconds
runDebug();
setInterval(runDebug, 30000);
</script>
"""
