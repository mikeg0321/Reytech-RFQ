#!/usr/bin/env python3
"""
Reytech RFQ Dashboard v2
Full automation: email polling → parse → SCPRS lookup → price → generate → draft email
Production-ready: password protected, env var config, gunicorn-compatible
"""

import os, json, uuid, sys, threading, time, logging, functools
from datetime import datetime, timezone, timedelta
from flask import (Flask, request, redirect, url_for, render_template_string,
                   send_file, jsonify, flash, Response)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rfq_parser import parse_rfq_attachments, identify_attachments
from reytech_filler_v4 import (load_config, get_pst_date, fill_703b, fill_704b, fill_bid_package)
from scprs_lookup import bulk_lookup, save_prices_from_rfq, get_price_db_stats
from email_poller import EmailPoller, EmailSender

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("dashboard")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "reytech-rfq-2026")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DATA_DIR = os.path.join(BASE_DIR, "data")
for d in [UPLOAD_DIR, OUTPUT_DIR, DATA_DIR]:
    os.makedirs(d, exist_ok=True)

CONFIG = load_config()

# Override config with env vars if present (for production)
if os.environ.get("GMAIL_PASSWORD"):
    CONFIG.setdefault("email", {})["email_password"] = os.environ["GMAIL_PASSWORD"]
if os.environ.get("GMAIL_ADDRESS"):
    CONFIG.setdefault("email", {})["email"] = os.environ["GMAIL_ADDRESS"]

POLL_STATUS = {"running": False, "last_check": None, "emails_found": 0, "error": None}

# ═══════════════════════════════════════════════════════════════════════
# Password Protection
# ═══════════════════════════════════════════════════════════════════════

DASH_USER = os.environ.get("DASH_USER", "reytech")
DASH_PASS = os.environ.get("DASH_PASS", "changeme")

def check_auth(username, password):
    return username == DASH_USER and password == DASH_PASS

def auth_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return Response(
                "🔒 Reytech RFQ Dashboard — Login Required",
                401, {"WWW-Authenticate": 'Basic realm="Reytech RFQ Dashboard"'})
        return f(*args, **kwargs)
    return decorated

# ═══════════════════════════════════════════════════════════════════════
# Data Layer
# ═══════════════════════════════════════════════════════════════════════

def rfq_db_path(): return os.path.join(DATA_DIR, "rfqs.json")
def load_rfqs():
    p = rfq_db_path()
    return json.load(open(p)) if os.path.exists(p) else {}
def save_rfqs(rfqs):
    json.dump(rfqs, open(rfq_db_path(), "w"), indent=2, default=str)

# ═══════════════════════════════════════════════════════════════════════
# Email Polling Thread
# ═══════════════════════════════════════════════════════════════════════

def email_poll_loop():
    """Background thread: check email every N seconds."""
    email_cfg = CONFIG.get("email", {})
    if not email_cfg.get("email_password"):
        POLL_STATUS["error"] = "Email password not configured"
        POLL_STATUS["running"] = False
        log.warning("Email polling disabled — no password in config")
        return
    
    interval = email_cfg.get("poll_interval_seconds", 120)
    poller = EmailPoller(email_cfg)
    POLL_STATUS["running"] = True
    
    while POLL_STATUS["running"]:
        try:
            if poller.connect():
                rfq_emails = poller.check_for_rfqs(save_dir=UPLOAD_DIR)
                POLL_STATUS["last_check"] = get_pst_date()
                POLL_STATUS["error"] = None
                
                for rfq_email in rfq_emails:
                    # Build template paths from identified attachments
                    templates = {}
                    for att in rfq_email["attachments"]:
                        if att["type"] != "unknown":
                            templates[att["type"]] = att["path"]
                    
                    if "704b" in templates:
                        rfq_data = parse_rfq_attachments(templates)
                        rfq_data["id"] = rfq_email["id"]
                        rfq_data["status"] = "new"
                        rfq_data["source"] = "email"
                        rfq_data["email_subject"] = rfq_email["subject"]
                        rfq_data["email_sender"] = rfq_email["sender_email"]
                        
                        # Auto SCPRS lookup
                        rfq_data["line_items"] = bulk_lookup(rfq_data.get("line_items", []))
                        
                        rfqs = load_rfqs()
                        rfqs[rfq_data["id"]] = rfq_data
                        save_rfqs(rfqs)
                        POLL_STATUS["emails_found"] += 1
                        log.info(f"Auto-imported RFQ #{rfq_data.get('solicitation_number')}")
                
                poller.disconnect()
        except Exception as e:
            POLL_STATUS["error"] = str(e)
            log.error(f"Poll error: {e}")
        
        time.sleep(interval)

# ═══════════════════════════════════════════════════════════════════════
# HTML Templates
# ═══════════════════════════════════════════════════════════════════════

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
.scprs-tag{font-size:9px;padding:2px 5px;border-radius:3px;margin-left:4px;font-weight:600}
.scprs-hi{background:rgba(52,211,153,.15);color:var(--gn)}
.scprs-med{background:rgba(251,191,36,.15);color:var(--yl)}
"""

PAGE_HOME = """
<div class="card">
 <div class="card-t">New RFQ</div>
 <form method="POST" action="/upload" enctype="multipart/form-data" id="uf">
  <div class="upl" id="dz" onclick="document.getElementById('fi').click()">
   <h3>Drop RFQ attachments here</h3>
   <p>Upload 3 PDFs (703B, 704B, Bid Package) or let email polling handle it</p>
   <input type="file" id="fi" name="files" multiple accept=".pdf" style="display:none">
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
  <button class="btn btn-sm btn-p" onclick="lookupScprs()" style="margin-left:8px">🔍 SCPRS Lookup</button>
 </div>

 <form method="POST" action="/rfq/{{rid}}/update" id="pf">
 <table class="it">
  <thead><tr>
   <th>#</th><th>Qty</th><th>Description</th><th>Part #</th>
   <th>Your Cost</th><th>SCPRS Last</th><th>Bid Price</th><th>Subtotal</th><th>Margin</th>
  </tr></thead>
  <tbody>
  {% for i in r.line_items %}
  <tr>
   <td>{{i.line_number}}</td>
   <td>{{i.qty}} {{i.uom}}</td>
   <td style="max-width:200px;font-size:11px">{{i.description.split('\\n')[0]}}</td>
   <td class="mono">{{i.item_number}}</td>
   <td><input type="number" step="0.01" name="cost_{{loop.index0}}" value="{{i.supplier_cost or ''}}" placeholder="0.00" oninput="recalc()"></td>
   <td>
    <input type="number" step="0.01" name="scprs_{{loop.index0}}" value="{{i.scprs_last_price or ''}}" placeholder="—" oninput="recalc()">
    {% if i.scprs_source %}<span class="scprs-tag scprs-{{'hi' if i.scprs_confidence=='high' else 'med'}}">{{i.scprs_source|replace('_',' ')}}</span>{% endif %}
   </td>
   <td><input type="number" step="0.01" name="price_{{loop.index0}}" value="{{i.price_per_unit or ''}}" placeholder="0.00" oninput="recalc()"></td>
   <td class="mono" id="sub_{{loop.index0}}">—</td>
   <td id="mg_{{loop.index0}}" style="font-weight:600;font-size:12px">—</td>
  </tr>
  {% endfor %}
  </tbody>
 </table>

 <div style="display:flex;justify-content:space-between;align-items:center;margin-top:14px;padding-top:14px;border-top:1px solid var(--bd)">
  <div><span style="color:var(--tx2);font-size:13px">Total: </span><span id="tot" style="font-family:'JetBrains Mono',monospace;font-size:20px;font-weight:700">$0</span></div>
  <div id="pft" style="font-size:13px">—</div>
 </div>

 <div class="bg">
  <button type="submit" class="btn btn-p">💾 Save Pricing</button>
  <button type="submit" formaction="/rfq/{{rid}}/generate" class="btn btn-g">⚡ Generate Bid Package</button>
 </div>
 </form>
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
  if(m!==null){el.textContent=m.toFixed(1)+'%';el.className=m>=20?'g-good':m>=10?'g-low':'g-bad'}
  else{el.textContent='—';el.className=''}
 }
 document.getElementById('tot').textContent=`$${tb.toFixed(2)}`;
 const pr=tb-tc;
 const pe=document.getElementById('pft');
 if(tb>0&&tc>0){const pp=(pr/tb*100).toFixed(1);pe.textContent=`Profit: $${pr.toFixed(2)} (${pp}%)`;pe.className=pr>=100?'g-good':pr>=50?'g-low':'g-bad'}
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
 fetch('/api/scprs/{{rid}}').then(r=>r.json()).then(d=>{
  if(d.results){
   d.results.forEach((r,i)=>{
    if(r.price){document.querySelector(`[name=scprs_${i}]`).value=r.price.toFixed(2)}
   });
   recalc();
  }
 });
}

recalc();
</script>
"""

# ═══════════════════════════════════════════════════════════════════════
# Routes
# ═══════════════════════════════════════════════════════════════════════

def render(content, **kw):
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Reytech RFQ</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>{BASE_CSS}</style></head><body>
<div class="hdr"><h1><span>Reytech</span> RFQ Dashboard</h1>
<div class="hdr-right">
 <div><span class="poll-dot {'poll-on' if POLL_STATUS['running'] else 'poll-off' if not CONFIG.get('email',{{}}).get('email_password') else 'poll-wait'}"></span>
 {'Polling' if POLL_STATUS['running'] else 'Email not configured' if not CONFIG.get('email',{{}}).get('email_password') else 'Starting...'}</div>
 <div>{get_pst_date()}</div>
</div></div>
<div class="ctr">
{{% with messages = get_flashed_messages(with_categories=true) %}}
 {{% for cat, msg in messages %}}<div class="alert al-{{'s' if cat=='success' else 'e' if cat=='error' else 'i'}}">{{% if cat=='success' %}}✅{{% elif cat=='error' %}}❌{{% else %}}ℹ️{{% endif %}} {{{{msg}}}}</div>{{% endfor %}}
{{% endwith %}}
""" + content + "</div></body></html>"
    return render_template_string(html, **kw)


@app.route("/")
@auth_required
def home():
    return render(PAGE_HOME, rfqs=load_rfqs())

@app.route("/upload", methods=["POST"])
@auth_required
def upload():
    files = request.files.getlist("files")
    if not files:
        flash("No files uploaded", "error"); return redirect("/")
    
    rfq_id = str(uuid.uuid4())[:8]
    rfq_dir = os.path.join(UPLOAD_DIR, rfq_id)
    os.makedirs(rfq_dir, exist_ok=True)
    
    saved = []
    for f in files:
        if f.filename and f.filename.lower().endswith(".pdf"):
            p = os.path.join(rfq_dir, f.filename)
            f.save(p); saved.append(p)
    
    if not saved:
        flash("No PDFs found", "error"); return redirect("/")
    
    templates = identify_attachments(saved)
    if "704b" not in templates:
        flash("Could not identify 704B", "error"); return redirect("/")
    
    rfq = parse_rfq_attachments(templates)
    rfq["id"] = rfq_id
    rfq["status"] = "pending"
    rfq["source"] = "upload"
    
    # Auto SCPRS lookup
    rfq["line_items"] = bulk_lookup(rfq.get("line_items", []))
    
    rfqs = load_rfqs()
    rfqs[rfq_id] = rfq
    save_rfqs(rfqs)
    
    scprs_found = sum(1 for i in rfq["line_items"] if i.get("scprs_last_price"))
    msg = f"RFQ #{rfq['solicitation_number']} parsed — {len(rfq['line_items'])} items"
    if scprs_found:
        msg += f", {scprs_found} SCPRS prices found"
    flash(msg, "success")
    return redirect(f"/rfq/{rfq_id}")


@app.route("/rfq/<rid>")
@auth_required
def detail(rid):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: flash("Not found", "error"); return redirect("/")
    return render(PAGE_DETAIL, r=r, rid=rid)


@app.route("/rfq/<rid>/update", methods=["POST"])
@auth_required
def update(rid):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return redirect("/")
    
    for i, item in enumerate(r["line_items"]):
        for field, key in [("cost", "supplier_cost"), ("scprs", "scprs_last_price"), ("price", "price_per_unit")]:
            v = request.form.get(f"{field}_{i}")
            if v:
                try: item[key] = float(v)
                except: pass
    
    r["status"] = "ready"
    save_rfqs(rfqs)
    
    # Save SCPRS prices for future lookups
    save_prices_from_rfq(r)
    
    flash("Pricing saved", "success")
    return redirect(f"/rfq/{rid}")


@app.route("/rfq/<rid>/generate", methods=["POST"])
@auth_required
def generate(rid):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return redirect("/")
    
    # Update pricing from form
    for i, item in enumerate(r["line_items"]):
        for field, key in [("cost", "supplier_cost"), ("scprs", "scprs_last_price"), ("price", "price_per_unit")]:
            v = request.form.get(f"{field}_{i}")
            if v:
                try: item[key] = float(v)
                except: pass
    
    r["sign_date"] = get_pst_date()
    sol = r["solicitation_number"]
    out = os.path.join(OUTPUT_DIR, sol)
    os.makedirs(out, exist_ok=True)
    
    try:
        t = r["templates"]
        fill_703b(t["703b"], r, CONFIG, f"{out}/{sol}_703B_Reytech.pdf")
        fill_704b(t["704b"], r, CONFIG, f"{out}/{sol}_704B_Reytech.pdf")
        fill_bid_package(t["bidpkg"], r, CONFIG, f"{out}/{sol}_BidPackage_Reytech.pdf")
        
        r["status"] = "generated"
        r["output_files"] = [f"{sol}_703B_Reytech.pdf", f"{sol}_704B_Reytech.pdf", f"{sol}_BidPackage_Reytech.pdf"]
        r["generated_at"] = datetime.now().isoformat()
        
        # Create draft email
        sender = EmailSender(CONFIG.get("email", {}))
        output_paths = [f"{out}/{f}" for f in r["output_files"]]
        r["draft_email"] = sender.create_draft_email(r, output_paths)
        
        # Save SCPRS prices
        save_prices_from_rfq(r)
        
        save_rfqs(rfqs)
        flash(f"Bid package generated for #{sol} — draft email ready", "success")
    except Exception as e:
        flash(f"Error: {e}", "error")
    
    return redirect(f"/rfq/{rid}")


@app.route("/rfq/<rid>/send", methods=["POST"])
@auth_required
def send_email(rid):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r or not r.get("draft_email"):
        flash("No draft to send", "error"); return redirect(f"/rfq/{rid}")
    
    try:
        sender = EmailSender(CONFIG.get("email", {}))
        sender.send(r["draft_email"])
        r["status"] = "sent"
        r["sent_at"] = datetime.now().isoformat()
        save_rfqs(rfqs)
        flash(f"Bid response sent to {r['draft_email']['to']}", "success")
    except Exception as e:
        flash(f"Send failed: {e}. Use 'Open in Mail App' instead.", "error")
    
    return redirect(f"/rfq/{rid}")


@app.route("/dl/<rid>/<fname>")
@auth_required
def download(rid, fname):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return redirect("/")
    p = os.path.join(OUTPUT_DIR, r["solicitation_number"], fname)
    if os.path.exists(p): return send_file(p, as_attachment=True)
    flash("File not found", "error"); return redirect(f"/rfq/{rid}")


@app.route("/api/scprs/<rid>")
@auth_required
def api_scprs(rid):
    """SCPRS lookup API endpoint for the dashboard JS."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return jsonify({"error": "not found"})
    
    results = []
    for item in r["line_items"]:
        from scprs_lookup import lookup_price
        result = lookup_price(item.get("item_number"), item.get("description"))
        results.append(result or {})
    
    return jsonify({"results": results})


@app.route("/api/status")
@auth_required
def api_status():
    return jsonify({
        "poll": POLL_STATUS,
        "scprs_db": get_price_db_stats(),
        "rfqs": len(load_rfqs()),
    })


# ═══════════════════════════════════════════════════════════════════════
# Startup
# ═══════════════════════════════════════════════════════════════════════

_poll_started = False

def start_polling():
    global _poll_started
    if _poll_started:
        return
    _poll_started = True
    email_cfg = CONFIG.get("email", {})
    if email_cfg.get("email_password"):
        poll_thread = threading.Thread(target=email_poll_loop, daemon=True)
        poll_thread.start()
        log.info("Email polling started")
    else:
        POLL_STATUS["error"] = "Set GMAIL_PASSWORD env var or email_password in config"
        log.info("Email polling disabled — no password configured")

# Start polling on import (for gunicorn) and on direct run
start_polling()

if __name__ == "__main__":
    email_cfg = CONFIG.get("email", {})
    port = int(os.environ.get("PORT", 5000))
    print(f"""
╔══════════════════════════════════════════════════╗
║          Reytech RFQ Dashboard v2               ║
║                                                  ║
║  🌐 http://localhost:{port}                        ║
║  📧 Email polling: {'ON' if email_cfg.get('email_password') else 'OFF (set password)':30s}║
║  🔒 Login: {DASH_USER} / {'*'*len(DASH_PASS):20s}     ║
║  📊 SCPRS DB: {get_price_db_stats()['total_items']} items{' ':27s}║
╚══════════════════════════════════════════════════╝
    """)
    app.run(host="0.0.0.0", port=port, debug=False)
