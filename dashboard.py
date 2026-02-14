#!/usr/bin/env python3
"""
Reytech RFQ Dashboard v2
Full automation: email polling → parse → SCPRS lookup → price → generate → draft email
Production-ready: password protected, env var config, gunicorn-compatible
"""

import os, json, uuid, sys, threading, time, logging, functools, re, shutil
from datetime import datetime, timezone, timedelta
from flask import (Flask, request, redirect, url_for, render_template_string,
                   send_file, jsonify, flash, Response)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rfq_parser import parse_rfq_attachments, identify_attachments
from reytech_filler_v4 import (load_config, get_pst_date, fill_703b, fill_704b, fill_bid_package)
from scprs_lookup import bulk_lookup, save_prices_from_rfq, get_price_db_stats
from email_poller import EmailPoller, EmailSender

# v6.0: Pricing intelligence (graceful fallback if files not present)
try:
    from pricing_oracle import recommend_prices_for_rfq, pricing_health_check
    from won_quotes_db import (ingest_scprs_result, find_similar_items,
                                get_kb_stats, get_price_history)
    PRICING_ORACLE_AVAILABLE = True
except ImportError:
    PRICING_ORACLE_AVAILABLE = False

# v6.1: Product Research Agent (graceful fallback)
try:
    from product_research import (research_product, research_rfq_items,
                                   quick_lookup, test_amazon_search,
                                   get_research_cache_stats, RESEARCH_STATUS)
    PRODUCT_RESEARCH_AVAILABLE = True
except ImportError:
    PRODUCT_RESEARCH_AVAILABLE = False

# v6.2: Price Check Processor (graceful fallback)
try:
    from price_check import (parse_ams704, process_price_check, lookup_prices,
                              test_parse, REYTECH_INFO)
    PRICE_CHECK_AVAILABLE = True
except ImportError:
    PRICE_CHECK_AVAILABLE = False

# v7.0: Auto-Processor Engine (graceful fallback)
try:
    from auto_processor import (auto_process_price_check, detect_document_type,
                                 score_quote_confidence, system_health_check,
                                 get_audit_stats, track_response_time)
    AUTO_PROCESSOR_AVAILABLE = True
except ImportError:
    AUTO_PROCESSOR_AVAILABLE = False

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

_shared_poller = None  # Shared poller instance for manual checks

def process_rfq_email(rfq_email):
    """Process a single RFQ email into the queue. Returns rfq_data or None.
    Deduplicates by checking email_uid against existing RFQs."""
    
    # Dedup: check if this email UID is already in the queue
    rfqs = load_rfqs()
    for existing in rfqs.values():
        if existing.get("email_uid") == rfq_email.get("email_uid"):
            log.info(f"Skipping duplicate email UID {rfq_email.get('email_uid')}: already in queue")
            return None
    
    templates = {}
    for att in rfq_email["attachments"]:
        if att["type"] != "unknown":
            templates[att["type"]] = att["path"]
    
    if "704b" not in templates:
        # Still save it as a raw entry so user can see it arrived
        rfq_data = {
            "id": rfq_email["id"],
            "solicitation_number": rfq_email.get("solicitation_hint", "unknown"),
            "status": "new",
            "source": "email",
            "email_uid": rfq_email.get("email_uid"),
            "email_subject": rfq_email["subject"],
            "email_sender": rfq_email["sender_email"],
            "requestor_name": rfq_email["sender_email"],
            "requestor_email": rfq_email["sender_email"],
            "due_date": "TBD",
            "line_items": [],
            "attachments_raw": [a["filename"] for a in rfq_email["attachments"]],
            "templates": templates,
            "parse_note": "704B not identified — manual review needed",
        }
    else:
        rfq_data = parse_rfq_attachments(templates)
        rfq_data["id"] = rfq_email["id"]
        rfq_data["status"] = "new"
        rfq_data["source"] = "email"
        rfq_data["email_uid"] = rfq_email.get("email_uid")
        rfq_data["email_subject"] = rfq_email["subject"]
        rfq_data["email_sender"] = rfq_email["sender_email"]
        # Auto SCPRS lookup
        rfq_data["line_items"] = bulk_lookup(rfq_data.get("line_items", []))
    
    rfqs[rfq_data["id"]] = rfq_data
    save_rfqs(rfqs)
    POLL_STATUS["emails_found"] += 1
    log.info(f"Auto-imported RFQ #{rfq_data.get('solicitation_number', 'unknown')}")
    return rfq_data


def do_poll_check():
    """Run a single email poll check. Used by both background thread and manual trigger."""
    global _shared_poller
    email_cfg = CONFIG.get("email", {})
    
    if not email_cfg.get("email_password"):
        POLL_STATUS["error"] = "Email password not configured"
        log.error("Poll check failed: no email_password in config")
        return []
    
    if _shared_poller is None:
        # Force absolute path for processed file
        email_cfg = dict(email_cfg)  # copy so we don't mutate
        email_cfg["processed_file"] = os.path.join(DATA_DIR, "processed_emails.json")
        _shared_poller = EmailPoller(email_cfg)
        log.info(f"Created poller for {email_cfg.get('email', 'NO EMAIL SET')}, processed file: {email_cfg['processed_file']}")
    
    imported = []
    try:
        connected = _shared_poller.connect()
        if connected:
            log.info("IMAP connected, checking for RFQs...")
            rfq_emails = _shared_poller.check_for_rfqs(save_dir=UPLOAD_DIR)
            POLL_STATUS["last_check"] = get_pst_date()
            POLL_STATUS["error"] = None
            log.info(f"Poll check complete: {len(rfq_emails)} RFQ emails found")
            
            for rfq_email in rfq_emails:
                rfq_data = process_rfq_email(rfq_email)
                if rfq_data:
                    imported.append(rfq_data)
        else:
            POLL_STATUS["error"] = f"IMAP connect failed for {email_cfg.get('email', '?')}"
            log.error(POLL_STATUS["error"])
    except Exception as e:
        POLL_STATUS["error"] = str(e)
        log.error(f"Poll error: {e}", exc_info=True)
        # Reset poller on error so next call creates a fresh one
        _shared_poller = None
    
    return imported


def email_poll_loop():
    """Background thread: check email every N seconds."""
    email_cfg = CONFIG.get("email", {})
    if not email_cfg.get("email_password"):
        POLL_STATUS["error"] = "Email password not configured"
        POLL_STATUS["running"] = False
        log.warning("Email polling disabled — no password in config")
        return
    
    interval = email_cfg.get("poll_interval_seconds", 120)
    POLL_STATUS["running"] = True
    
    while POLL_STATUS["running"]:
        do_poll_check()
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
 <div class="card-t">New RFQ / Price Check</div>
 <form method="POST" action="/upload" enctype="multipart/form-data" id="uf">
  <div class="upl" id="dz" onclick="document.getElementById('fi').click()">
   <h3>Drop PDF attachments here</h3>
   <p>Upload RFQ (703B, 704B, Bid Package) or AMS 704 Price Check</p>
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
  <button class="btn btn-sm btn-o" onclick="researchPrices()" style="margin-left:4px">🔬 Amazon Lookup</button>
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
    {% if i.scprs_source %}<span class="scprs-tag scprs-{{'hi' if i.scprs_confidence=='high' else 'med'}}" title="{{i.scprs_vendor|default('')}} {{i.scprs_date|default('')}} PO:{{i.scprs_po|default('')}}">{{i.scprs_source|replace('_',' ')}}</span>{% endif %}
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
 <button class="btn btn-sm btn-s" onclick="pollNow(this)" style="padding:4px 10px;font-size:11px;cursor:pointer" id="poll-btn">Check Now</button>
 <button class="btn btn-sm btn-s" onclick="resyncAll(this)" style="padding:4px 10px;font-size:11px;cursor:pointer;border-color:var(--or);color:var(--or)" title="Clear queue & re-import all emails">🔄 Resync</button>
 <div><span class="poll-dot {'poll-on' if POLL_STATUS['running'] else 'poll-off' if not CONFIG.get('email',{{}}).get('email_password') else 'poll-wait'}"></span>
 {'Polling' if POLL_STATUS['running'] else 'Email not configured' if not CONFIG.get('email',{{}}).get('email_password') else 'Starting...'}
 {' · Last: ' + POLL_STATUS['last_check'] if POLL_STATUS.get('last_check') else ''}</div>
 <div>{get_pst_date()}</div>
</div></div>
<div class="ctr">
{{% with messages = get_flashed_messages(with_categories=true) %}}
 {{% for cat, msg in messages %}}<div class="alert al-{{'s' if cat=='success' else 'e' if cat=='error' else 'i'}}">{{% if cat=='success' %}}✅{{% elif cat=='error' %}}❌{{% else %}}ℹ️{{% endif %}} {{{{msg}}}}</div>{{% endfor %}}
{{% endwith %}}
""" + content + """
<script>
function pollNow(btn){
 btn.disabled=true;btn.textContent='Checking...';
 fetch('/api/poll-now').then(r=>r.json()).then(d=>{
  if(d.found>0){btn.textContent=d.found+' found!';setTimeout(()=>location.reload(),800)}
  else{btn.textContent='No new emails';setTimeout(()=>{btn.textContent='Check Now';btn.disabled=false},2000)}
 }).catch(()=>{btn.textContent='Error';setTimeout(()=>{btn.textContent='Check Now';btn.disabled=false},2000)});
}
function resyncAll(btn){
 if(!confirm('Clear all RFQs and re-import from email?'))return;
 btn.disabled=true;btn.textContent='🔄 Syncing...';
 fetch('/api/resync').then(r=>r.json()).then(d=>{
  if(d.found>0){btn.textContent=d.found+' imported!';setTimeout(()=>location.reload(),800)}
  else{btn.textContent='0 found';setTimeout(()=>{btn.textContent='🔄 Resync';btn.disabled=false},2000)}
 }).catch(()=>{btn.textContent='Error';setTimeout(()=>{btn.textContent='🔄 Resync';btn.disabled=false},2000)});
}
</script>
</div></body></html>"""
    return render_template_string(html, **kw)


@app.route("/")
@auth_required
def home():
    return render(PAGE_HOME, rfqs=load_rfqs(), price_checks=_load_price_checks())

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
    
    # Check if this is a Price Check (AMS 704) instead of an RFQ
    if PRICE_CHECK_AVAILABLE and len(saved) == 1:
        if _is_price_check(saved[0]):
            return _handle_price_check_upload(saved[0], rfq_id)

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


def _is_price_check(pdf_path):
    """Detect if a PDF is an AMS 704 Price Check."""
    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        # Check first page text
        text = reader.pages[0].extract_text() or ""
        text_lower = text.lower()
        if "price check" in text_lower and ("ams 704" in text_lower or "worksheet" in text_lower):
            return True
        # Check form fields for AMS 704 patterns
        fields = reader.get_fields()
        if fields:
            field_names = set(fields.keys())
            ams704_markers = {"COMPANY NAME", "Requestor", "PRICE PER UNITRow1", "EXTENSIONRow1"}
            if len(ams704_markers & field_names) >= 3:
                return True
    except Exception:
        pass
    return False


def _handle_price_check_upload(pdf_path, pc_id):
    """Process an uploaded Price Check PDF."""
    # Save to data dir for persistence
    pc_file = os.path.join(DATA_DIR, f"pc_upload_{os.path.basename(pdf_path)}")
    shutil.copy2(pdf_path, pc_file)

    # Parse
    parsed = parse_ams704(pc_file)
    if parsed.get("error"):
        flash(f"Price Check parse error: {parsed['error']}", "error")
        return redirect("/")

    items = parsed.get("line_items", [])
    pc_num = parsed.get("header", {}).get("price_check_number", "unknown")
    institution = parsed.get("header", {}).get("institution", "")
    due_date = parsed.get("header", {}).get("due_date", "")

    # Save PC record
    pcs = _load_price_checks()
    pcs[pc_id] = {
        "id": pc_id,
        "pc_number": pc_num,
        "institution": institution,
        "due_date": due_date,
        "requestor": parsed.get("header", {}).get("requestor", ""),
        "ship_to": parsed.get("ship_to", ""),
        "items": items,
        "source_pdf": pc_file,
        "status": "parsed",
        "created_at": datetime.now().isoformat(),
        "parsed": parsed,
    }
    _save_price_checks(pcs)

    flash(f"Price Check #{pc_num} parsed — {len(items)} items from {institution}. Due {due_date}", "success")
    return redirect(f"/pricecheck/{pc_id}")


def _load_price_checks():
    path = os.path.join(DATA_DIR, "price_checks.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except:
            return {}
    return {}


def _save_price_checks(pcs):
    path = os.path.join(DATA_DIR, "price_checks.json")
    with open(path, "w") as f:
        json.dump(pcs, f, indent=2, default=str)


@app.route("/rfq/<rid>")
@auth_required
def detail(rid):
    # Check if this is actually a price check
    pcs = _load_price_checks()
    if rid in pcs:
        return redirect(f"/pricecheck/{rid}")
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
        t = r.get("templates", {})
        output_files = []
        
        if "703b" in t and os.path.exists(t["703b"]):
            fill_703b(t["703b"], r, CONFIG, f"{out}/{sol}_703B_Reytech.pdf")
            output_files.append(f"{sol}_703B_Reytech.pdf")
        
        if "704b" in t and os.path.exists(t["704b"]):
            fill_704b(t["704b"], r, CONFIG, f"{out}/{sol}_704B_Reytech.pdf")
            output_files.append(f"{sol}_704B_Reytech.pdf")
        
        if "bidpkg" in t and os.path.exists(t["bidpkg"]):
            fill_bid_package(t["bidpkg"], r, CONFIG, f"{out}/{sol}_BidPackage_Reytech.pdf")
            output_files.append(f"{sol}_BidPackage_Reytech.pdf")
        
        if not output_files:
            flash("No template PDFs found — upload the original RFQ PDFs first", "error")
            return redirect(f"/rfq/{rid}")
        
        r["status"] = "generated"
        r["output_files"] = output_files
        r["generated_at"] = datetime.now().isoformat()
        
        # Note which forms are missing
        missing = []
        if "703b" not in t: missing.append("703B")
        if "704b" not in t: missing.append("704B")
        if "bidpkg" not in t: missing.append("Bid Package")
        
        # Create draft email
        sender = EmailSender(CONFIG.get("email", {}))
        output_paths = [f"{out}/{f}" for f in r["output_files"]]
        r["draft_email"] = sender.create_draft_email(r, output_paths)
        
        # Save SCPRS prices
        save_prices_from_rfq(r)
        
        save_rfqs(rfqs)
        msg = f"Generated {len(output_files)} form(s) for #{sol}"
        if missing:
            msg += f" — missing: {', '.join(missing)}"
        else:
            msg += " — draft email ready"
        flash(msg, "success" if not missing else "info")
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


@app.route("/rfq/<rid>/delete", methods=["POST"])
@auth_required
def delete_rfq(rid):
    """Delete an RFQ from the queue and remove its UID from processed list."""
    rfqs = load_rfqs()
    if rid in rfqs:
        sol = rfqs[rid].get("solicitation_number", "?")
        # Remove this email's UID from processed list so it can be re-imported
        email_uid = rfqs[rid].get("email_uid")
        if email_uid:
            _remove_processed_uid(email_uid)
        del rfqs[rid]
        save_rfqs(rfqs)
        flash(f"Deleted RFQ #{sol}", "success")
    return redirect("/")


# ═══════════════════════════════════════════════════════════════════════
# Price Check Pages (v6.2)
# ═══════════════════════════════════════════════════════════════════════

@app.route("/pricecheck/<pcid>")
@auth_required
def pricecheck_detail(pcid):
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        flash("Price Check not found", "error"); return redirect("/")

    items = pc.get("items", [])
    header = pc.get("parsed", {}).get("header", {})

    items_html = ""
    for idx, item in enumerate(items):
        p = item.get("pricing", {})
        # Cost sources
        amazon_cost = p.get("amazon_price")
        scprs_cost = p.get("scprs_price")
        # Best available cost
        unit_cost = p.get("unit_cost") or amazon_cost or scprs_cost or 0
        # Markup and final price
        markup_pct = p.get("markup_pct", 25)
        final_price = p.get("recommended_price") or (round(unit_cost * (1 + markup_pct/100), 2) if unit_cost else 0)

        amazon_str = f"${amazon_cost:.2f}" if amazon_cost else "—"
        amazon_data = f'data-amazon="{amazon_cost:.2f}"' if amazon_cost else 'data-amazon="0"'
        scprs_str = f"${scprs_cost:.2f}" if scprs_cost else "—"
        cost_str = f"{unit_cost:.2f}" if unit_cost else ""
        final_str = f"{final_price:.2f}" if final_price else ""
        qty = item.get("qty", 1)
        ext = f"${final_price * qty:.2f}" if final_price else "—"

        # Amazon match link
        title = (p.get("amazon_title") or "")[:40]
        url = p.get("amazon_url", "")
        link = f'<a href="{url}" target="_blank" title="{p.get("amazon_title","")}">{title}</a>' if url and title else "—"

        # SCPRS confidence indicator
        scprs_conf = p.get("scprs_confidence", 0)
        scprs_badge = ""
        if scprs_cost:
            color = "#3fb950" if scprs_conf > 0.7 else ("#d29922" if scprs_conf > 0.4 else "#8b949e")
            scprs_badge = f' <span style="color:{color};font-size:10px" title="Confidence: {scprs_conf:.0%}">●</span>'

        # Confidence grade if scored
        conf = item.get("confidence", {})
        grade = conf.get("grade", "")
        grade_color = {"A": "#3fb950", "B": "#58a6ff", "C": "#d29922", "F": "#f85149"}.get(grade, "#8b949e")
        grade_html = f'<span style="color:{grade_color};font-weight:bold">{grade}</span>' if grade else "—"

        # Per-item profit
        item_profit = round((final_price - unit_cost) * qty, 2) if (final_price and unit_cost) else 0
        profit_color = "#3fb950" if item_profit > 0 else ("#f85149" if item_profit < 0 else "#8b949e")
        profit_str = f'<span style="color:{profit_color}">${item_profit:.2f}</span>' if (final_price and unit_cost) else "—"
        
        # No-bid state
        no_bid = item.get("no_bid", False)
        bid_checked = "" if no_bid else "checked"
        row_opacity = "opacity:0.4" if no_bid else ""

        items_html += f"""<tr style="{row_opacity}" data-row="{idx}">
         <td style="text-align:center"><input type="checkbox" name="bid_{idx}" {bid_checked} onchange="toggleBid({idx},this)"></td>
         <td><input type="number" name="itemnum_{idx}" value="{item.get('item_number','')}" class="num-in sm" style="width:35px"></td>
         <td><input type="number" name="qty_{idx}" value="{qty}" class="num-in sm" style="width:50px" onchange="recalcPC()"></td>
         <td><input type="text" name="uom_{idx}" value="{item.get('uom','EA').upper()}" style="width:40px;background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:4px;border-radius:4px;font-size:13px;text-transform:uppercase"></td>
         <td><input type="text" name="desc_{idx}" value="{item.get('description','').replace('"','&quot;')}" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:4px;border-radius:4px;font-size:13px"></td>
         <td>{scprs_str}{scprs_badge}</td>
         <td {amazon_data}>{amazon_str}</td>
         <td style="font-size:12px">{link}</td>
         <td><input type="number" step="0.01" min="0" name="cost_{idx}" value="{cost_str}" class="num-in" onchange="recalcRow({idx})"></td>
         <td><input type="number" step="1" min="0" max="200" name="markup_{idx}" value="{markup_pct}" class="num-in sm" onchange="recalcRow({idx})">%</td>
         <td><input type="number" step="0.01" min="0" name="price_{idx}" value="{final_str}" class="num-in" onchange="recalcPC()"></td>
         <td class="ext">{ext}</td>
         <td class="profit">{profit_str}</td>
         <td style="text-align:center">{grade_html}</td>
        </tr>"""

    download_html = ""
    if pc.get("output_pdf") and os.path.exists(pc.get("output_pdf", "")):
        fname = os.path.basename(pc["output_pdf"])
        download_html = f'<a href="/api/pricecheck/download/{fname}" class="btn btn-sm btn-g">📥 Download Completed PDF</a>'

    # 45-day expiry from processing date
    try:
        processed = pc.get("uploaded_at") or pc.get("created_at") or datetime.now().isoformat()
        if isinstance(processed, str):
            base = datetime.fromisoformat(processed.replace("Z", "+00:00"))
        else:
            base = processed
        expiry = base + timedelta(days=45)
        expiry_date = expiry.strftime("%m/%d/%Y")
    except:
        expiry_date = (datetime.now() + timedelta(days=45)).strftime("%m/%d/%Y")

    # Delivery dropdown state
    saved_delivery = pc.get("delivery_option", "5-7 business days")
    preset_options = ("3-5 business days", "5-7 business days", "7-14 business days")
    is_custom = saved_delivery not in preset_options and saved_delivery != ""
    del_sel = {opt: ("selected" if saved_delivery == opt else "") for opt in preset_options}
    del_sel["custom"] = "selected" if is_custom else ""
    # Default to 5-7 if nothing saved
    if not any(del_sel.values()):
        del_sel["5-7 business days"] = "selected"
    custom_val = saved_delivery if is_custom else ""
    custom_display = "inline-block" if is_custom else "none"

    # Pricing mode state
    saved_buffer = pc.get("price_buffer", 0)
    pricing_mode_sel = {0: "", 10: "", 15: "", 20: ""}
    if saved_buffer in pricing_mode_sel:
        pricing_mode_sel[saved_buffer] = "selected"
    else:
        pricing_mode_sel[0] = "selected"

    html = f"""<!doctype html><html><head><title>PC #{pc.get('pc_number','')}</title>
    <style>
     body{{font-family:system-ui;background:#0d1117;color:#c9d1d9;margin:0;padding:20px;font-size:14px}}
     .card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:10px 0}}
     table{{width:100%;border-collapse:collapse}} th,td{{padding:6px 8px;border:1px solid #30363d;text-align:left;font-size:13px}}
     th{{background:#21262d;font-size:11px;text-transform:uppercase;color:#8b949e;white-space:nowrap}}
     .btn{{padding:8px 16px;border-radius:6px;border:none;cursor:pointer;font-weight:600;text-decoration:none;display:inline-block;margin:4px;font-size:14px}}
     .btn-p{{background:#1f6feb;color:#fff}} .btn-g{{background:#238636;color:#fff}} .btn-o{{background:#da3633;color:#fff}}
     .btn-y{{background:#9e6a03;color:#fff}} .btn-v{{background:#8957e5;color:#fff}} .btn-sm{{padding:5px 12px;font-size:13px}}
     a{{color:#58a6ff}} h1{{margin:0;font-size:24px}} .meta{{color:#8b949e;font-size:14px}}
     .status{{padding:3px 8px;border-radius:4px;font-size:13px;font-weight:600;vertical-align:middle}}
     .status-parsed{{background:#1f6feb33;color:#58a6ff}} .status-priced{{background:#23863633;color:#3fb950}}
     .status-completed{{background:#23863633;color:#3fb950}} .status-converted{{background:#8957e533;color:#bc8cff}}
     .totals{{text-align:right;font-size:15px;margin-top:12px}} .totals b{{color:#3fb950}}
     .num-in{{width:80px;background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:4px 5px;border-radius:4px;text-align:right;font-size:13px}}
     .num-in.sm{{width:50px}}
     input[type=number]::-webkit-inner-spin-button{{opacity:1}}
     .msg{{padding:10px 14px;border-radius:6px;margin:8px 0;font-size:14px}}
     .msg-ok{{background:#23863622;color:#3fb950;border:1px solid #23863655}}
     .msg-warn{{background:#9e6a0322;color:#d29922;border:1px solid #9e6a0355}}
     .msg-err{{background:#da363322;color:#f85149;border:1px solid #da363355}}
    </style></head><body>
    <a href="/" style="color:#58a6ff">← Dashboard</a>
    <div class="card">
     <h1>Price Check #{pc.get('pc_number','unknown')}
      <span class="status status-{pc.get('status','parsed')}">{pc.get('status','parsed').upper()}</span></h1>
     <div class="meta" style="margin-top:8px">
      <b>Institution:</b> {header.get('institution',pc.get('institution',''))} &nbsp;|&nbsp;
      <b>Requestor:</b> {header.get('requestor',pc.get('requestor',''))} &nbsp;|&nbsp;
      <b>Due:</b> {pc.get('due_date','')} <span id="dueUrgency"></span> &nbsp;|&nbsp;
      <b>Ship to:</b> {pc.get('ship_to','')}
     </div>
     <div style="margin-top:12px">
      <button class="btn btn-p" onclick="runScprs(this)">🔍 SCPRS Lookup</button>
      <button class="btn btn-y" onclick="runLookup(this)">🔬 Amazon Lookup</button>
      <button class="btn btn-g" onclick="saveAndGenerate(this)">📄 Generate Completed 704</button>
      {download_html}
      <span style="margin-left:16px;border-left:1px solid #30363d;padding-left:16px">
       <button class="btn btn-v" onclick="convertToQuote(this)">🔄 Convert to Full Quote (704A/B + Package)</button>
      </span>
     </div>
     <div style="margin-top:8px">
      <button class="btn" style="background:#f0883e;color:#fff" onclick="autoProcess(this)">⚡ Auto-Process (SCPRS + Amazon + Price + Generate — one click)</button>
     </div>
     <div id="statusMsg"></div>
     <div id="confidenceBar"></div>
    </div>

    <div class="card" style="padding:14px 20px">
     <div style="display:flex;gap:24px;align-items:flex-start;flex-wrap:wrap">
      <div>
       <label style="font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:0.5px">Delivery Time</label><br>
       <select id="deliverySelect" style="background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:6px 10px;border-radius:4px;font-size:14px;min-width:200px">
        <option value="3-5 business days" {del_sel["3-5 business days"]}>3-5 business days</option>
        <option value="5-7 business days" {del_sel["5-7 business days"]}>5-7 business days</option>
        <option value="7-14 business days" {del_sel["7-14 business days"]}>7-14 business days</option>
        <option value="custom" {del_sel["custom"]}>Custom...</option>
       </select>
       <input type="text" id="deliveryCustom" placeholder="e.g. 2-3 weeks" value="{custom_val}" style="display:{custom_display};background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:6px 10px;border-radius:4px;font-size:14px;width:160px;margin-left:6px">
      </div>
      <div>
       <label style="font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:0.5px">Price Protection
        <span style="font-size:11px;text-transform:none" title="Amazon prices fluctuate. Buffer pads your cost basis so a temporary discount doesn't eat your margin when prices revert.">ⓘ</span>
       </label><br>
       <div style="display:flex;align-items:center;gap:8px">
        <select id="pricingMode" onchange="applyPricingMode()" style="background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:6px 10px;border-radius:4px;font-size:14px">
         <option value="0" {pricing_mode_sel.get(0,"")}>Current Price (no buffer)</option>
         <option value="10" {pricing_mode_sel.get(10,"")}>+10% buffer (light)</option>
         <option value="15" {pricing_mode_sel.get(15,"")}>+15% buffer (standard)</option>
         <option value="20" {pricing_mode_sel.get(20,"")}>+20% buffer (safe)</option>
        </select>
        <span style="font-size:12px;color:#8b949e">+</span>
        <input type="number" id="markupDefault" value="{pc.get('default_markup', 25)}" min="0" max="200" style="width:55px;background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px;font-size:14px;text-align:right" onchange="applyPricingMode()">
        <span style="font-size:12px;color:#8b949e">% markup</span>
       </div>
       <div id="pricingExplainer" style="font-size:12px;color:#8b949e;margin-top:4px"></div>
      </div>
      <div style="flex:1;min-width:250px">
       <label style="font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:0.5px">Supplier Notes <span style="font-size:11px;text-transform:none">(prints on 704)</span></label><br>
       <input type="text" id="supplierNotes" value="{pc.get('custom_notes','').replace('"','&quot;')}" placeholder="Optional — leave blank for no notes" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:6px 10px;border-radius:4px;font-size:14px">
      </div>
     </div>
    </div>

    <div class="card">
     <h3 style="margin-top:0">Line Items <span id="itemCount" style="font-weight:normal;color:#8b949e;font-size:14px">({len(items)} items)</span></h3>
     <table id="itemsTable">
      <tr><th style="width:28px">Bid</th><th>#</th><th>Qty</th><th>UOM</th><th>Description</th><th>SCPRS $</th><th>Amazon $</th><th>Amazon Match</th><th>Unit Cost</th><th>Markup</th><th>Our Price</th><th>Extension</th><th>Profit</th><th>Conf</th></tr>
      {items_html}
     </table>
     <div style="margin-top:8px">
      <button class="btn btn-sm" style="background:#21262d;color:#8b949e;border:1px solid #30363d" onclick="addRow()">+ Add Item</button>
     </div>
     <div class="totals" id="totals"></div>
     <div style="margin-top:10px;display:flex;align-items:center;gap:16px;font-size:13px">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer">
       <input type="checkbox" id="taxToggle" onchange="recalcPC()">
       <span>Include CA Sales Tax</span>
       <span id="taxRateDisplay" style="color:#8b949e;font-size:11px">(fetching rate...)</span>
      </label>
      <span style="color:#8b949e">|</span>
      <span style="color:#8b949e;font-size:12px">📅 Pricing valid through: <b id="expiryDate" style="color:#d29922">{expiry_date}</b> (45 days)</span>
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

    function applyPricingMode() {{
     const buffer=parseInt(document.getElementById('pricingMode').value)||0;
     const markup=parseInt(document.getElementById('markupDefault').value)||25;
     // Apply to all rows that have Amazon data
     document.querySelectorAll('tr[data-row]').forEach((row,i)=>{{
      const amazonCell=row.querySelector('td[data-amazon]');
      if(!amazonCell) return;
      const amazonPrice=parseFloat(amazonCell.getAttribute('data-amazon'))||0;
      if(amazonPrice<=0) return;
      // Protected cost = amazon × (1 + buffer%)
      const protectedCost=Math.round(amazonPrice*(1+buffer/100)*100)/100;
      const costInp=row.querySelector('[name=cost_'+i+']');
      const markupInp=row.querySelector('[name=markup_'+i+']');
      if(costInp) costInp.value=protectedCost.toFixed(2);
      if(markupInp) markupInp.value=markup;
      recalcRow(i);
     }});
     // Update explainer
     const ex=document.getElementById('pricingExplainer');
     if(buffer>0) {{
      ex.innerHTML='<span style="color:#d29922">🛡️ Cost padded +'+buffer+'% above Amazon price to absorb price swings</span>';
     }} else {{
      ex.innerHTML='<span style="color:#8b949e">Using raw Amazon price as cost — watch for temporary discounts</span>';
     }}
    }}
    // Show initial explainer
    (function(){{ const b=parseInt(document.getElementById('pricingMode').value)||0;
     const ex=document.getElementById('pricingExplainer');
     if(b>0) ex.innerHTML='<span style="color:#d29922">🛡️ Cost padded +'+b+'% above Amazon price</span>';
     else ex.innerHTML='<span style="color:#8b949e">Using raw Amazon price as cost</span>';
    }})();

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
     tr.innerHTML='<td style="text-align:center"><input type="checkbox" name="bid_'+idx+'" checked onchange="toggleBid('+idx+',this)"></td>'
      +'<td><input type="number" name="itemnum_'+idx+'" value="'+(idx+1)+'" class="num-in sm" style="width:35px"></td>'
      +'<td><input type="number" name="qty_'+idx+'" value="1" class="num-in sm" style="width:50px" onchange="recalcPC()"></td>'
      +'<td><input type="text" name="uom_'+idx+'" value="EA" style="width:40px;background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:4px;border-radius:4px;font-size:13px;text-transform:uppercase"></td>'
      +'<td><input type="text" name="desc_'+idx+'" value="" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:4px;border-radius:4px;font-size:13px" placeholder="Enter description"></td>'
      +'<td>—</td><td>—</td><td>—</td>'
      +'<td><input type="number" step="0.01" min="0" name="cost_'+idx+'" value="" class="num-in" onchange="recalcRow('+idx+')"></td>'
      +'<td><input type="number" step="1" min="0" max="200" name="markup_'+idx+'" value="25" class="num-in sm" onchange="recalcRow('+idx+')">%</td>'
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
     html+='<div style="text-align:right;min-width:220px;background:#21262d;padding:10px 14px;border-radius:6px;border:1px solid #30363d">';
     html+='<div style="font-size:11px;text-transform:uppercase;color:#8b949e;margin-bottom:4px;letter-spacing:0.5px">Profit Summary</div>';
     html+='<div><span style="color:#8b949e">Total Cost:</span> <b>$'+totalCost.toFixed(2)+'</b></div>';
     html+='<div><span style="color:#8b949e">Total Revenue:</span> <b>$'+sub.toFixed(2)+'</b></div>';
     let profitColor=totalProfit>0?'#3fb950':(totalProfit<0?'#f85149':'#8b949e');
     html+='<div style="font-size:15px;margin-top:2px"><span style="color:#8b949e">Profit:</span> <b style="color:'+profitColor+'">$'+totalProfit.toFixed(2)+'</b>';
     html+=' <span style="color:'+marginColor+';font-size:13px">('+margin.toFixed(1)+'% margin)</span></div>';
     html+='</div>';

     html+='</div>';
     document.getElementById('totals').innerHTML=html;
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
     data['price_buffer']=parseInt(document.getElementById('pricingMode').value)||0;
     data['default_markup']=parseInt(document.getElementById('markupDefault').value)||25;
     // Delivery option
     let delSel=document.getElementById('deliverySelect');
     data['delivery_option']=delSel.value==='custom'?document.getElementById('deliveryCustom').value:delSel.value;
     // Supplier notes
     data['custom_notes']=document.getElementById('supplierNotes').value;
     return data;
    }}

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
      if(d&&d.ok){{showMsg('✅ PDF generated! Prices saved to Won Quotes KB for future reference. Reloading...','ok');setTimeout(()=>location.reload(),1200)}}
      else{{showMsg('❌ PDF generation failed: '+(d?.error||'unknown'),'err')}}
     }}).catch(e=>{{btn.textContent='📄 Generate Completed 704';btn.disabled=false;showMsg('❌ Error: '+e,'err')}});
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
    </script>
    </body></html>"""
    return html


@app.route("/pricecheck/<pcid>/lookup")
@auth_required
def pricecheck_lookup(pcid):
    """Run Amazon lookup for all items in a Price Check."""
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"ok": False, "error": "price_check.py not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    parsed = pc.get("parsed", {})
    parsed = lookup_prices(parsed)
    pc["parsed"] = parsed
    pc["items"] = parsed.get("line_items", [])
    pc["status"] = "priced"
    _save_price_checks(pcs)

    found = sum(1 for i in pc["items"] if i.get("pricing", {}).get("amazon_price"))
    return jsonify({"ok": True, "found": found, "total": len(pc["items"])})


@app.route("/pricecheck/<pcid>/scprs-lookup")
@auth_required
def pricecheck_scprs_lookup(pcid):
    """Run SCPRS Won Quotes lookup for all items."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    items = pc.get("items", [])
    found = 0
    if PRICING_ORACLE_AVAILABLE:
        for item in items:
            try:
                matches = find_similar_items(
                    item_number=item.get("item_number", ""),
                    description=item.get("description", ""),
                )
                if matches:
                    best = matches[0]
                    quote = best.get("quote", best)
                    if not item.get("pricing"):
                        item["pricing"] = {}
                    item["pricing"]["scprs_price"] = quote.get("unit_price")
                    item["pricing"]["scprs_match"] = quote.get("description", "")[:60]
                    item["pricing"]["scprs_confidence"] = best.get("match_confidence", 0)
                    found += 1
            except Exception as e:
                log.error(f"SCPRS lookup error: {e}")

    pc["items"] = items
    pc["parsed"]["line_items"] = items
    _save_price_checks(pcs)
    return jsonify({"ok": True, "found": found, "total": len(items)})


@app.route("/pricecheck/<pcid>/save-prices", methods=["POST"])
@auth_required
def pricecheck_save_prices(pcid):
    """Save manually edited prices, costs, and markups from the UI."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    data = request.json or {}
    items = pc.get("items", [])
    
    # Save tax state
    pc["tax_enabled"] = data.get("tax_enabled", False)
    pc["tax_rate"] = data.get("tax_rate", 0)
    pc["delivery_option"] = data.get("delivery_option", "5-7 business days")
    pc["custom_notes"] = data.get("custom_notes", "")
    pc["price_buffer"] = data.get("price_buffer", 0)
    pc["default_markup"] = data.get("default_markup", 25)
    
    for key, val in data.items():
        try:
            if key in ("tax_enabled", "tax_rate"):
                continue
            parts = key.split("_", 1)
            if len(parts) != 2:
                continue
            field_type = parts[0]
            idx = int(parts[1])
            # Expand items list if new rows were added via UI
            while idx >= len(items):
                items.append({"item_number": "", "qty": 1, "uom": "ea",
                              "description": "", "pricing": {}})
            if 0 <= idx < len(items):
                if field_type in ("price", "cost", "markup"):
                    if not items[idx].get("pricing"):
                        items[idx]["pricing"] = {}
                    if field_type == "price":
                        items[idx]["pricing"]["recommended_price"] = float(val) if val else None
                    elif field_type == "cost":
                        items[idx]["pricing"]["unit_cost"] = float(val) if val else None
                    elif field_type == "markup":
                        items[idx]["pricing"]["markup_pct"] = float(val) if val else 25
                elif field_type == "qty":
                    items[idx]["qty"] = int(val) if val else 1
                elif field_type == "desc":
                    items[idx]["description"] = str(val) if val else ""
                elif field_type == "uom":
                    items[idx]["uom"] = str(val).upper() if val else "EA"
                elif field_type == "itemno":
                    items[idx]["item_number"] = str(val) if val else ""
                elif field_type == "bid":
                    items[idx]["no_bid"] = not bool(val)
        except (ValueError, IndexError):
            pass

    pc["items"] = items
    pc["parsed"]["line_items"] = items
    _save_price_checks(pcs)
    return jsonify({"ok": True})


@app.route("/pricecheck/<pcid>/generate")
@auth_required
def pricecheck_generate(pcid):
    """Generate completed Price Check PDF and ingest into Won Quotes KB."""
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"ok": False, "error": "price_check.py not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    from price_check import fill_ams704
    parsed = pc.get("parsed", {})
    source_pdf = pc.get("source_pdf", "")
    if not source_pdf or not os.path.exists(source_pdf):
        return jsonify({"ok": False, "error": "Source PDF not found"})

    pc_num = pc.get("pc_number", "unknown")
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip())
    output_path = os.path.join(DATA_DIR, f"PC_{safe_name}_Reytech_.pdf")

    result = fill_ams704(
        source_pdf=source_pdf,
        parsed_pc=parsed,
        output_pdf=output_path,
        tax_rate=pc.get("tax_rate", 0) if pc.get("tax_enabled") else 0.0,
        custom_notes=pc.get("custom_notes", ""),
        delivery_option=pc.get("delivery_option", ""),
    )

    if result.get("ok"):
        pc["output_pdf"] = output_path
        pc["status"] = "completed"
        pc["summary"] = result.get("summary", {})
        _save_price_checks(pcs)

        # Ingest completed prices into Won Quotes KB for future reference
        _ingest_pc_to_won_quotes(pc)

        return jsonify({"ok": True, "download": f"/api/pricecheck/download/{os.path.basename(output_path)}"})
    return jsonify({"ok": False, "error": result.get("error", "Unknown error")})


def _ingest_pc_to_won_quotes(pc):
    """Ingest completed Price Check pricing into Won Quotes KB."""
    if not PRICING_ORACLE_AVAILABLE:
        return
    try:
        items = pc.get("items", [])
        institution = pc.get("institution", "")
        pc_num = pc.get("pc_number", "")
        for item in items:
            pricing = item.get("pricing", {})
            price = pricing.get("recommended_price")
            if not price:
                continue
            ingest_scprs_result({
                "po_number": f"PC-{pc_num}",
                "item_number": item.get("item_number", ""),
                "description": item.get("description", ""),
                "unit_price": price,
                "supplier": "Reytech Inc.",
                "department": institution,
                "award_date": datetime.now().strftime("%Y-%m-%d"),
                "source": "price_check",
            })
        log.info(f"Ingested {len(items)} items from PC #{pc_num} into Won Quotes KB")
    except Exception as e:
        log.error(f"KB ingestion error: {e}")


@app.route("/pricecheck/<pcid>/convert-to-quote")
@auth_required
def pricecheck_convert_to_quote(pcid):
    """Convert a Price Check into a full RFQ with 704A/B and Bid Package."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    items = pc.get("items", [])
    header = pc.get("parsed", {}).get("header", {})

    # Build RFQ record from PC data
    rfq_id = str(uuid.uuid4())[:8]
    line_items = []
    for item in items:
        pricing = item.get("pricing", {})
        li = {
            "item_number": item.get("item_number", ""),
            "description": item.get("description", ""),
            "qty": item.get("qty", 1),
            "uom": item.get("uom", "ea"),
            "qty_per_uom": item.get("qty_per_uom", 1),
            "unit_cost": pricing.get("unit_cost") or pricing.get("amazon_price") or 0,
            "supplier_cost": pricing.get("unit_cost") or pricing.get("amazon_price") or 0,
            "our_price": pricing.get("recommended_price") or 0,
            "markup_pct": pricing.get("markup_pct", 25),
            "scprs_last_price": pricing.get("scprs_price"),
            "supplier_source": pricing.get("price_source", "price_check"),
            "supplier_url": pricing.get("amazon_url", ""),
        }
        line_items.append(li)

    rfq = {
        "id": rfq_id,
        "solicitation_number": f"PC-{pc.get('pc_number', 'unknown')}",
        "requestor_name": header.get("requestor", pc.get("requestor", "")),
        "requestor_email": "",
        "department": header.get("institution", pc.get("institution", "")),
        "ship_to": pc.get("ship_to", ""),
        "delivery_zip": header.get("zip_code", ""),
        "due_date": pc.get("due_date", ""),
        "phone": header.get("phone", ""),
        "line_items": line_items,
        "status": "pending",
        "source": "price_check",
        "source_pc_id": pcid,
        "award_method": "all_or_none",
        "created_at": datetime.now().isoformat(),
    }

    rfqs = load_rfqs()
    rfqs[rfq_id] = rfq
    save_rfqs(rfqs)

    # Update PC status
    pc["status"] = "converted"
    pc["converted_rfq_id"] = rfq_id
    _save_price_checks(pcs)

    return jsonify({"ok": True, "rfq_id": rfq_id})


@app.route("/api/resync")
@auth_required
def api_resync():
    """Clear entire queue + reset processed UIDs + re-poll inbox."""
    # 1. Clear queue
    save_rfqs({})
    # 2. Reset processed UIDs
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    if os.path.exists(proc_file):
        os.remove(proc_file)
        log.info("Cleared processed_emails.json")
    # 3. Reset poller so it rebuilds
    global _shared_poller
    _shared_poller = None
    # 4. Re-poll
    imported = do_poll_check()
    return jsonify({
        "ok": True,
        "cleared": True,
        "found": len(imported),
        "rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
    })


def _remove_processed_uid(uid):
    """Remove a single UID from processed_emails.json."""
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    if not os.path.exists(proc_file):
        return
    try:
        with open(proc_file) as f:
            processed = json.load(f)
        if isinstance(processed, list) and uid in processed:
            processed.remove(uid)
            with open(proc_file, "w") as f:
                json.dump(processed, f)
            log.info(f"Removed UID {uid} from processed list")
        elif isinstance(processed, dict) and uid in processed:
            del processed[uid]
            with open(proc_file, "w") as f:
                json.dump(processed, f)
    except Exception as e:
        log.error(f"Error removing UID: {e}")


@app.route("/api/clear-queue")
@auth_required
def api_clear_queue():
    """Clear all RFQs from the queue."""
    save_rfqs({})
    return jsonify({"ok": True, "message": "Queue cleared"})


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
    errors = []
    for item in r["line_items"]:
        try:
            from scprs_lookup import lookup_price, _build_search_terms
            item_num = item.get("item_number")
            desc = item.get("description")
            search_terms = _build_search_terms(item_num, desc)
            result = lookup_price(item_num, desc)
            if result:
                result["searched"] = search_terms
                results.append(result)
                # v6.0: Auto-ingest into Won Quotes KB
                if PRICING_ORACLE_AVAILABLE and result.get("price"):
                    try:
                        ingest_scprs_result(
                            po_number=result.get("po_number", ""),
                            item_number=item_num or "",
                            description=desc or "",
                            unit_price=result["price"],
                            quantity=1,
                            supplier=result.get("vendor", ""),
                            department=result.get("department", ""),
                            award_date=result.get("date", ""),
                            source=result.get("source", "scprs_live"),
                        )
                    except Exception:
                        pass  # Never let KB ingestion break the lookup flow
            else:
                results.append({
                    "price": None,
                    "note": f"No SCPRS data found",
                    "item_number": item_num,
                    "description": (desc or "")[:80],
                    "searched": search_terms,
                })
        except Exception as e:
            import traceback
            results.append({"price": None, "error": str(e), "traceback": traceback.format_exc()})
            errors.append(str(e))
    
    return jsonify({"results": results, "errors": errors if errors else None})


@app.route("/api/scprs-test")
@auth_required
def api_scprs_test():
    """SCPRS search test — ?q=stryker+xpr"""
    q = request.args.get("q", "stryker xpr")
    try:
        from scprs_lookup import test_search
        return jsonify(test_search(q))
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})


@app.route("/api/scprs-raw")
@auth_required
def api_scprs_raw():
    """Raw SCPRS debug — shows HTML field IDs found in search results."""
    q = request.args.get("q", "stryker xpr")
    try:
        from scprs_lookup import _get_session, _discover_grid_ids, SCPRS_SEARCH_URL, SEARCH_BUTTON, ALL_SEARCH_FIELDS, FIELD_DESCRIPTION
        from bs4 import BeautifulSoup
        
        session = _get_session()
        if not session.initialized:
            session.init_session()
        
        # Load search page
        page = session._load_page(2)
        icsid = session._extract_icsid(page)
        if icsid: session.icsid = icsid
        
        # POST search
        sv = {f: "" for f in ALL_SEARCH_FIELDS}
        sv[FIELD_DESCRIPTION] = q
        fd = session._build_form_data(page, SEARCH_BUTTON, sv)
        r = session.session.post(SCPRS_SEARCH_URL, data=fd, timeout=30)
        html = r.text
        soup = BeautifulSoup(html, "html.parser")
        
        import re
        count = re.search(r'(\d+)\s+to\s+(\d+)\s+of\s+(\d+)', html)
        discovered = _discover_grid_ids(soup, "ZZ_SCPR_RD_DVW")
        
        # Sample row 0 values
        row0 = {}
        for suffix in discovered:
            eid = f"ZZ_SCPR_RD_DVW_{suffix}$0"
            el = soup.find(id=eid)
            val = el.get_text(strip=True) if el else None
            row0[eid] = val
        
        # Also check for link-style elements
        link0 = soup.find("a", id="ZZ_SCPR_RD_DVW_CRDMEM_ACCT_NBR$0")
        
        # Broad scan: find ALL element IDs ending in $0
        all_row0_ids = {}
        for el in soup.find_all(id=re.compile(r'\$0$')):
            eid = el.get('id', '')
            if eid and ('SCPR' in eid or 'DVW' in eid or 'RSLT' in eid):
                all_row0_ids[eid] = el.get_text(strip=True)[:80]
        
        # Also discover with correct prefix
        discovered2 = _discover_grid_ids(soup, "ZZ_SCPR_RSLT_VW")
        
        # Table class scan
        tables = [(t.get("class",""), t.get("id",""), len(t.find_all("tr")))
                  for t in soup.find_all("table") if t.get("class")]
        grid_tables = [t for t in tables if "PSLEVEL1GRID" in str(t[0])]
        
        return jsonify({
            "query": q, "status": r.status_code, "size": len(html),
            "result_count": count.group(0) if count else "none",
            "id_discovered_RD_DVW": list(discovered.keys()),
            "id_discovered_RSLT_VW": list(discovered2.keys()),
            "all_row0_ids": all_row0_ids,
            "row0_values": row0,
            "po_link_found": link0.get_text(strip=True) if link0 else None,
            "grid_tables": grid_tables[:5],
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})


@app.route("/api/status")
@auth_required
def api_status():
    return jsonify({
        "poll": POLL_STATUS,
        "scprs_db": get_price_db_stats(),
        "rfqs": len(load_rfqs()),
    })


@app.route("/api/poll-now")
@auth_required
def api_poll_now():
    """Manual trigger: check email inbox right now."""
    try:
        imported = do_poll_check()
        return jsonify({
            "ok": True,
            "found": len(imported),
            "rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
            "last_check": POLL_STATUS.get("last_check"),
            "error": POLL_STATUS.get("error"),
        })
    except Exception as e:
        return jsonify({"ok": False, "found": 0, "error": str(e)})


@app.route("/api/diag")
@auth_required
def api_diag():
    """Diagnostic endpoint — shows email config, connection test, and inbox status."""
    import traceback
    try:
        return _api_diag_inner()
    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})

def _api_diag_inner():
    import traceback
    email_cfg = CONFIG.get("email", {})
    addr = email_cfg.get("email", "NOT SET")
    has_pw = bool(email_cfg.get("email_password"))
    host = email_cfg.get("imap_host", "imap.gmail.com")
    port = email_cfg.get("imap_port", 993)
    
    diag = {
        "config": {
            "email_address": addr,
            "has_password": has_pw,
            "password_length": len(email_cfg.get("email_password", "")),
            "imap_host": host,
            "imap_port": port,
            "imap_folder": email_cfg.get("imap_folder", "INBOX"),
        },
        "env_vars": {
            "GMAIL_ADDRESS_set": bool(os.environ.get("GMAIL_ADDRESS")),
            "GMAIL_PASSWORD_set": bool(os.environ.get("GMAIL_PASSWORD")),
            "GMAIL_ADDRESS_value": os.environ.get("GMAIL_ADDRESS", "NOT SET"),
        },
        "poll_status": POLL_STATUS,
        "connection_test": None,
        "inbox_test": None,
    }
    
    # Test IMAP connection
    try:
        import imaplib
        mail = imaplib.IMAP4_SSL(host, port)
        diag["connection_test"] = "SSL connected OK"
        
        try:
            mail.login(addr, email_cfg.get("email_password", ""))
            diag["connection_test"] = f"Logged in as {addr} OK"
            
            try:
                mail.select("INBOX")
                # Check total
                status, messages = mail.search(None, "ALL")
                total = len(messages[0].split()) if status == "OK" and messages[0] else 0
                # Check recent (last 3 days) — same as poller
                since_date = (datetime.now() - timedelta(days=3)).strftime("%d-%b-%Y")
                status3, recent = mail.uid("search", None, f"(SINCE {since_date})")
                recent_count = len(recent[0].split()) if status3 == "OK" and recent[0] else 0
                
                # Check how many already processed
                proc_file = os.path.join(DATA_DIR, "processed_emails.json")
                processed_uids = set()
                if os.path.exists(proc_file):
                    try:
                        with open(proc_file) as pf:
                            processed_uids = set(json.load(pf))
                    except: pass
                
                recent_uids = recent[0].split() if status3 == "OK" and recent[0] else []
                new_to_process = [u.decode() for u in recent_uids if u.decode() not in processed_uids]
                
                diag["inbox_test"] = {
                    "total_emails": total,
                    "recent_3_days": recent_count,
                    "already_processed": recent_count - len(new_to_process),
                    "new_to_process": len(new_to_process),
                }
                
                # Show subjects of emails that would be processed
                if new_to_process:
                    subjects = []
                    for uid_str in new_to_process[:5]:
                        st, data = mail.uid("fetch", uid_str.encode(), "(BODY.PEEK[HEADER.FIELDS (SUBJECT FROM)])")
                        if st == "OK":
                            subjects.append(data[0][1].decode("utf-8", errors="replace").strip())
                    diag["inbox_test"]["new_email_subjects"] = subjects
                
            except Exception as e:
                diag["inbox_test"] = f"SELECT/SEARCH failed: {e}"
            
            mail.logout()
        except imaplib.IMAP4.error as e:
            diag["connection_test"] = f"LOGIN FAILED: {e}"
        except Exception as e:
            diag["connection_test"] = f"LOGIN ERROR: {e}"
    except Exception as e:
        diag["connection_test"] = f"SSL CONNECT FAILED: {e}"
        diag["connection_traceback"] = traceback.format_exc()
    
    # Check processed emails file
    proc_file = email_cfg.get("processed_file", "data/processed_emails.json")
    if os.path.exists(proc_file):
        try:
            with open(proc_file) as f:
                processed = json.load(f)
            diag["processed_emails"] = {"count": len(processed), "ids": processed[-10:] if isinstance(processed, list) else list(processed)[:10]}
        except:
            diag["processed_emails"] = "corrupt file"
    else:
        diag["processed_emails"] = "file not found"
    
    # SCPRS diagnostics
    diag["scprs"] = {
        "db_stats": get_price_db_stats(),
        "db_exists": os.path.exists(os.path.join(BASE_DIR, "data", "scprs_prices.json")),
    }
    try:
        from scprs_lookup import test_connection
        import threading
        result = [False, "timeout"]
        def _test():
            try:
                result[0], result[1] = test_connection()
            except Exception as ex:
                result[1] = str(ex)
        t = threading.Thread(target=_test, daemon=True)
        t.start()
        t.join(timeout=15)  # Max 15 seconds for connectivity test (may need 2-3 loads)
        diag["scprs"]["fiscal_reachable"] = result[0]
        diag["scprs"]["fiscal_status"] = result[1]
    except Exception as e:
        diag["scprs"]["fiscal_reachable"] = False
        diag["scprs"]["fiscal_error"] = str(e)
    
    return jsonify(diag)


@app.route("/api/reset-processed")
@auth_required
def api_reset_processed():
    """Clear the processed emails list so all recent emails get re-scanned."""
    global _shared_poller
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    if os.path.exists(proc_file):
        os.remove(proc_file)
    _shared_poller = None  # Force new poller instance
    return jsonify({"ok": True, "message": "Processed emails list cleared. Hit Check Now to re-scan."})


# ═══════════════════════════════════════════════════════════════════════
# Pricing Oracle API (v6.0)
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/pricing/recommend", methods=["POST"])
@auth_required
def api_pricing_recommend():
    """Get three-tier pricing recommendation for an RFQ's line items."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Pricing oracle not available — check won_quotes_db.py and pricing_oracle.py are in repo"}), 503

    data = request.get_json() or {}
    rid = data.get("rfq_id")

    if rid:
        rfqs = load_rfqs()
        rfq = rfqs.get(rid)
        if not rfq:
            return jsonify({"error": f"RFQ {rid} not found"}), 404
        result = recommend_prices_for_rfq(rfq, config_overrides=data.get("config"))
    else:
        result = recommend_prices_for_rfq(data, config_overrides=data.get("config"))

    return jsonify(result)


@app.route("/api/won-quotes/search")
@auth_required
def api_won_quotes_search():
    """Search the Won Quotes Knowledge Base."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503

    query = request.args.get("q", "")
    item_number = request.args.get("item", "")
    max_results = int(request.args.get("max", 10))

    if not query and not item_number:
        return jsonify({"error": "Provide ?q=description or ?item=number"}), 400

    results = find_similar_items(
        item_number=item_number,
        description=query,
        max_results=max_results,
    )
    return jsonify({"query": query, "item_number": item_number, "results": results})


@app.route("/api/won-quotes/stats")
@auth_required
def api_won_quotes_stats():
    """Get Won Quotes KB statistics and pricing health check."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503

    stats = get_kb_stats()
    health = pricing_health_check()
    return jsonify({"stats": stats, "health": health})


@app.route("/api/won-quotes/dump")
@auth_required
def api_won_quotes_dump():
    """Debug: show first 10 raw KB records to verify what's stored."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503
    from won_quotes_db import load_won_quotes
    quotes = load_won_quotes()
    return jsonify({"total": len(quotes), "first_10": quotes[:10]})


@app.route("/api/debug/paths")
@auth_required
def api_debug_paths():
    """Debug: show actual filesystem paths and what exists."""
    import won_quotes_db
    results = {
        "dashboard_BASE_DIR": BASE_DIR,
        "dashboard_DATA_DIR": DATA_DIR,
        "won_quotes_DATA_DIR": won_quotes_db.DATA_DIR,
        "won_quotes_FILE": won_quotes_db.WON_QUOTES_FILE,
        "cwd": os.getcwd(),
        "app_file_location": os.path.abspath(__file__),
    }
    # Check what exists
    for path_name, path_val in list(results.items()):
        if path_val and os.path.exists(path_val):
            if os.path.isdir(path_val):
                try:
                    results[f"{path_name}_contents"] = os.listdir(path_val)
                except:
                    results[f"{path_name}_contents"] = "permission denied"
            else:
                results[f"{path_name}_exists"] = True
                results[f"{path_name}_size"] = os.path.getsize(path_val)
        else:
            results[f"{path_name}_exists"] = False
    # Check /app/data specifically
    for check_path in ["/app/data", "/app", DATA_DIR]:
        key = check_path.replace("/", "_")
        results[f"check{key}_exists"] = os.path.exists(check_path)
        if os.path.exists(check_path) and os.path.isdir(check_path):
            try:
                results[f"check{key}_contents"] = os.listdir(check_path)
            except:
                results[f"check{key}_contents"] = "permission denied"
    return jsonify(results)


@app.route("/api/won-quotes/migrate")
@auth_required
def api_won_quotes_migrate():
    """One-time migration: import existing scprs_prices.json into Won Quotes KB."""
    try:
        from scprs_lookup import migrate_local_db_to_won_quotes
        result = migrate_local_db_to_won_quotes()
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/won-quotes/seed")
@auth_required
def api_won_quotes_seed():
    """Start bulk SCPRS seed: searches ~20 common categories, drills into PO details,
    ingests unit prices into Won Quotes KB. Runs in background thread (~3-5 min)."""
    try:
        from scprs_lookup import bulk_seed_won_quotes, SEED_STATUS
        if SEED_STATUS.get("running"):
            return jsonify({"ok": False, "message": "Seed already running", "status": SEED_STATUS})
        t = threading.Thread(target=bulk_seed_won_quotes, daemon=True)
        t.start()
        return jsonify({"ok": True, "message": "Seed started in background. Check progress at /api/won-quotes/seed-status"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/won-quotes/seed-status")
@auth_required
def api_won_quotes_seed_status():
    """Check progress of bulk SCPRS seed job."""
    try:
        from scprs_lookup import SEED_STATUS
        return jsonify(SEED_STATUS)
    except Exception as e:
        return jsonify({"error": str(e)})


# ═══════════════════════════════════════════════════════════════════════
# Product Research API (v6.1 — Phase 6)
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/research/test")
@auth_required
def api_research_test():
    """Test Amazon search — ?q=nitrile+gloves"""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    q = request.args.get("q", "nitrile exam gloves")
    return jsonify(test_amazon_search(q))


@app.route("/api/research/lookup")
@auth_required
def api_research_lookup():
    """Quick product lookup — ?q=stryker+restraint+package"""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    q = request.args.get("q", "")
    if not q:
        return jsonify({"error": "Provide ?q=search+terms"}), 400
    return jsonify(quick_lookup(q))


@app.route("/api/research/rfq/<rid>")
@auth_required
def api_research_rfq(rid):
    """Research all line items in an RFQ. Runs in background thread."""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"error": f"RFQ {rid} not found"}), 404
    if RESEARCH_STATUS.get("running"):
        return jsonify({"ok": False, "message": "Research already running", "status": RESEARCH_STATUS})

    def _run_research():
        result = research_rfq_items(r)
        # Save updated supplier costs back to RFQ
        rfqs_fresh = load_rfqs()
        if rid in rfqs_fresh:
            rfqs_fresh[rid]["line_items"] = r["line_items"]
            save_rfqs(rfqs_fresh)

    t = threading.Thread(target=_run_research, daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "Research started. Check /api/research/status for progress."})


@app.route("/api/research/status")
@auth_required
def api_research_status():
    """Check progress of RFQ product research."""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    return jsonify(RESEARCH_STATUS)


@app.route("/api/research/cache-stats")
@auth_required
def api_research_cache_stats():
    """Get product research cache statistics."""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    return jsonify(get_research_cache_stats())


@app.route("/api/debug/env-check")
@auth_required
def api_debug_env_check():
    """Check if SERPAPI_KEY is visible to the app."""
    import os
    serp_val = os.environ.get("SERPAPI_KEY", "")
    all_keys = sorted(os.environ.keys())
    serp_matches = [k for k in all_keys if "SERP" in k.upper()]
    return jsonify({
        "SERPAPI_KEY_set": bool(serp_val),
        "SERPAPI_KEY_preview": f"{serp_val[:8]}..." if serp_val else "EMPTY",
        "serp_matching_keys": serp_matches,
        "all_env_keys": all_keys,
    })


@app.route("/api/config/set-serpapi-key", methods=["GET", "POST"])
@auth_required
def api_set_serpapi_key():
    """Store SerpApi key on persistent volume (bypasses Railway env var issues)."""
    if request.method == "POST":
        key = request.json.get("key", "") if request.is_json else request.args.get("key", "")
    else:
        key = request.args.get("key", "")
    if not key:
        return jsonify({"error": "Add ?key=YOUR_KEY to the URL"}), 400
    key_file = os.path.join(DATA_DIR, ".serpapi_key")
    with open(key_file, "w") as f:
        f.write(key.strip())
    return jsonify({"ok": True, "message": "SerpApi key saved to volume", "preview": f"{key[:8]}..."})


@app.route("/api/config/check-serpapi-key")
@auth_required
def api_check_serpapi_key():
    """Check if SerpApi key is stored on volume."""
    key_file = os.path.join(DATA_DIR, ".serpapi_key")
    if os.path.exists(key_file):
        with open(key_file) as f:
            key = f.read().strip()
        return jsonify({"stored": True, "preview": f"{key[:8]}..." if key else "EMPTY"})
    return jsonify({"stored": False})


# ═══════════════════════════════════════════════════════════════════════
# Price Check API (v6.2 — Phase 6)
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/pricecheck/parse", methods=["POST"])
@auth_required
def api_pricecheck_parse():
    """Parse an uploaded AMS 704 PDF. Upload as multipart file."""
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"error": "price_check.py not available"}), 503
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded. Use multipart form with 'file' field."}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400
    # Save to temp
    import tempfile
    tmp = os.path.join(DATA_DIR, f"pc_upload_{f.filename}")
    f.save(tmp)
    try:
        result = test_parse(tmp)
        result["uploaded_file"] = tmp
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pricecheck/process", methods=["POST"])
@auth_required
def api_pricecheck_process():
    """Full pipeline: parse → lookup → price → fill PDF."""
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"error": "price_check.py not available"}), 503

    # Accept file upload or path to existing file
    pdf_path = None
    if "file" in request.files:
        f = request.files["file"]
        pdf_path = os.path.join(DATA_DIR, f"pc_upload_{f.filename}")
        f.save(pdf_path)
    elif request.is_json and request.json.get("pdf_path"):
        pdf_path = request.json["pdf_path"]
    else:
        return jsonify({"error": "Upload a file or provide pdf_path in JSON"}), 400

    tax_rate = 0.0
    if request.is_json:
        tax_rate = float(request.json.get("tax_rate", 0.0))
    elif request.form.get("tax_rate"):
        tax_rate = float(request.form.get("tax_rate", 0.0))

    try:
        result = process_price_check(
            pdf_path=pdf_path,
            output_dir=DATA_DIR,
            tax_rate=tax_rate,
        )
        # If successful, make the PDF downloadable
        if result.get("ok") and result.get("output_pdf"):
            result["download_url"] = f"/api/pricecheck/download/{os.path.basename(result['output_pdf'])}"
        return jsonify(json.loads(json.dumps(result, default=str)))
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/pricecheck/download/<filename>")
@auth_required
def api_pricecheck_download(filename):
    """Download a completed Price Check PDF."""
    safe = os.path.basename(filename)
    path = os.path.join(DATA_DIR, safe)
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404
    return send_file(path, as_attachment=True, download_name=safe)


@app.route("/api/pricecheck/test-parse")
@auth_required
def api_pricecheck_test_parse():
    """Test parse the most recently uploaded PC PDF."""
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"error": "price_check.py not available"}), 503
    # Find most recent pc_upload file
    files = [f for f in os.listdir(DATA_DIR) if f.startswith("pc_upload_")]
    if not files:
        return jsonify({"error": "No uploaded PC files. POST a file to /api/pricecheck/parse first."})
    latest = sorted(files)[-1]
    return jsonify(test_parse(os.path.join(DATA_DIR, latest)))


# ═══════════════════════════════════════════════════════════════════════
# Auto-Processor API (v7.0 — Phase 7)
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/tax-rate")
@auth_required
def api_tax_rate():
    """Get CA sales tax rate. Uses ship-to zip if provided, else default CA rate."""
    zip_code = request.args.get("zip", "")
    # Try CDTFA lookup if we have a zip
    if zip_code:
        try:
            import requests as req
            # CDTFA tax rate lookup via their API
            resp = req.get(
                f"https://www.cdtfa.ca.gov/taxes-and-fees/rates.aspx",
                params={"city": "", "county": "", "zip": zip_code},
                timeout=5
            )
            # Parse rate from response if possible
            # For now fall through to default — full CDTFA scraper is in main codebase
        except:
            pass
    # Default CA rate — state govt PCs are typically tax-exempt anyway
    return jsonify({
        "rate": 0.0725,
        "jurisdiction": "CA Default",
        "note": "State government purchases are typically tax-exempt. Toggle is OFF by default for 704 PCs.",
    })


@app.route("/api/health")
@auth_required
def api_health():
    """Comprehensive system health check."""
    if not AUTO_PROCESSOR_AVAILABLE:
        return jsonify({"status": "degraded", "error": "auto_processor.py not available"})
    return jsonify(system_health_check())


@app.route("/api/audit-stats")
@auth_required
def api_audit_stats():
    """Processing statistics from audit log."""
    if not AUTO_PROCESSOR_AVAILABLE:
        return jsonify({"error": "auto_processor.py not available"}), 503
    return jsonify(get_audit_stats())


@app.route("/api/auto-process/pc", methods=["POST"])
@auth_required
def api_auto_process_pc():
    """Full autonomous pipeline for a Price Check PDF."""
    if not AUTO_PROCESSOR_AVAILABLE:
        return jsonify({"error": "auto_processor.py not available"}), 503
    if "file" not in request.files:
        return jsonify({"error": "Upload a PDF file"}), 400
    f = request.files["file"]
    pdf_path = os.path.join(DATA_DIR, f"pc_upload_{f.filename}")
    f.save(pdf_path)
    result = auto_process_price_check(pdf_path)
    return jsonify(json.loads(json.dumps(result, default=str)))


@app.route("/api/detect-type", methods=["POST"])
@auth_required
def api_detect_type():
    """Detect if a PDF is an RFQ or Price Check."""
    if not AUTO_PROCESSOR_AVAILABLE:
        return jsonify({"error": "auto_processor.py not available"}), 503
    if "file" not in request.files:
        return jsonify({"error": "Upload a PDF file"}), 400
    f = request.files["file"]
    pdf_path = os.path.join(DATA_DIR, f"detect_{f.filename}")
    f.save(pdf_path)
    result = detect_document_type(pdf_path)
    os.remove(pdf_path)
    return jsonify(result)


@app.route("/pricecheck/<pcid>/auto-process")
@auth_required
def pricecheck_auto_process(pcid):
    """Run full auto-process pipeline on an existing Price Check."""
    if not AUTO_PROCESSOR_AVAILABLE:
        return jsonify({"ok": False, "error": "auto_processor not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    source_pdf = pc.get("source_pdf", "")
    if not source_pdf or not os.path.exists(source_pdf):
        return jsonify({"ok": False, "error": "Source PDF not found"})

    result = auto_process_price_check(source_pdf, pc_id=pcid)

    # Update PC record with results
    if result.get("ok"):
        pc["items"] = result.get("parsed", {}).get("line_items", [])
        pc["parsed"] = result.get("parsed", {})
        pc["output_pdf"] = result.get("output_pdf")
        pc["confidence"] = result.get("confidence", {})
        pc["draft_email"] = result.get("draft_email", {})
        pc["timing"] = result.get("timing", {})
        pc["status"] = "completed"
        pc["summary"] = result.get("summary", {})
        _save_price_checks(pcs)
        # Ingest into KB
        _ingest_pc_to_won_quotes(pc)

    return jsonify(json.loads(json.dumps({
        "ok": result.get("ok", False),
        "timing": result.get("timing", {}),
        "confidence": result.get("confidence", {}),
        "steps": result.get("steps", []),
        "draft_email": result.get("draft_email", {}),
    }, default=str)))


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
