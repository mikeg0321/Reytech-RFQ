import sys
from pathlib import Path
# Compatibility for refactored structure
sys.path.insert(0, str(Path(__file__).parent.parent))
#!/usr/bin/env python3
"""
Reytech RFQ Dashboard v2
Full automation: email polling → parse → SCPRS lookup → price → generate → draft email
Production-ready: password protected, env var config, gunicorn-compatible
"""
import os, json, uuid, sys, threading, time, logging, functools, re, shutil, glob
from datetime import datetime, timezone, timedelta
from flask import (Flask, request, redirect, url_for, render_template_string,
                   send_file, jsonify, flash, Response)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src.forms.rfq_parser import parse_rfq_attachments, identify_attachments
from src.forms.reytech_filler_v4 import (load_config, get_pst_date, fill_703b, fill_704b, fill_bid_package)
from src.agents.scprs_lookup import bulk_lookup, save_prices_from_rfq, get_price_db_stats
from src.agents.email_poller import EmailPoller, EmailSender
# v6.0: Pricing intelligence (graceful fallback if files not present)
try:
    from src.knowledge.pricing_oracle import recommend_prices_for_rfq, pricing_health_check
    from src.knowledge.won_quotes_db import (ingest_scprs_result, find_similar_items,
                                get_kb_stats, get_price_history)
    PRICING_ORACLE_AVAILABLE = True
except ImportError:
    PRICING_ORACLE_AVAILABLE = False
# v6.1: Product Research Agent (graceful fallback)
try:
    from src.agents.product_research import (research_product, research_rfq_items,
                                   quick_lookup, test_amazon_search,
                                   get_research_cache_stats, RESEARCH_STATUS)
    PRODUCT_RESEARCH_AVAILABLE = True
except ImportError:
    PRODUCT_RESEARCH_AVAILABLE = False
# v6.2: Price Check Processor (graceful fallback)
try:
    from src.forms.price_check import (parse_ams704, process_price_check, lookup_prices,
                              test_parse, REYTECH_INFO, clean_description)
    PRICE_CHECK_AVAILABLE = True
except ImportError:
    PRICE_CHECK_AVAILABLE = False
# v7.1: Reytech Quote Generator (graceful fallback)
try:
    from src.forms.quote_generator import (generate_quote, generate_quote_from_pc,
                                  generate_quote_from_rfq, AGENCY_CONFIGS,
                                  get_all_quotes, search_quotes,
                                  peek_next_quote_number, update_quote_status,
                                  get_quote_stats, set_quote_counter)
    QUOTE_GEN_AVAILABLE = True
except ImportError:
    QUOTE_GEN_AVAILABLE = False
# v7.0: Auto-Processor Engine (graceful fallback)
try:
    from src.auto.auto_processor import (auto_process_price_check, detect_document_type,
                                 score_quote_confidence, system_health_check,
                                 get_audit_stats, track_response_time)
    AUTO_PROCESSOR_AVAILABLE = True
except ImportError:
    AUTO_PROCESSOR_AVAILABLE = False
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("dashboard")
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "reytech-rfq-2026")
# ── Request-level structured logging ────────────────────────────────────────
import time as _time
@app.before_request
def _log_request_start():
    request._start_time = _time.time()
@app.after_request
def _log_request_end(response):
    if hasattr(request, '_start_time'):
        duration_ms = round((_time.time() - request._start_time) * 1000, 1)
        # Skip static/health spam
        if request.path not in ('/api/health',) and not request.path.startswith('/static'):
            log.info("%s %s → %d (%.0fms)",
                     request.method, request.path, response.status_code, duration_ms,
                     extra={"route": request.path, "method": request.method,
                            "status": response.status_code, "duration_ms": duration_ms})
    return response
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
_shared_poller = None # Shared poller instance for manual checks
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
        email_cfg = dict(email_cfg) # copy so we don't mutate
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
* {margin:0;padding:0;box-sizing:border-box}
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
.b-sent{background:var(--gn)}
.meta-g{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-bottom:20px}
.meta-i{background:var(--sf2);border-radius:8px;padding:10px 12px}
.meta-l{font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px}
.meta-v{font-size:13px;font-weight:500;margin-top:3px}
table.it{width:100%;border-collapse:collapse;font-size:12px}
table.it th{text-align:left;padding:8px;font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--bd)}
table.it td{padding:8px;border-bottom:1px solid var(--bd);vertical-align:middle}
table.it input[type=number]{{background:var(--sf2);border:1px solid var(--bd);color:var(--tx);padding:5px 8px;border-radius:6px;width:88px;font-family:'JetBrains Mono',monospace;font-size:12px}}
table.it input:focus{{outline:none;border-color:var(--ac)}}
.mono{{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--tx2)}}
.btn{{display:inline-flex;align-items:center;gap:6px;padding:8px 16px;border-radius:7px;font-size:13px;font-weight:600;cursor:pointer;border:none;transition:.15s;text-decoration:none}}
.btn-p{{background:var(--ac);color:#fff}}.btn-p:hover{{background:var(--ac2)}}
.btn-s{{background:var(--sf2);color:var(--tx);border:1px solid var(--bd)}}.btn-s:hover{{border-color:var(--ac)}}
.btn-g{{background:var(--gn);color:#0f1117}}.btn-g:hover{{opacity:.9}}
.btn-o{{background:var(--or);color:#0f1117}}.btn-o:hover{{opacity:.9}}
.btn-sm{{padding:5px 10px;font-size:11px;border-radius:5px}}
.bg{{display:flex;gap:8px;margin-top:16px;flex-wrap:wrap}}
.alert{{padding:10px 14px;border-radius:8px;font-size:12px;margin-bottom:12px}}
.al-s{{background:rgba(52,211,153,.1);border:1px solid rgba(52,211,153,.3);color:var(--gn)}}
.al-e{{background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.3);color:var(--rd)}}
.al-i{{background:rgba(79,140,255,.1);border:1px solid rgba(79,140,255,.3);color:var(--ac)}}
.markup-bar{{display:flex;gap:6px;align-items:center;margin-bottom:12px;flex-wrap:wrap}}
.markup-bar span{{font-size:11px;color:var(--tx2);margin-right:4px}}
.g-good{{color:var(--gn)}}.g-low{{color:var(--yl)}}.g-bad{{color:var(--rd)}}
.empty{{text-align:center;padding:48px 20px;color:var(--tx2)}}
.draft-box{{background:var(--sf2);border:1px solid var(--bd);border-radius:8px;padding:16px;margin-top:16px;font-size:13px;white-space:pre-wrap;line-height:1.6}}
.modal-overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:1000;overflow-y:auto;padding:20px;justify-content:center;align-items:flex-start}}
.scprs-tag{{font-size:9px;padding:2px 5px;border-radius:3px;margin-left:4px;font-weight:600}}
.scprs-hi{{background:rgba(52,211,153,.15);color:var(--gn)}}
.scprs-med{{background:rgba(251,191,36,.15);color:var(--yl)}}
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
   <td style="max-width:220px;font-size:12px">{{i.description.split('\n')[0][:60]}}</td>
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
   <td style="text-align:center;border:1px solid #000;padding:4px">${q} {uom}</td>
   <td style="font-size:12px;border:1px solid #000;padding:4px">{desc}</td>
   <td style="text-align:right;border:1px solid #000;padding:4px">$ {c.toFixed(2)}</td>
   <td style="text-align:right;font-weight:600;border:1px solid #000;padding:4px">$ {p.toFixed(2)}</td>
   <td style="text-align:right;border:1px solid #000;padding:4px">$ {ext.toFixed(2)}</td>
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
   <span>STATE OF CALIFORNIA</span><span>{dept||'CALIFORNIA CORRECTIONAL HEALTH CARE SERVICES'}</span>
  </div>
  <div style="text-align:center;padding:8px;border-bottom:2px solid #000">
   <h3 style="margin:4px 0;font-size:14px;text-transform:uppercase;letter-spacing:1px">ACQUISITION QUOTE WORKSHEET — 704B</h3>
   <div style="font-size:12px">Solicitation #${sol}</div>
  </div>
  <table style="width:100%;border-collapse:collapse;margin-top:10px">
   <tr>
    <td style="border:1px solid #000;padding:4px 8px;width:50%"><span style="font-size:9px;color:#555;text-transform:uppercase;display:block">Requestor</span>{reqName}</td>
    <td style="border:1px solid #000;padding:4px 8px"><span style="font-size:9px;color:#555;text-transform:uppercase;display:block">Due Date</span><b>{due}</b></td>
   </tr>
   <tr>
    <td style="border:1px solid #000;padding:4px 8px" colspan="2"><span style="font-size:9px;color:#555;text-transform:uppercase;display:block">Delivery Location</span>{delivery||shipTo||'—'}</td>
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
   <tbody>{rowsHtml}</tbody>
  </table>
  <div style="text-align:right;margin-top:8px;font-size:15px;font-weight:700;padding:8px;border:2px solid #000;display:inline-block;float:right">
   TOTAL: ${total.toFixed(2)}
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
# ═══════════════════════════════════════════════════════════════════════
# Routes
# ═══════════════════════════════════════════════════════════════════════
def render(content, **kw):
    _email_cfg = CONFIG.get("email", {})
    _has_email = bool(_email_cfg.get("email_password"))
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Reytech RFQ</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>{BASE_CSS}</style></head><body>
<div class="hdr"><h1><a href="/" style="color:inherit;text-decoration:none"><span>Reytech</span> RFQ Dashboard</a></h1>
<div class="hdr-right">
 <a href="/" class="btn btn-sm" style="padding:4px 10px;font-size:11px;background:#21262d;color:#c9d1d9;text-decoration:none;border:1px solid #30363d">🏠 Home</a>
 <a href="/quotes" class="btn btn-sm" style="padding:4px 10px;font-size:11px;background:#1a3a5c;color:#fff;text-decoration:none">📋 Quotes</a>
 <button class="btn btn-sm btn-s" onclick="pollNow(this)" style="padding:4px 10px;font-size:11px;cursor:pointer" id="poll-btn">Check Now</button>
 <button class="btn btn-sm btn-s" onclick="resyncAll(this)" style="padding:4px 10px;font-size:11px;cursor:pointer;border-color:var(--or);color:var(--or)" title="Clear queue & re-import all emails">🔄 Resync</button>
 <div><span class="poll-dot {'poll-on' if POLL_STATUS['running'] else 'poll-off' if not _has_email else 'poll-wait'}"></span>
 {'Polling' if POLL_STATUS['running'] else 'Email not configured' if not _has_email else 'Starting...'}
 {' · Last: ' + POLL_STATUS['last_check'] if POLL_STATUS.get('last_check') else ''}</div>
 <div>{get_pst_date()}</div>
</div></div>
<div class="ctr">
{{% with messages = get_flashed_messages(with_categories=true) %}}
 {{% for cat, msg in messages %}}<div class="alert al-{{'s' if cat=='success' else 'e' if cat=='error' else 'i'}}">{{% if cat=='success' %}}✅{{% elif cat=='error' %}}❌{{% else %}}ℹ️{{% endif %}} {{{{msg}}}}</div{{% endfor %}}
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
   
    log.info("Upload: %d PDFs saved to %s", len(saved), rfq_id)
   
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
@app.route("/rfq/<rid>/upload-templates", methods=["POST"])
@auth_required
def upload_templates(rid):
    """Upload 703B/704B/Bid Package template PDFs for an RFQ."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error"); return redirect("/")
    files = request.files.getlist("templates")
    if not files:
        flash("No files uploaded", "error"); return redirect(f"/rfq/{rid}")
    rfq_dir = os.path.join(UPLOAD_DIR, rid)
    os.makedirs(rfq_dir, exist_ok=True)
    saved = []
    for f in files:
        if f.filename and f.filename.lower().endswith(".pdf"):
            p = os.path.join(rfq_dir, f.filename)
            f.save(p)
            saved.append(p)
    if not saved:
        flash("No PDFs found in upload", "error"); return redirect(f"/rfq/{rid}")
    # Identify which forms were uploaded
    new_templates = identify_attachments(saved)
    # Merge with existing templates (don't overwrite)
    existing = r.get("templates", {})
    for key, path in new_templates.items():
        existing[key] = path
    r["templates"] = existing
    # If we now have a 704B and didn't have line items, re-parse
    if "704b" in new_templates and not r.get("line_items"):
        try:
            parsed = parse_rfq_attachments(existing)
            r["line_items"] = parsed.get("line_items", r.get("line_items", []))
            r["solicitation_number"] = parsed.get("solicitation_number", r.get("solicitation_number", ""))
            r["delivery_location"] = parsed.get("delivery_location", r.get("delivery_location", ""))
            # Auto SCPRS lookup on new items
            r["line_items"] = bulk_lookup(r.get("line_items", []))
        except Exception as e:
            log.error(f"Re-parse error: {e}")
    save_rfqs(rfqs)
    found = [k for k in new_templates.keys()]
    flash(f"Templates uploaded: {', '.join(found).upper()}", "success")
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
@app.route("/rfq/<rid>/generate-quote")
@auth_required
def rfq_generate_quote(rid):
    """Generate a standalone Reytech-branded quote PDF from an RFQ."""
    if not QUOTE_GEN_AVAILABLE:
        flash("Quote generator not available", "error")
        return redirect(f"/rfq/{rid}")
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error"); return redirect("/")
    sol = r.get("solicitation_number", "unknown")
    safe_sol = re.sub(r'[^a-zA-Z0-9_-]', '_', sol.strip())
    out_dir = os.path.join(OUTPUT_DIR, sol)
    os.makedirs(out_dir, exist_ok=True)
    output_path = os.path.join(out_dir, f"{safe_sol}_Quote_Reytech.pdf")
    # Lock-in: reuse existing quote number if already assigned
    locked_qn = r.get("reytech_quote_number", "")
    result = generate_quote_from_rfq(r, output_path,
                                      quote_number=locked_qn if locked_qn else None)
    if result.get("ok"):
        # Add to output_files list
        fname = os.path.basename(output_path)
        if "output_files" not in r:
            r["output_files"] = []
        if fname not in r["output_files"]:
            r["output_files"].append(fname)
        r["reytech_quote_number"] = result.get("quote_number", "")
        save_rfqs(rfqs)
        flash(f"Reytech Quote #{result['quote_number']} generated — ${result['total']:,.2f}", "success")
    else:
        flash(f"Quote generation failed: {result.get('error', 'unknown')}", "error")
    return redirect(f"/rfq/{rid}")
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
    return render(PAGE_DETAIL_PC, pc=pc, pcid=pcid)
@app.route("/api/pricecheck/download/<filename>")
@auth_required
def api_pricecheck_download(filename):
    """Download a completed Price Check PDF."""
    safe = os.path.basename(filename)
    path = os.path.join(DATA_DIR, safe)
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404
    return send_file(path, as_attachment=True, download_name=safe)
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
    from src.forms.price_check import fill_ams704
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
@app.route("/pricecheck/<pcid>/generate-quote")
@auth_required
def pricecheck_generate_quote(pcid):
    """Generate a standalone Reytech-branded quote PDF from a Price Check."""
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "quote_generator.py not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    pc_num = pc.get("pc_number", "unknown")
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip())
    output_path = os.path.join(DATA_DIR, f"Quote_{safe_name}_Reytech.pdf")
    # Lock-in: reuse existing quote number if already assigned
    locked_qn = pc.get("reytech_quote_number", "")
    result = generate_quote_from_pc(
        pc, output_path,
        include_tax=pc.get("tax_enabled", False),
        tax_rate=pc.get("tax_rate", 0.0725) if pc.get("tax_enabled") else 0.0,
        quote_number=locked_qn if locked_qn else None,
    )
    if result.get("ok"):
        pc["reytech_quote_pdf"] = output_path
        pc["reytech_quote_number"] = result.get("quote_number", "")
        _save_price_checks(pcs)
        return jsonify({
            "ok": True,
            "download": f"/api/pricecheck/download/{os.path.basename(output_path)}",
            "quote_number": result.get("quote_number"),
        })
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
            from src.agents.scprs_lookup import lookup_price, _build_search_terms
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
                    except:
                        pass # Never let KB ingestion break the lookup flow
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
    return jsonify(test_search(q))
@app.route("/api/scprs-raw")
@auth_required
def api_scprs_raw():
    """Raw SCPRS debug — shows HTML field IDs found in search results."""
    q = request.args.get("q", "stryker xpr")
    try:
        from src.agents.scprs_lookup import _get_session, _discover_grid_ids, SCPRS_SEARCH_URL, SEARCH_BUTTON, ALL_SEARCH_FIELDS, FIELD_DESCRIPTION
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
    import imaplib
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
        from src.agents.scprs_lookup import test_connection
        import threading
        result = [False, "timeout"]
        def _test():
            try:
                result[0], result[1] = test_connection()
            except Exception as ex:
                result[1] = str(ex)
        t = threading.Thread(target=_test, daemon=True)
        t.start()
        t.join(timeout=15) # Max 15 seconds for connectivity test (may need 2-3 loads)
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
    _shared_poller = None # Force new poller instance
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
    from src.knowledge.won_quotes_db import load_won_quotes
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
        from src.agents.scprs_lookup import migrate_local_db_to_won_quotes
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
        from src.agents.scprs_lookup import bulk_seed_won_quotes, SEED_STATUS
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
        from src.agents.scprs_lookup import SEED_STATUS
        return jsonify(SEED_STATUS)
    except Exception as e:
        return jsonify({"error": str(e)})
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
# ─── Logo Upload + Quotes Database ────────────────────────────────────────────
@app.route("/settings/upload-logo", methods=["POST"])
@auth_required
def upload_logo():
    """Upload Reytech logo for quote PDFs."""
    if "logo" not in request.files:
        flash("No file selected", "error")
        return redirect(request.referrer or "/")
    f = request.files["logo"]
    if not f.filename:
        flash("No file selected", "error")
        return redirect(request.referrer or "/")
    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ("png", "jpg", "jpeg", "gif"):
        flash("Logo must be PNG, JPG, or GIF", "error")
        return redirect(request.referrer or "/")
    dest = os.path.join(DATA_DIR, f"reytech_logo.{ext}")
    # Remove old logos
    for old in glob.glob(os.path.join(DATA_DIR, "reytech_logo.*")):
        os.remove(old)
    f.save(dest)
    flash(f"Logo uploaded: {f.filename}", "success")
    return redirect(request.referrer or "/")
@app.route("/quotes/<quote_number>/status", methods=["POST"])
@auth_required
def quote_update_status(quote_number):
    """Mark a quote as won, lost, or pending."""
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"})
    data = request.json or request.form
    new_status = data.get("status", "").lower()
    po_number = data.get("po_number", "")
    notes = data.get("notes", "")
    if new_status not in ("won", "lost", "pending"):
        return jsonify({"ok": False, "error": f"Invalid status: {new_status}"})
    found = update_quote_status(quote_number, new_status, po_number, notes)
    if not found:
        return jsonify({"ok": False, "error": f"Quote {quote_number} not found"})
    return jsonify({"ok": True, "quote_number": quote_number, "status": new_status})
@app.route("/quotes")
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
        "won": ("✅ Won", "#3fb950", "rgba(52,211,153,.08)"),
        "lost": ("❌ Lost", "#f85149", "rgba(248,113,113,.08)"),
        "pending": ("⏳ Pending", "#d29922", "rgba(210,153,34,.08)"),
    }
    rows_html = ""
    for qt in quotes:
        fname = os.path.basename(qt.get("pdf_path", ""))
        dl = f'<a href="/api/pricecheck/download/{fname}" title="Download PDF">📥</a>' if fname else ""
        st = qt.get("status", "pending")
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
        rows_html += f"""<tr data-qn="{qn}">
         <td style="font-family:'JetBrains Mono',monospace;font-weight:700">{qn}</td>
         <td>{qt.get('date','')}</td>
         <td>{qt.get('agency','')}</td>
         <td style="max-width:200px">{qt.get('institution','')[:40]}</td>
         <td>{qt.get('rfq_number','')}</td>
         <td style="text-align:right;font-weight:600;font-family:'JetBrains Mono',monospace">${qt.get('total',0):,.2f}</td>
         <td style="text-align:center">{toggle}</td>
         <td style="text-align:center">
          <span style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;color:{color};background:{bg}">{lbl}</span>{po_html}
         </td>
         <td style="text-align:center;white-space:nowrap">
          <button onclick="markQuote('{qn}','won')" class="btn btn-sm" style="background:rgba(52,211,153,.15);color:#3fb950;border:1px solid rgba(52,211,153,.3);padding:2px 6px;font-size:11px;cursor:pointer" title="Mark Won">✅</button>
          <button onclick="markQuote('{qn}','lost')" class="btn btn-sm" style="background:rgba(248,113,113,.15);color:#f85149;border:1px solid rgba(248,113,113,.3);padding:2px 6px;font-size:11px;cursor:pointer" title="Mark Lost">❌</button>
          {dl}
         </td>
        </tr>
        <tr id="{detail_id}" style="display:none"><td colspan="9" style="background:var(--sf2);padding:8px 16px;border-left:3px solid var(--ac)">{detail_rows if detail_rows else '<span style="color:var(--tx2);font-size:11px">No item details available</span>'}</td></tr>"""
    # Win rate stats bar
    wr = stats.get("win_rate", 0)
    wr_color = "#3fb950" if wr >= 50 else ("#d29922" if wr >= 30 else "#f85149")
    stats_html = f"""
     <div style="display:flex;gap:20px;align-items:center;flex-wrap:wrap">
      <div><span style="color:var(--tx2)">Total:</span> <strong>{stats['total']}</strong></div>
      <div><span style="color:#3fb950">Won:</span> <strong>{stats['won']}</strong> (${stats['won_total']:,.0f})</div>
      <div><span style="color:#f85149">Lost:</span> <strong>{stats['lost']}</strong></div>
      <div><span style="color:#d29922">Pending:</span> <strong>{stats['pending']}</strong></div>
      <div><span style="color:var(--tx2)">Win Rate:</span> <strong style="color:{wr_color}">{wr}%</strong></div>
      <div style="margin-left:auto"><span style="color:var(--tx2)">Next:</span> <strong>{next_num}</strong></div>
     </div>
    """
    return render(f"""
     <h2 style="margin-bottom:12px">📋 Reytech Quotes Database</h2>
     <!-- Stats Bar -->
     <div class="card" style="margin-bottom:12px;padding:14px">{stats_html}</div>
     <!-- Search + Filters -->
     <div class="card" style="margin-bottom:12px;padding:14px">
      <form method="get" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
       <input name="q" value="{q}" placeholder="Search quotes..." style="flex:1;min-width:180px;padding:8px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx)">
       <select name="agency" style="padding:8px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx)">
        <option value="">All Agencies</option>
        <option value="CDCR" {"selected" if agency_filter=="CDCR" else ""}>CDCR</option>
        <option value="CCHCS" {"selected" if agency_filter=="CCHCS" else ""}>CCHCS</option>
        <option value="CalVet" {"selected" if agency_filter=="CalVet" else ""}>CalVet</option>
        <option value="DGS" {"selected" if agency_filter=="DGS" else ""}>DGS</option>
       </select>
       <select name="status" style="padding:8px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx)">
        <option value="">All Status</option>
        <option value="pending" {"selected" if status_filter=="pending" else ""}>⏳ Pending</option>
        <option value="won" {"selected" if status_filter=="won" else ""}>✅ Won</option>
        <option value="lost" {"selected" if status_filter=="lost" else ""}>❌ Lost</option>
       </select>
       <button type="submit" class="btn btn-p">Search</button>
      </form>
     </div>
     <!-- Logo Upload -->
     <div class="card" style="margin-bottom:12px;padding:14px">
      <div style="display:flex;gap:16px;align-items:center;flex-wrap:wrap">
       <span>Logo: {"✅ Uploaded" if logo_exists else "❌ Not uploaded (text fallback)"}</span>
       <form method="post" action="/settings/upload-logo" enctype="multipart/form-data" style="display:flex;gap:8px;align-items:center">
        <input type="file" name="logo" accept=".png,.jpg,.jpeg,.gif" style="font-size:13px">
        <button type="submit" class="btn btn-sm btn-g">Upload Logo</button>
       </form>
      </div>
     </div>
     <!-- Quotes Table -->
     <div class="card" style="padding:0;overflow-x:auto">
      <table>
       <thead><tr>
        <th>Quote #</th><th>Date</th><th>Agency</th><th>Institution</th><th>RFQ #</th>
        <th style="text-align:right">Total</th><th>Items</th><th>Status</th><th>Actions</th>
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
    """, title="Quotes Database")
# Start polling on import (for gunicorn) and on direct run
start_polling()
if __name__ == "__main__":
    email_cfg = CONFIG.get("email", {})
    port = int(os.environ.get("PORT", 5000))
    print(f"""
╔══════════════════════════════════════════════════╗
║ Reytech RFQ Dashboard v2 ║
║ ║
║ 🌐 http://localhost:{port} ║
║ 📧 Email polling: {'ON' if email_cfg.get('email_password') else 'OFF (set password)':30s}║
║ 🔒 Login: {DASH_USER} / {'*'*len(DASH_PASS):20s} ║
║ 📊 SCPRS DB: {get_price_db_stats()['total_items']} items{' ':27s}║
╚══════════════════════════════════════════════════╝
    """)
    app.run(host="0.0.0.0", port=port, debug=False)
