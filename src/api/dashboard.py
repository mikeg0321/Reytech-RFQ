#!/usr/bin/env python3
"""
Reytech RFQ Dashboard v2
Full automation: email polling → parse → SCPRS lookup → price → generate → draft email
Production-ready: password protected, env var config, gunicorn-compatible
"""
import os, json, uuid, threading, time, logging, functools, re, shutil, glob
from datetime import datetime, timezone, timedelta
from flask import (Flask, request, redirect, url_for, render_template_string,
                   send_file, jsonify, flash, Response)
from ..forms.rfq_parser import parse_rfq_attachments, identify_attachments
from ..forms.reytech_filler_v4 import load_config, get_pst_date, fill_703b, fill_704b, fill_bid_package
from ..agents.scprs_lookup import bulk_lookup, save_prices_from_rfq, get_price_db_stats
from ..agents.email_poller import EmailPoller, EmailSender
# v6.0: Pricing intelligence (graceful fallback if files not present)
try:
    from ..knowledge.pricing_oracle import recommend_prices_for_rfq, pricing_health_check
    from ..knowledge.won_quotes_db import (ingest_scprs_result, find_similar_items,
                                get_kb_stats, get_price_history)
    PRICING_ORACLE_AVAILABLE = True
except ImportError:
    PRICING_ORACLE_AVAILABLE = False
# v6.1: Product Research Agent (graceful fallback)
try:
    from ..agents.product_research import (research_product, research_rfq_items,
                                   quick_lookup, test_amazon_search,
                                   get_research_cache_stats, RESEARCH_STATUS)
    PRODUCT_RESEARCH_AVAILABLE = True
except ImportError:
    PRODUCT_RESEARCH_AVAILABLE = False
# v6.2: Price Check Processor (graceful fallback)
try:
    from ..forms.price_check import (parse_ams704, process_price_check, lookup_prices,
                              test_parse, REYTECH_INFO, clean_description)
    PRICE_CHECK_AVAILABLE = True
except ImportError:
    PRICE_CHECK_AVAILABLE = False
# v7.1: Reytech Quote Generator (graceful fallback)
try:
    from ..forms.quote_generator import (generate_quote, generate_quote_from_pc,
                                  generate_quote_from_rfq, AGENCY_CONFIGS,
                                  get_all_quotes, search_quotes,
                                  peek_next_quote_number, update_quote_status,
                                  get_quote_stats, set_quote_counter)
    QUOTE_GEN_AVAILABLE = True
except ImportError:
    QUOTE_GEN_AVAILABLE = False
# v7.0: Auto-Processor Engine (graceful fallback)
try:
    from ..auto.auto_processor import (auto_process_price_check, detect_document_type,
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
