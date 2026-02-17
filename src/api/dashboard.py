#!/usr/bin/env python3
"""
Reytech RFQ Dashboard — API Routes Blueprint
Refactored from monolithic dashboard.py into modular Blueprint.
All route handlers live here; app creation is in app.py.
"""

import os, json, uuid, sys, threading, time, logging, functools, re, shutil, glob
from datetime import datetime, timezone, timedelta
from flask import (Blueprint, request, redirect, url_for, render_template_string,
                   send_file, jsonify, flash, Response, current_app)

# Add project root to path for imports
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Import from src modules (with fallback to root-level imports)
try:
    from src.forms.reytech_filler_v4 import (load_config, get_pst_date, fill_703b, fill_704b, fill_bid_package)
except ImportError:
    from reytech_filler_v4 import (load_config, get_pst_date, fill_703b, fill_704b, fill_bid_package)

try:
    from src.forms.rfq_parser import parse_rfq_attachments, identify_attachments
except ImportError:
    from rfq_parser import parse_rfq_attachments, identify_attachments

try:
    from src.agents.scprs_lookup import bulk_lookup, save_prices_from_rfq, get_price_db_stats
except ImportError:
    from scprs_lookup import bulk_lookup, save_prices_from_rfq, get_price_db_stats

try:
    from src.agents.email_poller import EmailPoller, EmailSender
except ImportError:
    from email_poller import EmailPoller, EmailSender

# v6.0: Pricing intelligence (graceful fallback if files not present)
try:
    from src.knowledge.pricing_oracle import recommend_prices_for_rfq, pricing_health_check
    from src.knowledge.won_quotes_db import (ingest_scprs_result, find_similar_items,
                                get_kb_stats, get_price_history)
    PRICING_ORACLE_AVAILABLE = True
except ImportError:
    try:
        from pricing_oracle import recommend_prices_for_rfq, pricing_health_check
        from won_quotes_db import (ingest_scprs_result, find_similar_items,
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
    try:
        from product_research import (research_product, research_rfq_items,
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
    try:
        from price_check import (parse_ams704, process_price_check, lookup_prices,
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
    try:
        from quote_generator import (generate_quote, generate_quote_from_pc,
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
    try:
        from auto_processor import (auto_process_price_check, detect_document_type,
                                     score_quote_confidence, system_health_check,
                                     get_audit_stats, track_response_time)
        AUTO_PROCESSOR_AVAILABLE = True
    except ImportError:
        AUTO_PROCESSOR_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("dashboard")

bp = Blueprint("dashboard", __name__)
# Secret key set in app.py

# ── Request-level structured logging ────────────────────────────────────────
import time as _time

@bp.before_request
def _log_request_start():
    request._start_time = _time.time()

@bp.after_request
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

try:
    from src.core.paths import PROJECT_ROOT as BASE_DIR, DATA_DIR, UPLOAD_DIR, OUTPUT_DIR
except ImportError:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
# HTML Templates (extracted to src/api/templates.py)
# ═══════════════════════════════════════════════════════════════════════

from src.api.templates import BASE_CSS, PAGE_HOME, PAGE_DETAIL, build_pc_detail_html, build_quotes_page_content

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


@bp.route("/")
@auth_required
def home():
    return render(PAGE_HOME, rfqs=load_rfqs(), price_checks=_load_price_checks())

@bp.route("/upload", methods=["POST"])
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
    _transition_status(rfq, "pending", actor="system", notes="Parsed from email")
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


# ═══════════════════════════════════════════════════════════════════════
# Status Lifecycle — tracks every transition for PCs and RFQs
# ═══════════════════════════════════════════════════════════════════════

# PC lifecycle: parsed → priced → completed → won/lost/expired
# RFQ lifecycle: new → pending → ready → generated → sent → won/lost
PC_LIFECYCLE = ["parsed", "priced", "completed", "won", "lost", "expired"]
RFQ_LIFECYCLE = ["new", "pending", "ready", "generated", "sent", "won", "lost"]


def _transition_status(record, new_status, actor="system", notes=""):
    """Record a status transition with full history.
    
    Mutates record in place. Returns the record for chaining.
    """
    old_status = record.get("status", "")
    record["status"] = new_status
    now = datetime.now().isoformat()
    record["status_updated"] = now

    # Build status_history (create if missing for legacy records)
    history = record.get("status_history", [])
    entry = {"from": old_status, "to": new_status, "timestamp": now, "actor": actor}
    if notes:
        entry["notes"] = notes
    history.append(entry)
    record["status_history"] = history
    return record


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

    # Auto-assign quote number (draft) on PC intake
    draft_quote_num = ""
    if QUOTE_GEN_AVAILABLE:
        try:
            from src.forms.quote_generator import _next_quote_number, _save_all_quotes, get_all_quotes
            draft_quote_num = _next_quote_number()
            # Check for previous PCs from same institution
            existing_quotes = get_all_quotes()
            prev_pcs = [q for q in existing_quotes 
                        if q.get("institution", "").lower() == institution.lower() 
                        and q.get("status") in ("pending", "draft")]
            if prev_pcs:
                log.info("Found %d previous quote(s) for %s: %s", 
                         len(prev_pcs), institution,
                         ", ".join(q.get("quote_number", "") for q in prev_pcs))
            # Save as draft in quotes log
            draft_entry = {
                "quote_number": draft_quote_num,
                "date": datetime.now().strftime("%b %d, %Y"),
                "agency": parsed.get("header", {}).get("agency", ""),
                "institution": institution,
                "rfq_number": pc_num,
                "total": 0,
                "subtotal": 0,
                "tax": 0,
                "items_count": len(items),
                "pdf_path": "",
                "created_at": datetime.now().isoformat(),
                "status": "draft",
                "source_pc_id": pc_id,
                "items_text": " | ".join(i.get("description", "")[:50] for i in items[:5]),
            }
            all_quotes = get_all_quotes()
            all_quotes.append(draft_entry)
            _save_all_quotes(all_quotes)
            log.info("Auto-assigned draft quote %s to PC #%s (%s)", draft_quote_num, pc_num, institution)
        except Exception as e:
            log.error("Failed to auto-assign quote number: %s", e)

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
        "status_history": [{"from": "", "to": "parsed", "timestamp": datetime.now().isoformat(), "actor": "system"}],
        "created_at": datetime.now().isoformat(),
        "parsed": parsed,
        "reytech_quote_number": draft_quote_num,
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
        except Exception:
            return {}
    return {}


def _save_price_checks(pcs):
    path = os.path.join(DATA_DIR, "price_checks.json")
    with open(path, "w") as f:
        json.dump(pcs, f, indent=2, default=str)


@bp.route("/rfq/<rid>")
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


@bp.route("/rfq/<rid>/update", methods=["POST"])
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
                except Exception: pass
        # Save edited description
        desc_val = request.form.get(f"desc_{i}")
        if desc_val is not None:
            item["description"] = desc_val
    
    _transition_status(r, "ready", actor="user", notes="Pricing updated")
    save_rfqs(rfqs)
    
    # Save SCPRS prices for future lookups
    save_prices_from_rfq(r)
    
    flash("Pricing saved", "success")
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/upload-templates", methods=["POST"])
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


@bp.route("/rfq/<rid>/generate", methods=["POST"])
@auth_required
def generate(rid):
    log.info("Generate bid package for RFQ %s", rid)
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return redirect("/")
    
    # Update pricing from form
    for i, item in enumerate(r["line_items"]):
        for field, key in [("cost", "supplier_cost"), ("scprs", "scprs_last_price"), ("price", "price_per_unit")]:
            v = request.form.get(f"{field}_{i}")
            if v:
                try: item[key] = float(v)
                except Exception: pass
    
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
        
        _transition_status(r, "generated", actor="system", notes="Bid package filled")
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


@bp.route("/rfq/<rid>/generate-quote")
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
        log.info("Quote #%s generated for RFQ %s — $%s", result.get("quote_number"), rid, f"{result['total']:,.2f}")
        flash(f"Reytech Quote #{result['quote_number']} generated — ${result['total']:,.2f}", "success")
    else:
        log.error("Quote generation failed for RFQ %s: %s", rid, result.get("error", "unknown"))
        flash(f"Quote generation failed: {result.get('error', 'unknown')}", "error")

    return redirect(f"/rfq/{rid}")

@bp.route("/rfq/<rid>/send", methods=["POST"])
@auth_required
def send_email(rid):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r or not r.get("draft_email"):
        flash("No draft to send", "error"); return redirect(f"/rfq/{rid}")
    
    try:
        sender = EmailSender(CONFIG.get("email", {}))
        sender.send(r["draft_email"])
        _transition_status(r, "sent", actor="user", notes="Email sent to buyer")
        r["sent_at"] = datetime.now().isoformat()
        save_rfqs(rfqs)
        flash(f"Bid response sent to {r['draft_email']['to']}", "success")
    except Exception as e:
        flash(f"Send failed: {e}. Use 'Open in Mail App' instead.", "error")
    
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/delete", methods=["POST"])
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
        log.info("Deleted RFQ #%s (id=%s)", sol, rid)
        flash(f"Deleted RFQ #{sol}", "success")
    return redirect("/")


# ═══════════════════════════════════════════════════════════════════════
# Price Check Pages (v6.2)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/pricecheck/<pcid>")
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
        # Clean description for display (strip font specs, dimensions, etc.)
        raw_desc = item.get("description_raw") or item.get("description", "")
        display_desc = item.get("description", raw_desc)
        if PRICE_CHECK_AVAILABLE and raw_desc:
            display_desc = clean_description(raw_desc)
            # Persist cleaned version back
            if display_desc != item.get("description"):
                item["description"] = display_desc
                item["description_raw"] = raw_desc
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

        # Amazon match link + ASIN
        title = (p.get("amazon_title") or "")[:40]
        url = p.get("amazon_url", "")
        asin = p.get("amazon_asin", "")
        link_parts = []
        if url and title:
            link_parts.append(f'<a href="{url}" target="_blank" title="{p.get("amazon_title","")}">{title}</a>')
        if asin:
            link_parts.append(f'<span style="color:#58a6ff;font-size:10px;font-family:JetBrains Mono,monospace">ASIN: {asin}</span>')
        link = "<br>".join(link_parts) if link_parts else "—"

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
         <td style="text-align:center"><input type="checkbox" name="bid_{idx}" {bid_checked} onchange="toggleBid({idx},this)" style="width:18px;height:18px;cursor:pointer"></td>
         <td><input type="number" name="itemnum_{idx}" value="{item.get('item_number','')}" class="num-in sm" style="width:40px"></td>
         <td><input type="number" name="qty_{idx}" value="{qty}" class="num-in sm" style="width:55px" onchange="recalcPC()"></td>
         <td><input type="text" name="uom_{idx}" value="{item.get('uom','EA').upper()}" class="text-in" style="width:45px;text-transform:uppercase;text-align:center;font-weight:600"></td>
         <td><textarea name="desc_{idx}" class="text-in" style="width:100%;min-height:38px;resize:vertical;font-family:inherit;font-size:13px;line-height:1.4;padding:6px 8px" title="{raw_desc.replace('"','&quot;').replace('<','&lt;')}">{display_desc.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')}</textarea></td>
         <td style="font-weight:600;font-size:14px">{scprs_str}{scprs_badge}</td>
         <td style="font-weight:600;font-size:14px" {amazon_data}>{amazon_str}</td>
         <td style="font-size:12px;max-width:180px">{link}</td>
         <td><input type="number" step="0.01" min="0" name="cost_{idx}" value="{cost_str}" class="num-in" onchange="recalcRow({idx})"></td>
         <td><input type="number" step="1" min="0" max="200" name="markup_{idx}" value="{markup_pct}" class="num-in sm" style="width:48px" onchange="recalcRow({idx})"><span style="color:#8b949e;font-size:13px">%</span></td>
         <td><input type="number" step="0.01" min="0" name="price_{idx}" value="{final_str}" class="num-in" onchange="recalcPC()"></td>
         <td class="ext" style="font-weight:600;font-size:14px">{ext}</td>
         <td class="profit" style="font-size:14px">{profit_str}</td>
         <td style="text-align:center;font-size:15px">{grade_html}</td>
        </tr>"""

    download_html = ""
    if pc.get("output_pdf") and os.path.exists(pc.get("output_pdf", "")):
        fname = os.path.basename(pc["output_pdf"])
        download_html += f'<a href="/api/pricecheck/download/{fname}" class="btn btn-sm btn-g" style="font-size:13px">📥 Download 704</a>'
    if pc.get("reytech_quote_pdf") and os.path.exists(pc.get("reytech_quote_pdf", "")):
        qfname = os.path.basename(pc["reytech_quote_pdf"])
        qnum = pc.get("reytech_quote_number", "")
        download_html += f' <a href="/api/pricecheck/download/{qfname}" class="btn btn-sm" style="background:#1a3a5c;color:#fff;font-size:13px">📥 Quote {qnum}</a>'

    # 45-day expiry from processing date
    try:
        processed = pc.get("uploaded_at") or pc.get("created_at") or datetime.now().isoformat()
        if isinstance(processed, str):
            base = datetime.fromisoformat(processed.replace("Z", "+00:00"))
        else:
            base = processed
        expiry = base + timedelta(days=45)
        expiry_date = expiry.strftime("%m/%d/%Y")
    except Exception:
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

    # Pre-compute next quote number preview
    next_quote_preview = peek_next_quote_number() if QUOTE_GEN_AVAILABLE else ""
    
    html = build_pc_detail_html(
        pcid=pcid, pc=pc, items=items, items_html=items_html,
        download_html=download_html, expiry_date=expiry_date,
        header=header, custom_val=custom_val, custom_display=custom_display,
        del_sel=del_sel, next_quote_preview=next_quote_preview
    )
    return html


@bp.route("/pricecheck/<pcid>/lookup")
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
    _transition_status(pc, "priced", actor="user", notes="Prices saved")
    _save_price_checks(pcs)

    found = sum(1 for i in pc["items"] if i.get("pricing", {}).get("amazon_price"))
    return jsonify({"ok": True, "found": found, "total": len(pc["items"])})


@bp.route("/pricecheck/<pcid>/scprs-lookup")
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


@bp.route("/pricecheck/<pcid>/save-prices", methods=["POST"])
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


@bp.route("/pricecheck/<pcid>/generate")
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
        _transition_status(pc, "completed", actor="system", notes="704 PDF filled")
        pc["summary"] = result.get("summary", {})
        _save_price_checks(pcs)

        # Ingest completed prices into Won Quotes KB for future reference
        _ingest_pc_to_won_quotes(pc)

        return jsonify({"ok": True, "download": f"/api/pricecheck/download/{os.path.basename(output_path)}"})
    return jsonify({"ok": False, "error": result.get("error", "Unknown error")})


@bp.route("/pricecheck/<pcid>/generate-quote")
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


@bp.route("/pricecheck/<pcid>/convert-to-quote")
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
    _transition_status(pc, "completed", actor="system", notes="Reytech quote generated")
    pc["converted_rfq_id"] = rfq_id
    _save_price_checks(pcs)

    return jsonify({"ok": True, "rfq_id": rfq_id})


@bp.route("/api/resync")
@auth_required
def api_resync():
    """Clear entire queue + reset processed UIDs + re-poll inbox."""
    log.info("Full resync triggered — clearing queue and re-polling")
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


@bp.route("/api/clear-queue")
@auth_required
def api_clear_queue():
    """Clear all RFQs from the queue."""
    save_rfqs({})
    return jsonify({"ok": True, "message": "Queue cleared"})


@bp.route("/dl/<rid>/<fname>")
@auth_required
def download(rid, fname):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return redirect("/")
    p = os.path.join(OUTPUT_DIR, r["solicitation_number"], fname)
    if os.path.exists(p): return send_file(p, as_attachment=True)
    flash("File not found", "error"); return redirect(f"/rfq/{rid}")


@bp.route("/api/scprs/<rid>")
@auth_required
def api_scprs(rid):
    """SCPRS lookup API endpoint for the dashboard JS."""
    log.info("SCPRS lookup requested for RFQ %s", rid)
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


@bp.route("/api/scprs-test")
@auth_required
def api_scprs_test():
    """SCPRS search test — ?q=stryker+xpr"""
    q = request.args.get("q", "stryker xpr")
    try:
        from src.agents.scprs_lookup import test_search
        return jsonify(test_search(q))
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})


@bp.route("/api/scprs-raw")
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


@bp.route("/api/status")
@auth_required
def api_status():
    return jsonify({
        "poll": POLL_STATUS,
        "scprs_db": get_price_db_stats(),
        "rfqs": len(load_rfqs()),
    })


@bp.route("/api/poll-now")
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


@bp.route("/api/diag")
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
                    except Exception: pass
                
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
        except Exception:
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
        t.join(timeout=15)  # Max 15 seconds for connectivity test (may need 2-3 loads)
        diag["scprs"]["fiscal_reachable"] = result[0]
        diag["scprs"]["fiscal_status"] = result[1]
    except Exception as e:
        diag["scprs"]["fiscal_reachable"] = False
        diag["scprs"]["fiscal_error"] = str(e)
    
    return jsonify(diag)


@bp.route("/api/reset-processed")
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

@bp.route("/api/pricing/recommend", methods=["POST"])
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


@bp.route("/api/won-quotes/search")
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


@bp.route("/api/won-quotes/stats")
@auth_required
def api_won_quotes_stats():
    """Get Won Quotes KB statistics and pricing health check."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503

    stats = get_kb_stats()
    health = pricing_health_check()
    return jsonify({"stats": stats, "health": health})


@bp.route("/api/won-quotes/dump")
@auth_required
def api_won_quotes_dump():
    """Debug: show first 10 raw KB records to verify what's stored."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503
    from src.knowledge.won_quotes_db import load_won_quotes
    quotes = load_won_quotes()
    return jsonify({"total": len(quotes), "first_10": quotes[:10]})


@bp.route("/api/debug/paths")
@auth_required
def api_debug_paths():
    """Debug: show actual filesystem paths and what exists."""
    try:
        from src.knowledge import won_quotes_db
    except ImportError:
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
                except Exception:
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
            except Exception:
                results[f"check{key}_contents"] = "permission denied"
    return jsonify(results)


@bp.route("/api/won-quotes/migrate")
@auth_required
def api_won_quotes_migrate():
    """One-time migration: import existing scprs_prices.json into Won Quotes KB."""
    try:
        from src.agents.scprs_lookup import migrate_local_db_to_won_quotes
        result = migrate_local_db_to_won_quotes()
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/won-quotes/seed")
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


@bp.route("/api/won-quotes/seed-status")
@auth_required
def api_won_quotes_seed_status():
    """Check progress of bulk SCPRS seed job."""
    try:
        from src.agents.scprs_lookup import SEED_STATUS
        return jsonify(SEED_STATUS)
    except Exception as e:
        return jsonify({"error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
# CUSTOMERS CRM — Agency parent/child, QuickBooks-synced
# ═══════════════════════════════════════════════════════════════════════════════

def _load_customers():
    """Load customers CRM database. Auto-seeds from bundled file if missing."""
    path = os.path.join(DATA_DIR, "customers.json")
    try:
        with open(path) as f:
            data = json.load(f)
            if data:
                return data
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    # Auto-seed: Railway volume may not have customers.json yet
    # Check for seed file in repo root (not overridden by volume mount)
    seed_path = os.path.join(BASE_DIR, "customers_seed.json")
    if os.path.exists(seed_path):
        try:
            with open(seed_path) as f:
                data = json.load(f)
            if data:
                log.info(f"Auto-seeding {len(data)} customers from seed file")
                os.makedirs(DATA_DIR, exist_ok=True)
                with open(path, "w") as f:
                    json.dump(data, f, indent=2)
                return data
        except Exception as e:
            log.warning(f"Failed to seed customers: {e}")
    return []

def _save_customers(customers):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "customers.json")
    with open(path, "w") as f:
        json.dump(customers, f, indent=2)

@bp.route("/api/customers")
@auth_required
def api_customers():
    """Search customers. ?q=term&agency=CDCR&parent=true (parent-only)"""
    customers = _load_customers()
    q = request.args.get("q", "").lower()
    agency = request.args.get("agency", "")
    parent_only = request.args.get("parent", "") == "true"
    results = []
    for c in customers:
        if agency and c.get("agency", "").lower() != agency.lower():
            continue
        if parent_only and c.get("parent"):
            continue
        if q:
            searchable = " ".join([
                c.get("display_name", ""), c.get("company", ""),
                c.get("qb_name", ""), c.get("agency", ""),
                c.get("city", ""), c.get("abbreviation", ""),
            ]).lower()
            if q not in searchable:
                continue
        results.append(c)
    return jsonify(results)

@bp.route("/api/customers", methods=["POST"])
@auth_required
def api_customers_add():
    """Add a new customer. User confirms before saving."""
    data = request.json
    if not data or not data.get("display_name"):
        return jsonify({"ok": False, "error": "display_name required"})
    customers = _load_customers()
    # Check for duplicate
    existing = [c for c in customers
                if c.get("display_name", "").lower() == data["display_name"].lower()]
    if existing:
        return jsonify({"ok": False, "error": "Customer already exists",
                        "existing": existing[0]})
    entry = {
        "qb_name": data.get("qb_name", data["display_name"]),
        "display_name": data["display_name"],
        "company": data.get("company", data["display_name"]),
        "parent": data.get("parent", ""),
        "agency": data.get("agency", "DEFAULT"),
        "abbreviation": data.get("abbreviation", ""),
        "address": data.get("address", ""),
        "city": data.get("city", ""),
        "state": data.get("state", "CA"),
        "zip": data.get("zip", ""),
        "phone": data.get("phone", ""),
        "email": data.get("email", ""),
        "open_balance": 0,
        "source": "manual",
    }
    customers.append(entry)
    _save_customers(customers)
    log.info("Customer added: %s (agency=%s)", entry["display_name"], entry.get("agency", ""))
    return jsonify({"ok": True, "customer": entry})

@bp.route("/api/customers/hierarchy")
@auth_required
def api_customers_hierarchy():
    """Return parent/child agency tree."""
    customers = _load_customers()
    parents = {}
    for c in customers:
        if not c.get("parent"):
            parents[c["display_name"]] = {
                "agency": c.get("agency", ""),
                "company": c.get("company", ""),
                "children": [],
            }
    for c in customers:
        p = c.get("parent", "")
        if p and p in parents:
            parents[p]["children"].append({
                "display_name": c["display_name"],
                "abbreviation": c.get("abbreviation", ""),
                "city": c.get("city", ""),
            })
    return jsonify(parents)

@bp.route("/api/customers/match")
@auth_required
def api_customers_match():
    """Match an institution name to CRM. Returns best match + new flag."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"matched": False, "candidates": []})
    customers = _load_customers()
    q_upper = q.upper()
    # Exact match first (check all name fields)
    for c in customers:
        names = [c.get("display_name",""), c.get("company",""),
                 c.get("abbreviation",""), c.get("qb_name","")]
        if any(q_upper == n.upper() for n in names if n):
            return jsonify({"matched": True, "customer": c, "is_new": False})
    # Abbreviation expansion: CSP-Sacramento → California State Prison, Sacramento
    _ABBR_MAP = {
        "CSP": "California State Prison",
        "SCC": "Sierra Conservation Center",
        "CIM": "California Institution for Men",
        "CIW": "California Institution for Women",
        "CMC": "California Men's Colony",
        "CMF": "California Medical Facility",
        "CTF": "Correctional Training Facility",
        "CHCF": "California Health Care Facility",
        "SATF": "Substance Abuse Treatment Facility",
    }
    expanded = q
    for abbr, full in _ABBR_MAP.items():
        if q_upper.startswith(abbr + "-") or q_upper.startswith(abbr + " "):
            suffix = q[len(abbr):].lstrip("- ")
            expanded = f"{full}, {suffix}" if suffix else full
            break
    if expanded != q:
        exp_upper = expanded.upper()
        for c in customers:
            if exp_upper in c.get("display_name", "").upper():
                return jsonify({"matched": True, "customer": c, "is_new": False})
    # Abbreviation-only match (e.g. "SAC" → abbreviation field)
    if len(q) <= 5:
        for c in customers:
            if c.get("abbreviation", "").upper() == q_upper:
                return jsonify({"matched": True, "customer": c, "is_new": False})
    # Fuzzy: token overlap
    q_tokens = set(q_upper.split())
    scored = []
    for c in customers:
        search_text = " ".join([c.get("display_name",""), c.get("company",""),
                                c.get("abbreviation","")]).upper()
        c_tokens = set(search_text.split())
        overlap = len(q_tokens & c_tokens)
        if overlap > 0:
            scored.append((overlap / max(len(q_tokens), 1), c))
    scored.sort(key=lambda x: -x[0])
    candidates = [s[1] for s in scored[:5] if s[0] > 0.3]
    if candidates and scored[0][0] >= 0.6:
        return jsonify({"matched": True, "customer": candidates[0],
                        "is_new": False, "candidates": candidates[:3]})
    return jsonify({"matched": False, "is_new": True,
                    "candidates": candidates[:3],
                    "suggested_agency": _guess_agency(q)})

def _guess_agency(institution_name):
    """Guess agency from institution name for new customers."""
    upper = institution_name.upper()
    if any(kw in upper for kw in ("CCHCS", "HEALTH CARE SERVICE")):
        return "CCHCS"
    if any(kw in upper for kw in ("CALVET", "CAL VET", "VETERAN")):
        return "CalVet"
    if any(kw in upper for kw in ("STATE HOSPITAL", "DSH")):
        return "DSH"
    if any(kw in upper for kw in ("DGS", "GENERAL SERVICE")):
        return "DGS"
    # CDCR patterns
    cdcr_kw = ("CDCR", "CORRECTION", "STATE PRISON", "CONSERVATION CENTER",
               "INSTITUTION FOR", "FOLSOM", "PELICAN", "SAN QUENTIN", "CORCORAN")
    cdcr_pfx = ("CSP", "CIM", "CIW", "SCC", "CMC", "SATF", "CHCF", "PVSP",
                "KVSP", "LAC", "MCSP", "NKSP", "SAC", "WSP", "SOL", "FSP",
                "HDSP", "ISP", "CTF", "RJD", "CAL", "CEN", "ASP", "CCWF", "VSP")
    if any(kw in upper for kw in cdcr_kw):
        return "CDCR"
    for pfx in cdcr_pfx:
        if upper.startswith(pfx + "-") or upper.startswith(pfx + " ") or upper == pfx:
            return "CDCR"
    return "DEFAULT"

@bp.route("/api/quotes/history")
@auth_required
def api_quote_history():
    """Get quote history for an institution. Returns linked entities for UI."""
    institution = request.args.get("institution", "").strip()
    if not institution or not QUOTE_GEN_AVAILABLE:
        return jsonify([])
    quotes = get_all_quotes()
    inst_upper = institution.upper()
    matches = []
    for qt in reversed(quotes):
        qt_inst = qt.get("institution", "").upper()
        if inst_upper in qt_inst or qt_inst in inst_upper:
            source_pc = qt.get("source_pc_id", "")
            source_rfq = qt.get("source_rfq_id", "")
            
            # Compute days since creation for age display
            created = qt.get("created_at", "")
            days_ago = ""
            if created:
                try:
                    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    delta = datetime.now() - created_dt.replace(tzinfo=None)
                    days_ago = f"{delta.days}d ago" if delta.days > 0 else "today"
                except Exception:
                    pass

            matches.append({
                "quote_number": qt.get("quote_number"),
                "date": qt.get("date"),
                "total": qt.get("total", 0),
                "items_count": qt.get("items_count", 0),
                "status": qt.get("status", "pending"),
                "po_number": qt.get("po_number", ""),
                "items_text": qt.get("items_text", ""),
                "items_detail": qt.get("items_detail", []),
                "days_ago": days_ago,
                # Links for UI navigation
                "source_pc_id": source_pc,
                "source_pc_url": f"/pricecheck/{source_pc}" if source_pc else "",
                "source_rfq_id": source_rfq,
                "source_rfq_url": f"/rfq/{source_rfq}" if source_rfq else "",
                "quote_url": f"/quotes?q={qt.get('quote_number', '')}",
                # Lifecycle
                "status_history": qt.get("status_history", []),
            })
            if len(matches) >= 10:
                break
    return jsonify(matches)

# ═══════════════════════════════════════════════════════════════════════
# Product Research API (v6.1 — Phase 6)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/research/test")
@auth_required
def api_research_test():
    """Test Amazon search — ?q=nitrile+gloves"""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    q = request.args.get("q", "nitrile exam gloves")
    return jsonify(test_amazon_search(q))


@bp.route("/api/research/lookup")
@auth_required
def api_research_lookup():
    """Quick product lookup — ?q=stryker+restraint+package"""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    q = request.args.get("q", "")
    if not q:
        return jsonify({"error": "Provide ?q=search+terms"}), 400
    return jsonify(quick_lookup(q))


@bp.route("/api/research/rfq/<rid>")
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


@bp.route("/api/research/status")
@auth_required
def api_research_status():
    """Check progress of RFQ product research."""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    return jsonify(RESEARCH_STATUS)


@bp.route("/api/research/cache-stats")
@auth_required
def api_research_cache_stats():
    """Get product research cache statistics."""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    return jsonify(get_research_cache_stats())


@bp.route("/api/debug/env-check")
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


@bp.route("/api/config/set-serpapi-key", methods=["GET", "POST"])
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


@bp.route("/api/config/check-serpapi-key")
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

@bp.route("/api/pricecheck/parse", methods=["POST"])
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
    log.info("Price check parse: %s", f.filename)
    try:
        result = test_parse(tmp)
        result["uploaded_file"] = tmp
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/pricecheck/process", methods=["POST"])
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
        log.info("Price check process pipeline started: %s", pdf_path)
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


@bp.route("/api/pricecheck/download/<filename>")
@auth_required
def api_pricecheck_download(filename):
    """Download a completed Price Check PDF."""
    safe = os.path.basename(filename)
    path = os.path.join(DATA_DIR, safe)
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404
    return send_file(path, as_attachment=True, download_name=safe)


@bp.route("/api/pricecheck/test-parse")
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

@bp.route("/api/tax-rate")
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
        except Exception:
            pass
    # Default CA rate — state govt PCs are typically tax-exempt anyway
    return jsonify({
        "rate": 0.0725,
        "jurisdiction": "CA Default",
        "note": "State government purchases are typically tax-exempt. Toggle is OFF by default for 704 PCs.",
    })


@bp.route("/api/health")
@auth_required
def api_health():
    """Comprehensive system health check with path validation."""
    health = {"status": "ok", "checks": {}}
    
    # Path validation
    try:
        from src.core.paths import validate_paths
        path_check = validate_paths()
        health["checks"]["paths"] = {
            "ok": path_check["ok"],
            "errors": path_check["errors"],
            "warnings": path_check["warnings"],
        }
        if not path_check["ok"]:
            health["status"] = "degraded"
    except Exception as e:
        health["checks"]["paths"] = {"ok": False, "error": str(e)}
    
    # Data file checks
    data_checks = {}
    for name, path in [("customers", os.path.join(DATA_DIR, "customers.json")),
                       ("quotes_log", os.path.join(DATA_DIR, "quotes_log.json")),
                       ("quote_counter", os.path.join(DATA_DIR, "quote_counter.json"))]:
        if os.path.exists(path):
            try:
                with open(path) as f:
                    d = json.load(f)
                data_checks[name] = {"ok": True, "records": len(d) if isinstance(d, (list, dict)) else "?"}
            except Exception as e:
                data_checks[name] = {"ok": False, "error": str(e)}
                health["status"] = "degraded"
        else:
            data_checks[name] = {"ok": False, "error": "not found"}
    health["checks"]["data_files"] = data_checks
    
    # Module availability
    health["checks"]["modules"] = {
        "quote_generator": QUOTE_GEN_AVAILABLE,
        "price_check": PRICE_CHECK_AVAILABLE,
        "auto_processor": AUTO_PROCESSOR_AVAILABLE,
        "email_poller": bool(EmailPoller),
    }
    
    # Auto-processor health
    if AUTO_PROCESSOR_AVAILABLE:
        try:
            health["checks"]["auto_processor"] = system_health_check()
        except Exception:
            pass
    
    return jsonify(health)


@bp.route("/api/audit-stats")
@auth_required
def api_audit_stats():
    """Processing statistics from audit log."""
    if not AUTO_PROCESSOR_AVAILABLE:
        return jsonify({"error": "auto_processor.py not available"}), 503
    return jsonify(get_audit_stats())


@bp.route("/api/auto-process/pc", methods=["POST"])
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
    log.info("Auto-process started for %s", f.filename)
    result = auto_process_price_check(pdf_path)
    log.info("Auto-process complete for %s: status=%s", f.filename, result.get("status", "unknown"))
    return jsonify(json.loads(json.dumps(result, default=str)))


@bp.route("/api/detect-type", methods=["POST"])
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


@bp.route("/pricecheck/<pcid>/auto-process")
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
        _transition_status(pc, "completed", actor="auto", notes="Auto-processed")
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

def start_polling(app=None):
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

@bp.route("/settings/upload-logo", methods=["POST"])
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


@bp.route("/api/logo")
@auth_required
def serve_logo():
    """Serve the uploaded Reytech logo."""
    for ext in ("png", "jpg", "jpeg", "gif"):
        path = os.path.join(DATA_DIR, f"reytech_logo.{ext}")
        if os.path.exists(path):
            return send_file(path)
    return "", 404


@bp.route("/quotes/<quote_number>/status", methods=["POST"])
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


@bp.route("/quotes")
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
        "won":     ("✅ Won",     "#3fb950", "rgba(52,211,153,.08)"),
        "lost":    ("❌ Lost",    "#f85149", "rgba(248,113,113,.08)"),
        "pending": ("⏳ Pending", "#d29922", "rgba(210,153,34,.08)"),
        "draft":   ("📝 Draft",   "#8b949e", "rgba(139,148,160,.08)"),
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

        rows_html += f"""<tr data-qn="{qn}" style="{'opacity:0.6' if st in ('won','lost') else ''}">
         <td style="font-family:'JetBrains Mono',monospace;font-weight:700">{qn}</td>
         <td>{qt.get('date','')}</td>
         <td>{qt.get('agency','')}</td>
         <td style="max-width:260px;word-wrap:break-word;white-space:normal">{qt.get('institution','')}</td>
         <td>{qt.get('rfq_number','')}</td>
         <td style="text-align:right;font-weight:600;font-family:'JetBrains Mono',monospace">${qt.get('total',0):,.2f}</td>
         <td style="text-align:center">{toggle}</td>
         <td style="text-align:center">
          <span style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;color:{color};background:{bg}">{lbl}</span>{po_html}
         </td>
         <td style="text-align:center;white-space:nowrap">
          {"<span style=\"font-size:11px;color:#8b949e;padding:2px 6px\">decided</span>" if st in ("won","lost") else f"<button onclick=\"markQuote('{qn}','won')\" class=\"btn btn-sm\" style=\"background:rgba(52,211,153,.15);color:#3fb950;border:1px solid rgba(52,211,153,.3);padding:2px 6px;font-size:11px;cursor:pointer\" title=\"Mark Won\">✅</button><button onclick=\"markQuote('{qn}','lost')\" class=\"btn btn-sm\" style=\"background:rgba(248,113,113,.15);color:#f85149;border:1px solid rgba(248,113,113,.3);padding:2px 6px;font-size:11px;cursor:pointer\" title=\"Mark Lost\">❌</button>"}
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

    return render(build_quotes_page_content(
        stats_html=stats_html, q=q, agency_filter=agency_filter,
        status_filter=status_filter, logo_exists=logo_exists, rows_html=rows_html
    ), title="Quotes Database")


# ═══════════════════════════════════════════════════════════════════════
# Test Mode — QA/QC Infrastructure (Phase 12 Ready)
# ═══════════════════════════════════════════════════════════════════════

# Standard test fixture — realistic PC that exercises the full pipeline
TEST_PC_FIXTURE = {
    "header": {
        "institution": "CSP-Sacramento (TEST)",
        "requestor": "QA Tester",
        "phone": "916-555-0100",
    },
    "ship_to": "CSP-Sacramento, 100 Prison Road, Represa, CA 95671",
    "pc_number": "TEST-001",
    "due_date": "",  # Will be set to 30 days from now
    "items": [
        {"item_number": "1", "description": "Nitrile Exam Gloves, Medium, Box/100",
         "qty": 50, "uom": "BX", "pricing": {}},
        {"item_number": "2", "description": "Hand Sanitizer, 8oz Pump Bottle",
         "qty": 100, "uom": "EA", "pricing": {}},
        {"item_number": "3", "description": "Stryker Patient Restraint Package, Standard",
         "qty": 10, "uom": "KT", "pricing": {}},
    ],
}


@bp.route("/api/test/create-pc")
@auth_required
def api_test_create_pc():
    """Create a test Price Check with fixture data. Flagged as is_test=True."""
    from copy import deepcopy
    import uuid

    fixture = deepcopy(TEST_PC_FIXTURE)
    pc_id = f"test_{uuid.uuid4().hex[:8]}"
    now = datetime.now()
    fixture["due_date"] = (now + timedelta(days=30)).strftime("%m/%d/%Y")

    # Auto-assign draft quote number
    draft_qn = ""
    if QUOTE_GEN_AVAILABLE:
        try:
            draft_qn = peek_next_quote_number()
        except Exception:
            pass

    pcs = _load_price_checks()
    pc_record = {
        "id": pc_id,
        "pc_number": fixture["pc_number"],
        "institution": fixture["header"]["institution"],
        "due_date": fixture["due_date"],
        "requestor": fixture["header"]["requestor"],
        "ship_to": fixture["ship_to"],
        "items": fixture["items"],
        "source_pdf": "",
        "status": "parsed",
        "status_history": [{"from": "", "to": "parsed", "timestamp": now.isoformat(), "actor": "test"}],
        "created_at": now.isoformat(),
        "parsed": fixture,
        "reytech_quote_number": draft_qn,
        "is_test": True,
    }
    pcs[pc_id] = pc_record
    _save_price_checks(pcs)
    log.info("TEST: Created test PC %s (%s)", pc_id, fixture["pc_number"])
    return jsonify({"ok": True, "pc_id": pc_id, "url": f"/pricecheck/{pc_id}",
                    "message": f"Test PC created: {fixture['pc_number']} with {len(fixture['items'])} items"})


@bp.route("/api/test/cleanup")
@auth_required
def api_test_cleanup():
    """Remove all test records and optionally reset quote counter."""
    reset_counter = request.args.get("reset_counter", "false").lower() == "true"

    # Clean PCs
    pcs = _load_price_checks()
    test_pcs = [k for k, v in pcs.items() if v.get("is_test")]
    for k in test_pcs:
        del pcs[k]
    _save_price_checks(pcs)

    # Clean RFQs
    rfqs = load_rfqs()
    test_rfqs = [k for k, v in rfqs.items() if v.get("is_test")]
    for k in test_rfqs:
        del rfqs[k]
    if test_rfqs:
        save_rfqs(rfqs)

    # Clean quotes
    test_quotes = 0
    if QUOTE_GEN_AVAILABLE:
        quotes = get_all_quotes()
        original_len = len(quotes)
        clean_quotes = [q for q in quotes if not q.get("source_pc_id", "").startswith("test_")]
        test_quotes = original_len - len(clean_quotes)
        if test_quotes > 0:
            # Use quote_generator's save
            from src.forms.quote_generator import _save_all_quotes
            _save_all_quotes(clean_quotes)

    # Reset quote counter
    counter_reset = ""
    if reset_counter and QUOTE_GEN_AVAILABLE:
        # Find highest non-test quote number
        quotes = get_all_quotes()
        if quotes:
            nums = [q.get("quote_number", "") for q in quotes]
            # Parse R26Q15 → 15
            max_n = 0
            for n in nums:
                try:
                    max_n = max(max_n, int(n.split("Q")[-1]))
                except Exception:
                    pass
            set_quote_counter(max_n)
            counter_reset = f"Counter reset to {max_n}"
        else:
            set_quote_counter(0)
            counter_reset = "Counter reset to 0"

    log.info("TEST CLEANUP: %d PCs, %d RFQs, %d quotes removed. %s",
             len(test_pcs), len(test_rfqs), test_quotes, counter_reset)
    return jsonify({
        "ok": True,
        "removed": {"pcs": len(test_pcs), "rfqs": len(test_rfqs), "quotes": test_quotes},
        "counter_reset": counter_reset,
        "message": f"Cleaned {len(test_pcs)} test PCs, {len(test_rfqs)} RFQs, {test_quotes} quotes. {counter_reset}",
    })


@bp.route("/api/test/status")
@auth_required
def api_test_status():
    """Show current test data in the system."""
    pcs = _load_price_checks()
    test_pcs = {k: {"pc_number": v.get("pc_number"), "status": v.get("status"), "institution": v.get("institution")}
                for k, v in pcs.items() if v.get("is_test")}
    test_quotes = []
    if QUOTE_GEN_AVAILABLE:
        for q in get_all_quotes():
            if q.get("source_pc_id", "").startswith("test_"):
                test_quotes.append({"quote_number": q.get("quote_number"), "total": q.get("total", 0)})
    return jsonify({
        "test_pcs": test_pcs,
        "test_quotes": test_quotes,
        "counts": {"pcs": len(test_pcs), "quotes": len(test_quotes)},
    })


@bp.route("/api/test/cleanup-duplicates")
@auth_required
def api_cleanup_duplicates():
    """ONE-TIME: Deduplicate quotes_log.json and reset counter.

    What it does:
      1. Backs up quotes_log.json → quotes_log_backup_{timestamp}.json
      2. Deduplicates: keeps only the LAST entry per quote number
      3. Resets quote_counter.json to highest quote number + 1
      4. Returns full before/after report

    Safe to run multiple times — idempotent after first run.
    Hit: /api/test/cleanup-duplicates?dry_run=true to preview without writing.
    """
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "quote_generator not available"})

    dry_run = request.args.get("dry_run", "false").lower() == "true"
    quotes = get_all_quotes()
    original_count = len(quotes)

    # Deduplicate: walk forward, keep last occurrence of each quote number
    seen = {}
    for i, q in enumerate(quotes):
        qn = q.get("quote_number", "")
        if qn:
            seen[qn] = i  # last index wins

    # Build clean list preserving order of last occurrence
    clean = []
    used_indices = set(seen.values())
    for i in sorted(used_indices):
        clean.append(quotes[i])

    removed = original_count - len(clean)

    # Find highest quote number for counter reset
    max_num = 0
    for q in clean:
        qn = q.get("quote_number", "")
        try:
            n = int(qn.split("Q")[-1])
            max_num = max(max_num, n)
        except (ValueError, IndexError):
            pass

    # Build report
    from collections import Counter
    old_counts = Counter(q.get("quote_number", "") for q in quotes)
    dupes = {k: v for k, v in old_counts.items() if v > 1}

    report = {
        "dry_run": dry_run,
        "before": {"total_entries": original_count, "unique_quotes": len(seen)},
        "after": {"total_entries": len(clean), "unique_quotes": len(seen)},
        "removed": removed,
        "duplicates_found": dupes,
        "counter_will_be": max_num,
        "next_quote": f"R{str(datetime.now().year)[2:]}Q{max_num + 1}",
        "clean_quotes": [
            {"quote_number": q.get("quote_number"), "total": q.get("total", 0),
             "institution": q.get("institution", "")[:40], "status": q.get("status", "")}
            for q in clean
        ],
    }

    if not dry_run:
        # Backup
        backup_name = f"quotes_log_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        backup_path = os.path.join(DATA_DIR, backup_name)
        import shutil
        src_path = os.path.join(DATA_DIR, "quotes_log.json")
        if os.path.exists(src_path):
            shutil.copy2(src_path, backup_path)
            report["backup"] = backup_name

        # Write clean data
        from src.forms.quote_generator import _save_all_quotes
        _save_all_quotes(clean)

        # Reset counter
        set_quote_counter(max_num)

        log.info("CLEANUP: %d → %d quotes (%d duplicates removed). Counter → %d. Backup: %s",
                 original_count, len(clean), removed, max_num, backup_name)
        report["message"] = f"Done. {removed} duplicates removed. Counter reset to {max_num}. Backup: {backup_name}"
    else:
        report["message"] = f"DRY RUN: Would remove {removed} duplicates and reset counter to {max_num}. Add ?dry_run=false to execute."

    return jsonify(report)


@bp.route("/api/test/renumber-quote")
@auth_required
def api_renumber_quote():
    """Renumber a quote. Usage: ?old=R26Q1&new=R26Q16
    
    Also updates any PC that references the old quote number.
    """
    old = request.args.get("old", "").strip()
    new = request.args.get("new", "").strip()
    dry_run = request.args.get("dry_run", "false").lower() == "true"

    if not old or not new:
        return jsonify({"ok": False, "error": "Provide ?old=R26Q1&new=R26Q16"})

    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "quote_generator not available"})

    quotes = get_all_quotes()
    found = False
    for q in quotes:
        if q.get("quote_number") == old:
            if not dry_run:
                q["quote_number"] = new
                q["renumbered_from"] = old
                q["renumbered_at"] = datetime.now().isoformat()
            found = True
            break

    if not found:
        return jsonify({"ok": False, "error": f"Quote {old} not found"})

    # Update linked PCs
    pc_updated = ""
    pcs = _load_price_checks()
    for pid, pc in pcs.items():
        if pc.get("reytech_quote_number") == old:
            if not dry_run:
                pc["reytech_quote_number"] = new
            pc_updated = pid

    # Update counter if new number is higher
    try:
        new_num = int(new.split("Q")[-1])
    except (ValueError, IndexError):
        new_num = 0

    if not dry_run:
        from src.forms.quote_generator import _save_all_quotes
        _save_all_quotes(quotes)
        if pc_updated:
            _save_price_checks(pcs)
        if new_num > 0:
            set_quote_counter(new_num)
        log.info("RENUMBER: %s → %s (PC: %s, counter: %d)", old, new, pc_updated or "none", new_num)

    return jsonify({
        "ok": True,
        "dry_run": dry_run,
        "old": old,
        "new": new,
        "pc_updated": pc_updated or None,
        "counter_set_to": new_num,
        "next_quote": f"R{str(datetime.now().year)[2:]}Q{new_num + 1}",
        "message": f"{'DRY RUN: Would renumber' if dry_run else 'Renumbered'} {old} → {new}",
    })


@bp.route("/api/test/delete-quotes")
@auth_required
def api_delete_quotes():
    """Delete specific quotes by number. Usage: ?numbers=R26Q2,R26Q3,R26Q4

    Backs up before deleting. Also cleans linked PCs.
    """
    numbers_str = request.args.get("numbers", "").strip()
    dry_run = request.args.get("dry_run", "false").lower() == "true"

    if not numbers_str:
        return jsonify({"ok": False, "error": "Provide ?numbers=R26Q2,R26Q3,R26Q4"})

    to_delete = set(n.strip() for n in numbers_str.split(",") if n.strip())

    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "quote_generator not available"})

    quotes = get_all_quotes()
    original_count = len(quotes)
    deleted = []
    kept = []

    for q in quotes:
        qn = q.get("quote_number", "")
        if qn in to_delete:
            deleted.append({"quote_number": qn, "total": q.get("total", 0),
                           "institution": q.get("institution", "")})
        else:
            kept.append(q)

    # Clean linked PCs
    pcs_cleaned = []
    pcs = _load_price_checks()
    for pid, pc in pcs.items():
        if pc.get("reytech_quote_number") in to_delete:
            if not dry_run:
                pc["reytech_quote_number"] = ""
                pc["reytech_quote_pdf"] = ""
                _transition_status(pc, "parsed", actor="cleanup", notes=f"Quote deleted")
            pcs_cleaned.append(pid)

    if not dry_run and deleted:
        # Backup
        backup_name = f"quotes_log_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        import shutil
        src_path = os.path.join(DATA_DIR, "quotes_log.json")
        if os.path.exists(src_path):
            shutil.copy2(src_path, os.path.join(DATA_DIR, backup_name))
        from src.forms.quote_generator import _save_all_quotes
        _save_all_quotes(kept)
        if pcs_cleaned:
            _save_price_checks(pcs)
        log.info("DELETE QUOTES: %s removed (%d → %d). PCs cleaned: %s",
                 [d["quote_number"] for d in deleted], original_count, len(kept), pcs_cleaned)

    return jsonify({
        "ok": True,
        "dry_run": dry_run,
        "deleted": deleted,
        "remaining": len(kept),
        "pcs_cleaned": pcs_cleaned,
        "message": f"{'DRY RUN: Would delete' if dry_run else 'Deleted'} {len(deleted)} quotes: {[d['quote_number'] for d in deleted]}",
    })


# Start polling on import (for gunicorn) and on direct run
start_polling()
