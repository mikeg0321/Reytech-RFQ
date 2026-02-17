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
                                  get_quote_stats, set_quote_counter,
                                  _detect_agency)
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

def _pst_now_iso():
    """Return current PST datetime as ISO string (JS-parseable with time)."""
    pst = timezone(timedelta(hours=-8))
    return datetime.now(pst).isoformat()

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
        # Rate limit auth attempts
        auth_key = f"auth:{request.remote_addr}"
        if not _check_rate_limit(auth_key, RATE_LIMIT_AUTH_MAX):
            return Response("Rate limited — too many auth attempts", 429)
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return Response(
                "🔒 Reytech RFQ Dashboard — Login Required",
                401, {"WWW-Authenticate": 'Basic realm="Reytech RFQ Dashboard"'})
        # Rate limit all authenticated requests
        if not _check_rate_limit():
            return Response("Rate limited — slow down", 429)
        return f(*args, **kwargs)
    return decorated

# ═══════════════════════════════════════════════════════════════════════
# Security: Rate Limiting + Input Sanitization (Phase 22)
# ═══════════════════════════════════════════════════════════════════════

_rate_limiter = {}  # {ip: [timestamps]}
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 120    # requests per window (generous for normal use)
RATE_LIMIT_AUTH_MAX = 10  # auth attempts per window

def _check_rate_limit(key: str = None, max_requests: int = None) -> bool:
    """Check if request is within rate limits. Returns True if OK."""
    import time as _time
    key = key or request.remote_addr or "unknown"
    max_req = max_requests or RATE_LIMIT_MAX
    now = _time.time()
    window = _rate_limiter.get(key, [])
    window = [t for t in window if now - t < RATE_LIMIT_WINDOW]
    if len(window) >= max_req:
        return False
    window.append(now)
    _rate_limiter[key] = window
    # Cleanup old keys periodically
    if len(_rate_limiter) > 1000:
        cutoff = now - RATE_LIMIT_WINDOW * 2
        _rate_limiter.clear()
    return True

def _sanitize_input(value: str, max_length: int = 500, allow_html: bool = False) -> str:
    """Sanitize user input — strip dangerous characters."""
    if not isinstance(value, str):
        return str(value)[:max_length] if value else ""
    value = value[:max_length]
    if not allow_html:
        # Strip HTML tags
        value = re.sub(r'<[^>]+>', '', value)
        # Strip path traversal
        value = value.replace('../', '').replace('..\\', '')
        value = value.replace('\x00', '')  # null bytes
    return value.strip()

def _sanitize_path(path_str: str) -> str:
    """Sanitize file path input — prevent traversal attacks."""
    if not path_str:
        return ""
    # Resolve to prevent traversal
    clean = os.path.basename(path_str)
    # Only allow safe characters
    clean = re.sub(r'[^\w\-.]', '_', clean)
    return clean

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
            POLL_STATUS["last_check"] = _pst_now_iso()
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
# CRM Activity Log — Phase 16
# ═══════════════════════════════════════════════════════════════════════

CRM_LOG_FILE = os.path.join(DATA_DIR, "crm_activity.json")

def _load_crm_activity() -> list:
    try:
        with open(CRM_LOG_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def _save_crm_activity(activity: list):
    os.makedirs(DATA_DIR, exist_ok=True)
    if len(activity) > 5000:
        activity = activity[-5000:]
    with open(CRM_LOG_FILE, "w") as f:
        json.dump(activity, f, indent=2, default=str)

def _log_crm_activity(ref_id: str, event_type: str, description: str,
                       actor: str = "system", metadata: dict = None):
    """Log a CRM activity event.
    
    event_types: quote_won, quote_lost, quote_sent, quote_generated,
                 qb_po_created, email_sent, email_received, voice_call,
                 scprs_lookup, price_check, lead_scored, follow_up
    """
    activity = _load_crm_activity()
    activity.append({
        "id": f"crm-{datetime.now().strftime('%Y%m%d%H%M%S')}-{len(activity)}",
        "ref_id": ref_id,
        "event_type": event_type,
        "description": description,
        "actor": actor,
        "timestamp": datetime.now().isoformat(),
        "metadata": metadata or {},
    })
    _save_crm_activity(activity)

def _get_crm_activity(ref_id: str = None, event_type: str = None,
                       institution: str = None, limit: int = 50) -> list:
    """Get CRM activity, optionally filtered."""
    activity = _load_crm_activity()
    results = []
    for a in reversed(activity):
        if ref_id and ref_id != a.get("ref_id"):
            continue
        if event_type and event_type != a.get("event_type"):
            continue
        if institution:
            meta = a.get("metadata", {})
            if institution.lower() not in (
                meta.get("institution", "").lower() +
                meta.get("agency", "").lower() +
                a.get("description", "").lower()
            ):
                continue
        results.append(a)
        if len(results) >= limit:
            break
    return results

def _find_quote(quote_number: str) -> dict:
    """Find a single quote by number."""
    for qt in get_all_quotes():
        if qt.get("quote_number") == quote_number:
            return qt
    return None

# ═══════════════════════════════════════════════════════════════════════
# Order Management System — Phase 17
# ═══════════════════════════════════════════════════════════════════════

ORDERS_FILE = os.path.join(DATA_DIR, "orders.json")

def _load_orders() -> dict:
    try:
        with open(ORDERS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_orders(orders: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(ORDERS_FILE, "w") as f:
        json.dump(orders, f, indent=2, default=str)

def _create_order_from_quote(qt: dict, po_number: str = "") -> dict:
    """Create an order when a quote is won."""
    qn = qt.get("quote_number", "")
    oid = f"ORD-{qn}" if qn else f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    line_items = []
    for i, it in enumerate(qt.get("items_detail", [])):
        pn = it.get("part_number", "")
        supplier_url = ""
        if pn and pn.startswith("B0"):
            supplier_url = f"https://amazon.com/dp/{pn}"
        line_items.append({
            "line_id": f"L{i+1:03d}",
            "description": it.get("description", ""),
            "part_number": pn,
            "qty": it.get("qty", 0),
            "unit_price": it.get("unit_price", 0),
            "extended": round(it.get("qty", 0) * it.get("unit_price", 0), 2),
            "supplier": it.get("supplier", "Amazon") if pn and pn.startswith("B0") else "",
            "supplier_url": supplier_url,
            "sourcing_status": "pending",    # pending → ordered → shipped → delivered
            "tracking_number": "",
            "carrier": "",
            "ship_date": "",
            "delivery_date": "",
            "invoice_status": "pending",     # pending → partial → invoiced
            "invoice_number": "",
            "notes": "",
        })

    order = {
        "order_id": oid,
        "quote_number": qn,
        "po_number": po_number,
        "agency": qt.get("agency", ""),
        "institution": qt.get("institution", "") or qt.get("ship_to_name", ""),
        "ship_to_name": qt.get("ship_to_name", ""),
        "ship_to_address": qt.get("ship_to_address", []),
        "total": qt.get("total", 0),
        "subtotal": qt.get("subtotal", 0),
        "tax": qt.get("tax", 0),
        "line_items": line_items,
        "status": "new",  # new → sourcing → shipped → partial_delivery → delivered → invoiced → closed
        "invoice_type": "",  # partial or full
        "invoice_total": 0,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "status_history": [{"status": "new", "timestamp": datetime.now().isoformat(), "actor": "system"}],
    }

    orders = _load_orders()
    orders[oid] = order
    _save_orders(orders)
    _log_crm_activity(qn, "order_created",
                      f"Order {oid} created from quote {qn} — ${qt.get('total',0):,.2f}",
                      actor="system", metadata={"order_id": oid, "institution": order["institution"]})
    return order

def _update_order_status(oid: str):
    """Auto-calculate order status from line item statuses."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return
    items = order.get("line_items", [])
    if not items:
        return
    statuses = [it.get("sourcing_status", "pending") for it in items]
    inv_statuses = [it.get("invoice_status", "pending") for it in items]

    if all(s == "delivered" for s in statuses):
        if all(s == "invoiced" for s in inv_statuses):
            order["status"] = "closed"
        elif any(s == "invoiced" for s in inv_statuses):
            order["status"] = "invoiced"
        else:
            order["status"] = "delivered"
    elif any(s == "delivered" for s in statuses):
        order["status"] = "partial_delivery"
    elif any(s == "shipped" for s in statuses):
        order["status"] = "shipped"
    elif any(s == "ordered" for s in statuses):
        order["status"] = "sourcing"
    else:
        order["status"] = "new"

    order["updated_at"] = datetime.now().isoformat()
    orders[oid] = order
    _save_orders(orders)

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
    _poll_running = POLL_STATUS.get("running", False)
    _poll_last = POLL_STATUS.get("last_check", "")
    _poll_status = "Polling" if _poll_running else ("Email not configured" if not _has_email else "Starting...")
    _poll_class = "poll-on" if _poll_running else ("poll-off" if not _has_email else "poll-wait")
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Reytech RFQ</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>{BASE_CSS}</style></head><body>
<div class="hdr">
 <div style="display:flex;align-items:center;gap:14px">
  <a href="/" style="display:flex;align-items:center;gap:10px;text-decoration:none">
   <img src="/api/logo" alt="Reytech" style="height:44px;background:#fff;padding:6px 12px;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.2)" onerror="this.outerHTML='<span style=\\'font-size:20px;font-weight:700;color:var(--ac)\\'>Reytech</span>'">
   <span style="font-size:17px;font-weight:600;color:var(--tx);letter-spacing:-0.3px">RFQ Dashboard</span>
  </a>
 </div>
 <div style="display:flex;align-items:center;gap:6px">
  <a href="/" class="hdr-btn hdr-active">🏠 Home</a>
  <a href="/quotes" class="hdr-btn">📋 Quotes</a>
  <a href="/orders" class="hdr-btn">📦 Orders</a>
  <a href="/campaigns" class="hdr-btn">📞 Campaigns</a>
  <a href="/pipeline" class="hdr-btn">🔄 Pipeline</a>
  <a href="/agents" class="hdr-btn">🤖 Agents</a>
  <span style="width:1px;height:24px;background:var(--bd);margin:0 6px"></span>
  <button class="hdr-btn" onclick="pollNow(this)" id="poll-btn">⚡ Check Now</button>
  <button class="hdr-btn hdr-warn" onclick="resyncAll(this)" title="Clear queue & re-import all emails">🔄 Resync</button>
  <span style="width:1px;height:24px;background:var(--bd);margin:0 6px"></span>
  <div class="hdr-status">
   <div style="display:flex;align-items:center;gap:6px">
    <span class="poll-dot {_poll_class}"></span>
    <span>{_poll_status}</span>
   </div>
   <div class="hdr-time" id="poll-time" data-utc="{_poll_last}">{_poll_last or 'never'}</div>
  </div>
 </div>
</div>
<div class="ctr">
{{% with messages = get_flashed_messages(with_categories=true) %}}
 {{% for cat, msg in messages %}}<div class="alert al-{{'s' if cat=='success' else 'e' if cat=='error' else 'i'}}">{{% if cat=='success' %}}✅{{% elif cat=='error' %}}❌{{% else %}}ℹ️{{% endif %}} {{{{msg}}}}</div>{{% endfor %}}
{{% endwith %}}
""" + content + """
<script>
function _updatePollTime(ts){
 var el=document.getElementById('poll-time');
 if(el&&ts){el.dataset.utc=ts;try{var d=new Date(ts);if(!isNaN(d)){el.textContent=d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true})}}catch(e){el.textContent=ts}}
}
function pollNow(btn){
 btn.disabled=true;btn.textContent='Checking...';
 fetch('/api/poll-now',{credentials:'same-origin'}).then(r=>r.json()).then(d=>{
  _updatePollTime(d.last_check);
  if(d.found>0){btn.textContent=d.found+' found!';setTimeout(()=>location.reload(),800)}
  else{btn.textContent='No new emails';setTimeout(()=>{btn.textContent='⚡ Check Now';btn.disabled=false},2000)}
 }).catch(()=>{btn.textContent='Error';setTimeout(()=>{btn.textContent='⚡ Check Now';btn.disabled=false},2000)});
}
function resyncAll(btn){
 if(!confirm('Clear all RFQs and re-import from email?'))return;
 btn.disabled=true;btn.textContent='🔄 Syncing...';
 fetch('/api/resync',{credentials:'same-origin'}).then(r=>r.json()).then(d=>{
  _updatePollTime(d.last_check);
  if(d.found>0){btn.textContent=d.found+' imported!';setTimeout(()=>location.reload(),800)}
  else{btn.textContent='0 found';setTimeout(()=>{btn.textContent='🔄 Resync';btn.disabled=false},2000)}
 }).catch(()=>{btn.textContent='Error';setTimeout(()=>{btn.textContent='🔄 Resync';btn.disabled=false},2000)});
}
// Convert poll time to local
(function(){
 var el=document.getElementById('poll-time');
 if(el && el.dataset.utc){
  try{
   var d=new Date(el.dataset.utc);
   if(!isNaN(d)){el.textContent=d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true})}
  }catch(e){}
 }
})();
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
    except Exception as e:
        log.debug("Suppressed: %s", e)
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
            # Detect agency from all available data
            ship_to_raw = parsed.get("ship_to", "") or ""
            ship_parts = [p.strip() for p in ship_to_raw.split(",") if p.strip()]
            detect_data = {
                "institution": institution,
                "ship_to": ship_to_raw,
                "ship_to_name": ship_parts[0] if ship_parts else "",
                "requestor": parsed.get("header", {}).get("requestor", ""),
            }
            detected_agency = _detect_agency(detect_data)

            # Save as draft in quotes log
            draft_entry = {
                "quote_number": draft_quote_num,
                "date": datetime.now().strftime("%b %d, %Y"),
                "agency": detected_agency if detected_agency != "DEFAULT" else "",
                "institution": institution or (ship_parts[0] if ship_parts else ""),
                "rfq_number": pc_num,
                "total": 0,
                "subtotal": 0,
                "tax": 0,
                "items_count": len(items),
                "pdf_path": "",
                "created_at": datetime.now().isoformat(),
                "status": "draft",
                "source_pc_id": pc_id,
                "ship_to_name": ship_parts[0] if ship_parts else "",
                "ship_to_address": ship_parts[1:] if len(ship_parts) > 1 else ship_parts,
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
        except Exception as e:
            log.debug("Suppressed: %s", e)
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
                except Exception as e:

                    log.debug("Suppressed: %s", e)
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
                except Exception as e:

                    log.debug("Suppressed: %s", e)
    
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
        # CRM: log
        _log_crm_activity(result.get("quote_number", ""), "quote_generated",
                          f"Quote {result.get('quote_number','')} generated from RFQ {sol} — ${result.get('total',0):,.2f}",
                          actor="user", metadata={"rfq_id": rid, "agency": result.get("agency","")})
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
        # CRM: log email sent + update quote status to sent
        qn = r.get("reytech_quote_number", "")
        if qn and QUOTE_GEN_AVAILABLE:
            update_quote_status(qn, "sent", actor="system")
            _log_crm_activity(qn, "email_sent",
                              f"Quote {qn} emailed to {r['draft_email'].get('to','')}",
                              actor="user", metadata={"to": r['draft_email'].get('to','')})
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
# Agent Control Panel (Phase 14)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/agents")
@auth_required
def agents_page():
    """Agent Control Panel — click buttons instead of writing API calls."""
    from src.api.templates import render_agents_page
    return render_agents_page()


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

    # 45-day expiry from TODAY (not upload date)
    try:
        expiry = datetime.now() + timedelta(days=45)
        expiry_date = expiry.strftime("%m/%d/%Y")
    except Exception as e:
        log.debug("Suppressed: %s", e)
        expiry_date = (datetime.now() + timedelta(days=45)).strftime("%m/%d/%Y")
    today_date = datetime.now().strftime("%m/%d/%Y")

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
        del_sel=del_sel, next_quote_preview=next_quote_preview,
        today_date=today_date
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


@bp.route("/pricecheck/<pcid>/rename", methods=["POST"])
@auth_required
def pricecheck_rename(pcid):
    """Rename a price check's display number."""
    pcs = _load_price_checks()
    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(silent=True) or {}
    new_name = data.get("pc_number", "").strip()
    if not new_name:
        return jsonify({"ok": False, "error": "Name cannot be empty"})
    pcs[pcid]["pc_number"] = new_name
    _save_price_checks(pcs)
    log.info("RENAME PC %s → %s", pcid, new_name)
    return jsonify({"ok": True, "pc_number": new_name})


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
        # CRM: log quote generation
        _log_crm_activity(result.get("quote_number", ""), "quote_generated",
                          f"Quote {result.get('quote_number','')} generated — ${result.get('total',0):,.2f} for {pc.get('institution','')}",
                          actor="user", metadata={"institution": pc.get("institution",""), "agency": result.get("agency","")})
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
        "last_check": POLL_STATUS.get("last_check"),
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
                    except Exception as e:
                        log.debug("Suppressed: %s", e)
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
                    except Exception as e:

                        log.debug("Suppressed: %s", e)
                
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
        except Exception as e:
            log.debug("Suppressed: %s", e)
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
                except Exception as e:
                    log.debug("Suppressed: %s", e)
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
            except Exception as e:
                log.debug("Suppressed: %s", e)
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
                except Exception as e:
                    log.debug("Suppressed: %s", e)
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
        except Exception as e:
            log.debug("Suppressed: %s", e)
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
        except Exception as e:
            log.debug("Suppressed: %s", e)
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
    """Mark a quote as won, lost, or pending. Triggers won workflow if applicable."""
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

    result = {"ok": True, "quote_number": quote_number, "status": new_status}

    # ── Won workflow: QB PO + CRM activity ──
    if new_status == "won":
        # Log CRM activity
        _log_crm_activity(quote_number, "quote_won",
                          f"Quote {quote_number} marked WON" + (f" — PO: {po_number}" if po_number else ""),
                          actor="user")

        # Attempt QB PO creation if configured
        if QB_AVAILABLE and qb_configured():
            try:
                qt = _find_quote(quote_number)
                if qt:
                    items_for_qb = []
                    for it in qt.get("items_detail", []):
                        items_for_qb.append({
                            "description": it.get("description", ""),
                            "qty": it.get("qty", 1),
                            "unit_cost": it.get("unit_price", 0),
                        })
                    if items_for_qb:
                        # Find or use default vendor
                        institution = qt.get("institution", "") or qt.get("ship_to_name", "")
                        vendor = find_vendor(institution) if institution else None
                        if vendor:
                            po_result = create_purchase_order(
                                vendor_id=vendor["qb_id"],
                                items=items_for_qb,
                                memo=f"Reytech Quote {quote_number}" + (f" / PO {po_number}" if po_number else ""),
                                ship_to=institution,
                            )
                            if po_result:
                                result["qb_po"] = po_result
                                _log_crm_activity(quote_number, "qb_po_created",
                                                  f"QB PO #{po_result.get('doc_number','')} created — ${po_result.get('total',0):,.2f}",
                                                  actor="system")
                            else:
                                result["qb_po_error"] = "PO creation failed"
                        else:
                            result["qb_vendor_missing"] = f"No QB vendor match for '{institution}'"
            except Exception as e:
                log.error("Won workflow QB step failed: %s", e)
                result["qb_error"] = str(e)

    elif new_status == "lost":
        _log_crm_activity(quote_number, "quote_lost",
                          f"Quote {quote_number} marked LOST" + (f" — {notes}" if notes else ""),
                          actor="user")
        # Log competitor intelligence
        if PREDICT_AVAILABLE:
            try:
                log_competitor_intel(quote_number, "lost", {"notes": notes or ""})
            except Exception as e:
                log.error("Competitor intel logging failed: %s", e)
        # Log competitor intelligence
        if PREDICT_AVAILABLE:
            try:
                ci = log_competitor_intel(quote_number, "lost",
                                          {"notes": notes, "competitor": request.get_json(silent=True).get("competitor", "")})
                result["competitor_intel"] = ci.get("id", "")
            except Exception as e:
                log.error("Competitor intel log failed: %s", e)

    # ── Create Order for won quotes ──
    if new_status == "won":
        try:
            qt = _find_quote(quote_number)
            if qt:
                order = _create_order_from_quote(qt, po_number=po_number)
                result["order_id"] = order["order_id"]
                result["order_url"] = f"/order/{order['order_id']}"
        except Exception as e:
            log.error("Order creation failed: %s", e)
            result["order_error"] = str(e)

    return jsonify(result)


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
        "sent":    ("📤 Sent",    "#58a6ff", "rgba(88,166,255,.08)"),
        "expired": ("⏰ Expired", "#8b949e", "rgba(139,148,160,.08)"),
    }

    rows_html = ""
    for qt in quotes:
        fname = os.path.basename(qt.get("pdf_path", ""))
        dl = f'<a href="/api/pricecheck/download/{fname}" title="Download PDF" style="font-size:14px">📥</a>' if fname else ""
        st = qt.get("status", "pending")

        # Derive institution from ship_to if empty/missing
        institution = qt.get("institution", "")
        if not institution or institution.strip() == "":
            ship_name = qt.get("ship_to_name", "")
            if ship_name:
                institution = ship_name
            else:
                # Try from items_text or rfq_number as last resort
                institution = qt.get("rfq_number", "") or "—"

        # Fix DEFAULT agency using ALL available data
        agency = qt.get("agency", "")
        if agency in ("DEFAULT", "", None) and QUOTE_GEN_AVAILABLE:
            try:
                agency = _detect_agency(qt)
            except Exception as e:
                log.debug("Suppressed: %s", e)
                agency = ""
        if agency == "DEFAULT":
            agency = ""

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

        # Quote number links to dedicated detail page
        qn_cell = f'<a href="/quote/{qn}" style="color:var(--ac);text-decoration:none;font-family:\'JetBrains Mono\',monospace;font-weight:700" title="View quote details">{qn}</a>'

        # Decided rows get subtle opacity
        row_style = "opacity:0.5" if st in ("won", "lost", "expired") else ""

        rows_html += f"""<tr data-qn="{qn}" style="{row_style}">
         <td>{qn_cell}</td>
         <td class="mono" style="white-space:nowrap">{qt.get('date','')}</td>
         <td>{agency}</td>
         <td style="max-width:300px;word-wrap:break-word;white-space:normal;font-weight:500">{institution}</td>
         <td class="mono">{qt.get('rfq_number','')}</td>
         <td style="text-align:right;font-weight:600;font-family:'JetBrains Mono',monospace">${qt.get('total',0):,.2f}</td>
         <td style="text-align:center">{toggle}</td>
         <td style="text-align:center">
          <span style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;color:{color};background:{bg}">{lbl}</span>{po_html}
         </td>
         <td style="text-align:center;white-space:nowrap">
          {"<a href=\"/order/ORD-" + qn + "\" style=\"font-size:11px;color:#3fb950;text-decoration:none;padding:2px 6px\" title=\"View order\">📦 Order</a>" if st == "won" else "<span style=\"font-size:11px;color:#8b949e;padding:2px 6px\">lost</span>" if st == "lost" else f"<button onclick=\"markQuote('{qn}','won')\" class=\"btn btn-sm\" style=\"background:rgba(52,211,153,.15);color:#3fb950;border:1px solid rgba(52,211,153,.3);padding:2px 6px;font-size:11px;cursor:pointer\" title=\"Mark Won\">✅</button><button onclick=\"markQuote('{qn}','lost')\" class=\"btn btn-sm\" style=\"background:rgba(248,113,113,.15);color:#f85149;border:1px solid rgba(248,113,113,.3);padding:2px 6px;font-size:11px;cursor:pointer\" title=\"Mark Lost\">❌</button>" if st not in ("expired",) else "<span style=\"font-size:11px;color:#8b949e\">expired</span>"}
          {dl}
         </td>
        </tr>
        <tr id="{detail_id}" style="display:none"><td colspan="9" style="background:var(--sf2);padding:8px 16px;border-left:3px solid var(--ac)">{detail_rows if detail_rows else '<span style="color:var(--tx2);font-size:11px">No item details available</span>'}</td></tr>"""

    # Win rate stats bar
    wr = stats.get("win_rate", 0)
    wr_color = "#3fb950" if wr >= 50 else ("#d29922" if wr >= 30 else "#f85149")
    expired_count = sum(1 for qt in quotes if qt.get("status") == "expired")
    stats_html = f"""
     <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;font-size:13px;font-family:'JetBrains Mono',monospace">
      <span><b>{stats['total']}</b> total</span>
      <span style="color:#3fb950"><b>{stats['won']}</b> won (${stats['won_total']:,.0f})</span>
      <span style="color:#f85149"><b>{stats['lost']}</b> lost</span>
      <span style="color:#d29922"><b>{stats['pending']}</b> pending</span>
      {f'<span style="color:#8b949e"><b>{expired_count}</b> expired</span>' if expired_count else ''}
      <span>WR: <b style="color:{wr_color}">{wr}%</b></span>
      <span style="color:#8b949e">Next: <b style="color:var(--tx)">{next_num}</b></span>
     </div>
    """

    return render(build_quotes_page_content(
        stats_html=stats_html, q=q, agency_filter=agency_filter,
        status_filter=status_filter, logo_exists=logo_exists, rows_html=rows_html
    ), title="Quotes Database")


@bp.route("/quote/<qn>")
def quote_detail(qn):
    """Dedicated quote detail page."""
    if not QUOTE_GEN_AVAILABLE:
        flash("Quote generator not available", "error")
        return redirect("/")
    quotes = get_all_quotes()
    qt = None
    for q in quotes:
        if q.get("quote_number") == qn:
            qt = q
            break
    if not qt:
        flash(f"Quote {qn} not found", "error")
        return redirect("/quotes")

    # Derive institution from ship_to if empty
    institution = qt.get("institution", "")
    if not institution or institution.strip() == "":
        institution = qt.get("ship_to_name", "") or qt.get("rfq_number", "") or "—"

    # Fix DEFAULT agency using all available data
    agency = qt.get("agency", "")
    if agency in ("DEFAULT", "", None):
        try:
            agency = _detect_agency(qt)
        except Exception:
            agency = ""
    if agency == "DEFAULT":
        agency = ""

    st = qt.get("status", "pending")
    fname = os.path.basename(qt.get("pdf_path", ""))
    items = qt.get("items_detail", [])
    source_link = ""
    source_label = ""
    if qt.get("source_pc_id"):
        source_link = f'/pricecheck/{qt["source_pc_id"]}'
        source_label = "Price Check"
    elif qt.get("source_rfq_id"):
        source_link = f'/rfq/{qt["source_rfq_id"]}'
        source_label = "RFQ"

    # Status config
    status_cfg = {
        "won":     ("✅ Won",     "var(--gn)", "rgba(52,211,153,.1)"),
        "lost":    ("❌ Lost",    "var(--rd)", "rgba(248,113,113,.1)"),
        "pending": ("⏳ Pending", "var(--yl)", "rgba(251,191,36,.1)"),
        "draft":   ("📝 Draft",   "var(--tx2)", "rgba(139,148,160,.1)"),
        "sent":    ("📤 Sent",    "var(--ac)", "rgba(79,140,255,.1)"),
        "expired": ("⏰ Expired", "var(--tx2)", "rgba(139,148,160,.1)"),
    }
    lbl, color, bg = status_cfg.get(st, status_cfg["pending"])

    # Items table rows
    items_html = ""
    for it in items:
        desc = str(it.get("description", ""))
        pn = it.get("part_number", "")
        pn_cell = f'<a href="https://amazon.com/dp/{pn}" target="_blank" style="color:var(--ac)">{pn}</a>' if pn and pn.startswith("B0") else (pn or "—")
        up = it.get("unit_price", 0)
        qty = it.get("qty", 0)
        items_html += f"""<tr>
         <td style="color:var(--tx2)">{it.get('line_number', '')}</td>
         <td style="max-width:400px;word-wrap:break-word;white-space:normal">{desc}</td>
         <td class="mono">{pn_cell}</td>
         <td class="mono" style="text-align:center">{qty}</td>
         <td class="mono" style="text-align:right">${up:,.2f}</td>
         <td class="mono" style="text-align:right;font-weight:600">${up*qty:,.2f}</td>
        </tr>"""

    # Status history
    history = qt.get("status_history", [])
    history_html = ""
    for h in reversed(history[-10:]):
        history_html += f'<div style="font-size:11px;color:var(--tx2);padding:3px 0"><span class="mono">{h.get("timestamp","")[:16]}</span> → <b>{h.get("status","")}</b>{" by " + h.get("actor","") if h.get("actor") else ""}{" (PO: " + h["po_number"] + ")" if h.get("po_number") else ""}</div>'

    # Build action buttons separately to avoid f-string escaping
    if st in ('pending', 'sent'):
        action_btns = '<div style="border-top:1px solid var(--bd);margin-top:14px;padding-top:14px;display:flex;gap:8px;justify-content:center">'
        action_btns += f'<button onclick="markQuote(&quot;{qn}&quot;,&quot;won&quot;)" class="btn btn-g" style="font-size:13px">✅ Mark Won</button>'
        action_btns += f'<button onclick="markQuote(&quot;{qn}&quot;,&quot;lost&quot;)" class="btn" style="background:rgba(248,113,113,.15);color:var(--rd);border:1px solid rgba(248,113,113,.3);font-size:13px">❌ Mark Lost</button>'
        action_btns += '</div>'
    else:
        action_btns = ""

    content = f"""
    <div style="display:flex;gap:10px;align-items:center;margin-bottom:16px">
     <a href="/quotes" class="btn btn-s" style="font-size:13px">← Quotes</a>
     {f'<a href="{source_link}" class="btn btn-s" style="font-size:13px">📎 {source_label}</a>' if source_link else ''}
     {f'<a href="/api/pricecheck/download/{fname}" class="btn btn-s" style="font-size:13px">📥 Download PDF</a>' if fname else ''}
    </div>

    <!-- Header -->
    <div class="bento bento-2" style="margin-bottom:14px">
     <div class="card" style="margin:0">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px">
       <div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:28px;font-weight:700">{qn}</div>
        <div style="color:var(--tx2);font-size:12px;margin-top:4px">{agency}{' · ' if agency else ''}Generated {qt.get('date','')}</div>
       </div>
       <span style="padding:4px 12px;border-radius:12px;font-size:12px;font-weight:600;color:{color};background:{bg}">{lbl}</span>
      </div>
      <div class="meta-g" style="margin:0">
       <div class="meta-i"><div class="meta-l">Institution</div><div class="meta-v">{institution}</div></div>
       <div class="meta-i"><div class="meta-l">RFQ / PC #</div><div class="meta-v">{qt.get('rfq_number','—')}</div></div>
       <div class="meta-i"><div class="meta-l">Items</div><div class="meta-v">{qt.get('items_count',0)}</div></div>
       <div class="meta-i"><div class="meta-l">Expiry</div><div class="meta-v">{qt.get('expiry','—')}</div></div>
       {'<div class="meta-i"><div class="meta-l">PO Number</div><div class="meta-v" style="color:var(--gn);font-weight:600">' + qt.get("po_number","") + '</div></div>' if qt.get("po_number") else ''}
      </div>
     </div>
     <div class="card" style="margin:0">
      <div style="text-align:center;padding:12px 0">
       <div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px">Quote Total</div>
       <div style="font-family:'JetBrains Mono',monospace;font-size:36px;font-weight:700;color:var(--gn);margin:8px 0">${qt.get('total',0):,.2f}</div>
       <div style="display:flex;justify-content:center;gap:16px;font-size:12px;color:var(--tx2)">
        <span>Subtotal: <b>${qt.get('subtotal',0):,.2f}</b></span>
        <span>Tax: <b>${qt.get('tax',0):,.2f}</b></span>
       </div>
      </div>
      {'<div style="border-top:1px solid var(--bd);margin-top:14px;padding-top:14px"><div style="font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px">Status History</div>' + history_html + '</div>' if history_html else ''}
      {action_btns}
     </div>
    </div>

    <!-- Line Items -->
    <div class="card">
     <div class="card-t">Line Items</div>
     <div style="overflow-x:auto">
     <table class="home-tbl">
      <thead><tr>
       <th style="width:40px">#</th><th>Description</th><th style="width:120px">Part #</th>
       <th style="width:60px;text-align:center">Qty</th><th style="width:90px;text-align:right">Unit Price</th><th style="width:90px;text-align:right">Extended</th>
      </tr></thead>
      <tbody>{items_html if items_html else '<tr><td colspan="6" style="text-align:center;padding:16px;color:var(--tx2)">No item details stored</td></tr>'}</tbody>
     </table>
     </div>
    </div>

    <!-- CRM Section: Agency Intel + Activity Timeline -->
    <div class="bento bento-2" style="margin-top:14px">
     <div class="card" style="margin:0">
      <div class="card-t">🏢 Agency Intel</div>
      <div id="win-prediction" style="padding:6px 0;font-size:12px"></div>
      <div id="agency-intel" style="color:var(--tx2);font-size:12px;padding:4px 0">Loading agency data...</div>
     </div>
     <div class="card" style="margin:0">
      <div class="card-t">📋 Activity Timeline</div>
      <div id="crm-activity" style="max-height:320px;overflow-y:auto;font-size:12px">Loading...</div>
      <div style="margin-top:10px;border-top:1px solid var(--bd);padding-top:10px;display:flex;gap:6px">
       <input id="crm-note" placeholder="Add a note..." style="flex:1;padding:8px 10px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);font-size:12px">
       <button onclick="addNote()" class="btn btn-p" style="padding:8px 12px;font-size:12px">Add</button>
      </div>
     </div>
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

    function addNote() {{
      const note = document.getElementById('crm-note').value.trim();
      if (!note) return;
      fetch('/api/crm/activity', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{ref_id: '{qn}', event_type: 'note', description: note,
                              metadata: {{institution: '{institution}', agency: '{agency}'}} }})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{
          document.getElementById('crm-note').value = '';
          loadActivity();
        }}
      }});
    }}

    const eventIcons = {{
      'quote_won': '✅', 'quote_lost': '❌', 'quote_sent': '📤',
      'quote_generated': '📋', 'qb_po_created': '💰', 'email_sent': '📧',
      'email_received': '📨', 'voice_call': '📞', 'scprs_lookup': '🔍',
      'price_check': '📊', 'lead_scored': '🎯', 'follow_up': '🔔', 'note': '📝'
    }};

    function loadActivity() {{
      fetch('/api/crm/activity?ref_id={qn}&limit=30').then(r => r.json()).then(d => {{
        const el = document.getElementById('crm-activity');
        if (!d.ok || !d.activity.length) {{
          el.innerHTML = '<div style="color:var(--tx2);padding:12px">No activity yet</div>';
          return;
        }}
        el.innerHTML = d.activity.map(a => {{
          const icon = eventIcons[a.event_type] || '•';
          const ts = a.timestamp ? a.timestamp.substring(0,16).replace('T',' ') : '';
          const actor = a.actor && a.actor !== 'system' ? ' <span style="color:var(--ac)">' + a.actor + '</span>' : '';
          return '<div style="padding:6px 0;border-bottom:1px solid var(--bd);display:flex;gap:8px;align-items:baseline">' +
            '<span>' + icon + '</span>' +
            '<div style="flex:1"><div>' + a.description + actor + '</div>' +
            '<div style="font-size:10px;color:var(--tx2);font-family:monospace">' + ts + '</div></div></div>';
        }}).join('');
      }}).catch(() => {{
        document.getElementById('crm-activity').innerHTML = '<div style="color:var(--rd)">Failed to load</div>';
      }});
    }}

    function loadAgencyIntel() {{
      const agency = '{agency}' || '{institution}'.split('-')[0].split(' ')[0];
      if (!agency) {{
        document.getElementById('agency-intel').innerHTML = '<div style="color:var(--tx2)">No agency detected</div>';
        return;
      }}
      fetch('/api/crm/agency/' + encodeURIComponent(agency)).then(r => r.json()).then(d => {{
        if (!d.ok) {{ document.getElementById('agency-intel').innerHTML = '<div>No data</div>'; return; }}
        const wrColor = d.win_rate >= 50 ? 'var(--gn)' : (d.win_rate >= 30 ? 'var(--yl)' : 'var(--rd)');
        let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px">';
        html += '<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Quotes</div><div style="font-size:22px;font-weight:700">' + d.total_quotes + '</div></div>';
        html += '<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Win Rate</div><div style="font-size:22px;font-weight:700;color:' + wrColor + '">' + d.win_rate + '%</div></div>';
        html += '<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Won Value</div><div style="font-size:16px;font-weight:700;color:var(--gn)">$' + d.total_won_value.toLocaleString() + '</div></div>';
        html += '<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Pending</div><div style="font-size:22px;font-weight:700;color:var(--yl)">' + d.pending + '</div></div>';
        html += '</div>';
        if (d.institutions && d.institutions.length) {{
          html += '<div style="font-size:11px;color:var(--tx2);margin-bottom:6px"><b>Facilities:</b></div>';
          html += '<div style="display:flex;flex-wrap:wrap;gap:4px">';
          d.institutions.forEach(inst => {{
            html += '<span style="background:var(--sf2);padding:2px 8px;border-radius:10px;font-size:10px">' + inst + '</span>';
          }});
          html += '</div>';
        }}
        if (d.last_contact) {{
          const days = Math.floor((Date.now() - new Date(d.last_contact).getTime()) / 86400000);
          const color = days > 14 ? 'var(--rd)' : (days > 7 ? 'var(--yl)' : 'var(--gn)');
          html += '<div style="margin-top:10px;font-size:11px">Last contact: <b style="color:' + color + '">' + days + ' days ago</b></div>';
        }}
        document.getElementById('agency-intel').innerHTML = html;
      }}).catch(() => {{
        document.getElementById('agency-intel').innerHTML = '<div>Failed to load</div>';
      }});
    }}

    // Load on page ready
    loadActivity();
    loadAgencyIntel();

    // Win prediction
    fetch('/api/predict/win?institution={institution}&agency={agency}&value={qt.get("total",0)}')
      .then(r => r.json()).then(d => {{
        if (!d.ok) return;
        const pct = Math.round(d.probability * 100);
        const clr = pct >= 60 ? 'var(--gn)' : (pct >= 40 ? 'var(--yl)' : 'var(--rd)');
        const bar = '<div style="background:var(--sf2);border-radius:6px;height:8px;margin:6px 0;overflow:hidden"><div style="width:' + pct + '%;height:100%;background:' + clr + ';border-radius:6px;transition:width .5s"></div></div>';
        document.getElementById('win-prediction').innerHTML =
          '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">' +
          '<span style="font-weight:600;font-size:11px">🎯 Win Prediction</span>' +
          '<span style="font-size:18px;font-weight:700;color:' + clr + '">' + pct + '%</span></div>' + bar +
          '<div style="color:var(--tx2);font-size:10px">' + d.recommendation + ' <span style="opacity:.5">(' + d.confidence + ' confidence)</span></div>';
      }}).catch(() => {{}});
    </script>
    """
    return render(content, title=f"Quote {qn}")


# ═══════════════════════════════════════════════════════════════════════
# Order Management (Phase 17)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/orders")
@auth_required
def orders_page():
    """Orders dashboard — track sourcing, shipping, delivery, invoicing."""
    orders = _load_orders()
    order_list = sorted(orders.values(), key=lambda o: o.get("created_at", ""), reverse=True)

    status_cfg = {
        "new":              ("🆕 New",              "#58a6ff", "rgba(88,166,255,.1)"),
        "sourcing":         ("🛒 Sourcing",         "#d29922", "rgba(210,153,34,.1)"),
        "shipped":          ("🚚 Shipped",          "#bc8cff", "rgba(188,140,255,.1)"),
        "partial_delivery": ("📦 Partial",          "#d29922", "rgba(210,153,34,.1)"),
        "delivered":        ("✅ Delivered",         "#3fb950", "rgba(52,211,153,.1)"),
        "invoiced":         ("💰 Invoiced",         "#58a6ff", "rgba(88,166,255,.1)"),
        "closed":           ("🏁 Closed",           "#8b949e", "rgba(139,148,160,.1)"),
    }

    # Stats
    total_orders = len(order_list)
    active = sum(1 for o in order_list if o.get("status") not in ("closed",))
    total_value = sum(o.get("total", 0) for o in order_list)
    invoiced_value = sum(o.get("invoice_total", 0) for o in order_list)

    stats_html = f"""
    <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;font-size:13px;font-family:'JetBrains Mono',monospace">
     <span><b>{total_orders}</b> orders</span>
     <span style="color:#58a6ff"><b>{active}</b> active</span>
     <span style="color:#3fb950">value: <b>${total_value:,.0f}</b></span>
     <span style="color:#d29922">invoiced: <b>${invoiced_value:,.0f}</b></span>
    </div>"""

    rows = ""
    for o in order_list:
        oid = o.get("order_id", "")
        st = o.get("status", "new")
        lbl, clr, bg = status_cfg.get(st, status_cfg["new"])
        items = o.get("line_items", [])
        sourced = sum(1 for it in items if it.get("sourcing_status") in ("ordered", "shipped", "delivered"))
        shipped = sum(1 for it in items if it.get("sourcing_status") in ("shipped", "delivered"))
        delivered = sum(1 for it in items if it.get("sourcing_status") == "delivered")
        progress = f"{delivered}/{len(items)}" if items else "0/0"

        rows += f"""<tr style="{'opacity:0.5' if st == 'closed' else ''}">
         <td><a href="/order/{oid}" style="color:var(--ac);text-decoration:none;font-family:'JetBrains Mono',monospace;font-weight:700">{oid}</a></td>
         <td class="mono" style="white-space:nowrap">{o.get('created_at','')[:10]}</td>
         <td>{o.get('agency','')}</td>
         <td style="max-width:250px;word-wrap:break-word;white-space:normal;font-weight:500">{o.get('institution','')}</td>
         <td class="mono">{o.get('po_number','') or o.get('quote_number','')}</td>
         <td style="text-align:right;font-weight:600;font-family:'JetBrains Mono',monospace">${o.get('total',0):,.2f}</td>
         <td style="text-align:center">{progress}</td>
         <td style="text-align:center"><span style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;color:{clr};background:{bg}">{lbl}</span></td>
        </tr>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:14px">
     <h2 style="margin:0;font-size:20px;font-weight:700">📦 Orders</h2>
     <div>{stats_html}</div>
    </div>
    <div class="card" style="padding:0;overflow-x:auto">
     <table class="home-tbl" style="min-width:800px">
      <thead><tr>
       <th style="width:130px">Order</th><th style="width:90px">Date</th><th style="width:60px">Agency</th>
       <th>Institution</th><th style="width:100px">PO / Quote</th>
       <th style="text-align:right;width:90px">Total</th><th style="width:70px;text-align:center">Delivery</th>
       <th style="width:100px;text-align:center">Status</th>
      </tr></thead>
      <tbody>{rows if rows else '<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--tx2)">No orders yet — mark a quote as Won to create one</td></tr>'}</tbody>
     </table>
    </div>

    <!-- Pending Invoices from QuickBooks -->
    <div id="qb-invoices" class="card" style="margin-top:14px;padding:16px;display:none">
     <div class="card-t" style="margin-bottom:10px">💰 QuickBooks — Pending Invoices</div>
     <div id="inv-stats" style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px"></div>
     <div id="inv-table"></div>
    </div>
    <script>
    fetch('/api/qb/financial-context').then(r=>r.json()).then(d=>{{
     if(!d.ok) return;
     document.getElementById('qb-invoices').style.display='block';
     const s=document.getElementById('inv-stats');
     const mkStat=(label,val,color)=>'<div style="background:var(--sf2);padding:10px;border-radius:8px;text-align:center"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">'+label+'</div><div style="font-size:18px;font-weight:700;color:'+(color||'var(--tx)')+'">'+val+'</div></div>';
     s.innerHTML=mkStat('Open','$'+(d.total_receivable||0).toLocaleString(),'var(--yl)')
      +mkStat('Overdue','$'+(d.overdue_amount||0).toLocaleString(),'var(--rd)')
      +mkStat('Collected','$'+(d.total_collected||0).toLocaleString(),'var(--gn)')
      +mkStat('Invoices',d.invoice_count||0);
     const inv=d.pending_invoices||[];
     if(inv.length){{
      let t='<table class="tbl" style="width:100%"><thead><tr><th>Invoice</th><th>Customer</th><th style="text-align:right">Total</th><th style="text-align:right">Balance</th><th>Due</th><th>Days Out</th><th>Status</th></tr></thead><tbody>';
      inv.forEach(i=>{{
       const st=i.status==='overdue'?'<span style="color:var(--rd);font-weight:600">⚠️ OVERDUE</span>':'<span style="color:var(--yl)">Open</span>';
       t+='<tr><td class="mono">'+i.doc_number+'</td><td>'+i.customer+'</td><td style="text-align:right;font-weight:600" class="mono">$'+i.total.toLocaleString()+'</td><td style="text-align:right;color:var(--yl)" class="mono">$'+i.balance.toLocaleString()+'</td><td class="mono">'+i.due_date+'</td><td style="text-align:center">'+i.days_outstanding+'</td><td>'+st+'</td></tr>';
      }});
      t+='</tbody></table>';
      document.getElementById('inv-table').innerHTML=t;
     }} else {{
      document.getElementById('inv-table').innerHTML='<div style="color:var(--tx2);text-align:center;padding:12px">No pending invoices</div>';
     }}
    }}).catch(()=>{{}});
    </script>"""
    return render(content, title="Orders")


@bp.route("/order/<oid>")
@auth_required
def order_detail(oid):
    """Order detail page — line item sourcing, tracking, invoicing."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        flash(f"Order {oid} not found", "error")
        return redirect("/orders")

    st = order.get("status", "new")
    items = order.get("line_items", [])
    qn = order.get("quote_number", "")
    institution = order.get("institution", "")

    sourcing_cfg = {
        "pending":   ("⏳ Pending",   "#d29922", "rgba(210,153,34,.1)"),
        "ordered":   ("🛒 Ordered",   "#58a6ff", "rgba(88,166,255,.1)"),
        "shipped":   ("🚚 Shipped",   "#bc8cff", "rgba(188,140,255,.1)"),
        "delivered": ("✅ Delivered", "#3fb950", "rgba(52,211,153,.1)"),
    }
    inv_cfg = {
        "pending":  ("⏳", "#d29922"),
        "partial":  ("½", "#58a6ff"),
        "invoiced": ("✅", "#3fb950"),
    }

    # Line items table
    items_rows = ""
    for it in items:
        lid = it.get("line_id", "")
        desc = it.get("description", "")[:80]
        pn = it.get("part_number", "")
        sup_url = it.get("supplier_url", "")
        sup_link = f'<a href="{sup_url}" target="_blank" style="color:var(--ac);font-size:11px">🛒 {it.get("supplier","Amazon")}</a>' if sup_url else (it.get("supplier","") or "—")

        ss = it.get("sourcing_status", "pending")
        s_lbl, s_clr, s_bg = sourcing_cfg.get(ss, sourcing_cfg["pending"])
        tracking = it.get("tracking_number", "")
        tracking_html = f'<a href="https://track.aftership.com/{tracking}" target="_blank" style="color:var(--ac);font-size:10px">{tracking[:20]}</a>' if tracking else ""
        carrier = it.get("carrier", "")

        is_lbl, is_clr = inv_cfg.get(it.get("invoice_status","pending"), inv_cfg["pending"])

        items_rows += f"""<tr data-lid="{lid}">
         <td style="color:var(--tx2);font-size:11px">{lid}</td>
         <td style="max-width:300px;word-wrap:break-word;white-space:normal">{desc}</td>
         <td class="mono" style="font-size:11px">{pn or '—'}</td>
         <td>{sup_link}</td>
         <td class="mono" style="text-align:center">{it.get('qty',0)}</td>
         <td class="mono" style="text-align:right">${it.get('unit_price',0):,.2f}</td>
         <td style="text-align:center">
          <select onchange="updateLine('{oid}','{lid}','sourcing_status',this.value)" style="background:var(--sf);border:1px solid var(--bd);border-radius:4px;color:{s_clr};font-size:11px;padding:2px">
           <option value="pending" {"selected" if ss=="pending" else ""}>⏳ Pending</option>
           <option value="ordered" {"selected" if ss=="ordered" else ""}>🛒 Ordered</option>
           <option value="shipped" {"selected" if ss=="shipped" else ""}>🚚 Shipped</option>
           <option value="delivered" {"selected" if ss=="delivered" else ""}>✅ Delivered</option>
          </select>
         </td>
         <td style="font-size:10px">{carrier} {tracking_html}</td>
         <td style="text-align:center;font-size:12px;color:{is_clr}" title="{it.get('invoice_status','pending')}">{is_lbl}</td>
        </tr>"""

    status_cfg = {
        "new": "🆕 New", "sourcing": "🛒 Sourcing", "shipped": "🚚 Shipped",
        "partial_delivery": "📦 Partial Delivery", "delivered": "✅ Delivered",
        "invoiced": "💰 Invoiced", "closed": "🏁 Closed"
    }

    content = f"""
    <div style="display:flex;gap:10px;align-items:center;margin-bottom:16px">
     <a href="/orders" class="btn btn-s" style="font-size:13px">← Orders</a>
     {f'<a href="/quote/{qn}" class="btn btn-s" style="font-size:13px">📋 Quote {qn}</a>' if qn else ''}
    </div>

    <div class="bento bento-2" style="margin-bottom:14px">
     <div class="card" style="margin:0">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px">
       <div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:700">{oid}</div>
        <div style="color:var(--tx2);font-size:12px;margin-top:4px">{order.get('agency','')}{' · ' if order.get('agency') else ''}Created {order.get('created_at','')[:10]}</div>
       </div>
       <span style="padding:4px 12px;border-radius:12px;font-size:12px;font-weight:600;background:var(--sf2)">{status_cfg.get(st, st)}</span>
      </div>
      <div class="meta-g" style="margin:0">
       <div class="meta-i"><div class="meta-l">Institution</div><div class="meta-v">{institution}</div></div>
       <div class="meta-i"><div class="meta-l">PO Number</div><div class="meta-v" style="color:var(--gn);font-weight:600">{order.get('po_number','—')}</div></div>
       <div class="meta-i"><div class="meta-l">Quote</div><div class="meta-v">{qn or '—'}</div></div>
       <div class="meta-i"><div class="meta-l">Items</div><div class="meta-v">{len(items)}</div></div>
      </div>
     </div>
     <div class="card" style="margin:0">
      <div style="text-align:center;padding:12px 0">
       <div style="font-size:10px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px">Order Total</div>
       <div style="font-family:'JetBrains Mono',monospace;font-size:32px;font-weight:700;color:var(--gn);margin:8px 0">${order.get('total',0):,.2f}</div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-top:10px">
       <div style="background:var(--sf2);padding:8px;border-radius:8px;text-align:center">
        <div style="font-size:10px;color:var(--tx2)">Sourced</div>
        <div style="font-size:18px;font-weight:700;color:#58a6ff">{sum(1 for i in items if i.get('sourcing_status') in ('ordered','shipped','delivered'))}/{len(items)}</div>
       </div>
       <div style="background:var(--sf2);padding:8px;border-radius:8px;text-align:center">
        <div style="font-size:10px;color:var(--tx2)">Shipped</div>
        <div style="font-size:18px;font-weight:700;color:#bc8cff">{sum(1 for i in items if i.get('sourcing_status') in ('shipped','delivered'))}/{len(items)}</div>
       </div>
       <div style="background:var(--sf2);padding:8px;border-radius:8px;text-align:center">
        <div style="font-size:10px;color:var(--tx2)">Delivered</div>
        <div style="font-size:18px;font-weight:700;color:#3fb950">{sum(1 for i in items if i.get('sourcing_status') == 'delivered')}/{len(items)}</div>
       </div>
      </div>
      <div style="margin-top:12px;display:flex;gap:8px;justify-content:center">
       <button onclick="invoiceOrder('{oid}','partial')" class="btn btn-s" style="font-size:12px">½ Partial Invoice</button>
       <button onclick="invoiceOrder('{oid}','full')" class="btn btn-g" style="font-size:12px">💰 Full Invoice</button>
      </div>
     </div>
    </div>

    <!-- Line Items with sourcing controls -->
    <div class="card">
     <div class="card-t">Line Items — Sourcing & Tracking</div>
     <div style="overflow-x:auto">
     <table class="home-tbl" style="min-width:900px">
      <thead><tr>
       <th style="width:40px">#</th><th>Description</th><th style="width:80px">Part #</th>
       <th style="width:80px">Supplier</th><th style="width:40px;text-align:center">Qty</th>
       <th style="width:80px;text-align:right">Price</th><th style="width:100px;text-align:center">Status</th>
       <th style="width:140px">Tracking</th><th style="width:30px;text-align:center">Inv</th>
      </tr></thead>
      <tbody>{items_rows}</tbody>
     </table>
     </div>
    </div>

    <!-- Bulk actions -->
    <div class="card" style="margin-top:14px">
     <div class="card-t">Quick Actions</div>
     <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button onclick="bulkAddTracking('{oid}')" class="btn btn-s" style="font-size:12px">📋 Bulk Add Tracking</button>
      <button onclick="markAllOrdered('{oid}')" class="btn btn-s" style="font-size:12px">🛒 Mark All Ordered</button>
      <button onclick="markAllDelivered('{oid}')" class="btn btn-s" style="font-size:12px">✅ Mark All Delivered</button>
      <a href="/api/order/{oid}/reply-all" class="btn btn-s" style="font-size:12px">📧 Reply-All Confirmation</a>
     </div>
    </div>

    <script>
    function updateLine(oid, lid, field, value) {{
      fetch('/api/order/' + oid + '/line/' + lid, {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{[field]: value}})
      }}).then(r => r.json()).then(d => {{
        if (!d.ok) alert('Error: ' + (d.error||'unknown'));
        else location.reload();
      }});
    }}

    function invoiceOrder(oid, type) {{
      const num = prompt('Invoice number:');
      if (!num) return;
      fetch('/api/order/' + oid + '/invoice', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{type: type, invoice_number: num}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) location.reload();
        else alert('Error: ' + (d.error||'unknown'));
      }});
    }}

    function markAllOrdered(oid) {{
      fetch('/api/order/' + oid + '/bulk', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{sourcing_status: 'ordered'}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); }});
    }}

    function markAllDelivered(oid) {{
      fetch('/api/order/' + oid + '/bulk', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{sourcing_status: 'delivered'}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); }});
    }}

    function bulkAddTracking(oid) {{
      const tracking = prompt('Tracking number(s) — comma separated for multiple shipments:');
      if (!tracking) return;
      const carrier = prompt('Carrier (UPS/FedEx/USPS/Amazon):', 'Amazon');
      fetch('/api/order/' + oid + '/bulk-tracking', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{tracking: tracking, carrier: carrier}})
      }}).then(r => r.json()).then(d => {{ if(d.ok) location.reload(); }});
    }}
    </script>
    """
    return render(content, title=f"Order {oid}")


# ─── Order API Routes ──────────────────────────────────────────────────────

@bp.route("/api/order/<oid>/line/<lid>", methods=["POST"])
@auth_required
def api_order_update_line(oid, lid):
    """Update a single line item. POST JSON with any fields to update."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
    updated = False
    for it in order.get("line_items", []):
        if it.get("line_id") == lid:
            for field in ("sourcing_status", "tracking_number", "carrier",
                          "ship_date", "delivery_date", "invoice_status",
                          "invoice_number", "supplier", "supplier_url", "notes"):
                if field in data:
                    old_val = it.get(field, "")
                    it[field] = data[field]
                    if field == "sourcing_status" and old_val != data[field]:
                        _log_crm_activity(order.get("quote_number",""), f"line_{data[field]}",
                                          f"Order {oid} line {lid}: {old_val} → {data[field]} — {it.get('description','')[:60]}",
                                          actor="user", metadata={"order_id": oid})
            updated = True
            break
    if not updated:
        return jsonify({"ok": False, "error": "Line item not found"})
    order["updated_at"] = datetime.now().isoformat()
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    return jsonify({"ok": True})


@bp.route("/api/order/<oid>/bulk", methods=["POST"])
@auth_required
def api_order_bulk_update(oid):
    """Bulk update all line items. POST JSON with fields to set on all items."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
    for it in order.get("line_items", []):
        for field in ("sourcing_status", "carrier", "invoice_status"):
            if field in data:
                it[field] = data[field]
    order["updated_at"] = datetime.now().isoformat()
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), "order_bulk_update",
                      f"Order {oid}: bulk update — {data}",
                      actor="user", metadata={"order_id": oid})
    return jsonify({"ok": True})


@bp.route("/api/order/<oid>/bulk-tracking", methods=["POST"])
@auth_required
def api_order_bulk_tracking(oid):
    """Add tracking to all pending/ordered items. POST: {tracking, carrier}"""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
    tracking = data.get("tracking", "")
    carrier = data.get("carrier", "")
    updated = 0
    for it in order.get("line_items", []):
        if it.get("sourcing_status") in ("pending", "ordered"):
            it["tracking_number"] = tracking
            it["carrier"] = carrier
            it["sourcing_status"] = "shipped"
            it["ship_date"] = datetime.now().strftime("%Y-%m-%d")
            updated += 1
    order["updated_at"] = datetime.now().isoformat()
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), "tracking_added",
                      f"Order {oid}: tracking {tracking} ({carrier}) added to {updated} items",
                      actor="user", metadata={"order_id": oid, "tracking": tracking})
    return jsonify({"ok": True, "updated": updated})


@bp.route("/api/order/<oid>/invoice", methods=["POST"])
@auth_required
def api_order_invoice(oid):
    """Create partial or full invoice. POST: {type: 'partial'|'full', invoice_number}"""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return jsonify({"ok": False, "error": "Order not found"})
    data = request.get_json(silent=True) or {}
    inv_type = data.get("type", "full")
    inv_num = data.get("invoice_number", "")

    if inv_type == "full":
        # Mark all items as invoiced
        for it in order.get("line_items", []):
            it["invoice_status"] = "invoiced"
            it["invoice_number"] = inv_num
        order["invoice_type"] = "full"
        order["invoice_total"] = order.get("total", 0)
    elif inv_type == "partial":
        # Mark only delivered items as invoiced
        partial_total = 0
        for it in order.get("line_items", []):
            if it.get("sourcing_status") == "delivered":
                it["invoice_status"] = "invoiced"
                it["invoice_number"] = inv_num
                partial_total += it.get("extended", 0)
            elif it.get("sourcing_status") in ("shipped", "ordered"):
                it["invoice_status"] = "partial"
        order["invoice_type"] = "partial"
        order["invoice_total"] = partial_total

    order["invoice_number"] = inv_num
    order["updated_at"] = datetime.now().isoformat()
    order["status_history"].append({
        "status": f"invoice_{inv_type}",
        "timestamp": datetime.now().isoformat(),
        "actor": "user",
        "invoice_number": inv_num,
    })
    orders[oid] = order
    _save_orders(orders)
    _update_order_status(oid)
    _log_crm_activity(order.get("quote_number",""), f"invoice_{inv_type}",
                      f"Order {oid}: {inv_type} invoice #{inv_num} — ${order.get('invoice_total',0):,.2f}",
                      actor="user", metadata={"order_id": oid, "invoice": inv_num})
    return jsonify({"ok": True, "invoice_type": inv_type, "invoice_total": order.get("invoice_total", 0)})


@bp.route("/api/order/<oid>/reply-all")
@auth_required
def api_order_reply_all(oid):
    """Generate reply-all confirmation email for the won quote's original thread."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        flash("Order not found", "error")
        return redirect("/orders")

    qn = order.get("quote_number", "")
    institution = order.get("institution", "")
    po_num = order.get("po_number", "")
    total = order.get("total", 0)
    items = order.get("line_items", [])

    items_list = "\n".join(
        f"  - {it.get('description','')[:60]} (Qty {it.get('qty',0)}) — ${it.get('extended',0):,.2f}"
        for it in items[:15]
    )

    subject = f"RE: Reytech Quote {qn}" + (f" — PO {po_num}" if po_num else "") + " — Order Confirmation"
    body = f"""Thank you for your order!

We are pleased to confirm receipt of {"PO " + po_num if po_num else "your order"} for {institution}.

Quote: {qn}
Order Total: ${total:,.2f}
Items ({len(items)}):
{items_list}

We will process your order promptly and provide tracking information as items ship.

Please don't hesitate to reach out with any questions.

Best regards,
Mike Gonzalez
Reytech Inc.
949-229-1575
sales@reytechinc.com"""

    # Store as draft and redirect to a mailto link
    mailto = f"mailto:?subject={subject}&body={body}".replace("\n", "%0A").replace(" ", "%20")

    _log_crm_activity(qn, "email_sent", f"Order confirmation reply-all for {oid}",
                      actor="user", metadata={"order_id": oid})

    return redirect(mailto)


# ═══════════════════════════════════════════════════════════════════════
# Pipeline Dashboard (Phase 20)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/pipeline")
@auth_required
def pipeline_page():
    """Autonomous pipeline dashboard — full funnel visibility."""
    quotes = get_all_quotes()
    orders = _load_orders()
    crm = _load_crm_activity()
    leads = []
    try:
        import json as _json
        with open(os.path.join(DATA_DIR, "leads.json")) as f:
            leads = _json.load(f)
    except Exception:
        pass

    # ── Funnel Counts ──
    total_leads = len(leads)
    total_quotes = len(quotes)
    sent = sum(1 for q in quotes if q.get("status") in ("sent",))
    pending = sum(1 for q in quotes if q.get("status") in ("pending",))
    won = sum(1 for q in quotes if q.get("status") == "won")
    lost = sum(1 for q in quotes if q.get("status") == "lost")
    expired = sum(1 for q in quotes if q.get("status") == "expired")
    total_orders = len(orders)
    invoiced = sum(1 for o in orders.values() if o.get("status") in ("invoiced", "closed"))

    # ── Revenue ──
    total_quoted = sum(q.get("total", 0) for q in quotes)
    total_won = sum(q.get("total", 0) for q in quotes if q.get("status") == "won")
    total_pending = sum(q.get("total", 0) for q in quotes if q.get("status") in ("pending", "sent"))
    total_invoiced = sum(o.get("invoice_total", 0) for o in orders.values())

    # ── Conversion Rates ──
    def rate(a, b): return round(a/b*100) if b > 0 else 0
    lead_to_quote = rate(total_quotes, total_leads) if total_leads else "—"
    quote_to_sent = rate(sent + won + lost, total_quotes) if total_quotes else "—"
    sent_to_won = rate(won, won + lost) if (won + lost) else "—"
    won_to_invoiced = rate(invoiced, total_orders) if total_orders else "—"

    # ── Funnel bars ──
    max_count = max(total_leads, total_quotes, 1)
    def bar(count, color, label, sublabel=""):
        pct = max(5, round(count / max_count * 100))
        return f"""<div style="margin-bottom:8px">
         <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
          <span style="font-size:12px;font-weight:600">{label}</span>
          <span style="font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;color:{color}">{count}</span>
         </div>
         <div style="background:var(--sf2);border-radius:6px;height:24px;overflow:hidden">
          <div style="width:{pct}%;height:100%;background:{color};border-radius:6px;display:flex;align-items:center;padding-left:8px">
           <span style="font-size:10px;color:#fff;font-weight:600">{sublabel}</span>
          </div>
         </div>
        </div>"""

    funnel = (
        bar(total_leads, "#58a6ff", "🔍 Leads (SCPRS)", f"{total_leads} opportunities") +
        bar(total_quotes, "#bc8cff", "📋 Quotes Generated", f"${total_quoted:,.0f} total") +
        bar(sent + won + lost, "#d29922", "📤 Sent to Buyer", f"{sent} active") +
        bar(won, "#3fb950", "✅ Won", f"${total_won:,.0f} revenue") +
        bar(total_orders, "#58a6ff", "📦 Orders", f"{total_orders} active") +
        bar(invoiced, "#3fb950", "💰 Invoiced", f"${total_invoiced:,.0f}")
    )

    # ── Recent CRM events ──
    recent = sorted(crm, key=lambda e: e.get("timestamp", ""), reverse=True)[:15]
    evt_icons = {
        "quote_won": "✅", "quote_lost": "❌", "quote_sent": "📤", "quote_generated": "📋",
        "order_created": "📦", "voice_call": "📞", "email_sent": "📧", "note": "📝",
        "shipping_detected": "🚚", "invoice_full": "💰", "invoice_partial": "½",
    }
    events_html = ""
    for e in recent:
        icon = evt_icons.get(e.get("event_type", ""), "●")
        ts = e.get("timestamp", "")[:16].replace("T", " ")
        events_html += f"""<div style="padding:6px 0;border-bottom:1px solid var(--bd);font-size:12px;display:flex;gap:8px;align-items:flex-start">
         <span style="flex-shrink:0">{icon}</span>
         <div style="flex:1"><div>{e.get('description','')[:100]}</div><div style="color:var(--tx2);font-size:10px;margin-top:2px">{ts}</div></div>
        </div>"""

    # ── Prediction leaderboard for pending quotes ──
    predictions_html = ""
    if PREDICT_AVAILABLE:
        preds = []
        for q in quotes:
            if q.get("status") in ("pending", "sent"):
                p = predict_win_probability(
                    institution=q.get("institution", ""),
                    agency=q.get("agency", ""),
                    po_value=q.get("total", 0),
                )
                preds.append({**q, "win_prob": p["probability"], "rec": p["recommendation"]})
        preds.sort(key=lambda x: x["win_prob"], reverse=True)
        for q in preds[:10]:
            prob = round(q["win_prob"] * 100)
            clr = "#3fb950" if prob >= 60 else ("#d29922" if prob >= 40 else "#f85149")
            predictions_html += f"""<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--bd);font-size:12px">
             <span style="font-family:'JetBrains Mono',monospace;font-weight:700;color:{clr};min-width:36px">{prob}%</span>
             <a href="/quote/{q.get('quote_number','')}" style="color:var(--ac);text-decoration:none;font-weight:600">{q.get('quote_number','')}</a>
             <span style="color:var(--tx2);flex:1">{q.get('institution','')[:30]}</span>
             <span style="font-family:'JetBrains Mono',monospace">${q.get('total',0):,.0f}</span>
            </div>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
     <h2 style="margin:0;font-size:20px;font-weight:700">🔄 Pipeline Dashboard</h2>
     <div style="font-size:12px;font-family:'JetBrains Mono',monospace;display:flex;gap:16px">
      <span>📊 Leads: <b>{total_leads}</b></span>
      <span>📋 Quotes: <b>{total_quotes}</b></span>
      <span style="color:var(--gn)">💰 Pipeline: <b>${total_pending:,.0f}</b></span>
      <span style="color:var(--gn)">🏆 Won: <b>${total_won:,.0f}</b></span>
     </div>
    </div>

    <div class="bento bento-2">
     <div class="card" style="margin:0">
      <div class="card-t">📊 Sales Funnel</div>
      {funnel}
      <div style="margin-top:14px;padding-top:10px;border-top:1px solid var(--bd);display:grid;grid-template-columns:repeat(4,1fr);gap:8px">
       <div style="text-align:center;background:var(--sf2);padding:8px;border-radius:8px">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Lead→Quote</div>
        <div style="font-size:16px;font-weight:700">{lead_to_quote}{'%' if isinstance(lead_to_quote,int) else ''}</div>
       </div>
       <div style="text-align:center;background:var(--sf2);padding:8px;border-radius:8px">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Quote→Sent</div>
        <div style="font-size:16px;font-weight:700">{quote_to_sent}{'%' if isinstance(quote_to_sent,int) else ''}</div>
       </div>
       <div style="text-align:center;background:var(--sf2);padding:8px;border-radius:8px">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Win Rate</div>
        <div style="font-size:16px;font-weight:700;color:var(--gn)">{sent_to_won}{'%' if isinstance(sent_to_won,int) else ''}</div>
       </div>
       <div style="text-align:center;background:var(--sf2);padding:8px;border-radius:8px">
        <div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Won→Invoiced</div>
        <div style="font-size:16px;font-weight:700">{won_to_invoiced}{'%' if isinstance(won_to_invoiced,int) else ''}</div>
       </div>
      </div>
     </div>

     <div class="card" style="margin:0">
      <div class="card-t">⏱️ Recent Activity</div>
      <div style="max-height:400px;overflow-y:auto">
       {events_html if events_html else '<div style="color:var(--tx2);font-size:12px;padding:12px">No activity yet</div>'}
      </div>
     </div>
    </div>

    {'<div class="card" style="margin-top:14px"><div class="card-t">🎯 Win Prediction Leaderboard — Active Quotes</div><div style="max-height:320px;overflow-y:auto">' + predictions_html + '</div></div>' if predictions_html else ''}

    <div class="card" style="margin-top:14px">
     <div class="card-t">⚡ Quick Actions</div>
     <div style="display:flex;gap:8px;flex-wrap:wrap">
      <a href="/quotes?status=pending" class="btn btn-s" style="font-size:12px">📋 Pending Quotes ({pending})</a>
      <a href="/quotes?status=sent" class="btn btn-s" style="font-size:12px">📤 Sent Quotes ({sent})</a>
      <a href="/orders" class="btn btn-s" style="font-size:12px">📦 Active Orders ({total_orders})</a>
      <button onclick="fetch('/api/poll-now').then(r=>r.json()).then(d=>alert(JSON.stringify(d,null,2)))" class="btn btn-p" style="font-size:12px">⚡ Check Inbox</button>
     </div>
    </div>
    """
    return render(content, title="Pipeline")


# ═══════════════════════════════════════════════════════════════════════
# Pipeline API (Phase 20)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/pipeline/stats")
@auth_required
def api_pipeline_stats():
    """Full pipeline statistics as JSON."""
    quotes = get_all_quotes()
    orders = _load_orders()

    statuses = {}
    for q in quotes:
        s = q.get("status", "pending")
        statuses[s] = statuses.get(s, 0) + 1

    return jsonify({
        "ok": True,
        "quotes": {
            "total": len(quotes),
            "by_status": statuses,
            "total_value": sum(q.get("total", 0) for q in quotes),
            "won_value": sum(q.get("total", 0) for q in quotes if q.get("status") == "won"),
            "pending_value": sum(q.get("total", 0) for q in quotes if q.get("status") in ("pending", "sent")),
        },
        "orders": {
            "total": len(orders),
            "total_value": sum(o.get("total", 0) for o in orders.values()),
            "invoiced_value": sum(o.get("invoice_total", 0) for o in orders.values()),
        },
        "conversion": {
            "win_rate": round(statuses.get("won", 0) / max(statuses.get("won", 0) + statuses.get("lost", 0), 1) * 100, 1),
            "quote_count": len(quotes),
            "decided": statuses.get("won", 0) + statuses.get("lost", 0),
        }
    })


@bp.route("/api/pipeline/analyze-reply", methods=["POST"])
@auth_required
def api_analyze_reply():
    """Analyze an email reply for win/loss/question signals.
    POST: {subject, body, sender}"""
    if not REPLY_ANALYZER_AVAILABLE:
        return jsonify({"ok": False, "error": "Reply analyzer not available"})
    data = request.get_json(silent=True) or {}
    quotes = get_all_quotes()
    result = find_quote_from_reply(
        data.get("subject", ""), data.get("body", ""),
        data.get("sender", ""), quotes)
    result["ok"] = True

    # Auto-flag quote if high confidence win/loss
    if result.get("matched_quote") and result.get("confidence", 0) >= 0.6:
        signal = result.get("signal")
        qn = result["matched_quote"]
        if signal == "win":
            _log_crm_activity(qn, "win_signal_detected",
                              f"Email reply signals WIN for {qn} — {result.get('summary', '')}",
                              actor="system", metadata=result)
        elif signal == "loss":
            _log_crm_activity(qn, "loss_signal_detected",
                              f"Email reply signals LOSS for {qn} — {result.get('summary', '')}",
                              actor="system", metadata=result)
        elif signal == "question":
            _log_crm_activity(qn, "question_detected",
                              f"Buyer question detected for {qn} — follow up needed",
                              actor="system", metadata=result)

    return jsonify(result)


# ═══════════════════════════════════════════════════════════════════════
# Predictive Intelligence & Shipping Monitor (Phase 19)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/predict/win")
@auth_required
def api_predict_win():
    """Predict win probability for an institution/agency.
    GET ?institution=CSP-Sacramento&agency=CDCR&value=5000"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive module not available"})
    inst = request.args.get("institution", "")
    agency = request.args.get("agency", "")
    value = float(request.args.get("value", 0) or 0)
    result = predict_win_probability(institution=inst, agency=agency, po_value=value)
    return jsonify({"ok": True, **result})


@bp.route("/api/predict/batch", methods=["POST"])
@auth_required
def api_predict_batch():
    """Batch predict for multiple opportunities. POST JSON: [{institution, agency, value}, ...]"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive module not available"})
    data = request.get_json(silent=True) or []
    results = []
    for opp in data[:50]:
        pred = predict_win_probability(
            institution=opp.get("institution", ""),
            agency=opp.get("agency", ""),
            po_value=opp.get("value", 0),
        )
        results.append({**opp, **pred})
    results.sort(key=lambda r: r.get("probability", 0), reverse=True)
    return jsonify({"ok": True, "predictions": results})


@bp.route("/api/intel/competitors")
@auth_required
def api_competitor_insights():
    """Competitor intelligence summary.
    GET ?institution=...&agency=...&limit=20"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive module not available"})
    inst = request.args.get("institution", "")
    agency = request.args.get("agency", "")
    limit = int(request.args.get("limit", 20))
    result = get_competitor_insights(institution=inst, agency=agency, limit=limit)
    return jsonify({"ok": True, **result})


@bp.route("/api/shipping/scan-email", methods=["POST"])
@auth_required
def api_shipping_scan():
    """Scan an email for shipping/tracking info. POST: {subject, body, sender}"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Shipping monitor not available"})
    data = request.get_json(silent=True) or {}
    tracking_info = detect_shipping_email(
        data.get("subject", ""), data.get("body", ""), data.get("sender", ""))

    if not tracking_info.get("is_shipping"):
        return jsonify({"ok": True, "is_shipping": False})

    # Try to match to an order
    orders = _load_orders()
    matched_oid = match_tracking_to_order(tracking_info, orders)
    result = {"ok": True, **tracking_info, "matched_order": matched_oid}

    # Auto-update order if matched
    if matched_oid:
        update = update_order_from_tracking(matched_oid, tracking_info, orders)
        _save_orders(orders)
        _update_order_status(matched_oid)
        result["update"] = update
        _log_crm_activity(matched_oid, "shipping_detected",
                          f"Shipping email detected — {tracking_info.get('carrier','')} "
                          f"tracking {', '.join(tracking_info.get('tracking_numbers',[])) or 'N/A'} — "
                          f"status: {tracking_info.get('delivery_status','')}",
                          actor="system", metadata=tracking_info)

    return jsonify(result)


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

    fixture = deepcopy(TEST_PC_FIXTURE)
    pc_id = f"test_{uuid.uuid4().hex[:8]}"
    now = datetime.now()
    fixture["due_date"] = (now + timedelta(days=30)).strftime("%m/%d/%Y")

    # Auto-assign draft quote number
    draft_qn = ""
    if QUOTE_GEN_AVAILABLE:
        try:
            draft_qn = peek_next_quote_number()
        except Exception as e:
            log.debug("Suppressed: %s", e)
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
                except Exception as e:
                    log.debug("Suppressed: %s", e)
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


# ─── Item Identification Agent ───────────────────────────────────────────────

try:
    from src.agents.item_identifier import (identify_item, identify_pc_items,
                                            get_agent_status as item_id_agent_status)
    ITEM_ID_AVAILABLE = True
except ImportError:
    ITEM_ID_AVAILABLE = False

# ─── Lead Generation Agent ──────────────────────────────────────────────────

try:
    from src.agents.lead_gen_agent import (
        evaluate_po, add_lead, get_leads, update_lead_status,
        draft_outreach_email, get_agent_status as leadgen_agent_status,
        get_lead_analytics,
    )
    LEADGEN_AVAILABLE = True
except ImportError:
    LEADGEN_AVAILABLE = False

try:
    from src.agents.scprs_scanner import (
        get_scanner_status, start_scanner, stop_scanner, manual_scan,
    )
    SCANNER_AVAILABLE = True
except ImportError:
    SCANNER_AVAILABLE = False

try:
    from src.agents.predictive_intel import (
        predict_win_probability, log_competitor_intel, get_competitor_insights,
        detect_shipping_email, match_tracking_to_order, update_order_from_tracking,
    )
    PREDICT_AVAILABLE = True
except ImportError:
    PREDICT_AVAILABLE = False

try:
    from src.agents.reply_analyzer import analyze_reply, find_quote_from_reply
    REPLY_ANALYZER_AVAILABLE = True
except ImportError:
    REPLY_ANALYZER_AVAILABLE = False

try:
    from src.agents.quickbooks_agent import (
        fetch_vendors, find_vendor, create_purchase_order,
        get_recent_purchase_orders, get_agent_status as qb_agent_status,
        is_configured as qb_configured,
        fetch_invoices, get_invoice_summary, create_invoice,
        fetch_customers, find_customer, get_customer_balance_summary,
        get_financial_context,
    )
    QB_AVAILABLE = True
except ImportError:
    QB_AVAILABLE = False

try:
    from src.agents.email_outreach import (
        draft_for_pc, draft_for_lead, get_outbox, approve_email,
        update_draft, send_email as outreach_send, send_approved,
        delete_from_outbox, get_sent_log, get_agent_status as outreach_agent_status,
    )
    OUTREACH_AVAILABLE = True
except ImportError:
    OUTREACH_AVAILABLE = False

try:
    from src.agents.growth_agent import (
        win_loss_analysis, pricing_analysis, pipeline_health,
        lead_funnel, generate_recommendations, full_report,
        get_agent_status as growth_agent_status,
    )
    GROWTH_AVAILABLE = True
except ImportError:
    GROWTH_AVAILABLE = False

try:
    from src.agents.voice_agent import (
        place_call, get_call_log, get_agent_status as voice_agent_status,
        is_configured as voice_configured, SCRIPTS as VOICE_SCRIPTS,
        verify_credentials as voice_verify,
        import_twilio_to_vapi, get_vapi_call_details, get_vapi_calls,
    )
    VOICE_AVAILABLE = True
except ImportError:
    VOICE_AVAILABLE = False

try:
    from src.agents.voice_campaigns import (
        create_campaign, get_campaigns, get_campaign,
        execute_campaign_call, update_call_outcome,
        get_campaign_stats, list_scripts as campaign_list_scripts,
    )
    CAMPAIGNS_AVAILABLE = True
except ImportError:
    CAMPAIGNS_AVAILABLE = False

try:
    from src.agents.manager_agent import (
        generate_brief, get_agent_status as manager_agent_status,
    )
    MANAGER_AVAILABLE = True
except ImportError:
    MANAGER_AVAILABLE = False

try:
    from src.agents.orchestrator import (
        run_workflow, get_workflow_status, get_workflow_graph_viz,
    )
    ORCHESTRATOR_AVAILABLE = True
except ImportError:
    ORCHESTRATOR_AVAILABLE = False

try:
    from src.agents.qa_agent import full_scan, scan_html, agent_status as qa_agent_status
    QA_AVAILABLE = True
except ImportError:
    QA_AVAILABLE = False


@bp.route("/api/identify", methods=["POST"])
@auth_required
def api_identify_item():
    """Identify a single item. POST JSON: {"description": "...", "qty": 22, "uom": "EA"}"""
    if not ITEM_ID_AVAILABLE:
        return jsonify({"ok": False, "error": "Item identifier agent not available"})
    data = request.get_json(silent=True) or {}
    desc = data.get("description", "").strip()
    if not desc:
        return jsonify({"ok": False, "error": "No description provided"})
    result = identify_item(desc, qty=data.get("qty", 0), uom=data.get("uom", ""))
    return jsonify({"ok": True, **result})


@bp.route("/api/identify/pc/<pcid>")
@auth_required
def api_identify_pc(pcid):
    """Run item identification on all items in a Price Check."""
    if not ITEM_ID_AVAILABLE:
        return jsonify({"ok": False, "error": "Item identifier agent not available"})
    pcs = _load_price_checks()
    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"})
    pc = pcs[pcid]
    items = pc.get("items", [])
    if not items:
        return jsonify({"ok": False, "error": "No items in PC"})

    identified = identify_pc_items(items)
    # Save back
    pc["items"] = identified
    _save_price_checks(pcs)

    return jsonify({
        "ok": True,
        "items": len(identified),
        "identified": sum(1 for it in identified if it.get("identification")),
        "mode": identified[0].get("identification", {}).get("method", "none") if identified else "none",
        "results": [
            {
                "description": it.get("description", "")[:60],
                "search_term": it.get("_search_query", ""),
                "category": it.get("_category", ""),
                "method": it.get("identification", {}).get("method", ""),
            }
            for it in identified
        ],
    })


@bp.route("/api/agents/status")
@auth_required
def api_agents_status():
    """Status of all agents."""
    agents = {
        "item_identifier": item_id_agent_status() if ITEM_ID_AVAILABLE else {"status": "not_available"},
        "lead_gen": leadgen_agent_status() if LEADGEN_AVAILABLE else {"status": "not_available"},
        "scprs_scanner": get_scanner_status() if SCANNER_AVAILABLE else {"status": "not_available"},
        "quickbooks": qb_agent_status() if QB_AVAILABLE else {"status": "not_available"},
        "email_outreach": outreach_agent_status() if OUTREACH_AVAILABLE else {"status": "not_available"},
        "growth_strategy": growth_agent_status() if GROWTH_AVAILABLE else {"status": "not_available"},
        "voice_calls": voice_agent_status() if VOICE_AVAILABLE else {"status": "not_available"},
        "manager": manager_agent_status() if MANAGER_AVAILABLE else {"status": "not_available"},
        "orchestrator": get_workflow_status() if ORCHESTRATOR_AVAILABLE else {"status": "not_available"},
        "qa": qa_agent_status() if QA_AVAILABLE else {"status": "not_available"},
        "predictive_intel": {"status": "ready", "version": "1.0.0", "features": ["win_prediction", "competitor_intel", "shipping_monitor"]} if PREDICT_AVAILABLE else {"status": "not_available"},
    }
    try:
        from src.agents.product_research import get_research_cache_stats
        agents["product_research"] = get_research_cache_stats()
    except Exception as e:
        log.debug("Suppressed: %s", e)
        agents["product_research"] = {"status": "not_available"}

    return jsonify({"ok": True, "agents": agents,
                    "total": len(agents),
                    "active": sum(1 for a in agents.values() if a.get("status") != "not_available")})


@bp.route("/api/qa/scan")
@auth_required
def api_qa_scan():
    """Run full QA scan across all pages and source files."""
    if not QA_AVAILABLE:
        return jsonify({"ok": False, "error": "QA agent not available"})
    try:
        report = full_scan(current_app)
        return jsonify({"ok": True, **report})
    except Exception as e:
        log.exception("QA scan failed: %s", e)
        return jsonify({"ok": False, "error": str(e)})


# ─── Manager Brief Routes ───────────────────────────────────────────────────

@bp.route("/api/manager/brief")
@auth_required
def api_manager_brief():
    """Manager brief — everything you need to know right now."""
    if not MANAGER_AVAILABLE:
        return jsonify({"ok": False, "error": "Manager agent not available"})
    return jsonify({"ok": True, **generate_brief()})


@bp.route("/api/manager/metrics")
@auth_required
def api_manager_metrics():
    """Power BI-style metrics for dashboard KPIs."""
    if not MANAGER_AVAILABLE:
        return jsonify({"ok": False, "error": "Manager agent not available"})

    from datetime import timedelta
    from collections import defaultdict

    quotes = []
    try:
        qpath = os.path.join(DATA_DIR, "quotes_log.json")
        with open(qpath) as f:
            quotes = json.load(f)
    except Exception as e:
        log.debug("Suppressed: %s", e)
        pass

    pcs = _load_price_checks()
    now = datetime.now()

    # Revenue metrics
    won = [q for q in quotes if q.get("status") == "won"]
    lost = [q for q in quotes if q.get("status") == "lost"]
    pending = [q for q in quotes if q.get("status", "pending") == "pending"]
    total_revenue = sum(q.get("total", 0) for q in won)
    pipeline_value = sum(q.get("total", 0) for q in pending)

    # Monthly goal (configurable later, default $25K)
    monthly_goal = float(os.environ.get("MONTHLY_REVENUE_GOAL", "25000"))

    # This month's revenue
    month_start = now.replace(day=1, hour=0, minute=0, second=0)
    month_revenue = 0
    month_quotes = 0
    for q in won:
        ts = q.get("status_updated", q.get("created_at", ""))
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
                if dt >= month_start:
                    month_revenue += q.get("total", 0)
                    month_quotes += 1
            except (ValueError, TypeError):
                pass

    # Weekly quote volume (last 4 weeks)
    weekly_volume = []
    for w in range(4):
        week_end = now - timedelta(weeks=w)
        week_start = week_end - timedelta(weeks=1)
        count = 0
        value = 0
        for q in quotes:
            ts = q.get("created_at", q.get("generated_at", ""))
            if ts:
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
                    if week_start <= dt < week_end:
                        count += 1
                        value += q.get("total", 0)
                except (ValueError, TypeError):
                    pass
        label = f"Week {4-w}" if w > 0 else "This Week"
        weekly_volume.append({"label": label, "quotes": count, "value": round(value, 2)})
    weekly_volume.reverse()

    # Win rate trend (rolling - all time)
    decided = len(won) + len(lost)
    win_rate = round(len(won) / max(decided, 1) * 100)

    # Average deal size
    avg_deal = round(total_revenue / max(len(won), 1), 2)

    # Pipeline funnel
    pc_count = len(pcs) if isinstance(pcs, dict) else 0
    pc_parsed = sum(1 for p in (pcs.values() if isinstance(pcs, dict) else []) if p.get("status") == "parsed")
    pc_priced = sum(1 for p in (pcs.values() if isinstance(pcs, dict) else []) if p.get("status") == "priced")
    pc_completed = sum(1 for p in (pcs.values() if isinstance(pcs, dict) else []) if p.get("status") == "completed")

    # Response time (avg hours from PC upload to priced)
    response_times = []
    for pcid, pc in (pcs.items() if isinstance(pcs, dict) else []):
        history = pc.get("status_history", [])
        created_ts = None
        priced_ts = None
        for h in history:
            if h.get("to") == "parsed" and not created_ts:
                created_ts = h.get("timestamp")
            if h.get("to") == "priced" and not priced_ts:
                priced_ts = h.get("timestamp")
        if created_ts and priced_ts:
            try:
                c = datetime.fromisoformat(created_ts.replace("Z", "+00:00")).replace(tzinfo=None)
                p = datetime.fromisoformat(priced_ts.replace("Z", "+00:00")).replace(tzinfo=None)
                hours = (p - c).total_seconds() / 3600
                if 0 < hours < 720:  # Sanity: under 30 days
                    response_times.append(hours)
            except (ValueError, TypeError):
                pass
    avg_response = round(sum(response_times) / max(len(response_times), 1), 1)

    # Top institutions by revenue
    inst_rev = defaultdict(float)
    for q in won:
        inst_rev[q.get("institution", "Unknown")] += q.get("total", 0)
    top_institutions = sorted(inst_rev.items(), key=lambda x: x[1], reverse=True)[:5]

    return jsonify({
        "ok": True,
        "revenue": {
            "total": round(total_revenue, 2),
            "this_month": round(month_revenue, 2),
            "monthly_goal": monthly_goal,
            "goal_pct": round(month_revenue / max(monthly_goal, 1) * 100),
            "pipeline_value": round(pipeline_value, 2),
            "avg_deal": avg_deal,
        },
        "quotes": {
            "total": len(quotes),
            "won": len(won),
            "lost": len(lost),
            "pending": len(pending),
            "win_rate": win_rate,
            "this_month_won": month_quotes,
        },
        "funnel": {
            "pcs_total": pc_count,
            "parsed": pc_parsed,
            "priced": pc_priced,
            "completed": pc_completed,
            "quotes_generated": len(quotes),
            "quotes_won": len(won),
        },
        "weekly_volume": weekly_volume,
        "response_time_hours": avg_response,
        "top_institutions": [{"name": n, "revenue": round(v, 2)} for n, v in top_institutions],
    })


# ─── Orchestrator / Workflow Routes ──────────────────────────────────────────

@bp.route("/api/workflow/run", methods=["POST"])
@auth_required
def api_workflow_run():
    """Execute a named workflow pipeline."""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({"ok": False, "error": "Orchestrator not available"})
    data = request.json or {}
    name = data.get("workflow", "")
    inputs = data.get("inputs", {})
    if not name:
        return jsonify({"ok": False, "error": "Missing 'workflow' field"})
    result = run_workflow(name, inputs)
    return jsonify({"ok": not bool(result.get("error")), **result})


@bp.route("/api/workflow/status")
@auth_required
def api_workflow_status():
    """Orchestrator status and run history."""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({"ok": False, "error": "Orchestrator not available"})
    return jsonify({"ok": True, **get_workflow_status()})


@bp.route("/api/workflow/graph/<name>")
@auth_required
def api_workflow_graph(n):
    """Get workflow graph structure for visualization."""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({"ok": False, "error": "Orchestrator not available"})
    return jsonify({"ok": True, **get_workflow_graph_viz(n)})


# ─── SCPRS Scanner Routes ───────────────────────────────────────────────────

@bp.route("/api/scanner/start", methods=["POST"])
@auth_required
def api_scanner_start():
    """Start the SCPRS opportunity scanner."""
    if not SCANNER_AVAILABLE:
        return jsonify({"ok": False, "error": "Scanner not available"})
    data = request.get_json(silent=True) or {}
    interval = data.get("interval", 60)
    start_scanner(interval)
    return jsonify({"ok": True, "status": get_scanner_status()})


@bp.route("/api/scanner/stop", methods=["POST"])
@auth_required
def api_scanner_stop():
    """Stop the SCPRS opportunity scanner."""
    if not SCANNER_AVAILABLE:
        return jsonify({"ok": False, "error": "Scanner not available"})
    stop_scanner()
    return jsonify({"ok": True, "status": get_scanner_status()})


@bp.route("/api/scanner/scan", methods=["POST"])
@auth_required
def api_scanner_manual():
    """Run a single scan manually."""
    if not SCANNER_AVAILABLE:
        return jsonify({"ok": False, "error": "Scanner not available"})
    results = manual_scan()
    return jsonify({"ok": True, **results})


@bp.route("/api/scanner/status")
@auth_required
def api_scanner_status():
    """Get scanner status."""
    if not SCANNER_AVAILABLE:
        return jsonify({"ok": False, "error": "Scanner not available"})
    return jsonify({"ok": True, **get_scanner_status()})


# ─── QuickBooks Routes ──────────────────────────────────────────────────────

@bp.route("/api/qb/connect")
@auth_required
def api_qb_connect():
    """Start QuickBooks OAuth2 flow — redirects to Intuit login."""
    if not QB_AVAILABLE:
        return jsonify({"ok": False, "error": "QuickBooks agent not available"})
    from src.agents.quickbooks_agent import QB_CLIENT_ID, QB_SANDBOX
    if not QB_CLIENT_ID:
        return jsonify({"ok": False, "error": "Set QB_CLIENT_ID env var first"})
    # Build OAuth URL
    redirect_uri = request.url_root.rstrip("/") + "/api/qb/callback"
    scope = "com.intuit.quickbooks.accounting"
    auth_url = (
        f"https://appcenter.intuit.com/connect/oauth2?"
        f"client_id={QB_CLIENT_ID}&response_type=code&scope={scope}"
        f"&redirect_uri={redirect_uri}&state=reytech"
    )
    return redirect(auth_url)


@bp.route("/api/qb/callback")
def api_qb_callback():
    """QuickBooks OAuth2 callback — exchange code for tokens."""
    if not QB_AVAILABLE:
        flash("QuickBooks agent not available", "error")
        return redirect("/agents")
    code = request.args.get("code")
    realm_id = request.args.get("realmId")
    if not code:
        flash(f"QB OAuth failed: {request.args.get('error', 'no code')}", "error")
        return redirect("/agents")
    try:
        from src.agents.quickbooks_agent import (
            QB_CLIENT_ID, QB_CLIENT_SECRET, TOKEN_URL, _save_tokens
        )
        import base64 as _b64
        redirect_uri = request.url_root.rstrip("/") + "/api/qb/callback"
        auth = _b64.b64encode(f"{QB_CLIENT_ID}:{QB_CLIENT_SECRET}".encode()).decode()
        import requests as _req
        resp = _req.post(TOKEN_URL, headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        }, data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        import time as _time
        _save_tokens({
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "expires_at": _time.time() + data.get("expires_in", 3600),
            "realm_id": realm_id,
            "connected_at": datetime.now().isoformat(),
        })
        # Also save realm_id to env for future use
        os.environ["QB_REALM_ID"] = realm_id or ""
        flash(f"QuickBooks connected! Realm: {realm_id}", "success")
        _log_crm_activity("system", "qb_connected", f"QuickBooks Online connected (realm {realm_id})", actor="user")
    except Exception as e:
        flash(f"QB OAuth error: {e}", "error")
    return redirect("/agents")


@bp.route("/api/qb/status")
@auth_required
def api_qb_status():
    """QuickBooks connection status."""
    if not QB_AVAILABLE:
        return jsonify({"ok": False, "error": "QuickBooks agent not available"})
    return jsonify({"ok": True, **qb_agent_status()})


@bp.route("/api/qb/vendors")
@auth_required
def api_qb_vendors():
    """List QuickBooks vendors."""
    if not QB_AVAILABLE:
        return jsonify({"ok": False, "error": "QuickBooks agent not available"})
    if not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured. Set QB_CLIENT_ID, QB_CLIENT_SECRET, QB_REFRESH_TOKEN, QB_REALM_ID"})
    force = request.args.get("refresh", "").lower() in ("true", "1")
    vendors = fetch_vendors(force_refresh=force)
    return jsonify({"ok": True, "vendors": vendors, "count": len(vendors)})


@bp.route("/api/qb/vendors/find")
@auth_required
def api_qb_vendor_find():
    """Find a vendor by name. ?name=Amazon"""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    name = request.args.get("name", "")
    if not name:
        return jsonify({"ok": False, "error": "Provide ?name= parameter"})
    vendor = find_vendor(name)
    if vendor:
        return jsonify({"ok": True, "vendor": vendor})
    return jsonify({"ok": True, "vendor": None, "message": f"No vendor matching '{name}'"})


@bp.route("/api/qb/po/create", methods=["POST"])
@auth_required
def api_qb_create_po():
    """Create a Purchase Order in QuickBooks."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    data = request.get_json(silent=True) or {}
    vendor_id = data.get("vendor_id", "")
    items = data.get("items", [])
    if not vendor_id or not items:
        return jsonify({"ok": False, "error": "Provide vendor_id and items"})
    result = create_purchase_order(vendor_id, items,
                                   memo=data.get("memo", ""),
                                   ship_to=data.get("ship_to", ""))
    if result:
        return jsonify({"ok": True, "po": result})
    return jsonify({"ok": False, "error": "PO creation failed — check QB credentials"})


@bp.route("/api/qb/pos")
@auth_required
def api_qb_recent_pos():
    """Get recent Purchase Orders from QuickBooks."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    days = int(request.args.get("days", 30))
    pos = get_recent_purchase_orders(days_back=days)
    return jsonify({"ok": True, "purchase_orders": pos, "count": len(pos)})


@bp.route("/api/qb/invoices")
@auth_required
def api_qb_invoices():
    """Get invoices from QuickBooks. ?status=open|overdue|paid|all"""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    status = request.args.get("status", "all")
    force = request.args.get("refresh", "").lower() in ("true", "1")
    invoices = fetch_invoices(status=status, force_refresh=force)
    return jsonify({"ok": True, "invoices": invoices, "count": len(invoices)})


@bp.route("/api/qb/invoices/summary")
@auth_required
def api_qb_invoice_summary():
    """Get invoice metrics: open, overdue, paid counts and totals."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    return jsonify({"ok": True, **get_invoice_summary()})


@bp.route("/api/qb/invoices/create", methods=["POST"])
@auth_required
def api_qb_create_invoice():
    """Create an invoice in QuickBooks.
    POST: {customer_id, items: [{description, qty, unit_price}], po_number, memo}"""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    data = request.get_json(silent=True) or {}
    cid = data.get("customer_id", "")
    items = data.get("items", [])
    if not cid or not items:
        return jsonify({"ok": False, "error": "Provide customer_id and items"})
    result = create_invoice(cid, items, po_number=data.get("po_number", ""), memo=data.get("memo", ""))
    if result:
        return jsonify({"ok": True, "invoice": result})
    return jsonify({"ok": False, "error": "Invoice creation failed"})


@bp.route("/api/qb/customers")
@auth_required
def api_qb_customers():
    """List QuickBooks customers with balances."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    force = request.args.get("refresh", "").lower() in ("true", "1")
    customers = fetch_customers(force_refresh=force)
    return jsonify({"ok": True, "customers": customers, "count": len(customers)})


@bp.route("/api/qb/customers/balances")
@auth_required
def api_qb_customer_balances():
    """Customer balance summary: total AR, top balances."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    return jsonify({"ok": True, **get_customer_balance_summary()})


@bp.route("/api/qb/financial-context")
@auth_required
def api_qb_financial_context():
    """Comprehensive financial snapshot for all agents.
    Pulls invoices, customers, vendors — cached 1 hour."""
    if not QB_AVAILABLE or not qb_configured():
        return jsonify({"ok": False, "error": "QuickBooks not configured"})
    force = request.args.get("refresh", "").lower() in ("true", "1")
    ctx = get_financial_context(force_refresh=force)
    return jsonify(ctx)


# ─── CRM Activity Routes (Phase 16) ───────────────────────────────────────

@bp.route("/api/crm/activity")
@auth_required
def api_crm_activity():
    """Get CRM activity feed. ?ref_id=R26Q1&type=quote_won&institution=CSP&limit=50"""
    ref_id = request.args.get("ref_id")
    event_type = request.args.get("type")
    institution = request.args.get("institution")
    limit = int(request.args.get("limit", 50))
    activity = _get_crm_activity(ref_id=ref_id, event_type=event_type,
                                  institution=institution, limit=limit)
    return jsonify({"ok": True, "activity": activity, "count": len(activity)})


@bp.route("/api/crm/activity", methods=["POST"])
@auth_required
def api_crm_log_activity():
    """Manually log a CRM activity. POST JSON {ref_id, event_type, description}"""
    data = request.get_json(silent=True) or {}
    ref_id = data.get("ref_id", "")
    event_type = data.get("event_type", "note")
    description = data.get("description", "")
    if not description:
        return jsonify({"ok": False, "error": "description required"})
    _log_crm_activity(ref_id, event_type, description, actor="user",
                       metadata=data.get("metadata", {}))
    return jsonify({"ok": True})


@bp.route("/api/crm/agency/<agency_name>")
@auth_required
def api_crm_agency_summary(agency_name):
    """Agency CRM summary — quotes, win rate, recent activity, last contact."""
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"})

    quotes = get_all_quotes()
    agency_quotes = [q for q in quotes
                     if q.get("agency", "").lower() == agency_name.lower()
                     or q.get("institution", "").lower().startswith(agency_name.lower())]

    won = [q for q in agency_quotes if q.get("status") == "won"]
    lost = [q for q in agency_quotes if q.get("status") == "lost"]
    pending = [q for q in agency_quotes if q.get("status") in ("pending", "sent")]
    expired = [q for q in agency_quotes if q.get("status") == "expired"]

    total_won = sum(q.get("total", 0) for q in won)
    total_quoted = sum(q.get("total", 0) for q in agency_quotes)
    decided = len(won) + len(lost)
    win_rate = round(len(won) / decided * 100, 1) if decided else 0

    # Unique institutions
    institutions = list(set(q.get("institution", "") for q in agency_quotes if q.get("institution")))

    # Recent activity for this agency
    activity = _get_crm_activity(institution=agency_name, limit=20)

    # Last contact date
    last_contact = None
    for a in activity:
        if a.get("event_type") in ("email_sent", "voice_call", "quote_sent"):
            last_contact = a.get("timestamp")
            break

    return jsonify({
        "ok": True,
        "agency": agency_name,
        "total_quotes": len(agency_quotes),
        "won": len(won), "lost": len(lost),
        "pending": len(pending), "expired": len(expired),
        "total_won_value": total_won,
        "total_quoted_value": total_quoted,
        "win_rate": win_rate,
        "institutions": sorted(institutions),
        "last_contact": last_contact,
        "recent_activity": activity[:10],
    })


# ─── Lead Generation Routes ─────────────────────────────────────────────────

@bp.route("/api/predict/win")
@auth_required
def api_predict_win():
    """Predict win probability for a bid.
    ?institution=CSP-Sacramento&agency=CDCR&value=5000"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive intel not available"})
    result = predict_win_probability(
        institution=request.args.get("institution", ""),
        agency=request.args.get("agency", ""),
        category=request.args.get("category", ""),
        po_value=float(request.args.get("value", 0)),
    )
    return jsonify({"ok": True, **result})


@bp.route("/api/competitor/insights")
@bp.route("/api/intel/competitors")
@auth_required
def api_competitor_insights():
    """Get competitor intelligence summary.
    ?institution=CSP-Sacramento&agency=CDCR&limit=20"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive intel not available"})
    result = get_competitor_insights(
        institution=request.args.get("institution", ""),
        agency=request.args.get("agency", ""),
        limit=int(request.args.get("limit", 20)),
    )
    return jsonify({"ok": True, **result})


@bp.route("/api/shipping/detect", methods=["POST"])
@bp.route("/api/shipping/scan-email", methods=["POST"])
@auth_required
def api_shipping_detect():
    """Test shipping email detection. POST: {subject, body, sender}"""
    if not PREDICT_AVAILABLE:
        return jsonify({"ok": False, "error": "Predictive intel not available"})
    data = request.get_json(silent=True) or {}
    result = detect_shipping_email(
        subject=data.get("subject", ""),
        body=data.get("body", ""),
        sender=data.get("sender", ""),
    )
    # If shipping detected, try to match to order
    if result.get("is_shipping"):
        orders = _load_orders()
        matched_oid = match_tracking_to_order(result, orders)
        result["matched_order"] = matched_oid
        if matched_oid and data.get("auto_update"):
            update_result = update_order_from_tracking(matched_oid, result, orders)
            if update_result.get("updated_items"):
                _save_orders(orders)
                _update_order_status(matched_oid)
                _log_crm_activity(matched_oid, "tracking_auto_detected",
                                  f"Shipping detected from {data.get('sender','')}: "
                                  f"{result.get('carrier','')} {', '.join(result.get('tracking_numbers',[])[:2])}",
                                  actor="system", metadata=result)
            result["auto_update_result"] = update_result
    return jsonify({"ok": True, **result})


@bp.route("/api/shipping/detected")
@auth_required
def api_shipping_detected():
    """Get recently detected shipping emails."""
    ship_file = os.path.join(DATA_DIR, "detected_shipments.json")
    try:
        with open(ship_file) as f:
            shipments = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        shipments = []
    limit = int(request.args.get("limit", 20))
    shipments = sorted(shipments, key=lambda s: s.get("detected_at", ""), reverse=True)[:limit]
    return jsonify({"ok": True, "shipments": shipments, "count": len(shipments)})


@bp.route("/api/funnel/stats")
@auth_required
def api_funnel_stats():
    """Pipeline funnel stats — aggregated view of the full business pipeline."""
    # RFQs
    rfqs = load_rfqs()
    rfqs_active = sum(1 for r in rfqs.values() if r.get("status") not in ("completed", "won", "lost"))

    # Quotes
    quotes = get_all_quotes()
    quotes_pending = sum(1 for q in quotes if q.get("status") in ("pending", "draft"))
    quotes_sent = sum(1 for q in quotes if q.get("status") == "sent")
    quotes_won = sum(1 for q in quotes if q.get("status") == "won")
    quotes_lost = sum(1 for q in quotes if q.get("status") == "lost")
    total_quoted = sum(q.get("total", 0) for q in quotes)
    total_won = sum(q.get("total", 0) for q in quotes if q.get("status") == "won")

    # Orders
    orders = _load_orders()
    orders_active = sum(1 for o in orders.values() if o.get("status") not in ("closed",))
    orders_total = len(orders)
    items_shipped = 0
    items_delivered = 0
    for o in orders.values():
        for it in o.get("line_items", []):
            if it.get("sourcing_status") in ("shipped", "delivered"):
                items_shipped += 1
            if it.get("sourcing_status") == "delivered":
                items_delivered += 1
    order_value = sum(o.get("total", 0) for o in orders.values())
    invoiced_value = sum(o.get("invoice_total", 0) for o in orders.values())

    # Leads
    try:
        with open(os.path.join(DATA_DIR, "leads.json")) as f:
            leads = json.load(f)
        leads_count = len(leads) if isinstance(leads, list) else 0
        hot_leads = sum(1 for l in (leads if isinstance(leads, list) else [])
                        if isinstance(l, dict) and l.get("score", 0) >= 0.7)
    except (FileNotFoundError, json.JSONDecodeError):
        leads_count = 0
        hot_leads = 0

    # Win rate
    decided = quotes_won + quotes_lost
    win_rate = round(quotes_won / decided * 100) if decided > 0 else 0

    # Pipeline value = pending + sent quote totals
    pipeline_value = sum(q.get("total", 0) for q in quotes
                         if q.get("status") in ("pending", "sent", "draft"))

    # QuickBooks financial data
    qb_receivable = 0
    qb_overdue = 0
    qb_collected = 0
    qb_open_invoices = 0
    if QB_AVAILABLE and qb_configured():
        try:
            ctx = get_financial_context()
            if ctx.get("ok"):
                qb_receivable = ctx.get("total_receivable", 0)
                qb_overdue = ctx.get("overdue_amount", 0)
                qb_collected = ctx.get("total_collected", 0)
                qb_open_invoices = ctx.get("open_invoices", 0)
        except Exception:
            pass

    return jsonify({
        "ok": True,
        "rfqs_active": rfqs_active,
        "quotes_pending": quotes_pending,
        "quotes_sent": quotes_sent,
        "quotes_won": quotes_won,
        "quotes_lost": quotes_lost,
        "orders_active": orders_active,
        "orders_total": orders_total,
        "items_shipped": items_shipped,
        "items_delivered": items_delivered,
        "leads_count": leads_count,
        "hot_leads": hot_leads,
        "total_quoted": total_quoted,
        "total_won": total_won,
        "pipeline_value": pipeline_value,
        "order_value": order_value,
        "invoiced_value": invoiced_value,
        "win_rate": win_rate,
        "qb_receivable": qb_receivable,
        "qb_overdue": qb_overdue,
        "qb_collected": qb_collected,
        "qb_open_invoices": qb_open_invoices,
    })


@bp.route("/api/leads")
@auth_required
def api_leads_list():
    """Get leads, optionally filtered. ?status=new&min_score=0.6&limit=20"""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    status = request.args.get("status")
    min_score = float(request.args.get("min_score", 0))
    limit = int(request.args.get("limit", 50))
    leads = get_leads(status=status, min_score=min_score, limit=limit)
    return jsonify({"ok": True, "leads": leads, "count": len(leads)})


@bp.route("/api/leads/evaluate", methods=["POST"])
@auth_required
def api_leads_evaluate():
    """Evaluate a PO as a potential lead. POST JSON with PO data."""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    data = request.get_json(silent=True) or {}
    # Load won history for matching
    won_history = []
    try:
        from src.knowledge.won_quotes_db import get_all_items
        won_history = get_all_items()
    except Exception as e:
        log.debug("Suppressed: %s", e)
        pass
    lead = evaluate_po(data, won_history)
    if not lead:
        return jsonify({"ok": True, "qualified": False,
                        "reason": "Below confidence threshold or out of value range"})
    result = add_lead(lead)
    return jsonify({"ok": True, "qualified": True, "lead": lead, **result})


@bp.route("/api/leads/<lead_id>/status", methods=["POST"])
@auth_required
def api_leads_update_status(lead_id):
    """Update lead status. POST JSON: {"status": "contacted", "notes": "..."}"""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    data = request.get_json(silent=True) or {}
    return jsonify(update_lead_status(
        lead_id, data.get("status", ""), data.get("notes", "")))


@bp.route("/api/leads/<lead_id>/draft")
@auth_required
def api_leads_draft(lead_id):
    """Get outreach email draft for a lead."""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    leads = get_leads()
    lead = next((l for l in leads if l["id"] == lead_id), None)
    if not lead:
        return jsonify({"ok": False, "error": "Lead not found"})
    draft = draft_outreach_email(lead)
    return jsonify({"ok": True, **draft})


@bp.route("/api/leads/analytics")
@auth_required
def api_leads_analytics():
    """Lead conversion analytics."""
    if not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Lead gen agent not available"})
    return jsonify({"ok": True, **get_lead_analytics()})


# ─── Email Outreach Routes ──────────────────────────────────────────────────

@bp.route("/api/outbox")
@auth_required
def api_outbox_list():
    """Get email outbox. ?status=draft"""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    status = request.args.get("status")
    emails = get_outbox(status=status)
    return jsonify({"ok": True, "emails": emails, "count": len(emails)})


@bp.route("/api/outbox/draft/pc/<pcid>", methods=["POST"])
@auth_required
def api_outbox_draft_pc(pcid):
    """Draft a buyer email for a completed PC."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(silent=True) or {}
    email = draft_for_pc(pc,
                         quote_number=data.get("quote_number", pc.get("quote_number", "")),
                         pdf_path=data.get("pdf_path", ""))
    return jsonify({"ok": True, "email": email})


@bp.route("/api/outbox/draft/lead/<lead_id>", methods=["POST"])
@auth_required
def api_outbox_draft_lead(lead_id):
    """Draft outreach email for a lead."""
    if not OUTREACH_AVAILABLE or not LEADGEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Required agents not available"})
    leads = get_leads()
    lead = next((l for l in leads if l["id"] == lead_id), None)
    if not lead:
        return jsonify({"ok": False, "error": "Lead not found"})
    email = draft_for_lead(lead)
    return jsonify({"ok": True, "email": email})


@bp.route("/api/outbox/<email_id>/approve", methods=["POST"])
@auth_required
def api_outbox_approve(email_id):
    """Approve a draft email for sending."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify(approve_email(email_id))


@bp.route("/api/outbox/<email_id>/edit", methods=["POST"])
@auth_required
def api_outbox_edit(email_id):
    """Edit a draft. POST JSON: {"to": "...", "subject": "...", "body": "..."}"""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    data = request.get_json(silent=True) or {}
    return jsonify(update_draft(email_id, data))


@bp.route("/api/outbox/<email_id>/send", methods=["POST"])
@auth_required
def api_outbox_send(email_id):
    """Send a specific email."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify(outreach_send(email_id))


@bp.route("/api/outbox/send-approved", methods=["POST"])
@auth_required
def api_outbox_send_all():
    """Send all approved emails."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify({"ok": True, **send_approved()})


@bp.route("/api/outbox/<email_id>", methods=["DELETE"])
@auth_required
def api_outbox_delete(email_id):
    """Delete an email from outbox."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    return jsonify(delete_from_outbox(email_id))


@bp.route("/api/outbox/sent")
@auth_required
def api_outbox_sent_log():
    """Get sent email log."""
    if not OUTREACH_AVAILABLE:
        return jsonify({"ok": False, "error": "Email outreach agent not available"})
    limit = int(request.args.get("limit", 50))
    return jsonify({"ok": True, "sent": get_sent_log(limit=limit)})


# ─── Growth Strategy Routes ─────────────────────────────────────────────────

@bp.route("/api/growth/report")
@auth_required
def api_growth_report():
    """Full growth strategy report."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify({"ok": True, **full_report()})


@bp.route("/api/growth/win-loss")
@auth_required
def api_growth_win_loss():
    """Win/loss analysis breakdown."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify({"ok": True, **win_loss_analysis()})


@bp.route("/api/growth/pricing")
@auth_required
def api_growth_pricing():
    """Pricing intelligence analysis."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify({"ok": True, **pricing_analysis()})


@bp.route("/api/growth/pipeline")
@auth_required
def api_growth_pipeline():
    """Pipeline health analysis."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    return jsonify({"ok": True, **pipeline_health()})


@bp.route("/api/growth/recommendations")
@auth_required
def api_growth_recommendations():
    """Actionable recommendations."""
    if not GROWTH_AVAILABLE:
        return jsonify({"ok": False, "error": "Growth agent not available"})
    recs = generate_recommendations()
    return jsonify({"ok": True, "recommendations": recs, "count": len(recs)})


# ─── Voice Agent Routes ─────────────────────────────────────────────────────

@bp.route("/api/voice/call", methods=["POST"])
@auth_required
def api_voice_call():
    """Place an outbound call. POST JSON: {"phone": "+19165550100", "script": "lead_intro", "variables": {...}}"""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    data = request.get_json(silent=True) or {}
    phone = data.get("phone", "")
    if not phone:
        return jsonify({"ok": False, "error": "Provide phone number in E.164 format"})
    # Inject server URL for Vapi function calling webhook
    variables = data.get("variables", {})
    variables["server_url"] = request.url_root.rstrip("/") + "/api/voice/webhook"
    result = place_call(phone, script_key=data.get("script", "lead_intro"),
                        variables=variables)
    # CRM: log call
    ref_id = data.get("variables", {}).get("quote_number", "") or data.get("variables", {}).get("po_number", "")
    _log_crm_activity(ref_id or "outbound", "voice_call",
                      f"Outbound call to {phone} ({data.get('script','lead_intro')})" +
                      (" — " + result.get("call_sid", "") if result.get("ok") else " — FAILED"),
                      actor="user", metadata={"phone": phone, "script": data.get("script",""),
                                               "institution": data.get("variables",{}).get("institution","")})
    return jsonify(result)


@bp.route("/api/voice/log")
@auth_required
def api_voice_log():
    """Get call log."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    limit = int(request.args.get("limit", 50))
    return jsonify({"ok": True, "calls": get_call_log(limit=limit)})


@bp.route("/api/voice/scripts")
@auth_required
def api_voice_scripts():
    """Get available call scripts."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    return jsonify({"ok": True, "scripts": VOICE_SCRIPTS})


@bp.route("/api/voice/status")
@auth_required
def api_voice_status():
    """Voice agent status + setup instructions."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    return jsonify({"ok": True, **voice_agent_status()})


@bp.route("/api/voice/verify")
@auth_required
def api_voice_verify():
    """Verify Twilio credentials are valid by pinging the API."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    return jsonify(voice_verify())


@bp.route("/api/voice/import-twilio", methods=["POST"])
@auth_required
def api_voice_import_twilio():
    """Import Twilio phone number into Vapi for Reytech caller ID."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    return jsonify(import_twilio_to_vapi())


@bp.route("/api/voice/webhook", methods=["POST"])
def api_voice_vapi_webhook():
    """Vapi server URL webhook — handles function calls during live conversations.
    No auth required — Vapi calls this endpoint during active calls."""
    data = request.get_json(silent=True) or {}
    msg_type = data.get("message", {}).get("type", "")

    if msg_type == "function-call":
        fn = data.get("message", {}).get("functionCall", {})
        fn_name = fn.get("name", "")
        fn_params = fn.get("parameters", {})

        try:
            from src.agents.voice_knowledge import handle_tool_call
            result = handle_tool_call(fn_name, fn_params)
            return jsonify({"results": [{"result": result}]})
        except Exception as e:
            log.error("Vapi webhook tool call failed: %s", e)
            return jsonify({"results": [{"result": "I couldn't look that up right now."}]})

    elif msg_type == "end-of-call-report":
        # Log transcript to CRM
        call = data.get("message", {}).get("call", {})
        transcript = data.get("message", {}).get("transcript", "")
        summary = data.get("message", {}).get("summary", "")
        call_id = call.get("id", "")
        phone = call.get("customer", {}).get("number", "")

        if call_id:
            _log_crm_activity(call_id, "voice_call_completed",
                              f"Call to {phone} completed" + (f" — {summary[:200]}" if summary else ""),
                              actor="system", metadata={
                                  "call_id": call_id,
                                  "phone": phone,
                                  "transcript": transcript[:2000] if transcript else "",
                                  "summary": summary[:500] if summary else "",
                                  "duration": data.get("message", {}).get("durationSeconds", 0),
                              })
        return jsonify({"ok": True})

    return jsonify({"ok": True})


@bp.route("/api/voice/vapi-calls")
@auth_required
def api_voice_vapi_calls():
    """List recent Vapi calls with transcripts."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    limit = int(request.args.get("limit", 20))
    calls = get_vapi_calls(limit=limit)
    return jsonify({"ok": True, "calls": calls, "count": len(calls)})


@bp.route("/api/voice/call/<call_id>/details")
@auth_required
def api_voice_call_details(call_id):
    """Get Vapi call details including transcript."""
    if not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice agent not available"})
    details = get_vapi_call_details(call_id)
    return jsonify({"ok": not bool(details.get("error")), **details})


# ─── Campaign Routes ────────────────────────────────────────────────────────

@bp.route("/campaigns")
@auth_required
def campaigns_page():
    """Campaigns management page."""
    campaigns = get_campaigns() if CAMPAIGNS_AVAILABLE else []
    stats = get_campaign_stats() if CAMPAIGNS_AVAILABLE else {}
    scripts = list(VOICE_SCRIPTS.items()) if VOICE_AVAILABLE else []

    # Script options for dropdowns
    script_options = ""
    for key, sc in scripts:
        cat = sc.get("category", "other")
        script_options += f'<option value="{key}">[{cat}] {sc["name"]}</option>'

    # Source options
    source_options = """
    <option value="manual">Manual (add contacts)</option>
    <option value="hot_leads">🔥 Hot Leads (score ≥ 70%)</option>
    <option value="pending_quotes">📋 Pending Quotes (follow-up)</option>
    <option value="lost_quotes">❌ Lost Quotes (recovery)</option>
    <option value="won_customers">✅ Won Customers (thank you)</option>
    <option value="dormant">💤 Dormant Accounts (reactivation)</option>
    """

    # Campaign rows
    camp_rows = ""
    for c in campaigns[:20]:
        st = c.get("status", "draft")
        st_color = {"draft": "var(--tx2)", "active": "var(--gn)", "paused": "var(--yl)", "completed": "var(--ac)"}
        called = c["stats"]["called"]
        total = c["stats"]["total"]
        reached = c["stats"]["reached"]
        pct = round(called / total * 100) if total > 0 else 0
        camp_rows += f"""<tr>
         <td><a href="/campaign/{c['id']}" style="color:var(--ac);text-decoration:none;font-weight:600">{c['name']}</a></td>
         <td style="color:{st_color.get(st,'var(--tx2)')};font-weight:600">{st}</td>
         <td>{c.get('script_key','?')}</td>
         <td style="text-align:center">{total}</td>
         <td style="text-align:center">{called}/{total} ({pct}%)</td>
         <td style="text-align:center">{reached}</td>
         <td class="mono" style="font-size:11px">{c.get('created_at','')[:10]}</td>
        </tr>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
     <h1 style="margin:0">📞 Voice Campaigns</h1>
     <button class="btn btn-p" onclick="document.getElementById('new-camp').style.display='block'" style="padding:8px 16px">+ New Campaign</button>
    </div>

    <!-- Stats bar -->
    <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:16px">
     <div class="card" style="text-align:center;padding:12px"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Campaigns</div><div style="font-size:24px;font-weight:700">{stats.get('total_campaigns',0)}</div></div>
     <div class="card" style="text-align:center;padding:12px"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Total Calls</div><div style="font-size:24px;font-weight:700">{stats.get('total_called',0)}</div></div>
     <div class="card" style="text-align:center;padding:12px"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Connect Rate</div><div style="font-size:24px;font-weight:700;color:var(--gn)">{stats.get('connect_rate',0)}%</div></div>
     <div class="card" style="text-align:center;padding:12px"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Interested</div><div style="font-size:24px;font-weight:700;color:var(--ac)">{stats.get('total_interested',0)}</div></div>
     <div class="card" style="text-align:center;padding:12px"><div style="font-size:10px;color:var(--tx2);text-transform:uppercase">Est. Cost</div><div style="font-size:24px;font-weight:700">${stats.get('estimated_cost',0):.2f}</div></div>
    </div>

    <!-- New Campaign Form (hidden) -->
    <div id="new-camp" class="card" style="display:none;margin-bottom:16px;padding:16px">
     <div class="card-t" style="margin-bottom:12px">New Campaign</div>
     <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
      <div>
       <label style="font-size:11px;color:var(--tx2)">Campaign Name</label>
       <input id="camp-name" placeholder="Feb CDCR Outreach" style="width:100%;padding:8px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);margin-top:4px">
      </div>
      <div>
       <label style="font-size:11px;color:var(--tx2)">Contact Source</label>
       <select id="camp-source" style="width:100%;padding:8px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);margin-top:4px">{source_options}</select>
      </div>
      <div>
       <label style="font-size:11px;color:var(--tx2)">Default Script</label>
       <select id="camp-script" style="width:100%;padding:8px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);margin-top:4px">{script_options}</select>
      </div>
      <div>
       <label style="font-size:11px;color:var(--tx2)">Filter (agency)</label>
       <input id="camp-filter" placeholder="CDCR, CCHCS, etc." style="width:100%;padding:8px;background:var(--sf);border:1px solid var(--bd);border-radius:6px;color:var(--tx);margin-top:4px">
      </div>
     </div>
     <div style="margin-top:12px;display:flex;gap:8px">
      <button class="btn btn-p" onclick="createCampaign()" style="padding:8px 20px">Create Campaign</button>
      <button class="btn" onclick="document.getElementById('new-camp').style.display='none'" style="padding:8px 20px">Cancel</button>
     </div>
    </div>

    <!-- Available Scripts -->
    <div class="card" style="margin-bottom:16px;padding:16px">
     <div class="card-t" style="margin-bottom:10px">📜 {len(scripts)} Call Scripts Available</div>
     <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:8px">
      {''.join(f'<div style="padding:8px;background:var(--sf2);border-radius:8px;font-size:12px"><span style="color:var(--ac);font-weight:600">{sc["name"]}</span><br><span style="color:var(--tx2);font-size:10px">[{sc.get("category","?")}] {key}</span></div>' for key, sc in scripts)}
     </div>
    </div>

    <!-- Campaigns Table -->
    <div class="card" style="padding:16px">
     <div class="card-t" style="margin-bottom:10px">Campaigns</div>
     <table class="tbl" style="width:100%">
      <thead><tr>
       <th>Campaign</th><th>Status</th><th>Script</th><th>Contacts</th><th>Progress</th><th>Reached</th><th>Created</th>
      </tr></thead>
      <tbody>{camp_rows if camp_rows else '<tr><td colspan="7" style="text-align:center;color:var(--tx2);padding:20px">No campaigns yet — create one above</td></tr>'}</tbody>
     </table>
    </div>

    <script>
    function createCampaign() {{
      const name = document.getElementById('camp-name').value;
      if (!name) {{ alert('Enter a campaign name'); return; }}
      const source = document.getElementById('camp-source').value;
      const script = document.getElementById('camp-script').value;
      const agency = document.getElementById('camp-filter').value;
      fetch('/api/campaigns', {{
        method: 'POST', credentials: 'same-origin',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{name, script_key: script, target_type: source, filters: {{agency: agency}}}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{ location.reload(); }} else {{ alert(d.error || 'Failed'); }}
      }});
    }}
    </script>
    """
    return render(content, title="Voice Campaigns")


@bp.route("/campaign/<cid>")
@auth_required
def campaign_detail(cid):
    """Campaign detail page with contact list and dialer."""
    if not CAMPAIGNS_AVAILABLE:
        return redirect("/campaigns")
    camp = get_campaign(cid)
    if not camp:
        flash("Campaign not found", "error")
        return redirect("/campaigns")

    contacts = camp.get("contacts", [])
    stats = camp.get("stats", {})
    pending = [c for c in contacts if c.get("status") == "pending"]
    called = [c for c in contacts if c.get("status") == "called"]

    # Contact rows
    contact_rows = ""
    for i, c in enumerate(contacts):
        outcome = c.get("outcome", "")
        outcome_color = {"reached": "var(--gn)", "voicemail": "var(--yl)", "interested": "var(--ac)",
                         "no_answer": "var(--tx2)", "callback": "var(--warn)", "not_interested": "var(--rd)"}.get(outcome, "var(--tx2)")
        phone = c.get("phone", "")
        dial_btn = f'<button class="btn btn-sm" onclick="dialContact({i})" style="background:rgba(52,211,153,.15);color:var(--gn);border:1px solid rgba(52,211,153,.3);padding:2px 8px;font-size:10px">📞 Dial</button>' if c["status"] == "pending" and phone else ""
        outcome_btn = f'<select onchange="logOutcome(\'{phone}\',this.value)" style="font-size:10px;padding:2px;background:var(--sf);border:1px solid var(--bd);border-radius:4px;color:var(--tx)"><option value="">Log outcome...</option><option value="reached">✅ Reached</option><option value="voicemail">📱 Voicemail</option><option value="no_answer">❌ No Answer</option><option value="callback">📞 Callback</option><option value="interested">🎯 Interested</option><option value="not_interested">👎 Not Interested</option><option value="gatekeeper">🚪 Gatekeeper</option></select>' if c["status"] == "pending" or (c["status"] == "called" and not outcome) else ""

        contact_rows += f"""<tr>
         <td style="font-weight:500">{c.get('name','?')}</td>
         <td class="mono" style="font-size:11px">{phone or '<span style=\"color:var(--rd)\">no phone</span>'}</td>
         <td style="font-size:11px">{c.get('institution','')}</td>
         <td style="font-size:11px">{c.get('script', camp.get('script_key',''))}</td>
         <td style="text-align:center"><span style="color:{outcome_color};font-weight:600;font-size:11px">{outcome or c.get('status','')}</span></td>
         <td style="text-align:center;white-space:nowrap">{dial_btn} {outcome_btn}</td>
        </tr>"""

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
     <div>
      <h1 style="margin:0">{camp['name']}</h1>
      <div style="color:var(--tx2);font-size:12px;margin-top:4px">{camp.get('contact_source', camp.get('target_type',''))} • {camp.get('script_key','')} • {len(contacts)} contacts</div>
     </div>
     <div style="display:flex;gap:8px">
      <a href="/campaigns" class="btn" style="padding:8px 16px">← Back</a>
      <button class="btn btn-p" onclick="dialNext()" style="padding:8px 16px" {'disabled' if not pending else ''}>📞 Dial Next ({len(pending)} remaining)</button>
     </div>
    </div>

    <!-- Stats -->
    <div style="display:grid;grid-template-columns:repeat(6,1fr);gap:8px;margin-bottom:16px">
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Total</div><div style="font-size:20px;font-weight:700">{stats.get('total',0)}</div></div>
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Called</div><div style="font-size:20px;font-weight:700">{stats.get('called',0)}</div></div>
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Reached</div><div style="font-size:20px;font-weight:700;color:var(--gn)">{stats.get('reached',0)}</div></div>
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Voicemail</div><div style="font-size:20px;font-weight:700;color:var(--yl)">{stats.get('voicemail',0)}</div></div>
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Interested</div><div style="font-size:20px;font-weight:700;color:var(--ac)">{stats.get('interested',0)}</div></div>
     <div class="card" style="text-align:center;padding:10px"><div style="font-size:9px;color:var(--tx2);text-transform:uppercase">Callback</div><div style="font-size:20px;font-weight:700;color:var(--warn)">{stats.get('callback',0)}</div></div>
    </div>

    <!-- Contact List -->
    <div class="card" style="padding:16px">
     <div class="card-t" style="margin-bottom:10px">Contact List</div>
     <table class="tbl" style="width:100%">
      <thead><tr><th>Name</th><th>Phone</th><th>Institution</th><th>Script</th><th>Outcome</th><th>Actions</th></tr></thead>
      <tbody>{contact_rows}</tbody>
     </table>
    </div>

    <script>
    function dialContact(idx) {{
      fetch('/api/campaigns/{cid}/call', {{
        method: 'POST', credentials: 'same-origin',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{target_index: idx}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{ alert('Call placed: ' + (d.call_id||d.call_sid||'queued')); location.reload(); }}
        else {{ alert(d.error || 'Call failed'); }}
      }});
    }}
    function dialNext() {{
      fetch('/api/campaigns/{cid}/call', {{
        method: 'POST', credentials: 'same-origin',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) {{ alert('Calling: ' + (d.to||'next contact')); location.reload(); }}
        else {{ alert(d.error || 'No more contacts'); }}
      }});
    }}
    function logOutcome(phone, outcome) {{
      if (!outcome) return;
      fetch('/api/campaigns/{cid}/outcome', {{
        method: 'POST', credentials: 'same-origin',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{phone, outcome}})
      }}).then(r => r.json()).then(d => {{
        if (d.ok) location.reload();
      }});
    }}
    </script>
    """
    return render(content, title=f"Campaign: {camp['name']}")


@bp.route("/api/campaigns", methods=["GET", "POST"])
@auth_required
def api_campaigns():
    """List or create campaigns."""
    if not CAMPAIGNS_AVAILABLE:
        return jsonify({"ok": False, "error": "Campaigns not available"})
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        result = create_campaign(
            name=data.get("name", "Untitled"),
            script_key=data.get("script_key", "lead_intro"),
            target_type=data.get("target_type", "manual"),
            filters=data.get("filters", {}),
        )
        return jsonify({"ok": True, **result})
    return jsonify({"ok": True, "campaigns": get_campaigns()})


@bp.route("/api/campaigns/<cid>/call", methods=["POST"])
@auth_required
def api_campaign_call(cid):
    """Execute next call in campaign."""
    if not CAMPAIGNS_AVAILABLE or not VOICE_AVAILABLE:
        return jsonify({"ok": False, "error": "Voice/campaigns not available"})
    data = request.get_json(silent=True) or {}
    target_index = data.get("target_index")
    result = execute_campaign_call(cid, target_index=target_index)
    return jsonify(result)


@bp.route("/api/campaigns/<cid>/outcome", methods=["POST"])
@auth_required
def api_campaign_outcome(cid):
    """Log call outcome for a campaign contact."""
    if not CAMPAIGNS_AVAILABLE:
        return jsonify({"ok": False, "error": "Campaigns not available"})
    data = request.get_json(silent=True) or {}
    result = update_call_outcome(cid, phone=data.get("phone", ""), outcome=data.get("outcome", ""))
    return jsonify(result)


@bp.route("/api/campaigns/<cid>")
@auth_required
def api_campaign_detail(cid):
    """Get campaign details."""
    if not CAMPAIGNS_AVAILABLE:
        return jsonify({"ok": False, "error": "Campaigns not available"})
    camp = get_campaign(cid)
    if not camp:
        return jsonify({"ok": False, "error": "Not found"})
    return jsonify({"ok": True, **camp})


@bp.route("/api/campaigns/stats")
@auth_required
def api_campaign_stats():
    """Aggregate campaign analytics."""
    if not CAMPAIGNS_AVAILABLE:
        return jsonify({"ok": False, "error": "Campaigns not available"})
    return jsonify({"ok": True, **get_campaign_stats()})


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
        from src.forms.quote_generator import _save_all_quotes, _detect_agency

        # Fix DEFAULT agencies using all available data
        agencies_fixed = 0
        for q in clean:
            if q.get("agency", "DEFAULT") == "DEFAULT":
                detected = _detect_agency(q)
                if detected != "DEFAULT":
                    q["agency"] = detected
                    agencies_fixed += 1
        report["agencies_fixed"] = agencies_fixed

        _save_all_quotes(clean)

        # Reset counter
        set_quote_counter(max_num)

        log.info("CLEANUP: %d → %d quotes (%d duplicates removed, %d agencies fixed). Counter → %d. Backup: %s",
                 original_count, len(clean), removed, agencies_fixed, max_num, backup_name)
        report["message"] = f"Done. {removed} duplicates removed, {agencies_fixed} agencies fixed. Counter reset to {max_num}. Backup: {backup_name}"
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
