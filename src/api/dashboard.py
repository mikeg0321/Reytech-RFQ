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
# ── Dashboard Notifications (Feature 4.2 auto-draft alerts) ──────────────────
import collections as _collections
_notifications = _collections.deque(maxlen=20)

def _push_notification(notif: dict):
    notif.setdefault("ts", datetime.now().isoformat())
    notif.setdefault("read", False)
    _notifications.appendleft(notif)
    log.info("Notification: [%s] %s", notif.get("type",""), notif.get("title",""))



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

# ── Thread-safe locks for all mutated global state ──────────────────────────
_poll_status_lock   = threading.Lock()
_rate_limiter_lock  = threading.Lock()
_json_cache_lock    = threading.Lock()

# ── TTL JSON cache — eliminates redundant disk reads on hot routes ───────────
_json_cache: dict = {}   # path → {"data": ..., "mtime": float, "ts": float}
_JSON_CACHE_TTL = 2.0    # seconds — balance freshness vs perf

def _cached_json_load(path: str, fallback=None):
    """Load JSON with mtime-aware caching. Avoids re-reading unchanged files.
    Falls back to `fallback` on missing/corrupt file.
    Thread-safe via _json_cache_lock.
    """
    if fallback is None:
        fallback = {}
    if not os.path.exists(path):
        return fallback
    try:
        mtime = os.path.getmtime(path)
        with _json_cache_lock:
            cached = _json_cache.get(path)
            if cached and cached["mtime"] == mtime:
                return cached["data"]
        with open(path) as f:
            data = json.load(f)
        with _json_cache_lock:
            _json_cache[path] = {"data": data, "mtime": mtime, "ts": time.time()}
            # Evict oldest entries if cache grows large
            if len(_json_cache) > 50:
                oldest = sorted(_json_cache.items(), key=lambda x: x[1]["ts"])[:10]
                for k, _ in oldest:
                    del _json_cache[k]
        return data
    except (json.JSONDecodeError, OSError):
        return fallback

def _invalidate_cache(path: str):
    """Call after writing a JSON file to evict stale cache entry."""
    with _json_cache_lock:
        _json_cache.pop(path, None)

def _pst_now_iso():
    """Return current PST datetime as ISO string (JS-parseable with time)."""
    pst = timezone(timedelta(hours=-8))
    return datetime.now(pst).isoformat()

# ═══════════════════════════════════════════════════════════════════════
# Password Protection
# ═══════════════════════════════════════════════════════════════════════

DASH_USER = os.environ.get("DASH_USER", "reytech")
DASH_PASS = os.environ.get("DASH_PASS", "changeme")


# ── Security: Path validation utilities ──────────────────────────────────────
import re as _re

def _safe_filename(filename: str) -> str:
    """
    Sanitize a user-provided filename.
    - Strips path components (directory traversal)
    - Removes non-alphanumeric chars except . - _
    - Enforces max length
    """
    if not filename:
        return "upload.pdf"
    # Strip any directory components
    filename = os.path.basename(filename.replace('\\', '/'))
    # Remove anything suspicious
    filename = _re.sub(r'[^\w\-_\. ]', '_', filename)
    filename = filename.strip('. ')
    # Limit length
    if len(filename) > 120:
        filename = filename[:120]
    return filename or "upload.pdf"


def _safe_path(user_path: str, allowed_base: str) -> str:
    """
    Resolve a user-supplied path and verify it stays within allowed_base.
    Raises ValueError if path escapes the allowed directory.
    """
    try:
        base = os.path.realpath(allowed_base)
        full = os.path.realpath(os.path.join(base, os.path.basename(user_path)))
        if not full.startswith(base + os.sep) and full != base:
            raise ValueError(f"Path traversal blocked: {user_path!r}")
        return full
    except Exception as e:
        raise ValueError(f"Invalid path: {e}")


def _validate_pdf_path(pdf_path: str) -> str:
    """
    Accept a pdf_path from API request. Must be within DATA_DIR.
    Strips traversal attempts. Returns safe absolute path.
    """
    if not pdf_path:
        raise ValueError("No pdf_path provided")
    # Only allow paths within DATA_DIR  
    return _safe_path(pdf_path, DATA_DIR)


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
RATE_LIMIT_MAX = 300    # requests per window (generous for single-user dashboard with polling)
RATE_LIMIT_AUTH_MAX = 60  # auth attempts per window (was 10 — too low for polling pages)

def _check_rate_limit(key: str = None, max_requests: int = None) -> bool:
    """Check if request is within rate limits. Returns True if OK. Thread-safe."""
    key = key or (request.remote_addr if request else "unknown") or "unknown"
    max_req = max_requests or RATE_LIMIT_MAX
    now = time.time()
    with _rate_limiter_lock:
        window = _rate_limiter.get(key, [])
        window = [t for t in window if now - t < RATE_LIMIT_WINDOW]
        if len(window) >= max_req:
            return False
        window.append(now)
        _rate_limiter[key] = window
        # Cleanup old keys periodically (thread-safe, inside lock)
        if len(_rate_limiter) > 1000:
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
    return _cached_json_load(rfq_db_path(), fallback={})
def save_rfqs(rfqs):
    p = rfq_db_path()
    json.dump(rfqs, open(p, "w"), indent=2, default=str)
    _invalidate_cache(p)

# ═══════════════════════════════════════════════════════════════════════
# Email Polling Thread
# ═══════════════════════════════════════════════════════════════════════

_shared_poller = None  # Shared poller instance for manual checks

def process_rfq_email(rfq_email):
    """Process a single RFQ email into the queue. Returns rfq_data or None.
    Deduplicates by checking email_uid against existing RFQs.
    PRD Feature 4.2: After parsing, auto-triggers price check + draft quote generation.
    """
    
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
        rfq_data["line_items"] = bulk_lookup(rfq_data.get("line_items", []))
    
    rfqs[rfq_data["id"]] = rfq_data
    save_rfqs(rfqs)
    POLL_STATUS["emails_found"] += 1
    log.info(f"Auto-imported RFQ #{rfq_data.get('solicitation_number', 'unknown')}")

    # ── PRD Feature 4.2: Auto Price Check + Draft Quote ───────────────────
    # Runs in background thread so polling isn't blocked.
    # Guardrails: never sends, creates draft status, skips if parser confidence < 70%
    _trigger_auto_draft(rfq_data)

    return rfq_data


def _trigger_auto_draft(rfq_data: dict):
    """PRD Feature 4.2: Background auto-draft pipeline for inbound RFQ emails.

    Pipeline: RFQ parsed → create PC → run price lookup → generate draft quote
    Guardrails:
      - Never auto-sends anything
      - Creates quotes with status='draft' (yellow badge, needs review)
      - Skips if line_items < 1 (parser failed)
      - Skips if already has an auto_draft_pc_id (dedup)
      - If price lookup fails, still creates draft with $0 prices flagged
    """
    if rfq_data.get("auto_draft_pc_id"):
        return  # already processed
    if not rfq_data.get("line_items"):
        log.info("Auto-draft skipped: no line items in RFQ %s", rfq_data.get("id"))
        return

    import threading as _t
    def _run():
        try:
            _auto_draft_pipeline(rfq_data)
        except Exception as e:
            log.error("Auto-draft pipeline error for %s: %s", rfq_data.get("id"), e)

    _t.Thread(target=_run, daemon=True, name=f"auto-draft-{rfq_data.get('id','?')[:8]}").start()
    log.info("Auto-draft pipeline started for RFQ %s", rfq_data.get("solicitation_number"))


def _auto_draft_pipeline(rfq_data: dict):
    """Execute the full auto-draft pipeline (runs in background thread)."""
    import time as _time
    rfq_id = rfq_data.get("id", "")
    sol = rfq_data.get("solicitation_number", "?")
    items = rfq_data.get("line_items", [])

    log.info("[AutoDraft] Starting pipeline for RFQ %s (%d items)", sol, len(items))
    t0 = _time.time()

    # Step 1: Create a Price Check record from the RFQ
    pc_id = f"autodraft_{rfq_id[:8]}_{int(_time.time())}"
    pc_items = []
    for li in items:
        pc_items.append({
            "item_number": str(li.get("item_number", "")),
            "description": li.get("description", ""),
            "qty": li.get("qty", 1),
            "uom": li.get("uom", "ea"),
            "part_number": li.get("part_number", ""),
            "pricing": {},
            "no_bid": False,
        })

    pc = {
        "id": pc_id,
        "pc_number": f"AD-{sol[:20]}",
        "status": "parsed",
        "source": "email_auto_draft",
        "rfq_id": rfq_id,
        "solicitation_number": sol,
        "institution": rfq_data.get("department", rfq_data.get("requestor_name", "")),
        "items": pc_items,
        "parsed": {"header": {
            "institution": rfq_data.get("department", ""),
            "requestor": rfq_data.get("requestor_name", ""),
            "phone": rfq_data.get("phone", ""),
        }},
        "created_at": datetime.now().isoformat(),
        "is_auto_draft": True,
    }
    pcs = _load_price_checks()
    pcs[pc_id] = pc
    _save_price_checks(pcs)

    # Step 2: Run SCPRS + Amazon price lookup
    try:
        from src.forms.price_check import lookup_prices
        pc = lookup_prices(pc)
        pcs[pc_id] = pc
        _save_price_checks(pcs)
        priced = sum(1 for it in pc.get("items", []) if it.get("pricing", {}).get("recommended_price"))
        log.info("[AutoDraft] Prices found: %d/%d items", priced, len(pc_items))
    except Exception as pe:
        log.warning("[AutoDraft] Price lookup failed (will draft with $0): %s", pe)

    # Step 3: Generate draft Reytech quote PDF
    draft_result = {}
    if QUOTE_GEN_AVAILABLE:
        try:
            from src.forms.quote_generator import generate_quote_from_pc, _next_quote_number
            output_path = os.path.join(DATA_DIR, f"Draft_{pc_id}_Reytech.pdf")
            draft_result = generate_quote_from_pc(pc, output_path, include_tax=False)
            if draft_result.get("ok"):
                # Mark as draft status (not pending — needs review)
                from src.forms.quote_generator import get_all_quotes, _save_all_quotes
                all_q = get_all_quotes()
                for q in all_q:
                    if q.get("quote_number") == draft_result.get("quote_number"):
                        q["status"] = "draft"
                        q["auto_draft"] = True
                        q["source_rfq_id"] = rfq_id
                        q["requires_review"] = True
                        q["review_note"] = "Auto-generated from email RFQ — review prices before sending"
                        break
                _save_all_quotes(all_q)
                log.info("[AutoDraft] Draft quote %s generated in %.1fs",
                         draft_result.get("quote_number"), _time.time() - t0)
        except Exception as qe:
            log.warning("[AutoDraft] Quote generation failed: %s", qe)

    # Step 4: Update RFQ with auto_draft link
    rfqs = load_rfqs()
    if rfq_id in rfqs:
        rfqs[rfq_id]["auto_draft_pc_id"] = pc_id
        rfqs[rfq_id]["auto_draft_quote"] = draft_result.get("quote_number", "")
        rfqs[rfq_id]["auto_draft_at"] = datetime.now().isoformat()
        rfqs[rfq_id]["status"] = "auto_drafted"
        save_rfqs(rfqs)

    # Step 5: Log to CRM activity
    _log_crm_activity(
        draft_result.get("quote_number") or rfq_id,
        "auto_draft_generated",
        f"Auto-draft from email RFQ {sol}: {len(pc_items)} items, {draft_result.get('quote_number','pending')}",
        actor="system",
        metadata={"rfq_id": rfq_id, "pc_id": pc_id, "feature": "4.2"},
    )
    log.info("[AutoDraft] Complete for RFQ %s in %.1fs", sol, _time.time() - t0)
    # 🔔 Proactive alert — auto-draft ready for review
    try:
        from src.agents.notify_agent import send_alert
        qnum = draft_result.get("quote_number","?")
        contact = rfq_data.get("requestor_email","") or rfq_data.get("requestor_name","")
        send_alert(
            event_type="auto_draft_ready",
            title=f"📋 Auto-Draft Ready: {qnum}",
            body=f"Quote {qnum} auto-drafted from email RFQ {sol} ({contact}). Review and send when ready.",
            urgency="draft",
            context={"quote_number": qnum, "contact": contact, "entity_id": qnum},
            cooldown_key=f"auto_draft_{qnum}",
        )
    except Exception as _ne:
        log.debug("Auto-draft alert error: %s", _ne)



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
    return _cached_json_load(CRM_LOG_FILE, fallback=[])

def _save_crm_activity(activity: list):
    os.makedirs(DATA_DIR, exist_ok=True)
    if len(activity) > 5000:
        activity = activity[-5000:]
    with open(CRM_LOG_FILE, "w") as f:
        json.dump(activity, f, indent=2, default=str)
    _invalidate_cache(CRM_LOG_FILE)

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
    return _cached_json_load(ORDERS_FILE, fallback={})

def _save_orders(orders: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(ORDERS_FILE, "w") as f:
        json.dump(orders, f, indent=2, default=str)
    _invalidate_cache(ORDERS_FILE)

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

from src.api.templates import BASE_CSS, PAGE_HOME, PAGE_DETAIL, build_pc_detail_html, build_quotes_page_content, PAGE_CRM, DEBUG_PAGE_HTML

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
<meta name="description" content="Reytech RFQ Dashboard — AI-powered sales automation for CA state agency reseller">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>{BASE_CSS}</style></head><body>
<header class="hdr" role="banner">
 <div style="display:flex;align-items:center;gap:14px">
  <a href="/" aria-label="Reytech RFQ Dashboard — Home" style="display:flex;align-items:center;gap:10px;text-decoration:none">
   <img src="/api/logo" alt="Reytech logo" style="height:44px;background:#fff;padding:6px 12px;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.2)" onerror="this.outerHTML='<span style=\\'font-size:20px;font-weight:700;color:var(--ac)\\'>Reytech</span>'">
   <span style="font-size:17px;font-weight:600;color:var(--tx);letter-spacing:-0.3px" aria-hidden="true">RFQ Dashboard</span>
  </a>
 </div>
 <nav aria-label="Main navigation" style="display:flex;align-items:center;gap:6px">
  <a href="/" class="hdr-btn hdr-active" aria-label="Home" aria-current="page">🏠 Home</a>
  <a href="/search" class="hdr-btn" aria-label="Universal search">🔍 Search</a>
  <a href="/quotes" class="hdr-btn" aria-label="Quotes database">📋 Quotes</a>
  <a href="/orders" class="hdr-btn" aria-label="Orders tracking">📦 Orders</a>
  <a href="/contacts" class="hdr-btn" aria-label="CRM Contacts">👥 CRM</a>
  <a href="/vendors" class="hdr-btn" aria-label="Vendor ordering">🏭 Vendors</a>
  <a href="/intel/market" class="hdr-btn" aria-label="Market Intelligence">📊 Intel</a>
  <a href="/catalog" class="hdr-btn" aria-label="Product Catalog">📦 Catalog</a>
  <a href="/cchcs/expansion" class="hdr-btn" aria-label="Expand Facilities">🏥 Expand</a>
  <a href="/campaigns" class="hdr-btn" aria-label="Outreach campaigns">📞 Campaigns</a>
  <a href="/pipeline" class="hdr-btn" aria-label="Revenue pipeline">🔄 Pipeline</a>
  <a href="/growth" class="hdr-btn" aria-label="Growth engine">🚀 Growth</a>
  <a href="/intelligence" class="hdr-btn" aria-label="Sales intelligence">🧠 Intel</a>
  <a href="/agents" class="hdr-btn" aria-label="AI Agents manager">🤖 Agents</a>
  <span style="width:1px;height:24px;background:var(--bd);margin:0 6px" role="separator" aria-hidden="true"></span>
  <button class="hdr-btn" onclick="pollNow(this)" id="poll-btn" aria-label="Check for new emails now">⚡ Check Now</button>
  <button class="hdr-btn hdr-warn" onclick="resyncAll(this)" title="Clear queue & re-import all emails" aria-label="Resync all emails from inbox">🔄 Resync</button>
<div style="position:relative" id="notif-wrap">
   <button class="notif-bell" onclick="toggleNotifPanel()" id="notif-bell-btn" aria-label="Notifications" title="Notifications">
    🔔
    <span class="notif-badge" id="notif-badge">0</span>
   </button>
   <div class="notif-panel" id="notif-panel">
    <div class="notif-panel-hdr">
     <h4>🔔 Notifications</h4>
     <div style="display:flex;gap:8px;align-items:center">
      <span id="notif-cs-count" style="font-size:11px;color:var(--yl);display:none"></span>
      <button onclick="markAllRead()" style="font-size:11px;color:var(--tx2);background:none;border:none;cursor:pointer;padding:2px 6px">Mark all read</button>
      <button onclick="toggleNotifPanel()" style="font-size:16px;color:var(--tx2);background:none;border:none;cursor:pointer;line-height:1">×</button>
     </div>
    </div>
    <div class="notif-panel-body" id="notif-list">
     <div class="notif-empty">Loading...</div>
    </div>
    <div class="notif-footer">
     <a href="/outbox" class="hdr-btn" style="font-size:11px;padding:4px 10px">📬 Review Drafts</a>
     <a href="/api/notify/status" class="hdr-btn" style="font-size:11px;padding:4px 10px" target="_blank">⚙️ Alert Settings</a>
    </div>
   </div>
  </div>
  <span style="width:1px;height:24px;background:var(--bd);margin:0 6px" role="separator" aria-hidden="true"></span>
  <div class="hdr-status" role="status" aria-live="polite" aria-label="Email polling status">
   <div style="display:flex;align-items:center;gap:6px">
    <span class="poll-dot {_poll_class}" aria-hidden="true"></span>
    <span>{_poll_status}</span>
   </div>
   <div class="hdr-time" id="poll-time" data-utc="{_poll_last}">{_poll_last or 'never'}</div>
  </div>
 </nav>
</header>
<main class="ctr" role="main" id="main-content">
{{% with messages = get_flashed_messages(with_categories=true) %}}
 {{% for cat, msg in messages %}}<div class="alert al-{{'s' if cat=='success' else 'e' if cat=='error' else 'i'}}" role="alert" aria-live="assertive">{{% if cat=='success' %}}✅{{% elif cat=='error' %}}❌{{% else %}}ℹ️{{% endif %}} {{{{msg}}}}</div>{{% endfor %}}
{{% endwith %}}
""" + content + """
</main>
<script>
function _updatePollTime(ts){
 var el=document.getElementById('poll-time');
 if(el&&ts){el.dataset.utc=ts;try{var d=new Date(ts);if(!isNaN(d)){el.textContent=d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true})}}catch(e){el.textContent=ts}}
}
function pollNow(btn){
 btn.disabled=true;btn.setAttribute('aria-busy','true');btn.textContent='Checking...';
 fetch('/api/poll-now',{credentials:'same-origin'}).then(r=>r.json()).then(d=>{
  _updatePollTime(d.last_check);
  if(d.found>0){btn.textContent=d.found+' found!';setTimeout(()=>location.reload(),800)}
  else{btn.textContent='No new emails';setTimeout(()=>{btn.textContent='⚡ Check Now';btn.disabled=false;btn.removeAttribute('aria-busy')},2000)}
 }).catch(()=>{btn.textContent='Error';setTimeout(()=>{btn.textContent='⚡ Check Now';btn.disabled=false;btn.removeAttribute('aria-busy')},2000)});
}
function resyncAll(btn){
 if(!confirm('Clear all RFQs and re-import from email?'))return;
 btn.disabled=true;btn.setAttribute('aria-busy','true');btn.textContent='🔄 Syncing...';
 fetch('/api/resync',{credentials:'same-origin'}).then(r=>r.json()).then(d=>{
  _updatePollTime(d.last_check);
  if(d.found>0){btn.textContent=d.found+' imported!';setTimeout(()=>location.reload(),800)}
  else{btn.textContent='0 found';setTimeout(()=>{btn.textContent='🔄 Resync';btn.disabled=false},2000)}
 }).catch(()=>{btn.textContent='Error';setTimeout(()=>{btn.textContent='🔄 Resync';btn.disabled=false},2000)});
}

// ── Notification Bell ──────────────────────────────────────────────────────
(function initBell(){
  // Poll badge count every 30s
  function updateBellCount(){
    fetch('/api/notifications/bell-count',{credentials:'same-origin'})
    .then(r=>r.json()).then(d=>{
      var badge=document.getElementById('notif-badge');
      var btn=document.getElementById('notif-bell-btn');
      if(!badge||!d.ok)return;
      var total=d.total_badge||0;
      if(total>0){badge.textContent=total>99?'99+':total;badge.classList.add('show')}
      else{badge.classList.remove('show')}
      // CS drafts warning
      var csEl=document.getElementById('notif-cs-count');
      if(csEl&&d.cs_drafts>0){csEl.textContent=d.cs_drafts+' CS draft(s)';csEl.style.display='inline'}
      else if(csEl){csEl.style.display='none'}
    }).catch(()=>{});
  }
  updateBellCount();
  setInterval(updateBellCount,30000);
})();

function toggleNotifPanel(){
  var panel=document.getElementById('notif-panel');
  if(!panel)return;
  var isOpen=panel.classList.contains('open');
  panel.classList.toggle('open');
  if(!isOpen) loadNotifications();
}

function loadNotifications(){
  fetch('/api/notifications/persistent?limit=20',{credentials:'same-origin'})
  .then(r=>r.json()).then(d=>{
    var list=document.getElementById('notif-list');
    if(!list)return;
    if(!d.notifications||d.notifications.length===0){
      list.innerHTML='<div class="notif-empty">No notifications yet.<br><small style="color:var(--tx2)">You\'ll get alerts for new RFQs, CS drafts, and won quotes.</small></div>';
      return;
    }
    var URGENCY_ICON={urgent:'🚨',deal:'💰',draft:'📋',warning:'⏰',info:'ℹ️'};
    list.innerHTML=d.notifications.map(n=>{
      var ts=n.created_at?new Date(n.created_at).toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true}):'';
      var icon=URGENCY_ICON[n.urgency]||'🔔';
      return '<div class="notif-item '+(n.is_read?'':'unread')+' urgency-'+(n.urgency||'info')+'" onclick="notifClick(\''+n.deep_link+'\','+n.id+')">'
        +'<div class="notif-item-title">'+icon+' '+(n.title||'')+'</div>'
        +'<div class="notif-item-body">'+(n.body||'').substring(0,120)+'</div>'
        +'<div class="notif-item-time">'+ts+'</div>'
        +'</div>';
    }).join('');
  }).catch(()=>{
    var list=document.getElementById('notif-list');
    if(list)list.innerHTML='<div class="notif-empty">Could not load notifications.</div>';
  });
}

function notifClick(link,id){
  // Mark read
  if(id) fetch('/api/notifications/mark-read',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({ids:[id]})});
  // Navigate
  if(link&&link!=='/'){window.location.href=link}
  document.getElementById('notif-panel').classList.remove('open');
}

function markAllRead(){
  fetch('/api/notifications/mark-read',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({})})
  .then(()=>{
    document.getElementById('notif-badge').classList.remove('show');
    loadNotifications();
  });
}

// Close panel on outside click
document.addEventListener('click',function(e){
  var wrap=document.getElementById('notif-wrap');
  if(wrap&&!wrap.contains(e.target)){
    var panel=document.getElementById('notif-panel');
    if(panel)panel.classList.remove('open');
  }
});

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


def _header(page_title: str = "") -> str:
    """Standalone page header for Growth/Intelligence/Prospect pages that build their own full HTML."""
    _email_cfg = CONFIG.get("email", {})
    _has_email = bool(_email_cfg.get("email_password"))
    _poll_running = POLL_STATUS.get("running", False)
    _poll_last = POLL_STATUS.get("last_check", "")
    _poll_status = "Polling" if _poll_running else ("Email not configured" if not _has_email else "Starting...")
    _poll_class = "poll-on" if _poll_running else ("poll-off" if not _has_email else "poll-wait")
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{page_title} — Reytech</title>
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
  <a href="/" class="hdr-btn">🏠 Home</a>
  <a href="/search" class="hdr-btn{'{ hdr-active}' if page_title=='Search' else ''}">🔍 Search</a>
  <a href="/quotes" class="hdr-btn">📋 Quotes</a>
  <a href="/orders" class="hdr-btn">📦 Orders</a>
  <a href="/contacts" class="hdr-btn">👥 CRM</a>
  <a href="/vendors" class="hdr-btn">🏭 Vendors</a>
  <a href="/intel/market" class="hdr-btn">📊 Intel</a>
  <a href="/qa/intelligence" class="hdr-btn">🧠 QA</a>
  <a href="/catalog" class="hdr-btn">📦 Catalog</a>
  <a href="/cchcs/expansion" class="hdr-btn">🏥 Expand</a>
  <a href="/campaigns" class="hdr-btn">📞 Campaigns</a>
  <a href="/pipeline" class="hdr-btn">🔄 Pipeline</a>
  <a href="/growth" class="hdr-btn{'{ hdr-active}' if page_title=='Growth Engine' else ''}">🚀 Growth</a>
  <a href="/intelligence" class="hdr-btn{'{ hdr-active}' if page_title=='Sales Intelligence' else ''}">🧠 Intel</a>
  <a href="/agents" class="hdr-btn">🤖 Agents</a>
  <span style="width:1px;height:24px;background:var(--bd);margin:0 6px"></span>
  <button class="hdr-btn" onclick="pollNow(this)" id="poll-btn">⚡ Check Now</button>
<div style="position:relative" id="notif-wrap">
   <button class="notif-bell" onclick="toggleNotifPanel()" id="notif-bell-btn" aria-label="Notifications" title="Notifications">
    🔔
    <span class="notif-badge" id="notif-badge">0</span>
   </button>
   <div class="notif-panel" id="notif-panel">
    <div class="notif-panel-hdr">
     <h4>🔔 Notifications</h4>
     <div style="display:flex;gap:8px;align-items:center">
      <span id="notif-cs-count" style="font-size:11px;color:var(--yl);display:none"></span>
      <button onclick="markAllRead()" style="font-size:11px;color:var(--tx2);background:none;border:none;cursor:pointer;padding:2px 6px">Mark all read</button>
      <button onclick="toggleNotifPanel()" style="font-size:16px;color:var(--tx2);background:none;border:none;cursor:pointer;line-height:1">×</button>
     </div>
    </div>
    <div class="notif-panel-body" id="notif-list">
     <div class="notif-empty">Loading...</div>
    </div>
    <div class="notif-footer">
     <a href="/outbox" class="hdr-btn" style="font-size:11px;padding:4px 10px">📬 Review Drafts</a>
     <a href="/api/notify/status" class="hdr-btn" style="font-size:11px;padding:4px 10px" target="_blank">⚙️ Alert Settings</a>
    </div>
   </div>
  </div>
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
<div class="ctr">"""


# ══════════════════════════════════════════════════════════════════════════════
# Route Modules — loaded at import time, register routes onto this Blueprint
# Split from dashboard.py for maintainability (was 13,831 lines)
# ══════════════════════════════════════════════════════════════════════════════

def _load_route_module(module_name: str):
    """
    Load a route module by exec'ing it in this module's global namespace.
    This lets route functions reference bp, auth_required, etc. from dashboard scope.
    """
    module_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "modules", f"{module_name}.py")
    with open(module_path, "r") as _f:
        _src = _f.read()
    exec(compile(_src, module_path, "exec"), globals())
    log.debug(f"Route module loaded: {module_name}")


_ROUTE_MODULES = [
    "routes_rfq",          # Home, upload, RFQ pages, quote generation
    "routes_agents",       # Agent control panel, email templates
    "routes_pricecheck",   # Price check pages + lookup
    "routes_crm",          # CRM, pricing oracle, auto-processor
    "routes_intel",        # Growth intel, SCPRS, CCHCS, vendor, scheduler
]

for _mod in _ROUTE_MODULES:
    try:
        _load_route_module(_mod)
    except Exception as _e:
        log.error(f"Failed to load route module {_mod}: {_e}")
        import traceback; traceback.print_exc()

log.info(f"Dashboard: {len(_ROUTE_MODULES)} route modules loaded, {len([r for r in bp.deferred_functions])} deferred fns")
