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
            POLL_STATUS["error"] = f"IMAP connect failed for {email_cfg
