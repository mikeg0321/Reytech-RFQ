#!/usr/bin/env python3
"""
Reytech RFQ Dashboard — API Routes Blueprint
Refactored from monolithic dashboard.py into modular Blueprint.
All route handlers live here; app creation is in app.py.
"""

import os, json, uuid, sys, threading, time, logging, functools, re, shutil, glob
from datetime import datetime, timezone, timedelta
from src.api.trace import Trace
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

POLL_STATUS = {"running": False, "last_check": None, "emails_found": 0, "error": None, "paused": False}

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
    
    Multi-worker safe: max cache age of 2s ensures stale data from other
    workers' writes gets picked up even if mtime resolution is coarse.
    """
    if fallback is None:
        fallback = {}
    if not os.path.exists(path):
        return fallback
    try:
        mtime = os.path.getmtime(path)
        now = time.time()
        with _json_cache_lock:
            cached = _json_cache.get(path)
            # Use cache only if mtime matches AND cache is <2s old
            if cached and cached["mtime"] == mtime and (now - cached["ts"]) < 2.0:
                return cached["data"]
        with open(path) as f:
            data = json.load(f)
        with _json_cache_lock:
            _json_cache[path] = {"data": data, "mtime": mtime, "ts": now}
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
    # Atomic write: temp file → fsync → rename (prevents partial reads by other workers)
    tmp = p + ".tmp"
    with open(tmp, "w") as f:
        json.dump(rfqs, f, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)  # atomic on POSIX
    _invalidate_cache(p)

# ═══════════════════════════════════════════════════════════════════════
# RFQ File Storage — PDFs stored as BLOBs in SQLite (survives redeploys)
# ═══════════════════════════════════════════════════════════════════════

def _init_rfq_files_table():
    """Create rfq_files table if it doesn't exist."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Drop old table with FK constraint if it exists (new feature, no data to preserve)
            existing = conn.execute("SELECT sql FROM sqlite_master WHERE name='rfq_files'").fetchone()
            if existing and "FOREIGN KEY" in (existing[0] or ""):
                conn.execute("DROP TABLE rfq_files")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS rfq_files (
                    id          TEXT PRIMARY KEY,
                    rfq_id      TEXT NOT NULL,
                    filename    TEXT NOT NULL,
                    file_type   TEXT NOT NULL,
                    category    TEXT DEFAULT 'template',
                    mime_type   TEXT DEFAULT 'application/pdf',
                    file_size   INTEGER DEFAULT 0,
                    data        BLOB,
                    uploaded_by TEXT DEFAULT 'system',
                    created_at  TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rfq_files_rfq ON rfq_files(rfq_id)")
    except Exception as e:
        log.debug("rfq_files table init: %s", e)

# Init on import
_init_rfq_files_table()


def save_rfq_file(rfq_id: str, filename: str, file_type: str, data: bytes,
                   category: str = "template", uploaded_by: str = "system") -> str:
    """Save a PDF to the rfq_files table. Returns file_id."""
    import uuid
    file_id = f"rf_{uuid.uuid4().hex[:10]}"
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO rfq_files (id, rfq_id, filename, file_type, category, file_size, data, uploaded_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (file_id, rfq_id, filename, file_type, category, len(data), data, uploaded_by, datetime.now().isoformat()))
        log.info("Saved file %s (%s, %d bytes) for RFQ %s", filename, file_type, len(data), rfq_id)
    except Exception as e:
        log.error("Failed to save file %s for RFQ %s: %s", filename, rfq_id, e)
    return file_id


def get_rfq_file(file_id: str) -> dict:
    """Get a file by ID. Returns {id, filename, data, mime_type, ...} or None."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute("SELECT * FROM rfq_files WHERE id = ?", (file_id,)).fetchone()
            if row:
                return dict(row)
    except Exception as e:
        log.error("get_rfq_file error: %s", e)
    return None


def list_rfq_files(rfq_id: str, category: str = None) -> list:
    """List files for an RFQ. Returns list of dicts (without BLOB data)."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            if category:
                rows = conn.execute(
                    "SELECT id, rfq_id, filename, file_type, category, file_size, uploaded_by, created_at FROM rfq_files WHERE rfq_id = ? AND category = ? ORDER BY created_at",
                    (rfq_id, category)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, rfq_id, filename, file_type, category, file_size, uploaded_by, created_at FROM rfq_files WHERE rfq_id = ? ORDER BY created_at",
                    (rfq_id,)).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.error("list_rfq_files error: %s", e)
    return []


def _log_rfq_activity(rfq_id: str, action: str, details: str, actor: str = "system", metadata: dict = None):
    """Log an activity event for an RFQ."""
    _log_crm_activity(rfq_id, f"rfq_{action}", details, actor=actor, metadata=metadata or {})


# ═══════════════════════════════════════════════════════════════════════
# Email Templates — saveable/editable templates for PC, RFQ, customer svc
# ═══════════════════════════════════════════════════════════════════════

def _init_email_templates_table():
    """Create email_templates table and seed defaults."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS email_templates (
                    id          TEXT PRIMARY KEY,
                    name        TEXT NOT NULL,
                    category    TEXT NOT NULL,
                    subject     TEXT NOT NULL,
                    body        TEXT NOT NULL,
                    is_default  INTEGER DEFAULT 0,
                    created_at  TEXT NOT NULL,
                    updated_at  TEXT NOT NULL
                )
            """)
            # Seed defaults if empty
            count = conn.execute("SELECT COUNT(*) FROM email_templates").fetchone()[0]
            if count == 0:
                now = datetime.now().isoformat()
                defaults = [
                    ("et_rfq_bid", "RFQ Bid Response", "rfq",
                     "Reytech Inc. - Bid Response - Solicitation #{{solicitation}}",
                     """Dear {{requestor}},

Please find attached our bid response for Solicitation #{{solicitation}}.

Bid Package includes:
- AMS 703B - Request for Quotation (signed)
- AMS 704B - CCHCS Acquisition Quote Worksheet (with pricing)
- Bid Package & Forms (all required forms completed)
- Reytech Quote #{{quote_number}} on company letterhead

All items are quoted F.O.B. Destination, freight prepaid and included.
Pricing is valid for 45 calendar days from the due date.

Please let us know if you need any additional information.

Best regards,
Michael Guadan
Reytech Inc.
949-229-1575
sales@reytechinc.com
SB/DVBE Cert #2002605""", 1),
                    ("et_rfq_nobid", "RFQ No Bid", "rfq",
                     "Reytech Inc. - No Bid - Solicitation #{{solicitation}}",
                     """Dear {{requestor}},

Thank you for the opportunity to bid on Solicitation #{{solicitation}}.

After careful review, we have decided not to submit a bid for this solicitation at this time.

We look forward to future opportunities.

Best regards,
Michael Guadan
Reytech Inc.
949-229-1575
sales@reytechinc.com""", 0),
                    ("et_pc_quote", "Price Check Quote", "pc",
                     "Reytech Inc. - Quote #{{quote_number}} - {{institution}}",
                     """Dear {{requestor}},

Please find attached our quote for the requested items.

Quote #{{quote_number}}
Institution: {{institution}}

All items are quoted F.O.B. Destination, freight prepaid and included.
Pricing is valid for 45 calendar days.

Please let us know if you need any additional information.

Best regards,
Michael Guadan
Reytech Inc.
949-229-1575
sales@reytechinc.com
SB/DVBE Cert #2002605""", 1),
                    ("et_followup", "Follow-Up", "customer_service",
                     "Reytech Inc. - Following Up - {{subject}}",
                     """Dear {{requestor}},

I wanted to follow up on our recent communication regarding {{subject}}.

Please let us know if you have any questions or need additional information.

Best regards,
Michael Guadan
Reytech Inc.
949-229-1575
sales@reytechinc.com""", 0),
                    ("et_thankyou", "Thank You / Order Confirmation", "customer_service",
                     "Reytech Inc. - Order Confirmation - PO #{{po_number}}",
                     """Dear {{requestor}},

Thank you for your order (PO #{{po_number}}). We have received your purchase order and will begin processing immediately.

Estimated delivery: {{delivery_estimate}}

Please let us know if you have any questions.

Best regards,
Michael Guadan
Reytech Inc.
949-229-1575
sales@reytechinc.com""", 0),
                ]
                for tid, name, cat, subj, body, is_default in defaults:
                    conn.execute(
                        "INSERT INTO email_templates (id, name, category, subject, body, is_default, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
                        (tid, name, cat, subj, body, is_default, now, now))
    except Exception as e:
        log.debug("email_templates init: %s", e)

_init_email_templates_table()


def get_email_templates_db(category: str = None) -> list:
    """Get email templates, optionally filtered by category."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            if category:
                rows = conn.execute(
                    "SELECT id, name, category, subject, body, is_default FROM email_templates WHERE category = ? ORDER BY is_default DESC, name",
                    (category,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, name, category, subject, body, is_default FROM email_templates ORDER BY category, is_default DESC, name"
                ).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.error("get_email_templates: %s", e)
    return []


def save_email_template_db(template_id: str, name: str, category: str, subject: str, body: str, is_default: int = 0) -> str:
    """Save or update an email template."""
    try:
        from src.core.db import get_db
        import uuid
        if not template_id:
            template_id = f"et_{uuid.uuid4().hex[:8]}"
        now = datetime.now().isoformat()
        with get_db() as conn:
            existing = conn.execute("SELECT id FROM email_templates WHERE id = ?", (template_id,)).fetchone()
            if existing:
                conn.execute(
                    "UPDATE email_templates SET name=?, category=?, subject=?, body=?, is_default=?, updated_at=? WHERE id=?",
                    (name, category, subject, body, is_default, now, template_id))
            else:
                conn.execute(
                    "INSERT INTO email_templates (id, name, category, subject, body, is_default, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
                    (template_id, name, category, subject, body, is_default, now, now))
        return template_id
    except Exception as e:
        log.error("save_email_template: %s", e)
    return ""


def log_email_sent_db(direction: str, sender: str, recipient: str, subject: str,
                    body: str, attachments: list = None, quote_number: str = "",
                    rfq_id: str = "", contact_id: str = "") -> int:
    """Log an email to the email_log table. Returns row ID."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            cur = conn.execute("""
                INSERT INTO email_log (logged_at, direction, sender, recipient, subject, body_preview, full_body, attachments_json, quote_number, rfq_id, contact_id, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (datetime.now().isoformat(), direction, sender, recipient, subject,
                  body[:200] if body else "", body or "",
                  json.dumps(attachments or []), quote_number, rfq_id,
                  contact_id, "sent" if direction == "outbound" else "received"))
            return cur.lastrowid
    except Exception as e:
        log.error("log_email_sent: %s", e)
    return 0


# ═══════════════════════════════════════════════════════════════════════
# Price Check JSON helpers
# ═══════════════════════════════════════════════════════════════════════ (defined here to avoid import from routes_rfq
# which can't be imported directly because bp isn't defined at import time)
# ═══════════════════════════════════════════════════════════════════════

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


def _is_user_facing_pc(pc: dict) -> bool:
    """Canonical filter: is this PC for the standalone PC queue?
    Auto-price PCs (created from RFQ imports) belong to the RFQ row, not the PC queue.
    Standalone email PCs (Valentina's 704s) DO belong in the PC queue.
    Used by: home page, manager brief, workflow tester, pipeline summary."""
    if pc.get("source") == "email_auto_draft":
        return False
    if pc.get("is_auto_draft"):
        return False
    if pc.get("rfq_id"):
        return False
    return True


# ═══════════════════════════════════════════════════════════════════════
# Email Polling Thread
# ═══════════════════════════════════════════════════════════════════════

_shared_poller = None  # Shared poller instance for manual checks

def _ensure_contact_from_email(rfq_email: dict):
    """Auto-create/update CRM contact from inbound email sender.
    Called on every PC/RFQ arrival so the buyer is always in CRM.
    Writes to both SQLite (contacts table) and crm_contacts.json (CRM page source).
    """
    try:
        from src.core.db import upsert_contact
        import re as _re, hashlib
        
        sender_str = rfq_email.get("sender", "")
        sender_email = rfq_email.get("sender_email", "")
        if not sender_email:
            m = _re.search(r'[\w.+-]+@[\w.-]+', sender_str)
            sender_email = m.group(0).lower() if m else ""
        if not sender_email:
            return
        sender_email = sender_email.lower().strip()
        
        # Extract display name: "Valentina Demidenko <email>" → "Valentina Demidenko"
        sender_name = ""
        if "<" in sender_str:
            sender_name = sender_str.split("<")[0].strip().strip('"').strip("'")
        if not sender_name:
            local = sender_email.split("@")[0]
            sender_name = " ".join(w.capitalize() for w in _re.split(r'[._-]', local))
        
        # Derive agency from domain
        domain = sender_email.split("@")[-1].lower()
        agency_map = {
            "cdcr.ca.gov": "CDCR", "cdph.ca.gov": "CDPH", "dgs.ca.gov": "DGS",
            "dhcs.ca.gov": "DHCS", "cchcs.org": "CCHCS",
        }
        agency = agency_map.get(domain, domain.split(".")[0].upper() if ".gov" in domain else "")
        
        # Stable ID from email
        contact_id = hashlib.md5(sender_email.encode()).hexdigest()[:16]
        
        contact_data = {
            "id": contact_id,
            "buyer_name": sender_name,
            "buyer_email": sender_email,
            "agency": agency,
            "source": "email_inbound",
            "outreach_status": "active",
            "is_reytech_customer": True,
            "tags": ["email_sender", "buyer"],
        }
        
        # 1) SQLite
        upsert_contact(contact_data)
        
        # 2) crm_contacts.json (what the CRM page actually reads)
        crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
        try:
            with open(crm_path) as f:
                crm = json.load(f)
        except Exception:
            crm = {}
        
        if contact_id not in crm:
            crm[contact_id] = {
                "id": contact_id,
                "buyer_name": sender_name,
                "buyer_email": sender_email,
                "buyer_phone": "",
                "agency": agency,
                "title": "",
                "department": "",
                "linkedin": "",
                "notes": f"Auto-added from email {rfq_email.get('subject', '')[:60]}",
                "tags": ["email_sender", "buyer"],
                "total_spend": 0,
                "po_count": 0,
                "categories": {},
                "items_purchased": [],
                "purchase_orders": [],
                "last_purchase": "",
                "score": 50,
                "opportunity_score": 0,
                "outreach_status": "active",
                "activity": [{
                    "type": "email_received",
                    "detail": f"Inbound: {rfq_email.get('subject', '')[:80]}",
                    "timestamp": datetime.now().isoformat(),
                }],
            }
            with open(crm_path, "w") as f:
                json.dump(crm, f, indent=2, default=str)
            log.info("CRM contact created: %s <%s> → %s", sender_name, sender_email, agency)
        else:
            # Update existing: add activity, refresh name/agency if better
            existing = crm[contact_id]
            if not existing.get("buyer_name") or existing["buyer_name"] == sender_email:
                existing["buyer_name"] = sender_name
            if agency and not existing.get("agency"):
                existing["agency"] = agency
            existing.setdefault("activity", []).append({
                "type": "email_received",
                "detail": f"Inbound: {rfq_email.get('subject', '')[:80]}",
                "timestamp": datetime.now().isoformat(),
            })
            existing["outreach_status"] = "active"
            with open(crm_path, "w") as f:
                json.dump(crm, f, indent=2, default=str)
            log.info("CRM contact updated: %s <%s>", sender_name, sender_email)
    except Exception as e:
        log.debug("Contact auto-create failed (non-critical): %s", e)


def process_rfq_email(rfq_email):
    """Process a single RFQ email into the queue. Returns rfq_data or None.
    Deduplicates by checking email_uid against existing RFQs.
    PRD Feature 4.2: After parsing, auto-triggers price check + draft quote generation.
    """
    _trace = []  # Legacy trace for poll_diag compatibility
    _subj = rfq_email.get("subject", "?")[:50]
    _trace.append(f"START: {_subj}")
    t = Trace("email_pipeline", subject=_subj, email_uid=rfq_email.get("email_uid", "?"))
    
    # Dedup: check if this email UID is already in the queue
    rfqs = load_rfqs()
    for existing in rfqs.values():
        if existing.get("email_uid") == rfq_email.get("email_uid"):
            _trace.append("SKIP: duplicate email_uid in RFQ queue")
            log.info(f"Skipping duplicate email UID {rfq_email.get('email_uid')}: already in queue")
            POLL_STATUS.setdefault("_email_traces", []).append(_trace)
            t.ok("Skipped: duplicate email_uid in RFQ queue")
            return None
    
    # ── Route 704 price checks to PC queue, NOT the RFQ queue ──────────────
    attachments = rfq_email.get("attachments", [])
    pdf_paths = [a["path"] for a in attachments if a.get("path") and a["path"].lower().endswith(".pdf")]
    _trace.append(f"PDFs: {len(pdf_paths)} paths, PRICE_CHECK_AVAILABLE={PRICE_CHECK_AVAILABLE}")
    
    # Early PC detection flag from poller (known sender + subject patterns)
    is_early_pc = rfq_email.get("_pc_early_detect", False)
    if is_early_pc:
        _trace.append(f"EARLY PC DETECT: signals={rfq_email.get('_pc_signals', [])}")
    
    if pdf_paths and PRICE_CHECK_AVAILABLE:
        try:
            # Inline PC detection — can't import from routes_rfq (bp not defined at import time)
            def _is_pc_filename(path):
                bn = os.path.basename(path).lower()
                # Exclude 704B / 703B / bid package
                if any(x in bn for x in ["704b", "703b", "bid package", "bid_package", "quote worksheet"]):
                    return False
                # Match "AMS 704" pattern in filename (no B suffix)
                if "704" in bn and "ams" in bn:
                    return True
                # Match "704" alone (common for AMS 704 price checks)
                if "704" in bn and "704b" not in bn:
                    return True
                # Fallback: try PDF content
                try:
                    from pypdf import PdfReader
                    reader = PdfReader(path)
                    text = (reader.pages[0].extract_text() or "").lower()
                    if any(m in text for m in ["704b", "quote worksheet", "acquisition quote"]):
                        return False
                    if "price check" in text and ("ams 704" in text or "worksheet" in text):
                        return True
                    # Check for AMS 704 form fields
                    fields = reader.get_fields()
                    if fields:
                        fnames = set(fields.keys())
                        if len({"COMPANY NAME", "Requestor", "PRICE PER UNITRow1", "EXTENSIONRow1"} & fnames) >= 3:
                            return True
                except Exception:
                    pass
                return False
            
            # If early PC detect, try ALL PDFs as PC candidates (don't require filename match)
            if is_early_pc:
                pc_pdf = pdf_paths[0] if pdf_paths else None
                _trace.append(f"Early PC: using first PDF as PC form: {os.path.basename(pc_pdf) if pc_pdf else 'none'}")
            else:
                pc_pdf = next((p for p in pdf_paths if _is_pc_filename(p)), None)
            
            pc_checks = [f"{os.path.basename(p)}={'PC' if _is_pc_filename(p) else 'NO'}" for p in pdf_paths]
            _trace.append(f"PC checks: {pc_checks}")
            if pc_pdf:
                import uuid as _uuid
                pc_id = f"pc_{str(_uuid.uuid4())[:8]}"
                _trace.append(f"PC detected: {os.path.basename(pc_pdf)} → {pc_id}")
                
                existing_pcs = _load_price_checks()
                email_uid = rfq_email.get("email_uid")
                if email_uid and any(p.get("email_uid") == email_uid for p in existing_pcs.values()):
                    _trace.append("SKIP: duplicate email_uid in PC queue")
                    POLL_STATUS.setdefault("_email_traces", []).append(_trace)
                    t.ok("Skipped: duplicate email_uid in PC queue")
                    return None
                
                # ── Cross-queue dedup: if this email has RFQ templates (703B/704B/BidPkg),
                # it's an RFQ, not a PC. Don't create a PC entry for it.
                _has_rfq_forms = any(
                    any(x in os.path.basename(p).lower() for x in ["703b", "bid package", "bid_package"])
                    for p in pdf_paths
                )
                if _has_rfq_forms:
                    _trace.append(f"SKIP PC: email has RFQ forms (703B/BidPkg) alongside 704 — routing to RFQ queue instead")
                    log.info("Skipping PC for %s — email has RFQ forms, will create RFQ instead", _subj)
                    # Fall through to RFQ creation below
                else:
                    # Create the PC inline (can't import from routes_rfq — bp issue)
                    try:
                        import shutil as _shutil
                        pc_file = os.path.join(DATA_DIR, f"pc_upload_{os.path.basename(pc_pdf)}")
                        _shutil.copy2(pc_pdf, pc_file)
                        parsed = parse_ams704(pc_file)
                        parse_error = parsed.get("error")
                        
                        if parse_error:
                            # Still create minimal PC so email isn't lost
                            _trace.append(f"parse_ams704 error: {parse_error} — creating minimal PC")
                            pcs = _load_price_checks()
                            pcs[pc_id] = {
                                "id": pc_id,
                                "pc_number": os.path.basename(pc_pdf).replace(".pdf","")[:40],
                                "institution": "", "due_date": "", "requestor": "",
                                "ship_to": "", "items": [], "source_pdf": pc_file,
                                "status": "parse_error", "parse_error": parse_error,
                                "created_at": datetime.now().isoformat(),
                                "reytech_quote_number": "", "linked_quote_number": "",
                            }
                            _save_price_checks(pcs)
                            result = {"ok": True, "pc_id": pc_id, "parse_error": parse_error}
                        else:
                            items = parsed.get("line_items", [])
                            header = parsed.get("header", {})
                            pc_num = header.get("price_check_number", "unknown")
                            institution = header.get("institution", "")
                            due_date = header.get("due_date", "")
                            
                            # Dedup: same PC# + institution + due_date
                            pcs = _load_price_checks()
                            dup_id = None
                            for eid, epc in pcs.items():
                                if (epc.get("pc_number","").strip() == pc_num.strip()
                                        and epc.get("institution","").strip().lower() == institution.strip().lower()
                                        and epc.get("due_date","").strip() == due_date.strip()
                                        and pc_num != "unknown"):
                                    dup_id = eid
                                    break
                            
                            if dup_id:
                                _trace.append(f"DEDUP: PC #{pc_num} already exists as {dup_id}")
                                result = {"dedup": True, "existing_id": dup_id}
                            else:
                                # Cross-queue dedup: check if this PC number exists as an RFQ solicitation
                                _rfq_sols = {v.get("solicitation_number") for v in rfqs.values() if v.get("solicitation_number")}
                                if pc_num in _rfq_sols:
                                    _trace.append(f"SKIP PC: pc_number '{pc_num}' already in RFQ queue as solicitation")
                                    log.info("Cross-queue dedup: PC %s matches RFQ sol %s — skipping PC", pc_num, pc_num)
                                    POLL_STATUS.setdefault("_email_traces", []).append(_trace)
                                    t.ok("Skipped: cross-queue dedup (PC matches existing RFQ sol)")
                                    return None
                                
                                pcs[pc_id] = {
                                    "id": pc_id, "pc_number": pc_num,
                                    "institution": institution, "due_date": due_date,
                                    "requestor": header.get("requestor", ""),
                                    "ship_to": header.get("ship_to", ""),
                                    "items": items, "source_pdf": pc_file,
                                    "status": "parsed", "parsed": parsed,
                                    "created_at": datetime.now().isoformat(),
                                    "source": "email_auto",
                                    "reytech_quote_number": "",
                                    "linked_quote_number": "",
                                }
                                _save_price_checks(pcs)
                                result = {"ok": True, "pc_id": pc_id, "items": len(items)}
                        _trace.append(f"PC result: {result}")
                    except Exception as he:
                        _trace.append(f"PC create EXCEPTION: {he}")
                        result = {"error": str(he)}
                    
                    pcs = _load_price_checks()
                    _trace.append(f"PCs after create: {len(pcs)} ids={list(pcs.keys())[:5]}")
                    
                    if pc_id in pcs:
                        pcs[pc_id]["email_uid"] = email_uid
                        pcs[pc_id]["email_subject"] = rfq_email.get("subject", "")
                        pcs[pc_id]["requestor"] = pcs[pc_id].get("requestor") or rfq_email.get("sender_name") or rfq_email.get("sender_email", "")
                        _save_price_checks(pcs)
                        _ensure_contact_from_email(rfq_email)
                        _trace.append(f"PC CREATED: {pc_id}")
                        log.info("PC %s created successfully from email %s", pc_id, email_uid)
                        POLL_STATUS.setdefault("_email_traces", []).append(_trace)
                        t.ok("PC created", pc_id=pc_id, pc_number=pcs[pc_id].get("pc_number","?"))
                        return None
                    else:
                        # PC wasn't saved — if early detect, force-create a minimal PC
                        if is_early_pc:
                            _trace.append("PC NOT in storage but early-detect → force-creating minimal PC")
                            pcs = _load_price_checks()
                            pcs[pc_id] = {
                                "id": pc_id,
                                "pc_number": rfq_email.get("solicitation_hint", "unknown"),
                                "institution": "", "due_date": "", 
                                "requestor": rfq_email.get("sender_email", ""),
                                "ship_to": "", "items": [],
                                "source_pdf": pc_pdf if pc_pdf else "",
                                "status": "parse_error",
                                "parse_error": "Early-detect PC: parsing failed but sender/subject matched",
                                "created_at": datetime.now().isoformat(),
                                "email_uid": email_uid,
                                "email_subject": rfq_email.get("subject", ""),
                                "source": "email_auto",
                                "reytech_quote_number": "", "linked_quote_number": "",
                            }
                            _save_price_checks(pcs)
                            _ensure_contact_from_email(rfq_email)
                            _trace.append(f"FORCE PC CREATED: {pc_id}")
                            log.info("Force-created PC %s from early-detect email", pc_id)
                            POLL_STATUS.setdefault("_email_traces", []).append(_trace)
                            t.ok("PC force-created (early detect)", pc_id=pc_id)
                            return None
                        _trace.append(f"PC NOT in storage — falling through to RFQ")
                        log.warning("PC creation failed for %s (result=%s) — falling through to RFQ queue",
                                    _subj, result)
        except Exception as _e:
            _trace.append(f"EXCEPTION in PC block: {_e}")
            log.warning("704 detection in email polling: %s", _e)
            # If early-detect flagged this as a PC, create minimal PC rather than broken RFQ
            if is_early_pc:
                try:
                    import uuid as _uuid2
                    _force_id = f"pc_{str(_uuid2.uuid4())[:8]}"
                    pcs = _load_price_checks()
                    pcs[_force_id] = {
                        "id": _force_id,
                        "pc_number": rfq_email.get("solicitation_hint", "unknown"),
                        "institution": "", "due_date": "",
                        "requestor": rfq_email.get("sender_email", rfq_email.get("sender", "")),
                        "ship_to": "", "items": [],
                        "source_pdf": pdf_paths[0] if pdf_paths else "",
                        "status": "parse_error",
                        "parse_error": f"Early-detect exception: {_e}",
                        "created_at": datetime.now().isoformat(),
                        "email_uid": rfq_email.get("email_uid", ""),
                        "email_subject": rfq_email.get("subject", ""),
                        "source": "email_auto",
                        "reytech_quote_number": "", "linked_quote_number": "",
                    }
                    _save_price_checks(pcs)
                    _trace.append(f"FORCE PC on exception: {_force_id}")
                    log.info("Force-created PC %s after exception in early-detect path", _force_id)
                    POLL_STATUS.setdefault("_email_traces", []).append(_trace)
                    t.ok("PC force-created after exception", pc_id=_force_id)
                    return None
                except Exception as _fe:
                    _trace.append(f"Force PC also failed: {_fe}")
                    log.error("Force PC creation failed: %s", _fe)

    # ── Solicitation-number dedup against PC queue ──────────────────────────
    # If this email's solicitation number is already in the PC queue, it's the same
    # Block true duplicates: if this exact email UID was already imported as an RFQ
    try:
        _email_uid = rfq_email.get("email_uid", "")
        if _email_uid:
            for _eid, _erfq in rfqs.items():
                if _erfq.get("email_uid") == _email_uid:
                    log.info("Duplicate RFQ blocked: email UID %s already imported as %s", _email_uid, _eid)
                    return None
    except Exception:
        pass

    templates = {}
    for att in rfq_email["attachments"]:
        if att["type"] != "unknown":
            # Copy template to DATA_DIR so it survives within deploy
            try:
                import shutil as _sh
                tmpl_dir = os.path.join(DATA_DIR, "rfq_templates", rfq_email.get("id", "unknown"))
                os.makedirs(tmpl_dir, exist_ok=True)
                dest = os.path.join(tmpl_dir, os.path.basename(att["path"]))
                _sh.copy2(att["path"], dest)
                templates[att["type"]] = dest
            except Exception:
                templates[att["type"]] = att["path"]  # fallback to original
            # Also persist to DB (survives redeploys)
            try:
                src_path = templates.get(att["type"]) or att["path"]
                if os.path.exists(src_path):
                    with open(src_path, "rb") as _f:
                        save_rfq_file(
                            rfq_email.get("id", "unknown"),
                            att.get("filename", os.path.basename(src_path)),
                            f"template_{att['type']}",
                            _f.read(),
                            category="template",
                        )
            except Exception as _fe:
                log.debug("DB file save for template %s: %s", att.get("filename"), _fe)
    
    # Also store any non-template attachments (e.g. other PDFs)
    for att in rfq_email["attachments"]:
        if att["type"] == "unknown" and att.get("path") and os.path.exists(att["path"]):
            try:
                with open(att["path"], "rb") as _f:
                    save_rfq_file(
                        rfq_email.get("id", "unknown"),
                        att.get("filename", os.path.basename(att["path"])),
                        "attachment",
                        _f.read(),
                        category="attachment",
                    )
            except Exception:
                pass
    
    _trace.append(f"→ RFQ PATH: templates={list(templates.keys())}, attachments={[a.get('filename','?') for a in rfq_email.get('attachments',[])]}")
    
    if "704b" not in templates:
        rfq_data = {
            "id": rfq_email["id"],
            "solicitation_number": rfq_email.get("solicitation_hint", "unknown"),
            "status": "new",
            "source": "email",
            "email_uid": rfq_email.get("email_uid"),
            "email_subject": rfq_email["subject"],
            "email_sender": rfq_email["sender_email"],
            "email_message_id": rfq_email.get("message_id", ""),
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
        rfq_data["email_message_id"] = rfq_email.get("message_id", "")
        rfq_data["line_items"] = bulk_lookup(rfq_data.get("line_items", []))
    
    rfqs[rfq_data["id"]] = rfq_data
    save_rfqs(rfqs)
    POLL_STATUS["emails_found"] += 1
    _trace.append(f"RFQ CREATED: sol={rfq_data.get('solicitation_number','?')}")
    
    # ── Cross-queue cleanup: remove any PC with the same solicitation number ──
    # RFQ takes precedence (has all forms: 703B + 704B + Bid Package)
    sol_num = rfq_data.get("solicitation_number", "")
    if sol_num and sol_num != "unknown":
        try:
            pcs = _load_price_checks()
            pc_dups = [pid for pid, pc in pcs.items()
                       if pc.get("pc_number", "").replace("AD-", "").strip() == sol_num.strip()]
            if pc_dups:
                for pid in pc_dups:
                    del pcs[pid]
                _save_price_checks(pcs)
                _trace.append(f"Cross-queue cleanup: removed {len(pc_dups)} duplicate PC entries: {pc_dups}")
                log.info("Cross-queue cleanup: removed PCs %s (same sol# as RFQ %s)", pc_dups, sol_num)
        except Exception as _xqe:
            _trace.append(f"Cross-queue cleanup error: {_xqe}")
    
    POLL_STATUS.setdefault("_email_traces", []).append(_trace)
    t.ok("RFQ created", sol=rfq_data.get("solicitation_number","?"), rfq_id=rfq_data.get("id","?"))
    log.info(f"Auto-imported RFQ #{rfq_data.get('solicitation_number', 'unknown')}")
    
    # Log activity
    _log_rfq_activity(rfq_data["id"], "created",
        f"RFQ #{rfq_data.get('solicitation_number','?')} imported from email: {rfq_email.get('subject','')}",
        actor="system",
        metadata={"source": "email", "templates": list(templates.keys()),
                  "attachments": [a.get("filename","?") for a in rfq_email.get("attachments",[])]})
    
    # Ensure sender is in CRM
    _ensure_contact_from_email(rfq_email)

    # ── Auto Price Lookup (no quote generation) ─────────────────────────────
    # Creates PC + runs price lookup, but STOPS there.
    # User manually clicks "Generate Quote" when ready → that's when R26Qxx is assigned.
    _trigger_auto_price(rfq_data)

    return rfq_data


def _trigger_auto_price(rfq_data: dict):
    """Auto-price pipeline: Email → PC → price lookup → STOP (no quote).
    
    User manually clicks "Generate Quote" when ready.
    No quote numbers consumed until user action.
    """
    if rfq_data.get("auto_price_pc_id"):
        return  # already processed
    if not rfq_data.get("line_items"):
        log.info("Auto-price skipped: no line items in RFQ %s", rfq_data.get("id"))
        return

    import threading as _t
    def _run():
        try:
            _auto_price_pipeline(rfq_data)
        except Exception as e:
            log.error("Auto-price pipeline error for %s: %s", rfq_data.get("id"), e)

    _t.Thread(target=_run, daemon=True, name=f"auto-price-{rfq_data.get('id','?')[:8]}").start()
    log.info("Auto-price pipeline started for RFQ %s", rfq_data.get("solicitation_number"))


def _auto_price_pipeline(rfq_data: dict):
    """Execute auto-price pipeline (runs in background thread).
    
    Steps: Create PC → smart match for revisions → price lookup → notify user.
    Does NOT generate a quote — user does that manually.
    """
    import time as _time
    rfq_id = rfq_data.get("id", "")
    sol = rfq_data.get("solicitation_number", "?")
    items = rfq_data.get("line_items", [])

    t = Trace("auto_price", rfq_id=rfq_id, sol=sol, item_count=len(items))
    log.info("[AutoPrice] Starting pipeline for RFQ %s (%d items)", sol, len(items))
    t0 = _time.time()

    # Step 1: Smart match — check if this is a revision of an existing PC
    pcs = _load_price_checks()
    revision_of = None
    try:
        from src.agents.award_monitor import smart_match_pc
        match = smart_match_pc(rfq_data, pcs)
        if match:
            revision_of = match["pc_id"]
            t.step("Revision detected", revision_of=revision_of, score=match["score"])
            log.info("[AutoPrice] Revision detected: %s is update of PC %s (score=%d: %s)",
                     sol, revision_of, match["score"], ", ".join(match["reasons"]))
    except Exception as e:
        log.debug("Smart match error (non-critical): %s", e)

    # Step 2: Create Price Check record
    pc_id = f"auto_{rfq_id[:8]}_{int(_time.time())}"
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
        "pc_number": sol if sol != "?" else f"PC-{pc_id[:8]}",
        "status": "parsed",
        "source": "email_auto_draft",
        "is_auto_draft": True,
        "rfq_id": rfq_id,
        "solicitation_number": sol,
        "institution": rfq_data.get("department", rfq_data.get("requestor_name", "")),
        "requestor": rfq_data.get("requestor_name", rfq_data.get("requestor_email", "")),
        "contact_email": rfq_data.get("requestor_email", ""),
        "items": pc_items,
        "parsed": {"header": {
            "institution": rfq_data.get("department", ""),
            "requestor": rfq_data.get("requestor_name", ""),
            "phone": rfq_data.get("phone", ""),
        }},
        "created_at": datetime.now().isoformat(),
        "revision_of": revision_of,
    }
    pcs[pc_id] = pc
    _save_price_checks(pcs)

    # Step 3: Run SCPRS + Amazon price lookup
    try:
        from src.forms.price_check import lookup_prices
        pc = lookup_prices(pc)
        pc["status"] = "priced"
        pcs[pc_id] = pc
        _save_price_checks(pcs)
        priced = sum(1 for it in pc.get("items", []) if it.get("pricing", {}).get("recommended_price"))
        t.step("Prices looked up", priced=priced, total=len(pc_items))
        log.info("[AutoPrice] Prices found: %d/%d items", priced, len(pc_items))
    except Exception as pe:
        t.warn("Price lookup failed", error=str(pe))
        log.warning("[AutoPrice] Price lookup failed: %s", pe)

    # Step 4: Check for competitor price suggestions
    suggestions = []
    try:
        from src.agents.award_monitor import get_price_suggestions
        suggestions = get_price_suggestions(pc_items, pc.get("institution", ""))
        if suggestions:
            pc["price_suggestions"] = suggestions
            pcs[pc_id] = pc
            _save_price_checks(pcs)
            log.info("[AutoPrice] %d price suggestions from competitor history", len(suggestions))
    except Exception:
        pass

    # Step 5: Update RFQ with PC link
    rfqs = load_rfqs()
    if rfq_id in rfqs:
        rfqs[rfq_id]["auto_price_pc_id"] = pc_id
        rfqs[rfq_id]["auto_priced_at"] = datetime.now().isoformat()
        rfqs[rfq_id]["status"] = "priced"
        save_rfqs(rfqs)

    # Step 6: Log activity
    _log_crm_activity(
        pc_id,
        "pc_auto_priced",
        f"PC {pc.get('pc_number','')} auto-priced from email: {len(pc_items)} items"
        + (f" (revision of {revision_of})" if revision_of else "")
        + (f", {len(suggestions)} price warnings" if suggestions else ""),
        actor="system",
        metadata={"rfq_id": rfq_id, "pc_id": pc_id, "revision_of": revision_of},
    )

    # Step 7: Notify user — PC is ready for review (NOT a draft quote)
    try:
        from src.agents.notify_agent import send_alert
        contact = rfq_data.get("requestor_email", "") or rfq_data.get("requestor_name", "")
        title = f"📋 PC Ready: {pc.get('pc_number', sol)}"
        body = f"Price check from {contact}: {len(pc_items)} items priced."
        if revision_of:
            body += f" ⚠️ Revision of existing PC."
        if suggestions:
            body += f" ⚠️ {len(suggestions)} competitive price warnings."
        body += " Review and click Generate Quote when ready."
        send_alert(
            event_type="pc_ready",
            title=title, body=body, urgency="info",
            context={"pc_id": pc_id, "contact": contact},
            cooldown_key=f"pc_ready_{pc_id}",
        )
    except Exception as _ne:
        log.debug("PC ready alert error: %s", _ne)

    log.info("[AutoPrice] Complete for %s in %.1fs (no quote generated — user action required)",
             sol, _time.time() - t0)
    t.ok("Auto-price complete", duration_s=round(_time.time() - t0, 1), pc_id=pc_id)



def do_poll_check():
    """Run a single email poll check. Used by both background thread and manual trigger."""
    global _shared_poller
    t = Trace("email_poll")
    email_cfg = CONFIG.get("email", {})
    
    if not email_cfg.get("email_password"):
        POLL_STATUS["error"] = "Email password not configured"
        log.error("Poll check failed: no email_password in config")
        return []
    
    # Always destroy and recreate poller to pick up fresh processed_emails.json
    # (system-reset clears the file but old poller has stale in-memory set)
    _shared_poller = None
    
    # Force absolute path for processed file
    email_cfg = dict(email_cfg)  # copy so we don't mutate
    email_cfg["processed_file"] = os.path.join(DATA_DIR, "processed_emails.json")
    _shared_poller = EmailPoller(email_cfg)
    processed_count = len(_shared_poller._processed)
    log.info(f"Created fresh poller for {email_cfg.get('email', 'NO EMAIL SET')}, "
             f"processed: {processed_count} UIDs loaded")
    
    # Store diagnostics for debugging
    POLL_STATUS["_diag"] = {
        "processed_loaded": processed_count,
        "imap_connected": False,
        "uids_found": 0,
        "uids_new": 0,
        "rfqs_returned": 0,
        "pcs_routed": 0,
        "errors": [],
    }
    POLL_STATUS["_email_traces"] = []  # Per-email traces from process_rfq_email
    
    imported = []
    try:
        connected = _shared_poller.connect()
        POLL_STATUS["_diag"]["imap_connected"] = connected
        if connected:
            log.info("IMAP connected, checking for RFQs...")
            rfq_emails = _shared_poller.check_for_rfqs(save_dir=UPLOAD_DIR)
            POLL_STATUS["last_check"] = _pst_now_iso()
            POLL_STATUS["error"] = None
            POLL_STATUS["_diag"]["rfqs_returned"] = len(rfq_emails)
            # Capture poller-level diagnostics
            if hasattr(_shared_poller, '_diag'):
                POLL_STATUS["_diag"]["poller"] = _shared_poller._diag
            log.info(f"Poll check complete: {len(rfq_emails)} RFQ emails found")
            
            for rfq_email in rfq_emails:
                try:
                    rfq_data = process_rfq_email(rfq_email)
                    if rfq_data:
                        imported.append(rfq_data)
                    else:
                        POLL_STATUS["_diag"]["pcs_routed"] += 1
                except Exception as pe:
                    POLL_STATUS["_diag"]["errors"].append(f"process_rfq({rfq_email.get('subject','?')[:40]}): {pe}")
                    log.error("process_rfq_email error for '%s': %s", rfq_email.get("subject","?")[:50], pe, exc_info=True)
        else:
            POLL_STATUS["error"] = f"IMAP connect failed for {email_cfg.get('email', '?')}"
            log.error(POLL_STATUS["error"])
    except Exception as e:
        POLL_STATUS["error"] = str(e)
        POLL_STATUS["_diag"]["errors"].append(str(e))
        log.error(f"Poll error: {e}", exc_info=True)
        _shared_poller = None
        t.fail("Poll error", error=str(e))
    
    if t.status == "running":
        pcs_routed = POLL_STATUS.get("_diag", {}).get("pcs_routed", 0)
        t.ok("Poll complete", rfqs_imported=len(imported), pcs_routed=pcs_routed)
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
        if not POLL_STATUS.get("paused"):
            do_poll_check()
        else:
            log.debug("Email poller paused (system reset in progress)")
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
# Shared Manager Brief (app-wide) + Header JS
# ═══════════════════════════════════════════════════════════════════════

BRIEF_HTML = """<!-- Manager Brief — app-wide, loads via AJAX with sessionStorage cache -->
<div id="brief-section" class="card" style="margin-bottom:14px;display:none">
 <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px">
  <div style="min-width:0;flex:1">
   <div class="card-t" style="margin:0;display:flex;align-items:center;gap:8px;cursor:pointer" onclick="toggleBriefBody()">
    &#x1F9E0; Manager Brief
    <span id="brief-badge" style="font-size:10px;padding:2px 8px;border-radius:10px;background:var(--sf2);color:var(--tx2);font-weight:500"></span>
    <span id="brief-toggle" style="font-size:10px;color:var(--tx2);margin-left:4px">&#x25BC;</span>
   </div>
   <div id="brief-headline" style="font-size:15px;font-weight:600;margin-top:8px;line-height:1.4"></div>
  </div>
  <div style="display:flex;gap:6px;align-items:center;flex-shrink:0">
   <a href="/agents" class="btn btn-sm btn-s" style="font-size:10px;padding:4px 10px;white-space:nowrap">&#x1F4CA; Full Report</a>
   <button onclick="loadBrief(true)" id="brief-refresh-btn" style="font-size:10px;padding:4px 8px;background:rgba(79,140,255,.1);border:1px solid rgba(79,140,255,.3);color:var(--ac);border-radius:6px;cursor:pointer;white-space:nowrap">&#x1F504; Refresh</button>
  </div>
 </div>
 <div id="brief-body">
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px" id="brief-grid">
   <div>
    <div style="font-size:11px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;display:flex;align-items:center;gap:6px">
     Needs Your Attention <span id="approval-count" class="brief-count"></span>
    </div>
    <div id="approvals-list"></div>
   </div>
   <div>
    <div style="font-size:11px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Recent Activity</div>
    <div id="activity-list"></div>
   </div>
  </div>
  <div id="pipeline-bar" style="display:flex;gap:12px;flex-wrap:wrap;margin-top:16px;padding-top:14px;border-top:1px solid var(--bd)"></div>
  <div id="agents-row" style="display:none;margin-top:16px;padding-top:14px;border-top:1px solid var(--bd)">
   <div style="font-size:10px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Agent Status</div>
   <div id="agents-list" style="display:flex;gap:10px;flex-wrap:wrap"></div>
  </div>
 </div>
</div>"""

# Brief JS — sessionStorage cached (60s), instant on page nav, background refresh when stale
BRIEF_JS = """
function toggleBriefBody(){
 var body=document.getElementById('brief-body');var tog=document.getElementById('brief-toggle');
 if(!body)return;
 if(body.style.display==='none'){body.style.display='';tog.innerHTML='\\u25BC';}
 else{body.style.display='none';tog.innerHTML='\\u25B6';}
}
function loadBrief(force){
 var btn=document.getElementById('brief-refresh-btn');
 if(!document.getElementById('brief-section'))return;
 // SessionStorage cache — instant render on page nav
 if(!force){
  try{
   var cached=sessionStorage.getItem('_brief_data');
   var cachedTs=parseInt(sessionStorage.getItem('_brief_ts')||'0',10);
   if(cached&&(Date.now()-cachedTs)<60000){
    renderBrief(JSON.parse(cached));
    // Background refresh if >30s old
    if(Date.now()-cachedTs>30000){
     fetch('/api/manager/brief',{credentials:'same-origin'}).then(function(r){return r.json()}).then(function(d){
      if(d.ok){sessionStorage.setItem('_brief_data',JSON.stringify(d));sessionStorage.setItem('_brief_ts',''+Date.now());renderBrief(d)}
     }).catch(function(){});
    }
    return;
   }
  }catch(e){}
 }
 if(btn){btn.disabled=true;btn.textContent='\\u23F3';}
 var url='/api/manager/brief';if(force)url+='?nocache=1';
 fetch(url,{credentials:'same-origin'}).then(function(r){
  if(!r.ok)throw new Error('HTTP '+r.status);return r.json();
 }).then(function(data){
  if(!data.ok)return;
  try{sessionStorage.setItem('_brief_data',JSON.stringify(data));sessionStorage.setItem('_brief_ts',''+Date.now())}catch(e){}
  renderBrief(data);
 }).catch(function(err){
  console.error('Manager brief:',err);
  var sec=document.getElementById('brief-section');if(sec)sec.style.display='block';
  var badge=document.getElementById('brief-badge');
  if(badge){badge.textContent='retry';badge.style.background='rgba(251,191,36,.15)';badge.style.color='#fbbf24';badge.style.cursor='pointer';badge.onclick=function(){loadBrief(true);};}
  var hdr=document.getElementById('brief-headline');
  if(hdr){hdr.textContent='Click Refresh to load brief';hdr.style.color='#8b949e';}
 }).finally(function(){if(btn){btn.disabled=false;btn.textContent='🔄 Refresh';}});
}
function renderBrief(data){
 var sec=document.getElementById('brief-section');if(!sec)return;sec.style.display='block';
 var hl=document.getElementById('brief-headline');if(hl)hl.textContent=data.headline||'All clear';
 var badge=document.getElementById('brief-badge');
 if(badge){if(data.approval_count>0){badge.textContent=data.approval_count+' pending';badge.style.background='rgba(251,191,36,.15)';badge.style.color='#fbbf24';}else{badge.textContent='all clear';badge.style.background='rgba(52,211,153,.15)';badge.style.color='#34d399';}}
 var ac=document.getElementById('approval-count');if(ac&&data.approval_count>0)ac.textContent=data.approval_count;
 var al=document.getElementById('approvals-list');
 if(al){if(data.pending_approvals&&data.pending_approvals.length>0){al.innerHTML=data.pending_approvals.map(function(a){return '<div class="brief-item"><div class="brief-item-left"><span class="brief-icon">'+a.icon+'</span><div><div class="brief-title">'+a.title+'</div>'+(a.detail?'<div class="brief-detail">'+a.detail+'</div>':'')+'</div></div>'+(a.age?'<span class="brief-age">'+a.age+'</span>':'')+'</div>';}).join('');}else{al.innerHTML='<div class="brief-empty">Nothing pending \\u2014 all caught up</div>';}}
 var actList=document.getElementById('activity-list');
 if(actList){if(data.activity&&data.activity.length>0){actList.innerHTML=data.activity.map(function(a){return '<div class="brief-item"><div class="brief-item-left"><span class="brief-icon">'+a.icon+'</span><div><div class="brief-title">'+a.text+'</div>'+(a.detail?'<div class="brief-detail">'+a.detail+'</div>':'')+'</div></div>'+(a.age?'<span class="brief-age">'+a.age+'</span>':'')+'</div>';}).join('');}else{actList.innerHTML='<div class="brief-empty">No recent activity</div>';}}
 var bar=document.getElementById('pipeline-bar');
 if(bar){var s=data.summary||{};var q=s.quotes||{};var gr=s.growth||{};var ob=s.outbox||{};var rv=data.revenue||{};var ag=data.agents_summary||{};var stats=[{label:'Quotes',value:q.total||0,color:'var(--ac)'},{label:'Pipeline $',value:'$'+(q.pipeline_value||0).toLocaleString(),color:'var(--yl)'},{label:'Won $',value:'$'+(q.won_total||0).toLocaleString(),color:'var(--gn)'},{label:'Win Rate',value:(q.win_rate||0)+'%',color:q.win_rate>=50?'var(--gn)':'var(--yl)'},{label:'Growth',value:gr.total_prospects||0,color:'#bc8cff'},{label:'Agents',value:(ag.healthy||0)+'/'+(ag.total||0),color:ag.down>0?'var(--rd)':'var(--gn)'},{label:'Goal',value:rv.pct?rv.pct.toFixed(0)+'%':'0%',color:rv.pct>=50?'var(--gn)':rv.pct>=25?'var(--yl)':'var(--rd)'},{label:'Drafts',value:ob.drafts||0,color:ob.drafts>0?'var(--yl)':'var(--tx2)'}];bar.innerHTML=stats.map(function(s){return '<div class="stat-chip"><div class="stat-val" style="color:'+s.color+'">'+s.value+'</div><div class="stat-label">'+s.label+'</div></div>';}).join('');}
 var agRow=document.getElementById('agents-row');var agList=document.getElementById('agents-list');
 if(agRow&&agList&&data.agents&&data.agents.length>0){agRow.style.display='block';agList.innerHTML=data.agents.map(function(a){var isOk=a.status==='active'||a.status==='ready'||a.status==='connected';var isWait=a.status==='not configured'||a.status==='waiting';var color=isOk?'#3fb950':isWait?'#d29922':'#f85149';var bg=isOk?'rgba(52,211,153,.08)':isWait?'rgba(251,191,36,.08)':'rgba(248,113,113,.08)';var border=isOk?'rgba(52,211,153,.25)':isWait?'rgba(251,191,36,.25)':'rgba(248,113,113,.25)';return '<span style="font-size:13px;padding:8px 14px;border-radius:8px;background:'+bg+';border:1px solid '+border+';display:inline-flex;align-items:center;gap:7px;font-weight:500"><span style="width:10px;height:10px;border-radius:50%;background:'+color+';display:inline-block;flex-shrink:0;box-shadow:0 0 6px '+color+'66"></span><span style="font-size:15px">'+a.icon+'</span><span>'+a.name+'</span></span>';}).join('');}
}
loadBrief();
"""

# Shared header JS — pollNow, resync, notifications, poll time. Injected into BOTH render() and _header() pages.
SHARED_HEADER_JS = """
function _updatePollTime(ts){
 var el=document.getElementById('poll-time');
 if(el&&ts){el.dataset.utc=ts;try{var d=new Date(ts);if(!isNaN(d)){el.textContent=d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true})}}catch(e){el.textContent=ts}}
}
function pollNow(btn){
 btn.disabled=true;btn.setAttribute('aria-busy','true');btn.textContent='Checking...';
 fetch('/api/poll-now',{credentials:'same-origin'}).then(function(r){return r.json()}).then(function(d){
  _updatePollTime(d.last_check);
  if(d.found>0){btn.textContent=d.found+' found!';setTimeout(function(){location.reload()},800)}
  else{btn.textContent='No new emails';setTimeout(function(){btn.textContent='\\u26A1 Check Now';btn.disabled=false;btn.removeAttribute('aria-busy')},2000)}
 }).catch(function(){btn.textContent='Error';setTimeout(function(){btn.textContent='\\u26A1 Check Now';btn.disabled=false;btn.removeAttribute('aria-busy')},2000)});
}
function resyncAll(btn){
 if(!confirm('Clear all RFQs and re-import from email?'))return;
 btn.disabled=true;btn.setAttribute('aria-busy','true');btn.textContent='Syncing...';
 fetch('/api/resync',{credentials:'same-origin'}).then(function(r){return r.json()}).then(function(d){
  _updatePollTime(d.last_check);
  if(d.found>0){btn.textContent=d.found+' imported!';setTimeout(function(){location.reload()},800)}
  else{btn.textContent='0 found';setTimeout(function(){btn.textContent='Resync';btn.disabled=false},2000)}
 }).catch(function(){btn.textContent='Error';setTimeout(function(){btn.textContent='Resync';btn.disabled=false},2000)});
}
(function initBell(){
  function updateBellCount(){
    fetch('/api/notifications/bell-count',{credentials:'same-origin'})
    .then(function(r){return r.json()}).then(function(d){
      var badge=document.getElementById('notif-badge');
      if(!badge||!d.ok)return;
      var total=d.total_badge||0;
      if(total>0){badge.textContent=total>99?'99+':total;badge.classList.add('show')}
      else{badge.classList.remove('show')}
      var csEl=document.getElementById('notif-cs-count');
      if(csEl&&d.cs_drafts>0){csEl.textContent=d.cs_drafts+' CS draft(s)';csEl.style.display='inline'}
      else if(csEl){csEl.style.display='none'}
    }).catch(function(){});
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
  .then(function(r){return r.json()}).then(function(d){
    var list=document.getElementById('notif-list');
    if(!list)return;
    if(!d.notifications||d.notifications.length===0){
      list.innerHTML='<div class="notif-empty">No notifications yet.</div>';
      return;
    }
    var IC={urgent:'\\u1F6A8',deal:'\\u1F4B0',draft:'\\u1F4CB',warning:'\\u23F0',info:'\\u2139\\uFE0F'};
    list.innerHTML=d.notifications.map(function(n){
      var ts=n.created_at?new Date(n.created_at).toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true}):'';
      var icon=IC[n.urgency]||'\\u1F514';
      return '<div class="notif-item '+(n.is_read?'':'unread')+' urgency-'+(n.urgency||'info')+'" onclick="notifClick(&apos;'+n.deep_link+'&apos;,'+n.id+')">'
        +'<div class="notif-item-title">'+icon+' '+(n.title||'')+'</div>'
        +'<div class="notif-item-body">'+(n.body||'').substring(0,120)+'</div>'
        +'<div class="notif-item-time">'+ts+'</div>'
        +'</div>';
    }).join('');
  }).catch(function(){
    var list=document.getElementById('notif-list');
    if(list)list.innerHTML='<div class="notif-empty">Could not load notifications.</div>';
  });
}
function notifClick(link,id){
  if(id) fetch('/api/notifications/mark-read',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({ids:[id]})});
  if(link&&link!=='/'){window.location.href=link}
  var p=document.getElementById('notif-panel');if(p)p.classList.remove('open');
}
function markAllRead(){
  fetch('/api/notifications/mark-read',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({})})
  .then(function(){
    var b=document.getElementById('notif-badge');if(b)b.classList.remove('show');
    loadNotifications();
  });
}
document.addEventListener('click',function(e){
  var wrap=document.getElementById('notif-wrap');
  if(wrap&&!wrap.contains(e.target)){
    var panel=document.getElementById('notif-panel');
    if(panel)panel.classList.remove('open');
  }
});
(function(){
 var el=document.getElementById('poll-time');
 if(el&&el.dataset.utc){
  try{var d=new Date(el.dataset.utc);if(!isNaN(d)){el.textContent=d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true})}}catch(e){}
 }
})();
"""

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
<!-- BUILD: v20260220-1005-pdf-v4 -->
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
  <a href="/pricechecks" class="hdr-btn" aria-label="PC Archive">📋 PCs</a>
  <a href="/competitors" class="hdr-btn" aria-label="Competitor Intelligence">🎯 Compete</a>
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
""" + BRIEF_HTML + content + """
</main>
<script>""" + SHARED_HEADER_JS + BRIEF_JS + """</script>
</div></body></html>"""

    # Add volume warning if on Railway without persistent storage
    try:
        from src.core.paths import _USING_VOLUME
        if not _USING_VOLUME and os.environ.get("RAILWAY_ENVIRONMENT"):
            _warn_html = '<div style="background:#f8514920;border:1px solid #f85149;color:#f85149;padding:8px 16px;text-align:center;font-size:13px;font-weight:600;position:fixed;bottom:0;left:0;right:0;z-index:9999">⚠️ No persistent volume — data resets on each deploy. Add volume in Railway: Service → Storage → Mount: /data</div>'
            html = html.replace('</body>', _warn_html + '</body>')
    except Exception:
        pass

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
  <a href="/pricechecks" class="hdr-btn">📋 PCs</a>
  <a href="/competitors" class="hdr-btn">🎯 Compete</a>
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
<div class="ctr">""" + BRIEF_HTML + "<script>" + SHARED_HEADER_JS + BRIEF_JS + "</script>"


# ═══════════════════════════════════════════════════════════════════════
# Trace API — view workflow diagnostics
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/admin/traces")
@auth_required
def api_admin_traces():
    """View recent workflow traces. ?workflow=X to filter, ?status=fail for failures only."""
    from src.api.trace import get_traces, get_summary
    workflow = request.args.get("workflow")
    status = request.args.get("status")
    limit = int(request.args.get("limit", 50))
    
    if request.args.get("summary") == "1":
        return jsonify(get_summary())
    
    traces = get_traces(workflow=workflow, status=status, limit=limit)
    return jsonify({"traces": traces, "count": len(traces)})

@bp.route("/api/admin/traces/<trace_id>")
@auth_required
def api_admin_trace_detail(trace_id):
    """View a single trace with full step details."""
    from src.api.trace import get_trace
    t = get_trace(trace_id)
    if not t:
        return jsonify({"error": "Trace not found"}), 404
    return jsonify(t)

@bp.route("/api/admin/traces", methods=["DELETE"])
@auth_required
def api_admin_traces_clear():
    """Clear all traces."""
    from src.api.trace import clear_traces
    clear_traces()
    return jsonify({"ok": True, "cleared": True})


@bp.route("/api/qa/trace-diagnostic")
@auth_required
def api_qa_trace_diagnostic():
    """One-command auto-diagnosis. Analyzes traces for known failure patterns
    and returns specific fix instructions.
    
    Usage: fetch('/api/qa/trace-diagnostic').then(r=>r.json()).then(d=>console.log(JSON.stringify(d,null,2)))
    """
    from src.api.trace import get_traces, get_summary
    
    diag = {
        "status": "healthy",
        "bugs_detected": [],
        "warnings": [],
        "stats": {},
        "fix_commands": [],
        "raw_failures": [],
    }
    
    summary = get_summary()
    diag["stats"] = {
        "total_traces": summary.get("total", 0),
        "ok": summary.get("ok", 0),
        "fail": summary.get("fail", 0),
        "warn": summary.get("warn", 0),
        "running": summary.get("running", 0),
        "workflows": summary.get("workflows", {}),
    }
    
    if summary.get("total", 0) == 0:
        diag["status"] = "no_data"
        diag["fix_commands"].append({
            "description": "Run email poll to generate trace data",
            "command": "fetch('/api/admin/reset-and-poll',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({keep_quotes:[],counter:15})}).then(r=>r.json()).then(d=>console.log(d))",
        })
        diag["fix_commands"].append({
            "description": "Then check results (wait 30s)",
            "command": "fetch('/api/admin/poll-result').then(r=>r.json()).then(d=>console.log(JSON.stringify(d,null,2)))",
        })
        return jsonify(diag)
    
    # ── Analyze all traces for known bug patterns ──
    all_fails = get_traces(status="fail", limit=50)
    pipeline = get_traces(workflow="email_pipeline", limit=50)
    polls = get_traces(workflow="email_poll", limit=10)
    
    # Bug #10: 'bp not defined'
    bp_count = 0
    for t in pipeline:
        msgs = " ".join(s.get("msg", "") for s in t.get("steps", []))
        if "bp" in msgs and "not defined" in msgs:
            bp_count += 1
    if bp_count > 0:
        diag["status"] = "critical"
        diag["bugs_detected"].append({
            "id": "BUG_BP_NOT_DEFINED",
            "severity": "critical",
            "count": bp_count,
            "description": "Import from routes_rfq.py fails because @bp.route decorators execute at import time before bp is injected. Every _is_price_check and _handle_price_check_upload call throws NameError.",
            "fix": "All PC logic must be inlined in dashboard.py — never import from routes_rfq.py at runtime.",
        })
    
    # Bug #9: stale cache / wrong filename
    cache_skip_count = 0
    for t in pipeline:
        msgs = " ".join(s.get("msg", "") for s in t.get("steps", []))
        if "duplicate email_uid in RFQ queue" in msgs and len(t.get("steps", [])) <= 3:
            cache_skip_count += 1
    if cache_skip_count > len(pipeline) * 0.5 and cache_skip_count > 3:
        diag["status"] = "critical"
        diag["bugs_detected"].append({
            "id": "BUG_STALE_CACHE_OR_WRONG_FILE",
            "severity": "critical",
            "count": cache_skip_count,
            "description": f"{cache_skip_count}/{len(pipeline)} emails hit dedup immediately — either JSON cache is stale (not invalidated after reset) or reset cleared rfq_queue.json but code reads rfqs.json.",
            "fix": "Verify: (1) _json_cache.clear() after file reset, (2) rfq_db_path() returns rfqs.json and that's what reset clears.",
        })
    
    # PC routing: all going to RFQ
    pc_count = sum(1 for t in pipeline if any("PC created" in s.get("msg","") for s in t.get("steps",[])))
    rfq_count = sum(1 for t in pipeline if any("RFQ created" in s.get("msg","") for s in t.get("steps",[])))
    if pc_count == 0 and rfq_count > 3:
        sev = "warn" if bp_count > 0 else "critical"  # if bp error explains it, just warn
        diag["bugs_detected"].append({
            "id": "BUG_NO_PC_ROUTING",
            "severity": sev,
            "description": f"0 PCs created, {rfq_count} RFQs — price checks not being detected or routing to PC queue fails.",
            "fix": "Check _is_pc_filename() detection. Verify PRICE_CHECK_AVAILABLE=True and filename pattern 'ams' + '704' matches.",
        })
        if diag["status"] != "critical":
            diag["status"] = "degraded"
    
    # IMAP failures
    poll_fails = [t for t in polls if t["status"] == "fail"]
    if poll_fails:
        diag["status"] = "critical" if not diag["bugs_detected"] else diag["status"]
        last_err = poll_fails[0].get("steps", [{}])[-1].get("msg", "unknown")
        diag["bugs_detected"].append({
            "id": "BUG_IMAP_FAILURE",
            "severity": "critical",
            "description": f"IMAP poll failing: {last_err}",
            "fix": "Check email credentials in reytech_config.json and IMAP server connectivity.",
        })
    
    # Parse errors
    parse_err_count = 0
    for t in pipeline:
        msgs = " ".join(s.get("msg", "") for s in t.get("steps", []))
        if "parse_ams704" in msgs and ("error" in msgs.lower() or "FAIL" in msgs):
            parse_err_count += 1
    if parse_err_count > 2:
        diag["warnings"].append({
            "id": "WARN_PARSE_ERRORS",
            "count": parse_err_count,
            "description": f"parse_ams704 failed on {parse_err_count} PDFs (minimal PCs created as fallback).",
            "fix": "Check: pypdf installed, PDFs not encrypted/scanned. Fallback creates parse_error PCs.",
        })
    
    # Silent drops (traces stuck in 'running')
    running_count = sum(1 for t in pipeline if t["status"] == "running")
    if running_count > 0:
        diag["warnings"].append({
            "id": "WARN_INCOMPLETE_TRACES",
            "count": running_count,
            "description": f"{running_count} emails have traces stuck in 'running' — never reached ok/fail.",
            "fix": "Missing t.ok() or t.fail() call, or uncaught exception killed processing mid-trace.",
        })
    
    # Set overall status if no bugs found
    if not diag["bugs_detected"] and not diag["warnings"]:
        diag["status"] = "healthy"
    elif not diag["bugs_detected"]:
        diag["status"] = "minor_issues"
    
    # Add raw failure summaries
    diag["raw_failures"] = [
        {"summary": t.get("summary", ""), "workflow": t.get("workflow", ""), "id": t.get("id", "")}
        for t in all_fails[:10]
    ]
    
    # Generate actionable fix commands
    if diag["bugs_detected"]:
        diag["fix_commands"].append({
            "description": "View all failure traces",
            "command": "fetch('/api/admin/traces?status=fail').then(r=>r.json()).then(d=>d.traces.forEach(t=>console.log(t.summary)))",
        })
    diag["fix_commands"].append({
        "description": "View email pipeline traces",
        "command": "fetch('/api/admin/traces?workflow=email_pipeline').then(r=>r.json()).then(d=>d.traces.forEach(t=>console.log(t.summary)))",
    })
    diag["fix_commands"].append({
        "description": "Re-run full pipeline (reset + poll)",
        "command": "fetch('/api/admin/reset-and-poll',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({keep_quotes:[],counter:15})}).then(r=>r.json()).then(d=>console.log(d))",
    })
    diag["fix_commands"].append({
        "description": "Check poll result (30s later)",
        "command": "fetch('/api/admin/poll-result').then(r=>r.json()).then(d=>{console.log('PCs:',d.final_pcs,'RFQs:',d.final_rfqs);(d.email_traces||[]).forEach((t,i)=>console.log('E'+(i+1)+':',t.join(' → ')))})",
    })
    diag["fix_commands"].append({
        "description": "Run full QA health check",
        "command": "fetch('/api/qa/health').then(r=>r.json()).then(d=>console.log(d.grade, d.health_score+'/100', d.recommendations.join('; ')))",
    })
    
    return jsonify(diag)


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

# ── Start Award Monitor (checks SCPRS for PO awards on sent quotes) ─────
try:
    from src.agents.award_monitor import start_monitor as _start_award_monitor
    _start_award_monitor()
    log.info("Award monitor started (checks every 1h, SCPRS every 3 biz days)")
except Exception as _e:
    log.warning("Award monitor failed to start: %s", _e)
