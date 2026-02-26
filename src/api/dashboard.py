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

if DASH_PASS == "changeme":
    log.warning("⚠️  SECURITY: DASH_PASS is set to default 'changeme'. Set DASH_PASS env var for production!")


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
    data = {}
    if os.path.exists(path):
        try:
            import fcntl
            with open(path) as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)  # shared read lock
                data = json.load(f)
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except ImportError:
            with open(path) as f:
                data = json.load(f)
        except Exception:
            data = {}

    # ── Restore from SQLite if JSON is empty (post-deploy recovery) ──
    if not data:
        try:
            from src.core.db import get_db
            with get_db() as conn:
                rows = conn.execute(
                    "SELECT * FROM price_checks WHERE status NOT IN ('dismissed','cancelled')"
                ).fetchall()
                if rows:
                    for r in rows:
                        pc_id = r["id"]
                        items = []
                        try:
                            items = json.loads(r["items"] or "[]")
                        except Exception as _e:
                            log.debug("Suppressed: %s", _e)
                        data[pc_id] = {
                            "id": pc_id,
                            "pc_number": r.get("pc_number") or r.get("quote_number") or pc_id,
                            "institution": r.get("institution") or r["agency"] or "",
                            "requestor": r["requestor"] or "",
                            "items": items,
                            "source_pdf": r.get("source_file") or "",
                            "status": r["status"] or "parsed",
                            "created_at": r["created_at"] or "",
                            "reytech_quote_number": r.get("quote_number") or "",
                            "email_uid": r.get("email_uid") or "",
                            "email_subject": r.get("email_subject") or "",
                            "due_date": r.get("due_date") or "",
                            "source": "email_auto",
                        }
                    if data:
                        _save_price_checks(data)
                        log.info("Restored %d price checks from SQLite → JSON", len(data))
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    # ── End SQLite restore ────────────────────────────────────────────

    return data

def _save_price_checks(pcs):
    path = os.path.join(DATA_DIR, "price_checks.json")
    try:
        import fcntl
        with open(path, "w") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)  # exclusive write lock
            json.dump(pcs, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except ImportError:
        with open(path, "w") as f:
            json.dump(pcs, f, indent=2, default=str)

    # ── Sync to SQLite for persistence across deploys ─────────────
    try:
        from src.core.db import get_db
        with get_db() as conn:
            for pc_id, pc in pcs.items():
                items_json = json.dumps(pc.get("items", []))
                conn.execute("""
                    INSERT OR REPLACE INTO price_checks
                    (id, created_at, requestor, agency, institution, items, source_file,
                     quote_number, pc_number, total_items, status,
                     email_uid, email_subject, due_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    pc_id,
                    pc.get("created_at", ""),
                    pc.get("requestor", ""),
                    pc.get("institution", "") or pc.get("agency", ""),
                    pc.get("institution", "") or pc.get("agency", ""),
                    items_json,
                    pc.get("source_pdf", ""),
                    pc.get("reytech_quote_number", ""),
                    pc.get("pc_number", ""),
                    len(pc.get("items", [])),
                    pc.get("status", "parsed"),
                    pc.get("email_uid", ""),
                    pc.get("email_subject", ""),
                    pc.get("due_date", ""),
                ))
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    # ── End SQLite sync ───────────────────────────────────────────


def _merge_save_pc(pc_id: str, pc_data: dict):
    """Atomic read-modify-write: loads latest PCs, adds/updates one, saves.
    Prevents race conditions where background threads overwrite each other."""
    path = os.path.join(DATA_DIR, "price_checks.json")
    try:
        import fcntl
        # Open for read+write, create if needed
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        with os.fdopen(fd, "r+") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            content = f.read()
            pcs = json.loads(content) if content.strip() else {}
            pcs[pc_id] = pc_data
            f.seek(0)
            f.truncate()
            json.dump(pcs, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except ImportError:
        pcs = _load_price_checks()
        pcs[pc_id] = pc_data
        _save_price_checks(pcs)


def _is_user_facing_pc(pc: dict) -> bool:
    """Canonical filter: is this PC for the standalone PC queue?
    Auto-price PCs (created from RFQ imports) belong to the RFQ row, not the PC queue.
    Standalone email PCs (Valentina's 704s) DO belong in the PC queue.
    Dismissed/archived/deleted PCs are hidden from active queue.
    Terminal statuses (won/lost/expired) fall off the active queue → visible in Archive.
    Used by: home page, manager brief, workflow tester, pipeline summary."""
    if pc.get("source") == "email_auto_draft":
        return False
    if pc.get("is_auto_draft"):
        return False
    if pc.get("rfq_id"):
        return False
    # Admin cleanup statuses — hide from active queue
    if pc.get("status") in ("dismissed", "archived", "deleted", "duplicate", "no_response"):
        return False
    # Terminal statuses — done, move to archive
    if pc.get("status") in ("won", "lost", "expired"):
        return False
    # Parse errors with 0 items — nothing actionable, hide from queue
    if pc.get("status") == "parse_error" and not pc.get("items"):
        return False
    return True


# ═══════════════════════════════════════════════════════════════════════
# Email Polling Thread
# ═══════════════════════════════════════════════════════════════════════

_shared_poller = None  # Shared poller instance for manual checks

def _auto_price_new_pc(pc_id: str):
    """Auto-price a newly created PC: catalog match → SCPRS → apply best prices.
    Runs in background thread so email processing isn't blocked."""
    try:
        pcs = _load_price_checks()
        pc = pcs.get(pc_id)
        if not pc or not pc.get("items"):
            return
        
        items = pc["items"]
        found_count = 0
        log.info("Auto-pricing PC %s (%d items)", pc_id, len(items))

        # ── 1. Catalog match ──
        try:
            from src.agents.product_catalog import match_item, init_catalog_db
            init_catalog_db()
            for item in items:
                desc = item.get("description", "")
                pn = str(item.get("item_number", "") or "")
                if not desc and not pn:
                    continue
                matches = match_item(desc, pn, top_n=1)
                if matches and matches[0].get("match_confidence", 0) >= 0.55:
                    best = matches[0]
                    if not item.get("pricing"):
                        item["pricing"] = {}
                    p = item["pricing"]
                    cat_price = best.get("recommended_price") or best.get("sell_price", 0)
                    cat_cost = best.get("cost", 0)
                    if cat_price > 0 and not p.get("recommended_price"):
                        p["catalog_match"] = best.get("name", "")[:60]
                        p["catalog_confidence"] = best.get("match_confidence", 0)
                        p["catalog_sku"] = best.get("sku", "")
                        p["catalog_product_id"] = best.get("id")
                        p["catalog_cost"] = cat_cost if cat_cost > 0 else None
                        p["catalog_best_supplier"] = best.get("best_supplier", "")
                        if cat_cost > 0:
                            p["unit_cost"] = cat_cost
                            p["last_cost"] = cat_cost
                        p["recommended_price"] = round(cat_price, 2)
                        # Propagate MFG# if item doesn't have one
                        cat_mfg = best.get("mfg_number") or best.get("sku", "")
                        if cat_mfg and not item.get("mfg_number"):
                            item["mfg_number"] = cat_mfg
                        found_count += 1
                        log.debug("  Catalog match: %s → $%.2f (pid=%s)", desc[:40], cat_price, best.get("id"))
        except Exception as e:
            log.debug("Auto-price catalog error: %s", e)

        # ── 2. SCPRS won quotes KB ──
        try:
            if PRICING_ORACLE_AVAILABLE:
                for item in items:
                    p = item.get("pricing", {})
                    if p.get("recommended_price") or p.get("scprs_price"):
                        continue  # Already priced by catalog
                    desc = item.get("description", "")
                    pn = str(item.get("item_number", "") or "")
                    matches = find_similar_items(item_number=pn, description=desc)
                    if matches:
                        best = matches[0]
                        quote = best.get("quote", best)
                        scprs_price = quote.get("unit_price")
                        if scprs_price and scprs_price > 0:
                            if not item.get("pricing"):
                                item["pricing"] = {}
                            item["pricing"]["scprs_price"] = scprs_price
                            item["pricing"]["scprs_match"] = quote.get("description", "")[:60]
                            item["pricing"]["scprs_confidence"] = best.get("match_confidence", 0)
                            # Propagate part number from SCPRS match
                            scprs_pn = quote.get("item_number", "")
                            if scprs_pn and not item.get("mfg_number"):
                                item["mfg_number"] = scprs_pn
                            if not item["pricing"].get("unit_cost"):
                                item["pricing"]["unit_cost"] = scprs_price
                            if not item["pricing"].get("recommended_price"):
                                markup = 25
                                item["pricing"]["recommended_price"] = round(scprs_price * (1 + markup / 100), 2)
                            found_count += 1
                            log.debug("  SCPRS match: %s → $%.2f", desc[:40], scprs_price)
        except Exception as e:
            log.debug("Auto-price SCPRS error: %s", e)

        # ── 3. Claude web search for remaining unpriced items ──
        try:
            from src.agents.web_price_research import search_product_price
            for item in items:
                p = item.get("pricing", {})
                if p.get("recommended_price") or p.get("unit_cost"):
                    continue  # Already priced
                desc = item.get("description", "")
                pn = str(item.get("item_number", "") or "")
                if not desc and not pn:
                    continue
                result = search_product_price(description=desc, part_number=pn,
                    qty=item.get("qty", 1), uom=item.get("uom", "EA"))
                if result.get("found") and result.get("price", 0) > 0:
                    if not item.get("pricing"):
                        item["pricing"] = {}
                    item["pricing"]["web_price"] = result["price"]
                    item["pricing"]["web_source"] = result.get("source", "")
                    item["pricing"]["web_url"] = result.get("url", "")
                    item["pricing"]["unit_cost"] = result["price"]
                    # Store part/MFG number if found
                    web_pn = result.get("part_number", "")
                    if web_pn:
                        item["pricing"]["web_part_number"] = web_pn
                        if not item.get("mfg_number"):
                            item["mfg_number"] = web_pn
                    markup = 25
                    item["pricing"]["recommended_price"] = round(result["price"] * (1 + markup / 100), 2)
                    found_count += 1
                    log.debug("  Web match: %s → $%.2f via %s", desc[:40], result["price"], result.get("source",""))
                    # Write-back: save to catalog + product_suppliers
                    try:
                        from src.agents.product_catalog import (
                            match_item as _wm, add_to_catalog as _wa,
                            add_supplier_price as _ws, init_catalog_db as _wi
                        )
                        _wi()
                        _wmatches = _wm(desc, pn, top_n=1) if (desc or pn) else []
                        if _wmatches and _wmatches[0].get("match_confidence", 0) >= 0.55:
                            _wpid = _wmatches[0]["id"]
                        else:
                            _wpid = _wa(description=desc, part_number=pn or web_pn,
                                        cost=result["price"], source="auto_web_search")
                        if _wpid and result.get("source"):
                            _ws(_wpid, result["source"], result["price"],
                                url=result.get("url", ""), sku=web_pn)
                    except Exception as _e:
                        log.debug("Suppressed: %s", _e)
                time.sleep(1.0)  # Rate limit
        except ImportError:
            log.debug("web_price_research not available")
        except Exception as e:
            log.debug("Auto-price web search error: %s", e)

        # ── 4. Save if we found anything ──
        if found_count > 0:
            pcs = _load_price_checks()  # Reload fresh
            if pc_id in pcs:
                pcs[pc_id]["items"] = items
                pcs[pc_id]["auto_priced"] = True
                pcs[pc_id]["auto_priced_count"] = found_count
                pcs[pc_id]["auto_priced_at"] = datetime.now().isoformat()
                if pcs[pc_id].get("status") == "parsed":
                    pcs[pc_id]["status"] = "priced"
                _save_price_checks(pcs)
                log.info("Auto-priced PC %s: %d/%d items found prices", pc_id, found_count, len(items))
                try:
                    from src.agents.notify_agent import send_alert
                    send_alert("bell", f"Auto-priced: {pc.get('pc_number',pc_id)} — {found_count}/{len(items)} items", {"type": "auto_price"})
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)
        else:
            log.info("Auto-price PC %s: no matches found for %d items", pc_id, len(items))

    except Exception as e:
        log.error("Auto-price PC %s failed: %s", pc_id, e)


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
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)
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
                        # Auto-price in background thread
                        threading.Thread(target=_auto_price_new_pc, args=(pc_id,), daemon=True).start()
                        _trace.append(f"PC CREATED: {pc_id} (auto-pricing started)")
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

    # ── Solicitation-number dedup + self-email guard ────────────────────────
    # Block: exact email UID already imported as RFQ
    try:
        _email_uid = rfq_email.get("email_uid", "")
        if _email_uid:
            for _eid, _erfq in rfqs.items():
                if _erfq.get("email_uid") == _email_uid:
                    log.info("Duplicate RFQ blocked: email UID %s already imported as %s", _email_uid, _eid)
                    return None
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    
    # Block: self-email (our own sent replies picked up by poller)
    # EXCEPTION: forwarded emails from our own domain are valid (user forwarding an RFQ)
    _sender_email = rfq_email.get("sender_email", rfq_email.get("sender", "")).lower()
    _our_domains = ["reytechinc.com", "reytech.com"]
    _is_from_self = any(_sender_email.endswith(f"@{d}") for d in _our_domains)
    _subject_lower = rfq_email.get("subject", "").lower().strip()
    _is_forward = any(_subject_lower.startswith(p) for p in ["fwd:", "fw:", "fwd :", "fw :"])
    _has_attachments = bool(rfq_email.get("attachments"))
    if _is_from_self and not (_is_forward and _has_attachments):
        _trace.append(f"SKIP: self-email from {_sender_email}")
        log.info("Blocking self-email from %s: %s", _sender_email, _subj)
        POLL_STATUS.setdefault("_email_traces", []).append(_trace)
        t.ok("Skipped: self-email")
        return None
    elif _is_from_self and _is_forward:
        _trace.append(f"FORWARD from self: {_sender_email} — processing as RFQ with {len(rfq_email.get('attachments',[]))} attachments")
        log.info("Processing forwarded email from self: %s — %s", _sender_email, _subj)
    
    # Block: solicitation number already exists in active RFQ queue
    # E12: Enhanced duplicate detection — detect amendments and link revisions
    _sol_hint = rfq_email.get("solicitation_hint", "")
    if _sol_hint and _sol_hint != "unknown":
        for _eid, _erfq in rfqs.items():
            if (_erfq.get("solicitation_number") == _sol_hint 
                    and _erfq.get("status") not in ("dismissed",)):
                # Check if this is an amendment (different attachments or later date)
                _existing_uid = _erfq.get("email_uid", "")
                _new_uid = rfq_email.get("email_uid", "")
                if _existing_uid and _new_uid and _existing_uid != _new_uid:
                    # Different email — likely an amendment. Link them but still create.
                    _trace.append(f"AMENDMENT: sol {_sol_hint} exists as {_eid} but different email — creating as amendment")
                    log.info("Amendment detected: sol#%s existing=%s, creating linked amendment", _sol_hint, _eid)
                    rfq_email["_amendment_of"] = _eid
                    rfq_email["_is_amendment"] = True
                    break
                else:
                    _trace.append(f"DEDUP: sol {_sol_hint} already in RFQ queue as {_eid}")
                    log.info("Solicitation dedup: #%s already exists (id=%s) — skipping", _sol_hint, _eid)
                    POLL_STATUS.setdefault("_email_traces", []).append(_trace)
                    t.ok("Skipped: solicitation dedup", existing_id=_eid, sol=_sol_hint)
                    return None

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
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
    
    _trace.append(f"→ RFQ PATH: templates={list(templates.keys())}, attachments={[a.get('filename','?') for a in rfq_email.get('attachments',[])]}")
    
    if "704b" not in templates:
        # ── Generic RFQ path (Cal Vet, CalFire, DGS, etc.) ──────────────
        # No 704 forms — extract line items from whatever PDFs we have
        _all_pdf_paths = []
        for att in rfq_email.get("attachments", []):
            p = att.get("path", "")
            if p and os.path.exists(p) and p.lower().endswith(".pdf"):
                _all_pdf_paths.append(p)
        # Also include any template files already copied
        for _tp in templates.values():
            if _tp and os.path.exists(_tp) and _tp not in _all_pdf_paths:
                _all_pdf_paths.append(_tp)

        _generic_result = {}
        if _all_pdf_paths:
            try:
                from src.forms.generic_rfq_parser import parse_generic_rfq
                _generic_result = parse_generic_rfq(
                    _all_pdf_paths,
                    subject=rfq_email.get("subject", ""),
                    sender_email=rfq_email.get("sender_email", ""),
                    body=rfq_email.get("body_text", rfq_email.get("body", "")),
                )
                _trace.append(f"GENERIC PARSE: agency={_generic_result.get('agency','?')}, "
                              f"items={len(_generic_result.get('line_items',[]))}, "
                              f"sol={_generic_result.get('solicitation_number','?')}")
            except Exception as _gpe:
                log.warning("Generic RFQ parser failed: %s", _gpe)
                _trace.append(f"GENERIC PARSE FAILED: {_gpe}")

        _gen_items = _generic_result.get("line_items", [])
        _gen_sol = (_generic_result.get("solicitation_number")
                    or rfq_email.get("solicitation_hint", "unknown"))
        _gen_agency = _generic_result.get("agency", "unknown")
        _gen_agency_name = _generic_result.get("agency_name", "Unknown")

        rfq_data = {
            "id": rfq_email["id"],
            "solicitation_number": _gen_sol,
            "status": "new",
            "source": "email",
            "email_uid": rfq_email.get("email_uid"),
            "email_subject": rfq_email["subject"],
            "email_sender": rfq_email["sender_email"],
            "email_message_id": rfq_email.get("message_id", ""),
            "requestor_name": _generic_result.get("requestor_name") or rfq_email["sender_email"],
            "requestor_email": _generic_result.get("requestor_email") or rfq_email["sender_email"],
            "due_date": _generic_result.get("due_date") or "TBD",
            "delivery_location": _generic_result.get("institution") or _generic_result.get("ship_to", ""),
            "line_items": _gen_items,
            "attachments_raw": [a["filename"] for a in rfq_email["attachments"]],
            "templates": templates,
            "agency": _gen_agency,
            "agency_name": _gen_agency_name,
            "form_type": _generic_result.get("form_type", "generic_rfq"),
            "quote_type": _generic_result.get("quote_type", "formal"),
            "parse_details": _generic_result.get("parse_details", []),
            "parse_note": (f"{_gen_agency_name} — {len(_gen_items)} items parsed from PDFs"
                           if _gen_items
                           else f"{_gen_agency_name} — No 704B, PDF text parse found 0 items — manual entry needed"),
        }
        # Run price lookup if we got items
        if _gen_items:
            rfq_data["line_items"] = bulk_lookup(_gen_items)
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
        # Detect agency for 704-based RFQs too
        try:
            from src.forms.generic_rfq_parser import detect_agency as _detect_ag
            _ak, _ai = _detect_ag(rfq_email.get("subject",""), "",
                                   rfq_email.get("sender_email",""), "")
            rfq_data["agency"] = _ak if _ak != "unknown" else "cchcs"
            rfq_data["agency_name"] = _ai.get("name", "CCHCS")
            rfq_data["form_type"] = "ams_704"
            rfq_data["quote_type"] = "704b_fill"
        except Exception:
            rfq_data["agency"] = "cchcs"
            rfq_data["agency_name"] = "CCHCS"
            rfq_data["form_type"] = "ams_704"
            rfq_data["quote_type"] = "704b_fill"
    
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
    # Use atomic merge-save to avoid overwriting PCs created by other threads
    _merge_save_pc(pc_id, pc)

    # Step 3: Run SCPRS + Amazon price lookup
    try:
        from src.forms.price_check import lookup_prices
        pc = lookup_prices(pc)
        # Only mark 'priced' if items actually got prices — otherwise 'draft'
        priced = sum(1 for it in pc.get("items", []) if it.get("pricing", {}).get("recommended_price"))
        total_items = len(pc.get("items", []))
        if priced > 0:
            pc["status"] = "priced"
        else:
            pc["status"] = "draft"
        _merge_save_pc(pc_id, pc)
        t.step("Prices looked up", priced=priced, total=total_items)
        log.info("[AutoPrice] Prices found: %d/%d items → status=%s", priced, total_items, pc["status"])
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
            _merge_save_pc(pc_id, pc)
            log.info("[AutoPrice] %d price suggestions from competitor history", len(suggestions))
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # Step 5: Update RFQ with PC link
    rfqs = load_rfqs()
    if rfq_id in rfqs:
        rfqs[rfq_id]["auto_price_pc_id"] = pc_id
        rfqs[rfq_id]["auto_priced_at"] = datetime.now().isoformat()
        # Only set 'priced' if prices were actually found
        priced_count = sum(1 for it in pc.get("items", []) if it.get("pricing", {}).get("recommended_price"))
        rfqs[rfq_id]["status"] = "priced" if priced_count > 0 else "draft"
        rfqs[rfq_id]["auto_lookup_results"] = {
            "scprs_found": sum(1 for it in pc.get("items", []) if it.get("pricing", {}).get("scprs_price")),
            "amazon_found": sum(1 for it in pc.get("items", []) if it.get("pricing", {}).get("amazon_cost")),
            "catalog_found": sum(1 for it in pc.get("items", []) if it.get("pricing", {}).get("catalog_match")),
            "priced": priced_count,
            "total": len(pc.get("items", [])),
            "ran_at": datetime.now().isoformat(),
        }
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
    
    # Check for password in config OR environment (Railway sets GMAIL_PASSWORD as env var)
    effective_password = (email_cfg.get("email_password")
                          or os.environ.get("GMAIL_PASSWORD", ""))
    if not effective_password:
        POLL_STATUS["error"] = "Email password not configured"
        log.error("Poll check failed: no email_password in config or GMAIL_PASSWORD env var")
        return []
    # Inject env-var password into config so EmailPoller picks it up
    email_cfg = dict(email_cfg)
    email_cfg["email_password"] = effective_password
    # Also ensure email address falls back to env
    if not email_cfg.get("email"):
        email_cfg["email"] = os.environ.get("GMAIL_ADDRESS", "")
    
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
        # Pre-check: ensure we have disk space for new attachments
        import shutil as _shutil
        _, _, free_bytes = _shutil.disk_usage(UPLOAD_DIR)
        free_mb = free_bytes / 1024 / 1024
        if free_mb < 50:
            POLL_STATUS["error"] = f"Disk critically low: {free_mb:.0f}MB free — skipping poll"
            log.error(POLL_STATUS["error"])
            try:
                from src.agents.notify_agent import send_alert
                send_alert(event_type="system_error", title="⚠️ Disk Space Critical",
                          body=f"Only {free_mb:.0f}MB free. Email polling paused. Free space or resize volume.",
                          urgency="high", cooldown_key="disk_low")
            except Exception:
                pass
            return []
        
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
                    # CRITICAL: If processing failed (disk full, parse error, etc),
                    # remove UID from processed set so it gets retried next poll
                    failed_uid = rfq_email.get("email_uid", "")
                    if failed_uid and _shared_poller and hasattr(_shared_poller, '_processed'):
                        _shared_poller._processed.discard(failed_uid)
                        log.warning("Removed UID %s from processed set — will retry next poll", failed_uid)
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
    effective_password = (email_cfg.get("email_password")
                          or os.environ.get("GMAIL_PASSWORD", ""))
    if not effective_password:
        POLL_STATUS["error"] = "Email password not configured"
        POLL_STATUS["running"] = False
        log.warning("Email polling disabled — no password in config or GMAIL_PASSWORD env var")
        return
    # Ensure CONFIG has the password for do_poll_check
    CONFIG.setdefault("email", {})["email_password"] = effective_password
    
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
    data = _cached_json_load(ORDERS_FILE, fallback={})
    # ── Restore from SQLite if JSON is empty ─────────────────────
    if not data:
        try:
            from src.core.db import get_db
            with get_db() as conn:
                rows = conn.execute("SELECT * FROM orders").fetchall()
                for r in rows:
                    oid = r["id"]
                    items = []
                    try:
                        items = json.loads(r["items"] or "[]")
                    except Exception as _e:
                        log.debug("Suppressed: %s", _e)
                    data[oid] = {
                        "order_id": oid,
                        "quote_number": r.get("quote_number") or "",
                        "po_number": r.get("po_number") or "",
                        "agency": r.get("agency") or "",
                        "institution": r.get("institution") or "",
                        "total": r.get("total") or 0,
                        "status": r.get("status") or "new",
                        "line_items": items,
                        "created_at": r.get("created_at") or "",
                    }
                if data:
                    _save_orders(data)
                    log.info("Restored %d orders from SQLite → JSON", len(data))
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    # ── End restore ──────────────────────────────────────────────
    return data

def _save_orders(orders: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(ORDERS_FILE, "w") as f:
        json.dump(orders, f, indent=2, default=str)
    _invalidate_cache(ORDERS_FILE)
    # ── Sync to SQLite ───────────────────────────────────────────
    try:
        from src.core.db import get_db
        with get_db() as conn:
            for oid, o in orders.items():
                items_json = json.dumps(o.get("line_items", []))
                conn.execute("""
                    INSERT OR REPLACE INTO orders
                    (id, quote_number, po_number, agency, institution,
                     total, status, items, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    oid,
                    o.get("quote_number", ""),
                    o.get("po_number", ""),
                    o.get("agency", ""),
                    o.get("institution", o.get("customer", "")),
                    o.get("total", 0),
                    o.get("status", "new"),
                    items_json,
                    o.get("created_at", ""),
                    datetime.now().isoformat(),
                ))
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    # ── End sync ─────────────────────────────────────────────────

def _create_order_from_quote(qt: dict, po_number: str = "") -> dict:
    """Create an order when a quote is won."""
    qn = qt.get("quote_number", "")
    oid = f"ORD-{qn}" if qn else f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    line_items = []
    for i, it in enumerate(qt.get("items_detail", [])):
        pn = it.get("part_number", "")
        asin = it.get("asin", "")
        # Prefer stored supplier_url, then generate from ASIN/B0 part number
        supplier_url = it.get("supplier_url", "") or it.get("url", "")
        supplier = it.get("supplier", "")
        if not supplier_url:
            if asin:
                supplier_url = f"https://www.amazon.com/dp/{asin}"
            elif pn and pn.startswith("B0"):
                supplier_url = f"https://www.amazon.com/dp/{pn}"
        if not supplier and (asin or (pn and pn.startswith("B0"))):
            supplier = "Amazon"
        line_items.append({
            "line_id": f"L{i+1:03d}",
            "description": it.get("description", ""),
            "part_number": pn,
            "asin": asin,
            "qty": it.get("qty", 0),
            "unit_price": it.get("unit_price", 0),
            "extended": round(it.get("qty", 0) * it.get("unit_price", 0), 2),
            "supplier": supplier,
            "supplier_url": supplier_url,
            "cost": it.get("cost", 0),
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
    # ── Mark linked quote as 'won' since it has a confirmed order ──
    if qn:
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("""
                    UPDATE quotes SET status = 'won',
                        po_number = COALESCE(NULLIF(?, ''), po_number),
                        updated_at = ?
                    WHERE quote_number = ? AND status NOT IN ('won', 'cancelled')
                """, (po_number, datetime.now().isoformat(), qn))
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    # ── Pricing Intelligence: capture winning prices ──
    try:
        from src.knowledge.pricing_intel import record_winning_prices
        record_winning_prices(order)
    except Exception as _e:
        log.debug("Pricing intel capture: %s", _e)
    # ── Catalog Learning: teach supplier data from quote items ──
    try:
        from src.api.modules.routes_orders_full import _learn_supplier_from_order_line
        for it in line_items:
            if it.get("supplier") or it.get("supplier_url"):
                _learn_supplier_from_order_line(it, order)
    except Exception as _le:
        log.debug("Catalog learning from new order: %s", _le)
    return order


def _create_order_from_po_email(po_data: dict) -> dict:
    """Create an order from an inbound PO email (may not have a matching quote).
    
    When a quote is matched:
    - Auto-populates line items from quote (with sell prices, suppliers already set)
    - Merges PO items with quote items for maximum data
    - Records pricing intelligence for future quoting
    
    po_data keys: po_number, sender_email, subject, sol_number,
                  items (list of dicts), total, agency, institution,
                  po_pdf_path, matched_quote
    """
    qn = po_data.get("matched_quote", "")
    po_num = po_data.get("po_number", "")
    oid = f"ORD-{qn}" if qn else f"ORD-PO-{po_num or datetime.now().strftime('%Y%m%d%H%M%S')}"

    # ── Quote → Order auto-link: enrich from matched quote ──
    quote_items = []
    quote_data = {}
    if qn:
        try:
            import json as _json
            with open(os.path.join(DATA_DIR, "quotes_log.json")) as _qf:
                for q in _json.load(_qf):
                    if q.get("quote_number") == qn:
                        quote_data = q
                        quote_items = q.get("items_detail", q.get("items", []))
                        break
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

    # Build line items — prefer PO data, enrich from quote
    po_items = po_data.get("items", [])
    line_items = []
    
    # If PO has items, use those as base and enrich from quote
    source_items = po_items if po_items else quote_items
    for i, it in enumerate(source_items):
        pn = it.get("part_number", "") or it.get("manufacturer_part", "") or it.get("sku", "")
        desc = it.get("description", "") or it.get("item_description", "") or it.get("name", "")
        qty = it.get("qty", 0) or it.get("quantity", 0)
        up = it.get("unit_price", 0) or it.get("price", 0) or it.get("our_price", 0)
        supplier_url = it.get("supplier_url", "")
        supplier = it.get("supplier", "")
        cost = it.get("cost", 0) or it.get("supplier_price", 0)
        asin = it.get("asin", "")
        
        # Try to match to quote item for enrichment
        if qn and quote_items and not cost:
            for qi in quote_items:
                qi_desc = qi.get("description", "") or qi.get("name", "")
                qi_pn = qi.get("part_number", "") or qi.get("sku", "")
                if (pn and qi_pn and pn.lower() == qi_pn.lower()) or \
                   (desc and qi_desc and desc.lower()[:30] == qi_desc.lower()[:30]):
                    cost = qi.get("cost", 0) or qi.get("supplier_price", 0)
                    if not supplier:
                        supplier = qi.get("supplier", "")
                    if not supplier_url:
                        supplier_url = qi.get("supplier_url", "") or qi.get("url", "")
                    if not asin:
                        asin = qi.get("asin", "")
                    if not up:
                        up = qi.get("our_price", 0) or qi.get("unit_price", 0)
                    break

        # Auto-detect Amazon ASINs
        if not asin and pn and (pn.startswith("B0") or pn.startswith("b0")):
            asin = pn
        if asin and not supplier_url:
            supplier_url = f"https://www.amazon.com/dp/{asin}"
        if (asin or (pn and pn.upper().startswith("B0"))) and not supplier:
            supplier = "Amazon"

        margin = round((up - cost) / up * 100, 1) if up and cost and up > 0 else 0

        line_items.append({
            "line_id": f"L{i+1:03d}",
            "description": desc,
            "part_number": pn,
            "asin": asin,
            "qty": qty,
            "unit_price": up,
            "extended": round(qty * up, 2),
            "cost": cost,
            "margin_pct": margin,
            "supplier": supplier,
            "supplier_url": supplier_url,
            "sourcing_status": "pending",
            "tracking_number": "",
            "carrier": "",
            "ship_date": "",
            "delivery_date": "",
            "invoice_status": "pending",
            "invoice_number": "",
            "notes": "",
            "qb_item_id": "",
            "qb_item_name": "",
        })

    total = po_data.get("total", 0) or sum(it.get("extended", 0) for it in line_items)

    order = {
        "order_id": oid,
        "quote_number": qn,
        "po_number": po_num,
        "agency": po_data.get("agency", "") or quote_data.get("agency", ""),
        "institution": po_data.get("institution", "") or quote_data.get("institution", ""),
        "ship_to_name": po_data.get("institution", "") or quote_data.get("ship_to_name", "") or quote_data.get("institution", ""),
        "ship_to_address": quote_data.get("ship_to_address", []),
        "total": total,
        "subtotal": total,
        "tax": 0,
        "payment_terms": "Net 45",
        "line_items": line_items,
        "status": "new",
        "invoice_type": "",
        "invoice_total": 0,
        "source": "email_po",
        "po_pdf": po_data.get("po_pdf_path", ""),
        "sender_email": po_data.get("sender_email", ""),
        "buyer_name": quote_data.get("requestor_name", "") or quote_data.get("buyer_name", ""),
        "buyer_email": quote_data.get("requestor_email", "") or po_data.get("sender_email", ""),
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "status_history": [{"status": "new", "timestamp": datetime.now().isoformat(), "actor": "email_auto"}],
        "qb_customer_id": "",
        "qb_invoice_id": "",
    }

    orders = _load_orders()
    
    # Dedup: check if ANY order already exists with this PO number
    if po_num:
        for existing_oid, existing_order in orders.items():
            if existing_order.get("po_number") == po_num:
                log.info("Order for PO %s already exists (%s), skipping creation", po_num, existing_oid)
                return existing_order
    
    if oid in orders:
        log.info("Order %s already exists, skipping creation", oid)
        return orders[oid]
    
    orders[oid] = order
    _save_orders(orders)
    _log_crm_activity(qn or po_num, "order_created",
                      f"Order {oid} created from PO email — PO#{po_num} · ${total:,.2f}" + (f" · Linked to quote {qn}" if qn else ""),
                      actor="system", metadata={"order_id": oid, "po_number": po_num, "source": "email_po", "quote_linked": qn})
    # ── Mark linked quote as 'won' since PO confirms the order ──
    if qn:
        try:
            from src.core.db import get_db as _get_db
            with _get_db() as _conn:
                _conn.execute("""
                    UPDATE quotes SET status = 'won',
                        po_number = COALESCE(NULLIF(?, ''), po_number),
                        updated_at = ?
                    WHERE quote_number = ? AND status NOT IN ('won', 'cancelled')
                """, (po_num, datetime.now().isoformat(), qn))
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    log.info("Order %s created from PO email (quote=%s, po=%s, items=%d, total=$%.2f, costs=%d)",
             oid, qn, po_num, len(line_items), total, sum(1 for it in line_items if it.get("cost")))
    # ── Pricing Intelligence: capture winning prices ──
    try:
        from src.knowledge.pricing_intel import record_winning_prices
        record_winning_prices(order)
    except Exception as _e:
        log.debug("Pricing intel capture: %s", _e)
    # ── Catalog Learning: teach supplier data from PO items ──
    try:
        from src.api.modules.routes_orders_full import _learn_supplier_from_order_line
        for it in line_items:
            if it.get("supplier") or it.get("supplier_url"):
                _learn_supplier_from_order_line(it, order)
    except Exception as _le:
        log.debug("Catalog learning from PO order: %s", _le)
    return order

def _update_order_status(oid: str):
    """Auto-calculate order status from line item statuses.
    When ALL items delivered → notify Mike to send invoice."""
    orders = _load_orders()
    order = orders.get(oid)
    if not order:
        return
    items = order.get("line_items", [])
    if not items:
        return
    old_status = order.get("status", "new")
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

    # ── ALL DELIVERED TRIGGER — auto-draft invoice + notify Mike ──
    if order["status"] == "delivered" and old_status != "delivered":
        order["delivered_at"] = datetime.now().isoformat()
        
        # Auto-create draft invoice (structured for QuickBooks API push)
        qn = order.get("quote_number", "")
        po = order.get("po_number", "")
        total = order.get("total", 0)
        subtotal = order.get("subtotal", 0) or total
        tax = order.get("tax", 0)
        tax_rate = round((tax / subtotal * 100), 2) if subtotal else 0
        institution = order.get("institution", "")
        
        inv_number = f"INV-{po or oid.replace('ORD-','')}"
        
        # Build line items in QB-compatible format
        # QB API: Line[].DetailType = "SalesItemLineDetail"
        # QB API: Line[].SalesItemLineDetail = {ItemRef:{value,name}, Qty, UnitPrice}
        qb_lines = []
        for i, it in enumerate(items):
            qb_lines.append({
                # ── Our fields (display + tracking) ──
                "line_id": it.get("line_id", f"L{i+1:03d}"),
                "description": it.get("description", ""),
                "part_number": it.get("part_number", ""),
                "qty": it.get("qty", 0),
                "unit_price": it.get("unit_price", 0),
                "extended": it.get("extended", 0),
                # ── QB API mapping (populated when QB connected) ──
                "qb_item_ref": it.get("qb_item_id", ""),     # QB ItemRef.value
                "qb_item_name": it.get("qb_item_name", ""),   # QB ItemRef.name
            })
        
        order["draft_invoice"] = {
            # ── Our fields ──
            "invoice_number": inv_number,        # → QB DocNumber
            "status": "draft",                   # draft → pending_qb → synced → sent
            "created_at": datetime.now().isoformat(),
            "order_id": oid,
            "po_number": po,                     # → QB CustomField or memo
            "quote_number": qn,
            
            # ── Customer / billing ──
            "bill_to_name": institution,          # → QB CustomerRef.name
            "bill_to_email": order.get("sender_email", ""),  # → QB BillEmail.Address
            "qb_customer_id": order.get("qb_customer_id", ""),  # → QB CustomerRef.value
            
            # ── Ship-to (for QB ShipAddr) ──
            "ship_to_name": order.get("ship_to_name", institution),
            "ship_to_address": order.get("ship_to_address", []),
            
            # ── Line items ──
            "items": qb_lines,
            
            # ── Totals ──
            "subtotal": subtotal,
            "tax_rate": tax_rate,                 # → QB TxnTaxDetail.TaxLine[].TaxPercent
            "tax": tax,                           # → QB TxnTaxDetail.TotalTax
            "total": total,
            
            # ── Payment terms ──
            "terms": order.get("payment_terms", "Net 45"),  # → QB SalesTermRef
            "due_date": "",                       # → QB DueDate (calculated on finalize)
            
            # ── QB sync tracking ──
            "qb_invoice_id": "",                  # Set after QB API push
            "qb_sync_token": "",                  # For QB updates
            "qb_synced_at": "",                   # Last sync timestamp
            "qb_status": "",                      # created / sent / paid / voided
        }
        
        orders[oid] = order
        _save_orders(orders)
        
        try:
            from src.agents.notify_agent import send_alert
            send_alert(
                event_type="all_delivered",
                title=f"✅ All items delivered — draft invoice ready!",
                body=(
                    f"Order {oid} · {institution}\n"
                    f"PO: {po or 'N/A'} · Quote: {qn or 'N/A'}\n"
                    f"Total: ${total:,.2f} · {len(items)} items all confirmed delivered.\n"
                    f"Draft invoice {inv_number} created — review and send."
                ),
                urgency="deal",
                cooldown_key=f"delivered_{oid}",
            )
        except Exception as _ne:
            log.debug("All-delivered notify error: %s", _ne)
        _log_crm_activity(qn, "all_delivered",
                          f"All {len(items)} items delivered for {oid}. Draft invoice {inv_number} created. Total ${total:,.2f}.",
                          actor="system", metadata={"order_id": oid, "po_number": po, "invoice": inv_number})
        
        # ── Catalog Learning: teach supplier data from completed order ──
        try:
            from src.api.modules.routes_orders_full import learn_from_completed_order
            learn_from_completed_order(oid)
        except Exception as _le:
            log.debug("Catalog learning from completed order: %s", _le)

# ═══════════════════════════════════════════════════════════════════════
# HTML Templates (extracted to src/api/templates.py)
# ═══════════════════════════════════════════════════════════════════════

from src.api.templates import BASE_CSS
from src.api.render import render_page

# Shim for legacy pages that build HTML in Python — wraps content in base template
def _wrap_page(content, title="Reytech"):
    return render_page("generic.html", page_title=title, content=content)

# ═══════════════════════════════════════════════════════════════════════
# Shared Manager Brief (app-wide) + Header JS
# ═══════════════════════════════════════════════════════════════════════

BRIEF_HTML = """<!-- Manager Brief — app-wide context, loads via AJAX with sessionStorage cache -->
<div id="brief-section" class="card" style="margin-top:16px;margin-bottom:14px;display:none">
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
  <div id="brief-grid">
   <div style="font-size:11px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;display:flex;align-items:center;gap:6px">
    Needs Your Attention <span id="approval-count" class="brief-count"></span>
   </div>
   <div id="approvals-list"></div>
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
 var bar=document.getElementById('pipeline-bar');
 if(bar){var s=data.summary||{};var q=s.quotes||{};var gr=s.growth||{};var ob=s.outbox||{};var rv=data.revenue||{};var ag=data.agents_summary||{};var stats=[{label:'Quotes',value:q.total||0,color:'var(--ac)'},{label:'Pipeline $',value:'$'+(q.pipeline_value||0).toLocaleString(),color:'var(--yl)'},{label:'Won $',value:'$'+(q.won_total||0).toLocaleString(),color:'var(--gn)'},{label:'Win Rate',value:(q.win_rate||0)+'%',color:q.win_rate>=50?'var(--gn)':'var(--yl)'},{label:'Growth',value:gr.total_prospects||0,color:'#bc8cff'},{label:'Agents',value:(ag.healthy||0)+'/'+(ag.total||0),color:ag.down>0?'var(--rd)':'var(--gn)'},{label:'Goal',value:rv.pct?rv.pct.toFixed(0)+'%':'0%',color:rv.pct>=50?'var(--gn)':rv.pct>=25?'var(--yl)':'var(--rd)'},{label:'Drafts',value:ob.drafts||0,color:ob.drafts>0?'var(--yl)':'var(--tx2)'}];bar.innerHTML=stats.map(function(s){return '<div class="stat-chip"><div class="stat-val" style="color:'+s.color+'">'+s.value+'</div><div class="stat-label">'+s.label+'</div></div>';}).join('');}
 var agRow=document.getElementById('agents-row');var agList=document.getElementById('agents-list');
 if(agRow&&agList&&data.agents&&data.agents.length>0){agRow.style.display='block';agList.innerHTML=data.agents.map(function(a){var isOk=a.status==='active'||a.status==='ready'||a.status==='connected';var isWait=a.status==='not configured'||a.status==='waiting';var color=isOk?'#3fb950':isWait?'#d29922':'#f85149';var bg=isOk?'rgba(52,211,153,.08)':isWait?'rgba(251,191,36,.08)':'rgba(248,113,113,.08)';var border=isOk?'rgba(52,211,153,.25)':isWait?'rgba(251,191,36,.25)':'rgba(248,113,113,.25)';return '<span style="font-size:13px;padding:8px 14px;border-radius:8px;background:'+bg+';border:1px solid '+border+';display:inline-flex;align-items:center;gap:7px;font-weight:500"><span style="width:10px;height:10px;border-radius:50%;background:'+color+';display:inline-block;flex-shrink:0;box-shadow:0 0 6px '+color+'66"></span><span style="font-size:15px">'+a.icon+'</span><span>'+a.name+'</span></span>';}).join('');}
}
loadBrief();
"""

# Shared header JS — pollNow, resync, notifications, poll time. Injected into BOTH render() and _header() pages.
# SHARED_HEADER_JS removed — extracted to src/static/header.js


# ═══════════════════════════════════════════════════════════════════════
# Routes
# ═══════════════════════════════════════════════════════════════════════

# render() function removed — all pages now use render_page() from src/api/render.py

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
    "routes_rfq",              # Home, upload, RFQ pages, quote generation
    "routes_agents",           # Agent control panel, email templates
    "routes_pricecheck",       # Price check pages + lookup
    "routes_crm",              # CRM, pricing oracle, auto-processor
    "routes_intel",            # SCPRS, CCHCS, vendors, growth, funnel, forecasting
    "routes_orders_full",      # Orders, supplier lookup, quote-order link, invoice
    "routes_voice_contacts",   # Intelligence page, voice, contacts, campaigns
    "routes_catalog_finance",  # Catalog, shipping, pricing, margins, payments, audit
    "routes_prd28",           # PRD-28: Quote lifecycle, email overhaul, leads, revenue, vendor intel
    "routes_analytics",       # PRD-29: Pipeline analytics, buyer intel, margin optimizer, settings, API v1
    "routes_order_tracking",  # PRD-29: PO tracking, separate email inbox, line item lifecycle
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

# ── Start Follow-Up Engine (auto-creates follow-up drafts) ──────────────
try:
    from src.agents.follow_up_engine import start_follow_up_scheduler
    start_follow_up_scheduler()
    log.info("Follow-up engine started (scans every 1h)")
except Exception as _e:
    log.warning("Follow-up engine failed to start: %s", _e)

# ── Start Quote Lifecycle (auto-expire, follow-up triggers) ──────────────
try:
    from src.agents.quote_lifecycle import start_lifecycle_scheduler
    start_lifecycle_scheduler()
    log.info("Quote lifecycle scheduler started (checks every 1h)")
except Exception as _e:
    log.warning("Quote lifecycle failed to start: %s", _e)

# ── Start Email Retry Scheduler (retries failed emails) ─────────────────
try:
    from src.agents.email_lifecycle import start_retry_scheduler
    start_retry_scheduler()
    log.info("Email retry scheduler started (checks every 15m)")
except Exception as _e:
    log.warning("Email retry scheduler failed to start: %s", _e)

# ── Start Lead Nurture Scheduler (drip sequences + rescoring) ────────────
try:
    from src.agents.lead_nurture_agent import start_nurture_scheduler
    start_nurture_scheduler()
    log.info("Lead nurture scheduler started (daily)")
except Exception as _e:
    log.warning("Lead nurture scheduler failed to start: %s", _e)


# ═══════════════════════════════════════════════════════════════════════════════
# Force Recapture — guaranteed to load (not in exec'd module)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/force-recapture", methods=["POST", "GET"])
@auth_required
def _force_recapture():
    """Delete RFQ/PC by keyword, clear UID, re-poll. GET with ?match=calvet also works."""
    if request.method == "GET":
        match_kw = request.args.get("match", "").lower().strip()
        exact_id = request.args.get("rfq_id", "").strip()
    else:
        data = request.get_json(silent=True) or {}
        match_kw = (data.get("match") or request.args.get("match", "")).lower().strip()
        exact_id = (data.get("rfq_id") or request.args.get("rfq_id", "")).strip()
    
    if not match_kw and not exact_id:
        return jsonify({"ok": False, "error": "Provide ?match=keyword or ?rfq_id=id"})
    
    removed_rfqs = []
    removed_pcs = []
    cleared_uids = []
    
    # Remove matching RFQs
    rfqs = load_rfqs()
    to_remove = []
    for rid, r in rfqs.items():
        if exact_id and rid == exact_id:
            to_remove.append(rid)
        elif match_kw:
            searchable = " ".join([
                r.get("solicitation_number", ""), r.get("email_sender", ""),
                r.get("email_subject", ""), r.get("agency", ""),
                r.get("agency_name", ""), r.get("requestor_email", ""),
            ]).lower()
            if match_kw in searchable:
                to_remove.append(rid)
    
    for rid in to_remove:
        r = rfqs.pop(rid)
        uid = r.get("email_uid")
        if uid:
            cleared_uids.append(uid)
        removed_rfqs.append({"id": rid, "sol": r.get("solicitation_number", "?"),
                             "items": len(r.get("line_items", []))})
    if to_remove:
        save_rfqs(rfqs)
    
    # Remove matching PCs
    pcs = _load_price_checks()
    pc_remove = []
    for pid, pc in pcs.items():
        if exact_id and pid == exact_id:
            pc_remove.append(pid)
        elif match_kw:
            searchable = " ".join([
                pc.get("pc_number", ""), pc.get("email_subject", ""),
                pc.get("requestor", ""), str(pc.get("institution", "")),
            ]).lower()
            if match_kw in searchable:
                pc_remove.append(pid)
    for pid in pc_remove:
        pc = pcs.pop(pid)
        uid = pc.get("email_uid")
        if uid:
            cleared_uids.append(uid)
        removed_pcs.append({"id": pid, "pc": pc.get("pc_number", "?")})
    if pc_remove:
        _save_price_checks(pcs)
    
    # Clear UIDs from processed list
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    try:
        if cleared_uids and os.path.exists(proc_file):
            with open(proc_file) as f:
                processed = json.load(f)
            if isinstance(processed, list):
                processed = [u for u in processed if u not in cleared_uids]
                with open(proc_file, "w") as f:
                    json.dump(processed, f)
    except Exception:
        pass
    
    global _shared_poller
    
    # If nothing matched in queues but we have a keyword, 
    # nuke the processed list entirely so the email gets re-captured
    if not removed_rfqs and not removed_pcs and match_kw:
        old_count = 0
        # CRITICAL: Pause background poller to prevent race condition
        # The background thread's poller has an in-memory _processed set
        # that it saves back to disk at the end of each cycle, overwriting
        # our deletion. We must: pause → flush memory → delete file → unpause.
        POLL_STATUS["paused"] = True
        time.sleep(1)  # Let current cycle finish
        try:
            # Flush in-memory processed set from existing poller
            if _shared_poller and hasattr(_shared_poller, '_processed'):
                old_count = len(_shared_poller._processed)
                _shared_poller._processed.clear()
                log.info("Flushed %d UIDs from in-memory poller", old_count)
            # Delete on-disk file
            if os.path.exists(proc_file):
                with open(proc_file) as f:
                    proc_data = json.load(f)
                if not old_count:
                    old_count = len(proc_data) if isinstance(proc_data, list) else 0
                os.remove(proc_file)
            log.info("Force-recapture: cleared %d processed UIDs for '%s'", old_count, match_kw)
        except Exception as e:
            log.error("Force-recapture cleanup error: %s", e)
        _shared_poller = None
        POLL_STATUS["paused"] = False
        return jsonify({
            "ok": True,
            "message": f"Cleared {old_count} processed UIDs. Hit Check Now to re-import.",
            "cleared_uids": old_count,
        })
    
    # Reset poller so next Check Now uses fresh state
    _shared_poller = None
    
    return jsonify({
        "ok": True,
        "removed_rfqs": removed_rfqs,
        "removed_pcs": removed_pcs,
        "cleared_uids": len(cleared_uids),
        "next_step": "Hit Check Now to re-import",
    })


# ═══════════════════════════════════════════════════════════════════════
# Deep Email Diagnostic — trace every email through the pipeline
# ═══════════════════════════════════════════════════════════════════════
@bp.route("/api/email-trace")
def api_email_trace():
    """Show processed UIDs state and allow nuclear clear."""
    global _shared_poller
    
    action = request.args.get("action", "")
    
    # Load processed UIDs from disk
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    try:
        with open(proc_file) as f:
            processed_disk = json.load(f)
    except:
        processed_disk = []
    
    # In-memory poller state
    poller_mem = []
    if _shared_poller and hasattr(_shared_poller, '_processed'):
        poller_mem = list(_shared_poller._processed)
    
    # Nuclear clear
    if action == "nuke":
        # 1. Pause background thread
        POLL_STATUS["paused"] = True
        time.sleep(1)
        
        # 2. Clear in-memory poller
        old_mem = len(poller_mem)
        if _shared_poller and hasattr(_shared_poller, '_processed'):
            _shared_poller._processed.clear()
        
        # 3. Delete processed file
        old_disk = len(processed_disk)
        try:
            if os.path.exists(proc_file):
                os.remove(proc_file)
        except:
            pass
        
        # 4. Kill poller so next Check Now creates fresh one
        _shared_poller = None
        
        # 5. Unpause
        POLL_STATUS["paused"] = False
        
        return jsonify({
            "ok": True,
            "action": "NUKED",
            "cleared_memory": old_mem,
            "cleared_disk": old_disk,
            "next": "Hit Check Now on dashboard to re-import all emails",
        })
    
    # Diagnostic only
    diag = {}
    try:
        if _shared_poller and hasattr(_shared_poller, '_diag'):
            raw = _shared_poller._diag
            diag = {k: (list(v) if isinstance(v, set) else v) for k, v in raw.items()}
    except Exception as e:
        diag = {"error": str(e)}
    
    try:
        poll_st = {k: str(v) for k, v in POLL_STATUS.items()}
    except:
        poll_st = {}
    
    from flask import Response
    import json as _json
    result = {
        "processed_on_disk": len(processed_disk),
        "processed_in_memory": len(poller_mem),
        "poller_exists": _shared_poller is not None,
        "disk_uids": processed_disk,
        "memory_uids": poller_mem,
        "last_poll_diag": diag,
        "poll_status": poll_st,
        "hint": "Add ?action=nuke to clear everything, then hit Check Now",
    }
    return Response(_json.dumps(result, default=str), mimetype="application/json")


@bp.route("/api/disk-cleanup")
def api_disk_cleanup():
    """Show disk usage and clean old upload files."""
    import shutil
    
    action = request.args.get("action", "")
    
    # Get disk usage
    total, used, free = shutil.disk_usage("/")
    
    # Count upload dirs
    upload_sizes = {}
    total_upload = 0
    if os.path.exists(UPLOAD_DIR):
        for entry in os.scandir(UPLOAD_DIR):
            if entry.is_dir():
                dir_size = sum(f.stat().st_size for f in os.scandir(entry.path) if f.is_file())
                upload_sizes[entry.name] = dir_size
                total_upload += dir_size
            elif entry.is_file():
                upload_sizes[entry.name] = entry.stat().st_size
                total_upload += entry.stat().st_size
    
    # Data dir sizes
    data_sizes = {}
    total_data = 0
    if os.path.exists(DATA_DIR):
        for entry in os.scandir(DATA_DIR):
            if entry.is_file():
                sz = entry.stat().st_size
                data_sizes[entry.name] = round(sz / 1024 / 1024, 2)
                total_data += sz
            elif entry.is_dir():
                dir_sz = 0
                try:
                    for root, dirs, files in os.walk(entry.path):
                        for f in files:
                            dir_sz += os.path.getsize(os.path.join(root, f))
                except:
                    pass
                data_sizes[entry.name + "/"] = round(dir_sz / 1024 / 1024, 2)
                total_data += dir_sz
    
    result = {
        "disk_total_mb": round(total / 1024 / 1024),
        "disk_used_mb": round(used / 1024 / 1024),
        "disk_free_mb": round(free / 1024 / 1024),
        "uploads_total_mb": round(total_upload / 1024 / 1024, 1),
        "uploads_dirs": len([k for k in upload_sizes if not k.startswith(".")]),
        "data_total_mb": round(total_data / 1024 / 1024, 1),
        "data_files": dict(sorted(data_sizes.items(), key=lambda x: -x[1])[:20]),
    }
    
    if action == "clean":
        # Get list of RFQ dirs still referenced by active queue items
        active_dirs = set()
        try:
            rfqs = load_rfqs()
            for r in rfqs.values():
                d = r.get("rfq_dir", "")
                if d:
                    active_dirs.add(os.path.basename(d))
        except:
            pass
        try:
            pcs = _load_price_checks()
            for pc in pcs.values():
                d = pc.get("rfq_dir", "")
                if d:
                    active_dirs.add(os.path.basename(d))
        except:
            pass
        
        removed = 0
        freed = 0
        if os.path.exists(UPLOAD_DIR):
            for entry in os.scandir(UPLOAD_DIR):
                if entry.is_dir() and entry.name not in active_dirs:
                    dir_size = sum(f.stat().st_size for f in os.scandir(entry.path) if f.is_file())
                    shutil.rmtree(entry.path, ignore_errors=True)
                    removed += 1
                    freed += dir_size
        
        result["action"] = "cleaned"
        result["removed_dirs"] = removed
        result["freed_mb"] = round(freed / 1024 / 1024, 1)
        result["kept_active"] = len(active_dirs)
    
    if action == "nuke-uploads":
        # Remove ALL upload dirs
        freed = 0
        removed = 0
        if os.path.exists(UPLOAD_DIR):
            for entry in os.scandir(UPLOAD_DIR):
                if entry.is_dir():
                    dir_size = sum(f.stat().st_size for f in os.scandir(entry.path) if f.is_file())
                    shutil.rmtree(entry.path, ignore_errors=True)
                    removed += 1
                    freed += dir_size
        result["action"] = "nuked-uploads"
        result["removed_dirs"] = removed
        result["freed_mb"] = round(freed / 1024 / 1024, 1)
    
    if action == "trim-data":
        # Trim large data files: truncate logs, compact JSON, remove caches
        freed = 0
        trimmed = []
        safe_to_truncate = ["crm_activity.json", "email_log.json", "notification_log.json",
                            "follow_up_log.json", "detected_shipments.json", "lead_scores.json"]
        safe_to_delete = []
        
        for fname in os.listdir(DATA_DIR):
            fpath = os.path.join(DATA_DIR, fname)
            if not os.path.isfile(fpath):
                continue
            fsize = os.path.getsize(fpath)
            
            # Truncate large log-type JSON files to last 200 entries
            if fname in safe_to_truncate and fsize > 100_000:
                try:
                    with open(fpath) as f:
                        data = json.load(f)
                    if isinstance(data, list) and len(data) > 200:
                        old_size = fsize
                        with open(fpath, "w") as f:
                            json.dump(data[-200:], f)
                        new_size = os.path.getsize(fpath)
                        freed += old_size - new_size
                        trimmed.append(f"{fname}: {round(old_size/1024)}K → {round(new_size/1024)}K")
                except:
                    pass
            
            # Delete .bak files and temp files
            if fname.endswith(".bak") or fname.endswith(".tmp"):
                freed += fsize
                os.remove(fpath)
                trimmed.append(f"deleted {fname} ({round(fsize/1024)}K)")
        
        # Clean SQLite WAL/SHM files (they can get huge)
        for ext in ["-wal", "-shm"]:
            for fname in os.listdir(DATA_DIR):
                if fname.endswith(ext):
                    fpath = os.path.join(DATA_DIR, fname)
                    fsize = os.path.getsize(fpath)
                    if fsize > 1_000_000:  # Only if > 1MB
                        freed += fsize
                        os.remove(fpath)
                        trimmed.append(f"deleted {fname} ({round(fsize/1024/1024,1)}MB)")
        
        result["action"] = "trimmed-data"
        result["freed_mb"] = round(freed / 1024 / 1024, 1)
        result["trimmed"] = trimmed
    
    return Response(json.dumps(result, default=str), mimetype="application/json")

