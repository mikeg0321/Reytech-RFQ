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

# Import from src modules — no root-level fallbacks (root dupes deleted in S1.1)
from src.forms.reytech_filler_v4 import (load_config, get_pst_date, fill_703b, fill_704b, fill_bid_package, fill_obs1600, fill_obs1600_fields)
from src.forms.rfq_parser import parse_rfq_attachments, identify_attachments
from src.agents.scprs_lookup import bulk_lookup, save_prices_from_rfq, get_price_db_stats
from src.agents.email_poller import EmailPoller, EmailSender

# v6.0: Pricing intelligence (graceful — module might not exist yet)
try:
    from src.knowledge.pricing_oracle import recommend_prices_for_rfq, pricing_health_check
    from src.knowledge.won_quotes_db import (ingest_scprs_result, find_similar_items,
                                get_kb_stats, get_price_history)
    PRICING_ORACLE_AVAILABLE = True
except ImportError:
    PRICING_ORACLE_AVAILABLE = False

# v6.1: Product Research Agent (graceful — requires API keys)
try:
    from src.agents.product_research import (research_product, research_rfq_items,
                                   quick_lookup, test_amazon_search,
                                   get_research_cache_stats, RESEARCH_STATUS)
    PRODUCT_RESEARCH_AVAILABLE = True
except ImportError:
    PRODUCT_RESEARCH_AVAILABLE = False

# v6.2: Price Check Processor
try:
    from src.forms.price_check import (parse_ams704, process_price_check, lookup_prices,
                              test_parse, REYTECH_INFO, clean_description)
    PRICE_CHECK_AVAILABLE = True
except ImportError:
    PRICE_CHECK_AVAILABLE = False

# v7.1: Reytech Quote Generator
try:
    from src.forms.quote_generator import (generate_quote, generate_quote_from_pc,
                                  generate_quote_from_rfq, AGENCY_CONFIGS,
                                  get_all_quotes, search_quotes,
                                  peek_next_quote_number, update_quote_status,
                                  get_quote_stats, set_quote_counter,
                                  _detect_agency)
    QUOTE_GEN_AVAILABLE = True
except ImportError:
    QUOTE_GEN_AVAILABLE = False

# v7.0: Auto-Processor Engine
try:
    from src.auto.auto_processor import (auto_process_price_check, detect_document_type,
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



from src.api.shared import (bp, auth_required, check_auth, _check_rate_limit,
                            DASH_USER, DASH_PASS, RATE_LIMIT_MAX, RATE_LIMIT_AUTH_MAX)
# Secret key set in app.py

# ── time alias for auto-pricing and other timing ─────────────────────────────
import time as _time
# Auth guard + request logging + CSRF now in src/api/shared.py

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

# ── Pending PO Award Review Queue ──────────────────────────────────
_pending_po_reviews = []  # In-memory + persisted to data/pending_po_reviews.json

def _load_pending_pos():
    global _pending_po_reviews
    try:
        with open(os.path.join(DATA_DIR, "pending_po_reviews.json")) as f:
            _pending_po_reviews = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        _pending_po_reviews = []
    return _pending_po_reviews

def _save_pending_pos():
    with open(os.path.join(DATA_DIR, "pending_po_reviews.json"), "w") as f:
        json.dump(_pending_po_reviews, f, indent=2, default=str)

def _add_pending_po(po_data):
    """Add a PO to the pending review queue instead of auto-creating order."""
    po_data["review_status"] = "pending"
    po_data["detected_at"] = datetime.now().isoformat()
    _load_pending_pos()
    _pending_po_reviews.append(po_data)
    # Trim old entries (>90 days) to prevent unbounded growth
    _cutoff = (datetime.now() - timedelta(days=90)).isoformat()
    while len(_pending_po_reviews) > 200:
        if _pending_po_reviews[0].get("detected_at", "") < _cutoff:
            _pending_po_reviews.pop(0)
        else:
            break
    _save_pending_pos()
    _po_total = 0
    try:
        _po_total = float(po_data.get("total", 0) or 0)
    except (ValueError, TypeError):
        _po_total = 0
    try:
        _push_notification({
            "type": "po_award",
            "title": f"🏆 PO Received: {po_data.get('po_number', '?')}",
            "detail": f"From {po_data.get('agency', po_data.get('sender_email', '?'))} — {len(po_data.get('items', []))} items, ${_po_total:,.2f}",
            "action_url": "/awards",
            "urgent": True,
        })
    except Exception:
        pass
    log.info("PO %s added to pending review queue (%d items, $%.2f)",
             po_data.get("po_number", "?"), len(po_data.get("items", [])), _po_total)

# ── Thread-safe locks for all mutated global state ──────────────────────────
_poll_status_lock   = threading.Lock()
_rate_limiter_lock  = threading.Lock()
_json_cache_lock    = threading.Lock()
_save_rfqs_lock     = threading.Lock()
_save_pcs_lock      = threading.Lock()

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
            # Use cache only if mtime matches AND cache is <10s old
            if cached and cached["mtime"] == mtime and (now - cached["ts"]) < 10.0:
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
# Auth credentials now in src/api/shared.py

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

# check_auth, auth_required, _check_rate_limit now imported from src.api.shared

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
def _normalize_rfq_fields(rfqs: dict) -> dict:
    """Ensure both field name aliases exist on every RFQ dict.

    SQLite uses 'items' + 'rfq_number'; JSON/templates use 'line_items' + 'solicitation_number'.
    This guarantees every template works regardless of which name it references.
    """
    for rid, r in rfqs.items():
        if not isinstance(r, dict):
            continue
        # items ↔ line_items
        if "items" in r and "line_items" not in r:
            r["line_items"] = r["items"]
        if "line_items" in r and "items" not in r:
            r["items"] = r["line_items"]
        # rfq_number ↔ solicitation_number
        if "rfq_number" in r and "solicitation_number" not in r:
            r["solicitation_number"] = r["rfq_number"]
        if "solicitation_number" in r and "rfq_number" not in r:
            r["rfq_number"] = r["solicitation_number"]
    return rfqs


def load_rfqs():
    """Load RFQs — SQLite primary with data_json blob for full fidelity.

    Priority: data_json blob (complete dict) > structured columns > JSON file fallback.
    Structured columns are kept for indexing/querying. data_json has ALL fields.
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM rfqs ORDER BY received_at DESC LIMIT 10000"
            ).fetchall()
            if not rows:
                # Empty DB — fall back to JSON
                return _normalize_rfq_fields(_cached_json_load(rfq_db_path(), fallback={}))

            result = {}
            for row in rows:
                d = dict(row)
                rid = d.get("id", "")
                if not rid:
                    continue

                # If data_json exists, it's the authoritative source
                blob = d.pop("data_json", None)
                if blob:
                    try:
                        full = json.loads(blob)
                        # Overlay structured columns for any that are newer
                        # (e.g. status updated via direct SQL)
                        for key in ("status", "updated_at", "reytech_quote_number"):
                            if d.get(key) and d[key] != full.get(key):
                                full[key] = d[key]
                        result[rid] = full
                        continue
                    except (json.JSONDecodeError, TypeError):
                        pass

                # No data_json — use structured columns (legacy/migration path)
                # Parse items from JSON string
                items_raw = d.get("items", "[]")
                if isinstance(items_raw, str):
                    try:
                        d["items"] = json.loads(items_raw)
                    except Exception:
                        d["items"] = []
                result[rid] = d

            # Merge any fields from JSON file not yet in data_json (one-time migration)
            json_data = _cached_json_load(rfq_db_path(), fallback={})
            if json_data:
                for rid, r in result.items():
                    jr = json_data.get(rid, {})
                    if not jr:
                        continue
                    # Only merge fields missing from the loaded record
                    for key, val in jr.items():
                        if key not in r or (not r[key] and val):
                            r[key] = val

            return _normalize_rfq_fields(result)
    except Exception as e:
        log.warning("load_rfqs failed: %s", str(e)[:200])

    return _normalize_rfq_fields(_cached_json_load(rfq_db_path(), fallback={}))

def save_rfqs(rfqs):
    with _save_rfqs_lock:
        import traceback
        p = rfq_db_path()
        _invalidate_cache(p)
        # ── PRIMARY: Write to SQLite ──────────────────────────────────
        try:
            from src.core.db import get_db
            with get_db() as conn:
                for rid, r in rfqs.items():
                    conn.execute("""
                        INSERT OR REPLACE INTO rfqs
                        (id, received_at, agency, institution, requestor_name, requestor_email,
                         rfq_number, items, status, source, email_uid, notes,
                         solicitation_number, due_date, email_subject, body_text, form_type,
                         reytech_quote_number, shipping_option, shipping_amount, delivery_location,
                         updated_at, data_json)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),?)
                    """, (
                        rid, r.get("received_at", ""), r.get("agency", ""),
                        r.get("institution", ""), r.get("requestor_name", ""),
                        r.get("requestor_email", ""),
                        r.get("rfq_number", "") or r.get("solicitation_number", ""),
                        json.dumps(r.get("line_items", r.get("items", [])), default=str),
                        r.get("status", "new"), r.get("source", ""),
                        r.get("email_uid", ""), r.get("notes", ""),
                        r.get("solicitation_number", "") or r.get("rfq_number", ""),
                        r.get("due_date", ""),
                        r.get("email_subject", ""),
                        (r.get("body_text", "") or "")[:3000],
                        r.get("form_type", ""),
                        r.get("reytech_quote_number", ""),
                        r.get("shipping_option", "included"),
                        r.get("shipping_amount", 0),
                        r.get("delivery_location", ""),
                        json.dumps(r, default=str),
                    ))
        except Exception as e:
            log.error("SQLite write failed for rfqs: %s", str(e)[:200])
        # ── BACKUP: Write JSON cache with data guard ──────────────────
        try:
            caller = traceback.extract_stack()[-2]
            reason = f"{caller.filename.split('/')[-1]}:{caller.lineno} {caller.name}"
            from src.core.data_guard import safe_save_json
            safe_save_json(p, rfqs, reason=reason)
        except Exception as e:
            log.warning("JSON backup write failed for rfqs: %s", e)

def backfill_rfq_metadata(dry_run=False):
    """Extract solicitation numbers and due dates from existing RFQs that are missing them.

    Recovery sources (in priority order):
    1. Stored PDFs in rfq_files table (re-parse 703B/704B forms)
    2. Email subject/body text
    3. Matching price check data
    """
    rfqs = load_rfqs()
    updated = 0
    details = []
    for rid, r in rfqs.items():
        changed = False
        _needs_sol = not r.get("solicitation_number") or r.get("solicitation_number") == "unknown"
        _needs_due = not r.get("due_date") or r.get("due_date") in ("", "TBD")

        if not _needs_sol and not _needs_due:
            continue

        # ── Source 1: Re-parse stored PDFs from rfq_files table ──────────
        if _needs_sol or _needs_due:
            try:
                from src.core.db import get_db
                import tempfile
                with get_db() as db:
                    files = db.execute(
                        "SELECT filename, file_type, data FROM rfq_files WHERE rfq_id = ?",
                        (rid,)).fetchall()
                for frow in files:
                    fname = (frow[0] or "").lower()
                    fdata = frow[2]
                    if not fdata or not fname.endswith(".pdf"):
                        continue
                    # Write to temp file for parsing
                    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                        tmp.write(fdata)
                        tmp_path = tmp.name
                    try:
                        if "703b" in fname or "703b" in (frow[1] or "").lower():
                            from src.forms.rfq_parser import parse_703b
                            parsed = parse_703b(tmp_path)
                            if _needs_sol and parsed.get("solicitation_number"):
                                r["solicitation_number"] = parsed["solicitation_number"]
                                _needs_sol = False
                                changed = True
                            if _needs_due and parsed.get("due_date"):
                                r["due_date"] = parsed["due_date"]
                                _needs_due = False
                                changed = True
                            # Also recover requestor info if missing
                            if not r.get("requestor_name") and parsed.get("requestor_name"):
                                r["requestor_name"] = parsed["requestor_name"]
                                changed = True
                        elif "704b" in fname or "704" in fname or "704" in (frow[1] or "").lower():
                            from src.forms.rfq_parser import parse_704b
                            parsed = parse_704b(tmp_path)
                            header = parsed.get("header", {})
                            if _needs_sol and header.get("solicitation_number"):
                                r["solicitation_number"] = header["solicitation_number"]
                                _needs_sol = False
                                changed = True
                        else:
                            # Try text extraction for solicitation/due date
                            try:
                                from pypdf import PdfReader
                                reader = PdfReader(tmp_path)
                                text = " ".join((p.extract_text() or "") for p in reader.pages[:3])
                                if _needs_sol:
                                    sol = _extract_solicitation(text)
                                    if sol:
                                        r["solicitation_number"] = sol
                                        _needs_sol = False
                                        changed = True
                                if _needs_due:
                                    due = _extract_due_date(text)
                                    if due:
                                        r["due_date"] = due
                                        _needs_due = False
                                        changed = True
                            except Exception:
                                pass
                    finally:
                        try:
                            os.unlink(tmp_path)
                        except Exception:
                            pass
            except Exception as _e:
                log.debug("Backfill PDF re-parse for %s: %s", rid, _e)

        # ── Source 2: Email subject/body text ──────────────────────────────
        if _needs_sol or _needs_due:
            subject = r.get("email_subject", "")
            body = r.get("body_text", r.get("email_body", ""))
            combined = f"{subject} {body} {r.get('parse_note', '')}"
            if _needs_sol:
                sol = _extract_solicitation(combined)
                if sol:
                    r["solicitation_number"] = sol
                    _needs_sol = False
                    changed = True
            if _needs_due:
                due = _extract_due_date(combined)
                if due:
                    r["due_date"] = due
                    _needs_due = False
                    changed = True

        # ── Source 3: Matching price check data ───────────────────────────
        if _needs_sol or _needs_due:
            email = r.get("requestor_email", "")
            if email:
                try:
                    from src.core.db import get_db
                    with get_db() as db:
                        pc_row = db.execute("""
                            SELECT pc_data FROM price_checks
                            WHERE pc_data LIKE ?
                            ORDER BY created_at DESC LIMIT 1
                        """, (f"%{email}%",)).fetchone()
                    if pc_row:
                        pc_data = json.loads(pc_row[0] or "{}")
                        if _needs_sol:
                            _sol = pc_data.get("pc_number", pc_data.get("solicitation_number", ""))
                            if _sol:
                                r["solicitation_number"] = _sol
                                changed = True
                        if _needs_due:
                            _due = pc_data.get("due_date", "")
                            if _due:
                                r["due_date"] = _due
                                changed = True
                except Exception:
                    pass

        if changed:
            updated += 1
            details.append({"id": rid, "sol": r.get("solicitation_number", ""),
                            "due": r.get("due_date", ""), "source": "recovered"})
        else:
            details.append({"id": rid, "sol": "", "due": "", "source": "no_data_found",
                            "email": r.get("requestor_email", "")})

    if updated and not dry_run:
        save_rfqs(rfqs)
        log.info("Backfilled metadata for %d RFQs: %s", updated,
                 [d for d in details if d["source"] == "recovered"])
    return {"updated": updated, "details": details, "dry_run": dry_run,
            "total_checked": len(details)}


def imap_backfill_rfq_metadata(dry_run=False):
    """Re-fetch original emails from IMAP to recover solicitation#, due dates, and PDFs.

    For each RFQ missing metadata:
    1. Connect to IMAP, fetch email by UID
    2. Extract subject, body, PDF attachments
    3. Re-parse PDFs (703B/704B form fields, text extraction)
    4. Update RFQ with recovered data
    5. Store PDFs in rfq_files table
    """
    import imaplib
    import email as _email_mod
    from email.header import decode_header as _decode_header
    import tempfile

    rfqs = load_rfqs()
    results = []

    # Connect to IMAP
    imap_user = os.environ.get("GMAIL_ADDRESS", "")
    imap_pass = os.environ.get("GMAIL_PASSWORD", "")
    if not imap_user or not imap_pass:
        return {"error": "GMAIL_ADDRESS or GMAIL_PASSWORD not set", "updated": 0}

    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        mail.login(imap_user, imap_pass)
        mail.select("INBOX")
    except Exception as e:
        return {"error": f"IMAP connect failed: {e}", "updated": 0}

    def _decode_hdr(header):
        if not header:
            return ""
        try:
            parts = _decode_header(header)
            out = ""
            for content, charset in parts:
                if isinstance(content, bytes):
                    out += content.decode(charset or "utf-8", errors="replace")
                else:
                    out += content
            return out
        except Exception:
            return str(header)

    def _get_body(msg):
        bodies = []
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        bodies.append(payload.decode("utf-8", errors="replace"))
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                bodies.append(payload.decode("utf-8", errors="replace"))
        return "\n".join(bodies)

    def _get_pdfs(msg):
        pdfs = []
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            filename = part.get_filename()
            if not filename:
                continue
            filename = _decode_hdr(filename) if isinstance(filename, str) else str(filename)
            if not filename.lower().endswith(".pdf"):
                continue
            payload = part.get_payload(decode=True)
            if payload:
                pdfs.append({"filename": filename, "data": payload})
            # Also check nested message/rfc822 parts
        for part in msg.walk():
            if part.get_content_type() == "message/rfc822":
                inner = part.get_payload()
                msgs = inner if isinstance(inner, list) else [inner] if hasattr(inner, 'walk') else []
                for inner_msg in msgs:
                    for ipart in inner_msg.walk():
                        fn = ipart.get_filename()
                        if not fn:
                            continue
                        fn = _decode_hdr(fn) if isinstance(fn, str) else str(fn)
                        if fn.lower().endswith(".pdf"):
                            pl = ipart.get_payload(decode=True)
                            if pl:
                                pdfs.append({"filename": fn, "data": pl})
        return pdfs

    updated = 0
    for rid, r in rfqs.items():
        _needs_sol = not r.get("solicitation_number") or r.get("solicitation_number") == "unknown"
        _needs_due = not r.get("due_date") or r.get("due_date") in ("", "TBD")
        _needs_subject = not r.get("email_subject")
        _needs_body = not r.get("body_text")

        if not (_needs_sol or _needs_due or _needs_subject or _needs_body):
            continue

        uid = r.get("email_uid", "")
        if not uid:
            results.append({"id": rid, "status": "no_uid"})
            continue

        try:
            status, data = mail.uid("fetch", uid.encode(), "(BODY.PEEK[])")
            if status != "OK" or not data or not data[0]:
                results.append({"id": rid, "status": "fetch_failed", "uid": uid})
                continue

            msg = _email_mod.message_from_bytes(data[0][1])
            subject = _decode_hdr(msg["Subject"]) or ""
            body = _get_body(msg)
            pdfs = _get_pdfs(msg)
            combined = f"{subject} {body}"

            entry = {"id": rid, "uid": uid, "subject": subject[:80],
                     "pdfs": len(pdfs), "sol": "", "due": ""}

            # Update email_subject and body_text
            if _needs_subject and subject:
                r["email_subject"] = subject
            if _needs_body and body:
                r["body_text"] = body[:3000]

            # Parse PDFs for solicitation and due date
            for pdf in pdfs:
                fname_lower = pdf["filename"].lower()
                # Store PDF in rfq_files
                if not dry_run:
                    try:
                        import re as _re
                        safe_fn = _re.sub(r'[^\w\-_. ()]+', '_', pdf["filename"])
                        ftype = "unknown"
                        if "703b" in fname_lower:
                            ftype = "template_703b"
                        elif "704b" in fname_lower or "704" in fname_lower:
                            ftype = "template_704b"
                        elif "bid" in fname_lower and "package" in fname_lower:
                            ftype = "template_bidpkg"
                        save_rfq_file(rid, safe_fn, ftype, pdf["data"],
                                      category="template" if ftype != "unknown" else "attachment")
                    except Exception as _fe:
                        log.debug("Store PDF %s for %s: %s", pdf["filename"], rid, _fe)

                # Parse for metadata
                if _needs_sol or _needs_due:
                    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                        tmp.write(pdf["data"])
                        tmp_path = tmp.name
                    try:
                        if "703b" in fname_lower:
                            from src.forms.rfq_parser import parse_703b
                            parsed = parse_703b(tmp_path)
                            if _needs_sol and parsed.get("solicitation_number"):
                                _psol = parsed["solicitation_number"].strip().rstrip("_.- ")
                                if _psol and len(_psol) > 2 and "_____" not in _psol and _psol.lower() not in ("number", "response"):
                                    r["solicitation_number"] = _psol
                                    _needs_sol = False
                                    entry["sol"] = _psol
                            if _needs_due and parsed.get("due_date"):
                                r["due_date"] = _normalize_date(parsed["due_date"])
                                _needs_due = False
                                entry["due"] = r["due_date"]
                            # Recover requestor info
                            if not r.get("requestor_name") and parsed.get("requestor_name"):
                                r["requestor_name"] = parsed["requestor_name"]
                            if not r.get("requestor_email") and parsed.get("requestor_email"):
                                r["requestor_email"] = parsed["requestor_email"]
                            # Also set form_type
                            if not r.get("form_type"):
                                r["form_type"] = "ams_704"
                        elif "704b" in fname_lower or "704" in fname_lower:
                            from src.forms.rfq_parser import parse_704b
                            parsed = parse_704b(tmp_path)
                            header = parsed.get("header", {})
                            if _needs_sol and header.get("solicitation_number"):
                                _hsol = header["solicitation_number"].strip().rstrip("_.- ")
                                if _hsol and len(_hsol) > 2 and "_____" not in _hsol and _hsol.lower() not in ("number", "response"):
                                    r["solicitation_number"] = _hsol
                                    _needs_sol = False
                                    entry["sol"] = _hsol
                            if not r.get("form_type"):
                                r["form_type"] = "ams_704"
                        else:
                            # Generic PDF — text extraction
                            try:
                                from pypdf import PdfReader
                                reader = PdfReader(tmp_path)
                                text = " ".join((p.extract_text() or "") for p in reader.pages[:3])
                                if _needs_sol:
                                    sol = _extract_solicitation(text)
                                    if sol:
                                        r["solicitation_number"] = sol
                                        _needs_sol = False
                                        entry["sol"] = sol
                                if _needs_due:
                                    due = _extract_due_date(text)
                                    if due:
                                        r["due_date"] = due
                                        _needs_due = False
                                        entry["due"] = due
                            except Exception:
                                pass
                    except Exception as _pe:
                        log.debug("Parse PDF %s for %s: %s", pdf["filename"], rid, _pe)
                    finally:
                        try:
                            os.unlink(tmp_path)
                        except Exception:
                            pass

            # Fallback: extract from email text
            if _needs_sol:
                sol = _extract_solicitation(combined)
                if sol:
                    r["solicitation_number"] = sol
                    entry["sol"] = sol
            if _needs_due:
                due = _extract_due_date(combined)
                if due:
                    r["due_date"] = due
                    entry["due"] = due

            entry["recovered"] = bool(entry["sol"] or entry["due"] or subject)
            results.append(entry)
            if entry["recovered"]:
                updated += 1

        except Exception as e:
            results.append({"id": rid, "uid": uid, "status": "error", "error": str(e)[:200]})

    try:
        mail.logout()
    except Exception:
        pass

    if updated and not dry_run:
        save_rfqs(rfqs)
        log.info("IMAP backfill: recovered metadata for %d RFQs", updated)

    return {"updated": updated, "results": results, "dry_run": dry_run}


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
            
            # ── Dedup cleanup: remove older duplicates (keep latest per rfq_id+filename+category) ──
            try:
                count = conn.execute("SELECT COUNT(*) FROM rfq_files").fetchone()[0]
                if count > 0:
                    dupes = conn.execute("""
                        DELETE FROM rfq_files WHERE id NOT IN (
                            SELECT id FROM (
                                SELECT id, ROW_NUMBER() OVER (
                                    PARTITION BY rfq_id, filename, category 
                                    ORDER BY created_at DESC
                                ) as rn FROM rfq_files
                            ) WHERE rn = 1
                        )
                    """)
                    if dupes.rowcount > 0:
                        log.info("Dedup cleanup: removed %d duplicate rfq_files rows", dupes.rowcount)
            except Exception as _de:
                log.debug("Dedup cleanup skipped: %s", _de)
    except Exception as e:
        log.debug("rfq_files table init: %s", e)

# Init on import
_init_rfq_files_table()

# Seed buyer preferences and form template registry
try:
    from src.core.dal import seed_known_buyer_preferences, seed_form_template_registry
    seed_known_buyer_preferences()
    seed_form_template_registry()
except Exception as _seed_e:
    log.debug("Seed on startup: %s", _seed_e)


def save_rfq_file(rfq_id: str, filename: str, file_type: str, data: bytes,
                   category: str = "template", uploaded_by: str = "system") -> str:
    """Save a PDF to the rfq_files table. Returns file_id.
    
    DEDUP: If a file with the same rfq_id + filename + category already exists,
    updates the existing row instead of creating a duplicate.
    """
    import uuid
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Check for existing file with same rfq_id + filename + category
            existing = conn.execute(
                "SELECT id FROM rfq_files WHERE rfq_id=? AND filename=? AND category=? LIMIT 1",
                (rfq_id, filename, category)).fetchone()
            
            if existing:
                # Update existing row
                file_id = existing["id"]
                conn.execute("""
                    UPDATE rfq_files SET file_type=?, file_size=?, data=?, uploaded_by=?, created_at=?
                    WHERE id=?
                """, (file_type, len(data), data, uploaded_by, datetime.now().isoformat(), file_id))
                log.info("Updated file %s (%s, %d bytes) for RFQ %s", filename, file_type, len(data), rfq_id)
            else:
                # Insert new row
                file_id = f"rf_{uuid.uuid4().hex[:10]}"
                conn.execute("""
                    INSERT INTO rfq_files (id, rfq_id, filename, file_type, category, file_size, data, uploaded_by, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (file_id, rfq_id, filename, file_type, category, len(data), data, uploaded_by, datetime.now().isoformat()))
                log.info("Saved file %s (%s, %d bytes) for RFQ %s", filename, file_type, len(data), rfq_id)
    except Exception as e:
        log.error("Failed to save file %s for RFQ %s: %s", filename, rfq_id, e)
        file_id = f"rf_{uuid.uuid4().hex[:10]}"
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
    """List files for an RFQ. Returns list of dicts (without BLOB data).
    
    DEDUP: If multiple rows exist for the same filename+category (legacy),
    only returns the most recent one per filename+category.
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            if category:
                rows = conn.execute(
                    "SELECT id, rfq_id, filename, file_type, category, file_size, uploaded_by, created_at FROM rfq_files WHERE rfq_id = ? AND category = ? ORDER BY created_at DESC",
                    (rfq_id, category)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, rfq_id, filename, file_type, category, file_size, uploaded_by, created_at FROM rfq_files WHERE rfq_id = ? ORDER BY created_at DESC",
                    (rfq_id,)).fetchall()
            # Dedup: keep only the latest row per (filename, category)
            seen = set()
            deduped = []
            for r in rows:
                key = (r["filename"], r["category"])
                if key not in seen:
                    seen.add(key)
                    deduped.append(dict(r))
            # Return in chronological order (oldest first)
            deduped.reverse()
            return deduped
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

_pc_cache = None
_pc_cache_time = 0

def _load_price_checks(include_items=True):
    """Load price checks — DAL (SQLite) primary, JSON fallback.

    Layer 4 migration: DAL first, JSON fallback if DAL empty/fails.
    In-memory cache (30s TTL) prevents repeated DB queries.
    """
    global _pc_cache, _pc_cache_time
    import time as _t
    now = _t.time()
    if include_items and _pc_cache is not None and (now - _pc_cache_time) < 30:
        return _pc_cache

    data = {}

    # SQLite primary read — use data_json blob if available
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM price_checks ORDER BY created_at DESC LIMIT 10000"
            ).fetchall()
            for row in rows:
                d = dict(row)
                pcid = d.get("id", "")
                if not pcid:
                    continue
                blob = d.pop("data_json", None)
                if blob:
                    try:
                        full = json.loads(blob)
                        for key in ("status", "quote_number"):
                            if d.get(key) and d[key] != full.get(key):
                                full[key] = d[key]
                        data[pcid] = full
                        continue
                    except (json.JSONDecodeError, TypeError):
                        pass
                # No data_json — use structured columns
                items_raw = d.get("items", "[]")
                if isinstance(items_raw, str):
                    try:
                        d["items"] = json.loads(items_raw)
                    except Exception:
                        d["items"] = []
                data[pcid] = d
    except Exception as e:
        log.warning("SQLite load_pcs failed, falling back to JSON: %s", str(e)[:200])

    # JSON fallback — only if SQLite returned nothing
    if not data:
        json_path = os.path.join(DATA_DIR, "price_checks.json")
        if os.path.exists(json_path):
            try:
                with open(json_path) as f:
                    data = json.load(f)
            except Exception:
                data = {}

    # Normalize: ensure items/line_items both exist
    for pcid, pc in data.items():
        if not isinstance(pc, dict):
            continue
        if "items" in pc and "line_items" not in pc:
            pc["line_items"] = list(pc["items"])  # shallow copy to prevent aliasing
        if "line_items" in pc and "items" not in pc:
            pc["items"] = list(pc["line_items"])  # shallow copy to prevent aliasing

    # Only cache full results (with items)
    if include_items:
        _pc_cache = data
        _pc_cache_time = _t.time()
    return data

def _save_single_pc(pc_id, pc):
    """Save a SINGLE price check to SQLite without touching any other PCs.
    This prevents background agents from overwriting user's edits on other PCs."""
    with _save_pcs_lock:
        global _pc_cache, _pc_cache_time
        _pc_cache = None
        _pc_cache_time = 0
        try:
            from src.core.db import get_db
            with get_db() as conn:
                items_json = json.dumps(pc.get("items", []), default=str)
                _pc_clean = {k: v for k, v in pc.items() if k != "pc_data"}
                pc_blob = json.dumps(_pc_clean, default=str)
                conn.execute("""
                    INSERT OR REPLACE INTO price_checks
                    (id, created_at, requestor, agency, institution, items, source_file,
                     quote_number, pc_number, total_items, status,
                     email_uid, email_subject, due_date, pc_data, ship_to, data_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    pc_blob,
                    pc.get("ship_to", ""),
                    json.dumps(pc, default=str),
                ))
        except Exception as e:
            log.error("DB save_single_pc failed for %s: %s", pc_id, e)


def _save_price_checks(pcs):
    """Save price checks to SQLite (primary) + JSON backup cache."""
    with _save_pcs_lock:
        global _pc_cache, _pc_cache_time
        _pc_cache = None  # Invalidate cache
        _pc_cache_time = 0
        # ── PRIMARY: Write to SQLite ──────────────────────────────────
        try:
            from src.core.db import get_db
            with get_db() as conn:
                # Ensure dynamic columns exist (covers DBs created before schema updates)
                for col, default in [
                    ("institution", "''"), ("pc_number", "''"),
                    ("status", "'parsed'"), ("email_uid", "''"),
                    ("email_subject", "''"), ("due_date", "''"),
                    ("pc_data", "'{}'"), ("ship_to", "''"),
                ]:
                    try:
                        conn.execute(f"SELECT {col} FROM price_checks LIMIT 0")
                    except Exception:
                        try:
                            conn.execute(f"ALTER TABLE price_checks ADD COLUMN {col} TEXT DEFAULT {default}")
                        except Exception:
                            pass

                for pc_id, pc in pcs.items():
                    items_json = json.dumps(pc.get("items", []), default=str)
                    # Store full PC blob for lossless round-trip (strip pc_data to prevent recursive nesting)
                    _pc_clean = {k: v for k, v in pc.items() if k != "pc_data"}
                    pc_blob = json.dumps(_pc_clean, default=str)
                    conn.execute("""
                        INSERT OR REPLACE INTO price_checks
                        (id, created_at, requestor, agency, institution, items, source_file,
                         quote_number, pc_number, total_items, status,
                         email_uid, email_subject, due_date, pc_data, ship_to, data_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        pc_blob,
                        pc.get("ship_to", ""),
                        json.dumps(pc, default=str),
                    ))
        except Exception as e:
            log.error("DB save failed for price_checks: %s", e)

        # ── BACKUP: Write JSON cache with data guard ──────────────────
        try:
            import traceback
            from src.core.data_guard import safe_save_json
            caller = traceback.extract_stack()[-2]
            reason = f"{caller.filename.split('/')[-1]}:{caller.lineno}"
            path = os.path.join(DATA_DIR, "price_checks.json")
            safe_save_json(path, pcs, reason=reason)
        except Exception as e:
            log.warning("JSON backup write failed for price_checks: %s", e)


def _merge_save_pc(pc_id: str, pc_data: dict):
    """Atomic single-PC save: writes directly to SQLite.
    DB handles concurrency via WAL mode — no file locking needed."""
    pc_data["id"] = pc_id
    _save_price_checks({pc_id: pc_data})  # Uses DB-primary _save_price_checks


def _is_user_facing_pc(pc: dict) -> bool:
    """Canonical filter: is this PC for the standalone PC queue?
    Auto-price PCs (created from RFQ imports) belong to the RFQ row, not the PC queue.
    Standalone email PCs (Valentina's 704s) DO belong in the PC queue.
    Simplified status model: new, draft, sent, not_responding.
    Only new + draft show in active queue. Sent + not_responding → archive only.
    Used by: home page, manager brief, workflow tester, pipeline summary."""
    if pc.get("source") == "email_auto_draft":
        return False
    if pc.get("is_auto_draft"):
        return False
    if pc.get("rfq_id"):
        return False
    # Terminal / inactive statuses — only in archive
    status = pc.get("status", "new")
    if status in ("dismissed", "archived", "deleted", "duplicate", "no_response", "not_responding"):
        return False
    if status in ("sent", "pending_award", "won", "lost", "expired"):
        return False
    # Parse errors with 0 items — nothing actionable
    if status == "parse_error" and not pc.get("items"):
        return False
    # Ghost PCs: 0 items + draft/new status + no real solicitation
    items = pc.get("items", [])
    if len(items) == 0 and status in ("new", "draft", "parsed", ""):
        sol = pc.get("solicitation_number", "") or pc.get("pc_number", "")
        if not sol or sol == "unknown":
            return False
    return True


# ═══════════════════════════════════════════════════════════════════════
# Email Polling Thread
# ═══════════════════════════════════════════════════════════════════════

_shared_poller = None  # Shared poller instance for manual checks

def _auto_price_new_pc(pc_id: str):
    """Auto-price a newly created PC: catalog match → SCPRS → apply best prices.
    Runs in background thread so email processing isn't blocked."""
    _ap_status = {"pc_id": pc_id, "started": datetime.now().isoformat(), "steps": [], "error": None}
    try:
        _ap_status["steps"].append("loading PC")
        pcs = _load_price_checks()
        pc = pcs.get(pc_id)
        if not pc or not pc.get("items"):
            _ap_status["steps"].append(f"SKIP: PC not found or no items (found={pc_id in pcs}, items={len(pc.get('items',[])) if pc else 0})")
            _save_auto_price_status(pc_id, _ap_status)
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
                        p["catalog_best_supplier"] = best.get("best_supplier", "") or best.get("best_supplier_name", "")
                        if cat_cost > 0:
                            p["unit_cost"] = cat_cost
                            p["last_cost"] = cat_cost
                        p["recommended_price"] = round(cat_price, 2)
                        # Propagate MFG# if item doesn't have one
                        cat_mfg = best.get("mfg_number") or best.get("sku", "")
                        if cat_mfg and not item.get("mfg_number"):
                            item["mfg_number"] = cat_mfg
                        # Propagate supplier URL + name from catalog
                        cat_url = best.get("best_supplier_url", "")
                        cat_supplier = best.get("best_supplier_name", "") or best.get("best_supplier", "")
                        if cat_url and not item.get("item_link"):
                            item["item_link"] = cat_url
                            p["catalog_url"] = cat_url
                        if cat_supplier and not item.get("item_supplier"):
                            item["item_supplier"] = cat_supplier
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
                    # Populate item_link — this is what the UI reads and catalog saves
                    web_url = result.get("url", "")
                    if web_url and not item.get("item_link"):
                        item["item_link"] = web_url
                        # Auto-detect supplier from URL
                        try:
                            from src.agents.item_link_lookup import detect_supplier
                            item["item_supplier"] = detect_supplier(web_url)
                        except Exception:
                            item["item_supplier"] = result.get("source", "Web")
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

        # ── 4. FI$Cal market intelligence enrichment ──
        try:
            from src.agents.quote_intelligence import enrich_extracted_items
            enriched = enrich_extracted_items(items)
            for i, enriched_item in enumerate(enriched):
                if i < len(items) and enriched_item.get("intelligence"):
                    items[i]["intelligence"] = enriched_item["intelligence"]
                    # If no price found yet, use FI$Cal recommendation
                    rec = enriched_item["intelligence"].get("recommendation", {})
                    if rec.get("quote_price") and not items[i].get("pricing", {}).get("recommended_price"):
                        if not items[i].get("pricing"):
                            items[i]["pricing"] = {}
                        items[i]["pricing"]["recommended_price"] = rec["quote_price"]
                        items[i]["pricing"]["price_source"] = f"fiscal_{rec.get('confidence', 'low')}"
                        found_count += 1
            log.info("FI$Cal intelligence: enriched %d items", len(enriched))
        except ImportError:
            log.debug("quote_intelligence not available")
        except Exception as e:
            log.debug("FI$Cal enrichment error: %s", e)

        # ── 5. Unified Pricing Oracle ──
        try:
            from src.core.pricing_oracle_v2 import get_pricing, lock_cost, auto_learn_mapping
            for item in items:
                desc = item.get("description", "")
                if not desc:
                    continue
                oracle = get_pricing(
                    description=desc,
                    quantity=item.get("quantity", item.get("qty", 1)),
                    cost=item.get("pricing", {}).get("unit_cost") or item.get("supplier_cost"),
                    item_number=item.get("item_number", ""),
                )
                if oracle.get("recommendation", {}).get("quote_price"):
                    item["oracle_price"] = oracle["recommendation"]["quote_price"]
                    item["oracle_confidence"] = oracle["recommendation"]["confidence"]
                    item["oracle_rationale"] = oracle["recommendation"]["rationale"]
                    item["oracle_strategies"] = oracle.get("strategies", [])
                    item["oracle_competitors"] = oracle.get("competitors", [])
                    if not item.get("pricing", {}).get("recommended_price"):
                        if not item.get("pricing"):
                            item["pricing"] = {}
                        item["pricing"]["recommended_price"] = oracle["recommendation"]["quote_price"]
                        item["pricing"]["price_source"] = f"oracle_{oracle['recommendation']['confidence']}"
                        found_count += 1
                # Auto-lock cost
                cost_val = item.get("pricing", {}).get("unit_cost") or item.get("supplier_cost")
                if cost_val:
                    try:
                        lock_cost(desc, float(cost_val), source="auto_price", expires_days=30,
                                  item_number=item.get("item_number", ""))
                    except Exception:
                        pass
                # Auto-learn mapping
                if item.get("pricing", {}).get("catalog_match"):
                    auto_learn_mapping(desc, item["pricing"]["catalog_match"],
                                       item_number=item.get("item_number", ""), confidence=0.6)
        except ImportError:
            log.debug("pricing_oracle_v2 not available")
        except Exception as e:
            log.debug("Oracle auto-price: %s", e)

        # ── 6. Save if we found anything ──
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
        log.error("Auto-price PC %s failed: %s", pc_id, e, exc_info=True)
        _ap_status["error"] = f"{type(e).__name__}: {str(e)[:200]}"
        _ap_status["steps"].append(f"CRASH: {e}")
        _save_auto_price_status(pc_id, _ap_status)


def _save_auto_price_status(pc_id: str, status: dict):
    """Save auto-price debug status so it's visible via API."""
    try:
        status_file = os.path.join(DATA_DIR, "auto_price_status.json")
        data = {}
        if os.path.exists(status_file):
            with open(status_file) as f:
                data = json.load(f)
        data[pc_id] = status
        # Keep only last 20
        if len(data) > 20:
            keys = sorted(data.keys(), key=lambda k: data[k].get("started", ""))
            for k in keys[:-20]:
                del data[k]
        with open(status_file, "w") as f:
            json.dump(data, f, indent=2, default=str)
    except Exception:
        pass


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


def _extract_signature_contact(body: str) -> dict:
    """Extract contact info from email body/signature.
    Looks for patterns like: name, title, phone, address, institution."""
    import re
    if not body:
        return {}
    
    result = {}
    lines = body.strip().split("\n")
    
    # Look for phone numbers
    phone_pat = re.compile(r'(?:Phone|Tel|Ph|Cell|Mobile|Office|Direct)[\s:]*([0-9().\-\s]{10,20})', re.IGNORECASE)
    phone_match = phone_pat.search(body)
    if phone_match:
        result["phone"] = phone_match.group(1).strip()
    elif not result.get("phone"):
        # Bare phone number pattern
        bare_phone = re.search(r'\b(\d{3}[.\-]\d{3}[.\-]\d{4})\b', body)
        if bare_phone:
            result["phone"] = bare_phone.group(1)
    
    # Look for .gov email addresses (prioritize over sender)
    gov_email = re.search(r'[\w.+-]+@[\w.-]*\.gov\b', body, re.IGNORECASE)
    if gov_email:
        result["email"] = gov_email.group(0).lower()
    
    # Look for street address patterns (must start at beginning of line)
    addr_pat = re.compile(r'(?:^|\n)\s*(\d{2,5}\s+[\w\s.]+(?:Road|Rd|Street|St|Avenue|Ave|Drive|Dr|Way|Blvd|Boulevard|Pkwy|Parkway|Lane|Ln|Court|Ct)\.?)', re.IGNORECASE)
    addr_match = addr_pat.search(body)
    if addr_match:
        result["address"] = addr_match.group(1).strip()
        # Try to get city/state/zip from next line or nearby
        addr_pos = addr_match.end()
        after = body[addr_pos:addr_pos+100]
        csz = re.search(r'[\n,]\s*([A-Za-z\s.]+,?\s*(?:CA|California)\.?\s*\d{5})', after)
        if csz:
            result["address"] += "\n" + csz.group(1).strip()
    
    # Look for known California agency patterns
    agency_patterns = [
        (r'(?:Veterans?\s+Home\s+of\s+California|CalVet|CALVET)[\s,\-:]*(\w[\w\s]*?)(?:\n|$)', "calvet"),
        (r'(?:CDCR|Corrections?\s+and\s+Rehabilitation)', "cdcr"),
        (r'(?:Department\s+of\s+General\s+Services|DGS)', "dgs"),
    ]
    for pat, agency in agency_patterns:
        m = re.search(pat, body, re.IGNORECASE)
        if m:
            result["agency"] = agency
            if m.lastindex and m.group(1):
                location = m.group(1).strip().rstrip(",.- ")
                if location and len(location) > 2:
                    result["institution"] = f"CalVet - {location}" if agency == "calvet" else location
            break
    
    # Look for city names that map to known facilities
    _calvet_cities = ["Redding", "Yountville", "Barstow", "Chula Vista", "Fresno", "West Los Angeles", "Ventura"]
    for city in _calvet_cities:
        if city.lower() in body.lower():
            if not result.get("institution"):
                result["institution"] = f"Veterans Home of California - {city}"
            result["city"] = city
            break
    
    # Look for name (first non-empty line that looks like a name, near end of email)
    # Focus on lines after "---" or signature markers
    sig_start = body
    for marker in ["--", "___", "Best regards", "Regards", "Sincerely", "Thank you", "Thanks"]:
        idx = body.rfind(marker)
        if idx > 0:
            sig_start = body[idx:]
            break
    
    name_pat = re.compile(r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*$', re.MULTILINE)
    name_match = name_pat.search(sig_start)
    if name_match:
        candidate = name_match.group(1).strip()
        # Filter common non-name lines
        skip = ["United States", "Best Regards", "Thank You", "Please Note", "Office Technician"]
        if not any(s.lower() == candidate.lower() for s in skip):
            result["name"] = candidate
    
    return result


def _get_pc_items(pc):
    """Get items from a PC regardless of storage format."""
    items = pc.get("items", [])
    if items and isinstance(items, list) and len(items) > 0:
        return items
    pc_data = pc.get("pc_data", {})
    if isinstance(pc_data, str):
        try:
            pc_data = json.loads(pc_data)
        except (json.JSONDecodeError, TypeError):
            pc_data = {}
    if isinstance(pc_data, dict):
        items = pc_data.get("items", [])
        if items:
            return items
    return pc.get("line_items", [])


def _fuzzy_item_overlap(rfq_items, pc_items, threshold=0.6):
    """Count items with >=60% description similarity."""
    from difflib import SequenceMatcher
    matches = 0
    matched_pc_indices = set()
    for rfq_item in rfq_items:
        rfq_desc = (rfq_item.get("description", "") or "").lower().strip()
        if not rfq_desc or len(rfq_desc) < 5:
            continue
        best_sim = 0
        best_idx = -1
        for idx, pc_item in enumerate(pc_items):
            if idx in matched_pc_indices:
                continue
            pc_desc = (pc_item.get("description", pc_item.get("desc", "")) or "").lower().strip()
            if not pc_desc or len(pc_desc) < 5:
                continue
            rfq_words = rfq_desc.split()[:3]
            pc_words = pc_desc.split()[:3]
            if rfq_words == pc_words:
                best_sim = 0.9
                best_idx = idx
                break
            sim = SequenceMatcher(None, rfq_desc[:80], pc_desc[:80]).ratio()
            if sim > best_sim:
                best_sim = sim
                best_idx = idx
        if best_sim >= threshold and best_idx >= 0:
            matches += 1
            matched_pc_indices.add(best_idx)
    return matches


def _link_rfq_to_pc(rfq_data, _trace):
    """F1: Auto-link new RFQ to matching Price Check, port pricing, detect diffs.

    Matching: sol# (exact) > requestor+fuzzy items > agency+fuzzy items.
    Ports: cost, bid, SCPRS, Amazon, item_link, MFG#, supplier.
    Records audit trail for every ported price.
    Marks PC as converted (NEVER deleted — Law 22).
    Returns True if linked."""
    try:
        pcs = _load_price_checks()
    except Exception:
        return False
    if not pcs:
        return False

    sol = (rfq_data.get("solicitation_number") or "").strip()
    rfq_items_list = rfq_data.get("line_items", rfq_data.get("items", []))

    # Collect all RFQ identifiers for requestor matching
    rfq_identifiers = set()
    for field in ["requestor_email", "requestor_name", "email_sender", "requestor"]:
        val = (rfq_data.get(field, "") or "").lower().strip()
        if val and len(val) >= 3:
            rfq_identifiers.add(val)
            if "@" in val:
                rfq_identifiers.add(val.split("@")[0])

    matched_pid = None
    match_reason = ""

    for pid, pc in pcs.items():
        if pc.get("status") in ("dismissed", "cancelled"):
            continue
        pc_sol = (pc.get("pc_number", "") or "").replace("AD-", "").strip()
        pc_items_list = _get_pc_items(pc)

        # Match 1: Same solicitation number
        if sol and sol != "unknown" and pc_sol == sol:
            matched_pid, match_reason = pid, f"sol#{sol}"
            break

        # Match 2: Same requestor + >=50% fuzzy item overlap
        pc_identifiers = set()
        for field in ["requestor", "requestor_email", "requestor_name", "email"]:
            val = (pc.get(field, "") or "").lower().strip()
            if val and len(val) >= 3:
                pc_identifiers.add(val)
                if "@" in val:
                    pc_identifiers.add(val.split("@")[0])
        requestor_match = bool(pc_identifiers & rfq_identifiers)
        if not requestor_match:
            for pc_id in pc_identifiers:
                for rfq_id in rfq_identifiers:
                    if len(pc_id) >= 3 and len(rfq_id) >= 3:
                        if pc_id in rfq_id or rfq_id in pc_id:
                            requestor_match = True
                            break
                if requestor_match:
                    break
        if requestor_match and rfq_items_list and pc_items_list:
            overlap = _fuzzy_item_overlap(rfq_items_list, pc_items_list)
            if overlap >= max(1, len(pc_items_list) * 0.5):
                matched_pid, match_reason = pid, f"requestor+{overlap}/{len(pc_items_list)}_items"
                break

        # Match 3: Same agency/institution + >=80% fuzzy item overlap
        if pc_items_list and rfq_items_list:
            pc_inst = (pc.get("institution", "") or "").strip()
            rfq_inst = (rfq_data.get("delivery_location", "") or rfq_data.get("agency_name", "") or "").strip()
            _inst_match = False
            if pc_inst and rfq_inst and len(pc_inst) >= 3 and len(rfq_inst) >= 3:
                try:
                    from src.core.institution_resolver import same_institution
                    _inst_match = same_institution(pc_inst, rfq_inst)
                except ImportError:
                    _inst_match = pc_inst.lower() in rfq_inst.lower() or rfq_inst.lower() in pc_inst.lower()
            if _inst_match:
                overlap = _fuzzy_item_overlap(rfq_items_list, pc_items_list)
                if overlap >= max(1, len(pc_items_list) * 0.8):
                    matched_pid, match_reason = pid, f"agency+{overlap}/{len(pc_items_list)}_items"
                    break

    if not matched_pid:
        return False

    pc = pcs[matched_pid]
    rfq_data["linked_pc_id"] = matched_pid
    rfq_data["linked_pc_number"] = pc.get("pc_number", "")
    rfq_data["linked_pc_match_reason"] = match_reason
    _trace.append(f"PC LINKED: {matched_pid} ({match_reason})")
    log.info("Auto-linked RFQ %s → PC %s (%s)", rfq_data["id"], matched_pid, match_reason)

    # ── Port ALL fields from PC items to RFQ items (full transfer, not cherry-pick) ──
    pc_items = _get_pc_items(pc)
    ported = 0
    diff_added, diff_removed, diff_qty = [], [], []

    for rfq_item in rfq_data.get("line_items", []):
        from difflib import SequenceMatcher as _SM
        rd = (rfq_item.get("description", "") or "").lower().strip()
        match = None
        best_sim = 0
        for pci in pc_items:
            pd = (pci.get("description", pci.get("desc", "")) or "").lower().strip()
            if not pd or len(pd) < 5:
                continue
            if rd == pd:
                match = pci
                break
            sim = _SM(None, rd[:80], pd[:80]).ratio()
            if sim > best_sim and sim >= 0.6:
                best_sim = sim
                match = pci

        if not match:
            diff_added.append(rfq_item.get("description", "")[:50])
            continue

        desc = rfq_item.get("description", "")

        # ── Port PC fields → RFQ fields with proper name mapping ──────────
        # PC and RFQ use different field names for the same data.
        # This mapping ensures nothing is lost in translation.
        _pricing = match.get("pricing", {}) or {}

        # Cost: PC uses vendor_cost or pricing.unit_cost → RFQ uses supplier_cost
        _pc_cost = (match.get("vendor_cost")
                    or _pricing.get("unit_cost")
                    or match.get("cost")
                    or match.get("unit_cost")
                    or match.get("unit_price") or 0)
        if _pc_cost and not rfq_item.get("supplier_cost"):
            rfq_item["supplier_cost"] = _pc_cost

        # Bid price: PC uses unit_price or pricing.recommended_price → RFQ uses price_per_unit
        _pc_price = (match.get("unit_price")
                     or _pricing.get("recommended_price")
                     or match.get("bid_price")
                     or match.get("sell_price") or 0)
        if _pc_price and not rfq_item.get("price_per_unit"):
            rfq_item["price_per_unit"] = _pc_price

        # Markup: same name in both
        _pc_markup = match.get("markup_pct") or _pricing.get("markup_pct")
        if _pc_markup and not rfq_item.get("markup_pct"):
            rfq_item["markup_pct"] = _pc_markup

        # MFG/Part number: PC uses mfg_number → RFQ uses item_number
        _pc_mfg = (match.get("mfg_number")
                   or match.get("part_number")
                   or match.get("item_number") or "")
        if _pc_mfg and not rfq_item.get("item_number"):
            rfq_item["item_number"] = _pc_mfg

        # URL: both use item_link but PC may also have url/product_url/amazon_url
        _pc_link = (match.get("item_link")
                    or match.get("url")
                    or match.get("product_url")
                    or match.get("amazon_url") or "")
        if _pc_link and not rfq_item.get("item_link"):
            rfq_item["item_link"] = _pc_link

        # Supplier name
        _pc_supplier = match.get("item_supplier") or match.get("supplier") or ""
        if _pc_supplier and not rfq_item.get("item_supplier"):
            rfq_item["item_supplier"] = _pc_supplier

        # Description: only fill if RFQ has none
        _pc_desc = match.get("description") or match.get("desc") or ""
        if _pc_desc and not rfq_item.get("description"):
            rfq_item["description"] = _pc_desc

        # Qty/UOM: keep RFQ's values (from the formal doc), don't overwrite
        if not rfq_item.get("qty") and match.get("qty"):
            rfq_item["qty"] = match["qty"]
        if not rfq_item.get("uom") and match.get("uom"):
            rfq_item["uom"] = match["uom"]

        # SCPRS pricing intelligence
        _scprs = _pricing.get("scprs_price")
        if _scprs and not rfq_item.get("scprs_last_price"):
            rfq_item["scprs_last_price"] = _scprs
            if _pricing.get("scprs_po"):
                rfq_item["scprs_po"] = _pricing["scprs_po"]
            if _pricing.get("scprs_match"):
                rfq_item["scprs_vendor"] = _pricing["scprs_match"]

        # Copy any remaining PC fields that RFQ doesn't have at all
        for key, val in match.items():
            if key not in rfq_item and val and key != "pricing":
                rfq_item[key] = val

        # Tag the source
        rfq_item["source_pc"] = matched_pid
        rfq_item["imported_from_pc"] = True
        rfq_item["imported_at"] = datetime.now(timezone.utc).isoformat()
        rfq_item["_from_pc"] = pc.get("pc_number", "")
        ported += 1

        # Detect qty changes
        pc_qty = match.get("qty", 0) or 0
        rfq_qty = rfq_item.get("qty", 0) or 0
        if pc_qty and rfq_qty and pc_qty != rfq_qty:
            diff_qty.append({"desc": desc[:50], "pc": pc_qty, "rfq": rfq_qty})

        # Check for PO screenshots
        try:
            po_num = pc.get("po_number", "")
            if po_num:
                for ext in [".png", ".html"]:
                    po_path = os.path.join(DATA_DIR, "po_records", f"{po_num}{ext}")
                    if os.path.exists(po_path):
                        rfq_item["po_screenshot"] = po_path
        except Exception:
            pass

    # Items in PC but not in RFQ (fuzzy)
    for pci in pc_items:
        pd = (pci.get("description", pci.get("desc", "")) or "").lower().strip()
        if not pd or len(pd) < 5:
            continue
        found = False
        for ri in rfq_data.get("line_items", []):
            rd = (ri.get("description", "") or "").lower().strip()
            if rd and _SM(None, pd[:80], rd[:80]).ratio() >= 0.6:
                found = True
                break
        if not found:
            diff_removed.append(pci.get("description", pci.get("desc", ""))[:50])

    # Copy PC-level metadata to the RFQ
    rfq_data["source_pc"] = matched_pid
    rfq_data["source_pc_number"] = pc.get("pc_number", "")
    rfq_data["source_pc_status"] = pc.get("status", "")
    rfq_data["source_pc_requestor"] = pc.get("requestor", "")

    # Port PC-level fields the RFQ needs for quoting
    if not rfq_data.get("delivery_location") and pc.get("ship_to"):
        rfq_data["delivery_location"] = pc["ship_to"]
    if not rfq_data.get("tax_rate") and pc.get("tax_rate"):
        rfq_data["tax_rate"] = pc["tax_rate"]
        rfq_data["tax_source"] = pc.get("tax_source", "ported_from_pc")
        rfq_data["tax_validated"] = True
    if not rfq_data.get("shipping_option") and pc.get("delivery_option"):
        rfq_data["shipping_option"] = pc["delivery_option"]
    if not rfq_data.get("quote_notes") and pc.get("custom_notes"):
        rfq_data["quote_notes"] = pc["custom_notes"]

    # Copy source PDF if it exists
    source_file = pc.get("source_file", "")
    if source_file and os.path.exists(source_file):
        rfq_data["source_file"] = source_file
        rfq_data["pc_pdf_path"] = source_file

    # Store diff
    rfq_data["pc_diff"] = {
        "ported": ported,
        "added": diff_added,
        "removed": diff_removed,
        "qty_changed": diff_qty,
    }

    _trace.append(f"PORTED {ported} prices, diff: +{len(diff_added)} -{len(diff_removed)} Δ{len(diff_qty)}")
    log.info("Ported %d item prices PC %s → RFQ %s (+%d/-%d/Δ%d)",
             ported, pc.get("pc_number", ""), rfq_data["id"],
             len(diff_added), len(diff_removed), len(diff_qty))

    # Mark PC as converted (don't delete — pricing history is valuable)
    pc["converted_to_rfq"] = True
    pc["linked_rfq_id"] = rfq_data["id"]
    pc["linked_rfq_at"] = datetime.now().isoformat()
    _save_price_checks(pcs)

    return True


def _check_delivery_status(email_data, track_result):
    """Detect delivery confirmation emails and update order items to 'delivered'."""
    import re as _re
    combined = f"{email_data.get('subject', '')} {email_data.get('body', '')}".lower()
    
    delivery_keywords = ["delivered", "has been delivered", "delivery complete",
                         "package delivered", "your package was delivered",
                         "left at", "signed by", "delivered to"]
    is_delivered = any(kw in combined for kw in delivery_keywords) or track_result.get("is_delivery_confirmation", False)
    
    if not is_delivered:
        return
    
    matched_orders = track_result.get("matched_orders", [])
    tracking_numbers = [t["number"] for t in track_result.get("tracking_numbers", [])]
    
    # Also try to match by tracking number in existing orders
    if not matched_orders and tracking_numbers:
        try:
            orders_path = os.path.join(DATA_DIR, "orders.json")
            with open(orders_path) as f:
                orders = json.load(f)
            for oid, order in orders.items():
                if order.get("status") in ("cancelled", "deleted", "closed"):
                    continue
                for it in order.get("line_items", []):
                    if it.get("tracking_number") in tracking_numbers:
                        if oid not in matched_orders:
                            matched_orders.append(oid)
        except Exception:
            pass
    
    if not matched_orders:
        return
    
    # Update matched items from shipped → delivered
    try:
        orders_path = os.path.join(DATA_DIR, "orders.json")
        with open(orders_path) as f:
            orders = json.load(f)
        
        updated = 0
        for oid in matched_orders:
            order = orders.get(oid)
            if not order:
                continue
            for it in order.get("line_items", []):
                # Mark as delivered if shipped AND has matching tracking
                if it.get("sourcing_status") == "shipped":
                    tn = it.get("tracking_number", "")
                    if tn in tracking_numbers or not tracking_numbers:
                        it["sourcing_status"] = "delivered"
                        it["delivered_at"] = datetime.now().isoformat()
                        updated += 1
            
            # Update order-level status if all items delivered
            all_delivered = all(
                i.get("sourcing_status") == "delivered"
                for i in order.get("line_items", [])
            )
            if all_delivered and order.get("line_items"):
                order["status"] = "delivered"
            elif updated > 0:
                order["status"] = "partial_delivery"
            
            order["updated_at"] = datetime.now().isoformat()
            orders[oid] = order
        
        if updated > 0:
            with open(orders_path, "w") as f:
                json.dump(orders, f, indent=2, default=str)
            log.info("Delivery detected: %d items marked delivered across %d orders", 
                     updated, len(matched_orders))
    except Exception as e:
        log.error("Delivery status update failed: %s", e)


def _extract_solicitation(text):
    """Extract solicitation/RFQ number from text (email subject, body, PDF text).

    Targets CDCR/CalVet patterns: PR 10837814, PREQ 10840485, Solicitation #25-067MC,
    Request for Quote: 10840878, RFQ SAC 10840487, etc.
    """
    import re as _re
    _text = str(text)

    # Garbage filter — reject common false positives
    _garbage = {"response", "number", "quote", "request", "bid", "vendor",
                "price", "check", "form", "item", "unit", "total", "date",
                "name", "email", "phone", "fax", "attachment", "page"}

    patterns = [
        # PR/PREQ + number (CDCR standard)
        r'(?:PR|PREQ|P\.?R\.?)\s*#?\s*(\d{7,})',
        # Solicitation/RFQ/Bid + number
        r'(?:solicitation|sol)\s*[#:\s]+(\d{5,}[\w\-]*)',
        r'(?:rfq|bid|ifb|rfi)\s+(?:[A-Z]{2,4}\s+)?(\d{5,}[\w\-]*)',
        # "Request for Quote/Bid: NUMBER" or "Request for Bid- NUMBER"
        r'(?:request\s+for\s+(?:quot(?:e|ation)|bid))\s*[:\-]\s*(\d{5,}[\w\-]*)',
        # Solicitation #XX-XXXXX format
        r'(?:solicitation|sol|rfq|bid)\s*[#:\s]+([A-Z0-9]{2,4}[\-/]\d{2,}[\w\-]*)',
        # Ref/Requisition + number
        r'(?:ref|requisition|req)\s+(?:requisition\s+)?(\d{2,4}[\-/]\d{2,4}[\w\-]*)',
        # Bare #NUMBER with 5+ digits
        r'#\s*(\d{5,}[\w\-]*)',
        # Standalone 7+ digit number (CDCR PR numbers)
        r'\b(\d{7,8})\b',
    ]
    for p in patterns:
        m = _re.search(p, _text, _re.IGNORECASE)
        if m:
            val = m.group(1).strip().rstrip("_.- ")
            # Filter garbage
            if val.lower() in _garbage:
                continue
            if "_____" in val or len(val) < 2:
                continue
            return val
    return ""


def _extract_due_date(text):
    """Extract due date from text (email subject, body, PDF text).
    Normalizes all dates to MM/DD/YYYY format."""
    import re as _re
    from datetime import datetime as _dt
    patterns = [
        r'(?:due|deadline|respond by|response due|close[sd]?)\s*[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})',
        r'(?:due|deadline)\s*[:\s]+(\w+ \d{1,2},?\s*\d{4})',
        r'DUE\s+(\d{1,2}/\d{1,2}/\d{2,4})',
        r'(\d{1,2}/\d{1,2}/\d{2,4})\s*(?:at\s+\d|by\s+\d)',
    ]
    for p in patterns:
        m = _re.search(p, str(text), _re.IGNORECASE)
        if m:
            date_str = m.group(1).strip()
            for fmt in ["%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y",
                        "%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y"]:
                try:
                    return _dt.strptime(date_str, fmt).strftime("%m/%d/%Y")
                except ValueError:
                    continue
            return date_str
    return ""


def _normalize_date(date_str):
    """Normalize a date string to MM/DD/YYYY. Used for dates from PDF form fields."""
    if not date_str:
        return date_str
    from datetime import datetime as _dt
    for fmt in ["%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y",
                "%Y-%m-%d", "%B %d, %Y", "%B %d %Y", "%b %d, %Y"]:
        try:
            return _dt.strptime(str(date_str).strip(), fmt).strftime("%m/%d/%Y")
        except (ValueError, TypeError):
            continue
    return str(date_str).strip()


def process_rfq_email(rfq_email):
    """Process a single RFQ email into the queue. Returns rfq_data or None.
    Deduplicates by checking email_uid against existing RFQs.
    PRD Feature 4.2: After parsing, auto-triggers price check + draft quote generation.
    """
    if isinstance(rfq_email, str):
        log.error("process_rfq_email got string instead of dict: %s", rfq_email[:100])
        return None
    if not isinstance(rfq_email, dict):
        log.error("process_rfq_email got %s instead of dict", type(rfq_email).__name__)
        return None
    # Ensure attachments are dicts not strings
    atts = rfq_email.get("attachments", [])
    if atts and isinstance(atts, list):
        rfq_email["attachments"] = [a for a in atts if isinstance(a, dict)]
    _trace = []  # Legacy trace for poll_diag compatibility
    _subj = rfq_email.get("subject", "?")[:50]
    _trace.append(f"START: {_subj}")
    t = Trace("email_pipeline", subject=_subj, email_uid=rfq_email.get("email_uid", "?"))
    
    # Dedup: check if this email UID is already in the queue
    rfqs = load_rfqs()
    _incoming_uid = rfq_email.get("email_uid", "")
    if _incoming_uid:  # Only dedup if we have a real UID
        for _eid, existing in rfqs.items():
            if existing.get("email_uid") == _incoming_uid:
                _trace.append(f"SKIP: duplicate email_uid {_incoming_uid} matches existing RFQ {_eid} (sol={existing.get('solicitation_number','?')})")
                log.info("Skipping duplicate email UID %s: matches RFQ %s (sol=%s)",
                         _incoming_uid, _eid, existing.get("solicitation_number", "?"))
                POLL_STATUS.setdefault("_email_traces", []).append(_trace)
                t.ok("Skipped: duplicate email_uid in RFQ queue", existing_id=_eid)
                return None

    # Dedup layer 2: check solicitation number + sender
    _incoming_sender = (rfq_email.get("sender", "") or "").lower()
    _incoming_subject = rfq_email.get("subject", "") or ""
    _incoming_sol = ""
    # Extract solicitation from subject
    import re as _dedup_re
    _sol_match = _dedup_re.search(r'(?:solicitation|rfq|sol)\s*#?\s*(\d+)', _incoming_subject, _dedup_re.IGNORECASE)
    if _sol_match:
        _incoming_sol = _sol_match.group(1)

    if _incoming_sol and _incoming_sender:
        for _eid, existing in rfqs.items():
            _ex_sol = existing.get("solicitation_number", "") or existing.get("rfq_number", "")
            _ex_email = (existing.get("requestor_email", "") or "").lower()
            if _ex_sol == _incoming_sol and _incoming_sender and _incoming_sender in _ex_email:
                log.info("Skipping duplicate: sol=%s from %s already exists as RFQ %s",
                         _incoming_sol, _incoming_sender, _eid)
                t.ok("Skipped: duplicate solicitation+sender", existing_id=_eid)
                return None

    # ── Route 704 price checks to PC queue, NOT the RFQ queue ──────────────
    attachments = rfq_email.get("attachments", [])
    pdf_paths = [a["path"] for a in attachments if a.get("path") and a["path"].lower().endswith(".pdf")]
    _trace.append(f"PDFs: {len(pdf_paths)} paths, PRICE_CHECK_AVAILABLE={PRICE_CHECK_AVAILABLE}")
    
    # Early PC detection flag from poller (known sender + subject patterns)
    is_early_pc = rfq_email.get("_pc_early_detect", False)
    if is_early_pc:
        _trace.append(f"EARLY PC DETECT: signals={rfq_email.get('_pc_signals', [])}")
    
    # ── SECURE MESSAGE DETECTION — create stub so email isn't silently dropped ──
    _all_attachments = rfq_email.get("attachments", [])
    _has_secure_msg = any(
        "securemessage" in (a.get("filename", "") or a.get("name", "") or a.get("path", "") or "").lower()
        or (a.get("filename", "") or a.get("name", "") or "").lower().startswith("secure")
        for a in _all_attachments
    )
    _has_real_pdf = bool(pdf_paths)
    if _has_secure_msg and not _has_real_pdf:
        import uuid as _uuid2
        _stub_id = f"rfq_{str(_uuid2.uuid4())[:8]}"
        _sol = rfq_email.get("solicitation_hint", "") or extract_solicitation_number(
            rfq_email.get("subject", ""), rfq_email.get("body_text", ""), []
        )
        _stub = {
            "id": _stub_id,
            "solicitation_number": _sol or "",
            "rfq_number": _sol or "",
            "email_subject": rfq_email.get("subject", ""),
            "requestor_email": rfq_email.get("sender_email", ""),
            "requestor_name": rfq_email.get("sender", ""),
            "agency_name": rfq_email.get("sender", ""),
            "status": "needs_attachment",
            "parse_note": "⚠️ Secure portal email — PDF must be downloaded manually from the secure link in the original email, then uploaded here via Upload & Parse.",
            "line_items": [],
            "created_at": datetime.now().isoformat(),
            "email_uid": rfq_email.get("email_uid", ""),
            "form_type": "generic_rfq",
            "body_preview": rfq_email.get("body_preview", ""),
        }
        _rfq_store = load_rfqs()
        _uid = rfq_email.get("email_uid", "")
        if _uid and any(v.get("email_uid") == _uid for v in _rfq_store.values()):
            _trace.append(f"SKIP: secure stub already exists for uid={_uid}")
            t.ok("Skipped: secure stub already exists")
            return None
        _rfq_store[_stub_id] = _stub
        save_rfqs(_rfq_store)
        log.info("📧 Secure-message stub created: %s sol=%s from %s",
                 _stub_id, _sol, rfq_email.get("sender_email", ""))
        _trace.append(f"SECURE STUB: {_stub_id} sol={_sol}")
        POLL_STATUS.setdefault("_email_traces", []).append(_trace)
        t.ok("Secure-message stub created", stub_id=_stub_id)
        return _stub

    # ── ROUTING RULE: 1 PDF = Price Check, multiple PDFs = RFQ ──────────────
    # Single 704 form → PC (market pricing research)
    # Multiple PDFs (704B + 703B + bid package) → RFQ (formal bid)
    is_single_pdf = len(pdf_paths) == 1
    
    if (is_single_pdf or is_early_pc) and pdf_paths and PRICE_CHECK_AVAILABLE:
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
                if email_uid:
                    existing_match = None
                    existing_match_id = None
                    for pid, p in existing_pcs.items():
                        if p.get("email_uid") == email_uid:
                            existing_match = p
                            existing_match_id = pid
                            break
                    if existing_match:
                        # Allow re-processing if the existing PC was a failed parse
                        if existing_match.get("status") == "parse_error" and not existing_match.get("items"):
                            log.info("Replacing parse_error PC %s (0 items) with fresh parse", existing_match_id)
                            del existing_pcs[existing_match_id]
                            _save_price_checks(existing_pcs)
                            _trace.append(f"Replaced stale parse_error PC: {existing_match_id}")
                        else:
                            _trace.append(f"SKIP: duplicate email_uid in PC queue (existing={existing_match_id}, status={existing_match.get('status')}, items={len(existing_match.get('items', []))})")
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
                        _file_size = os.path.getsize(pc_file)
                        _trace.append(f"PDF copied: {os.path.basename(pc_file)} ({_file_size} bytes)")
                        log.info("PC parse: %s (%d bytes)", os.path.basename(pc_file), _file_size)
                        
                        parsed = parse_ams704(pc_file)
                        parse_error = parsed.get("error")
                        _item_count = len(parsed.get("line_items", []))
                        _method = parsed.get("parse_method", "?")
                        _trace.append(f"parse_ams704: method={_method} items={_item_count} error={parse_error}")
                        log.info("PC parse result: method=%s items=%d error=%s", _method, _item_count, parse_error)
                        
                        if parse_error:
                            # Still create minimal PC so email isn't lost
                            _trace.append(f"parse_ams704 error: {parse_error} — creating minimal PC")
                            _pc_status = "new" if is_early_pc else "parse_error"
                            pcs = _load_price_checks()
                            pcs[pc_id] = {
                                "id": pc_id,
                                "pc_number": rfq_email.get("solicitation_hint", "") or os.path.basename(pc_pdf).replace(".pdf","")[:40],
                                "institution": "", "due_date": "",
                                "requestor": rfq_email.get("sender_email", rfq_email.get("sender", "")),
                                "requestor_email": rfq_email.get("sender_email", ""),
                                "ship_to": "", "items": [], "source_pdf": pc_file,
                                "status": _pc_status,
                                "parse_note": f"Non-704 form — add items manually" if is_early_pc else "",
                                "parse_error": parse_error,
                                "email_uid": rfq_email.get("email_uid", ""),
                                "email_subject": rfq_email.get("subject", ""),
                                "created_at": datetime.now().isoformat(),
                                "reytech_quote_number": "", "linked_quote_number": "",
                            }
                            _save_price_checks(pcs)
                            # Persist source PDF to DB so it survives redeploys
                            try:
                                if pc_file and os.path.exists(pc_file):
                                    with open(pc_file, "rb") as _pdf_f:
                                        _pdf_bytes = _pdf_f.read()
                                    save_rfq_file(pc_id, os.path.basename(pc_file),
                                                  "application/pdf", _pdf_bytes,
                                                  category="source", uploaded_by="email_poller")
                            except Exception as _db_e:
                                log.debug("PC %s: DB PDF save failed: %s", pc_id, _db_e)
                            result = {"ok": True, "pc_id": pc_id, "parse_error": parse_error}
                        else:
                            items = parsed.get("line_items", [])
                            header = parsed.get("header", {})
                            # Fallback: if no items from PDF, try email body
                            if not items:
                                try:
                                    from src.forms.price_check import parse_items_from_email_body
                                    _body = rfq_email.get("body", "")
                                    if _body and len(_body) > 100:
                                        _bp = parse_items_from_email_body(_body)
                                        if _bp.get("line_items"):
                                            items = _bp["line_items"]
                                            parsed["line_items"] = items
                                            parsed["parse_method"] = "email_body"
                                            for _k, _v in _bp.get("header", {}).items():
                                                if _v and not header.get(_k):
                                                    header[_k] = _v
                                            _trace.append(f"EMAIL BODY: parsed {len(items)} items (no PDF items)")
                                            log.info("PC %s: parsed %d items from email body", pc_id, len(items))
                                except Exception as _ebody_e:
                                    _trace.append(f"Email body parse failed: {_ebody_e}")
                                    log.debug("Email body parse: %s", _ebody_e)
                            pc_num = header.get("price_check_number", "") or ""
                            institution = header.get("institution", "")
                            due_date = header.get("due_date", "")
                            # Fallback: derive pc_number from email subject if PDF didn't yield one
                            if not pc_num or pc_num == "unknown":
                                import re as _pcre2
                                _subj_raw = rfq_email.get("subject", "")
                                _stripped = _pcre2.sub(
                                    r'^(?:price\s+)?quote\s+request\s*[-\u2013]\s*|^ams\s*704\s*[-\u2013]\s*|^price\s+check\s*[-\u2013]\s*',
                                    '', _subj_raw, flags=_pcre2.IGNORECASE
                                ).strip()
                                _stripped = _pcre2.sub(r'\s*[-\u2013]\s*\d{2}\.\d{2}\.\d{2,4}', '', _stripped).strip()
                                pc_num = _stripped or os.path.basename(pc_pdf).replace(".pdf", "")[:40] or "unknown"
                                _trace.append(f"pc_number fallback from subject: '{pc_num}'")
                            
                            # Dedup: same PC# + institution + due_date
                            pcs = _load_price_checks()
                            dup_id = None
                            for eid, epc in pcs.items():
                                if (epc.get("pc_number","").strip() == pc_num.strip()
                                        and epc.get("institution","").strip().lower() == institution.strip().lower()
                                        and epc.get("due_date","").strip() == due_date.strip()
                                        and pc_num not in ("unknown", "")):
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
                                    "requestor_email": rfq_email.get("sender_email", ""),
                                    "requestor_name": rfq_email.get("sender_name", rfq_email.get("sender_email", "")),
                                    "ship_to": header.get("ship_to", ""),
                                    "items": items, "source_pdf": pc_file,
                                    "status": "parsed" if items else "new",
                                    "parsed": parsed,
                                    "email_uid": rfq_email.get("email_uid", ""),
                                    "email_subject": rfq_email.get("subject", ""),
                                    "created_at": datetime.now().isoformat(),
                                    "source": "email_auto",
                                    "reytech_quote_number": "",
                                    "linked_quote_number": "",
                                }
                                # Detect multi-page PDFs that may need splitting
                                if len(items) > 8:
                                    pcs[pc_id]["_split_hint"] = {
                                        "total_items": len(items),
                                        "source_pages": (len(items) + 7) // 8,
                                        "suggested_splits": [],
                                    }
                                    for _si, _sitem in enumerate(items):
                                        _item_num = _sitem.get("item_number", "")
                                        try:
                                            if int(_item_num) == 1 and _si > 0:
                                                pcs[pc_id]["_split_hint"]["suggested_splits"].append(_si)
                                        except (ValueError, TypeError):
                                            pass
                                    log.info("PC %s: split hint — %d items, splits at %s",
                                             pc_id, len(items), pcs[pc_id]["_split_hint"]["suggested_splits"])
                                _save_price_checks(pcs)
                                # Persist source PDF to DB so it survives redeploys
                                try:
                                    if pc_file and os.path.exists(pc_file):
                                        with open(pc_file, "rb") as _pdf_f:
                                            _pdf_bytes = _pdf_f.read()
                                        save_rfq_file(pc_id, os.path.basename(pc_file),
                                                      "application/pdf", _pdf_bytes,
                                                      category="source", uploaded_by="email_poller")
                                except Exception as _db_e:
                                    log.debug("PC %s: DB PDF save failed: %s", pc_id, _db_e)
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
                        try:
                            from src.core.dal import log_lifecycle_event as _lle_pc
                            _lle_pc("pc", pc_id, "email_received",
                                f"From {rfq_email.get('sender_email', '?')}: {rfq_email.get('subject', '?')[:60]}",
                                actor="system", detail={"sender": rfq_email.get("sender_email", ""), "subject": rfq_email.get("subject", "")})
                            _lle_pc("pc", pc_id, "email_parsed",
                                f"Parsed {len(items)} items",
                                actor="system", detail={"item_count": len(items)})
                        except Exception:
                            pass
                        POLL_STATUS.setdefault("_email_traces", []).append(_trace)
                        t.ok("PC created", pc_id=pc_id, pc_number=pcs[pc_id].get("pc_number","?"))
                        return None
                    else:
                        # PC wasn't saved — if early detect, force-create a minimal PC
                        if is_early_pc:
                            _trace.append("PC NOT in storage but early-detect → force-creating minimal PC")
                            # Extract metadata from email body
                            _body = rfq_email.get("body_text", "") or rfq_email.get("body_preview", "")
                            _sender_name = (rfq_email.get("sender", "").split("<")[0].strip()
                                           .replace('"','').strip() or
                                           rfq_email.get("sender_email", ""))
                            _institution = ""
                            _due_date = ""
                            # Try to extract institution from body
                            import re as _re_pc
                            for _inst_pat in [
                                r'(?:california|ca)\s+institution\s+(?:for\s+)?\w+',
                                r'(?:CSP|CIW|CIM|CTF|SAC|LAC|SQ|CHCF|SATF|PVSP)\b[\w\s\-]*',
                            ]:
                                _m = _re_pc.search(_body, _re_pc.IGNORECASE)
                                if _m:
                                    _institution = _m.group(0).strip()[:60]
                                    break
                            # Extract due date
                            _due_m = _re_pc.search(r'(?:respond|due|deadline|COB)\s+(?:by\s+)?(\d{1,2}/\d{1,2}/\d{2,4})', _body, _re_pc.IGNORECASE)
                            if _due_m:
                                _due_date = _due_m.group(1)
                            
                            pcs = _load_price_checks()
                            pcs[pc_id] = {
                                "id": pc_id,
                                "pc_number": rfq_email.get("solicitation_hint", "") or rfq_email.get("subject", "")[:40],
                                "institution": _institution,
                                "due_date": _due_date,
                                "requestor": _sender_name,
                                "requestor_email": rfq_email.get("sender_email", ""),
                                "ship_to": "", "items": [],
                                "source_pdf": pc_pdf if pc_pdf else "",
                                "status": "new",  # SHOW on dashboard — not parse_error
                                "parse_note": "Non-704 form — add items manually or upload 704",
                                "created_at": datetime.now().isoformat(),
                                "email_uid": email_uid,
                                "email_subject": rfq_email.get("subject", ""),
                                "source": "email_auto",
                                "reytech_quote_number": "", "linked_quote_number": "",
                            }
                            _save_price_checks(pcs)
                            # Persist source PDF to DB
                            try:
                                _pc_pdf_path = pc_pdf if pc_pdf else ""
                                if _pc_pdf_path and os.path.exists(_pc_pdf_path):
                                    with open(_pc_pdf_path, "rb") as _pdf_f:
                                        save_rfq_file(pc_id, os.path.basename(_pc_pdf_path),
                                                      "application/pdf", _pdf_f.read(),
                                                      category="source", uploaded_by="email_poller")
                            except Exception:
                                pass
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
                        "pc_number": rfq_email.get("solicitation_hint", "") or rfq_email.get("subject", "")[:40],
                        "institution": "", "due_date": "",
                        "requestor": rfq_email.get("sender_email", rfq_email.get("sender", "")),
                        "requestor_email": rfq_email.get("sender_email", ""),
                        "ship_to": "", "items": [],
                        "source_pdf": pdf_paths[0] if pdf_paths else "",
                        "status": "new",  # SHOW on dashboard
                        "parse_note": f"Non-704 form (error: {_e}) — add items manually",
                        "created_at": datetime.now().isoformat(),
                        "email_uid": rfq_email.get("email_uid", ""),
                        "email_subject": rfq_email.get("subject", ""),
                        "source": "email_auto",
                        "reytech_quote_number": "", "linked_quote_number": "",
                    }
                    _save_price_checks(pcs)
                    # Persist source PDF to DB
                    try:
                        _exc_pdf = pdf_paths[0] if pdf_paths else ""
                        if _exc_pdf and os.path.exists(_exc_pdf):
                            with open(_exc_pdf, "rb") as _pdf_f:
                                save_rfq_file(_force_id, os.path.basename(_exc_pdf),
                                              "application/pdf", _pdf_f.read(),
                                              category="source", uploaded_by="email_poller")
                    except Exception:
                        pass
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
            "body_text": rfq_email.get("body_text", rfq_email.get("body_preview", ""))[:3000],
        }
        
        # ── Supplement contact info from email body/signature ──────────
        # For forwarded emails, the original sender's signature has the real contact
        _body = rfq_email.get("body_text", rfq_email.get("body_preview", ""))
        if _body:
            sig_contact = _extract_signature_contact(_body)
            if sig_contact:
                if sig_contact.get("name") and not _generic_result.get("requestor_name"):
                    rfq_data["requestor_name"] = sig_contact["name"]
                if sig_contact.get("email") and not _generic_result.get("requestor_email"):
                    rfq_data["requestor_email"] = sig_contact["email"]
                if sig_contact.get("phone"):
                    rfq_data["requestor_phone"] = sig_contact["phone"]
                if sig_contact.get("address") and not rfq_data.get("delivery_location"):
                    rfq_data["delivery_location"] = sig_contact["address"]
                if sig_contact.get("institution"):
                    if not rfq_data.get("delivery_location"):
                        rfq_data["delivery_location"] = sig_contact["institution"]
                    rfq_data["institution_name"] = sig_contact["institution"]
                _trace.append(f"SIG CONTACT: {sig_contact}")
        
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
        rfq_data["body_text"] = (rfq_email.get("body_text", rfq_email.get("body_preview", "")) or "")[:3000]
        rfq_data["requestor_name"] = rfq_data.get("requestor_name") or rfq_email.get("sender_email", "")
        rfq_data["requestor_email"] = rfq_data.get("requestor_email") or rfq_email.get("sender_email", "")
        rfq_data["line_items"] = bulk_lookup(rfq_data.get("line_items", []))
        rfq_data["_original_items"] = [dict(i) for i in rfq_data.get("line_items", [])]
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
    
    # ── Fallback: extract solicitation + due date from email text ──────────
    _email_subject = rfq_email.get("subject", "")
    _email_body = rfq_email.get("body_text", rfq_email.get("body", rfq_email.get("body_preview", "")))
    _combined_text = f"{_email_subject} {_email_body}"
    if not rfq_data.get("solicitation_number") or rfq_data.get("solicitation_number") == "unknown":
        _sol = _extract_solicitation(_combined_text)
        if _sol:
            rfq_data["solicitation_number"] = _sol
            _trace.append(f"SOL FROM EMAIL TEXT: {_sol}")
    if not rfq_data.get("due_date") or rfq_data.get("due_date") == "TBD":
        _due = _extract_due_date(_combined_text)
        if _due:
            rfq_data["due_date"] = _due
            _trace.append(f"DUE FROM EMAIL TEXT: {_due}")

    # ── Auto-detect required forms from email body ──────────────────────
    try:
        from src.core.agency_config import extract_required_forms_from_text
        _form_result = extract_required_forms_from_text(_combined_text)
        if _form_result["forms"]:
            # Set as package_forms checklist (user can override on detail page)
            _pkg = {}
            for _fid in _form_result["forms"]:
                _pkg[_fid] = True
            rfq_data["package_forms"] = _pkg
            rfq_data["_detected_forms"] = _form_result["raw_matches"]
            _trace.append(f"FORMS DETECTED FROM EMAIL: {_form_result['forms']}")
            log.info("Auto-detected %d required forms from email text: %s",
                     len(_form_result["forms"]), _form_result["forms"])
    except Exception as _fe:
        log.debug("Form detection from email: %s", _fe)

    # ── Resolve buyer name: email sender is default, parsed name only if valid ──
    try:
        from src.core.contracts import resolve_buyer_name
        _from_header = rfq_email.get("sender", "")
        _sender_name = ""
        _sn_match = re.match(r'^([^<]+)<', _from_header)
        if _sn_match:
            _sender_name = _sn_match.group(1).strip().strip('"')
        _sender_email = rfq_email.get("sender_email", rfq_data.get("requestor_email", ""))
        rfq_data["requestor_name"] = resolve_buyer_name(
            rfq_data.get("requestor_name", ""), _sender_name, _sender_email)
        if not rfq_data.get("requestor_email") and _sender_email:
            rfq_data["requestor_email"] = _sender_email
    except Exception:
        pass

    rfqs[rfq_data["id"]] = rfq_data
    save_rfqs(rfqs)
    POLL_STATUS["emails_found"] += 1
    # SMS + webhook notification for new RFQ
    try:
        from src.agents.notify_agent import notify_new_rfq_sms
        notify_new_rfq_sms(rfq_data)
    except Exception as _e:
        log.debug("RFQ SMS notify: %s", _e)
    try:
        from src.core.webhooks import fire_webhook
        fire_webhook("rfq.created", {
            "rfq_id": rfq_data.get("id", ""),
            "solicitation_number": rfq_data.get("solicitation_number", ""),
            "agency": rfq_data.get("agency", ""),
            "item_count": len(rfq_data.get("line_items", rfq_data.get("items", []))),
            "due_date": rfq_data.get("due_date", ""),
        })
    except Exception as _e:
        log.debug("RFQ webhook fire: %s", _e)
    _trace.append(f"RFQ CREATED: sol={rfq_data.get('solicitation_number','?')}")
    
    # ── F1: Auto-link RFQ to matching PC, port pricing ──────────────
    if _link_rfq_to_pc(rfq_data, _trace):
        # Re-save with ported pricing
        rfqs[rfq_data["id"]] = rfq_data
        save_rfqs(rfqs)
    
    # ── Cross-queue cleanup: remove unlinked PCs with same sol# ──
    # Converted PCs are preserved (they have pricing history).
    sol_num = rfq_data.get("solicitation_number", "")
    if sol_num and sol_num != "unknown":
        try:
            pcs = _load_price_checks()
            pc_dups = [pid for pid, pc in pcs.items()
                       if pc.get("pc_number", "").replace("AD-", "").strip() == sol_num.strip()
                       and not pc.get("converted_to_rfq")]
            if pc_dups:
                for pid in pc_dups:
                    pcs[pid]["converted_to_rfq"] = rfq_data["id"]
                    pcs[pid]["converted_at"] = datetime.now().isoformat()
                    pcs[pid]["status"] = "converted"
                _save_price_checks(pcs)
                _trace.append(f"Marked {len(pc_dups)} PCs as converted (preserved)")
                log.info("PCs %s marked converted for RFQ %s", pc_dups, rfq_data["id"])
        except Exception as _xqe:
            _trace.append(f"Cross-queue cleanup error: {_xqe}")
    
    POLL_STATUS.setdefault("_email_traces", []).append(_trace)
    t.ok("RFQ created", sol=rfq_data.get("solicitation_number","?"), rfq_id=rfq_data.get("id","?"))
    log.info(f"Auto-imported RFQ #{rfq_data.get('solicitation_number', 'unknown')}")
    try:
        from src.core.dal import log_lifecycle_event as _lle_rfq
        _rid = rfq_data.get("id", "")
        _lle_rfq("rfq", _rid, "email_received",
            f"From {rfq_email.get('sender_email', '?')}: {rfq_email.get('subject', '?')[:60]}",
            actor="system", detail={"sender": rfq_email.get("sender_email", ""), "subject": rfq_email.get("subject", "")})
        _lle_rfq("rfq", _rid, "email_parsed",
            f"Parsed {len(rfq_data.get('line_items', []))} items from {len(templates)} templates",
            actor="system", detail={"item_count": len(rfq_data.get("line_items", [])), "templates": list(templates.keys())})
    except Exception:
        pass
    
    # Log activity
    _log_rfq_activity(rfq_data["id"], "created",
        f"RFQ #{rfq_data.get('solicitation_number','?')} imported from email: {rfq_email.get('subject','')}",
        actor="system",
        metadata={"source": "email", "templates": list(templates.keys()),
                  "attachments": [a.get("filename","?") for a in rfq_email.get("attachments",[])]})
    
    # Ensure sender is in CRM
    _ensure_contact_from_email(rfq_email)

    # ── F10: Auto-Price from catalog + product_catalog + price_history ────
    # For items that have no pricing, try all available sources:
    # 1. core/catalog (products table — seed data + manual adds)
    # 2. agents/product_catalog (product_catalog table — grows from PC work)
    # 3. price_history (previous quotes)
    try:
        _auto_priced = 0
        for _item in rfq_data.get("line_items", []):
            if _item.get("price_per_unit") or _item.get("supplier_cost"):
                continue  # already has pricing
            _desc = (_item.get("description", "") or "")[:50]
            _pn = _item.get("item_number", "") or ""
            if not _desc and not _pn:
                continue

            # Source 1: core catalog (products table)
            try:
                from src.core.catalog import search_catalog
                _matches = search_catalog(_pn or _desc[:30], limit=1)
                if _matches:
                    _m = _matches[0]
                    if _m.get("typical_cost") and _m["typical_cost"] > 0:
                        _item["supplier_cost"] = _m["typical_cost"]
                    if _m.get("list_price") and _m["list_price"] > 0:
                        _item["price_per_unit"] = _m["list_price"]
                    if _m.get("sku"):
                        _item["_auto_source"] = f"catalog:{_m['sku']}"
                    _auto_priced += 1
                    continue
            except Exception:
                pass

            # Source 2: product_catalog (grows from PC quoting work)
            try:
                from src.agents.product_catalog import match_item, init_catalog_db
                init_catalog_db()
                _pc_matches = match_item(_desc, _pn, top_n=1)
                if _pc_matches and _pc_matches[0].get("match_confidence", 0) >= 0.45:
                    _pm = _pc_matches[0]
                    if _pm.get("cost") and float(_pm["cost"]) > 0:
                        _item["supplier_cost"] = float(_pm["cost"])
                    if _pm.get("sell_price") and float(_pm["sell_price"]) > 0:
                        _item["price_per_unit"] = float(_pm["sell_price"])
                    elif _pm.get("last_sold_price") and float(_pm["last_sold_price"]) > 0:
                        _item["price_per_unit"] = float(_pm["last_sold_price"])
                    _item["_auto_source"] = f"pc_catalog:{_pm.get('name','')[:30]}"
                    _item["_catalog_product_id"] = _pm.get("id")
                    if _item.get("supplier_cost") or _item.get("price_per_unit"):
                        _auto_priced += 1
                        continue
            except Exception:
                pass

            # Source 3: price_history
            try:
                from src.core.db import get_price_history_db
                _hist = get_price_history_db(
                    description=_desc[:40] if not _pn else "",
                    part_number=_pn, limit=3
                )
                if _hist:
                    _prices = [h["unit_price"] for h in _hist if h.get("unit_price")]
                    if _prices:
                        _avg = sum(_prices) / len(_prices)
                        _item["supplier_cost"] = round(_avg * 0.75, 2)
                        _item["price_per_unit"] = round(_avg, 2)
                        _item["_auto_source"] = f"history:avg({len(_prices)})"
                        _auto_priced += 1
            except Exception:
                pass

        if _auto_priced:
            rfq_data["auto_priced_count"] = _auto_priced
            rfq_data["status"] = "auto_priced"
            rfqs[rfq_data["id"]] = rfq_data
            save_rfqs(rfqs)
            log.info("F10: Auto-priced %d items for RFQ %s from catalog/history",
                     _auto_priced, rfq_data.get("solicitation_number", ""))
    except Exception as _ap_e:
        log.debug("F10 auto-price: %s", _ap_e)

    # ── Auto Price Lookup (no quote generation) ─────────────────────────────
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
    # Guard: DATA_DIR/UPLOAD_DIR may not be in scope when called from an injected module
    try:
        _DATA_DIR = DATA_DIR
        _UPLOAD_DIR = UPLOAD_DIR
    except NameError:
        from src.core.paths import DATA_DIR as _DATA_DIR, UPLOAD_DIR as _UPLOAD_DIR
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
    
    # Reuse existing poller if available — just reload processed file
    # (avoids creating a new IMAP connection on every poll cycle)
    email_cfg = dict(email_cfg)  # copy so we don't mutate
    email_cfg["email_password"] = effective_password
    email_cfg["processed_file"] = os.path.join(_DATA_DIR, "processed_emails.json")
    if not email_cfg.get("email"):
        email_cfg["email"] = os.environ.get("GMAIL_ADDRESS", "")

    if _shared_poller is None:
        _shared_poller = EmailPoller(email_cfg)
        POLL_STATUS["_poller_instance"] = _shared_poller
        log.info("Created poller for %s", email_cfg.get("email", "NO EMAIL SET"))
    else:
        # Reload processed UIDs from file (picks up system-reset changes)
        try:
            _shared_poller._load_processed()
        except Exception:
            pass
    processed_count = len(_shared_poller._processed)
    log.debug("Poll: %d processed UIDs loaded", processed_count)
    
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
        _, _, free_bytes = _shutil.disk_usage(_UPLOAD_DIR)
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
        POLL_STATUS["last_check"] = _pst_now_iso()  # mark attempt regardless of outcome
        if connected:
            log.info("IMAP connected, checking for RFQs...")
            rfq_emails = _shared_poller.check_for_rfqs(save_dir=_UPLOAD_DIR)
            POLL_STATUS["error"] = None
            POLL_STATUS["_diag"]["rfqs_returned"] = len(rfq_emails)
            # Capture poller-level diagnostics
            if hasattr(_shared_poller, '_diag'):
                _raw = _shared_poller._diag
                POLL_STATUS["_diag"]["poller"] = {
                    k: list(v) if isinstance(v, set) else v
                    for k, v in _raw.items()
                }
            log.info(f"Poll check complete: {len(rfq_emails)} RFQ emails found")
            
            for rfq_email in rfq_emails:
                try:
                    # Check for shipping/tracking/delivery emails BEFORE RFQ processing
                    try:
                        from src.agents.order_digest import scan_email_for_tracking, apply_tracking_to_order
                        track_result = scan_email_for_tracking(
                            rfq_email.get("subject", ""),
                            rfq_email.get("body", ""),
                            rfq_email.get("sender", ""),
                        )
                        if track_result.get("has_tracking"):
                            for oid in track_result.get("matched_orders", []):
                                for tn in track_result.get("tracking_numbers", []):
                                    apply_tracking_to_order(oid, tn["number"], tn["carrier"])
                            log.info("Sales@ tracking scan: %d tracking numbers, %d matched orders from '%s'",
                                     len(track_result.get("tracking_numbers", [])),
                                     len(track_result.get("matched_orders", [])),
                                     rfq_email.get("subject", "?")[:50])
                        # Check for delivery confirmation
                        if track_result.get("is_shipping_email"):
                            _check_delivery_status(rfq_email, track_result)
                    except Exception as _te:
                        log.debug("Sales@ tracking scan: %s", _te)
                    # Route PO emails to pending review instead of auto-processing
                    if rfq_email.get("_is_po"):
                        _load_pending_pos()
                        _add_pending_po(rfq_email.get("_po_data", {}))
                        log.info("PO routed to pending review: PO#%s", rfq_email.get("_po_data", {}).get("po_number", "?"))
                        continue
                    rfq_data = process_rfq_email(rfq_email)
                    if rfq_data:
                        imported.append(rfq_data)
                    else:
                        POLL_STATUS["_diag"]["pcs_routed"] += 1
                except Exception as pe:
                    _subj_err = rfq_email.get('subject','?')[:40] if isinstance(rfq_email, dict) else str(rfq_email)[:40]
                    POLL_STATUS["_diag"]["errors"].append(f"process_rfq({_subj_err}): {pe}")
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

    # ── Second inbox: mike@ (orders, some PCs/quotes) ──────────────────────
    mike_addr = os.environ.get("GMAIL_ADDRESS_2", "")
    mike_pwd = os.environ.get("GMAIL_PASSWORD_2", "")
    POLL_STATUS["_mike_diag"] = {"configured": bool(mike_addr and mike_pwd), "addr": mike_addr}
    if mike_addr and mike_pwd:
        try:
            mike_cfg = dict(email_cfg)
            mike_cfg["email"] = mike_addr
            mike_cfg["email_password"] = mike_pwd
            mike_cfg["processed_file"] = os.path.join(_DATA_DIR, "processed_emails_mike.json")
            mike_cfg["inbox_name"] = "mike"
            mike_poller = EmailPoller(mike_cfg)
            POLL_STATUS["_mike_diag"]["processed_loaded"] = len(mike_poller._processed)
            log.info("Polling second inbox: %s (%d processed UIDs)",
                     mike_addr, len(mike_poller._processed))
            if mike_poller.connect():
                POLL_STATUS["_mike_diag"]["connected"] = True
                mike_emails = mike_poller.check_for_rfqs(save_dir=_UPLOAD_DIR)
                POLL_STATUS["_mike_diag"]["emails_returned"] = len(mike_emails)
                POLL_STATUS["_mike_diag"]["subjects"] = [e.get("subject", "?")[:60] for e in mike_emails]
                if hasattr(mike_poller, '_diag'):
                    POLL_STATUS["_mike_diag"]["poller_diag"] = {
                        k: list(v) if isinstance(v, set) else v
                        for k, v in mike_poller._diag.items()
                    }
                log.info("Second inbox: %d emails found", len(mike_emails))
                for rfq_email in mike_emails:
                    try:
                        # Check for shipping/tracking emails before RFQ processing
                        try:
                            from src.agents.order_digest import scan_email_for_tracking, apply_tracking_to_order
                            track_result = scan_email_for_tracking(
                                rfq_email.get("subject", ""),
                                rfq_email.get("body", ""),
                                rfq_email.get("sender", ""),
                            )
                            if track_result.get("has_tracking"):
                                for oid in track_result.get("matched_orders", []):
                                    for tn in track_result.get("tracking_numbers", []):
                                        apply_tracking_to_order(oid, tn["number"], tn["carrier"])
                                log.info("Tracking scan: %d tracking numbers, %d matched orders",
                                         len(track_result.get("tracking_numbers", [])),
                                         len(track_result.get("matched_orders", [])))
                        except Exception as _te:
                            log.debug("Tracking scan error: %s", _te)
                        # Route PO emails to pending review instead of auto-processing
                        if rfq_email.get("_is_po"):
                            _load_pending_pos()
                            _add_pending_po(rfq_email.get("_po_data", {}))
                            log.info("PO routed to pending review: PO#%s", rfq_email.get("_po_data", {}).get("po_number", "?"))
                            continue
                        rfq_data = process_rfq_email(rfq_email)
                        if rfq_data:
                            imported.append(rfq_data)
                    except Exception as pe:
                        log.error("process_rfq_email (mike@) error: %s", pe, exc_info=True)
                        POLL_STATUS["_mike_diag"].setdefault("errors", []).append(str(pe))
                        failed_uid = rfq_email.get("email_uid", "")
                        if failed_uid:
                            mike_poller._processed.discard(failed_uid)
            else:
                POLL_STATUS["_mike_diag"]["connected"] = False
                log.warning("IMAP connect failed for %s", mike_addr)
        except Exception as e:
            POLL_STATUS["_mike_diag"]["exception"] = str(e)
            log.error("Second inbox poll error: %s", e, exc_info=True)

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
    log.info("Email poll loop started — interval=%ds, polling now...", interval)

    while POLL_STATUS["running"]:
        if not POLL_STATUS.get("paused"):
            try:
                do_poll_check()
                try:
                    from src.core.scheduler import heartbeat
                    heartbeat("email-poller", success=True)
                except Exception:
                    pass
            except Exception as e:
                POLL_STATUS["error"] = f"Poll loop crash: {e}"
                log.error("Email poll loop crash: %s", e, exc_info=True)
                try:
                    from src.core.scheduler import heartbeat
                    heartbeat("email-poller", success=False, error=str(e)[:200])
                except Exception:
                    pass
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
    _now = datetime.now().isoformat()
    activity = _load_crm_activity()
    activity.append({
        "id": f"crm-{datetime.now().strftime('%Y%m%d%H%M%S')}-{len(activity)}",
        "ref_id": ref_id,
        "event_type": event_type,
        "description": description,
        "actor": actor,
        "timestamp": _now,
        "metadata": metadata or {},
    })
    _save_crm_activity(activity)

    # Dual-write to SQLite activity_log for unified feed
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT INTO activity_log
                (contact_id, logged_at, event_type, subject, body, actor, metadata)
                VALUES (?,?,?,?,?,?,?)
            """, (ref_id, _now, event_type, description[:200],
                  description, actor,
                  json.dumps(metadata or {}, default=str)))
    except Exception:
        pass

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
    """Load orders — DAL (SQLite) primary, JSON fallback."""
    try:
        from src.core.dal import list_orders as _dal_list
        rows = _dal_list(limit=10000)
        if rows:
            return {r["id"]: r for r in rows}
    except Exception as e:
        log.warning("DAL list_orders failed, falling back to JSON: %s", str(e)[:200])
    return _cached_json_load(ORDERS_FILE, fallback={})

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
    # ── Fire webhook for order_created event ──
    try:
        from src.core.webhooks import fire_event
        fire_event("order_created", {
            "order_id": order["order_id"],
            "quote_number": qn,
            "agency": order.get("agency", ""),
            "institution": order.get("institution", ""),
            "total": f"${order.get('total', 0):,.2f}",
            "items": len(line_items),
        })
    except Exception as _we:
        log.debug("Webhook fire: %s", _we)
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

    # ── VALIDATION GATE: reject phantom orders from bad PO email parses ──
    has_real_items = any(
        (li.get("description", "") or "").strip() or (li.get("part_number", "") or "").strip()
        for li in line_items
    )
    if not has_real_items and total == 0 and not qn:
        # No items, no value, no linked quote → garbage parse, skip
        log.warning("Skipping phantom order creation: PO=%s has no items, $0 total, no linked quote",
                    po_num)
        return {"order_id": "", "skipped": True, "reason": "no_items_no_value"}

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
    # ── Notify Mike: new PO arrived ──────────────────────────────
    try:
        from src.agents.notify_agent import send_alert
        send_alert(
            event_type="po_received",
            title=f"🏆 New PO: {po_num or oid}",
            body=f"PO #{po_num} from {order.get('institution', 'unknown')} — ${total:,.2f}" +
                 (f"\nLinked to quote {qn}" if qn else "") +
                 f"\n{len(line_items)} line items",
            urgency="deal",
            context={"order_id": oid, "po_number": po_num, "quote_number": qn},
            cooldown_key=f"po_received:{po_num or oid}",
        )
    except Exception as _ne:
        log.debug("Order notification: %s", _ne)
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

    # ── DAL sync (Layer 3) ──
    try:
        from src.core.dal import update_order_status as _dal_uo
        _dal_uo(oid, order["status"])
    except Exception:
        pass

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
    <span id="brief-badge" style="font-size:13px;padding:2px 8px;border-radius:10px;background:var(--sf2);color:var(--tx2);font-weight:500"></span>
    <span id="brief-toggle" style="font-size:13px;color:var(--tx2);margin-left:4px">&#x25BC;</span>
   </div>
   <div id="brief-headline" style="font-size:15px;font-weight:600;margin-top:8px;line-height:1.4"></div>
  </div>
  <div style="display:flex;gap:6px;align-items:center;flex-shrink:0">
   <a href="/agents" class="btn btn-sm btn-s" style="font-size:13px;padding:4px 10px;white-space:nowrap">&#x1F4CA; Full Report</a>
   <button onclick="loadBrief(true)" id="brief-refresh-btn" style="font-size:13px;padding:4px 8px;background:rgba(79,140,255,.1);border:1px solid rgba(79,140,255,.3);color:var(--ac);border-radius:6px;cursor:pointer;white-space:nowrap">&#x1F504; Refresh</button>
  </div>
 </div>
 <div id="brief-body">
  <div id="brief-grid">
   <div style="font-size:14px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;display:flex;align-items:center;gap:6px">
    Needs Your Attention <span id="approval-count" class="brief-count"></span>
   </div>
   <div id="approvals-list"></div>
  </div>
  <div id="pipeline-bar" style="display:flex;gap:12px;flex-wrap:wrap;margin-top:16px;padding-top:14px;border-top:1px solid var(--bd)"></div>
  <div id="agents-row" style="display:none;margin-top:16px;padding-top:14px;border-top:1px solid var(--bd)">
   <div style="font-size:13px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Agent Status</div>
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
    limit = min(int(request.args.get("limit", 50)), 200)

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
    try:
        from src.api.trace import clear_traces
        clear_traces()
        return jsonify({"ok": True, "cleared": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

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
# ── Scheduler & Backup API (F4 + F5) ─────────────────────────────────────────

@bp.route("/api/scheduler/status")
@auth_required
def scheduler_status():
    """Returns health status of all background jobs."""
    try:
        from src.core.scheduler import get_all_jobs, backup_health
        jobs = get_all_jobs()
        bh = backup_health()
        dead = [j for j in jobs if j["status"] == "dead"]
        return jsonify({
            "ok": True,
            "jobs": jobs,
            "total": len(jobs),
            "dead_count": len(dead),
            "dead_jobs": [j["name"] for j in dead],
            "backup_health": bh,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/admin/backups")
@auth_required
def admin_backups():
    """List available database backups."""
    try:
        from src.core.scheduler import list_backups, backup_health
        return jsonify({
            "ok": True,
            "backups": list_backups(),
            "health": backup_health(),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/admin/backup-now", methods=["GET", "POST"])
@auth_required
def admin_backup_now():
    """Trigger an immediate database backup."""
    try:
        from src.core.scheduler import run_backup
        result = run_backup()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/admin/cleanup-queue", methods=["GET", "POST"])
@auth_required
def api_cleanup_queue():
    """Bulk cleanup of ghost/duplicate PCs and RFQs.

    GET = dry run (shows what would be deleted)
    POST with execute=true = actually delete

    Rules:
    - Delete PCs with 0 items and status in (new, draft, parsed)
    - Delete RFQs with 0 items and status in (new, draft)
    - Delete duplicate solicitation numbers (keep the one with most items)
    - Delete entries with solicitation = 'unknown' and 0 items
    - NEVER delete entries with status sent/won/lost/generated or items > 0
    """
    execute = request.args.get("execute", "false").lower() == "true"
    if request.method == "POST":
        data = request.get_json(force=True, silent=True) or {}
        execute = data.get("execute", execute)

    results = {"pc_deleted": [], "rfq_deleted": [], "pc_kept": [], "rfq_kept": [], "dry_run": not execute}

    # ── Clean Price Checks (aggressive) ──
    pcs = _load_price_checks()
    pc_delete_ids = set()
    import re as _cleanup_re
    _today_str = datetime.now().strftime("%Y-%m-%d")

    for pid, pc in pcs.items():
        sol = pc.get("solicitation_number", "") or pc.get("pc_number", "") or ""
        status = pc.get("status", "")
        items = len(pc.get("items", []))
        created = pc.get("created_at", "") or ""

        # Always delete: dismissed/archived/deleted/duplicate
        if status in ("dismissed", "archived", "deleted", "duplicate"):
            pc_delete_ids.add(pid)
            results["pc_deleted"].append({"id": pid[:12], "sol": sol, "status": status, "items": items, "reason": f"status={status}"})
            continue

        # Keep anything created today
        if _today_str in created:
            results["pc_kept"].append({"id": pid[:12], "sol": sol, "status": status, "items": items})
            continue

        # Keep sent/won/pending_award with items AND numeric solicitation
        if status in ("sent", "won", "pending_award") and items > 0 and _cleanup_re.match(r'^\d+$', sol or ""):
            results["pc_kept"].append({"id": pid[:12], "sol": sol, "status": status, "items": items})
            continue

        # Keep draft/priced PCs with items AND numeric solicitation
        if status in ("new", "draft", "parsed", "priced", "ready", "auto_drafted", "quoted", "generated") and items > 0 and _cleanup_re.match(r'^\d+$', sol or ""):
            results["pc_kept"].append({"id": pid[:12], "sol": sol, "status": status, "items": items})
            continue

        # Everything else: delete (ghosts, non-numeric sols, 0 items)
        pc_delete_ids.add(pid)
        results["pc_deleted"].append({"id": pid[:12], "sol": sol, "status": status, "items": items, "reason": "no numeric sol or no items"})

    # ── Clean RFQs (aggressive) ──
    rfqs = load_rfqs()
    rfq_delete_ids = set()

    for rid, r in rfqs.items():
        sol = r.get("solicitation_number", "") or r.get("rfq_number", "") or ""
        status = r.get("status", "")
        items_count = len(r.get("line_items", r.get("items", [])))
        created = r.get("received_at", "") or r.get("created_at", "") or ""

        # Always delete: dismissed/archived/deleted/duplicate/cancelled
        if status in ("dismissed", "archived", "deleted", "duplicate", "cancelled"):
            rfq_delete_ids.add(rid)
            results["rfq_deleted"].append({"id": rid[:12], "sol": sol, "buyer": r.get("requestor_name", "")[:20], "status": status, "items": items_count, "reason": f"status={status}"})
            continue

        # Keep anything created today
        if _today_str in created:
            results["rfq_kept"].append({"id": rid[:12], "sol": sol, "buyer": r.get("requestor_name", "")[:20], "status": status, "items": items_count})
            continue

        # Keep sent/won/generated with items
        if status in ("sent", "won", "generated") and items_count > 0:
            results["rfq_kept"].append({"id": rid[:12], "sol": sol, "buyer": r.get("requestor_name", "")[:20], "status": status, "items": items_count})
            continue

        # Keep new/draft/ready with items AND real solicitation
        if status in ("new", "draft", "ready") and items_count > 0 and sol and sol != "unknown":
            results["rfq_kept"].append({"id": rid[:12], "sol": sol, "buyer": r.get("requestor_name", "")[:20], "status": status, "items": items_count})
            continue

        # Everything else: delete
        rfq_delete_ids.add(rid)
        results["rfq_deleted"].append({"id": rid[:12], "sol": sol, "buyer": r.get("requestor_name", "")[:20], "status": status, "items": items_count, "reason": "no real sol or no items"})

    # ── Execute deletes ──
    if execute:
        if pc_delete_ids:
            for pid in pc_delete_ids:
                pcs.pop(pid, None)
            _save_price_checks(pcs)
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    for pid in pc_delete_ids:
                        conn.execute("DELETE FROM price_checks WHERE id = ?", (pid,))
            except Exception:
                pass

        if rfq_delete_ids:
            for rid in rfq_delete_ids:
                rfqs.pop(rid, None)
            save_rfqs(rfqs)
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    for rid in rfq_delete_ids:
                        conn.execute("DELETE FROM rfqs WHERE id = ?", (rid,))
            except Exception:
                pass

    results["summary"] = {
        "pc_deleted": len(results["pc_deleted"]),
        "pc_kept": len(results["pc_kept"]),
        "rfq_deleted": len(results["rfq_deleted"]),
        "rfq_kept": len(results["rfq_kept"]),
    }
    return jsonify(results)


@bp.route("/api/admin/hard-cleanup", methods=["GET", "POST"])
@auth_required
def api_hard_cleanup():
    """Aggressive cleanup — keeps only real business items."""
    execute = request.args.get("execute", "false").lower() == "true"
    if request.method == "POST":
        data = request.get_json(force=True, silent=True) or {}
        execute = data.get("execute", execute)

    import re as _hc_re

    # ── RFQ FILTER ──
    rfqs = load_rfqs()
    rfq_keep = {}
    rfq_delete = {}

    for rid, r in rfqs.items():
        sol = r.get("solicitation_number", "") or r.get("rfq_number", "")
        status = r.get("status", "")
        items_count = len(r.get("line_items", r.get("items", [])))

        keep = False
        reason = ""

        # Keep sent/won/generated with items (real business)
        if status in ("sent", "won", "generated") and items_count > 0:
            keep, reason = True, "real business"
        # Keep new/draft with real solicitation AND items
        elif status in ("new", "draft", "ready") and items_count > 0 and sol and sol != "unknown":
            keep, reason = True, "active work"

        if keep:
            rfq_keep[rid] = {"sol": sol[:15], "buyer": r.get("requestor_name", "")[:25], "status": status, "items": items_count, "reason": reason}
        else:
            rfq_delete[rid] = {"sol": sol[:15], "buyer": r.get("requestor_name", "")[:25], "status": status, "items": items_count}

    # Dedup: if multiple RFQs have same sol, keep the best one
    # Priority: sent/won > generated > draft/new, then most items as tiebreak
    _status_rank = {"won": 5, "sent": 4, "generated": 3, "ready": 2, "draft": 1, "new": 0}
    _sol_best = {}
    for rid in list(rfq_keep.keys()):
        sol = rfq_keep[rid]["sol"]
        if not sol or sol == "unknown":
            continue
        rank = _status_rank.get(rfq_keep[rid]["status"], 0)
        items = rfq_keep[rid]["items"]
        score = (rank, items)
        if sol in _sol_best:
            prev_rid, prev_score = _sol_best[sol]
            if score > prev_score:
                rfq_delete[prev_rid] = rfq_keep.pop(prev_rid)
                rfq_delete[prev_rid]["reason"] = "duplicate sol (kept better)"
                _sol_best[sol] = (rid, score)
            else:
                rfq_delete[rid] = rfq_keep.pop(rid)
                rfq_delete[rid]["reason"] = "duplicate sol (kept better)"
        else:
            _sol_best[sol] = (rid, score)

    # ── PC FILTER ──
    pcs = _load_price_checks()
    pc_keep = {}
    pc_delete = {}

    for pid, pc in pcs.items():
        sol = pc.get("solicitation_number", "") or pc.get("pc_number", "")
        status = pc.get("status", "")
        items_count = len(pc.get("items", []))

        keep = False
        reason = ""

        # NEVER keep duplicate/dismissed/archived regardless
        if status in ("duplicate", "dismissed", "archived", "deleted"):
            keep = False
        # Keep sent/won with items and numeric sol
        elif status in ("sent", "won", "pending_award") and items_count > 0 and _hc_re.match(r'^\d+$', sol or ""):
            keep, reason = True, "sent/won"
        # Keep new/draft with real numeric sol AND items
        elif status in ("new", "draft", "parsed", "priced", "ready") and items_count > 0 and _hc_re.match(r'^\d+$', sol or ""):
            keep, reason = True, "active with real sol"

        if keep:
            pc_keep[pid] = {"sol": sol[:15], "buyer": (pc.get("requestor", "") or pc.get("buyer", ""))[:25], "status": status, "items": items_count, "reason": reason}
        else:
            pc_delete[pid] = {"sol": sol[:15], "buyer": (pc.get("requestor", "") or pc.get("buyer", ""))[:25], "status": status, "items": items_count}

    # Dedup PCs: same solicitation → keep the best one
    _pc_sol_best = {}
    for pid in list(pc_keep.keys()):
        sol = pc_keep[pid]["sol"]
        if not sol or sol == "unknown":
            continue
        rank = _status_rank.get(pc_keep[pid]["status"], 0)
        items = pc_keep[pid]["items"]
        score = (rank, items)
        if sol in _pc_sol_best:
            prev_pid, prev_score = _pc_sol_best[sol]
            if score > prev_score:
                pc_delete[prev_pid] = pc_keep.pop(prev_pid)
                _pc_sol_best[sol] = (pid, score)
            else:
                pc_delete[pid] = pc_keep.pop(pid)
        else:
            _pc_sol_best[sol] = (pid, score)

    # Cross-check: delete PCs whose solicitation matches a sent/won RFQ
    # These are re-imported duplicates from the re-poll
    _sent_rfq_sols = set()
    for rid in rfq_keep:
        if rfq_keep[rid]["status"] in ("sent", "won", "generated"):
            s = rfq_keep[rid]["sol"]
            if s:
                _sent_rfq_sols.add(s)

    for pid in list(pc_keep.keys()):
        sol = pc_keep[pid]["sol"]
        if sol and sol in _sent_rfq_sols:
            pc_delete[pid] = pc_keep.pop(pid)
            pc_delete[pid]["reason"] = f"duplicates sent RFQ sol#{sol}"

    if execute:
        for rid in rfq_delete:
            rfqs.pop(rid, None)
        save_rfqs(rfqs)

        for pid in pc_delete:
            pcs.pop(pid, None)
        _save_price_checks(pcs)

        try:
            from src.core.db import get_db
            with get_db() as conn:
                for rid in rfq_delete:
                    conn.execute("DELETE FROM rfqs WHERE id = ?", (rid,))
                for pid in pc_delete:
                    conn.execute("DELETE FROM price_checks WHERE id = ?", (pid,))
        except Exception as _e:
            log.warning("SQLite cleanup: %s", _e)

    return jsonify({
        "dry_run": not execute,
        "rfq_keep": list(rfq_keep.values()),
        "rfq_delete": list(rfq_delete.values()),
        "pc_keep": list(pc_keep.values()),
        "pc_delete": list(pc_delete.values()),
        "summary": {
            "rfq_keep": len(rfq_keep),
            "rfq_delete": len(rfq_delete),
            "pc_keep": len(pc_keep),
            "pc_delete": len(pc_delete),
        }
    })


@bp.route("/api/admin/force-repoll", methods=["GET", "POST"])
@auth_required
def api_force_repoll():
    """Clear processed email cache and trigger re-poll."""
    global _shared_poller
    cleared = 0
    if _shared_poller and hasattr(_shared_poller, '_processed'):
        cleared = len(_shared_poller._processed)
        _shared_poller._processed.clear()
    # Clear disk cache
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    try:
        if os.path.exists(proc_file):
            with open(proc_file) as f:
                disk_uids = json.load(f)
            cleared += len(disk_uids)
            os.remove(proc_file)
    except Exception:
        pass
    # Clear SQLite
    try:
        from src.core.db import get_db
        with get_db() as conn:
            db_count = conn.execute("SELECT COUNT(*) FROM processed_emails").fetchone()[0]
            conn.execute("DELETE FROM processed_emails")
            cleared += db_count
    except Exception:
        pass
    return jsonify({"ok": True, "cleared_uids": cleared, "next": "Hit Check Now or wait for auto-poll"})


@bp.route("/api/admin/delete-pc/<pcid>", methods=["GET", "POST"])
@auth_required
def api_admin_delete_pc(pcid):
    """Delete a single PC by ID."""
    pcs = _load_price_checks()
    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"})
    sol = pcs[pcid].get("solicitation_number", "") or pcs[pcid].get("pc_number", "")
    del pcs[pcid]
    _save_price_checks(pcs)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM price_checks WHERE id = ?", (pcid,))
    except Exception:
        pass
    return jsonify({"ok": True, "deleted": pcid, "sol": sol})


@bp.route("/api/admin/delete-rfq/<rid>", methods=["GET", "POST"])
@auth_required
def api_admin_delete_rfq(rid):
    """Delete a single RFQ by ID."""
    rfqs = load_rfqs()
    if rid not in rfqs:
        return jsonify({"ok": False, "error": "RFQ not found"})
    sol = rfqs[rid].get("solicitation_number", "") or rfqs[rid].get("rfq_number", "")
    del rfqs[rid]
    save_rfqs(rfqs)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM rfqs WHERE id = ?", (rid,))
    except Exception:
        pass
    return jsonify({"ok": True, "deleted": rid, "sol": sol})


@bp.route("/api/admin/delete-by-sol", methods=["GET", "POST"])
@auth_required
def api_admin_delete_by_sol():
    """Delete PCs and RFQs by solicitation number.

    Usage: /api/admin/delete-by-sol?sol=10840485&type=pc
           /api/admin/delete-by-sol?sol=10840485&type=both
    """
    sol = request.args.get("sol", "").strip()
    dtype = request.args.get("type", "pc").strip()  # pc, rfq, both

    if not sol:
        return jsonify({"ok": False, "error": "?sol= required"})

    deleted = []

    if dtype in ("pc", "both"):
        pcs = _load_price_checks()
        to_del = [pid for pid, pc in pcs.items()
                  if (pc.get("solicitation_number", "") or pc.get("pc_number", "")) == sol]
        for pid in to_del:
            buyer = pcs[pid].get("requestor", "") or pcs[pid].get("buyer", "")
            del pcs[pid]
            deleted.append({"type": "pc", "id": pid, "sol": sol, "buyer": buyer})
        if to_del:
            _save_price_checks(pcs)

    if dtype in ("rfq", "both"):
        rfqs = load_rfqs()
        to_del = [rid for rid, r in rfqs.items()
                  if (r.get("solicitation_number", "") or r.get("rfq_number", "")) == sol]
        for rid in to_del:
            buyer = rfqs[rid].get("requestor_name", "")
            del rfqs[rid]
            deleted.append({"type": "rfq", "id": rid, "sol": sol, "buyer": buyer})
        if to_del:
            save_rfqs(rfqs)

    # Clean SQLite too
    try:
        from src.core.db import get_db
        with get_db() as conn:
            for d in deleted:
                if d["type"] == "pc":
                    conn.execute("DELETE FROM price_checks WHERE id = ?", (d["id"],))
                else:
                    conn.execute("DELETE FROM rfqs WHERE id = ?", (d["id"],))
    except Exception:
        pass

    return jsonify({"ok": True, "deleted": deleted, "count": len(deleted)})


@bp.route("/api/admin/cleanup-quote-numbers", methods=["GET", "POST"])
@auth_required
def api_cleanup_quote_numbers():
    """Clean ghost quote numbers and reset counter.
    GET = always dry_run (safe from browser). POST body for real cleanup.
    POST body: {"keep": ["R26Q30"], "reset_to": 30, "dry_run": false}
    """
    if request.method == "GET":
        data = {"dry_run": not request.args.get("execute"),
                "keep": request.args.get("keep", "").split(",") if request.args.get("keep") else [],
                "reset_to": int(request.args.get("reset_to")) if request.args.get("reset_to") else None,
                "rename": request.args.get("rename", "")}  # e.g. rename=R26Q37:R26Q30
    else:
        data = request.get_json(force=True, silent=True) or {}
    keep = set(data.get("keep", []))
    reset_to = data.get("reset_to")
    dry_run = data.get("dry_run", True)
    rename_str = data.get("rename", "")

    # Parse rename: "R26Q37:R26Q30" → rename R26Q37 to R26Q30
    rename_map = {}
    if rename_str:
        for pair in rename_str.split(","):
            if ":" in pair:
                old_qn, new_qn = pair.strip().split(":", 1)
                rename_map[old_qn.strip()] = new_qn.strip()

    rfqs = load_rfqs()
    cleaned = []
    kept = []
    renamed = []

    for rid, r in rfqs.items():
        qn = r.get("reytech_quote_number", "")
        if not qn or not qn.startswith("R26Q"):
            continue
        # Check if this quote should be renamed
        if qn in rename_map:
            new_qn = rename_map[qn]
            if not dry_run:
                r["reytech_quote_number"] = new_qn
            renamed.append({"old": qn, "new": new_qn, "rfq": rid})
            continue
        if qn in keep:
            kept.append({"qn": qn, "rfq": rid, "action": "kept"})
            continue
        # Everything else gets cleaned — ghost or not
        if not dry_run:
            r["reytech_quote_number"] = ""
            r.pop("_manifest_id", None)
        cleaned.append({"qn": qn, "rfq": rid, "status": r.get("status", ""), "items": len(r.get("line_items", r.get("items", [])))})

    db_cleaned = 0
    if not dry_run:
        save_rfqs(rfqs)
        try:
            from src.core.db import get_db
            with get_db() as conn:
                for c in cleaned:
                    conn.execute("DELETE FROM quotes WHERE quote_number = ?", (c["qn"],))
                    conn.execute("DELETE FROM quote_number_ledger WHERE quote_number = ?", (c["qn"],))
                    db_cleaned += 1
                # Rename in DB too
                for rn in renamed:
                    conn.execute("UPDATE quotes SET quote_number = ? WHERE quote_number = ?",
                                 (rn["new"], rn["old"]))
                    conn.execute("UPDATE quote_number_ledger SET quote_number = ? WHERE quote_number = ?",
                                 (rn["new"], rn["old"]))
        except Exception as e:
            log.warning("DB quote cleanup: %s", e)

    counter_result = None
    if reset_to is not None and not dry_run:
        try:
            from src.forms.quote_generator import set_quote_counter
            set_quote_counter(int(reset_to))
            counter_result = f"Counter reset to {reset_to}"
        except Exception as e:
            counter_result = f"Counter reset failed: {e}"

    return jsonify({
        "ok": True,
        "dry_run": dry_run,
        "renamed": renamed,
        "cleaned": cleaned,
        "kept": kept,
        "db_cleaned": db_cleaned,
        "counter": counter_result,
    })


# ── Email Classification API (F6) ────────────────────────────────────────────

@bp.route("/api/email/review-queue")
@auth_required
def email_review_queue():
    """Get emails needing manual classification review."""
    try:
        from src.agents.email_classifier import get_review_queue
        limit = min(int(request.args.get("limit", 20)), 100)
        queue = get_review_queue(limit=limit)
        return jsonify({"ok": True, "count": len(queue), "queue": queue})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/email/classify-test", methods=["POST"])
@auth_required
def email_classify_test():
    """Test email classification on provided text (for debugging)."""
    try:
        from src.agents.email_classifier import classify_email
        from src.core.validators import sanitize_string
        data = request.get_json(silent=True) or {}
        result = classify_email(
            subject=sanitize_string(data.get("subject", ""), max_len=500),
            body=sanitize_string(data.get("body", ""), max_len=10000),
            sender=sanitize_string(data.get("sender", ""), max_len=200),
            attachment_names=data.get("attachments", [])[:20],
        )
        return jsonify({"ok": True, **result})
    except Exception as e:
        log.error("classify-test failed: %s", str(e)[:200])
        return jsonify({"ok": False, "error": str(e)})


# ── Margin Optimizer API (F7) ────────────────────────────────────────────────

@bp.route("/api/margins/summary")
@auth_required
def margins_summary():
    """Category-level margin stats, low-margin alerts, should-have-won detection."""
    try:
        from src.knowledge.margin_optimizer import get_margin_summary
        return jsonify(get_margin_summary())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/margins/item")
@auth_required
def margins_item():
    """Per-item pricing intelligence. GET ?description=gauze"""
    try:
        from src.knowledge.margin_optimizer import get_item_pricing
        desc = request.args.get("description", "")
        if len(desc) < 2:
            return jsonify({"ok": False, "error": "description required (min 2 chars)"})
        return jsonify(get_item_pricing(desc))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Order Lifecycle API (F8) ─────────────────────────────────────────────────

@bp.route("/api/orders/<order_id>/transition", methods=["POST"])
@auth_required
def order_transition(order_id):
    """Move an order to a new status. POST JSON: {status, notes, tracking_number, ...}"""
    try:
        from src.core.order_lifecycle import transition_order, ORDER_FLOW
        from src.core.validators import validate_required, validate_enum, sanitize_string, ValidationError
        data = request.get_json(silent=True) or {}
        try:
            new_status = validate_enum(data, "status", list(ORDER_FLOW.keys()))
        except ValidationError as ve:
            return jsonify({"ok": False, "error": str(ve)}), 400
        # Sanitize optional fields
        notes = sanitize_string(data.get("notes", ""), max_len=1000)
        tracking = sanitize_string(data.get("tracking_number", ""), max_len=200)
        clean_data = {k: v for k, v in data.items() if k not in ("status",)}
        if notes:
            clean_data["notes"] = notes
        if tracking:
            clean_data["tracking_number"] = tracking
        result = transition_order(order_id, new_status, actor="user", **clean_data)
        return jsonify(result)
    except Exception as e:
        log.error("Order transition %s failed: %s", order_id, str(e)[:200])
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/orders/<order_id>/detail")
@auth_required
def order_detail_api(order_id):
    """Full order detail with lifecycle timeline."""
    try:
        from src.core.order_lifecycle import get_order_detail
        return jsonify(get_order_detail(order_id))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/revenue/ytd")
@auth_required
def revenue_ytd():
    """YTD revenue from orders + revenue log. Includes overdue invoices."""
    try:
        from src.core.order_lifecycle import get_revenue_ytd
        return jsonify(get_revenue_ytd())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/growth/prospects")
@auth_required
def growth_prospects():
    """Scored prospect list from SCPRS data for outreach prioritization."""
    try:
        from src.agents.prospect_scorer import score_prospects
        limit = min(request.args.get("limit", 50, type=int), 200)
        return jsonify(score_prospects(limit=limit))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/orders/unpaid")
@auth_required
def unpaid_invoices():
    """Flag invoices older than N days without payment."""
    try:
        from src.core.order_lifecycle import get_revenue_ytd
        # Unpaid invoice query
        threshold = request.args.get("days", 30, type=int)
        now = __import__("datetime").datetime.now()
        cutoff = (now - __import__("datetime").timedelta(days=threshold)).isoformat()
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT id, quote_number, agency, institution, po_number,
                       total, status, updated_at
                FROM orders
                WHERE status IN ('invoiced', 'delivered')
                  AND updated_at < ? AND total > 0
                ORDER BY updated_at ASC
            """, (cutoff,)).fetchall()
            unpaid = [dict(r) for r in rows]
            total_outstanding = sum(r.get("total", 0) for r in unpaid)
        return jsonify({
            "ok": True, "count": len(unpaid),
            "total_outstanding": round(total_outstanding, 2),
            "threshold_days": threshold, "unpaid": unpaid
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── System Operations API (Sprint 5) ────────────────────────────────────────

@bp.route("/ver")
def public_version():
    """Public — returns deployed git commit. No auth needed. Use to verify Railway deployed latest code."""
    import subprocess as _sp
    try:
        commit = _sp.check_output(["git", "rev-parse", "--short", "HEAD"],
                                   stderr=_sp.DEVNULL).decode().strip()
    except Exception:
        commit = "04fc5bd"  # hardcoded — git not available at runtime in Railway
    return jsonify({"commit": commit, "expected": "04fc5bd",
                    "up_to_date": commit == "04fc5bd"})


@bp.route("/api/system/health")
@auth_required
def system_health():
    import os as _os
    health = {"status": "ok", "checks": {}}
    try:
        # DB health
        from src.core.db import get_db, DB_PATH
        with get_db() as conn:
            tables = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]
            db_size = _os.path.getsize(DB_PATH) if _os.path.exists(DB_PATH) else 0
            health["checks"]["database"] = {
                "ok": True, "tables": tables,
                "size_mb": round(db_size / 1048576, 1)
            }
    except Exception as e:
        health["checks"]["database"] = {"ok": False, "error": str(e)}
        health["status"] = "degraded"

    try:
        # Scheduler health
        from src.core.scheduler import get_scheduler_status
        sched = get_scheduler_status()
        dead = sched.get("dead_count", 0)
        health["checks"]["scheduler"] = {
            "ok": dead == 0, "jobs": sched.get("job_count", 0),
            "dead_jobs": dead
        }
        if dead > 0:
            health["status"] = "degraded"
    except Exception as e:
        health["checks"]["scheduler"] = {"ok": False, "error": str(e)}

    try:
        # Backup health
        from src.core.scheduler import backup_health
        bh = backup_health()
        health["checks"]["backups"] = bh
        if not bh.get("ok"):
            health["status"] = "degraded"
    except Exception as e:
        health["checks"]["backups"] = {"ok": False, "error": str(e)}

    try:
        # Schema migration status
        from src.core.migrations import get_migration_status
        ms = get_migration_status()
        health["checks"]["schema"] = {
            "ok": ms.get("up_to_date", False),
            "version": ms.get("current_version", 0),
            "pending": len(ms.get("pending", []))
        }
    except Exception as e:
        health["checks"]["schema"] = {"ok": False, "error": str(e)}

    return jsonify(health)


@bp.route("/api/system/migrations")
@auth_required
def migration_status():
    """Schema migration status and history."""
    try:
        from src.core.migrations import get_migration_status
        return jsonify(get_migration_status())
    except Exception as e:
        return jsonify({"error": str(e)})


@bp.route("/api/system/migrations/run", methods=["POST"])
@auth_required
def run_migrations_api():
    """Apply pending schema migrations."""
    try:
        from src.core.migrations import run_migrations
        result = run_migrations()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/system/integrity")
@auth_required
def data_integrity():
    """Run data integrity checks across all tables."""
    try:
        from src.core.data_integrity import run_integrity_checks
        return jsonify(run_integrity_checks())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/system/pdf-versions")
@auth_required
def pdf_template_versions():
    """Current PDF template versions and generation stats."""
    try:
        from src.forms.pdf_versioning import get_version_info, TEMPLATE_VERSIONS
        info = get_version_info()
        return jsonify({"ok": True, "templates": info,
                        "registry": {k: v["history"] for k, v in TEMPLATE_VERSIONS.items()}})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/system/trace/<doc_id>")
@auth_required
def trace_document_api(doc_id):
    """Trace a document through the full RFQ→Quote→Order pipeline.
    GET /api/system/trace/R26Q14?type=quote
    """
    from src.core.data_tracer import trace_document
    doc_type = request.args.get("type", "auto")
    return jsonify(trace_document(doc_id, doc_type=doc_type))


@bp.route("/api/system/pipeline")
@auth_required
def pipeline_stats():
    """Pipeline overview: counts and conversion rates across all stages."""
    from src.core.data_tracer import get_pipeline_stats
    return jsonify(get_pipeline_stats())


@bp.route("/api/system/qa")
@auth_required
def qa_dashboard():
    """QA dashboard — combined health, integrity, pipeline, and test status."""
    result = {"ok": True, "checked_at": datetime.now().isoformat(), "sections": {}}
    
    # 1. System health
    try:
        from src.core.db import get_db, DB_PATH
        import os as _os
        with get_db() as conn:
            tables = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]
            db_size = _os.path.getsize(DB_PATH) if _os.path.exists(DB_PATH) else 0
        result["sections"]["database"] = {
            "ok": True, "tables": tables,
            "size_mb": round(db_size / 1048576, 1)
        }
    except Exception as e:
        result["sections"]["database"] = {"ok": False, "error": str(e)}
        result["ok"] = False
    
    # 2. Data integrity
    try:
        from src.core.data_integrity import run_integrity_checks
        ic = run_integrity_checks()
        result["sections"]["integrity"] = {
            "ok": ic["ok"], "passed": ic["passed"], "failed": ic["failed"],
            "details": [c for c in ic["checks"] if not c["ok"]]
        }
        if not ic["ok"]:
            result["ok"] = False
    except Exception as e:
        result["sections"]["integrity"] = {"ok": False, "error": str(e)}
    
    # 3. Pipeline stats
    try:
        from src.core.data_tracer import get_pipeline_stats
        ps = get_pipeline_stats()
        result["sections"]["pipeline"] = ps
    except Exception as e:
        result["sections"]["pipeline"] = {"ok": False, "error": str(e)}
    
    # 4. Schema status
    try:
        from src.core.migrations import get_migration_status
        ms = get_migration_status()
        result["sections"]["schema"] = {
            "ok": ms.get("up_to_date", False),
            "version": ms.get("current_version"),
            "pending": len(ms.get("pending", []))
        }
    except Exception as e:
        result["sections"]["schema"] = {"ok": False, "error": str(e)}
    
    # 5. Route health
    try:
        from flask import current_app
        rules = list(current_app.url_map.iter_rules())
        result["sections"]["routes"] = {"ok": len(rules) > 500, "count": len(rules)}
    except Exception as e:
        result["sections"]["routes"] = {"ok": False, "error": str(e)}
    
    # 6. PDF template versions
    try:
        from src.forms.pdf_versioning import get_version_info
        result["sections"]["pdf_templates"] = get_version_info()
    except Exception as e:
        result["sections"]["pdf_templates"] = {"error": str(e)}
    
    return jsonify(result)


@bp.route("/api/system/preflight")
@auth_required
def system_preflight():
    """Combined pre-flight check: health + integrity + schema."""
    result = {"status": "ok", "checks": {}}
    
    # Health
    try:
        from src.core.db import get_db, DB_PATH
        import os as _os
        with get_db() as conn:
            tables = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]
            db_size = _os.path.getsize(DB_PATH) if _os.path.exists(DB_PATH) else 0
        result["checks"]["database"] = {"ok": True, "tables": tables,
                                         "size_mb": round(db_size / 1048576, 1)}
    except Exception as e:
        result["checks"]["database"] = {"ok": False, "error": str(e)}
        result["status"] = "degraded"

    # Schema
    try:
        from src.core.migrations import get_migration_status
        ms = get_migration_status()
        result["checks"]["schema"] = {
            "ok": ms.get("up_to_date", False),
            "version": ms.get("current_version"),
            "pending": len(ms.get("pending", []))
        }
    except Exception as e:
        result["checks"]["schema"] = {"ok": False, "error": str(e)}

    # Integrity
    try:
        from src.core.data_integrity import run_integrity_checks
        ic = run_integrity_checks()
        result["checks"]["integrity"] = {
            "ok": ic["ok"], "passed": ic["passed"],
            "failed": ic["failed"]
        }
        if not ic["ok"]:
            result["status"] = "degraded"
    except Exception as e:
        result["checks"]["integrity"] = {"ok": False, "error": str(e)}

    # Route count
    try:
        from flask import current_app
        rules = list(current_app.url_map.iter_rules())
        result["checks"]["routes"] = {"ok": len(rules) > 500, "count": len(rules)}
    except Exception as e:
        result["checks"]["routes"] = {"ok": False, "error": str(e)}

    return jsonify(result)


@bp.route("/api/system/routes")
@auth_required
def api_route_map():
    """Auto-generated API documentation — all routes with methods."""
    from flask import current_app
    routes = []
    for rule in sorted(current_app.url_map.iter_rules(), key=lambda r: r.rule):
        if rule.rule.startswith("/static"):
            continue
        methods = sorted(rule.methods - {"HEAD", "OPTIONS"})
        routes.append({
            "path": rule.rule,
            "methods": methods,
            "endpoint": rule.endpoint,
        })
    return jsonify({
        "total": len(routes),
        "api_routes": [r for r in routes if r["path"].startswith("/api/")],
        "page_routes": [r for r in routes if not r["path"].startswith("/api/")],
    })


# Route Modules — loaded at import time, register routes onto this Blueprint
# Split from dashboard.py for maintainability (was 13,831 lines)
# ══════════════════════════════════════════════════════════════════════════════

# Cross-module globals: defined in later modules, used by earlier ones.
# Pre-define defaults so modules loaded first can reference them safely.
# These get overwritten to True when the defining module loads successfully.
INTEL_AVAILABLE = False
PREDICT_AVAILABLE = False
QB_AVAILABLE = False
GROWTH_AVAILABLE = False
OUTREACH_AVAILABLE = False
VOICE_AVAILABLE = False
CAMPAIGNS_AVAILABLE = False
SCANNER_AVAILABLE = False
LEADGEN_AVAILABLE = False
ITEM_ID_AVAILABLE = False
REPLY_ANALYZER_AVAILABLE = False
QA_AVAILABLE = False
MANAGER_AVAILABLE = False
ORCHESTRATOR_AVAILABLE = False
CATALOG_AVAILABLE = False
_WF_AVAILABLE = False

def _load_route_module(module_name: str):
    """
    Load a route module using importlib (not exec).
    Injects dashboard globals so route functions can reference bp, auth_required, etc.
    Route registrations (@bp.route) happen during exec_module.
    """
    import importlib.util
    module_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "modules", f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(
        f"src.api.modules.{module_name}", module_path)
    mod = importlib.util.module_from_spec(spec)
    # Inject shared dashboard globals into module (preserves existing behavior)
    _shared = {k: v for k, v in globals().items()
               if not k.startswith('_load_route_module')}
    # Save module identity before injection (Python 3.12+ checks name match)
    _saved_name = mod.__name__
    _saved_spec = mod.__spec__
    _saved_file = getattr(mod, '__file__', None)
    mod.__dict__.update(_shared)
    # Restore identity so loader can find the right module
    mod.__name__ = _saved_name
    mod.__spec__ = _saved_spec
    if _saved_file:
        mod.__file__ = _saved_file
    import sys as _sys
    spec.loader.exec_module(mod)
    _sys.modules[spec.name] = mod  # Cache AFTER exec so regular imports don't re-execute and double-register routes
    # Copy new definitions back so later modules can reference them
    for k, v in mod.__dict__.items():
        if not k.startswith('__') and k not in _shared:
            globals()[k] = v
    log.debug("Route module loaded: %s (%d new symbols)", module_name,
              sum(1 for k in mod.__dict__ if not k.startswith('__') and k not in _shared))


_ROUTE_MODULES = [
    "routes_rfq",              # Home, upload, RFQ pages, quote generation
    "routes_agents",           # Agent control panel, email templates
    "routes_pricecheck",       # Price check pages + lookup
    "routes_crm",              # CRM, pricing oracle, auto-processor
    "routes_intel",            # SCPRS, CCHCS, vendors, funnel, forecasting
    "routes_growth_prospects",  # Growth strategy, prospect management, campaigns (split from routes_intel)
    "routes_orders_full",      # Orders, supplier lookup, quote-order link, invoice
    "routes_voice_contacts",   # Intelligence page, voice, contacts, campaigns
    "routes_catalog_finance",  # Catalog, shipping, pricing, margins, payments, audit
    "routes_prd28",           # PRD-28: Quote lifecycle, email overhaul, leads, revenue, vendor intel
    "routes_analytics",       # PRD-29: Pipeline analytics, buyer intel, margin optimizer, settings, API v1
    "routes_order_tracking",  # PRD-29: PO tracking, separate email inbox, line item lifecycle
    "routes_orders_enhance",  # Order enhancements: timeline, margins, aging, KPI, reorder, proofs
    "routes_growth_intel",    # Features #8,10,11,13: Catalog growth, price alerts, win/loss, outreach
    "routes_v1",              # MCP-ready /api/v1/ endpoints for external AI agents
    "routes_search",          # Universal search across all record types
]

for _mod in _ROUTE_MODULES:
    try:
        _load_route_module(_mod)
    except Exception as _e:
        log.error(f"Failed to load route module {_mod}: {_e}")
        import traceback; traceback.print_exc()

log.info(f"Dashboard: {len(_ROUTE_MODULES)} route modules loaded, {len([r for r in bp.deferred_functions])} deferred fns")

# ── Award Monitor merged into Award Tracker (single thread) ─────────────

# ── Boot Health Check — deferred to background thread so app starts fast ──
def _deferred_boot_checks():
    """Run health check + recovery in background so boot doesn't hang."""
    import time as _bt
    _bt.sleep(10)  # Let the app start first
    try:
        from src.core.data_guard import boot_health_check
        _boot_health = boot_health_check()
        if not _boot_health["ok"]:
            log.error("BOOT HEALTH ISSUES: %s", _boot_health["issues"])
    except Exception as _e:
        log.warning("Boot health check failed: %s", _e)

    # Boot Recovery (inside deferred thread)
    for _json_name, _table, _id_col in [("price_checks.json", "price_checks", "id"),
                                          ("rfqs.json", "rfqs", "id")]:
        try:
            _jpath = os.path.join(DATA_DIR, _json_name)
            _is_empty = True
            if os.path.exists(_jpath) and os.path.getsize(_jpath) > 100:
                try:
                    with open(_jpath) as _jf:
                        _jdata = json.load(_jf)
                    if _jdata and len(_jdata) > 0:
                        _is_empty = False
                except Exception:
                    pass
            if _is_empty:
                log.warning("BOOT: %s empty — rebuilding from SQLite", _json_name)
                from src.core.db import get_db as _bdb
                with _bdb() as _bc:
                    if _table == "price_checks":
                        _rows = _bc.execute("SELECT id, pc_data FROM price_checks WHERE pc_data IS NOT NULL").fetchall()
                        _rebuilt = {}
                        for _r in _rows:
                            try:
                                _pd = json.loads(_r[1]) if isinstance(_r[1], str) else _r[1]
                                if isinstance(_pd, dict):
                                    _rebuilt[_r[0]] = {k: v for k, v in _pd.items() if k != "pc_data"}
                            except Exception:
                                pass
                    else:
                        _rows = _bc.execute("SELECT * FROM rfqs").fetchall()
                        _rebuilt = {}
                        for _r in _rows:
                            _d = dict(_r)
                            _items = json.loads(_d.get("items", "[]")) if isinstance(_d.get("items"), str) else _d.get("items", [])
                            _rebuilt[_d["id"]] = {"id": _d["id"], "line_items": _items, "items": _items,
                                                   "status": _d.get("status", ""), "requestor_name": _d.get("requestor_name", ""),
                                                   "requestor_email": _d.get("requestor_email", ""), "solicitation_number": _d.get("solicitation_number", _d.get("rfq_number", "")),
                                                   "agency": _d.get("agency", ""), "source": _d.get("source", "")}
                    if _rebuilt:
                        from src.core.data_guard import safe_save_json
                        safe_save_json(_jpath, _rebuilt, reason="boot_recovery")
                        log.info("BOOT: Rebuilt %s: %d records", _json_name, len(_rebuilt))
        except Exception as _re:
            log.warning("BOOT: %s recovery failed: %s", _json_name, _re)

import threading as _boot_thr
_boot_thr.Thread(target=_deferred_boot_checks, daemon=True, name="boot-checks").start()
log.info("Boot checks deferred to background (app starts immediately)")

# ── Background agent schedulers (disabled in tests via ENABLE_BACKGROUND_AGENTS=false) ──
if os.environ.get("ENABLE_BACKGROUND_AGENTS", "true").lower() not in ("false", "0", "off"):
    # ── Start Follow-Up Engine (auto-creates follow-up drafts) ──────────────
    try:
        from src.agents.follow_up_engine import start_follow_up_scheduler
        start_follow_up_scheduler()
        log.info("Follow-up engine started (scans every 1h)")
    except Exception as _e:
        log.warning("Follow-up engine failed to start: %s", _e)

    # ── Start Award Tracker (polls SCPRS 3x/day for PO awards) ──────────────
    try:
        from src.agents.award_tracker import start_award_tracker
        start_award_tracker()
        log.info("Award tracker started (polls SCPRS every 8h)")
    except Exception as _e:
        log.warning("Award tracker failed to start: %s", _e)

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

    # ── Start Order Digest Scheduler (daily digest + tracking checks) ────────
    try:
        from src.agents.order_digest import start_order_digest_scheduler
        start_order_digest_scheduler()
        log.info("Order digest scheduler started (every 4h, digest at 8am)")
    except Exception as _e:
        log.warning("Order digest scheduler failed to start: %s", _e)

    # ── Start Lead Nurture Scheduler (drip sequences + rescoring) ────────────
    try:
        from src.agents.lead_nurture_agent import start_nurture_scheduler
        start_nurture_scheduler()
        log.info("Lead nurture scheduler started (daily)")
    except Exception as _e:
        log.warning("Lead nurture scheduler failed to start: %s", _e)

    # ── Start Google Drive Backup Scheduler (nightly at 11pm PST) ────────────
    try:
        from src.agents.drive_backup import start_backup_scheduler
        start_backup_scheduler()
        log.info("Drive backup scheduler started (nightly at 11pm PST)")
    except Exception as _e:
        log.warning("Drive backup scheduler failed to start: %s", _e)

    # ── Start PO Tracking Email Poller (auto-updates order status from vendor emails) ──
    try:
        # _start_po_poller is already in namespace from exec'd routes_order_tracking
        _start_po_poller()
        log.info("PO tracking poller started (checks vendor emails every 5min)")
    except Exception as _e:
        log.warning("PO tracking poller failed to start: %s", _e)

    # ── Start Invoice Poller (picks up QB invoice emails, enhances PDF) ─────
    try:
        from src.agents.invoice_processor import start_invoice_poller
        start_invoice_poller()
    except Exception as _e:
        log.warning("Invoice poller failed to start: %s", _e)

    # ── FI$Cal Exhaustive Scrape (2AM PST nightly) ──────────────
    try:
        from src.agents.scprs_browser import schedule_full_fiscal_scrape
        schedule_full_fiscal_scrape(target_hour_pst=2)
        log.info("FI$Cal exhaustive scrape scheduled for 2:00 AM PST")
    except Exception as _e:
        log.warning("FI$Cal scrape scheduler failed: %s", _e)

    # ── System Auditor (5:30 AM PST after data pull) ────────────
    try:
        from src.agents.system_auditor import schedule_system_audit
        schedule_system_audit()
        log.info("System audit scheduled for 5:30 AM PST")
    except Exception as _e:
        log.warning("System auditor failed: %s", _e)

    # ── Auto-populate catalog on boot if empty ──────────────────
    try:
        from src.core.db import get_db as _get_db
        with _get_db() as _db:
            # Ensure table exists before querying
            _db.execute("""CREATE TABLE IF NOT EXISTS scprs_catalog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                description TEXT NOT NULL,
                last_unit_price REAL,
                last_quantity INTEGER DEFAULT 1,
                last_supplier TEXT DEFAULT '',
                last_department TEXT DEFAULT '',
                last_po_number TEXT DEFAULT '',
                last_date TEXT DEFAULT '',
                times_seen INTEGER DEFAULT 1,
                updated_at TEXT DEFAULT ''
            )""")
            _cat = _db.execute("SELECT COUNT(*) FROM scprs_catalog").fetchone()[0]
            if _cat == 0:
                log.info("Catalog empty — populating from won_quotes...")
                try:
                    from src.knowledge.won_quotes_db import get_db as _get_wq
                    _wq = _get_wq()
                    _rows = _wq.execute(
                        "SELECT description, unit_price, quantity, supplier, "
                        "department, po_number, award_date FROM won_quotes WHERE unit_price > 0"
                    ).fetchall()
                    for _r in _rows:
                        _desc = (_r[0] or "")[:500]
                        if not _desc or not _r[1] or _r[1] <= 0:
                            continue
                        try:
                            _db.execute(
                                "INSERT OR IGNORE INTO scprs_catalog "
                                "(description, last_unit_price, last_quantity, "
                                "last_supplier, last_department, last_po_number, "
                                "last_date, times_seen, updated_at) "
                                "VALUES (?,?,?,?,?,?,?,1,datetime('now'))",
                                (_desc, _r[1], _r[2] or 1, _r[3] or "", _r[4] or "", _r[5] or "", _r[6] or ""))
                        except Exception:
                            pass
                    _db.commit()
                    log.info("Catalog populated: %d items",
                             _db.execute("SELECT COUNT(*) FROM scprs_catalog").fetchone()[0])
                except Exception as _e2:
                    log.warning("Catalog population failed: %s", _e2)
            else:
                log.info("Catalog has %d items", _cat)
    except Exception as _e:
        log.warning("Catalog check failed: %s", _e)

    # ── Due Date Reminders (hourly, SMS + bell) ──────────────────
    try:
        from src.agents.due_date_reminder import start_reminder_scheduler
        start_reminder_scheduler()
        log.info("Due date reminder scheduler started (hourly)")
    except Exception as _e:
        log.warning("Due date reminders failed: %s", _e)

    # ── Daily disk cleanup (snapshots + PO records) ────────────
    try:
        import threading as _thr_cleanup
        def _daily_cleanup():
            import time as _tc
            _tc.sleep(300)  # 5 min after boot
            while True:
                try:
                    from src.core.data_guard import cleanup_old_snapshots
                    removed = cleanup_old_snapshots(days=7)
                    if removed:
                        log.info("Daily cleanup: %d old snapshots removed", removed)
                    from src.agents.scprs_browser import _cleanup_old_po_records
                    po_removed = _cleanup_old_po_records()
                    if po_removed:
                        log.info("Daily cleanup: %d old PO records removed", po_removed)
                except Exception as _ce:
                    log.debug("Daily cleanup: %s", _ce)
                _tc.sleep(86400)
        _thr_cleanup.Thread(target=_daily_cleanup, daemon=True, name="daily-cleanup").start()
        log.info("Daily disk cleanup started")
    except Exception as _e:
        log.warning("Daily cleanup setup: %s", _e)

    # ── Form Updater (1st + 15th of month, 3AM PST) ──────────
    try:
        def _form_update_scheduler():
            import time as _fut
            _fut.sleep(300)
            while True:
                try:
                    _pst = timezone(timedelta(hours=-8))
                    _now = datetime.now(_pst)
                    if _now.day in (1, 15) and 2 <= _now.hour <= 4:
                        _skip = False
                        try:
                            from src.core.usage_tracker import get_recent_activity
                            if get_recent_activity(minutes=30) > 0:
                                _skip = True
                        except Exception:
                            pass
                        if not _skip:
                            from src.agents.form_updater import update_all_forms
                            _result = update_all_forms()
                            if _result.get("updated", 0) > 0:
                                log.info("Form updater: %d forms updated", _result["updated"])
                            _fut.sleep(86400)
                            continue
                    _fut.sleep(3600)
                except Exception as _fe:
                    log.warning("Form updater: %s", _fe)
                    _fut.sleep(3600)
        threading.Thread(target=_form_update_scheduler, daemon=True, name="form-updater").start()
        log.info("Form updater scheduled (1st + 15th, 3AM PST)")
    except Exception as _e:
        log.warning("Form updater setup: %s", _e)

    # ── Backfill RFQ metadata on boot ──────────────────────────
    try:
        _bf_meta = backfill_rfq_metadata()
        if _bf_meta.get("updated"):
            log.info("RFQ metadata backfill: %d RFQs updated — %s", _bf_meta["updated"], _bf_meta.get("details", []))
    except Exception as _e:
        log.warning("RFQ metadata backfill failed: %s", _e)

    # ── Backfill item memory from existing PCs/quotes ──────────
    try:
        from src.core.pricing_oracle_v2 import backfill_item_memory
        _bf = backfill_item_memory()
        log.info("Item memory backfill: %d items learned", _bf)
    except Exception as _e:
        log.warning("Item memory backfill failed: %s", _e)

else:
    log.info("Background agents disabled via ENABLE_BACKGROUND_AGENTS=false")


# ═══════════════════════════════════════════════════════════════════════════════
# Force Recapture — guaranteed to load (not in exec'd module)
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/force-recapture-legacy", methods=["POST", "GET"])  # Primary handler in routes_pricecheck.py
@auth_required
def _force_recapture():
    """LEGACY — primary /api/force-recapture is in routes_pricecheck.py."""
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
            # Clear SQLite processed_emails + fingerprints (layers 3+4)
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    conn.execute("DELETE FROM processed_emails")
                    try:
                        conn.execute("DELETE FROM email_fingerprints")
                    except Exception:
                        pass
            except Exception:
                pass
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
@auth_required
def api_email_trace():
    """Show processed UIDs state and allow nuclear clear."""
    global _shared_poller
    
    action = request.args.get("action", "")
    
    # Load processed UIDs from disk
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    try:
        with open(proc_file) as f:
            processed_disk = json.load(f)
    except Exception:
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
        except Exception:
            pass
        
        # 4. Clear SQLite processed_emails table (layer 3)
        db_cleared = 0
        try:
            from src.core.db import get_db
            with get_db() as conn:
                db_cleared = conn.execute("SELECT COUNT(*) FROM processed_emails").fetchone()[0]
                conn.execute("DELETE FROM processed_emails")
        except Exception:
            pass
        
        # 5. Clear email_fingerprints table (layer 4 — cross-inbox dedup)
        fp_cleared = 0
        try:
            from src.core.db import get_db
            with get_db() as conn:
                try:
                    fp_cleared = conn.execute("SELECT COUNT(*) FROM email_fingerprints").fetchone()[0]
                    conn.execute("DELETE FROM email_fingerprints")
                except Exception:
                    pass
        except Exception:
            pass
        
        # 6. Kill poller so next Check Now creates fresh one
        _shared_poller = None
        
        # 7. Unpause
        POLL_STATUS["paused"] = False
        
        return jsonify({
            "ok": True,
            "action": "NUKED",
            "cleared_memory": old_mem,
            "cleared_disk": old_disk,
            "cleared_db": db_cleared,
            "cleared_fingerprints": fp_cleared,
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
    except Exception:
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


@bp.route("/api/health/startup")
@auth_required
def api_health_startup():
    """Startup health check for home page banner."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            pc_count = conn.execute("SELECT COUNT(*) FROM price_checks").fetchone()[0]
            rfq_count = conn.execute("SELECT COUNT(*) FROM rfqs").fetchone()[0]
        warnings = []
        if not os.environ.get("GMAIL_ADDRESS_2"):
            warnings.append("GMAIL_ADDRESS_2 not set — mike@ inbox not being polled")
        if not os.environ.get("GMAIL_PASSWORD_2"):
            warnings.append("GMAIL_PASSWORD_2 not set — mike@ inbox not being polled")
        return jsonify({
            "ok": True,
            "price_checks": pc_count,
            "rfqs": rfq_count,
            "status": "healthy",
            "warnings": warnings,
            "mike_inbox_configured": bool(os.environ.get("GMAIL_ADDRESS_2") and os.environ.get("GMAIL_PASSWORD_2")),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/disk-cleanup")
@auth_required
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
                except Exception:
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
        except Exception:
            pass
        try:
            pcs = _load_price_checks()
            for pc in pcs.values():
                d = pc.get("rfq_dir", "")
                if d:
                    active_dirs.add(os.path.basename(d))
        except Exception:
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

    if action == "vacuum":
        # VACUUM the database to reclaim space
        import sqlite3 as _sq
        db_path = os.path.join(DATA_DIR, "reytech.db")
        before = os.path.getsize(db_path)
        try:
            vc = _sq.connect(db_path, timeout=120)
            vc.execute("VACUUM")
            vc.close()
            after = os.path.getsize(db_path)
            result["action"] = "vacuumed"
            result["db_before_mb"] = round(before / 1024 / 1024, 1)
            result["db_after_mb"] = round(after / 1024 / 1024, 1)
            result["freed_mb"] = round((before - after) / 1024 / 1024, 1)
            log.info("VACUUM: %.1fMB → %.1fMB (freed %.1fMB)", before/1048576, after/1048576, (before-after)/1048576)
        except Exception as e:
            result["action"] = "vacuum_failed"
            result["error"] = str(e)
    
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
                except Exception:
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

