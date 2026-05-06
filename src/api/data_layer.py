"""
data_layer.py — Stable import interface for data access functions.

These functions live in dashboard.py but need to be independently importable
for testing, agents, and eventual Blueprint migration. This module provides
a clean import path without circular dependency issues.

Usage:
    from src.api.data_layer import load_rfqs, _save_single_rfq, _load_price_checks

Instead of:
    from src.api.dashboard import load_rfqs  # fragile, circular import risk
"""

# ── Lazy imports to avoid circular dependency with dashboard.py ──────────────
# Dashboard.py is the source of truth until functions are migrated here.
# Each function is lazily imported on first call.

import os
import json
import threading
import time
import logging
import re
import collections

log = logging.getLogger("reytech.data_layer")

# ── Path constants (from config.py — no dashboard dependency) ────────────────
from src.api.config import DATA_DIR, UPLOAD_DIR, OUTPUT_DIR, BASE_DIR

# ── Thread locks (owned by this module, shared with dashboard) ───────────────
_json_cache_lock = threading.Lock()
# RLock so a single thread can acquire across nested load+mutate+save calls
# without deadlocking on _save_single_rfq's internal acquire. The autosave
# handler wraps load_rfqs → mutate → _save_single_rfq in this lock to close
# the read-modify-write race that overwrote in-flight operator edits when
# two close-together autosaves both loaded the same stale snapshot. (Mike P0
# 2026-05-06 RFQ a5b09b56.)
_save_rfqs_lock = threading.RLock()
_save_pcs_lock = threading.RLock()

# ── TTL JSON cache ───────────────────────────────────────────────────────────
_json_cache: dict = {}
_JSON_CACHE_TTL = 2.0


def _cached_json_load(path: str, fallback=None):
    """Load JSON with mtime-aware caching. Thread-safe."""
    if fallback is None:
        fallback = {}
    if not os.path.exists(path):
        return fallback
    try:
        mtime = os.path.getmtime(path)
        now = time.time()
        with _json_cache_lock:
            cached = _json_cache.get(path)
            if cached and cached["mtime"] == mtime and (now - cached["ts"]) < 10.0:
                return cached["data"]
        with open(path) as f:
            data = json.load(f)
        with _json_cache_lock:
            _json_cache[path] = {"data": data, "mtime": mtime, "ts": now}
            if len(_json_cache) > 50:
                oldest = sorted(_json_cache.items(), key=lambda x: x[1]["ts"])[:10]
                for k, _ in oldest:
                    del _json_cache[k]
        return data
    except (json.JSONDecodeError, OSError):
        return fallback


def _invalidate_cache(path: str):
    """Evict a stale cache entry after writing a JSON file."""
    with _json_cache_lock:
        _json_cache.pop(path, None)


# ── Utility functions ────────────────────────────────────────────────────────

def _pst_now_iso():
    """Return current Pacific datetime as ISO string (PST/PDT aware)."""
    from zoneinfo import ZoneInfo
    from datetime import datetime
    return datetime.now(ZoneInfo("America/Los_Angeles")).isoformat()


def _safe_filename(filename: str) -> str:
    """Sanitize a user-provided filename."""
    if not filename:
        return "upload.pdf"
    filename = os.path.basename(filename.replace('\\', '/'))
    filename = re.sub(r'[^\w\-_\. ]', '_', filename)
    filename = filename.strip('. ')
    if len(filename) > 120:
        filename = filename[:120]
    return filename or "upload.pdf"


def _safe_path(user_path: str, allowed_base: str) -> str:
    """Resolve a user-supplied path within an allowed base directory."""
    try:
        base = os.path.realpath(allowed_base)
        full = os.path.realpath(os.path.join(base, os.path.basename(user_path)))
        if not full.startswith(base + os.sep) and full != base:
            raise ValueError(f"Path traversal blocked: {user_path!r}")
        return full
    except Exception as e:
        raise ValueError(f"Invalid path: {e}")


def _validate_pdf_path(pdf_path: str) -> str:
    """Validate a PDF path is within DATA_DIR."""
    if not pdf_path:
        raise ValueError("No pdf_path provided")
    return _safe_path(pdf_path, DATA_DIR)


def _sanitize_input(value: str, max_length: int = 500, allow_html: bool = False) -> str:
    """Sanitize user input."""
    if not isinstance(value, str):
        return str(value)[:max_length] if value else ""
    value = value[:max_length]
    if not allow_html:
        value = re.sub(r'<[^>]+>', '', value)
        value = value.replace('../', '').replace('..\\', '')
        value = value.replace('\x00', '')
    return value.strip()


def _sanitize_path(path_str: str) -> str:
    """Sanitize file path input."""
    if not path_str:
        return ""
    clean = os.path.basename(path_str)
    clean = re.sub(r'[^\w\-.]', '_', clean)
    return clean


# ── Notifications ────────────────────────────────────────────────────────────
_notifications = collections.deque(maxlen=20)


def _push_notification(notif: dict):
    from datetime import datetime
    notif.setdefault("ts", datetime.now().isoformat())
    notif.setdefault("read", False)
    _notifications.appendleft(notif)
    log.info("Notification: [%s] %s", notif.get("type", ""), notif.get("title", ""))


# ── RFQ path helper ──────────────────────────────────────────────────────────
def rfq_db_path():
    return os.path.join(DATA_DIR, "rfqs.json")


# ── Delegate functions (import from dashboard until fully migrated) ──────────
# These are here so route modules and tests can do:
#   from src.api.data_layer import load_rfqs
# without knowing the function lives in dashboard.py.

def _get_dashboard_fn(name):
    """Lazy-import a function from dashboard.py to avoid circular imports."""
    import importlib
    mod = importlib.import_module("src.api.dashboard")
    return getattr(mod, name)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA ACCESS — Real implementations (migrated from dashboard.py)
# ═══════════════════════════════════════════════════════════════════════════════

# ── PC cache (module-level globals) ──
_pc_cache = None
_pc_cache_time = 0


def _normalize_rfq_fields(rfqs: dict) -> dict:
    """Ensure both field name aliases exist on every RFQ dict."""
    for rid, r in rfqs.items():
        if not isinstance(r, dict):
            continue
        if "items" in r and "line_items" not in r:
            r["line_items"] = r["items"]
        if "line_items" in r and "items" not in r:
            r["items"] = r["line_items"]
        if "rfq_number" in r and "solicitation_number" not in r:
            r["solicitation_number"] = r["rfq_number"]
        if "solicitation_number" in r and "rfq_number" not in r:
            r["rfq_number"] = r["solicitation_number"]
    return rfqs


def load_rfqs():
    """Load RFQs from SQLite (single source of truth)."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM rfqs ORDER BY received_at DESC LIMIT 10000"
            ).fetchall()
            if not rows:
                json_path = rfq_db_path()
                if os.path.exists(json_path):
                    try:
                        with open(json_path) as f:
                            json_data = json.load(f)
                        if json_data:
                            log.info("MIGRATION: Importing %d RFQs from JSON to SQLite", len(json_data))
                            for rid, r in json_data.items():
                                try:
                                    _save_single_rfq(rid, r)
                                except Exception as _e:
                                    log.debug("suppressed: %s", _e)
                            os.rename(json_path, json_path + ".migrated")
                            return _normalize_rfq_fields(json_data)
                    except Exception as e:
                        log.warning("RFQ JSON migration failed: %s", e)
                return _normalize_rfq_fields({})

            result = {}
            for row in rows:
                d = dict(row)
                rid = d.get("id", "")
                if not rid:
                    continue
                blob = d.pop("data_json", None)
                if blob:
                    try:
                        full = json.loads(blob)
                        for key in ("status", "updated_at", "reytech_quote_number"):
                            if d.get(key) and d[key] != full.get(key):
                                full[key] = d[key]
                        result[rid] = full
                        continue
                    except (json.JSONDecodeError, TypeError) as _e:
                        log.debug("suppressed: %s", _e)
                items_raw = d.get("items", "[]")
                if isinstance(items_raw, str):
                    try:
                        d["items"] = json.loads(items_raw)
                    except Exception:
                        d["items"] = []
                result[rid] = d
            return _normalize_rfq_fields(result)
    except Exception as e:
        log.warning("load_rfqs failed: %s", str(e)[:200])
    return _normalize_rfq_fields({})


def _save_single_rfq(rfq_id, r, raise_on_error=False):
    """Save a SINGLE RFQ to SQLite.

    raise_on_error: when True, propagate DB failures to the caller instead of
    logging and swallowing. User-facing save endpoints must set this so they
    can surface failures to the UI — otherwise the response lies about
    persistence and data is silently lost (same failure mode as the 2026-04-16
    PC session).
    """
    # Parity with _save_single_pc: every RFQ carries a deadline.
    try:
        from src.core.deadline_defaults import apply_default_if_missing
        apply_default_if_missing(r)
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    with _save_rfqs_lock:
        p = rfq_db_path()
        _invalidate_cache(p)
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO rfqs
                    (id, received_at, agency, institution, requestor_name, requestor_email,
                     rfq_number, items, status, source, email_uid, notes,
                     solicitation_number, due_date, email_subject, body_text, form_type,
                     reytech_quote_number, shipping_option, shipping_amount, delivery_location,
                     updated_at, data_json)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),?)
                """, (
                    rfq_id, r.get("received_at", ""), r.get("agency", ""),
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
            log.error("DB save_single_rfq failed for %s: %s", rfq_id, e)
            if raise_on_error:
                raise


def save_rfqs(rfqs, raise_on_error=False):
    """Save ALL RFQs.

    raise_on_error: when True, propagate DB failures so user-facing routes can
    return ok:false instead of pretending success. Default preserves legacy
    log-and-swallow for background agents (email poller, intake, etc.) that
    don't have a user waiting on the response.
    """
    with _save_rfqs_lock:
        p = rfq_db_path()
        _invalidate_cache(p)
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
            if raise_on_error:
                raise


def _load_price_checks(include_items=True):
    """Load price checks from SQLite (single source of truth)."""
    global _pc_cache, _pc_cache_time
    import time as _t
    now = _t.time()
    if include_items and _pc_cache is not None and (now - _pc_cache_time) < 30:
        return _pc_cache

    data = {}
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
                    except (json.JSONDecodeError, TypeError) as _e:
                        log.debug("suppressed: %s", _e)
                items_raw = d.get("items", "[]")
                if isinstance(items_raw, str):
                    try:
                        d["items"] = json.loads(items_raw)
                    except Exception:
                        d["items"] = []
                data[pcid] = d
    except Exception as e:
        log.warning("SQLite load_pcs failed: %s", str(e)[:200])

    # One-time migration from JSON
    if not data:
        json_path = os.path.join(DATA_DIR, "price_checks.json")
        if os.path.exists(json_path):
            try:
                with open(json_path) as f:
                    json_data = json.load(f)
                if json_data:
                    log.info("MIGRATION: Importing %d PCs from JSON to SQLite", len(json_data))
                    for pcid, pc in json_data.items():
                        try:
                            _save_single_pc(pcid, pc)
                        except Exception as _e:
                            log.debug("suppressed: %s", _e)
                    data = json_data
                    os.rename(json_path, json_path + ".migrated")
            except Exception as e:
                log.warning("PC JSON migration failed: %s", e)

    # Sync items/line_items aliases — `items` is the canonical column on
    # `price_checks`, so when the blob's `line_items` (or parsed.line_items)
    # has drifted from it, force-realign here. 2026-05-05 incident
    # (pc_177b18e6): quote_model_v2_enabled adapter read stale `line_items`
    # while saved-prices wrote to `items`, blanking a row in the UI.
    for pcid, pc in data.items():
        if not isinstance(pc, dict):
            continue
        if "items" in pc:
            pc["line_items"] = list(pc["items"])
            if isinstance(pc.get("parsed"), dict):
                pc["parsed"]["line_items"] = list(pc["items"])
        elif "line_items" in pc:
            pc["items"] = list(pc["line_items"])

    if include_items and len(data) < 500:
        _pc_cache = data
        _pc_cache_time = _t.time()
    elif include_items:
        log.warning("PC cache skipped: %d records exceeds 500 limit", len(data))
        _pc_cache = None
        _pc_cache_time = 0
    return data


def _save_single_pc(pc_id, pc, raise_on_error=False):
    """Save a SINGLE price check to SQLite.

    Auto-tags CCHCS Non-IT RFQ packets with packet_type=cchcs_non_it
    before persisting. Centralizing at the save layer means every PC
    that hits the DB gets tagged, regardless of which ingest path
    created it (email poller, manual upload, REST admin, test harness).
    tag_pc_if_packet is idempotent and defensive (returns False on any
    error) so it cannot break the save path.

    raise_on_error: when True, propagate DB failures to the caller instead
    of logging and swallowing. User-facing save endpoints must set this so
    they can surface failures to the UI — otherwise the response lies about
    persistence and data is silently lost.
    """
    try:
        from src.agents.cchcs_packet_detector import tag_pc_if_packet
        tag_pc_if_packet(pc)
    except Exception as _e:
        log.debug("suppressed: %s", _e)  # never let tagging break a save

    # Ensure every PC carries a deadline (header → email → now+2 biz days).
    # Centralized here so all ingest paths + admin edits + test harnesses
    # get the same default, matching the tag_pc_if_packet pattern above.
    try:
        from src.core.deadline_defaults import apply_default_if_missing
        apply_default_if_missing(pc)
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # Sync alias keys to the canonical `items` BEFORE serialization — the
    # SQL column `items` and the embedded blob (data_json/pc_data) must
    # tell the same story or downstream readers (quote_adapter, list views,
    # exports) silently see different line-item sets. 2026-05-05 incident
    # pc_177b18e6: a save updated `items` but `line_items` stayed stale
    # in the blob → quote_model_v2 adapter read the stale alias → row
    # blanked in UI. Mike had to flip `quote_model_v2_enabled` off to
    # mitigate. This realign closes the underlying divergence.
    if isinstance(pc, dict) and "items" in pc:
        pc["line_items"] = list(pc["items"])
        if isinstance(pc.get("parsed"), dict):
            pc["parsed"]["line_items"] = list(pc["items"])
    elif isinstance(pc, dict) and "line_items" in pc:
        pc["items"] = list(pc["line_items"])

    with _save_pcs_lock:
        global _pc_cache, _pc_cache_time

        def _do():
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

        try:
            from src.core.db import db_retry
            db_retry(_do, max_retries=5, delay=2.0)
        except Exception as e:
            log.error("DB save_single_pc failed for %s: %s", pc_id, e)
            if raise_on_error:
                raise
        finally:
            # Invalidate AFTER write commits. Invalidating before lets a
            # concurrent reader populate the cache with pre-write state and
            # serve stale data for 30s — banner-missing race seen in prod
            # smoke 2026-04-19.
            _pc_cache = None
            _pc_cache_time = 0
            # Also invalidate the home page combined-init cache. 2026-05-06
            # incident: Mike marked a PC as duplicate but "Ready to Review"
            # stayed at 2 for the full 90s cache window because /api/dashboard/init
            # holds its own snapshot. Reach in defensively — module load order
            # means the cache may not exist yet when this fires.
            try:
                from src.api.modules import routes_prd28 as _rprd
                if hasattr(_rprd, "_dash_init_cache"):
                    _rprd._dash_init_cache["data"] = None
                    _rprd._dash_init_cache["ts"] = 0
            except Exception as _e:
                log.debug("dash_init_cache invalidation suppressed: %s", _e)


def _save_price_checks(pcs, raise_on_error=False):
    """Save ALL price checks to SQLite.

    raise_on_error: when True, propagate DB failures so user-facing routes can
    return ok:false. Default preserves legacy log-and-swallow for background
    callers.
    """
    with _save_pcs_lock:
        global _pc_cache, _pc_cache_time
        try:
            from src.core.db import get_db
            with get_db() as conn:
                for pc_id, pc in pcs.items():
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
            log.error("DB save failed for price_checks: %s", e)
            if raise_on_error:
                raise
        finally:
            # Invalidate AFTER write commits (see _save_single_pc note).
            _pc_cache = None
            _pc_cache_time = 0
            try:
                from src.api.modules import routes_prd28 as _rprd
                if hasattr(_rprd, "_dash_init_cache"):
                    _rprd._dash_init_cache["data"] = None
                    _rprd._dash_init_cache["ts"] = 0
            except Exception as _e:
                log.debug("dash_init_cache invalidation suppressed: %s", _e)


def _merge_save_pc(pc_id: str, pc_data: dict):
    """Atomic single-PC save."""
    pc_data["id"] = pc_id
    _save_price_checks({pc_id: pc_data})


def _is_user_facing_pc(pc: dict) -> bool:
    """Should this PC show in the PC queue on the homepage?

    A PC is user-facing when it's actionable work — either it has items,
    or it has a real solicitation/pc number, or it came from a real buyer
    email (even if the parser returned 0 items, that's a parse failure
    that still needs human attention, not noise).

    2026-04-12 regression context: 12 Valencia/CDCR PCs were silently
    hidden from the home queue because the parser failed to extract
    items from their Docusign-signed 704 PDFs, and this function returned
    False for any empty-item PC without a solicitation number. The user
    thought the email poller was broken when in fact the PCs existed
    as parse failures waiting for triage.
    """
    status = pc.get("status", "new")
    if status in ("dismissed", "archived", "deleted", "duplicate",
                  "no_response", "not_responding", "expired", "reclassified"):
        return False
    items = pc.get("items", [])
    if isinstance(items, str):
        try:
            items = json.loads(items)
        except Exception:
            items = []
    item_count = len(items) if isinstance(items, list) else 0
    if item_count > 0:
        return True
    sol = pc.get("solicitation_number", "") or pc.get("pc_number", "")
    if sol and sol != "unknown":
        return True
    # Zero items, no solicitation — but came from email with a real
    # sender. This is a parse failure: surface it, don't hide it.
    if pc.get("email_subject") or pc.get("sender_email") or pc.get("original_sender"):
        return True
    return False


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

def _load_orders():
    """Load orders — delegates to order_dal (V2)."""
    try:
        from src.core.order_dal import load_orders_dict
        return load_orders_dict()
    except Exception:
        return _get_dashboard_fn("_load_orders")()

def _save_orders(orders):
    """Save orders — delegates to order_dal (V2)."""
    try:
        from src.core.order_dal import save_order, save_line_items_batch
        for oid, o in orders.items():
            save_order(oid, o, actor="system")
            items = o.get("line_items", o.get("items", []))
            if items and isinstance(items, list):
                save_line_items_batch(oid, items)
    except Exception:
        _get_dashboard_fn("_save_orders")(orders)

def _save_single_order(order_id, order):
    """Save single order — delegates to order_dal (V2)."""
    try:
        from src.core.order_dal import save_order, save_line_items_batch
        save_order(order_id, order, actor="system")
        items = order.get("line_items", order.get("items", []))
        if items and isinstance(items, list):
            save_line_items_batch(order_id, items)
    except Exception:
        _get_dashboard_fn("_save_single_order")(order_id, order)

def _load_crm_activity():
    return _get_dashboard_fn("_load_crm_activity")()

def _log_crm_activity(ref_id, event_type, description, actor="system", metadata=None):
    return _get_dashboard_fn("_log_crm_activity")(ref_id, event_type, description, actor, metadata)

def _get_crm_activity(ref_id=None, event_type=None, institution=None, limit=50):
    return _get_dashboard_fn("_get_crm_activity")(ref_id, event_type, institution, limit)

def save_rfq_file(rfq_id, filename, file_type, data, category="template", uploaded_by="system"):
    return _get_dashboard_fn("save_rfq_file")(rfq_id, filename, file_type, data, category, uploaded_by)

def get_rfq_file(file_id):
    return _get_dashboard_fn("get_rfq_file")(file_id)

def list_rfq_files(rfq_id, category=None):
    return _get_dashboard_fn("list_rfq_files")(rfq_id, category)
