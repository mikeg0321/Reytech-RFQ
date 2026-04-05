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
_save_rfqs_lock = threading.Lock()
_save_pcs_lock = threading.Lock()

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


# Data access functions delegated to dashboard.py
# These will be migrated here one at a time in future PRs.

def load_rfqs():
    return _get_dashboard_fn("load_rfqs")()

def _save_single_rfq(rfq_id, r):
    return _get_dashboard_fn("_save_single_rfq")(rfq_id, r)

def save_rfqs(rfqs):
    return _get_dashboard_fn("save_rfqs")(rfqs)

def _normalize_rfq_fields(rfqs):
    return _get_dashboard_fn("_normalize_rfq_fields")(rfqs)

def _load_price_checks(include_items=True):
    return _get_dashboard_fn("_load_price_checks")(include_items)

def _save_single_pc(pc_id, pc):
    return _get_dashboard_fn("_save_single_pc")(pc_id, pc)

def _save_price_checks(pcs):
    return _get_dashboard_fn("_save_price_checks")(pcs)

def _merge_save_pc(pc_id, pc_data):
    return _get_dashboard_fn("_merge_save_pc")(pc_id, pc_data)

def _is_user_facing_pc(pc):
    return _get_dashboard_fn("_is_user_facing_pc")(pc)

def _get_pc_items(pc):
    return _get_dashboard_fn("_get_pc_items")(pc)

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
