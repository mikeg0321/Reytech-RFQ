"""
src/api/shared.py — Shared Blueprint + Auth infrastructure.

Extracted from dashboard.py so route modules can explicitly import
their core dependencies instead of relying on _load_route_module injection.

Usage in route modules:
    from src.api.shared import bp, auth_required, api_response
"""
import os
import time
import json
import logging
import functools
import threading

from flask import Blueprint, request, Response, jsonify, g

log = logging.getLogger("reytech")

# ── Blueprint ────────────────────────────────────────────────────────────────
bp = Blueprint("dashboard", __name__)

# ── Auth credentials ─────────────────────────────────────────────────────────
DASH_USER = os.environ.get("DASH_USER", "reytech")
DASH_PASS = os.environ.get("DASH_PASS", "changeme")
API_KEY = os.environ.get("API_KEY", "")

if DASH_PASS == "changeme":
    if os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("PORT"):
        raise RuntimeError("SECURITY: DASH_PASS must be set in production. Configure via Railway secrets.")
    log.warning("DASH_PASS is default 'changeme' — OK for local dev only")


def check_auth(username, password):
    return username == DASH_USER and password == DASH_PASS


def _check_api_key() -> bool:
    """Check X-API-Key header. Returns True if valid, False if absent, raises 401 if invalid."""
    key = request.headers.get("X-API-Key", "")
    if not key:
        return False
    if not API_KEY:
        return False  # No API_KEY configured — ignore header
    if key == API_KEY:
        g.api_auth = True
        return True
    # Key present but invalid — hard fail
    return None  # sentinel for "invalid key"


# ── Rate Limiting ────────────────────────────────────────────────────────────
_rate_limiter = {}
_rate_limiter_lock = threading.Lock()
_rate_limiter_last_cleanup = 0.0
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 600          # 600 req/min for authenticated users
RATE_LIMIT_AUTH_MAX = 20      # 20 FAILED auth attempts/min
_RATE_CLEANUP_INTERVAL = 60   # seconds between full eviction sweeps


def _check_rate_limit(key: str = None, max_requests: int = None) -> bool:
    """Check if request is within rate limits. Returns True if OK. Thread-safe."""
    global _rate_limiter_last_cleanup
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
        # Periodic cleanup — evict stale entries every 60s
        if now - _rate_limiter_last_cleanup > _RATE_CLEANUP_INTERVAL:
            _rate_limiter_last_cleanup = now
            stale_keys = [k for k, v in _rate_limiter.items()
                          if not v or (now - v[-1]) > RATE_LIMIT_WINDOW]
            for k in stale_keys:
                del _rate_limiter[k]
    return True


# ── Auth decorator ───────────────────────────────────────────────────────────
def auth_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        # Check X-API-Key first
        api_result = _check_api_key()
        if api_result is None:
            return jsonify({"ok": False, "error": "Invalid API key"}), 401
        if api_result is True:
            if not _check_rate_limit():
                return Response("Rate limited — slow down", 429)
            return f(*args, **kwargs)
        # Fall back to Basic Auth
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            auth_key = f"auth_fail:{request.remote_addr}"
            if not _check_rate_limit(auth_key, RATE_LIMIT_AUTH_MAX):
                return Response("Rate limited — too many failed auth attempts. Wait 60 seconds.", 429)
            return Response(
                "Reytech RFQ Dashboard — Login Required",
                401, {"WWW-Authenticate": 'Basic realm="Reytech RFQ Dashboard"'})
        if not _check_rate_limit():
            return Response("Rate limited — slow down", 429)
        return f(*args, **kwargs)
    return decorated


# ── Request tracing (before_request) ──────────────────────────────────────────
@bp.before_request
def _set_request_trace():
    """Assign a trace ID to every request for log correlation."""
    try:
        from src.core.tracing import set_trace_id
        set_trace_id(operation=request.endpoint or request.path)
    except Exception:
        pass


# ── Global auth guard (before_request) ───────────────────────────────────────
@bp.before_request
def _global_auth_guard():
    """Global auth guard — every request must authenticate except allowlisted paths."""
    _path = request.path
    if (_path.startswith("/static/") or
        _path.startswith("/api/email/track/") or
        _path == "/api/v1/harvest/status" or
        _path in ("/health", "/api/health", "/api/health/startup", "/ping",
                   "/favicon.ico", "/login",
                   "/api/qb/callback", "/api/voice/webhook", "/api/build",
                   "/api/webhook/inbound")):
        pass
    else:
        # Check X-API-Key first
        api_result = _check_api_key()
        if api_result is None:
            return jsonify({"ok": False, "error": "Invalid API key"}), 401
        if api_result is True:
            request._start_time = time.time()
            return None
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            auth_key = f"auth_fail:{request.remote_addr}"
            if not _check_rate_limit(auth_key, RATE_LIMIT_AUTH_MAX):
                return Response("Rate limited — too many failed auth attempts. Wait 60 seconds.", 429)
            log.warning("AUTH DENIED: %s %s from %s", request.method, _path, request.remote_addr)
            try:
                from src.core.security import _log_audit_internal
                _log_audit_internal("auth_denied", f"{request.method} {_path} from {request.remote_addr}")
            except Exception:
                pass
            return Response(
                "Reytech RFQ Dashboard — Login Required",
                401, {"WWW-Authenticate": 'Basic realm="Reytech RFQ Dashboard"'})
    request._start_time = time.time()


@bp.before_request
def _csrf_origin_check():
    """Basic CSRF protection — verify Origin header on state-changing requests."""
    if request.method in ("POST", "PUT", "DELETE", "PATCH"):
        origin = request.headers.get("Origin", "")
        if origin:
            # Allow same-origin and Railway URLs
            allowed = [request.host_url.rstrip("/")]
            if "railway.app" in request.host:
                allowed.append("https://" + request.host)
            if not any(origin.startswith(a) for a in allowed):
                from flask import abort
                log.warning("CSRF: blocked %s %s from origin %s",
                            request.method, request.path, origin)
                abort(403)


@bp.after_request
def _log_request_end(response):
    if hasattr(request, '_start_time'):
        duration_ms = round((time.time() - request._start_time) * 1000, 1)
        if request.path not in ('/api/health',) and not request.path.startswith('/static'):
            log.info("%s %s → %d (%.0fms)",
                     request.method, request.path, response.status_code, duration_ms,
                     extra={"route": request.path, "method": request.method,
                            "status": response.status_code, "duration_ms": duration_ms})
    return response


# ── API Response Helper ──────────────────────────────────────────────────────

def api_response(data=None, error=None, status=200):
    """Standard API response shape for machine consumption.

    Usage:
        return api_response({"rfqs": [...]})
        return api_response(error="Not found", status=404)
    """
    payload = {"ok": error is None}
    if data is not None:
        payload["data"] = data
    if error is not None:
        payload["error"] = error
    return jsonify(payload), status


# ── API Versioning ──────────────────────────────────────────────────────────
# /api/v1/* routes live in routes_v1.py as dedicated endpoints (not redirects)
# This keeps v1 stable while /api/* can evolve freely.
