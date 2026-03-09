"""
src/api/shared.py — Shared Blueprint + Auth infrastructure.

Extracted from dashboard.py so route modules can explicitly import
their core dependencies instead of relying on _load_route_module injection.

Usage in route modules:
    from src.api.shared import bp, auth_required
"""
import os
import time
import json
import logging
import functools
import threading

from flask import Blueprint, request, Response

log = logging.getLogger("reytech")

# ── Blueprint ────────────────────────────────────────────────────────────────
bp = Blueprint("dashboard", __name__)

# ── Auth credentials ─────────────────────────────────────────────────────────
DASH_USER = os.environ.get("DASH_USER", "reytech")
DASH_PASS = os.environ.get("DASH_PASS", "changeme")

if DASH_PASS == "changeme":
    log.warning("⚠️  SECURITY: DASH_PASS is set to default 'changeme'. Set DASH_PASS env var for production!")


def check_auth(username, password):
    return username == DASH_USER and password == DASH_PASS


# ── Rate Limiting ────────────────────────────────────────────────────────────
_rate_limiter = {}
_rate_limiter_lock = threading.Lock()
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 600          # 600 req/min for authenticated users (was 300)
RATE_LIMIT_AUTH_MAX = 20      # 20 FAILED auth attempts/min (was 60 counting ALL)


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
        if len(_rate_limiter) > 1000:
            _rate_limiter.clear()
    return True


# ── Auth decorator ───────────────────────────────────────────────────────────
def auth_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            # Only count FAILED attempts toward rate limit
            auth_key = f"auth_fail:{request.remote_addr}"
            if not _check_rate_limit(auth_key, RATE_LIMIT_AUTH_MAX):
                return Response("Rate limited — too many failed auth attempts. Wait 60 seconds.", 429)
            return Response(
                "🔒 Reytech RFQ Dashboard — Login Required",
                401, {"WWW-Authenticate": 'Basic realm="Reytech RFQ Dashboard"'})
        # Authenticated — generous limit
        if not _check_rate_limit():
            return Response("Rate limited — slow down", 429)
        return f(*args, **kwargs)
    return decorated


# ── Global auth guard (before_request) ───────────────────────────────────────
@bp.before_request
def _global_auth_guard():
    """Global auth guard — every request must authenticate except allowlisted paths."""
    _path = request.path
    if (_path.startswith("/static/") or
        _path.startswith("/api/email/track/") or
        _path in ("/health", "/api/health", "/api/health/startup", "/ping",
                   "/favicon.ico", "/login",
                   "/api/qb/callback", "/api/voice/webhook", "/api/build")):
        pass
    else:
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            # Only count FAILED attempts toward rate limit
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
                "🔒 Reytech RFQ Dashboard — Login Required",
                401, {"WWW-Authenticate": 'Basic realm="Reytech RFQ Dashboard"'})
    # CSRF: Disabled — all state-changing routes use @auth_required (HTTP Basic Auth)
    # which is inherently CSRF-resistant (browsers don't auto-send Basic Auth cross-origin)
    # Request timing
    request._start_time = time.time()


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
