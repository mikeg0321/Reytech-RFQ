"""
Security Middleware — Rate Limiting + CSRF Protection
=====================================================
Production hardening for multi-user scenario.

Rate Limiting:
- In-memory token bucket per IP address
- Configurable limits per endpoint group
- 429 response when exceeded

CSRF Protection:
- Token-based for state-changing endpoints (POST/PUT/DELETE)
- Tokens stored in Flask session
- Exempt API endpoints that use auth headers
"""

import os
import time
import secrets
import logging
import functools
from collections import defaultdict
from threading import Lock
from datetime import datetime

from flask import request, session, jsonify, abort

log = logging.getLogger("reytech.security")

# ═══════════════════════════════════════════════════════════════════════════════
# Rate Limiting
# ═══════════════════════════════════════════════════════════════════════════════

class RateLimiter:
    """Simple in-memory rate limiter using token bucket algorithm."""
    
    def __init__(self):
        self._buckets = defaultdict(lambda: {"tokens": 60, "last_refill": time.time()})
        self._lock = Lock()
    
    def check(self, key: str, max_tokens: int = 60, refill_rate: float = 1.0) -> bool:
        """Check if request is allowed. Returns True if allowed, False if rate limited.
        
        Args:
            key: Unique key for the bucket (usually IP + endpoint group)
            max_tokens: Maximum burst capacity
            refill_rate: Tokens added per second
        """
        with self._lock:
            bucket = self._buckets[key]
            now = time.time()
            elapsed = now - bucket["last_refill"]
            
            # Refill tokens
            bucket["tokens"] = min(max_tokens, bucket["tokens"] + elapsed * refill_rate)
            bucket["last_refill"] = now
            
            if bucket["tokens"] >= 1:
                bucket["tokens"] -= 1
                return True
            return False
    
    def cleanup(self, max_age: int = 3600):
        """Remove stale buckets older than max_age seconds."""
        now = time.time()
        with self._lock:
            stale = [k for k, v in self._buckets.items() if now - v["last_refill"] > max_age]
            for k in stale:
                del self._buckets[k]


# Global rate limiter instance
_limiter = RateLimiter()


# Rate limit tiers
RATE_LIMITS = {
    "default":     {"max_tokens": 60,  "refill_rate": 2.0},   # 120/min
    "api":         {"max_tokens": 30,  "refill_rate": 1.0},   # 60/min
    "auth":        {"max_tokens": 5,   "refill_rate": 0.1},   # 6/min (login attempts)
    "heavy":       {"max_tokens": 10,  "refill_rate": 0.2},   # 12/min (PDF gen, research)
    "poll":        {"max_tokens": 5,   "refill_rate": 0.05},  # 3/min (email polling)
}


def rate_limit(tier: str = "default"):
    """Decorator to apply rate limiting to a route."""
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if os.environ.get("DISABLE_RATE_LIMIT", "").lower() == "true":
                return f(*args, **kwargs)
            
            ip = request.remote_addr or "unknown"
            key = f"{ip}:{tier}"
            limits = RATE_LIMITS.get(tier, RATE_LIMITS["default"])
            
            if not _limiter.check(key, **limits):
                log.warning("Rate limit exceeded: %s tier=%s", ip, tier)
                _log_audit_internal("rate_limited", f"IP {ip} exceeded {tier} rate limit")
                return jsonify({"ok": False, "error": "Rate limit exceeded. Please try again shortly."}), 429
            
            return f(*args, **kwargs)
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════════════════════════
# CSRF Protection
# ═══════════════════════════════════════════════════════════════════════════════

def generate_csrf_token() -> str:
    """Generate a CSRF token and store it in the session."""
    if "_csrf_token" not in session:
        session["_csrf_token"] = secrets.token_hex(32)
    return session["_csrf_token"]


def validate_csrf_token() -> bool:
    """Validate CSRF token from request header or form data."""
    expected = session.get("_csrf_token", "")
    if not expected:
        return False
    
    # Check header first (for AJAX requests)
    token = request.headers.get("X-CSRF-Token", "")
    if not token:
        # Check form data
        token = request.form.get("_csrf_token", "")
    if not token:
        # Check JSON body
        data = request.get_json(silent=True) or {}
        token = data.get("_csrf_token", "")
    
    return secrets.compare_digest(token, expected)


def csrf_protect(f):
    """Decorator to require CSRF token on state-changing requests.
    Only applies to POST/PUT/DELETE. Exempt if Authorization header present."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if os.environ.get("DISABLE_CSRF", "").lower() == "true":
            return f(*args, **kwargs)
        
        if request.method in ("POST", "PUT", "DELETE"):
            # Exempt if using Authorization header (API clients)
            if request.headers.get("Authorization"):
                return f(*args, **kwargs)
            
            # Exempt AJAX with matching origin (simple CSRF protection)
            origin = request.headers.get("Origin", "")
            host = request.host_url.rstrip("/")
            if origin and origin == host:
                return f(*args, **kwargs)
            
            # For form submissions, require token
            if request.content_type and "form" in request.content_type:
                if not validate_csrf_token():
                    log.warning("CSRF validation failed: %s %s", request.method, request.path)
                    _log_audit_internal("csrf_failed", f"{request.method} {request.path} from {request.remote_addr}")
                    return jsonify({"ok": False, "error": "CSRF validation failed"}), 403
        
        return f(*args, **kwargs)
    return wrapper


# ═══════════════════════════════════════════════════════════════════════════════
# Audit Trail Integration
# ═══════════════════════════════════════════════════════════════════════════════

def _log_audit_internal(action: str, details: str = "", metadata: dict = None):
    """Log to audit trail (internal use — avoids circular imports)."""
    try:
        import sqlite3
        from src.core.paths import DATA_DIR
        conn = sqlite3.connect(os.path.join(DATA_DIR, "reytech.db"))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_trail (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                action TEXT NOT NULL,
                details TEXT,
                ip_address TEXT,
                user_agent TEXT,
                metadata TEXT
            )
        """)
        import json
        conn.execute(
            "INSERT INTO audit_trail (timestamp, action, details, ip_address, user_agent, metadata) VALUES (?,?,?,?,?,?)",
            (datetime.now().isoformat(), action, details[:500],
             request.remote_addr if request else "",
             (request.user_agent.string[:200] if request and request.user_agent else ""),
             json.dumps(metadata or {}, default=str)[:1000])
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def audit_action(action_name: str):
    """Decorator to auto-log actions to audit trail."""
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            result = f(*args, **kwargs)
            try:
                details = f"{request.method} {request.path}"
                if kwargs:
                    details += f" {kwargs}"
                _log_audit_internal(action_name, details)
            except Exception:
                pass
            return result
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════════════════════════
# Security Headers Middleware
# ═══════════════════════════════════════════════════════════════════════════════

def add_security_headers(response):
    """Add security headers to every response."""
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if not response.headers.get("Cache-Control"):
        response.headers["Cache-Control"] = "no-store"
    return response


def init_security(app):
    """Initialize security middleware on the Flask app."""
    app.after_request(add_security_headers)
    app.jinja_env.globals["csrf_token"] = generate_csrf_token
    log.info("Security middleware initialized: rate limiting, CSRF, security headers")
