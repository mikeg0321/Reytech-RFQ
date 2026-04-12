"""
error_handler.py — Standardized error handling to prevent silent failures.
Sprint 6 (M2): Wraps common patterns with logging so failures are visible.

Usage:
    from src.core.error_handler import safe_call, log_error

    # Instead of:  try: ... except Exception: pass
    # Use:        result = safe_call(risky_function, arg1, arg2, default=None, context="loading data")
"""
import logging
import traceback
from functools import wraps

try:
    from src.core.pii_mask import mask_pii
except Exception:
    def mask_pii(text):
        return text or ""

log = logging.getLogger("reytech.errors")


def safe_call(fn, *args, default=None, context: str = "", **kwargs):
    """
    Call a function safely, logging any exception instead of silently swallowing it.
    Returns `default` on failure.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        caller = context or fn.__name__ if hasattr(fn, '__name__') else "unknown"
        log.warning("safe_call(%s) failed: %s", caller, str(e)[:200])
        return default


def log_error(e: Exception, context: str = "", level: str = "error"):
    """Log an exception with context. Use instead of bare `except Exception: pass`."""
    msg = f"{context}: {type(e).__name__}: {str(e)[:300]}"
    getattr(log, level, log.error)(msg)


def safe_route(f):
    """
    Decorator for Flask route handlers that catches unhandled exceptions
    and returns a proper JSON error response instead of a 500 page.

    Usage:
        @bp.route("/api/something")
        @auth_required
        @safe_route
        def my_endpoint():
            ...
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            log.error("Route %s failed: %s", f.__name__, mask_pii(str(e)[:300]),
                      exc_info=True)
            from flask import jsonify
            return jsonify({
                "ok": False,
                "error": f"Internal error: {type(e).__name__}",
                "detail": str(e)[:200],
            }), 500
    return wrapper


def safe_page(f):
    """
    Decorator for Flask routes that return HTML pages.
    On unhandled exception, renders an error page instead of crashing.

    Usage:
        @bp.route("/some-page")
        @auth_required
        @safe_page
        def my_page():
            ...
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            log.error("Page %s failed: %s", f.__name__, mask_pii(str(e)[:300]),
                      exc_info=True)
            from flask import render_template_string
            return render_template_string("""
                <div style="padding:24px;font-family:system-ui">
                    <h2 style="color:#c00">Something went wrong</h2>
                    <p>{{ error_type }}: {{ error_msg }}</p>
                    <a href="/" style="color:#0066cc">&larr; Back to Home</a>
                </div>
            """, error_type=type(e).__name__, error_msg=str(e)[:200]), 500
    return wrapper


def safe_background(fn):
    """
    Decorator for background thread functions that logs exceptions
    instead of crashing silently.

    Usage:
        @safe_background
        def _poll_loop():
            while True:
                process_emails()
                time.sleep(60)
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            log.critical(
                "BACKGROUND TASK CRASHED: %s — %s\n%s",
                fn.__name__, mask_pii(str(e)[:300]),
                traceback.format_exc()[-500:]
            )
            raise  # Re-raise so thread can handle restart logic
    return wrapper
