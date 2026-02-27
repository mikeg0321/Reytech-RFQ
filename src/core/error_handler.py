"""
error_handler.py — Standardized error handling to prevent silent failures.
Sprint 6 (M2): Wraps common patterns with logging so failures are visible.

Usage:
    from src.core.error_handler import safe_call, log_error

    # Instead of:  try: ... except: pass
    # Use:        result = safe_call(risky_function, arg1, arg2, default=None, context="loading data")
"""
import logging
import traceback
from functools import wraps

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
    """Log an exception with context. Use instead of bare `except: pass`."""
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
            log.error("Route %s failed: %s", f.__name__, str(e)[:300],
                      exc_info=True)
            from flask import jsonify
            return jsonify({
                "ok": False,
                "error": f"Internal error: {type(e).__name__}",
                "detail": str(e)[:200],
            }), 500
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
                fn.__name__, str(e)[:300],
                traceback.format_exc()[-500:]
            )
            raise  # Re-raise so thread can handle restart logic
    return wrapper
