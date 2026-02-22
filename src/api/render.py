"""
src/api/render.py — Template rendering helper

Provides render_page() which replaces render_template_string(render(...))
with proper Jinja2 template rendering. Automatically injects shared context
(poll status, email config) so templates don't need to know about it.
"""

from flask import render_template

def render_page(template_name: str, active_page: str = "", **context):
    """Render a Jinja2 template with shared dashboard context.
    
    Usage:
        return render_page("revenue.html", active_page="Revenue", ytd=11000)
    """
    # Inject poll status (same logic as dashboard.render())
    try:
        from src.api.dashboard import CONFIG, POLL_STATUS
        _email_cfg = CONFIG.get("email", {})
        _has_email = bool(_email_cfg.get("email_password"))
        _poll_running = POLL_STATUS.get("running", False)
        _poll_last = POLL_STATUS.get("last_check", "")
        poll_status = "Polling" if _poll_running else ("Email not configured" if not _has_email else "Starting...")
        poll_class = "poll-on" if _poll_running else ("poll-off" if not _has_email else "poll-wait")
    except Exception:
        poll_status = "Starting..."
        poll_class = "poll-wait"
        _poll_last = ""

    context.setdefault("poll_status", poll_status)
    context.setdefault("poll_class", poll_class)
    context.setdefault("poll_last", _poll_last)
    context.setdefault("active_page", active_page)

    return render_template(template_name, **context)
