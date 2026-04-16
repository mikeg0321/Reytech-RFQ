"""Shadow Mode Admin Dashboard — view fill engine divergences.

Routes:
    GET  /admin/shadow-diffs          — Dashboard page
    GET  /api/admin/shadow-diffs      — JSON API for diff data
    POST /api/admin/shadow-diffs/clear — Clear the diff log
"""
import logging

from flask import jsonify, request

from src.api.shared import bp, auth_required

log = logging.getLogger(__name__)


@bp.route("/admin/shadow-diffs")
@auth_required
def shadow_diffs_page():
    """Shadow mode divergence dashboard."""
    from src.api.render import render_page
    from src.forms.shadow_mode import get_recent_diffs, get_diff_summary

    diffs = get_recent_diffs(limit=100)
    summary = get_diff_summary()

    return render_page("shadow_diffs.html", active_page="Admin",
                       diffs=diffs, summary=summary)


@bp.route("/api/admin/shadow-diffs")
@auth_required
def api_shadow_diffs():
    """JSON API: recent shadow diffs + summary stats."""
    from src.forms.shadow_mode import get_recent_diffs, get_diff_summary

    limit = int(request.args.get("limit", 100))
    diffs = get_recent_diffs(limit=limit)
    summary = get_diff_summary()

    return jsonify({"ok": True, "diffs": diffs, "summary": summary})


@bp.route("/api/admin/shadow-diffs/clear", methods=["POST"])
@auth_required
def api_shadow_diffs_clear():
    """Clear the shadow diff log."""
    import os
    from src.forms.shadow_mode import _SHADOW_LOG

    try:
        if os.path.exists(_SHADOW_LOG):
            os.remove(_SHADOW_LOG)
        return jsonify({"ok": True, "message": "Shadow diff log cleared"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
