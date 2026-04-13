"""Admin endpoints for runtime feature flags — Item C of the P0
resilience backlog.

Endpoints:
    GET    /api/admin/flags              List all flags
    GET    /api/admin/flags/<key>        Get one flag (value + metadata)
    POST   /api/admin/flags              Set/update a flag
                                          body: {key, value, description?}
    DELETE /api/admin/flags/<key>        Delete a flag (revert to code default)

All endpoints require auth. Setting a flag invalidates the per-worker
cache so subsequent reads see the new value within one request cycle.

Note: cache is per-worker. With multiple gunicorn workers, updates
propagate one-at-a-time as each worker's 60s TTL expires. If you
need immediate cluster-wide propagation, restart the service.
"""
import logging

from flask import jsonify, request

from src.api.shared import bp, auth_required
from src.core.error_handler import safe_route

log = logging.getLogger("reytech")


@bp.route("/api/admin/flags", methods=["GET"])
@auth_required
@safe_route
def api_flags_list():
    """List every currently-set flag."""
    from src.core.flags import list_flags
    flags = list_flags()
    return jsonify({"ok": True, "count": len(flags), "flags": flags})


@bp.route("/api/admin/flags/<key>", methods=["GET"])
@auth_required
@safe_route
def api_flags_get(key):
    """Return a single flag by key, or 404 if unset."""
    from src.core.flags import list_flags
    flags = list_flags()
    match = next((f for f in flags if f["key"] == key), None)
    if match is None:
        return jsonify({"ok": False, "error": f"flag '{key}' not set"}), 404
    return jsonify({"ok": True, "flag": match})


@bp.route("/api/admin/flags", methods=["POST"])
@auth_required
@safe_route
def api_flags_set():
    """Upsert a flag.

    Body: {"key": "pipeline.delivery_threshold", "value": "80",
           "description": "optional human note"}

    The value is always stored as a string; the caller-side
    get_flag(key, default) coerces it into the default's type at
    read time (bool / int / float / str).
    """
    data = request.get_json(silent=True) or {}
    key = (data.get("key") or "").strip()
    if not key:
        return jsonify({"ok": False, "error": "key is required"}), 400
    if "value" not in data:
        return jsonify({"ok": False, "error": "value is required"}), 400

    value = data["value"]
    description = (data.get("description") or "").strip()
    # Attribution: best effort — read from Basic Auth username if present
    updated_by = ""
    try:
        if request.authorization and request.authorization.username:
            updated_by = request.authorization.username
    except Exception:
        pass

    from src.core.flags import set_flag
    ok = set_flag(key, value, updated_by=updated_by, description=description)
    if not ok:
        return jsonify({"ok": False, "error": "set failed — check logs"}), 500
    return jsonify({"ok": True, "key": key, "value": str(value),
                    "updated_by": updated_by})


@bp.route("/api/admin/flags/<key>", methods=["DELETE"])
@auth_required
@safe_route
def api_flags_delete(key):
    """Delete a flag so the next read returns the code default."""
    from src.core.flags import delete_flag
    ok = delete_flag(key)
    if not ok:
        return jsonify({"ok": False, "error": "delete failed — check logs"}), 500
    return jsonify({"ok": True, "deleted": key})
