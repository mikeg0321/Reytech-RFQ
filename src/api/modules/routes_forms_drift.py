# routes_forms_drift.py
#
# Phase 1.6 PR3i (2026-04-26).
#
# Forms-drift monitor surface: scan-on-demand + read latest report.
# The full automatic monthly scheduler is left for a follow-up — for
# now Mike can fire the scan from /settings or via curl whenever he
# wants a fresh drift snapshot.

import logging

from flask import jsonify

from src.api.shared import bp, auth_required
from src.agents.forms_drift_monitor import (
    scan_forms_drift, save_report, latest_report,
)

log = logging.getLogger("reytech")


@bp.route("/api/forms-drift/scan", methods=["POST"])
@auth_required
def api_forms_drift_scan():
    """Run drift scan now and persist the report."""
    from flask import request
    days = int(request.args.get("days") or 30)
    report = scan_forms_drift(days=days)
    path = save_report(report)
    return jsonify({"ok": True, "report_path": path, "report": report})


@bp.route("/api/forms-drift/latest", methods=["GET"])
@auth_required
def api_forms_drift_latest():
    """Return the most recently saved drift report."""
    r = latest_report()
    if not r:
        return jsonify({"ok": True, "report": None,
                        "message": "no drift report yet — POST /api/forms-drift/scan"}), 200
    return jsonify({"ok": True, "report": r})
