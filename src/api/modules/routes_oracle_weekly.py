"""
routes_oracle_weekly.py — Oracle weekly digest email endpoints.

Mike's request 2026-04-27: replace the disabled stale-data digest
emails with one high-signal weekly email driven by connected data
sources (calibration, swap-link telemetry, live category buckets).

Endpoints:
  POST /api/oracle/weekly-email             — send the report
  POST /api/oracle/weekly-email?dry_run=1   — preview without sending
"""
import logging
from datetime import datetime

log = logging.getLogger("reytech.routes_oracle_weekly")


@bp.route("/api/oracle/weekly-email", methods=["POST"])
@auth_required
@safe_route
def api_oracle_weekly_email():
    """Build + send the Oracle weekly digest.

    Body / query: {"dry_run": true} returns the body without sending.
    """
    try:
        from src.agents.oracle_weekly import send_weekly_email
        body = request.get_json(force=True, silent=True) or {}
        dry_run = (body.get("dry_run")
                   or request.args.get("dry_run") in ("1", "true", "yes"))
        to_override = body.get("to") or None
        result = send_weekly_email(dry_run=bool(dry_run),
                                   to_override=to_override)
        return jsonify(result)
    except Exception as e:
        log.exception("oracle-weekly-email")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/oracle/weekly-email/preview", methods=["GET"])
@auth_required
@safe_route
def api_oracle_weekly_preview():
    """Render the weekly report as JSON for inspection (dry-run-equivalent)."""
    try:
        from src.agents.oracle_weekly import build_weekly_report
        return jsonify(build_weekly_report())
    except Exception as e:
        log.exception("oracle-weekly-preview")
        return jsonify({"ok": False, "error": str(e)}), 500
