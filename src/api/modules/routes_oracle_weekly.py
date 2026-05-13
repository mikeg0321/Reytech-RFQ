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


@bp.route("/api/admin/auto-recommendations", methods=["GET"])
@auth_required
@safe_route
def api_admin_auto_recommendations():
    """PR-S — JSON payload for the auto-recommend dashboard.

    Window defaults to 7d; ?window=N override allowed for ad-hoc
    investigation. Returns the same dict as build_auto_recommendations
    so the front-end can render rows without duplicating logic.
    """
    try:
        from src.agents.auto_recommendations import build_auto_recommendations
        window = int(request.args.get("window", 7))
        return jsonify(build_auto_recommendations(window_days=window))
    except Exception as e:
        log.exception("auto-recommendations")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/admin/auto-recommendations", methods=["GET"])
@auth_required
@safe_route
def admin_auto_recommendations_page():
    """PR-S — minimal HTML view of the auto-recommendations.

    Reuses the JSON builder so the in-app surface stays in sync with the
    digest email. Server-rendered (no JS) — operator-readable status
    table with each agency's bucket + suggestion.
    """
    try:
        from src.agents.auto_recommendations import build_auto_recommendations
        window = int(request.args.get("window", 7))
        rep = build_auto_recommendations(window_days=window)
    except Exception as e:
        log.exception("admin-auto-recommendations")
        return (f"<h1>Auto-recommendations unavailable</h1><pre>{e}</pre>", 500)

    color_to_css = {
        "warn": "background:#fff4d6;border-left:4px solid #d97706;",
        "info": "background:#e0f2fe;border-left:4px solid #0284c7;",
        "good": "background:#dcfce7;border-left:4px solid #16a34a;",
        "neutral": "background:#f3f4f6;border-left:4px solid #6b7280;",
    }
    rows_html = []
    for r in rep["recommendations"]:
        style = color_to_css.get(r.get("color", "neutral"), color_to_css["neutral"])
        rows_html.append(
            f'<tr style="{style}">'
            f'<td><strong>{r["agency"]}</strong></td>'
            f'<td>{r["line_count"]}</td>'
            f'<td>{r["quote_count"]}</td>'
            f'<td>{r["median_drift_pct"] if r["median_drift_pct"] is not None else "—"}</td>'
            f'<td>{r["capped_pct"]:.0f}%</td>'
            f'<td><strong>{r["headline"]}</strong><br/>'
            f'<span style="font-size:0.9em">{r["suggestion"]}</span></td>'
            f'</tr>'
        )
    body = "".join(rows_html) or '<tr><td colspan="6">No drift data in window</td></tr>'
    return (
        "<html><head><title>Auto-Recommendations — PR-S</title>"
        "<style>body{font-family:sans-serif;max-width:1100px;margin:20px auto;padding:0 16px}"
        "table{width:100%;border-collapse:collapse}"
        "th,td{padding:8px 10px;text-align:left;vertical-align:top}"
        "th{background:#f3f4f6;border-bottom:2px solid #d1d5db}"
        "tr{border-bottom:1px solid #e5e7eb}</style></head><body>"
        f"<h1>Auto-Recommendations — last {rep['window_days']} days</h1>"
        f"<p><strong>Summary:</strong> {rep['summary_line']}</p>"
        f"<p>{rep['total_lines']} lines across {rep['total_agencies']} agencies "
        f"({rep['total_quotes']} quotes).</p>"
        "<table><thead><tr>"
        "<th>Agency</th><th>Lines</th><th>Quotes</th>"
        "<th>Median drift %</th><th>Capped %</th><th>Recommendation</th>"
        "</tr></thead><tbody>"
        f"{body}"
        "</tbody></table>"
        f"<p style='color:#6b7280;font-size:0.85em'>Generated at {rep['generated_at']}</p>"
        "</body></html>"
    )
