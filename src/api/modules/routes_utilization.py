"""Admin endpoints for utilization tracking — Phase 4 of the
PC↔RFQ refactor.

Read-only dashboard data:

    GET /api/admin/utilization/summary?days=7
    GET /api/admin/utilization/top?days=7&limit=20
    GET /api/admin/utilization/feature/<feature>?days=30
    GET /api/admin/utilization/dead?days=30

Internal-only per the product spec (Mike wants to see which
features are being used, not per-user attribution for external
consumption).
"""
import logging

from flask import jsonify, request

from src.api.shared import bp, auth_required
from src.core.error_handler import safe_route

log = logging.getLogger("reytech")


@bp.route("/api/admin/utilization/summary", methods=["GET"])
@auth_required
@safe_route
def api_utilization_summary():
    from src.core.utilization import summary
    days = max(1, min(int(request.args.get("days", 7)), 90))
    return jsonify(summary(days=days))


@bp.route("/api/admin/utilization/top", methods=["GET"])
@auth_required
@safe_route
def api_utilization_top():
    from src.core.utilization import top_features
    days = max(1, min(int(request.args.get("days", 7)), 90))
    limit = max(1, min(int(request.args.get("limit", 20)), 100))
    return jsonify({"ok": True, "days": days, "top": top_features(days=days, limit=limit)})


@bp.route("/api/admin/utilization/feature/<feature>", methods=["GET"])
@auth_required
@safe_route
def api_utilization_feature(feature):
    from src.core.utilization import feature_series
    days = max(1, min(int(request.args.get("days", 30)), 365))
    return jsonify({
        "ok": True,
        "feature": feature,
        "days": days,
        "series": feature_series(feature, days=days),
    })


# Known feature namespace — used for dead-feature detection. Keep in
# sync with features that have actual record_feature_use() call sites.
KNOWN_FEATURES = [
    "ingest.classify_request",
    "ingest.process_buyer_request",
    "pc.generate_quote",
    "pc.reparse",
    "pc.upload_pdf",
    "pc.auto_price",
    "rfq.generate_package",
    "rfq.upload_parse_doc",
    "cchcs_packet.generate",
    "oracle.lookup",
    "catalog.match",
    "scprs.lookup",
    "amazon.lookup",
    "grok.validate_product",
    "linker.triangulated",
    "form_qa.run",
    "form_qa.overlay_bounds",
    "package.completeness_check",
]


@bp.route("/api/admin/utilization/dead", methods=["GET"])
@auth_required
@safe_route
def api_utilization_dead():
    from src.core.utilization import dead_features
    days = max(1, min(int(request.args.get("days", 30)), 365))
    return jsonify({
        "ok": True,
        "days": days,
        "dead": dead_features(KNOWN_FEATURES, days=days),
        "tracked": KNOWN_FEATURES,
    })
