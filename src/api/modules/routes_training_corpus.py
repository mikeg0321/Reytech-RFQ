# routes_training_corpus.py
#
# Phase 1.6 PR3g (2026-04-26).
#
# Read-only view of the on-disk training corpus and one POST endpoint
# to trigger bootstrap from the UI. The actual bootstrap is normally
# run via scripts/build_training_corpus.py; this endpoint exists for
# /settings convenience.

import logging

from flask import jsonify, request

from src.api.shared import bp, auth_required
from src.agents.training_corpus import (
    bootstrap_from_orders,
    coverage_report,
    build_training_pair,
)

log = logging.getLogger("reytech")


@bp.route("/api/training-corpus/coverage", methods=["GET"])
@auth_required
def api_training_corpus_coverage():
    """Per-buyer training-pair coverage report from disk."""
    return jsonify({"ok": True, **coverage_report()})


@bp.route("/api/training-corpus/bootstrap", methods=["POST"])
@auth_required
def api_training_corpus_bootstrap():
    """Run the bootstrap walk synchronously. POST only."""
    days = int(request.args.get("days") or 365)
    force = (request.args.get("force") or "").lower() in ("1", "true", "yes")
    limit = request.args.get("limit")
    limit = int(limit) if limit and limit.isdigit() else None

    summary = bootstrap_from_orders(days=days, force=force, limit=limit)
    return jsonify({"ok": True, "summary": summary})


@bp.route("/api/training-corpus/build/<quote_type>/<quote_id>",
          methods=["POST"])
@auth_required
def api_training_corpus_build_one(quote_type, quote_id):
    """Build training pair for a single quote (PC or RFQ)."""
    qt = (quote_type or "").lower().strip()
    if qt not in ("pc", "rfq", "order"):
        return jsonify({"ok": False, "error": "quote_type must be pc/rfq/order"}), 400
    force = (request.args.get("force") or "").lower() in ("1", "true", "yes")
    result = build_training_pair(quote_id, qt, force=force)
    return jsonify(result)
