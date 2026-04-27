# routes_fill_plan.py
#
# Phase 1.6 PR3a of PLAN_ONCE_AND_FOR_ALL.md (2026-04-26).
#
# Read-only view of the email-contract → fill-plan binding.
# Surfaces, per quote, every required form joined to:
#   - the buyer's attached blank (if any)
#   - the matched FormProfile (buyer-specific or generic fallback)
#   - missing critical fields (signature, supplier name, total)
#
# Lets the operator see "which forms are going to fill correctly,
# which will fall back to generic, which will fill blank" BEFORE
# generating the package — the gap that's been silent until now.

import logging

from flask import jsonify

from src.api.shared import bp, auth_required
from src.agents.fill_plan_builder import build_fill_plan

log = logging.getLogger("reytech")


@bp.route("/api/quote/<quote_type>/<quote_id>/fill-plan", methods=["GET"])
@auth_required
def api_quote_fill_plan(quote_type, quote_id):
    """Return the fill-plan for a quote.

    Path:
        quote_type ∈ {"pc", "rfq"}
        quote_id   = PC id or RFQ id
    """
    qt = (quote_type or "").lower().strip()
    if qt not in ("pc", "rfq"):
        return jsonify({"ok": False, "error": "quote_type must be pc or rfq"}), 400

    try:
        plan = build_fill_plan(quote_id, qt)
        return jsonify({"ok": True, "plan": plan.to_dict()})
    except Exception as e:
        log.error("api_quote_fill_plan(%s, %s) error: %s", qt, quote_id, e,
                  exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
