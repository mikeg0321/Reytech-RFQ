"""Cross-sell intel API routes.

Mike P0 2026-05-11 needle-mover #2. Surfaces the data from
`src.agents.cross_sell_intel` to operator-facing tools.

Routes:
  GET /api/cross-sell/prospects?top_n=20&days_back=365
      → list of buyers ranked by spend × recency
  GET /api/cross-sell/top-items?top_n=10&days_back=365
      → list of Reytech-sellable items ranked by competitor spend
  GET /api/cross-sell/recommendations?days_back=90
      → 3-5 actionable bullets distilled from the data
"""
import logging

from flask import jsonify, request

from src.api.shared import bp, auth_required

log = logging.getLogger(__name__)


def _int_arg(name: str, default: int, lo: int, hi: int) -> int:
    """Bounded int query-arg parse — clamps to [lo, hi]."""
    try:
        v = int(request.args.get(name, default))
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))


@bp.route("/api/cross-sell/prospects")
@auth_required
def api_cross_sell_prospects():
    """Ranked list of cross-sell prospects (buyers who bought items
    Reytech sells, from competitors)."""
    try:
        from src.agents.cross_sell_intel import get_prospects
        top_n = _int_arg("top_n", 20, 1, 200)
        days_back = _int_arg("days_back", 365, 7, 1825)
        prospects = get_prospects(top_n=top_n, days_back=days_back)
        return jsonify({
            "ok": True,
            "prospects": prospects,
            "count": len(prospects),
            "params": {"top_n": top_n, "days_back": days_back},
        })
    except Exception as e:
        log.error("api_cross_sell_prospects failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/cross-sell/top-items")
@auth_required
def api_cross_sell_top_items():
    """Top Reytech-sellable items by competitor spend."""
    try:
        from src.agents.cross_sell_intel import get_top_items_by_spend
        top_n = _int_arg("top_n", 10, 1, 100)
        days_back = _int_arg("days_back", 365, 7, 1825)
        items = get_top_items_by_spend(top_n=top_n, days_back=days_back)
        return jsonify({
            "ok": True,
            "items": items,
            "count": len(items),
            "params": {"top_n": top_n, "days_back": days_back},
        })
    except Exception as e:
        log.error("api_cross_sell_top_items failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/cross-sell/recommendations")
@auth_required
def api_cross_sell_recommendations():
    """3-5 actionable bullets distilled from the cross-sell data.

    Mike's ask: "the app is smart enough to give insights... but I am
    not getting any value from it." This endpoint is the value
    extractor — recommendations the operator can act on directly.
    """
    try:
        from src.agents.cross_sell_intel import get_general_recommendations
        days_back = _int_arg("days_back", 90, 7, 1825)
        out = get_general_recommendations(days_back=days_back)
        return jsonify(out)
    except Exception as e:
        log.error("api_cross_sell_recommendations failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
