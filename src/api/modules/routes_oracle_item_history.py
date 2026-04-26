# routes_oracle_item_history.py
#
# Phase 4.2 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-25).
# Buyer-product pricing history lookup. Operator about to bid sees:
#   - prior Reytech quotes for this buyer × this item (won/lost + our price)
#   - SCPRS competitor wins (won_quotes_kb) for the same buyer × item
#   - winning-price stats (min/median/max)
#   - oracle's current recommended markup for the agency
#
# This is the read-side complement to PR #548 (Mark Won/Lost write side).
# Where #548 closes the calibration loop, this surfaces what the loop
# learned so the next quote benefits.

import json
import logging
import statistics

from flask import jsonify, request

from src.api.shared import bp, auth_required
# Reuse the same Jaccard / agency-substring helpers the joinback uses,
# so a row that matched there matches here too — same heuristic in both.
from src.core.oracle_backfill import (
    _agency_match, _description_match_score,
)

log = logging.getLogger("reytech")


@bp.route("/api/oracle/item-history")
@auth_required
def api_oracle_item_history():
    """Return prior bid history for (agency, description).

    Query params:
        agency (str, required)
        description (str, required)
        limit (int, optional, default 10) — cap each list
        threshold (float, optional, default 0.45) — description Jaccard floor

    Response:
      {
        ok, agency, description,
        matches: {
          quotes: [{quote_number, status, our_price, created_at, po_number, score}],
          kb:     [{po_number, winning_vendor, winning_price, reytech_won,
                    reytech_price, award_date, score}]
        },
        stats: {
          matches_total, wins, losses, win_rate_pct,
          our_winning_prices, our_losing_prices,
          competitor_winning_prices: [min, median, max]
        },
        oracle: {markup_pct, confidence, scope, sample_size, rationale}
      }
    """
    agency = (request.args.get("agency") or "").strip()
    description = (request.args.get("description") or "").strip()
    if not agency or not description:
        return jsonify({"ok": False,
                        "error": "agency and description required"}), 400

    try:
        limit = max(1, min(50, int(request.args.get("limit", "10"))))
    except (TypeError, ValueError):
        limit = 10
    try:
        threshold = max(0.1, min(1.0, float(request.args.get("threshold", "0.45"))))
    except (TypeError, ValueError):
        threshold = 0.45

    matches_quotes = []
    matches_kb = []

    # ── Pull from quotes table ──
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT quote_number, status, agency, institution,
                       line_items, total, po_number, created_at
                FROM quotes
                WHERE is_test = 0
                  AND status IN ('won', 'lost', 'sent')
                  AND line_items IS NOT NULL
            """).fetchall()
    except Exception as e:
        log.exception("item_history quotes load")
        return jsonify({"ok": False, "error": str(e)}), 500

    for q in rows:
        q_agency = q["agency"] or q["institution"] or ""
        if not _agency_match(agency, q_agency):
            continue
        try:
            items = json.loads(q["line_items"] or "[]")
        except Exception:
            items = []
        best_item = None
        for it in items:
            if not isinstance(it, dict):
                continue
            score = _description_match_score(description, it.get("description", ""))
            if score < threshold:
                continue
            if best_item is None or score > best_item["score"]:
                best_item = {
                    "score": round(score, 3),
                    "our_price": float(
                        it.get("unit_price")
                        or it.get("bid_price")
                        or (it.get("pricing") or {}).get("recommended_price")
                        or 0
                    ),
                    "description": it.get("description", ""),
                }
        if best_item is None:
            continue
        matches_quotes.append({
            "quote_number": q["quote_number"],
            "status": q["status"],
            "our_price": best_item["our_price"],
            "matched_description": best_item["description"],
            "created_at": q["created_at"],
            "po_number": q["po_number"] or "",
            "score": best_item["score"],
        })

    matches_quotes.sort(key=lambda r: r.get("created_at", ""), reverse=True)

    # ── Pull from won_quotes_kb ──
    try:
        with get_db() as conn:
            kb_rows = conn.execute("""
                SELECT id, item_description, agency, winning_price,
                       winning_vendor, reytech_won, reytech_price,
                       price_delta, award_date, po_number
                FROM won_quotes_kb
            """).fetchall()
    except Exception as e:
        log.debug("item_history kb load: %s", e)
        kb_rows = []

    for k in kb_rows:
        if not _agency_match(agency, k["agency"] or ""):
            continue
        score = _description_match_score(description, k["item_description"] or "")
        if score < threshold:
            continue
        matches_kb.append({
            "po_number": k["po_number"] or "",
            "winning_vendor": k["winning_vendor"] or "",
            "winning_price": float(k["winning_price"] or 0),
            "reytech_won": int(k["reytech_won"] or 0),
            "reytech_price": float(k["reytech_price"] or 0) if k["reytech_price"] else None,
            "award_date": k["award_date"] or "",
            "matched_description": k["item_description"] or "",
            "score": round(score, 3),
        })

    matches_kb.sort(key=lambda r: r.get("award_date", ""), reverse=True)

    # ── Aggregate stats ──
    our_winning = [r["our_price"] for r in matches_quotes
                   if r["status"] == "won" and r["our_price"] > 0]
    our_losing = [r["our_price"] for r in matches_quotes
                  if r["status"] == "lost" and r["our_price"] > 0]
    competitor_prices = [r["winning_price"] for r in matches_kb
                         if r["winning_price"] > 0]

    wins_total = len(our_winning) + sum(1 for k in matches_kb if k["reytech_won"])
    losses_total = len(our_losing) + sum(
        1 for k in matches_kb
        if (not k["reytech_won"]) and k.get("reytech_price")
    )
    decided_total = wins_total + losses_total
    win_rate = round(100.0 * wins_total / decided_total, 1) if decided_total else None

    def _stats(values):
        if not values:
            return None
        return {
            "min": round(min(values), 2),
            "median": round(statistics.median(values), 2),
            "max": round(max(values), 2),
            "n": len(values),
        }

    stats = {
        "matches_total": len(matches_quotes) + len(matches_kb),
        "wins": wins_total,
        "losses": losses_total,
        "win_rate_pct": win_rate,
        "our_winning_prices": _stats(our_winning),
        "our_losing_prices": _stats(our_losing),
        "competitor_winning_prices": _stats(competitor_prices),
    }

    # ── Oracle's current recommendation for this agency ──
    oracle = {"markup_pct": None, "confidence": None, "scope": None,
              "sample_size": 0, "rationale": "not loaded"}
    try:
        from src.core.pricing_oracle_v2 import recommend_quote_markup
        oracle = recommend_quote_markup(agency)
    except Exception as e:
        log.debug("item_history oracle: %s", e)
        oracle["rationale"] = f"oracle unavailable: {e}"

    # ── Cap matches ──
    matches_quotes = matches_quotes[:limit]
    matches_kb = matches_kb[:limit]

    return jsonify({
        "ok": True,
        "agency": agency,
        "description": description,
        "matches": {
            "quotes": matches_quotes,
            "kb": matches_kb,
        },
        "stats": stats,
        "oracle": oracle,
    })
