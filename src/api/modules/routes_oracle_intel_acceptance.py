# routes_oracle_intel_acceptance.py
#
# Phase 4.7.3 of PLAN_PRICING_ENGINE_INTEGRATION.md (2026-04-27).
# Telemetry capture for category-intel suggested_alternative swap
# decisions. Each accept/reject is logged so the damping factor
# (currently hand-tuned at 0.5x) can be learned from rejection
# rate after ~30 days of operator activity.
#
# Endpoints:
#   POST /api/oracle/intel-acceptance        log one decision
#   GET  /api/oracle/intel-acceptance-stats  rollup by category +
#                                            overall acceptance rate

import logging

from flask import jsonify, request

from src.api.shared import bp, auth_required

log = logging.getLogger("reytech")


@bp.route("/api/oracle/intel-acceptance", methods=["POST"])
@auth_required
def api_intel_acceptance_log():
    """Record a single category-intel suggestion decision.

    Body:
      {
        "description": "Propet Walker",          # required
        "agency": "CDCR Sacramento",             # optional
        "category": "footwear-orthopedic",       # required
        "flavor": "B",                           # A/B/C — required
        "engine_markup_pct": 22.0,               # optional
        "engine_price": 122.00,                  # optional
        "suggested_markup_pct": 11.0,            # optional
        "suggested_price": 111.00,               # optional
        "final_price": 111.00,                   # optional
        "accepted": true,                        # required (bool)
        "quote_number": "R26Q42",                # optional
        "pcid": "abc-123"                        # optional
      }

    Always returns {ok: true, id: <log_id>}. Caller is the swap-link
    click handler in pc_detail.html (or future autosave diff watcher).
    """
    try:
        body = request.get_json(silent=True) or {}
        description = (body.get("description") or "").strip()
        category = (body.get("category") or "").strip()
        flavor = (body.get("flavor") or "").strip().upper()
        accepted = bool(body.get("accepted"))

        if not description or not category or flavor not in ("A", "B", "C"):
            return jsonify({"ok": False,
                            "error": "description, category, flavor (A/B/C) required"}), 400

        from src.core.db import get_db
        with get_db() as conn:
            cur = conn.execute("""
                INSERT INTO intel_acceptance_log
                  (description, agency, category, flavor,
                   engine_markup_pct, engine_price,
                   suggested_markup_pct, suggested_price,
                   final_price, accepted, quote_number, pcid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                description[:500],
                (body.get("agency") or "")[:200],
                category[:100],
                flavor,
                body.get("engine_markup_pct"),
                body.get("engine_price"),
                body.get("suggested_markup_pct"),
                body.get("suggested_price"),
                body.get("final_price"),
                1 if accepted else 0,
                (body.get("quote_number") or "")[:50],
                (body.get("pcid") or "")[:50],
            ))
            conn.commit()
            log_id = cur.lastrowid
        return jsonify({"ok": True, "id": log_id, "accepted": accepted})
    except Exception as e:
        log.exception("intel_acceptance_log")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/oracle/intel-acceptance-stats")
@auth_required
def api_intel_acceptance_stats():
    """Rollup of acceptance decisions by category.

    Query params:
        days (int, default 90) — lookback window
        category (str, optional) — narrow to one bucket
        flavor (str, optional)   — narrow to one A/B/C flavor

    Response:
      {
        ok, days, total, accepted, rejected, accept_rate_pct,
        by_category: [{category, total, accepted, rejected,
                       accept_rate_pct}, ...]
      }
    """
    try:
        try:
            days = max(1, min(3650, int(request.args.get("days", "90"))))
        except (TypeError, ValueError):
            days = 90
        category_filter = (request.args.get("category") or "").strip()
        flavor_filter = (request.args.get("flavor") or "").strip().upper()

        from src.core.db import get_db
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=days)).date().isoformat()

        sql = """
            SELECT category, accepted, COUNT(*) as n
            FROM intel_acceptance_log
            WHERE recorded_at >= ?
        """
        params = [cutoff]
        if category_filter:
            sql += " AND category = ?"
            params.append(category_filter)
        if flavor_filter and flavor_filter in ("A", "B", "C"):
            sql += " AND flavor = ?"
            params.append(flavor_filter)
        sql += " GROUP BY category, accepted"

        with get_db() as conn:
            rows = conn.execute(sql, params).fetchall()

        by_cat = {}
        for r in rows:
            cat = r["category"]
            entry = by_cat.setdefault(cat, {"accepted": 0, "rejected": 0})
            if int(r["accepted"]):
                entry["accepted"] += int(r["n"])
            else:
                entry["rejected"] += int(r["n"])

        out = []
        total_acc = total_rej = 0
        for cat, e in by_cat.items():
            total = e["accepted"] + e["rejected"]
            rate = (round(100.0 * e["accepted"] / total, 1)
                    if total else None)
            out.append({
                "category": cat,
                "total": total,
                "accepted": e["accepted"],
                "rejected": e["rejected"],
                "accept_rate_pct": rate,
            })
            total_acc += e["accepted"]
            total_rej += e["rejected"]
        out.sort(key=lambda x: x["total"], reverse=True)

        grand_total = total_acc + total_rej
        grand_rate = (round(100.0 * total_acc / grand_total, 1)
                      if grand_total else None)

        return jsonify({
            "ok": True,
            "days": days,
            "category_filter": category_filter,
            "flavor_filter": flavor_filter,
            "total": grand_total,
            "accepted": total_acc,
            "rejected": total_rej,
            "accept_rate_pct": grand_rate,
            "by_category": out,
        })
    except Exception as e:
        log.exception("intel_acceptance_stats")
        return jsonify({"ok": False, "error": str(e)}), 500
