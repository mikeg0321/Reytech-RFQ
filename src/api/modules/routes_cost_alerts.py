# routes_cost_alerts.py
#
# Phase 4.3 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-26).
# Catalog-cost change detection — operator-facing version of "your cost
# went up 18% on the gloves you've been quoting flat."
#
# v1: scans the existing price_history table for items where the
#     most-recent supplier price differs >threshold% from a prior
#     known price. No new scraping — uses signals already collected
#     by the regular pricing-lookup pipelines.
# v2 (future): scheduled background worker scrapes catalog rows
#     quoted in the last 30 days, regardless of whether a fresh
#     pricing pull happened.

import logging
from collections import defaultdict
from datetime import datetime, timedelta

from flask import jsonify, request

from src.api.shared import bp, auth_required

log = logging.getLogger("reytech")


# Sources that are real costs (not reference ceilings). SCPRS / amazon
# retail intentionally excluded — those are reference data, not what we
# pay. See CLAUDE.md "Pricing Guard Rails".
_COST_SOURCES = ("manual", "catalog", "vendor", "won_quote", "operator",
                 "grainger", "uline", "msc", "fastenal")


def _ensure_table():
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cost_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mfg_number TEXT,
                description TEXT,
                source TEXT,
                prior_price REAL,
                new_price REAL,
                delta_pct REAL,
                prior_found_at TEXT,
                new_found_at TEXT,
                detected_at TEXT NOT NULL DEFAULT (datetime('now')),
                status TEXT NOT NULL DEFAULT 'pending',
                agency TEXT,
                quote_number TEXT
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_cost_alerts_dedup
            ON cost_alerts(mfg_number, source, new_found_at)
        """)


@bp.route("/api/admin/scan-cost-alerts", methods=["POST"])
@auth_required
def api_admin_scan_cost_alerts():
    """Scan price_history for items whose latest supplier price differs
    >threshold% from the prior known price for the same (mfg, source)
    pair. Inserts into cost_alerts (idempotent on (mfg, source, found_at)).

    Body params:
        threshold_pct (float, optional, default 10.0)
        dry_run (bool, optional, default False)
        recent_days (int, optional, default 60) — only consider price
            history rows newer than this for the "new" side
    """
    body = request.json or {}
    try:
        threshold_pct = max(0.5, float(body.get("threshold_pct", 10.0)))
    except (TypeError, ValueError):
        threshold_pct = 10.0
    try:
        recent_days = max(1, min(365, int(body.get("recent_days", 60))))
    except (TypeError, ValueError):
        recent_days = 60
    dry_run = bool(body.get("dry_run", False))

    cutoff = (datetime.now() - timedelta(days=recent_days)).date().isoformat()

    result = {
        "ok": True,
        "dry_run": dry_run,
        "threshold_pct": threshold_pct,
        "recent_days": recent_days,
        "scanned_items": 0,
        "alerts_inserted": 0,
        "alerts_skipped_dupe": 0,
        "errors": [],
    }

    _ensure_table()

    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Pull recent price-history rows for cost-source items only
            placeholders = ",".join("?" * len(_COST_SOURCES))
            rows = conn.execute(
                f"""SELECT part_number, description, source,
                          unit_price, found_at
                    FROM price_history
                    WHERE part_number IS NOT NULL
                      AND part_number != ''
                      AND unit_price > 0
                      AND source IN ({placeholders})
                    ORDER BY part_number, source, found_at""",
                _COST_SOURCES,
            ).fetchall()

            # Group by (mfg, source) so we can spot deltas chronologically
            grouped = defaultdict(list)
            for r in rows:
                grouped[(r["part_number"], r["source"])].append(r)

            for (mfg, src), pairs in grouped.items():
                if len(pairs) < 2:
                    continue
                # Newest price
                newest = pairs[-1]
                if newest["found_at"] < cutoff:
                    continue
                # Compare against second-newest from a prior date
                prior = None
                for r in reversed(pairs[:-1]):
                    if r["found_at"] < newest["found_at"]:
                        prior = r
                        break
                if prior is None:
                    continue
                result["scanned_items"] += 1

                prior_p = float(prior["unit_price"] or 0)
                new_p = float(newest["unit_price"] or 0)
                if prior_p <= 0 or new_p <= 0:
                    continue
                delta_pct = round(100.0 * (new_p - prior_p) / prior_p, 2)
                if abs(delta_pct) < threshold_pct:
                    continue

                if dry_run:
                    result["alerts_inserted"] += 1
                    continue

                try:
                    conn.execute("""
                        INSERT INTO cost_alerts
                        (mfg_number, description, source, prior_price,
                         new_price, delta_pct, prior_found_at, new_found_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (mfg, newest["description"] or prior["description"] or "",
                          src, prior_p, new_p, delta_pct,
                          prior["found_at"], newest["found_at"]))
                    result["alerts_inserted"] += 1
                except Exception as e:
                    # UNIQUE collision = already alerted, harmless
                    if "UNIQUE constraint" in str(e):
                        result["alerts_skipped_dupe"] += 1
                    else:
                        result["errors"].append(f"{mfg}/{src}: {e}")
    except Exception as e:
        log.exception("scan-cost-alerts")
        return jsonify({"ok": False, "error": str(e)}), 500

    log.info(
        "cost_alerts scan: scanned=%d inserted=%d skipped_dupe=%d "
        "errors=%d threshold=%.1f%% dry_run=%s",
        result["scanned_items"], result["alerts_inserted"],
        result["alerts_skipped_dupe"], len(result["errors"]),
        threshold_pct, dry_run,
    )
    return jsonify(result)


@bp.route("/api/admin/cost-alerts")
@auth_required
def api_admin_cost_alerts():
    """List pending cost alerts. Newest first.

    Query params:
        status (str, optional, default 'pending') — pending|dismissed|applied|all
        limit (int, optional, default 50)
    """
    status = (request.args.get("status") or "pending").strip().lower()
    try:
        limit = max(1, min(500, int(request.args.get("limit", "50"))))
    except (TypeError, ValueError):
        limit = 50

    _ensure_table()

    try:
        from src.core.db import get_db
        with get_db() as conn:
            if status == "all":
                rows = conn.execute("""
                    SELECT * FROM cost_alerts
                    ORDER BY ABS(delta_pct) DESC, detected_at DESC
                    LIMIT ?
                """, (limit,)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT * FROM cost_alerts
                    WHERE status = ?
                    ORDER BY ABS(delta_pct) DESC, detected_at DESC
                    LIMIT ?
                """, (status, limit)).fetchall()
            count_pending = conn.execute(
                "SELECT COUNT(*) FROM cost_alerts WHERE status='pending'"
            ).fetchone()[0]
    except Exception as e:
        log.exception("cost-alerts list")
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({
        "ok": True,
        "filter_status": status,
        "limit": limit,
        "count_pending": count_pending,
        "alerts": [
            {
                "id": r["id"],
                "mfg_number": r["mfg_number"],
                "description": (r["description"] or "")[:120],
                "source": r["source"],
                "prior_price": r["prior_price"],
                "new_price": r["new_price"],
                "delta_pct": r["delta_pct"],
                "prior_found_at": r["prior_found_at"],
                "new_found_at": r["new_found_at"],
                "detected_at": r["detected_at"],
                "status": r["status"],
            }
            for r in rows
        ],
    })


@bp.route("/api/admin/cost-alerts/<int:alert_id>/status", methods=["POST"])
@auth_required
def api_admin_cost_alert_set_status(alert_id):
    """Mark an alert as dismissed or applied. Body: {status}."""
    body = request.json or {}
    new_status = (body.get("status") or "").strip().lower()
    if new_status not in ("pending", "dismissed", "applied"):
        return jsonify({"ok": False, "error": "invalid status"}), 400
    try:
        from src.core.db import get_db
        with get_db() as conn:
            cur = conn.execute(
                "UPDATE cost_alerts SET status=? WHERE id=?",
                (new_status, alert_id),
            )
            updated = bool(cur.rowcount)
        if not updated:
            return jsonify({"ok": False, "error": "not_found"}), 404
        return jsonify({"ok": True, "id": alert_id, "status": new_status})
    except Exception as e:
        log.exception("cost-alert-set-status")
        return jsonify({"ok": False, "error": str(e)}), 500
