# routes_supplier_sku_lookup.py
#
# Phase 1.7 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-25).
# Reverse lookup: buyer-quoted supplier SKU → manufacturer part number.
# Powered by the supplier_skus table (migration 30).

from flask import jsonify, request
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")


@bp.route("/api/catalog/supplier-sku-lookup")
@auth_required
def api_supplier_sku_lookup():
    """Look up an MFG# given a supplier and a supplier SKU.

    Query params:
        supplier (str, required): supplier name (e.g. "mckesson")
        sku (str, required): the supplier's part/item number

    Response:
        200 {ok: true, supplier, sku, mfg_number, description}  if found
        404 {ok: false, error: "not_found"}                     if missing
        400 {ok: false, error: "..."}                           on bad input
    """
    supplier = (request.args.get("supplier") or "").strip().lower()
    sku = (request.args.get("sku") or "").strip()
    if not supplier or not sku:
        return jsonify({"ok": False, "error": "supplier and sku required"}), 400

    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT supplier, supplier_sku, mfg_number, description "
                "FROM supplier_skus WHERE supplier=? AND supplier_sku=?",
                (supplier, sku),
            ).fetchone()
    except Exception as e:
        log.exception("supplier_sku_lookup")
        return jsonify({"ok": False, "error": str(e)}), 500

    if not row:
        return jsonify({"ok": False, "error": "not_found",
                        "supplier": supplier, "sku": sku}), 404

    return jsonify({
        "ok": True,
        "supplier": row["supplier"],
        "sku": row["supplier_sku"],
        "mfg_number": row["mfg_number"] or "",
        "description": row["description"] or "",
    })


@bp.route("/api/catalog/supplier-skus-stats")
@auth_required
def api_supplier_skus_stats():
    """Counts of imported supplier SKUs, broken down by supplier.

    Useful for /health/quoting to surface "1,260 McKesson SKUs imported,
    last refreshed 2h ago".
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT supplier, COUNT(*) AS count,
                       MAX(updated_at) AS last_updated
                FROM supplier_skus
                GROUP BY supplier
                ORDER BY count DESC
            """).fetchall()
            total = conn.execute(
                "SELECT COUNT(*) AS c FROM supplier_skus"
            ).fetchone()
    except Exception as e:
        log.exception("supplier_skus_stats")
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({
        "ok": True,
        "total": int(total["c"] if total else 0),
        "by_supplier": [
            {"supplier": r["supplier"], "count": r["count"],
             "last_updated": r["last_updated"]}
            for r in rows
        ],
    })
