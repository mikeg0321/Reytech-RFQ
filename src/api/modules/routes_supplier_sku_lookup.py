# routes_supplier_sku_lookup.py
#
# Phase 1.7 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-25).
# Reverse lookup: buyer-quoted supplier SKU → manufacturer part number.
# Powered by the supplier_skus table (migration 30).

import os
import tempfile
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


@bp.route("/api/admin/import-supplier-skus", methods=["POST"])
@auth_required
def api_admin_import_supplier_skus():
    """One-shot importer for a supplier-SKU CSV.

    Body: raw CSV text. Header row required:
        Type,Item,Description,Preferred Vendor,MPN
    Same shape as the McKesson catalog export.

    Query params:
        dry_run=1   — parse but don't write
        supplier=…  — override target supplier name (default 'mckesson')

    Idempotent: re-uploading the same CSV refreshes timestamps + descriptions
    via UPSERT, no duplicates.
    """
    csv_body = request.get_data(as_text=True)
    if not csv_body or len(csv_body) < 50:
        return jsonify({"ok": False, "error": "empty or tiny body"}), 400

    dry_run = request.args.get("dry_run", "0") in ("1", "true", "yes")
    supplier_override = (request.args.get("supplier") or "").strip().lower()

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False,
            encoding="utf-8", newline="",
        ) as fh:
            fh.write(csv_body)
            tmp_path = fh.name

        from scripts.import_mckesson_catalog import import_csv
        if supplier_override:
            from scripts import import_mckesson_catalog as _imp
            _orig = _imp.SUPPLIER_NAME
            _imp.SUPPLIER_NAME = supplier_override
            try:
                result = import_csv(tmp_path, dry_run=dry_run)
            finally:
                _imp.SUPPLIER_NAME = _orig
        else:
            result = import_csv(tmp_path, dry_run=dry_run)
    except Exception as e:
        log.exception("supplier-skus import")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    return jsonify(result)
