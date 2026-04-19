"""Locked-cost admin panel.

Surfaces the supplier_costs table so the operator can see which prices
are pinned, when they expire, and clear stale ones before they wreck a
quote's margin.

Routes:
    GET  /admin/locked-costs                          — HTML panel
    GET  /api/admin/locked-costs                      — JSON: all locked rows
    POST /api/admin/locked-costs/unlock               — Delete by description
    POST /api/admin/locked-costs/extend               — Push expiry out N days
"""
import logging
import sqlite3
from datetime import datetime, timedelta

from flask import jsonify, request

from src.api.shared import bp, auth_required

log = logging.getLogger(__name__)


def _db():
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.row_factory = sqlite3.Row
    return db


def _classify(expires_at: str) -> str:
    """active | expiring (within 7 days) | expired."""
    if not expires_at:
        return "active"
    try:
        dt = datetime.fromisoformat(expires_at.split("+")[0].replace("Z", ""))
    except Exception:
        return "active"
    now = datetime.now()
    if dt < now:
        return "expired"
    if dt < now + timedelta(days=7):
        return "expiring"
    return "active"


def _fetch_all(limit: int = 500) -> list[dict]:
    db = _db()
    try:
        rows = db.execute(
            "SELECT description, item_number, cost, supplier, source, source_url, "
            "confirmed_at, expires_at, notes FROM supplier_costs "
            "ORDER BY confirmed_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    except Exception as e:
        log.debug("locked-costs query failed: %s", e)
        rows = []
    finally:
        db.close()

    out = []
    for r in rows:
        d = dict(r)
        d["status"] = _classify(d.get("expires_at") or "")
        out.append(d)
    return out


def _summary(rows: list[dict]) -> dict:
    totals = {"total": len(rows), "active": 0, "expiring": 0, "expired": 0}
    for r in rows:
        totals[r["status"]] = totals.get(r["status"], 0) + 1
    return totals


@bp.route("/admin/locked-costs")
@auth_required
def locked_costs_page():
    """Locked supplier costs panel."""
    from src.api.render import render_page
    rows = _fetch_all(limit=500)
    return render_page(
        "locked_costs.html",
        active_page="Admin",
        rows=rows,
        summary=_summary(rows),
    )


@bp.route("/api/admin/locked-costs")
@auth_required
def api_locked_costs():
    """JSON list of locked costs with status classifications."""
    limit = int(request.args.get("limit", 500))
    rows = _fetch_all(limit=limit)
    return jsonify({"ok": True, "rows": rows, "summary": _summary(rows)})


@bp.route("/api/admin/locked-costs/unlock", methods=["POST"])
@auth_required
def api_locked_costs_unlock():
    """Delete a locked cost. Body: {description, supplier}."""
    body = request.get_json(silent=True) or {}
    desc = (body.get("description") or "").strip()
    supplier = (body.get("supplier") or "").strip()
    if not desc:
        return jsonify({"ok": False, "error": "description required"}), 400
    db = _db()
    try:
        cur = db.execute(
            "DELETE FROM supplier_costs WHERE description = ? AND supplier = ?",
            (desc, supplier),
        )
        db.commit()
        deleted = cur.rowcount
    finally:
        db.close()
    return jsonify({"ok": True, "deleted": deleted})


@bp.route("/api/admin/locked-costs/extend", methods=["POST"])
@auth_required
def api_locked_costs_extend():
    """Push the expiry of a lock out by N days. Body: {description, supplier, days}."""
    body = request.get_json(silent=True) or {}
    desc = (body.get("description") or "").strip()
    supplier = (body.get("supplier") or "").strip()
    days = int(body.get("days", 30))
    if not desc:
        return jsonify({"ok": False, "error": "description required"}), 400
    if days < 1 or days > 365:
        return jsonify({"ok": False, "error": "days must be 1-365"}), 400
    new_expiry = (datetime.now() + timedelta(days=days)).isoformat()
    db = _db()
    try:
        cur = db.execute(
            "UPDATE supplier_costs SET expires_at = ? "
            "WHERE description = ? AND supplier = ?",
            (new_expiry, desc, supplier),
        )
        db.commit()
        updated = cur.rowcount
    finally:
        db.close()
    return jsonify({"ok": True, "updated": updated, "new_expiry": new_expiry})
