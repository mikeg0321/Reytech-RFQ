"""
routes_intelligence.py — surviving non-flagged intelligence-layer routes.

Most of this module's original surface (document upload via docling,
NL query, compliance matrix extraction, bid/no-bid scoring, UNSPSC
batch retag, agency compliance templates) was deleted 2026-04-29 in
the Plan §3.3 flag sprint — those features were gated on
`docling_intake`, `nl_query_enabled`, `compliance_matrix`, `bid_scoring`,
and `unspsc_enrichment` feature flags that were never enabled in
production. The supporting agent modules (`compliance_extractor`,
`docling_parser`, `nl_query_agent`, `bid_decision_agent`,
`unspsc_classifier`) and their tests were deleted in the same PR.

What's left here is the two routes that had no feature flag and have
real callers:

  - GET /api/quotes/expiring        — 7-day expiry surface for /quotes
  - GET /api/contacts/<email>/address — ship-to auto-fill on quote pages

If these grow new sibling functionality, fine to add it here. If they
move to a more topical module (e.g. `routes_quotes_expiry.py`), this
file can be deleted entirely.
"""
import logging

log = logging.getLogger("reytech.routes_intelligence")


@bp.route("/api/quotes/expiring", methods=["GET"])
@auth_required
def api_quotes_expiring():
    """Get quotes expiring within 7 days."""
    try:
        from src.core.db import get_db
        from datetime import datetime, timedelta
        now = datetime.now()
        cutoff = (now + timedelta(days=7)).isoformat()
        with get_db() as conn:
            rows = conn.execute(
                """SELECT quote_number, agency, institution, total, expires_at, sent_at, contact_name, status
                   FROM quotes WHERE expires_at != '' AND expires_at <= ? AND status NOT IN ('won','lost','cancelled','expired')
                   ORDER BY expires_at ASC LIMIT 20""",
                (cutoff,)
            ).fetchall()
        results = []
        for row in rows:
            d = dict(row)
            try:
                exp = datetime.fromisoformat(d["expires_at"])
                d["days_remaining"] = max(0, (exp - now).days)
                d["severity"] = "critical" if d["days_remaining"] <= 3 else "warning"
            except (ValueError, TypeError):
                d["days_remaining"] = -1
                d["severity"] = "unknown"
            results.append(d)
        return jsonify({"ok": True, "expiring": results, "count": len(results)})
    except Exception as e:
        log.error("Expiring quotes error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/contacts/<email>/address", methods=["GET"])
@auth_required
def api_contact_address(email):
    """Lookup contact address by email for ship-to auto-fill."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT address, city, state, zip, ship_to_default, agency FROM contacts WHERE buyer_email = ?",
                (email,)
            ).fetchone()
            if not row:
                return jsonify({"ok": False, "error": "Contact not found"}), 404
            d = dict(row)
            d["ok"] = True
            return jsonify(d)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
