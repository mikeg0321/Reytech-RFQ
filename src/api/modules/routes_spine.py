"""routes_spine.py — Flask routes for The Spine.

This module is the transport layer for the Spine. It lives under
src/api/modules/ (per project convention) — NOT under src/spine/ —
because the Spine package itself is Flask-free by Charter rule #11.

Endpoints (single writer, single reader, zero fan-out):

    GET    /spine/quotes/<quote_id>          → canonical Quote JSON
    POST   /spine/quotes/<quote_id>/state    → persist full Quote state
    GET    /spine/quotes/<quote_id>/pdf      → rendered Quote PDF bytes
    GET    /spine/quotes/<quote_id>/events   → append-only event log

Body validation happens inside Quote.model_validate() — extra='forbid'
rejects unknown fields with HTTP 422. State-machine + cost-basis
preconditions raise SpineValidationError → HTTP 409. Storage write
errors → 500.

The factory `make_spine_blueprint(db_path, auth_decorator=...)` lets
tests register the same route definitions on an isolated Flask app
without dragging in dashboard's globals. Prod calls the factory at
module load and registers the result on the shared dashboard bp.
"""
from __future__ import annotations

import logging
import os
from typing import Callable

from flask import Blueprint, Response, jsonify, request

from src.spine import (
    Quote,
    SpineValidationError,
    init_db,
    read_event_log,
    read_quote,
    render_quote_pdf,
    write_quote,
)

log = logging.getLogger("reytech.spine")


# ──────────────────────────────────────────────────────────────────────
# Factory — used by both prod wiring (below) and tests.
# ──────────────────────────────────────────────────────────────────────


def make_spine_blueprint(
    db_path: str,
    *,
    auth_decorator: Callable | None = None,
) -> Blueprint:
    """Build a Flask Blueprint with the Spine's HTTP surface.

    Args:
        db_path: SQLite path for the spine_quotes table. Caller is
            responsible for having called init_db(db_path) already.
        auth_decorator: Optional decorator to wrap every route (prod
            passes auth_required; tests pass None to bypass).

    Returns:
        A Blueprint ready to register on a Flask app or another bp.
    """
    spine_bp = Blueprint("spine", __name__)

    def _wrap(f: Callable) -> Callable:
        return auth_decorator(f) if auth_decorator else f

    # ─── GET /spine/quotes/<quote_id> ─────────────────────────────────

    @spine_bp.route("/spine/quotes/<quote_id>", methods=["GET"])
    @_wrap
    def get_quote(quote_id: str):
        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            log.exception("spine.get_quote: load failed for %s", quote_id)
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404
        return jsonify(quote.to_persisted_dict())

    # ─── POST /spine/quotes/<quote_id>/state ──────────────────────────

    @spine_bp.route("/spine/quotes/<quote_id>/state", methods=["POST"])
    @_wrap
    def post_quote_state(quote_id: str):
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({
                "error": "bad_body",
                "detail": "request body must be a JSON object",
            }), 400

        # URL ID must match body ID — defense in depth against caller
        # bugs that POST to one ID with another ID's payload.
        if body.get("quote_id") != quote_id:
            return jsonify({
                "error": "quote_id_mismatch",
                "url_quote_id": quote_id,
                "body_quote_id": body.get("quote_id"),
            }), 400

        try:
            quote = Quote.model_validate(body)
        except Exception as e:
            # Pydantic ValidationError (extra-forbidden, type mismatch,
            # missing field, etc.) → 422. The full Pydantic error
            # structure goes back to the client so the UI can highlight
            # the offending field.
            log.info("spine.post_quote_state: validation failed for %s: %s",
                     quote_id, e)
            return jsonify({
                "error": "validation_failed",
                "detail": str(e),
            }), 422

        actor = (
            request.headers.get("X-Spine-Actor")
            or _try_session_user()
            or "operator"
        )
        note = request.headers.get("X-Spine-Note")

        try:
            persisted = write_quote(db_path, quote, actor=actor, note=note)
        except SpineValidationError as e:
            # State-machine / business-rule violation (e.g., tried to
            # overwrite a sent quote). Distinct from validation_failed
            # — the body parsed fine, the state transition is illegal.
            return jsonify({
                "error": "state_transition_rejected",
                "detail": str(e),
            }), 409
        except Exception as e:
            log.exception("spine.post_quote_state: write failed for %s", quote_id)
            return jsonify({"error": "write_failed", "detail": str(e)}), 500

        return jsonify(persisted.to_persisted_dict()), 200

    # ─── GET /spine/quotes/<quote_id>/pdf ─────────────────────────────

    @spine_bp.route("/spine/quotes/<quote_id>/pdf", methods=["GET"])
    @_wrap
    def get_quote_pdf(quote_id: str):
        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            log.exception("spine.get_quote_pdf: load failed for %s", quote_id)
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        try:
            pdf_bytes = render_quote_pdf(quote)
        except Exception as e:
            log.exception("spine.get_quote_pdf: render failed for %s", quote_id)
            return jsonify({"error": "render_failed", "detail": str(e)}), 500

        # inline so iframes / Chrome can preview without forcing download.
        inline = request.args.get("inline", "1") != "0"
        disposition = (
            f'inline; filename="spine_quote_{quote_id}.pdf"'
            if inline else
            f'attachment; filename="spine_quote_{quote_id}.pdf"'
        )
        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={"Content-Disposition": disposition},
        )

    # ─── GET /spine/quotes/<quote_id>/events ──────────────────────────

    @spine_bp.route("/spine/quotes/<quote_id>/events", methods=["GET"])
    @_wrap
    def get_quote_events(quote_id: str):
        events = read_event_log(db_path, quote_id)
        if not events:
            # Distinguish "no events" (impossible — write_quote always
            # appends) from "not found".
            try:
                quote = read_quote(db_path, quote_id)
            except Exception:
                quote = None
            if quote is None:
                return jsonify({"error": "not_found", "quote_id": quote_id}), 404
        return jsonify({"quote_id": quote_id, "events": events})

    return spine_bp


def _try_session_user() -> str | None:
    """Best-effort: return logged-in operator name from Flask session.

    Falls back to None if session isn't available or not populated —
    tests typically run without session, prod operator login populates
    it. The route falls back to 'operator' if both this and the
    X-Spine-Actor header are absent.
    """
    try:
        from flask import session
        u = session.get("user")
        if isinstance(u, str) and u.strip():
            return u.strip()
    except Exception:
        return None
    return None


# ──────────────────────────────────────────────────────────────────────
# Prod wiring — runs when dashboard.py exec()s this module.
# Tests use make_spine_blueprint() directly with a tmp db_path.
# ──────────────────────────────────────────────────────────────────────

SPINE_DB_PATH = os.environ.get(
    "SPINE_DB_PATH",
    # Fall back to a path adjacent to the legacy DATA_DIR. Importing
    # DATA_DIR is allowed here (this is the wiring layer, not src/spine/).
    None,
)
if SPINE_DB_PATH is None:
    try:
        from src.core.paths import DATA_DIR
        SPINE_DB_PATH = str(os.path.join(str(DATA_DIR), "spine.db"))
    except Exception:
        # Last-ditch fallback for environments where paths.py is unavailable.
        SPINE_DB_PATH = os.path.join(os.getcwd(), "data", "spine.db")

# Initialize schema (idempotent).
try:
    init_db(SPINE_DB_PATH)
    log.info("spine: init_db OK at %s", SPINE_DB_PATH)
except Exception as e:
    log.exception("spine: init_db failed at %s", SPINE_DB_PATH)

# Register on the shared dashboard blueprint with prod auth.
# Idempotent: if 'spine' is already nested on bp (this module re-imported
# in the same process — happens under pytest's test isolation, importlib
# reloads, or dashboard's exec-loader running alongside a direct import),
# skip the second registration. A second register_blueprint call on bp
# would queue the same name twice, then crash with
# "name 'spine' is already registered for a different blueprint
# 'dashboard.spine'" when bp is finally registered on an app.
try:
    from src.api.shared import bp, auth_required

    _already_nested = False
    try:
        _already_nested = any(
            getattr(child, "name", None) == "spine"
            for child, _opts in getattr(bp, "_blueprints", [])
        )
    except Exception:
        _already_nested = False

    if not _already_nested:
        _spine_prod_bp = make_spine_blueprint(SPINE_DB_PATH, auth_decorator=auth_required)
        bp.register_blueprint(_spine_prod_bp)
        log.info("spine: routes registered on dashboard bp")
    else:
        log.info("spine: already registered on dashboard bp — idempotent skip")
except Exception:
    # If we're being imported standalone (e.g., a test directly imports
    # this module), shared.bp may not be available. The factory is
    # still exported and tests can call it themselves.
    log.exception("spine: dashboard wiring skipped (shared.bp unavailable)")
