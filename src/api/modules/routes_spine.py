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
from datetime import datetime, timezone
from typing import Callable

from flask import Blueprint, Response, jsonify, request

import json

from src.spine import (
    Quote,
    SpineRenderMismatchError,
    SpineValidationError,
    SUPPORTED_UOM,
    init_db,
    iter_snapshots,
    latest_snapshot,
    read_event_log,
    read_quote,
    read_snapshot,
    render_quote_pdf,
    write_quote,
    write_snapshot,
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

        # Enforce the linear state machine at the trust boundary.
        # Without this, model_validate() validates the NEW state's
        # preconditions but never compares against the prior status,
        # so _ALLOWED_TRANSITIONS in spine/model.py is dead code on
        # the POST path. Found by CHROME_WALKTHROUGH_GATE W-S-006.
        try:
            prior_quote = read_quote(db_path, quote_id)
        except Exception as e:
            log.exception("spine.post_quote_state: prior-state load failed for %s", quote_id)
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if prior_quote is not None and prior_quote.status != quote.status:
            try:
                prior_quote.with_status(quote.status)
            except SpineValidationError as e:
                return jsonify({
                    "error": "state_transition_rejected",
                    "detail": str(e),
                }), 409

            # Snapshot precondition for finalized → sent (W-S-009):
            # a quote may only enter "sent" if the operator has approved
            # a snapshot whose state_json matches the about-to-be-sent
            # state byte-for-byte. This is the structural enforcement
            # of "the bytes shipped to the buyer are the bytes the
            # operator approved." Without it, an operator can edit a
            # finalized quote and ship the edited version, with the
            # displayed-vs-delivered drift the 5/15 incident class is
            # built on.
            #
            # Identity comparison excludes:
            #   - updated_at: timestamp on the same shape, not part of it
            #   - status: snapshot is taken at finalized; we're entering
            #     sent — they will always differ on status by design
            if quote.status.value == "sent":
                snap = latest_snapshot(db_path, quote_id)
                if snap is None:
                    return jsonify({
                        "error": "state_transition_rejected",
                        "detail": (
                            "finalized → sent requires a snapshot to exist. "
                            "POST /spine/quotes/<id>/snapshot before "
                            "transitioning to sent. The snapshot IS the "
                            "audit record of what was approved for ship."
                        ),
                    }), 409
                if not _identity_matches(snap, quote):
                    return jsonify({
                        "error": "state_transition_rejected",
                        "detail": (
                            "finalized → sent rejected: current state has "
                            "diverged from the latest approved snapshot "
                            f"({snap['snapshot_id']}). Re-snapshot to "
                            "approve the current state, or revert your "
                            "edits to match the approved snapshot."
                        ),
                    }), 409

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

    # ─── GET /spine/quotes/<quote_id>/edit (operator UI) ──────────────

    @spine_bp.route("/spine/quotes/<quote_id>/edit", methods=["GET"])
    @_wrap
    def edit_quote(quote_id: str):
        """Render the operator editor for a Spine quote.

        Reads the canonical Quote, renders spine_pc_detail.html with
        the current state. The template's Save button POSTs the full
        state back to /spine/quotes/<id>/state — one POST per click,
        no per-keystroke autosave.
        """
        try:
            from flask import render_template
        except Exception:
            return jsonify({"error": "template_engine_unavailable"}), 500

        quote = read_quote(db_path, quote_id)
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        # Surface the latest snapshot (if any) so the editor can show
        # the lock banner, the "matches current state" indicator, and
        # gate the Mark-Sent button on snapshot freshness. The send
        # endpoint enforces the same rule server-side; the UI just
        # gives the operator an early-warning signal.
        snap = latest_snapshot(db_path, quote_id)
        snap_matches_current = _identity_matches(snap, quote) if snap else False

        # Pre-compute display values so the template stays logic-light.
        return render_template(
            "spine_pc_detail.html",
            quote=quote.to_persisted_dict(),
            latest_snap=snap,
            snap_matches_current=snap_matches_current,
            supported_uom=SUPPORTED_UOM,
            # Pre-formatted computed fields for display (model still
            # owns the math; template just shows the values).
            subtotal_display=f"${quote.subtotal_cents/100:,.2f}",
            tax_display=f"${quote.tax_cents/100:,.2f}",
            total_display=f"${quote.total_cents/100:,.2f}",
            tax_rate_pct=f"{quote.tax_rate_bps/100:.2f}",
            line_displays=[
                {
                    "line_no": li.line_no,
                    "description": li.description,
                    "mfg_number": li.mfg_number or "",
                    "qty": li.qty,
                    "uom": li.uom,
                    "cost_dollars": f"{li.cost_cents/100:.2f}",
                    "unit_price_dollars": f"{li.unit_price_cents/100:.2f}",
                    "extension_display": f"${li.extension_cents/100:,.2f}",
                    # Editable markup input takes a numeric value, not a
                    # formatted string. Falls back to empty string when
                    # cost is zero (markup undefined — operator sees
                    # the empty cell as "type a markup to set price").
                    "markup_value": (
                        f"{li.markup_pct_display:.1f}"
                        if li.markup_pct_display is not None else ""
                    ),
                    "cost_source_url": li.cost_source_url or "",
                }
                for li in quote.line_items
            ],
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

    # ─── POST /spine/quotes/<quote_id>/snapshot ───────────────────────
    #
    # Renders the current Quote to PDF, runs the matching gate, persists
    # the bytes + sha256 + state to spine_quote_snapshots. The snapshot
    # is the immutable, byte-identical record of what was approved for
    # ship. Status must be `finalized` (operator confirmed pricing) or
    # `sent` (re-snapshot of a sent quote is legal for audit replay).
    # Returns 200 with snapshot_id + sha256.
    #
    # This endpoint IS the matching gate at the trust boundary: a row
    # in spine_quote_snapshots is the proof that displayed == persisted
    # == about-to-be-delivered.

    @spine_bp.route("/spine/quotes/<quote_id>/snapshot", methods=["POST"])
    @_wrap
    def post_quote_snapshot(quote_id: str):
        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            log.exception("spine.post_snapshot: load failed for %s", quote_id)
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        if quote.status.value not in ("finalized", "sent"):
            return jsonify({
                "error": "snapshot_precondition_failed",
                "detail": (
                    f"snapshot requires status in (finalized, sent); "
                    f"current status is {quote.status.value}. "
                    "Transition to finalized first."
                ),
            }), 409

        actor = (
            request.headers.get("X-Spine-Actor")
            or _try_session_user()
            or "operator"
        )
        note = request.headers.get("X-Spine-Note")

        try:
            result = write_snapshot(db_path, quote, actor=actor, note=note)
        except SpineRenderMismatchError as e:
            # The gate caught a render-vs-model divergence. Surface
            # the detail so the operator sees exactly which cell lies.
            log.error("spine.post_snapshot: render gate caught divergence for %s: %s",
                      quote_id, e)
            return jsonify({
                "error": "render_mismatch",
                "detail": str(e),
            }), 409
        except SpineValidationError as e:
            return jsonify({"error": "snapshot_rejected", "detail": str(e)}), 409
        except Exception as e:
            log.exception("spine.post_snapshot: write failed for %s", quote_id)
            return jsonify({"error": "snapshot_failed", "detail": str(e)}), 500

        return jsonify(result), 200

    # ─── GET /spine/quotes/<quote_id>/snapshots ───────────────────────
    #
    # Newest-first list of snapshot metadata (NO bytes). Operator UI
    # uses this to display the audit chain ("Snapshot snap_abc... at
    # 2026-05-15 21:30 by operator — sha256 14c580...").

    @spine_bp.route("/spine/quotes/<quote_id>/snapshots", methods=["GET"])
    @_wrap
    def get_quote_snapshots(quote_id: str):
        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404
        snaps = iter_snapshots(db_path, quote_id)
        # Strip state_json from list view — it's large and only
        # needed by the snapshot-detail endpoint.
        return jsonify({
            "quote_id": quote_id,
            "snapshots": [{k: v for k, v in s.items() if k != "state_json"}
                          for s in snaps],
        })

    # ─── GET /spine/quotes/<quote_id>/oracle-suggestions ──────────────
    #
    # Read-only window into the Pricing Oracle. Oracle SUGGESTS;
    # operator DECIDES; substrate STORES only operator-typed values.
    # See project_spine_oracle_wiring_plan_2026_05_15.md (memory) for
    # the architectural rule. This endpoint returns FIXTURE data in
    # v1 — PR-O4 will swap the body in oracle_proxy.suggestions_for_quote
    # to call the real parent-repo oracle. The JSON shape is the
    # contract and is preserved across that swap.

    @spine_bp.route(
        "/spine/quotes/<quote_id>/oracle-suggestions",
        methods=["GET"],
    )
    @_wrap
    def get_oracle_suggestions(quote_id: str):
        from src.spine_bridge import suggestions_for_quote, suggestion_to_dict

        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        suggestions = suggestions_for_quote(quote)
        return jsonify({
            "quote_id": quote_id,
            "oracle_version": "fixture-v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "lines": [suggestion_to_dict(s) for s in suggestions],
        })

    # ─── GET /spine/quotes/<quote_id>/snapshot/<sid>/pdf ──────────────
    #
    # Stream the immutable PDF bytes for a specific snapshot. This is
    # what the send-to-buyer path reads — never a fresh render. URL
    # carries both quote_id (for auth/scope checks) and snapshot_id
    # (for the bytes).

    @spine_bp.route(
        "/spine/quotes/<quote_id>/snapshot/<snapshot_id>/pdf",
        methods=["GET"],
    )
    @_wrap
    def get_snapshot_pdf(quote_id: str, snapshot_id: str):
        snap = read_snapshot(db_path, snapshot_id)
        if snap is None:
            return jsonify({
                "error": "not_found",
                "snapshot_id": snapshot_id,
            }), 404
        # Scope check: snapshot_id must belong to the quote in the URL.
        if snap["quote_id"] != quote_id:
            return jsonify({
                "error": "snapshot_scope_mismatch",
                "url_quote_id": quote_id,
                "snapshot_quote_id": snap["quote_id"],
            }), 400
        inline = request.args.get("inline", "1") != "0"
        disposition = (
            f'inline; filename="snapshot_{snapshot_id}.pdf"'
            if inline else
            f'attachment; filename="snapshot_{snapshot_id}.pdf"'
        )
        return Response(
            bytes(snap["pdf_bytes"]),
            mimetype="application/pdf",
            headers={
                "Content-Disposition": disposition,
                "X-Spine-Snapshot-Sha256": snap["sha256"],
                "X-Spine-Snapshot-CreatedAt": snap["created_at"],
            },
        )

    return spine_bp


# Fields excluded from snapshot ↔ live-quote identity comparison.
# updated_at: ticks on every save; not part of the approved shape.
# status:     snapshot was taken at finalized, comparison happens when
#             the operator is transitioning to sent — by construction
#             they differ on status; that's not divergence.
_IDENTITY_EXCLUDED_FIELDS = ("updated_at", "status")


def _identity_matches(snap: dict, quote: Quote) -> bool:
    """True iff the snapshot's persisted state == quote.to_persisted_dict()
    after dropping the excluded fields.

    This is the single source of truth for "the live quote still
    matches what the operator approved." Both the route precondition
    (send blocking) and the template indicator (Mark-Sent disable)
    use this; drift between them would itself be a bug class.
    """
    snap_identity = {
        k: v for k, v in json.loads(snap["state_json"]).items()
        if k not in _IDENTITY_EXCLUDED_FIELDS
    }
    quote_identity = {
        k: v for k, v in quote.to_persisted_dict().items()
        if k not in _IDENTITY_EXCLUDED_FIELDS
    }
    return snap_identity == quote_identity


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
