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
    contract_vs_quote,
    find_contract_for_quote,
    init_db,
    iter_quote_ids,
    iter_snapshots,
    latest_rejections,
    latest_snapshot,
    read_event_log,
    read_quote,
    read_snapshot,
    render_quote_pdf,
    write_quote,
    write_snapshot,
)
from src.spine.contract_diff import delta_to_dict

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

    # ─── GET /spine/quotes/ (index) ───────────────────────────────────
    # Operator entry point — every Spine quote in this DB as a clickable
    # link to the editor. Added 2026-05-17 because navigating to
    # /spine/quotes/ used to 404 with no way to discover IDs.

    @spine_bp.route("/spine/quotes/", methods=["GET"])
    @spine_bp.route("/spine/quotes", methods=["GET"])
    @_wrap
    def list_quotes():
        try:
            from flask import render_template
        except Exception:
            return jsonify({"error": "template_engine_unavailable"}), 500

        rows: list[dict] = []
        for qid in iter_quote_ids(db_path):
            q = read_quote(db_path, qid)
            if q is None:
                continue
            # Pull the link + carry hint so the operator can see at a
            # glance "this RFQ is linked to PC R26PC####".
            links = []
            try:
                from src.spine import find_links_from as _fl
                links = _fl(db_path, qid)
            except Exception:
                links = []
            top_link = links[0] if links else None
            rows.append({
                "quote_id": qid,
                "display_number": q.display_number,
                "agency": q.agency,
                "facility": q.facility,
                "solicitation_number": q.solicitation_number,
                "status": q.status.value,
                "line_count": len(q.line_items),
                "subtotal_display": f"${q.subtotal_cents / 100:,.2f}",
                "total_display": f"${q.total_cents / 100:,.2f}",
                "updated_at": q.updated_at.isoformat() if q.updated_at else "",
                "linked_to": top_link["to_quote_id"] if top_link else None,
                "link_confidence": top_link["confidence"] if top_link else None,
            })

        # Sort newest-first by updated_at.
        rows.sort(key=lambda r: r["updated_at"], reverse=True)

        # JSON shortcut for ops tools / smoke checks.
        if request.args.get("format") == "json":
            return jsonify({"count": len(rows), "quotes": rows})

        return render_template("spine_quotes_index.html", quotes=rows)

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

        # Strip server-side computed fields the editor template echoed
        # back. PR #1040 added display_number to the GET payload so the
        # editor title could render the R{yy}Q#### identifier; the
        # default JS Save flow round-trips the same dict back here, and
        # Quote's extra='forbid' refuses it. Computed fields are NEVER
        # stored — strip on the trust boundary.
        for _stripped in ("display_number", "subtotal_cents", "tax_cents", "total_cents"):
            body.pop(_stripped, None)
        for _li in body.get("line_items") or []:
            if isinstance(_li, dict):
                _li.pop("extension_cents", None)
                _li.pop("markup_pct_display", None)

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

    # ─── GET /spine/quotes/<quote_id>/contract ────────────────────────
    #
    # Returns the EmailContract that drove this quote's ingest — the
    # master ground-truth record of what the buyer asked for. The
    # contract is append-only; what you see here is byte-faithful to
    # what was extracted from the inbound RFQ.

    @spine_bp.route(
        "/spine/quotes/<quote_id>/contract",
        methods=["GET"],
    )
    @_wrap
    def get_quote_contract(quote_id: str):
        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        contract = find_contract_for_quote(db_path, quote_id)
        if contract is None:
            return jsonify({
                "error": "no_contract",
                "detail": (
                    "No EmailContract found for this quote. The quote "
                    "may have been ingested before the email-contract "
                    "substrate existed (pre-2026-05-16 data) or via a "
                    "path that did not call write_email_contract."
                ),
            }), 404
        return jsonify(contract.model_dump(mode="json"))

    # ─── GET /spine/quotes/<quote_id>/contract-diff ───────────────────
    #
    # Returns the per-field delta between the contract (buyer-stated
    # truth) and the current quote (operator state). Every override
    # is traceable to the field path, contract value, and operator
    # value. This is what gives operators + auditors a clean "what
    # changed and why" view at any point in the quote's lifecycle.

    @spine_bp.route(
        "/spine/quotes/<quote_id>/contract-diff",
        methods=["GET"],
    )
    @_wrap
    def get_quote_contract_diff(quote_id: str):
        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404
        contract = find_contract_for_quote(db_path, quote_id)
        if contract is None:
            return jsonify({
                "error": "no_contract",
                "detail": "no EmailContract found for this quote",
            }), 404

        deltas = contract_vs_quote(contract, quote)
        return jsonify({
            "quote_id": quote_id,
            "contract_id": contract.contract_id,
            "delta_count": len(deltas),
            "deltas": [delta_to_dict(d) for d in deltas],
            "clean": len(deltas) == 0,
        })

    # ─── GET /spine/quotes/<quote_id>/forms/703b/pdf ──────────────────
    #
    # CCHCS 703B — RFQ Informal Competitive cover sheet. Fills the
    # vendor identity + solicitation # from the Quote + ReytechIdentity
    # env config, runs the matching gate (SpineFormFillError on any
    # divergence), returns the bytes. ?fillable=1 leaves form widgets
    # for last-minute operator edits in Adobe; default is flat per
    # government convention.

    @spine_bp.route(
        "/spine/quotes/<quote_id>/forms/703b/pdf",
        methods=["GET"],
    )
    @_wrap
    def get_703b_pdf(quote_id: str):
        from src.spine.agency_forms import (
            ReytechIdentity, fill_703b_pdf, SpineFormFillError,
        )

        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        fillable = request.args.get("fillable", "0") == "1"
        try:
            pdf_bytes = fill_703b_pdf(
                quote,
                ReytechIdentity.from_env(),
                flatten=not fillable,
            )
        except SpineFormFillError as e:
            log.error("spine.get_703b: fill gate caught divergence for %s: %s",
                      quote_id, e)
            return jsonify({"error": "form_fill_mismatch", "detail": str(e)}), 409
        except FileNotFoundError as e:
            return jsonify({"error": "template_missing", "detail": str(e)}), 500
        except Exception as e:
            log.exception("spine.get_703b: fill failed for %s", quote_id)
            return jsonify({"error": "fill_failed", "detail": str(e)}), 500

        inline = request.args.get("inline", "1") != "0"
        disposition = (
            f'inline; filename="703b_{quote_id}.pdf"'
            if inline else
            f'attachment; filename="703b_{quote_id}.pdf"'
        )
        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={"Content-Disposition": disposition},
        )

    # ─── GET /spine/quotes/<quote_id>/forms/704b/pdf ──────────────────
    #
    # CCHCS 704B — line-item RFQ response (39-row capacity across two
    # pages). Same shape as 703B: pypdf /V writes + pikepdf appearance
    # streams + (default) flatten, two-path matching gate, ?fillable=1
    # escape hatch. Refuses Quotes with >39 line items until overflow
    # rendering ships (parent's reportlab pattern, follow-up PR).

    @spine_bp.route(
        "/spine/quotes/<quote_id>/forms/704b/pdf",
        methods=["GET"],
    )
    @_wrap
    def get_704b_pdf(quote_id: str):
        from src.spine.agency_forms import (
            ReytechIdentity, fill_704b_pdf, SpineFormFillError,
        )

        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        fillable = request.args.get("fillable", "0") == "1"
        try:
            pdf_bytes = fill_704b_pdf(
                quote,
                ReytechIdentity.from_env(),
                flatten=not fillable,
            )
        except SpineFormFillError as e:
            log.error("spine.get_704b: fill gate caught divergence for %s: %s",
                      quote_id, e)
            return jsonify({"error": "form_fill_mismatch", "detail": str(e)}), 409
        except FileNotFoundError as e:
            return jsonify({"error": "template_missing", "detail": str(e)}), 500
        except Exception as e:
            log.exception("spine.get_704b: fill failed for %s", quote_id)
            return jsonify({"error": "fill_failed", "detail": str(e)}), 500

        inline = request.args.get("inline", "1") != "0"
        disposition = (
            f'inline; filename="704b_{quote_id}.pdf"'
            if inline else
            f'attachment; filename="704b_{quote_id}.pdf"'
        )
        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={"Content-Disposition": disposition},
        )

    # ─── GET /spine/quotes/<quote_id>/forms/bidpkg/pdf ────────────────
    #
    # CCHCS Bid Package — multi-form identity bundle (CUF, Darfur,
    # Bidder Decl 105, DVBE 843, STD 21). Same fill pipeline + gate as
    # 703B/704B. ?fillable=1 escape hatch retained for last-mile edits.

    @spine_bp.route(
        "/spine/quotes/<quote_id>/forms/bidpkg/pdf",
        methods=["GET"],
    )
    @_wrap
    def get_bidpkg_pdf(quote_id: str):
        from src.spine.agency_forms import (
            ReytechIdentity, fill_bidpkg_pdf, SpineFormFillError,
        )

        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        fillable = request.args.get("fillable", "0") == "1"
        try:
            pdf_bytes = fill_bidpkg_pdf(
                quote,
                ReytechIdentity.from_env(),
                flatten=not fillable,
            )
        except SpineFormFillError as e:
            log.error("spine.get_bidpkg: fill gate caught divergence for %s: %s",
                      quote_id, e)
            return jsonify({"error": "form_fill_mismatch", "detail": str(e)}), 409
        except FileNotFoundError as e:
            return jsonify({"error": "template_missing", "detail": str(e)}), 500
        except Exception as e:
            log.exception("spine.get_bidpkg: fill failed for %s", quote_id)
            return jsonify({"error": "fill_failed", "detail": str(e)}), 500

        inline = request.args.get("inline", "1") != "0"
        disposition = (
            f'inline; filename="bidpkg_{quote_id}.pdf"'
            if inline else
            f'attachment; filename="bidpkg_{quote_id}.pdf"'
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
        # display_number (the buyer-facing R{yy}Q####) is a computed
        # field — to_persisted_dict() correctly excludes it (substrate
        # rule: no derived values in persisted state), so we surface it
        # to the template as a separate kwarg alongside the money strings.
        quote_dict = quote.to_persisted_dict()
        quote_dict["display_number"] = quote.display_number

        # Surface ship-to + tax provenance from the EmailContract so
        # the operator can visually confirm "this 7.75% is right for
        # this address" instead of trusting a number. Closes Mike's
        # 5/18 feedback: "7.75 is cdtfa confirmed, but theres not
        # logic or qa visual its good".
        contract = find_contract_for_quote(db_path, quote_id)
        ship_to_address = contract.ship_to_address if contract else None
        ship_to_facility = contract.ship_to_facility if contract else None
        # CDTFA verify URL pre-fills the address as best we can — the
        # CDTFA lookup form expects a single address string; operator
        # pastes if the deep link doesn't carry over.
        from urllib.parse import quote_plus
        cdtfa_verify_url = (
            f"https://maps.cdtfa.ca.gov/?address={quote_plus(ship_to_address or '')}"
            if ship_to_address
            else "https://maps.cdtfa.ca.gov/"
        )

        return render_template(
            "spine_pc_detail.html",
            quote=quote_dict,
            latest_snap=snap,
            snap_matches_current=snap_matches_current,
            supported_uom=SUPPORTED_UOM,
            ship_to_address=ship_to_address,
            ship_to_facility=ship_to_facility,
            cdtfa_verify_url=cdtfa_verify_url,
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

    # ─── GET /spine/quotes/<quote_id>/package ─────────────────────────
    #
    # The output-vs-contract gate. Returns the list of forms the package
    # WOULD include for this quote, validated against the linked
    # EmailContract's required_forms. Refuses 409 on any divergence:
    #   - no EmailContract bound to the quote
    #   - contract requires a form for which no Spine renderer is
    #     registered (FORM_REGISTRY gap)
    # The actual per-form bytes still come from the existing per-form
    # routes (/forms/703b/pdf, /forms/704b/pdf, etc.) — this endpoint
    # is the gate, not the bundler. Closes 5/15 finding #7 structurally:
    # operator cannot ship a packet that has more or fewer forms than
    # the buyer's email asked for.

    @spine_bp.route("/spine/quotes/<quote_id>/package", methods=["GET"])
    @_wrap
    def get_quote_package(quote_id: str):
        from src.spine.agency_forms import FORM_REGISTRY

        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            log.exception("spine.get_package: load failed for %s", quote_id)
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        contract = find_contract_for_quote(db_path, quote_id)
        if contract is None:
            return jsonify({
                "error": "no_contract",
                "detail": (
                    "no EmailContract linked to quote_id={}; the package "
                    "endpoint requires contract-defined required_forms. "
                    "Either ingest via spine_ingest with EmailContract or "
                    "write_email_contract directly before requesting "
                    "/package."
                ).format(quote_id),
            }), 409

        required = list(contract.required_forms)
        required_set = set(required)
        registered_set = set(FORM_REGISTRY.keys())
        missing = sorted(required_set - registered_set)
        if missing:
            return jsonify({
                "error": "renderer_missing",
                "detail": (
                    "contract requires forms {} but no Spine renderer "
                    "is registered. Either register one in "
                    "src/spine/agency_forms/__init__.py FORM_REGISTRY or "
                    "remove the form from contract.required_forms."
                ).format(missing),
                "required_forms": sorted(required_set),
                "registered_forms": sorted(registered_set),
                "missing": missing,
            }), 409

        # Build per-form URLs. The Quote PDF uses the existing /pdf route
        # (not /forms/quote/pdf — predates the agency-forms pattern).
        per_form_routes = {
            "quote":  f"/spine/quotes/{quote_id}/pdf",
            "703b":   f"/spine/quotes/{quote_id}/forms/703b/pdf",
            "704b":   f"/spine/quotes/{quote_id}/forms/704b/pdf",
            "bidpkg": f"/spine/quotes/{quote_id}/forms/bidpkg/pdf",
        }

        files = []
        for code in required:
            url = per_form_routes.get(code)
            if url is None:
                # FORM_REGISTRY has the renderer but no HTTP route yet —
                # surfaceable error so operator + CI catches the gap.
                return jsonify({
                    "error": "no_http_route",
                    "detail": (
                        "FORM_REGISTRY has renderer for {} but no "
                        "per-form HTTP route mapping in routes_spine."
                    ).format(code),
                    "missing_route_for": code,
                }), 500
            files.append({
                "form_code": code,
                "filename": f"{quote_id}_{code}.pdf",
                "url": url,
            })

        # Final identity check — rendered set MUST equal required set.
        # (Trivially true given the loop above, but explicit guards the
        # invariant against future loops that filter or reorder.)
        rendered_codes = {f["form_code"] for f in files}
        if rendered_codes != required_set:
            log.error(
                "spine.get_package: rendered_set=%r != required_set=%r "
                "(quote=%s) — substrate invariant violated",
                rendered_codes, required_set, quote_id,
            )
            return jsonify({
                "error": "output_contract_mismatch",
                "detail": (
                    "rendered forms {} do not match contract required "
                    "forms {} — substrate invariant violation"
                ).format(sorted(rendered_codes), sorted(required_set)),
            }), 409

        return jsonify({
            "quote_id": quote_id,
            "contract_id": contract.contract_id,
            "required_forms": required,
            "response_packaging": contract.response_packaging,
            "files": files,
        })

    # ─── GET /spine/queue/rejected ────────────────────────────────────
    #
    # Triage surface for the missed-bid silent-drop class. Every email
    # the ingest pipeline considered and refused emits a row to
    # spine_ingest_rejections; this route surfaces them newest-first
    # with optional reason_code filter. The Telegram missed-bid watcher
    # (queued) consumes this same read path.

    @spine_bp.route("/spine/queue/rejected", methods=["GET"])
    @_wrap
    def get_queue_rejected():
        # Validate limit at the route boundary so a bad param returns
        # 400 instead of 500.
        raw_limit = request.args.get("limit", "50").strip()
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            return jsonify({
                "error": "bad_request",
                "detail": f"limit must be an integer 1..1000; got {raw_limit!r}",
            }), 400
        if limit < 1 or limit > 1000:
            return jsonify({
                "error": "bad_request",
                "detail": f"limit must be 1..1000; got {limit}",
            }), 400

        reason_code = request.args.get("reason_code", "").strip() or None

        try:
            rows = latest_rejections(
                db_path, limit=limit, reason_code=reason_code,
            )
        except SpineValidationError as e:
            return jsonify({"error": "bad_request", "detail": str(e)}), 400
        except Exception as e:
            log.exception("spine.get_queue_rejected: read failed")
            return jsonify({"error": "read_failed", "detail": str(e)}), 500

        return jsonify({
            "count": len(rows),
            "limit": limit,
            "reason_code_filter": reason_code,
            "rejections": rows,
        })

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

        # Record the snapshot in the event log too, with a deep link
        # to the immutable bytes. Reviewer 2026-05-15: without this,
        # the audit chain split across spine_quote_snapshots and
        # event_log — the event log is the canonical chain operators
        # check, so a snapshot must show up there with everything an
        # auditor needs to retrieve the bytes.
        snap_url = (
            f"/spine/quotes/{quote_id}/snapshot/{result['snapshot_id']}/pdf"
        )
        snapshot_event_note = (
            f"snapshotted: id={result['snapshot_id']} "
            f"sha256={result['sha256'][:16]} "
            f"pdf_url={snap_url}"
            + (f" note={note}" if note else "")
        )
        try:
            write_quote(db_path, quote, actor=actor, note=snapshot_event_note)
        except Exception as e:
            # Event-log write failure must not lose the snapshot —
            # the bytes are already persisted. Log and surface a
            # warning in the response so monitoring can pick up.
            log.warning(
                "spine.post_snapshot: event-log write failed for %s: %s",
                quote_id, e,
            )

        # Echo the snapshot URL back so the UI can link to it without
        # constructing the URL client-side.
        return jsonify({**result, "snapshot_pdf_url": snap_url}), 200

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

    # ─── POST /spine/quotes/<quote_id>/send-prep ──────────────────────
    #
    # Prepare a send envelope: subject + body + the immutable
    # snapshot URL + Gmail-compose deep link. Closes the snapshot loop:
    # bytes that ship to the buyer ARE the snapshot bytes, never a
    # re-render.
    #
    # The Spine does not call Gmail itself — the parent app (or the
    # operator) handles the actual send. This endpoint produces a
    # complete, ready-to-fill compose URL plus a downloadable PDF
    # link. UI opens Gmail in a new tab; operator attaches the PDF
    # and clicks Send. Each prep call is recorded in the event log
    # via X-Spine-Note for full audit trail.

    @spine_bp.route("/spine/quotes/<quote_id>/send-prep", methods=["POST"])
    @_wrap
    def post_send_prep(quote_id: str):
        import re
        from urllib.parse import quote as urlencode_q

        body = request.get_json(silent=True) or {}
        to_raw = (body.get("to") or "").strip()
        cc_raw = (body.get("cc") or "").strip()
        if not to_raw:
            return jsonify({
                "error": "missing_recipient",
                "detail": "'to' is required",
            }), 400
        email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        if not email_re.match(to_raw):
            return jsonify({
                "error": "invalid_recipient",
                "detail": f"'to' must look like an email address; got {to_raw!r}",
            }), 422
        if cc_raw and not email_re.match(cc_raw):
            return jsonify({
                "error": "invalid_cc",
                "detail": f"'cc' must look like an email address; got {cc_raw!r}",
            }), 422

        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        if quote.status.value not in ("finalized", "sent"):
            return jsonify({
                "error": "send_precondition_failed",
                "detail": (
                    f"send-prep requires status in (finalized, sent); "
                    f"current status is {quote.status.value}. "
                    "Snapshot the quote first; the bytes shipped to the "
                    "buyer are the snapshot bytes."
                ),
            }), 409

        snap = latest_snapshot(db_path, quote_id)
        if snap is None:
            return jsonify({
                "error": "no_snapshot",
                "detail": (
                    "send-prep requires an approved snapshot. "
                    "POST /spine/quotes/<id>/snapshot first."
                ),
            }), 409
        if not _identity_matches(snap, quote):
            return jsonify({
                "error": "snapshot_stale",
                "detail": (
                    "Latest snapshot does not match current state. "
                    "Re-snapshot to approve the current state before "
                    "preparing the send."
                ),
            }), 409

        # Build the envelope. Subject and body are deterministic from
        # the quote state — same inputs → same envelope, so two preps
        # for the same quote produce the same email shape.
        total_str = f"${quote.total_cents/100:,.2f}"
        subject = (
            f"Reytech Quote — Solicitation {quote.solicitation_number} — "
            f"{total_str}"
        )
        line_count = len(quote.line_items)
        plural = "s" if line_count != 1 else ""
        snapshot_url_path = (
            f"/spine/quotes/{quote_id}/snapshot/{snap['snapshot_id']}/pdf"
        )
        body_text = (
            f"Hello,\n\n"
            f"Please find attached Reytech Inc.'s quote for "
            f"solicitation {quote.solicitation_number} "
            f"({quote.facility}).\n\n"
            f"Summary:\n"
            f"  Line items:  {line_count} item{plural}\n"
            f"  Subtotal:    ${quote.subtotal_cents/100:,.2f}\n"
            f"  Tax ({quote.tax_rate_bps/100:.2f}%):  "
            f"${quote.tax_cents/100:,.2f}\n"
            f"  Shipping:    $0.00\n"
            f"  Total:       {total_str}\n\n"
            f"Prices firm 30 days unless otherwise stated. "
            f"Reytech Inc. is a California Small Business / DVBE supplier.\n\n"
            f"Quote ID:    {quote.quote_id}\n"
            f"Snapshot ID: {snap['snapshot_id']}\n"
            f"SHA-256:     {snap['sha256']}\n\n"
            f"Please contact rfq@reytechinc.com or 949-229-1575 with "
            f"any questions.\n"
        )

        # Gmail compose URL — opens a fresh draft in a new tab with
        # the subject + body pre-filled. Operator attaches the PDF
        # (downloaded from snapshot_url_path) before sending.
        gmail_compose_url = (
            "https://mail.google.com/mail/?view=cm&fs=1"
            f"&to={urlencode_q(to_raw)}"
            + (f"&cc={urlencode_q(cc_raw)}" if cc_raw else "")
            + f"&su={urlencode_q(subject)}"
            + f"&body={urlencode_q(body_text)}"
        )

        pdf_filename = (
            f"Reytech_Quote_{quote.solicitation_number}_"
            f"{quote.quote_id}.pdf"
        )

        envelope = {
            "snapshot_id": snap["snapshot_id"],
            "snapshot_pdf_url": snapshot_url_path,
            "snapshot_pdf_filename": pdf_filename,
            "sha256": snap["sha256"],
            "to": [to_raw],
            "cc": [cc_raw] if cc_raw else [],
            "subject": subject,
            "body": body_text,
            "gmail_compose_url": gmail_compose_url,
        }

        # Record the prep in the event log so the audit chain shows
        # who prepared what envelope when, before the operator
        # actually clicks Send in Gmail. We write a NO-OP state save
        # (same Quote, same status) with the prep note. The substrate
        # has only one writer (write_quote); reusing it here keeps
        # the audit chain coherent without a second writer for events.
        actor = (
            request.headers.get("X-Spine-Actor")
            or _try_session_user()
            or "operator"
        )
        prep_note = (
            f"prepared send envelope to={to_raw}"
            + (f" cc={cc_raw}" if cc_raw else "")
            + f" snapshot={snap['snapshot_id']}"
            + f" sha256={snap['sha256'][:16]}"
        )
        try:
            write_quote(db_path, quote, actor=actor, note=prep_note)
        except Exception as e:
            # Audit write failure should not block returning the
            # envelope — operator can still send manually. Log and
            # carry on.
            log.warning(
                "send-prep audit write failed for %s: %s",
                quote_id, e,
            )

        return jsonify(envelope), 200

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
