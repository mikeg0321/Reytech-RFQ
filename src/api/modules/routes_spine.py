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
    find_links_from,
    find_low_confidence_contracts,
    init_db,
    iter_quote_ids,
    iter_snapshots,
    latest_rejections,
    latest_snapshot,
    read_event_log,
    read_quote,
    read_snapshot,
    render_quote_pdf,
    write_email_contract,
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

        # Buyer-facing Bill-to / Ship-to / RFQ-title come from the
        # EmailContract that drove ingest. A missing contract is the
        # legacy path — renderer falls back to quote.facility/agency
        # for both To and Ship-to so legacy quotes still render.
        try:
            contract = find_contract_for_quote(db_path, quote_id)
        except Exception:
            log.exception("spine.get_quote_pdf: contract lookup failed for %s", quote_id)
            contract = None

        try:
            pdf_bytes = render_quote_pdf(quote, contract=contract)
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

    # ─── CCHCS packet rendering — shared by every /forms/*/pdf route ──
    #
    # The CCHCS Non-Cloud RFQ Packet is ONE buyer-supplied PDF that
    # already bundles the 703B cover sheet, the 704B line-item table, and
    # the bid-package attachments (CUF / Darfur / AMS 708 / Civil Rights
    # / Seller's Permit). Reytech fills THAT document — it does not
    # generate three separate forms.
    #
    # The Spine's own from-scratch per-form renderers
    # (src/spine/agency_forms/cchcs_{703b,704b,bidpkg}) produced packets
    # that failed CCHCS responsiveness review (the 2026-05-18 "trash"
    # output). They are RETIRED at this route layer: every
    # /forms/{703b,704b,bidpkg,packet}/pdf endpoint now serves the output
    # of the legacy-filler adapter (src/spine/packet_render.py), which
    # fills the buyer's actual packet PDF — verified-correct since
    # 2026-04-13. See handoff-2026-05-20-legacy-adapter-build.

    def _serve_cchcs_packet(quote_id: str):
        """Render + stream the filled CCHCS Non-Cloud RFQ packet.

        Preview render — strict=False so a gate-flagged packet is still
        shown to the operator (the hard gate lives at snapshot/send).
        Gate state is surfaced in X-Spine-Packet-* response headers.
        """
        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            log.exception("spine.packet: load failed for %s", quote_id)
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        try:
            contract = find_contract_for_quote(db_path, quote_id)
        except Exception:
            log.exception("spine.packet: contract lookup failed for %s", quote_id)
            contract = None

        from src.spine.packet_render import render_cchcs_packet_via_legacy
        res = render_cchcs_packet_via_legacy(quote, contract, strict=False)

        if not res.get("pdf_bytes"):
            # No bytes at all — no contract bound, the buyer's packet PDF
            # could not be located, or the parse failed. Actionable 409.
            return jsonify({
                "error": "packet_render_failed",
                "detail": res.get("error") or "packet could not be rendered",
            }), 409

        gate = (res.get("fill_result") or {}).get("gate") or {}
        inline = request.args.get("inline", "1") != "0"
        # ?flatten=1 — bake form widgets into static page content for the
        # buyer-bound copy. Off by default so the preview render keeps
        # editable fields for the Inspector + Chrome walkthrough.
        flatten_requested = request.args.get("flatten", "0") == "1"
        pdf_bytes = res["pdf_bytes"]
        if flatten_requested:
            from src.spine.flatten import flatten_pdf_bytes
            pdf_bytes = flatten_pdf_bytes(pdf_bytes)
        disposition = (
            f'inline; filename="cchcs_packet_{quote_id}.pdf"'
            if inline else
            f'attachment; filename="cchcs_packet_{quote_id}.pdf"'
        )
        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={
                "Content-Disposition": disposition,
                "X-Spine-Packet-Gate-Passed": str(bool(gate.get("passed", False))),
                "X-Spine-Packet-Gate-Issues": str(
                    len(gate.get("critical_issues", []) or [])
                ),
                "X-Spine-Packet-Source": os.path.basename(res.get("source_pdf", "")),
                "X-Spine-Flattened": "1" if flatten_requested else "0",
            },
        )

    # ─── Format-aware standalone-form rendering ───────────────────────
    #
    # The CCHCS Non-Cloud Packet (above) is the MINORITY format — one
    # bundled buyer PDF. The COMMON format is the standalone set: AMS
    # 703B *or* 703C + AMS 704B + the CDCR Bid Package, as three separate
    # buyer template PDFs. Which format applies is declared by the
    # EmailContract's `response_packaging` (LAW 6) — never guessed:
    #   single_pdf            → the packet bundles every form; serve the
    #                           packet adapter (src/spine/packet_render.py).
    #   separate_pdfs / either → the standalone-form adapter
    #                           (src/spine/forms_render.py), which fills
    #                           the buyer's separate template PDFs.

    def _serve_cchcs_form(quote_id: str, which: str):
        """Render + stream one form of the CCHCS standalone set.

        `which` is one of "703" / "704b" / "bidpkg". Dispatches by the
        contract's declared packaging; a single_pdf quote has no separate
        templates, so its 703/704b/bidpkg all resolve to the bundled
        packet. Fails 409 (never a blank document) when the format is
        separate but the buyer's template PDFs can't be located.
        """
        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            log.exception("spine.forms: load failed for %s", quote_id)
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404

        try:
            contract = find_contract_for_quote(db_path, quote_id)
        except Exception:
            log.exception("spine.forms: contract lookup failed for %s", quote_id)
            contract = None

        packaging = (
            getattr(contract, "response_packaging", "separate_pdfs")
            if contract is not None else "separate_pdfs"
        )
        # single_pdf: the buyer's Non-Cloud Packet bundles all three
        # forms — there are no separate templates; serve the packet.
        if packaging == "single_pdf":
            return _serve_cchcs_packet(quote_id)

        from src.spine.forms_render import render_cchcs_forms_via_legacy

        # Preview render — surface a flagged form rather than hide it;
        # the hard gate lives at snapshot/send.
        res = render_cchcs_forms_via_legacy(quote, contract, strict=False)
        sub = (res.get("forms") or {}).get(which) or {}
        pdf_bytes = sub.get("pdf_bytes") or b""
        if not pdf_bytes:
            # No bytes — no contract bound, a template PDF could not be
            # located, or the filler crashed. Actionable 409.
            return jsonify({
                "error": "form_render_failed",
                "detail": (sub.get("error") or res.get("error")
                           or f"{which} could not be rendered"),
            }), 409

        inline = request.args.get("inline", "1") != "0"
        # ?flatten=1 — bake form widgets into static page content for the
        # buyer-bound copy. Off by default so the preview render keeps
        # editable fields for the Inspector + Chrome walkthrough.
        flatten_requested = request.args.get("flatten", "0") == "1"
        if flatten_requested:
            from src.spine.flatten import flatten_pdf_bytes
            pdf_bytes = flatten_pdf_bytes(pdf_bytes)
        fname = f"cchcs_{which}_{quote_id}.pdf"
        disposition = (
            f'inline; filename="{fname}"' if inline
            else f'attachment; filename="{fname}"'
        )
        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={
                "Content-Disposition": disposition,
                "X-Spine-Form-Code": str(sub.get("form_code", which)),
                "X-Spine-Form-Ok": str(bool(sub.get("ok", False))),
                "X-Spine-Form-Template": os.path.basename(
                    sub.get("template", "") or ""
                ),
                "X-Spine-Flattened": "1" if flatten_requested else "0",
            },
        )

    # ─── GET /spine/quotes/<quote_id>/forms/packet/pdf ────────────────
    #
    # Canonical route for the filled CCHCS Non-Cloud RFQ packet. Always
    # the packet adapter — packet is the bundled single-PDF format.

    @spine_bp.route(
        "/spine/quotes/<quote_id>/forms/packet/pdf",
        methods=["GET"],
    )
    @_wrap
    def get_packet_pdf(quote_id: str):
        return _serve_cchcs_packet(quote_id)

    # ─── GET /spine/quotes/<quote_id>/forms/703b/pdf ──────────────────
    #
    # The 703 cover sheet. Format-aware: single_pdf → the packet (the
    # 703B is page 1 of it); separate_pdfs → the standalone 703B/703C
    # filled by forms_render (the contract's required_forms picks the
    # variant).

    @spine_bp.route(
        "/spine/quotes/<quote_id>/forms/703b/pdf",
        methods=["GET"],
    )
    @_wrap
    def get_703b_pdf(quote_id: str):
        return _serve_cchcs_form(quote_id, "703")

    # ─── GET /spine/quotes/<quote_id>/forms/704b/pdf ──────────────────
    #
    # The 704B quote worksheet. Format-aware: single_pdf → the packet;
    # separate_pdfs → the standalone 704B filled by forms_render.

    @spine_bp.route(
        "/spine/quotes/<quote_id>/forms/704b/pdf",
        methods=["GET"],
    )
    @_wrap
    def get_704b_pdf(quote_id: str):
        return _serve_cchcs_form(quote_id, "704b")

    # ─── GET /spine/quotes/<quote_id>/forms/bidpkg/pdf ────────────────
    #
    # The CDCR Bid Package. Format-aware: single_pdf → the packet (the
    # bid-package attachments are spliced inside it); separate_pdfs →
    # the standalone Bid Package filled by forms_render.

    @spine_bp.route(
        "/spine/quotes/<quote_id>/forms/bidpkg/pdf",
        methods=["GET"],
    )
    @_wrap
    def get_bidpkg_pdf(quote_id: str):
        return _serve_cchcs_form(quote_id, "bidpkg")

    # ─── GET /spine/quotes/<quote_id>/inspector ───────────────────────
    #
    # The Inspector gate's JSON report — math + identity + coverage +
    # cost-basis reconcile against the SAME bytes the buyer will see.
    # `ok=True` is the send-gate value (the future /send-prep gating
    # in PR-6 calls this and refuses on non-clean). Operator UI can
    # poll this for the pre-send checklist.
    #
    # The report is RECONSTRUCTED per request — there is no
    # stored InspectorReport; the source of truth is the Spine quote
    # state + the contract. Two consecutive calls with the same state
    # produce identical reports (the Inspector is deterministic).

    @spine_bp.route(
        "/spine/quotes/<quote_id>/inspector",
        methods=["GET"],
    )
    @_wrap
    def get_inspector_report(quote_id: str):
        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            log.exception("spine.inspector: load failed for %s", quote_id)
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404
        try:
            contract = find_contract_for_quote(db_path, quote_id)
        except Exception:
            log.exception("spine.inspector: contract lookup failed for %s", quote_id)
            contract = None
        from src.spine.inspector import reconcile_quote_to_package

        report = reconcile_quote_to_package(quote, contract)
        return jsonify(report.model_dump()), 200

    # ─── GET /spine/quotes/<quote_id>/visual-qa ───────────────────────
    #
    # The visual-fidelity gate (task #20, shipped as PR-12). Peer to
    # /inspector — same shape (ok + issues), different concern. Inspector
    # verifies VALUES (math/identity/coverage); Visual-QA verifies the
    # rendered page LOOKS right (no comb-spacing, no clipping, no
    # (cid:N) glyph artifacts from stale appearance streams).
    #
    # Built after the 2026-05-23 Demidenko PC near-miss: the math
    # Inspector passed clean on flat output that visually shipped
    # `30 Carnoustie Way Trabuco Ca` (clipped) and
    # `s a l e s @ r e y t e c h i n c .` (comb-spaced). The math
    # was right; the bytes were wrong. This gate catches that class.
    #
    # Severity in v1: every finding is `warning`. The send-prep route
    # surfaces these alongside the math Inspector but does NOT yet
    # block on a warning. Flip-to-blocking is a separate PR after Mike's
    # first week of operator feedback.

    @spine_bp.route(
        "/spine/quotes/<quote_id>/visual-qa",
        methods=["GET"],
    )
    @_wrap
    def get_visual_qa_report(quote_id: str):
        try:
            quote = read_quote(db_path, quote_id)
        except Exception as e:
            log.exception("spine.visual-qa: load failed for %s", quote_id)
            return jsonify({"error": "load_failed", "detail": str(e)}), 500
        if quote is None:
            return jsonify({"error": "not_found", "quote_id": quote_id}), 404
        try:
            contract = find_contract_for_quote(db_path, quote_id)
        except Exception:
            log.exception("spine.visual-qa: contract lookup failed for %s", quote_id)
            contract = None

        # Build the bytes the buyer would see, FLAT (via the PR-10
        # appearance-regen path). Format-aware so single_pdf gets the
        # packet and separate_pdfs gets each of 703B/704B/bidpkg.
        from src.spine.flatten import flatten_pdf_bytes
        from src.spine.visual_qa import VisualQAReport, inspect_pdf_visual

        artifacts: list[dict[str, Any]] = []  # {form_code, flat_pdf_bytes}
        packaging = (
            contract.response_packaging if contract else "single_pdf"
        )
        try:
            if packaging == "single_pdf":
                from src.spine.packet_render import render_cchcs_packet_via_legacy
                import tempfile

                with tempfile.TemporaryDirectory() as td:
                    res = render_cchcs_packet_via_legacy(
                        quote, contract, output_dir=td, strict=False)
                if res.get("ok") and res.get("pdf_bytes"):
                    artifacts.append({
                        "form_code": "packet",
                        "flat_pdf_bytes": flatten_pdf_bytes(res["pdf_bytes"]),
                    })
            else:
                from src.spine.forms_render import render_cchcs_forms_via_legacy
                import tempfile

                with tempfile.TemporaryDirectory() as td:
                    res = render_cchcs_forms_via_legacy(
                        quote, contract, output_dir=td, strict=False)
                if res.get("ok"):
                    # forms_render returns key "703" (the form letter is
                    # decided at render time — 703B vs 703C); the
                    # consumer-facing form_code in this report is "703b"
                    # to match the send-prep envelope's vocabulary.
                    _key_to_label = {"703": "703b", "704b": "704b",
                                     "bidpkg": "bidpkg"}
                    for key, label in _key_to_label.items():
                        sub = (res.get("forms") or {}).get(key) or {}
                        if sub.get("pdf_bytes"):
                            artifacts.append({
                                "form_code": label,
                                "flat_pdf_bytes": flatten_pdf_bytes(sub["pdf_bytes"]),
                            })
        except Exception as e:
            log.exception("spine.visual-qa: render failed for %s", quote_id)
            return jsonify({
                "error": "render_failed", "detail": str(e),
                "quote_id": quote_id,
            }), 500

        # Run the Tier-1 detectors per artifact, aggregate.
        per_form: list[dict[str, Any]] = []
        all_blocking = 0
        all_warnings = 0
        for art in artifacts:
            r = inspect_pdf_visual(art["flat_pdf_bytes"])
            per_form.append({
                "form_code": art["form_code"],
                "report": r.model_dump(),
            })
            all_blocking += r.blocking_count
            all_warnings += r.warning_count

        return jsonify({
            "quote_id": quote_id,
            "response_packaging": packaging,
            "ok": all_blocking == 0,
            "total_blocking": all_blocking,
            "total_warnings": all_warnings,
            "per_form": per_form,
        }), 200

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

    # ─── POST /spine/quotes/<quote_id>/contract-override ──────────────
    #
    # Operator path for filling in EmailContract fields the inbound
    # parser missed (5/18 Mike: "shouldn't I be allowed to fill out
    # if not caught in the email contract so it shows on documents?").
    #
    # The contract substrate is append-only by Charter rule — corrections
    # are never written in-place. Instead, this endpoint:
    #   1. Loads the latest contract for `quote_id` (via find_contract_
    #      for_quote → newest by ingested_at).
    #   2. model_copy()s with the operator-supplied field overrides.
    #   3. Writes a NEW contract row (same rfq_id, new contract_id with
    #      `_op<ts>` suffix). Now find_contract_for_quote returns this
    #      corrected version, and every downstream renderer (Quote PDF,
    #      703B, 704B, bidpkg) picks up the operator's fills on the
    #      next request.
    #
    # Body: JSON dict whose keys are any subset of EmailContract fields
    # the operator wants to set/overwrite. Common fields parsers miss:
    #   buyer_name, buyer_phone, buyer_title, ship_to_address,
    #   ship_to_facility, release_date, due_date.
    # `actor` is required (recorded in the new contract_id for audit).
    #
    # Returns 200 with {new_contract_id, prior_contract_id, fields_set:
    # [...]} on success. 404 when no prior contract exists for the
    # quote. 409 when the merged contract fails Pydantic validation.

    @spine_bp.route(
        "/spine/quotes/<quote_id>/contract-override",
        methods=["POST"],
    )
    @_wrap
    def post_contract_override(quote_id: str):
        from datetime import datetime, timezone
        from src.spine.email_contract import EmailContract

        body = request.get_json(silent=True) or {}
        actor = (body.pop("actor", None) or "").strip()
        if not actor:
            return jsonify({
                "error": "bad_request",
                "detail": "actor (non-empty string) is required in body",
            }), 400

        # The contract substrate is the source of truth for these field
        # names; we use the model's fields to validate the override
        # keys and reject typos at the boundary.
        allowed_fields = set(EmailContract.model_fields.keys()) - {
            # Provenance / linkage fields the operator should NEVER
            # override directly — they're managed by the substrate.
            "contract_id", "rfq_id", "pc_id",
            "source_email_id", "source_thread_id",
            "ingested_at", "ingest_parser_version",
        }
        bad_keys = [k for k in body if k not in allowed_fields]
        if bad_keys:
            return jsonify({
                "error": "bad_request",
                "detail": (
                    f"unknown or non-overridable field(s): {bad_keys}. "
                    f"allowed: {sorted(allowed_fields)}"
                ),
            }), 400

        prior = find_contract_for_quote(db_path, quote_id)
        if prior is None:
            return jsonify({
                "error": "no_contract",
                "detail": (
                    f"no EmailContract bound to quote_id={quote_id!r}. "
                    "Contract-override requires a prior contract to "
                    "build the correction from (so source_email_id + "
                    "required_forms etc. carry forward)."
                ),
            }), 404

        # Merge: operator overrides win, prior contract fills the rest.
        try:
            corrected = prior.model_copy(update=body)
        except Exception as e:
            return jsonify({
                "error": "validation_failed",
                "detail": (
                    f"merged contract failed validation: {e}. "
                    "Check field types (dates as ISO strings, ints as "
                    "ints, lists as lists)."
                ),
            }), 409

        # New contract_id ties the correction to the prior + actor +
        # timestamp so the audit chain is human-readable. Same rfq_id
        # so find_contract_for_quote picks this up as the latest.
        # Microsecond resolution so two rapid corrections from the same
        # actor in the same second don't collide on contract_id (the
        # spine_email_contracts unique constraint would reject the
        # second write).
        now = datetime.now(timezone.utc)
        ts = f"{int(now.timestamp())}{now.microsecond:06d}"
        actor_slug = "".join(c for c in actor if c.isalnum() or c in "_-")[:24]
        # Append-only naming: prior contract_id stays untouched; the
        # new id is derived from it so downstream forensic tooling can
        # follow the chain.
        prior_id = prior.contract_id
        # Trim potential prior `_op...` suffix to keep the chain
        # shallow — `_op` substrings nest otherwise on repeat overrides.
        # The full prior chain is preserved in the contract_id chain
        # readable on /events.
        base_id = prior_id.split("_op")[0]
        new_id = f"{base_id}_op{ts}_{actor_slug}"[:80]

        corrected = corrected.model_copy(update={"contract_id": new_id})

        try:
            meta = write_email_contract(db_path, corrected)
        except SpineValidationError as e:
            return jsonify({"error": "write_failed", "detail": str(e)}), 409
        except Exception as e:
            log.exception("spine.contract_override: write failed for %s", quote_id)
            return jsonify({"error": "write_failed", "detail": str(e)}), 500

        return jsonify({
            "ok": True,
            "quote_id": quote_id,
            "new_contract_id": meta["contract_id"],
            "prior_contract_id": prior_id,
            "fields_set": sorted(body.keys()),
            "actor": actor,
            "sha256": meta["sha256"],
            "ingested_at": meta["ingested_at"],
        }), 200

    # ─── GET /spine/queue/rejected ────────────────────────────────────
    #
    # Triage surface for the missed-bid silent-drop class. Every email
    # the ingest pipeline considered and refused emits a row to
    # spine_ingest_rejections; this route surfaces them newest-first
    # with optional reason_code filter. The Telegram missed-bid watcher
    # (queued) consumes this same read path.

    # ─── GET /spine/queue/low-confidence ──────────────────────────────
    #
    # Pillar-1 / G3 triage surface (chrome MCP audit 2026-05-26): emails
    # the parser accepted but flagged with parse_confidence='low' or
    # 'medium' need operator review before they silently ship a bad
    # quote. Companion to /queue/rejected (which surfaces refused
    # emails). Together they form the full ingest-triage view.
    #
    # Read-only — no mutations. The operator's action is to open the
    # individual /spine/quotes/<rfq_id> page and review/fix/ship.

    @spine_bp.route("/spine/queue/low-confidence", methods=["GET"])
    @_wrap
    def get_queue_low_confidence():
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

        # Optional confidence filter — default surfaces both low + medium.
        # `?level=low` narrows to just low-confidence; same for medium.
        level = (request.args.get("level") or "").strip().lower()
        if level in ("low", "medium"):
            confidence_levels = (level,)
        else:
            confidence_levels = ("low", "medium")

        try:
            rows = find_low_confidence_contracts(
                db_path, limit=limit,
                confidence_levels=confidence_levels,
            )
        except SpineValidationError as e:
            return jsonify({"error": "bad_request", "detail": str(e)}), 400
        except Exception as e:
            log.exception("spine.get_queue_low_confidence: read failed")
            return jsonify({"error": "read_failed", "detail": str(e)}), 500

        return jsonify({
            "count": len(rows),
            "limit": limit,
            "confidence_levels": list(confidence_levels),
            "contracts": rows,
        })

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

        # ── Inspector gate ────────────────────────────────────────
        # Job #1 §0 acceptance: every send through the 3-quote gate
        # must carry a clean InspectorReport. Run it here before the
        # operator gets the Gmail compose URL — a non-clean report
        # blocks 409 with the full report attached so the UI can
        # show the operator exactly what to fix.
        #
        # The gate runs only when an EmailContract is bound. Legacy
        # quotes that predate the contract substrate keep working
        # (envelope is marked `inspector_skipped` so the operator knows
        # the math-reconcile wasn't run); CCHCS quotes in Job #1 always
        # ingest through the contract path (LAW 6) and so always gate.
        try:
            contract_for_inspector = find_contract_for_quote(db_path, quote_id)
        except Exception:
            contract_for_inspector = None
        inspector_report = None
        inspector_skipped_reason: str | None = None
        if contract_for_inspector is None:
            inspector_skipped_reason = (
                "no EmailContract bound — Inspector math-reconcile skipped"
            )
        else:
            from src.spine.inspector import reconcile_quote_to_package

            inspector_report = reconcile_quote_to_package(
                quote, contract_for_inspector)
            if not inspector_report.ok:
                return jsonify({
                    "error": "inspector_blocked",
                    "detail": (
                        f"Inspector report has {inspector_report.blocking_count} "
                        f"blocking issue(s); resolve before send."
                    ),
                    "report": inspector_report.model_dump(),
                }), 409

        # ── Attachment-disposition gate (LAW 6 "Teeth") ───────────────
        # Every attachment in attachment_refs must have a recorded
        # AttachmentDisposition, AND every parsed disposition whose
        # cross_references list is non-empty must have
        # cross_refs_resolved=True.  Either failure → 409 with
        # diagnostic detail so the operator knows exactly which
        # attachment was not accounted for.
        #
        # The gate runs only when an EmailContract is bound (same
        # condition as the Inspector gate above).
        if contract_for_inspector is not None:
            _contract = contract_for_inspector
            _refs = list(_contract.attachment_refs)
            _disps = list(_contract.attachment_dispositions)
            _disposed_refs = {d.ref for d in _disps}

            # (a) Any attachment_ref with no matching disposition?
            _unaccounted = [r for r in _refs if r not in _disposed_refs]
            if _unaccounted:
                return jsonify({
                    "error": "disposition_missing",
                    "detail": (
                        f"{len(_unaccounted)} attachment(s) in the contract "
                        "have no recorded disposition. Ingest must classify "
                        "every attachment as 'parsed' or 'classified_non_rfq' "
                        "before send-prep is allowed."
                    ),
                    "unaccounted_refs": _unaccounted,
                }), 409

            # (b) Any parsed disposition with an unresolved cross-reference?
            _unresolved = [
                d.ref for d in _disps
                if d.status == "parsed"
                and d.cross_references
                and not d.cross_refs_resolved
            ]
            if _unresolved:
                return jsonify({
                    "error": "cross_reference_unresolved",
                    "detail": (
                        f"{len(_unresolved)} parsed attachment(s) carry "
                        "cross-references to targets that were never parsed. "
                        "Ingest must locate and parse all cross-referenced "
                        "attachments before send-prep is allowed."
                    ),
                    "unresolved_refs": _unresolved,
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

        # ── Form attachments — every required_form beyond the quote ───
        # The Reytech Quote PDF is already covered by snapshot_pdf_url.
        # Format-B (separate_pdfs) needs the operator to also attach the
        # 703B + 704B + Bid Package; Format-A (single_pdf) needs only the
        # bundled packet. Each URL is flatten-on by default so the bytes
        # the buyer receives are non-editable.
        required_forms = list(
            getattr(contract_for_inspector, "required_forms", None) or []
        )
        packaging = getattr(
            contract_for_inspector, "response_packaging", "separate_pdfs")
        form_attachments: list[dict] = []
        if packaging == "single_pdf":
            # The packet bundles every form. Single attachment.
            if any(f != "quote" for f in required_forms):
                form_attachments.append({
                    "form_code": "packet",
                    "url": f"/spine/quotes/{quote_id}/forms/packet/pdf?flatten=1",
                    "filename": (
                        f"Reytech_Packet_{quote.solicitation_number}_"
                        f"{quote.quote_id}.pdf"
                    ),
                })
        else:
            # Separate-PDFs — one attachment per non-quote required form.
            _route_map = {
                "703b": "703b", "703c": "703b",   # both 703 variants → 703b route
                "704b": "704b", "bidpkg": "bidpkg",
            }
            seen_routes: set[str] = set()
            for f in required_forms:
                if f == "quote":
                    continue
                route_form = _route_map.get(f)
                if route_form is None or route_form in seen_routes:
                    continue
                seen_routes.add(route_form)
                form_attachments.append({
                    "form_code": f,
                    "url": f"/spine/quotes/{quote_id}/forms/{route_form}/pdf?flatten=1",
                    "filename": (
                        f"Reytech_{f.upper()}_{quote.solicitation_number}_"
                        f"{quote.quote_id}.pdf"
                    ),
                })

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
            # PR-6 additions — Inspector verdict + per-form attachments.
            "inspector_ok": (inspector_report.ok
                             if inspector_report is not None else None),
            "inspector_blocking_count": (
                inspector_report.blocking_count
                if inspector_report is not None else 0),
            "inspector_warning_count": (
                inspector_report.warning_count
                if inspector_report is not None else 0),
            "inspector_skipped": inspector_skipped_reason,
            "form_attachments": form_attachments,
            "response_packaging": packaging,
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

    # ─── POST /spine/quotes/<quote_id>/carry-forward-prices ──────────
    #
    # Operator-triggered auto-pricer. Takes validated costs from the
    # linked PC (highest-confidence quote-link) and carries them onto
    # the current quote's unpriced lines, matched on normalized MFG#.
    #
    # Same pure function the shadow-ingest path uses (auto_pricer.
    # carry_forward_costs); this just exposes it as an operator action
    # so the linked-but-not-yet-priced state can be resolved in one
    # click instead of requiring re-ingest.
    #
    # Why this exists (chrome MCP audit 2026-05-26): the next-window
    # priority queue named this as the highest-leverage step to shorten
    # per-ship time for the 3 overdue Job #1 RFQs. Each ship saves the
    # operator from re-typing costs that already live on the prior PC.

    @spine_bp.route(
        "/spine/quotes/<quote_id>/carry-forward-prices",
        methods=["POST"],
    )
    @_wrap
    def carry_forward_prices(quote_id: str):
        from src.spine.auto_pricer import carry_forward_costs

        # Load the target quote (RFQ being edited).
        try:
            target = read_quote(db_path, quote_id)
        except Exception as e:
            return jsonify({"ok": False, "error": "load_failed",
                            "detail": str(e)}), 500
        if target is None:
            return jsonify({"ok": False, "error": "not_found",
                            "quote_id": quote_id}), 404

        # Resolve the source PC. Body may pass an explicit override
        # (`from_pc_id`); otherwise take the highest-confidence link.
        body = request.get_json(silent=True) or {}
        from_pc_id = (body.get("from_pc_id") or "").strip()

        if not from_pc_id:
            try:
                links = find_links_from(db_path, quote_id)
            except Exception as e:
                return jsonify({"ok": False, "error": "link_lookup_failed",
                                "detail": str(e)}), 500
            if not links:
                return jsonify({
                    "ok": False,
                    "error": "no_linked_pc",
                    "quote_id": quote_id,
                    "hint": "No prior PC is linked to this quote — "
                            "carry-forward needs a source.",
                }), 409
            from_pc_id = links[0]["to_quote_id"]

        if from_pc_id == quote_id:
            return jsonify({"ok": False, "error": "self_link",
                            "quote_id": quote_id}), 400

        try:
            source = read_quote(db_path, from_pc_id)
        except Exception as e:
            return jsonify({"ok": False, "error": "source_load_failed",
                            "from_pc_id": from_pc_id,
                            "detail": str(e)}), 500
        if source is None:
            return jsonify({"ok": False, "error": "source_not_found",
                            "from_pc_id": from_pc_id}), 404

        # Compute the carry. carry_forward_costs is pure — same inputs
        # always produce the same (quote, summary). It only carries on
        # exact MFG# match AND when target line cost_cents == 0.
        try:
            new_target, summary = carry_forward_costs(target, source)
        except Exception as e:
            return jsonify({"ok": False, "error": "carry_failed",
                            "detail": str(e)}), 500

        # If nothing was carried, no point writing — return the
        # summary so the UI can show "0 lines carried" without
        # incrementing the event log noise.
        if not summary.get("carried"):
            return jsonify({
                "ok": True,
                "quote_id": quote_id,
                "source_quote_id": from_pc_id,
                "wrote": False,
                "summary": summary,
            })

        # Persist via the single canonical writer (write_quote enforces
        # one-writer invariant per §0 LAW 1 + test_one_writer.py).
        try:
            persisted = write_quote(
                db_path,
                new_target,
                actor="operator:auto_pricer_button",
                note=(
                    f"carry_forward_costs from {from_pc_id}: "
                    f"carried={len(summary.get('carried', []))} "
                    f"skipped_already_priced="
                    f"{len(summary.get('skipped_already_priced', []))} "
                    f"skipped_no_match="
                    f"{len(summary.get('skipped_no_match', []))}"
                ),
            )
        except Exception as e:
            return jsonify({"ok": False, "error": "write_failed",
                            "detail": str(e)}), 500

        return jsonify({
            "ok": True,
            "quote_id": quote_id,
            "source_quote_id": from_pc_id,
            "wrote": True,
            "updated_at": persisted.updated_at.isoformat()
                          if hasattr(persisted.updated_at, "isoformat")
                          else str(persisted.updated_at),
            "summary": summary,
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
