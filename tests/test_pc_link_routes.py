"""Route-layer tests for the CCHCS PC→RFQ handoff.

These routes are the operator-facing entrypoint into the 4-helper chain that
lives in `src/core/pc_rfq_linker.py` (matcher, promote-verbatim,
qty-change-summary, selective-reprice). Helper logic is covered by unit and
E2E tests elsewhere; this file locks the HTTP contract:

  - GET  /api/rfq/<rid>/pc-link-suggestions
  - POST /api/rfq/<rid>/confirm-pc-link  (body: {pc_id, reprice: bool})

Mike's 2026-04-20 rule stays enforced at this layer:
  1. Suggestions never auto-link — operator always confirms.
  2. Confirm ports PC prices verbatim (the publish-for-bidding commitment).
  3. Reprice is OPT-IN and only touches qty-changed lines.
"""
from __future__ import annotations

import json
import os

import pytest


def _write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f)


def _cchcs_rfq(rid="rfq-pc-link-1", lines=None):
    return {
        "id": rid,
        "solicitation_number": "PC-2026-042-RFQ",
        "requestor_email": "buyer@cchcs.ca.gov",
        "institution": "CCHCS",
        "agency": "CCHCS",
        "status": "new",
        "line_items": lines if lines is not None else [
            {"mfg_number": "W12919", "description": "BP cuff", "quantity": 10},
            {"mfg_number": "FN4368", "description": "Gloves", "quantity": 100},
        ],
    }


def _cchcs_pc(pcid="pc-042", pc_number="PC-2026-042"):
    return {
        "id": pcid,
        "pc_number": pc_number,
        "agency": "CCHCS",
        "institution": "California Correctional Health Care Services",
        "requestor": "buyer@cchcs.ca.gov",
        "items": [
            {"mfg_number": "W12919", "description": "BP cuff adult",
             "quantity": 10, "unit_price": 45.00, "supplier_cost": 25.00,
             "bid_price": 45.00, "markup_pct": 80},
            {"mfg_number": "FN4368", "description": "Gloves nitrile",
             "quantity": 50, "unit_price": 18.50, "supplier_cost": 10.00,
             "bid_price": 18.50, "markup_pct": 85},
        ],
    }


def _seed(temp_data_dir, rfqs, pcs):
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), rfqs)
    _write_json(os.path.join(temp_data_dir, "price_checks.json"), pcs)


# ── GET /api/rfq/<rid>/pc-link-suggestions ────────────────────────────────

def test_suggestions_404_for_unknown_rfq(auth_client):
    resp = auth_client.get("/api/rfq/does-not-exist/pc-link-suggestions")
    assert resp.status_code == 404
    body = resp.get_json()
    assert body["ok"] is False


def test_suggestions_rejects_path_traversal(auth_client):
    resp = auth_client.get("/api/rfq/..%2Fetc/pc-link-suggestions")
    # _validate_rid either rejects 400 or framework returns 404 — both are safe.
    # Contract: must NOT return 200 with data.
    assert resp.status_code in (400, 404)


def test_suggestions_returns_candidates_for_matching_cchcs_pc(
    auth_client, temp_data_dir
):
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _seed(temp_data_dir, {rfq["id"]: rfq}, {pc["id"]: pc})

    resp = auth_client.get(f"/api/rfq/{rfq['id']}/pc-link-suggestions")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["rfq_id"] == rfq["id"]
    assert body["already_linked"] is False
    assert isinstance(body["suggestions"], list)
    assert len(body["suggestions"]) >= 1

    top = body["suggestions"][0]
    assert top["pc_id"] == pc["id"]
    assert top["pc_number"] == "PC-2026-042"
    assert top["line_matches"] == 2
    assert top["line_total"] == 2
    assert top["is_exact"] is True
    assert top["match_pct"] >= 90
    assert isinstance(top["reasons"], list)
    assert top["pc_item_count"] == 2


def test_suggestions_empty_when_no_cchcs_pcs(auth_client, temp_data_dir):
    rfq = _cchcs_rfq()
    _seed(temp_data_dir, {rfq["id"]: rfq}, {})

    resp = auth_client.get(f"/api/rfq/{rfq['id']}/pc-link-suggestions")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["suggestions"] == []


def test_suggestions_reports_already_linked(auth_client, temp_data_dir):
    rfq = _cchcs_rfq()
    rfq["linked_pc_id"] = "pc-042"
    pc = _cchcs_pc()
    _seed(temp_data_dir, {rfq["id"]: rfq}, {pc["id"]: pc})

    resp = auth_client.get(f"/api/rfq/{rfq['id']}/pc-link-suggestions")
    body = resp.get_json()
    assert body["already_linked"] is True
    assert body["linked_pc_id"] == "pc-042"


# ── POST /api/rfq/<rid>/confirm-pc-link ───────────────────────────────────

def test_confirm_404_for_unknown_rfq(auth_client):
    resp = auth_client.post(
        "/api/rfq/nope/confirm-pc-link",
        json={"pc_id": "whatever"},
    )
    assert resp.status_code == 404


def test_confirm_requires_pc_id(auth_client, temp_data_dir):
    rfq = _cchcs_rfq()
    _seed(temp_data_dir, {rfq["id"]: rfq}, {})
    resp = auth_client.post(f"/api/rfq/{rfq['id']}/confirm-pc-link", json={})
    assert resp.status_code == 400
    assert resp.get_json()["ok"] is False


def test_confirm_404_for_unknown_pc(auth_client, temp_data_dir):
    rfq = _cchcs_rfq()
    _seed(temp_data_dir, {rfq["id"]: rfq}, {})
    resp = auth_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": "missing-pc"},
    )
    assert resp.status_code == 404


def test_confirm_ports_pc_prices_verbatim_and_flags_qty_change(
    auth_client, temp_data_dir
):
    """The core contract: PC prices port verbatim; FN4368 qty jumped 50→100
    so it gets flagged for re-price; W12919 qty matched so its commitment
    price survives untouched."""
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _seed(temp_data_dir, {rfq["id"]: rfq}, {pc["id"]: pc})

    resp = auth_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": pc["id"], "reprice": False},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["pc_id"] == pc["id"]
    assert body["promote"]["promoted"] == 2
    assert body["promote"]["qty_changed"] == 1
    assert body["promote"]["no_match"] == 0
    # Opt-out of reprice → no reprice block
    assert body["reprice"] is None

    summary = body["qty_change_summary"]
    assert len(summary) == 2
    # W12919 matched qty
    match_unchanged = [s for s in summary if s["pc_qty"] == 10][0]
    assert match_unchanged["qty_changed"] is False
    assert match_unchanged["current_unit_price"] == 45.00
    # FN4368 qty bumped
    match_changed = [s for s in summary if s["pc_qty"] == 50][0]
    assert match_changed["qty_changed"] is True
    assert match_changed["rfq_qty"] == 100
    # Commitment price still on the flagged line (reprice not requested)
    assert match_changed["current_unit_price"] == 18.50


def test_confirm_persists_rfq_to_disk(auth_client, temp_data_dir):
    """After confirm, the RFQ on disk must carry the PC link + verbatim prices
    so a page reload reflects the operator's decision."""
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _seed(temp_data_dir, {rfq["id"]: rfq}, {pc["id"]: pc})

    resp = auth_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": pc["id"]},
    )
    assert resp.status_code == 200

    # Reload from the route layer — get fresh suggestions and confirm linked
    resp2 = auth_client.get(f"/api/rfq/{rfq['id']}/pc-link-suggestions")
    body2 = resp2.get_json()
    assert body2["already_linked"] is True
    assert body2["linked_pc_id"] == pc["id"]


def test_confirm_with_reprice_flag_runs_selective_reprice(
    auth_client, temp_data_dir
):
    """reprice=True invokes the selective reprice helper. Without a wired
    pricer (pricer=None at route layer, deferred), qty-changed lines come
    back as `skipped_no_price` — commitment prices on unchanged lines must
    still be untouched."""
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _seed(temp_data_dir, {rfq["id"]: rfq}, {pc["id"]: pc})

    resp = auth_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": pc["id"], "reprice": True},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["reprice"] is not None
    # 1 qty-changed line, pricer=None → skipped_no_price
    assert body["reprice"]["skipped_no_price"] == 1
    assert body["reprice"]["repriced"] == 0
    # 1 matched-qty line counted as no_change
    assert body["reprice"]["skipped_no_change"] == 1

    # Commitment line still has its PC price — reprice didn't touch it
    unchanged = [s for s in body["qty_change_summary"] if s["pc_qty"] == 10][0]
    assert unchanged["current_unit_price"] == 45.00


def test_suggestions_requires_auth(anon_client, temp_data_dir):
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _seed(temp_data_dir, {rfq["id"]: rfq}, {pc["id"]: pc})
    resp = anon_client.get(f"/api/rfq/{rfq['id']}/pc-link-suggestions")
    # @auth_required must gate this
    assert resp.status_code in (401, 403)


def test_confirm_requires_auth(anon_client, temp_data_dir):
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _seed(temp_data_dir, {rfq["id"]: rfq}, {pc["id"]: pc})
    resp = anon_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": pc["id"]},
    )
    assert resp.status_code in (401, 403)
