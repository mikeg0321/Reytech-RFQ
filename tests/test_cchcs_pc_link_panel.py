"""Template + route regression guards for the CCHCS PC→RFQ link panel.

Lock three contracts that the operator-facing UI depends on:

  1. The panel markup is present on the RFQ detail page for a CCHCS RFQ that
     has no PC linked yet. `data-testid="cchcs-pc-link-panel"` and the
     panel-populating fetch to `/api/rfq/<rid>/pc-link-suggestions` are the
     hooks this test pins.

  2. The panel is NOT rendered for non-CCHCS RFQs (CalVet, CDCR, DSH, etc.)
     even when no PC is linked — PC workflow is CCHCS-only today.

  3. The panel is NOT rendered when a PC is already linked — operator already
     confirmed the link, nothing to surface.

And two route contracts:

  4. After operator-confirmed promote, the route populates `r.pc_diff` so the
     existing Linked banner at rfq_detail.html:297 renders "N prices ported /
     ΔN qty changes" truthfully.

  5. The per-line `qty_changed` flag persists on the saved RFQ so downstream
     fillers / UI markers can read it.
"""
from __future__ import annotations

import json
import os


def _write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f)


def _cchcs_rfq(rid="rfq-panel-test"):
    return {
        "id": rid,
        "solicitation_number": "PC-2026-999-RFQ",
        "requestor_email": "buyer@cchcs.ca.gov",
        "institution": "CCHCS",
        "agency": "CCHCS",
        "status": "new",
        "line_items": [
            {"mfg_number": "W12919", "description": "BP cuff", "quantity": 10},
            {"mfg_number": "FN4368", "description": "Gloves", "quantity": 100},
        ],
    }


def _cchcs_pc(pcid="pc-999"):
    return {
        "id": pcid,
        "pc_number": "PC-2026-999",
        "agency": "CCHCS",
        "institution": "California Correctional Health Care Services",
        "requestor": "buyer@cchcs.ca.gov",
        "items": [
            {"mfg_number": "W12919", "description": "BP cuff adult",
             "quantity": 10, "unit_price": 45.00, "supplier_cost": 25.00,
             "bid_price": 45.00},
            {"mfg_number": "FN4368", "description": "Gloves nitrile",
             "quantity": 50, "unit_price": 18.50, "supplier_cost": 10.00,
             "bid_price": 18.50},
        ],
    }


# ── Template-layer contract ───────────────────────────────────────────────

def test_panel_renders_for_unlinked_cchcs_rfq(auth_client, temp_data_dir):
    rfq = _cchcs_rfq()
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), {rfq["id"]: rfq})

    resp = auth_client.get(f"/rfq/{rfq['id']}")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'data-testid="cchcs-pc-link-panel"' in html, (
        "CCHCS PC link panel missing from unlinked CCHCS RFQ detail page"
    )
    # JS must actually call the new endpoint — pin the URL shape.
    assert "/pc-link-suggestions" in html


def test_panel_hidden_when_pc_already_linked(auth_client, temp_data_dir):
    rfq = _cchcs_rfq()
    rfq["linked_pc_id"] = "already-linked-pc"
    rfq["linked_pc_number"] = "PC-2026-888"
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), {rfq["id"]: rfq})

    resp = auth_client.get(f"/rfq/{rfq['id']}")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'data-testid="cchcs-pc-link-panel"' not in html, (
        "CCHCS panel must not render on an already-linked RFQ — "
        "operator already confirmed."
    )


def test_panel_hidden_for_non_cchcs_rfq(auth_client, temp_data_dir):
    """Non-CCHCS agencies don't use the PC workflow. Even unlinked,
    the panel must not appear."""
    rfq = _cchcs_rfq(rid="rfq-calvet-test")
    rfq["agency"] = "CalVet"
    rfq["institution"] = "California Department of Veterans Affairs"
    rfq["department"] = "CalVet"
    rfq["requestor_email"] = "buyer@calvet.ca.gov"
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), {rfq["id"]: rfq})

    resp = auth_client.get(f"/rfq/{rfq['id']}")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'data-testid="cchcs-pc-link-panel"' not in html, (
        "CCHCS panel leaked onto non-CCHCS RFQ — PC workflow is CCHCS-only."
    )


# ── Route-layer contract: pc_diff + per-line qty_changed ─────────────────

def test_confirm_sets_pc_diff_for_linked_banner(auth_client, temp_data_dir):
    """The Linked banner at rfq_detail.html:297 reads r.pc_diff.ported and
    r.pc_diff.qty_changed. The confirm route must populate these so the
    banner is truthful post-link."""
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), {rfq["id"]: rfq})
    _write_json(os.path.join(temp_data_dir, "price_checks.json"), {pc["id"]: pc})

    resp = auth_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": pc["id"]},
    )
    assert resp.status_code == 200

    # Now render the RFQ detail — the Linked banner should show counts.
    detail = auth_client.get(f"/rfq/{rfq['id']}")
    assert detail.status_code == 200
    html = detail.get_data(as_text=True)
    assert "Linked to PC #PC-2026-999" in html or "Linked to PC #pc-999" in html, (
        "Linked banner missing pc_number/id after confirm"
    )
    # "2 prices ported" + "Δ1 qty change"
    assert "2 prices ported" in html, "Ported count missing from Linked banner"
    assert "&#916;1 qty change" in html or "Δ1 qty change" in html, (
        "Qty-change count missing from Linked banner"
    )


def test_confirm_persists_per_line_qty_changed_flag(auth_client, temp_data_dir):
    """The `qty_changed=True` flag on each line is what downstream fillers,
    repricers, and UI markers read. It must land on the saved RFQ."""
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), {rfq["id"]: rfq})
    _write_json(os.path.join(temp_data_dir, "price_checks.json"), {pc["id"]: pc})

    resp = auth_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": pc["id"]},
    )
    assert resp.status_code == 200

    # Reload via suggestions endpoint — fetches saved state.
    resp2 = auth_client.get(f"/api/rfq/{rfq['id']}/pc-link-suggestions")
    assert resp2.get_json()["already_linked"] is True

    # Direct inspection through data_layer load to confirm disk state.
    from src.api.data_layer import load_rfqs
    rfqs = load_rfqs()
    saved = rfqs[rfq["id"]]
    items = saved.get("line_items") or saved.get("items") or []
    assert len(items) == 2
    # W12919 (qty 10 == 10) unchanged, FN4368 (50 → 100) flagged
    by_mfg = {it.get("mfg_number"): it for it in items}
    assert by_mfg["W12919"]["qty_changed"] is False
    assert by_mfg["W12919"]["unit_price"] == 45.00
    assert by_mfg["FN4368"]["qty_changed"] is True
    assert by_mfg["FN4368"]["pc_original_qty"] == 50
    # Commitment price survives on the flagged line until operator reprices
    assert by_mfg["FN4368"]["unit_price"] == 18.50
