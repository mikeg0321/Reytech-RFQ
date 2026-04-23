"""Bundle-5 PR-5b — Mark-as-sent manually + post-send lock.

Closes audit item AA (session_audit 2026-04-22): RFQ 10840486 was emailed
via the manual merge-script + Gmail, so the in-app send route never fired.
The record stayed at status=generated indefinitely, polluting the active
queue. This file locks the escape-valve endpoints and the post-send
line-item lock so future refactors can't silently regress them.
"""
from __future__ import annotations

import io
import json
from pathlib import Path


# ── Mark Sent Manually — RFQ ────────────────────────────────────────────────


def test_rfq_mark_sent_manually_flips_status_and_stamps_metadata(
        auth_client, temp_data_dir):
    """Happy path: POST multipart, status flips, manual_sent_metadata set."""
    from src.api.data_layer import _save_single_rfq, load_rfqs
    rid = "rfq_manual_send_001"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "RFQ-TEST-10840",
        "solicitation_number": "RFQ-TEST-10840",
        "institution": "CCHCS",
        "requestor_email": "buyer@state.ca.gov",
        "line_items": [{"description": "Item A", "qty": 1, "uom": "EA"}],
    })

    resp = auth_client.post(
        f"/api/rfq/{rid}/mark-sent-manually",
        data={
            "sent_to": "buyer@state.ca.gov",
            "sent_at": "2026-04-22T10:00:00",
            "notes": "Sent via manual Gmail — LPA IT",
        },
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200, resp.data[:300]
    body = resp.get_json()
    assert body["ok"] is True
    assert body["status"] == "sent"
    assert body["sent_to"] == "buyer@state.ca.gov"
    assert body["prior_status"] == "generated"

    # Record must be updated in the DAL (SQLite + JSON).
    rfq_after = load_rfqs()[rid]
    assert rfq_after["status"] == "sent"
    assert rfq_after["sent_to"] == "buyer@state.ca.gov"
    assert rfq_after["sent_method"] == "manual"
    md = rfq_after["manual_sent_metadata"]
    assert md["prior_status"] == "generated"
    assert md["notes"] == "Sent via manual Gmail — LPA IT"
    assert md["attachment"] is None


def test_rfq_mark_sent_manually_saves_attachment(auth_client, temp_data_dir):
    """Attachment upload is saved under uploads/manual_sent/<rid>/."""
    from src.api.data_layer import _save_single_rfq, load_rfqs
    rid = "rfq_attach_001"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "RFQ-ATTACH",
        "line_items": [{"description": "X", "qty": 1}],
    })

    fake_pdf = io.BytesIO(b"%PDF-1.4\n%%EOF\n")
    resp = auth_client.post(
        f"/api/rfq/{rid}/mark-sent-manually",
        data={
            "sent_to": "b@x.gov",
            "notes": "manual",
            "attachment": (fake_pdf, "quote_10840486_hand_filled.pdf"),
        },
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    att = body["attachment"]
    assert att is not None
    assert att["filename"] == "quote_10840486_hand_filled.pdf"
    assert Path(att["path"]).exists()
    assert att["size"] > 0

    rfq_after = load_rfqs()[rid]
    md = rfq_after["manual_sent_metadata"]
    assert md["attachment"]["filename"] == "quote_10840486_hand_filled.pdf"


def test_rfq_mark_sent_manually_defaults_sent_to_to_buyer_email(
        auth_client, temp_data_dir):
    """If the caller omits sent_to, the record's buyer email is used."""
    from src.api.data_layer import _save_single_rfq
    rid = "rfq_default_to_001"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "R1",
        "requestor_email": "default-buyer@agency.ca.gov",
        "line_items": [],
    })

    resp = auth_client.post(
        f"/api/rfq/{rid}/mark-sent-manually",
        data={"notes": "no sent_to override"},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["sent_to"] == "default-buyer@agency.ca.gov"


def test_rfq_mark_sent_manually_returns_404_for_missing_rfq(auth_client):
    resp = auth_client.post(
        "/api/rfq/does_not_exist/mark-sent-manually",
        data={"sent_to": "x@y.gov"},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 404
    assert resp.get_json()["ok"] is False


# ── Mark Sent Manually — PC ─────────────────────────────────────────────────


def test_pc_mark_sent_manually_flips_status_and_stamps_metadata(
        auth_client, temp_data_dir):
    """PC parallel to the RFQ endpoint — same contract, same metadata shape."""
    from src.api.data_layer import _save_single_pc, _load_price_checks
    pid = "pc_manual_send_001"
    _save_single_pc(pid, {
        "id": pid, "status": "generated",
        "pc_number": "TEST-MSEND",
        "institution": "CCHCS",
        "requestor_email": "buyer@state.ca.gov",
        "items": [{"description": "Item A", "qty": 1}],
    })

    resp = auth_client.post(
        f"/api/pricecheck/{pid}/mark-sent-manually",
        data={"sent_to": "buyer@state.ca.gov", "notes": "manual gmail"},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200, resp.data[:300]
    body = resp.get_json()
    assert body["ok"] is True
    assert body["status"] == "sent"

    pc_after = _load_price_checks()[pid]
    assert pc_after["status"] == "sent"
    assert pc_after["sent_method"] == "manual"
    assert pc_after["manual_sent_metadata"]["prior_status"] == "generated"


def test_pc_mark_sent_manually_returns_404_for_missing_pc(auth_client):
    resp = auth_client.post(
        "/api/pricecheck/nope/mark-sent-manually",
        data={},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 404


# ── Post-send lock (banner + readonly script) ───────────────────────────────


def test_rfq_detail_renders_post_send_lock_banner_when_sent(
        auth_client, temp_data_dir):
    from src.api.data_layer import _save_single_rfq
    rid = "rfq_lock_sent"
    _save_single_rfq(rid, {
        "id": rid, "status": "sent",
        "rfq_number": "RFQ-LOCK",
        "solicitation_number": "RFQ-LOCK",
        "sent_at": "2026-04-22T10:00:00",
        "line_items": [{"description": "X", "qty": 1, "uom": "EA"}],
    })
    resp = auth_client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")
    assert 'data-testid="rfq-post-send-lock-banner"' in html
    # Client-side lock script must be present so new inputs added later
    # are automatically locked when status is sent.
    assert "lockSentRfqFields" in html


def test_rfq_detail_does_not_render_lock_banner_when_active(
        auth_client, temp_data_dir):
    from src.api.data_layer import _save_single_rfq
    rid = "rfq_lock_active"
    _save_single_rfq(rid, {
        "id": rid, "status": "draft",
        "rfq_number": "RFQ-ACT",
        "solicitation_number": "RFQ-ACT",
        "line_items": [{"description": "X", "qty": 1, "uom": "EA"}],
    })
    resp = auth_client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")
    assert 'data-testid="rfq-post-send-lock-banner"' not in html


def test_rfq_detail_shows_mark_sent_manually_button_when_generated(
        auth_client, temp_data_dir):
    from src.api.data_layer import _save_single_rfq
    rid = "rfq_btn_001"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "RFQ-BTN",
        "solicitation_number": "RFQ-BTN",
        "line_items": [{"description": "X", "qty": 1}],
    })
    resp = auth_client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")
    # Button wired to the modal, not the old plain updateRfqStatus('sent')
    assert 'data-testid="rfq-mark-sent-primary"' in html
    assert "openMarkSentModal()" in html
    # Modal markup rendered on the page:
    assert 'id="mark-sent-modal"' in html
    assert "submitMarkSent" in html
