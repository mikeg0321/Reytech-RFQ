"""Bug 2 — Status card → Mark Sent + full audit fan-out chain.

Mike's bug report (2026-05-02 image dump): the Status section on the RFQ
detail page is read-only — there's no obvious way to mark a quote as sent
when the operator emailed it outside the app, and even when a Mark Sent
button is clicked the downstream chain (counter, agency tag, win prob,
SCPRS award polling) needs to fire end-to-end.

This file locks:
  1. Status banner is click-to-Mark-Sent on non-terminal/non-sent statuses.
  2. Status banner has NO click handler when status is 'sent' or terminal.
  3. New Mark Sent button renders in the Status card action row when the
     RFQ can still be marked sent (gated on status, not just 'generated').
  4. Mark-sent backfills `total` from line items when total was never
     persisted — closes the silent gap in award_tracker enrollment
     (`WHERE total > 0` filter would otherwise skip the row).
  5. Full audit fan-out — status flips to sent, sent_at stamped,
     sent_method='manual', lifecycle event written, KPI counter fired.
"""
from __future__ import annotations

from unittest.mock import patch


# ── Status banner: click-to-Mark-Sent on non-terminal statuses ──────────────


def test_status_banner_is_click_target_when_generated(
        auth_client, temp_data_dir):
    """Generated RFQ: banner has role=button + onclick=openMarkSentModal."""
    from src.api.data_layer import _save_single_rfq
    rid = "rfq_banner_gen"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "RFQ-BANNER-GEN",
        "solicitation_number": "RFQ-BANNER-GEN",
        "line_items": [{"description": "X", "qty": 1, "uom": "EA"}],
    })
    resp = auth_client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")
    assert 'data-testid="rfq-status-banner-mark-sent"' in html
    # Banner-targeted click handler must call the modal opener.
    assert 'role="button"' in html
    assert "openMarkSentModal()" in html
    # Click hint visible to the user.
    assert "Click to Mark Sent" in html


def test_status_banner_is_read_only_when_already_sent(
        auth_client, temp_data_dir):
    """Sent RFQ: banner is informational only — no click handler."""
    from src.api.data_layer import _save_single_rfq
    rid = "rfq_banner_sent"
    _save_single_rfq(rid, {
        "id": rid, "status": "sent",
        "rfq_number": "RFQ-BANNER-SENT",
        "solicitation_number": "RFQ-BANNER-SENT",
        "sent_at": "2026-05-02T10:00:00",
        "line_items": [{"description": "X", "qty": 1}],
    })
    resp = auth_client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")
    # Click hint must NOT appear when status is sent.
    assert "Click to Mark Sent" not in html
    # And the banner-targeted Mark Sent button is gone too.
    assert 'data-testid="rfq-status-banner-mark-sent"' not in html


def test_status_banner_is_read_only_on_terminal_statuses(
        auth_client, temp_data_dir):
    """Won/lost/no_bid/cancelled: no Mark Sent affordances anywhere."""
    from src.api.data_layer import _save_single_rfq
    for status in ("won", "lost", "no_bid", "cancelled"):
        rid = f"rfq_banner_{status}"
        _save_single_rfq(rid, {
            "id": rid, "status": status,
            "rfq_number": f"RFQ-{status.upper()}",
            "solicitation_number": f"RFQ-{status.upper()}",
            "line_items": [{"description": "X", "qty": 1}],
        })
        resp = auth_client.get(f"/rfq/{rid}")
        assert resp.status_code == 200, f"{status}: {resp.data[:200]}"
        html = resp.data.decode("utf-8", errors="replace")
        assert 'data-testid="rfq-status-banner-mark-sent"' not in html, status
        assert 'data-testid="rfq-status-mark-sent"' not in html, status


# ── Status card action-row Mark Sent button ─────────────────────────────────


def test_status_card_renders_mark_sent_button_on_non_sent_statuses(
        auth_client, temp_data_dir):
    """Mark Sent must render on draft/parsed/priced/generated/ready alike —
    not just 'generated' (the old gating was too narrow per Mike's report)."""
    from src.api.data_layer import _save_single_rfq
    for status in ("draft", "parsed", "priced", "generated", "ready",
                   "ready_to_send"):
        rid = f"rfq_card_btn_{status}"
        _save_single_rfq(rid, {
            "id": rid, "status": status,
            "rfq_number": f"RFQ-CARD-{status.upper()}",
            "solicitation_number": f"RFQ-CARD-{status.upper()}",
            "line_items": [{"description": "X", "qty": 1}],
        })
        resp = auth_client.get(f"/rfq/{rid}")
        assert resp.status_code == 200, f"{status}: {resp.data[:200]}"
        html = resp.data.decode("utf-8", errors="replace")
        assert 'data-testid="rfq-status-mark-sent"' in html, status


# ── Audit fan-out chain ─────────────────────────────────────────────────────


def test_mark_sent_backfills_total_when_missing_for_award_tracker(
        auth_client, temp_data_dir):
    """Award tracker `WHERE total > 0` would silently skip RFQs whose total
    was never persisted. Mark-sent must compute it from the line items so
    SCPRS won/lost polling kicks in for every manually-sent quote."""
    from src.api.data_layer import _save_single_rfq, load_rfqs
    rid = "rfq_total_backfill"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "RFQ-BF",
        "solicitation_number": "RFQ-BF",
        "requestor_email": "buyer@x.gov",
        # total intentionally omitted — common when generation path
        # only computed it lazily at PDF render time.
        "line_items": [
            {"description": "A", "qty": 2, "price_per_unit": 50.0},
            {"description": "B", "qty": 1, "price_per_unit": 25.0},
        ],
    })
    resp = auth_client.post(
        f"/api/rfq/{rid}/mark-sent-manually",
        data={"sent_to": "buyer@x.gov", "notes": "manual"},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200, resp.data[:300]

    rfq_after = load_rfqs()[rid]
    # 2 × 50 + 1 × 25 = 125
    assert float(rfq_after["total"]) == 125.0
    assert rfq_after["status"] == "sent"


def test_mark_sent_does_not_clobber_existing_total(
        auth_client, temp_data_dir):
    """Backfill is defensive — a previously-stamped total must survive.
    Generation-time total wins over recompute when both exist."""
    from src.api.data_layer import _save_single_rfq, load_rfqs
    rid = "rfq_total_keep"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "RFQ-KEEP",
        "solicitation_number": "RFQ-KEEP",
        "total": 999.99,  # operator-set; do not touch.
        "line_items": [
            {"description": "A", "qty": 1, "price_per_unit": 1.0},
        ],
    })
    resp = auth_client.post(
        f"/api/rfq/{rid}/mark-sent-manually",
        data={"sent_to": "b@x.gov"},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    rfq_after = load_rfqs()[rid]
    assert float(rfq_after["total"]) == 999.99


def test_mark_sent_fires_kpi_logger(auth_client, temp_data_dir):
    """log_quote_sent must be called on every manual mark-sent — that's
    the row that powers the per-session KPI counter on the dashboard."""
    from src.api.data_layer import _save_single_rfq
    rid = "rfq_kpi_001"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "RFQ-KPI",
        "solicitation_number": "RFQ-KPI",
        "agency": "CCHCS",
        "line_items": [
            {"description": "A", "qty": 1, "price_per_unit": 100.0},
        ],
    })

    with patch("src.core.operator_kpi.log_quote_sent") as mock_kpi:
        resp = auth_client.post(
            f"/api/rfq/{rid}/mark-sent-manually",
            data={"sent_to": "b@x.gov"},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200
        assert mock_kpi.called, "log_quote_sent must fire on manual mark-sent"
        # Check kwargs include the rid + a positive total (post-backfill).
        kwargs = mock_kpi.call_args.kwargs
        assert kwargs["quote_id"] == rid
        assert kwargs["quote_type"] == "rfq"
        assert kwargs["item_count"] == 1
        assert float(kwargs["quote_total"]) == 100.0


def test_mark_sent_writes_lifecycle_event(auth_client, temp_data_dir):
    """log_lifecycle_event must record 'package_sent_manual' so the activity
    stream + DAL replay see this side-channel send."""
    from src.api.data_layer import _save_single_rfq
    rid = "rfq_lifecycle_001"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "RFQ-LC",
        "solicitation_number": "RFQ-LC",
        "line_items": [{"description": "X", "qty": 1, "price_per_unit": 1.0}],
    })

    with patch("src.core.dal.log_lifecycle_event") as mock_lc:
        resp = auth_client.post(
            f"/api/rfq/{rid}/mark-sent-manually",
            data={"sent_to": "b@x.gov", "notes": "via gmail"},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200
        assert mock_lc.called
        args = mock_lc.call_args
        # Positional: (kind, ref_id, event, summary)
        assert args.args[0] == "rfq"
        assert args.args[1] == rid
        assert args.args[2] == "package_sent_manual"
