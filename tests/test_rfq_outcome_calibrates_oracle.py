"""IN-1 regression guard: /api/rfq/<rid>/outcome must calibrate Oracle.

Prior to 2026-04-21 the endpoint wrote wl_log + catalog rows but never
called calibrate_from_outcome, so every manual RFQ win/loss evaporated
from Oracle — the same silent-no-op shape as the Feb-Apr markQuote bug
project_oracle_v5_phase1.md thought was closed.

This test asserts that calibrate_from_outcome is invoked for both the
won and lost paths, with the agency/items the RFQ carries. Locked in so
the wiring can't go silently missing again.
"""
from __future__ import annotations

from unittest.mock import patch


def _seed_rfq():
    """Seed a single RFQ via the data layer (SQLite-backed)."""
    from src.api.data_layer import save_rfqs
    rfqs = {
        "rfq_in1": {
            "status": "quoted",
            "solicitation_number": "SOL-IN1",
            "agency_name": "CCHCS",
            "agency": "CCHCS",
            "delivery_location": "STATE PRISON",
            "requestor_name": "Test Buyer",
            "line_items": [
                {"description": "Nitrile Gloves L", "price_per_unit": 10.0, "qty": 100},
                {"description": "Exam Gown", "price_per_unit": 4.25, "qty": 50},
            ],
            "parsed_at": "2026-04-01T10:00:00",
            "received_at": "2026-04-01T10:00:00",
            "status_history": [],
        }
    }
    save_rfqs(rfqs)


def test_rfq_outcome_won_calls_calibrate(auth_client, temp_data_dir):
    _seed_rfq()

    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as mock_cal, \
         patch("src.agents.product_catalog.record_won_price"), \
         patch("src.agents.product_catalog.init_catalog_db"):
        resp = auth_client.post(
            "/api/rfq/rfq_in1/outcome",
            json={"outcome": "won", "reason": "best price"},
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)
        assert resp.get_json().get("ok") is True

    assert mock_cal.called, "calibrate_from_outcome must fire on RFQ won"
    args, kwargs = mock_cal.call_args
    # positional: items, outcome
    assert args[1] == "won"
    assert kwargs.get("agency") == "CCHCS"
    assert len(args[0]) == 2  # both line items passed through


def test_rfq_outcome_lost_price_reason_maps_to_price_bucket(auth_client, temp_data_dir):
    _seed_rfq()

    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as mock_cal:
        resp = auth_client.post(
            "/api/rfq/rfq_in1/outcome",
            json={
                "outcome": "lost",
                "reason": "lost on price",
                "competitor": "ACME Supply",
                "competitor_price": 8.50,
            },
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)

    assert mock_cal.called
    kwargs = mock_cal.call_args.kwargs
    assert kwargs.get("loss_reason") == "price"
    # competitor_price should have been fanned out across every line index
    winner_prices = kwargs.get("winner_prices")
    assert winner_prices == {0: 8.5, 1: 8.5}


def test_rfq_outcome_lost_non_price_reason_maps_to_other(auth_client, temp_data_dir):
    _seed_rfq()

    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as mock_cal:
        resp = auth_client.post(
            "/api/rfq/rfq_in1/outcome",
            json={"outcome": "lost", "reason": "relationship with incumbent"},
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)

    assert mock_cal.called
    assert mock_cal.call_args.kwargs.get("loss_reason") == "other"


def test_rfq_outcome_no_response_does_not_calibrate(auth_client, temp_data_dir):
    """no_response / expired aren't calibration-worthy — they're silence, not loss."""
    _seed_rfq()

    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as mock_cal:
        resp = auth_client.post(
            "/api/rfq/rfq_in1/outcome",
            json={"outcome": "no_response", "reason": ""},
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)

    assert not mock_cal.called, "no_response must NOT feed Oracle"
