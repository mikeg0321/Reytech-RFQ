"""Regression guard: manual RFQ creation must propagate is_test.

Audit 2026-04-22 (RE-AUDIT-5, P0):
  The manual RFQ creation route at routes_rfq.py:`api_rfq_create_manual`
  did not copy the client's `is_test` flag into the persisted rfq dict.
  Test RFQs typed in through the dashboard therefore landed with no
  is_test marker and leaked into production queries/analytics (the
  email-ingest path already honors the flag; manual was the gap).

This test locks the propagation in so the flag can never silently drop
out again.
"""
from __future__ import annotations


def test_manual_rfq_create_propagates_is_test_true(client):
    """POST with is_test=True must persist is_test=True on the rfq row."""
    resp = client.post(
        "/api/rfq/create-manual",
        json={
            "solicitation_number": "TEST-SOL-IS-TEST-TRUE",
            "agency": "cchcs",
            "requestor_name": "QA Tester",
            "is_test": True,
        },
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body.get("ok") is True
    rid = body["rfq_id"]

    from src.api.data_layer import load_rfqs
    rfqs = load_rfqs()
    assert rid in rfqs, f"RFQ {rid} not persisted"
    assert rfqs[rid].get("is_test") is True, (
        "is_test=True from manual-create payload did not persist — "
        "RE-AUDIT-5 regression. Check routes_rfq.py api_rfq_create_manual."
    )


def test_manual_rfq_create_defaults_is_test_false(client):
    """Omitting is_test must default to False (not missing, not truthy)."""
    resp = client.post(
        "/api/rfq/create-manual",
        json={
            "solicitation_number": "TEST-SOL-IS-TEST-DEFAULT",
            "agency": "cchcs",
            "requestor_name": "Real Buyer",
        },
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    rid = resp.get_json()["rfq_id"]

    from src.api.data_layer import load_rfqs
    rfqs = load_rfqs()
    assert rid in rfqs
    assert rfqs[rid].get("is_test") is False, (
        "omitted is_test must default to False, not None/missing. "
        "Downstream filters use `not v.get('is_test')` which is OK for "
        "missing/False, but explicit False is the contract."
    )


def test_manual_rfq_create_is_test_false_explicit(client):
    """is_test=False explicit must also persist as False."""
    resp = client.post(
        "/api/rfq/create-manual",
        json={
            "solicitation_number": "TEST-SOL-IS-TEST-FALSE",
            "agency": "cchcs",
            "is_test": False,
        },
    )
    assert resp.status_code == 200
    rid = resp.get_json()["rfq_id"]

    from src.api.data_layer import load_rfqs
    rfqs = load_rfqs()
    assert rfqs[rid].get("is_test") is False
