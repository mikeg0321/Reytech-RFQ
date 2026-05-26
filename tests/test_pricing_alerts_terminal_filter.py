"""Pin: /api/pricing-alerts excludes ALL terminal/inactive statuses.

2026-05-26 Mr. Wolf 13-badge audit: the home ⚠ badge said "13" but
its title decomposed to 3 stale + 9 unpriced + 1 drift-flag = 13.
Investigation found that 2-of-the-9 unpriced RFQs were Argarin/Ragadio
records dispositioned to `no_bid` per the 2026-05-26 substrate-wave
handoff. They shouldn't count as "unpriced" — operator already
decided not to bid.

Root cause: the filter at routes_rfq_admin.py:1888 only excluded 5
of the 11 terminal/inactive statuses canonically used elsewhere in
the codebase (rfq_detail.html, routes_intel_ops.py, etc.).

Substrate-singleness fix: extend `_TERMINAL_OR_INACTIVE` to the
union of every "operator is done with this" status. Same defect class
as PRs #1076/#1086/#1088.
"""
from __future__ import annotations


def test_terminal_set_includes_no_bid():
    """no_bid is the canonical operator-dispositioned status. It MUST
    be excluded from the alert query — otherwise dispositioned RFQs
    inflate the home badge for days/weeks."""
    import inspect
    from src.api.modules import routes_rfq_admin as ram
    src = inspect.getsource(ram.api_pricing_alerts)
    assert "no_bid" in src, (
        "/api/pricing-alerts must exclude no_bid status — see "
        "tests/test_pricing_alerts_terminal_filter.py for rationale"
    )


def test_terminal_set_includes_complete_canonical_union():
    """Pin the full union of terminal/inactive statuses. Adding a new
    status anywhere in the app means it must also land here, or the
    home badge will silently re-inflate."""
    import inspect
    from src.api.modules import routes_rfq_admin as ram
    src = inspect.getsource(ram.api_pricing_alerts)
    expected = [
        "dismissed", "sent", "won", "lost", "cancelled",
        "no_bid", "no_response", "expired",
        "archived", "deleted", "duplicate",
    ]
    missing = [s for s in expected if f'"{s}"' not in src]
    assert not missing, (
        f"/api/pricing-alerts is missing terminal statuses {missing}. "
        "Add to _TERMINAL_OR_INACTIVE tuple in api_pricing_alerts. "
        "Reason: these are all operator-finished states; counting them "
        "as alerts inflates the home ⚠ badge with already-resolved work."
    )


def test_pricing_alerts_excludes_no_bid_records(auth_client, monkeypatch):
    """End-to-end smoke: a no_bid RFQ with all unpriced items must
    NOT contribute to total_alerts."""
    from src.api.modules import routes_rfq_admin as ram

    # Override load_rfqs to return ONLY a single no_bid record.
    def fake_load():
        return {
            "rfq_test_no_bid": {
                "status": "no_bid",
                "solicitation_number": "TEST123",
                "line_items": [
                    {"description": "item A", "qty": 1, "price_per_unit": 0},
                ],
                "created_at": "2026-05-01T00:00:00",
            },
        }

    monkeypatch.setattr(ram, "load_rfqs", fake_load)
    # Clear the function's module-level cache so the test sees fresh data
    monkeypatch.setattr(ram, "_pricing_alerts_cache",
                        {"data": None, "ts": 0})

    r = auth_client.get("/api/pricing-alerts")
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] is True
    # The no_bid RFQ must NOT appear in unpriced
    unpriced_ids = [u.get("id") for u in data.get("unpriced_rfqs", [])]
    assert "rfq_test_no_bid" not in unpriced_ids, (
        "no_bid RFQ leaked into the unpriced alert set"
    )


def test_pricing_alerts_still_counts_active_unpriced(auth_client, monkeypatch):
    """Sanity: a TRULY unpriced active RFQ (status='new') still fires.
    The fix is targeted at terminal statuses; active work still counts."""
    from src.api.modules import routes_rfq_admin as ram

    def fake_load():
        return {
            "rfq_test_active": {
                "status": "new",
                "solicitation_number": "ACTIVE99",
                "line_items": [
                    {"description": "needs price", "qty": 1, "price_per_unit": 0},
                ],
                "created_at": "2026-05-25T00:00:00",
            },
        }

    monkeypatch.setattr(ram, "load_rfqs", fake_load)
    monkeypatch.setattr(ram, "_pricing_alerts_cache",
                        {"data": None, "ts": 0})

    r = auth_client.get("/api/pricing-alerts")
    data = r.get_json()
    unpriced_ids = [u.get("id") for u in data.get("unpriced_rfqs", [])]
    assert "rfq_test_active" in unpriced_ids, (
        "active unpriced RFQ should STILL count — only terminal "
        "statuses are filtered out"
    )
