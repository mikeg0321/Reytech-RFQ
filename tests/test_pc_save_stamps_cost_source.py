"""Phase 4-A regression: when an operator types a cost into a PC cell and
saves, `_do_save_prices` MUST stamp `pricing.cost_source = 'operator'` on
that item. Without this stamp, the upcoming "Refresh costs" workflow can't
distinguish operator-typed values from legacy/Amazon-poisoned values, and
will wipe operator work on PCs created since Phase 1 deployed.

Single load-bearing test — if this fails, Refresh is unsafe to ship.
"""
import os
import sqlite3
import tempfile

import pytest


@pytest.fixture
def app_with_test_pc(monkeypatch):
    """Spin up the Flask app with a seeded test PC."""
    import sys
    sys.path.insert(0, ".")
    monkeypatch.setenv("SECRET_KEY", "test-secret-key-32-characters-long-yes")
    monkeypatch.setenv("DASH_USER", "Reytech")
    monkeypatch.setenv("DASH_PASS", "Reytech")
    monkeypatch.setenv("ENABLE_TEST_ROUTES", "1")

    from app import create_app
    app = create_app()
    return app


def test_operator_cost_save_stamps_cost_source(app_with_test_pc):
    """The load-bearing safety test for Phase 4-A.

    Operator types a cost into a fresh PC → autosave POSTs the value →
    `_do_save_prices` MUST stamp `pricing.cost_source = 'operator'` on
    that item. Without this stamp, Refresh would wipe the value because
    the predicate can't tell operator-typed from auto-fill."""
    import base64
    auth = base64.b64encode(b"Reytech:Reytech").decode()
    hdrs = {"Authorization": f"Basic {auth}"}
    client = app_with_test_pc.test_client()

    # 1. Create a fresh test PC
    r = client.get("/api/test/create-pc", headers=hdrs)
    assert r.status_code == 200, r.get_data(as_text=True)
    pc_id = r.get_json().get("pc_id")
    assert pc_id

    # 2. Operator "types" a cost on item 0
    r = client.post(
        f"/pricecheck/{pc_id}/save-prices",
        headers={**hdrs, "Content-Type": "application/json"},
        json={"cost_0": "400.00", "markup_0": "35"},
    )
    assert r.status_code == 200, r.get_data(as_text=True)

    # 3. Reload the PC and assert cost_source was stamped
    import src.api.dashboard as _dash_mod
    fn = getattr(_dash_mod, "_load_price_checks", None)
    assert fn, "_load_price_checks should be exposed via dashboard"
    pcs = fn()
    pc = pcs.get(pc_id)
    assert pc, f"PC {pc_id} not found after save"
    items = pc.get("items", [])
    assert items, "PC has no items"
    item0 = items[0]
    pricing = item0.get("pricing", {}) or {}

    saved_cost = pricing.get("unit_cost") or item0.get("vendor_cost")
    assert saved_cost == 400.00, f"cost did not persist: {saved_cost}"

    cost_source = pricing.get("cost_source")
    assert cost_source == "operator", (
        f"_do_save_prices MUST stamp cost_source='operator' on operator-typed "
        f"cost saves. Got cost_source={cost_source!r}. Without this stamp, "
        f"the Refresh workflow will wipe operator work on every PC created "
        f"since Phase 1 deploy. See project_in_flight_pc_recovery_gap memory."
    )


def test_zero_cost_save_clears_cost_source(app_with_test_pc):
    """When the operator clears a cost (sets it to 0/empty), the cost_source
    stamp must be REMOVED — otherwise a later non-operator cascade hit on
    the same row would be incorrectly tagged 'operator' if it lingered."""
    import base64
    auth = base64.b64encode(b"Reytech:Reytech").decode()
    hdrs = {"Authorization": f"Basic {auth}"}
    client = app_with_test_pc.test_client()

    r = client.get("/api/test/create-pc", headers=hdrs)
    pc_id = r.get_json().get("pc_id")
    assert pc_id

    # Stamp once
    client.post(
        f"/pricecheck/{pc_id}/save-prices",
        headers={**hdrs, "Content-Type": "application/json"},
        json={"cost_0": "400.00"},
    )
    # Then clear
    client.post(
        f"/pricecheck/{pc_id}/save-prices",
        headers={**hdrs, "Content-Type": "application/json"},
        json={"cost_0": ""},
    )

    import src.api.dashboard as _dash_mod
    fn = getattr(_dash_mod, "_load_price_checks", None)
    pcs = fn()
    pc = pcs.get(pc_id)
    item0 = pc["items"][0]
    pricing = item0.get("pricing", {}) or {}
    assert "cost_source" not in pricing or pricing.get("cost_source") != "operator", (
        f"After clearing cost, cost_source should be removed. Got "
        f"pricing={pricing!r}. Without this clear, a Refresh→cascade flow "
        f"could surface a stale 'operator' tag on a row whose cost has been "
        f"reset by a different code path."
    )
