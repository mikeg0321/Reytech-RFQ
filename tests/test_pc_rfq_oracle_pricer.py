"""Tests for the oracle pricer adapter + end-to-end reprice flow.

The adapter (`src/core/pc_rfq_reprice_adapter.oracle_pricer_for_line`) is the
production pricer passed into `reprice_qty_changed_lines`. Until PR #291 the
confirm-pc-link route passed `None` here, which turned every qty-drifted
line into `skipped_no_price`. Now the oracle is wired in; these tests lock
the contract Mike cares about:

  1. The adapter returns ONLY the allowlisted price fields — no description,
     no qty, no mfg# leakage. The reprice helper's own allowlist backstops
     this, but guarding here too catches regressions at the right layer.

  2. The adapter returns None (not a fabricated price) when the oracle
     can't find enough data. Skipped lines beat wrong lines.

  3. The confirm-pc-link route with reprice=true actually calls the oracle
     (via monkeypatch), with the drifted line's new qty and agency. PC
     commitment prices on qty-unchanged lines survive untouched.

  4. The per-line `repriced_reason="qty_change"` marker is written by the
     helper so the RFQ detail template can render a "✓ reprice" badge.
"""
from __future__ import annotations

import json
import os

import pytest

from src.core.pc_rfq_reprice_adapter import oracle_pricer_for_line
from src.core import pc_rfq_reprice_adapter as _adapter_mod


# ── Adapter unit tests ────────────────────────────────────────────────────

def test_adapter_returns_only_allowlisted_price_fields(monkeypatch):
    """Even if the oracle's result dict contains description/qty-shaped keys,
    the adapter must discard them and return only {supplier_cost, unit_price,
    bid_price, markup_pct}."""
    def _fake_get_pricing(description, quantity, cost, item_number, department):
        return {
            "description": "SHOULD NOT LEAK",
            "quantity": 9999,
            "cost": {"locked_cost": 8.5, "provided_cost": 10.0},
            "recommendation": {
                "quote_price": 17.0,
                "markup_pct": 100.0,
                "description": "SHOULD NOT LEAK EITHER",
                "quantity": 9999,
            },
        }
    monkeypatch.setattr(_adapter_mod, "get_pricing", _fake_get_pricing, raising=False)
    # ImportError-safe: the real import is lazy inside the function.
    import src.core.pricing_oracle_v2 as _poll
    monkeypatch.setattr(_poll, "get_pricing", _fake_get_pricing)

    out = oracle_pricer_for_line(
        {"description": "Test item", "quantity": 100,
         "mfg_number": "MFG-1", "supplier_cost": 10.0},
        agency="CCHCS",
    )
    assert out is not None
    assert set(out.keys()) <= {"supplier_cost", "unit_price", "bid_price", "markup_pct"}
    assert out["unit_price"] == 17.0
    assert out["bid_price"] == 17.0
    # Prefer oracle's locked_cost over provided_cost
    assert out["supplier_cost"] == 8.5
    assert out["markup_pct"] == 100.0


def test_adapter_returns_none_when_oracle_has_no_quote_price(monkeypatch):
    """If get_pricing returns no quote_price (and no strategies), the
    adapter returns None so the helper counts the line as skipped_no_price."""
    def _fake_no_price(*args, **kwargs):
        return {"recommendation": {"quote_price": None}, "cost": {}, "strategies": []}
    import src.core.pricing_oracle_v2 as _poll
    monkeypatch.setattr(_poll, "get_pricing", _fake_no_price)

    out = oracle_pricer_for_line(
        {"description": "Obscure item", "quantity": 50,
         "supplier_cost": 5.0},
        agency="CCHCS",
    )
    assert out is None


def test_adapter_returns_none_when_oracle_raises(monkeypatch):
    """Never propagate oracle exceptions to the reprice path — turn them
    into None so the operator sees `skipped_no_price` and follows up."""
    def _fake_boom(*args, **kwargs):
        raise RuntimeError("DB exploded")
    import src.core.pricing_oracle_v2 as _poll
    monkeypatch.setattr(_poll, "get_pricing", _fake_boom)

    out = oracle_pricer_for_line(
        {"description": "Test item", "quantity": 10},
        agency="CCHCS",
    )
    assert out is None


def test_adapter_falls_back_to_strategy_price_when_quote_price_missing(monkeypatch):
    """Older oracle paths populate `strategies` without setting `quote_price`
    at the top level. The adapter must still extract a usable bid."""
    def _fake_with_strategies(*args, **kwargs):
        return {
            "recommendation": {
                "quote_price": None,
                "markup_pct": None,
                "strategies": [
                    {"name": "Win Price", "price": 22.50, "markup_pct": 125.0},
                ],
            },
            "cost": {"provided_cost": 10.0},
        }
    import src.core.pricing_oracle_v2 as _poll
    monkeypatch.setattr(_poll, "get_pricing", _fake_with_strategies)

    out = oracle_pricer_for_line(
        {"description": "Test", "quantity": 20, "supplier_cost": 10.0},
        agency="CCHCS",
    )
    assert out is not None
    assert out["unit_price"] == 22.5


def test_adapter_computes_markup_when_oracle_omits_it(monkeypatch):
    """If the oracle returns price + cost but no markup_pct, compute it
    ourselves so the RFQ line carries a consistent markup field."""
    def _fake_no_markup(*args, **kwargs):
        return {
            "recommendation": {"quote_price": 20.0, "markup_pct": None},
            "cost": {"locked_cost": 10.0},
        }
    import src.core.pricing_oracle_v2 as _poll
    monkeypatch.setattr(_poll, "get_pricing", _fake_no_markup)

    out = oracle_pricer_for_line(
        {"description": "Test", "quantity": 50},
        agency="CCHCS",
    )
    assert out is not None
    # (20-10)/10 * 100 = 100%
    assert out["markup_pct"] == 100.0


def test_adapter_skips_empty_description():
    out = oracle_pricer_for_line({"quantity": 10}, agency="CCHCS")
    assert out is None


# ── Route-level: reprice=true invokes oracle, respects PC commitments ─────

def _cchcs_rfq():
    return {
        "id": "rfq-reprice-1",
        "solicitation_number": "PC-2026-555-RFQ",
        "requestor_email": "buyer@cchcs.ca.gov",
        "institution": "CCHCS",
        "agency": "CCHCS",
        "status": "new",
        "line_items": [
            # W12919: same qty — PC commitment price MUST survive
            {"mfg_number": "W12919", "description": "BP cuff", "quantity": 10},
            # FN4368: qty 50 → 150 — MUST be repriced by the oracle
            {"mfg_number": "FN4368", "description": "Gloves", "quantity": 150},
        ],
    }


def _cchcs_pc():
    return {
        "id": "pc-555", "pc_number": "PC-2026-555",
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


def _write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f)


def test_confirm_with_reprice_calls_oracle_only_for_drifted_lines(
    auth_client, temp_data_dir, monkeypatch
):
    """The whole point of selective reprice: the commitment line stays,
    the drifted line gets a fresh oracle price."""
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), {rfq["id"]: rfq})
    _write_json(os.path.join(temp_data_dir, "price_checks.json"), {pc["id"]: pc})

    oracle_calls = []

    def _fake_oracle(description, quantity, cost, item_number, department):
        oracle_calls.append({
            "description": description, "quantity": quantity,
            "cost": cost, "item_number": item_number,
            "department": department,
        })
        # Volume break at 150 units: cheaper per-unit
        return {
            "recommendation": {"quote_price": 15.75, "markup_pct": 75.0},
            "cost": {"locked_cost": 9.0},
        }
    import src.core.pricing_oracle_v2 as _poll
    monkeypatch.setattr(_poll, "get_pricing", _fake_oracle)

    resp = auth_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": pc["id"], "reprice": True},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body["ok"] is True
    assert body["reprice"]["repriced"] == 1
    assert body["reprice"]["skipped_no_change"] == 1
    assert body["reprice"]["skipped_no_price"] == 0

    # Oracle called EXACTLY once — only for the drifted FN4368 line.
    assert len(oracle_calls) == 1
    call = oracle_calls[0]
    assert "gloves" in call["description"].lower() \
        or "FN4368" in (call.get("item_number") or "")
    assert call["quantity"] == 150   # the new qty, not the PC qty
    assert call["department"] == "CCHCS"

    # Verify disk state: commitment line untouched, drifted line repriced.
    from src.api.data_layer import load_rfqs
    saved = load_rfqs()[rfq["id"]]
    items = saved.get("line_items") or saved.get("items") or []
    by_mfg = {it.get("mfg_number"): it for it in items}
    # W12919: PC commitment price VERBATIM — never saw the oracle
    assert by_mfg["W12919"]["unit_price"] == 45.00
    assert by_mfg["W12919"]["bid_price"] == 45.00
    assert "repriced_reason" not in by_mfg["W12919"]
    # FN4368: new oracle price, audit marker set, qty_changed cleared
    assert by_mfg["FN4368"]["unit_price"] == 15.75
    assert by_mfg["FN4368"]["bid_price"] == 15.75
    assert by_mfg["FN4368"]["supplier_cost"] == 9.00
    assert by_mfg["FN4368"]["markup_pct"] == 75.0
    assert by_mfg["FN4368"]["repriced_reason"] == "qty_change"
    assert by_mfg["FN4368"]["qty_changed"] is False


def test_confirm_with_reprice_skips_drifted_lines_when_oracle_has_no_data(
    auth_client, temp_data_dir, monkeypatch
):
    """When the oracle returns no quote_price for a drifted line, that line
    keeps its PC commitment price AND the `qty_changed=True` flag so the
    operator can re-price manually. Mike's rule: wrong is worse than missing."""
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), {rfq["id"]: rfq})
    _write_json(os.path.join(temp_data_dir, "price_checks.json"), {pc["id"]: pc})

    def _fake_no_data(*args, **kwargs):
        return {"recommendation": {"quote_price": None}, "cost": {}, "strategies": []}
    import src.core.pricing_oracle_v2 as _poll
    monkeypatch.setattr(_poll, "get_pricing", _fake_no_data)

    resp = auth_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": pc["id"], "reprice": True},
    )
    body = resp.get_json()
    assert body["reprice"]["repriced"] == 0
    assert body["reprice"]["skipped_no_price"] == 1
    assert body["reprice"]["skipped_no_change"] == 1

    # Drifted line STILL flagged so the operator sees the Δ badge on the
    # detail page and knows to follow up. PC commitment price survives.
    from src.api.data_layer import load_rfqs
    saved = load_rfqs()[rfq["id"]]
    items = saved.get("line_items") or saved.get("items") or []
    by_mfg = {it.get("mfg_number"): it for it in items}
    assert by_mfg["FN4368"]["qty_changed"] is True
    assert by_mfg["FN4368"]["unit_price"] == 18.50   # PC commitment kept
