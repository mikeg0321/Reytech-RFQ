"""Regression tests for RFQ tax lookup persistence.

Covers bugs surfaced on R26Q36 RFQ detail page:
  - rfqTaxLookup() JS wrote to non-existent DOM IDs → rate never visible
  - Duplicate /api/rfq/<rid>/lookup-tax-rate routes — force_live was ignored
  - Delivery_location saves, but chained tax lookup failed silently

Backend coverage here: the route persists tax_rate/tax_validated, and
force_live is honored. JS fix is verified by a template string check.
"""
from __future__ import annotations

import os

import pytest


@pytest.fixture
def _mock_tax_agent(monkeypatch):
    """Return a controllable get_tax_rate stub."""
    calls = {"count": 0, "force_live_seen": None, "args": None}

    def _fake_get_tax_rate(ship_to_name="", ship_to_address=None,
                          street=None, city=None, zip_code=None, force_live=False):
        calls["count"] += 1
        calls["force_live_seen"] = force_live
        calls["args"] = {"street": street, "city": city, "zip": zip_code}
        return {
            "rate": 0.0975,
            "jurisdiction": "Los Angeles County",
            "city": "Los Angeles",
            "county": "Los Angeles",
            "source": "cdtfa_api",
            "confidence": "high",
        }

    import src.agents.tax_agent as _tax
    monkeypatch.setattr(_tax, "get_tax_rate", _fake_get_tax_rate, raising=True)
    return calls


def _reload_rfq(rid: str) -> dict:
    """Reload an RFQ through the dashboard API (SQLite-backed)."""
    from src.api.dashboard import load_rfqs
    return load_rfqs().get(rid, {})


class TestTaxLookupPersistence:
    def test_lookup_persists_tax_rate_and_validated(
        self, auth_client, seed_rfq, _mock_tax_agent,
    ):
        rid = seed_rfq
        resp = auth_client.post(
            f"/api/rfq/{rid}/lookup-tax-rate",
            json={"address": "123 Main St, Los Angeles, CA 90049",
                  "force_live": True},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert body["rate"] == 9.75
        assert "Los Angeles" in body.get("jurisdiction", "")

        r = _reload_rfq(rid)
        assert r.get("tax_rate") == 9.75
        assert r.get("tax_validated") is True
        assert r.get("tax_source") == "cdtfa_api"

    def test_force_live_flag_propagates_to_agent(
        self, auth_client, seed_rfq, _mock_tax_agent,
    ):
        rid = seed_rfq
        auth_client.post(
            f"/api/rfq/{rid}/lookup-tax-rate",
            json={"address": "500 Pine Ave, Sacramento, CA 95814",
                  "force_live": True},
        )
        assert _mock_tax_agent["force_live_seen"] is True

    def test_only_one_rfq_lookup_tax_rate_route_registered(self, app):
        """Guard against accidentally reintroducing the duplicate route."""
        target = "/api/rfq/<rid>/lookup-tax-rate"
        matches = [r for r in app.url_map.iter_rules() if str(r) == target]
        assert len(matches) == 1, (
            f"Expected exactly one {target} route, found {len(matches)}: "
            f"{[str(m) for m in matches]}"
        )


class TestDeliveryLocationPersists:
    def test_update_field_persists_delivery_location(
        self, auth_client, seed_rfq,
    ):
        rid = seed_rfq
        new_addr = "456 Oak Ave, San Diego, CA 92101"
        resp = auth_client.post(
            f"/api/rfq/{rid}/update-field",
            json={"delivery_location": new_addr},
        )
        assert resp.status_code == 200
        assert resp.get_json().get("ok") is True

        r = _reload_rfq(rid)
        assert r.get("delivery_location") == new_addr


class TestTaxLookupJsTargetsRealDom:
    """Guard against regressing the DOM-ID bug in rfqTaxLookup()."""

    def test_rfq_tax_lookup_writes_to_tax_rate_element(self):
        tpl_path = os.path.join("src", "templates", "rfq_detail.html")
        with open(tpl_path, encoding="utf-8") as f:
            html = f.read()

        # Locate the rfqTaxLookup function body.
        start = html.find("function rfqTaxLookup(")
        assert start != -1, "rfqTaxLookup() not found in template"
        end = html.find("\nfunction ", start + 1)
        body = html[start:end if end != -1 else start + 2000]

        # Must target the real element id — not the old stale IDs.
        assert "getElementById('tax-rate')" in body, (
            "rfqTaxLookup must write to #tax-rate (the real input)"
        )
        assert "tax-rate-display" not in body, (
            "stale #tax-rate-display reference reintroduced"
        )
        assert "tax-rate-input" not in body, (
            "stale #tax-rate-input reference reintroduced"
        )
        assert "recalc" in body and "triggerAutosave" in body, (
            "rfqTaxLookup must recalc + triggerAutosave after rate update"
        )

    def test_pc_save_ship_to_updates_cached_tax_rate(self):
        """PC's saveShipTo() must sync cachedTaxRate after live lookup —
        otherwise the display updates but the math uses the stale rate."""
        tpl_path = os.path.join("src", "templates", "pc_detail.html")
        with open(tpl_path, encoding="utf-8") as f:
            html = f.read()

        start = html.find("function saveShipTo(")
        assert start != -1, "saveShipTo() not found in template"
        end = html.find("\n    function ", start + 1)
        body = html[start:end if end != -1 else start + 3000]

        assert "cachedTaxRate = d.rate" in body, (
            "saveShipTo must sync cachedTaxRate after tax lookup"
        )
        # Must not reference a non-existent #taxRate input.
        assert "getElementById('taxRate')" not in body, (
            "#taxRate input doesn't exist on PC — use cachedTaxRate instead"
        )
