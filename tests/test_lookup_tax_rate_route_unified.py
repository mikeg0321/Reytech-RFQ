"""Bundle-1 PR-1e: `/api/rfq/<rid>/lookup-tax-rate` now calls
`tax_resolver.resolve_tax`. Closes audit Y end-to-end.

Before this PR: the route had a private regex parser that could
disagree with `quote_generator`'s parallel facility-first path.
Now both callers land on the same pipeline.

These tests pin:
  - Route response shape preserved for existing UI consumers
  - `tax_facility_code` persists on the record
  - `confidence` is derived from validated + facility_code
  - `tax_validated` is True only on trusted sources (no more
    green checkmarks on 7.25% fallback)
"""
from __future__ import annotations

import json
import os

import pytest
from unittest.mock import patch


def _seed(temp_data_dir, sample_rfq, **overrides):
    rfq = dict(sample_rfq)
    rfq.update(overrides)
    path = os.path.join(temp_data_dir, "rfqs.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({rfq["id"]: rfq}, f)
    return rfq["id"]


class TestRouteCallsResolveTax:
    """The route must call `tax_resolver.resolve_tax`. Not the
    legacy inline regex + `get_tax_rate` chain."""

    def test_route_delegates_to_resolve_tax(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(
            temp_data_dir, sample_rfq,
            delivery_location=(
                "CA State Prison Sacramento, 100 Prison Road, "
                "Folsom CA 95671"
            ),
        )
        with patch("src.core.tax_resolver.resolve_tax") as mocked:
            mocked.return_value = {
                "ok": True, "rate": 0.0775,
                "jurisdiction": "SACRAMENTO", "city": "Represa",
                "county": "Sacramento",
                "source": "cdtfa_api",
                "facility_code": "CSP-SAC",
                "resolve_reason": "facility_registry:exact",
                "validated": True,
            }
            resp = client.post(
                f"/api/rfq/{rid}/lookup-tax-rate",
                json={},
            )
            assert resp.status_code == 200
            assert mocked.called, "route must invoke resolve_tax"
            # Called with the delivery_location from the record
            args, kwargs = mocked.call_args
            assert "CA State Prison Sacramento" in (args[0] if args else "")


class TestResponseShapePreserved:
    """The UI that consumes this route hasn't changed. Response
    fields must still be present and correctly typed."""

    def test_response_has_all_legacy_fields(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(
            temp_data_dir, sample_rfq,
            delivery_location="CSP-SAC",
        )
        with patch("src.core.tax_resolver.resolve_tax") as mocked:
            mocked.return_value = {
                "ok": True, "rate": 0.0775,
                "jurisdiction": "SACRAMENTO", "city": "Represa",
                "county": "Sacramento",
                "source": "cdtfa_api",
                "facility_code": "CSP-SAC",
                "resolve_reason": "facility_registry:exact",
                "validated": True,
            }
            resp = client.post(
                f"/api/rfq/{rid}/lookup-tax-rate",
                json={},
            )
            payload = resp.get_json()
        for key in ("ok", "rate", "jurisdiction", "city", "county",
                    "confidence", "source"):
            assert key in payload, f"missing legacy key {key!r}"
        # And new PR-1e additions
        for key in ("facility_code", "resolve_reason"):
            assert key in payload, f"missing PR-1e key {key!r}"
        # rate returned as percentage (matches UI contract)
        assert payload["rate"] == 7.75


class TestConfidenceDerived:
    """UI badge logic relied on `confidence=High/Medium/Low`. PR-1e
    derives this from validated + facility_code so it stays
    meaningful without requiring resolve_tax to track confidence."""

    def test_validated_plus_facility_is_high(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(temp_data_dir, sample_rfq, delivery_location="CSP-SAC")
        with patch("src.core.tax_resolver.resolve_tax") as mocked:
            mocked.return_value = {
                "ok": True, "rate": 0.0775, "jurisdiction": "SACRAMENTO",
                "city": "", "county": "", "source": "cdtfa_api",
                "facility_code": "CSP-SAC", "resolve_reason": "x",
                "validated": True,
            }
            resp = client.post(
                f"/api/rfq/{rid}/lookup-tax-rate", json={},
            )
        assert resp.get_json()["confidence"] == "High"

    def test_validated_no_facility_is_medium(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(
            temp_data_dir, sample_rfq,
            delivery_location="123 Main St, Oakland, CA 94607",
        )
        with patch("src.core.tax_resolver.resolve_tax") as mocked:
            mocked.return_value = {
                "ok": True, "rate": 0.0975, "jurisdiction": "OAKLAND",
                "city": "Oakland", "county": "Alameda",
                "source": "cdtfa_api",
                "facility_code": "", "resolve_reason": "address_parse",
                "validated": True,
            }
            resp = client.post(
                f"/api/rfq/{rid}/lookup-tax-rate", json={},
            )
        assert resp.get_json()["confidence"] == "Medium"

    def test_fallback_is_low(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(
            temp_data_dir, sample_rfq,
            delivery_location="Unknown",
        )
        with patch("src.core.tax_resolver.resolve_tax") as mocked:
            mocked.return_value = {
                "ok": True, "rate": 0.0725, "jurisdiction": "CALIFORNIA (DEFAULT)",
                "city": "", "county": "", "source": "default",
                "facility_code": "", "resolve_reason": "address_parse",
                "validated": False,
            }
            resp = client.post(
                f"/api/rfq/{rid}/lookup-tax-rate", json={},
            )
        assert resp.get_json()["confidence"] == "Low"


class TestValidatedFlagPersists:
    """Critical UI contract: `tax_validated` on the saved record
    drives the green-check badge. PR-1e plumbs `resolve_tax.validated`
    through so the badge means "real CDTFA hit", not "we got something
    back" like the old `tax_validated=True` unconditional set did."""

    def test_tax_validated_true_on_cdtfa_api(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(temp_data_dir, sample_rfq, delivery_location="CSP-SAC")
        with patch("src.core.tax_resolver.resolve_tax") as mocked:
            mocked.return_value = {
                "ok": True, "rate": 0.0775, "jurisdiction": "SACRAMENTO",
                "city": "", "county": "", "source": "cdtfa_api",
                "facility_code": "CSP-SAC", "resolve_reason": "x",
                "validated": True,
            }
            resp = client.post(
                f"/api/rfq/{rid}/lookup-tax-rate", json={},
            )
        assert resp.status_code == 200
        # Read back the saved record via the data layer (not raw
        # rfqs.json — records persist to SQLite in the test env)
        from src.api.data_layer import load_rfqs
        saved = load_rfqs()
        assert saved[rid]["tax_validated"] is True
        assert saved[rid]["tax_facility_code"] == "CSP-SAC"

    def test_tax_validated_false_on_default(
        self, client, temp_data_dir, sample_rfq
    ):
        """The critical fix: `default` source must NOT set
        tax_validated=True. Before PR-1e the legacy route
        unconditionally set it to True on any non-error result,
        so the green-check badge showed for 7.25% fallbacks."""
        rid = _seed(
            temp_data_dir, sample_rfq, delivery_location="Unknown",
        )
        with patch("src.core.tax_resolver.resolve_tax") as mocked:
            mocked.return_value = {
                "ok": True, "rate": 0.0725,
                "jurisdiction": "CALIFORNIA (DEFAULT)",
                "city": "", "county": "", "source": "default",
                "facility_code": "", "resolve_reason": "address_parse",
                "validated": False,
            }
            resp = client.post(
                f"/api/rfq/{rid}/lookup-tax-rate", json={},
            )
        assert resp.status_code == 200
        from src.api.data_layer import load_rfqs
        saved = load_rfqs()
        assert saved[rid]["tax_validated"] is False, (
            "tax_validated must be False on source='default' — "
            "was always-True in the pre-PR-1e route, which shipped "
            "green-check badges on 7.25% fallbacks"
        )


class TestForceLivePassesThrough:
    """The `?force_live=1` knob on the route must reach
    `resolve_tax(force_live=True)` so ops can skip the cache."""

    def test_force_live_propagates(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(temp_data_dir, sample_rfq, delivery_location="CSP-SAC")
        with patch("src.core.tax_resolver.resolve_tax") as mocked:
            mocked.return_value = {
                "ok": True, "rate": 0.0775, "jurisdiction": "",
                "city": "", "county": "", "source": "cdtfa_api",
                "facility_code": "CSP-SAC", "resolve_reason": "x",
                "validated": True,
            }
            resp = client.post(
                f"/api/rfq/{rid}/lookup-tax-rate",
                json={"force_live": True},
            )
        assert resp.status_code == 200
        args, kwargs = mocked.call_args
        # Second arg (or kwarg) is force_live=True
        assert kwargs.get("force_live") is True, (
            f"force_live didn't propagate; kwargs={kwargs}"
        )
