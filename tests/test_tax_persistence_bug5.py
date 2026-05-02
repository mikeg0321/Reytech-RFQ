"""Regression tests for Bug 5 — tax persistence + KPI strip + green-check
gate. Mike screenshotted a CalVet RFQ where the FRESNO chip + ✅ green
badge appeared next to a 0% rate and the KPI Tax cell read $0.00 — a
silent revenue miss because the operator visually trusted the green.

Two distinct fixes pinned here:

  Bug-5a — recalc() now reads the tax rate from the live #tax-rate input
  (or falls back to the server-rendered value) instead of a Jinja-baked
  const. The const had only been refreshed on full page reload, so any
  in-page edit or post-FRESNO-lookup write to the input went into the
  KPI strip math invisible to the operator.

  Bug-5b — both the lookup-tax-rate route and the generation auto-
  validate path now require rate > 0 to flip tax_validated=True. A 0%
  CDTFA cache response previously survived as a "validated" jurisdiction
  and got the green ✅ treatment.

PR-bug5.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest


# ─────────────────────────────────────────────────────────────────────────
# Bug-5a — KPI tax cell reads live input, not Jinja const
# ─────────────────────────────────────────────────────────────────────────

class TestKpiReadsLiveTaxInput:

    def test_recalc_reads_live_tax_input(self):
        """Static-string assertion that recalc() resolves rfqTaxRate from
        the input element, not from a baked const. We don't run JS here —
        but if someone reverts the live-read pattern, this trips first."""
        with open("src/templates/rfq_detail.html", encoding="utf-8") as f:
            src = f.read()
        # Live-input read is in place
        assert "_kpiTaxInput=document.getElementById('tax-rate')" in src, (
            "recalc() must read tax_rate from the live input, not a "
            "Jinja-baked const (Bug-5a)."
        )
        assert "rfqTaxRate=_kpiTaxInput?(parseFloat(_kpiTaxInput.value)" in src
        # The pure-const fallback line is gone (it was: const rfqTaxRate=parseFloat(...)|0;)
        assert "const rfqTaxRate=parseFloat({{" not in src, (
            "Old const-only rfqTaxRate pattern still present; KPI strip "
            "will go stale after FRESNO lookup."
        )


# ─────────────────────────────────────────────────────────────────────────
# Bug-5b — green ✅ requires rate > 0
# ─────────────────────────────────────────────────────────────────────────

class TestValidatedRequiresNonZero:

    def test_template_green_check_requires_nonzero_rate(self):
        """The tax-validation badge must render ✅ only when rate > 0."""
        with open("src/templates/rfq_detail.html", encoding="utf-8") as f:
            src = f.read()
        # The conditional now requires (r.tax_rate|float) > 0
        assert (
            "r.get('tax_validated') and (r.get('tax_rate')|float) > 0"
            in src
        ), "Template must require rate>0 alongside tax_validated for ✅ (Bug-5b)."

    def test_lookup_tax_route_requires_nonzero_rate(self, auth_client, seed_rfq):
        """Hitting the route with a resolver that returns rate=0 must NOT
        persist tax_validated=True."""
        rid = seed_rfq
        # Patch resolve_tax → ok=True, rate=0 (zero-rate cache hit)
        zero_response = {
            "ok": True, "rate": 0, "validated": True,
            "source": "cdtfa_api", "jurisdiction": "FRESNO",
            "facility_code": "FRES",
        }
        with patch("src.core.tax_resolver.resolve_tax",
                   return_value=zero_response):
            r = auth_client.post(
                f"/api/rfq/{rid}/lookup-tax-rate",
                data=json.dumps({"address": "1 Main St, Fresno, CA 93706"}),
                content_type="application/json",
            )
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        # Reload through the loader the app uses
        from src.api.dashboard import load_rfqs
        rec = load_rfqs().get(rid, {})
        # Rate persisted (so we keep the resolver's response)
        assert rec.get("tax_rate") == 0
        # But tax_validated must NOT be True for a 0% rate
        assert rec.get("tax_validated") is False, (
            "tax_validated must be False when rate=0, even when the resolver "
            "claims validation — guards against green-check on $0 tax."
        )

    def test_lookup_tax_route_persists_when_rate_nonzero(self, auth_client,
                                                        seed_rfq):
        """Sanity — the happy path still persists validated=True."""
        rid = seed_rfq
        good_response = {
            "ok": True, "rate": 0.0775, "validated": True,
            "source": "cdtfa_api", "jurisdiction": "FRESNO",
            "facility_code": "FRES",
        }
        with patch("src.core.tax_resolver.resolve_tax",
                   return_value=good_response):
            r = auth_client.post(
                f"/api/rfq/{rid}/lookup-tax-rate",
                data=json.dumps({"address": "1 Main St, Fresno, CA 93706"}),
                content_type="application/json",
            )
        assert r.status_code == 200
        from src.api.dashboard import load_rfqs
        rec = load_rfqs().get(rid, {})
        assert rec.get("tax_rate") == 7.75
        assert rec.get("tax_validated") is True
        assert rec.get("tax_jurisdiction") == "FRESNO"
