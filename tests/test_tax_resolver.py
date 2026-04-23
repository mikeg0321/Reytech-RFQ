"""Bundle-1 PR-1c: unified `resolve_tax()`.

Audit item Y (2026-04-22): the RFQ detail page's
`/api/rfq/<rid>/lookup-tax-rate` endpoint and the quote generator's
inline tax lookup diverged on the same record — UI showed 7.25%
stale, quote PDF used 7.75%. This PR introduces a single entry
point that both callers use.

These tests pin:
  - Normalized response shape (the keys every caller can rely on)
  - Facility-first priority (canonical zip always wins)
  - Address-parse fallback works on facility-led inputs (audit X)
  - Empty / crashed / default-rate branches all return a dict, never raise
"""
from __future__ import annotations

import pytest
from unittest.mock import patch

from src.core.tax_resolver import resolve_tax


# ── Response shape contract ───────────────────────────────────────

class TestNormalizedShape:
    def test_empty_input_returns_empty_response(self):
        r = resolve_tax("")
        assert r["ok"] is False
        assert r["rate"] is None
        assert r["validated"] is False
        assert r["resolve_reason"] == "empty_input"
        # Every caller-contract key must be present even on empty
        for key in ("ok", "rate", "jurisdiction", "city", "county",
                    "source", "facility_code", "resolve_reason",
                    "validated"):
            assert key in r, f"missing key {key!r} on empty response"

    def test_all_callers_get_same_dict_shape(self):
        """Regression guard: if someone adds a key to ONE branch
        but forgets another, future callers that destructure the
        dict will break silently. Every branch must emit the same
        key set."""
        # Mock the underlying tax_agent so the test runs offline
        with patch("src.agents.tax_agent.get_tax_rate") as mocked:
            mocked.return_value = {
                "rate": 0.0875, "rate_pct": "8.750%",
                "jurisdiction": "SACRAMENTO", "city": "Sacramento",
                "county": "Sacramento", "confidence": "High",
                "source": "cdtfa_api",
                "formatted_address": "100 Prison Road, Represa, CA 95671",
            }
            r_fac = resolve_tax(
                "CA State Prison Sacramento, 100 Prison Road, Folsom CA 95671"
            )
            r_addr = resolve_tax("123 Main St, Oakland, CA 94607")
            r_empty = resolve_tax("")

        for r in (r_fac, r_addr, r_empty):
            keys = set(r.keys())
            expected = {
                "ok", "rate", "jurisdiction", "city", "county",
                "source", "facility_code", "resolve_reason", "validated",
            }
            assert keys == expected, (
                f"dict keys mismatch: got {keys}, expected {expected}"
            )


# ── Priority: facility registry wins ──────────────────────────────

class TestFacilityPriority:
    """Audit Y's core fix: when the input resolves to a canonical
    facility, use THAT facility's zip for CDTFA — not whatever zip
    the regex might pluck out of the raw string."""

    def test_csp_sac_hits_canonical_zip(self):
        """Even though the buyer text might not include a zip,
        canonical resolution provides 95671 → CDTFA."""
        with patch("src.agents.tax_agent.get_tax_rate") as mocked:
            mocked.return_value = {
                "rate": 0.0775, "rate_pct": "7.750%",
                "jurisdiction": "SACRAMENTO", "city": "Represa",
                "county": "Sacramento", "source": "cdtfa_api",
            }
            r = resolve_tax("CA State Prison Sacramento")
            assert r["ok"] is True
            assert r["facility_code"] == "CSP-SAC"
            assert r["resolve_reason"].startswith("facility_registry:")
            # Confirm the call sent the CANONICAL zip, not whatever
            # the regex might have parsed
            called_kwargs = mocked.call_args.kwargs
            assert called_kwargs["zip_code"] == "95671"
            # And the CANONICAL street — the audit-W-fix 100 Prison Road
            assert called_kwargs["street"] == "100 Prison Road"

    def test_wsp_facility_led_uses_facility_zip(self):
        """Audit X + Y crossover: WSP facility-led delivery string
        must route through facility_registry → canonical zip 93280,
        not parse + guess."""
        with patch("src.agents.tax_agent.get_tax_rate") as mocked:
            mocked.return_value = {
                "rate": 0.0825, "rate_pct": "8.250%",
                "jurisdiction": "WASCO", "city": "Wasco",
                "county": "Kern", "source": "cdtfa_api",
            }
            r = resolve_tax(
                "WSP - Wasco State Prison, 701 Scofield Avenue, Wasco, CA 93280"
            )
            assert r["ok"] is True
            assert r["facility_code"] == "WSP"
            assert mocked.call_args.kwargs["zip_code"] == "93280"


# ── Fallback: address parse ──────────────────────────────────────

class TestAddressParseFallback:
    """When the input doesn't match any canonical facility, the
    resolver falls through to parsing the raw string. Must handle
    the facility-led / comma-optional formats that PR #463 (audit X)
    fixed for the route."""

    def test_unknown_institution_falls_back_to_parser(self):
        """Random custom delivery address → no facility match →
        address parser picks up the zip."""
        with patch("src.agents.tax_agent.get_tax_rate") as mocked:
            mocked.return_value = {
                "rate": 0.0975, "rate_pct": "9.750%",
                "jurisdiction": "OAKLAND", "city": "Oakland",
                "county": "Alameda", "source": "cdtfa_api",
            }
            r = resolve_tax("Warehouse 12, 500 Example Blvd, Oakland, CA 94607")
            assert r["ok"] is True
            assert r["facility_code"] == ""
            assert r["resolve_reason"] == "address_parse"
            # Parser should have extracted zip=94607 from the string
            assert mocked.call_args.kwargs["zip_code"] == "94607"


# ── Validated flag ───────────────────────────────────────────────

class TestValidatedFlag:
    """`validated=True` iff the source is trustworthy (cdtfa_api /
    cache / persisted_cache). Fallbacks and defaults read False —
    the UI uses this to show "7.25% (fallback — verify)" instead
    of a green confirmed badge."""

    def test_cdtfa_api_is_validated(self):
        with patch("src.agents.tax_agent.get_tax_rate") as mocked:
            mocked.return_value = {
                "rate": 0.0775, "source": "cdtfa_api",
                "jurisdiction": "SACRAMENTO", "city": "", "county": "",
            }
            r = resolve_tax("CSP-SAC")
        assert r["validated"] is True

    def test_default_is_not_validated(self):
        """`source='default'` → `validated=False`."""
        with patch("src.agents.tax_agent.get_tax_rate") as mocked:
            mocked.return_value = {
                "rate": 0.0725, "source": "default",
                "jurisdiction": "CALIFORNIA (DEFAULT)", "city": "", "county": "",
            }
            r = resolve_tax("Unknown place with no zip")
        assert r["validated"] is False

    def test_fallback_is_not_validated(self):
        with patch("src.agents.tax_agent.get_tax_rate") as mocked:
            mocked.return_value = {
                "rate": 0.0725, "source": "fallback",
                "jurisdiction": "SOMEWHERE", "city": "", "county": "",
            }
            r = resolve_tax("123 Test St, Anywhere, CA 90210")
        assert r["validated"] is False


# ── Crash safety ─────────────────────────────────────────────────

class TestCrashSafety:
    def test_tax_agent_crash_returns_dict_not_raise(self):
        """If the tax agent itself crashes, resolve_tax must
        degrade to `ok:False` + reason, NOT propagate the
        exception. A quote generation flow must never abort
        because CDTFA blew up."""
        with patch("src.agents.tax_agent.get_tax_rate") as mocked:
            mocked.side_effect = RuntimeError("CDTFA exploded")
            r = resolve_tax("123 Test St, City, CA 90210")
        assert r["ok"] is False
        assert r["resolve_reason"] == "lookup_crashed"
