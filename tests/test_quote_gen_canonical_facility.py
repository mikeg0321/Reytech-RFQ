"""Regression: `quote_generator._lookup_facility` reads from canonical
`facility_registry`, not the local `FACILITY_DB` snapshot.

Incident `feedback_quoting_core_repeats_failing.md` (2026-04-24):
Mike's CalVet Barstow PC `f81c4e9b` / RFQ `8a1dcf77` rendered the
final Reytech Quote PDF with:
  - Ship-to: "CAL - Calipatria State Prison" (wrong)
  - Agency: "Dept. of Corrections and Rehabilitation" (wrong)

Root cause: `quote_generator.FACILITY_DB` was a duplicate of the
canonical registry with its own iteration order. When
`_lookup_facility("California Department of Veterans Affairs - Barstow
Division")` ran, the loop iterated CDCR codes first; the city-fallback
map's "BARSTOW" lookup competed with whatever the substring-match
loop found, and Calipatria won via a non-deterministic match path.

Fix: collapse `_lookup_facility` to delegate to
`facility_registry.resolve()`, which applies audit-W-safe priority
(exact alias → unique substring → unique zip → None).

These tests pin the canonical-source contract so the next per-symptom
patch can't bypass the registry again.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.forms.quote_generator import (
    _lookup_facility,
    _lookup_facility_by_zip,
    _registry_record_to_legacy_dict,
)


# ── Critical regression: f81c4e9b CalVet Barstow ─────────────────────


def test_calvet_barstow_resolves_to_correct_facility_not_calipatria():
    """The exact ship-to text from f81c4e9b that triggered the
    incident must resolve to CALVETHOME-BF, not CAL."""
    inputs = [
        "Veterans Home of California - Barstow",
        "California Department of Veterans Affairs - Barstow Division, Skilled Nursing Unit",
        "100 E Veterans Pkwy, Barstow, CA 92311",
        "Barstow Veterans Home",
    ]
    for src in inputs:
        result = _lookup_facility(src)
        assert result is not None, f"Failed to resolve: {src!r}"
        assert result["code"] == "CALVETHOME-BF", (
            f"{src!r} resolved to {result.get('code')!r} ({result.get('name')!r}) "
            f"— expected CALVETHOME-BF (Veterans Home of California - Barstow). "
            f"This is the f81c4e9b regression — quote_generator must read from "
            f"canonical facility_registry."
        )
        assert result["parent"] == "CalVet"
        assert result["address"] == ["100 E Veterans Pkwy", "Barstow, CA 92311"]


def test_calvet_barstow_does_not_match_calipatria():
    """Negative assertion: under no circumstances should a CalVet
    Barstow input resolve to CAL (Calipatria State Prison) — this is
    the literal symptom in the prod PDF."""
    for src in (
        "Veterans Home of California - Barstow",
        "Barstow Veterans Home",
        "100 E Veterans Pkwy, Barstow, CA 92311",
    ):
        result = _lookup_facility(src)
        assert result is None or result["code"] != "CAL", (
            f"{src!r} matched CAL (Calipatria) — this is the regression. "
            f"Got {result!r}"
        )
        if result is not None:
            assert "Calipatria" not in result["name"]
            assert "Calipatria" not in " ".join(result["address"])


def test_calipatria_input_still_resolves_correctly():
    """Sanity: a real Calipatria input must STILL resolve to CAL.
    The fix must not blow up the legitimate Calipatria path."""
    result = _lookup_facility("CAL - Calipatria State Prison")
    assert result is not None
    assert result["code"] == "CAL"
    assert "Calipatria" in result["name"]


# ── Audit W contract: CSP-SAC vs FSP shared zip ──────────────────────


def test_csp_sac_resolves_to_100_prison_road_not_300():
    """Audit W lock-in: CSP-SAC at 100 Prison Road, NOT 300."""
    result = _lookup_facility("CSP-SAC")
    assert result is not None
    assert result["code"] == "CSP-SAC"
    assert result["address"][0] == "100 Prison Road", (
        f"CSP-SAC ship-to drifted: {result['address']!r} — "
        f"should be 100 Prison Road (audit W fix)"
    )


def test_fsp_resolves_to_300_prison_road():
    """The other Folsom prison stays at 300."""
    result = _lookup_facility("FSP - Folsom State Prison")
    assert result is not None
    assert result["code"] == "FSP"
    assert result["address"][0] == "300 Prison Road"


# ── Cross-source consistency: lookup + tax_resolver agree ────────────


def test_lookup_facility_returns_same_record_as_facility_registry():
    """The legacy-dict shape must mirror the canonical registry record
    exactly — no field divergence."""
    from src.core.facility_registry import resolve

    for canonical_name in (
        "Veterans Home of California - Barstow",
        "CSP-SAC",
        "CIW - California Institution for Women",
    ):
        rec = resolve(canonical_name)
        legacy = _lookup_facility(canonical_name)
        assert rec is not None and legacy is not None
        # Round-trip via the adapter — should match what _lookup_facility returns
        adapted = _registry_record_to_legacy_dict(rec)
        assert legacy == adapted, (
            f"Lookup for {canonical_name!r} drifted from registry: "
            f"legacy={legacy!r} vs registry-adapted={adapted!r}"
        )


# ── Zip lookup also reads from canonical registry ────────────────────


def test_zip_lookup_finds_barstow_92311():
    """Zip 92311 → Barstow Veterans Home, no CDCR confusion."""
    result, codes = _lookup_facility_by_zip("Some address with zip 92311")
    assert result is not None
    assert result["code"] == "CALVETHOME-BF"
    assert "CALVETHOME-BF" in codes


def test_zip_lookup_returns_ambiguous_codes_for_shared_zip():
    """95671 has both FSP and CSP-SAC — codes list must reflect that
    so caller can disambiguate by name."""
    result, codes = _lookup_facility_by_zip("Address in zip 95671")
    assert result is not None
    assert len(codes) >= 2, (
        f"95671 should map to both FSP and CSP-SAC; got {codes!r}"
    )
    assert "CSP-SAC" in codes
    assert "FSP" in codes


def test_zip_lookup_returns_none_for_unknown_zip():
    result, codes = _lookup_facility_by_zip("Address with zip 99999")
    assert result is None
    assert codes == []


# ── Adapter shape contract ──────────────────────────────────────────


def test_legacy_dict_adapter_preserves_all_fields():
    """All downstream callers in quote_generator depend on these dict
    keys: name / parent / parent_full / address (list of 2 strings) /
    code / zip. Pin the shape so a future registry-record change
    can't silently break quote PDF rendering."""
    from src.core.facility_registry import get
    rec = get("CALVETHOME-BF")
    assert rec is not None
    legacy = _registry_record_to_legacy_dict(rec)
    assert set(legacy.keys()) >= {"name", "parent", "parent_full", "address", "code", "zip"}
    assert isinstance(legacy["address"], list) and len(legacy["address"]) == 2
    assert legacy["name"] == "Veterans Home of California - Barstow"
    assert legacy["parent"] == "CalVet"
    assert legacy["parent_full"] == "California Department of Veterans Affairs"
    assert legacy["code"] == "CALVETHOME-BF"
    assert legacy["zip"] == "92311"


def test_adapter_handles_none_input():
    assert _registry_record_to_legacy_dict(None) is None


# ── S2 closure: tombstone delete (DATA_ARCHITECTURE_MAP §7) ─────────────
#
# `quote_generator.FACILITY_DB`, `ZIP_TO_FACILITY`, and the dead
# `_lookup_facility_legacy` fallback (with its embedded `_CITY_MAP`)
# were deleted because:
#   1. `_lookup_facility_legacy` had zero callers (verified via grep).
#   2. `FACILITY_DB[171]` still encoded the audit-W ghost ("300 Prison
#      Road" for CSP-SAC) — anyone reading it directly would silently
#      regress the audit-W fix that `core/facility_registry.py` shipped.
#   3. `quote_contract.py:61-62` had an outstanding TODO documenting
#      this exact deletion, gated on "no renderer needs it" — confirmed.
# These tests fail loud if the dead code is re-introduced.


def test_facility_db_constant_is_gone():
    """Re-introducing this constant would resurrect the audit-W ghost."""
    import src.forms.quote_generator as qg
    assert not hasattr(qg, "FACILITY_DB"), (
        "FACILITY_DB was a duplicate of facility_registry.FACILITIES_BY_CODE "
        "with stale audit-W data ('300 Prison Road' for CSP-SAC). Do not "
        "re-introduce — `_registry_record_to_legacy_dict(facility_registry.get(code))` "
        "produces the same shape with correct data."
    )


def test_zip_to_facility_constant_is_gone():
    """ZIP_TO_FACILITY was built from FACILITY_DB and never read in the live
    path — `_lookup_facility_by_zip` delegates to facility_registry.all_facilities()
    instead. Removed in the same cleanup."""
    import src.forms.quote_generator as qg
    assert not hasattr(qg, "ZIP_TO_FACILITY"), (
        "ZIP_TO_FACILITY was dead code built from the FACILITY_DB tombstone. "
        "Use `_lookup_facility_by_zip(text)` (which delegates to facility_registry) "
        "instead."
    )


def test_lookup_facility_legacy_function_is_gone():
    """The legacy fallback returned audit-W-buggy addresses on its FACILITY_DB
    iteration path. Removed because zero call sites needed it; per
    `feedback_app_is_source_of_truth`, hard-fail-on-import-error beats
    silent-wrong-address-on-quote."""
    import src.forms.quote_generator as qg
    assert not hasattr(qg, "_lookup_facility_legacy"), (
        "_lookup_facility_legacy used the deleted FACILITY_DB and would "
        "regress the audit-W fix. If facility_registry import fails at boot, "
        "let the app crash — silent-wrong addresses on quotes are unacceptable."
    )
