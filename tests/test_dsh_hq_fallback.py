"""PR-AT — DSH HQ facility fallback + atascadero alias.

The DSH 25CB021 end-to-end test (rfq_efbdef4a) surfaced a substrate
gap: facility_registry has all 5 DSH hospitals (Atascadero, Patton,
Coalinga, Metropolitan, Napa) but no agency-level fallback. The
buyer's RFQ carried agency="DSH — STATE HOSPITALS" with no specific
facility text, so `resolve()` returned None, ship_to stayed "CA"
(2 chars), and PR-AI auto-tax was skipped.

PR-AT adds:
  1. `DSH-HQ` FacilityRecord (Sacramento, 95814) with agency-only
     aliases — handles the unspecified-facility case.
  2. Bare `"atascadero"` alias for DSH-Atascadero — handles buyer
     PDF text like "DEPT. OF STATE HOSPITALS - ATASCADERO" that
     was previously unresolvable.

Tests pin:
  1. Agency-only text "dsh — state hospitals" resolves to DSH-HQ.
  2. Agency-only short form "DSH" resolves to DSH-HQ.
  3. Specific facility name still wins (DSH-Atascadero, not HQ).
  4. Bare "atascadero" now resolves to DSH-Atascadero.
  5. DSH-HQ aliases do NOT shadow specific facilities (no regression
     on "patton state hospital" → DSH-Patton).
  6. The 5 specific facilities each still resolve correctly.
"""
from __future__ import annotations


def test_dsh_em_dash_resolves_to_hq():
    """'DSH — STATE HOSPITALS' (em-dash, from auto-classifier) → HQ."""
    from src.core.facility_registry import resolve

    fac = resolve("DSH — STATE HOSPITALS")
    assert fac is not None
    assert fac.code == "DSH-HQ"
    assert "Sacramento" in fac.address_line2


def test_dsh_hyphen_resolves_to_hq():
    """'DSH - STATE HOSPITALS' (hyphen variant) → HQ."""
    from src.core.facility_registry import resolve

    fac = resolve("DSH - STATE HOSPITALS")
    assert fac is not None
    assert fac.code == "DSH-HQ"


def test_bare_dsh_resolves_to_hq():
    """'DSH' alone → HQ. No more None-fallback for agency-only text."""
    from src.core.facility_registry import resolve

    fac = resolve("DSH")
    assert fac is not None
    assert fac.code == "DSH-HQ"


def test_department_of_state_hospitals_resolves_to_hq():
    """Long-form agency name → HQ."""
    from src.core.facility_registry import resolve

    fac = resolve("Department of State Hospitals")
    assert fac is not None
    assert fac.code == "DSH-HQ"


def test_atascadero_state_hospital_still_resolves_to_atascadero():
    """Specific facility name wins — HQ alias doesn't shadow."""
    from src.core.facility_registry import resolve

    fac = resolve("Atascadero State Hospital")
    assert fac is not None
    assert fac.code == "DSH-Atascadero"


def test_bare_atascadero_now_resolves_to_atascadero():
    """PR-AT addition: bare 'atascadero' → DSH-Atascadero."""
    from src.core.facility_registry import resolve

    fac = resolve("Atascadero")
    assert fac is not None
    assert fac.code == "DSH-Atascadero"


def test_dept_of_state_hospitals_atascadero_resolves_to_atascadero():
    """The buyer-PDF pattern 'DEPT. OF STATE HOSPITALS - ATASCADERO'
    contains BOTH the agency-only phrase ('dept of state hospitals')
    and the bare facility token ('atascadero'). Specific wins."""
    from src.core.facility_registry import resolve

    fac = resolve("DEPT. OF STATE HOSPITALS - ATASCADERO")
    assert fac is not None
    assert fac.code == "DSH-Atascadero"


def test_patton_state_hospital_still_resolves():
    """No regression: 'Patton State Hospital' → DSH-Patton."""
    from src.core.facility_registry import resolve

    fac = resolve("Patton State Hospital")
    assert fac is not None
    assert fac.code == "DSH-Patton"


def test_coalinga_state_hospital_still_resolves():
    """No regression: 'Coalinga State Hospital' → DSH-Coalinga."""
    from src.core.facility_registry import resolve

    fac = resolve("Coalinga State Hospital")
    assert fac is not None
    assert fac.code == "DSH-Coalinga"


def test_metropolitan_state_hospital_still_resolves():
    """No regression: 'Metropolitan State Hospital' → DSH-Metropolitan."""
    from src.core.facility_registry import resolve

    fac = resolve("Metropolitan State Hospital")
    assert fac is not None
    assert fac.code == "DSH-Metropolitan"


def test_napa_state_hospital_still_resolves():
    """No regression: 'Napa State Hospital' → DSH-Napa."""
    from src.core.facility_registry import resolve

    fac = resolve("Napa State Hospital")
    assert fac is not None
    assert fac.code == "DSH-Napa"


def test_dsh_hq_canonical_name_marks_unspecified():
    """The HQ canonical_name must signal 'facility unspecified' so
    operators looking at the detail page know to override the
    ship-to before generating a real package."""
    from src.core.facility_registry import get

    fac = get("DSH-HQ")
    assert fac is not None
    assert "unspecified" in fac.canonical_name.lower()


def test_dsh_hq_pure_agency_resolves_to_hq():
    """Pure agency-level text → HQ. Must not pick a random
    specific facility just because the text mentions 'state hospitals'."""
    from src.core.facility_registry import resolve

    fac = resolve("dsh - state hospitals headquarters")
    assert fac is not None
    assert fac.code == "DSH-HQ"


def test_ambiguous_dsh_plus_specific_returns_none():
    """When text contains BOTH an agency phrase AND a specific
    facility token, the resolver correctly refuses to guess —
    returns None. This is the substrate's safety: ambiguity must
    never silently resolve to a single guess (per registry
    docstring). Caller (heal v2 / ingest) falls through to
    needs_operator_input."""
    from src.core.facility_registry import resolve

    # "DSH" matches DSH-HQ alias AND "Patton State Hospital" matches
    # DSH-Patton alias → 2 candidates → None (correct safety)
    fac = resolve("Patton State Hospital - DSH")
    assert fac is None
