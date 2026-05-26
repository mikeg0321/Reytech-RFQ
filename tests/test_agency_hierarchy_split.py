"""Pin: parent-agency hierarchy lives in its own module + re-exports work.

PR #13 (audit Item 4 / 2026-05-26): extracted PARENT_AGENCIES,
OVERLAP_PATTERNS, and `_detect_parent` from agency_config.py to a
dedicated `src/core/agency_hierarchy.py`. agency_config.py re-imports
for back-compat so every existing caller works unchanged.

These tests pin:
  1. The new module exists with the expected public surface.
  2. The re-exports in agency_config.py resolve to the SAME objects
     (not copies) — so any future mutation, identity check, or
     `from src.core.agency_config import PARENT_AGENCIES` keeps the
     legacy callsite path working.
  3. The detection logic is unchanged (smoke through `_detect_parent`).
"""
from __future__ import annotations


def test_agency_hierarchy_module_exists():
    """The extracted module should be importable on its own."""
    import src.core.agency_hierarchy as ah
    assert hasattr(ah, "PARENT_AGENCIES")
    assert hasattr(ah, "OVERLAP_PATTERNS")
    assert hasattr(ah, "_detect_parent")
    assert hasattr(ah, "detect_parent")  # public alias


def test_agency_config_reexports_are_same_objects():
    """Identity equivalence: imports via agency_config and via
    agency_hierarchy must resolve to the same in-memory objects.
    Catches a refactor that accidentally copies the data."""
    from src.core.agency_config import (
        PARENT_AGENCIES as A_CFG,
        OVERLAP_PATTERNS as O_CFG,
        _detect_parent as DP_CFG,
    )
    from src.core.agency_hierarchy import (
        PARENT_AGENCIES as A_HIER,
        OVERLAP_PATTERNS as O_HIER,
        _detect_parent as DP_HIER,
    )
    assert A_CFG is A_HIER
    assert O_CFG is O_HIER
    assert DP_CFG is DP_HIER


def test_parent_agencies_shape_unchanged():
    """The 5 expected parents are present with their required keys."""
    from src.core.agency_hierarchy import PARENT_AGENCIES
    expected_parents = {"CDCR", "DSH", "CALVET", "DGS", "CALFIRE"}
    assert set(PARENT_AGENCIES.keys()) == expected_parents
    for parent_id, info in PARENT_AGENCIES.items():
        assert "name" in info, f"{parent_id} missing 'name'"
        assert "strong_patterns" in info, f"{parent_id} missing 'strong_patterns'"
        assert "domains" in info, f"{parent_id} missing 'domains'"
        assert "children" in info, f"{parent_id} missing 'children'"


def test_detect_parent_email_domain_wins():
    """Real-world signal: a cchcs.ca.gov sender resolves to CDCR
    regardless of body text."""
    from src.core.agency_hierarchy import _detect_parent
    assert _detect_parent("any text", email_domain="cchcs.ca.gov") == "CDCR"
    assert _detect_parent("any text", email_domain="dsh.ca.gov") == "DSH"
    assert _detect_parent("any text", email_domain="calvet.ca.gov") == "CALVET"


def test_detect_parent_text_pattern_fallback():
    """Without an email domain, strong text patterns resolve the parent."""
    from src.core.agency_hierarchy import _detect_parent
    assert _detect_parent("CCHCS quote request") == "CDCR"
    assert _detect_parent("Department of State Hospitals") == "DSH"
    assert _detect_parent("CalFire procurement") == "CALFIRE"


def test_detect_parent_returns_none_for_no_match():
    from src.core.agency_hierarchy import _detect_parent
    assert _detect_parent("") is None
    assert _detect_parent("nothing matching here") is None


def test_overlap_patterns_contains_coalinga():
    """The whole reason this hierarchy exists: COALINGA is the
    PVSP (CCHCS) vs Coalinga State Hospital (DSH) tie-breaker case."""
    from src.core.agency_hierarchy import OVERLAP_PATTERNS
    assert "COALINGA" in OVERLAP_PATTERNS
