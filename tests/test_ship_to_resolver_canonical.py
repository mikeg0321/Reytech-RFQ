"""Migration test — ship_to_resolver consumes canonical via facade.

The 2026-04-25 follow-up to PR #517 migrates `ship_to_resolver.lookup_buyer_ship_to`
off `institution_resolver.get_ship_to_address` and onto the new
`quote_contract.ship_to_for_text` facade. This test pins the migration so
nobody re-introduces the direct institution_resolver import — the
architectural-ratchet test only enforces the rule for renderer paths
(src/forms, src/agents/packet*, etc.); ship_to_resolver lives outside those
paths, so we need a targeted import-purity check here.
"""
from __future__ import annotations

from src.core import ship_to_resolver
from src.core.quote_contract import ship_to_for_text


def test_facade_returns_canonical_address_for_csp_sac():
    """The Audit W canonical address must come back through the facade."""
    out = ship_to_for_text("CA State Prison Sacramento")
    assert out == "100 Prison Road, Represa, CA 95671", out


def test_facade_returns_canonical_address_for_barstow():
    """The 2026-04-24 Barstow incident facility resolves through the facade."""
    out = ship_to_for_text("Veterans Home of California - Barstow")
    assert "100 E Veterans Pkwy" in out
    assert "Barstow, CA 92311" in out


def test_facade_returns_empty_on_no_match():
    """Empty / unknown input must return empty string (preserves the
    caller's `if auto:` truthiness check in ship_to_resolver)."""
    assert ship_to_for_text("") == ""
    assert ship_to_for_text("totally unknown facility") == ""


def test_facade_returns_empty_on_ambiguous_text():
    """Bare 'Folsom' is ambiguous between CSP-SAC and FSP. The facade
    must NOT silently guess — return empty so the caller can fall through."""
    assert ship_to_for_text("Folsom") == ""


def test_lookup_buyer_ship_to_uses_canonical_facade():
    """End-to-end: passing an institution name to lookup_buyer_ship_to
    (with no DB / no CRM loader) must resolve through the canonical
    facade to a canonical address with `source="canonical_facility_registry"`."""
    out = ship_to_resolver.lookup_buyer_ship_to(
        institution="CA State Prison Sacramento"
    )
    assert out["ship_to"] == "100 Prison Road, Represa, CA 95671"
    assert out["source"] == "canonical_facility_registry"
    assert out["institution"] == "CA State Prison Sacramento"


def test_ship_to_resolver_does_not_import_institution_resolver_directly():
    """The migration's structural assertion: ship_to_resolver.py source
    must NOT contain the legacy `from src.core.institution_resolver
    import get_ship_to_address` line. Catches accidental re-introduction
    in a future PR."""
    import inspect
    src = inspect.getsource(ship_to_resolver)
    assert "from src.core.institution_resolver import get_ship_to_address" \
        not in src, (
            "ship_to_resolver.py is importing institution_resolver again — "
            "consume canonical via `quote_contract.ship_to_for_text` instead."
        )


def test_facade_address_format_matches_legacy_for_smoke():
    """During the cutover, the facade output should be SHAPE-compatible
    with the old institution_resolver string (single comma-separated
    line). If a future facade refactor changes shape (e.g. tuple of
    lines), this test fires so the caller in ship_to_resolver gets
    updated in the same PR."""
    out = ship_to_for_text("CA State Prison Sacramento")
    assert isinstance(out, str)
    # Two-line canonical address joined with ", " — exactly one comma
    # at the address_line1/line2 boundary plus one inside the city
    # line. Catches accidental tuple-return regressions.
    assert out.count(",") >= 2


def test_institution_resolver_no_longer_carries_facility_addresses():
    """Absence guard — Plan §4.2 / S2 follow-up (2026-04-27).

    `_FACILITY_ADDRESSES` was a parallel-universe dict on
    institution_resolver that duplicated addresses canonically owned by
    `facility_registry.FacilityRecord`. It was deleted because zero
    external callers consumed `get_ship_to_address` (the only reader)
    after `ship_to_resolver` migrated to `quote_contract.ship_to_for_text`.

    If a future PR re-introduces either name on institution_resolver,
    this test fires so the parallel dict can't grow back. New facility
    addresses MUST be added to `facility_registry._SEED` instead.
    """
    from src.core import institution_resolver
    assert not hasattr(institution_resolver, "_FACILITY_ADDRESSES"), (
        "institution_resolver._FACILITY_ADDRESSES is back. Add new facility "
        "addresses to facility_registry._SEED — there is one canonical source."
    )
    assert not hasattr(institution_resolver, "get_ship_to_address"), (
        "institution_resolver.get_ship_to_address is back. Use "
        "quote_contract.ship_to_for_text(text) instead — it resolves "
        "through the canonical facility_registry."
    )
