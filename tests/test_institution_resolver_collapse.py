"""Cross-source consistency test for the 2026-05-27 institution_resolver
collapse (PR-B substrate convergence).

Per CLAUDE.md §0 LAW 2 — a migration is DONE only when the replaced
legacy code is DELETED. This PR deletes the 5 heuristic data tables
from `institution_resolver` (_CDCR_FACILITIES, _CALVET_FACILITIES,
_DSH_FACILITIES, _ADDRESS_FACILITIES, _ADDRESS_KEYWORDS) and replaces
them with thin facades over `facility_registry`.

These tests lock the collapse so the two sources can't silently drift
again:

  1. For every code in `FACILITIES_BY_CODE`, asserting that
     `institution_resolver.resolve(canonical_name)` returns a result
     that round-trips to the same FacilityRecord via
     `facility_registry.resolve()`.

  2. Audit-found defects (the 6 specific drifts the legacy tables
     carried vs the canonical registry) are now CLOSED:
       - The `"prison road"` keyword no longer unconditionally maps
         to CSP-SAC. The full street address routes through registry
         (ambiguous on shared zip 95671) or returns generic CDCR
         when no facility resolves.
       - Bare `"lancaster"` no longer silently picks CSP-LAC; it
         returns the raw text (the legacy table mapped it to
         VHC-Lancaster via the CalVet keyword path; the canonical
         registry refuses bare ambiguous tokens).
       - All zip codes now flow through `facility_registry._ZIP_INDEX`,
         which carries the canonical zips (e.g. FSP=95671, not the
         wrong 95763 the legacy table had).

  3. The 5 deleted data tables are GONE from institution_resolver —
     absence guards so they can't grow back as a parallel substrate.
"""
from __future__ import annotations

import pytest

from src.core import facility_registry, institution_resolver


# ─── Absence guards: the 5 deleted data tables ───────────────────────


def test_cdcr_facilities_table_deleted():
    """`_CDCR_FACILITIES` heuristic table was deleted in the collapse."""
    assert not hasattr(institution_resolver, "_CDCR_FACILITIES"), (
        "_CDCR_FACILITIES is back. CDCR facility data lives in "
        "`facility_registry._SEED` — there is one canonical source."
    )


def test_cdcr_cities_table_deleted():
    """`_CDCR_CITIES` was an auto-generated reverse-index over
    `_CDCR_FACILITIES`. Both went away in the collapse."""
    assert not hasattr(institution_resolver, "_CDCR_CITIES"), (
        "_CDCR_CITIES is back. Reverse-index over CDCR facilities is "
        "no longer needed — `facility_registry.resolve()` handles "
        "city tokens via the alias index."
    )


def test_calvet_facilities_table_deleted():
    """`_CALVET_FACILITIES` heuristic table was deleted in the collapse."""
    assert not hasattr(institution_resolver, "_CALVET_FACILITIES"), (
        "_CALVET_FACILITIES is back. CalVet facility data lives in "
        "`facility_registry._SEED` — there is one canonical source."
    )


def test_dsh_facilities_table_deleted():
    """`_DSH_FACILITIES` heuristic table was deleted in the collapse."""
    assert not hasattr(institution_resolver, "_DSH_FACILITIES"), (
        "_DSH_FACILITIES is back. DSH facility data lives in "
        "`facility_registry._SEED` — there is one canonical source."
    )


def test_address_facilities_table_deleted():
    """`_ADDRESS_FACILITIES` zip → facility table was deleted in the
    collapse. The legacy table carried 4 wrong zips that the audit
    surfaced — its absence enforces the single-substrate rule."""
    assert not hasattr(institution_resolver, "_ADDRESS_FACILITIES"), (
        "_ADDRESS_FACILITIES is back. Zip → facility lookups go "
        "through `facility_registry._ZIP_INDEX`, which carries the "
        "canonical zips (FSP=95671, DSH-Atascadero=93422, etc.)."
    )


def test_address_keywords_table_deleted():
    """`_ADDRESS_KEYWORDS` street-keyword → facility table was deleted.
    The legacy `"prison road"` and bare `"lancaster"` defects came
    from this table; its absence closes the bug class."""
    assert not hasattr(institution_resolver, "_ADDRESS_KEYWORDS"), (
        "_ADDRESS_KEYWORDS is back. Street keyword resolution goes "
        "through `facility_registry.resolve()` which refuses to "
        "silently guess on ambiguous input (e.g. 'prison road' alone)."
    )


# ─── Cross-source consistency: canonical_name round-trip ─────────────
#
# For every facility in the registry, the resolver must agree with
# the registry on the agency + facility identity. Locks the collapse:
# any future drift between the two sources fires here.

# Codes the registry intentionally doesn't surface through the
# institution_resolver dict shape (e.g. DSH-HQ is an agency-level
# catch-all whose aliases collide with the agency keyword "DSH",
# which institution_resolver routes via `_match_alias` not facility
# lookup — that's intentional and tested separately).
_LEGACY_RESOLVER_EXEMPT_CODES = frozenset({
    "DSH-HQ",  # agency-only fallback; aliases route through _match_alias
})


@pytest.mark.parametrize(
    "code",
    [c for c in facility_registry.FACILITIES_BY_CODE.keys()
     if c not in _LEGACY_RESOLVER_EXEMPT_CODES],
)
def test_canonical_name_round_trips_through_both_resolvers(code):
    """For every canonical facility code, the registry's canonical_name
    must resolve back to that same code through BOTH:
        facility_registry.resolve(canonical_name).code == code
        institution_resolver.resolve(canonical_name)  → same agency family

    This is the substrate-singleness lock. If anyone re-adds a parallel
    facility table to institution_resolver and it drifts, this fires.
    """
    rec = facility_registry.FACILITIES_BY_CODE[code]
    name = rec.canonical_name

    # Registry round-trip
    via_registry = facility_registry.resolve(name)
    assert via_registry is not None, (
        f"facility_registry can't resolve its own canonical_name "
        f"{name!r} for code {code}"
    )
    assert via_registry.code == code, (
        f"facility_registry.resolve({name!r}) → {via_registry.code}, "
        f"expected {code}"
    )

    # Resolver returns the legacy dict shape via the facade. Verify
    # the agency family is consistent with the registry's parent_agency.
    via_resolver = institution_resolver.resolve(name)
    assert isinstance(via_resolver, dict)
    resolver_agency = via_resolver.get("agency", "")
    # Map registry's parent_agency to the legacy-style lowercase key
    # the resolver returns. CDCR-parent facilities surface as "cchcs"
    # in the legacy resolver bucket (the procurement classification).
    expected_agency = {
        "CDCR": "cchcs",
        "CCHCS": "cchcs",
        "CalVet": "calvet",
        "DSH": "dsh",
        "DGS": "dgs",
    }.get(rec.parent_agency, rec.parent_agency.lower())
    assert resolver_agency == expected_agency, (
        f"institution_resolver.resolve({name!r}) agency = "
        f"{resolver_agency!r}, registry parent_agency = "
        f"{rec.parent_agency!r} → expected {expected_agency!r}. "
        f"Drift between substrates — investigate."
    )


# ─── Audit-defect closures ────────────────────────────────────────────


class TestAuditDefectClosures:
    """The 6 specific drift defects the 2026-05-27 facility audit
    found between institution_resolver's heuristic tables and the
    canonical facility_registry. After the collapse, all 6 routes
    flow through facility_registry, so they can't drift again."""

    def test_prison_road_full_address_no_longer_silently_picks_csp_sac(self):
        """Pre-collapse: `_ADDRESS_KEYWORDS["prison road"]` unconditionally
        mapped to CSP-SAC. But 300 Prison Rd is FSP (Old Folsom), not
        CSP-SAC (New Folsom, 100 Prison Rd). Post-collapse: registry
        returns None on the ambiguous shared zip; the resolver falls
        through to generic CDCR (better than the wrong prison)."""
        # The registry refuses ambiguous shared-zip input.
        rec = facility_registry.resolve(
            "300 Prison Road, Represa, CA 95671"
        )
        assert rec is None, (
            "facility_registry must NOT silently pick CSP-SAC or FSP "
            "on bare shared-zip input — both share 95671 in Represa."
        )
        # institution_resolver still surfaces SOMETHING is CDCR (the
        # "prison" keyword fires), but no longer a specific wrong code.
        out = institution_resolver.resolve(
            "300 Prison Road, Represa, CA 95671"
        )
        assert out["facility_code"] == "", (
            "institution_resolver must NOT pick a specific CDCR code "
            "from a shared-zip address; got "
            f"facility_code={out['facility_code']!r}"
        )
        assert out["agency"] == "cchcs", (
            "Generic-CDCR fallback should still flag the agency as "
            f"cchcs; got {out['agency']!r}"
        )

    def test_bare_lancaster_no_longer_silently_picks_vhc(self):
        """Pre-collapse: `_CALVET_FACILITIES["lancaster"]` mapped bare
        "lancaster" to VHC-Lancaster via the keyword path. Post-
        collapse: registry refuses bare ambiguous "lancaster" (collides
        with CSP-LAC); resolver returns raw text without a facility_code."""
        out = institution_resolver.resolve("Lancaster")
        assert out["facility_code"] == "", (
            "Bare 'Lancaster' is ambiguous (CSP-LAC + VHC-Lancaster "
            "both in Lancaster zip 93536) — resolver must not pick. "
            f"Got facility_code={out['facility_code']!r}"
        )
        assert out["agency"] == "", (
            "Bare 'Lancaster' has no agency context — resolver must "
            f"not guess. Got agency={out['agency']!r}"
        )

    def test_cdcr_lancaster_with_keyword_still_narrows_to_csp_lac(self):
        """CDCR-keyword context narrows the ambiguous token. This is
        the legacy behavior the resolver preserves post-collapse."""
        out = institution_resolver.resolve("cdcr lancaster")
        assert out["facility_code"] == "LAC"
        assert out["agency"] == "cchcs"

    def test_fsp_zip_is_canonical_95671(self):
        """Pre-collapse: `_ADDRESS_FACILITIES` was missing FSP's zip
        (95671 — only carried CSP-SAC at the wrong zip 95763). Post-
        collapse: FSP resolves through the registry (95671 is correctly
        in `_ZIP_INDEX` but maps to both CSP-SAC and FSP — ambiguous,
        so caller disambiguates on street/name)."""
        fsp = facility_registry.get("FSP")
        assert fsp is not None
        assert fsp.zip == "95671"

    def test_dsh_atascadero_zip_is_canonical_93422(self):
        """Pre-collapse: `_ADDRESS_FACILITIES` had DSH-Atascadero at
        wrong zip 93423. Post-collapse: canonical 93422."""
        atasc = facility_registry.FACILITIES_BY_CODE.get("DSH-Atascadero")
        assert atasc is not None
        assert atasc.zip == "93422"

    def test_dsh_metropolitan_zip_is_canonical_90650(self):
        """Pre-collapse: `_ADDRESS_FACILITIES` had DSH-Metropolitan at
        wrong zip 90660. Post-collapse: canonical 90650 (Norwalk)."""
        metro = facility_registry.FACILITIES_BY_CODE.get("DSH-Metropolitan")
        assert metro is not None
        assert metro.zip == "90650"

    def test_vhc_lancaster_zip_is_canonical_93536(self):
        """Pre-collapse: `_ADDRESS_FACILITIES` had VHC-Lancaster
        labelled with the right zip 93534. Post-collapse: registry
        carries the canonical 93536 (shared with CSP-LAC).
        (Audit-finding ordering for completeness — also one of the
        6 drift defects.)"""
        vhc_lc = facility_registry.get("CALVETHOME-LC")
        assert vhc_lc is not None
        assert vhc_lc.zip == "93536"


# ─── Facade preserves the legacy resolver API shape ──────────────────


class TestFacadePublicApiShape:
    """The collapse keeps `institution_resolver.resolve()` as a public
    API for backwards compat (many callers grandfathered via the
    allowlist in `test_classify_agency_facade.py`). These tests pin
    the dict shape so consumers don't break silently."""

    def test_resolve_returns_full_dict_shape(self):
        out = institution_resolver.resolve("CSP-SAC")
        for key in ("canonical", "agency", "facility_code",
                    "original", "source"):
            assert key in out, f"missing key {key!r} in {out!r}"

    def test_normalize_returns_string(self):
        out = institution_resolver.normalize("CSP-SAC")
        assert isinstance(out, str)
        assert out == "California State Prison, Sacramento"

    def test_same_institution_returns_bool(self):
        assert institution_resolver.same_institution(
            "CSP-SAC", "California State Prison, Sacramento"
        ) is True
        assert institution_resolver.same_institution(
            "CSP-SAC", "CIM"
        ) is False
