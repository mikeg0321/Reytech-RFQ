"""Institution resolver: CDCR + facility abbrev embedded in noisy text.

2026-05-06 Mike P0: pc_e06e345d had institution resolved to generic
"CDCR" even though the form had CSP-SAC. Root cause was that the
keyword-fallback block in `_match_cdcr` only searched `_CDCR_CITIES`
(indexed by city/keyword) and missed CDCR facility abbreviations
(SAC, CIM, etc.) sitting next to "cdcr"/"cchcs" keywords.

These tests pin the abbreviation-aware fallback so the regression
class doesn't return.
"""

from src.core.institution_resolver import resolve


class TestCdcrKeywordFallbackPicksAbbreviation:
    def test_csp_sac_after_cdcr_keyword(self):
        # Normalizes to "cdcr csp sac" — old code returned generic CDCR;
        # new code picks SAC via the abbreviation scan.
        out = resolve("CDCR CSP-SAC")
        assert out["canonical"] == "California State Prison, Sacramento"
        assert out["agency"] == "cchcs"
        assert out["facility_code"] == "SAC"

    def test_cim_with_unit_suffix(self):
        out = resolve("CDCR CIM ML EOP")
        assert out["facility_code"] == "CIM"
        assert out["agency"] == "cchcs"

    def test_cchcs_plus_sac_alone(self):
        # No "csp" prefix, just CCHCS and the SAC abbrev.
        out = resolve("CCHCS SAC")
        assert out["facility_code"] == "SAC"
        assert out["canonical"] == "California State Prison, Sacramento"

    def test_csp_no_facility_returns_csp_canonical(self):
        # "cdcr csp" with no follow-on city/abbrev should at minimum
        # return "California State Prison" (CSP), not bare "CDCR".
        out = resolve("cdcr csp")
        assert out["facility_code"] == "CSP"
        assert out["canonical"] == "California State Prison"


class TestCdcrKeywordFallbackPreservesExistingPaths:
    def test_csp_sac_explicit_still_works(self):
        out = resolve("CSP-SAC")
        assert out["facility_code"] == "SAC"

    def test_full_facility_name_still_works(self):
        out = resolve("California State Prison Sacramento")
        assert out["facility_code"] == "SAC"

    def test_bare_cdcr_still_returns_generic(self):
        # No facility signal anywhere — generic CDCR is still the right answer.
        out = resolve("CDCR")
        assert out["canonical"] == "CDCR"
        assert out["facility_code"] == ""

    def test_city_name_still_resolves(self):
        # The city-keyword path must still work when no abbrev is present.
        # Use Lancaster (uniquely maps to LAC) since some cities like
        # Corcoran host two facilities and last-write-wins in the index.
        out = resolve("cdcr lancaster")
        assert out["facility_code"] == "LAC"
        assert out["canonical"] == "California State Prison, Los Angeles County"
