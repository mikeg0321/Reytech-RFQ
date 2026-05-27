"""Tests for resolve_agency_patterns — fixes the silent no-op of
category-intel agency filtering.

Bug found 2026-04-27: the `quotes` table stores expanded agency names
("California Veterans Home, Yountville") but operator UI passes
abbreviations ("CalVet"). A naive `calvet in row_agency` substring
match returned zero rows, which made the entire pricing-engine
integration (Phase 4.7 / Flavor B) silently no-op on prod traffic.

Originally pinned against CCHCS. Rewritten 2026-05-27 (Job #1) — the
`DEFAULT_AGENCY_CONFIGS["cchcs"]` entry was DELETED per §0 LAW 2
(Spine is canonical CCHCS path). The pattern-expansion mechanism
itself is unchanged; the tests now pin it against surviving entries
(CalVet, which still owns the full match-pattern list for its
buyers). The CCHCS path's "abbreviation → full agency string" route
is dead by design — operator-side UI for CCHCS now reads through
Spine, not the legacy modulation dict.
"""

import pytest

from src.core.agency_config import resolve_agency_patterns


class TestResolveAgencyPatterns:
    def test_empty_returns_empty_list(self):
        assert resolve_agency_patterns("") == []
        assert resolve_agency_patterns(None) == []

    def test_known_key_returns_full_pattern_list(self):
        # Rewritten post-CCHCS-deletion: CalVet is the surviving
        # surface with a non-trivial pattern list.
        patterns = resolve_agency_patterns("calvet")
        assert "calvet" in patterns
        assert "veterans home" in patterns
        # Lowercased patterns
        assert all(p == p.lower() for p in patterns)
        # Non-trivial expansion
        assert len(patterns) > 3

    def test_alias_resolves_to_canonical(self):
        # "cal vet" with a space is in the alias map → canonical "calvet"
        patterns = resolve_agency_patterns("cal vet")
        assert "calvet" in patterns
        assert "veterans home" in patterns or "vhc" in patterns

    def test_freeform_full_name_resolves(self):
        # Operator-side might pass the full agency name — match_agency
        # should classify it via match_patterns
        patterns = resolve_agency_patterns("California Veterans Home Yountville")
        assert "calvet" in patterns or "veterans home" in patterns

    def test_unknown_falls_back_to_input_lowercased(self):
        patterns = resolve_agency_patterns("Some Brand New Buyer")
        assert patterns == ["some brand new buyer"]

    def test_cchcs_falls_back_to_input_post_deletion(self):
        """CCHCS no longer has a config entry (§0 LAW 2, Job #1 deletion).
        The function must still return SOMETHING — the lowercased input —
        so callers don't crash. Spine is canonical for CCHCS now."""
        assert resolve_agency_patterns("cchcs") == ["cchcs"]
        assert resolve_agency_patterns("CCHCS") == ["cchcs"]

    def test_patterns_match_real_quote_agency_strings(self):
        """The whole point: a "CalVet" filter must hit rows where
        agency='California Veterans Home, Yountville' (the expanded
        name the quotes table actually stores)."""
        patterns = resolve_agency_patterns("CalVet")
        sample_quote_agencies = [
            "california veterans home, yountville",
            "calvet — chula vista",
            "vhc-barstow veterans home",
        ]
        for ag in sample_quote_agencies:
            assert any(p in ag for p in patterns), \
                f"agency '{ag}' should match at least one pattern from CalVet"


class TestModulationUsesPatterns:
    """End-to-end: the modulation now correctly counts agency-scoped
    quotes via the pattern expansion. Rewritten against CalVet after
    the CCHCS dict was deleted (§0 LAW 2, Job #1)."""

    def test_bucket_stats_counts_expanded_agency_rows(self, tmp_path, monkeypatch):
        import sqlite3
        import json

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT,
                agency TEXT,
                institution TEXT,
                line_items TEXT,
                is_test INTEGER DEFAULT 0,
                total REAL DEFAULT 0
            )
        """)
        # Seed 5 footwear LOSSES under expanded CalVet agency name
        for i in range(5):
            conn.execute(
                "INSERT INTO quotes (status, agency, institution, line_items, is_test) "
                "VALUES (?, ?, ?, ?, 0)",
                ("lost", "California Veterans Home, Yountville", "VHC-Yountville",
                 json.dumps([{"description": f"Propet orthopedic shoe diabetic mens {i+8}"}]))
            )
        conn.commit()

        from src.core.category_intel_modulation import _bucket_stats

        # Naive abbreviation filter — must NOT return zero (the bug)
        stats = _bucket_stats(conn, "footwear-orthopedic", "CalVet")
        assert stats["quotes"] == 5, \
            f"CalVet-abbreviation filter should expand and match 5 rows, got {stats}"
        assert stats["losses"] == 5

        conn.close()
