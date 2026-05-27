"""Tests for resolve_agency_patterns — fixes the silent no-op of
category-intel agency filtering.

Bug found 2026-04-27: the `quotes` table stores expanded agency names
("California Correctional Health Care Services") but operator UI passes
abbreviations ("CCHCS"). A naive `cchcs in row_agency` substring match
returned zero rows, which made the entire pricing-engine integration
(Phase 4.7 / Flavor B) silently no-op on prod traffic.
"""

import pytest

from src.core.agency_config import resolve_agency_patterns


class TestResolveAgencyPatterns:
    def test_empty_returns_empty_list(self):
        assert resolve_agency_patterns("") == []
        assert resolve_agency_patterns(None) == []

    def test_known_key_returns_full_pattern_list(self):
        patterns = resolve_agency_patterns("cchcs")
        assert "cchcs" in patterns
        assert "cdcr" in patterns
        # Lowercased patterns
        assert all(p == p.lower() for p in patterns)
        # Non-trivial expansion
        assert len(patterns) > 5

    def test_alias_resolves_to_canonical(self):
        # "cdcr" is aliased to cchcs in get_agency_config — same
        # behavior here, so cdcr-input gets cchcs full pattern list
        patterns = resolve_agency_patterns("cdcr")
        assert "cchcs" in patterns
        assert "folsom" in patterns or "cdcr" in patterns

    def test_freeform_full_name_resolves(self):
        # Operator-side might pass the full agency name — match_agency
        # should classify it
        patterns = resolve_agency_patterns("California Correctional Health Care Services")
        # Should resolve to cchcs and return its full pattern list
        assert "cchcs" in patterns or "cdcr" in patterns

    def test_unknown_falls_back_to_input_lowercased(self):
        patterns = resolve_agency_patterns("Some Brand New Buyer")
        assert patterns == ["some brand new buyer"]

    def test_patterns_match_real_quote_agency_strings(self):
        """The whole point: an "CCHCS" filter must hit a row where
        agency='California Correctional Health Care Services'."""
        patterns = resolve_agency_patterns("CCHCS")
        sample_quote_agencies = [
            "california correctional health care services",
            "cdcr — folsom state prison",
            "cchcs / cdcr",
            "ironwood state prison",
        ]
        for ag in sample_quote_agencies:
            assert any(p in ag for p in patterns), \
                f"agency '{ag}' should match at least one pattern from CCHCS"


class TestModulationUsesPatterns:
    """End-to-end: the modulation now correctly counts agency-scoped
    quotes via the pattern expansion."""

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
        # Seed 5 footwear LOSSES under expanded CDCR agency name
        for i in range(5):
            conn.execute(
                "INSERT INTO quotes (status, agency, institution, line_items, is_test) "
                "VALUES (?, ?, ?, ?, 0)",
                ("lost", "California Correctional Health Care Services", "Folsom State Prison",
                 json.dumps([{"description": f"Propet orthopedic shoe diabetic mens {i+8}"}]))
            )
        conn.commit()

        from src.core.category_intel_modulation import _bucket_stats

        # Naive abbreviation filter — must NOT return zero (the bug)
        stats = _bucket_stats(conn, "footwear-orthopedic", "CCHCS")
        assert stats["quotes"] == 5, \
            f"CCHCS-abbreviation filter should expand and match 5 rows, got {stats}"
        assert stats["losses"] == 5

        conn.close()
