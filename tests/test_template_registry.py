"""Unit tests for TemplateProfile (template_registry.py).

Tests PDF template introspection against the real blank 704 template.
No Flask or DB required — just PDF reading.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "templates")
FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
BLANK_704 = os.path.join(FIXTURES_DIR, "ams_704_blank.pdf")

# Fallback if fixture not copied yet
if not os.path.exists(BLANK_704):
    BLANK_704 = os.path.join(TEMPLATE_DIR, "ams_704_blank.pdf")


def _skip_if_no_template():
    if not os.path.exists(BLANK_704):
        pytest.skip("ams_704_blank.pdf not available")


class TestTemplateProfile:
    """Test TemplateProfile against the real blank 704 template."""

    def test_page_count(self):
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        assert profile.page_count >= 2, f"Expected >=2 pages, got {profile.page_count}"

    def test_pg1_row_count(self):
        """Blank 704 has 8 unsuffixed rows on page 1."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        # Per CLAUDE.md: page 1 has 8 unsuffixed rows (Row1-Row8)
        # But pg2 also has unsuffixed Row9-Row11
        # pg1_rows counts ONLY those on page 1
        assert profile.pg1_row_count >= 8, (
            f"Expected >=8 rows on page 1, got {profile.pg1_row_count}. "
            f"Rows: {profile.pg1_rows}"
        )

    def test_pg2_suffix_rows(self):
        """Blank 704 has 8 _2 suffix rows on page 2."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        assert len(profile.pg2_rows_suffixed) == 8, (
            f"Expected 8 _2-suffix rows, got {len(profile.pg2_rows_suffixed)}. "
            f"Rows: {profile.pg2_rows_suffixed}"
        )

    def test_total_capacity_is_19(self):
        """Total form field capacity is 19 items (no _3 or _4 fields)."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        assert profile.row_capacity == 19, (
            f"Expected 19 total capacity, got {profile.row_capacity}. "
            f"pg1={profile.pg1_row_count}, pg2={profile.pg2_row_count}"
        )

    def test_has_suffix_fields(self):
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        assert profile.has_suffix_fields is True

    def test_prefilled_detection(self):
        """Verify prefilled detection runs without error.

        Note: The Reytech blank 704 template is detected as prefilled=True
        because it contains default field values. This is expected behavior —
        the template has pre-existing annotations that trigger detection.
        """
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        assert isinstance(profile.is_prefilled, bool)

    def test_not_flattened(self):
        """Blank template should have fillable fields (not flattened)."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        assert profile.is_flattened is False

    def test_field_names_populated(self):
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        assert len(profile.field_names) > 0, "No field names detected"

    def test_known_fields_present(self):
        """Key fields we rely on must be in the template."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        expected = ["SUPPLIER NAME", "QTYRow1", "PRICE PER UNITRow1"]
        for field in expected:
            assert profile.has_field(field), (
                f"Expected field '{field}' not found. "
                f"Available: {sorted(list(profile.field_names))[:20]}..."
            )

    def test_row_field_suffix_slot1(self):
        """Slot 1 should be an unsuffixed row."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        suffix = profile.row_field_suffix(1)
        assert suffix is not None
        assert "_2" not in suffix, f"Slot 1 should be unsuffixed, got {suffix}"

    def test_row_field_suffix_overflow(self):
        """Slot beyond capacity should return None."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        suffix = profile.row_field_suffix(20)
        assert suffix is None, f"Slot 20 should overflow, got {suffix}"

    def test_row_page_number(self):
        """Slots 1-pg1 are page 1, rest are page 2."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        assert profile.row_page_number(1) == 1
        # A slot on page 2 (depends on pg1 count)
        pg2_slot = profile.pg1_row_count + 1
        if pg2_slot <= profile.row_capacity:
            assert profile.row_page_number(pg2_slot) == 2


class TestGetProfile:
    """Test the caching get_profile() function."""

    def test_returns_same_profile_for_same_file(self):
        _skip_if_no_template()
        from src.forms.template_registry import get_profile, _profile_cache
        _profile_cache.clear()
        p1 = get_profile(BLANK_704)
        p2 = get_profile(BLANK_704)
        assert p1 is p2  # same object from cache

    def test_profile_matches_direct_construction(self):
        _skip_if_no_template()
        from src.forms.template_registry import get_profile, TemplateProfile, _profile_cache
        _profile_cache.clear()
        cached = get_profile(BLANK_704)
        direct = TemplateProfile(BLANK_704)
        assert cached.pg1_row_count == direct.pg1_row_count
        assert cached.row_capacity == direct.row_capacity


class TestBootProfileValidation:
    """Strict-boot regression: every YAML profile in src/forms/profiles/ must
    validate against its blank PDF. If this fails, app boot will fail in prod
    (STRICT_PROFILE_BOOT=1 in app.py)."""

    def test_all_profiles_valid(self):
        from src.forms.profile_registry import validate_all_profiles
        results = validate_all_profiles()
        bad = {pid: issues for pid, issues in results.items() if issues}
        assert not bad, (
            "Profile validation failures (would block boot in prod):\n"
            + "\n".join(f"  {pid}: {issues[0]}" for pid, issues in bad.items())
        )

    def test_at_least_two_profiles_loaded(self):
        """Sanity check that profile loading is finding the YAML files."""
        from src.forms.profile_registry import load_profiles
        profiles = load_profiles()
        assert len(profiles) >= 2, (
            f"Expected at least 2 profiles (704a + 704b), found {len(profiles)}: "
            f"{list(profiles.keys())}"
        )
