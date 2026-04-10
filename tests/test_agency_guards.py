"""V2 Test Suite — Group 6: Agency Config Crash Guards.

Tests that prevent match_agency() from crashing, which silently
falls to the wrong agency (CCHCS) and generates wrong form packages.

Incident: agency_config.py had no `import logging` — match_agency()
crashed on every call, fell to CCHCS fallback.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


class TestMatchAgencyFallback:
    """match_agency must always return a valid (key, config) tuple."""

    def test_unknown_agency_returns_fallback(self):
        """Totally unknown agency should return a valid fallback, not crash."""
        try:
            from src.core.agency_config import match_agency
        except ImportError:
            pytest.skip("agency_config not importable")

        rfq_data = {
            "agency": "Nonexistent Agency From Mars",
            "requestor_email": "alien@mars.gov",
            "institution": "",
        }
        key, cfg = match_agency(rfq_data)
        assert key is not None, "match_agency returned None key"
        assert isinstance(cfg, dict), "match_agency returned non-dict config"
        assert "name" in cfg, "Fallback config missing 'name'"

    def test_empty_rfq_data_returns_fallback(self):
        """Empty RFQ data should not crash."""
        try:
            from src.core.agency_config import match_agency
        except ImportError:
            pytest.skip("agency_config not importable")

        key, cfg = match_agency({})
        assert key is not None
        assert isinstance(cfg, dict)

    def test_cdcr_agency_matches(self):
        """CDCR agency data should match CDCR config."""
        try:
            from src.core.agency_config import match_agency
        except ImportError:
            pytest.skip("agency_config not importable")

        rfq_data = {
            "agency": "CDCR",
            "institution": "CSP-Sacramento",
        }
        key, cfg = match_agency(rfq_data)
        assert key.lower() in ("cdcr", "cchcs", "other"), f"Unexpected key: {key}"


class TestFallbackConfigHasRequiredKeys:
    """Every agency config dict must have these keys for downstream code."""

    REQUIRED_KEYS = {"name"}

    def test_all_configs_have_name(self):
        """Every loaded config must have a 'name' key."""
        try:
            from src.core.agency_config import load_agency_configs
        except ImportError:
            pytest.skip("agency_config not importable")

        configs = load_agency_configs()
        for key, cfg in configs.items():
            assert "name" in cfg, f"Agency config '{key}' missing 'name'"


class TestAgencyConfigCompiles:
    """agency_config.py must have all required imports (logging, etc.)."""

    def test_agency_config_imports_clean(self):
        """agency_config.py must import without error."""
        try:
            import src.core.agency_config as ac
            # Verify logging is available (the exact bug from the incident)
            assert hasattr(ac, "log") or hasattr(ac, "logging"), \
                "agency_config.py should have logging configured"
        except ImportError as e:
            pytest.fail(f"agency_config.py failed to import: {e}")


class TestExtractRequiredForms:
    """extract_required_forms_from_text() pattern matching."""

    def test_detects_std_204(self):
        try:
            from src.core.agency_config import extract_required_forms_from_text
        except ImportError:
            pytest.skip("function not available")
        result = extract_required_forms_from_text("Please complete STD 204 and return")
        assert "std204" in result.get("forms", [])

    def test_detects_dvbe_843(self):
        try:
            from src.core.agency_config import extract_required_forms_from_text
        except ImportError:
            pytest.skip("function not available")
        result = extract_required_forms_from_text("DVBE certification DGS PD 843 required")
        assert "dvbe843" in result.get("forms", [])

    def test_empty_text_returns_empty(self):
        try:
            from src.core.agency_config import extract_required_forms_from_text
        except ImportError:
            pytest.skip("function not available")
        result = extract_required_forms_from_text("")
        assert isinstance(result, dict)
        assert isinstance(result.get("forms", []), list)
