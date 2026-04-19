"""Tests that agency_config's DB-load and JSON-parse paths surface
skip reasons via a drainable module-level ledger (PR #185).

Before: three silent-skip patterns at lines 216, 279, 343 swallowed
DB connect failures and JSON parse errors with `log.debug("suppressed: %s", _e)`.
Operators got the hardcoded `DEFAULT_AGENCY_CONFIGS` even when learned
overrides existed but were unreadable, with no signal that the override
layer was disabled.

After: the same fallback values are still returned (these are graceful
degradation paths — the system continues working on defaults), but a
SkipReason is appended to a module-level ledger that the orchestrator
drains to surface degraded-feature warnings via the standard 3-channel
envelope.

Severity choices:
  - DB unavailable for entire load → WARNING (a whole feature is dark)
  - DB unavailable for buyer-history match → WARNING (one match path off)
  - Per-row JSON parse failure → INFO (one corrupt row, others fine)
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock

from src.core import agency_config
from src.core.dependency_check import Severity, SkipReason


def _drain_clean():
    agency_config.drain_skips()


class TestDrainSkipsContract:
    def test_drain_returns_list_and_clears(self):
        _drain_clean()
        agency_config._record_skip(SkipReason(
            name="x", reason="y", severity=Severity.INFO, where="t",
        ))
        first = agency_config.drain_skips()
        assert len(first) == 1
        second = agency_config.drain_skips()
        assert second == []

    def test_drain_idempotent_when_empty(self):
        _drain_clean()
        assert agency_config.drain_skips() == []
        assert agency_config.drain_skips() == []


class TestLoadAgencyConfigsSkips:
    def test_db_unavailable_emits_warning_skip_and_returns_defaults(self):
        _drain_clean()
        # Patch get_db to raise — simulates DB connection failure.
        def _raise_db(*a, **kw):
            raise RuntimeError("DB unavailable in test")

        with patch("src.core.db.get_db", side_effect=_raise_db):
            configs = agency_config.load_agency_configs()

        # Defaults still returned — the override layer is degraded, not fatal.
        assert "cchcs" in configs, "defaults should still be present"

        skips = agency_config.drain_skips()
        assert any(
            s.name == "agency_package_configs"
            and s.severity is Severity.WARNING
            and "load_agency_configs" in s.where
            for s in skips
        ), skips

    def test_corrupt_row_json_emits_info_skip(self):
        _drain_clean()
        # Mock a DB connection that returns one good row + one corrupt-JSON row.
        # The good row's overrides should still apply; the corrupt row should
        # emit an INFO skip but not abort the loop.
        good_row = ("cchcs", '["703b","704b"]', '["std204"]')
        # Use a real key (calvet) so the `if key in configs:` check passes
        # and we actually exercise the JSON parse.
        bad_row = ("calvet", "{not-valid-json", "[]")

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [good_row, bad_row]
        mock_ctx = MagicMock()
        mock_ctx.__enter__.return_value = mock_conn
        mock_ctx.__exit__.return_value = None

        with patch("src.core.db.get_db", return_value=mock_ctx):
            configs = agency_config.load_agency_configs()

        # Good row was applied
        assert configs["cchcs"]["required_forms"] == ["703b", "704b"]
        skips = agency_config.drain_skips()
        # The bad row emitted an INFO skip; the loop kept going.
        assert any(
            s.severity is Severity.INFO
            and "agency_package_configs" in s.name
            and "calvet" in s.reason
            for s in skips
        ), skips


class TestMatchAgencyBuyerHistorySkip:
    def test_buyer_history_db_failure_emits_warning_and_falls_back(self):
        _drain_clean()
        rfq_data = {
            "agency": "Some Random Agency",
            "requestor_email": "buyer@example.gov",
        }
        # Patch get_db so the OUTER load_agency_configs call works
        # (returns defaults) but the buyer-history query inside match_agency
        # fails. Easiest: make get_db raise on the SECOND call.
        call_count = {"n": 0}
        real_get_db = None
        try:
            from src.core.db import get_db as real_get_db
        except Exception:
            pass

        def _flaky_db(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1 and real_get_db:
                # First call (load_agency_configs) — let it succeed by raising
                # so it falls through to defaults without polluting the ledger
                # with a load-failure skip we don't care about for this test.
                # Use a clean drain after to isolate the second-call skip.
                raise RuntimeError("simulated for load")
            raise RuntimeError("simulated buyer-history DB failure")

        with patch("src.core.db.get_db", side_effect=_flaky_db):
            key, cfg = agency_config.match_agency(rfq_data)

        # No agency match (random agency) → falls back to "other".
        assert key == "other"
        skips = agency_config.drain_skips()
        # The match_agency buyer-history skip must be present.
        assert any(
            s.severity is Severity.WARNING
            and "buyer_history" in s.where
            for s in skips
        ), skips


class TestGetBuyerFormPreferencesSkips:
    def test_corrupt_forms_used_json_emits_info_skip(self):
        _drain_clean()
        # Mock DB returns one good row + one row with corrupt forms_used JSON.
        good_row = ("cchcs", '["704b","quote"]')
        bad_row = ("cchcs", "{not-valid-json")

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [good_row, bad_row]
        mock_ctx = MagicMock()
        mock_ctx.__enter__.return_value = mock_conn
        mock_ctx.__exit__.return_value = None

        with patch("src.core.db.get_db", return_value=mock_ctx):
            prefs = agency_config.get_buyer_form_preferences("buyer@example.gov")

        # Good row's forms still in the union — partial-degradation, not failure.
        assert prefs is not None
        assert "704b" in prefs["forms"]
        skips = agency_config.drain_skips()
        assert any(
            s.severity is Severity.INFO
            and "agency_form_history" in s.name
            and "get_buyer_form_preferences" in s.where
            for s in skips
        ), skips


class TestNoSkipsOnCleanRun:
    def test_clean_load_emits_no_skips(self):
        """Clean run = DB connects, table exists, returns zero override rows.
        Defaults stand alone; no degradation, no skip noise."""
        _drain_clean()
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []  # no overrides
        mock_ctx = MagicMock()
        mock_ctx.__enter__.return_value = mock_conn
        mock_ctx.__exit__.return_value = None
        with patch("src.core.db.get_db", return_value=mock_ctx):
            configs = agency_config.load_agency_configs()
        assert "cchcs" in configs
        assert agency_config.drain_skips() == []
