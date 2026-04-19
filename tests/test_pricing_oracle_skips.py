"""Tests that pricing_oracle_v2's per-data-source search functions surface
skip reasons via a drainable module-level ledger (PR #186).

Before: the five search-source helpers (`_search_won_quotes`,
`_search_winning_prices`, `_search_scprs_catalog`, `_search_po_lines`,
`_search_product_catalog`) all wrap their query in `try: ... except Exception
as e: log.debug("X search: %s", e); return prices`. When a data-source table
is missing, malformed, or the column shape drifts, the function silently
returns `[]` and the caller (`get_pricing`) just doesn't include that source
in `sources_used`. Operators see "no historical data" with no signal that
the data source crashed.

After: the same `[]` is still returned (the caller signature is preserved —
these are best-effort enrichments and must not raise), but a SkipReason is
appended to a module-level ledger. Routes/orchestrator drain the ledger after
running pricing to surface degraded-data-source warnings via the standard
3-channel envelope.

Severity choice:
  - One data source crashes → WARNING (a whole feature is dark for this lookup).
  - The caller still gets a usable price from the other sources, so it's not
    BLOCKER, but operators must know the result is built on partial data.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.core import pricing_oracle_v2
from src.core.dependency_check import Severity, SkipReason


def _drain_clean():
    pricing_oracle_v2.drain_skips()


class TestDrainSkipsContract:
    def test_drain_returns_list_and_clears(self):
        _drain_clean()
        pricing_oracle_v2._record_skip(SkipReason(
            name="x", reason="y", severity=Severity.WARNING, where="t",
        ))
        first = pricing_oracle_v2.drain_skips()
        assert len(first) == 1
        second = pricing_oracle_v2.drain_skips()
        assert second == []

    def test_drain_idempotent_when_empty(self):
        _drain_clean()
        assert pricing_oracle_v2.drain_skips() == []
        assert pricing_oracle_v2.drain_skips() == []


def _failing_db():
    """A db-like object whose .execute(...) always raises."""
    db = MagicMock()
    db.execute.side_effect = RuntimeError("simulated table missing")
    return db


class TestSearchWonQuotesSkip:
    def test_query_failure_emits_warning_and_returns_empty(self):
        _drain_clean()
        prices = pricing_oracle_v2._search_won_quotes(
            _failing_db(), "gauze 4x4 sterile", item_number="W12919",
        )
        # Caller signature preserved: empty list, no exception.
        assert prices == []
        skips = pricing_oracle_v2.drain_skips()
        assert any(
            s.name == "won_quotes"
            and s.severity is Severity.WARNING
            and "_search_won_quotes" in s.where
            for s in skips
        ), skips


class TestSearchWinningPricesSkip:
    def test_query_failure_emits_warning_and_returns_empty(self):
        _drain_clean()
        prices = pricing_oracle_v2._search_winning_prices(
            _failing_db(), "gauze 4x4 sterile", item_number="W12919",
        )
        assert prices == []
        skips = pricing_oracle_v2.drain_skips()
        assert any(
            s.name == "winning_prices"
            and s.severity is Severity.WARNING
            and "_search_winning_prices" in s.where
            for s in skips
        ), skips


class TestSearchScprsCatalogSkip:
    def test_query_failure_emits_warning_and_returns_empty(self):
        _drain_clean()
        prices = pricing_oracle_v2._search_scprs_catalog(
            _failing_db(), "gauze 4x4 sterile",
        )
        assert prices == []
        skips = pricing_oracle_v2.drain_skips()
        assert any(
            s.name == "scprs_catalog"
            and s.severity is Severity.WARNING
            and "_search_scprs_catalog" in s.where
            for s in skips
        ), skips


class TestSearchPoLinesSkip:
    def test_query_failure_emits_warning_and_returns_empty(self):
        _drain_clean()
        prices = pricing_oracle_v2._search_po_lines(
            _failing_db(), "gauze 4x4 sterile",
        )
        assert prices == []
        skips = pricing_oracle_v2.drain_skips()
        assert any(
            s.name == "scprs_po_lines"
            and s.severity is Severity.WARNING
            and "_search_po_lines" in s.where
            for s in skips
        ), skips


class TestSearchProductCatalogSkip:
    def test_query_failure_emits_warning_and_returns_empty(self):
        _drain_clean()
        prices = pricing_oracle_v2._search_product_catalog(
            _failing_db(), "gauze 4x4 sterile",
        )
        assert prices == []
        skips = pricing_oracle_v2.drain_skips()
        assert any(
            s.name == "product_catalog"
            and s.severity is Severity.WARNING
            and "_search_product_catalog" in s.where
            for s in skips
        ), skips


class TestEmptyTokensShortCircuitsWithoutSkip:
    """A description that tokenizes to nothing is a caller bug / no-op,
    not a data-source failure — must NOT pollute the ledger."""

    def test_empty_description_no_skip(self):
        _drain_clean()
        # Empty tokenization → returns [] without touching the DB.
        db = MagicMock()
        db.execute.side_effect = AssertionError("DB must not be hit when no tokens")
        assert pricing_oracle_v2._search_won_quotes(db, "") == []
        assert pricing_oracle_v2._search_winning_prices(db, "") == []
        assert pricing_oracle_v2._search_scprs_catalog(db, "") == []
        assert pricing_oracle_v2._search_po_lines(db, "") == []
        # _search_product_catalog uses a flat OR — empty tokens yield "" WHERE,
        # which is a different branch; check it separately below if needed.
        skips = pricing_oracle_v2.drain_skips()
        assert skips == [], skips


class TestHealthyQueryEmitsNoSkip:
    """When the DB returns a result set, the function must not record a skip."""

    def test_won_quotes_clean_run(self):
        _drain_clean()
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []  # no rows, no error
        prices = pricing_oracle_v2._search_won_quotes(db, "gauze 4x4 sterile")
        assert prices == []
        assert pricing_oracle_v2.drain_skips() == []

    def test_scprs_catalog_clean_run(self):
        _drain_clean()
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []
        prices = pricing_oracle_v2._search_scprs_catalog(db, "gauze 4x4 sterile")
        assert prices == []
        assert pricing_oracle_v2.drain_skips() == []


class TestMultipleFailuresAccumulate:
    """The orchestrator dedupes by (name, reason, severity); the module
    itself records every failure so per-call context is available upstream."""

    def test_two_failed_calls_accumulate_two_skips(self):
        _drain_clean()
        db = _failing_db()
        pricing_oracle_v2._search_won_quotes(db, "gauze 4x4 sterile")
        pricing_oracle_v2._search_won_quotes(db, "tape 1in adhesive")
        skips = pricing_oracle_v2.drain_skips()
        wq_skips = [s for s in skips if s.name == "won_quotes"]
        assert len(wq_skips) == 2, wq_skips
