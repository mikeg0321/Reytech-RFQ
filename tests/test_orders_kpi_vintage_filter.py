"""ISSUE-11 (2026-05-29 sweep): /orders headline KPIs must exclude the stale
2023-2025 bulk import WITHOUT hiding genuine current-year POs.

The import is identified by its created_at mass-insert signature — NOT quote
vintage — because a real 2026 PO can ride a 2025-vintage quote (R25Q..). Mike
2026-05-29: "we have won POs in 2026 … so it's not none."
"""

from src.core.canonical_state import (
    is_historical_import_order,
    BULK_IMPORT_CREATED_AT_PREFIX,
    REVENUE_YEAR,
    po_numbers_match,
    solicitation_numbers_match,
)


class TestSolicitationNumbersMatch:
    """ISSUE-11 sweep: PO ingest must match a won quote by solicitation in the
    canonical quotes table, tolerant of label prefixes (PREQ/PR/PO)."""

    def test_prefix_tolerant(self):
        assert solicitation_numbers_match("PREQ 10846357", "10846357") is True
        assert solicitation_numbers_match("PR-10846357", "10846357") is True

    def test_exact_and_leading_zeros(self):
        assert solicitation_numbers_match("10846357", "10846357") is True
        assert solicitation_numbers_match("00000767", "00000767") is True

    def test_distinct_no_match(self):
        assert solicitation_numbers_match("10846357", "00000767") is False

    def test_empty(self):
        assert solicitation_numbers_match("", "10846357") is False
        assert solicitation_numbers_match(None, None) is False


class TestPoNumbersMatch:
    """ISSUE-11 sweep: order↔won-quote link key is the PO number (agency-prefix
    tolerant), NOT rfq.solicitation. Used to credit $0 stub orders."""

    def test_exact_match(self):
        assert po_numbers_match("4500750017", "4500750017") is True

    def test_agency_prefix_tolerant_both_directions(self):
        assert po_numbers_match("8955-0000076737", "0000076737") is True
        assert po_numbers_match("0000076737", "8955-0000076737") is True

    def test_distinct_pos_do_not_match(self):
        assert po_numbers_match("4500750017", "0000076737") is False

    def test_short_cores_rejected(self):
        assert po_numbers_match("123", "1234567") is False  # <7 digit core

    def test_empty_or_none(self):
        assert po_numbers_match("4500750017", "") is False
        assert po_numbers_match(None, "4500750017") is False

    def test_strips_non_digits(self):
        assert po_numbers_match("PO 4500750017", "4500750017") is True


class TestHistoricalImportOrder:
    def test_bulk_import_minute_is_historical(self):
        o = {"created_at": BULK_IMPORT_CREATED_AT_PREFIX + ":15.351562", "total": 24572.96}
        assert is_historical_import_order(o) is True

    def test_pre_current_year_is_historical(self):
        assert is_historical_import_order({"created_at": "2024-09-01T10:00:00"}) is True
        assert is_historical_import_order({"created_at": "2025-12-31T23:59:59"}) is True

    def test_real_current_year_po_is_kept(self):
        # a genuine 2026 PO created on ANY other timestamp survives
        assert is_historical_import_order({"created_at": "2026-02-21T00:29:41"}) is False
        assert is_historical_import_order({"created_at": "2026-05-29T12:00:00"}) is False

    def test_2026_po_on_2025_quote_is_kept(self):
        # the case Mike flagged: real 2026 PO, prior-year vintage quote.
        # Keyed on created_at, the R25Q quote_number is irrelevant → kept.
        o = {"created_at": "2026-03-10T09:00:00", "quote_number": "R25Q140", "total": 5000}
        assert is_historical_import_order(o) is False

    def test_po_date_is_source_of_truth_and_wins(self):
        # once po_date is backfilled from the email/PDF, it overrides the
        # created_at heuristic — even an import-minute created_at.
        o = {"created_at": BULK_IMPORT_CREATED_AT_PREFIX + ":15.0",
             "po_date": "2026-03-01"}
        assert is_historical_import_order(o) is False   # real 2026 PO → kept
        o2 = {"created_at": "2026-06-01T00:00:00", "po_date": "2024-08-18"}
        assert is_historical_import_order(o2) is True    # real 2024 PO → historical

    def test_missing_or_bad_timestamp_is_kept(self):
        assert is_historical_import_order({}) is False
        assert is_historical_import_order({"created_at": ""}) is False
        assert is_historical_import_order({"created_at": "not-a-date"}) is False

    def test_current_year_constant_is_2026(self):
        # guards the cutoff; bump REVENUE_YEAR annually (canonical_state owns it)
        assert REVENUE_YEAR == 2026

    def test_test_rows_are_not_specially_excluded_here(self):
        # is_test filtering is the caller's job; this predicate is date-only
        o = {"created_at": "2024-01-01T00:00:00", "is_test": True}
        assert is_historical_import_order(o) is True
