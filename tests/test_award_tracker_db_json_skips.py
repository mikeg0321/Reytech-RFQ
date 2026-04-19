"""Tests that award_tracker and db.py JSON-parse paths surface skip
reasons via drainable module-level ledgers (PR #187).

Before: when a row's `line_items`, `items_detail`, `metadata`, etc. JSON
column was malformed (truncated by a crashed writer, double-encoded, etc.),
the loaders silently fell back to `[]` / `{}` / `None` and continued. The
caller never knew that a row had been served with stripped-out structured
data — they saw a "valid-looking" empty list. Most cascading bugs in
loss-analysis and PC-restore flows root-caused to one of these silent
fallbacks dropping a row's items mid-pipeline.

After: the same fallback values are still returned (these are graceful-
degradation paths — one corrupt row must not abort the whole loader),
but a SkipReason is appended to a module-level ledger that callers drain
to surface the corruption count via the standard 3-channel envelope.

Severity choice:
  - Per-row JSON parse failure → INFO (one corrupt row, others fine).
  - The orchestrator dedupes by (name, reason, severity), so 100 corrupt
    rows surface as one INFO line in the warnings panel.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from src.agents import award_tracker
from src.core import db as core_db
from src.core.dependency_check import Severity, SkipReason


# ── award_tracker ─────────────────────────────────────────────────────────────

class TestAwardTrackerDrainContract:
    def test_drain_returns_list_and_clears(self):
        award_tracker.drain_skips()
        award_tracker._record_skip(SkipReason(
            name="x", reason="y", severity=Severity.INFO, where="t",
        ))
        first = award_tracker.drain_skips()
        assert len(first) == 1
        assert award_tracker.drain_skips() == []

    def test_drain_idempotent_when_empty(self):
        award_tracker.drain_skips()
        assert award_tracker.drain_skips() == []
        assert award_tracker.drain_skips() == []


class TestAwardTrackerLineItemsParse:
    """The `_parse_line_items_safely(raw, where=...)` helper is the seam:
    one helper used by both solicitation extraction and run_award_check
    item parsing. On corrupt JSON it returns `[]` plus an INFO skip."""

    def test_valid_json_returns_parsed_list_no_skip(self):
        award_tracker.drain_skips()
        items = award_tracker._parse_line_items_safely(
            '[{"description":"gauze","qty":10}]',
            where="test",
        )
        assert items == [{"description": "gauze", "qty": 10}]
        assert award_tracker.drain_skips() == []

    def test_corrupt_json_returns_empty_and_emits_info_skip(self):
        award_tracker.drain_skips()
        items = award_tracker._parse_line_items_safely(
            "{not-valid-json",
            where="run_award_check.line_items",
        )
        assert items == []
        skips = award_tracker.drain_skips()
        assert any(
            s.severity is Severity.INFO
            and s.name == "line_items_json"
            and "run_award_check.line_items" in s.where
            for s in skips
        ), skips

    def test_empty_string_returns_empty_no_skip(self):
        """Empty/None inputs are normal (newly-created rows) — no skip."""
        award_tracker.drain_skips()
        assert award_tracker._parse_line_items_safely("", where="test") == []
        assert award_tracker._parse_line_items_safely(None, where="test") == []
        assert award_tracker._parse_line_items_safely("[]", where="test") == []
        assert award_tracker.drain_skips() == []

    def test_non_list_json_returns_empty_with_skip(self):
        """`line_items` must decode to a list. A bare object/number is
        a writer bug — surface it."""
        award_tracker.drain_skips()
        items = award_tracker._parse_line_items_safely(
            '{"oops":"wrong shape"}',
            where="run_award_check.line_items",
        )
        assert items == []
        skips = award_tracker.drain_skips()
        assert any(
            s.name == "line_items_json"
            and s.severity is Severity.INFO
            for s in skips
        ), skips


# ── db.py ─────────────────────────────────────────────────────────────────────

class TestDbDrainContract:
    def test_drain_returns_list_and_clears(self):
        core_db.drain_skips()
        core_db._record_skip(SkipReason(
            name="x", reason="y", severity=Severity.INFO, where="t",
        ))
        first = core_db.drain_skips()
        assert len(first) == 1
        assert core_db.drain_skips() == []

    def test_drain_idempotent_when_empty(self):
        core_db.drain_skips()
        assert core_db.drain_skips() == []
        assert core_db.drain_skips() == []


class TestDbJsonFieldDecoder:
    """`_decode_json_field(value, *, field, where)` is the seam used by
    `_row_to_quote`, the batch row decoders, and the metadata loaders.
    Returns the decoded value on success, the original on `None`/empty,
    and a typed empty (`[]` for list-typed fields, `{}` otherwise) on
    parse failure plus an INFO skip."""

    def test_valid_json_list_decoded(self):
        core_db.drain_skips()
        out = core_db._decode_json_field(
            '[1,2,3]', field="items_detail", where="_row_to_quote",
        )
        assert out == [1, 2, 3]
        assert core_db.drain_skips() == []

    def test_valid_json_dict_decoded(self):
        core_db.drain_skips()
        out = core_db._decode_json_field(
            '{"a":1}', field="metadata", where="_row_to_audit",
        )
        assert out == {"a": 1}
        assert core_db.drain_skips() == []

    def test_none_passthrough_no_skip(self):
        core_db.drain_skips()
        assert core_db._decode_json_field(None, field="items_detail", where="t") is None
        assert core_db._decode_json_field("", field="items_detail", where="t") == ""
        assert core_db.drain_skips() == []

    def test_corrupt_json_emits_info_skip_and_returns_typed_empty(self):
        core_db.drain_skips()
        out = core_db._decode_json_field(
            "{not-valid", field="items_detail", where="_row_to_quote",
        )
        # items_detail is list-typed → empty list fallback so callers
        # iterating it don't crash.
        assert out == []
        skips = core_db.drain_skips()
        assert any(
            s.name == "items_detail_json"
            and s.severity is Severity.INFO
            and "_row_to_quote" in s.where
            for s in skips
        ), skips

    def test_corrupt_json_for_dict_field_returns_empty_dict(self):
        core_db.drain_skips()
        out = core_db._decode_json_field(
            "{not-valid", field="metadata", where="_row_to_audit",
        )
        assert out == {}
        skips = core_db.drain_skips()
        assert any(
            s.name == "metadata_json"
            and s.severity is Severity.INFO
            for s in skips
        ), skips

    def test_already_decoded_value_passthrough(self):
        """If the column already contains a list/dict (e.g., joined query),
        return as-is without trying to decode."""
        core_db.drain_skips()
        decoded = [{"line_no": 1}]
        out = core_db._decode_json_field(decoded, field="items_detail", where="t")
        assert out is decoded
        assert core_db.drain_skips() == []
