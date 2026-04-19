"""Tests that item_link_lookup's claude_* lookup paths surface skip
reasons via a drainable module-level ledger (PR #184).

Before: when ANTHROPIC_API_KEY was unset or `requests` was unavailable,
`claude_amazon_lookup` and `claude_product_lookup` returned `{}` after
a `log.debug(...)` line. Operators saw "no enrichment" with no signal
that the LLM lookup tier was disabled. This was 3 of the 24 HIGH-severity
silent-skip instances from the 2026-04-18 audit.

After: the same `{}` is still returned (caller signature preserved —
these are best-effort enrichments and must not raise), but a SkipReason
is appended to a module-level ledger. Callers (orchestrator, routes)
drain the ledger after running enrichment to surface degraded-feature
warnings via the standard 3-channel envelope.
"""
from __future__ import annotations

import os
from unittest.mock import patch

from src.agents import item_link_lookup
from src.core.dependency_check import Severity, SkipReason


def _drain_clean():
    """Test isolation — discard any skips from previous tests."""
    item_link_lookup.drain_skips()


class TestDrainSkipsContract:
    def test_drain_returns_list_and_clears(self):
        _drain_clean()
        item_link_lookup._record_skip(SkipReason(
            name="x", reason="y", severity=Severity.WARNING, where="t",
        ))
        first = item_link_lookup.drain_skips()
        assert len(first) == 1
        # Drain is destructive — second drain returns empty.
        second = item_link_lookup.drain_skips()
        assert second == []

    def test_drain_idempotent_when_empty(self):
        _drain_clean()
        assert item_link_lookup.drain_skips() == []
        assert item_link_lookup.drain_skips() == []


class TestClaudeAmazonLookupSkips:
    def test_missing_api_key_emits_warning_skip(self):
        _drain_clean()
        with patch.dict(os.environ, {}, clear=True):
            result = item_link_lookup.claude_amazon_lookup("B0EXAMPLE12")
        assert result == {}, "lookup must still return {} so callers don't break"
        skips = item_link_lookup.drain_skips()
        assert any(
            s.name == "ANTHROPIC_API_KEY"
            and s.severity is Severity.WARNING
            and "claude_amazon_lookup" in s.where
            for s in skips
        ), skips

    def test_empty_asin_short_circuits_without_skip(self):
        """An empty ASIN is a caller bug, not a missing dep — no skip."""
        _drain_clean()
        result = item_link_lookup.claude_amazon_lookup("")
        assert result == {}
        # No skip — there was no dependency check that failed; caller passed nothing.
        assert item_link_lookup.drain_skips() == []

    def test_api_key_present_no_skip(self):
        _drain_clean()
        # Patch the requests call so we don't actually hit Anthropic.
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-test"}), \
             patch.object(item_link_lookup, "requests") as mock_req:
            mock_req.post.side_effect = RuntimeError("network blocked in tests")
            result = item_link_lookup.claude_amazon_lookup("B0EXAMPLE12")
        # Returns {} due to the network error, but the env-var skip path
        # was NOT taken — no API_KEY skip in the ledger.
        assert result == {}
        skips = item_link_lookup.drain_skips()
        assert not any(s.name == "ANTHROPIC_API_KEY" for s in skips), skips


class TestClaudeProductLookupSkips:
    def test_missing_api_key_emits_warning_skip(self):
        _drain_clean()
        with patch.dict(os.environ, {}, clear=True):
            result = item_link_lookup.claude_product_lookup(
                "https://supplier.example.com/item/123",
                supplier="Example",
            )
        assert result == {}
        skips = item_link_lookup.drain_skips()
        assert any(
            s.name == "ANTHROPIC_API_KEY"
            and s.severity is Severity.WARNING
            and "claude_product_lookup" in s.where
            for s in skips
        ), skips


class TestRequestsLibraryUnavailableSkip:
    def test_no_requests_emits_skip_for_amazon_lookup(self):
        _drain_clean()
        with patch.object(item_link_lookup, "HAS_REQUESTS", False), \
             patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-test"}):
            result = item_link_lookup.claude_amazon_lookup("B0EXAMPLE12")
        assert result == {}
        skips = item_link_lookup.drain_skips()
        assert any(
            s.name == "requests"
            and s.severity is Severity.WARNING
            and "claude_amazon_lookup" in s.where
            for s in skips
        ), skips

    def test_no_requests_emits_skip_for_product_lookup(self):
        _drain_clean()
        with patch.object(item_link_lookup, "HAS_REQUESTS", False):
            result = item_link_lookup.claude_product_lookup(
                "https://supplier.example.com/item/123",
            )
        assert result == {}
        skips = item_link_lookup.drain_skips()
        assert any(
            s.name == "requests"
            and "claude_product_lookup" in s.where
            for s in skips
        ), skips


class TestSkipsAreDeduplicatedInLedger:
    def test_repeated_calls_record_separate_skips(self):
        """The ledger keeps every event so the orchestrator's add_skip()
        layer (PR #182) can dedupe by (name, reason, severity) — but the
        lookup module itself records each occurrence so we don't lose
        per-item context if upstream wants it."""
        _drain_clean()
        with patch.dict(os.environ, {}, clear=True):
            item_link_lookup.claude_amazon_lookup("B0AAA")
            item_link_lookup.claude_amazon_lookup("B0BBB")
        skips = item_link_lookup.drain_skips()
        api_skips = [s for s in skips if s.name == "ANTHROPIC_API_KEY"]
        assert len(api_skips) == 2, api_skips
