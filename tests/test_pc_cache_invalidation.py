"""Regression: PC cache must reflect writes immediately, with no stale window.

Incident 2026-04-19: smoke test "PC detail: 1-click banner present" failed
intermittently because `_save_price_checks` invalidated `_pc_cache` BEFORE
writing to SQLite. A concurrent read between invalidate and commit would
populate the cache with the pre-write snapshot and serve stale data for
the 30-second cache TTL — including a brand-new test PC appearing as
"not found" (302 redirect to `/`) on the very next request.

These tests pin the contract: after `_save_*` returns, the next
`_load_price_checks()` call MUST see the new/updated record. Cache
invalidation has to happen AFTER the DB commit, not before.
"""
from __future__ import annotations

import pytest

from src.api.data_layer import (
    _load_price_checks,
    _save_price_checks,
    _save_single_pc,
)


def _make_pc(pc_id: str, **extra) -> dict:
    base = {
        "id": pc_id,
        "pc_number": f"PC-{pc_id[:6]}",
        "institution": "TestInst",
        "items": [],
        "status": "parsed",
        "created_at": "2026-04-19T00:00:00",
    }
    base.update(extra)
    return base


class TestSaveImmediatelyVisible:

    def test_save_price_checks_visible_on_next_load(self, temp_data_dir):
        pc_id = "test_cache_invl_001"
        _save_price_checks({pc_id: _make_pc(pc_id, is_quote_request=True)})
        loaded = _load_price_checks()
        assert pc_id in loaded, "newly-saved PC missing from next load"
        assert loaded[pc_id].get("is_quote_request") is True

    def test_save_single_pc_visible_on_next_load(self, temp_data_dir):
        pc_id = "test_cache_invl_002"
        _save_single_pc(pc_id, _make_pc(pc_id, is_quote_request=True))
        loaded = _load_price_checks()
        assert pc_id in loaded
        assert loaded[pc_id].get("is_quote_request") is True

    def test_update_via_save_price_checks_overwrites_cached_value(self, temp_data_dir):
        """Warm cache, mutate via save, verify cache reflects mutation."""
        pc_id = "test_cache_invl_003"
        _save_single_pc(pc_id, _make_pc(pc_id, status="parsed"))
        # Warm the cache
        first = _load_price_checks()
        assert first[pc_id]["status"] == "parsed"
        # Mutate
        _save_price_checks({pc_id: _make_pc(pc_id, status="quoted")})
        second = _load_price_checks()
        assert second[pc_id]["status"] == "quoted", (
            "cache served stale 'parsed' status after save mutated to 'quoted'"
        )

    def test_create_then_immediate_lookup_smoke_pattern(self, temp_data_dir):
        """Mirror the smoke flow: create PC → look it up by id immediately.

        This is the exact pattern that broke in production — `/api/test/create-pc`
        followed by `/pricecheck/<id>` saw the GET return 302 (not found) because
        a parallel reader populated the cache with the pre-create snapshot.
        """
        pc_id = "test_smoke_pattern_001"
        _save_price_checks({pc_id: _make_pc(pc_id, is_quote_request=True)})
        # Same pattern as _pricecheck_detail_inner:
        pcs = _load_price_checks()
        pc = pcs.get(pc_id)
        assert pc is not None, (
            "freshly-saved PC was None on next load — banner-missing race regressed"
        )
        assert pc.get("is_quote_request") is True
