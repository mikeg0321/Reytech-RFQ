"""ASIN-recycle detection — Surface #6 from the 2026-05-04 chain.

Mike's incident: Amazon ASIN B08TVK1JQS pointed to an Echo Dot once and
a Heel Donut today. Naively overwriting the cached title (a) silently
serves the new product's metadata to PCs that thought they were getting
the old product, (b) violates `feedback_item_identity` which says PC
descriptions are load-bearing.

Detection lives at cache write-time in `product_research._cache_store`:
when the new title's token-overlap with the cached title is below a
threshold, we flag the entry `recycled_suspected` and stash the previous
title. The QA agent reads the flag and emits a warning when any PC item
references a recycled ASIN.

Guard rails this test pins:
1. Token-overlap heuristic returns the right shape (0..1, empty inputs
   return 1.0 to avoid false positives on first-cache).
2. ASIN-keyed cache write below threshold flips the flag + stashes
   previous_title.
3. ASIN-keyed cache write above threshold (same / similar product)
   leaves the flag clear.
4. Non-ASIN-keyed writes never flip the flag (regular query lookups
   would otherwise cascade false positives).
5. `is_asin_cache_recycled` returns the right shape for both flagged
   and clean ASINs.
6. PC QA `_check_asin_recycled` emits a WARNING (not BLOCKER) for items
   whose ASIN is flagged, and stays silent when not flagged.
"""
from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    """Point product_research at a temp cache file so tests don't pollute
    the real `data/product_research_cache.json`."""
    from src.agents import product_research as pr
    cache_path = tmp_path / "product_research_cache.json"
    monkeypatch.setattr(pr, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(pr, "CACHE_FILE", str(cache_path))
    return pr, cache_path


# ─── Token overlap helper ────────────────────────────────────────────────

def test_title_overlap_identical():
    from src.agents.product_research import _title_token_overlap
    assert _title_token_overlap("Heel Donut Cushion", "Heel Donut Cushion") == 1.0


def test_title_overlap_disjoint():
    from src.agents.product_research import _title_token_overlap
    overlap = _title_token_overlap("Echo Dot Smart Speaker", "Heel Donut Cushion")
    assert overlap == 0.0, f"Disjoint titles should be 0.0, got {overlap}"


def test_title_overlap_partial():
    from src.agents.product_research import _title_token_overlap
    overlap = _title_token_overlap(
        "Heel Donut Pressure Relief Cushion",
        "Heel Donut Foam Cushion",
    )
    # 3 shared (heel, donut, cushion) / 5 total unique tokens
    assert 0.4 < overlap < 0.7


def test_title_overlap_empty_returns_one():
    """Empty inputs return 1.0 — first-cache scenarios shouldn't false-positive."""
    from src.agents.product_research import _title_token_overlap
    assert _title_token_overlap("", "Echo Dot") == 1.0
    assert _title_token_overlap("Echo Dot", "") == 1.0
    assert _title_token_overlap("", "") == 1.0


# ─── Cache-write detection ───────────────────────────────────────────────

def test_cache_store_flips_recycled_when_titles_diverge(isolated_cache):
    pr, cache_path = isolated_cache
    asin = "B08TVK1JQS"

    pr._cache_store(f"asin:{asin}", {
        "title": "Echo Dot (4th Gen) Smart Speaker with Alexa",
        "asin": asin, "price": 49.99, "url": f"https://amazon.com/dp/{asin}",
    })
    # Re-store with a totally different product (Mike's exact incident shape).
    pr._cache_store(f"asin:{asin}", {
        "title": "Heel Donut Pressure Relief Cushion",
        "asin": asin, "price": 7.99, "url": f"https://amazon.com/dp/{asin}",
    })

    cache = json.loads(cache_path.read_text())
    entry = cache[pr._cache_key(f"asin:{asin}")]
    assert entry.get("recycled_suspected") is True, (
        "ASIN cache write with disjoint title MUST flag recycled_suspected"
    )
    assert entry.get("previous_title") == "Echo Dot (4th Gen) Smart Speaker with Alexa"
    assert "recycled_at" in entry


def test_cache_store_no_flag_when_titles_similar(isolated_cache):
    """Refreshing the SAME product (minor title tweak) must NOT flag."""
    pr, cache_path = isolated_cache
    asin = "B0FAKE1234"

    pr._cache_store(f"asin:{asin}", {
        "title": "Acme Heel Donut Pressure Relief Foam Cushion",
        "asin": asin, "price": 7.99, "url": "",
    })
    pr._cache_store(f"asin:{asin}", {
        "title": "Acme Heel Donut Pressure Relief Cushion (Foam)",
        "asin": asin, "price": 8.49, "url": "",
    })

    cache = json.loads(cache_path.read_text())
    entry = cache[pr._cache_key(f"asin:{asin}")]
    assert not entry.get("recycled_suspected"), (
        "Title refresh with high token overlap MUST NOT flip the flag"
    )


def test_cache_store_first_write_no_false_positive(isolated_cache):
    """First-time cache for an ASIN has nothing to compare against."""
    pr, cache_path = isolated_cache
    pr._cache_store("asin:B0NEWASIN1", {
        "title": "Some Brand New Product",
        "asin": "B0NEWASIN1", "price": 19.99, "url": "",
    })
    cache = json.loads(cache_path.read_text())
    entry = list(cache.values())[0]
    assert not entry.get("recycled_suspected")


def test_cache_store_non_asin_key_never_flags(isolated_cache):
    """Regular query-string lookups have UNSTABLE keys (md5 of query),
    so a divergent title between two cache hits is not a recycle signal —
    it's just two different queries. Don't false-positive."""
    pr, cache_path = isolated_cache

    pr._cache_store("Echo Dot Smart Speaker", {
        "title": "Echo Dot 4th Gen", "asin": "B08TVK1JQS",
        "price": 49.99, "url": "",
    })
    # Same query string → same cache key → ovewrite. New title differs.
    pr._cache_store("Echo Dot Smart Speaker", {
        "title": "Heel Donut Cushion", "asin": "B08TVK1JQS",
        "price": 7.99, "url": "",
    })

    cache = json.loads(cache_path.read_text())
    entry = list(cache.values())[0]
    assert not entry.get("recycled_suspected"), (
        "Non-asin: cache keys must NOT participate in recycle detection."
    )


# ─── is_asin_cache_recycled helper ───────────────────────────────────────

def test_is_asin_cache_recycled_flagged(isolated_cache):
    pr, _ = isolated_cache
    asin = "B08TVK1JQS"
    pr._cache_store(f"asin:{asin}", {"title": "Echo Dot", "asin": asin, "price": 49.99, "url": ""})
    pr._cache_store(f"asin:{asin}", {"title": "Heel Donut Cushion", "asin": asin, "price": 7.99, "url": ""})

    info = pr.is_asin_cache_recycled(asin)
    assert info.get("recycled") is True
    assert info.get("previous_title") == "Echo Dot"
    assert info.get("current_title") == "Heel Donut Cushion"


def test_is_asin_cache_recycled_clean(isolated_cache):
    pr, _ = isolated_cache
    pr._cache_store("asin:B0CLEAN0001", {"title": "Foo", "asin": "B0CLEAN0001", "price": 1.0, "url": ""})
    info = pr.is_asin_cache_recycled("B0CLEAN0001")
    assert info == {}


def test_is_asin_cache_recycled_unknown_asin(isolated_cache):
    pr, _ = isolated_cache
    assert pr.is_asin_cache_recycled("B0NEVERSEEN") == {}
    assert pr.is_asin_cache_recycled("") == {}


# ─── pc_qa_agent integration ─────────────────────────────────────────────

def test_qa_warns_when_item_asin_is_recycled(isolated_cache):
    pr, _ = isolated_cache
    asin = "B08TVK1JQS"
    # Seed the cache with a recycle event.
    pr._cache_store(f"asin:{asin}", {"title": "Echo Dot 4th Gen", "asin": asin, "price": 49.99, "url": ""})
    pr._cache_store(f"asin:{asin}", {"title": "Heel Donut Pressure Relief Cushion", "asin": asin, "price": 7.99, "url": ""})

    from src.agents.pc_qa_agent import _check_asin_recycled
    issues = _check_asin_recycled(0, {"asin": asin}, {})
    assert len(issues) == 1
    issue = issues[0]
    assert issue["severity"] == "warning", "Per ten-minute-escape-valve must be soft warning, not blocker"
    assert issue["category"] == "identity"
    assert issue["field"] == "asin"
    assert asin in issue["message"]
    assert "Echo Dot" in issue["message"], "Operator needs the previous title to spot the divergence"
    assert "Heel Donut" in issue["message"]


def test_qa_silent_when_item_has_no_asin(isolated_cache):
    from src.agents.pc_qa_agent import _check_asin_recycled
    assert _check_asin_recycled(0, {"description": "Some product"}, {}) == []


def test_qa_silent_when_asin_not_recycled(isolated_cache):
    pr, _ = isolated_cache
    pr._cache_store("asin:B0CLEAN123", {"title": "Foo", "asin": "B0CLEAN123", "price": 1.0, "url": ""})

    from src.agents.pc_qa_agent import _check_asin_recycled
    assert _check_asin_recycled(0, {"asin": "B0CLEAN123"}, {}) == []


def test_qa_silent_when_no_bid(isolated_cache):
    """no_bid items are skipped by every other QA item-check; mirror that."""
    pr, _ = isolated_cache
    asin = "B0NOBID0001"
    pr._cache_store(f"asin:{asin}", {"title": "Echo Dot", "asin": asin, "price": 1.0, "url": ""})
    pr._cache_store(f"asin:{asin}", {"title": "Heel Donut Cushion", "asin": asin, "price": 1.0, "url": ""})

    from src.agents.pc_qa_agent import _check_asin_recycled
    assert _check_asin_recycled(0, {"asin": asin, "no_bid": True}, {}) == []


def test_qa_reads_asin_from_pricing_dict(isolated_cache):
    """ASIN sometimes lives on `item.pricing.asin` rather than `item.asin`
    (e.g. when merged in from `lookup_amazon_product`). Both paths must work."""
    pr, _ = isolated_cache
    asin = "B0PRICE001"
    pr._cache_store(f"asin:{asin}", {"title": "Echo Dot", "asin": asin, "price": 1.0, "url": ""})
    pr._cache_store(f"asin:{asin}", {"title": "Heel Donut Cushion", "asin": asin, "price": 1.0, "url": ""})

    from src.agents.pc_qa_agent import _check_asin_recycled
    issues = _check_asin_recycled(0, {}, {"asin": asin})
    assert len(issues) == 1


def test_qa_ignores_non_10char_asin(isolated_cache):
    """Real Amazon ASINs are exactly 10 characters. Anything else is operator
    free-text and shouldn't trigger a cache lookup."""
    from src.agents.pc_qa_agent import _check_asin_recycled
    assert _check_asin_recycled(0, {"asin": "TOO-SHORT"}, {}) == []
    assert _check_asin_recycled(0, {"asin": "TOOLONGASINFORSURE"}, {}) == []


# ─── Source-level guard: run_qa wires the new check ──────────────────────

def test_run_qa_includes_asin_recycle_in_check_loop():
    """Pin that `_check_asin_recycled` is called from the per-item loop in
    `run_qa()` — a future refactor that drops the call must fail this test."""
    src = Path("src/agents/pc_qa_agent.py").read_text(encoding="utf-8")
    # The check must be wired into the per-item loop, not just defined.
    assert "_check_asin_recycled(idx, item, p)" in src, (
        "run_qa() must call _check_asin_recycled in the per-item loop "
        "alongside _check_math / _check_completeness / _check_identity / "
        "_check_sources_disagree."
    )
