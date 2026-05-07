"""ASIN-recycle cache eviction — closes the failure class behind the
2026-05-06 second URL-paste incident.

The 2026-05-04 detection (test_asin_recycle_detection) flagged recycled
ASINs at write time, but `_cache_lookup` short-circuited on cache hit
without consulting the flag. Result: a stale entry under
`asin:B00Y0L8FPW` (Anker SoundCore from before Amazon recycled the ASIN
to Wits & Wagers Deluxe Board Game) kept being served on every URL
paste because the lookup never re-fetched, so `_cache_store` never got
a chance to detect the divergence and set `recycled_suspected`.

Fix shape:
- New `_cache_evict(query)` helper in product_research.
- URL-paste handler in routes_pricecheck_admin compares the row's
  `pc_description` against the cached title's token overlap. When the
  overlap is below the recycle threshold (30 on the 0-100 scale), it
  evicts the asin entry and retries `lookup_from_url(url)` once,
  forcing an upstream re-fetch. The retry's fresh title flows into
  `_cache_store` which sets `recycled_suspected` correctly going forward.

Guard rails this test pins:
1. `_cache_evict` removes an existing entry and returns True.
2. `_cache_evict` is a no-op (returns False) when the key is absent.
3. `_cache_evict` survives a corrupt cache file gracefully.
4. The eviction decision predicate (token-overlap < 30 against
   pc_description) fires on the real-world incident shape and does
   NOT fire on a healthy match.
"""
from __future__ import annotations

import json
import pytest


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    """Point product_research at a temp cache file."""
    from src.agents import product_research as pr
    cache_path = tmp_path / "product_research_cache.json"
    monkeypatch.setattr(pr, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(pr, "CACHE_FILE", str(cache_path))
    return pr, cache_path


# ─── _cache_evict unit tests ─────────────────────────────────────────────


def test_cache_evict_removes_existing_entry(isolated_cache):
    pr, cache_path = isolated_cache
    pr._cache_store("asin:B00Y0L8FPW", {
        "title": "Anker SoundCore Bluetooth Speaker",
        "price": 16.0,
        "asin": "B00Y0L8FPW",
        "found": True,
    })
    # Sanity: entry is present.
    assert pr._cache_lookup("asin:B00Y0L8FPW") is not None

    evicted = pr._cache_evict("asin:B00Y0L8FPW")

    assert evicted is True
    assert pr._cache_lookup("asin:B00Y0L8FPW") is None
    # Cache file persisted without the evicted key.
    on_disk = json.loads(cache_path.read_text())
    key = pr._cache_key("asin:B00Y0L8FPW")
    assert key not in on_disk


def test_cache_evict_missing_key_is_noop(isolated_cache):
    pr, _ = isolated_cache
    # No entry stored — eviction returns False, doesn't raise.
    assert pr._cache_evict("asin:NEVERSEEN") is False


def test_cache_evict_corrupt_cache_file_does_not_raise(isolated_cache):
    pr, cache_path = isolated_cache
    cache_path.write_text("{ this is not valid json")
    # Corrupt cache file → _load_cache returns {} (per existing behavior),
    # so eviction reports no-op rather than crashing the URL-paste handler.
    assert pr._cache_evict("asin:B00Y0L8FPW") is False


# ─── Eviction-decision predicate (mirrors the URL-paste handler logic) ───


def _quick_token_match(desc_a: str, desc_b: str) -> int:
    """Mirror of `routes_pricecheck_admin._quick_token_match` — duplicated
    here so the predicate test doesn't depend on Flask app boot. The real
    function is tested via the URL-paste flow's existing semantic-match
    coverage; this copy pins the threshold the eviction logic uses."""
    import re as _re
    _stops = {'the', 'and', 'for', 'with', 'pack', 'of', 'per', 'ea', 'each',
              'box', 'pk', 'set', 'in', 'by', 'to', 'is', 'it', 'at', 'on',
              'or', 'an', 'as', 'from', 'bulk', 'assorted', 'count', 'ct',
              'qty', 'quantity', 'item', 'product', 'new', 'brand'}

    def _tok(s):
        s = s.lower()
        s = _re.sub(
            r'(\d+\.?\d*)\s*["”]?\s*[xX×]\s*(\d+\.?\d*)\s*["”]?',
            r'\1x\2', s,
        )
        s = _re.sub(r'(\d)\.(\d)', r'\1_D_\2', s)
        s = _re.sub(r'[^a-z0-9\s_]', ' ', s)
        s = s.replace('_D_', '.')
        return {w for w in s.split() if len(w) > 1 and w not in _stops}

    a, b = _tok(desc_a), _tok(desc_b)
    if not a or not b:
        return 0
    overlap = len(a & b)
    recall = overlap / len(a)
    precision = overlap / len(b)
    return round((2 * recall + precision) / 3 * 100)


def test_eviction_predicate_fires_on_recycled_asin_incident():
    """The exact pc_description vs cached-title shape from the 2026-05-06
    Mike screenshot. Row description is the board game; cache returned
    Anker speaker. Predicate must flag this as evict-and-retry."""
    pc_desc = "Wits & Wagers Deluxe Board Game by North Star Games"
    cached_title = (
        "Anker SoundCore Bluetooth Speaker with 24-Hour Playtime, "
        "Stereo Sound and Bass-D"
    )
    score = _quick_token_match(pc_desc, cached_title)
    assert score < 30, (
        f"Recycled-ASIN shape (board game vs Anker speaker) must score "
        f"below 30 for eviction to fire — got {score}."
    )


def test_eviction_predicate_quiet_on_healthy_match():
    """A correct lookup should NOT trigger eviction — preserves cache for
    the normal case."""
    pc_desc = "Wits & Wagers Deluxe Board Game by North Star Games"
    cached_title = "Wits & Wagers Deluxe by North Star Games — Family Board Game"
    score = _quick_token_match(pc_desc, cached_title)
    assert score >= 30, (
        f"Healthy match should score >= 30 (no eviction) — got {score}. "
        f"If this fires, the eviction predicate is too aggressive and "
        f"every URL paste will round-trip Amazon."
    )


def test_eviction_predicate_quiet_on_close_variant():
    """Same product, different SKU description (a real near-match) must
    NOT trigger eviction."""
    pc_desc = "All-New Echo Dot 5th Gen 2022 Smart speaker with Alexa Charcoal"
    cached_title = "All-New Echo Dot (5th Gen, 2022 release) | Smart speaker with Alexa | Charcoal"
    score = _quick_token_match(pc_desc, cached_title)
    assert score >= 30, (
        f"Same-product variant must score >= 30 — got {score}. Eviction "
        f"would force a wasteful Amazon round-trip on a perfect cache hit."
    )
