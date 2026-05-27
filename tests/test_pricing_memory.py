"""Tests for `src.core.pricing_memory.resolve` — the single canonical
read-side resolver for "what do we know about this product?" that closes
the rfq_0124647e URL-paste catalog-hydration substrate bug (2026-05-26).

Substrate invariant under test: when scrape returns no price, the cost
field must still hydrate from the catalog (operator-confirmed rows only)
or from a linked PC by description token match — not return $0.00 with a
"No product data found" message while the data sits one SQL query away.

Provenance discipline is also locked: scraped/SCPRS rows in the catalog
do NOT surface through this resolver, even if they'd produce a cost. The
SQL gate (cost_source IN ('operator', 'catalog_confirmed')) is enforced
by `product_catalog.find_by_mfg_exact` and the new
`pricing_memory._try_catalog_by_url` / `_try_catalog_by_asin` mirrors.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest


# ── Helpers ────────────────────────────────────────────────────────────


def _seed_catalog_row(
    *, name="Widget Test",
    cost=24.50, sell_price=42.80,
    mfg_number="MFG-TEST-001", upc="012345678905",
    cost_source="operator",
    cost_source_url="https://www.amazon.com/dp/B08TVK1JQS",
    cost_accepted_at=None,
    is_test=0,
):
    """Insert a product_catalog row through the canonical init path so
    the test runs the same migration the production app does."""
    from src.agents.product_catalog import _get_conn, init_catalog_db
    init_catalog_db()
    if cost_accepted_at is None:
        cost_accepted_at = datetime.now(timezone.utc).isoformat()
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        # Clean any prior seed under the same name so reruns don't trip UNIQUE.
        conn.execute("DELETE FROM product_catalog WHERE name = ?", (name,))
        conn.execute(
            "INSERT INTO product_catalog (name, sku, description, category, "
            " item_type, uom, sell_price, cost, margin_pct, "
            " mfg_number, upc, "
            " cost_source, cost_source_url, cost_accepted_at, "
            " is_test, search_tokens, "
            " created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (name, mfg_number, name, "test", "Non-Inventory", "EA",
             sell_price, cost, 25.0, mfg_number, upc,
             cost_source, cost_source_url, cost_accepted_at,
             is_test, name.lower(),
             now, now),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def clean_catalog():
    """Wipe seeded test rows after each test so tests don't bleed."""
    yield
    try:
        from src.agents.product_catalog import _get_conn
        conn = _get_conn()
        conn.execute("DELETE FROM product_catalog WHERE name LIKE 'Widget Test%' OR name LIKE 'pricing_memory_%'")
        conn.commit()
        conn.close()
    except Exception:
        pass


# ── Tests ──────────────────────────────────────────────────────────────


class TestUrlKeyedLookup:
    """The screenshot bug specifically — URL pasted, scrape blocked,
    catalog has the data, cost stuck at $0.00. New URL-keyed lookup
    closes this."""

    def test_url_paste_hydrates_from_catalog_when_scrape_empty(self, clean_catalog):
        """Scrape returns nothing → resolve falls through to catalog_by_url
        and surfaces the operator-confirmed cost. Source chain shows
        catalog[url] as the producing source."""
        _seed_catalog_row(
            name="Widget Test URL Hydrate",
            cost=24.50,
            sell_price=42.80,
            cost_source_url="https://www.amazon.com/dp/B08TVK1JQS",
        )
        from src.core.pricing_memory import resolve

        def _fake_scrape(_):
            return {}  # scrape blocked / empty

        hit = resolve(
            url="https://www.amazon.com/dp/B08TVK1JQS",
            scrape_fn=_fake_scrape,
        )
        assert hit.cost == 24.50, (
            f"cost should hydrate from catalog when scrape empty, got {hit.cost}"
        )
        assert hit.sell_price == 42.80
        assert any(s.startswith("catalog[url]") for s in hit.source_chain), (
            f"chain missing catalog[url] hit: {hit.source_chain}"
        )

    def test_scrape_with_price_wins_over_catalog(self, clean_catalog):
        """Operator just pasted — fresh scrape intent beats catalog memory.
        Catalog still appears in chain as alternate context, but cost is
        the scrape value."""
        _seed_catalog_row(
            name="Widget Test Scrape Wins",
            cost=99.99,
            cost_source_url="https://www.amazon.com/dp/B08FRESH01",
        )
        from src.core.pricing_memory import resolve
        hit = resolve(
            url="https://www.amazon.com/dp/B08FRESH01",
            scrape_fn=lambda _: {"price": 30.00, "list_price": 35.00, "supplier": "Amazon"},
        )
        assert hit.cost == 35.00, "fresh scrape MSRP should win over catalog $99.99"
        assert hit.source_chain[0].startswith("scrape["), hit.source_chain

    def test_query_string_normalization(self, clean_catalog):
        """The URL with tracking params must collapse to the same catalog
        row as the canonical URL. Otherwise the same product on two
        different campaign URLs fails to match."""
        _seed_catalog_row(
            name="Widget Test Normalize",
            cost=15.00,
            cost_source_url="https://www.amazon.com/dp/B08NORM001",
        )
        from src.core.pricing_memory import resolve
        hit = resolve(
            url="https://www.amazon.com/dp/B08NORM001?ref=abc123&tag=affiliate",
            scrape_fn=lambda _: {},
        )
        assert hit.cost == 15.00, (
            "tracking-param URL should normalize to canonical form for catalog lookup"
        )


class TestMfgUpcFallback:
    """Existing `find_by_mfg_exact` cascade remains the workhorse when
    URL doesn't match. Resolver must surface its hits in the chain."""

    def test_resolves_by_mfg_when_no_url(self, clean_catalog):
        _seed_catalog_row(
            name="Widget Test MFG Only",
            mfg_number="MFG-RESOLVE-42",
            cost=12.34,
            cost_source_url="",
        )
        from src.core.pricing_memory import resolve
        hit = resolve(mfg="MFG-RESOLVE-42")
        assert hit.cost == 12.34
        assert any("catalog[mfg" in s for s in hit.source_chain), hit.source_chain


class TestProvenance:
    """The SQL gate (cost_source IN ('operator', 'catalog_confirmed'))
    must not leak Amazon/SCPRS scraped values through any resolver path.
    These are the rules CLAUDE.md / Pricing Guard Rails enshrines."""

    def test_amazon_scrape_row_does_not_surface(self, clean_catalog):
        """A row tagged cost_source='amazon_scrape' has a cost but is
        REFERENCE-only — it must not become a resolver hit."""
        _seed_catalog_row(
            name="Widget Test Provenance Refused",
            cost=77.77,
            cost_source="amazon_scrape",  # poisoned source
            cost_source_url="https://www.amazon.com/dp/B08POISON1",
        )
        from src.core.pricing_memory import resolve
        hit = resolve(
            url="https://www.amazon.com/dp/B08POISON1",
            scrape_fn=lambda _: {},
        )
        # Cost must NOT come from the poisoned row. It can still surface
        # via oracle / PC fallback (other paths) but not from catalog.
        catalog_hits = [s for s in hit.source_chain if s.startswith("catalog[")]
        assert not catalog_hits, (
            f"amazon_scrape-tagged row must not surface as catalog hit: {hit.source_chain}"
        )

    def test_is_test_row_does_not_surface(self, clean_catalog):
        """Test fixtures with is_test=1 must never bias real lookups."""
        _seed_catalog_row(
            name="Widget Test Is-Test Filter",
            cost=88.88,
            is_test=1,
            cost_source_url="https://www.amazon.com/dp/B08ISTEST1",
        )
        from src.core.pricing_memory import resolve
        hit = resolve(
            url="https://www.amazon.com/dp/B08ISTEST1",
            scrape_fn=lambda _: {},
        )
        assert hit.cost == 0.0, (
            f"is_test=1 row leaked through resolver: chain={hit.source_chain}"
        )


class TestMemoryHitShape:
    """The JSON-shape contract the frontend chip-ribbon consumes."""

    def test_empty_resolve_returns_zero_cost_hit(self):
        from src.core.pricing_memory import resolve, MemoryHit
        hit = resolve()
        assert isinstance(hit, MemoryHit)
        assert hit.cost == 0.0
        assert hit.source_chain == []
        assert hit.has_cost is False

    def test_to_jsonable_includes_chain_evidence_age(self, clean_catalog):
        _seed_catalog_row(
            name="Widget Test JSONable",
            cost=10.00,
            cost_source_url="https://www.amazon.com/dp/B08JSON001",
        )
        from src.core.pricing_memory import resolve, to_jsonable
        hit = resolve(
            url="https://www.amazon.com/dp/B08JSON001",
            scrape_fn=lambda _: {},
        )
        payload = to_jsonable(hit)
        assert "cost" in payload and payload["cost"] == 10.00
        assert "source_chain" in payload and isinstance(payload["source_chain"], list)
        assert "evidence_pc_id" in payload
        assert "age_days" in payload
        assert "memory_sell_price" not in payload  # field name lives on response
