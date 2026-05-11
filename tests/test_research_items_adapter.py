"""Pin the research_items adapter wraps bulk_web_search correctly.

Background: `research_items` was a phantom in both routes_analytics and
routes_rfq_gen — both callers expected enriched items back (with
amazon_price/item_link/item_supplier merged onto each input item),
but `bulk_web_search` returns a parallel list of {idx, found, price,
source, url} dicts keyed by idx. The adapter does the merge so existing
callers work without changes.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(rel: str) -> str:
    return Path(rel).read_text(encoding="utf-8")


class TestAdapterExists:
    def test_research_items_resolves(self):
        from src.agents.web_price_research import research_items
        assert callable(research_items)


class TestAdapterMergesByIdx:
    def test_merges_hit_onto_input_item(self):
        """When bulk_web_search returns a result for an item, that item
        gets amazon_price/item_link/item_supplier populated."""
        from src.agents import web_price_research as wpr

        items = [
            {"description": "nitrile gloves", "part_number": "NG-100", "qty": 10},
        ]
        fake_result = [{
            "idx": 0, "found": True, "price": 24.99,
            "source": "amazon.com", "url": "https://amazon.com/x",
            "title": "Nitrile Gloves 100ct",
        }]
        with patch.object(wpr, "bulk_web_search", return_value=fake_result):
            out = wpr.research_items(items)

        assert out is items  # mutates in place + returns same list
        assert out[0]["amazon_price"] == 24.99
        assert out[0]["item_link"] == "https://amazon.com/x"
        assert out[0]["item_supplier"] == "amazon.com"

    def test_missing_result_leaves_item_untouched(self):
        """If bulk_web_search returns found=False, item must NOT gain
        an amazon_price (don't clobber existing data)."""
        from src.agents import web_price_research as wpr

        items = [{"description": "x", "amazon_price": 50.0}]  # already has a price
        with patch.object(wpr, "bulk_web_search",
                          return_value=[{"idx": 0, "found": False}]):
            out = wpr.research_items(items)
        # Pre-existing amazon_price should still be 50, not overwritten
        assert out[0]["amazon_price"] == 50.0
        assert "item_link" not in out[0]

    def test_empty_input_returns_empty(self):
        from src.agents.web_price_research import research_items
        assert research_items([]) == []

    def test_assigns_idx_when_missing(self):
        """Caller may not have set idx — adapter assigns position-based idx."""
        from src.agents import web_price_research as wpr
        items = [{"description": "a"}, {"description": "b"}]
        with patch.object(wpr, "bulk_web_search", return_value=[]):
            wpr.research_items(items)
        assert items[0]["idx"] == 0
        assert items[1]["idx"] == 1


class TestNoPhantomBaseline:
    """Both old call sites must still import `research_items` (now real)."""

    def test_routes_analytics_imports_real_name(self):
        src = _read("src/api/modules/routes_analytics.py")
        assert "from src.agents.web_price_research import research_items" in src

    def test_routes_rfq_gen_imports_real_name(self):
        src = _read("src/api/modules/routes_rfq_gen.py")
        assert "from src.agents.web_price_research import research_items" in src
