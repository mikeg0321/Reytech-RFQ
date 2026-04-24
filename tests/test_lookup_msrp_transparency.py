"""Regression: lookup result transparency flags + hallucination guard.

Two surfaces under test:

1. **`single_price_promoted` flag** — when a supplier page returns one
   price (no separate MSRP / Was / strikethrough), `lookup_from_url`
   promotes the sale price to `list_price` so downstream code doesn't
   warn "MSRP not found." That's a UX win, but the operator needs to
   know the MSRP is unverified (it might actually be a sale price
   that expires mid-quote). The flag drives the UI badge.

2. **`enrichment_gap` flag** — when we have price + title but no
   manufacturer / mfg_number / UPC / photo, the operator is quoting
   against a thin identity signal. Flag surfaces so they can verify
   product identity before sending.

3. **`claude_product_lookup` hallucination guard** — mirrors the
   `claude_amazon_lookup` ASIN guard from PR #483. When Claude's
   web_search returns nothing useful for the URL host, the model
   sometimes invents a totally unrelated product. If the URL host
   is nowhere in Claude's response trail, discard the result.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _make_resp(content_blocks):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"content": content_blocks}
    return resp


@pytest.fixture
def with_api_key(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-not-real")
    yield


# ── single_price_promoted + enrichment_gap ────────────────────────────


def test_single_price_promoted_flag_set_when_only_sale_price_present():
    """A supplier page returning sale_price but no list_price should
    end up with single_price_promoted=True after lookup_from_url."""
    from src.agents import item_link_lookup

    fake_scrape = {
        "title": "New Solutions Sitting Safe Wheel Locks",
        "sale_price": 133.79,
        "price": 133.79,
        "manufacturer": "New Solutions",
        "mfg_number": "WL085P",
        "part_number": "763904",
    }
    with patch.object(item_link_lookup, "_scrape_generic", return_value=fake_scrape):
        result = item_link_lookup.lookup_from_url(
            "https://hdsupplysolutions.com/p/foo-p763904"
        )
    assert result["single_price_promoted"] is True
    # And the promotion happened.
    assert result["list_price"] == 133.79
    assert result["sale_price"] is None


def test_single_price_promoted_false_when_msrp_separate():
    """A page with both list_price and sale_price → no promotion → flag False."""
    from src.agents import item_link_lookup
    fake_scrape = {
        "title": "Some Product",
        "list_price": 199.99,
        "sale_price": 149.99,
        "price": 149.99,
        "manufacturer": "Brand",
        "mfg_number": "ABC123",
    }
    with patch.object(item_link_lookup, "_scrape_generic", return_value=fake_scrape):
        result = item_link_lookup.lookup_from_url(
            "https://example.com/product/foo"
        )
    assert result.get("single_price_promoted") is False


def test_enrichment_gap_true_when_no_anchor_fields():
    """Got a price but no manufacturer/mfg/upc/photo → flag True."""
    from src.agents import item_link_lookup
    fake_scrape = {
        "title": "Generic Part 5",
        "price": 50.0,
        "list_price": 50.0,
    }
    with patch.object(item_link_lookup, "_scrape_generic", return_value=fake_scrape):
        result = item_link_lookup.lookup_from_url(
            "https://example.com/random/product"
        )
    assert result["enrichment_gap"] is True


def test_enrichment_gap_false_when_manufacturer_present():
    from src.agents import item_link_lookup
    fake_scrape = {
        "title": "Branded Product",
        "price": 50.0,
        "list_price": 50.0,
        "manufacturer": "Real Brand",
    }
    with patch.object(item_link_lookup, "_scrape_generic", return_value=fake_scrape):
        result = item_link_lookup.lookup_from_url(
            "https://example.com/real/product"
        )
    assert result["enrichment_gap"] is False


def test_enrichment_gap_false_when_only_photo_present():
    """Photo URL alone is enough of an anchor."""
    from src.agents import item_link_lookup
    fake_scrape = {
        "title": "Visual Product",
        "price": 75.0,
        "list_price": 75.0,
        "photo_url": "https://cdn.example.com/img.jpg",
    }
    with patch.object(item_link_lookup, "_scrape_generic", return_value=fake_scrape):
        result = item_link_lookup.lookup_from_url(
            "https://example.com/visual/product"
        )
    assert result["enrichment_gap"] is False


# ── claude_product_lookup hallucination guard ─────────────────────────


def test_claude_product_lookup_drops_when_host_absent_from_trail(with_api_key):
    """Claude returned a result but never references the URL host —
    likely fabricated. Discard."""
    from src.agents import item_link_lookup
    url = "https://hdsupplysolutions.com/p/foo-p763904"
    fake_response = _make_resp([
        {"type": "tool_use", "input": {"query": "headphones bluetooth"}},
        {"type": "tool_result", "content": [
            {"type": "text", "text": "Found wireless headphones at amazon.com..."}
        ]},
        {"type": "text", "text": (
            '{"title":"Anker Soundcore Headphones",'
            '"list_price":59.99,"sale_price":null,'
            '"manufacturer":"Anker","mfg_number":"A3025",'
            '"upc":"","photo_url":""}'
        )},
    ])
    with patch.object(item_link_lookup, "requests") as mock_requests:
        mock_requests.post.return_value = fake_response
        item_link_lookup.HAS_REQUESTS = True
        result = item_link_lookup.claude_product_lookup(url, "HD Supply")
    assert result == {}, (
        f"Expected hallucination guard to drop result, got: {result}"
    )


def test_claude_product_lookup_keeps_when_host_in_tool_use(with_api_key):
    """Legitimate path: tool_use input mentions the URL host."""
    from src.agents import item_link_lookup
    url = "https://hdsupplysolutions.com/p/foo-p763904"
    fake_response = _make_resp([
        {"type": "tool_use", "input": {
            "query": "hdsupplysolutions.com wheel locks p763904"
        }},
        {"type": "tool_result", "content": [
            {"type": "text", "text": "Product page snippet..."}
        ]},
        {"type": "text", "text": (
            '{"title":"New Solutions Sitting Safe Wheel Locks",'
            '"list_price":133.79,"sale_price":null,'
            '"manufacturer":"New Solutions","mfg_number":"WL085P",'
            '"upc":"","photo_url":""}'
        )},
    ])
    with patch.object(item_link_lookup, "requests") as mock_requests:
        mock_requests.post.return_value = fake_response
        item_link_lookup.HAS_REQUESTS = True
        result = item_link_lookup.claude_product_lookup(url, "HD Supply")
    assert result.get("title", "").startswith("New Solutions")
    assert result.get("list_price") == 133.79


def test_claude_product_lookup_keeps_when_host_in_tool_result(with_api_key):
    """Legitimate path: tool_result content mentions the URL host."""
    from src.agents import item_link_lookup
    url = "https://hdsupplysolutions.com/p/foo-p763904"
    fake_response = _make_resp([
        {"type": "tool_use", "input": {"query": "wheel locks new solutions"}},
        {"type": "tool_result", "content": [
            {"type": "text", "text": (
                "Found match at hdsupplysolutions.com — page title 'New Solutions...'"
            )}
        ]},
        {"type": "text", "text": (
            '{"title":"New Solutions Sitting Safe Wheel Locks",'
            '"list_price":133.79,"sale_price":null,'
            '"manufacturer":"New Solutions","mfg_number":"",'
            '"upc":"","photo_url":""}'
        )},
    ])
    with patch.object(item_link_lookup, "requests") as mock_requests:
        mock_requests.post.return_value = fake_response
        item_link_lookup.HAS_REQUESTS = True
        result = item_link_lookup.claude_product_lookup(url, "HD Supply")
    assert result.get("title", "").startswith("New Solutions")
