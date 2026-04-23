"""Regression: claude_amazon_lookup must drop responses where the
requested ASIN never appears in the model's response trail.

Incident 2026-04-23: ASIN B077JQYDTN ("Sport Medical Alert Bracelet
Replacement Wrist Strap") came back as "Anker Soundcore Life Q20
Active Noise Cancelling Headphones". Both the catalog price ($14.99)
and the wrong title were displayed to the operator. Cost was
correctly blocked by the client-side match guard (0% match), but the
"⚠ Found: Anker Soundcore..." line is misleading garbage — the model
fabricated a totally unrelated product when web_search returned no
useful results for the ASIN.

Defense: if the requested ASIN is absent from the entire response
trail (text + tool_use input + tool_result content), discard the
result. The legitimate path always references the ASIN somewhere
because the prompt and tool_use both pin to the ASIN URL.
"""
from __future__ import annotations

import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _make_resp(content_blocks):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"content": content_blocks}
    return resp


def _import_module():
    from src.agents import item_link_lookup
    return item_link_lookup


@pytest.fixture
def with_api_key(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-not-real")
    yield


def test_drops_hallucinated_response_when_asin_absent(with_api_key):
    """Claude returned a different product entirely (Anker Soundcore
    headphones for the medical bracelet ASIN). Trail never mentions
    the ASIN. Result must be dropped."""
    mod = _import_module()
    asin = "B077JQYDTN"
    fake_response = _make_resp([
        {"type": "tool_use", "input": {"query": "headphones noise cancelling"}},
        {"type": "tool_result", "content": [
            {"type": "text", "text": "Search returned wireless headphones..."},
        ]},
        {"type": "text", "text": (
            '{"title":"Anker Soundcore Life Q20 Headphones",'
            '"list_price":59.99,"sale_price":null,'
            '"manufacturer":"Anker","mfg_number":"A3025011",'
            '"upc":"194644023157","photo_url":""}'
        )},
    ])
    with patch.object(mod, "requests") as mock_requests:
        mock_requests.post.return_value = fake_response
        mod.HAS_REQUESTS = True
        result = mod.claude_amazon_lookup(asin)
    assert result == {}, (
        f"Expected hallucination guard to drop result, got: {result}"
    )


def test_keeps_valid_response_when_asin_in_tool_use_input(with_api_key):
    """Legitimate path: tool_use input mentions the ASIN URL.
    The text block returns a real-looking product."""
    mod = _import_module()
    asin = "B077JQYDTN"
    fake_response = _make_resp([
        {"type": "tool_use", "input": {
            "query": f"amazon.com/dp/{asin} sport medical alert bracelet"
        }},
        {"type": "tool_result", "content": [
            {"type": "text", "text": "Product page snippet..."},
        ]},
        {"type": "text", "text": (
            '{"title":"Sport Medical Alert ID Bracelet Replacement Strap",'
            '"list_price":14.99,"sale_price":null,'
            '"manufacturer":"American Medical ID","mfg_number":"",'
            '"upc":"","photo_url":""}'
        )},
    ])
    with patch.object(mod, "requests") as mock_requests:
        mock_requests.post.return_value = fake_response
        mod.HAS_REQUESTS = True
        result = mod.claude_amazon_lookup(asin)
    assert result.get("title", "").startswith("Sport Medical Alert")
    assert result.get("list_price") == 14.99


def test_keeps_valid_response_when_asin_in_tool_result(with_api_key):
    """Other legitimate path: tool_result content mentions the ASIN
    even if tool_use query didn't quote it directly."""
    mod = _import_module()
    asin = "B077JQYDTN"
    fake_response = _make_resp([
        {"type": "tool_use", "input": {"query": "medical alert bracelet replacement"}},
        {"type": "tool_result", "content": [
            {"type": "text", "text": (
                f"Found product on amazon.com — ASIN {asin}, "
                "available in 3 colors..."
            )},
        ]},
        {"type": "text", "text": (
            '{"title":"Sport Medical Alert ID Bracelet Replacement Strap",'
            '"list_price":14.99,"sale_price":null,'
            '"manufacturer":"American Medical ID","mfg_number":"",'
            '"upc":"","photo_url":""}'
        )},
    ])
    with patch.object(mod, "requests") as mock_requests:
        mock_requests.post.return_value = fake_response
        mod.HAS_REQUESTS = True
        result = mod.claude_amazon_lookup(asin)
    assert result.get("title", "").startswith("Sport Medical Alert")


def test_handles_string_tool_result_content(with_api_key):
    """Some tool_result blocks have content as a plain string instead
    of a list of blocks. Trail extraction must handle that shape."""
    mod = _import_module()
    asin = "B077JQYDTN"
    fake_response = _make_resp([
        {"type": "tool_use", "input": {"query": "amazon dp page"}},
        {"type": "tool_result", "content": (
            f"Page text containing ASIN {asin} and product details."
        )},
        {"type": "text", "text": (
            '{"title":"Sport Medical Alert Bracelet",'
            '"list_price":14.99,"sale_price":null,'
            '"manufacturer":"","mfg_number":"","upc":"","photo_url":""}'
        )},
    ])
    with patch.object(mod, "requests") as mock_requests:
        mock_requests.post.return_value = fake_response
        mod.HAS_REQUESTS = True
        result = mod.claude_amazon_lookup(asin)
    assert "Sport Medical" in result.get("title", "")
