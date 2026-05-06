"""URL paste substrate fix — PR-5 from 2026-05-06 audit.

Mike P0: "Everytime I paste a URL, the wrong description and same Anker
pricing shows up". Audit identified four substrate-level contamination
sources, all closed in this PR:

1. `_extract_asin` ran on any URL — non-Amazon URLs with `/dp/<10char>`
   or `/product/<10char>` paths minted fake ASINs that the rest of the
   pipeline then treated as Amazon, pulling wrong catalog data.

2. The lookup endpoint wrote unconditionally to the product catalog on
   every URL paste — operator-rejected URLs polluted the catalog with
   wrong products. Catalog ingestion is now operator-confirmed only,
   via `_do_save_prices`.

3. Hallucination guard accepted the host root substring (e.g. "amazon")
   anywhere in the response trail — including free-form text where Claude
   could fabricate references. Tightened to require the FULL host (e.g.
   "amazon.com") in tool_use input or tool_result content only.

4. JS captured buyer description AFTER overwriting it with the lookup
   title, so the match-score gate scored the title against itself (100%)
   and let any URL fill cost. Fixed by capturing `_origDesc` at function
   entry and scoring against it; RFQ description protection is now
   symmetric to PC.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── Fix 1: _extract_asin host validation ──────────────────────────


def test_extract_asin_rejects_non_amazon_host():
    """Staples / HD Supply / Walmart URLs that happen to contain
    `/dp/<10char>` or `/product/<10char>` must NOT mint a fake ASIN."""
    from src.agents.item_link_lookup import _extract_asin
    bogus_urls = [
        "https://www.staples.com/product/B077JQYDTN",
        "https://hdsupplysolutions.com/p/dp/B0CXX12345",
        "https://walmart.com/ip/something/dp/B0AAAAAAAA",
        "https://target.com/product/0123456789",
        "https://example.com/foo/dp/ABCDEFGHIJ",
    ]
    for url in bogus_urls:
        assert _extract_asin(url) == "", (
            f"Expected empty ASIN for non-Amazon host, got match for: {url}"
        )


def test_extract_asin_accepts_amazon_family():
    """Real Amazon URLs (and amzn.to short links, a.co) must still
    extract the ASIN — this is the load-bearing path."""
    from src.agents.item_link_lookup import _extract_asin
    cases = [
        ("https://www.amazon.com/dp/B077JQYDTN", "B077JQYDTN"),
        ("https://amazon.com/dp/B077JQYDTN", "B077JQYDTN"),
        ("https://www.amazon.com/gp/product/B0CXX12345", "B0CXX12345"),
        ("https://www.amazon.co.uk/dp/B077JQYDTN", "B077JQYDTN"),
        ("https://smile.amazon.com/dp/B077JQYDTN", "B077JQYDTN"),
    ]
    for url, expected in cases:
        assert _extract_asin(url) == expected, (
            f"Expected {expected} from {url}, got {_extract_asin(url)!r}"
        )


def test_extract_asin_handles_malformed_url():
    """Garbage in must not raise — must just return empty string."""
    from src.agents.item_link_lookup import _extract_asin
    assert _extract_asin("") == ""
    assert _extract_asin("not a url") == ""
    assert _extract_asin("ftp://wat.com/dp/B0AAAAAAAA") == ""


# ── Fix 2: Catalog write-back gate ────────────────────────────────


def test_lookup_endpoint_does_not_write_to_catalog():
    """The lookup endpoint must not import or call add_to_catalog /
    add_supplier_price. Catalog ingestion is gated on operator-confirmed
    save in routes_pricecheck.py:_do_save_prices."""
    src_path = os.path.join(
        os.path.dirname(__file__), "..",
        "src/api/modules/routes_pricecheck_admin.py",
    )
    with open(src_path, encoding="utf-8") as f:
        src = f.read()
    # Find the api_item_link_lookup function body
    start = src.index("def api_item_link_lookup(")
    end = src.index("\n@bp.route", start) if "\n@bp.route" in src[start:] else len(src)
    # Some files end with the function — fall back to next def-at-col-0
    if end == len(src):
        # Look for next top-level def or decorator
        next_def = src.find("\ndef ", start + 1)
        if next_def > 0:
            end = next_def
    body = src[start:end]
    forbidden = ("add_to_catalog", "add_supplier_price",
                 "enrich_catalog_product")
    for token in forbidden:
        assert token not in body, (
            f"api_item_link_lookup must not call {token} — "
            "catalog write-back happens only on operator-confirmed save."
        )


# ── Fix 3: Tightened hallucination guard ──────────────────────────


def _make_resp(content_blocks):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"content": content_blocks}
    return resp


@pytest.fixture
def with_api_key(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-not-real")
    yield


def test_hallucination_guard_blocks_when_host_only_in_text(with_api_key):
    """The model fabricated an Anker product for a Staples URL and
    quoted "I searched staples.com" in free-form text — but never
    actually called a tool against the host. Old guard would pass
    because "staples" appeared in trail; tightened guard requires the
    host in tool_use/tool_result evidence specifically."""
    from src.agents import item_link_lookup as mod
    fake_response = _make_resp([
        {"type": "tool_use", "input": {"query": "headphones noise cancelling"}},
        {"type": "tool_result", "content": [
            {"type": "text", "text": "Search returned wireless headphones..."},
        ]},
        {"type": "text", "text": (
            "I searched staples.com and found:\n"
            '{"title":"Anker Soundcore Headphones","list_price":59.99,'
            '"sale_price":null,"manufacturer":"Anker","mfg_number":"",'
            '"upc":"","photo_url":""}'
        )},
    ])
    with patch.object(mod, "requests") as mock_requests:
        mock_requests.post.return_value = fake_response
        mod.HAS_REQUESTS = True
        result = mod.claude_product_lookup(
            "https://www.staples.com/product/12345", supplier="Staples"
        )
    assert result == {}, (
        f"Expected hallucination guard to drop result (host only in text), got: {result}"
    )


def test_hallucination_guard_passes_when_host_in_tool_use(with_api_key):
    """Legitimate path: tool_use input mentions the full host. The
    grounding signal is real."""
    from src.agents import item_link_lookup as mod
    fake_response = _make_resp([
        {"type": "tool_use", "input": {
            "query": "staples.com office supplies model 12345"
        }},
        {"type": "tool_result", "content": [
            {"type": "text", "text": "Product page snippet..."},
        ]},
        {"type": "text", "text": (
            '{"title":"Office Supply Real Product","list_price":24.99,'
            '"sale_price":null,"manufacturer":"Staples","mfg_number":"",'
            '"upc":"","photo_url":""}'
        )},
    ])
    with patch.object(mod, "requests") as mock_requests:
        mock_requests.post.return_value = fake_response
        mod.HAS_REQUESTS = True
        result = mod.claude_product_lookup(
            "https://www.staples.com/product/12345", supplier="Staples"
        )
    assert result.get("list_price") == 24.99


def test_hallucination_guard_strips_www_prefix(with_api_key):
    """tool_evidence may quote "amazon.com" while URL host is "www.amazon.com".
    Guard normalizes by stripping leading "www." before substring check."""
    from src.agents import item_link_lookup as mod
    fake_response = _make_resp([
        {"type": "tool_use", "input": {
            "query": "amazon.com B077JQYDTN bracelet"
        }},
        {"type": "tool_result", "content": "Found on amazon.com..."},
        {"type": "text", "text": (
            '{"title":"Real Bracelet","list_price":14.99,"sale_price":null,'
            '"manufacturer":"","mfg_number":"","upc":"","photo_url":""}'
        )},
    ])
    with patch.object(mod, "requests") as mock_requests:
        mock_requests.post.return_value = fake_response
        mod.HAS_REQUESTS = True
        result = mod.claude_product_lookup(
            "https://www.amazon.com/dp/B077JQYDTN", supplier="Amazon"
        )
    assert result.get("list_price") == 14.99


# ── Fix 4: JS — _origDesc captured before mutation ────────────────


def _read_js(path):
    p = os.path.join(os.path.dirname(__file__), "..", path)
    with open(p, encoding="utf-8") as f:
        return f.read()


def test_js_captures_orig_desc_before_mutation():
    """The match-score gate must read buyer description BEFORE the
    description-fill block can overwrite it. _origDesc capture must
    happen at function entry, before descEl.value is touched."""
    src = _read_js("src/static/shared_item_utils.js")
    # Find _applyLinkData function
    start = src.index("function _applyLinkData(")
    body = src[start:start + 4000]
    # _origDesc capture must precede the description fill that does descEl.value =
    orig_idx = body.find("_origDesc")
    desc_value_idx = body.find("descEl.value = d.description")
    assert 0 < orig_idx < desc_value_idx, (
        "_origDesc must be captured before descEl.value = d.description; "
        f"orig at {orig_idx}, desc-fill at {desc_value_idx}"
    )


def test_js_match_score_uses_orig_desc():
    """_productMatchScore must score against _origDesc, not the
    post-mutation desc value (which would be 100% match against itself)."""
    src = _read_js("src/static/shared_item_utils.js")
    start = src.index("function _applyLinkData(")
    body = src[start:start + 4000]
    # _matchScore = _productMatchScore(_origDesc, _lookupT)
    assert "_productMatchScore(_origDesc, _lookupT)" in body, (
        "Match score must use _origDesc as the buyer-side comparand"
    )


def test_js_rfq_blocks_low_match_description_fill():
    """RFQ mode used to fill description blindly when current was short.
    Now requires _matchScore >= 40 OR _aiVerified — symmetric to PC."""
    src = _read_js("src/static/shared_item_utils.js")
    start = src.index("function _applyLinkData(")
    body = src[start:start + 4000]
    assert "rfqMatchOK" in body, (
        "RFQ description fill must gate on a match-score check"
    )
    assert "desc BLOCKED" in body, (
        "RFQ low-match path must surface a 'desc BLOCKED' badge"
    )


def test_js_orig_desc_missing_governs_cost_gate():
    """The _origDescMissing branch must be checked for BOTH PC and RFQ
    (not just PC). Manual-add RFQ rows have empty description — same
    contamination risk as manual-add PC rows."""
    src = _read_js("src/static/shared_item_utils.js")
    start = src.index("function _applyLinkData(")
    body = src[start:start + 4000]
    # Old code had `var _pcDescMissing = isPC && (!_pcDescV || _pcDescV.length < 3);`
    # Guard: that PC-only conditional must be gone
    assert "isPC && (!_pcDescV || _pcDescV.length < 3)" not in body, (
        "Old PC-only _pcDescMissing gate must be replaced by symmetric _origDescMissing"
    )
    assert "_origDescMissing" in body, (
        "Symmetric description-missing gate must use _origDescMissing"
    )
