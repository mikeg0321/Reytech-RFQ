"""Pin Tier 3c Phase 2 — wire `anthropic_quota.check_quota` + `log_call`
into the 10 REST-based Anthropic callers (audit 2026-05-07).

Phase 1 (PR #843) shipped the substrate + migrated 4 SDK callers.
This phase covers the 10 REST callers that POST to
`api.anthropic.com/v1/messages` directly:

  src/agents/cost_reduction_agent.py     (1 site, with retry on 429)
  src/agents/product_catalog.py          (1 site)
  src/agents/requirement_extractor.py    (1 site)
  src/agents/web_price_research.py       (1 site, with 2 inline retries)
  src/forms/pdf_visual_qa.py             (1 site)
  src/forms/supplier_quote_parser.py     (1 site)
  src/forms/vision_parser.py             (3 sites, 3 distinct functions)
  src/agents/item_link_lookup.py         (3 sites, 3 distinct functions)

Each migration adds:
  1. `check_quota(agent=...)` gate before the POST. When the daily
     cap is exhausted, the function returns its empty/skip shape
     without ever burning a Claude call.
  2. `log_call(agent=..., tokens_in=, tokens_out=, model=,
     response_time_ms=, error=...)` after the response. Token usage
     comes from `data["usage"]["input_tokens"]` / `["output_tokens"]`
     (REST shape — different from the SDK shape `resp.usage.input_tokens`
     used in Phase 1).

These tests pin the gate behavior end-to-end through one
representative caller from each file. We use `patch.dict` to set
ANTHROPIC_API_KEY and monkeypatch the api_quota.api_quota singleton
so no real HTTP / DB happens.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _stub_api_quota(monkeypatch, can_call_returns: bool = True):
    """Replace the api_quota singleton with a MagicMock so we can
    observe `can_call(service)` and `log_call(service, **kw)`."""
    fake = MagicMock()
    fake.can_call = MagicMock(return_value=can_call_returns)
    fake.log_call = MagicMock()
    monkeypatch.setattr("src.core.api_quota.api_quota", fake)
    return fake


# ─── cost_reduction_agent ─────────────────────────────────────────

def test_cost_reduction_agent_skipped_when_quota_closed(monkeypatch):
    """When `api_quota.can_call("claude")` returns False, the
    function returns the canonical skip shape and NEVER calls
    `requests.post`."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    fake_quota = _stub_api_quota(monkeypatch, can_call_returns=False)

    posted = {"n": 0}

    def fail_post(*a, **kw):
        posted["n"] += 1
        raise AssertionError("POST should not run when quota closed")

    monkeypatch.setattr(
        "src.agents.cost_reduction_agent.requests.post", fail_post)

    from src.agents.cost_reduction_agent import research_cost_reduction
    result = research_cost_reduction(
        description="3M Promogran Matrix Wound Dressing 4x4",
        current_cost=87.41,
        competitor_price=120.0,
        quantity=10, uom="EA",
    )
    assert result["ok"] is False
    assert "quota_exceeded" in result["error"]
    assert posted["n"] == 0
    fake_quota.can_call.assert_called_with("claude")


# ─── product_catalog ──────────────────────────────────────────────

def test_product_catalog_skipped_when_quota_closed(monkeypatch):
    """`product_catalog.ai_find_product` imports `requests as _req`
    INSIDE the function body, so the gate in our migration runs
    BEFORE that import — meaning we don't need to monkeypatch the
    requests module to prove the gate skipped (the import never
    runs)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    _stub_api_quota(monkeypatch, can_call_returns=False)

    from src.agents.product_catalog import ai_find_product
    out = ai_find_product(description="Test product", quantity=1)
    assert out["ok"] is False
    assert "quota_exceeded" in out["error"]


# ─── requirement_extractor ────────────────────────────────────────

def test_requirement_extractor_skipped_when_quota_closed(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    _stub_api_quota(monkeypatch, can_call_returns=False)

    def fail_post(*a, **kw):
        raise AssertionError("POST should not run")

    monkeypatch.setattr(
        "src.agents.requirement_extractor.requests.post", fail_post)

    from src.agents.requirement_extractor import extract_requirements
    out = extract_requirements(
        email_body="Please send your quote by Friday. Need 100 widgets.",
        subject="RFQ - widget quote",
        attachments=[])
    # Returns None on skip (matches existing API-error / 429 fallback)
    # OR returns the regex-fallback dataclass — either way no POST runs.
    # The contract: gate prevents the Claude POST, so no AssertionError.
    assert out is None or hasattr(out, "forms_required")


# ─── web_price_research ───────────────────────────────────────────

def test_web_price_research_skipped_when_quota_closed(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    _stub_api_quota(monkeypatch, can_call_returns=False)

    def fail_post(*a, **kw):
        raise AssertionError("POST should not run")

    monkeypatch.setattr(
        "src.agents.web_price_research.requests.post", fail_post)
    # Bypass the on-disk cache by stubbing _load_cache to return {}.
    monkeypatch.setattr(
        "src.agents.web_price_research._load_cache", lambda: {})

    from src.agents.web_price_research import search_product_price
    out = search_product_price(
        description="Test widget", part_number="W12919")
    assert out["found"] is False
    assert "quota_exceeded" in out["error"]


# ─── pdf_visual_qa ────────────────────────────────────────────────

def test_pdf_visual_qa_skipped_when_quota_closed(monkeypatch):
    """`pdf_visual_qa._call_vision_api` raises RuntimeError on skip
    rather than returning a dict — the calling QA pipeline expects
    a raise, not a None."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    _stub_api_quota(monkeypatch, can_call_returns=False)

    from src.forms.pdf_visual_qa import _call_vision_api
    with pytest.raises(RuntimeError) as exc:
        _call_vision_api(
            api_key="sk-test",
            page_images=[{"base64": "x", "media_type": "image/png"}],
            user_prompt="check this")
    assert "quota_exceeded" in str(exc.value)


# ─── supplier_quote_parser ────────────────────────────────────────

def test_supplier_quote_parser_skipped_when_quota_closed(
        monkeypatch, tmp_path):
    """`_parse_supplier_quote_vision` imports `requests as _req`
    INSIDE the function — gate runs before that import. We stub
    the upstream `_pdf_to_images` to skip the real PDF rendering."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    _stub_api_quota(monkeypatch, can_call_returns=False)

    # Stub the PDF-to-image helper if it exists, so we never try to
    # render a real PDF (the gate runs after rendering in this fn).
    import src.forms.supplier_quote_parser as sqp_mod
    if hasattr(sqp_mod, "_pdf_to_images"):
        monkeypatch.setattr(sqp_mod, "_pdf_to_images",
                            lambda p, max_pages=4:
                            [{"base64": "x", "media_type": "image/png"}])

    fake_pdf = tmp_path / "fake.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4 stub")

    from src.forms.supplier_quote_parser import (
        _parse_supplier_quote_vision)
    out = _parse_supplier_quote_vision(str(fake_pdf))
    assert out is None


# ─── vision_parser._call_vision_api ───────────────────────────────

def test_vision_parser_call_vision_api_skipped(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    _stub_api_quota(monkeypatch, can_call_returns=False)

    fake_req = MagicMock()
    fake_req.post = MagicMock(
        side_effect=AssertionError("POST should not run"))
    monkeypatch.setattr("src.forms.vision_parser._requests", fake_req)

    from src.forms.vision_parser import _call_vision_api
    out = _call_vision_api(
        page_images=[{"base64": "x", "media_type": "image/png"}])
    assert out is None
    fake_req.post.assert_not_called()


# ─── vision_parser.parse_from_text ────────────────────────────────

def test_vision_parser_parse_from_text_skipped(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    _stub_api_quota(monkeypatch, can_call_returns=False)

    fake_req = MagicMock()
    fake_req.post = MagicMock(
        side_effect=AssertionError("POST should not run"))
    monkeypatch.setattr("src.forms.vision_parser._requests", fake_req)

    from src.forms.vision_parser import parse_from_text
    out = parse_from_text(
        text="this is the body of a procurement document " * 10)
    assert out is None
    fake_req.post.assert_not_called()


# ─── vision_parser.extract_email_from_screenshot ──────────────────

def test_vision_parser_extract_email_skipped(monkeypatch, tmp_path):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    _stub_api_quota(monkeypatch, can_call_returns=False)

    fake_req = MagicMock()
    fake_req.post = MagicMock(
        side_effect=AssertionError("POST should not run"))
    monkeypatch.setattr("src.forms.vision_parser._requests", fake_req)

    # Stub the image-to-base64 helper so we never touch a real file.
    monkeypatch.setattr(
        "src.forms.vision_parser._image_file_to_base64",
        lambda p: {"base64": "x", "media_type": "image/png"})

    from src.forms.vision_parser import extract_email_from_screenshot
    out = extract_email_from_screenshot(str(tmp_path / "fake.png"))
    assert out is None
    fake_req.post.assert_not_called()


# ─── item_link_lookup ─────────────────────────────────────────────

def test_item_link_lookup_amazon_skipped(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    _stub_api_quota(monkeypatch, can_call_returns=False)

    def fail_post(*a, **kw):
        raise AssertionError("POST should not run")

    monkeypatch.setattr(
        "src.agents.item_link_lookup.requests.post", fail_post)

    from src.agents.item_link_lookup import claude_amazon_lookup
    out = claude_amazon_lookup("B07XYZTEST")
    # Returns {} on skip — matches existing "API HTTP error" path.
    assert out == {}


def test_item_link_lookup_semantic_match_skipped(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    _stub_api_quota(monkeypatch, can_call_returns=False)

    def fail_post(*a, **kw):
        raise AssertionError("POST should not run")

    monkeypatch.setattr(
        "src.agents.item_link_lookup.requests.post", fail_post)

    from src.agents.item_link_lookup import claude_semantic_match
    out = claude_semantic_match(
        pc_description="3M Promogran Matrix Wound Dressing",
        found_title="3M Promogran Matrix Sterile",
        found_price=87.41)
    assert out["ok"] is False
    assert "quota_exceeded" in out["reasoning"]


def test_item_link_lookup_product_lookup_skipped(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    _stub_api_quota(monkeypatch, can_call_returns=False)

    def fail_post(*a, **kw):
        raise AssertionError("POST should not run")

    monkeypatch.setattr(
        "src.agents.item_link_lookup.requests.post", fail_post)

    from src.agents.item_link_lookup import claude_product_lookup
    out = claude_product_lookup(
        url="https://www.amazon.com/dp/B07XYZTEST",
        supplier="amazon")
    assert out == {}


# ─── log_call telemetry — one representative caller ───────────────

def test_log_call_records_tokens_after_success(monkeypatch):
    """End-to-end: a successful call records tokens_in/tokens_out
    from `data["usage"]` (REST shape) into api_quota.log_call."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    fake_quota = _stub_api_quota(monkeypatch, can_call_returns=True)
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = {
        "model": "claude-haiku-4-5-20251001",
        "usage": {"input_tokens": 1234, "output_tokens": 567},
        "content": [{"type": "text", "text": '{"match": false, '
                                              '"confidence": 0.4, '
                                              '"reason": "different"}'}],
    }

    monkeypatch.setattr(
        "src.agents.item_link_lookup.requests.post",
        lambda *a, **kw: fake_resp)

    from src.agents.item_link_lookup import claude_semantic_match
    out = claude_semantic_match(
        pc_description="X", found_title="X", found_price=10.0)
    # Real call should have run and parsed the JSON response.
    assert out["ok"] is True or out["is_match"] is False

    # Telemetry fired with token counts from REST `data["usage"]`.
    found = False
    for call in fake_quota.log_call.call_args_list:
        args, kw = call
        if (kw.get("tokens_in") == 1234
                and kw.get("tokens_out") == 567
                and kw.get("model") == "claude-haiku-4-5-20251001"):
            found = True
            break
    assert found, (
        f"expected log_call(tokens_in=1234, tokens_out=567, "
        f"model='claude-haiku-...') in {fake_quota.log_call.call_args_list}")
