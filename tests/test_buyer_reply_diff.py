"""Contract tests for src/agents/buyer_reply_diff.py.

Pins the helper boundary + skip-reason matrix for buyer-reply diff
extraction (PR-F1). Mirrors the patch-the-LLM-boundary pattern from
test_compliance_validator.

Contracts:
  * _normalize_items strips items down to {line_no, description, qty,
    unit_price} and skips non-dict rows.
  * The tool schema has the five bucket keys + 'notes', all required.
  * Skip reasons fire for: no reply text, no current items, missing
    API key, missing anthropic SDK, LLM exception.
  * Successful extraction returns (diff, None) with all 5 buckets
    present even if empty.
  * The diff is capped at 50 entries per bucket (defensive against
    runaway model output).
  * `_empty=True` when every bucket is empty.
"""
from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mod():
    if "src.agents.buyer_reply_diff" in sys.modules:
        del sys.modules["src.agents.buyer_reply_diff"]
    return importlib.import_module("src.agents.buyer_reply_diff")


# ─── _normalize_items ────────────────────────────────────────────────


def test_normalize_strips_to_canonical_fields(mod):
    items = [
        {"line_number": 1, "description": "Widget A",
         "qty": 10, "unit_price": 5.50,
         "catalog_match": "ignored"},
        {"line_no": 2, "description": "Widget B",
         "quantity": "3", "price_per_unit": "12.34"},
    ]
    out = mod._normalize_items(items)
    assert out == [
        {"line_no": 1, "description": "Widget A",
         "qty": 10.0, "unit_price": 5.50},
        {"line_no": 2, "description": "Widget B",
         "qty": 3.0, "unit_price": 12.34},
    ]


def test_normalize_handles_non_dict_rows(mod):
    items = [None, "garbage", {"description": "ok", "qty": 1,
                                "unit_price": 1}]
    out = mod._normalize_items(items)
    assert len(out) == 1
    assert out[0]["description"] == "ok"


def test_normalize_assigns_line_no_when_missing(mod):
    items = [{"description": "A", "qty": 1, "unit_price": 1},
             {"description": "B", "qty": 1, "unit_price": 1}]
    out = mod._normalize_items(items)
    assert [x["line_no"] for x in out] == [1, 2]


# ─── tool schema ─────────────────────────────────────────────────────


def test_tool_schema_has_required_buckets(mod):
    s = mod._build_tool_schema()
    assert s["name"] == "record_quote_diff"
    required = s["input_schema"]["required"]
    assert set(required) == {
        "price_changes", "qty_changes",
        "items_added", "items_removed", "notes",
    }


# ─── skip reasons ────────────────────────────────────────────────────


def test_extract_skips_when_reply_text_empty(mod):
    diff, reason = mod.extract_quote_diff(
        "", [{"description": "x", "qty": 1, "unit_price": 1}])
    assert reason == "no reply text"
    assert diff["_empty"] is True
    # Buckets always present even when skipped
    for k in ("price_changes", "qty_changes",
              "items_added", "items_removed", "notes"):
        assert k in diff


def test_extract_skips_when_current_items_empty(mod):
    diff, reason = mod.extract_quote_diff("buyer wrote some text", [])
    assert reason == "no current items"


def test_extract_skips_when_api_key_missing(mod, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    diff, reason = mod.extract_quote_diff(
        "please reduce qty",
        [{"description": "x", "qty": 5, "unit_price": 1}])
    assert reason == "ANTHROPIC_API_KEY not set"


def test_extract_skips_when_anthropic_sdk_missing(mod, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    with patch.object(mod, "_invoke_llm_diff",
                      side_effect=ImportError("no anthropic")):
        diff, reason = mod.extract_quote_diff(
            "please reduce qty",
            [{"description": "x", "qty": 5, "unit_price": 1}])
    assert reason == "anthropic SDK not installed"


def test_extract_carries_llm_exception_in_reason(mod, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    with patch.object(mod, "_invoke_llm_diff",
                      side_effect=RuntimeError("rate limit")):
        diff, reason = mod.extract_quote_diff(
            "please reduce qty",
            [{"description": "x", "qty": 5, "unit_price": 1}])
    assert reason.startswith("LLM call failed: RuntimeError: ")
    assert "rate limit" in reason


# ─── successful extraction ───────────────────────────────────────────


def test_extract_returns_full_diff_shape(mod, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    fake_diff = {
        "price_changes": [{"line_no": 1, "requested_unit_price": 4.50,
                           "rationale": "wants 4.50"}],
        "qty_changes": [{"line_no": 2, "requested_qty": 8,
                         "rationale": "wants 8 not 10"}],
        "items_added": [],
        "items_removed": [],
        "notes": [{"category": "deadline",
                   "note": "needs by Friday"}],
        "_response_id": "msg_abc",
    }
    with patch.object(mod, "_invoke_llm_diff", return_value=fake_diff):
        diff, reason = mod.extract_quote_diff(
            "Can I get item 1 at $4.50 and only 8 of item 2? Need by Friday.",
            [
                {"description": "Widget A", "qty": 10, "unit_price": 5.0,
                 "line_number": 1},
                {"description": "Widget B", "qty": 10, "unit_price": 12.0,
                 "line_number": 2},
            ])

    assert reason is None
    assert diff["_empty"] is False
    assert diff["_response_id"] == "msg_abc"
    assert len(diff["price_changes"]) == 1
    assert diff["price_changes"][0]["line_no"] == 1
    assert diff["price_changes"][0]["requested_unit_price"] == 4.50
    assert len(diff["qty_changes"]) == 1
    assert diff["qty_changes"][0]["requested_qty"] == 8
    assert diff["notes"][0]["category"] == "deadline"


def test_extract_empty_diff_marked(mod, monkeypatch):
    """A polite 'thanks for the quote' reply produces empty buckets;
    the wrapper sets _empty=True so the UI can collapse the panel."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    with patch.object(mod, "_invoke_llm_diff",
                      return_value={"price_changes": [], "qty_changes": [],
                                    "items_added": [], "items_removed": [],
                                    "notes": [], "_response_id": "msg_z"}):
        diff, reason = mod.extract_quote_diff(
            "Thanks, I'll review and get back to you.",
            [{"description": "x", "qty": 1, "unit_price": 1}])

    assert reason is None
    assert diff["_empty"] is True


def test_extract_caps_runaway_buckets_at_50(mod, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    huge = {
        "price_changes": [{"line_no": i, "requested_unit_price": 1.0,
                           "rationale": "x"} for i in range(120)],
        "qty_changes": [],
        "items_added": [],
        "items_removed": [],
        "notes": [],
        "_response_id": "msg_huge",
    }
    with patch.object(mod, "_invoke_llm_diff", return_value=huge):
        diff, reason = mod.extract_quote_diff(
            "rampaging buyer email",
            [{"description": "x", "qty": 1, "unit_price": 1}])

    assert reason is None
    assert len(diff["price_changes"]) == 50


def test_extract_drops_non_list_bucket_values(mod, monkeypatch):
    """Defensive: if the LLM returns a wrong-type value for a bucket
    (e.g. None or string), we keep the empty list default rather
    than blowing up the iteration in the caller."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    bad = {
        "price_changes": None,
        "qty_changes": "not a list",
        "items_added": [],
        "items_removed": [],
        "notes": [],
        "_response_id": "msg_bad",
    }
    with patch.object(mod, "_invoke_llm_diff", return_value=bad):
        diff, reason = mod.extract_quote_diff(
            "buyer text",
            [{"description": "x", "qty": 1, "unit_price": 1}])

    assert reason is None
    assert diff["price_changes"] == []
    assert diff["qty_changes"] == []


# ─── _invoke_llm_diff Anthropic boundary ─────────────────────────────


def test_invoke_llm_diff_calls_anthropic_with_tool_use(mod):
    """Verify the boundary actually constructs a tool_use call and
    parses the tool_use response block — without hitting the network."""
    fake_block = MagicMock()
    fake_block.type = "tool_use"
    fake_block.name = "record_quote_diff"
    fake_block.input = {
        "price_changes": [{"line_no": 1, "requested_unit_price": 5.0,
                           "rationale": "y"}],
        "qty_changes": [],
        "items_added": [],
        "items_removed": [],
        "notes": [],
    }
    fake_resp = MagicMock(content=[fake_block], id="msg_test")
    fake_client = MagicMock()
    fake_client.messages.create.return_value = fake_resp

    fake_anthropic = MagicMock()
    fake_anthropic.Anthropic.return_value = fake_client
    with patch.dict(sys.modules, {"anthropic": fake_anthropic}):
        result = mod._invoke_llm_diff(
            api_key="k",
            reply_text="reduce price",
            current_items=[{"description": "x", "qty": 1,
                            "unit_price": 1}],
        )

    fake_client.messages.create.assert_called_once()
    call_kwargs = fake_client.messages.create.call_args.kwargs
    assert call_kwargs["model"] == "claude-sonnet-4-6"
    assert call_kwargs["tool_choice"]["name"] == "record_quote_diff"
    assert result["_response_id"] == "msg_test"
    assert result["price_changes"][0]["line_no"] == 1
