"""Pin Tier 3c Anthropic quota gate (audit 2026-05-07).

Until this tier shipped, only `src/agents/item_identifier.py` called
`api_quota.can_call("grok")` — and even there the result was
log-only, not enforced. ~14 Anthropic call sites had no daily $ cap
at all, so a runaway agent (infinite-retry bug, wedge in
buyer_reply_diff loop) could burn unbounded spend.

This module pins the quota substrate behavior:
  1. `check_quota()` returns None when quota is open (proceed) and
     a skip-reason string when exhausted.
  2. `log_call()` records cost/usage rows in `api_usage` and never
     raises — quota tracking must NEVER break a real business path.
  3. Both helpers fail-open on api_quota module-level errors.
  4. Service constant is "claude" so call sites can't drift.
  5. Integration: `extract_quote_diff` (Tier 3b cached path) checks
     quota before calling Claude and surfaces the skip-reason
     through its existing skipped_reason contract.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


# ─── check_quota ──────────────────────────────────────────────────

def test_check_quota_returns_none_when_under_cap(monkeypatch):
    """Open quota → caller should proceed (None == OK)."""
    from src.core import anthropic_quota
    fake_api = MagicMock()
    fake_api.can_call = MagicMock(return_value=True)
    monkeypatch.setattr("src.core.api_quota.api_quota", fake_api)
    assert anthropic_quota.check_quota(agent="x") is None


def test_check_quota_returns_skip_reason_when_exhausted(monkeypatch):
    """Closed quota → caller gets a skip-reason string."""
    from src.core import anthropic_quota
    fake_api = MagicMock()
    fake_api.can_call = MagicMock(return_value=False)
    monkeypatch.setattr("src.core.api_quota.api_quota", fake_api)
    out = anthropic_quota.check_quota(agent="form_profiler")
    assert out is not None
    assert "claude" in out
    assert "quota" in out.lower()


def test_check_quota_uses_claude_service_constant(monkeypatch):
    """The service argument passed to api_quota.can_call MUST be
    `"claude"` — the daily summary aggregates per service, and call
    sites drifting to `"Claude"` or `"claude-haiku"` would split
    the bucket."""
    from src.core import anthropic_quota
    captured = {}
    fake_api = MagicMock()

    def fake_can_call(service):
        captured["service"] = service
        return True

    fake_api.can_call = fake_can_call
    monkeypatch.setattr("src.core.api_quota.api_quota", fake_api)
    anthropic_quota.check_quota(agent="x")
    assert captured["service"] == "claude"


def test_check_quota_fails_open_on_api_quota_error(monkeypatch):
    """If `api_quota.can_call` raises (DB locked, etc.), the gate
    must return None so business work proceeds. A broken tracker
    must NEVER block revenue."""
    from src.core import anthropic_quota
    fake_api = MagicMock()
    fake_api.can_call = MagicMock(side_effect=Exception("DB locked"))
    monkeypatch.setattr("src.core.api_quota.api_quota", fake_api)
    assert anthropic_quota.check_quota(agent="x") is None


# ─── log_call ─────────────────────────────────────────────────────

def test_log_call_passes_tokens_and_model_to_api_quota(monkeypatch):
    from src.core import anthropic_quota
    captured = {}

    fake_api = MagicMock()

    def fake_log_call(service, **kwargs):
        captured["service"] = service
        captured.update(kwargs)

    fake_api.log_call = fake_log_call
    monkeypatch.setattr("src.core.api_quota.api_quota", fake_api)

    anthropic_quota.log_call(
        agent="form_profiler",
        tokens_in=1500,
        tokens_out=300,
        response_time_ms=842,
        model="claude-sonnet-4-6",
    )
    assert captured["service"] == "claude"
    assert captured["agent"] == "form_profiler"
    assert captured["tokens_in"] == 1500
    assert captured["tokens_out"] == 300
    assert captured["response_time_ms"] == 842
    assert captured["model"] == "claude-sonnet-4-6"


def test_log_call_fails_open_on_db_error(monkeypatch, caplog):
    """`api_quota.log_call` raising must not propagate — telemetry
    is best-effort."""
    from src.core import anthropic_quota
    fake_api = MagicMock()
    fake_api.log_call = MagicMock(side_effect=RuntimeError("disk full"))
    monkeypatch.setattr("src.core.api_quota.api_quota", fake_api)
    # Must not raise.
    anthropic_quota.log_call(agent="x", model="m")


def test_log_call_normalizes_none_inputs(monkeypatch):
    """None inputs should be coerced to 0 / "" — keeps INSERT happy."""
    from src.core import anthropic_quota
    captured = {}

    fake_api = MagicMock()
    fake_api.log_call = lambda service, **kw: captured.update(kw)
    monkeypatch.setattr("src.core.api_quota.api_quota", fake_api)

    # All optional args omitted.
    anthropic_quota.log_call(agent="")
    assert captured["tokens_in"] == 0
    assert captured["tokens_out"] == 0
    assert captured["error"] == ""
    assert captured["model"] == ""


def test_extract_usage_handles_missing_attrs():
    """`_extract_usage` must not crash on response objects that
    lack `.usage` (older SDK / mock). Returns (0, 0, "")."""
    from src.core.anthropic_quota import _extract_usage
    resp = MagicMock(spec=[])  # no attrs
    ti, to, m = _extract_usage(resp)
    assert ti == 0
    assert to == 0
    assert m == ""


def test_extract_usage_pulls_token_counts():
    from src.core.anthropic_quota import _extract_usage
    resp = MagicMock()
    resp.usage = MagicMock(input_tokens=1234, output_tokens=567)
    resp.model = "claude-sonnet-4-6"
    ti, to, m = _extract_usage(resp)
    assert ti == 1234
    assert to == 567
    assert m == "claude-sonnet-4-6"


# ─── Integration: buyer_reply_diff respects the gate ─────────────

def test_buyer_reply_diff_skipped_when_quota_exhausted(monkeypatch):
    """End-to-end: when quota is closed, `extract_quote_diff` returns
    its empty-diff with the canonical skip-reason and never invokes
    Claude. Pre-Tier-3c, every "Extract changes" click would burn
    a Claude call regardless of daily spend."""
    from src.agents.buyer_reply_diff import extract_quote_diff

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    fake_api = MagicMock()
    fake_api.can_call = MagicMock(return_value=False)
    monkeypatch.setattr("src.core.api_quota.api_quota", fake_api)

    invoked = {"n": 0}

    def fake_invoke(**kwargs):
        invoked["n"] += 1
        return {}

    monkeypatch.setattr(
        "src.agents.buyer_reply_diff._invoke_llm_diff", fake_invoke)

    diff, reason = extract_quote_diff(
        "buyer asks for a discount",
        [{"description": "Widget", "qty": 10, "unit_price": 5.0}],
    )
    assert reason is not None
    assert "claude" in reason.lower()
    assert "quota" in reason.lower()
    assert invoked["n"] == 0  # NEVER called the LLM


def test_buyer_reply_diff_logs_call_after_success(monkeypatch):
    """When the call succeeds, `log_call` records the usage row so
    the daily summary reflects the spend."""
    from src.agents.buyer_reply_diff import extract_quote_diff

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    fake_api = MagicMock()
    fake_api.can_call = MagicMock(return_value=True)
    log_calls = []

    def fake_log(service, **kw):
        log_calls.append({"service": service, **kw})

    fake_api.log_call = fake_log
    monkeypatch.setattr("src.core.api_quota.api_quota", fake_api)

    # Stub the SDK boundary — `_invoke_llm_diff` is what hits Claude.
    # Build a fake response object with usage attrs so the post-call
    # `log_call` records token counts.
    class FakeBlock:
        type = "tool_use"
        name = "record_quote_diff"
        input = {"price_changes": [], "qty_changes": [],
                 "items_added": [], "items_removed": [], "notes": []}

    fake_resp = MagicMock()
    fake_resp.content = [FakeBlock()]
    fake_resp.id = "msg_test_123"
    fake_resp.usage = MagicMock(input_tokens=500, output_tokens=200)
    fake_resp.model = "claude-sonnet-4-6"

    fake_client = MagicMock()
    fake_client.messages.create.return_value = fake_resp

    import sys
    import types
    fake_anthropic = types.ModuleType("anthropic")
    fake_anthropic.Anthropic = lambda **kw: fake_client
    monkeypatch.setitem(sys.modules, "anthropic", fake_anthropic)

    diff, reason = extract_quote_diff(
        "buyer asks for a discount",
        [{"description": "Widget", "qty": 10, "unit_price": 5.0}],
    )
    assert reason is None
    # Telemetry recorded.
    assert any(c["service"] == "claude"
               and c.get("agent") == "buyer_reply_diff"
               and c.get("tokens_in") == 500
               and c.get("tokens_out") == 200
               for c in log_calls), f"no claude log_call: {log_calls}"
