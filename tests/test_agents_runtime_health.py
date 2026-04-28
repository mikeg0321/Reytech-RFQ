"""Tests for `_build_agents_runtime_health` — the /health/quoting card
that aggregates `get_agent_status()` across all runtime agents
(Plan §4.3 sub-4).

The card surfaces the AGENT-LAYER status reporters: a failing import
or get_agent_status() = the agent module is broken on the runtime,
not just missing config. Tests lock the normalization invariants so
a future agent renaming or status-shape tweak doesn't silently break
the card.
"""
from __future__ import annotations

import pytest


def _build():
    from src.api.modules.routes_health import _build_agents_runtime_health
    return _build_agents_runtime_health()


# ── Smoke / shape ───────────────────────────────────────────────────────


def test_returns_one_row_per_known_agent():
    """Every agent in _AGENTS_WITH_STATUS gets exactly one row."""
    from src.api.modules.routes_health import _AGENTS_WITH_STATUS
    out = _build()
    assert out["total"] == len(_AGENTS_WITH_STATUS)
    assert len(out["agents"]) == len(_AGENTS_WITH_STATUS)
    names = {a["name"] for a in out["agents"]}
    assert names == set(_AGENTS_WITH_STATUS)


def test_per_agent_shape_is_stable():
    """Templates rely on these keys — any rename is a breaking change."""
    out = _build()
    for a in out["agents"]:
        for k in ("name", "status", "detail", "ok", "error", "payload"):
            assert k in a, f"agent {a.get('name')} missing {k}"


def test_overall_status_among_known_buckets():
    out = _build()
    assert out["status"] in ("healthy", "warn", "error", "unknown")


def test_healthy_count_plus_errored_count_equals_total():
    out = _build()
    assert out["healthy"] + out["errored"] == out["total"]


# ── Real-world: every shipped agent should report ok ───────────────────


def test_no_agent_errors_on_fresh_test_db():
    """A green test DB should produce zero errored agents — anything
    failing here means an agent's get_agent_status() crashes on
    import or with empty data, which IS a real boot regression."""
    out = _build()
    if out["errored"] > 0:
        broken = [
            f"{a['name']}: {a['error']}"
            for a in out["agents"] if not a["ok"]
        ]
        pytest.fail(
            f"{out['errored']} agent(s) errored on green test DB:\n  "
            + "\n  ".join(broken)
        )


# ── Detail-string priority ──────────────────────────────────────────────


def test_detail_string_uses_priority_keys_when_present(monkeypatch):
    """The detail string should pick from _AGENT_DETAIL_KEYS in order.
    Patch one agent's get_agent_status to a known shape and confirm."""
    import sys
    import types
    from src.api.modules import routes_health as _rh

    fake = types.ModuleType("src.agents.cs_agent")
    fake.get_agent_status = lambda: {
        "status": "ok",
        "pending_cs_drafts": 7,   # priority 1
        "outbox_total": 99,       # priority 4 — should NOT win
    }
    monkeypatch.setitem(sys.modules, "src.agents.cs_agent", fake)
    out = _build()
    cs = next(a for a in out["agents"] if a["name"] == "cs_agent")
    assert "pending_cs_drafts=7" in cs["detail"]
    assert cs["ok"] is True


def test_falls_back_to_capabilities_count_when_no_counter(monkeypatch):
    """If the payload has no recognized counter key but exposes
    capabilities (cs_agent's shape), surface the count rather than
    leaving detail blank — operators get something useful."""
    import sys
    import types

    fake = types.ModuleType("src.agents.cs_agent")
    fake.get_agent_status = lambda: {
        "status": "active",
        "capabilities": ["a", "b", "c"],
    }
    monkeypatch.setitem(sys.modules, "src.agents.cs_agent", fake)
    out = _build()
    cs = next(a for a in out["agents"] if a["name"] == "cs_agent")
    assert cs["detail"] == "3 capabilities"


# ── Defensive: broken agent doesn't take down the card ─────────────────


def test_agent_raising_on_status_call_is_isolated(monkeypatch):
    """If one agent's get_agent_status() raises, the card still
    surfaces the other 13 — a single broken runner can't crash
    /health/quoting."""
    import sys
    import types

    fake = types.ModuleType("src.agents.cs_agent")

    def _explode():
        raise RuntimeError("simulated agent failure")
    fake.get_agent_status = _explode
    monkeypatch.setitem(sys.modules, "src.agents.cs_agent", fake)
    out = _build()
    cs = next(a for a in out["agents"] if a["name"] == "cs_agent")
    assert cs["ok"] is False
    assert "simulated agent failure" in cs["error"]
    # Other agents still report.
    others = [a for a in out["agents"] if a["name"] != "cs_agent"]
    assert any(a["ok"] for a in others), \
        "all agents errored — defensive isolation broken"


def test_agent_returning_non_dict_marked_error(monkeypatch):
    """Some legacy reporters might return None. Mark it errored
    rather than crashing on .get() downstream."""
    import sys
    import types

    fake = types.ModuleType("src.agents.cs_agent")
    fake.get_agent_status = lambda: None
    monkeypatch.setitem(sys.modules, "src.agents.cs_agent", fake)
    out = _build()
    cs = next(a for a in out["agents"] if a["name"] == "cs_agent")
    assert cs["ok"] is False
    assert "non-dict" in cs["error"].lower()


def test_overall_status_is_error_when_5plus_agents_broken(monkeypatch):
    """Threshold check: 1-4 broken = warn; ≥5 = error (something
    systemic broke)."""
    import sys
    import types
    from src.api.modules.routes_health import _AGENTS_WITH_STATUS

    def _explode():
        raise RuntimeError("simulated")

    # Break the first 5 agents.
    for name in _AGENTS_WITH_STATUS[:5]:
        fake = types.ModuleType(f"src.agents.{name}")
        fake.get_agent_status = _explode
        monkeypatch.setitem(sys.modules, f"src.agents.{name}", fake)
    out = _build()
    assert out["status"] == "error"
    assert out["errored"] >= 5


# ── /health/quoting integration ─────────────────────────────────────────


def test_health_quoting_json_includes_agents_runtime(auth_client):
    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "agents_runtime" in data
    ar = data["agents_runtime"]
    assert "agents" in ar
    assert "status" in ar
    assert "total" in ar


def test_health_quoting_html_renders_agents_runtime_card(auth_client):
    resp = auth_client.get("/health/quoting")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "Agent runtime" in body
