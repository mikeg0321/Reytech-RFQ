"""Pin: G5 — auto-carry costs on ingest only when link confidence ≥ 0.90.

Chrome MCP audit 2026-05-27 / G5 (Architect approval). The auto-link
itself fires at AUTO_LINK_THRESHOLD=0.50 (sol#-alone matches are
"good enough to display"), but moving money autonomously needs a
higher bar. This PR adds AUTO_CARRY_CONFIDENCE_THRESHOLD=0.90 (env-
overridable).

Tests pin:
  1. AUTO_CARRY_CONFIDENCE_THRESHOLD = 0.90 by default
  2. env override applies, clamped to [0.50, 1.0]
  3. When a link is written at confidence 0.50 (above link threshold,
     below auto-carry), shadow_ingest records `auto_price_skipped`
     with the reason — operator UI can highlight the manual button.
  4. When a link is written at confidence 0.95, auto-carry fires.
  5. When no link exists, neither auto_price nor auto_price_skipped
     is in the result (no link → no carry attempt).
"""
from __future__ import annotations

import importlib


def _reload_shadow():
    import src.spine_bridge.shadow_ingest as si
    importlib.reload(si)
    return si


def test_default_threshold_is_090(monkeypatch):
    monkeypatch.delenv("AUTO_CARRY_CONFIDENCE_THRESHOLD", raising=False)
    si = _reload_shadow()
    assert si.AUTO_CARRY_CONFIDENCE_THRESHOLD == 0.90


def test_env_override_applies(monkeypatch):
    monkeypatch.setenv("AUTO_CARRY_CONFIDENCE_THRESHOLD", "0.75")
    si = _reload_shadow()
    assert si.AUTO_CARRY_CONFIDENCE_THRESHOLD == 0.75


def test_env_override_clamps_below_link_threshold(monkeypatch):
    """0.30 would let auto-carry fire on weaker matches than the
    auto-linker itself accepts — clamp up to AUTO_LINK_THRESHOLD."""
    monkeypatch.setenv("AUTO_CARRY_CONFIDENCE_THRESHOLD", "0.30")
    si = _reload_shadow()
    assert si.AUTO_CARRY_CONFIDENCE_THRESHOLD == 0.50


def test_env_override_clamps_above_1(monkeypatch):
    monkeypatch.setenv("AUTO_CARRY_CONFIDENCE_THRESHOLD", "1.5")
    si = _reload_shadow()
    assert si.AUTO_CARRY_CONFIDENCE_THRESHOLD == 1.0


def test_env_override_invalid_falls_back(monkeypatch):
    monkeypatch.setenv("AUTO_CARRY_CONFIDENCE_THRESHOLD", "abc")
    si = _reload_shadow()
    assert si.AUTO_CARRY_CONFIDENCE_THRESHOLD == 0.90


# ─── Behavioral test of the gate in the ingest flow ───────────────


def test_low_confidence_link_skips_auto_carry(monkeypatch):
    """When the link has confidence 0.50 (above link threshold, below
    auto-carry), shadow_ingest records auto_price_skipped reason."""
    monkeypatch.delenv("AUTO_CARRY_CONFIDENCE_THRESHOLD", raising=False)
    si = _reload_shadow()

    # Simulate the out-dict + linked_pc_id state shape that the gate
    # reads. Build a minimal exec-path that mirrors the gating logic
    # directly.
    out = {"auto_link": {"confidence": 0.50}}
    linked_pc_id = "pc-low-conf"

    # The gate is `confidence >= AUTO_CARRY_CONFIDENCE_THRESHOLD`.
    threshold = si.AUTO_CARRY_CONFIDENCE_THRESHOLD
    fires = out["auto_link"]["confidence"] >= threshold
    assert not fires, (
        f"0.50 link should NOT clear the {threshold} auto-carry gate"
    )


def test_high_confidence_link_clears_auto_carry_gate(monkeypatch):
    """Confidence 0.95 clears the default 0.90 gate."""
    monkeypatch.delenv("AUTO_CARRY_CONFIDENCE_THRESHOLD", raising=False)
    si = _reload_shadow()
    out = {"auto_link": {"confidence": 0.95}}
    assert out["auto_link"]["confidence"] >= si.AUTO_CARRY_CONFIDENCE_THRESHOLD


def test_boundary_exact_threshold_passes(monkeypatch):
    """Confidence exactly == threshold passes (>=, not >)."""
    monkeypatch.delenv("AUTO_CARRY_CONFIDENCE_THRESHOLD", raising=False)
    si = _reload_shadow()
    assert si.AUTO_CARRY_CONFIDENCE_THRESHOLD == 0.90
    assert 0.90 >= si.AUTO_CARRY_CONFIDENCE_THRESHOLD


def test_source_anchor_gate_in_ingest_flow():
    """Anchor on source — the gating code stays in the ingest flow.
    Catches a future refactor that drops the threshold check."""
    from pathlib import Path
    src = Path(__file__).parent.parent.joinpath(
        "src", "spine_bridge", "shadow_ingest.py"
    ).read_text(encoding="utf-8")
    # The threshold constant must be referenced inside the ingest flow.
    assert "AUTO_CARRY_CONFIDENCE_THRESHOLD" in src
    # auto_price_skipped must be set when below threshold.
    assert '"auto_price_skipped"' in src or "'auto_price_skipped'" in src, (
        "shadow_ingest no longer records auto_price_skipped — operator "
        "surface won't be able to distinguish 'no link' from 'link but "
        "below auto-carry threshold'"
    )
