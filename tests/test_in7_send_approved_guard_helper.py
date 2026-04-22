"""IN-7 regression: shared `send_approved_guard_ok()` helper + killswitch
parity across all three live outbound-email routes.

Before 2026-04-22:
  - routes_intel_ops.api_outbox_send_all had its own inlined flag check.
  - routes_growth_prospects.api_growth_outreach had a second, slightly
    different inlined check.
  - routes_growth_prospects.api_growth_distro_campaign had NO check at
    all — the distro endpoint bypassed the killswitch that the outreach
    endpoint above it honored.

The audit called this out as a "killswitch inconsistency." Fix: one
shared helper in src/core/flags.py that every live-send route must call.

This suite locks three things in:
  1. `send_approved_guard_ok` exists in src.core.flags and behaves
     correctly (allow when flag on, deny when flag off, fail-closed
     when the DB errors).
  2. All three route modules import and call the helper — no in-file
     copies of `get_flag("outbox.send_approved_enabled", ...)` remain.
  3. The distro-campaign endpoint — previously unguarded — now calls
     the helper too.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest


ROOT = Path(__file__).resolve().parents[1]
FLAGS_PY = ROOT / "src" / "core" / "flags.py"
INTEL_OPS_PY = ROOT / "src" / "api" / "modules" / "routes_intel_ops.py"
GROWTH_PY = ROOT / "src" / "api" / "modules" / "routes_growth_prospects.py"


# ── Helper behavior ────────────────────────────────────────────────────

def test_helper_returns_allow_when_flag_true():
    from src.core.flags import send_approved_guard_ok
    with patch("src.core.flags.get_flag", return_value=True):
        ok, blocked = send_approved_guard_ok(label="Test action")
    assert ok is True
    assert blocked is None


def test_helper_returns_deny_payload_when_flag_false():
    from src.core.flags import send_approved_guard_ok
    with patch("src.core.flags.get_flag", return_value=False):
        ok, blocked = send_approved_guard_ok(label="Test action")
    assert ok is False
    assert isinstance(blocked, dict)
    assert blocked["ok"] is False
    assert "Test action" in blocked["error"], (
        "Blocked payload must name the action for specific logs / UI."
    )
    assert blocked.get("blocked_reason") == "ux_audit_p0_2"


def test_helper_fails_closed_when_flag_layer_raises():
    """If the DB / flag layer itself blows up, the helper must deny,
    not accidentally open the killswitch."""
    from src.core.flags import send_approved_guard_ok

    def _boom(*a, **kw):
        raise RuntimeError("flag layer exploded")

    with patch("src.core.flags.get_flag", side_effect=_boom):
        ok, blocked = send_approved_guard_ok(label="Risky action")
    assert ok is False
    assert isinstance(blocked, dict)
    assert "Risky action" in blocked["error"]


# ── Call-site wiring ───────────────────────────────────────────────────

def test_intel_ops_route_uses_shared_helper():
    """routes_intel_ops must import and call send_approved_guard_ok,
    not re-inline its own get_flag check."""
    src = INTEL_OPS_PY.read_text(encoding="utf-8")
    assert "send_approved_guard_ok" in src, (
        "IN-7 regressed: routes_intel_ops.py no longer references "
        "send_approved_guard_ok — send-approved endpoint lost the "
        "shared killswitch helper."
    )
    # The inlined pre-IN-7 flag check should be gone.
    assert 'get_flag("outbox.send_approved_enabled"' not in src, (
        "IN-7 regressed: routes_intel_ops.py re-inlined "
        "get_flag(\"outbox.send_approved_enabled\", ...) instead of "
        "calling send_approved_guard_ok."
    )


def test_growth_outreach_uses_shared_helper():
    """routes_growth_prospects' outreach endpoint must use the helper."""
    src = GROWTH_PY.read_text(encoding="utf-8")
    assert "send_approved_guard_ok" in src, (
        "IN-7 regressed: routes_growth_prospects.py lost the helper import."
    )
    assert 'get_flag("outbox.send_approved_enabled"' not in src, (
        "IN-7 regressed: routes_growth_prospects.py re-inlined the flag "
        "check instead of calling send_approved_guard_ok."
    )


def test_distro_campaign_gates_live_sends():
    """The distro-campaign endpoint previously bypassed the killswitch.
    It must now call the shared helper for live (dry_run=false) sends."""
    src = GROWTH_PY.read_text(encoding="utf-8")
    # Find the distro-campaign route and the first helper call after it.
    idx = src.find("api_growth_distro_campaign")
    assert idx != -1, "distro-campaign route function missing"
    tail = src[idx : idx + 2000]
    assert "send_approved_guard_ok" in tail, (
        "IN-7 regressed: api_growth_distro_campaign no longer calls "
        "send_approved_guard_ok — distro sends will bypass the "
        "killswitch that outreach sends respect."
    )
    assert "Distro-list campaign" in tail, (
        "Distro-campaign guard label drifted — breaks log/UI specificity."
    )


def test_helper_is_exported():
    """send_approved_guard_ok must be in flags.py's __all__ so wildcard
    imports pick it up and so `from src.core.flags import *` in future
    code doesn't lose the helper."""
    src = FLAGS_PY.read_text(encoding="utf-8")
    # __all__ block must list the helper
    assert '"send_approved_guard_ok"' in src, (
        "send_approved_guard_ok missing from flags.py __all__"
    )
