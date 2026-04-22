"""BUILD-4 P1 regression guards — health gate on stale calibration.

Context: `/health/quoting` returned 13 individual cards but no
single-field status. An external monitor (Railway cron, uptime robot,
Mike's own alert script) had to parse every card to know whether the
system was healthy. The oracle_calibration card already computed
`status == "stale"` when `last_updated` was >14 days old, but nothing
aggregated that signal upward.

BUILD-4 added `_build_health_gate(oracle_calibration)` and surfaces its
result as `gate` on both /health/quoting (template) and
/api/health/quoting (JSON). Monitors can now alert on a single field:
`gate.status != "healthy"` or `gate.healthy == False`.

The gate is deliberately monotonic — adding new card inputs can only
raise severity, never lower it. `_GATE_SEVERITY` is the ranking.

These guards lock: severity ordering, each oracle_calibration status
maps to the right severity, both routes include the gate, and the
staleness threshold on the calibration card itself is still 14 days.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HEALTH_PATH = ROOT / "src" / "api" / "modules" / "routes_health.py"


# ── Gate helper unit tests ──────────────────────────────────────────────────

def test_gate_severity_ordering():
    from src.api.modules.routes_health import _GATE_SEVERITY
    # healthy must be 0, stale must be strictly worse than no_data,
    # degraded strictly worse than stale.
    assert _GATE_SEVERITY["healthy"] == 0
    assert _GATE_SEVERITY["no_data"] == 1
    assert _GATE_SEVERITY["losses_only"] == 1
    assert _GATE_SEVERITY["stale"] == 2
    assert _GATE_SEVERITY["degraded"] == 3
    # Ordering invariants — if a future refactor swaps levels the gate
    # would degrade "stale" silently.
    assert _GATE_SEVERITY["stale"] > _GATE_SEVERITY["no_data"]
    assert _GATE_SEVERITY["degraded"] > _GATE_SEVERITY["stale"]


def test_gate_healthy_when_calibration_fresh():
    from src.api.modules.routes_health import _build_health_gate
    gate = _build_health_gate({
        "status": "healthy",
        "days_since_update": 2,
        "rows": 42,
    })
    assert gate["status"] == "healthy"
    assert gate["severity"] == 0
    assert gate["healthy"] is True
    assert gate["reasons"] == []


def test_gate_flips_stale_when_calibration_stale():
    """The whole point of BUILD-4 — a stale calibration MUST show up on
    the gate so the monitor fires."""
    from src.api.modules.routes_health import _build_health_gate
    gate = _build_health_gate({
        "status": "stale",
        "days_since_update": 21,
        "rows": 42,
    })
    assert gate["status"] == "stale"
    assert gate["severity"] == 2
    assert gate["healthy"] is False
    assert len(gate["reasons"]) == 1
    r = gate["reasons"][0]
    assert r["source"] == "oracle_calibration"
    assert r["status"] == "stale"
    assert r["days_since_update"] == 21


def test_gate_handles_no_data():
    from src.api.modules.routes_health import _build_health_gate
    gate = _build_health_gate({
        "status": "no_data",
        "days_since_update": None,
        "rows": 0,
    })
    assert gate["status"] == "no_data"
    assert gate["severity"] == 1
    assert gate["healthy"] is False
    assert gate["reasons"][0]["status"] == "no_data"


def test_gate_handles_losses_only():
    """losses_only means the feedback loop is firing but we're not
    winning — that's a pricing/market signal, not a pipeline bug.
    Severity 1 surfaces it without crying wolf."""
    from src.api.modules.routes_health import _build_health_gate
    gate = _build_health_gate({
        "status": "losses_only",
        "days_since_update": 3,
        "rows": 10,
    })
    assert gate["status"] == "losses_only"
    assert gate["severity"] == 1
    assert gate["healthy"] is False


def test_gate_handles_missing_input():
    """If _build_oracle_calibration_card() ever returns None (DB
    blown up, table missing), the gate must not crash — observability
    failures can't take down the dashboard."""
    from src.api.modules.routes_health import _build_health_gate
    gate = _build_health_gate(None)
    assert gate["status"] == "healthy"
    assert gate["severity"] == 0
    assert gate["healthy"] is True

    gate = _build_health_gate({})
    assert gate["status"] == "healthy"
    assert gate["severity"] == 0


def test_gate_unknown_status_is_healthy():
    """An unrecognized status (future refactor adds a new card state)
    must default to 0 — fail-open on observability, not fail-closed
    on an unknown label that's probably still fine."""
    from src.api.modules.routes_health import _build_health_gate
    gate = _build_health_gate({"status": "brand_new_status"})
    assert gate["severity"] == 0
    assert gate["healthy"] is True


# ── Source-level wiring guards ──────────────────────────────────────────────

def test_oracle_calibration_staleness_threshold_still_14_days():
    """The card's own is_stale threshold is what the gate reads. A
    regression that loosens the threshold to 30/60/90 days would hide
    staleness from the gate without triggering any test."""
    src = HEALTH_PATH.read_text(encoding="utf-8")
    assert "days_since_update > 14" in src, (
        "BUILD-4: oracle_calibration staleness threshold must remain "
        "14 days. Changing it is a policy change and should update the "
        "gate documentation."
    )


def test_html_route_includes_gate():
    src = HEALTH_PATH.read_text(encoding="utf-8")
    # Find the quoting_health_page function body.
    m = re.search(
        r"def quoting_health_page.*?return render_page",
        src, re.DOTALL,
    )
    assert m, "BUILD-4: quoting_health_page body not found"
    body = m.group(0)
    assert '"gate": _build_health_gate(' in body, (
        "BUILD-4: /health/quoting (HTML) must pass gate to the template "
        "so the dashboard banner can render the top-level status"
    )


def test_json_route_includes_gate():
    src = HEALTH_PATH.read_text(encoding="utf-8")
    m = re.search(
        r"def quoting_health_json.*?(?=\n@bp\.route|\ndef |\Z)",
        src, re.DOTALL,
    )
    assert m, "BUILD-4: quoting_health_json body not found"
    body = m.group(0)
    assert '"gate": _build_health_gate(' in body, (
        "BUILD-4: /api/health/quoting (JSON) must include gate so "
        "external monitors can alert on a single field"
    )


def test_gate_only_computes_calibration_once_per_request():
    """If _build_oracle_calibration_card() were called twice per
    request (once for the card, once for the gate), the DB hit would
    double. The route must compute it once and pass the value into
    the gate helper."""
    src = HEALTH_PATH.read_text(encoding="utf-8")
    # In both route functions the pattern must be: one assignment,
    # one reference to that variable in the gate call.
    for func_name in ["quoting_health_page", "quoting_health_json"]:
        m = re.search(
            rf"def {func_name}.*?(?=\n@bp\.route|\ndef [a-z_]+\()",
            src, re.DOTALL,
        )
        assert m, f"BUILD-4: {func_name} body not found"
        body = m.group(0)
        calls = body.count("_build_oracle_calibration_card()")
        assert calls == 1, (
            f"BUILD-4: {func_name} must call "
            f"_build_oracle_calibration_card() exactly once, found "
            f"{calls} — duplicate calls double the DB cost per request"
        )
        assert "_build_health_gate(oracle_cal)" in body, (
            f"BUILD-4: {func_name} must pass the cached oracle_cal "
            "value into _build_health_gate, not re-fetch it"
        )
