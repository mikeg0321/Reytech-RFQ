"""Plan §3.1 close — Diagnostics card on /health/quoting.

Folds the cheap half of /api/agents/health-sweep (QB config + DB
table grade) into the page so the operator no longer needs to know
the legacy diagnostic URL. The expensive 14-endpoint batch-test
stays on a button (synchronous render must not make HTTP self-calls).
"""
import pytest


def test_diagnostics_card_shape_smoke(temp_data_dir):
    """The card builder must always return the documented shape so the
    template never throws UndefinedError. Smoke-checks every key and the
    row schema."""
    from src.api.modules.routes_health import _build_diagnostics_card
    out = _build_diagnostics_card()

    assert "status" in out and out["status"] in ("healthy", "warn", "error", "unknown")
    assert "total" in out and isinstance(out["total"], int) and out["total"] > 0
    assert "healthy" in out and isinstance(out["healthy"], int)
    assert "grade" in out and out["grade"] in ("A", "B", "C", "F")
    assert isinstance(out["rows"], list)
    for r in out["rows"]:
        assert "name" in r and r["name"]
        assert "ok" in r and isinstance(r["ok"], bool)
        assert "detail" in r  # may be empty string but key present


def test_diagnostics_card_includes_qb_and_core_tables(temp_data_dir):
    """Card must enumerate QuickBooks + the 5 core DB tables. If the set
    drifts, the operator's diagnostics surface silently shrinks."""
    from src.api.modules.routes_health import _build_diagnostics_card
    out = _build_diagnostics_card()
    names = {r["name"] for r in out["rows"]}
    expected = {"QuickBooks", "database", "catalog", "quotes", "orders", "price_checks"}
    assert expected.issubset(names), \
        f"Diagnostics card missing checks: {expected - names}"


def test_diagnostics_card_grade_matches_healthy_ratio(temp_data_dir):
    """Grade A iff every check passes; B iff at most 1 fails; C/F otherwise."""
    from src.api.modules.routes_health import _build_diagnostics_card
    out = _build_diagnostics_card()
    if out["healthy"] == out["total"]:
        assert out["grade"] == "A" and out["status"] == "healthy"
    elif out["healthy"] >= out["total"] - 1:
        assert out["grade"] == "B" and out["status"] == "warn"
    else:
        assert out["grade"] in ("C", "F") and out["status"] == "error"


def test_diagnostics_card_keyed_in_quoting_page(client):
    """The /health/quoting page must include the diagnostics key in
    its render data so the template doesn't fall through to default
    empty state silently."""
    r = client.get("/health/quoting")
    assert r.status_code == 200
    body = r.data.decode("utf-8", errors="replace")
    # The card title must appear in the rendered HTML
    assert "Diagnostics —" in body, \
        "Diagnostics card title not found in /health/quoting render"


def test_diagnostics_in_json_endpoint(client):
    """The JSON variant /api/health/quoting must include the diagnostics
    key — external monitors consume this shape."""
    r = client.get("/api/health/quoting")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert "diagnostics" in body
    dx = body["diagnostics"]
    assert dx["total"] >= 6  # QB + 5 DB checks at minimum
    assert dx["grade"] in ("A", "B", "C", "F")
