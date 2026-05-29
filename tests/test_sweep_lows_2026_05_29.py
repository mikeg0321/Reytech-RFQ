"""Bug-sweep low-severity cleanups (2026-05-29).

1. /api/oracle/item-history with the __markup_only__ sentinel description hung the
   markup-config widget (full quotes+KB scan that matched nothing). Now short-circuits.
2. /favicon.ico had no handler → 404 'Failed to load resource' on every page console.
   Now returns 204.
"""
from __future__ import annotations


def test_oracle_item_history_markup_only_short_circuits(auth_client):
    r = auth_client.get("/api/oracle/item-history?agency=CSP-Sacramento&description=__markup_only__&threshold=2.0")
    assert r.status_code == 200, r.data
    d = r.get_json()
    assert d["ok"] is True
    assert d["matches"]["quotes"] == [] and d["matches"]["kb"] == []
    assert d["stats"]["matches_total"] == 0
    assert "markup-only" in (d.get("note") or "")


def test_oracle_item_history_still_requires_real_params(auth_client):
    # guard: the short-circuit didn't break the required-param check
    r = auth_client.get("/api/oracle/item-history?agency=&description=")
    assert r.status_code == 400


def test_favicon_returns_204_not_404(anon_client):
    r = anon_client.get("/favicon.ico")
    assert r.status_code == 204, f"favicon should be 204, got {r.status_code}"
