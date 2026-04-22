"""IN-3 / IN-4 regression guard: ghost-data endpoints must block on prod.

Prior to 2026-04-21 the following endpoints would happily seed fixture PCs,
mutate the quote counter, or load demo buyer rows into the live DB from
production:

  POST /api/test/create-pc
  POST /api/test/cleanup
  POST /api/test/status
  POST /api/test/cleanup-duplicates
  POST /api/test/renumber-quote
  POST /api/test/delete-quotes
  POST /api/intel/seed-demo
  POST /api/debug/fix/seed_demo

All seven are now gated behind ``ENABLE_TEST_ROUTES=1`` when running on
Railway (``RAILWAY_ENVIRONMENT`` or ``PORT`` set). This test flips the
Railway flag on without the opt-in and confirms each route returns 403
with ``error == "disabled_on_production"``.
"""
from __future__ import annotations

import pytest


GATED_GETS = [
    "/api/test/create-pc",
    "/api/test/cleanup",
    "/api/test/status",
    "/api/test/cleanup-duplicates",
    "/api/test/renumber-quote?old=R26Q1&new=R26Q99",
    "/api/test/delete-quotes?numbers=R26Q1",
]


@pytest.mark.parametrize("path", GATED_GETS)
def test_test_routes_blocked_on_prod(auth_client, monkeypatch, path):
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.delenv("ENABLE_TEST_ROUTES", raising=False)
    resp = auth_client.get(path)
    assert resp.status_code == 403, f"{path} not gated: {resp.status_code}"
    body = resp.get_json() or {}
    assert body.get("error") == "disabled_on_production", body


def test_seed_demo_intel_route_blocked_on_prod(auth_client, monkeypatch):
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.delenv("ENABLE_TEST_ROUTES", raising=False)
    resp = auth_client.post("/api/intel/seed-demo")
    assert resp.status_code == 403
    assert (resp.get_json() or {}).get("error") == "disabled_on_production"


def test_debug_fix_seed_demo_blocked_on_prod(auth_client, monkeypatch):
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.delenv("ENABLE_TEST_ROUTES", raising=False)
    resp = auth_client.post("/api/debug/fix/seed_demo")
    assert resp.status_code == 403
    assert (resp.get_json() or {}).get("error") == "disabled_on_production"


def test_opt_in_env_re_enables(auth_client, monkeypatch):
    """ENABLE_TEST_ROUTES=1 must re-enable the routes so QA can still use them."""
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.setenv("ENABLE_TEST_ROUTES", "1")
    # create-pc is the richest side-effect one — just verifying the gate lets
    # traffic through, not that the downstream seed succeeds.
    resp = auth_client.get("/api/test/create-pc")
    # Any non-403 proves the gate was not the blocker.
    assert resp.status_code != 403, resp.get_data(as_text=True)


def test_local_dev_not_blocked(auth_client, monkeypatch):
    """Without RAILWAY_ENVIRONMENT or PORT set, local dev hits the fixture."""
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.delenv("ENABLE_TEST_ROUTES", raising=False)
    resp = auth_client.get("/api/test/status")
    assert resp.status_code != 403
