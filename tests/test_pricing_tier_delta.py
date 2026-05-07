"""Contract tests for /api/pricing-tier-delta + /summary
(post-quote item #6, 2026-05-07).

Pins the beacon endpoint behavior:
  * Beacons under the $0.50 noise floor are accepted but NOT logged.
  * Beacons fired faster than the rate-limit window for the same
    record_id silently drop.
  * The endpoint returns 204 even on garbage input — never propagates
    a 500 to the client (fire-and-forget invariant).
  * Summary aggregates by day, by record, max delta, recent 20.
  * Auth: beacon endpoint is open (must accept after session expiry);
    summary endpoint requires auth.
"""
from __future__ import annotations

import json
import os
import time

import pytest


@pytest.fixture
def isolated_log(tmp_path, monkeypatch):
    """Point routes_pricing_tier_delta at a tmp DATA_DIR so each test
    sees a fresh JSONL log file. The route module is loaded via exec()
    into dashboard's globals, so we patch both the module-level
    `_data_dir` (for direct callers) AND the dashboard global (for the
    exec'd copy that the Flask app routes actually call into)."""
    from src.api.modules import routes_pricing_tier_delta as m
    monkeypatch.setattr(m, "_data_dir", lambda: str(tmp_path))
    try:
        from src.api import dashboard as dash
        if hasattr(dash, "_data_dir"):
            monkeypatch.setattr(dash, "_data_dir", lambda: str(tmp_path),
                                raising=False)
    except Exception:
        pass
    m._recent_fires.clear()
    return tmp_path


def _read_log(tmp_path) -> list:
    log_path = tmp_path / "pricing_tier_delta_log.jsonl"
    if not log_path.exists():
        return []
    rows = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


# ─── beacon endpoint ──────────────────────────────────────────────────


def test_beacon_logs_actionable_delta(client, isolated_log):
    payload = {
        "context": "pc", "record_id": "pc_alpha",
        "active_buf": 15, "tier_revenue": 1234.56,
        "row_revenue": 1230.00, "delta": 4.56,
        "tier_cost": 950.00, "item_count": 6,
        "url": "/pricecheck/pc_alpha",
    }
    r = client.post("/api/pricing-tier-delta", json=payload)
    assert r.status_code == 204
    rows = _read_log(isolated_log)
    assert len(rows) == 1
    assert rows[0]["record_id"] == "pc_alpha"
    assert rows[0]["delta"] == 4.56
    assert rows[0]["item_count"] == 6


def test_beacon_drops_below_noise_floor(client, isolated_log):
    payload = {"record_id": "pc_alpha", "delta": 0.49}
    r = client.post("/api/pricing-tier-delta", json=payload)
    assert r.status_code == 204
    assert _read_log(isolated_log) == []


def test_beacon_drops_at_exactly_noise_floor(client, isolated_log):
    """Threshold is strictly greater-than $0.50. $0.50 even = drop.
    This pins the boundary — flipping to >= would log every quote."""
    payload = {"record_id": "pc_alpha", "delta": 0.50}
    client.post("/api/pricing-tier-delta", json=payload)
    assert _read_log(isolated_log) == []


def test_beacon_accepts_negative_delta(client, isolated_log):
    """Tier > row (operator hand-priced lower than markup suggests)
    is just as actionable as tier < row. Use abs in filter."""
    payload = {"record_id": "pc_alpha", "delta": -2.50}
    client.post("/api/pricing-tier-delta", json=payload)
    rows = _read_log(isolated_log)
    assert len(rows) == 1
    assert rows[0]["delta"] == -2.50


def test_beacon_rate_limits_per_record(client, isolated_log):
    """Two beacons within the rate-limit window for the same record
    drop the second. The keystroke-driven panel fires showTierComparison
    on every input event; we don't want a row of identical log entries."""
    payload = {"record_id": "pc_alpha", "delta": 5.00}
    client.post("/api/pricing-tier-delta", json=payload)
    client.post("/api/pricing-tier-delta", json=payload)
    rows = _read_log(isolated_log)
    assert len(rows) == 1


def test_beacon_does_not_rate_limit_different_records(client, isolated_log):
    client.post("/api/pricing-tier-delta",
                json={"record_id": "pc_alpha", "delta": 5.00})
    client.post("/api/pricing-tier-delta",
                json={"record_id": "pc_beta", "delta": 5.00})
    rows = _read_log(isolated_log)
    assert {r["record_id"] for r in rows} == {"pc_alpha", "pc_beta"}


def test_beacon_accepts_text_plain_body(client, isolated_log):
    """sendBeacon defaults to text/plain; the endpoint must parse it
    even though it isn't application/json."""
    payload = {"record_id": "pc_alpha", "delta": 5.00,
               "context": "pc", "active_buf": 15,
               "tier_revenue": 100, "row_revenue": 95,
               "item_count": 3}
    r = client.post(
        "/api/pricing-tier-delta",
        data=json.dumps(payload),
        content_type="text/plain",
    )
    assert r.status_code == 204
    assert len(_read_log(isolated_log)) == 1


def test_beacon_returns_204_on_garbage(client, isolated_log):
    """Fire-and-forget invariant: a malformed beacon must not propagate
    a 5xx to the browser console (would scare operators)."""
    r = client.post("/api/pricing-tier-delta",
                    data="this is not json",
                    content_type="application/json")
    assert r.status_code == 204
    assert _read_log(isolated_log) == []


def test_beacon_without_auth_succeeds(anon_client, isolated_log):
    """The beacon endpoint must accept POSTs even after the session
    expired — otherwise the operator's last edit before logout never
    surfaces in telemetry."""
    payload = {"record_id": "pc_alpha", "delta": 5.00}
    r = anon_client.post("/api/pricing-tier-delta", json=payload)
    assert r.status_code == 204
    assert len(_read_log(isolated_log)) == 1


# ─── summary endpoint ─────────────────────────────────────────────────


def test_summary_empty_when_no_log(client, isolated_log):
    r = client.get("/api/pricing-tier-delta/summary")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["total_logged"] == 0
    assert body["by_day"] == {}


def test_summary_aggregates_by_day_and_record(client, isolated_log):
    # Seed 3 events across 2 records
    for i, rec in enumerate(["pc_a", "pc_a", "pc_b"]):
        from src.api.modules import routes_pricing_tier_delta as m
        m._recent_fires.clear()  # bypass rate limit for test
        client.post("/api/pricing-tier-delta",
                    json={"record_id": rec, "delta": 5.0 + i})

    r = client.get("/api/pricing-tier-delta/summary?days=7")
    body = r.get_json()
    assert body["total_logged"] == 3
    assert body["by_record"]["pc_a"] == 2
    assert body["by_record"]["pc_b"] == 1
    assert body["max_delta"] >= 5.0
    assert len(body["recent"]) == 3


def test_summary_requires_auth(anon_client, isolated_log):
    """Summary surface contains aggregated record_ids — auth-gate it
    even though the beacon side is open."""
    r = anon_client.get("/api/pricing-tier-delta/summary")
    assert r.status_code in (401, 302, 403)


def test_summary_clamps_days_to_max_90(client, isolated_log):
    r = client.get("/api/pricing-tier-delta/summary?days=999")
    body = r.get_json()
    assert body["days"] == 90


def test_summary_clamps_days_to_min_1(client, isolated_log):
    r = client.get("/api/pricing-tier-delta/summary?days=0")
    body = r.get_json()
    assert body["days"] == 1
