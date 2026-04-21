"""Observability tests for the CCHCS PC→RFQ handoff.

Two surfaces are covered:

  1. Activity logging — confirming a PC→RFQ link must write a
     `pc_rfq_linked` entry to the CRM activity feed (both JSON +
     SQLite activity_log) so ops can see the handoff in the unified
     timeline. Metadata carries the full promote + reprice result so
     downstream dashboards don't have to re-scan the RFQ.

  2. Health endpoint `/api/health/pc-rfq-link` — returns ok:true with
     24h counts + currently-unresolved qty drift. Defensive: must never
     500 even when data is partial, so ops can poll it every minute
     without false alerts.
"""
from __future__ import annotations

import json
import os

import pytest


@pytest.fixture(autouse=True)
def _isolated_crm_log(temp_data_dir, monkeypatch):
    """Redirect dashboard.CRM_LOG_FILE to the per-test temp dir.

    CRM_LOG_FILE is computed at module-import time from the ORIGINAL
    DATA_DIR, so monkeypatching dashboard.DATA_DIR (in the app fixture)
    doesn't redirect it — activity from one test would persist into the
    next. Patch the constant itself to pin isolation.
    """
    from src.api import dashboard
    isolated = os.path.join(temp_data_dir, "crm_activity.json")
    monkeypatch.setattr(dashboard, "CRM_LOG_FILE", isolated)
    # Clear the json-load cache so the first read sees our empty file,
    # not a stale cached list.
    try:
        dashboard._invalidate_cache(isolated)
    except Exception:
        pass
    yield isolated


def _write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f)


def _cchcs_rfq(rid="rfq-obs-1", lines=None, linked_pc_id=None):
    r = {
        "id": rid,
        "solicitation_number": "PC-2026-OBS-RFQ",
        "requestor_email": "buyer@cchcs.ca.gov",
        "institution": "CCHCS",
        "agency": "CCHCS",
        "status": "new",
        "line_items": lines if lines is not None else [
            {"mfg_number": "W12919", "description": "BP cuff", "quantity": 10},
            {"mfg_number": "FN4368", "description": "Gloves", "quantity": 100},
        ],
    }
    if linked_pc_id:
        r["linked_pc_id"] = linked_pc_id
    return r


def _cchcs_pc(pcid="pc-obs", pc_number="PC-2026-OBS"):
    return {
        "id": pcid,
        "pc_number": pc_number,
        "agency": "CCHCS",
        "institution": "California Correctional Health Care Services",
        "requestor": "buyer@cchcs.ca.gov",
        "items": [
            {"mfg_number": "W12919", "description": "BP cuff adult",
             "quantity": 10, "unit_price": 45.00, "supplier_cost": 25.00,
             "bid_price": 45.00, "markup_pct": 80},
            {"mfg_number": "FN4368", "description": "Gloves nitrile",
             "quantity": 50, "unit_price": 18.50, "supplier_cost": 10.00,
             "bid_price": 18.50, "markup_pct": 85},
        ],
    }


# ── Activity logging ──────────────────────────────────────────────────────

def test_confirm_pc_link_writes_crm_activity_entry(
    auth_client, temp_data_dir
):
    """After confirm-pc-link, crm_activity.json must carry a
    `pc_rfq_linked` row with full promote metadata — that's what the
    ops timeline + health endpoint both read."""
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), {rfq["id"]: rfq})
    _write_json(os.path.join(temp_data_dir, "price_checks.json"), {pc["id"]: pc})

    resp = auth_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": pc["id"], "reprice": False},
    )
    assert resp.status_code == 200

    # Inspect the activity feed directly — this is what the timeline UI
    # and /api/health/pc-rfq-link will both read.
    from src.api.data_layer import _load_crm_activity
    activity = _load_crm_activity() or []
    link_events = [a for a in activity
                   if a.get("event_type") == "pc_rfq_linked"]
    assert len(link_events) == 1
    event = link_events[0]
    # Description carries human-readable summary for the timeline
    assert "PC-2026-OBS" in event["description"]
    assert "ported" in event["description"]
    # Metadata carries structured data for dashboards — must include
    # promote result so we don't have to re-scan the RFQ to report counts
    meta = event.get("metadata") or {}
    assert meta.get("rfq_id") == rfq["id"]
    assert meta.get("pc_id") == pc["id"]
    assert meta.get("promote", {}).get("promoted") == 2
    assert meta.get("promote", {}).get("qty_changed") == 1
    # reprice=False → no reprice block recorded
    assert meta.get("reprice") is None
    assert meta.get("reprice_requested") is False


def test_confirm_pc_link_activity_includes_reprice_metadata(
    auth_client, temp_data_dir, monkeypatch
):
    """When reprice=true, the activity metadata must include the reprice
    result so the health endpoint can surface 24h reprice counts without
    re-scanning RFQ line items."""
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), {rfq["id"]: rfq})
    _write_json(os.path.join(temp_data_dir, "price_checks.json"), {pc["id"]: pc})

    # Stub oracle so the test is deterministic — the fixture DB may or
    # may not have the gloves catalog row.
    import src.core.pricing_oracle_v2 as _poll
    monkeypatch.setattr(_poll, "get_pricing", lambda **kw: {
        "recommendation": {"quote_price": 14.50, "markup_pct": 60.0},
        "cost": {"locked_cost": 9.0},
    })

    resp = auth_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": pc["id"], "reprice": True},
    )
    assert resp.status_code == 200

    from src.api.data_layer import _load_crm_activity
    activity = _load_crm_activity() or []
    event = next(a for a in activity
                 if a.get("event_type") == "pc_rfq_linked")
    meta = event.get("metadata") or {}
    assert meta.get("reprice_requested") is True
    rep = meta.get("reprice") or {}
    assert rep.get("repriced") == 1
    assert rep.get("skipped_no_change") == 1
    # Description must carry the reprice summary so the timeline shows
    # it without the reader expanding metadata
    assert "repriced" in event["description"]


# ── /api/health/pc-rfq-link ───────────────────────────────────────────────

def test_health_endpoint_returns_ok_with_empty_data(auth_client, temp_data_dir):
    """Defensive contract: no RFQs, no activity → endpoint still ok:true
    with zeros. Ops must be able to poll every minute without false alerts."""
    resp = auth_client.get("/api/health/pc-rfq-link")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["links_24h"] == 0
    assert body["reprices_24h"] == 0
    assert body["skipped_no_price_24h"] == 0
    assert body["cchcs_linked_total"] == 0
    assert body["cchcs_unlinked_total"] == 0
    assert body["unresolved_qty_drift"] == 0
    assert body["recent_links"] == []


def test_health_endpoint_counts_cchcs_link_state(auth_client, temp_data_dir):
    """RFQ scan: linked CCHCS RFQs counted separately from unlinked ones.
    Non-CCHCS RFQs excluded from both buckets."""
    rfqs = {
        "linked-1": _cchcs_rfq("linked-1", linked_pc_id="pc-1"),
        "linked-2": _cchcs_rfq("linked-2", linked_pc_id="pc-2"),
        "unlinked-1": _cchcs_rfq("unlinked-1"),
        "other-agency": {
            "id": "other-agency", "agency": "CalVet",
            "institution": "Veterans Home",
            "line_items": [],
        },
    }
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), rfqs)

    resp = auth_client.get("/api/health/pc-rfq-link")
    body = resp.get_json()
    assert body["cchcs_linked_total"] == 2
    assert body["cchcs_unlinked_total"] == 1


def test_health_endpoint_counts_unresolved_qty_drift(auth_client, temp_data_dir):
    """Lines with qty_changed=True and no qty_change reprice are the
    "needs manual pricing" backlog. Lines that have been repriced are
    NOT counted."""
    rfq = _cchcs_rfq(lines=[
        # Drifted, not yet repriced → counts
        {"mfg_number": "A", "description": "a", "quantity": 100,
         "qty_changed": True, "pc_original_qty": 50},
        # Drifted but already repriced → excluded
        {"mfg_number": "B", "description": "b", "quantity": 100,
         "qty_changed": False, "repriced_reason": "qty_change"},
        # Clean line → excluded
        {"mfg_number": "C", "description": "c", "quantity": 10},
    ])
    rfq["linked_pc_id"] = "pc-xyz"
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), {rfq["id"]: rfq})

    resp = auth_client.get("/api/health/pc-rfq-link")
    body = resp.get_json()
    assert body["unresolved_qty_drift"] == 1


def test_health_endpoint_counts_24h_link_activity(
    auth_client, temp_data_dir, monkeypatch
):
    """24h windows: a confirm-pc-link call writes a pc_rfq_linked event,
    which must show up in links_24h + recent_links immediately."""
    rfq = _cchcs_rfq()
    pc = _cchcs_pc()
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), {rfq["id"]: rfq})
    _write_json(os.path.join(temp_data_dir, "price_checks.json"), {pc["id"]: pc})

    # Stub oracle so reprice=true produces deterministic counts.
    import src.core.pricing_oracle_v2 as _poll
    monkeypatch.setattr(_poll, "get_pricing", lambda **kw: {
        "recommendation": {"quote_price": 14.50, "markup_pct": 60.0},
        "cost": {"locked_cost": 9.0},
    })

    confirm = auth_client.post(
        f"/api/rfq/{rfq['id']}/confirm-pc-link",
        json={"pc_id": pc["id"], "reprice": True},
    )
    assert confirm.status_code == 200

    resp = auth_client.get("/api/health/pc-rfq-link")
    body = resp.get_json()
    assert body["ok"] is True
    assert body["links_24h"] == 1
    assert body["reprices_24h"] == 1
    assert body["skipped_no_price_24h"] == 0
    assert len(body["recent_links"]) == 1
    recent = body["recent_links"][0]
    assert "PC-2026-OBS" in recent["description"]
    assert recent["timestamp"]  # non-empty ISO timestamp


def test_health_endpoint_ignores_old_link_events(
    auth_client, _isolated_crm_log
):
    """Events older than 24h must not inflate links_24h. recent_links is
    the full history (capped at 5), but the count is strictly 24h."""
    from src.api.data_layer import _log_crm_activity
    # Old event (2 days ago) — should NOT count in links_24h. Inject via
    # the real logger then rewrite the timestamp so the SQLite dual-write
    # doesn't need mocking.
    _log_crm_activity(
        "old-rfq", "pc_rfq_linked", "old link",
        actor="user", metadata={"rfq_id": "old-rfq"},
    )
    crm_path = _isolated_crm_log
    with open(crm_path) as f:
        events = json.load(f)
    from datetime import datetime, timedelta
    events[-1]["timestamp"] = (datetime.now() - timedelta(days=2)).isoformat()
    _write_json(crm_path, events)
    # Clear the load cache so our rewrite is seen.
    from src.api import dashboard
    try:
        dashboard._invalidate_cache(crm_path)
    except Exception:
        pass

    resp = auth_client.get("/api/health/pc-rfq-link")
    body = resp.get_json()
    assert body["links_24h"] == 0  # old event excluded
    # But recent_links still shows it (history list, not 24h-scoped)
    assert len(body["recent_links"]) == 1


def test_health_endpoint_requires_auth(anon_client):
    resp = anon_client.get("/api/health/pc-rfq-link")
    assert resp.status_code in (401, 403)
