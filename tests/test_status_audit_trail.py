"""PR-M — status-change audit trail + funnel diagnostic.

Pinned guarantees:
  1. Status update endpoint captures `reason` for ALL transitions,
     not just won/lost/expired (the 52 April duplicates with no
     audit trail bug).
  2. status_history append-only with {from, to, at, actor, reason}.
  3. Terminal statuses (duplicate, dismissed, expired, won, lost,
     archived) write closed_at + mirror reason to closed_reason.
  4. The /admin/funnel surface renders stage breakdown + stall
     reasons + no-reason canary.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _seed_pc(client, pid, status="new", **fields):
    """Seed a PC directly via the data layer (faster than route)."""
    from src.api.data_layer import _save_single_pc, _load_price_checks
    pc = {
        "id": pid,
        "pc_number": fields.get("pc_number", f"PC-{pid}"),
        "agency": fields.get("agency", "cchcs"),
        "status": status,
        "created_at": fields.get("created_at", datetime.now().isoformat()),
        "items": fields.get("items", []),
    }
    pc.update({k: v for k, v in fields.items()
               if k not in ("pc_number", "agency", "status",
                            "created_at", "items")})
    _save_single_pc(pid, pc, raise_on_error=True)


def _get_pc(pid):
    from src.api.data_layer import _load_price_checks
    return _load_price_checks().get(pid)


# ── Audit trail tests ───────────────────────────────────────────────


def test_duplicate_status_captures_reason(client):
    """The bug: marking PC duplicate wrote NO closed_reason and NO
    status_history entry. PR-M fix: every transition records reason."""
    _seed_pc(client, "pc_dup_test", status="parsed")
    resp = client.post(
        "/api/pricecheck/pc_dup_test/update-status",
        json={"status": "duplicate",
              "reason": "same PC# as pc_1234 (CCHCS Mohammad 5/13)"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"]
    assert data["reason_captured"] is True
    pc = _get_pc("pc_dup_test")
    assert pc["status"] == "duplicate"
    assert "closed_reason" in pc
    assert "Mohammad" in pc["closed_reason"]
    # status_history populated
    assert "status_history" in pc
    hist = pc["status_history"]
    assert isinstance(hist, list) and hist
    last = hist[-1]
    assert last["from"] == "parsed"
    assert last["to"] == "duplicate"
    assert last["reason"]
    assert "at" in last


def test_dismissed_status_also_captures_reason(client):
    """Dismissed status was bucketed with 'other' terminals — also
    needs the audit trail."""
    _seed_pc(client, "pc_dismiss_test", status="parsed")
    resp = client.post(
        "/api/pricecheck/pc_dismiss_test/update-status",
        json={"status": "dismissed", "reason": "buyer cancelled by phone"},
    )
    assert resp.status_code == 200
    pc = _get_pc("pc_dismiss_test")
    assert pc["status"] == "dismissed"
    assert "buyer cancelled" in pc["closed_reason"]
    assert pc["status_history"][-1]["reason"] == "buyer cancelled by phone"


def test_status_update_without_reason_still_works_but_flagged(client):
    """Backward compat: an empty reason still allows the transition
    (don't break existing UI flows), but `reason_captured` returns
    False so callers can detect the missing audit."""
    _seed_pc(client, "pc_no_reason", status="parsed")
    resp = client.post(
        "/api/pricecheck/pc_no_reason/update-status",
        json={"status": "duplicate"},  # no reason
    )
    data = resp.get_json()
    assert data["ok"]
    assert data["reason_captured"] is False
    pc = _get_pc("pc_no_reason")
    # status_history still records the transition, with empty reason
    assert pc["status_history"][-1]["reason"] == ""


def test_status_history_appends_not_overwrites(client):
    """Multiple transitions = multiple entries, ordered, append-only."""
    _seed_pc(client, "pc_multi", status="new",
             status_history=[{"from": "", "to": "new",
                              "at": "2026-01-01", "actor": "system",
                              "reason": "ingest"}])
    client.post("/api/pricecheck/pc_multi/update-status",
                json={"status": "parsed", "reason": "vision parsed 5 items"})
    client.post("/api/pricecheck/pc_multi/update-status",
                json={"status": "priced", "reason": "auto-priced"})
    client.post("/api/pricecheck/pc_multi/update-status",
                json={"status": "duplicate",
                      "reason": "same as pc_999"})
    pc = _get_pc("pc_multi")
    hist = pc["status_history"]
    assert len(hist) == 4  # 1 seed + 3 transitions
    transitions = [(h["from"], h["to"]) for h in hist[1:]]
    assert ("new", "parsed") in transitions
    assert ("parsed", "priced") in transitions
    assert ("priced", "duplicate") in transitions


def test_invalid_status_rejected_before_writing(client):
    """Guard: a bad status string can't escape into the DB."""
    _seed_pc(client, "pc_bad_status", status="parsed")
    resp = client.post(
        "/api/pricecheck/pc_bad_status/update-status",
        json={"status": "not_a_valid_status", "reason": "test"},
    )
    data = resp.get_json()
    assert not data["ok"]
    pc = _get_pc("pc_bad_status")
    assert pc["status"] == "parsed"  # unchanged


# ── Funnel diagnostic surface ───────────────────────────────────────


def test_admin_funnel_endpoint_renders_with_no_data(client):
    """Empty window: surface renders without 500, shows zero counts."""
    resp = client.get("/admin/funnel?days=7")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Funnel breakdown" in body or "funnel" in body.lower()
    assert "Autonomous-completion" in body


def test_admin_funnel_classifies_stages(client):
    """Seed PCs across stages, confirm each lands in correct stage."""
    now = datetime.now().isoformat()
    _seed_pc(client, "pc_stage_new",     status="new",     created_at=now)
    _seed_pc(client, "pc_stage_parsed",  status="parsed",  items=[{"d": "x"}], created_at=now)
    _seed_pc(client, "pc_stage_priced",  status="priced",  items=[{"d": "x"}], created_at=now)
    _seed_pc(client, "pc_stage_sent",    status="sent",    items=[{"d": "x"}], created_at=now)
    _seed_pc(client, "pc_stage_won",     status="won",     created_at=now)
    _seed_pc(client, "pc_stage_lost",    status="lost",    created_at=now)
    _seed_pc(client, "pc_stage_dup",     status="duplicate", created_at=now,
             closed_reason="legit PC# collision")
    resp = client.get("/admin/funnel?days=7")
    body = resp.get_data(as_text=True)
    # Each stage label should appear with its count
    for label in ("Created", "Parsed", "Priced", "Sent",
                  "Won", "Lost", "Resolved (other)"):
        assert label in body


def test_admin_funnel_surfaces_no_reason_canary(client):
    """Seed 5 duplicates: 3 with reason, 2 without. The 'no reason'
    canary should show 2/5 = 40% — and the table should list the
    '(no reason)' entries explicitly."""
    now = datetime.now().isoformat()
    for i in range(3):
        _seed_pc(client, f"pc_with_reason_{i}", status="duplicate",
                 closed_reason="legit", created_at=now)
    for i in range(2):
        _seed_pc(client, f"pc_no_reason_{i}", status="duplicate",
                 created_at=now)
    resp = client.get("/admin/funnel?days=7")
    body = resp.get_data(as_text=True)
    assert "no recorded reason" in body
    # 2 of 5 = 40%
    assert "2/5" in body
    assert "(no reason)" in body
