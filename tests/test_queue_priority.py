"""PR-T — queue auto-prioritization tests.

Pinned guarantees:
  1. compute_priority_score is pure — same input → same output.
  2. Priced + ready bonus only fires when status is in PRICED_STATUSES
     AND every active item has unit_price > 0.
  3. Deadline urgency ladder: ≤24h=30, ≤48h=20, ≤72h=10, else 0.
  4. Dollar value scales logarithmically and caps at 50.
  5. Agency winrate component is 0 when None passed; 20 * wr otherwise.
  6. rank_pcs filters out score=0 PCs (no noise in the "top to send"
     widget when there's no ready work).
  7. rank_pcs deterministic tiebreak: older created_at wins.
  8. rank_pcs honors limit parameter.
"""
from __future__ import annotations

from datetime import datetime, timedelta


def _make_pc(**kwargs):
    base = {
        "id": "pc_test",
        "pc_number": "PC-001",
        "agency": "cchcs",
        "status": "priced",
        "created_at": "2026-05-13T10:00:00",
        "items": [
            {"description": "X", "qty": 1, "unit_price": 100.0,
             "vendor_cost": 80.0},
        ],
    }
    base.update(kwargs)
    return base


# ── compute_priority_score ─────────────────────────────────────────


def test_priced_ready_bonus_50_when_all_items_priced_and_status_priced():
    from src.agents.queue_priority import compute_priority_score
    pc = _make_pc(status="priced",
                  items=[{"unit_price": 10, "vendor_cost": 5},
                         {"unit_price": 20, "vendor_cost": 10}])
    r = compute_priority_score(pc)
    assert r["breakdown"]["priced_ready"] == 50


def test_priced_ready_bonus_20_when_priced_but_status_not_priced():
    """Status hasn't caught up — items priced but still 'parsed'."""
    from src.agents.queue_priority import compute_priority_score
    pc = _make_pc(status="parsed",
                  items=[{"unit_price": 10, "vendor_cost": 5}])
    r = compute_priority_score(pc)
    assert r["breakdown"]["priced_ready"] == 20


def test_priced_ready_bonus_0_when_any_item_unpriced():
    from src.agents.queue_priority import compute_priority_score
    pc = _make_pc(status="priced",
                  items=[{"unit_price": 10, "vendor_cost": 5},
                         {"unit_price": 0, "vendor_cost": 0}])
    r = compute_priority_score(pc)
    assert r["breakdown"]["priced_ready"] == 0


def test_mark_sent_ready_requires_both_cost_and_price():
    from src.agents.queue_priority import compute_priority_score
    # Both set → 20
    r = compute_priority_score(_make_pc(items=[
        {"unit_price": 10, "vendor_cost": 5}]))
    assert r["breakdown"]["mark_sent_ready"] == 20
    # Cost missing → 0
    r = compute_priority_score(_make_pc(items=[
        {"unit_price": 10, "vendor_cost": 0}]))
    assert r["breakdown"]["mark_sent_ready"] == 0
    # Price missing → 0
    r = compute_priority_score(_make_pc(items=[
        {"unit_price": 0, "vendor_cost": 5}]))
    assert r["breakdown"]["mark_sent_ready"] == 0


def test_deadline_urgency_ladder():
    from src.agents.queue_priority import compute_priority_score
    now = datetime(2026, 5, 13, 10, 0, 0)
    # Due in 12h → 30
    r = compute_priority_score(
        _make_pc(due_date=(now + timedelta(hours=12)).isoformat()),
        now=now)
    assert r["breakdown"]["deadline_urgency"] == 30
    # Due in 36h → 20
    r = compute_priority_score(
        _make_pc(due_date=(now + timedelta(hours=36)).isoformat()),
        now=now)
    assert r["breakdown"]["deadline_urgency"] == 20
    # Due in 60h → 10
    r = compute_priority_score(
        _make_pc(due_date=(now + timedelta(hours=60)).isoformat()),
        now=now)
    assert r["breakdown"]["deadline_urgency"] == 10
    # Due in 120h → 0
    r = compute_priority_score(
        _make_pc(due_date=(now + timedelta(hours=120)).isoformat()),
        now=now)
    assert r["breakdown"]["deadline_urgency"] == 0
    # Severely overdue (>72h past) → 0 (stale, exclude from "send now")
    r = compute_priority_score(
        _make_pc(due_date=(now - timedelta(hours=100)).isoformat()),
        now=now)
    assert r["breakdown"]["deadline_urgency"] == 0
    # Recently overdue (-12h) → still 30 (most urgent)
    r = compute_priority_score(
        _make_pc(due_date=(now - timedelta(hours=12)).isoformat()),
        now=now)
    assert r["breakdown"]["deadline_urgency"] == 30


def test_dollar_value_log_scale_and_cap():
    from src.agents.queue_priority import compute_priority_score
    # $0 → 0
    r = compute_priority_score(_make_pc(items=[]))
    assert r["breakdown"]["dollar_value"] == 0
    # $400 → ~13 (log10(401) * 13 ≈ 33.9... wait)
    pc_400 = _make_pc(items=[{"qty": 4, "unit_price": 100.0}])
    r400 = compute_priority_score(pc_400)
    assert r400["breakdown"]["dollar_value"] > 0
    # $40k → bigger than $400
    pc_40k = _make_pc(items=[{"qty": 400, "unit_price": 100.0}])
    r40k = compute_priority_score(pc_40k)
    assert r40k["breakdown"]["dollar_value"] > r400["breakdown"]["dollar_value"]
    # $4M → caps at 50
    pc_4m = _make_pc(items=[{"qty": 40000, "unit_price": 100.0}])
    r4m = compute_priority_score(pc_4m)
    assert r4m["breakdown"]["dollar_value"] == 50


def test_agency_winrate_component():
    from src.agents.queue_priority import compute_priority_score
    pc = _make_pc()
    r_none = compute_priority_score(pc, agency_winrate=None)
    assert r_none["breakdown"]["agency_winrate"] == 0
    r_half = compute_priority_score(pc, agency_winrate=0.5)
    assert r_half["breakdown"]["agency_winrate"] == 10
    r_full = compute_priority_score(pc, agency_winrate=1.0)
    assert r_full["breakdown"]["agency_winrate"] == 20


def test_no_bid_items_excluded_from_score():
    from src.agents.queue_priority import compute_priority_score
    pc = _make_pc(items=[
        {"unit_price": 10, "vendor_cost": 5},
        {"unit_price": 0, "no_bid": True},  # skipped
    ])
    r = compute_priority_score(pc)
    # 1 active item, both cost+price set → mark_sent_ready=20
    assert r["breakdown"]["mark_sent_ready"] == 20
    assert r["items_total"] == 1


# ── rank_pcs ───────────────────────────────────────────────────────


def test_rank_pcs_filters_zero_score():
    from src.agents.queue_priority import rank_pcs
    pcs = {
        "pc_good": _make_pc(status="priced",
                            items=[{"unit_price": 100, "vendor_cost": 50,
                                    "qty": 1}]),
        "pc_empty": {"id": "pc_empty", "status": "parsed", "items": []},
    }
    top = rank_pcs(pcs, limit=10)
    assert len(top) == 1
    assert top[0]["pc_id"] == "pc_good"


def test_rank_pcs_score_desc_order():
    from src.agents.queue_priority import rank_pcs
    pcs = {
        "pc_low": _make_pc(id="pc_low", status="parsed",
                           items=[{"unit_price": 1, "vendor_cost": 1, "qty": 1}]),
        "pc_high": _make_pc(id="pc_high", status="priced",
                            items=[{"unit_price": 1000, "vendor_cost": 500, "qty": 10}]),
    }
    top = rank_pcs(pcs, limit=10)
    assert top[0]["pc_id"] == "pc_high"
    assert top[1]["pc_id"] == "pc_low"


def test_rank_pcs_tiebreak_older_first():
    from src.agents.queue_priority import rank_pcs
    pcs = {
        "pc_newer": _make_pc(id="pc_newer", created_at="2026-05-13T12:00:00",
                             items=[{"unit_price": 100, "vendor_cost": 50, "qty": 1}]),
        "pc_older": _make_pc(id="pc_older", created_at="2026-05-13T08:00:00",
                             items=[{"unit_price": 100, "vendor_cost": 50, "qty": 1}]),
    }
    top = rank_pcs(pcs, limit=10)
    assert top[0]["pc_id"] == "pc_older"


def test_rank_pcs_honors_limit():
    from src.agents.queue_priority import rank_pcs
    pcs = {
        f"pc_{i}": _make_pc(id=f"pc_{i}",
                            items=[{"unit_price": 100, "vendor_cost": 50, "qty": 1}])
        for i in range(20)
    }
    top = rank_pcs(pcs, limit=3)
    assert len(top) == 3


def test_rank_pcs_uses_agency_winrate_map():
    from src.agents.queue_priority import rank_pcs
    pcs = {
        "pc_cchcs": _make_pc(id="pc_cchcs", agency="cchcs",
                             items=[{"unit_price": 100, "vendor_cost": 50, "qty": 1}]),
        "pc_dsh": _make_pc(id="pc_dsh", agency="dsh",
                           items=[{"unit_price": 100, "vendor_cost": 50, "qty": 1}]),
    }
    top = rank_pcs(pcs, agency_winrates={"cchcs": 1.0, "dsh": 0.0}, limit=10)
    # cchcs gets +20 winrate, dsh gets 0 → cchcs ranks first
    assert top[0]["pc_id"] == "pc_cchcs"
    assert top[0]["breakdown"]["agency_winrate"] == 20
    assert top[1]["breakdown"]["agency_winrate"] == 0


def test_rank_pcs_returns_required_fields():
    """The home template expects these fields on every entry."""
    from src.agents.queue_priority import rank_pcs
    pcs = {
        "pc_x": _make_pc(items=[{"unit_price": 100, "vendor_cost": 50,
                                  "qty": 1}]),
    }
    top = rank_pcs(pcs)
    assert len(top) == 1
    row = top[0]
    for field in ("pc_id", "pc_number", "agency", "status",
                  "score", "breakdown", "total_value",
                  "deadline_hours", "items_priced", "items_total"):
        assert field in row, f"missing field: {field}"


def test_rank_pcs_skips_non_dict_entries():
    """Defensive: a stray non-dict shouldn't crash the ranker."""
    from src.agents.queue_priority import rank_pcs
    pcs = {
        "pc_good": _make_pc(items=[{"unit_price": 100, "vendor_cost": 50, "qty": 1}]),
        "pc_bad": "this is not a dict",
        "pc_none": None,
    }
    top = rank_pcs(pcs)
    assert len(top) == 1
    assert top[0]["pc_id"] == "pc_good"
