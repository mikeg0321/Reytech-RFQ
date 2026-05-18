"""Tests for routes_admin_spine_backfill.

Three endpoints:
- GET  /api/admin/spine/counter/<name>
- POST /api/admin/spine/counter/<name>
- POST /api/admin/spine/backfill-display-numbers

Each is DASH_PASS-gated. The backfill is idempotent: a second run with
no unstamped quotes is a no-op.

Test strategy: seed a temp spine DB with a mix of stamped + unstamped
quotes, point the route at it via SPINE_DB_PATH env, then exercise
each endpoint through the auth_client fixture.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


def _seed_spine_db(db_path: str, *, include_unstamped: int = 2,
                   include_stamped: int = 1) -> dict:
    """Seed a temp spine DB with N unstamped + M stamped quotes.
    Returns the quote_ids in each group."""
    from src.spine import (
        init_db, write_quote, LineItem, Quote,
    )

    init_db(db_path)
    fresh = datetime.now(timezone.utc) - timedelta(days=1)

    def _li():
        return LineItem(
            line_no=1, description="seed item", mfg_number="MFG-X",
            qty=1, uom="EA", cost_cents=5000,
            cost_source_url="https://supplier.example.com/sku",
            cost_validated_at=fresh, unit_price_cents=6750,
        )

    unstamped_ids: list[str] = []
    for i in range(include_unstamped):
        qid = f"legacy-{i:03d}"
        q = Quote(
            quote_id=qid, agency="CCHCS", facility="SATF",
            solicitation_number="10847262", line_items=[_li()],
            tax_rate_bps=825,
        )
        # write_quote auto-assigns quote_seq on first INSERT. To create
        # a deliberately unstamped row, write_quote and then forcibly
        # NULL the seq columns directly in SQLite (mimicking a row
        # that pre-dates PR #1040).
        write_quote(db_path, q, actor="seed", note="seed legacy")
        import sqlite3, json
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT state_json FROM spine_quotes WHERE quote_id = ?", (qid,)
        ).fetchone()
        state = json.loads(row[0])
        state.pop("quote_seq", None)
        state.pop("quote_year", None)
        conn.execute(
            "UPDATE spine_quotes SET state_json = ? WHERE quote_id = ?",
            (json.dumps(state), qid),
        )
        conn.commit()
        conn.close()
        unstamped_ids.append(qid)

    stamped_ids: list[str] = []
    for i in range(include_stamped):
        qid = f"already-stamped-{i:03d}"
        q = Quote(
            quote_id=qid, agency="CCHCS", facility="SATF",
            solicitation_number="10847262", line_items=[_li()],
            tax_rate_bps=825,
        )
        write_quote(db_path, q, actor="seed")  # auto-assigns seq
        stamped_ids.append(qid)

    return {"unstamped": unstamped_ids, "stamped": stamped_ids}


# ──────────────────────────────────────────────────────────────────────
# Counter: GET
# ──────────────────────────────────────────────────────────────────────


def test_get_counter_returns_null_when_unset(auth_client, tmp_path,
                                              monkeypatch):
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    r = auth_client.get("/api/admin/spine/counter/quote_2026")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["counter_name"] == "quote_2026"
    assert body["value"] is None


def test_get_counter_after_seed(auth_client, tmp_path, monkeypatch):
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    _seed_spine_db(str(db), include_unstamped=0, include_stamped=2)
    r = auth_client.get("/api/admin/spine/counter/quote_2026")
    body = r.get_json()
    assert body["value"] == 2  # 2 stamped writes consumed 2 sequence values


# ──────────────────────────────────────────────────────────────────────
# Counter: POST set
# ──────────────────────────────────────────────────────────────────────


def test_set_counter_walks_up_in_max_jump_steps(auth_client, tmp_path,
                                                  monkeypatch):
    """COUNTER_MAX_JUMP=5; setting 0→39 walks via 5,10,15,...,39."""
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    r = auth_client.post(
        "/api/admin/spine/counter/quote_2026",
        json={"value": 39, "actor": "mike"},
    )
    assert r.status_code == 200, r.get_json()
    body = r.get_json()
    assert body["ok"] is True
    assert body["prior_value"] == 0
    assert body["new_value"] == 39
    assert body["steps_taken"][0] == 5
    assert body["steps_taken"][-1] == 39
    # Confirm via a read.
    r2 = auth_client.get("/api/admin/spine/counter/quote_2026")
    assert r2.get_json()["value"] == 39


def test_set_counter_idempotent_when_already_at_value(auth_client, tmp_path,
                                                       monkeypatch):
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    auth_client.post("/api/admin/spine/counter/quote_2026",
                     json={"value": 10, "actor": "mike"})
    r = auth_client.post("/api/admin/spine/counter/quote_2026",
                          json={"value": 10, "actor": "mike"})
    assert r.status_code == 200
    body = r.get_json()
    assert body["new_value"] == 10


def test_set_counter_allows_decrement(auth_client, tmp_path, monkeypatch):
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    auth_client.post("/api/admin/spine/counter/quote_2026",
                     json={"value": 50, "actor": "mike"})
    r = auth_client.post("/api/admin/spine/counter/quote_2026",
                          json={"value": 39, "actor": "mike"})
    assert r.status_code == 200
    assert r.get_json()["new_value"] == 39


def test_set_counter_rejects_missing_value(auth_client, tmp_path,
                                            monkeypatch):
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    r = auth_client.post("/api/admin/spine/counter/quote_2026", json={})
    assert r.status_code == 400
    assert "value" in r.get_json()["error"]


def test_set_counter_rejects_negative_value(auth_client, tmp_path,
                                              monkeypatch):
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    r = auth_client.post("/api/admin/spine/counter/quote_2026",
                          json={"value": -1, "actor": "mike"})
    assert r.status_code == 400


# ──────────────────────────────────────────────────────────────────────
# Backfill
# ──────────────────────────────────────────────────────────────────────


def test_backfill_dry_run_reports_plan_without_writing(auth_client, tmp_path,
                                                        monkeypatch):
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    seeded = _seed_spine_db(str(db), include_unstamped=3, include_stamped=1)

    r = auth_client.post(
        "/api/admin/spine/backfill-display-numbers",
        json={"actor": "mike", "dry_run": True, "year": 2026},
    )
    assert r.status_code == 200, r.get_json()
    body = r.get_json()
    assert body["ok"] is True
    assert body["dry_run"] is True
    assert body["would_assign_count"] == 3
    assert body["skipped_already_stamped_count"] == 1
    assert body["next_assignment_starts_at"] >= 1
    assert len(body["first_3_to_assign"]) == 3
    # NO new writes happened — value didn't move during dry_run.
    # Note: _seed_spine_db calls write_quote for ALL 4 quotes (auto-
    # assigning seqs), then we manually NULL the seq on the 3
    # "unstamped" ones in SQLite. So the counter sat at 4 entering
    # the dry-run, and we expect it to STILL be at 4 after.
    r2 = auth_client.get("/api/admin/spine/counter/quote_2026")
    val_after_dry = r2.get_json()["value"]
    assert val_after_dry == 4


def test_backfill_stamps_every_unassigned_quote(auth_client, tmp_path,
                                                 monkeypatch):
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    seeded = _seed_spine_db(str(db), include_unstamped=3, include_stamped=1)

    # Align counter to start at 39 (Mike's last legacy was R26Q39).
    auth_client.post("/api/admin/spine/counter/quote_2026",
                     json={"value": 39, "actor": "mike"})

    r = auth_client.post(
        "/api/admin/spine/backfill-display-numbers",
        json={"actor": "mike", "year": 2026},
    )
    assert r.status_code == 200, r.get_json()
    body = r.get_json()
    assert body["ok"] is True
    assert body["dry_run"] is False
    assert len(body["assigned"]) == 3
    # Numbers should be 40, 41, 42 (continuing from counter=39).
    nums = [a["display_number"] for a in body["assigned"]]
    assert "R26Q40" in nums
    assert "R26Q41" in nums
    assert "R26Q42" in nums

    # Verify each unstamped quote now has a display_number on read.
    from src.spine import read_quote
    for qid in seeded["unstamped"]:
        q = read_quote(str(db), qid)
        assert q.display_number is not None
        assert q.display_number.startswith("R26Q")


def test_backfill_idempotent_second_run_no_op(auth_client, tmp_path,
                                                monkeypatch):
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    _seed_spine_db(str(db), include_unstamped=2, include_stamped=0)

    r1 = auth_client.post(
        "/api/admin/spine/backfill-display-numbers",
        json={"actor": "mike", "year": 2026},
    )
    assert len(r1.get_json()["assigned"]) == 2

    r2 = auth_client.post(
        "/api/admin/spine/backfill-display-numbers",
        json={"actor": "mike", "year": 2026},
    )
    assert r2.status_code == 200
    body2 = r2.get_json()
    assert body2["assigned"] == []
    assert body2["skipped_already_stamped_count"] == 2


def test_backfill_uses_oldest_first(auth_client, tmp_path, monkeypatch):
    """Older created_at gets the lower number — preserves chronology."""
    import sqlite3
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    seeded = _seed_spine_db(str(db), include_unstamped=2, include_stamped=0)

    # Force the second-seeded quote to look OLDER than the first.
    conn = sqlite3.connect(str(db))
    old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    conn.execute(
        "UPDATE spine_quotes SET created_at = ? WHERE quote_id = ?",
        (old, seeded["unstamped"][1]),
    )
    # And the state_json's created_at field too (read_quote parses that).
    import json
    row = conn.execute(
        "SELECT state_json FROM spine_quotes WHERE quote_id = ?",
        (seeded["unstamped"][1],),
    ).fetchone()
    state = json.loads(row[0])
    state["created_at"] = old
    conn.execute(
        "UPDATE spine_quotes SET state_json = ? WHERE quote_id = ?",
        (json.dumps(state), seeded["unstamped"][1]),
    )
    conn.commit()
    conn.close()

    r = auth_client.post(
        "/api/admin/spine/backfill-display-numbers",
        json={"actor": "mike", "year": 2026},
    )
    body = r.get_json()
    # The oldest quote (seeded[1]) should get R26Q1, the newer R26Q2.
    by_id = {a["quote_id"]: a["display_number"] for a in body["assigned"]}
    older_id = seeded["unstamped"][1]
    newer_id = seeded["unstamped"][0]
    # Extract the integer suffix.
    older_seq = int(by_id[older_id].split("Q", 1)[1])
    newer_seq = int(by_id[newer_id].split("Q", 1)[1])
    assert older_seq < newer_seq, (
        f"oldest quote {older_id} should get lower seq than newer "
        f"{newer_id}; got {by_id}"
    )


# ──────────────────────────────────────────────────────────────────────
# Auth
# ──────────────────────────────────────────────────────────────────────


def test_endpoints_require_auth(anon_client, tmp_path, monkeypatch):
    """All three endpoints must be DASH_PASS gated."""
    db = tmp_path / "spine.db"
    monkeypatch.setenv("SPINE_DB_PATH", str(db))
    for r in (
        anon_client.get("/api/admin/spine/counter/quote_2026"),
        anon_client.post("/api/admin/spine/counter/quote_2026",
                          json={"value": 1, "actor": "x"}),
        anon_client.post("/api/admin/spine/backfill-display-numbers",
                          json={"actor": "x"}),
    ):
        assert r.status_code in (401, 403), (
            f"endpoint must reject anon, got {r.status_code}"
        )
