"""Tests for /health/quoting body-extract telemetry card.

Locks the kill-criterion semantics: when >=3 records were extracted but
ALL stuck in needs_review, status='kill_signal' so the operator knows to
flip the flag off. When at least one parsed, status='healthy'. Empty
window → 'no_data' (gray, not error).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest


def _build():
    from src.api.modules.routes_health import _build_body_extract_card
    return _build_body_extract_card()


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    for tbl in ("rfqs", "price_checks"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()


def _seed_rfq(conn, *, rid, status, body_items, hours_ago=2):
    when = (datetime.now() - timedelta(hours=hours_ago)).isoformat()
    items = body_items
    data_json = json.dumps({"items": items})
    conn.execute(
        """INSERT INTO rfqs (id, received_at, status, items, data_json,
                             agency, institution, requestor_name, requestor_email,
                             rfq_number, solicitation_number)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (rid, when, status, json.dumps(items), data_json,
         "calvet", "VHC-WLA", "Buyer", "buyer@calvet.ca.gov",
         f"AUTO_{rid[-8:]}", f"AUTO_{rid[-8:]}"),
    )


def _bx_item(stage="tabular_full", qty=5, desc="Widget"):
    return {
        "qty": qty,
        "description": desc,
        "source": "email_body_regex",
        "source_stage": stage,
        "needs_review": True,
    }


# ── No data ────────────────────────────────────────────────────────────


def test_no_data_when_no_records():
    with _conn() as c:
        _wipe(c)
    out = _build()
    assert out["status"] == "no_data"
    assert out["total_24h"] == 0
    assert out["total_7d"] == 0


def test_no_data_when_records_have_no_body_extracted_items():
    """Records with attachment-derived items (no body extraction) must NOT
    show up — the card is specifically about the body-extract path."""
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, rid="rfq-no-body", status="parsed",
                  body_items=[{"qty": 3, "description": "PDF item",
                               "source": "pdf_text"}])
        c.commit()
    out = _build()
    assert out["status"] == "no_data"
    assert out["total_7d"] == 0


# ── Status semantics ───────────────────────────────────────────────────


def test_healthy_when_at_least_one_record_parsed():
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, rid="rfq-parsed", status="parsed",
                  body_items=[_bx_item()])
        c.commit()
    out = _build()
    assert out["status"] == "healthy"
    assert out["total_7d"] == 1
    assert out["status_split_7d"]["parsed"] == 1


def test_kill_signal_when_3_or_more_all_stuck_in_needs_review():
    """Operationalizes the kill criterion: 3+ extracted records, ALL
    triage = the extractor is producing junk operators reject."""
    with _conn() as c:
        _wipe(c)
        for i in range(3):
            _seed_rfq(c, rid=f"rfq-stuck-{i}", status="needs_review",
                      body_items=[_bx_item()])
        c.commit()
    out = _build()
    assert out["status"] == "kill_signal"
    assert out["status_split_7d"]["needs_review"] == 3
    assert out["status_split_7d"]["parsed"] == 0


def test_warming_when_few_records_no_parsed_yet():
    """1-2 records all in needs_review = warming, not kill_signal yet.
    Sample size too small to draw conclusions."""
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, rid="rfq-warm-1", status="needs_review",
                  body_items=[_bx_item()])
        _seed_rfq(c, rid="rfq-warm-2", status="needs_review",
                  body_items=[_bx_item()])
        c.commit()
    out = _build()
    assert out["status"] == "warming"


# ── Per-stage breakdown ────────────────────────────────────────────────


def test_by_stage_counts_items_correctly():
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, rid="rfq-tab", status="parsed",
                  body_items=[_bx_item(stage="tabular_full"),
                              _bx_item(stage="tabular_full")])
        _seed_rfq(c, rid="rfq-bullet", status="parsed",
                  body_items=[_bx_item(stage="bullet")])
        _seed_rfq(c, rid="rfq-inline", status="parsed",
                  body_items=[_bx_item(stage="inline_qty_x_desc"),
                              _bx_item(stage="inline_qty_x_desc"),
                              _bx_item(stage="inline_qty_x_desc")])
        c.commit()
    out = _build()
    assert out["by_stage_7d"]["tabular_full"] == 2
    assert out["by_stage_7d"]["bullet"] == 1
    assert out["by_stage_7d"]["inline_qty_x_desc"] == 3


def test_unknown_stage_bucket_catches_legacy_items():
    """Items predating PR-B-stages won't have source_stage. They should
    land in the 'unknown' bucket so the count still surfaces."""
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, rid="rfq-legacy", status="parsed", body_items=[
            {"qty": 1, "description": "Old", "source": "email_body_regex"},
        ])
        c.commit()
    out = _build()
    assert out["by_stage_7d"]["unknown"] == 1


# ── 24h vs 7d window ───────────────────────────────────────────────────


def test_24h_window_excludes_older_records():
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, rid="rfq-recent", status="parsed",
                  body_items=[_bx_item()], hours_ago=2)
        _seed_rfq(c, rid="rfq-old", status="parsed",
                  body_items=[_bx_item()], hours_ago=4 * 24)  # 4 days ago
        c.commit()
    out = _build()
    assert out["total_24h"] == 1
    assert out["total_7d"] == 2


# ── Shape contract ─────────────────────────────────────────────────────


def test_response_shape_is_stable():
    """Template binds to these keys — any rename breaks the page."""
    with _conn() as c:
        _wipe(c)
    out = _build()
    expected = {"status", "total_24h", "total_7d", "by_stage_7d",
                "status_split_7d", "rows"}
    assert expected.issubset(set(out.keys()))


def test_rows_capped_at_5():
    with _conn() as c:
        _wipe(c)
        for i in range(10):
            _seed_rfq(c, rid=f"rfq-row-{i:02d}", status="parsed",
                      body_items=[_bx_item()])
        c.commit()
    out = _build()
    assert len(out["rows"]) <= 5
