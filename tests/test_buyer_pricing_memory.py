"""Tests for `_build_buyer_pricing_memory` — the per-buyer rollup panel
on /growth-intel that surfaces Plan §6.2 sub-1.

Locks the aggregation invariants so a future tweak to the SQL doesn't
silently drop a buyer or skew the win-rate denominator. The card joins
straight to the `quotes` table grouped by `contact_email`; tests insert
realistic rows directly so the math is reproducible offline.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

import pytest


def _build(window_days=90, limit=20):
    from src.api.modules.routes_growth_intel import _build_buyer_pricing_memory
    return _build_buyer_pricing_memory(window_days=window_days, limit=limit)


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    """Reset the quotes table so any conftest-seeded leftovers don't bias
    counts."""
    conn.execute("DELETE FROM quotes")
    conn.commit()


def _seed_quote(conn, *, quote_number, contact_email, contact_name="",
                agency="CDCR", status="sent", total=100.0, days_ago=1,
                is_test=0):
    """Direct insert — the conftest's seed_db_quote helper doesn't expose
    contact_email or sent_at, both of which are load-bearing here.
    sent_at is set to the same offset as created_at so the WHERE-window
    filter behaves predictably."""
    when = (datetime.now() - timedelta(days=days_ago)).isoformat()
    conn.execute("""
        INSERT INTO quotes
          (quote_number, agency, status, total, created_at, updated_at,
           contact_email, contact_name, sent_at, is_test, line_items)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (quote_number, agency, status, total, when, when,
          contact_email, contact_name, when, is_test, "[]"))


# ── Empty / window ──────────────────────────────────────────────────────


def test_returns_empty_rows_when_no_quotes():
    with _conn() as c:
        _wipe(c)
    out = _build()
    assert out["ok"] is True
    assert out["rows"] == []
    assert out["total_buyers"] == 0
    assert out["totals"] == {
        "quotes": 0, "won": 0, "lost": 0, "pending": 0, "value_usd": 0.0,
    }


def test_quotes_outside_window_are_excluded():
    """A quote 200 days old should NOT appear in the 90d window."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="OLD-1", contact_email="ancient@buyer.gov",
                    days_ago=200, status="won", total=500.0)
        _seed_quote(c, quote_number="NEW-1", contact_email="recent@buyer.gov",
                    days_ago=5, status="sent", total=300.0)
        c.commit()
    out = _build(window_days=90)
    assert len(out["rows"]) == 1
    assert out["rows"][0]["contact_email"] == "recent@buyer.gov"


def test_anonymous_quotes_with_blank_email_excluded():
    """A quote with no contact_email shouldn't show up — it can't anchor a
    per-buyer rollup, and lumping them into '(unknown)' would corrupt the
    real buyers' totals."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="ANON-1", contact_email="", days_ago=5)
        _seed_quote(c, quote_number="REAL-1", contact_email="real@buyer.gov",
                    days_ago=5)
        c.commit()
    out = _build()
    assert len(out["rows"]) == 1
    assert out["rows"][0]["contact_email"] == "real@buyer.gov"


def test_test_quotes_are_excluded():
    """Quotes with is_test=1 (smoke fixtures, regression seeds) must not
    pollute real-buyer rollups."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="TEST-1", contact_email="t@example.com",
                    days_ago=5, is_test=1)
        _seed_quote(c, quote_number="REAL-1", contact_email="r@example.com",
                    days_ago=5, is_test=0)
        c.commit()
    out = _build()
    assert len(out["rows"]) == 1
    assert out["rows"][0]["contact_email"] == "r@example.com"


# ── Per-buyer aggregation ───────────────────────────────────────────────


def test_aggregates_count_and_status_tally_per_buyer():
    with _conn() as c:
        _wipe(c)
        # buyer A: 2 won, 1 lost, 1 pending
        for i in range(2):
            _seed_quote(c, quote_number=f"A-W{i}", contact_email="a@cdcr.ca.gov",
                        contact_name="Alice A", status="won", total=200.0,
                        days_ago=5+i)
        _seed_quote(c, quote_number="A-L1", contact_email="a@cdcr.ca.gov",
                    contact_name="Alice A", status="lost", total=150.0, days_ago=4)
        _seed_quote(c, quote_number="A-P1", contact_email="a@cdcr.ca.gov",
                    contact_name="Alice A", status="pending", total=100.0,
                    days_ago=1)
        # buyer B: 1 sent, no resolution
        _seed_quote(c, quote_number="B-S1", contact_email="b@calvet.ca.gov",
                    contact_name="Bob B", status="sent", total=50.0, days_ago=3)
        c.commit()

    out = _build()
    assert len(out["rows"]) == 2
    by_email = {r["contact_email"]: r for r in out["rows"]}

    a = by_email["a@cdcr.ca.gov"]
    assert a["quote_count"] == 4
    assert a["won_count"] == 2
    assert a["lost_count"] == 1
    assert a["pending_count"] == 1
    # decided = 3, won = 2 → 66.7%
    assert a["win_rate_pct"] == 66.7
    assert a["total_value_usd"] == 650.0  # 200+200+150+100

    b = by_email["b@calvet.ca.gov"]
    assert b["quote_count"] == 1
    assert b["pending_count"] == 1
    # No decided quotes yet → win_rate is None so UI shows '—'
    assert b["win_rate_pct"] is None


def test_status_pending_buckets_include_sent_draft_priced():
    """The 'pending' bucket must include status values the operator is
    actively waiting on, not just literal 'pending'. Otherwise a quote
    sitting in status='sent' looks like neither won nor pending — a hole."""
    with _conn() as c:
        _wipe(c)
        for i, s in enumerate(("pending", "sent", "draft", "priced")):
            _seed_quote(c, quote_number=f"X-{i}", contact_email="x@ex.gov",
                        status=s, days_ago=5+i)
        c.commit()
    out = _build()
    assert out["rows"][0]["pending_count"] == 4


def test_results_sorted_by_recency_desc():
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="OLD", contact_email="old@x.gov", days_ago=80)
        _seed_quote(c, quote_number="MID", contact_email="mid@x.gov", days_ago=20)
        _seed_quote(c, quote_number="NEW", contact_email="new@x.gov", days_ago=1)
        c.commit()
    out = _build()
    emails = [r["contact_email"] for r in out["rows"]]
    assert emails == ["new@x.gov", "mid@x.gov", "old@x.gov"]


def test_limit_caps_returned_rows():
    with _conn() as c:
        _wipe(c)
        for i in range(7):
            _seed_quote(c, quote_number=f"Q-{i}",
                        contact_email=f"buyer{i}@x.gov",
                        days_ago=i + 1)
        c.commit()
    out = _build(limit=3)
    assert len(out["rows"]) == 3
    # total_buyers reflects the unfiltered universe so the UI can show
    # "showing top 3 of 7 buyers"
    assert out["total_buyers"] == 7


# ── Totals across visible rows ──────────────────────────────────────────


def test_totals_sum_across_returned_rows_only():
    """Totals reflect the visible rows (consistent with the `LIMIT` we
    applied) so an operator reading the totals isn't confused by hidden
    counts that don't appear above."""
    with _conn() as c:
        _wipe(c)
        # 3 buyers all in window. Limit=2 → only top 2 are returned.
        _seed_quote(c, quote_number="A", contact_email="a@x.gov",
                    status="won", total=100.0, days_ago=1)
        _seed_quote(c, quote_number="B", contact_email="b@x.gov",
                    status="lost", total=200.0, days_ago=2)
        _seed_quote(c, quote_number="C", contact_email="c@x.gov",
                    status="pending", total=300.0, days_ago=3)
        c.commit()
    out = _build(limit=2)
    assert len(out["rows"]) == 2
    assert out["totals"]["quotes"] == 2
    assert out["totals"]["value_usd"] == 300.0  # a+b only
    assert out["totals"]["won"] == 1
    assert out["totals"]["lost"] == 1
    assert out["totals"]["pending"] == 0


# ── Schema tolerance ────────────────────────────────────────────────────


def test_safe_default_when_query_raises(monkeypatch):
    """If the SQL path raises (table missing, malformed row, lock timeout),
    the panel must return its safe default — never bubble up and crash
    /growth-intel."""
    from src.api.modules import routes_growth_intel as _rgi
    class _Boom:
        def __enter__(self): raise RuntimeError("simulated DB failure")
        def __exit__(self, *a): return False
    monkeypatch.setattr(_rgi, "get_db", lambda: _Boom())
    out = _build()
    assert out["ok"] is False
    assert out["rows"] == []


# ── /growth-intel HTML render ───────────────────────────────────────────


def test_growth_intel_page_renders_buyer_pricing_memory_section(auth_client):
    """The /growth-intel HTML must render the new section header so the
    operator sees it, and the empty-state copy locks behavior on a
    fresh-boot dashboard."""
    resp = auth_client.get("/growth-intel")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "Buyer Pricing Memory" in body, (
        "/growth-intel HTML missing the 'Buyer Pricing Memory' card. "
        "Inspect growth_intelligence.html around the {% set _bpm = "
        "buyer_pricing_memory %} block."
    )
    # Empty-state copy is present whenever buyer_pricing_memory.rows is empty
    # (which is the case in a fresh test DB).
    assert ("No quotes with a buyer email" in body
            or "Showing top" in body), (
        "Neither empty-state nor populated-state markup is rendering — "
        "template logic broke around the _bpm.rows branch."
    )
