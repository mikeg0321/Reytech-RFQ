"""Tests for `_build_buyer_detail` — the per-buyer drilldown page on
/growth-intel/buyer (Plan §6.2 sub-2).

Locks the per-quote and per-bucket aggregation invariants so a future
schema tweak can't silently drop quotes or skew the chip totals. The
detail joins straight to:
    quotes (per buyer, in window)
    → source PC items[].pricing.cost_source
    → source RFQ items[].pricing.cost_source

Same chip buckets the /health/quoting cost-source card uses (PR #619),
so an operator sees pricing-pipeline health *per buyer*.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest


def _build(email, window_days=180):
    from src.api.modules.routes_growth_intel import _build_buyer_detail
    return _build_buyer_detail(email, window_days=window_days)


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    """Reset tables this test cares about so leftover seeds from conftest
    don't bias counts/chips."""
    for tbl in ("quotes", "price_checks", "rfqs"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()


def _seed_quote(conn, *, quote_number, contact_email, contact_name="",
                agency="CDCR", status="sent", total=100.0, days_ago=1,
                is_test=0, source_pc_id=None, source_rfq_id=None):
    when = (datetime.now() - timedelta(days=days_ago)).isoformat()
    conn.execute("""
        INSERT INTO quotes
          (quote_number, agency, status, total, created_at, updated_at,
           contact_email, contact_name, sent_at, is_test, line_items,
           source_pc_id, source_rfq_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (quote_number, agency, status, total, when, when,
          contact_email, contact_name, when, is_test, "[]",
          source_pc_id, source_rfq_id))


def _seed_pc(conn, pc_id: str, items: list):
    """Mirror of the get_pc() data shape — items column carries the JSON list
    directly. PR #619's chip card uses the same shape."""
    pc_data = {"items": items}
    conn.execute("""
        INSERT INTO price_checks (id, created_at, status, items, pc_data)
        VALUES (?, ?, ?, ?, ?)
    """, (pc_id, datetime.now().isoformat(), "sent",
          json.dumps(items), json.dumps(pc_data)))


def _seed_rfq(conn, rfq_id: str, items: list):
    data = {"items": items}
    conn.execute("""
        INSERT INTO rfqs (id, received_at, status, items, data_json)
        VALUES (?, ?, ?, ?, ?)
    """, (rfq_id, datetime.now().isoformat(), "sent",
          json.dumps(items), json.dumps(data)))


def _items_with_sources(*sources):
    """Build a minimal items[] list where each entry has the given
    cost_source. The chip aggregator only reads pricing.cost_source so
    the rest of the row can be sparse."""
    return [{"description": f"item-{i}",
             "pricing": {"cost_source": s}}
            for i, s in enumerate(sources)]


# ── Empty / window / email match ────────────────────────────────────────


def test_empty_email_returns_error_state():
    out = _build("")
    assert out["ok"] is False
    assert out["error"]
    assert out["quotes"] == []


def test_returns_empty_when_no_quotes_for_email():
    with _conn() as c:
        _wipe(c)
    out = _build("nobody@example.com")
    assert out["ok"] is True
    assert out["quotes"] == []
    assert out["header"]["quote_count"] == 0
    # All chip buckets zero on empty.
    assert all(v == 0 for v in out["totals"].values())


def test_quote_outside_window_excluded():
    """Wide default window is 180d. A quote 200d old must NOT appear."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="OLD-1",
                    contact_email="alice@cdcr.ca.gov",
                    days_ago=200, status="won", total=500.0)
        _seed_quote(c, quote_number="NEW-1",
                    contact_email="alice@cdcr.ca.gov",
                    days_ago=10, status="sent", total=300.0)
        c.commit()
    out = _build("alice@cdcr.ca.gov", window_days=180)
    assert len(out["quotes"]) == 1
    assert out["quotes"][0]["quote_number"] == "NEW-1"


def test_email_match_is_case_insensitive():
    """Buyers sometimes mix case. Click-through must still find the row.
    The rollup groups on the literal email, but the detail must match
    case-insensitively or a buyer who quoted as 'Alice@…' followed by
    'alice@…' shows up split."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="UPPER",
                    contact_email="Alice@CDCR.ca.gov", days_ago=5)
        _seed_quote(c, quote_number="LOWER",
                    contact_email="alice@cdcr.ca.gov", days_ago=3)
        c.commit()
    out = _build("alice@cdcr.ca.gov")
    assert len(out["quotes"]) == 2


def test_test_quotes_excluded():
    """is_test=1 quotes (smoke fixtures) must not show on real-buyer page."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="TEST",
                    contact_email="x@x.gov", days_ago=5, is_test=1)
        _seed_quote(c, quote_number="REAL",
                    contact_email="x@x.gov", days_ago=5, is_test=0)
        c.commit()
    out = _build("x@x.gov")
    assert len(out["quotes"]) == 1
    assert out["quotes"][0]["quote_number"] == "REAL"


# ── Header KPI math ─────────────────────────────────────────────────────


def test_header_aggregates_status_counts_and_win_rate():
    with _conn() as c:
        _wipe(c)
        # 2 won, 1 lost, 1 pending → win_rate = 66.7% (2 of 3 decided)
        for i in range(2):
            _seed_quote(c, quote_number=f"W{i}",
                        contact_email="b@x.gov", contact_name="Bob",
                        status="won", total=200.0, days_ago=10 + i)
        _seed_quote(c, quote_number="L1",
                    contact_email="b@x.gov", contact_name="Bob",
                    status="lost", total=150.0, days_ago=8)
        _seed_quote(c, quote_number="P1",
                    contact_email="b@x.gov", contact_name="Bob",
                    status="pending", total=100.0, days_ago=2)
        c.commit()
    out = _build("b@x.gov")
    h = out["header"]
    assert h["quote_count"] == 4
    assert h["won_count"] == 2
    assert h["lost_count"] == 1
    assert h["pending_count"] == 1
    assert h["win_rate_pct"] == 66.7
    assert h["won_value_usd"] == 400.0   # 200 + 200
    assert h["total_value_usd"] == 650.0  # 200 + 200 + 150 + 100
    assert h["contact_name"] == "Bob"


def test_win_rate_none_when_no_decided_quotes():
    """All pending → win_rate is None so the UI shows '—' rather than 0%."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="P1",
                    contact_email="c@x.gov", status="pending", days_ago=2)
        c.commit()
    out = _build("c@x.gov")
    assert out["header"]["win_rate_pct"] is None


def test_first_seen_last_seen_track_window_extremes():
    """Rows are DESC by recency: index 0 = last_seen, last = first_seen."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="OLD",
                    contact_email="d@x.gov", days_ago=60)
        _seed_quote(c, quote_number="NEW",
                    contact_email="d@x.gov", days_ago=2)
        c.commit()
    out = _build("d@x.gov")
    # last_seen is more recent than first_seen.
    assert out["header"]["last_seen"] > out["header"]["first_seen"]


# ── Cost-source chips ───────────────────────────────────────────────────


def test_chips_aggregated_from_source_pc():
    with _conn() as c:
        _wipe(c)
        _seed_pc(c, "pc-A", _items_with_sources(
            "operator", "operator", "catalog", "amazon"))
        _seed_quote(c, quote_number="Q-A",
                    contact_email="e@x.gov", days_ago=3,
                    source_pc_id="pc-A")
        c.commit()
    out = _build("e@x.gov")
    assert len(out["quotes"]) == 1
    chips = out["quotes"][0]["chips"]
    assert chips["operator"] == 2
    assert chips["catalog"] == 1
    assert chips["amazon"] == 1
    # Per-buyer totals roll up across all quotes.
    assert out["totals"]["operator"] == 2
    assert out["totals"]["catalog"] == 1
    assert out["totals"]["amazon"] == 1


def test_chips_aggregated_from_source_rfq():
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, "rfq-A", _items_with_sources("scprs", "scprs", None))
        _seed_quote(c, quote_number="Q-RFQ",
                    contact_email="f@x.gov", days_ago=3,
                    source_rfq_id="rfq-A")
        c.commit()
    out = _build("f@x.gov")
    chips = out["quotes"][0]["chips"]
    assert chips["scprs"] == 2
    # None / blank cost_source → needs_lookup bucket (PR #619 invariant).
    assert chips["needs_lookup"] == 1


def test_missing_source_flagged_when_pc_absent():
    """Quote points to a PC id that no longer exists — UI must show
    'source missing' rather than silently zero."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="ORPHAN",
                    contact_email="g@x.gov", days_ago=3,
                    source_pc_id="pc-deleted")
        c.commit()
    out = _build("g@x.gov")
    q = out["quotes"][0]
    assert q["missing_source"] is True
    assert all(v == 0 for v in q["chips"].values())


def test_chips_zero_when_no_source_id_at_all():
    """Quote without source_pc_id or source_rfq_id (unusual but possible
    for legacy rows) — missing_source flagged so we don't fake a chip mix."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="ROOTLESS",
                    contact_email="h@x.gov", days_ago=3)
        c.commit()
    out = _build("h@x.gov")
    assert out["quotes"][0]["missing_source"] is True


def test_unknown_cost_source_lands_in_unknown_bucket():
    """A cost_source value the bucketer doesn't recognize must NOT be
    silently dropped — it lands in 'unknown' so it surfaces as a grey
    chip rather than vanishing."""
    with _conn() as c:
        _wipe(c)
        _seed_pc(c, "pc-mystery",
                 _items_with_sources("some_new_pipeline_value"))
        _seed_quote(c, quote_number="Q-MYS",
                    contact_email="i@x.gov", days_ago=3,
                    source_pc_id="pc-mystery")
        c.commit()
    out = _build("i@x.gov")
    chips = out["quotes"][0]["chips"]
    assert chips["unknown"] == 1


# ── Schema tolerance ────────────────────────────────────────────────────


def test_safe_default_when_query_raises(monkeypatch):
    """If the SQL path raises, the page must return its safe default —
    never bubble up and crash the buyer detail render."""
    from src.api.modules import routes_growth_intel as _rgi

    class _Boom:
        def __enter__(self): raise RuntimeError("simulated DB failure")
        def __exit__(self, *a): return False

    monkeypatch.setattr(_rgi, "get_db", lambda: _Boom())
    out = _build("anyone@x.gov")
    assert out["ok"] is False
    assert out["quotes"] == []
    assert out["header"]["quote_count"] == 0


# ── /growth-intel/buyer route ───────────────────────────────────────────


def test_buyer_detail_page_renders_with_data(auth_client):
    with _conn() as c:
        _wipe(c)
        _seed_pc(c, "pc-route", _items_with_sources("operator", "catalog"))
        _seed_quote(c, quote_number="ROUTE-1",
                    contact_email="route@x.gov", contact_name="Routy",
                    status="won", total=400.0, days_ago=3,
                    source_pc_id="pc-route")
        c.commit()
    resp = auth_client.get("/growth-intel/buyer?email=route@x.gov")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "Routy" in body
    assert "ROUTE-1" in body
    # Header KPI strip rendered.
    assert "QUOTES" in body and "WON" in body and "LOST" in body
    # Cost-source chip totals card rendered (because totals > 0).
    assert "Cost-source mix" in body


def test_buyer_detail_page_renders_empty_state_for_unknown_email(auth_client):
    with _conn() as c:
        _wipe(c)
    resp = auth_client.get("/growth-intel/buyer?email=ghost@nowhere.gov")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "No quotes for" in body
    assert "ghost@nowhere.gov" in body


def test_growth_intel_page_links_to_buyer_detail(auth_client):
    """Wiring proof: the rollup card on /growth-intel must contain a
    click-through link to the new detail route. Without this the new
    page is unreachable from the UI."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="LINK-1",
                    contact_email="linktest@x.gov",
                    contact_name="Link Test", days_ago=3)
        c.commit()
    resp = auth_client.get("/growth-intel")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    # urlencode turns '@' into '%40' and '.' stays. Either form acceptable
    # depending on Jinja's filter behavior, so test the prefix + email host.
    assert "/growth-intel/buyer?email=" in body
    assert "linktest" in body
