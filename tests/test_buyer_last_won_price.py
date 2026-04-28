"""Tests for `_last_won_price_for_buyer` — the helper that powers the
"Last won (buyer)" column on the per-quote cost trace (Plan §6.2's
headline: "delta vs. our last winning bid for that buyer").

Match priority is locked here:
  1. Exact part_number == part_number wins (most reliable signal)
  2. Description contains all of the first 3 ≥3-char words from the
     query item

The fuzzy description path is the buyer-stable-RFQ-text shortcut —
buyers tend to copy/paste from the same vendor catalog, so leading
words pin the product without us having to reconcile fuzzy SKUs.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    conn.execute("DELETE FROM quotes")
    conn.commit()


def _seed_won(conn, *, quote_number, contact_email, items,
              days_ago=10, status="won", is_test=0):
    when = (datetime.now() - timedelta(days=days_ago)).isoformat()
    conn.execute("""
        INSERT INTO quotes
          (quote_number, agency, status, total, created_at, updated_at,
           contact_email, sent_at, is_test, line_items)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (quote_number, "CDCR", status, 100.0, when, when,
          contact_email, when, is_test, json.dumps(items)))


def _lookup(email, description="", part_number="", exclude="",
            days=730):
    """Open a connection and call the helper directly — same shape the
    cost-trace builder uses internally."""
    from src.api.modules.routes_growth_intel import (
        _last_won_price_for_buyer as _fn,
    )
    with _conn() as c:
        return _fn(c, contact_email=email, description=description,
                   part_number=part_number,
                   exclude_quote_number=exclude, days=days)


# ── Empty / no-match ────────────────────────────────────────────────────


def test_returns_empty_when_email_blank():
    out = _lookup("", description="anything")
    assert out == {}


def test_returns_empty_when_no_quotes_at_all():
    with _conn() as c:
        _wipe(c)
    out = _lookup("nobody@x.gov", description="any item")
    assert out == {}


def test_returns_empty_when_buyer_has_only_lost_quotes():
    """Lost quotes don't anchor a 'last won' — they're not a price
    we'd repeat."""
    with _conn() as c:
        _wipe(c)
        _seed_won(c, quote_number="L1", contact_email="b@x.gov",
                  status="lost", days_ago=30,
                  items=[{"description": "Bandage 4x4 sterile",
                          "pricing": {"unit_price": 8.50}}])
        c.commit()
    out = _lookup("b@x.gov", description="Bandage 4x4 sterile")
    assert out == {}


# ── Match priority ──────────────────────────────────────────────────────


def test_part_number_match_wins_over_description():
    """When both exist on the historical item, exact part_number is
    authoritative — descriptions can drift, MFG#s shouldn't."""
    with _conn() as c:
        _wipe(c)
        _seed_won(c, quote_number="HIST", contact_email="b@x.gov",
                  days_ago=30,
                  items=[{"description": "Different wording",
                          "part_number": "MFG-99",
                          "pricing": {"unit_price": 12.34}}])
        c.commit()
    out = _lookup("b@x.gov",
                  description="Brand new item the description doesn't match",
                  part_number="MFG-99")
    assert out["price"] == 12.34
    assert out["quote_number"] == "HIST"
    assert out["won_at"]


def test_description_match_when_no_part_number():
    """When part_number is missing on either side, fall back to
    description-words match. First 3 ≥3-char words must all appear."""
    with _conn() as c:
        _wipe(c)
        _seed_won(c, quote_number="DESC", contact_email="b@x.gov",
                  days_ago=10,
                  items=[{"description": "Sterile gauze pads 4x4 inch box",
                          "pricing": {"unit_price": 7.25}}])
        c.commit()
    out = _lookup("b@x.gov", description="Sterile gauze pads")
    assert out["price"] == 7.25


def test_description_match_is_case_insensitive():
    with _conn() as c:
        _wipe(c)
        _seed_won(c, quote_number="CASE", contact_email="b@x.gov",
                  days_ago=10,
                  items=[{"description": "STERILE GAUZE PADS",
                          "pricing": {"unit_price": 7.25}}])
        c.commit()
    out = _lookup("b@x.gov", description="sterile GAUZE pads")
    assert out["price"] == 7.25


def test_no_match_when_only_partial_description_overlap():
    """All 3 words must appear. Two of three is not a match — too
    error-prone to risk false positives on price decisions."""
    with _conn() as c:
        _wipe(c)
        _seed_won(c, quote_number="PARTIAL", contact_email="b@x.gov",
                  days_ago=10,
                  items=[{"description": "Sterile gauze 4x4",
                          "pricing": {"unit_price": 7.25}}])
        c.commit()
    # "abdominal" is not in the historical desc → fail.
    out = _lookup("b@x.gov", description="Sterile abdominal pads")
    assert out == {}


# ── Exclusion of self ──────────────────────────────────────────────────


def test_excludes_the_query_quote_itself():
    """When tracing a won quote's cost, that same quote must NOT match
    itself as 'last won'. Otherwise every line shows delta=0 trivially."""
    with _conn() as c:
        _wipe(c)
        _seed_won(c, quote_number="SELF", contact_email="b@x.gov",
                  days_ago=5,
                  items=[{"description": "Item A unique words",
                          "pricing": {"unit_price": 10.00}}])
        c.commit()
    out = _lookup("b@x.gov", description="Item A unique",
                  exclude="SELF")
    assert out == {}


def test_test_quotes_excluded():
    """is_test=1 quotes can't anchor a real-price reference."""
    with _conn() as c:
        _wipe(c)
        _seed_won(c, quote_number="T1", contact_email="b@x.gov",
                  is_test=1, days_ago=5,
                  items=[{"description": "Test gadget item",
                          "pricing": {"unit_price": 99.00}}])
        c.commit()
    out = _lookup("b@x.gov", description="Test gadget item")
    assert out == {}


def test_email_match_is_case_insensitive():
    with _conn() as c:
        _wipe(c)
        _seed_won(c, quote_number="CASEEMAIL",
                  contact_email="Alice@CDCR.ca.gov", days_ago=10,
                  items=[{"description": "Item case email",
                          "pricing": {"unit_price": 5.00}}])
        c.commit()
    out = _lookup("alice@cdcr.ca.gov", description="Item case email")
    assert out["price"] == 5.00


# ── Recency: most-recent won wins ──────────────────────────────────────


def test_returns_most_recent_match_when_multiple_won_quotes():
    """Given two won quotes both matching the item, return the more
    recent unit_price — it's the closer-to-now data point."""
    with _conn() as c:
        _wipe(c)
        _seed_won(c, quote_number="OLD", contact_email="b@x.gov",
                  days_ago=300,
                  items=[{"description": "Recurring item buyer",
                          "pricing": {"unit_price": 10.00}}])
        _seed_won(c, quote_number="NEW", contact_email="b@x.gov",
                  days_ago=30,
                  items=[{"description": "Recurring item buyer",
                          "pricing": {"unit_price": 11.00}}])
        c.commit()
    out = _lookup("b@x.gov", description="Recurring item buyer")
    assert out["price"] == 11.00
    assert out["quote_number"] == "NEW"


def test_outside_days_window_excluded():
    """The lookup window is 730 days (2 years) by default. A 3-year-old
    won quote shouldn't anchor today's bid."""
    with _conn() as c:
        _wipe(c)
        _seed_won(c, quote_number="ANCIENT", contact_email="b@x.gov",
                  days_ago=1100,
                  items=[{"description": "Three year old item",
                          "pricing": {"unit_price": 7.25}}])
        c.commit()
    out = _lookup("b@x.gov", description="Three year old item",
                  days=730)
    assert out == {}
    # But within a wider window it's findable.
    out = _lookup("b@x.gov", description="Three year old item",
                  days=1500)
    assert out["price"] == 7.25


# ── Cost-trace integration ──────────────────────────────────────────────


def test_cost_trace_attaches_last_won_to_each_item():
    """The per-quote cost trace builder must surface last_won +
    delta_vs_last_won on each item it returns."""
    from src.api.modules.routes_growth_intel import _build_quote_cost_trace
    with _conn() as c:
        _wipe(c)
        # A historical won quote with a known item.
        _seed_won(c, quote_number="WON-PRIOR", contact_email="b@x.gov",
                  days_ago=60,
                  items=[{"description": "Bandage 4x4",
                          "part_number": "B-44",
                          "pricing": {"unit_price": 8.00}}])
        # The current quote (NOT won — could be sent/pending) referencing
        # the same item but priced differently.
        _seed_won(c, quote_number="CURRENT", contact_email="b@x.gov",
                  status="sent", days_ago=2,
                  items=[{"description": "Bandage 4x4",
                          "part_number": "B-44",
                          "pricing": {"unit_price": 9.50}}])
        # The current quote must point to a source PC for the cost
        # trace to populate items[]; mirror PR #625's seed pattern.
        c.execute("""
            INSERT INTO price_checks (id, created_at, status, items, pc_data)
            VALUES (?, ?, ?, ?, ?)
        """, ("pc-LW", datetime.now().isoformat(), "sent",
              json.dumps([{"description": "Bandage 4x4",
                           "part_number": "B-44",
                           "qty": 5,
                           "pricing": {"unit_price": 9.50,
                                       "unit_cost": 4.00,
                                       "cost_source": "operator"}}]),
              json.dumps({"items": []})))
        c.execute("UPDATE quotes SET source_pc_id = 'pc-LW' WHERE quote_number = 'CURRENT'")
        c.commit()
    trace = _build_quote_cost_trace("CURRENT")
    assert trace["found"] is True
    assert len(trace["items"]) == 1
    item = trace["items"][0]
    assert item["last_won"]["price"] == 8.00
    assert item["last_won"]["quote_number"] == "WON-PRIOR"
    # Delta = current 9.50 - last won 8.00 = 1.50 (we're bidding HIGHER
    # than last time we won — caution flag).
    assert item["delta_vs_last_won"] == 1.50


def test_cost_trace_renders_last_won_column(auth_client):
    """The new column must appear on the rendered HTML."""
    with _conn() as c:
        _wipe(c)
        c.execute("""
            INSERT INTO price_checks (id, created_at, status, items, pc_data)
            VALUES (?, ?, ?, ?, ?)
        """, ("pc-COL", datetime.now().isoformat(), "sent",
              json.dumps([{"description": "Test", "qty": 1,
                           "pricing": {"unit_price": 1.0,
                                       "cost_source": "operator"}}]),
              json.dumps({"items": []})))
        _seed_won(c, quote_number="COL-Q", contact_email="x@y.gov",
                  status="sent", days_ago=1,
                  items=[{"description": "Test", "pricing": {"unit_price": 1.0}}])
        c.execute("UPDATE quotes SET source_pc_id = 'pc-COL' WHERE quote_number = 'COL-Q'")
        c.commit()
    resp = auth_client.get("/growth-intel/quote?id=COL-Q")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "Last won (buyer)" in body
