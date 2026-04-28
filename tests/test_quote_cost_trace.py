"""Tests for `_build_quote_cost_trace` — the per-quote cost trace page
on /growth-intel/quote (Plan §6.2 sub-3).

Locks the per-line invariants so a future tweak to pricing.cost_source
or pricing.unit_cost can't silently break the trace. The trace joins
straight to the source PC/RFQ items[].pricing dict, same shape used by
PR #619's chip card and PR #624's buyer detail.
"""
from __future__ import annotations

import json
from datetime import datetime

import pytest


def _build(quote_number):
    from src.api.modules.routes_growth_intel import _build_quote_cost_trace
    return _build_quote_cost_trace(quote_number)


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    for tbl in ("quotes", "price_checks", "rfqs"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()


def _seed_quote(conn, *, quote_number, status="sent", total=300.0,
                contact_email="buyer@x.gov", contact_name="Buyer",
                agency="CDCR", source_pc_id=None, source_rfq_id=None):
    when = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO quotes
          (quote_number, agency, status, total, created_at, updated_at,
           contact_email, contact_name, sent_at, is_test, line_items,
           source_pc_id, source_rfq_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (quote_number, agency, status, total, when, when,
          contact_email, contact_name, when, 0, "[]",
          source_pc_id, source_rfq_id))


def _seed_pc(conn, pc_id: str, items: list):
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


def _item(description, qty=1, unit_cost=0, unit_price=0,
          cost_source="operator", scprs_price=0, catalog_cost=0,
          amazon_price=0, markup_pct=25, part_number=""):
    return {
        "description": description,
        "qty": qty,
        "uom": "EA",
        "part_number": part_number,
        "pricing": {
            "unit_cost": unit_cost,
            "unit_price": unit_price,
            "extension": unit_price * qty,
            "cost_source": cost_source,
            "scprs_price": scprs_price,
            "catalog_cost": catalog_cost,
            "amazon_price": amazon_price,
            "markup_pct": markup_pct,
        },
    }


# ── Empty / not-found ───────────────────────────────────────────────────


def test_empty_quote_number_returns_error_state():
    out = _build("")
    assert out["ok"] is False
    assert out["found"] is False
    assert out["error"]


def test_missing_quote_number_returns_not_found():
    with _conn() as c:
        _wipe(c)
    out = _build("DOES-NOT-EXIST")
    assert out["ok"] is True
    assert out["found"] is False
    assert out["items"] == []


# ── Header passthrough ──────────────────────────────────────────────────


def test_header_returns_buyer_and_status_metadata():
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="HDR-1", status="won", total=999.0,
                    contact_email="alice@x.gov", contact_name="Alice")
        c.commit()
    out = _build("HDR-1")
    assert out["found"] is True
    h = out["header"]
    assert h["status"] == "won"
    assert h["total"] == 999.0
    assert h["contact_email"] == "alice@x.gov"
    assert h["contact_name"] == "Alice"


def test_quote_with_no_source_returns_header_but_no_items():
    """Header should still surface so the operator can see the quote
    metadata even when source PC/RFQ is missing."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="ORPHAN-1")
        c.commit()
    out = _build("ORPHAN-1")
    assert out["found"] is True
    assert out["items"] == []
    assert out["totals"]["extension_total"] == 0.0


# ── Per-line trace ──────────────────────────────────────────────────────


def test_per_line_trace_from_source_pc():
    with _conn() as c:
        _wipe(c)
        _seed_pc(c, "pc-CT", [
            _item("Widget", qty=10, unit_cost=5.00, unit_price=8.00,
                  cost_source="operator", scprs_price=10.00,
                  catalog_cost=5.20, amazon_price=12.00),
            _item("Gizmo", qty=2, unit_cost=15.00, unit_price=22.00,
                  cost_source="catalog", scprs_price=25.00,
                  catalog_cost=14.50, amazon_price=20.00),
        ])
        _seed_quote(c, quote_number="CT-1", total=124.0,
                    source_pc_id="pc-CT")
        c.commit()
    out = _build("CT-1")
    assert len(out["items"]) == 2

    w = out["items"][0]
    assert w["description"] == "Widget"
    assert w["qty"] == 10
    assert w["unit_cost"] == 5.00
    assert w["unit_price"] == 8.00
    assert w["extension"] == 80.00
    assert w["cost_source"] == "operator"
    assert w["scprs_price"] == 10.00
    assert w["catalog_cost"] == 5.20
    assert w["amazon_price"] == 12.00
    # Margin = ext (80) - cost*qty (50) = 30
    assert w["margin_dollars"] == 30.00


def test_per_line_trace_from_source_rfq():
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, "rfq-CT", [
            _item("Bandage", qty=5, unit_cost=2.00, unit_price=4.00,
                  cost_source="catalog"),
        ])
        _seed_quote(c, quote_number="CT-RFQ", total=20.0,
                    source_rfq_id="rfq-CT")
        c.commit()
    out = _build("CT-RFQ")
    assert len(out["items"]) == 1
    assert out["items"][0]["description"] == "Bandage"
    assert out["items"][0]["margin_dollars"] == 10.00  # 20 - 10


def test_unknown_cost_source_lands_in_unknown_bucket():
    with _conn() as c:
        _wipe(c)
        _seed_pc(c, "pc-X", [
            _item("Mystery", unit_cost=1.0, unit_price=2.0,
                  cost_source="some_new_pipeline_value"),
        ])
        _seed_quote(c, quote_number="UNK-1", source_pc_id="pc-X")
        c.commit()
    out = _build("UNK-1")
    assert out["items"][0]["cost_source"] == "unknown"
    assert out["items"][0]["cost_source_raw"] == "some_new_pipeline_value"
    assert out["totals"]["chips"]["unknown"] == 1


# ── Totals + margin math ────────────────────────────────────────────────


def test_totals_aggregate_cost_extension_and_margin_pct():
    with _conn() as c:
        _wipe(c)
        _seed_pc(c, "pc-T", [
            _item("A", qty=10, unit_cost=5, unit_price=10),   # ext 100, cost 50
            _item("B", qty=5,  unit_cost=20, unit_price=30),  # ext 150, cost 100
        ])
        _seed_quote(c, quote_number="T-1", source_pc_id="pc-T")
        c.commit()
    out = _build("T-1")
    t = out["totals"]
    assert t["extension_total"] == 250.00
    assert t["unit_cost_total"] == 150.00
    assert t["margin_dollars"] == 100.00
    # 100 / 250 = 40%
    assert t["margin_pct"] == 40.0


def test_margin_pct_none_when_unit_cost_unknown():
    """When every line has unit_cost=0 (the cost-gap state PR #621/#622
    are progressively closing), margin_pct must be None — a 100%
    'margin' would be misleading. UI shows '—' + a warning band."""
    with _conn() as c:
        _wipe(c)
        _seed_pc(c, "pc-G", [
            _item("Gappy", qty=2, unit_cost=0, unit_price=50),
        ])
        _seed_quote(c, quote_number="GAP-1", source_pc_id="pc-G")
        c.commit()
    out = _build("GAP-1")
    assert out["totals"]["margin_pct"] is None
    assert out["totals"]["extension_total"] == 100.00


def test_chip_totals_tally_per_quote():
    with _conn() as c:
        _wipe(c)
        _seed_pc(c, "pc-C", [
            _item("a", cost_source="operator", unit_cost=1, unit_price=2),
            _item("b", cost_source="operator", unit_cost=1, unit_price=2),
            _item("c", cost_source="amazon", unit_cost=1, unit_price=2),
            _item("d", cost_source=None, unit_cost=0, unit_price=0),
        ])
        _seed_quote(c, quote_number="CHIP-1", source_pc_id="pc-C")
        c.commit()
    out = _build("CHIP-1")
    chips = out["totals"]["chips"]
    assert chips["operator"] == 2
    assert chips["amazon"] == 1
    assert chips["needs_lookup"] == 1


# ── Schema tolerance ────────────────────────────────────────────────────


def test_safe_default_when_query_raises(monkeypatch):
    from src.api.modules import routes_growth_intel as _rgi

    class _Boom:
        def __enter__(self): raise RuntimeError("simulated DB failure")
        def __exit__(self, *a): return False

    monkeypatch.setattr(_rgi, "get_db", lambda: _Boom())
    out = _build("any")
    assert out["ok"] is False
    assert out["found"] is False
    assert out["items"] == []


# ── /growth-intel/quote route ───────────────────────────────────────────


def test_quote_trace_page_renders_with_items(auth_client):
    with _conn() as c:
        _wipe(c)
        _seed_pc(c, "pc-route", [
            _item("Widget", qty=4, unit_cost=10, unit_price=15,
                  cost_source="operator"),
        ])
        _seed_quote(c, quote_number="RT-1", source_pc_id="pc-route",
                    contact_email="r@x.gov", contact_name="Routy")
        c.commit()
    resp = auth_client.get("/growth-intel/quote?id=RT-1")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "Cost trace" in body
    assert "RT-1" in body
    assert "Widget" in body
    # KPI summary card.
    assert "MARGIN $" in body
    # Cost-source mix card.
    assert "Cost-source mix on this quote" in body


def test_quote_trace_page_renders_not_found_for_unknown_quote(auth_client):
    with _conn() as c:
        _wipe(c)
    resp = auth_client.get("/growth-intel/quote?id=DOES-NOT-EXIST")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")
    assert "not found" in body.lower()


def test_buyer_detail_page_links_to_quote_trace(auth_client):
    """Wiring proof: the buyer detail's quote table must contain a
    click-through link to /growth-intel/quote?id=<n>. Without this the
    new page is unreachable from the UI."""
    with _conn() as c:
        _wipe(c)
        _seed_pc(c, "pc-link", [
            _item("Linkable", unit_cost=1, unit_price=2,
                  cost_source="catalog"),
        ])
        _seed_quote(c, quote_number="LINK-Q1",
                    contact_email="link@x.gov", contact_name="Link",
                    source_pc_id="pc-link")
        c.commit()
    resp = auth_client.get("/growth-intel/buyer?email=link@x.gov")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "/growth-intel/quote?id=LINK-Q1" in body
