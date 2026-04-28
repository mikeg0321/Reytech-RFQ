"""Tests for `_build_orders_drift_card` — the /health/quoting card
that surfaces divergence between order write paths (Plan §4.3 sub-5).

The S3-prep silo gate per PLAN_ONCE_AND_FOR_ALL.md §5.1 is "100 PO
writes with zero divergence." This card is the metric that gate
reads. Tests lock the three drift counters + status thresholds so a
future tweak to either schema or the unified write path can't
silently regress the gate signal.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest


def _build():
    from src.api.modules.routes_health import _build_orders_drift_card
    return _build_orders_drift_card()


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    for tbl in ("orders", "quotes"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()


def _seed_quote(conn, *, quote_number, status="won", is_test=0,
                days_ago=10):
    when = (datetime.now() - timedelta(days=days_ago)).isoformat()
    conn.execute("""
        INSERT INTO quotes
          (quote_number, agency, status, total, created_at, updated_at,
           is_test, line_items)
        VALUES (?,?,?,?,?,?,?,?)
    """, (quote_number, "CDCR", status, 100.0, when, when,
          is_test, "[]"))


def _seed_order(conn, *, order_id, quote_number="", po_number="",
                status="open", is_test=0):
    when = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO orders
          (id, quote_number, po_number, agency, institution,
           total, status, items, created_at, updated_at, is_test)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (order_id, quote_number, po_number, "CDCR", "",
          100.0, status, "[]", when, when, is_test))


# ── Empty / unknown ─────────────────────────────────────────────────────


def test_unknown_when_no_won_quotes():
    """No won quotes yet = no divergence baseline. Card sits in unknown
    so it doesn't flash green prematurely."""
    with _conn() as c:
        _wipe(c)
    out = _build()
    assert out["status"] == "unknown"
    assert out["total_won_quotes"] == 0
    assert out["drift_pct"] == 0.0


def test_safe_default_when_db_query_raises(monkeypatch):
    from src.api.modules import routes_health as _rh

    class _Boom:
        def __enter__(self): raise RuntimeError("simulated DB failure")
        def __exit__(self, *a): return False

    monkeypatch.setattr(_rh, "get_db", lambda: _Boom())
    out = _build()
    assert out["status"] == "unknown"
    assert out["total_orders"] == 0


# ── Healthy path ────────────────────────────────────────────────────────


def test_healthy_when_every_won_quote_has_matching_order():
    with _conn() as c:
        _wipe(c)
        for i in range(3):
            qn = f"Q{i}"
            _seed_quote(c, quote_number=qn, status="won")
            _seed_order(c, order_id=f"o{i}", quote_number=qn,
                        po_number=f"PO-{i}")
        c.commit()
    out = _build()
    assert out["status"] == "healthy"
    assert out["drift_pct"] == 0.0
    assert out["won_quotes_no_order"] == 0
    assert out["duplicate_po_numbers"] == 0


# ── Orphan won quotes ──────────────────────────────────────────────────


def test_warn_when_5pct_of_won_quotes_orphaned():
    """1 of 10 won quotes has no orders row → 10% drift → warn."""
    with _conn() as c:
        _wipe(c)
        for i in range(10):
            qn = f"Q{i}"
            _seed_quote(c, quote_number=qn, status="won")
            if i < 9:
                _seed_order(c, order_id=f"o{i}", quote_number=qn,
                            po_number=f"PO-{i}")
        c.commit()
    out = _build()
    assert out["won_quotes_no_order"] == 1
    assert out["total_won_quotes"] == 10
    assert out["drift_pct"] == 10.0
    assert out["status"] == "warn"


def test_error_when_20pct_or_more_won_quotes_orphaned():
    """3 of 10 won quotes have no orders → 30% drift → error."""
    with _conn() as c:
        _wipe(c)
        for i in range(10):
            qn = f"Q{i}"
            _seed_quote(c, quote_number=qn, status="won")
            if i < 7:
                _seed_order(c, order_id=f"o{i}", quote_number=qn,
                            po_number=f"PO-{i}")
        c.commit()
    out = _build()
    assert out["won_quotes_no_order"] == 3
    assert out["drift_pct"] == 30.0
    assert out["status"] == "error"


# ── Duplicate PO numbers ───────────────────────────────────────────────


def test_error_when_duplicate_po_numbers_exist():
    """The drift card's legacy `duplicate_po_numbers` counter still
    flags any po_number appearing on 2+ rows — even legitimate
    multi-quote POs (one buyer PO covering N quotes). The
    po_aggregate card (PR #632) reframes those as legit; this
    counter is preserved as a raw signal until the legacy semantics
    are migrated.

    Note: as of S3-prep PR-2 (PR #634), same-PO + same-quote on
    multiple rows is BLOCKED by the partial UNIQUE index. So this
    test deliberately uses DIFFERENT quote_numbers under the same
    po_number — a legit multi-quote PO that still counts as
    `duplicate_po_numbers` in the legacy counter."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="Q1", status="won")
        _seed_quote(c, quote_number="Q2", status="won")
        _seed_order(c, order_id="o1", quote_number="Q1", po_number="PO-DUP")
        _seed_order(c, order_id="o2", quote_number="Q2", po_number="PO-DUP")
        c.commit()
    out = _build()
    assert out["duplicate_po_numbers"] == 1
    assert out["status"] == "error"


def test_empty_or_null_po_numbers_dont_count_as_duplicates():
    """Multiple orders with empty po_number aren't 'duplicate POs' —
    they're 'orders missing PO' (different counter)."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="Q1", status="won")
        _seed_order(c, order_id="o1", quote_number="Q1", po_number="",
                    status="open")
        _seed_order(c, order_id="o2", quote_number="Q1", po_number="",
                    status="open")
        c.commit()
    out = _build()
    assert out["duplicate_po_numbers"] == 0
    assert out["orders_no_po"] == 2


# ── Orders missing po_number ───────────────────────────────────────────


def test_orders_missing_po_only_counted_when_status_implies_one():
    """A status='active' or 'cancelled' order doesn't necessarily have
    a PO yet. Only count missing-PO when status implies fulfillment is
    underway (open/shipped/closed/completed/invoiced)."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="Q1", status="won")
        _seed_order(c, order_id="o1", quote_number="Q1", po_number="",
                    status="active")     # not counted
        _seed_order(c, order_id="o2", quote_number="Q1", po_number="",
                    status="cancelled")  # not counted
        _seed_order(c, order_id="o3", quote_number="Q1", po_number="",
                    status="open")       # COUNTED
        _seed_order(c, order_id="o4", quote_number="Q1", po_number="",
                    status="closed")     # COUNTED
        c.commit()
    out = _build()
    assert out["orders_no_po"] == 2


def test_warn_when_active_orders_missing_po_even_at_zero_drift():
    """Drift_pct can be 0% (every won quote has an order) but if those
    orders are missing po_number, that's still warn-worthy."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="Q1", status="won")
        _seed_order(c, order_id="o1", quote_number="Q1", po_number="",
                    status="open")
        c.commit()
    out = _build()
    assert out["drift_pct"] == 0.0
    assert out["orders_no_po"] == 1
    assert out["status"] == "warn"


# ── Test exclusion ──────────────────────────────────────────────────────


def test_test_quotes_and_orders_excluded():
    """is_test=1 quotes/orders mustn't pollute the drift signal."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="T1", status="won", is_test=1)
        _seed_quote(c, quote_number="REAL1", status="won")
        _seed_order(c, order_id="o-real", quote_number="REAL1",
                    po_number="PO-1")
        c.commit()
    out = _build()
    assert out["total_won_quotes"] == 1
    assert out["status"] == "healthy"


# ── /health/quoting integration ─────────────────────────────────────────


def test_health_quoting_json_includes_orders_drift(auth_client):
    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "orders_drift" in data
    od = data["orders_drift"]
    assert od["status"] in ("healthy", "warn", "error", "unknown")
    for k in ("total_orders", "orders_no_po", "duplicate_po_numbers",
              "won_quotes_no_order", "total_won_quotes", "drift_pct"):
        assert k in od


def test_health_quoting_html_renders_orders_drift_card(auth_client):
    resp = auth_client.get("/health/quoting")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "Orders drift" in body
    # Card description references the silo it covers so a future maintainer
    # finds the docs reference quickly.
    assert "S3-prep" in body


# ── Actionable detail (orphan_quotes / duplicate_pos) ───────────────────


def test_orphan_quotes_and_duplicate_pos_present_in_payload_keys():
    """Detail fields exist on every response, even when empty, so the
    template's `{% if _od.orphan_quotes %}` guard never KeyErrors."""
    with _conn() as c:
        _wipe(c)
    out = _build()
    assert "orphan_quotes" in out
    assert "duplicate_pos" in out
    assert out["orphan_quotes"] == []
    assert out["duplicate_pos"] == []


def test_orphan_quotes_lists_won_quotes_without_orders_row():
    """The list is THE actionable surface — operator sees which quote
    numbers to investigate. Each entry carries quote_number, agency,
    total, sent_at."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="ORPHAN-A", status="won")
        _seed_quote(c, quote_number="ORPHAN-B", status="won")
        _seed_quote(c, quote_number="HASORDER", status="won")
        _seed_order(c, order_id="o1", quote_number="HASORDER",
                    po_number="PO-1")
        c.commit()
    out = _build()
    qns = sorted(q["quote_number"] for q in out["orphan_quotes"])
    assert qns == ["ORPHAN-A", "ORPHAN-B"]
    sample = out["orphan_quotes"][0]
    assert "agency" in sample
    assert "total" in sample
    assert "sent_at" in sample


def test_orphan_quotes_capped_at_20():
    """100% drift on a fresh boot would dump 100+ rows into the page —
    cap at 20 so the dashboard stays bounded."""
    with _conn() as c:
        _wipe(c)
        for i in range(25):
            _seed_quote(c, quote_number=f"BULK-{i:02d}", status="won")
        c.commit()
    out = _build()
    assert out["won_quotes_no_order"] == 25
    assert len(out["orphan_quotes"]) == 20


def test_orphan_quotes_excludes_test_quotes():
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="TQ", status="won", is_test=1)
        _seed_quote(c, quote_number="REAL", status="won")
        c.commit()
    out = _build()
    qns = [q["quote_number"] for q in out["orphan_quotes"]]
    assert "TQ" not in qns
    assert "REAL" in qns


def test_duplicate_pos_lists_po_with_quote_refs():
    """Operator sees WHICH po_number is duplicated AND which quote
    rows reference it — enough context to act without running ad-hoc
    SQL."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="DUP-A", status="won")
        _seed_quote(c, quote_number="DUP-B", status="won")
        _seed_order(c, order_id="o1", quote_number="DUP-A",
                    po_number="PO-SHARED")
        _seed_order(c, order_id="o2", quote_number="DUP-B",
                    po_number="PO-SHARED")
        c.commit()
    out = _build()
    assert len(out["duplicate_pos"]) == 1
    entry = out["duplicate_pos"][0]
    assert entry["po_number"] == "PO-SHARED"
    assert entry["count"] == 2
    assert sorted(entry["quote_numbers"]) == ["DUP-A", "DUP-B"]


def test_duplicate_pos_empty_when_healthy():
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="OK", status="won")
        _seed_order(c, order_id="o", quote_number="OK", po_number="PO-OK")
        c.commit()
    out = _build()
    assert out["duplicate_pos"] == []


def test_duplicate_pos_capped_at_20():
    with _conn() as c:
        _wipe(c)
        for i in range(25):
            qa = f"DA{i:02d}"
            qb = f"DB{i:02d}"
            _seed_quote(c, quote_number=qa, status="won")
            _seed_quote(c, quote_number=qb, status="won")
            po = f"PO-DUP-{i:02d}"
            _seed_order(c, order_id=f"oa{i}", quote_number=qa, po_number=po)
            _seed_order(c, order_id=f"ob{i}", quote_number=qb, po_number=po)
        c.commit()
    out = _build()
    assert out["duplicate_po_numbers"] == 25
    assert len(out["duplicate_pos"]) == 20


def test_health_quoting_html_renders_orphan_quotes_details(auth_client):
    """When the card is in error state with orphans, the template
    shows the <details> drawer with the quote list — not just the
    counter."""
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="ORPHAN-X", status="won")
        c.commit()
    resp = auth_client.get("/health/quoting")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")
    assert "Orphan won quotes" in body
    assert "ORPHAN-X" in body
