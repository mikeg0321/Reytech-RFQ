"""Tests for `_classify_po_by_prefix` + the PO prefix pattern card.

Per Mike 2026-04-28: California buyer POs follow agency-specific
canonical prefixes:
  - CalVet: 8955-00000…
  - CCHCS:  4500…
  - DSH:    4440-…

These are confirmed by reading buyer PO emails. An order whose
po_number matches none of these is either non-standard or — far
more commonly per PR #632 verification — a parse bug where the
prefix was stripped before storage. The card surfaces the
unidentified bucket so the operator can fix the ingest path
rather than have all new records inherit the same defect.

Lock the prefixes here so a future regression that "normalizes"
the prefix off the front of a PO number gets caught before it
ships.
"""
from __future__ import annotations

from datetime import datetime

import pytest


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


def _seed_order(conn, *, order_id, po_number="", quote_number="",
                total=100.0, agency="CDCR", is_test=0):
    when = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO orders
          (id, quote_number, po_number, agency, institution,
           total, status, items, created_at, updated_at, is_test)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (order_id, quote_number, po_number, agency, "",
          total, "open", "[]", when, when, is_test))


def _classify(po):
    from src.api.modules.routes_health import _classify_po_by_prefix
    return _classify_po_by_prefix(po)


def _build_card():
    from src.api.modules.routes_health import _build_po_prefix_card
    return _build_po_prefix_card()


# ── Prefix matcher ──────────────────────────────────────────────────────


@pytest.mark.parametrize("po,expected", [
    # CalVet — 8955-00000 prefix (note the dash + zeros)
    ("8955-0000044935",      "CalVet"),
    ("8955-0000099999",      "CalVet"),
    # CCHCS — 4500 prefix
    ("4500123456",           "CCHCS"),
    ("4500ABCDEF",           "CCHCS"),
    # DSH — 4440- prefix (with dash)
    ("4440-1234567",         "DSH"),
    ("4440-NN",              "DSH"),
    # Misses
    ("",                     ""),
    (None,                   ""),
    ("0000053217",           ""),    # CalVet PO with prefix STRIPPED
    ("0000064806",           ""),    # also stripped CalVet
    ("4501-XYZ",             ""),    # close to CCHCS but not exact
    ("4440",                 ""),    # missing the dash that DSH uses
    ("8955",                 ""),    # too short for CalVet's 8955-00000
    ("8955-0",               ""),    # not enough zeros
    # Whitespace tolerance
    ("  8955-00000123  ",    "CalVet"),
])
def test_classify_po_by_prefix(po, expected):
    assert _classify(po) == expected


def test_classifier_is_case_insensitive_for_letter_tails():
    """Some buyer POs have letter tails ('4500ABC1234'). Match is
    case-insensitive on the prefix; we don't care about the tail
    case since we only match the prefix portion."""
    assert _classify("4500abc1234") == "CCHCS"
    assert _classify("4500ABC1234") == "CCHCS"


# ── Card builder ────────────────────────────────────────────────────────


def test_card_unknown_when_no_pos():
    with _conn() as c:
        _wipe(c)
    out = _build_card()
    assert out["status"] == "unknown"
    assert out["total_with_po"] == 0
    assert out["unidentified"] == 0
    assert out["unidentified_samples"] == []


def test_card_classifies_each_prefix_into_its_bucket():
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="cv1", po_number="8955-00000111",
                    quote_number="Q1")
        _seed_order(c, order_id="cv2", po_number="8955-00000222",
                    quote_number="Q2")
        _seed_order(c, order_id="cc1", po_number="4500777",
                    quote_number="Q3")
        _seed_order(c, order_id="dsh1", po_number="4440-1234",
                    quote_number="Q4")
        c.commit()
    out = _build_card()
    assert out["total_with_po"] == 4
    assert out["by_prefix"]["CalVet"] == 2
    assert out["by_prefix"]["CCHCS"] == 1
    assert out["by_prefix"]["DSH"] == 1
    assert out["unidentified"] == 0
    assert out["status"] == "healthy"


def test_card_warns_when_some_unidentified():
    """1 of 5 unidentified = 20% — below the 30% systemic
    threshold, so it's a `warn` (operator should look) not an
    `error` (parse bug across the board)."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="cv1", po_number="8955-00000a")
        _seed_order(c, order_id="cv2", po_number="8955-00000b")
        _seed_order(c, order_id="cv3", po_number="8955-00000c")
        _seed_order(c, order_id="cv4", po_number="8955-00000d")
        _seed_order(c, order_id="bad", po_number="0000053217")
        c.commit()
    out = _build_card()
    assert out["unidentified"] == 1
    assert out["unidentified_pct"] == 20.0
    assert out["status"] == "warn"


def test_card_errors_when_majority_unidentified():
    """≥30% unidentified = systemic parse bug, not a one-off.
    Worst case on prod 2026-04-28: most CalVet POs were stored
    without the `8955-` prefix."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="ok", po_number="8955-00000ok")
        for i in range(3):
            _seed_order(c, order_id=f"strip{i}",
                        po_number=f"00000{i:05d}")
        c.commit()
    out = _build_card()
    assert out["unidentified"] == 3
    assert out["unidentified_pct"] == 75.0
    assert out["status"] == "error"


def test_card_excludes_test_orders():
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="t1", po_number="WHATEVER", is_test=1)
        c.commit()
    out = _build_card()
    assert out["total_with_po"] == 0


def test_card_excludes_empty_po_numbers():
    """Empty po_number is the no-PO-yet signal counted by the
    drift card. This card is about classifying the ones that DO
    have a PO."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="e", po_number="")
        _seed_order(c, order_id="ok", po_number="8955-00000ok")
        c.commit()
    out = _build_card()
    assert out["total_with_po"] == 1
    assert out["by_prefix"]["CalVet"] == 1


def test_unidentified_samples_sorted_by_total_desc():
    """Big-money unidentified POs come first — those are the ones
    worth chasing. A $100k unidentified PO is more important than
    a $50 one."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="small", po_number="0001", total=50.0)
        _seed_order(c, order_id="big",   po_number="0002", total=100000.0)
        _seed_order(c, order_id="mid",   po_number="0003", total=500.0)
        c.commit()
    out = _build_card()
    samples = out["unidentified_samples"]
    assert len(samples) == 3
    assert samples[0]["po_number"] == "0002"   # biggest
    assert samples[1]["po_number"] == "0003"
    assert samples[2]["po_number"] == "0001"   # smallest


def test_unidentified_samples_capped_at_10():
    with _conn() as c:
        _wipe(c)
        for i in range(15):
            _seed_order(c, order_id=f"u{i}",
                        po_number=f"NONSTD-{i:03d}",
                        total=100.0 + i)
        c.commit()
    out = _build_card()
    assert out["unidentified"] == 15
    assert len(out["unidentified_samples"]) == 10


# ── /health/quoting integration ─────────────────────────────────────────


def test_health_quoting_json_includes_po_prefix(auth_client):
    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "po_prefix" in data
    pp = data["po_prefix"]
    for k in ("status", "total_with_po", "by_prefix", "unidentified",
              "unidentified_pct", "unidentified_samples"):
        assert k in pp
    for label in ("CalVet", "CCHCS", "DSH"):
        assert label in pp["by_prefix"]


def test_health_quoting_html_renders_po_prefix_card(auth_client):
    resp = auth_client.get("/health/quoting")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")
    assert "PO prefix patterns" in body
    # Each prefix appears as a column heading.
    assert "CalVet (8955-)" in body
    assert "CCHCS (4500)" in body
    assert "DSH (4440-)" in body
    assert "UNIDENTIFIED" in body
