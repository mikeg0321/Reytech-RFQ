"""Tests for PR-5 — the orders-only Gmail investigator + manual PO
rewrite endpoint.

Covers:
  (a) `_gmail_search_hint_for_po` — picks a Gmail query string from the
      classification (rfq_as_po → quote#, looks_canonical → PO,
      bare_numeric_unknown → both, sentinel → empty).
  (b) `_investigate_orders_only_row` — given a fake Gmail service, walks
      message metadata, runs `extract_canonical_po` over each subject,
      ranks canonical-prefixed candidates first.
  (c) `/api/admin/orders-only-investigate` — read-only orchestrator
      that batches the per-row investigation.
  (d) `/api/admin/orders-po-rewrite` — applies a manual rewrite with
      audit logging, idempotent on no-op, dry_run path, error cases.

Mocks Gmail by monkeypatching `src.core.gmail_api`. No real OAuth.
Live prod 2026-04-29 had 7 orders_only rows; the live verification
step runs separately via curl after merge.
"""
from __future__ import annotations

import pytest


# ── Gmail search hint helper ──────────────────────────────────────────


@pytest.mark.parametrize("po,quote,cls,expected", [
    # rfq_as_po → quote# wins (the PO field is bogus)
    ("RFQ882023",  "Q882023", "rfq_as_po",  '"Q882023"'),
    ("RFQ Gowns",  "R23O20",  "rfq_as_po",  '"R23O20"'),
    # looks_canonical → PO directly
    ("8955-0000050349", "Q1", "looks_canonical", '"8955-0000050349"'),
    # bare_numeric_unknown → both PO and quote# OR'd
    ("10820146",   "R26Q42",  "bare_numeric_unknown",
     '"10820146" OR "R26Q42"'),
    # sentinel → empty (no useful query)
    ("TEST",       "Q1",      "sentinel",   ""),
    # unknown with both → PO and quote#
    ("ABC",        "Q1",      "unknown",    '"ABC" OR "Q1"'),
    # unknown with empty quote → PO only
    ("ABC",        "",        "unknown",    '"ABC"'),
    # rfq_as_po without quote# → falls through to OR
    ("RFQ Gowns",  "",        "rfq_as_po",  '"RFQ Gowns"'),
    # Whitespace tolerated
    (" 8955-0000050349 ", "", "looks_canonical",
     '"8955-0000050349"'),
])
def test_gmail_search_hint_for_po(po, quote, cls, expected):
    from src.api.modules.routes_health import _gmail_search_hint_for_po
    assert _gmail_search_hint_for_po(po, quote, cls) == expected


# ── Per-row investigator with a fake Gmail service ────────────────────


class _FakeGmail:
    """Minimal stand-in for googleapiclient.discovery.Resource. Used
    only as a sentinel — `gmail_api.list_message_ids` and
    `get_message_metadata` are monkeypatched to read from this
    object's `_msgs` map directly."""

    def __init__(self, msgs):
        self._msgs = msgs  # {msg_id: {subject, snippet}}

    def ids_for_query(self, query):
        return list(self._msgs.keys())


@pytest.fixture
def patch_gmail(monkeypatch):
    """Patches src.core.gmail_api.list_message_ids /
    get_message_metadata / is_configured to drive the investigator
    deterministically. Returns a setter the test calls with the
    fake message map."""
    from src.core import gmail_api as _ga

    state = {"msgs": {}, "configured": True}

    def _list_ids(service, query="", max_results=500):
        return list(state["msgs"].keys())

    def _meta(service, msg_id):
        return state["msgs"].get(msg_id, {})

    monkeypatch.setattr(_ga, "list_message_ids", _list_ids)
    monkeypatch.setattr(_ga, "get_message_metadata", _meta)
    monkeypatch.setattr(_ga, "is_configured",
                        lambda: state["configured"])
    monkeypatch.setattr(_ga, "get_service",
                        lambda inbox_name="sales": _FakeGmail(state["msgs"]))

    def _set(msgs=None, configured=True):
        state["msgs"] = msgs or {}
        state["configured"] = configured

    return _set


def test_investigator_extracts_canonical_po_from_subject(patch_gmail):
    from src.api.modules.routes_intel_ops import (
        _investigate_orders_only_row,
    )
    patch_gmail({
        "m1": {"subject": "PO 8955-0000076737 Atascadero",
               "snippet": "issued 2026-04-15"},
    })
    row = {
        "order_id": "o1",
        "po_number": "RFQ882023",
        "quote_number": "Q882023",
        "classification": "rfq_as_po",
        "gmail_search_hint": '"Q882023"',
    }
    out = _investigate_orders_only_row(row, _FakeGmail({}))
    assert out["matched_message_count"] == 1
    cands = [c["canonical"] for c in out["candidates"]]
    assert "8955-0000076737" in cands
    assert out["suggested_rewrite"] == "8955-0000076737"


def test_investigator_ranks_canonical_prefix_first(patch_gmail):
    """A subject with both a date-like long digit run and a real
    8955- PO must surface the prefixed PO as the suggested rewrite,
    not the date."""
    from src.api.modules.routes_intel_ops import (
        _investigate_orders_only_row,
    )
    patch_gmail({
        "m1": {"subject": "Date 02192026 Order",
               "snippet": "PO 8955-0000044935 details"},
    })
    row = {
        "order_id": "o2",
        "po_number": "10820146",
        "quote_number": "Q1",
        "classification": "bare_numeric_unknown",
        "gmail_search_hint": '"10820146" OR "Q1"',
    }
    out = _investigate_orders_only_row(row, _FakeGmail({}))
    assert out["suggested_rewrite"] == "8955-0000044935"


def test_investigator_skips_when_no_search_query(patch_gmail):
    """sentinel-classified rows have an empty hint; investigator
    must not call Gmail."""
    from src.api.modules.routes_intel_ops import (
        _investigate_orders_only_row,
    )
    patch_gmail({"m1": {"subject": "anything", "snippet": ""}})
    row = {
        "order_id": "o3",
        "po_number": "TEST",
        "quote_number": "",
        "classification": "sentinel",
        "gmail_search_hint": "",
    }
    out = _investigate_orders_only_row(row, _FakeGmail({}))
    assert out["matched_message_count"] == 0
    assert out["candidates"] == []
    assert out["suggested_rewrite"] == ""


def test_investigator_does_not_suggest_own_po_back(patch_gmail):
    """If Gmail surfaces the same PO that's already on the row,
    suggested_rewrite must skip it (no point rewriting to itself)."""
    from src.api.modules.routes_intel_ops import (
        _investigate_orders_only_row,
    )
    patch_gmail({
        "m1": {"subject": "PO 8955-0000050349 confirmation",
               "snippet": ""},
    })
    row = {
        "order_id": "o4",
        "po_number": "8955-0000050349",
        "quote_number": "Q1",
        "classification": "looks_canonical",
        "gmail_search_hint": '"8955-0000050349"',
    }
    out = _investigate_orders_only_row(row, _FakeGmail({}))
    assert out["candidates"]  # got a hit
    # But it shouldn't suggest rewriting to the same value
    assert out["suggested_rewrite"] == ""


def test_investigator_isolates_per_message_errors(monkeypatch, patch_gmail):
    from src.core import gmail_api as _ga
    from src.api.modules.routes_intel_ops import (
        _investigate_orders_only_row,
    )
    patch_gmail({
        "m1": {"subject": "8955-0000044935 ok", "snippet": ""},
        "m2": {"subject": "wont-be-read", "snippet": ""},
    })

    real_meta = _ga.get_message_metadata
    def _flaky(service, msg_id):
        if msg_id == "m2":
            raise RuntimeError("simulated 500")
        return real_meta(service, msg_id)
    monkeypatch.setattr(_ga, "get_message_metadata", _flaky)

    row = {
        "order_id": "o5",
        "po_number": "RFQ Gowns",
        "quote_number": "Q1",
        "classification": "rfq_as_po",
        "gmail_search_hint": '"Q1"',
    }
    out = _investigate_orders_only_row(row, _FakeGmail({}))
    # m1 still produced a candidate even though m2 raised
    cands = [c["canonical"] for c in out["candidates"]]
    assert "8955-0000044935" in cands
    assert out["error"] == ""  # batch-level error stays empty


# ── Endpoint-level tests ──────────────────────────────────────────────


def _seed_orders(conn, rows):
    try:
        conn.execute("DELETE FROM orders")
    except Exception:
        pass
    for r in rows:
        conn.execute("""
            INSERT INTO orders
              (id, quote_number, po_number, agency, institution,
               total, status, items, created_at, updated_at, is_test)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (r["id"], r.get("quote_number", "Q1"),
              r["po_number"], r.get("agency", "CDCR"),
              r.get("institution", ""),
              float(r.get("total", 100)), "open", "[]",
              "2026-04-28T10:00:00", "2026-04-28T10:00:00",
              int(r.get("is_test", 0))))


def test_investigate_endpoint_503_when_gmail_unconfigured(
    auth_client, patch_gmail,
):
    patch_gmail(configured=False)
    resp = auth_client.post(
        "/api/admin/orders-only-investigate",
        json={},
    )
    assert resp.status_code == 503
    data = resp.get_json()
    assert data["ok"] is False
    assert data["error"] == "gmail_api_not_configured"


def test_investigate_endpoint_returns_suggestions(
    auth_client, patch_gmail,
):
    from src.core.db import get_db
    with get_db() as c:
        # Wipe SCPRS so all rows fall into orders_only
        try:
            c.execute("DELETE FROM scprs_reytech_wins")
        except Exception:
            pass
        _seed_orders(c, [
            {"id": "o1", "po_number": "RFQ882023",
             "quote_number": "Q882023"},
        ])
        c.commit()
    patch_gmail({
        "m1": {"subject": "PO 8955-0000076737 Atascadero", "snippet": ""},
    })
    resp = auth_client.post(
        "/api/admin/orders-only-investigate",
        json={"order_ids": ["o1"]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["examined"] == 1
    assert data["rows_with_suggestion"] == 1
    assert data["rows"][0]["suggested_rewrite"] == "8955-0000076737"


def test_investigate_endpoint_filters_by_order_ids(
    auth_client, patch_gmail,
):
    from src.core.db import get_db
    with get_db() as c:
        try:
            c.execute("DELETE FROM scprs_reytech_wins")
        except Exception:
            pass
        _seed_orders(c, [
            {"id": "o1", "po_number": "RFQ882023",
             "quote_number": "Q882023"},
            {"id": "o2", "po_number": "10820146", "quote_number": "Q26"},
        ])
        c.commit()
    patch_gmail({
        "m1": {"subject": "8955-0000076737", "snippet": ""},
    })
    resp = auth_client.post(
        "/api/admin/orders-only-investigate",
        json={"order_ids": ["o2"]},
    )
    data = resp.get_json()
    assert data["examined"] == 1
    assert data["rows"][0]["order_id"] == "o2"


# ── PO rewrite endpoint ───────────────────────────────────────────────


def test_po_rewrite_applies_update(auth_client):
    from src.core.db import get_db
    with get_db() as c:
        _seed_orders(c, [
            {"id": "o1", "po_number": "RFQ882023"},
        ])
        c.commit()
    resp = auth_client.post(
        "/api/admin/orders-po-rewrite",
        json={"order_id": "o1", "new_po": "8955-0000076737",
              "reason": "investigator suggested"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["old_po"] == "RFQ882023"
    assert data["new_po"] == "8955-0000076737"
    assert data["rows_updated"] == 1
    assert data["audit_logged"] is True
    with get_db() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id='o1'"
        ).fetchone()
    assert row["po_number"] == "8955-0000076737"


def test_po_rewrite_dry_run_does_not_write(auth_client):
    from src.core.db import get_db
    with get_db() as c:
        _seed_orders(c, [
            {"id": "o1", "po_number": "RFQ882023"},
        ])
        c.commit()
    resp = auth_client.post(
        "/api/admin/orders-po-rewrite",
        json={"order_id": "o1", "new_po": "8955-0000076737",
              "dry_run": True},
    )
    data = resp.get_json()
    assert data["dry_run"] is True
    assert data["rows_updated"] == 0
    with get_db() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id='o1'"
        ).fetchone()
    assert row["po_number"] == "RFQ882023"


def test_po_rewrite_idempotent_noop(auth_client):
    """If the new_po already matches what's stored, return ok with
    rows_updated=0 — second-run safety."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_orders(c, [
            {"id": "o1", "po_number": "8955-0000076737"},
        ])
        c.commit()
    resp = auth_client.post(
        "/api/admin/orders-po-rewrite",
        json={"order_id": "o1", "new_po": "8955-0000076737"},
    )
    data = resp.get_json()
    assert data["ok"] is True
    assert data["noop"] is True
    assert data["rows_updated"] == 0


def test_po_rewrite_404_unknown_order(auth_client):
    from src.core.db import get_db
    with get_db() as c:
        _seed_orders(c, [])
        c.commit()
    resp = auth_client.post(
        "/api/admin/orders-po-rewrite",
        json={"order_id": "missing", "new_po": "8955-0000076737"},
    )
    assert resp.status_code == 404
    data = resp.get_json()
    assert data["ok"] is False
    assert "missing" in data["error"]


def test_po_rewrite_400_missing_fields(auth_client):
    resp = auth_client.post(
        "/api/admin/orders-po-rewrite",
        json={"order_id": "o1"},
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert data["ok"] is False


def test_po_rewrite_400_no_body(auth_client):
    """Empty body must return 400 cleanly, not 415 (the no-Content-Type
    bug we fixed in PR #642)."""
    resp = auth_client.post("/api/admin/orders-po-rewrite")
    assert resp.status_code == 400
