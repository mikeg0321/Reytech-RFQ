"""Property tests for the canonical-state truth layer.

These tests are the contract every panel migration in PR-2..PR-6 will
lean on. If a predicate's behavior drifts from the locked-in glossary,
this file fails first so the regression is caught before any consumer
ships against the wrong definition.

Two layers of coverage:

  1. Pure-Python predicate tests — `is_active_queue`, `is_real_sent`,
     `is_sourceable_po`, `is_year_revenue` exercised against dict
     records. Boundary cases for status enums, missing fields, year
     edges, sentinel PO numbers, the sent_at == created_at trap.

  2. SQL VIEW tests — seed real rows, query the view, assert the same
     records the predicates accept also surface from the view. This is
     how we catch SQL-vs-Python drift; if a future migration changes
     one definition without the other, the view-vs-predicate equality
     fails and the PR is blocked.
"""
from __future__ import annotations

import pytest

from src.core.canonical_state import (
    ACTIVE_QUEUE_EXCLUDED_STATUSES,
    INVOICED_OR_PAID_STATUSES,
    REVENUE_YEAR,
    TERMINAL_STATUSES,
    active_queue_sql_clause,
    is_active_queue,
    is_real_sent,
    is_sourceable_po,
    is_year_revenue,
    revenue_year_end,
    revenue_year_sql_clause,
    revenue_year_start,
)


# ─── Constants ───────────────────────────────────────────────────────────


def test_revenue_year_is_calendar_2026():
    """Mike (2026-05-02): explicit calendar Jan 1 – Dec 31, 2026."""
    assert REVENUE_YEAR == 2026
    assert revenue_year_start() == "2026-01-01"
    assert revenue_year_end() == "2027-01-01"  # exclusive end


def test_active_queue_excluded_set_matches_glossary():
    """Locked: sent + the 4 terminal statuses."""
    assert ACTIVE_QUEUE_EXCLUDED_STATUSES == frozenset({
        "sent", "won", "lost", "no_bid", "cancelled",
    })


def test_terminal_set_is_subset_of_excluded():
    """Sanity: all terminals are excluded, but 'sent' is excluded
    without being terminal (still moves to won/lost)."""
    assert TERMINAL_STATUSES.issubset(ACTIVE_QUEUE_EXCLUDED_STATUSES)
    assert "sent" not in TERMINAL_STATUSES
    assert "sent" in ACTIVE_QUEUE_EXCLUDED_STATUSES


# ─── is_active_queue ─────────────────────────────────────────────────────


@pytest.mark.parametrize("status", [
    "new", "parsed", "priced", "generated", "ready", "ready_to_send",
    "draft", "in_progress", "",  # missing status defaults to ''
])
def test_active_queue_includes_pre_send_statuses(status):
    assert is_active_queue({"status": status}) is True


@pytest.mark.parametrize("status", [
    "sent", "won", "lost", "no_bid", "cancelled",
    "SENT", "Won", "LOST",  # case-insensitive
])
def test_active_queue_excludes_closed_statuses(status):
    assert is_active_queue({"status": status}) is False


def test_active_queue_excludes_test_rows():
    assert is_active_queue({"status": "new", "is_test": 1}) is False
    assert is_active_queue({"status": "new", "is_test": True}) is False


def test_active_queue_handles_none_status():
    """Real-world: some imported rows have status=None."""
    assert is_active_queue({"status": None}) is True
    assert is_active_queue({}) is True  # no status field at all


# ─── is_real_sent ────────────────────────────────────────────────────────


def test_real_sent_happy_path():
    rec = {
        "status": "sent",
        "created_at": "2026-05-01T10:00:00",
        "sent_at": "2026-05-02T14:30:00",
    }
    assert is_real_sent(rec) is True


def test_real_sent_rejects_non_sent_status():
    rec = {
        "status": "generated",
        "created_at": "2026-05-01T10:00:00",
        "sent_at": "2026-05-02T14:30:00",
    }
    assert is_real_sent(rec) is False


def test_real_sent_rejects_empty_sent_at():
    """status=sent but no sent_at = misconfigured writer, not delivered."""
    rec = {
        "status": "sent",
        "created_at": "2026-05-01T10:00:00",
        "sent_at": "",
    }
    assert is_real_sent(rec) is False


def test_real_sent_rejects_sent_at_equal_to_created_at():
    """The bug Mike caught — column rendering created_at as sent_at,
    making every Sent row stamp the creation date. Treat exact match
    as "writer is lying", not "real send moment"."""
    rec = {
        "status": "sent",
        "created_at": "2026-05-02T10:00:00",
        "sent_at": "2026-05-02T10:00:00",  # exact same string
    }
    assert is_real_sent(rec) is False


def test_real_sent_accepts_legit_same_day_send():
    """Sent the same day it was created (operator was fast) — must
    still pass as long as the timestamps differ."""
    rec = {
        "status": "sent",
        "created_at": "2026-05-02T10:00:00",
        "sent_at": "2026-05-02T10:00:01",  # 1 second later
    }
    assert is_real_sent(rec) is True


def test_real_sent_rejects_unparseable_sent_at():
    rec = {
        "status": "sent",
        "created_at": "2026-05-01T10:00:00",
        "sent_at": "not-a-date",
    }
    assert is_real_sent(rec) is False


def test_real_sent_excludes_test_rows():
    rec = {
        "status": "sent",
        "created_at": "2026-05-01T10:00:00",
        "sent_at": "2026-05-02T14:30:00",
        "is_test": 1,
    }
    assert is_real_sent(rec) is False


# ─── is_sourceable_po ────────────────────────────────────────────────────


def test_sourceable_po_happy_path():
    rec = {"po_number": "0000054321", "status": "new"}
    assert is_sourceable_po(rec) is True


@pytest.mark.parametrize("status", sorted(INVOICED_OR_PAID_STATUSES))
def test_sourceable_po_excludes_invoiced_or_paid(status):
    rec = {"po_number": "0000054321", "status": status}
    assert is_sourceable_po(rec) is False


@pytest.mark.parametrize("po_number", [
    "", "N/A", "n/a", "TBD", "tbd", "PENDING", "?", "x", "xxx",
    "none", "null", "TEST-001", "test_po_123",
    None,  # missing po_number
    "??", "x-x-x",
])
def test_sourceable_po_excludes_sentinel_po_numbers(po_number):
    rec = {"po_number": po_number, "status": "new"}
    assert is_sourceable_po(rec) is False


def test_sourceable_po_excludes_already_quoted():
    """Order with a quote_number is already sourced (by us)."""
    rec = {
        "po_number": "0000054321",
        "status": "new",
        "quote_number": "R26Q-1234",
    }
    assert is_sourceable_po(rec) is False


def test_sourceable_po_includes_delivered_but_not_invoiced():
    """Delivered orders with terms-30 are still pending invoice +
    payment. Mike's def: not invoiced AND not paid → still sourceable
    relative to revenue tracking. Future iterations may tighten this
    once line-delivery tracking ships."""
    rec = {"po_number": "0000054321", "status": "delivered"}
    assert is_sourceable_po(rec) is True


def test_sourceable_po_excludes_test_rows():
    rec = {"po_number": "0000054321", "status": "new", "is_test": 1}
    assert is_sourceable_po(rec) is False


# ─── is_year_revenue ─────────────────────────────────────────────────────


@pytest.mark.parametrize("ts,expected", [
    # Inside 2026 — included.
    ("2026-01-01T00:00:00", True),
    ("2026-06-15T12:00:00", True),
    ("2026-12-31T23:59:59", True),
    # Outside 2026 — excluded.
    ("2025-12-31T23:59:59", False),
    ("2027-01-01T00:00:01", False),
    ("2024-06-15T12:00:00", False),
    # Bare date strings.
    ("2026-01-01", True),
    ("2025-12-31", False),
    # ISO with timezone.
    ("2026-06-15T12:00:00+00:00", True),
    ("2026-06-15T12:00:00Z", True),
    # Missing / malformed → False (not "current year" — the disease).
    ("", False),
    ("not-a-date", False),
    (None, False),
])
def test_year_revenue_boundary(ts, expected):
    rec = {"created_at": ts}
    assert is_year_revenue(rec, year=2026) is expected


def test_year_revenue_uses_alternate_field():
    """revenue_log rows use logged_at, not created_at."""
    rec = {"logged_at": "2026-06-15T12:00:00"}
    assert is_year_revenue(rec, year=2026, timestamp_field="logged_at") is True


def test_year_revenue_excludes_test_rows():
    rec = {"created_at": "2026-06-15T12:00:00", "is_test": 1}
    assert is_year_revenue(rec, year=2026) is False


# ─── SQL helpers ─────────────────────────────────────────────────────────


def test_active_queue_sql_clause_round_trip():
    """The SQL clause must accept exactly the same statuses as the
    Python predicate. Drift would mean the home page count and the
    revenue card disagree."""
    clause, params = active_queue_sql_clause()
    assert "is_test = 0" in clause
    assert set(params) == ACTIVE_QUEUE_EXCLUDED_STATUSES


def test_revenue_year_sql_clause_uses_half_open_interval():
    clause, params = revenue_year_sql_clause(2026)
    assert "is_test = 0" in clause
    assert ">=" in clause and "<" in clause
    assert params == ("2026-01-01", "2027-01-01")


def test_revenue_year_sql_clause_rejects_unsafe_field():
    """Defense-in-depth against caller-supplied field name → SQL injection."""
    with pytest.raises(ValueError):
        revenue_year_sql_clause(2026, timestamp_field="created_at; DROP TABLE")


# ─── SQL VIEW round-trip ─────────────────────────────────────────────────
#
# Seed real rows, run the SQL view, count how many appear, and assert
# it matches the Python predicate. This is the canary for SQL-vs-Python
# drift: if a future migration changes one definition without the
# other, this test fails.


def test_sql_view_active_queue_rfqs_matches_predicate(
        auth_client, temp_data_dir):
    """Seed 5 RFQs (3 active, 2 terminal), assert the view returns 3."""
    from src.api.data_layer import _save_single_rfq
    from src.core.db import get_db

    seeds = [
        ("v_aq_1", "new"),
        ("v_aq_2", "priced"),
        ("v_aq_3", "generated"),
        ("v_aq_4", "sent"),       # excluded
        ("v_aq_5", "won"),         # excluded
    ]
    for rid, status in seeds:
        _save_single_rfq(rid, {
            "id": rid, "status": status,
            "rfq_number": rid.upper(),
            "solicitation_number": rid.upper(),
            "line_items": [],
        })

    with get_db() as conn:
        cur = conn.execute("SELECT id FROM v_active_queue_rfqs WHERE id LIKE 'v_aq_%'")
        view_ids = {r[0] for r in cur.fetchall()}

    expected = {rid for rid, status in seeds
                if is_active_queue({"status": status, "is_test": 0})}
    assert view_ids == expected, (
        f"SQL view diverged from predicate: view={view_ids} expected={expected}"
    )
