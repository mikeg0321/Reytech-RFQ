"""Pin canonical-first resolver in `Quote.from_legacy_dict` (2026-05-07).

The 2026-05-05 pc_177b18e6 incident: a writer updated `pc["items"]`
(2 entries) but `pc["line_items"]` stayed stale (1 entry). The
`quote_model_v2_enabled` adapter read `line_items` first (`d.get(
"line_items") or d.get("items") or ...`), so the UI rendered 1 row
even though SQLite had 2. Mike had to flip the v2 flag off prod
mitigate.

PR #796/#797 closed the writer-side gap (sync helper writes both).
This PR closes the reader-side gap by:

  1. Switching the read order so canonical `items` wins.
  2. Logging a WARNING if the two aliases disagree, so any future
     writer that bypasses the sync helper surfaces in logs before
     it reaches a user.

These tests pin both behaviors. Re-enabling `quote_model_v2_enabled`
prod-side is unblocked once the ratchet ships.
"""
from __future__ import annotations

import logging

import pytest

from src.core.quote_model import Quote


def _base_record(items, line_items=None):
    """Minimal-shape PC record for adapter testing."""
    rec = {
        "id": "pc_alias_test",
        "pc_number": "PC-ALIAS-001",
        "items": items,
    }
    if line_items is not None:
        rec["line_items"] = line_items
    return rec


def _make_item(desc, qty=1, unit_cost=10.0):
    return {
        "description": desc,
        "qty": qty,
        "unit_cost": unit_cost,
        "uom": "EA",
    }


def test_canonical_items_wins_when_line_items_is_stale():
    """The 2026-05-05 incident shape: items=2, line_items=1 (stale).
    Resolver MUST render 2 rows (the canonical `items`)."""
    rec = _base_record(
        items=[_make_item("Widget A"), _make_item("Widget B")],
        line_items=[_make_item("Widget A")],  # stale, missing B
    )
    q = Quote.from_legacy_dict(rec, doc_type="pc")
    assert len(q.line_items) == 2
    descriptions = [li.description for li in q.line_items]
    assert descriptions == ["Widget A", "Widget B"]


def test_canonical_items_wins_when_line_items_is_extra():
    """Inverse drift shape: items=1, line_items=2 (line_items has
    leftover row from before deletion). Resolver renders 1 row from
    canonical `items`."""
    rec = _base_record(
        items=[_make_item("Widget A")],
        line_items=[_make_item("Widget A"),
                    _make_item("Widget DELETED")],
    )
    q = Quote.from_legacy_dict(rec, doc_type="pc")
    assert len(q.line_items) == 1
    assert q.line_items[0].description == "Widget A"


def test_alias_drift_logs_warning(caplog):
    """When divergence exists, the resolver logs a WARNING so it shows
    up in CI/prod logs before a user notices the rendered count is
    wrong. Lengths and the record id must both appear in the message."""
    rec = _base_record(
        items=[_make_item("A"), _make_item("B"), _make_item("C")],
        line_items=[_make_item("A")],
    )
    with caplog.at_level(logging.WARNING, logger="src.core.quote_model"):
        Quote.from_legacy_dict(rec, doc_type="pc")
    msgs = [r.getMessage() for r in caplog.records
            if r.levelno >= logging.WARNING]
    drift_msgs = [m for m in msgs if "alias drift" in m]
    assert drift_msgs, f"expected alias drift warning; got: {msgs}"
    assert "items=3" in drift_msgs[0]
    assert "line_items=1" in drift_msgs[0]
    assert "pc_alias_test" in drift_msgs[0]


def test_no_warning_when_aliases_agree(caplog):
    """Steady state — both aliases match (post-PR #796 sync). The
    resolver should NOT spam a warning every render."""
    rec = _base_record(
        items=[_make_item("A"), _make_item("B")],
        line_items=[_make_item("A"), _make_item("B")],
    )
    with caplog.at_level(logging.WARNING, logger="src.core.quote_model"):
        Quote.from_legacy_dict(rec, doc_type="pc")
    drift_msgs = [r.getMessage() for r in caplog.records
                  if r.levelno >= logging.WARNING
                  and "alias drift" in r.getMessage()]
    assert not drift_msgs, (
        f"unexpected alias drift warning when aligned: {drift_msgs}")


def test_falls_through_to_parsed_when_top_level_empty():
    """Pre-existing fallback: when `items` and `line_items` are both
    absent, fall through to `parsed.line_items`. Must still work."""
    rec = {
        "id": "pc_parsed_only",
        "pc_number": "PC-PARSED",
        "parsed": {"line_items": [_make_item("From parsed")]},
    }
    q = Quote.from_legacy_dict(rec, doc_type="pc")
    assert len(q.line_items) == 1
    assert q.line_items[0].description == "From parsed"


def test_empty_items_returns_no_line_items():
    """Sanity: empty list resolves to zero line items (not crash)."""
    rec = _base_record(items=[])
    q = Quote.from_legacy_dict(rec, doc_type="pc")
    assert q.line_items == []
