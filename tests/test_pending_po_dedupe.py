"""Tests for _dedupe_pending_pos + _add_pending_po.

Regression for 2026-04-12 incident: prod had 80 pending PO review entries
but only 6 unique PO numbers, because _add_pending_po appended on every
detection without checking for an existing entry. The home banner
rendered all 80 into a single string and broke the page layout.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.api.dashboard import _dedupe_pending_pos


class TestDedupePendingPOs:
    """_dedupe_pending_pos collapses duplicates to a single entry per PO#."""

    def test_empty_list(self):
        assert _dedupe_pending_pos([]) == []

    def test_no_duplicates_passthrough(self):
        entries = [
            {"po_number": "4500735594", "total": 100, "items": [{"x": 1}],
             "detected_at": "2026-04-01T00:00:00"},
            {"po_number": "4500752793", "total": 200, "items": [],
             "detected_at": "2026-04-02T00:00:00"},
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 2
        assert {e["po_number"] for e in out} == {"4500735594", "4500752793"}

    def test_duplicates_collapsed_to_one(self):
        entries = [
            {"po_number": "4500735594", "total": 0, "items": [],
             "detected_at": "2026-03-23T05:36:00"},
            {"po_number": "4500735594", "total": 0, "items": [],
             "detected_at": "2026-03-23T05:37:00"},
            {"po_number": "4500735594", "total": 0, "items": [],
             "detected_at": "2026-03-23T05:38:00"},
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 1
        assert out[0]["po_number"] == "4500735594"

    def test_prefers_entry_with_nonzero_total(self):
        """When the same PO appears multiple times, prefer the one that
        actually parsed a total — that's the richer record."""
        entries = [
            {"po_number": "X", "total": 0, "items": [],
             "detected_at": "2026-03-23T05:36:00"},
            {"po_number": "X", "total": 1500.50, "items": [{"a": 1}, {"b": 2}],
             "detected_at": "2026-03-23T05:37:00"},
            {"po_number": "X", "total": 0, "items": [],
             "detected_at": "2026-03-23T05:38:00"},
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 1
        assert out[0]["total"] == 1500.50
        assert len(out[0]["items"]) == 2

    def test_drops_empty_po_numbers(self):
        """Entries with no po_number are parse failures, not actionable
        PO awards — they must not appear in the review queue."""
        entries = [
            {"po_number": "", "total": 0, "items": []},
            {"po_number": None, "total": 0, "items": []},
            {"po_number": "none", "total": 0, "items": []},
            {"po_number": "4500735594", "total": 100, "items": [{"a": 1}]},
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 1
        assert out[0]["po_number"] == "4500735594"

    def test_sorted_by_detected_at(self):
        """Output is oldest-first so /awards renders stably."""
        entries = [
            {"po_number": "B", "detected_at": "2026-04-02T00:00:00"},
            {"po_number": "A", "detected_at": "2026-04-01T00:00:00"},
            {"po_number": "C", "detected_at": "2026-04-03T00:00:00"},
        ]
        out = _dedupe_pending_pos(entries)
        assert [e["po_number"] for e in out] == ["A", "B", "C"]

    def test_non_dict_entries_skipped(self):
        out = _dedupe_pending_pos([None, "string", 42, {"po_number": "A"}])
        assert len(out) == 1
        assert out[0]["po_number"] == "A"

    def test_real_incident_shape_collapses(self):
        """Reproduces the 2026-04-12 prod shape: 80 entries, 6 uniques,
        mostly $0 from parse failures. Dedupe must collapse to 6."""
        import datetime as dt
        base = dt.datetime(2026, 3, 23, 5, 36)
        po_counts = {
            "4500735594": 28,
            "8955-0000076737": 24,
            "4500752793": 15,
            "4600012345": 3,
            "9001-0000000001": 2,
            "4500999888": 1,
        }
        entries = []
        for po, n in po_counts.items():
            for i in range(n):
                entries.append({
                    "po_number": po,
                    "total": 0,
                    "items": [],
                    "detected_at": (base + dt.timedelta(minutes=i)).isoformat(),
                })
        assert len(entries) == 73
        out = _dedupe_pending_pos(entries)
        assert len(out) == 6
        assert {e["po_number"] for e in out} == set(po_counts.keys())
