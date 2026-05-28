"""Tests for _dedupe_pending_pos + _add_pending_po.

Regression for 2026-04-12 incident: prod had 80 pending PO review entries
but only 6 unique PO numbers, because _add_pending_po appended on every
detection without checking for an existing entry. The home banner
rendered all 80 into a single string and broke the page layout.

Extended 2026-05-28 (home audit P0): dedup key is now normalized so
format variants (``8955-000076737`` vs ``0000076737`` vs ``76737-``)
collapse to one banner entry, AND phantom rows (total==0 + no buyer +
no agency) are dropped — the old behavior surfaced ``PO#xxxxx $0.00``
rows with no actionable data.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.api.dashboard import (
    _dedupe_pending_pos,
    _normalize_po_number,
    _is_phantom_po,
)


# Convenience: real entries have at least one of total / buyer / agency.
# These tests use buyer="A" / agency="X" to opt out of the phantom filter
# without changing what the dedup key sees.
def _real(po, **kw):
    return {"po_number": po, "buyer": kw.pop("buyer", "Buyer A"), **kw}


class TestDedupePendingPOs:
    """_dedupe_pending_pos collapses duplicates to a single entry per PO#."""

    def test_empty_list(self):
        assert _dedupe_pending_pos([]) == []

    def test_no_duplicates_passthrough(self):
        entries = [
            _real("4500735594", total=100, items=[{"x": 1}],
                  detected_at="2026-04-01T00:00:00"),
            _real("4500752793", total=200, items=[],
                  detected_at="2026-04-02T00:00:00"),
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 2
        assert {e["po_number"] for e in out} == {"4500735594", "4500752793"}

    def test_duplicates_collapsed_to_one(self):
        entries = [
            _real("4500735594", total=100, items=[],
                  detected_at="2026-03-23T05:36:00"),
            _real("4500735594", total=100, items=[],
                  detected_at="2026-03-23T05:37:00"),
            _real("4500735594", total=100, items=[],
                  detected_at="2026-03-23T05:38:00"),
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 1
        assert out[0]["po_number"] == "4500735594"

    def test_prefers_entry_with_nonzero_total(self):
        """When the same PO appears multiple times, prefer the one that
        actually parsed a total — that's the richer record."""
        entries = [
            _real("X", total=0, items=[],
                  detected_at="2026-03-23T05:36:00"),
            _real("X", total=1500.50, items=[{"a": 1}, {"b": 2}],
                  detected_at="2026-03-23T05:37:00"),
            _real("X", total=0, items=[],
                  detected_at="2026-03-23T05:38:00"),
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 1
        assert out[0]["total"] == 1500.50
        assert len(out[0]["items"]) == 2

    def test_drops_empty_po_numbers(self):
        """Entries with no po_number are parse failures, not actionable
        PO awards — they must not appear in the review queue."""
        entries = [
            _real("", total=0),
            _real(None, total=0),
            _real("none", total=0),
            _real("4500735594", total=100, items=[{"a": 1}]),
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 1
        assert out[0]["po_number"] == "4500735594"

    def test_sorted_by_detected_at(self):
        """Output is oldest-first so /awards renders stably."""
        entries = [
            _real("B", detected_at="2026-04-02T00:00:00"),
            _real("A", detected_at="2026-04-01T00:00:00"),
            _real("C", detected_at="2026-04-03T00:00:00"),
        ]
        out = _dedupe_pending_pos(entries)
        assert [e["po_number"] for e in out] == ["A", "B", "C"]

    def test_non_dict_entries_skipped(self):
        out = _dedupe_pending_pos([None, "string", 42, _real("A")])
        assert len(out) == 1
        assert out[0]["po_number"] == "A"

    def test_real_incident_shape_collapses(self):
        """Reproduces the 2026-04-12 prod shape: 80 entries collapse to 6
        uniques when each entry carries SOME signal (a buyer in this
        repro). Phantom rows are tested separately."""
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
                entries.append(_real(
                    po, total=0, items=[],
                    detected_at=(base + dt.timedelta(minutes=i)).isoformat(),
                ))
        assert len(entries) == 73
        out = _dedupe_pending_pos(entries)
        assert len(out) == 6
        # Note: 8955-0000076737 and 9001-0000000001 normalize to keys
        # different from each other, so all 6 originals survive.
        assert {e["po_number"] for e in out} == set(po_counts.keys())


class TestNormalizationDedup:
    """2026-05-28 audit P0: format variants of the same PO collapse to one
    banner entry. ``8955-000076737`` / ``0000076737`` / ``76737-`` all
    map to the same canonical key after _normalize_po_number runs."""

    def test_normalize_strips_non_alphanumeric_and_leading_zeros(self):
        # Mirrors the client-side normalizer in home.html exactly:
        # po.replace(/[^a-zA-Z0-9]/g, '').replace(/^0+/, '')
        # — strip non-alphanumerics, THEN strip leading zeros from the
        # combined string (not from each segment).
        assert _normalize_po_number("8955-000076737") == "8955000076737"
        assert _normalize_po_number("0000076737") == "76737"
        assert _normalize_po_number("76737-") == "76737"
        assert _normalize_po_number("4500735594") == "4500735594"
        assert _normalize_po_number("") == ""
        assert _normalize_po_number(None) == ""

    def test_leading_zero_variants_collapse(self):
        entries = [
            _real("0000076737", total=100,
                  detected_at="2026-04-01T00:00:00"),
            _real("76737-", total=100,
                  detected_at="2026-04-01T00:01:00"),
            _real("0076737", total=100,
                  detected_at="2026-04-01T00:02:00"),
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 1, "Three format variants of the same PO must collapse"

    def test_richer_record_wins_after_normalization(self):
        """When two true format-variants of the same PO arrive, keep
        the entry with the non-zero total even if the other one
        arrived first."""
        entries = [
            _real("00076737-", total=0, items=[],
                  detected_at="2026-04-01T00:00:00"),
            _real("76737", total=1500, items=[{"a": 1}],
                  detected_at="2026-04-01T00:01:00"),
        ]
        out = _dedupe_pending_pos(entries)
        # Both normalize to "76737". Non-zero total wins.
        assert len(out) == 1
        assert out[0]["total"] == 1500


class TestPhantomFilter:
    """2026-05-28 audit P0: phantom rows (total==0 AND no buyer AND no
    agency) are dropped from the response. They rendered as content-free
    ``PO#xxxxx $0.00`` rows that gave the operator nothing actionable."""

    def test_phantom_with_only_po_number_is_dropped(self):
        entries = [
            {"po_number": "12345", "total": 0, "buyer": None, "agency": ""},
        ]
        out = _dedupe_pending_pos(entries)
        assert out == []

    def test_phantom_with_blank_buyer_and_agency_is_dropped(self):
        entries = [
            {"po_number": "12345", "total": 0, "buyer": "  ", "agency": ""},
        ]
        out = _dedupe_pending_pos(entries)
        assert out == []

    def test_entry_with_buyer_is_kept(self):
        entries = [
            {"po_number": "12345", "total": 0, "buyer": "Jane Doe", "agency": ""},
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 1

    def test_entry_with_agency_is_kept(self):
        entries = [
            {"po_number": "12345", "total": 0, "buyer": None, "agency": "CCHCS"},
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 1

    def test_entry_with_nonzero_total_is_kept_even_without_buyer_agency(self):
        entries = [
            {"po_number": "12345", "total": 500, "buyer": None, "agency": ""},
        ]
        out = _dedupe_pending_pos(entries)
        assert len(out) == 1

    def test_is_phantom_po_helper(self):
        assert _is_phantom_po({"po_number": "X", "total": 0, "buyer": None, "agency": ""})
        assert _is_phantom_po({"po_number": "X", "total": "0", "buyer": "", "agency": "  "})
        assert not _is_phantom_po({"po_number": "X", "total": 100, "buyer": None, "agency": ""})
        assert not _is_phantom_po({"po_number": "X", "total": 0, "buyer": "Mike", "agency": ""})
        assert not _is_phantom_po({"po_number": "X", "total": 0, "buyer": None, "agency": "CCHCS"})
