"""Tests for the CCHCS packet → PC item matcher.

Built 2026-04-13 overnight. See _overnight_review/MORNING_REVIEW.md.
"""
import pytest

from src.agents.cchcs_pc_matcher import (
    match_packet_to_pcs,
    _tokens,
    _jaccard,
    _normalize_mfg,
    _normalize_sol,
)


@pytest.fixture
def packet():
    """A CCHCS packet-shaped dict with 1 line item (DS8178 scanner)."""
    return {
        "header": {
            "solicitation_number": "10843276",
            "institution": "CA State Prison Sacramento",
        },
        "line_items": [
            {
                "row_index": 1,
                "qty": 15,
                "uom": "SETS",
                "description": "Handheld Scanner w/ USB cable and standard cradle",
                "mfg_number": "DS8178",
                "part_number": "DS8178",
            }
        ],
    }


def _make_pc(pcid, sol=None, items=None, status="parsed"):
    return {
        "id": pcid,
        "pc_number": sol or pcid,
        "status": status,
        "items": items or [],
    }


def _make_item(mfg="", desc="", cost=0.0, price=0.0, upc=""):
    return {
        "mfg_number": mfg,
        "part_number": mfg,
        "upc": upc,
        "description": desc,
        "unit_price": price,
        "pricing": {"unit_cost": cost, "recommended_price": price},
    }


class TestTokenization:
    def test_strips_stopwords(self):
        assert "the" not in _tokens("the quick brown fox")
        assert "quick" in _tokens("the quick brown fox")

    def test_lowercases_and_drops_punct(self):
        t = _tokens("DS-8178, Handheld Scanner!")
        assert "ds" in t or "ds8178" in t or "8178" in t
        assert "handheld" in t
        assert "scanner" in t

    def test_empty_string(self):
        assert _tokens("") == set()

    def test_jaccard_identical(self):
        a = {"apple", "banana", "cherry"}
        assert _jaccard(a, a) == 1.0

    def test_jaccard_disjoint(self):
        assert _jaccard({"a"}, {"b"}) == 0.0

    def test_jaccard_half_overlap(self):
        a = {"a", "b"}
        b = {"b", "c"}
        # Intersection: {"b"} (size 1), Union: {"a","b","c"} (size 3)
        assert abs(_jaccard(a, b) - 1 / 3) < 0.01


class TestNormalizers:
    def test_normalize_mfg(self):
        assert _normalize_mfg("ds8178") == "DS8178"
        assert _normalize_mfg(" DS 8178 ") == "DS8178"
        assert _normalize_mfg("") == ""

    def test_normalize_sol(self):
        assert _normalize_sol("10843276") == "10843276"
        assert _normalize_sol("#10843276") == "10843276"
        assert _normalize_sol(" 10843276 ") == "10843276"


class TestMatchByMfgNumber:
    def test_exact_mfg_match(self, packet):
        pcs = {
            "pc_xyz": _make_pc("pc_xyz", items=[
                _make_item(mfg="DS8178", desc="Barcode Scanner",
                           cost=295, price=395),
            ]),
        }
        r = match_packet_to_pcs(packet, pcs)
        assert r["matched_count"] == 1
        assert r["unmatched_count"] == 0
        assert 1 in r["price_overrides"]
        o = r["price_overrides"][1]
        assert o["unit_cost"] == 295.0
        assert o["unit_price"] == 395.0
        assert o["source_pc_id"] == "pc_xyz"
        assert "mfg_number" in o["match_strategy"]

    def test_dismissed_pc_ignored(self, packet):
        pcs = {
            "pc_bad": _make_pc("pc_bad", status="dismissed", items=[
                _make_item(mfg="DS8178", cost=200, price=250),
            ]),
            "pc_good": _make_pc("pc_good", items=[
                _make_item(mfg="DS8178", cost=295, price=395),
            ]),
        }
        r = match_packet_to_pcs(packet, pcs)
        assert r["matched_count"] == 1
        assert r["price_overrides"][1]["source_pc_id"] == "pc_good"

    def test_nothing_matches(self, packet):
        pcs = {
            "pc_other": _make_pc("pc_other", items=[
                _make_item(mfg="ABC123", desc="Totally different thing"),
            ]),
        }
        r = match_packet_to_pcs(packet, pcs)
        assert r["matched_count"] == 0
        assert r["unmatched_count"] == 1
        assert r["price_overrides"] == {}


class TestMatchBySolicitation:
    def test_sol_match_prefers_sol_pool(self, packet):
        """When a PC exists with the same sol#, its items are searched
        first (and reported with solicitation+mfg_number strategy)."""
        pcs = {
            "pc_sol": _make_pc("pc_sol", sol="10843276", items=[
                _make_item(mfg="DS8178", cost=300, price=400),
            ]),
            "pc_other": _make_pc("pc_other", items=[
                _make_item(mfg="DS8178", cost=295, price=395),
            ]),
        }
        r = match_packet_to_pcs(packet, pcs)
        assert r["matched_count"] == 1
        o = r["price_overrides"][1]
        # Should prefer the sol-matched PC
        assert o["source_pc_id"] == "pc_sol"
        assert o["unit_price"] == 400.0
        assert "solicitation" in o["match_strategy"]


class TestMatchByDescription:
    def test_description_fallback(self):
        packet = {
            "header": {},
            "line_items": [
                {
                    "row_index": 1,
                    "description": "Handheld Scanner with USB cable",
                    "mfg_number": "",  # No MFG# → forces description fallback
                    "qty": 1, "uom": "EA",
                }
            ],
        }
        pcs = {
            "pc_scan": _make_pc("pc_scan", items=[
                _make_item(desc="Handheld barcode scanner USB", cost=100, price=150),
            ]),
        }
        r = match_packet_to_pcs(packet, pcs)
        assert r["matched_count"] == 1
        assert r["price_overrides"][1]["match_strategy"] == "description"

    def test_description_below_threshold_rejected(self):
        packet = {
            "header": {},
            "line_items": [
                {
                    "row_index": 1,
                    "description": "Handheld scanner",
                    "mfg_number": "",
                    "qty": 1, "uom": "EA",
                }
            ],
        }
        pcs = {
            "pc_other": _make_pc("pc_other", items=[
                _make_item(desc="Copy paper 8.5x11 white"),
            ]),
        }
        r = match_packet_to_pcs(packet, pcs)
        assert r["matched_count"] == 0
        assert r["unmatched_count"] == 1


class TestMatchedButNoPrice:
    def test_match_with_zero_price_is_not_override(self, packet):
        """If a PC item matches by MFG# but has no cost or price,
        don't put it in price_overrides (leaves row blank in output)."""
        pcs = {
            "pc_empty": _make_pc("pc_empty", items=[
                _make_item(mfg="DS8178", desc="scanner", cost=0, price=0),
            ]),
        }
        r = match_packet_to_pcs(packet, pcs)
        assert r["price_overrides"] == {}
        assert r["unmatched_count"] == 1
        # But the report should show the match was found
        assert r["report"][0]["source_pc_id"] == "pc_empty"
        assert "no cost/price" in r["report"][0]["reason"]

    def test_cost_only_triggers_default_markup(self, packet):
        """PC item has cost but no price → apply 25% default markup."""
        pcs = {
            "pc_cost_only": _make_pc("pc_cost_only", items=[
                _make_item(mfg="DS8178", cost=400, price=0),
            ]),
        }
        r = match_packet_to_pcs(packet, pcs)
        assert r["matched_count"] == 1
        o = r["price_overrides"][1]
        assert o["unit_cost"] == 400.0
        # 400 * 1.25 = 500
        assert o["unit_price"] == 500.0


class TestReportShape:
    def test_every_row_gets_a_report_entry(self):
        """Whether matched or not, every packet row appears in report."""
        packet = {
            "header": {},
            "line_items": [
                {"row_index": 1, "mfg_number": "DS8178", "description": "Scanner", "qty": 1},
                {"row_index": 2, "mfg_number": "NOTINDB", "description": "Nothing", "qty": 1},
            ],
        }
        pcs = {
            "pc_one": _make_pc("pc_one", items=[
                _make_item(mfg="DS8178", cost=100, price=150),
            ]),
        }
        r = match_packet_to_pcs(packet, pcs)
        assert len(r["report"]) == 2
        assert r["matched_count"] == 1
        assert r["unmatched_count"] == 1


class TestEmptyInputs:
    def test_empty_packet(self):
        r = match_packet_to_pcs({"header": {}, "line_items": []}, {})
        assert r["matched_count"] == 0
        assert r["price_overrides"] == {}

    def test_empty_pcs_dict(self, packet):
        r = match_packet_to_pcs(packet, {})
        assert r["matched_count"] == 0
        assert r["unmatched_count"] == 1
