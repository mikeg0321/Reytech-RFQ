"""Shared SCPRS line classifier — uses real product_catalog instead of
the legacy 25-key hardcoded dict.

Mike P0 2026-05-11 (cross-sell hunting arc): both `cchcs_intel_puller`
and `scprs_universal_pull` had their own keyword dicts that drove the
`reytech_sells` / `opportunity_flag` columns on scprs_po_lines. The
cross-sell intel surface — "buyer X bought items from competitor Y
that Reytech sells" — runs on those flags. With only 25 keywords,
most matches were missed; the surface was inert.

The new shared `src.core.scprs_classifier.classify_line()` delegates
to `product_catalog.match_item()` (UPC / supplier SKU / mfg# / token
overlap at 0.65 threshold), falling back to keywords for adjacent
categories Reytech doesn't have in catalog yet.

These tests pin:
  * Threshold gate (0.65) is honored
  * Keyword fallback fires when catalog match is below threshold
  * `match_source` correctly distinguishes catalog vs keyword vs other
"""
from __future__ import annotations

from unittest.mock import patch

from src.core.scprs_classifier import classify_line, MATCH_CONFIDENCE_THRESHOLD


# ─── Catalog match (Tier 1) ──────────────────────────────────────────────


def test_catalog_match_above_threshold_sets_win_back():
    """A product_catalog match at >= 0.65 confidence stamps WIN_BACK."""
    fake_match = [{
        "id": 42,
        "name": "Nitrile Exam Gloves Box of 100",
        "sku": "NITRILE-M",
        "mfg_number": "GLO-NIT-M",
        "category": "exam_gloves",
        "match_confidence": 0.92,
    }]
    with patch("src.agents.product_catalog.match_item", return_value=fake_match):
        out = classify_line("Nitrile examination glove medium", item_id="GLO-NIT-M")
    assert out["reytech_sells"] == 1
    assert out["opportunity_flag"] == "WIN_BACK"
    assert out["reytech_sku"] == "NITRILE-M"
    assert out["category"] == "exam_gloves"
    assert out["match_source"] == "catalog"
    assert out["match_confidence"] == 0.92


def test_catalog_match_falls_back_to_mfg_number_when_sku_absent():
    """When the catalog row has mfg_number but no sku, reytech_sku
    falls back to mfg_number so the surface still has something to
    show the operator."""
    fake_match = [{
        "id": 7,
        "name": "Penlight",
        "sku": None,
        "mfg_number": "MCK-161574",
        "category": "clinical",
        "match_confidence": 0.78,
    }]
    with patch("src.agents.product_catalog.match_item", return_value=fake_match):
        out = classify_line("Penlight white light disposable", item_id="161574")
    assert out["reytech_sku"] == "MCK-161574"


def test_catalog_match_below_threshold_falls_through_to_keyword():
    """A 0.55 catalog match is below the 0.65 gate — must fall through
    to keyword fallback (not stamp WIN_BACK on a weak match)."""
    fake_match = [{
        "id": 99,
        "sku": "WRONG-SKU",
        "category": "wrong_category",
        "match_confidence": 0.55,  # below 0.65
    }]
    with patch("src.agents.product_catalog.match_item", return_value=fake_match):
        out = classify_line("Nitrile exam gloves", item_id="")
    # Keyword fallback caught "nitrile" → WIN_BACK with the NITRILE-M sku
    assert out["match_source"] == "keyword"
    assert out["opportunity_flag"] == "WIN_BACK"
    assert out["reytech_sku"] == "NITRILE-M"


# ─── Keyword fallback (Tier 2) ───────────────────────────────────────────


def test_keyword_fallback_win_back():
    """Catalog returns nothing — keyword fallback fires WIN_BACK for
    items Reytech is known to sell."""
    with patch("src.agents.product_catalog.match_item", return_value=[]):
        out = classify_line("Adult brief size large")
    assert out["match_source"] == "keyword"
    assert out["reytech_sells"] == 1
    assert out["opportunity_flag"] == "WIN_BACK"
    assert out["reytech_sku"] == "BRIEFS-M"
    assert out["category"] == "incontinence"


def test_keyword_fallback_gap_item():
    """Adjacent-category items Reytech doesn't sell get GAP_ITEM."""
    with patch("src.agents.product_catalog.match_item", return_value=[]):
        out = classify_line("Sterile gauze pad 4x4")
    assert out["match_source"] == "keyword"
    assert out["reytech_sells"] == 0
    assert out["opportunity_flag"] == "GAP_ITEM"
    assert out["category"] == "wound_care"


def test_keyword_fallback_handles_n95():
    """N95 respirator → WIN_BACK (Reytech sells)."""
    with patch("src.agents.product_catalog.match_item", return_value=[]):
        out = classify_line("3M N95 1860 respirator")
    assert out["opportunity_flag"] == "WIN_BACK"
    assert out["category"] == "respiratory"


# ─── Tier 3: no match ────────────────────────────────────────────────────


def test_no_match_returns_other():
    """Items that match nothing — neither catalog nor keyword — get
    category=other and no opportunity flag."""
    with patch("src.agents.product_catalog.match_item", return_value=[]):
        out = classify_line("Caterpillar D9 bulldozer tracks", item_id="CAT-D9-T")
    assert out["category"] == "other"
    assert out["reytech_sells"] == 0
    assert out["opportunity_flag"] is None
    assert out["match_source"] == "other"


# ─── Robustness ──────────────────────────────────────────────────────────


def test_catalog_module_unavailable_does_not_crash():
    """If product_catalog import fails (test env without DB, etc.), the
    classifier falls through to keyword without raising. SCPRS ingest
    must never crash because the catalog module is broken."""
    with patch("src.agents.product_catalog.match_item",
               side_effect=ImportError("simulated")):
        out = classify_line("Adult brief size large")
    # Keyword still works
    assert out["match_source"] == "keyword"
    assert out["opportunity_flag"] == "WIN_BACK"


def test_empty_description_returns_other():
    """Defensive: empty description → category=other, no flag, no crash."""
    with patch("src.agents.product_catalog.match_item", return_value=[]):
        out = classify_line("", item_id="")
    assert out["category"] == "other"
    assert out["reytech_sells"] == 0
    assert out["opportunity_flag"] is None


def test_threshold_constant_is_065():
    """Pin the threshold constant. If product_catalog raises its own
    internal Jaccard cutoff, this test reminds us to mirror it here."""
    assert MATCH_CONFIDENCE_THRESHOLD == 0.65


# ─── Confidence at the boundary ──────────────────────────────────────────


def test_exactly_at_threshold_is_catalog_match():
    """Confidence == threshold (0.65) passes the gate — `>=`, not `>`."""
    fake_match = [{
        "id": 1, "sku": "X", "category": "test", "match_confidence": 0.65,
    }]
    with patch("src.agents.product_catalog.match_item", return_value=fake_match):
        out = classify_line("Something", item_id="X")
    assert out["match_source"] == "catalog"
    assert out["reytech_sells"] == 1


def test_just_below_threshold_falls_through():
    """0.649 fails the gate."""
    fake_match = [{
        "id": 1, "sku": "X", "category": "test", "match_confidence": 0.649,
    }]
    with patch("src.agents.product_catalog.match_item", return_value=fake_match):
        out = classify_line("Caterpillar D9 bulldozer", item_id="CAT-D9")
    assert out["match_source"] != "catalog"
