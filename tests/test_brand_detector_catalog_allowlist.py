"""Regression: brand detector uses catalog allowlist + expanded stop list.

Live verification on prod 2026-04-24 (PC pc_307630ce) found the brand
detector firing on parsing-artifact tokens like "SHINY", "PNEUMATIC",
"HANDLE", "ALUMINUM" — capitalized words that aren't actually brands.

Two-layer fix:
1. Expanded `_BRAND_STOP_NOUNS` with materials / colors / adjectives /
   process words.
2. **Catalog allowlist** as the strong signal: when
   `product_catalog.manufacturer` has data, the token must appear in
   that allowlist. When the catalog is empty (tests, fresh install),
   fall back to heuristic-only so the signal isn't silently killed.

Detector remains transparency-only — never drops comps, only tags
samples with `brand_anchored: True | False | None`.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.core import pricing_oracle_v2 as oracle
from src.core.pricing_oracle_v2 import (
    _detect_brand_token,
    _BRAND_STOP_NOUNS,
    _reset_known_brands_cache_for_tests,
    _tokenize,
)


@pytest.fixture(autouse=True)
def _clear_brand_cache():
    """Each test starts with a clean cache + no override."""
    oracle._KNOWN_BRANDS_OVERRIDE = None
    _reset_known_brands_cache_for_tests()
    yield
    oracle._KNOWN_BRANDS_OVERRIDE = None
    _reset_known_brands_cache_for_tests()


# ── Regression: false positives on parsing-artifact words ─────────────


def test_shiny_first_token_is_not_a_brand():
    """'SHINY REPLACEMENT PAD FITS STAMP R-532' — SHINY is an
    adjective, not a brand."""
    src = "SHINY REPLACEMENT PAD FITS STAMP R-532"
    assert _detect_brand_token(src, _tokenize(src)) is False


def test_pneumatic_first_token_is_not_a_brand():
    """'PNEUMATIC WHEELS 44 x 25 x 37" ALUMINUM PLATFORM TRUCK'."""
    src = 'PNEUMATIC WHEELS 44 x 25 x 37" ALUMINUM PLATFORM TRUCK'
    assert _detect_brand_token(src, _tokenize(src)) is False


def test_handle_first_token_is_not_a_brand():
    """'HANDLE FOR ALUMINUM PLATFORM TRUCKS 24"'."""
    src = 'HANDLE FOR ALUMINUM PLATFORM TRUCKS 24"'
    assert _detect_brand_token(src, _tokenize(src)) is False


def test_aluminum_first_token_is_not_a_brand():
    src = "ALUMINUM PLATFORM TRUCK 24x48"
    assert _detect_brand_token(src, _tokenize(src)) is False


def test_statement_first_token_is_not_a_brand():
    """Buyer-email parsing artifact: 'STATEMENT OF COMPLIANCE - The above'."""
    src = "STATEMENT OF COMPLIANCE - The above signed as compliant"
    assert _detect_brand_token(src, _tokenize(src)) is False


def test_payment_discount_offers_is_not_a_brand():
    """'*Payment Discount Offers' — header text, not a product."""
    src = "*Payment Discount Offers"
    assert _detect_brand_token(src, _tokenize(src)) is False


def test_color_first_token_is_not_a_brand():
    for color in ("BLACK FLAT SHELF UTILITY CART", "WHITE BANDAGE 3x10",
                  "RED INK PAD"):
        assert _detect_brand_token(color, _tokenize(color)) is False, color


# ── Catalog allowlist as the strong signal ────────────────────────────


def test_catalog_allowlist_required_when_populated():
    """When the catalog has known brands, an unknown capitalized token
    is NOT marked as a brand (even if not in stop list)."""
    oracle._KNOWN_BRANDS_OVERRIDE = {"stanley", "sedeo", "supregear"}
    _reset_known_brands_cache_for_tests()
    # "Acme" is capitalized + not a stop noun, but NOT in the catalog
    # allowlist → must return False.
    src = "Acme Premium Widget"
    assert _detect_brand_token(src, _tokenize(src)) is False


def test_catalog_allowlist_match_returns_true():
    """A token that's both capitalized AND in the catalog → True."""
    oracle._KNOWN_BRANDS_OVERRIDE = {"stanley", "sedeo", "supregear"}
    _reset_known_brands_cache_for_tests()
    src = "Stanley RoamAlert Wrist Strap, 6 in"
    assert _detect_brand_token(src, _tokenize(src)) is True


def test_catalog_allowlist_lowercase_in_source_still_fails():
    """Allowlist match alone isn't enough — also requires capitalized
    in source description."""
    oracle._KNOWN_BRANDS_OVERRIDE = {"stanley"}
    _reset_known_brands_cache_for_tests()
    src = "stanley wrist strap (lowercase typed)"
    assert _detect_brand_token(src, _tokenize(src)) is False


def test_catalog_allowlist_does_not_override_stop_list():
    """If a brand made it into the catalog incorrectly (e.g. someone
    typed 'WHITE' as a manufacturer), the stop list still wins."""
    oracle._KNOWN_BRANDS_OVERRIDE = {"white"}
    _reset_known_brands_cache_for_tests()
    src = "WHITE Bandage Roll 3x10"
    assert _detect_brand_token(src, _tokenize(src)) is False


# ── Empty catalog falls back to heuristic-only ────────────────────────


def test_empty_catalog_falls_back_to_heuristic():
    """When the catalog has no manufacturer data, the detector must
    NOT silently kill the signal — fall back to (capitalized + not
    stop-noun)."""
    oracle._KNOWN_BRANDS_OVERRIDE = set()  # empty, NOT None
    _reset_known_brands_cache_for_tests()
    # Random capitalized brand-looking word, not in stop list.
    src = "Reytecho Custom Adapter"
    assert _detect_brand_token(src, _tokenize(src)) is True


def test_empty_catalog_still_respects_stop_list():
    """Fallback path still honors stop nouns."""
    oracle._KNOWN_BRANDS_OVERRIDE = set()
    _reset_known_brands_cache_for_tests()
    src = "Pneumatic Wheels 44 inch"
    assert _detect_brand_token(src, _tokenize(src)) is False


# ── Real brands from Mike's screenshot still detect correctly ─────────


def test_real_brands_from_screenshot_pc():
    """The 6-item PC f81c4e9b that surfaced this whole thread:
    Stanley / Sedeo / Supregear are real brands; New / Universal /
    'Sport Medical Data' are generic phrases."""
    oracle._KNOWN_BRANDS_OVERRIDE = {"stanley", "sedeo", "supregear",
                                     "roamalert"}
    _reset_known_brands_cache_for_tests()
    cases = [
        ("Stanley RoamAlert Wrist Strap: 6 in Overall Lg, Gray", True),
        ("Sedeo Pro Armrest Pad, 6A Comfort 2 inch Wide", True),
        ("Supregear Quad Cane Tips Heavy Duty Rubber", True),
        # Universal IS in the stop list — generic.
        ("Universal Medical Data Sport Medical Alert Bracelet", False),
        # New IS in the stop list — generic.
        ("New Solutions Sitting Safe Anti-Roll Back Wheel Locks", False),
    ]
    for src, expected in cases:
        actual = _detect_brand_token(src, _tokenize(src))
        assert actual is expected, (
            f"{src!r}: expected {expected}, got {actual}"
        )


# ── Token length / edge cases ─────────────────────────────────────────


def test_two_char_tokens_skipped():
    """Single/double-char tokens can't reliably be brands."""
    oracle._KNOWN_BRANDS_OVERRIDE = {"3m", "ge"}  # even real 2-char brands
    _reset_known_brands_cache_for_tests()
    src = "3M Tape Industrial Grade"
    # 3M is 2 chars after tokenization → skipped; "Tape" is in stop.
    assert _detect_brand_token(src, _tokenize(src)) is False


def test_empty_input_returns_false():
    assert _detect_brand_token("", []) is False
    assert _detect_brand_token("anything", []) is False
    assert _detect_brand_token("", [["stanley"]]) is False


# ── Stop list expansion sanity ────────────────────────────────────────


def test_stop_list_includes_new_offenders():
    """Spot-check the words that surfaced in the prod incident."""
    new_offenders = [
        "shiny", "pneumatic", "handle", "aluminum", "statement",
        "payment", "compliance", "claim", "national", "labor",
        "black", "white", "red", "blue", "self",
    ]
    for word in new_offenders:
        assert word in _BRAND_STOP_NOUNS, f"{word!r} should be in stop list"
