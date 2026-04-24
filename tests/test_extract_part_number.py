"""Regression: `_extract_part_number` covers the MFG# shapes Reytech
encounters in the wild + doesn't false-positive on parsing artifacts
or Amazon ASINs.

History:
- The base regex chain handled labeled patterns (`MFG W12919`),
  trailing-dash patterns (`- W14100`, `- 16753`), and pure
  alphanumeric (`FN4368`).
- 2026-04-24 incident: Mike's f81c4e9b PC items had MFG#s in
  `WL085P` shape — letters + digits + trailing letter, no label, no
  dash. The chain missed them, leaving the MFG# partition unable to
  fire even after PR #497's backfill ran.
- This file pins both the new shape and the existing shapes so we
  don't regress one to fix the other.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.forms.price_check import _extract_part_number


# ── New: letters + digits + trailing letter (incident 2026-04-24) ─────


@pytest.mark.parametrize("desc,expected", [
    ("Stanley RoamAlert Wrist Strap WL085P, 6 in", "WL085P"),
    ("New Solutions Anti-Roll Wheel Locks AB123C", "AB123C"),
    ("Sedeo Pro Armrest Pad FN4368X right side", "FN4368X"),
    # Padding with junk before/after still extracts cleanly.
    ("(WL085P)", "WL085P"),
    ("see: AB123C - replacement", "AB123C"),
])
def test_letters_digits_trailing_letter_extracted(desc, expected):
    assert _extract_part_number(desc) == expected


# ── False-positive guard: parsing-artifact descriptions ───────────────


@pytest.mark.parametrize("desc", [
    "PNEUMATIC WHEELS LARGE",
    "STATEMENT OF COMPLIANCE - The above signed",
    "SHINY REPLACEMENT PAD",
    "ALUMINUM PLATFORM TRUCK 24x48",
    "BLACK FLAT SHELF UTILITY",
    # Pure prose with no part-number-shaped tokens.
    "The buyer requested delivery before close of business",
])
def test_no_extraction_from_pure_prose(desc):
    assert _extract_part_number(desc) == ""


# ── Amazon ASIN must not be mistaken for an MFG# ──────────────────────


def test_amazon_asin_not_extracted_as_mfg_number():
    """ASIN format `B0` + 8 mixed letters/digits should NOT match the
    new letters+digits+trailing-letter pattern. The pattern requires
    letters-then-digits-then-one-letter — ASINs interleave."""
    # Real ASIN from the 2026-04-23 incident.
    assert _extract_part_number("Amazon ASIN B0CX1BD86P inline") == ""
    # Another common shape.
    assert _extract_part_number("see B077JQYDTN on amazon") == ""


# ── Existing patterns: regression check (must not break) ──────────────


@pytest.mark.parametrize("desc,expected", [
    # Trailing dash + alphanumeric
    ("JUMBO JACKS - W14100", "W14100"),
    ("EASY PACK - 16753", "16753"),
    # Labeled
    ("MFG W12919 Item", "W12919"),
    ("Item: WC-2280 ALUMINUM", "WC-2280"),
    ("SKU ABC-1234", "ABC-1234"),
    # Pure alphanumeric (existing 2-4 letter + 3-8 digit pattern)
    ("FN4368 alone", "FN4368"),
    # Single letter + 4-6 digits (existing S&S Worldwide pattern)
    ("- W12919", "W12919"),
])
def test_existing_patterns_still_work(desc, expected):
    assert _extract_part_number(desc) == expected


# ── Length / character validation backstop ────────────────────────────


def test_too_short_codes_rejected():
    """3-char codes pass length check but the validation in
    `_extract_part_number` requires letter+digit OR dash+digit+>=5
    chars OR digit-only-5+. 3-char letter+digit makes it through if
    the regex happens to match."""
    # Below 3 chars: skipped.
    assert _extract_part_number("X1") == ""


def test_pure_short_digits_rejected():
    """Quote rules require >=5 digits for pure-digit MFG#s."""
    assert _extract_part_number("see 123 next item") == ""
    assert _extract_part_number("see 1234 next item") == ""


def test_empty_input():
    assert _extract_part_number("") == ""
    assert _extract_part_number("   ") == ""
    assert _extract_part_number(None) == ""
