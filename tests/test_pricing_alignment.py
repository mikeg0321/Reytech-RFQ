"""Cross-document pricing alignment gate tests (Mike P0 2026-05-07,
post-quote queue Q7 — 'pricing has to be 100% truth').

Pins the substrate against the exact divergence shape Mike's
2026-05-06 RFQ a5b09b56 incident exposed:

  ✓ 704B filled + signed — $514.72
  ...
  Quote R26Q40 generated: $492.58 total

Both inside the SAME generate-package run. The alignment gate must
detect this kind of mismatch and surface it as a QA blocker.
"""
from __future__ import annotations

import pytest

from src.forms.pricing_alignment import (
    check_alignment,
    compute_canonical_totals,
)


# ─── compute_canonical_totals ─────────────────────────────────────────


def test_canonical_totals_matches_screenshot_18_22_state():
    """The exact 6-item state at 2026-05-06 18:22 (Mike's session
    final regenerate). Subtotal $452.95, tax 8.75% = $39.63, total
    $492.58."""
    rfq = {
        "tax_rate": 0.0875,
        "tax_enabled": True,
        "line_items": [
            {"qty": 10, "price_per_unit": 8.84,
             "description": "Stuff2Color Love Letters"},
            {"qty": 10, "price_per_unit": 12.83,
             "description": "Stuff2Color Heart Hands"},
            {"qty": 10, "price_per_unit": 11.40,
             "description": "Butterfly Eyes"},
            {"qty": 2,  "price_per_unit": 11.08,
             "description": "Blossom Bliss"},
            {"qty": 2,  "price_per_unit": 14.30,
             "description": "Colorful Reflections"},
            {"qty": 1,  "price_per_unit": 71.49,
             "description": "Wits & Wagers"},
        ],
    }
    result = compute_canonical_totals(rfq)
    assert result["subtotal"] == 452.95
    assert result["tax"] == 39.63
    assert result["total"] == 492.58
    assert result["items_priced"] == 6
    assert result["items_total"] == 6


def test_canonical_handles_zero_priced_rows():
    rfq = {
        "tax_rate": 0.0875, "tax_enabled": True,
        "line_items": [
            {"qty": 10, "price_per_unit": 5.00},
            {"qty": 5, "price_per_unit": 0},
        ],
    }
    r = compute_canonical_totals(rfq)
    assert r["subtotal"] == 50.00
    assert r["items_priced"] == 1
    assert r["items_total"] == 2


def test_canonical_tax_disabled():
    """When tax_enabled is False (e.g., DGS purchase order with no tax),
    tax must be 0 regardless of rate."""
    rfq = {
        "tax_rate": 0.0875,
        "tax_enabled": False,
        "line_items": [{"qty": 10, "price_per_unit": 5.00}],
    }
    r = compute_canonical_totals(rfq)
    assert r["subtotal"] == 50.00
    assert r["tax"] == 0.00
    assert r["total"] == 50.00


def test_canonical_normalizes_percent_form_tax_rate():
    """Some legacy paths carry tax_rate as percent (8.75) instead of
    fraction (0.0875). compute_canonical_totals must normalize."""
    rfq = {
        "tax_rate": 8.75,  # percent form
        "tax_enabled": True,
        "line_items": [{"qty": 10, "price_per_unit": 5.00}],
    }
    r = compute_canonical_totals(rfq)
    assert r["tax_rate"] == 0.0875
    assert r["tax"] == 4.38  # 50 × 0.0875


def test_canonical_string_qty_and_price():
    """Form-submitted values arrive as strings; helper coerces."""
    rfq = {
        "tax_rate": 0.0875, "tax_enabled": True,
        "line_items": [{"qty": "10", "price_per_unit": "5.00"}],
    }
    r = compute_canonical_totals(rfq)
    assert r["subtotal"] == 50.00


def test_canonical_empty_rfq():
    r = compute_canonical_totals({})
    assert r["subtotal"] == 0
    assert r["total"] == 0
    assert r["items_total"] == 0


def test_canonical_ignores_non_dict_items():
    """Defensive: malformed item rows (None, strings) skip cleanly."""
    rfq = {
        "tax_rate": 0.0875, "tax_enabled": True,
        "line_items": [
            None,
            "garbage",
            {"qty": 10, "price_per_unit": 5.00},
        ],
    }
    r = compute_canonical_totals(rfq)
    assert r["subtotal"] == 50.00
    assert r["items_total"] == 1


def test_canonical_per_row_extension_rounding():
    """Subtotal must equal sum-of-rounded-extensions, not
    sum-then-round. Otherwise a 40-item quote can drift by cents."""
    rfq = {
        "tax_rate": 0.0875, "tax_enabled": True,
        "line_items": [
            {"qty": 3, "price_per_unit": 1.333},  # → 4.00 (rounded)
            {"qty": 3, "price_per_unit": 1.333},  # → 4.00 (rounded)
            {"qty": 3, "price_per_unit": 1.333},  # → 4.00 (rounded)
        ],
    }
    r = compute_canonical_totals(rfq)
    assert r["subtotal"] == 12.00  # 3 × $4.00 rounded extensions
    # Verify per-row entries are also rounded
    for ext in r["line_extensions"]:
        assert ext["extension"] == 4.00


# ─── check_alignment without PDF files ────────────────────────────────


def test_check_alignment_no_files_returns_canonical_only():
    """When no PDFs exist yet (called pre-fill), canonical computation
    runs and per-row invariant checks fire, but no PDF blockers."""
    rfq = {
        "tax_rate": 0.0875, "tax_enabled": True,
        "line_items": [{"qty": 1, "price_per_unit": 100.00}],
    }
    result = check_alignment(rfq, [])
    assert result["ok"]
    assert result["canonical"]["total"] == 108.75
    assert result["blockers"] == []


def test_check_alignment_per_row_invariant_passes():
    """Healthy items pass the per-row math invariant."""
    rfq = {
        "tax_rate": 0.0875, "tax_enabled": True,
        "line_items": [{"qty": 10, "price_per_unit": 8.84}],
    }
    result = check_alignment(rfq, [])
    assert not any(b["field"] == "row_invariant" for b in result["blockers"])
