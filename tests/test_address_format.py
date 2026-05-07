"""Canonical address formatter — pins the substrate against the real
shapes seen on prod 2026-05-06 (Mike P0 RFQ a5b09b56) and prevents
regression on the canonical output.

Every customer-facing PDF (quote, 703B, 704B, bid package) and PC
view goes through this helper. Failures here mean ugly addresses on
buyer-facing artifacts.
"""
from __future__ import annotations

import pytest

from src.core.address_format import (
    format_address_canonical,
    format_for_pdf,
    parse_address_blob,
)


# ─── Pattern A — the bug pattern from RFQ a5b09b56 ────────────────────


def test_inst_dash_street_with_split_city_state_zip():
    """The exact shape that overflowed Mike's quote PDF margin."""
    raw = (
        "CIW - California Institution for Women - 16756 Chino Corona Road\n"
        "Corona\nCA\n92880\nUnited States"
    )
    result = parse_address_blob(raw)
    assert result["name"] == "CIW - California Institution for Women"
    assert result["lines"] == [
        "16756 Chino Corona Road",
        "Corona, CA 92880",
    ]


def test_inst_dash_street_drops_country():
    """Implied country (United States) is always dropped."""
    raw = "Foo - 123 Bar St\nCity\nNV\n88888\nUSA"
    result = parse_address_blob(raw)
    assert "United States" not in " ".join(result["lines"])
    assert "USA" not in " ".join(result["lines"])


def test_inst_dash_street_preserves_foreign_country():
    """Non-US country lines are real data — keep them."""
    raw = "Foo - 123 Bar St\nCity\nON\nM5V 3A8\nCanada"
    result = parse_address_blob(raw)
    assert "Canada" in " ".join(result["lines"])


# ─── Pattern B — already canonical multi-line ─────────────────────────


def test_canonical_multiline_passes_through():
    """Inputs already in the right shape don't get mangled."""
    raw = "ACME Corp\n123 Main St\nAnytown, NV 89000"
    result = parse_address_blob(raw)
    assert result["name"] == "ACME Corp"
    assert result["lines"] == ["123 Main St", "Anytown, NV 89000"]


def test_inst_name_with_legit_internal_dashes():
    """An institution name with `- ` inside but no street-number after
    must NOT be split. `CIW - California Institution for Women` is the
    name on its own when followed by other lines."""
    raw = "CIW - California Institution for Women\n123 Main St\nAnytown, CA 90001"
    result = parse_address_blob(raw)
    assert result["name"] == "CIW - California Institution for Women"
    assert result["lines"] == ["123 Main St", "Anytown, CA 90001"]


# ─── Pattern C — single-line CSV ──────────────────────────────────────


def test_single_line_csv_with_inst_first():
    raw = "ACME Corp, 123 Main St, Anytown, NV, 89000"
    result = parse_address_blob(raw)
    assert result["name"] == "ACME Corp"
    assert result["lines"] == ["123 Main St", "Anytown, NV 89000"]


def test_single_line_csv_with_street_first():
    """CSV starting with a street number — no institution name."""
    raw = "123 Main St, Anytown, NV, 89000"
    result = parse_address_blob(raw)
    assert result["name"] == ""
    assert result["lines"] == ["123 Main St", "Anytown, NV 89000"]


# ─── Edge cases ───────────────────────────────────────────────────────


def test_empty_input_returns_empty_canonical():
    for empty in ("", None, "   "):
        result = parse_address_blob(empty)
        assert result == {"name": "", "lines": []}


def test_just_institution_name():
    result = parse_address_blob("Standalone Buyer Org")
    assert result["name"] == "Standalone Buyer Org"
    assert result["lines"] == []


def test_inst_with_dashed_street_only_one_line():
    """Single-line `INST - STREET` (no city/state/zip)."""
    raw = "Foo - 123 Bar St"
    result = parse_address_blob(raw)
    assert result["name"] == "Foo"
    assert result["lines"] == ["123 Bar St"]


def test_pre_folded_city_state_zip_passes_through():
    """Already `City, ST ZIP` — don't re-fold."""
    raw = "Foo Org\n123 Main St\nCorona, CA 92880"
    result = parse_address_blob(raw)
    assert result["lines"][-1] == "Corona, CA 92880"


def test_split_zip_only_no_state_falls_back_safely():
    """Bad input where state is missing — preserves what's there
    rather than crashing."""
    raw = "Foo Org\n123 Main St\nCorona\n92880"
    result = parse_address_blob(raw)
    # Doesn't crash. Best-effort output.
    assert "92880" in " ".join(result["lines"])
    assert "Corona" in " ".join(result["lines"])


# ─── format_address_canonical (parts → canonical) ─────────────────────


def test_format_canonical_full_parts():
    result = format_address_canonical(
        institution="ACME Corp",
        street="123 Main St",
        city="Anytown",
        state="CA",
        zip_code="90001",
    )
    assert result == {
        "name": "ACME Corp",
        "lines": ["123 Main St", "Anytown, CA 90001"],
    }


def test_format_canonical_drops_implied_country():
    result = format_address_canonical(
        institution="Foo", street="1 Bar Ln", city="X", state="CA",
        zip_code="90001", country="United States",
    )
    assert "United States" not in " ".join(result["lines"])


def test_format_canonical_keeps_foreign_country():
    result = format_address_canonical(
        institution="Foo", street="1 Bar Ln", city="X", state="ON",
        zip_code="M5V 3A8", country="Canada",
    )
    assert "Canada" in " ".join(result["lines"])


def test_format_canonical_skips_empty_parts():
    result = format_address_canonical(institution="Just Name")
    assert result == {"name": "Just Name", "lines": []}


# ─── format_for_pdf (canonical → tuple) ───────────────────────────────


def test_format_for_pdf_returns_tuple():
    parsed = parse_address_blob("Foo - 123 Bar\nCity, CA 90001")
    name, lines = format_for_pdf(parsed)
    assert name == "Foo"
    assert lines == ["123 Bar", "City, CA 90001"]


def test_format_for_pdf_handles_empty():
    name, lines = format_for_pdf({"name": "", "lines": []})
    assert name == ""
    assert lines == []
