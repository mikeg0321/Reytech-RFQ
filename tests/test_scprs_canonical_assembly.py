"""Unit tests for SCPRS canonical PO assembly.

SCPRS strips the agency prefix and stores it in a separate column
(Business Unit). To compare against `orders.po_number` (which carries
the prefix when the parse path is correct), we re-attach via per-BU
rules. Mike confirmed the BU values 2026-04-28:
  8955 → CalVet  → '8955-' + po_doc
  4440 → DSH     → '4440-' + po_doc
  5225 → CCHCS   → po_doc as-is (4500 is already in the po_doc)
"""
from __future__ import annotations

import pytest


@pytest.mark.parametrize("bu,po,expected", [
    # CalVet — prefix on
    ("8955",      "0000076737",   "8955-0000076737"),
    ("'8955",     "'0000076737",  "8955-0000076737"),  # apostrophe-stripping
    ("8955",      "0000071826",   "8955-0000071826"),

    # DSH — prefix on
    ("4440",      "0000063878",   "4440-0000063878"),
    ("'4440",     "'0000050349",  "4440-0000050349"),

    # CCHCS — po_doc as-is (already has 4500)
    ("5225",      "4500752793",   "4500752793"),
    ("'5225",     "'4500737702",  "4500737702"),
    ("5225",      "4500745796",   "4500745796"),

    # Empty/garbage BU — fall through to po_doc as-is
    ("",          "9999999999",   "9999999999"),
    ("XXXX",      "12345678",     "12345678"),
    (None,        "0000050000",   "0000050000"),

    # Empty po_doc — return empty (nothing to canonicalize)
    ("8955",      "",             ""),
    ("",          "",             ""),
    (None,        None,           ""),

    # Whitespace tolerated
    ("  8955  ",  "  0000076737  ", "8955-0000076737"),
])
def test_scprs_canonical_po(bu, po, expected):
    from src.api.modules.routes_health import _scprs_canonical_po
    assert _scprs_canonical_po(bu, po) == expected


def test_scprs_canonical_po_calvet_dash_format():
    """The dash separates CalVet's authority code from the tail. This
    matches what's printed on the actual STD-65 PO PDF and what the
    parse path must produce after PR #636's regex fix."""
    from src.api.modules.routes_health import _scprs_canonical_po
    out = _scprs_canonical_po("8955", "0000076737")
    assert out.startswith("8955-")
    assert "-" in out
    assert out.split("-")[1] == "0000076737"


def test_scprs_canonical_po_cchcs_no_dash():
    """CCHCS's PO numbers don't have a dash — they're contiguous like
    `4500752793`. The 4500 is the master purchasing authority code,
    NOT a prefix that needs assembling. So BU=5225 maps to po_doc
    as-is, with no dash insertion."""
    from src.api.modules.routes_health import _scprs_canonical_po
    out = _scprs_canonical_po("5225", "4500752793")
    assert out == "4500752793"
    assert "-" not in out


def test_scprs_canonical_po_cchcs_does_not_double_prefix():
    """Defensive: even if SCPRS ever emits BU=4500 for CCHCS (unlikely
    but possible), we should NOT prepend another '4500-' — the po_doc
    already starts with 4500. Map by BU code 5225 specifically; for
    any unrecognized BU return po_doc as-is."""
    from src.api.modules.routes_health import _scprs_canonical_po
    # BU 4500 is unrecognized → falls through to po_doc as-is
    out = _scprs_canonical_po("4500", "4500752793")
    assert out == "4500752793"
    assert not out.startswith("4500-4500")
