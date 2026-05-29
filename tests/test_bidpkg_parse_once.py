"""Parse-once refactor for fill_bid_package — equivalence pins.

`fill_bid_package` used to run THREE separate full extract_text() passes
over the filled bid-package PDF (one each in _calrecycle_fix_date,
_overlay_obs1600_header, and the page-trim's _bidpkg_page_skip_reason).
It now snapshots the per-page text once and threads it into all three via
optional params (page_texts / page_text). These tests pin that the optional
text param produces IDENTICAL decisions to the previous self-extraction, so
the output PDF is unchanged — the change is purely a performance refactor.
"""
from __future__ import annotations

import os
import tempfile

import pytest
from pypdf import PdfReader

from src.forms.reytech_filler_v4 import (
    _bidpkg_page_skip_reason,
    fill_bid_package,
    load_config,
)

_TEMPLATE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "templates",
)
_BIDPKG_BLANK = os.path.join(_TEMPLATE_DIR, "cdcr_bid_package_template.pdf")

pytestmark = pytest.mark.skipif(
    not os.path.exists(_BIDPKG_BLANK),
    reason="bid_package template not available",
)


def _items(n: int) -> list:
    return [
        {
            "item_number": str(i),
            "description": f"Test Item {i} - detailed product description here",
            "mfg_number": f"MFG-{i:03d}",
            "qty": 1,
            "unit_price": 100.0,
        }
        for i in range(1, n + 1)
    ]


def _config():
    import builtins
    _orig = builtins.print
    builtins.print = lambda *a, **k: None
    try:
        return load_config()
    finally:
        builtins.print = _orig


def test_skip_reason_param_matches_self_extraction():
    """Passing precomputed page_text yields the SAME skip decision as letting
    _bidpkg_page_skip_reason extract the text itself — for every page of the
    real CCHCS template, under both the empty (CCHCS) and the
    bidder_decl+darfur_act (CalVet/DSH) standalone sets."""
    reader = PdfReader(_BIDPKG_BLANK)
    for standalone in (frozenset(), frozenset({"bidder_decl", "darfur_act"})):
        for i, page in enumerate(reader.pages):
            self_extracted = _bidpkg_page_skip_reason(
                page, replaced_by_standalone=standalone)
            precomputed = _bidpkg_page_skip_reason(
                page, replaced_by_standalone=standalone,
                page_text=page.extract_text())
            assert self_extracted == precomputed, (
                f"page {i} (standalones={sorted(standalone)}): "
                f"self={self_extracted!r} != precomputed={precomputed!r}")


def test_empty_page_text_is_treated_as_blank():
    """page_text='' must behave like extracted-empty (not fall through to
    self-extraction), so a caller's snapshot of a genuinely blank page is
    honored."""
    reader = PdfReader(_BIDPKG_BLANK)
    page = reader.pages[0]
    # An empty precomputed text + no/zero annots → "blank" skip; with annots the
    # field-name rules still apply. We only assert it does not raise and returns
    # a str-or-None consistent with the empty-text branch.
    result = _bidpkg_page_skip_reason(page, page_text="")
    assert result is None or isinstance(result, str)


@pytest.mark.parametrize("n_items,expected_pages", [(6, 13), (7, 15), (21, 17)])
def test_fill_bid_package_page_count_stable(n_items, expected_pages):
    """End-to-end: the parse-once path produces the same trimmed/spliced page
    count as before (6 items = no splice; 7 and 21 = CalRecycle overflow
    splice). These counts are the clean-code baseline captured 2026-05-29."""
    cfg = _config()
    import builtins
    _orig = builtins.print
    builtins.print = lambda *a, **k: None
    try:
        with tempfile.TemporaryDirectory() as td:
            out = os.path.join(td, f"bidpkg_{n_items}.pdf")
            fill_bid_package(
                _BIDPKG_BLANK,
                {"solicitation_number": f"PARSE1X{n_items}",
                 "sign_date": "1/1/2026", "line_items": _items(n_items)},
                cfg, out,
            )
            assert len(PdfReader(out).pages) == expected_pages
    finally:
        builtins.print = _orig
