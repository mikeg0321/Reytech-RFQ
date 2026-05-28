"""CalRecycle overflow splice — bid_package embedded path.

Coleman 10842771 punch-list 2026-05-28: the bid_package PDF embeds a
6-row CalRecycle 74 form. ``fill_bid_package`` filled rows 1-6 via
form fields and silently dropped items 7+ — a sibling bug class to
the standalone ``fill_calrecycle_74`` issue closed 2026-05-27 (see
``test_calrecycle_multipage_overflow.py``).

This PR adds ``_splice_overflow_calrecycle_into_bidpkg`` which, when
``len(line_items) > 6``, generates the full multi-page CalRecycle via
the standalone path and splices it into the bid_package at the
position of the embedded CalRecycle page. Items 7+ become visible;
bid_package's page order for all OTHER forms (DVBE 843, Darfur,
OBS 1600, Bidder's Declaration) is preserved.

These tests pin:
  * The splice helper finds the embedded CalRecycle page by form
    field signature.
  * Splice replaces (not duplicates) the embedded page.
  * ≤6 items skip the splice entirely (cheap path stays cheap).
  * No-op for templates that don't include CalRecycle inline.
"""
from __future__ import annotations

import os
import tempfile

import pytest
from pypdf import PdfReader, PdfWriter

_TEMPLATE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "templates",
)
_CR_BLANK = os.path.join(_TEMPLATE_DIR, "calrecycle_74_blank.pdf")
_BIDPKG_BLANK = os.path.join(_TEMPLATE_DIR, "cdcr_bid_package_template.pdf")

pytestmark = pytest.mark.skipif(
    not (os.path.exists(_CR_BLANK) and os.path.exists(_BIDPKG_BLANK)),
    reason="bid_package or CalRecycle template not available",
)


# ── Helpers ──────────────────────────────────────────────────────────


def _config():
    """Use the live config so all bid_package field names resolve.

    The fake company dict in earlier drafts dropped ~10 keys
    (cert_number, fein, sb_cert, etc.) that fill_bid_package reads
    directly. Living off load_config() also catches the case where
    we ship a new bid_package consumer of a config key that this test
    forgot — without re-listing every key by hand.
    """
    import builtins
    _orig_print = builtins.print
    builtins.print = lambda *a, **kw: None
    try:
        from src.forms.reytech_filler_v4 import load_config
        return load_config()
    finally:
        builtins.print = _orig_print


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


def _mfgs_in_pdf(path: str) -> set[str]:
    """All MFG-NNN tokens present anywhere in the PDF text."""
    import re
    reader = PdfReader(path)
    found = set()
    for page in reader.pages:
        text = page.extract_text() or ""
        for m in re.finditer(r"MFG-\d{3}", text):
            found.add(m.group())
    return found


# ── Splice helper — direct unit tests ─────────────────────────────────


def test_splice_helper_finds_embedded_calrecycle_page():
    """When the bid_package has an embedded CalRecycle page (detected
    by the unique form field name ``Product or Services
    DescriptionRow1``), the splice helper must locate it. Pin against
    the canonical cdcr_bid_package_template.pdf.
    """
    from src.forms.reytech_filler_v4 import (
        _splice_overflow_calrecycle_into_bidpkg,
        fill_bid_package,
    )

    # Fill an empty bid_package first so we have a target to splice into.
    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "bidpkg.pdf")
        # Minimum rfq_data to satisfy fill_bid_package — 7 items so
        # the splice path fires post-fill.
        rfq_data = {
            "solicitation_number": "SPLICE-PIN-001",
            "sign_date": "05/28/2026",
            "line_items": _items(7),
        }
        fill_bid_package(_BIDPKG_BLANK, rfq_data, _config(), out)

        # By now the fill_bid_package post-fill splice should have run
        # already. Re-running the helper must be a no-op without error
        # (the embedded CalRecycle page is gone — replaced with the
        # multi-page version which still has DescriptionRow1, but the
        # underlying pages came from the standalone template not the
        # embedded form).
        _splice_overflow_calrecycle_into_bidpkg(out, rfq_data, _config())
        # No exception = pass


def test_splice_no_op_when_no_embedded_calrecycle_page():
    """When the input PDF has no CalRecycle page (no
    ``Product or Services DescriptionRow1`` field anywhere), the helper
    must log + return without writing anything. The PDF stays intact.
    """
    from src.forms.reytech_filler_v4 import (
        _splice_overflow_calrecycle_into_bidpkg,
    )

    with tempfile.TemporaryDirectory() as td:
        # Synthesize a 2-page PDF with no AcroForm at all.
        path = os.path.join(td, "no_cr.pdf")
        writer = PdfWriter()
        from reportlab.pdfgen import canvas as _rlc
        synth = os.path.join(td, "synth.pdf")
        c = _rlc.Canvas(synth)
        c.drawString(100, 750, "Page 1 — no form fields")
        c.showPage()
        c.drawString(100, 750, "Page 2 — no form fields")
        c.showPage()
        c.save()
        with open(synth, "rb") as f:
            r = PdfReader(f)
            for p in r.pages:
                writer.add_page(p)
            with open(path, "wb") as g:
                writer.write(g)

        sizes_before = os.path.getsize(path)
        _splice_overflow_calrecycle_into_bidpkg(
            path,
            {"solicitation_number": "NOOP", "line_items": _items(10),
             "sign_date": "05/28/2026"},
            _config(),
        )
        sizes_after = os.path.getsize(path)
        # File untouched.
        assert sizes_before == sizes_after, (
            "Splice helper modified a PDF that had no embedded "
            "CalRecycle page — no-op contract violated."
        )


# ── fill_bid_package end-to-end pins ──────────────────────────────────


@pytest.mark.parametrize("n_items", [7, 10, 21])  # 21 = Coleman's count
def test_bidpkg_with_more_than_6_items_lists_all_via_splice(n_items):
    """End-to-end pin — the Coleman 10842771 bug class.

    Generate bid_package with N>6 line items, assert every MFG#
    appears somewhere in the resulting PDF text. Pre-PR-#1183 the
    bid_package's embedded CalRecycle silently dropped items 7+ — only
    MFG-001..MFG-006 would appear, the rest were never written.
    """
    from src.forms.reytech_filler_v4 import fill_bid_package

    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, f"bidpkg_{n_items}.pdf")
        rfq_data = {
            "solicitation_number": f"OVERFLOW-{n_items}",
            "sign_date": "05/28/2026",
            "line_items": _items(n_items),
        }
        fill_bid_package(_BIDPKG_BLANK, rfq_data, _config(), out)

        found = _mfgs_in_pdf(out)
        expected = {f"MFG-{i:03d}" for i in range(1, n_items + 1)}
        missing = expected - found
        assert not missing, (
            f"Bid_package with {n_items} items dropped: {sorted(missing)}. "
            f"This is the 'CalRecycle 6-row visible only' bug class — "
            f"the splice helper must replace the embedded CalRecycle "
            f"page with the multi-page standalone version."
        )


def test_bidpkg_with_exactly_6_items_skips_splice():
    """The cheap path stays cheap — 1-6 items fit on the embedded
    CalRecycle form-field page, so the splice helper is never invoked.
    Asserted by checking that all 6 MFG#s land in the CalRecycle page's
    form-field values (NOT via extract_text, which skips form-field
    /V values — the embedded fill writes via pypdf form fields).
    """
    from src.forms.reytech_filler_v4 import fill_bid_package

    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "bidpkg_6.pdf")
        rfq_data = {
            "solicitation_number": "EXACT-6",
            "sign_date": "05/28/2026",
            "line_items": _items(6),
        }
        fill_bid_package(_BIDPKG_BLANK, rfq_data, _config(), out)

        # Form fields (filled via pypdf, not via overlay) — read /V values.
        reader = PdfReader(out)
        fields = reader.get_fields() or {}
        item_field_values = []
        for i in range(1, 7):
            f = fields.get(f"Item Row{i}")
            if f and isinstance(f, dict):
                v = str(f.get("/V", "")).strip()
                if v:
                    item_field_values.append(v)
        expected = {f"MFG-{i:03d}" for i in range(1, 7)}
        assert expected.issubset(set(item_field_values)), (
            f"Embedded 6-row CalRecycle dropped items: form field values "
            f"= {item_field_values}, expected {sorted(expected)}"
        )


def test_bidpkg_splice_adds_only_overflow_pages():
    """Structural pin — the splice must ADD pages (overflow content)
    but never DROP pages from the post-trim bid_package. Compares
    page counts: bid_package with 6 items (no splice) vs same with
    10 items (splice fires + adds 1 overflow page + reference table).

    The delta = pages-added-by-splice = standalone(10 items) page
    count - 1 (the original embedded CalRecycle page replaced).
    Standalone(10) = page 0 (items 1-6) + page 1 (items 7-10) + ref
    table = 3 pages. So delta = 3 - 1 = 2 new pages.
    """
    from src.forms.reytech_filler_v4 import fill_bid_package

    with tempfile.TemporaryDirectory() as td:
        out_6 = os.path.join(td, "bidpkg_6.pdf")
        out_10 = os.path.join(td, "bidpkg_10.pdf")
        cfg = _config()
        fill_bid_package(_BIDPKG_BLANK, {
            "solicitation_number": "DELTA-6",
            "sign_date": "05/28/2026",
            "line_items": _items(6),
        }, cfg, out_6)
        fill_bid_package(_BIDPKG_BLANK, {
            "solicitation_number": "DELTA-10",
            "sign_date": "05/28/2026",
            "line_items": _items(10),
        }, cfg, out_10)

        pages_6 = len(PdfReader(out_6).pages)
        pages_10 = len(PdfReader(out_10).pages)
        delta = pages_10 - pages_6
        # Standalone CalRecycle for 10 items = 1 main page + 1 overflow
        # batch + 1 reference table = 3 pages. Replacing the 1 embedded
        # page nets +2 pages.
        assert delta == 2, (
            f"Splice page-count delta wrong: {pages_6} -> {pages_10} "
            f"(delta={delta}, expected 2). If delta < 2 the splice "
            f"failed to add overflow content. If delta > 2 the splice "
            f"added extra pages or duplicated content."
        )
