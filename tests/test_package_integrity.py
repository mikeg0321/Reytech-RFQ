"""The QA gate that WOULD HAVE CAUGHT the CCHCS 10842771 stress package.

That package shipped with CalRecycle 74 duplicated (pages 2-5 AND 16-19) and the
seller's permit duplicated (pages 15 AND 26) because the assembler emitted standalone
copies of forms the bid package already contained, and NOTHING in QA looked at the
assembled package for repeated forms. It also shipped with a BLANK 703A bidder-info
block. These tests pin both failure modes against the canonical
`src/forms/package_integrity.py` detector so neither can ship again — for ANY form
type or PC, since the detector judges a finished PDF, not a generation path.

Build-this-bug-then-prove-the-gate-catches-it (CLAUDE.md §4).
"""
from __future__ import annotations

import pytest

from src.forms.package_integrity import (
    detect_duplicate_forms,
    find_blank_bidder_info,
    check_package,
)

# A realistic CalRecycle-74-like form page (long enough to clear the blank-page floor).
_CALRECYCLE = (
    "STATE OF CALIFORNIA To be completed by the State agency "
    "Department of Resources Recycling and Recovery CalRecycle 74 "
    "Recycled Content Certification SABRC reporting requirements line items follow"
)
_SELLERS_PERMIT = (
    "DISPLAY THIS PERMIT CONSPICUOUSLY AT THE PLACE OF BUSINESS FOR WHICH IT IS ISSUED "
    "CALIFORNIA DEPARTMENT OF TAX AND FEE ADMINISTRATION SELLERS PERMIT REYTECH INC"
)
_703A = (
    "AMS 703A REQUEST FOR QUOTATION NON-IT GOODS BIDDER INFORMATION "
    "Business Name Reytech Inc Address 30 Carnoustie Way Trabuco Canyon CA "
    "Federal Employer Identification Number FEIN Retailer CA Sellers Permit Number"
)
_QUOTE = "REYTECH INC QUOTE Solicitation 10842771 line item pricing subtotal tax total"


def _make_pdf(tmp_path, name, page_texts):
    """Compose a multi-page PDF, one page per text. Skips if reportlab missing."""
    pytest.importorskip("pdfplumber")
    try:
        from reportlab.pdfgen import canvas
    except ImportError:
        pytest.skip("reportlab not available")
    path = str(tmp_path / name)
    c = canvas.Canvas(path)
    for txt in page_texts:
        # wrap into lines so pdfplumber extracts substantial text per page
        y = 760
        for chunk in [txt[i:i + 90] for i in range(0, len(txt), 90)]:
            c.drawString(54, y, chunk)
            y -= 16
        c.showPage()
    c.save()
    return path


def test_detects_duplicated_form_block(tmp_path):
    """The exact 10842771 shape: a form (CalRecycle) included twice + seller's
    permit twice. The gate must flag BOTH."""
    pkg = _make_pdf(tmp_path, "dup.pdf", [
        _703A, _CALRECYCLE, _QUOTE, _SELLERS_PERMIT,  # first occurrences
        _CALRECYCLE,                                   # CalRecycle AGAIN (bid-pkg dup)
        _SELLERS_PERMIT,                               # seller's permit AGAIN
    ])
    dups = detect_duplicate_forms(pkg)
    sigs = {tuple(d["pages"]) for d in dups}
    assert (2, 5) in sigs, f"CalRecycle duplication not detected: {dups}"
    assert (4, 6) in sigs, f"Seller's permit duplication not detected: {dups}"
    res = check_package(pkg, company_name="Reytech Inc")
    assert res["ok"] is False
    assert len(res["blockers"]) >= 2


def test_clean_package_passes(tmp_path):
    """One copy of each form + bidder info present → clean."""
    pkg = _make_pdf(tmp_path, "clean.pdf", [_703A, _CALRECYCLE, _SELLERS_PERMIT, _QUOTE])
    assert detect_duplicate_forms(pkg) == []
    res = check_package(pkg, company_name="Reytech Inc")
    assert res["ok"] is True, res["blockers"]


def test_blank_separator_pages_do_not_false_flag(tmp_path):
    """Two near-empty separator pages must NOT count as a duplicated form
    (the 10842771 package had blank pages 10 & 14)."""
    pkg = _make_pdf(tmp_path, "blanks.pdf", [_703A, " ", _QUOTE, " "])
    assert detect_duplicate_forms(pkg) == []


def test_blank_bidder_info_blocks(tmp_path):
    """The other 10842771 bug: bidder/company identity never landed. A package
    whose forms never mention the company name is blocked."""
    pkg = _make_pdf(tmp_path, "nobidder.pdf", [
        _CALRECYCLE, _SELLERS_PERMIT.replace("REYTECH INC", "")  # no company name anywhere
    ])
    info = find_blank_bidder_info(pkg, "Reytech Inc")
    assert info["present"] is False
    res = check_package(pkg, company_name="Reytech Inc")
    assert res["ok"] is False
    assert any("Bidder info missing" in b for b in res["blockers"])


def test_bidder_info_present_space_tolerant(tmp_path):
    """Gap-rendered 'R E Y T E C H' must still count as present (no false block)."""
    pkg = _make_pdf(tmp_path, "gapname.pdf", [_703A.replace("Reytech Inc", "R e y t e c h   I n c")])
    assert find_blank_bidder_info(pkg, "Reytech Inc")["present"] is True
