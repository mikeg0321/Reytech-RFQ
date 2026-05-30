"""AMS 708 (GenAI Disclosure) fill correctness.

fill_genai_708 previously wrote field names that do not exist on the real
AMS 708 (Rev. 03/2025) form ("Company Name", "No", "NoGenAI", ...), so it
produced a BLANK 708 wherever it ran. The real AcroForm fields are
708_Text1..16 + 708_Check Box1 (Yes) / 708_Check Box2 (No).

Reytech never uses GenAI, so the correct output checks "No" (708_Check Box2)
and fills the 15 detail cells "N/A". This test extracts the real 708 pages
from the repo's CDCR bid-package template, fills them, and reads the values
back — the checkbox + N/A cells are the part that has regressed before.
"""
import os
from pathlib import Path

import pytest

TMPL = Path("data/templates/cdcr_bid_package_template.pdf")
pytestmark = pytest.mark.skipif(not TMPL.exists(), reason="bid-package template absent")


def _extract_708(dst):
    from pypdf import PdfReader, PdfWriter
    r = PdfReader(str(TMPL))
    w = PdfWriter()
    for i in (9, 10):          # GenAI pages (0-based) carrying the 708_* widgets
        w.add_page(r.pages[i])
    with open(dst, "wb") as f:
        w.write(f)
    return dst


CONFIG = {"company": {
    "name": "Reytech Inc.", "phone": "949-229-1575",
    "address": "30 Carnoustie Way", "city": "Trabuco Canyon",
    "state": "CA", "zip": "92679", "owner": "Michael Guadan",
    "title": "Owner", "fein": "47-4588061", "cert_number": "2002605",
}}


def test_genai_708_fills_real_fields(tmp_path):
    os.environ.setdefault("DASH_USER", "x"); os.environ.setdefault("DASH_PASS", "x")
    from pypdf import PdfReader
    from src.forms.reytech_filler_v4 import fill_genai_708

    blank = _extract_708(str(tmp_path / "708_blank.pdf"))
    out = str(tmp_path / "708_filled.pdf")
    fill_genai_708(blank, {"solicitation_number": "10843276",
                           "sign_date": "05/30/2026"}, CONFIG, out)

    flds = PdfReader(out).get_fields() or {}

    def v(k):
        f = flds.get(k)
        return None if f is None else f.get("/V")

    # Header
    assert v("708_Text1") == "10843276"
    assert "Reytech" in str(v("708_Text3"))
    assert v("708_Text4") == "949-229-1575"
    assert v("708_Text7") == "CA"
    # THE checkbox that regressed: "No GenAI" = Check Box2 set, Yes box off
    assert str(v("708_Check Box2")) == "/Yes", "No-GenAI box (708_Check Box2) not checked"
    assert v("708_Check Box1") in (None, "/Off"), "Yes box must NOT be checked"
    # Detail cells N/A
    assert v("708_Text11") == "N/A"
    assert v("708_Text12.0") == "N/A"
    assert v("708_Text14") == "N/A"
    assert v("708_Text16") == "05/30/2026"


def test_genai_708_uses_real_field_names_not_guesses():
    """Anti-regression: the filler must reference the real 708_* fields, never
    the old non-existent guesses that produced a blank form."""
    src = Path("src/forms/reytech_filler_v4.py").read_text(encoding="utf-8")
    fn = src[src.index("def fill_genai_708"):src.index("def fill_std205")]
    assert '"708_Check Box2": "/Yes"' in fn
    assert '"708_Text1"' in fn and '"708_Text3"' in fn
    # the old broken guesses must be gone
    assert '"NoGenAI' not in fn
    assert '"Company Name"' not in fn
