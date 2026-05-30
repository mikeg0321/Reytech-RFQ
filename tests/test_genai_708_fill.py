"""AMS 708 (GenAI Disclosure) fill correctness.

fill_genai_708 previously wrote field names that do not exist on the real
AMS 708 (Rev. 03/2025) form ("Company Name", "No", "NoGenAI", ...), so it
produced a BLANK 708 wherever it ran. The real AcroForm fields are
708_Text1..16 + 708_Check Box1 (Yes) / 708_Check Box2 (No), each on-state
"/Yes".

Reytech never uses GenAI, so the correct output checks "No" (708_Check Box2)
and fills the 15 detail cells "N/A".

NOTE on the fixture: the 708 form lives inside the CDCR bid-package template.
You CANNOT extract just the 708 pages with PdfWriter.add_page — pypdf page
copy drops the AcroForm, so the fields disappear. The filler runs in place on
the full template (exactly how fill_bid_package uses it), so the test fills a
copy of the full template and reads the 708_* values back.
"""
import os
import shutil
from pathlib import Path

import pytest

TMPL = Path("data/templates/cdcr_bid_package_template.pdf")
pytestmark = pytest.mark.skipif(not TMPL.exists(), reason="bid-package template absent")

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

    src = str(tmp_path / "pkg_in.pdf")
    out = str(tmp_path / "pkg_out.pdf")
    shutil.copy(str(TMPL), src)               # full template — AcroForm intact
    fill_genai_708(src, {"solicitation_number": "10843276",
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


def _values_dict_source() -> str:
    """Return just the `values = {...}` literal of fill_genai_708 (no comments),
    so the anti-regression check inspects real code, not the explanatory note."""
    src = Path("src/forms/reytech_filler_v4.py").read_text(encoding="utf-8")
    fn = src[src.index("def fill_genai_708"):src.index("def fill_std205")]
    start = fn.index("values = {")
    end = fn.index("}", start) + 1
    return fn[start:end]


def test_genai_708_uses_real_field_names_not_guesses():
    """Anti-regression: the values dict must use real 708_* fields, never the
    old non-existent guesses that produced a blank form."""
    vd = _values_dict_source()
    assert '"708_Check Box2": "/Yes"' in vd
    assert '"708_Text1"' in vd and '"708_Text3"' in vd
    # the old broken guesses must be gone from the actual field map
    assert "NoGenAI" not in vd
    assert '"Company Name"' not in vd
    assert '"Vendor Name"' not in vd
