"""Regression: every AcroForm filler must produce PDFs whose form-field pages
include /Helv in /Resources/Font, and where auto-shrink picks the largest font
size that actually fits using real Helvetica metrics (not a heuristic that
underestimates font width).

Why this exists: pypdf's auto_regenerate writes /AP streams that reference
/Helv N Tf, but blank PDFs typically only define /F1 (or /TT0, /C2_0, etc.) at
the page level. Acrobat resolves /Helv from the AcroForm /DR fallback;
Chrome PDFium does NOT — it falls back to a substituted font with different
glyph widths, causing visible clipping (e.g. "Trabuco Cany..." instead of
"Trabuco Canyon"). _ensure_helv_font_on_pages() in reytech_filler_v4.py is
the canonical fix; this test guards it.
"""
import os
import tempfile

import pytest
from pypdf import PdfReader
from pypdf.generic import NameObject

from src.forms.reytech_filler_v4 import (
    _helv_string_width,
    fill_std1000,
    fill_std205,
)


CONFIG = {"company": {
    "name": "Reytech Inc.",
    "phone": "949-229-1575",
    "cert_number": "2002605",
    "address": "30 Carnoustie Way",
    "city": "Trabuco Canyon",
    "state": "CA",
    "zip": "92679",
    "fein": "47-4588061",
    "email": "sales@reytechinc.com",
    "owner": "Michael Guadan",
    "title": "Owner",
}}
RFQ_DATA = {
    "solicitation_number": "RFQ-CV-001",
    "sign_date": "04/24/2026",
    "line_items": [],
}


def _pages_with_text_widgets(reader):
    pages = []
    for idx, page in enumerate(reader.pages):
        if "/Annots" not in page:
            continue
        for annot in page["/Annots"]:
            obj = annot.get_object()
            if obj.get("/FT") == "/Tx":
                pages.append((idx, page))
                break
            parent = obj.get("/Parent")
            if parent and parent.get_object().get("/FT") == "/Tx":
                pages.append((idx, page))
                break
    return pages


def _has_helv(page):
    res = page.get("/Resources")
    if not res:
        return False
    res = res.get_object()
    fonts = res.get("/Font")
    if not fonts:
        return False
    fonts = fonts.get_object()
    return NameObject("/Helv") in fonts


def _fill(filler, blank_name):
    blank = os.path.join("tests", "fixtures", blank_name)
    out = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    out.close()
    filler(blank, RFQ_DATA, CONFIG, out.name)
    return out.name


@pytest.mark.parametrize("filler,blank", [
    (fill_std1000, "std1000_blank.pdf"),
    (fill_std205, "std205_blank.pdf"),
])
def test_helv_added_to_every_form_field_page(filler, blank):
    """Every page with a text-input widget must have /Helv in its font resources
    so embedded /AP streams resolve to a real Helvetica in every PDF viewer."""
    out = _fill(filler, blank)
    try:
        reader = PdfReader(out)
        pages = _pages_with_text_widgets(reader)
        assert pages, f"{blank}: expected at least one page with text widgets"
        missing = [idx for idx, page in pages if not _has_helv(page)]
        assert not missing, (
            f"{blank}: pages {missing} have text widgets but no /Helv in "
            f"/Resources/Font — Chrome will clip with substituted-font metrics"
        )
    finally:
        os.unlink(out)


def test_std1000_city_field_uses_largest_size_that_fits():
    """STD 1000 City is 67.2pt wide. 'Trabuco Canyon' (14ch) should auto-shrink
    to the largest Helvetica size whose real string width fits, NOT a heuristic
    that picks a smaller-than-needed size."""
    out = _fill(fill_std1000, "std1000_blank.pdf")
    try:
        reader = PdfReader(out)
        city_da = None
        city_value = None
        for page in reader.pages:
            if "/Annots" not in page:
                continue
            for annot in page["/Annots"]:
                obj = annot.get_object()
                if str(obj.get("/T", "")) == "City":
                    city_da = str(obj.get("/DA", ""))
                    city_value = str(obj.get("/V", ""))
                    break
            if city_da:
                break
        assert city_value == "Trabuco Canyon"
        assert "/Helv" in city_da, f"unexpected font in /DA: {city_da!r}"
        # Pull the size out of "/Helv N Tf 0 g"
        size = float(city_da.split()[1])
        # Field is 67.2pt wide; usable = 67.2 - 6 = 61.2pt with safety pad.
        # Largest Helv size where stringWidth("Trabuco Canyon") <= 61.2pt is 8pt.
        assert size == 8, (
            f"expected /Helv 8 (largest size that fits with safety pad), got {size}. "
            f"Real width at 8pt = {_helv_string_width('Trabuco Canyon', 8):.1f}, "
            f"at 9pt = {_helv_string_width('Trabuco Canyon', 9):.1f}"
        )
    finally:
        os.unlink(out)


def test_std205_contact_1_row_filled_completely():
    """STD 205 Contact 1 must have name + phone + email all populated.
    Filling EMAIL alone leaves a half-populated row that looks incomplete."""
    out = _fill(fill_std205, "std205_blank.pdf")
    try:
        reader = PdfReader(out)
        fields = reader.get_fields() or {}
        name = fields.get("contactName1", {}).get("/V", "")
        phone = fields.get("TELEPHONE_1", {}).get("/V", "")
        email = fields.get("EMAIL", {}).get("/V", "")
        assert name and "Michael Guadan" in name, f"contactName1={name!r}"
        assert phone == "949-229-1575", f"TELEPHONE_1={phone!r}"
        assert email == "sales@reytechinc.com", f"EMAIL={email!r}"
    finally:
        os.unlink(out)


def test_helv_string_width_matches_reportlab_metrics():
    """_helv_string_width must use real Helvetica metrics. If reportlab is
    unavailable it falls back to 0.55em — verify the real path is exercised."""
    # Real Helvetica width for "Trabuco Canyon" at 8pt is 59.14pt; the
    # 0.55em fallback would give 14 * 8 * 0.55 = 61.6pt. Real should win.
    w = _helv_string_width("Trabuco Canyon", 8)
    assert 58.0 <= w <= 60.0, (
        f"expected reportlab Helvetica metrics (~59pt), got {w:.2f}pt — "
        f"the 0.55em fallback would give ~61.6pt"
    )


def test_cchcs_attachment_filler_includes_helv():
    """fill_bidder_declaration goes through cchcs_attachment_fillers._fill_and_serialize,
    which is a separate code path from fill_and_sign_pdf. That path must also
    inject /Helv into the page resources — otherwise pages from that filler
    arrive in the merged package without /Helv and Chrome clips them."""
    from src.forms.cchcs_attachment_fillers import fill_bidder_declaration

    reytech = {
        "company_name": "Reytech Inc.",
        "cert_number": "2002605",
        "cert_type": "SB/DVBE",
        "description_of_goods": "Medical/Office and other supplies",
        "compliance": {"claiming_sb_preference": True, "uses_subcontractors": False},
    }
    parsed = {"header": {"solicitation_number": "RFQ-CV-001"}}

    buf = fill_bidder_declaration(reytech, parsed)
    assert buf is not None, "fill_bidder_declaration returned None"
    reader = PdfReader(buf)
    pages = _pages_with_text_widgets(reader)
    assert pages, "Bidder Decl: expected at least one page with text widgets"
    missing = [idx for idx, page in pages if not _has_helv(page)]
    assert not missing, (
        f"Bidder Decl: pages {missing} have text widgets but no /Helv — "
        f"_fill_and_serialize must call _ensure_helv_font_on_pages"
    )
