"""Mike P0 2026-05-06 (Audit P0 #5): _add_signature_to_pdf must validate
the detected signature rect before drawing. A mis-detected rect in the
top 30% of the page would draw the signature image floating above the
form (invisible to the buyer).

Pre-fix: the function trusted whatever `_detect_sig_field_rect` returned
and only used the AMS 704 hardcoded fallback when detection returned
None. A buyer-supplied PDF with a header label named 'Signature'
(common in DocuSign-flattened forms) would silently mis-place the sig.

Post-fix: detected rects are validated against `fb > 0.7 * page_height`
(top 30% = mis-detection). Trusted sources (sig_rect_override from DOCX
extraction, hardcoded AMS 704 fallback) bypass the check.

Source-level guards plus a behavioral test that monkey-patches the
detector to return a bogus top-of-page rect.
"""
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── source-level: validation pattern is in place ──────────────────


def test_sig_rect_validation_uses_top_30pct_threshold():
    body = (REPO / "src/forms/price_check.py").read_text(encoding="utf-8")
    fn_start = body.find("def _add_signature_to_pdf(")
    assert fn_start > 0, "_add_signature_to_pdf not found"
    # Function body — search next ~3500 chars
    fn_body = body[fn_start:fn_start + 4500]
    # Must check fb against 0.7 * page_height
    assert "0.7 * page_height" in fn_body or "page_height * 0.7" in fn_body, (
        "_add_signature_to_pdf must validate detected sig rect against "
        "fb > 0.7 * page_height (top 30%% of page = mis-detection). "
        "Pre-fix did no validation — buyer PDFs with header labels named "
        "'Signature' would mis-place the signature image."
    )


def test_sig_rect_validation_only_applies_to_detected_rects():
    """The override path (DOCX-extracted cells) and the hardcoded fallback
    are trusted — validation must NOT apply to them, otherwise the AMS 704
    fallback (fb=388 on 612-height page = 63% up) would itself be discarded
    and produce an infinite fallback loop."""
    body = (REPO / "src/forms/price_check.py").read_text(encoding="utf-8")
    fn_start = body.find("def _add_signature_to_pdf(")
    fn_body = body[fn_start:fn_start + 4500]
    # The validation block must gate on _from_detection (a flag set only when
    # the rect came from _detect_sig_field_rect, not from override or fallback)
    assert "_from_detection" in fn_body, (
        "Validation must distinguish detected rects from trusted override/"
        "fallback sources via a _from_detection flag — otherwise the AMS 704 "
        "hardcoded fallback (fb=388 on 612-height page) would itself trigger "
        "the validation and create an infinite fallback loop."
    )


# ── behavioral: bogus top-of-page detection is rejected ───────────


def test_top_of_page_detection_is_discarded(monkeypatch):
    """Monkey-patch `_detect_sig_field_rect` to return a bogus rect at the
    top of the page (fb=550 on 612-height landscape). The function should
    reject it and fall back to AMS 704 hardcoded coords."""
    from src.forms import price_check
    from pypdf import PdfWriter

    # Build a minimal writer with one landscape page (792x612)
    writer = PdfWriter()
    # Use a blank page so mediabox is well-defined
    from pypdf.generic import RectangleObject
    page = writer.add_blank_page(width=792, height=612)
    assert float(page.mediabox.width) == 792.0
    assert float(page.mediabox.height) == 612.0

    # Patch the detector to return a bogus top-of-page rect.
    # fb=550 is >0.7 * 612 = 428 → should be rejected.
    bogus = (100.0, 550.0, 400.0, 580.0)
    monkeypatch.setattr(price_check, "_detect_sig_field_rect",
                        lambda *_a, **_kw: bogus)

    # We don't actually want the signature drawn — we just want to prove
    # the validation triggered. Capture log warnings to confirm.
    import logging
    captured = []

    class _Capture(logging.Handler):
        def emit(self, record):
            captured.append(record.getMessage())

    handler = _Capture()
    price_check.log.addHandler(handler)
    try:
        # The function will likely fail at the actual draw step (no real PDF
        # writer state, no signature image lookup), but the validation
        # warning fires first. We catch any exception after.
        try:
            price_check._add_signature_to_pdf(writer)
        except Exception:
            pass
    finally:
        price_check.log.removeHandler(handler)

    # Validation warning must have fired
    matched = [m for m in captured
               if "discarding as mis-detection" in m
               or "in top 30%" in m]
    assert matched, (
        f"Expected the validation warning to fire when fb=550 (top 10%% "
        f"of 612-height page). Captured logs: {captured}"
    )
