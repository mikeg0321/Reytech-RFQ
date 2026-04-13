"""Fill a DOCX-converted 704 via _fill_pdf_text_overlay and measure
where the overlay text actually landed vs where the detector said it
should go. Reports the drift pattern so we can calibrate the detector.

This is the bridge between "I know what the detector returns" and
"I know where the text actually lands on the page" — the gap that
makes the DOCX 704 bug hard to see without running the full fill path.
"""
import json
import os
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
REVIEW_DIR = os.path.join(REPO, "_overnight_review")
sys.path.insert(0, REPO)

os.environ.setdefault("SECRET_KEY", "diag")
os.environ.setdefault("DASH_USER", "diag")
os.environ.setdefault("DASH_PASS", "diag")
os.environ.setdefault("FLASK_ENV", "testing")


def main():
    from src.forms.doc_converter import convert_to_pdf
    from src.forms.price_check import (
        _detect_ams704_overlay_positions,
        _fill_pdf_text_overlay,
    )
    import pdfplumber

    src_docx = os.path.join(REPO, "tests", "fixtures", "docx_704",
                            "sample_non_food.docx")
    out_dir = os.path.join(REVIEW_DIR, "docx_converted")
    os.makedirs(out_dir, exist_ok=True)

    print(f"converting {os.path.basename(src_docx)}...")
    src_pdf = convert_to_pdf(src_docx, output_dir=out_dir)
    print(f"  -> {src_pdf}")

    # Build fake field_values mimicking what fill_ams704 would produce
    # for a 3-item PC. Values include supplier info + per-row prices.
    field_values = [
        {"field_id": "COMPANY NAME", "value": "Reytech Inc."},
        {"field_id": "Address", "value": "30 Carnoustie Way, Trabuco Canyon CA 92679"},
        {"field_id": "Phone Number_2", "value": "949-229-1575"},
        {"field_id": "EMail Address", "value": "sales@reytechinc.com"},
        {"field_id": "COMPANY REPRESENTATIVE print name", "value": "Michael Guadan"},
        {"field_id": "Certified SBMB", "value": "2002605"},
        {"field_id": "Certified DVBE", "value": "2002605"},
        {"field_id": "Date Price Check Expires", "value": "5/31/2027"},
        {"field_id": "Delivery Date and Time ARO", "value": "5-7 days"},
        {"field_id": "Discount Offered", "value": "Included"},
        # Row data — 3 rows with distinct test prices
        {"field_id": "PRICE PER UNITRow1", "value": "111.11"},
        {"field_id": "EXTENSIONRow1", "value": "222.22"},
        {"field_id": "PRICE PER UNITRow2", "value": "333.33"},
        {"field_id": "EXTENSIONRow2", "value": "444.44"},
        {"field_id": "PRICE PER UNITRow3", "value": "555.55"},
        {"field_id": "EXTENSIONRow3", "value": "666.66"},
    ]

    out_pdf = os.path.join(out_dir, "sample_non_food_filled.pdf")
    print(f"\nfilling via _fill_pdf_text_overlay...")
    _fill_pdf_text_overlay(src_pdf, field_values, out_pdf)
    print(f"  -> {out_pdf}")

    # Now read the detector output + measure where text landed
    print(f"\ndetector output:")
    detected = _detect_ams704_overlay_positions(src_pdf)
    for i, d in enumerate(detected or []):
        if d is None:
            print(f"  pg{i+1}: None")
            continue
        print(f"  pg{i+1}: {len(d['item_rows'])} rows, "
              f"price_x={d['price_x']}, ext_x={d['ext_x']}")
        for j, (yb, yt) in enumerate(d["item_rows"]):
            print(f"    row{j+1}: y_bot={yb:.1f} y_top={yt:.1f} band={yt-yb:.1f}pt")

    print(f"\nactual text positions in filled PDF:")
    with pdfplumber.open(out_pdf) as pdf:
        for i, page in enumerate(pdf.pages):
            ph = float(page.height)
            pw = float(page.width)
            print(f"  pg{i+1}: {pw:.0f}x{ph:.0f}")
            words = page.extract_words(keep_blank_chars=False,
                                        x_tolerance=2, y_tolerance=2)
            # Find our test values — they're unique 6-digit numbers
            test_values = {"111.11", "222.22", "333.33", "444.44",
                           "555.55", "666.66"}
            for w in words:
                t = w["text"].strip().replace(",", "")
                if t in test_values:
                    rl_y0 = ph - float(w["bottom"])
                    rl_y1 = ph - float(w["top"])
                    print(f"    '{t}' at x=({float(w['x0']):.1f},{float(w['x1']):.1f}) "
                          f"rl_y=({rl_y0:.1f},{rl_y1:.1f})")

    # Render the filled PDF for visual inspection
    print(f"\nrendering filled PDF...")
    import fitz
    doc = fitz.open(out_pdf)
    for i, page in enumerate(doc):
        pix = page.get_pixmap(dpi=150)
        out = os.path.join(REVIEW_DIR, "screenshots", "docx_704",
                           f"non_food_filled_pg{i+1:02d}.png")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        pix.save(out)
        print(f"  {out}")
    doc.close()

    print("\nDONE")


if __name__ == "__main__":
    main()
