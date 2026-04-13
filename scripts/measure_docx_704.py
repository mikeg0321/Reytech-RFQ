"""Convert the two DOCX 704 samples via the production doc_converter,
then measure the actual cell rects in each PDF and compare to what
_detect_ams704_overlay_positions() returns today. The goal is to
quantify the drift pattern so we can build a correct DOCX calibration.

Writes everything to _overnight_review/docx_704_measurements.json plus
PNG renders of every page via PyMuPDF for visual inspection.
"""
import json
import os
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)  # scripts/ is one level below repo root
REVIEW_DIR = os.path.join(REPO, "_overnight_review")
os.makedirs(REVIEW_DIR, exist_ok=True)
sys.path.insert(0, REPO)

os.environ.setdefault("SECRET_KEY", "diag")
os.environ.setdefault("DASH_USER", "diag")
os.environ.setdefault("DASH_PASS", "diag")
os.environ.setdefault("FLASK_ENV", "testing")


FIXTURES = [
    os.path.join(REPO, "tests", "fixtures", "docx_704", "sample_non_food.docx"),
    os.path.join(REPO, "tests", "fixtures", "docx_704", "sample_food.docx"),
]


def convert_docx(docx_path, out_dir):
    from src.forms.doc_converter import convert_to_pdf
    print(f"  converting {os.path.basename(docx_path)}...")
    pdf = convert_to_pdf(docx_path, output_dir=out_dir)
    print(f"    -> {os.path.basename(pdf)}")
    return pdf


def measure_rects(pdf_path):
    """Use pdfplumber to dump cell rects, horizontal lines, and word
    positions for every page. Returns a structured dict per page."""
    import pdfplumber
    out = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            ph = float(page.height)
            pw = float(page.width)
            page_info = {
                "page": i + 1,
                "width": pw,
                "height": ph,
                "producer": None,
                "rects": [],
                "h_lines": [],
                "item_numbers": [],
                "ext_header": None,
                "price_header": None,
            }
            # Rects (table cells)
            for r in page.rects:
                w = float(r["width"])
                h = float(r["height"])
                if w < 20 or h < 6:
                    continue
                rl_y0 = ph - float(r["bottom"])
                rl_y1 = ph - float(r["top"])
                page_info["rects"].append({
                    "x0": round(float(r["x0"]), 1),
                    "y0": round(rl_y0, 1),
                    "x1": round(float(r["x1"]), 1),
                    "y1": round(rl_y1, 1),
                    "w": round(w, 1),
                    "h": round(h, 1),
                })
            # Horizontal lines
            h_lines = sorted(set(
                round(e["top"], 1) for e in page.edges
                if abs(e["top"] - e["bottom"]) < 2
                and e["x1"] - e["x0"] > pw * 0.3
            ))
            page_info["h_lines"] = h_lines

            # Words
            words = page.extract_words(keep_blank_chars=False,
                                         x_tolerance=3, y_tolerance=3)
            for w in words:
                t = w["text"].strip()
                if t.upper() == "EXTENSION" and w["x0"] > pw * 0.6:
                    page_info["ext_header"] = {
                        "x0": round(float(w["x0"]), 1),
                        "x1": round(float(w["x1"]), 1),
                        "top": round(float(w["top"]), 1),
                    }
                if t.upper() in ("PRICE", "PRIC") and w["x0"] > pw * 0.6:
                    page_info["price_header"] = {
                        "x0": round(float(w["x0"]), 1),
                        "x1": round(float(w["x1"]), 1),
                        "top": round(float(w["top"]), 1),
                    }
                if t.isdigit() and 1 <= int(t) <= 50 and float(w["x0"]) < pw * 0.07:
                    page_info["item_numbers"].append({
                        "num": int(t),
                        "top": round(float(w["top"]), 1),
                        "bottom": round(float(w["bottom"]), 1),
                        "rl_top": round(ph - float(w["top"]), 1),
                        "rl_bot": round(ph - float(w["bottom"]), 1),
                    })
            out.append(page_info)

        # Producer metadata
        producer = pdf.metadata.get("Producer") or pdf.metadata.get("Creator") or ""
        for p in out:
            p["producer"] = producer
    return out


def run_current_detector(pdf_path):
    """Invoke the current _detect_ams704_overlay_positions and capture
    what it returns so we can compare the 'detected' rows against the
    actual cell rects measured above."""
    from src.forms.price_check import _detect_ams704_overlay_positions
    detected = _detect_ams704_overlay_positions(pdf_path)
    out = []
    if detected is None:
        return None
    for i, pg in enumerate(detected):
        if pg is None:
            out.append({"page": i + 1, "detected": False})
            continue
        out.append({
            "page": i + 1,
            "detected": True,
            "n_rows": len(pg.get("item_rows", [])),
            "item_rows": [(round(yb, 1), round(yt, 1))
                          for yb, yt in pg.get("item_rows", [])],
            "desc_tops": [round(t, 1) for t in pg.get("desc_tops", [])],
            "price_x": tuple(round(x, 1) for x in pg.get("price_x") or ()),
            "ext_x": tuple(round(x, 1) for x in pg.get("ext_x") or ()),
            "supplier_cells": {
                k: tuple(round(x, 1) for x in v)
                for k, v in (pg.get("supplier_cells") or {}).items()
            },
            "orig_values": pg.get("orig_values", []),
        })
    return out


def render_pages(pdf_path, out_dir, prefix):
    import fitz
    doc = fitz.open(pdf_path)
    saved = []
    for i, page in enumerate(doc):
        pix = page.get_pixmap(dpi=150)
        path = os.path.join(out_dir, f"{prefix}_pg{i+1:02d}.png")
        pix.save(path)
        saved.append(path)
    doc.close()
    return saved


def main():
    out_pdf_dir = os.path.join(REVIEW_DIR, "docx_converted")
    os.makedirs(out_pdf_dir, exist_ok=True)
    screenshots_dir = os.path.join(REVIEW_DIR, "screenshots", "docx_704")
    os.makedirs(screenshots_dir, exist_ok=True)

    report = {}
    for fx in FIXTURES:
        if not os.path.exists(fx):
            print(f"MISSING fixture: {fx}")
            continue
        name = os.path.basename(fx).replace(".docx", "")
        print(f"\n=== {name} ===")
        try:
            pdf_path = convert_docx(fx, out_pdf_dir)
        except Exception as e:
            print(f"  conversion FAILED: {e}")
            report[name] = {"error": f"conversion failed: {e}"}
            continue

        print("  measuring cell rects...")
        measured = measure_rects(pdf_path)
        print(f"    {len(measured)} page(s), producer={measured[0].get('producer','?')}")
        for p in measured:
            n_items = len(p["item_numbers"])
            n_rects = len(p["rects"])
            print(f"    pg{p['page']}: {n_rects} rects, {n_items} item numbers, "
                  f"ext_header={p['ext_header']}, price_header={p['price_header']}")

        print("  running current _detect_ams704_overlay_positions...")
        detected = run_current_detector(pdf_path)
        if detected is None:
            print("    detector returned None (no detection)")
        else:
            for d in detected:
                if d["detected"]:
                    print(f"    pg{d['page']}: {d['n_rows']} rows detected, "
                          f"price_x={d['price_x']}, ext_x={d['ext_x']}")
                else:
                    print(f"    pg{d['page']}: detection failed")

        print("  rendering PNGs...")
        pngs = render_pages(pdf_path, screenshots_dir, name)
        print(f"    {len(pngs)} page(s) rendered")

        report[name] = {
            "pdf": os.path.basename(pdf_path),
            "measured": measured,
            "detected": detected,
            "pngs": [os.path.basename(p) for p in pngs],
        }

    dest = os.path.join(REVIEW_DIR, "docx_704_measurements.json")
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nwrote {dest}")


if __name__ == "__main__":
    main()
