"""Precise coordinate measurements for DSH AttA/B/C overlay fillers.

For each label we need to find ("Firm's Legal Name:", "Phone Number:", etc),
report the BOTTOM of the label so we can write the value just below it,
and the bounding box of the cell beneath.

PDF coords: pdfplumber y0 = top from page top. We need reportlab y, which
is measured from the bottom: rl_y = page_height - pdfplumber_y.

For each anchor, output: (label, page_y_top, suggested_rl_x, suggested_rl_y).
"""
from __future__ import annotations

from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "dsh"


def find(words, label_words):
    """Return the (x0, top, bottom) of a label like 'Firm's Legal Name:' which
    spans multiple word entries from extract_words."""
    needles = label_words
    n = len(needles)
    for i in range(len(words) - n + 1):
        if all(words[i + j]["text"].rstrip(":,") == needles[j].rstrip(":,")
               for j in range(n)):
            x0 = words[i]["x0"]
            top = min(words[i + j]["top"] for j in range(n))
            bot = max(words[i + j]["bottom"] for j in range(n))
            return (x0, top, bot)
    return None


def report(name, path, anchors):
    print(f"\n=== {name} ({path.name}) ===")
    with pdfplumber.open(str(path)) as pdf:
        page = pdf.pages[0]
        page_h = page.height
        words = page.extract_words(use_text_flow=True)
        for label_text, *needle in anchors:
            words_needle = needle if needle else label_text.split()
            found = find(words, words_needle)
            if found is None:
                print(f"  {label_text!r:<45} NOT FOUND")
            else:
                x0, top, bot = found
                rl_y_below = page_h - bot - 12  # 12pt below label
                print(f"  {label_text!r:<45} x={x0:6.1f}  bottom={bot:6.1f}  "
                      f"rl_y_below={rl_y_below:6.1f}")


def main():
    report("AttA", FIXTURES / "dsh_25CB020_attachA_bidder.pdf", [
        ("Firm's Legal Name:", "Firm's", "Legal", "Name:"),
        ("Seller's Permit", "Seller's", "Permit"),
        ("Firm's Address:", "Firm's", "Address:"),
        ("City, State, and Zip:", "City,", "State,", "and", "Zip:"),
        ("Bidder's FEIN", "Bidder's", "FEIN", "Number:"),
        ("Approximate Lead Time:", "Approximate", "Lead", "Time:"),
        ("Product Warranty Period:", "Product", "Warranty", "Period:"),
        ("Date Solicitation Expires", "Date", "Solicitation", "Expires"),
        ("DVBE Participation %:", "DVBE", "Participation", "%:"),
        ("Designated Contact Person:", "Designated", "Contact", "Person:"),
        ("Phone Number:", "Phone", "Number:"),
        ("E-Mail:", "E-Mail:"),
        ("Printed Name and Title", "Printed", "Name", "and", "Title", "of", "Signatory:"),
        ("Signature:", "Signature:"),
        ("Date Executed:", "Date", "Executed:"),
    ])

    report("AttB", FIXTURES / "dsh_25CB020_attachB_pricing.pdf", [
        ("Vendor Name:", "Vendor", "Name:"),
        ("# (header)", "DESCRIPTION", "OF", "GOODS", "/", "SERVICES"),
        ("SUBTOTAL:", "SUBTOTAL:"),
        ("OTHER, NON-SHIPPING*", "OTHER,", "NON-SHIPPING*"),
        ("TOTAL:", "TOTAL:"),
    ])

    # AttB: also find the Y of each item-row (rows 1-7)
    print("\n  AttB pricing-table Y centers (from rects):")
    with pdfplumber.open(str(FIXTURES / "dsh_25CB020_attachB_pricing.pdf")) as pdf:
        page = pdf.pages[0]
        # extract_table returns text per row but we need rect y-centers for placement
        words = page.extract_words(use_text_flow=True)
        # Find each item number at column ~32 px left edge
        for w in words:
            txt = w["text"].strip()
            if txt in {"1", "2", "3", "4", "5", "6", "7"} and w["x0"] < 50:
                rl_y = page.height - (w["top"] + w["bottom"]) / 2 - 4
                print(f"    item {txt} top={w['top']:.1f}  rl_y_unitprice={rl_y:.1f}")

    report("AttC", FIXTURES / "dsh_25CB020_attachC_forms.pdf", [
        ("Vendor Name:", "Vendor", "Name:"),
    ])


if __name__ == "__main__":
    main()
