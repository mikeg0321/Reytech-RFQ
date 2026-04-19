"""One-off inspector for DSH packet + PostConsumer Cert blanks.

Reports for each PDF:
  - page count
  - per-page AcroForm field count + names
  - text lines with positions (so we can find anchor labels for overlay)
  - drawn rects (so we can find table cells for line items / checkboxes)

Run: python scripts/inspect_dsh_blanks.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pdfplumber
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "data" / "templates" / "dsh"
FIXTURES = ROOT / "tests" / "fixtures" / "dsh"

TARGETS = [
    ("postconsumer_template", TEMPLATES / "postconsumer_content_cert_blank.pdf"),
    ("dvbe_dsh_template",     TEMPLATES / "dvbe_declaration_dsh.pdf"),
    ("darfur_dsh_template",   TEMPLATES / "darfur_act_dsh.pdf"),
    ("bidder_dsh_template",   TEMPLATES / "bidder_declaration_dsh.pdf"),
    ("std204_dsh_template",   TEMPLATES / "std204_dsh.pdf"),
    ("attA_blank_packet",     FIXTURES  / "dsh_25CB020_attachA_bidder.pdf"),
    ("attB_blank_packet",     FIXTURES  / "dsh_25CB020_attachB_pricing.pdf"),
    ("attC_blank_packet",     FIXTURES  / "dsh_25CB020_attachC_forms.pdf"),
]


def inspect(name: str, path: Path) -> None:
    print(f"\n{'='*80}\n{name}: {path.name}\n{'='*80}")
    if not path.exists():
        print("  (missing)")
        return

    reader = PdfReader(str(path))
    print(f"  pages: {len(reader.pages)}")
    fields = reader.get_form_text_fields() or {}
    all_fields = reader.get_fields() or {}
    print(f"  total form fields: {len(all_fields)}")
    if all_fields:
        for fname, fobj in list(all_fields.items())[:30]:
            ftype = fobj.get("/FT", "?")
            print(f"    field={fname!r} type={ftype}")

    with pdfplumber.open(str(path)) as pdf:
        for pi, page in enumerate(pdf.pages):
            print(f"\n  -- page {pi+1} ({page.width:.0f} x {page.height:.0f}) --")
            words = page.extract_words(use_text_flow=True)
            print(f"  text segments: {len(words)}")
            # show first 30 words with positions
            for w in words[:40]:
                print(f"    text={w['text']!r:<40} x={w['x0']:.1f} y={w['top']:.1f}")
            rects = page.rects or []
            print(f"  rects: {len(rects)}")
            for r in rects[:12]:
                print(f"    rect x0={r['x0']:.1f} y0={r['top']:.1f} "
                      f"w={r['width']:.1f} h={r['height']:.1f}")


def main() -> int:
    for name, p in TARGETS:
        inspect(name, p)
    return 0


if __name__ == "__main__":
    sys.exit(main())
