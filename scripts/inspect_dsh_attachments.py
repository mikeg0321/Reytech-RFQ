"""Dump full text + table structure for the 3 flat DSH attachments.

Goal: identify the fields/cells the operator needs to fill on
each attachment so we can build overlay fillers.

Run: python scripts/inspect_dsh_attachments.py > tmp/dsh_atts.txt
"""
from __future__ import annotations

from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "dsh"

ATTS = [
    ("AttA — Bidder Information",   FIXTURES / "dsh_25CB020_attachA_bidder.pdf"),
    ("AttB — Pricing",              FIXTURES / "dsh_25CB020_attachB_pricing.pdf"),
    ("AttC — Required Forms",       FIXTURES / "dsh_25CB020_attachC_forms.pdf"),
]


def dump(name: str, path: Path) -> None:
    print(f"\n{'='*100}\n{name}: {path.name}\n{'='*100}")
    with pdfplumber.open(str(path)) as pdf:
        for pi, page in enumerate(pdf.pages):
            print(f"\n--- page {pi+1} ({page.width:.0f} x {page.height:.0f}) ---")
            print("\nFULL TEXT:")
            print(page.extract_text())
            print("\nTABLES:")
            for ti, table in enumerate(page.extract_tables() or []):
                print(f"\n  Table {ti+1}:")
                for row in table:
                    print(f"    {row}")
            print("\nLARGE RECTS (likely cells):")
            rects = page.rects or []
            big = [r for r in rects if r["width"] > 30 and r["height"] > 8]
            for r in big[:60]:
                print(f"    x0={r['x0']:.1f} y0={r['top']:.1f} "
                      f"w={r['width']:.1f} h={r['height']:.1f} (bottom={r['bottom']:.1f})")


def main() -> int:
    for name, p in ATTS:
        dump(name, p)
    return 0


if __name__ == "__main__":
    main()
