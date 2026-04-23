"""One-shot PDF merge: buyer's LPA template + Reytech forms for RFQ 10840486.

Written 2026-04-22 to give the operator a SINGLE combined PDF that can be
hand-filled in Acrobat:
  - Pages 1-N: the buyer's LPA IT RFQ template (from 10840486_703B_Reytech.pdf,
    which is the buyer's template with whatever generic fields the 703B filler
    happened to match — mostly still blank, needs hand-fill).
  - Pages N+1..: Reytech-generated Quote (CORRECTED), Bid Package, Sellers Permit.

The buyer's template pages stay fillable so the operator can complete them
in Acrobat. The Reytech pages are already filled (ship-to, tax, pricing)
by the finalize_rfq_10840486.py run.

Output: /data/output/10840486/10840486_RFQ_Package_Reytech_CORRECTED.pdf

Usage:
  railway ssh /opt/venv/bin/python scripts/merge_rfq_package_10840486.py
"""
import os
import sys

sys.path.insert(0, "/app")

OUT_DIR = "/data/output/10840486"
SOURCES = [
    # Order matters — this is the order pages appear in the merged PDF.
    # Operator fills the buyer's template first, then reviews Reytech output.
    ("10840486_703B_Reytech.pdf",
     "Buyer LPA template (hand-fill in Acrobat)"),
    ("10840486_Quote_Reytech_CORRECTED.pdf",
     "Reytech Quote — CORRECTED ship-to + 7.75% tax"),
    ("10840486_BidPackage_Reytech.pdf",
     "Reytech Bid Package — DVBE 843, Darfur, CalRecycle etc."),
    ("10840486_SellersPermit_Reytech.pdf",
     "Reytech CA Sellers Permit"),
]
OUT_FILE = "10840486_RFQ_Package_Reytech_CORRECTED.pdf"


def main():
    missing = []
    paths = []
    for fname, _desc in SOURCES:
        p = os.path.join(OUT_DIR, fname)
        if not os.path.exists(p):
            missing.append(fname)
        paths.append(p)
    if missing:
        print(f"ERROR: missing source files: {missing}")
        sys.exit(1)

    out_path = os.path.join(OUT_DIR, OUT_FILE)

    from pypdf import PdfWriter
    writer = PdfWriter()
    for p, (fname, desc) in zip(paths, SOURCES):
        size = os.path.getsize(p)
        print(f"[APPEND] {fname} ({size:,} bytes) — {desc}")
        writer.append(p)
    with open(out_path, "wb") as f:
        writer.write(f)
    writer.close()

    out_size = os.path.getsize(out_path)
    print(f"\n✓ Merged {len(paths)} PDFs → {out_path} ({out_size:,} bytes)")
    print()
    print("Next steps:")
    print(f"  1. Download: https://web-production-dcee9.up.railway.app"
          f"/api/download/10840486/{OUT_FILE}")
    print(f"  2. Open in Acrobat.")
    print(f"  3. Hand-fill the LPA template pages (page 1-N).")
    print(f"  4. Save + send. Single attachment.")


if __name__ == "__main__":
    main()
