"""V2 of merge script — Quote PDF EXCLUDED per Mike 2026-04-23.

The Reytech Quote PDF is submitted SEPARATELY from the RFQ package.
The package attachment is ONLY: filled LPA template + bid package + sellers permit.

North star reference (what Mike actually sent for this quote):
  tests/fixtures/rfq_packages/10840486_rfq_package_NORTHSTAR.pdf
  20 pages total. Page inventory:
    1-5   LPA IT RFQ (buyer's form, filled by Reytech)
    6-8   Section dividers + Attachments 1 & 6
    9-13  CCHCS Certifications + Attachment 8
    14-16 State forms (DVBE 843 / CUF / Bidder declaration)
    17-19 Certification/Bidder wrap-up pages
    20    CA Sellers Permit

Our v1 bundle (with Quote) = 21 pages — 1 extra page for the Quote that
shouldn't be there. v2 drops the Quote and matches the 20-page north star
page-count. Signature placement still needs a dedicated audit pass
(audit item BB — north star fixture now in place for regression).

Usage:
  railway ssh /opt/venv/bin/python scripts/merge_rfq_package_10840486_v2.py
"""
import os, sys
sys.path.insert(0, "/app")

OUT_DIR = "/data/output/10840486"
SOURCES = [
    # Quote INTENTIONALLY OMITTED — submitted separately, not part of package.
    ("10840486_703B_Reytech.pdf",
     "Buyer LPA template (hand-fill in Acrobat)"),
    ("10840486_BidPackage_Reytech.pdf",
     "Bid Package — DVBE 843, Darfur, CalRecycle etc."),
    ("10840486_SellersPermit_Reytech.pdf",
     "CA Sellers Permit"),
]
OUT_FILE = "10840486_RFQ_Package_Reytech_v2.pdf"


def main():
    missing = [fn for fn, _ in SOURCES if not os.path.exists(os.path.join(OUT_DIR, fn))]
    if missing:
        print(f"ERROR missing: {missing}")
        sys.exit(1)

    from pypdf import PdfWriter
    out_path = os.path.join(OUT_DIR, OUT_FILE)
    w = PdfWriter()
    for fn, desc in SOURCES:
        p = os.path.join(OUT_DIR, fn)
        print(f"[APPEND] {fn} ({os.path.getsize(p):,} bytes) — {desc}")
        w.append(p)
    with open(out_path, "wb") as f:
        w.write(f)
    w.close()

    out_size = os.path.getsize(out_path)
    print(f"\n✓ Merged {len(SOURCES)} PDFs → {out_path} ({out_size:,} bytes)")
    print()
    print("Download:")
    print(f"  https://web-production-dcee9.up.railway.app/api/download/10840486/{OUT_FILE}")
    print()
    print("Submit separately:")
    print(f"  Quote R26Q37 → 10840486_Quote_Reytech_CORRECTED.pdf")
    print()
    print("NOTE: signatures still need reconciliation vs north star fixture")
    print("      tests/fixtures/rfq_packages/10840486_rfq_package_NORTHSTAR.pdf")
    print("      (audit item BB, not fixed in v2).")


if __name__ == "__main__":
    main()
