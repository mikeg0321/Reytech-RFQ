"""Smoke test of fill_bidpkg_pdf."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.spine.model import Quote, LineItem
from src.spine.agency_forms import ReytechIdentity, fill_bidpkg_pdf


def identity() -> ReytechIdentity:
    return ReytechIdentity(
        business_name="Reytech Inc.",
        address="1 Reytech Way, Irvine, CA 92602",
        contact_person="Michael Greenwald",
        title="President",
        phone="949-229-1575",
        email="rfq@reytechinc.com",
        fein="99-1234567",
        sellers_permit="SR-100-12345",
        cert_number="0012345",
    )


def quote() -> Quote:
    return Quote(
        quote_id="rfq_bidpkg_smoke",
        agency="CCHCS",
        facility="Test - CCWF",
        solicitation_number="10846581",
        line_items=[LineItem(
            line_no=1,
            description="GLOVES, NITRILE, MEDICAL EXAMINATION GRADE",
            mfg_number="MK-2103L",
            qty=10, uom="BX",
            cost_cents=1000, unit_price_cents=2000,
        )],
        tax_rate_bps=898,
    )


def main():
    today = datetime(2026, 5, 16)
    for flat in (True, False):
        try:
            pdf = fill_bidpkg_pdf(quote(), identity(), today=today, flatten=flat)
            out = Path(__file__).resolve().parents[1] / "_diag" / "bidpkg" / f"smoke_flat{flat}.pdf"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(pdf)
            print(f"OK flat={flat}: {len(pdf)} bytes -> {out}")
        except Exception as e:
            print(f"FAIL flat={flat}: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
