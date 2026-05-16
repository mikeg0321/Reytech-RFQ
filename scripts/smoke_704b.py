"""Smoke test of fill_704b_pdf with 8 and 25 line items (cross page boundary)."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.spine.model import Quote, LineItem
from src.spine.agency_forms.cchcs_703b import ReytechIdentity
from src.spine.agency_forms.cchcs_704b import fill_704b_pdf


def identity() -> ReytechIdentity:
    return ReytechIdentity(
        business_name="Reytech Inc.",
        address="100 Main St, San Diego CA 92101",
        contact_person="Michael Greenwald",
        title="President",
        phone="555-555-5555",
        fax="",
        email="mike@reytech.io",
        fein="12-3456789",
        sellers_permit="SR-100-12345",
        cert_number="0012345",
        cert_expiration="12/31/2027",
        payment_terms_days=45,
        payment_discount_pct=0.0,
        delivery_days=30,
    )


def quote(n: int) -> Quote:
    items = []
    for i in range(1, n + 1):
        items.append(LineItem(
            line_no=i,
            description=f"Test product line {i} ZZUNIQUE{i:03d}",
            qty=10 + i,
            uom="EA",
            unit_price_cents=1099 + i * 11,
            cost_cents=799 + i * 11,
        ))
    return Quote(
        quote_id=f"smoke-704b-{n:03d}",
        agency="CCHCS",
        facility="CCHCS Stockton",
        solicitation_number="10846581",
        tax_rate_bps=898,
        line_items=items,
    )


def run(n: int, *, flatten: bool):
    today = datetime(2026, 5, 16, 12, 0, 0)
    q = quote(n)
    print(f"\n--- {n} items, flatten={flatten} ---")
    try:
        pdf_bytes = fill_704b_pdf(q, identity(), today=today, flatten=flatten)
    except Exception as e:
        print(f"FAIL: {type(e).__name__}: {e}")
        return
    out = Path(__file__).resolve().parents[1] / "_diag" / "704b" / f"smoke_{n}_flat{flatten}.pdf"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(pdf_bytes)
    print(f"OK: {len(pdf_bytes)} bytes -> {out}")


def main():
    for n in (1, 8, 23, 24, 39):
        run(n, flatten=True)
        run(n, flatten=False)


if __name__ == "__main__":
    main()
