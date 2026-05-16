"""Diagnostic for 704B fill — isolate the pikepdf transformation issue.

Smoke-fills the 704B template via the cchcs_704b module with flatten=True
and flatten=False, dumping field state and extracted text at each stage,
so we can isolate where the row-level data goes missing.
"""
from __future__ import annotations

import io
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.spine.model import Quote, LineItem
from src.spine.agency_forms.cchcs_703b import ReytechIdentity
from src.spine.agency_forms import cchcs_704b


def build_quote(n: int) -> Quote:
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
        quote_id="diag-704b-001",
        agency="CCHCS",
        facility="CCHCS Stockton",
        solicitation_number="10846581",
        tax_rate_bps=898,
        line_items=items,
    )


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


def dump(label: str, pdf_bytes: bytes) -> None:
    import pypdf
    import pdfplumber

    print(f"\n=== {label} ===")
    print(f"bytes: {len(pdf_bytes)}")
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    fields = reader.get_fields() or {}
    print(f"field count: {len(fields)}")
    sample_names = [
        "COMPANY NAME",
        "DEPARTMENT",
        "SOLICITATION",
        "REQUESTOR",
        "ITEM DESCRIPTION PRODUCT SPECIFICATIONRow1",
        "QTYRow1",
        "PRICE PER UNITRow1",
        "SUBTOTALRow1",
        "ITEM DESCRIPTION PRODUCT SPECIFICATIONRow5",
    ]
    for name in sample_names:
        f = fields.get(name)
        v = f.get("/V") if f else "FIELD MISSING"
        print(f"  {name!r}: {v!r}")
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            print(f"page {i+1} chars: {len(text)}")
            for token in ("ZZUNIQUE001", "ZZUNIQUE005", "ZZUNIQUE008", "10846581"):
                marker = "+" if token in text else "-"
                print(f"  {marker} {token}")


def main():
    quote = build_quote(8)
    today = datetime(2026, 5, 16, 12, 0, 0)

    # 1. Build pre-pikepdf bytes (just pypdf writes).
    import pypdf
    reader = pypdf.PdfReader(str(cchcs_704b._BLANK_TEMPLATE))
    writer = pypdf.PdfWriter(clone_from=reader)
    field_values = cchcs_704b._field_map(quote, identity(), today)
    print(f"\nfield_values to write: {len(field_values)}")
    print(f"  sample: ITEM DESCRIPTION PRODUCT SPECIFICATIONRow1 -> "
          f"{field_values.get('ITEM DESCRIPTION PRODUCT SPECIFICATIONRow1')!r}")
    print(f"  sample: ITEM DESCRIPTION PRODUCT SPECIFICATIONRow5 -> "
          f"{field_values.get('ITEM DESCRIPTION PRODUCT SPECIFICATIONRow5')!r}")
    for page in writer.pages:
        writer.update_page_form_field_values(page, field_values, auto_regenerate=True)
    buf = io.BytesIO()
    writer.write(buf)
    pypdf_only = buf.getvalue()
    dump("pypdf-only (no pikepdf)", pypdf_only)

    # 2. After pikepdf generate_appearance_streams, no flatten.
    import pikepdf
    with pikepdf.open(io.BytesIO(pypdf_only)) as pdf:
        pdf.generate_appearance_streams()
        out = io.BytesIO()
        pdf.save(out)
        pikepdf_no_flatten = out.getvalue()
    dump("pypdf + pikepdf appearance (no flatten)", pikepdf_no_flatten)

    # 3. After pikepdf flatten_annotations.
    with pikepdf.open(io.BytesIO(pypdf_only)) as pdf:
        pdf.generate_appearance_streams()
        pdf.flatten_annotations(mode="all")
        out = io.BytesIO()
        pdf.save(out)
        pikepdf_flat = out.getvalue()
    dump("pypdf + pikepdf appearance + flatten", pikepdf_flat)

    # Save all 3 for visual inspection.
    out_dir = Path(__file__).resolve().parents[1] / "_diag" / "704b"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "01_pypdf_only.pdf").write_bytes(pypdf_only)
    (out_dir / "02_pikepdf_appearance.pdf").write_bytes(pikepdf_no_flatten)
    (out_dir / "03_pikepdf_flat.pdf").write_bytes(pikepdf_flat)
    print(f"\nWrote: {out_dir}")


if __name__ == "__main__":
    main()
