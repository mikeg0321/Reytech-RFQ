"""One-shot: render the 9e63456e CCHCS replay fixture to a sample PDF.

Run from worktree root:
    py -3.14 scripts/spine_render_demo.py

Produces _diag/spine_9e63456e_sample.pdf so Mike can eyeball the
layout, fonts, table sizing, and totals block before the operator UI
gets built around it.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Make src/ importable when running as a script from the repo root.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.spine import LineItem, Quote, QuoteStatus, render_quote_pdf  # noqa: E402


def _fresh_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def build_9e63456e_fixture() -> Quote:
    """Same fixture as tests/spine/test_quote_pdf.py::_build_9e63456e_quote.

    7 rows summing to $46,836.20 at 8.25% tax → $50,700.19. Row 6
    (Item 2555) is the real-world row from today's CCHCS manifest;
    other rows are back-solved to hit the manifest sum.
    """
    items = [
        LineItem(
            line_no=1,
            description="GLOVES, EXAM, NITRILE, POWDER-FREE, LARGE, 100/BOX",
            mfg_number="MK-2103L",
            qty=10, uom="BX",
            cost_cents=3500,
            cost_source_url="https://supplier.example.com/sku/MK-2103L",
            cost_validated_at=_fresh_ts(),
            unit_price_cents=5000,
        ),
        LineItem(
            line_no=2,
            description="MARKERS, DRY ERASE, BLACK, FINE TIP, DOZEN",
            mfg_number="EXP-86001",
            qty=25, uom="DZ",
            cost_cents=2200,
            cost_source_url="https://supplier.example.com/sku/EXP-86001",
            cost_validated_at=_fresh_ts(),
            unit_price_cents=3500,
        ),
        LineItem(
            line_no=3,
            description="BOARD, DRY ERASE, 36in × 48in, ALUMINUM FRAME",
            mfg_number="QRT-S537",
            qty=5, uom="EA",
            cost_cents=14500,
            cost_source_url="https://supplier.example.com/sku/QRT-S537",
            cost_validated_at=_fresh_ts(),
            unit_price_cents=18000,
        ),
        LineItem(
            line_no=4,
            description="ENVELOPE, MANILA, 9x12, CLASP, BOX OF 100",
            mfg_number="UNV-35262",
            qty=50, uom="BX",
            cost_cents=500,
            cost_source_url="https://supplier.example.com/sku/UNV-35262",
            cost_validated_at=_fresh_ts(),
            unit_price_cents=750,
        ),
        LineItem(
            line_no=5,
            description="PAPER, COPY, 8.5x11, 20LB, WHITE, REAM",
            mfg_number="HAM-103267",
            qty=20, uom="CS",
            cost_cents=3000,
            cost_source_url="https://supplier.example.com/sku/HAM-103267",
            cost_validated_at=_fresh_ts(),
            unit_price_cents=4500,
        ),
        LineItem(
            line_no=6,
            description="LABELS, BLANK, CIRCLE, 3/4\" DIA, BLUE",
            mfg_number="2555",
            qty=1000, uom="PAC",
            cost_cents=2085,
            cost_source_url="https://supplier.example.com/labels/2555",
            cost_validated_at=_fresh_ts(),
            unit_price_cents=2815,
        ),
        LineItem(
            line_no=7,
            description="STICKERS, REINFORCEMENT, ROUND, BEIGE, 200/PACK",
            mfg_number="AVE-5722",
            qty=540, uom="PAC",
            cost_cents=1850,
            cost_source_url="https://supplier.example.com/sku/AVE-5722",
            cost_validated_at=_fresh_ts(),
            unit_price_cents=2803,
        ),
    ]
    return Quote(
        quote_id="9e63456e-sample",
        agency="CCHCS",
        facility="SATF Corcoran 93212",
        solicitation_number="10847262",
        line_items=items,
        tax_rate_bps=825,
        status=QuoteStatus.PRICED,
    )


def main() -> int:
    quote = build_9e63456e_fixture()
    out_dir = ROOT / "_diag"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "spine_9e63456e_sample.pdf"
    pdf_bytes = render_quote_pdf(quote)
    out_path.write_bytes(pdf_bytes)

    print(f"Wrote sample Spine Quote PDF to:")
    print(f"  {out_path.absolute()}")
    print()
    print(f"Quote totals (from Quote.* computed fields):")
    print(f"  subtotal_cents = {quote.subtotal_cents:>10,}  -> ${quote.subtotal_cents/100:,.2f}")
    print(f"  tax_cents      = {quote.tax_cents:>10,}  -> ${quote.tax_cents/100:,.2f}")
    print(f"  total_cents    = {quote.total_cents:>10,}  -> ${quote.total_cents/100:,.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
