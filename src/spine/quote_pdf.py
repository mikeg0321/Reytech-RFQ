"""The Spine — Quote PDF renderer.

Single function: render_quote_pdf(quote) → bytes.

Reads from Spine Quote model only. NO QuoteContract assembly. NO
tax_resolver / facility_registry / agency_config calls. NO
shipping_option=included → tax_cents=0 branch (that field doesn't
exist in the Spine model).

Tax line is computed via quote.tax_cents (banker's rounded integer
cents). Total line is quote.total_cents. Subtotal is
quote.subtotal_cents. The PDF cannot diverge from the model because
the model's @computed_field properties ARE the math.

The renderer is deliberately spartan in v1 (Day-3 gate). Visual polish
(letterhead, branding, watermark) is a separate later PR. What matters
here: the math is bit-exact, the layout is legible, pdfplumber can
extract every total back out for round-trip verification.
"""
from __future__ import annotations

import io
from datetime import datetime
from typing import TYPE_CHECKING

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

if TYPE_CHECKING:
    from src.spine.model import Quote


# ──────────────────────────────────────────────────────────────────────
# Reytech identity — bare metadata only. Real letterhead/branding is a
# later polish PR; v1 prioritizes correctness over visual identity.
# ──────────────────────────────────────────────────────────────────────

REYTECH_NAME = "Reytech Inc."
REYTECH_EMAIL = "rfq@reytechinc.com"
REYTECH_PHONE = "949-229-1575"


# ──────────────────────────────────────────────────────────────────────
# Money formatting — integer-cents-in, US-formatted-string-out.
# ──────────────────────────────────────────────────────────────────────


def format_dollars(cents: int) -> str:
    """Format integer cents as US dollar string.

    >>> format_dollars(4683620)
    '$46,836.20'
    >>> format_dollars(386399)
    '$3,863.99'
    >>> format_dollars(0)
    '$0.00'
    >>> format_dollars(5)
    '$0.05'
    """
    sign = "-" if cents < 0 else ""
    cents = abs(int(cents))
    whole, frac = divmod(cents, 100)
    return f"{sign}${whole:,}.{frac:02d}"


def format_tax_rate(bps: int) -> str:
    """Format basis points as a percent label.

    >>> format_tax_rate(825)
    '8.25%'
    >>> format_tax_rate(897)
    '8.97%'
    >>> format_tax_rate(1000)
    '10.00%'
    """
    return f"{bps / 100:.2f}%"


def _escape_pdf_text(s: str) -> str:
    """Escape XML-special chars before wrapping in a reportlab Paragraph.

    reportlab.platypus.Paragraph parses its input as inline XML — bare
    `&`, `<`, or `>` in operator-entered descriptions would either
    crash the render or produce garbage. Escape them here so any line
    item description renders safely regardless of content.
    """
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# ──────────────────────────────────────────────────────────────────────
# Renderer
# ──────────────────────────────────────────────────────────────────────


def render_quote_pdf(quote: "Quote", *, today: datetime | None = None) -> bytes:
    """Render `quote` as a Reytech Quote PDF, return bytes.

    The full math comes from the Quote model's computed fields:
        subtotal_cents = sum(line.extension_cents)
        tax_cents      = banker's-rounded (subtotal * tax_rate_bps / 10000)
        total_cents    = subtotal + tax  (shipping is the constant $0.00)

    Args:
        quote: Validated Spine Quote.
        today: Optional clock injection for deterministic test rendering.

    Returns:
        Bytes of a single PDF document. Caller decides whether to write
        to disk, attach to email, or stream to an HTTP response.
    """
    today = today or datetime.now()
    buf = io.BytesIO()

    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.6 * inch,
        title=f"Reytech Quote {quote.quote_id}",
        author=REYTECH_NAME,
    )

    story: list = []
    story.extend(_header(today, quote))
    story.append(Spacer(1, 0.18 * inch))
    story.extend(_quote_meta(quote, today))
    story.append(Spacer(1, 0.14 * inch))
    story.append(_line_item_table(quote))
    story.append(Spacer(1, 0.18 * inch))
    story.append(_totals_block(quote))
    story.append(Spacer(1, 0.30 * inch))
    story.extend(_footer())

    doc.build(buf_drawn_objects := story)
    return buf.getvalue()


# ──────────────────────────────────────────────────────────────────────
# Sections — each returns a list of Platypus flowables.
# ──────────────────────────────────────────────────────────────────────


def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "company": ParagraphStyle(
            "company", parent=base["Normal"],
            fontSize=16, leading=20, alignment=1, spaceAfter=2,
            fontName="Helvetica-Bold",
        ),
        "contact": ParagraphStyle(
            "contact", parent=base["Normal"],
            fontSize=9, leading=11, alignment=1, textColor=colors.HexColor("#444444"),
        ),
        "h1": ParagraphStyle(
            "h1", parent=base["Normal"],
            fontSize=18, leading=22, alignment=1, spaceBefore=4, spaceAfter=4,
            fontName="Helvetica-Bold",
        ),
        "meta_label": ParagraphStyle(
            "meta_label", parent=base["Normal"],
            fontSize=9, leading=11, fontName="Helvetica-Bold",
            textColor=colors.HexColor("#222222"),
        ),
        "meta_value": ParagraphStyle(
            "meta_value", parent=base["Normal"],
            fontSize=10, leading=12, fontName="Helvetica",
        ),
        "footer": ParagraphStyle(
            "footer", parent=base["Normal"],
            fontSize=8, leading=10, textColor=colors.HexColor("#666666"),
        ),
        "li_desc": ParagraphStyle(
            # Wrapping style for the line-item description column so
            # long descriptions reflow inside their cell instead of
            # overflowing into the QTY column. Closes the
            # text-width-overflow class from memory.
            "li_desc", parent=base["Normal"],
            fontSize=9, leading=11, fontName="Helvetica",
            spaceBefore=0, spaceAfter=0,
        ),
    }


def _header(today: datetime, quote: "Quote") -> list:
    s = _styles()
    return [
        Paragraph(REYTECH_NAME, s["company"]),
        Paragraph(f"{REYTECH_EMAIL} &nbsp;&nbsp;|&nbsp;&nbsp; {REYTECH_PHONE}", s["contact"]),
        Spacer(1, 0.10 * inch),
        Paragraph("QUOTE", s["h1"]),
    ]


def _quote_meta(quote: "Quote", today: datetime) -> list:
    s = _styles()
    # Left: TO + solicitation. Right: Quote ID + date.
    left = [
        [Paragraph("TO:", s["meta_label"]),
         Paragraph(f"{quote.facility}<br/>Agency: {quote.agency}", s["meta_value"])],
        [Paragraph("SOLICITATION:", s["meta_label"]),
         Paragraph(quote.solicitation_number, s["meta_value"])],
    ]
    right = [
        [Paragraph("QUOTE ID:", s["meta_label"]),
         Paragraph(quote.quote_id, s["meta_value"])],
        [Paragraph("DATE:", s["meta_label"]),
         Paragraph(today.strftime("%Y-%m-%d"), s["meta_value"])],
    ]
    meta = Table(
        [[Table(left, colWidths=[1.1 * inch, 2.7 * inch]),
          Table(right, colWidths=[0.9 * inch, 1.7 * inch])]],
        colWidths=[4.0 * inch, 3.0 * inch],
    )
    meta.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))
    return [meta]


def _line_item_table(quote: "Quote") -> Table:
    """Render line items. Columns chosen for legibility + extraction.

    Width budget (~7.0 inch usable): Line 0.40 | MFG 0.95 | Desc 2.85 |
    Qty 0.55 | UOM 0.45 | Unit 0.90 | Ext 0.90 = 7.00.

    Description is wrapped in a Paragraph so long product names
    reflow inside the cell instead of overflowing into the QTY
    column. The qty column was widened from 0.45 → 0.55 inch to fit
    comma-grouped values like "1,000" at 9pt Helvetica without
    crowding.
    """
    s = _styles()
    header = ["#", "MFG #", "DESCRIPTION", "QTY", "UOM", "UNIT PRICE", "EXTENSION"]
    rows: list[list] = [header]
    for li in quote.line_items:
        rows.append([
            str(li.line_no),
            li.mfg_number or "",
            Paragraph(_escape_pdf_text(li.description), s["li_desc"]),
            f"{li.qty:,}",
            li.uom,
            format_dollars(li.unit_price_cents),
            format_dollars(li.extension_cents),
        ])

    col_widths = [
        0.40 * inch,   # #
        0.95 * inch,   # MFG #
        2.85 * inch,   # DESCRIPTION (wraps via Paragraph)
        0.55 * inch,   # QTY
        0.45 * inch,   # UOM
        0.90 * inch,   # UNIT PRICE
        0.90 * inch,   # EXTENSION
    ]
    tbl = Table(rows, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 9),
        ("FONT", (0, 1), (-1, -1), "Helvetica", 9),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#222222")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("ALIGN", (3, 1), (4, -1), "CENTER"),   # qty, uom centered
        ("ALIGN", (5, 1), (6, -1), "RIGHT"),    # unit price, extension right
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LINEBELOW", (0, 0), (-1, 0), 0.6, colors.HexColor("#222222")),
        ("LINEBELOW", (0, "splitfirst"), (-1, -1), 0.25, colors.HexColor("#cccccc")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#f7f7f7")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return tbl


def _totals_block(quote: "Quote") -> Table:
    """Totals box, right-aligned. Pulls every value from quote.* fields.

    Lines (in order):
        SUBTOTAL
        TAX (X.XX%)
        SHIPPING            $0.00     ← always; Charter invariant #7
        TOTAL

    The SHIPPING line is always rendered with $0.00. There is no
    shipping field in the Quote model — the literal is in the
    template, not derived from data. This means there is no path for
    a future bug to produce a non-zero shipping line.
    """
    rows = [
        ["SUBTOTAL", format_dollars(quote.subtotal_cents)],
        [f"TAX ({format_tax_rate(quote.tax_rate_bps)})", format_dollars(quote.tax_cents)],
        ["SHIPPING", format_dollars(0)],
        ["TOTAL", format_dollars(quote.total_cents)],
    ]
    tbl = Table(rows, colWidths=[1.6 * inch, 1.4 * inch], hAlign="RIGHT")
    tbl.setStyle(TableStyle([
        ("FONT", (0, 0), (-1, -2), "Helvetica", 10),
        ("FONT", (0, -1), (-1, -1), "Helvetica-Bold", 11),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
        ("LINEABOVE", (0, -1), (-1, -1), 0.8, colors.HexColor("#222222")),
        ("LINEBELOW", (0, -1), (-1, -1), 1.6, colors.HexColor("#222222")),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
    ]))
    return tbl


def _footer() -> list:
    s = _styles()
    return [
        Paragraph(
            "Prices firm 30 days unless otherwise stated. "
            "Reytech Inc. is a California Small Business / DVBE supplier. "
            "Tax computed per CDTFA-published rate for the ship-to jurisdiction.",
            s["footer"],
        ),
    ]
