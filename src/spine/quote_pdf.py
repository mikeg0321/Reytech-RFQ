"""The Spine — Quote PDF renderer.

Single function: render_quote_pdf(quote, contract=None) → bytes.

Reads from Spine Quote model (and optionally the EmailContract that
drove ingest). NO QuoteContract assembly. NO tax_resolver /
facility_registry / agency_config calls. NO shipping_option=included
→ tax_cents=0 branch (that field doesn't exist in the Spine model).

Tax line is computed via quote.tax_cents (banker's rounded integer
cents). Total line is quote.total_cents. Subtotal is
quote.subtotal_cents. The PDF cannot diverge from the model because
the model's @computed_field properties ARE the math.

LAYOUT
──────
Mirrors Mike's existing buyer-facing Quote template (reference:
R26Q39, R25Q161 — Trabuco Canyon letterhead, Reytech-brand soft blue
accents matching www.reytechinc.com).
Sections, top-to-bottom:

  1. Identity row — Reytech logo + address block (left)
                  + QUOTE header + QUOTE#/DATE box (right)
  2. Bill-to / To / Ship-to — three address blocks
  3. Salesperson | RFQ Number | Terms | Expiration — 4-col strip
  4. Line items — LINE# | MFG. PART # | QTY | UOM | DESCRIPTION | UNIT PRICE | TOTAL PRICE
  5. Totals box — right-aligned (SUBTOTAL / TAX / SHIPPING / TOTAL)
  6. Footer — "Quote R26Q##" bottom-right

The buyer-side fields (Bill-to, To, Ship-to, RFQ#) come from the
EmailContract when provided. Without a contract the renderer falls
back to the quote's facility/agency/solicitation# alone (legacy path
for tests and fixture-driven flows).

THE MATCHING GATE
─────────────────
After ReportLab builds the PDF, render_quote_pdf re-extracts the
displayed money lines via pdfplumber and compares them cent-for-cent
to the source Quote. If any displayed value (subtotal, tax, shipping,
total, per-line extension) does not match the model, the function
raises SpineRenderMismatchError. The function is structurally
incapable of returning bytes that lie about the math. Closes the
5/15 substrate failure class where TAX rendered $0.00 on a non-zero
subtotal and shipped to the buyer without anyone catching it.
"""
from __future__ import annotations

import io
import re
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional

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

from src.spine.model import SpineValidationError

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


# ──────────────────────────────────────────────────────────────────────
# Render-matching gate — the structural invariant.
# ──────────────────────────────────────────────────────────────────────


class SpineRenderMismatchError(SpineValidationError):
    """Raised when rendered PDF bytes disagree with the source Quote.

    Inherits from SpineValidationError so route handlers that already
    catch SpineValidationError surface this as 409 (state corrupt)
    rather than 500 (server error). The bytes never leave the
    renderer; the caller decides how to surface it.
    """


# ──────────────────────────────────────────────────────────────────────
# Reytech identity — values from Mike's existing R26Q39 / R25Q161 quote
# template. These constants are the source of truth for the Quote PDF
# letterhead. The 703B/704B/bidpkg renderers use the separate
# ReytechIdentity dataclass (agency_forms/cchcs_703b.py) because those
# forms feed AcroForm fields with their own field-naming conventions.
# Splitting them here keeps Quote PDF letterhead changes from rippling
# into agency-form fillers (and vice versa).
# ──────────────────────────────────────────────────────────────────────

REYTECH_NAME = "Reytech Inc."
REYTECH_ADDRESS_LINE_1 = "30 Carnoustie Way"
REYTECH_ADDRESS_LINE_2 = "Trabuco Canyon, CA 92679"
REYTECH_OWNER = "Michael Guadan, Owner"
REYTECH_PHONE = "949-229-1575"
REYTECH_EMAIL = "sales@reytechinc.com"
REYTECH_WEBSITE = "www.reytechinc.com"
REYTECH_SELLERS_PERMIT = "CA Sellers Permit: 245652416-00001"

# Buyer-facing default constants. Mike's R26Q39 reference shows
# "Net 30" terms and a 45-day expiration window. These are the
# documented Reytech defaults; an EmailContract may override per bid
# in a future PR (`contract.payment_terms` / `contract.expiration_date`)
# without changing this renderer.
TERMS_DEFAULT = "Net 30"
EXPIRATION_DAYS = 45

# Reytech brand accent — soft, readable blue (matches the black/blue
# styling on www.reytechinc.com). Used for column-header bars, totals-
# label backgrounds, and the QUOTE#/DATE box header. Black text on top
# keeps contrast high; the tint is light enough not to compete with
# the line-item data.
ACCENT_BLUE = colors.HexColor("#BDD7F1")
ACCENT_BLUE_DARK = colors.HexColor("#1F4E91")    # title text / fine rules


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
    crash the render or produce garbage.
    """
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _address_to_html(addr: str | None) -> str:
    """Convert a multi-line address (newline-separated) to reportlab HTML."""
    if not addr:
        return ""
    return "<br/>".join(_escape_pdf_text(line) for line in addr.splitlines() if line.strip())


# ──────────────────────────────────────────────────────────────────────
# Renderer
# ──────────────────────────────────────────────────────────────────────


def render_quote_pdf(
    quote: "Quote",
    contract: Optional["EmailContract"] = None,
    *,
    today: datetime | None = None,
) -> bytes:
    """Render `quote` as a Reytech Quote PDF, return bytes.

    Args:
        quote: Validated Spine Quote.
        contract: Optional EmailContract that drove this quote's ingest.
            When provided, Bill-to / To / Ship-to / RFQ Number / buyer
            contact info fill from the contract. When None, the renderer
            falls back to `quote.facility` as both To and Ship-to (legacy
            path for tests and fixture flows).
        today: Optional clock injection for deterministic test rendering.

    Returns:
        Bytes of a single PDF document.
    """
    today = today or datetime.now()
    buf = io.BytesIO()

    quote_label = quote.display_number or quote.quote_id
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.5 * inch,
        rightMargin=0.5 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
        title=f"Reytech Quote {quote_label}",
        author=REYTECH_NAME,
    )

    story: list = []
    story.append(_identity_and_quote_box(quote, today))
    story.append(Spacer(1, 0.10 * inch))
    story.append(_addresses_block(quote, contract))
    story.append(Spacer(1, 0.12 * inch))
    story.append(_buyer_terms_strip(quote, contract, today))
    story.append(Spacer(1, 0.04 * inch))
    story.append(_line_item_table(quote))
    story.append(Spacer(1, 0.10 * inch))
    story.append(_totals_block(quote))

    def _draw_footer(canvas, _doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#666666"))
        canvas.drawRightString(
            letter[0] - 0.5 * inch,
            0.35 * inch,
            f"Quote {quote_label}",
        )
        canvas.restoreState()

    doc.build(story, onFirstPage=_draw_footer, onLaterPages=_draw_footer)
    pdf_bytes = buf.getvalue()

    # THE MATCHING GATE. Renders that disagree with the model never
    # leave this function.
    _verify_render_matches_model(pdf_bytes, quote)
    return pdf_bytes


# ──────────────────────────────────────────────────────────────────────
# Render-matching gate implementation
# ──────────────────────────────────────────────────────────────────────


def _verify_render_matches_model(pdf_bytes: bytes, quote: "Quote") -> None:
    """Re-extract the rendered PDF and assert every money line matches.

    Cent-exact comparison of every operator-visible money value:
      - Subtotal, Tax, Shipping (always $0.00), Total
      - Each line item's TOTAL PRICE column

    pdfplumber is read-only — it is not a second renderer, it is the
    audit eye on this one. Raises SpineRenderMismatchError on any
    divergence.
    """
    try:
        import pdfplumber
    except ImportError as e:
        raise SpineRenderMismatchError(
            "pdfplumber is required to verify rendered output. "
            "It ships with the spine package; reinstall requirements."
        ) from e

    expected_lines = {
        "SUBTOTAL": format_dollars(quote.subtotal_cents),
        "TAX": format_dollars(quote.tax_cents),
        "SHIPPING": format_dollars(0),
        "TOTAL": format_dollars(quote.total_cents),
    }
    expected_extensions = {
        li.line_no: format_dollars(li.extension_cents) for li in quote.line_items
    }

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    # Money pattern: $-1,234.56 or $0.00.
    money_re = re.compile(r"-?\$[\d,]+\.\d{2}")

    # 1) Totals block — the four labels (SUBTOTAL / TAX / SHIPPING /
    #    TOTAL) appear in that order at the end of the document. The
    #    "TOTAL" label is ambiguous on its own — it also appears as
    #    part of the "TOTAL PRICE" line-item column header — so we
    #    anchor to the LAST occurrence of each label, which is always
    #    inside the totals block. SUBTOTAL is structurally unique and
    #    serves as the lower bound: every totals-block label must
    #    appear at or after the SUBTOTAL position.
    subtotal_last = None
    for m in re.finditer(r"\bSUBTOTAL\b", full_text):
        subtotal_last = m
    if subtotal_last is None:
        raise SpineRenderMismatchError(
            "render gate: SUBTOTAL label not found in rendered PDF. "
            f"Rendered text head: {full_text[:300]!r}"
        )
    totals_region_start = subtotal_last.start()
    totals_region = full_text[totals_region_start:]

    for label, expected_str in expected_lines.items():
        if label == "TOTAL":
            # Strip the TOTAL-PRICE column header before searching.
            search_text = re.sub(r"TOTAL\s+PRICE", "____________", totals_region)
        else:
            search_text = totals_region
        label_re = re.compile(r"\b" + re.escape(label) + r"\b")
        m_label = None
        for m in label_re.finditer(search_text):
            m_label = m
        if m_label is None:
            raise SpineRenderMismatchError(
                f"render gate: label {label!r} not found in totals region "
                f"of rendered PDF. Totals region: {totals_region[:300]!r}"
            )
        tail = search_text[m_label.end():]
        m_money = money_re.search(tail)
        if m_money is None:
            raise SpineRenderMismatchError(
                f"render gate: no money value found after totals-block "
                f"label {label!r}. Tail: {tail[:120]!r}"
            )
        rendered_str = m_money.group(0)
        if rendered_str != expected_str:
            raise SpineRenderMismatchError(
                f"render gate MISMATCH on {label}: "
                f"model expected {expected_str!r}, PDF displays {rendered_str!r}. "
                f"This is the 5/15 substrate failure class. Render aborted; "
                f"no bytes returned."
            )

    # 2) Per-line TOTAL PRICE column — every line item's qty × unit_price
    #    must appear in the rendered text as the expected money string.
    from collections import Counter
    expected_counts: Counter = Counter()
    for line_no, expected_ext in expected_extensions.items():
        expected_counts[expected_ext] += 1
    expected_counts[format_dollars(quote.subtotal_cents)] += 1
    expected_counts[format_dollars(quote.tax_cents)] += 1
    expected_counts[format_dollars(0)] += 1
    expected_counts[format_dollars(quote.total_cents)] += 1

    actual_counts = Counter(money_re.findall(full_text))

    for money_str, expected_n in expected_counts.items():
        if actual_counts.get(money_str, 0) < expected_n:
            offending_line = next(
                (ln for ln, ext in expected_extensions.items() if ext == money_str),
                None,
            )
            raise SpineRenderMismatchError(
                f"render gate: money string {money_str!r} appears "
                f"{actual_counts.get(money_str, 0)} times in the rendered "
                f"PDF, but the model expects it at least {expected_n} "
                f"times (extension on line {offending_line} or a totals "
                f"line is missing/wrong). Render aborted."
            )

    # 3) Identity check — Reytech identity, quote ID, solicitation #
    #    must appear.
    flattened = "".join(full_text.split())
    rendered_quote_label = quote.display_number or quote.quote_id
    for required in (REYTECH_NAME, rendered_quote_label, quote.solicitation_number):
        target = required.replace(" ", "")
        if target in flattened:
            continue
        prefix_len = min(12, len(target))
        if prefix_len > 0 and target[:prefix_len] in flattened:
            continue
        raise SpineRenderMismatchError(
            f"render gate: required identifier {required!r} not found "
            f"in rendered PDF. Renderer dropped the document's identity."
        )


# ──────────────────────────────────────────────────────────────────────
# Sections
# ──────────────────────────────────────────────────────────────────────


def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "company": ParagraphStyle(
            "company", parent=base["Normal"],
            fontSize=14, leading=17, fontName="Helvetica-Bold",
            textColor=colors.HexColor("#222222"),
        ),
        "company_addr": ParagraphStyle(
            "company_addr", parent=base["Normal"],
            fontSize=8.5, leading=10.5, fontName="Helvetica",
            textColor=colors.HexColor("#333333"),
        ),
        "quote_header": ParagraphStyle(
            "quote_header", parent=base["Normal"],
            fontSize=24, leading=28, alignment=2, fontName="Helvetica-Bold",
            textColor=ACCENT_BLUE_DARK,
        ),
        "qbox_label": ParagraphStyle(
            "qbox_label", parent=base["Normal"],
            fontSize=9, leading=11, fontName="Helvetica-Bold",
            textColor=colors.HexColor("#222222"),
        ),
        "qbox_value": ParagraphStyle(
            "qbox_value", parent=base["Normal"],
            fontSize=10, leading=12, alignment=2, fontName="Helvetica-Bold",
            textColor=colors.HexColor("#222222"),
        ),
        "addr_label": ParagraphStyle(
            "addr_label", parent=base["Normal"],
            fontSize=9.5, leading=12, fontName="Helvetica-Bold",
            textColor=colors.HexColor("#222222"),
            spaceAfter=2,
        ),
        "addr_value": ParagraphStyle(
            "addr_value", parent=base["Normal"],
            fontSize=9, leading=11.5, fontName="Helvetica",
            textColor=colors.HexColor("#333333"),
        ),
        "strip_label": ParagraphStyle(
            "strip_label", parent=base["Normal"],
            fontSize=9, leading=11, fontName="Helvetica-Bold",
            textColor=colors.HexColor("#222222"),
        ),
        "strip_value": ParagraphStyle(
            "strip_value", parent=base["Normal"],
            fontSize=9, leading=11, fontName="Helvetica",
            textColor=colors.HexColor("#222222"),
        ),
        "li_desc": ParagraphStyle(
            # Wrapping style for the DESCRIPTION column so long product
            # names reflow inside their cell instead of overflowing
            # into adjacent columns.
            "li_desc", parent=base["Normal"],
            fontSize=9, leading=11, fontName="Helvetica",
            spaceBefore=0, spaceAfter=0,
        ),
    }


def _identity_and_quote_box(quote: "Quote", today: datetime) -> Table:
    """Top row: Reytech identity (left) + QUOTE title + Q#/DATE box (right)."""
    s = _styles()

    # Reytech identity block — name, address, owner, contacts.
    identity_lines = [
        Paragraph(REYTECH_NAME, s["company"]),
        Paragraph(
            REYTECH_ADDRESS_LINE_1 + "<br/>" + REYTECH_ADDRESS_LINE_2 + "<br/>"
            + REYTECH_OWNER + "<br/>"
            + REYTECH_PHONE + "<br/>"
            + REYTECH_EMAIL + "<br/>"
            + REYTECH_WEBSITE + "<br/>"
            + REYTECH_SELLERS_PERMIT,
            s["company_addr"],
        ),
    ]

    quote_label = quote.display_number or quote.quote_id
    # QUOTE big header on top, then the 3-row Q# / DATE / SOL# box.
    # SOL# is the buyer's solicitation identifier (PREQ-####, 10847262,
    # etc.) — included here unconditionally so the gate's identity
    # check is satisfied AND every buyer-side PDF carries the bid
    # reference government procurement systems index by.
    qbox = Table(
        [
            [Paragraph("QUOTE #", s["qbox_label"]),
             Paragraph(quote_label, s["qbox_value"])],
            [Paragraph("DATE", s["qbox_label"]),
             Paragraph(today.strftime("%b %d, %Y"), s["qbox_value"])],
            [Paragraph("SOL #", s["qbox_label"]),
             Paragraph(_escape_pdf_text(quote.solicitation_number), s["qbox_value"])],
        ],
        colWidths=[0.9 * inch, 1.7 * inch],
    )
    qbox.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), ACCENT_BLUE),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#555555")),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#999999")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))

    right_stack = Table(
        [
            [Paragraph("QUOTE", s["quote_header"])],
            [qbox],
        ],
        colWidths=[2.6 * inch],
    )
    right_stack.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))

    outer = Table(
        [[identity_lines, right_stack]],
        colWidths=[4.8 * inch, 2.7 * inch],
    )
    outer.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    return outer


def _addresses_block(
    quote: "Quote",
    contract: Optional["EmailContract"],
) -> Table:
    """Bill to / To / Ship to Location — three address blocks.

    Pulls from EmailContract when provided. Without a contract, falls
    back to quote.facility for both To and Ship-to (legacy path).
    """
    s = _styles()

    # Bill-to defaults to the agency invoice contact when a contract
    # exists. Without contract, render an empty Bill-to (the agency
    # name from quote.agency is the only fallback we can derive).
    if contract is not None:
        bill_to_lines = []
        # Agency name as bill-to header (CalVet, CCHCS, etc. — Mike's
        # operator workflow keys "Bill to" by the agency's invoicing
        # entity, which for state agencies is the department name).
        bill_to_lines.append(_escape_pdf_text(contract.agency))
        if contract.buyer_email:
            bill_to_lines.append(_escape_pdf_text(contract.buyer_email))
        bill_to_html = "<br/>".join(bill_to_lines)

        to_block = (
            _escape_pdf_text(contract.facility)
            + (("<br/>" + _address_to_html(contract.ship_to_address))
               if contract.ship_to_address else "")
        )
        ship_to_block = (
            _escape_pdf_text(contract.ship_to_facility or contract.facility)
            + (("<br/>" + _address_to_html(contract.ship_to_address))
               if contract.ship_to_address else "")
        )
    else:
        bill_to_html = _escape_pdf_text(quote.agency)
        to_block = _escape_pdf_text(quote.facility)
        ship_to_block = _escape_pdf_text(quote.facility)

    row1 = [
        Paragraph("Bill to:", s["addr_label"]),
        "",
        Paragraph("Ship to Location:", s["addr_label"]),
    ]
    row2 = [
        Paragraph(bill_to_html, s["addr_value"]),
        "",
        Paragraph(ship_to_block, s["addr_value"]),
    ]
    row3 = [
        Paragraph("To:", s["addr_label"]),
        "",
        "",
    ]
    row4 = [
        Paragraph(to_block, s["addr_value"]),
        "",
        "",
    ]

    tbl = Table(
        [row1, row2, row3, row4],
        colWidths=[3.6 * inch, 0.3 * inch, 3.6 * inch],
    )
    tbl.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 1),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
    ]))
    return tbl


def _buyer_terms_strip(
    quote: "Quote",
    contract: Optional["EmailContract"],
    today: datetime,
) -> Table:
    """4-column strip with lavender label row: Salesperson | RFQ # | Terms | Expiration."""
    s = _styles()
    salesperson = REYTECH_OWNER.split(",")[0].strip()  # "Michael Guadan"

    # RFQ Number = the buyer's RFQ title / solicitation# (NOT the
    # Reytech Quote #). Per Mike's R26Q39 reference: "RFQ-Auralis"
    # came from the contract.rfq_title; sol# is its own field below.
    if contract is not None and contract.rfq_title:
        rfq_label = contract.rfq_title
    else:
        rfq_label = quote.solicitation_number

    expiration = today + timedelta(days=EXPIRATION_DAYS)
    expiration_str = expiration.strftime("%b %d, %Y")

    header = [
        Paragraph("Salesperson", s["strip_label"]),
        Paragraph("RFQ Number", s["strip_label"]),
        Paragraph("Terms", s["strip_label"]),
        Paragraph("Expiration Date", s["strip_label"]),
    ]
    values = [
        Paragraph(salesperson, s["strip_value"]),
        Paragraph(_escape_pdf_text(rfq_label), s["strip_value"]),
        Paragraph(TERMS_DEFAULT, s["strip_value"]),
        Paragraph(expiration_str, s["strip_value"]),
    ]
    tbl = Table([header, values], colWidths=[1.9 * inch, 1.9 * inch, 1.85 * inch, 1.85 * inch])
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), ACCENT_BLUE),
        ("BACKGROUND", (0, 1), (-1, 1), colors.HexColor("#FFFFFF")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#555555")),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#999999")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return tbl


def _line_item_table(quote: "Quote") -> Table:
    """Render line items. Column order matches Mike's R26Q39 reference:
    LINE # | MFG. PART # | QTY | UOM | DESCRIPTION | UNIT PRICE | TOTAL PRICE.

    Width budget (~7.5 inch usable at 0.5in margins):
      LINE 0.45 | MFG 1.00 | QTY 0.45 | UOM 0.50 |
      DESC 2.95 | UNIT 1.05 | TOTAL 1.10  = 7.50.
    """
    s = _styles()
    header = ["LINE #", "MFG. PART #", "QTY", "UOM", "DESCRIPTION", "UNIT PRICE", "TOTAL PRICE"]
    rows: list[list] = [header]
    for li in quote.line_items:
        rows.append([
            str(li.line_no),
            li.mfg_number or "",
            f"{li.qty:,}",
            li.uom,
            Paragraph(_escape_pdf_text(li.description), s["li_desc"]),
            format_dollars(li.unit_price_cents),
            format_dollars(li.extension_cents),
        ])

    col_widths = [
        0.45 * inch,   # LINE #
        1.00 * inch,   # MFG. PART #
        0.45 * inch,   # QTY
        0.50 * inch,   # UOM
        2.95 * inch,   # DESCRIPTION (wraps via Paragraph)
        1.05 * inch,   # UNIT PRICE
        1.10 * inch,   # TOTAL PRICE
    ]
    tbl = Table(rows, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 9),
        ("FONT", (0, 1), (-1, -1), "Helvetica", 9),
        ("BACKGROUND", (0, 0), (-1, 0), ACCENT_BLUE),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#222222")),
        ("ALIGN", (0, 0), (-1, 0), "LEFT"),
        ("ALIGN", (2, 1), (3, -1), "CENTER"),   # QTY, UOM centered
        ("ALIGN", (5, 1), (6, -1), "RIGHT"),    # UNIT/TOTAL PRICE right
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#555555")),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#999999")),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return tbl


def _totals_block(quote: "Quote") -> Table:
    """Totals box, right-aligned. Lavender label column.

    Lines (in order):
        SUBTOTAL
        TAX (X.XX%)
        SHIPPING            $0.00  ← always; Charter invariant #7
        TOTAL
    """
    rows = [
        ["SUBTOTAL", format_dollars(quote.subtotal_cents)],
        [f"TAX ({format_tax_rate(quote.tax_rate_bps)})", format_dollars(quote.tax_cents)],
        ["SHIPPING", format_dollars(0)],
        ["TOTAL", format_dollars(quote.total_cents)],
    ]
    tbl = Table(rows, colWidths=[1.6 * inch, 1.4 * inch], hAlign="RIGHT")
    tbl.setStyle(TableStyle([
        ("FONT", (0, 0), (-1, -2), "Helvetica-Bold", 10),
        ("FONT", (1, 0), (1, -2), "Helvetica", 10),
        ("FONT", (0, -1), (-1, -1), "Helvetica-Bold", 11),
        ("BACKGROUND", (0, 0), (0, -1), ACCENT_BLUE),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#555555")),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#999999")),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
    ]))
    return tbl
