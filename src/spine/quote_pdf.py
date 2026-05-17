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

The renderer is deliberately spartan in v1 (Day-3 gate). Visual polish
(letterhead, branding, watermark) is a separate later PR. What matters
here: the math is bit-exact, the layout is legible, AND the renderer
will not produce bytes that disagree with the model.
"""
from __future__ import annotations

import io
import re
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

from src.spine.model import SpineValidationError

if TYPE_CHECKING:
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

    The presence of this exception in the codebase is part of the
    contract. Removing it without first removing the call to
    _verify_render_matches_model is a substrate regression.
    """


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
        title=f"Reytech Quote {quote.display_number or quote.quote_id}",
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

    doc.build(story)
    pdf_bytes = buf.getvalue()

    # THE MATCHING GATE. Renders that disagree with the model never
    # leave this function. See module docstring for the failure-class
    # this closes.
    _verify_render_matches_model(pdf_bytes, quote)
    return pdf_bytes


# ──────────────────────────────────────────────────────────────────────
# Render-matching gate implementation
# ──────────────────────────────────────────────────────────────────────


def _verify_render_matches_model(pdf_bytes: bytes, quote: "Quote") -> None:
    """Re-extract the rendered PDF and assert every money line matches.

    Cent-exact comparison of every operator-visible money value:
      - Subtotal, Tax, Shipping (always $0.00), Total
      - Each line item's Extension column

    pdfplumber is read-only — it is not a second renderer, it is the
    audit eye on this one. If extraction can't find a value the gate
    is supposed to verify, that is itself a render failure (the cell
    is missing or unparseable in the bytes) and raises.

    Raises SpineRenderMismatchError on any divergence.
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

    # 1) Totals block — each label appears once; the value that
    #    follows it must match the model. Word boundaries on the
    #    label so "TOTAL" doesn't substring-match inside "SUBTOTAL".
    #    Whitespace inside the label is collapsed because pdfplumber
    #    may split "TAX (7.75%)" across the label/value boundary.
    for label, expected_str in expected_lines.items():
        label_re = re.compile(r"\b" + re.escape(label) + r"\b")
        m_label = label_re.search(full_text)
        if m_label is None:
            raise SpineRenderMismatchError(
                f"render gate: label {label!r} not found in rendered PDF text. "
                f"Renderer must emit the {label} line on every quote. "
                f"Rendered text head: {full_text[:300]!r}"
            )
        # Search money tokens after the label match's END (so a label
        # like "SUBTOTAL" doesn't pick up the dollar amount that
        # belongs to a row above the totals block).
        tail = full_text[m_label.end():]
        m_money = money_re.search(tail)
        if m_money is None:
            raise SpineRenderMismatchError(
                f"render gate: no money value found after label {label!r}. "
                f"Tail: {tail[:120]!r}"
            )
        rendered_str = m_money.group(0)
        if rendered_str != expected_str:
            raise SpineRenderMismatchError(
                f"render gate MISMATCH on {label}: "
                f"model expected {expected_str!r}, PDF displays {rendered_str!r}. "
                f"This is the 5/15 substrate failure class. Render aborted; "
                f"no bytes returned."
            )

    # 2) Per-line extension column — every line item's qty × unit_price
    #    must appear in the rendered text as the expected money string.
    #
    #    A bare `in` check would pass even if the renderer wrote the
    #    wrong extension for a row, because the same money string can
    #    legitimately appear elsewhere on the page (e.g., the single-
    #    line-item case where subtotal == extension). The robust check
    #    is a count: build the multiset of money strings the model says
    #    should appear (4 totals + N extensions), then assert the
    #    rendered text contains each money string at least that many
    #    times. A renderer that lies about one extension will reduce
    #    the count of the correct string by 1.
    from collections import Counter
    expected_counts = Counter()
    for line_no, expected_ext in expected_extensions.items():
        expected_counts[expected_ext] += 1
    expected_counts[format_dollars(quote.subtotal_cents)] += 1
    expected_counts[format_dollars(quote.tax_cents)] += 1
    expected_counts[format_dollars(0)] += 1
    expected_counts[format_dollars(quote.total_cents)] += 1

    actual_counts = Counter(money_re.findall(full_text))

    for money_str, expected_n in expected_counts.items():
        if actual_counts.get(money_str, 0) < expected_n:
            # Pin down which line went missing for the operator-facing
            # error message: the first line whose extension equals
            # money_str is the most likely culprit.
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
    #    must appear. Substrate guarantees the operator can identify
    #    the document; renderer that drops the identity is broken.
    #
    #    pdfplumber's text extraction interleaves table columns in
    #    layout order, so a long quote_id that wraps across cells can
    #    end up split (e.g., "rfq_PREQ10846581_tes" followed by other
    #    cells' content followed by trailing "t"). Wrap-tolerant
    #    check: verify the full string OR a meaningfully-long leading
    #    prefix is contiguous in the whitespace-collapsed text. The
    #    prefix is long enough to be uniquely identifying (12 chars
    #    handles all real Reytech ID schemes); accepting a wrap means
    #    we don't false-positive on cosmetic layout, but we still
    #    catch any renderer that drops the identity entirely.
    flattened = "".join(full_text.split())
    # The rendered identifier is display_number when assigned (post-#1040
    # ingest); otherwise the internal quote_id (legacy rows). The gate
    # follows the renderer — whichever the operator+buyer will see is
    # what must be present in the rendered PDF.
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
    # Buyer-facing identifier on top: prefer the substrate-assigned
    # R{yy}Q#### (PR #1040). Falls back to the internal quote_id only
    # for legacy rows that pre-date the sequential-numbering substrate
    # — once those are quoted out the fallback never fires.
    quote_label = quote.display_number or quote.quote_id
    right = [
        [Paragraph("QUOTE NUMBER:", s["meta_label"]),
         Paragraph(quote_label, s["meta_value"])],
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
