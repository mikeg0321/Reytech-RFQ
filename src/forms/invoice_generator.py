"""
Reytech Invoice PDF Generator
================================
Branded invoice PDFs from draft_invoice data.
Matches quote PDF styling for consistent brand experience.

Usage:
    from src.forms.invoice_generator import generate_invoice_pdf
    path = generate_invoice_pdf(order)  # order dict with draft_invoice key
"""

import os
import logging
from datetime import datetime
from typing import Optional

from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import Color, HexColor
from reportlab.lib.utils import simpleSplit, ImageReader
from reportlab.pdfgen import canvas

log = logging.getLogger("invoice_gen")

try:
    from src.core.paths import DATA_DIR, OUTPUT_DIR
except ImportError:
    _root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.path.join(_root, "data")
    OUTPUT_DIR = os.path.join(_root, "output")

# ── Colors (match quote PDF branding) ──
FILL    = Color(0.765, 0.765, 0.882)   # #C3C3E0 lavender
LBL_BD  = Color(0.278, 0.278, 0.553)   # #46468D
VAL_BD  = Color(0.0, 0.251, 0.502)     # #004080
TBL_BD  = Color(0.278, 0.278, 0.553)
BLACK   = HexColor("#000000")
WHITE   = HexColor("#FFFFFF")
GRAY    = HexColor("#555555")
ALT_ROW = Color(0.96, 0.96, 0.98)
GREEN   = HexColor("#238636")
NAVY    = HexColor("#1a2744")

REYTECH = {
    "name":     "Reytech Inc.",
    "line1":    "30 Carnoustie Way",
    "line2":    "Trabuco Canyon, CA 92679",
    "contact":  "Michael Guadan",
    "title":    "Owner",
    "phone":    "949-229-1575",
    "email":    "sales@reytechinc.com",
    "cert":     "SB/DVBE Cert #2002605",
    "web":      "reytechinc.com",
}

PAGE_W, PAGE_H = letter  # 612 x 792
MARGIN_L = 36
MARGIN_R = 36
MARGIN_T = 36
MARGIN_B = 50
CONTENT_W = PAGE_W - MARGIN_L - MARGIN_R


def _find_logo():
    """Find Reytech logo in data directory."""
    for name in ("reytech_logo.png", "logo.png", "reytech_logo.jpg", "logo.jpg"):
        p = os.path.join(DATA_DIR, name)
        if os.path.exists(p):
            return p
    return None


def _draw_header(c, inv, page_num=1, total_pages=1):
    """Draw invoice header with logo, company info, and invoice details."""
    y = PAGE_H - MARGIN_T

    # Logo
    logo = _find_logo()
    if logo:
        try:
            img = ImageReader(logo)
            c.drawImage(img, MARGIN_L, y - 60, width=120, height=60,
                        preserveAspectRatio=True, mask="auto")
        except Exception:
            c.setFont("Helvetica-Bold", 16)
            c.setFillColor(NAVY)
            c.drawString(MARGIN_L, y - 18, REYTECH["name"])
    else:
        c.setFont("Helvetica-Bold", 16)
        c.setFillColor(NAVY)
        c.drawString(MARGIN_L, y - 18, REYTECH["name"])

    # Company info (right side)
    c.setFont("Helvetica", 8)
    c.setFillColor(GRAY)
    rx = PAGE_W - MARGIN_R
    c.drawRightString(rx, y - 10, REYTECH["line1"])
    c.drawRightString(rx, y - 20, REYTECH["line2"])
    c.drawRightString(rx, y - 30, f"{REYTECH['phone']} | {REYTECH['email']}")
    c.drawRightString(rx, y - 40, REYTECH["cert"])

    # "INVOICE" title
    y -= 75
    c.setFont("Helvetica-Bold", 22)
    c.setFillColor(BLACK)
    c.drawString(MARGIN_L, y, "INVOICE")
    
    if total_pages > 1:
        c.setFont("Helvetica", 9)
        c.setFillColor(GRAY)
        c.drawRightString(rx, y + 4, f"Page {page_num} of {total_pages}")

    y -= 20

    # Invoice details box
    box_y = y - 60
    c.setStrokeColor(LBL_BD)
    c.setLineWidth(0.5)

    # Left column: Bill To
    c.setFont("Helvetica-Bold", 9)
    c.setFillColor(FILL)
    c.rect(MARGIN_L, box_y, 260, 58, fill=1, stroke=1)
    c.setFillColor(BLACK)
    c.drawString(MARGIN_L + 6, box_y + 44, "BILL TO:")
    c.setFont("Helvetica", 9)
    bill_name = inv.get("bill_to_name", "")
    c.drawString(MARGIN_L + 6, box_y + 30, bill_name[:50])
    bill_email = inv.get("bill_to_email", "")
    if bill_email:
        c.drawString(MARGIN_L + 6, box_y + 18, bill_email[:50])
    ship_name = inv.get("ship_to_name", "")
    if ship_name and ship_name != bill_name:
        c.setFont("Helvetica", 8)
        c.setFillColor(GRAY)
        c.drawString(MARGIN_L + 6, box_y + 6, f"Ship to: {ship_name[:45]}")

    # Right column: Invoice metadata
    meta_x = MARGIN_L + 280
    meta_w = CONTENT_W - 280
    c.setFillColor(FILL)
    c.rect(meta_x, box_y, meta_w, 58, fill=1, stroke=1)
    c.setFillColor(BLACK)

    inv_num = inv.get("invoice_number", "")
    po_num = inv.get("po_number", "")
    qn = inv.get("quote_number", "")
    terms = inv.get("terms", "Net 45")
    inv_date = inv.get("created_at", "")[:10] or datetime.now().strftime("%Y-%m-%d")

    c.setFont("Helvetica-Bold", 9)
    c.drawString(meta_x + 6, box_y + 44, f"Invoice #: {inv_num}")
    c.setFont("Helvetica", 9)
    c.drawString(meta_x + 6, box_y + 32, f"Date: {inv_date}")
    c.drawString(meta_x + 6, box_y + 20, f"PO #: {po_num}")
    if qn:
        c.drawString(meta_x + 6, box_y + 8, f"Quote: {qn}  |  Terms: {terms}")
    else:
        c.drawString(meta_x + 6, box_y + 8, f"Terms: {terms}")

    return box_y - 15  # Return Y position for table start


def _draw_table_header(c, y):
    """Draw the line items table header."""
    row_h = 18
    c.setFillColor(Color(0.17, 0.17, 0.35))  # Dark navy header
    c.rect(MARGIN_L, y - row_h, CONTENT_W, row_h, fill=1, stroke=0)

    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 8)
    
    cols = [
        (MARGIN_L + 4, "LINE"),
        (MARGIN_L + 36, "DESCRIPTION"),
        (MARGIN_L + 340, "QTY"),
        (MARGIN_L + 390, "UNIT PRICE"),
        (MARGIN_L + 470, "EXTENDED"),
    ]
    for cx, label in cols:
        c.drawString(cx, y - 13, label)

    return y - row_h


def _draw_line_item(c, y, idx, item, row_h=16):
    """Draw a single line item row."""
    # Alternate row background
    if idx % 2 == 1:
        c.setFillColor(ALT_ROW)
        c.rect(MARGIN_L, y - row_h, CONTENT_W, row_h, fill=1, stroke=0)

    c.setFillColor(BLACK)
    c.setFont("Helvetica", 8)

    desc = item.get("description", "") or item.get("name", "")
    pn = item.get("part_number", "")
    qty = item.get("qty", 0) or item.get("quantity", 0)
    up = item.get("unit_price", 0) or item.get("price", 0)
    ext = item.get("extended", 0) or round(qty * up, 2)

    # Line number
    c.drawString(MARGIN_L + 8, y - 12, str(idx + 1))

    # Description (may wrap)
    desc_text = desc[:65]
    if pn:
        desc_text += f"  [{pn}]"
    lines = simpleSplit(desc_text, "Helvetica", 8, 295)
    for li, line in enumerate(lines[:2]):
        c.drawString(MARGIN_L + 36, y - 12 - (li * 10), line)

    # Qty
    c.drawRightString(MARGIN_L + 370, y - 12, str(int(qty)) if qty == int(qty) else f"{qty:.1f}")

    # Unit price
    c.drawRightString(MARGIN_L + 450, y - 12, f"${up:,.2f}")

    # Extended
    c.setFont("Helvetica-Bold", 8)
    c.drawRightString(MARGIN_L + CONTENT_W - 8, y - 12, f"${ext:,.2f}")

    actual_h = max(row_h, len(lines) * 10 + 8)
    return y - actual_h


def _draw_totals(c, y, inv):
    """Draw subtotal, tax, and total at the bottom."""
    subtotal = inv.get("subtotal", 0)
    tax = inv.get("tax", 0)
    total = inv.get("total", 0)
    tax_rate = inv.get("tax_rate", 0)

    x_label = MARGIN_L + 360
    x_val = MARGIN_L + CONTENT_W - 8

    y -= 8
    c.setStrokeColor(TBL_BD)
    c.setLineWidth(0.5)
    c.line(MARGIN_L + 340, y + 2, MARGIN_L + CONTENT_W, y + 2)

    c.setFont("Helvetica", 9)
    c.setFillColor(BLACK)
    c.drawRightString(x_label, y - 12, "Subtotal:")
    c.drawRightString(x_val, y - 12, f"${subtotal:,.2f}")

    if tax > 0:
        y -= 16
        rate_str = f" ({tax_rate:.2f}%)" if tax_rate else ""
        c.drawRightString(x_label, y - 12, f"Tax{rate_str}:")
        c.drawRightString(x_val, y - 12, f"${tax:,.2f}")

    y -= 20
    c.setFillColor(Color(0.17, 0.17, 0.35))
    c.rect(MARGIN_L + 340, y - 18, CONTENT_W - 340, 22, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 11)
    c.drawRightString(x_label, y - 12, "TOTAL DUE:")
    c.drawRightString(x_val, y - 12, f"${total:,.2f}")

    return y - 30


def _draw_footer(c, inv):
    """Draw footer with payment info and certification."""
    y = MARGIN_B + 40
    c.setFont("Helvetica", 7.5)
    c.setFillColor(GRAY)

    terms = inv.get("terms", "Net 45")
    po = inv.get("po_number", "")

    c.drawString(MARGIN_L, y, f"Payment Terms: {terms}")
    if po:
        c.drawString(MARGIN_L, y - 10, f"Reference PO: {po}")
    c.drawString(MARGIN_L, y - 20, f"{REYTECH['cert']} | {REYTECH['web']}")
    c.drawRightString(PAGE_W - MARGIN_R, y - 20, "Thank you for your business!")

    # Bottom line
    c.setStrokeColor(LBL_BD)
    c.setLineWidth(0.5)
    c.line(MARGIN_L, y - 28, PAGE_W - MARGIN_R, y - 28)


def generate_invoice_pdf(order: dict, output_dir: str = "") -> Optional[str]:
    """Generate a branded invoice PDF from an order with draft_invoice data.
    
    Args:
        order: Order dict with "draft_invoice" key
        output_dir: Where to save (default: output/<institution>/)
    
    Returns:
        Path to generated PDF, or None on failure
    """
    inv = order.get("draft_invoice")
    if not inv:
        log.warning("No draft_invoice in order %s", order.get("order_id", "?"))
        return None

    inv_num = inv.get("invoice_number", "INV-000")
    institution = inv.get("bill_to_name", "") or order.get("institution", "")
    items = inv.get("items", [])

    if not items:
        log.warning("No line items in invoice %s", inv_num)
        return None

    # Output path
    if not output_dir:
        safe_inst = "".join(c if c.isalnum() or c in " -_" else "" for c in institution)[:40].strip() or "invoice"
        output_dir = os.path.join(OUTPUT_DIR, safe_inst)
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"Invoice_{inv_num}_{datetime.now().strftime('%Y%m%d')}.pdf"
    output_path = os.path.join(output_dir, filename)

    # Calculate pages needed
    items_per_page_first = 18
    items_per_page_cont = 24
    if len(items) <= items_per_page_first:
        total_pages = 1
    else:
        remaining = len(items) - items_per_page_first
        total_pages = 1 + (remaining + items_per_page_cont - 1) // items_per_page_cont

    c = canvas.Canvas(output_path, pagesize=letter)
    c.setTitle(f"Invoice {inv_num}")
    c.setAuthor(REYTECH["name"])

    item_idx = 0
    for page in range(1, total_pages + 1):
        y = _draw_header(c, inv, page, total_pages)
        y = _draw_table_header(c, y)

        items_this_page = items_per_page_first if page == 1 else items_per_page_cont

        while item_idx < len(items) and items_this_page > 0:
            item = items[item_idx]
            y = _draw_line_item(c, y, item_idx, item)
            item_idx += 1
            items_this_page -= 1

            if y < MARGIN_B + 100:
                break

        # Totals on last page
        if item_idx >= len(items):
            y = _draw_totals(c, y, inv)

        _draw_footer(c, inv)
        
        if page < total_pages:
            c.showPage()

    c.save()
    log.info("Invoice PDF generated: %s (%d items, %d pages)", output_path, len(items), total_pages)
    return output_path
