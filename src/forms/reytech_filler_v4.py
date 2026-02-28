


#!/usr/bin/env python3
"""
Reytech Bid Package Filler v4
- PST timezone for dates (no more future dates)
- Signature whitelist: only signs where applicable
- Improved horizontal alignment of signature
- Font 11pt default, 9pt for tight 704B grid
- No $ prefix on merchandise subtotal
"""

import json, os, io
from datetime import datetime, timezone, timedelta
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, TextStringObject
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.lib.utils import ImageReader
from PIL import Image

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "reytech_config.json")
SIGNATURE_PATH = os.path.join(SCRIPT_DIR, "signature_transparent.png")

# ── Signature whitelist ──────────────────────────────────────────────
# Only these /Sig fields get the signature image. Everything else stays blank.
SIGN_FIELDS = {
    # 703B
    "Signature1",          # 703B Bidder Signature + 704B Vendor Sig + CalRecycle 74
    # Standalone forms
    "Signature",           # CalRecycle 74 standalone + STD 1000 standalone
    "Signature4",          # STD 204 Payee Data Record
    # Bid Package
    "Signature_CUF",       # CUF (MC-345)
    "Signature_darfur",    # Darfur Option #1 ONLY
    "Signature29",         # GSPD-05-105 Bidder Declaration
    "Signature1_PD843",    # DVBE 1st block only
    "708_Signature15",     # GenAI 708
    "Signature_std21",     # STD 21 Drug-Free
    "OBS 1600 Signature",  # OBS 1600 Food Cert — text field but needs signature image
    "AuthorizedRepresentative[0]",  # CV 012 CUF page 2 authorizing signature
}
# NOTE: Signature1 appears in 703B, 704B, and CalRecycle 74 — all get signed.
# NOT signed: Signature2_darfur (Option #2), Signature2/3/4_PD843 (blocks 2-4)

# ── Tight fields (9pt font) ─────────────────────────────────────────
TIGHT_FIELDS = set()
for i in range(1, 16):
    for prefix in ["Row", "QTYRow", "UOMRow", "QTY PER UOMRow", "UNSPSCRow",
                    "ITEM NUMBERRow", "PRICE PER UNITRow", "SUBTOTALRow",
                    "SUBSTITUTED ITEM Include manufacturer part number andor reference numberRow"]:
        TIGHT_FIELDS.add(f"{prefix}{i}")
TIGHT_FIELDS.add("fill_154")

# CalRecycle 74 description fields — narrow (246pt), need auto-size down to 7pt
for i in range(1, 7):
    TIGHT_FIELDS.add(f"Product or Services DescriptionRow{i}")
    TIGHT_FIELDS.add(f"Item Row{i}")

# STD 1000 — City field is only 67pt wide
TIGHT_FIELDS.add("City")


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def get_pst_date():
    """Get current date in PST."""
    pst = timezone(timedelta(hours=-8))
    return datetime.now(pst).strftime("%m/%d/%Y")


def set_field_fonts(writer, field_values, default_size=11, tight_size=9):
    """Set font sizes — 11pt default, auto-sized for tight numeric fields."""
    da_default = f"/Helv {default_size} Tf 0 g"
    
    # Approximate character widths at different font sizes (Helvetica)
    CHAR_WIDTH = {7: 3.9, 8: 4.5, 9: 5.0, 10: 5.6}
    
    for page in writer.pages:
        if "/Annots" not in page:
            continue
        for annot in page["/Annots"]:
            obj = annot.get_object()
            ft = obj.get("/FT")
            name = str(obj.get("/T", ""))
            parent = obj.get("/Parent")
            is_text = (ft == "/Tx") or (not ft and parent and
                       str(parent.get_object().get("/FT", "")) == "/Tx")
            if not is_text:
                continue
            
            if name in TIGHT_FIELDS:
                # Auto-size: check content width vs field width
                content = str(field_values.get(name, ""))
                rect = obj.get("/Rect")
                field_w = float(rect[2]) - float(rect[0]) if rect else 60
                
                # Try 9pt first, drop to 8 or 7 if needed
                font_sz = tight_size
                for try_sz in [9, 8, 7]:
                    est_width = len(content) * CHAR_WIDTH.get(try_sz, 5.0)
                    if est_width < field_w - 4:  # 4pt padding
                        font_sz = try_sz
                        break
                    font_sz = try_sz
                
                obj[NameObject("/DA")] = TextStringObject(f"/Helv {font_sz} Tf 0 g")
            else:
                obj[NameObject("/DA")] = TextStringObject(da_default)


def create_signature_overlay(sig_entries, page_width, page_height, sig_image_path, sign_date=None):
    """
    Create PDF overlay with signature images.
    sig_entries: list of (name, [left, bottom, right, top])
    Only signs fields in SIGN_FIELDS whitelist.
    """
    packet = io.BytesIO()
    c = rl_canvas.Canvas(packet, pagesize=(page_width, page_height))

    if not os.path.exists(sig_image_path):
        c.save(); packet.seek(0); return packet

    sig_img = Image.open(sig_image_path)
    img_reader = ImageReader(sig_img)
    img_w, img_h = sig_img.size
    aspect = img_w / img_h

    for name, rect in sig_entries:
        # Skip fields not in whitelist
        if name not in SIGN_FIELDS:
            continue

        field_w = rect[2] - rect[0]
        field_h = rect[3] - rect[1]

        # Detect vertical field (GSPD-05-105 Signature29 is 32×303)
        is_vertical = field_h > field_w * 2

        if is_vertical:
            # GSPD-05-105: skip the vertical field entirely and draw signature
            # as a horizontal overlay at the bottom (certification line area)
            draw_h = 18  # fixed height for clean look
            draw_w = draw_h * aspect
            if draw_w > 150:
                draw_w = 150
                draw_h = draw_w / aspect
            # Position at bottom of vertical field, horizontally centered
            x = rect[0] - draw_w + 10  # draw LEFT of the vertical strip
            y = rect[1] + 2  # at the bottom where certification line is
            c.drawImage(img_reader, x, y, draw_w, draw_h, mask='auto')
            continue

        # ── Horizontal signature ──
        # For small combo fields (704B SIGNATURE/DATE), draw sig + date
        has_room_for_date = field_w < 200 and sign_date and field_w > 100

        # Scale to fit — slightly larger than before
        draw_h = field_h * 0.90
        draw_w = draw_h * aspect

        # Cap width
        max_w = field_w * 0.50 if has_room_for_date else field_w * 0.85
        if draw_w > max_w:
            draw_w = max_w
            draw_h = draw_w / aspect

        # Position: left-aligned, vertically centered
        x = rect[0] + 2
        y = rect[1] + (field_h - draw_h) / 2

        c.drawImage(img_reader, x, y, draw_w, draw_h, mask='auto')

        # Draw date next to sig in small combo fields
        if has_room_for_date:
            date_x = x + draw_w + 6
            date_y = rect[1] + (field_h / 2) - 4
            c.setFont("Helvetica", 9)
            c.setFillColorRGB(0, 0, 0)
            c.drawString(date_x, date_y, sign_date)

    c.save()
    packet.seek(0)
    return packet


def fill_and_sign_pdf(input_path, field_values, output_path,
                       default_font=11, tight_font=9, sig_image=None, sign_date=None):
    reader = PdfReader(input_path)
    writer = PdfWriter()
    writer.append(reader)

    clean_values = {k: v for k, v in field_values.items() if v is not None}
    set_field_fonts(writer, clean_values, default_font, tight_font)

    for page in writer.pages:
        try:
            writer.update_page_form_field_values(page, clean_values, auto_regenerate=False)
        except Exception:
            pass

    sig_path = sig_image or SIGNATURE_PATH
    for page in writer.pages:
        if "/Annots" not in page:
            continue
        sig_entries = []
        for annot in page["/Annots"]:
            obj = annot.get_object()
            ft = str(obj.get("/FT", ""))
            name = str(obj.get("/T", ""))
            if "/Rect" not in obj:
                continue
            # Overlay signature on /Sig fields
            if ft == "/Sig":
                try:
                    r = [float(x) for x in obj["/Rect"]]
                    sig_entries.append((name, r))
                except Exception:
                    pass
            # Also overlay on text fields that are in the SIGN_FIELDS whitelist
            elif name in SIGN_FIELDS:
                try:
                    r = [float(x) for x in obj["/Rect"]]
                    sig_entries.append((name, r))
                except Exception:
                    pass

        if sig_entries:
            mediabox = page.get("/MediaBox", [0, 0, 612, 792])
            pw, ph = float(mediabox[2]), float(mediabox[3])
            overlay_buf = create_signature_overlay(sig_entries, pw, ph, sig_path, sign_date)
            overlay_reader = PdfReader(overlay_buf)
            if overlay_reader.pages:
                page.merge_page(overlay_reader.pages[0])

    with open(output_path, "wb") as f:
        writer.write(f)


# ═══════════════════════════════════════════════════════════════════════
# Form Fillers
# ═══════════════════════════════════════════════════════════════════════

def fill_703b(input_path, rfq_data, config, output_path):
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())

    # Bid expiration = always 45 days from generation date (today)
    try:
        gen_date = datetime.strptime(sign_date, "%m/%d/%Y")
    except Exception:
        gen_date = datetime.now()
    bid_exp = (gen_date + timedelta(days=45)).strftime("%m/%d/%Y")

    # Parse due_date flexibly (handles both 2-digit and 4-digit year)
    due_date_str = rfq_data.get("due_date", "")
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            datetime.strptime(due_date_str, fmt)
            break
        except Exception:
            pass

    values = {
        "703B_Business Name": company["name"],
        "703B_Address": company["address"],
        "703B_Contact Person": company["owner"],
        "703B_Title": company["title"],
        "703B_Phone": company["phone"],
        "703B_Email": company["email"],
        "703B_Federal Employer Identification Number FEIN": company["fein"],
        "703B_Retailers CA Sellers Permit Number": company["sellers_permit"],
        "703B_SBMBDVBE Certification.0": company["cert_number"],
        "703B_Certification Expiration Date": company["cert_expiration"],
        "703B_Payment discount offered on invoices to be paid within": "N/A",
        "703B_days of receipt": "0",
        "703B_Solicitation Number": rfq_data.get("solicitation_number", ""),
        "703B_Release Date": rfq_data.get("release_date", ""),
        "703B_Due Date": rfq_data.get("due_date", ""),
        "703B_BidExpirationDate": bid_exp,
        "703B_Sign_Date": sign_date,
        "703B_Deliveries must be completed within": rfq_data.get("delivery_days", "30"),
        "703B_Name": rfq_data.get("requestor_name", ""),
        "703B_Email_2": rfq_data.get("requestor_email", ""),
        "703B_Phone_2": rfq_data.get("requestor_phone", ""),
        "703B_Check Box2": "/Yes", "703B_Check Box4": "/Yes",
        "703B_Check Box5": "/Yes", "703B_Check Box7": "/Yes",
        "703B_ResponseList.0": "/Yes", "703B_ResponseList.1": "/Yes",
        "703B_ResponseList.2": "/Yes", "703B_ResponseList.3": "/Yes",
        "703B_ResponseList.4": "/Yes", "703B_ResponseList.5": "/Yes",
        "703B_ResponseList.6": "/Yes", "703B_ResponseList.7": "/Yes",
        "703B_ResponseList.14": "/Yes",
    }
    if rfq_data.get("delivery_location"):
        values["703B_Dropdown2"] = rfq_data["delivery_location"]

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)
    print(f"  ✓ 703B filled + signed ({sign_date})")


def fill_704b(input_path, rfq_data, config, output_path):
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())

    values = {
        "COMPANY NAME": company["name"],
        "PERSON PROVIDING QUOTE": company["owner"],
        "Contract_Number": "N/A",
    }

    line_items = rfq_data.get("line_items", [])
    merchandise_subtotal = 0.0

    seq = 0  # sequential line item counter
    for item in line_items:
        row_num = item.get("form_row", item.get("row_index", item.get("line_number", 0)))
        if not row_num:
            continue
        seq += 1
        price = item.get("price_per_unit", 0)
        qty = item.get("qty", 0)
        subtotal = round(price * qty, 2)
        merchandise_subtotal += subtotal
        values[f"PRICE PER UNITRow{row_num}"] = f"{price:.2f}" if price else ""
        values[f"SUBTOTALRow{row_num}"] = f"{subtotal:.2f}" if subtotal else ""

        # Fill ITEM # column with sequential line number (1, 2, 3...)
        values[f"ITEM NUMBERRow{row_num}"] = str(seq)

        # SUBSTITUTED ITEM column: only fill when item is marked as a substitute
        # Otherwise explicitly clear it (original 704A may have reference data in this field)
        sub_field = f"SUBSTITUTED ITEM Include manufacturer part number andor reference numberRow{row_num}"
        if item.get("is_substitute"):
            sub_desc = item.get("description", "")
            mfg = item.get("mfg_number", "")
            sub_text = f"{sub_desc} (MFG# {mfg})" if mfg else sub_desc
            values[sub_field] = sub_text
        else:
            values[sub_field] = ""

    # Leading space pushes text past the printed "$"
    values["fill_154"] = f" {merchandise_subtotal:.2f}"

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)
    print(f"  ✓ 704B filled + signed — ${merchandise_subtotal:,.2f}")


def fill_obs1600_fields(rfq_data, config, food_items=None):
    """
    Build field values dict for OBS 1600 (CA Agricultural Food Product Certification).
    
    Reytech is a reseller/distributor — none of our food products are CA-grown
    or produced, so Code and % columns are always "N/A" and CA Grown is always "No".
    
    Args:
        rfq_data: RFQ data dict with 'line_items' or 'items'
        config: Config dict with company info
        food_items: Pre-classified food items (optional; will auto-detect if None)
    
    Returns:
        dict of field_id -> value for all OBS 1600 fields
    """
    from src.forms.food_classifier import is_food_item
    
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())
    
    # Get items from rfq_data
    items = rfq_data.get("line_items", rfq_data.get("items", rfq_data.get("items_detail", [])))
    if isinstance(items, str):
        import json as _json
        try: items = _json.loads(items)
        except: items = []
    
    # Collect food items (just detect food vs non-food, no category classification needed)
    if food_items is None:
        food_items = []
        for item in items:
            desc = item.get("description", "")
            if is_food_item(desc):
                food_items.append({
                    "line_number": item.get("line_number", len(food_items) + 1),
                    "description": desc,
                })
    
    values = {}
    
    # Clear ALL 18 rows first (in case template has pre-filled data)
    for row in range(1, 19):
        values[f"OBS 1600 PG 1 LI # - ROW {row}"] = ""
        values[f"OBS 1600 FOOD PROD PG 1 - ROW {row}"] = ""
        values[f"OBS 1600 PG 1 CODE - ROW {row}"] = ""
        values[f"OBS 1600 CA GROWN PG1 - ROW {row}"] = ""
        values[f"OBS 1600 % OF PRODUCT PG 1 - ROW {row}"] = ""
    
    # Fill rows with food items
    # Code = "N/A", CA Grown = "No", % = "N/A" (Reytech is a reseller, not a grower)
    # Description truncated to ~55 chars to fit the form field
    for i, item in enumerate(food_items[:18]):
        row = i + 1
        desc = item.get("description", "")
        # Truncate description to fit form field — keep enough to identify the item
        if len(desc) > 55:
            desc = desc[:52] + "..."
        values[f"OBS 1600 PG 1 LI # - ROW {row}"] = str(item.get("line_number", row))
        values[f"OBS 1600 FOOD PROD PG 1 - ROW {row}"] = desc
        values[f"OBS 1600 PG 1 CODE - ROW {row}"] = "N/A"
        values[f"OBS 1600 CA GROWN PG1 - ROW {row}"] = "No"
        values[f"OBS 1600 % OF PRODUCT PG 1 - ROW {row}"] = "N/A"
    
    # Signature block
    values["OBS 1600 Print Name"] = company["owner"]
    values["OBS 1600 Title"] = company["title"]
    values["OBS 1600 Date"] = sign_date
    
    return values


def _overlay_obs1600_header(writer, solicitation_number, vendor_name="Reytech Inc.", page_index=3):
    """Overlay Vendor Name and Solicitation # onto OBS 1600 page header.
    
    These are static labels on the form (not fillable fields), so we overlay text.
    Coordinates measured from pdfplumber: Vendor Name label ends at x≈124, y≈201;
    Solicitation # label ends at x≈120, y≈216 (from top). 
    ReportLab uses y-from-bottom, so y_rl = 792 - y_top.
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas as rl_canvas
    import io
    
    W, H = letter  # 612 x 792
    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=letter)
    c.setFont("Helvetica", 10)
    
    # Vendor Name: after "Vendor Name :" label — x≈128, top≈201 → rl_y = 792-201-4 = 587
    c.drawString(128, H - 205, vendor_name)
    
    # Solicitation #: after "Solicitation # :" label — x≈124, top≈216 → rl_y = 792-216-4 = 572
    c.drawString(124, H - 220, str(solicitation_number))
    
    c.save()
    buf.seek(0)
    
    overlay_reader = PdfReader(buf)
    if overlay_reader.pages and page_index < len(writer.pages):
        writer.pages[page_index].merge_page(overlay_reader.pages[0])


def fill_obs1600(input_path, rfq_data, config, output_path, food_items=None):
    """
    Fill OBS 1600 form as standalone PDF.
    Uses fillable fields if present, otherwise overlays text.
    """
    sign_date = rfq_data.get("sign_date", get_pst_date())
    sol = rfq_data.get("solicitation_number", "")
    values = fill_obs1600_fields(rfq_data, config, food_items)
    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)
    
    # Overlay Vendor Name and Solicitation # (not fillable fields in template)
    try:
        reader = PdfReader(output_path)
        writer = PdfWriter()
        writer.append(reader)
        _overlay_obs1600_header(writer, sol, vendor_name="Reytech Inc.", page_index=0)
        with open(output_path, "wb") as f:
            writer.write(f)
    except Exception as _e:
        print(f"  ⚠ OBS 1600 header overlay failed: {_e}")
    
    actual_food = len([k for k in values if 'FOOD PROD' in k and values[k]])
    print(f"  ✓ OBS 1600 Food Certification filled ({sol}, {actual_food} food items)")


# ═══════════════════════════════════════════════════════════════════════
# STD 1000 — GenAI Reporting and Factsheet
# ═══════════════════════════════════════════════════════════════════════

def fill_std1000(input_path, rfq_data, config, output_path):
    """Fill STD 1000 GenAI Reporting form with company info + line items.
    Line items are overlaid via ReportLab because pypdf multiline rendering is unreliable."""
    company = config["company"]
    sol = rfq_data.get("solicitation_number", "")
    sign_date = rfq_data.get("sign_date", get_pst_date())

    values = {
        "Number Bidder ID  Vendor ID optional": company["cert_number"],
        "Business Name": company["name"],
        "Business Telephone Number": company["phone"],
        "Business Address": "30 Carnoustie Way",
        "City": "Trabuco Canyon",
        "State": "CA",
        "Zip Code": "92679",
        # NOTE: "Contract / Description of Purchase" is overlaid via ReportLab
        # GenAI = No
        "No If no skip to Signature section of this form": "/On",
        "Date": sign_date,
    }

    # Only fill solicitation line if we have one
    if sol:
        values["Solicitation  Contract Number"] = sol

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)

    # Overlay line items into the "Contract / Description of Purchase" field
    # Field rect: x=15, y=312, w=582, h=203  (page 0)
    items = rfq_data.get("line_items", [])
    _overlay_std1000_description(output_path, items)

    print(f"  ✓ STD 1000 GenAI filled ({sol}, {len(items)} items)")


def _overlay_std1000_description(pdf_path, items, page_index=0):
    """Overlay line items into the STD 1000 'Contract / Description of Purchase' field."""
    from reportlab.lib.pagesizes import letter

    # Field rect from template: [16, 346, 598, 548]
    x_start = 20
    y_top = 540       # Just below top of field (548 - 8pt padding)
    max_width = 570
    box_height = 185  # 540 - 346 = ~194, minus padding

    # Build lines
    lines = []
    n = len(items)
    for i, item in enumerate(items, 1):
        pn = item.get("item_number", item.get("part_number", ""))
        desc = item.get("description", "")
        qty = item.get("qty", 1)
        uom = item.get("uom", "EA")
        if n <= 15:
            lines.append(f"{i}. {pn}  {desc}  (Qty: {qty} {uom})")
        else:
            # Condensed format for many items
            lines.append(f"{i}. {pn} {desc} ({qty} {uom})")
    if not lines:
        lines = ["N/A"]

    # Auto-size: try font sizes from 10pt down to 5.5pt
    # Leading tightens at smaller sizes for density
    font_name = "Helvetica"
    size_options = [
        (10, 12), (9, 11), (8.5, 10.5), (8, 10), (7.5, 9.5),
        (7, 8.5), (6.5, 8), (6, 7.5), (5.5, 7),
    ]
    chosen_size, chosen_leading = size_options[-1]  # fallback to smallest
    for sz, ld in size_options:
        if len(lines) * ld <= box_height - 4:
            chosen_size, chosen_leading = sz, ld
            break

    # If still overflows at smallest font, truncate with "...and X more"
    max_lines = int((box_height - 4) / chosen_leading)
    if len(lines) > max_lines:
        remaining = len(lines) - max_lines + 1
        lines = lines[:max_lines - 1]
        lines.append(f"...and {remaining} more items (see quote for full list)")

    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=letter)
    text_obj = c.beginText(x_start, y_top)
    text_obj.setFont(font_name, chosen_size)
    text_obj.setLeading(chosen_leading)

    for line in lines:
        # Truncate if line is too wide
        while c.stringWidth(line, font_name, chosen_size) > max_width and len(line) > 20:
            line = line[:len(line) - 4] + "..."
        text_obj.textLine(line)

    c.drawText(text_obj)
    c.save()
    buf.seek(0)

    reader = PdfReader(pdf_path)
    writer = PdfWriter()
    writer.append(reader)

    overlay_reader = PdfReader(buf)
    if overlay_reader.pages:
        writer.pages[page_index].merge_page(overlay_reader.pages[0])

    with open(pdf_path, "wb") as f:
        writer.write(f)


# ═══════════════════════════════════════════════════════════════════════
# STD 204 — Payee Data Record
# ═══════════════════════════════════════════════════════════════════════

def fill_std204(input_path, rfq_data, config, output_path):
    """Fill STD 204 Payee Data Record with Reytech company info."""
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())

    values = {
        # Section 1 — Payee Information
        " Must match the payee\u2019s federal tax return)": "R. Michael Guadan",
        "BUSINESS NAME, DBA NAME or DISREGARDED SINGLE MEMBER LLC NAME (If different from above)": company["name"],
        ") (See instructions on Page 2)": "30 Carnoustie Way",
        "CITY STATE ZIP CODE": "Trabuco Canyon CA 92679",
        "EMAIL ADDRESS": "sales@reytechinc.com",
        # Section 2 — Entity Type: ALL OTHERS
        "corpOthers": "/On",
        # Section 3 — FEIN
        "Federal Employer Identification Number (FEIN)": company["fein"],
        # Section 4 — CA Resident
        "calRes": "/On",
        # Section 5 — Certification
        "NAME OF AUTHORIZED PAYEE REPRESENTATIVE": "R. Michael Guadan",
        "TITLE": company["title"],
        "EMAIL ADDRESS_2": "mike@reytechinc.com",
        "DATE": sign_date,
        "TELEPHONE include area code": company["phone"],
        # Section 6 — Paying State Agency (left for agency to fill)
        "UNITSECTION": "Procurement",
    }

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)
    print(f"  ✓ STD 204 Payee Data Record filled")


# ═══════════════════════════════════════════════════════════════════════
# CV 012 — Commercially Useful Function (CUF) Certification Form
# ═══════════════════════════════════════════════════════════════════════

CUF_WRITTEN_STATEMENT = (
    "Reytech will fully manage all aspects of ordering, delivery, and customer service "
    "for the products required under this contract. As the primary point of contact, "
    "Reytech will directly handle product sourcing, order management, and delivery "
    "coordination to ensure timely and accurate fulfillment. We also manage all customer "
    "service inquiries to provide seamless support to the State. Reytech does not "
    "subcontract any portion of this work, maintaining complete control and accountability "
    "for every stage of the process to meet the commercially useful function (CUF) "
    "requirements. Additional clarifying information can be provided upon request."
)

def fill_cv012_cuf(input_path, rfq_data, config, output_path):
    """Fill CV 012 CUF Certification Form (both pages) with Reytech info + signature.
    Checkboxes and written statement are overlaid via ReportLab (XFA radios only allow one)."""
    company = config["company"]
    sol = rfq_data.get("solicitation_number", "")
    sign_date = rfq_data.get("sign_date", get_pst_date())

    values = {
        # Page 1 — Header
        "SolicitationNumber[0]": sol,
        "DoingBusinessAs[0]": company["name"],
        "OSDSRefNumber[0]": company["cert_number"],
        "ExpirationDate[0]": company.get("cert_expiration", "May 31, 2027"),
        # NOTE: RadioButtonList[0-5] handled via ReportLab overlay (XFA radio = single select)
        # NOTE: ProvideAWrittenStatement overlaid via ReportLab (XFA doesn't wrap)
        # Page 2 — Authorizing Signature
        "Title[0]": company["title"],
        "PrintedName[0]": "Michael Guadan",
        "Date[0]": sign_date,
    }

    # Step 1: fill form fields + signature overlay
    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)

    # Step 2: overlay checkmarks + written statement on page 0 via ReportLab
    _overlay_cuf_page0(output_path, CUF_WRITTEN_STATEMENT)

    print(f"  ✓ CV 012 CUF filled (2 pages, sol={sol})")


def _overlay_cuf_page0(pdf_path, statement_text, page_index=0):
    """Overlay checkmarks (DVBE/SB/MB, Q1-5) and written statement on CUF page 0."""
    from reportlab.lib.pagesizes import letter

    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=letter)

    # ── Checkmarks ──
    # Positions from XFA RadioButtonList kid rects (center of each 10x10 box)
    # "Mark all that apply": DVBE, Small Business, Micro Business — check ALL three
    checks_mark_all = [
        (152.8, 531.0),   # DVBE
        (267.0, 531.0),   # Small Business
        (377.8, 531.0),   # Micro Business
    ]
    # Q1-3 = Yes (kid[0]), Q4-5 = No (kid[1])
    checks_answers = [
        (513.3, 508.0),   # Q1 Yes
        (513.3, 481.0),   # Q2 Yes
        (513.3, 454.0),   # Q3 Yes
        (571.4, 427.0),   # Q4 No
        (571.4, 400.0),   # Q5 No
    ]

    c.setFont("ZapfDingbats", 12)
    checkmark = "\x34"  # ZapfDingbats ✔
    for x, y in checks_mark_all + checks_answers:
        c.drawCentredString(x, y, checkmark)

    # ── Written Statement (word-wrapped) ──
    x_start = 28
    y_top = 295
    max_width = 555
    font_name = "Helvetica"
    font_size = 10
    leading = 13

    text_obj = c.beginText(x_start, y_top)
    text_obj.setFont(font_name, font_size)
    text_obj.setLeading(leading)

    words = statement_text.split()
    line = ""
    for word in words:
        test = f"{line} {word}".strip()
        if c.stringWidth(test, font_name, font_size) > max_width:
            text_obj.textLine(line)
            line = word
        else:
            line = test
    if line:
        text_obj.textLine(line)

    c.drawText(text_obj)
    c.save()
    buf.seek(0)

    # Merge overlay onto page
    reader = PdfReader(pdf_path)
    writer = PdfWriter()
    writer.append(reader)

    overlay_reader = PdfReader(buf)
    if overlay_reader.pages:
        writer.pages[page_index].merge_page(overlay_reader.pages[0])

    with open(pdf_path, "wb") as f:
        writer.write(f)


# ═══════════════════════════════════════════════════════════════════════
# Barstow CUF — Veterans Home of California - Barstow specific form
# ═══════════════════════════════════════════════════════════════════════

def generate_barstow_cuf(rfq_data, config, output_path):
    """Generate the Barstow-specific CUF form (ReportLab) — simple Yes/No questionnaire."""
    company = config["company"]

    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.lib import colors

    c = rl_canvas.Canvas(output_path, pagesize=letter)
    w, h = letter

    # ── Title block ──
    y = h - 1.0 * inch
    c.setFont("Helvetica-Bold", 13)
    c.drawCentredString(w / 2, y, "CALIFORNIA DEPARTMENT OF VETERANS AFFAIRS")
    y -= 18
    c.drawCentredString(w / 2, y, "VETERANS HOME OF CALIFORNIA - BARSTOW")
    y -= 20
    c.setFont("Helvetica-Bold", 12)
    c.drawCentredString(w / 2, y, "COMMERCIALLY USEFUL FUNCTION DOCUMENTATION")

    # ── Underline ──
    y -= 6
    c.setStrokeColor(colors.black)
    c.setLineWidth(0.8)
    c.line(1.0 * inch, y, w - 1.0 * inch, y)

    # ── Company ──
    y -= 36
    c.setFont("Helvetica", 11)
    c.drawString(1.2 * inch, y, "COMPANY:")
    y -= 22
    c.setFont("Helvetica-Bold", 13)
    c.drawCentredString(w / 2, y, company["name"])
    # Underline company name
    name_w = c.stringWidth(company["name"], "Helvetica-Bold", 13)
    c.setLineWidth(0.5)
    c.line(w / 2 - name_w / 2 - 20, y - 3, w / 2 + name_w / 2 + 20, y - 3)

    # ── Preamble ──
    y -= 36
    c.setFont("Helvetica", 9.5)
    preamble = (
        "All certified Small Business, Micro business, and/or DVBE contractors, subcontractors or "
        "suppliers must meet or suppliers must meet the commercially useful function "
        "requirements under Government Code, Section 14837 (d)(4) (for SB & MB) and Military "
        "& Veterans Code, Section 999(b)(5)(B0)(for DVBE)."
    )
    text_obj = c.beginText(1.2 * inch, y)
    text_obj.setFont("Helvetica", 9.5)
    # Word-wrap
    max_w = w - 2.4 * inch
    words = preamble.split()
    line = ""
    for word in words:
        test = f"{line} {word}".strip()
        if c.stringWidth(test, "Helvetica", 9.5) > max_w:
            text_obj.textLine(line)
            line = word
        else:
            line = test
    if line:
        text_obj.textLine(line)
    c.drawText(text_obj)
    y = text_obj.getY()

    y -= 18
    c.setFont("Helvetica", 10)
    c.drawString(1.2 * inch, y, "Please answer the following questions, as they apply to your company for the")
    y -= 14
    c.drawString(1.2 * inch, y, "goods/services that are being acquired in this procurement.")

    # ── Questions table ──
    questions = [
        ("1", "Will your company be responsible for the execution of a distinct\nelement of the resulting purchase order?", True),
        ("2", "Will your company be actually performing, managing, or\nsupervising an element of the resulting purchase order?", True),
        ("3", "Will your company be performing work on the resulting purchase\norder that is normal for its business, services and/or functions?", True),
        ("4", "Will there be any subcontracting that is greater than that expected to\nbe subcontracted by normal industry practices for the resulting\npurchase order?", False),
    ]

    y -= 30
    left = 1.2 * inch
    q_col = left + 0.3 * inch
    yes_col = w - 2.4 * inch
    no_col = w - 1.6 * inch
    row_h = 42

    for num, text, answer_yes in questions:
        # Draw row box
        c.setStrokeColor(colors.black)
        c.setLineWidth(0.5)
        lines = text.split("\n")
        actual_h = max(row_h, 14 * len(lines) + 10)

        c.rect(left, y - actual_h + 14, w - 2.4 * inch, actual_h)

        # Number
        c.setFont("Helvetica-Bold", 10)
        c.drawCentredString(left + 0.15 * inch, y, num)

        # Question text
        c.setFont("Helvetica", 9.5)
        ty = y
        for line in lines:
            c.drawString(q_col, ty, line)
            ty -= 13

        # Yes/No boxes
        box_size = 13
        yes_y = y - actual_h / 2 + 14
        c.setFont("Helvetica-Bold", 10)
        c.drawString(yes_col, yes_y + 16, "Yes")
        c.drawString(no_col, yes_y + 16, "No")
        c.rect(yes_col + 2, yes_y, box_size, box_size)
        c.rect(no_col + 2, yes_y, box_size, box_size)

        # Check the right box
        if answer_yes:
            # Fill Yes box
            c.setFillColor(colors.black)
            c.rect(yes_col + 2, yes_y, box_size, box_size, fill=1)
            c.setFillColor(colors.white)
            c.setFont("Helvetica-Bold", 10)
            c.drawCentredString(yes_col + 2 + box_size / 2, yes_y + 2, "Yes")
            c.setFillColor(colors.black)
        else:
            # Fill No box
            c.setFillColor(colors.black)
            c.rect(no_col + 2, yes_y, box_size, box_size, fill=1)
            c.setFillColor(colors.white)
            c.setFont("Helvetica-Bold", 10)
            c.drawCentredString(no_col + 2 + box_size / 2, yes_y + 2, "No")
            c.setFillColor(colors.black)

        y -= actual_h + 4

    # ── Disqualification note ──
    y -= 20
    c.setFont("Helvetica", 9.5)
    note = (
        "For a response of NO in questions 1-3, or a response of YES in question 4, may result in "
        "your bid being eliminated from consideration at the State's option prior to award. Bidders "
        "may be required to submit additional written clarifying information."
    )
    text_obj = c.beginText(1.2 * inch, y)
    text_obj.setFont("Helvetica", 9.5)
    words = note.split()
    line = ""
    for word in words:
        test = f"{line} {word}".strip()
        if c.stringWidth(test, "Helvetica", 9.5) > max_w:
            text_obj.textLine(line)
            line = word
        else:
            line = test
    if line:
        text_obj.textLine(line)
    c.drawText(text_obj)

    c.save()
    print(f"  ✓ Barstow CUF generated (ReportLab)")


# ═══════════════════════════════════════════════════════════════════════
# CalRecycle 74 — Standalone with overflow pages
# ═══════════════════════════════════════════════════════════════════════

def fill_calrecycle_standalone(input_path, rfq_data, config, output_path):
    """Fill CalRecycle 74 form with line items. Adds overflow pages for >6 items."""
    company = config["company"]
    sol = rfq_data.get("solicitation_number", "")
    sign_date = rfq_data.get("sign_date", get_pst_date())
    items = rfq_data.get("line_items", [])

    # Common company fields
    base_values = {
        "ContractorCompany Name": company["name"],
        "Address": company["address"],
        "Phone_2": company["phone"],
        "Print Name": company["owner"],
        "Title": company["title"],
        "Date": sign_date,
    }

    # Fill first page (up to 6 items)
    values = dict(base_values)
    for idx, item in enumerate(items[:6], start=1):
        pn = item.get("item_number", item.get("part_number", ""))
        desc = item.get("description", "")
        if len(desc) > 60:
            desc = desc[:57] + "..."
        values[f"Item Row{idx}"] = pn
        values[f"Product or Services DescriptionRow{idx}"] = desc
        values[f"1Percent Postconsumer Recycled Content MaterialRow{idx}"] = "0%"
        values[f"2SABRC Product Category CodeRow{idx}"] = "N/A"

    if not items:
        values["Product or Services DescriptionRow1"] = "All Items"
        values["1Percent Postconsumer Recycled Content MaterialRow1"] = "0%"
        values["2SABRC Product Category CodeRow1"] = "N/A"

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)

    # Overflow: if >6 items, append additional CalRecycle pages
    if len(items) > 6:
        remaining = items[6:]
        # Use blank CalRecycle template for overflow
        tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "templates")
        blank_cr = os.path.join(tmpl_dir, "calrecycle_74_blank.pdf")
        if not os.path.exists(blank_cr):
            print(f"  ⚠ CalRecycle overflow: blank template not found at {blank_cr}")
            return

        # Process in batches of 6
        overflow_pages = []
        for batch_start in range(0, len(remaining), 6):
            batch = remaining[batch_start:batch_start + 6]
            ov_values = dict(base_values)
            for idx, item in enumerate(batch, start=1):
                pn = item.get("item_number", item.get("part_number", ""))
                desc = item.get("description", "")
                if len(desc) > 60:
                    desc = desc[:57] + "..."
                ov_values[f"Item Row{idx}"] = pn
                ov_values[f"Product or Services DescriptionRow{idx}"] = desc
                ov_values[f"1Percent Postconsumer Recycled Content MaterialRow{idx}"] = "0%"
                ov_values[f"2SABRC Product Category CodeRow{idx}"] = "N/A"

            ov_path = output_path.replace(".pdf", f"_overflow_{batch_start}.pdf")
            fill_and_sign_pdf(blank_cr, ov_values, ov_path, sign_date=sign_date)
            overflow_pages.append(ov_path)

        # Merge: original + overflow pages (page 0 only from each overflow, skip ref table)
        from pypdf import PdfReader as _PR, PdfWriter as _PW
        writer = _PW()
        main_reader = _PR(output_path)
        # Add page 0 (filled CalRecycle) from main
        writer.add_page(main_reader.pages[0])

        # Add page 0 from each overflow (skip page 1 = reference table)
        for ov_path in overflow_pages:
            ov_reader = _PR(ov_path)
            writer.add_page(ov_reader.pages[0])

        # Add reference table (page 1) from main at the end
        if len(main_reader.pages) > 1:
            writer.add_page(main_reader.pages[1])

        with open(output_path, "wb") as f:
            writer.write(f)

        # Cleanup temp files
        for ov_path in overflow_pages:
            try:
                os.remove(ov_path)
            except Exception:
                pass

        print(f"  ✓ CalRecycle 74 filled ({sol}, {len(items)} items, {len(overflow_pages)} overflow pages)")
    else:
        print(f"  ✓ CalRecycle 74 filled ({sol}, {len(items)} items)")


def fill_bid_package(input_path, rfq_data, config, output_path):
    company = config["company"]
    sol = rfq_data.get("solicitation_number", "")
    sign_date = rfq_data.get("sign_date", get_pst_date())

    values = {
        # CUF
        "DOING BUSINESS AS DBA NAME_CUF": company["name"],
        "OSDS REF  CURRENTLY CERTIFIED FIRMS ONLY_CUF": company["cert_number"],
        "Date_CUF": sign_date, "Text7_CUF": sol,
        "Check_CUF1": "/Yes", "Check_CUF3": "/Yes",
        "Check_CUF5": "/Yes", "Check_CUF7": "/Yes",
        "Check_CUF9": "/Yes", "Check_CUF11": "/Yes",

        # Darfur — Option #1 only (not scrutinized)
        "CompanyVendor Name Printed_darfur": company["name"],
        "Federal ID Number_darfur": company["fein"],
        "Printed Name and Title of Person Signing_darfur": f"{company['owner']}, {company['title']}",
        "Date__darfur": sign_date,

        # Bidder Declaration (GSPD-05-105)
        "Text0_105": sol, "Text1_105": "SB/DVBE",
        "Check3_105": "/Yes", "Check5_105": "/Yes", "Check8_105": "/Yes",
        "Text2_105": "N/A", "Text4_105": "N/A",
        "Page1_105": "1", "Page2_105": "1",

        # DVBE (PD 843) — 1st block only
        "Text1_PD843": company["name"], "Text2_PD843": company["cert_number"],
        "Text3_PD843": company.get("description_of_goods", "Medical/Office supplies"),
        "Text4_PD843": sol, "Check1_PD843": "/Yes",
        "Text6_PD843": company["name"], "Date1_PD843": sign_date,
        "Text11_PD843": "N/A",

        # STD 21 (Drug-Free)
        "Text1_std21": company["name"], "Text2_std21": company["fein"],
        "Text3_std21": "7/10/2025", "Text4_std21": company["owner"],
        "Text5_std21": "229-1575", "Text6_std21": "949",
        "Text7_std21": company["title"], "Text8_std21": company["address"],
        "Text9_std21": company.get("drug_free_expiration", "7/1/2028"),

        # CalRecycle 74 — company info + signature
        "ContractorCompany Name": company["name"],
        "Address": company["address"], "Phone_2": company["phone"],
        "Print Name": company["owner"], "Title": company["title"],
        "Date": sign_date,
    }

    # ── CalRecycle 74: populate each line item row ──
    line_items = rfq_data.get("line_items", [])
    for idx, item in enumerate(line_items[:6], start=1):  # Template has 6 rows max
        pn = item.get("item_number", item.get("part_number", ""))
        desc = item.get("description", "")
        # Truncate description to fit form field
        if len(desc) > 60:
            desc = desc[:57] + "..."
        values[f"Item Row{idx}"] = pn
        values[f"Product or Services DescriptionRow{idx}"] = desc
        values[f"1Percent Postconsumer Recycled Content MaterialRow{idx}"] = "0%"
        values[f"2SABRC Product Category CodeRow{idx}"] = "N/A"

    if not line_items:
        # Fallback if no line items
        values["Product or Services DescriptionRow1"] = "All Items"
        values["1Percent Postconsumer Recycled Content MaterialRow1"] = "0%"
        values["2SABRC Product Category CodeRow1"] = "N/A"

    if len(line_items) > 6:
        import logging
        logging.getLogger("reytech").warning(
            "CalRecycle 74: %d items but only 6 rows on template. Items 7+ not listed.", len(line_items)
        )

    values.update({
        # GenAI (708)
        "708_Text1": sol, "708_Text3": company["name"],
        "708_Text4": company["phone"], "708_Text5": "30 Carnoustie Way",
        "708_Text6": "Trabuco Canyon", "708_Text7": "CA", "708_Text8": "92679",
        "708_Check Box2": "/Yes",
        "708_Text11": "N/A",
        "708_Text12.0": "N/A", "708_Text12.1": "N/A", "708_Text12.2": "N/A",
        "708_Text12.3.0": "N/A", "708_Text12.3.1": "N/A", "708_Text12.3.2": "N/A",
        "708_Text13.0": "N/A", "708_Text13.1": "N/A", "708_Text13.2": "N/A",
        "708_Text13.3": "N/A", "708_Text13.4": "N/A", "708_Text13.5": "N/A",
        "708_Text13.6": "N/A", "708_Text13.7": "N/A", "708_Text14": "N/A",
        "708_Text16": sign_date,

        "Date_PD802": sign_date,
    })

    # ── OBS 1600: Auto-fill food items if present in RFQ ──
    obs1600_values = fill_obs1600_fields(rfq_data, config)
    values.update(obs1600_values)

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)
    
    # ── OBS 1600 Header: Overlay Vendor Name + Solicitation # (not fillable fields) ──
    try:
        reader = PdfReader(output_path)
        writer = PdfWriter()
        writer.append(reader)
        _overlay_obs1600_header(writer, sol, vendor_name="Reytech Inc.", page_index=3)
        with open(output_path, "wb") as f:
            writer.write(f)
    except Exception as _e:
        print(f"  ⚠ OBS 1600 header overlay failed: {_e}")
    
    food_count = len([k for k in obs1600_values if 'FOOD PROD' in k and obs1600_values[k]])
    extra = f", {food_count} food items" if food_count else ""
    print(f"  ✓ Bid Package filled + signed ({sol}{extra})")


# ═══════════════════════════════════════════════════════════════════════
# Pricing
# ═══════════════════════════════════════════════════════════════════════

def calculate_recommended_price(cost, scprs_price, source_type, config):
    rules = config["pricing_rules"]
    undercut = rules["scprs_undercut_pct"]
    floor = rules["profit_floor_amazon"] if source_type == "amazon" else rules["profit_floor_general"]
    if not scprs_price:
        return None, None, "no_scprs_data"
    recommended = round(scprs_price * (1 - undercut), 2)
    profit = recommended - cost
    if cost >= scprs_price:
        return recommended, None, "below_cost"
    if profit < floor:
        return recommended, round((profit / recommended) * 100, 1), "low_margin"
    return recommended, round((profit / recommended) * 100, 1), "good"


def apply_pricing_to_rfq(rfq_data, config):
    for item in rfq_data.get("line_items", []):
        rec, margin, status = calculate_recommended_price(
            item.get("supplier_cost", 0), item.get("scprs_last_price"),
            item.get("source_type", "general"), config)
        item["recommended_price"] = rec
        item["margin_pct"] = margin
        item["pricing_status"] = status
        if rec and status in ("good", "low_margin"):
            item.setdefault("price_per_unit", rec)
    return rfq_data


def print_pricing_summary(rfq_data):
    print(f"\n{'='*80}")
    print(f"{'#':>3} | {'Description':30s} | {'Cost':>8} | {'SCPRS':>8} | {'Bid':>8} | {'Margin':>6} | St")
    print("-"*80)
    tc = tb = 0
    for item in rfq_data.get("line_items", []):
        d = item["description"].split("\n")[0][:30]
        co, s, p = item.get("supplier_cost",0), item.get("scprs_last_price",0), item.get("price_per_unit",0)
        m, st, q = item.get("margin_pct","N/A"), item.get("pricing_status",""), item.get("qty",1)
        icon = {"good":"✅","low_margin":"⚠️","below_cost":"🚫","no_scprs_data":"❓"}.get(st,"")
        print(f"{item['line_number']:3d} | {d:30s} | ${co:7.2f} | ${s:7.2f} | ${p:7.2f} | {m:>5}% | {icon}")
        tc += co*q; tb += p*q
    mg = ((tb-tc)/tb*100) if tb>0 else 0
    print("-"*80)
    print(f"{'TOTALS':>36s} | ${tc:7.2f} |          | ${tb:7.2f} | {mg:5.1f}%")
    print(f"{'PROFIT':>36s} |          |          | ${tb-tc:7.2f}")
    print("="*80)


def generate_bid_package(rfq_data, templates, output_dir, config=None):
    if config is None:
        config = load_config()
    os.makedirs(output_dir, exist_ok=True)
    sol = rfq_data["solicitation_number"]
    rfq_data = apply_pricing_to_rfq(rfq_data, config)
    print_pricing_summary(rfq_data)
    print(f"\nGenerating bid package for #{sol}...")
    fill_703b(templates["703b"], rfq_data, config, f"{output_dir}/{sol}_703B_Reytech.pdf")
    fill_704b(templates["704b"], rfq_data, config, f"{output_dir}/{sol}_704B_Reytech.pdf")
    fill_bid_package(templates["bidpkg"], rfq_data, config, f"{output_dir}/{sol}_BidPackage_Reytech.pdf")
    print(f"\n✅ Complete bid package: {output_dir}/")


if __name__ == "__main__":
    config = load_config()
    sign_date = get_pst_date()
    print(f"Using date: {sign_date} (PST)")

    rfq_data = {
        "solicitation_number": "10838043",
        "release_date": "02/09/2026",
        "due_date": "02/11/2026",
        "sign_date": sign_date,
        "delivery_days": "05",
        "delivery_location": "SCC - Sierra Conservation Center, 5100 O'Byrnes Ferry Road, Jamestown, CA 95327",
        "requestor_name": "Renel Alford",
        "requestor_email": "Renel.Alford@cdcr.ca.gov",
        "requestor_phone": "(916) 691-4767",
        "line_items": [
            {"line_number": 1, "form_row": 1, "qty": 2, "uom": "Set",
             "description": "X-Restraint Full Set w/\n6500-001-401, 6500-001-403,\n6500-001-404, 6500-001-405",
             "item_number": "6500-001-430", "supplier_cost": 312.50, "scprs_last_price": 478.00, "source_type": "medical"},
            {"line_number": 2, "form_row": 4, "qty": 2, "uom": "EA",
             "description": '2" Green Strap for Stryker Chair',
             "item_number": "6250-001-125", "supplier_cost": 42.75, "scprs_last_price": 72.50, "source_type": "general"},
            {"line_number": 3, "form_row": 5, "qty": 2, "uom": "EA",
             "description": '2" Black Belt for Stryker Chair',
             "item_number": "6250-001-126", "supplier_cost": 42.75, "scprs_last_price": 72.50, "source_type": "general"},
        ]
    }
    templates = {
        "703b": "/mnt/user-data/uploads/10838043_AMS_703B_-_RFQ_-_Informal_Competitive_-_Attachment_1.pdf",
        "704b": "/mnt/user-data/uploads/10838043_AMS_704B_-_CCHCS_Acquisition_Quote_Worksheet_-_Attachment_2.pdf",
        "bidpkg": "/mnt/user-data/uploads/10838043_BID_PACKAGE___FORMS__Under_100k___-_Attachment_3.pdf",
    }
    generate_bid_package(rfq_data, templates, "/home/claude/output_v4", config)
