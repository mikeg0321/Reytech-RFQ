


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
    # Bid Package
    "Signature_CUF",       # CUF (MC-345)
    "Signature_darfur",    # Darfur Option #1 ONLY
    "Signature29",         # GSPD-05-105 Bidder Declaration
    "Signature1_PD843",    # DVBE 1st block only
    "708_Signature15",     # GenAI 708
    "Signature_std21",     # STD 21 Drug-Free
}
# NOTE: Signature1 appears in 703B, 704B, and CalRecycle 74 — all get signed.
# NOT signed: Signature2_darfur (Option #2), Signature2/3/4_PD843 (blocks 2-4)

# ── Tight fields (9pt font) ─────────────────────────────────────────
TIGHT_FIELDS = set()
for i in range(1, 16):
    for prefix in ["Row", "QTYRow", "UOMRow", "QTY PER UOMRow", "UNSPSCRow",
                    "ITEM NUMBERRow", "PRICE PER UNITRow", "SUBTOTALRow"]:
        TIGHT_FIELDS.add(f"{prefix}{i}")
TIGHT_FIELDS.add("fill_154")


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
            if ft == "/Sig" and "/Rect" in obj:
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

    bid_exp = ""
    try:
        due = datetime.strptime(rfq_data["due_date"], "%m/%d/%Y")
        bid_exp = (due + timedelta(days=45)).strftime("%m/%d/%Y")
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

    for item in line_items:
        row_num = item.get("form_row", item.get("line_number", 0))
        if not row_num:
            continue
        price = item.get("price_per_unit", 0)
        qty = item.get("qty", 0)
        subtotal = round(price * qty, 2)
        merchandise_subtotal += subtotal
        values[f"PRICE PER UNITRow{row_num}"] = f"{price:.2f}" if price else ""
        values[f"SUBTOTALRow{row_num}"] = f"{subtotal:.2f}" if subtotal else ""

    # Leading space pushes text past the printed "$"
    values["fill_154"] = f" {merchandise_subtotal:.2f}"

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)
    print(f"  ✓ 704B filled + signed — ${merchandise_subtotal:,.2f}")


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

        # CalRecycle 74
        "ContractorCompany Name": company["name"],
        "Address": company["address"], "Phone_2": company["phone"],
        "Print Name": company["owner"], "Title": company["title"],
        "Date": sign_date,
        "Product or Services DescriptionRow2": "All Items",
        "1Percent Postconsumer Recycled Content MaterialRow2": "0",
        "2SABRC Product Category CodeRow2": "0",

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
    }

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)
    print(f"  ✓ Bid Package filled + signed ({sol})")


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
