


#!/usr/bin/env python3
"""
Reytech Bid Package Filler v4
- PST timezone for dates (no more future dates)
- Signature whitelist: only signs where applicable
- Improved horizontal alignment of signature
- Font 11pt default, 9pt for tight 704B grid
- No $ prefix on merchandise subtotal
"""

import json, os, io, re as _re_mod
from datetime import datetime, timezone, timedelta


def _sanitize_for_pdf(text: str) -> str:
    """Replace unicode chars that Helvetica can't render (causes black boxes in PDF).
    Maps smart quotes, em-dashes, bullets, etc. to ASCII equivalents."""
    if not text:
        return text
    replacements = {
        "\u2018": "'", "\u2019": "'",   # smart single quotes
        "\u201c": '"', "\u201d": '"',   # smart double quotes
        "\u2013": "-", "\u2014": "-",   # en-dash, em-dash
        "\u2026": "...",                 # ellipsis
        "\u2022": "*",                   # bullet
        "\u00ae": "(R)", "\u2122": "(TM)",  # registered, trademark
        "\u00a0": " ",                   # non-breaking space
        "\u200b": "",                    # zero-width space
        "\u00b0": "deg",                 # degree symbol
        "\u00d7": "x",                   # multiplication sign
        "\u2032": "'", "\u2033": '"',   # prime, double prime
        "\ufeff": "",                    # BOM
    }
    for char, repl in replacements.items():
        text = text.replace(char, repl)
    # Strip any remaining non-ASCII that Helvetica can't handle
    text = text.encode("ascii", errors="replace").decode("ascii")
    return text


def _sol_display(val: str) -> str:
    """Normalize solicitation number for form fields.
    If missing or 'unknown', display 'RFQ' instead."""
    if not val or val.strip().lower() == "unknown":
        return "RFQ"
    return val


# ═══════════════════════════════════════════════════════════════════════════
# FORM FIELD OWNERSHIP RULES (hard rules — never violate)
# Per buyer feedback: Grace Pfost, CCHCS AMS PS, 2026-03-17
# ═══════════════════════════════════════════════════════════════════════════
#
# AMS 704B (Quote Worksheet) — buyer pre-fills, vendor adds pricing:
#   BUYER FILLS (never overwrite):
#     DEPARTMENT, PHONE/EMAIL, SOLICITATION#, REQUESTOR, DATE,
#     all line item fields (QTY, UOM, DESCRIPTION, #)
#   VENDOR FILLS (we write these):
#     COMPANY NAME, PERSON PROVIDING QUOTE, SIGNATURE DATE,
#     Contract_Number, PRICE PER UNIT, SUBTOTAL columns,
#     MERCHANDISE SUBTOTAL, TAX, TOTAL
#
# AMS 704 (Price Check Response) — when original_mode=True:
#   Same rule as 704B. Only write pricing + vendor info.
#   Ship to: only write if template field is currently empty.
#
# AMS 703B/703C (Bid Response) — Reytech's form, we fill everything.
# Certification forms (CUF, Darfur, DVBE, OBS1600, etc.) — we fill all.
# ═══════════════════════════════════════════════════════════════════════════
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, TextStringObject
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.lib.utils import ImageReader
from PIL import Image

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "reytech_config.json")
SIGNATURE_PATH = os.path.join(SCRIPT_DIR, "signature_transparent.png")


def _normalize_item(item):
    """Normalize item field names for PDF generation.
    Handles both PC format and RFQ format items."""
    n = dict(item)
    n["description"] = (item.get("description")
                        or item.get("desc")
                        or item.get("item_description")
                        or "").strip()
    qty = item.get("qty") or item.get("quantity") or item.get("QTY") or 0
    try:
        n["qty"] = float(str(qty).replace(",", ""))
    except (ValueError, TypeError):
        n["qty"] = 0
    price = (item.get("price_per_unit")
             or item.get("bid_price")
             or item.get("unit_price")
             or item.get("sell_price") or 0)
    try:
        n["price_per_unit"] = float(str(price).replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        n["price_per_unit"] = 0
    cost = (item.get("supplier_cost")
            or item.get("cost")
            or item.get("unit_cost") or 0)
    try:
        n["supplier_cost"] = float(str(cost).replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        n["supplier_cost"] = 0
    n["part_number"] = str(
        item.get("part_number")
        or item.get("item_number")
        or item.get("catalog_number")
        or item.get("mfg_number")
        or "")
    n["uom"] = str(
        item.get("uom")
        or item.get("UOM")
        or item.get("unit_of_measure")
        or "EA")
    n["line_number"] = item.get("line_number") or item.get("#") or 0
    return n

# ── Signature whitelist ──────────────────────────────────────────────
# Only these /Sig fields get the signature image. Everything else stays blank.
SIGN_FIELDS = {
    # 703B — field name varies by template version
    "Signature1",          # AMS 703B (most versions) + 704B Vendor Sig + CalRecycle 74
    "Bidder Signature",    # AMS 703B Rev. 03/2025 alternate name
    "703B_Bidder Signature",  # prefixed variant
    "BidderSignature",     # no-space variant
    # Standalone forms
    "Signature",           # CalRecycle 74 standalone + STD 1000 standalone
    "Signature3",          # STD 205 Payee Data Record Supplement
    "Signature4",          # STD 204 Payee Data Record
    # Bid Package
    "Signature_CUF",       # CUF (MC-345)
    "Signature_darfur",    # Darfur Option #1 (legacy name)
    "Authorized Signature",  # Darfur Act DGS PD 1 actual /Sig field name
    "Signature29",         # GSPD-05-105 Bidder Declaration
    "Signature1_PD843",    # DVBE 1st block only
    "DVBEowner1signature", # DVBE 843 unlocked template owner sig
    "708_Signature15",     # GenAI 708
    "Signature_std21",     # STD 21 Drug-Free
    "OBS 1600 Signature",  # OBS 1600 Food Cert — text field but needs signature image
    "AuthorizedRepresentative[0]",  # CV 012 CUF page 2 authorizing signature
}
# RULE: When adding a new form, identify its exact signature field name from the PDF
# and add it here. Use: python3 -c "from pypdf import PdfReader; ..."
# to dump field names before writing any filler code.
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

# STD 204 — tight fields
TIGHT_FIELDS.add("Federal Employer Identification Number (FEIN)")
TIGHT_FIELDS.add("EMAIL ADDRESS_2")
TIGHT_FIELDS.add("EMAIL ADDRESS")
TIGHT_FIELDS.add("CITY STATE ZIP CODE")

# CalRecycle Date field — only 73pt wide
TIGHT_FIELDS.add("Date")
# 703B date fields — narrow in template
TIGHT_FIELDS.add("703B_Sign_Date")
TIGHT_FIELDS.add("703B_Release Date")
TIGHT_FIELDS.add("703B_Due Date")
TIGHT_FIELDS.add("703B_BidExpirationDate")
TIGHT_FIELDS.add("703B_Certification Expiration Date")
# Bid Package date fields
TIGHT_FIELDS.add("Date_CUF")
TIGHT_FIELDS.add("Date__darfur")
TIGHT_FIELDS.add("Date1_PD843")
# PD 843 DVBE fields — name/desc/solicitation get clipped at default 11pt
TIGHT_FIELDS.add("DVBEname")
TIGHT_FIELDS.add("description")
TIGHT_FIELDS.add("SCno")
TIGHT_FIELDS.add("Text1_PD843")
TIGHT_FIELDS.add("Text3_PD843")
TIGHT_FIELDS.add("Text4_PD843")
TIGHT_FIELDS.add("Date_PD802")
TIGHT_FIELDS.add("708_Text16")
# Generic date fields that may appear in newer templates
for _df in ("Sign Date", "SignDate", "DateSigned", "Date Signed", "Expiration Date"):
    TIGHT_FIELDS.add(_df)


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def pst_now() -> datetime:
    """Current time in US/Pacific (handles PST/PDT automatically)."""
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/Los_Angeles"))


def get_pst_date():
    """Get current date in Pacific time (PST/PDT aware)."""
    return pst_now().strftime("%m/%d/%Y")


def set_field_fonts(writer, field_values, default_size=11, tight_size=9):
    """Set font sizes and FORCE appearance regeneration for clean text rendering."""
    import re as _re_sff
    # Sanitize all field values before writing to PDF
    _clean = {}
    for _k, _v in field_values.items():
        if isinstance(_v, str):
            _v = _re_sff.sub(r'[^\x20-\x7E\n]', '', _v)
            _v = _re_sff.sub(r'/{2,}', '/', _v)
            _v = _re_sff.sub(r'[ \t]{2,}', ' ', _v).strip()
        _clean[_k] = _v
    field_values = _clean
    da_default = f"/Helv {default_size} Tf 0 g"
    
    # Conservative character widths at different font sizes (Helvetica, mixed text).
    # Using 0.6em as average glyph advance — wider than naive estimate to avoid clipping.
    CHAR_WIDTH = {5: 3.1, 6: 3.7, 7: 4.3, 8: 4.9, 9: 5.5, 10: 6.1, 11: 6.7}
    
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
            
            content = str(field_values.get(name, ""))
            rect = obj.get("/Rect")
            field_w = float(rect[2]) - float(rect[0]) if rect else 100
            
            if name in TIGHT_FIELDS or (content and len(content) * CHAR_WIDTH.get(default_size, 6.1) > field_w - 4):
                # Auto-size: fit content to field width
                font_sz = tight_size
                for try_sz in [tight_size, 8, 7, 6, 5]:
                    est_width = len(content) * CHAR_WIDTH.get(try_sz, 5.0)
                    if est_width < field_w - 4:
                        font_sz = try_sz
                        break
                    font_sz = try_sz
                
                obj[NameObject("/DA")] = TextStringObject(f"/Helv {font_sz} Tf 0 g")
            else:
                obj[NameObject("/DA")] = TextStringObject(da_default)
            
            # CRITICAL: Remove existing appearance stream so PDF viewer regenerates it
            # This fixes the letter spacing issue — old AP has wrong character widths
            if "/AP" in obj:
                del obj[NameObject("/AP")]


def create_signature_overlay(sig_entries, page_width, page_height, sig_image_path, sign_date=None):
    """
    Create PDF overlay with signature images.
    sig_entries: list of (name, [left, bottom, right, top], is_sig_field)
    /Sig fields always get signed; text fields need SIGN_FIELDS whitelist.
    """
    packet = io.BytesIO()
    c = rl_canvas.Canvas(packet, pagesize=(page_width, page_height))

    if not os.path.exists(sig_image_path):
        c.save(); packet.seek(0); return packet

    sig_img = Image.open(sig_image_path)
    img_reader = ImageReader(sig_img)
    img_w, img_h = sig_img.size
    aspect = img_w / img_h

    for entry in sig_entries:
        # Support both old (name, rect) and new (name, rect, is_sig_field) tuples
        if len(entry) == 3:
            name, rect, is_sig_field = entry
        else:
            name, rect = entry
            is_sig_field = False
        # Text fields need whitelist; /Sig fields always sign
        if not is_sig_field and name not in SIGN_FIELDS:
            continue

        field_w = rect[2] - rect[0]
        field_h = rect[3] - rect[1]

        # Detect vertical field (GSPD-05-105 Signature29 is 32×303)
        is_vertical = field_h > field_w * 2

        if is_vertical:
            draw_h = 24
            draw_w = draw_h * aspect
            if draw_w > 160:
                draw_w = 160
                draw_h = draw_w / aspect
            x = rect[0] - draw_w + 10
            y = rect[1] + 2
            c.drawImage(img_reader, x, y, draw_w, draw_h, mask='auto')
            continue

        # ── Horizontal signature ──
        # Combo field: signature + date side by side (narrow fields).
        # EXCLUDE: 708_Signature15 (708_Text16 is the Date field)
        # EXCLUDE: PD843 signatures (Date1/2/3/4_PD843 are separate text fields)
        # EXCLUDE: Signature1 (CalRecycle 74 — Date is the unnamed field at x≈505)
        is_separate_date_field = (
            "708_Signature" in name or
            "_PD843" in name or
            "DVBEowner" in name or  # DVBE 843 has separate date fields
            "_CUF" in name or       # CUF has Date_CUF
            "_darfur" in name or    # Darfur has Date__darfur
            "_std21" in name or     # STD 21 has date fields
            "_PD802" in name or     # PD 802 has Date_PD802
            name == "Signature1" or
            name == "Signature29"   # Bidder Dec — no combo date
        )
        # Don't draw date next to signature if there's a separate Date field on the same page
        # (CalRecycle 74 has both Signature1 and Date fields — _calrecycle_fix_date handles Date)
        _is_calrecycle_sig = (name == "Signature1" or name == "Signature")
        # NEVER draw combo dates — all forms have separate date fields.
        # Dates are filled by fill_bid_package/fill_703b/fill_703c values dict.
        has_room_for_date = False

        # PRIMARY: size by width (fill the signature line)
        # Real signatures span most of the line, not a tiny portion
        if has_room_for_date:
            draw_w = field_w * 0.55  # Leave room for date
        else:
            draw_w = field_w * 0.75  # Fill 75% of the line
        
        draw_h = draw_w / aspect
        
        # Cap: don't overflow more than 1.5x field height (avoids overlapping other fields)
        if draw_h > field_h * 1.5:
            draw_h = field_h * 1.5
            draw_w = draw_h * aspect
        
        # Minimum: never smaller than 20pt tall
        if draw_h < 20:
            draw_h = 20
            draw_w = draw_h * aspect

        # Position: left-aligned, centered on the signature line
        # Let it overflow above the field slightly (natural look)
        x = rect[0] + 3
        y = rect[1] + (field_h - draw_h) / 2  # Centered, may go above field

        c.drawImage(img_reader, x, y, draw_w, draw_h, mask='auto')

        # Draw date next to sig in combo fields
        if has_room_for_date:
            date_x = x + draw_w + 8
            date_y = rect[1] + (field_h / 2) - 5
            c.setFont("Helvetica", 10)
            c.setFillColorRGB(0, 0, 0)
            c.drawString(date_x, date_y, sign_date)

    c.save()
    packet.seek(0)
    return packet


def fill_and_sign_pdf(input_path, field_values, output_path,
                       default_font=11, tight_font=9, sig_image=None, sign_date=None):
    import os as _os
    if not _os.path.exists(input_path):
        raise FileNotFoundError(f"Template PDF not found: {input_path}")
    _os.makedirs(_os.path.dirname(output_path) or ".", exist_ok=True)
    try:
        reader = PdfReader(input_path)
    except Exception as e:
        raise ValueError(f"Cannot read template PDF {_os.path.basename(input_path)}: {e}") from e
    writer = PdfWriter()
    writer.append(reader)

    # NOTE: Do NOT call transfer_rotation_to_content() here.
    # Annotation /Rect coordinates are in the page's native (pre-rotation) space.
    # transfer_rotation_to_content() transforms the content stream but does NOT
    # update /Rect values, causing all field values to render at wrong positions.
    # PDF viewers apply /Rotate to the whole page uniformly — leave it intact.

    clean_values = {k: (_sanitize_for_pdf(v) if isinstance(v, str) else v)
                     for k, v in field_values.items() if v is not None}

    # ── Pre-fill validation: catch field mismatches BEFORE writing ──
    # Uses TemplateProfile to compare intended fields against actual template fields.
    # This turns silent failures into explicit warnings.
    try:
        from src.forms.template_registry import get_profile
        _pre_profile = get_profile(input_path)
        _pre_unmatched = _pre_profile.validate_mapping(clean_values)
        if _pre_unmatched:
            import logging as _pflog
            _pflog.getLogger("reytech.forms").warning(
                "fill_and_sign_pdf PRE-FILL: %d/%d field names not in template %s: %s",
                len(_pre_unmatched), len(clean_values),
                _os.path.basename(input_path), _pre_unmatched[:15])
    except Exception:
        pass  # TemplateProfile is best-effort — never block filling

    # Convert bool checkbox values to PDF format and track unchecked fields
    _original_clean = dict(clean_values)
    for k, v in list(clean_values.items()):
        if v is True:
            clean_values[k] = "/Yes"
        elif v is False:
            clean_values[k] = "/Off"

    set_field_fonts(writer, clean_values, default_font, tight_font)

    for page in writer.pages:
        try:
            # Rotated pages (e.g. GSPD-05-105 Bidder Declaration with /Rotate=90):
            # auto_regenerate=True generates appearance streams in pre-rotation space,
            # causing text to render at wrong angles and bleed through page content.
            # Use auto_regenerate=False — PDF viewers apply /Rotate uniformly and
            # will render field values correctly from the /V entry.
            page_rotate = int(page.get("/Rotate", 0))
            use_auto_regen = (page_rotate == 0)
            writer.update_page_form_field_values(page, clean_values, auto_regenerate=use_auto_regen)
        except Exception:
            try:
                writer.update_page_form_field_values(page, clean_values, auto_regenerate=False)
            except Exception:
                pass

    sig_path = sig_image or SIGNATURE_PATH
    for page in writer.pages:
        if "/Annots" not in page:
            continue
        sig_entries = []
        _page_mb = page.get("/MediaBox", [0, 0, 612, 792])
        _page_h = float(_page_mb[3])
        for annot in page["/Annots"]:
            obj = annot.get_object()
            ft = str(obj.get("/FT", ""))
            name = str(obj.get("/T", ""))
            if "/Rect" not in obj:
                continue
            if name not in SIGN_FIELDS:
                continue
            try:
                r = [float(x) for x in obj["/Rect"]]
            except Exception:
                continue
            # For generic field names (Signature1, Signature), only sign if
            # the field is in the lower 40% of the page — signature lines
            # are always near the bottom, never in the header/body area.
            # Skip Signature29 (Bidder Declaration GSPD-05-105) — rotated form,
            # coordinates don't map correctly. The 703C signature covers this.
            if name == "Signature29":
                continue
            if name in ("Signature1", "Signature"):
                field_y = r[1]  # bottom of field rect
                if field_y > _page_h * 0.4:
                    continue
            is_sig = ft == "/Sig"
            sig_entries.append((name, r, is_sig))

        if sig_entries:
            mediabox = page.get("/MediaBox", [0, 0, 612, 792])
            pw, ph = float(mediabox[2]), float(mediabox[3])
            overlay_buf = create_signature_overlay(sig_entries, pw, ph, sig_path, sign_date)
            overlay_reader = PdfReader(overlay_buf)
            if overlay_reader.pages:
                page.merge_page(overlay_reader.pages[0])

    # Force-clear checkboxes that should be unchecked
    _unchecked = [k for k, v in _original_clean.items() if v is False or v == "/Off"]
    if _unchecked:
        for page in writer.pages:
            if "/Annots" in page:
                for annot in page["/Annots"]:
                    annot_obj = annot.get_object()
                    field_name = annot_obj.get("/T", "")
                    if isinstance(field_name, str) and field_name in _unchecked:
                        annot_obj.update({
                            NameObject("/V"): NameObject("/Off"),
                            NameObject("/AS"): NameObject("/Off"),
                        })

    with open(output_path, "wb") as f:
        writer.write(f)

    # Post-fill verification: read back and log unmatched fields
    try:
        verify_reader = PdfReader(output_path)
        actual_fields = verify_reader.get_fields() or {}
        intended_keys = set(clean_values.keys())
        actual_keys = set(actual_fields.keys())
        unmatched = intended_keys - actual_keys
        if unmatched:
            import logging as _vlog
            _vlog.getLogger("reytech.forms").warning(
                "fill_and_sign_pdf: %d/%d intended fields not found in output: %s",
                len(unmatched), len(intended_keys), sorted(unmatched)[:10])
    except Exception:
        pass  # verification is best-effort, never block output


# ═══════════════════════════════════════════════════════════════════════
# Form Fillers
# ═══════════════════════════════════════════════════════════════════════

def fill_703c(input_path, rfq_data, config, output_path):
    """Fill AMS 703C Rev 03/2025 (Fair and Reasonable / Exempt).
    Field names use '703C_' prefix. Verified from actual PDF field dump."""
    from pypdf import PdfReader as _PR703c
    reader = _PR703c(input_path)
    fields = reader.get_fields() or {}
    field_names = set(fields.keys())

    has_703b_prefix = any(f.startswith("703B_") for f in field_names)
    has_703c_prefix = any(f.startswith("703C_") for f in field_names)

    if has_703b_prefix and not has_703c_prefix:
        return fill_703b(input_path, rfq_data, config, output_path)

    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())
    try:
        gen_date = datetime.strptime(sign_date, "%m/%d/%Y")
    except Exception:
        gen_date = datetime.now()
    bid_exp = (gen_date + timedelta(days=45)).strftime("%m/%d/%Y")

    p = "703C_" if has_703c_prefix else ""

    values = {
        # Company info
        f"{p}Business Name": company["name"],
        f"{p}Address": company["address"],
        f"{p}Contact Person": company["owner"],
        f"{p}Title": company["title"],
        f"{p}Phone": company["phone"],
        f"{p}Email": company["email"],
        f"{p}Federal Employer Identification Number FEIN": company["fein"],
        f"{p}Retailers CA Sellers Permit Number": company["sellers_permit"],
        f"{p}SBMBDVBE Certification.0": company["cert_number"],
        f"{p}Certification Expiration Date": company["cert_expiration"],
        f"{p}BidExpirationDate": bid_exp,
        f"{p}Date": sign_date,
        # Solicitation
        f"{p}Solicitation Number": _sol_display(rfq_data.get("solicitation_number", "")),
        f"{p}Release Date": rfq_data.get("release_date", ""),
        f"{p}Due Date": rfq_data.get("due_date", ""),
        f"{p}Deliveries must be completed within": rfq_data.get("delivery_days", "30"),
        # Payment discount
        f"{p}Payment discount offered on invoices to be paid within": "N/A",
        f"{p}days of receipt": "0",
        # Requestor
        f"{p}Name": rfq_data.get("requestor_name", ""),
        f"{p}Email_2": rfq_data.get("requestor_email", ""),
        f"{p}Phone_2": rfq_data.get("requestor_phone", ""),
        # Checkboxes — manufacturer: No, non-small business: No
        f"{p}Check Box2": "/Yes",  # Manufacturer: No
        f"{p}Check Box4": "/Yes",  # Non-small business claiming: No
        f"{p}Check Box5": "/Yes",
        f"{p}Check Box7": "/Yes",
        # Response list checkboxes (page 8)
        f"{p}ResponseList.0": "/Yes", f"{p}ResponseList.1": "/Yes",
        f"{p}ResponseList.2": "/Yes", f"{p}ResponseList.3": "/Yes",
        f"{p}ResponseList.4": "/Yes", f"{p}ResponseList.5": "/Yes",
        f"{p}ResponseList.6": "/Yes", f"{p}ResponseList.7": "/Yes",
        f"{p}ResponseList.8": "/Yes", f"{p}ResponseList.9": "/Yes",
        f"{p}ResponseList.14": "/Yes",
        f"{p}ResponseList.16": "/Yes",
    }

    if rfq_data.get("delivery_location"):
        values[f"{p}Dropdown2"] = rfq_data["delivery_location"]

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)
    # 703C has Signature1 as /Sig — fill_and_sign_pdf handles it. No overlay needed.
    print(f"  ✓ 703C filled + signed ({sign_date})")


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
        "703B_Solicitation Number": _sol_display(rfq_data.get("solicitation_number", "")),
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

    # Check if template has /Sig fields (if so, fill_and_sign_pdf handles signature)
    _reader_check = PdfReader(input_path)
    _has_sig_field = False
    for _pg in _reader_check.pages:
        if "/Annots" in _pg:
            for _ann in _pg["/Annots"]:
                _obj = _ann.get_object()
                if str(_obj.get("/FT", "")) == "/Sig" and str(_obj.get("/T", "")) in SIGN_FIELDS:
                    _has_sig_field = True
                    break

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)

    # Only overlay positional signature if the template has NO /Sig field
    # (703B Rev 03/2025 uses a printed line, not a form field)
    if not _has_sig_field:
        try:
            _703b_overlay_signature(output_path, sign_date)
        except Exception as _se:
            print(f"  ⚠ 703B positional sig overlay failed: {_se}")

    print(f"  ✓ 703B filled + signed ({sign_date})")


def _703b_overlay_signature(pdf_path, sign_date):
    """
    Overlay signature image at the 'Bidder Signature' line in AMS 703B.
    The template has no /Sig field — the line is just a printed 'X___' line.
    Uses pdfminer to find the exact Y position of the 'Bidder Signature' label.
    """
    from pypdf import PdfReader as _PR, PdfWriter as _PW
    import io as _io
    from reportlab.pdfgen import canvas as _rl
    from reportlab.lib.utils import ImageReader as _IR
    from PIL import Image as _Img

    if not os.path.exists(SIGNATURE_PATH):
        print("  ⚠ 703B sig: signature image not found")
        return

    reader = _PR(pdf_path)

    # Find page with "Bidder Signature" label — scan all pages, use last match
    target_page = 0
    for i, pg in enumerate(reader.pages):
        txt = (pg.extract_text() or "").lower()
        if "bidder signature" in txt:
            target_page = i  # keep scanning — sig page is usually last match

    page = reader.pages[target_page]
    mb = page.get("/MediaBox", [0, 0, 612, 792])
    pw, ph = float(mb[2]), float(mb[3])

    # Try pdfminer to get exact Y position of "Bidder Signature" text
    sig_y = None
    try:
        from pdfminer.high_level import extract_pages
        from pdfminer.layout import LTTextBox, LTTextLine, LTChar
        import pdfminer.high_level as _pmhl
        from io import BytesIO as _BIO
        for page_layout in extract_pages(pdf_path, page_numbers=[target_page]):
            for element in page_layout:
                if not isinstance(element, LTTextBox):
                    continue
                for line in element:
                    if not isinstance(line, LTTextLine):
                        continue
                    txt = line.get_text().lower().strip()
                    if "bidder signature" in txt:
                        # y0 is the bottom of the text line
                        sig_y = line.y0
                        break
                if sig_y is not None:
                    break
        if sig_y is not None:
            print(f"  ✓ 703B sig line found at y={sig_y:.1f} (page {target_page})")
    except Exception as _pme:
        print(f"  ℹ 703B pdfminer scan skipped ({_pme}) — using fallback y")

    # Fallback positions to try if pdfminer unavailable
    if sig_y is None:
        # AMS 703B Rev 03/2025: sig line is ~87pt from bottom on an 8.5x11 page
        # Try proportional: ~11% from bottom
        sig_y = ph * 0.11

    # Signature sits ABOVE the "Bidder Signature" label.
    # pdfminer returns the bottom of the label text line.
    # Add label height (~10pt) + spacing (~8pt) to place sig above the label.
    y = sig_y + 18

    sig_img = _Img.open(SIGNATURE_PATH)
    ir = _IR(sig_img)
    iw, ih = sig_img.size
    aspect = iw / ih
    draw_w = 160
    draw_h = draw_w / aspect

    packet = _io.BytesIO()
    c = _rl.Canvas(packet, pagesize=(pw, ph))
    c.drawImage(ir, 38, y, draw_w, draw_h, mask="auto")
    # Do NOT draw date here — 703B_Sign_Date form field already fills the Date box on the right
    c.save()
    packet.seek(0)

    from pypdf import PdfReader as _PR2, PdfWriter as _PW2
    overlay = _PR2(packet)
    writer = _PW2()
    writer.append(reader)
    writer.pages[target_page].merge_page(overlay.pages[0])
    with open(pdf_path, "wb") as _f:
        writer.write(_f)
    print(f"  ✓ 703B sig overlay applied (page {target_page}, y={y:.1f})")


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

    # ── Template introspection via TemplateProfile (single source of truth) ──
    from src.forms.template_registry import get_profile
    from src.forms.ams704_helpers import normalize_line_item, build_row_field_name
    _profile = get_profile(input_path)

    def _row_field(slot):
        """Return the Row field-name suffix for a 1-based sequential slot."""
        r = _profile.row_field_suffix(slot)
        if r is not None:
            return r
        p2_slot = slot - _profile.pg1_row_count
        return f"Row{p2_slot}_2"

    _is_prefilled = _profile.is_prefilled
    _prefilled_item_rows = dict(_profile.prefilled_item_rows)
    print(f"  704B layout (TemplateProfile): pg0={_profile.pg1_row_count} rows, "
          f"pg1={len(_profile.pg2_rows_suffixed)}_2+{len(_profile.pg2_rows_plain)} plain")
    if _is_prefilled:
        print(f"  704B: agency pre-filled detected ({len(_prefilled_item_rows)} item rows: {_prefilled_item_rows})")

    # Fix duplicate line numbers at generation time only (does not save back)
    for _i, _item in enumerate(line_items, start=1):
        _item["line_number"] = _i

    seq = 0
    for _raw_item in line_items:
        seq += 1
        item = normalize_line_item(_raw_item)
        price = item["price_per_unit"]
        qty = item["qty"]
        subtotal = round(price * qty, 2)
        merchandise_subtotal += subtotal

        if _is_prefilled:
            # Agency pre-filled: ONLY write price + subtotal to the correct row
            item_num = item.get("line_number") or seq
            if item_num in _prefilled_item_rows:
                r = _prefilled_item_rows[item_num]
            else:
                r = _row_field(seq)
            values[f"PRICE PER UNIT{r}"] = f"{price:.2f}" if price else ""
            values[f"SUBTOTAL{r}"] = f"{subtotal:.2f}" if subtotal else ""
        else:
            # Fresh template: write everything
            r = _row_field(seq)
            values[f"PRICE PER UNIT{r}"] = f"{price:.2f}" if price else ""
            values[f"SUBTOTAL{r}"] = f"{subtotal:.2f}" if subtotal else ""
            values[f"ITEM NUMBER{r}"] = item["part_number"]
            values[f"QTY{r}"] = str(qty) if qty else ""
            values[f"UOM{r}"] = item["uom"]
            values[f"ITEM DESCRIPTION PRODUCT SPECIFICATION{r}"] = item["description"]
            values[f"#{r}"] = str(seq)
            sub_field = f"SUBSTITUTED ITEM Include manufacturer part number andor reference number{r}"
            if item.get("is_substitute"):
                mfg = item.get("mfg_number", "")
                values[sub_field] = f"{item['description']} (MFG# {mfg})" if mfg else item["description"]
            else:
                values[sub_field] = ""

    # ═══ VENDOR FIELDS ONLY — buyer header fields are NEVER overwritten ═══
    # Buyer fills: DEPARTMENT, PHONEEMAIL, SOLICITATION#, REQUESTOR, DATE
    # See FORM FIELD OWNERSHIP RULES at top of file.
    for sfx in ("", "_2"):
        # Vendor identification
        values[f"COMPANY NAME{sfx}"] = company["name"]
        values[f"Company Name{sfx}"] = company["name"]
        values[f"Vendor Name{sfx}"] = company["name"]
        values[f"PERSON PROVIDING QUOTE{sfx}"] = company["owner"]
        values[f"Person Providing Quote{sfx}"] = company["owner"]
        # Contract reference (vendor's contract number)
        values[f"Contract_Number{sfx}"] = _sol_display(rfq_data.get("solicitation_number", ""))
        # Vendor signature date (NOT the buyer's DATE field)
        values[f"SIGNATURE DATE{sfx}"] = sign_date
        values[f"Signature Date{sfx}"] = sign_date

    # Log all field names in this template for diagnostics (via TemplateProfile)
    print(f"  704B template fields ({len(_profile.field_names)}): {sorted(_profile.field_names)[:20]}")

    # Pre-fill validation: warn about field names we're about to write that don't exist
    _unmatched = _profile.validate_mapping(values)
    if _unmatched:
        print(f"  \u26a0 704B: {len(_unmatched)} field names not in template (will be ignored): {_unmatched[:10]}")

    # Leading space pushes text past the printed "$"
    values["fill_154"] = f" {merchandise_subtotal:.2f}"
    values["fill_154_2"] = f" {merchandise_subtotal:.2f}"

    # Fill all pages of the template first (needed to write to _2 fields on page 2)
    import tempfile, os as _os
    tmp_path = output_path + ".tmp704b.pdf"
    fill_and_sign_pdf(input_path, values, tmp_path, sign_date=sign_date)

    # Some CCHCS combined templates embed the 703B form as page 0 of the 704B file.
    # Detection is cached in TemplateProfile — no need to re-scan the filled PDF.
    try:
        from pypdf import PdfReader as _PR, PdfWriter as _PW
        _reader = _PR(tmp_path)

        if _profile.has_embedded_703b:
            _writer = _PW()
            for _pg in _reader.pages[1:]:
                _writer.add_page(_pg)
            with open(output_path, "wb") as _f:
                _writer.write(_f)
            print(f"  ℹ 704B: trimmed embedded 703B from page 0")
        else:
            import shutil as _sh
            _sh.copy2(tmp_path, output_path)
    except Exception as _trim_err:
        import shutil as _sh
        _sh.copy2(tmp_path, output_path)
        print(f"  ⚠ 704B trim check failed ({_trim_err}) — keeping all pages")
    finally:
        try:
            _os.remove(tmp_path)
        except Exception:
            pass

    # ── 704B signature overlay — "SIGNATURE / DATE" is printed text, not a form field ──
    try:
        from pypdf import PdfReader as _PR704s, PdfWriter as _PW704s
        _reader_sig = _PR704s(output_path)
        _writer_sig = _PW704s()
        _writer_sig.append(_reader_sig)
        _signed_704b = False
        for _pg_idx, _pg in enumerate(_reader_sig.pages):
            _txt = (_pg.extract_text() or "").upper()
            if "VENDOR INFORMATION" in _txt and "COMPANY NAME" in _txt:
                # This is the 704B vendor page — overlay signature at SIGNATURE/DATE area
                _mb = _pg.get("/MediaBox", [0, 0, 612, 792])
                _pw, _ph = float(_mb[2]), float(_mb[3])
                # SIGNATURE / DATE is in the VENDOR INFORMATION header row, right column
                # Standard position: right third of vendor header, y near top of item table
                import io as _io704
                from reportlab.pdfgen import canvas as _rl704
                _packet = _io704.BytesIO()
                _c = _rl704.Canvas(_packet, pagesize=(_pw, _ph))
                if os.path.exists(SIGNATURE_PATH):
                    from reportlab.lib.utils import ImageReader as _IR704
                    from PIL import Image as _Img704
                    _sig = _Img704.open(SIGNATURE_PATH)
                    _ir = _IR704(_sig)
                    # Find "SIGNATURE" text Y position using pdfminer
                    _sig_y = None
                    try:
                        from pdfminer.high_level import extract_pages as _ep704
                        from pdfminer.layout import LTTextBox, LTTextLine
                        for _layout in _ep704(output_path, page_numbers=[_pg_idx]):
                            for _elem in _layout:
                                if not isinstance(_elem, LTTextBox): continue
                                for _line in _elem:
                                    if not isinstance(_line, LTTextLine): continue
                                    if "SIGNATURE" in _line.get_text().upper() and "DATE" in _line.get_text().upper():
                                        _sig_y = _line.y0
                                        _sig_x = _line.x0
                                        break
                                if _sig_y: break
                    except Exception:
                        pass
                    if _sig_y:
                        # Draw signature BELOW the SIGNATURE/DATE label (in the cell)
                        _draw_h = 22
                        _draw_w = _draw_h * (_sig.size[0] / _sig.size[1])
                        _c.drawImage(_ir, _sig_x + 2, _sig_y - _draw_h - 2, _draw_w, _draw_h, mask='auto')
                        # Draw date to the right of signature
                        _c.setFont("Helvetica", 10)
                        _c.drawString(_sig_x + _draw_w + 12, _sig_y - _draw_h + 4, sign_date)
                        _signed_704b = True
                _c.save()
                _packet.seek(0)
                if _signed_704b:
                    _overlay = _PR704s(_packet)
                    _writer_sig.pages[_pg_idx].merge_page(_overlay.pages[0])
                break
        if _signed_704b:
            with open(output_path, "wb") as _f704:
                _writer_sig.write(_f704)
            print(f"  ✓ 704B signature overlaid at SIGNATURE/DATE")
        else:
            print(f"  ⚠ 704B: could not locate SIGNATURE/DATE area")
    except Exception as _704sig_e:
        print(f"  ⚠ 704B signature overlay failed: {_704sig_e}")

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
        except Exception: items = []
    
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
    sol = _sol_display(rfq_data.get("solicitation_number", ""))
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
    sol = _sol_display(rfq_data.get("solicitation_number", ""))
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
    """Overlay line items into the STD 1000 'Contract / Description of Purchase' field.
    Uses two-column layout when items exceed single-column capacity."""
    from reportlab.lib.pagesizes import letter

    # Field rect from template: [16, 346, 598, 548]
    x_start = 20
    y_top = 540
    total_width = 570
    box_height = 185

    # Build lines
    lines = []
    for i, item in enumerate(items, 1):
        pn = item.get("item_number", item.get("part_number", ""))
        desc = item.get("description", "")
        qty = item.get("qty", 1)
        uom = item.get("uom", "EA")
        # Clean up description — strip refs, model#, UPC for brevity
        if " - " in desc and len(desc) > 80:
            desc = desc.split(" - ")[0].strip()
        for m in ["(R)", "(TM)", "®", "™"]:
            desc = desc.replace(m, "")
        desc = desc.rstrip(" -")
        # Sanitize unicode chars that Helvetica can't render (causes black boxes)
        desc = _sanitize_for_pdf(desc)
        pn = _sanitize_for_pdf(pn)
        lines.append(f"{i}. {pn}, {qty} {uom} - {desc}")
    if not lines:
        lines = ["N/A"]

    font_name = "Helvetica"

    # Single column: ≤15 items at 10pt
    # Two columns: >15 items
    if len(lines) <= 15:
        font_size = 10
        leading = 12
        cols = 1
    else:
        # Two columns — pick font to fit half the lines per column
        lines_per_col = (len(lines) + 1) // 2  # ceil division
        font_size = 9
        leading = 11
        # Scale down if needed
        for sz, ld in [(9, 11), (8, 10), (7.5, 9.5), (7, 9), (6.5, 8)]:
            if lines_per_col * ld <= box_height - 4:
                font_size, leading = sz, ld
                break
        cols = 2

    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=letter)

    if cols == 1:
        col_width = total_width
        text_obj = c.beginText(x_start, y_top)
        text_obj.setFont(font_name, font_size)
        text_obj.setLeading(leading)
        for line in lines:
            while c.stringWidth(line, font_name, font_size) > col_width and len(line) > 20:
                line = line[:len(line) - 4] + "..."
            text_obj.textLine(line)
        c.drawText(text_obj)
    else:
        col_gap = 12
        col_width = (total_width - col_gap) / 2
        mid = (len(lines) + 1) // 2
        col1_lines = lines[:mid]
        col2_lines = lines[mid:]

        # Left column
        t1 = c.beginText(x_start, y_top)
        t1.setFont(font_name, font_size)
        t1.setLeading(leading)
        for line in col1_lines:
            while c.stringWidth(line, font_name, font_size) > col_width and len(line) > 20:
                line = line[:len(line) - 4] + "..."
            t1.textLine(line)
        c.drawText(t1)

        # Right column
        x2 = x_start + col_width + col_gap
        t2 = c.beginText(x2, y_top)
        t2.setFont(font_name, font_size)
        t2.setLeading(leading)
        for line in col2_lines:
            while c.stringWidth(line, font_name, font_size) > col_width and len(line) > 20:
                line = line[:len(line) - 4] + "..."
            t2.textLine(line)
        c.drawText(t2)

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
        # Section 3 — FEIN: leave blank, overlay digits instead
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

    # Overlay FEIN digits at correct positions (field has individual underlines)
    _overlay_std204_fein(output_path, company["fein"])

    print(f"  ✓ STD 204 Payee Data Record filled")


def _overlay_std204_fein(pdf_path, fein, page_index=0):
    """Overlay FEIN digits at spaced positions matching the underline marks on STD 204.
    FEIN format: XX-XXXXXXX (9 digits with dash)
    Field rect: [398.7, 366.8, 584.5, 383.2]
    """
    from reportlab.lib.pagesizes import letter

    # Extract just digits
    digits = [c for c in fein if c.isdigit()]
    if len(digits) != 9:
        return

    # Field position on page
    field_left = 398.7
    field_bottom = 366.8
    field_width = 185.8
    field_height = 16.4

    # 9 digit positions + dash = 10 slots across the field
    # Format: [d1] [d2] [-] [d3] [d4] [d5] [d6] [d7] [d8] [d9]
    slot_width = field_width / 10.0  # ~18.6pt per slot
    y_pos = field_bottom + 3  # baseline offset from bottom

    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=letter)
    c.setFont("Helvetica", 12)

    # Draw each digit centered in its slot
    # Slots: 0=d1, 1=d2, 2=dash, 3..9=d3..d9
    slot_map = [0, 1, 3, 4, 5, 6, 7, 8, 9]  # skip slot 2 (dash)
    for i, digit in enumerate(digits):
        slot = slot_map[i]
        x = field_left + (slot + 0.5) * slot_width
        c.drawCentredString(x, y_pos, digit)

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
    sol = _sol_display(rfq_data.get("solicitation_number", ""))
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
# STD 205 — Payee Data Record Supplement (ReportLab)
# ═══════════════════════════════════════════════════════════════════════

def generate_std205(rfq_data, config, output_path):
    """Generate STD 205 Payee Data Record Supplement via ReportLab."""
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())

    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.lib import colors

    c = rl_canvas.Canvas(output_path, pagesize=letter)
    w, h = letter
    margin = 0.75 * inch

    # Header
    y = h - 0.6 * inch
    c.setFont("Helvetica", 8)
    c.drawString(margin, y, "STATE OF CALIFORNIA – STATE CONTROLLERS OFFICE")
    y -= 11
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin, y, "PAYEE DATA RECORD SUPPLEMENT")
    y -= 11
    c.setFont("Helvetica", 7)
    c.drawString(margin, y, "(This form is optional. Form is used to provide remittance address information if different than the mailing address on the STD 204.)")
    y -= 9
    c.drawString(margin, y, "STD 205 (New 03/2021)")

    # Payee Information section
    y -= 24
    c.setStrokeColor(colors.black)
    c.setLineWidth(0.8)
    c.setFont("Helvetica-Bold", 10)
    c.drawCentredString(w / 2, y, "Payee Information (must match the STD 204)")
    y -= 4
    c.line(margin, y, w - margin, y)

    y -= 16
    c.setFont("Helvetica", 8)
    c.drawString(margin + 4, y, "NAME (Required. Do not leave blank.)")
    y -= 14
    c.setFont("Courier", 11)
    c.drawString(margin + 4, y, company["name"])

    y -= 20
    c.setFont("Helvetica", 8)
    c.drawString(margin + 4, y, "BUSINESS NAME, DBA NAME or DISREGARDED SINGLE MEMBER LLC NAME")
    c.drawString(w / 2 + 40, y, "TAX ID NUMBER (Required)")
    y -= 14
    c.setFont("Courier", 11)
    # Leave business name blank (same as name)
    c.drawString(w / 2 + 40, y, company["fein"])
    c.line(margin, y - 4, w - margin, y - 4)

    # Remittance addresses — all N/A
    y -= 26
    c.setFont("Helvetica-Bold", 10)
    c.drawCentredString(w / 2, y, "Additional Remittance Address Information")
    y -= 4
    c.line(margin, y, w - margin, y)
    y -= 14
    c.setFont("Helvetica", 8)
    c.drawString(margin + 4, y, "Use the fields below to provide remittance addresses for payee if different from the mailing address on the STD 204.")
    y -= 10
    c.setFont("Helvetica-Oblique", 8)
    c.drawString(margin + 4, y, "The addresses provided below are for remittance purposes only.")

    # 5 address blocks — N/A
    for i in range(1, 6):
        y -= 20
        c.setFont("Helvetica-Bold", 9)
        c.drawString(margin + 4, y, str(i))
        c.setFont("Helvetica", 8)
        c.drawString(margin + 20, y, "REMITTANCE ADDRESS (number, street, apt or suite no.)")
        y -= 14
        c.setFont("Courier", 10)
        c.drawString(margin + 20, y, "N/A" if i == 1 else "")
        y -= 14
        c.setFont("Helvetica", 8)
        c.drawString(margin + 20, y, "CITY")
        c.drawString(w / 2 - 20, y, "STATE")
        c.drawString(w / 2 + 60, y, "ZIP CODE")
        y -= 12
        if i == 1:
            c.setFont("Courier", 10)
            c.drawString(margin + 20, y, "N/A")
        c.line(margin + 20, y - 4, w - margin, y - 4)

    # Contact info
    y -= 24
    c.setFont("Helvetica-Bold", 10)
    c.drawCentredString(w / 2, y, "Additional Contact Information")
    y -= 4
    c.line(margin, y, w - margin, y)

    y -= 18
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margin + 4, y, "1")
    c.setFont("Helvetica", 8)
    c.drawString(margin + 20, y, "CONTACT NAME")
    y -= 14
    c.setFont("Courier", 10)
    c.drawString(margin + 20, y, f"{company['owner']}   ({company['title']})")
    y -= 14
    c.setFont("Helvetica", 8)
    c.drawString(margin + 20, y, "TELEPHONE (Include area code)")
    c.drawString(w / 2, y, "EMAIL")
    y -= 12
    c.setFont("Courier", 10)
    c.drawString(margin + 20, y, company["phone"])
    c.drawString(w / 2, y, company["email"])

    # Certification
    y -= 30
    c.setFont("Helvetica-Bold", 10)
    c.drawCentredString(w / 2, y, "Certification")
    y -= 4
    c.line(margin, y, w - margin, y)
    y -= 14
    c.setFont("Helvetica-Oblique", 8)
    c.drawString(margin + 4, y, "I hereby certify under penalty of perjury that the information provided on this supplemental document is true and correct.")

    # Signature block
    y -= 24
    c.setFont("Helvetica", 8)
    c.drawString(margin + 4, y, "NAME OF AUTHORIZED PAYEE REPRESENTATIVE")
    c.drawString(w / 2 - 20, y, "TITLE")
    c.drawString(w - 2.5 * inch, y, "E-MAIL ADDRESS")
    y -= 14
    c.setFont("Courier", 10)
    c.drawString(margin + 4, y, company["owner"])
    c.drawString(w / 2 - 20, y, company["title"])
    c.drawString(w - 2.5 * inch, y, company["email"])

    y -= 20
    c.setFont("Helvetica", 8)
    c.drawString(margin + 4, y, "SIGNATURE")
    c.drawString(w / 2 - 20, y, "DATE")
    c.drawString(w - 2.5 * inch, y, "TELEPHONE (Include area code)")
    y -= 14
    # Signature image
    if os.path.exists(SIGNATURE_PATH):
        c.drawImage(SIGNATURE_PATH, margin + 4, y - 10, width=1.2 * inch, height=0.4 * inch,
                     preserveAspectRatio=True, mask='auto')
    c.setFont("Courier", 10)
    c.drawString(w / 2 - 20, y, sign_date)
    c.drawString(w - 2.5 * inch, y, company["phone"])

    c.save()
    print(f"  ✓ STD 205 Payee Supplement generated (ReportLab)")


# ═══════════════════════════════════════════════════════════════════════
# Bidder Declaration GSPD-05-106 (ReportLab)
# ═══════════════════════════════════════════════════════════════════════

def _overlay_signature(writer, sign_date):
    """Overlay signature image on pages that have signature areas."""
    try:
        if not os.path.exists(SIGNATURE_PATH):
            return
        from PIL import Image as _Img
        sig_img = _Img.open(SIGNATURE_PATH)
        sig_w, sig_h = sig_img.size
        # Scale signature to ~150pt wide
        scale = 150.0 / sig_w
        img_w = sig_w * scale
        img_h = sig_h * scale

        for page in writer.pages:
            if "/Annots" not in page:
                continue
            for annot in page["/Annots"]:
                obj = annot.get_object()
                name = str(obj.get("/T", ""))
                ft = str(obj.get("/FT", ""))
                if name in SIGN_FIELDS or (ft == "/Sig" and name in SIGN_FIELDS):
                    if "/Rect" in obj:
                        rect = [float(x) for x in obj["/Rect"]]
                        x = rect[0] + 2
                        y = rect[1] + 2
                        from io import BytesIO
                        from reportlab.pdfgen import canvas as rl_c
                        buf = BytesIO()
                        pw = float(page.mediabox.width)
                        ph = float(page.mediabox.height)
                        c = rl_c.Canvas(buf, pagesize=(pw, ph))
                        c.drawImage(SIGNATURE_PATH, x, y, img_w, img_h, mask='auto')
                        c.save()
                        buf.seek(0)
                        overlay = PdfReader(buf)
                        page.merge_page(overlay.pages[0])
                        break  # One signature per page
    except Exception as e:
        import logging
        logging.getLogger("filler").debug("Signature overlay: %s", e)


def fill_bidder_declaration(input_path, rfq_data, config, output_path):
    """Fill Bidder Declaration GSPD-05-105 from actual state template.

    Template has pre-checked boxes: Box3 (broker Yes), Box5 (rental Yes), Box8 (rental N/A).
    We must explicitly clear these and set the correct answers.
    Answers: subcontractors=No, broker=No, rental=No.
    """
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())
    sol = _sol_display(rfq_data.get("solicitation_number", "") or rfq_data.get("rfq_number", ""))

    values = {
        "Solicitaion #": sol,
        # Certification fields are for listing OTHER certifications/subcontractors —
        # NOT for Reytech's own DVBE cert number. Leave blank.
        "Certification": "",
        "Certification #": "",
        "Certification 2": "",
        "Certification 3": "",
        "Product list": "Medical supplies, office supplies, and related products as specified in the solicitation. "
                        "Reytech Inc. sources, prices, and delivers all products directly.",
        "page": "1",
        "of #": "1",
    }

    # Checkbox corrections: which to CHECK and which to CLEAR
    # Force ALL checkboxes — don't rely on template defaults
    # Answers: No subcontractors, No broker, No rental
    check_yes = {"Check Box3", "Check Box5", "Check Box8"}  # No subs, No broker, N/A rental
    check_off = {"Check Box1", "Check Box2", "Check Box4", "Check Box6", "Check Box7"}

    # Fill text fields first
    reader = PdfReader(input_path)
    writer = PdfWriter()
    writer.append(reader)

    clean_values = {k: v for k, v in values.items() if v is not None}
    set_field_fonts(writer, clean_values, 11, 9)

    for page in writer.pages:
        try:
            writer.update_page_form_field_values(page, clean_values, auto_regenerate=False)
        except Exception:
            pass

    # Force-set checkboxes via direct annotation manipulation
    from pypdf.generic import NameObject
    for page in writer.pages:
        if "/Annots" not in page:
            continue
        for annot in page["/Annots"]:
            obj = annot.get_object()
            field_name = str(obj.get("/T", ""))
            if field_name in check_yes:
                obj.update({
                    NameObject("/V"): NameObject("/Yes"),
                    NameObject("/AS"): NameObject("/Yes"),
                })
            elif field_name in check_off:
                obj.update({
                    NameObject("/V"): NameObject("/Off"),
                    NameObject("/AS"): NameObject("/Off"),
                })

    # Add signature overlay
    _overlay_signature(writer, sign_date)

    # ── Overlay signature + date on certification line ──
    # Page is landscape 792x612. Certification text at ~y=58, Page at y=35.
    # Place signature at y=12 (between cert text and page number)
    try:
        if os.path.exists(SIGNATURE_PATH):
            from io import BytesIO
            from reportlab.pdfgen import canvas as rl_c

            page = writer.pages[0]
            pw = float(page.mediabox.width)   # 792
            ph = float(page.mediabox.height)  # 612

            buf = BytesIO()
            c = rl_c.Canvas(buf, pagesize=(pw, ph))

            # Signature image — left side under certification
            c.drawImage(SIGNATURE_PATH, 70, 8, 120, 35, mask='auto')

            # Signature line
            c.setStrokeColorRGB(0, 0, 0)
            c.setLineWidth(0.5)
            c.line(70, 6, 250, 6)

            # Labels
            c.setFont("Helvetica", 7)
            c.drawString(70, -2, "Authorized Signature")

            # Date — right of signature
            c.setFont("Helvetica", 10)
            c.drawString(280, 12, sign_date)
            c.setLineWidth(0.5)
            c.line(280, 6, 380, 6)
            c.setFont("Helvetica", 7)
            c.drawString(280, -2, "Date")

            c.save()
            buf.seek(0)
            overlay_reader = PdfReader(buf)
            page.merge_page(overlay_reader.pages[0])
    except Exception as _sig_e:
        import logging
        logging.getLogger("filler").warning("Bidder Decl signature overlay: %s", _sig_e)

    with open(output_path, "wb") as f:
        writer.write(f)
    print(f"  ✓ Bidder Declaration filled from template (GSPD-05-105, sol={sol})")


def generate_bidder_declaration(rfq_data, config, output_path):
    """Generate Bidder Declaration GSPD-05-106 via ReportLab (fallback)."""
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())

    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.lib import colors

    c = rl_canvas.Canvas(output_path, pagesize=letter)
    w, h = letter
    margin = 0.6 * inch

    # Header
    y = h - 0.6 * inch
    c.setFont("Helvetica", 7)
    c.drawString(margin, y, "State of California—Department of General Services, Procurement Division")
    y -= 10
    c.drawString(margin, y, "GSPD-05-106 (REV 08/09) Verbal Version")
    y -= 4
    c.line(margin, y, w - margin, y)

    y -= 22
    c.setFont("Helvetica-Bold", 14)
    c.drawCentredString(w / 2, y, "BIDDER DECLARATION")

    # Section 1
    y -= 24
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin, y, "1.")
    c.drawString(margin + 18, y, "Prime bidder information (Review attached Bidder Declaration Instructions):")

    y -= 18
    c.setFont("Helvetica", 9)
    c.drawString(margin + 22, y, "a. Identify current California certification(s) (MB, SB, NVSA, DVBE):")
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin + 340, y, "SB/MB/DVBE")
    c.setFont("Helvetica", 9)
    c.drawString(margin + 420, y, "or None ___")

    y -= 18
    c.drawString(margin + 22, y, "b. Will subcontractors be used for this contract? Yes ___ No")
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin + 340, y, "✓")

    y -= 30
    c.setFont("Helvetica", 9)
    c.drawString(margin + 22, y, "c. If you are a California certified DVBE:")
    y -= 14
    c.drawString(margin + 36, y, "(1) Are you a broker or agent? Yes ___ No")
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin + 280, y, "✓")
    y -= 14
    c.setFont("Helvetica", 9)
    c.drawString(margin + 36, y, "(2) If the contract includes equipment rental, does your company own at least 51%")
    y -= 12
    c.drawString(margin + 50, y, "of the equipment provided? Yes ___ No ___ N/A")
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin + 340, y, "✓")

    # Section 2 — Subcontractors table
    y -= 28
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin, y, "2.")
    c.setFont("Helvetica", 9)
    c.drawString(margin + 18, y, "If no subcontractors will be used, skip to certification below. Otherwise, list all subcontractors:")

    # Table header
    y -= 18
    c.setFont("Helvetica-Bold", 7)
    cols = [margin, margin + 130, margin + 260, margin + 340, margin + 420, margin + 475, margin + 510]
    headers = ["Subcontractor Name, Contact\nPhone & Fax", "Subcontractor Address\n& Email",
               "CA Certification", "Work performed or\ngoods provided", "% of\nbid price",
               "Good\nStand?", "51%\nRental?"]
    for i, hdr in enumerate(headers):
        for j, line in enumerate(hdr.split("\n")):
            c.drawString(cols[i] + 2, y - j * 8, line)

    y -= 22
    c.setStrokeColor(colors.black)
    c.setLineWidth(0.5)
    c.line(margin, y, w - margin, y)

    # N/A row
    y -= 14
    c.setFont("Courier", 9)
    c.drawString(cols[0] + 2, y, "N/A")
    c.drawString(cols[1] + 2, y, "N/A")
    c.drawString(cols[2] + 2, y, "N/A")
    c.drawString(cols[3] + 2, y, "N/A")
    c.drawString(cols[4] + 2, y, "N/A")

    # Certification
    y -= 80
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin, y, "3.")
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margin + 18, y, "CERTIFICATION: By signing this form, I certify under penalty of perjury that the information")
    y -= 12
    c.drawString(margin + 18, y, "provided is true and correct.")

    y -= 22
    c.setFont("Helvetica", 9)
    c.drawString(margin + 18, y, "Printed Name:")
    c.setFont("Courier", 10)
    c.drawString(margin + 90, y, f"R. {company['owner']}")

    c.setFont("Helvetica", 9)
    c.drawString(w / 2 + 20, y, "Date Signed:")
    c.setFont("Courier", 10)
    c.drawString(w / 2 + 90, y, sign_date)

    # Signature
    y -= 18
    c.setFont("Helvetica", 9)
    c.drawString(margin + 18, y, "Signature:")
    if os.path.exists(SIGNATURE_PATH):
        c.drawImage(SIGNATURE_PATH, margin + 75, y - 8, width=1.2 * inch, height=0.4 * inch,
                     preserveAspectRatio=True, mask='auto')

    y -= 20
    c.setFont("Helvetica", 8)
    c.drawString(w - 1.5 * inch, y, f"Page 1 of 1")

    c.save()
    print(f"  ✓ Bidder Declaration generated (ReportLab)")


# ═══════════════════════════════════════════════════════════════════════
# DVBE Declarations DGS PD 843 (ReportLab)
# ═══════════════════════════════════════════════════════════════════════

def generate_dvbe_843(rfq_data, config, output_path):
    """Fill official DVBE 843 template (DGS PD 843 Rev. 9/2019)."""
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())
    sol = _sol_display(rfq_data.get("solicitation_number", ""))

    _data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
    template_path = os.path.join(_data_dir, "templates", "dvbe_843_blank.pdf")
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"DVBE 843 template not found at {template_path}")

    values = {
        "DVBEname": company["name"],
        "DVBErefno": company["cert_number"],
        "description": "Medical supplies and equipment",
        "SCno": sol,
        "YNagent": "/1",
        "DVBEowner1": company["owner"],
        "DVBEowner1date": sign_date,
        "DVBEowner2": "N/A",
        "DVBEmgr": "N/A",
        "Principal": "N/A",
        "PrincipalPhone": company["phone"],
        "PrincipalAddress": company["address"],
        "PageNo": "1",
        "TotalPages": "1",
    }

    # fill_and_sign_pdf fills fields + overlays signature on DVBEowner1signature (in SIGN_FIELDS)
    fill_and_sign_pdf(template_path, values, output_path, sign_date=sign_date)
    print(f"  ✓ DVBE 843 filled from template ({sol})")


# ═══════════════════════════════════════════════════════════════════════
# Darfur Contracting Act DGS PD 1 (ReportLab — 2 pages)
# ═══════════════════════════════════════════════════════════════════════

def generate_darfur_act(rfq_data, config, output_path):
    """Generate Darfur Contracting Act certification DGS PD 1 (2 pages) via ReportLab."""
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())

    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import Paragraph as RLParagraph
    from reportlab.lib.enums import TA_LEFT

    c = rl_canvas.Canvas(output_path, pagesize=letter)
    w, h = letter
    margin = 0.8 * inch

    # ── Page 1 ──
    y = h - 0.6 * inch
    c.setFont("Helvetica", 7)
    c.drawString(margin, y, "STATE OF CALIFORNIA")
    c.drawString(w - 3 * inch, y, "DEPARTMENT OF GENERAL SERVICES")
    y -= 10
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin, y, "DARFUR CONTRACTING ACT CERTIFICATION")
    c.setFont("Helvetica", 7)
    c.drawString(w - 3 * inch, y, "PROCUREMENT DIVISION")
    y -= 10
    c.drawString(margin, y, "DGS PD 1 (Rev. 12/19)")

    # Preamble text
    y -= 30
    c.setFont("Helvetica", 9)
    preamble_lines = [
        "Public Contract Code Sections 10475 -10481 applies to any company that currently or",
        "within the previous three years has had business activities or other operations outside",
        "of the United States. For such a company to bid on or submit a proposal for a State of",
        "California contract, the company must certify that it is either a) not a scrutinized",
        "company; or b) a scrutinized company that has been granted permission by the",
        "Department of General Services to submit a proposal.",
        "",
        "If your company has not, within the previous three years, had any business activities",
        "or other operations outside of the United States, you do not need to complete this form.",
    ]
    for line in preamble_lines:
        c.drawString(margin, y, line)
        y -= 12

    # Option 1 - Certification
    y -= 16
    c.setFont("Helvetica-Bold", 11)
    c.drawString(margin, y, "OPTION #1 - CERTIFICATION")
    y -= 16
    c.setFont("Helvetica", 9)
    option1_lines = [
        "If your company, within the previous three years, has had business activities or other",
        "operations outside of the United States, in order to be eligible to submit a bid or",
        "proposal, please insert your company name and Federal ID Number and complete the",
        "certification below.",
    ]
    for line in option1_lines:
        c.drawString(margin, y, line)
        y -= 12

    # Certification statement (highlighted block)
    y -= 8
    c.setFillColor(colors.Color(1, 1, 0.8))
    c.rect(margin, y - 48, w - 2 * margin, 52, fill=1, stroke=0)
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 9)
    cert_lines = [
        "I, the official named below, CERTIFY UNDER PENALTY OF PERJURY that a) the",
        "prospective proposer/bidder named below is not a scrutinized company per Public",
        "Contract Code 10476; and b) I am duly authorized to legally bind the prospective",
        "proposer/bidder named below. This certification is made under the laws of the State of California.",
    ]
    for line in cert_lines:
        c.drawString(margin + 4, y, line)
        y -= 12

    # Highlight the word "not" in line 2 of the certification
    # "prospective proposer/bidder named below is not a scrutinized..."
    # Calculate position: line 2 is at y_line2, "not" starts after "...below is "
    from reportlab.pdfbase.pdfmetrics import stringWidth as _sw
    _font, _size = "Helvetica", 9
    _line2_text = "prospective proposer/bidder named below is not a scrutinized company per Public"
    _prefix = "prospective proposer/bidder named below is "
    _not_x = margin + 4 + _sw(_prefix, _font, _size)
    _not_w = _sw("not", _font, _size)
    _not_y = y + 12 * 2 + 2  # line 2 baseline (cert_lines[1], counting from bottom up)
    c.saveState()
    c.setFillColor(colors.Color(1, 0.9, 0))   # bright yellow
    c.rect(_not_x - 1, _not_y - 2, _not_w + 2, _size + 3, fill=1, stroke=0)
    c.setFillColor(colors.black)
    c.setFont(_font, _size)
    c.drawString(_not_x, _not_y, "not")
    c.restoreState()

    # Company info table
    y -= 12
    c.setStrokeColor(colors.black)
    c.setLineWidth(0.5)
    row_h = 22
    col_mid = w / 2 + 40
    c.rect(margin, y - row_h, col_mid - margin, row_h)
    c.rect(col_mid, y - row_h, w - margin - col_mid, row_h)
    c.setFont("Helvetica-Oblique", 8)
    c.drawString(margin + 4, y - 4, "Company/Vendor Name (Printed)")
    c.drawString(col_mid + 4, y - 4, "Federal ID Number")
    c.setFont("Courier", 11)
    c.drawString(margin + 4, y - 18, company["name"])
    c.drawString(col_mid + 4, y - 18, company["fein"])

    y -= row_h
    c.rect(margin, y - row_h, col_mid - margin, row_h)
    c.rect(col_mid, y - row_h, w - margin - col_mid, row_h)
    c.setFont("Helvetica-Oblique", 8)
    c.drawString(margin + 4, y - 4, "By (Authorized Signature)")
    c.drawString(col_mid + 4, y - 4, "Date")
    if os.path.exists(SIGNATURE_PATH):
        c.drawImage(SIGNATURE_PATH, margin + 4, y - row_h + 2, width=1.2 * inch, height=0.35 * inch,
                     preserveAspectRatio=True, mask='auto')
    c.setFont("Courier", 11)
    c.drawString(col_mid + 4, y - 18, sign_date)

    y -= row_h
    c.rect(margin, y - row_h, w - 2 * margin, row_h)
    c.setFont("Helvetica-Oblique", 8)
    c.drawString(margin + 4, y - 4, "Printed Name and Title of Person Signing")
    c.setFont("Courier", 11)
    c.drawString(margin + 4, y - 18, f"R. {company['owner']}, {company['title']}")

    y -= row_h + 30
    c.setFont("Helvetica", 8)
    c.drawCentredString(w / 2, y, "Page 1 of 2")

    c.showPage()

    # ── Page 2 — Option 2 (N/A) ──
    y = h - 0.8 * inch
    c.setFont("Helvetica", 9)
    page2_lines = [
        "We are a scrutinized company as defined in Public Contract Code section 10476, but",
        "we have received written permission from the Department of General Services to",
        "submit a bid or proposal pursuant to Public Contract Code section 10477(b). A copy of",
        "the written permission from DGS is included with our bid or proposal.",
    ]
    for line in page2_lines:
        c.drawString(margin, y, line)
        y -= 12

    # N/A table
    y -= 16
    c.setStrokeColor(colors.black)
    c.setLineWidth(0.5)
    row_h = 22
    c.rect(margin, y - row_h, col_mid - margin, row_h)
    c.rect(col_mid, y - row_h, w - margin - col_mid, row_h)
    c.setFont("Helvetica-Oblique", 8)
    c.drawString(margin + 4, y - 4, "Company/Vendor Name (Printed)")
    c.drawString(col_mid + 4, y - 4, "Federal ID Number")
    c.setFont("Courier", 11)
    c.drawString(margin + 4, y - 18, "N/A")
    c.drawString(col_mid + 4, y - 18, "N/A")

    y -= row_h
    c.rect(margin, y - row_h, col_mid - margin, row_h)
    c.rect(col_mid, y - row_h, w - margin - col_mid, row_h)
    c.setFont("Helvetica-Oblique", 8)
    c.drawString(margin + 4, y - 4, "By (Authorized Signature)")
    c.drawString(col_mid + 4, y - 4, "Date")

    y -= row_h
    c.rect(margin, y - row_h, w - 2 * margin, row_h)
    c.setFont("Helvetica-Oblique", 8)
    c.drawString(margin + 4, y - 4, "Printed Name and Title of Person Signing")
    c.setFont("Courier", 11)
    c.drawString(margin + 4, y - 18, "N/A")

    y -= row_h + 30
    c.setFont("Helvetica", 8)
    c.drawCentredString(w / 2, y, "Page 2 of 2")

    c.save()
    print(f"  ✓ Darfur Act certification generated (ReportLab, 2 pages)")


# ═══════════════════════════════════════════════════════════════════════
# Drug-Free Workplace STD 21 (ReportLab)
# ═══════════════════════════════════════════════════════════════════════

def generate_drug_free(rfq_data, config, output_path):
    """Generate Drug-Free Workplace Certification STD 21 via ReportLab."""
    company = config["company"]
    # Use the existing execution date and expiration
    exec_date = "9/28/2023"
    expire_date = company.get("drug_free_expiration", "9/1/2026")

    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.lib import colors

    c = rl_canvas.Canvas(output_path, pagesize=letter)
    w, h = letter
    margin = 0.8 * inch

    # Header
    y = h - 0.6 * inch
    c.setFont("Helvetica", 8)
    c.drawString(margin, y, "STATE OF CALIFORNIA")
    y -= 12
    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin, y, "DRUG-FREE WORKPLACE CERTIFICATION")
    y -= 12
    c.setFont("Helvetica", 8)
    c.drawString(margin, y, "STD. 21 (Rev. 10/2019)")
    y -= 4
    c.line(margin, y, w - margin, y)

    # Certification header
    y -= 20
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(w / 2, y, "CERTIFICATION")
    y -= 4
    c.line(margin, y, w - margin, y)

    # Oath
    y -= 18
    c.setFont("Helvetica-BoldOblique", 9)
    oath_lines = [
        "I, the official named below, hereby swear that I am duly authorized legally to bind the contractor or",
        "grant recipient to the certification described below. I am fully aware that this certification, executed",
        "on the date below, is made under penalty of perjury under the laws of the State of California.",
    ]
    for line in oath_lines:
        c.drawString(margin, y, line)
        y -= 12

    # Company info box
    y -= 10
    c.setStrokeColor(colors.black)
    c.setLineWidth(0.5)

    box_y = y - 80
    c.rect(margin, box_y, w - 2 * margin, 80)

    c.setFont("Helvetica", 7)
    c.drawString(margin + 4, y - 4, "CONTRACTOR/BIDDER FIRM NAME")
    c.drawString(w - margin - 150, y - 4, "FEDERAL ID NUMBER")
    c.setFont("Courier", 10)
    c.drawString(margin + 4, y - 16, company["name"])
    c.drawString(w - margin - 150, y - 16, company["fein"])

    c.setFont("Helvetica", 7)
    c.drawString(margin + 4, y - 28, "BY (Authorized Signature)")
    c.drawString(w - margin - 150, y - 28, "DATE EXECUTED")
    if os.path.exists(SIGNATURE_PATH):
        c.drawImage(SIGNATURE_PATH, margin + 4, y - 50, width=1.2 * inch, height=0.35 * inch,
                     preserveAspectRatio=True, mask='auto')
    c.setFont("Courier", 10)
    c.drawString(w - margin - 150, y - 42, exec_date)

    c.setFont("Helvetica", 7)
    c.drawString(margin + 4, y - 54, "PRINTED NAME AND TITLE OF PERSON SIGNING")
    c.drawString(w - margin - 150, y - 54, "TELEPHONE NUMBER (Include Area Code)")
    c.setFont("Courier", 10)
    c.drawString(margin + 4, y - 66, company["owner"])
    c.drawString(w - margin - 150, y - 66, f"({company['phone'][:3]}) {company['phone'][4:]}")

    c.setFont("Helvetica", 7)
    c.drawString(margin + 4, y - 76, "TITLE")
    c.drawString(w / 2, y - 76, "CONTRACTOR/BIDDER FIRM'S MAILING ADDRESS")
    c.setFont("Courier", 10)
    c.drawString(margin + 4, y - 86 + 4, company["title"])
    c.drawString(w / 2, y - 86 + 4, company["address"])

    y = box_y - 8

    # Certification body
    c.setFont("Helvetica", 9)
    c.drawString(margin, y, "The contractor or grant recipient named above hereby certifies compliance with Government Code Section 8355")
    y -= 12
    c.drawString(margin, y, "in matters relating to providing a drug-free workplace. The above named contractor or grant recipient will:")
    y -= 18

    items = [
        "1. Publish a statement notifying employees that unlawful manufacture, distribution, dispensation,\n"
        "   possession, or use of a controlled substance is prohibited and specifying actions to be taken against\n"
        "   employees for violations, as required by Government Code Section 8355(a).",
        "2. Establish a Drug-Free Awareness Program as required by Government Code Section 8355(b), to inform\n"
        "   employees about all of the following:\n"
        "   (a) The dangers of drug abuse in the workplace,\n"
        "   (b) The person's or organization's policy of maintaining a drug-free workplace,\n"
        "   (c) Any available counseling, rehabilitation and employee assistance programs, and\n"
        "   (d) Penalties that may be imposed upon employees for drug abuse violations.",
        "3. Provide as required by Government Code Section 8355(c), that every employee who works on the proposed\n"
        "   contract or grant:\n"
        "   (a) Will receive a copy of the company's drug-free workplace policy statement, and\n"
        "   (b) Will agree to abide by the terms of the company's statement as a condition of employment.",
        f"4. At the election of the contractor or grantee, from and after the \"Date Executed\" and until {expire_date}\n"
        "   (NOT TO EXCEED 36 MONTHS), the state will regard this certificate as valid for all contracts or grants\n"
        "   entered into between the contractor or grantee and this state agency.",
    ]

    c.setFont("Helvetica", 8)
    for item in items:
        for line in item.split("\n"):
            c.drawString(margin, y, line.strip())
            y -= 10
        y -= 4

    c.save()
    print(f"  ✓ Drug-Free Workplace STD 21 generated (ReportLab)")


# ═══════════════════════════════════════════════════════════════════════
# CalRecycle 74 — Standalone with overflow pages
# ═══════════════════════════════════════════════════════════════════════

def _calrecycle_overlay_items(pdf_path, items):
    """
    Overlay line item text onto CalRecycle 74 using ReportLab.
    
    pypdf form filling clips text in the narrow fields (42pt item#, 246pt desc).
    ReportLab overlay gives exact control over font size and positioning.
    
    Scans the PDF for field coordinates to handle any CalRecycle template version.
    """
    import io as _io
    from pypdf import PdfReader as _PR, PdfWriter as _PW
    from reportlab.pdfgen import canvas as _rlc
    import re as _re

    try:
        reader = _PR(pdf_path)
        writer = _PW()
        writer.append(reader)

        # Find CalRecycle page — has "Product or Services DescriptionRow1"
        cr_page_idx = None
        row_coords = {}  # {row_num: {field_name: [x0, y0, x1, y1]}}
        
        for pg_idx, pg in enumerate(reader.pages):
            annots = pg.get("/Annots", []) or []
            found_desc_row1 = False
            for a in annots:
                obj = a.get_object() if hasattr(a, "get_object") else a
                name = str(obj.get("/T", ""))
                rect = obj.get("/Rect")
                if not rect:
                    continue
                
                # Detect row fields
                for row_n in range(1, 7):
                    if f"Row{row_n}" in name:
                        coords = [float(x) for x in rect]
                        row_coords.setdefault(row_n, {})[name] = coords
                        if "DescriptionRow1" in name:
                            found_desc_row1 = True
            
            if found_desc_row1:
                cr_page_idx = pg_idx
                break

        if cr_page_idx is None:
            print("  ⚠ CalRecycle overlay: could not find CalRecycle page")
            return

        page = writer.pages[cr_page_idx]
        mb = page.get("/MediaBox", [0, 0, 612, 792])
        pw, ph = float(mb[2]), float(mb[3])

        # Create overlay canvas
        packet = _io.BytesIO()
        c = _rlc.Canvas(packet, pagesize=(pw, ph))
        c.setFillColorRGB(0, 0, 0)

        for idx, item in enumerate(items, start=1):
            if idx not in row_coords:
                continue
            
            fields = row_coords[idx]
            
            # Item # — 42pt wide field, use 5.5pt font
            item_rect = fields.get(f"Item Row{idx}")
            if item_rect:
                pn = str(item.get("item_number", item.get("part_number", "")))
                font_sz = 5.5
                # Auto-shrink if still too wide
                item_w = item_rect[2] - item_rect[0] - 4
                while c.stringWidth(pn, "Helvetica", font_sz) > item_w and font_sz > 4:
                    font_sz -= 0.5
                if c.stringWidth(pn, "Helvetica", font_sz) > item_w:
                    # Still too wide — truncate
                    while len(pn) > 2 and c.stringWidth(pn, "Helvetica", font_sz) > item_w:
                        pn = pn[:-1]
                c.setFont("Helvetica", font_sz)
                c.drawString(item_rect[0] + 2, item_rect[1] + 8, pn)

            # Description — 246pt wide, use 6pt font with smart truncation
            desc_rect = fields.get(f"Product or Services DescriptionRow{idx}")
            if desc_rect:
                desc = _calrecycle_clean_desc(item)
                desc_w = desc_rect[2] - desc_rect[0] - 6
                font_sz = 6.5
                # Check if it fits, shrink if needed
                while c.stringWidth(desc, "Helvetica", font_sz) > desc_w and font_sz > 5:
                    font_sz -= 0.5
                # Still too wide — truncate at word boundary
                if c.stringWidth(desc, "Helvetica", font_sz) > desc_w:
                    while len(desc) > 10 and c.stringWidth(desc, "Helvetica", font_sz) > desc_w:
                        cut = desc[:-4].rfind(" ")
                        if cut > len(desc) // 2:
                            desc = desc[:cut] + "..."
                        else:
                            desc = desc[:-4] + "..."
                c.setFont("Helvetica", font_sz)
                c.drawString(desc_rect[0] + 3, desc_rect[1] + 8, desc)

            # Percent — "0%"
            pct_key = f"1Percent Postconsumer Recycled Content MaterialRow{idx}"
            pct_rect = fields.get(pct_key)
            if pct_rect:
                c.setFont("Helvetica", 7)
                c.drawString(pct_rect[0] + 8, pct_rect[1] + 8, "0%")

            # SABRC Code — "N/A"
            sabrc_key = f"2SABRC Product Category CodeRow{idx}"
            sabrc_rect = fields.get(sabrc_key)
            if sabrc_rect:
                c.setFont("Helvetica", 7)
                c.drawString(sabrc_rect[0] + 6, sabrc_rect[1] + 8, "N/A")

        c.save()
        packet.seek(0)

        from pypdf import PdfReader as _PR2
        overlay = _PR2(packet)
        if overlay.pages:
            page.merge_page(overlay.pages[0])

        with open(pdf_path, "wb") as _f:
            writer.write(_f)
        print(f"  ✓ CalRecycle overlay: {len(items)} items drawn at exact field coordinates")
    except Exception as _e:
        print(f"  ⚠ CalRecycle overlay failed: {_e}")
        import traceback; traceback.print_exc()


def _calrecycle_clean_desc(item):
    """Clean description for CalRecycle overlay. More generous than form-field version."""
    import re as _re
    desc = item.get("description", "")
    
    # Strip after first " - " if left side is substantial
    if " - " in desc:
        left = desc.split(" - ")[0].strip()
        if _re.match(r'^\d+\s+[A-Z]{2,4}$', left):
            desc = desc.split(" - ", 1)[1].strip()
        else:
            desc = left
    # Strip label:value patterns
    desc = _re.sub(r'\s*\b(?:U?S?B?ISBN|SKU|Ref|Cat|MFG|NDC|UPC|GTIN|Item)\s*#?\s*:?\s*[\w\-]*',
                    '', desc, flags=_re.IGNORECASE)
    desc = _re.sub(r':\s*[A-Z]*\d[\w\-]{3,}.*', '', desc, flags=_re.IGNORECASE)
    desc = _re.sub(r'\s*#?\d{6,}[\w\-]*.*', '', desc)
    desc = _re.sub(r'\s*\([^)]*\)\s*$', '', desc)
    for m in ["(R)", "(TM)", "®", "™"]:
        desc = desc.replace(m, "")
    desc = _re.sub(r'^\d+\s+[A-Z]{2,3}\s*[-–]\s*', '', desc)
    desc = _re.sub(r'\s{2,}', ' ', desc).strip(" ,;-:/")
    return desc

def _calrecycle_fix_date(pdf_path, sign_date):
    """
    Overlay the date onto CalRecycle 74 Date fields ONLY.
    The CalRecycle 74 has an unnamed "Date" field that can't be filled via the values dict.
    IMPORTANT: Only overlay on CalRecycle pages — never on Darfur, DVBE, CUF, PD802, etc.
    """
    import io as _io
    from pypdf import PdfReader as _PR, PdfWriter as _PW
    from reportlab.pdfgen import canvas as _rlc
    try:
        reader = _PR(pdf_path)
        writer = _PW()
        writer.append(reader)

        # Find CalRecycle pages first, then look for "Date" fields ONLY on those pages
        cr_pages = set()
        for pg_idx, pg in enumerate(reader.pages):
            try:
                txt = (pg.extract_text() or "").upper()
            except Exception:
                txt = ""
            if "CALRECYCLE" in txt or "POSTCONSUMER RECYCLED" in txt:
                cr_pages.add(pg_idx)

        if not cr_pages:
            return  # No CalRecycle pages in this PDF

        date_fields = []  # [(page_idx, rect), ...]
        for pg_idx in cr_pages:
            pg = reader.pages[pg_idx]
            annots = pg.get("/Annots", []) or []
            for a in annots:
                obj = a.get_object() if hasattr(a, "get_object") else a
                name = str(obj.get("/T", ""))
                # Only match exact "Date" field (CalRecycle's unnamed date field)
                # Never match Date_CUF, Date__darfur, Date1_PD843, Date_PD802, etc.
                if name == "Date":
                    rect = obj.get("/Rect")
                    if rect:
                        date_fields.append((pg_idx, [float(x) for x in rect]))

        if not date_fields:
            # Fallback: overlay at known CalRecycle date position
            for pg_idx in cr_pages:
                date_fields.append((pg_idx, [460, 148, 540, 166]))
                print(f"  ℹ CalRecycle date: using fallback position on page {pg_idx}")
                break

        if not date_fields:
            return

        for pg_idx, date_rect in date_fields:
            page = writer.pages[pg_idx]
            mb = page.get("/MediaBox", [0, 0, 612, 792])
            pw, ph = float(mb[2]), float(mb[3])

            packet = _io.BytesIO()
            c = _rlc.Canvas(packet, pagesize=(pw, ph))
            c.setFont("Helvetica", 9)
            c.setFillColorRGB(0, 0, 0)
            c.drawString(date_rect[0] + 3, date_rect[1] + 5, sign_date)
            c.save()
            packet.seek(0)

            from pypdf import PdfReader as _PR2
            overlay = _PR2(packet)
            page.merge_page(overlay.pages[0])

        with open(pdf_path, "wb") as _f:
            writer.write(_f)
        print(f"  ✓ CalRecycle date overlaid on {len(date_fields)} pages → {sign_date}")
    except Exception as _e:
        print(f"  ⚠ CalRecycle date fix failed: {_e}")


def fill_calrecycle_standalone(input_path, rfq_data, config, output_path):
    """Fill CalRecycle 74 form with line items. Adds overflow pages for >6 items."""
    company = config["company"]
    sol = _sol_display(rfq_data.get("solicitation_number", ""))
    sign_date = rfq_data.get("sign_date", get_pst_date())
    items = rfq_data.get("line_items", [])

    # Common company fields (Date handled by _calrecycle_fix_date overlay)
    # Clear buyer-area fields (Purchasing Agent, Phone, Email) — those are
    # for the STATE agency to fill, not the supplier
    base_values = {
        "ContractorCompany Name": company["name"],
        "Address": company["address"],
        "Phone_2": company["phone"],
        "Print Name": company["owner"],
        "Title": company["title"],
        # Blank out buyer fields — not our data to fill
        "Purchasing Agent": " ",
        "Phone": " ",
        "Email": " ",
        "E-mail": " ",  # alternate field name
        "PO": " ",
        "State Agency": " ",
    }

    # Maximum chars for CalRecycle description field (246pt wide).
    # At 7pt Helvetica ≈55 chars, at 6pt ≈63 chars, at 5pt ≈76 chars.
    # We target 55 since the font auto-sizer in set_field_fonts() will
    # scale down if needed — but giving it 55 means it stays at readable 7pt.
    _CR_MAX = 55

    def _short_desc(item):
        """Extract product name for CalRecycle 246pt-wide field.
        
        Strategy: strip catalog noise first, then word-boundary truncate.
        This handles ANY future description format — not pattern-specific.
        """
        import re as _re
        desc = item.get("description", "")

        # ── Phase 1: strip catalog noise ──
        # Everything after first " - " (ref numbers, UPCs, secondary info)
        # But validate: if the left part is just a qty like "10 EA", use right side
        if " - " in desc:
            left = desc.split(" - ")[0].strip()
            if _re.match(r'^\d+\s+[A-Z]{2,4}$', left):
                desc = desc.split(" - ", 1)[1].strip()
            else:
                desc = left
        # Label:value patterns — strip the label AND value (ISBN:, SKU:, NDC:, etc.)
        desc = _re.sub(r'\s*\b(?:U?S?B?ISBN|SKU|Ref|Cat|MFG|NDC|UPC|GTIN|Item)\s*#?\s*:?\s*[\w\-]*',
                        '', desc, flags=_re.IGNORECASE)
        # Colon followed by alphanumeric reference codes (catches remaining "Code: 12345")
        desc = _re.sub(r':\s*[A-Z]*\d[\w\-]{3,}.*', '', desc, flags=_re.IGNORECASE)
        # Standalone long numeric codes (6+ digits, likely catalog/part numbers)
        desc = _re.sub(r'\s*#?\d{6,}[\w\-]*.*', '', desc)
        # Parenthesized content at end
        desc = _re.sub(r'\s*\([^)]*\)\s*$', '', desc)
        # Registration marks
        for m in ["(R)", "(TM)", "®", "™"]:
            desc = desc.replace(m, "")
        # Leading qty patterns: "10 EA - ", "2 BX - "
        desc = _re.sub(r'^\d+\s+[A-Z]{2,3}\s*[-–]\s*', '', desc)
        # Collapse whitespace from stripped segments
        desc = _re.sub(r'\s{2,}', ' ', desc).strip(" ,;-:/")

        # ── Phase 2: smart truncate at word boundary ──
        if len(desc) > _CR_MAX:
            cut = desc[:_CR_MAX - 3].rfind(" ")
            if cut > _CR_MAX // 2:
                desc = desc[:cut] + "..."
            else:
                desc = desc[:_CR_MAX - 3] + "..."
        return desc

    def _short_item(item):
        """Truncate item number to fit 42pt field (max ~7 chars at 7pt)."""
        pn = item.get("item_number", item.get("part_number", ""))
        if len(pn) > 10:
            pn = pn[:10]
        return pn

    # Fill header fields only — line items use ReportLab overlay for precise rendering
    values = dict(base_values)
    # DON'T fill item/description/percent/SABRC form fields — overlay handles them

    if not items:
        values["Product or Services DescriptionRow1"] = "All Items"
        values["1Percent Postconsumer Recycled Content MaterialRow1"] = "0%"
        values["2SABRC Product Category CodeRow1"] = "N/A"

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)
    _calrecycle_fix_date(output_path, sign_date)

    # ── Overlay line items using ReportLab for precise text rendering ──
    # pypdf form filling clips text in narrow fields. ReportLab gives us
    # exact control over font size, position, and truncation.
    if items:
        _calrecycle_overlay_items(output_path, items[:6])

    # Overflow: if >6 items, append additional CalRecycle pages
    if len(items) > 6:
        remaining = items[6:]
        tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "templates")
        blank_cr = os.path.join(tmpl_dir, "calrecycle_74_blank.pdf")
        if not os.path.exists(blank_cr):
            print(f"  ⚠ CalRecycle overflow: blank template not found at {blank_cr}")
            return

        overflow_pages = []
        for batch_start in range(0, len(remaining), 6):
            batch = remaining[batch_start:batch_start + 6]
            ov_values = dict(base_values)
            ov_path = output_path.replace(".pdf", f"_overflow_{batch_start}.pdf")
            fill_and_sign_pdf(blank_cr, ov_values, ov_path, sign_date=sign_date)
            _calrecycle_fix_date(ov_path, sign_date)
            _calrecycle_overlay_items(ov_path, batch)
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
    sol = _sol_display(rfq_data.get("solicitation_number", ""))
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

        # CalRecycle 74 — company info + signature (Date handled by overlay only — no double-write)
        "ContractorCompany Name": company["name"],
        "Address": company["address"], "Phone_2": company["phone"],
        "Print Name": company["owner"], "Title": company["title"],
    }

    # ── CalRecycle 74: populate each line item row ──
    import re as _re_cr
    def _cr_desc(item):
        """Clean description for CalRecycle 74.
        Field is 246pt wide. Font auto-sizer goes down to 6pt = ~66 chars max.
        Strip part numbers, UPC suffixes, and trailing noise.
        """
        desc = item.get("description", "")
        if " - " in desc:
            desc = desc.split(" - ")[0].strip()
        desc = desc.rstrip(" -")
        for m in ["(R)", "(TM)", "®", "™"]:
            desc = desc.replace(m, "")
        desc = _re_cr.sub(r'\s*\([^)]*\)\s*$', '', desc)
        desc = _re_cr.sub(r'^\d+\s+EA\s*[-–]\s*', '', desc)
        desc = _re_cr.sub(r'\s*Model\s*#.*', '', desc, flags=_re_cr.IGNORECASE)
        desc = _re_cr.sub(r'\s*UPC\s*#.*', '', desc, flags=_re_cr.IGNORECASE)
        desc = desc.strip(" ,;-/")
        # Cap at 80 chars — font auto-sizer will reduce to fit (9→8→7→6pt).
        if len(desc) > 80:
            desc = desc[:77] + "..."
        return desc

    line_items = rfq_data.get("line_items", [])
    for idx, item in enumerate(line_items[:6], start=1):  # Template has 6 rows max
        pn = item.get("item_number", item.get("part_number", ""))
        desc = _cr_desc(item)
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

    # Fix CalRecycle unnamed Date field (no /T name — can't be filled via values dict)
    try:
        _calrecycle_fix_date(output_path, sign_date)
    except Exception as _crd_e:
        print(f"  ⚠ CalRecycle date fix skipped: {_crd_e}")

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
    
    # ── Trim to submission pages only ───────────────────────────────────
    # Strategy: scan the FILLED output for EVERY page using _bidpkg_page_skip_reason().
    # Skip pages (SABRC table, GenAI defs, VSDS instruction, Darfur pg2, etc.) have
    # no signature overlays, so their text is fully extractable in the filled PDF.
    # Blank-template scan is used as a secondary check to catch field-fingerprint skips
    # (e.g. OBS 1600, GSPD Bidder Declaration) that may not be detectable by text alone.
    try:
        local_keep = set(_compute_bidpkg_keep_indices(input_path))
        local_total = len(PdfReader(input_path).pages)
        reader = PdfReader(output_path)
        total_pages = len(reader.pages)
        valid_keep = []

        for i in range(total_pages):
            # PRIMARY: scan the filled output directly — most reliable for skip pages
            reason = _bidpkg_page_skip_reason(reader.pages[i])
            if reason:
                print(f"  BidPkg skip pg{i:02d} (filled scan): {reason}")
                continue
            # SECONDARY: if blank-template scan also says skip, respect that
            # (catches field-fingerprint patterns like OBS 1600, GSPD Bidder Decl)
            if i < local_total and i not in local_keep:
                print(f"  BidPkg skip pg{i:02d} (blank-template scan)")
                continue
            valid_keep.append(i)

        if valid_keep and valid_keep != list(range(total_pages)):
            writer = PdfWriter()
            for i in valid_keep:
                writer.add_page(reader.pages[i])
            with open(output_path, "wb") as f:
                writer.write(f)
            skipped = total_pages - len(valid_keep)
            print(f"  ✓ BidPackage trimmed: {len(valid_keep)} submission pages kept, {skipped} removed")
        else:
            print(f"  ✓ BidPackage: all {total_pages} pages kept (no trimming needed)")
    except Exception as _te:
        print(f"  ⚠ BidPackage page trim failed (using full output): {_te}")

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
    sol = _sol_display(rfq_data.get("solicitation_number", ""))
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


# ═══════════════════════════════════════════════════════════════════════
# BidPackage template page map — computed once from blank template
# ═══════════════════════════════════════════════════════════════════════

# BidPackage template page map — computed once from blank template
# ═══════════════════════════════════════════════════════════════════════

_BIDPKG_KEEP_CACHE = {}  # {template_path: (mtime, [indices])}


def _bidpkg_page_skip_reason(page):
    """
    Shared logic: return skip reason string if page should be excluded from the
    submission package, or None if it should be kept.

    Works against both the blank template (clean text) AND filled output pages.
    Uses field-name fingerprints as primary signal when available, with text
    patterns as fallback — because text extraction is unreliable on filled PDFs.
    """
    text = (page.extract_text() or "").strip()
    t = text.lower()
    n_fields = len(page.get("/Annots", [])) if "/Annots" in page else 0

    # ── Collect field name fingerprints (first 3 fields) ──────────────
    field_names = []
    if "/Annots" in page:
        for annot in page.get("/Annots", [])[:5]:
            try:
                fn = str(annot.get_object().get("/T", ""))
                if fn:
                    field_names.append(fn)
            except Exception:
                pass
    field_sig = " ".join(field_names).lower()

    # ── Hard skip by field-name fingerprint ───────────────────────────
    # OBS 1600 food entry form (fields named OBS 1600 *)
    if any("obs 1600" in f.lower() for f in field_names):
        return "OBS 1600 food entry form"
    # GSPD-05-105 Bidder Declaration (from template, NOT the standalone)
    # Field names: Text0_105, Check3_105, Page1_105, Signature29
    if any("gspd" in f.lower() or "subcontractor" in f.lower() for f in field_names):
        return "GSPD-05-105 Bidder Declaration (use standalone)"
    if any(f.endswith("_105") for f in field_names) and any(f.startswith("Text") for f in field_names):
        return "GSPD-05-105 Bidder Declaration (field pattern _105)"
    # STD 105 Bidder Declaration by field naming convention
    if "solicitation number" in field_sig and "subcontractor" in field_sig:
        return "Bidder Declaration fields"

    # ── Hard skip by text pattern ──────────────────────────────────────
    # Truly blank (no text, no fields)
    if len(text) == 0 and n_fields == 0:
        return "blank (no text, no fields)"
    # OBS 1600 food entry form — blank text WITH many fields AND at least one OBS field name.
    # NOTE: Do NOT rely on n_fields alone (DVBE 843 also has ~29 blank-text fields).
    # CalRecycle SABRC reference table — identified by the email address in header
    # or by the product category code table header. The CalRecycle 74 FILL form
    # also mentions "SABRC" in its intro text, so we must NOT match on "sabrc" alone.
    if "sabrc@calrecycle" in t:
        return "CalRecycle SABRC reference table"
    if "code* product categories" in t and "product subcategories" in t:
        return "CalRecycle SABRC category table"
    # OBS 1600 footnotes
    if '"produced" is used interchangeably' in t:
        return "OBS 1600 footnotes"
    # OBS 1600 food category codes table
    if "code category" in t and "coffee" in t and "dairy" in t:
        return "OBS 1600 food codes"
    # VSDS email submission instruction
    if "submit the completed dgs pd 802" in t:
        return "VSDS submission instruction"
    # Darfur Contracting Act (both pages — standalone version used instead)
    if "10475" in text and "scrutinized" in t and "public contract code" in t:
        return "Darfur pg1 (standalone used)"
    if "scrutinized company" in t and ("10476" in text or "written permission" in t):
        return "Darfur pg2 (standalone used)"
    # GenAI definition pages (3 of 4 and 4 of 4)
    if ("3 of 4" in text or "4 of 4" in text) and (
            "genai" in t or "definition" in t.replace("definitions", "definition")):
        return f"GenAI definitions page ({text[:30]})"
    # Bidder Declaration (text-based detection — GSPD-05-105 form)
    if "gspd" in t and ("bidder declaration" in t or "subcontractor" in t):
        return "GSPD Bidder Declaration text"

    return None  # KEEP


def _compute_bidpkg_keep_indices(template_path):
    """
    Scan blank BidPackage template and return page indices to keep in submission.
    Uses _bidpkg_page_skip_reason() — same logic applied to extra Railway pages.
    Cached by file mtime; auto-invalidates when template is updated.
    """
    import os as _os
    global _BIDPKG_KEEP_CACHE
    try:
        mtime = _os.path.getmtime(template_path)
    except OSError:
        mtime = 0

    cached = _BIDPKG_KEEP_CACHE.get(template_path)
    if cached and cached[0] == mtime:
        return cached[1]

    reader = PdfReader(template_path)
    keep = []

    for i, page in enumerate(reader.pages):
        reason = _bidpkg_page_skip_reason(page)
        if reason:
            print(f"  BidPkg skip pg{i:02d}: {reason}")
        else:
            keep.append(i)

    _BIDPKG_KEEP_CACHE[template_path] = (mtime, keep)
    print(f"  ✓ BidPackage page map: keep indices {keep} (of {len(reader.pages)} total)")
    return keep


def fill_genai_708(input_path, rfq_data, config, output_path):
    """Fill AMS 708 GenAI Disclosure. Always: No GenAI used."""
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())
    sol = _sol_display(rfq_data.get("solicitation_number", ""))

    values = {
        "Solicitation Number": sol, "Solicitation": sol, "Solicitation #": sol,
        "Company Name": company["name"], "Vendor Name": company["name"],
        "Contact Person": company["owner"], "Date": sign_date,
        "Date1_af_date": sign_date,
    }

    # Scan fields and check "No" for GenAI usage
    try:
        from pypdf import PdfReader
        r = PdfReader(input_path)
        fields = r.get_fields() or {}
        for fname, fobj in fields.items():
            fn_lower = fname.lower()
            if ("no" in fn_lower and ("genai" in fn_lower or "ai" in fn_lower or "option" in fn_lower)) or \
               ("check" in fn_lower and "no" in fn_lower):
                values[fname] = "/Yes"
        # Common checkbox names
        for prefix in ["", "_2"]:
            values[f"NoGenAI{prefix}"] = "/Yes"
            values[f"No{prefix}"] = "/Yes"
            values[f"Check_No{prefix}"] = "/Yes"
    except Exception:
        pass

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)
    print(f"  ✓ 708 GenAI filled — No GenAI used")


def fill_std205(input_path, rfq_data, config, output_path):
    """Fill STD 205 Payee Data Record Supplement from blank template.

    Fields (from actual PDF):
      nameReq1: Company name (required)
      taxIDNumber: FEIN
      remAddress1: Remittance address line 1
      CITY1, STATE1, ZIPCODE1: City/State/ZIP for remittance
      TELEPHONE_1: Phone
      EMAIL: Email
      contactName1: Contact person name
      certName: Certification printed name
      certTelephone: Certification phone
      TITLE: Title of signer
      DATE: Signature date
      Signature3: Signature field
    """
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())

    # Parse address into components
    addr = company.get("address", "")
    city = company.get("city", "Trabuco Canyon")
    state = company.get("state", "CA")
    zipcode = company.get("zip", "92679")
    # Try to parse from full address string if components missing
    if not city and addr:
        import re
        m = re.search(r'(.+?),?\s+([A-Z]{2})\s+(\d{5})', addr)
        if m:
            city = m.group(1).split(',')[-1].strip()
            state = m.group(2)
            zipcode = m.group(3)

    values = {
        # Section 1: Payee Information (required)
        "nameReq1": company["name"],
        "taxIDNumber": company.get("fein", ""),
        # Section 2: Remittance Address #1 ONLY (same as STD 204)
        "remAddress1": addr.split(',')[0] if ',' in addr else addr,
        "CITY1": city,
        "STATE1": state,
        "ZIPCODE1": zipcode,
        # Do NOT fill contactName, TELEPHONE, or addresses 2-5
        # Those are "additional contact information" — leave blank
        # Certification section
        "certName": f"{company.get('owner', '')}, {company.get('title', 'Owner')}",
        "certTelephone": company.get("phone", ""),
        "EMAIL": company.get("email", ""),
        "TITLE": company.get("title", "Owner"),
        "DATE": sign_date,
    }

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)
    print(f"  ✓ STD 205 filled from template")


def fill_darfur_standalone(input_path, rfq_data, config, output_path):
    """Fill Darfur Certification Form — Option 1 ONLY (not a scrutinized company).

    Page 1: Option 1 — fill company name, FEIN, signature, date.
    Page 2: Option 2 — LEAVE COMPLETELY BLANK (we are not a scrutinized company).
    """
    company = config["company"]
    sign_date = rfq_data.get("sign_date", get_pst_date())

    # ONLY page 1 (Option 1) fields — NO _2 suffix fields
    values = {
        "CompanyVendor Name": company["name"],
        "Federal ID Number": company.get("fein", ""),
        "Printed Name and Title of Person Signing": f"{company['owner']}, {company.get('title', 'President')}",
        "Date of signature": sign_date,
    }

    fill_and_sign_pdf(input_path, values, output_path, sign_date=sign_date)

    # Highlight the Option 1 certification paragraph
    try:
        from pypdf import PdfReader as _PR, PdfWriter as _PW
        from io import BytesIO as _BIO
        from reportlab.pdfgen import canvas as _rlc
        from reportlab.lib.colors import Color

        reader = _PR(output_path)
        writer = _PW()
        writer.append(reader)

        page = writer.pages[0]
        pw = float(page.mediabox.width)
        ph = float(page.mediabox.height)

        buf = _BIO()
        c = _rlc.Canvas(buf, pagesize=(pw, ph))
        # Yellow highlight with transparency
        c.setFillColor(Color(1, 1, 0, alpha=0.25))
        # The "I, the official named below, CERTIFY UNDER PENALTY OF PERJURY..." paragraph
        # Coordinates from the DGS PD 1 template layout
        c.rect(58, 378, 500, 95, fill=True, stroke=False)
        c.save()
        buf.seek(0)

        overlay = _PR(buf)
        page.merge_page(overlay.pages[0])

        with open(output_path, "wb") as _f:
            writer.write(_f)
    except Exception as _he:
        import logging
        logging.getLogger("filler").debug("Darfur highlight: %s", _he)

    print(f"  ✓ Darfur Act filled from template — Option 1 only (page 2 blank)")

