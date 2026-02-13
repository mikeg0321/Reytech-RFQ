"""
Reytech Formal Quote PDF Generator v5
- Shared column grid with side borders (vertical dividers)
- Center-aligned headers by default (configurable per-column)
- Dynamic row heights for any content length
- CDTFA tax rate API: auto-lookup by ship-to address/zip
- Logo upload, CRM DB, sequential quote numbers
"""
import os, json, logging, re
from datetime import datetime, timedelta
from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import HexColor, black
from reportlab.pdfgen import canvas

log = logging.getLogger("quote_gen")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ASSETS_DIR, exist_ok=True)

COMPANY = {
    "name": "Reytech Inc.", "address_1": "30 Carnoustie Way",
    "city_state": "Trabuco Canyon, CA 92679", "owner": "Michael Guadan",
    "title": "Owner", "phone": "949-229-1575", "email": "sales@reytechinc.com",
    "sellers_permit": "245652416-00001", "salesperson": "Mike Guadan",
}

AGENCY_CONFIG = {
    "CalVet": {
        "show_bill_to": True,
        "bill_to_name": "Dept. of Corrections and Rehabilitation",
        "bill_to_lines": ["P.O. Box 187021", "Sacramento, CA 95818-7021", "United States"],
        "show_sellers_permit": True, "default_tax_rate": 0.0725, "to_label": "To:",
    },
    "CCHCS": {
        "show_bill_to": False, "bill_to_name": "", "bill_to_lines": [],
        "show_sellers_permit": False, "default_tax_rate": 0.0725, "to_label": "To",
    },
    "CDCR": {
        "show_bill_to": False, "bill_to_name": "", "bill_to_lines": [],
        "show_sellers_permit": False, "default_tax_rate": 0.0725, "to_label": "To",
    },
}

HEADER_BG = HexColor("#b8c6e0")
BORDER_CLR = HexColor("#333333")
ROW_LINE_CLR = HexColor("#bbbbbb")
PAD = 4

# Import tax agent
try:
    from tax_agent import get_tax_rate as _cdtfa_lookup, extract_zip, extract_city
except ImportError:
    # Fallback if tax_agent not available
    def _cdtfa_lookup(**kwargs):
        return {"rate": 0.0725, "source": "default", "note": "tax_agent not found"}
    def extract_zip(t):
        import re
        if isinstance(t, list): t = " ".join(t)
        m = re.search(r'\b(\d{5})(?:-\d{4})?\b', str(t))
        return m.group(1) if m else ""
    def extract_city(t):
        import re
        if isinstance(t, list): t = " ".join(t)
        m = re.search(r'([A-Za-z\s\.]+),\s*[A-Z]{2}\s+\d{5}', str(t))
        return m.group(1).strip() if m else ""


# ═══════════════════════════════════════════════════════════════
# Logo
# ═══════════════════════════════════════════════════════════════
def get_logo_path():
    for ext in ["png", "jpg", "jpeg", "gif"]:
        p = os.path.join(ASSETS_DIR, f"logo.{ext}")
        if os.path.exists(p): return p
    return None

def save_logo(file_data, filename):
    ext = filename.rsplit(".", 1)[-1].lower()
    if ext not in ("png", "jpg", "jpeg", "gif"): return None
    for e in ["png", "jpg", "jpeg", "gif"]:
        old = os.path.join(ASSETS_DIR, f"logo.{e}")
        if os.path.exists(old): os.remove(old)
    path = os.path.join(ASSETS_DIR, f"logo.{ext}")
    with open(path, "wb") as f: f.write(file_data)
    return path


# ═══════════════════════════════════════════════════════════════
# Quote Numbers
# ═══════════════════════════════════════════════════════════════
def _counter_path(): return os.path.join(DATA_DIR, "quote_counter.json")
def _load_counter():
    try:
        with open(_counter_path()) as f: return json.load(f)
    except: return {}
def _save_counter(d):
    with open(_counter_path(), "w") as f: json.dump(d, f, indent=2)

def get_next_quote_number():
    d = _load_counter(); yr = datetime.now().strftime("%y")
    n = d.get(f"year_{yr}", 0) + 1; d[f"year_{yr}"] = n; _save_counter(d)
    return f"R{yr}Q{n}"

def peek_next_quote_number():
    d = _load_counter(); yr = datetime.now().strftime("%y")
    return f"R{yr}Q{d.get(f'year_{yr}', 0) + 1}"

def reset_quote_counter(year=None):
    d = _load_counter(); yr = year or datetime.now().strftime("%y")
    d[f"year_{yr}"] = 0; _save_counter(d)

def set_quote_counter(number):
    d = _load_counter(); yr = datetime.now().strftime("%y")
    d[f"year_{yr}"] = number; _save_counter(d)


# ═══════════════════════════════════════════════════════════════
# CRM Contacts
# ═══════════════════════════════════════════════════════════════
def _contacts_path(): return os.path.join(DATA_DIR, "contacts.json")
def load_contacts():
    try:
        with open(_contacts_path()) as f: return json.load(f)
    except: return []
def save_contacts(c):
    with open(_contacts_path(), "w") as f: json.dump(c, f, indent=2)

def add_contact(name, address_lines, agency="", email="", phone="", contact_type="ship_to"):
    contacts = load_contacts()
    for c in contacts:
        if c["name"].lower() == name.lower() and c["type"] == contact_type:
            c.update({"address_lines": address_lines, "agency": agency,
                       "email": email or c.get("email",""), "updated": datetime.now().isoformat()})
            save_contacts(contacts); return c
    contact = {"id": len(contacts)+1, "name": name, "address_lines": address_lines,
               "agency": agency, "email": email, "phone": phone, "type": contact_type,
               "created": datetime.now().isoformat(), "updated": datetime.now().isoformat()}
    contacts.append(contact); save_contacts(contacts); return contact

def search_contacts(query="", contact_type=None):
    q = query.lower()
    return [c for c in load_contacts()
            if (not contact_type or c.get("type") == contact_type)
            and (not q or q in f"{c['name']} {' '.join(c.get('address_lines',[]))} {c.get('agency','')}".lower())]

def seed_default_contacts():
    defaults = [
        {"name":"Dept. of Corrections and Rehabilitation",
         "address_lines":["P.O. Box 187021","Sacramento, CA 95818-7021","United States"],
         "agency":"CDCR","type":"bill_to"},
        {"name":"SCC - Sierra Conservation Center",
         "address_lines":["5100 O'Byrnes Ferry Road","Jamestown, CA 95327","United States"],
         "agency":"CDCR","type":"ship_to"},
        {"name":"California Health Care Facility (CHCF)",
         "address_lines":["7707 S. Crescencia Drive","Stockton, CA 95215","United States"],
         "agency":"CCHCS","type":"ship_to"},
        {"name":"California Medical Facility (CMF)",
         "address_lines":["1600 California Drive","Vacaville, CA 95696","United States"],
         "agency":"CCHCS","type":"ship_to"},
        {"name":"CalVet - Veterans Home, Yountville",
         "address_lines":["260 California Drive","Yountville, CA 94599","United States"],
         "agency":"CalVet","type":"ship_to"},
        {"name":"CalVet - Veterans Home, Barstow",
         "address_lines":["100 E. Veterans Parkway","Barstow, CA 92311","United States"],
         "agency":"CalVet","type":"ship_to"},
    ]
    existing = {c["name"].lower() for c in load_contacts()}
    n = 0
    for d in defaults:
        if d["name"].lower() not in existing:
            add_contact(d["name"], d["address_lines"], d.get("agency",""), contact_type=d["type"]); n += 1
    return n

def ingest_address_from_rfq(rfq_data):
    addr = rfq_data.get("delivery_address", "")
    if not addr: return None
    parts = [p.strip() for p in addr.split(",")]
    name = parts[0] if parts else ""
    lines = [", ".join(parts[1:])] if len(parts) > 1 else []
    lines.append("United States")
    email = rfq_data.get("requestor_email", "").lower()
    agency = "CalVet" if "calvet" in email else "CCHCS"
    return add_contact(name, lines, agency, email, contact_type="ship_to")


# ═══════════════════════════════════════════════════════════════
# COLUMN GRID — single source of truth for alignment
# ═══════════════════════════════════════════════════════════════
# Widths as fractions of total table width
COL_WIDTHS = [0.06, 0.15, 0.055, 0.065, 0.38, 0.14, 0.15]
COL_LABELS = ["LINE #", "MFG. PART #", "QTY", "UOM", "DESCRIPTION", "UNIT PRICE", "TOTAL PRICE"]
# Header alignment (all center by default)
COL_HDR_ALIGN = ["C", "C", "C", "C", "C", "C", "C"]
# Data alignment
COL_DATA_ALIGN = ["C", "L", "C", "C", "L", "R", "R"]


def _col_edges(lm, tw):
    widths = [tw * p for p in COL_WIDTHS]
    edges = []; x = lm
    for w in widths:
        edges.append(x); x += w
    return edges, widths


def _draw_logo(c, x, y, max_w=130, max_h=45):
    logo = get_logo_path()
    if logo:
        try:
            from reportlab.lib.utils import ImageReader
            img = ImageReader(logo); iw, ih = img.getSize()
            s = min(max_w/iw, max_h/ih); dw, dh = iw*s, ih*s
            c.drawImage(logo, x, y-dh, width=dw, height=dh, preserveAspectRatio=True, mask='auto')
            return dw, dh
        except Exception as e:
            log.warning(f"Logo: {e}")
    c.setFillColor(HexColor("#1a2744"))
    p = c.beginPath(); p.moveTo(x,y); p.lineTo(x+25,y); p.lineTo(x+25,y-18); p.close()
    c.drawPath(p, fill=1)
    c.setFillColor(HexColor("#4a7bcc"))
    p2 = c.beginPath(); p2.moveTo(x,y); p2.lineTo(x,y-18); p2.lineTo(x+25,y-18); p2.close()
    c.drawPath(p2, fill=1)
    return 25, 18


def _wrap(text, n=50):
    words, lines, cur = text.split(), [], ""
    for w in words:
        t = cur + (" " if cur else "") + w
        if len(t) > n and cur: lines.append(cur); cur = w
        else: cur = t
    if cur: lines.append(cur)
    return lines or [""]


def _draw_cell_text(c, text, x, w, y_mid, align="C", font="Helvetica", size=9):
    """Draw single-line text in a cell at vertical center."""
    c.setFont(font, size)
    if align == "L":   c.drawString(x + PAD, y_mid, text)
    elif align == "R": c.drawRightString(x + w - PAD, y_mid, text)
    else:              c.drawCentredString(x + w/2, y_mid, text)


def _draw_multiline(c, lines, x, w, y_top, font="Helvetica", size=8, line_h=11):
    """Draw multi-line text starting from top of cell."""
    c.setFont(font, size)
    ty = y_top - size - 2
    for ln in lines:
        c.drawString(x + PAD, ty, ln)
        ty -= line_h


def _draw_col_borders(c, edges, widths, y_top, y_bot, color=BORDER_CLR, width=0.3):
    """Draw vertical column dividers (side borders)."""
    c.setStrokeColor(color); c.setLineWidth(width)
    # Left edge
    c.line(edges[0], y_top, edges[0], y_bot)
    # Right edge
    rm = edges[-1] + widths[-1]
    c.line(rm, y_top, rm, y_bot)
    # Internal column dividers
    for i in range(1, len(edges)):
        c.line(edges[i], y_top, edges[i], y_bot)


# ═══════════════════════════════════════════════════════════════
# PDF GENERATOR
# ═══════════════════════════════════════════════════════════════

def generate_quote_pdf(
    output_path, quote_number=None, quote_date=None, agency="CCHCS",
    rfq_number="", ship_to_name="", ship_to_address=None,
    bill_to_name=None, bill_to_lines=None,
    line_items=None, terms="Net 45", expiration_date=None,
    tax_rate=None, shipping=0.0,
    show_bill_to=None, show_sellers_permit=None,
    auto_tax=True,
):
    """
    Generate formal Reytech quote PDF.
    If auto_tax=True and tax_rate is None, looks up rate from CDTFA by ship-to zip.
    """
    # ─── Defaults ─────────────────────────────────────────────
    if quote_number is None: quote_number = get_next_quote_number()
    if quote_date is None: quote_date = datetime.now()
    elif isinstance(quote_date, str):
        try: quote_date = datetime.strptime(quote_date, "%m/%d/%Y")
        except: quote_date = datetime.now()
    if expiration_date is None: expiration_date = quote_date + timedelta(days=45)
    elif isinstance(expiration_date, str):
        try: expiration_date = datetime.strptime(expiration_date, "%m/%d/%Y")
        except: expiration_date = quote_date + timedelta(days=45)

    cfg = AGENCY_CONFIG.get(agency, AGENCY_CONFIG["CCHCS"])
    ship_to_address = ship_to_address or []
    line_items = line_items or []
    _show_bill = show_bill_to if show_bill_to is not None else cfg["show_bill_to"]
    _show_permit = show_sellers_permit if show_sellers_permit is not None else cfg["show_sellers_permit"]
    _bill_name = bill_to_name or cfg.get("bill_to_name", "")
    _bill_lines = bill_to_lines or cfg.get("bill_to_lines", [])
    _to_label = cfg.get("to_label", "To")

    # ─── Tax rate: auto-lookup from CDTFA if needed ───────────
    tax_info = None
    if tax_rate is None:
        if auto_tax:
            tax_info = _cdtfa_lookup(
                ship_to_name=ship_to_name,
                ship_to_address=ship_to_address,
            )
            tax_rate = tax_info.get("rate", cfg["default_tax_rate"])
            log.info(f"Tax: {tax_rate} via {tax_info.get('source','')} "
                     f"({tax_info.get('jurisdiction','')})")
        else:
            tax_rate = cfg["default_tax_rate"]

    # ─── Totals ───────────────────────────────────────────────
    for item in line_items:
        item["total_price"] = round(item.get("qty", 0) * item.get("unit_price", 0), 2)
    subtotal = sum(i["total_price"] for i in line_items)
    tax = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax + shipping, 2)
    date_str = quote_date.strftime("%b %d, %Y")
    exp_str = expiration_date.strftime("%b %d, %Y")

    # ─── Canvas ───────────────────────────────────────────────
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    c = canvas.Canvas(output_path, pagesize=letter)
    W, H = letter; LM, RM = 40, W - 40; TW = RM - LM
    edges, widths = _col_edges(LM, TW)
    desc_chars = int(widths[4] / 5.0)  # conservative wrap margin

    # ─── ZONE 1: "QUOTE" ─────────────────────────────────────
    y = H - 55
    c.setFont("Helvetica-Bold", 32); c.setFillColor(black)
    c.drawRightString(RM, y, "QUOTE")
    c.setStrokeColor(black); c.setLineWidth(2)
    c.line(RM - 175, y - 8, RM, y - 8)

    # ─── ZONE 2: Quote#/Date ─────────────────────────────────
    bw = 230; bx = RM - bw; by = y - 14; rh = 24
    for i, (lbl, val) in enumerate([("QUOTE #", quote_number), ("DATE", date_str)]):
        ry = by - (i+1)*rh
        c.setFillColor(HEADER_BG); c.rect(bx, ry, bw, rh, fill=1, stroke=0)
        c.setStrokeColor(black); c.setLineWidth(0.7)
        c.rect(bx, ry, bw, rh, stroke=1, fill=0)
        c.setFillColor(black)
        c.setFont("Helvetica-Bold", 11); c.drawString(bx+10, ry+7, lbl)
        c.setFont("Helvetica-Bold", 13); c.drawRightString(RM-10, ry+7, val)
    qbox_bottom = by - 2*rh

    # ─── ZONE 3: Company info ─────────────────────────────────
    logo_y = H - 52
    lw, lh = _draw_logo(c, LM, logo_y)
    c.setFillColor(black); c.setFont("Helvetica-BoldOblique", 15)
    c.drawString(LM + lw + 10, logo_y - 14, COMPANY["name"])
    cy = logo_y - max(lh, 20) - 10
    c.setFont("Helvetica", 9)
    for ln in [COMPANY["address_1"], COMPANY["city_state"], COMPANY["owner"],
               COMPANY["title"], COMPANY["phone"]]:
        c.drawString(LM, cy, ln); cy -= 13
    cy -= 3; c.drawString(LM, cy, COMPANY["email"]); cy -= 15
    if _show_permit:
        c.setFont("Helvetica", 8.5)
        c.drawString(LM, cy, f"CA Sellers Permit: {COMPANY['sellers_permit']}"); cy -= 15

    # ─── ZONE 4: Bill-to ─────────────────────────────────────
    if _show_bill:
        bill_y = qbox_bottom - 10; bill_x = bx - 40
        c.setFont("Helvetica-Bold", 10); c.drawString(bill_x, bill_y, "Bill to:")
        c.setFont("Helvetica", 9)
        c.drawString(bill_x + 55, bill_y, _bill_name)
        bby = bill_y - 13
        for ln in _bill_lines:
            c.drawString(bill_x + 55, bby, ln); bby -= 13
        zone_bottom = min(cy, bby) - 8
    else:
        zone_bottom = min(cy, qbox_bottom) - 15

    # ─── ZONE 5: Ship-to ─────────────────────────────────────
    y = zone_bottom
    c.setFont("Helvetica-Bold", 11); c.drawString(LM, y, _to_label)
    lbl_w = c.stringWidth(_to_label, "Helvetica-Bold", 11) + 8
    c.setFont("Helvetica", 9.5); c.drawString(LM + lbl_w, y, ship_to_name)
    ay = y - 13
    for ln in ship_to_address:
        c.drawString(LM + lbl_w, ay, ln); ay -= 13
    sx = LM + 270
    c.setFont("Helvetica-Bold", 9.5); c.drawString(sx, y, "Ship to Location:")
    slw = c.stringWidth("Ship to Location:", "Helvetica-Bold", 9.5) + 8
    c.setFont("Helvetica", 9.5); c.drawString(sx + slw, y, ship_to_name)
    sy = y - 13
    for ln in ship_to_address:
        c.drawString(sx + slw, sy, ln); sy -= 13
    y = min(ay, sy) - 6

    # ─── ZONE 6: Info bar ─────────────────────────────────────
    bar_h = 34; col_w4 = TW / 4; bar_y = y - bar_h
    c.setFillColor(HEADER_BG)
    c.rect(LM, bar_y + bar_h/2, TW, bar_h/2, fill=1, stroke=0)
    c.setStrokeColor(black); c.setLineWidth(0.5)
    c.rect(LM, bar_y, TW, bar_h, stroke=1, fill=0)
    c.line(LM, bar_y + bar_h/2, RM, bar_y + bar_h/2)
    for i, (h, v) in enumerate(zip(
        ["Salesperson", "RFQ Number", "Terms", "Expiration Date"],
        [COMPANY["salesperson"], str(rfq_number), terms, exp_str]
    )):
        x = LM + i*col_w4
        if i > 0: c.line(x, bar_y, x, bar_y + bar_h)
        c.setFillColor(black)
        c.setFont("Helvetica-Bold", 9); c.drawCentredString(x + col_w4/2, bar_y + bar_h/2 + 5, h)
        c.setFont("Helvetica", 10); c.drawCentredString(x + col_w4/2, bar_y + 6, v)
    y = bar_y - 8

    # ═══════════════════════════════════════════════════════════
    # ZONE 7: LINE ITEMS TABLE
    # ═══════════════════════════════════════════════════════════

    def _draw_table_header(y_pos):
        """Draw column header row. Returns y position of header bottom."""
        hh = 20; hy = y_pos - hh
        # Background
        c.setFillColor(HEADER_BG); c.rect(LM, hy, TW, hh, fill=1, stroke=0)
        # Horizontal borders
        c.setStrokeColor(black); c.setLineWidth(0.7)
        c.line(LM, hy, RM, hy); c.line(LM, hy + hh, RM, hy + hh)
        # Vertical column borders
        _draw_col_borders(c, edges, widths, hy + hh, hy, BORDER_CLR, 0.5)
        # Labels (center-aligned by default)
        c.setFillColor(black); c.setFont("Helvetica-Bold", 8)
        for i, (label, align) in enumerate(zip(COL_LABELS, COL_HDR_ALIGN)):
            _draw_cell_text(c, label, edges[i], widths[i], hy + 6, align, "Helvetica-Bold", 8)
        return hy

    hdr_bottom = _draw_table_header(y)
    y = hdr_bottom
    table_top = hdr_bottom + 20  # track top of the table for side borders
    page_num = 1

    # ─── Data rows ────────────────────────────────────────────
    for idx, item in enumerate(line_items):
        desc = item.get("description", "").replace("\n", " ")
        dl = _wrap(desc, desc_chars)
        row_h = max(22, len(dl) * 11 + 10)
        ry = y - row_h

        # Page break
        if ry < 80:
            # Draw side borders for this page's table section
            _draw_col_borders(c, edges, widths, table_top, y, BORDER_CLR, 0.5)
            c.setFont("Helvetica", 8); c.setFillColor(HexColor("#888"))
            c.drawRightString(RM, 25, f"Page {page_num}")
            c.showPage(); page_num += 1; y = H - 40
            hdr_bottom = _draw_table_header(y)
            y = hdr_bottom; table_top = hdr_bottom + 20
            ry = y - row_h

        c.setFillColor(black)
        y_mid = ry + row_h/2 - 3  # vertical center for single-line cells

        # Cell data
        _draw_cell_text(c, str(item.get("line_number", idx+1)),
                        edges[0], widths[0], y_mid, COL_DATA_ALIGN[0], "Helvetica", 9)
        _draw_cell_text(c, item.get("part_number", ""),
                        edges[1], widths[1], y_mid, COL_DATA_ALIGN[1], "Helvetica", 9)
        _draw_cell_text(c, str(item.get("qty", "")),
                        edges[2], widths[2], y_mid, COL_DATA_ALIGN[2], "Helvetica", 9)
        _draw_cell_text(c, item.get("uom", ""),
                        edges[3], widths[3], y_mid, COL_DATA_ALIGN[3], "Helvetica", 9)
        _draw_multiline(c, dl, edges[4], widths[4], y, "Helvetica", 8)
        _draw_cell_text(c, f"${item.get('unit_price',0):,.2f}",
                        edges[5], widths[5], y_mid, COL_DATA_ALIGN[5], "Helvetica", 9)
        _draw_cell_text(c, f"${item.get('total_price',0):,.2f}",
                        edges[6], widths[6], y_mid, COL_DATA_ALIGN[6], "Helvetica", 9)

        # Row separator
        c.setStrokeColor(ROW_LINE_CLR); c.setLineWidth(0.3)
        c.line(LM, ry, RM, ry)

        y = ry

    # Side borders for the full data section
    _draw_col_borders(c, edges, widths, table_top, y, BORDER_CLR, 0.5)

    # Table bottom (solid black)
    c.setStrokeColor(black); c.setLineWidth(0.8)
    c.line(LM, y, RM, y)

    # ═══════════════════════════════════════════════════════════
    # ZONE 8: TOTALS — aligned to last 2 columns
    # ═══════════════════════════════════════════════════════════
    tot_x = edges[5]; tot_w = widths[5] + widths[6]; mid_x = edges[6]
    y -= 4

    def _trow(label, value_str, bold=False, shaded=False):
        nonlocal y; trh = 20; y -= trh
        if shaded:
            c.setFillColor(HEADER_BG); c.rect(tot_x, y, tot_w, trh, fill=1, stroke=0)
        c.setStrokeColor(HexColor("#999")); c.setLineWidth(0.3)
        c.rect(tot_x, y, tot_w, trh, stroke=1, fill=0)
        c.line(mid_x, y, mid_x, y + trh)
        c.setFillColor(black)
        c.setFont("Helvetica-Bold", 9); c.drawRightString(mid_x - PAD, y + 6, label)
        f = "Helvetica-Bold" if bold else "Helvetica"
        c.setFont(f, 10); c.drawRightString(RM - PAD, y + 6, value_str)

    _trow("SUBTOTAL", f"${subtotal:,.2f}", shaded=True)
    _trow("SALES TAX", f"${tax:,.2f}")
    _trow("SHIPPING", f"{shipping:.2f}")
    _trow("TOTAL", f"${total:,.2f}", bold=True, shaded=True)

    # Tax rate note
    if tax_info and tax_info.get("source") == "cdtfa_api":
        c.setFont("Helvetica", 6.5); c.setFillColor(HexColor("#999"))
        note = f"Tax rate {tax_rate*100:.3f}% ({tax_info.get('jurisdiction','')}) via CDTFA"
        c.drawRightString(RM, y - 10, note)

    # Footer
    c.setFont("Helvetica", 8); c.setFillColor(HexColor("#888"))
    c.drawRightString(RM, 25, f"{page_num} of {page_num}")
    c.save()

    result = {
        "quote_number": quote_number, "output_path": output_path,
        "subtotal": subtotal, "tax": tax, "tax_rate": tax_rate,
        "shipping": shipping, "total": total, "agency": agency,
        "line_item_count": len(line_items),
    }
    if tax_info:
        result["tax_info"] = tax_info
    return result


def generate_quote_from_rfq(rfq_data, agency=None, tax_rate=None, shipping=0.0, output_dir=None):
    sol = rfq_data.get("solicitation_number", "unknown")
    if agency is None:
        email = rfq_data.get("requestor_email", "").lower()
        agency = "CalVet" if "calvet" in email else "CCHCS"
    raw = rfq_data.get("delivery_address", "")
    parts = [p.strip() for p in raw.split(",")]
    stn = parts[0] if parts else ""
    sta = [", ".join(parts[1:])] if len(parts) > 1 else []
    sta.append("United States")
    ingest_address_from_rfq(rfq_data)
    items = []
    for it in rfq_data.get("line_items", []):
        desc = it.get("description","").replace("\n"," ").strip()
        oem = it.get("item_number","").replace("-","")
        full = f"{desc} OEM#: {oem}" if oem else desc
        items.append({
            "line_number": it.get("line_number", len(items)+1),
            "part_number": it.get("item_number",""), "qty": it.get("qty",0),
            "uom": it.get("uom","EA"), "description": full,
            "unit_price": it.get("price_per_unit",0) or it.get("supplier_cost",0),
        })
    if output_dir is None: output_dir = os.path.join(BASE_DIR, "output", sol)
    qn = get_next_quote_number(); fn = f"{qn}_{agency}.pdf"
    r = generate_quote_pdf(
        output_path=os.path.join(output_dir, fn), quote_number=qn,
        agency=agency, rfq_number=sol, ship_to_name=stn,
        ship_to_address=sta, line_items=items, tax_rate=tax_rate, shipping=shipping,
    )
    r["filename"] = fn; return r


if __name__ == "__main__":
    seed_default_contacts()
    items = [
        {"line_number":1,"part_number":"6500-001-430","qty":2,"uom":"SET",
         "description":"X-RESTRAINT PACKAGE by Stryker Medical New OEM Original Outright OEM#: 6500001430",
         "unit_price":454.40},
        {"line_number":2,"part_number":"6250-001-125","qty":2,"uom":"EACH",
         "description":"RESTRAINT STRAP, CHEST, GREEN, FOR USE WITH: FOR MODEL 6250/6251/6252 STAIR-PRO\u00ae STAIR CHAIR by Stryker Medical New OEM Original Outright OEM#: 6250001125",
         "unit_price":69.12},
        {"line_number":3,"part_number":"6250-001-126","qty":2,"uom":"EACH",
         "description":"RESTRAINT STRAP, CHEST, BLACK, FOR USE WITH: FOR MODEL 6250/6251/6252 STAIR-PRO\u00ae STAIR CHAIR by Stryker Medical OEM#: 6250001126",
         "unit_price":69.12},
    ]
    ship = ["5100 O'Byrnes Ferry Road", "Jamestown, CA 95327", "United States"]
    for ag in ["CalVet", "CCHCS"]:
        r = generate_quote_pdf(
            output_path=f"test_quote_{ag}.pdf", quote_number="R26Q14",
            quote_date=datetime(2026, 2, 11), agency=ag, rfq_number="10838043",
            ship_to_name="SCC - Sierra Conservation Center",
            ship_to_address=ship, line_items=items,
            auto_tax=False, tax_rate=0.0725,  # Use known rate for test
        )
        print(f"{ag}: ${r['total']:,.2f}")

    # Test zip extraction
    print(f"\nZip test: {extract_zip_from_address(ship)}")
    print(f"City test: {extract_city_from_address(ship)}")
    print(f"Next quote: {peek_next_quote_number()}")
