"""Overlay-based fillers for DSH (Department of State Hospitals) packet attachments.

DSH ships per-solicitation packets through Proofpoint secure mail with three
flat (zero AcroForm field) PDFs the bidder must return filled:

  Attachment A  — Bidder's Information & Certifications  (vendor identity + sig)
  Attachment B  — Goods & Services Pricing Page          (line-item unit prices + totals)
  Attachment C  — Required Forms checklist               (vendor name only)

Because the templates are flat, we draw with reportlab on top of the original
buyer PDF and merge each canvas page back onto the source via pypdf. The
coordinates were measured directly from a real packet (DSH 25CB020) using
pdfplumber; see scripts/measure_dsh_atts.py for the survey that produced them.

Coordinate system: reportlab uses bottom-up Y. Anchor labels were located at
their pdfplumber `bottom` (top-down), then converted via
    rl_y = page_height - pdfplumber_bottom - 12
so the value sits ~12pt below the label, inside its cell.

These fillers expect the BUYER'S attachment PDF as input — not a generic
template — because the layout (line items, qty, sol number) is solicitation-
specific. The +RFQ manual upload pipeline persists the buyer files under
data/uploads/manual_rfq/<ts>_<idx>_<safefn>.pdf; package_engine resolves the
right file by filename pattern.
"""
from __future__ import annotations

import io
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("reytech.dsh_attachments")

try:
    from pypdf import PdfReader, PdfWriter
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

try:
    from reportlab.pdfgen import canvas as rl_canvas
    from reportlab.lib.utils import ImageReader
    HAS_RL = True
except ImportError:
    HAS_RL = False


# ── Shared helpers ───────────────────────────────────────────────────────

def _today_mmddyyyy() -> str:
    return datetime.now().strftime("%m/%d/%Y")


def _money(v) -> str:
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return ""


def _signature_png_path() -> Optional[str]:
    here = os.path.dirname(os.path.abspath(__file__))
    for p in (os.path.join(here, "signature_transparent.png"),
              "/app/src/forms/signature_transparent.png"):
        if os.path.exists(p):
            return p
    return None


def _overlay_pdf(
    src_pdf_bytes: bytes,
    draw_fn,
) -> Optional[io.BytesIO]:
    """Draw onto every page of src_pdf via reportlab, then merge each
    canvas page onto the source. `draw_fn(canvas, page_index, page_w, page_h)`
    issues the drawing commands.

    Returns BytesIO of the merged PDF, or None on failure.
    """
    if not HAS_PYPDF or not HAS_RL:
        log.error("missing pypdf or reportlab — cannot overlay")
        return None
    try:
        reader = PdfReader(io.BytesIO(src_pdf_bytes))
        writer = PdfWriter(clone_from=reader)
    except Exception as e:
        log.error("DSH overlay: source load failed: %s", e)
        return None

    for pi, page in enumerate(writer.pages):
        try:
            mb = page.mediabox
            page_w, page_h = float(mb.width), float(mb.height)
        except Exception:
            page_w, page_h = 612.0, 792.0
        buf = io.BytesIO()
        c = rl_canvas.Canvas(buf, pagesize=(page_w, page_h))
        try:
            draw_fn(c, pi, page_w, page_h)
        except Exception as e:
            log.error("DSH overlay: draw_fn page %d: %s", pi, e)
            c.save()
            continue
        c.save()
        buf.seek(0)
        try:
            page.merge_page(PdfReader(buf).pages[0])
        except Exception as e:
            log.error("DSH overlay: merge page %d: %s", pi, e)
            continue

    out = io.BytesIO()
    try:
        writer.write(out)
    except Exception as e:
        log.error("DSH overlay: writer.write failed: %s", e)
        return None
    out.seek(0)
    return out


def _read_bytes(pdf: Any) -> Optional[bytes]:
    """Accept a path str, an open file, or raw bytes."""
    if pdf is None:
        return None
    if isinstance(pdf, (bytes, bytearray)):
        return bytes(pdf)
    if isinstance(pdf, io.BytesIO):
        pdf.seek(0)
        return pdf.read()
    if isinstance(pdf, str) and os.path.exists(pdf):
        with open(pdf, "rb") as fh:
            return fh.read()
    if hasattr(pdf, "read"):
        return pdf.read()
    return None


def _company(reytech_info: Dict[str, Any]) -> Dict[str, Any]:
    return (reytech_info or {}).get("company", {}) or {}


def _draw_text(c, x, y, text, font="Helvetica", size=9):
    if text is None:
        return
    c.setFont(font, size)
    c.drawString(x, y, str(text))


# ── Attachment A — Bidder's Information ──────────────────────────────────
#
# Anchor coordinates (reportlab Y, bottom-up) measured from a real packet.
# Each Y is `page_height - pdfplumber_bottom - 12`, placing the value ~12pt
# below its label inside the cell.

_ATT_A_FIELDS = {
    # (x, y, key)  — key resolved against company / parsed
    "firm_name":          (28.0, 482.3),
    "sellers_permit":     (415.0, 482.3),
    "firm_address":       (28.0, 453.2),
    "city_state_zip":     (358.0, 453.2),
    "fein":               (28.0, 424.2),
    "lead_time":          (28.0, 395.1),
    "warranty":           (174.0, 395.1),
    "sol_expires":        (358.0, 395.1),
    "dvbe_pct":           (485.0, 395.1),
    "contact_name":       (28.0, 339.0),
    "phone":              (358.0, 339.0),
    "email":              (485.0, 339.0),
    "signer_title":       (28.0, 120.1),
    "date_executed":      (485.0, 90.4),
}


def fill_dsh_attachment_a(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
    *,
    src_pdf: Any,
) -> Optional[io.BytesIO]:
    """Fill DSH Attachment A — Bidder's Information & Certifications.

    Overlays vendor identity, certs, lead time, warranty, contact info, and
    signature onto the buyer's flat AttA PDF.
    """
    src = _read_bytes(src_pdf)
    if src is None:
        log.error("AttA: src_pdf unreadable")
        return None
    co = _company(reytech_info)
    sig_path = _signature_png_path()

    values = {
        "firm_name":      co.get("name", ""),
        "sellers_permit": co.get("sellers_permit", ""),
        "firm_address":   co.get("street", ""),
        "city_state_zip": f"{co.get('city', '')}, {co.get('state', '')} {co.get('zip', '')}".strip(", "),
        "fein":           co.get("fein", ""),
        "lead_time":      (parsed or {}).get("lead_time", "5-7 business days"),
        "warranty":       (parsed or {}).get("warranty", "Per manufacturer"),
        "sol_expires":    (parsed or {}).get("sol_expires", ""),
        "dvbe_pct":       (parsed or {}).get("dvbe_pct", "100%"),
        "contact_name":   co.get("owner", ""),
        "phone":          co.get("phone", ""),
        "email":          co.get("email", ""),
        "signer_title":   f"{co.get('owner', '')}, {co.get('title', 'Owner')}",
        "date_executed":  _today_mmddyyyy(),
    }

    def draw(c, pi, pw, ph):
        if pi != 0:
            return
        for key, (x, y) in _ATT_A_FIELDS.items():
            _draw_text(c, x, y, values.get(key, ""))
        # SB / DVBE checkboxes — both YES (Reytech is SB/DVBE certified)
        cert = co.get("cert_number", "")
        # SB YES (cert # cell)
        _draw_text(c, 174.0, 424.2, f"SB:Y  Cert#{cert}", size=8)
        # DVBE YES (cert # cell)
        _draw_text(c, 358.0, 424.2, f"DVBE:Y  Cert#{cert}", size=8)
        # Signature image at signature line
        if sig_path:
            try:
                c.drawImage(
                    ImageReader(sig_path),
                    50.0, 80.0, width=130.0, height=30.0,
                    mask="auto", preserveAspectRatio=True, anchor="sw",
                )
            except Exception as e:
                log.debug("AttA signature draw failed: %s", e)

    return _overlay_pdf(src, draw)


# ── Attachment B — Goods & Services Pricing Page ─────────────────────────
#
# Pricing rows 1-7 measured from a real DSH packet. UNIT PRICE column starts
# at x≈455 (right-aligned). The Y centers below place text vertically inside
# each row. Subtotal/Other/Total cells anchored from their label bottoms.

_ATT_B_ROW_Y = {
    1: 443.1,
    2: 404.9,
    3: 366.4,
    4: 327.9,
    5: 293.8,
    6: 263.7,
    7: 233.5,
}
_ATT_B_UNIT_X = 482.0     # right edge of UNIT PRICE column (past UOM/CS)
_ATT_B_EXT_X  = 575.0     # right edge of EXTENSION column
_ATT_B_VENDOR_XY = (90.0, 640.7)
# Totals: value sits on the SAME row as the label (label-left, value-right),
# so use rl_y = page_h - label_bottom (no +12 cell-below offset).
_ATT_B_SUBTOTAL_XY = (575.0, 130.6)
_ATT_B_OTHER_XY    = (575.0, 116.8)
_ATT_B_TOTAL_XY    = (575.0, 103.0)


def _line_items(parsed: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pull line items from the parsed RFQ in priced form. Falls back to an
    empty list if the parser hasn't filled prices yet (operator can edit
    the PDF before sending)."""
    items = (parsed or {}).get("items") or (parsed or {}).get("line_items") or []
    if not isinstance(items, list):
        return []
    return items


def fill_dsh_attachment_b(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
    *,
    src_pdf: Any,
) -> Optional[io.BytesIO]:
    """Fill DSH Attachment B — Pricing Page.

    Overlays vendor name + per-line unit prices + extensions + subtotal
    + total onto the buyer's flat AttB PDF. Currently supports up to 7
    rows (the standard DSH AttB capacity); items beyond row 7 are
    reported in `warnings` upstream rather than silently dropped.
    """
    src = _read_bytes(src_pdf)
    if src is None:
        log.error("AttB: src_pdf unreadable")
        return None
    co = _company(reytech_info)
    items = _line_items(parsed)

    subtotal = 0.0
    rows: List[Tuple[int, float, float, float]] = []
    for idx, item in enumerate(items[:7], start=1):
        qty = float(item.get("qty") or item.get("quantity") or 0)
        unit = float(item.get("unit_price") or item.get("price") or 0)
        ext = qty * unit
        subtotal += ext
        rows.append((idx, qty, unit, ext))

    other = float((parsed or {}).get("other_charges") or 0.0)
    total = subtotal + other

    def draw(c, pi, pw, ph):
        if pi != 0:
            return
        # Vendor name
        _draw_text(c, *_ATT_B_VENDOR_XY, co.get("name", ""))
        # Per-row unit price + extension (right-aligned)
        c.setFont("Helvetica", 9)
        for (idx, qty, unit, ext) in rows:
            y = _ATT_B_ROW_Y.get(idx)
            if y is None:
                continue
            c.drawRightString(_ATT_B_UNIT_X, y, _money(unit))
            c.drawRightString(_ATT_B_EXT_X,  y, _money(ext))
        # Totals
        c.drawRightString(*_ATT_B_SUBTOTAL_XY, _money(subtotal))
        c.drawRightString(*_ATT_B_OTHER_XY,    _money(other))
        c.setFont("Helvetica-Bold", 10)
        c.drawRightString(*_ATT_B_TOTAL_XY,    _money(total))

    return _overlay_pdf(src, draw)


# ── Attachment C — Required Forms checklist ──────────────────────────────
#
# Vendor only fills "Vendor Name:". The checkboxes pre-printed by DSH
# indicate which forms must accompany the bid; we don't need to interact
# with them.

_ATT_C_VENDOR_XY = (90.0, 666.4)


def fill_dsh_attachment_c(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
    *,
    src_pdf: Any,
) -> Optional[io.BytesIO]:
    """Fill DSH Attachment C — Required Forms.

    Overlays vendor name onto the buyer's flat AttC PDF. The form checklist
    itself is buyer-defined; vendor responsibility is just to attest by name.
    """
    src = _read_bytes(src_pdf)
    if src is None:
        log.error("AttC: src_pdf unreadable")
        return None
    co = _company(reytech_info)

    def draw(c, pi, pw, ph):
        if pi != 0:
            return
        _draw_text(c, *_ATT_C_VENDOR_XY, co.get("name", ""))

    return _overlay_pdf(src, draw)


# ── Dispatcher ───────────────────────────────────────────────────────────

FILLERS = {
    "fill_dsh_attachment_a": fill_dsh_attachment_a,
    "fill_dsh_attachment_b": fill_dsh_attachment_b,
    "fill_dsh_attachment_c": fill_dsh_attachment_c,
}
