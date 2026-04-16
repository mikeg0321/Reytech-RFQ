"""Fill Engine — profile-driven PDF form filling.

Single entry point: fill(quote, profile) → bytes.
One function for PCs and RFQs. The profile determines HOW to fill
(AcroForm vs overlay vs hybrid). The Quote provides WHAT to fill.

Usage:
    from src.forms.fill_engine import fill
    from src.core.quote_model import Quote
    from src.forms.profile_registry import load_profiles, match_profile

    profiles = load_profiles()
    profile = match_profile(pdf_path, profiles) or profiles["704a_reytech_standard"]
    quote = Quote.from_legacy_dict(pc_dict, doc_type="pc")

    filled_pdf_bytes = fill(quote, profile)
"""
import io
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.core.quote_model import Quote
from src.forms.profile_registry import FormProfile

log = logging.getLogger(__name__)

_PST = timezone(timedelta(hours=-8))


def fill(quote: Quote, profile: FormProfile) -> bytes:
    """Fill a PDF form using the profile's field map and the Quote's data.

    Args:
        quote: Canonical Quote object with all data
        profile: Form profile declaring field mappings + fill mode

    Returns:
        Filled PDF as bytes (ready to write to disk or serve)

    Raises:
        ValueError: if profile.blank_pdf doesn't exist
        RuntimeError: if fill fails
    """
    if not profile.blank_pdf or not os.path.exists(profile.blank_pdf):
        raise ValueError(f"Blank PDF not found: {profile.blank_pdf}")

    if profile.fill_mode == "acroform":
        return _fill_acroform(quote, profile)
    elif profile.fill_mode == "overlay":
        return _fill_overlay(quote, profile)
    elif profile.fill_mode == "hybrid":
        return _fill_hybrid(quote, profile)
    else:
        raise ValueError(f"Unknown fill_mode: {profile.fill_mode}")


def _fill_acroform(quote: Quote, profile: FormProfile) -> bytes:
    """Fill using PyPDFForm — AcroForm field fill + flatten."""
    from PyPDFForm import PdfWrapper

    field_values = {}

    # ── Static fields (vendor, header, buyer, ship_to, totals) ──
    field_map = _build_static_field_map(quote, profile)
    field_values.update(field_map)

    # ── Row items ──
    row_values = _build_row_field_map(quote, profile)
    field_values.update(row_values)

    # ── Page metadata ──
    total_pages = _compute_page_count(quote, profile)
    page_field = profile.get_field("page.number")
    of_field = profile.get_field("page.of")
    if page_field:
        field_values[page_field.pdf_field] = "1"
    if of_field:
        field_values[of_field.pdf_field] = str(total_pages)

    # ── Fill ──
    log.info("fill_acroform: %s, %d fields, %d items",
             profile.id, len(field_values), len(quote.line_items))

    try:
        from pypdf import PdfReader, PdfWriter
        from pypdf.generic import BooleanObject, NameObject, TextStringObject, NumberObject

        reader = PdfReader(profile.blank_pdf)
        # clone_from preserves AcroForm + /DR (default resources) end-to-end.
        # This is the approach from PR #88 confirmed working on 2026-04-15.
        # Fields remain EDITABLE — buyer can still open in Adobe and sign.
        writer = PdfWriter(clone_from=reader)

        # NeedAppearances tells the viewer to generate appearance streams on open.
        # Chrome, Edge, Acrobat all honor this. Fields render AND stay editable.
        if "/AcroForm" in writer._root_object:
            writer._root_object["/AcroForm"][NameObject("/NeedAppearances")] = BooleanObject(True)

        # Clear signature field (will be stamped as image overlay on approval)
        field_values["Signature and Date"] = " "

        # Fill text fields
        for page in writer.pages:
            writer.update_page_form_field_values(page, field_values)

        # Fill checkboxes — pypdf doesn't handle these in update_page_form_field_values
        cb_values = {
            "shipping.fob_prepaid": quote.header.shipping_terms == "FOB Destination",
            "shipping.fob_ppadd": False,
            "shipping.fob_collect": False,
            "header.price_check": quote.doc_type.value == "pc",
            "header.am_pst": quote.header.due_time is not None and quote.header.due_time.hour < 12,
            "header.pm_pst": quote.header.due_time is not None and quote.header.due_time.hour >= 12,
        }
        checkbox_map = {}
        for fm in profile.fields:
            if "[n]" in fm.semantic:
                continue
            if fm.semantic in cb_values and cb_values[fm.semantic]:
                checkbox_map[fm.pdf_field] = True

        for page in writer.pages:
            for annot_ref in (page.get("/Annots") or []):
                try:
                    annot = annot_ref.get_object()
                    name = str(annot.get("/T", ""))
                    if name in checkbox_map:
                        # Find the "on" state from /AP/N keys
                        ap = annot.get("/AP", {})
                        if hasattr(ap, 'get_object'):
                            ap = ap.get_object()
                        ap_n = ap.get("/N", {}) if isinstance(ap, dict) else {}
                        if hasattr(ap_n, 'get_object'):
                            ap_n = ap_n.get_object()
                        on_states = [str(k).lstrip("/") for k in (ap_n.keys() if isinstance(ap_n, dict) else []) if str(k) != "/Off"]
                        if on_states:
                            on = on_states[0]
                            annot[NameObject("/V")] = NameObject(f"/{on}")
                            annot[NameObject("/AS")] = NameObject(f"/{on}")
                except Exception:
                    pass

        # Signature is NOT applied at generate time — it's applied on approval.
        # The generate step produces an editable draft. The approve/send step
        # adds the PNG signature stamp + date and locks the PDF.

        result_buf = io.BytesIO()
        writer.write(result_buf)
        result = result_buf.getvalue()
        log.info("fill_acroform: success, %d bytes (clone_from + NeedAppearances + sig stamp)", len(result))
        return result
    except Exception as e:
        log.error("fill_acroform failed for %s: %s", profile.id, e, exc_info=True)
        raise RuntimeError(f"AcroForm fill failed: {e}") from e


def _fill_overlay(quote: Quote, profile: FormProfile) -> bytes:
    """Fill using reportlab overlay at profile-declared coordinates.

    Used for flat/scanned buyer PDFs where AcroForm fields don't exist.
    The profile declares exact (x, y, width, height) for each field.
    """
    # TODO: Implement when overlay profiles are defined
    raise NotImplementedError("Overlay fill mode not yet implemented — use Simple Submit as fallback")


def _fill_hybrid(quote: Quote, profile: FormProfile) -> bytes:
    """AcroForm for fields that exist, overlay for the rest."""
    # TODO: Implement when hybrid profiles are defined
    raise NotImplementedError("Hybrid fill mode not yet implemented — use Simple Submit as fallback")


# ── Field map builders ───────────────────────────────────────────────────────

def _build_static_field_map(quote: Quote, profile: FormProfile) -> dict[str, str]:
    """Build field values for non-row fields (vendor, header, buyer, etc.)."""
    values = {}

    # Map semantic paths to Quote attribute accessors
    accessors = {
        # Vendor
        "vendor.name": quote.vendor.name,
        "vendor.supplier_name": quote.vendor.name,
        "vendor.representative": quote.vendor.representative,
        "vendor.address": quote.vendor.address.display(),
        "vendor.phone": quote.vendor.phone,
        "vendor.email": quote.vendor.email,
        "vendor.sb_cert": quote.vendor.sb_cert,
        "vendor.dvbe_cert": quote.vendor.dvbe_cert,
        "vendor.delivery": quote.header.delivery_days,
        "vendor.discount": "Included",
        "vendor.expires": _expiry_date(),
        "vendor.signature": "",  # Handled separately by signature stamp

        # Header
        "header.solicitation_number": quote.header.solicitation_number,
        "header.due_date": (
            quote.header.due_date.strftime("%m/%d/%Y") if quote.header.due_date else ""
        ),
        "header.due_time": (
            quote.header.due_time.strftime("%I:%M %p") if quote.header.due_time else ""
        ),

        # Buyer
        "buyer.requestor_name": quote.buyer.requestor_name,
        "buyer.institution": quote.header.institution_key,
        "buyer.phone": quote.buyer.requestor_phone,
        "buyer.date_of_request": datetime.now(_PST).strftime("%m/%d/%Y"),

        # Ship to
        "ship_to.address": quote.ship_to.display(),
        "ship_to.zip_code": quote.ship_to.zip_code,

        # Totals — always write values (never blank, even if $0)
        "totals.subtotal": _fmt_money(quote.subtotal) or "0.00",
        "totals.freight": "0.00",
        "totals.tax": "0.00",
        "totals.total": _fmt_money(quote.subtotal) or "0.00",
        "totals.notes": quote.buyer.notes,
    }

    # Checkboxes
    checkbox_values = {
        "header.am_pst": (
            quote.header.due_time is not None
            and quote.header.due_time.hour < 12
        ),
        "header.pm_pst": (
            quote.header.due_time is not None
            and quote.header.due_time.hour >= 12
        ),
        "header.price_check": quote.doc_type.value == "pc",
        "shipping.fob_prepaid": quote.header.shipping_terms == "FOB Destination",
        "shipping.fob_ppadd": False,
        "shipping.fob_collect": False,
    }

    for fm in profile.fields:
        if "[n]" in fm.semantic:
            continue  # Row fields handled separately

        if fm.semantic in accessors:
            val = accessors[fm.semantic]
            if val:
                values[fm.pdf_field] = str(val)
        elif fm.semantic in checkbox_values:
            if checkbox_values[fm.semantic]:
                values[fm.pdf_field] = True

    return values


def _build_row_field_map(quote: Quote, profile: FormProfile) -> dict[str, str]:
    """Build field values for item rows across all pages."""
    values = {}
    active_items = [it for it in quote.line_items if not it.no_bid]

    capacities = profile.page_row_capacities
    if not capacities:
        return values

    item_idx = 0
    for page_num, capacity in enumerate(capacities, start=1):
        for row in range(1, capacity + 1):
            if item_idx >= len(active_items):
                break

            item = active_items[item_idx]
            row_fields = profile.get_row_fields(row, page=page_num)

            for sem, pdf_field in row_fields.items():
                val = _get_item_field_value(item, sem)
                if val:
                    values[pdf_field] = val

            item_idx += 1

    return values


def _get_item_field_value(item, semantic: str) -> str:
    """Extract a value from a LineItem by semantic field name."""
    # semantic looks like "items[3].description" — extract the field part
    parts = semantic.split(".")
    if len(parts) < 2:
        return ""
    field = parts[-1]

    mapping = {
        "item_no": str(item.line_no),
        "description": item.description,
        "qty": str(int(item.qty)) if item.qty == int(item.qty) else str(item.qty),
        "uom": item.uom,
        "qty_per_uom": str(item.qty_per_uom) if item.qty_per_uom != 1 else "",
        "unit_price": _fmt_money(item.unit_price),
        "extension": _fmt_money(item.extension),
        "substituted": item.item_no,  # MFG# goes in substituted item column
    }

    return mapping.get(field, "")


def _compute_page_count(quote: Quote, profile: FormProfile) -> int:
    """How many pages needed for the quote's items."""
    active = len([it for it in quote.line_items if not it.no_bid])
    total = 0
    pages = 0
    for cap in profile.page_row_capacities:
        pages += 1
        total += cap
        if total >= active:
            return pages
    return max(pages, 1)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _fmt_money(val: Decimal) -> str:
    """Format a Decimal as a dollar string without the $ sign."""
    if val == 0:
        return ""
    return f"{float(val):.2f}"


def _expiry_date(days: int = 45) -> str:
    """Price check expiry date (45 days from today in PST)."""
    return (datetime.now(_PST) + timedelta(days=days)).strftime("%m/%d/%Y")


def approve_and_sign(draft_pdf_bytes: bytes, signature_image_path: str = "") -> bytes:
    """Apply PNG signature stamp + date to a draft PDF, then flatten (lock).

    Called on operator approval — NOT during generate. The generate step
    produces an editable draft. This function locks it for sending.

    Args:
        draft_pdf_bytes: The editable draft PDF from fill()
        signature_image_path: Path to PNG signature image. If empty, uses
            the default Reytech signature from data/reytech_logo.png or
            the vendor config.

    Returns:
        Signed + flattened PDF as bytes
    """
    from pypdf import PdfReader, PdfWriter
    from pypdf.generic import NameObject
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas as rl_canvas

    reader = PdfReader(io.BytesIO(draft_pdf_bytes))
    writer = PdfWriter(clone_from=reader)

    # Find handwritten signature PNG (transparent background)
    if not signature_image_path:
        for candidate in [
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "signature_transparent.png"),
            "src/forms/signature_transparent.png",
            "signature_transparent.png",
            os.path.join(os.environ.get("DATA_DIR", "data"), "signature_transparent.png"),
        ]:
            if os.path.exists(candidate):
                signature_image_path = candidate
                break

    # Create a reportlab overlay with signature + date on page 1
    page_w = float(reader.pages[0].mediabox.width)
    page_h = float(reader.pages[0].mediabox.height)
    sig_overlay = io.BytesIO()
    c = rl_canvas.Canvas(sig_overlay, pagesize=(page_w, page_h))

    # "Signature and Date" field rect: [280, 390, 602, 412] (PDF coords, bottom-left origin)
    # Layout: SIGNATURE on left, DATE on right
    # Keep signature INSIDE the field — don't bleed into Phone Number row below
    field_x0, field_y0, field_x1, field_y1 = 285, 393, 600, 411

    # White-out the old "R. Michael Guadan" text that's baked into the template
    c.setFillColorRGB(1, 1, 1)
    c.rect(field_x0 - 5, field_y0 - 4, field_x1 - field_x0 + 10, field_y1 - field_y0 + 4, fill=1, stroke=0)
    c.setFillColorRGB(0, 0, 0)

    # Signature image on the LEFT side
    sig_w = 210
    sig_h = 20

    if signature_image_path and os.path.exists(signature_image_path):
        try:
            # mask='auto' uses the alpha channel for transparency
            c.drawImage(signature_image_path, field_x0 + 20, field_y0, sig_w, sig_h,
                        preserveAspectRatio=True, anchor='sw', mask='auto')
        except Exception as e:
            log.warning("Signature image failed: %s — writing text instead", e)
            c.setFont("Helvetica-Oblique", 12)
            c.drawString(field_x0 + 5, field_y0 + 5, "R. Michael Guadan")

    # Date on the RIGHT side of the field
    date_str = datetime.now(_PST).strftime("%m/%d/%Y")
    c.setFillColorRGB(0, 0, 0)
    c.setFont("Helvetica", 11)
    c.drawString(field_x1 - 80, field_y0 + 5, date_str)

    c.save()
    sig_overlay.seek(0)

    # Merge signature overlay onto page 1
    sig_reader = PdfReader(sig_overlay)
    writer.pages[0].merge_page(sig_reader.pages[0])

    # Write result
    result_buf = io.BytesIO()
    writer.write(result_buf)
    result = result_buf.getvalue()
    log.info("approve_and_sign: %d bytes (signature + date stamped)", len(result))
    return result
