"""Per-attachment fillers for CCHCS Non-IT RFQ packet templates.

Each fill function takes the Reytech identity+compliance dict and the
parsed packet context (solicitation #, line items, etc.), loads the
corresponding blank template from data/templates/, writes the right
values into its form fields, merges a signature PNG onto the signature
widget, and returns an in-memory `BytesIO` of the filled PDF ready to
be spliced into the output packet.

Field names are pulled from the blank templates themselves — see
_overnight_review/template_fields.json for the full schema of each.
Reytech ground truth values come from the reference packages at
_overnight_review/attachment_ground_truth.json.

Canonical identity per project memory: Michael Guadan / Owner /
sales@reytechinc.com (no variants).
"""
from __future__ import annotations

import io
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

log = logging.getLogger("reytech.cchcs_attachments")

try:
    from pypdf import PdfReader, PdfWriter
    from pypdf.generic import (
        ArrayObject,
        BooleanObject,
        DictionaryObject,
        NameObject,
        TextStringObject,
    )
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False


# ── Template path resolution ─────────────────────────────────────────────

def _template_path(filename: str) -> Optional[str]:
    """Find a template by filename under data/templates/. Checks repo
    root first (dev), then /app (Railway)."""
    candidates = []
    try:
        repo_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        candidates.append(os.path.join(repo_root, "data", "templates", filename))
    except Exception as _e:
        log.debug('suppressed in _template_path: %s', _e)
    candidates.append(os.path.join("/app", "data", "templates", filename))
    return next((p for p in candidates if os.path.exists(p)), None)


def _signature_png_path() -> Optional[str]:
    here = os.path.dirname(os.path.abspath(__file__))
    for p in [
        os.path.join(here, "signature_transparent.png"),
        "/app/src/forms/signature_transparent.png",
    ]:
        if os.path.exists(p):
            return p
    return None


# ── Shared helpers ───────────────────────────────────────────────────────

def _today_mmddyyyy() -> str:
    return datetime.now().strftime("%m/%d/%Y")


def _full_field_name(annot: Any) -> str:
    parts: List[str] = []
    node: Any = annot
    seen = set()
    while node is not None and id(node) not in seen:
        seen.add(id(node))
        t = node.get("/T") if hasattr(node, "get") else None
        if t is not None:
            parts.append(str(t))
        p = node.get("/Parent") if hasattr(node, "get") else None
        if p is None:
            break
        try:
            node = p.get_object() if hasattr(p, "get_object") else p
        except Exception:
            break
    return ".".join(reversed(parts))


def _set_need_appearances(writer: "PdfWriter") -> None:
    try:
        root = writer._root_object
        if "/AcroForm" not in root:
            return
        af = root["/AcroForm"]
        if hasattr(af, "get_object"):
            af = af.get_object()
        af[NameObject("/NeedAppearances")] = BooleanObject(True)
    except Exception as e:
        log.debug("NeedAppearances set failed: %s", e)


def _best_on_state(annot: Any) -> Optional[str]:
    try:
        ap = annot.get("/AP")
        if ap is None:
            return None
        ap = ap.get_object() if hasattr(ap, "get_object") else ap
        n = ap.get("/N")
        if n is None:
            return None
        n = n.get_object() if hasattr(n, "get_object") else n
        for key in n.keys():
            k = str(key)
            if k != "/Off":
                return k
    except Exception as _e:
        log.debug('suppressed in _best_on_state: %s', _e)
    return None


def _apply_checkbox_updates(writer: "PdfWriter", updates: Dict[str, Any]) -> int:
    """Walk every page's annotations and tick/untick checkbox widgets
    whose full name matches an update key."""
    if not updates:
        return 0
    written = 0
    for page in writer.pages:
        annots = page.get("/Annots")
        if annots is None:
            continue
        try:
            annots = annots.get_object() if hasattr(annots, "get_object") else annots
        except Exception:
            continue
        for annot_ref in annots:
            try:
                annot = annot_ref.get_object()
                name = _full_field_name(annot)
                if not name or name not in updates:
                    continue
                desired = updates[name]
                if desired in (True, "/Yes", "Yes", "yes", 1, "1", "/On", "On"):
                    export_name = _best_on_state(annot) or "/Yes"
                else:
                    export_name = "/Off"
                export = NameObject(export_name)
                annot[NameObject("/V")] = export
                annot[NameObject("/AS")] = export
                try:
                    parent = annot.get("/Parent")
                    if parent is not None:
                        pobj = parent.get_object()
                        pobj[NameObject("/V")] = export
                except Exception as _e:
                    log.debug('suppressed in _apply_checkbox_updates: %s', _e)
                written += 1
            except Exception:
                continue
    return written


def _overlay_signature_on_widgets(
    writer: "PdfWriter",
    target_field_names: tuple,
) -> int:
    """Draw the Reytech signature PNG inside any widget whose field name
    exactly or endswith-matches one of the target names. Returns number
    of successful overlays. Clears the widget /V first so typed text
    doesn't bleed through.
    """
    sig_path = _signature_png_path()
    if not sig_path:
        return 0
    try:
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.lib.utils import ImageReader
    except ImportError:
        return 0

    drawn = 0
    for page_idx, page in enumerate(writer.pages):
        annots = page.get("/Annots")
        if annots is None:
            continue
        try:
            annots = annots.get_object() if hasattr(annots, "get_object") else annots
        except Exception:
            continue
        for annot_ref in annots:
            try:
                annot = annot_ref.get_object()
                full = _full_field_name(annot)
                if not any(full == t or full.endswith(t) for t in target_field_names):
                    continue
                rect = annot.get("/Rect")
                if rect is None:
                    continue
                sig_rect = tuple(float(x) for x in rect)
                try:
                    annot[NameObject("/V")] = TextStringObject("")
                except Exception as _e:
                    log.debug('suppressed in _overlay_signature_on_widgets: %s', _e)

                page_w, page_h = 612.0, 792.0
                try:
                    mb = page.mediabox
                    page_w, page_h = float(mb.width), float(mb.height)
                except Exception as _e:
                    log.debug('suppressed in _overlay_signature_on_widgets: %s', _e)
                fl, fb, fr, ft = sig_rect
                pad = 2.0
                buf = io.BytesIO()
                c = rl_canvas.Canvas(buf, pagesize=(page_w, page_h))
                c.drawImage(
                    ImageReader(sig_path),
                    fl + pad,
                    fb + pad,
                    width=(fr - fl) - pad * 2,
                    height=(ft - fb) - pad * 2,
                    mask="auto",
                    preserveAspectRatio=True,
                    anchor="sw",
                )
                c.save()
                buf.seek(0)
                page.merge_page(PdfReader(buf).pages[0])
                drawn += 1
            except Exception as e:
                log.debug("overlay sig on %s failed: %s", full if 'full' in dir() else '?', e)
                continue
    return drawn


def _fill_and_serialize(
    template_path: str,
    text_updates: Dict[str, str],
    checkbox_updates: Dict[str, Any],
    signature_targets: tuple = (),
) -> Optional[io.BytesIO]:
    """Load a blank template, apply text + checkbox updates, overlay
    signature PNG, and return a BytesIO of the filled PDF."""
    if not HAS_PYPDF:
        return None
    try:
        reader = PdfReader(template_path)
        writer = PdfWriter(clone_from=reader)
    except Exception as e:
        log.error("template load failed: %s: %s", template_path, e)
        return None

    # Text fields per page
    for page_idx in range(len(writer.pages)):
        try:
            writer.update_page_form_field_values(
                writer.pages[page_idx], text_updates
            )
        except Exception as e:
            log.debug("text update page %d: %s", page_idx, e)

    # Checkboxes
    _apply_checkbox_updates(writer, checkbox_updates)

    # Signature overlays
    if signature_targets:
        _overlay_signature_on_widgets(writer, signature_targets)

    _set_need_appearances(writer)

    out = io.BytesIO()
    try:
        writer.write(out)
    except Exception as e:
        log.error("template write failed: %s: %s", template_path, e)
        return None
    out.seek(0)
    return out


# ── Per-attachment fillers ───────────────────────────────────────────────

def _sol_number(parsed: Dict[str, Any]) -> str:
    return (parsed or {}).get("header", {}).get("solicitation_number", "") or ""


def fill_bidder_declaration(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
) -> Optional[io.BytesIO]:
    """Fill the GSPD-05-105 Bidder Declaration. Reytech is a prime
    supplier with no subcontractors by default — the 3 subcontractor
    blocks remain blank. The top section affirms SB/DVBE status.
    """
    path = _template_path("bidder_declaration_blank.pdf")
    if not path:
        return None
    compliance = reytech_info.get("compliance", {}) or {}
    sol = _sol_number(parsed)
    cert_type = reytech_info.get("cert_type", "SB/DVBE")
    goods = reytech_info.get("description_of_goods", "Medical/Office and other supplies")

    text_updates = {
        "Solicitaion #": sol,  # [sic] template typo
        "Product list": goods,
        "Text1": cert_type,
        "page": "1",
        "of #": "1",
        # Subcontractor rows 1-3 all blank (no subcontractors)
        "Name, phone, fax": "",
        "Address, email": "",
        "Certification": "",
        "Work or goods": "",
        "%": "",
        "Name, phone, fax 2": "",
        "Address, email 2": "",
        "Certification 2": "",
        "Work or goods 2": "",
        "% 2": "",
        "Name, phone, fax 3": "",
        "Address, email 3": "",
        "Certification 3": "",
        "Work or goods 3": "",
        "% 3": "",
    }
    # Check Box 3, 5, 8 ticked per Reytech ground-truth reference packs:
    #   Check Box3 = "I am a CA certified SB/MB/DVBE bidder"
    #   Check Box5 = "I will perform the contract with my own staff"
    #   Check Box8 = "No subcontractors"
    checkbox_updates = {
        "Check Box3": compliance.get("claiming_sb_preference", True),
        "Check Box5": True,
        "Check Box8": not compliance.get("uses_subcontractors", False),
    }
    return _fill_and_serialize(path, text_updates, checkbox_updates)


def fill_dvbe_843(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
) -> Optional[io.BytesIO]:
    """Fill the DVBE Declarations STD 843. Reytech as prime DVBE.
    References reytech_info for owner/cert/address."""
    path = _template_path("dvbe_843_blank.pdf")
    if not path:
        return None
    sol = _sol_number(parsed)
    firm = reytech_info.get("company_name", "Reytech Inc.")
    cert = reytech_info.get("cert_number", "2002605")
    owner = reytech_info.get("representative", "Michael Guadan")
    phone = reytech_info.get("phone", "")
    address = reytech_info.get("address", "")
    goods = reytech_info.get("description_of_goods", "Medical/Office supplies")
    today = _today_mmddyyyy()

    text_updates = {
        "DVBEname": firm,
        "DVBErefno": cert,
        "description": goods,
        "SCno": sol or "RFQ",
        "DVBEowner1": owner,
        "DVBEowner1date": today,
        "DVBEowner2": "N/A",
        "DVBEowner2date": "",
        "Principal": "N/A",
        "PrincipalPhone": phone,
        "PrincipalAddress": address,
        "DVBEowner3": "",
        "DVBEowner3Date": "",
        "DVBEowner3Address": "",
        "DVBEowner3Phone": "",
        "DVBEowner3TaxID": "",
        "DVBEmgr": "N/A",
        "DVBEmanagerDate": "",
        "PageNo": "1",
        "TotalPages": "1",
        "SCPRS Reference Number": sol or "",
    }
    # YNagent is a 2-widget radio — ticking "Yes" = owner of record
    checkbox_updates = {
        "YNagent": True,
        "OwnBusiness": True,
        "OwnEquipment": True,
    }
    return _fill_and_serialize(
        path,
        text_updates,
        checkbox_updates,
        signature_targets=("DVBEowner1signature",),
    )


def fill_calrecycle_74(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
) -> Optional[io.BytesIO]:
    """Fill CalRecycle 74 Postconsumer Recycled-Content Certification.
    Mirrors each packet line item into its own row (Row1..Row6) with
    0% postconsumer + N/A SABRC code. Template has 6 rows max."""
    path = _template_path("calrecycle_74_blank.pdf")
    if not path:
        return None
    sol = _sol_number(parsed)
    firm = reytech_info.get("company_name", "Reytech Inc.")
    address = reytech_info.get("address", "")
    phone = reytech_info.get("phone", "")
    owner = reytech_info.get("representative", "Michael Guadan")
    title = reytech_info.get("title", "Owner")
    compliance = reytech_info.get("compliance", {}) or {}
    pct = compliance.get("postconsumer_recycled_percent", "0%")
    sabrc = compliance.get("sabrc_product_category", "N/A")
    today = _today_mmddyyyy()

    text_updates: Dict[str, str] = {
        "ContractorCompany Name": firm,
        "Address": address,
        "Phone_2": phone,
        "Print Name": owner,
        "Title": title,
        "Date": today,
    }

    # Loop line items into rows 1..6 (template capacity)
    line_items = (parsed or {}).get("line_items", []) or []
    for i, item in enumerate(line_items[:6], 1):
        desc = (item.get("description") or "").strip()[:120]
        text_updates[f"Purchase Order  RFQ  RFP  IFB  Cal Card Order Row{i}"] = sol or "RFQ"
        text_updates[f"Item Row{i}"] = str(item.get("row_index") or i)
        text_updates[f"Product or Services DescriptionRow{i}"] = desc
        text_updates[f"1Percent Postconsumer Recycled Content MaterialRow{i}"] = pct
        text_updates[f"2SABRC Product Category CodeRow{i}"] = sabrc

    # No SABRC-compliant checkboxes ticked (0% postconsumer = not compliant)
    checkbox_updates: Dict[str, Any] = {}

    return _fill_and_serialize(
        path,
        text_updates,
        checkbox_updates,
        signature_targets=("Signature",),  # template signature is a text widget
    )


def fill_std204(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
) -> Optional[io.BytesIO]:
    """Fill STD 204 Payee Data Record. Reytech is a CA-resident
    corporation. Legal name matches federal tax return per Reytech
    canonical identity (Michael Guadan — no R. prefix)."""
    path = _template_path("std204_blank.pdf")
    if not path:
        return None
    firm = reytech_info.get("company_name", "Reytech Inc.")
    owner = reytech_info.get("representative", "Michael Guadan")
    title = reytech_info.get("title", "Owner")
    street = reytech_info.get("street", "")
    city = reytech_info.get("city", "")
    state = reytech_info.get("state", "CA")
    zip_code = reytech_info.get("zip", "")
    city_state_zip = f"{city} {state} {zip_code}".strip()
    email = reytech_info.get("email", "")
    fein = reytech_info.get("fein", "")
    phone = reytech_info.get("phone", "")
    compliance = reytech_info.get("compliance", {}) or {}
    unit = compliance.get("unit_section", "Procurement")
    today = _today_mmddyyyy()

    text_updates = {
        "NAME (This is required. Do not leave this line blank. Must match the payee\u2019s federal tax return)": owner,
        "BUSINESS NAME, DBA NAME or DISREGARDED SINGLE MEMBER LLC NAME (If different from above)": firm,
        "MAILING ADDRESS (number, street, apt. or suite no.) (See instructions on Page 2)": street,
        "CITY STATE ZIP CODE": city_state_zip,
        "EMAIL ADDRESS": email,
        "Federal Employer Identification Number (FEIN)": fein,
        "NAME OF AUTHORIZED PAYEE REPRESENTATIVE": owner,
        "TITLE": title,
        "EMAIL ADDRESS_2": email,
        "DATE": today,
        "TELEPHONE include area code": phone,
        "UNITSECTION": unit,
    }
    # corpOthers = "Corporation: Other" (Reytech Inc. is a C-corp, not
    # medical/legal/exempt). calRes = CA Resident Yes.
    checkbox_updates = {
        "corpOthers": True,
        "calRes": True,
    }
    return _fill_and_serialize(
        path,
        text_updates,
        checkbox_updates,
        signature_targets=("Signature4",),
    )


def fill_ca_civil_rights(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
) -> Optional[io.BytesIO]:
    """Fill the CA Civil Rights Laws Attachment. Template fields from
    the state-issued PDF (ProposerBidder Firm Name Printed variant)."""
    path = _template_path("ca_civil_rights_attachment_blank.pdf")
    if not path:
        return None
    firm = reytech_info.get("company_name", "Reytech Inc.")
    fein = reytech_info.get("fein", "")
    owner = reytech_info.get("representative", "Michael Guadan")
    title = reytech_info.get("title", "Owner")
    county = reytech_info.get("county", "Orange")
    state = reytech_info.get("state", "CA") or "CA"
    if state == "CA":
        state = "California"
    today = _today_mmddyyyy()

    text_updates = {
        "ProposerBidder Firm Name Printed": firm,
        "Federal ID Number": fein,
        "Printed Name and Title of Person Signing": f"{owner}, {title}",
        "Executed in the County of": county,
        "Executed in the State of": state,
        "mm/dd/yyyy": today,
    }
    return _fill_and_serialize(
        path,
        text_updates,
        {},
        signature_targets=("Signature",),
    )


def fill_darfur_act(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
) -> Optional[io.BytesIO]:
    """Fill DARFUR Contracting Act Certification. Reytech is not a
    scrutinized company and does not do business in Sudan (per
    reytech_config.json compliance flags), so the non-scrutinized
    declaration on page 1 is all we sign."""
    path = _template_path("darfur_act_blank.pdf")
    if not path:
        return None
    firm = reytech_info.get("company_name", "Reytech Inc.")
    fein = reytech_info.get("fein", "")
    owner = reytech_info.get("representative", "Michael Guadan")
    title = reytech_info.get("title", "Owner")
    today = _today_mmddyyyy()

    text_updates = {
        "CompanyVendor Name": firm,
        "Federal ID Number": fein,
        "Date of signature": today,
        "Printed Name and Title of Person Signing": f"{owner}, {title}",
        # Page 2 is only for "scrutinized but authorized" companies —
        # Reytech leaves it blank.
        "CompanyVendor Name Printed_2": "",
        "Federal ID Number_2": "",
        "Date of signature 2": "",
        "Printed Name and Title of Person Initialing": "",
    }
    return _fill_and_serialize(
        path,
        text_updates,
        {},
        signature_targets=("Authorized Signature",),
    )


def splice_static(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
    *,
    template_filename: str = "sellers_permit_reytech.pdf",
) -> Optional[io.BytesIO]:
    """Pass-through for pre-filled static attachments (e.g. the
    sellers permit PDF is already a completed Reytech document).
    Just loads the template and returns it unchanged."""
    path = _template_path(template_filename)
    if not path:
        return None
    try:
        with open(path, "rb") as f:
            data = f.read()
        return io.BytesIO(data)
    except Exception as e:
        log.error("static splice %s: %s", template_filename, e)
        return None


# ── Dispatcher ───────────────────────────────────────────────────────────

FILLERS = {
    "fill_bidder_declaration": fill_bidder_declaration,
    "fill_dvbe_843": fill_dvbe_843,
    "fill_calrecycle_74": fill_calrecycle_74,
    "fill_std204": fill_std204,
    "fill_ca_civil_rights": fill_ca_civil_rights,
    "fill_darfur_act": fill_darfur_act,
    "splice_static": splice_static,
}


def run_filler(
    filler_name: str,
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
) -> Optional[io.BytesIO]:
    fn = FILLERS.get(filler_name)
    if fn is None:
        log.warning("no filler registered for %r", filler_name)
        return None
    try:
        return fn(reytech_info, parsed)
    except Exception as e:
        log.error("filler %s crashed: %s", filler_name, e, exc_info=True)
        return None


__all__ = [
    "FILLERS",
    "run_filler",
    "fill_bidder_declaration",
    "fill_dvbe_843",
    "fill_calrecycle_74",
    "fill_std204",
    "fill_ca_civil_rights",
    "fill_darfur_act",
    "splice_static",
]
