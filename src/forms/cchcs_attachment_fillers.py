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
        NumberObject,
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
    """Walk every page's annotations and set checkbox / radio widgets
    whose full name matches an update key.

    Update values:
      - truthy (True / "/Yes" / 1 / ...) → tick the box (single checkbox).
      - falsy  (False / "/Off" / "" / ...) → untick.
      - a specific export string ("/0", "/1", ...) → RADIO choice: only
        the widget whose own on-state equals the string is ticked, every
        sibling is set /Off. This is the ONLY correct way to drive a
        one-of-N radio (e.g. DVBE 843 Section 2 broker declaration). A
        generic truthy ticks every widget AND lets the parent /V end up
        whichever sibling was iterated last — that is the bug that
        rendered "DVBE is a broker" instead of "is not a broker".

    The radio group's /V is written ONCE per parent, after the widget
    loop, to the chosen value — never flip-flopped per sibling.
    """
    if not updates:
        return 0
    written = 0
    parent_choice: Dict[int, str] = {}
    parent_objs: Dict[int, Any] = {}
    _ON = (True, "/Yes", "Yes", "yes", 1, "1", "/On", "On")
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
                widget_on = _best_on_state(annot)  # this widget's own on-state
                if (isinstance(desired, str) and desired.startswith("/")
                        and desired not in ("/Off", "/On", "/Yes")):
                    # Specific radio export value — tick only the matching
                    # sibling; the group's /V is the chosen value.
                    export_name = desired if widget_on == desired else "/Off"
                    chosen = desired
                elif desired in _ON:
                    export_name = widget_on or "/Yes"
                    chosen = export_name
                else:
                    export_name = "/Off"
                    chosen = "/Off"
                export = NameObject(export_name)
                annot[NameObject("/V")] = export
                annot[NameObject("/AS")] = export
                try:
                    parent = annot.get("/Parent")
                    if parent is not None:
                        pobj = parent.get_object()
                        pkey = id(pobj)
                        parent_objs[pkey] = pobj
                        parent_choice[pkey] = chosen
                except Exception as _e:
                    log.debug('suppressed in _apply_checkbox_updates: %s', _e)
                written += 1
            except Exception:
                continue
    # One resolved /V per parent. For a radio every sibling agreed on
    # `chosen`, so this is deterministic regardless of widget order.
    for pkey, pobj in parent_objs.items():
        try:
            pobj[NameObject("/V")] = NameObject(parent_choice[pkey])
        except Exception as _e:
            log.debug('suppressed parent /V write: %s', _e)
    return written


# Target on-page height for an overlaid signature image, in PDF points.
# Signature widget rects on government forms are often very thin (8-14pt
# tall); fitting an image INTO that height with preserveAspectRatio
# shrinks it to a micro mark. We instead size the signature off the
# field WIDTH and cap the height here, letting it sit on the field's
# baseline and rise above a thin rect — the real-world ink convention.
_SIGNATURE_MAX_HEIGHT = 30.0
_SIGNATURE_PAD = 2.0


def _signature_draw_box(field_rect: tuple, img_aspect: float) -> tuple:
    """Given a signature widget /Rect and the signature image's aspect
    ratio (width / height), return the (x, y, w, h) to draw the image at.

    Drives size off the field WIDTH (not the often-thin field height) so
    the signature renders at a consistent, legible size everywhere —
    closes the "micro-sized signatures throughout the document" defect.
    Single source of truth: the packet-filler signature overlays import
    this so flat/attachment signatures never diverge.
    """
    fl, fb, fr, ft = field_rect
    fw = fr - fl
    aspect = img_aspect if img_aspect and img_aspect > 0 else 3.2
    draw_w = max(fw - _SIGNATURE_PAD * 2, 1.0)
    draw_h = draw_w / aspect
    if draw_h > _SIGNATURE_MAX_HEIGHT:
        draw_h = _SIGNATURE_MAX_HEIGHT
        draw_w = draw_h * aspect
    return fl + _SIGNATURE_PAD, fb + _SIGNATURE_PAD, draw_w, draw_h


def _neutralize_signature_widget(annot: Any) -> None:
    """Fully neutralize a signature widget so nothing it carries renders
    over the drawn signature PNG.

    A widget annotation paints ON TOP of page content, so a "sign here"
    / e-sign tab on the source template's signature field renders OVER
    the signature PNG the overlay merges into the page content stream.
    The CCHCS AMS 708 /Sig field's red "SIGN" tab comes from its /MK
    appearance-characteristics dict (/BG red background + /CA "SIGN"
    caption) — and with /NeedAppearances set a viewer REGENERATES that
    tab from /MK even after /AP is removed. So strip /V, /AP, /AS AND
    /MK, and set the Hidden flag. The merged page-content PNG is then
    the sole visible signature.
    """
    try:
        annot[NameObject("/V")] = TextStringObject("")
    except Exception as _e:
        log.debug('sig widget /V clear: %s', _e)
    for _k in ("/AP", "/AS", "/MK"):
        try:
            if _k in annot:
                del annot[NameObject(_k)]
        except Exception as _e:
            log.debug('sig widget %s strip: %s', _k, _e)
    # Hidden flag (bit 2 → value 2): no regenerated appearance can
    # render. The signature PNG lives in the page content stream and
    # is unaffected.
    try:
        cur = annot.get("/F", 0)
        annot[NameObject("/F")] = NumberObject(int(cur) | 2)
    except Exception as _e:
        log.debug('sig widget /F hidden: %s', _e)


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

    img = ImageReader(sig_path)
    try:
        _iw, _ih = img.getSize()
        img_aspect = (_iw / _ih) if _ih else 3.2
    except Exception:
        img_aspect = 3.2

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
                _neutralize_signature_widget(annot)

                page_w, page_h = 612.0, 792.0
                try:
                    mb = page.mediabox
                    page_w, page_h = float(mb.width), float(mb.height)
                except Exception as _e:
                    log.debug('suppressed in _overlay_signature_on_widgets: %s', _e)
                x, y, w, h = _signature_draw_box(sig_rect, img_aspect)
                buf = io.BytesIO()
                c = rl_canvas.Canvas(buf, pagesize=(page_w, page_h))
                c.drawImage(
                    img, x, y, width=w, height=h,
                    mask="auto", preserveAspectRatio=True, anchor="sw",
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

    # Ensure /Helv is resolvable in every form-field page's /Resources/Font.
    # Without this, Chrome PDFium clips long values when re-rendering /AP.
    # See feedback_acroform_helv_resource memory + PR #510.
    try:
        from src.forms.reytech_filler_v4 import _ensure_helv_font_on_pages as _eh
        _eh(writer)
    except Exception as e:
        log.debug("ensure_helv_font_on_pages suppressed: %s", e)

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
        # Section 1: the solicitation # goes in Solicitation/Contract
        # Number. SCPRS Reference Number is "FOR STATE USE ONLY" — the
        # state fills it; Reytech leaves it blank.
        "SCno": sol,
        "SCPRS Reference Number": "",
        "DVBEowner1": owner,
        "DVBEowner1date": today,
        # Reytech has a single 100%-DV owner. The 2nd-owner / manager
        # lines stay BLANK — not "N/A". An empty line reads correctly as
        # "no second owner"; "N/A" is gratuitous filler (Mike, 2026-05-21).
        "DVBEowner2": "",
        "DVBEowner2date": "",
        # The Firm/Principal block applies ONLY when the DVBE is acting
        # as a broker/agent. Reytech declares it is NOT a broker
        # (Section 2) → the entire principal block stays blank.
        "Principal": "",
        "PrincipalPhone": "",
        "PrincipalAddress": "",
        "DVBEowner3": "",
        "DVBEowner3Date": "",
        "DVBEowner3Address": "",
        "DVBEowner3Phone": "",
        "DVBEowner3TaxID": "",
        "DVBEmgr": "",
        "DVBEmanagerDate": "",
        "PageNo": "1",
        "TotalPages": "1",
    }
    # Section 2 — "Check only ONE box." YNagent is a one-of-two radio:
    #   /1 = top box   = "the DVBE is NOT a broker or agent"  ← Reytech
    #   /0 = lower box = "the DVBE IS a broker or agent"
    # (/1 vs /0 → position confirmed from the template widget rects: the
    # /1 widget sits higher on the page.) Section 3 applies only to DVBEs
    # that RENT equipment; Reytech is a supply reseller, so OwnBusiness /
    # OwnEquipment are left UNCHECKED.
    checkbox_updates = {
        "YNagent": "/1",
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

    DELEGATES TO `reytech_filler_v4.fill_calrecycle_standalone` which
    handles >6 line items via reportlab overlay + overflow pages.

    Reframed 2026-05-27 (Coleman sol# 10842771, 21-line RFQ): the prior
    implementation hardcoded `line_items[:6]` because the AcroForm
    template caps at 6 rows. That silently dropped items 7-21 on the
    Coleman package. The standalone path in reytech_filler_v4 already
    paginates correctly (verified by tests/test_calrecycle_multipage_overflow.py).
    This is the bridge that brings packet-path callers onto that
    implementation without rewriting the standalone.
    """
    path = _template_path("calrecycle_74_blank.pdf")
    if not path:
        return None

    sol = _sol_number(parsed)
    firm = reytech_info.get("company_name", "Reytech Inc.")
    address = reytech_info.get("address", "")
    phone = reytech_info.get("phone", "")
    owner = reytech_info.get("representative", "Michael Guadan")
    title = reytech_info.get("title", "Owner")
    line_items = (parsed or {}).get("line_items", []) or []

    # Adapt to fill_calrecycle_standalone's expected shapes.
    rfq_data: Dict[str, Any] = {
        "solicitation_number": sol or "",
        "sign_date": _today_mmddyyyy(),
        "line_items": line_items,
    }
    config: Dict[str, Any] = {
        "company": {
            "name": firm,
            "address": address,
            "phone": phone,
            "owner": owner,
            "title": title,
        }
    }

    # Write to a temp output file, read bytes back into BytesIO.
    import tempfile
    tmp_dir = tempfile.mkdtemp(prefix="calrecycle_pkt_")
    out_path = os.path.join(tmp_dir, "calrecycle_74.pdf")
    try:
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        fill_calrecycle_standalone(path, rfq_data, config, out_path)
        if not os.path.exists(out_path):
            log.warning("CalRecycle delegate produced no output at %s", out_path)
            return None
        with open(out_path, "rb") as fh:
            return io.BytesIO(fh.read())
    except Exception as _e:
        log.error("CalRecycle standalone delegation failed: %s", _e, exc_info=True)
        return None
    finally:
        # Best-effort cleanup of the temp dir.
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
            os.rmdir(tmp_dir)
        except Exception as _ce:
            log.debug("calrecycle temp cleanup: %s", _ce)


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
    # STD 204 Section 3 FEIN is a 9-cell comb field — the "##-#######"
    # form mask provides 9 digit cells, so the dash is NOT a cell.
    # Passing "47-4588061" (10 chars) overflows and the LAST digit is
    # silently dropped ("47-458806"). Strip non-digits → 9 digits, all
    # retained, dash supplied by the pre-printed form mask.
    fein_digits = "".join(c for c in fein if c.isdigit())
    phone = reytech_info.get("phone", "")
    today = _today_mmddyyyy()

    text_updates = {
        "NAME (This is required. Do not leave this line blank. Must match the payee\u2019s federal tax return)": owner,
        "BUSINESS NAME, DBA NAME or DISREGARDED SINGLE MEMBER LLC NAME (If different from above)": firm,
        "MAILING ADDRESS (number, street, apt. or suite no.) (See instructions on Page 2)": street,
        "CITY STATE ZIP CODE": city_state_zip,
        "EMAIL ADDRESS": email,
        "Federal Employer Identification Number (FEIN)": fein_digits,
        "NAME OF AUTHORIZED PAYEE REPRESENTATIVE": owner,
        "TITLE": title,
        "EMAIL ADDRESS_2": email,
        "DATE": today,
        "TELEPHONE include area code": phone,
        # Section 6 ("Paying State Agency", incl. UNIT/SECTION) is FOR
        # STATE USE ONLY — the paying agency completes it. Reytech
        # writes nothing there; the field stays blank.
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


def fill_std_1000(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
) -> Optional[io.BytesIO]:
    """Fill CA STD 1000 GenAI Disclosure Form.

    Reytech does not use GenAI in the products supplied — tick "No"
    on the GenAI-usage question, fill identity fields + sol#,
    skip the GenAI-specific blocks per the form's "If No, skip to
    Signature section" instruction.

    Spine-side wrapper at src/spine/agency_forms/std_1000.py.
    """
    path = _template_path("std1000_blank.pdf")
    if not path:
        return None
    sol = _sol_number(parsed)
    firm = reytech_info.get("company_name", "Reytech Inc.")
    phone = reytech_info.get("phone", "")
    street = reytech_info.get("street", "")
    city = reytech_info.get("city", "")
    state = reytech_info.get("state", "CA")
    zip_code = reytech_info.get("zip", "")
    today = _today_mmddyyyy()

    text_updates = {
        "Business Name": firm,
        "Business Telephone Number": phone,
        "Business Address": street,
        "City": city,
        "State": state,
        "Zip Code": zip_code,
        "Date": today,
        "Solicitation  Contract Number": sol,
        "Contract / Description of Purchase":
            reytech_info.get("description_of_goods",
                             "Medical/Office supplies"),
    }
    # Tick "No" — the form's "If No, skip to Signature" instruction
    # means we leave items 1-6 blank.
    checkbox_updates = {
        "No If no skip to Signature section of this form": True,
    }
    return _fill_and_serialize(
        path,
        text_updates,
        checkbox_updates,
        signature_targets=("Signature",),
    )


def fill_cuf(
    reytech_info: Dict[str, Any],
    parsed: Dict[str, Any],
) -> Optional[io.BytesIO]:
    """Fill CV 012 Commercially Useful Function (CUF) form.

    Reytech is a DVBE-certified supplier of goods; for the 6 CUF
    questions the answer is "Yes" — we perform the function ourselves
    (purchase inventory, manage logistics, deliver). Radio button
    indices `/1` map to "Yes" per the form's NameTree (Adobe XFA
    convention used by CV 012). 0 = unselected, 1 = first option (Yes).

    Spine-side wrapper at src/spine/agency_forms/cuf.py.
    """
    path = _template_path("cv012_cuf_blank.pdf")
    if not path:
        return None
    sol = _sol_number(parsed)
    firm = reytech_info.get("company_name", "Reytech Inc.")
    owner = reytech_info.get("representative", "Michael Guadan")
    title = reytech_info.get("title", "Owner")
    cert = reytech_info.get("cert_number", "2002605")
    cert_exp = reytech_info.get("cert_expiration", "")
    today = _today_mmddyyyy()

    text_updates = {
        "form1[0].#subform[0].SolicitationNumber[0]": sol,
        "form1[0].#subform[0].DoingBusinessAs[0]": firm,
        "form1[0].#subform[0].OSDSRefNumber[0]": cert,
        "form1[0].#subform[0].ExpirationDate[0]": cert_exp,
        "form1[0].#subform[1].AuthorizedRepresentative[0]": owner,
        "form1[0].#subform[1].Title[0]": title,
        "form1[0].#subform[1].PrintedName[0]": owner,
        "form1[0].#subform[1].Date[0]": today,
    }
    # All 6 CUF radios = "Yes" (Reytech performs the work directly).
    # The form's radio NameTree uses "/1" for the Yes option per the
    # XFA convention; the existing _apply_checkbox_updates handles
    # both "true" and "/1" mappings through _best_on_state.
    checkbox_updates = {
        "form1[0].#subform[0].RadioButtonList[0]": True,
        "form1[0].#subform[0].RadioButtonList[1]": True,
        "form1[0].#subform[0].RadioButtonList[2]": True,
        "form1[0].#subform[0].RadioButtonList[3]": True,
        "form1[0].#subform[0].RadioButtonList[4]": True,
        "form1[0].#subform[0].RadioButtonList[5]": True,
    }
    return _fill_and_serialize(
        path,
        text_updates,
        checkbox_updates,
        signature_targets=(),  # CV 012 signature is the PrintedName text
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
