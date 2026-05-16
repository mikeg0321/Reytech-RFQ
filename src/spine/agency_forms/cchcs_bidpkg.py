"""CCHCS Bid Package — multi-form identity bundle.

Bundle PDF (14 pages, 271 AcroForm fields) containing:
  - CUF (California Unified Certification): DBA, cert #, sol#, date
  - Darfur Act Certification: company name × 2, FEIN × 2, owner × 2
  - Bidder Declaration GSPD-05-105: sol#, SB/DVBE declaration, checkboxes
  - DVBE Declaration PD-843: company, cert#, sol#, 4 signature blocks
  - STD 21 Drug-Free Workplace: company, FEIN, phone, owner, address
  - (CalRecycle 74 + OBS 1600 line-item rows — left blank in v1; line-item
    population is a follow-up that will mirror the 704B per-row pattern.)

Architectural pattern matches cchcs_703b.py and cchcs_704b.py:
  1. Pure fill function: Quote + ReytechIdentity + today → bytes.
  2. pypdf writes /V values; pikepdf generates appearance streams +
     (default) flatten_annotations(mode="all").
  3. Matching gate: re-extract + verify identity + sol# + cert # are
     visible (flat path) OR set on /V (fillable path).
  4. Default flat (government convention); ?fillable=1 escape hatch.

The Spine substrate stores zero new fields; identity is config-driven
via ReytechIdentity (env vars REYTECH_*). The Charter rule that
identity NEVER lives in spine_quotes still holds.
"""
from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from src.spine.agency_forms.cchcs_703b import ReytechIdentity, SpineFormFillError

if TYPE_CHECKING:
    from src.spine.model import Quote


_THIS_DIR = Path(__file__).resolve().parent
_BLANK_TEMPLATE = _THIS_DIR / "templates" / "cchcs_bidpkg_blank.pdf"


# ──────────────────────────────────────────────────────────────────────
# Field map — translates (Quote + identity + today) → {field_name: value}
# ──────────────────────────────────────────────────────────────────────


def _field_map(
    quote: "Quote",
    identity: ReytechIdentity,
    today: datetime,
) -> dict[str, str]:
    """Build the AcroForm value dict for the bid package's identity
    blocks. CalRecycle 74 and OBS 1600 line-item rows are intentionally
    left out in v1 — they're follow-up work and many CCHCS quotes
    don't require recycled-content disclosure."""
    sign_date = today.strftime("%m/%d/%Y")
    sol = quote.solicitation_number
    owner_title = f"{identity.contact_person}, {identity.title}"

    # Pull a description-of-goods one-liner from the first line item.
    # The DVBE 843 wants a "description of goods or services" — the
    # parent uses a static "Medical/Office supplies" string; the Spine
    # pulls from the actual quote so the substrate stays the source.
    if quote.line_items:
        desc_first = quote.line_items[0].description[:60]
    else:
        desc_first = "Medical/Office supplies"

    values: dict[str, str] = {
        # ── CUF (California Unified Certification) ──────────────────
        "DOING BUSINESS AS DBA NAME_CUF": identity.business_name,
        "OSDS REF  CURRENTLY CERTIFIED FIRMS ONLY_CUF": identity.cert_number,
        "Date_CUF": sign_date,
        "Text7_CUF": sol,
        # CUF question checkboxes — Reytech's standard "yes" answers.
        "Check_CUF1": "/Yes",
        "Check_CUF3": "/Yes",
        "Check_CUF5": "/Yes",
        "Check_CUF7": "/Yes",
        "Check_CUF9": "/Yes",
        "Check_CUF11": "/Yes",

        # ── Darfur Act Certification (Option 1 — not scrutinized) ──
        "CompanyVendor Name Printed_darfur": identity.business_name,
        "Federal ID Number_darfur": identity.fein,
        "Printed Name and Title of Person Signing_darfur": owner_title,
        "Date__darfur": sign_date,

        # ── Bidder Declaration GSPD-05-105 ──────────────────────────
        "Text0_105": sol,
        "Text1_105": "SB/DVBE",
        # Check3 = SB; Check5 = DVBE; Check8 = certified self-perform.
        "Check3_105": "/Yes",
        "Check5_105": "/Yes",
        "Check8_105": "/Yes",
        # N/A in the "subcontractor" cells — Reytech bids self-perform.
        "Text2_105": "N/A",
        "Text4_105": "N/A",
        "Page1_105": "1",
        "Page2_105": "1",

        # ── DVBE Declaration PD-843 (1st block — primary bidder) ────
        "Text1_PD843": identity.business_name,
        "Text2_PD843": identity.cert_number,
        "Text3_PD843": desc_first,
        "Text4_PD843": sol,
        "Check1_PD843": "/Yes",
        "Text6_PD843": identity.business_name,
        "Date1_PD843": sign_date,
        "Text11_PD843": "N/A",

        # ── STD 21 Drug-Free Workplace ──────────────────────────────
        "Text1_std21": identity.business_name,
        "Text2_std21": identity.fein,
        "Text3_std21": sign_date,
        "Text4_std21": identity.contact_person,
        "Text5_std21": _strip_phone_area(identity.phone),
        "Text6_std21": _phone_area(identity.phone),
        "Text7_std21": identity.title,
        "Text8_std21": identity.address,
        # Drug-free certificate expiration — operator-config provided.
        "Text9_std21": (
            getattr(identity, "drug_free_expiration", "") or "07/01/2028"
        ),
    }

    return values


def _phone_area(phone: str) -> str:
    """Best-effort area-code extraction. STD 21 has separate area-code
    + local-number fields — the parent splits them. We follow."""
    digits = "".join(c for c in (phone or "") if c.isdigit())
    if len(digits) >= 10:
        return digits[-10:-7]
    return ""


def _strip_phone_area(phone: str) -> str:
    """STD 21 's "phone" field is the 7-digit local number."""
    digits = "".join(c for c in (phone or "") if c.isdigit())
    if len(digits) >= 10:
        return f"{digits[-7:-4]}-{digits[-4:]}"
    if len(digits) == 7:
        return f"{digits[:3]}-{digits[3:]}"
    return phone or ""


# ──────────────────────────────────────────────────────────────────────
# Filler
# ──────────────────────────────────────────────────────────────────────


def fill_bidpkg_pdf(
    quote: "Quote",
    identity: ReytechIdentity | None = None,
    *,
    today: datetime | None = None,
    flatten: bool = True,
) -> bytes:
    """Fill the CCHCS bid package and return bytes.

    Same architectural pipeline as 703B / 704B:
    pypdf for AcroForm /V writes + pikepdf for appearance generation
    and (default) flatten_annotations(mode='all'). Matching gate runs
    at the end and raises SpineFormFillError on any divergence.

    Raises SpineFormFillError if:
      - Any required identifier doesn't appear in the rendered output
        (flat mode) OR isn't set on the /V (fillable mode).
    """
    import pypdf
    import pikepdf

    if identity is None:
        identity = ReytechIdentity.from_env()
    today = today or datetime.now()

    if not _BLANK_TEMPLATE.exists():
        raise FileNotFoundError(
            f"Bid package blank template not found at {_BLANK_TEMPLATE}. "
            "Re-copy from parent repo data/templates/cdcr_bid_package_template.pdf."
        )

    field_values = _field_map(quote, identity, today)

    reader = pypdf.PdfReader(str(_BLANK_TEMPLATE))
    writer = pypdf.PdfWriter(clone_from=reader)
    for page in writer.pages:
        writer.update_page_form_field_values(
            page, field_values, auto_regenerate=True,
        )

    intermediate = io.BytesIO()
    writer.write(intermediate)

    with pikepdf.open(io.BytesIO(intermediate.getvalue())) as pdf:
        pdf.generate_appearance_streams()
        if flatten:
            pdf.flatten_annotations(mode="all")
        out = io.BytesIO()
        pdf.save(out)
        pdf_bytes = out.getvalue()

    _verify_bidpkg_matches_model(
        pdf_bytes, quote, identity, field_values, flatten=flatten,
    )
    return pdf_bytes


# ──────────────────────────────────────────────────────────────────────
# Matching gate
# ──────────────────────────────────────────────────────────────────────


# Required identifiers — every bid package must show these. Operator
# AND auditor depend on them. Mapped here as (field_name, label) so
# the gate's error message can be specific about which one is missing.
_REQUIRED_IDENTIFIERS = (
    ("DOING BUSINESS AS DBA NAME_CUF", "Business Name (CUF)"),
    ("OSDS REF  CURRENTLY CERTIFIED FIRMS ONLY_CUF", "Cert Number (CUF)"),
    ("Text7_CUF", "Solicitation Number (CUF)"),
    ("CompanyVendor Name Printed_darfur", "Business Name (Darfur)"),
    ("Federal ID Number_darfur", "FEIN (Darfur)"),
    ("Text0_105", "Solicitation Number (Bidder Decl)"),
    ("Text1_PD843", "Business Name (DVBE 843)"),
    ("Text2_PD843", "Cert Number (DVBE 843)"),
    ("Text4_PD843", "Solicitation Number (DVBE 843)"),
    ("Text1_std21", "Business Name (STD 21)"),
    ("Text2_std21", "FEIN (STD 21)"),
)


def _verify_bidpkg_matches_model(
    pdf_bytes: bytes,
    quote: "Quote",
    identity: ReytechIdentity,
    field_values: dict[str, str],
    *,
    flatten: bool,
) -> None:
    """Re-extract and assert every required identifier is present.
    Same two-path pattern as cchcs_703b/704b gates."""
    import pypdf

    if flatten:
        try:
            import pdfplumber
        except ImportError as e:
            raise SpineFormFillError(
                "pdfplumber required to verify flat bid package output."
            ) from e

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        flattened_text = "".join(full_text.split())

        for field_name, label in _REQUIRED_IDENTIFIERS:
            expected = field_values.get(field_name, "").strip()
            if not expected:
                continue
            target = expected.replace(" ", "")
            if target in flattened_text:
                continue
            # Loose match for long header values that may wrap.
            if len(target) >= 8 and target[:8] in flattened_text:
                continue
            raise SpineFormFillError(
                f"bid package fill gate (flat): {label!r} expected to "
                f"contain {expected!r} but not found in rendered PDF text. "
                "AcroForm fill failed silently or pikepdf dropped the field."
            )
        return

    # Fillable path: check pypdf /V values directly.
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    fields = reader.get_fields() or {}
    for field_name, label in _REQUIRED_IDENTIFIERS:
        expected = field_values.get(field_name, "").strip()
        if not expected:
            continue
        f = fields.get(field_name)
        actual = f.get("/V") if f else None
        if actual is None or str(actual).strip() != expected:
            raise SpineFormFillError(
                f"bid package fill gate (fillable): {label!r} field "
                f"{field_name!r} expected /V {expected!r} but got {actual!r}."
            )
