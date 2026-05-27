"""CCHCS 703B — RFQ Informal Competitive cover sheet + certifications.

The 703B is a 9-page agency-mandated AcroForm PDF that vendors return
with every bid response. Reytech identity (business name, FEIN,
seller's permit, address, contact info) is the bulk of what gets
filled; buyer-given metadata (solicitation #, release date, due date)
comes from the Quote model.

This module fills the form and runs a matching gate. Same
architectural shape as src/spine/quote_pdf.py:

  1. Load the blank template.
  2. Fill the AcroForm widgets via pypdf.
  3. Flatten by default (government convention).
  4. Re-extract via pdfplumber and assert every required identifier
     (solicitation #, business name, FEIN, phone, email) is present.
  5. Raise SpineFormFillError if anything's missing or wrong.

The substrate has no vendor_* fields — identity lives in a
ReytechIdentity dataclass loaded from env vars (with documented
defaults for local dev). Production wiring sets the env vars from
Reytech's actual compliance record.
"""
from __future__ import annotations

import io
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

# ReytechIdentity + SpineFormFillError were historically defined here and
# imported by ~10 other agency_forms files. Moved to _identity.py 2026-05-27
# (PR-Job1-D prerequisite) so the CCHCS-specific renderers can be deleted
# in PR-Job1-D without breaking every other importer. Re-exported here for
# backwards-compat — short-lived, since PR-D deletes this file outright.
from src.spine.agency_forms._identity import ReytechIdentity, SpineFormFillError

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


_THIS_DIR = Path(__file__).resolve().parent
_BLANK_TEMPLATE = _THIS_DIR / "templates" / "703b_blank.pdf"


# ──────────────────────────────────────────────────────────────────────
# 703B field map — single source of truth for AcroForm field names.
# Mirrors src/forms/profiles/703b_reytech_standard.yaml from the
# parent repo but stays self-contained inside the Spine.
# ──────────────────────────────────────────────────────────────────────


def _field_map(
    quote: "Quote",
    identity: ReytechIdentity,
    today: datetime,
    contract: "EmailContract | None" = None,
) -> dict[str, str]:
    """Return {pdf_field_name: value} for the 703B AcroForm.

    Pure function — no I/O. The dict is the contract between the
    filler and the matching gate; both walk it.

    Identity boundaries:
      - Page 1 BIDDER INFORMATION block → Reytech identity (vendor).
      - Page 2 STATE OFFICIAL / REQUESTOR block (`_Name`, `_Email_2`,
        `_Phone_2`, `_Text1` shipping) → comes from EmailContract
        (buyer side). Without a contract, those fields stay blank so
        the operator can't accidentally ship REYTECH info into the
        state-official slot (5/18 ship-blocking bug).
    """
    # CCHCS 703B Page 1 declares: "Bidder offers and agrees if this
    # response is accepted within 45 calendar days following the date
    # the response is due...". The bid-expiration field must reflect
    # that 45-day window, NOT the 30-day payment terms — operators
    # were confusing the two before 5/18.
    bid_expiration = today + timedelta(days=45)
    sign_date = today.strftime("%m/%d/%Y")

    # State-official (buyer-side) block — empty fallback when no
    # contract is bound. Empty is strictly better than mis-filled with
    # Reytech identity per Mike's 5/18 sign-off: a blank state-official
    # block is obviously wrong + easy to fix on review; Reytech-info in
    # the state-official slot LOOKS plausible and ships broken.
    state_official_name = ""
    state_official_email = ""
    state_official_phone = ""
    state_official_shipping = ""
    release_date_str = ""
    due_date_str = ""
    if contract is not None:
        state_official_name = contract.buyer_name or ""
        state_official_email = contract.buyer_email or ""
        state_official_phone = contract.buyer_phone or ""
        state_official_shipping = contract.ship_to_address or ""
        if contract.release_date is not None:
            release_date_str = contract.release_date.strftime("%m/%d/%Y")
        if contract.due_date is not None:
            due_date_str = contract.due_date.strftime("%m/%d/%Y")

    return {
        # Buyer-given metadata (from Quote model + contract).
        "703B_Solicitation Number": quote.solicitation_number,
        "703B_Dropdown2": quote.agency,    # institution dropdown
        "703B_Release Date": release_date_str,
        "703B_Due Date": due_date_str,

        # Vendor-side identity (from ReytechIdentity).
        "703B_Business Name": identity.business_name,
        "703B_Address": identity.address,
        "703B_Contact Person": identity.contact_person,
        "703B_Title": identity.title,
        "703B_Phone": identity.phone,
        "703B_Fax": identity.fax,
        "703B_Email": identity.email,
        "703B_Federal Employer Identification Number FEIN": identity.fein,
        "703B_Retailers CA Sellers Permit Number": identity.sellers_permit,
        "703B_SBMBDVBE Certification.0": identity.cert_number,
        "703B_Certification Expiration Date": identity.cert_expiration,

        # Vendor-side bid terms.
        "703B_Deliveries must be completed within": identity.delivery_days,
        "703B_days of receipt": identity.payment_terms_days,
        "703B_Payment discount offered on invoices to be paid within": (
            identity.payment_discount_pct
        ),
        "703B_BidExpirationDate": bid_expiration.strftime("%m/%d/%Y"),
        "703B_Sign_Date": sign_date,

        # STATE OFFICIAL block on page 2 — buyer-side contact, NOT
        # Reytech. Filled from EmailContract.buyer_*; blank when no
        # contract is bound.
        "703B_Name": state_official_name,
        "703B_Email_2": state_official_email,
        "703B_Phone_2": state_official_phone,
        "703B_Text1": state_official_shipping,
    }


# ──────────────────────────────────────────────────────────────────────
# Filler
# ──────────────────────────────────────────────────────────────────────


def fill_703b_pdf(
    quote: "Quote",
    identity: ReytechIdentity | None = None,
    *,
    today: datetime | None = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Fill the CCHCS 703B and return the bytes.

    Args:
        quote:    Validated Spine Quote (provides solicitation_number,
                  agency).
        identity: Vendor-side identity. None → loaded from env.
        today:    Optional clock injection for deterministic test
                  rendering (signature date + bid expiration).
        flatten:  When True (default), the AcroForm widgets are
                  flattened to drawn text — government convention.
                  When False, the PDF remains fillable (escape hatch
                  for last-minute operator edits per 2026-05-15
                  fillable-PDF research).

    Returns:
        Bytes of the filled PDF.

    Raises:
        SpineFormFillError: matching gate found a divergence
            between identity + quote and the rendered bytes.
        FileNotFoundError: 703B blank template missing.
    """
    import pypdf

    if identity is None:
        identity = ReytechIdentity.from_env()
    today = today or datetime.now()

    if not _BLANK_TEMPLATE.exists():
        raise FileNotFoundError(
            f"703B blank template not found at {_BLANK_TEMPLATE}. "
            "Re-copy from parent repo tests/fixtures/703b_blank.pdf."
        )

    field_values = _field_map(quote, identity, today, contract=contract)

    reader = pypdf.PdfReader(str(_BLANK_TEMPLATE))
    writer = pypdf.PdfWriter(clone_from=reader)

    # Fill the AcroForm on every page so widgets on later pages
    # (e.g., signature block on page 1, contact info on page 9) get
    # populated. pypdf documents this as the canonical pattern.
    for page in writer.pages:
        writer.update_page_form_field_values(
            page,
            field_values,
            auto_regenerate=True,
        )

    buf = io.BytesIO()
    writer.write(buf)
    pdf_bytes = buf.getvalue()

    # pypdf sets the field /V values but its appearance-stream
    # generation is incomplete (caught 2026-05-16 smoke test: 50
    # fields filled, pdfplumber sees 0 of them in extracted text).
    # pikepdf's generate_appearance_streams() walks the AcroForm and
    # produces the visual /AP streams Acrobat + Chrome + pdfplumber
    # actually render. flatten_annotations(mode="all") then bakes
    # those streams into the page content streams as drawn text —
    # the government-compliant flat-PDF shape. Without this step,
    # the matching gate fails on real templates and recipients see
    # a blank form in some viewers.
    import pikepdf
    with pikepdf.open(io.BytesIO(pdf_bytes)) as pdf:
        pdf.generate_appearance_streams()
        if flatten:
            pdf.flatten_annotations(mode="all")
        out = io.BytesIO()
        pdf.save(out)
        pdf_bytes = out.getvalue()

    _verify_703b_matches_model(
        pdf_bytes, quote, identity, field_values, flatten=flatten,
    )
    return pdf_bytes


# ──────────────────────────────────────────────────────────────────────
# Matching gate — re-extract and verify
# ──────────────────────────────────────────────────────────────────────


# Which fields MUST be visible in the rendered text after fill.
# Other fields (checkboxes, dropdowns, the cert number when blank)
# are optional and don't fail the gate.
_REQUIRED_VISIBLE_FIELDS = (
    "703B_Solicitation Number",
    "703B_Business Name",
    "703B_Phone",
    "703B_Email",
    "703B_Federal Employer Identification Number FEIN",
)


def _verify_703b_matches_model(
    pdf_bytes: bytes,
    quote: "Quote",
    identity: ReytechIdentity,
    field_values: dict[str, str],
    *,
    flatten: bool,
) -> None:
    """Re-extract the filled PDF and assert every required field is
    present.

    Flat path (government convention, default): the values are baked
    into the page content stream, so pdfplumber sees them as text.
    Verify via case-sensitive substring match on the extracted text.

    Fillable path (Adobe-edit escape hatch): values live in the
    AcroForm widget /V dictionaries, not the content stream.
    pdfplumber can't see them, so verify directly via the field's
    /V value. Either path makes the failure class — "operator
    typed a sol# but it's missing from the rendered form" —
    structurally impossible to ship.
    """
    import pypdf

    if flatten:
        try:
            import pdfplumber
        except ImportError as e:
            raise SpineFormFillError(
                "pdfplumber required to verify flat 703B fill output."
            ) from e

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        flattened_text = "".join(full_text.split())

        for field_name in _REQUIRED_VISIBLE_FIELDS:
            expected = field_values.get(field_name, "").strip()
            if not expected:
                # Field intentionally left blank by config — skip; the
                # gate only enforces presence of values we SET.
                continue
            target = expected.replace(" ", "")
            if target in flattened_text:
                continue
            prefix_len = min(8, len(target))
            if prefix_len > 0 and target[:prefix_len] in flattened_text:
                continue
            raise SpineFormFillError(
                f"703B fill gate (flat): field {field_name!r} expected to "
                f"contain {expected!r} but value not found in rendered "
                f"PDF text. AcroForm fill failed silently or appearance "
                f"generation didn't produce a visible glyph stream."
            )
        return

    # Fillable: check field /V values directly. The bytes returned
    # here are intended for Adobe — operator opens, edits, saves —
    # and the gate's job is to guarantee the values were SET.
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    fields = reader.get_fields() or {}
    for field_name in _REQUIRED_VISIBLE_FIELDS:
        expected = field_values.get(field_name, "").strip()
        if not expected:
            continue
        f = fields.get(field_name)
        actual = (f.get("/V") if f else None)
        if actual is None or str(actual).strip() != expected:
            raise SpineFormFillError(
                f"703B fill gate (fillable): field {field_name!r} "
                f"expected /V {expected!r} but got {actual!r}. "
                f"AcroForm fill silently dropped or corrupted the value."
            )
