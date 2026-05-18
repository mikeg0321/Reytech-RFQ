"""CCHCS 704B — RFQ line-item bid response form.

The 704B is the bidder's line-item response: per-row ITEM NUMBER /
DESCRIPTION / UNSPSC / QTY / UOM / QTY PER UOM / PRICE PER UNIT /
SUBTOTAL across 23 rows on page 1 + 16 rows on page 2 (39-row
capacity). Identity fields (COMPANY NAME, DEPARTMENT, SOLICITATION,
etc.) live in the page-1 header.

Same architectural shape as cchcs_703b.py:
  1. Pure fill function: Quote + ReytechIdentity + today → bytes.
  2. pypdf writes /V values; pikepdf generates appearance streams +
     optionally flatten_annotations(mode="all").
  3. Matching gate: re-extract + verify every required line's
     description + extension is present (or, in fillable mode,
     every /V is set).
  4. Default flat (government convention); ?fillable=1 escape hatch.

Overflow handling: Quote with >39 line items raises
`SpineFormFillError(reason="too_many_lines")`. Overflow-to-extra-pages
is a follow-up PR (parent's `_append_overflow_pages` reportlab
canvas pattern). Today: refuse early, surface the limit, no silent
truncation.
"""
from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from src.spine.agency_forms.cchcs_703b import ReytechIdentity, SpineFormFillError

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


_THIS_DIR = Path(__file__).resolve().parent
_BLANK_TEMPLATE = _THIS_DIR / "templates" / "704b_blank.pdf"


# ──────────────────────────────────────────────────────────────────────
# Row capacity — discovered from the 704b_blank.pdf 2026-05-16 inspection
# ──────────────────────────────────────────────────────────────────────

# Page 1 has rows 1..24 EXCEPT row 16 (template oddity, verified by
# field-name enumeration). Page 2 has Row1_2..Row16_2.
_PAGE1_ROW_NUMBERS = tuple(n for n in range(1, 25) if n != 16)   # 23 rows
_PAGE2_ROW_NUMBERS = tuple(range(1, 17))                           # 16 rows
_MAX_LINE_ITEMS = len(_PAGE1_ROW_NUMBERS) + len(_PAGE2_ROW_NUMBERS)  # 39


def _row_assignment(line_no: int) -> tuple[str, int]:
    """Map a 1-based logical line_no to (page_suffix, template_row_no).

    page_suffix is "" for page 1 fields, "_2" for page 2 fields.
    """
    if line_no < 1 or line_no > _MAX_LINE_ITEMS:
        raise SpineFormFillError(
            f"line_no {line_no} out of range; 704B template capacity is "
            f"{_MAX_LINE_ITEMS}. Overflow rendering not yet implemented."
        )
    if line_no <= len(_PAGE1_ROW_NUMBERS):
        return ("", _PAGE1_ROW_NUMBERS[line_no - 1])
    p2_idx = line_no - len(_PAGE1_ROW_NUMBERS) - 1
    return ("_2", _PAGE2_ROW_NUMBERS[p2_idx])


# Column field-name prefixes — discovered from the template.
_ITEM_NUMBER_PREFIX = "ITEM NUMBER"
_DESCRIPTION_PREFIX = "ITEM DESCRIPTION PRODUCT SPECIFICATION"
_UNSPSC_PREFIX = "UNSPSC"
_QTY_PREFIX = "QTY"
_UOM_PREFIX = "UOM"
_QTY_PER_UOM_PREFIX = "QTY PER UOM"
_PRICE_PER_UNIT_PREFIX = "PRICE PER UNIT"
_SUBTOTAL_PREFIX = "SUBTOTAL"


def _dollars(cents: int) -> str:
    """Format integer cents as US dollar string (no leading $)."""
    sign = "-" if cents < 0 else ""
    cents = abs(int(cents))
    whole, frac = divmod(cents, 100)
    return f"{sign}{whole:,}.{frac:02d}"


# ──────────────────────────────────────────────────────────────────────
# Field map — pure function: quote + identity → {pdf_field: value}
# ──────────────────────────────────────────────────────────────────────


def _field_map(
    quote: "Quote",
    identity: ReytechIdentity,
    today: datetime,
    contract: "EmailContract | None" = None,
) -> dict[str, str]:
    """Build the AcroForm value dict from the Quote + identity.

    The REQUESTOR / DEPARTMENT / PHONEEMAIL block on page 1 is the
    STATE OFFICIAL contacting Reytech (the buyer side), not Reytech
    itself. When a contract is provided, those fields come from
    contract.buyer_* fields. Without a contract, they fall back to
    Reytech identity (legacy path; preserves the pre-#1053 shape so
    tests + fixtures keep passing).
    """
    if len(quote.line_items) > _MAX_LINE_ITEMS:
        raise SpineFormFillError(
            f"704B fill: quote has {len(quote.line_items)} line items, "
            f"template capacity is {_MAX_LINE_ITEMS}. Overflow "
            "rendering is a follow-up PR; this filler refuses early "
            "rather than silently truncating."
        )

    # State-official block: who at CCHCS requested this bid?
    if contract is not None:
        requestor_name = contract.buyer_name or ""
        requestor_phone = contract.buyer_phone or ""
        requestor_email = contract.buyer_email or ""
    else:
        # Legacy fallback — no contract bound. Render Reytech as
        # requestor (matches pre-#1053 behavior). Operator must
        # hand-correct on the rare contract-less render.
        requestor_name = identity.contact_person
        requestor_phone = identity.phone
        requestor_email = identity.email

    phone_email_parts = [p for p in (requestor_phone, requestor_email) if p]
    phone_email = " / ".join(phone_email_parts)

    values: dict[str, str] = {
        # Page-1 header / identity block.
        "COMPANY NAME": identity.business_name,
        "DEPARTMENT": quote.agency,
        "SOLICITATION": quote.solicitation_number,
        # State official (buyer-side) — comes from contract.buyer_*.
        "REQUESTOR": requestor_name,
        "PHONEEMAIL": phone_email,
        # PERSON PROVIDING QUOTE is the Reytech-side rep (the bidder).
        "PERSON PROVIDING QUOTE": identity.contact_person,
        "Date1_af_date": today.strftime("%m/%d/%Y"),
    }

    merchandise_subtotal_cents = 0
    for li in quote.line_items:
        suffix, row_no = _row_assignment(li.line_no)
        def k(prefix: str) -> str:
            return f"{prefix}Row{row_no}{suffix}"

        # ITEM NUMBER column = buyer's MFG / catalog #, not the row#.
        # Mirrors PR #1045 (parser fix); 704B writer was missed in
        # that PR. Caught 5/18 on R26Q40 vision-walk — 704B showed
        # "1" / "2" instead of "503-0142-01" / "008-0869-00", which
        # would fail CCHCS responsiveness because the agency cross-
        # checks ITEM NUMBER against the manufacturer's catalog.
        # Fallback to row# only when mfg_number is blank.
        values[k(_ITEM_NUMBER_PREFIX)] = li.mfg_number or str(li.line_no)
        values[k(_DESCRIPTION_PREFIX)] = li.description
        # UNSPSC isn't tracked in the Spine model yet — leave blank.
        values[k(_UNSPSC_PREFIX)] = ""
        values[k(_QTY_PREFIX)] = str(li.qty)
        values[k(_UOM_PREFIX)] = li.uom
        values[k(_QTY_PER_UOM_PREFIX)] = "1"
        values[k(_PRICE_PER_UNIT_PREFIX)] = _dollars(li.unit_price_cents)
        values[k(_SUBTOTAL_PREFIX)] = _dollars(li.extension_cents)
        merchandise_subtotal_cents += li.extension_cents

    # MERCHANDISE SUBTOTAL — bottom-of-page grand-total field on the
    # 704B template (field name `fill_154`). Required by CCHCS for the
    # bid responsiveness check; left blank pre-#1053 → reviewer cannot
    # verify line-sum without hand math, blocks award.
    values["fill_154"] = _dollars(merchandise_subtotal_cents)

    return values


# ──────────────────────────────────────────────────────────────────────
# Filler
# ──────────────────────────────────────────────────────────────────────


def fill_704b_pdf(
    quote: "Quote",
    identity: ReytechIdentity | None = None,
    *,
    today: datetime | None = None,
    flatten: bool = True,
    contract: "EmailContract | None" = None,
) -> bytes:
    """Fill the CCHCS 704B line-item form and return bytes.

    Same architectural pipeline as fill_703b_pdf:
    pypdf for field /V writes + pikepdf for appearance generation
    and (default) flatten_annotations(mode='all'). Matching gate
    runs at the end and raises SpineFormFillError on any divergence.

    Raises SpineFormFillError if:
      - Line item count > _MAX_LINE_ITEMS (no silent truncation).
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
            f"704B blank template not found at {_BLANK_TEMPLATE}. "
            "Re-copy from parent repo data/templates/704b_blank.pdf."
        )

    field_values = _field_map(quote, identity, today, contract=contract)

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

    _verify_704b_matches_model(
        pdf_bytes, quote, identity, field_values, flatten=flatten,
    )
    return pdf_bytes


# ──────────────────────────────────────────────────────────────────────
# Matching gate
# ──────────────────────────────────────────────────────────────────────


# Required identifiers on every 704B. Operator must see these.
_REQUIRED_HEADER_FIELDS = (
    "COMPANY NAME",
    "DEPARTMENT",
    "SOLICITATION",
    "PHONEEMAIL",
)


def _verify_704b_matches_model(
    pdf_bytes: bytes,
    quote: "Quote",
    identity: ReytechIdentity,
    field_values: dict[str, str],
    *,
    flatten: bool,
) -> None:
    """Re-extract and assert every line item + header identifier is
    present. Same two-path pattern as cchcs_703b's gate."""
    import pypdf

    if flatten:
        try:
            import pdfplumber
        except ImportError as e:
            raise SpineFormFillError(
                "pdfplumber required to verify flat 704B output."
            ) from e

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        flattened_text = "".join(full_text.split())

        # 1. Header identifiers.
        for field_name in _REQUIRED_HEADER_FIELDS:
            expected = field_values.get(field_name, "").strip()
            if not expected:
                continue
            target = expected.replace(" ", "")
            if target in flattened_text:
                continue
            # Loose match for long header values.
            if len(target) >= 8 and target[:8] in flattened_text:
                continue
            raise SpineFormFillError(
                f"704B fill gate (flat): header field {field_name!r} "
                f"expected to contain {expected!r} but not found in "
                f"rendered PDF text."
            )

        # 2. Per-line: every description AND every subtotal money
        # string must appear at least once. The Counter-based check
        # used by quote_pdf is overkill here (the 704B has one
        # subtotal per line, distinct values likely); substring
        # check per line is enough for the gate.
        for li in quote.line_items:
            desc_target = "".join(li.description[:12].split())
            if len(desc_target) >= 6 and desc_target not in flattened_text:
                raise SpineFormFillError(
                    f"704B fill gate (flat): line {li.line_no} description "
                    f"prefix {desc_target!r} not found in rendered PDF text. "
                    "AcroForm fill failed silently or template lacks the row."
                )
            sub_target = _dollars(li.extension_cents).replace(",", "")
            # Comma-stripped form for the substring check (extracted
            # PDF text may collapse or preserve commas inconsistently).
            if sub_target not in flattened_text.replace(",", ""):
                raise SpineFormFillError(
                    f"704B fill gate (flat): line {li.line_no} subtotal "
                    f"{sub_target!r} not found in rendered PDF text."
                )
        return

    # Fillable: check field /V values directly via pypdf.
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    fields = reader.get_fields() or {}
    for field_name in _REQUIRED_HEADER_FIELDS:
        expected = field_values.get(field_name, "").strip()
        if not expected:
            continue
        f = fields.get(field_name)
        actual = f.get("/V") if f else None
        if actual is None or str(actual).strip() != expected:
            raise SpineFormFillError(
                f"704B fill gate (fillable): header field {field_name!r} "
                f"expected /V {expected!r} but got {actual!r}."
            )
    for li in quote.line_items:
        suffix, row_no = _row_assignment(li.line_no)
        desc_field = f"{_DESCRIPTION_PREFIX}Row{row_no}{suffix}"
        f = fields.get(desc_field)
        actual = f.get("/V") if f else None
        if actual is None or str(actual).strip() != li.description.strip():
            raise SpineFormFillError(
                f"704B fill gate (fillable): line {li.line_no} description "
                f"field {desc_field!r} expected /V {li.description!r} but "
                f"got {actual!r}."
            )
