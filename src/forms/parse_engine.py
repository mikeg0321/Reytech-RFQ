"""Parse Engine — profile-driven PDF form extraction.

Single entry point: parse(pdf_path) → (Quote, list[ParseWarning]).
Uses the same YAML profiles as the fill engine, so parser and filler
can never disagree on field names.

Usage:
    from src.forms.parse_engine import parse

    quote, warnings = parse("path/to/buyer_704.pdf")
    # quote.header.institution_key = "CSP-Sacramento"
    # quote.line_items[0].description = "Name tags, black/white"
"""
import logging
import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from src.core.quote_model import Quote, QuoteHeader, BuyerInfo, Address, LineItem, Provenance
from src.forms.profile_registry import FormProfile, load_profiles, match_profile

log = logging.getLogger(__name__)


@dataclass
class ParseWarning:
    field: str
    message: str
    severity: str = "warning"  # warning | error


def parse(pdf_path: str, profiles: dict[str, FormProfile] | None = None) -> tuple[Quote, list[ParseWarning]]:
    """Parse a PDF form into a Quote object using profile-driven field extraction.

    1. Open PDF, compute fingerprint, match to a profile
    2. Read AcroForm fields using the profile's semantic map
    3. Build a Quote from the extracted data
    4. Return Quote + any parse warnings

    Falls back to empty Quote with warnings if no profile matches.
    """
    warnings: list[ParseWarning] = []

    if not os.path.exists(pdf_path):
        return Quote(), [ParseWarning("file", f"PDF not found: {pdf_path}", "error")]

    if profiles is None:
        profiles = load_profiles()

    # Match profile by content fingerprint
    profile = match_profile(pdf_path, profiles)
    if not profile:
        warnings.append(ParseWarning("profile", "No profile matched — using fallback empty parse"))
        return Quote(provenance=Provenance(
            source="pdf",
            parsed_from_files=[pdf_path],
            parse_warnings=["No profile matched"],
        )), warnings

    # Read PDF fields
    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        pdf_fields = reader.get_fields() or {}
    except Exception as e:
        warnings.append(ParseWarning("pdf", f"Failed to read PDF: {e}", "error"))
        return Quote(provenance=Provenance(
            source="pdf",
            parsed_from_files=[pdf_path],
            parse_warnings=[str(e)],
        )), warnings

    def _get(pdf_field_name: str) -> str:
        """Extract a field value from the PDF."""
        field = pdf_fields.get(pdf_field_name)
        if field is None:
            return ""
        if isinstance(field, dict):
            val = field.get("/V", "")
        else:
            val = str(field)
        return str(val).strip() if val else ""

    # ── Extract header fields ──
    header = QuoteHeader()
    buyer = BuyerInfo()
    ship_to = Address()

    for fm in profile.fields:
        if "[n]" in fm.semantic:
            continue  # Row fields handled below

        val = _get(fm.pdf_field)
        if not val:
            continue

        # Map semantic name to Quote field
        if fm.semantic == "header.solicitation_number":
            header.solicitation_number = val
        elif fm.semantic == "header.due_date":
            from datetime import datetime
            for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    header.due_date = datetime.strptime(val, fmt).date()
                    break
                except ValueError:
                    continue
            if not header.due_date:
                warnings.append(ParseWarning("header.due_date", f"Unparseable date: {val}"))
        elif fm.semantic == "header.due_time":
            from datetime import datetime, time
            for tfmt in ("%I:%M %p", "%I:%M%p", "%H:%M"):
                try:
                    t = datetime.strptime(val, tfmt)
                    header.due_time = time(t.hour, t.minute)
                    header.due_time_explicit = True
                    break
                except ValueError:
                    continue
        elif fm.semantic == "buyer.requestor_name":
            buyer.requestor_name = val
        elif fm.semantic == "buyer.institution":
            header.institution_key = val
        elif fm.semantic == "buyer.phone":
            buyer.requestor_phone = val
        elif fm.semantic == "ship_to.address":
            ship_to.full = val
        elif fm.semantic == "ship_to.zip_code":
            ship_to.zip_code = val
        elif fm.semantic == "totals.notes":
            buyer.notes = val

    # ── Extract line items ──
    line_items = []
    capacities = profile.page_row_capacities
    item_idx = 0

    for page_num, capacity in enumerate(capacities, start=1):
        for row in range(1, capacity + 1):
            row_fields = profile.get_row_fields(row, page=page_num)
            if not row_fields:
                continue

            # Check if this row has any data
            desc_val = ""
            qty_val = ""
            price_val = ""
            uom_val = ""
            item_no_val = ""
            sub_val = ""

            for sem, pdf_field in row_fields.items():
                val = _get(pdf_field)
                if not val:
                    continue
                field_part = sem.split(".")[-1]
                if field_part == "description":
                    desc_val = val
                elif field_part == "qty":
                    qty_val = val
                elif field_part == "unit_price":
                    price_val = val
                elif field_part == "uom":
                    uom_val = val
                elif field_part == "item_no":
                    item_no_val = val
                elif field_part == "substituted":
                    sub_val = val

            # Skip empty rows
            if not desc_val and not qty_val and not price_val:
                continue

            item_idx += 1
            item = LineItem(
                line_no=item_idx,
                description=desc_val,
                qty=_safe_decimal(qty_val, Decimal("1")),
                uom=uom_val or "EA",
                item_no=sub_val or item_no_val,  # MFG# from substituted column preferred
            )

            # Parse price if present (buyer may have pre-filled)
            if price_val:
                price = _safe_decimal(price_val.replace(",", "").replace("$", ""), Decimal("0"))
                if price > 0:
                    item.unit_cost = price  # Treat buyer's price as a reference
                    item.price_source = "buyer_prefilled"

            line_items.append(item)

    if not line_items:
        warnings.append(ParseWarning("items", "No line items found in PDF"))

    # ── Build Quote ──
    quote = Quote(
        doc_type="pc",  # Default — caller can override
        header=header,
        buyer=buyer,
        ship_to=ship_to,
        line_items=line_items,
        provenance=Provenance(
            source="pdf",
            parsed_from_files=[pdf_path],
            classifier_shape=profile.id,
            parse_warnings=[w.message for w in warnings],
        ),
    )

    log.info("parse_engine: %s → %d items, profile=%s, %d warnings",
             os.path.basename(pdf_path), len(line_items), profile.id, len(warnings))

    return quote, warnings


def _safe_decimal(val: str, default: Decimal = Decimal("0")) -> Decimal:
    """Parse a string to Decimal, returning default on failure."""
    if not val:
        return default
    try:
        return Decimal(val.strip())
    except (InvalidOperation, ValueError):
        return default
