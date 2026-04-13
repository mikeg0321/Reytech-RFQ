"""CCHCS Non-IT RFQ Packet parser.

The CCHCS "Non-Cloud RFQ Packet" is a single 18-page fillable PDF that
bundles the cover sheet, line item table, DVBE program requirements,
response checklist, RFQ form, and attachments 1-11 (including the AMS 708
GenAI questionnaire) into one document. It is NOT a 704 — it has its own
form field names and its own line item schema.

Schema reference: buyer pre-fills the HEADER block (solicitation number,
institution, requestor email, due date) and the LINE ITEM TABLE (up to
10 rows with Qty/Unit/Description/Model number per row). The supplier
fills in Price Per Unit + Extension Total per row, supplier info, SB/MB/
DVBE cert, signature, date, and the various compliance checkboxes.

Output shape matches the existing PC `parsed` schema so the rest of the
pipeline (pricing, save-prices, generate) can consume it unchanged.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Dict, Any, List, Optional

log = logging.getLogger("reytech.cchcs_packet")

try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False


# ── Field name constants (the canonical schema pulled from the Apr 2026 packet) ──

HEADER_FIELDS = {
    "solicitation_number": "Solicitation No",
    "institution": "Institution Name",
    "institution_address_1": "Institution Address 1",
    "institution_address_2": "Institution Address 2",
    "requestor_email": "Email",
    "due_date": "before",
    "due_time": "Time",
    "request_date": "Today",
    "questions_due": "Questions Due Date/Time",
    "requestor_name": "Text7",
}

# Line item field templates. Up to 10 rows indexed 1..10.
LINE_ITEM_TEMPLATES = {
    "qty": "Qty{n}",
    "uom": "Unit{n}",
    "description": "Item Description{n}",
    "mfg_number": "Model or Part Number{n}",
    "price_per_unit": "Price Per Unit{n}",
    "extension": "Extension Total{n}",
}

MAX_ROWS = 10

# Supplier-side fields we will need to fill later — surfaced here so the
# filler can import them directly instead of re-deriving.
SUPPLIER_FIELDS = {
    "company_name": "Supplier Name",
    "address_1": "Supplier Address 1",
    "address_2": "Supplier Address 2",
    "address_3": "Supplier Address 3",
    "contact_name": "Contact Name",
    "phone": "Phone",
    "email": "Supplier Email",
    "cert_number": "SBMBDVBE Certification  if applicable",  # double space is intentional per the PDF
    "cert_expiration": "Expiration Date",
    "signature": "Signature1_es_:signer:signature",
    "date_signed": "Date_es_:date",
    "rev": "Rev",
    "amount_total": "Amount",
}

TOTALS_FIELDS = {
    "subtotal": "Extension TotalSubtotal",
    "freight": "Extension TotalFOB Destination Freight Prepaid",
    "sales_tax": "Extension TotalSales Tax",
    "grand_total": "Extension TotalTotal",
}

# Filename / subject patterns the poller can use to detect this format
FILENAME_PATTERN = re.compile(
    r"(?:Non[- ]?Cloud\s*)?RFQ\s*Packet.*?(?:PREQ|REQ)?(\d{5,})",
    re.IGNORECASE,
)
SUBJECT_PATTERN = re.compile(
    r"\b(?:PREQ|Non[- ]?Cloud)\s*(\d{5,})|(?:PREQ|Non[- ]?Cloud).*?(\d{5,})",
    re.IGNORECASE,
)


def looks_like_cchcs_packet(filename: str = "", subject: str = "") -> bool:
    """Cheap pattern check. Returns True if either the filename or subject
    matches the CCHCS packet format. Safe to call without the PDF in hand."""
    if filename and FILENAME_PATTERN.search(filename):
        return True
    if subject and SUBJECT_PATTERN.search(subject):
        return True
    return False


def _read_fields(path: str) -> Dict[str, str]:
    """Return a flat dict of field_name -> string value for every form
    field in the PDF. Empty strings for fields with no value. Raises on
    IO / corruption — caller decides how to handle."""
    reader = PdfReader(path)
    raw = reader.get_fields() or {}
    out: Dict[str, str] = {}
    for name, spec in raw.items():
        val = ""
        if isinstance(spec, dict):
            v = spec.get("/V", "")
            val = str(v).strip() if v else ""
        out[str(name)] = val
    return out


def _float(v: Any, default: float = 0.0) -> float:
    if v is None or v == "":
        return default
    try:
        return float(str(v).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def _int(v: Any, default: int = 0) -> int:
    if v is None or v == "":
        return default
    try:
        return int(float(str(v).replace(",", "").strip()))
    except (TypeError, ValueError):
        return default


def parse_cchcs_packet(pdf_path: str) -> Dict[str, Any]:
    """Parse a CCHCS Non-IT RFQ packet into the canonical Reytech parsed
    shape. Returns a dict with keys compatible with the existing PC
    parser output so downstream code doesn't need to special-case this
    format.

    Shape:
        {
            "ok": bool,
            "error": str,                    # only if ok=False
            "packet_type": "cchcs_non_it",
            "header": {...},
            "line_items": [...],
            "supplier_current": {...},       # what's currently in the packet
            "existing_prices": {...},        # row_index -> float (for backcompat)
            "source_pdf": "...",
            "field_count": int,
            "parse_method": "cchcs_packet_fields",
            "parse_quality": {...},
        }
    """
    result: Dict[str, Any] = {
        "ok": True,
        "packet_type": "cchcs_non_it",
        "header": {},
        "line_items": [],
        "supplier_current": {},
        "existing_prices": {},
        "source_pdf": pdf_path,
        "field_count": 0,
        "parse_method": "cchcs_packet_fields",
    }

    if not HAS_PYPDF:
        result["ok"] = False
        result["error"] = "pypdf not available"
        return result
    if not os.path.exists(pdf_path):
        result["ok"] = False
        result["error"] = f"source PDF not found: {pdf_path}"
        return result

    try:
        fields = _read_fields(pdf_path)
    except Exception as e:
        log.error("cchcs_packet: unreadable PDF %s: %s", pdf_path, e)
        result["ok"] = False
        result["error"] = f"pypdf read error: {e}"
        return result

    result["field_count"] = len(fields)

    # ── Header ──
    for key, fname in HEADER_FIELDS.items():
        result["header"][key] = fields.get(fname, "")

    # Normalize a handful of common header aliases so the rest of the
    # pipeline can pick them up without knowing the packet format:
    result["header"]["pc_number"] = result["header"].get("solicitation_number", "")
    result["header"]["agency"] = "CDCR"  # CCHCS is routed under CDCR in agency_config
    result["header"]["zip_code"] = _extract_zip(result["header"].get("institution_address_2", ""))
    result["header"]["requestor"] = result["header"].get("requestor_name", "")

    # ── Line items ──
    for n in range(1, MAX_ROWS + 1):
        row = {}
        any_value = False
        for slot, tmpl in LINE_ITEM_TEMPLATES.items():
            v = fields.get(tmpl.format(n=n), "")
            row[slot] = v
            if v:
                any_value = True

        # A row with nothing but qty OR description counts — buyer may
        # fill either. A totally empty row is skipped.
        has_buyer_data = any(row.get(k) for k in ("qty", "uom", "description", "mfg_number"))
        if not has_buyer_data:
            continue

        qty = _int(row.get("qty"), 0)
        uom = (row.get("uom") or "").strip() or "EA"
        desc = (row.get("description") or "").strip()
        mfg = (row.get("mfg_number") or "").strip()
        price_unit = _float(row.get("price_per_unit"))
        ext = _float(row.get("extension"))

        item = {
            "row_index": n,
            "item_number": str(n),
            "qty": qty,
            "uom": uom.upper()[:8],
            "qty_per_uom": 1,  # CCHCS packet has no QPU column
            "description": desc[:300],
            "mfg_number": mfg[:100],
            "part_number": mfg[:100],
            "unit_price": price_unit,
            "extension": ext,
            "pricing": {
                "unit_cost": 0.0,   # supplier fills
                "recommended_price": price_unit,
            },
        }
        result["line_items"].append(item)
        if price_unit:
            result["existing_prices"][n] = price_unit

    # ── Supplier current state ──
    for key, fname in SUPPLIER_FIELDS.items():
        result["supplier_current"][key] = fields.get(fname, "")

    # ── Totals roll-up if buyer pre-filled (they won't — but defensive) ──
    result["header"]["subtotal"] = _float(fields.get(TOTALS_FIELDS["subtotal"]))
    result["header"]["freight"] = _float(fields.get(TOTALS_FIELDS["freight"]))
    result["header"]["tax"] = _float(fields.get(TOTALS_FIELDS["sales_tax"]))
    result["header"]["total"] = _float(fields.get(TOTALS_FIELDS["grand_total"]))

    # ── Parse quality metrics ──
    items_found = len(result["line_items"])
    # Score: we need header sol# AND at least 1 item. 100 if all buyer
    # fields present, deductions per missing header field.
    score = 0
    if result["header"].get("solicitation_number"):
        score += 40
    if result["header"].get("institution"):
        score += 20
    if result["header"].get("requestor_email"):
        score += 10
    if result["header"].get("due_date"):
        score += 10
    if items_found >= 1:
        score += 20
    result["parse_quality"] = {
        "grade": "A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60 else "F",
        "score": score,
        "expected_items": items_found,
        "parsed_items": items_found,
        "warnings": [] if items_found >= 1 else ["no line items found"],
    }

    log.info(
        "cchcs_packet parsed %s: sol=%s items=%d score=%d",
        os.path.basename(pdf_path),
        result["header"].get("solicitation_number", "?"),
        items_found,
        score,
    )
    return result


def _extract_zip(s: str) -> str:
    if not s:
        return ""
    m = re.search(r"\b(\d{5}(?:-\d{4})?)\b", s)
    return m.group(1) if m else ""


__all__ = [
    "parse_cchcs_packet",
    "looks_like_cchcs_packet",
    "HEADER_FIELDS",
    "LINE_ITEM_TEMPLATES",
    "SUPPLIER_FIELDS",
    "TOTALS_FIELDS",
    "MAX_ROWS",
]
