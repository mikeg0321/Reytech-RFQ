"""AMS 701B "Purchase Order Distribution List" parser.

A *supplemental* attachment that buyers (CDCR/CCHCS) attach when one order
ships to many facilities. Its columns are labeled LINE ITEM NUMBER / QUANTITY /
UNIT OF MEASURE / DESCRIPTION — which look exactly like a line-item table — but
each row is a per-facility DELIVERY ALLOCATION, not a distinct SKU. Feeding it
to the item parsers mints phantom line items and discards the facility / address
columns (Coleman 10842771 incident; see §0 LAW 6 "READ THE WHOLE CONTRACT").

This module detects such an attachment and parses it into a structured
distribution list so ingest can (a) keep it OUT of the line-item parsers and
(b) carry the per-facility allocation forward for ship-to + tax resolution.

Pure: no DB, no Flask, no network. pdfplumber only.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

log = logging.getLogger("reytech.distribution_list")

# Header text that identifies an AMS 701B / generic distribution list. Matched
# against the first page's text (case-insensitive). Header-based, not filename-
# based — buyers name the file anything.
_HEADER_SIGNATURES = (
    "purchase order distribution list",
    "ams 701b",
    "ams701b",
    "delivery distribution list",
    "distribution list",
)

# Column-header tokens that must co-occur to treat a table as a distribution
# list (guards against a normal item table that merely says "distribution"
# somewhere). A real 701B carries a Facility column alongside qty/description.
_FACILITY_COL_TOKENS = ("facility", "plant", "delivery location")

_ZIP_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\b")


def _first_page_text(path: str) -> str:
    import pdfplumber
    with pdfplumber.open(path) as pdf:
        if not pdf.pages:
            return ""
        return (pdf.pages[0].extract_text() or "")


def is_distribution_list(path: str) -> bool:
    """True if `path` is an AMS 701B / delivery distribution list PDF.

    Header-text based. Requires a distribution-list signature AND a facility
    column token so a normal RFQ that mentions "distribution" isn't misread.
    """
    if not path or not path.lower().endswith(".pdf"):
        return False
    try:
        text = _first_page_text(path).lower()
    except Exception as e:
        log.debug("is_distribution_list: could not read %s: %s", path, e)
        return False
    if not text:
        return False
    has_sig = any(sig in text for sig in _HEADER_SIGNATURES)
    has_facility_col = any(tok in text for tok in _FACILITY_COL_TOKENS)
    return has_sig and has_facility_col


def _extract_mfg(description: str) -> str:
    """Pull the trailing part number from a description like
    '... Defibrillators - 8700-0893-01' -> '8700-0893-01'. The part number can
    itself contain hyphens, so split on the LAST ' - ' delimiter, not '-'."""
    if not description:
        return ""
    if " - " in description:
        return description.rsplit(" - ", 1)[-1].strip()
    return ""


def parse_distribution_list(path: str) -> Dict[str, Any]:
    """Parse an AMS 701B into a structured distribution list.

    Returns:
        {
          "form": "AMS 701B",
          "source_file": <basename>,
          "row_count": int,
          "rows": [ {line, qty, uom, description, mfg_number,
                     facility_code, facility_address, zip, primary_contact,
                     receiver_email}, ... ],
          "sku_totals": { mfg_number: total_qty },
          "distinct_facilities": [facility_code, ...],
        }

    Caller is responsible for resolving each row's facility_address to a tax
    jurisdiction (tax_resolver) — this module only structures what the buyer
    put on the page.
    """
    import os
    import pdfplumber

    rows: List[Dict[str, Any]] = []
    with pdfplumber.open(path) as pdf:
        for pg in pdf.pages:
            for table in pg.extract_tables() or []:
                for r in table:
                    if not r or not r[0]:
                        continue
                    c0 = (r[0] or "").strip()
                    if not c0.isdigit():
                        continue  # header / grouping rows
                    desc = (r[3] or "").replace("\n", " ").strip() if len(r) > 3 else ""
                    addr = (r[5] or "").replace("\n", " ").strip() if len(r) > 5 else ""
                    zip_m = _ZIP_RE.search(addr)
                    try:
                        qty = int(float((r[1] or "0").strip() or 0))
                    except (ValueError, TypeError):
                        qty = 0
                    rows.append({
                        "line": int(c0),
                        "qty": qty,
                        "uom": (r[2] or "").strip() if len(r) > 2 else "",
                        "description": desc,
                        "mfg_number": _extract_mfg(desc),
                        "facility_code": (r[4] or "").strip() if len(r) > 4 else "",
                        "facility_address": addr,
                        "zip": zip_m.group(1) if zip_m else "",
                        "primary_contact": (r[6] or "").strip() if len(r) > 6 else "",
                        "receiver_email": (r[7] or "").strip() if len(r) > 7 else "",
                    })

    sku_totals: Dict[str, int] = {}
    for row in rows:
        sku_totals[row["mfg_number"]] = sku_totals.get(row["mfg_number"], 0) + row["qty"]

    return {
        "form": "AMS 701B",
        "source_file": os.path.basename(path),
        "row_count": len(rows),
        "rows": rows,
        "sku_totals": sku_totals,
        "distinct_facilities": sorted({r["facility_code"] for r in rows if r["facility_code"]}),
    }


# Cross-reference cues a primary form may use to point at a supplemental
# attachment (§0 LAW 6 "mandatory reading orders"). Used by ingest to flag a
# referenced-but-unparsed attachment.
_CROSS_REF_CUES = (
    "see attached distribution list",
    "attached distribution list",
    "distribution list",
    "supplemental",
    "see attachment",
    "see attached schedule",
)


def text_references_distribution(text: str) -> bool:
    """True if parsed form text points at a supplemental distribution list."""
    if not text:
        return False
    t = text.lower()
    return any(cue in t for cue in _CROSS_REF_CUES)
