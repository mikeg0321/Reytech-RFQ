"""
Office document text extractor for procurement parsing pipeline.

Extracts text/tables from XLS, XLSX, DOC, DOCX files so they can be
fed into the same Claude API extraction used by the vision parser.

Supported formats:
- XLSX: openpyxl (already in requirements)
- XLS:  xlrd (optional — falls back to error message)
- DOCX: python-docx (optional — falls back to error message)
- DOC:  not natively supported — user should convert to DOCX
"""

import os
import logging

log = logging.getLogger("reytech.doc_converter")

# Accepted office extensions (lowercase, with dot)
OFFICE_EXTS = (".xls", ".xlsx", ".doc", ".docx")

# All parseable extensions (office + pdf + image)
ALL_UPLOAD_EXTS = (
    ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff", ".tif",
    ".xls", ".xlsx", ".doc", ".docx",
)

# HTML accept string for file inputs
ACCEPT_ALL = (
    ".pdf,.png,.jpg,.jpeg,.gif,.webp,.bmp,.tiff,"
    ".xls,.xlsx,.doc,.docx"
)


def is_office_doc(filename: str) -> bool:
    """Check if filename has an office document extension."""
    return os.path.splitext(filename)[1].lower() in OFFICE_EXTS


def extract_text(file_path: str) -> str:
    """Extract text content from an office document.

    Returns a plain-text representation suitable for Claude API extraction.
    Raises ValueError if the format is unsupported or a required library is missing.
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".xlsx":
        return _extract_xlsx(file_path)
    elif ext == ".xls":
        return _extract_xls(file_path)
    elif ext == ".docx":
        return _extract_docx(file_path)
    elif ext == ".doc":
        raise ValueError(
            "Legacy .doc format is not supported. Please save as .docx and re-upload."
        )
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def _extract_xlsx(file_path: str) -> str:
    """Extract text from XLSX using openpyxl."""
    try:
        from openpyxl import load_workbook
    except ImportError:
        raise ValueError("openpyxl is not installed — cannot read XLSX files")

    wb = load_workbook(file_path, read_only=True, data_only=True)
    parts = []

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue

        parts.append(f"=== Sheet: {sheet_name} ===")

        for row in rows:
            # Skip fully empty rows
            vals = [str(c) if c is not None else "" for c in row]
            if not any(v.strip() for v in vals):
                continue
            parts.append("\t".join(vals))

    wb.close()

    if not parts:
        raise ValueError("XLSX file has no data in any sheet")

    text = "\n".join(parts)
    log.info("Extracted %d chars from XLSX (%d sheets)", len(text), len(wb.sheetnames))
    return text


def _extract_xls(file_path: str) -> str:
    """Extract text from legacy XLS using xlrd."""
    try:
        import xlrd
    except ImportError:
        raise ValueError(
            "xlrd is not installed — cannot read legacy .xls files. "
            "Please save as .xlsx and re-upload."
        )

    wb = xlrd.open_workbook(file_path)
    parts = []

    for sheet_idx in range(wb.nsheets):
        ws = wb.sheet_by_index(sheet_idx)
        if ws.nrows == 0:
            continue

        parts.append(f"=== Sheet: {ws.name} ===")

        for row_idx in range(ws.nrows):
            vals = []
            for col_idx in range(ws.ncols):
                cell = ws.cell(row_idx, col_idx)
                vals.append(str(cell.value) if cell.value is not None else "")
            if not any(v.strip() for v in vals):
                continue
            parts.append("\t".join(vals))

    if not parts:
        raise ValueError("XLS file has no data in any sheet")

    text = "\n".join(parts)
    log.info("Extracted %d chars from XLS (%d sheets)", len(text), wb.nsheets)
    return text


def _extract_docx(file_path: str) -> str:
    """Extract text from DOCX using python-docx."""
    try:
        import docx
    except ImportError:
        raise ValueError(
            "python-docx is not installed — cannot read DOCX files. "
            "Please convert to PDF and re-upload."
        )

    doc = docx.Document(file_path)
    parts = []

    # Extract paragraphs
    for para in doc.paragraphs:
        text = para.text.strip()
        if text:
            parts.append(text)

    # Extract tables (common in procurement docs)
    for table in doc.tables:
        parts.append("")  # blank line before table
        for row in table.rows:
            vals = [cell.text.strip() for cell in row.cells]
            parts.append("\t".join(vals))

    if not parts:
        raise ValueError("DOCX file has no text or table content")

    text = "\n".join(parts)
    log.info("Extracted %d chars from DOCX (%d paragraphs, %d tables)",
             len(text), len(doc.paragraphs), len(doc.tables))
    return text


# ═══════════════════════════════════════════════════════════════════════
# Regex fallback: parse simple item lists from extracted text
# ═══════════════════════════════════════════════════════════════════════

import re

def parse_items_from_text(text: str) -> list:
    """Parse item descriptions + quantities from plain text (regex fallback).

    Handles common patterns like:
        Product description here
        Qty: 7
        ASIN = B0CZRF2DZR

        Another product description
        Qty = 4 Each

    Returns list of dicts in the standard line_items format.
    """
    if not text or len(text.strip()) < 10:
        return []

    lines = [l.strip() for l in text.split("\n")]
    items = []
    current_desc = ""
    current_qty = 1
    current_uom = "each"
    current_part = ""
    current_asin = ""

    # Patterns
    qty_pat = re.compile(r'^(?:qty|quantity)\s*[:=]\s*(\d+)\s*(.*)', re.IGNORECASE)
    asin_pat = re.compile(r'^ASIN\s*[:=]\s*(\S+)', re.IGNORECASE)
    upc_pat = re.compile(r'^UPC\s*[:=]\s*(\S+)', re.IGNORECASE)
    model_pat = re.compile(r'^(?:Model\s*(?:Number|#|No\.?)?|MFG\s*#?|Part\s*(?:Number|#|No\.?))\s*[:=]\s*(\S+)', re.IGNORECASE)
    unit_count_pat = re.compile(r'^Unit\s*Count\s*[:=]', re.IGNORECASE)
    # Skip lines that are metadata (product dimensions, color, style, etc.)
    meta_pat = re.compile(r'^(?:Product Dimensions|Color|Style|Base Material|Top Material|'
                          r'Finish Type|Special Feature|Brand|Item Weight|Manufacturer)\b', re.IGNORECASE)

    def _flush():
        nonlocal current_desc, current_qty, current_uom, current_part, current_asin
        if current_desc and len(current_desc) >= 5:
            items.append({
                "item_number": str(len(items) + 1),
                "qty": current_qty,
                "uom": current_uom,
                "qty_per_uom": 1,
                "description": current_desc,
                "part_number": current_part or current_asin,
                "item_link": "",
                "row_index": len(items),
            })
        current_desc = ""
        current_qty = 1
        current_uom = "each"
        current_part = ""
        current_asin = ""

    for line in lines:
        if not line:
            continue

        # Skip table metadata lines
        if meta_pat.match(line):
            continue
        if unit_count_pat.match(line):
            continue

        # Check for qty line
        m = qty_pat.match(line)
        if m:
            current_qty = int(m.group(1))
            uom_text = m.group(2).strip().lower()
            if uom_text and uom_text in ("each", "ea", "pack", "set", "box", "case", "pair"):
                current_uom = uom_text
            continue

        # Check for ASIN
        m = asin_pat.match(line)
        if m:
            current_asin = m.group(1)
            continue

        # Check for UPC / Model / Part number
        m = upc_pat.match(line) or model_pat.match(line)
        if m:
            current_part = m.group(1)
            continue

        # If line is long enough to be a description (>20 chars) and we already
        # have a description buffered, flush the previous item first
        if len(line) > 20 and current_desc:
            _flush()

        # Buffer as description (or append to short existing desc)
        if not current_desc:
            current_desc = line
        elif len(current_desc) < 20:
            current_desc += " " + line

    # Flush last item
    _flush()

    if items:
        log.info("Regex fallback parsed %d items from text", len(items))
    return items
