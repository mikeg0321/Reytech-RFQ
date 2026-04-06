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
