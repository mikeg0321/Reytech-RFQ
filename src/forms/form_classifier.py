"""Classify an uploaded file into a 'contract bundle' slot.

Used by the unified contract-upload route to decide, per file:
  - Where the bytes go on disk
  - Which slot on the RFQ record to populate
  - What category to use in the rfq_files table

Classification order:
  1. Mimetype-based image check → email_screenshot
  2. Filename-based for PDFs via src.forms.rfq_parser.identify_attachments
  3. PDF form-field fingerprint fallback (703a/703b/703c/704b)
  4. Otherwise → attachment
"""
from __future__ import annotations

import logging
import os
from io import BytesIO

log = logging.getLogger(__name__)

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff"}
TEMPLATE_SLOTS = {"703a", "703b", "703c", "704b", "bidpkg",
                  "dsh_attA", "dsh_attB", "dsh_attC"}


def _ext(name: str) -> str:
    return os.path.splitext(name or "")[1].lower()


def _fingerprint_pdf(data: bytes) -> str | None:
    """Inspect AcroForm field names to guess form type.

    Returns one of TEMPLATE_SLOTS or None when no confident match.
    """
    try:
        from pypdf import PdfReader
        reader = PdfReader(BytesIO(data))
        fields = reader.get_fields() or {}
    except Exception as e:
        log.debug("fingerprint: pypdf read failed: %s", e)
        return None
    names = list(fields.keys())
    if not names:
        return None
    has_prefix = lambda pfx: any(n.startswith(pfx) for n in names)
    if has_prefix("703C_"):
        return "703c"
    if has_prefix("703B_"):
        return "703b"
    if has_prefix("703A_"):
        return "703a"
    # 704B variants: Reytech template uses "COMPANY REPRESENTATIVE print name"
    # + numbered row fields; buyer 704B uses "ITEM DESCRIPTION PRODUCT
    # SPECIFICATIONRow1" + "Contract_Number".
    joined = "\n".join(names)
    if "ITEM DESCRIPTION PRODUCT SPECIFICATIONRow" in joined:
        return "704b"
    if "COMPANY REPRESENTATIVE" in joined and "Date of Request" in joined:
        return "704b"
    return None


def classify(filename: str, data: bytes) -> dict:
    """Classify a single uploaded file.

    Returns:
        {
          "kind":     "email_screenshot" | "template" | "attachment",
          "slot":     None | "703a" | "703b" | "703c" | "704b" | "bidpkg" | "dsh_attA"/B/C,
          "category": DB category column for rfq_files,
          "reason":   short human-readable explanation,
        }
    """
    ext = _ext(filename)

    # Step 1 — images go straight to email_screenshot
    if ext in IMAGE_EXTS:
        return {"kind": "email_screenshot", "slot": None,
                "category": "email_screenshot",
                "reason": f"image ({ext}) routed to email contract"}

    # Only PDFs are candidates for template slots. Everything else is an
    # attachment — Word docs, text files, .eml, etc.
    if ext != ".pdf":
        return {"kind": "attachment", "slot": None,
                "category": "attachment",
                "reason": f"non-pdf ({ext or 'no ext'}) stored as attachment"}

    # Step 2 — filename-based classifier (same rules as the bulk-email flow)
    try:
        from src.forms.rfq_parser import identify_attachments
        hits = identify_attachments([filename])
    except Exception as e:
        log.debug("identify_attachments failed: %s", e)
        hits = {}
    if hits:
        slot = next(iter(hits.keys()))
        return {"kind": "template", "slot": slot, "category": "template",
                "reason": f"filename matched {slot}"}

    # Step 3 — PDF field fingerprint
    slot = _fingerprint_pdf(data)
    if slot:
        return {"kind": "template", "slot": slot, "category": "template",
                "reason": f"form fields matched {slot}"}

    # Step 4 — give up, store as attachment
    return {"kind": "attachment", "slot": None, "category": "attachment",
            "reason": "no template match — stored as attachment"}
