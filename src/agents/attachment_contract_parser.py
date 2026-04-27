"""attachment_contract_parser.py — Phase 1.6 PR3b.

The buyer's "Bid Instructions" / "Solicitation Cover Sheet" PDF often
holds the authoritative forms list — the email body just says "see
attached, due Friday." Today the requirement_extractor only reads the
email body + subject, so attachment-borne requirements vanish.

This module:
  1. Extracts text from each PDF attachment (pdfplumber, no LLM cost)
  2. Runs the existing _extract_with_regex on each attachment
  3. Merges per-attachment results into one RFQRequirements
  4. Returns the merged contract — caller unions with email contract

Design notes:
  - Deterministic-only path on attachments. Running Claude on every
    attached PDF (often 10-20 per package) would 10-20× the per-quote
    extractor cost. Stays within `requirement_extractor` regex budget.
  - Cover-sheet detection: filenames containing 'instruction', 'cover',
    'solicitation', 'bid_package' get prioritized — their forms list
    overrides others on conflict.
"""

import logging
import os
from typing import Optional

log = logging.getLogger("reytech.attachment_contract")

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    log.debug("pdfplumber not available — attachment parsing disabled")


# Filenames containing any of these tokens get treated as authoritative
# cover sheets. Their requirements take precedence on conflict.
COVER_SHEET_TOKENS = (
    "bid_instructions", "bid instructions",
    "solicitation",
    "cover_sheet", "cover sheet", "coversheet",
    "bid_package", "bid package",
    "rfq_cover", "rfq cover",
    "instructions to bidders",
    "request for quotation",
)


def parse_attachments_for_requirements(attachments: list,
                                       max_pages_per_pdf: int = 10):
    """Walk attachments, extract text, run regex extractor, merge.

    Args:
        attachments: list of {filename, file_path?, file_bytes?}
        max_pages_per_pdf: cap pages parsed per PDF (perf bound; cover
            sheets are typically pages 1-3, so 10 is generous)

    Returns:
        RFQRequirements with merged fields from all parseable PDFs.
        Returns an empty instance if pdfplumber missing or nothing
        parseable.
    """
    from src.agents.requirement_extractor import (
        RFQRequirements, _extract_with_regex,
    )

    if not attachments or not HAS_PDFPLUMBER:
        return RFQRequirements()

    cover_results = []
    other_results = []

    for att in attachments:
        if not _is_pdf(att):
            continue
        text = _extract_pdf_text(att, max_pages_per_pdf)
        if not text or len(text) < 50:
            continue
        try:
            r = _extract_with_regex(text, attachments=[])
            if not r.has_requirements:
                continue
        except Exception as e:
            log.debug("attachment regex extract failed for %s: %s",
                      att.get("filename"), e)
            continue
        if _looks_like_cover_sheet(att):
            cover_results.append(r)
        else:
            other_results.append(r)

    # Merge: cover sheets first (authoritative), then others
    merged = RFQRequirements()
    for r in cover_results + other_results:
        _merge_into(merged, r)

    if merged.has_requirements:
        merged.extraction_method = "attachment_regex"
        # Confidence reflects deterministic-only source
        merged.confidence = 0.55

    return merged


# ─── Helpers ───────────────────────────────────────────────────────────────

def _is_pdf(att: dict) -> bool:
    name = (att.get("filename", "") or "").lower()
    ftype = (att.get("file_type", "") or "").lower()
    return name.endswith(".pdf") or ftype == "pdf" or "pdf" in ftype


def _looks_like_cover_sheet(att: dict) -> bool:
    name = (att.get("filename", "") or "").lower().replace("-", "_")
    return any(tok.replace("-", "_") in name for tok in COVER_SHEET_TOKENS)


def _extract_pdf_text(att: dict, max_pages: int) -> str:
    """Read up to N pages of text from a PDF attachment.

    Supports two attachment shapes:
      - {file_path: "/path/to/file.pdf"}    (PCs)
      - {file_id, filename}                 (RFQs, blob in DB)
    """
    path = att.get("file_path")
    if path and os.path.isfile(path):
        return _pdf_text_from_path(path, max_pages)

    file_id = att.get("file_id")
    if file_id is not None:
        return _pdf_text_from_blob(file_id, max_pages)

    return ""


def _pdf_text_from_path(path: str, max_pages: int) -> str:
    try:
        with pdfplumber.open(path) as pdf:
            return _walk_pages(pdf, max_pages)
    except Exception as e:
        log.debug("pdfplumber open(%s) failed: %s", path, e)
        return ""


def _pdf_text_from_blob(file_id, max_pages: int) -> str:
    """Load BLOB from rfq_files DB and parse."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT data FROM rfq_files WHERE id = ?", (file_id,)
            ).fetchone()
            if not row or not row["data"]:
                return ""
            import io
            with pdfplumber.open(io.BytesIO(row["data"])) as pdf:
                return _walk_pages(pdf, max_pages)
    except Exception as e:
        log.debug("pdfplumber blob(%s) failed: %s", file_id, e)
        return ""


def _walk_pages(pdf, max_pages: int) -> str:
    chunks = []
    for i, page in enumerate(pdf.pages):
        if i >= max_pages:
            break
        try:
            t = page.extract_text() or ""
            if t:
                chunks.append(t)
        except Exception as _e:
            log.debug("page.extract_text suppressed: %s", _e)
    return "\n".join(chunks)


def _merge_into(target, source) -> None:
    """In-place merge — target absorbs non-empty fields from source.

    Lists union; scalars take target value if non-empty else source.
    """
    # Lists: union preserving order
    for fld in ("forms_required", "special_instructions",
                "attachment_types", "template_urls",
                "raw_form_matches"):
        seen = set(getattr(target, fld) or [])
        for v in (getattr(source, fld) or []):
            if v not in seen:
                getattr(target, fld).append(v)
                seen.add(v)

    # Scalars: prefer existing non-empty target value, else source
    for fld in ("due_date", "due_time", "delivery_location",
                "buyer_name", "buyer_email", "buyer_phone",
                "solicitation_number"):
        if not getattr(target, fld) and getattr(source, fld):
            setattr(target, fld, getattr(source, fld))

    # Booleans: OR
    if getattr(source, "food_items_present", False):
        target.food_items_present = True


def merge_with_email_contract(email_contract, attachment_contract):
    """Merge attachment requirements INTO email contract (in place).

    Email contract wins on scalar conflicts (it was authored directly
    to the buyer; attachments are reference docs). Lists union.

    Returns the email_contract for chaining; mutation is in-place too.
    """
    if not attachment_contract or not getattr(attachment_contract,
                                              "has_requirements", False):
        return email_contract
    _merge_into(email_contract, attachment_contract)
    # Bump method label so callers can see attachment contributed
    if email_contract.extraction_method in ("regex", "claude"):
        email_contract.extraction_method = (
            email_contract.extraction_method + "+attachment"
        )
    elif not email_contract.extraction_method or \
         email_contract.extraction_method == "none":
        email_contract.extraction_method = "attachment_regex"
    return email_contract
