"""Attachment-deadline extraction — PR-AO.

The four ⚠ DEFAULT records Mike was seeing on /home (pc_93edc64e,
pc_5728f934, rfq_0ebe242f, rfq_b57f85f7) all had:
  - email body with no parsable deadline (buyer wrote "see attached")
  - email subject with no parsable deadline
  - a PDF attachment whose cover page clearly stated "Due By: 5/13/26"
    in the page text

`apply_default_if_missing` runs subject→body→default, then stops. The
buyer-attachment PDF text — already persisted to `rfq_files` at ingest
by PR-A — was never scanned.

This module adds the missing tier. `extract_deadline_from_pdf(path)`:
  - pdfplumber-extracts the text from every page
  - runs the SAME regex extractor used for body/subject
    (`requirement_extractor._extract_due_date` / `_extract_due_time`)
  - returns (date_str_iso, time_str) or (None, None)

It's intentionally a re-use of the existing regex rather than a new
Vision call:
  - pdfplumber is already a hard dep (used everywhere in this codebase)
  - The regex covers labeled patterns ("Due Date:", "Closing Date:",
    "Submit By:") AND verb-prefixed ("by 5/13/26", "due May 13 2026")
    which is what RFP cover pages overwhelmingly use.
  - No API call cost, no rate-limit risk, no async-ness to plumb.
  - Vision can be layered on top later for scanned-image PDFs (rare).
"""
from __future__ import annotations

import logging
import os
from typing import Iterable, Optional, Tuple

log = logging.getLogger(__name__)


def _pdf_text(pdf_path: str, max_pages: int = 5) -> str:
    """Extract concatenated text from the first `max_pages` of a PDF.

    Returns empty string on any failure. Caps pages because RFP
    deadlines are virtually always on the cover page or in the first
    "Schedule" section — a 60-page packet doesn't need a full scan,
    and capping bounds the cost on accidentally-huge attachments.
    """
    if not pdf_path or not os.path.exists(pdf_path):
        return ""
    try:
        import pdfplumber
    except Exception as e:
        log.debug("pdfplumber unavailable: %s", e)
        return ""
    out = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                if i >= max_pages:
                    break
                try:
                    t = page.extract_text() or ""
                    if t.strip():
                        out.append(t)
                except Exception:
                    continue
    except Exception as e:
        log.debug("pdfplumber open %s failed: %s", pdf_path, e)
        return ""
    return "\n".join(out)


def extract_deadline_from_pdf(
    pdf_path: str,
) -> Tuple[Optional[str], Optional[str]]:
    """Pull (due_date_iso, due_time) from a single PDF, or (None, None).

    `due_date_iso` is YYYY-MM-DD (matches `_extract_due_date`'s return
    shape). `due_time` is the matched time substring or empty.

    Caller is responsible for converting YYYY-MM-DD → MM/DD/YYYY if
    needed for the legacy display format.
    """
    text = _pdf_text(pdf_path)
    if not text:
        return None, None
    try:
        from src.agents.requirement_extractor import (
            _extract_due_date,
            _extract_due_time,
        )
    except Exception as e:
        log.debug("requirement_extractor unavailable: %s", e)
        return None, None
    try:
        date_iso = _extract_due_date(text)
        if not date_iso:
            return None, None
        time_str = _extract_due_time(text) or ""
        return date_iso, time_str
    except Exception as e:
        log.debug("attachment regex extract %s failed: %s", pdf_path, e)
        return None, None


def extract_deadline_from_paths(
    pdf_paths: Iterable[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Try a list of attachment paths in order, return the first hit.

    Returns (date_iso, time_str, source_filename) or (None, None, None).
    The source filename is included so the audit trail can pin WHICH
    attachment yielded the deadline.
    """
    for p in pdf_paths or []:
        if not p:
            continue
        try:
            date_iso, time_str = extract_deadline_from_pdf(p)
        except Exception as e:
            log.debug("extract loop %s failed: %s", p, e)
            continue
        if date_iso:
            return date_iso, time_str, os.path.basename(p)
    return None, None, None
