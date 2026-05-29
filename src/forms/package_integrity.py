"""Canonical package-integrity gate — ONE detector for EVERY generated package.

Mike (Architect) 2026-05-29, after the CCHCS 10842771 stress package shipped with
CalRecycle 74 and the seller's permit DUPLICATED (the bid package already contained
them, AND the assembler emitted standalone copies — the dedup guard existed only for
DVBE 843). Nothing in the QA layer looked at the *assembled* package for repeated
forms or empty bidder info, so it shipped.

This module is that missing gate. It is intentionally generation-agnostic: it takes a
finished PDF and judges it, so the SAME check guards every form type and every surface
— RFQ packages, PC packages, and any future agency packet. "One canonical output"
(§0 LAW 1) needs one canonical integrity check.

Two checks:
  - detect_duplicate_forms(pdf): the same form page appearing more than once
    (CalRecycle ×2, seller's permit ×2). Blank separator pages are ignored.
  - find_blank_bidder_info(pdf, company_name): the bidder/company identity never
    landed on the response forms (the 703A "Business Name: ___" blank-slot bug).

Both return structured findings; callers decide block vs warn.
"""
from __future__ import annotations

import hashlib
import logging
import re

log = logging.getLogger("reytech.package_integrity")

# Pages with fewer than this many non-whitespace chars are treated as blank
# separators / overflow pages and excluded from duplicate detection (otherwise
# two blank pages would false-flag as a "duplicate form").
_BLANK_PAGE_MAX_CHARS = 40
# A page's identity signature is its FULL normalized text. Using only a prefix
# (the original 600-char attempt) false-positived multi-page forms whose pages
# share a long boilerplate header (CalRecycle 74's 4 pages all begin "STATE OF
# CALIFORNIA To be completed by the State agency..." — flagged as duplicates of
# each other). Full text distinguishes the pages of one form while still matching
# two byte-identical COPIES of the same form (the real duplication). Verified on
# the 10842771 regen: full-hash finds only the genuine seller's-permit dup.


def _norm(text: str) -> str:
    return re.sub(r"\s+", "", (text or "")).lower()


def _page_signature(text: str) -> str:
    return hashlib.md5(_norm(text).encode("utf-8", "ignore")).hexdigest()


def _extract_pages(pdf_path: str) -> list[str]:
    """Return per-page text. Empty list if the PDF can't be read (caller treats
    an unreadable package as its own problem, not a false 'clean')."""
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as p:
            return [(pg.extract_text() or "") for pg in p.pages]
    except Exception as e:
        log.warning("package_integrity: cannot read %s: %s", pdf_path, e)
        return []


def detect_duplicate_forms(pdf_path: str, pages: list[str] | None = None) -> list[dict]:
    """Find form pages that appear more than once in the assembled package.

    Returns a list of {"pages": [int...], "snippet": str} — one entry per repeated
    page signature. Empty list = no duplicate forms. Blank/separator pages are
    ignored. A multi-page form duplicated as a block surfaces as several entries
    (one per page), which is fine — any non-empty result means BLOCK.

    `pages` (optional): precomputed per-page text from a single _extract_pages()
    call by the caller, so check_package doesn't parse the (large) package twice.
    None => extract here.
    """
    if pages is None:
        pages = _extract_pages(pdf_path)
    sigs: dict[str, dict] = {}
    for i, text in enumerate(pages):
        if len(_norm(text)) < _BLANK_PAGE_MAX_CHARS:
            continue
        sig = _page_signature(text)
        slot = sigs.setdefault(sig, {"pages": [], "snippet": re.sub(r"\s+", " ", text)[:60]})
        slot["pages"].append(i + 1)
    return [
        {"pages": slot["pages"], "snippet": slot["snippet"]}
        for slot in sigs.values()
        if len(slot["pages"]) > 1
    ]


def find_blank_bidder_info(pdf_path: str, company_name: str, pages: list[str] | None = None) -> dict:
    """Assert the bidder/company identity actually landed on the response forms.

    The 703A/703B etc. carry a BIDDER INFORMATION block (Business Name / Address /
    FEIN / Seller's Permit). The 10842771 package shipped that block BLANK. The
    cheapest reliable signal that bidder info filled is: the company name appears
    on the package at least once (space-tolerant, so gap-rendered 'R E Y T E C H'
    still counts as present rather than a false alarm).

    Returns {"present": bool, "company_name": str}. present=False ⇒ bidder info
    never rendered ⇒ caller should BLOCK.
    """
    name_norm = _norm(company_name)
    if not name_norm:
        return {"present": True, "company_name": company_name}  # nothing to assert
    if pages is None:
        pages = _extract_pages(pdf_path)
    for text in pages:
        if name_norm in _norm(text):
            return {"present": True, "company_name": company_name}
    return {"present": False, "company_name": company_name}


def check_package(pdf_path: str, company_name: str = "") -> dict:
    """Run all integrity checks on a finished package. Canonical entry point for
    every gate (RFQ + PC).

    Returns {"ok": bool, "blockers": [str], "duplicate_forms": [...],
             "bidder_info_present": bool}. ok=False ⇒ do not ship.
    """
    # Parse the package's per-page text ONCE and feed both detectors — they
    # previously each called _extract_pages independently, parsing the (often
    # ~9 MB merged) package twice per gate run.
    pages = _extract_pages(pdf_path)
    dups = detect_duplicate_forms(pdf_path, pages=pages)
    blockers: list[str] = []
    for d in dups:
        blockers.append(
            f"Duplicate form: pages {d['pages']} are the same form "
            f"({d['snippet']!r}) — a form is included both standalone and inside "
            f"the bid package. Emit it once."
        )
    bidder_present = True
    if company_name:
        info = find_blank_bidder_info(pdf_path, company_name, pages=pages)
        bidder_present = info["present"]
        if not bidder_present:
            blockers.append(
                f"Bidder info missing: company name {company_name!r} appears nowhere "
                f"in the package — the BIDDER INFORMATION block did not fill."
            )
    return {
        "ok": not blockers,
        "blockers": blockers,
        "duplicate_forms": dups,
        "bidder_info_present": bidder_present,
    }
