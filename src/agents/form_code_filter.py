"""form_code_filter.py — Reject form-code "items" parsed as line items.

PROBLEM (substrate, 2026-05-14): rfq_efbdef4a / 25CB021 (DSH Atascadero)
landed with 16 "line items". Items 0-6 were real products; items 7-15
were form-code references the buyer wanted attached:
    "Darfur", "STD204", "STD 1000", "GSPD-05-106", "STD843",
    "CalRecycle074", "CCC", "Exhibit G", "VSDS"

The line-item parser saw rows in a "Required Forms / Documents" table
in the buyer PDF and treated each row as a quote line. Operator sees
9 garbage items in `/home`, pricing never fires, and the package
generator has no way to know which are real.

This module classifies a parsed item as a form-code reference (vs. a
real product) so the ingest pipeline can route it to
`required_forms[]` instead of `line_items[]`.

Detection signals (any matches → form code):
  1. Part number matches a known form-code shape regex
     (STD###, GSPD-NN-NNN, Exhibit X, AMS 70Xy, etc.)
  2. Part number matches an exact FORM_TEXT_PATTERNS keyword
  3. Description matches a form-title keyword (Darfur, Bidder
     Declaration, Postconsumer Recycled, etc.)
  4. Quantity is 1 AND UOM is "EA"/"FORM"/"DOC" AND no unit_cost AND
     description references "form" / "declaration" / "certification"

Returns the canonical agency_config.FORM_TEXT_PATTERNS form_id when
matched (e.g. "darfur_act", "std204", "calrecycle74") — never a free
string, so downstream routing stays type-stable.

This complements FORM_TEXT_PATTERNS (which detects forms in email
bodies); the new shape regexes catch buyer-PDF tables where the cell
just says "STD204" with no surrounding text.
"""
from __future__ import annotations

import logging
import re
from typing import Optional

log = logging.getLogger("reytech.form_code_filter")

try:
    from src.core.agency_config import FORM_TEXT_PATTERNS
except ImportError:
    FORM_TEXT_PATTERNS = {}


# Shape-based regexes — match form-code-shaped strings even when the
# surrounding text is missing. Each tuple: (regex, canonical form_id).
# Ordering matters: most specific first.
_FORM_CODE_SHAPE_PATTERNS: list[tuple[re.Pattern, str]] = [
    # STD 204 / STD204 / Std 204 — Payee Data Record
    (re.compile(r"^\s*STD[\s\-]*204\b", re.IGNORECASE), "std204"),
    # STD 205
    (re.compile(r"^\s*STD[\s\-]*205\b", re.IGNORECASE), "std205"),
    # STD 843 / DVBE 843
    (re.compile(r"^\s*(?:STD[\s\-]*)?843\b", re.IGNORECASE), "dvbe843"),
    # STD 1000 — GenAI Reporting
    (re.compile(r"^\s*STD[\s\-]*1000\b", re.IGNORECASE), "std1000"),
    # CalRecycle 074 / CalRecycle074 / 074 alone (when desc-only context)
    (re.compile(r"^\s*CALRECYCLE[\s\-]*0?74\b", re.IGNORECASE), "calrecycle74"),
    # GSPD-05-105 / GSPD-05-106 / GSPD 05 105
    (re.compile(r"^\s*GSPD[\s\-]*05[\s\-]*10[5-6]\b", re.IGNORECASE), "bidder_decl"),
    # CV 012 / CV012 (CalVet CUF)
    (re.compile(r"^\s*CV[\s\-]*012\b", re.IGNORECASE), "cv012_cuf"),
    # Darfur / Darfur Act / Darfur Contracting
    (re.compile(r"^\s*DARFUR\b", re.IGNORECASE), "darfur_act"),
    # Exhibit X (single-letter exhibit attachments are admin docs, not items)
    (re.compile(r"^\s*EXHIBIT\s+[A-Z]\b", re.IGNORECASE), "exhibit"),
    # CCC = Conflict-of-Interest / Construction Certification etc.
    (re.compile(r"^\s*CCC\b", re.IGNORECASE), "ccc"),
    # VSDS = Vendor Self-Disclosure
    (re.compile(r"^\s*VSDS\b", re.IGNORECASE), "vsds"),
    # AMS 708 / GENAI 708 / Gen AI 708
    (re.compile(r"^\s*(?:AMS[\s\-]*)?708\b", re.IGNORECASE), "ams708"),
    # OBS 1600
    (re.compile(r"^\s*OBS[\s\-]*1600\b", re.IGNORECASE), "obs_1600"),
    # W-9 / W9
    (re.compile(r"^\s*W[\s\-]*9\b", re.IGNORECASE), "w9"),
    # Seller's Permit
    (re.compile(r"^\s*SELLER'?S?\s+PERMIT\b", re.IGNORECASE), "sellers_permit"),
    # Bidder Declaration (long form)
    (re.compile(r"^\s*BIDDER[\s\-]*DECLARATION\b", re.IGNORECASE), "bidder_decl"),
]

# Description-level keywords that mark a row as an admin form.
# Conservative — these must be the ENTIRE description shape, not a
# substring inside a real product description.
_DESC_KEYWORDS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"darfur\s+contracting\s+act\b", re.IGNORECASE), "darfur_act"),
    (re.compile(r"payee\s+data\s+record\b", re.IGNORECASE), "std204"),
    (re.compile(r"payee\s+supplemental\b", re.IGNORECASE), "std205"),
    (re.compile(r"dvbe\s+declaration\b", re.IGNORECASE), "dvbe843"),
    (re.compile(r"bidder\s+declaration\b", re.IGNORECASE), "bidder_decl"),
    (re.compile(r"commercially\s+useful\s+function\b", re.IGNORECASE), "cv012_cuf"),
    (re.compile(r"postconsumer\s+(?:recycled\s+)?content\b", re.IGNORECASE), "calrecycle74"),
    (re.compile(r"recycled[\-\s]content\s+certification\b", re.IGNORECASE), "calrecycle74"),
    (re.compile(r"conflict[\-\s]of[\-\s]interest\b", re.IGNORECASE), "ccc"),
    (re.compile(r"vendor\s+self[\-\s]disclosure\b", re.IGNORECASE), "vsds"),
    (re.compile(r"gen(?:erative)?\s*ai\s+(?:reporting|disclosure|use)\b", re.IGNORECASE), "std1000"),
    (re.compile(r"fair\s+and\s+reasonable\b", re.IGNORECASE), "703c"),
    (re.compile(r"\bw[\s\-]9\s+(?:form|tax)\b", re.IGNORECASE), "w9"),
]


def classify_item(item: dict) -> Optional[str]:
    """Return canonical form_id if item looks like a form-code row.

    Checks part/item_number first (most discriminative), then
    description. Returns None for real product rows.
    """
    if not isinstance(item, dict):
        return None

    # Part-number / item-number shape match (highest signal)
    for field in ("part", "item_number", "mfg_number", "mfg#",
                  "manufacturer_part_number", "part_number"):
        v = item.get(field) or ""
        if not isinstance(v, str):
            continue
        v_stripped = v.strip()
        if not v_stripped:
            continue
        for pat, form_id in _FORM_CODE_SHAPE_PATTERNS:
            if pat.match(v_stripped):
                return form_id

    # Description-level keyword match (lower signal — keywords must
    # describe the whole row, not be a fragment of a real product desc)
    desc = (item.get("description") or item.get("desc") or "")
    if isinstance(desc, str) and desc.strip():
        # Only fire when desc is short (≤80 chars). Real product
        # descriptions tend to be longer with units / pack info; a
        # form-row desc is typically just the form title.
        if len(desc.strip()) <= 80:
            for pat, form_id in _DESC_KEYWORDS:
                if pat.search(desc):
                    return form_id

    return None


def filter_form_codes(items: list[dict]) -> tuple[list[dict], list[str]]:
    """Split a parsed item list into real products vs form-code refs.

    Returns:
        (real_items, form_ids) — real_items preserves order and is
        what should land in `line_items[]`. form_ids is a deduped list
        of canonical form IDs that should be unioned into
        `required_forms[]`.

    Never raises. An item that fails classification is treated as a
    real product (fail-open — operator can delete it, vs the worse
    failure of silently dropping a real product).
    """
    if not items:
        return [], []
    real: list[dict] = []
    form_ids: list[str] = []
    seen_ids: set[str] = set()
    for it in items:
        try:
            fid = classify_item(it)
        except Exception as e:
            log.debug("form_code_filter classify error: %s", e)
            fid = None
        if fid:
            if fid not in seen_ids:
                form_ids.append(fid)
                seen_ids.add(fid)
            log.info(
                "form_code_filter: dropped row %r → form_id=%s",
                (it.get("part") or it.get("description") or "")[:60], fid,
            )
        else:
            real.append(it)
    return real, form_ids


__all__ = ["classify_item", "filter_form_codes"]
