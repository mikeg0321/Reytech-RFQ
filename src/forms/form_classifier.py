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


def _collect_classifier_hints() -> list[dict]:
    """Build the ordered list of classifier hints from profile YAMLs.

    Each entry is {slot, priority, field_prefixes, field_contains} taken
    directly from the profile's `classifier_hints` block. Sorted by
    priority descending, then by profile id for stable ordering.
    """
    try:
        from src.forms.profile_registry import load_profiles
        profiles = load_profiles()
    except Exception as e:
        log.debug("classifier: load_profiles failed: %s", e)
        return []

    hints: list[dict] = []
    for pid, p in profiles.items():
        raw = getattr(p, "raw_yaml", {}) or {}
        block = raw.get("classifier_hints")
        if not isinstance(block, dict):
            continue
        slot = block.get("slot")
        if not slot:
            continue
        hints.append({
            "profile_id": pid,
            "slot": slot,
            "priority": int(block.get("priority", 0)),
            "field_prefixes": list(block.get("field_prefixes", []) or []),
            "field_contains": list(block.get("field_contains", []) or []),
        })
    hints.sort(key=lambda h: (-h["priority"], h["profile_id"]))
    return hints


def _fingerprint_pdf(data: bytes) -> str | None:
    """Inspect AcroForm field names to guess form type.

    Consults profile YAML classifier_hints first (data-driven), then falls
    back to hardcoded checks for slots with no profile yet (703c).
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

    joined = "\n".join(names)
    for hint in _collect_classifier_hints():
        for pfx in hint["field_prefixes"]:
            if any(n.startswith(pfx) for n in names):
                return hint["slot"]
        for sub in hint["field_contains"]:
            if sub in joined:
                return hint["slot"]

    # Hardcoded fallback: 703c has no profile yet (buyer-supplied variant).
    if any(n.startswith("703C_") for n in names):
        return "703c"
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
