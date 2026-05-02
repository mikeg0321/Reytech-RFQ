"""Pre-flight double-signature detector for outbound packages.

Built 2026-05-01 (PR-B1). Mike's directive: hard-block Send when any form
has a double signature — the prior incident (cf. CLAUDE.md "Never
double-sign" rule + feedback_form_filling.md) had `_703b_overlay_signature`
running on PDFs that already had a `/Sig` AcroField, producing two stamped
signatures on the same line. Buyers reject those packages.

This scanner walks each generated PDF before draft creation:
  1. Counts `/Sig` AcroField widgets per page (Gmail-side rendering: each is
     a real signature box).
  2. Counts text overlays at "signature-typical" coordinates (lower 25% of
     page where Reytech's overlay-signature drawer lives) that look like a
     signature: cursive font, image stamp, or "Michael Guadan" string.
  3. Cross-references the two — if a page has both an /Sig field AND an
     overlay-drawn signature in the same lower-band, flags as DOUBLE_SIG.

Returns a list of issues. Empty list = clean.
"""
from __future__ import annotations

import logging
import os
from typing import List, Dict

log = logging.getLogger(__name__)

# Reytech canonical signer name — appears verbatim on legitimate signature
# overlays. Counting two within the same page band = the bug.
SIGNER_NAME = "Michael Guadan"

# A page is split into top/middle/bottom thirds. Signature stamps almost
# always live in the bottom third.
SIG_BAND_BOTTOM_PCT = 0.30


def scan_package_for_double_sigs(pdf_paths) -> List[Dict]:
    """Walk each PDF and return any double-sig findings.

    Args:
        pdf_paths: iterable of (form_id, absolute_path) tuples.

    Returns:
        List of issue dicts, one per detection:
          [{form_id, filename, page (1-indexed), kind, detail}]
        kind ∈ {"double_acroform_sig", "acroform_plus_overlay",
                "overlay_repeated", "overlay_pair_same_band"}.

    Design note: the scanner is intentionally lenient on false positives in
    the *positive direction* — we'd rather over-flag and have Mike click
    through than under-flag and let a double-sig PDF go to the buyer.
    """
    issues: List[Dict] = []

    try:
        import pdfplumber  # already a project dep
    except ImportError:
        log.warning("pdfplumber not available — double-sig scan skipped")
        return []

    for entry in pdf_paths:
        if isinstance(entry, (tuple, list)) and len(entry) >= 2:
            form_id, path = entry[0], entry[1]
        else:
            form_id, path = "?", entry
        if not path or not os.path.exists(path):
            continue
        try:
            issues.extend(_scan_one_pdf(form_id, path))
        except Exception as e:
            log.warning("double-sig scan error on %s: %s", path, e)

    return issues


def _scan_one_pdf(form_id, path):
    import pdfplumber
    found = []
    filename = os.path.basename(path)

    with pdfplumber.open(path) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            page_h = page.height or 1
            band_top_y = page_h * (1 - SIG_BAND_BOTTOM_PCT)

            # Count /Sig AcroForm widget annotations in the bottom band
            acro_sig_count = 0
            for ann in (page.annots or []):
                if not isinstance(ann, dict):
                    continue
                ann_type = (ann.get("data", {}).get("FT") or
                            ann.get("data", {}).get("Subtype") or "")
                # /Sig is a form-field type; widget annotation Subtype is
                # /Widget but the field type FT is /Sig
                ft = ann.get("data", {}).get("FT", "")
                if str(ft).strip("/").lower() == "sig":
                    # Check rect is in bottom band
                    rect = ann.get("rect") or [0, 0, 0, 0]
                    y_mid = (float(rect[1]) + float(rect[3])) / 2
                    if y_mid >= band_top_y:
                        acro_sig_count += 1

            # Count "Michael Guadan" name text overlays in the bottom band
            overlay_sig_count = 0
            try:
                # extract_words gives positioned tokens
                for w in (page.extract_words() or []):
                    if not (w.get("top") and w.get("text")):
                        continue
                    if w["top"] >= band_top_y - 5:
                        # Look for any token that's part of the signer name
                        if (w["text"].strip() == "Michael" or
                            w["text"].strip() == "Guadan"):
                            overlay_sig_count += 1
            except Exception as _e:
                log.debug("extract_words failed on %s pg %d: %s",
                          filename, page_idx, _e)

            # Two name-tokens (Michael + Guadan) = one signature; >2 indicates double
            overlay_sig_pairs = overlay_sig_count // 2

            # CASE A: two AcroForm sig boxes in the same band (rare — most
            # forms have one /Sig field). This is a template authoring bug.
            if acro_sig_count >= 2:
                found.append({
                    "form_id": form_id, "filename": filename,
                    "page": page_idx, "kind": "double_acroform_sig",
                    "detail": f"{acro_sig_count} /Sig AcroFields in lower band",
                })
                continue

            # CASE B: AcroForm sig + overlay-drawn sig on the same page
            # (the canonical "we double-signed it" bug)
            if acro_sig_count >= 1 and overlay_sig_pairs >= 1:
                found.append({
                    "form_id": form_id, "filename": filename,
                    "page": page_idx, "kind": "acroform_plus_overlay",
                    "detail": "/Sig field present AND overlay name drawn in lower band",
                })
                continue

            # CASE C: two overlay-drawn signatures (no AcroForm — both stamped)
            if overlay_sig_pairs >= 2:
                found.append({
                    "form_id": form_id, "filename": filename,
                    "page": page_idx, "kind": "overlay_pair_same_band",
                    "detail": f"{overlay_sig_pairs} '{SIGNER_NAME}' stamps in lower band",
                })

    return found
