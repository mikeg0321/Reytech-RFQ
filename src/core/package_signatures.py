"""Bundle — Audit BB: package-signature audit helper.

Mike 2026-04-23 reviewing the generated RFQ 10840486 package:

    "missing some signatures and in some places has double signatures."

### Why a helper
The fix direction from the audit memo explicitly says: **do NOT
patch signature placement heuristics without a regression fixture
proving the fix + preserving good pages.** That 3-strikes guardrail
came out of the 2026-04-03 multi-page 704 incident where 11
consecutive commits patched symptoms instead of diagnosing a shared
root cause.

This module is that regression-fixture infrastructure. It counts
signature evidence per page — AcroForm `/Sig` fields, signature
widget annotations, image XObjects likely to be pasted sig scans,
and text markers — and emits a page-indexed dict so any caller
can compare a generated package against the canonical north-star
PDF at `tests/fixtures/rfq_packages/10840486_rfq_package_NORTHSTAR.pdf`.

### Contract
`audit_package_signatures(path) -> Dict[int, Dict[str, Any]]`

Returns `{page_num_1based: {acroform_sigs, widget_sigs, image_xobjects,
text_markers, has_any_signature}}`. Callers (test + script) decide
what "match" means for their use case.

### What "has_any_signature" means
True if ANY of:
- AcroForm `/Sig` field on this page
- `/Widget` annotation with field type `/Sig`
- Image XObject embedded on the page (Reytech pastes pre-rendered
  sig images onto forms that don't expose `/Sig` fields)

This is a permissive heuristic intended to catch all three common
signature representations. False positives on image-heavy pages are
possible — callers should cross-reference with the text markers.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

log = logging.getLogger("reytech.package_signatures")


_TEXT_SIGNATURE_MARKERS: tuple = (
    r"signature",
    r"signed\s+by",
    r"/s/",
    r"Michael\s+Guadan",
    r"Reytech\s+Inc",
    r"executed\s+by",
    r"date\s+signed",
)


def _page_text_markers(page) -> List[str]:
    """Extract signature-related phrases from a page's text stream.
    Best-effort — some PDFs have non-extractable text, in which case
    the caller should fall back to annotation / image detection."""
    try:
        txt = page.extract_text() or ""
    except Exception as e:
        log.debug("extract_text crashed: %s", e)
        return []
    hits = []
    for pat in _TEXT_SIGNATURE_MARKERS:
        if re.search(pat, txt, re.IGNORECASE):
            hits.append(pat)
    return hits


def _page_acroform_sig_count(page) -> int:
    """Count AcroForm signature fields (FT=/Sig) annotated on this page."""
    try:
        annots = page.get("/Annots")
        if annots is None:
            return 0
        # /Annots can be an indirect ref; resolve
        try:
            items = annots.get_object() if hasattr(annots, "get_object") else annots
        except Exception:
            items = annots
        if items is None:
            return 0
        n = 0
        for ref in items:
            try:
                obj = ref.get_object() if hasattr(ref, "get_object") else ref
            except Exception:
                continue
            if not obj:
                continue
            ft = obj.get("/FT")
            # /Subtype /Widget + /FT /Sig = AcroForm signature
            if str(ft) == "/Sig":
                n += 1
        return n
    except Exception as e:
        log.debug("acroform-sig-count crashed: %s", e)
        return 0


def _page_widget_sig_count(page) -> int:
    """Count `/Widget` annotations on this page — widgets are the
    visual representation of form fields; a sig field renders as a
    widget. Useful when the field type is declared at the form root
    rather than per-annotation."""
    try:
        annots = page.get("/Annots")
        if annots is None:
            return 0
        try:
            items = annots.get_object() if hasattr(annots, "get_object") else annots
        except Exception:
            items = annots
        if items is None:
            return 0
        n = 0
        for ref in items:
            try:
                obj = ref.get_object() if hasattr(ref, "get_object") else ref
            except Exception:
                continue
            if not obj:
                continue
            subtype = obj.get("/Subtype")
            ft = obj.get("/FT")
            if str(subtype) == "/Widget" and str(ft) == "/Sig":
                n += 1
        return n
    except Exception as e:
        log.debug("widget-sig-count crashed: %s", e)
        return 0


def _page_image_xobject_count(page) -> int:
    """Count image XObjects on this page. Reytech's signature path
    for forms that don't expose `/Sig` fields is to paste a
    pre-rendered PNG onto the signature line (via reportlab overlay).
    Image count is a proxy — can be noisy on pages with logos /
    icons, but a necessary signal because AcroForm + Widget counts
    miss image-based sigs."""
    try:
        res = page.get("/Resources")
        if res is None:
            return 0
        try:
            res_obj = res.get_object() if hasattr(res, "get_object") else res
        except Exception:
            res_obj = res
        xobj = res_obj.get("/XObject") if res_obj else None
        if xobj is None:
            return 0
        try:
            xobj_obj = xobj.get_object() if hasattr(xobj, "get_object") else xobj
        except Exception:
            xobj_obj = xobj
        n = 0
        for name, ref in xobj_obj.items():
            try:
                obj = ref.get_object() if hasattr(ref, "get_object") else ref
            except Exception:
                continue
            if not obj:
                continue
            if str(obj.get("/Subtype")) == "/Image":
                n += 1
        return n
    except Exception as e:
        log.debug("image-xobject-count crashed: %s", e)
        return 0


def audit_package_signatures(pdf_path: str) -> Dict[int, Dict[str, Any]]:
    """Return a page-indexed signature audit of a bid package PDF.

    Dict shape per page:
      {
        "acroform_sigs":  int,   # /Sig AcroForm fields on page
        "widget_sigs":    int,   # /Widget /FT=/Sig annots on page
        "image_xobjects": int,   # images on page (sig-image proxy)
        "text_markers":   [str], # regex patterns that hit
        "has_any_signature": bool,
      }

    Safe on any input — returns an empty dict on unreadable PDFs.
    """
    try:
        from pypdf import PdfReader
    except Exception as e:
        log.error("pypdf unavailable: %s", e)
        return {}
    try:
        reader = PdfReader(pdf_path)
    except Exception as e:
        log.error("PdfReader(%r) crashed: %s", pdf_path, e)
        return {}

    out: Dict[int, Dict[str, Any]] = {}
    for idx, page in enumerate(reader.pages, start=1):
        acroform = _page_acroform_sig_count(page)
        widget = _page_widget_sig_count(page)
        images = _page_image_xobject_count(page)
        markers = _page_text_markers(page)
        out[idx] = {
            "acroform_sigs": acroform,
            "widget_sigs": widget,
            "image_xobjects": images,
            "text_markers": markers,
            "has_any_signature": bool(
                acroform or widget or (images > 0 and markers)
            ),
        }
    return out


def compare_to_northstar(
    generated_path: str,
    northstar_path: str,
) -> Dict[str, Any]:
    """Diff a generated package vs the canonical north star.

    Returns:
      {
        "page_count_gen": int,
        "page_count_ns":  int,
        "matches": bool,                 # page count matches + per-page sig match
        "per_page": [
          {
            "page": int,
            "expected_sig": bool,        # north star has_any_signature
            "actual_sig": bool,          # generated has_any_signature
            "match": bool,
            "ns_counts":  {...},         # full north-star audit dict
            "gen_counts": {...},         # full generated audit dict
          },
          ...
        ],
        "missing_on": [int],             # page numbers where NS has sig but gen doesn't
        "extra_on":   [int],             # page numbers where gen has sig but NS doesn't
      }
    """
    ns = audit_package_signatures(northstar_path)
    gen = audit_package_signatures(generated_path)
    pages = sorted(set(ns.keys()) | set(gen.keys()))
    per_page = []
    missing: List[int] = []
    extra: List[int] = []
    for p in pages:
        exp = bool(ns.get(p, {}).get("has_any_signature"))
        act = bool(gen.get(p, {}).get("has_any_signature"))
        per_page.append({
            "page": p,
            "expected_sig": exp,
            "actual_sig": act,
            "match": exp == act,
            "ns_counts": ns.get(p, {}),
            "gen_counts": gen.get(p, {}),
        })
        if exp and not act:
            missing.append(p)
        elif act and not exp:
            extra.append(p)
    return {
        "page_count_gen": len(gen),
        "page_count_ns": len(ns),
        "matches": (
            len(gen) == len(ns) and not missing and not extra
        ),
        "per_page": per_page,
        "missing_on": missing,
        "extra_on": extra,
    }
