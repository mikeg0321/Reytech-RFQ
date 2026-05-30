"""AMS 708 — Generative AI Use Disclosure (standalone filler).

2026-05-12: rfq_0ebe242f (CCWF Ashley) email contract told us
explicitly that CCHCS/CDCR is moving from STD 1000 → AMS 708 for the
GenAI disclosure form. The 708 form fields already exist *inside* the
CDCR bid-package template (`data/templates/cdcr_bid_package_template.pdf`,
pages carrying the `708_*` AcroForm widgets) and are filled by
`fill_bid_package` — so whenever a package includes the bid package,
the 708 is already delivered inside it.

A STANDALONE AMS 708 file is only needed when the 708 is required
WITHOUT the bid package (buyer wants the forms as separate
attachments). There is no separate single-form AMS 708 blank with an
intact AcroForm — the `708_*` widgets only survive inside the full
bid-package template. So we DERIVE the standalone form from that one
source of truth: fill the full template (reusing `fill_genai_708`,
the canonical field map), then keep only the page(s) that carry the
`708_*` widgets. `PdfWriter.append()` + page removal preserves the
AcroForm /Fields for the retained pages (unlike `add_page`, which
drops them — that was the pitfall noted in test_genai_708_fill).
"""
from __future__ import annotations

import logging
import os
import tempfile

log = logging.getLogger("reytech.fill_ams708")


def _bidpkg_template_path() -> str:
    """The CDCR bid-package template — the single source of the 708 form."""
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "..",
        "data", "templates", "cdcr_bid_package_template.pdf",
    )


def ams708_template_available() -> bool:
    """Cheap presence check — is the source template on disk?"""
    try:
        return os.path.exists(_bidpkg_template_path())
    except Exception:
        return False


def _detect_708_pages(reader) -> list[int]:
    """Return 0-based indices of pages carrying any `708_*` AcroForm field.

    Detected dynamically rather than hardcoded so a template revision
    that shifts the 708 pages doesn't silently emit the wrong pages.
    """
    pages: set[int] = set()
    for i, page in enumerate(reader.pages):
        for annot in (page.get("/Annots") or []):
            try:
                name = annot.get_object().get("/T", "")
                if name and "708" in str(name):
                    pages.add(i)
            except Exception:
                continue
    return sorted(pages)


def fill_ams708_standalone(rfq_data: dict, config: dict, output_path: str) -> bool:
    """Fill a standalone AMS 708 PDF for the given RFQ + Reytech config.

    Derives the form from the CDCR bid-package template: fills the full
    template via the canonical `fill_genai_708` field map, then keeps
    only the 708 page(s). Returns True iff the file was written.

    Never raises — returns False (logged) when the source template is
    absent or the fill fails, so the package generator can surface the
    gap in the operator's errors list rather than crash.
    """
    from pypdf import PdfReader, PdfWriter

    tmpl = _bidpkg_template_path()
    if not os.path.exists(tmpl):
        log.warning(
            "fill_ams708_standalone: source template not present at %s — "
            "AMS 708 cannot be rendered standalone.", tmpl,
        )
        return False

    try:
        from src.forms.reytech_filler_v4 import fill_genai_708

        # 1. Fill the 708_* fields on a copy of the full bid-package template.
        with tempfile.TemporaryDirectory() as td:
            full = os.path.join(td, "full_filled.pdf")
            fill_genai_708(tmpl, rfq_data, config, full)

            # 2. Keep only the page(s) carrying 708_* widgets. append()+remove
            #    preserves the AcroForm /Fields for retained pages.
            reader = PdfReader(full)
            keep = set(_detect_708_pages(reader))
            if not keep:
                log.warning(
                    "fill_ams708_standalone: no 708_* fields found in %s — "
                    "template may have changed.", os.path.basename(tmpl),
                )
                return False

            writer = PdfWriter()
            writer.append(reader)
            for idx in range(len(writer.pages) - 1, -1, -1):
                if idx not in keep:
                    del writer.pages[idx]

            # Keep field appearances rendering after page removal.
            try:
                writer.set_need_appearances_writer(True)
            except Exception:
                pass

            with open(output_path, "wb") as fh:
                writer.write(fh)

        log.info(
            "AMS 708 standalone derived from bid-package template → %s "
            "(pages kept: %s)", output_path, sorted(keep),
        )
        return True
    except Exception as e:
        log.exception("fill_ams708_standalone failed: %s", e)
        return False
