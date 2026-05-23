"""PDF flattening — bake form fields into static page content.

When the Spine ships bytes to a buyer the package should be **final**:
the values the operator approved render as page content, with no
editable form fields left behind. That's what flattening does — and
it's the right tool for "prevent editing after approval" (Mike,
2026-05-22), because it needs no password, the recipient opens the
PDF normally, but there is nothing to edit.

Why flatten only at send / preview-of-send time
-----------------------------------------------
The Inspector + Chrome walkthrough gates read field VALUES out of the
rendered output to verify drift. A flattened PDF has zero fields, so
the verifier loses its surface. Flatten therefore lives at the SEND
boundary: ``/forms/*/pdf?flatten=1`` (operator's download for the
buyer-bound copy), and at the snapshot/send envelope (PR-6). The
default preview route stays editable so the Inspector can run.

Implementation
--------------
Uses PyMuPDF (``fitz.Document.bake``) — the only Python PDF library
in the dep stack that flattens widget annotations correctly across
rotated pages, complex /MK rotations, and the CCHCS forms' layout
quirks. ``pypdf.PdfWriter`` 6.x lacks a public ``flatten`` method
(`_flatten` is private + brittle on widget /AP regen); fitz's ``bake``
handles both annots and widgets in a single deterministic pass.
Verified 2026-05-22 on the 30-item 704B: 362 fields → 0 fields, every
value still visible.
"""
from __future__ import annotations

import io
import logging

log = logging.getLogger("reytech.spine.flatten")


def flatten_pdf_bytes(data: bytes) -> bytes:
    """Return a copy of ``data`` with all form widgets + annotations
    baked into static page content.

    ``data`` empty / corrupt / not a PDF → returns ``data`` unchanged
    (best-effort; never raises). On a healthy PDF the returned bytes
    have zero AcroForm fields and render identically to the input.
    """
    if not data or data[:5] != b"%PDF-":
        return data
    try:
        import fitz

        doc = fitz.open(stream=data, filetype="pdf")
        try:
            doc.bake(annots=True, widgets=True)
            buf = io.BytesIO()
            doc.save(buf)
            return buf.getvalue()
        finally:
            doc.close()
    except Exception as e:  # pragma: no cover - defensive
        log.warning("flatten: bake failed (%s) — returning input unchanged", e)
        return data


def flatten_pdf_file(input_path: str, output_path: str) -> None:
    """Flatten ``input_path`` and write the result to ``output_path``.

    Best-effort: a flatten failure copies the input through unchanged
    (never returns a partially-baked or empty file).
    """
    with open(input_path, "rb") as fh:
        data = fh.read()
    flat = flatten_pdf_bytes(data)
    with open(output_path, "wb") as fh:
        fh.write(flat)


__all__ = ["flatten_pdf_bytes", "flatten_pdf_file"]
