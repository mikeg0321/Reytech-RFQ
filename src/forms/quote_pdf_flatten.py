"""Flatten an editable AcroForm quote PDF to a locked flat PDF.

Used at flatten-on-send time (PR-D3 of the Editable Quote PDF arc,
2026-05-03). The operator's working copy carries AcroForm fields that
they may have edited in Adobe Reader / Preview. Before the buyer copy
ships:

  1. Read the operator's AcroForm field values via
     `quote_pdf_edits.read_quote_pdf_edits`.
  2. Flatten the PDF — burn each field's value into the page content
     as static text and remove the /AcroForm dict — so the buyer
     receives a non-editable copy.
  3. Caller takes the dict + flat bytes and decides what to do with
     them (push to Quote DB, replace stored attachment, audit-log,
     email).

Pure transformation. No DB I/O, no record mutation, no network.
Caller (a route) owns those side effects.
"""
from __future__ import annotations

import io
import logging
from typing import Any, Dict, Tuple

log = logging.getLogger("reytech.quote_pdf_flatten")


def flatten_quote_pdf(
    editable_pdf_bytes: bytes,
) -> Tuple[bytes, Dict[str, str]]:
    """Take editable AcroForm PDF bytes and return (flat_bytes, edits_dict).

    Args:
      editable_pdf_bytes: bytes of a PDF that may carry AcroForm fields.
        If no /AcroForm dict exists, the bytes are returned unchanged
        and edits_dict is empty.

    Returns:
      (flat_bytes, edits_dict)
        flat_bytes — PDF bytes with /AcroForm removed and field values
                     burned into the page content via pypdf's flatten
                     mechanism. If flatten fails the original bytes are
                     returned (best-effort, F1 from locked scope).
        edits_dict — same shape as quote_pdf_edits.read_quote_pdf_edits.

    Raises: never. Returns (input_bytes, {}) on any internal failure
    so the caller's send flow is never blocked by a flatten error.
    """
    if not editable_pdf_bytes:
        return b"", {}

    # Step 1: read field values BEFORE flattening (otherwise flatten
    # may strip the /V values pypdf reads from).
    try:
        from src.forms.quote_pdf_edits import read_quote_pdf_edits
        edits = read_quote_pdf_edits(editable_pdf_bytes)
    except Exception as e:
        log.warning("flatten: read_quote_pdf_edits failed: %s", e)
        edits = {}

    # Step 2: flatten via pypdf. There are two ways to remove an
    # AcroForm: (a) call writer.flatten() (newer pypdf) which burns
    # field values into page content, or (b) drop the /AcroForm key
    # from the catalog (cheap but the field values vanish from the
    # rendered output). We do (a) when available, (b) as fallback.
    try:
        from pypdf import PdfReader, PdfWriter
        reader = PdfReader(io.BytesIO(editable_pdf_bytes))
        writer = PdfWriter()
        # Clone so we operate on writer-owned objects.
        writer.clone_document_from_reader(reader)

        flattened_via_writer = False
        # pypdf >= 4 has PdfWriter.flatten(); older releases don't.
        if hasattr(writer, "flatten"):
            try:
                writer.flatten()
                flattened_via_writer = True
            except Exception as fe:
                log.debug("writer.flatten() raised, will drop /AcroForm: %s", fe)

        if not flattened_via_writer:
            # Fallback: nuke /AcroForm so Adobe Reader stops showing
            # the form widgets. Field values that are already burned
            # into /V remain; the appearance stream may be stripped
            # depending on viewer. Buyer shouldn't see the empty
            # widgets in modern viewers because /AcroForm is gone.
            try:
                root = writer._root_object
                if "/AcroForm" in root:
                    del root["/AcroForm"]
            except Exception as ke:
                log.warning("flatten fallback /AcroForm-strip failed: %s", ke)

        out = io.BytesIO()
        writer.write(out)
        return out.getvalue(), edits
    except Exception as e:
        log.warning("flatten_quote_pdf: full flatten failed, returning original: %s", e)
        return editable_pdf_bytes, edits


def diff_to_quote_fields(edits: Dict[str, str]) -> Dict[str, Any]:
    """Map AcroForm field names → Quote DB column updates.

    PR-D1 emits these field names; this helper translates them to the
    canonical Quote DB row updates so PR-D3 callers can push edits back
    via a single dict.

    Returns a dict with up to these keys:
      ship_to_name        ← ship_name field
      ship_to_address     ← list of [ship_addr_1, ship_addr_2, …]
      bill_to_name        ← bill_name field
      bill_to_address     ← list of [bill_addr_1, bill_addr_2, …]
      contact_name        ← to_name field (the "To:" person)
      contact_address     ← list of [to_addr_1, to_addr_2, …]

    Empty fields are dropped — only non-empty edits land in the result.
    Caller (route) decides whether to apply any/all of these to the row.
    """
    out: Dict[str, Any] = {}
    if not edits:
        return out

    def _collect_addr(prefix: str) -> list:
        lines = []
        i = 1
        # Cap at 12 lines so a corrupted field name pattern can't loop.
        while i <= 12:
            v = (edits.get(f"{prefix}_{i}") or "").strip()
            if not v:
                break
            lines.append(v)
            i += 1
        return lines

    if (v := edits.get("ship_name", "").strip()):
        out["ship_to_name"] = v
    ship_addr = _collect_addr("ship_addr")
    if ship_addr:
        out["ship_to_address"] = ship_addr

    if (v := edits.get("bill_name", "").strip()):
        out["bill_to_name"] = v
    bill_addr = _collect_addr("bill_addr")
    if bill_addr:
        out["bill_to_address"] = bill_addr

    if (v := edits.get("to_name", "").strip()):
        out["contact_name"] = v
    to_addr = _collect_addr("to_addr")
    if to_addr:
        out["contact_address"] = to_addr

    return out


__all__ = [
    "flatten_quote_pdf",
    "diff_to_quote_fields",
]
