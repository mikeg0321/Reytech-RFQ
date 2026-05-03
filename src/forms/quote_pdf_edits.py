"""Read AcroForm field values back from an editable quote PDF.

Companion to PR-D1's editable quote generator. The operator generates
an editable working copy, edits buyer/ship-to fields in Adobe Reader /
Preview / Chrome's built-in PDF viewer, then sends. PR-D3 will call
this helper at flatten-on-send time to push the operator's edits back
to the Quote DB row before the buyer copy is flattened and emailed.

Pure read — no DB writes, no record mutation, no PDF mutation. The
caller decides what to do with the returned dict.

Field names that PR-D1 emits (must stay in lockstep here):
  bill_name, bill_addr_<N>          — Bill To block
  to_name, to_addr_<N>               — To: block (left column)
  ship_name, ship_addr_<N>           — Ship To Location block

Returned shape (all string values, AcroForm is text-only by design):
  {
    "bill_name": "CCHCS Accounting",
    "bill_addr_1": "PO Box 588500",
    "to_name": "CSP Sacramento - New Folsom",
    "ship_name": "CSP Sacramento - New Folsom",
    "ship_addr_1": "100 Prison Road",
    "ship_addr_2": "Represa, CA 95671",
    ...
  }

Empty dict when:
  • PDF has no /AcroForm dict (flat PDF, never been editable)
  • PDF has /AcroForm but no fields that match the known prefixes
  • pypdf can't read the PDF (corrupt, encrypted, etc) — never raises

Multi-page-stable: AcroForm fields are referenced by name regardless
of which page they render on. A 3-page quote with `ship_addr_1` on
page 1 reads the same as a 1-page quote.
"""
from __future__ import annotations

import io
import logging
from typing import Any, Dict, Union

log = logging.getLogger("reytech.quote_pdf_edits")


# Known prefixes from PR-D1's AcroForm emission — keep in sync.
_KNOWN_PREFIXES = (
    "bill_name", "bill_addr_",
    "to_name", "to_addr_",
    "ship_name", "ship_addr_",
)


def _is_known_field(name: str) -> bool:
    """Whitelist filter — accept only fields PR-D1 emits, ignore strays."""
    if not name:
        return False
    if name in _KNOWN_PREFIXES:
        return True
    return any(name.startswith(p) for p in _KNOWN_PREFIXES if p.endswith("_"))


def _coerce_value(raw: Any) -> str:
    """Coerce a pypdf field value (which can be str, bytes, IndirectObject,
    or something with .__str__) to a clean string. Strips None / empty."""
    if raw is None:
        return ""
    s = str(raw)
    # pypdf occasionally wraps values in PDF object reprs like "<TextStringObject 'x'>"
    # — when seen, fall through to str() which yields the inner text. The .strip()
    # call also strips PDF byte order marks if any.
    return s.strip()


def read_quote_pdf_edits(pdf_bytes: Union[bytes, bytearray, io.IOBase]) -> Dict[str, str]:
    """Return a {field_name: value} dict of AcroForm edits in the PDF.

    Args:
      pdf_bytes: raw bytes of the PDF, OR a file-like object pypdf can read.

    Returns:
      Dict mapping field_name -> string value. Empty dict on any failure
      (helper is best-effort and never raises).

    Only fields whose names match PR-D1's emit list are included. Stray
    /AcroForm fields from third-party tooling are ignored.
    """
    try:
        from pypdf import PdfReader
    except Exception as e:
        log.error("pypdf not importable: %s", e)
        return {}

    try:
        if isinstance(pdf_bytes, (bytes, bytearray)):
            stream = io.BytesIO(pdf_bytes)
        else:
            stream = pdf_bytes
        reader = PdfReader(stream)
    except Exception as e:
        log.warning("read_quote_pdf_edits: PdfReader failed: %s", e)
        return {}

    try:
        # get_fields returns None when no AcroForm dict exists.
        fields = reader.get_fields()
    except Exception as e:
        log.warning("read_quote_pdf_edits: get_fields failed: %s", e)
        return {}

    if not fields:
        return {}

    out: Dict[str, str] = {}
    for name, descriptor in fields.items():
        if not _is_known_field(name):
            continue
        if not isinstance(descriptor, dict):
            continue
        # AcroForm field value lives at /V; pypdf also exposes it as 'value'
        # on its FieldDictionary class. Try /V first because it's the
        # underlying PDF spec; 'value' is pypdf's parsed convenience.
        raw = descriptor.get("/V") if "/V" in descriptor else descriptor.get("value")
        out[name] = _coerce_value(raw)
    return out


def edits_diff(original: Dict[str, str], current: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    """Return a {field_name: {before, after}} diff for fields that changed.

    Used by PR-D3 to log what the operator edited before flatten-on-send,
    so the audit row captures a precise record of in-PDF changes.

    Fields present in `current` but not `original` count as additions
    (before=""). Fields removed are not surfaced (operator can't delete
    AcroForm fields from the PDF without re-generating).
    """
    diff: Dict[str, Dict[str, str]] = {}
    for k, after in (current or {}).items():
        before = (original or {}).get(k, "")
        if str(after) != str(before):
            diff[k] = {"before": str(before), "after": str(after)}
    return diff


__all__ = [
    "read_quote_pdf_edits",
    "edits_diff",
]
