"""form_field_extractor.py — Pull AcroForm field values from buyer PDFs.

PROBLEM (substrate, 2026-05-14): buyer PDFs (AMS 703B / 703C / STD 204
/ DSH RFQ packets) carry the authoritative answers — Solicitation
Number, Due Date, Ship-To, Quote Number, Buyer Name — in *form-field
values*, not in the static template text. `pdfplumber.extract_text()`
only returns static template text (the labels "Solicitation Number:",
"Due Date:"), never the buyer-typed values. So the body-regex extractor
in `requirement_extractor` and `attachment_contract_parser` reads the
LABEL but never the VALUE, and both 10847262 / 25CB021 landed with
default/wrong scalars (due_date 5/18 vs form's 5/15; sol# "PREQ ..."
vs form's bare digits).

This module:
  1. Opens each PDF attachment via pypdf
  2. Reads AcroForm field values (PdfReader.get_form_text_fields, plus
     the lower-level `get_fields()` for non-text fields if needed)
  3. Maps field NAMES (case-insensitive, fuzzy) onto canonical contract
     scalars: solicitation_number, due_date, due_time, ship_to, buyer_*
  4. Returns a FormFieldValues object the caller can merge into the
     email contract — form-field values WIN over body-regex matches
     (they are the buyer's literal submission)

Field-name detection is fuzzy because the same logical field is named
differently across form revisions:
  - 703B/703C: "Solicitation Number", "SolicitationNumber", "sol_num"
  - STD 204:   "Solicitation Number" (different control)
  - DSH:       "RFQ Number", "Quote Number"
We match on normalized lower-case substring containment so any of
the above lands in the same canonical slot.
"""
from __future__ import annotations

import io
import logging
import os
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional

log = logging.getLogger("reytech.form_field_extractor")

try:
    import pypdf
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False
    log.debug("pypdf not available — form-field extraction disabled")


# ── Canonical field-name aliases ───────────────────────────────────────────
# Map normalized field-name fragments to canonical contract keys. Order
# matters when one field name matches multiple aliases — most specific
# first. Match is case-insensitive substring containment after stripping
# whitespace/punctuation.

_SOL_NUM_ALIASES = [
    "solicitation_number", "solicitation number", "solicitationnumber",
    "sol_number", "sol number", "solicitation #", "solicitation no",
    "rfq number", "rfq_number", "rfqnumber", "rfq #", "rfq no",
    "bid number", "bid_number", "bid #",
    "quote number", "quote_number", "quotenumber", "quote #",
    # 25CB021's PDF uses just "Number" inside a Solicitation block —
    # too ambiguous to include here; rely on the labels above.
]

_DUE_DATE_ALIASES = [
    "due_date", "due date", "duedate",
    "date_due", "date due",
    "response_due", "response due",
    "submission_deadline", "submission deadline",
    "closing_date", "closing date", "closingdate",
    "bid_due", "bid due",
    "quote_due", "quote due",
    "deadline",
]

_DUE_TIME_ALIASES = [
    "due_time", "due time", "duetime",
    "time_due", "time due",
    "response_time", "response time",
    "closing_time", "closing time",
]

_SHIP_TO_ALIASES = [
    "ship_to", "ship to", "shipto",
    "ship_address", "ship address", "shipaddress",
    "delivery_address", "delivery address", "deliveryaddress",
    "delivery_location", "delivery location",
    "deliver_to", "deliver to",
    "destination", "destination address",
]

_BUYER_NAME_ALIASES = [
    "buyer_name", "buyer name", "buyername",
    "contact_name", "contact name", "contactname",
    "requestor", "requester", "requested_by", "requested by",
    "agency_contact", "agency contact",
    "procurement_contact", "procurement contact",
]

_BUYER_EMAIL_ALIASES = [
    "buyer_email", "buyer email", "buyeremail",
    "contact_email", "contact email", "contactemail",
    "email", "e-mail", "e_mail",
]

_RELEASE_DATE_ALIASES = [
    "release_date", "release date", "releasedate",
    "issue_date", "issue date", "issuedate",
    "posted_date", "posted date",
]


@dataclass
class FormFieldValues:
    """Canonical scalars pulled from one or more PDF AcroForms."""
    solicitation_number: str = ""
    due_date: str = ""             # ISO YYYY-MM-DD when parseable
    due_time: str = ""             # raw string; downstream parses
    ship_to: str = ""
    buyer_name: str = ""
    buyer_email: str = ""
    release_date: str = ""
    raw_fields: dict = field(default_factory=dict)
    source_files: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def has_values(self) -> bool:
        return any([
            self.solicitation_number, self.due_date, self.due_time,
            self.ship_to, self.buyer_name, self.buyer_email,
        ])

    def merge(self, other: "FormFieldValues") -> None:
        """In-place merge. `self` keeps its non-empty scalars; `other`
        fills empty slots. Lists union."""
        if not other:
            return
        for fld in ("solicitation_number", "due_date", "due_time",
                    "ship_to", "buyer_name", "buyer_email", "release_date"):
            if not getattr(self, fld) and getattr(other, fld):
                setattr(self, fld, getattr(other, fld))
        for k, v in (other.raw_fields or {}).items():
            self.raw_fields.setdefault(k, v)
        for src in (other.source_files or []):
            if src not in self.source_files:
                self.source_files.append(src)


# ─── Public API ────────────────────────────────────────────────────────────

def extract_from_attachments(attachments: list) -> FormFieldValues:
    """Walk attachments, pull AcroForm values from each PDF, merge.

    Args:
        attachments: list of {filename, file_path?, file_id?, file_bytes?}

    Returns:
        FormFieldValues with merged scalars. Cover-sheet-shaped
        filenames (containing "rfq", "cover", "instruction") are read
        first so their values take precedence on conflict.
    """
    out = FormFieldValues()
    if not attachments or not HAS_PYPDF:
        return out

    cover_first = sorted(
        attachments,
        key=lambda a: 0 if _looks_authoritative(a) else 1,
    )

    for att in cover_first:
        if not _is_pdf(att):
            continue
        single = _extract_from_one_attachment(att)
        if not single:
            continue
        out.merge(single)

    return out


def extract_from_pdf_bytes(pdf_bytes: bytes,
                           source_label: str = "") -> Optional[FormFieldValues]:
    """Pull AcroForm values from raw PDF bytes. Returns None on parse
    failure. Useful for tests and direct-blob callers."""
    if not pdf_bytes or not HAS_PYPDF:
        return None
    try:
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    except Exception as e:
        log.debug("pypdf open failed for %s: %s", source_label, e)
        return None
    return _values_from_reader(reader, source_label)


# ─── Internals ─────────────────────────────────────────────────────────────

_AUTHORITATIVE_NAME_TOKENS = (
    "rfq", "cover", "instruction", "solicitation", "bid_package",
    "bid package", "requestforquotation", "request_for_quotation",
)


def _looks_authoritative(att: dict) -> bool:
    name = (att.get("filename") or "").lower().replace("-", "_")
    return any(tok in name for tok in _AUTHORITATIVE_NAME_TOKENS)


def _is_pdf(att: dict) -> bool:
    name = (att.get("filename") or "").lower()
    ftype = (att.get("file_type") or "").lower()
    return name.endswith(".pdf") or ftype == "pdf" or "pdf" in ftype


def _extract_from_one_attachment(att: dict) -> Optional[FormFieldValues]:
    """Open one attachment via path / file_id / file_bytes and extract."""
    label = att.get("filename") or att.get("file_id") or "<unknown>"

    # file_bytes shortcut (tests + direct passthrough)
    if att.get("file_bytes"):
        return extract_from_pdf_bytes(att["file_bytes"], source_label=str(label))

    # file_path on disk
    path = att.get("file_path")
    if path and os.path.isfile(path):
        try:
            reader = pypdf.PdfReader(path)
        except Exception as e:
            log.debug("pypdf open(%s) failed: %s", path, e)
            return None
        return _values_from_reader(reader, str(label))

    # DB-blob fallback (RFQ ingest stores PDFs in rfq_files.data)
    file_id = att.get("file_id")
    if file_id is not None:
        return _extract_from_db_blob(file_id, str(label))

    return None


def _extract_from_db_blob(file_id, label: str) -> Optional[FormFieldValues]:
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT data FROM rfq_files WHERE id = ?", (file_id,)
            ).fetchone()
            if not row or not row["data"]:
                return None
            return extract_from_pdf_bytes(row["data"], source_label=label)
    except Exception as e:
        log.debug("form_field_extractor db blob(%s) failed: %s", file_id, e)
        return None


def _values_from_reader(reader, source_label: str) -> Optional[FormFieldValues]:
    """Pull form-field text from a pypdf.PdfReader. Returns None when
    the PDF has no AcroForm at all (saves caller from None-checks)."""
    fields = None
    try:
        fields = reader.get_form_text_fields()
    except Exception as e:
        log.debug("get_form_text_fields(%s) failed: %s", source_label, e)
        return None
    if not fields:
        return None

    out = FormFieldValues()
    out.source_files.append(source_label)

    # Stash everything for audit / debugging
    out.raw_fields = {str(k): ("" if v is None else str(v)) for k, v in fields.items()}

    for raw_name, raw_val in fields.items():
        if raw_val is None:
            continue
        name = _normalize_name(raw_name)
        val = str(raw_val).strip()
        if not val:
            continue

        if _name_matches(name, _SOL_NUM_ALIASES):
            if not out.solicitation_number:
                out.solicitation_number = _normalize_sol_num(val)
        elif _name_matches(name, _DUE_DATE_ALIASES):
            if not out.due_date:
                iso = _normalize_date(val)
                if iso:
                    out.due_date = iso
        elif _name_matches(name, _DUE_TIME_ALIASES):
            if not out.due_time:
                out.due_time = val
        elif _name_matches(name, _SHIP_TO_ALIASES):
            if not out.ship_to:
                out.ship_to = val
        elif _name_matches(name, _BUYER_NAME_ALIASES):
            if not out.buyer_name:
                out.buyer_name = val
        elif _name_matches(name, _BUYER_EMAIL_ALIASES):
            if not out.buyer_email:
                out.buyer_email = val
        elif _name_matches(name, _RELEASE_DATE_ALIASES):
            if not out.release_date:
                iso = _normalize_date(val)
                if iso:
                    out.release_date = iso

    return out


def _normalize_name(name: str) -> str:
    """Lowercase, collapse whitespace+underscores, strip punctuation."""
    if not name:
        return ""
    s = str(name).lower()
    s = re.sub(r"[\s_\-\.]+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s.strip()


def _name_matches(normalized_name: str, aliases: list) -> bool:
    """True when the normalized field name contains any alias."""
    if not normalized_name:
        return False
    for alias in aliases:
        # Aliases use space-separated tokens to match _normalize_name
        target = re.sub(r"[\s_\-\.]+", " ", alias.lower()).strip()
        if target and target in normalized_name:
            return True
    return False


_PREFIX_NOISE = re.compile(
    r"^\s*(?:PREQ|REQ|REF|RFQ|BID|SOL|QUOTE)\s*[:#\-]?\s*",
    re.IGNORECASE,
)


def _normalize_sol_num(raw: str) -> str:
    """Strip subject-prefix tokens (PREQ/REQ/REF/RFQ/BID/SOL/QUOTE)
    from a solicitation-number form-field value.

    Rationale: form-field values are authoritative; if the value still
    carries a subject-prefix the upstream extractor added by accident,
    drop it. A bare digits/short-alphanum form is what downstream
    forms-fill + filename builders expect.

    Examples:
      "PREQ 10847262" → "10847262"
      "RFQ # 25CB021" → "25CB021"
      "10847262"      → "10847262"  (unchanged)
      "25CB021"       → "25CB021"   (unchanged)
    """
    if not raw:
        return ""
    cleaned = _PREFIX_NOISE.sub("", raw).strip()
    return cleaned or raw.strip()


_DATE_PATTERNS = [
    # 05/15/2026, 5/15/2026, 5-15-26
    (re.compile(r"^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$"), "mdy"),
    # 2026-05-15
    (re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$"), "ymd"),
    # May 15, 2026 / May 15 2026
    (re.compile(r"^([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})$"), "long"),
]


def _normalize_date(raw: str) -> str:
    """Parse a form-field date value into ISO YYYY-MM-DD. Returns "" on
    parse failure (caller falls back to body regex)."""
    if not raw:
        return ""
    s = str(raw).strip()
    for pat, kind in _DATE_PATTERNS:
        m = pat.match(s)
        if not m:
            continue
        try:
            if kind == "mdy":
                mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
                if yy < 100:
                    yy += 2000
                return datetime(yy, mm, dd).strftime("%Y-%m-%d")
            if kind == "ymd":
                yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
                return datetime(yy, mm, dd).strftime("%Y-%m-%d")
            if kind == "long":
                dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}",
                                       "%B %d %Y")
                return dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            continue
    return ""


__all__ = [
    "FormFieldValues",
    "extract_from_attachments",
    "extract_from_pdf_bytes",
]
