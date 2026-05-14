"""Mirror-fill — populate a target PDF from a prior good submission.

Substrate move (PR mr-wolf #4, Pattern 4 + 7). The bug class this kills:
when a buyer sends a form variant Reytech has no dedicated filler for
(703A when only `fill_703b` exists, AMS 708 when only STD 1000 exists,
etc.), the dispatcher falls through to a coincidentally-overlapping
filler or surfaces "unknown" and forces operator hand-intervention.

Mike's prototype (2026-05-13, `scripts/fill_703a_from_prior.py`) proved
out the substrate: when two form variants share AcroForm field-name
SUFFIXES (e.g. "703B_Business Name" vs "703A_Business Name"), a prior
good submission of variant A can populate variant B by translating the
prefix. Reytech's company info, certification numbers, payment terms,
delivery terms, signatures are identical across variants. Buyer-
specific fields (sol#, dates, ship-to, requestor name) come from
`rfq_data` overrides.

This module generalizes that prototype into a pure reusable function.
No filesystem I/O assumptions, no hardcoded paths — callers supply
target bytes / prior bytes / prefix mapping; the function returns
filled bytes.

Coupled with `src/forms/form_registry.py` which knows the per-form
prefix mapping, this becomes the substrate for closing every future
"buyer sent us a variant we don't have a code-fill for" gap.
"""
from __future__ import annotations

import io
import logging
from typing import Iterable, Mapping

log = logging.getLogger("reytech.mirror_fill")


def _coerce_pdf_bytes(source) -> bytes:
    """Accept bytes, file-path string, or path-like; return bytes."""
    if isinstance(source, bytes):
        return source
    if isinstance(source, (str, bytes)) or hasattr(source, "__fspath__"):
        with open(source, "rb") as f:
            return f.read()
    if hasattr(source, "read"):
        return source.read()
    raise TypeError(f"unsupported PDF source: {type(source)!r}")


def _extract_filled_fields(pdf_bytes: bytes) -> dict:
    """Return {field_name: value_str} for every field with a non-empty
    text value on the input PDF. Skips signature objects (ByteRange
    markers) and whitespace-only / "\\r" placeholder values."""
    try:
        from pypdf import PdfReader
    except ImportError:
        log.error("pypdf unavailable — mirror_fill cannot read fields")
        return {}
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
    except Exception as e:
        log.warning("mirror_fill: prior PDF read failed: %s", e)
        return {}
    fields = reader.get_fields() or {}
    out: dict[str, str] = {}
    for name, fld in fields.items():
        raw = fld.get("/V")
        if raw is None:
            continue
        s = str(raw)
        if not s.strip() or s == "\r":
            continue
        if "ByteRange" in s:
            # Signature object — captured value is the binary cert, not text.
            continue
        out[name] = s
    return out


def _field_names_and_filled(pdf_bytes: bytes) -> tuple[set[str], set[str]]:
    """Return (every_field_name, already_filled_field_names) on the
    target PDF. `already_filled` lets the mirror-fill respect any
    buyer pre-population — never overwrite operator/buyer text."""
    try:
        from pypdf import PdfReader
    except ImportError:
        return set(), set()
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
    except Exception as e:
        log.warning("mirror_fill: target PDF read failed: %s", e)
        return set(), set()
    fields = reader.get_fields() or {}
    all_names: set[str] = set()
    filled: set[str] = set()
    for name, fld in fields.items():
        all_names.add(name)
        raw = fld.get("/V")
        if raw is None:
            continue
        s = str(raw)
        if s.strip() and s != "\r":
            filled.add(name)
    return all_names, filled


def mirror_fill_from_prior_pdf(
    target,
    prior,
    *,
    source_prefix: str,
    target_prefix: str,
    overrides: Mapping[str, str] | None = None,
    preserve_buyer_filled: bool = True,
) -> bytes:
    """Populate `target` PDF from `prior` PDF by prefix translation.

    `target` / `prior` — bytes, path string, or path-like. Both are
    read into memory; nothing on disk is touched (the result is
    returned as bytes — callers write to disk if they want).

    `source_prefix` — the AcroForm field-name prefix used in `prior`
    (e.g., "703B_"). All fields starting with this prefix are
    extracted by suffix.

    `target_prefix` — the prefix the target PDF expects (e.g.,
    "703A_"). For each `source_prefix+SUFFIX` extracted, the value is
    written to `target_prefix+SUFFIX` on the target.

    `overrides` — `{full_target_field_name: value}` applied AFTER
    suffix-mirroring. Used for buyer-specific values that must come
    from `rfq_data` rather than the prior submission (sol#, due_date,
    today's signature date, bid expiration). Override values win
    over the mirrored value AND over buyer-filled fields when the
    operator explicitly wants to overwrite (e.g., today's date even
    if the buyer typed a different date).

    `preserve_buyer_filled` — when True (default), fields that are
    ALREADY filled in `target` are left alone (the buyer pre-filled
    them, we don't overwrite). When False, the mirror would overwrite
    everything; use only for fresh templates.

    Returns the filled PDF as bytes.

    Never raises on missing input fields — returns the target
    unchanged if `prior` has no usable values OR the prefix mapping
    matches nothing. Logs warnings so operations sees the gap.
    """
    target_bytes = _coerce_pdf_bytes(target)
    prior_bytes = _coerce_pdf_bytes(prior)

    if not source_prefix or not target_prefix:
        log.warning(
            "mirror_fill called without prefix mapping "
            "(source=%r target=%r) — returning target unchanged",
            source_prefix, target_prefix,
        )
        return target_bytes

    prior_filled = _extract_filled_fields(prior_bytes)
    if not prior_filled:
        log.warning(
            "mirror_fill: prior PDF has no extractable filled fields — "
            "returning target unchanged"
        )
        return target_bytes

    # Build {suffix: value} from prior's source_prefix fields.
    prior_by_suffix: dict[str, str] = {}
    for name, value in prior_filled.items():
        if not name.startswith(source_prefix):
            continue
        suffix = name[len(source_prefix):]
        prior_by_suffix[suffix] = value
    if not prior_by_suffix:
        log.warning(
            "mirror_fill: no fields on prior PDF carry the source prefix "
            "%r — returning target unchanged",
            source_prefix,
        )
        return target_bytes

    target_all, target_filled = _field_names_and_filled(target_bytes)
    if not target_all:
        log.warning(
            "mirror_fill: target PDF has no AcroForm fields — returning "
            "target unchanged"
        )
        return target_bytes

    # Build the updates map.
    updates: dict[str, str] = {}
    for suffix, value in prior_by_suffix.items():
        target_name = f"{target_prefix}{suffix}"
        if target_name not in target_all:
            continue
        if preserve_buyer_filled and target_name in target_filled:
            continue
        updates[target_name] = value

    # Overrides win regardless of buyer-filled state — that's the point
    # of an override (operator-typed today's date vs a stale prior date).
    if overrides:
        for k, v in overrides.items():
            if k in target_all:
                updates[k] = str(v)

    if not updates:
        log.info(
            "mirror_fill: prefix mapping matched %d prior fields but 0 "
            "target fields — returning target unchanged",
            len(prior_by_suffix),
        )
        return target_bytes

    # Write the updates back via pypdf's two-surface protocol:
    #   1. update_page_form_field_values per page (writes /V on the
    #      widget annotations attached to /Annots).
    #   2. Direct write to AcroForm/Fields entries (survives in readers
    #      that consult the catalog instead of the page-annot list).
    # Both passes are idempotent — running them together is what makes
    # the filled PDF render correctly across Adobe Reader, Preview,
    # Chrome's built-in viewer, and the pypdf round-tripper.
    try:
        from pypdf import PdfWriter
        from pypdf.generic import NameObject, TextStringObject
    except ImportError:
        log.error("pypdf unavailable — mirror_fill cannot write")
        return target_bytes

    try:
        writer = PdfWriter(clone_from=io.BytesIO(target_bytes))
    except Exception as e:
        log.warning("mirror_fill: writer clone failed: %s", e)
        return target_bytes

    for page in writer.pages:
        try:
            writer.update_page_form_field_values(page, updates)
        except Exception as e:
            log.debug("mirror_fill: page update suppressed: %s", e)

    try:
        if writer._root_object and "/AcroForm" in writer._root_object:
            acro = writer._root_object["/AcroForm"]
            fields_arr = acro.get("/Fields", [])
            for fld_ref in fields_arr:
                fld = fld_ref.get_object() if hasattr(fld_ref, "get_object") else fld_ref
                name = fld.get("/T")
                if name and str(name) in updates:
                    fld[NameObject("/V")] = TextStringObject(updates[str(name)])
    except Exception as e:
        log.debug("mirror_fill: AcroForm direct write suppressed: %s", e)

    buf = io.BytesIO()
    writer.write(buf)
    out = buf.getvalue()
    log.info(
        "mirror_fill: wrote %d field(s) (prior_prefix=%r target_prefix=%r, "
        "prior_filled=%d, target_fields=%d, preserved_buyer=%d)",
        len(updates), source_prefix, target_prefix,
        len(prior_filled), len(target_all),
        len(target_filled) if preserve_buyer_filled else 0,
    )
    return out


def mirror_fill_summary(
    target,
    prior,
    *,
    source_prefix: str,
    target_prefix: str,
    overrides: Mapping[str, str] | None = None,
) -> dict:
    """Diagnostic helper — return what `mirror_fill_from_prior_pdf` WOULD
    do without actually writing the PDF. Useful for tests, audit logs,
    and operator-side preview before commit.

    Returns:
        {
            "prior_filled_count":   int,
            "prior_by_suffix":      {suffix: value},
            "target_field_count":   int,
            "target_filled_count":  int,
            "mirror_updates":       {target_name: value},
            "override_updates":     {target_name: value},
            "skipped_buyer_filled": [target_names],
            "skipped_missing":      [target_names],
        }
    """
    target_bytes = _coerce_pdf_bytes(target)
    prior_bytes = _coerce_pdf_bytes(prior)
    prior_filled = _extract_filled_fields(prior_bytes)
    target_all, target_filled = _field_names_and_filled(target_bytes)

    prior_by_suffix: dict[str, str] = {}
    for name, value in prior_filled.items():
        if name.startswith(source_prefix):
            prior_by_suffix[name[len(source_prefix):]] = value

    mirror_updates: dict[str, str] = {}
    skipped_buyer: list[str] = []
    skipped_missing: list[str] = []
    for suffix, value in prior_by_suffix.items():
        target_name = f"{target_prefix}{suffix}"
        if target_name not in target_all:
            skipped_missing.append(target_name)
            continue
        if target_name in target_filled:
            skipped_buyer.append(target_name)
            continue
        mirror_updates[target_name] = value

    override_updates = {
        k: str(v) for k, v in (overrides or {}).items() if k in target_all
    }

    return {
        "prior_filled_count":   len(prior_filled),
        "prior_by_suffix":      prior_by_suffix,
        "target_field_count":   len(target_all),
        "target_filled_count":  len(target_filled),
        "mirror_updates":       mirror_updates,
        "override_updates":     override_updates,
        "skipped_buyer_filled": skipped_buyer,
        "skipped_missing":      skipped_missing,
    }
