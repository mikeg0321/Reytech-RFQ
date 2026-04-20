"""Read-back verifier — catch fills that silently dropped data.

The fill engine writes values to specific AcroForm fields. On some profiles
or pypdf combinations a field may accept a value but show blank when the
PDF is reopened (missing /AP appearances, mismatched field type, etc.).
The existing qa_engine.validate() has value-matching for a narrow whitelist
of fields (vendor.name, header.solicitation_number, a few more) — anything
outside that list can go blank and nothing flags it.

This module is the completeness backstop: for every pdf_field the filler
*intended* to write a non-empty value to, verify the output PDF actually
reads back a non-empty value. Raise if any expected write is missing.

    from src.forms.readback import verify_readback, ReadbackReport
    report = verify_readback(pdf_bytes, quote, profile)
    if not report.passed:
        raise StageBlocked(f"readback failed: {report.summary}")

Design choices:
  * Reuses fill_engine._build_static_field_map and _build_row_field_map
    as the source of truth for "what the filler wrote". No duplicated
    semantic-to-Quote mapping — if the filler changes, readback follows.
  * Presence check only (non-empty == pass). Deep value equality is
    qa_engine's job; this module is the zero-dollar-item / blank-vendor
    detector, not a second QA engine.
  * Skips checkbox fields (pypdf read-back is unreliable for /AP states
    and qa_engine covers boolean state already).
"""
from __future__ import annotations

import io
import logging
from dataclasses import dataclass, field
from typing import Optional

from src.core.quote_model import Quote
from src.forms.profile_registry import FormProfile

log = logging.getLogger(__name__)


@dataclass
class ReadbackMiss:
    pdf_field: str
    expected_value: str
    kind: str  # "static" | "row"


@dataclass
class ReadbackReport:
    passed: bool
    profile_id: str
    fields_expected: int = 0
    fields_readback_ok: int = 0
    misses: list[ReadbackMiss] = field(default_factory=list)

    @property
    def summary(self) -> str:
        if self.fields_expected == 0:
            return f"{self.profile_id}: no expected writes"
        return (
            f"{self.profile_id}: {self.fields_readback_ok}/{self.fields_expected} "
            f"fields read back non-empty, {len(self.misses)} missing"
        )


def _read_pdf_value(pdf_fields: dict, name: str) -> str:
    entry = pdf_fields.get(name)
    if entry is None:
        return ""
    if isinstance(entry, dict):
        val = entry.get("/V", "")
    else:
        val = str(entry)
    if val is None:
        return ""
    return str(val).strip()


def verify_readback(
    pdf_bytes: bytes,
    quote: Quote,
    profile: FormProfile,
) -> ReadbackReport:
    """Diff the filled PDF against the values the filler intended to write.

    Returns a ReadbackReport. passed=True when every expected non-empty
    write read back with a non-empty value. Expected writes are computed
    by reusing the fill_engine's own field map builders, so any update to
    the filler's semantic mapping flows through without code duplication.
    """
    from src.forms.fill_engine import (
        _build_static_field_map,
        _build_row_field_map,
    )

    report = ReadbackReport(passed=True, profile_id=profile.id)

    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pdf_fields = reader.get_fields() or {}
    except Exception as e:
        log.warning("readback: failed to open PDF for %s: %s", profile.id, e)
        report.passed = False
        report.misses.append(
            ReadbackMiss(pdf_field="<pdf>", expected_value=str(e), kind="static")
        )
        return report

    # Static fields — vendor, buyer, header, totals.
    try:
        static_expected = _build_static_field_map(quote, profile)
    except Exception as e:
        log.warning("readback: _build_static_field_map raised: %s", e)
        static_expected = {}

    for pdf_field_name, expected in static_expected.items():
        if expected in (True, False):
            continue
        if not expected or not str(expected).strip():
            continue
        report.fields_expected += 1
        actual = _read_pdf_value(pdf_fields, pdf_field_name)
        if actual:
            report.fields_readback_ok += 1
        else:
            report.misses.append(ReadbackMiss(
                pdf_field=pdf_field_name,
                expected_value=str(expected)[:80],
                kind="static",
            ))

    # Row fields — item descriptions, unit prices, extensions.
    try:
        row_expected = _build_row_field_map(quote, profile)
    except Exception as e:
        log.warning("readback: _build_row_field_map raised: %s", e)
        row_expected = {}

    for pdf_field_name, expected in row_expected.items():
        if not expected or not str(expected).strip():
            continue
        report.fields_expected += 1
        actual = _read_pdf_value(pdf_fields, pdf_field_name)
        if actual:
            report.fields_readback_ok += 1
        else:
            report.misses.append(ReadbackMiss(
                pdf_field=pdf_field_name,
                expected_value=str(expected)[:80],
                kind="row",
            ))

    report.passed = len(report.misses) == 0
    log.info("readback: %s", report.summary)
    return report
