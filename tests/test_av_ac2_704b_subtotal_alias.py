"""PR-AV-AC2 — verify_704b_computations must accept SUBTOTAL as an
alias for EXTENSION on buyer-supplied CCHCS 704B variants.

Background
----------
The 5/15 PREQ 10847262 (rfq_9e63456e) post-ship audit found the
form_qa banner reporting:

    Subtotal mismatch: sum of extensions = $0.00, but
    MERCHANDISE SUBTOTAL = $29279.35

Forensic walkthrough of the buyer's `Quote Worksheet - 704B -
Attachment 2.pdf` (the `pc_704_pdf_fillable` shape — 362 inline
form fields across 2 pages, multi-column per row) showed all 7
line-total fields filled correctly:

    SUBTOTALRow1 = 1862.70    (30 × 62.09)
    SUBTOTALRow2 = 6075.00    (50 × 121.50)
    SUBTOTALRow3 = 6075.00    (50 × 121.50)
    SUBTOTALRow4 = 3025.25    (25 × 121.01)
    SUBTOTALRow5 = 2632.50    (15 × 175.50)
    SUBTOTALRow6 = 9440.00    (1000 × 9.44)
    SUBTOTALRow7 =  168.90    (6 × 28.15)
    sum         = 29279.35    matches MERCHANDISE SUBTOTAL ✓

But `verify_704b_computations` was hardcoded to read
`EXTENSIONRow{n}` / `EXTENSION{row_key}` — neither field exists in
this template variant. So every row landed in the `elif ext is not
None: computed_total += ext` branch failing the guard, and
`computed_total` stayed at 0.0. The mismatch banner fired even
though the math was correct.

The fix
-------
Add `SUBTOTALRow{n}{suffix}` / `SUBTOTAL{row_key}` as fallbacks in
the ext lookup chain. EXTENSION-named templates still win on first
match; SUBTOTAL-named buyer variants now resolve via the fallback.

What this test pins
-------------------
  - SUBTOTALRow{n}-named fields produce a clean pass (no mismatch
    issue) when the math is internally consistent
  - EXTENSIONRow{n}-named fields still work (no regression)
  - When both EXTENSION and SUBTOTAL are present, EXTENSION wins
    (deterministic preference; predictable for legacy data)
  - Mismatch detection still works: a real subtotal/extension
    mismatch on a SUBTOTAL-named template still triggers the issue
  - Source-grep guard: PR-AV-AC2 marker present + fallback chain
    contains both SUBTOTALRow{n}{suffix} and SUBTOTAL{row_key}
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


# Helper: write a synthetic PDF carrying the named form fields with
# given /V values. Used to drive verify_704b_computations without a
# real CCHCS 704B template (those aren't in fixtures/).
def _build_704b_like_pdf(tmp_path, name_to_value):
    """Create a 2-page PDF with AcroForm text fields named per
    name_to_value mapping. Returns the output path."""
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from pypdf import PdfReader, PdfWriter
    from pypdf.generic import (
        NameObject, TextStringObject, ArrayObject, DictionaryObject,
        IndirectObject, NumberObject,
    )

    # 1. Create blank 2-page PDF via reportlab.
    raw = tmp_path / "raw.pdf"
    c = canvas.Canvas(str(raw), pagesize=letter)
    c.showPage()  # page 1
    c.showPage()  # page 2
    c.save()

    # 2. Add a text-field annotation for each name on page 1, with /V
    #    pre-populated. pypdf doesn't have a high-level form-field
    #    builder, so we drop low-level dictionaries directly.
    reader = PdfReader(str(raw))
    writer = PdfWriter()
    writer.append(reader)

    page = writer.pages[0]
    annots = []

    field_refs = []
    for name, value in name_to_value.items():
        field = DictionaryObject({
            NameObject("/Type"): NameObject("/Annot"),
            NameObject("/Subtype"): NameObject("/Widget"),
            NameObject("/FT"): NameObject("/Tx"),
            NameObject("/T"): TextStringObject(name),
            NameObject("/V"): TextStringObject(str(value)),
            NameObject("/Rect"): ArrayObject(
                [NumberObject(0), NumberObject(0),
                 NumberObject(0), NumberObject(0)]
            ),
            NameObject("/F"): NumberObject(4),
        })
        ref = writer._add_object(field)
        field_refs.append(ref)
        annots.append(ref)

    page[NameObject("/Annots")] = ArrayObject(annots)

    # AcroForm root — required so PdfReader.get_fields() can find
    # them later (this is exactly the AC1 lesson).
    acroform = DictionaryObject({
        NameObject("/Fields"): ArrayObject(field_refs),
        NameObject("/NeedAppearances"): writer._add_object(
            DictionaryObject()
        ),
    })
    # Use a simple Boolean — DictionaryObject for /NeedAppearances
    # was wrong above; replace with True (pypdf's BooleanObject).
    from pypdf.generic import BooleanObject
    acroform[NameObject("/NeedAppearances")] = BooleanObject(True)
    writer._root_object[NameObject("/AcroForm")] = acroform

    out = tmp_path / "synth_704b.pdf"
    with open(out, "wb") as f:
        writer.write(f)
    return out


def _verify(pdf_path):
    """Convenience wrapper — import lazily so test collection doesn't
    pull form_qa's heavyweight imports at module import time."""
    from src.forms.form_qa import verify_704b_computations
    return verify_704b_computations(str(pdf_path), {})


# ── Tests ───────────────────────────────────────────────────────────


def test_subtotal_named_fields_pass_when_math_consistent(tmp_path):
    """SUBTOTALRow{n} naming + correct math = no mismatch issue.

    Pins the AC2 fix: the buyer-template variant's field name now
    resolves cleanly and the audit doesn't false-flag a working
    package.
    """
    pdf = _build_704b_like_pdf(tmp_path, {
        "QTYRow1": "30", "PRICE PER UNITRow1": "62.09", "SUBTOTALRow1": "1862.70",
        "QTYRow2": "50", "PRICE PER UNITRow2": "121.50", "SUBTOTALRow2": "6075.00",
        "QTYRow3": "6",  "PRICE PER UNITRow3": "28.15",  "SUBTOTALRow3": "168.90",
        "fill_154": "8106.60",  # sum of subtotals above
    })
    result = _verify(pdf)
    assert result["passed"], (
        f"clean SUBTOTAL-named 704B should pass; got issues={result['issues']}"
    )
    # No mismatch issue should appear
    mismatch = [i for i in result["issues"] if "Subtotal mismatch" in i]
    assert not mismatch, f"unexpected mismatch issue: {mismatch}"


def test_extension_named_fields_still_pass(tmp_path):
    """Regression guard: legacy EXTENSION-named template variant
    still works exactly as before.
    """
    pdf = _build_704b_like_pdf(tmp_path, {
        "QTYRow1": "10", "PRICE PER UNITRow1": "5.00", "EXTENSIONRow1": "50.00",
        "QTYRow2": "20", "PRICE PER UNITRow2": "3.50", "EXTENSIONRow2": "70.00",
        "fill_154": "120.00",
    })
    result = _verify(pdf)
    assert result["passed"], (
        f"legacy EXTENSION-named template should pass; got issues={result['issues']}"
    )


def test_extension_wins_when_both_present(tmp_path):
    """Determinism: if both EXTENSION and SUBTOTAL exist for the
    same row, the EXTENSION value drives the math.

    This matters for hybrid templates where one column carries the
    canonical line total and the other is a derived/cross-reference
    cell — we don't want the audit to silently pick the wrong one.
    """
    pdf = _build_704b_like_pdf(tmp_path, {
        "QTYRow1": "10", "PRICE PER UNITRow1": "5.00",
        "EXTENSIONRow1": "50.00",   # EXTENSION value drives the total
        "SUBTOTALRow1": "999.99",   # ignored when EXTENSION present
        "fill_154": "50.00",
    })
    result = _verify(pdf)
    assert result["passed"], (
        f"EXTENSION should win when both present; got issues={result['issues']}"
    )


def test_mismatch_still_detected_on_subtotal_template(tmp_path):
    """The AC2 fallback must not hide REAL mismatches. A bid where
    SUBTOTALRows don't sum to fill_154 still trips the issue.
    """
    pdf = _build_704b_like_pdf(tmp_path, {
        "QTYRow1": "10", "PRICE PER UNITRow1": "5.00", "SUBTOTALRow1": "50.00",
        "QTYRow2": "20", "PRICE PER UNITRow2": "3.50", "SUBTOTALRow2": "70.00",
        "fill_154": "999.99",  # intentionally wrong
    })
    result = _verify(pdf)
    assert not result["passed"]
    assert any("Subtotal mismatch" in i for i in result["issues"]), (
        f"mismatch must be detected on SUBTOTAL-named template; "
        f"issues={result['issues']}"
    )


def test_source_grep_ac2_marker_present():
    """Lock the fallback in source so future refactors don't drop it
    silently.
    """
    target = REPO_ROOT / "src" / "forms" / "form_qa.py"
    src = target.read_text(encoding="utf-8")
    assert "PR-AV-AC2" in src, "PR-AV-AC2 marker must remain in form_qa.py"
    # Both fallback patterns must be in the source
    assert "SUBTOTALRow{n}{suffix}" in src, (
        "form_qa must read SUBTOTALRow{n}{suffix} as a fallback "
        "for the canonical EXTENSION naming"
    )
    assert "SUBTOTAL{row_key}" in src, (
        "form_qa must read SUBTOTAL{row_key} as a fallback for "
        "the canonical EXTENSION naming"
    )
