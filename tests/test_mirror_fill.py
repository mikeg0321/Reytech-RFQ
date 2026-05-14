"""mirror_fill_from_prior_pdf — pin the substrate behavior.

PR mr-wolf #4. Closes Pattern 4 (template fallback drift) by making
mirror-fill the canonical path for any buyer-form variant whose
fill_* function doesn't exist or doesn't carry per-bid overrides.

These tests pin the prefix-translation contract, the buyer-preserved
invariant, and the override-wins semantics against a synthesized
prior + target PDF pair. Real CCHCS / CalVet / DSH templates are NOT
in the repo; we synthesize minimal AcroForm PDFs in the tests so the
suite stays hermetic.
"""
from __future__ import annotations

import io
from typing import Iterable

import pytest

from src.forms.mirror_fill import (
    mirror_fill_from_prior_pdf,
    mirror_fill_summary,
)


# ── Test fixture: build a minimal AcroForm PDF with named text fields ──


def _build_acroform_pdf(fields: dict[str, str]) -> bytes:
    """Build a minimal single-page PDF with AcroForm text fields whose
    names + values come from `fields`. Uses pypdf's PdfWriter +
    add_form_topname / add_form_field. Returns bytes."""
    from pypdf import PdfWriter
    from pypdf.generic import (
        ArrayObject, DictionaryObject, NameObject, NumberObject,
        TextStringObject, FloatObject, IndirectObject,
    )

    writer = PdfWriter()
    page = writer.add_blank_page(width=612, height=792)

    annots: list = []
    for i, (name, value) in enumerate(fields.items()):
        # Build a /Tx (text) form-field widget annotation.
        widget = DictionaryObject({
            NameObject("/Type"):    NameObject("/Annot"),
            NameObject("/Subtype"): NameObject("/Widget"),
            NameObject("/FT"):      NameObject("/Tx"),
            NameObject("/T"):       TextStringObject(name),
            NameObject("/V"):       TextStringObject(value),
            NameObject("/DV"):      TextStringObject(""),
            NameObject("/Ff"):      NumberObject(0),
            NameObject("/Rect"):    ArrayObject([
                FloatObject(50 + (i % 4) * 130),
                FloatObject(700 - (i // 4) * 30),
                FloatObject(170 + (i % 4) * 130),
                FloatObject(720 - (i // 4) * 30),
            ]),
            NameObject("/P"):       page.indirect_reference,
        })
        widget_ref = writer._add_object(widget)
        annots.append(widget_ref)

    page[NameObject("/Annots")] = ArrayObject(annots)

    acro = DictionaryObject({
        NameObject("/Fields"): ArrayObject(annots),
        NameObject("/NeedAppearances"): NumberObject(1),
    })
    writer._root_object[NameObject("/AcroForm")] = writer._add_object(acro)

    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def _field_values(pdf_bytes: bytes) -> dict[str, str]:
    """Read back {field_name: value} from a PDF."""
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(pdf_bytes))
    out: dict[str, str] = {}
    for name, fld in (reader.get_fields() or {}).items():
        v = fld.get("/V")
        out[name] = str(v) if v is not None else ""
    return out


# ── 703B → 703A prefix translation ──────────────────────────────────


@pytest.fixture
def prior_703b_pdf() -> bytes:
    """A prior good 703B submission carrying Reytech's static fields."""
    return _build_acroform_pdf({
        "703B_Business Name":     "Reytech Inc.",
        "703B_FEIN":              "82-XXXXXXX",
        "703B_Address":           "30 Carnoustie Way, Trabuco Canyon CA",
        "703B_Phone":             "949-229-1575",
        "703B_Contact Person":    "Michael Guadan",
        "703B_Solicitation Number": "10838974",   # old sol# — must NOT carry over verbatim
        "703B_BidExpirationDate": "04/12/2026",  # old expiration
        "703B_Payment Terms":     "Net 45",
        "703B_Bid Date":          "03/13/2026",
    })


@pytest.fixture
def target_703a_pdf() -> bytes:
    """Buyer's 703A template — same suffixes as 703B, prefix flipped.
    The buyer pre-filled `703A_Solicitation Number` with the NEW sol#
    + a buyer-typed value in `703A_Department` (which has no 703B
    counterpart — must remain intact)."""
    return _build_acroform_pdf({
        "703A_Business Name":       "",
        "703A_FEIN":                "",
        "703A_Address":             "",
        "703A_Phone":               "",
        "703A_Contact Person":      "",
        "703A_Solicitation Number": "10846357",  # buyer pre-filled new sol#
        "703A_BidExpirationDate":   "",
        "703A_Payment Terms":       "",
        "703A_Bid Date":            "",
        "703A_Department":          "PVSP Mailroom",  # buyer-typed; no 703B sibling
    })


def test_static_fields_mirror_from_prior(prior_703b_pdf, target_703a_pdf):
    """Reytech's Business Name + FEIN + Address etc. should populate
    the 703A from the prior 703B via suffix translation."""
    out = mirror_fill_from_prior_pdf(
        target_703a_pdf, prior_703b_pdf,
        source_prefix="703B_", target_prefix="703A_",
    )
    vals = _field_values(out)
    assert vals["703A_Business Name"] == "Reytech Inc."
    assert vals["703A_FEIN"] == "82-XXXXXXX"
    assert vals["703A_Address"] == "30 Carnoustie Way, Trabuco Canyon CA"
    assert vals["703A_Phone"] == "949-229-1575"
    assert vals["703A_Contact Person"] == "Michael Guadan"
    assert vals["703A_Payment Terms"] == "Net 45"


def test_buyer_pre_filled_fields_are_preserved(prior_703b_pdf, target_703a_pdf):
    """The buyer pre-filled 703A_Solicitation Number with the NEW sol#.
    Mirror-fill must NOT overwrite that with the prior's old sol#."""
    out = mirror_fill_from_prior_pdf(
        target_703a_pdf, prior_703b_pdf,
        source_prefix="703B_", target_prefix="703A_",
    )
    vals = _field_values(out)
    # Buyer's value wins — prior had "10838974", buyer typed "10846357".
    assert vals["703A_Solicitation Number"] == "10846357"


def test_overrides_win_over_prior(prior_703b_pdf, target_703a_pdf):
    """When the operator supplies an override (today's date, this bid's
    expiration), it wins over both the prior's value AND any buyer-
    typed value. Overrides are explicit operator intent."""
    out = mirror_fill_from_prior_pdf(
        target_703a_pdf, prior_703b_pdf,
        source_prefix="703B_", target_prefix="703A_",
        overrides={
            "703A_BidExpirationDate": "06/12/2026",
            "703A_Bid Date":          "05/13/2026",
            "703A_Solicitation Number": "10846357",   # explicit operator override
        },
    )
    vals = _field_values(out)
    assert vals["703A_BidExpirationDate"] == "06/12/2026"
    assert vals["703A_Bid Date"] == "05/13/2026"
    assert vals["703A_Solicitation Number"] == "10846357"


def test_fields_without_sibling_are_left_alone(prior_703b_pdf, target_703a_pdf):
    """703A_Department has no 703B_Department counterpart on the prior.
    The buyer-typed value must remain intact after mirror-fill."""
    out = mirror_fill_from_prior_pdf(
        target_703a_pdf, prior_703b_pdf,
        source_prefix="703B_", target_prefix="703A_",
    )
    vals = _field_values(out)
    assert vals["703A_Department"] == "PVSP Mailroom"


def test_empty_prior_returns_target_unchanged(target_703a_pdf):
    """A prior PDF with no values yields no updates — the target is
    returned unchanged (operator sees the buyer's template as-is)."""
    empty_prior = _build_acroform_pdf({"703B_Business Name": ""})
    out = mirror_fill_from_prior_pdf(
        target_703a_pdf, empty_prior,
        source_prefix="703B_", target_prefix="703A_",
    )
    # mirror_fill returns target unchanged when prior is empty —
    # don't assert byte-equality (pypdf may re-serialize), but assert
    # the un-pre-filled fields stayed empty.
    vals = _field_values(out)
    assert vals["703A_Business Name"] == ""
    assert vals["703A_Department"] == "PVSP Mailroom"  # buyer's value intact


def test_no_matching_prefix_yields_no_updates(prior_703b_pdf, target_703a_pdf):
    """If the source_prefix doesn't match any prior field, mirror-fill
    returns the target unchanged. Catches mis-configured prefix in
    `form_registry` before it silently no-ops in production."""
    out = mirror_fill_from_prior_pdf(
        target_703a_pdf, prior_703b_pdf,
        source_prefix="999_NONEXISTENT_", target_prefix="703A_",
    )
    vals = _field_values(out)
    # Buyer pre-fills intact, Reytech fields still empty.
    assert vals["703A_Solicitation Number"] == "10846357"
    assert vals["703A_Business Name"] == ""


def test_summary_diagnostic_pre_commit_audit(prior_703b_pdf, target_703a_pdf):
    """`mirror_fill_summary` mirrors `mirror_fill_from_prior_pdf` but
    returns the planned updates without writing. Used by tests +
    operator-side preview + the audit trail."""
    summary = mirror_fill_summary(
        target_703a_pdf, prior_703b_pdf,
        source_prefix="703B_", target_prefix="703A_",
        overrides={"703A_Bid Date": "05/13/2026"},
    )
    assert summary["prior_filled_count"] == 9
    assert summary["target_field_count"] == 10  # 703A's 9 + 703A_Department
    assert "703A_Business Name" in summary["mirror_updates"]
    assert summary["mirror_updates"]["703A_Business Name"] == "Reytech Inc."
    # Buyer pre-filled 703A_Solicitation Number → skipped.
    assert "703A_Solicitation Number" in summary["skipped_buyer_filled"]
    # The override is in its own bucket — wins over skipped.
    assert "703A_Bid Date" in summary["override_updates"]
    # 703B_Department doesn't exist on prior → nothing for that field.
    # 703B_Department NOT being on prior means we don't even try to map
    # to 703A_Department, so no "skipped_missing" entry there either.
